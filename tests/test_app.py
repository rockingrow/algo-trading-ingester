from fastapi.testclient import TestClient

from ingester.app import create_app
from ingester.core import IngestionFactory
from ingester.gateways.crypto.binance import BinanceIngestion
from ingester.gateways.forex.mt5 import Mt5Ingestion
from ingester.runtime import IngesterRuntime
from ingester.schemas import GatewayEnum, MarketEnum, Timeframe
from ingester.settings import (
  AppSettings,
  BinanceSettings,
  MarketSettings,
  Mt5Settings,
  Settings,
)
from tests.fakes import (
  FakeConnection,
  FakeKlineHistory,
  FakeKlineStream,
  FakeNotifier,
  FakePublisher,
  FakeTerminal,
  kline,
  rate,
)

T0_MS = 1_790_589_600_000


def make_settings(*, markets=None, problems=None) -> Settings:
  """Settings with the market files already "loaded" — no TOML is read."""
  if markets is None:
    markets = {
      MarketEnum.FOREX: MarketSettings(
        market=MarketEnum.FOREX,
        gateways={
          GatewayEnum.MT5: Mt5Settings(
            _env_file=None,
            ENABLE=True,
            SYMBOLS=["XAUUSD"],
            TIMEFRAMES=["M5"],
            POLL_INTERVAL_SECONDS=0.01,
          )
        },
      ),
      MarketEnum.CRYPTO: MarketSettings(
        market=MarketEnum.CRYPTO,
        gateways={
          GatewayEnum.BINANCE: BinanceSettings(
            _env_file=None,
            ENABLE=True,
            SYMBOLS=["BTCUSDT"],
            TIMEFRAMES=["M5"],
            RECONNECT_INTERVAL_SECONDS=0.01,
          )
        },
      ),
    }
  config = Settings(
    _env_file=None,
    app=AppSettings(_env_file=None, INSTANCE_ID="vps-test"),
    markets=markets,
  )
  config.market_problems = list(problems or [])
  return config


def make_runtime_factory(connection: FakeConnection, notifier: FakeNotifier):
  terminal = FakeTerminal()
  terminal.set_rates("XAUUSD", Timeframe.M5, [rate(1_790_000_100)])
  stream = FakeKlineStream([kline(T0_MS, interval="5m")])

  def build_mt5(ctx):
    return Mt5Ingestion(
      config=ctx.config_as(Mt5Settings),
      terminal=terminal,
      publisher=ctx.publisher,
      notifier=ctx.notifier,
      instance_id=ctx.settings.app.instance_id,
    )

  def build_binance(ctx):
    return BinanceIngestion(
      config=ctx.config_as(BinanceSettings),
      stream=stream,
      history=FakeKlineHistory(),
      publisher=ctx.publisher,
      notifier=ctx.notifier,
      instance_id=ctx.settings.app.instance_id,
    )

  def runtime_factory(config: Settings) -> IngesterRuntime:
    factory = IngestionFactory()
    factory.register(GatewayEnum.MT5, build_mt5)
    factory.register(GatewayEnum.BINANCE, build_binance)
    return IngesterRuntime(
      config,
      factory=factory,
      notifier=notifier,
      connection=connection,
      publisher=FakePublisher(),
      subject_filter="TEST.>",
    )

  return runtime_factory


def test_lifespan_health_and_status():
  connection, notifier = FakeConnection(), FakeNotifier()
  app = create_app(make_settings(), make_runtime_factory(connection, notifier))

  with TestClient(app) as client:
    assert client.get("/health").json()["status"] == "ok"
    body = client.get("/status").json()
    assert body["instance_id"] == "vps-test"
    assert client.get("/docs").status_code == 404  # docs off by default

  assert connection.connected and connection.closed
  assert notifier.started and notifier.stopped
  assert any("Ingester Running" in m for m in notifier.messages)
  assert "Ingester Stopped" in notifier.messages[-1]


def test_both_markets_run_side_by_side():
  connection, notifier = FakeConnection(), FakeNotifier()
  app = create_app(make_settings(), make_runtime_factory(connection, notifier))

  with TestClient(app) as client:
    gateways = client.get("/status").json()["gateways"]

  by_gateway = {snap["gateway"]: snap for snap in gateways}
  assert set(by_gateway) == {"mt5", "binance"}
  assert by_gateway["mt5"]["market"] == "forex"
  assert by_gateway["mt5"]["symbols"] == ["XAUUSD"]
  assert by_gateway["binance"]["market"] == "crypto"
  assert by_gateway["binance"]["symbols"] == ["BTCUSDT"]
  # The "running" notification names the market, so an operator reading it can
  # tell which file each stream came from.
  running = next(m for m in notifier.messages if "Ingester Running" in m)
  assert "forex/MT5" in running
  assert "crypto/BINANCE" in running


def test_only_the_enabled_markets_start():
  connection, notifier = FakeConnection(), FakeNotifier()
  crypto_only = {
    MarketEnum.CRYPTO: MarketSettings(
      market=MarketEnum.CRYPTO,
      gateways={
        GatewayEnum.BINANCE: BinanceSettings(
          _env_file=None, ENABLE=True, SYMBOLS=["BTCUSDT"], TIMEFRAMES=["M5"]
        )
      },
    )
  }
  app = create_app(
    make_settings(markets=crypto_only), make_runtime_factory(connection, notifier)
  )

  with TestClient(app) as client:
    gateways = client.get("/status").json()["gateways"]

  assert [snap["gateway"] for snap in gateways] == ["binance"]


def test_an_unreadable_market_file_degrades_instead_of_crashing():
  # SOURCE_MARKET names a market whose TOML is missing: the markets that did
  # load still run, and the operator is told which one did not.
  connection, notifier = FakeConnection(), FakeNotifier()
  config = make_settings(problems=["market 'crypto': config/crypto.toml is missing"])
  app = create_app(config, make_runtime_factory(connection, notifier))

  with TestClient(app) as client:
    assert len(client.get("/status").json()["gateways"]) == 2

  assert any("Ingester Degraded" in m for m in notifier.messages)
  assert any("crypto.toml is missing" in m for m in notifier.messages)


def test_dead_nats_degrades_instead_of_crashing():
  # A market-data gateway that exits because NATS blinked drops bars nobody can
  # get back. It stays up, keeps detecting, and says it is degraded.
  connection, notifier = FakeConnection(fail=True), FakeNotifier()
  app = create_app(make_settings(), make_runtime_factory(connection, notifier))

  with TestClient(app) as client:
    assert client.get("/health").status_code == 200
    gateways = client.get("/status").json()["gateways"]
    assert all(snap["status"] == "running" for snap in gateways)

  assert not connection.connected
  assert any("Ingester Degraded" in m for m in notifier.messages)
  assert any("nats down" in m for m in notifier.messages)
  assert notifier.stopped


def test_a_gateway_that_will_not_start_does_not_take_the_others_down():
  connection, notifier = FakeConnection(), FakeNotifier()
  stream = FakeKlineStream()

  def explode(ctx):
    raise RuntimeError("terminal missing")

  def build_binance(ctx):
    return BinanceIngestion(
      config=ctx.config_as(BinanceSettings),
      stream=stream,
      history=FakeKlineHistory(),
      publisher=ctx.publisher,
      notifier=ctx.notifier,
      instance_id=ctx.settings.app.instance_id,
    )

  def runtime_factory(config: Settings) -> IngesterRuntime:
    factory = IngestionFactory()
    factory.register(GatewayEnum.MT5, explode)
    factory.register(GatewayEnum.BINANCE, build_binance)
    return IngesterRuntime(
      config,
      factory=factory,
      notifier=notifier,
      connection=connection,
      publisher=FakePublisher(),
      subject_filter="TEST.>",
    )

  app = create_app(make_settings(), runtime_factory)
  with TestClient(app) as client:
    # Forex is down, crypto keeps ingesting.
    gateways = client.get("/status").json()["gateways"]
    assert [snap["gateway"] for snap in gateways] == ["binance"]

  assert any("forex/mt5" in m and "terminal missing" in m for m in notifier.messages)
  assert connection.closed and notifier.stopped


def test_nothing_ingesting_is_reported_degraded():
  # Up but publishing nothing must never read as healthy: this is every
  # "enable = false", or an empty SOURCE_MARKET.
  connection, notifier = FakeConnection(), FakeNotifier()
  config = make_settings(markets={})
  app = create_app(config, make_runtime_factory(connection, notifier))

  with TestClient(app) as client:
    assert client.get("/status").json()["gateways"] == []

  assert any("Ingester Degraded" in m for m in notifier.messages)
  assert any("no gateway is ingesting" in m for m in notifier.messages)
