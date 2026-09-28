from fastapi.testclient import TestClient

from ingester.app import create_app
from ingester.core import IngestionFactory
from ingester.gateways.mt5 import Mt5Ingestion
from ingester.runtime import IngesterRuntime
from ingester.schemas import GatewayEnum, Timeframe
from ingester.settings import AppSettings, Mt5Settings, Settings
from tests.fakes import FakeConnection, FakeNotifier, FakePublisher, FakeTerminal, rate


def make_settings() -> Settings:
  return Settings(
    _env_file=None,
    app=AppSettings(_env_file=None, INSTANCE_ID="vps-test", GATEWAYS=["mt5"]),
    mt5=Mt5Settings(
      _env_file=None,
      SYMBOLS=["XAUUSD"],
      TIMEFRAMES=["M5"],
      POLL_INTERVAL_SECONDS=0.01,
    ),
  )


def make_runtime_factory(connection: FakeConnection, notifier: FakeNotifier):
  terminal = FakeTerminal()
  terminal.set_rates("XAUUSD", Timeframe.M5, [rate(1_790_000_100)])

  def build(ctx):
    return Mt5Ingestion(
      config=ctx.settings.mt5,
      terminal=terminal,
      publisher=ctx.publisher,
      notifier=ctx.notifier,
      instance_id=ctx.settings.app.instance_id,
    )

  def runtime_factory(config: Settings) -> IngesterRuntime:
    factory = IngestionFactory()
    factory.register(GatewayEnum.MT5, build)
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
    (gateway,) = body["gateways"]
    assert gateway["gateway"] == "mt5"
    assert gateway["symbols"] == ["XAUUSD"]
    assert gateway["timeframes"] == ["M5"]
    assert client.get("/docs").status_code == 404  # docs off by default

  assert connection.connected and connection.closed
  assert notifier.started and notifier.stopped
  assert any("Ingester Running" in m for m in notifier.messages)
  assert "Ingester Stopped" in notifier.messages[-1]


def test_dead_nats_degrades_instead_of_crashing():
  # A market-data gateway that exits because NATS blinked drops bars nobody can
  # get back. It stays up, keeps detecting, and says it is degraded.
  connection, notifier = FakeConnection(fail=True), FakeNotifier()
  app = create_app(make_settings(), make_runtime_factory(connection, notifier))

  with TestClient(app) as client:
    assert client.get("/health").status_code == 200
    (gateway,) = client.get("/status").json()["gateways"]
    assert gateway["status"] == "running"

  assert not connection.connected
  assert any("Ingester Degraded" in m for m in notifier.messages)
  assert any("nats down" in m for m in notifier.messages)
  assert notifier.stopped


def test_a_gateway_that_will_not_start_does_not_take_the_others_down():
  connection, notifier = FakeConnection(), FakeNotifier()

  def explode(ctx):
    raise RuntimeError("terminal missing")

  def runtime_factory(config: Settings) -> IngesterRuntime:
    factory = IngestionFactory()
    factory.register(GatewayEnum.MT5, explode)
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
    assert client.get("/status").json()["gateways"] == []

  assert any("terminal missing" in m for m in notifier.messages)
  assert connection.closed and notifier.stopped
