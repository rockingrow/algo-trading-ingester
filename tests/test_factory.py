import inspect

import pytest

from ingester.core import (
  BaseIngestion,
  GatewayNotRegisteredError,
  IngestionContext,
  IngestionFactory,
)
from ingester.schemas import GatewayEnum, MarketEnum
from ingester.settings import BinanceSettings, Mt5Settings, Settings
from tests.fakes import FakeNotifier, FakePublisher


def make_context(gateway_config=None) -> IngestionContext:
  return IngestionContext(
    settings=Settings(_env_file=None, markets={}),
    publisher=FakePublisher(),
    notifier=FakeNotifier(),
    gateway_config=gateway_config
    or Mt5Settings(_env_file=None, ENABLE=True, SYMBOLS=["XAUUSD"]),
  )


@pytest.fixture
def context() -> IngestionContext:
  return make_context()


def test_create_uses_registered_builder(context):
  factory = IngestionFactory()
  sentinel = object()
  factory.register(GatewayEnum.MT5, lambda ctx: (sentinel, ctx))
  assert factory.create(GatewayEnum.MT5, context) == (sentinel, context)
  assert factory.registered == [GatewayEnum.MT5]


def test_unknown_gateway_raises(context):
  factory = IngestionFactory()
  factory.register(GatewayEnum.MT5, lambda ctx: None)
  with pytest.raises(GatewayNotRegisteredError, match="binance.*registered: mt5"):
    factory.create(GatewayEnum.BINANCE, context)


def test_duplicate_registration_is_rejected():
  factory = IngestionFactory()
  factory.register(GatewayEnum.MT5, lambda ctx: None)
  with pytest.raises(ValueError):
    factory.register(GatewayEnum.MT5, lambda ctx: None)


def test_context_narrows_the_gateway_config(context):
  config = context.config_as(Mt5Settings)
  assert config.SYMBOLS == ["XAUUSD"]
  assert config.MARKET is MarketEnum.FOREX


def test_context_rejects_the_wrong_config_class(context):
  # The settings registry and the builder disagreeing is a wiring bug; it
  # surfaces here instead of as an AttributeError on the first poll.
  with pytest.raises(TypeError, match="BinanceSettings"):
    context.config_as(BinanceSettings)


def test_context_hands_builders_the_shared_dependencies(context):
  # Pinned to the constructor it feeds: every argument BaseIngestion requires
  # besides the gateway's own config, and nothing it would refuse.
  parameters = inspect.signature(BaseIngestion.__init__).parameters
  required = {
    name
    for name, parameter in parameters.items()
    if name not in ("self", "config") and parameter.default is parameter.empty
  }
  dependencies = context.ingestion_dependencies()
  assert set(dependencies) == required
  assert dependencies["publisher"] is context.publisher
  assert dependencies["notifier"] is context.notifier
  assert dependencies["instance_id"] == context.settings.app.instance_id


def test_the_same_builder_serves_every_market():
  # One [binance] table per market file, one registration.
  factory = IngestionFactory()
  factory.register(GatewayEnum.BINANCE, lambda ctx: ctx.gateway_config.MARKET)
  for market in (MarketEnum.CRYPTO, MarketEnum.FOREX):
    context = make_context(BinanceSettings(_env_file=None, MARKET=market))
    assert factory.create(GatewayEnum.BINANCE, context) is market


def test_default_factory_registers_every_gateway():
  from ingester.providers import make_ingestion_factory

  registered = make_ingestion_factory().registered
  assert GatewayEnum.MT5 in registered
  assert GatewayEnum.BINANCE in registered


def test_registered_binance_builder_wires_the_real_gateway():
  # Only the Binance half: the real MT5 terminal adapter imports a package
  # that exists on Windows alone, and this suite runs anywhere.
  from ingester.gateways.crypto.binance import BinanceIngestion
  from ingester.providers import make_ingestion_factory

  config = BinanceSettings(_env_file=None, ENABLE=True, SYMBOLS=["BTCUSDT"])
  ingestion = make_ingestion_factory().create(GatewayEnum.BINANCE, make_context(config))
  # Built, never started: no socket is opened.
  assert isinstance(ingestion, BinanceIngestion)
  assert ingestion.snapshot()["market"] == "crypto"
  assert ingestion.symbols == ["BTCUSDT"]


def test_registered_mt5_builder_wires_the_real_gateway(monkeypatch):
  # The real adapter imports a Windows-only package, so it is swapped for the
  # fake; the builder's own wiring is what runs.
  from ingester.gateways.forex import mt5
  from ingester.providers import make_ingestion_factory
  from tests.fakes import FakeTerminal

  monkeypatch.setattr(mt5, "MetaTrader5Terminal", FakeTerminal)
  ingestion = make_ingestion_factory().create(GatewayEnum.MT5, make_context())
  # Built, never started: no terminal session is opened.
  assert isinstance(ingestion, mt5.Mt5Ingestion)
  assert ingestion.snapshot()["market"] == "forex"
  assert ingestion.symbols == ["XAUUSD"]
