import pytest

from ingester.core import GatewayNotRegisteredError, IngestionContext, IngestionFactory
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


def test_the_same_builder_serves_every_market():
  # One [binance] table per market file, one registration.
  factory = IngestionFactory()
  factory.register(GatewayEnum.BINANCE, lambda ctx: ctx.gateway_config.MARKET)
  for market in (MarketEnum.CRYPTO, MarketEnum.CFD):
    context = make_context(BinanceSettings(_env_file=None, MARKET=market))
    assert factory.create(GatewayEnum.BINANCE, context) is market


def test_default_factory_registers_every_gateway():
  from ingester.providers import make_ingestion_factory

  registered = make_ingestion_factory().registered
  assert GatewayEnum.MT5 in registered
  assert GatewayEnum.BINANCE in registered
