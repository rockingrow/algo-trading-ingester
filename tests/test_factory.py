import pytest

from ingestor.core import GatewayNotRegisteredError, IngestionContext, IngestionFactory
from ingestor.schemas import GatewayEnum
from ingestor.settings import Settings
from tests.fakes import FakeNotifier, FakePublisher


@pytest.fixture
def context() -> IngestionContext:
  return IngestionContext(
    settings=Settings(_env_file=None),
    publisher=FakePublisher(),
    notifier=FakeNotifier(),
  )


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


def test_create_all_deduplicates_in_order(context):
  factory = IngestionFactory()
  factory.register(GatewayEnum.MT5, lambda ctx: "mt5")
  factory.register(GatewayEnum.BINANCE, lambda ctx: "binance")
  gateways = [GatewayEnum.BINANCE, GatewayEnum.MT5, GatewayEnum.BINANCE]
  assert factory.create_all(gateways, context) == ["binance", "mt5"]


def test_default_factory_registers_mt5():
  from ingestor.providers import make_ingestion_factory

  assert GatewayEnum.MT5 in make_ingestion_factory().registered
