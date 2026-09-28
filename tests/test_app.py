import pytest
from fastapi.testclient import TestClient

from ingestor.app import create_app
from ingestor.core import IngestionFactory
from ingestor.gateways.mt5 import Mt5Ingestion
from ingestor.runtime import IngestorRuntime
from ingestor.schemas import GatewayEnum, Timeframe
from ingestor.settings import AppSettings, Mt5Settings, Settings
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

  def runtime_factory(config: Settings) -> IngestorRuntime:
    factory = IngestionFactory()
    factory.register(GatewayEnum.MT5, build)
    return IngestorRuntime(
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
  assert any("Ingestor Running" in m for m in notifier.messages)
  assert "Ingestor Stopped" in notifier.messages[-1]


def test_startup_failure_is_notified_and_cleaned_up():
  connection, notifier = FakeConnection(fail=True), FakeNotifier()
  app = create_app(make_settings(), make_runtime_factory(connection, notifier))

  with pytest.raises(ConnectionError):
    with TestClient(app):
      pass

  assert any("Failed To Start" in m for m in notifier.messages)
  assert notifier.stopped
