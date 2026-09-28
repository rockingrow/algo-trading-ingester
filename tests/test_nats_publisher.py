from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from ingester.schemas import (
  Bar,
  BarClosedEvent,
  EventSource,
  GatewayEnum,
  MarketEnum,
  Timeframe,
)
from ingester.services.nats_service import NatsPublisher
from ingester.settings import NatsSettings

OPEN = datetime(2026, 9, 28, 10, 0, tzinfo=UTC)


class Recorder:
  def __init__(self):
    self.calls = []

  async def publish(self, subject, data, **kwargs):
    self.calls.append((subject, data, kwargs))


def make_event(symbol="XAUUSD.m") -> BarClosedEvent:
  bar = Bar(
    open_time=OPEN,
    close_time=OPEN + timedelta(minutes=15),
    open=1,
    high=2,
    low=0.5,
    close=1.5,
  )
  source = EventSource(
    gateway=GatewayEnum.MT5, market=MarketEnum.FOREX, ingester_id="x"
  )
  return BarClosedEvent.create(
    source=source, symbol=symbol, timeframe=Timeframe.M15, bar=bar
  )


def make_publisher(**config):
  nc, js = Recorder(), Recorder()
  connection = SimpleNamespace(
    config=NatsSettings(_env_file=None, **config), is_connected=True, nc=nc, js=js
  )
  return NatsPublisher(connection), nc, js


async def test_core_publish_subject_and_body():
  publisher, nc, js = make_publisher(SUBJECT_PREFIX="INGEST")
  event = make_event()
  await publisher.publish(event)

  ((subject, data, kwargs),) = nc.calls
  assert subject == "INGEST.bar.closed.mt5.XAUUSD_m.M15"
  assert BarClosedEvent.model_validate_json(data) == event
  assert kwargs == {}
  assert js.calls == []
  assert publisher.subject_filter == "INGEST.>"


async def test_jetstream_publish_sets_msg_id():
  publisher, nc, js = make_publisher(JETSTREAM_ENABLED=True, PUBLISH_TIMEOUT=2.0)
  event = make_event("EURUSD")
  await publisher.publish(event)

  ((subject, _, kwargs),) = js.calls
  assert subject == "INGEST.bar.closed.mt5.EURUSD.M15"
  assert kwargs["headers"] == {"Nats-Msg-Id": event.event_id}
  assert kwargs["timeout"] == 2.0
  assert nc.calls == []
