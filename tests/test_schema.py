from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from ingester.schemas import (
  Bar,
  BarClosedEvent,
  EventSource,
  GatewayEnum,
  MarketEnum,
  Timeframe,
)
from ingester.settings import settings

OPEN = datetime(2026, 9, 28, 10, 0, tzinfo=UTC)


def make_bar(**overrides) -> Bar:
  fields = dict(
    open_time=OPEN,
    close_time=OPEN + timedelta(minutes=15),
    open=1.0,
    high=2.0,
    low=0.5,
    close=1.5,
    volume=10,
  )
  fields.update(overrides)
  return Bar(**fields)


SOURCE = EventSource(
  gateway=GatewayEnum.MT5, market=MarketEnum.FOREX, ingester_id="vps-1"
)


def test_bar_rejects_naive_datetime():
  with pytest.raises(ValidationError, match="timezone-aware"):
    make_bar(open_time=datetime(2026, 9, 28, 10, 0))


@pytest.mark.parametrize("overrides", [{"high": 1.2}, {"low": 1.2}, {"volume": -1}])
def test_bar_rejects_inconsistent_ohlc(overrides):
  with pytest.raises(ValidationError):
    make_bar(**overrides)


def test_bar_rejects_close_before_open():
  with pytest.raises(ValidationError, match="close_time"):
    make_bar(close_time=OPEN)


def test_bar_normalises_to_utc():
  from zoneinfo import ZoneInfo

  local = OPEN.astimezone(ZoneInfo("Asia/Ho_Chi_Minh"))
  bar = make_bar(open_time=local)
  assert bar.open_time.utcoffset() == timedelta(0)
  assert bar.open_time == OPEN


def test_bar_closed_event_id_is_deterministic():
  one = BarClosedEvent.create(
    source=SOURCE, symbol="XAUUSD", timeframe=Timeframe.M15, bar=make_bar()
  )
  two = BarClosedEvent.create(
    source=SOURCE, symbol="XAUUSD", timeframe=Timeframe.M15, bar=make_bar()
  )
  assert one.event_id == two.event_id == f"mt5:XAUUSD:M15:{int(OPEN.timestamp())}"
  assert one.schema_version == settings.contract.VERSION


def test_a_bar_closed_event_carries_no_warmup_fields():
  # History is asked for, not replayed onto this subject, so nothing on the
  # wire has to say which of the two a message is.
  event = BarClosedEvent.create(
    source=SOURCE, symbol="XAUUSD", timeframe=Timeframe.M15, bar=make_bar()
  )
  assert not {"warmup_bar", "warmup_index", "warmup_total"} & set(
    event.model_dump(mode="json")
  )


def test_subject_tokens_sanitise_symbol_but_payload_keeps_it():
  event = BarClosedEvent.create(
    source=SOURCE, symbol="XAUUSD.m", timeframe=Timeframe.H1, bar=make_bar()
  )
  assert event.subject_tokens == ("bar", "closed", "mt5", "XAUUSD_m", "H1")
  assert event.symbol == "XAUUSD.m"


def test_event_round_trips_through_json():
  event = BarClosedEvent.create(
    source=SOURCE, symbol="EURUSD", timeframe=Timeframe.M1, bar=make_bar()
  )
  decoded = BarClosedEvent.model_validate_json(event.model_dump_json())
  assert decoded == event


def test_timeframe_seconds():
  assert Timeframe.M15.seconds == 900
  assert Timeframe.H4.delta == timedelta(hours=4)
