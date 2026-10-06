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


def test_bar_closed_defaults_to_a_live_bar():
  event = BarClosedEvent.create(
    source=SOURCE, symbol="XAUUSD", timeframe=Timeframe.M15, bar=make_bar()
  )
  assert event.warmup_bar is False
  assert event.warmup_index is None
  assert event.warmup_total is None


def make_warmup(**overrides) -> BarClosedEvent:
  fields = dict(warmup_bar=True, warmup_index=150, warmup_total=150)
  fields.update(overrides)
  return BarClosedEvent.create(
    source=SOURCE, symbol="XAUUSD", timeframe=Timeframe.M15, bar=make_bar(), **fields
  )


def test_warmup_fields_do_not_change_the_event_id():
  # The same bar can arrive live from one process and as warm-up from the next —
  # and as a different position in the next window again. JetStream
  # de-duplicates on event_id, so none of the three may reach it.
  live = BarClosedEvent.create(
    source=SOURCE, symbol="XAUUSD", timeframe=Timeframe.M15, bar=make_bar()
  )
  last = make_warmup()
  first = make_warmup(warmup_index=1)
  assert last.warmup_bar is True
  assert (last.warmup_index, last.warmup_total) == (150, 150)
  assert last.event_id == first.event_id == live.event_id


def test_a_warmup_bar_must_be_numbered():
  # Half a series leaves a subscriber ending its warm-up on
  # warmup_index == warmup_total waiting for a message that never comes.
  with pytest.raises(ValidationError, match="both warmup_index and warmup_total"):
    make_warmup(warmup_total=None)
  with pytest.raises(ValidationError, match="both warmup_index and warmup_total"):
    make_warmup(warmup_index=None)


def test_a_live_bar_must_not_be_numbered():
  with pytest.raises(ValidationError, match="belong to a warmup bar only"):
    make_warmup(warmup_bar=False)


def test_warmup_index_cannot_run_past_the_window():
  with pytest.raises(ValidationError, match="warmup_index must be <= warmup_total"):
    make_warmup(warmup_index=151)


def test_warmup_index_is_one_based():
  with pytest.raises(ValidationError, match="greater than or equal to 1"):
    make_warmup(warmup_index=0)


def test_the_warmup_numbers_survive_the_json_round_trip():
  event = make_warmup(warmup_index=7, warmup_total=16)
  decoded = BarClosedEvent.model_validate_json(event.model_dump_json())
  assert (decoded.warmup_bar, decoded.warmup_index, decoded.warmup_total) == (
    True,
    7,
    16,
  )
  assert decoded == event


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
