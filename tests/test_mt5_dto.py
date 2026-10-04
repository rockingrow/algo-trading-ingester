from datetime import UTC, datetime, timedelta

from ingester.gateways.forex.mt5.dto import Mt5RateDTO, server_time_to_utc
from ingester.schemas import Timeframe
from tests.fakes import rate

# 2026-07-01 12:00 written on the server's wall clock, stored "as if UTC".
SERVER_NOON = int(datetime(2026, 7, 1, 12, 0, tzinfo=UTC).timestamp())


def test_server_time_utc_is_passthrough():
  assert server_time_to_utc(SERVER_NOON, "UTC") == datetime(
    2026, 7, 1, 12, 0, tzinfo=UTC
  )


def test_server_time_is_shifted_by_server_zone_with_dst():
  # Europe/Athens is UTC+3 in July (the usual "GMT+3 summer" MT5 server).
  assert server_time_to_utc(SERVER_NOON, "Europe/Athens") == datetime(
    2026, 7, 1, 9, 0, tzinfo=UTC
  )


def test_dto_maps_to_canonical_bar():
  dto = Mt5RateDTO.from_record(rate(SERVER_NOON, price=2000.0), "Europe/Athens")
  bar = dto.to_bar(Timeframe.M15)
  assert bar.open_time == datetime(2026, 7, 1, 9, 0, tzinfo=UTC)
  assert bar.close_time - bar.open_time == timedelta(minutes=15)
  assert (bar.open, bar.high, bar.low, bar.close) == (2000.0, 2000.5, 1999.5, 2000.25)
  # Forex has no real volume, so tick volume stands in for it.
  assert bar.volume == 42.0
  assert bar.tick_count == 42
  assert bar.spread == 12.0


def test_dto_prefers_real_volume_when_present():
  record = {**rate(SERVER_NOON), "real_volume": 1500}
  bar = Mt5RateDTO.from_record(record, "UTC").to_bar(Timeframe.M1)
  assert bar.volume == 1500.0
  assert bar.tick_count == 42


def test_dto_ignores_unknown_fields():
  record = {**rate(SERVER_NOON), "something_new": 1}
  assert Mt5RateDTO.from_record(record, "UTC").time == SERVER_NOON
