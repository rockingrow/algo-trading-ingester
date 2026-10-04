from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from ingester.gateways.crypto.binance import (
  INTERVAL_TIMEFRAMES,
  TIMEFRAME_INTERVALS,
  BinanceKlineDTO,
  stream_name,
)
from ingester.schemas import Timeframe
from tests.fakes import kline, rest_kline

# 2026-09-28T10:00:00Z, a 15-minute boundary.
T0_MS = 1_790_589_600_000


def candle(**overrides) -> dict:
  raw = dict(kline(T0_MS, interval="15m")["data"]["k"])
  raw.update(overrides)
  return raw


def test_string_prices_become_a_canonical_bar():
  dto = BinanceKlineDTO.model_validate(candle())
  bar = dto.to_bar(Timeframe.M15)

  assert bar.open_time == datetime(2026, 9, 28, 10, 0, tzinfo=UTC)
  # Binance's own "T" is 10:14:59.999; the canonical close is the next open, so
  # a subscriber can chain bars without knowing that quirk.
  assert bar.close_time == datetime(2026, 9, 28, 10, 15, tzinfo=UTC)
  assert (bar.open, bar.high, bar.low, bar.close) == (100.0, 101.0, 99.0, 100.5)
  assert bar.volume == 1.5
  assert bar.quote_volume == 150.0
  assert bar.tick_count == 42
  # A central-limit order book quotes no broker spread.
  assert bar.spread is None


def test_only_the_final_update_is_a_closed_bar():
  assert BinanceKlineDTO.model_validate(candle(x=True)).is_closed
  assert not BinanceKlineDTO.model_validate(candle(x=False)).is_closed


def test_interval_maps_to_the_canonical_timeframe():
  assert BinanceKlineDTO.model_validate(candle(i="15m")).timeframe is Timeframe.M15
  # An interval Binance offers but the contract does not speak (3m, 1M …).
  assert BinanceKlineDTO.model_validate(candle(i="3m")).timeframe is None


def test_every_timeframe_round_trips():
  assert set(TIMEFRAME_INTERVALS) == set(Timeframe)
  for timeframe, interval in TIMEFRAME_INTERVALS.items():
    assert INTERVAL_TIMEFRAMES[interval] is timeframe


def test_stream_names_are_lower_case():
  assert stream_name("BTCUSDT", Timeframe.M15) == "btcusdt@kline_15m"
  assert stream_name("ethusdt", Timeframe.H4) == "ethusdt@kline_4h"


def test_an_impossible_candle_is_rejected():
  # high below the open: the canonical Bar refuses it rather than publishing a
  # candle no strategy can trust.
  dto = BinanceKlineDTO.model_validate(candle(h="1.0"))
  with pytest.raises(ValidationError, match="high"):
    dto.to_bar(Timeframe.M15)


def test_negative_volume_is_rejected():
  with pytest.raises(ValidationError):
    BinanceKlineDTO.model_validate(candle(v="-1"))


# ── REST rows (the backfill's payload) ──────────────────────────────


def test_a_rest_row_becomes_the_same_bar_as_a_websocket_kline():
  row = rest_kline(T0_MS, price=100.0, interval_ms=900_000)
  dto = BinanceKlineDTO.from_rest_row(
    row, symbol="BTCUSDT", interval="15m", now_ms=T0_MS + 900_000
  )
  assert dto.symbol == "BTCUSDT"
  assert dto.timeframe is Timeframe.M15
  bar = dto.to_bar(Timeframe.M15)
  assert bar.open_time == datetime(2026, 9, 28, 10, 0, tzinfo=UTC)
  # Derived from the timeframe, not Binance's "last millisecond of the bar".
  assert bar.close_time == datetime(2026, 9, 28, 10, 15, tzinfo=UTC)
  assert (bar.open, bar.high, bar.low, bar.close) == (100.0, 101.0, 99.0, 100.5)
  assert bar.volume == 1.5
  assert bar.quote_volume == 150.0
  assert bar.tick_count == 42
  assert bar.spread is None


def test_a_rest_row_is_closed_only_once_now_is_past_its_close_time():
  row = rest_kline(T0_MS, interval_ms=900_000)

  def closed_at(now_ms: int) -> bool:
    return BinanceKlineDTO.from_rest_row(
      row, symbol="BTCUSDT", interval="15m", now_ms=now_ms
    ).is_closed

  # REST carries no "x" flag, so the bar still forming is recognised by time.
  assert closed_at(T0_MS + 1) is False
  # Binance's close time is the bar's last millisecond.
  assert closed_at(T0_MS + 900_000 - 1) is False
  assert closed_at(T0_MS + 900_000) is True


def test_a_truncated_rest_row_is_rejected():
  with pytest.raises(ValueError, match="field"):
    BinanceKlineDTO.from_rest_row(
      [T0_MS, "1.0"], symbol="BTCUSDT", interval="15m", now_ms=T0_MS
    )


def test_an_impossible_rest_row_is_rejected():
  row = rest_kline(T0_MS)
  row[2] = "1.0"  # high below the open
  dto = BinanceKlineDTO.from_rest_row(
    row, symbol="BTCUSDT", interval="1m", now_ms=T0_MS + 60_000
  )
  with pytest.raises(ValidationError, match="high"):
    dto.to_bar(Timeframe.M1)
