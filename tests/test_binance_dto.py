from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from ingester.gateways.binance import (
  INTERVAL_TIMEFRAMES,
  TIMEFRAME_INTERVALS,
  BinanceKlineDTO,
  stream_name,
)
from ingester.schemas import Timeframe
from tests.fakes import kline

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
