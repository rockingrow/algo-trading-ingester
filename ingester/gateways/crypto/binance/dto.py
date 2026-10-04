"""
ingester/gateways/crypto/binance/dto.py — Binance kline → canonical :class:`Bar`.

The one place that knows what a Binance kline looks like. A combined-stream
frame nests the event under ``data``, and the kline under ``data.k``, with
single-letter keys fixed by Binance's contract:

=========  ====================================================
``t``      bar **open** time, milliseconds since epoch (UTC)
``T``      bar close time, ms — ``t + interval - 1ms``
``s``      symbol, upper case (``BTCUSDT``)
``i``      interval in Binance's spelling (``15m``)
``o`` …    OHLC prices, as decimal **strings**
``v``      base-asset volume (``1.5`` BTC)
``q``      quote-asset volume (``98000.0`` USDT)
``n``      number of trades in the bar
``x``      true on the final update of a bar — the close
=========  ====================================================

The REST klines endpoint carries the same bar as a positional array instead of
a keyed object — same fields, same units, no ``x`` flag — and
:meth:`BinanceKlineDTO.from_rest_row` is the one place that knows which index
holds what.

Binance already stamps milliseconds in UTC, so there is no timezone to guess
(unlike MT5). The canonical ``close_time`` is derived as
``open_time + timeframe`` rather than taken from ``T``: Binance's ``T`` is one
millisecond short of the next bar's open, and a subscriber comparing bars across
venues should not have to know that.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from pydantic import ConfigDict, Field

from ingester.core.dto import BaseBarDTO
from ingester.schemas.enums import Timeframe
from ingester.schemas.market_event_schema import Bar

#: Canonical timeframe → Binance's own interval spelling, and back. Binance has
#: no W1-aligned-to-Monday mismatch to worry about: both start the week on
#: Monday 00:00 UTC.
TIMEFRAME_INTERVALS: dict[Timeframe, str] = {
  Timeframe.M1: "1m",
  Timeframe.M5: "5m",
  Timeframe.M15: "15m",
  Timeframe.M30: "30m",
  Timeframe.H1: "1h",
  Timeframe.H4: "4h",
  Timeframe.D1: "1d",
  Timeframe.W1: "1w",
}

INTERVAL_TIMEFRAMES: dict[str, Timeframe] = {
  interval: timeframe for timeframe, interval in TIMEFRAME_INTERVALS.items()
}


class BinanceKlineDTO(BaseBarDTO):
  """One validated kline from a ``<symbol>@kline_<interval>`` stream.

  Field names are ours; the aliases are Binance's. Prices arrive as strings and
  are coerced by pydantic — Binance sends decimals as text to keep them exact
  on the wire, and a float is what the canonical ``Bar`` carries.
  """

  # On top of the base config: fields can be set by our names as well as by
  # Binance's single-letter aliases.
  model_config = ConfigDict(populate_by_name=True)

  open_time_ms: int = Field(alias="t", ge=0)
  close_time_ms: int = Field(alias="T", ge=0)
  symbol: str = Field(alias="s")
  interval: str = Field(alias="i")
  open: float = Field(alias="o")
  high: float = Field(alias="h")
  low: float = Field(alias="l")
  close: float = Field(alias="c")
  #: Base-asset volume: 1.5 means 1.5 BTC on BTCUSDT.
  volume: float = Field(alias="v", ge=0)
  #: Quote-asset volume: the same bar in USDT.
  quote_volume: float = Field(alias="q", ge=0)
  trade_count: int = Field(alias="n", ge=0)
  #: ``true`` only on the final update of a bar. Anything else is a bar still
  #: forming, and publishing it would hand strategies an unfinished candle.
  is_closed: bool = Field(alias="x", default=False)

  @classmethod
  def from_rest_row(
    cls, row: Sequence[Any], *, symbol: str, interval: str, now_ms: int
  ) -> BinanceKlineDTO:
    """One kline from the REST endpoint, whose rows are positional arrays.

    The symbol and interval are not in the row — they were the request — and
    neither is ``x``: REST returns the bar still forming like any other. It is
    recognised by its close time, which Binance sets to the bar's last
    millisecond, so a bar is closed exactly once *now* is past it.
    """
    if len(row) < 9:
      raise ValueError(f"Binance kline row has {len(row)} field(s), expected 9+")
    close_time_ms = int(row[6])
    return cls(
      open_time_ms=int(row[0]),
      close_time_ms=close_time_ms,
      symbol=symbol,
      interval=interval,
      open=row[1],
      high=row[2],
      low=row[3],
      close=row[4],
      volume=row[5],
      quote_volume=row[7],
      trade_count=int(row[8]),
      is_closed=close_time_ms < now_ms,
    )

  @property
  def timeframe(self) -> Timeframe | None:
    """Canonical timeframe, or ``None`` for an interval we do not speak."""
    return INTERVAL_TIMEFRAMES.get(self.interval)

  def to_bar(self, timeframe: Timeframe) -> Bar:
    open_time = datetime.fromtimestamp(self.open_time_ms / 1000, tz=UTC)
    return Bar(
      open_time=open_time,
      # Not Binance's ``T``: that is one millisecond before the next bar opens,
      # and every other gateway reports the next bar's open time.
      close_time=open_time + timeframe.delta,
      open=self.open,
      high=self.high,
      low=self.low,
      close=self.close,
      volume=self.volume,
      # Crypto has real trades, not ticks — the count means the same thing to a
      # subscriber: how much activity formed the bar.
      tick_count=self.trade_count,
      quote_volume=self.quote_volume,
      # A central-limit order book has no broker spread to report.
      spread=None,
    )


def stream_name(symbol: str, timeframe: Timeframe) -> str:
  """``BTCUSDT`` + ``M15`` → ``btcusdt@kline_15m`` (Binance wants lower case)."""
  return f"{symbol.lower()}@kline_{TIMEFRAME_INTERVALS[timeframe]}"
