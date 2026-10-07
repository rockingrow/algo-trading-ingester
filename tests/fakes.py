"""Test doubles for the ingester's interfaces."""

from __future__ import annotations

import asyncio
import threading
from fnmatch import fnmatchcase
from typing import Any

from ingester.schemas.enums import Timeframe
from ingester.schemas.market_event_schema import MarketEvent


class FakePublisher:
  def __init__(self) -> None:
    self.events: list[MarketEvent] = []
    self.is_connected = True
    self.subject_filter = "TEST.>"

  async def publish(self, event: MarketEvent) -> None:
    self.events.append(event)


class FakeNotifier:
  def __init__(self) -> None:
    self.messages: list[str] = []
    self.started = False
    self.stopped = False

  async def send_message(self, message_text: str) -> None:
    self.messages.append(message_text)

  async def start(self) -> None:
    self.started = True

  async def stop(self) -> None:
    self.stopped = True


class FakeConnection:
  def __init__(self, fail: bool = False) -> None:
    self.fail = fail
    self.connected = False
    self.closed = False

  async def connect(self) -> None:
    if self.fail:
      raise ConnectionError("nats down")
    self.connected = True

  async def close(self) -> None:
    self.closed = True


def kline(
  open_time_ms: int,
  *,
  symbol: str = "BTCUSDT",
  interval: str = "1m",
  price: float = 100.0,
  closed: bool = True,
  combined: bool = True,
) -> dict[str, Any]:
  """A Binance combined-stream frame carrying one kline update.

  Prices are strings, as Binance sends them. ``combined=False`` gives the bare
  event, the shape a single ``<symbol>@kline_<interval>`` stream delivers.
  """
  candle = {
    "t": open_time_ms,
    # Binance's close time is one millisecond before the next bar opens.
    "T": open_time_ms + 59_999,
    "s": symbol,
    "i": interval,
    "o": f"{price}",
    "c": f"{price + 0.5}",
    "h": f"{price + 1}",
    "l": f"{price - 1}",
    "v": "1.5",
    "q": "150.0",
    "n": 42,
    "x": closed,
  }
  event = {"e": "kline", "E": open_time_ms + 60_000, "s": symbol, "k": candle}
  if not combined:
    return event
  return {"stream": f"{symbol.lower()}@kline_{interval}", "data": event}


def rest_kline(
  open_time_ms: int, *, price: float = 100.0, interval_ms: int = 60_000
) -> list:
  """One row of Binance's REST klines response — positional, prices as strings.

  ``close_time`` is the bar's last millisecond, as Binance reports it.
  """
  return [
    open_time_ms,
    f"{price}",
    f"{price + 1}",
    f"{price - 1}",
    f"{price + 0.5}",
    "1.5",
    open_time_ms + interval_ms - 1,
    "150.0",
    42,
    "0",
    "0",
    "0",
  ]


class FakeKlineHistory:
  """Scriptable stand-in for Binance's REST klines endpoint.

  Rows are scripted per ``(symbol, interval)``; an unscripted stream answers
  with none, and :attr:`error` is raised instead of answering at all.
  """

  def __init__(self, rows: dict[tuple[str, str], list] | None = None) -> None:
    self.rows: dict[tuple[str, str], list] = dict(rows or {})
    self.calls: list[tuple[str, str, int]] = []
    #: Raised by every :meth:`klines` call while it is set.
    self.error: Exception | None = None

  def set_rows(self, symbol: str, interval: str, rows: list) -> None:
    self.rows[(symbol, interval)] = list(rows)

  async def klines(self, *, symbol: str, interval: str, limit: int) -> list:
    self.calls.append((symbol, interval, limit))
    if self.error is not None:
      raise self.error
    return list(self.rows.get((symbol, interval), []))[-limit:]


class FakeKlineStream:
  """Scriptable stand-in for a Binance websocket.

  Frames are queued up front or pushed while the ingestion runs; a queued
  ``Exception`` is raised out of :meth:`receive`, which is how a test drops the
  socket. An exhausted queue simply blocks, like a quiet connection.
  """

  def __init__(self, frames: list[Any] | None = None) -> None:
    self.queue: asyncio.Queue[Any] = asyncio.Queue()
    for frame in frames or []:
      self.queue.put_nowait(frame)
    self.opened: list[tuple[str, list[str]]] = []
    self.close_calls = 0
    #: Raised by the next :meth:`open` call, then cleared.
    self.open_error: Exception | None = None

  def push(self, frame: Any) -> None:
    self.queue.put_nowait(frame)

  async def open(self, url: str, streams: Any) -> None:
    self.opened.append((url, list(streams)))
    if self.open_error is not None:
      error, self.open_error = self.open_error, None
      raise error

  async def receive(self) -> Any:
    frame = await self.queue.get()
    if isinstance(frame, Exception):
      raise frame
    return frame

  async def close(self) -> None:
    self.close_calls += 1


def rate(time: int, price: float = 1.0) -> dict[str, Any]:
  """An MT5-shaped rate record."""
  return {
    "time": time,
    "open": price,
    "high": price + 0.5,
    "low": price - 0.5,
    "close": price + 0.25,
    "tick_volume": 42,
    "spread": 12,
    "real_volume": 0,
  }


class FakeTerminal:
  """Scriptable stand-in for MetaTrader5; thread-safe for the watcher thread."""

  def __init__(self) -> None:
    self._lock = threading.Lock()
    self.rates: dict[tuple[str, Timeframe], list[dict[str, Any]]] = {}
    #: Broker catalogue for symbol resolution. ``None`` = derive it from the
    #: scripted rates, so a test that does not care about affixes sees the
    #: symbol it configured.
    self.catalogue: list[str] | None = None
    self.connected = True
    self.initialize_ok = True
    self.initialize_calls = 0
    self.shutdown_calls = 0
    #: ``(thread name, count)`` per rates read — which thread touched the
    #: terminal, and how many bars it asked for.
    self.reads: list[tuple[str, int]] = []

  def set_rates(self, symbol: str, timeframe: Timeframe, records: list[dict]) -> None:
    with self._lock:
      self.rates[(symbol, timeframe)] = list(records)

  def initialize(self, **_: Any) -> bool:
    self.initialize_calls += 1
    return self.initialize_ok

  def shutdown(self) -> None:
    self.shutdown_calls += 1

  def last_error(self) -> tuple[int, str]:
    return (-1, "fake error")

  def is_connected(self) -> bool:
    return self.connected

  def server_name(self) -> str | None:
    return "Fake-Server"

  def symbol_names(self, pattern: str) -> list[str]:
    with self._lock:
      names = (
        list(self.catalogue)
        if self.catalogue is not None
        else sorted({symbol for symbol, _ in self.rates})
      )
    return [name for name in names if fnmatchcase(name, pattern)]

  def symbol_select(self, symbol: str, enable: bool = True) -> bool:
    return True

  def copy_rates_from_pos(
    self, symbol: str, timeframe: Timeframe, start_pos: int, count: int
  ) -> list[dict[str, Any]] | None:
    with self._lock:
      self.reads.append((threading.current_thread().name, count))
      records = self.rates.get((symbol, timeframe))
      if records is None:
        return None
      # MT5 returns oldest → newest; position 0 (the forming bar) is excluded
      # by the caller asking from start_pos=1, so the fake just returns tail.
      return records[-count:]


async def wait_for(predicate, timeout: float = 3.0, interval: float = 0.01) -> None:
  loop = asyncio.get_running_loop()
  deadline = loop.time() + timeout
  while not predicate():
    if loop.time() > deadline:
      raise AssertionError("condition not met in time")
    await asyncio.sleep(interval)
