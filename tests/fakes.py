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
