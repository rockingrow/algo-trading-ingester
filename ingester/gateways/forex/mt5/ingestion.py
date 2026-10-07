"""
ingester/gateways/forex/mt5/ingestion.py — MT5 bar-close detection (business logic).

The MetaTrader5 package has no event callbacks, so a "bar closed" event is
derived by polling. For every (symbol, timeframe) the watcher thread reads the
newest *completed* bars — position 1 onwards, since position 0 is the bar still
forming — and emits every bar whose open time is later than the last one it
emitted.

* **Symbols**: the market file's ``[mt5]`` table names the bare instrument
  (``XAUUSD``) and :mod:`~ingester.gateways.forex.mt5.symbols` maps it onto whatever
  this broker calls it (Exness: ``XAUUSDm``). Only the MetaTrader5 calls use the
  broker's name — the published ``symbol`` stays the configured one.
* **Start-up**: the first read only records the latest closed bar; nothing is
  emitted, so a restart never re-publishes history. The bars a subscriber
  needs from before that are not this gateway's to push — it does not know who
  is listening or how long their indicator window is. They are asked for
  (:meth:`Mt5Ingestion._read_history`) and answered in one reply.
* **Gap recovery**: each read fetches ``recovery_bars`` bars, so bars that
  closed while the terminal was disconnected are still published, oldest
  first, once it reconnects.
* **History**: a request is served on this gateway's own thread, between two
  polls, because the MetaTrader5 package keeps thread-affine global state. It
  reads from position 1, like the poll — position 0 is still forming.
* **Timing**: MT5 opens a new bar on its first tick, so a close is seen on the
  first tick of the next bar plus up to one ``poll_interval_seconds``.

Everything else — the thread, reconnects, publishing, notifications — is the
core's job (:class:`~ingester.core.ingestion.ThreadedIngestion`).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any, ClassVar

from ingester.core.errors import (
  GatewayConnectionError,
  HistoryUnavailableError,
  SymbolResolutionError,
)
from ingester.core.ingestion import StreamKey, ThreadedIngestion
from ingester.gateways.forex.mt5.dto import Mt5RateDTO
from ingester.gateways.forex.mt5.symbols import resolve_symbol
from ingester.gateways.forex.mt5.terminal import Mt5Terminal, RateRecord
from ingester.logger import get_logger
from ingester.schemas.enums import GatewayEnum, Timeframe
from ingester.schemas.market_event_schema import Bar
from ingester.settings import Mt5Settings

log = get_logger(__name__)


class Mt5Ingestion(ThreadedIngestion):
  """Watches MT5 for closed bars on the configured symbols × timeframes."""

  gateway: ClassVar[GatewayEnum] = GatewayEnum.MT5

  def __init__(self, *, config: Mt5Settings, terminal: Mt5Terminal, **kwargs: Any):
    super().__init__(
      config=config,
      poll_interval=config.POLL_INTERVAL_SECONDS,
      reconnect_interval=config.RECONNECT_INTERVAL_SECONDS,
      **kwargs,
    )
    self._mt5_config = config
    self._terminal = terminal
    # The newest bar already emitted per stream is remembered by the core, as a
    # server-clock epoch; it survives reconnects, so the first read after one
    # knows where it left off.
    # Streams currently returning no data — warned about once, not every poll.
    self._silent: set[StreamKey] = set()
    # Configured symbol → the name this broker actually sells it under.
    # Rebuilt on every connect; only symbols in here are polled.
    self._broker_symbol: dict[str, str] = {}

  # ── ThreadedIngestion hooks ───────────────────────────────────────

  def connect(self) -> None:
    ok = self._terminal.initialize(
      path=self._mt5_config.TERMINAL_PATH or None,
      login=self._mt5_config.LOGIN,
      password=self._mt5_config.PASSWORD or None,
      server=self._mt5_config.SERVER or None,
      timeout=self._mt5_config.TIMEOUT_MS,
    )
    if not ok:
      code, message = self._terminal.last_error()
      raise GatewayConnectionError(f"MT5 initialize failed [{code}] {message}")

    self._resolve_symbols()

    server = self._terminal.server_name()
    self._set_venue(server)
    log.info("MT5 connected (server=%s)", server or "?")

  def poll(self) -> None:
    if not self._terminal.is_connected():
      raise GatewayConnectionError("MT5 terminal is not connected to its trade server")
    for symbol, broker_symbol in self._broker_symbol.items():
      for timeframe in self.timeframes:
        try:
          self._poll_stream(symbol, broker_symbol, timeframe)
        except GatewayConnectionError:
          raise
        except Exception:
          # One malformed record must not starve the other streams of this
          # cycle; the next poll retries this one.
          log.exception("MT5 %s %s poll failed", symbol, timeframe.value)

  def disconnect(self) -> None:
    self._terminal.shutdown()

  # ── Symbol resolution ─────────────────────────────────────────────

  def _resolve_symbols(self) -> None:
    """Map each configured symbol onto the broker's name, then select it.

    Re-run on every connect: a broker can withdraw or rename an instrument
    while we are down, and a symbol that failed to resolve deserves another go
    after a reconnect.
    """
    resolved: dict[str, str] = {}
    for symbol in self.symbols:
      broker_symbol = self._resolve_one(symbol)
      if broker_symbol is None:
        continue
      if not self._terminal.symbol_select(broker_symbol, True):
        code, message = self._terminal.last_error()
        log.warning("MT5 cannot select %s [%s] %s", broker_symbol, code, message)
      resolved[symbol] = broker_symbol
    if not resolved and self.symbols:
      log.error(
        "MT5 resolved none of the configured symbols (%s) — nothing to poll",
        ",".join(self.symbols),
      )
    self._broker_symbol = resolved

  def _resolve_one(self, symbol: str) -> str | None:
    try:
      candidates = self._terminal.symbol_names(f"*{symbol}*")
    except Exception as exc:
      # A catalogue read must never stop the gateway — the name as configured
      # is still the best guess.
      log.warning(
        "MT5 symbol lookup for %s failed (%s), using the name verbatim",
        symbol,
        exc,
      )
      return symbol
    try:
      broker_symbol = resolve_symbol(symbol, candidates, self._mt5_config.SYMBOL_SUFFIX)
    except SymbolResolutionError as exc:
      log.error("MT5 cannot resolve symbol %s: %s", symbol, exc)
      return None
    if broker_symbol != symbol:
      log.info("MT5 resolved %s → %s", symbol, broker_symbol)
    return broker_symbol

  # ── Business logic ────────────────────────────────────────────────

  def _poll_stream(self, symbol: str, broker_symbol: str, timeframe: Timeframe) -> None:
    key = (symbol, timeframe)
    records = self._terminal.copy_rates_from_pos(
      broker_symbol, timeframe, 1, self._mt5_config.RECOVERY_BARS
    )
    if not records:
      if key not in self._silent:
        self._silent.add(key)
        code, message = self._terminal.last_error()
        log.warning(
          "MT5 returned no bars for %s (%s) %s [%s] %s",
          symbol,
          broker_symbol,
          timeframe.value,
          code,
          message,
        )
      return
    self._silent.discard(key)

    log.debug("MT5 raw %s %s: %s", symbol, timeframe.value, records)

    rates = self._rates(records)

    if self._open_mark(symbol, timeframe) is None:
      # Nothing is published from the first read. Whatever closed before this
      # process started is history, and history is the subscriber's to ask for.
      self._remember_bar(symbol, timeframe, rates[-1].time)
      log.info(
        "MT5 %s %s primed at bar %s — publishing from the next close",
        symbol,
        timeframe.value,
        rates[-1].to_bar(timeframe).open_time.isoformat(),
      )
      return

    for rate in rates:
      if self._is_new_bar(symbol, timeframe, rate.time):
        self._emit(symbol, timeframe, rate.time, rate.to_bar(timeframe))

  def _rates(self, records: Sequence[RateRecord]) -> list[Mt5RateDTO]:
    """Validated records, one per open time, oldest first."""
    by_time = {
      dto.time: dto
      for dto in (
        Mt5RateDTO.from_record(record, self._mt5_config.SERVER_TIMEZONE)
        for record in records
      )
    }
    return [by_time[open_mark] for open_mark in sorted(by_time)]

  # ── History, on request ───────────────────────────────────────────

  async def _read_history(
    self, symbol: str, timeframe: Timeframe, count: int
  ) -> list[Bar]:
    return await self._call_on_thread(
      lambda: self._read_closed_bars(symbol, timeframe, count)
    )

  def _read_closed_bars(
    self, symbol: str, timeframe: Timeframe, count: int
  ) -> list[Bar]:
    """The newest *count* closed bars. Runs on the gateway thread only.

    Any timeframe MetaTrader 5 has may be asked for, not just the ones this
    gateway polls: what a subscriber warms a window on is its decision. The
    symbol does have to be configured, because that is what resolved the
    broker's name for it and put it in Market Watch.
    """
    broker_symbol = self._broker_symbol.get(symbol)
    if broker_symbol is None:
      raise HistoryUnavailableError(
        f"MT5 has not resolved {symbol} on this broker — see the connect log"
      )
    if not self._terminal.is_connected():
      raise HistoryUnavailableError("MT5 terminal is not connected to its trade server")
    records = self._terminal.copy_rates_from_pos(broker_symbol, timeframe, 1, count)
    if not records:
      code, message = self._terminal.last_error()
      raise HistoryUnavailableError(
        f"MT5 returned no bars for {symbol} ({broker_symbol}) {timeframe.value} "
        f"[{code}] {message}"
      )
    bars = [rate.to_bar(timeframe) for rate in self._rates(records)]
    log.info(
      "MT5 %s %s history: %d of %d bar(s) asked for, %s..%s",
      symbol,
      timeframe.value,
      len(bars),
      count,
      bars[0].open_time.isoformat(),
      bars[-1].open_time.isoformat(),
    )
    return bars

  def _emit(self, symbol: str, timeframe: Timeframe, open_mark: int, bar: Bar) -> None:
    """Hand one converted bar to the pipeline, then move the stream's mark."""
    if log.isEnabledFor(logging.DEBUG):
      # Guarded: model_dump_json() would otherwise run on every bar even when
      # DEBUG is off, because arguments are evaluated before the call.
      log.debug(
        "MT5 %s %s DTO bar (pre-publish): %s",
        symbol,
        timeframe.value,
        bar.model_dump_json(),
      )
    self.emit_bar(symbol, timeframe, bar)
    self._remember_bar(symbol, timeframe, open_mark)
