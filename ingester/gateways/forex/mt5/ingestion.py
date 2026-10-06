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
  emitted, so a restart never re-publishes history. With
  ``backfill_on_start`` it publishes that whole window instead, recovering
  the bars a crash skipped — flagged ``warmup_bar`` and numbered
  ``warmup_index`` of ``warmup_total``, oldest first, so a subscriber can warm
  indicators on them without acting on them and knows which one ends the
  window.
* **Gap recovery**: each read fetches ``warmup_bars`` bars, so bars that
  closed while the terminal was disconnected are still published, oldest
  first. They are late rather than warm-up, so they are not flagged.
* **Timing**: MT5 opens a new bar on its first tick, so a close is seen on the
  first tick of the next bar plus up to one ``poll_interval_seconds``.

Everything else — the thread, reconnects, publishing, notifications — is the
core's job (:class:`~ingester.core.ingestion.ThreadedIngestion`).
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from ingester.core.errors import GatewayConnectionError, SymbolResolutionError
from ingester.core.ingestion import StreamKey, ThreadedIngestion
from ingester.gateways.forex.mt5.dto import Mt5RateDTO
from ingester.gateways.forex.mt5.symbols import resolve_symbol
from ingester.gateways.forex.mt5.terminal import Mt5Terminal
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
    # server-clock epoch; it survives reconnects, so the warm-up read knows
    # where it left off.
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
      broker_symbol, timeframe, 1, self._mt5_config.WARMUP_BARS
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

    # One bar per open time, oldest first. A window that repeated a bar would
    # make the warm-up series below shorter than its own ``warmup_total``, and
    # the subscriber waits for that number.
    by_time = {
      dto.time: dto
      for dto in (
        Mt5RateDTO.from_record(r, self._mt5_config.SERVER_TIMEZONE) for r in records
      )
    }
    rates = [by_time[open_mark] for open_mark in sorted(by_time)]

    if self._open_mark(symbol, timeframe) is None:
      if not self._mt5_config.BACKFILL_ON_START:
        self._remember_bar(symbol, timeframe, rates[-1].time)
        log.info(
          "MT5 %s %s primed at bar %s",
          symbol,
          timeframe.value,
          rates[-1].to_bar(timeframe).open_time.isoformat(),
        )
        return
      # Nothing is remembered yet, so the whole window counts as unpublished
      # and the bars that closed while this process was down are recovered. A
      # bar the previous process already sent is republished — harmless,
      # because ``event_id`` is deterministic and JetStream drops it as a
      # duplicate.
      self._emit_warmup(symbol, timeframe, rates)
      return

    for rate in rates:
      if self._is_new_bar(symbol, timeframe, rate.time):
        self._emit(symbol, timeframe, rate.time, rate.to_bar(timeframe))

  def _emit_warmup(
    self, symbol: str, timeframe: Timeframe, rates: list[Mt5RateDTO]
  ) -> None:
    """Publish the start-up window as a numbered series, oldest first.

    ``warmup_total`` is the length of *this* window rather than the configured
    ``warmup_bars``: a broker with a thinner archive hands back fewer bars, and
    a subscriber that ends its warm-up on ``warmup_index == warmup_total``
    would otherwise wait for a bar MT5 never had.

    Called only for a stream nothing is remembered for yet, so every bar in the
    window is new and the numbering is contiguous by construction.
    """
    # The whole window is converted before the first bar is published: a record
    # the schema rejects half-way through would otherwise truncate a series
    # that has already announced its own length, and the mark it moved means
    # the next poll would not retry it — leaving a subscriber waiting for an
    # index that never arrives. Raising here publishes nothing and leaves the
    # mark untouched, so the next poll reads the window again.
    window = [(rate.time, rate.to_bar(timeframe)) for rate in rates]
    total = len(window)
    log.info(
      "MT5 %s %s backfilling %d closed bar(s) from %s",
      symbol,
      timeframe.value,
      total,
      window[0][1].open_time.isoformat(),
    )
    for index, (open_mark, bar) in enumerate(window, start=1):
      self._emit(
        symbol, timeframe, open_mark, bar, warmup_index=index, warmup_total=total
      )

  def _emit(
    self,
    symbol: str,
    timeframe: Timeframe,
    open_mark: int,
    bar: Bar,
    *,
    warmup_index: int | None = None,
    warmup_total: int | None = None,
  ) -> None:
    """Hand one converted bar to the pipeline, then move the stream's mark.

    A numbered call is a warm-up bar and an unnumbered one is live; the two
    cannot disagree because there is only one place that decides.
    """
    warmup_bar = warmup_index is not None
    if log.isEnabledFor(logging.DEBUG):
      # Guarded: model_dump_json() would otherwise run on every bar even when
      # DEBUG is off, because arguments are evaluated before the call.
      log.debug(
        "MT5 %s %s DTO bar (pre-publish): %s",
        symbol,
        timeframe.value,
        bar.model_dump_json(),
      )
    self.emit_bar(
      symbol,
      timeframe,
      bar,
      warmup_bar=warmup_bar,
      warmup_index=warmup_index,
      warmup_total=warmup_total,
    )
    self._remember_bar(symbol, timeframe, open_mark)
