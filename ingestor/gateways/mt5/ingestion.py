"""
ingestor/gateways/mt5/ingestion.py — MT5 bar-close detection (business logic only).

The MetaTrader5 package has no event callbacks, so a "bar closed" event is
derived by polling. For every (symbol, timeframe) the watcher thread reads the
newest *completed* bars — position 1 onwards, since position 0 is the bar still
forming — and emits every bar whose open time is later than the last one it
emitted.

* **Start-up**: the first read only records the latest closed bar; nothing is
  emitted, so a restart never re-publishes history.
* **Catch-up**: each read fetches ``MT5_CATCHUP_BARS`` bars, so bars that closed
  while the terminal was disconnected are still published, oldest first.
* **Timing**: MT5 opens a new bar on its first tick, so a close is seen on the
  first tick of the next bar plus up to one ``MT5_POLL_INTERVAL_SECONDS``.

Everything else — the thread, reconnects, publishing, notifications — is the
core's job (:class:`~ingestor.core.ingestion.ThreadedIngestion`).
"""

from __future__ import annotations

from typing import Any, ClassVar

from ingestor.core.errors import GatewayConnectionError
from ingestor.core.ingestion import ThreadedIngestion
from ingestor.gateways.mt5.dto import Mt5RateDTO
from ingestor.gateways.mt5.terminal import Mt5Terminal
from ingestor.logger import get_logger
from ingestor.schemas.enums import GatewayEnum, Timeframe
from ingestor.settings import Mt5Settings

log = get_logger(__name__)

StreamKey = tuple[str, Timeframe]


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
    # Server-clock epoch of the newest bar already emitted, per stream. Kept
    # across reconnects so the catch-up read knows where it left off.
    self._last_open: dict[StreamKey, int] = {}
    # Streams currently returning no data — warned about once, not every poll.
    self._silent: set[StreamKey] = set()

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

    for symbol in self.symbols:
      if not self._terminal.symbol_select(symbol, True):
        code, message = self._terminal.last_error()
        log.warning("MT5 cannot select %s [%s] %s", symbol, code, message)

    server = self._terminal.server_name()
    self._set_venue(server)
    log.info("MT5 connected (server=%s)", server or "?")

  def poll(self) -> None:
    if not self._terminal.is_connected():
      raise GatewayConnectionError("MT5 terminal is not connected to its trade server")
    for symbol in self.symbols:
      for timeframe in self.timeframes:
        self._poll_stream(symbol, timeframe)

  def disconnect(self) -> None:
    self._terminal.shutdown()

  # ── Business logic ────────────────────────────────────────────────

  def _poll_stream(self, symbol: str, timeframe: Timeframe) -> None:
    key = (symbol, timeframe)
    records = self._terminal.copy_rates_from_pos(
      symbol, timeframe, 1, self._mt5_config.CATCHUP_BARS
    )
    if not records:
      if key not in self._silent:
        self._silent.add(key)
        code, message = self._terminal.last_error()
        log.warning(
          "MT5 returned no bars for %s %s [%s] %s",
          symbol,
          timeframe.value,
          code,
          message,
        )
      return
    self._silent.discard(key)

    rates = sorted(
      (Mt5RateDTO.from_record(r, self._mt5_config.SERVER_TIMEZONE) for r in records),
      key=lambda rate: rate.time,
    )

    last = self._last_open.get(key)
    if last is None:
      self._last_open[key] = rates[-1].time
      log.info(
        "MT5 %s %s primed at bar %s",
        symbol,
        timeframe.value,
        rates[-1].to_bar(timeframe).open_time.isoformat(),
      )
      return

    for rate in rates:
      if rate.time > last:
        self.emit_bar(symbol, timeframe, rate.to_bar(timeframe))
        last = rate.time
    self._last_open[key] = last
