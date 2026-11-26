"""
ingester/gateways/crypto/binance/ingestion.py — Binance bar-close detection.

Binance pushes a kline update several times a second and flags the final one
with ``k.x == true``, so a closed bar needs no polling: the gateway opens one
combined websocket for every (symbol, timeframe), reads frames, and emits the
bars Binance marks closed.

* **Symbols**: the market file names them the way Binance spells them
  (``BTCUSDT``). The stream name is lower-cased for the subscription
  (``btcusdt@kline_15m``); the published ``symbol`` is the configured one, so
  ``event_id`` is stable.
* **One socket, every stream**: Binance's combined endpoint takes the whole
  subscription in the URL, so symbols × timeframes share one connection — and
  one reconnect.
* **Start-up**: the socket only carries bars that close while it is connected,
  so the bars that closed before this process started are not published. A
  subscriber that needs them asks (:meth:`BinanceIngestion._read_history`) and
  is answered from the REST klines endpoint, in one reply.
* **History**: a request for more bars than one klines call returns is read
  page by page, and refused rather than answered short — the reply has no way
  to say it was cut off, so a short window would read as the whole of the
  venue's history.
* **De-duplication**: a reconnect can replay the bar that closed while the
  socket was down, and Binance occasionally repeats a final update. The core
  remembers the newest open time already emitted per stream, so each bar is
  emitted once. (The publisher's ``event_id`` would also let JetStream drop it, but
  core NATS would not.)
* **Reconnects**: Binance closes a connection after 24 hours, and a silent
  socket is dropped by the stream's idle timeout. Either way the loop below
  dials again every ``reconnect_interval_seconds``. That idle timeout watches
  the socket, not each stream on it: one stream going quiet while the others
  deliver is not detected, which is the trade-off for sharing a connection.

Everything else — the publish queue, notifications, status — is the core's job
(:class:`~ingester.core.ingestion.AsyncStreamIngestion`), including the
connect → receive → reconnect loop itself.
"""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from typing import Any, ClassVar
from urllib.parse import urlsplit

from ingester.core.errors import GatewayConnectionError, HistoryUnavailableError
from ingester.core.ingestion import AsyncStreamIngestion
from ingester.gateways.crypto.binance.dto import (
  TIMEFRAME_INTERVALS,
  BinanceKlineDTO,
  stream_name,
)
from ingester.gateways.crypto.binance.history import KlineHistory
from ingester.gateways.crypto.binance.stream import KlineStream
from ingester.logger import get_logger
from ingester.schemas.enums import GatewayEnum, Timeframe
from ingester.schemas.market_event_schema import Bar, utcnow
from ingester.settings import BinanceSettings

log = get_logger(__name__)

#: Rows Binance returns for one klines request, at most.
KLINES_LIMIT = 1000


class BinanceIngestion(AsyncStreamIngestion):
  """Watches a Binance kline websocket for closed bars."""

  gateway: ClassVar[GatewayEnum] = GatewayEnum.BINANCE

  def __init__(
    self,
    *,
    config: BinanceSettings,
    stream: KlineStream,
    history: KlineHistory,
    **kwargs: Any,
  ):
    super().__init__(
      config=config,
      reconnect_interval=config.RECONNECT_INTERVAL_SECONDS,
      **kwargs,
    )
    self._binance_config = config
    self._stream = stream
    self._history = history
    #: Binance's upper-case symbol → the name written in the market file.
    self._configured_symbol: dict[str, str] = {
      symbol.upper(): symbol for symbol in config.SYMBOLS
    }
    #: Frames already complained about once — an unknown interval or symbol
    #: would otherwise log on every update.
    self._unknown: set[str] = set()

  # ── AsyncStreamIngestion hooks ────────────────────────────────────

  async def connect(self) -> None:
    # The endpoint's host is the venue-side origin a subscriber can trace back:
    # it tells spot from futures from testnet.
    self._set_venue(urlsplit(self._binance_config.WS_URL).hostname)
    streams = [
      stream_name(symbol, timeframe)
      for symbol in self.symbols
      for timeframe in self.timeframes
    ]
    await self._stream.open(self._binance_config.WS_URL, streams)

  async def receive(self) -> Mapping[str, Any]:
    return await self._stream.receive()

  async def disconnect(self) -> None:
    await self._stream.close()

  # ── Business logic ────────────────────────────────────────────────

  def handle(self, frame: Mapping[str, Any]) -> None:
    """Emit the bar a frame carries, if it carries a closed one."""
    if "error" in frame:
      raise GatewayConnectionError(f"Binance reported an error: {frame['error']}")
    # A combined stream nests the event under "data"; a single stream does not.
    payload = frame.get("data", frame)
    if not isinstance(payload, Mapping) or payload.get("e") != "kline":
      # Subscription acknowledgements and other event types are not ours.
      return
    raw = payload.get("k")
    if not isinstance(raw, Mapping):
      raise ValueError("kline frame has no 'k' object")

    kline = BinanceKlineDTO.model_validate(raw)
    if not kline.is_closed:
      return

    timeframe = kline.timeframe
    if timeframe is None:
      self._warn_once(
        f"interval:{kline.interval}",
        "Binance sent interval %s, which maps to no canonical timeframe",
        kline.interval,
      )
      return
    symbol = self._configured_symbol.get(kline.symbol.upper())
    if symbol is None:
      self._warn_once(
        f"symbol:{kline.symbol}",
        "Binance sent symbol %s, which is not configured for this gateway",
        kline.symbol,
      )
      return

    if not self._is_new_bar(symbol, timeframe, kline.open_time_ms):
      log.debug("Binance %s %s repeated bar, skipped", symbol, timeframe.value)
      return
    # The bar is built and handed over *before* the stream's mark moves. The
    # other order loses the bar for good when ``to_bar`` refuses the candle —
    # an impossible high, say: the mark would already have passed it, so the
    # corrected update Binance sends next reads as a bar we have had.
    bar = kline.to_bar(timeframe)
    self.emit_bar(symbol, timeframe, bar)
    self._remember_bar(symbol, timeframe, kline.open_time_ms)

  # ── History, on request ───────────────────────────────────────────

  async def _read_history(
    self, symbol: str, timeframe: Timeframe, count: int
  ) -> list[Bar]:
    """The newest *count* closed bars, from the REST klines endpoint.

    Any timeframe Binance has may be asked for, not just the ones on the
    socket. One klines request returns at most :data:`KLINES_LIMIT` rows, so a
    larger *count* is read page by page, newest first, each page ending just
    before the oldest row already held. Reading stops at *count*, or once a
    page comes back short: the venue has no older bars, which is not
    truncation and needs no saying.

    A read that cannot be finished is **refused**, never answered short: the
    reply carries no way to say "cut off", so fewer bars than asked would read
    as "this is all the venue has" and leave a strategy on a window it thinks
    is whole. Both ways out of a page loop are refusals — the pages taking
    longer than ``history_budget_seconds``, and a page that repeats bars
    already held, which is a venue ignoring ``endTime``.
    """
    interval = TIMEFRAME_INTERVALS[timeframe]
    deadline = time.monotonic() + self._binance_config.HISTORY_BUDGET_SECONDS
    # One more than asked for on the first page: its newest row is the bar
    # still forming, which ``_closed_klines`` drops.
    limit = min(count + 1, KLINES_LIMIT)
    rows = await self._klines(symbol, interval, limit)
    # Closed klines by open time, which also collapses a row Binance repeats.
    closed = self._closed_klines(rows, symbol=symbol, interval=interval)
    while len(closed) < count and len(rows) >= limit:
      if time.monotonic() >= deadline:
        raise HistoryUnavailableError(
          f"Binance {symbol} {timeframe.value}: reading {count} bar(s) was still "
          f"unfinished after {self._binance_config.HISTORY_BUDGET_SECONDS:.0f}s "
          f"({len(closed)} read) — ask for fewer"
        )
      oldest_ms = min(int(row[0]) for row in rows)
      limit = min(count - len(closed), KLINES_LIMIT)
      rows = await self._klines(symbol, interval, limit, end_time_ms=oldest_ms - 1)
      page = self._closed_klines(rows, symbol=symbol, interval=interval)
      new = {open_ms: kline for open_ms, kline in page.items() if open_ms not in closed}
      if not new and len(rows) >= limit:
        raise HistoryUnavailableError(
          f"Binance {symbol} {timeframe.value}: the page before {oldest_ms} came "
          f"back with bars already read, so the window cannot be filled "
          f"({len(closed)} of {count} bar(s))"
        )
      closed.update(new)
    # Trimmed from the newest end: a venue that hands back more rows than asked
    # must not widen the window the subscriber asked for.
    bars = [closed[open_ms].to_bar(timeframe) for open_ms in sorted(closed)[-count:]]
    if not bars:
      raise HistoryUnavailableError(
        f"Binance returned no closed bar for {symbol} {timeframe.value}"
      )
    log.info(
      "Binance %s %s history: %d of %d bar(s) asked for, %s..%s",
      symbol,
      timeframe.value,
      len(bars),
      count,
      bars[0].open_time.isoformat(),
      bars[-1].open_time.isoformat(),
    )
    return bars

  async def _klines(
    self, symbol: str, interval: str, limit: int, *, end_time_ms: int | None = None
  ) -> Sequence[Sequence[Any]]:
    try:
      return await self._history.klines(
        symbol=symbol, interval=interval, limit=limit, end_time_ms=end_time_ms
      )
    except GatewayConnectionError as exc:
      raise HistoryUnavailableError(str(exc)) from exc

  @staticmethod
  def _closed_klines(
    rows: Sequence[Sequence[Any]], *, symbol: str, interval: str
  ) -> dict[int, BinanceKlineDTO]:
    """The closed klines among *rows*, by open time.

    Keyed by open time, so a row Binance repeats collapses into one bar. REST
    returns the bar still forming like any other and carries no ``x`` flag, so
    a row is closed when either test says so: it is **not** the newest row of
    the page — the one place Binance puts a forming bar — or its close time is
    far enough past this host's clock to be sure of
    (:data:`~ingester.gateways.crypto.binance.dto.CLOSED_BAR_TOLERANCE_MS`).

    Neither test alone is enough. The clock is the only thing that can tell a
    page whose newest row *is* closed — an older page, or a symbol whose last
    bar is long past — and it drifts, so it is never what rules a bar closed
    while the bar before it would do. The position is exact but says nothing
    about the rows behind it, which are older than a forming bar and closed
    whatever the clock reads.
    """
    now_ms = int(utcnow().timestamp() * 1000)
    by_open = {
      kline.open_time_ms: kline
      for kline in (
        BinanceKlineDTO.from_rest_row(
          row, symbol=symbol, interval=interval, now_ms=now_ms
        )
        for row in rows
      )
    }
    newest_ms = max(by_open, default=None)
    return {
      open_ms: kline
      for open_ms, kline in by_open.items()
      if open_ms != newest_ms or kline.is_closed
    }

  def _warn_once(self, key: str, message: str, *args: Any) -> None:
    if key in self._unknown:
      return
    self._unknown.add(key)
    log.warning(message, *args)
