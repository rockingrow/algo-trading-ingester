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
  so by default the bars that closed before this process started are simply
  not published. With ``backfill_on_start`` the gateway reads the last
  ``warmup_bars`` closed bars per stream from the REST klines endpoint once
  the socket is open, and publishes those first — so a subscriber gets
  backfill and live bars as one continuous series, the bars a crash skipped
  included. Those carry ``warmup_bar`` and are numbered ``warmup_index`` of
  ``warmup_total``, oldest first, so a subscriber knows which one ends the
  window; what the socket delivers carries neither.
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

import asyncio
from collections.abc import Mapping
from typing import Any, ClassVar
from urllib.parse import urlsplit

from ingester.core.errors import GatewayConnectionError
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
from ingester.schemas.market_event_schema import utcnow
from ingester.settings import BinanceSettings

log = get_logger(__name__)


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
    # After the socket, never before: a bar closing between the two would
    # otherwise fall into the gap. The overlap the other way round is free —
    # the core's de-duplication drops a backfilled bar the socket repeats.
    await self._backfill()

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
    self._remember_bar(symbol, timeframe, kline.open_time_ms)
    self.emit_bar(symbol, timeframe, kline.to_bar(timeframe))

  # ── Backfill ──────────────────────────────────────────────────────

  async def _backfill(self) -> None:
    """Publish the closed bars the socket missed, oldest first.

    Per stream, and only for a stream nothing is remembered for yet: after a
    reconnect the socket has already fed the others, so re-reading them would
    only re-publish what was published seconds ago. A stream whose history
    cannot be read is logged and skipped — the live socket is what keeps bars
    flowing, and a REST failure must not cost it — and the next reconnect
    tries that stream again.
    """
    if not self._binance_config.BACKFILL_ON_START:
      return
    for symbol in self.symbols:
      for timeframe in self.timeframes:
        if self._open_mark(symbol, timeframe) is not None:
          continue
        try:
          await self._backfill_stream(symbol, timeframe)
        except asyncio.CancelledError:
          raise
        except Exception as exc:
          log.warning("Binance %s %s backfill failed: %s", symbol, timeframe.value, exc)

  async def _backfill_stream(self, symbol: str, timeframe: Timeframe) -> None:
    interval = TIMEFRAME_INTERVALS[timeframe]
    rows = await self._history.klines(
      symbol=symbol,
      interval=interval,
      # One more than asked for: Binance's newest row is the bar still
      # forming, and publishing it would hand strategies an unfinished candle.
      limit=self._binance_config.WARMUP_BARS + 1,
    )
    now_ms = int(utcnow().timestamp() * 1000)
    # One kline per open time, oldest first. A response that repeated a bar
    # would make the series below shorter than its own ``warmup_total``, and
    # the subscriber waits for that number.
    by_open = {
      dto.open_time_ms: dto
      for dto in (
        BinanceKlineDTO.from_rest_row(
          row, symbol=symbol, interval=interval, now_ms=now_ms
        )
        for row in rows
      )
    }
    klines = [by_open[open_ms] for open_ms in sorted(by_open)]
    closed = [kline for kline in klines if kline.is_closed]
    # Trimmed from the newest end: a venue that hands back more rows than asked
    # must not widen the window the operator configured.
    closed = closed[-self._binance_config.WARMUP_BARS :]
    # Filtered before the window is numbered, not inside the emit loop: a bar
    # dropped halfway would leave a gap in ``warmup_index``. Nothing can be
    # dropped today — ``_backfill`` only visits streams with no mark yet — and
    # doing it here keeps that true however the caller changes.
    window = [
      kline
      for kline in closed
      if self._is_new_bar(symbol, timeframe, kline.open_time_ms)
    ]
    if not window:
      log.info("Binance %s %s has no closed bar to backfill", symbol, timeframe.value)
      return
    # ``warmup_total`` is the length of *this* window rather than the configured
    # ``warmup_bars``: a stream listed days ago has less history than that, and
    # a subscriber that ends its warm-up on ``warmup_index == warmup_total``
    # would otherwise wait for a bar Binance never had.
    #
    # Converted in full before the first bar is published, for the same reason:
    # a row the schema rejects half-way through would truncate a series that
    # has already announced its length, and the marks it moved mean the next
    # reconnect would not retry it. Raising here publishes nothing and leaves
    # the marks untouched, so ``_backfill`` logs the stream and the next
    # reconnect reads it again.
    bars = [(kline.open_time_ms, kline.to_bar(timeframe)) for kline in window]
    total = len(bars)
    log.info(
      "Binance %s %s backfilling %d closed bar(s) from %s",
      symbol,
      timeframe.value,
      total,
      bars[0][1].open_time.isoformat(),
    )
    for index, (open_time_ms, bar) in enumerate(bars, start=1):
      self._remember_bar(symbol, timeframe, open_time_ms)
      self.emit_bar(
        symbol,
        timeframe,
        bar,
        warmup_bar=True,
        warmup_index=index,
        warmup_total=total,
      )

  def _warn_once(self, key: str, message: str, *args: Any) -> None:
    if key in self._unknown:
      return
    self._unknown.add(key)
    log.warning(message, *args)
