"""
ingester/gateways/binance/ingestion.py — Binance bar-close detection.

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
* **De-duplication**: a reconnect can replay the bar that closed while the
  socket was down, and Binance occasionally repeats a final update. The newest
  open time already emitted per stream is remembered, so each bar is emitted
  once. (The publisher's ``event_id`` would also let JetStream drop it, but
  core NATS would not.)
* **Reconnects**: Binance closes a connection after 24 hours, and a silent
  socket is dropped by the stream's idle timeout. Either way the loop below
  dials again every ``reconnect_interval_seconds``. That idle timeout watches
  the socket, not each stream on it: one stream going quiet while the others
  deliver is not detected, which is the trade-off for sharing a connection.

Everything else — the publish queue, notifications, status — is the core's job
(:class:`~ingester.core.ingestion.BaseIngestion`).
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, ClassVar
from urllib.parse import urlsplit

from ingester.core.errors import GatewayConnectionError
from ingester.core.ingestion import BaseIngestion
from ingester.gateways.binance.dto import BinanceKlineDTO, stream_name
from ingester.gateways.binance.stream import KlineStream
from ingester.logger import get_logger
from ingester.schemas.enums import GatewayEnum, GatewayStatusEnum, Timeframe
from ingester.settings import BinanceSettings

log = get_logger(__name__)

StreamKey = tuple[str, Timeframe]


class BinanceIngestion(BaseIngestion):
  """Watches a Binance kline websocket for closed bars."""

  gateway: ClassVar[GatewayEnum] = GatewayEnum.BINANCE

  def __init__(self, *, config: BinanceSettings, stream: KlineStream, **kwargs: Any):
    super().__init__(config=config, **kwargs)
    self._binance_config = config
    self._stream = stream
    self._task: asyncio.Task[None] | None = None
    #: Binance's upper-case symbol → the name written in the market file.
    self._configured_symbol: dict[str, str] = {
      symbol.upper(): symbol for symbol in config.SYMBOLS
    }
    #: Open time (ms) of the newest bar already emitted, per stream.
    self._last_open_ms: dict[StreamKey, int] = {}
    #: Frames already complained about once — an unknown interval or symbol
    #: would otherwise log on every update.
    self._unknown: set[str] = set()

  # ── BaseIngestion hooks ───────────────────────────────────────────

  async def _start_source(self) -> None:
    # The endpoint's host is the venue-side origin a subscriber can trace back:
    # it tells spot from futures from testnet.
    self._set_venue(urlsplit(self._binance_config.WS_URL).hostname)
    self._task = asyncio.create_task(
      self._run(), name=f"{self.gateway.value}-ingestion"
    )

  async def _stop_source(self) -> None:
    task, self._task = self._task, None
    if task is not None:
      task.cancel()
      try:
        await task
      except asyncio.CancelledError:
        pass
    # Closed here rather than in the task: a socket closed while the task is
    # being cancelled can have its own close cancelled out from under it.
    await self._stream.close()

  # ── Connect → consume → reconnect ─────────────────────────────────

  async def _run(self) -> None:
    streams = [
      stream_name(symbol, timeframe)
      for symbol in self.symbols
      for timeframe in self.timeframes
    ]
    interval = self._binance_config.RECONNECT_INTERVAL_SECONDS
    while True:
      try:
        await self._stream.open(self._binance_config.WS_URL, streams)
      except asyncio.CancelledError:
        raise
      except Exception as exc:
        log.warning("Binance connect failed: %s", exc)
        self._set_status(GatewayStatusEnum.DISCONNECTED, str(exc))
        await self._stream.close()
        await asyncio.sleep(interval)
        continue

      try:
        await self._consume()
      except asyncio.CancelledError:
        raise
      except GatewayConnectionError as exc:
        log.warning("Binance connection lost: %s", exc)
        self._set_status(GatewayStatusEnum.DISCONNECTED, str(exc))
      except Exception as exc:
        # A bug in the read loop must not end the feed; reconnect and carry on.
        log.exception("Binance stream failed")
        self._set_status(GatewayStatusEnum.DISCONNECTED, f"{type(exc).__name__}: {exc}")
      await self._stream.close()
      await asyncio.sleep(interval)

  async def _consume(self) -> None:
    """Read frames until the socket drops.

    The gateway counts as RUNNING on the first frame, not on the handshake: a
    socket Binance accepts and drops at once — a rate limit, say — would
    otherwise flap RUNNING → DISCONNECTED on every reconnect and fill the
    status chat with it.
    """
    live = False
    while True:
      frame = await self._stream.receive()
      if not live:
        live = True
        self._set_status(GatewayStatusEnum.RUNNING)
      try:
        self._handle(frame)
      except (asyncio.CancelledError, GatewayConnectionError):
        # Binance rejecting the subscription is the connection's problem, not
        # this frame's — it belongs to the reconnect loop.
        raise
      except Exception:
        # One malformed frame must not cost the connection; the next one is
        # read as usual.
        log.exception("Binance frame rejected: %s", frame)

  # ── Business logic ────────────────────────────────────────────────

  def _handle(self, frame: Mapping[str, Any]) -> None:
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

    key = (symbol, timeframe)
    if kline.open_time_ms <= self._last_open_ms.get(key, -1):
      log.debug("Binance %s %s repeated bar, skipped", symbol, timeframe.value)
      return
    self._last_open_ms[key] = kline.open_time_ms
    self.emit_bar(symbol, timeframe, kline.to_bar(timeframe))

  def _warn_once(self, key: str, message: str, *args: Any) -> None:
    if key in self._unknown:
      return
    self._unknown.add(key)
    log.warning(message, *args)
