"""
ingester/core/ingestion.py — The reusable ingestion core.

Every gateway shares the same pipeline; only the part that talks to the venue
differs. The core owns the shared part, the gateway supplies the rest
(Template Method):

::

  venue ──▶ gateway business logic ──▶ DTO.to_bar() ──▶ emit_bar()
                                                          │  (thread-safe)
                                                          ▼
                              asyncio.Queue ──▶ dispatcher ──▶ EventPublisher
                                                               (NATS, one way)

* :class:`BaseIngestion` — lifecycle, the thread-safe hand-off queue, the
  dispatcher that publishes canonical events, status tracking and operator
  notifications. Subclasses implement ``_start_source`` / ``_stop_source``.
  It also keeps the per-stream record of the newest bar already emitted, so
  every gateway de-duplicates the same way.
* :class:`AsyncStreamIngestion` — for venues that push frames over an asyncio
  connection (the Binance websocket). Runs one task with a connect → receive →
  reconnect loop. Subclasses implement ``connect``, ``receive``, ``handle`` and
  ``disconnect``.
* :class:`ThreadedIngestion` — for venues whose SDK is blocking (MetaTrader 5).
  Runs one dedicated thread with a connect → poll → reconnect loop. Subclasses
  implement only ``connect``, ``poll`` and ``disconnect``.

The core depends on the :class:`EventPublisher` and :class:`Notifier`
abstractions, never on NATS or Telegram directly.
"""

from __future__ import annotations

import asyncio
import threading
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, ClassVar

from ingester.core.errors import GatewayConnectionError
from ingester.helpers import messages
from ingester.interfaces.notifier_protocol import Notifier
from ingester.interfaces.publisher_protocol import EventPublisher
from ingester.logger import get_logger
from ingester.schemas.enums import GatewayEnum, GatewayStatusEnum, Timeframe
from ingester.schemas.market_event_schema import (
  Bar,
  BarClosedEvent,
  EventSource,
  MarketEvent,
  utcnow,
)
from ingester.settings import GatewaySettings

log = get_logger(__name__)

#: One stream of bars: a configured symbol on one timeframe.
StreamKey = tuple[str, Timeframe]

#: Statuses in which the source is live and ``start`` must not run again.
_ACTIVE = frozenset(
  {
    GatewayStatusEnum.STARTING,
    GatewayStatusEnum.RUNNING,
    GatewayStatusEnum.DISCONNECTED,
  }
)


class BaseIngestion(ABC):
  """Shared lifecycle + publish pipeline for one gateway.

  Thread-safety contract: :meth:`emit_bar` and :meth:`_set_status` may be
  called from any thread; everything else runs on the event loop.
  """

  #: Set by each concrete gateway.
  gateway: ClassVar[GatewayEnum]

  def __init__(
    self,
    *,
    config: GatewaySettings,
    publisher: EventPublisher,
    notifier: Notifier,
    instance_id: str,
    drain_timeout: float = 5.0,
  ) -> None:
    self._config = config
    self._publisher = publisher
    self._notifier = notifier
    self._instance_id = instance_id
    self._drain_timeout = drain_timeout
    self._source = EventSource(
      gateway=self.gateway, market=config.MARKET, ingester_id=instance_id
    )
    self._status = GatewayStatusEnum.IDLE
    self._status_detail: str | None = None
    self._loop: asyncio.AbstractEventLoop | None = None
    self._queue: asyncio.Queue[MarketEvent] | None = None
    self._dispatcher: asyncio.Task[None] | None = None
    self._background: set[asyncio.Task[Any]] = set()
    # Stats, surfaced by ``snapshot`` for the /status endpoint.
    self._published = 0
    self._failed = 0
    self._last_published_at: datetime | None = None
    self._last_error: str | None = None
    self._last_bar: dict[str, str] = {}
    #: Open time of the newest bar already emitted, per stream, in the venue's
    #: own integer clock (MT5: server-clock seconds, Binance: milliseconds).
    #: Kept across reconnects, so a replayed or re-read bar is recognised.
    self._open_marks: dict[StreamKey, int] = {}

  # ── Public API ────────────────────────────────────────────────────

  @property
  def symbols(self) -> list[str]:
    return list(self._config.SYMBOLS)

  @property
  def timeframes(self) -> list[Timeframe]:
    return list(self._config.TIMEFRAMES)

  @property
  def status(self) -> GatewayStatusEnum:
    return self._status

  async def start(self) -> None:
    """Start the dispatcher, then the gateway's source."""
    if self._status in _ACTIVE:
      return
    if not self.symbols or not self.timeframes:
      raise ValueError(
        f"{self.gateway.value}: at least one symbol and one timeframe must be "
        f"configured (got symbols={self.symbols}, timeframes={self.timeframes})"
      )

    self._loop = asyncio.get_running_loop()
    self._queue = asyncio.Queue()
    self._dispatcher = asyncio.create_task(
      self._dispatch_loop(), name=f"{self.gateway.value}-dispatcher"
    )
    self._set_status(GatewayStatusEnum.STARTING)
    log.info(
      "%s ingestion starting: symbols=%s timeframes=%s",
      self.gateway.value,
      ",".join(self.symbols),
      ",".join(tf.value for tf in self.timeframes),
    )
    try:
      await self._start_source()
    except Exception as exc:
      self._set_status(GatewayStatusEnum.FAILED, str(exc))
      await self._stop_dispatcher()
      raise

  async def stop(self) -> None:
    """Stop the source, publish whatever it already emitted, then report."""
    if self._status not in _ACTIVE:
      return
    try:
      await self._stop_source()
    finally:
      await self._stop_dispatcher()
      self._set_status(GatewayStatusEnum.STOPPED)
      await self._flush_background()

  def snapshot(self) -> dict[str, Any]:
    return {
      "gateway": self.gateway.value,
      "status": self._status.value,
      "status_detail": self._status_detail,
      "market": self._source.market.value,
      "venue": self._source.venue,
      "symbols": self.symbols,
      "timeframes": [tf.value for tf in self.timeframes],
      "published": self._published,
      "failed": self._failed,
      "last_published_at": (
        self._last_published_at.isoformat() if self._last_published_at else None
      ),
      "last_error": self._last_error,
      "last_bar_open_time": dict(self._last_bar),
    }

  # ── For subclasses ────────────────────────────────────────────────

  @abstractmethod
  async def _start_source(self) -> None:
    """Begin producing data (open a socket, start a thread …). Non-blocking."""

  @abstractmethod
  async def _stop_source(self) -> None:
    """Stop producing data. After it returns, no more ``emit_*`` calls happen."""

  def _set_venue(self, venue: str | None) -> None:
    """Record the venue-side origin (e.g. the MT5 trade server) on events."""
    if venue and venue != self._source.venue:
      self._source = self._source.model_copy(update={"venue": venue})

  def _open_mark(self, symbol: str, timeframe: Timeframe) -> int | None:
    """Open time of the newest bar remembered for a stream, ``None`` before the
    first one."""
    return self._open_marks.get((symbol, timeframe))

  def _is_new_bar(self, symbol: str, timeframe: Timeframe, open_mark: int) -> bool:
    """Whether a bar opening at *open_mark* is later than every bar this
    stream has already emitted.

    Together with :meth:`_remember_bar` this is the one de-duplication rule
    every gateway shares. Both are called from the gateway's own source — its
    thread or its task — never from two places at once.
    """
    last = self._open_marks.get((symbol, timeframe))
    return last is None or open_mark > last

  def _remember_bar(self, symbol: str, timeframe: Timeframe, open_mark: int) -> None:
    """Record *open_mark* as the newest bar handled for a stream."""
    self._open_marks[(symbol, timeframe)] = open_mark

  def emit_bar(self, symbol: str, timeframe: Timeframe, bar: Bar) -> None:
    """Hand one closed bar to the publish pipeline. Safe from any thread."""
    event = BarClosedEvent.create(
      source=self._source, symbol=symbol, timeframe=timeframe, bar=bar
    )
    self._last_bar[f"{symbol}:{timeframe.value}"] = bar.open_time.isoformat()
    self._call_in_loop(self._enqueue, event)

  def _set_status(self, status: GatewayStatusEnum, detail: str | None = None) -> None:
    """Record a status change and notify operators. Safe from any thread."""
    self._call_in_loop(self._apply_status, status, detail)

  # ── Internals ─────────────────────────────────────────────────────

  def _call_in_loop(self, callback: Any, *args: Any) -> None:
    loop = self._loop
    if loop is None or loop.is_closed():
      log.warning("%s: event loop gone, dropping %s", self.gateway.value, callback)
      return
    try:
      running = asyncio.get_running_loop()
    except RuntimeError:
      running = None
    if running is loop:
      callback(*args)
    else:
      loop.call_soon_threadsafe(callback, *args)

  def _enqueue(self, event: MarketEvent) -> None:
    if self._queue is None:
      log.warning("%s: not started, dropping %s", self.gateway.value, event.event_id)
      return
    self._queue.put_nowait(event)

  def _apply_status(self, status: GatewayStatusEnum, detail: str | None) -> None:
    if status == self._status:
      return
    previous, self._status, self._status_detail = self._status, status, detail
    log.info(
      "%s status %s → %s%s",
      self.gateway.value,
      previous.value,
      status.value,
      f" ({detail})" if detail else "",
    )
    text = messages.gateway_status(
      gateway=self.gateway,
      instance_id=self._instance_id,
      status=status,
      detail=detail,
    )
    task = asyncio.create_task(self._notifier.send_message(text))
    self._background.add(task)
    task.add_done_callback(self._background.discard)

  async def _dispatch_loop(self) -> None:
    assert self._queue is not None
    while True:
      event = await self._queue.get()
      try:
        await self._publisher.publish(event)
        self._published += 1
        self._last_published_at = utcnow()
        log.debug("Published %s", event.event_id)
      except asyncio.CancelledError:
        raise
      except Exception as exc:
        self._failed += 1
        self._last_error = f"{type(exc).__name__}: {exc}"
        log.error("Failed to publish %s: %s", event.event_id, exc)
      finally:
        self._queue.task_done()

  async def _stop_dispatcher(self) -> None:
    """Give queued events a bounded chance to publish, then cancel."""
    # Let hand-offs a stopped thread scheduled via call_soon_threadsafe land in
    # the queue first — ``join`` returns at once on an empty queue.
    await asyncio.sleep(0)
    if self._queue is not None and self._dispatcher is not None:
      try:
        await asyncio.wait_for(self._queue.join(), timeout=self._drain_timeout)
      except TimeoutError:
        log.warning(
          "%s: %d event(s) not published before shutdown",
          self.gateway.value,
          self._queue.qsize(),
        )
    if self._dispatcher is not None:
      self._dispatcher.cancel()
      try:
        await self._dispatcher
      except asyncio.CancelledError:
        pass
      self._dispatcher = None

  async def _flush_background(self) -> None:
    if self._background:
      await asyncio.gather(*self._background, return_exceptions=True)


class AsyncStreamIngestion(BaseIngestion):
  """Runs an asyncio-native venue feed on one task.

  The task owns the whole venue session: ``connect``, every ``receive`` and
  ``disconnect`` are awaited on it, and it dials again every
  ``reconnect_interval`` seconds after the session drops. Subclasses implement
  the four hooks and call :meth:`emit_bar` from ``handle``.
  """

  def __init__(self, *, reconnect_interval: float, **kwargs: Any) -> None:
    super().__init__(**kwargs)
    self._reconnect_interval = reconnect_interval
    self._task: asyncio.Task[None] | None = None

  # ── Hooks: the gateway's business logic ───────────────────────────

  @abstractmethod
  async def connect(self) -> None:
    """Open the venue session. Raise ``GatewayConnectionError`` on failure."""

  @abstractmethod
  async def receive(self) -> Any:
    """Next frame from the venue.

    Raise ``GatewayConnectionError`` when the session is gone or went quiet.
    """

  @abstractmethod
  def handle(self, frame: Any) -> None:
    """``emit_bar`` the closed bar a frame carries, if it carries one.

    Raise ``GatewayConnectionError`` when the frame says the session itself is
    broken; any other exception rejects this frame only.
    """

  @abstractmethod
  async def disconnect(self) -> None:
    """Close the venue session. Must be safe to call when not connected."""

  # ── BaseIngestion ─────────────────────────────────────────────────

  async def _start_source(self) -> None:
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
    # Closed here rather than in the task: a session closed while the task is
    # being cancelled can have its own close cancelled out from under it.
    await self.disconnect()

  # ── Task body: connect → consume → reconnect ──────────────────────

  async def _run(self) -> None:
    while True:
      try:
        await self.connect()
      except asyncio.CancelledError:
        raise
      except Exception as exc:
        log.warning("%s connect failed: %s", self.gateway.value, exc)
        self._set_status(GatewayStatusEnum.DISCONNECTED, str(exc))
        await self.disconnect()
        await asyncio.sleep(self._reconnect_interval)
        continue

      try:
        await self._consume()
      except asyncio.CancelledError:
        raise
      except GatewayConnectionError as exc:
        log.warning("%s connection lost: %s", self.gateway.value, exc)
        self._set_status(GatewayStatusEnum.DISCONNECTED, str(exc))
      except Exception as exc:
        # A bug in the read loop must not end the feed; reconnect and carry on.
        log.exception("%s stream failed", self.gateway.value)
        self._set_status(GatewayStatusEnum.DISCONNECTED, f"{type(exc).__name__}: {exc}")
      await self.disconnect()
      await asyncio.sleep(self._reconnect_interval)

  async def _consume(self) -> None:
    """Read frames until the session drops.

    The gateway counts as RUNNING on the first frame, not on the handshake: a
    session the venue accepts and drops at once — a rate limit, say — would
    otherwise flap RUNNING → DISCONNECTED on every reconnect and fill the
    status chat with it.
    """
    live = False
    while True:
      frame = await self.receive()
      if not live:
        live = True
        self._set_status(GatewayStatusEnum.RUNNING)
      try:
        self.handle(frame)
      except (asyncio.CancelledError, GatewayConnectionError):
        # The venue rejecting the session is the connection's problem, not
        # this frame's — it belongs to the reconnect loop.
        raise
      except Exception:
        # One malformed frame must not cost the connection; the next one is
        # read as usual.
        log.exception("%s frame rejected: %s", self.gateway.value, frame)


class ThreadedIngestion(BaseIngestion):
  """Runs a blocking venue SDK on one dedicated thread.

  The thread owns the whole venue session — ``connect``, every ``poll`` and
  ``disconnect`` run on it — which matters for SDKs such as MetaTrader5 that
  keep global, thread-affine state. Subclasses implement the three hooks and
  call :meth:`emit_bar` from ``poll``.
  """

  def __init__(
    self,
    *,
    poll_interval: float,
    reconnect_interval: float,
    join_timeout: float = 10.0,
    **kwargs: Any,
  ) -> None:
    super().__init__(**kwargs)
    self._poll_interval = poll_interval
    self._reconnect_interval = reconnect_interval
    self._join_timeout = join_timeout
    self._stop_event = threading.Event()
    self._thread: threading.Thread | None = None

  # ── Hooks: the gateway's business logic ───────────────────────────

  @abstractmethod
  def connect(self) -> None:
    """Open the venue session. Raise ``GatewayConnectionError`` on failure."""

  @abstractmethod
  def poll(self) -> None:
    """Check for new closed bars and ``emit_bar`` each one.

    Raise ``GatewayConnectionError`` when the session is gone.
    """

  @abstractmethod
  def disconnect(self) -> None:
    """Close the venue session. Must be safe to call when not connected."""

  # ── BaseIngestion ─────────────────────────────────────────────────

  async def _start_source(self) -> None:
    self._stop_event.clear()
    self._thread = threading.Thread(
      target=self._thread_main, name=f"{self.gateway.value}-ingestion", daemon=True
    )
    self._thread.start()

  async def _stop_source(self) -> None:
    self._stop_event.set()
    if self._thread is not None:
      await asyncio.to_thread(self._thread.join, self._join_timeout)
      if self._thread.is_alive():
        log.warning(
          "%s thread did not stop within %.0fs", self.gateway.value, self._join_timeout
        )
      self._thread = None

  # ── Thread body ───────────────────────────────────────────────────

  def _thread_main(self) -> None:
    connected = False
    try:
      while not self._stop_event.is_set():
        if not connected:
          connected = self._try_connect()
          if not connected:
            self._stop_event.wait(self._reconnect_interval)
            continue

        try:
          self.poll()
        except GatewayConnectionError as exc:
          log.warning("%s connection lost: %s", self.gateway.value, exc)
          self._set_status(GatewayStatusEnum.DISCONNECTED, str(exc))
          self._safe_disconnect()
          connected = False
          continue
        except Exception:
          # A bug in one poll must not kill the feed; the next poll retries.
          log.exception("%s poll failed", self.gateway.value)

        self._stop_event.wait(self._poll_interval)
    finally:
      if connected:
        self._safe_disconnect()

  def _try_connect(self) -> bool:
    try:
      self.connect()
    except Exception as exc:
      log.warning("%s connect failed: %s", self.gateway.value, exc)
      self._set_status(GatewayStatusEnum.DISCONNECTED, str(exc))
      self._safe_disconnect()
      return False
    self._set_status(GatewayStatusEnum.RUNNING)
    return True

  def _safe_disconnect(self) -> None:
    try:
      self.disconnect()
    except Exception:
      log.exception("%s disconnect failed", self.gateway.value)
