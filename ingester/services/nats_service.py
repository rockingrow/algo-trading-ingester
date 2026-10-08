"""
ingester/services/nats_service.py — The NATS connection and bar publishing.

:class:`NatsConnection` owns the connection lifecycle and its callbacks;
:class:`NatsPublisher` implements :class:`~ingester.interfaces.EventPublisher`
on top of it. Publishing is one way and expects no reply. The same connection
also carries the one subscription the ingester holds — history requests, in
:mod:`ingester.services.history_service`.

Two delivery modes, chosen by ``NATS_JETSTREAM_ENABLED``:

* **Core NATS** (default) — fire-and-forget. While reconnecting, nats-py buffers
  writes and flushes them once the link is back.
* **JetStream** — each event is persisted on a stream named after
  ``NATS_SUBJECT_PREFIX`` so a subscriber that was down can replay it.
  ``event_id`` rides as ``Nats-Msg-Id``, so a re-publish of the same bar is
  dropped by the stream.

Reconnecting is not unlimited. :class:`ReconnectWatchdog` counts the failed
attempts in a rolling window and, past ``NATS_GIVE_UP_AFTER_ATTEMPTS``, stops
the whole service: an ingester that cannot reach NATS publishes nothing, and a
process that retries silently for hours is worse than one that is plainly down.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import suppress
from typing import Any

import nats as nats_lib
from nats.aio.client import Client as NATSClient
from nats.js import JetStreamContext
from nats.js import api as js_api

from ingester.helpers import messages
from ingester.interfaces.notifier_protocol import Notifier
from ingester.logger import get_logger
from ingester.schemas.market_event_schema import MarketEvent
from ingester.settings import NatsSettings
from ingester.shutdown import request_shutdown

log = get_logger(__name__)

#: Floor for the watchdog's tick, so a very small ``RECONNECT_TIME_WAIT``
#: cannot turn the check into a busy loop.
_MIN_WATCHDOG_INTERVAL = 0.5


class ReconnectWatchdog:
  """Counts failed NATS connection attempts inside a rolling window.

  One attempt is recorded per retry cadence while the link is down, and
  anything older than the window is forgotten. A successful reconnect
  deliberately does **not** clear the count: a link that keeps flapping never
  carries bars reliably either, so it has to be able to trip this too.

  It trips once. Giving up stops the process, and the caller must not be told
  to do that twice.
  """

  def __init__(
    self,
    max_attempts: int,
    window_seconds: float,
    clock: Callable[[], float] = time.monotonic,
  ) -> None:
    self._max_attempts = max_attempts
    self._window_seconds = window_seconds
    self._clock = clock
    self._failures: deque[float] = deque()
    self._tripped = False

  @property
  def enabled(self) -> bool:
    """``NATS_GIVE_UP_AFTER_ATTEMPTS=0`` means retry for as long as we run."""
    return self._max_attempts > 0

  @property
  def attempts(self) -> int:
    """Failed attempts still inside the window."""
    return len(self._failures)

  def record_failure(self) -> bool:
    """Record one failed attempt; ``True`` when that was one too many."""
    if not self.enabled or self._tripped:
      return False
    now = self._clock()
    self._failures.append(now)
    cutoff = now - self._window_seconds
    while self._failures and self._failures[0] < cutoff:
      self._failures.popleft()
    if len(self._failures) < self._max_attempts:
      return False
    self._tripped = True
    return True


class NatsConnection:
  """Owns the NATS client and reports connection changes to operators."""

  def __init__(
    self,
    config: NatsSettings,
    notifier: Notifier,
    instance_id: str,
    *,
    stop_service: Callable[[str], None] = request_shutdown,
    clock: Callable[[], float] = time.monotonic,
  ) -> None:
    self._config = config
    self._notifier = notifier
    self._instance_id = instance_id
    self._nc: NATSClient | None = None
    self._js: JetStreamContext | None = None
    # Set while we close on purpose, so the drain's own disconnect callback is
    # not reported to operators as an outage.
    self._closing = False
    #: Called after every re-established connection, in registration order.
    self._reconnect_hooks: list[Callable[[], Awaitable[None]]] = []
    # How the connection stops the process once it has given up. Injected so
    # tests can watch the decision without taking the test runner down.
    self._stop_service = stop_service
    self._watchdog = ReconnectWatchdog(
      config.GIVE_UP_AFTER_ATTEMPTS, config.GIVE_UP_WINDOW_SECONDS, clock=clock
    )
    self._watchdog_task: asyncio.Task[None] | None = None

  def on_reconnect(self, hook: Callable[[], Awaitable[None]]) -> None:
    """Run *hook* each time the connection comes back after a drop."""
    self._reconnect_hooks.append(hook)

  @property
  def config(self) -> NatsSettings:
    return self._config

  @property
  def is_connected(self) -> bool:
    return self._nc is not None and self._nc.is_connected

  @property
  def nc(self) -> NATSClient:
    if self._nc is None:
      raise RuntimeError("NATS is not connected — call connect() first")
    return self._nc

  @property
  def js(self) -> JetStreamContext:
    if self._js is None:
      self._js = self.nc.jetstream()
    return self._js

  async def connect(self) -> None:
    # Started before the first attempt, so a server that is already down at
    # boot is counted too: the bounded first connect below abandons nats-py's
    # retry loop, and without the watchdog the process would sit there
    # publishing nothing for as long as it ran.
    self._start_watchdog()

    options: dict[str, Any] = {
      "servers": [self._config.url],
      "name": f"ingester-{self._instance_id}",
      "connect_timeout": self._config.CONNECT_TIMEOUT,
      # Retry forever once up; the initial connect is bounded below.
      "max_reconnect_attempts": -1,
      "reconnect_time_wait": self._config.RECONNECT_TIME_WAIT,
      "disconnected_cb": self._on_disconnected,
      "reconnected_cb": self._on_reconnected,
      "error_cb": self._on_error,
    }
    if self._config.TOKEN:
      options["token"] = self._config.TOKEN

    # nats-py applies max_reconnect_attempts to the first connect too, so -1
    # would hang forever against a dead server. Bound it: fail fast at boot.
    try:
      self._nc = await asyncio.wait_for(
        nats_lib.connect(**options), timeout=self._config.CONNECT_TIMEOUT
      )
    except TimeoutError as exc:
      raise ConnectionError(
        f"NATS at {self._config.url} did not answer within "
        f"{self._config.CONNECT_TIMEOUT:.1f}s"
      ) from exc
    log.info("NATS connected to %s", self._config.url)

    if self._config.JETSTREAM_ENABLED:
      await self._ensure_stream()

  async def close(self) -> None:
    # Before the early return: the watchdog outlives a connect that failed,
    # and a shutdown must not leave it counting.
    await self._stop_watchdog()
    if self._nc is None:
      return
    self._closing = True
    try:
      # Drain flushes anything still buffered instead of dropping it.
      await self._nc.drain()
    except Exception as exc:
      log.warning("NATS drain failed, closing hard: %s", exc)
      await self._nc.close()
    finally:
      self._nc = None
      self._js = None
      self._closing = False
      log.info("NATS connection closed")

  async def _ensure_stream(self) -> None:
    """Create the stream if absent; never reconfigure one that exists."""
    name = self._config.stream_name
    subjects = [f"{self._config.SUBJECT_PREFIX}.>"]
    try:
      info = await self.js.stream_info(name)
    except Exception:
      info = None
    if info is not None:
      log.info("JetStream stream %s already exists", name)
      self._warn_stream_drift(info, subjects[0])
      return
    await self.js.add_stream(
      js_api.StreamConfig(
        name=name,
        subjects=subjects,
        retention=js_api.RetentionPolicy.LIMITS,
        storage=js_api.StorageType.FILE,
        max_age=self._config.STREAM_MAX_AGE_SECONDS,
        duplicate_window=self._config.DUPLICATE_WINDOW_SECONDS,
      )
    )
    log.info("JetStream stream %s created (subjects=%s)", name, subjects)

  def _warn_stream_drift(self, info: Any, wanted_subject: str) -> None:
    """Report an existing stream that no longer matches these settings.

    A stream is never reconfigured once it exists, so both of these fail
    quietly long after the change that caused them: a stream whose subjects
    miss our prefix rejects every publish, and one whose duplicate window is
    shorter than a reconnect's recovery read lets a re-read bar through twice.
    """
    config = getattr(info, "config", None)
    if config is None:
      return

    subjects = list(getattr(config, "subjects", None) or [])
    if wanted_subject not in subjects:
      log.error(
        "JetStream stream %s listens on %s but this ingester publishes to %s — "
        "publishes will be rejected unless an existing subject covers it. "
        "The stream pre-dates this NATS_SUBJECT_PREFIX; recreate it or publish "
        "under the prefix it already carries.",
        self._config.stream_name,
        subjects or "nothing",
        wanted_subject,
      )

    window = getattr(config, "duplicate_window", None)
    wanted_window = self._config.DUPLICATE_WINDOW_SECONDS
    if window is not None and window < wanted_window:
      log.warning(
        "JetStream stream %s de-duplicates over %.0fs, less than the "
        "configured %.0fs; a bar re-read after a longer outage than that is "
        "stored twice. Recreate the stream to widen the window.",
        self._config.stream_name,
        window,
        wanted_window,
      )

  # ── Giving up ─────────────────────────────────────────────────────

  def _start_watchdog(self) -> None:
    if not self._watchdog.enabled or self._watchdog_task is not None:
      return
    self._watchdog_task = asyncio.get_running_loop().create_task(
      self._watch_connection(), name="nats-reconnect-watchdog"
    )
    log.info(
      "NATS watchdog armed: stopping the service after %d failed attempts "
      "within %.0f min",
      self._config.GIVE_UP_AFTER_ATTEMPTS,
      self._config.GIVE_UP_WINDOW_SECONDS / 60,
    )

  async def _stop_watchdog(self) -> None:
    task, self._watchdog_task = self._watchdog_task, None
    if task is None:
      return
    task.cancel()
    with suppress(asyncio.CancelledError):
      await task

  async def _watch_connection(self) -> None:
    """Count one failed attempt per retry cadence while the link is down.

    Polling rather than nats-py's ``error_cb``: that callback also fires for
    errors raised on a healthy connection, and it never fires at all when the
    first connect was abandoned — the two cases this has to cover.
    """
    interval = max(self._config.RECONNECT_TIME_WAIT, _MIN_WATCHDOG_INTERVAL)
    while True:
      await asyncio.sleep(interval)
      if await self._check_connection():
        return

  async def _check_connection(self) -> bool:
    """One watchdog tick. ``True`` once it has given up and said so."""
    if self._closing or self.is_connected:
      return False
    if not self._watchdog.record_failure():
      return False
    await self._give_up()
    return True

  async def _give_up(self) -> None:
    """Report that NATS is unreachable and stop the service for good."""
    attempts = self._watchdog.attempts
    window_seconds = self._config.GIVE_UP_WINDOW_SECONDS
    reason = (
      f"NATS at {self._config.url} is unreachable: {attempts} failed "
      f"connection attempts within {window_seconds / 60:.0f} min, nothing can "
      "be published"
    )
    log.error("%s. Giving up; the service will not restart on its own.", reason)
    # Queued, and the graceful shutdown drains that queue — so the message
    # explaining the stop is delivered before the process is gone.
    await self._notifier.send_message(
      messages.nats_gave_up(
        url=self._config.url,
        instance_id=self._instance_id,
        attempts=attempts,
        window_seconds=window_seconds,
      )
    )
    self._stop_service(reason)

  # ── Callbacks ─────────────────────────────────────────────────────

  async def _on_disconnected(self) -> None:
    if self._closing:
      return
    log.warning("NATS disconnected from %s", self._config.url)
    await self._notifier.send_message(
      messages.nats_disconnected(url=self._config.url, instance_id=self._instance_id)
    )

  async def _on_reconnected(self) -> None:
    log.info("NATS reconnected to %s", self._config.url)
    await self._notifier.send_message(
      messages.nats_reconnected(url=self._config.url, instance_id=self._instance_id)
    )
    for hook in self._reconnect_hooks:
      try:
        await hook()
      except Exception:
        # One hook failing must not cost the others, nor nats-py its callback.
        log.exception("A NATS reconnect hook failed")

  async def _on_error(self, exc: Exception) -> None:
    log.error("NATS error: %s", exc)


class NatsPublisher:
  """:class:`EventPublisher` that writes canonical events to NATS subjects.

  Subject: ``<NATS_SUBJECT_PREFIX>.<event.subject_tokens…>``, e.g.
  ``INGEST.bar.closed.mt5.XAUUSD.M15``. Body: the event as JSON.
  """

  def __init__(self, connection: NatsConnection) -> None:
    self._connection = connection
    self._config = connection.config

  @property
  def is_connected(self) -> bool:
    return self._connection.is_connected

  @property
  def subject_filter(self) -> str:
    """Wildcard matching everything this publisher can emit."""
    return f"{self._config.SUBJECT_PREFIX}.>"

  def subject_for(self, event: MarketEvent) -> str:
    return ".".join((self._config.SUBJECT_PREFIX, *event.subject_tokens))

  async def publish(self, event: MarketEvent) -> None:
    subject = self.subject_for(event)
    data = event.model_dump_json().encode()
    if self._config.JETSTREAM_ENABLED:
      # A JetStream publish while reconnecting would sit out the whole ack
      # timeout; fail fast instead so the error is reported immediately.
      if not self.is_connected:
        raise ConnectionError(f"NATS ({self._config.url}) is not connected")
      ack = await self._connection.js.publish(
        subject,
        data,
        timeout=self._config.PUBLISH_TIMEOUT,
        headers={js_api.Header.MSG_ID.value: event.event_id},
      )
      if ack.duplicate:
        # The stream acknowledged the publish and stored nothing: this
        # ``Nats-Msg-Id`` was already seen inside the duplicate window. No
        # JetStream consumer will ever be handed this message, so it must not
        # read as "published" in the log.
        log.warning(
          "JetStream DROPPED %s as a duplicate → %s (stream=%s, kept seq=%s) — "
          "no consumer receives this message",
          event.event_id,
          subject,
          ack.stream,
          ack.seq,
        )
        return
      log.info(
        "Published %s → %s (stream=%s seq=%s)",
        event.event_id,
        subject,
        ack.stream,
        ack.seq,
      )
    else:
      await self._connection.nc.publish(subject, data)
      log.info("Published %s → %s", event.event_id, subject)
    log.debug("Published payload %s: %s", event.event_id, data.decode())
