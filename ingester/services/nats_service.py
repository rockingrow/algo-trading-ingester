"""
ingester/services/nats_service.py — One-way NATS publishing.

:class:`NatsConnection` owns the connection lifecycle and its callbacks;
:class:`NatsPublisher` implements :class:`~ingester.interfaces.EventPublisher`
on top of it. The ingester only ever *publishes* — it holds no subscriptions and
expects no replies.

Two delivery modes, chosen by ``NATS_JETSTREAM_ENABLED``:

* **Core NATS** (default) — fire-and-forget. While reconnecting, nats-py buffers
  writes and flushes them once the link is back.
* **JetStream** — each event is persisted on stream ``NATS_STREAM_NAME`` so a
  subscriber that was down can replay it. ``event_id`` rides as
  ``Nats-Msg-Id``, so a re-publish of the same bar is dropped by the stream.
"""

from __future__ import annotations

import asyncio
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

log = get_logger(__name__)


class NatsConnection:
  """Owns the NATS client and reports connection changes to operators."""

  def __init__(
    self, config: NatsSettings, notifier: Notifier, instance_id: str
  ) -> None:
    self._config = config
    self._notifier = notifier
    self._instance_id = instance_id
    self._nc: NATSClient | None = None
    self._js: JetStreamContext | None = None
    # Set while we close on purpose, so the drain's own disconnect callback is
    # not reported to operators as an outage.
    self._closing = False

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
    name = self._config.STREAM_NAME
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
    shorter than a start-up backfill lets replayed bars through twice.
    """
    config = getattr(info, "config", None)
    if config is None:
      return

    subjects = list(getattr(config, "subjects", None) or [])
    if wanted_subject not in subjects:
      log.error(
        "JetStream stream %s listens on %s but this ingester publishes to %s — "
        "publishes will be rejected unless an existing subject covers it. "
        "Check NATS_STREAM_NAME against NATS_SUBJECT_PREFIX.",
        self._config.STREAM_NAME,
        subjects or "nothing",
        wanted_subject,
      )

    window = getattr(config, "duplicate_window", None)
    wanted_window = self._config.DUPLICATE_WINDOW_SECONDS
    if window is not None and window < wanted_window:
      log.warning(
        "JetStream stream %s de-duplicates over %.0fs, less than the "
        "configured %.0fs; a restart that backfills further back than that "
        "will republish bars. Recreate the stream to widen the window.",
        self._config.STREAM_NAME,
        window,
        wanted_window,
      )

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
      await self._connection.js.publish(
        subject,
        data,
        timeout=self._config.PUBLISH_TIMEOUT,
        headers={js_api.Header.MSG_ID.value: event.event_id},
      )
    else:
      await self._connection.nc.publish(subject, data)
    log.info("Published %s → %s", event.event_id, subject)
    log.debug("Published payload %s: %s", event.event_id, data.decode())
