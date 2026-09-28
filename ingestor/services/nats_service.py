"""
ingestor/services/nats_service.py — One-way NATS publishing.

:class:`NatsConnection` owns the connection lifecycle and its callbacks;
:class:`NatsPublisher` implements :class:`~ingestor.interfaces.EventPublisher`
on top of it. The ingestor only ever *publishes* — it holds no subscriptions and
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

from ingestor.helpers import messages
from ingestor.interfaces.notifier_protocol import Notifier
from ingestor.logger import get_logger
from ingestor.schemas.market_event_schema import MarketEvent
from ingestor.settings import NatsSettings

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
      "name": f"ingestor-{self._instance_id}",
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
      await self.js.stream_info(name)
      log.info("JetStream stream %s already exists", name)
      return
    except Exception:
      pass
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
