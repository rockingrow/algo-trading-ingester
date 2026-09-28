"""
ingester/runtime.py — Starts and stops everything, in order, and reports it.

Kept apart from FastAPI so the whole lifecycle can be exercised in tests with
fakes; ``app.py`` only calls :meth:`start` / :meth:`stop` from its lifespan.

Start: notifier → NATS → every configured gateway → "running" notification.
Stop:  gateways (publishing what they already emitted) → "stopped"
       notification → NATS drain → notifier drain.
"""

from __future__ import annotations

import asyncio
from typing import Protocol

from ingester.core.factory import IngestionContext, IngestionFactory
from ingester.helpers import messages
from ingester.interfaces.ingestion_protocol import Ingestion
from ingester.interfaces.log_forwarder_protocol import LogForwarder
from ingester.interfaces.notifier_protocol import Notifier
from ingester.interfaces.publisher_protocol import EventPublisher
from ingester.logger import attach_handler, detach_handler, get_logger
from ingester.schemas.enums import ServiceStatusEnum
from ingester.settings import Settings

log = get_logger(__name__)


class Connection(Protocol):
  async def connect(self) -> None: ...

  async def close(self) -> None: ...


class ManagedNotifier(Notifier, Protocol):
  async def start(self) -> None: ...

  async def stop(self) -> None: ...


class IngesterRuntime:
  def __init__(
    self,
    config: Settings,
    *,
    factory: IngestionFactory,
    notifier: ManagedNotifier,
    connection: Connection,
    publisher: EventPublisher,
    subject_filter: str,
    log_notifier: ManagedNotifier | None = None,
    log_forwarder: LogForwarder | None = None,
  ) -> None:
    self._config = config
    self._factory = factory
    self._notifier = notifier
    self._log_notifier = log_notifier
    self._connection = connection
    self.publisher = publisher
    self._subject_filter = subject_filter
    self.ingestions: list[Ingestion] = []
    self.status = ServiceStatusEnum.STOPPED
    self._log_forwarder = log_forwarder
    self._forwarding = False

  @property
  def instance_id(self) -> str:
    return self._config.app.instance_id

  @property
  def endpoint(self) -> str:
    host = self._config.app.HOST
    host = "localhost" if host in ("0.0.0.0", "::") else host
    return f"http://{host}:{self._config.app.PORT}"

  async def start(self) -> None:
    """Start everything that can start; log and report the rest.

    Nothing here raises. A market-data gateway that exits because NATS was
    briefly down, or because one venue refused a symbol, is worse than one that
    runs degraded and says so — the bars it drops while a supervisor restarts
    it are unrecoverable.
    """
    await self._notifier.start()
    await self._start_error_forwarding()

    problems: list[str] = []

    try:
      await self._connection.connect()
    except Exception as exc:
      # nats-py reconnects on its own once the server answers; until then each
      # publish fails loudly and is counted in the gateway's snapshot.
      log.error("NATS unavailable at start-up: %s", exc)
      problems.append(f"NATS {self._config.nats.url}: {type(exc).__name__}: {exc}")

    context = IngestionContext(
      settings=self._config, publisher=self.publisher, notifier=self._notifier
    )
    for gateway in dict.fromkeys(self._config.app.GATEWAYS):
      try:
        ingestion = self._factory.create(gateway, context)
        await ingestion.start()
      except Exception as exc:
        log.exception("Gateway %s failed to start", gateway.value)
        problems.append(f"{gateway.value}: {type(exc).__name__}: {exc}")
        continue
      self.ingestions.append(ingestion)

    self.status = ServiceStatusEnum.RUNNING
    log.info("Ingester running: %s", ", ".join(self._streams()) or "no gateway")
    await self._notifier.send_message(
      messages.service_started(
        app_name=self._config.app.NAME,
        instance_id=self.instance_id,
        endpoint=self.endpoint,
        nats_url=self._config.nats.url,
        subject_filter=self._subject_filter,
        streams=self._streams(),
      )
    )
    if problems:
      await self._notifier.send_message(
        messages.service_degraded(
          app_name=self._config.app.NAME,
          instance_id=self.instance_id,
          problems=problems,
        )
      )

  async def stop(self) -> None:
    if self.status is not ServiceStatusEnum.RUNNING:
      return
    await self._stop_ingestions()
    self.status = ServiceStatusEnum.STOPPED
    await self._notifier.send_message(
      messages.service_stopped(
        app_name=self._config.app.NAME, instance_id=self.instance_id
      )
    )
    await self._shutdown()

  async def _start_error_forwarding(self) -> None:
    """Mirror ERROR records into their own Telegram chat, if configured."""
    if self._log_notifier is None or self._log_forwarder is None:
      return
    await self._log_notifier.start()
    self._log_forwarder.bind(asyncio.get_running_loop())
    attach_handler(self._log_forwarder)
    self._forwarding = True
    log.info("Forwarding ERROR log records to the dedicated error chat")

  async def _stop_error_forwarding(self) -> None:
    if self._forwarding and self._log_forwarder is not None:
      detach_handler(self._log_forwarder)
      self._log_forwarder.unbind()
      self._forwarding = False
    if self._log_notifier is not None:
      await self._log_notifier.stop()

  def _streams(self) -> list[str]:
    return [
      f"{snap['gateway'].upper()}: {','.join(snap['symbols'])} × "
      f"{','.join(snap['timeframes'])}"
      for snap in (ingestion.snapshot() for ingestion in self.ingestions)
    ]

  async def _stop_ingestions(self) -> None:
    for ingestion in reversed(self.ingestions):
      try:
        await ingestion.stop()
      except Exception:
        log.exception("Failed to stop %s ingestion", ingestion.gateway.value)

  async def _shutdown(self) -> None:
    await self._stop_ingestions()
    try:
      await self._connection.close()
    except Exception:
      log.exception("Failed to close NATS")
    # The error mirror goes last but one, so a failure during shutdown is still
    # reported; the status notifier is what closes the sequence.
    await self._stop_error_forwarding()
    await self._notifier.stop()
