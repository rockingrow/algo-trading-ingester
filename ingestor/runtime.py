"""
ingestor/runtime.py — Starts and stops everything, in order, and reports it.

Kept apart from FastAPI so the whole lifecycle can be exercised in tests with
fakes; ``app.py`` only calls :meth:`start` / :meth:`stop` from its lifespan.

Start: notifier → NATS → every configured gateway → "running" notification.
Stop:  gateways (publishing what they already emitted) → "stopped"
       notification → NATS drain → notifier drain.
"""

from __future__ import annotations

from typing import Protocol

from ingestor.core.factory import IngestionContext, IngestionFactory
from ingestor.helpers import messages
from ingestor.interfaces.ingestion_protocol import Ingestion
from ingestor.interfaces.notifier_protocol import Notifier
from ingestor.interfaces.publisher_protocol import EventPublisher
from ingestor.logger import get_logger
from ingestor.schemas.enums import ServiceStatusEnum
from ingestor.settings import Settings

log = get_logger(__name__)


class Connection(Protocol):
  async def connect(self) -> None: ...

  async def close(self) -> None: ...


class ManagedNotifier(Notifier, Protocol):
  async def start(self) -> None: ...

  async def stop(self) -> None: ...


class IngestorRuntime:
  def __init__(
    self,
    config: Settings,
    *,
    factory: IngestionFactory,
    notifier: ManagedNotifier,
    connection: Connection,
    publisher: EventPublisher,
    subject_filter: str,
  ) -> None:
    self._config = config
    self._factory = factory
    self._notifier = notifier
    self._connection = connection
    self.publisher = publisher
    self._subject_filter = subject_filter
    self.ingestions: list[Ingestion] = []
    self.status = ServiceStatusEnum.STOPPED

  @property
  def instance_id(self) -> str:
    return self._config.app.instance_id

  @property
  def endpoint(self) -> str:
    host = self._config.app.HOST
    host = "localhost" if host in ("0.0.0.0", "::") else host
    return f"http://{host}:{self._config.app.PORT}"

  async def start(self) -> None:
    await self._notifier.start()
    try:
      await self._connection.connect()
      context = IngestionContext(
        settings=self._config, publisher=self.publisher, notifier=self._notifier
      )
      self.ingestions = self._factory.create_all(self._config.app.GATEWAYS, context)
      for ingestion in self.ingestions:
        await ingestion.start()
    except Exception as exc:
      log.exception("Ingestor failed to start")
      self.status = ServiceStatusEnum.FAILED
      await self._notifier.send_message(
        messages.service_failed(
          app_name=self._config.app.NAME,
          instance_id=self.instance_id,
          error=f"{type(exc).__name__}: {exc}",
        )
      )
      await self._shutdown()
      raise

    self.status = ServiceStatusEnum.RUNNING
    log.info("Ingestor running: %s", ", ".join(self._streams()))
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
    await self._notifier.stop()
