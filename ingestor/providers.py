"""
ingestor/providers.py — Composition root: builds concrete services from settings.

This is the only module that knows which implementation backs each interface,
and the only place a new gateway has to be registered.
"""

from __future__ import annotations

from ingestor.core.factory import IngestionFactory
from ingestor.gateways.mt5 import build_mt5_ingestion
from ingestor.runtime import IngestorRuntime
from ingestor.schemas.enums import GatewayEnum
from ingestor.services.nats_service import NatsConnection, NatsPublisher
from ingestor.services.notification_service import (
  NullNotifier,
  QueuedNotifier,
  TelegramNotifier,
)
from ingestor.settings import Settings, TelegramSettings


def make_ingestion_factory() -> IngestionFactory:
  """Every gateway the ingestor can run. Binance: register it here."""
  factory = IngestionFactory()
  factory.register(GatewayEnum.MT5, build_mt5_ingestion)
  return factory


def make_notifier(config: TelegramSettings) -> QueuedNotifier:
  """Telegram when enabled, otherwise a no-op — always behind a queue."""
  inner = TelegramNotifier(config) if config.ENABLED else NullNotifier()
  return QueuedNotifier(inner, maxsize=config.QUEUE_SIZE)


def make_runtime(config: Settings) -> IngestorRuntime:
  """Wire the production runtime: Telegram, NATS and the registered gateways."""
  notifier = make_notifier(config.telegram)
  connection = NatsConnection(config.nats, notifier, config.app.instance_id)
  publisher = NatsPublisher(connection)
  return IngestorRuntime(
    config,
    factory=make_ingestion_factory(),
    notifier=notifier,
    connection=connection,
    publisher=publisher,
    subject_filter=publisher.subject_filter,
  )
