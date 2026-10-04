"""
ingester/providers.py — Composition root: builds concrete services from settings.

This is the only module that knows which implementation backs each interface,
and the only place a new gateway has to be registered.
"""

from __future__ import annotations

from ingester.core.factory import IngestionFactory
from ingester.gateways.crypto.binance import build_binance_ingestion
from ingester.gateways.forex.mt5 import build_mt5_ingestion
from ingester.runtime import IngesterRuntime
from ingester.schemas.enums import GatewayEnum
from ingester.services.nats_service import NatsConnection, NatsPublisher
from ingester.services.notification_service import (
  NullNotifier,
  QueuedNotifier,
  TelegramLogHandler,
  TelegramNotifier,
)
from ingester.settings import Settings, TelegramSettings


def make_ingestion_factory() -> IngestionFactory:
  """Every gateway the ingester can run. A new venue: register it here."""
  factory = IngestionFactory()
  factory.register(GatewayEnum.MT5, build_mt5_ingestion)
  factory.register(GatewayEnum.BINANCE, build_binance_ingestion)
  return factory


def make_notifier(config: TelegramSettings) -> QueuedNotifier:
  """Telegram when enabled, otherwise a no-op — always behind a queue."""
  inner = TelegramNotifier(config) if config.ENABLED else NullNotifier()
  return QueuedNotifier(inner, maxsize=config.QUEUE_SIZE)


def make_error_notifier(config: TelegramSettings) -> QueuedNotifier | None:
  """The separate chat ERROR records go to, or ``None`` when switched off.

  Its own queue, so a flood of errors cannot delay a lifecycle message — and
  its own token, so the error chat can belong to a different bot.
  """
  if not (config.ENABLED and config.LOG_ERRORS_ENABLED):
    return None
  inner = TelegramNotifier(
    config, bot_token=config.log_bot_token, chat_ids=config.log_chat_ids
  )
  return QueuedNotifier(inner, maxsize=config.QUEUE_SIZE)


def make_runtime(config: Settings) -> IngesterRuntime:
  """Wire the production runtime: Telegram, NATS and the registered gateways."""
  notifier = make_notifier(config.telegram)
  error_notifier = make_error_notifier(config.telegram)
  connection = NatsConnection(config.nats, notifier, config.app.instance_id)
  publisher = NatsPublisher(connection)
  return IngesterRuntime(
    config,
    factory=make_ingestion_factory(),
    notifier=notifier,
    connection=connection,
    publisher=publisher,
    subject_filter=publisher.subject_filter,
    log_notifier=error_notifier,
    log_forwarder=(
      None
      if error_notifier is None
      else TelegramLogHandler(
        error_notifier,
        instance_id=config.app.instance_id,
        dedup_window=config.telegram.LOG_DEDUP_WINDOW,
      )
    ),
  )
