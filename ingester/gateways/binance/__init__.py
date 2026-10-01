"""Binance gateway — closed bars from the public kline websocket (any OS)."""

from ingester.core.factory import IngestionContext
from ingester.gateways.binance.dto import (
  INTERVAL_TIMEFRAMES,
  TIMEFRAME_INTERVALS,
  BinanceKlineDTO,
  stream_name,
)
from ingester.gateways.binance.ingestion import BinanceIngestion
from ingester.gateways.binance.stream import KlineStream, WebsocketKlineStream
from ingester.settings import BinanceSettings


def build_binance_ingestion(context: IngestionContext) -> BinanceIngestion:
  """Factory builder: wire a real websocket into the Binance ingestion."""
  config = context.config_as(BinanceSettings)
  return BinanceIngestion(
    config=config,
    stream=WebsocketKlineStream(
      ping_interval=config.PING_INTERVAL_SECONDS,
      ping_timeout=config.PING_TIMEOUT_SECONDS,
      idle_timeout=config.IDLE_TIMEOUT_SECONDS,
    ),
    publisher=context.publisher,
    notifier=context.notifier,
    instance_id=context.settings.app.instance_id,
  )


__all__ = [
  "INTERVAL_TIMEFRAMES",
  "TIMEFRAME_INTERVALS",
  "BinanceIngestion",
  "BinanceKlineDTO",
  "KlineStream",
  "WebsocketKlineStream",
  "build_binance_ingestion",
  "stream_name",
]
