"""Binance gateway — closed bars from the public kline websocket (any OS)."""

from ingester.core.factory import IngestionContext
from ingester.gateways.crypto.binance.dto import (
  INTERVAL_TIMEFRAMES,
  TIMEFRAME_INTERVALS,
  BinanceKlineDTO,
  stream_name,
)
from ingester.gateways.crypto.binance.history import HttpKlineHistory, KlineHistory
from ingester.gateways.crypto.binance.ingestion import BinanceIngestion
from ingester.gateways.crypto.binance.stream import KlineStream, WebsocketKlineStream
from ingester.settings import BinanceSettings


def build_binance_ingestion(context: IngestionContext) -> BinanceIngestion:
  """Factory builder: wire a real websocket and REST client into the Binance
  ingestion."""
  config = context.config_as(BinanceSettings)
  return BinanceIngestion(
    config=config,
    stream=WebsocketKlineStream(
      ping_interval=config.PING_INTERVAL_SECONDS,
      ping_timeout=config.PING_TIMEOUT_SECONDS,
      idle_timeout=config.IDLE_TIMEOUT_SECONDS,
    ),
    history=HttpKlineHistory(
      url=config.KLINES_URL, timeout=config.HTTP_TIMEOUT_SECONDS
    ),
    **context.ingestion_dependencies(),
  )


__all__ = [
  "INTERVAL_TIMEFRAMES",
  "TIMEFRAME_INTERVALS",
  "BinanceIngestion",
  "BinanceKlineDTO",
  "HttpKlineHistory",
  "KlineHistory",
  "KlineStream",
  "WebsocketKlineStream",
  "build_binance_ingestion",
  "stream_name",
]
