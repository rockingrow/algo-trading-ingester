"""MetaTrader 5 gateway — closed bars from a local MT5 terminal (Windows)."""

from ingester.core.factory import IngestionContext
from ingester.gateways.forex.mt5.dto import Mt5RateDTO
from ingester.gateways.forex.mt5.ingestion import Mt5Ingestion
from ingester.gateways.forex.mt5.terminal import MetaTrader5Terminal, Mt5Terminal
from ingester.settings import Mt5Settings


def build_mt5_ingestion(context: IngestionContext) -> Mt5Ingestion:
  """Factory builder: wire the real terminal into the MT5 ingestion."""
  return Mt5Ingestion(
    config=context.config_as(Mt5Settings),
    terminal=MetaTrader5Terminal(),
    **context.ingestion_dependencies(),
  )


__all__ = [
  "MetaTrader5Terminal",
  "Mt5Ingestion",
  "Mt5RateDTO",
  "Mt5Terminal",
  "build_mt5_ingestion",
]
