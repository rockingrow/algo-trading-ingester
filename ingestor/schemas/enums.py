"""
ingestor/schemas/enums.py — Vocabulary shared by every gateway.

These enums are part of the wire contract: their *values* end up in NATS
subjects and JSON payloads read by ``qte-ingest``, so renaming a value is a
breaking change for every subscriber.
"""

from __future__ import annotations

from datetime import timedelta
from enum import StrEnum


class GatewayEnum(StrEnum):
  """Upstream market-data source an event came from."""

  MT5 = "mt5"
  BINANCE = "binance"


class MarketEnum(StrEnum):
  """Asset class of the instrument, so a subscriber can route without a lookup."""

  FOREX = "forex"
  CFD = "cfd"
  CRYPTO = "crypto"


class EventTypeEnum(StrEnum):
  """Kind of market event carried by an envelope. Doubles as a subject token."""

  BAR_CLOSED = "bar.closed"


class Timeframe(StrEnum):
  """Canonical bar timeframes — the same labels ``quant-trading-engine`` speaks.

  Each gateway maps its own spelling onto these (MT5 ``TIMEFRAME_M15``, Binance
  ``"15m"``) so nothing gateway-specific leaks past the DTO layer.
  """

  M1 = "M1"
  M5 = "M5"
  M15 = "M15"
  M30 = "M30"
  H1 = "H1"
  H4 = "H4"
  D1 = "D1"
  W1 = "W1"

  @property
  def seconds(self) -> int:
    return _TIMEFRAME_SECONDS[self]

  @property
  def delta(self) -> timedelta:
    return timedelta(seconds=self.seconds)


_TIMEFRAME_SECONDS: dict[Timeframe, int] = {
  Timeframe.M1: 60,
  Timeframe.M5: 300,
  Timeframe.M15: 900,
  Timeframe.M30: 1800,
  Timeframe.H1: 3600,
  Timeframe.H4: 14400,
  Timeframe.D1: 86400,
  Timeframe.W1: 604800,
}


class ServiceStatusEnum(StrEnum):
  """Lifecycle of the whole ingestor process."""

  RUNNING = "running"
  STOPPED = "stopped"
  FAILED = "failed"


class GatewayStatusEnum(StrEnum):
  """Lifecycle of one gateway ingestion."""

  IDLE = "idle"
  STARTING = "starting"
  RUNNING = "running"
  DISCONNECTED = "disconnected"
  STOPPED = "stopped"
  FAILED = "failed"
