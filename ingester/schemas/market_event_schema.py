"""
ingester/schemas/market_event_schema.py — The canonical wire contract.

Every gateway (MT5 today, Binance next) turns its raw payload into these models
through its own DTO, so a subscriber such as ``qte-ingest`` decodes exactly one
shape no matter where the data came from.

Layout of a message::

  MarketEvent            ← envelope: id, type, version, source, timestamps
  └── BarClosedEvent     ← + symbol, timeframe, bar
      └── Bar            ← OHLCV of one completed bar, in UTC

Adding a new kind of event (ticks, trades, order-book snapshots) means adding a
new ``MarketEvent`` subclass with its own ``event_type`` and ``subject_tokens``;
nothing in the publisher or the core ingestion has to change.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from ingester.schemas.enums import EventTypeEnum, GatewayEnum, MarketEnum, Timeframe

#: Bumped on any breaking change to the payload shape. Subscribers should
#: reject (or route aside) a major version they do not understand.
SCHEMA_VERSION = "2.0"

# NATS treats ``.`` as a token separator and ``*``/``>`` as wildcards, and
# brokers love symbol suffixes such as ``XAUUSD.m`` — so a symbol is reduced to
# a safe token for the *subject* only. The payload always keeps it verbatim.
_UNSAFE_SUBJECT_CHARS = re.compile(r"[^A-Za-z0-9_-]")


def subject_token(value: str) -> str:
  """Make *value* usable as a single NATS subject token."""
  return _UNSAFE_SUBJECT_CHARS.sub("_", value.strip())


def utcnow() -> datetime:
  return datetime.now(UTC)


def _as_utc(value: datetime) -> datetime:
  """Reject naive datetimes rather than guess their zone."""
  if value.tzinfo is None:
    raise ValueError("datetime must be timezone-aware (UTC)")
  return value.astimezone(UTC)


class Bar(BaseModel):
  """One completed OHLCV bar, keyed by its **open** time (UTC).

  Fields that only some venues provide are optional rather than zero-filled, so
  "unknown" is never confused with "zero": MT5 forex has ``spread`` and a tick
  count but no real volume, Binance has quote volume and a trade count.
  """

  model_config = ConfigDict(frozen=True, allow_inf_nan=False)

  open_time: datetime
  close_time: datetime
  open: float
  high: float
  low: float
  close: float
  #: Traded volume when the venue reports one, otherwise the tick volume.
  volume: float = Field(default=0.0, ge=0)
  #: Number of ticks (MT5) or trades (Binance) that formed the bar.
  tick_count: int | None = Field(default=None, ge=0)
  #: Quote-asset volume (crypto venues).
  quote_volume: float | None = Field(default=None, ge=0)
  #: Spread in points at the bar (MT5).
  spread: float | None = Field(default=None, ge=0)

  @field_validator("open_time", "close_time")
  @classmethod
  def _utc(cls, value: datetime) -> datetime:
    return _as_utc(value)

  @model_validator(mode="after")
  def _check_shape(self) -> Bar:
    if self.close_time <= self.open_time:
      raise ValueError("close_time must be after open_time")
    if self.high < max(self.open, self.close, self.low):
      raise ValueError("high must be >= open, close and low")
    if self.low > min(self.open, self.close, self.high):
      raise ValueError("low must be <= open, close and high")
    return self


class EventSource(BaseModel):
  """Where an event came from — enough for a subscriber to trace it back."""

  model_config = ConfigDict(frozen=True)

  gateway: GatewayEnum
  market: MarketEnum
  #: Identifies this ingester process (several may run on different VPS).
  ingester_id: str
  #: Venue-side origin, e.g. the MT5 trade server name. Optional.
  venue: str | None = None


class MarketEvent(BaseModel):
  """Envelope shared by every event the ingester publishes."""

  model_config = ConfigDict(frozen=True)

  schema_version: str = SCHEMA_VERSION
  #: Deterministic id — the same bar always yields the same id, so it doubles as
  #: the JetStream ``Nats-Msg-Id`` and a subscriber-side de-duplication key.
  event_id: str
  event_type: EventTypeEnum
  source: EventSource
  emitted_at: datetime = Field(default_factory=utcnow)

  @property
  def subject_tokens(self) -> tuple[str, ...]:
    """Subject tokens below the configured prefix. Overridden per event type."""
    raise NotImplementedError


class BarClosedEvent(MarketEvent):
  """Published once per completed bar per (gateway, symbol, timeframe)."""

  event_type: Literal[EventTypeEnum.BAR_CLOSED] = EventTypeEnum.BAR_CLOSED
  symbol: str
  timeframe: Timeframe
  bar: Bar

  @classmethod
  def create(
    cls, *, source: EventSource, symbol: str, timeframe: Timeframe, bar: Bar
  ) -> BarClosedEvent:
    """Build the event with its deterministic id."""
    open_epoch = int(bar.open_time.timestamp())
    event_id = f"{source.gateway.value}:{symbol}:{timeframe.value}:{open_epoch}"
    return cls(
      event_id=event_id,
      source=source,
      symbol=symbol,
      timeframe=timeframe,
      bar=bar,
    )

  @property
  def subject_tokens(self) -> tuple[str, ...]:
    """``bar.closed.<gateway>.<symbol>.<timeframe>``."""
    return (
      *self.event_type.value.split("."),
      self.source.gateway.value,
      subject_token(self.symbol),
      self.timeframe.value,
    )
