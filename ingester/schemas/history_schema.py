"""
ingester/schemas/history_schema.py — The history request/reply contract.

The ingester publishes a bar when it closes and nothing else. A subscriber that
needs the bars *before* it started listening — to fill an indicator window —
asks for them, and gets them in one reply::

  request  →  <rpc_prefix>.history.<gateway>.<symbol>.<timeframe>
              {"schema_version": "1.0.0", "request_id": "…",
               "symbol": "XAUUSD", "timeframe": "M15", "count": 150}

  reply    ←  {"schema_version": "1.0.0", "request_id": "…", "status": "ok",
               "source": {…}, "symbol": "XAUUSD", "timeframe": "M15",
               "requested": 150, "truncated": false, "bars": [Bar, …]}

The subscriber decides which series and how many bars; the ingester holds no
opinion about either. ``bars`` are closed bars only, oldest first, in the same
:class:`~ingester.schemas.market_event_schema.Bar` shape a ``bar.closed`` event
carries, so one decoder reads both.

It is one message on purpose. A window sent as a numbered series of events has
to be reassembled, and every message of it can be lost on its own; a reply
either arrives whole or the request times out and is asked again.

The ingester also says when it is there to be asked
(:class:`OnlineAnnouncement`), so a subscriber reacts to it connecting rather
than polling for it.

A request that cannot be served is still answered — ``status: "error"`` with a
``code`` a caller can branch on — so "the ingester said no" never looks like
"the ingester is not there".
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from ingester.schemas.enums import Timeframe
from ingester.schemas.market_event_schema import (
  Bar,
  EventSource,
  schema_version,
  utcnow,
)

#: The subject token that separates history requests from anything a later
#: request type adds under the same RPC prefix.
HISTORY_TOKEN = "history"
#: The subject token of the announcement below.
ONLINE_TOKEN = "online"


class HistoryErrorCode(StrEnum):
  """Why a history request was refused. Part of the wire contract."""

  #: The request body is not a request this version can read.
  BAD_REQUEST = "bad_request"
  #: The symbol is not one this gateway is configured to ingest.
  UNKNOWN_SYMBOL = "unknown_symbol"
  #: The venue cannot be read right now — disconnected, or it returned nothing.
  #: Worth retrying.
  UNAVAILABLE = "unavailable"
  #: The venue did not answer in time. Worth retrying.
  TIMEOUT = "timeout"
  #: A bug on this side. The log has the traceback.
  INTERNAL = "internal"


class OnlineAnnouncement(BaseModel):
  """ "This gateway is connected and answering history requests", said once.

  Published on ``<rpc_prefix>.online.<gateway>`` when the responder starts
  listening, and again every time the NATS connection is re-established. A
  subscriber waits for it instead of asking on a timer: until it arrives there
  is nobody to ask, and when it does, the subscriber checks which of its
  windows are short and requests exactly those.

  Core NATS, not persisted — a subscriber that was not listening when it was
  said finds out the other way, by asking: with no ingester subscribed NATS
  answers *no responders* at once.
  """

  model_config = ConfigDict(frozen=True)

  schema_version: str = Field(default_factory=schema_version)
  event_type: Literal["ingester.online"] = "ingester.online"
  source: EventSource
  announced_at: datetime = Field(default_factory=utcnow)
  #: What this gateway is configured to ingest — the symbols a history request
  #: may name. Timeframes are the ones it publishes; any other may still be
  #: asked for.
  symbols: list[str]
  timeframes: list[Timeframe]
  #: Why it is being said: the process started, or the connection came back.
  reason: Literal["started", "reconnected"] = "started"


class HistoryRequest(BaseModel):
  """One "give me the newest *count* closed bars of this series" question."""

  model_config = ConfigDict(frozen=True, extra="ignore")

  schema_version: str = Field(default_factory=schema_version)
  #: Echoed on the reply so a caller can match the two in its logs.
  request_id: str = ""
  #: The bare instrument, as the market file spells it (``XAUUSD``).
  symbol: str = Field(min_length=1)
  timeframe: Timeframe
  count: int = Field(ge=1)


class HistoryError(BaseModel):
  model_config = ConfigDict(frozen=True)

  code: HistoryErrorCode
  message: str


class HistoryReply(BaseModel):
  """The answer to one :class:`HistoryRequest`."""

  model_config = ConfigDict(frozen=True)

  schema_version: str = Field(default_factory=schema_version)
  request_id: str = ""
  status: Literal["ok", "error"]
  served_at: datetime = Field(default_factory=utcnow)
  #: Where the bars came from. ``None`` on an error raised before a gateway
  #: was involved.
  source: EventSource | None = None
  symbol: str | None = None
  timeframe: Timeframe | None = None
  #: The ``count`` that was asked for.
  requested: int | None = None
  #: True when fewer bars are returned than the venue could have served: the
  #: request was over this ingester's limit, or the reply would not have fitted
  #: in one NATS message. Fewer bars because the venue has no more history is
  #: not truncation.
  truncated: bool = False
  #: Closed bars, oldest first.
  bars: list[Bar] = Field(default_factory=list)
  error: HistoryError | None = None

  @classmethod
  def failure(
    cls,
    code: HistoryErrorCode,
    message: str,
    *,
    request: HistoryRequest | None = None,
    source: EventSource | None = None,
  ) -> HistoryReply:
    return cls(
      status="error",
      request_id=request.request_id if request else "",
      source=source,
      symbol=request.symbol if request else None,
      timeframe=request.timeframe if request else None,
      requested=request.count if request else None,
      error=HistoryError(code=code, message=message),
    )
