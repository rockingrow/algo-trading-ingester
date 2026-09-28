from __future__ import annotations

from typing import Protocol, runtime_checkable

from ingestor.schemas.enums import Timeframe
from ingestor.schemas.market_event_schema import Bar


@runtime_checkable
class BarDTO(Protocol):
  """A gateway's raw bar, validated, that knows how to become a canonical ``Bar``.

  Each gateway owns one DTO for its own payload shape (an MT5 rate record, a
  Binance kline). The DTO is the *only* place that knows that shape; past
  ``to_bar`` everything speaks the canonical schema.
  """

  def to_bar(self, timeframe: Timeframe) -> Bar: ...
