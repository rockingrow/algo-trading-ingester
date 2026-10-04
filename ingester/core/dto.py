"""
ingester/core/dto.py — The base every gateway's bar DTO extends.

A DTO is the one place that knows a venue's payload shape. What the DTOs share
is not their fields — an MT5 rate and a Binance kline have none in common — but
how strictly they are read and the single thing they must be able to do: become
a canonical :class:`Bar`. :class:`BaseBarDTO` holds exactly that, and satisfies
the :class:`~ingester.interfaces.dto_protocol.BarDTO` protocol.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from pydantic import BaseModel, ConfigDict

from ingester.schemas.enums import Timeframe
from ingester.schemas.market_event_schema import Bar


class BaseBarDTO(BaseModel, ABC):
  """One validated venue bar that knows how to become a canonical ``Bar``.

  Frozen, because a bar read off a venue is a fact; unknown fields are ignored,
  because venues add fields without notice; and NaN/infinity are refused,
  because a price that is not a number must never reach a strategy. A subclass
  adds to this config (pydantic merges it), it does not restate it.
  """

  model_config = ConfigDict(frozen=True, extra="ignore", allow_inf_nan=False)

  @abstractmethod
  def to_bar(self, timeframe: Timeframe) -> Bar:
    """The canonical bar for this record, with every time in UTC."""
