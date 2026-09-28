"""
ingestor/gateways/mt5/dto.py — MT5 rate record → canonical :class:`Bar`.

The one place that knows what an MT5 bar looks like. ``copy_rates_*`` returns
records with these fields:

=============  =====================================================
``time``       bar **open** time, seconds since epoch in *server* time
``open`` …     OHLC prices
``tick_volume``  ticks in the bar (the only volume forex/CFD has)
``spread``     spread in points
``real_volume``  exchange volume (0 unless the symbol is exchange-traded)
=============  =====================================================

MT5 stamps ``time`` with the trade server's wall clock written as if it were
UTC — most brokers run GMT+2/+3 — so it is re-read in ``server_timezone`` and
converted to real UTC here. Get that zone wrong and every bar is hours off.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from functools import lru_cache
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field

from ingestor.gateways.mt5.terminal import RateRecord
from ingestor.schemas.enums import Timeframe
from ingestor.schemas.market_event_schema import Bar

_EPOCH = datetime(1970, 1, 1)


@lru_cache(maxsize=16)
def _zone(name: str) -> ZoneInfo:
  return ZoneInfo(name)


def server_time_to_utc(server_epoch: int, server_timezone: str) -> datetime:
  """Interpret an MT5 server-clock epoch in *server_timezone* and return UTC."""
  wall_clock = _EPOCH + timedelta(seconds=server_epoch)
  return wall_clock.replace(tzinfo=_zone(server_timezone)).astimezone(UTC)


class Mt5RateDTO(BaseModel):
  """One validated MT5 rate record, plus the zone its clock is in."""

  model_config = ConfigDict(frozen=True, extra="ignore", allow_inf_nan=False)

  time: int = Field(ge=0)
  open: float
  high: float
  low: float
  close: float
  tick_volume: int = Field(default=0, ge=0)
  spread: int = Field(default=0, ge=0)
  real_volume: int = Field(default=0, ge=0)
  server_timezone: str = "UTC"

  @classmethod
  def from_record(cls, record: RateRecord, server_timezone: str) -> Mt5RateDTO:
    return cls.model_validate({**record, "server_timezone": server_timezone})

  def to_bar(self, timeframe: Timeframe) -> Bar:
    open_time = server_time_to_utc(self.time, self.server_timezone)
    return Bar(
      open_time=open_time,
      close_time=open_time + timeframe.delta,
      open=self.open,
      high=self.high,
      low=self.low,
      close=self.close,
      # Exchange volume when the symbol has one, else ticks — the only
      # activity measure a forex/CFD bar carries.
      volume=float(self.real_volume or self.tick_volume),
      tick_count=self.tick_volume,
      spread=float(self.spread),
    )
