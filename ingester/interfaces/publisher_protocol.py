from __future__ import annotations

from typing import Protocol, runtime_checkable

from ingester.schemas.market_event_schema import MarketEvent


@runtime_checkable
class EventPublisher(Protocol):
  """Pushes canonical market events downstream — one way, fire and forget.

  The ingestion core depends on this abstraction only, so the transport (NATS
  today) can be swapped or faked without touching any gateway.
  """

  @property
  def is_connected(self) -> bool: ...

  async def publish(self, event: MarketEvent) -> None: ...
