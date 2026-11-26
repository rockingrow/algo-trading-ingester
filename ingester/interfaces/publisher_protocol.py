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

  async def publish(self, event: MarketEvent) -> bool:
    """Send *event*. ``True`` when the transport took it, ``False`` when it
    acknowledged the write and stored nothing because it had this
    ``event_id`` already — a bar no subscriber will be handed, which must not
    be counted as one that was delivered.

    Raise on a failure the caller may retry; the same event sent twice is
    de-duplicated by ``event_id`` on the far side.
    """
    ...
