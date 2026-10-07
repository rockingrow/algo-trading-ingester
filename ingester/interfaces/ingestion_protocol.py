from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from ingester.schemas.enums import GatewayEnum, GatewayStatusEnum, Timeframe
from ingester.schemas.market_event_schema import Bar, EventSource


@runtime_checkable
class Ingestion(Protocol):
  """What the app needs from a running gateway — start it, stop it, inspect
  it, and ask it for the bars that closed before anyone was listening."""

  @property
  def gateway(self) -> GatewayEnum: ...

  @property
  def status(self) -> GatewayStatusEnum: ...

  async def start(self) -> None: ...

  async def stop(self) -> None: ...

  def snapshot(self) -> dict[str, Any]: ...

  @property
  def source(self) -> EventSource: ...

  @property
  def symbols(self) -> list[str]: ...

  @property
  def timeframes(self) -> list[Timeframe]: ...

  async def fetch_history(
    self, symbol: str, timeframe: Timeframe, count: int
  ) -> tuple[str, list[Bar]]:
    """The configured symbol name and its newest *count* closed bars."""
    ...
