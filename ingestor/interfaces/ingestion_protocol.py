from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from ingestor.schemas.enums import GatewayEnum, GatewayStatusEnum


@runtime_checkable
class Ingestion(Protocol):
  """What the app needs from a running gateway — start it, stop it, inspect it."""

  @property
  def gateway(self) -> GatewayEnum: ...

  @property
  def status(self) -> GatewayStatusEnum: ...

  async def start(self) -> None: ...

  async def stop(self) -> None: ...

  def snapshot(self) -> dict[str, Any]: ...
