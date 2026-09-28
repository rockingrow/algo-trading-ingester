"""
ingester/core/factory.py — Builds gateway ingestions by name.

The app never imports a concrete gateway: it asks the factory for whatever
``APP_GATEWAYS`` lists. Adding a venue is one ``register`` call in
``ingester/providers.py`` — the app, the core and the other gateways stay
untouched (open/closed).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from ingester.core.errors import GatewayNotRegisteredError
from ingester.interfaces.ingestion_protocol import Ingestion
from ingester.interfaces.notifier_protocol import Notifier
from ingester.interfaces.publisher_protocol import EventPublisher
from ingester.schemas.enums import GatewayEnum
from ingester.settings import Settings


@dataclass(frozen=True)
class IngestionContext:
  """Shared dependencies handed to every gateway builder."""

  settings: Settings
  publisher: EventPublisher
  notifier: Notifier


IngestionBuilder = Callable[[IngestionContext], Ingestion]


class IngestionFactory:
  """Registry of ``gateway → builder``."""

  def __init__(self) -> None:
    self._builders: dict[GatewayEnum, IngestionBuilder] = {}

  @property
  def registered(self) -> list[GatewayEnum]:
    return list(self._builders)

  def register(self, gateway: GatewayEnum, builder: IngestionBuilder) -> None:
    if gateway in self._builders:
      raise ValueError(f"Gateway {gateway.value!r} is already registered")
    self._builders[gateway] = builder

  def create(self, gateway: GatewayEnum, context: IngestionContext) -> Ingestion:
    try:
      builder = self._builders[gateway]
    except KeyError:
      known = ", ".join(g.value for g in self._builders) or "none"
      raise GatewayNotRegisteredError(
        f"No ingestion registered for gateway {gateway.value!r} (registered: {known})"
      ) from None
    return builder(context)

  def create_all(
    self, gateways: Iterable[GatewayEnum], context: IngestionContext
  ) -> list[Ingestion]:
    # dict.fromkeys de-duplicates while keeping the configured order.
    return [self.create(gateway, context) for gateway in dict.fromkeys(gateways)]
