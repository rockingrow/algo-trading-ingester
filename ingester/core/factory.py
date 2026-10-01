"""
ingester/core/factory.py — Builds gateway ingestions by name.

The app never imports a concrete gateway: it asks the factory for whatever the
market files enable. Adding a venue is one ``register`` call in
``ingester/providers.py`` — the app, the core and the other gateways stay
untouched (open/closed).

One builder serves every market: the per-gateway settings travel in the
:class:`IngestionContext`, so the same ``[binance]`` builder can be asked for
once per market that enables it.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from ingester.core.errors import GatewayNotRegisteredError
from ingester.interfaces.ingestion_protocol import Ingestion
from ingester.interfaces.notifier_protocol import Notifier
from ingester.interfaces.publisher_protocol import EventPublisher
from ingester.schemas.enums import GatewayEnum
from ingester.settings import GatewaySettings, Settings

ConfigT = TypeVar("ConfigT", bound=GatewaySettings)


@dataclass(frozen=True)
class IngestionContext:
  """Shared dependencies handed to every gateway builder, plus the settings of
  the one gateway being built."""

  settings: Settings
  publisher: EventPublisher
  notifier: Notifier
  #: The ``[gateway]`` table this ingestion is built from, already validated
  #: into the gateway's own ``GatewaySettings`` subclass. It carries the market
  #: the table was read from, so a builder never has to be told twice.
  gateway_config: GatewaySettings

  def config_as(self, expected: type[ConfigT]) -> ConfigT:
    """The gateway's settings, narrowed to the class its builder needs.

    A mismatch means the registry in ``settings.GATEWAY_SETTINGS`` and the
    builder in ``providers.py`` disagree — a wiring bug, caught here instead of
    as an ``AttributeError`` on the first poll.
    """
    if not isinstance(self.gateway_config, expected):
      raise TypeError(
        f"Expected {expected.__name__} for this gateway, got "
        f"{type(self.gateway_config).__name__}"
      )
    return self.gateway_config


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
