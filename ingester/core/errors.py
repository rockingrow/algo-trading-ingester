"""
ingester/core/errors.py — Exceptions the core understands.

A gateway raises :class:`GatewayConnectionError` when the *venue* is unreachable
(terminal closed, socket dropped). The core turns that into a DISCONNECTED
status, an operator notification and a reconnect loop. Any other exception is
treated as a bug in one poll and logged without tearing the connection down.
"""


class IngesterError(Exception):
  """Base class for ingester errors."""


class GatewayConnectionError(IngesterError):
  """The upstream venue cannot be reached; the core will reconnect."""


class GatewayNotRegisteredError(IngesterError):
  """No builder is registered for the requested gateway."""


class SymbolResolutionError(IngesterError):
  """A configured symbol maps to no broker instrument, or to several."""
