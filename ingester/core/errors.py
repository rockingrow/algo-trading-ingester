"""
ingester/core/errors.py — Exceptions the core understands.

A gateway raises :class:`GatewayConnectionError` when the *venue* is unreachable
(terminal closed, socket dropped). The core turns that into a DISCONNECTED
status, an operator notification and a reconnect loop. Any other exception is
treated as a bug in one poll and logged without tearing the connection down.

A history request has two errors of its own, because the caller has to tell
"ask again" from "stop asking": :class:`HistoryUnavailableError` and
:class:`UnknownSymbolError`.
"""


class IngesterError(Exception):
  """Base class for ingester errors."""


class GatewayConnectionError(IngesterError):
  """The upstream venue cannot be reached; the core will reconnect."""


class GatewayNotRegisteredError(IngesterError):
  """No builder is registered for the requested gateway."""


class SymbolResolutionError(IngesterError):
  """A configured symbol maps to no broker instrument, or to several."""


class HistoryUnavailableError(IngesterError):
  """A history request cannot be served right now; asking again may work.

  The venue is disconnected, returned nothing, or the gateway serves no
  history at all. Distinct from :class:`UnknownSymbolError`, which no retry
  fixes.
  """


class UnknownSymbolError(IngesterError):
  """A history request named a symbol this gateway is not configured for."""
