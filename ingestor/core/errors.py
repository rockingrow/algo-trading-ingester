"""
ingestor/core/errors.py — Exceptions the core understands.

A gateway raises :class:`GatewayConnectionError` when the *venue* is unreachable
(terminal closed, socket dropped). The core turns that into a DISCONNECTED
status, an operator notification and a reconnect loop. Any other exception is
treated as a bug in one poll and logged without tearing the connection down.
"""


class IngestorError(Exception):
  """Base class for ingestor errors."""


class GatewayConnectionError(IngestorError):
  """The upstream venue cannot be reached; the core will reconnect."""


class GatewayNotRegisteredError(IngestorError):
  """No builder is registered for the requested gateway."""
