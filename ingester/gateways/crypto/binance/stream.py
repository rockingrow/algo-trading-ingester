"""
ingester/gateways/crypto/binance/stream.py — The seam between our code and the socket.

:class:`KlineStream` is the slice of a websocket the Binance gateway needs:
open a combined stream, read one JSON frame, close. :class:`WebsocketKlineStream`
implements it over the ``websockets`` package; tests implement it with a fake,
so the bar-close logic in ``ingestion.py`` runs without a network.

Frames come back as plain ``dict``s, so nothing past this module knows that
Binance wraps a combined stream in ``{"stream": …, "data": …}``.

Every failure the socket can produce is raised as
:class:`~ingester.core.errors.GatewayConnectionError`: the ingestion's job is to
reconnect, not to tell a handshake timeout from a half-closed TCP connection.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from ingester.core.errors import GatewayConnectionError
from ingester.logger import get_logger

log = get_logger(__name__)


class KlineStream(Protocol):
  """What the Binance gateway needs from a websocket connection."""

  async def open(self, url: str, streams: Sequence[str]) -> None:
    """Connect and subscribe to *streams*. Raise ``GatewayConnectionError``."""
    ...

  async def receive(self) -> Mapping[str, Any]:
    """Next decoded frame. Raise ``GatewayConnectionError`` when the socket is
    gone or has gone quiet."""
    ...

  async def close(self) -> None:
    """Close the socket. Must be safe to call when not connected."""
    ...


class WebsocketKlineStream:
  """:class:`KlineStream` over the ``websockets`` package.

  Binance builds the subscription into the URL of its combined-stream endpoint
  (``/stream?streams=a/b/c``), so there is no subscribe frame to send and no
  acknowledgement to wait for.
  """

  def __init__(
    self,
    *,
    ping_interval: float,
    ping_timeout: float,
    idle_timeout: float,
    open_timeout: float = 10.0,
  ) -> None:
    self._ping_interval = ping_interval
    self._ping_timeout = ping_timeout
    self._idle_timeout = idle_timeout
    self._open_timeout = open_timeout
    self._socket: ClientConnection | None = None

  async def open(self, url: str, streams: Sequence[str]) -> None:
    target = f"{url.rstrip('/')}?streams={'/'.join(streams)}"
    try:
      self._socket = await connect(
        target,
        ping_interval=self._ping_interval,
        ping_timeout=self._ping_timeout,
        open_timeout=self._open_timeout,
      )
    except (WebSocketException, OSError, TimeoutError) as exc:
      raise GatewayConnectionError(
        f"Binance websocket {url}: {type(exc).__name__}: {exc}"
      ) from exc
    log.info("Binance websocket open: %d stream(s)", len(streams))

  async def receive(self) -> Mapping[str, Any]:
    socket = self._socket
    if socket is None:
      raise GatewayConnectionError("Binance websocket is not open")
    try:
      # A websocket can stay "open" long after the peer stopped sending. The
      # ping keep-alive covers most of it; this bounds the rest, so a silent
      # socket is dropped instead of starving every stream on it.
      frame = await asyncio.wait_for(socket.recv(), timeout=self._idle_timeout)
    except TimeoutError:
      raise GatewayConnectionError(
        f"Binance websocket silent for {self._idle_timeout:.0f}s"
      ) from None
    except (WebSocketException, OSError) as exc:
      raise GatewayConnectionError(
        f"Binance websocket: {type(exc).__name__}: {exc}"
      ) from exc
    if isinstance(frame, bytes):
      frame = frame.decode("utf-8")
    try:
      payload = json.loads(frame)
    except ValueError as exc:
      raise GatewayConnectionError(f"Binance sent a non-JSON frame: {exc}") from None
    if not isinstance(payload, dict):
      raise GatewayConnectionError(f"Binance sent an unexpected frame: {frame[:120]!r}")
    return payload

  async def close(self) -> None:
    socket = self._socket
    if socket is None:
      return
    try:
      await socket.close()
    except Exception:
      # Closing a socket that is already gone is not worth a reconnect.
      log.debug("Binance websocket close failed", exc_info=True)
    # Cleared only once the close has actually run: cancelled midway, the
    # socket is still ours to close, and a later ``close`` must not no-op.
    self._socket = None
