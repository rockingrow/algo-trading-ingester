"""
ingester/gateways/crypto/binance/history.py — The seam between our code and
Binance's REST klines endpoint.

The websocket only ever delivers bars that close *while it is connected*, so
the bars that closed before this process started are not on it. When a
subscriber asks for them they are read over REST by :class:`KlineHistory` — the
slice of the endpoint a history request needs: ``limit`` klines for one
(symbol, interval), newest last.

:class:`HttpKlineHistory` implements it over ``httpx``; tests implement it with
a fake, so the history logic in ``ingestion.py`` runs without a network.

Rows come back as Binance sends them — plain arrays — and
:meth:`~ingester.gateways.crypto.binance.dto.BinanceKlineDTO.from_rest_row` is
the only place that knows what sits at which index.

Every failure is raised as
:class:`~ingester.core.errors.GatewayConnectionError`: the caller's job is to
decide whether a request is worth retrying, not to tell a 429 from a DNS
failure.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

import httpx

from ingester.core.errors import GatewayConnectionError
from ingester.logger import get_logger

log = get_logger(__name__)


class KlineHistory(Protocol):
  """What the Binance gateway needs from the REST klines endpoint."""

  async def klines(
    self, *, symbol: str, interval: str, limit: int
  ) -> Sequence[Sequence[Any]]:
    """The newest *limit* klines for a stream, oldest first — including the bar
    still forming, which Binance always returns last.

    Raise ``GatewayConnectionError`` when the endpoint cannot be read.
    """
    ...


class HttpKlineHistory:
  """:class:`KlineHistory` over ``httpx``.

  A client per call rather than one kept open: history is asked for a handful
  of times when a subscriber starts, and a connection pool that lives for the
  life of the process would sit idle for the rest of it.
  """

  def __init__(self, *, url: str, timeout: float) -> None:
    self._url = url
    self._timeout = timeout

  async def klines(
    self, *, symbol: str, interval: str, limit: int
  ) -> Sequence[Sequence[Any]]:
    params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
    try:
      async with httpx.AsyncClient(timeout=self._timeout) as client:
        response = await client.get(self._url, params=params)
        response.raise_for_status()
        payload = response.json()
    except httpx.HTTPStatusError as exc:
      # Binance answers a bad symbol or a rate limit with a JSON body naming
      # the reason; the status alone would send an operator guessing.
      raise GatewayConnectionError(
        f"Binance klines {symbol} {interval}: HTTP "
        f"{exc.response.status_code} {exc.response.text[:200]}"
      ) from None
    except (httpx.HTTPError, ValueError) as exc:
      raise GatewayConnectionError(
        f"Binance klines {symbol} {interval}: {type(exc).__name__}: {exc}"
      ) from None
    if not isinstance(payload, list):
      raise GatewayConnectionError(
        f"Binance klines {symbol} {interval}: expected a list, got "
        f"{type(payload).__name__}"
      )
    log.debug("Binance klines %s %s: %d row(s)", symbol, interval, len(payload))
    return payload
