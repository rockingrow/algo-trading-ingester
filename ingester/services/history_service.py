"""
ingester/services/history_service.py — Answers history requests over NATS.

The one place the ingester *listens*. A subscriber that needs the bars from
before it started asks on::

  <rpc_prefix>.history.<gateway>.<symbol>.<timeframe>

and :class:`HistoryResponder` answers on the request's reply subject with a
single :class:`~ingester.schemas.history_schema.HistoryReply`. The contract is
in :mod:`ingester.schemas.history_schema`; what is decided here is transport:

* **No new connection, and nothing inbound.** The subscription rides the
  connection the publisher already holds, which this process dialled. A request
  travels back down it, so the host needs no open port.
* **Core NATS, outside the stream.** ``rpc_prefix`` is never under
  ``NATS_SUBJECT_PREFIX`` (the settings refuse it), so JetStream neither stores
  a request nor answers it with its own acknowledgement.
* **A queue group per gateway.** Two ingesters running the same gateway — a
  fail-over pair — share ``<rpc_prefix>-history-<gateway>``, so one of them
  answers a request rather than both.
* **One request at a time per gateway.** nats-py runs a subscription's callback
  serially, which is what a single venue session wants anyway.
* **One message, sized to fit.** A reply larger than the server's
  ``max_payload`` could not be sent at all, so the oldest bars are dropped
  until it fits and the reply says ``truncated``.
* **Every request with a reply subject is answered**, including the ones that
  cannot be served: an ``error`` reply lets the caller tell "no" from "nobody
  home", and a caller that hears nothing retries on a timeout it cannot
  distinguish from an outage.

It also **announces itself**: once the subscriptions are in place, and again
after every reconnect, each gateway publishes an
:class:`~ingester.schemas.history_schema.OnlineAnnouncement` on
``<rpc_prefix>.online.<gateway>``. That is what lets a subscriber start
independently and react to the ingester arriving — check its windows, ask for
the short ones — instead of asking on a timer. The announcement is flushed
only after the subscriptions are, so a request it provokes always finds a
responder.

A subscription survives a reconnect — nats-py re-establishes it — so the only
thing done on one is to announce again. The exception is a NATS that was down
at start-up, when there is nothing to re-establish: the first connect that
works subscribes then, so a late NATS does not leave the ingester publishing
bars and answering no request until a restart.
"""

from __future__ import annotations

import asyncio
from typing import Any

from nats.aio.msg import Msg
from pydantic import ValidationError

from ingester.core.errors import HistoryUnavailableError, UnknownSymbolError
from ingester.interfaces.ingestion_protocol import Ingestion
from ingester.logger import get_logger
from ingester.schemas.history_schema import (
  HISTORY_TOKEN,
  ONLINE_TOKEN,
  HistoryErrorCode,
  HistoryReply,
  HistoryRequest,
  OnlineAnnouncement,
)
from ingester.services.nats_service import NatsConnection
from ingester.settings import NatsSettings

log = get_logger(__name__)

#: Bytes kept free in a reply for the protocol line and the reply subject, so a
#: payload sized exactly to ``max_payload`` is never the one that is refused.
_PAYLOAD_HEADROOM = 4096


class HistoryResponder:
  """Subscribes once per running gateway and answers its history requests."""

  def __init__(self, connection: NatsConnection, config: NatsSettings) -> None:
    self._connection = connection
    self._config = config
    self._subscriptions: list[Any] = []
    self._ingestions: list[Ingestion] = []
    connection.on_reconnect(self._on_connected)

  def online_subject_for(self, ingestion: Ingestion) -> str:
    """Where *ingestion*'s gateway says it is there to be asked."""
    return f"{self._config.rpc_prefix}.{ONLINE_TOKEN}.{ingestion.gateway.value}"

  def subject_for(self, ingestion: Ingestion) -> str:
    """Every history request addressed to *ingestion*'s gateway."""
    return f"{self._config.rpc_prefix}.{HISTORY_TOKEN}.{ingestion.gateway.value}.>"

  def queue_for(self, ingestion: Ingestion) -> str:
    return f"{self._config.rpc_prefix}-{HISTORY_TOKEN}-{ingestion.gateway.value}"

  async def start(self, ingestions: list[Ingestion]) -> None:
    """Listen for each gateway's requests. Raises when NATS is not connected.

    The gateways are remembered *before* the first subscribe, so a start that
    failed on an unconnected NATS can be replayed from the reconnect hook with
    the same set.
    """
    self._ingestions = list(ingestions)
    for ingestion in ingestions:
      subject = self.subject_for(ingestion)
      queue = self.queue_for(ingestion)

      async def handle(message: Msg, ingestion: Ingestion = ingestion) -> None:
        await self._on_request(ingestion, message)

      subscription = await self._connection.nc.subscribe(
        subject, queue=queue, cb=handle
      )
      self._subscriptions.append(subscription)
      log.info("History requests served on %s (queue=%s)", subject, queue)
    # The server has to know about the subscriptions before anyone is told to
    # use them, or the first request an announcement provokes finds nobody.
    await self._connection.nc.flush()
    await self.announce("started")

  async def announce(self, reason: str = "started") -> None:
    """Tell subscribers each gateway is connected and answering requests."""
    for ingestion in self._ingestions:
      announcement = OnlineAnnouncement(
        source=ingestion.source,
        symbols=ingestion.symbols,
        timeframes=ingestion.timeframes,
        reason=reason,
      )
      subject = self.online_subject_for(ingestion)
      await self._connection.nc.publish(
        subject, announcement.model_dump_json().encode()
      )
      log.info(
        "Announced %s online (%s) on %s: symbols=%s timeframes=%s",
        ingestion.gateway.value,
        reason,
        subject,
        ",".join(announcement.symbols),
        ",".join(timeframe.value for timeframe in announcement.timeframes),
      )

  async def _on_connected(self) -> None:
    """Re-announce after a reconnect — or subscribe, if that never happened.

    nats-py re-establishes a subscription itself, so a reconnect only has to
    announce. A connection that was **down at start-up** is the other case:
    :meth:`start` raised on it and there is nothing subscribed, so the first
    connect that works has to do what start-up could not. Without this, the
    ingester would publish bars on a late connection and still answer no
    history request until it was restarted.
    """
    if self._subscriptions:
      await self.announce("reconnected")
      return
    if not self._ingestions:
      return
    log.info("NATS is up: subscribing to history requests after a failed start")
    await self.start(self._ingestions)

  async def stop(self) -> None:
    self._ingestions = []
    subscriptions, self._subscriptions = self._subscriptions, []
    for subscription in subscriptions:
      try:
        await subscription.unsubscribe()
      except Exception as exc:
        # Shutting down anyway; the connection's own close ends it for good.
        log.debug("Unsubscribing from history requests failed: %s", exc)

  # ── One request ───────────────────────────────────────────────────

  async def _on_request(self, ingestion: Ingestion, message: Msg) -> None:
    if not message.reply:
      # Published rather than requested: there is nowhere to send an answer.
      log.warning(
        "Ignoring a history request on %s with no reply subject", message.subject
      )
      return
    try:
      reply = await self._answer(ingestion, message)
      await message.respond(self._encode(reply))
    except asyncio.CancelledError:
      raise
    except Exception:
      # Nothing may escape: nats-py would log it and the caller would wait out
      # its whole timeout for an answer that is not coming.
      log.exception("History request on %s could not be answered", message.subject)

  async def _answer(self, ingestion: Ingestion, message: Msg) -> HistoryReply:
    try:
      request = HistoryRequest.model_validate_json(message.data)
    except ValidationError as exc:
      problem = exc.errors()[0]
      field = ".".join(str(part) for part in problem["loc"]) or "body"
      log.warning("Unreadable history request on %s: %s", message.subject, exc)
      return HistoryReply.failure(
        HistoryErrorCode.BAD_REQUEST,
        f"{field}: {problem['msg']}",
        source=ingestion.source,
      )

    count = min(request.count, self._config.HISTORY_MAX_BARS)
    log.info(
      "History request %s: %s %s × %d from %s",
      request.request_id or "-",
      request.symbol,
      request.timeframe.value,
      request.count,
      message.subject,
    )
    try:
      symbol, bars = await asyncio.wait_for(
        ingestion.fetch_history(request.symbol, request.timeframe, count),
        timeout=self._config.HISTORY_TIMEOUT,
      )
    except UnknownSymbolError as exc:
      return self._refuse(ingestion, request, HistoryErrorCode.UNKNOWN_SYMBOL, str(exc))
    except HistoryUnavailableError as exc:
      return self._refuse(ingestion, request, HistoryErrorCode.UNAVAILABLE, str(exc))
    except TimeoutError:
      return self._refuse(
        ingestion,
        request,
        HistoryErrorCode.TIMEOUT,
        f"{ingestion.gateway.value} did not answer within "
        f"{self._config.HISTORY_TIMEOUT:.0f}s",
      )
    except Exception as exc:
      log.exception(
        "History request %s for %s %s failed",
        request.request_id or "-",
        request.symbol,
        request.timeframe.value,
      )
      return self._refuse(
        ingestion, request, HistoryErrorCode.INTERNAL, f"{type(exc).__name__}: {exc}"
      )

    return HistoryReply(
      status="ok",
      request_id=request.request_id,
      source=ingestion.source,
      symbol=symbol,
      timeframe=request.timeframe,
      requested=request.count,
      truncated=count < request.count,
      bars=bars,
    )

  def _refuse(
    self,
    ingestion: Ingestion,
    request: HistoryRequest,
    code: HistoryErrorCode,
    message: str,
  ) -> HistoryReply:
    log.warning(
      "History request %s for %s %s refused (%s): %s",
      request.request_id or "-",
      request.symbol,
      request.timeframe.value,
      code.value,
      message,
    )
    return HistoryReply.failure(code, message, request=request, source=ingestion.source)

  # ── Fitting one message ───────────────────────────────────────────

  def _encode(self, reply: HistoryReply) -> bytes:
    """The reply as JSON, with its oldest bars dropped until it fits.

    The newest bars are the ones kept: a window is read from its newest end,
    and a caller that got fewer than it asked for can still decide whether
    what it holds is enough.
    """
    data = reply.model_dump_json().encode()
    budget = self._payload_budget()
    while len(data) > budget and len(reply.bars) > 1:
      # Proportional, then a margin: one pass is nearly always enough, and the
      # loop is there for the reply whose bars are not all the same size.
      keep = max(1, int(len(reply.bars) * budget / len(data) * 0.95))
      log.warning(
        "History reply %s is %d bytes, over the %d the server takes — keeping the "
        "newest %d of %d bar(s)",
        reply.request_id or "-",
        len(data),
        budget,
        keep,
        len(reply.bars),
      )
      reply = reply.model_copy(update={"bars": reply.bars[-keep:], "truncated": True})
      data = reply.model_dump_json().encode()
    return data

  def _payload_budget(self) -> int:
    return max(1, int(self._connection.nc.max_payload) - _PAYLOAD_HEADROOM)
