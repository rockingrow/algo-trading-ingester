"""History requests, answered over a fake NATS connection.

What is pinned here is the transport contract: which subject and queue group a
gateway listens on, that every request with a reply subject gets exactly one
answer — the refusals included — and that the answer is one message that fits.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from ingester.core.errors import HistoryUnavailableError, UnknownSymbolError
from ingester.schemas import (
  Bar,
  EventSource,
  GatewayEnum,
  HistoryReply,
  MarketEnum,
  OnlineAnnouncement,
  Timeframe,
)
from ingester.services.history_service import HistoryResponder
from ingester.settings import NatsSettings

OPEN = datetime(2026, 9, 28, 10, 0, tzinfo=UTC)
SOURCE = EventSource(
  gateway=GatewayEnum.MT5, market=MarketEnum.FOREX, ingester_id="test", venue="Fake"
)


def make_bars(count: int) -> list[Bar]:
  return [
    Bar(
      open_time=OPEN + timedelta(minutes=15 * index),
      close_time=OPEN + timedelta(minutes=15 * (index + 1)),
      open=1.0,
      high=2.0,
      low=0.5,
      close=1.5,
      volume=10.0,
      tick_count=10,
    )
    for index in range(count)
  ]


class FakeIngestion:
  """A gateway that answers history from a script."""

  gateway = GatewayEnum.MT5
  source = SOURCE
  symbols = ["XAUUSD", "USOIL"]
  timeframes = [Timeframe.M15]

  def __init__(self, bars: list[Bar] | None = None) -> None:
    self.bars = bars if bars is not None else make_bars(3)
    self.calls: list[tuple[str, Timeframe, int]] = []
    self.error: Exception | None = None
    self.delay = 0.0

  async def fetch_history(self, symbol: str, timeframe: Timeframe, count: int):
    self.calls.append((symbol, timeframe, count))
    if self.delay:
      await asyncio.sleep(self.delay)
    if self.error is not None:
      raise self.error
    return symbol.upper(), self.bars[-count:]


class FakeSubscription:
  def __init__(self) -> None:
    self.unsubscribed = False

  async def unsubscribe(self) -> None:
    self.unsubscribed = True


class FakeNats:
  def __init__(self, max_payload: int = 1_048_576) -> None:
    self.max_payload = max_payload
    self.subscriptions: dict[str, tuple[str, Any, FakeSubscription]] = {}
    #: Everything done on the connection, in order: the order is the contract.
    self.journal: list[str] = []
    self.published: list[tuple[str, bytes]] = []
    #: Raised out of ``subscribe`` while set — a NATS that is not connected.
    self.fail_subscribe: Exception | None = None

  async def subscribe(self, subject: str, queue: str = "", cb: Any = None):
    if self.fail_subscribe is not None:
      raise self.fail_subscribe
    subscription = FakeSubscription()
    self.subscriptions[subject] = (queue, cb, subscription)
    self.journal.append(f"subscribe {subject}")
    return subscription

  async def flush(self) -> None:
    self.journal.append("flush")

  async def publish(self, subject: str, data: bytes) -> None:
    self.published.append((subject, data))
    self.journal.append(f"publish {subject}")


class FakeConnection:
  """The slice of ``NatsConnection`` the responder uses."""

  def __init__(self, nats: FakeNats) -> None:
    self.nc = nats
    self.reconnect_hooks: list[Any] = []

  @property
  def fail_subscribe(self) -> Any:
    return self.nc.fail_subscribe

  @fail_subscribe.setter
  def fail_subscribe(self, error: Any) -> None:
    self.nc.fail_subscribe = error

  def on_reconnect(self, hook: Any) -> None:
    self.reconnect_hooks.append(hook)

  async def reconnect(self) -> None:
    for hook in self.reconnect_hooks:
      await hook()


class FakeMessage:
  def __init__(self, subject: str, data: bytes, reply: str = "_INBOX.1") -> None:
    self.subject = subject
    self.data = data
    self.reply = reply
    self.responses: list[bytes] = []

  async def respond(self, data: bytes) -> None:
    self.responses.append(data)


SUBJECT = "INGESTER_RPC.history.mt5.>"


def make_responder(nats: FakeNats | None = None, **config):
  nats = nats or FakeNats()
  settings = NatsSettings(_env_file=None, SUBJECT_PREFIX="INGESTER", **config)
  return HistoryResponder(FakeConnection(nats), settings), nats


async def ask(nats: FakeNats, body: Any, *, reply: str = "_INBOX.1") -> FakeMessage:
  data = body if isinstance(body, bytes) else json.dumps(body).encode()
  message = FakeMessage("INGESTER_RPC.history.mt5.XAUUSD.M15", data, reply)
  _, callback, _ = nats.subscriptions[SUBJECT]
  await callback(message)
  return message


def decoded(message: FakeMessage) -> HistoryReply:
  (raw,) = message.responses
  return HistoryReply.model_validate_json(raw)


REQUEST = {"request_id": "r-1", "symbol": "XAUUSD", "timeframe": "M15", "count": 2}


async def test_each_gateway_listens_on_its_own_subject_and_queue_group():
  responder, nats = make_responder()
  await responder.start([FakeIngestion()])

  queue, _, _ = nats.subscriptions[SUBJECT]
  # A queue group: two ingesters running the same gateway answer a request
  # once between them, not once each.
  assert queue == "INGESTER_RPC-history-mt5"


async def test_requests_are_never_served_inside_the_published_subject_tree():
  # The stream captures INGESTER.> — a request there would be stored as market
  # data and answered by the stream's own acknowledgement first.
  responder, nats = make_responder()
  await responder.start([FakeIngestion()])
  assert not any(subject.startswith("INGESTER.") for subject in nats.subscriptions)


async def test_a_request_is_answered_with_the_bars_in_one_reply():
  ingestion = FakeIngestion()
  responder, nats = make_responder()
  await responder.start([ingestion])

  reply = decoded(await ask(nats, REQUEST))

  assert ingestion.calls == [("XAUUSD", Timeframe.M15, 2)]
  assert reply.status == "ok"
  assert reply.request_id == "r-1"
  assert reply.source == SOURCE
  assert (reply.symbol, reply.timeframe, reply.requested) == (
    "XAUUSD",
    Timeframe.M15,
    2,
  )
  assert reply.truncated is False
  assert reply.bars == make_bars(3)[-2:]
  assert reply.error is None


async def test_a_request_over_the_limit_is_capped_and_says_so():
  ingestion = FakeIngestion(make_bars(10))
  responder, nats = make_responder(HISTORY_MAX_BARS=4)
  await responder.start([ingestion])

  reply = decoded(await ask(nats, {**REQUEST, "count": 500}))

  assert ingestion.calls == [("XAUUSD", Timeframe.M15, 4)]
  assert len(reply.bars) == 4
  assert reply.requested == 500
  assert reply.truncated is True


async def test_fewer_bars_than_asked_for_is_not_truncation():
  # The venue simply has no more history; nothing was withheld.
  responder, nats = make_responder()
  await responder.start([FakeIngestion(make_bars(3))])

  reply = decoded(await ask(nats, {**REQUEST, "count": 150}))

  assert len(reply.bars) == 3
  assert reply.truncated is False


async def test_a_reply_too_large_for_one_message_keeps_the_newest_bars():
  bars = make_bars(400)
  nats = FakeNats(max_payload=24_000)
  responder, _ = make_responder(nats)
  await responder.start([FakeIngestion(bars)])

  message = await ask(nats, {**REQUEST, "count": 400})
  reply = decoded(message)

  assert len(message.responses[0]) <= 24_000
  assert reply.truncated is True
  assert 0 < len(reply.bars) < 400
  # Trimmed from the oldest end: a window is read from its newest bar back.
  assert reply.bars[-1] == bars[-1]
  assert reply.bars == bars[-len(reply.bars) :]


@pytest.mark.parametrize(
  ("error", "code"),
  [
    (UnknownSymbolError("mt5 is not configured for 'EURUSD'"), "unknown_symbol"),
    (HistoryUnavailableError("MT5 terminal is not connected"), "unavailable"),
    (RuntimeError("boom"), "internal"),
  ],
)
async def test_a_request_that_cannot_be_served_is_still_answered(error, code):
  ingestion = FakeIngestion()
  ingestion.error = error
  responder, nats = make_responder()
  await responder.start([ingestion])

  reply = decoded(await ask(nats, REQUEST))

  assert reply.status == "error"
  assert reply.error.code == code
  assert reply.request_id == "r-1"
  assert reply.bars == []


async def test_a_gateway_that_does_not_answer_in_time_is_reported_as_a_timeout():
  ingestion = FakeIngestion()
  ingestion.delay = 5.0
  responder, nats = make_responder(HISTORY_TIMEOUT=0.05)
  await responder.start([ingestion])

  reply = decoded(await ask(nats, REQUEST))

  assert reply.error.code == "timeout"


@pytest.mark.parametrize(
  "body",
  [
    b"not json",
    {"symbol": "XAUUSD", "timeframe": "M15"},
    {"symbol": "XAUUSD", "timeframe": "M15", "count": 0},
    {"symbol": "XAUUSD", "timeframe": "M7", "count": 5},
    {"symbol": "", "timeframe": "M15", "count": 5},
  ],
)
async def test_an_unreadable_request_is_refused_without_reaching_the_gateway(body):
  ingestion = FakeIngestion()
  responder, nats = make_responder()
  await responder.start([ingestion])

  reply = decoded(await ask(nats, body))

  assert reply.error.code == "bad_request"
  assert ingestion.calls == []


async def test_a_publish_with_no_reply_subject_is_ignored():
  ingestion = FakeIngestion()
  responder, nats = make_responder()
  await responder.start([ingestion])

  message = await ask(nats, REQUEST, reply="")

  assert message.responses == []
  assert ingestion.calls == []


async def test_stop_unsubscribes():
  responder, nats = make_responder()
  await responder.start([FakeIngestion()])
  await responder.stop()

  _, _, subscription = nats.subscriptions[SUBJECT]
  assert subscription.unsubscribed is True


# ── Announcing itself ───────────────────────────────────────────────

ONLINE = "INGESTER_RPC.online.mt5"


async def test_the_gateway_announces_itself_only_once_it_can_be_asked():
  # A subscriber reacts to the announcement by sending requests. Said before
  # the server knows about the subscription, the first of them finds nobody.
  responder, nats = make_responder()
  await responder.start([FakeIngestion()])

  assert nats.journal == [f"subscribe {SUBJECT}", "flush", f"publish {ONLINE}"]


async def test_the_announcement_says_who_is_online_and_what_it_ingests():
  responder, nats = make_responder()
  await responder.start([FakeIngestion()])

  ((subject, data),) = nats.published
  announcement = OnlineAnnouncement.model_validate_json(data)
  assert subject == ONLINE
  assert announcement.event_type == "ingester.online"
  assert announcement.source == SOURCE
  assert announcement.symbols == ["XAUUSD", "USOIL"]
  assert announcement.timeframes == [Timeframe.M15]
  assert announcement.reason == "started"


async def test_the_announcement_is_not_inside_the_published_subject_tree():
  responder, nats = make_responder()
  await responder.start([FakeIngestion()])
  assert not any(subject.startswith("INGESTER.") for subject, _ in nats.published)


async def test_a_reconnect_announces_again():
  # The subscriber may have missed closes while the link was down, and a
  # subscription that was restored is worth nothing to someone who stopped
  # asking.
  nats = FakeNats()
  settings = NatsSettings(_env_file=None, SUBJECT_PREFIX="INGESTER")
  connection = FakeConnection(nats)
  responder = HistoryResponder(connection, settings)
  await responder.start([FakeIngestion()])

  await connection.reconnect()

  reasons = [
    OnlineAnnouncement.model_validate_json(data).reason for _, data in nats.published
  ]
  assert reasons == ["started", "reconnected"]


async def test_a_late_first_connect_subscribes_after_a_failed_start():
  # NATS was down at start-up: there is no subscription to re-establish, so
  # the first connect that works has to do what start() could not. Without it
  # the ingester publishes bars and answers no history request until a restart.
  nats = FakeNats()
  settings = NatsSettings(_env_file=None, SUBJECT_PREFIX="INGESTER")
  connection = FakeConnection(nats)
  responder = HistoryResponder(connection, settings)
  connection.fail_subscribe = RuntimeError("NATS is not connected")

  with pytest.raises(RuntimeError):
    await responder.start([FakeIngestion()])
  assert nats.subscriptions == {}

  connection.fail_subscribe = None
  await connection.reconnect()

  assert SUBJECT in nats.subscriptions
  reasons = [
    OnlineAnnouncement.model_validate_json(data).reason for _, data in nats.published
  ]
  assert reasons == ["started"]


async def test_a_late_connect_without_gateways_announces_nothing():
  nats = FakeNats()
  connection = FakeConnection(nats)
  HistoryResponder(connection, NatsSettings(_env_file=None, SUBJECT_PREFIX="INGESTER"))

  await connection.reconnect()

  assert nats.subscriptions == {}
  assert nats.published == []


async def test_nothing_is_announced_before_start_or_after_stop():
  nats = FakeNats()
  connection = FakeConnection(nats)
  responder = HistoryResponder(
    connection, NatsSettings(_env_file=None, SUBJECT_PREFIX="INGESTER")
  )
  await connection.reconnect()
  assert nats.published == []

  await responder.start([FakeIngestion()])
  await responder.stop()
  await connection.reconnect()
  assert len(nats.published) == 1
