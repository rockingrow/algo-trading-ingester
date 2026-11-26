"""The NATS connection's own retries: the attempt window, the stop it
triggers, the background retry of a first connect that failed, and what its
callbacks leave in the log."""

import asyncio
from types import SimpleNamespace

import pytest
from nats.js.errors import NotFoundError

from ingester.services.nats_service import NatsConnection, ReconnectWatchdog
from ingester.settings import NatsSettings
from tests.fakes import FakeNotifier


class FakeClock:
  def __init__(self) -> None:
    self.now = 1000.0

  def __call__(self) -> float:
    return self.now

  def advance(self, seconds: float) -> None:
    self.now += seconds


async def _noop() -> None:
  return None


def _link_up(connection) -> None:
  """Stand in for a live nats-py client: ``is_connected`` reads off it.

  ``drain`` is there so ``close()`` takes its normal path on it.
  """
  connection._nc = SimpleNamespace(
    is_connected=True, drain=_noop, close=_noop, jetstream=lambda: None
  )


def _link_down(connection) -> None:
  connection._nc = None


def make_connection(clock=None, **config):
  """A connection that never dials anything: ``_nc`` stays ``None``, so it
  reads as disconnected — the state the watchdog is there for."""
  clock = clock or FakeClock()
  stops = []
  notifier = FakeNotifier()
  connection = NatsConnection(
    NatsSettings(_env_file=None, **config),
    notifier,
    "host-1",
    stop_service=stops.append,
    clock=clock,
  )
  return connection, notifier, stops, clock


# ── The window ─────────────────────────────────────────────────────


def test_trips_once_the_window_holds_enough_failures():
  clock = FakeClock()
  watchdog = ReconnectWatchdog(3, 1800.0, clock=clock)
  assert watchdog.enabled

  assert watchdog.record_failure() is False
  assert watchdog.record_failure() is False
  assert watchdog.record_failure() is True
  assert watchdog.attempts == 3


def test_it_trips_only_once():
  watchdog = ReconnectWatchdog(1, 1800.0, clock=FakeClock())
  assert watchdog.record_failure() is True
  # Stopping the service twice is not a thing; the second tick says nothing.
  assert watchdog.record_failure() is False


def test_failures_older_than_the_window_are_forgotten():
  clock = FakeClock()
  watchdog = ReconnectWatchdog(3, 1800.0, clock=clock)
  watchdog.record_failure()
  watchdog.record_failure()

  clock.advance(1800.1)
  assert watchdog.record_failure() is False
  assert watchdog.attempts == 1


def test_a_failure_on_the_window_edge_still_counts():
  clock = FakeClock()
  watchdog = ReconnectWatchdog(2, 1800.0, clock=clock)
  watchdog.record_failure()
  clock.advance(1800.0)
  assert watchdog.record_failure() is True


def test_zero_attempts_disables_it():
  watchdog = ReconnectWatchdog(0, 1800.0, clock=FakeClock())
  assert watchdog.enabled is False
  for _ in range(100):
    assert watchdog.record_failure() is False


# ── What the connection does with it ───────────────────────────────


async def test_the_service_is_stopped_and_the_reason_notified():
  connection, notifier, stops, _ = make_connection(
    GIVE_UP_AFTER_ATTEMPTS=3, GIVE_UP_WINDOW_SECONDS=1800.0
  )

  assert await connection._check_connection() is False
  assert await connection._check_connection() is False
  assert stops == []

  assert await connection._check_connection() is True
  assert len(stops) == 1
  assert "unreachable" in stops[0]
  assert "3 failed" in stops[0]

  # The operator is told it stays down: nothing restarts the service.
  assert len(notifier.messages) == 1
  message = notifier.messages[0]
  assert "NATS Unreachable" in message
  assert "30 min" in message
  assert "not</b> restarting" in message


async def test_a_connected_link_never_gives_up():
  connection, notifier, stops, _ = make_connection(GIVE_UP_AFTER_ATTEMPTS=1)
  _link_up(connection)  # as if nats-py had the link back

  assert await connection._check_connection() is False
  assert (stops, notifier.messages) == ([], [])


async def test_a_deliberate_close_is_not_an_outage():
  connection, _, stops, _ = make_connection(GIVE_UP_AFTER_ATTEMPTS=1)
  connection._closing = True

  assert await connection._check_connection() is False
  assert stops == []


async def test_flapping_inside_the_window_gives_up_too():
  """A reconnect does not clear the count — a link that keeps dropping is not
  carrying bars either."""
  connection, _, stops, _ = make_connection(
    GIVE_UP_AFTER_ATTEMPTS=2, GIVE_UP_WINDOW_SECONDS=1800.0
  )

  assert await connection._check_connection() is False
  _link_up(connection)
  assert await connection._check_connection() is False
  _link_down(connection)
  assert await connection._check_connection() is True
  assert len(stops) == 1


# ── The watchdog task's lifecycle ──────────────────────────────────


async def test_the_task_ticks_on_its_own_and_ends_with_the_service_stopped():
  connection, notifier, stops, _ = make_connection(GIVE_UP_AFTER_ATTEMPTS=1)
  connection._start_watchdog()

  # One tick at the interval floor is enough with a threshold of one, and the
  # task returns as soon as it has given up.
  await asyncio.wait_for(connection._watchdog_task, timeout=5.0)

  assert len(stops) == 1
  assert len(notifier.messages) == 1


async def test_closing_cancels_the_watchdog_even_after_a_failed_connect():
  connection, _, stops, _ = make_connection(GIVE_UP_AFTER_ATTEMPTS=5)
  connection._start_watchdog()
  task = connection._watchdog_task

  await connection.close()  # _nc is None: the connect never succeeded

  assert task.cancelled() or task.done()
  assert connection._watchdog_task is None
  assert stops == []


async def test_the_watchdog_is_not_armed_when_disabled():
  connection, _, _, _ = make_connection(GIVE_UP_AFTER_ATTEMPTS=0)
  connection._start_watchdog()
  assert connection._watchdog_task is None


# ── Retrying a first connect that failed ───────────────────────────


async def test_a_failed_first_connect_is_retried_in_the_background():
  # nats-py's reconnect loop belongs to a client that a failed connect never
  # created, so without this the process never reaches a NATS that came up
  # after it did.
  connection, notifier, _, _ = make_connection(RECONNECT_TIME_WAIT=0.01)
  attempts = []

  async def dial():
    attempts.append(1)
    if len(attempts) < 3:
      raise ConnectionError("unreachable")
    _link_up(connection)

  connection._dial = dial
  hooks = []
  connection.on_reconnect(lambda: hooks.append(1) or asyncio.sleep(0))

  with pytest.raises(ConnectionError):
    await connection.connect()
  assert connection._connect_retry_task is not None

  await asyncio.wait_for(connection._connect_retry_task, timeout=3.0)

  assert len(attempts) == 3
  assert connection.is_connected
  # Reported and hooked like a reconnect: that is what re-subscribes to
  # history requests and announces the gateways as online.
  assert any("reconnect" in message.lower() for message in notifier.messages)
  assert hooks == [1]
  await connection.close()


async def test_a_successful_first_connect_starts_no_retry():
  connection, _, _, _ = make_connection(RECONNECT_TIME_WAIT=0.01)

  async def dial():
    _link_up(connection)

  connection._dial = dial
  await connection.connect()

  assert connection._connect_retry_task is None
  await connection.close()


async def test_closing_stops_the_retry():
  connection, _, _, _ = make_connection(RECONNECT_TIME_WAIT=0.01)

  async def dial():
    raise ConnectionError("unreachable")

  connection._dial = dial
  with pytest.raises(ConnectionError):
    await connection.connect()
  task = connection._connect_retry_task

  await connection.close()

  assert task.cancelled() or task.done()
  assert connection._connect_retry_task is None


# ── What an outage leaves in the log ───────────────────────────────


async def test_error_callback_names_the_exception_type(caplog):
  """A failed reconnect arrives as an exception with no message of its own.

  Logged with %s it reads as "NATS error:" and nothing else, which is what an
  operator has to work with while the link is down — so the type has to be
  there.
  """
  connection, _, _, _ = make_connection()

  with caplog.at_level("ERROR", logger="ingester.services.nats_service"):
    await connection._on_error(TimeoutError())

  (record,) = caplog.records
  assert record.getMessage() == "NATS error: TimeoutError()"


# ── A client whose JetStream set-up failed ─────────────────────────


class _BrokenStreamClient:
  """A client that connects but cannot have its stream created, and fires the
  disconnected callback from ``close()`` as nats-py does.

  *info_error* is what ``stream_info`` raises: ``NotFoundError`` for a server
  with no such stream, anything else for a read that simply did not answer.
  """

  def __init__(self, disconnected_cb, info_error: Exception | None = None) -> None:
    self.is_connected = True
    self.closed = False
    self.add_stream_calls = 0
    self._info_error = info_error or NotFoundError()
    self._disconnected_cb = disconnected_cb

  def jetstream(self):
    async def stream_info(name):
      raise self._info_error

    async def add_stream(config):
      self.add_stream_calls += 1
      raise RuntimeError("JetStream not enabled")

    return SimpleNamespace(stream_info=stream_info, add_stream=add_stream)

  async def close(self) -> None:
    self.closed = True
    self.is_connected = False
    await self._disconnected_cb()


@pytest.fixture
def broken_stream_server(monkeypatch):
  """A NATS server whose stream cannot be set up.

  ``clients`` collects every client dialled; ``info_error`` is what
  ``stream_info`` raises, which a test sets before connecting.
  """
  server = SimpleNamespace(clients=[], info_error=None)

  async def connect(**options):
    client = _BrokenStreamClient(options["disconnected_cb"], server.info_error)
    server.clients.append(client)
    return client

  monkeypatch.setattr("ingester.services.nats_service.nats_lib.connect", connect)
  # The retry cadence is floored for production; a test does not wait it out.
  monkeypatch.setattr("ingester.services.nats_service._MIN_WATCHDOG_INTERVAL", 0.01)
  return server


async def test_a_client_whose_stream_cannot_be_set_up_is_closed_not_leaked(
  broken_stream_server,
):
  connection, notifier, _, _ = make_connection(
    JETSTREAM_ENABLED=True, RECONNECT_TIME_WAIT=0.01
  )

  with pytest.raises(ConnectionError, match="JetStream stream"):
    await connection.connect()
  # Retried like any connect that failed — every attempt with a new client.
  await asyncio.sleep(0.2)
  retry = connection._connect_retry_task
  await connection.close()

  assert retry is not None
  assert len(broken_stream_server.clients) >= 3
  assert all(client.closed for client in broken_stream_server.clients)
  assert not connection.is_connected
  # Closed on purpose, so not reported to operators as an outage.
  assert not any("disconnected" in message.lower() for message in notifier.messages)


async def test_a_stream_that_cannot_be_read_keeps_the_connection(
  broken_stream_server,
):
  # The live host's link stalls for tens of seconds at a time, so stream_info
  # times out on a stream that is perfectly there. Creating it blind would
  # fail on "already in use" and throw a working connection away — and the
  # watchdog would stop the service over it.
  broken_stream_server.info_error = TimeoutError()
  connection, notifier, stops, _ = make_connection(
    JETSTREAM_ENABLED=True, RECONNECT_TIME_WAIT=0.01
  )

  await connection.connect()

  assert connection.is_connected
  assert broken_stream_server.clients[0].add_stream_calls == 0
  assert connection._connect_retry_task is None
  assert stops == []
  await connection.close()
