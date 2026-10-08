"""Giving up on NATS: the rolling attempt window, and the stop it triggers."""

import asyncio
from types import SimpleNamespace

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


def _link_up(connection) -> None:
  """Stand in for a live nats-py client: ``is_connected`` reads off it."""
  connection._nc = SimpleNamespace(is_connected=True)


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
