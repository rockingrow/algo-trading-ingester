import pytest

from ingestor.gateways.mt5 import Mt5Ingestion
from ingestor.schemas import BarClosedEvent, GatewayStatusEnum, Timeframe
from ingestor.settings import Mt5Settings
from tests.fakes import FakeNotifier, FakePublisher, FakeTerminal, rate, wait_for

M1 = Timeframe.M1
T0 = 1_790_000_040  # a minute boundary


def make_ingestion(terminal: FakeTerminal, **overrides):
  fields = dict(
    SYMBOLS=["XAUUSD"],
    TIMEFRAMES=[M1],
    POLL_INTERVAL_SECONDS=0.01,
    RECONNECT_INTERVAL_SECONDS=0.01,
    CATCHUP_BARS=5,
  )
  config = Mt5Settings(_env_file=None, **{**fields, **overrides})
  publisher, notifier = FakePublisher(), FakeNotifier()
  ingestion = Mt5Ingestion(
    config=config,
    terminal=terminal,
    publisher=publisher,
    notifier=notifier,
    instance_id="test",
  )
  return ingestion, publisher, notifier


@pytest.fixture
def terminal() -> FakeTerminal:
  terminal = FakeTerminal()
  terminal.set_rates("XAUUSD", M1, [rate(T0 - 60), rate(T0)])
  return terminal


async def test_first_read_primes_without_publishing(terminal):
  ingestion, publisher, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._last_open)
  await ingestion.stop()
  assert publisher.events == []


async def test_new_closed_bar_is_published_once(terminal):
  ingestion, publisher, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._last_open)

  terminal.set_rates("XAUUSD", M1, [rate(T0 - 60), rate(T0), rate(T0 + 60, 2.0)])
  await wait_for(lambda: len(publisher.events) == 1)
  await wait_for(lambda: ingestion.snapshot()["published"] == 1)
  await ingestion.stop()

  (event,) = publisher.events
  assert isinstance(event, BarClosedEvent)
  assert event.symbol == "XAUUSD"
  assert event.timeframe is M1
  assert int(event.bar.open_time.timestamp()) == T0 + 60
  assert event.bar.close == 2.25
  assert event.source.venue == "Fake-Server"
  assert event.source.ingestor_id == "test"


async def test_missed_bars_are_caught_up_in_order(terminal):
  ingestion, publisher, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._last_open)

  terminal.set_rates(
    "XAUUSD", M1, [rate(T0), rate(T0 + 60), rate(T0 + 120), rate(T0 + 180)]
  )
  await wait_for(lambda: len(publisher.events) == 3)
  await ingestion.stop()
  opens = [int(e.bar.open_time.timestamp()) for e in publisher.events]
  assert opens == [T0 + 60, T0 + 120, T0 + 180]


async def test_disconnect_then_reconnect_notifies_and_resumes(terminal):
  ingestion, publisher, notifier = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._last_open)

  terminal.connected = False
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.DISCONNECTED)

  # A bar closes while we are down; it is recovered after reconnecting.
  terminal.set_rates("XAUUSD", M1, [rate(T0), rate(T0 + 60)])
  terminal.connected = True
  await wait_for(lambda: len(publisher.events) == 1)
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)
  await ingestion.stop()

  assert terminal.shutdown_calls >= 2  # once on the drop, once on stop
  joined = "\n".join(notifier.messages)
  for status in ("starting", "running", "disconnected", "stopped"):
    assert f"gateway {status}" in joined


async def test_initialize_failure_keeps_retrying(terminal):
  terminal.initialize_ok = False
  ingestion, _, notifier = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: terminal.initialize_calls >= 3)
  assert ingestion.status is GatewayStatusEnum.DISCONNECTED
  terminal.initialize_ok = True
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)
  await ingestion.stop()
  assert ingestion.status is GatewayStatusEnum.STOPPED
  # Repeated failures notify once, not once per attempt.
  assert sum("disconnected" in m for m in notifier.messages) == 1


async def test_unknown_symbol_is_skipped_not_fatal(terminal):
  ingestion, publisher, _ = make_ingestion(terminal, SYMBOLS=["NOPE", "XAUUSD"])
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._last_open)
  assert ingestion.status is GatewayStatusEnum.RUNNING
  await ingestion.stop()


async def test_start_requires_symbols(terminal):
  ingestion, _, _ = make_ingestion(terminal, SYMBOLS=[])
  with pytest.raises(ValueError, match="symbol"):
    await ingestion.start()
