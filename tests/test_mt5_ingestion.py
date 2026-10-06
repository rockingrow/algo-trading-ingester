import pytest

from ingester.gateways.forex.mt5 import Mt5Ingestion
from ingester.schemas import BarClosedEvent, GatewayStatusEnum, Timeframe
from ingester.settings import Mt5Settings
from tests.fakes import (
  FakeNotifier,
  FakePublisher,
  FakeTerminal,
  rate,
  wait_for,
  warmup_series,
)

M1 = Timeframe.M1
T0 = 1_790_000_040  # a minute boundary


def make_ingestion(terminal: FakeTerminal, **overrides):
  fields = dict(
    SYMBOLS=["XAUUSD"],
    TIMEFRAMES=[M1],
    POLL_INTERVAL_SECONDS=0.01,
    RECONNECT_INTERVAL_SECONDS=0.01,
    WARMUP_BARS=5,
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
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)
  await ingestion.stop()
  assert publisher.events == []


async def test_new_closed_bar_is_published_once(terminal):
  ingestion, publisher, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)

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
  assert event.source.ingester_id == "test"


async def test_missed_bars_are_caught_up_in_order(terminal):
  ingestion, publisher, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)

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
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)

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
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.DISCONNECTED)
  terminal.initialize_ok = True
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)
  await ingestion.stop()
  assert ingestion.status is GatewayStatusEnum.STOPPED
  # Repeated failures notify once, not once per attempt.
  assert sum("disconnected" in m for m in notifier.messages) == 1


async def test_unknown_symbol_is_skipped_not_fatal(terminal):
  ingestion, publisher, _ = make_ingestion(terminal, SYMBOLS=["NOPE", "XAUUSD"])
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)
  # Status crosses from the watcher thread via call_soon_threadsafe, so it may
  # land a moment after the stream is primed.
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)
  await ingestion.stop()


async def test_start_requires_symbols(terminal):
  ingestion, _, _ = make_ingestion(terminal, SYMBOLS=[])
  with pytest.raises(ValueError, match="symbol"):
    await ingestion.start()


async def test_configured_base_resolves_to_the_broker_symbol():
  # .env says XAUUSD; this broker only sells XAUUSDm.
  terminal = FakeTerminal()
  terminal.catalogue = ["XAUUSDm", "EURUSDm"]
  terminal.set_rates("XAUUSDm", M1, [rate(T0 - 60), rate(T0)])
  ingestion, publisher, _ = make_ingestion(terminal, SYMBOLS=["XAUUSD"])
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)

  terminal.set_rates("XAUUSDm", M1, [rate(T0), rate(T0 + 60)])
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()

  assert ingestion._broker_symbol == {"XAUUSD": "XAUUSDm"}
  # Rates were read from XAUUSDm, but the wire keeps the configured name so
  # event_id survives a change of broker.
  (event,) = publisher.events
  assert event.symbol == "XAUUSD"
  assert event.event_id == f"mt5:XAUUSD:M1:{T0 + 60}"


async def test_configured_suffix_breaks_ambiguity():
  terminal = FakeTerminal()
  terminal.catalogue = ["EURUSDm", "EURUSDc"]
  terminal.set_rates("EURUSDc", M1, [rate(T0)])
  ingestion, _, _ = make_ingestion(terminal, SYMBOLS=["EURUSD"], SYMBOL_SUFFIX="c")
  await ingestion.start()
  await wait_for(lambda: ("EURUSD", M1) in ingestion._open_marks)
  await ingestion.stop()
  assert ingestion._broker_symbol == {"EURUSD": "EURUSDc"}


async def test_unresolvable_symbol_is_skipped_and_the_rest_still_run(terminal):
  terminal.catalogue = ["XAUUSD"]
  ingestion, _, _ = make_ingestion(terminal, SYMBOLS=["NOPE", "XAUUSD"])
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)
  await ingestion.stop()
  assert ingestion._broker_symbol == {"XAUUSD": "XAUUSD"}


async def test_backfill_publishes_the_startup_window(terminal):
  # What a crash costs today: these three bars closed while we were down.
  terminal.set_rates("XAUUSD", M1, [rate(T0 - 120), rate(T0 - 60), rate(T0)])
  ingestion, publisher, _ = make_ingestion(terminal, BACKFILL_ON_START=True)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 3)
  await ingestion.stop()

  opens = [int(e.bar.open_time.timestamp()) for e in publisher.events]
  assert opens == [T0 - 120, T0 - 60, T0]


async def test_only_the_startup_window_is_flagged_warmup(terminal):
  terminal.set_rates("XAUUSD", M1, [rate(T0 - 60), rate(T0)])
  ingestion, publisher, _ = make_ingestion(terminal, BACKFILL_ON_START=True)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 2)

  # A bar that closes while the process is running is live, however late the
  # poll finds it — only the window read back at start-up is warm-up.
  terminal.set_rates("XAUUSD", M1, [rate(T0), rate(T0 + 60)])
  await wait_for(lambda: len(publisher.events) == 3)
  await ingestion.stop()

  # The whole numbered window first, oldest first, then the live bar: the
  # subscriber ends its warm-up on the bar where index == total.
  assert warmup_series(publisher) == [
    (True, 1, 2),
    (True, 2, 2),
    (False, None, None),
  ]


async def test_warmup_total_is_the_window_mt5_returned_not_warmup_bars(terminal):
  # The broker has three bars of history; warmup_bars asks for fifty. A total
  # of fifty would leave the subscriber warming up forever.
  terminal.set_rates("XAUUSD", M1, [rate(T0 - 120), rate(T0 - 60), rate(T0)])
  ingestion, publisher, _ = make_ingestion(
    terminal, BACKFILL_ON_START=True, WARMUP_BARS=50
  )
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 3)
  await ingestion.stop()

  assert warmup_series(publisher) == [(True, 1, 3), (True, 2, 3), (True, 3, 3)]


async def test_a_repeated_bar_does_not_shorten_the_warmup_series(terminal):
  # MT5 handing the same bar back twice in one window must not leave the series
  # one short of the warmup_total it announces.
  terminal.set_rates("XAUUSD", M1, [rate(T0 - 60), rate(T0), rate(T0)])
  ingestion, publisher, _ = make_ingestion(terminal, BACKFILL_ON_START=True)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 2)
  await ingestion.stop()

  assert warmup_series(publisher) == [(True, 1, 2), (True, 2, 2)]


async def test_a_broken_record_leaves_the_whole_warmup_window_unpublished(terminal):
  # A truncated series is worse than none: the subscriber ends its warm-up on
  # index == total, so a window that announces 3 and stops at 1 strands it.
  broken = {**rate(T0 - 60), "high": 0.0}
  terminal.catalogue = ["XAUUSD", "EURUSD"]
  terminal.set_rates("XAUUSD", M1, [rate(T0 - 120), broken, rate(T0)])
  terminal.set_rates("EURUSD", M1, [rate(T0 - 60), rate(T0)])
  ingestion, publisher, _ = make_ingestion(
    terminal, SYMBOLS=["XAUUSD", "EURUSD"], BACKFILL_ON_START=True
  )
  await ingestion.start()
  # EURUSD is polled in the same cycle, so its window proves XAUUSD's was
  # attempted and published nothing at all.
  await wait_for(lambda: len(publisher.events) == 2)
  assert {event.symbol for event in publisher.events} == {"EURUSD"}
  assert ("XAUUSD", M1) not in ingestion._open_marks

  # And the mark never moved, so the next poll retries the whole window rather
  # than resuming past the bar it never published.
  terminal.set_rates("XAUUSD", M1, [rate(T0 - 120), rate(T0 - 60), rate(T0)])
  await wait_for(lambda: len(publisher.events) == 5)
  await ingestion.stop()

  assert warmup_series(publisher)[2:] == [(True, 1, 3), (True, 2, 3), (True, 3, 3)]


async def test_backfill_off_still_primes_silently(terminal):
  ingestion, publisher, _ = make_ingestion(terminal, BACKFILL_ON_START=False)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)
  await ingestion.stop()
  assert publisher.events == []


async def test_a_broken_stream_does_not_starve_the_others(terminal):
  # A record MT5 should never produce: high below the open. The DTO rejects it,
  # and the second symbol must still be polled in the same cycle.
  broken = {**rate(T0 + 60), "high": 0.0}
  terminal.catalogue = ["XAUUSD", "EURUSD"]
  terminal.set_rates("EURUSD", M1, [rate(T0 - 60), rate(T0)])
  ingestion, publisher, _ = make_ingestion(terminal, SYMBOLS=["XAUUSD", "EURUSD"])
  await ingestion.start()
  await wait_for(lambda: ("EURUSD", M1) in ingestion._open_marks)

  terminal.set_rates("XAUUSD", M1, [rate(T0), broken])
  terminal.set_rates("EURUSD", M1, [rate(T0), rate(T0 + 60)])
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()

  (event,) = publisher.events
  assert event.symbol == "EURUSD"
  assert ingestion.status is GatewayStatusEnum.STOPPED
