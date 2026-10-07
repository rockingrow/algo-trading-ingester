import asyncio

import pytest

from ingester.core.errors import HistoryUnavailableError, UnknownSymbolError
from ingester.gateways.forex.mt5 import Mt5Ingestion
from ingester.schemas import BarClosedEvent, GatewayStatusEnum, Timeframe
from ingester.settings import Mt5Settings
from tests.fakes import FakeNotifier, FakePublisher, FakeTerminal, rate, wait_for

M1 = Timeframe.M1
T0 = 1_790_000_040  # a minute boundary


def make_ingestion(terminal: FakeTerminal, **overrides):
  fields = dict(
    SYMBOLS=["XAUUSD"],
    TIMEFRAMES=[M1],
    POLL_INTERVAL_SECONDS=0.01,
    RECONNECT_INTERVAL_SECONDS=0.01,
    RECOVERY_BARS=5,
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


async def test_a_restart_publishes_nothing_it_found_already_closed(terminal):
  # Three bars closed while the process was down. They are history now: the
  # subscriber asks for them, the gateway does not push them.
  terminal.set_rates("XAUUSD", M1, [rate(T0 - 120), rate(T0 - 60), rate(T0)])
  ingestion, publisher, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)
  await ingestion.stop()
  assert publisher.events == []


# ── History, on request ─────────────────────────────────────────────


async def test_history_returns_the_closed_bars_asked_for_oldest_first(terminal):
  terminal.set_rates(
    "XAUUSD", M1, [rate(T0 - 180), rate(T0 - 120), rate(T0 - 60), rate(T0)]
  )
  ingestion, publisher, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)

  symbol, bars = await ingestion.fetch_history("XAUUSD", M1, 3)
  await ingestion.stop()

  assert symbol == "XAUUSD"
  assert [int(bar.open_time.timestamp()) for bar in bars] == [T0 - 120, T0 - 60, T0]
  # Asking for history publishes nothing and does not move the live mark.
  assert publisher.events == []
  assert ingestion.snapshot()["history_served"] == 1


async def test_history_is_read_on_the_gateway_thread(terminal):
  # MetaTrader5 keeps thread-affine state: a read from the event loop's thread
  # is the bug this hand-off exists to prevent.
  ingestion, _, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)
  terminal.reads.clear()

  await ingestion.fetch_history("XAUUSD", M1, 2)
  await ingestion.stop()

  assert {thread for thread, _ in terminal.reads} == {"mt5-ingestion"}


async def test_history_is_served_without_waiting_out_the_poll_interval(terminal):
  ingestion, _, _ = make_ingestion(terminal, POLL_INTERVAL_SECONDS=30.0)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)

  _, bars = await asyncio.wait_for(ingestion.fetch_history("XAUUSD", M1, 2), 2.0)
  await ingestion.stop()
  assert len(bars) == 2


async def test_history_may_ask_for_a_timeframe_the_gateway_does_not_poll(terminal):
  terminal.set_rates("XAUUSD", Timeframe.M15, [rate(T0 - 900), rate(T0)])
  ingestion, _, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)

  _, bars = await ingestion.fetch_history("XAUUSD", Timeframe.M15, 10)
  await ingestion.stop()

  assert [bar.close_time - bar.open_time for bar in bars] == [Timeframe.M15.delta] * 2


async def test_history_uses_the_broker_symbol_and_answers_with_the_configured_one():
  terminal = FakeTerminal()
  terminal.catalogue = ["XAUUSDm"]
  terminal.set_rates("XAUUSDm", M1, [rate(T0 - 60), rate(T0)])
  ingestion, _, _ = make_ingestion(terminal, SYMBOLS=["XAUUSD"])
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)

  symbol, bars = await ingestion.fetch_history("xauusd", M1, 2)
  await ingestion.stop()

  assert symbol == "XAUUSD"
  assert len(bars) == 2


async def test_history_collapses_a_bar_mt5_repeats(terminal):
  terminal.set_rates("XAUUSD", M1, [rate(T0 - 60), rate(T0), rate(T0)])
  ingestion, _, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)

  _, bars = await ingestion.fetch_history("XAUUSD", M1, 3)
  await ingestion.stop()
  assert [int(bar.open_time.timestamp()) for bar in bars] == [T0 - 60, T0]


async def test_history_for_an_unconfigured_symbol_is_refused(terminal):
  ingestion, _, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)

  with pytest.raises(UnknownSymbolError, match="EURUSD"):
    await ingestion.fetch_history("EURUSD", M1, 5)
  await ingestion.stop()
  assert ingestion.snapshot()["history_refused"] == 1


async def test_history_is_unavailable_while_the_terminal_is_disconnected(terminal):
  ingestion, _, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)
  terminal.connected = False
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.DISCONNECTED)

  with pytest.raises(HistoryUnavailableError, match="not connected"):
    await ingestion.fetch_history("XAUUSD", M1, 5)
  await ingestion.stop()


async def test_history_with_no_bars_is_unavailable_not_empty(terminal):
  ingestion, _, _ = make_ingestion(terminal)
  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)

  # Nothing scripted for H1: MT5 answers None, which is "cannot say", not
  # "there are no bars".
  with pytest.raises(HistoryUnavailableError, match="no bars"):
    await ingestion.fetch_history("XAUUSD", Timeframe.H1, 5)
  await ingestion.stop()


async def test_history_before_start_and_after_stop_is_unavailable(terminal):
  ingestion, _, _ = make_ingestion(terminal)
  with pytest.raises(HistoryUnavailableError, match="not running"):
    await ingestion.fetch_history("XAUUSD", M1, 5)

  await ingestion.start()
  await wait_for(lambda: ("XAUUSD", M1) in ingestion._open_marks)
  await ingestion.stop()
  with pytest.raises(HistoryUnavailableError, match="not running"):
    await ingestion.fetch_history("XAUUSD", M1, 5)


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
