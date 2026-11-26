from datetime import UTC, datetime

import pytest

from ingester.core.errors import (
  GatewayConnectionError,
  HistoryUnavailableError,
  UnknownSymbolError,
)
from ingester.gateways.crypto.binance import BinanceIngestion
from ingester.schemas import BarClosedEvent, GatewayStatusEnum, Timeframe
from ingester.schemas.market_event_schema import utcnow
from ingester.settings import BinanceSettings
from tests.fakes import (
  FakeKlineHistory,
  FakeKlineStream,
  FakeNotifier,
  FakePublisher,
  kline,
  rest_kline,
  wait_for,
)

M1 = Timeframe.M1
# 2026-09-28T10:00:00Z, a minute boundary.
T0_MS = 1_790_589_600_000


def make_ingestion(
  stream: FakeKlineStream, history: FakeKlineHistory | None = None, **overrides
):
  fields = dict(
    ENABLE=True,
    SYMBOLS=["BTCUSDT"],
    TIMEFRAMES=[M1],
    RECONNECT_INTERVAL_SECONDS=0.01,
  )
  config = BinanceSettings(_env_file=None, **{**fields, **overrides})
  publisher, notifier = FakePublisher(), FakeNotifier()
  ingestion = BinanceIngestion(
    config=config,
    stream=stream,
    history=history or FakeKlineHistory(),
    publisher=publisher,
    notifier=notifier,
    instance_id="test",
  )
  return ingestion, publisher, notifier


async def test_subscribes_to_every_symbol_and_timeframe():
  stream = FakeKlineStream()
  ingestion, _, _ = make_ingestion(
    stream, SYMBOLS=["BTCUSDT", "ETHUSDT"], TIMEFRAMES=[M1, Timeframe.M15]
  )
  await ingestion.start()
  await wait_for(lambda: stream.opened)
  await ingestion.stop()

  url, streams = stream.opened[0]
  assert url == "wss://stream.binance.com:9443/stream"
  assert streams == [
    "btcusdt@kline_1m",
    "btcusdt@kline_15m",
    "ethusdt@kline_1m",
    "ethusdt@kline_15m",
  ]


async def test_closed_kline_is_published():
  stream = FakeKlineStream([kline(T0_MS)])
  ingestion, publisher, _ = make_ingestion(stream)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 1)
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)
  await ingestion.stop()

  (event,) = publisher.events
  assert isinstance(event, BarClosedEvent)
  assert event.symbol == "BTCUSDT"
  assert event.timeframe is M1
  assert event.source.market.value == "crypto"
  assert event.source.gateway.value == "binance"
  assert event.event_id == f"binance:BTCUSDT:M1:{T0_MS // 1000}"
  assert event.bar.quote_volume == 150.0


async def test_a_bar_still_forming_is_never_published():
  # Publishing an unfinished candle hands strategies a price that can still
  # move; only x=true is a close.
  stream = FakeKlineStream([kline(T0_MS, closed=False), kline(T0_MS + 60_000)])
  ingestion, publisher, _ = make_ingestion(stream)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()

  (event,) = publisher.events
  assert int(event.bar.open_time.timestamp()) == (T0_MS + 60_000) // 1000


async def test_a_candle_the_schema_refuses_can_still_arrive_corrected():
  # Binance sends a close whose high is below its open: Bar refuses it. The
  # stream's mark must not have moved, or the corrected update that follows
  # reads as a bar already published and the close is lost for good.
  broken = kline(T0_MS)
  broken["data"]["k"]["h"] = "1.0"
  stream = FakeKlineStream([broken])
  ingestion, publisher, _ = make_ingestion(stream)
  await ingestion.start()
  await wait_for(lambda: stream.opened)
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)

  stream.push(kline(T0_MS))
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()

  assert int(publisher.events[0].bar.open_time.timestamp() * 1000) == T0_MS


async def test_a_repeated_close_is_published_once():
  stream = FakeKlineStream([kline(T0_MS), kline(T0_MS), kline(T0_MS + 60_000)])
  ingestion, publisher, _ = make_ingestion(stream)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 2)
  await ingestion.stop()

  opens = [int(e.bar.open_time.timestamp()) for e in publisher.events]
  assert opens == [T0_MS // 1000, (T0_MS + 60_000) // 1000]


async def test_a_single_stream_frame_is_understood_too():
  # Without the combined-stream wrapper, e.g. a one-stream endpoint.
  stream = FakeKlineStream([kline(T0_MS, combined=False)])
  ingestion, publisher, _ = make_ingestion(stream)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()


async def test_unrelated_frames_are_ignored():
  stream = FakeKlineStream(
    [
      {"result": None, "id": 1},  # subscription acknowledgement
      {"data": {"e": "24hrTicker", "s": "BTCUSDT"}},
      {"data": {"e": "kline", "k": {"nonsense": True}}},  # rejected, not fatal
      kline(T0_MS),
    ]
  )
  ingestion, publisher, _ = make_ingestion(stream)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()
  assert ingestion.status is GatewayStatusEnum.STOPPED


async def test_an_unconfigured_symbol_or_interval_is_skipped():
  stream = FakeKlineStream(
    [
      kline(T0_MS, symbol="DOGEUSDT"),  # never subscribed
      kline(T0_MS, interval="3m"),  # no canonical timeframe
      kline(T0_MS),
    ]
  )
  ingestion, publisher, _ = make_ingestion(stream)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()
  assert publisher.events[0].symbol == "BTCUSDT"


async def test_dropped_socket_reconnects_and_resumes():
  stream = FakeKlineStream(
    [kline(T0_MS), GatewayConnectionError("socket closed"), kline(T0_MS + 60_000)]
  )
  ingestion, publisher, notifier = make_ingestion(stream)
  await ingestion.start()
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.DISCONNECTED)
  await wait_for(lambda: len(publisher.events) == 2)
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)
  await ingestion.stop()

  assert len(stream.opened) >= 2
  joined = "\n".join(notifier.messages)
  for status in ("starting", "running", "disconnected", "stopped"):
    assert f"gateway {status}" in joined


async def test_connect_failure_keeps_retrying():
  stream = FakeKlineStream([kline(T0_MS)])
  stream.open_error = GatewayConnectionError("handshake refused")
  ingestion, publisher, notifier = make_ingestion(stream)
  await ingestion.start()
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.DISCONNECTED)
  # The second dial succeeds and the queued bar lands.
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()

  assert len(stream.opened) >= 2
  # Repeated failures notify once, not once per attempt.
  assert sum("disconnected" in m for m in notifier.messages) == 1


async def test_a_binance_error_frame_drops_the_connection():
  stream = FakeKlineStream(
    [{"error": {"code": 3, "msg": "Invalid JSON"}}, kline(T0_MS)]
  )
  ingestion, publisher, _ = make_ingestion(stream)
  await ingestion.start()
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.DISCONNECTED)
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()


async def test_stop_closes_the_socket():
  stream = FakeKlineStream()
  ingestion, _, _ = make_ingestion(stream)
  await ingestion.start()
  await wait_for(lambda: stream.opened)
  await ingestion.stop()
  assert stream.close_calls >= 1
  assert ingestion.status is GatewayStatusEnum.STOPPED


async def test_start_requires_symbols():
  ingestion, _, _ = make_ingestion(FakeKlineStream(), SYMBOLS=[])
  with pytest.raises(ValueError, match="symbol"):
    await ingestion.start()


# ── History, on request ─────────────────────────────────────────────


async def test_a_start_reads_nothing_over_rest():
  # Nothing is pushed at start-up any more: a restart publishes only the bars
  # that close from here on, and history waits to be asked for.
  history = FakeKlineHistory({("BTCUSDT", "1m"): [rest_kline(T0_MS)]})
  stream = FakeKlineStream([kline(T0_MS + 60_000)])
  ingestion, publisher, _ = make_ingestion(stream, history)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()

  assert history.calls == []
  assert int(publisher.events[0].bar.open_time.timestamp()) == (T0_MS + 60_000) // 1000


async def test_history_returns_closed_bars_oldest_first_and_publishes_nothing():
  history = FakeKlineHistory(
    {
      ("BTCUSDT", "1m"): [
        rest_kline(T0_MS - 120_000),
        rest_kline(T0_MS - 60_000),
        rest_kline(T0_MS),
      ]
    }
  )
  ingestion, publisher, _ = make_ingestion(FakeKlineStream(), history)

  symbol, bars = await ingestion.fetch_history("btcusdt", M1, 3)

  assert symbol == "BTCUSDT"
  assert [int(bar.open_time.timestamp() * 1000) for bar in bars] == [
    T0_MS - 120_000,
    T0_MS - 60_000,
    T0_MS,
  ]
  assert publisher.events == []


async def test_history_asks_one_row_past_the_count_and_drops_the_forming_bar():
  now_open_ms = int(utcnow().timestamp() * 1000) // 60_000 * 60_000
  history = FakeKlineHistory(
    {
      ("BTCUSDT", "1m"): [
        rest_kline(now_open_ms - 120_000),
        rest_kline(now_open_ms - 60_000),
        # Binance's newest row is the bar still forming.
        rest_kline(now_open_ms),
      ]
    }
  )
  ingestion, _, _ = make_ingestion(FakeKlineStream(), history)

  _, bars = await ingestion.fetch_history("BTCUSDT", M1, 2)

  assert history.calls == [("BTCUSDT", "1m", 3)]
  assert [int(bar.open_time.timestamp() * 1000) for bar in bars] == [
    now_open_ms - 120_000,
    now_open_ms - 60_000,
  ]


async def test_history_never_returns_more_than_was_asked_for():
  rows = [rest_kline(T0_MS + step * 60_000) for step in range(6)]
  history = FakeKlineHistory({("BTCUSDT", "1m"): rows})
  ingestion, _, _ = make_ingestion(FakeKlineStream(), history)

  _, bars = await ingestion.fetch_history("BTCUSDT", M1, 2)

  assert [int(bar.open_time.timestamp() * 1000) for bar in bars] == [
    T0_MS + 4 * 60_000,
    T0_MS + 5 * 60_000,
  ]


async def test_a_short_first_page_ends_the_read():
  history = FakeKlineHistory({("BTCUSDT", "1m"): [rest_kline(T0_MS)]})
  ingestion, _, _ = make_ingestion(FakeKlineStream(), history)

  await ingestion.fetch_history("BTCUSDT", M1, 5000)

  # One request at Binance's own cap; it came back short, so there is no more.
  assert history.calls == [("BTCUSDT", "1m", 1000)]


async def test_history_past_one_klines_request_is_read_page_by_page():
  first_ms = T0_MS - 2_499 * 60_000
  rows = [rest_kline(first_ms + step * 60_000) for step in range(2_500)]
  history = FakeKlineHistory({("BTCUSDT", "1m"): rows})
  ingestion, _, _ = make_ingestion(FakeKlineStream(), history)

  _, bars = await ingestion.fetch_history("BTCUSDT", M1, 2_200)

  opens = [int(bar.open_time.timestamp() * 1000) for bar in bars]
  # Every bar asked for — not one klines request's worth — newest last, with
  # no gap and no repeat where two pages meet.
  assert len(bars) == 2_200
  assert opens == sorted(set(opens))
  assert opens[-1] == T0_MS
  assert opens[-1] - opens[0] == 2_199 * 60_000
  assert [limit for _, _, limit in history.calls] == [1000, 1000, 200]
  # Each page ends just before the oldest row already held.
  assert history.end_times == [
    None,
    T0_MS - 999 * 60_000 - 1,
    T0_MS - 1_999 * 60_000 - 1,
  ]


async def test_history_stops_where_the_venue_runs_out():
  rows = [rest_kline(T0_MS + step * 60_000) for step in range(1_500)]
  history = FakeKlineHistory({("BTCUSDT", "1m"): rows})
  ingestion, _, _ = make_ingestion(FakeKlineStream(), history)

  _, bars = await ingestion.fetch_history("BTCUSDT", M1, 5_000)

  assert len(bars) == 1_500
  assert len(history.calls) == 2


async def test_the_forming_bar_is_dropped_even_when_this_host_clock_runs_fast(
  monkeypatch,
):
  # The host cannot sync NTP, so its clock drifts. A clock a few seconds fast
  # must not turn the bar still forming into a "closed" one.
  forming_open_ms = int(utcnow().timestamp() * 1000) // 60_000 * 60_000
  # Five seconds past the forming bar's close, by this host's fast clock.
  fast_now = datetime.fromtimestamp((forming_open_ms + 65_000) / 1000, tz=UTC)
  history = FakeKlineHistory(
    {
      ("BTCUSDT", "1m"): [
        rest_kline(forming_open_ms - 60_000),
        rest_kline(forming_open_ms),
      ]
    }
  )
  ingestion, _, _ = make_ingestion(FakeKlineStream(), history)
  monkeypatch.setattr(
    "ingester.gateways.crypto.binance.ingestion.utcnow", lambda: fast_now
  )

  _, bars = await ingestion.fetch_history("BTCUSDT", M1, 5)

  assert [int(bar.open_time.timestamp() * 1000) for bar in bars] == [
    forming_open_ms - 60_000
  ]


async def test_the_newest_row_is_kept_when_it_closed_long_ago():
  # A symbol that stopped trading has no bar forming, so its newest row is a
  # closed bar and must not be mistaken for one.
  history = FakeKlineHistory(
    {("BTCUSDT", "1m"): [rest_kline(T0_MS - 60_000), rest_kline(T0_MS)]}
  )
  ingestion, _, _ = make_ingestion(FakeKlineStream(), history)

  _, bars = await ingestion.fetch_history("BTCUSDT", M1, 5)

  assert [int(bar.open_time.timestamp() * 1000) for bar in bars] == [
    T0_MS - 60_000,
    T0_MS,
  ]


async def test_a_page_that_repeats_bars_already_read_is_refused():
  # A venue ignoring end_time_ms hands back the same page forever. Answering
  # with what we hold would read as "this is all the history there is".
  rows = [rest_kline(T0_MS - step * 60_000) for step in range(1_000)]
  history = FakeKlineHistory({("BTCUSDT", "1m"): rows})

  async def klines(*, symbol, interval, limit, end_time_ms=None):
    history.calls.append((symbol, interval, limit))
    return rows[-limit:]

  history.klines = klines
  ingestion, _, _ = make_ingestion(FakeKlineStream(), history)

  with pytest.raises(HistoryUnavailableError, match="already read"):
    await ingestion.fetch_history("BTCUSDT", M1, 2_000)


async def test_a_read_that_outruns_its_budget_is_refused():
  rows = [rest_kline(T0_MS - step * 60_000) for step in range(3_000)]
  history = FakeKlineHistory({("BTCUSDT", "1m"): rows})
  ingestion, _, _ = make_ingestion(
    FakeKlineStream(), history, HISTORY_BUDGET_SECONDS=0.001
  )

  # Refused with the count read so far, not left for the subscriber to time
  # out on NATS_HISTORY_TIMEOUT.
  with pytest.raises(HistoryUnavailableError, match="ask for fewer"):
    await ingestion.fetch_history("BTCUSDT", M1, 3_000)
  assert len(history.calls) == 1


async def test_history_may_ask_for_a_timeframe_that_is_not_on_the_socket():
  history = FakeKlineHistory(
    {("BTCUSDT", "15m"): [rest_kline(T0_MS, interval_ms=900_000)]}
  )
  ingestion, _, _ = make_ingestion(FakeKlineStream(), history)

  _, bars = await ingestion.fetch_history("BTCUSDT", Timeframe.M15, 1)

  assert bars[0].close_time - bars[0].open_time == Timeframe.M15.delta


async def test_history_collapses_a_row_binance_repeats():
  history = FakeKlineHistory(
    {
      ("BTCUSDT", "1m"): [
        rest_kline(T0_MS - 60_000),
        rest_kline(T0_MS),
        rest_kline(T0_MS),
      ]
    }
  )
  ingestion, _, _ = make_ingestion(FakeKlineStream(), history)

  _, bars = await ingestion.fetch_history("BTCUSDT", M1, 5)
  assert len(bars) == 2


async def test_history_for_an_unconfigured_symbol_is_refused():
  ingestion, _, _ = make_ingestion(FakeKlineStream(), FakeKlineHistory())
  with pytest.raises(UnknownSymbolError, match="ETHUSDT"):
    await ingestion.fetch_history("ETHUSDT", M1, 5)


async def test_a_failed_klines_request_is_unavailable_and_the_socket_carries_on():
  history = FakeKlineHistory()
  history.error = GatewayConnectionError("429 Too Many Requests")
  stream = FakeKlineStream()
  ingestion, publisher, _ = make_ingestion(stream, history)
  await ingestion.start()
  await wait_for(lambda: stream.opened)

  with pytest.raises(HistoryUnavailableError, match="429"):
    await ingestion.fetch_history("BTCUSDT", M1, 5)

  stream.push(kline(T0_MS))
  await wait_for(lambda: len(publisher.events) == 1)
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)
  await ingestion.stop()


async def test_history_with_only_the_forming_bar_is_unavailable():
  now_open_ms = int(utcnow().timestamp() * 1000) // 60_000 * 60_000
  history = FakeKlineHistory({("BTCUSDT", "1m"): [rest_kline(now_open_ms)]})
  ingestion, _, _ = make_ingestion(FakeKlineStream(), history)

  with pytest.raises(HistoryUnavailableError, match="no closed bar"):
    await ingestion.fetch_history("BTCUSDT", M1, 5)
