import pytest

from ingester.core.errors import GatewayConnectionError
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
  warmup_series,
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


# ── backfill_on_start ───────────────────────────────────────────────


async def test_no_backfill_without_the_option():
  # The default: nothing is read over REST, so a restart publishes only the
  # bars that close from here on.
  history = FakeKlineHistory({("BTCUSDT", "1m"): [rest_kline(T0_MS)]})
  stream = FakeKlineStream([kline(T0_MS + 60_000)])
  ingestion, publisher, _ = make_ingestion(stream, history)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()

  assert history.calls == []
  assert int(publisher.events[0].bar.open_time.timestamp()) == (T0_MS + 60_000) // 1000


async def test_backfill_publishes_history_then_live_bars():
  # The point of the option: one continuous series, backfill first.
  history = FakeKlineHistory(
    {
      ("BTCUSDT", "1m"): [
        rest_kline(T0_MS),
        rest_kline(T0_MS + 60_000),
        # Binance always returns the bar still forming last; its close time is
        # in the future, so it must not be published.
        rest_kline(int(utcnow().timestamp() * 1000) // 60_000 * 60_000),
      ]
    }
  )
  stream = FakeKlineStream([kline(T0_MS + 120_000)])
  ingestion, publisher, _ = make_ingestion(
    stream, history, BACKFILL_ON_START=True, WARMUP_BARS=16
  )
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 3)
  await ingestion.stop()

  opens = [int(e.bar.open_time.timestamp()) for e in publisher.events]
  assert opens == [
    T0_MS // 1000,
    (T0_MS + 60_000) // 1000,
    (T0_MS + 120_000) // 1000,
  ]
  # What REST handed back is the numbered warm-up window, oldest first; what
  # the socket delivered afterwards is live. The subscriber ends its warm-up on
  # the bar where index == total.
  assert warmup_series(publisher) == [
    (True, 1, 2),
    (True, 2, 2),
    (False, None, None),
  ]
  bar = publisher.events[0].bar
  assert (bar.open, bar.high, bar.low, bar.close) == (100.0, 101.0, 99.0, 100.5)
  assert bar.quote_volume == 150.0
  assert bar.tick_count == 42
  # close_time is derived, not Binance's "last millisecond of the bar".
  assert int(bar.close_time.timestamp()) == (T0_MS + 60_000) // 1000


async def test_backfill_reads_every_stream_one_bar_past_the_window():
  history = FakeKlineHistory()
  ingestion, _, _ = make_ingestion(
    FakeKlineStream(),
    history,
    SYMBOLS=["BTCUSDT", "ETHUSDT"],
    TIMEFRAMES=[M1, Timeframe.M15],
    BACKFILL_ON_START=True,
    WARMUP_BARS=4,
  )
  await ingestion.start()
  await wait_for(lambda: len(history.calls) == 4)
  await ingestion.stop()

  # limit is warmup_bars + 1: the extra row is the forming bar, dropped.
  assert history.calls == [
    ("BTCUSDT", "1m", 5),
    ("BTCUSDT", "15m", 5),
    ("ETHUSDT", "1m", 5),
    ("ETHUSDT", "15m", 5),
  ]


async def test_warmup_total_is_the_window_binance_returned_not_warmup_bars():
  # Three closed bars of history against warmup_bars = 16. A total of 16 would
  # leave the subscriber warming up forever.
  history = FakeKlineHistory(
    {("BTCUSDT", "1m"): [rest_kline(T0_MS + i * 60_000) for i in range(3)]}
  )
  ingestion, publisher, _ = make_ingestion(
    FakeKlineStream(), history, BACKFILL_ON_START=True, WARMUP_BARS=16
  )
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 3)
  await ingestion.stop()

  assert warmup_series(publisher) == [(True, 1, 3), (True, 2, 3), (True, 3, 3)]


async def test_a_repeated_row_does_not_shorten_the_warmup_series():
  # A response that repeats an open time must not leave the series one short of
  # the warmup_total it announces.
  history = FakeKlineHistory(
    {
      ("BTCUSDT", "1m"): [
        rest_kline(T0_MS),
        rest_kline(T0_MS),
        rest_kline(T0_MS + 60_000),
      ]
    }
  )
  ingestion, publisher, _ = make_ingestion(
    FakeKlineStream(), history, BACKFILL_ON_START=True, WARMUP_BARS=16
  )
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 2)
  await ingestion.stop()

  assert warmup_series(publisher) == [(True, 1, 2), (True, 2, 2)]


async def test_a_broken_row_leaves_the_whole_warmup_window_unpublished():
  # Same rule as MT5: a series that stops short of its own warmup_total strands
  # a subscriber that ends its warm-up on index == total.
  broken = rest_kline(T0_MS + 60_000)
  broken[2] = "0"  # high below the open — the schema rejects the bar
  history = FakeKlineHistory(
    {("BTCUSDT", "1m"): [rest_kline(T0_MS), broken, rest_kline(T0_MS + 120_000)]}
  )
  stream = FakeKlineStream([kline(T0_MS + 180_000)])
  ingestion, publisher, _ = make_ingestion(
    stream, history, BACKFILL_ON_START=True, WARMUP_BARS=16
  )
  await ingestion.start()
  # The live socket is unaffected — a REST failure must never cost it bars.
  await wait_for(lambda: len(publisher.events) == 1)
  await ingestion.stop()

  assert warmup_series(publisher) == [(False, None, None)]


async def test_backfill_never_exceeds_warmup_bars():
  history = FakeKlineHistory(
    {("BTCUSDT", "1m"): [rest_kline(T0_MS + i * 60_000) for i in range(6)]}
  )
  ingestion, publisher, _ = make_ingestion(
    FakeKlineStream(), history, BACKFILL_ON_START=True, WARMUP_BARS=2
  )
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 2)
  await ingestion.stop()

  # The newest two closed bars, not the whole response.
  opens = [int(e.bar.open_time.timestamp()) for e in publisher.events]
  assert opens == [(T0_MS + 4 * 60_000) // 1000, (T0_MS + 5 * 60_000) // 1000]


async def test_a_backfilled_bar_the_socket_repeats_is_published_once():
  history = FakeKlineHistory({("BTCUSDT", "1m"): [rest_kline(T0_MS)]})
  # The socket replays the same close, then delivers the next one.
  stream = FakeKlineStream([kline(T0_MS), kline(T0_MS + 60_000)])
  ingestion, publisher, _ = make_ingestion(stream, history, BACKFILL_ON_START=True)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 2)
  await ingestion.stop()

  opens = [int(e.bar.open_time.timestamp()) for e in publisher.events]
  assert opens == [T0_MS // 1000, (T0_MS + 60_000) // 1000]


async def test_backfill_does_not_repeat_on_reconnect():
  history = FakeKlineHistory({("BTCUSDT", "1m"): [rest_kline(T0_MS)]})
  stream = FakeKlineStream(
    [
      kline(T0_MS + 60_000),
      GatewayConnectionError("socket closed"),
      kline(T0_MS + 120_000),
    ]
  )
  ingestion, publisher, _ = make_ingestion(stream, history, BACKFILL_ON_START=True)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 3)
  await wait_for(lambda: len(stream.opened) >= 2)
  await ingestion.stop()

  # The stream was fed by the socket, so the reconnect re-reads nothing: one
  # REST call in total, not one per dial.
  assert len(history.calls) == 1


async def test_a_failed_backfill_does_not_cost_the_socket():
  history = FakeKlineHistory()
  history.error = GatewayConnectionError("429 Too Many Requests")
  stream = FakeKlineStream([kline(T0_MS)])
  ingestion, publisher, _ = make_ingestion(stream, history, BACKFILL_ON_START=True)
  await ingestion.start()
  await wait_for(lambda: len(publisher.events) == 1)
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)
  await ingestion.stop()

  assert int(publisher.events[0].bar.open_time.timestamp()) == T0_MS // 1000


async def test_a_stream_with_no_closed_history_publishes_nothing():
  # Only the bar still forming came back.
  now_open_ms = int(utcnow().timestamp() * 1000) // 60_000 * 60_000
  history = FakeKlineHistory({("BTCUSDT", "1m"): [rest_kline(now_open_ms)]})
  ingestion, publisher, _ = make_ingestion(
    FakeKlineStream(), history, BACKFILL_ON_START=True
  )
  await ingestion.start()
  await wait_for(lambda: history.calls)
  await ingestion.stop()

  assert publisher.events == []
