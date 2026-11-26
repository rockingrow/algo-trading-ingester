"""The shared ingestion bases, exercised through a minimal venue."""

import asyncio
import logging
import threading
from typing import Any, ClassVar

import pytest
from pydantic import ValidationError

from ingester.core import (
  AsyncStreamIngestion,
  BaseBarDTO,
  GatewayConnectionError,
  ThreadedIngestion,
)
from ingester.gateways.crypto.binance import BinanceKlineDTO
from ingester.gateways.forex.mt5 import Mt5RateDTO
from ingester.interfaces import BarDTO
from ingester.schemas import GatewayEnum, GatewayStatusEnum, Timeframe
from ingester.settings import BinanceSettings, Mt5Settings
from tests.fakes import FakeNotifier, FakePublisher, kline, rate, wait_for

M1 = Timeframe.M1


class ScriptedIngestion(AsyncStreamIngestion):
  """A venue whose frames are plain values, queued by the test."""

  gateway: ClassVar[GatewayEnum] = GatewayEnum.BINANCE

  def __init__(self, frames: list[Any], **kwargs: Any) -> None:
    super().__init__(**kwargs)
    self.frames: asyncio.Queue[Any] = asyncio.Queue()
    for frame in frames:
      self.frames.put_nowait(frame)
    self.connects = 0
    self.disconnects = 0
    self.connect_error: Exception | None = None
    self.handled: list[Any] = []

  async def connect(self) -> None:
    self.connects += 1
    if self.connect_error is not None:
      error, self.connect_error = self.connect_error, None
      raise error

  async def receive(self) -> Any:
    frame = await self.frames.get()
    if isinstance(frame, Exception):
      raise frame
    return frame

  def handle(self, frame: Any) -> None:
    if frame == "bad":
      raise ValueError("malformed frame")
    self.handled.append(frame)

  async def disconnect(self) -> None:
    self.disconnects += 1


def make_ingestion(
  frames: list[Any], **overrides: Any
) -> tuple[ScriptedIngestion, FakeNotifier]:
  notifier = FakeNotifier()
  config = BinanceSettings(_env_file=None, ENABLE=True, SYMBOLS=["BTCUSDT"])
  fields: dict[str, Any] = {
    "reconnect_interval": 0.01,
    "publisher": FakePublisher(),
    "notifier": notifier,
    "instance_id": "test",
    # One attempt unless a test is about retrying: the others are about what
    # a failure leaves in the log, and would otherwise wait out the delays.
    "publish_attempts": 1,
  }
  ingestion = ScriptedIngestion(frames, config=config, **{**fields, **overrides})
  return ingestion, notifier


def bar(open_epoch: int) -> Any:
  """One canonical bar, built the way the Binance gateway builds it."""
  raw = kline(open_epoch * 1000)["data"]["k"]
  return BinanceKlineDTO.model_validate(raw).to_bar(M1)


class CollectingHandler(logging.Handler):
  """Records straight off the dispatcher's logger.

  Attached to the module's own logger, not the root: ``ingester`` does not
  propagate, so ``caplog`` would see nothing.
  """

  def __init__(self) -> None:
    super().__init__(logging.DEBUG)
    self.records: list[logging.LogRecord] = []

  def emit(self, record: logging.LogRecord) -> None:
    self.records.append(record)

  def levels_for(self, needle: str) -> list[str]:
    return [
      record.levelname for record in self.records if needle in record.getMessage()
    ]


@pytest.fixture
def dispatcher_logs():
  handler = CollectingHandler()
  logger = logging.getLogger("ingester.core.ingestion")
  logger.addHandler(handler)
  try:
    yield handler
  finally:
    logger.removeHandler(handler)


async def test_a_run_of_identical_publish_failures_is_one_error(dispatcher_logs):
  # An unreachable NATS fails every bar, and every ERROR is mirrored to
  # Telegram: the first failure is the news, the rest are the same outage.
  ingestion, _ = make_ingestion([])
  ingestion._publisher.fail_with = ConnectionError("NATS is not connected")
  await ingestion.start()
  for open_time in (60, 120, 180):
    ingestion.emit_bar("BTCUSDT", M1, bar(open_time))
  await wait_for(lambda: len(dispatcher_logs.levels_for("Failed to publish")) == 3)
  await ingestion.stop()

  assert dispatcher_logs.levels_for("Failed to publish") == [
    "ERROR",
    "WARNING",
    "WARNING",
  ]
  assert ingestion.snapshot()["failed"] == 3


async def test_a_different_failure_is_reported_again(dispatcher_logs):
  ingestion, _ = make_ingestion([])
  ingestion._publisher.fail_with = ConnectionError("NATS is not connected")
  await ingestion.start()
  ingestion.emit_bar("BTCUSDT", M1, bar(60))
  await wait_for(lambda: len(dispatcher_logs.levels_for("Failed to publish")) == 1)

  ingestion._publisher.fail_with = TimeoutError("ack timed out")
  ingestion.emit_bar("BTCUSDT", M1, bar(120))
  await wait_for(lambda: len(dispatcher_logs.levels_for("Failed to publish")) == 2)
  await ingestion.stop()

  assert dispatcher_logs.levels_for("Failed to publish") == ["ERROR", "ERROR"]


async def test_a_successful_publish_ends_the_run(dispatcher_logs):
  ingestion, _ = make_ingestion([])
  ingestion._publisher.fail_with = ConnectionError("NATS is not connected")
  await ingestion.start()
  ingestion.emit_bar("BTCUSDT", M1, bar(60))
  await wait_for(lambda: len(dispatcher_logs.levels_for("Failed to publish")) == 1)

  ingestion._publisher.fail_with = None
  ingestion.emit_bar("BTCUSDT", M1, bar(120))
  await wait_for(lambda: ingestion._publisher.events != [])

  ingestion._publisher.fail_with = ConnectionError("NATS is not connected")
  ingestion.emit_bar("BTCUSDT", M1, bar(180))
  await wait_for(lambda: len(dispatcher_logs.levels_for("Failed to publish")) == 2)
  await ingestion.stop()

  assert dispatcher_logs.levels_for("Failed to publish") == ["ERROR", "ERROR"]


async def test_a_failed_publish_is_retried_before_the_bar_is_given_up(
  dispatcher_logs,
):
  # The bar has closed and the venue will not push it again: dropping it on
  # the first failure leaves a hole until a subscriber asks for history. The
  # retry is safe because event_id de-duplicates on the far side.
  ingestion, _ = make_ingestion([], publish_attempts=3, publish_retry_delay=0)
  publisher = ingestion._publisher
  publisher.fail_with = ConnectionError("NATS is not connected")
  publisher.fail_times = 1  # the next attempt is allowed through
  await ingestion.start()
  ingestion.emit_bar("BTCUSDT", M1, bar(60))

  await wait_for(lambda: publisher.events != [])
  await ingestion.stop()

  assert len(publisher.attempts) == 2
  assert ingestion.snapshot()["published"] == 1
  assert ingestion.snapshot()["failed"] == 0
  # Only the attempt that gave up is an error; a retry that worked is not.
  assert dispatcher_logs.levels_for("Failed to publish") == []
  assert dispatcher_logs.levels_for("retrying in") == ["WARNING"]


async def test_the_attempts_are_bounded():
  ingestion, _ = make_ingestion([], publish_attempts=3, publish_retry_delay=0)
  publisher = ingestion._publisher
  publisher.fail_with = ConnectionError("NATS is not connected")
  await ingestion.start()
  ingestion.emit_bar("BTCUSDT", M1, bar(60))
  await wait_for(lambda: ingestion.snapshot()["failed"] == 1)
  await ingestion.stop()

  assert len(publisher.attempts) == 3


async def test_a_bar_the_broker_already_had_is_not_counted_as_published():
  # JetStream acknowledges a duplicate and stores nothing: no consumer is
  # handed it, so it must not read as a bar that was delivered.
  ingestion, _ = make_ingestion([])
  ingestion._publisher.stored = False
  await ingestion.start()
  ingestion.emit_bar("BTCUSDT", M1, bar(60))
  await wait_for(lambda: ingestion.snapshot()["duplicates"] == 1)
  await ingestion.stop()

  snapshot = ingestion.snapshot()
  assert (snapshot["published"], snapshot["failed"]) == (0, 0)


async def test_async_stream_is_running_on_the_first_frame_not_the_handshake():
  ingestion, _ = make_ingestion([])
  await ingestion.start()
  await wait_for(lambda: ingestion.connects == 1)
  assert ingestion.status is GatewayStatusEnum.STARTING

  ingestion.frames.put_nowait(1)
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)
  await ingestion.stop()
  assert ingestion.handled == [1]


async def test_async_stream_reconnects_after_the_session_drops():
  ingestion, notifier = make_ingestion([1, GatewayConnectionError("gone"), 2])
  await ingestion.start()
  await wait_for(lambda: ingestion.handled == [1, 2])
  await wait_for(lambda: ingestion.status is GatewayStatusEnum.RUNNING)
  await ingestion.stop()

  assert ingestion.connects == 2
  assert any("disconnected" in message for message in notifier.messages)


async def test_async_stream_retries_a_failed_connect():
  ingestion, _ = make_ingestion([1])
  ingestion.connect_error = GatewayConnectionError("refused")
  await ingestion.start()
  await wait_for(lambda: ingestion.handled == [1])
  await ingestion.stop()
  assert ingestion.connects == 2


async def test_async_stream_survives_a_frame_its_gateway_rejects():
  # One malformed frame is logged and skipped; the session is kept.
  ingestion, _ = make_ingestion([1, "bad", 2])
  await ingestion.start()
  await wait_for(lambda: ingestion.handled == [1, 2])
  await ingestion.stop()
  assert ingestion.connects == 1


async def test_async_stream_stop_disconnects_and_ends_the_task():
  ingestion, _ = make_ingestion([])
  await ingestion.start()
  await wait_for(lambda: ingestion.connects == 1)
  await ingestion.stop()
  assert ingestion.disconnects >= 1
  assert ingestion.status is GatewayStatusEnum.STOPPED
  assert ingestion._task is None


class StuckIngestion(ThreadedIngestion):
  """A blocking venue whose poll does not return until the test lets it."""

  gateway: ClassVar[GatewayEnum] = GatewayEnum.MT5

  def __init__(self, **kwargs: Any) -> None:
    super().__init__(**kwargs)
    self.release = threading.Event()
    self.polls = 0

  def connect(self) -> None:
    return None

  def poll(self) -> None:
    self.polls += 1
    self.release.wait(5.0)

  def disconnect(self) -> None:
    return None


async def test_a_gateway_whose_thread_will_not_stop_is_not_started_again():
  # Clearing the stop event under a thread still inside a venue call would
  # wake it back up, and two threads would then share one process-global SDK
  # session — the MetaTrader5 case ThreadedIngestion exists for.
  ingestion = StuckIngestion(
    config=Mt5Settings(_env_file=None, SYMBOLS=["XAUUSD"]),
    poll_interval=0.01,
    reconnect_interval=0.01,
    join_timeout=0.05,
    publisher=FakePublisher(),
    notifier=FakeNotifier(),
    instance_id="test",
  )
  await ingestion.start()
  await wait_for(lambda: ingestion.polls == 1)
  thread = ingestion._thread
  await ingestion.stop()
  assert thread is not None and thread.is_alive()

  with pytest.raises(GatewayConnectionError, match="has not stopped"):
    await ingestion.start()

  assert ingestion.status is GatewayStatusEnum.FAILED
  ingestion.release.set()
  await asyncio.to_thread(thread.join, 5.0)
  assert not thread.is_alive()


def test_a_stream_remembers_only_bars_newer_than_the_last_one():
  ingestion, _ = make_ingestion([])
  assert ingestion._open_mark("BTCUSDT", M1) is None
  assert ingestion._is_new_bar("BTCUSDT", M1, 0)

  ingestion._remember_bar("BTCUSDT", M1, 100)
  assert ingestion._open_mark("BTCUSDT", M1) == 100
  assert not ingestion._is_new_bar("BTCUSDT", M1, 100)  # a repeat
  assert not ingestion._is_new_bar("BTCUSDT", M1, 40)  # a replayed older bar
  assert ingestion._is_new_bar("BTCUSDT", M1, 160)
  # Streams are independent: another timeframe or symbol starts from nothing.
  assert ingestion._is_new_bar("BTCUSDT", Timeframe.M5, 100)
  assert ingestion._is_new_bar("ETHUSDT", M1, 100)


def test_base_dto_cannot_be_used_without_a_to_bar():
  with pytest.raises(TypeError):
    BaseBarDTO()


@pytest.mark.parametrize(
  "dto",
  [
    Mt5RateDTO.from_record(rate(60), "UTC"),
    BinanceKlineDTO.model_validate(kline(60_000)["data"]["k"]),
  ],
)
def test_every_gateway_dto_inherits_the_strict_base(dto):
  assert isinstance(dto, BaseBarDTO)
  assert isinstance(dto, BarDTO)
  config = type(dto).model_config
  assert config["frozen"] is True
  assert config["extra"] == "ignore"
  assert config["allow_inf_nan"] is False
  with pytest.raises(ValidationError):
    dto.close = 1.0
  bar = dto.to_bar(M1)
  assert bar.close_time - bar.open_time == M1.delta


def test_a_price_that_is_not_a_number_is_refused_by_every_dto():
  with pytest.raises(ValidationError):
    Mt5RateDTO.from_record({**rate(60), "close": float("nan")}, "UTC")
  with pytest.raises(ValidationError):
    BinanceKlineDTO.model_validate({**kline(60_000)["data"]["k"], "c": "NaN"})
