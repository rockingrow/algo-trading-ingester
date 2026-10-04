"""The shared ingestion bases, exercised through a minimal venue."""

import asyncio
from typing import Any, ClassVar

import pytest
from pydantic import ValidationError

from ingester.core import AsyncStreamIngestion, BaseBarDTO, GatewayConnectionError
from ingester.gateways.crypto.binance import BinanceKlineDTO
from ingester.gateways.forex.mt5 import Mt5RateDTO
from ingester.interfaces import BarDTO
from ingester.schemas import GatewayEnum, GatewayStatusEnum, Timeframe
from ingester.settings import BinanceSettings
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


def make_ingestion(frames: list[Any]) -> tuple[ScriptedIngestion, FakeNotifier]:
  notifier = FakeNotifier()
  config = BinanceSettings(_env_file=None, ENABLE=True, SYMBOLS=["BTCUSDT"])
  ingestion = ScriptedIngestion(
    frames,
    config=config,
    reconnect_interval=0.01,
    publisher=FakePublisher(),
    notifier=notifier,
    instance_id="test",
  )
  return ingestion, notifier


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
