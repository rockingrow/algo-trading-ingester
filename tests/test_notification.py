import asyncio
import logging
import sys

from ingester.services.notification_service import (
  ChatTarget,
  QueuedNotifier,
  TelegramLogHandler,
  TelegramNotifier,
  parse_chat_targets,
)
from ingester.settings import TelegramSettings
from tests.fakes import FakeNotifier


def test_parse_chat_targets():
  assert parse_chat_targets(" -1001, -1002_924584,@my_group_2,, -1001 ") == [
    ChatTarget("-1001"),
    ChatTarget("-1002", 924584),
    ChatTarget("@my_group_2"),
  ]
  assert parse_chat_targets("") == []


async def test_queued_notifier_delivers_and_drains_on_stop():
  inner = FakeNotifier()
  notifier = QueuedNotifier(inner)
  await notifier.start()
  await notifier.send_message("one")
  await notifier.send_message("two")
  await notifier.stop()
  assert inner.messages == ["one", "two"]


async def test_queued_notifier_drops_when_full():
  inner = FakeNotifier()
  notifier = QueuedNotifier(inner, maxsize=1)
  await notifier.send_message("kept")
  await notifier.send_message("dropped")  # worker not started: queue is full
  await notifier.start()
  await notifier.stop()
  assert inner.messages == ["kept"]


async def test_queued_notifier_survives_inner_failure():
  class Boom:
    calls = 0

    async def send_message(self, text):
      Boom.calls += 1
      raise RuntimeError("telegram down")

  notifier = QueuedNotifier(Boom())
  await notifier.start()
  await notifier.send_message("a")
  await notifier.send_message("b")
  await notifier.stop()
  assert Boom.calls == 2


async def test_telegram_notifier_posts_to_each_target(monkeypatch):
  import httpx

  requests = []

  def handler(request: httpx.Request) -> httpx.Response:
    requests.append(request)
    return httpx.Response(200, json={"ok": True})

  real_client = httpx.AsyncClient
  monkeypatch.setattr(
    httpx,
    "AsyncClient",
    lambda **kw: real_client(transport=httpx.MockTransport(handler), **kw),
  )
  notifier = TelegramNotifier(
    TelegramSettings(_env_file=None, BOT_TOKEN="T", CHAT_IDS="-1,-2_7")
  )
  await notifier.send_message("<b>hi</b>")
  await asyncio.sleep(0)

  import json

  bodies = sorted((json.loads(r.content) for r in requests), key=lambda b: b["chat_id"])
  assert [b["chat_id"] for b in bodies] == ["-1", "-2"]
  assert bodies[1]["message_thread_id"] == 7
  assert all(b["parse_mode"] == "HTML" for b in bodies)
  assert str(requests[0].url) == "https://api.telegram.org/botT/sendMessage"


class RecordingNotifier:
  def __init__(self) -> None:
    self.messages: list[str] = []

  async def send_message(self, message_text: str) -> None:
    self.messages.append(message_text)


def _handler(notifier, **kwargs):
  handler = TelegramLogHandler(notifier, instance_id="vps-test", **kwargs)
  handler.bind(asyncio.get_running_loop())
  return handler


async def _settle():
  # emit() hops through call_soon_threadsafe, then the send task runs.
  for _ in range(3):
    await asyncio.sleep(0)


def _record(message: str, *, name: str = "ingester.core.ingestion", exc=None):
  return logging.LogRecord(
    name=name,
    level=logging.ERROR,
    pathname=__file__,
    lineno=1,
    msg=message,
    args=(),
    exc_info=exc,
  )


async def test_error_record_is_forwarded_with_context():
  notifier = RecordingNotifier()
  _handler(notifier).emit(_record("publish failed"))
  await _settle()

  (message,) = notifier.messages
  assert "publish failed" in message
  assert "ingester.core.ingestion" in message
  assert "vps-test" in message


async def test_identical_errors_are_deduplicated_inside_the_window():
  notifier = RecordingNotifier()
  handler = _handler(notifier, dedup_window=60.0)
  for _ in range(5):
    handler.emit(_record("NATS error: connection refused"))
  await _settle()
  assert len(notifier.messages) == 1


async def test_one_log_statement_is_deduplicated_however_its_text_differs():
  # The spam this exists for: "Failed to publish %s" carries a new event_id
  # per bar, so an unreachable NATS used to send one message per closed bar.
  notifier = RecordingNotifier()
  handler = _handler(notifier, dedup_window=60.0)
  for open_time in range(5):
    record = _record("Failed to publish %s: %s")
    record.args = (f"mt5:XAUUSD:M1:{open_time}", "NATS is not connected")
    handler.emit(record)
  await _settle()

  (message,) = notifier.messages
  assert "mt5:XAUUSD:M1:0" in message


async def test_the_next_message_says_how_many_were_suppressed():
  notifier = RecordingNotifier()
  handler = _handler(notifier, dedup_window=60.0)
  handler.emit(_record("Failed to publish %s"))
  for _ in range(3):
    handler.emit(_record("Failed to publish %s"))
  handler._dedup_window = 0.0  # the window has passed
  handler.emit(_record("Failed to publish %s"))
  await _settle()

  first, second = notifier.messages
  assert "more like this" not in first
  assert "3 more like this" in second


async def test_different_errors_are_not_deduplicated():
  notifier = RecordingNotifier()
  handler = _handler(notifier, dedup_window=60.0)
  handler.emit(_record("first"))
  handler.emit(_record("second"))
  await _settle()
  assert len(notifier.messages) == 2


async def test_dedup_window_expires():
  notifier = RecordingNotifier()
  handler = _handler(notifier, dedup_window=0.0)
  handler.emit(_record("same"))
  handler.emit(_record("same"))
  await _settle()
  assert len(notifier.messages) == 2


async def test_the_notifiers_own_failures_are_not_forwarded():
  # Otherwise a broken Telegram reports itself over Telegram, forever.
  notifier = RecordingNotifier()
  _handler(notifier).emit(
    _record("Telegram send failed", name="ingester.services.notification_service")
  )
  await _settle()
  assert notifier.messages == []


async def test_traceback_is_included():
  notifier = RecordingNotifier()
  try:
    raise ValueError("boom")
  except ValueError:
    exc_info = sys.exc_info()
  _handler(notifier).emit(_record("poll failed", exc=exc_info))
  await _settle()

  (message,) = notifier.messages
  assert "ValueError" in message and "boom" in message


async def test_unbound_handler_drops_instead_of_raising():
  notifier = RecordingNotifier()
  handler = TelegramLogHandler(notifier, instance_id="vps-test")
  handler.emit(_record("before the loop exists"))
  await _settle()
  assert notifier.messages == []
