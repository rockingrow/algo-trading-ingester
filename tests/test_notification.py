import asyncio

from ingestor.services.notification_service import (
  ChatTarget,
  QueuedNotifier,
  TelegramNotifier,
  parse_chat_targets,
)
from ingestor.settings import TelegramSettings
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
