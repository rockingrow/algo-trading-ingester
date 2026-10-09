"""
ingester/services/notification_service.py — Operator notifications.

Three :class:`~ingester.interfaces.Notifier` implementations:

* :class:`TelegramNotifier` — sends HTML messages through the Bot API.
* :class:`NullNotifier` — used when ``TELEGRAM_ENABLED=false``; drops everything.
* :class:`QueuedNotifier` — decorates any notifier with a bounded queue and a
  background worker, so a slow or blocked ``api.telegram.org`` can never stall
  the ingestion pipeline or a NATS reconnect callback.

:class:`TelegramLogHandler` is the separate path for failures: a logging
handler that mirrors ERROR records into their own chat, so an operator does not
have to spot them between the lifecycle messages.

Notifications are best-effort: no method here raises.
"""

from __future__ import annotations

import asyncio
import logging
import re
import threading
import time
import traceback
from dataclasses import dataclass
from typing import NamedTuple

import httpx

from ingester.helpers import messages
from ingester.interfaces.notifier_protocol import Notifier
from ingester.logger import get_logger
from ingester.settings import TelegramSettings

log = get_logger(__name__)

# ``<chat id>_<topic id>`` addresses a topic in a forum supergroup, the shape a
# topic link shows (t.me/c/2173777783/924584 → -1002173777783_924584). Only a
# numeric id may carry it: ``@my_group_2`` is a username, not a topic.
_CHAT_TOPIC_RE = re.compile(r"(?P<chat>-?\d+)_(?P<topic>\d+)")


class ChatTarget(NamedTuple):
  """One Telegram destination: a chat, optionally a forum topic inside it."""

  chat_id: str
  message_thread_id: int | None = None


def parse_chat_targets(raw: str | None) -> list[ChatTarget]:
  """Parse ``"-100111,-100222_924584,@channel"`` into de-duplicated targets."""
  targets: list[ChatTarget] = []
  for spec in (raw or "").split(","):
    spec = spec.strip()
    if not spec:
      continue
    match = _CHAT_TOPIC_RE.fullmatch(spec)
    target = (
      ChatTarget(match["chat"], int(match["topic"])) if match else ChatTarget(spec)
    )
    if target not in targets:
      targets.append(target)
  return targets


class NullNotifier:
  """Notifier that does nothing — keeps callers free of ``if enabled`` checks."""

  async def send_message(self, message_text: str) -> None:
    log.debug("Notification (disabled): %s", message_text)


class TelegramNotifier:
  """Sends HTML messages to every configured chat via the Telegram Bot API."""

  API_URL = "https://api.telegram.org/bot{token}/sendMessage"

  def __init__(
    self,
    config: TelegramSettings,
    *,
    bot_token: str | None = None,
    chat_ids: str | None = None,
  ) -> None:
    # The overrides let the error channel reuse this class with its own token
    # and chats without a second settings model.
    self._token = config.BOT_TOKEN if bot_token is None else bot_token
    self._targets = parse_chat_targets(
      config.CHAT_IDS if chat_ids is None else chat_ids
    )
    self._timeout = config.HTTP_TIMEOUT
    if not self._token or not self._targets:
      log.warning("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_IDS must both be set")

  async def send_message(self, message_text: str) -> None:
    if not self._token or not self._targets:
      return
    url = self.API_URL.format(token=self._token)
    try:
      async with httpx.AsyncClient(timeout=self._timeout) as client:
        await asyncio.gather(
          *(
            self._deliver(client, url, target, message_text) for target in self._targets
          )
        )
    except Exception as exc:
      log.warning("Telegram send failed: %s", exc)

  async def _deliver(
    self, client: httpx.AsyncClient, url: str, target: ChatTarget, text: str
  ) -> None:
    payload: dict[str, object] = {
      "chat_id": target.chat_id,
      "text": text,
      "parse_mode": "HTML",
      "disable_web_page_preview": True,
    }
    if target.message_thread_id is not None:
      payload["message_thread_id"] = target.message_thread_id
    try:
      response = await client.post(url, json=payload)
      if response.status_code != 200:
        log.warning(
          "Telegram rejected message for chat %s: %s %s",
          target.chat_id,
          response.status_code,
          response.text[:200],
        )
    except httpx.HTTPError as exc:
      # Never log the URL: it embeds the bot token.
      log.warning(
        "Telegram request for chat %s failed: %s", target.chat_id, type(exc).__name__
      )


class QueuedNotifier:
  """Decorator: ``send_message`` enqueues and returns; a worker delivers.

  ``stop`` drains what is queued (bounded by *drain_timeout*) so the final
  "service stopped" message still goes out on shutdown.
  """

  def __init__(
    self, inner: Notifier, *, maxsize: int = 100, drain_timeout: float = 10.0
  ) -> None:
    self._inner = inner
    self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=maxsize)
    self._drain_timeout = drain_timeout
    self._worker: asyncio.Task[None] | None = None

  async def start(self) -> None:
    if self._worker is None:
      self._worker = asyncio.create_task(self._run(), name="notifier")

  async def stop(self) -> None:
    if self._worker is None:
      return
    try:
      await asyncio.wait_for(self._queue.join(), timeout=self._drain_timeout)
    except TimeoutError:
      log.warning("%d notification(s) dropped on shutdown", self._queue.qsize())
    self._worker.cancel()
    try:
      await self._worker
    except asyncio.CancelledError:
      pass
    self._worker = None

  async def send_message(self, message_text: str) -> None:
    try:
      self._queue.put_nowait(message_text)
    except asyncio.QueueFull:
      log.warning("Notification queue full, dropping message")

  async def _run(self) -> None:
    while True:
      text = await self._queue.get()
      try:
        await self._inner.send_message(text)
      except Exception as exc:
        log.warning("Notifier failed: %s", exc)
      finally:
        self._queue.task_done()


class TelegramLogHandler(logging.Handler):
  """Mirrors ERROR-level log records into their own Telegram chat.

  Installed on the package root logger, so it sees records from every thread —
  including the gateway's watcher thread. Delivery is handed to the event loop
  and then to *notifier*'s queue, so a blocked ``api.telegram.org`` can never
  slow a poll down.

  **De-duplication is on the log template, not the formatted message.** Inside
  ``dedup_window``, the first record of a given ``logger:level:msg`` is sent and
  the rest are counted. The spam this exists for repeats with a *different*
  text every time — ``"Failed to publish %s: %s"`` carries a fresh ``event_id``
  per bar, so one unreachable NATS turned every bar close into its own message
  — and keying on the formatted message never collapsed any of it. The cost is
  that two different failures sharing one template read as one message plus a
  count; the full text of each is in the day's log file either way.

  The count is not dropped: the next record on that key, after the window has
  passed, says how many were suppressed meanwhile.
  """

  #: Records from this module are never forwarded — a failing Telegram send
  #: must not try to report itself over Telegram.
  _SELF = __name__

  def __init__(
    self,
    notifier: Notifier,
    *,
    instance_id: str,
    dedup_window: float = 60.0,
  ) -> None:
    super().__init__(logging.ERROR)
    self._notifier = notifier
    self._instance_id = instance_id
    self._dedup_window = dedup_window
    self._seen: dict[str, _DedupState] = {}
    self._lock = threading.Lock()
    self._loop: asyncio.AbstractEventLoop | None = None
    self._tasks: set[asyncio.Task[None]] = set()

  def bind(self, loop: asyncio.AbstractEventLoop) -> None:
    """Point the handler at the loop that owns the notifier's queue."""
    self._loop = loop

  def unbind(self) -> None:
    self._loop = None

  def emit(self, record: logging.LogRecord) -> None:
    try:
      if record.name.startswith(self._SELF):
        return
      loop = self._loop
      if loop is None or loop.is_closed():
        return
      suppressed, repeats = self._decide(record)
      if suppressed:
        return
      message = record.getMessage()
      if repeats:
        # Said on the first record that gets through after the window, so a
        # collapsed burst is visible as a burst rather than as one failure.
        window = self._dedup_window
        message = f"{message}\n({repeats} more like this in the last {window:.0f}s)"
      text = messages.log_error(
        instance_id=self._instance_id,
        logger_name=record.name,
        level=record.levelname,
        message=message,
        traceback_text=_format_exception(record),
      )
      loop.call_soon_threadsafe(self._send, text)
    except Exception:
      # logging must never raise into the caller's code path.
      self.handleError(record)

  def _decide(self, record: logging.LogRecord) -> tuple[bool, int]:
    """``(suppress this record, how many were suppressed before it)``.

    The key is the *unformatted* ``record.msg``, so every call of one log
    statement shares it however its arguments differ.
    """
    key = f"{record.name}:{record.levelno}:{record.msg}"
    now = time.monotonic()
    with self._lock:
      cutoff = now - self._dedup_window
      for seen, state in list(self._seen.items()):
        if state.at <= cutoff and seen != key:
          # Forgotten unsent: nothing is left to attribute the count to, and
          # the window has passed. Keeping it would report a stale number later.
          del self._seen[seen]
      state = self._seen.get(key)
      if state is not None and state.at > cutoff:
        state.repeats += 1
        return True, 0
      repeats = state.repeats if state is not None else 0
      self._seen[key] = _DedupState(at=now, repeats=0)
    return False, repeats

  def _send(self, text: str) -> None:
    task = asyncio.create_task(self._notifier.send_message(text))
    self._tasks.add(task)
    task.add_done_callback(self._tasks.discard)


@dataclass
class _DedupState:
  """When a log key was last sent, and how many were dropped since."""

  at: float
  repeats: int = 0


def _format_exception(record: logging.LogRecord) -> str | None:
  if not record.exc_info:
    return None
  return "".join(traceback.format_exception(*record.exc_info))
