from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class Notifier(Protocol):
  """Anything that can deliver a human-readable (HTML) message to operators."""

  async def send_message(self, message_text: str) -> None: ...
