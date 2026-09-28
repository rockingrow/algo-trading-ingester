"""
ingester/interfaces/log_forwarder_protocol.py — Contract for the error mirror.

A logging handler that ships records somewhere off-process. It needs the event
loop that owns the delivery queue, and that loop only exists once the runtime
has started — hence :meth:`bind` / :meth:`unbind` rather than a constructor
argument. Keeping it here lets ``runtime.py`` install the mirror without
knowing that Telegram is what backs it.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Protocol, runtime_checkable


@runtime_checkable
class LogForwarder(Protocol):
  """A :class:`logging.Handler` the runtime binds to the loop, then attaches."""

  def bind(self, loop: asyncio.AbstractEventLoop) -> None: ...

  def unbind(self) -> None: ...

  def handle(self, record: logging.LogRecord) -> bool: ...
