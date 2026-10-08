"""
ingester/shutdown.py — Ask *this* process to stop, gracefully and for good.

Used when the ingester decides it must not keep running: NATS stayed
unreachable long enough that it is publishing nothing and only an operator can
fix it (see ``NATS_GIVE_UP_AFTER_ATTEMPTS``).

It raises ``SIGINT`` in this process — the same signal Ctrl-C sends — rather
than exiting on the spot, because that path runs the whole ordered shutdown:
uvicorn's handler sets ``should_exit``, the FastAPI lifespan unwinds and
:meth:`~ingester.runtime.IngesterRuntime.stop` stops the gateways, drains NATS
and flushes the Telegram queue. Killing the process outright would drop the
notification that explains *why* it stopped, which is the one thing the
operator needs.

``signal.raise_signal`` and not ``os.kill``: on Windows ``os.kill`` with
``SIGTERM`` calls ``TerminateProcess`` (nothing graceful about it), and
``CTRL_C_EVENT`` goes to every process in the console group, not just this one.

Nothing here restarts the service — that is the point. Not to be confused with
:mod:`ingester.stop`, the ``make stop`` command, which runs *outside* the
process and frees the HTTP port by force.
"""

from __future__ import annotations

import signal

from ingester.logger import get_logger

log = get_logger(__name__)


def request_shutdown(reason: str) -> None:
  """Start this process's graceful shutdown, logging *reason* first.

  Returns immediately: the signal is handled by uvicorn on the main thread,
  so the caller — a callback or a watchdog task — stays short.
  """
  log.error("Stopping the ingester: %s", reason)
  signal.raise_signal(signal.SIGINT)
