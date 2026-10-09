"""
ingester/start.py — Start the ingester as a detached background process.

Entry point of ``make start`` (``uv run python -m ingester.start``). It is the
counterpart of :mod:`ingester.stop`: that one frees ``APP_PORT``, this one puts
a process on it and returns the shell immediately. ``make dev`` (``make run``)
stays the blocking foreground run, which is what Ctrl-C and an ordered shutdown
need.

Three deliberate choices:

* **Refuse to start a second one.** The port is checked with
  :func:`ingester.stop.find_pids` before spawning — the same probe ``make stop``
  uses — because a second run would otherwise die on *address already in use*
  seconds after this command reported success, long after the shell moved on.
* **The child outlives this shell.** ``DETACHED_PROCESS`` on Windows, a new
  session elsewhere, so closing the terminal (or a CI step ending) does not take
  the ingester with it. It is stopped with ``make stop``, which is why no PID
  file is written: ``APP_PORT`` already identifies the process.
* **Console output goes to a file.** A detached process has nowhere to write;
  anything the application logger has not taken over yet — an import error, a
  traceback from a failed start-up — would be lost. It is appended to
  ``<LOG_DIR>/ingester.out.log``, next to the application logs.

Starting is not waiting for *ready*: the command reports the PID it spawned.
Whether the gateways came up is in the logs and on ``GET /health``.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

from ingester.settings import AppSettings, LoggingSettings
from ingester.stop import find_pids

#: Name of the file the detached child's stdout and stderr are appended to.
CONSOLE_LOG_NAME = "ingester.out.log"

#: How long to watch the spawned child before reporting it as started. A
#: process that dies this fast died on start-up (a bad ``.env``, a port taken
#: between the check and the spawn), and reporting that as "started" would send
#: the operator to ``/health`` on a service that was never there.
_SETTLE_SECONDS = 1.5

#: Spawns the child; injected so tests never start a real process.
Spawner = Callable[[list[str], Path], "subprocess.Popen[bytes]"]


def _detached_flags() -> dict[str, object]:
  """Platform options that cut the child loose from this console."""
  if sys.platform.startswith("win"):
    # DETACHED_PROCESS gives it no console at all; the new group keeps a
    # Ctrl-C in this terminal from reaching it.
    return {
      "creationflags": (
        subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
      )
    }
  # A new session detaches it from the terminal's job control the same way.
  return {"start_new_session": True}


def spawn_detached(command: list[str], console_log: Path) -> subprocess.Popen[bytes]:
  """Start *command* detached, appending its console output to *console_log*."""
  console_log.parent.mkdir(parents=True, exist_ok=True)
  handle = console_log.open("ab")
  try:
    return subprocess.Popen(
      command,
      stdin=subprocess.DEVNULL,
      stdout=handle,
      stderr=subprocess.STDOUT,
      cwd=Path.cwd(),
      **_detached_flags(),  # type: ignore[arg-type]
    )
  finally:
    # The child holds its own duplicate of the descriptor; this one is ours.
    handle.close()


def start_detached(
  port: int,
  console_log: Path,
  *,
  spawn: Spawner | None = None,
  settle_seconds: float = _SETTLE_SECONDS,
  sleep: Callable[[float], None] = time.sleep,
) -> int:
  """Spawn a detached ingester and report it. Returns a process exit code."""
  # Resolved here, not as a default, so a test can replace the module-level
  # spawner — a default bound at definition time would start a real process.
  spawn = spawn or spawn_detached
  held_by = sorted(find_pids(port))
  if held_by:
    print(
      f"Port {port} is already held by PID "
      f"{', '.join(str(pid) for pid in held_by)}: the ingester looks like it "
      "is already running. Use 'make stop' first."
    )
    return 1

  command = [sys.executable, "-m", "ingester"]
  try:
    process = spawn(command, console_log)
  except OSError as error:
    print(f"Could not start the ingester: {error}")
    return 1

  sleep(settle_seconds)
  exit_code = process.poll()
  if exit_code is not None:
    print(
      f"The ingester exited immediately (code {exit_code}). "
      f"See {console_log} for what it printed."
    )
    return 1

  print(f"Ingester started in the background: PID {process.pid}, port {port}.")
  print(f"  console output: {console_log}")
  print("  stop it with: make stop")
  return 0


def main(argv: list[str] | None = None) -> int:
  parser = argparse.ArgumentParser(
    prog="python -m ingester.start",
    description=(
      "Start the ingester as a detached background process (APP_PORT in .env)."
    ),
  )
  parser.add_argument(
    "--port",
    type=int,
    default=None,
    help="TCP port to check before starting; defaults to APP_PORT from .env",
  )
  args = parser.parse_args(argv)
  port = args.port if args.port is not None else AppSettings().PORT
  console_log = Path(LoggingSettings().DIR) / CONSOLE_LOG_NAME
  return start_detached(port, console_log)


if __name__ == "__main__":
  raise SystemExit(main())
