"""
ingester/stop.py — Force-stop the ingester by freeing its HTTP port.

Entry point of ``make stop`` (``uv run python -m ingester.stop``). The port is
not hard-coded: it comes from ``APP_PORT`` in ``.env`` through
:class:`~ingester.settings.AppSettings` — the very value ``make run`` binds, so
the two can never drift apart. ``--port`` overrides it for a one-off.

Two deliberate choices:

* **Every process on the port, not just the newest listener.** A detached or
  crashed run can leave a worker still holding the socket, and the next
  ``make run`` then dies on *address already in use*. Only the socket's
  **local** port is matched, so a process that merely *connected* to someone
  else's ``:8090`` — a browser tab on another host's ``/status``, a monitoring
  agent — is never killed. PID 0/4 (Windows ``TIME_WAIT`` entries and
  ``System``) are skipped too: they are not ours to kill. Nothing else is
  filtered, so whatever else is on that port goes down with it — point
  ``APP_PORT`` at a port of this service's own.
* **The kill is hard** (``taskkill /F /T`` on Windows, ``SIGKILL`` elsewhere),
  because that is what frees a wedged port. The cost is that the process skips
  its ordered shutdown: no Telegram *stopped* notification and no NATS drain.
  Published bars are already gone downstream and nothing local is buffered to
  disk, so a restart loses no data — but stop the process with Ctrl-C when you
  want the notification.

``AppSettings`` is read on its own rather than through ``settings``: stopping a
port must not depend on the market TOML files loading.
"""

from __future__ import annotations

import argparse
import os
import re
import signal
import subprocess
import sys
from collections.abc import Callable

from ingester.settings import AppSettings

#: True when running on Windows, where ports are read with ``netstat`` and
#: processes killed with ``taskkill`` instead of a signal.
IS_WINDOWS = sys.platform.startswith("win")

#: Windows names these as the owner of a socket that has no process we may
#: kill: 0 for ``TIME_WAIT``/idle entries, 4 for the ``System`` process.
UNKILLABLE_PIDS = frozenset({0, 4})

#: The signal the POSIX branch sends. Windows has no ``SIGKILL`` and takes the
#: ``taskkill`` branch instead, but the attribute is resolved here so the module
#: imports — and its POSIX path stays testable — on either platform.
_SIGKILL = getattr(signal, "SIGKILL", signal.SIGTERM)

#: ``ss`` writes the owner as ``users:(("python",pid=1234,fd=7))``.
_SS_PID = re.compile(r"pid=(\d+)")

#: A command runner: returns the exit code and the combined output.
Runner = Callable[[list[str]], tuple[int, str]]


def run_command(command: list[str]) -> tuple[int, str]:
  """Run *command*, returning its exit code and ``stdout + stderr``.

  A missing tool is not an error: it comes back as a non-zero code with an
  empty output, and the caller falls through to the next probe.
  """
  try:
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
  except OSError as error:
    return 1, str(error)
  return completed.returncode, completed.stdout + completed.stderr


def parse_netstat(output: str, port: int) -> set[int]:
  """PIDs from ``netstat -ano`` whose **local** address ends in ``:port``.

  Rows look like ``TCP  0.0.0.0:8090  0.0.0.0:0  LISTENING  12345`` (and
  ``[::]:8090`` for IPv6, ``UDP`` with one column less). The foreign address is
  ignored on purpose — the same port in it means we are the client.
  """
  suffix = f":{port}"
  pids: set[int] = set()
  for line in output.splitlines():
    columns = line.split()
    if len(columns) < 4 or columns[0].upper() not in ("TCP", "UDP"):
      continue
    if not columns[1].endswith(suffix):
      continue
    try:
      pids.add(int(columns[-1]))
    except ValueError:
      continue
  return pids


def parse_lsof(output: str) -> set[int]:
  """PIDs from ``lsof -t``: one PID per line, nothing else."""
  pids: set[int] = set()
  for token in output.split():
    try:
      pids.add(int(token))
    except ValueError:
      continue
  return pids


def parse_ss(output: str) -> set[int]:
  """PIDs from ``ss -tanp``, read out of its ``users:((…pid=1234…))`` column."""
  return {int(pid) for pid in _SS_PID.findall(output)}


def _killable(pids: set[int]) -> set[int]:
  """Drop the PIDs that must never be killed, including this process."""
  return {pid for pid in pids if pid > 0 and pid not in UNKILLABLE_PIDS} - {os.getpid()}


def find_pids(
  port: int, run: Runner = run_command, windows: bool = IS_WINDOWS
) -> set[int]:
  """Every PID whose socket has *port* as its **local** port.

  ``ss`` is asked first because ``sport = :<port>`` says exactly that; ``lsof``
  is the fallback for a host without it (macOS), and there it is narrowed to
  ``-sTCP:LISTEN``. Plain ``lsof -iTCP:<port>`` would also match a socket whose
  *foreign* port is this one — a client connected to some other host's
  ``:8090`` — and killing that is not this command's business. The fallback is
  therefore the listener only, which is what has to go for the port to be free.
  """
  if windows:
    return _killable(parse_netstat(run(["netstat", "-ano"])[1], port))

  pids = parse_ss(run(["ss", "-tanp", f"sport = :{port}"])[1])
  if not pids:
    pids = parse_lsof(run(["lsof", "-t", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"])[1])
  return _killable(pids)


def kill_pid(
  pid: int,
  run: Runner = run_command,
  windows: bool = IS_WINDOWS,
  send_signal: Callable[[int, int], None] = os.kill,
) -> tuple[bool, str]:
  """Force-kill *pid*. Returns whether it is gone, and why not when it is not.

  A process that died between the listing and the kill counts as gone: the
  port is free either way.
  """
  if windows:
    # /T takes the children with it — uvicorn's reloader holds the socket in a
    # child process, and killing only the parent leaves the port bound.
    code, output = run(["taskkill", "/F", "/T", "/PID", str(pid)])
    if code == 0:
      return True, ""
    if "not found" in output.lower():
      return True, ""
    return False, output.strip() or f"taskkill exited {code}"

  try:
    send_signal(pid, _SIGKILL)
  except ProcessLookupError:
    return True, ""
  except OSError as error:  # PermissionError: another user's process
    return False, str(error)
  return True, ""


def stop_port(
  port: int,
  run: Runner = run_command,
  windows: bool = IS_WINDOWS,
  send_signal: Callable[[int, int], None] = os.kill,
) -> int:
  """Kill everything on *port* and report it. Returns a process exit code."""
  pids = sorted(find_pids(port, run=run, windows=windows))
  if not pids:
    print(f"Port {port} is free: nothing to stop.")
    return 0

  print(f"Port {port} is held by PID {', '.join(str(pid) for pid in pids)}.")
  failures: list[str] = []
  for pid in pids:
    killed, reason = kill_pid(pid, run=run, windows=windows, send_signal=send_signal)
    if killed:
      print(f"  killed {pid}")
    else:
      failures.append(f"  PID {pid}: {reason}")

  if failures:
    print(f"Could not stop every process on port {port}:")
    print("\n".join(failures))
    print("Run the command again from an elevated shell.")
    return 1

  remaining = sorted(find_pids(port, run=run, windows=windows))
  if remaining:
    print(
      f"Port {port} is still held by PID "
      f"{', '.join(str(pid) for pid in remaining)}: run the command again."
    )
    return 1

  print(f"Port {port} is free.")
  return 0


def main(argv: list[str] | None = None) -> int:
  parser = argparse.ArgumentParser(
    prog="python -m ingester.stop",
    description=(
      "Force-stop the ingester: kill every process holding its HTTP port "
      "(APP_PORT in .env)."
    ),
  )
  parser.add_argument(
    "--port",
    type=int,
    default=None,
    help="TCP port to free; defaults to APP_PORT from .env",
  )
  args = parser.parse_args(argv)
  port = args.port if args.port is not None else AppSettings().PORT
  return stop_port(port)


if __name__ == "__main__":
  raise SystemExit(main())
