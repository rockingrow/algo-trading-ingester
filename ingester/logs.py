"""
ingester/logs.py — Follow the service log, the way ``tail -f`` would.

Entry point of ``make logging`` (``uv run python -m ingester.logs``). It exists
instead of a plain ``tail -f`` for two reasons:

* **The log rolls by date.** :class:`~ingester.logger._DailyFileHandler` writes
  ``<LOG_DIR>/<YYYYMMDD>.log`` and opens a new file at midnight, so a reader
  pinned to one path goes quiet exactly when the service does not. This follows
  the *newest* file instead: when a later date appears in the directory, it
  switches to it and says so.
* **Windows has no ``tail``.** ``Get-Content -Wait`` is PowerShell-only and the
  Makefile is run from several shells; a Python reader behaves the same
  everywhere, which is what the rest of the Makefile does too.

``--grep`` keeps only the lines matching a regular expression, which is what
makes the command usable at ``LOG_LEVEL=DEBUG``: the MT5 gateway dumps whole
rate arrays, and ``--grep "Published|ERROR"`` turns that back into something a
person can watch.

``--lines`` decides how much of the existing file is printed before following
(0 for nothing, only what arrives). ``--no-follow`` makes it print that tail
and exit, which is the form to pipe into ``grep``. ``--console`` follows the
detached run's console file (``make start``) rather than the application log:
that is where a start-up crash lands, before the logger owns the output.

Reading is decoded leniently (``errors="replace"``): a line being written while
it is read can split a multi-byte character, and a log reader must not die of
it. Nothing here writes to the log directory.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from collections.abc import Callable
from pathlib import Path

from ingester.settings import LoggingSettings
from ingester.start import CONSOLE_LOG_NAME

#: ``20261009.log`` — the daily file's name, and the order it sorts in.
_DAILY_LOG = re.compile(r"^\d{8}\.log$")

#: How often the file is checked for new bytes, and the directory for a newer
#: file. Short enough to read as realtime, long enough to idle at no cost.
_POLL_SECONDS = 0.25


def newest_log(directory: Path) -> Path | None:
  """The highest-dated ``<YYYYMMDD>.log`` in *directory*, if any.

  The name sorts chronologically, so the last one is the current day's — and
  if the service has not written today yet, the most recent day it did.
  """
  candidates = sorted(
    path for path in directory.glob("*.log") if _DAILY_LOG.match(path.name)
  )
  return candidates[-1] if candidates else None


def tail_lines(path: Path, lines: int) -> str:
  """The last *lines* lines of *path*, read from the end.

  Only the tail is read, so following a log that has grown for weeks costs the
  same as following a fresh one.
  """
  if lines <= 0:
    return ""
  block = 8192
  with path.open("rb") as handle:
    handle.seek(0, 2)
    size = handle.tell()
    data = b""
    while size > 0 and data.count(b"\n") <= lines:
      step = min(block, size)
      size -= step
      handle.seek(size)
      data = handle.read(step) + data
  text = data.decode("utf-8", errors="replace")
  return "\n".join(text.splitlines()[-lines:])


def _emit(text: str) -> None:
  """Print *text* and flush it.

  stdout is block-buffered when it is a pipe, so ``make logging | grep`` would
  otherwise show nothing until 4 KiB had accumulated — a follower has to come
  out line by line whatever it is attached to.
  """
  print(text, flush=True)


def matching(text: str, pattern: re.Pattern[str] | None) -> str:
  """*text*'s lines that match *pattern*; all of them when there is none.

  Line endings are normalised on the way through: the log is written in text
  mode, so on Windows every line carries a ``\\r`` that would otherwise be
  printed and piped along.
  """
  lines = text.splitlines()
  if pattern is not None:
    lines = [line for line in lines if pattern.search(line)]
  return "\n".join(lines)


def follow(
  directory: Path,
  *,
  lines: int = 50,
  follow_new: bool = True,
  fixed_path: Path | None = None,
  pattern: re.Pattern[str] | None = None,
  write: Callable[[str], None] | None = None,
  sleep: Callable[[float], None] = time.sleep,
  stop_after: int | None = None,
) -> int:
  """Print the tail of the current log and keep printing what is appended.

  *fixed_path* follows one file and never rolls to another — the detached
  run's console log, which has no date in its name. *stop_after* bounds the
  number of poll iterations, which is what makes this testable; left at
  ``None`` it follows until the reader is interrupted.
  """
  write = write or _emit
  path = fixed_path or newest_log(directory)
  if path is None:
    write(
      f"No log file in {directory} yet. The service writes "
      f"<LOG_DIR>/<YYYYMMDD>.log on its first log line."
    )
    return 1

  if path.is_file():
    write(f"── {path} ──")
    tail = matching(tail_lines(path, lines), pattern)
    if tail:
      write(tail)
  position = path.stat().st_size if path.is_file() else 0

  if not follow_new:
    return 0

  polls = 0
  while stop_after is None or polls < stop_after:
    polls += 1
    sleep(_POLL_SECONDS)

    # A newer day's file wins over the one being read: that is where the
    # service is writing now, and the old one will never grow again.
    if fixed_path is None:
      current = newest_log(directory)
      if current is not None and current != path:
        path, position = current, 0
        write(f"── {path} ──")

    if not path.is_file():
      continue
    size = path.stat().st_size
    if size < position:
      # Truncated or replaced under us; start over rather than seek past it.
      write(f"── {path} was truncated, following from its start ──")
      position = 0
    if size == position:
      continue
    with path.open("rb") as handle:
      handle.seek(position)
      chunk = handle.read()
      position = handle.tell()
    text = chunk.decode("utf-8", errors="replace")
    text = matching(text.removesuffix("\n"), pattern)
    if text:
      write(text)
  return 0


def main(argv: list[str] | None = None) -> int:
  parser = argparse.ArgumentParser(
    prog="python -m ingester.logs",
    description=(
      "Follow the ingester's log (LOG_DIR in .env), rolling to the new file "
      "at midnight."
    ),
  )
  parser.add_argument(
    "-n",
    "--lines",
    type=int,
    default=50,
    help="lines of the existing file to print first (default: 50)",
  )
  parser.add_argument(
    "--no-follow",
    dest="follow",
    action="store_false",
    help="print that tail and exit, instead of following",
  )
  parser.add_argument(
    "--console",
    action="store_true",
    help=f"follow <LOG_DIR>/{CONSOLE_LOG_NAME} (what a detached run printed)",
  )
  parser.add_argument(
    "--grep",
    default=None,
    help="only lines matching this regular expression (case-insensitive)",
  )
  parser.add_argument(
    "--dir",
    type=Path,
    default=None,
    help="log directory; defaults to LOG_DIR from .env",
  )
  args = parser.parse_args(argv)

  # The log carries arrows and emoji; a Windows console defaults to cp1252 and
  # would raise on the first one. Replace what it cannot show instead.
  if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

  directory = args.dir if args.dir is not None else Path(LoggingSettings().DIR)
  if not directory.is_dir():
    print(f"No log directory at {directory}: the service has not run yet.")
    return 1

  fixed_path = directory / CONSOLE_LOG_NAME if args.console else None
  if fixed_path is not None and not fixed_path.is_file():
    print(f"No console log at {fixed_path}: the service was not started detached.")
    return 1

  try:
    pattern = re.compile(args.grep, re.IGNORECASE) if args.grep else None
  except re.error as error:
    print(f"--grep is not a valid regular expression: {error}")
    return 2

  try:
    return follow(
      directory,
      lines=args.lines,
      follow_new=args.follow,
      fixed_path=fixed_path,
      pattern=pattern,
    )
  except KeyboardInterrupt:
    # Ctrl-C is how this command ends, not a failure to report.
    print()
    return 0


if __name__ == "__main__":
  raise SystemExit(main())
