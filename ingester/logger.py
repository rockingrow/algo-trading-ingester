"""
ingester/logger.py — Daily-file logger (optionally mirrored to stdout).

The day's file is always written. stdout is a *mirror*, kept behind
``LOG_CONSOLE`` because it is not free: ``logging`` is synchronous, so the write
happens on the thread that logged — the gateway's own poll thread — and a
console that cannot keep up holds that thread up. Under a supervisor that
already captures stdout (systemd, pm2, NSSM) the mirror is duplicate work, and
on Windows a console with Quick Edit enabled blocks every writer the moment
someone selects text in it.

Old day files are deleted by ``LOG_RETENTION_DAYS``, because nothing else ever
did: at ``DEBUG`` this service has written 400 MB in a single day, and the files
only accumulated.
"""

from __future__ import annotations

import copy
import datetime
import logging
import sys
from pathlib import Path

from ingester.settings import settings

LOGS_DIR = Path(settings.logging.DIR)

#: What a day file is called, and the only name shape pruning will delete.
_DATE_FORMAT = "%Y%m%d"


def prune_old_logs(
  directory: Path,
  retention_days: int,
  *,
  today: datetime.date | None = None,
) -> list[Path]:
  """Delete ``<directory>/<YYYYMMDD>.log`` older than *retention_days*.

  Returns the files deleted. ``retention_days <= 0`` keeps everything, which is
  the escape hatch for a host whose logs are someone else's to rotate.

  Only names that parse as a date are considered, so a file this service did
  not create — or did not create *as a day file* — is never touched. A file
  that cannot be deleted (open elsewhere, no permission) is skipped rather
  than raised on: losing the log is not worth losing the service.
  """
  if retention_days <= 0 or not directory.is_dir():
    return []

  cutoff = (today or datetime.date.today()) - datetime.timedelta(days=retention_days)
  deleted: list[Path] = []
  for path in sorted(directory.glob("*.log")):
    try:
      stamped = datetime.datetime.strptime(path.stem, _DATE_FORMAT).date()
    except ValueError:
      continue  # not a day file: ingester.out.log and anything else
    if stamped >= cutoff:
      continue
    try:
      path.unlink()
    except OSError:
      continue
    deleted.append(path)
  return deleted


class _DailyFileHandler(logging.FileHandler):
  """FileHandler that rolls to a new dated file at midnight without a restart."""

  def __init__(self, directory: Path, encoding: str = "utf-8", retention_days: int = 0):
    directory.mkdir(parents=True, exist_ok=True)
    self.directory = directory
    self.retention_days = retention_days
    self.current_date = datetime.datetime.now().strftime(_DATE_FORMAT)
    super().__init__(directory / f"{self.current_date}.log", "a", encoding)
    prune_old_logs(directory, retention_days)

  def emit(self, record: logging.LogRecord) -> None:
    new_date = datetime.datetime.now().strftime(_DATE_FORMAT)
    if new_date != self.current_date:
      self.close()
      self.current_date = new_date
      self.baseFilename = str(self.directory / f"{new_date}.log")
      self.stream = self._open()
      # A service that runs for weeks would otherwise only ever prune at the
      # restart it may never get.
      prune_old_logs(self.directory, self.retention_days)
    super().emit(record)


class _UtcStampMixin:
  """Stamp records as UTC, then the host's local clock and offset.

  The leading time is the record's epoch converted to UTC (never the local
  wall clock with a label pasted on); the bracket shows the same instant on
  the machine's clock, e.g. ``2026-10-05 00:21:59 UTC (07:21:59 UTC+07:00)``.
  """

  def formatTime(self, record: logging.LogRecord, datefmt: str | None = None):
    utc = datetime.datetime.fromtimestamp(record.created, datetime.timezone.utc)
    local = utc.astimezone()
    offset = local.utcoffset() or datetime.timedelta(0)
    minutes = int(offset.total_seconds() // 60)
    sign = "+" if minutes >= 0 else "-"
    hours, mins = divmod(abs(minutes), 60)
    return (
      f"{utc:%Y-%m-%d %H:%M:%S} UTC ({local:%H:%M:%S} UTC{sign}{hours:02d}:{mins:02d})"
    )


class _UtcFormatter(_UtcStampMixin, logging.Formatter):
  pass


def uvicorn_log_config() -> dict:
  """uvicorn's LOGGING_CONFIG with the same timestamp format as ours."""
  from uvicorn.config import LOGGING_CONFIG
  from uvicorn.logging import AccessFormatter, DefaultFormatter

  class _UtcDefaultFormatter(_UtcStampMixin, DefaultFormatter):
    pass

  class _UtcAccessFormatter(_UtcStampMixin, AccessFormatter):
    pass

  cfg = copy.deepcopy(LOGGING_CONFIG)
  fmt = "%(asctime)s | %(levelprefix)s %(message)s"
  classes = {"default": _UtcDefaultFormatter, "access": _UtcAccessFormatter}
  for name, formatter_class in classes.items():
    cfg["formatters"][name]["()"] = formatter_class
    cfg["formatters"][name]["fmt"] = fmt
  return cfg


ROOT_LOGGER = "ingester"


def _configure_root() -> logging.Logger:
  """Attach the file handler — and the stdout mirror, if asked — exactly once.

  Every module logger (``ingester.*``) propagates up to it, so there is exactly
  one open handle on the day's log file however many modules log.
  """
  root = logging.getLogger(ROOT_LOGGER)
  if root.handlers:
    return root

  level = getattr(logging, settings.logging.LEVEL.upper(), logging.INFO)
  root.setLevel(level)
  fmt = _UtcFormatter(
    fmt="%(asctime)s | %(levelname)-8s | %(threadName)s | %(name)s | %(message)s",
  )
  handlers: list[logging.Handler] = [
    _DailyFileHandler(LOGS_DIR, retention_days=settings.logging.RETENTION_DAYS)
  ]
  if settings.logging.CONSOLE:
    handlers.append(logging.StreamHandler(sys.stdout))
  for handler in handlers:
    handler.setLevel(level)
    handler.setFormatter(fmt)
    root.addHandler(handler)
  root.propagate = False
  return root


def attach_handler(handler: logging.Handler) -> None:
  """Add an extra handler to the package root — the Telegram error mirror.

  Only ``ingester.*`` records reach it: uvicorn keeps its own loggers, so an
  error raised inside the ASGI layer is not forwarded.
  """
  _configure_root().addHandler(handler)


def detach_handler(handler: logging.Handler) -> None:
  logging.getLogger(ROOT_LOGGER).removeHandler(handler)


def get_logger(name: str) -> logging.Logger:
  """Logger for *name*, parented under the configured ``ingester`` root."""
  _configure_root()
  if name != ROOT_LOGGER and not name.startswith(f"{ROOT_LOGGER}."):
    name = f"{ROOT_LOGGER}.{name}"
  return logging.getLogger(name)
