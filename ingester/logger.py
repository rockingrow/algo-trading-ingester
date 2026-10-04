"""
ingester/logger.py — Console + daily-file logger, shared by every module.
"""

from __future__ import annotations

import copy
import datetime
import logging
import sys
from pathlib import Path

from ingester.settings import settings

LOGS_DIR = Path(settings.logging.DIR)


class _DailyFileHandler(logging.FileHandler):
  """FileHandler that rolls to a new dated file at midnight without a restart."""

  def __init__(self, directory: Path, encoding: str = "utf-8"):
    directory.mkdir(parents=True, exist_ok=True)
    self.directory = directory
    self.current_date = datetime.datetime.now().strftime("%Y%m%d")
    super().__init__(directory / f"{self.current_date}.log", "a", encoding)

  def emit(self, record: logging.LogRecord) -> None:
    new_date = datetime.datetime.now().strftime("%Y%m%d")
    if new_date != self.current_date:
      self.close()
      self.current_date = new_date
      self.baseFilename = str(self.directory / f"{new_date}.log")
      self.stream = self._open()
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
  """Attach the console + file handlers once, to the package's root logger.

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
  for handler in (logging.StreamHandler(sys.stdout), _DailyFileHandler(LOGS_DIR)):
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
