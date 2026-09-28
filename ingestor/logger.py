"""
ingestor/logger.py — Console + daily-file logger, shared by every module.
"""

from __future__ import annotations

import copy
import datetime
import logging
import sys
from pathlib import Path

from ingestor.settings import settings

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


def _tz_suffix() -> str:
  """Local timezone abbreviation or UTC offset, e.g. ``ICT`` or ``+0700``."""
  now = datetime.datetime.now(datetime.timezone.utc).astimezone()
  return now.strftime("%Z") or now.strftime("%z")


def uvicorn_log_config() -> dict:
  """uvicorn's LOGGING_CONFIG with the same timestamp format as ours."""
  from uvicorn.config import LOGGING_CONFIG

  cfg = copy.deepcopy(LOGGING_CONFIG)
  fmt = f"%(asctime)s {_tz_suffix()} | %(levelprefix)s %(message)s"
  for name in ("default", "access"):
    cfg["formatters"][name]["fmt"] = fmt
    cfg["formatters"][name]["datefmt"] = "%Y-%m-%d %H:%M:%S"
  return cfg


ROOT_LOGGER = "ingestor"


def _configure_root() -> logging.Logger:
  """Attach the console + file handlers once, to the package's root logger.

  Every module logger (``ingestor.*``) propagates up to it, so there is exactly
  one open handle on the day's log file however many modules log.
  """
  root = logging.getLogger(ROOT_LOGGER)
  if root.handlers:
    return root

  level = getattr(logging, settings.logging.LEVEL.upper(), logging.INFO)
  root.setLevel(level)
  fmt = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(threadName)s | %(name)s | %(message)s",
    datefmt=f"%Y-%m-%d %H:%M:%S {_tz_suffix()}",
  )
  for handler in (logging.StreamHandler(sys.stdout), _DailyFileHandler(LOGS_DIR)):
    handler.setLevel(level)
    handler.setFormatter(fmt)
    root.addHandler(handler)
  root.propagate = False
  return root


def get_logger(name: str) -> logging.Logger:
  """Logger for *name*, parented under the configured ``ingestor`` root."""
  _configure_root()
  if name != ROOT_LOGGER and not name.startswith(f"{ROOT_LOGGER}."):
    name = f"{ROOT_LOGGER}.{name}"
  return logging.getLogger(name)
