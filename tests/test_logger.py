"""The logger's two policies: the optional stdout mirror and log retention."""

import datetime
import logging
import sys

import pytest

from ingester import logger

# ── Retention ──────────────────────────────────────────────────────


def day_files(directory, *stamps):
  """Create ``<stamp>.log`` for each stamp, with a byte in it."""
  for stamp in stamps:
    (directory / f"{stamp}.log").write_text("x", encoding="utf-8")


def test_deletes_only_the_files_older_than_the_window(tmp_path):
  day_files(tmp_path, "20261001", "20261009", "20261010", "20261015")
  today = datetime.date(2026, 10, 15)

  deleted = logger.prune_old_logs(tmp_path, 7, today=today)

  # The cutoff is today - 7 days = 2026-10-08, and it is inclusive: a file
  # stamped exactly on it is still inside the window.
  assert [path.name for path in deleted] == ["20261001.log"]
  assert sorted(p.name for p in tmp_path.glob("*.log")) == [
    "20261009.log",
    "20261010.log",
    "20261015.log",
  ]


def test_the_cutoff_day_itself_is_kept(tmp_path):
  day_files(tmp_path, "20261008", "20261007")
  today = datetime.date(2026, 10, 15)

  deleted = logger.prune_old_logs(tmp_path, 7, today=today)

  assert [path.name for path in deleted] == ["20261007.log"]


def test_retention_of_zero_keeps_everything(tmp_path):
  day_files(tmp_path, "20200101", "20261015")

  assert logger.prune_old_logs(tmp_path, 0, today=datetime.date(2026, 10, 15)) == []
  assert len(list(tmp_path.glob("*.log"))) == 2


@pytest.mark.parametrize("retention", [0, -1, -30])
def test_a_non_positive_window_never_deletes(tmp_path, retention):
  day_files(tmp_path, "20200101")

  assert logger.prune_old_logs(tmp_path, retention) == []
  assert list(tmp_path.glob("*.log"))


def test_a_name_that_is_not_a_date_is_never_touched(tmp_path):
  # A file this service did not create as a day file is not ours to delete.
  day_files(tmp_path, "20200101")
  for name in ("ingester.out.log", "notes.log", "2026-10-01.log", "202601.log"):
    (tmp_path / name).write_text("keep me", encoding="utf-8")

  deleted = logger.prune_old_logs(tmp_path, 7, today=datetime.date(2026, 10, 15))

  assert [path.name for path in deleted] == ["20200101.log"]
  assert (tmp_path / "ingester.out.log").is_file()
  assert (tmp_path / "notes.log").is_file()
  assert (tmp_path / "2026-10-01.log").is_file()
  assert (tmp_path / "202601.log").is_file()


def test_a_missing_directory_is_not_an_error(tmp_path):
  assert logger.prune_old_logs(tmp_path / "nope", 7) == []


def test_a_file_that_cannot_be_deleted_is_skipped(tmp_path, monkeypatch):
  # Losing a log file is not worth losing the service.
  day_files(tmp_path, "20200101", "20200102")
  real_unlink = logger.Path.unlink

  def refuse(self, *args, **kwargs):
    if self.name == "20200101.log":
      raise PermissionError("held open by something else")
    return real_unlink(self, *args, **kwargs)

  monkeypatch.setattr(logger.Path, "unlink", refuse)

  deleted = logger.prune_old_logs(tmp_path, 7, today=datetime.date(2026, 10, 15))

  assert [path.name for path in deleted] == ["20200102.log"]
  assert (tmp_path / "20200101.log").is_file()


def test_the_file_handler_prunes_when_it_opens(tmp_path):
  day_files(tmp_path, "20200101")

  handler = logger._DailyFileHandler(tmp_path, retention_days=7)
  try:
    assert not (tmp_path / "20200101.log").exists()
    # Today's file is the one it just opened, and it survives its own pruning.
    today = datetime.datetime.now().strftime("%Y%m%d")
    assert (tmp_path / f"{today}.log").is_file()
  finally:
    handler.close()


def test_the_file_handler_keeps_everything_by_default(tmp_path):
  day_files(tmp_path, "20200101")

  handler = logger._DailyFileHandler(tmp_path)
  try:
    assert (tmp_path / "20200101.log").is_file()
  finally:
    handler.close()


# ── The stdout mirror ──────────────────────────────────────────────


def configured_handlers(monkeypatch, *, console, retention=0):
  """Configure the package root from scratch and return its handlers."""
  monkeypatch.setattr(logger.settings.logging, "CONSOLE", console, raising=False)
  monkeypatch.setattr(
    logger.settings.logging, "RETENTION_DAYS", retention, raising=False
  )
  root = logging.getLogger(logger.ROOT_LOGGER)
  existing = list(root.handlers)
  for handler in existing:
    root.removeHandler(handler)
  try:
    return list(logger._configure_root().handlers)
  finally:
    for handler in list(root.handlers):
      if handler not in existing:
        root.removeHandler(handler)
        handler.close()
    for handler in existing:
      root.addHandler(handler)


def test_the_console_mirror_is_attached_when_asked(monkeypatch):
  handlers = configured_handlers(monkeypatch, console=True)

  streams = [h for h in handlers if type(h) is logging.StreamHandler]
  assert len(streams) == 1
  assert streams[0].stream is sys.stdout
  assert any(isinstance(h, logger._DailyFileHandler) for h in handlers)


def test_the_day_file_is_written_even_with_the_mirror_off(monkeypatch):
  # LOG_CONSOLE only drops the mirror: the file is the log, not a copy of it.
  handlers = configured_handlers(monkeypatch, console=False)

  assert not [h for h in handlers if type(h) is logging.StreamHandler]
  assert len([h for h in handlers if isinstance(h, logger._DailyFileHandler)]) == 1
