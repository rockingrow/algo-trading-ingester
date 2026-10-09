"""The ``make logging`` command: which file is followed, and what it prints."""

from pathlib import Path

from ingester import logs


def write(path: Path, text: str) -> None:
  with path.open("a", encoding="utf-8") as handle:
    handle.write(text)


def recorder():
  """A ``write`` that records, in place of printing."""
  written: list[str] = []
  return written, written.append


# ── Choosing the file ──────────────────────────────────────────────


def test_the_newest_dated_file_is_the_one_followed(tmp_path):
  for name in ("20261007.log", "20261009.log", "20261008.log"):
    write(tmp_path / name, "x\n")
  # Not a daily log: a console log must never be picked by date.
  write(tmp_path / logs.CONSOLE_LOG_NAME, "x\n")

  assert logs.newest_log(tmp_path) == tmp_path / "20261009.log"


def test_no_log_file_yet_is_reported(tmp_path):
  written, write_line = recorder()

  assert logs.follow(tmp_path, write=write_line, sleep=lambda _: None) == 1
  assert "No log file" in written[0]


# ── The initial tail ───────────────────────────────────────────────


def test_only_the_last_lines_are_printed(tmp_path):
  path = tmp_path / "20261009.log"
  write(path, "".join(f"line {number}\n" for number in range(100)))

  assert logs.tail_lines(path, 3) == "line 97\nline 98\nline 99"
  assert logs.tail_lines(path, 0) == ""


def test_the_tail_is_printed_under_the_file_name(tmp_path):
  write(tmp_path / "20261009.log", "first\nsecond\n")
  written, write_line = recorder()

  code = logs.follow(tmp_path, lines=5, follow_new=False, write=write_line)

  assert code == 0
  assert "20261009.log" in written[0]
  assert written[1] == "first\nsecond"


# ── Following ──────────────────────────────────────────────────────


def test_appended_lines_are_printed_as_they_arrive(tmp_path):
  path = tmp_path / "20261009.log"
  write(path, "old\n")
  written, write_line = recorder()
  appended = []

  def sleep(_seconds: float) -> None:
    # Stands in for the service writing between two polls.
    appended.append(1)
    write(path, f"new {len(appended)}\n")

  logs.follow(tmp_path, lines=1, write=write_line, sleep=sleep, stop_after=2)

  assert written[-2:] == ["new 1", "new 2"]


def test_midnight_rolls_to_the_new_file(tmp_path):
  # What plain "tail -f" cannot do: the logger opens a new dated file and the
  # old one never grows again.
  write(tmp_path / "20261009.log", "yesterday\n")
  written, write_line = recorder()

  def sleep(_seconds: float) -> None:
    write(tmp_path / "20261010.log", "today\n")

  logs.follow(tmp_path, lines=1, write=write_line, sleep=sleep, stop_after=1)

  assert "20261010.log" in written[-2]
  assert written[-1] == "today"


def test_a_truncated_file_is_followed_from_its_start(tmp_path):
  path = tmp_path / "20261009.log"
  write(path, "a lot of text\n")
  written, write_line = recorder()

  def sleep(_seconds: float) -> None:
    path.write_text("fresh\n", encoding="utf-8")

  logs.follow(tmp_path, lines=0, write=write_line, sleep=sleep, stop_after=1)

  assert any("truncated" in line for line in written)
  assert written[-1] == "fresh"


def test_the_console_log_is_followed_as_given(tmp_path):
  # --console: it has no date in its name, so it is never rolled away from.
  write(tmp_path / "20261009.log", "application\n")
  console = tmp_path / logs.CONSOLE_LOG_NAME
  write(console, "traceback\n")
  written, write_line = recorder()

  logs.follow(tmp_path, lines=5, follow_new=False, fixed_path=console, write=write_line)

  assert written[-1] == "traceback"


# ── Filtering ──────────────────────────────────────────────────────


def test_grep_keeps_only_matching_lines(tmp_path):
  import re

  path = tmp_path / "20261009.log"
  write(path, "DEBUG MT5 raw XAUUSD\nERROR failed to publish\n")
  written, write_line = recorder()

  def sleep(_seconds: float) -> None:
    write(path, "DEBUG more noise\nERROR again\n")

  logs.follow(
    tmp_path,
    lines=5,
    pattern=re.compile("error", re.IGNORECASE),
    write=write_line,
    sleep=sleep,
    stop_after=1,
  )

  assert written[1] == "ERROR failed to publish"
  assert written[-1] == "ERROR again"


# ── The command line ───────────────────────────────────────────────


def test_a_missing_log_directory_is_reported(tmp_path, capsys):
  assert logs.main(["--dir", str(tmp_path / "nowhere"), "--no-follow"]) == 1
  assert "No log directory" in capsys.readouterr().out


def test_console_without_a_detached_run_is_reported(tmp_path, capsys):
  write(tmp_path / "20261009.log", "x\n")

  code = logs.main(["--dir", str(tmp_path), "--console", "--no-follow"])

  assert code == 1
  assert "No console log" in capsys.readouterr().out


def test_a_broken_grep_is_reported(tmp_path, capsys):
  write(tmp_path / "20261009.log", "x\n")

  code = logs.main(["--dir", str(tmp_path), "--no-follow", "--grep", "[unclosed"])

  assert code == 2
  assert "not a valid regular expression" in capsys.readouterr().out
