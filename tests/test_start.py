"""The ``make start`` command: the port guard, the spawn and its reporting."""

from ingester import start


class FakeProcess:
  """A spawned child: ``exit_code`` is what ``poll()`` reports."""

  def __init__(self, pid=4242, exit_code=None):
    self.pid = pid
    self._exit_code = exit_code

  def poll(self):
    return self._exit_code


def spawner(process=None, error=None):
  """A fake spawner recording the command and the console log it was given."""
  calls = []

  def spawn(command, console_log):
    calls.append((command, console_log))
    if error is not None:
      raise error
    return process if process is not None else FakeProcess()

  spawn.calls = calls
  return spawn


def test_starts_detached_and_reports_the_pid(monkeypatch, tmp_path, capsys):
  monkeypatch.setattr(start, "find_pids", lambda port: set())
  spawn = spawner(FakeProcess(pid=777))

  code = start.start_detached(
    8090, tmp_path / "ingester.out.log", spawn=spawn, sleep=lambda _: None
  )

  assert code == 0
  command, console_log = spawn.calls[0]
  assert command[1:] == ["-m", "ingester"]
  assert console_log == tmp_path / "ingester.out.log"
  assert "777" in capsys.readouterr().out


def test_refuses_to_start_when_the_port_is_taken(monkeypatch, tmp_path, capsys):
  monkeypatch.setattr(start, "find_pids", lambda port: {4120})
  spawn = spawner()

  code = start.start_detached(
    8090, tmp_path / "out.log", spawn=spawn, sleep=lambda _: None
  )

  assert code == 1
  assert spawn.calls == []
  assert "4120" in capsys.readouterr().out


def test_reports_a_child_that_died_on_start_up(monkeypatch, tmp_path, capsys):
  monkeypatch.setattr(start, "find_pids", lambda port: set())
  spawn = spawner(FakeProcess(exit_code=1))

  code = start.start_detached(
    8090, tmp_path / "out.log", spawn=spawn, sleep=lambda _: None
  )

  assert code == 1
  assert "exited immediately" in capsys.readouterr().out


def test_reports_a_spawn_that_failed(monkeypatch, tmp_path, capsys):
  monkeypatch.setattr(start, "find_pids", lambda port: set())
  spawn = spawner(error=OSError("no such executable"))

  code = start.start_detached(
    8090, tmp_path / "out.log", spawn=spawn, sleep=lambda _: None
  )

  assert code == 1
  assert "no such executable" in capsys.readouterr().out


def test_spawn_creates_the_log_directory(tmp_path):
  console_log = tmp_path / "logs" / start.CONSOLE_LOG_NAME
  process = start.spawn_detached(
    [__import__("sys").executable, "-c", "print('hello from the child')"],
    console_log,
  )
  process.wait(timeout=30)

  assert console_log.is_file()
  assert "hello from the child" in console_log.read_text(encoding="utf-8")


def test_main_takes_the_port_override_and_the_configured_log_dir(
  monkeypatch, tmp_path, capsys
):
  monkeypatch.setenv("LOG_DIR", str(tmp_path))
  monkeypatch.setattr(start, "find_pids", lambda port: set())
  spawn = spawner(FakeProcess())
  monkeypatch.setattr(start, "spawn_detached", spawn)
  monkeypatch.setattr(start.time, "sleep", lambda _: None)

  assert start.main(["--port", "8099"]) == 0
  assert spawn.calls[0][1] == tmp_path / start.CONSOLE_LOG_NAME
  assert "8099" in capsys.readouterr().out
