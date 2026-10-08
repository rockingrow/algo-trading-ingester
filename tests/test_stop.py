"""The ``make stop`` command: which PIDs hold the port, and killing them."""

import os

from ingester import stop

NETSTAT = """
Active Connections

  Proto  Local Address          Foreign Address        State
  TCP    0.0.0.0:8090           0.0.0.0:0              LISTENING       4120
  TCP    127.0.0.1:8090         127.0.0.1:51234        ESTABLISHED     4120
  TCP    127.0.0.1:51234        127.0.0.1:8090         ESTABLISHED     9001
  TCP    [::]:8090              [::]:0                 LISTENING       4121
  TCP    0.0.0.0:18090          0.0.0.0:0              LISTENING       7777
  TCP    0.0.0.0:4222           0.0.0.0:0              LISTENING       3030
  TCP    127.0.0.1:8090         127.0.0.1:51299        TIME_WAIT       0
  UDP    0.0.0.0:8090           *:*                                    5150
"""

SS = """
LISTEN 0  2048  0.0.0.0:8090  0.0.0.0:*  users:(("python",pid=4120,fd=9))
"""


def runner(outputs):
  """A fake command runner: returns a canned (code, output) per executable."""
  calls = []

  def run(command):
    calls.append(command)
    return outputs.get(command[0], (1, ""))

  run.calls = calls
  return run


# ── Reading the port's owners ──────────────────────────────────────


def test_netstat_lists_only_the_local_port_owners():
  pids = stop.parse_netstat(NETSTAT, 8090)
  # 4120 listens and serves a connection, 4121 is its IPv6 socket, 5150 is UDP
  # on the same port. 9001 only *connects* to :8090, 7777 holds :18090 and
  # 3030 another port — none of them is ours.
  assert pids == {4120, 4121, 5150, 0}


def test_unkillable_pids_and_our_own_are_dropped():
  run = runner({"netstat": (0, NETSTAT)})
  # PID 0 came out of the TIME_WAIT row above; 4 is the System process.
  assert stop.find_pids(8090, run=run, windows=True) == {4120, 4121, 5150}
  assert stop._killable({0, 4, os.getpid(), 4120}) == {4120}


def test_ss_answers_first_on_posix():
  # "sport = :8090" is the only selector that means the *local* port, so it is
  # the one asked first.
  run = runner({"ss": (0, SS)})
  assert stop.find_pids(8090, run=run, windows=False) == {4120}
  assert [command[0] for command in run.calls] == ["ss"]
  assert "sport = :8090" in run.calls[0]


def test_lsof_is_the_fallback_and_only_takes_listeners():
  # lsof -iTCP:<port> on its own also matches a socket whose *foreign* port is
  # 8090 — a client of someone else's :8090 — so the fallback is narrowed to
  # the listening socket, which is the one holding the port.
  run = runner({"lsof": (0, "4120\n4121\n")})  # ss answers (1, ""): not installed
  assert stop.find_pids(8090, run=run, windows=False) == {4120, 4121}
  assert [command[0] for command in run.calls] == ["ss", "lsof"]
  assert "-sTCP:LISTEN" in run.calls[1]


def test_nothing_on_the_port_is_not_an_error():
  run = runner({"netstat": (0, NETSTAT)})
  assert stop.find_pids(4000, run=run, windows=True) == set()


# ── Killing ────────────────────────────────────────────────────────


def test_windows_kill_is_forced_and_takes_children():
  run = runner({"taskkill": (0, "SUCCESS: ...")})
  assert stop.kill_pid(4120, run=run, windows=True) == (True, "")
  assert run.calls == [["taskkill", "/F", "/T", "/PID", "4120"]]


def test_windows_kill_of_a_vanished_process_counts_as_gone():
  run = runner({"taskkill": (128, 'ERROR: The process "4120" not found.')})
  killed, reason = stop.kill_pid(4120, run=run, windows=True)
  assert (killed, reason) == (True, "")


def test_windows_kill_reports_why_it_failed():
  run = runner({"taskkill": (1, "ERROR: Access is denied.")})
  killed, reason = stop.kill_pid(4120, run=run, windows=True)
  assert killed is False
  assert "Access is denied" in reason


def test_posix_kill_sends_a_signal_and_survives_a_race():
  sent = []
  assert stop.kill_pid(
    4120, windows=False, send_signal=lambda pid, sig: sent.append((pid, sig))
  ) == (
    True,
    "",
  )
  assert sent == [(4120, stop._SIGKILL)]

  def gone(pid, sig):
    raise ProcessLookupError

  assert stop.kill_pid(4120, windows=False, send_signal=gone) == (True, "")


def test_posix_kill_reports_a_refusal():
  def denied(pid, sig):
    raise PermissionError("Operation not permitted")

  killed, reason = stop.kill_pid(4120, windows=False, send_signal=denied)
  assert killed is False
  assert "not permitted" in reason


# ── The command as a whole ─────────────────────────────────────────


def test_stop_port_kills_every_owner_and_confirms_the_port_is_free(capsys):
  freed = {"done": False}

  def run(command):
    if command[0] == "netstat":
      return (0, "" if freed["done"] else NETSTAT)
    freed["done"] = True  # taskkill
    return (0, "SUCCESS")

  assert stop.stop_port(8090, run=run, windows=True) == 0
  output = capsys.readouterr().out
  assert "4120, 4121, 5150" in output
  assert "killed 4120" in output
  assert "Port 8090 is free." in output


def test_stop_port_on_a_free_port_does_nothing(capsys):
  run = runner({"netstat": (0, NETSTAT)})
  assert stop.stop_port(4000, run=run, windows=True) == 0
  assert "nothing to stop" in capsys.readouterr().out
  assert [command[0] for command in run.calls] == ["netstat"]


def test_stop_port_fails_when_a_process_survives(capsys):
  run = runner({"netstat": (0, NETSTAT), "taskkill": (1, "ERROR: Access is denied.")})
  assert stop.stop_port(8090, run=run, windows=True) == 1
  output = capsys.readouterr().out
  assert "Could not stop every process" in output
  assert "elevated shell" in output


def test_stop_port_fails_when_the_port_is_still_held(capsys):
  # taskkill reports success, yet the socket is still bound: say so instead of
  # letting the next "make run" fail on an address already in use.
  run = runner({"netstat": (0, NETSTAT), "taskkill": (0, "SUCCESS")})
  assert stop.stop_port(8090, run=run, windows=True) == 1
  assert "still held by PID" in capsys.readouterr().out


def test_port_defaults_to_app_port_and_is_overridable(monkeypatch):
  ports = []
  monkeypatch.setattr(stop, "stop_port", lambda port: ports.append(port) or 0)
  monkeypatch.setenv("APP_PORT", "9100")

  assert stop.main([]) == 0
  assert stop.main(["--port", "4222"]) == 0
  assert ports == [9100, 4222]
