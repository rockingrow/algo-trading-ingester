"""The ``make status`` command: is a process on the port, and what it answers."""

import json
import urllib.error

import pytest

from ingester import status
from tests.test_stop import NETSTAT, runner


def payload(**overrides):
  """A ``GET /status`` body with one healthy gateway, overridable per test."""
  body = {
    "instance_id": "ingester-test",
    "version": "1.2.3",
    "schema_version": 1,
    "nats": {"connected": True, "subject_filter": "INGESTER.bar.closed.>"},
    "gateways": [
      {
        "gateway": "mt5",
        "status": "running",
        "status_detail": None,
        "market": "forex",
        "venue": "mt5",
        "symbols": ["XAUUSD"],
        "timeframes": ["M5"],
        "published": 12,
        "failed": 0,
        "last_published_at": "2026-10-09T13:05:00+00:00",
        "last_error": None,
        "last_bar_open_time": {},
        "history_served": 2,
        "history_refused": 0,
      }
    ],
  }
  body.update(overrides)
  return body


def fetcher(answer):
  """A fake ``/status`` fetch: returns *answer*, or raises it when it is an error."""
  urls = []

  def fetch(url):
    urls.append(url)
    if isinstance(answer, Exception):
      raise answer
    return answer

  fetch.urls = urls
  return fetch


# ── Which URL is dialled ───────────────────────────────────────────


@pytest.mark.parametrize(
  ("host", "expected"),
  [
    # A wildcard bind is not an address: loopback reaches the same process.
    ("0.0.0.0", "http://127.0.0.1:8090/status"),
    ("::", "http://127.0.0.1:8090/status"),
    ("", "http://127.0.0.1:8090/status"),
    ("127.0.0.1", "http://127.0.0.1:8090/status"),
    ("10.0.0.5", "http://10.0.0.5:8090/status"),
    ("::1", "http://[::1]:8090/status"),
    ("[::1]", "http://[::1]:8090/status"),
  ],
)
def test_status_url_handles_wildcards_and_ipv6(host, expected):
  assert status.status_url(host, 8090) == expected


# ── Reporting ──────────────────────────────────────────────────────


def test_a_free_port_means_not_running(capsys):
  run = runner({"netstat": (0, NETSTAT)})
  fetch = fetcher(payload())

  assert status.report_status("0.0.0.0", 4000, run=run, windows=True, fetch=fetch) == 1

  out = capsys.readouterr().out
  assert "is not running" in out
  assert "make start" in out
  # Nothing to ask when nothing is there.
  assert fetch.urls == []


def test_a_healthy_service_reports_its_gateways(capsys):
  run = runner({"netstat": (0, NETSTAT)})
  fetch = fetcher(payload())

  assert status.report_status("0.0.0.0", 8090, run=run, windows=True, fetch=fetch) == 0

  out = capsys.readouterr().out
  assert "Ingester is running" in out
  assert "4120" in out  # the PID holding the port
  assert "ingester-test" in out
  assert "1.2.3" in out
  assert "connected" in out
  assert "forex/mt5" in out
  assert "XAUUSD" in out
  assert "Degraded" not in out
  assert fetch.urls == ["http://127.0.0.1:8090/status"]


def test_a_held_but_silent_port_is_reported_as_wedged(capsys):
  # The one thing find_pids cannot tell on its own: a process that holds the
  # port but never finished starting up.
  run = runner({"netstat": (0, NETSTAT)})
  fetch = fetcher(OSError("Connection refused"))

  assert status.report_status("0.0.0.0", 8090, run=run, windows=True, fetch=fetch) == 2

  out = capsys.readouterr().out
  assert "is not answering" in out
  assert "Connection refused" in out
  assert "LOG_DIR" in out


def test_a_disconnected_nats_is_degraded(capsys):
  run = runner({"netstat": (0, NETSTAT)})
  fetch = fetcher(
    payload(nats={"connected": False, "subject_filter": "INGESTER.bar.closed.>"})
  )

  assert status.report_status("0.0.0.0", 8090, run=run, windows=True, fetch=fetch) == 2

  out = capsys.readouterr().out
  assert "disconnected" in out
  assert "NATS is disconnected" in out


def test_a_stopped_gateway_is_degraded_and_named(capsys):
  run = runner({"netstat": (0, NETSTAT)})
  body = payload()
  body["gateways"][0]["status"] = "error"
  body["gateways"][0]["status_detail"] = "terminal not reachable"
  body["gateways"][0]["last_error"] = "initialize() failed"
  fetch = fetcher(body)

  assert status.report_status("0.0.0.0", 8090, run=run, windows=True, fetch=fetch) == 2

  out = capsys.readouterr().out
  assert "terminal not reachable" in out
  assert "initialize() failed" in out
  assert "not running: forex/mt5" in out


def test_no_gateway_enabled_is_degraded(capsys):
  run = runner({"netstat": (0, NETSTAT)})
  fetch = fetcher(payload(gateways=[]))

  assert status.report_status("0.0.0.0", 8090, run=run, windows=True, fetch=fetch) == 2

  out = capsys.readouterr().out
  assert "none enabled" in out
  assert "no gateway is enabled" in out


# ── The fetch itself ───────────────────────────────────────────────


def test_fetch_turns_every_failure_into_one_oserror(monkeypatch):
  # The caller has a single failure to handle, whatever went wrong.
  for raised, expected in [
    (urllib.error.URLError("refused"), "refused"),
    (urllib.error.HTTPError("u", 503, "nope", {}, None), "HTTP 503"),
  ]:

    def urlopen(url, timeout=None, raised=raised):
      raise raised

    monkeypatch.setattr(status.urllib.request, "urlopen", urlopen)
    with pytest.raises(OSError, match=expected):
      status.fetch_status("http://127.0.0.1:8090/status")


def test_fetch_rejects_an_answer_that_is_not_a_json_object(monkeypatch):
  class Response:
    def read(self):
      return b"[1, 2]"

    def __enter__(self):
      return self

    def __exit__(self, *exc):
      return False

  monkeypatch.setattr(
    status.urllib.request, "urlopen", lambda url, timeout=None: Response()
  )
  with pytest.raises(OSError, match="not a JSON object"):
    status.fetch_status("http://127.0.0.1:8090/status")


def test_fetch_decodes_a_json_object(monkeypatch):
  class Response:
    def read(self):
      return json.dumps({"instance_id": "x"}).encode("utf-8")

    def __enter__(self):
      return self

    def __exit__(self, *exc):
      return False

  monkeypatch.setattr(
    status.urllib.request, "urlopen", lambda url, timeout=None: Response()
  )
  assert status.fetch_status("http://127.0.0.1:8090/status") == {"instance_id": "x"}
