"""
ingester/status.py — Report whether the ingester is running, and what it is doing.

Entry point of ``make status`` (``uv run python -m ingester.status``). It is the
read-only counterpart of :mod:`ingester.stop`, and it answers from *outside*
the process: ``make start`` runs in the foreground, so a service put in the
background by a supervisor (systemd, pm2, a Windows service) is only reachable
by asking the port.

Two probes, in this order, because they answer different questions:

* **Is a process there?** ``APP_PORT`` is checked with
  :func:`ingester.stop.find_pids` — the same probe ``make stop`` kills by, so
  the two can never disagree about what is running.
* **Is it healthy?** ``GET /status`` on the loopback address then reports what
  each gateway is doing. A process that holds the port but will not answer is
  wedged, and saying so is the point: ``find_pids`` alone cannot tell a running
  ingester from a hung one.

The exit code is meant for scripts: ``0`` running and answering, ``1`` not
running, ``2`` running but unreachable or degraded. ``make status`` therefore
prints make's own ``Error 1`` line when the service is down — the text above it
is the answer.

``AppSettings`` is read on its own rather than through ``settings``: reporting
on a port must not depend on the market TOML files loading.
"""

from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.request
from collections.abc import Callable
from typing import Any

from ingester.settings import AppSettings
from ingester.stop import IS_WINDOWS, Runner, find_pids, run_command

#: How long to wait for ``GET /status``. The endpoint touches nothing blocking,
#: so a process that has not answered by now is not going to.
_HTTP_TIMEOUT_SECONDS = 3.0

#: ``APP_HOST`` values that mean "every interface" and cannot be dialled as
#: given; the service is reached over loopback instead.
_WILDCARD_HOSTS = frozenset({"0.0.0.0", "::", "[::]", ""})

#: Gateway statuses that are not a problem. Anything else makes the command
#: report degraded, so a stuck gateway is visible without reading the log.
_HEALTHY_GATEWAY_STATUSES = frozenset({"running"})

#: Where to look when the port is held but nothing answers. The log is the
#: day file, or whatever the supervisor captured from stdout.
_LOG_HINT = "the day's file in LOG_DIR (or the supervisor's own log)"

#: Fetches ``/status``; injected so no test opens a socket.
Fetcher = Callable[[str], dict[str, Any]]


def fetch_status(url: str) -> dict[str, Any]:
  """``GET`` *url* and decode its JSON body.

  Raises ``OSError`` for anything that means "no usable answer" — a refused
  connection, a timeout, an HTTP error, a body that is not JSON — so the
  caller has one failure to handle.
  """
  try:
    with urllib.request.urlopen(url, timeout=_HTTP_TIMEOUT_SECONDS) as response:
      payload = json.loads(response.read().decode("utf-8"))
  except urllib.error.HTTPError as error:
    raise OSError(f"HTTP {error.code}") from error
  except urllib.error.URLError as error:
    raise OSError(str(error.reason)) from error
  except json.JSONDecodeError as error:
    raise OSError(f"the answer was not JSON: {error}") from error
  if not isinstance(payload, dict):
    raise OSError("the answer was not a JSON object")
  return payload


def status_url(host: str, port: int) -> str:
  """The ``/status`` URL to dial for a service bound to *host*.

  A wildcard bind is not an address: ``http://0.0.0.0:8090`` does not reach it
  on Windows, so loopback is used. An IPv6 literal is bracketed.
  """
  if host in _WILDCARD_HOSTS:
    host = "127.0.0.1"
  if ":" in host and not host.startswith("["):
    host = f"[{host}]"
  return f"http://{host}:{port}/status"


def _format_gateway(gateway: dict[str, Any]) -> str:
  """One gateway as a single line: who it is, how it is, what it has sent."""
  symbols = ", ".join(gateway.get("symbols") or []) or "no symbols"
  timeframes = ", ".join(gateway.get("timeframes") or []) or "no timeframes"
  line = (
    f"    {gateway.get('market', '?')}/{gateway.get('venue', '?')}"
    f" - {gateway.get('status', 'unknown')}"
  )
  detail = gateway.get("status_detail")
  if detail:
    line += f" ({detail})"
  line += (
    f"\n      {symbols} | {timeframes}"
    f"\n      published {gateway.get('published', 0)},"
    f" failed {gateway.get('failed', 0)},"
    f" history {gateway.get('history_served', 0)} served"
    f"/{gateway.get('history_refused', 0)} refused"
  )
  last_published = gateway.get("last_published_at")
  line += f"\n      last bar published at {last_published or 'never'}"
  last_error = gateway.get("last_error")
  if last_error:
    line += f"\n      last error: {last_error}"
  return line


def report_status(
  host: str,
  port: int,
  *,
  run: Runner = run_command,
  windows: bool = IS_WINDOWS,
  fetch: Fetcher = fetch_status,
) -> int:
  """Print what the ingester on *port* is doing. Returns a process exit code."""
  pids = sorted(find_pids(port, run=run, windows=windows))
  if not pids:
    print(f"Ingester is not running: port {port} is free.")
    print("  start it with: make start")
    return 1

  held_by = ", ".join(str(pid) for pid in pids)
  url = status_url(host, port)
  try:
    payload = fetch(url)
  except OSError as error:
    # The port is held but nothing answers: a start-up that never finished, a
    # wedged process, or something else entirely sitting on APP_PORT.
    print(f"Ingester is not answering: port {port} is held by PID {held_by}.")
    print(f"  {url} failed: {error}")
    print(f"  see {_LOG_HINT} for what it printed")
    return 2

  print(f"Ingester is running: PID {held_by}, port {port}.")
  print(f"  instance : {payload.get('instance_id', '?')}")
  print(
    f"  version  : {payload.get('version', '?')}"
    f" (schema {payload.get('schema_version', '?')})"
  )

  nats = payload.get("nats") or {}
  nats_connected = bool(nats.get("connected"))
  print(
    f"  nats     : {'connected' if nats_connected else 'disconnected'}"
    f" ({nats.get('subject_filter', 'no subject filter')})"
  )

  gateways = payload.get("gateways") or []
  if not gateways:
    print("  gateways : none enabled")
  else:
    print("  gateways :")
    for gateway in gateways:
      print(_format_gateway(gateway))

  unhealthy = [
    f"{gateway.get('market', '?')}/{gateway.get('venue', '?')}"
    for gateway in gateways
    if gateway.get("status") not in _HEALTHY_GATEWAY_STATUSES
  ]
  if not nats_connected or unhealthy or not gateways:
    problems = []
    if not nats_connected:
      problems.append("NATS is disconnected")
    if not gateways:
      problems.append("no gateway is enabled")
    if unhealthy:
      problems.append(f"not running: {', '.join(unhealthy)}")
    print(f"Degraded - {'; '.join(problems)}.")
    return 2

  return 0


def main(argv: list[str] | None = None) -> int:
  parser = argparse.ArgumentParser(
    prog="python -m ingester.status",
    description=(
      "Report whether the ingester is running: the holder of its HTTP port "
      "(APP_PORT in .env) and what GET /status says."
    ),
  )
  parser.add_argument(
    "--port",
    type=int,
    default=None,
    help="TCP port to check; defaults to APP_PORT from .env",
  )
  parser.add_argument(
    "--host",
    default=None,
    help="host to dial; defaults to APP_HOST from .env (a wildcard means loopback)",
  )
  args = parser.parse_args(argv)
  app = AppSettings()
  return report_status(
    args.host if args.host is not None else app.HOST,
    args.port if args.port is not None else app.PORT,
  )


if __name__ == "__main__":
  raise SystemExit(main())
