"""
ingester/helpers/messages.py — Telegram (HTML) message templates.

Formatting lives here so services only decide *when* to notify, never *how*
the text looks. Every dynamic value is HTML-escaped: an exception message
containing ``<`` would otherwise make the Bot API reject the whole send.
"""

from __future__ import annotations

from collections.abc import Iterable
from html import escape

from ingester.helpers import emoji_constants as em
from ingester.schemas.enums import GatewayEnum, GatewayStatusEnum

_GATEWAY_STATUS_ICON: dict[GatewayStatusEnum, str] = {
  GatewayStatusEnum.STARTING: em.GATEWAY_STARTING,
  GatewayStatusEnum.RUNNING: em.GATEWAY_RUNNING,
  GatewayStatusEnum.DISCONNECTED: em.GATEWAY_DISCONNECTED,
  GatewayStatusEnum.STOPPED: em.GATEWAY_STOPPED,
  GatewayStatusEnum.FAILED: em.SERVICE_FAILED,
}


def _code(value: object) -> str:
  return f"<code>{escape(str(value))}</code>"


def _header(icon: str, title: str, app_name: str, instance_id: str) -> str:
  return f"{icon} <b>{escape(title)}</b> — {escape(app_name)} ({_code(instance_id)})"


def service_started(
  *,
  app_name: str,
  instance_id: str,
  endpoint: str,
  nats_url: str,
  subject_filter: str,
  streams: Iterable[str],
) -> str:
  lines = [
    _header(em.SERVICE_RUNNING, "Ingester Running", app_name, instance_id),
    f"{em.ENDPOINT} Endpoint: {_code(endpoint)}",
    f"{em.PUBLISH} NATS: {_code(nats_url)} → {_code(subject_filter)}",
  ]
  lines += [f"{em.CHART} {escape(stream)}" for stream in streams]
  return "\n".join(lines)


def service_stopped(*, app_name: str, instance_id: str) -> str:
  return _header(em.SERVICE_STOPPED, "Ingester Stopped", app_name, instance_id)


def service_degraded(
  *, app_name: str, instance_id: str, problems: Iterable[str]
) -> str:
  """Started, but something it needs is down. The process stays up."""
  lines = [
    _header(em.SERVICE_DEGRADED, "Ingester Degraded", app_name, instance_id),
  ]
  lines += [f"{em.SERVICE_FAILED} {_code(problem)}" for problem in problems]
  return "\n".join(lines)


def gateway_status(
  *,
  gateway: GatewayEnum,
  instance_id: str,
  status: GatewayStatusEnum,
  detail: str | None = None,
) -> str:
  icon = _GATEWAY_STATUS_ICON.get(status, em.GATEWAY_STARTING)
  text = (
    f"{icon} <b>{escape(gateway.value.upper())} gateway {escape(status.value)}</b>"
    f" ({_code(instance_id)})"
  )
  if detail:
    text += f"\n{_code(detail)}"
  return text


def nats_disconnected(*, url: str, instance_id: str) -> str:
  return f"{em.NATS_DISCONNECTED} <b>NATS Disconnected</b> {_code(url)} ({_code(instance_id)})"


def nats_reconnected(*, url: str, instance_id: str) -> str:
  return (
    f"{em.NATS_RECONNECTED} <b>NATS Reconnected</b> {_code(url)} ({_code(instance_id)})"
  )


def nats_gave_up(
  *, url: str, instance_id: str, attempts: int, window_seconds: float
) -> str:
  """NATS stayed unreachable, so the ingester is stopping itself.

  The last message this process sends: it says the service is down on purpose,
  and that only an operator can bring it back.
  """
  return "\n".join(
    [
      f"{em.SERVICE_FAILED} <b>NATS Unreachable — Ingester Stopping</b>"
      f" ({_code(instance_id)})",
      f"{em.NATS_DISCONNECTED} {_code(url)} — {attempts} failed connection"
      f" attempts in {window_seconds / 60:.0f} min, no bar is being published.",
      f"{em.SERVICE_STOPPED} Shutting down, and <b>not</b> restarting on its"
      " own — start it again by hand once NATS is back.",
    ]
  )


def _clip(text: str, limit: int, *, keep_tail: bool = False) -> str:
  """Trim to *limit* characters — Telegram rejects messages over 4096."""
  if len(text) <= limit:
    return text
  return f"…{text[-limit:]}" if keep_tail else f"{text[:limit]}…"


def log_error(
  *,
  instance_id: str,
  logger_name: str,
  level: str,
  message: str,
  traceback_text: str | None = None,
) -> str:
  """One ERROR log record, formatted for the dedicated error chat."""
  lines = [
    f"{em.LOG_ERROR} <b>{escape(level)}</b> ({_code(instance_id)})",
    f"{em.MODULE} {_code(logger_name)}",
    f"<pre>{escape(_clip(message, 900))}</pre>",
  ]
  if traceback_text:
    # The tail carries the actual exception; the head is usually framework noise.
    lines.append(f"<pre>{escape(_clip(traceback_text, 1500, keep_tail=True))}</pre>")
  return "\n".join(lines)
