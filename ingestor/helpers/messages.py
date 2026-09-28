"""
ingestor/helpers/messages.py — Telegram (HTML) message templates.

Formatting lives here so services only decide *when* to notify, never *how*
the text looks. Every dynamic value is HTML-escaped: an exception message
containing ``<`` would otherwise make the Bot API reject the whole send.
"""

from __future__ import annotations

from collections.abc import Iterable
from html import escape

from ingestor.helpers import emoji_constants as em
from ingestor.schemas.enums import GatewayEnum, GatewayStatusEnum

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
    _header(em.SERVICE_RUNNING, "Ingestor Running", app_name, instance_id),
    f"{em.ENDPOINT} Endpoint: {_code(endpoint)}",
    f"{em.PUBLISH} NATS: {_code(nats_url)} → {_code(subject_filter)}",
  ]
  lines += [f"{em.CHART} {escape(stream)}" for stream in streams]
  return "\n".join(lines)


def service_stopped(*, app_name: str, instance_id: str) -> str:
  return _header(em.SERVICE_STOPPED, "Ingestor Stopped", app_name, instance_id)


def service_failed(*, app_name: str, instance_id: str, error: str) -> str:
  return (
    f"{_header(em.SERVICE_FAILED, 'Ingestor Failed To Start', app_name, instance_id)}\n"
    f"{_code(error)}"
  )


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
