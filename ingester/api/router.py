"""
ingester/api/router.py — Read-only HTTP surface: liveness and per-gateway status.

The ingester's real output goes to NATS; these endpoints exist for health checks
and for an operator to see what each gateway is doing.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

from ingester import __version__
from ingester.schemas.market_event_schema import SCHEMA_VERSION

router = APIRouter()


@router.get("/health")
async def health(request: Request) -> dict[str, Any]:
  publisher = request.app.state.publisher
  return {
    "status": "ok",
    "version": __version__,
    "nats_connected": publisher.is_connected,
  }


@router.get("/status")
async def status(request: Request) -> dict[str, Any]:
  state = request.app.state
  return {
    "instance_id": state.instance_id,
    "version": __version__,
    "schema_version": SCHEMA_VERSION,
    "nats": {
      "connected": state.publisher.is_connected,
      "subject_filter": state.publisher.subject_filter,
    },
    "gateways": [ingestion.snapshot() for ingestion in state.ingestions],
  }
