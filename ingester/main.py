"""
ingester/main.py — Entry point: ``uv run python -m ingester.main``.
"""

from __future__ import annotations

import uvicorn

from ingester import __version__
from ingester.app import create_app
from ingester.logger import get_logger, uvicorn_log_config
from ingester.settings import settings

log = get_logger(__name__)


def main() -> None:
  log.info(
    "Starting %s v%s (%s)", settings.app.NAME, __version__, settings.app.instance_id
  )
  log.info("HTTP     → http://%s:%d", settings.app.HOST, settings.app.PORT)
  log.info("NATS     → %s (prefix %s)", settings.nats.url, settings.nats.SUBJECT_PREFIX)
  log.info(
    "Markets  → %s (from %s)",
    ", ".join(m.value for m in settings.source.MARKET),
    settings.source.CONFIG_DIR,
  )
  log.info(
    "Gateways → %s",
    ", ".join(
      f"{market.value}/{gateway.value}"
      for market, gateway, _ in settings.gateway_configs
    )
    or "none enabled",
  )
  for problem in settings.market_problems:
    log.error("Market configuration: %s", problem)

  uvicorn.run(
    create_app(),
    host=settings.app.HOST,
    port=settings.app.PORT,
    workers=1,
    loop="asyncio",
    log_level=settings.logging.LEVEL.lower(),
    log_config=uvicorn_log_config(),
  )


if __name__ == "__main__":
  main()
