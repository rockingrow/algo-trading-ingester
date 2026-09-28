"""
ingester/app.py — FastAPI application factory.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI

from ingester import __version__
from ingester.api.router import router
from ingester.providers import make_runtime
from ingester.runtime import IngesterRuntime
from ingester.settings import Settings, settings


def create_app(
  config: Settings = settings,
  runtime_factory: Callable[[Settings], IngesterRuntime] = make_runtime,
) -> FastAPI:
  """Build the app. *runtime_factory* is swappable so tests can inject fakes."""

  @asynccontextmanager
  async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    runtime = runtime_factory(config)
    await runtime.start()
    app.state.instance_id = runtime.instance_id
    app.state.publisher = runtime.publisher
    app.state.ingestions = runtime.ingestions
    try:
      yield
    finally:
      await runtime.stop()

  docs = config.app.DOCS_ENABLED
  app = FastAPI(
    title=config.app.NAME,
    version=__version__,
    lifespan=lifespan,
    docs_url="/docs" if docs else None,
    redoc_url="/redoc" if docs else None,
    openapi_url="/openapi.json" if docs else None,
  )
  app.include_router(router)
  return app
