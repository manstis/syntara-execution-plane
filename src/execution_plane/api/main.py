"""Standalone command ingress and safe recovery queries for the Execution Plane."""

from __future__ import annotations

import logging
import ssl
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, cast

import uvicorn
from fastapi import FastAPI, HTTPException
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.api.resources import create_resource_router
from execution_plane.commands.runtime import build_ep_runtime
from execution_plane.config import get_ep_settings
from execution_plane_client.api import create_router

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from execution_plane_client.service import MediatorService


def create_app(*, runtime: MediatorService | None = None) -> FastAPI:  # noqa: C901 - Application factory includes its lifecycle and health routes.
    """Create metadata without I/O; initialize production trust during lifespan."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = None
        service = runtime
        try:
            if service is None:
                settings = get_ep_settings()
                engine = create_async_engine(settings.database_url)
                service = build_ep_runtime(settings, async_sessionmaker(engine, expire_on_commit=False))
            app.state.ep_runtime = service
            yield
        finally:
            if service is not None:
                await service.close()
            if engine is not None:
                await engine.dispose()

    app = FastAPI(
        title="Execution Plane API",
        version="1.0.0",
        description="Durable command exchange and scoped execution resource queries.",
        lifespan=lifespan,
    )
    if runtime is not None:
        app.state.ep_runtime = runtime

    def current() -> MediatorService:
        service = getattr(app.state, "ep_runtime", None)
        if service is None:
            raise HTTPException(status_code=503, detail="Command runtime is not initialized")
        return cast("MediatorService", service)

    app.include_router(create_router(current))
    app.include_router(create_resource_router(current))

    @app.get("/healthz/live", include_in_schema=False)
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/healthz/ready", include_in_schema=False)
    async def ready() -> dict[str, str]:
        try:
            async with current().sessions() as session:
                await session.execute(text("SELECT 1"))
        except Exception:  # noqa: BLE001 - Readiness must not expose database exception details.
            raise HTTPException(status_code=503, detail="Command database unavailable") from None
        return {"status": "ready"}

    return app


app = create_app()


def main() -> None:
    """Run the API with mandatory CA-verified client certificates."""
    logging.basicConfig(level=logging.INFO)
    settings = get_ep_settings()
    if not all((settings.api_tls_cert_path, settings.api_tls_key_path, settings.api_tls_client_ca_path)):
        error = "EP API requires its TLS certificate, key and trusted client CA"
        raise ValueError(error)
    uvicorn.run(
        "execution_plane.api.main:app",
        host=settings.api_host,
        port=settings.api_port,
        ssl_certfile=settings.api_tls_cert_path,
        ssl_keyfile=settings.api_tls_key_path,
        ssl_ca_certs=settings.api_tls_client_ca_path,
        ssl_cert_reqs=ssl.CERT_REQUIRED,
        http="execution_plane.api.tls:TLSH11Protocol",
    )
