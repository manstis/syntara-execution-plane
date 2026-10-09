"""Transport-neutral EP API assembly; a transport package supplies the runtime."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, cast

from fastapi import FastAPI, HTTPException
from sqlalchemy import text

from execution_plane.api.resources import create_resource_router
from execution_plane_client.api import CommandRuntime, create_router

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from sqlalchemy.ext.asyncio import AsyncSession


def create_app(*, runtime: CommandRuntime | None = None) -> FastAPI:  # noqa: C901 - Lifecycle and route injection are explicit.
    """Create the EP API around a runtime selected by the installed transport."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if runtime is not None:
            app.state.ep_runtime = runtime
            app.state.ep_sessions = runtime.sessions
            try:
                yield
            finally:
                await runtime.close()
        else:
            # Useful for schema inspection and route tests. Production bootstraps
            # always inject a runtime from an explicit transport package.
            yield

    app = FastAPI(
        title="Execution Plane API",
        version="1.0.0",
        description="Command exchange and scoped execution resource queries.",
        lifespan=lifespan,
    )
    if runtime is not None:
        app.state.ep_runtime = runtime
        app.state.ep_sessions = runtime.sessions

    def current() -> CommandRuntime:
        service = getattr(app.state, "ep_runtime", None)
        if service is None:
            raise HTTPException(status_code=503, detail="Command runtime is not initialized")
        return cast("CommandRuntime", service)

    def domain_session() -> AsyncSession:
        session_factory = getattr(app.state, "ep_sessions", None)
        if session_factory is None:
            raise HTTPException(status_code=503, detail="Execution Plane database is not initialized")
        return cast("Callable[[], AsyncSession]", session_factory)()

    app.include_router(create_router(current))
    app.include_router(create_resource_router(current, domain_session))

    @app.get("/healthz/live", include_in_schema=False)
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/healthz/ready", include_in_schema=False)
    async def ready() -> dict[str, str]:
        try:
            async with domain_session() as session:
                await session.execute(text("SELECT 1"))
        except Exception:  # noqa: BLE001 - Readiness must not expose database exception details.
            raise HTTPException(status_code=503, detail="Command database unavailable") from None
        return {"status": "ready"}

    return app


app = create_app()
