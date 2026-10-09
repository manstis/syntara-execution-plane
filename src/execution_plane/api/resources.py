"""Authenticated safe resource reads; every mutation uses the command ingress."""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated
from uuid import UUID  # noqa: TC003 - FastAPI resolves route annotations at runtime.

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.security import HTTPBearer
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: TC002 - FastAPI resolves dependency types.

from execution_plane.commands.queries import EPResourceQueries
from execution_plane_client.api import authenticate_query
from execution_plane_client.errors import CommandNotFound
from execution_plane_client.resources import ClusterRead, ExecutionTargetRead, WorkItemRead
from execution_plane_client.security import AuthenticatedIdentity  # noqa: TC001 - FastAPI dependency annotation.

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from execution_plane_client.service import MediatorService


def create_resource_router(current: Callable[[], MediatorService]) -> APIRouter:  # noqa: C901 - Six explicit resource routes preserve precise response schemas.
    """Attach the same certificate/assertion boundary to bounded resource reads."""
    router = APIRouter(prefix="/v1", dependencies=[Depends(HTTPBearer(auto_error=False))])

    async def identity(request: Request) -> AuthenticatedIdentity:
        return await authenticate_query(request, current())

    async def database() -> AsyncIterator[AsyncSession]:
        async with current().sessions() as session:
            yield session

    authenticated, connection = Depends(identity), Depends(database)

    @router.get("/work-items", response_model=list[WorkItemRead])
    async def work_items(
        identity: AuthenticatedIdentity = authenticated,
        session: AsyncSession = connection,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> list[WorkItemRead]:
        return await EPResourceQueries(current().journal).work_items(
            session, identity.scope, limit=limit, peer_id=identity.peer_id
        )

    @router.get("/work-items/{resource_id}", response_model=WorkItemRead)
    async def work_item(
        resource_id: UUID, identity: AuthenticatedIdentity = authenticated, session: AsyncSession = connection
    ) -> WorkItemRead:
        try:
            return await EPResourceQueries(current().journal).work_item(
                session, identity.scope, resource_id, peer_id=identity.peer_id
            )
        except CommandNotFound:
            raise HTTPException(status_code=404, detail="Work item not found") from None

    @router.get("/clusters", response_model=list[ClusterRead])
    async def clusters(
        identity: AuthenticatedIdentity = authenticated,
        session: AsyncSession = connection,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> list[ClusterRead]:
        return await EPResourceQueries(current().journal).clusters(session, identity.scope, limit=limit)

    @router.get("/clusters/{resource_id}", response_model=ClusterRead)
    async def cluster(
        resource_id: UUID, identity: AuthenticatedIdentity = authenticated, session: AsyncSession = connection
    ) -> ClusterRead:
        try:
            return await EPResourceQueries(current().journal).cluster(session, identity.scope, resource_id)
        except CommandNotFound:
            raise HTTPException(status_code=404, detail="Cluster not found") from None

    @router.get("/execution-targets", response_model=list[ExecutionTargetRead])
    async def targets(
        identity: AuthenticatedIdentity = authenticated,
        session: AsyncSession = connection,
        limit: Annotated[int, Query(ge=1, le=200)] = 50,
    ) -> list[ExecutionTargetRead]:
        return await EPResourceQueries(current().journal).targets(session, identity.scope, limit=limit)

    @router.get("/execution-targets/{resource_id}", response_model=ExecutionTargetRead)
    async def target(
        resource_id: UUID, identity: AuthenticatedIdentity = authenticated, session: AsyncSession = connection
    ) -> ExecutionTargetRead:
        try:
            return await EPResourceQueries(current().journal).target(session, identity.scope, resource_id)
        except CommandNotFound:
            raise HTTPException(status_code=404, detail="Execution target not found") from None

    return router
