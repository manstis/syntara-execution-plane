"""Direct-transport composition for the EP API and worker applications."""

from __future__ import annotations

import asyncio
import logging
import ssl
from pathlib import Path
from typing import TYPE_CHECKING

import uvicorn
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.api.auth import build_certificate_auth
from execution_plane.api.main import create_app
from execution_plane.commands.runtime import EPExchangeObserver, build_ep_registry
from execution_plane.config import get_ep_settings
from execution_plane.worker import run_worker
from execution_plane_client.configuration import load_peer_trust
from execution_plane_transport_direct.completion_recorder import DirectCompletionRecorder
from execution_plane_transport_direct.ep_queries import EPDirectCommandQueryProvider
from execution_plane_transport_direct.resource_intents import InMemoryResourceIntentStore
from execution_plane_transport_direct.runtime import DirectRuntimeFactory
from execution_plane_transport_direct.transport import TLSConfig

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane.config import EPSettings
    from execution_plane_transport_direct.service import DirectCommandRuntime


def build_ep_direct_runtime(settings: EPSettings, session_factory: Callable[[], AsyncSession]) -> DirectCommandRuntime:
    """Compose EP handlers with direct receipt and immediate outbound HTTP."""
    if settings.command_peers_path is None or settings.command_signing_key_path is None:
        error = "EP_COMMAND_PEERS_PATH and EP_COMMAND_SIGNING_KEY_PATH are required"
        raise ValueError(error)
    trust = load_peer_trust(settings.command_peers_path)

    tls_ca = settings.command_tls_ca_path or settings.api_tls_client_ca_path
    tls_cert = settings.command_tls_cert_path or settings.api_tls_cert_path
    tls_key = settings.command_tls_key_path or settings.api_tls_key_path
    if not tls_ca or not tls_cert or not tls_key:
        error = "EP command transport requires a trusted CA and client certificate/key"
        raise ValueError(error)

    queries = EPDirectCommandQueryProvider(session_factory)
    _catalog, registry = build_ep_registry(
        queries=queries,
        resource_intents=InMemoryResourceIntentStore(),
    )
    factory = DirectRuntimeFactory(
        trust=trust,
        local_peer_id=settings.participant_id,
        expected_role="executor",
        signing_key_id=settings.command_signing_key_id,
        signing_private_key=Path(settings.command_signing_key_path).read_text(),
        tls=TLSConfig(tls_ca, tls_cert, tls_key),
        certificate_auth=build_certificate_auth(trust.certificate_names),
        observer=EPExchangeObserver(),
        timeout_seconds=settings.command_options.request_timeout_seconds,
    )
    runtime = factory.build(registry, queries=queries, sessions=session_factory)
    runtime.work_completion_recorder = DirectCompletionRecorder(transport=runtime.transport, queries=queries)
    return runtime


def api_main() -> None:
    """Start the EP API with the direct runtime selected by this entry point."""
    logging.basicConfig(level=logging.INFO)
    settings = get_ep_settings()
    if not all((settings.api_tls_cert_path, settings.api_tls_key_path, settings.api_tls_client_ca_path)):
        error = "EP API requires its TLS certificate, key and trusted client CA"
        raise ValueError(error)
    engine = create_async_engine(settings.database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    runtime = build_ep_direct_runtime(settings, sessions)
    try:
        uvicorn.run(
            create_app(runtime=runtime),
            host=settings.api_host,
            port=settings.api_port,
            ssl_certfile=settings.api_tls_cert_path,
            ssl_keyfile=settings.api_tls_key_path,
            ssl_ca_certs=settings.api_tls_client_ca_path,
            ssl_cert_reqs=ssl.CERT_REQUIRED,
            http="execution_plane.api.tls:TLSH11Protocol",
        )
    finally:
        asyncio.run(engine.dispose())


async def _run_worker() -> None:
    settings = get_ep_settings()
    engine = create_async_engine(settings.database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    runtime = build_ep_direct_runtime(settings, sessions)
    try:
        await run_worker(settings.database_url, runtime)
    finally:
        await runtime.close()
        await engine.dispose()


def worker_main() -> None:
    """Run EP provider processing with the direct command runtime."""
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_run_worker())
