"""Compose EP's registered command adapters with explicit trust and persistence."""

from __future__ import annotations

import base64
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from execution_plane.api.auth import build_certificate_auth
from execution_plane.commands.clusters import ClusterCommandHandler
from execution_plane.commands.control import CancelCommandHandler
from execution_plane.commands.receipts import EPReceiptHook
from execution_plane.commands.targets import TargetCommandHandler
from execution_plane.commands.work import WorkCommandHandler
from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.configuration import load_peer_trust
from execution_plane_client.handlers import CommandRegistry
from execution_plane_client.journal.schema import build_journal_schema
from execution_plane_client.service import JournalServiceBuilder, MediatorService
from execution_plane_client.transport import TLSConfig

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane.config import EPSettings
    from execution_plane_client.observers import MetricValue


class EPExchangeObserver:
    """Record only shared protocol-safe fields in EP structured telemetry."""

    def record(self, event: str, fields: Mapping[str, MetricValue]) -> None:
        """Publish bounded correlation and timing values supplied by the exchange."""
        structlog.get_logger("execution_plane.exchange").info(event, **dict(fields))


def build_ep_runtime(settings: EPSettings, session_factory: Callable[[], AsyncSession]) -> MediatorService:
    """Build the same durable exchange for API ingress and background EP workers."""
    # Require trust and a local signing key before composing the command exchange.
    if settings.command_peers_path is None or settings.command_signing_key_path is None:
        error = "EP_COMMAND_PEERS_PATH and EP_COMMAND_SIGNING_KEY_PATH are required"
        raise ValueError(error)
    trust = load_peer_trust(settings.command_peers_path)

    # Prefer command-specific mTLS files, with the API server's client identity as fallback.
    tls_ca = settings.command_tls_ca_path or settings.api_tls_client_ca_path
    tls_cert = settings.command_tls_cert_path or settings.api_tls_cert_path
    tls_key = settings.command_tls_key_path or settings.api_tls_key_path
    if not tls_ca or not tls_cert or not tls_key:
        error = "EP command transport requires a trusted CA and client certificate/key"
        raise ValueError(error)

    # Keep prior journal keys available for reads and use the active key for new entries.
    keys = {key_id: base64.urlsafe_b64decode(value) for key_id, value in settings.journal_decryption_keys.items()}
    keys[settings.journal_encryption_key_id] = base64.urlsafe_b64decode(settings.credential_encryption_key or "")
    observer = EPExchangeObserver()

    # The shared builder validates EP's executor identity and creates its journal and transport.
    builder = JournalServiceBuilder(
        sessions=session_factory,
        trust=trust,
        local_peer_id=settings.participant_id,
        expected_role="executor",
        schema=build_journal_schema("execution_plane"),
        journal_key_id=settings.journal_encryption_key_id,
        journal_keys=keys,
        signing_key_id=settings.command_signing_key_id,
        signing_private_key=Path(settings.command_signing_key_path).read_text(),
        tls=TLSConfig(tls_ca, tls_cert, tls_key),
        certificate_auth=build_certificate_auth(trust.certificate_names),
        options=settings.command_options,
        observer=observer,
    )
    journal = builder.journal

    # Bind EP's domain handlers to the same journal used by ingress and background workers.
    catalog, registry = CommandCatalog(), CommandRegistry()
    cluster_handler, target_handler = ClusterCommandHandler(journal), TargetCommandHandler(journal)
    for action in ("register", "provision", "update", "delete"):
        registry.register(catalog.definition(f"cluster.{action}", 1), cluster_handler)
    for action in ("register", "update", "delete"):
        registry.register(catalog.definition(f"execution_target.{action}", 1), target_handler)
    registry.register(catalog.definition("work.execute", 1), WorkCommandHandler(journal))
    registry.register(catalog.definition("command.cancel", 1), CancelCommandHandler(journal))

    # Validate receipt-time resource grants before accepting EP commands into the journal.
    return builder.build(
        registry=registry,
        receipt_hook=EPReceiptHook(journal),
    )
