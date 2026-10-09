"""Factory for authenticated, Journal-backed participant runtimes."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from execution_plane_client.crypto import AESGCMCipher
from execution_plane_client.observers import ExchangeObserver, NullObserver
from execution_plane_client.security import AssertionSigner, AssertionVerifier
from execution_plane_transport_direct.transport import HttpCommandTransport, TLSConfig
from execution_plane_transport_journal.configuration import ExchangeOptions
from execution_plane_transport_journal.service import JournalCommandRuntime, build_journal_service
from execution_plane_transport_journal.store import ExchangeJournalStore

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.api import CertificateAuthenticator
    from execution_plane_client.configuration import PeerTrust
    from execution_plane_client.handlers import CommandRegistry, CompletionRecorder, HandledHook, ReceiptPolicy
    from execution_plane_client.queries import SessionCommandQueryProvider
    from execution_plane_transport_journal.schema import JournalSchema


def _now() -> datetime:
    return datetime.now(UTC)


class JournalRuntimeFactory:
    """Build a participant's durable runtime from its local security inputs."""

    def __init__(
        self,
        *,
        sessions: Callable[[], AsyncSession],
        trust: PeerTrust,
        local_peer_id: str,
        expected_role: str,
        schema: JournalSchema,
        journal_key_id: str,
        journal_keys: Mapping[str, bytes],
        signing_key_id: str,
        signing_private_key: str,
        tls: TLSConfig,
        certificate_auth: CertificateAuthenticator,
        options: ExchangeOptions | None = None,
        observer: ExchangeObserver | None = None,
        clock: Callable[[], datetime] = _now,
        default_peer_id: str | None = None,
    ) -> None:
        """Create the journal and retain participant-owned transport configuration."""
        local = trust.peers.get(local_peer_id)
        if local is None:
            error = f"Local peer {local_peer_id!r} is not registered"
            raise ValueError(error)
        if local.role != expected_role:
            error = f"Local peer must have role {expected_role!r}, not {local.role!r}"
            raise ValueError(error)
        if local.command_url is None:
            error = "A durable service requires its own command URL"
            raise ValueError(error)
        if default_peer_id is not None and (default_peer_id not in trust.peers or default_peer_id == local_peer_id):
            error = "The default command peer must be a registered remote peer"
            raise ValueError(error)
        self.journal = ExchangeJournalStore(
            schema,
            AESGCMCipher(active_key_id=journal_key_id, keys=journal_keys),
        )
        self.sessions = sessions
        self.trust = trust
        self.local_peer = local
        self.signing_key_id = signing_key_id
        self.signing_private_key = signing_private_key
        self.tls = tls
        self.certificate_auth = certificate_auth
        self.options = options or ExchangeOptions()
        self.observer = observer or NullObserver()
        self.clock = clock
        self.default_peer_id = default_peer_id
        self._built = False

    def build(
        self,
        registry: CommandRegistry,
        *,
        receipt_hook: ReceiptPolicy | None = None,
        handled_hook: HandledHook | None = None,
        completion_recorder: CompletionRecorder | None = None,
        queries: SessionCommandQueryProvider | None = None,
    ) -> JournalCommandRuntime:
        """Create signer, verifier, Journal transport and workers once."""
        if self._built:
            error = "A Journal runtime factory can only build one runtime"
            raise RuntimeError(error)
        signer = AssertionSigner(
            local_id=self.local_peer.peer_id,
            issuer=self.local_peer.issuer,
            key_id=self.signing_key_id,
            private_key=self.signing_private_key,
            peers=self.trust.peers,
        )
        transport = HttpCommandTransport(
            signer=signer,
            peers=self.trust.peers,
            tls=self.tls,
            timeout_seconds=self.options.request_timeout_seconds,
            observer=self.observer,
        )
        verifier = AssertionVerifier(
            local_id=self.local_peer.peer_id,
            audience=self.local_peer.audience,
            command_url=self.local_peer.command_url,
            peers=self.trust.peers,
        )
        service = build_journal_service(
            store=self.journal,
            sessions=self.sessions,
            local_peer=self.local_peer,
            peers=self.trust.peers,
            registry=registry,
            transport=transport,
            verifier=verifier,
            certificate_auth=self.certificate_auth,
            receipt_hook=receipt_hook,
            handled_hook=handled_hook,
            completion_recorder=completion_recorder,
            queries=queries,
            clock=self.clock,
            observer=self.observer,
            options=self.options,
            default_peer_id=self.default_peer_id,
        )
        self._built = True
        return service
