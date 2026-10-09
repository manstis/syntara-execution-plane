"""Factory for authenticated, immediate participant runtimes."""

from __future__ import annotations

from typing import TYPE_CHECKING

from execution_plane_client.observers import NullObserver
from execution_plane_client.security import AssertionSigner, AssertionVerifier
from execution_plane_transport_direct.transport import HttpCommandTransport, TLSConfig

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    from execution_plane_client.api import CertificateAuthenticator
    from execution_plane_client.configuration import PeerTrust
    from execution_plane_client.handlers import CommandRegistry
    from execution_plane_client.observers import ExchangeObserver
    from execution_plane_client.queries import CommandStateProvider
    from execution_plane_client.security import PeerConfig

from execution_plane_transport_direct.service import DirectCommandRuntime, build_direct_service


class DirectRuntimeFactory:
    """Construct the client receiver with immediate HTTP transport and no Journal."""

    def __init__(
        self,
        *,
        trust: PeerTrust,
        local_peer_id: str,
        expected_role: str,
        signing_key_id: str,
        signing_private_key: str,
        tls: TLSConfig,
        certificate_auth: CertificateAuthenticator,
        observer: ExchangeObserver | None = None,
        timeout_seconds: float = 10,
        default_peer_id: str | None = None,
    ) -> None:
        """Validate the local identity and retain explicit security configuration."""
        local = trust.peers.get(local_peer_id)
        if local is None:
            error = f"Local peer {local_peer_id!r} is not registered"
            raise ValueError(error)
        if local.role != expected_role:
            error = f"Local peer must have role {expected_role!r}, not {local.role!r}"
            raise ValueError(error)
        if local.command_url is None:
            error = "A direct service requires its own command URL"
            raise ValueError(error)
        if default_peer_id is not None and (default_peer_id not in trust.peers or default_peer_id == local_peer_id):
            error = "The default command peer must be a registered remote peer"
            raise ValueError(error)
        self._trust = trust
        self._local: PeerConfig = local
        self._signing_key_id = signing_key_id
        self._signing_private_key = signing_private_key
        self._tls = tls
        self._certificate_auth = certificate_auth
        self._observer = observer or NullObserver()
        self._timeout_seconds = timeout_seconds
        self._default_peer_id = default_peer_id
        self._built = False

    def build(
        self,
        registry: CommandRegistry,
        *,
        queries: CommandStateProvider | None = None,
        sessions: Callable[[], AsyncSession] | None = None,
        max_in_memory_commands: int = 10_000,
    ) -> DirectCommandRuntime:
        """Create the immediate runtime and shared receiver around registered handlers."""
        if self._built:
            error = "A direct runtime factory can only build one runtime"
            raise RuntimeError(error)
        signer = AssertionSigner(
            local_id=self._local.peer_id,
            issuer=self._local.issuer,
            key_id=self._signing_key_id,
            private_key=self._signing_private_key,
            peers=self._trust.peers,
        )
        transport = HttpCommandTransport(
            signer=signer,
            peers=self._trust.peers,
            tls=self._tls,
            observer=self._observer,
            timeout_seconds=self._timeout_seconds,
        )
        verifier = AssertionVerifier(
            local_id=self._local.peer_id,
            audience=self._local.audience,
            command_url=self._local.command_url,
            peers=self._trust.peers,
        )
        service = build_direct_service(
            local_id=self._local.peer_id,
            local_role=self._local.role,
            command_url=self._local.command_url,
            peers=self._trust.peers,
            registry=registry,
            transport=transport,
            verifier=verifier,
            certificate_auth=self._certificate_auth,
            queries=queries,
            sessions=sessions,
            default_peer_id=self._default_peer_id,
            max_in_memory_commands=max_in_memory_commands,
        )
        self._built = True
        return service
