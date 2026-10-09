"""Expose the TLS-verified client certificate through a trusted ASGI extension."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from uvicorn.protocols.http.h11_impl import H11Protocol

if TYPE_CHECKING:
    import asyncio


class TLSH11Protocol(H11Protocol):
    """Inject transport certificate data before Uvicorn's ASGI task can run."""

    _peercert: dict[str, Any] | None = None

    def connection_made(self, transport: asyncio.Transport) -> None:  # type: ignore[override]
        """Read the certificate already verified during the TLS handshake."""
        super().connection_made(transport)
        tls = transport.get_extra_info("ssl_object")
        self._peercert = tls.getpeercert() if tls is not None else None

    def handle_events(self) -> None:
        """Attach certificate metadata to each HTTP scope on this connection."""
        super().handle_events()
        if self._peercert and self.scope is not None:
            self.scope.setdefault("extensions", {})["tls"] = {"peercert": self._peercert}
