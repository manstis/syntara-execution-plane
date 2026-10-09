"""Exact peer routing and DNS-pinned HTTPS connections with hostname verification."""

from __future__ import annotations

import asyncio
import ipaddress
import re
import socket
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from execution_plane_client.errors import UnsafeDestination

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Mapping

    from execution_plane_client.security import PeerConfig

    type Resolver = Callable[[str, int], Awaitable[list[str]]]


async def resolve_host(host: str, port: int) -> list[str]:
    """Resolve one connection attempt using the platform's asynchronous resolver."""
    addresses = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return sorted({address[4][0] for address in addresses})


@dataclass(frozen=True)
class RegisteredDestination:
    """Validated route and the network policy it must satisfy before connecting."""

    url: str
    peer_id: str
    hostname: str
    port: int
    host_header: str
    networks: tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, ...]


class DestinationPolicy:
    """Require a configured peer route and allowlisted resolution on every attempt."""

    def __init__(self, peers: Mapping[str, PeerConfig], *, resolver: Resolver = resolve_host) -> None:
        """Copy peer policy; resolver injection is for deterministic network tests."""
        self._peers = dict(peers)
        self._resolver = resolver

    def validate(self, url: str, peer_id: str, *, query: bool = False) -> RegisteredDestination:
        """Authorize only exact ingress or supported GET resources on a peer."""
        peer = self._peers.get(peer_id)
        if peer is None:
            raise UnsafeDestination
        parsed = urlsplit(url)
        base = urlsplit(peer.query_base_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or any(char.isspace() for char in url)
        ):
            raise UnsafeDestination
        if query:
            resource_path = re.escape(base.path.rstrip("/")) + (
                r"/(?:capabilities|commands/[0-9a-f-]+(?:/children)?"
                r"|(?:work-items|clusters|execution-targets)(?:/[0-9a-f-]+)?)"
            )
            if parsed.netloc != base.netloc or not re.fullmatch(resource_path, parsed.path):
                raise UnsafeDestination
        elif url != peer.command_url or parsed.query:
            raise UnsafeDestination
        try:
            networks = tuple(ipaddress.ip_network(network) for network in peer.permitted_networks)
            port = parsed.port or 443
        except ValueError:
            raise UnsafeDestination from None
        if not networks:
            raise UnsafeDestination
        return RegisteredDestination(url, peer_id, parsed.hostname, port, parsed.netloc, networks)

    async def resolve(self, destination: RegisteredDestination) -> list[str]:
        """Reject mixed safe/unsafe DNS answers and return approved numeric addresses."""
        addresses = await self._resolver(destination.hostname, destination.port)
        if not addresses:
            raise UnsafeDestination
        for value in addresses:
            try:
                address = ipaddress.ip_address(value)
            except ValueError:
                raise UnsafeDestination from None
            if not any(address in network for network in destination.networks):
                raise UnsafeDestination
        return addresses
