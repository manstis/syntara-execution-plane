"""Transport-neutral registration of EP command handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

from execution_plane.commands.clusters import ClusterCommandHandler
from execution_plane.commands.control import CancelCommandHandler
from execution_plane.commands.targets import TargetCommandHandler
from execution_plane.commands.work import WorkCommandHandler
from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.handlers import CommandRegistry
from execution_plane_client.observers import ExchangeObserver

if TYPE_CHECKING:
    from collections.abc import Mapping

    from execution_plane_client.observers import MetricValue
    from execution_plane_client.queries import CommandQueryProvider
    from execution_plane_client.resources import ResourceIntentStore


class EPExchangeObserver(ExchangeObserver):
    """Record protocol-safe exchange fields in EP structured telemetry."""

    def record(self, event: str, fields: Mapping[str, MetricValue]) -> None:
        """Publish bounded correlation and timing values supplied by the exchange."""
        structlog.get_logger("execution_plane.exchange").info(event, **dict(fields))


def build_ep_registry(
    *, queries: CommandQueryProvider, resource_intents: ResourceIntentStore
) -> tuple[CommandCatalog, CommandRegistry]:
    """Create the shared EP registry without choosing how commands are received or sent."""
    catalog, registry = CommandCatalog(), CommandRegistry()
    cluster_handler = ClusterCommandHandler(resource_intents)
    target_handler = TargetCommandHandler(resource_intents)
    for action in ("register", "provision", "update", "delete"):
        registry.register(catalog.definition(f"cluster.{action}", 1), cluster_handler)
    for action in ("register", "update", "delete"):
        registry.register(catalog.definition(f"execution_target.{action}", 1), target_handler)
    registry.register(catalog.definition("work.execute", 1), WorkCommandHandler())
    registry.register(catalog.definition("command.cancel", 1), CancelCommandHandler(queries))
    return catalog, registry
