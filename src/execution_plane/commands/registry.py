"""Install only the commands handled by EP."""

from execution_plane.commands.clusters import ClusterCommandHandler
from execution_plane.commands.context import EPCommandContext
from execution_plane.commands.control import CancelCommandHandler
from execution_plane.commands.targets import TargetCommandHandler
from execution_plane.commands.work import WorkCommandHandler
from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.registry import CommandRegistry


def build_ep_registry() -> CommandRegistry[EPCommandContext]:
    """Construct the stateless handler registry without opening a database."""
    catalog = CommandCatalog()
    registry = CommandRegistry[EPCommandContext]()
    registry.register(catalog.definition("work.execute", 1), WorkCommandHandler())
    registry.register(catalog.definition("command.cancel", 1), CancelCommandHandler())
    for action in ("register", "provision", "update", "delete"):
        registry.register(catalog.definition(f"cluster.{action}", 1), ClusterCommandHandler())
    for action in ("register", "update", "delete"):
        registry.register(catalog.definition(f"execution_target.{action}", 1), TargetCommandHandler())
    return registry
