"""Versioned definitions and capability advertisement for installed handlers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from execution_plane_client.commands import ResourceRevision
from execution_plane_client.commands.clusters import ClusterProvision, ClusterRegister, ClusterUpdate
from execution_plane_client.commands.control import Acceptance, CancelCommand, Completion
from execution_plane_client.commands.targets import TargetRegister, TargetUpdate
from execution_plane_client.commands.work import WorkExecute
from execution_plane_client.errors import ProtocolConflict, UnsupportedCommand
from execution_plane_client.protocol import CapabilitiesRead, MessageEnvelope, WireModel

if TYPE_CHECKING:
    from execution_plane_client.handlers import CommandRegistry


@dataclass(frozen=True)
class CommandDefinition:
    """Payload, child and execution policies for one command revision."""

    command_type: str
    revision: int
    payload_schema: type[WireModel]
    completion_type: str | None
    kind: Literal["action", "completion", "acceptance"]
    required_scope: Literal["project", "resource", "parent"]
    source_roles: frozenset[str]
    destination_roles: frozenset[str]
    may_create_children: bool = True
    requires_acceptance: bool = True
    cancellable: bool = False
    reconciliation: Literal["domain", "external", "receipt"] = "domain"


class CommandCatalog:
    """Validate the shared catalog independently of any installed participant."""

    def __init__(self) -> None:
        """Install revision-one definitions and their finite completion contracts."""
        actions: dict[str, type[WireModel]] = {
            "work.execute": WorkExecute,
            "cluster.register": ClusterRegister,
            "cluster.provision": ClusterProvision,
            "cluster.update": ClusterUpdate,
            "cluster.delete": ResourceRevision,
            "execution_target.register": TargetRegister,
            "execution_target.update": TargetUpdate,
            "execution_target.delete": ResourceRevision,
            "command.cancel": CancelCommand,
        }
        self._definitions: dict[tuple[str, int], CommandDefinition] = {}
        for kind, payload in actions.items():
            scope: Literal["project", "resource", "parent"] = "project" if kind == "work.execute" else "resource"
            action = CommandDefinition(
                kind,
                1,
                payload,
                f"{kind}.completed",
                "action",
                scope,
                frozenset({"controller"}),
                frozenset({"executor"}),
                cancellable=kind == "work.execute",
            )
            completion = CommandDefinition(
                f"{kind}.completed",
                1,
                Completion,
                None,
                "completion",
                "parent",
                action.destination_roles,
                action.source_roles,
                may_create_children=False,
                reconciliation="external",
            )
            self._definitions[kind, 1] = action
            self._definitions[completion.command_type, 1] = completion
        self._definitions["command.accepted", 1] = CommandDefinition(
            "command.accepted",
            1,
            Acceptance,
            None,
            "acceptance",
            "parent",
            frozenset({"controller", "executor"}),
            frozenset({"controller", "executor"}),
            may_create_children=False,
            requires_acceptance=False,
            reconciliation="receipt",
        )

    def definition(self, command_type: str, revision: int) -> CommandDefinition:
        """Return a supported definition or a safe explicit rejection."""
        try:
            return self._definitions[command_type, revision]
        except KeyError:
            error = "Unsupported command revision"
            raise UnsupportedCommand(error) from None

    def validate(self, message: MessageEnvelope) -> None:
        """Check payload and cross-envelope invariants before persistence."""
        command = message.command
        definition = self.definition(command.command_type, command.command_revision)
        payload = definition.payload_schema.model_validate(command.payload)
        if definition.required_scope == "project" and message.scope.project_id is None:
            error = "Command requires a project"
            raise ProtocolConflict(error)
        if definition.kind != "action" and command.parent_command_id is None:
            error = "Control and completion commands require a parent"
            raise ProtocolConflict(error)
        if definition.kind == "completion" and command.parent_sequence is None:
            error = "Completion requires a parent sequence"
            raise ProtocolConflict(error)
        if definition.kind == "acceptance":
            assert isinstance(payload, Acceptance)  # noqa: S101 - Internal catalog invariant.
            if (
                payload.message_id != command.acknowledges_message_id
                or payload.command_id != command.parent_command_id
                or command.parent_sequence is not None
            ):
                error = "Acceptance must identify its exact parent message"
                raise ProtocolConflict(error)
        elif command.acknowledges_message_id is not None:
            error = "Only acceptance may acknowledge a message"
            raise ProtocolConflict(error)
        if isinstance(payload, WorkExecute) and command.execution_policy is not None:
            if command.execution_policy.timeout_seconds != payload.invocation.timeout_seconds:
                error = "Execution timeout differs from invocation"
                raise ProtocolConflict(error)
        elif command.execution_policy is not None:
            error = "Execution policy is only valid for work"
            raise ProtocolConflict(error)


class CapabilityRegistry:
    """Advertise installed handlers, including the mandatory receipt control."""

    def __init__(self, handlers: CommandRegistry) -> None:
        """Use the handler registry as the single source of availability."""
        self._handlers = handlers

    def advertise(self) -> CapabilitiesRead:
        """Return only commands this participant can durably take responsibility for."""
        commands = {"command.accepted": [1]}
        completions = []
        for definition in self._handlers.definitions():
            commands.setdefault(definition.command_type, []).append(definition.revision)
            if definition.kind == "completion":
                completions.append(definition.command_type)
        return CapabilitiesRead(
            commands={kind: sorted(set(versions)) for kind, versions in sorted(commands.items())},
            receipt_handlers=sorted(commands),
            completion_handlers=sorted(set(completions)),
        )
