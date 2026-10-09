"""The installed catalog gates command payloads and participant capabilities."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError


@pytest.mark.parametrize(
    "kind",
    [
        "work.execute",
        "cluster.register",
        "cluster.provision",
        "cluster.update",
        "cluster.delete",
        "execution_target.register",
        "execution_target.update",
        "execution_target.delete",
        "command.cancel",
    ],
)
def test_every_action_has_versioned_completion(kind):
    from execution_plane_client.catalog import CommandCatalog

    catalog = CommandCatalog()
    definition = catalog.definition(kind, 1)
    assert definition.completion_type == f"{kind}.completed"
    assert catalog.definition(definition.completion_type, 1).may_create_children is False
    assert catalog.definition("command.accepted", 1).requires_acceptance is False


def test_unsupported_revision_is_explicit():
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.errors import UnsupportedCommand

    with pytest.raises(UnsupportedCommand):
        CommandCatalog().definition("work.execute", 99)


@pytest.mark.parametrize(
    "change",
    [
        {"timeout_seconds": 0},
        {"max_output_bytes": True},
        {"version": 2},
        {"credentials": []},
    ],
)
def test_bad_node_invocation_is_rejected(change):
    from execution_plane_client.commands.work import NodeInvocation

    data = {
        "version": 1,
        "operation": "execute",
        "inputs": {},
        "credentials": {},
        "workflow_context": {},
        "settings": {},
        "timeout_seconds": 30,
        "max_output_bytes": 1024,
    }
    data.update(change)
    with pytest.raises(ValidationError):
        NodeInvocation.model_validate(data)


@pytest.mark.parametrize(
    "data",
    [
        {"backend_type": "vanilla_k8s", "placement": {"type": "rhel"}},
        {"backend_type": "openshell", "placement": {"type": "kubernetes", "namespace": "ep"}},
        {"backend_type": "vanilla_k8s", "placement": {"type": "kubernetes", "namespace": "../other"}},
        {
            "backend_type": "vanilla_k8s",
            "placement": {"type": "kubernetes", "namespace": "ep", "node_selectors": ["invalid selector"]},
        },
    ],
)
def test_invalid_target_backend_or_placement(data):
    from execution_plane_client.commands.targets import TargetRegister

    with pytest.raises(ValidationError):
        TargetRegister.model_validate(
            dict(
                resource_key=UUID(int=10),
                desired_revision=1,
                cluster_id=UUID(int=20),
                name="workers",
                endpoint="https://cluster.test",
                credential="test-only",
                **data,
            )
        )


def test_target_update_cannot_change_default_protection():
    from execution_plane_client.commands.targets import TargetUpdate

    with pytest.raises(ValidationError):
        TargetUpdate(resource_key=UUID(int=10), desired_revision=2, is_default=False)


def test_completion_requires_parent_and_terminal_state(wire_message):
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.errors import ProtocolConflict
    from execution_plane_client.protocol import MessageEnvelope

    wire_message["command"].update(command_type="work.execute.completed", payload={"status": "succeeded", "result": {}})
    with pytest.raises(ProtocolConflict):
        CommandCatalog().validate(MessageEnvelope.model_validate(wire_message))


def test_builder_produces_valid_stable_identity(wire_message):
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.commands import CommandIdentity, CommandRoute
    from execution_plane_client.commands.work import WorkExecute, work_execute

    command = work_execute(
        identity=CommandIdentity(
            command_id=UUID(int=2), message_id=UUID(int=1), occurred_at=datetime(2026, 10, 9, tzinfo=UTC)
        ),
        route=CommandRoute(
            source_id="ao",
            destination_id="ep",
            scope=wire_message["scope"],
            target_url="https://ep.test/v1/commands",
            callback_url="https://ao.test/v1/commands",
        ),
        payload=WorkExecute(
            work_correlation_id=UUID(int=4),
            image="node:test",
            invocation={
                "version": 1,
                "operation": "execute",
                "inputs": {},
                "credentials": {},
                "workflow_context": {},
                "settings": {},
                "timeout_seconds": 30,
                "max_output_bytes": 1024,
            },
        ),
    )
    CommandCatalog().validate(command)
    assert command.command_id == UUID(int=2)
    assert command.message_id == UUID(int=1)
    assert command.command.payload["invocation"]["version"] == 1


def test_capabilities_advertise_only_registered_handlers():
    from execution_plane_client.catalog import CapabilityRegistry, CommandCatalog
    from execution_plane_client.errors import ProtocolConflict
    from execution_plane_client.handlers import CommandRegistry

    class Handler:
        async def handle(self, message, context, session) -> None:
            return None

    catalog = CommandCatalog()
    handlers = CommandRegistry()
    handlers.register(catalog.definition("work.execute", 1), Handler())
    capabilities = CapabilityRegistry(handlers).advertise()
    assert capabilities.commands == {"command.accepted": [1], "work.execute": [1]}
    assert capabilities.completion_handlers == []
    with pytest.raises(ProtocolConflict):
        handlers.register(catalog.definition("work.execute", 1), Handler())


def test_update_preserves_explicit_null_and_omitted_fields(wire_message):
    from execution_plane_client.commands import CommandIdentity, CommandRoute
    from execution_plane_client.commands.clusters import ClusterUpdate, cluster_update

    envelope = cluster_update(
        identity=CommandIdentity(
            command_id=UUID(int=2), message_id=UUID(int=1), occurred_at=datetime(2026, 10, 9, tzinfo=UTC)
        ),
        route=CommandRoute(
            source_id="ao",
            destination_id="ep",
            scope=wire_message["scope"],
            target_url="https://ep.test/v1/commands",
            callback_url="https://ao.test/v1/commands",
        ),
        payload=ClusterUpdate(resource_key=UUID(int=10), desired_revision=2, ca_certificate=None, labels={}),
    )
    assert envelope.command.payload["ca_certificate"] is None
    assert envelope.command.payload["labels"] == {}
    assert "credential" not in envelope.command.payload
    assert "project_ids" not in envelope.command.payload


@pytest.mark.parametrize("kind", ["cluster.register", "cluster.update", "execution_target.register"])
async def test_management_endpoints_preserve_https_requirement(kind):
    from uuid import uuid4

    from pydantic import ValidationError

    from execution_plane_client.catalog import CommandCatalog

    values = {"resource_key": uuid4(), "desired_revision": 1, "endpoint": "http://insecure.test"}
    if kind == "cluster.register":
        values.update(name="cluster", credential="private")
    if kind == "execution_target.register":
        values.update(
            name="target",
            cluster_id=uuid4(),
            backend_type="vanilla_k8s",
            placement={"type": "kubernetes", "namespace": "nodes"},
        )
    with pytest.raises(ValidationError):
        CommandCatalog().definition(kind, 1).payload_schema.model_validate(values)
