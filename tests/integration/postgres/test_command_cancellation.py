"""Cancellation reserves future work identity and rejects irreversible undo."""

from datetime import UTC, datetime
from uuid import uuid4

from execution_plane_client.commands import CommandIdentity, CommandRoute
from execution_plane_client.commands.control import CancelCommand, cancel_command

from .command_helpers import resource_case


def cancellation(target, scope):
    return cancel_command(
        identity=CommandIdentity(command_id=uuid4(), message_id=uuid4(), occurred_at=datetime.now(UTC)),
        route=CommandRoute(
            source_id="ao",
            destination_id="ep",
            scope=scope,
            target_url="https://ep.test/v1/commands",
            callback_url="https://ao.test/v1/commands",
        ),
        payload=CancelCommand(target_command_id=target),
    )


async def test_cancel_before_original_keeps_scoped_intent():
    from execution_plane.commands.control import CancelCommandHandler

    async with resource_case() as case:
        case.registry.register(case.catalog.definition("command.cancel", 1), CancelCommandHandler(case.command_queries))
        target, project = uuid4(), uuid4()
        message = cancellation(target, {"client_id": "ao", "project_id": project, "all_projects": False})
        assert (await case.run(message)).status == "succeeded"
        async with case.db.sessions() as session:
            intent = await case.db.journal.get(session, message.scope, message.command_id, "inbound")
            assert intent.target_command_id == target
            assert intent.project_id == project


async def test_resource_cancellation_reports_unsupported_instead_of_undoing():
    from execution_plane.commands.control import CancelCommandHandler

    async with resource_case() as case:
        case.registry.register(case.catalog.definition("command.cancel", 1), CancelCommandHandler(case.command_queries))
        original = case.message()
        await case.run(original)
        result = await case.run(cancellation(original.command_id, original.scope))
        assert result.status == "failed"
        assert result.error_code == "cancellation_not_supported"
        assert len(await case.clusters()) == 1
