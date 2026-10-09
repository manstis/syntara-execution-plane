"""EP command/resource reads preserve client, project and peer boundaries."""

from uuid import uuid4

import pytest

from execution_plane_client.errors import CommandNotFound
from execution_plane_client.protocol import CommandScope

from .command_helpers import work_case


async def test_domain_query_observes_claimed_work_and_redacts_private_context(work_message):
    from execution_plane.commands.queries import EPResourceQueries
    from execution_plane_client.queries import JournalQueryProvider

    async with work_case(work_message) as case:
        queries = JournalQueryProvider(case.db.journal)
        resource_queries = EPResourceQueries(case.db.journal)
        item = await case.store.claim_one()
        async with case.db.sessions() as session:
            read = await queries.get(session, work_message.scope, work_message.command_id, peer_id="ao")
            assert read.status.value == "running"
            assert read.resource.resource_id == item.id
            work = await resource_queries.work_items(session, work_message.scope, peer_id="ao")
            assert len(work) == 1
            assert "payload" not in work[0].model_dump()
            assert work[0].status == "claimed"
            other_scope = CommandScope(client_id="other", project_id=work_message.scope.project_id, all_projects=False)
            assert await resource_queries.work_items(session, other_scope, peer_id="ao") == []
            wrong_project = CommandScope(client_id="ao", project_id=uuid4(), all_projects=False)
            with pytest.raises(CommandNotFound):
                await resource_queries.work_item(session, wrong_project, item.id, peer_id="ao")
            with pytest.raises(CommandNotFound):
                await queries.get(session, work_message.scope, work_message.command_id, peer_id="other")
            targets = await resource_queries.targets(session, work_message.scope)
            assert targets[0].placement.namespace == "nodes"
            assert "credential" not in str(
                [row.model_dump() for row in await resource_queries.clusters(session, work_message.scope)]
            )
