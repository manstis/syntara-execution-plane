"""End-to-end dispatch integration test: submit → dispatch → COMPLETED.

Requires a real PostgreSQL database (``EP_TEST_DATABASE_URL``) and a live
Kubernetes cluster (``EP_IT_K8S_*`` env vars). Both are provisioned in CI by
the ``test-integration`` GitHub Actions job. Tests are skipped automatically
when either resource is absent.
"""

from __future__ import annotations

import base64
import os
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.cluster.cluster_store import ClusterStore
from execution_plane.commands.receipts import EPReceiptHook
from execution_plane.commands.work import EPCompletionRecorder, WorkCommandHandler
from execution_plane.config import get_ep_settings
from execution_plane.execution_target.execution_target_store import ExecutionTargetStore
from execution_plane.execution_target_reconciler.placement import WorkerManagerRegistry
from execution_plane.models.execution_target import BackendType
from execution_plane.models.work_item import WorkItemStatus
from execution_plane.work_store import WorkStore
from execution_plane.worker import _process_item
from execution_plane.worker_manager.vanilla_k8s.manager import VanillaK8sWorkerManager
from execution_plane_client.catalog import CommandCatalog
from execution_plane_client.commands import CommandIdentity, CommandRoute
from execution_plane_client.commands.work import WorkExecute, work_execute
from execution_plane_client.crypto import AESGCMCipher
from execution_plane_client.handlers import CommandRegistry
from execution_plane_client.ingress import CommandReceiptHandler
from execution_plane_client.journal.schema import build_journal_schema
from execution_plane_client.journal.store import ExchangeJournalStore
from execution_plane_client.security import AuthenticatedIdentity
from execution_plane_client.workers.handling import JournalHandlingWorker

_NODE_IMAGE = "quay.io/ahetheri/syntara-node-script:migration-test"

_SCRIPT_INVOCATION = {
    "version": 1,
    "operation": "execute",
    "inputs": {"language": "bash", "code": "echo ep-integration-ok"},
    "credentials": {"resolved": {}},
    "workflow_context": {},
    "settings": {},
    "timeout_seconds": 120,
}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_work_item_dispatched_to_kind_cluster_reaches_completed(ep_cluster) -> None:
    """Submit a script work item and assert it completes through a single dispatch cycle.

    The ep_cluster fixture provisions an ACTIVE Cluster+ExecutionTarget backed by
    the kind cluster provisioned in CI. claim_one() assigns that target to the work
    item; _process_item() drives the full cold-start pod lifecycle synchronously.
    """
    database_url = os.environ["EP_TEST_DATABASE_URL"]
    settings = get_ep_settings()
    project_id = uuid.uuid4()
    client_id = "ep-it-dispatch"
    command_id = uuid.uuid4()
    catalog, registry = CommandCatalog(), CommandRegistry()
    journal = ExchangeJournalStore(
        build_journal_schema("execution_plane"),
        AESGCMCipher(active_key_id="kind", keys={"kind": base64.urlsafe_b64decode(settings.credential_encryption_key)}),
    )
    registry.register(catalog.definition("work.execute", 1), WorkCommandHandler(journal))
    ingress = CommandReceiptHandler(journal, catalog, registry, local_id="ep", receipt_hook=EPReceiptHook(journal))
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    message = work_execute(
        identity=CommandIdentity(command_id=command_id, message_id=uuid.uuid4(), occurred_at=datetime.now(UTC)),
        route=CommandRoute(
            source_id=client_id,
            destination_id="ep",
            scope={"client_id": client_id, "project_id": project_id, "all_projects": False},
            target_url="https://ep.test/v1/commands",
            callback_url="https://ao.test/v1/commands",
        ),
        payload=WorkExecute(image=_NODE_IMAGE, invocation=_SCRIPT_INVOCATION, work_correlation_id=uuid.uuid4()),
    )
    async with sessions.begin() as session:
        await ingress.receive(
            message, AuthenticatedIdentity(client_id, message.scope, frozenset({"work.execute"})), session
        )
    handler = JournalHandlingWorker(journal, sessions, registry, local_id="ep")
    await handler.run_once()
    async with sessions() as session:
        record = await journal.get(session, message.scope, command_id, "inbound")
        assert record is not None
        work_id = record.resource_id

    async with (
        WorkStore.from_database_url(database_url) as work_store,
        ClusterStore.from_database_url(database_url) as cluster_store,
        ExecutionTargetStore.from_database_url(database_url) as target_store,
    ):
        work_store.set_completion_recorder(EPCompletionRecorder(journal))

        claimed = await work_store.claim_one()
        assert claimed is not None, "claim_one() returned None — no eligible target is ACTIVE"
        assert claimed.id == work_id

        worker_managers = WorkerManagerRegistry()
        workload_manager = VanillaK8sWorkerManager(target_store, cluster_store, work_store, settings)
        worker_managers.register(BackendType.VANILLA_K8S, workload_manager)

        await _process_item(claimed, work_store, target_store, worker_managers, settings)

        final_item = await work_store.get(work_id, client_id=client_id, project_id=project_id)

    assert final_item is not None
    assert final_item.status is WorkItemStatus.COMPLETED, (
        f"Expected COMPLETED but got {final_item.status}; result={final_item.result}"
    )

    async with sessions() as session:
        table = journal.schema.table
        child = await session.scalar(
            select(table.c.entry_id).where(
                table.c.parent_command_id == command_id, table.c.command_type == "work.execute.completed"
            )
        )
        assert child is not None
        assert (await journal.load_message(session, child)).command.payload["status"] == "succeeded"
    await engine.dispose()
