"""Publish EP work completions over direct transport after provider commits."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, cast

from execution_plane_client.handlers import CompletionRecorder
from execution_plane_transport_direct.work_completion import completion_for_work_item

if TYPE_CHECKING:
    from execution_plane.models.work_item import WorkItem
    from execution_plane_client.queries import CommandStateProvider
    from execution_plane_client.transport import Transport

logger = logging.getLogger(__name__)


class DirectCompletionRecorder(CompletionRecorder):
    """Create the callback envelope after commit and send it once over HTTP."""

    def __init__(self, *, transport: Transport, queries: CommandStateProvider) -> None:
        """Use the selected direct transport and the EP-local command projection."""
        self._transport = transport
        self._queries = queries

    async def __call__(self, _session: object, _item: object) -> None:
        """Leave the provider transaction free of HTTP effects."""

    async def after_commit(self, item: object) -> None:
        """Advance local command state, then attempt the reverse completion POST."""
        item = cast("WorkItem", item)
        completion = completion_for_work_item(item)
        if completion is None:
            return
        original, child, result = completion
        self._queries.append_child(original, child, result)
        try:
            await self._transport.send(child)
        except Exception:
            logger.exception("Direct work completion callback failed", extra={"command_id": str(item.request_id)})
