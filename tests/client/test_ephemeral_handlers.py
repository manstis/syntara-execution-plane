"""Transport-neutral handlers receive only their command context and dispatcher."""

from datetime import UTC, datetime


async def test_registered_handler_dispatches_without_journal_context(message):
    from execution_plane_client.catalog import CommandCatalog
    from execution_plane_client.handlers import CommandDispatcher, CommandRegistry, HandlerContext
    from execution_plane_client.protocol import MessageEnvelope
    from execution_plane_client.transport import DispatchReceipt, DispatchState

    class Dispatcher(CommandDispatcher):
        def __init__(self) -> None:
            self.sent = []

        async def send(self, envelope: MessageEnvelope) -> DispatchReceipt:
            self.sent.append(envelope)
            return DispatchReceipt(message_id=envelope.message_id, state=DispatchState.DELIVERED)

    class Handler:
        async def handle(
            self, envelope: MessageEnvelope, context: HandlerContext, _session: object | None = None
        ) -> None:
            await context.dispatcher.send(envelope)

    dispatcher = Dispatcher()
    context = HandlerContext(
        scope=message.scope,
        local_id="ep",
        peer_id="ao",
        dispatcher=dispatcher,
        clock=lambda: datetime.now(UTC),
    )
    registry = CommandRegistry()
    registry.register(CommandCatalog().definition("work.execute", 1), Handler())

    await registry.get("work.execute", 1).handle(message, context)

    assert dispatcher.sent == [message]
