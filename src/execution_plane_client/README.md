# Execution Plane Client

`execution-plane-client` contains the shared command contract for AO and EP:
versioned messages, typed command builders, command catalog and registry,
handler interfaces, peer authentication, the transport protocol, and the FastAPI
router for command receipt and completion callbacks. Its runtime does not need
SQLAlchemy, asyncpg, SQLModel, or an application database.

Every participant that can submit commands mounts `create_router(...)` so it can
receive completion commands at its configured callback URL. HTTP 202 confirms
delivery; callers query command status separately to observe execution results.
There is no separate acknowledgement command.

Install `execution-plane-transport-direct` and construct its
`DirectRuntimeFactory` for immediate authenticated HTTP dispatch. Install
`execution-plane-transport-journal` and construct `JournalRuntimeFactory` when
durable queued dispatch, retry, recovery, and transaction-aware submission are
required. The Journal distribution depends on the direct transport, which in
turn depends on this client. The client never imports either transport package.

Build the client wheel with `uv build src/execution_plane_client` from the EP
checkout. AO and EP register their own application-specific handlers and query
providers while sharing these protocol definitions and receiver routes.
