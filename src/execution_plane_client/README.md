# Execution Plane Client

Shared command contracts and durable exchange for independently deployed
participants. This distribution is independent of the EP service and AO.
It does not initialize application settings, databases, or transports on import.

Build with `uv build src/execution_plane_client` from the EP checkout.
Install the resulting wheel into each participant, which supplies its own
database sessions, peer credentials, encryption keys, and command handlers.

Participant-private correlation belongs in the journal's encrypted local context.
`get_local_context` and `set_local_context` provide scoped reads and generation-fenced
updates within caller-owned transactions; `list_local_context_entries` pages an
opaque correlation group. Context and prepared effects are never wire payloads
or public query fields. Journal schema version 2 adds the encrypted context,
its generation and a correlation index; each participant owns its migration.

Delivery and handling wake immediately after journal transactions commit, using
PostgreSQL `LISTEN/NOTIFY` in the shared runtime. This covers actions, reverse
completions and acceptance commands equally. Producers keep ownership of their
transactions; HTTP runs after committed claims, outside domain transactions.

`MediatorService.run()` supervises a dedicated asyncpg listener using one
autocommit connection from the participant's pool. Allow pool capacity for that
connection plus concurrent journal transactions; the listener requires a stable
session connection (not a transaction-pooled database proxy). Reconnects and health
probes restore notifications, and startup/reconnect scans recover existing work.
Notifications contain only a work category; the journal remains authoritative.

`ExchangeOptions.poll_seconds` is the fallback scan interval. New work wakes the
workers without waiting for that interval; retries and observations also schedule
their persisted deadlines. `batch_size` bounds concurrent delivery/handling tasks,
and free capacity is replenished as individual tasks finish. Lowering the fallback
interval is not necessary for normal dispatch latency. If no worker capacity is
available, work remains durably queued until capacity becomes available.
