# Shared Execution Plane client and bilateral command integration

## Status and scope

The 12 October [single-journal correction](2026-10-12-single-command-journal-design.md)
supersedes the original command-projection split. The journal is the sole durable
source of command intent and status in each participant.

Approved by the user on 9 October 2026. This makes the architectural direction in
Syntara's `2026-10-08-durable-ep-command-handshake-design.md` concrete for the
separate EP repository and the AO application. It records the user's subsequent
scope decisions; it is not an implementation-completion report.

Deliver an independently installable `execution-plane-client` distribution,
importable as `execution_plane_client`, whose source is under
`src/execution_plane_client` in this repository. Both `execution-plane` and AO
depend on it. The dependency is one-way: the shared library imports neither EP
nor AO. It must be usable by another participant with its own database,
credentials, handlers, and application lifecycle.

Update both repositories. There are no production consumers and no requirement
to preserve the previous HTTP protocol. Commands are the only external way to
request EP actions. Keep resource queries for observation and recovery. No
client service or client container is introduced; EP's Containerfile and compose
configuration continue to build one EP service image.

The following EP areas receive only narrow integration changes: `cluster/`,
`execution_target/`, `execution_target_reconciler/`, `models/`, `node_protocol/`,
`worker_manager/`, `drain_monitor.py`, `store_base.py`, `store_errors.py`,
`work_store.py`, and `worker.py`. The API and surrounding orchestration may be
refactored. Command service wiring replaces `event_delivery.py`. Provider dispatch,
claim fencing, cancellation confirmation, draining, and resource cleanup retain
their existing semantics.

## Architecture and selected approach

Use the shared mediator with thin participant adapters and explicit transaction
integration. EP keeps its work and resource stores. AO keeps its workflow
correlation and Temporal task-token state. Transport durability belongs to the
shared journal in each participant's own database.

A bridge that later converts existing completion events into journal records
would also preserve an outbox recovery path, but would introduce a second
delivery queue and an additional reconciliation boundary. Prefer a narrow
completion hook that appends the journal command in the result transaction.
Rewriting EP's worker and resource models into a new generic execution engine is
outside the requested scope.

```text
AO domain transaction              EP domain transaction
  activity/integration state        command/work/resource state
  + outbound journal                + inbound/outbound journal
          |                                  |
          +---- shared mediator protocol ----+
                 POST /v1/commands

execution_plane_client: wire contracts, catalog, journal, transport,
                        receipts, delivery, reconciliation, query contracts
execution_plane:        command adapters, provider state, worker execution
syntara:                authorization, integration state, Temporal delivery
```

The client package supplies public protocol/data models, typed command builders,
`MediatorClient`, `MediatorService`, handler/capability registries, journal
metadata and persistence operations, transport/security interfaces, workers,
and a mountable FastAPI router. Keep wire models independent of EP ORM models.
Participant code supplies resource readers and domain handlers. Shared journal
queries supply command status and resource references.

The client accepts caller-provided async SQLAlchemy/SQLModel sessions and
session factories. `submit(command, session)` validates and appends locally;
it neither commits nor performs HTTP. The package does not read EP/AO
environment variables or initialize application connections at import time.

## Command catalog

All definitions are versioned in the shared package. Convenience methods build
the same envelopes as generic submission. No convenience method bypasses the
journal with a direct resource mutation request.

| Command | Meaning |
| --- | --- |
| `work.execute` | Submit the existing versioned node invocation, image, output configuration, and execution policy. |
| `cluster.register` | Record a cluster definition without creating a target. The resource remains `REGISTERING` until its default target is usable. |
| `execution_target.register` | Register a target against an existing cluster. Registering its usable default permits the cluster to become active. |
| `cluster.provision` | Create a cluster and its default target together using the existing provisioning behavior. |
| `cluster.update` / `cluster.delete` | Change desired cluster configuration or request its existing drain/delete lifecycle. |
| `execution_target.update` / `execution_target.delete` | Update or drain a target within the existing registry's restrictions. |
| `command.cancel` | Durably request cancellation of another command, subject to its declared cancellation capability. |
| `<action>.completed` | Report the action's result, failure, cancellation, supersession, or explicitly unknown outcome as a child command. |
| `command.accepted` | Confirm durable persistence of a particular message; this is a protocol control command. |

Preserve protected-default-target rules. A cluster without a usable default is
not eligible for work. Registration/provisioning completion describes the
resource operation performed; a registered cluster can legitimately still be
waiting for its separately registered target. Read responses expose that state.

Desired-state resource commands carry a stable resource key and monotonically
increasing desired revision. `cluster.provision` supports create-or-converge
semantics for AO integration synchronization. A later provision revision can
update or reactivate the same logical integration resource. Deletion is a
separate revisioned command. Stale revisions cannot overwrite newer state;
identical revision/content is idempotent and changed content at the same
revision is a conflict. Supersession is an explicit result, not a false success.
Serialize competing mutations for the same resource and retain revision
tombstones after physical resource deletion.

Cancellation is submitted through `POST /v1/commands` like every other action.
There is no separate HTTP mutation bypass. Cancellation that arrives before
its target leaves durable intent so a late original command cannot start work.
For work already running, retain the existing cooperative/forced-stop rules;
the cancellation request being accepted does not mean execution has stopped.
A terminal result wins the existing completion/cancellation race. Resource
deletion uses drain commands, not an unsafe generic undo operation.

## Wire protocol and acknowledgement rules

Persist a stable logical command ID, message ID, parent ID when applicable,
command type/revision, sequence, immutable routing, payload, and occurrence
time. Bind source, destination, initiating client, and project/grant context to
authenticated identity. Retries reuse the persisted envelope exactly; fresh
short-lived authentication assertions do not change its identity.

Deduplicate both message identity and scoped command identity. Compare a
canonical hash covering the complete immutable envelope, including routing,
execution policy, and parent/sequence metadata. Reject changed identity reuse
with HTTP 409. Scope command IDs and resource ownership to the authenticated
initiating client; queries and acknowledgements cannot cross that boundary.

On receipt, validate authentication, authorization, destination, catalog schema,
and identity before accepting the command. Commit the inbound journal record, indexed command intent, and outbound
acceptance together before
returning HTTP 2xx. This resolves the earlier diagram that returned 202 before
persistence. Do not wait for a callback HTTP request or provider execution
inside this transaction.

For ordinary commands, HTTP 2xx confirms transport receipt; the sender waits for
the separate `command.accepted` identifying its exact `message_id`. Acceptance
itself is durably retried until its receiver returns 2xx after persistence. Do
not generate acceptance of acceptance. Duplicates re-use/re-enable the same
acceptance identity as necessary, without creating another domain operation.

An acceptance may race the sender's HTTP response. Sender state updates must
not regress an already acknowledged journal row to sent/retrying. A sent
command with no acceptance remains eligible for retry and reconciliation after
its acceptance deadline. An acknowledgement from the wrong peer, for the
wrong command, or with the wrong scope is rejected.

Completion is a separate command with its own stable IDs and original
`parent_command_id`. Its `target_url` is the destination formerly called the
completion callback URL; its `callback_url` is the sender's own command ingress
for acceptance. Normal submission reverses these roles. Completions are
acknowledged on durable receipt, independently of subsequent Temporal handling.
Optional handled notifications must be explicitly registered capabilities;
neither participant requires them in the initial integration.

Progress notifications are not advertised in this version. Terminal results
and `outcome_unknown` are supported. Transport sequence remains an immutable
message-identity component; use a distinct per-parent child sequence for child
ordering so it is not confused with the child command's own transport sequence.

## Persistence and recovery

Each participant owns one local `exchange_journal`, with inbound and outbound
views. The shared package owns its versioned schema definition and migration
integration. Each application applies it through its own Alembic chain; there
are no cross-database joins or shared runtime credentials. Schema/metadata
configuration must allow AO's default schema and EP's `execution_plane` schema
without importing the other application's models or mutating global metadata.

Rows contain the identities, authenticated ownership/scope, direction,
type/revision, sequence/parent sequence, encrypted immutable envelope and hash,
target/callback routing, delivery and handling state, retry timestamps/counts,
lease owner/generation/expiry, safe diagnostics, reconciliation indicators, and
creation/receipt/acceptance/handling timestamps. Enforce unique identities and
indexes for due delivery, due handling, parent queries, and scoped command
lookup. Do not store expiring bearer tokens as retry credentials.

The shared journal also owns command status, resource references, indexed resource
intent and observed revisions, deletion markers, and cancellation targets. A
unique current resource reservation points to a real accepted journal entry.
Existing `WorkItem`, cluster, and target records retain provider state. No
separate command, resource-revision, or cancellation-intent tables remain.

Delivery and handling use independent leases with fencing. Claim with row
locking and `SKIP LOCKED`; perform network/provider calls after committing the
claim. Stale workers cannot overwrite a newer generation. Use bounded
exponential backoff with jitter for retryable failures, explicit rejection for
permanent protocol failures, and reconciliation for ambiguous side effects.
Distinguish delivery retries from provider execution retries.

Required local atomicity:

- AO private journal context or integration change plus outbound command.
- Inbound journal plus acceptance and command/adaptor state that establishes
  durable local responsibility.
- EP terminal/unknown work state plus its completion command.
- AO completion receipt plus durable local-handler responsibility.

No HTTP or Temporal RPC runs inside these database transactions. A handler
with external effects records its in-progress state first, uses an idempotency
key where supported, and reconciles uncertainty rather than replaying blindly.
The existing node provider cannot recover all lost results; preserve explicit
unknown outcomes and never automatically rerun uncertain work.

Use polling for correctness and crash recovery. Existing EP work-item
`LISTEN/NOTIFY` may remain; new journal delivery does not depend on notification
delivery. No automatic journal expiry or archival is introduced. Retaining
deduplication identities is required until a separate retention policy exists.

## EP integration and minimal-change boundaries

Replace `api/main.py`'s mutation handlers with composition of the shared command
router, authentication, participant configuration, and EP command adapters.
Resource GET routes use shared response contracts and EP readers.

Place EP domain adapters and service wiring outside the protected existing
directories where practical. Shared journal code owns revision tracking and
command lifecycle state. Reuse public
registry/store operations. Add explicit opt-in support for a caller-owned
transaction only where required; existing standalone store calls retain their
current ownership semantics. Do not hide commits with session proxies or
bypass stores by changing their ORM resources directly in HTTP handlers.

Add a narrow completion recorder used by every existing terminal and
reconciliation transition in `WorkStore`, including cancellation and stale
claim recovery. It appends the completion command before the result transaction
commits. `WorkItem` retains the provider result; the journal is the sole durable
completion responsibility. Remove `CompletionEvent`, its table, and its retry
methods, together with the obsolete `ClusterBinding` model and reconciler.

Use `commands/service.py` for the shared delivery and reconciliation lifecycle.
Delete `event_delivery.py`. Worker changes configure the completion hook
and supervise the shared workers. Keep allocation, gRPC, heartbeat fencing,
NetworkPolicy, drain monitoring, and cleanup behavior intact.

## AO integration

Use `commands/activities.py` and `commands/integrations.py` for AO domain adapters.
Remove the obsolete `bridge.py` and `integration_sync.py` module paths. Token
refresh uses the shared journal API to invalidate older handling attempts and
retry the latest outcome in the same transaction as the private context update.

Replace the private HTTP protocol in `syntara/execution_plane/client.py` with
configuration and typed convenience adapters over the shared package. Retain
AO's authorization boundary before enqueuing commands or issuing queries.

In `activities/ep/ep_dispatch_activity.py`, persist the stable work command and
encrypted Temporal correlation together, then enter asynchronous completion.
HTTP delivery retries belong to the shared worker. A Temporal retry reuses the
original command/payload while refreshing only the AO-local activity token and
attempt generation. Persisted acceptance and completion may arrive in either
order. Handle completion arriving during the activity's async handoff without
losing it or delivering against an obsolete token.

Store Temporal identity, attempt and task token in encrypted local context on
the outbound journal record; remove the activity-binding table and its separate
status lifecycle. Journal context generations fence stale effects; prepared
effects and handling state own completion and uncertainty. Replace the separate
completion inbox and cancellation delivery columns/loops with journal state
and registered handlers. Temporal completion/failure runs outside transactions;
successful delivery is recorded with the expected activity attempt. Ambiguous
Temporal RPC outcomes remain reconciliation-required rather than being marked
successful or indiscriminately retried. Permanent dispatch rejection must also
reach the waiting activity through a durable local failure path.

Replace the integration-sync outbox with commands appended in
`IntegrationService`'s transaction. Resolve and freeze each revision's payload,
including credential material, before enqueueing; persist it encrypted and
never rebuild that revision from subsequently changed integration state during
delivery. Use `cluster.provision` for upsert/reactivation and `cluster.delete`
for removal. Update user-visible observed state only when the returned resource
revision matches the integration's current desired revision.

Mount the same receipt protocol at AO's configured `/v1/commands` endpoint and
run the shared workers from AO's existing worker lifecycle. Replace the legacy
`/events` ingress. Preserve certificate authentication and normal middleware;
do not move ingress under a prefix that bypasses those controls. AO's user-facing
read facade remains scoped through its existing authorization machinery.

## Queries and capabilities

Both participants expose authenticated command status and child queries through
the shared API. EP exposes resource reads for work items, clusters, and targets,
including single-resource and bounded list operations. Status includes the
resource reference, latest child sequence, safe delivery/handling diagnostics,
timestamps, and explicit unknown/reconciliation state. Never return credentials,
Temporal tokens, or opaque encrypted private context.

`GET /v1/capabilities` advertises supported command revisions and registered
receipt/completion handlers. Validate local definitions on enqueue and peer
capabilities before network delivery; do not perform capability HTTP requests
inside caller transactions. Cache capabilities with refresh. An unavailable
peer causes a durable retry; unsupported commands cause an explicit rejection,
including delivery of failure to any waiting AO actor.

Reconciliation uses command status/children first and resource reads only when
the domain adapter requires detail. Recover a missing completion by its
original stable identity; do not synthesize a conflicting second completion.

## Security and configuration

Register peer identities and exact command/query destinations in participant
configuration. Reject arbitrary caller destinations, userinfo, fragments, and
redirects. Require HTTPS and trusted mTLS for service transport. Configured
internal service addresses are intentional; unregistered destinations and
unapproved network resolution are rejected. Test transports are explicitly
injected rather than enabled by a production authentication bypass.

Use ES256 short-lived service assertions with issuer, audience, client/project
grant, issue/expiry times, `kid`, command/message identity, and a SHA-256 digest
of the complete canonical envelope. Verify the digest before persistence so
payload, route, sequence, and identity tampering fails authentication. Sign a
fresh assertion for each attempt without changing the persisted envelope.
Accept overlapping configured public keys by `kid` for rotation. AO can adapt
its existing key manager; EP gains its own signing identity and verification
configuration. Query assertions bind the authorized peer and scope.

Encrypt private command payloads, results, credentials, and actor context with
participant-owned keys. The library accepts encryption/key-provider interfaces;
it must not depend on EP's environment-specific encryption classes or AO's
secret service. Redact errors and logs. Correlate command/message IDs, sequence,
client, and resource references. Expose acceptance/handling latency, retry and
conflict counts, queue age, and reconciliation/unknown-outcome observations
through a configurable observer/metrics integration.

## Packaging, builds, and migrations

Use separate project metadata and wheel contents for the client and EP. EP's
root project remains the service project and depends on the local client
project. The client wheel must import and run independently without Kubernetes,
gRPC, Temporal, or application configuration. Lock compatible dependency
versions across both applications.

AO obtains the package from the side-by-side EP checkout. From AO's `backend`
directory the source is `../../syntara-execution-plane/src/execution_plane_client`.
Local/container build wiring supplies that client source explicitly, preserving
the relative project layout during dependency resolution and installing the
client non-editably in runtime images. Do not install the EP service distribution
into AO or rely on developer-specific absolute paths. CI/build preparation must
also supply the sibling EP checkout containing the client package.
Published/pinned package acquisition can replace the local source without
changing imports or protocol. Update AO's lock and generated requirements as
well as EP's lock. Preserve the supported hermetic build path.

Each repository adds a forward Alembic revision and includes shared journal
metadata in its migration environment. EP transfers and removes the three command
projection tables. AO removes its activity binding and replaces redundant
transport tables. No compatibility HTTP workers remain. Absence of production
consumers does not authorize silently discarding development records: document
the cutover and refuse destructive migration when unresolved legacy work would
be lost. Do not reset a developer database automatically.

## Validation and acceptance criteria

Use real PostgreSQL for journal atomicity, uniqueness races, locking, fencing,
schema integration, and migrations; mock HTTP/provider effects when testing
specific fault boundaries. Exercise the same shared implementation configured
as two participants in separate databases/schemas.

Required cases include rollback without HTTP, commit-before-2xx, duplicate and
conflicting command/message reuse, acceptance response races, lost HTTP
responses, missing callbacks, restart after claims, stale lease writes,
completion before acceptance, independent cluster/target registration,
provisioning, revision reordering, deletion tombstones, cancellation before
submission, unknown provider outcomes, and isolated client/project queries.

AO validation includes activity-token refresh, early completion handoff,
ambiguous Temporal delivery, integration payload immutability, stale observed
revision protection, and the replacement of the old test double with a fake
peer that exercises the new protocol. Preserve existing provider/registry
regressions and the kind dispatch test where infrastructure is available.

Build and inspect both distributions; verify AO installs only the shared
client. Regenerate EP OpenAPI and AO contracts/generated clients as required
by each repository. Run the required repository checks, including AO's
`make install` before checks, and validate the affected container/compose build
paths. Record unavailable infrastructure and actual check results explicitly.
