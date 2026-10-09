# Shared Execution Plane Client and AO Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking. Do not start implementation until the user has reviewed this plan and selected an execution method.

**Goal:** Deliver one reusable durable command library, integrate EP with minimal runtime changes, and migrate AO's dispatch, completion, cancellation, and integration synchronization to the same protocol.

**Architecture:** Each participant imports `execution_plane_client`, owns a local journal, and registers domain handlers. EP retains its execution/resource projections; AO retains encrypted Temporal correlation and integration state. All EP actions use commands, while queries provide recovery and observation.

**Tech Stack:** Python `>=3.12,<3.15`, uv, Hatchling, FastAPI, SQLModel/Pydantic v2, SQLAlchemy async sessions, PostgreSQL, Alembic, httpx, PyJWT ES256, AES-GCM, pytest, and existing AO Temporal APIs.

**Spec:** [Approved design](../specs/2026-10-09-execution-plane-client-design.md). The source architectural design is `syntara/docs/superpowers/specs/2026-10-08-durable-ep-command-handshake-design.md` in the sibling checkout. Read both; the approved local design and subsequent user instructions resolve differences.

## Global Constraints

- Distribution `execution-plane-client`; import `execution_plane_client`; source `src/execution_plane_client` in EP. Separate wheel from `execution-plane`.
- The dependency is one-way: the shared library imports neither EP nor AO.
- Commands are the only external way to request EP actions. Keep resource queries for observation and recovery.
- There are no production consumers and no requirement to preserve the previous HTTP protocol.
- No client service or client container is introduced; EP builds one service image.
- Only narrow integration changes in EP `cluster/`, `execution_target/`, `execution_target_reconciler/`, `models/`, `node_protocol/`, `worker_manager/`, `drain_monitor.py`, `store_base.py`, `store_errors.py`, `work_store.py`, and `worker.py`.
- `event_delivery.py` must use the new handshake. Completion `target_url` is the previous callback destination; completion `callback_url` is EP ingress.
- `cluster.register` and `execution_target.register` are independent; `cluster.provision` creates a cluster and its default target.
- `submit(command, session)` neither commits nor performs HTTP. No HTTP or Temporal RPC runs inside domain database transactions.
- Require HTTPS, mTLS, signed envelopes, scoped authorization, and encrypted private data. Never expose credentials or Temporal tokens through generic queries/logs.
- Use polling for correctness and crash recovery. No automatic journal expiry or archival is introduced.
- AO and EP are checked out side by side. AO backend's client source is `../../syntara-execution-plane/src/execution_plane_client`.
- Run AO `make install` before checks/tests; use Alembic for schema changes, and regenerate its lock, requirements, and API contracts.
- Preserve unknown outcomes and claim fencing; never automatically rerun work after an ambiguous Execute boundary.
- Every implementation commit includes `Co-authored-by: OpenAI Codex <noreply@openai.com>` exactly. Stage only files belonging to that task; do not include unrelated user changes.

## Review Focus

These risks are assigned explicit tests below rather than left to a final review:

1. Acceptance arrives before the sender records HTTP success: acknowledged state must never regress (Task 5).
2. Cancellation precedes the original submission, or an old revision arrives after physical deletion: durable intent/tombstones must prevent unwanted work/resource resurrection (Task 8).
3. A Temporal retry replaces its task token while an older completion RPC is in flight: the old attempt must not complete the new binding (Task 12).
4. A caller mutates nested payload data or rotates a management credential after enqueueing: persisted content and retry identity must remain unchanged (Tasks 3 and 13).
5. The client is installed in a clean environment or a non-editable container: it must not import EP/AO settings or depend on a sibling runtime source tree (Tasks 1 and 15).

---

## Repository paths, execution setup, and checks

Paths prefixed `EP/` are relative to this repository. Paths prefixed `AO/` are
relative to the sibling `syntara` repository; run its backend commands from
`AO/backend`. All other source/test paths in a task explicitly identify their
repository. These prefixes are documentation conventions, not shell variables.

At execution time use the worktree skill to create/reuse an isolated *pair* of
checkouts under one parent, named `syntara` and `syntara-execution-plane`, so the
approved path dependency resolves. Preserve the design commit in the EP base.
Record starting branch/commit and dirty files in both repositories. Read AO's
root/backend AGENTS instructions. Request normal tool escalation where Git
metadata, AO writes, dependency downloads, or container execution require it;
do not reset databases, rewrite user changes, or bypass sandbox restrictions.

The current migration heads observed during planning are EP `i5e6f7a8b9c0` and
AO `3d4e5f607182`. Re-read the graph at execution time and append to the actual
heads if intervening work has changed it. Never edit historical revisions.

Tasks are sequential because the protocol, EP, and AO share public interfaces.
Tasks 1–6 produce an independently testable library; 7–10 produce an EP peer;
11–14 migrate AO; 15–16 validate packaging and the full exchange. These are
three dependent deliveries within one approved design, not unrelated features.

Each task follows red → green → focused regression → commit. Commands below
are executed only during implementation. Run broader required checks once at
Task 16 unless a failure or later change warrants an earlier repeat.

EP focused test command: `uv run pytest <the listed test path> -q`.
PostgreSQL tests require a disposable `EP_TEST_DATABASE_URL`; they must run,
not merely skip, before claiming journal or migration correctness.

AO focused tests use the existing wrapper after `make install`:

```bash
./tools/scripts/run-with-testcontainers.sh --label 'EP command tests' -- \
  uv run --no-sync pytest tests/unit/execution_plane tests/integration/execution_plane -q
```

Substitute the exact task test paths for those two directories. At final
validation use the required `make` targets. Do not claim that a missing tool,
skipped integration test, or unavailable cluster passed.

## File and responsibility map

```text
EP/src/execution_plane_client/
  pyproject.toml, README.md, LICENSE, __init__.py, py.typed
  protocol.py              immutable envelope, scope, status and query schemas
  errors.py                typed protocol/persistence/transport exceptions
  catalog.py               definitions, revisions, capabilities, policy
  commands/                work.py, clusters.py, targets.py, control.py
  crypto.py                canonical JSON, encryption and keyed digest helpers
  security.py              assertion signing/verification and peer grants
  destinations.py          registered URLs, network policy, safe resolution
  journal/
    schema.py              isolated versioned metadata/table construction
    records.py             validated journal/claim/state records
    store.py               transactional append, lookup, acceptance
    claims.py              fenced delivery/handling claims and transitions
  handlers.py              transactional/external handler contracts, registry
  client.py                enqueue convenience methods and peer queries
  ingress.py               validate, persist, deduplicate, accept
  transport.py             authenticated bounded HTTP operations
  workers/
    delivery.py            due delivery, retry, capability checks
    handling.py            local transactional/external handling
    reconciliation.py      generic query recovery and adapter hooks
  api.py                   mountable receipt/status/children/capabilities router
  service.py               dependency injection and worker lifetime
  observers.py             safe events and metrics integration contract

EP/src/execution_plane/commands/
  models.py, store.py      command projection, resource revision, cancellation intent
  resources.py             scoped reads and resource reference conversion
  receipts.py              transactional EP acceptance hook
  work.py                  work adapter and completion recorder
  control.py               cancellation capability and durable intent adapter
  clusters.py, targets.py  resource command adapters using existing stores
  reconciliation.py       state observation, drain/delete completion
  runtime.py              settings, handlers, security and mediator composition

AO/backend/src/syntara/execution_plane/
  client.py, runtime.py    shared client/service configured for AO
  models.py                Temporal domain binding, not a second transport inbox
  bridge.py                activity enqueue and workflow cancellation entry points
  handlers.py              journal-to-Temporal external handler and rejection handler
  integration_sync.py      frozen revision construction and result application
  router.py                shared receipt/query router with AO auth adapters
```

Keep `api.py`, `service.py`, and adapters focused. Split a file further when
implementation would mix unrelated concerns; preserve the public interfaces
below and update this map if a filename changes.

## Cleanup correction (2026-10-09)

The initial completion of this plan retained obsolete EP persistence and module
names. The user's follow-up supersedes the retention guidance in Tasks 9–10:
remove `CompletionEvent`, `ClusterBinding`, their tables and legacy methods;
replace `event_delivery.py` with `commands/service.py`; move AO domain adapters
from `bridge.py` and `integration_sync.py` to `commands/activities.py` and
`commands/integrations.py`. Work outcomes and outbound completion commands commit
atomically without an intermediary event row. Shared journal methods own handling
recovery when Temporal correlation changes. Remove `ExecutionPlaneActivityBinding`
and store only Temporal identity, token and attempt in encrypted journal-private
context; journal effects and handling records replace its status lifecycle.
Historical migrations remain reproducible.

## Task 1: Independently buildable client package and protocol values

**Files:** Create `EP/src/execution_plane_client/{pyproject.toml,README.md,LICENSE,__init__.py,py.typed,protocol.py,errors.py}`; modify `EP/pyproject.toml`, `EP/uv.lock`; create `EP/tests/client/test_protocol.py`, `EP/tests/client/test_distribution.py`, `EP/tests/client/conftest.py`.

**Interfaces produced:** `CommandScope(client_id: str, project_id: UUID | None, all_projects: bool)`; `Command(command_id: UUID, parent_command_id: UUID | None, command_type: str, command_revision: int, target_url: str, callback_url: str, payload: dict[str, object], occurred_at: datetime, parent_sequence: int | None, acknowledges_message_id: UUID | None, execution_policy: ExecutionPolicy | None)`; `MessageEnvelope(message_id: UUID, message_type: str, command_id: UUID, sequence: int, source_id: str, destination_id: str, scope: CommandScope, command: Command)`.

Define `ExecutionPolicy(timeout_seconds: int, max_attempts: int | None)`,
`CommandStatus`, `CommandRead`, `ChildCommandRead`, `ChildPage`,
`CapabilitiesRead`, and `ResourceReference(kind: str, resource_id: UUID,
resource_key: UUID | None, revision: int | None)` in `protocol.py`. Explicitly
enumerate accepted/running/succeeded/failed/cancel_requested/cancelled/
superseded/outcome_unknown status values. Safe reads include no request payload.
Define `ProtocolConflict`, `UnsupportedCommand`, `UnauthorizedPeer`,
`UnsafeDestination`, `RetryableDelivery`, `PermanentRejection`, `StaleClaim`,
`OutcomeUnknown`, and `CommandNotFound` in `errors.py`.

- [x] Write protocol validation tests: outer/inner command ID mismatch, naive time, nonpositive revisions, forbidden unknown fields, non-JSON/nonfinite payload values, invalid scope, payload exceeding 512 KiB. Provide a `message` fixture with fixed UUIDs/time and an explicit test-only HTTPS peer URL.

```python
def test_envelope_rejects_mismatched_command_id(message):
    value = message.model_dump(mode="json")
    value["command_id"] = "00000000-0000-0000-0000-000000000099"
    with pytest.raises(ValidationError, match="command_id"):
        MessageEnvelope.model_validate(value)
```

- [x] Run `uv run pytest tests/client/test_protocol.py -q`; verify the missing package/contract fails first.
- [x] Add the package and strict SQLModel/Pydantic wire values. Preserve aware UTC instants on serialization. Snapshot encoded content at persistence time; model freezing alone does not make nested dictionaries immutable.
- [x] Declare EP's normal dependency and a local path source. Use separate Hatch wheel selection so the service wheel cannot accidentally include the client or vice versa:

```toml
# EP root: append dependency "execution-plane-client>=0.1.0,<0.2.0".
[tool.uv.sources]
execution-plane-client = { path = "src/execution_plane_client", editable = true }

# Inside the client project's pyproject.toml:
[tool.hatch.build.targets.wheel]
include = ["/*.py", "/py.typed", "/commands/**", "/journal/**", "/workers/**"]
[tool.hatch.build.targets.wheel.sources]
"" = "execution_plane_client"
[tool.hatch.build]
dev-mode-exact = true
```

The client declares only its own SQLModel/SQLAlchemy, FastAPI, httpx,
cryptography, and PyJWT dependencies. Its sdist includes project metadata,
README/LICENSE, and the listed source; no dependency on files outside the
client project. Regenerate the lock with uv, never by hand.

- [x] Build both artifacts with `uv build` and `uv build src/execution_plane_client`; test the client wheel's members/dependency metadata and install it in a temporary clean venv. Import with EP/AO environment variables absent. Assert no `execution_plane`, `syntara`, `kubernetes`, `grpc`, or `temporalio` runtime dependency and rebuild the wheel from its sdist.
- [x] Re-run the two tests and commit `feat: add independent execution plane client package` with the required trailer.

## Task 2: Typed command catalog and handler contracts

**Files:** Create `EP/src/execution_plane_client/catalog.py`, `handlers.py`, `commands/{__init__,work,clusters,targets,control}.py`; extend `protocol.py`; create `EP/tests/client/test_catalog.py`.

**Interfaces produced:** `CommandCatalog.definition(command_type: str, revision: int) -> CommandDefinition`; `validate(message: MessageEnvelope) -> None`; `CapabilityRegistry.advertise() -> CapabilitiesRead`; `CommandRegistry.register(definition: CommandDefinition, handler: TransactionalHandler | ExternalHandler) -> None`.

`CommandDefinition` contains request/completion schema, required scope,
allowed source/destination roles, child policy, and cancellation/reconciliation
policy. `HandlerContext` contains authenticated scope, local/peer IDs, journal
store, and injected clock; it has no ambient application globals.

Transactional handlers expose `handle(message, context, session) -> HandlingResult`.
`HandlingResult` has `status: CommandStatus | None`, `resource: ResourceReference | None`,
`result: dict[str, object]`, and `completion_required: bool`.
External handlers expose `prepare(message, context, session) -> PreparedEffect`,
`execute(effect) -> EffectResult`, `finish(effect, result, context, session) -> HandlingResult`,
and `reconcile(effect) -> EffectResult | None`. `PreparedEffect` has entry/command
IDs, expected domain generation, and private in-memory context; `EffectResult`
contains succeeded/failed/unknown and safe result. Persist preparation before
`execute`; finish only with the claim/domain generation that prepared it.

- [x] Write a parametrized catalog test for all nine action types, their completed types, and acceptance. Reject unknown revisions, unsupported backend/placement combinations, forbidden default-target mutations, invalid invocation limits, and completion without a parent.

```python
@pytest.mark.parametrize("kind", ["work.execute", "cluster.register", "cluster.provision",
    "cluster.update", "cluster.delete", "execution_target.register",
    "execution_target.update", "execution_target.delete", "command.cancel"])
def test_every_action_has_versioned_completion(catalog, kind):
    definition = catalog.definition(kind, 1)
    assert definition.completion_type == f"{kind}.completed"
    assert catalog.definition(definition.completion_type, 1).may_create_children is False
```

- [x] Run `uv run pytest tests/client/test_catalog.py -q` and observe failure.
- [x] Implement typed payloads: work invocation/image/output; cluster definition/credential/CA/type/projects/labels; target cluster reference/backend/placement/default flag; provision including default target; resource updates/deletes with `resource_key` and `desired_revision`; cancellation with target command ID. Use `Literal` discriminators and existing EP validation semantics without importing EP models.
- [x] Make `command.accepted` a control definition that acknowledges an exact message and never creates acceptance children. Reject duplicate handler registration and advertise only installed revisions. Define typed builders `work_execute`, `cluster_register`, `cluster_provision`, `cluster_update`, `cluster_delete`, `target_register`, `target_update`, `target_delete`, and `cancel_command`, each accepting explicit identity/routing/scope and returning `MessageEnvelope`.
- [x] Run the catalog tests and commit `feat: define versioned command catalog and handler interfaces` with the required trailer.

## Task 3: Encrypted journal schema and transactional append/deduplication

**Files:** Create `EP/src/execution_plane_client/crypto.py`, `journal/{__init__,schema,records,store}.py`; create `EP/tests/client/test_crypto.py`, `EP/tests/integration/postgres/test_exchange_journal.py`, `EP/tests/integration/postgres/exchange_helpers.py`.

**Interfaces produced:** `canonical_bytes(message: MessageEnvelope) -> bytes`;
`PayloadCipher.encrypt(data: bytes, *, associated_data: bytes) -> str` and
`decrypt(value: str, *, associated_data: bytes) -> bytes`; an AES-GCM
implementation with injected key/key ID; `JournalSchema(metadata: MetaData,
table: Table, version: int)` from `build_journal_schema(schema: str | None)`;
`ExchangeJournalStore(schema: JournalSchema, cipher: PayloadCipher)`.

Store methods all take an explicit `session: AsyncSession`:
`append_outbound(session, message) -> JournalEntry`,
`append_or_confirm_inbound(session, message) -> tuple[JournalEntry, bool]`,
`get(session, scope, command_id, direction) -> JournalEntry | None`,
`list_children(session, scope, command_id, after, limit) -> ChildPage`,
`acknowledge(session, acceptance, identity) -> JournalEntry`, and
`load_message(session, entry_id) -> MessageEnvelope`.
`JournalEntry` is a validated SQLModel record, not an application ORM model.

- [x] Implement `journal_db` fixture in `exchange_helpers.py`: a new random schema in the disposable PostgreSQL database, schema metadata created for that test, session factory, cipher with a fixed test key, and cleanup confined to its own random schema. Expose `.store`, `.sessions`, and `.read_rows()`; never touch configured runtime databases.
- [x] Write and run failing rollback, concurrent duplicate, conflicting identity/routing, encryption-at-rest, and nested-mutation tests:

```python
async def test_append_does_not_commit_or_keep_payload_alias(journal_db, message):
    expected = canonical_bytes(message)
    async with journal_db.sessions() as session:
        row = await journal_db.store.append_outbound(session, message)
        message.command.payload["added_after_enqueue"] = True
        saved = await journal_db.store.load_message(session, row.entry_id)
        assert canonical_bytes(saved) == expected
        await session.rollback()
    assert await journal_db.read_rows() == []
```

- [x] Implement private metadata and journal columns from the spec. Use separate delivery/handling attempts, due times, owner/generation/expiry; keep rejection and reconciliation diagnostics distinct. Store canonical plaintext SHA-256 plus encrypted bytes bound by authenticated associated data to entry/message/scope identity. Never hash ciphertext for deduplication.
- [x] Add unique message identity and scoped logical identity constraints, including source/destination and direction. New messages cannot rebind existing command IDs, routes, policy, sequence, or parent. Use PostgreSQL conflict-safe insertion or savepoints; never roll back the caller's unrelated transaction when confirming a duplicate.

```python
encoded = canonical_bytes(message)
digest = hashlib.sha256(encoded).hexdigest()
# The INSERT has immutable indexed identity plus ciphertext derived from encoded.
# ON CONFLICT DO NOTHING is followed by an identity/hash comparison in this transaction.
```

- [x] Index due delivery/handling and child/scoped lookup. Ensure importing the client does not add journal tables to global `SQLModel.metadata`; build two schemas in one process to prove isolation.
- [x] Run crypto and real-PostgreSQL journal tests; commit `feat: persist encrypted bilateral command journal` with the required trailer.

## Task 4: Peer authentication, transport destinations, and fresh assertions

**Files:** Create `EP/src/execution_plane_client/{security,destinations,transport,observers}.py`; create `EP/tests/client/test_security.py`, `test_destinations.py`, `test_transport.py`.

**Interfaces produced:** `PeerConfig` (peer ID, issuer/audience, exact command URL,
query base URL, grants, trusted public-key map, permitted network ranges);
`AuthenticatedIdentity(peer_id: str, scope: CommandScope, scopes: frozenset[str])`;
`AssertionSigner.sign(message) -> str`, `AssertionVerifier.verify(token, message) -> AuthenticatedIdentity`;
`DestinationPolicy.validate(url, peer_id) -> RegisteredDestination`;
`HttpCommandTransport.send(message) -> TransportReceipt`,
`get_command(peer_id, scope, command_id) -> CommandRead`,
`get_children(peer_id, scope, command_id, after) -> ChildPage`,
`get_capabilities(peer_id) -> CapabilitiesRead`, `aclose() -> None`.
`ExchangeObserver.record(event: str, fields: Mapping[str, str | int | float | bool]) -> None`
receives only explicitly safe fields and includes a no-op default.

- [x] Write/run failing tests for tampered payload/routes/sequence, wrong issuer/audience/kid/client/project, expired or future assertions, callback peer mismatch, unregistered URL, redirects, mixed DNS resolution, and leaked Authorization/result data in diagnostics.

```python
def test_signature_covers_routing(message, signer, verifier):
    token = signer.sign(message)
    changed = message.model_copy(update={"command": message.command.model_copy(
        update={"callback_url": "https://unregistered.test/v1/commands"})})
    with pytest.raises(UnauthorizedPeer):
        verifier.verify(token, changed)
```

- [x] Sign ES256 assertions with `kid`, short expiry, issue time, issuer/audience, scoped grants, command/message IDs, and canonical-envelope digest. Enforce a maximum assertion lifetime and bounded clock skew. Permit explicitly configured old/new verification keys; mint new assertions for every retry. Query assertions carry the same identity/grant checks without a command body.
- [x] Require explicit HTTPS peer routes; disable redirect following and environment-proxy inheritance. Reject userinfo/fragments and query strings on command ingress. Resolve/check allowed addresses and ensure the connection uses an approved resolution while preserving TLS hostname/SNI verification; do not validate DNS and then let the client independently resolve a different address. Allow configured private service networks, not arbitrary internal URLs.
- [x] Build bounded httpx pools with trusted CA and required client identity; impose request/response size and timeout limits. Parse duplicate JSON keys as invalid. Map timeout/429/5xx to retry; malformed success or ambiguous network errors remain uncertain; permanent authorized protocol 4xx becomes rejection. Scrub arbitrary peer response text from saved diagnostics.
- [x] Verify observer events for rejected identity, retry, duplicate/conflict and latency without credentials; run tests and commit `feat: authenticate and constrain command transport` with the required trailer.

## Task 5: Durable receipt, acceptance, and fenced delivery

**Files:** Create `EP/src/execution_plane_client/ingress.py`, `journal/claims.py`, `workers/{__init__,delivery}.py`; extend `journal/store.py`; create `EP/tests/integration/postgres/test_exchange_handshake.py`, `test_exchange_claims.py`.

**Interfaces produced:** `CommandReceiptHandler.receive(message, identity, session) -> ReceiptResult(entry: JournalEntry, duplicate: bool)` with a configured transactional receipt hook; `JournalClaim(entry_id, owner_id, generation, message)`; `claim_due_delivery(session, owner_id, now, limit) -> list[JournalClaim]`; `record_delivery(session, claim, outcome, now) -> bool`; `JournalDeliveryWorker.run_once() -> int`, `run() -> None`.

Delivery outcomes distinguish transport receipt, acknowledged control,
retryable error, permanent rejection, and acceptance timeout. A built-in
acceptance ID is UUID5 from the original message ID in a fixed protocol
namespace. Its parent is the original command, and its payload/acknowledges
field identifies the exact original message. Parent outcome sequence excludes
acceptance-control messages.

- [x] Extend `journal_db` with controllable clock and transport mock. Write/run failing tests for rollback of receipt/acceptance/hook together, duplicate ACK reactivation, no ACK-of-ACK, loss after receiver commit, expiry/reclaim fencing, and this response race:

```python
async def test_late_transport_response_cannot_undo_acceptance(delivery_case):
    claim = await delivery_case.claim()
    await delivery_case.persist_acceptance_for(claim)
    await delivery_case.record_http_202(claim)
    assert (await delivery_case.read(claim)).delivery_state == "acknowledged"
```

Define `delivery_case` in `exchange_helpers.py` with those four methods using
real sessions/store operations. The mock network only controls ordering;
state assertions read committed PostgreSQL rows.

- [x] Persist inbound record, receipt-hook responsibility, and acceptance in one caller transaction. For control acceptance, validate expected peer/scope/referenced command before changing the outbound row; store the immutable inbound acceptance and return without creating a new acceptance.
- [x] Claim queued/retrying rows and sent rows whose acceptance deadline elapsed, using `FOR UPDATE SKIP LOCKED`. Commit claims before HTTP. Make every post-request update conditional on lease owner/generation and non-acknowledged state:

```python
update(table).where(
    table.c.entry_id == claim.entry_id,
    table.c.delivery_owner == claim.owner_id,
    table.c.delivery_generation == claim.generation,
    table.c.delivery_state != "acknowledged",
)
```

- [x] Check cached peer capabilities outside transactions before send; cache expiry triggers refresh. Unsupported revisions reject durably; temporary capability outages retain queued intent. Check the sender has the completion handler it requires. Define transport timeout/acceptance deadline separately; apply capped exponential retry with injected jitter and clock.
- [x] Emit safe observer measurements and a durable local rejection-handling obligation. Receipt of an acceptance may reopen that obligation for rejection reconciliation but must not lose an already received completion. Run both PostgreSQL files and commit `feat: implement durable acceptance and fenced delivery` with the required trailer.

## Task 6: Local handling, reconciliation, client methods, and shared router

**Files:** Create `EP/src/execution_plane_client/{client,api,service}.py`, `workers/{handling,reconciliation}.py`; extend handlers, claims, observers; create `EP/tests/client/test_api.py`, `EP/tests/integration/postgres/test_exchange_workers.py`, `test_exchange_pair.py`.

**Interfaces produced:** `MediatorClient.submit(message, session) -> JournalEntry`,
typed enqueue methods for Task 2 builders, `get(scope, command_id) -> CommandRead`,
`list_child_commands(scope, command_id, after=0, limit=50) -> ChildPage`,
`cancel(message, session) -> JournalEntry` (requires a `command.cancel` envelope);
`JournalHandlingWorker.run_once() -> int`; `ReconciliationWorker.run_once() -> int`;
`MediatorService(client, ingress, delivery, handling, reconciliation, queries, capabilities)`
with `run()`, `close()`, and
`create_router(prefix="/v1", *, router: APIRouter | None = None) -> APIRouter`.
When supplied, the router instance carries participant dependencies and route
metadata; the shared package never imports the participant's router subclass.

`CommandQueryProvider.get(session, scope, command_id) -> CommandRead` and
`children(session, scope, command_id, after, limit) -> ChildPage` are injected.
The journal supplies the default participant view; EP overlays its domain
projection. Recovered children include the original envelope only through a
peer-authorized recovery representation, with private request context excluded.
Queries never expose another recipient's command payload or raw credentials.

- [x] Build `PeerPair` in `exchange_helpers.py`: independent schemas/session factories, injected HTTPS-name ASGI transports/signers, `ao`, `ep`, `pump(rounds: int)`, `restart(peer: str)`, and `drop_next_response(peer: str)`. Each `PeerEndpoint` exposes `sessions: async_sessionmaker[AsyncSession]`, `service: MediatorService`, and `delivery_state(command_id: UUID) -> str` reading committed rows. The pair's `effects: int` counts completed fake external calls. The fixture configures real shared services with a test transactional handler and fake external effect counter.
- [x] Write/run failing two-peer tests: HTTP receipt before acceptance, completion before acceptance, restart after handler claim, malformed response, unsupported peer capability, lost completion recovery, no duplicate external effect, and external effect uncertainty.

```python
async def test_lost_response_retries_same_effect(peer_pair, message):
    peer_pair.drop_next_response("ep")
    async with peer_pair.ao.sessions.begin() as session:
        await peer_pair.ao.service.client.submit(message, session)
    await peer_pair.pump(rounds=8)
    assert peer_pair.effects == 1
    assert await peer_pair.ao.delivery_state(message.command_id) == "acknowledged"
```

- [x] Transactional handlers run with their handling-state update in one local transaction. External handlers persist preparation/claim first, execute outside a transaction, and finish with both journal and domain generation checks. Reclaimed external processing requires `reconcile`, never automatic replay. `None` from reconciliation means retain unknown state and schedule another observation.
- [x] Implement status-first reconciliation and original-identity child recovery. Authenticate the peer's recovery response; validate command/parent/hash/owner before routing it through the normal durable receipt path. Do not manufacture new message IDs from a status response. Permanent sender rejection invokes the registered local rejection handler so AO cannot wait forever for a completion that will never be sent.
- [x] Shared router exposes only `POST /v1/commands` plus GET command, children, and capabilities. Inject identity/session dependencies; own the ingress transaction and commit before returning 202. Map conflict/unauthorized/unsupported/not-found to 409/401-or-403/422/404. Do not expose an unjournaled cancel endpoint. Lifecycle shutdown cancels/awaits workers and closes pools without recording false success.
- [x] Run unit and PostgreSQL worker/pair tests, then commit `feat: expose reusable mediator client workers and API` with the required trailer.

## Task 7: EP command projections and explicit transaction participation

**Files:** Create `EP/src/execution_plane/commands/{__init__,models,store,receipts}.py`; modify `EP/src/execution_plane/store_base.py`, `cluster/cluster_store.py`, `execution_target/execution_target_store.py`, relevant methods of `work_store.py`, `migrations/env.py`; create `EP/src/execution_plane/migrations/versions/j6e7f8a9b0c1_add_command_exchange.py`, `EP/tests/integration/postgres/test_command_transactions.py`, `EP/tests/migrations/test_command_migration.py`; extend `EP/tests/test_store_base.py`.

**Interfaces produced:** SQLModel `CommandRecord`, `ResourceRevision`, and
`CommandCancellationIntent` in EP's schema; `CommandStore.accept(session, message) -> CommandRecord`,
`get(session, scope, command_id) -> CommandRecord | None`,
`reserve_resource_revision(session, message) -> RevisionDecision`,
`finish(session, command_id, scope, result: HandlingResult) -> None`,
`next_child_sequence(session, command_id, scope) -> int`;
`StoreBase.from_transaction(session: AsyncSession) -> Self`;
`EPReceiptHook.__call__(message, identity, session) -> None`.

`RevisionDecision` is apply/identical/superseded; conflicting content raises
`ProtocolConflict`. `ResourceRevision` persists resource type/key, client,
desired and observed revision, desired hash, resource ID, and deletion flag.
`CommandCancellationIntent` reserves cancellation by client/project/target ID
without requiring the target command to have arrived.

- [x] Write/run failing tests proving that a journal receipt, command record, and borrowed-store mutation roll back together; existing standalone/from-session store calls still commit as before; a failed nested dedup insert does not poison a caller transaction; redaction of returned models does not clear persisted credentials.

```python
async def test_work_store_can_join_outer_transaction(command_db, work_request):
    async with command_db.sessions() as session:
        store = WorkStore.from_transaction(session)
        await store.dispatch(**work_request)
        await session.rollback()
    assert await command_db.work_count() == 0
```

Define `command_db` in `EP/tests/integration/postgres/command_helpers.py` using
migrated disposable EP schema, sessions, and scoped SQL read helpers. Define
`work_request` with a valid Task 2 invocation and existing eligible test target.

- [x] Add opt-in transaction ownership. In participating mutation methods replace direct commit/rollback with explicit helpers, preserving default behavior:

```python
async def _commit(self, session: AsyncSession) -> None:
    if self._owns_transaction:
        await session.commit()
    else:
        await session.flush()

async def _rollback(self, session: AsyncSession) -> None:
    if self._owns_transaction:
        await session.rollback()
```

Set `_owns_transaction=True` for all existing constructors, and False only for
`from_transaction`. Use a savepoint for recoverable `IntegrityError` paths in
borrowed transactions; simply suppressing rollback and querying an aborted
session is invalid. Do not add network calls or session proxies.

- [x] Implement receipt projection and resource reservation under row locks. Keep generic projection payload encrypted. Allocate parent child sequence under the parent lock. Validate scope/resource ownership before recording acceptance; ensure latest-revision locking and domain handling take locks in the same order.
- [x] Register shared journal metadata without expanding Alembic ownership to another service schema. Add the forward revision after the current EP head, including constraints/indexes/default runtime grants. Detect unresolved legacy work/completion delivery before a destructive cutover; report counts, never payloads. Keep existing domain tables intact.
- [x] Verify upgrade/schema drift and disposable-database downgrade/re-upgrade; run relevant existing store tests plus new PostgreSQL tests and commit `feat: add EP command projection and transactional store integration` with the required trailer.

## Task 8: EP resource commands, revision fencing, and cancellation intent

**Files:** Create `EP/src/execution_plane/commands/{clusters,targets,control,reconciliation}.py`; extend `commands/store.py`; make only required narrow registry/store extensions; create `EP/tests/integration/postgres/test_resource_commands.py`, `test_command_cancellation.py`.

**Interfaces produced:** `ClusterCommandHandler.handle(message, context, session) -> HandlingResult`,
`TargetCommandHandler.handle(message, context, session) -> HandlingResult`,
`CancelCommandHandler.handle(message, context, session) -> HandlingResult`,
`EPResourceReconciler.run_once() -> int`.
`EPResourceReconciler` observes drain completion and emits the final child in
the transaction that updates command/revision observation; requesting a drain
is not falsely reported as completed deletion.

- [x] Write/run a three-command registration test and a provisioning test:

```python
async def test_registration_and_provision_are_distinct(resource_case):
    cluster = await resource_case.register_cluster()
    assert cluster.status == "registering"
    assert await resource_case.targets(cluster.id) == []
    await resource_case.register_default_target(cluster.id)
    assert (await resource_case.cluster(cluster.id)).status == "active"
    provisioned = await resource_case.provision_cluster()
    assert len(await resource_case.targets(provisioned.id)) == 1
```

`resource_case` in `command_helpers.py` submits through real receipt/handling
workers using Task 2 builders, then reads committed resources. Its registration
helpers return safe resource views; they do not mutate ORM state directly.

- [x] Add tests for duplicate register/provision, concurrent revisions, equal-revision conflict, older accepted work superseded before handling, old upsert after deletion, same-name resources owned by different clients, cross-client target/cluster references, unauthorized projects, protected default mutation, and cancellation arriving first.

```python
async def test_cancel_before_submit_prevents_work(command_case):
    original = command_case.work_message()
    await command_case.receive_cancel_for(original)
    await command_case.receive_and_handle(original)
    assert await command_case.provider_calls() == 0
    assert (await command_case.read(original.command_id)).status == "cancelled"
```

Define those methods in `command_helpers.py` as actual shared-service operations
plus a provider-call spy; `read` uses the Task 7 command projection until the
Task 10 HTTP query exists.

- [x] `cluster.register` uses `ClusterStore.from_transaction(session).create` without discovery. Target registration uses the existing target registry and activates the cluster only after checking its unique usable default. `cluster.provision` composes existing provisioning with borrowed transactional stores and restores a partially converged logical resource by its resource key, never by unscoped name. Put `CancelCommandHandler` in `commands/control.py`.
- [x] Lock the revision row before applying resource mutations, compare the reserved revision/hash again, and mark superseded work explicitly. Preserve resource-key tombstones after the drain monitor deletes physical rows. Handle reactivation as a later revision. Provision update applies credential, CA, labels, project grants, endpoint, and placement consistently, including explicit clearing where supported; omitted and null values must not be confused.
- [x] Cancellation validates target scope and declared capability. If the target is absent, insert durable cancellation intent; apply it before creating a WorkItem when the original later arrives. If present, use existing WorkStore cancellation methods in the same transaction and emit final state only when stopped/terminal. Unsupported irreversible resource undo returns an explicit failed cancellation result; use delete commands for draining.
- [x] Run new PostgreSQL cases and existing cluster/target/drain tests. Commit `feat: handle cluster target and cancellation commands` with the required trailer.

## Task 9: Work adapter and atomic completion handshake

**Files:** Create `EP/src/execution_plane/commands/work.py`; modify `EP/src/execution_plane/work_store.py`, `event_delivery.py`, and only wiring in `worker.py`; extend `EP/tests/test_work_store.py`, `test_event_delivery.py`, `test_worker.py`; create `EP/tests/integration/postgres/test_work_command_completion.py`.

**Interfaces produced:** `WorkCommandHandler.handle(message, context, session) -> HandlingResult`;
`CompletionRecorder.__call__(session: AsyncSession, event: CompletionEvent) -> None`;
`WorkStore.set_completion_recorder(recorder: CompletionRecorder) -> None`;
`EPCompletionRecorder` implementing that callable;
`CompletionEventDelivery(settings, *, runtime=None)` retaining its lifecycle
entry points `run(store)` and `close()` while delegating to the shared service.

- [x] Write/run tests for work mapping and atomic rollback when completion journaling fails. Parameterize every producer: `set_result`, `mark_reconciliation_required`, stale claim recovery in `claim_one`, cancellation of pending/claimed work, and the cancellation branch of `requeue_pre_execute_failure`.

```python
async def test_completion_journal_failure_rolls_back_result(work_case):
    await work_case.fail_journal_insert()
    with pytest.raises(RuntimeError, match="journal insert"):
        await work_case.complete_claimed_work()
    item = await work_case.reload_work()
    assert item.completed_at is None
    assert await work_case.completion_count() == 0
```

`work_case` in `command_helpers.py` creates accepted command/resource linkage,
uses real WorkStore claims, injects one failing recorder store call, and reads
both journal and WorkItem in fresh sessions.

- [x] Dispatch via the borrowed transaction WorkStore. Use stable command identity as WorkStore request ID, retain caller correlation, and store command→work ID in the same transaction. Check cancellation intent before dispatch. Handling the enqueue is complete even though provider execution is still pending; do not emit a work completion at this stage.
- [x] Enforce execution policy: invocation timeout and declared timeout must agree; a configured maximum attempt count bounds only positively safe pre-Execute retries. Add a narrow check before requeue/dispatch that fails exhausted work through the same completion recorder. Unknown outcomes never consume a retry to start another external action. Test absent/one/multiple maximum attempts and timeout mismatch.
- [x] Factor existing event creation through one awaited helper that both preserves the local outcome projection and invokes the recorder before commit:

```python
async def _record_completion(self, session, event):
    session.add(event)
    if self._completion_recorder is not None:
        await self._completion_recorder(session, event)
```

Initialize `_completion_recorder: CompletionRecorder | None = None` in every
WorkStore construction path. Runtime setup installs the recorder before polling starts. For command-backed
runtime work, missing command mapping/recorder is a configuration/invariant
failure, not a reason to silently drop delivery. Keep test/internal standalone
store behavior explicit. Derive stable completion IDs from parent and outcome
identity; map `completed`→`succeeded` and `reconciliation_required`→`outcome_unknown`.
Allocate child sequence once and persist terminal projection plus outbound
envelope together. No HTTP is permitted in the recorder.

- [x] Refactor event delivery to supervise shared delivery/handling/reconciliation; remove the old direct `/events` callback transport. Acceptance of a completion updates journal delivery; it does not claim AO finished Temporal processing. Keep result-before-cleanup order and existing claim-owner/generation arguments unchanged.
- [x] Run work/worker/event tests and real-PostgreSQL completion tests. Confirm protected worker-manager/node-protocol files have no unnecessary diff. Commit `feat: journal work completions in the provider transaction` with the required trailer.

## Task 10: EP API, safe queries, configuration, and service lifecycle

**Files:** Create `EP/src/execution_plane/commands/{resources,runtime}.py`; modify `EP/src/execution_plane/api/{main,auth,schemas}.py`, `config.py`, `worker.py`, `event_delivery.py`, `compose.yaml`, `.env.example`, `tools/generate_secrets.sh`, `tools/generate_certs.py`, `EP/deploy/kubernetes/base/{api,worker,migrate-job,kustomization}.yaml`; create `EP/tests/test_command_api.py`, `EP/tests/integration/postgres/test_command_queries.py`; extend config/OpenAPI tests.

**Interfaces produced:** `build_ep_runtime(settings, session_factory) -> MediatorService`;
`EPCommandQueryProvider` implementing Task 6 queries;
scoped resource list/get adapters returning shared `WorkItemRead`, `ClusterRead`,
`ExecutionTargetRead` schemas in client `protocol.py`. `CommandRead` contains
resource/provider reference, latest child sequence, safe error, delivery and
handling state, timestamps, and explicit uncertainty.

- [x] Write/run route tests proving the only action route is `/v1/commands`, receipt does not return 202 if commit fails, and resource/status/children reads enforce client/project grants and redact credentials/private payloads:

```python
def test_only_command_ingress_mutates(app):
    mutations = {(route.path, method) for route in app.routes
                 for method in getattr(route, "methods", set())
                 if method in {"POST", "PUT", "PATCH", "DELETE"}}
    assert mutations == {("/v1/commands", "POST")}
```

- [x] Replace legacy write/cancel routes and static capabilities with the shared router. Keep health endpoints. Expose `GET /v1/commands/{command_id}`, its `/children`, `/v1/capabilities`, and list/get work-items, clusters, and execution-targets. Use bounded cursor pagination for children; enforce maximum list sizes. Overlay live WorkItem state on command reads so a claimed/dispatched work item cannot be reported as merely queued.
- [x] Add participant ID, public ingress URL, registered peer URLs/grants/network ranges, key IDs/signing key path/verification key map, journal polling/lease/retry settings, and encryption provider wiring. Give API/worker/migrator consistent schema/key configuration. Compose and Kubernetes mount the required EP signing and mTLS credentials and AO trust keys; update health checks for required client certificates without weakening command authentication.
- [x] Retire the cluster-binding transport loop from worker wiring; the new command/revision observer replaces it. Avoid deleting/reworking protected cluster/model code merely because legacy helpers are now unused. New resource completion, drain observation, and worker shutdown all use the shared service lifecycle.
- [x] Regenerate `EP/openapi.yaml` with `uv run python tools/export_openapi.py`. Run route/query/config tests and `uv run pytest tests/test_api_openapi.py -q`; commit `feat: serve command-only EP API and scoped recovery queries` with the required trailer.

## Task 11: AO client dependency, mediator configuration, and journal migration

**Files:** Modify `AO/backend/pyproject.toml`, `uv.lock`, `requirements.txt`; create `AO/backend/src/syntara/execution_plane/runtime.py`; refactor `client.py`; modify `core/config/base.py`, `core/database/migrations/{models,env}.py`, AO environment examples; create `AO/backend/src/syntara/core/database/migrations/versions/4e5f60718293_add_ep_exchange_journal.py`, `AO/backend/tests/unit/execution_plane/test_runtime.py`, `AO/backend/tests/integration/execution_plane/test_journal_migration.py`.

**Interfaces produced:** `build_ao_runtime(session_factory, *, temporal_client=None) -> MediatorService`;
`get_ep_client(runtime) -> MediatorClient`; AO adapters for existing JWT key
manager, mTLS client, certificate-authenticated ingress identity, and encryption.
Factories accept dependencies; request handlers respect `get_db` and FastAPI
dependency overrides instead of opening ambient production sessions.

- [x] Add the approved local source to AO metadata and regenerate locks/requirements with uv/make. Run `make install` before any AO checks. Test import/configuration without importing `execution_plane` and test AO's dependency overrides select the supplied database.

```toml
[tool.uv.sources]
execution-plane-client = { path = "../../syntara-execution-plane/src/execution_plane_client", editable = true }
```

- [x] Write/run failing journal metadata and migration tests: AO default schema, separate EP schema excluded, no second application's table registration, fresh upgrade, preserved domain binding data, and explicit refusal with unresolved legacy delivery. Test that migration diagnostics contain counts but no payload/token values.
- [x] Add shared schema metadata to AO migrations without sharing an engine/schema with EP. Generate the Alembic revision from the models on a disposable database and inspect it; separate required custom conversion/preflight SQL with `# CUSTOM:`/`# END CUSTOM` per AO instructions. Keep legacy table removal until Tasks 12–14 stop using them; a following cleanup revision handles removal.
- [x] Build AO peer configuration from existing key manager and internal TLS helpers. Add EP verification keys and exact AO/EP ingress URLs through settings. The runtime has query/receipt capabilities even in the API process; Temporal external handling runs only in workers with an injected Temporal client.
- [x] Run migration/runtime tests, `make sync-requirements`, and commit `feat: configure AO shared command mediator and journal` with the required trailer.

## Task 12: AO durable script enqueue, completion, and cancellation

**Files:** Refactor `AO/backend/src/syntara/execution_plane/{models,bridge}.py`; create `handlers.py`; modify `AO/backend/src/syntara/workflows/workflow_engine/activities/ep/ep_dispatch_activity.py`, `workflows/services/execution_service.py`; add `AO/backend/src/syntara/core/database/migrations/versions/5f60718293a4_update_ep_activity_bindings.py`; create `AO/backend/tests/integration/execution_plane/test_activity_handshake.py`, `test_temporal_delivery.py`; update `AO/backend/tests/unit/workflows/activities/test_script_activity.py`.

**Interfaces produced:** `enqueue_script(session, *, command_id, project_id, temporal_identity, task_token, activity_attempt, payload) -> MessageEnvelope`;
`enqueue_workflow_cancellations(session: AsyncSession, temporal_workflow_id: str) -> int`
appending cancel commands without committing;
`request_ep_cancellation_for_workflow(temporal_workflow_id: str) -> int` as a
transaction-owning wrapper for callers without an existing domain session;
`TemporalCompletionHandler` implementing Task 2's
external contract; `AODispatchRejectionHandler` implementing a transactional
local rejection hook and durable Temporal failure responsibility.

`TemporalIdentity` is an AO SQLModel value with namespace/workflow/run/activity
IDs. Keep the binding's token ciphertext, current attempt/generation, handoff
state, expected completion identity, safe error, and updated times. Transport
retries/leases live only in the journal. Stable command UUID derives from
namespace/workflow/run/activity/generation using UUID5; retries do not select
a new image, invocation, route, or timeout.

- [x] Write/run tests for binding+outbound atomicity, async handoff without HTTP, original payload reuse after activity retry, immediate completion before async handoff, definitive rejection delivered as failure, cancellation while submission is queued, and this fencing case:

```python
async def test_old_rpc_cannot_finish_refreshed_activity(temporal_case):
    effect = await temporal_case.prepare_completion(attempt=1)
    await temporal_case.retry_activity(attempt=2)
    await temporal_case.finish_success(effect)
    assert (await temporal_case.binding()).activity_attempt == 2
    assert (await temporal_case.binding()).status != "completed"
```

`temporal_case` is created in `AO/backend/tests/integration/execution_plane/conftest.py`
using AO test sessions and a fake injected Temporal client whose RPC completion
can be held/released. Its methods execute the actual handler prepare/finish and
enqueue functions, not ORM-only test mutations.

- [x] Replace activity HTTP retry loop with one domain+journal transaction, then `activity.raise_complete_async()`. Preserve payload validation and script enablement. Refresh only the binding's token/attempt on Temporal retry; re-arm local handling of an already durable completion for the new attempt without changing the immutable command.
- [x] Autogenerate and review the binding-change migration before running the new integration tests against migrated schemas. Preserve encrypted tokens/correlation and derive command IDs consistently from the existing domain identity; refuse unresolved legacy handoffs that cannot be converted safely. Keep transport-column removal for Task 14.
- [x] Implement external Temporal prepare/execute/finish. A completion may be durably accepted while handoff is still pending; defer its local handling. Do not assume a fixed sleep proves Temporal has entered async completion. Classify definitive not-ready responses separately from ambiguous RPC outcomes. Persist the attempt generation before RPC and compare it before marking handled. For ambiguous RPC/crash-after-send, use Temporal observation where conclusive; otherwise retain explicit reconciliation-required state and never assert success or blindly repeat.
- [x] Map succeeded to `handle.complete`, failed/cancelled/unknown to the existing non-retryable domain errors. Rejection before EP acceptance creates a durable AO-local failure delivery with the same binding fencing. Workflow cancellation appends a stable `command.cancel` intent in the caller's domain transaction using `enqueue_workflow_cancellations`; commit durable intent before any Temporal cancellation RPC. The wrapper owns a transaction only where no caller transaction exists. Test rollback of workflow cancellation intent and journal together; do not wait for EP HTTP inside the workflow state transaction.
- [x] Run activity/Temporal tests including existing workflow cancellation behavior. Commit `feat: migrate AO workflow handoff to durable commands` with the required trailer.

## Task 13: AO immutable integration revisions and completion handlers

**Files:** Refactor `AO/backend/src/syntara/execution_plane/integration_sync.py`; modify `AO/backend/src/syntara/integrations/services/integration_service.py`, execution-plane handlers; update `AO/backend/tests/integration/integrations/test_integration_cluster_sync.py`, `test_integration_router_cluster_sync.py`; create `AO/backend/tests/integration/execution_plane/test_integration_commands.py`.

**Interfaces produced:** `enqueue_integration_revision(session, integration, operation: Literal["upsert", "delete"]) -> MessageEnvelope`;
`IntegrationCompletionHandler.handle(message, context, session) -> HandlingResult`;
`reconcile_integration(session_factory, client, integration_id) -> None` as an
adapter hook used by the shared reconciler, not another transport worker.

- [x] Write/run tests for rollback of integration+command, credential/config/project-assignment snapshot, disabled/deleted integration, same revision retry hash, callback for an old revision, superseded outcome, and physical deletion followed by stale replay:

```python
async def test_queued_revision_survives_credential_change(integration_case):
    message_id = await integration_case.enqueue_with_token("first-test-token")
    await integration_case.rotate_token("second-test-token")
    message = await integration_case.load_journal_message(message_id)
    assert message.command.payload["credential"] == "first-test-token"
    assert "first-test-token" not in await integration_case.raw_journal_text()
```

Define `integration_case` in AO execution-plane integration-test conftest using
existing credential/integration fixtures, the real IntegrationService, and
fresh-session journal reads. Values above are test-only dummy credentials.

- [x] Convert `_queue_execution_plane_sync` into an awaited operation and update every caller (create/update, enable/disable, project assignment/removal, delete). Resolve management credentials and build canonical payload while the source revision is fixed. Use the caller session for database-backed credential resolution. If a credential backend requires external I/O, resolve outside the mutation transaction, then lock/recheck credential/config revisions before committing the frozen snapshot; retry a changed snapshot rather than doing external I/O under locks.
- [x] Increment the integration revision and append `cluster.provision` or `cluster.delete` in the same transaction; freeze resource key, credential, CA, scope, labels, endpoint and placement. For deletes, serialize the required identity before removing the integration. Disabled state is explicit desired state and must preserve existing drain behavior.
- [x] Apply only completion/observed revision matching the integration's current desired revision. Preserve pending/ready/error/deleting semantics for the UI. Generic command query comes first in reconciliation; resource reads enrich provider detail. Remove transport `_claim_outbox`, `_deliver`, retry counts, and `_mark_outbox` from the integration adapter.
- [x] Run integration service/router tests and new revision tests. Commit `feat: synchronize AO integration revisions through commands` with the required trailer.

## Task 14: AO ingress, worker lifecycle, old transport removal, and test peer

**Files:** Create `AO/backend/src/syntara/execution_plane/router.py`; modify `api/{main,execution_plane_facade}.py`, `workflows/workflow_engine/services/temporal_worker.py`, execution-plane models/runtime; remove obsolete `execution_plane/integration_sync_model.py`; create `AO/backend/src/syntara/core/database/migrations/versions/60718293a4b5_retire_ep_transport_tables.py`; modify migration imports, `AO/backend/tests/fixtures/fake_execution_plane.py`, integration fixtures `client.py` and `temporal.py`; create `AO/backend/tests/unit/api/test_execution_plane_commands.py`, `AO/backend/tests/integration/execution_plane/test_ao_ingress.py`.

**Interfaces produced:** AO router mounts shared `/v1/commands`/query/capability
paths with injected certificate/assertion verification and AO sessions;
`run_ao_mediator(temporal_client, session_factory) -> None` replaces the two
old transport bridges; `FakeExecutionPlanePeer` in tests implements the shared
protocol over an injected test transport and offers `pump()`/`close()`.

- [x] Write/run tests for valid mTLS identity plus signed message, wrong certificate CN, missing assertion, scope mismatch, commit-before-202, duplicate completion with one local delivery, safe facade authorization, and graceful worker shutdown.

```python
async def test_receipt_requires_both_cert_and_assertion(ao_http_case):
    response = await ao_http_case.post_valid_message(cert_authenticated=False)
    assert response.status_code == 403
    assert await ao_http_case.journal_count() == 0
```

`ao_http_case` uses the app's normal middleware/dependency overrides and a
transactional test DB. Never mount test bypasses in production configuration.

- [x] Mount shared ingress outside excluded internal prefixes, preserve audit/rate-limit/body-size/certificate checks, and keep the user-facing read facade's ProjectScopeFilter. Pass a `SyntaraRouter` instance to `create_router` with AO's `NO_PERMISSION` route-scanner marker and mandatory machine-identity dependencies; that marker exempts user-policy checks, never peer authentication. The generic peer query scope is verified independently of user-facing routes. Replace old event ingress and HTTP-client calls in the facade with shared query methods.
- [x] Start one shared AO service worker group with registered Temporal/integration handlers from the existing Temporal-worker lifecycle. API processes only accept/query. Ensure replica claims fence duplicates. Stop/cancel/await all tasks and close transports on startup failure and shutdown.
- [x] Remove old inbox/outbox/HTTP transport code and cancellation delivery columns once no callers remain. Autogenerate the cleanup migration; abort with safe counts if unresolved legacy rows would be lost. Preserve needed activity-binding/domain records and outcome history. Do not delete pending rows automatically.
- [x] Update the fake EP to receive/persist/accept commands and deliver completion commands asynchronously through the shared protocol; remove the synchronous-completed-submit shortcut. Keep representative local script execution confined to tests. Update fixtures so transport retries, handoff, cancellation and completion ordering are exercised with actual journal operations.
- [x] Run ingress/facade/workflow/integration tests and migration upgrade checks. Regenerate AO API specs/contracts with `make gen-contracts` from AO root and include generated-client removals/additions. Commit `feat: complete AO command ingress and retire legacy transport` with the required trailer.

## Task 15: Paired-checkout builds, CI source acquisition, and hermetic packaging

**Files:** Modify `EP/Containerfile`, `EP/compose.yaml`, `EP/.github/workflows/ci.yml`, `EP/README.md`; modify `AO/backend/containers/syntara/Containerfile`, `AO/backend/Makefile`, `AO/podman-compose.yml`, `AO/backend/tools/ci/{sanitize_pyproject_for_hermetic.py,local-hermetic-build.sh}`, `AO/backend/artifacts.lock.yaml`, `AO/backend/requirements.txt`, `AO/.github/actions/setup-test-env/action.yml`, `AO/.github/workflows/{ci-backend,_build-backend-image}.yml`; create `AO/backend/tools/ci/{ep_client_source.py,pin_ep_client_source.py}`, `AO/.github/actions/setup-ep-client/action.yml`, `AO/backend/ep-client-source.json`, `AO/backend/tests/unit/execution_plane/test_client_build_source.py`; extend `EP/tests/client/test_distribution.py`.

**Interfaces produced:** `prepare_client_source(local: Path | None, archive: Path | None,
destination: Path, expected_sha256: str | None) -> None` in `ep_client_source.py`;
`pin_ep_client_source.py` reads the completed EP Git revision and a configured
remote/archive location, verifies the actual archive bytes, and writes the
source lock plus Hermeto generic-artifact entry. `ep-client-source.json` records
repository, exact revision, archive URL/checksum, and package subdirectory.
It is generated from a real implementation revision, not a guessed future SHA.

Local development continues using the side-by-side path dependency. The
remote/archive descriptor is for CI source acquisition and hermetic prefetch;
it does not change AO's Python dependency into a Git dependency. A CI worker
reconstructs the same paired layout before resolving dependencies. Publication
of that referenced EP revision follows the normal repository review/push
workflow; do not claim remote CI is verified until the pin is reachable. Do not
publish a package or push a branch solely to work around a missing dependency
without the applicable execution-workflow authorization.

- [x] Write/run failing tests for missing local source, checksum mismatch, archive path traversal/symlinks outside the selected package, wrong distribution name, sanitized source override retention, requirements export, and a runtime install with no sibling source present:

```python
def test_source_preparation_never_installs_ep_service(source_case):
    source_case.prepare_local_client()
    assert (source_case.destination / "pyproject.toml").exists()
    assert not (source_case.destination / "execution_plane").exists()
    assert source_case.project_name() == "execution-plane-client"
```

Define `source_case` in the build test with a temporary miniature paired tree,
client metadata/files, and the real source-preparation helper. It must not
read user secrets or copy `.git`, `.venv`, `.secrets`, or the EP runtime tree.

- [x] EP's existing `COPY src` supplies both projects; retain `uv sync --frozen --no-dev --no-editable` and build only the root service image. Verify root/service sdist metadata and independent client artifacts are each self-consistent.
- [x] Add a local named build context to AO pointing specifically at the sibling client project. Preserve AO's backend context. Use a scratch source stage so hermetic builds can instead obtain the prefetched archive without attempting an accidental registry pull:

```dockerfile
ARG EP_CLIENT_SOURCE=scratch
FROM ${EP_CLIENT_SOURCE} AS ep_client_source
# In the AO builder, after copying the preparation helper:
RUN --mount=from=ep_client_source,source=/,target=/tmp/ep-client-source \
    python3.12 /usr/local/bin/ep_client_source.py \
      --local /tmp/ep-client-source \
      --archive /cachi2/output/deps/generic/execution-plane-client-source.tar.gz \
      --source-lock /opt/app-root/src/ep-client-source.json \
      --destination /opt/syntara-execution-plane/src/execution_plane_client
```

Copy `ep-client-source.json` before this step. The helper's CLI chooses a valid
local project first; otherwise it requires the lock's checksum and extracts
only the locked client subdirectory from the prefetched archive. The archive
path must reject a missing checksum, not treat `expected_sha256=None` as trusted.
Failure of both sources is an explicit build error. Preserve relative path
resolution from `/opt/app-root/src` and install dependencies non-editably in
both normal and hermetic AO build branches. Supply the source through compose
`build.additional_contexts.ep_client_source`; verify the installed
podman-compose version forwards it, and update the declared development
version if necessary. Do not silently ignore an unsupported compose key.

```bash
# From AO/backend, build with the same explicit source context as compose:
podman build --build-context ep_client_source=../../syntara-execution-plane/src/execution_plane_client \
  -f containers/syntara/Containerfile --target syntara -t localhost/syntara:ep-commands .
```

- [x] Add CI source setup before every AO dependency/build job that resolves this package, including reuse through setup-test-env. The action reads the source lock, checks out the exact EP revision, and provides a sibling path; when actions/checkout requires a destination beneath GITHUB_WORKSPACE, check out into `.ep-client-source` and create a verified sibling symlink for the declared path dependency. Never overwrite an existing unrelated checkout. Reuse the same source lock for image jobs and cache keys. Search the remaining workflows for `make install`/`uv sync` to catch callers outside the two primary files.
- [x] Exclude only the local client distribution itself from AO's generated pip requirements (`--no-emit-package execution-plane-client`), retaining its transitive dependencies for prefetch. Keep its local source override in the hermetic sanitizer, materialize the checksum-verified source before offline `uv lock`, and include its archive in `artifacts.lock.yaml`. Local hermetic tooling can supply the checked-out package/archive before the offline build; it must validate equivalent metadata/content. Test that no offline step falls back to PyPI/Git/network.
- [x] Build both service images and inspect installed distribution metadata in a container without source mounts. AO must contain `execution-plane-client` and no `execution-plane` distribution. Run the existing local hermetic build path with its required credentials/infrastructure when available; record an unavailable external pipeline or unpublished source pin explicitly instead of claiming success.
- [x] Update both READMEs with paired checkout, package build/install, required key/peer settings, commands-only cutover, and artifact pin update procedure. Commit build changes separately in each repository with the required trailer; keep the AO source descriptor synchronized with the actual EP implementation revision.

## Task 16: Cross-participant fault matrix, required checks, and review handoff

**Files:** Create `EP/tests/integration/postgres/test_exchange_fault_matrix.py`, `AO/backend/tests/integration/execution_plane/test_bilateral_commands.py`; update `EP/tests/integration/kind/test_dispatch.py`, AO workflow fixtures where necessary, `EP/docs/integration.md`, and `AO/backend/docs/execution-plane/integration.md`; add `EP/docs/superpowers/plans/2026-10-09-execution-plane-client-validation.md` for observed results.

**Interfaces consumed:** all public Task 1–15 interfaces. No new protocol or
domain behavior belongs in this final task; discoveries go back to their
owning task/tests before this matrix is repeated.

- [x] Build a two-participant integration fixture using the real EP application/handlers and AO runtime/handlers in distinct databases (or independently owned schemas in the disposable test server), with an injected fake Temporal service and fake node-provider execution. In AO tests import only the client package; run EP as a separate test peer/process installed from its wheel so AO does not acquire an EP runtime dependency.
- [x] Exercise fault injection at each durable boundary with a parametrized test:

```python
@pytest.mark.parametrize("failure_point", [
    "before_sender_commit", "after_receiver_commit", "after_acceptance_commit",
    "after_handler_claim", "after_result_commit", "after_completion_receipt",
    "after_temporal_rpc_before_finish",
])
async def test_restarts_preserve_responsibility(bilateral_case, failure_point):
    await bilateral_case.execute_with_failure(failure_point)
    await bilateral_case.restart_and_reconcile()
    await bilateral_case.assert_durable_or_explicitly_unknown()
    assert bilateral_case.provider_execute_count <= 1
```

Define `bilateral_case` in AO integration-test conftest. Its methods operate on
the two live services, stop/restart workers at controlled transport/handler
hooks, and assert committed row counts/status/identity, callback acceptance,
and Temporal binding state. For `before_sender_commit`, assert no request was
sent and no actor is falsely marked accepted. An ambiguous Temporal result is
an expected explicit uncertainty, not an expected successful completion.

- [x] Run end-to-end provision → execute → complete → cancel/query → drain/delete; test isolated projects, duplicate sends, conflicting routes, signature key rotation, lost acceptance, child-page pagination, terminal completion before acceptance, exhausted safe retries, stale revisions, and uncertain provider execution. Verify metrics and logs distinguish each class without private data.
- [x] Run EP required checks after focused tests pass:

```bash
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy --strict src
uv run pytest
uv run pytest tests/integration/postgres -q
uv run python tools/export_openapi.py
git diff --check
```

Run the kind dispatch regression with `EP_TEST_DATABASE_URL` and the existing
kind credentials/fixtures where available. A skipped kind suite is a recorded
coverage limit, not proof of provider integration. Avoid printing secrets.

- [x] Run AO required checks after its environment and source dependency are prepared:

```bash
make format
make lint
make test-all
make typecheck
make check-migrations
make sync-requirements
```

Run `make gen-contracts` from AO root and verify a second generation has no
drift. Review formatter/generated changes and exclude unrelated changes.
If a repository-wide failure predates this work, capture the exact failure and
baseline evidence; do not silently disable checks or fix unrelated systems.

- [x] Check import/dependency boundaries, secrets in raw PostgreSQL/journal/logs,
old action routes absent, no leftover transport loops or production fake-peer
imports, scoped query access, schema ownership, unchanged protected provider
behavior, and both non-editable image installs. Confirm no HTTP occurs inside
tracked DB transactions with transport spies in the integration suite.
- [x] Complete the validation record with command, environment, result, and
material limitations. Request the code review required by the chosen execution
workflow; fix verified findings and rerun only affected checks. Commit remaining
tests/docs in their respective repositories with the required attribution.
Use the branch-finishing skill to present/integrate the two related changes;
do not merge, deploy, or reset data as part of merely completing implementation.

## Spec coverage and execution handoff

| Approved requirement | Implementing tasks |
| --- | --- |
| Separate reusable package and one-way dependencies | 1, 2, 11, 15 |
| Complete command catalog and actions-only ingress | 2, 8, 10, 14 |
| Journal schema, encryption, caller transaction ownership | 3, 7, 9, 11–13 |
| Symmetric acceptance/completion and no acknowledgement loop | 5, 6, 9, 14 |
| Retry, fencing, unknown outcomes, reconciliation | 5, 6, 8, 9, 12, 16 |
| Discrete registration and combined provisioning | 8 |
| Revision ordering, supersession, deletion tombstones | 7, 8, 13 |
| Scoped queries, child sequence, capabilities | 2, 6, 10, 14 |
| mTLS, assertion signatures, destinations, key rotation | 4, 10, 11, 14, 16 |
| AO Temporal correlation and immutable integration payloads | 12, 13 |
| Legacy protocol retirement and safe schema cutover | 7, 10, 11, 14 |
| Minimal EP runtime changes and result-before-cleanup | 7–10, 16 |
| Side-by-side local builds, CI and hermetic source acquisition | 15 |
| Observability and required artifact/regression checks | 4–6, 10, 16 |

Packaging references checked while planning:
[Hatch path rewriting and editable builds](https://hatch.pypa.io/latest/config/build/),
[uv path dependencies](https://docs.astral.sh/uv/concepts/projects/dependencies/),
and [Podman additional build contexts](https://docs.podman.io/en/stable/markdown/podman-build.1.html#build-context-name-value).
Use installed versions when verifying commands; the local lock/build checks
remain the evidence for this implementation.

Recommended execution: **native in this session**, followed by an independent
whole-change review. The tasks are tightly coupled through transaction and
protocol interfaces and require coordinated changes in two repositories;
keeping that context with one implementer reduces interface churn. The
alternative is subagent-driven task implementation with a fresh reviewer for
each task, trading additional context/review cost for earlier independent
checks. The user selects the method after reviewing this plan.
