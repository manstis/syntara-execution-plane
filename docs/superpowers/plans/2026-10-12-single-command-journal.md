# Single command journal implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox syntax for tracking.

**Goal:** Remove all three EP command tables and make the shared journal authoritative for command intent, observation and status in both participants.

**Architecture:** Store indexed intent and observations on real journal command rows. The shared worker resumes explicit observations with fenced claims; participant handlers supply domain operations only.

**Tech Stack:** Python, SQLModel/SQLAlchemy Core, PostgreSQL, Alembic, pytest, uv.

**Spec:** `docs/superpowers/specs/2026-10-12-single-command-journal-design.md`

## Global constraints

- Work in the original EP and AO checkouts; no worktrees and no pushes.
- Keep `ResourceReference` as a nonpersistent API value.
- Keep provider execution semantics and encrypted private data.
- Every commit includes `Co-authored-by: OpenAI Codex <noreply@openai.com>`.
- Shared code imports neither EP nor AO.

## Review focus

- New resource receipt during older handler execution must not deadlock or lose its last physical observation.
- Cancellation before original receipt and during admission must prevent duplicate work and preserve exact scope.
- Expired observation claims must resume observation, never replay initial domain mutations.
- Unknown outcomes must remain recoverable without repeatedly emitting identical completion children.
- Migration must preserve tombstones and pending observations or refuse the transaction without losing data.

## Task 1: Shared journal intent and observation contract

**Files:** `src/execution_plane_client/journal/{schema,records,store,claims,intents}.py`, `handlers.py`, `workers/handling.py`, `ingress.py`, `queries.py`; new PostgreSQL intent/observation tests.

**Interfaces:** `ExchangeJournalStore.intents` exposes `lock(session,message)`, `resource(session,message) -> JournalEntry`, `observe_resource(session,message,*,resource_id,observed_revision=None,deleted=None) -> JournalEntry`, `owner(session,resource_type,resource_id) -> JournalEntry | None`, `cancelled(session,message) -> bool`, and `record_result(session,entry_id,result)`. Resource reservation is automatic on new inbound append. JournalEntry adds resource metadata and observation scheduling state. HandlingResult adds `observe_after_seconds: float | None`; handlers with that result implement `observe(message,context,session)`.

- [x] Add real journal-only tests for revision ordering/conflict/ownership/cancellation, and restart/fencing of asynchronous observation; run to observe failure.
- [x] Implement journal metadata and intent API with advisory locks before row/family locks. Add partial unique resource-head and owner indexes.
- [x] Integrate shared receipt/handling/completion reads and writes, preserving immutable envelopes and unknown outcomes.
- [x] Run shared PostgreSQL tests, lint and strict types; correct any failures.

## Task 2: EP uses journal intent and shared observation

**Files:** `src/execution_plane/commands/{clusters,targets,resources,work,control,receipts,queries,runtime,service}.py`, `api/resources.py`, migration metadata and EP fixtures/tests. Delete `commands/{models,store,reconciliation}.py`.

**Consumes:** Task 1 interfaces. **Produces:** EP handlers using journal resource reservations, explicit observe methods, journal-only query provider, no extra polling loop.

- [x] Add regression proving resource actions, work completion and cancellation run with only the journal plus provider tables; observe failure.
- [x] Replace mutable ResourceRevision updates with explicit journal observation calls in resource handlers.
- [x] Replace cancellation lookup, work correlation and status updates with journal APIs. Remove EPHandledHook and EPCommandQueryProvider status overlay.
- [x] Route resource peer authorization through journal associations. Run existing resource/cancellation/completion/query tests using shared observation worker.

## Task 3: Migrations and AO compatibility

**Files:** both Alembic chains, migration tests, AO command/client fixtures, client source metadata.

**Consumes:** final journal schema from Task 1. **Produces:** matching participant schemas and a safe EP cutover.

- [x] Generate migrations against disposable databases after updating model metadata.
- [x] Add custom, separately marked data transfer/preflight to preserve authoritative EP projections and guard unsafe downgrades.
- [x] Test upgrade, downgrade constraints, metadata parity and rejected unsafe state.
- [x] Update AO shared schema and tests.
- [x] Refresh the client-source archive/pin after EP implementation is committed.

## Task 4: Integration verification and local commits

- [x] Run EP full suite, lint, formatting, strict types and migration parity.
- [x] Run affected AO command/context/migration and bilateral tests with an installed EP peer; run canonical AO checks and report existing unrelated failures accurately.
- [x] Perform fresh independent review, fix material findings with regression tests, and record results.
- [x] Commit in both original checkouts with attribution, leave clean trees, and do not push.

**Validation record:** `docs/superpowers/plans/2026-10-12-single-command-journal-validation.md`
