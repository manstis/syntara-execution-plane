# Single command journal validation

The correction removes EP's `CommandRecord`, `ResourceRevision` and
`CommandCancellationIntent` database models, their store and the separate command
reconciliation loop. The shared client's `exchange_journal` now owns intent,
status, resource reservations, cancellation targets and pending observations.
`ResourceReference` remains a nonpersistent API value. Provider models retain
their existing domain responsibilities.

## Verification

- EP full suite against disposable PostgreSQL: **428 passed, 1 skipped**.
  Command: `EP_TEST_DATABASE_URL=... EP_MIGRATION_DATABASE_URL=... uv run --no-sync pytest -q --timeout=45`.
  The skip requires a live kind cluster.
- EP `make lint format typecheck`: passed; 188 formatted files and 112 files
  checked with strict types.
- EP `alembic check`: no new upgrade operations detected.
- EP container build: passed, image `localhost/codex-ep-command:20261012`,
  ID `39c5ecd0168c4ae66f763c2d4dee2255f11881c3f332eab6251c697c2deb395c`.
- AO affected command, API, context, migration and installed bilateral suites:
  **88 passed** against the rebuilt EP image. Command from `syntara/backend`:
  `EP_BILATERAL_IMAGE=localhost/codex-ep-command:20261012 tools/scripts/run-with-testcontainers.sh -- uv run --no-sync pytest -o addopts='' tests/integration/execution_plane tests/unit/execution_plane tests/unit/api/test_execution_plane_facade.py tests/unit/api/test_execution_plane_commands.py -q --disable-warnings`.
- AO `make install`, `make ruff`, `make format`, `make check-migrations` and
  `make typecheck`: passed. Strict types covered 2,611 source/test files and
  23 test SDK files. Migration checks included metadata parity and the empty
  downgrade/upgrade cycle.

The EP suite includes 34 migration tests. The new migration preserves command
outcomes, resource tombstones, ownership, pending observations and cancellation
requests. It refuses orphaned or ambiguous responsibility before dropping the
three tables. Its downgrade refuses a nonempty journal. AO's matching migration
preserves immutable envelopes and encrypted private activity context, and refuses
to discard populated intent metadata. No user database was migrated.

## Independent review

The final review identified two material issues, both fixed with regressions:

1. Outbound enqueue and peer rejection could acquire intent and family locks in
   opposite orders. Intent locks now apply only to inbound command admission and
   handling; a deterministic PostgreSQL test reproduced the deadlock before the
   correction and passes afterwards. The shared regression suite passed 154 tests.
2. Removing the command-query overlay initially lost `cancel_requested` on the
   original work command. The cancellation handler now records that journal
   status atomically with the WorkItem update. Migration tests also cover valid
   cancellation state transfer and ensure bounded terminal outcomes are preserved.

The scoped review found the lock correction addressed. No other material findings
remain. Real PostgreSQL coverage also exercises supersession, conflicting
revisions, cross-scope access, cancellation before admission, observation claim
expiry, stale fencing, transaction rollback and duplicate completion suppression.

## Status interpretation

Command `running` means the handler admitted work to provider execution, including
time queued for a worker. Resource reads retain `pending`, `claimed` and
`dispatched` provider detail. This replaces the old query-time status overlay with
the persisted journal status. Callers that inferred provider dispatch from command
status must instead use the WorkItem resource query.

## Scope of checks

AO verification targets the affected execution-plane integration and unit suites;
it does not claim a green whole-repository AO test suite. Existing full-suite
collection and unrelated frontend lint failures were recorded in earlier work.
Both repositories remain on `feat/ANSTRAT-1803-durable-commands`; the integration
decision is local commits without a push or publication.

## Local handoff

- EP implementation commit: `106630c3fc1f917db096bf2459d032987bf357e0`.
- AO implementation/source-pin commit: `e91e6f1b3`.
- AO source archive checksum:
  `17b41adbe62f45157aac44a841ccf0c0c4ed7cd6c7173e0408a673e28f5093ac`.
- After refreshing the source pin, the 18 source-preparation unit tests passed.
  The actual local archive checksum and client-only extraction were also verified,
  including the intent API and schema version 3.
- AO's canonical formatting checks and normal commit hooks passed. Hooks were not
  bypassed in either repository. No commits or artifacts were pushed or published.

The client-source pin names the implementation commit. Subsequent EP changes to
this validation record do not alter the client package or require a new pin.
