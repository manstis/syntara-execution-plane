# Command wakeup validation

Implemented in the original EP and AO checkouts on
`feat/ANSTRAT-1803-durable-commands`, without worktrees or publication.

## Behaviour

Journal appends publish transactional PostgreSQL notifications. The shared
listener wakes outbound delivery and inbound handling after commit in either
participant. This includes reverse completion and acceptance commands. The
journal remains the only durable contract; no schema migration is required.

Workers replenish available concurrency as individual tasks finish and use
persisted retry, acceptance, observation and lease deadlines. Polling remains
a recovery fallback. Producer calls retain their transaction semantics and do
not wait for HTTP. One additional stable database connection is required by each
running shared service listener.

## Evidence

- The initial bilateral regression failed with the old polling implementation:
  a command committed after idle scans could not complete within three seconds
  when the fallback interval was sixty seconds.
- Twelve new real-PostgreSQL cases cover bilateral commands/completions/ACKs,
  commit versus rollback, savepoint rollback, a commit racing an empty scan,
  reconnect/rescan, missed-notification fallback, retry and observation deadlines,
  competing delivery claims, and slow delivery/handling capacity.
- Focused PostgreSQL exchange tests: **26 passed**.
- Full EP suite: **440 passed, 1 skipped** in 35.82 seconds. The skip requires
  a live Kubernetes cluster.
- EP `make lint format typecheck`: passed; 191 formatted Python files and
  114 source files checked by strict mypy.
- EP container built successfully as `localhost/codex-ep-wakeups:20261012`.
- AO `make ruff typecheck`: passed, including strict mypy over 2,611 backend
  source/test files and 23 SDK files.
- Both repositories' `make install` targets passed after refreshing lock metadata.
- Independent whole-change review: no actionable findings or deferred issues.

- AO affected command/API unit and integration suites, including the installed
  bilateral peer: **88 passed** in 72.37 seconds against the rebuilt EP image.
- AO source-preparation tests after refreshing the pin: **18 passed**.
- AO canonical `make format`: passed after normalizing generated artifact YAML.
  Normal commit hooks passed, including dependency export; the requirements
  change only attributes the already-installed asyncpg dependency to the client.

AO validation targets the affected execution-plane suites. The full AO repository
suite and unrelated frontend lint were not rerun; their existing baseline issues
are described in [the preceding validation](2026-10-12-single-command-journal-validation.md#scope-of-checks).

## Local handoff

- EP implementation: `eed28a265ae9107da40bc13291665237c2fc1be7`.
- AO dependency/source pin: `54527a2c46b08eecabd2854c35719d0e3d840be3`.
- Client archive SHA-256:
  `84850a8a46c4f4d3ac6add2c4035b127f8728b6ea83f9e93db9b1585dba05bb4`.
- The actual archive was checksum-verified and safely extracted using AO's source
  preparation code; it includes the listener and scheduler and excludes the EP
  service package.
- Task-owned disposable PostgreSQL was removed. No user databases were changed.
- No hooks were bypassed; no commits or artifacts were pushed or published.

The AO source pin names the EP implementation commit. This later validation-only
update does not change client source or require another pin.
