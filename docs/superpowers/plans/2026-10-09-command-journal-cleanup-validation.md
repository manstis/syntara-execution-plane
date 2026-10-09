# Command journal cleanup — 9 October 2026

The corrective work is applied directly to the original EP and AO checkouts on
`feat/ANSTRAT-1803-durable-commands`. No worktrees are changed or created.

The original implementation retained obsolete persistence while minimizing changes
to protected modules. That retention was incorrect for the requested replacement.
The correction removes EP's binding reconciler/model, completion-event model/table,
delivery methods and unused request-ID cancellation path. `commands/service.py`
owns EP shared-worker lifecycle. `WorkStore` records its result and the completion
command in one transaction, requiring the journal recorder.

AO removes the old bridge/sync module paths and activity-binding model/table.
Its domain adapters freeze commands and credentials; the original outbound journal
record stores only encrypted Temporal identity, attempt and token. Journal effects
and handling state own completion, uncertainty and recovery. The private context
is not part of wire envelopes or query contracts. Provider resource/work state,
its encryption and read-only historical AO outcomes retain their domain purpose.

## Verification

- EP full final suite: **395 passed, 1 skipped** (live Kubernetes configuration
  unavailable). Strict types: **113 files**, no issues. Lint and format pass.
- PostgreSQL regressions reproduce and then verify journal-only outcomes, atomic
  rollback, repeat suppression, scope/generation fencing, ciphertext rebinding
  rejection, legacy-table removal and preserved journal-owned work results.
- Real migration test refuses an unjournaled completion, accepts the same outcome
  after its child command is durable, and preserves work/result/journal data.
- AO focused Temporal/context/migration matrix: **28 passed**, followed by
  **8 migration tests passed** including three additional uncertain-parent cases.
- AO full strict types: **2,610 source/test files plus 23 SDK files**, no issues.
- AO autogeneration, upgrade, downgrade/re-upgrade and `alembic check` pass against
  a separate disposable database. No user database was migrated.
- Final EP installed-peer image: `b559a7a442d7`.

Independent review found two blockers, both reproduced and fixed: family locks
could deadlock duplicate acceptance; and AO cutover could drop correlation after
Temporal finished but EP still reported an unknown outcome. Parent-first locking
and an unresolved-parent preflight pass their real PostgreSQL regressions. Scoped
re-review found no remaining Critical or Important defect.

The AO repository-wide `make test-all` target still has the previously documented
nested `pytest_plugins` collection errors. `make lint` passes Python checks/types
but its unchanged frontend ESLint hook fails the root Storybook package lookup.
Its formatting-only example edits were verified against both parsed YAML and the
hook log, backed up in `/tmp`, and restored; no unrelated example changes are part
of the correction.

The broad affected AO run passed 300 tests and failed one historical migration
fixture that incorrectly ran revision 607 preflight against the new schema.
The fixture now uses its historical schema and restores head afterward. Both
owning migration/integration suites pass (19 tests). The final command/client
matrix, against the rebuilt installed EP image and final source pin, passes
**79 tests**, including all nine bilateral cases. Final canonical formatting,
file-format checks and full AO type checks pass.

AO pins EP commit `639584bb6927fca735b4087017d23b4d8fbe6448`; the exact local source
archive SHA256 is `81c8db6ed79f2eb568ee09145978b61c5b4d34f49e96187fb774c6c3f876da5c`.
No commits, archives or images were pushed or published. Source availability in
remote CI remains subject to the normal publication workflow, as before.

Local implementation commits: EP `639584b`, AO `a7de69d88`.
