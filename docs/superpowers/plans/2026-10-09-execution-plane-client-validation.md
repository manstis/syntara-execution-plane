# Execution Plane client validation — 9 October 2026

This records the initial implementation. The subsequent in-place correction and
current verification are in [command-journal-cleanup-validation.md](2026-10-09-command-journal-cleanup-validation.md).

Implementation lives on `feat/durable-command-client` in paired worktrees for
EP and AO. AO final implementation commit: `89cba8c2b`; EP runtime/client
commit: `8767657` (later EP commits only finalize this record). Base revisions: EP `89532f2ec7133ca3043b79a413795cd9805be096`,
AO `4151af7d2`. No publication, deployment or merge is part of this delivery.

## Environment and package boundaries

Python 3.14.7 / uv 0.12.19 on the host; Python 3.12 in service images;
PostgreSQL 15 in disposable Podman containers. Journal tests create isolated
schemas. Migration tests use disposable databases, not user databases.

- Independent client wheel/sdist and service sdist checks: 3 passed. Clean
  client installation does not require either participant's runtime sources.
- Final EP image `localhost/codex-ep-command:20261009` (`9b7359c434ab`) built; both distributions
  install non-editably in site-packages, without the client source tree.
- Final AO image `localhost/codex-ao-command:20261009` (`19c425fe0c56`) and actual Compose build
  `localhost/codex-ao-compose:20261009` passed. Offline inspection without
  source mounts finds `execution-plane-client`, and no `execution-plane`.
- Scratch/archive source stage built with `--network=none`; checksum, traversal,
  symlink, wrong-project, local-source and source-lock tests pass.
- Full hermetic script was attempted but requires unavailable `RHSM_ORG_ID`.
  The offline source stage is verified; the complete external pipeline is not.
- Source lock pins actual EP revision
  `8767657f950f0107a1d83d50af860c31b11fbd54`; exact archive SHA256
  `44cc66f3a782db4d3a4f92d15204c92030326e28f7d6c1d82053fd97427eefdb`.
  The revision/release asset is unpublished. Remote CI requires its normal
  publication workflow; local verification does not prove remote fetchability.

## EP checks

`uv run ruff check src tests`, `ruff format --check src tests`, and
`mypy --strict src` pass (113 typed source files). Full suite after review fixes: 384 passed,
1 kind test skipped. PostgreSQL coverage: 68 original cases plus 6 review regressions passed, including the
transaction spy added to both authenticated HTTP peers and provider execution.
`tools/export_openapi.py` and `git diff --check` pass.

The kind test now submits a real command, invokes the existing provider and
asserts its durable completion child. It could not run here because
`EP_IT_K8S_ENDPOINT` / `EP_IT_K8S_TOKEN` were not configured. This is a provider
coverage limit, not a successful Kubernetes run.

The command migration round trip and command-owned metadata comparison pass.
A full EP Alembic metadata comparison reports baseline default/index drift in
untouched legacy tables (`cluster_bindings`, `completion_events`, `work_items`).
Those protected models were not changed to mask that pre-existing drift.

## AO checks

- `make format` passes. Strict `make typecheck` passes: 2,607 source/test files
  plus 23 test-SDK files. Full commit-hook pyrefly, changed-file frontend ESLint and frontend TypeScript pass.
- `make check-migrations` passes all graph, metadata and full downgrade/upgrade
  checks. Preflight refuses pending legacy work, callbacks, integration delivery
  and resource observations; completed history survives cleanup.
- `make sync-requirements` passes. Two consecutive root `make gen-contracts`
  runs produce identical tracked schema and TypeScript contract bytes. The
  existing example-copy step changes unrelated example formatting; those
  unrelated generated changes are restored, not included in this feature.
- `make lint` passes backend lint, formatting, types and validation hooks, but
  its unchanged frontend ESLint hook fails on Storybook's package.json lookup
  from the repository root. No ESLint/package-lock/Storybook configuration was
  changed by this branch. Frontend warnings are also emitted.
- `make test-all` cannot collect the unchanged nested `pytest_plugins` in
  `tests/unit/conftest.py` and `tests/integration/conftest.py`. A minimal
  temporary tree using the base revision's exact plugin declarations reproduces
  both collection errors. The separate full targets were run instead.
- Full `make test-unit`: 11,561 passed, 47 skipped, 1 xfailed, 1 xpassed,
  3 failed. All failures are unchanged telemetry conversion tests expecting
  `container_image_version='img-tag'` while the configured value is empty.
  The same three fail in isolation; telemetry implementation/tests are
  byte-for-byte unchanged from the base revision.
- Full `make test-integration`: 2,845 passed, 203 skipped, 2 failed. Both failures
  created OpenShift credentials with no stored secret; the new synchronous
  snapshot requires a usable credential. The fixtures now provide stored
  bearer tokens. All 60 owning integration-service/Temporal delivery tests pass.
  The final affected matrix also reruns the entire integration CRUD directory.
- Final affected AO/real-Temporal regression: 380 passed, including all 9
  installed-EP bilateral cases, all integration CRUD tests, both review adapter
  regressions and actual Temporal workflow patterns. No failures or skips in
  this final matrix. New tests were added beyond the earlier 181-case matrix.

## Bilateral and fault evidence

All 9 installed-peer tests pass. EP runs in its independently built image; only
its test control script is mounted. AO imports only the client. They use real
production EP HTTP routes, assertion verification, domain handlers and stores,
and real AO journal/Temporal handlers, in independent journal schemas. The
transport/certificate boundary and external provider/Temporal calls are
injected; separate real HTTPS tests validate certificate enforcement.

The matrix kills/restarts the EP process and reconstructs AO's service after:

1. Before sender commit (rollback): no work send or accepted responsibility.
2. Receiver commit with lost HTTP response.
3. Acceptance commit.
4. Handler claim commit.
5. Result/completion commit.
6. AO completion receipt.
7. Temporal RPC before local finish: explicit uncertainty, one RPC, no replay.

Each applicable case retains exactly one work item, one completion identity
and at most one provider execution. Additional tests cover provision, work,
completion, cancellation, command/child reads, actual draining and deletion;
completion before lost acceptance; and cross-project HTTP query rejection.
The test line transport serializes concurrent sends on its single control
channel; production HTTP pooling is exercised separately.

Other focused suites cover old revisions after deletion, cancellation before
submission, payload/credential mutation, signing-key overlap, conflicting
routes, page cursors, safe retry exhaustion, provider uncertainty, old claim
fencing and Temporal token replacement. Raw journal assertions check that
invocation text, tokens and management credentials are encrypted. Safe observer
and HTTP error tests check that private data is not emitted.

Four final adapter regressions were reproduced before fixing: maximum legal
integration names overflowing default-target names; disabled credentials
blocking drain; removed image settings blocking retries of already frozen
work; and HTTP-delivered legacy integrations losing their pending observation
at cutover. Their owning tests now pass.

## Independent review

One independent whole-change review examined both repositories against the
approved design and plan. All five findings were graded Important and fixed
in one implementation pass. No Critical or Minor findings remain.

- Large provider results previously rolled back terminal persistence. EP now
  retains the actual result/status and atomically emits a bounded failure with
  the work-item reference. Both a 600 KiB result and an envelope-overhead boundary
  were reproduced before the fix. Command queries report the delivered outcome.
- Valid near-limit completion bodies previously failed when AO added private
  token/identity context. Private preparation validates finite JSON independently
  of the wire cap. The actual AO adapter regression now passes; oversize delivery
  failures become explicit nonretryable `ExecutionPlaneResultTooLarge` errors.
- Work-item lists previously exceeded the transport cap with three 400 KiB
  results. Lists now omit result bodies; individual reads explicitly mark
  results above 512 KiB as omitted and report their byte size. Full results stay
  in EP storage; there is no download API in this version.
- Cancellation received before its original command could lose a concurrent
  handling race. Receipt now reserves scoped cancellation before ACK, serialized
  with work admission. The regression schedules both handlers concurrently and
  proves that no WorkItem is created.
- Reconciliation repeatedly selected old unresolved batches. Peer reads now
  order by next observation time; EP resource observation advances its timestamp
  even without progress. Tests with 11 peer records and 101 resource drains prove
  that the next batch is observed.

All six EP review regressions failed for the intended reasons before the fix
and pass afterward. AO's valid large completion and explicit error classification
also have observed RED-to-GREEN evidence. No second review was commissioned.

The final task gate separately passed 9 EP package/review cases and 29 AO
source/delivery cases. All task-owned disposable database containers were
removed after verification. The feature worktrees and built images are retained.
