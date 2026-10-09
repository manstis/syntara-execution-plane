# Syntara Execution Plane

The Execution Plane (EP) accepts and runs work dispatched by Syntara. This repository owns the standalone HTTP API, worker package, cluster and execution-target registries, work-item persistence, migrations for the `execution_plane` PostgreSQL schema, and the service container image.

Syntara owns user authorization, workflow dispatch, integrations, and the user interface. EP owns accepted work, execution state, and completion-command delivery. Temporal task tokens stay in Syntara and are not part of the EP API or schema. The first deployment may use the existing PostgreSQL server, but EP requires a distinct database and runtime role.

EP is one of several Syntara repositories. [Syntara](https://github.com/syntara-orchestration/syntara) is the orchestrator that accepts user intent and dispatches work; it depends on the shared execution-plane-client package to submit durable commands to EP. The [Syntara Plugin SDK](https://github.com/syntara-orchestration/syntara-plugin-sdk) is a firmly planned direct dependency: it provides the package the container entrypoint runs and the protobuf contract EP speaks to each node over gRPC, so it is shared by both the container images and the wire protocol. [Syntara Step Types](https://github.com/syntara-orchestration/syntara-step-types) defines the step and payload types a workload acts on; because EP routes and runs work through the gRPC contract and stays largely agnostic to the data being passed, it is only a potential dependency and may never become a required one.

## Requirements

- Python 3.12, 3.13, or 3.14
- [`uv`](https://docs.astral.sh/uv/)
- PostgreSQL for the service, plus a Kubernetes/OpenShift target for isolated script workloads

## Development

```bash
uv sync --locked --all-groups
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
uv run mypy --strict src
```

For a separate local EP database, copy `.env.example` to `.env` if you need to
override defaults. Generate local secrets and TLS material first (`make setup`,
or `./tools/generate_secrets.sh` and `uv run python tools/generate_certs.py`).
If a sibling Syntara checkout already has `backend/.secrets/jwt-primary.pub`,
the secrets script copies it so EP can verify tokens issued by local AO.
The standalone compose setup creates a local PostgreSQL server, an EP-owned
database, and separate migration/runtime roles.
Local work execution still requires a reachable Kubernetes/OpenShift target and
an image tag available to that cluster; there is no in-process script fallback.

The API accepts actions only through `POST /v1/commands`. Cluster registration,
execution-target registration, provisioning, updates, draining and cancellation
all use durable command messages. Scoped GET endpoints expose commands, child
outcomes, capabilities, work items, clusters and execution targets. A transport
202 confirms committed receipt; the receiver separately sends `command.accepted`.
Completion is another command with its own acceptance. Provider uncertainty is
reported as `outcome_unknown` and never triggers automatic execution replay.

API and worker use the same peer trust file, database, journal encryption key and
EP signing identity. `deploy/command-peers.example.json` documents the trust-file
shape. Review each peer's canonical URLs, issuer/audience, verification key IDs,
client/project grants, certificate common name and permitted destination CIDRs.
The API requires a CA-verified client certificate **and** a short-lived assertion
bound to the exact message or GET target. Redirects and unregistered destinations
are rejected. `EP_COMMAND_OPTIONS` can override bounded polling/lease/retry values
as JSON; provider timeout/maximum attempts belong to the immutable work command.

For paired development, generate AO's certificates first, then issue EP's
certificate under that CA without copying its private key:

```bash
./tools/generate_secrets.sh
uv run python tools/generate_certs.py \
  --ca-cert ../syntara/backend/.secrets/certs/ca.pem \
  --ca-key ../syntara/backend/.secrets/certs/ca.key
```

The example peer URLs use `execution-plane-api` and `syntara` on one container
network. For separate compose networks, configure reachable HTTPS URLs and matching
certificate SANs in both trust files. Export EP's public command key to AO; never
copy EP's private signing key there. All previous `/events`, work-item write and
cluster-binding write APIs are replaced.

For direct processes, set `EP_DATABASE_URL`, `EP_CREDENTIAL_ENCRYPTION_KEY_PATH`,
`EP_COMMAND_PEERS_PATH`, `EP_COMMAND_SIGNING_KEY_PATH`, and the API TLS certificate,
key and client-CA paths, then run `uv run execution-plane-api` or
`uv run execution-plane-worker`. The worker can use `EP_COMMAND_TLS_CA_PATH`,
`EP_COMMAND_TLS_CERT_PATH`, and `EP_COMMAND_TLS_KEY_PATH` for its outbound identity.
The worker installs the completion recorder before polling and journals provider
results before cleanup. Each participant has one `exchange_journal` table for
command intent, status, resource revisions, cancellation and recovery. The shared
handling worker also schedules observations of pending drains and cancellations.
Work items, clusters and execution targets retain provider state. Command GET
responses read the journal; resource GET responses expose scoped provider state.
Delivery and handling wake on transactional PostgreSQL notifications in both
directions, with polling retained for recovery. The shared runtime reserves one
autocommit listener connection from its database pool; provide capacity for that
connection alongside normal worker transactions.

The single-journal migration transfers the former command records, resource
revision reservations and cancellation intents into journal entries before
dropping their tables. It preserves deletion markers and pending observations,
and refuses a cutover if responsibility cannot be represented safely. Downgrade
requires an empty journal; restore a pre-cutover backup for populated databases.
Earlier migrations remove obsolete completion-event and cluster-binding tables.

Apply the EP migrations independently from Syntara's migration chain:

```bash
DATABASE_URL=postgresql+asyncpg://user:password@localhost/execution_plane \
EP_CREDENTIAL_ENCRYPTION_KEY=<base64url-encoded-32-byte-key> \
uv run alembic -c alembic.ini upgrade head
```

Build the worker image from this repository root:

```bash
podman build -f Containerfile -t localhost/execution-plane:dev .
```

The independent local compose stack is started from this repository with
`make setup && uvx podman-compose up --build`. The worker submits each script as a
short-lived Kubernetes Job in the selected execution target. Configure
`EP_WORKLOAD_RUNNER_IMAGE` to an image available to that target cluster and grant
the stored target credential permission to create/read Secrets, Jobs, Pods, Pod
logs, and NetworkPolicies in the configured namespace. `EP_WORKLOAD_ALLOWED_EGRESS_CIDRS`
is an operator-reviewed JSON list of destinations the workload may reach; without
it, workloads can resolve DNS but have no general egress. If a whole IP family is
allowed, also set `EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS` to the AO, Temporal,
Execution Plane, and database address ranges that must remain unreachable.
Per-integration Kubernetes API trust roots are stored encrypted in EP.

The target-cluster service account needs `create/get/delete` on Jobs, `list` on
Pods, `get` on `pods/portforward`, and `create/get/list/patch/delete` on
NetworkPolicies. It does not need workload Secret, Pod-log, or Pod-exec
permissions.
`EP_WORKLOAD_ALLOWED_EGRESS_CIDRS` is an operator-reviewed JSON list of
destinations the workload may reach; without it, workloads can resolve DNS but
have no general egress. If a whole IP family is allowed, also set
`EP_WORKLOAD_FORBIDDEN_EGRESS_CIDRS` to the AO, Temporal, Execution Plane, and
database address ranges that must remain unreachable. Validate enforcement with
the target cluster's CNI before enabling workloads. Per-integration Kubernetes
API trust roots and submitted invocation payloads are encrypted at rest by EP.

The Kubernetes base manifests are in `deploy/kubernetes/base`; provide the
database URLs, AES key, AO JWT verification key, callback mTLS material, server
TLS identities, command signing keys and peer trust file as Kubernetes Secrets and a
ConfigMap before applying them. AO supplies the node image reference in the
request and must pin it by digest. EP applies a NetworkPolicy before creating the
Job; it denies ingress and permits only DNS plus configured egress CIDRs. Verify
that the target cluster's CNI enforces NetworkPolicy before enabling script
workloads.

The management image runs as UID 1001. Its default command starts the controller worker; use `execution-plane-api` for the HTTP service. EP does not connect to AO's database or Temporal. User code runs in a separate workload Pod with bounded CPU and memory, a read-only root filesystem, no service-account token, no Linux capabilities, gRPC-only application communication, and a per-attempt NetworkPolicy. A Kubernetes Job can start a container more than once; this implementation uses one completion, no Job retries, an attempt identity, a lease fence, and `reconciliation_required` after an uncertain Execute boundary. The existing node runtime cannot replay a result after EP crashes between receiving it and committing it. Target-cluster CNI enforcement, production RBAC, approved egress ranges, cancellation behavior, and live-cluster recovery still require deployment validation before production isolation can be claimed.

## Ownership

Use this repository for EP worker, registry, migration, and placement changes. Make coordinated API, authorization, integration, and workflow changes in [Syntara](https://github.com/syntara-orchestration/syntara), and link the changes across pull requests.

## Shared client and paired builds

`src/execution_plane_client` is a separate Python project. EP depends on it through
`tool.uv.sources`; AO resolves the same source from a sibling checkout:

```text
workspace/
  syntara/backend/
  syntara-execution-plane/src/execution_plane_client/
```

Build the independent artifacts with `uv build src/execution_plane_client`, and
build the service with `uv build`. The service source archive carries the declared
client source path, while each wheel contains only its own package. The existing
Containerfile builds one EP image with both distributions installed non-editably;
there is no separate client image or service. AO's backend image accepts a named
`ep_client_source` context pointing only at this client project.

AO owns the CI source pin in `backend/ep-client-source.json` and the matching
Hermeto archive entry. Its `tools.ci.pin_ep_client_source` command generates an
exact source asset from a committed EP revision and records the actual checksum.
The normal repository publication workflow must make both revision and exact
asset reachable; local implementation does not imply that remote CI was verified.
See the sibling AO backend README for pinning and offline build commands.

For a single Compose stack, AO's compose file uses the separately built EP image
and generated AO/EP peer registrations. For separate stacks, share a trusted CA
and reachable network explicitly, and review the URLs/network grants in each
`command-peers.json`. AO's default JWT signing key ID is `orchestrator-primary`;
EP's independent command signing key defaults to `primary`.
