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

The API accepts actions and completion callbacks through `POST /v1/commands`.
Cluster registration, execution-target registration, provisioning, updates,
draining and cancellation use the shared command contract. Scoped GET endpoints
expose commands, child outcomes, capabilities, work items, clusters and execution
targets. HTTP 202 confirms delivery only; callers poll command status for its
outcome. Completion is another command posted to the original callback URL.
Provider uncertainty is reported as `outcome_unknown` and never triggers
automatic execution replay.

API and worker use the same peer trust file and EP signing identity. Journal
deployments also configure a database and journal encryption key.
`deploy/command-peers.example.json` documents the trust-file shape. Review each
peer's canonical URLs, issuer/audience, verification key IDs, client/project
grants, certificate common name and permitted destination CIDRs.
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

Choose the runtime by installing and invoking its factory: `DirectRuntimeFactory`
from `execution-plane-transport-direct` dispatches immediately over HTTP, while
`JournalRuntimeFactory` from `execution-plane-transport-journal` adds a durable
queue, retries and recovery. Both participants expose the shared callback router;
the Journal package is optional. EP work, cluster and execution-target state
remains in the EP database in either mode.

Apply EP domain migrations independently from Syntara's migration chain. A
Journal deployment runs the Journal migration chain as well; it owns a separate
Alembic version table in the same `execution_plane` schema:

```bash
DATABASE_URL=postgresql+asyncpg://user:password@localhost/execution_plane make migrate-direct

DATABASE_URL=postgresql+asyncpg://user:password@localhost/execution_plane \
EP_JOURNAL_DATABASE_URL=postgresql+asyncpg://user:password@localhost/execution_plane make migrate-journal
```

Build either transport-specific image from this repository root. The
`Containerfile.direct`/`compose.direct.yaml` pair selects direct mode; the
`.journal` files select Journal mode:

```bash
podman build -f Containerfile.direct -t localhost/execution-plane:dev .
podman build -f Containerfile.journal -t localhost/execution-plane-journal:dev .
```

Use the explicit Make targets to select a transport:

```bash
make image-direct
make image-journal
make compose-up-direct
make compose-down-direct
make compose-up-journal
make compose-down-journal
```

The two Compose projects use separate databases and publish the direct and
Journal APIs on ports 8001 and 8002 by default. Both use the same generated local
secrets; override the port or image through `.env` as needed. The Journal Compose
stack runs both EP domain and Journal migrations before starting the API and
worker.

For Kubernetes, choose one deployment: `deploy/kubernetes/base` selects direct
mode, and `deploy/kubernetes/overlays/journal` selects Journal mode. The Journal
overlay uses the Journal image and entry points and updates the migration Job to
run both Alembic chains:

Apply the direct base with `kubectl apply -k deploy/kubernetes/base`, or apply
the Journal overlay with `kubectl apply -k deploy/kubernetes/overlays/journal`.

The worker submits each script as a short-lived Kubernetes Job in the selected
execution target. Configure
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

Local helper scripts under `tools/` cover the rest of a first bring-up. EP does
not issue tokens; `tools/generate_jwt_for_ep.py` signs an AO service JWT with the
local or sibling Syntara ES256 key. Submit requires a `project_id` claim.
`tools/deploy_kind_execution_target.py` creates a kind cluster, installs workload
RBAC, and registers it through a cluster binding. `tools/submit_work_item.py`
posts a script work item to the local API.

```bash
make setup
uvx podman-compose up --build -d

# Service token for curl. Submit needs --project-id.
uv run python tools/generate_jwt_for_ep.py
uv run python tools/generate_jwt_for_ep.py --project-id 00000000-0000-0000-0000-000000000001

# Kind cluster + default ExecutionTarget (needs kind and kubectl)
uv run python tools/deploy_kind_execution_target.py

# Submit a script work item; --wait polls until it is terminal
uv run python tools/submit_work_item.py
uv run python tools/submit_work_item.py --language bash --code 'echo hello' --wait
```

Makefile aliases: `make generate-token`, `make kind-target`, and `make submit-work`.

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
client source path, while each wheel contains only its own package.
`Containerfile.direct` builds the direct HTTP transport image;
`Containerfile.journal` builds the Journal transport image and includes its
migration assets. AO's backend image accepts a named `ep_client_source` context
pointing only at this client project.

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
