# Execution Plane Client

Shared Pydantic command contracts, command catalog, in-memory handler registry
and a generic handler protocol. No HTTP router, network client, persistence,
authentication implementation or EP imports are included.

Participants register their own handlers. EP implements command submission and
resource queries; the caller handles `work.execute.completed` using this same
protocol. Commands have no persisted status or child history. A receipt confirms
that the domain handler returned, not that asynchronous resource work is complete.

## Constructing messages

Instantiate a typed command and call `to_message()`; the command supplies its
wire type and revision and builds the envelope:

```python
from uuid import uuid4

from execution_plane_client.commands.clusters import ClusterRegister

message = ClusterRegister(
    resource_key=uuid4(),
    desired_revision=1,
    name="workers",
    endpoint="https://cluster.example",
    namespace="execution-plane",
    credential="cluster-token",
).to_message(
    source_id="syntara-orchestration",
    destination_id="execution-plane",
    client_id="syntara-orchestration",
)

body = message.model_dump(mode="json")  # Send using your application's HTTP client.
```

Every concrete command supports this method, including `ClusterDelete` and
`TargetDelete`. Partial updates serialize only supplied fields, retaining explicit
nulls and placement discriminators without resetting omitted configuration.

By default, each call generates a command UUID and UTC timestamp; `message_id`
defaults to that command UUID. Keep the returned message for retries. To rebuild
it from existing domain state, pass `command_id` and `occurred_at` explicitly,
plus `message_id` if it differs. `parent_command_id` and `sequence` are optional;
`Completion` requires a parent work command UUID. Source, destination and client
scope are always supplied by the caller; the helper does not authenticate them.

Build with `uv build src/execution_plane_client`. See the service's integration
documentation for command examples and the current HTTP authentication contract.
