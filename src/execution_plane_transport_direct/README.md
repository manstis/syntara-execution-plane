# Direct HTTP transport

`execution-plane-transport-direct` provides immediate authenticated HTTP
dispatch for Execution Plane. It implements the transport-neutral contracts in
`execution-plane-client` and does not persist a delivery queue or require a
Journal table.

## Run Execution Plane

Install the direct transport extra and run the API and worker as separate
processes with the same EP configuration:

```sh
pip install 'execution-plane[direct]'
execution-plane-api
execution-plane-worker
```

The entry points read `EP_DATABASE_URL`, peer trust and signing-key settings,
and the API/command TLS settings from the environment. The worker persists and
executes WorkItems through EP's domain database. Direct transport sends
commands immediately; receipt/deduplication and delivery do not have Journal
retry or restart recovery.

## Embed the EP API

An application that hosts the API itself can build the direct runtime and pass
it to EP's FastAPI application factory:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.api.main import create_app
from execution_plane.config import get_ep_settings
from execution_plane_transport_direct.ep_runtime import build_ep_direct_runtime

settings = get_ep_settings()
engine = create_async_engine(settings.database_url)
sessions = async_sessionmaker(engine, expire_on_commit=False)

runtime = build_ep_direct_runtime(settings, sessions)
app = create_app(runtime=runtime)
```

`create_app` mounts the shared command and completion-callback router and
closes the runtime on shutdown. A custom application wrapper should also
dispose the SQLAlchemy engine. The standard `execution-plane-api` entry point
handles that lifecycle and configures mutual TLS. The worker runs separately
so it can claim WorkItems and publish their completion callbacks.

Every AO peer must advertise the EP command URL and trust the configured EP
certificate. HTTP 202 confirms that the receiving runtime accepted the
command; query command status to learn its execution outcome.
