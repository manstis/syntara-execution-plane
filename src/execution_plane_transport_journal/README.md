# Journal transport

`execution-plane-transport-journal` adds durable inbound handling and outbound
delivery to Execution Plane's shared command contracts. It persists immutable
command envelopes and delivery state in one participant-local
`execution_plane.exchange_journal` table, then uses HTTP to deliver them. It
depends on `execution-plane-transport-direct` for the authenticated HTTP
transport and on `execution-plane-client` for the command and router contracts.

## Run Execution Plane

Install the Journal extra. It includes the direct transport dependency:

```sh
pip install 'execution-plane[journal]'
```

Create the Journal table in the same database configured by `EP_DATABASE_URL`.
From the Execution Plane source checkout, run:

```sh
EP_JOURNAL_DATABASE_URL="$EP_DATABASE_URL" \
  alembic -c src/execution_plane_transport_journal/alembic.ini upgrade head
```

Configure the EP database, peer trust, signing key, TLS, and credential
encryption settings, then run the API and worker as separate processes:

```sh
execution-plane-api-journal
execution-plane-worker-journal
```

Both processes must use the same database and Journal encryption keys. The
active Journal key uses `EP_JOURNAL_ENCRYPTION_KEY_ID` and the base64url
32-byte `EP_CREDENTIAL_ENCRYPTION_KEY` (or its `_PATH` setting); supply older
key IDs in `EP_JOURNAL_DECRYPTION_KEYS` when decrypting existing entries. The
Journal package owns a separate Alembic version table, so its migration can
run alongside EP's domain migrations.

## Embed the EP API

An application that hosts the API itself can build the Journal runtime and
pass it to EP's FastAPI factory:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from execution_plane.api.main import create_app
from execution_plane.config import get_ep_settings
from execution_plane_transport_journal.ep_runtime import build_ep_journal_runtime

settings = get_ep_settings()
engine = create_async_engine(settings.database_url)
sessions = async_sessionmaker(engine, expire_on_commit=False)

runtime = build_ep_journal_runtime(settings, sessions)
app = create_app(runtime=runtime)
```

`create_app` mounts the shared command and completion-callback router and
closes the runtime on shutdown. A custom application wrapper should also
dispose the SQLAlchemy engine. The standard `execution-plane-api-journal`
entry point handles that lifecycle and configures mutual TLS. Run the worker
separately; it handles accepted commands, retries outbound HTTP delivery, and
reconciles uncertain outcomes.

The transport returns HTTP 202 after the inbound envelope is committed to the
Journal. Completion callbacks use the same durable outbound queue, so both
directions recover after process restarts.
