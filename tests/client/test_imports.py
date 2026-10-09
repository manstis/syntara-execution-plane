"""The base client import remains independent of transport implementations."""

import subprocess
import sys


def test_client_root_import_does_not_load_transport_or_database_modules():
    script = """
import sys
import execution_plane_client
from execution_plane_client.api import create_router
from execution_plane_client import queries

assert create_router is not None
assert not hasattr(queries, 'InMemoryCommandQueryProvider')
for name in ('asyncpg', 'httpx', 'sqlalchemy', 'sqlmodel'):
    assert not any(module == name or module.startswith(name + '.') for module in sys.modules)
for name in ('execution_plane_transport_direct', 'execution_plane_transport_journal'):
    assert not any(module == name or module.startswith(name + '.') for module in sys.modules)
"""
    subprocess.run([sys.executable, "-c", script], check=True)
