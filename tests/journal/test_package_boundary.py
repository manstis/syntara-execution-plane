"""Keep Journal runtime ownership downstream of the shared client contracts."""

import subprocess
import sys


def test_client_root_does_not_import_sqlalchemy_or_journal_modules():
    script = """
import sys
import execution_plane_client

for dependency in ('asyncpg', 'httpx', 'sqlalchemy', 'sqlmodel'):
    assert not any(name == dependency or name.startswith(dependency + '.') for name in sys.modules)
for package in ('execution_plane_transport_direct', 'execution_plane_transport_journal'):
    assert not any(name == package or name.startswith(package + '.') for name in sys.modules)
"""
    subprocess.run([sys.executable, "-c", script], check=True)


def test_journal_runtime_imports_client_contracts_without_reverse_imports():
    script = """
import sys
from execution_plane_client.transport import DispatchReceipt, Transport
from execution_plane_transport_journal.runtime import JournalRuntimeFactory

assert DispatchReceipt is not None
assert Transport is not None
assert JournalRuntimeFactory is not None
assert not any(name.startswith('execution_plane_client.journal') for name in sys.modules)
assert 'execution_plane_client.transport' in sys.modules
"""
    subprocess.run([sys.executable, "-c", script], check=True)


def test_direct_transport_import_has_no_journal_or_database_dependency():
    script = """
import sys
from execution_plane_transport_direct import DirectRuntimeFactory, HttpCommandTransport

assert DirectRuntimeFactory is not None
assert HttpCommandTransport is not None
assert 'httpx' in sys.modules
for name in ('asyncpg', 'sqlalchemy', 'sqlmodel', 'execution_plane_transport_journal'):
    assert not any(module == name or module.startswith(name + '.') for module in sys.modules)
"""
    subprocess.run([sys.executable, "-c", script], check=True)
