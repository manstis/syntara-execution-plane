"""Build and install actual artifacts to catch dependency and source leakage."""

import json
import os
import subprocess
import tarfile
import zipfile
from pathlib import Path

import pytest


@pytest.mark.parametrize("editable", [False, True])
def test_distribution_installs_without_application_source(tmp_path, editable):
    root = Path(__file__).resolve().parents[2]
    client = root / "src/execution_plane_client"
    assert (client / "pyproject.toml").is_file(), "Client must be an independent project"
    artifacts = tmp_path / "artifacts"
    subprocess.run(["uv", "build", str(client), "--out-dir", str(artifacts)], check=True, capture_output=True)
    wheel = next(artifacts.glob("*.whl"))
    with zipfile.ZipFile(wheel) as archive:
        members = archive.namelist()
        assert "execution_plane_client/protocol.py" in members
        assert "execution_plane_client/py.typed" in members
        assert not any(name.startswith(("execution_plane/", "syntara/")) for name in members)
    sdist = next(artifacts.glob("*.tar.gz"))
    unpacked = tmp_path / "sdist"
    with tarfile.open(sdist) as archive:
        archive.extractall(unpacked, filter="data")
    source = next(unpacked.iterdir())
    subprocess.run(
        ["uv", "build", str(source), "--wheel", "--out-dir", str(tmp_path / "rebuilt")], check=True, capture_output=True
    )
    python = tmp_path / "isolated/bin/python"
    subprocess.run(["uv", "venv", str(python.parent.parent)], check=True, capture_output=True)
    requirement = ["--editable", str(client)] if editable else [str(wheel)]
    subprocess.run(["uv", "pip", "install", "--python", str(python), *requirement], check=True, capture_output=True)
    env = {key: value for key, value in os.environ.items() if not key.startswith(("EP_", "SYNTARA_", "PYTHONPATH"))}
    probe = subprocess.run(
        [
            str(python),
            "-I",
            "-c",
            "import json; from importlib.metadata import distributions; "
            "from execution_plane_client.protocol import MessageEnvelope; "
            "print(json.dumps(sorted(d.metadata['Name'].lower() for d in distributions())))",
        ],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    installed = set(json.loads(probe.stdout))
    assert probe.stderr == ""
    assert "execution-plane-client" in installed
    assert not installed.intersection({"execution-plane", "syntara", "kubernetes", "grpcio", "temporalio"})
    service_artifacts = tmp_path / "service"
    subprocess.run(["uv", "build", str(root), "--out-dir", str(service_artifacts)], check=True, capture_output=True)
    with zipfile.ZipFile(next(service_artifacts.glob("*.whl"))) as archive:
        assert not any(name.startswith("execution_plane_client/") for name in archive.namelist())


def test_service_sdist_contains_its_declared_client_path(tmp_path):
    root = Path(__file__).resolve().parents[2]
    subprocess.run(["uv", "build", str(root), "--sdist", "--out-dir", str(tmp_path)], check=True, capture_output=True)
    with tarfile.open(next(tmp_path.glob("*.tar.gz"))) as archive:
        names = archive.getnames()
    assert any(name.endswith("/src/execution_plane_client/pyproject.toml") for name in names)
