"""Verify paired-checkout certificate issuance using only disposable files."""

import shutil
import subprocess
import sys
from pathlib import Path

from cryptography import x509


def test_external_ca_issues_ep_identity_without_copying_private_ca_key(tmp_path):
    script = Path(__file__).resolve().parents[1] / "tools/generate_certs.py"
    for name in ("ao", "ep", "other"):
        tools = tmp_path / name / "tools"
        tools.mkdir(parents=True)
        shutil.copyfile(script, tools / script.name)
    ca_script = tmp_path / "ao/tools/generate_certs.py"
    ep_script = tmp_path / "ep/tools/generate_certs.py"
    subprocess.run([sys.executable, str(ca_script)], check=True, capture_output=True)
    ca = tmp_path / "ao/.secrets/certs"
    original_key = (ca / "ca.key").read_bytes()
    command = [sys.executable, str(ep_script), "--ca-cert", str(ca / "ca.pem"), "--ca-key", str(ca / "ca.key")]
    subprocess.run(command, check=True, capture_output=True)
    issued = tmp_path / "ep/.secrets/certs"
    certificate = x509.load_pem_x509_certificate((issued / "execution-plane.crt").read_bytes())
    certificate.verify_directly_issued_by(x509.load_pem_x509_certificate((ca / "ca.pem").read_bytes()))
    assert not (issued / "ca.key").exists()
    assert (ca / "ca.key").read_bytes() == original_key
    assert subprocess.run([sys.executable, str(ep_script)], check=False, capture_output=True).returncode != 0
    subprocess.run([sys.executable, str(tmp_path / "other/tools/generate_certs.py")], check=True, capture_output=True)
    other_ca = tmp_path / "other/.secrets/certs"
    changed = [
        sys.executable,
        str(ep_script),
        "--ca-cert",
        str(other_ca / "ca.pem"),
        "--ca-key",
        str(other_ca / "ca.key"),
    ]
    assert subprocess.run(changed, check=False, capture_output=True).returncode != 0
    subprocess.run([*changed, "--force"], check=True, capture_output=True)
    rotated = x509.load_pem_x509_certificate((issued / "execution-plane.crt").read_bytes())
    rotated.verify_directly_issued_by(x509.load_pem_x509_certificate((other_ca / "ca.pem").read_bytes()))
