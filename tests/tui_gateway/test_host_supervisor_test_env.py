"""Credential and native-runtime contracts for real HostSupervisor children."""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

from tests.tui_gateway.host_supervisor_test_env import synthetic_host_env
from tui_gateway.host_supervisor import HostSupervisor


def test_synthetic_host_env_pins_home_and_excludes_credentials(
    monkeypatch, tmp_path: Path
):
    home = tmp_path / "hermes-home"
    home.mkdir()
    repo_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("OPENAI_API_KEY", "synthetic-provider-secret")
    monkeypatch.setenv("GITHUB_TOKEN", "synthetic-github-secret")

    env = synthetic_host_env(home, repo_root)

    assert env["HERMES_HOME"] == str(home)
    assert env["PYTHONPATH"] == str(repo_root)
    assert env.get("OPENAI_API_KEY") is None
    assert env.get("GITHUB_TOKEN") is None
    assert env.get("HERMES_COMPUTE_HOST_PRODUCER_COMPLETION_FD") is None


@pytest.mark.platforms("windows")
def test_synthetic_host_env_supports_native_socket_and_compute_host(tmp_path: Path):
    home = tmp_path / "hermes-home"
    home.mkdir()
    repo_root = Path(__file__).resolve().parents[2]
    env = synthetic_host_env(home, repo_root)

    native_keys = {key.upper() for key in env}
    assert "SYSTEMROOT" in native_keys
    assert "WINDIR" in native_keys

    socket_probe = (
        "import socket; "
        "sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM); "
        "sock.close(); print('socket-ok')"
    )
    result = subprocess.run(
        [sys.executable, "-c", socket_probe],
        env=env,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=15,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "socket-ok" in result.stdout

    fixture = Path(__file__).with_name("compute_host_producer_gate_child_r3.py")
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=[sys.executable, str(fixture), "--mode", "normal"],
        cwd=repo_root,
        env=env,
        expected_build_sha="unknown",
        expected_hermes_home=str(home),
        heartbeat_secs=60,
    )
    try:
        supervisor.start()
        assert supervisor.is_running()
    finally:
        supervisor.shutdown()
