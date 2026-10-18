"""Causal hello-handshake tests for the R3 supervisor lock correction.

The startup test uses the real R4 child fixture and production HostSupervisor. The
identity test uses real Popen objects only to exercise the reader's stale/foreign/
duplicate CAS; it never substitutes a fake process owner or a synthetic normal hello
for the startup path.
"""

from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

from tests.waiting import wait_until
from tui_gateway.host_supervisor import HostSupervisor


pytestmark = pytest.mark.platforms("posix")


def _real_supervisor(tmp_path: Path) -> HostSupervisor:
    root = Path(__file__).resolve().parents[2]
    fixture = root / "tests" / "tui_gateway" / "compute_host_owner_child.py"
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    return HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=[sys.executable, str(fixture), "--mode", "normal"],
        cwd=root,
        env={"HERMES_HOME": str(hermes_home), "PYTHONPATH": str(root)},
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )


def _waiting_process() -> subprocess.Popen[str]:
    return subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdin.readline()"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
    )


def _close_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is None:
        assert proc.stdin is not None
        proc.stdin.close()
        proc.wait(timeout=10)
    assert proc.poll() is not None


def test_real_host_startup_receives_hello_without_lifecycle_lock_deadlock(tmp_path):
    supervisor = _real_supervisor(tmp_path)
    try:
        supervisor.start()
        wait_until(supervisor._hello_event.is_set, timeout=30, what="real host hello")
        assert supervisor._hello.get("type") == "hello"
        assert supervisor._host_generation == 1
        assert supervisor._host_boot_id
    finally:
        supervisor.shutdown()
        assert supervisor._proc is None or supervisor._proc.poll() is not None


def test_hello_cas_rejects_foreign_stale_and_duplicate_frames(tmp_path):
    current = _waiting_process()
    foreign = _waiting_process()
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        expected_build_sha="unknown",
        expected_hermes_home="/profile/custody",
    )
    supervisor._proc = current
    supervisor._host_generation = 7
    first = {"type": "hello", "boot_id": "boot-first"}
    try:
        supervisor._handle_host_frame(first, proc=foreign, host_generation=7)
        assert not supervisor._hello_event.is_set()
        supervisor._handle_host_frame(first, proc=current, host_generation=6)
        assert not supervisor._hello_event.is_set()
        supervisor._handle_host_frame(first, proc=current, host_generation=7)
        assert supervisor._hello_event.is_set()
        supervisor._handle_host_frame(
            {"type": "hello", "boot_id": "boot-duplicate"}, proc=current, host_generation=7)
        assert supervisor._hello == first
    finally:
        _close_process(current)
        _close_process(foreign)
