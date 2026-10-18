"""R6 recovery proofs for a named Windows owner Job.

The portable cases cover the closed registry/error boundary.  Windows cases use
the real named Job APIs: a dead recorded PID is recoverable only after the exact
Job is absent or its kernel active-member count is zero.  No case adopts a PID
or terminates an unrelated process.
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import uuid

import psutil
import pytest

from hermes_cli.local_runtime.processes import _WindowsJob, _WindowsJobAbsent
from tui_gateway.host_supervisor import HostSupervisor


def _job_name() -> str:
    return f"Local\\AgenteDesktop.ComputeHost.{uuid.uuid4().hex}"


def _record(name: str, pid: int = 999_999_991) -> dict[str, object]:
    return {
        "owner_record_schema": 2,
        "owner_kind": "windows_job",
        "host_pid": pid,
        "boot_id": "boot-r6",
        "host_generation": 7,
        "job_name": name,
        "job_token": name.rsplit(".", 1)[-1],
        "build_sha": "unknown",
        "started_at": 1.0,
        "argv": [sys.executable, "-m", "tui_gateway.compute_host"],
    }


@pytest.mark.platforms("not windows")
def test_unknown_or_malformed_windows_owner_record_refuses_and_retains(tmp_path: Path):
    registry = tmp_path / "owner.json"
    registry.write_text(json.dumps({"owner_kind": "windows_job", "host_pid": 999}), encoding="utf-8")
    supervisor = HostSupervisor(registry_path=registry, autostart=False)

    assert supervisor.reconcile_startup_orphan() == "owner-unavailable"
    assert registry.exists()


@pytest.mark.platforms("not windows")
def test_named_job_absence_unlink_failure_refuses_and_retains(tmp_path: Path, monkeypatch):
    registry = tmp_path / "owner.json"
    registry.write_text(json.dumps(_record(_job_name())), encoding="utf-8")
    supervisor = HostSupervisor(registry_path=registry, autostart=False)
    monkeypatch.setattr("tui_gateway.host_supervisor._pid_alive", lambda _pid: False)
    monkeypatch.setattr(
        "tui_gateway.windows_owner_recovery._WindowsJob.open_existing",
        lambda _name: (_ for _ in ()).throw(_WindowsJobAbsent(2, "absent")),
    )
    monkeypatch.setattr(supervisor, "_remove_registry", lambda _expected=None: False)

    assert supervisor.reconcile_startup_orphan() == "owner-unavailable"
    assert registry.exists()


@pytest.mark.platforms("not windows")
def test_named_job_access_failure_refuses_without_registry_delete(tmp_path: Path, monkeypatch):
    registry = tmp_path / "owner.json"
    registry.write_text(json.dumps(_record(_job_name())), encoding="utf-8")
    supervisor = HostSupervisor(registry_path=registry, autostart=False)
    monkeypatch.setattr("tui_gateway.host_supervisor._pid_alive", lambda _pid: False)
    monkeypatch.setattr(
        "tui_gateway.windows_owner_recovery._WindowsJob.open_existing",
        lambda _name: (_ for _ in ()).throw(PermissionError("query refused")),
    )

    assert supervisor.reconcile_startup_orphan() == "owner-unavailable"
    assert registry.exists()


@pytest.mark.platforms("windows")
def test_dead_host_empty_named_job_is_recoverable(tmp_path: Path):
    name = _job_name()
    owner = _WindowsJob(name=name)
    registry = tmp_path / "owner.json"
    try:
        exited = subprocess.Popen([sys.executable, "-c", "pass"])
        dead_pid = exited.pid
        assert exited.wait(timeout=10) == 0
        registry.write_text(json.dumps(_record(name, dead_pid)), encoding="utf-8")
        supervisor = HostSupervisor(registry_path=registry, autostart=False)

        assert supervisor.reconcile_startup_orphan() == "owner-unavailable-absent"
        assert not registry.exists()
    finally:
        owner.close()


@pytest.mark.platforms("windows")
def test_dead_recorded_host_with_live_named_job_refuses_without_kill(tmp_path: Path):
    name = _job_name()
    owner = _WindowsJob(name=name)
    child = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        creationflags=0x00000004,  # CREATE_SUSPENDED: assign before user code runs
    )
    supervisor = None
    try:
        owner.assign(child)
        psutil.Process(child.pid).resume()
        exited = subprocess.Popen([sys.executable, "-c", "pass"])
        dead_pid = exited.pid
        assert exited.wait(timeout=10) == 0
        registry = tmp_path / "owner.json"
        registry.write_text(json.dumps(_record(name, dead_pid)), encoding="utf-8")
        supervisor = HostSupervisor(registry_path=registry, autostart=False)

        assert supervisor.reconcile_startup_orphan() == "owner-unavailable"
        assert registry.exists()
        assert child.poll() is None
    finally:
        # Recovery intentionally retains its own handle to this test's live Job. Close that
        # exact retained handle too, so the last test-owned Job close can settle its child.
        try:
            recovery_owner = getattr(supervisor, "_recovery_owner", None)
            if recovery_owner is not None:
                recovery_owner.close()
        finally:
            owner.close()
            child.wait(timeout=10)

