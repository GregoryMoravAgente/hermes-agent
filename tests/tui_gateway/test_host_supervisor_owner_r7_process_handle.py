"""R7 process-handle identity and registry-lease boundaries.

Portable cases cover the closed typed contract and retryable custody.  The native
case uses the real Windows Job, ComputeHost, startup pipe, callback, and ACK; it
does not treat a dead PID or a fresh Job-membership query as completion authority.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import types

import pytest

from hermes_cli import backend_retirement
from tests.waiting import wait_until
from tui_gateway.host_supervisor import HostSupervisor
from tui_gateway.windows_owner_recovery import (
    RegistryLease,
    owner_record_key,
    record_matches_key,
)
from tui_gateway.windows_producer_identity import (
    bind_producer_identity,
    valid_producer_startup_record,
)


def _windows_startup(*, pid: int = 4312, creation: int = 77) -> dict[str, object]:
    return {
        "type": "producer.startup", "schema": 2, "host_pid": pid,
        "host_boot_id": "boot-r7", "host_creation_filetime": creation,
    }


def test_windows_startup_record_is_closed_and_exact():
    assert valid_producer_startup_record(_windows_startup(), is_windows=True) is True
    assert valid_producer_startup_record(
        {**_windows_startup(), "host_creation_filetime": True}, is_windows=True
    ) is False
    assert valid_producer_startup_record(
        {key: value for key, value in _windows_startup().items() if key != "host_creation_filetime"},
        is_windows=True,
    ) is False
    assert valid_producer_startup_record(
        {**_windows_startup(), "foreign": "field"}, is_windows=True
    ) is False
    assert valid_producer_startup_record(
        {**_windows_startup(), "schema": 1}, is_windows=True
    ) is False


@pytest.mark.parametrize(
    ("pid", "creation", "in_job"),
    [(4313, 77, True), (4312, 78, True), (4312, 77, False), (4312, 77, None)],
)
def test_process_handle_binding_rejects_stale_or_unproven_identity(
    pid: int, creation: int, in_job: bool | None,
):
    process = types.SimpleNamespace(is_open=True, pid=4312, creation_filetime=77)
    assert bind_producer_identity(
        process=process, expected_pid=pid,
        expected_creation_filetime=creation, in_owned_job=in_job,
    ) is False
    process.is_open = False
    assert bind_producer_identity(
        process=process, expected_pid=4312,
        expected_creation_filetime=77, in_owned_job=True,
    ) is False


def test_failed_process_handle_close_retains_retryable_custody(tmp_path: Path):
    class _CloseFailure:
        is_open = True

        def close(self):
            raise RuntimeError("opaque native close failure")

    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    handle = _CloseFailure()
    supervisor._producer_process_handle = handle
    assert supervisor._try_close_producer_handle() is False
    assert supervisor._producer_process_handle is handle


def test_registry_delete_requires_exact_owner_key(tmp_path: Path):
    registry = tmp_path / "owner.json"
    expected = owner_record_key(
        owner_identity=(7, "boot-r7", 4312), owner_kind="windows_job",
        owner_job_name="Local\\AgenteDesktop.ComputeHost." + "a" * 32,
    )
    record = {
        "owner_record_schema": 2, "owner_kind": "windows_job", "host_pid": 4312,
        "boot_id": "foreign", "host_generation": 7,
        "job_name": expected[-1], "job_token": "a" * 32,
        "build_sha": "unknown", "started_at": 1.0, "argv": [sys.executable],
    }
    registry.write_text(json.dumps(record), encoding="utf-8")
    supervisor = HostSupervisor(autostart=False, registry_path=registry)
    supervisor._registry_owner_key = expected
    assert supervisor._acquire_registry_lease() is True
    assert record_matches_key(record, expected) is False
    assert supervisor._remove_registry() is False
    assert registry.exists()
    assert supervisor._release_registry_lease() is True


@pytest.mark.platforms("windows")
def test_native_registry_lease_blocks_competing_process(tmp_path: Path):
    registry = tmp_path / "owner.json"
    lease = RegistryLease.acquire(registry)
    assert lease is not None
    ready = tmp_path / "ready"
    release = tmp_path / "release"
    child_code = (
        "import sys,time\n"
        "from pathlib import Path\n"
        "from tui_gateway.host_supervisor import HostSupervisor\n"
        "s=HostSupervisor(registry_path=Path(sys.argv[1]), autostart=False)\n"
        "held=s._acquire_registry_lease()\n"
        "Path(sys.argv[2]).write_text('held' if held else 'refused')\n"
        "while not Path(sys.argv[3]).exists():\n"
        "    time.sleep(.05)\n"
        "held and s._release_registry_lease()\n"
    )
    root = Path(__file__).resolve().parents[2]
    env = {**os.environ, "PYTHONPATH": str(root)}
    child = subprocess.Popen(
        [sys.executable, "-c", child_code, str(registry), str(ready), str(release)],
        cwd=root, env=env,
    )
    try:
        wait_until(ready.exists, timeout=10, what="competing registry lease result")
        assert ready.read_text(encoding="utf-8") == "refused"
        release.write_text("release", encoding="utf-8")
        assert child.wait(timeout=10) == 0
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
        assert lease.release() is True


def _real_r7_supervisor(tmp_path: Path) -> HostSupervisor:
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_gate_child_r3.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    return HostSupervisor(
        autostart=False, registry_path=tmp_path / "compute-host.json",
        argv=[sys.executable, str(fixture), "--mode", "normal"], cwd=root,
        env={"HERMES_HOME": str(hermes_home), "PYTHONPATH": str(root)},
        expected_build_sha="unknown", expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )


@pytest.mark.platforms("windows")
def test_native_process_handle_binds_callback_and_ack_after_exit(monkeypatch, tmp_path: Path, record_property):
    supervisor = _real_r7_supervisor(tmp_path)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    profile_home = tmp_path / "hermes-home"
    lease = fence.acquire(
        owner_kind="compute-host-turn", owner_id=str(tmp_path), profile_id=str(profile_home),
    )
    callback_done = threading.Event()
    frames: list[dict] = []

    def callback(frame: dict) -> bool:
        frames.append(dict(frame))
        callback_done.set()
        return True

    callback._retirement_lease = lease
    try:
        supervisor.start()
        process_handle = supervisor._producer_process_handle
        assert process_handle is not None and process_handle.is_open
        assert supervisor._producer_creation_filetime is not None
        assert supervisor._owner.contains_process_handle(process_handle) is True
        record_property("launcher_pid", supervisor.pid)
        record_property("producer_pid", process_handle.pid)
        record_property("producer_creation_filetime", supervisor._producer_creation_filetime)
        record_property("exact_job_membership", True)
        supervisor.submit_turn(
            {
                "type": "turn.start", "sid": "compute-owner-sid",
                "request_id": "owner-r7-turn", "turn_id": "owner-r7-turn",
                "session_key": "compute-owner-session", "profile_home": str(profile_home),
                "text": "owner process identity turn", "history": [],
                "history_version": 0, "cols": 80, "cwd": str(tmp_path), "source": "desktop",
            },
            on_complete=callback,
        )
        wait_until(callback_done.is_set, timeout=30, what="R7 producer callback")
        assert len(frames) == 1
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="R7 turn lease release")
        receipt = supervisor.shutdown_clean(timeout=30)
        assert receipt["clean"] is True
        # The child has exited, but this retained handle still authenticates the ACK.
        assert process_handle.is_open
        assert supervisor.acknowledge_clean_stop(receipt) is True
        assert process_handle.is_open is False
    finally:
        if supervisor._owner is not None:
            supervisor.shutdown()
