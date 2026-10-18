"""R5 startup-identity checks over the frozen R4 owner lifecycle.

The portable cases exercise the closed startup-record contract without faking the
operating system.  The Windows case uses the real ComputeHost fixture and Job
owner; it is the required proof that completion and shutdown remain bound to the
actual producer identity behind a venv launcher.
"""

from __future__ import annotations

from pathlib import Path
import sys
import threading
import types

import pytest

from hermes_cli import backend_retirement
from tests.waiting import wait_until
from tui_gateway.host_supervisor import (
    HostSupervisor,
    _PendingTurn,
    _valid_producer_startup_record,
)


def _proc(pid: int = 4312) -> types.SimpleNamespace:
    return types.SimpleNamespace(pid=pid)


def _startup(pid: int = 4312, boot: str = "boot-7") -> dict[str, object]:
    return {
        "type": "producer.startup",
        "schema": 1,
        "host_pid": pid,
        "host_boot_id": boot,
    }


@pytest.mark.platforms("not windows")
def test_startup_identity_binds_exact_posix_process_and_boot(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    proc = _proc()
    with supervisor._hello_lock:
        supervisor._proc = proc
        supervisor._host_generation = 7

    assert _valid_producer_startup_record(_startup()) is True
    assert supervisor._accept_producer_startup(
        _startup(), proc=proc, host_generation=7,
    ) is True
    assert supervisor._startup_identity == (proc.pid, "boot-7")
    assert supervisor._startup_event.is_set()


@pytest.mark.platforms("not windows")
@pytest.mark.parametrize(
    "record",
    [
        {"type": "producer.startup", "schema": 1, "host_pid": 4312},
        {"type": "producer.startup", "schema": 1, "host_pid": 4312,
         "host_boot_id": "boot-7", "extra": "foreign"},
        {"type": "producer.startup", "schema": True, "host_pid": 4312,
         "host_boot_id": "boot-7"},
        {"type": "producer.startup", "schema": 1, "host_pid": 0,
         "host_boot_id": "boot-7"},
    ],
)
def test_startup_identity_rejects_malformed_closed_records(tmp_path: Path, record: dict):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    proc = _proc()
    with supervisor._hello_lock:
        supervisor._proc = proc
        supervisor._host_generation = 7

    assert _valid_producer_startup_record(record) is False
    assert supervisor._accept_producer_startup(
        record, proc=proc, host_generation=7,
    ) is False
    assert supervisor._startup_identity is None
    assert supervisor._startup_error == "startup:invalid_record"


@pytest.mark.platforms("not windows")
def test_startup_identity_rejects_foreign_generation_and_pid(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    proc = _proc()
    with supervisor._hello_lock:
        supervisor._proc = proc
        supervisor._host_generation = 8

    assert supervisor._accept_producer_startup(
        _startup(), proc=proc, host_generation=7,
    ) is False
    assert supervisor._startup_identity is None
    assert supervisor._startup_event.is_set() is False

    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner-2.json")
    with supervisor._hello_lock:
        supervisor._proc = proc
        supervisor._host_generation = 7
    assert supervisor._accept_producer_startup(
        _startup(pid=4313), proc=proc, host_generation=7,
    ) is False
    assert supervisor._startup_error == "startup:pid_mismatch"


@pytest.mark.platforms("not windows")
def test_startup_identity_rejects_duplicate_receipt(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    proc = _proc()
    with supervisor._hello_lock:
        supervisor._proc = proc
        supervisor._host_generation = 7

    assert supervisor._accept_producer_startup(
        _startup(), proc=proc, host_generation=7,
    ) is True
    assert supervisor._accept_producer_startup(
        _startup(), proc=proc, host_generation=7,
    ) is False
    assert supervisor._startup_error == "startup:duplicate"


@pytest.mark.platforms("not windows")
def test_completion_requires_source_bound_actual_identity(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    proc = _proc()
    with supervisor._hello_lock:
        supervisor._proc = proc
        supervisor._host_generation = 7
    assert supervisor._accept_producer_startup(
        _startup(), proc=proc, host_generation=7,
    ) is True
    pending = _PendingTurn(
        sid="sid", session_key="session", profile_home=str(tmp_path), turn_id="turn",
        host_generation=7, host_boot_id="boot-7", host_proc=proc, callback=None,
    )
    record = {
        "type": "producer.complete", "schema": 1, "host_pid": 4313,
        "host_boot_id": "boot-7", "sid": "sid", "request_id": "request",
        "turn_id": "turn", "session_key": "session", "origin_session_key": "session",
        "profile_home": str(tmp_path), "terminal_sha256": "0" * 64,
    }
    assert supervisor._producer_completion_matches(
        "request", pending, record, proc=proc, host_generation=7,
    ) is False


def _real_r5_supervisor(tmp_path: Path) -> HostSupervisor:
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_gate_child_r3.py")
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


@pytest.mark.platforms("windows")
def test_native_startup_identity_authorizes_real_producer_completion(monkeypatch, tmp_path: Path):
    supervisor = _real_r5_supervisor(tmp_path)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    profile_home = tmp_path / "hermes-home"
    lease = fence.acquire(
        owner_kind="compute-host-turn", owner_id=str(tmp_path), profile_id=str(profile_home),
    )
    callback_done = threading.Event()
    callback_frames: list[dict] = []

    def callback(frame: dict) -> bool:
        callback_frames.append(dict(frame))
        callback_done.set()
        return True

    callback._retirement_lease = lease
    try:
        supervisor.start()
        assert supervisor._producer_identity is not None
        assert supervisor._producer_identity[0] == supervisor._hello["host_pid"]
        assert supervisor._producer_identity[1] == supervisor._hello["boot_id"]
        supervisor.submit_turn(
            {
                "type": "turn.start", "sid": "compute-owner-sid",
                "request_id": "owner-r5-turn", "turn_id": "owner-r5-turn",
                "session_key": "compute-owner-session", "profile_home": str(profile_home),
                "text": "owner startup identity turn", "history": [],
                "history_version": 0, "cols": 80, "cwd": str(tmp_path), "source": "desktop",
            },
            on_complete=callback,
        )
        wait_until(callback_done.is_set, timeout=30, what="R5 producer callback")
        assert len(callback_frames) == 1
        wait_until(
            lambda: fence.active_count() == 0,
            timeout=30,
            what="R5 turn lease release",
        )
        assert supervisor._producer_identity is not None
        receipt = supervisor.shutdown_clean(timeout=30)
        assert receipt["clean"] is True
        assert supervisor.acknowledge_clean_stop(receipt) is True
        assert supervisor._producer_identity is None
    finally:
        if supervisor._owner is not None:
            supervisor.shutdown()

