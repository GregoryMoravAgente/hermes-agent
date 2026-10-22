"""Portable state-contract RED overlay for the Windows owner R4 proposal.

These tests do not claim native Job proof.  The existing real Windows producer and
Job tests remain the authority for Popen/Job/descendant behavior; this overlay only
forces the parent lifecycle races that must be refused or retained before that proof.
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
from tui_gateway.host_supervisor import (
    HostSupervisor,
    _PendingTurn,
    _StreamState,
    _pid_alive,
)


class _FakeOwner:
    def __init__(self, *, close_failures: int = 0) -> None:
        self.close_failures = close_failures
        self.close_calls = 0

    def empty(self) -> bool:
        return True

    def close(self) -> None:
        self.close_calls += 1
        if self.close_calls <= self.close_failures:
            raise OSError("synthetic native close failure")


class _FakeStdin:
    def __init__(self) -> None:
        self.payloads: list[str] = []

    def write(self, payload: str) -> int:
        self.payloads.append(payload)
        return len(payload)

    def flush(self) -> None:
        return None


def _fake_proc(pid: int = 4312, *, exited: bool = False):
    return types.SimpleNamespace(
        pid=pid,
        returncode=0 if exited else None,
        poll=lambda: 0 if exited else None,
        stdin=_FakeStdin(),
    )


def _seed_parent_state(supervisor: HostSupervisor, tmp_path: Path, *, exited: bool = False):
    proc = _fake_proc(exited=exited)
    owner = _FakeOwner()
    states = (_StreamState(eof=True), _StreamState(eof=True), _StreamState(eof=True))
    for state in states:
        state.done.set()
    with supervisor._lock:
        supervisor._proc = proc
        supervisor._owner = owner
        supervisor._owner_identity = (7, "boot-7", proc.pid)
        supervisor._producer_identity = (proc.pid, "boot-7")
        # This is only the closed typed state contract for portable ACK cases. Native
        # handle/FILETIME/Job validity remains exclusively the real R7 Windows proof.
        supervisor._producer_process_handle = types.SimpleNamespace(
            pid=proc.pid, creation_filetime=77, is_open=True, close=lambda: None,
        )
        supervisor._producer_creation_filetime = 77
        supervisor._stream_states[id(proc)] = states
        if not hasattr(supervisor, "_exit_settled"):
            supervisor._exit_settled = {}
        supervisor._shutdown_ack = None
        supervisor._exit_settled[id(proc)] = types.SimpleNamespace(
            is_set=lambda: exited,
            wait=lambda timeout=None: exited,
        )
    return proc, owner


def _real_r4_supervisor(
    tmp_path: Path, *, ack_gate_paths: tuple[Path, Path] | None = None,
) -> HostSupervisor:
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_gate_child_r3.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    argv = [sys.executable, str(fixture), "--mode", "normal"]
    if ack_gate_paths is not None:
        ready, release = ack_gate_paths
        argv.extend((
            "--shutdown-ack-ready", str(ready),
            "--shutdown-ack-release", str(release),
        ))
    return HostSupervisor(
        autostart=False, registry_path=tmp_path / "compute-host.json", argv=argv,
        cwd=root, env={"HERMES_HOME": str(hermes_home), "PYTHONPATH": str(root)},
        expected_build_sha="unknown", expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )


def test_pending_turn_refuses_shutdown_before_any_destructive_frame(monkeypatch, tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    proc, _owner = _seed_parent_state(supervisor, tmp_path)
    pending = _PendingTurn(
        sid="sid", session_key="session", profile_home=str(tmp_path), turn_id="turn",
        host_generation=7, host_boot_id="boot-7", host_proc=proc, callback=None,
        retirement_lease=object(),
    )
    with supervisor._lock:
        supervisor._pending_turns["turn"] = pending
    sent: list[dict] = []
    monkeypatch.setattr(supervisor, "_send_frame", lambda frame, **_kw: sent.append(frame))

    result = supervisor.shutdown_clean(timeout=0)

    assert sent == []
    assert result["clean"] is False
    assert result["status"] == "pending_work"
    assert result["shutdown_sent"] is False
    assert result["pending_turns"] == 1
    assert supervisor._shutdown_admission_closed is True
    assert supervisor._pending_turns["turn"] is pending


def test_shutdown_gate_refuses_new_turn_but_allows_existing_response_drain(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    proc, _owner = _seed_parent_state(supervisor, tmp_path)
    with supervisor._lock:
        supervisor._closing = True
        supervisor._shutdown_admission_closed = True
        supervisor._pending_turns["turn"] = ("existing", None)

    with pytest.raises(RuntimeError, match="compute host is closing"):
        supervisor.submit_turn({"sid": "new"})

    supervisor.interrupt("existing", request_id="interrupt-1")
    assert json.loads(proc.stdin.payloads[-1]) == {
        "type": "interrupt", "sid": "existing", "request_id": "interrupt-1",
    }
    with pytest.raises(RuntimeError, match="existing response refused"):
        supervisor.interrupt("other", request_id="interrupt-2")


def test_ack_race_requires_exit_settlement_and_zero_pending_records(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    proc, owner = _seed_parent_state(supervisor, tmp_path, exited=True)
    receipt = {
        "status": "clean_stopped", "clean": True, "ack_required": True,
        "host_generation": 7, "host_boot_id": "boot-7", "host_pid": proc.pid,
    }
    with supervisor._lock:
        supervisor._pending_shutdown_receipt = dict(receipt)
        supervisor._exit_settled[id(proc)] = types.SimpleNamespace(
            is_set=lambda: False, wait=lambda timeout=None: False,
        )

    assert supervisor.acknowledge_clean_stop(receipt) is False
    with supervisor._lock:
        supervisor._pending_turns["late"] = _PendingTurn(
            sid="sid", session_key="session", profile_home=str(tmp_path), turn_id="late",
            host_generation=7, host_boot_id="boot-7", host_proc=proc, callback=None,
            retirement_lease=object(),
        )

    assert supervisor.acknowledge_clean_stop(receipt) is False
    with supervisor._lock:
        supervisor._exit_settled[id(proc)] = types.SimpleNamespace(
            is_set=lambda: True, wait=lambda timeout=None: True,
        )
    assert supervisor.acknowledge_clean_stop(receipt) is False
    with supervisor._lock:
        supervisor._pending_turns.clear()
    monkeypatch_remove = lambda *_args, **_kwargs: True
    supervisor._remove_registry = monkeypatch_remove
    assert supervisor.acknowledge_clean_stop(receipt) is True
    assert owner.close_calls == 1


def test_missing_shutdown_ack_is_never_a_clean_receipt():
    states = (_StreamState(eof=True), _StreamState(eof=True), _StreamState(eof=True))
    for state in states:
        state.done.set()
    result = HostSupervisor._stop_result(
        shutdown_sent=True, process_exited=True, natural_exit=True,
        stdout=states[0], stderr=states[1], completion=states[2], owner_state="empty",
        identity=(7, "boot-7", 4312), shutdown_ack=False, exit_settled=True,
    )
    assert result["clean"] is False
    assert result["status"] == "shutdown_ack_missing"


def test_natural_exit_without_shutdown_ack_is_not_clean(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    _proc, owner = _seed_parent_state(supervisor, tmp_path, exited=True)
    with supervisor._lock:
        supervisor._shutdown_sent = True

    result = supervisor.shutdown_clean(timeout=0)

    assert result["clean"] is False
    assert result["status"] == "shutdown_ack_missing"
    assert result["shutdown_sent"] is True
    assert result["shutdown_ack"] is False
    assert supervisor._owner is owner


def test_late_control_callback_uncertainty_blocks_shutdown(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    _proc, _owner = _seed_parent_state(supervisor, tmp_path)
    with supervisor._lock:
        supervisor._late_control_handlers["late-1"] = (0.0, None)

    result = supervisor.shutdown_clean(timeout=0)

    assert result["clean"] is False
    assert result["status"] == "pending_work"
    assert result["late_controls"] == 1
    assert result["shutdown_sent"] is False
    assert supervisor._shutdown_admission_closed is True


def test_shutdown_ack_requires_exact_process_generation_boot_and_drain(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    proc, _owner = _seed_parent_state(supervisor, tmp_path)
    with supervisor._lock:
        supervisor._host_generation = 7
        supervisor._shutdown_request_id = "shutdown-7"
    valid = {"type": "shutdown.ack", "request_id": "shutdown-7", "host_pid": proc.pid,
             "host_boot_id": "boot-7", "drained": True}
    invalid = [
        ({**valid, "drained": False}, proc, 7),
        ({**valid, "host_pid": proc.pid + 1}, proc, 7),
        ({**valid, "host_boot_id": "foreign"}, proc, 7),
        ({**valid, "request_id": "foreign"}, proc, 7),
        (valid, types.SimpleNamespace(pid=proc.pid), 7),
        (valid, proc, 8),
    ]
    for frame, source, generation in invalid:
        supervisor._handle_host_frame(frame, proc=source, host_generation=generation)
        assert supervisor._shutdown_ack is None
    supervisor._handle_host_frame(valid, proc=proc, host_generation=7)
    assert supervisor._shutdown_ack == valid


def test_owner_close_failure_retains_receipt_and_retries(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    proc, owner = _seed_parent_state(supervisor, tmp_path, exited=True)
    owner.close_failures = 1
    receipt = {
        "status": "clean_stopped", "clean": True, "ack_required": True,
        "host_generation": 7, "host_boot_id": "boot-7", "host_pid": proc.pid,
    }
    with supervisor._lock:
        supervisor._pending_shutdown_receipt = dict(receipt)
        supervisor._exit_settled[id(proc)] = types.SimpleNamespace(
            is_set=lambda: True, wait=lambda timeout=None: True,
        )
    supervisor._remove_registry = lambda *_args, **_kwargs: True

    assert supervisor.acknowledge_clean_stop(receipt) is False
    assert supervisor._pending_shutdown_receipt == receipt
    assert supervisor._owner is owner
    assert supervisor.acknowledge_clean_stop(receipt) is True
    assert owner.close_calls == 2


def test_live_windows_job_orphan_refuses_duplicate_start(tmp_path: Path, monkeypatch):
    registry = tmp_path / "owner.json"
    registry.write_text(json.dumps({
        "owner_kind": "windows_job", "host_pid": os.getpid(),
        "host_generation": 7, "boot_id": "live-boot",
    }), encoding="utf-8")
    supervisor = HostSupervisor(registry_path=registry, autostart=False)
    monkeypatch.setattr("tui_gateway.host_supervisor._pid_alive", lambda _pid: True)
    called = []
    monkeypatch.setattr(supervisor, "_spawn_locked", lambda **_kw: called.append(True))

    assert supervisor.reconcile_startup_orphan() == "owner-unavailable"
    assert registry.exists()
    with pytest.raises(RuntimeError, match="owner is still live"):
        supervisor.start()
    assert called == []



def test_destructive_owner_release_retains_identity_after_close_failure(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    _proc, owner = _seed_parent_state(supervisor, tmp_path, exited=True)
    owner.close_failures = 1
    identity = supervisor._owner_identity
    supervisor._release_owner()
    assert supervisor._owner is owner
    assert supervisor._owner_identity == identity
    assert supervisor.capture_ready is False
    assert owner.close_calls == 1
    supervisor._release_owner()
    assert supervisor._owner is None
    assert supervisor._owner_identity is None
    assert owner.close_calls == 2


@pytest.mark.platforms("windows")
def test_native_shutdown_ack_barrier_precedes_close_and_reader_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
):
    """The real child must publish ACK before close/finalization can win the race."""
    ready = tmp_path / "shutdown-ack-ready"
    release = tmp_path / "shutdown-ack-release"
    supervisor = _real_r4_supervisor(tmp_path, ack_gate_paths=(ready, release))
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
    receipt_box: list[dict] = []
    shutdown_done = threading.Event()
    stopper: threading.Thread | None = None
    acked = False
    try:
        supervisor.start()
        process_handle = supervisor._producer_process_handle
        assert process_handle is not None and process_handle.is_open
        supervisor.submit_turn(
            {
                "type": "turn.start", "sid": "compute-owner-sid",
                "request_id": "owner-r4-ack-gate", "turn_id": "owner-r4-ack-gate",
                "session_key": "compute-owner-session", "profile_home": str(profile_home),
                "text": "owner ACK barrier turn", "history": [], "history_version": 0,
                "cols": 80, "cwd": str(tmp_path), "source": "desktop",
            },
            on_complete=callback,
        )
        wait_until(callback_done.is_set, timeout=30, what="ACK barrier producer callback")
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="ACK barrier lease release")

        def stop() -> None:
            receipt_box.append(supervisor.shutdown_clean(timeout=30))
            shutdown_done.set()

        stopper = threading.Thread(target=stop, name="compute-owner-ack-barrier-stop")
        stopper.start()
        wait_until(ready.exists, timeout=30, what="real shutdown ACK write barrier")
        assert shutdown_done.is_set() is False
        assert supervisor._proc is not None and supervisor._proc.poll() is None
        assert supervisor._owner is not None
        assert supervisor.capture_ready is False
        release.write_text("release", encoding="utf-8")
        wait_until(shutdown_done.is_set, timeout=30, what="shutdown ACK barrier receipt")
        stopper.join(timeout=2)
        assert not stopper.is_alive()
        receipt = receipt_box[0]
        assert receipt["status"] == "clean_stopped"
        assert receipt["clean"] is True
        assert receipt["shutdown_ack"] is True
        assert receipt["owner_state"] == "empty"
        assert supervisor.acknowledge_clean_stop(receipt) is True
        acked = True
        assert process_handle.is_open is False
    finally:
        release.write_text("release", encoding="utf-8")
        if stopper is not None:
            stopper.join(timeout=35)
        if not acked:
            supervisor.shutdown()
        if fence.active_count():
            fence.release(lease)


@pytest.mark.platforms("windows")
def test_windows_pid_probe_does_not_signal_live_foreign_process():
    """A liveness probe must not terminate a live process outside this owner Job."""
    child = subprocess.Popen(
        [sys.executable, "-c", "input()"],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        text=True,
    )
    pid = child.pid
    try:
        assert _pid_alive(pid) is True
        assert child.poll() is None
    finally:
        child.terminate()
        child.wait(timeout=10)
    assert _pid_alive(pid) is False


def test_real_shutdown_ack_barrier_serializes_before_transport_close(tmp_path: Path):
    """Real run_host stays alive until its ACK write is released, on the actual OS."""
    ready = tmp_path / "portable-ack-ready"
    release = tmp_path / "portable-ack-release"
    supervisor = _real_r4_supervisor(tmp_path, ack_gate_paths=(ready, release))
    results = []
    errors = []
    done = threading.Event()

    def stop():
        try:
            results.append(supervisor.shutdown_clean(timeout=30))
        except BaseException as exc:
            errors.append(type(exc).__name__)
        finally:
            done.set()

    stopper = None
    try:
        supervisor.start()
        proc = supervisor._proc
        assert proc is not None
        stopper = threading.Thread(target=stop, name="portable-real-ack-stop")
        stopper.start()
        wait_until(ready.exists, timeout=15, what="real child ACK write barrier")
        # This is a bounded observation of the actual child, not a timing sleep or OS fake.
        with pytest.raises(subprocess.TimeoutExpired):
            proc.wait(timeout=1.0)
        assert done.is_set() is False
        assert supervisor.capture_ready is False
        release.write_text("release", encoding="utf-8")
        wait_until(done.is_set, timeout=30, what="real post-barrier ACK receipt")
        stopper.join(timeout=2)
        assert not stopper.is_alive()
        assert not errors
        assert len(results) == 1
        assert results[0]["shutdown_ack"] is True
        assert results[0]["natural_exit"] is True
        assert results[0]["exit_settled"] is True
        # Full clean/Job/retained-handle acceptance stays in the unchanged native R7 test.
    finally:
        release.write_text("release", encoding="utf-8")
        if stopper is not None:
            stopper.join(timeout=35)
        supervisor.shutdown()  # Exact fixture containment, never counted as clean acceptance.
