"""R2 owner/capture boundaries over the tested Gate1 producer source."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import threading
import types

import pytest

from hermes_cli import backend_retirement
from tests.waiting import wait_until
from tests.tui_gateway.host_supervisor_test_env import synthetic_host_env
from tui_gateway.host_supervisor import HostSupervisor, _StreamState


_SID = "compute-owner-sid"
_SESSION_KEY = "compute-owner-session"


def _completion_reader_state(payload: bytes) -> _StreamState:
    read_fd, write_fd = os.pipe()
    try:
        os.write(write_fd, payload)
    finally:
        os.close(write_fd)
    supervisor = HostSupervisor(autostart=False)
    state = _StreamState()
    supervisor._drain_producer_completion(
        read_fd, types.SimpleNamespace(pid=1234), 1, state,
    )
    return state


def _frame(tmp_path: Path, request_id: str, *, profile_home: Path) -> dict:
    return {
        "type": "turn.start",
        "sid": _SID,
        "request_id": request_id,
        "turn_id": request_id,
        "session_key": _SESSION_KEY,
        "profile_home": str(profile_home),
        "text": "owner handoff turn",
        "history": [],
        "history_version": 0,
        "cols": 80,
        "cwd": str(tmp_path),
        "source": "desktop",
    }


def _real_r4_supervisor(tmp_path: Path) -> tuple[backend_retirement.RetirementFence, HostSupervisor]:
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_gate_child_r3.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    fence = backend_retirement.RetirementFence()
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=[sys.executable, str(fixture), "--mode", "normal"],
        cwd=root,
        env=synthetic_host_env(hermes_home, root),
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )
    return fence, supervisor


def _submit_real_turn(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> tuple[HostSupervisor, backend_retirement.RetirementFence, Path, threading.Event, list[dict]]:
    fence, supervisor = _real_r4_supervisor(tmp_path)
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
    supervisor.start()
    supervisor.submit_turn(
        _frame(tmp_path, "owner-turn", profile_home=profile_home), on_complete=callback,
    )
    wait_until(callback_done.is_set, timeout=30, what="real producer callback")
    wait_until(lambda: fence.active_count() == 0, timeout=30, what="turn lease release")
    return supervisor, fence, profile_home, callback_done, callback_frames


@pytest.mark.platforms("windows")
def test_real_r4_producer_turn_keeps_job_and_registry_until_ack(monkeypatch, tmp_path: Path):
    """The real private producer receipt precedes, but does not replace, owner ACK."""
    supervisor, _fence, _profile_home, callback_done, callback_frames = _submit_real_turn(
        monkeypatch, tmp_path,
    )
    acked = False
    try:
        assert callback_done.is_set()
        assert len(callback_frames) == 1
        receipt = supervisor.shutdown_clean(timeout=30)
        assert receipt["status"] == "clean_stopped"
        assert receipt["clean"] is True
        assert receipt["ack_required"] is True
        assert receipt["owner_state"] == "empty"
        assert receipt["host_generation"] == 1
        assert receipt["host_boot_id"]
        assert receipt["host_pid"] > 0
        assert supervisor.capture_ready is False
        registry = json.loads((tmp_path / "compute-host.json").read_text(encoding="utf-8"))
        assert registry["owner_kind"] == "windows_job"
        assert registry["host_generation"] == receipt["host_generation"]
        assert registry["boot_id"] == receipt["host_boot_id"]
        assert supervisor._owner is not None

        assert supervisor.acknowledge_clean_stop(receipt) is True
        acked = True
        assert supervisor.capture_ready is True
        assert supervisor._owner is None
        assert not (tmp_path / "compute-host.json").exists()
        assert supervisor.acknowledge_clean_stop(receipt) is False
    finally:
        if not acked:
            supervisor.shutdown()


@pytest.mark.platforms("not windows")
def test_real_r4_producer_turn_refuses_unowned_clean_stop(monkeypatch, tmp_path: Path):
    """Natural process/pipe exit cannot substitute for a physical owner."""
    supervisor, _fence, _profile_home, callback_done, callback_frames = _submit_real_turn(
        monkeypatch, tmp_path,
    )
    try:
        assert callback_done.is_set()
        assert len(callback_frames) == 1
        result = supervisor.shutdown_clean(timeout=30)
        assert result["clean"] is False
        assert result["status"] == "owner_unavailable"
        assert result["owner_state"] == "unavailable"
        assert result["process_exited"] is True
        assert result["natural_exit"] is True
        assert result["stdout_eof"] is True
        assert result["stderr_eof"] is True
        assert result["completion_eof"] is True
        assert supervisor.capture_ready is False
    finally:
        supervisor.shutdown()


def test_reader_exception_is_not_reported_as_eof(caplog):
    """An assertion/reader error must refuse clean capture even if the thread exits."""
    class ExplodingReader:
        def readline(self, _limit):
            raise AssertionError("SYNTHETIC_OPAQUE_READER_VALUE")

    supervisor = HostSupervisor(autostart=False)
    state = _StreamState()
    proc = types.SimpleNamespace(stdout=ExplodingReader())
    supervisor._drain_stdout(proc, 1, state)
    assert state.done.is_set()
    assert state.failed is True
    assert state.eof is False
    assert "SYNTHETIC_OPAQUE_READER_VALUE" not in caplog.text


def test_reader_failure_refuses_clean_stop_even_after_owner_drain():
    failed = _StreamState(failed=True, error="stdout:AssertionError")
    failed.eof = False
    stderr = _StreamState(eof=True)
    completion = _StreamState(eof=True)
    result = HostSupervisor._stop_result(
        shutdown_sent=True,
        process_exited=True,
        natural_exit=True,
        stdout=failed,
        stderr=stderr,
        completion=completion,
        owner_state="empty",
        identity=(1, "validated-boot", 1234),
    )
    assert result["clean"] is False
    assert result["status"] == "incomplete"
    assert result["stdout_failed"] is True
    assert result["stdout_eof"] is False


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        (b"not-json\n", "completion:invalid_record"),
        (b"[]\n", "completion:non_object"),
        (b'{"request_id":"foreign"}\n', "completion:unmatched_record"),
    ],
)
def test_private_completion_reader_rejects_malformed_or_foreign_records(
    payload: bytes, error: str,
):
    state = _completion_reader_state(payload)

    assert state.done.is_set()
    assert state.failed is True
    assert state.eof is False
    assert state.error == error


def test_reader_eof_without_done_is_not_clean():
    streams = (_StreamState(eof=True), _StreamState(eof=True), _StreamState(eof=True))
    result = HostSupervisor._stop_result(
        shutdown_sent=True,
        process_exited=True,
        natural_exit=True,
        stdout=streams[0],
        stderr=streams[1],
        completion=streams[2],
        owner_state="empty",
        identity=(1, "validated-boot", 1234),
    )

    assert result["clean"] is False
    assert result["stdout_eof"] is True
    assert result["stdout_done"] is False


@pytest.mark.platforms("windows")
def test_ack_registry_failure_keeps_clean_receipt_retryable(monkeypatch, tmp_path: Path):
    """A failed registry unlink cannot publish capture readiness or consume the receipt."""
    supervisor, _fence, _profile_home, callback_done, callback_frames = _submit_real_turn(
        monkeypatch, tmp_path,
    )
    acked = False
    attempts = 0
    real_remove = supervisor._remove_registry

    def fail_once(_expected=None) -> bool:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return False
        return real_remove(_expected)

    monkeypatch.setattr(supervisor, "_remove_registry", fail_once)
    try:
        assert callback_done.is_set()
        assert len(callback_frames) == 1
        receipt = supervisor.shutdown_clean(timeout=30)
        assert receipt["clean"] is True
        assert supervisor.acknowledge_clean_stop(receipt) is False
        assert supervisor.capture_ready is False
        assert supervisor._owner is not None
        assert supervisor._pending_shutdown_receipt == receipt
        assert (tmp_path / "compute-host.json").exists()

        assert supervisor.acknowledge_clean_stop(receipt) is True
        acked = True
        assert supervisor.capture_ready is True
        assert not (tmp_path / "compute-host.json").exists()
    finally:
        if not acked:
            supervisor.shutdown()
