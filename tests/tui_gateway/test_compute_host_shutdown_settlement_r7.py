"""Real child/parent custody proof for a shutdown-wins turn-start refusal."""

from __future__ import annotations

import signal
from pathlib import Path
import sys
import threading

import pytest

from hermes_cli import backend_retirement
from tests.waiting import wait_until
from tui_gateway.host_supervisor import HostSupervisor, _PendingTurn


pytestmark = pytest.mark.platforms("posix")

_SID = "r7-shutdown-sid"
_SESSION_KEY = "r7-shutdown-session"


def _frame(tmp_path: Path, request_id: str) -> dict:
    return {
        "type": "turn.start",
        "sid": _SID,
        "request_id": request_id,
        "turn_id": request_id,
        "session_key": _SESSION_KEY,
        "profile_home": "",
        "text": "r7 delayed shutdown refusal",
        "history": [],
        "history_version": 0,
        "cols": 80,
        "cwd": str(tmp_path),
        "source": "desktop",
    }


def _supervisor(
    monkeypatch, tmp_path: Path, *, gate_before_admission: bool = False,
    shutdown_wait: float | None = None, admission_expired: bool = False,
):
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_shutdown_admission_r7_child.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    paths = {
        "turn-entered": tmp_path / "turn-entered",
        "allow-turn": tmp_path / "allow-turn",
        "shutdown-entered": tmp_path / "shutdown-entered",
    }
    args = [
        sys.executable,
        str(fixture),
        "--turn-entered",
        str(paths["turn-entered"]),
        "--allow-turn",
        str(paths["allow-turn"]),
        "--shutdown-entered",
        str(paths["shutdown-entered"]),
    ]
    if gate_before_admission:
        paths["frame-received"] = tmp_path / "frame-received"
        paths["allow-frame"] = tmp_path / "allow-frame"
        args.extend([
            "--frame-received", str(paths["frame-received"]),
            "--allow-frame", str(paths["allow-frame"]),
            "--gate-before-admission",
        ])
    if shutdown_wait is not None:
        args.extend(["--shutdown-wait", str(shutdown_wait)])
    if admission_expired:
        paths["admission-expired"] = tmp_path / "admission-expired"
        args.extend(["--admission-expired", str(paths["admission-expired"])])
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=args,
        cwd=root,
        env={"HERMES_HOME": str(hermes_home), "PYTHONPATH": str(root)},
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )
    return fence, supervisor, paths


def test_shutdown_wins_after_turn_frame_entry_settles_registered_lease(
    monkeypatch, tmp_path: Path,
):
    """The refusal receipt arrives before close, while no stdout-only release is allowed."""
    fence, supervisor, paths = _supervisor(monkeypatch, tmp_path)
    lease = fence.acquire(
        owner_kind="compute-host-turn",
        owner_id=f"{tmp_path}:r7-shutdown",
        profile_id="",
    )
    callback_frames: list[dict] = []
    callback_done = threading.Event()

    def completion(frame: dict) -> bool:
        callback_frames.append(dict(frame))
        callback_done.set()
        return True

    completion._retirement_lease = lease
    request_id = "r7-shutdown-turn"
    proc = None
    try:
        supervisor.submit_turn(_frame(tmp_path, request_id), on_complete=completion)
        wait_until(paths["turn-entered"].exists, timeout=30, what="child turn admission entry")
        pending = supervisor._pending_turns.get(request_id)
        assert isinstance(pending, _PendingTurn)
        assert pending.producer_settled is False
        assert callback_frames == []
        assert fence.active_count() == 1

        # The parent has frozen new admission, while this exact already-registered
        # request remains owned by the child reader. Signal shutdown is the real
        # ComputeHost path used by the host lifecycle, not a forged stdout frame.
        with supervisor._lock:
            supervisor._closing = True
            supervisor._shutdown_admission_closed = True
            proc = supervisor._proc
        assert proc is not None
        proc.send_signal(signal.SIGTERM)
        wait_until(
            paths["shutdown-entered"].exists,
            timeout=30,
            what="child shutdown admission barrier",
        )
        assert proc.poll() is None
        assert callback_frames == []
        assert fence.active_count() == 1
        assert supervisor.has_pending_turn(_SID)

        # Let the real delayed handler observe shutdown_requested. Its turn.error
        # and private producer.complete must both be emitted before process close.
        paths["allow-turn"].write_text("release", encoding="utf-8")
        wait_until(callback_done.is_set, timeout=30, what="authenticated refusal callback")
        assert len(callback_frames) == 1
        assert callback_frames[0]["type"] == "turn.error"
        assert callback_frames[0]["request_id"] == request_id
        assert callback_frames[0]["sid"] == _SID
        assert callback_frames[0]["reason"] == "shutdown_requested"
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="exact lease release")
        wait_until(lambda: not supervisor.has_pending_turn(_SID), timeout=30,
                   what="settled registered turn")
        wait_until(lambda: proc.poll() is not None, timeout=30, what="natural child exit")
        assert proc.returncode == 0
        assert supervisor.capture_ready is False
    finally:
        paths["allow-turn"].write_text("release", encoding="utf-8")
        proc = supervisor._proc
        if proc is not None and proc.poll() is None:
            supervisor._terminate_process(proc)
        supervisor.shutdown()
        if fence.active_count():
            assert fence.release(lease)


def test_stalled_admitted_handler_expires_without_clean_capture(
    monkeypatch, tmp_path: Path,
):
    """A counted handler cannot make shutdown wait forever or publish clean custody."""
    fence, supervisor, paths = _supervisor(
        monkeypatch, tmp_path, shutdown_wait=0.25, admission_expired=True)
    lease = fence.acquire(
        owner_kind="compute-host-turn",
        owner_id=f"{tmp_path}:r8-stalled",
        profile_id="",
    )
    callback_frames: list[dict] = []

    def completion(frame: dict) -> bool:
        callback_frames.append(dict(frame))
        return True

    completion._retirement_lease = lease
    request_id = "r8-stalled-turn"
    proc = None
    try:
        supervisor.submit_turn(_frame(tmp_path, request_id), on_complete=completion)
        wait_until(paths["turn-entered"].exists, timeout=30, what="child turn admission entry")
        with supervisor._lock:
            supervisor._closing = True
            supervisor._shutdown_admission_closed = True
            proc = supervisor._proc
        assert proc is not None
        proc.send_signal(signal.SIGTERM)
        wait_until(paths["shutdown-entered"].exists, timeout=5, what="child shutdown entry")
        wait_until(
            paths["admission-expired"].exists,
            timeout=3,
            what="bounded admission expiry",
        )
        wait_until(lambda: proc.poll() is not None, timeout=3, what="bounded child exit")
        assert proc.returncode == 0
        assert callback_frames == []
        assert fence.active_count() == 1
        assert supervisor.has_pending_turn(_SID)
        assert supervisor.capture_ready is False
    finally:
        paths["allow-turn"].write_text("release", encoding="utf-8")
        proc = supervisor._proc
        if proc is not None and proc.poll() is None:
            supervisor._terminate_process(proc)
        supervisor.shutdown()
        if fence.active_count():
            assert fence.release(lease)


def test_parent_registered_before_child_admission_remains_unknown(
    monkeypatch, tmp_path: Path,
):
    """A frame held before handle_frame cannot become a stdout-only lease release."""
    fence, supervisor, paths = _supervisor(
        monkeypatch, tmp_path, gate_before_admission=True)
    lease = fence.acquire(
        owner_kind="compute-host-turn",
        owner_id=f"{tmp_path}:r8-late-frame",
        profile_id="",
    )
    callback_frames: list[dict] = []

    def completion(frame: dict) -> bool:
        callback_frames.append(dict(frame))
        return True

    completion._retirement_lease = lease
    request_id = "r8-late-frame"
    proc = None
    try:
        supervisor.submit_turn(_frame(tmp_path, request_id), on_complete=completion)
        wait_until(paths["frame-received"].exists, timeout=30, what="child pre-admission frame")
        with supervisor._lock:
            supervisor._closing = True
            supervisor._shutdown_admission_closed = True
            proc = supervisor._proc
        assert proc is not None
        proc.send_signal(signal.SIGTERM)
        wait_until(paths["shutdown-entered"].exists, timeout=5, what="child shutdown entry")
        wait_until(lambda: proc.poll() is not None, timeout=5, what="natural child exit")
        assert proc.returncode == 0
        assert not paths["turn-entered"].exists()
        assert callback_frames == []
        assert fence.active_count() == 1
        assert supervisor.has_pending_turn(_SID)
        assert supervisor.capture_ready is False
    finally:
        paths["allow-frame"].write_text("release", encoding="utf-8")
        paths["allow-turn"].write_text("release", encoding="utf-8")
        proc = supervisor._proc
        if proc is not None and proc.poll() is None:
            supervisor._terminate_process(proc)
        supervisor.shutdown()
        if fence.active_count():
            assert fence.release(lease)
