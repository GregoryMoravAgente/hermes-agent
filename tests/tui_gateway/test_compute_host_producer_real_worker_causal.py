"""Executable causal coverage for the real production prompt worker.

The real-worker case uses the existing synthetic AIAgent factory only as a local provider seam.
The submit function, session admission, worker reservation/thread, ComputeHost join, producer
completion channel, and HostSupervisor callback gate remain production code. The two no-worker
cases are intentionally RED on frozen R3b: ``_run_prompt_submit`` returns False, yet the host
currently emits ``turn.end``.
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import threading

import pytest

from hermes_cli import backend_retirement
from tests.waiting import wait_until
from tui_gateway.host_supervisor import HostSupervisor


pytestmark = pytest.mark.platforms("posix")

_SID = "real-production-worker-sid"
_SESSION_KEY = "real-production-worker-session"


def _frame(tmp_path: Path, request_id: str) -> dict:
    return {
        "type": "turn.start",
        "sid": _SID,
        "request_id": request_id,
        "turn_id": request_id,
        "session_key": _SESSION_KEY,
        "profile_home": "",
        "text": "real production worker causal turn",
        "history": [],
        "history_version": 0,
        "cols": 80,
        "cwd": str(tmp_path),
        "source": "desktop",
        # This is the documented child-side inert borrow of the parent's accepted slot.
        "active_session_lease": {
            "session_id": _SESSION_KEY,
            "lease_id": "parent-test-lease",
            "source": "desktop",
        },
    }


def _supervisor(monkeypatch, tmp_path: Path, mode: str):
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_real_worker_child.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    paths = {
        "hermes-home": hermes_home,
        "worker-release": tmp_path / "worker-release",
        "worker-started": tmp_path / "worker-started",
        "worker-exited": tmp_path / "worker-exited",
        "session-removed": tmp_path / "session-removed",
        "submit-return": tmp_path / "submit-return.json",
    }
    args = [
        sys.executable, str(fixture), "--mode", mode,
        "--worker-release", str(paths["worker-release"]),
        "--worker-started", str(paths["worker-started"]),
        "--worker-exited", str(paths["worker-exited"]),
        "--session-removed", str(paths["session-removed"]),
        "--submit-return", str(paths["submit-return"]),
    ]
    rpc_frames: list[dict] = []
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=args,
        cwd=root,
        env={
            "HERMES_HOME": str(hermes_home),
            "PYTHONPATH": str(root),
            "HERMES_ISO_CERTIFY_SYNTH_TURN": "1",
        },
        rpc_sink=lambda frame: rpc_frames.append(dict(frame)),
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )
    return fence, supervisor, paths, rpc_frames


def _submit_with_lease(fence, supervisor, frame, callback_frames, callback_done):
    lease = fence.acquire(
        owner_kind="compute-host-real-worker-test",
        owner_id=f"{frame['request_id']}-owner",
        profile_id="",
    )

    def completion(receipt: dict) -> bool:
        callback_frames.append(dict(receipt))
        callback_done.set()
        return True

    completion._retirement_lease = lease
    supervisor.submit_turn(frame, on_complete=completion)


def test_real_production_worker_removal_exception_waits_for_natural_exit(monkeypatch, tmp_path):
    """A real accepted worker may disappear from the registry without an early callback or receipt."""
    fence, supervisor, paths, rpc_frames = _supervisor(monkeypatch, tmp_path, "real-worker")
    callback_frames: list[dict] = []
    callback_done = threading.Event()
    try:
        _submit_with_lease(
            fence, supervisor, _frame(tmp_path, "real-worker-turn"), callback_frames, callback_done)
        wait_until(paths["worker-started"].exists, timeout=30, what="real production worker start")
        wait_until(paths["session-removed"].exists, timeout=30, what="real session removal")
        assert paths["session-removed"].read_text(encoding="utf-8") == "removed"
        wait_until(paths["submit-return"].exists, timeout=30, what="real production submit result")
        assert json.loads(paths["submit-return"].read_text(encoding="utf-8"))["accepted"] is True
        assert callback_frames == []
        assert supervisor.has_pending_turn(_SID)
        assert supervisor.capture_ready is False
        assert not paths["worker-exited"].exists()

        paths["worker-release"].write_text("release", encoding="utf-8")
        wait_until(paths["worker-exited"].exists, timeout=30, what="real worker natural exit")
        wait_until(callback_done.is_set, timeout=30, what="post-exit supervisor callback")
        assert len(callback_frames) == 1
        # The agent caught the provider exception inside the production prompt thread; the host
        # terminal is a transport boundary, while the relayed message.complete carries status=error.
        assert callback_frames[0]["type"] == "turn.end"
        assert not supervisor.has_pending_turn(_SID)
        assert fence.active_count() == 0
        assert any(
            frame.get("params", {}).get("type") == "message.complete"
            and frame.get("params", {}).get("payload", {}).get("status") == "error"
            for frame in rpc_frames
            if isinstance(frame, dict)
        )
    finally:
        paths["worker-release"].write_text("release", encoding="utf-8")
        supervisor.shutdown()
        proc = supervisor._proc
        assert proc is None or proc.poll() is not None


@pytest.mark.parametrize("mode", ["no-agent", "closing"])
def test_prompt_refusal_does_not_become_a_completed_turn_end(monkeypatch, tmp_path, mode):
    """False from the real submit contract must settle the host as an error, never ``turn.end``."""
    fence, supervisor, paths, _rpc_frames = _supervisor(monkeypatch, tmp_path, mode)
    callback_frames: list[dict] = []
    callback_done = threading.Event()
    try:
        _submit_with_lease(
            fence, supervisor, _frame(tmp_path, f"{mode}-turn"), callback_frames, callback_done)
        wait_until(paths["submit-return"].exists, timeout=30, what=f"{mode} submit refusal")
        assert json.loads(paths["submit-return"].read_text(encoding="utf-8"))["accepted"] is False
        wait_until(callback_done.is_set, timeout=30, what=f"{mode} host refusal callback")
        assert len(callback_frames) == 1
        assert callback_frames[0]["type"] == "turn.error"
        assert not paths["worker-started"].exists()
        assert not paths["worker-exited"].exists()
        assert not paths["session-removed"].exists()
        assert not supervisor.has_pending_turn(_SID)
        assert fence.active_count() == 0
    finally:
        paths["worker-release"].write_text("release", encoding="utf-8")
        supervisor.shutdown()
        proc = supervisor._proc
        assert proc is None or proc.poll() is not None
