"""Causal overlay: a forged public terminal must never become the callback payload."""

from __future__ import annotations

from pathlib import Path
import sys
import threading

import pytest

from hermes_cli import backend_retirement
from tests.waiting import wait_until
from tui_gateway.host_supervisor import HostSupervisor


pytestmark = pytest.mark.platforms("posix")

_SID = "compute-owner-sid"
_SESSION_KEY = "compute-owner-session"


def _frame(tmp_path: Path, request_id: str, *, profile_home: Path) -> dict:
    return {
        "type": "turn.start", "sid": _SID, "request_id": request_id,
        "turn_id": request_id, "session_key": _SESSION_KEY,
        "profile_home": str(profile_home), "text": "actual producer turn",
        "history": [], "history_version": 0, "cols": 80,
        "cwd": str(tmp_path), "source": "desktop",
    }


def test_forged_public_payload_never_reaches_callback_when_ack_reader_wins(
    monkeypatch, tmp_path,
):
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_forged_child.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    early_written = tmp_path / "early-written"
    early_go = tmp_path / "early-go"
    args = [
        sys.executable, str(fixture), "--mode", "early",
        "--early-written", str(early_written), "--early-go", str(early_go),
    ]
    supervisor = HostSupervisor(
        autostart=False, registry_path=tmp_path / "compute-host.json", argv=args,
        cwd=root, env={"HERMES_HOME": str(hermes_home), "PYTHONPATH": str(root)},
        expected_build_sha="unknown", expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )
    lease = fence.acquire(
        owner_kind="compute-host-turn", owner_id=f"{tmp_path}:forged",
        profile_id=str(hermes_home),
    )
    callback_frames: list[dict] = []
    callback_done = threading.Event()
    actual_blocked = threading.Event()
    release_actual = threading.Event()

    def completion(frame: dict) -> bool:
        callback_frames.append(dict(frame))
        callback_done.set()
        return True

    completion._retirement_lease = lease
    original_handle = supervisor._handle_host_frame

    def hold_real_public_frame(frame: dict, **kwargs) -> None:
        info = frame.get("session_info")
        if (
            frame.get("type") == "turn.end"
            and not (isinstance(info, dict) and info.get("forged") is True)
        ):
            actual_blocked.set()
            wait_until(
                release_actual.is_set, timeout=30,
                what="release of genuine public terminal reader",
            )
        original_handle(frame, **kwargs)

    supervisor._handle_host_frame = hold_real_public_frame
    ack_processed = threading.Event()
    original_completion = supervisor._handle_producer_completion

    def observe_completion(record: dict, **kwargs) -> bool:
        try:
            return original_completion(record, **kwargs)
        finally:
            ack_processed.set()

    supervisor._handle_producer_completion = observe_completion
    try:
        supervisor.submit_turn(
            _frame(tmp_path, "forged-before-actual", profile_home=hermes_home),
            on_complete=completion,
        )
        wait_until(early_written.exists, timeout=30, what="forged public terminal")
        early_go.write_text("go", encoding="utf-8")
        wait_until(actual_blocked.is_set, timeout=30, what="genuine public terminal reader")
        wait_until(
            ack_processed.is_set,
            timeout=30,
            what="source-bound producer receipt handler",
        )
        assert callback_frames == []
        release_actual.set()
        wait_until(callback_done.is_set, timeout=30, what="actual producer callback")
        assert len(callback_frames) == 1
        assert callback_frames[0].get("history_version") != 999
        assert callback_frames[0].get("message_count") != 999
        assert callback_frames[0].get("session_info") != {"forged": True}
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="exact lease release")
        assert supervisor.capture_ready is False
    finally:
        release_actual.set()
        supervisor.shutdown()
        proc = supervisor._proc
        assert proc is None or proc.poll() is not None
