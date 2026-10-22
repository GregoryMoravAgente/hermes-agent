"""R2 causal checks for terminal binding and completion-channel lifecycle.

These tests supplement the real-child gate tests. They deliberately stop at the
producer boundary: retained host/resource custody and process descendants remain
unknown and are never converted into a capture-ready result.
"""

from __future__ import annotations

import io
import os
from pathlib import Path
import sys
import threading

import pytest

from hermes_cli import backend_retirement
from tests.waiting import wait_until
from tests.tui_gateway.host_supervisor_test_env import synthetic_host_env
from tui_gateway.compute_host import ComputeHost, _write_completion_bytes
from tui_gateway.host_supervisor import (
    HostSupervisor,
    _PendingTurn,
    _valid_producer_completion_record,
)


pytestmark = pytest.mark.platforms("posix")

_SID = "compute-owner-sid"
_SESSION_KEY = "compute-owner-session"


def _held_supervisor(monkeypatch, tmp_path: Path):
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_gate_child.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    release = tmp_path / "held-worker-release"
    args = [
        sys.executable, str(fixture), "--mode", "held-exception",
        "--worker-release", str(release),
    ]
    supervisor = HostSupervisor(
        autostart=False, registry_path=tmp_path / "compute-host.json", argv=args,
        cwd=root, env=synthetic_host_env(hermes_home, root),
        expected_build_sha="unknown", expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )
    return fence, supervisor, hermes_home, release


def _frame(tmp_path: Path, request_id: str, profile_home: Path) -> dict:
    return {
        "type": "turn.start", "sid": _SID, "request_id": request_id,
        "turn_id": request_id, "session_key": _SESSION_KEY,
        "profile_home": str(profile_home), "text": "held producer turn",
        "history": [], "history_version": 0, "cols": 80,
        "cwd": str(tmp_path), "source": "desktop",
    }


def test_exception_with_live_producer_worker_keeps_completion_unsettled(
    monkeypatch, tmp_path,
):
    fence, supervisor, profile_home, release = _held_supervisor(monkeypatch, tmp_path)
    lease = fence.acquire(
        owner_kind="compute-host-turn", owner_id=f"{tmp_path}:held",
        profile_id=str(profile_home),
    )
    callback_frames: list[dict] = []
    callback_done = threading.Event()

    def completion(frame: dict) -> bool:
        callback_frames.append(frame)
        callback_done.set()
        return True

    completion._retirement_lease = lease
    request_id = "held-exception-turn"
    try:
        supervisor.submit_turn(_frame(tmp_path, request_id, profile_home), on_complete=completion)
        wait_until(
            lambda: (
                (pending := supervisor._pending_turns.get(request_id)) is not None
                and isinstance(pending, _PendingTurn)
                and bool(pending.terminal_candidates)
            ),
            timeout=30,
            what="bounded exception terminal candidate",
        )
        pending = supervisor._pending_turns[request_id]
        assert isinstance(pending, _PendingTurn)
        assert pending.producer_settled is False
        assert callback_frames == []
        assert fence.active_count() == 1
        release.write_text("release", encoding="utf-8")
        # The watcher owns the accepted worker until its real join and session
        # cleanup complete.  Shutdown must therefore happen only after the
        # source-bound exception callback settles the exact turn lease.
        wait_until(callback_done.is_set, timeout=30, what="post-worker exception callback")
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="exception turn lease release")
        wait_until(lambda: not supervisor.has_pending_turn(_SID), timeout=30,
                   what="exception pending turn removal")
        receipt = supervisor.shutdown_clean(timeout=30)
        assert receipt["clean"] is False
        # ACK serialization now succeeds, while POSIX still lacks the native
        # process-owner proof required for a clean capture. These are independent.
        assert receipt["status"] == "owner_unavailable"
        assert receipt["owner_state"] == "unavailable"
        assert receipt["process_exited"] is True
        assert receipt["natural_exit"] is True
        assert receipt["shutdown_ack"] is True
        assert receipt["exit_settled"] is True
        assert supervisor.capture_ready is False
        proc = supervisor._proc
        assert proc is None or proc.poll() is not None
    finally:
        release.write_text("release", encoding="utf-8")
        supervisor.shutdown()
        if fence.active_count():
            assert fence.release(lease)


def test_completion_close_waits_for_inflight_writer_before_closing_fd():
    read_fd, write_fd = os.pipe()
    host = ComputeHost(stdout=io.StringIO(), completion_fd=write_fd, heartbeat_secs=0)
    host._completion_lock.acquire()
    writer_entered = threading.Event()
    writer_done = threading.Event()
    close_done = threading.Event()
    original_emit = host._emit_producer_completion

    def gated_emit(terminal: dict) -> None:
        writer_entered.set()
        try:
            original_emit(terminal)
        finally:
            writer_done.set()

    host._emit_producer_completion = gated_emit
    terminal = {
        "type": "turn.end", "sid": _SID, "request_id": "close-race",
        "turn_id": "close-race", "session_key": _SESSION_KEY,
        "origin_session_key": _SESSION_KEY, "profile_home": "/tmp/profile",
        "host_boot_id": "boot", "host_ns": 1,
    }
    writer = threading.Thread(target=host._emit_producer_completion, args=(terminal,))
    writer.start()
    try:
        wait_until(writer_entered.is_set, timeout=5, what="completion writer entered")
        closer = threading.Thread(target=lambda: (host.close(), close_done.set()))
        closer.start()
        wait_until(
            host._completion_close_requested.is_set,
            timeout=5,
            what="completion close request",
        )
        # The writer owns the same lock; close cannot mark the descriptor closed
        # while the in-flight write is still waiting for that lock.
        assert host._completion_closed.is_set() is False
        host._completion_lock.release()
        wait_until(writer_done.is_set, timeout=5, what="completion writer finished")
        wait_until(close_done.is_set, timeout=5, what="completion close finished")
        writer.join(timeout=1)
        closer.join(timeout=1)
        assert not writer.is_alive()
        assert not closer.is_alive()
        assert host._completion_closed.is_set()
        unrelated_read, unrelated_write = os.pipe()
        try:
            host._emit_producer_completion(terminal)
        finally:
            os.close(unrelated_write)
        assert os.read(unrelated_read, 4096) == b""
        try:
            os.close(unrelated_read)
        except OSError:
            pass
    finally:
        if host._completion_lock.locked():
            host._completion_lock.release()
        host.close()
        os.close(read_fd)


def test_completion_writer_rejects_zero_progress_and_accepts_bounded_short_writes():
    with pytest.raises(OSError, match="no progress"):
        _write_completion_bytes(1, b"record", write_fn=lambda _fd, _view: 0)

    calls: list[int] = []

    def short_writer(_fd, view) -> int:
        calls.append(len(view))
        return 1

    _write_completion_bytes(1, b"record", write_fn=short_writer)
    assert calls == [6, 5, 4, 3, 2, 1]


def test_completion_record_schema_is_strict_and_digest_bound():
    valid = {
        "type": "producer.complete", "schema": 1, "host_pid": 9,
        "host_boot_id": "boot", "sid": "sid", "request_id": "request",
        "turn_id": "turn", "session_key": "session", "origin_session_key": "origin",
        "profile_home": "/tmp/profile", "terminal_sha256": "a" * 64,
    }
    assert _valid_producer_completion_record(valid)
    for mutation in (
        {**valid, "schema": 2},
        {key: value for key, value in valid.items() if key != "terminal_sha256"},
        {**valid, "terminal_sha256": "A" * 64},
        {**valid, "terminal_sha256": "short"},
        {**valid, "unexpected": "field"},
        {**valid, "sid": ""},
        {**valid, "profile_home": None},
    ):
        assert not _valid_producer_completion_record(mutation)


def test_private_completion_reader_bounds_unterminated_record_without_state_change(tmp_path):
    read_fd, write_fd = os.pipe()
    write_closed = False
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "host.json")
    finished = threading.Event()
    reader = threading.Thread(
        target=lambda: (supervisor._drain_producer_completion(read_fd, object(), 1), finished.set()),
        daemon=True,
    )
    reader.start()
    try:
        os.write(write_fd, b"x" * 4097)
        os.close(write_fd)
        write_closed = True
        wait_until(finished.is_set, timeout=5, what="bounded completion reader")
        assert supervisor._pending_turns == {}
    finally:
        if reader.is_alive():
            reader.join(timeout=1)
        if not write_closed:
            os.close(write_fd)
