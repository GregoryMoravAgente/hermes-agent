"""R3 causal checks for accepted-session ownership and bounded producer custody."""

from __future__ import annotations

import contextlib
import io
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
from tui_gateway import compute_host as compute_host_module
from tui_gateway.compute_host import ComputeHost
from tui_gateway import host_supervisor as host_supervisor_module
from tui_gateway.host_supervisor import (
    HostSupervisor,
    _PendingTurn,
    _terminal_digest,
)


pytestmark = pytest.mark.platforms("posix")

_SID = "compute-owner-sid"
_SESSION_KEY = "compute-owner-session"


def _frame(tmp_path: Path, request_id: str, *, profile_home: Path) -> dict:
    return {
        "type": "turn.start",
        "sid": _SID,
        "request_id": request_id,
        "turn_id": request_id,
        "session_key": _SESSION_KEY,
        "profile_home": str(profile_home),
        "text": "removed-session producer turn",
        "history": [],
        "history_version": 0,
        "cols": 80,
        "cwd": str(tmp_path),
        "source": "desktop",
    }


def _removed_session_supervisor(monkeypatch, tmp_path: Path):
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_gate_child_r3.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    paths = {
        "hermes-home": hermes_home,
        "worker-release": tmp_path / "worker-release",
        "worker-started": tmp_path / "worker-started",
        "worker-finished": tmp_path / "worker-finished",
        "session-removed": tmp_path / "session-removed",
    }
    args = [
        sys.executable, str(fixture), "--mode", "removed-session-held-exception",
        "--worker-release", str(paths["worker-release"]),
        "--worker-started", str(paths["worker-started"]),
        "--worker-finished", str(paths["worker-finished"]),
        "--session-removed", str(paths["session-removed"]),
    ]
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=args,
        cwd=root,
        env=synthetic_host_env(hermes_home, root),
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )
    return fence, supervisor, paths


def _watcher_failure_supervisor(monkeypatch, tmp_path: Path, failure: str):
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_failure_child_r3c.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    paths = {
        "hermes-home": hermes_home,
        "worker-release": tmp_path / "worker-release",
        "worker-started": tmp_path / "worker-started",
        "worker-finished": tmp_path / "worker-finished",
        "session-removed": tmp_path / "session-removed",
        "failure-seen": tmp_path / "failure-seen",
    }
    args = [
        sys.executable, str(fixture),
        "--failure", failure,
        "--failure-seen", str(paths["failure-seen"]),
        "--mode", "removed-session-held-exception",
        "--worker-release", str(paths["worker-release"]),
        "--worker-started", str(paths["worker-started"]),
        "--worker-finished", str(paths["worker-finished"]),
        "--session-removed", str(paths["session-removed"]),
    ]
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=args,
        cwd=root,
        env=synthetic_host_env(hermes_home, root),
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )
    return fence, supervisor, paths


def _assert_watcher_failure_custody(
    supervisor: HostSupervisor,
    fence,
    paths: dict[str, Path],
    failure: str,
    callback_frames: list[dict],
    ack_processed: threading.Event,
) -> None:
    wait_until(paths["worker-started"].exists, timeout=30, what="accepted worker start")
    wait_until(paths["session-removed"].exists, timeout=30, what="real session registry removal")
    if failure == "thread-start":
        wait_until(paths["failure-seen"].exists, timeout=30, what="injected watcher failure")
    wait_until(
        lambda: (
            (pending := supervisor._pending_turns.get("removed-session-turn")) is not None
            and isinstance(pending, _PendingTurn)
            and bool(pending.terminal_candidates)
        ),
        timeout=30,
        what="single bounded public exception candidate",
    )
    pending = supervisor._pending_turns["removed-session-turn"]
    assert isinstance(pending, _PendingTurn)
    assert len(pending.terminal_candidates) == 1
    candidate = next(iter(pending.terminal_candidates.values()))
    assert candidate["type"] == "turn.error"
    assert candidate["reason"] == "exception"
    assert pending.producer_settled is False
    assert pending.terminal is None
    assert callback_frames == []
    assert not ack_processed.is_set()
    assert fence.active_count() == 1
    assert supervisor.capture_ready is False
    assert supervisor.has_pending_turn(_SID)


def _contain_failed_watcher_host(supervisor: HostSupervisor) -> None:
    """Bound test-owned child cleanup after a deliberately non-clean refusal.

    This is resource containment only.  It must never be used as evidence of a
    settled producer, a released lease, or a capture-ready host.
    """
    proc = supervisor._proc
    if proc is None:
        return
    if proc.poll() is None:
        supervisor._terminate_process(proc)
    wait_until(lambda: proc.poll() is not None, timeout=30, what="test-owned failed host exit")
    for stream in (proc.stdin, proc.stdout, proc.stderr):
        if stream is not None:
            with contextlib.suppress(Exception):
                stream.close()


def _run_watcher_failure_case(monkeypatch, tmp_path: Path, failure: str):
    fence, supervisor, paths = _watcher_failure_supervisor(monkeypatch, tmp_path, failure)
    profile_home = paths["hermes-home"]
    lease = fence.acquire(
        owner_kind="compute-host-turn",
        owner_id=f"{tmp_path}:{failure}",
        profile_id=str(profile_home),
    )
    callback_frames: list[dict] = []
    ack_processed = threading.Event()
    original_completion = supervisor._handle_producer_completion

    def completion(frame: dict) -> bool:
        callback_frames.append(dict(frame))
        return True

    completion._retirement_lease = lease

    def observe_completion(record: dict, **kwargs) -> bool:
        try:
            return original_completion(record, **kwargs)
        finally:
            ack_processed.set()

    supervisor._handle_producer_completion = observe_completion
    try:
        supervisor.submit_turn(
            _frame(tmp_path, "removed-session-turn", profile_home=profile_home),
            on_complete=completion,
        )
        _assert_watcher_failure_custody(
            supervisor, fence, paths, failure, callback_frames, ack_processed,
        )
        paths["worker-release"].write_text("release", encoding="utf-8")
        wait_until(paths["worker-finished"].exists, timeout=30, what="accepted worker exit")
        if failure == "cleanup":
            wait_until(paths["failure-seen"].exists, timeout=30, what="injected watcher failure")
        # Failure is fail-closed: the candidate remains pending after the worker exits
        # because no private completion receipt was produced by the failed watcher.
        wait_until(
            lambda: (
                (pending := supervisor._pending_turns.get("removed-session-turn")) is not None
                and isinstance(pending, _PendingTurn)
                and len(pending.terminal_candidates) == 1
            ),
            timeout=30,
            what="failed watcher custody remains pending",
        )
        assert callback_frames == []
        assert not ack_processed.is_set()
        assert fence.active_count() == 1
        assert supervisor.capture_ready is False
        pending = supervisor._pending_turns["removed-session-turn"]
        assert isinstance(pending, _PendingTurn)
        assert pending.retirement_lease is lease
        assert pending.lease_release_pending is False
        refusal = supervisor.shutdown_clean(timeout=0)
        assert refusal["clean"] is False
        assert refusal["status"] == "pending_work"
        assert refusal["shutdown_sent"] is False
        assert refusal["pending_turns"] == 1
        assert refusal["admission_closed"] is True
        assert supervisor.capture_ready is False
    finally:
        paths["worker-release"].write_text("release", encoding="utf-8")
        # The production shutdown gate correctly refuses destructive cleanup
        # while this known-error candidate remains unknown.  Contain the
        # test-owned child separately; do not convert that forced exit into a
        # producer receipt or a clean-capture result.
        _contain_failed_watcher_host(supervisor)
        if fence.active_count():
            assert fence.release(lease)


def test_watcher_thread_start_failure_retains_one_unknown_candidate(monkeypatch, tmp_path):
    _run_watcher_failure_case(monkeypatch, tmp_path, "thread-start")


def test_watcher_cleanup_failure_retains_one_unknown_candidate(monkeypatch, tmp_path):
    _run_watcher_failure_case(monkeypatch, tmp_path, "cleanup")


def test_removed_session_registry_entry_does_not_settle_live_worker(
    monkeypatch, tmp_path,
):
    """The exact detached session owns the worker until its real thread exits."""
    fence, supervisor, paths = _removed_session_supervisor(monkeypatch, tmp_path)
    profile_home = paths["hermes-home"]
    lease = fence.acquire(
        owner_kind="compute-host-turn",
        owner_id=f"{tmp_path}:removed-session",
        profile_id=str(profile_home),
    )
    callback_frames: list[dict] = []
    callback_done = threading.Event()
    ack_processed = threading.Event()
    original_completion = supervisor._handle_producer_completion

    def completion(frame: dict) -> bool:
        callback_frames.append(dict(frame))
        callback_done.set()
        return True

    completion._retirement_lease = lease

    def observe_completion(record: dict, **kwargs) -> bool:
        try:
            return original_completion(record, **kwargs)
        finally:
            ack_processed.set()

    supervisor._handle_producer_completion = observe_completion
    try:
        supervisor.submit_turn(
            _frame(tmp_path, "removed-session-turn", profile_home=profile_home),
            on_complete=completion,
        )
        wait_until(paths["worker-started"].exists, timeout=30, what="accepted worker start")
        wait_until(paths["session-removed"].exists, timeout=30, what="real session registry removal")
        assert paths["session-removed"].read_text(encoding="utf-8") == "removed"
        wait_until(
            lambda: (
                (pending := supervisor._pending_turns.get("removed-session-turn")) is not None
                and isinstance(pending, _PendingTurn)
                and bool(pending.terminal_candidates)
            ),
            timeout=30,
            what="bounded public exception candidate while worker is live",
        )
        pending = supervisor._pending_turns["removed-session-turn"]
        assert isinstance(pending, _PendingTurn)
        candidate = next(iter(pending.terminal_candidates.values()))
        assert candidate["type"] == "turn.error"
        assert candidate["reason"] == "exception"
        assert pending.producer_settled is False
        assert pending.terminal is None
        assert supervisor.has_pending_turn(_SID)
        # R2 publishes the private receipt from a registry re-lookup that now returns
        # None; observe that causal receipt directly so R2 fails on this fail-closed
        # assertion. R3b keeps the public candidate but still withholds the receipt.
        assert ack_processed.wait(5) is False
        assert callback_frames == []
        assert fence.active_count() == 1
        assert supervisor.capture_ready is False

        paths["worker-release"].write_text("release", encoding="utf-8")
        wait_until(paths["worker-finished"].exists, timeout=30, what="accepted worker exit")
        wait_until(callback_done.is_set, timeout=30, what="post-worker producer callback")
        assert len(callback_frames) == 1
        assert callback_frames[0]["type"] == "turn.error"
        assert callback_frames[0]["reason"] == "exception"
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="exact lease release")
        assert not supervisor.has_pending_turn(_SID)
        assert supervisor.capture_ready is False
    finally:
        paths["worker-release"].write_text("release", encoding="utf-8")
        supervisor.shutdown()
        proc = supervisor._proc
        assert proc is None or proc.poll() is not None


def test_completion_close_waits_inside_real_write_before_closing_fd(monkeypatch):
    """The close barrier is entered while the production write helper owns the lock."""
    read_fd, write_fd = os.pipe()
    host = ComputeHost(stdout=io.StringIO(), completion_fd=write_fd, heartbeat_secs=0)
    write_entered = threading.Event()
    allow_write = threading.Event()
    writer_done = threading.Event()
    close_done = threading.Event()
    original_write = compute_host_module._write_completion_bytes

    def barrier_write(fd: int, data: bytes, **kwargs) -> None:
        write_entered.set()
        wait_until(allow_write.is_set, timeout=5, what="test-owned in-flight write release")
        original_write(fd, data, **kwargs)

    monkeypatch.setattr(compute_host_module, "_write_completion_bytes", barrier_write)
    terminal = {
        "type": "turn.end", "sid": _SID, "request_id": "close-race-r3",
        "turn_id": "close-race-r3", "session_key": _SESSION_KEY,
        "origin_session_key": _SESSION_KEY, "profile_home": "/tmp/profile",
        "host_boot_id": "boot", "host_ns": 1,
    }
    writer = threading.Thread(
        target=lambda: (host._emit_producer_completion(terminal), writer_done.set()),
        name="r3-completion-writer",
    )
    closer = threading.Thread(
        target=lambda: (host.close(), close_done.set()),
        name="r3-completion-closer",
    )
    try:
        writer.start()
        wait_until(write_entered.is_set, timeout=5, what="production write entered")
        closer.start()
        wait_until(
            host._completion_close_requested.is_set,
            timeout=5,
            what="completion close request",
        )
        assert close_done.is_set() is False
        assert host._completion_closed.is_set() is False
        allow_write.set()
        wait_until(writer_done.is_set, timeout=5, what="production write finished")
        wait_until(close_done.is_set, timeout=5, what="completion close finished")
        writer.join(timeout=1)
        closer.join(timeout=1)
        assert not writer.is_alive()
        assert not closer.is_alive()
        assert host._completion_closed.is_set()
        assert os.read(read_fd, 4096)
    finally:
        allow_write.set()
        host.close()
        writer.join(timeout=2)
        closer.join(timeout=2)
        os.close(read_fd)


def test_stdout_frame_budget_refuses_oversized_terminal_without_partial_parse():
    """An oversized public line is drained and rejected before JSON/state handling."""
    supervisor = HostSupervisor(autostart=False)
    frames: list[dict] = []
    supervisor._handle_host_frame = lambda frame, **_kwargs: frames.append(dict(frame))
    frame_budget = getattr(host_supervisor_module, "_MAX_TERMINAL_FRAME_BYTES", 1 * 1024 * 1024)
    oversized = json.dumps({
        "type": "turn.end", "sid": _SID, "request_id": "oversized",
        "payload": "x" * (frame_budget + 128),
    }, separators=(",", ":")).encode("utf-8") + b"\n"
    valid = b'{"type":"hb","progress_counter":1}\n'
    fake_proc = types.SimpleNamespace(stdout=io.BytesIO(oversized + valid))
    finished = threading.Event()
    reader = threading.Thread(
        target=lambda: (supervisor._drain_stdout(fake_proc, 1), finished.set()),
        name="r3-bounded-stdout-reader",
    )
    reader.start()
    try:
        wait_until(finished.is_set, timeout=10, what="bounded stdout reader")
        assert frames == [{"type": "hb", "progress_counter": 1}]
    finally:
        reader.join(timeout=1)


def test_unknown_transport_custody_is_not_overridden_by_producer_receipt():
    """A source receipt can prove turn finalization but cannot clear physical custody unknown."""
    supervisor = HostSupervisor(autostart=False)
    proc = types.SimpleNamespace(pid=99123)
    terminal = {
        "type": "turn.error", "sid": _SID, "request_id": "unknown-custody",
        "turn_id": "unknown-custody", "session_key": _SESSION_KEY,
        "origin_session_key": _SESSION_KEY, "profile_home": "/tmp/profile",
        "host_boot_id": "boot", "reason": "transport_unknown",
        "message": "transport outcome unknown", "custody": "unknown",
    }
    pending = _PendingTurn(
        sid=_SID, session_key=_SESSION_KEY, profile_home="/tmp/profile",
        turn_id="unknown-custody", host_generation=1, host_boot_id="boot",
        host_proc=proc, callback=lambda _frame: pytest.fail("unknown custody callback"),
    )
    pending.terminal = dict(terminal)
    supervisor._proc = proc
    supervisor._host_generation = 1
    supervisor._producer_identity = (proc.pid, "boot")
    supervisor._pending_turns["unknown-custody"] = pending
    record = {
        "type": "producer.complete", "schema": 1, "host_pid": proc.pid,
        "host_boot_id": "boot", "sid": _SID, "request_id": "unknown-custody",
        "turn_id": "unknown-custody", "session_key": _SESSION_KEY,
        "origin_session_key": _SESSION_KEY, "profile_home": "/tmp/profile",
        "terminal_sha256": _terminal_digest(terminal),
    }
    # The receipt is authenticated and accepted even though its terminal still
    # carries unknown physical custody; acceptance does not deliver the callback.
    assert supervisor._handle_producer_completion(record, proc=proc, host_generation=1) is True
    assert supervisor._pending_turns["unknown-custody"] is pending
    assert pending.producer_settled is True
    assert pending.callback_succeeded is False
    assert pending.finalizing is False
    assert pending.terminal is not None
    assert pending.terminal.get("custody") == "unknown"
    assert supervisor.capture_ready is False
