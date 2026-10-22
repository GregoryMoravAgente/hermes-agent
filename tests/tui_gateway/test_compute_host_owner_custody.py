"""Actual producer/reader custody tests for the compute-host owner seam.

These tests launch a real HostSupervisor Popen whose child calls production
``tui_gateway.compute_host.run_host(stdout=...)`` and runs the real ``_run_real_turn`` pipeline
with the neutral synthetic-agent seams from ``test_compute_host_turn_protocol.py``.
They do not replace HostSupervisor/ComputeHost or handcraft the normal terminal.

The ordinary callback is opened by the source-bound producer-completion record.
The second test keeps retained host/resource custody fail-closed while an inherited
writer remains alive; that resource state is separate from the completed UI turn.
"""

from __future__ import annotations

import os
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


def _wait_path(path: Path, timeout: float = 30.0) -> bool:
    return bool(wait_until(path.exists, timeout=timeout, what=f"{path.name} to exist"))


def _try_wait(predicate, *, timeout: float, what: str) -> bool:
    try:
        wait_until(predicate, timeout=timeout, what=what)
    except AssertionError:
        return False
    return True


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _wait_pid_exit(pid: int, timeout: float = 30.0) -> bool:
    return bool(wait_until(lambda: not _pid_alive(pid), timeout=timeout, what=f"pid {pid} to exit"))


def _frame(tmp_path: Path, request_id: str, *, profile_home: Path) -> dict:
    return {
        "type": "turn.start",
        "sid": _SID,
        "request_id": request_id,
        "turn_id": request_id,
        "session_key": _SESSION_KEY,
        "profile_home": str(profile_home),
        "text": "hello from actual child",
        "history": [],
        "history_version": 0,
        "cols": 80,
        "cwd": str(tmp_path),
        "source": "desktop",
    }


def _supervisor(monkeypatch, tmp_path: Path, *, mode: str):
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_owner_child.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    args = [sys.executable, str(fixture), "--mode", mode]
    paths = {}
    if mode == "descendant":
        for name in (
            "descendant-release",
            "descendant-ready",
            "descendant-exit-ack",
            "descendant-pid",
            "descendant-reaped",
            "descendant-reap-error",
            "real-terminal-returned",
        ):
            paths[name] = tmp_path / name
            args.extend([f"--{name}", str(paths[name])])
    paths["hermes-home"] = hermes_home
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=args,
        cwd=root,
        env={
            "HERMES_HOME": str(hermes_home),
            "PYTHONPATH": str(root),
        },
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )
    return fence, supervisor, paths


def _observe_turn_frames(supervisor: HostSupervisor):
    frames: list[dict] = []
    normal_terminal = threading.Event()
    duplicate_terminal = threading.Event()
    original = supervisor._handle_host_frame

    def observe(frame: dict, **kwargs):
        if frame.get("type") == "turn.end" and frame.get("request_id"):
            frames.append(dict(frame))
            if normal_terminal.is_set():
                duplicate_terminal.set()
        result = original(frame, **kwargs)
        if frame.get("type") == "turn.end" and not normal_terminal.is_set():
            normal_terminal.set()
        return result

    supervisor._handle_host_frame = observe
    return frames, normal_terminal, duplicate_terminal


def _stop(supervisor: HostSupervisor, paths: dict[str, Path]) -> None:
    release = paths.get("descendant-release")
    exit_ack = paths.get("descendant-exit-ack")
    pid_path = paths.get("descendant-pid")
    reaped = paths.get("descendant-reaped")
    reap_error = paths.get("descendant-reap-error")
    try:
        if release is not None:
            release.write_text("release", encoding="utf-8")
            # A host can fail before the helper is spawned, so the absence of its
            # pidfile means there is no descendant receipt to await. Once the pidfile
            # exists, require both the helper's bounded ack and physical exit.
            if pid_path is not None and pid_path.exists():
                assert exit_ack is not None
                assert reaped is not None and reap_error is not None
                pid = int(pid_path.read_text(encoding="utf-8"))
                ack_seen = _try_wait(
                    exit_ack.exists,
                    timeout=30,
                    what="descendant exit acknowledgment",
                )
                reaper_seen = _try_wait(
                    lambda: reaped.exists() or reap_error.exists(),
                    timeout=75,
                    what="descendant reaper result",
                )
                pid_exited = _try_wait(
                    lambda: not _pid_alive(pid),
                    timeout=30,
                    what="inherited-writer descendant to exit",
                )
                assert ack_seen, "descendant did not acknowledge finite test cleanup"
                assert reaper_seen, "descendant reaper did not report a bounded result"
                assert reaped.exists(), (
                    "exact descendant Popen was not reaped while the compute host was alive: "
                    f"{reap_error.read_text(encoding='utf-8') if reap_error.exists() else 'no error receipt'}"
                )
                assert pid_exited, "inherited-writer descendant remained alive after release"
    finally:
        supervisor.shutdown()
        proc = supervisor._proc
        assert proc is None or proc.poll() is not None, (
            "HostSupervisor._proc remained live after shutdown"
        )


def test_real_child_terminal_eventually_calls_completion_and_releases_exact_lease(
    monkeypatch, tmp_path
):
    """A real producer boundary opens the ordinary callback and exact lease release."""
    fence, supervisor, paths = _supervisor(monkeypatch, tmp_path, mode="normal")
    profile_home = paths["hermes-home"]
    frames, normal_terminal, _duplicate = _observe_turn_frames(supervisor)
    callback_frames: list[dict] = []
    callback_done = threading.Event()
    lease = fence.acquire(
        owner_kind="compute-host-turn",
        owner_id=f"{tmp_path}:owner-turn",
        profile_id=str(profile_home),
    )
    assert lease is not None

    def completion(frame: dict) -> bool:
        callback_frames.append(frame)
        callback_done.set()
        return True

    completion._retirement_lease = lease
    try:
        request_id = "actual-normal-terminal"
        supervisor.submit_turn(
            _frame(tmp_path, request_id, profile_home=profile_home), on_complete=completion
        )
        assert normal_terminal.wait(30), f"real child produced no turn.end: {frames!r}"
        # Desired contract: after the real producer terminal, an existing owner path
        # must invoke the callback and release only this exact RetirementLease.
        assert callback_done.wait(10), "no source-bound producer completion reached the callback"
        assert callback_frames[-1]["request_id"] == request_id
        assert fence.active_count() == 0
        assert not supervisor.has_pending_turn(_SID)
    finally:
        _stop(supervisor, paths)


def test_inherited_stdout_descendant_keeps_resource_custody_unknown_after_turn_callback(
    monkeypatch, tmp_path
):
    """A matching descendant frame cannot alter the already completed UI turn or capture gate."""
    fence, supervisor, paths = _supervisor(monkeypatch, tmp_path, mode="descendant")
    profile_home = paths["hermes-home"]
    frames, normal_terminal, duplicate_terminal = _observe_turn_frames(supervisor)
    callback_frames: list[dict] = []
    callback_done = threading.Event()
    lease = fence.acquire(
        owner_kind="compute-host-turn",
        owner_id=f"{tmp_path}:descendant-turn",
        profile_id=str(profile_home),
    )
    assert lease is not None

    def completion(frame: dict) -> bool:
        callback_frames.append(frame)
        callback_done.set()
        return True

    completion._retirement_lease = lease
    try:
        request_id = "actual-descendant-terminal"
        supervisor.submit_turn(
            _frame(tmp_path, request_id, profile_home=profile_home), on_complete=completion
        )
        assert normal_terminal.wait(30), f"real child produced no normal turn.end: {frames!r}"
        assert _wait_path(paths["real-terminal-returned"])
        assert _wait_path(paths["descendant-ready"])
        assert duplicate_terminal.wait(30), f"descendant did not emit matching frame: {frames!r}"
        assert _wait_path(paths["descendant-pid"])
        assert _pid_alive(int(paths["descendant-pid"].read_text(encoding="utf-8")))
        matching = [frame for frame in frames if frame.get("request_id") == request_id]
        assert len(matching) >= 2, matching
        # The second frame has the same ids and boot id but is physically written by
        # the inherited pipe writer. Producer completion may already settle the turn
        # lease, but retained host/resource custody and capture remain unknown.
        assert callback_done.wait(10)
        assert callback_frames[-1]["request_id"] == request_id
        assert fence.active_count() == 0
        assert not supervisor.has_pending_turn(_SID)
        assert supervisor.capture_ready is False
    finally:
        _stop(supervisor, paths)
