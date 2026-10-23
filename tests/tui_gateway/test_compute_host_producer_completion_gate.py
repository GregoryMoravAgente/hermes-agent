"""Causal tests for the source-bound ComputeHost producer-completion gate.

The child is the real ``ComputeHost`` and the real ``run_host``/``_run_real_turn``
pipeline. Only the agent/provider seams are the existing neutral test seams. The
tests deliberately keep host/resource custody unknown; that later physical-owner
gate is not folded into ordinary UI turn completion.
"""

from __future__ import annotations

import os
from pathlib import Path
import sys
import threading

import pytest

from hermes_cli import backend_retirement
from tests.waiting import wait_until
from tests.tui_gateway.host_supervisor_test_env import synthetic_host_env
from tui_gateway.host_supervisor import HostSupervisor, _PendingTurn


pytestmark = pytest.mark.platforms("posix")


_SID = "compute-owner-sid"
_SESSION_KEY = "compute-owner-session"


def _frame(tmp_path: Path, request_id: str, *, profile_home: Path, turn_id: str | None = None) -> dict:
    return {
        "type": "turn.start",
        "sid": _SID,
        "request_id": request_id,
        "turn_id": turn_id or request_id,
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
    fixture = Path(__file__).with_name("compute_host_producer_gate_child.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    args = [sys.executable, str(fixture), "--mode", mode]
    paths: dict[str, Path] = {"hermes-home": hermes_home}
    if mode == "early":
        for name in ("early-written", "early-go"):
            paths[name] = tmp_path / name
            args.extend((f"--{name}", str(paths[name])))
    elif mode == "retry":
        for name in ("retry-go", "real-terminal-returned"):
            paths[name] = tmp_path / name
            args.extend((f"--{name}", str(paths[name])))
    elif mode == "late":
        for name in (
            "descendant-release", "descendant-ready", "descendant-exit-ack",
            "descendant-pid", "descendant-reaped", "descendant-reap-error",
            "real-terminal-returned", "duplicate-go", "duplicate-written",
        ):
            paths[name] = tmp_path / name
            args.extend((f"--{name}", str(paths[name])))
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


def _lease(fence, tmp_path: Path, profile_home: Path, label: str):
    return fence.acquire(
        owner_kind="compute-host-turn",
        owner_id=f"{tmp_path}:{label}",
        profile_id=str(profile_home),
    )


def _pending(supervisor: HostSupervisor, request_id: str) -> _PendingTurn | None:
    with supervisor._lock:
        record = supervisor._pending_turns.get(request_id)
        return record if isinstance(record, _PendingTurn) else None


def _stop(supervisor: HostSupervisor, paths: dict[str, Path]) -> None:
    release = paths.get("descendant-release")
    pid_path = paths.get("descendant-pid")
    exit_ack = paths.get("descendant-exit-ack")
    reaped = paths.get("descendant-reaped")
    reap_error = paths.get("descendant-reap-error")
    try:
        if release is not None:
            release.write_text("release", encoding="utf-8")
            if pid_path is not None and pid_path.exists():
                assert exit_ack is not None and reaped is not None and reap_error is not None
                pid = int(pid_path.read_text(encoding="utf-8"))
                wait_until(
                    lambda: exit_ack.exists(),
                    timeout=30,
                    what="descendant exit acknowledgment",
                )
                wait_until(
                    lambda: reaped.exists() or reap_error.exists(),
                    timeout=75,
                    what="descendant reaper result",
                )
                assert reaped.exists(), (
                    "exact descendant Popen was not reaped while the compute host was alive: "
                    f"{reap_error.read_text(encoding='utf-8') if reap_error.exists() else 'no error receipt'}"
                )
                wait_until(
                    lambda: not _pid_alive(pid),
                    timeout=30,
                    what="inherited-writer descendant to exit",
                )
    finally:
        supervisor.shutdown()
        proc = supervisor._proc
        assert proc is None or proc.poll() is not None, "HostSupervisor._proc remained live after shutdown"


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


def test_real_producer_boundary_releases_turn_and_allows_next_turn(monkeypatch, tmp_path):
    fence, supervisor, paths = _supervisor(monkeypatch, tmp_path, mode="normal")
    profile_home = paths["hermes-home"]
    first_lease = _lease(fence, tmp_path, profile_home, "first")
    second_lease = _lease(fence, tmp_path, profile_home, "second")
    first_done = threading.Event()
    second_done = threading.Event()
    callback_errors: list[BaseException] = []

    def second(frame: dict) -> bool:
        second_done.set()
        return True

    second._retirement_lease = second_lease

    def first(frame: dict) -> bool:
        first_done.set()
        try:
            supervisor.submit_turn(
                _frame(tmp_path, "gate1-next", profile_home=profile_home),
                on_complete=second,
            )
        except BaseException as exc:
            callback_errors.append(exc)
            return False
        return True

    first._retirement_lease = first_lease
    try:
        supervisor.submit_turn(
            _frame(tmp_path, "gate1-first", profile_home=profile_home), on_complete=first
        )
        wait_until(first_done.is_set, timeout=30, what="first producer-bound callback")
        wait_until(second_done.is_set, timeout=30, what="next-turn callback")
        assert callback_errors == []
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="exact turn leases to release")
        assert not supervisor.has_pending_turn(_SID)
        assert supervisor.capture_ready is False
    finally:
        _stop(supervisor, paths)


def test_public_terminal_before_producer_boundary_cannot_callback_or_admit_next(
    monkeypatch, tmp_path
):
    fence, supervisor, paths = _supervisor(monkeypatch, tmp_path, mode="early")
    profile_home = paths["hermes-home"]
    lease = _lease(fence, tmp_path, profile_home, "early")
    callback_done = threading.Event()
    successor_done = threading.Event()
    callback_frames: list[dict] = []

    def successor(frame: dict) -> bool:
        successor_done.set()
        return True

    successor._retirement_lease = _lease(fence, tmp_path, profile_home, "early-successor")

    def completion(frame: dict) -> bool:
        callback_frames.append(frame)
        callback_done.set()
        supervisor.submit_turn(
            _frame(tmp_path, "early-successor", profile_home=profile_home),
            on_complete=successor,
        )
        return True

    completion._retirement_lease = lease
    try:
        supervisor.submit_turn(
            _frame(tmp_path, "early-first", profile_home=profile_home), on_complete=completion
        )
        wait_until(paths["early-written"].exists, timeout=30, what="early public terminal")
        assert callback_frames == []
        assert not callback_done.is_set()
        assert not successor_done.is_set()
        paths["early-go"].write_text("go", encoding="utf-8")
        wait_until(callback_done.is_set, timeout=30, what="producer-bound callback")
        wait_until(successor_done.is_set, timeout=30, what="successor after producer boundary")
        assert supervisor.capture_ready is False
    finally:
        _stop(supervisor, paths)


def test_inherited_duplicate_cannot_settle_reused_request_id(monkeypatch, tmp_path):
    fence, supervisor, paths = _supervisor(monkeypatch, tmp_path, mode="late")
    profile_home = paths["hermes-home"]
    first_lease = _lease(fence, tmp_path, profile_home, "late-first")
    first_done = threading.Event()
    successor_done = threading.Event()

    def first(frame: dict) -> bool:
        first_done.set()
        return True

    first._retirement_lease = first_lease

    def successor(frame: dict) -> bool:
        successor_done.set()
        return True

    try:
        supervisor.submit_turn(
            _frame(tmp_path, "reused-request", profile_home=profile_home), on_complete=first
        )
        wait_until(first_done.is_set, timeout=30, what="first producer-bound callback")
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="first exact lease release")
        successor_lease = _lease(fence, tmp_path, profile_home, "late-successor")
        successor._retirement_lease = successor_lease
        supervisor.submit_turn(
            _frame(
                tmp_path, "reused-request", turn_id="reused-request-successor",
                profile_home=profile_home,
            ),
            on_complete=successor,
        )
        paths["duplicate-go"].write_text("go", encoding="utf-8")
        wait_until(paths["duplicate-written"].exists, timeout=30, what="late duplicate writer")
        wait_until(successor_done.is_set, timeout=30, what="successor callback")
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="successor exact lease release")
        assert not supervisor.has_pending_turn(_SID)
        assert supervisor.capture_ready is False
    finally:
        _stop(supervisor, paths)


def test_callback_failure_keeps_exact_lease_until_source_retry(monkeypatch, tmp_path):
    fence, supervisor, paths = _supervisor(monkeypatch, tmp_path, mode="retry")
    profile_home = paths["hermes-home"]
    lease = _lease(fence, tmp_path, profile_home, "retry")
    attempts: list[int] = []
    succeeded = threading.Event()

    def completion(frame: dict) -> bool:
        attempts.append(1)
        if len(attempts) == 1:
            raise RuntimeError("test callback failure")
        succeeded.set()
        return True

    completion._retirement_lease = lease
    request_id = "retry-turn"
    try:
        supervisor.submit_turn(
            _frame(tmp_path, request_id, profile_home=profile_home), on_complete=completion
        )
        wait_until(
            lambda: (pending := _pending(supervisor, request_id)) is not None
            and pending.producer_settled,
            timeout=30,
            what="producer completion despite callback failure",
        )
        assert attempts == [1]
        assert fence.active_count() == 1
        assert supervisor.has_pending_turn(_SID)
        paths["retry-go"].write_text("retry", encoding="utf-8")
        wait_until(succeeded.is_set, timeout=30, what="callback retry")
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="retried exact lease release")
        assert not supervisor.has_pending_turn(_SID)
        assert supervisor.capture_ready is False
    finally:
        _stop(supervisor, paths)
