"""Causal I03 to I15 process-scope controls.

The controls use the production coordinator, process registry, and local reader.  A completed
Future or an empty registry cannot prove that a direct turn had no child; a real pre-effect
release may prove that one reservation created no child, while an uncontained POSIX child stays
unknown until a source-owned scope is available.
"""

import os
import shlex
import sys
import threading

import pytest

from gateway.runtime_custody import CustodyRefusal, RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence
from tests.waiting import wait_until
from tools.process_registry import ProcessRegistry


def _child_command(code: str) -> str:
    return f"{shlex.quote(sys.executable)} -c {shlex.quote(code)}"


def _registry(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    import tools.process_registry as process_registry_module

    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    # This keeps the POSIX control honest: host PID/PGID observation is not descendant proof.
    monkeypatch.setattr(process_registry_module, "_SYSTEMD_SCOPE_AVAILABLE", False, raising=False)
    coordinator = RuntimeCustodyCoordinator(
        process_instance="i03-process-scope-test", authority_epoch=4, fence=RetirementFence(),
    )
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    return registry, coordinator, registry._process_profile_id()


@pytest.mark.platforms("linux", "macos")
def test_pre_effect_foreground_release_is_the_only_childless_scope_receipt(tmp_path, monkeypatch):
    registry, coordinator, profile_id = _registry(tmp_path, monkeypatch)
    task_id = "api-direct-childless"
    scope = registry.begin_process_scope(
        owner_task_id=task_id, profile_id=profile_id, authority_epoch=coordinator.authority_epoch,
    )
    assert scope is not None
    custody = registry.reserve_foreground_custody(
        profile_id=profile_id, scope_fence=scope, owner_task_id=task_id,
    )
    assert custody is not None and custody.foreground_reserved is True
    assert custody.release_unaccepted() is True
    assert scope.release_foreground(custody.owner_id) is True

    receipt = scope.close()
    assert receipt["observed"] is True
    assert receipt["created_session_ids"] == ()
    assert receipt["pending_reservation_ids"] == ()
    assert receipt["childless"] is True
    assert coordinator.status()["active_owner_classes"] == []


@pytest.mark.platforms("linux", "macos")
def test_late_scope_producer_invalidates_cached_childless_receipt(tmp_path, monkeypatch):
    """A producer arriving after close cannot reuse the earlier childless observation."""
    registry, coordinator, profile_id = _registry(tmp_path, monkeypatch)
    scope = registry.begin_process_scope(
        owner_task_id="api-direct-late-producer", profile_id=profile_id,
        authority_epoch=coordinator.authority_epoch,
    )
    assert scope is not None
    first = scope.close()
    assert first["childless"] is True
    assert scope.record_session("late-session", "api-direct-late-producer") is False

    refreshed = scope.close()
    assert refreshed["childless"] is False
    assert refreshed["late_creation"] is True
    assert refreshed["created_session_ids"] == ()


@pytest.mark.platforms("linux", "macos")
def test_terminal_claim_rechecks_late_scope_before_coordinator_release(tmp_path, monkeypatch):
    """A late producer keeps the original coordinator lease unknown before terminalization."""
    registry, coordinator, profile_id = _registry(tmp_path, monkeypatch)
    task_id = "api-direct-terminal-race"
    scope = registry.begin_process_scope(
        owner_task_id=task_id, profile_id=profile_id,
        authority_epoch=coordinator.authority_epoch,
    )
    parent = coordinator.reserve_root(
        "I03_api_run_direct", task_id, profile_id=profile_id,
        authority_epoch=coordinator.authority_epoch,
    )
    assert scope is not None and not isinstance(parent, CustodyRefusal)
    assert scope.close()["childless"] is True
    assert scope.record_session("late-session", task_id) is False
    receipt, refusal = scope.terminalize(coordinator, parent)
    assert receipt["late_creation"] is True
    assert refusal is False
    assert "I03_api_run_direct" in coordinator.status()["active_owner_classes"]


@pytest.mark.platforms("linux", "macos")
def test_terminalization_serializes_concurrent_producer_after_fence(
    tmp_path, monkeypatch,
):
    """A producer racing the terminal fence cannot create a post-release child."""
    registry, coordinator, profile_id = _registry(tmp_path, monkeypatch)
    task_id = "api-direct-concurrent-terminal"
    scope = registry.begin_process_scope(
        owner_task_id=task_id, profile_id=profile_id,
        authority_epoch=coordinator.authority_epoch,
    )
    parent = coordinator.reserve_root(
        "I03_api_run_direct", task_id, profile_id=profile_id,
        authority_epoch=coordinator.authority_epoch,
    )
    assert scope is not None and not isinstance(parent, CustodyRefusal)

    terminal_entered = threading.Event()
    allow_terminal = threading.Event()
    producer_started = threading.Event()
    terminal_result = []
    producer_result = []
    errors = []

    class _TerminalGate:
        def mark_terminal(self, handle):
            terminal_entered.set()
            if not allow_terminal.wait(timeout=5):
                raise AssertionError("terminal gate was not released")
            return coordinator.mark_terminal(handle)

    def terminalize():
        try:
            terminal_result.append(scope.terminalize(_TerminalGate(), parent))
        except BaseException as exc:  # surface worker failures through the assertion below
            errors.append(exc)

    def produce():
        try:
            producer_started.set()
            producer_result.append(scope.record_session("late-session", task_id))
        except BaseException as exc:
            errors.append(exc)

    terminal_thread = threading.Thread(target=terminalize, name="i03-terminalizer")
    producer_thread = threading.Thread(target=produce, name="i03-late-producer")
    terminal_thread.start()
    assert terminal_entered.wait(timeout=5)
    producer_thread.start()
    assert producer_started.wait(timeout=5)
    allow_terminal.set()
    terminal_thread.join(timeout=5)
    producer_thread.join(timeout=5)

    assert not terminal_thread.is_alive()
    assert not producer_thread.is_alive()
    assert not errors
    assert terminal_result and terminal_result[0][0]["childless"] is True
    assert terminal_result[0][1] is None
    assert producer_result == [False]
    assert coordinator.status()["active_owner_classes"] == []


@pytest.mark.platforms("linux", "macos")
def test_real_process_session_separates_public_task_and_private_owner(
    tmp_path, monkeypatch,
):
    """A real child keeps the public session key separate from its per-run registry owner."""
    registry, coordinator, profile_id = _registry(tmp_path, monkeypatch)
    public_task_id = "api-direct-shared-session"
    owner_task_id = "api-direct-private-run"
    scope = registry.begin_process_scope(
        owner_task_id=owner_task_id, profile_id=profile_id,
        authority_epoch=coordinator.authority_epoch,
    )
    parent = coordinator.reserve_root(
        "I03_api_run_direct", owner_task_id, profile_id=profile_id,
        authority_epoch=coordinator.authority_epoch,
    )
    assert scope is not None
    assert not isinstance(parent, CustodyRefusal)
    gate = tmp_path / "private-owner-child-gate"
    os.mkfifo(gate)
    session = None
    try:
        child_code = f"from pathlib import Path; Path({str(gate)!r}).read_text(encoding='utf-8')"
        session = registry.spawn_local(
            _child_command(child_code), cwd=str(tmp_path), task_id=public_task_id,
            owner_task_id=owner_task_id, session_key=public_task_id,
            parent_handle=parent, scope_fence=scope,
        )
        assert session.task_id == public_task_id
        assert session.owner_task_id == owner_task_id
        wait_until(
            lambda: session.process is not None and session.process.poll() is None,
            what="private-owner child to remain live",
        )
        gate.write_text("release", encoding="utf-8")
        wait_until(lambda: session._completion_event.is_set(), what="private-owner child EOF")
        receipt = scope.close()
        assert receipt["childless"] is False
        assert session.id in receipt["created_session_ids"]
    finally:
        if session is not None and session.process is not None and session.process.poll() is None:
            try:
                fd = os.open(str(gate), os.O_WRONLY | os.O_NONBLOCK)
                try:
                    os.write(fd, b"release")
                finally:
                    os.close(fd)
            except OSError:
                pass
            try:
                registry.kill_process(session.id, source="test_cleanup", consume_output=False)
            except (OSError, ValueError):
                pass
        if gate.exists():
            gate.unlink()


@pytest.mark.platforms("linux", "macos")
def test_real_delayed_child_keeps_i03_parent_unknown_until_scope_proof(
    tmp_path, monkeypatch,
):
    registry, coordinator, profile_id = _registry(tmp_path, monkeypatch)
    task_id = "api-direct-delayed-child"
    scope = registry.begin_process_scope(
        owner_task_id=task_id, profile_id=profile_id, authority_epoch=coordinator.authority_epoch,
    )
    assert scope is not None
    parent = coordinator.reserve_root(
        "I03_api_run_direct", task_id, profile_id=profile_id,
        authority_epoch=coordinator.authority_epoch,
    )
    assert not isinstance(parent, CustodyRefusal)
    gate = tmp_path / "child-gate"
    os.mkfifo(gate)
    session = None
    try:
        child_code = (
            "from pathlib import Path; print('i03-child', flush=True); "
            f"Path({str(gate)!r}).read_text(encoding='utf-8')"
        )
        session = registry.spawn_local(
            _child_command(child_code), cwd=str(tmp_path), task_id=task_id,
            owner_task_id=task_id, session_key=task_id, parent_handle=parent,
            scope_fence=scope,
        )
        session.notify_on_complete = True
        wait_until(
            lambda: session.process is not None and session.process.poll() is None,
            what="delayed I03 child to remain live",
        )
        assert session.custody is not None
        assert session.custody.handle.lease.parent_lease_id == parent.lease.lease_id
        pending = coordinator.mark_terminal(parent)
        assert isinstance(pending, CustodyRefusal)
        assert pending.code == "custody_pending"
        assert "I15_process_registry_terminal" in coordinator.status()["active_owner_classes"]

        gate.write_text("release", encoding="utf-8")
        wait_until(
            lambda: session._completion_event.is_set(),
            what="delayed I03 child reader completion",
        )
        delivered = registry.drain_notifications(
            session_key=task_id, skip_poll_observed=False,
        )
        assert delivered and delivered[0][0]["session_id"] == session.id
        token = delivered[0][0].get("delivery_token", "")
        assert token
        registry.acknowledge_completion_delivery(token)
        assert session.custody_state == "unknown"
        scope_receipt = scope.close()
        assert scope_receipt["childless"] is False
        assert session.id in scope_receipt["created_session_ids"]
        assert coordinator.mark_terminal(parent).code == "custody_pending"
    finally:
        if session is not None and session.process is not None and session.process.poll() is None:
            try:
                fd = os.open(str(gate), os.O_WRONLY | os.O_NONBLOCK)
                try:
                    os.write(fd, b"release")
                finally:
                    os.close(fd)
            except OSError:
                pass
            try:
                registry.kill_process(session.id, source="test_cleanup", consume_output=False)
            except (OSError, ValueError):
                pass
        if gate.exists():
            gate.unlink()
