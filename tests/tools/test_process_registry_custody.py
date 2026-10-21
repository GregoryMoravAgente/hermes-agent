"""Real process-custody controls for the terminal process registry.

These tests deliberately use a real local child and its real stdout reader.  The
coordinator is the production retirement fence; a test double cannot prove the
reader EOF, durable-result, handle-close, and completion-delivery ordering that
the I15 owner requires.
"""

import json
import os
import shlex
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence
import tools.environments.base as base_environment
from tools.environments.local import LocalEnvironment
from tools.process_registry import ProcessRegistry, ProcessSession
from tools.terminal_tool_background import yield_to_background_handler


def _child_command(code: str) -> str:
    return f"{shlex.quote(sys.executable)} -c {shlex.quote(code)}"


@pytest.fixture()
def custody_registry(tmp_path, monkeypatch):
    """Bind one fresh registry to one real, isolated retirement fence."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    import tools.process_registry as process_registry_module

    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(process_registry_module, "_SYSTEMD_SCOPE_AVAILABLE", False, raising=False)
    coordinator = RuntimeCustodyCoordinator(
        process_instance="process-registry-custody-test",
        authority_epoch=7,
        fence=RetirementFence(),
    )
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    return registry, coordinator


def _wait_for_delivery(registry: ProcessRegistry, session, timeout: float = 10.0) -> list:
    waited = registry.wait_for_pending_completions(
        task_id=session.owner_task_id,
        timeout=timeout,
        poll_interval=0.1,
    )
    if not session._completion_event.is_set():
        reader = session._reader_thread
        print("native-completion-diagnostic: " + json.dumps({
            "pid": session.pid,
            "returncode": getattr(session.process, "returncode", None),
            "reader_alive": reader.is_alive() if reader is not None else None,
            "reader_phase": session.reader_phase,
            "reader_finish_requested": session._reader_finish_requested.is_set(),
            "exited": session.exited,
            "custody_state": session.custody_state,
            "custody_error": session.custody_error,
            "reader_eof": session.custody_reader_eof,
            "scope_settled": session.custody_scope_settled,
            "durable": session.custody_durable,
            "handle_closed": session.custody_handle_closed,
            "output_chars": session.total_output_chars,
        }, sort_keys=True))
    assert session._completion_event.is_set(), (
        f"real child did not reach terminal reader path: {waited}"
    )
    delivered = registry.drain_notifications(
        session_key=session.session_key,
        skip_poll_observed=False,
    )
    assert delivered and delivered[0][0]["session_id"] == session.id
    assert session.custody_delivery_acked is False
    token = delivered[0][0].get("delivery_token", "")
    registry.acknowledge_completion_delivery(f"foreign-{token}")
    assert session.custody_delivery_acked is False
    registry.acknowledge_completion_delivery(token)
    return delivered


@pytest.mark.platforms("any")
def test_process_without_native_handoff_stays_unknown(custody_registry, tmp_path):
    """A direct spawn without an authenticated native scope cannot claim clean custody."""
    registry, _coordinator = custody_registry
    custody = registry.reserve_foreground_custody()
    proc = subprocess.Popen(
        [sys.executable, "-c", "print('custody-ok', flush=True)"],
        cwd=str(tmp_path), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    session = registry.adopt_local(
        proc, command="direct-command", cwd=str(tmp_path), task_id="task-custody",
        session_key="session-custody", notify_on_complete=True, custody=custody,
    )

    _wait_for_delivery(registry, session)

    assert session.custody_state == "unknown"
    assert session.custody_process_bound is True
    assert session.custody_reader_eof is True
    assert session.custody_scope_settled is False
    assert session.custody_durable is True
    assert session.custody_handle_closed is True
    assert session.custody_delivery_acked is True
    assert registry.preservation_owner_evidence(
        process_instance=_coordinator.process_instance,
        authority_epoch=_coordinator.authority_epoch,
        close_generation=0,
        owner_id=f"{_coordinator.process_instance}:I15_process_registry_terminal",
        profile_id=str(tmp_path / "foreign-profile"),
    ) is None


@pytest.mark.platforms("any")
def test_real_foreground_to_background_handoff_keeps_the_same_process_identity(
    custody_registry, tmp_path
):
    """Yield-to-background adopts the actual Popen and preserves its reader lease."""
    registry, _coordinator = custody_registry
    custody = registry.reserve_foreground_custody()
    assert custody is not None
    proc = subprocess.Popen(
        [sys.executable, "-c", "import sys; print('handoff-ok', flush=True); sys.stdin.read()"],
        cwd=str(tmp_path), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT, text=True,
    )
    session = registry.adopt_local(
        proc, command="foreground-command", cwd=str(tmp_path), task_id="task-handoff",
        session_key="session-handoff", notify_on_complete=True,
        custody=custody,
    )
    proc.stdin.close()
    original_pid, original_start = proc.pid, session.host_start_time
    _wait_for_delivery(registry, session)

    assert session.pid == original_pid
    assert session.host_start_time == original_start
    assert session.custody_state == "unknown"
    assert session.custody_delivery_acked is True


@pytest.mark.platforms("linux", "macos")
def test_real_environment_yield_reserves_before_popen_and_transfers_one_lease(
    custody_registry, tmp_path, monkeypatch
):
    """The production LocalEnvironment yield seam transfers its pre-effect reservation."""
    registry, coordinator = custody_registry
    import tools.process_registry as process_registry_module

    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    gate = tmp_path / "yield-gate"
    os.mkfifo(gate)
    code = (
        "from pathlib import Path; print('environment-handoff', flush=True); "
        f"Path({str(gate)!r}).read_text()"
    )
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    yield_once = {"pending": True}
    monkeypatch.setattr(
        base_environment, "consume_yield",
        lambda _thread_id: yield_once.pop("pending", False),
    )
    handler = yield_to_background_handler(
        command=f"{shlex.quote(sys.executable)} -c {shlex.quote(code)}",
        env_type="local", cwd=str(tmp_path), effective_task_id="task-environment-handoff",
        task_id="task-environment-handoff", session_key="session-environment-handoff",
    )
    session = None
    try:
        result = env.execute(
            f"{shlex.quote(sys.executable)} -c {shlex.quote(code)}",
            cwd=str(tmp_path), yield_handler=handler,
        )
        session = registry.get(result["yielded_session_id"])
        assert session is not None
        assert session.process_instance == coordinator.process_instance
        assert session.custody_state == "accepted"
        assert session.custody_owner_id.startswith(f"{coordinator.process_instance}:process:")
        gate.write_text("release", encoding="utf-8")
        _wait_for_delivery(registry, session)
        # POSIX PGID snapshots are intentionally not descendant proof.  The lease stays
        # unknown until a source-owned systemd scope is available.
        assert session.custody_state == "unknown"
    finally:
        cleanup_sessions = [session] if session is not None else []
        if not cleanup_sessions:
            for entry in registry.list_sessions(session_key="session-environment-handoff"):
                recovered = registry.get(entry["session_id"])
                if recovered is not None:
                    cleanup_sessions.append(recovered)
        for cleanup in cleanup_sessions:
            if cleanup.process is not None and cleanup.process.poll() is None:
                # A failed assertion must not strand the FIFO child. Nonblocking open avoids
                # making teardown wait forever when the child has not reached the read yet;
                # the registry kill path then retains any ambiguous custody as unknown.
                try:
                    fd = os.open(str(gate), os.O_WRONLY | os.O_NONBLOCK)
                    try:
                        os.write(fd, b"release")
                    finally:
                        os.close(fd)
                except OSError:
                    pass
                try:
                    registry.kill_process(cleanup.id, source="test_cleanup", consume_output=False)
                except (OSError, ValueError, subprocess.TimeoutExpired):
                    pass
            if cleanup.process is not None:
                try:
                    cleanup.process.wait(timeout=10)
                except (OSError, subprocess.TimeoutExpired):
                    pass
        if gate.exists():
            gate.unlink()


@pytest.mark.platforms("linux", "macos")
def test_persistence_failure_keeps_real_child_custody_unknown_and_retryable(
    custody_registry, tmp_path, monkeypatch
):
    """A result-write failure never turns a real exited child into a clean owner."""
    registry, coordinator = custody_registry
    import tools.process_registry as process_registry_module

    monkeypatch.setattr(process_registry_module, "save_completed_result", lambda _session: False)
    session = registry.spawn_local(
        _child_command("print('durability-failure', flush=True)"),
        cwd=str(tmp_path), task_id="task-failure", session_key="session-failure",
    )
    session.notify_on_complete = True
    _wait_for_delivery(registry, session)

    assert session.custody_state == "unknown"
    assert session.custody_error == "read_failed"
    assert coordinator.status()["active_owner_classes"] == ["I15_process_registry_terminal"]
    assert registry.preservation_owner_evidence(
        process_instance=coordinator.process_instance,
        authority_epoch=coordinator.authority_epoch,
        close_generation=0,
        owner_id=f"{coordinator.process_instance}:I15_process_registry_terminal",
        profile_id=registry._process_profile_id(),
    ) is None


@pytest.mark.platforms("linux", "macos")
def test_checkpoint_recovery_never_adopts_opaque_process_custody(tmp_path, monkeypatch):
    """A checkpoint carries identity for diagnosis, never a new lease authority."""
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    import tools.process_registry as process_registry_module

    checkpoint = tmp_path / "processes.json"
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", checkpoint)
    proc = subprocess.Popen([sys.executable, "-c", "import sys; sys.stdin.read()"], stdin=subprocess.PIPE)
    try:
        start = ProcessRegistry._safe_host_start_time(proc.pid)
        assert start is not None
        owner = ProcessRegistry()
        owner._running["proc_checkpoint_custody"] = ProcessSession(
            id="proc_checkpoint_custody", command="real-child", task_id="task-checkpoint",
            started_at=time.time(), pid=proc.pid, pid_scope="host", host_start_time=start,
            custody_owner_id="process-registry-custody-test:process:proc_checkpoint_custody",
            process_instance="process-registry-custody-test", authority_epoch=7,
            custody_state="accepted",
        )
        owner._write_checkpoint()
        assert json.loads(checkpoint.read_text(encoding="utf-8"))[0]["custody_state"] == "accepted"

        successor = ProcessRegistry()
        assert successor.recover_from_checkpoint() == 1
        recovered = successor.get("proc_checkpoint_custody")
        assert recovered is not None
        assert recovered.custody_state == "unknown"
        assert recovered.custody_error == "successor_proof_missing"
    finally:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=10)


@pytest.mark.platforms("any")
def test_foreground_custody_consumption_is_exact_record_cas(custody_registry):
    registry, _coordinator = custody_registry
    first = registry.reserve_foreground_custody()
    replacement = SimpleNamespace(custody=first)
    first.native_scope = replacement
    assert registry.retain_foreground_custody(replacement) is True

    # A stale callback cannot remove the current transaction, even when it reuses the
    # same owner id. Only the exact retained object may clear the map entry.
    stale = SimpleNamespace(custody=first)
    assert registry.consume_foreground_custody(stale) is False
    assert registry._foreground_custody_transactions[first.owner_id] is replacement
    assert registry.consume_foreground_custody(first) is True
    assert registry._foreground_custody_transactions == {}
