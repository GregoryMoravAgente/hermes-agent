"""Native Windows process-registry custody controls.

These cases run only on Windows because a POSIX process group cannot stand in for a
kernel Job.  The child is a real Python process; the test does not fake ``sys.platform``
or replace the Job implementation.
"""
from __future__ import annotations

from tests.tools.foreground_custody_controllers import (
    _controller_transaction_snapshot, _start_late_writer_controller,
    _start_blocking_inherited_writer_controller, _wire_managed_turn_callback,
)

import contextlib
import ctypes
from ctypes import wintypes
import json
import os
import shlex
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from agent.tool_result_custody import _payload_equal
from gateway.runtime_custody import RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence
import tools.environments.base as base_environment
from tools.environments.local import LocalEnvironment
from tools.process_registry import ProcessRegistry
from tools.process_registry_custody import ForegroundParentBinding, ProcessCustody
from tools.process_registry_windows import WindowsLaunchTransaction
from tools.terminal_tool_background import yield_to_background_handler
from tests.tools.process_registry_native_profile import _popen_kwargs, prepare_native_profile
from tests.tools.foreground_custody_diagnostics import (
    blocking_child_code,
    execute_with_diagnostics,
    emit_reader_observation,
    failure_details,
    inherited_descendant_parent_code,
    shell_python_command,
    socket_descendant_code,
    stdin_child_code,
)
from tests.waiting import wait_until


def _coordinator() -> RuntimeCustodyCoordinator:
    return RuntimeCustodyCoordinator(
        process_instance="windows-process-custody-test",
        authority_epoch=9,
        fence=RetirementFence(),
    )




def _drain_windows_completion(registry: ProcessRegistry, session) -> dict:
    waited = registry.wait_for_pending_completions(
        task_id=session.owner_task_id,
        timeout=10,
        poll_interval=0.1,
    )
    assert session._completion_event.is_set(), (
        f"real Windows child did not finish its reader path: {waited}"
    )
    delivered = registry.drain_notifications(
        session_key=session.session_key,
        skip_poll_observed=False,
    )
    assert delivered and delivered[0][0]["session_id"] == session.id
    event = delivered[0][0]
    token = event.get("delivery_token", "")
    assert token
    registry.acknowledge_completion_delivery(f"foreign-{token}")
    assert session.custody_delivery_acked is False
    registry.acknowledge_completion_delivery(token)
    assert session.custody_delivery_acked is True
    return event


def _persistence_agent(tmp_path, db, session_id: str):
    """Build a real AIAgent with a real SessionDB for the managed foreground test."""
    from run_agent import AIAgent

    with (
        patch("model_tools.get_tool_definitions", return_value=[]),
        patch("model_tools.check_toolset_requirements", return_value={}),
        patch("agent.process_bootstrap.OpenAI"),
        patch("run_agent._hermes_home", tmp_path),
        patch("agent.model_metadata.fetch_model_metadata", return_value={}),
    ):
        agent = AIAgent(
            api_key="test-key",
            base_url="https://openrouter.ai/api/v1",
            quiet_mode=True,
            skip_context_files=True,
            skip_memory=True,
        )
    agent._session_db = db
    agent._session_db_created = True
    agent.session_id = session_id
    agent._last_flushed_db_idx = 0
    agent._flushed_db_message_ids = set()
    agent._flushed_db_message_session_id = None
    agent._persist_disabled = False
    agent._current_turn_id = "managed-turn"
    agent._current_run_generation = 3
    agent._flush_messages_to_session_db = MagicMock(
        wraps=agent._flush_messages_to_session_db,
    )
    return agent










def _assert_real_db_callback_cannot_ack(
    tmp_path, transaction, registry, result, *, session_id: str, session_key: str,
    call_id: str, turn_id: str, generation: int, task_id: str,
):
    """Send actual output through SessionDB/TurnRunner and require a custody refusal."""
    from agent.tool_executor import _ToolCallRef, _commit_tool_result
    from hermes_state import SessionDB
    from tools.budget_config import DEFAULT_BUDGET

    db = SessionDB(tmp_path / f"{session_id}.db")
    db.create_session(session_id=session_id, source="tui", model="test/model")
    agent = _persistence_agent(tmp_path, db, session_id)
    agent._current_turn_id = turn_id
    agent._current_run_generation = generation
    _wire_managed_turn_callback(agent, session_id, session_key, generation)
    assert registry.foreground_binding_active(
        session_id=session_id, session_key=session_key, turn_id=turn_id,
        run_generation=generation, tool_call_id=call_id,
    ) is True
    assert agent.tool_result_custody_required(tool_call_id=call_id) is True
    projected = []
    agent.tool_progress_callback = lambda *args, **kwargs: projected.append((args, kwargs))
    ref = _ToolCallRef(
        name="terminal", args={"command": task_id}, task_id=task_id,
        call_id=call_id, trace=[],
    )
    messages = []
    committed = _commit_tool_result(
        agent,
        messages,
        ref,
        result["output"],
        budget=DEFAULT_BUDGET,
        tool_duration=0.01,
        is_error=False,
        blocked=False,
        effect_disposition=None,
    )
    assert committed is None
    assert agent._flush_messages_to_session_db.call_count == 1
    assert projected == []
    record = transaction._foreground_custody_record
    assert record.session is None
    assert transaction.custody.delivery_acked is False
    assert registry._foreground_custody_transactions[transaction.custody.owner_id] is transaction
    assert transaction.process_handle_closed is False
    assert transaction.job_handle_closed is False
    return agent, db


@pytest.mark.platforms("windows")
def test_real_child_is_assigned_before_resume_and_closes_after_job_empty(tmp_path):
    coordinator = _coordinator()
    custody = ProcessCustody.reserve(coordinator, "windows-process-owner", str(tmp_path))
    transaction = WindowsLaunchTransaction(custody)
    child_code = stdin_child_code(tmp_path / "hermes-custody-f1-child.jsonl", "native-custody-ok")
    proc = transaction.launch(
        [sys.executable, "-c", child_code],
        **{**_popen_kwargs(tmp_path), "stdin": subprocess.PIPE},
    )
    try:
        assert transaction.assigned is True
        assert transaction.resumed is True
        assert custody.pid_scope == "windows_job"
        assert transaction.observe_empty() is None
        assert proc.stdout is not None
        assert proc.stdin is not None
        proc.stdin.close()
        assert proc.stdout.read().strip() == "native-custody-ok"
        proc.wait(timeout=10)
        assert transaction.observe_empty() is True, failure_details(tmp_path, transaction)
        assert transaction.close_after_observed_empty() is True
        assert transaction.process_handle_closed is True
        assert transaction.job_handle_closed is True
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


@pytest.mark.platforms("windows")
def test_popen_failure_can_release_only_before_a_process_effect(tmp_path):
    coordinator = _coordinator()
    custody = ProcessCustody.reserve(coordinator, "windows-process-owner", str(tmp_path))
    transaction = WindowsLaunchTransaction(custody)
    with pytest.raises((OSError, FileNotFoundError)):
        transaction.launch(
            [str(tmp_path / "missing-hermes-child.exe")],
            **_popen_kwargs(tmp_path),
        )
    assert transaction.process is None
    assert transaction.assigned is False
    assert transaction.no_effect_verified is True
    assert transaction.release_before_effect() is True
    assert coordinator.status()["active_owner_classes"] == []


@pytest.mark.platforms("windows")
def test_duplicate_foreground_transaction_cannot_replace_authenticated_binding(tmp_path):
    registry = ProcessRegistry()
    coordinator = _coordinator()
    registry.bind_runtime_custody(coordinator)
    custody = registry.reserve_foreground_custody(
        profile_id=str(tmp_path),
        binding=ForegroundParentBinding("session", "session-key", "call", "turn", 3),
    )
    first = WindowsLaunchTransaction(custody)
    custody.native_scope = first
    assert registry.retain_foreground_custody(first) is True
    replacement = WindowsLaunchTransaction(custody)
    assert registry.retain_foreground_custody(replacement) is False
    assert registry._foreground_custody_transactions[custody.owner_id] is first


@pytest.mark.platforms("windows")
def test_reservation_refusal_blocks_foreground_spawn(tmp_path, monkeypatch):
    """A closing coordinator cannot fall through to an unowned local Popen."""
    import tools.process_registry as process_registry_module

    class ClosingRegistry:
        def reserve_foreground_custody(self, **_kwargs):
            raise RuntimeError("runtime custody is closing")

    marker = tmp_path / "child-started"
    child_code = f"from pathlib import Path; Path({str(marker)!r}).write_text('started')"
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
    monkeypatch.setattr(process_registry_module, "process_registry", ClosingRegistry())
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    yield_once = {"pending": True}
    monkeypatch.setattr(
        base_environment,
        "consume_yield",
        lambda _thread_id: yield_once.pop("pending", False),
    )
    handler = yield_to_background_handler(
        command=command,
        env_type="local",
        cwd=str(tmp_path),
        effective_task_id="task-refusal",
        task_id="task-refusal",
        session_key="session-refusal",
    )
    with pytest.raises(RuntimeError, match="runtime custody is closing"):
        env.execute(command, cwd=str(tmp_path), yield_handler=handler)
    assert marker.exists() is False


@pytest.mark.platforms("windows")
def test_opaque_constructor_failure_retains_native_custody(tmp_path, monkeypatch):
    coordinator = _coordinator()
    custody = ProcessCustody.reserve(coordinator, "windows-process-owner", str(tmp_path))
    transaction = WindowsLaunchTransaction(custody)

    class BrokenJob:
        def __init__(self):
            raise RuntimeError("opaque native constructor failure")

    monkeypatch.setattr(transaction, "_native_job_type", lambda: BrokenJob)
    with pytest.raises(RuntimeError):
        transaction.launch([sys.executable, "-c", "print('never-started')"], **_popen_kwargs(tmp_path))
    assert transaction.process is None
    assert transaction.no_effect_verified is False
    assert transaction.state == "unknown"
    assert custody.state == "unknown"
    assert transaction.release_before_effect() is False


@pytest.mark.platforms("windows")
def test_identity_mismatch_does_not_adopt_a_different_process(tmp_path):
    coordinator = _coordinator()
    custody = ProcessCustody.reserve(coordinator, "windows-process-owner", str(tmp_path))
    transaction = WindowsLaunchTransaction(custody)
    proc = transaction.launch(
        [sys.executable, "-c", "import time; time.sleep(0.2)"],
        **_popen_kwargs(tmp_path),
    )
    try:
        assert transaction.matches(object(), proc.pid, transaction.host_start_time) is False
        assert transaction.matches(proc, proc.pid, transaction.host_start_time) is True
    finally:
        proc.wait(timeout=10)
        assert transaction.observe_empty() is True
        assert transaction.close_after_observed_empty() is True


@pytest.mark.platforms("windows")
def test_partial_job_assignment_retains_exact_unknown_custody(tmp_path):
    coordinator = _coordinator()
    custody = ProcessCustody.reserve(coordinator, "windows-process-owner", str(tmp_path))
    transaction = WindowsLaunchTransaction(custody)
    real_job_type = transaction._native_job_type()
    proc = None

    class AssignFailureJob:
        def __init__(self):
            self._job = real_job_type()

        def assign(self, process):
            raise OSError("synthetic assign refusal")

        def __getattr__(self, name):
            return getattr(self._job, name)

    transaction._native_job_type = lambda: AssignFailureJob
    try:
        with pytest.raises(OSError):
            transaction.launch(
                [sys.executable, "-c", "import sys; sys.stdin.read()"],
                **{**_popen_kwargs(tmp_path), "stdin": subprocess.PIPE},
            )
        proc = transaction.process
        assert proc is not None
        assert transaction.assigned is False
        assert transaction.no_effect_verified is False
        assert transaction.state == "unknown"
        assert custody.state == "unknown"
        assert transaction.release_before_effect() is False
    finally:
        if proc is not None:
            with contextlib.suppress(OSError):
                proc.kill()
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                proc.wait(timeout=10)
        if transaction.job is not None and not transaction.job_handle_closed:
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                transaction.job.close()
        if proc.stdout is not None:
            with contextlib.suppress(OSError, ValueError):
                proc.stdout.close()


@pytest.mark.platforms("windows")
def test_partial_resume_failure_retains_exact_unknown_custody(tmp_path, monkeypatch):
    import psutil

    coordinator = _coordinator()
    custody = ProcessCustody.reserve(coordinator, "windows-process-owner", str(tmp_path))
    transaction = WindowsLaunchTransaction(custody)
    real_process = psutil.Process
    proc = None

    class ResumeFailure:
        def __init__(self, pid):
            self._process = real_process(pid)

        def resume(self):
            raise OSError("synthetic resume refusal")

        def __getattr__(self, name):
            return getattr(self._process, name)

    monkeypatch.setattr(psutil, "Process", ResumeFailure)
    try:
        with pytest.raises(OSError):
            transaction.launch(
                [sys.executable, "-c", "import sys; sys.stdin.read()"],
                **{**_popen_kwargs(tmp_path), "stdin": subprocess.PIPE},
            )
        proc = transaction.process
        assert proc is not None
        assert transaction.assigned is True
        assert transaction.resumed is False
        assert transaction.no_effect_verified is False
        assert transaction.state == "unknown"
        assert custody.state == "unknown"
        assert transaction.release_before_effect() is False
    finally:
        if proc is not None:
            with contextlib.suppress(OSError):
                proc.kill()
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                proc.wait(timeout=10)
        if transaction.job is not None and not transaction.job_handle_closed:
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                transaction.job.close()


@pytest.mark.platforms("windows")
def test_native_close_failure_retains_same_job_for_exact_retry(tmp_path):
    coordinator = _coordinator()
    custody = ProcessCustody.reserve(coordinator, "windows-process-owner", str(tmp_path))
    transaction = WindowsLaunchTransaction(custody)
    custody.native_scope = transaction
    proc = transaction.launch(
        [sys.executable, "-c", "print('close-retry', flush=True)"],
        **_popen_kwargs(tmp_path),
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.read().strip() == "close-retry"
        proc.wait(timeout=10)
        assert transaction.observe_empty() is True
        original_close = transaction.job.close
        original_job = transaction.job
        original_job_handle = transaction.job._handle

        def fail_close():
            raise OSError("synthetic close refusal")

        transaction.job.close = fail_close
        try:
            assert transaction.close_after_observed_empty() is False
        finally:
            transaction.job.close = original_close
        assert custody.state == "unknown"
        assert transaction.state == "close_failed"
        assert transaction.close_attempts == 1
        assert transaction.close_failures == 1
        assert transaction.close_retryable is True
        assert transaction.close_job is original_job
        assert transaction.close_job_handle is original_job_handle
        assert transaction.process_handle_closed is True
        assert transaction.job_handle_closed is False
        assert custody.reconcile_unknown() is True
        assert transaction.close_after_observed_empty() is True
        assert transaction.state == "closed"
        assert transaction.close_retryable is False
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


@pytest.mark.platforms("windows")
def test_real_environment_yield_adopts_and_settles_native_custody(tmp_path, monkeypatch):
    """Exercise the real LocalEnvironment -> handler -> reader -> ACK path."""
    import tools.process_registry as process_registry_module

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(10)
    host, port = listener.getsockname()
    child_code = (
        "import socket; "
        f"peer=socket.create_connection(({host!r}, {port})); "
        "print('vertical-ready', flush=True); peer.recv(1); "
        "print('vertical-tail', flush=True); peer.close()"
    )
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    yield_once = {"pending": True}
    monkeypatch.setattr(
        base_environment,
        "consume_yield",
        lambda _thread_id: yield_once.pop("pending", False),
    )
    handler = yield_to_background_handler(
        command=command,
        env_type="local",
        cwd=str(tmp_path),
        effective_task_id="task-windows-vertical",
        task_id="task-windows-vertical",
        session_key="session-windows-vertical",
    )
    session = None
    connection = None
    try:
        result = env.execute(command, cwd=str(tmp_path), yield_handler=handler)
        session = registry.get(result["yielded_session_id"])
        assert session is not None
        assert session.custody is not None
        transaction = session.custody.native_scope
        assert transaction is not None
        assert transaction.matches(session.process, session.pid, session.host_start_time)
        assert transaction.assigned is True
        assert transaction.resumed is True
        assert session.custody_state == "accepted"
        record = transaction._foreground_custody_record
        assert record.phase == "reader_transferred"
        assert session._foreground_reader_transfer is True
        transfer = session.custody.foreground_transfer
        assert transfer is not None
        assert transfer.transaction is transaction
        assert transfer.reader_thread is record.evidence["reader_thread"]
        assert transfer.output is session._reader_output
        assert transfer.reader_stop is session._reader_stop
        assert session._reader_thread is record.evidence["reader_thread"]
        assert session._reader_output is not None
        assert session._reader_stop is not None
        assert session._reader_stop.is_set() is False
        assert session._reader_finish_thread is not None
        connection, _ = listener.accept()
        connection.sendall(b"x")
        connection.close()
        connection = None
        _drain_windows_completion(registry, session)
        assert session.output_buffer.count("vertical-ready") == 1
        assert session.output_buffer.count("vertical-tail") == 1
        assert session.custody_state == "terminal"
        assert transaction.process_handle_closed is True
        assert transaction.job_handle_closed is True
        assert coordinator.status()["active_owner_classes"] == []
    finally:
        if connection is not None:
            with contextlib.suppress(OSError):
                connection.close()
        with contextlib.suppress(OSError):
            listener.close()
        if session is not None and session.process is not None and session.process.poll() is None:
            with contextlib.suppress(Exception):
                registry.kill_process(session.id, source="test_cleanup", consume_output=False)
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                session.process.wait(timeout=10)
        with contextlib.suppress(Exception):
            env.cleanup()


@pytest.mark.platforms("windows")
def test_real_environment_no_yield_admits_completed_child_from_same_transaction(tmp_path, monkeypatch):
    """A no-yield foreground wait uses the original Popen/Job and never starts a second reader."""
    import tools.process_registry as process_registry_module

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(base_environment, "consume_yield", lambda _thread_id: False)

    child_code = "print('foreground-eof', flush=True)"
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    handler = yield_to_background_handler(
        command=command, env_type="local", cwd=str(tmp_path), effective_task_id="task-foreground-eof",
        task_id="task-foreground-eof", session_key="session-foreground-eof",
    )
    captured_evidence = {}
    original_record_evidence = handler._record_foreground_evidence

    def _capture_real_evidence(proc, evidence):
        captured_evidence["value"] = dict(evidence)
        original_record_evidence(proc, evidence)

    # Observe the real BaseEnvironment callback; do not manufacture record evidence in
    # production merely to make this no-yield assertion convenient.
    handler._record_foreground_evidence = _capture_real_evidence
    result = env.execute(command, cwd=str(tmp_path), yield_handler=handler)
    transaction = handler._launch_transaction
    assert transaction is not None
    assert handler._foreground_pending is True
    assert handler._finalize_foreground(result) is True
    assert handler._foreground_pending is False
    assert transaction.process is not None
    assert transaction.process_handle_closed is True
    assert transaction.job_handle_closed is True
    assert registry._foreground_custody_transactions == {}
    evidence = captured_evidence["value"]
    assert evidence["status"] == "true_eof"
    assert evidence["eof_kind"] == "win32_broken_pipe"
    assert evidence["reader_done"] is True
    assert evidence["drain_joined"] is True
    assert evidence["stdout_closed"] is True
    sessions = [session for session in registry._finished.values() if session.session_key == "session-foreground-eof"]
    assert len(sessions) == 1
    session = sessions[0]
    assert session.process is transaction.process
    assert session.pid == transaction.pid
    assert session.host_start_time == transaction.host_start_time
    assert session._reader_thread is None
    assert session.custody_state == "terminal"


@pytest.mark.platforms("windows")
def test_managed_tool_commit_uses_real_db_turn_callback_before_native_settlement(tmp_path, monkeypatch):
    """The production tool commit settles the original Job only after its real DB row and TurnRunner callback."""
    import tools.process_registry as process_registry_module
    from agent.tool_executor import _ToolCallRef, _commit_tool_result
    from gateway.run_turn_runner import TurnRunner
    from gateway.turn_context import TurnContext
    from hermes_state import SessionDB
    from tools.budget_config import DEFAULT_BUDGET

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")

    session_id, session_key, call_id, turn_id, generation = (
        "managed-session", "managed-route", "managed-call", "managed-turn", 3,
    )
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(10)
    host, port = listener.getsockname()
    descendant_path = tmp_path / "hermes-custody-f2-descendant.jsonl"
    parent_path = tmp_path / "hermes-custody-f2-parent.jsonl"
    late_code = socket_descendant_code(
        descendant_path,
        host,
        port,
        "sys.stdout.buffer.write(b'late-ready\\n'); sys.stdout.buffer.flush();",
        "sys.stdout.buffer.write(b'late-tail-\\xe2'); sys.stdout.buffer.flush(); "
        "sys.stdout.buffer.write(b'\\x82\\xac\\n'); sys.stdout.buffer.flush();",
    )
    child_code = inherited_descendant_parent_code(
        parent_path, late_code, "managed-parent",
    )
    command = shell_python_command(child_code)
    handler = yield_to_background_handler(
        command=command,
        env_type="local",
        cwd=str(tmp_path),
        effective_task_id="managed-task",
        task_id="managed-task",
        session_key=session_key,
        session_id=session_id,
        tool_call_id=call_id,
        turn_id=turn_id,
        run_generation=generation,
    )
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    db = SessionDB(tmp_path / "managed-session.db")
    db.create_session(session_id=session_id, source="tui", model="test/model")
    agent = None
    transaction = None
    controller = None
    controller_errors = None
    released = None
    try:
        controller, controller_errors, released, controller_state = _start_late_writer_controller(
            handler, listener, diagnostic_dir=tmp_path,
        )
        result = execute_with_diagnostics(env, 
            command,
            cwd=str(tmp_path), yield_handler=handler,
        )
        controller.join(timeout=10)
        assert controller.is_alive() is False
        assert controller_errors == [], controller_state
        assert released is not None and released.is_set()
        assert "late-tail-€" in result["output"]
        transaction = handler._launch_transaction
        assert transaction is not None
        assert handler._finalize_foreground(result) is True

        agent = _persistence_agent(tmp_path, db, session_id)
        ctx = TurnContext(
            source=SimpleNamespace(platform=None),
            session_id=session_id,
            session_key=session_key,
            run_generation=generation,
            user_config={},
            mute_notification_reply=False,
            progress_callback=None,
            native_tool_start_callback=None,
            native_tool_complete_callback=None,
            voice_ack_callback=None,
            _native_slack_task_cards=False,
            _step_callback_sync=None,
            _status_callback_sync=None,
            _event_callback_sync=None,
            _status_adapter=None,
            process_task_id=None,
            process_baseline=None,
            voice_turn=False,
        )
        ctx._hooks_ref = SimpleNamespace(loaded_hooks=[])
        holder = SimpleNamespace(
            _ctx=ctx,
            _approval_owner=None,
            _runner=SimpleNamespace(
                _service_tier=None,
                _consume_pending_turn_sidecar_notes=lambda _key: [],
            ),
            _make_bg_review_callbacks=lambda: (lambda _message: None, lambda: None),
            _merge_turn_request_overrides=TurnRunner._merge_turn_request_overrides,
            _clarify_callback_sync=lambda *_args, **_kwargs: None,
            _notice_callback_sync=lambda *_args, **_kwargs: None,
            _attach_session_title_callback=lambda *_args, **_kwargs: None,
            combined_tool_start_callback=None,
            combined_tool_complete_callback=None,
        )
        TurnRunner._wire_turn_agent_callbacks(holder, agent, {}, None, None, None, False)
        ref = _ToolCallRef(
            name="terminal", args={"command": "managed-child"}, task_id="managed-task",
            call_id=call_id, trace=[],
        )
        messages = []
        committed = _commit_tool_result(
            agent,
            messages,
            ref,
            result["output"],
            budget=DEFAULT_BUDGET,
            tool_duration=0.01,
            is_error=False,
            blocked=False,
            effect_disposition=None,
        )
        assert committed is not None
        assert agent._flush_messages_to_session_db.call_count == 1
        assert db.get_message_role(session_id, messages[-1]["_row_id"]) == "tool"
        persisted_rows = db.get_messages_as_conversation(session_id, include_row_ids=True)
        persisted = next(row for row in persisted_rows if row.get("_row_id") == messages[-1]["_row_id"])
        proof = transaction._foreground_custody_record.durable_proof
        assert proof is not None
        assert _payload_equal(proof.readback, persisted)
        assert len(proof.readback_digest) == 64
        assert len(proof.message_digest) == 64
        assert persisted["content"] == messages[-1]["content"]
        assert "late-tail-€" in persisted["content"]
        assert persisted["tool_call_id"] == call_id
        assert transaction._foreground_custody_record.session.process is transaction.process
        assert transaction._foreground_custody_record.session.pid == transaction.pid
        assert transaction._foreground_custody_record.session.host_start_time == transaction.host_start_time
        assert registry._foreground_custody_transactions == {}
        assert transaction.process_handle_closed is True
        assert transaction.job_handle_closed is True
        assert coordinator.status()["active_owner_classes"] == []
    finally:
        if transaction is not None and transaction.process is not None:
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                if transaction.process.poll() is None:
                    transaction.process.kill()
                transaction.process.wait(timeout=10)
        if agent is not None:
            agent._session_db = None
        db.close()
        with contextlib.suppress(OSError):
            listener.close()
        with contextlib.suppress(Exception):
            env.cleanup()


@pytest.mark.platforms("windows")
def test_real_environment_deadline_crossing_retains_live_reader_and_stdin_no_ack(tmp_path, monkeypatch):
    """A live reader/stdin lease crossing the original deadline cannot be acknowledged by DB delivery."""
    import tools.process_registry as process_registry_module

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")

    grandchild_path = tmp_path / "hermes-custody-f3-descendant.jsonl"
    parent_path = tmp_path / "hermes-custody-f3-parent.jsonl"
    grandchild_code = blocking_child_code(grandchild_path, "deadline-ready")
    child_code = inherited_descendant_parent_code(
        parent_path, grandchild_code, "deadline-parent",
    )
    command = shell_python_command(child_code)
    session_id, session_key, call_id, turn_id, generation = (
        "deadline-live-session", "deadline-live-route", "deadline-live-call", "deadline-live-turn", 3,
    )
    handler = yield_to_background_handler(
        command=command,
        env_type="local",
        cwd=str(tmp_path),
        effective_task_id="deadline-live-task",
        task_id="deadline-live-task",
        session_key=session_key,
        session_id=session_id,
        tool_call_id=call_id,
        turn_id=turn_id,
        run_generation=generation,
    )
    env = LocalEnvironment(cwd=str(tmp_path), timeout=1)
    controller_errors = []
    controller_state = {}

    def _observe_live_leases():
        transaction = None
        try:
            controller_state["phase"] = "transaction_wait"
            transaction = wait_until(
                lambda: getattr(handler, "_launch_transaction", None),
                timeout=10,
                what="the live deadline transaction",
            )
            controller_state["transaction"] = transaction
            controller_state["transaction_snapshot"] = _controller_transaction_snapshot(transaction)
            controller_state["phase"] = "direct_exit_wait"
            wait_until(
                lambda: transaction.process is not None and transaction.process.poll() is not None,
                timeout=10,
                what="the direct child to exit while its descendant remains",
            )
            controller_state["direct_exited"] = True
            controller_state["transaction_snapshot"] = _controller_transaction_snapshot(transaction)
            controller_state["phase"] = "stdin_alive_wait"
            wait_until(
                lambda: (
                    getattr(transaction.process, "_hermes_stdin_thread", None) is not None
                    and transaction.process._hermes_stdin_thread.is_alive()
                ),
                timeout=10,
                what="the real stdin writer to remain active",
            )
            controller_state["stdin_active"] = True
            controller_state["phase"] = "complete"
        except BaseException as exc:
            controller_state["error"] = {
                "phase": controller_state.get("phase", "unknown"),
                "type": type(exc).__name__,
                "message": str(exc)[:512],
            }
            if transaction is not None:
                controller_state["transaction_snapshot"] = _controller_transaction_snapshot(transaction)
                controller_state["error"]["transaction_snapshot"] = controller_state[
                    "transaction_snapshot"
                ]
                reader = getattr(transaction.process, "_hermes_stdout_thread", None) if transaction else None
                stdin_writer = getattr(transaction.process, "_hermes_stdin_thread", None) if transaction else None
                controller_state["diagnostics"] = failure_details(
                    tmp_path,
                    transaction,
                    controller_state=controller_state,
                    reader_thread_alive=reader.is_alive() if reader is not None else None,
                    stdin_thread_alive=stdin_writer.is_alive() if stdin_writer is not None else None,
                )
            controller_errors.append(exc)

    controller = threading.Thread(target=_observe_live_leases, daemon=True)
    controller.start()
    transaction = None
    agent = None
    db = None
    try:
        result = execute_with_diagnostics(env, 
            command,
            cwd=str(tmp_path),
            timeout=1,
            stdin_data="I" * (1024 * 1024),
            yield_handler=handler,
        )
        controller.join(timeout=10)
        assert controller.is_alive() is False
        assert controller_errors == [], controller_state
        assert controller_state["direct_exited"] is True
        assert controller_state["stdin_active"] is True
        transaction = handler._launch_transaction
        assert transaction is controller_state["transaction"]
        assert result["hermes_timed_out"] is True
        assert result["foreground_reader_pending"] is True
        evidence = transaction._foreground_custody_record.evidence
        assert evidence["deadline_expired"] is True
        assert evidence["true_eof"] is False
        assert evidence["stdout_closed"] is False
        reader_thread = evidence["reader_thread"]
        assert reader_thread is evidence["reader_state"]["thread"]
        assert handler._finalize_foreground(result) is False
        assert transaction.custody.state == "unknown"
        assert registry._foreground_custody_transactions[transaction.custody.owner_id] is transaction
        assert transaction.process_handle_closed is False
        assert transaction.job_handle_closed is False

        agent, db = _assert_real_db_callback_cannot_ack(
            tmp_path,
            transaction,
            registry,
            result,
            session_id=session_id,
            session_key=session_key,
            call_id=call_id,
            turn_id=turn_id,
            generation=generation,
            task_id="deadline-live-task",
        )
    finally:
        controller.join(timeout=1)
        if transaction is None:
            transaction = controller_state.get("transaction")
        if transaction is not None and transaction.process is not None:
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                if transaction.process.poll() is None:
                    transaction.process.kill()
                transaction.process.wait(timeout=10)
            with contextlib.suppress(AssertionError):
                wait_until(
                    lambda: transaction.observe_empty() is True,
                    timeout=10,
                    what="the retained deadline Job to become empty",
                )
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                transaction.close_after_observed_empty()
            with contextlib.suppress(Exception):
                registry.consume_foreground_custody(transaction)
        if agent is not None:
            agent._session_db = None
        if db is not None:
            db.close()
        with contextlib.suppress(Exception):
            env.cleanup()


@pytest.mark.platforms("windows")
def test_real_environment_postdeadline_native_eof_still_refuses_same_transaction(tmp_path, monkeypatch):
    """A natural child/109 EOF observed after the original deadline remains a refusal."""
    import tools.environments.base as base_environment
    import tools.process_registry as process_registry_module

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(10)
    host, port = listener.getsockname()
    descendant_path = tmp_path / "hermes-custody-f4-descendant.jsonl"
    parent_path = tmp_path / "hermes-custody-f4-parent.jsonl"
    late_code = socket_descendant_code(
        descendant_path,
        host,
        port,
        "sys.stdout.buffer.write(b'postdeadline-ready\\n'); sys.stdout.buffer.flush();",
        "sys.stdout.buffer.write(b'postdeadline-tail-\\xe2'); sys.stdout.buffer.flush(); "
        "sys.stdout.buffer.write(b'\\x82\\xac\\n'); sys.stdout.buffer.flush();",
    )
    child_code = inherited_descendant_parent_code(
        parent_path, late_code, "postdeadline-parent",
    )
    command = shell_python_command(child_code)
    session_id, session_key, call_id, turn_id, generation = (
        "deadline-eof-session", "deadline-eof-route", "deadline-eof-call", "deadline-eof-turn", 3,
    )
    handler = yield_to_background_handler(
        command=command,
        env_type="local",
        cwd=str(tmp_path),
        effective_task_id="deadline-eof-task",
        task_id="deadline-eof-task",
        session_key=session_key,
        session_id=session_id,
        tool_call_id=call_id,
        turn_id=turn_id,
        run_generation=generation,
    )
    env = LocalEnvironment(cwd=str(tmp_path), timeout=1)
    barrier_state = {}
    barrier_state["connection_state"] = "not_connected"
    barrier_ready = threading.Event()
    process_exited_observed = threading.Event()
    release_poll = threading.Event()
    controller_errors = []
    original_launch = WindowsLaunchTransaction.launch
    original_start_drain = base_environment._start_drain_thread

    def _launch_with_poll_barrier(transaction, *args, **kwargs):
        proc = original_launch(transaction, *args, **kwargs)
        original_poll = proc.poll
        first_poll_at = {}
        poll_calls = {"count": 0}

        def _poll_barrier():
            first_poll_at.setdefault("value", time.monotonic())
            actual = original_poll()
            poll_calls["count"] += 1
            barrier_ready.set()
            if actual is None:
                return None
            barrier_state["observed_returncode"] = actual
            process_exited_observed.set()
            assert release_poll.wait(10), "real non-null poll result was not released"
            return actual

        proc.poll = _poll_barrier
        barrier_state.update(
            transaction=transaction,
            process=proc,
            original_poll=original_poll,
            first_poll_at=first_poll_at,
            poll_calls=poll_calls,
            process_exited_observed=process_exited_observed,
        )
        barrier_state["transaction_snapshot"] = _controller_transaction_snapshot(transaction)
        return proc

    def _start_recording_reader(proc, output, stop=None, evidence=None, retain_until_eof=False):
        thread = original_start_drain(
            proc, output, stop, evidence, retain_until_eof=retain_until_eof,
        )
        barrier_state["reader_thread"] = thread
        barrier_state["evidence"] = evidence
        return thread

    monkeypatch.setattr(WindowsLaunchTransaction, "launch", _launch_with_poll_barrier)
    monkeypatch.setattr(base_environment, "_start_drain_thread", _start_recording_reader)

    def _release_after_real_exit_and_eof():
        connection = None
        try:
            barrier_state["phase"] = "transaction_wait"
            wait_until(
                lambda: barrier_state.get("transaction"),
                timeout=10,
                what="the postdeadline transaction",
            )
            barrier_state["phase"] = "poll_barrier_wait"
            wait_until(barrier_ready.is_set, timeout=10, what="the poll observation barrier")
            barrier_state["phase"] = "listener_accept"
            connection, _ = listener.accept()
            barrier_state["connection_state"] = "connected"
            barrier_state["phase"] = "deadline_wait"
            wait_until(
                lambda: time.monotonic() >= barrier_state["first_poll_at"]["value"] + 1,
                timeout=10,
                what="the original one-second deadline",
            )
            barrier_state["phase"] = "release_send"
            connection.sendall(b"release")
            # The retained reader observes pipe EOF independently of proc.poll.
            # Keep the main poll barrier held until the same reader has its tail.
            barrier_state["phase"] = "reader_eof_wait"
            wait_until(
                lambda: (
                    barrier_state["evidence"].get("status") == "true_eof"
                    and barrier_state["evidence"].get("eof_kind") == "win32_broken_pipe"
                    and barrier_state["evidence"]["reader_state"].get("done") is True
                ),
                timeout=10,
                what="native ERROR_BROKEN_PIPE after the late writer closes",
            )
            wait_until(
                lambda: not barrier_state["reader_thread"].is_alive(),
                timeout=10,
                what="the original reader thread to join",
            )
            barrier_state["phase"] = "process_exit_wait"
            wait_until(
                process_exited_observed.is_set,
                timeout=10,
                what="the real non-null process poll result",
            )
            assert barrier_state.get("observed_returncode") is not None
            barrier_state["reader_eof_after_deadline"] = True
            release_poll.set()
            barrier_state["phase"] = "complete"
        except BaseException as exc:
            barrier_state["error"] = {
                "phase": barrier_state.get("phase", "unknown"),
                "connection_state": barrier_state.get("connection_state", "unknown"),
                "type": type(exc).__name__,
                "message": str(exc)[:512],
            }
            transaction = barrier_state.get("transaction")
            if transaction is not None:
                barrier_state["transaction_snapshot"] = _controller_transaction_snapshot(transaction)
                barrier_state["error"]["transaction_snapshot"] = barrier_state[
                    "transaction_snapshot"
                ]
                reader = barrier_state.get("reader_thread")
                barrier_state["diagnostics"] = failure_details(
                    tmp_path,
                    transaction,
                    barrier_state={
                        "phase": barrier_state.get("phase"),
                        "connection_state": barrier_state.get("connection_state"),
                        "reader_thread_alive": reader.is_alive() if reader is not None else None,
                        "evidence": barrier_state.get("evidence"),
                    },
                )
            controller_errors.append(exc)
            release_poll.set()
        finally:
            if connection is not None:
                with contextlib.suppress(OSError):
                    connection.close()
            barrier_state["connection_state"] = "closed"

    controller = threading.Thread(target=_release_after_real_exit_and_eof, daemon=True)
    controller.start()
    transaction = None
    agent = None
    db = None
    try:
        result = execute_with_diagnostics(env, command, cwd=str(tmp_path), timeout=1, yield_handler=handler)
        controller.join(timeout=10)
        emit_reader_observation(barrier_state)
        assert controller.is_alive() is False
        assert controller_errors == [], barrier_state
        assert barrier_state["reader_eof_after_deadline"] is True
        assert barrier_state["poll_calls"]["count"] > 0
        assert barrier_state["observed_returncode"] is not None
        transaction = handler._launch_transaction
        assert transaction is barrier_state["transaction"]
        assert "postdeadline-tail-€" in result["output"]
        evidence = transaction._foreground_custody_record.evidence
        assert evidence["deadline_expired"] is True
        assert evidence["status"] == "true_eof"
        assert evidence["eof_kind"] == "win32_broken_pipe"
        assert evidence["eof_kind"] == evidence["reader_state"]["eof_kind"]
        assert evidence["reader_state"]["done"] is True
        assert evidence["true_eof"] is False
        assert evidence["stdout_closed"] is False
        assert handler._finalize_foreground(result) is False
        assert transaction.custody.state == "unknown"
        assert registry._foreground_custody_transactions[transaction.custody.owner_id] is transaction

        agent, db = _assert_real_db_callback_cannot_ack(
            tmp_path,
            transaction,
            registry,
            result,
            session_id=session_id,
            session_key=session_key,
            call_id=call_id,
            turn_id=turn_id,
            generation=generation,
            task_id="deadline-eof-task",
        )
    finally:
        controller.join(timeout=1)
        if transaction is None:
            transaction = barrier_state.get("transaction")
        if transaction is not None and transaction.process is not None:
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                if transaction.process.poll() is None:
                    transaction.process.kill()
                transaction.process.wait(timeout=10)
            with contextlib.suppress(AssertionError):
                wait_until(
                    lambda: transaction.observe_empty() is True,
                    timeout=10,
                    what="the postdeadline Job to become empty",
                )
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                transaction.close_after_observed_empty()
            with contextlib.suppress(Exception):
                registry.consume_foreground_custody(transaction)
        if agent is not None:
            agent._session_db = None
        if db is not None:
            db.close()
        with contextlib.suppress(OSError):
            listener.close()
        with contextlib.suppress(Exception):
            env.cleanup()


@pytest.mark.platforms("windows")
def test_real_environment_non109_pipe_failure_retains_same_transaction(tmp_path, monkeypatch):
    """A real invalidated reader handle is a refusal, never a managed EOF proof."""
    import tools.process_registry as process_registry_module

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(10)
    host, port = listener.getsockname()
    descendant_path = tmp_path / "hermes-custody-f5-descendant.jsonl"
    parent_path = tmp_path / "hermes-custody-f5-parent.jsonl"
    late_code = socket_descendant_code(
        descendant_path,
        host,
        port,
        "sys.stdout.write('non109-ready\\n'); sys.stdout.flush();",
        "sys.stdout.write('non109-tail\\n'); sys.stdout.flush();",
    )
    child_code = inherited_descendant_parent_code(
        parent_path, late_code, "non109-parent",
    )
    command = shell_python_command(child_code)
    session_id, session_key, call_id, turn_id, generation = (
        "non109-session", "non109-route", "non109-call", "non109-turn", 3,
    )
    handler = yield_to_background_handler(
        command=command,
        env_type="local",
        cwd=str(tmp_path),
        effective_task_id="non109-task",
        task_id="non109-task",
        session_key=session_key,
        session_id=session_id,
        tool_call_id=call_id,
        turn_id=turn_id,
        run_generation=generation,
    )
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    controller = None
    controller_errors = None
    released = None
    controller_state = {}
    transaction = None
    agent = None
    db = None
    try:
        controller, controller_errors, released, controller_state = _start_late_writer_controller(
            handler, listener, invalidate_reader=True, diagnostic_dir=tmp_path,
        )
        result = execute_with_diagnostics(env, command, cwd=str(tmp_path), yield_handler=handler)
        controller.join(timeout=10)
        assert controller.is_alive() is False
        assert controller_errors == [], failure_details(
            tmp_path, controller_state.get("transaction"), controller_state=controller_state,
        )
        assert released is not None and released.is_set()
        transaction = handler._launch_transaction
        assert transaction is controller_state["transaction"]
        evidence = transaction._foreground_custody_record.evidence
        assert evidence["status"] == "pipe_error"
        assert evidence["true_eof"] is False
        assert evidence.get("eof_kind") != "win32_broken_pipe"
        assert handler._finalize_foreground(result) is False
        assert transaction.custody.state == "unknown"
        assert registry._foreground_custody_transactions[transaction.custody.owner_id] is transaction
        assert transaction.process_handle_closed is False
        assert transaction.job_handle_closed is False
        agent, db = _assert_real_db_callback_cannot_ack(
            tmp_path,
            transaction,
            registry,
            result,
            session_id=session_id,
            session_key=session_key,
            call_id=call_id,
            turn_id=turn_id,
            generation=generation,
            task_id="non109-task",
        )
    finally:
        if controller is not None:
            controller.join(timeout=1)
        if transaction is None:
            transaction = controller_state.get("transaction")
        if transaction is not None and transaction.process is not None:
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                if transaction.process.poll() is None:
                    transaction.process.kill()
                transaction.process.wait(timeout=10)
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                if transaction.observe_empty() is True:
                    transaction.close_after_observed_empty()
            with contextlib.suppress(Exception):
                registry.consume_foreground_custody(transaction)
        if agent is not None:
            agent._session_db = None
        if db is not None:
            db.close()
        with contextlib.suppress(OSError):
            listener.close()
        with contextlib.suppress(Exception):
            env.cleanup()


@pytest.mark.platforms("windows")
def test_ordinary_local_environment_keeps_nonmanaged_reader_compatibility(tmp_path):
    """The ordinary LocalEnvironment reader keeps its historical non-managed contract."""
    evidence = {}

    def _record_evidence(_proc, observed):
        evidence.update(observed)

    handler = SimpleNamespace(
        _retain_reader_until_eof=False,
        _record_foreground_evidence=_record_evidence,
    )
    child_code = "print('ordinary-crt-control', flush=True)"
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    try:
        result = env.execute(command, cwd=str(tmp_path), yield_handler=handler)
        assert result["returncode"] == 0
        assert "ordinary-crt-control" in result["output"]
        assert evidence["reader_lease"] is False
        assert evidence["reader_state"]["done"] is True
        assert evidence["stdout_closed"] is True
        assert evidence["status"] in {"true_eof", "post_exit_idle"}
        if evidence["status"] == "true_eof":
            assert evidence.get("eof_kind") in {"win32_read_empty", "win32_broken_pipe"}
    finally:
        with contextlib.suppress(Exception):
            env.cleanup()


@pytest.mark.platforms("windows")
@pytest.mark.parametrize(
    "failure_mode",
    (
        "settlement_false", "settlement_raise", "stale_memory_marker", "stale_role",
        "stale_epoch", "stale_nested_metadata",
    ),
)
def test_managed_turn_callback_refusal_keeps_exact_pending_transaction(
    tmp_path, monkeypatch, failure_mode,
):
    """A real callback refusal or stale proof never loses the original native transaction."""
    import tools.process_registry as process_registry_module
    from agent.tool_executor import _ToolCallRef, _commit_tool_result
    from agent.context_compressor import _DB_PERSISTED_MARKER
    from hermes_state import SessionDB
    from tools.budget_config import DEFAULT_BUDGET

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")

    suffix = failure_mode.replace("_", "-")
    session_id, session_key, call_id, turn_id, generation = (
        f"managed-{suffix}", f"route-{suffix}", f"call-{suffix}", "managed-turn", 3,
    )
    child_code = "print('managed-refusal', flush=True)"
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    handler = yield_to_background_handler(
        command=command, env_type="local", cwd=str(tmp_path),
        effective_task_id=f"task-{suffix}", task_id=f"task-{suffix}",
        session_key=session_key, session_id=session_id, tool_call_id=call_id,
        turn_id=turn_id, run_generation=generation,
    )
    db = SessionDB(tmp_path / f"{suffix}.db")
    transaction = None
    agent = None
    try:
        db.create_session(session_id=session_id, source="tui", model="test/model")
        result = env.execute(command, cwd=str(tmp_path), yield_handler=handler)
        transaction = handler._launch_transaction
        assert transaction is not None
        assert handler._foreground_pending is True
        assert handler._finalize_foreground(result) is True
        original_process = transaction.process
        original_pid = transaction.pid
        agent = _persistence_agent(tmp_path, db, session_id)
        _wire_managed_turn_callback(agent, session_id, session_key, generation)
        if failure_mode == "stale_nested_metadata":
            agent.tool_result_metadata_callback = lambda *_args: {
                "nested": {"items": [{"marker": "original"}]},
            }
        assert callable(agent.tool_result_committed_callback)
        projected = []
        agent.tool_progress_callback = lambda *args, **kwargs: projected.append((args, kwargs))
        admit_calls = []
        original_admit = registry.admit_completed_foreground

        def fail_first_settlement(transaction_arg, *args, **kwargs):
            admit_calls.append((transaction_arg, dict(kwargs)))
            if len(admit_calls) == 1:
                if failure_mode == "settlement_raise":
                    raise OSError("native settlement unavailable")
                return False
            return original_admit(transaction_arg, *args, **kwargs)

        monkeypatch.setattr(registry, "admit_completed_foreground", fail_first_settlement)
        ref = _ToolCallRef(
            name="terminal", args={"command": "managed-refusal"}, task_id=f"task-{suffix}",
            call_id=call_id, trace=[],
        )
        messages = []
        committed = _commit_tool_result(
            agent, messages, ref, "managed refusal output", budget=DEFAULT_BUDGET,
            tool_duration=0.01, is_error=False, blocked=False, effect_disposition=None,
        )
        assert committed is None
        assert agent._flush_messages_to_session_db.call_count == 1
        assert projected == []
        record = transaction._foreground_custody_record
        proof = record.durable_proof
        assert len(admit_calls) == 1
        assert admit_calls[0][0] is transaction
        assert admit_calls[0][1]["session_key"] == session_key
        assert proof is not None
        assert proof.database is db
        assert proof.message is messages[-1]
        assert db.get_message_role(session_id, proof.row_id) == "tool"
        persisted_rows = db.get_messages_as_conversation(proof.session_id, include_row_ids=True)
        persisted = next(row for row in persisted_rows if row.get("_row_id") == proof.row_id)
        assert _payload_equal(proof.readback, persisted)
        assert persisted["content"] == proof.message["content"]
        assert persisted["tool_call_id"] == call_id
        assert record.phase == "delivery_committed"
        assert record.durable_proof is proof
        assert transaction.custody.state == "unknown"
        assert transaction.custody.reason == "foreground_delivery_settlement_failed"
        assert transaction.custody.durable is True
        assert transaction.custody.delivery_acked is True
        assert type(record.delivery_close_generation) is int
        assert registry._foreground_custody_transactions[transaction.custody.owner_id] is transaction
        assert transaction.process_handle_closed is False
        assert transaction.job_handle_closed is False

        def _stale_memory_marker():
            # This is intentionally the in-memory marker control.  The durable row control is
            # stale_role below; the marker itself is not a SQLite column.
            proof.message[_DB_PERSISTED_MARKER] = False

        def _stale_role():
            with sqlite3.connect(db.db_path, timeout=10) as connection:
                changed = connection.execute(
                    "UPDATE messages SET role = ? WHERE id = ? AND session_id = ?",
                    ("assistant", proof.row_id, proof.session_id),
                ).rowcount
                assert changed == 1
                connection.commit()
            assert db.get_message_role(proof.session_id, proof.row_id) == "assistant"

        def _stale_epoch():
            coordinator.authority_epoch = coordinator.authority_epoch + 1

        def _stale_nested_metadata():
            with sqlite3.connect(db.db_path, timeout=10) as connection:
                changed = connection.execute(
                    "UPDATE messages SET display_metadata = ? WHERE id = ? AND session_id = ?",
                    (json.dumps({"nested": {"items": [{"marker": "tampered"}]}}),
                     proof.row_id, proof.session_id),
                ).rowcount
                assert changed == 1
                connection.commit()
            persisted = next(
                row for row in db.get_messages_as_conversation(proof.session_id, include_row_ids=True)
                if row.get("_row_id") == proof.row_id
            )
            assert persisted["display_metadata"]["nested"]["items"][0]["marker"] == "tampered"

        {
            "stale_memory_marker": _stale_memory_marker,
            "stale_role": _stale_role,
            "stale_epoch": _stale_epoch,
            "stale_nested_metadata": _stale_nested_metadata,
        }.get(failure_mode, lambda: None)()

        # Keep the delegating spy installed through the real retry so the second
        # same-transaction settlement is observable without changing production code.
        registry.preservation_owner_evidence(
            process_instance=coordinator.process_instance,
            authority_epoch=coordinator.authority_epoch,
            close_generation=0,
            owner_id=f"{coordinator.process_instance}:I15_process_registry_terminal",
            profile_id=registry._process_profile_id(),
        )
        if failure_mode in {"settlement_false", "settlement_raise"}:
            assert len(admit_calls) == 2
            assert admit_calls[1][0] is transaction
            assert admit_calls[1][1]["session_key"] == session_key
            assert agent._flush_messages_to_session_db.call_count == 1
            assert record.durable_proof is proof
            assert registry._foreground_custody_transactions == {}
            assert transaction.process is original_process
            assert transaction.pid == original_pid
            assert transaction.process_handle_closed is True
            assert transaction.job_handle_closed is True
            assert coordinator.status()["active_owner_classes"] == []
        else:
            assert len(admit_calls) == 1
            assert agent._flush_messages_to_session_db.call_count == 1
            assert transaction.custody.state == "unknown"
            assert transaction.custody.reason == "foreground_durable_proof_missing"
            assert record.durable_proof is proof
            assert registry._foreground_custody_transactions[transaction.custody.owner_id] is transaction
            assert transaction.process is original_process
            assert transaction.pid == original_pid
            assert transaction.process_handle_closed is False
            assert transaction.job_handle_closed is False
    finally:
        if transaction is not None and transaction.process is not None:
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                if transaction.process.poll() is None:
                    transaction.process.kill()
                transaction.process.wait(timeout=10)
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                if transaction.observe_empty() is True:
                    transaction.close_after_observed_empty()
            with contextlib.suppress(Exception):
                registry.consume_foreground_custody(transaction)
        if agent is not None:
            agent._session_db = None
        db.close()
        with contextlib.suppress(Exception):
            env.cleanup()


@pytest.mark.platforms("windows")
def test_durable_result_failure_stays_unknown_after_native_job_empty(tmp_path, monkeypatch):
    """A failed durable write cannot be reaccepted merely because the Job is empty."""
    import tools.process_registry as process_registry_module

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(process_registry_module, "save_completed_result", lambda _session: False)
    monkeypatch.setattr(base_environment, "consume_yield", lambda _thread_id: False)

    child_code = "print('durability-boundary', flush=True)"
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    handler = yield_to_background_handler(
        command=command, env_type="local", cwd=str(tmp_path),
        effective_task_id="task-durable-boundary", task_id="task-durable-boundary",
        session_key="session-durable-boundary",
    )
    transaction = None
    try:
        result = env.execute(command, cwd=str(tmp_path), yield_handler=handler)
        transaction = handler._launch_transaction
        assert transaction is not None
        observed = {}
        admit = registry.admit_completed_foreground

        def capture_admission(*args, **kwargs):
            accepted = admit(*args, **kwargs)
            observed["state"] = transaction.custody.state
            observed["reason"] = transaction.custody.reason
            observed["durable"] = transaction.custody.durable
            return accepted

        monkeypatch.setattr(registry, "admit_completed_foreground", capture_admission)
        assert handler._finalize_foreground(result) is False
        assert observed == {
            "state": "unknown", "reason": "read_failed", "durable": False,
        }
        assert transaction.observe_empty() is True
        assert transaction.custody.state == "unknown"
        assert transaction.custody.durable is False
        assert transaction.custody.reconcile_unknown() is False
        assert registry._foreground_custody_transactions[transaction.custody.owner_id] is transaction
        assert transaction.process_handle_closed is False
        assert transaction.job_handle_closed is False
    finally:
        if transaction is not None and transaction.process is not None:
            if transaction.process.poll() is None:
                transaction.process.kill()
                transaction.process.wait(timeout=10)
            if transaction.observe_empty() is True:
                transaction.close_after_observed_empty()


@pytest.mark.platforms("windows")
@pytest.mark.parametrize(
    ("status", "eof_kind", "deadline_expired"),
    (("post_exit_idle", None, False), ("true_eof", "win32_read_empty", False),
     ("true_eof", "win32_broken_pipe", True)),
)
def test_completed_admission_rejects_idle_or_ambiguous_drain_evidence(
    tmp_path, monkeypatch, status, eof_kind, deadline_expired,
):
    """Post-exit idle and pipe errors never become a fabricated reader EOF."""
    import tools.process_registry as process_registry_module

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    custody = registry.reserve_foreground_custody()
    transaction = WindowsLaunchTransaction(custody)
    custody.native_scope = transaction
    proc = transaction.launch(
        [sys.executable, "-c", "print('idle-control', flush=True)"], **_popen_kwargs(tmp_path)
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.read().strip() == "idle-control"
        proc.wait(timeout=10)
        evidence = {
            "status": status, "eof_kind": eof_kind, "true_eof": status == "true_eof",
            "deadline_expired": deadline_expired,
            "natural_exit": True,
            "drain_joined": True, "stdin_writer_joined": True, "stdout_closed": True,
            "process": proc, "pid": proc.pid,
        }
        assert registry.admit_completed_foreground(
            transaction, command="idle-control", cwd=str(tmp_path), task_id="task-idle",
            owner_task_id="task-idle", session_key="session-idle", output="idle-control\n",
            returncode=proc.returncode, evidence=evidence,
        ) is False
        assert custody.state == "unknown"
        assert registry._foreground_custody_transactions[custody.owner_id] is transaction
    finally:
        if proc.poll() is None:
            with contextlib.suppress(OSError):
                proc.kill()
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                proc.wait(timeout=10)
        if transaction.job is not None and not transaction.job_handle_closed:
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                transaction.job.close()
        if proc.stdout is not None:
            with contextlib.suppress(OSError, ValueError):
                proc.stdout.close()


@pytest.mark.platforms("windows")
def test_managed_kill_uses_same_job_and_finishes_original_reader(tmp_path, monkeypatch):
    """Stopping a yielded command uses its Job and lets its original reader settle it."""
    import tools.process_registry as process_registry_module

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(10)
    host, port = listener.getsockname()
    descendant_code = (
        "import socket,sys; "
        f"peer=socket.create_connection(({host!r}, {port})); "
        "print('managed-kill-descendant', flush=True); peer.recv(1)"
    )
    child_code = (
        "import subprocess,sys,time; "
        f"subprocess.Popen([sys.executable, '-c', {descendant_code!r}], "
        "stdin=sys.stdin.buffer, stdout=sys.stdout.buffer, stderr=sys.stdout.buffer, close_fds=False); "
        "print('managed-kill-parent', flush=True); time.sleep(60)"
    )
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    yield_once = {"pending": True}
    monkeypatch.setattr(
        base_environment,
        "consume_yield",
        lambda _thread_id: yield_once.pop("pending", False),
    )
    handler = yield_to_background_handler(
        command=command,
        env_type="local",
        cwd=str(tmp_path),
        effective_task_id="task-managed-kill",
        task_id="task-managed-kill",
        session_key="session-managed-kill",
    )
    controller = None
    release = None
    session = None
    transaction = None
    killer = None
    allow_return = None
    original_terminate = None
    try:
        controller, controller_errors, connected, release, _controller_state = (
            _start_blocking_inherited_writer_controller(handler, listener)
        )
        result = env.execute(command, cwd=str(tmp_path), yield_handler=handler)
        session = registry.get(result["yielded_session_id"])
        assert session is not None
        assert session.custody is not None
        transaction = session.custody.native_scope
        assert transaction is not None
        wait_until(connected.is_set, timeout=10, what="the inherited writer connection")
        wait_until(
            lambda: (
                "managed-kill-parent" in session.output_buffer
                and "managed-kill-descendant" in session.output_buffer
            ),
            timeout=10,
            what="the parent and inherited writer output",
        )

        transfer = session.custody.foreground_transfer
        assert transfer is not None
        original_process = transaction.process
        original_job = transaction.job
        original_reader = transfer.reader_thread
        assert original_process is session.process
        assert original_job is not None
        assert original_reader is session._reader_thread

        native_requested = threading.Event()
        allow_return = threading.Event()
        terminate_calls = []
        original_terminate = transaction.terminate_owned_tree

        def delayed_terminate():
            terminate_calls.append(True)
            requested = original_terminate()
            native_requested.set()
            assert allow_return.wait(timeout=10)
            return requested

        transaction.terminate_owned_tree = delayed_terminate
        kill_result = {}

        def stop_from_owner_thread():
            kill_result["value"] = registry.kill_process(
                session.id, source="user.stop", consume_output=False,
            )

        killer = threading.Thread(target=stop_from_owner_thread, daemon=True)
        killer.start()
        wait_until(native_requested.is_set, timeout=10, what="the native Job termination request")
        assert session.exited is False
        assert session._completion_event.is_set() is False
        assert transaction.process is original_process
        assert transaction.job is original_job
        assert transaction.process_handle_closed is False
        assert transaction.job_handle_closed is False
        assert transaction.state == "termination_requested"
        assert transfer.reader_thread is original_reader
        assert transfer.phase != "terminal"
        assert original_terminate() is False

        allow_return.set()
        killer.join(timeout=10)
        assert killer.is_alive() is False
        first = kill_result["value"]
        assert first["status"] == "kill_requested"
        assert first["completion_pending"] is True
        assert first["completion_reason"] == "killed"
        assert first["termination_source"] == "user.stop"
        assert len(terminate_calls) == 1

        _drain_windows_completion(registry, session)
        assert session.exited is True
        assert session.exit_code == -15
        assert session.completion_reason == "killed"
        assert session.termination_source == "user.stop"
        assert session.custody_state == "terminal"
        assert transaction.process is original_process
        assert transaction.job is original_job
        assert transaction.native_empty_observed is True
        assert transaction.process_handle_closed is True
        assert transaction.job_handle_closed is True
        assert transfer.reader_thread is original_reader
        assert session.output_buffer.count("managed-kill-parent") == 1
        assert session.output_buffer.count("managed-kill-descendant") == 1

        replay = registry.kill_process(session.id, source="user.stop.replay", consume_output=True)
        assert replay["status"] == "already_exited"
        assert len(terminate_calls) == 1
    finally:
        if release is not None:
            release.set()
        if allow_return is not None:
            allow_return.set()
        if transaction is not None and original_terminate is not None:
            transaction.terminate_owned_tree = original_terminate
        if killer is not None:
            killer.join(timeout=1)
        if controller is not None:
            controller.join(timeout=10)
            assert controller.is_alive() is False
            assert controller_errors == []
        with contextlib.suppress(OSError):
            listener.close()
        if transaction is not None and transaction.process is not None:
            with contextlib.suppress(Exception):
                if transaction.process.poll() is None:
                    transaction.terminate_owned_tree()
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                transaction.process.wait(timeout=10)
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                if transaction.observe_empty() is True:
                    transaction.close_after_observed_empty()
        with contextlib.suppress(Exception):
            env.cleanup()


@pytest.mark.platforms("windows")
def test_managed_kill_retries_same_job_after_real_boundary_failure(tmp_path, monkeypatch):
    """A real Job API failure retries once on the same handle, then the original reader settles."""
    import tools.process_registry as process_registry_module

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    yield_once = {"pending": True}
    monkeypatch.setattr(
        base_environment,
        "consume_yield",
        lambda _thread_id: yield_once.pop("pending", False),
    )

    child_code = "import time; print('managed-kill-failure', flush=True); time.sleep(60)"
    command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    parent_session_id = "conversation-managed-kill-failure"
    monkeypatch.setenv("HERMES_SESSION_ID", parent_session_id)
    handler = yield_to_background_handler(
        command=command,
        env_type="local",
        cwd=str(tmp_path),
        effective_task_id="task-managed-kill-failure",
        task_id="task-managed-kill-failure",
        session_key="session-managed-kill-failure",
        session_id=parent_session_id,
        tool_call_id="call-managed-kill-failure", turn_id="turn-managed-kill-failure", run_generation=3,
    )
    transaction = None
    try:
        result = env.execute(command, cwd=str(tmp_path), yield_handler=handler)
        session = registry.get(result["yielded_session_id"])
        assert session is not None
        assert session.parent_session_id == parent_session_id
        assert session.custody.foreground_binding == ForegroundParentBinding(
            parent_session_id, "session-managed-kill-failure", "call-managed-kill-failure",
            "turn-managed-kill-failure", 3,
        )
        transaction = session.custody.native_scope
        assert transaction is not None
        wait_until(
            lambda: "managed-kill-failure" in session.output_buffer,
            timeout=10,
            what="the managed stop failure child output",
        )
        transfer = session.custody.foreground_transfer
        assert transfer is not None
        original_process = transaction.process
        original_job = transaction.job
        original_reader = transfer.reader_thread
        original_job_handle = transaction.job._handle
        native_calls = []
        real_terminate = transaction.job._api.TerminateJobObject
        real_terminate.argtypes = [wintypes.HANDLE, wintypes.UINT]
        real_terminate.restype = wintypes.BOOL

        def fail_once_then_call_real_api(handle, exit_code):
            native_calls.append((handle, exit_code))
            if len(native_calls) == 1:
                ctypes.set_last_error(5)
                return False
            return real_terminate(handle, exit_code)

        monkeypatch.setattr(
            transaction.job._api,
            "TerminateJobObject",
            fail_once_then_call_real_api,
        )
        monkeypatch.setattr(
            registry,
            "_terminate_host_pid",
            lambda *_args, **_kwargs: pytest.fail("managed stop used a PID fallback"),
        )

        refusal = registry.kill_process(
            session.id, source="user.stop", consume_output=False,
        )
        assert refusal["status"] == "error"
        assert refusal["error"] == "managed_foreground_termination_unknown"
        assert session.exited is False
        assert session._completion_event.is_set() is False
        assert transfer.phase == "unknown"
        assert session.custody_state == "unknown"
        assert transaction.state == "termination_failed"
        assert transaction.termination_attempts == 1
        assert transaction.termination_failures == 1
        assert transaction.termination_retryable is True
        assert transaction.process is original_process
        assert transaction.job is original_job
        assert transaction.termination_job is original_job
        assert transaction.termination_job_handle is original_job_handle
        assert transfer.reader_thread is original_reader
        assert transaction.process_handle_closed is False
        assert transaction.job_handle_closed is False
        assert original_process.poll() is None

        mismatch = registry.kill_process(
            session.id, source="other.stop", consume_output=False,
        )
        assert mismatch["status"] == "error"
        assert mismatch["error"] == "managed_foreground_termination_retry_refused"
        assert transaction.termination_attempts == 1
        assert len(native_calls) == 1

        retry = registry.kill_process(
            session.id, source="user.stop", consume_output=False,
        )
        assert retry["status"] == "kill_requested"
        assert retry["completion_pending"] is True
        assert transfer.kill_requested is True
        assert transaction.termination_retryable is False
        assert transaction.state == "termination_requested"
        assert transaction.termination_attempts == 2
        assert transaction.termination_failures == 1
        assert len(native_calls) == 2
        assert native_calls[0][0] == original_job_handle
        assert native_calls[1][0] == original_job_handle
        assert native_calls[0][1] == native_calls[1][1]
        assert session.custody_state == "unknown"
        assert transaction.custody.durable is False
        assert transaction.custody.streams_closed is False

        event = _drain_windows_completion(registry, session)
        assert event["completion_reason"] == "killed"
        assert event["termination_source"] == "user.stop"
        assert session.exited is True
        assert session.exit_code == -15
        assert session.completion_reason == "killed"
        assert session.termination_source == "user.stop"
        assert session.id in registry._finished
        assert session.custody_reader_eof is True
        assert session.custody_durable is True
        assert session.custody_handle_closed is True
        assert session.custody_state == "terminal"
        assert transaction.process is original_process
        assert transaction.job is original_job
        assert transfer.reader_thread is original_reader
        assert len(native_calls) == 2

        replay = registry.kill_process(
            session.id, source="user.stop", consume_output=True,
        )
        assert replay["status"] == "already_exited"
        assert len(native_calls) == 2
    finally:
        if transaction is not None and transaction.process is not None:
            with contextlib.suppress(Exception):
                if transaction.process.poll() is None:
                    transaction.terminate_owned_tree()
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                transaction.process.wait(timeout=10)
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                if transaction.observe_empty() is True:
                    transaction.close_after_observed_empty()
        with contextlib.suppress(Exception):
            env.cleanup()

