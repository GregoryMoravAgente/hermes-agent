"""Consuming Windows background-process custody controls.

The positive cases enter through ``ProcessRegistry.spawn_local`` and consume the
normal reader, durable-result, delivery, Job-empty, and exact-handle close path.
The failure cases retain the same source-owned transaction; they never replace a
child or use a PID-only cleanup proof.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence
from tools.process_registry import ProcessRegistry
from tools import terminal_tool_background
from tests.tools.foreground_custody_diagnostics import shell_python_command
from tests.tools.process_registry_native_profile import prepare_native_profile


def _coordinator() -> RuntimeCustodyCoordinator:
    return RuntimeCustodyCoordinator(
        process_instance="windows-background-producer-test",
        authority_epoch=11,
        fence=RetirementFence(),
    )


def _child(code: str) -> str:
    return shell_python_command(code)


def _registry(tmp_path, monkeypatch) -> ProcessRegistry:
    import tools.process_registry as process_registry_module

    registry = ProcessRegistry()
    registry.bind_runtime_custody(_coordinator())
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    prepare_native_profile(tmp_path, monkeypatch)
    return registry


def _spawn(registry, command: str, tmp_path, *, notify_on_complete: bool = True):
    return registry.spawn_local(
        command,
        cwd=str(tmp_path),
        task_id="windows-background-task",
        owner_task_id="windows-background-owner",
        session_key="windows-background-session",
        persist_on_release=True,
        notify_on_complete=notify_on_complete,
    )


def _drain_and_ack(registry, session):
    events = registry.drain_notifications(session_key=session.session_key)
    matching = [event for event, _text in events if event.get("session_id") == session.id]
    assert len(matching) == 1
    token = matching[0].get("delivery_token")
    assert isinstance(token, str) and token
    registry.acknowledge_completion_delivery(token)
    return matching[0]


@pytest.mark.platforms("windows")
def test_real_spawn_local_reaches_reader_durable_delivery_and_exact_close(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    session = _spawn(registry, _child("import time; print('background-custody-ok', flush=True); time.sleep(0.2)"), tmp_path)
    transaction = session.custody.native_scope
    assert transaction is not None
    assert transaction.matches(session.process, session.pid, session.host_start_time)
    assert transaction.assigned is True
    assert transaction.scope_captured is True
    assert transaction.resumed is True
    assert session.custody_process_bound is True
    assert session.reader_phase in {"starting", "started"}

    result = registry.wait_for_pending_completions(
        task_id=session.owner_task_id, timeout=10, poll_interval=0.05,
    )
    assert session.id in result["completed"]
    assert session._completion_event.is_set()
    assert session.reader_phase == "eof"
    assert session.custody_reader_eof is True
    assert session.custody_durable is True
    assert session.custody_scope_settled is True
    assert transaction.observe_empty() is True

    # The production notification queue emits the token after durable persistence;
    # acknowledge only that emitted token and let the normal finalizer close custody.
    event = _drain_and_ack(registry, session)
    assert event["session_id"] == session.id
    assert session.custody_delivery_acked is True
    assert session.custody_handle_closed is True
    assert transaction.process_handle_closed is True
    assert transaction.job_handle_closed is True
    assert session.custody_handle_closed is True


@pytest.mark.platforms("windows")
def test_real_fast_public_producer_emits_one_token_before_final_close(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    import tools.process_registry as process_registry_module

    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(terminal_tool_background, "_async_delivery_state", lambda *_args: True)
    result = json.loads(terminal_tool_background.spawn_background_process(
        command=_child("print('fast-background-custody', flush=True)"),
        env=SimpleNamespace(env={}, host_cwd=str(tmp_path)),
        env_type="local", effective_task_id="windows-background-task",
        task_id="windows-background-task", session_key="windows-background-session",
        workdir=str(tmp_path), cwd=str(tmp_path), effective_pty=False,
        notify_on_complete=True, watch_patterns=None, approval_note=None,
        completion_output_chars=0, pty_disabled_reason=None, heartbeat_seconds=0,
        persist_on_release=True, mounted_host=str(tmp_path), auto_backgrounded=False,
    ))
    assert result["notify_on_complete"] is True
    session = registry.get(result["session_id"])
    assert session is not None
    transaction = session.custody.native_scope
    assert transaction is not None
    assert transaction.matches(session.process, session.pid, session.host_start_time)
    settled = registry.wait_for_pending_completions(
        task_id=session.owner_task_id, timeout=10, poll_interval=0.05,
    )
    assert settled["completed"] == [session.id]
    assert session.reader_phase == "eof"
    assert session.custody_durable is True
    assert transaction.observe_empty() is True
    event = _drain_and_ack(registry, session)
    assert event["session_id"] == session.id
    assert session.custody_delivery_acked is True
    assert session.custody_handle_closed is True
    assert transaction.process_handle_closed is True
    assert transaction.job_handle_closed is True


@pytest.mark.platforms("windows")
def test_public_native_kill_uses_same_job_and_stays_pending_until_reader_settles(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    session = _spawn(registry, _child("import time; print('held', flush=True); time.sleep(30)"), tmp_path)
    transaction = session.custody.native_scope
    assert transaction.scope_ready is True
    result = registry.kill_process(session.id, source="test.native.kill")
    assert result["status"] == "killing"
    assert transaction.termination_attempts == 1
    assert transaction.state == "termination_requested"
    assert session.exited is False
    # A native request is not a completion proof; the retained reader owns EOF and
    # the normal durability/delivery/Job-empty finalizer remains responsible.
    settled = registry.wait_for_pending_completions(
        task_id=session.owner_task_id, timeout=10, poll_interval=0.05,
    )
    assert session.id in settled["completed"]
    assert session.reader_phase == "eof"
    assert transaction.observe_empty() is True
    _drain_and_ack(registry, session)
    assert session.custody_delivery_acked is True
    assert session.custody_handle_closed is True


@pytest.mark.platforms("windows")
def test_pre_effect_preparation_failure_retains_unknown_on_release_refusal(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)

    def fail_argv(*_args, **_kwargs):
        raise OSError("argv preparation refused")

    monkeypatch.setattr(registry, "_scope_argv", fail_argv)
    monkeypatch.setattr(registry, "_custody_release_unaccepted", lambda _session: False)
    with pytest.raises(OSError, match="argv preparation refused"):
        _spawn(registry, _child("print('must-not-run', flush=True)"), tmp_path)

    retained = next(iter(registry._running.values()))
    assert retained.process is None
    assert retained.pid is None
    assert retained.custody_state == "unknown"
    assert retained.custody_error == "native_pre_effect_release_failed"



@pytest.mark.platforms("windows")
def test_post_popen_assignment_failure_retains_exact_session_and_no_windows_scope(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    import tools.process_registry_windows as windows_module

    original_job = windows_module.WindowsLaunchTransaction._native_job_type
    real_job_type = original_job(None)

    class AssignFails:
        def __init__(self):
            self._job = real_job_type()

        def assign(self, _process):
            raise OSError("assignment boundary refused")

        def __getattr__(self, name):
            return getattr(self._job, name)

    monkeypatch.setattr(
        windows_module.WindowsLaunchTransaction,
        "_native_job_type",
        lambda _transaction: AssignFails,
    )
    with pytest.raises(OSError, match="assignment boundary refused"):
        _spawn(registry, _child("import time; time.sleep(30)"), tmp_path)

    retained = next(iter(registry._running.values()))
    transaction = retained.custody.native_scope
    assert transaction is not None
    assert transaction.process is not None
    assert transaction.matches(retained.process, retained.pid, retained.host_start_time)
    assert transaction.assigned is False
    assert retained.custody_process_bound is False
    assert retained.custody_state == "unknown"
    assert transaction.no_effect_verified is False
    # The public registry first observes exact Job membership.  This fake refuses before
    # the native assign call, so the suspended child is proven a Job nonmember and is
    # terminated through its retained process handle; no empty-Job guess or PID fallback.
    assert registry.get(retained.id) is retained
    kill = registry.kill_process(retained.id, source="test.assignment.ambiguous")
    assert kill["status"] == "killing"
    completion = registry.wait_for_pending_completions(
        task_id=retained.owner_task_id, timeout=10, poll_interval=0.05,
    )
    assert retained.id in completion["completed"]
    _drain_and_ack(registry, retained)
    assert transaction.assignment_attempted is True
    assert transaction.assignment_observation == "nonmember"
    assert transaction.termination_process is transaction.process
    assert transaction.termination_job is None
    assert transaction.termination_attempts == 1


@pytest.mark.platforms("windows")
def test_post_call_assignment_failure_uses_exact_job_membership(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    import tools.process_registry_windows as windows_module

    original_job = windows_module.WindowsLaunchTransaction._native_job_type
    real_job_type = original_job(None)

    class AssignAfterEffect:
        def __init__(self):
            self._job = real_job_type()

        def assign(self, process):
            self._job.assign(process)
            raise OSError("assignment result became ambiguous")

        def __getattr__(self, name):
            return getattr(self._job, name)

    monkeypatch.setattr(
        windows_module.WindowsLaunchTransaction,
        "_native_job_type",
        lambda _transaction: AssignAfterEffect,
    )
    with pytest.raises(OSError, match="assignment result became ambiguous"):
        _spawn(registry, _child("import time; time.sleep(30)"), tmp_path)

    retained = next(iter(registry._running.values()))
    transaction = retained.custody.native_scope
    assert transaction is not None
    original_process = transaction.process
    original_job_handle = transaction.job._handle
    first_kill = registry.kill_process(retained.id, source="test.assignment.post_call")
    assert first_kill["status"] == "killing"
    assert transaction.assignment_observation == "member"
    # Membership is not itself a scope receipt: recovery re-captures the same
    # Job/process/start tuple and binds the existing lease before termination.
    assert transaction.assigned is True
    assert transaction.scope_captured is True
    assert transaction.resumed is False
    assert retained.custody_process_bound is True
    assert transaction.termination_process is None
    assert transaction.termination_job is transaction.job
    assert transaction.termination_job_handle is original_job_handle
    assert transaction.process is original_process
    settled = registry.wait_for_pending_completions(
        task_id=retained.owner_task_id, timeout=10, poll_interval=0.05,
    )
    assert retained.id in settled["completed"]
    assert transaction.observe_empty() is True
    _drain_and_ack(registry, retained)
    assert transaction.process_handle_closed is True
    assert transaction.job_handle_closed is True


@pytest.mark.platforms("windows")
def test_post_call_member_rebind_failure_keeps_same_objects_unknown(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    import tools.process_registry_windows as windows_module

    original_job = windows_module.WindowsLaunchTransaction._native_job_type
    real_job_type = original_job(None)

    class AssignAfterEffect:
        def __init__(self):
            self._job = real_job_type()

        def assign(self, process):
            self._job.assign(process)
            raise OSError("assignment result became ambiguous")

        def __getattr__(self, name):
            return getattr(self._job, name)

    monkeypatch.setattr(
        windows_module.WindowsLaunchTransaction,
        "_native_job_type",
        lambda _transaction: AssignAfterEffect,
    )
    with pytest.raises(OSError, match="assignment result became ambiguous"):
        _spawn(registry, _child("import time; time.sleep(30)"), tmp_path)

    retained = next(iter(registry._running.values()))
    transaction = retained.custody.native_scope
    assert transaction is not None
    original_process, original_job = transaction.process, transaction.job
    original_capture = windows_module.WindowsLaunchTransaction._capture_scope_identity

    def refuse_capture(_transaction):
        raise OSError("member scope recapture refused")

    monkeypatch.setattr(
        windows_module.WindowsLaunchTransaction, "_capture_scope_identity", refuse_capture,
    )
    refused = registry.kill_process(retained.id, source="test.assignment.rebind_failure")
    assert refused["status"] == "error"
    assert transaction.process is original_process
    assert transaction.job is original_job
    assert transaction.assigned is False
    assert transaction.scope_captured is False
    assert transaction.assignment_observation == "member"
    assert transaction.termination_attempts == 0
    assert retained.custody_state == "unknown"

    # Restore the exact source method only to clean the retained child through
    # the same original Job; the failed branch never releases or replaces it.
    monkeypatch.setattr(
        windows_module.WindowsLaunchTransaction, "_capture_scope_identity", original_capture,
    )
    cleanup = registry.kill_process(retained.id, source="test.assignment.rebind_cleanup")
    assert cleanup["status"] == "killing"
    settled = registry.wait_for_pending_completions(
        task_id=retained.owner_task_id, timeout=10, poll_interval=0.05,
    )
    assert retained.id in settled["completed"]
    assert transaction.observe_empty() is True
    _drain_and_ack(registry, retained)
    assert transaction.process_handle_closed is True
    assert transaction.job_handle_closed is True


@pytest.mark.platforms("windows")
def test_assignment_membership_query_unavailable_retries_same_job_and_process(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    import tools.process_registry_windows as windows_module

    original_job = windows_module.WindowsLaunchTransaction._native_job_type
    real_job_type = original_job(None)

    class AssignAfterQueryUnavailable:
        def __init__(self):
            self._job = real_job_type()
            self.query_count = 0

        def assign(self, process):
            self._job.assign(process)
            raise OSError("assignment query is ambiguous")

        def contains_process_handle(self, process_ref):
            self.query_count += 1
            if self.query_count == 1:
                return None
            return self._job.contains_process_handle(process_ref)

        def __getattr__(self, name):
            return getattr(self._job, name)

    monkeypatch.setattr(
        windows_module.WindowsLaunchTransaction,
        "_native_job_type",
        lambda _transaction: AssignAfterQueryUnavailable,
    )
    with pytest.raises(OSError, match="assignment query is ambiguous"):
        _spawn(registry, _child("import time; time.sleep(30)"), tmp_path)

    retained = next(iter(registry._running.values()))
    transaction = retained.custody.native_scope
    assert transaction is not None
    original_process = transaction.process
    original_job = transaction.job
    original_job_handle = original_job._handle
    refused = registry.kill_process(retained.id, source="test.assignment.query_unavailable")
    assert refused["status"] == "error"
    assert transaction.assignment_observation == "query_unavailable"
    assert transaction.termination_attempts == 0
    retried = registry.kill_process(retained.id, source="test.assignment.query_unavailable")
    assert retried["status"] == "killing"
    assert transaction.assignment_observation == "member"
    assert transaction.assigned is True
    assert transaction.scope_captured is True
    assert transaction.resumed is False
    assert retained.custody_process_bound is True
    assert transaction.process is original_process
    assert transaction.job is original_job
    assert transaction.termination_job is original_job
    assert transaction.termination_job_handle is original_job_handle
    settled = registry.wait_for_pending_completions(
        task_id=retained.owner_task_id, timeout=10, poll_interval=0.05,
    )
    assert retained.id in settled["completed"]
    assert transaction.observe_empty() is True
    _drain_and_ack(registry, retained)
    assert transaction.process_handle_closed is True
    assert transaction.job_handle_closed is True


@pytest.mark.platforms("windows")
def test_scope_capture_failure_retains_exact_job_for_public_retry(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    import tools.process_registry_windows as windows_module

    def fail_capture(_transaction):
        raise OSError("scope capture boundary refused")

    monkeypatch.setattr(
        windows_module.WindowsLaunchTransaction, "_capture_scope_identity", fail_capture,
    )
    with pytest.raises(OSError, match="scope capture boundary refused"):
        _spawn(registry, _child("import time; time.sleep(30)"), tmp_path)

    retained = next(iter(registry._running.values()))
    transaction = retained.custody.native_scope
    assert transaction is not None
    assert transaction.assigned is True
    assert transaction.assignment_attempted is True
    assert transaction.scope_captured is False
    assert transaction.scope_capture_ambiguous is True
    assert transaction.can_bind_process is False
    original_job, original_handle = transaction.job, transaction.job._handle

    result = registry.kill_process(retained.id, source="test.scope_capture.ambiguous")
    assert result["status"] == "killing"
    completion = registry.wait_for_pending_completions(
        task_id=retained.owner_task_id, timeout=10, poll_interval=0.05,
    )
    assert retained.id in completion["completed"]
    assert transaction.termination_job is original_job
    assert transaction.termination_job_handle is original_handle
    assert transaction.observe_empty() is None
    assert retained.custody_state == "unknown"
    _drain_and_ack(registry, retained)
    assert transaction.job is original_job
    assert transaction.job._handle is original_handle


@pytest.mark.platforms("windows")
def test_scope_or_resume_failure_keeps_assigned_job_without_claiming_resume(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    import psutil

    monkeypatch.setattr(psutil.Process, "resume", lambda _process: (_ for _ in ()).throw(OSError("resume refused")))
    with pytest.raises(OSError, match="resume refused"):
        _spawn(registry, _child("import time; time.sleep(30)"), tmp_path)

    retained = next(iter(registry._running.values()))
    transaction = retained.custody.native_scope
    assert transaction is not None
    assert transaction.assigned is True
    assert transaction.scope_captured is True
    assert transaction.resumed is False
    assert transaction.can_bind_process is True
    assert retained.custody_process_bound is True
    assert retained.custody_state == "unknown"
    assert registry.kill_process(retained.id, source="test.resume.unknown")["status"] == "killing"
    completion = registry.wait_for_pending_completions(
        task_id=retained.owner_task_id, timeout=10, poll_interval=0.05,
    )
    assert retained.id in completion["completed"]
    assert transaction.observe_empty() is True
    _drain_and_ack(registry, retained)
    assert transaction.termination_attempts == 1


@pytest.mark.platforms("windows")
def test_reader_constructor_failure_allows_one_same_session_retry(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    import threading

    original_thread = threading.Thread
    calls = {"count": 0}

    def fail_constructor(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("reader construction boundary refused")
        return original_thread(*args, **kwargs)

    monkeypatch.setattr(threading, "Thread", fail_constructor)
    with pytest.raises(RuntimeError, match="reader construction boundary refused"):
        _spawn(registry, _child("import time; print('reader-constructor-retry', flush=True); time.sleep(0.2)"), tmp_path)

    retained = next(iter(registry._running.values()))
    assert retained.reader_phase == "not_started"
    original_process = retained.process
    assert registry._ensure_reader_registered(retained) is True
    assert retained._reader_thread is not None
    assert retained.process is original_process
    assert retained.reader_phase == "started"
    completion = registry.wait_for_pending_completions(
        task_id=retained.owner_task_id, timeout=10, poll_interval=0.05,
    )
    assert retained.id in completion["completed"]
    _drain_and_ack(registry, retained)


@pytest.mark.platforms("windows")
def test_reader_start_ambiguous_never_creates_second_reader(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    import threading

    original_start = threading.Thread.start
    calls = {"count": 0}

    def fail_once(thread):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("reader start boundary refused")
        return original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", fail_once)
    with pytest.raises(RuntimeError, match="reader start boundary refused"):
        _spawn(registry, _child("print('reader-retry', flush=True)"), tmp_path)

    retained = next(iter(registry._running.values()))
    assert retained.reader_phase == "ambiguous"
    reader = retained._reader_thread
    assert reader is not None
    # Ambiguous start does not create a second reader.  The public retry is only
    # legal for a proven never-started construction; it must refuse this state.
    assert registry._ensure_reader_registered(retained) is False
    assert retained._reader_thread is reader
    assert retained.reader_phase == "ambiguous"
    kill = registry.kill_process(retained.id, source="test.reader_start.ambiguous")
    assert kill["status"] == "killing"
    assert retained.custody.state == "unknown"
    assert retained.custody.native_scope.termination_attempts == 1


@pytest.mark.platforms("windows")
def test_foreign_job_observation_remains_unknown_and_cannot_close(tmp_path, monkeypatch):
    registry = _registry(tmp_path, monkeypatch)
    session = _spawn(registry, _child("import time; time.sleep(30)"), tmp_path)
    transaction = session.custody.native_scope
    assert transaction is not None
    original_job = transaction.scope_observation_job
    transaction.scope_observation_job = object()
    try:
        assert transaction.observe_empty() is None
        assert transaction.scope_observation == "foreign"
        assert transaction.close_after_observed_empty() is False
        assert session.custody_state == "accepted"
    finally:
        transaction.scope_observation_job = original_job
        registry.kill_process(session.id, source="test.foreign.cleanup")
        registry.wait_for_pending_completions(
            task_id=session.owner_task_id, timeout=10, poll_interval=0.05,
        )
