"""Windows native scope retry through the real environment and registry consumers.

The positive case uses LocalEnvironment, the production yield handler, the real reader,
ProcessRegistry finalization, and completion acknowledgement. Lower-level cases inject one
native query failure or a foreign Job identity only after a real transaction exists; neither
creates a replacement process or treats a missing observation as empty.
"""
from __future__ import annotations

import contextlib
import ctypes
import json
import socket
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence
import tools.environments.base as base_environment
from tools.environments.local import LocalEnvironment
from tools.process_registry import ProcessRegistry
from tools.process_registry_custody import ForegroundParentBinding, ProcessCustody
from tools.process_registry_foreground_retry import (
    NATIVE_SCOPE_NONEMPTY,
    NATIVE_SCOPE_OBSERVATION_FAILURE,
    mark_native_scope_unknown,
    prepare_native_scope_retry,
)
from tools.process_registry_windows import WindowsLaunchTransaction
from tools.terminal_tool_background import yield_to_background_handler
from tests.tools.foreground_custody_diagnostics import (
    child_phase_prefix, execute_with_diagnostics, failure_details, shell_python_command,
)
from tests.tools.process_registry_native_profile import _popen_kwargs, prepare_native_profile
from tests.waiting import wait_until


def _coordinator() -> RuntimeCustodyCoordinator:
    return RuntimeCustodyCoordinator(
        process_instance="windows-native-scope-retry-test",
        authority_epoch=11,
        fence=RetirementFence(),
    )


def _observe_native_queries(transaction, monkeypatch):
    """Record the exact retained-Job query results without changing their behavior."""
    trace = []
    job = transaction.job
    get_last_error = getattr(ctypes, "get_last_error", lambda: None)
    max_trace_records = 64

    def _identity_fields():
        process = transaction.process
        job_handle = getattr(job, "_handle", None)
        return {
            "job_object_id": id(job) if job is not None else None,
            "job_handle_object_id": id(job_handle) if job_handle is not None else None,
            "process_object_id": id(process) if process is not None else None,
            "process_pid": getattr(process, "pid", None),
            "process_host_start_time": transaction.host_start_time,
        }

    def _record(operation, result=None, error=None):
        observation = {
            "operation": operation,
            "result": result,
            "last_error": get_last_error(),
            "assigned": transaction.assigned,
            "resumed": transaction.resumed,
            "scope_observation": transaction.scope_observation,
            "scope_observation_failures": transaction.scope_observation_failures,
            "native_empty_observed": transaction.native_empty_observed,
            "process_handle_closed": transaction.process_handle_closed,
            "job_handle_closed": transaction.job_handle_closed,
        }
        observation.update(_identity_fields())
        if error is not None:
            observation["error_type"] = type(error).__name__
            observation["error_message"] = str(error)[:512]
        if len(trace) < max_trace_records - 1:
            trace.append(observation)
        elif len(trace) == max_trace_records - 1:
            trace.append({
                "operation": "trace_overflow",
                "max_trace_records": max_trace_records,
                **_identity_fields(),
            })

    original_contains = job.contains_process_handle
    original_empty = job.empty

    def _contains(process_ref):
        try:
            result = original_contains(process_ref)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            _record("contains_process_handle_error", error=exc)
            raise
        _record("contains_process_handle", result)
        return result

    def _empty():
        try:
            result = original_empty()
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            _record("empty_error", error=exc)
            raise
        _record("empty", result)
        return result

    monkeypatch.setattr(job, "contains_process_handle", _contains)
    monkeypatch.setattr(job, "empty", _empty)
    _record("observer_start")
    return trace


def _wait_for_descendant_marker(marker, trace, transaction):
    """Keep the bounded native trace attached when the unchanged marker wait fails."""
    try:
        wait_until(lambda: marker.exists(), timeout=10, what="descendant natural exit")
    except AssertionError as exc:
        failure = {
            "trace": trace[-64:],
            "job_object_id": id(transaction.job) if transaction.job is not None else None,
            "job_handle_object_id": (id(transaction.job._handle)
                                     if getattr(transaction.job, "_handle", None) is not None else None),
            "process_object_id": id(transaction.process) if transaction.process is not None else None,
            "process_pid": getattr(transaction.process, "pid", None),
            "process_host_start_time": transaction.host_start_time,
        }
        diagnostics = failure_details(marker.parent, transaction, native_query_trace=trace[-64:])
        raise AssertionError(
            f"{exc}; native_query_trace={json.dumps(failure, sort_keys=True)}; {diagnostics}"
        ) from exc


def _drain_completion(registry: ProcessRegistry, session) -> dict:
    waited = registry.wait_for_pending_completions(
        task_id=session.owner_task_id,
        timeout=10,
        poll_interval=0.1,
    )
    assert session._completion_event.is_set(), f"reader did not finish: {waited}"
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


def _managed_registry(tmp_path, monkeypatch):
    import tools.process_registry as process_registry_module

    coordinator = _coordinator()
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    return registry, coordinator


def _two_socket_child(tmp_path, listener, *, descendant_marker: str):
    host, port = listener.getsockname()
    descendant_path = tmp_path / "hermes-custody-f7-descendant.jsonl"
    parent_path = tmp_path / "hermes-custody-f7-parent.jsonl"
    descendant = (
        child_phase_prefix(descendant_path)
        + "import socket; _hermes_custody_phase('descendant_start'); "
        f"peer=socket.create_connection(({host!r}, {port})); "
        "_hermes_custody_phase('descendant_connected'); peer.sendall(b'D'); "
        "_hermes_custody_phase('descendant_release_sent'); peer.recv(1); "
        "_hermes_custody_phase('descendant_release_received'); peer.close(); "
        "_hermes_custody_phase('descendant_before_marker'); "
        f"from pathlib import Path; Path({descendant_marker!r}).write_text('exited'); "
        "_hermes_custody_phase('descendant_marker_written')"
    )
    return (
        child_phase_prefix(parent_path)
        + "import subprocess,sys,socket; _hermes_custody_phase('parent_before_popen'); "
        f"_hermes_custody_child_pid=subprocess.Popen([sys.executable, '-c', {descendant!r}], "
        "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).pid; "
        "_hermes_custody_phase('parent_after_popen', child_pid=_hermes_custody_child_pid); "
        f"peer=socket.create_connection(({host!r}, {port})); "
        "_hermes_custody_phase('parent_connected'); peer.sendall(b'P'); "
        "print('native-parent', flush=True); peer.recv(1); "
        "_hermes_custody_phase('parent_release_received'); peer.close()"
    )


@pytest.mark.platforms("windows")
def test_real_environment_yield_finishes_after_same_job_descendant_exit(tmp_path, monkeypatch):
    """The real reader/ACK path closes the original Job after a real descendant exits."""
    registry, coordinator = _managed_registry(tmp_path, monkeypatch)
    monkeypatch.setattr(base_environment, "consume_yield", lambda _thread_id: True)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(2)
    listener.settimeout(10)
    marker = tmp_path / "descendant-exited"
    child_code = _two_socket_child(tmp_path, listener, descendant_marker=str(marker))
    command = shell_python_command(child_code)
    env = LocalEnvironment(cwd=str(tmp_path), timeout=10)
    handler = yield_to_background_handler(
        command=command,
        env_type="local",
        cwd=str(tmp_path),
        effective_task_id="task-native-scope",
        task_id="task-native-scope",
        session_key="session-native-scope",
        session_id="session-native-scope",
        tool_call_id="tool-native-scope",
        turn_id="turn-native-scope",
        run_generation=7,
    )
    connections = {}
    session = None
    try:
        result = execute_with_diagnostics(env, command, cwd=str(tmp_path), yield_handler=handler)
        session = registry.get(result["yielded_session_id"])
        assert session is not None and session.custody is not None
        transaction = session.custody.native_scope
        assert transaction is not None
        native_query_trace = _observe_native_queries(transaction, monkeypatch)
        assert transaction.assigned is True and transaction.resumed is True
        assert transaction.scope_observation_process is transaction.process
        assert transaction.scope_observation_job is transaction.job
        expected_binding = ForegroundParentBinding(
            session_id="session-native-scope",
            session_key="session-native-scope",
            tool_call_id="tool-native-scope",
            turn_id="turn-native-scope",
            run_generation=7,
        )
        record = transaction._foreground_custody_record
        assert session.custody.foreground_binding == expected_binding
        assert record.binding == expected_binding
        # Registration intentionally consumes the foreground reservation map.  The
        # authenticated owner is now the real background session's transfer, not a
        # stale foreground-map entry or a reconstructed binding.
        assert not registry.foreground_binding_active(
            session_id=expected_binding.session_id,
            session_key=expected_binding.session_key,
            turn_id=expected_binding.turn_id,
            run_generation=expected_binding.run_generation,
            tool_call_id=expected_binding.tool_call_id,
        )
        transfer = session.custody.foreground_transfer
        assert transfer is not None
        assert transfer.transaction is transaction
        assert transfer.process is transaction.process
        assert transfer.reader_thread is session._reader_thread
        assert record.session is session
        assert session._foreground_reader_transfer is True
        assert registry.get(session.id) is session
        for _ in range(2):
            connection, _ = listener.accept()
            marker_byte = connection.recv(1)
            connections[marker_byte] = connection
        assert set(connections) == {b"D", b"P"}
        connections[b"P"].sendall(b"x")
        connections[b"P"].close()
        del connections[b"P"]
        wait_until(
            lambda: (
                session.custody_state == "unknown"
                and session.custody.reason == NATIVE_SCOPE_NONEMPTY
                and getattr(session.custody.foreground_transfer, "phase", None) == "scope_pending"
                and not session._completion_event.is_set()
            ),
            timeout=10,
            what="same-owner finalizer to retain typed nonempty custody",
        )
        assert transaction.process_handle_closed is False
        assert transaction.job_handle_closed is False
        assert session.custody_delivery_acked is False
        connections[b"D"].sendall(b"x")
        connections[b"D"].close()
        del connections[b"D"]
        _wait_for_descendant_marker(marker, native_query_trace, transaction)
        _drain_completion(registry, session)
        assert session.process is transaction.process
        assert session.custody_state == "terminal"
        assert any(
            item["operation"] == "empty" and item["result"] is True
            for item in native_query_trace
        )
        assert transaction.process_handle_closed is True
        assert transaction.job_handle_closed is True
        assert coordinator.status()["active_owner_classes"] == []
    finally:
        for connection in connections.values():
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
def test_opaque_query_then_nonempty_keeps_same_transaction_pending_until_drain(tmp_path, monkeypatch):
    """One opaque query retry may become typed nonempty pending, then drain the same Job."""
    coordinator = _coordinator()
    custody = ProcessCustody.reserve(coordinator, "windows-scope-owner", str(tmp_path))
    transaction = WindowsLaunchTransaction(custody)
    custody.native_scope = transaction
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(2)
    listener.settimeout(10)
    marker = tmp_path / "query-descendant-exited"
    child_code = _two_socket_child(tmp_path, listener, descendant_marker=str(marker))
    proc = transaction.launch(
        [sys.executable, "-c", child_code],
        **_popen_kwargs(tmp_path),
    )
    connections = {}
    original_empty = transaction.job.empty
    query_calls = []

    def fail_once_then_observe():
        query_calls.append(True)
        if len(query_calls) == 1:
            raise OSError("native scope query unavailable")
        return original_empty()

    transaction.job.empty = fail_once_then_observe
    try:
        for _ in range(2):
            connection, _ = listener.accept()
            connections[connection.recv(1)] = connection
        connections[b"P"].sendall(b"x")
        connections[b"P"].close()
        del connections[b"P"]
        assert proc.stdout is not None
        assert proc.stdout.read().strip() == "native-parent"
        proc.wait(timeout=10)
        assert transaction.observe_empty() is None, failure_details(tmp_path, transaction)
        assert transaction.scope_observation == "query_unavailable"
        custody.mark_unknown(NATIVE_SCOPE_OBSERVATION_FAILURE)
        assert custody.reconcile_unknown() is False
        assert custody.reason == NATIVE_SCOPE_NONEMPTY
        assert transaction.scope_observation_retries == 0
        assert len(query_calls) == 2
        connections[b"D"].sendall(b"x")
        connections[b"D"].close()
        del connections[b"D"]
        _wait_for_descendant_marker(marker, [], transaction)
        try:
            wait_until(
                lambda: transaction.observe_empty() is True,
                timeout=10,
                what="same Job to become empty after opaque retry",
            )
        except AssertionError as exc:
            raise AssertionError(
                f"{exc}; {failure_details(tmp_path, transaction)}"
            ) from exc
        assert custody.reconcile_unknown() is True
        assert transaction.close_after_observed_empty() is True
        assert transaction.process_handle_closed is True
        assert transaction.job_handle_closed is True
    finally:
        transaction.job.empty = original_empty
        for connection in connections.values():
            with contextlib.suppress(OSError):
                connection.close()
        with contextlib.suppress(OSError):
            listener.close()
        if proc.poll() is None:
            with contextlib.suppress(OSError):
                proc.kill()
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                proc.wait(timeout=10)
        if transaction.job is not None and not transaction.job_handle_closed:
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                transaction.job.close()


@pytest.mark.platforms("windows")
def test_repeated_opaque_scope_failures_stay_sticky_after_nonempty(tmp_path):
    """A legitimate nonempty refresh cannot replenish the opaque failure budget."""
    coordinator = _coordinator()
    custody = ProcessCustody.reserve(coordinator, "windows-scope-owner", str(tmp_path))
    transaction = WindowsLaunchTransaction(custody)
    custody.native_scope = transaction
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(2)
    listener.settimeout(10)
    child_code = _two_socket_child(
        tmp_path, listener, descendant_marker=str(tmp_path / "sticky-descendant-exited")
    )
    proc = transaction.launch(
        [sys.executable, "-c", child_code],
        **_popen_kwargs(tmp_path),
    )
    connections = {}
    original_empty = transaction.job.empty
    query_calls = []

    def fail_then_nonempty_then_fail():
        query_calls.append(True)
        if len(query_calls) in {1, 3}:
            raise OSError("native scope query unavailable")
        return original_empty()

    transaction.job.empty = fail_then_nonempty_then_fail
    try:
        for _ in range(2):
            connection, _ = listener.accept()
            connections[connection.recv(1)] = connection
        assert transaction.observe_empty() is None, failure_details(tmp_path, transaction)
        assert transaction.scope_observation_failures == 1
        custody.mark_unknown(NATIVE_SCOPE_OBSERVATION_FAILURE)
        assert custody.reconcile_unknown() is False
        assert custody.reason == NATIVE_SCOPE_NONEMPTY
        assert transaction.scope_observation_failures == 1
        assert custody.reconcile_unknown() is False
        assert custody.reason == "read_failed"
        assert transaction.scope_observation_failures == 2
        assert transaction.native_empty_observed is False
        assert transaction.process_handle_closed is False
        assert transaction.job_handle_closed is False
        assert len(query_calls) == 3
    finally:
        transaction.job.empty = original_empty
        for connection in connections.values():
            with contextlib.suppress(OSError):
                connection.close()
        with contextlib.suppress(OSError):
            listener.close()
        if proc.poll() is None:
            with contextlib.suppress(OSError):
                proc.kill()
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                proc.wait(timeout=10)
        if transaction.job is not None and not transaction.job_handle_closed:
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                transaction.job.close()


@pytest.mark.platforms("windows")
def test_foreign_scope_snapshot_stays_unknown_after_real_transaction(tmp_path, monkeypatch):
    """A replacement Job cannot replace the launch-bound native scope or release custody."""
    coordinator = _coordinator()
    custody = ProcessCustody.reserve(coordinator, "windows-scope-owner", str(tmp_path))
    transaction = WindowsLaunchTransaction(custody)
    custody.native_scope = transaction
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(2)
    listener.settimeout(10)
    marker = tmp_path / "foreign-descendant-exited"
    child_code = _two_socket_child(tmp_path, listener, descendant_marker=str(marker))
    proc = transaction.launch(
        [sys.executable, "-c", child_code],
        **_popen_kwargs(tmp_path),
    )
    original_job = transaction.job
    native_query_trace = _observe_native_queries(transaction, monkeypatch)
    connections = {}
    try:
        for _ in range(2):
            connection, _ = listener.accept()
            connections[connection.recv(1)] = connection
        assert set(connections) == {b"D", b"P"}
        assert transaction.observe_empty() is None, failure_details(tmp_path, transaction)
        assert transaction.scope_observation == "nonempty"
        mark_native_scope_unknown(custody)
        assert custody.state == "unknown"
        assert custody.reason == NATIVE_SCOPE_NONEMPTY

        transaction.job = SimpleNamespace(_handle=object())
        assert prepare_native_scope_retry(custody) is False
        assert transaction.scope_observation == "foreign"
        assert custody.reconcile_unknown() is False
        assert custody.state == "unknown"
        assert custody.reason == "foreign_custody"
        assert transaction.process_handle_closed is False
        assert transaction.job_handle_closed is False

        transaction.job = original_job
        for connection in connections.values():
            with contextlib.suppress(OSError):
                connection.sendall(b"x")
            with contextlib.suppress(OSError):
                connection.close()
        connections.clear()
        _wait_for_descendant_marker(marker, native_query_trace, transaction)
        proc.wait(timeout=10)
        try:
            wait_until(
                lambda: transaction.observe_empty() is True,
                timeout=10,
                what="the original retained Job to report empty",
            )
        except AssertionError as exc:
            raise AssertionError(
                f"{exc}; {failure_details(tmp_path, transaction)}"
            ) from exc
        assert any(
            item["operation"] == "empty" and item["result"] is True
            for item in native_query_trace
        )
        assert transaction.close_after_observed_empty() is True
        assert transaction.process_handle_closed is True
        assert transaction.job_handle_closed is True
    finally:
        transaction.job = original_job
        for connection in connections.values():
            with contextlib.suppress(OSError):
                connection.close()
        with contextlib.suppress(OSError):
            listener.close()
        if proc.poll() is None:
            with contextlib.suppress(OSError):
                proc.kill()
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                proc.wait(timeout=10)
        with contextlib.suppress(Exception):
            transaction.observe_empty()
        with contextlib.suppress(Exception):
            transaction.close_after_observed_empty()
