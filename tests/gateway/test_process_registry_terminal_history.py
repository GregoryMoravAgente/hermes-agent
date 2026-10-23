"""Terminal process history is display data, not live I15 custody.

The Windows case in this file uses the real gateway bootstrap and native process registry.  The
portable case only proves the conservative refusal path: a finished object without a typed
terminal custody receipt must still block a coordinator replacement.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import subprocess
import sys
from types import SimpleNamespace

import pytest


def _registry_session_snapshot(registry):
    """Read one bounded, per-session custody snapshot without changing registry state."""
    snapshots = []
    with registry._lock:
        stores = (("running", registry._running), ("finished", registry._finished))
        for location, store in stores:
            for session in store.values():
                custody = getattr(session, "custody", None)
                native = getattr(custody, "native_scope", None)
                snapshots.append({
                    "location": location,
                    "session_id": getattr(session, "id", None),
                    "profile_id": getattr(session, "profile_id", None),
                    "exited": getattr(session, "exited", None),
                    "reader_phase": getattr(session, "reader_phase", None),
                    "completion_event_set": bool(
                        getattr(getattr(session, "_completion_event", None), "is_set", lambda: False)()
                    ),
                    "scope_stop_pending": getattr(session, "_scope_stop_pending", None),
                    "completion_reason": getattr(session, "completion_reason", None),
                    "exit_code": getattr(session, "exit_code", None),
                    "output_chars": len(getattr(session, "output_buffer", "") or ""),
                    "output_digest": hashlib.sha256(
                        (getattr(session, "output_buffer", "") or "").encode()
                    ).hexdigest(),
                    "custody": {
                        name: getattr(custody, name, None)
                        for name in (
                            "state", "process_bound", "reader_eof", "scope_settled",
                            "cross_process_proven", "durable", "streams_closed", "handle_closed",
                            "watcher_registered", "delivery_acked",
                        )
                    } if custody is not None else None,
                    "native": {
                        name: getattr(native, name, None)
                        for name in (
                            "state", "native_empty_observed", "scope_observation",
                            "process_handle_closed", "job_handle_closed", "close_retryable",
                        )
                    } if native is not None else None,
                })
    return tuple(sorted(snapshots, key=lambda item: (item["location"], item["session_id"] or "")))


@pytest.fixture
def terminal_history_snapshot():
    """Provide a read-only product-session snapshot for the rebind boundary."""
    return _registry_session_snapshot


def test_untyped_finished_history_blocks_rebind_and_preserves_output(terminal_history_snapshot):
    """No typed/native terminal receipt means the old session remains reachable and blocking."""
    from tools.process_registry import ProcessRegistry, ProcessSession
    from tools.process_registry_zero_factory import ProcessRegistryZeroFactory

    registry = ProcessRegistry()
    session = ProcessSession(
        id="proc_terminal_history_unknown",
        command="synthetic diagnostic command",
        output_buffer="retained output\n",
    )
    session.exited = True
    session.reader_phase = "eof"
    session._completion_event.set()
    with registry._lock:
        registry._finished[session.id] = session
    before = terminal_history_snapshot(registry)
    first_coordinator = object()
    second_coordinator = object()
    registry._i15_zero_factory = ProcessRegistryZeroFactory(registry, first_coordinator)

    with pytest.raises(RuntimeError, match="live custody"):
        registry.bind_process_registry_zero_factory(second_coordinator, profiles=())

    after = terminal_history_snapshot(registry)
    assert before == after
    assert registry._finished[session.id] is session
    assert session.output_buffer == "retained output\n"


@pytest.mark.parametrize("pending_kind", ("queue", "token", "watcher"))
def test_pending_delivery_blocks_rebind_without_finished_history(pending_kind):
    """A completion still in flight blocks rebind even when the history index is empty."""
    from tools.process_registry import ProcessRegistry
    from tools.process_registry_zero_factory import ProcessRegistryZeroFactory

    registry = ProcessRegistry()
    registry._i15_zero_factory = ProcessRegistryZeroFactory(registry, object())
    if pending_kind == "queue":
        registry.completion_queue.put({"type": "completion", "session_id": "proc-pending"})
    elif pending_kind == "token":
        registry._completion_tokens["pending-token"] = (
            "proc-pending", "owner", "profile", "instance", 1, 1,
        )
    else:
        registry.pending_watchers.append({"session_id": "proc-pending"})

    with pytest.raises(RuntimeError, match="live custody"):
        registry.bind_process_registry_zero_factory(object(), profiles=())


@pytest.mark.platforms("windows")
@pytest.mark.asyncio
async def test_real_terminal_history_allows_new_runtime_coordinator(
    tmp_path, monkeypatch, terminal_history_snapshot,
):
    """A real native child can remain readable while a later runtime binds a new coordinator."""
    from gateway.session_context import clear_session_vars, set_session_vars
    from tests.gateway.conftest import runtime_owned_api
    from tests.tools.foreground_custody_diagnostics import shell_python_command
    from tools import process_registry as process_registry_module
    from tools.process_registry_terminal_history import finished_session_is_terminal_history
    from tools.terminal_tool_background import spawn_background_process

    first_root = tmp_path / "first-runtime"
    second_root = tmp_path / "second-runtime"
    owner_task_id = "terminal-history-rebind"
    session_key = "agent:main:windows:terminal-history-rebind"
    registry = process_registry_module.process_registry
    session_id = None
    before_second = None
    tokens = set_session_vars(
        platform="telegram", chat_id="terminal-history-rebind", session_key=session_key,
        async_delivery=True,
    )
    try:
        # Completion watchers must be admitted by the real public scheduler before the profile
        # is retired.  The test still exercises the retained native history/rebind boundary;
        # it no longer leaves a valid watcher stranded in the pre-start queue.
        async with runtime_owned_api(first_root, monkeypatch, start_public_runtime=True):
            result = json.loads(spawn_background_process(
                command=shell_python_command(
                    "import sys; print('terminal-history-proof', flush=True)", temp_dir=first_root,
                ),
                env=SimpleNamespace(env={}, host_cwd=str(first_root)),
                env_type="local", effective_task_id=owner_task_id, task_id=owner_task_id,
                session_key=session_key, workdir=str(first_root), cwd=str(first_root),
                effective_pty=False, notify_on_complete=True, watch_patterns=None,
                approval_note=None, completion_output_chars=0, pty_disabled_reason=None,
                heartbeat_seconds=0, persist_on_release=True, mounted_host=str(first_root),
                auto_backgrounded=False,
            ))
            assert result["error"] is None, result
            session_id = result["session_id"]
            await asyncio.to_thread(
                registry.wait_for_pending_completions,
                task_id=owner_task_id, timeout=30, poll_interval=0.05,
            )
            session = registry.get(session_id)
            assert session is not None and session._completion_event.is_set()
            delivered = registry.drain_notifications(
                session_key=session_key, skip_poll_observed=False,
            )
            event = next(
                (item for item, _text in delivered if item.get("session_id") == session_id),
                None,
            )
            assert event is not None, delivered
            token = event.get("delivery_token")
            assert isinstance(token, str) and token
            registry.acknowledge_completion_delivery(token)
            assert session.custody is not None
            assert session.custody.state == "terminal"
            assert session.custody.handle is None
            assert session.custody.delivery_acked is True
            native = session.custody.native_scope
            assert native is not None
            assert native.state == "closed"
            assert native.native_empty_observed is True
            assert native.process_handle_closed is True
            assert native.job_handle_closed is True
            assert "terminal-history-proof" in session.output_buffer
            assert finished_session_is_terminal_history(
                session,
                completion_tokens=registry._completion_tokens,
                pending_watchers=registry.pending_watchers,
            ) is True
            # A current-generation terminal child remains part of the current I15 inventory;
            # only a later coordinator may treat this closed history as historical.
            assert finished_session_is_terminal_history(
                session,
                completion_tokens=registry._completion_tokens,
                pending_watchers=registry.pending_watchers,
                live_coordinator=session.custody.coordinator,
            ) is False
            original_native = session.custody.native_scope
            replacement = SimpleNamespace(
                custody=session.custody,
                process=session.process,
                pid=session.pid,
                host_start_time=session.host_start_time,
                state="closed",
                native_empty_observed=True,
                scope_observation="empty",
                process_handle_closed=True,
                job_handle_closed=True,
                close_retryable=False,
            )
            session.custody.native_scope = replacement
            try:
                assert finished_session_is_terminal_history(
                    session,
                    completion_tokens=registry._completion_tokens,
                    pending_watchers=registry.pending_watchers,
                ) is False
                with pytest.raises(RuntimeError, match="live custody"):
                    registry.bind_process_registry_zero_factory(object(), profiles=())
            finally:
                session.custody.native_scope = original_native

        before_second = terminal_history_snapshot(registry)
        assert any(
            item["session_id"] == session_id
            and item["location"] == "finished"
            and item["custody"]["state"] == "terminal"
            and item["native"]["state"] == "closed"
            for item in before_second
        )

        async with runtime_owned_api(second_root, monkeypatch):
            retained = registry.get(session_id)
            assert retained is not None
            assert retained.output_buffer.endswith("terminal-history-proof\n")
            assert terminal_history_snapshot(registry) == before_second
    finally:
        clear_session_vars(tokens)


@pytest.mark.platforms("windows")
def test_real_failed_native_close_keeps_history_rebind_blocked(tmp_path):
    """A real Job close failure stays UNKNOWN and blocks replacement with the same object."""
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_cli.backend_retirement import RetirementFence
    from tools.process_registry import ProcessRegistry, ProcessSession
    from tools.process_registry_custody import ProcessCustody
    from tools.process_registry_windows import WindowsLaunchTransaction
    from tools.process_registry_zero_factory import ProcessRegistryZeroFactory
    from tests.tools.process_registry_native_profile import _popen_kwargs

    coordinator = RuntimeCustodyCoordinator(
        process_instance="terminal-history-close-failure",
        authority_epoch=9,
        fence=RetirementFence(),
    )
    custody = ProcessCustody.reserve(coordinator, "terminal-history-close", str(tmp_path))
    transaction = WindowsLaunchTransaction(custody)
    custody.native_scope = transaction
    process = transaction.launch(
        [sys.executable, "-c", "print('close-failure-history', flush=True)"],
        **_popen_kwargs(tmp_path),
    )
    original_close = None
    try:
        assert process.stdout is not None
        assert process.stdout.read().strip() == "close-failure-history"
        process.wait(timeout=10)
        assert transaction.observe_empty() is True
        original_close = transaction.job.close

        def fail_close():
            raise OSError("synthetic native close refusal")

        transaction.job.close = fail_close
        try:
            assert transaction.close_after_observed_empty() is False
        finally:
            transaction.job.close = original_close
        assert custody.state == "unknown"
        assert transaction.state == "close_failed"
        assert transaction.close_retryable is True
        session = ProcessSession(
            id="proc_terminal_history_close_failure",
            command="real native close failure",
            process=process,
            pid=transaction.pid,
            host_start_time=transaction.host_start_time,
            exited=True,
            output_buffer="close-failure-history\n",
            custody=custody,
        )
        session.reader_phase = "eof"
        session._completion_event.set()
        registry = ProcessRegistry()
        with registry._lock:
            registry._finished[session.id] = session
        registry._i15_zero_factory = ProcessRegistryZeroFactory(registry, object())
        with pytest.raises(RuntimeError, match="live custody"):
            registry.bind_process_registry_zero_factory(object(), profiles=())
        assert registry._finished[session.id] is session
        assert session.custody.native_scope is transaction
        assert transaction.job_handle_closed is False
    finally:
        if original_close is not None and transaction.job is not None:
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                transaction.job.close = original_close
        if transaction.close_retryable:
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                assert custody.reconcile_unknown() is True
                assert transaction.close_after_observed_empty() is True
        if process.poll() is None:
            with contextlib.suppress(OSError):
                process.kill()
            with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                process.wait(timeout=10)

