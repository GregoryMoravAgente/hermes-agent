"""The production watcher detach refreshes the canonical I10 source binding."""

import asyncio
import json
from types import SimpleNamespace

import pytest


def test_startup_watcher_detach_rebinds_i10_source_for_each_profile(started_mux, monkeypatch):
    """A real startup detach must leave every profile readable without weakening identity checks."""
    from gateway.run_notifications import i10_source_snapshot
    from tools import process_registry as process_registry_module

    runner, _homes, _loop, call = started_mux
    authorities = tuple(runner.session_authorities)
    assert len(authorities) >= 2

    async def snapshots():
        return tuple(i10_source_snapshot(authority) for authority in authorities)

    readbacks = call(snapshots())
    assert len(readbacks) == len(authorities)
    assert all(readback["registry"]["completion_queue_depth"] == 0 for readback in readbacks)

    registry = process_registry_module.process_registry
    with monkeypatch.context() as replaced:
        replaced.setattr(registry, "pending_watchers", list(registry.pending_watchers))
        with pytest.raises(RuntimeError, match="identity"):
            call(snapshots())


@pytest.mark.asyncio
async def test_startup_watcher_batch_stays_indexed_during_mid_yield_transfer(
    tmp_path, monkeypatch,
):
    """An accepted native watcher left in the detached batch remains visible at a yield."""
    from gateway.run_notifications import i10_source_snapshot
    from gateway.session_context import clear_session_vars, set_session_vars
    from tests.gateway.conftest import (
        _runtime_owned_start_public, _runtime_owned_stop_public, runtime_owned_api,
    )
    from tests.tools.foreground_custody_diagnostics import shell_python_command
    from tools import process_registry as process_registry_module
    from tools.terminal_tool_background import spawn_background_process

    runtime_context = runtime_owned_api(tmp_path, monkeypatch)
    adapter = await runtime_context.__aenter__()
    runner = adapter.gateway_runner
    registry = process_registry_module.process_registry
    handles = {
        "control_server": None,
        "native_control_server": None,
        "native_control_threads": (),
        "socket_control_server": None,
    }
    session_ids = []
    owner_task_id = "i10-mid-yield-watcher-batch"
    observed = asyncio.get_running_loop().create_future()
    process_watcher_count = 0
    original_spawn = runner._spawn_supervised
    authority = runner.session_authority

    async def observe_batch():
        try:
            registry.pending_watchers.append(appended_watcher)
            observed.set_result(i10_source_snapshot(authority))
        except BaseException as exc:
            observed.set_exception(exc)

    def observe_after_one_hundred(coro_factory, name, **kwargs):
        nonlocal process_watcher_count
        result = original_spawn(coro_factory, name, **kwargs)
        if name.startswith("process_watcher:"):
            process_watcher_count += 1
            if process_watcher_count == 100:
                asyncio.create_task(observe_batch())
        return result

    monkeypatch.setattr(runner, "_spawn_supervised", observe_after_one_hundred)
    tokens = set_session_vars(
        platform="telegram", chat_id="i10-mid-yield", session_key="agent:main:telegram:dm:i10-mid-yield",
        async_delivery=True,
    )
    command = shell_python_command("import time; time.sleep(1)", temp_dir=tmp_path)
    try:
        for index in range(102):
            session_key = f"agent:main:telegram:dm:i10-mid-yield-{index}"
            result = json.loads(spawn_background_process(
                command=command,
                env=SimpleNamespace(env={}, host_cwd=str(tmp_path)),
                env_type="local", effective_task_id=owner_task_id, task_id=owner_task_id,
                session_key=session_key, workdir=str(tmp_path), cwd=str(tmp_path),
                effective_pty=False, notify_on_complete=True, watch_patterns=None,
                approval_note=None, completion_output_chars=0, pty_disabled_reason=None,
                heartbeat_seconds=0, persist_on_release=True, mounted_host=str(tmp_path),
                auto_backgrounded=False,
            ))
            assert result["error"] is None, result
            session_ids.append(result["session_id"])
        # This is an actual descriptor from the production admission path. Keep it out of the
        # initial detached batch so the observer can append the exact object during the yield;
        # no synthetic watcher shape or native handle is introduced by the test.
        appended_watcher = registry.pending_watchers.pop()
        assert len(registry.pending_watchers) == 101

        await _runtime_owned_start_public(runner, tmp_path, handles)
        readback = await asyncio.wait_for(observed, timeout=5)
        assert process_watcher_count >= 100
        assert session_ids[-2] in readback["registry"]["pending_watchers"]
        assert session_ids[-1] in readback["registry"]["pending_watchers"]
        assert any(
            pending is appended_watcher for pending in registry.pending_watchers
        )
    finally:
        clear_session_vars(tokens)
        for session_id in session_ids:
            registry.kill_process(session_id, source="test.i10.mid_yield", consume_output=False)
        await asyncio.to_thread(
            registry.wait_for_pending_completions,
            task_id=owner_task_id, timeout=30, poll_interval=0.05,
        )
        for event, _text in registry.drain_notifications():
            token = event.get("delivery_token")
            if isinstance(token, str) and token:
                registry.acknowledge_completion_delivery(token)
        await _runtime_owned_stop_public(runner, handles)
        await runtime_context.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_startup_watcher_task_creation_failure_retains_exact_descriptor(
    tmp_path, monkeypatch,
):
    """A supervisor task-creation refusal keeps the admitted native watcher canonical."""
    from gateway.run_notifications import i10_source_snapshot
    from gateway.session_context import clear_session_vars, set_session_vars
    from tests.gateway.conftest import (
        _runtime_owned_start_public, _runtime_owned_stop_public, runtime_owned_api,
    )
    from tests.tools.foreground_custody_diagnostics import shell_python_command
    from tools import process_registry as process_registry_module
    from tools.terminal_tool_background import spawn_background_process

    runtime_context = runtime_owned_api(tmp_path, monkeypatch)
    adapter = await runtime_context.__aenter__()
    runner = adapter.gateway_runner
    registry = process_registry_module.process_registry
    handles = {
        "control_server": None,
        "native_control_server": None,
        "native_control_threads": (),
        "socket_control_server": None,
    }
    session_ids = []
    owner_task_id = "i10-task-creation-failure"
    authority = runner.session_authority
    tokens = set_session_vars(
        platform="telegram", chat_id="i10-task-creation-failure",
        session_key="agent:main:telegram:dm:i10-task-creation-failure",
        async_delivery=True,
    )
    command = shell_python_command("import time; time.sleep(1)", temp_dir=tmp_path)
    try:
        result = json.loads(spawn_background_process(
            command=command,
            env=SimpleNamespace(env={}, host_cwd=str(tmp_path)),
            env_type="local", effective_task_id=owner_task_id, task_id=owner_task_id,
            session_key="agent:main:telegram:dm:i10-task-creation-failure-0",
            workdir=str(tmp_path), cwd=str(tmp_path), effective_pty=False,
            notify_on_complete=True, watch_patterns=None, approval_note=None,
            completion_output_chars=0, pty_disabled_reason=None, heartbeat_seconds=0,
            persist_on_release=True, mounted_host=str(tmp_path), auto_backgrounded=False,
        ))
        assert result["error"] is None, result
        session_ids.append(result["session_id"])
        admitted_watcher = registry.pending_watchers[0]
        original_spawn = runner._spawn_supervised
        task_creation_attempts = 0

        def reject_task_creation(coro):
            nonlocal task_creation_attempts
            task_creation_attempts += 1
            # The production coroutine was constructed, but no supervisor task was admitted.
            coro.close()
            raise RuntimeError("test supervisor task creation refusal")

        def refuse_process_watcher(coro_factory, name, **kwargs):
            if not name.startswith("process_watcher:"):
                return original_spawn(coro_factory, name, **kwargs)
            with monkeypatch.context() as blocked:
                blocked.setattr(asyncio, "create_task", reject_task_creation)
                return original_spawn(coro_factory, name, **kwargs)

        monkeypatch.setattr(runner, "_spawn_supervised", refuse_process_watcher)
        await _runtime_owned_start_public(runner, tmp_path, handles)

        assert task_creation_attempts == 1
        assert any(
            pending is admitted_watcher for pending in registry.pending_watchers
        )
        readback = i10_source_snapshot(authority)
        assert admitted_watcher["session_id"] in readback["registry"]["pending_watchers"]
    finally:
        clear_session_vars(tokens)
        for session_id in session_ids:
            registry.kill_process(session_id, source="test.i10.task_creation_failure", consume_output=False)
        await asyncio.to_thread(
            registry.wait_for_pending_completions,
            task_id=owner_task_id, timeout=30, poll_interval=0.05,
        )
        for event, _text in registry.drain_notifications():
            token = event.get("delivery_token")
            if isinstance(token, str) and token:
                registry.acknowledge_completion_delivery(token)
        await _runtime_owned_stop_public(runner, handles)
        await runtime_context.__aexit__(None, None, None)
