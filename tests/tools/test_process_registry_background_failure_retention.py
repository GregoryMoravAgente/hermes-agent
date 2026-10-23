"""Portable public-producer controls for background failure retention.

These tests exercise the public terminal-to-registry launch seam and the portable
Popen/reader/setup refusal boundaries.  They deliberately assert retention of the
same session/object and the original exception; completion delivery remains covered
by the genuine Windows producer tests in the companion module.
"""
from __future__ import annotations

import json
import os
import threading
from types import SimpleNamespace

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence
from tools.process_registry import ProcessRegistry
from tools import terminal_tool_background


def _child(code: str) -> str:
    import sys

    return f"{sys.executable} -c {code!r}"


def _public_args(*, env_type: str = "local") -> dict:
    return {
        "env": SimpleNamespace(env={}),
        "env_type": env_type,
        "command": "echo public-background",
        "cwd": ".",
        "effective_task_id": "portable-task",
        "task_id": "portable-task",
        "session_key": "portable-session",
        "effective_pty": False,
        "notify_on_complete": True,
    }


def test_public_spawn_forwards_notification_before_local_launch():
    calls = []
    expected = SimpleNamespace(id="proc_public", pid=123, notify_on_complete=True)

    class CapturingRegistry:
        def spawn_local(self, **kwargs):
            calls.append(("local", kwargs))
            return expected

    assert terminal_tool_background._spawn(CapturingRegistry(), **_public_args()) is expected
    assert calls[0][1]["notify_on_complete"] is True


def test_public_spawn_forwards_notification_before_sandbox_launch():
    calls = []
    expected = SimpleNamespace(id="proc_sandbox", pid=456, notify_on_complete=True)

    class CapturingRegistry:
        def spawn_via_env(self, **kwargs):
            calls.append(("sandbox", kwargs))
            return expected

    assert terminal_tool_background._spawn(
        CapturingRegistry(), **_public_args(env_type="docker")
    ) is expected
    assert calls[0][1]["notify_on_complete"] is True


def test_public_bind_failure_retains_same_popen_and_original_error(tmp_path, monkeypatch):
    registry = ProcessRegistry()

    def refuse_bind(_session):
        raise RuntimeError("portable bind refused")

    monkeypatch.setattr(registry, "_custody_bind_process", refuse_bind)
    with pytest.raises(RuntimeError, match="portable bind refused"):
        registry.spawn_local(
            _child("import time; time.sleep(30)"), cwd=str(tmp_path),
            task_id="portable-task", owner_task_id="portable-owner",
            session_key="portable-session", notify_on_complete=True,
        )

    retained = next(iter(registry._running.values()))
    assert retained.process is not None
    assert retained.pid == retained.process.pid
    assert retained.notify_on_complete is True
    assert registry.get(retained.id) is retained
    registry.kill_process(retained.id, consume_output=False)


def test_public_reader_start_ambiguity_keeps_one_reader_and_same_session(tmp_path, monkeypatch):
    registry = ProcessRegistry()
    import threading

    original_start = threading.Thread.start
    calls = {"count": 0}

    def refuse_first_start(thread):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("portable reader start refused")
        return original_start(thread)

    monkeypatch.setattr(threading.Thread, "start", refuse_first_start)
    with pytest.raises(RuntimeError, match="portable reader start refused"):
        registry.spawn_local(
            _child("print('portable-reader', flush=True)"), cwd=str(tmp_path),
            task_id="portable-task", owner_task_id="portable-owner",
            session_key="portable-session", notify_on_complete=True,
        )

    retained = next(iter(registry._running.values()))
    reader = retained._reader_thread
    assert reader is not None
    assert retained.reader_phase == "ambiguous"
    assert registry._ensure_reader_registered(retained) is False
    assert retained._reader_thread is reader
    assert registry.get(retained.id) is retained
    registry.kill_process(retained.id, consume_output=False)


def test_public_setup_release_refusal_preserves_original_and_blocks_fallback(tmp_path, monkeypatch):
    registry = ProcessRegistry()

    def refuse_scope(*_args, **_kwargs):
        raise OSError("portable setup refused")

    monkeypatch.setattr(registry, "_scope_argv", refuse_scope)
    monkeypatch.setattr(registry, "_custody_release_unaccepted", lambda _session: False)
    with pytest.raises(OSError, match="portable setup refused"):
        registry.spawn_local(
            _child("print('must-not-run', flush=True)"), cwd=str(tmp_path),
            task_id="portable-task", owner_task_id="portable-owner",
            session_key="portable-session", notify_on_complete=True,
        )

    retained = next(iter(registry._running.values()))
    assert retained.process is None
    assert retained.pid is None
    assert registry.get(retained.id) is retained


def test_public_pty_release_refusal_preserves_setup_error_and_blocks_pipe_fallback(tmp_path, monkeypatch):
    registry = ProcessRegistry()
    import tools.process_registry as process_registry_module

    monkeypatch.setattr(process_registry_module, "worker_output_log", lambda _session_id: None)

    def refuse_pty(*_args, **_kwargs):
        raise OSError("portable PTY setup refused")

    monkeypatch.setattr(registry, "_spawn_local_pty", refuse_pty)
    monkeypatch.setattr(registry, "_custody_release_unaccepted", lambda _session: False)
    with pytest.raises(OSError, match="portable PTY setup refused"):
        registry.spawn_local(
            _child("print('must-not-fallback', flush=True)"), cwd=str(tmp_path), use_pty=True,
            task_id="portable-task", owner_task_id="portable-owner",
            session_key="portable-session", notify_on_complete=True,
        )

    retained = next(iter(registry._running.values()))
    assert retained.process is None
    assert retained.pid is None
    assert registry.get(retained.id) is retained


def test_public_terminal_fast_child_emits_one_token_before_ack_close(tmp_path, monkeypatch):
    """The public terminal helper arms delivery before the real local launch."""
    import tools.process_registry as process_registry_module

    registry = ProcessRegistry()
    registry.bind_runtime_custody(RuntimeCustodyCoordinator(
        process_instance="portable-public-producer-test", authority_epoch=13,
        fence=RetirementFence(),
    ))
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(terminal_tool_background, "_async_delivery_state", lambda *_args: True)
    result = json.loads(terminal_tool_background.spawn_background_process(
        command=_child("print('public-fast', flush=True)"),
        env=SimpleNamespace(env={}, host_cwd=str(tmp_path)), env_type="local",
        effective_task_id="portable-public-task", task_id="portable-public-task",
        session_key="portable-public-session", workdir=str(tmp_path), cwd=str(tmp_path),
        effective_pty=False, notify_on_complete=True, watch_patterns=None,
        approval_note=None, completion_output_chars=0, pty_disabled_reason=None,
        heartbeat_seconds=0, persist_on_release=True, mounted_host=str(tmp_path),
        auto_backgrounded=False,
    ))
    assert result["notify_on_complete"] is True
    session = registry.get(result["session_id"])
    assert session is not None
    settled = registry.wait_for_pending_completions(
        task_id=session.owner_task_id, timeout=10, poll_interval=0.05,
    )
    assert session.id in settled["completed"]
    assert session.custody_durable is True
    events = registry.drain_notifications(session_key=session.session_key)
    matching = [event for event, _text in events if event.get("session_id") == session.id]
    assert len(matching) == 1
    token = matching[0].get("delivery_token")
    assert isinstance(token, str) and token
    registry.acknowledge_completion_delivery(token)
    assert session.custody_delivery_acked is True


def test_public_sandbox_bind_failure_retries_same_session_and_handle(tmp_path, monkeypatch):
    """A marker-visible sandbox child stays unknown until the same lease rebinds."""
    import tools.process_registry as process_registry_module

    class MarkerEnvironment:
        def __init__(self):
            self.env = {}
            self.host_cwd = str(tmp_path)
            self.pid = os.getpid()
            self.killed = threading.Event()
            self.commands = []

        def get_temp_dir(self):
            return str(tmp_path)

        def execute(self, command, **_kwargs):
            self.commands.append(command)
            if "kill -0" in command:
                return {"output": "1\n" if self.killed.is_set() else "0\n", "returncode": 0}
            if "cat " in command and ".exit" in command:
                return {"output": "0\n" if self.killed.is_set() else "", "returncode": 0}
            if "cat " in command and ".pid" in command:
                return {"output": f"{self.pid}\n", "returncode": 0}
            if command.lstrip().startswith("kill "):
                self.killed.set()
                return {"output": "", "returncode": 0}
            if "nohup bash" in command or "echo $!" in command:
                return {"output": f"{self.pid}\n", "returncode": 0}
            return {"output": "0 0\n", "returncode": 0}

    registry = ProcessRegistry()
    registry.bind_runtime_custody(RuntimeCustodyCoordinator(
        process_instance="portable-sandbox-producer-test", authority_epoch=17,
        fence=RetirementFence(),
    ))
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(terminal_tool_background, "_async_delivery_state", lambda *_args: True)
    env = MarkerEnvironment()
    host_birth_queries = []

    def forbid_sandbox_host_birth(pid):
        host_birth_queries.append(pid)
        raise AssertionError("sandbox marker must not query host PID birth")

    monkeypatch.setattr(registry, "_safe_host_start_time", forbid_sandbox_host_birth)
    original_bind = registry._custody_bind_process
    bind_calls = 0
    rebound = threading.Event()

    def fail_once(session):
        nonlocal bind_calls
        bind_calls += 1
        if bind_calls == 1:
            raise RuntimeError("sandbox bind refused")
        rebound.set()
        return original_bind(session)

    monkeypatch.setattr(registry, "_custody_bind_process", fail_once)
    result = json.loads(terminal_tool_background.spawn_background_process(
        command="echo sandbox-public", env=env, env_type="docker",
        effective_task_id="portable-sandbox-task", task_id="portable-sandbox-task",
        session_key="portable-sandbox-session", workdir=None, cwd="/workspace",
        effective_pty=False, notify_on_complete=True, watch_patterns=None,
        approval_note=None, completion_output_chars=0, pty_disabled_reason=None,
        heartbeat_seconds=0, persist_on_release=True, mounted_host=None,
        auto_backgrounded=False,
    ))
    session = registry.get(result["session_id"])
    assert session is not None
    original_handle = session.custody.handle
    assert original_handle is not None
    original_owner = session.custody.owner_id
    original_process_instance = session.custody.process_instance
    original_epoch = session.custody.authority_epoch
    assert rebound.wait(timeout=8) is True
    assert bind_calls >= 2
    assert registry.get(session.id) is session
    assert session.pid == env.pid
    assert host_birth_queries == []
    assert session.host_start_time is None
    assert session.pid_scope == "sandbox"
    assert session.custody.host_start_time is None
    assert session.custody_process_bound is False
    assert session.custody_state == "unknown"
    assert session.custody.handle is original_handle
    assert session.custody.owner_id == original_owner
    assert session.custody.process_instance == original_process_instance
    assert session.custody.authority_epoch == original_epoch
    assert list(registry._running) == [session.id]
    assert getattr(session, "_sandbox_bind_retry_pending", False) is True
    registry.kill_process(session.id, source="test.sandbox.cleanup", consume_output=False)
    assert session._completion_event.wait(timeout=8) is True
    events = registry.drain_notifications(session_key=session.session_key)
    matching = [event for event, _text in events if event.get("session_id") == session.id]
    assert len(matching) == 1
    token = matching[0].get("delivery_token")
    assert isinstance(token, str) and token
    registry.acknowledge_completion_delivery(token)
    assert session.custody_delivery_acked is True
    assert session.custody_state == "unknown"


def test_public_pty_baseexception_retains_original_and_never_pipe_fallback(tmp_path, monkeypatch):
    """A PTY BaseException before Popen is not an ImportError no-effect fallback."""
    registry = ProcessRegistry()
    import tools.process_registry as process_registry_module

    monkeypatch.setattr(process_registry_module, "worker_output_log", lambda _session_id: None)

    def interrupt_pty(*_args, **_kwargs):
        raise KeyboardInterrupt("pty setup interrupted")

    monkeypatch.setattr(registry, "_spawn_local_pty", interrupt_pty)
    with pytest.raises(KeyboardInterrupt, match="pty setup interrupted"):
        registry.spawn_local(
            _child("print('must-not-pipe', flush=True)"), cwd=str(tmp_path), use_pty=True,
            task_id="portable-pty-interrupt-task", owner_task_id="portable-pty-interrupt-owner",
            session_key="portable-pty-interrupt-session", notify_on_complete=True,
        )

    retained = next(iter(registry._running.values()))
    assert retained.process is None
    assert retained._pty is None
    assert retained.pid is None
    assert registry.get(retained.id) is retained


@pytest.mark.platforms("posix")
def test_public_pty_bind_failure_retains_exact_pty_and_original_error(tmp_path, monkeypatch):
    """A real PTY bind failure keeps the PTY object and refuses a pipe relaunch."""
    import tools.process_registry as process_registry_module

    registry = ProcessRegistry()
    registry.bind_runtime_custody(RuntimeCustodyCoordinator(
        process_instance="portable-pty-retention", authority_epoch=31,
        fence=RetirementFence(),
    ))
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(process_registry_module, "worker_output_log", lambda _session_id: None)

    def refuse_bind(_session):
        raise RuntimeError("portable PTY bind refused")

    monkeypatch.setattr(registry, "_custody_bind_process", refuse_bind)
    with pytest.raises(RuntimeError, match="portable PTY bind refused"):
        registry.spawn_local(
            _child("print('pty-bound', flush=True)"), cwd=str(tmp_path), use_pty=True,
            task_id="portable-pty-task", owner_task_id="portable-pty-owner",
            session_key="portable-pty-session", notify_on_complete=True,
        )
    retained = next(iter(registry._running.values()))
    assert retained._pty is not None
    assert retained.pid == retained._pty.pid
    assert retained.custody_state == "unknown"
    assert registry.get(retained.id) is retained
    registry.kill_process(retained.id, source="test.pty.cleanup", consume_output=False)


@pytest.mark.platforms("posix")
def test_public_pty_systemd_stop_failure_keeps_setup_error_and_no_pipe_fallback(tmp_path, monkeypatch):
    """A systemd scope stop refusal retains setup state instead of starting a pipe child."""
    import tools.process_registry as process_registry_module

    registry = ProcessRegistry()
    registry.bind_runtime_custody(RuntimeCustodyCoordinator(
        process_instance="portable-pty-retention", authority_epoch=31,
        fence=RetirementFence(),
    ))
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(process_registry_module, "worker_output_log", lambda _session_id: None)

    def refuse_scope(session, *_args, **_kwargs):
        session.systemd_unit = "hermes-test.scope"
        raise OSError("systemd PTY setup refused")

    monkeypatch.setattr(registry, "_scope_argv", refuse_scope)
    monkeypatch.setattr(process_registry_module, "_stop_systemd_unit", lambda _unit: False)
    with pytest.raises(OSError, match="systemd PTY setup refused"):
        registry.spawn_local(
            _child("print('must-not-pipe', flush=True)"), cwd=str(tmp_path), use_pty=True,
            task_id="portable-systemd-task", owner_task_id="portable-systemd-owner",
            session_key="portable-systemd-session", notify_on_complete=True,
        )
    retained = next(iter(registry._running.values()))
    assert retained.process is None
    assert retained._pty is None
    assert retained.pid is None
    assert retained.systemd_unit == "hermes-test.scope"
    assert retained.custody_state == "unknown"
    assert registry.get(retained.id) is retained
