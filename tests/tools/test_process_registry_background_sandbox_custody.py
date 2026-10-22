"""Real supported-backend controls for sandbox background custody.

These controls deliberately use DockerEnvironment rather than the marker-only fake from the
portable unit module.  The response-loss wrapper raises only after the real backend has accepted
the launch command; it never fabricates a PID, process, exit result, or custody handle.
"""
from __future__ import annotations

import json

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from gateway.session_context import clear_session_vars, set_session_vars
from hermes_cli.backend_retirement import RetirementFence
from tests.tools.process_registry_native_profile import prepare_native_profile
from tools.process_registry import ProcessRegistry
from tools import terminal_tool_background


_IMAGE = "python:3.14.7-slim-bookworm"


def _registry(tmp_path, monkeypatch) -> ProcessRegistry:
    import tools.process_registry as process_registry_module

    registry = ProcessRegistry()
    registry.bind_runtime_custody(RuntimeCustodyCoordinator(
        process_instance="docker-background-producer-r7",
        authority_epoch=29,
        fence=RetirementFence(),
    ))
    prepare_native_profile(tmp_path, monkeypatch)
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    return registry


def _backend_or_skip(task_id: str):
    from tools.environments import docker as docker_module

    docker_executable = docker_module.find_docker()
    if docker_executable is None:
        pytest.skip("Docker backend is unavailable")
    # The bounded source proposal must never make a test run pull an image.  Root
    # checks the known image inventory before execution; an absent image is an
    # honest infrastructure skip, while the constructor would otherwise pull it.
    image_probe = docker_module._docker_query(
        [docker_executable, "image", "inspect", _IMAGE, "--format", "{{.Id}}"],
        timeout=10,
        fail="Docker image preflight failed: %s",
        nonzero="Docker image preflight returned %d: %s",
    )
    if image_probe is None:
        pytest.skip("Docker image is not preloaded")
    try:
        return docker_module.DockerEnvironment(
            image=_IMAGE,
            cwd="/root",
            timeout=30,
            persistent_filesystem=False,
            task_id=task_id,
            network=False,
            persist_across_processes=False,
        )
    except (OSError, RuntimeError) as exc:
        pytest.skip(f"Docker backend could not start: {type(exc).__name__}")


class _PostLaunchBackend:
    """Delegate every operation to a real backend, losing only the launch response."""

    def __init__(self, backend, *, lose_response: bool = False, lose_after_launch: bool = False):
        self._backend = backend
        self._lose_response = lose_response
        self._lose_after_launch = lose_after_launch
        self._launched = False
        self.launch_calls = 0
        self.host_cwd = getattr(backend, "host_cwd", None)
        self.env = getattr(backend, "env", {})

    def get_temp_dir(self):
        return self._backend.get_temp_dir()

    def execute(self, command, **kwargs):
        if self._launched and self._lose_after_launch:
            raise RuntimeError("sandbox environment disappeared after launch")
        result = self._backend.execute(command, **kwargs)
        if not self._launched and "nohup bash" in command and "echo $!" in command:
            self._launched = True
            self.launch_calls += 1
            if self._lose_response:
                raise RuntimeError("sandbox launch response was lost after child creation")
        return result


def _public_spawn(registry, env, *, task_id: str) -> str:
    tokens = set_session_vars(
        platform="cli", source="terminal", session_key=f"r7-{task_id}",
        session_id=f"history-{task_id}", profile="default",
        async_delivery=True, session_history_delivery="1",
    )
    try:
        return terminal_tool_background.spawn_background_process(
            command="python -c 'import time; print(\"r7-docker-child\", flush=True); time.sleep(0.2)'",
            env=env,
            env_type="docker",
            effective_task_id=task_id,
            task_id=task_id,
            session_key=f"r7-{task_id}",
            workdir="/root",
            cwd="/root",
            effective_pty=False,
            notify_on_complete=True,
            watch_patterns=None,
            approval_note=None,
            completion_output_chars=0,
            pty_disabled_reason=None,
            heartbeat_seconds=0,
            persist_on_release=True,
            mounted_host=None,
            auto_backgrounded=False,
        )
    finally:
        clear_session_vars(tokens)


def _one_event_and_ack(registry, session):
    events = registry.drain_notifications(session_key=session.session_key)
    matching = [event for event, _text in events if event.get("session_id") == session.id]
    assert len(matching) == 1
    token = matching[0].get("delivery_token")
    assert isinstance(token, str) and token
    registry.acknowledge_completion_delivery(token)
    assert session.custody_delivery_acked is True
    return matching[0]


def test_real_docker_response_loss_reconciles_same_session_without_duplicate_launch(tmp_path, monkeypatch):
    """A real Docker child whose launch reply is lost completes from the same marker."""
    registry = _registry(tmp_path, monkeypatch)
    backend = _backend_or_skip("r7-response-loss")
    env = _PostLaunchBackend(backend, lose_response=True)
    try:
        result = json.loads(_public_spawn(registry, env, task_id="r7-response-loss"))
        session = registry.get(result["session_id"])
        assert session is not None
        original_handle = session.custody.handle
        settled = registry.wait_for_pending_completions(
            task_id=session.owner_task_id, timeout=35, poll_interval=0.1,
        )
        assert session.id in settled["completed"]
        assert env.launch_calls == 1
        assert session.pid is not None
        assert getattr(session, "_sandbox_marker_pid", None) == session.pid
        assert session.custody.handle is original_handle
        assert session.custody.pid == session.pid
        assert session.host_start_time is None
        assert session.pid_scope == "sandbox"
        assert session.custody.host_start_time is None
        assert session.custody_process_bound is False
        assert session.custody_durable is True
        assert session.custody_reader_eof is True
        event = _one_event_and_ack(registry, session)
        assert event["session_id"] == session.id
        # Docker has no host Job/reader scope proof; the unsupported final native
        # boundary remains unknown rather than being converted to success.
        assert session.custody_state == "unknown"
        assert registry.get(session.id) is session
    finally:
        backend.cleanup(force_remove=True)
        backend.wait_for_cleanup()


def test_real_docker_environment_loss_retains_unknown_and_does_not_relaunch(tmp_path, monkeypatch):
    """A real backend disappearing after launch leaves one unknown session and no retry launch."""
    registry = _registry(tmp_path, monkeypatch)
    backend = _backend_or_skip("r7-environment-loss")
    env = _PostLaunchBackend(backend, lose_after_launch=True)
    try:
        result = json.loads(_public_spawn(registry, env, task_id="r7-environment-loss"))
        session = registry.get(result["session_id"])
        assert session is not None
        original_handle = session.custody.handle
        settled = registry.wait_for_pending_completions(
            task_id=session.owner_task_id, timeout=20, poll_interval=0.1,
        )
        assert session.id in settled["completed"]
        assert env.launch_calls == 1
        assert registry.get(session.id) is session
        assert session.custody.handle is original_handle
        assert session.custody_state == "unknown"
        assert session.custody_error in {"backend_lost", "read_failed", "partial_launch"}
        assert session.pid is None or getattr(session, "_sandbox_marker_pid", session.pid) == session.pid
        assert session.host_start_time is None
        assert session.pid_scope == "sandbox"
        assert session.custody.host_start_time is None
        assert session.custody_process_bound is False
        event = _one_event_and_ack(registry, session)
        assert event["session_id"] == session.id
    finally:
        backend.cleanup(force_remove=True)
        backend.wait_for_cleanup()
