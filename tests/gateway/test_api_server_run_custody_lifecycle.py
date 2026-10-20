"""Route-level custody lifecycle proofs for API runs."""

import asyncio
import os
import shlex
import sys
import threading
from contextlib import suppress
from unittest.mock import MagicMock, patch

import pytest
from aiohttp.test_utils import TestClient, TestServer

from tests.gateway.test_api_server_custody_fixtures import (
    create_runs_app, make_adapter, use_idempotency_db,
)
from tests.gateway.test_api_server_runs import _make_slow_agent
from tests.waiting import async_wait_until, wait_until


class _RecordingDirectAgent:
    """Small real route consumer that records the authenticated private run owner."""

    def __init__(self, seen, session_id, barrier=None):
        self._seen = seen
        self._barrier = barrier
        self.session_id = session_id or ""
        self.session_prompt_tokens = 0
        self.session_completion_tokens = 0
        self.session_total_tokens = 0
        self.provider = ""
        self.model = ""

    def run_conversation(self, **kwargs):
        from gateway.session_context import get_i03_custody_binding

        binding = get_i03_custody_binding()
        self._seen.append((
            kwargs["task_id"],
            binding.process_task_id,
            binding.process_owner_task_id,
        ))
        if self._barrier is not None:
            self._barrier.wait(timeout=5)
        return {"final_response": "done"}


class _ProcessBackedDirectAgent:
    """Route test agent whose turn uses the production registry/reader/ACK path."""

    def __init__(self, registry):
        self._registry = registry
        self._process_owner_task_ids = set()
        self.session_id = ""
        self.session_prompt_tokens = 0
        self.session_completion_tokens = 0
        self.session_total_tokens = 0
        self.provider = "fixture-provider"
        self.model = "fixture-model"

    def run_conversation(self, **kwargs):
        from gateway.session_context import get_i03_custody_binding

        binding = get_i03_custody_binding()
        assert binding is not None
        assert binding.process_scope is not None
        child_code = 'import time; print("route-child", flush=True); time.sleep(0.05)'
        command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
        session = self._registry.spawn_local(
            command, cwd=".", task_id=kwargs["task_id"], owner_task_id=binding.process_owner_task_id,
            session_key=kwargs["task_id"], parent_handle=binding.handle,
            scope_fence=binding.process_scope,
        )
        session.notify_on_complete = True
        wait_until(
            lambda: session._completion_event.is_set(), timeout=5,
            what="route child completion publication",
        )
        events = self._registry.drain_notifications(
            session_key=kwargs["task_id"], skip_poll_observed=False,
        )
        assert events and events[0][0]["session_id"] == session.id
        token = events[0][0].get("delivery_token")
        assert isinstance(token, str) and token
        self._registry.acknowledge_completion_delivery(token)
        wait_until(
            lambda: session.custody_state != "accepted", timeout=5,
            what="route child custody settlement",
        )
        assert session.custody_state == "terminal", vars(session.custody)
        return {"final_response": "done"}


def test_stream_begin_failure_resets_binding_before_custody_finalizer(monkeypatch):
    """A failed stream writer setup resets its token while retaining the armed custody scope."""
    from gateway.platforms.api_server_run_execution import (
        begin_legacy_stream_turn, finish_legacy_stream_custody, reserve_legacy_stream,
    )
    from gateway.session_context import get_i03_custody_binding

    adapter = make_adapter(monkeypatch=monkeypatch)
    custody = reserve_legacy_stream(adapter, None, "stream-writer-failure")
    with patch(
        "gateway.platforms.api_server_run_execution.begin_legacy_direct_turn",
        side_effect=RuntimeError("writer refused"),
    ):
        with pytest.raises(RuntimeError, match="writer refused"):
            begin_legacy_stream_turn(
                adapter, custody, task_id="stream-task", profile_id=custody.profile_id,
                session_id="stream-session")

    assert get_i03_custody_binding() is None
    assert custody.process_scope is not None
    finish_legacy_stream_custody(custody, "stream-task", None)
    assert custody.released is False
    assert custody.unknown_reason == "custody_pending"


def test_late_scope_producer_clears_stale_process_proof(monkeypatch):
    """A late producer cannot reuse an earlier I15 proof boolean for terminal release."""
    from gateway.platforms.api_server_run_custody import ApiRunCustody

    adapter = make_adapter(monkeypatch=monkeypatch)
    custody, refusal = ApiRunCustody.reserve(adapter, "api-run-owner", None)
    assert refusal is None
    assert custody.arm_process_scope("shared-session", owner_task_id="api-run-owner") is None
    custody.process_proof = True
    assert custody.process_scope.close()["childless"] is True
    assert custody.process_scope.record_session("late-session", "api-run-owner") is False
    assert custody.close_process_scope() is None
    assert custody.process_proof is False


@pytest.mark.asyncio
async def test_pre_worker_failure_closes_the_launch_boundary_before_release(monkeypatch):
    adapter = make_adapter(monkeypatch=monkeypatch)
    app = create_runs_app(adapter)
    async with TestClient(TestServer(app)) as client:
        with patch.object(adapter, "_create_agent", side_effect=RuntimeError("create failed")):
            response = await client.post("/v1/runs", json={"input": "hello"})
            assert response.status == 202
            run_id = (await response.json())["run_id"]
            await async_wait_until(
                lambda: adapter._run_statuses.get(run_id, {}).get("status") == "failed",
                timeout=5, what="pre-worker failed run")
            await async_wait_until(
                lambda: adapter._run_custody.get(run_id) is not None
                and adapter._run_custody[run_id].released,
                timeout=5, what="pre-worker custody release")
    custody = adapter._run_custody[run_id]
    assert custody.no_worker_finalized is True
    assert custody.unknown_reason is None
    assert custody.process_scope_receipt is not None
    assert custody.process_scope_receipt["childless"] is True


@pytest.mark.asyncio
async def test_transport_ttl_drops_stream_but_retains_live_control_and_custody(monkeypatch):
    adapter = make_adapter(monkeypatch=monkeypatch)
    adapter._max_concurrent_runs = 1
    app = create_runs_app(adapter)
    async with TestClient(TestServer(app)) as client:
        with patch.object(adapter, "_create_agent") as create:
            agent, ready, _ = _make_slow_agent()
            create.return_value = agent
            response = await client.post("/v1/runs", json={"input": "hello"})
            assert response.status == 202
            run_id = (await response.json())["run_id"]
            assert ready.wait(timeout=3)
            task = adapter._active_run_tasks[run_id]
            adapter._run_streams_created[run_id] -= adapter._RUN_STREAM_TTL + 1
            with patch(
                "gateway.platforms.api_server.asyncio.sleep",
                side_effect=[None, asyncio.CancelledError()],
            ):
                with pytest.raises(asyncio.CancelledError):
                    await adapter._sweep_orphaned_runs()
            assert adapter._active_run_tasks[run_id] is task
            assert adapter._run_custody[run_id].transport_retired is True
            assert adapter._run_custody[run_id].released is False
            await client.post(f"/v1/runs/{run_id}/stop")


@pytest.mark.asyncio
async def test_queued_run_retains_one_worker_future_on_launch_and_custody(monkeypatch):
    """The queued route keeps the original worker Future as its custody witness."""
    adapter = make_adapter(monkeypatch=monkeypatch)
    app = create_runs_app(adapter)
    async with TestClient(TestServer(app)) as client:
        with patch.object(adapter, "_create_agent") as create:
            agent, ready, interrupted = _make_slow_agent()
            create.return_value = agent
            response = await client.post("/v1/runs", json={"input": "hello"})
            assert response.status == 202
            run_id = (await response.json())["run_id"]
            assert ready.wait(timeout=3)
            await async_wait_until(
                lambda: adapter._run_launches.get(run_id) is not None
                and adapter._run_launches[run_id].worker_future is not None,
                timeout=5, what="queued run worker future")
            launch = adapter._run_launches[run_id]
            custody = adapter._run_custody[run_id]
            assert launch.execution_task is adapter._active_run_tasks[run_id]
            assert launch.worker_future is custody.worker_future
            assert launch.worker_future.done() is False
            stop = await client.post(f"/v1/runs/{run_id}/stop")
            assert stop.status == 200
            await async_wait_until(
                lambda: interrupted.is_set() and launch.worker_future.done(),
                timeout=5, what="queued run worker settlement")


@pytest.mark.asyncio
async def test_direct_run_binds_executor_scope_and_releases_after_close(monkeypatch):
    """The direct /v1/runs worker carries its binding through scope close and release."""
    adapter = make_adapter(monkeypatch=monkeypatch)
    app = create_runs_app(adapter)
    seen = {}
    agent = MagicMock()
    agent.session_prompt_tokens = 0
    agent.session_completion_tokens = 0
    agent.session_total_tokens = 0

    def _run_conversation(**_kwargs):
        from gateway.session_context import get_i03_custody_binding
        seen["binding"] = get_i03_custody_binding()
        return {"final_response": "done"}

    agent.run_conversation.side_effect = _run_conversation
    async with TestClient(TestServer(app)) as client:
        with patch.object(adapter, "_create_agent", return_value=agent):
            response = await client.post("/v1/runs", json={"input": "hello"})
            assert response.status == 202
            run_id = (await response.json())["run_id"]
            await async_wait_until(
                lambda: adapter._run_custody.get(run_id) is not None
                and adapter._run_custody[run_id].released,
                timeout=5, what="direct custody release")
    custody = adapter._run_custody[run_id]
    assert seen["binding"] is custody
    assert custody.process_scope_receipt["owner_task_id"] == custody.process_task_id
    assert custody.process_scope_receipt["owner_task_id"] == custody.process_owner_task_id
    assert custody.process_scope_receipt["childless"] is True
    assert custody.unknown_reason is None
    assert adapter.active_agent_work_count() == 0


@pytest.mark.platforms("windows")
@pytest.mark.asyncio
async def test_real_route_process_sqlite_readback_and_finalizer_keep_private_owner(
    monkeypatch, tmp_path,
):
    """The public route consumes a real child/reader/ACK and durable run row before release."""
    path = tmp_path / "route-idempotency.db"
    adapter = make_adapter(monkeypatch=monkeypatch)
    use_idempotency_db(adapter, path)
    app = create_runs_app(adapter)
    agent = _ProcessBackedDirectAgent(adapter._fixture_process_registry)
    async with TestClient(TestServer(app)) as client:
        with patch.object(adapter, "_create_agent", return_value=agent):
            response = await client.post(
                "/v1/runs", json={"input": "hello"}, headers={"Idempotency-Key": "route-child"},
            )
            assert response.status == 202
            run_id = (await response.json())["run_id"]
            await async_wait_until(
                lambda: adapter._run_custody.get(run_id) is not None
                and adapter._run_custody[run_id].released,
                timeout=8, what="real route custody release",
            )
    custody = adapter._run_custody[run_id]
    assert custody.unknown_reason is None
    assert custody.process_scope_receipt["created_session_ids"]
    assert run_id in agent._process_owner_task_ids
    adapter._run_idempotency_store.close()

    restarted = make_adapter(monkeypatch=monkeypatch)
    use_idempotency_db(restarted, path)
    restarted_app = create_runs_app(restarted)
    async with TestClient(TestServer(restarted_app)) as client:
        status = await client.get(f"/v1/runs/{run_id}")
        body = await status.json()
    assert status.status == 200
    assert body["status"] == "completed"
    assert body["output"] == "done"


@pytest.mark.asyncio
async def test_direct_scope_close_failure_retains_unknown_custody(monkeypatch):
    """A real direct route close failure remains unknown instead of releasing its owner."""
    adapter = make_adapter(monkeypatch=monkeypatch)
    app = create_runs_app(adapter)
    agent = MagicMock()
    agent.session_prompt_tokens = 0
    agent.session_completion_tokens = 0
    agent.session_total_tokens = 0
    agent.run_conversation.return_value = {"final_response": "done"}
    async with TestClient(TestServer(app)) as client:
        with patch.object(adapter, "_create_agent", return_value=agent):
            with patch(
                "tools.process_registry_custody_evidence.ProcessScopeFence.close",
                side_effect=RuntimeError("scope close failed"),
            ):
                response = await client.post("/v1/runs", json={"input": "hello"})
                assert response.status == 202
                run_id = (await response.json())["run_id"]
                await async_wait_until(
                    lambda: adapter._run_custody.get(run_id) is not None
                    and adapter._run_custody[run_id].unknown_reason == "process_scope_unavailable",
                    timeout=5, what="direct custody unknown")
    custody = adapter._run_custody[run_id]
    assert custody.released is False
    scope = custody.process_scope
    assert scope is not None
    assert custody.close_process_scope() is None
    assert custody.process_scope is scope
    assert custody.process_scope_receipt["owner_task_id"] == custody.process_owner_task_id
    assert custody.released is False


@pytest.mark.asyncio
async def test_same_session_runs_keep_distinct_process_owner_ids(monkeypatch):
    """Shared conversation identity cannot make one run prove another run's process scope."""
    adapter = make_adapter(monkeypatch=monkeypatch)
    app = create_runs_app(adapter)
    seen = []
    barrier = threading.Barrier(2)

    def _create_agent(**kwargs):
        return _RecordingDirectAgent(seen, kwargs.get("session_id"), barrier)

    async with TestClient(TestServer(app)) as client:
        with patch.object(adapter, "_create_agent", side_effect=_create_agent):
            run_ids = []
            for value in ("first", "second"):
                response = await client.post(
                    "/v1/runs", json={"input": value, "session_id": "shared-session"})
                assert response.status == 202
                run_ids.append((await response.json())["run_id"])
            for run_id in run_ids:
                await async_wait_until(
                    lambda run_id=run_id: adapter._run_custody.get(run_id) is not None
                    and adapter._run_custody[run_id].released,
                    timeout=5, what=f"shared-session run {run_id} release")

    assert {task_id for task_id, _public_task_id, _owner in seen} == {"shared-session"}
    assert {public_task_id for _task_id, public_task_id, _owner in seen} == {"shared-session"}
    assert {owner for _task_id, _public_task_id, owner in seen} == set(run_ids)
    for run_id in run_ids:
        custody = adapter._run_custody[run_id]
        assert custody.process_task_id == "shared-session"
        assert custody.process_owner_task_id == run_id
        assert custody.process_scope_receipt["owner_task_id"] == run_id


@pytest.mark.platforms("linux", "macos")
def test_agent_hard_close_reaps_private_owner_but_keeps_same_session_sibling(
    tmp_path, monkeypatch,
):
    """The real AIAgent.close consumer targets only its private run owner."""
    from run_agent import AIAgent
    from tools.process_registry import ProcessRegistry
    import tools.process_registry as process_registry_module

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes-home"))
    monkeypatch.setattr(process_registry_module, "CHECKPOINT_PATH", tmp_path / "processes.json")
    monkeypatch.setattr(process_registry_module, "_SYSTEMD_SCOPE_AVAILABLE", False, raising=False)
    registry = ProcessRegistry()
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    monkeypatch.setattr("run_agent.cleanup_vm", lambda _task_id: None)
    monkeypatch.setattr("run_agent.cleanup_browser", lambda _task_id: None)
    monkeypatch.setattr("tools.computer_use.tool.release_computer_use_session", lambda _task_id: None)

    public_session = "shared-hard-close-session"
    private_owner = "private-hard-close-run"
    sibling_owner = "sibling-hard-close-run"
    owner_gate = tmp_path / "owner-gate"
    sibling_gate = tmp_path / "sibling-gate"
    os.mkfifo(owner_gate)
    os.mkfifo(sibling_gate)
    sessions = []

    def spawn(gate, owner):
        child_code = f"from pathlib import Path; Path({str(gate)!r}).read_text(encoding='utf-8')"
        return registry.spawn_local(
            f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}",
            cwd=str(tmp_path), task_id=public_session, owner_task_id=owner,
            session_key=public_session,
        )

    try:
        owner_session = spawn(owner_gate, private_owner)
        sibling_session = spawn(sibling_gate, sibling_owner)
        sessions.extend((owner_session, sibling_session))
        wait_until(
            lambda: owner_session.process is not None and owner_session.process.poll() is None,
            timeout=5, what="private owner child to remain live",
        )
        wait_until(
            lambda: sibling_session.process is not None and sibling_session.process.poll() is None,
            timeout=5, what="same-session sibling child to remain live",
        )

        agent = AIAgent.__new__(AIAgent)
        agent.session_id = public_session
        agent.client = None
        agent._active_children_lock = threading.Lock()
        agent._active_children = set()
        agent._end_session_on_close = False
        agent._session_messages = []
        agent._process_owner_task_ids = {private_owner}
        agent.close()

        wait_until(
            lambda: owner_session.process is not None and owner_session.process.poll() is not None,
            timeout=5, what="private owner child to be reaped by AIAgent.close",
        )
        assert sibling_session.process is not None and sibling_session.process.poll() is None
        assert sibling_session.task_id == public_session
        assert sibling_session.session_key == public_session
        assert sibling_session.owner_task_id == sibling_owner
        assert registry.get(sibling_session.id) is sibling_session
    finally:
        for session, gate in zip(sessions, (owner_gate, sibling_gate)):
            process = getattr(session, "process", None)
            if process is not None and process.poll() is None:
                try:
                    fd = os.open(str(gate), os.O_WRONLY | os.O_NONBLOCK)
                    try:
                        os.write(fd, b"release")
                    finally:
                        os.close(fd)
                except OSError:
                    pass
                with suppress(Exception):
                    wait_until(
                        lambda process=process: process.poll() is not None,
                        timeout=5, what=f"{session.owner_task_id} child cleanup",
                    )
            if process is not None and process.poll() is None:
                with suppress(Exception):
                    registry.kill_process(session.id, source="test_cleanup", consume_output=False)
        for gate in (owner_gate, sibling_gate):
            if gate.exists():
                gate.unlink()
