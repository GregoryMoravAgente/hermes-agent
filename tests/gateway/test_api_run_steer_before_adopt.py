"""POST /v1/runs/{id}/steer accepted while the claimed turn is still building its agent reaches it."""
import asyncio
import threading
from types import SimpleNamespace

import pytest

from agent.interrupt_control import InterruptControlMixin
from tests.gateway.conftest import runtime_owned_api
from tests.gateway.test_api_unknown_resolution import _AUTH_HEADERS, _runs_client
from tests.waiting import async_wait_until


class Agent(InterruptControlMixin):
    def __init__(self):
        self._pending_steer = None
        self._interrupt_requested = False

    def interrupt(self, *args, **kwargs):
        self._interrupt_requested = True


@pytest.mark.asyncio
async def test_run_steer_before_adoption_reaches_the_turns_agent_not_the_cached_one(tmp_path, monkeypatch):
    scope = runtime_owned_api(tmp_path, monkeypatch)
    adapter = await scope.__aenter__()
    runner = adapter.gateway_runner
    authority = runner.session_authority
    stale, fresh = Agent(), Agent()
    claimed, steered = asyncio.Event(), threading.Event()
    seen = {}

    async def answer(event):
        from gateway.session_results import execution_result
        authority = runner.session_authority
        sid = next(iter(authority.sessions))
        claimed.set()
        await asyncio.to_thread(steered.wait, 5)
        # The turn builds a fresh agent (the cached one is replaced) and the wiring adopts it.
        generation = authority._handle(SimpleNamespace(profile_id=authority.profile_id, session_id=sid)).execution_generation
        await asyncio.to_thread(authority.adopt_agent, sid, generation, fresh)
        seen["drained"] = fresh._drain_pending_steer()
        execution_result.get()["result"] = {"final_response": "ok", "messages": []}
        return "ok"

    # The previous turn's reusable agent is what the route resolves to before adoption.
    runner._handle_message = answer
    runner._cached_agent_for = lambda route: stale
    runner._wire_adapter_handlers(adapter)
    runner._adapter_for_source = lambda source: adapter
    client = _runs_client(adapter)
    await client.start_server()
    try:
        started = await client.post("/v1/runs", json={"input": "build it", "session_id": "api-session"},
                                    headers=_AUTH_HEADERS | {"Idempotency-Key": "steer-run"})
        assert started.status == 202, await started.text()
        run_id = (await started.json())["run_id"]
        await asyncio.wait_for(claimed.wait(), 5)

        for text in ("use the staging db", "and skip tests"):
            response = await client.post(f"/v1/runs/{run_id}/steer", json={"input": text}, headers=_AUTH_HEADERS)
            assert response.status == 200, await response.text()
            assert (await response.json())["accepted"] is True
        steered.set()
        await asyncio.wait_for(authority.sessions["api-session"].task, 5)

        assert seen["drained"] == "use the staging db\nand skip tests", "the acknowledged steer was lost"
        assert stale._pending_steer is None, "the replaced cached agent must not swallow it"
        assert not authority.pending_steers
        await async_wait_until(
            lambda: adapter._run_custody.get(run_id) is None
            and adapter._run_launches.get(run_id) is None,
            timeout=5, what="structured I02 finalizer retirement",
        )
    finally:
        steered.set()
        for task in list(adapter._active_run_tasks.values()):
            task.cancel()
        await client.close()
        adapter._response_store.close()
        adapter._run_idempotency_store.close()
        await scope.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_structured_run_retains_unknown_when_terminal_sentinel_is_refused(
    tmp_path, monkeypatch,
):
    """A real /v1/runs producer refusal retains the exact child and I02 completion."""
    with pytest.raises(RuntimeError, match="api_runtime_cleanup_unproven"):
        async with runtime_owned_api(tmp_path, monkeypatch, cleanup_readback=True) as adapter:
            release = asyncio.Event()
            claimed = asyncio.Event()

            async def answer(event):
                from gateway.session_results import execution_result
                claimed.set()
                await release.wait()
                execution_result.get()["result"] = {"final_response": "ok", "messages": []}
                return "ok"

            runner = adapter.gateway_runner
            runner._handle_message = answer
            runner._wire_adapter_handlers(adapter)
            runner._adapter_for_source = lambda source: adapter
            client = _runs_client(adapter)
            await client.start_server()
            try:
                started = await client.post(
                    "/v1/runs", json={"input": "retain refusal", "session_id": "api-session"},
                    headers=_AUTH_HEADERS | {"Idempotency-Key": "sentinel-refusal"},
                )
                assert started.status == 202, await started.text()
                run_id = (await started.json())["run_id"]
                await asyncio.wait_for(claimed.wait(), 5)
                await async_wait_until(
                    lambda: adapter._run_launches.get(run_id) is not None,
                    timeout=5, what="structured launch before sentinel fault",
                )
                launch = adapter._run_launches[run_id]
                original_put = launch.queue.put_nowait

                def refuse_sentinel(event):
                    if event is None:
                        raise RuntimeError("sentinel refused")
                    return original_put(event)

                launch.queue.put_nowait = refuse_sentinel
                release.set()
                await async_wait_until(
                    lambda: adapter._run_custody.get(run_id) is not None
                    and adapter._run_custody[run_id].unknown_reason == "stream_close_unproven",
                    timeout=5, what="structured sentinel refusal",
                )
                custody = adapter._run_custody[run_id]
                assert custody.released is False
                assert launch.terminal_event_outcome is True
                assert launch.stream_close_outcome is None
                assert adapter._run_launches.get(run_id) is launch
            finally:
                release.set()
                await client.close()
