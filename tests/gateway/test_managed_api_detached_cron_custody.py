"""Public API detached-cron custody through the started gateway and real agent path."""
from __future__ import annotations

import asyncio
import json

import aiohttp
import pytest


async def _wait_until(deadline: float, predicate, *, what: str) -> None:
    """Poll one predicate inside the caller's single absolute 90-second budget."""
    loop = asyncio.get_running_loop()
    while True:
        if predicate():
            return
        remaining = deadline - loop.time()
        assert remaining > 0, f"timed out waiting for {what}"
        await asyncio.sleep(min(0.02, remaining))


async def _await_owned_task(task):
    """Await the exact detached task on the loop that created it."""
    return await asyncio.shield(task)


@pytest.mark.asyncio
async def test_public_started_api_detached_cron_runs_real_agent_and_settles(
    started_mux, monkeypatch,
):
    """A real started API gateway owns the detached AIAgent cron attempt end to end."""
    from cron import executions, jobs
    from cron.scheduler_provider import InProcessCronScheduler
    from gateway.config import Platform

    runner, homes, _loop, _call = started_mux
    home = homes["default"]
    adapter = runner.adapters[Platform.API_SERVER]
    provider = InProcessCronScheduler()
    claimed = []
    original_claim_fire = provider.claim_fire

    def observe_claim(job_id, **kwargs):
        result = original_claim_fire(job_id, **kwargs)
        if result is not None:
            claimed.append(result)
        return result

    monkeypatch.setattr(provider, "claim_fire", observe_claim)
    monkeypatch.setattr("cron.scheduler_provider.resolve_cron_scheduler", lambda: provider)
    monkeypatch.setattr(
        "plugins.cron_providers.chronos.verify.get_fire_verifier",
        lambda: (lambda **_kwargs: {"purpose": "cron_fire"}),
    )

    custody = runner.runtime_custody
    baseline_active_owner_classes = tuple(custody.status()["active_owner_classes"])

    with jobs.use_cron_store(home):
        job = jobs.create_job(
            prompt="Reply with the synthetic cron result.",
            schedule="every 1h",
            name="managed public API agent cron",
            deliver="local",
        )
        deadline = asyncio.get_running_loop().time() + 90
        listener_url = f"http://{adapter._host}:{adapter._port}/api/cron/fire"
        async with aiohttp.ClientSession() as client:
            async with asyncio.timeout_at(deadline):
                async with client.post(
                    listener_url,
                    headers={"Authorization": f"Bearer {adapter._api_key}"},
                    json={"job_id": job["id"]},
                ) as response:
                    assert response.status == 202
            await _wait_until(
                deadline,
                lambda: bool(claimed) and claimed[0].get("_cron_custody") is not None,
                what="the public API claim receipt",
            )
            claimed_job = claimed[0]
            child = claimed_job["_cron_custody"]
            parent = child.parent
            assert child.owner_class == "I12_cron_fire"
            assert parent is not None
            assert parent.owner_class == "I14_api_detached_cron"
            assert parent.child is child
            assert child.handle.profile_id == str(home.resolve())
            assert parent.handle.profile_id == str(home.resolve())
            assert child.handle.process_instance == parent.handle.process_instance
            assert child.handle.authority_epoch == parent.handle.authority_epoch

            await _wait_until(
                deadline,
                lambda: child._future is not None,
                what="the exact detached thread task",
            )
            detached_task = child._future
            remaining = deadline - asyncio.get_running_loop().time()
            assert remaining > 0, "detached task exhausted the single test budget before joining"
            owner_future = asyncio.run_coroutine_threadsafe(
                _await_owned_task(detached_task), _loop,
            )
            await asyncio.wait_for(
                asyncio.shield(asyncio.wrap_future(owner_future)), timeout=remaining,
            )
            await _wait_until(
                deadline,
                lambda: child.settled and parent.settled,
                what="the I12 child and I14 parent terminal finalizers",
            )
            assert detached_task.done() and not detached_task.cancelled()
            assert child._physical_ready() is True

        execution_id = claimed_job["execution_id"]
        record = executions.get_execution(execution_id)
        # Keep the actual synthetic agent failure in native CI's captured output;
        # status alone cannot distinguish execution failure from custody settlement.
        print(json.dumps({
            "cron_execution_status": record.get("status") if record else None,
            "cron_execution_error": record.get("error") if record else None,
        }, sort_keys=True))
        assert record is not None
        assert record["id"] == execution_id
        assert record["job_id"] == job["id"]
        assert record["source"] == "builtin"
        assert record["status"] == "completed"
        assert record["error"] is None
        assert record["delivery_outcome"] == "suppressed"

    status = custody.status()
    assert (
        tuple(status["active_owner_classes"]) == baseline_active_owner_classes
        and not {"I14_api_detached_cron", "I12_cron_fire"}.intersection(
            status["active_owner_classes"]
        )
    )
    assert status["unknown_owner_classes"] == []
