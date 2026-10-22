"""Managed API cron admission must refuse providers without an exact custody receipt."""
from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter, cors_middleware
from cron.runtime_custody import CronOwnerBinding
from gateway.runtime_custody import CustodyRefusal, RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence


def _new_api_custody(coordinator, owner_id: str):
    handle = coordinator.reserve_root(
        "I14_api_detached_cron", owner_id, profile_id="synthetic-profile",
        authority_epoch=coordinator.authority_epoch)
    assert not isinstance(handle, CustodyRefusal)
    return CronOwnerBinding(
        coordinator=coordinator, handle=handle, owner_class="I14_api_detached_cron",
        owner_id=owner_id, job_id="synthetic-job", execution_id=None,
        source="api-cron", scheduled_instant=None)


def _new_cron_fire_adapter():
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "synthetic-key"}))
    adapter._pending_agent_requests = 1
    return adapter


@pytest.mark.asyncio
async def test_managed_api_refuses_legacy_boolean_provider_before_detach(monkeypatch):
    coordinator = RuntimeCustodyCoordinator(
        process_instance="api-custody-test", authority_epoch=13, fence=RetirementFence())
    runner = SimpleNamespace(
        _draining=False,
        _external_drain_active=False,
        _running=True,
        adapters={},
        runtime_custody=coordinator,
        session_runtime_descriptor={
            "instance_id": "api-custody-test", "authority_epoch": 13, "state": "ready"},
    )

    class LegacyProvider:
        def fire_due(self, job_id, *, adapters=None, loop=None):
            raise AssertionError("legacy provider must not be detached")

    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "synthetic-key"}))
    adapter.gateway_runner = runner
    app = web.Application(middlewares=[cors_middleware])
    app["api_server_adapter"] = adapter
    app.router.add_post("/api/cron/fire", adapter._handle_cron_fire)
    monkeypatch.setattr("cron.scheduler_provider.resolve_cron_scheduler", lambda: LegacyProvider())
    monkeypatch.setattr(
        "plugins.cron_providers.chronos.verify.get_fire_verifier",
        lambda: (lambda **kw: {"purpose": "cron_fire"}),
    )
    with patch("gateway.run._gateway_runner_ref", lambda: runner):
        async with TestClient(TestServer(app)) as client:
            response = await client.post(
                "/api/cron/fire", headers={"Authorization": "Bearer valid"},
                json={"job_id": "legacy-job"})
    assert response.status == 503
    assert coordinator.status()["active_owner_classes"] == []


@pytest.mark.asyncio
async def test_managed_api_refuses_provider_receipt_before_returning_202(monkeypatch):
    coordinator = RuntimeCustodyCoordinator(
        process_instance="api-custody-receipt-test", authority_epoch=14, fence=RetirementFence())
    runner = SimpleNamespace(
        _draining=False,
        _external_drain_active=False,
        _running=True,
        adapters={},
        runtime_custody=coordinator,
        session_runtime_descriptor={
            "instance_id": "api-custody-receipt-test", "authority_epoch": 14, "state": "ready"},
    )

    class SplitProvider:
        name = "chronos"

        def claim_fire(self, job_id: str, **_kwargs):
            return {"id": job_id, "execution_id": "foreign-execution", "_cron_custody": object()}

        def fire_claimed(self, *_args, **_kwargs):
            raise AssertionError("a foreign receipt must never be detached")

    async def _ready(*_args, **_kwargs):
        return None, {}

    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "synthetic-key"}))
    adapter.gateway_runner = runner
    app = web.Application(middlewares=[cors_middleware])
    app["api_server_adapter"] = adapter
    app.router.add_post("/api/cron/fire", adapter._handle_cron_fire)
    monkeypatch.setattr("cron.scheduler_provider.resolve_cron_scheduler", lambda: SplitProvider())
    monkeypatch.setattr(
        "plugins.cron_providers.chronos.verify.get_fire_verifier",
        lambda: (lambda **kw: {"purpose": "cron_fire"}),
    )
    monkeypatch.setattr(
        "gateway.platforms.api_server_fire_startup.live_adapters_once_started", _ready)
    with patch("gateway.run._gateway_runner_ref", lambda: runner):
        async with TestClient(TestServer(app)) as client:
            response = await client.post(
                "/api/cron/fire", headers={"Authorization": "Bearer valid"},
                json={"job_id": "foreign-job"})
    assert response.status == 503
    assert coordinator.status()["active_owner_classes"] == ["I14_api_detached_cron"]


@pytest.mark.asyncio
async def test_managed_api_split_provider_runs_real_ledger_task_and_releases_custody(
    monkeypatch, tmp_path,
):
    """The managed HTTP route consumes the real provider receipt and detached task to terminal."""
    from cron import executions, jobs, scheduler
    from cron.scheduler_provider import InProcessCronScheduler

    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    scripts_dir = home / "scripts"
    (home / "cron").mkdir(parents=True)
    scripts_dir.mkdir(parents=True)
    db = home / "cron" / "executions.db"
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: home)
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", db)
    coordinator = RuntimeCustodyCoordinator(
        process_instance="api-real-provider-test", authority_epoch=15, fence=RetirementFence())
    runner = SimpleNamespace(
        _draining=False,
        _external_drain_active=False,
        _running=True,
        adapters={},
        runtime_custody=coordinator,
        session_runtime_descriptor={
            "instance_id": "api-real-provider-test", "authority_epoch": 15, "state": "ready"},
        config=SimpleNamespace(
            multiplex_profiles=False,
            _runtime_profile_homes=[("default", home)],
        ),
        _primary_profile_name="default",
    )
    script = scripts_dir / "cron-success.py"
    script.write_text("print('managed cron result', flush=True)\n", encoding="utf-8")
    with jobs.use_cron_store(home):
        job = jobs.create_job(
            prompt="",
            schedule="every 1h",
            name="managed HTTP job",
            script=str(script),
            no_agent=True,
            deliver="local",
        )
    provider = InProcessCronScheduler()
    claimed_ids = []
    claimed_bindings = []
    real_claim_fire = provider.claim_fire

    def _claim_fire(job_id, **kwargs):
        claimed = real_claim_fire(job_id, **kwargs)
        if claimed is not None:
            claimed_ids.append(claimed["execution_id"])
            claimed_bindings.append(claimed["_cron_custody"])
        return claimed

    monkeypatch.setattr(provider, "claim_fire", _claim_fire)
    async def _ready(*_args, **_kwargs):
        return None, {}

    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": "synthetic-key"}))
    adapter.gateway_runner = runner
    app = web.Application(middlewares=[cors_middleware])
    app["api_server_adapter"] = adapter
    app.router.add_post("/api/cron/fire", adapter._handle_cron_fire)
    monkeypatch.setattr("cron.scheduler_provider.resolve_cron_scheduler", lambda: provider)
    monkeypatch.setattr(
        "plugins.cron_providers.chronos.verify.get_fire_verifier",
        lambda: (lambda **_kw: {"purpose": "cron_fire"}),
    )
    monkeypatch.setattr(
        "gateway.platforms.api_server_fire_startup.live_adapters_once_started", _ready)
    with jobs.use_cron_store(home):
        with patch("gateway.run._gateway_runner_ref", lambda: runner):
            async with TestClient(TestServer(app)) as client:
                response = await client.post(
                    "/api/cron/fire", headers={"Authorization": "Bearer valid"},
                    json={"job_id": job["id"]})
                assert response.status == 202
                from tests.waiting import async_wait_until

                await async_wait_until(
                    lambda: bool(claimed_bindings) and claimed_bindings[0]._future is not None,
                    timeout=30, what="the exact detached provider task binding")
                binding = claimed_bindings[0]
                assert binding.handle.profile_id == str(home.resolve())
                assert binding.parent.handle.profile_id == str(home.resolve())
                task = binding._future
                await asyncio.wait_for(asyncio.shield(task), timeout=30)
                await async_wait_until(
                    lambda: binding.settled and binding.parent.settled,
                    timeout=30, what="the exact fire and API custody finalizers")
                assert task.done() and not task.cancelled()
                assert binding._physical_ready() is True
    assert claimed_ids
    record = executions.get_execution(claimed_ids[0])
    assert record is not None
    assert record["job_id"] == job["id"]
    assert record["source"] == "builtin"
    assert record["status"] == "completed"
    assert record["delivery_outcome"] == "suppressed"
    assert coordinator.status()["active_owner_classes"] == []


@pytest.mark.asyncio
async def test_managed_api_cancellation_before_provider_start_releases_unaccepted_custody(
    monkeypatch,
):
    """Cancellation before the provider boundary releases only the still-unaccepted I14 root."""
    from gateway.platforms import api_server_cron_fire as fire_api
    from gateway.platforms.api_server import _release_pending_api_work

    coordinator = RuntimeCustodyCoordinator(
        process_instance="api-cancel-before-claim", authority_epoch=16, fence=RetirementFence())
    api_custody = _new_api_custody(coordinator, "api:cancel-before-claim")
    adapter = _new_cron_fire_adapter()
    reservation = {"active": True, "detached": False}
    startup_started = asyncio.Event()
    startup_release = asyncio.Event()
    provider_called = False

    async def _blocked_startup(*_args, **_kwargs):
        startup_started.set()
        await startup_release.wait()
        return None, {}

    class SplitProvider:
        name = "chronos"

        def claim_fire(self, _job_id):
            nonlocal provider_called
            provider_called = True

        def fire_claimed(self, *_args, **_kwargs):
            raise AssertionError("pre-start cancellation must not reach detached execution")

    monkeypatch.setattr(
        "gateway.platforms.api_server_fire_startup.live_adapters_once_started", _blocked_startup)
    task = asyncio.create_task(fire_api._dispatch_split_fire(
        adapter, reservation, api_custody, SplitProvider(), "cancel-before-claim",
        SimpleNamespace(), 0.0))
    await startup_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert provider_called is False
    assert api_custody.settled
    assert api_custody._claim_future is None
    assert coordinator.status()["active_owner_classes"] == []
    _release_pending_api_work(adapter, reservation)
    assert adapter._pending_agent_requests == 0


@pytest.mark.asyncio
async def test_managed_api_cancellation_during_claim_retains_real_thread_until_unaccepted(
    monkeypatch,
):
    """An unresolved provider claim keeps its exact thread/task until the provider says no claim."""
    from gateway.platforms import api_server_cron_fire as fire_api
    from tests.waiting import async_wait_until

    coordinator = RuntimeCustodyCoordinator(
        process_instance="api-cancel-during-claim", authority_epoch=17, fence=RetirementFence())
    api_custody = _new_api_custody(coordinator, "api:cancel-during-claim")
    adapter = _new_cron_fire_adapter()
    reservation = {"active": True, "detached": False}
    claim_started = threading.Event()
    claim_release = threading.Event()

    class SplitProvider:
        name = "chronos"

        def claim_fire(self, _job_id):
            claim_started.set()
            claim_release.wait(30)

        def fire_claimed(self, *_args, **_kwargs):
            raise AssertionError("an unaccepted claim must not detach")

    async def _ready(*_args, **_kwargs):
        return None, {}

    monkeypatch.setattr(
        "gateway.platforms.api_server_fire_startup.live_adapters_once_started", _ready)
    task = asyncio.create_task(fire_api._dispatch_split_fire(
        adapter, reservation, api_custody, SplitProvider(), "cancel-during-claim",
        SimpleNamespace(), 0.0))
    assert await asyncio.to_thread(claim_started.wait, 30)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert reservation["detached"] is True
    assert api_custody.settled is False
    assert api_custody._claim_future is not None
    assert api_custody._claim_future.done() is False
    assert coordinator.status()["active_owner_classes"] == ["I14_api_detached_cron"]

    claim_release.set()
    await async_wait_until(
        lambda: api_custody.settled and api_custody._claim_future is None
        and adapter._pending_agent_requests == 0,
        timeout=30, what="cancelled claim reconciliation")
    assert coordinator.status()["active_owner_classes"] == []


@pytest.mark.asyncio
async def test_managed_api_cancelled_continuation_during_real_claim_keeps_unknown_custody(
    monkeypatch,
):
    """A cancelled reconciliation keeps the real provider task and releases API bookkeeping."""
    from gateway.platforms import api_server_cron_fire as fire_api
    from tests.waiting import async_wait_until

    coordinator = RuntimeCustodyCoordinator(
        process_instance="api-cancelled-continuation", authority_epoch=171,
        fence=RetirementFence())
    api_custody = _new_api_custody(coordinator, "api:cancelled-continuation")
    adapter = _new_cron_fire_adapter()
    reservation = {"active": True, "detached": False}
    claim_started = threading.Event()
    claim_release = threading.Event()
    continuation_awaiting = asyncio.Event()
    await_calls = 0
    claim_future = None

    class SplitProvider:
        name = "chronos"

        def claim_fire(self, _job_id):
            claim_started.set()
            claim_release.wait(30)

        def fire_claimed(self, *_args, **_kwargs):
            raise AssertionError("an unresolved claim must not detach")

    async def _ready(*_args, **_kwargs):
        return None, {}

    original_await = fire_api._await_split_claim

    async def _mark_await(*args, **kwargs):
        nonlocal await_calls
        await_calls += 1
        if await_calls == 2:
            continuation_awaiting.set()
        return await original_await(*args, **kwargs)

    monkeypatch.setattr(
        "gateway.platforms.api_server_fire_startup.live_adapters_once_started", _ready)
    monkeypatch.setattr(fire_api, "_await_split_claim", _mark_await)
    try:
        task = asyncio.create_task(fire_api._dispatch_split_fire(
            adapter, reservation, api_custody, SplitProvider(), "cancelled-continuation",
            SimpleNamespace(), 0.0))
        assert await asyncio.to_thread(claim_started.wait, 30)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        continuation = await async_wait_until(
            lambda: next((candidate for candidate in adapter._background_tasks
                          if not candidate.done()), None),
            timeout=30, what="tracked cancelled-claim continuation")
        await asyncio.wait_for(continuation_awaiting.wait(), timeout=30)
        claim_future = api_custody._claim_future
        assert claim_future is not None
        continuation.cancel()
        await continuation

        assert reservation["active"] is False
        assert adapter._pending_agent_requests == 0
        assert api_custody._unknown is True
        assert api_custody.settled is False
        assert claim_future.done() is False
        assert coordinator.status()["active_owner_classes"] == ["I14_api_detached_cron"]
    finally:
        claim_release.set()
        if claim_future is not None:
            await asyncio.wait_for(asyncio.shield(claim_future), timeout=30)
    assert claim_future is not None
    assert api_custody._claim_future is claim_future
    assert claim_future.done() is True
    assert api_custody._unknown is True
    assert api_custody.settled is False
    assert coordinator.status()["active_owner_classes"] == ["I14_api_detached_cron"]


@pytest.mark.asyncio
async def test_managed_api_cancelled_continuation_before_first_step_releases_only_pending_api(
    monkeypatch,
):
    """A continuation cancelled before its first instruction still leaves real claim custody unknown."""
    from gateway.platforms import api_server_cron_fire as fire_api

    coordinator = RuntimeCustodyCoordinator(
        process_instance="api-cancelled-before-continuation", authority_epoch=172,
        fence=RetirementFence())
    api_custody = _new_api_custody(coordinator, "api:cancelled-before-continuation")
    adapter = _new_cron_fire_adapter()
    reservation = {"active": True, "detached": False}
    claim_started = threading.Event()
    claim_release = threading.Event()
    continuation_started = asyncio.Event()
    callback_finished = asyncio.Event()
    captured = {}
    claim_future = None

    class SplitProvider:
        name = "chronos"

        def claim_fire(self, _job_id):
            claim_started.set()
            claim_release.wait(30)

        def fire_claimed(self, *_args, **_kwargs):
            raise AssertionError("an unresolved claim must not detach")

    async def _ready(*_args, **_kwargs):
        return None, {}

    original_continue = fire_api._continue_cancelled_split_fire

    async def _mark_continuation_entry(*args, **kwargs):
        continuation_started.set()
        return await original_continue(*args, **kwargs)

    original_track = adapter._track_background_task

    def _cancel_before_first_step(task, **kwargs):
        original_track(task, **kwargs)
        captured["continuation"] = task
        # Registered after production's finalizer, so this observes that callback phase without
        # depending on a new private interface that does not exist on the causal-before source.
        task.add_done_callback(lambda _done: callback_finished.set())
        task.cancel()

    monkeypatch.setattr(
        "gateway.platforms.api_server_fire_startup.live_adapters_once_started", _ready)
    monkeypatch.setattr(fire_api, "_continue_cancelled_split_fire", _mark_continuation_entry)
    adapter._track_background_task = _cancel_before_first_step
    try:
        task = asyncio.create_task(fire_api._dispatch_split_fire(
            adapter, reservation, api_custody, SplitProvider(),
            "cancelled-before-continuation", SimpleNamespace(), 0.0))
        assert await asyncio.to_thread(claim_started.wait, 30)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        continuation = captured["continuation"]
        await asyncio.wait_for(callback_finished.wait(), timeout=30)
        assert continuation.done() is True
        claim_future = api_custody._claim_future
        assert continuation.cancelled() is True
        assert continuation_started.is_set() is False
        assert reservation["active"] is False
        assert adapter._pending_agent_requests == 0
        assert api_custody._unknown is True
        assert api_custody.settled is False
        assert claim_future is not None
        assert claim_future.done() is False
        assert coordinator.status()["active_owner_classes"] == ["I14_api_detached_cron"]
    finally:
        claim_release.set()
        if claim_future is not None:
            await asyncio.wait_for(asyncio.shield(claim_future), timeout=30)
    assert claim_future is not None
    assert api_custody._claim_future is claim_future
    assert claim_future.done() is True
    assert api_custody._unknown is True
    assert api_custody.settled is False
    assert coordinator.status()["active_owner_classes"] == ["I14_api_detached_cron"]


@pytest.mark.asyncio
async def test_managed_api_cancellation_after_claim_reuses_receipt_and_detaches(
    monkeypatch, tmp_path,
):
    """A cancellation racing a completed claim reuses its exact receipt and runs the real fire."""
    from cron import executions, jobs, scheduler
    from cron.scheduler_provider import InProcessCronScheduler
    from gateway.platforms import api_server_cron_fire as fire_api
    from tests.waiting import async_wait_until

    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    scripts_dir = home / "scripts"
    (home / "cron").mkdir(parents=True)
    scripts_dir.mkdir(parents=True)
    db = home / "cron" / "executions.db"
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: home)
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", db)
    coordinator = RuntimeCustodyCoordinator(
        process_instance="api-cancel-after-claim", authority_epoch=18, fence=RetirementFence())
    runner = SimpleNamespace(
        _draining=False,
        _external_drain_active=False,
        _running=True,
        adapters={},
        runtime_custody=coordinator,
        session_runtime_descriptor={
            "instance_id": "api-cancel-after-claim", "authority_epoch": 18, "state": "ready"},
        config=SimpleNamespace(
            multiplex_profiles=False,
            _runtime_profile_homes=[("default", home)],
        ),
        _primary_profile_name="default",
    )
    script = scripts_dir / "cron-cancel-race.py"
    script.write_text("print('managed cron race', flush=True)\n", encoding="utf-8")
    with jobs.use_cron_store(home):
        job = jobs.create_job(
            prompt="",
            schedule="every 1h",
            name="managed cancellation race",
            script=str(script),
            no_agent=True,
            deliver="local",
        )
    provider = InProcessCronScheduler()
    claimed_bindings = []
    real_claim_fire = provider.claim_fire

    def _claim_fire(job_id, **kwargs):
        claimed = real_claim_fire(job_id, **kwargs)
        if claimed is not None:
            claimed_bindings.append(claimed["_cron_custody"])
        return claimed

    monkeypatch.setattr(provider, "claim_fire", _claim_fire)
    async def _ready(*_args, **_kwargs):
        return None, {}

    monkeypatch.setattr(
        "gateway.platforms.api_server_fire_startup.live_adapters_once_started", _ready)
    with patch("gateway.run._gateway_runner_ref", lambda: runner):
        adapter = _new_cron_fire_adapter()
        api_custody, refusal = fire_api._reserve_api_custody(provider, job["id"], True)
        assert refusal is None
        reservation = {"active": True, "detached": False}
        outer = {}
        real_start = fire_api._start_split_claim

        def _start_and_cancel_after_claim(*args, **kwargs):
            claim_task = real_start(*args, **kwargs)
            claim_task.add_done_callback(lambda _done: outer["task"].cancel())
            return claim_task

        monkeypatch.setattr(fire_api, "_start_split_claim", _start_and_cancel_after_claim)
        with jobs.use_cron_store(home):
            outer["task"] = asyncio.create_task(fire_api._dispatch_split_fire(
                adapter, reservation, api_custody, provider, job["id"],
                SimpleNamespace(), 0.0))
            with pytest.raises(asyncio.CancelledError):
                await outer["task"]
            await async_wait_until(
                lambda: bool(claimed_bindings) and claimed_bindings[0]._future is not None,
                timeout=30, what="exact receipt after cancellation")
            binding = claimed_bindings[0]
            task = binding._future
            await asyncio.wait_for(asyncio.shield(task), timeout=30)
            await async_wait_until(
                lambda: binding.settled and api_custody.settled,
                timeout=30, what="post-cancel detached finalizers")
            assert task.done() and not task.cancelled()
            assert api_custody._claim_future is None
    assert reservation["detached"] is True
    assert adapter._pending_agent_requests == 0
    assert coordinator.status()["active_owner_classes"] == []
