"""Managed API cron admission must refuse providers without an exact custody receipt."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter, cors_middleware
from gateway.runtime_custody import RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence


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
