"""R10 causal controls for redirected Teams-store ownership and the real Graph HTTP seam."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from tests.waiting import async_wait_until


def _install_custody(authority):
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_cli.backend_retirement import RetirementFence

    coordinator = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id,
        authority_epoch=authority.epoch,
        fence=RetirementFence(),
    )
    authority.runner.runtime_custody = coordinator
    return coordinator


@pytest.mark.asyncio
async def test_graph_http_typed_foreign_result_keeps_unknown_custody(tmp_path, monkeypatch):
    """The authenticated Graph route acknowledges durable acceptance but keeps a bad worker identity unknown."""
    import aiohttp
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.msgraph_webhook import MSGraphWebhookAdapter
    from gateway.run import GatewayRunner
    from gateway.session_authority import SchedulerTaskHandoff, initialize_session_authority
    from gateway.session_ingress_context import register_transport_home
    from hermes_state import SessionDB

    runner = GatewayRunner()
    authority = await initialize_session_authority(
        runner, profile_id=str(tmp_path.resolve()), instance_id="r10-http", db=SessionDB(tmp_path / "state.db")
    )
    coordinator = _install_custody(authority)
    adapter = MSGraphWebhookAdapter(PlatformConfig(enabled=True, extra={
        "host": "127.0.0.1", "port": 0, "client_state": "r10-state",
        "accepted_resources": ["users/r10/messages"],
    }))
    runner.adapters[Platform.MSGRAPH_WEBHOOK] = adapter
    runner._wire_adapter_handlers(adapter)
    register_transport_home(runner, None, tmp_path)
    assert await adapter.connect()
    task = asyncio.create_task(asyncio.sleep(0, result={
        "job_id": "foreign-job", "status": "completed",
    }))

    async def scheduler(_notification, _event):
        return SchedulerTaskHandoff(task=task, expected_job_id="owned-job")

    adapter.set_notification_scheduler(scheduler)
    notification = {
        "id": "r10-http", "subscriptionId": "r10-sub",
        "resource": "users/r10/messages/foreign", "clientState": "r10-state",
    }
    try:
        port = next(iter(adapter._runner.sites))._server.sockets[0].getsockname()[1]
        async with aiohttp.ClientSession() as client:
            response = await client.post(
                f"http://127.0.0.1:{port}{adapter._webhook_path}",
                json={"value": [notification]},
            )
            assert response.status == 202
            await response.read()
        await async_wait_until(
            lambda: any(record.state == "unknown" for record in coordinator._records.values()),
            timeout=10, what="foreign Graph worker custody to become unknown",
        )
        record = next(iter(coordinator._records.values()))
        assert record.handle.owner_class == "I01_native_inbound"
        assert record.reason == "foreign_custody"
        assert adapter._seen_receipts == {"id:r10-http"}
    finally:
        await adapter.disconnect()
        authority.db.close()


@pytest.mark.asyncio
async def test_profile_bound_store_recovery_and_foreign_profile_refusal(tmp_path):
    """Real bind recovery stamps the active profile and a successor profile cannot claim the same override."""
    import json
    from gateway.config import GatewayConfig, Platform
    from gateway.run import GatewayRunner
    from gateway.runtime_ownership import process_ownership
    from gateway.session_authority import initialize_session_authority
    from plugins.teams_pipeline import runtime as runtime_module
    from plugins.teams_pipeline.pipeline import TeamsMeetingPipeline
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner, TeamsStoreOwnershipError
    from hermes_state import SessionDB

    shared_store = tmp_path / "shared" / "teams_pipeline_store.json"
    profile_home = tmp_path / "profile-a"
    runner = GatewayRunner(GatewayConfig(platforms={}))
    authority = await initialize_session_authority(
        runner, profile_id=str(profile_home), instance_id="r10-recovery", db=SessionDB(profile_home / "state.db")
    )
    _install_custody(authority)
    owner = TeamsStoreOwner.for_authority(authority, owner_id="teams:r10-recovery")
    store = TeamsPipelineStore(shared_store, owner=owner, lock=True)
    job_id = "r10-recoverable"
    store.upsert_job(job_id, {
        "event_id": job_id, "source_event_type": "graph.notification",
        "dedupe_key": f"id:{job_id}", "status": "received",
    })
    pipeline = TeamsMeetingPipeline(graph_client=object(), store=store, config={})

    async def finish(job_key):
        store.upsert_job(job_key, {"status": "completed"})
        return SimpleNamespace(job_id=job_key, status="completed")

    pipeline.run_job = finish
    class Adapter:
        def __init__(self):
            self._background_tasks = set()
            self.scheduler = None

        def set_notification_scheduler(self, scheduler):
            self.scheduler = scheduler

    adapter = Adapter()
    runner.adapters[Platform.MSGRAPH_WEBHOOK] = adapter
    runner.session_runtime_descriptor = {
        "instance_id": authority.instance_id, "authority_epoch": authority.epoch,
    }
    runner._teams_pipeline_runtime = pipeline
    runner._teams_pipeline_runtime_error = None
    owned_home = profile_home.resolve()
    process_ownership.reserve([owned_home])
    try:
        assert runtime_module.bind_gateway_runtime(runner) is True
        await async_wait_until(
            lambda: store.get_job(job_id).get("status") == "completed",
            timeout=10, what="the real bound Teams recovery task",
        )
        await async_wait_until(
            lambda: authority._preservation_scheduler_handles == {},
            timeout=10, what="the recovered Teams custody release",
        )
        claimed = store.get_job(job_id)
        assert claimed["runtime_profile_id"] == owner.profile_id
        assert claimed["runtime_authority_epoch"] == str(authority.epoch)
    finally:
        process_ownership.release(owned_home)
        store.close()
        authority.db.close()

    state = json.loads(shared_store.read_text(encoding="utf-8-sig"))
    assert set(state) == {"subscriptions", "notification_receipts", "event_timestamps", "jobs", "sink_records"}
    successor_owner = TeamsStoreOwner(
        profile_id=owner.profile_id, authority_epoch="r10-next-epoch",
        instance_id="r10-next-instance", owner_id="teams:r10-next",
    )
    successor = TeamsPipelineStore(shared_store, owner=successor_owner, lock=True)
    successor.close()
    foreign_owner = TeamsStoreOwner(
        profile_id=str(tmp_path / "profile-b"), authority_epoch="foreign-epoch",
        instance_id="foreign-instance", owner_id="teams:foreign",
    )
    with pytest.raises(TeamsStoreOwnershipError, match="profile_mismatch"):
        TeamsPipelineStore(shared_store, owner=foreign_owner, lock=True)
