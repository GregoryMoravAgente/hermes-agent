"""R9 causal controls for typed Teams ownership and recovery admission.

The tests use the real Graph scheduling boundary, the real Teams store, and the real profile
authority.  They keep generic joinable schedulers compatible while refusing foreign durable-job
results and recovery claims without a published owner.
"""

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
async def test_graph_scheduler_rejects_foreign_completed_job_result(tmp_path, monkeypatch):
    """The actual Graph adapter retains I01 custody when a scheduler returns another job id."""
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.msgraph_webhook import MSGraphWebhookAdapter
    from gateway.run import GatewayRunner
    from gateway.session_authority import SchedulerTaskHandoff, initialize_session_authority
    from gateway.session_ingress_context import register_transport_home
    from hermes_state import SessionDB

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    runner = GatewayRunner()
    authority = await initialize_session_authority(
        runner, profile_id="default", instance_id="r9-foreign", db=SessionDB(tmp_path / "state.db")
    )
    coordinator = _install_custody(authority)
    adapter = MSGraphWebhookAdapter(PlatformConfig(enabled=True, extra={
        "host": "127.0.0.1", "port": 0, "client_state": "r9-state",
        "accepted_resources": ["users/r9/messages"],
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
        "id": "r9-foreign", "subscriptionId": "r9-sub",
        "resource": "users/r9/messages/foreign", "clientState": "r9-state",
    }
    try:
        await adapter._schedule_notification(
            notification, adapter._build_message_event(notification, "id:r9-foreign")
        )
        await async_wait_until(
            lambda: any(record.state == "unknown" for record in coordinator._records.values()),
            timeout=10, what="foreign Teams job custody to become unknown",
        )
        record = next(iter(coordinator._records.values()))
        assert record.handle.owner_class == "I01_native_inbound"
        assert record.reason == "foreign_custody"
        assert authority._preservation_scheduler_handles["id:r9-foreign"]["expected_job_id"] == "owned-job"
    finally:
        await adapter.disconnect()
        authority.db.close()


@pytest.mark.asyncio
async def test_generic_joinable_scheduler_none_result_releases_its_own_lease(tmp_path):
    """A non-Teams scheduler's successful None result is its joinable ownership boundary."""
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from hermes_state import SessionDB

    runner = GatewayRunner()
    authority = await initialize_session_authority(
        runner, profile_id="default", instance_id="r9-generic", db=SessionDB(tmp_path / "state.db")
    )
    coordinator = _install_custody(authority)
    handle = authority._preservation_root("I01_native_inbound", "generic:r9")
    authority._preservation_bind_scheduler("generic:r9", handle)
    task = asyncio.create_task(asyncio.sleep(0, result=None))
    await task
    authority._preservation_scheduler_done("generic:r9", task)
    assert authority._preservation_scheduler_handles == {}
    assert coordinator.status()["active_owner_classes"] == []
    authority.db.close()


@pytest.mark.asyncio
async def test_teams_recovery_refuses_without_authority_or_profile_ownership(tmp_path):
    """Production bind does not claim a durable Teams job without both authority and lock custody."""
    import json
    from gateway.config import GatewayConfig, Platform
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from hermes_state import SessionDB
    from plugins.teams_pipeline import runtime as runtime_module
    from plugins.teams_pipeline.store import TeamsPipelineStore

    class Adapter:
        def __init__(self):
            self._background_tasks = set()
            self.scheduler = None

        def set_notification_scheduler(self, scheduler):
            self.scheduler = scheduler

    def seeded_store(path, job_id):
        store = TeamsPipelineStore(path)
        store.upsert_job(job_id, {
            "event_id": job_id, "source_event_type": "graph.notification",
            "dedupe_key": f"id:{job_id}", "status": "received",
        })
        return store

    no_authority_store = seeded_store(tmp_path / "no-authority.json", "r9-no-authority")
    no_authority_adapter = Adapter()
    no_authority_gateway = SimpleNamespace(
        adapters={Platform.MSGRAPH_WEBHOOK: no_authority_adapter},
        config=GatewayConfig(platforms={}),
        _teams_pipeline_runtime=SimpleNamespace(store=no_authority_store),
        _teams_pipeline_runtime_error=None,
    )
    assert runtime_module.bind_gateway_runtime(no_authority_gateway) is False
    assert no_authority_store.get_job("r9-no-authority").get("runtime_claim") is None

    home = tmp_path / "unowned-profile"
    home.mkdir()
    authority_db = SessionDB(home / "state.db")
    runner = GatewayRunner(GatewayConfig(platforms={}))
    authority = await initialize_session_authority(
        runner, profile_id="default", instance_id="r9-unowned", db=authority_db
    )
    runner.session_runtime_descriptor = {
        "instance_id": authority.instance_id, "authority_epoch": authority.epoch,
    }
    store = seeded_store(home / "teams.json", "r9-unowned")
    adapter = Adapter()
    runner.adapters[Platform.MSGRAPH_WEBHOOK] = adapter
    runner._teams_pipeline_runtime = SimpleNamespace(store=store)
    runner._teams_pipeline_runtime_error = None
    try:
        assert runtime_module.bind_gateway_runtime(runner) is False
        assert store._closed is True
        persisted = json.loads(store.path.read_text(encoding="utf-8-sig"))
        assert persisted["jobs"]["r9-unowned"].get("runtime_claim") is None
    finally:
        authority.db.close()
