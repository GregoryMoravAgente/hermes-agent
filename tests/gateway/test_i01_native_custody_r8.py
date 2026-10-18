"""R8 causal controls for Graph admission, Teams recovery, and retry custody.

The Graph case uses the authenticated HTTP route and blocks the exact
``hermes_state_runtime.admit_session_input`` function imported by that route.  The Teams cases
use the real store, gateway bind, runtime custody coordinator, and joinable asyncio tasks.
"""

import asyncio
import contextlib
import threading
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
async def test_authenticated_graph_http_cancel_holds_real_admission_worker(monkeypatch):
    """A cancelled signed HTTP request cannot release a root before its shielded real write settles."""
    import aiohttp
    import hermes_state_runtime
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.msgraph_webhook import MSGraphWebhookAdapter
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from hermes_state_runtime import list_session_admissions

    runner = GatewayRunner()
    authority = await initialize_session_authority(
        runner, profile_id="default", instance_id="r8-graph-cancel", db=None
    )
    authority._schedule = lambda _ref: None
    coordinator = _install_custody(authority)
    adapter = MSGraphWebhookAdapter(PlatformConfig(enabled=True, extra={
        "host": "127.0.0.1", "port": 0, "client_state": "r8-authenticated",
        "accepted_resources": ["users/r8/messages"],
    }))
    runner.adapters[Platform.MSGRAPH_WEBHOOK] = adapter
    runner._wire_adapter_handlers(adapter)
    assert await adapter.connect()
    port = next(iter(adapter._runner.sites))._server.sockets[0].getsockname()[1]
    url = f"http://127.0.0.1:{port}{adapter._webhook_path}"
    entered, release = threading.Event(), threading.Event()
    server_handler = None
    original_schedule = adapter._schedule_notification

    async def capture_server_handler(notification, event):
        nonlocal server_handler
        server_handler = asyncio.current_task()
        return await original_schedule(notification, event)

    monkeypatch.setattr(adapter, "_schedule_notification", capture_server_handler)
    original_admit = hermes_state_runtime.admit_session_input

    def held_admit(*args, **kwargs):
        entered.set()
        assert release.wait(10), "the real admission worker did not reach its release barrier"
        return original_admit(*args, **kwargs)

    monkeypatch.setattr(hermes_state_runtime, "admit_session_input", held_admit)
    notification = {
        "id": "r8-graph-cancel", "subscriptionId": "r8-sub",
        "resource": "users/r8/messages/cancel", "clientState": "r8-authenticated",
    }

    def rows():
        return [row for sid in authority.sessions for row in list_session_admissions(
            authority.db, session_id=sid, pending_only=False
        )]

    request = None
    try:
        async with aiohttp.ClientSession() as client:
            request = asyncio.create_task(client.post(url, json={"value": [notification]}))
            assert await asyncio.to_thread(entered.wait, 10)
            assert server_handler is not None
            server_handler.cancel()
            with pytest.raises(asyncio.CancelledError):
                await server_handler
            with contextlib.suppress(asyncio.CancelledError, aiohttp.ClientError):
                await request
        assert coordinator.status()["active_owner_classes"] == ["I01_native_inbound"]
        release.set()
        admitted = await async_wait_until(rows, timeout=15, what="the late Graph admission row")
        assert len(admitted) == 1 and admitted[0]["status"] == "queued"
        assert admitted[0]["request_id"] == "id:r8-graph-cancel"
        assert admitted[0]["principal_id"].startswith("msgraph:")
        assert admitted[0]["payload"]["native_text_v1"]["provenance"]
        await async_wait_until(
            lambda: admitted[0]["admission_id"] in authority._preservation_handles,
            timeout=10,
            what="the late Graph custody binding",
        )
        assert coordinator.status()["active_owner_classes"] == ["I01_native_inbound"]
    finally:
        release.set()
        if request is not None and not request.done():
            request.cancel()
        await adapter.disconnect()
        authority.db.close()


@pytest.mark.asyncio
async def test_startup_recovery_claims_each_durable_teams_job_once(tmp_path, monkeypatch):
    """The real bind path adopts a persisted job once and releases custody after its terminal write."""
    from pathlib import Path

    from gateway.config import GatewayConfig, Platform
    from gateway.run import GatewayRunner
    from gateway.runtime_ownership import process_ownership
    from gateway.session_authority import initialize_session_authority
    from plugins.teams_pipeline import runtime as runtime_module
    from plugins.teams_pipeline import pipeline as pipeline_module
    from plugins.teams_pipeline.models import MeetingArtifact, TeamsMeetingRef, TeamsMeetingSummaryPayload
    from plugins.teams_pipeline.pipeline import TeamsMeetingPipeline
    from plugins.teams_pipeline.store import TeamsPipelineStore

    class Adapter:
        def __init__(self):
            self._background_tasks = set()
            self.scheduler = None

        def set_notification_scheduler(self, scheduler):
            self.scheduler = scheduler

    store = TeamsPipelineStore(tmp_path / "teams-store.json")
    runs = []
    started = asyncio.Event()
    release = asyncio.Event()
    async def resolve_meeting(client, *, meeting_id=None, join_web_url=None, tenant_id=None,
                              organizer_user_id=None):
        return TeamsMeetingRef(meeting_id=str(meeting_id), tenant_id=tenant_id,
                               organizer_user_id=organizer_user_id)

    async def fetch_transcript(client, meeting_ref):
        return MeetingArtifact(artifact_type="transcript", artifact_id="r8-transcript"), "r8 transcript"

    async def no_call_record(client, meeting_ref, *, call_record_id=None, allow_permission_errors=True):
        return None

    async def summarize(**kwargs):
        return TeamsMeetingSummaryPayload(
            meeting_ref=kwargs["resolved_meeting"], title="R8 meeting",
            transcript_text=kwargs["transcript_text"], summary="completed",
            confidence="high", confidence_notes="local causal fixture",
            source_artifacts=kwargs["artifacts"])

    monkeypatch.setattr(pipeline_module, "resolve_meeting_reference", resolve_meeting)
    monkeypatch.setattr(pipeline_module, "fetch_preferred_transcript_text", fetch_transcript)
    monkeypatch.setattr(pipeline_module, "enrich_meeting_with_call_record", no_call_record)
    pipeline = TeamsMeetingPipeline(
        graph_client=object(), store=store, config={"transcript_min_chars": 1}, summarize_fn=summarize)
    job = pipeline.create_job_from_notification({
        "id": "r8-event", "changeType": "updated",
        "resource": "communications/onlineMeetings/r8-meeting",
        "resourceData": {"id": "r8-meeting"},
    })
    original_resolve = pipeline._resolve_meeting

    async def blocked_resolve(*args, **kwargs):
        runs.append(args[0].job_id)
        started.set()
        await release.wait()
        return await original_resolve(*args, **kwargs)

    monkeypatch.setattr(pipeline, "_resolve_meeting", blocked_resolve)
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    authority = await initialize_session_authority(
        runner, profile_id="default", instance_id="r8-recovery"
    )
    _install_custody(authority)
    runner.session_runtime_descriptor = {"instance_id": "r8-recovery"}
    adapter = Adapter()
    runner.adapters[Platform.MSGRAPH_WEBHOOK] = adapter
    runner._teams_pipeline_runtime = None
    runner._teams_pipeline_runtime_error = None
    monkeypatch.setattr(runtime_module, "build_pipeline_runtime", lambda _gateway: pipeline)

    owned_home = Path(authority.db.db_path).resolve().parent
    process_ownership.reserve([owned_home])
    try:
        assert runtime_module.bind_gateway_runtime(runner) is True
        await async_wait_until(lambda: started.is_set(), timeout=10, what="the adopted Teams task")
        duplicate = await adapter.scheduler({
            "id": "r8-event", "changeType": "updated",
            "resource": "communications/onlineMeetings/r8-meeting",
        }, object())
        assert duplicate is None
        release.set()
        await asyncio.gather(*list(adapter._background_tasks))
        assert runs == [job.job_id]
        assert store.get_job(job.job_id)["status"] == "completed"
        await async_wait_until(
            lambda: authority._preservation_scheduler_handles == {},
            timeout=10, what="terminal Teams custody release",
        )

        # A rebind sees no recoverable state and cannot schedule the same job again.
        assert runtime_module.bind_gateway_runtime(runner) is True
        assert runs == [job.job_id]
    finally:
        release.set()
        process_ownership.release(owned_home)
        authority.db.close()


@pytest.mark.asyncio
async def test_runtime_bind_failure_refuses_authenticated_graph_work(tmp_path, monkeypatch):
    """A cold Teams runtime yields a retryable HTTP refusal and never records a receipt."""
    import aiohttp
    from gateway import run_runtime
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.msgraph_webhook import MSGraphWebhookAdapter
    from gateway.run import GatewayRunner
    from gateway.runtime_ownership import process_ownership
    from gateway.session_authority import initialize_session_authority
    from hermes_state import SessionDB
    from plugins.teams_pipeline import runtime as runtime_module

    home = tmp_path / "profile"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    runner = GatewayRunner()
    authority_db = SessionDB(home / "state.db")
    authority = await initialize_session_authority(
        runner, profile_id="default", instance_id="r8-bind-failure", db=authority_db
    )
    coordinator = _install_custody(authority)
    runner._running = True
    runner.session_runtime_descriptor = {
        "instance_id": authority.instance_id, "authority_epoch": authority.epoch,
    }
    owned_home = home.resolve()
    process_ownership.reserve([owned_home])
    adapter = MSGraphWebhookAdapter(PlatformConfig(enabled=True, extra={
        "host": "127.0.0.1", "port": 0, "client_state": "r8-bind-state",
        "accepted_resources": ["users/r8/messages"],
    }))
    runner.adapters[Platform.MSGRAPH_WEBHOOK] = adapter
    runner._wire_adapter_handlers(adapter)
    assert await adapter.connect()
    monkeypatch.setattr(
        runtime_module, "build_pipeline_runtime",
        lambda _gateway: (_ for _ in ()).throw(RuntimeError("store unavailable")),
    )
    assert runtime_module.bind_gateway_runtime(runner) is False
    port = next(iter(adapter._runner.sites))._server.sockets[0].getsockname()[1]
    notification = {
        "id": "r8-bind-failure", "subscriptionId": "r8-sub",
        "resource": "users/r8/messages/failure", "clientState": "r8-bind-state",
    }
    try:
        async with aiohttp.ClientSession() as client:
            response = await client.post(
                f"http://127.0.0.1:{port}{adapter._webhook_path}",
                json={"value": [notification]},
            )
            assert response.status == 503
            await response.read()
        assert adapter._seen_receipts == set()
        close = await run_runtime.begin_preservation_drain(
            runner, {
                "instance_id": authority.instance_id,
                "authority_epoch": authority.epoch,
                "owner_id": "r8-bind-failure", "scope": "process", "deadline_s": 30,
            }, "test-peer")
        assert close["status"] == "closing"
        pending = await run_runtime.prepare_preservation_handoff(
            runner, {"receipt": close}, "test-peer")
        assert pending == {
            "status": "refused", "reason": "unknown_owner",
            "owner_class": "I01_native_inbound",
        }
        status = coordinator.status()
        assert status["active_owner_classes"] == ["I01_native_inbound"]
        assert {record.state for record in coordinator._records.values()} == {"unknown"}
        assert status["owner_states"]["I01_native_inbound"] == "unwired"
        assert authority.preservation_owner_ready("I01_native_inbound") is False
        reopened = await run_runtime.abort_preservation_drain(
            runner, {"receipt": close}, "test-peer")
        assert reopened["status"] == "open"
    finally:
        process_ownership.release(owned_home)
        await adapter.disconnect()
        authority.db.close()


@pytest.mark.asyncio
async def test_scheduler_failed_is_terminal_but_retry_scheduled_keeps_exact_successor(
    tmp_path,
):
    """The scheduler callback releases persisted failed work but retains retry custody for adoption."""
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from plugins.teams_pipeline.store import TeamsPipelineStore

    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    authority = await initialize_session_authority(
        runner, profile_id="default", instance_id="r8-status"
    )
    coordinator = _install_custody(authority)
    store = TeamsPipelineStore(tmp_path / "teams-store.json")
    runner._teams_pipeline_runtime = SimpleNamespace(store=store)
    for status in ("failed", "retry_scheduled"):
        job_id = f"r8-{status}"
        store.upsert_job(job_id, {
            "event_id": job_id, "source_event_type": "graph.notification",
            "dedupe_key": f"id:{job_id}", "status": status,
        })
        scheduler_id = authority._preservation_adopt_scheduler_job(job_id)
        task = asyncio.create_task(asyncio.sleep(0, result=SimpleNamespace(
            job_id=job_id, status=status)))
        await task
        authority._preservation_scheduler_done(scheduler_id, task)
        if status == "failed":
            assert job_id not in authority._preservation_scheduler_retry_handles
        else:
            assert job_id in authority._preservation_scheduler_retry_handles

    retry_id = "r8-retry_scheduled"
    recovery_id = authority._preservation_adopt_scheduler_job(retry_id)
    store.upsert_job(retry_id, {"status": "completed"})
    terminal = asyncio.create_task(asyncio.sleep(0, result=SimpleNamespace(
        job_id=retry_id, status="completed")))
    await terminal
    authority._preservation_scheduler_done(recovery_id, terminal)
    assert coordinator.status()["active_owner_classes"] == []
    authority.db.close()
