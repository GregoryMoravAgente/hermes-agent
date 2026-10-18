"""Causal R7 controls for exact producer identity, durable Teams jobs, and Graph retry custody.

These tests use the real SessionAuthority, RuntimeCustodyCoordinator, Graph HTTP adapter, and
TeamsPipelineStore.  Barriers only stop a real worker boundary; they do not inject a coordinator
state or replace a provider callback with a fake completion.
"""

import asyncio
import threading

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
async def test_pending_roots_keep_shared_request_id_bound_to_full_identity(tmp_path):
    """A request id shared by Graph/native and API roots cannot select the other object."""
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority

    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / 'sessions'))
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='r7-identity')
    coordinator = _install_custody(authority)
    native = authority._preservation_root('I01_native_inbound', 'native:shared-request')
    api = authority._preservation_root('I02_api_session_turn', 'api:shared-request')
    authority._preservation_pending_root(
        'shared-request', native, principal_id='msgraph:route-native', session_id='native-session')
    authority._preservation_pending_root(
        'shared-request', api, principal_id='api', session_id='api-session')

    # Deliver the API callback first. The old request-id FIFO selected the native object here,
    # then swapped the two roots on the next callback; the callback object must remain authoritative.
    authority._preservation_bind_or_release({
        'admission_id': 'admission-api', 'status': 'queued',
        'principal_id': 'api', 'target_session_id': 'api-session',
        'request_id': 'shared-request',
    }, api)
    authority._preservation_bind_or_release({
        'admission_id': 'admission-native', 'status': 'queued',
        'principal_id': 'msgraph:route-native', 'target_session_id': 'native-session',
        'request_id': 'shared-request',
    }, native)

    assert authority._preservation_handles == {
        'admission-native': native,
        'admission-api': api,
    }
    assert authority._preservation_pending_roots == {}
    assert {record.handle.owner_class for record in coordinator._records.values()} == {
        'I01_native_inbound', 'I02_api_session_turn',
    }
    for handle in (native, api):
        assert coordinator.mark_terminal(handle) is None
    authority.db.close()


@pytest.mark.asyncio
async def test_durable_teams_job_blocks_i01_ready_across_runtime_rebind(tmp_path, monkeypatch):
    """A received Teams job remains an I01 owner after the pipeline is recreated."""
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from plugins.teams_pipeline import pipeline as pipeline_module
    from plugins.teams_pipeline.models import MeetingArtifact, TeamsMeetingRef, TeamsMeetingSummaryPayload
    from plugins.teams_pipeline.pipeline import TeamsMeetingPipeline
    from plugins.teams_pipeline.store import TeamsPipelineStore

    async def resolve_meeting(client, *, meeting_id=None, join_web_url=None, tenant_id=None,
                              organizer_user_id=None):
        return TeamsMeetingRef(meeting_id=str(meeting_id), tenant_id=tenant_id,
                               organizer_user_id=organizer_user_id)

    async def fetch_transcript(client, meeting_ref):
        return MeetingArtifact(artifact_type='transcript', artifact_id='r7-transcript'), 'r7 transcript'

    async def no_call_record(client, meeting_ref, *, call_record_id=None, allow_permission_errors=True):
        return None

    async def summarize(**kwargs):
        return TeamsMeetingSummaryPayload(
            meeting_ref=kwargs['resolved_meeting'], title='R7 meeting',
            transcript_text=kwargs['transcript_text'], summary='completed',
            confidence='high', confidence_notes='local causal fixture',
            source_artifacts=kwargs['artifacts'])

    monkeypatch.setattr(pipeline_module, 'resolve_meeting_reference', resolve_meeting)
    monkeypatch.setattr(pipeline_module, 'fetch_preferred_transcript_text', fetch_transcript)
    monkeypatch.setattr(pipeline_module, 'enrich_meeting_with_call_record', no_call_record)

    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / 'sessions'))
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='r7-teams')
    _install_custody(authority)
    # This test isolates the durable job gate after the existing I01 source has been consumed.
    authority._native_inbound_seen = True
    store_path = tmp_path / 'teams-store.json'
    first = TeamsMeetingPipeline(
        graph_client=object(), store=TeamsPipelineStore(store_path),
        config={'transcript_min_chars': 1}, summarize_fn=summarize)
    job = first.create_job_from_notification({
        'id': 'teams-r7-job', 'changeType': 'updated',
        'resource': 'communications/onlineMeetings/meeting-r7',
        'resourceData': {'id': 'meeting-r7'},
    })
    assert job.status == 'received'
    runner._teams_pipeline_runtime = first
    assert authority.preservation_owner_ready('I01_native_inbound') is False

    restarted = TeamsMeetingPipeline(
        graph_client=object(), store=TeamsPipelineStore(store_path),
        config={'transcript_min_chars': 1}, summarize_fn=summarize)
    runner._teams_pipeline_runtime = restarted
    assert authority.preservation_owner_ready('I01_native_inbound') is False

    completed = await restarted.run_job(job.job_id)
    assert completed.status == 'completed'
    assert authority.preservation_owner_ready('I01_native_inbound') is True
    authority.db.close()


@pytest.mark.asyncio
async def test_graph_unjoinable_scheduler_returns_retryable_http_failure():
    """A scheduler without a joinable task leaves no successful Graph receipt."""
    import aiohttp
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.msgraph_webhook import MSGraphWebhookAdapter
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority

    runner = GatewayRunner()
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='r7-scheduler')
    coordinator = _install_custody(authority)
    adapter = MSGraphWebhookAdapter(PlatformConfig(enabled=True, extra={
        'host': '127.0.0.1', 'port': 0, 'client_state': 'r7-state',
        'accepted_resources': ['users/r7/messages'],
    }))
    runner.adapters[Platform.MSGRAPH_WEBHOOK] = adapter
    runner._wire_adapter_handlers(adapter)
    assert await adapter.connect()
    port = next(iter(adapter._runner.sites))._server.sockets[0].getsockname()[1]
    url = f'http://127.0.0.1:{port}{adapter._webhook_path}'
    notification = {
        'id': 'r7-unjoinable', 'subscriptionId': 'r7-sub',
        'resource': 'users/r7/messages/1', 'clientState': 'r7-state',
    }
    calls = []

    def unjoinable_scheduler(raw, event):
        calls.append(raw['id'])
        return object()

    adapter.set_notification_scheduler(unjoinable_scheduler)
    try:
        async with aiohttp.ClientSession() as client:
            first = await client.post(url, json={'value': [notification]})
            assert first.status == 503
            second = await client.post(url, json={'value': [notification]})
            assert second.status == 503
        assert calls == ['r7-unjoinable', 'r7-unjoinable']
        assert adapter._seen_receipts == set()
        assert coordinator.status()['active_owner_classes'] == ['I01_native_inbound']
        assert {record.state for record in coordinator._records.values()} == {'unknown'}
    finally:
        await adapter.disconnect()
        authority.db.close()


@pytest.mark.asyncio
async def test_authenticated_graph_route_and_graph_admission_cancellation_share_custody(
    monkeypatch
):
    """The signed Graph route and its actual admission worker retain the exact root on cancellation."""
    import aiohttp
    import hermes_state_runtime
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.msgraph_webhook import MSGraphWebhookAdapter
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from hermes_state_runtime import list_session_admissions

    runner = GatewayRunner()
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='r7-graph-cancel')
    coordinator = _install_custody(authority)
    adapter = MSGraphWebhookAdapter(PlatformConfig(enabled=True, extra={
        'host': '127.0.0.1', 'port': 0, 'client_state': 'r7-authenticated',
        'accepted_resources': ['users/r7/messages'],
    }))
    runner.adapters[Platform.MSGRAPH_WEBHOOK] = adapter
    runner._wire_adapter_handlers(adapter)
    monkeypatch.setattr(authority, '_schedule', lambda _ref: None)
    assert await adapter.connect()
    port = next(iter(adapter._runner.sites))._server.sockets[0].getsockname()[1]
    url = f'http://127.0.0.1:{port}{adapter._webhook_path}'
    authenticated = {
        'id': 'r7-auth-route', 'subscriptionId': 'r7-sub',
        'resource': 'users/r7/messages/auth', 'clientState': 'r7-authenticated',
    }
    try:
        async with aiohttp.ClientSession() as client:
            # Exercise the real authenticated route and canonical event builder. The explicit
            # drop scheduler has no work, so this request leaves no admission behind.
            adapter.set_notification_scheduler(lambda raw, event: None)
            response = await client.post(url, json={'value': [authenticated]})
            assert response.status == 202
            adapter._seen_receipts.clear()
            adapter.set_notification_scheduler(None)

        entered, release = threading.Event(), threading.Event()
        original_write = hermes_state_runtime.admit_session_input

        def held_write(*args, **kwargs):
            entered.set()
            assert release.wait(10)
            return original_write(*args, **kwargs)

        monkeypatch.setattr(hermes_state_runtime, 'admit_session_input', held_write)
        event = adapter._build_message_event(
            {
                'id': 'r7-graph-cancel', 'subscriptionId': 'r7-sub',
                'resource': 'users/r7/messages/cancel', 'clientState': 'r7-authenticated',
            }, 'id:r7-graph-cancel')
        task = asyncio.create_task(adapter._admit_notification(event))
        assert await asyncio.to_thread(entered.wait, 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert coordinator.status()['active_owner_classes'] == ['I01_native_inbound']

        release.set()

        def all_rows():
            return [row for sid in authority.sessions for row in list_session_admissions(
                authority.db, session_id=sid, pending_only=False)]

        rows = await async_wait_until(all_rows, timeout=15, what='the late Graph admission row')
        assert len(rows) == 1 and rows[0]['status'] == 'queued'
        await async_wait_until(
            lambda: rows[0]['admission_id'] in authority._preservation_handles,
            timeout=10, what='the late Graph custody binding')
        assert coordinator.status()['active_owner_classes'] == ['I01_native_inbound']
    finally:
        if 'release' in locals():
            release.set()
        await adapter.disconnect()
        authority.db.close()
