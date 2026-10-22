"""The preservation lease is consumed by the real native/API/FIFO entrypoints.

These tests use the same adapters and state database as the gateway admission tests.  They do not
exercise the coordinator by injecting synthetic owner states: each assertion follows the durable
admission or claim produced by an existing production entrypoint.
"""
import asyncio
import shlex
import sys
from contextlib import contextmanager
from functools import partial
from pathlib import Path

import pytest

from tests.waiting import async_wait_until


def _install_custody(authority, *source_adapters):
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.runtime_custody_profiles import ProfileScopeResolver
    from gateway.preservation_source_close import register_source_adapter
    from hermes_cli.backend_retirement import RetirementFence

    authority.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id,
        authority_epoch=authority.epoch,
        fence=RetirementFence(),
        profile_scope=(ProfileScopeResolver(authority.runner)
                       if getattr(authority.runner, 'session_authorities', None) is not None else None),
    )
    # These consumer fixtures use the canonical owner fixture, which intentionally does not
    # run the full GatewayRunner adapter bootstrap. Register the real adapter instance through
    # the production source-close seam so begin_preservation_drain observes the same source
    # ownership that a wired runner would install. The adapter itself remains the real test
    # transport; no registry state is fabricated here.
    for adapter in source_adapters:
        register_source_adapter(authority.runner, adapter, getattr(adapter, '_owner_profile', None))
    return authority.runner.runtime_custody


@contextmanager
def _reserved_profile_owner(authority, monkeypatch):
    """Keep the real profile lock incarnation through the native process evidence consumer."""
    from gateway.runtime_ownership import process_ownership
    from gateway.session_authorities import SessionAuthorities
    from gateway.session_authority import SessionAuthority

    home = Path(authority.db.db_path).resolve().parent
    monkeypatch.setenv('HERMES_HOME', str(home))
    authority = SessionAuthority(
        authority.runner, profile_id=str(home), instance_id=authority.instance_id,
        db=authority.db, epoch=authority.epoch,
    )
    monkeypatch.setattr(authority.runner, 'session_authority', authority)
    authorities = SessionAuthorities(home)
    authorities.add(home, authority)
    monkeypatch.setattr(authority.runner, 'session_authorities', authorities, raising=False)
    process_ownership.reserve([home])
    try:
        yield authority
    finally:
        coordinator = getattr(authority.runner, 'runtime_custody', None)
        # A failed native proof retains its real lock until this isolated test process exits.
        # Only a settled source graph permits the explicit fixture release.
        if coordinator is not None and not coordinator.status()['active_owner_classes']:
            if not process_ownership.release(home):
                raise RuntimeError('native consumer profile ownership was not released')


@pytest.mark.asyncio
async def test_native_entrypoint_binds_real_durable_admission(tmp_path, monkeypatch):
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.event import MessageEvent
    from gateway.preservation_source_close import source_handoff_context
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from gateway.session_ingress_context import native_callback
    from hermes_constants import get_hermes_home
    from hermes_state_runtime import list_session_admissions

    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter

    monkeypatch.setenv('WHATSAPP_ALLOWED_USERS', '15551234567')
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / 'sessions'))
    adapter = WhatsAppAdapter(PlatformConfig(enabled=True, extra={'session_path': str(tmp_path / 'wa')}))
    adapter.gateway_runner, runner.adapters = runner, {Platform.WHATSAPP: adapter}
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='native-r2')
    _install_custody(authority)
    runner._wire_adapter_handlers(adapter)
    monkeypatch.setattr(authority, '_schedule', lambda _ref: None)
    event = MessageEvent(
        text='native preservation',
        source=adapter.build_source(
            chat_id='15551234567@s.whatsapp.net',
            user_id='15551234567@s.whatsapp.net',
            chat_type='dm',
        ),
        message_id='native-preservation-r2',
    )
    try:
        owner = runner._transport_owner(event.source)
        assert owner is not None and owner[0] is adapter
        source_adapter, source_profile = owner
        source_close = runner._preservation_source_close
        token = source_close.admit_source(
            source_adapter, event, profile=source_profile, event=event,
            phase='native-fixture',
        )
        completed = False
        try:
            with source_handoff_context((token,)):
                with native_callback(
                    runner, event, get_hermes_home(), source_profile, source_token=token,
                ):
                    receipt = await authority.admit_native(event)
                    source_close.mark_producer_done(token)
                    completed = True
        except BaseException:  # health: allow BLE001 -- cancellation retains the real source lease
            if not completed:
                source_close.mark_unknown(token, 'native_fixture_admission_failed')
            raise
        rows = list_session_admissions(authority.db, session_id=receipt.ref.session_id)
        assert len(rows) == 1 and rows[0]['status'] == 'queued'
        handle = authority._preservation_handles[rows[0]['admission_id']]
        assert handle.owner_class == 'I01_native_inbound'
    finally:
        authority.db.close()


def test_api_entrypoint_binds_real_session_turn_row(api, owner):
    from gateway.session_api_turn import admit_api_turn

    coordinator = _install_custody(owner)
    _, ref, row = admit_api_turn(
        api,
        session_id='api-preservation-r2',
        request_id='api-preservation-r2',
        user_message='hello',
        conversation_history=[],
    )
    assert ref.session_id == 'api-preservation-r2'
    assert row['status'] == 'queued'
    assert (authority_handle := owner._preservation_handles[row['admission_id']])
    assert authority_handle.owner_class == 'I02_api_session_turn'
    assert coordinator.status()['active_owner_classes'] == ['I02_api_session_turn']


@pytest.mark.asyncio
async def test_fifo_claim_activates_child_from_real_api_admission(api, owner):
    from gateway.session_api_turn import admit_api_turn

    coordinator = _install_custody(owner)
    _, ref, row = admit_api_turn(
        api,
        session_id='api-fifo-preservation-r2',
        request_id='api-fifo-preservation-r2',
        user_message='hello',
        conversation_history=[],
    )
    live = owner.sessions[ref.session_id]
    claimed, first = await owner._claim_next(ref, live)
    assert claimed['admission_id'] == row['admission_id']
    assert first['admission_id'] == row['admission_id']
    child = owner._preservation_children[row['admission_id']]
    assert child.owner_class == 'I04_fifo_turn_execution'
    assert coordinator.status()['active_owner_classes'] == [
        'I02_api_session_turn', 'I04_fifo_turn_execution']


@pytest.mark.asyncio
async def test_real_api_waiter_settles_under_close_and_aborts_with_active_custody(api, owner, monkeypatch):
    """The API observer and full claim/execute/settle path own the causal boundary.

    Only the provider result is bounded.  The test does not install a marker or call a settlement
    helper directly: ``observe_api_turn`` registers the real waiter, the FIFO claims the row, and
    the production admission settlement resolves it.  Aborting while the provider owns the parent
    and FIFO-child leases must reopen admission without releasing either lease; a later reuse of the
    old receipt must fail closed.
    """
    from gateway import run_runtime
    from gateway.session_api_turn import admit_api_turn, observe_api_turn
    from gateway.session_results import execution_result
    from hermes_state_runtime import RuntimeStoreError, get_session_admission

    coordinator = _install_custody(owner, api)
    owner.runner.session_runtime_descriptor = {
        'instance_id': owner.instance_id,
        'authority_epoch': owner.epoch,
    }
    owner.runner._external_drain_active = False
    owner.runner._preservation_drain_active = False
    owner.runner._draining = False
    monkeypatch.setattr(owner, '_schedule', lambda _ref: None)
    monkeypatch.setattr(run_runtime, '_authorities', lambda _runner: [owner])
    monkeypatch.setattr(run_runtime, '_preservation_ownership_reason',
                        lambda _runner, **_kwargs: None)
    provider_started, provider_release = asyncio.Event(), asyncio.Event()

    async def bounded_provider(_event):
        provider_started.set()
        await provider_release.wait()
        result = {
            'final_response': 'settled under reversible close',
            'messages': [], 'completed': True,
        }
        captured = execution_result.get()
        captured['terminal_result'] = result
        captured['result'] = result
        return result['final_response']

    owner.runner._handle_message = bounded_provider
    _, ref, row = admit_api_turn(
        api,
        session_id='api-preservation-causal-r4',
        request_id='api-preservation-causal-r4',
        user_message='accepted before close',
        conversation_history=[],
    )
    admission_id = row['admission_id']
    assert row['status'] == 'queued'
    observer = asyncio.create_task(observe_api_turn((owner, ref, row)))
    await async_wait_until(lambda: admission_id in owner.waiters, timeout=5,
                           what='the API observer waiter')

    params = {
        'instance_id': owner.instance_id,
        'authority_epoch': owner.epoch,
        'owner_id': 'causal-r4',
        'scope': 'process',
        'deadline_s': 30,
    }
    receipt = await run_runtime.begin_preservation_drain(owner.runner, params, 'test-peer')
    assert receipt['status'] == 'closing'
    with pytest.raises(RuntimeStoreError, match='runtime_draining'):
        admit_api_turn(
            api,
            session_id='api-preservation-fresh-during-close',
            request_id='api-preservation-fresh-during-close',
            user_message='must be refused while closing',
            conversation_history=[],
        )

    drain = asyncio.create_task(owner._drain(ref))
    await asyncio.wait_for(provider_started.wait(), 5)
    assert coordinator.status()['active_owner_classes'] == [
        'I02_api_session_turn', 'I04_fifo_turn_execution']

    reopened = await run_runtime.abort_preservation_drain(
        owner.runner, {'receipt': receipt}, 'test-peer')
    assert reopened['status'] == 'open'
    assert coordinator.status()['active_owner_classes'] == [
        'I02_api_session_turn', 'I04_fifo_turn_execution']

    provider_release.set()
    result, usage = await asyncio.wait_for(observer, 10)
    await asyncio.wait_for(drain, 10)
    assert result['final_response'] == 'settled under reversible close'
    assert usage == {}
    settled = get_session_admission(owner.db, admission_id=admission_id)
    assert settled is not None and settled['status'] == 'terminal'
    assert coordinator.status()['active_owner_classes'] == []
    assert admission_id not in owner._preservation_handles
    assert admission_id not in owner._preservation_children

    stale = await run_runtime.abort_preservation_drain(
        owner.runner, {'receipt': receipt}, 'test-peer')
    assert stale == {'status': 'refused', 'reason': 'stale_close_receipt'}


@pytest.mark.asyncio
async def test_real_native_adapter_ledger_finalization_releases_marker(tmp_path, owner, monkeypatch):
    """A native adapter owns marker adoption and releases it through the real delivery ledger.

    The provider is bounded, but admission, native waiter registration, FIFO execution, terminal
    settlement, adapter delivery, and ledger finalization all use production code.  This replaces the
    R3 synthetic ``marker_adopters``/``Event.set`` shortcut and supplies the source-backed I01
    evidence used by preservation handoff.
    """
    with _reserved_profile_owner(owner, monkeypatch) as owner:
        from gateway import delivery_ledger as dl, run_runtime
        from gateway.config import GatewayConfig, Platform, PlatformConfig
        from gateway.platforms.base import BasePlatformAdapter, SendResult
        from gateway.platforms.event import MessageEvent
        from gateway.run import GatewayRunner
        from gateway.session_contract import SessionRef
        from gateway.session_ingress import admit_message
        from gateway.session_results import execution_result
        from gateway.session import AsyncSessionStore, SessionSource
        from hermes_state_runtime import get_session_admission

        class LedgerAdapter(BasePlatformAdapter):
            def __init__(self):
                super().__init__(PlatformConfig(enabled=True, typing_indicator=False), Platform.SLACK)
                self.send_started = asyncio.Event()
                self.send_release = asyncio.Event()

            async def connect(self, *, is_reconnect=False):
                return True

            async def disconnect(self):
                return None

            async def get_chat_info(self, chat_id):
                return None

            async def send(self, chat_id, content, reply_to=None, metadata=None):
                self.send_started.set()
                await self.send_release.wait()
                return SendResult(success=True, message_id='native-r4-delivery')

        coordinator = _install_custody(owner)
        owner.runner.config = GatewayConfig(sessions_dir=tmp_path / 'sessions')
        owner.runner.session_runtime_descriptor = {
            'instance_id': owner.instance_id,
            'authority_epoch': owner.epoch,
        }
        owner.runner._external_drain_active = False
        owner.runner._preservation_drain_active = False
        owner.runner._draining = False
        adapter = LedgerAdapter()
        adapter.gateway_runner = owner.runner
        adapter._owner_profile = 'default'
        from gateway.preservation_source_close import register_source_adapter
        register_source_adapter(owner.runner, adapter, adapter._owner_profile)
        owner.runner.adapters = {Platform.SLACK: adapter}
        owner.runner._delivery_adapter_for = lambda _source: adapter
        owner.runner._adapter_for_source = lambda _source: adapter
        owner.runner._is_user_authorized_for_source = lambda _source, allow_adapter_delegation=False: True
        owner.runner._adapter_flag = lambda *_args, **_kwargs: False
        owner.runner._adapter_profile_for_source = lambda _source: None
        owner.runner._cached_agent_for = lambda _route: None
        owner.runner._is_session_running = lambda _route: False
        owner.runner.async_session_store = AsyncSessionStore(owner.runner.session_store)
        owner.runner._mark_durable_active_turn = partial(GatewayRunner._mark_durable_active_turn, owner.runner)
        owner.runner._clear_durable_active_turn = partial(GatewayRunner._clear_durable_active_turn, owner.runner)
        monkeypatch.setattr(owner, '_schedule', lambda _ref: None)
        monkeypatch.setattr(run_runtime, '_authorities', lambda _runner: [owner])
        monkeypatch.setattr(run_runtime, '_preservation_ownership_reason',
                            lambda _runner, **_kwargs: None)
        ledger_path = tmp_path / 'delivery-ledger.db'
        monkeypatch.setattr(dl, '_db_path', lambda: ledger_path)

        # I15 evidence must come from a real ProcessRegistry reader/terminal/delivery cycle.
        # The adapter ledger above only proves I01; an empty registry cannot be sealed as I15.
        monkeypatch.setenv('HERMES_HOME', str(Path(owner.db.db_path).resolve().parent))
        import tools.process_registry as process_registry_module
        from tools.process_registry import ProcessRegistry

        monkeypatch.setattr(process_registry_module, 'CHECKPOINT_PATH', tmp_path / 'processes.json')
        monkeypatch.setattr(process_registry_module, '_SYSTEMD_SCOPE_AVAILABLE', False, raising=False)
        registry = ProcessRegistry()
        registry.bind_runtime_custody(coordinator)
        monkeypatch.setattr(process_registry_module, 'process_registry', registry)
        child_code = "print('i15-preservation', flush=True)"
        registry_session = registry.spawn_local(
            f'{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}',
            cwd=str(tmp_path), task_id='i15-preservation', session_key='i15-preservation',
            notify_on_complete=True,
        )
        waited = registry.wait_for_pending_completions(
            task_id=registry_session.owner_task_id, timeout=10, poll_interval=0.1,
        )
        assert registry_session._completion_event.is_set(), waited
        delivered = registry.drain_notifications(
            session_key=registry_session.session_key, skip_poll_observed=False,
        )
        assert delivered and delivered[0][0]['session_id'] == registry_session.id
        delivery_token = delivered[0][0].get('delivery_token', '')
        assert delivery_token
        registry.acknowledge_completion_delivery(delivery_token)
        assert registry_session.custody_delivery_acked is True

        provider_started, provider_release = asyncio.Event(), asyncio.Event()

        async def bounded_provider(event):
            provider_started.set()
            await provider_release.wait()
            result = {'final_response': 'native reply', 'messages': [], 'completed': True}
            captured = execution_result.get()
            captured['terminal_result'] = result
            captured['result'] = result
            await owner.runner._mark_durable_active_turn(event, adapter._event_session_key(event))
            return result['final_response']

        owner.runner._handle_message = bounded_provider
        adapter.set_message_handler(partial(admit_message, owner))
        source = SessionSource(platform=Platform.SLACK, chat_id='r4-native-chat', chat_type='dm',
                               user_id='r4-native-user')
        event = MessageEvent(text='native preservation', source=source, message_id='native-r4')
        session_key = adapter._event_session_key(event)
        inbound = asyncio.create_task(adapter._process_message_background(event, session_key))
        await async_wait_until(lambda: bool(owner._preservation_handles), timeout=5,
                               what='the native admission lease')
        admission_id = next(iter(owner._preservation_handles))
        await async_wait_until(lambda: admission_id in owner.native_waiters, timeout=5,
                               what='the native delivery waiter')
        row = get_session_admission(owner.db, admission_id=admission_id)
        ref = SessionRef(owner.profile_id, row['target_session_id'])
        params = {
            'instance_id': owner.instance_id,
            'authority_epoch': owner.epoch,
            'owner_id': 'native-r4', 'scope': 'process', 'deadline_s': 30,
        }
        receipt = await run_runtime.begin_preservation_drain(owner.runner, params, 'test-peer')
        assert receipt['status'] == 'closing'
        drain = asyncio.create_task(owner._drain(ref))
        await asyncio.wait_for(provider_started.wait(), 5)
        # The live native waiter removes its admission-keyed registry entry in its finally block
        # immediately after the settlement waiter resolves. Capture the actual adopter before releasing
        # the provider; the adapter keeps this same event object through ledger ownership.
        adopter = owner.marker_adopters[admission_id]
        provider_release.set()
        await async_wait_until(lambda: adapter.send_started.is_set(), timeout=10,
                               what='the ledgered native send')
        with dl._connect() as conn:
            state = conn.execute(
                'SELECT state FROM delivery_obligations WHERE obligation_id IS NOT NULL'
            ).fetchone()[0]
        assert state == 'attempting'
        assert adopter._gateway_marker_released.is_set(), 'marker releases after ledger ownership'
        adapter.send_release.set()
        await asyncio.wait_for(asyncio.gather(inbound, drain), 15)
        settled = get_session_admission(owner.db, admission_id=admission_id)
        assert settled is not None and settled['status'] == 'terminal'
        with dl._connect() as conn:
            assert conn.execute(
                'SELECT state FROM delivery_obligations WHERE obligation_id IS NOT NULL'
            ).fetchone()[0] == 'delivered'
        assert coordinator.status()['active_owner_classes'] == []
        prepared = await run_runtime.prepare_preservation_handoff(
            owner.runner, {'receipt': receipt}, 'test-peer')
        assert prepared == {'status': 'refused', 'reason': 'unknown_owner', 'owner_class': 'I03_api_run_direct'}
        states = coordinator.status()['owner_states']
        assert states['I01_native_inbound'] == 'sealed'
        assert states['I02_api_session_turn'] == 'sealed'
        assert states['I04_fifo_turn_execution'] == 'sealed'
        from gateway.runtime_custody import OWNER_CLASSES
        assert set(states) == set(OWNER_CLASSES)
        # I15 is already wired by the existing process-registry integration.  Keep the assertion
        # causal and roster-based: the remaining first unclaimed owner is the I03 route, and a stale
        # aggregate count must not be updated whenever another owner is added to the graph.
        assert states['I15_process_registry_terminal'] == 'sealed'
        assert states['I03_api_run_direct'] == 'unwired'
        assert {owner for owner, state in states.items() if state == 'sealed'} == {
            'I01_native_inbound', 'I02_api_session_turn', 'I04_fifo_turn_execution',
            'I15_process_registry_terminal'}
        reopened = await run_runtime.abort_preservation_drain(
            owner.runner, {'receipt': receipt}, 'test-peer')
        assert reopened['status'] == 'open'


@pytest.mark.asyncio
async def test_webhook_producer_reserves_i01_before_payload_preparation(tmp_path, monkeypatch):
    """The receipt-only webhook producer owns I01 before its real preparation await.

    This is the causal boundary the direct native path already had: a close beginning while
    ``prepare_native`` is fetching/retaining payload media must see the exact producer root as
    accepted custody, and a later durable admission must bind that same root rather than minting a
    second lease after preparation.
    """
    import hashlib
    import hmac
    import json

    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    import gateway.platforms.webhook_ingress as ingress
    from gateway.config import PlatformConfig
    from gateway.platforms.webhook import WebhookAdapter
    from tests.gateway.fixtures.webhook_route_authority import mount_authority

    adapter = WebhookAdapter(PlatformConfig(enabled=True, extra={
        'secret': 'owned-secret', 'routes': {'fixture': {'prompt': '{text}'}}}))
    app = web.Application()
    mount_authority(app, adapter)
    app.router.add_post('/webhooks/{route_name}', adapter._handle_webhook)
    entered, release = asyncio.Event(), asyncio.Event()
    snapshots = []
    original_prepare = ingress._prepared

    async def paused_prepare(authority, event):
        snapshots.append(authority.runner.runtime_custody.status())
        entered.set()
        await release.wait()
        return await original_prepare(authority, event)

    monkeypatch.setattr(ingress, '_prepared', paused_prepare)
    async with TestClient(TestServer(app)) as client:
        authority = adapter._message_handler.__self__.session_authority
        _install_custody(authority)
        # The route fixture's observer is unrelated to this producer-boundary proof; keep the
        # production authority wrapper, which accepts the private handle transfer.
        adapter.__dict__.pop('handle_message', None)
        body = json.dumps({'text': 'webhook preservation'}).encode()
        headers = {'X-GitHub-Delivery': 'i01-webhook-1', 'X-Hub-Signature-256':
                   'sha256=' + hmac.new(b'owned-secret', body, hashlib.sha256).hexdigest()}
        task = asyncio.create_task(client.post('/webhooks/fixture', data=body, headers=headers))
        await asyncio.wait_for(entered.wait(), 5)
        assert snapshots[-1]['active_owner_classes'] == ['I01_native_inbound']
        assert snapshots[-1]['unwired_owner_classes']
        release.set()
        response = await asyncio.wait_for(task, 10)
        assert response.status == 202, await response.text()
        rows = authority.db._read_all('SELECT * FROM session_admissions')
        assert len(rows) == 1
        row = rows[0]
        assert row is not None and row['status'] == 'queued'
        assert authority._preservation_handles[row['admission_id']].owner_class == 'I01_native_inbound'


@pytest.mark.asyncio
async def test_hosted_room_binding_refuses_fresh_close_before_sql_write(api, owner, monkeypatch):
    """A hosted-room route preflight cannot bind a hidden session after a durable close."""
    import hashlib

    from gateway import run_runtime, session_api
    from gateway.hosted_room_peer import HostedMemberDispatch, PROTOCOL_VERSION
    from gateway.session_api import hosted_member_session
    from hermes_state_runtime import RuntimeStoreError

    coordinator = _install_custody(owner, api)
    owner.runner.session_runtime_descriptor = {
        'instance_id': owner.instance_id,
        'authority_epoch': owner.epoch,
    }
    owner.runner._external_drain_active = False
    owner.runner._preservation_drain_active = False
    owner.runner._draining = False
    monkeypatch.setattr(run_runtime, '_preservation_ownership_reason',
                        lambda _runner, **_kwargs: None)

    prompt = 'hosted room close race'
    identity = ['home-preservation', 'room-preservation', 'member-preservation', 'default']
    dispatch = HostedMemberDispatch.from_mapping({
        'protocol_version': PROTOCOL_VERSION,
        'room_id': identity[1],
        'home_install_id': identity[0],
        'authority_gateway_id': 'gateway-preservation',
        'authority_epoch': max(1, owner.epoch),
        'member_id': identity[2],
        'target_install_id': 'target-preservation',
        'target_profile': identity[3],
        'task_id': 'task-preservation',
        'execution_generation': 1,
        'source_event_seq': 1,
        'cancellation_scope_id': 'cancel-preservation',
        'prompt': prompt,
        'prompt_digest': hashlib.sha256(prompt.encode()).hexdigest(),
        'capability_digest': 'a' * 64,
        'execution_policy_digest': 'b' * 64,
        'trace_id': 'trace-preservation',
    })
    generation, session_id = hosted_member_session(owner.db, identity)
    preflight = {}
    close = {}

    original_hosted_member_session = session_api.hosted_member_session
    original_run_steps_off_loop = session_api.run_steps_off_loop

    def record_hosted_member_session(db, requested_identity):
        result = original_hosted_member_session(db, requested_identity)
        preflight['result'] = result
        return result

    async def close_then_bind(authority, steps, *, orphaned=None):
        assert preflight['result'] == (generation, session_id)
        receipt = await run_runtime.begin_preservation_drain(
            owner.runner,
            {
                'instance_id': owner.instance_id,
                'authority_epoch': owner.epoch,
                'owner_id': 'hosted-room-close-race',
                'scope': 'process',
                'deadline_s': 30,
            },
            'test-peer',
        )
        assert receipt['status'] == 'closing'
        close['receipt'] = receipt
        return await original_run_steps_off_loop(authority, steps, orphaned=orphaned)

    monkeypatch.setattr(session_api, 'hosted_member_session', record_hosted_member_session)
    monkeypatch.setattr(session_api, 'run_steps_off_loop', close_then_bind)
    monkeypatch.setattr(owner, '_schedule', lambda _ref: (_ for _ in ()).throw(
        AssertionError('fresh hosted-room refusal must not schedule a turn')))

    try:
        with pytest.raises(RuntimeStoreError, match='runtime_draining'):
            await api._ensure_hosted_member_session(dispatch)
        with owner.db._read_ctx() as conn:
            assert conn.execute('SELECT 1 FROM sessions WHERE id=?', (session_id,)).fetchone() is None
            assert conn.execute(
                'SELECT 1 FROM gateway_routing WHERE entry_json LIKE ?',
                (f'%{session_id}%',),
            ).fetchone() is None
            assert conn.execute(
                'SELECT 1 FROM state_meta WHERE key=?',
                ('gateway.api.binding.v1.' + session_id,),
            ).fetchone() is None
            assert conn.execute(
                'SELECT 1 FROM session_admissions WHERE target_session_id=?',
                (session_id,),
            ).fetchone() is None
        assert coordinator.status()['active_owner_classes'] == []
    finally:
        if 'receipt' in close:
            reopened = await run_runtime.abort_preservation_drain(
                owner.runner, {'receipt': close['receipt']}, 'test-peer')
            assert reopened['status'] == 'open'


def test_i03_bind_parent_uses_exact_existing_i02_handle():
    from gateway.platforms.api_server_run_custody import ApiRunCustody
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_cli.backend_retirement import RetirementFence
    from types import SimpleNamespace

    coordinator = RuntimeCustodyCoordinator(process_instance="p", authority_epoch=2, fence=RetirementFence())
    parent = coordinator.reserve_root("I02_api_session_turn", "admission", profile_id="p1", authority_epoch=2)
    runner = SimpleNamespace(
        runtime_custody=coordinator,
        session_runtime_descriptor={"instance_id": "p", "authority_epoch": 2, "served_profiles": ["p1"]},
    )
    child, refusal = ApiRunCustody.reserve(SimpleNamespace(gateway_runner=runner), "run", "p1")
    assert refusal is None
    assert child.bind_parent(parent, "I02_api_session_turn") is None
