"""Causal I03/I05/I06/I11 ownership checks over real gateway run/control and finalizer seams.

The provider/transport work is bounded; custody assertions follow the production entrypoints and
durable retry/finalizer paths.  The proposal does not seal any other owner class.
"""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.session import SessionSource
from gateway.session_contract import SessionRef


def _install_custody(authority):
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_cli.backend_retirement import RetirementFence

    authority.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id, authority_epoch=authority.epoch, fence=RetirementFence())
    return authority.runner.runtime_custody


def _connection(authority):
    from gateway.session_controls import AuthorityConnection

    capabilities = {'session:read', 'session:submit', 'session:control', 'session:approve', 'session:respond'}
    return AuthorityConnection(
        authority, SimpleNamespace(write=lambda _frame: None), {
            'user_id': 'fixture-owner', 'profile_id': authority.profile_id,
            'instance_id': authority.instance_id, 'capabilities': capabilities})


def _bind_i04_parent(authority, session_id, *, principal_id, request_id, payload):
    """Install the same durable admission/parent identity that a FIFO claim consumes."""
    from hermes_state_runtime import admit_session_input

    row = admit_session_input(
        authority.db, epoch=authority.epoch, principal_id=principal_id, session_id=session_id,
        request_id=request_id, payload=payload)
    parent = authority._preservation_root('I04_fifo_turn_execution', f'test-parent:{row["admission_id"]}')
    authority._preservation_handles[row['admission_id']] = parent
    authority.sessions[session_id].event_stream.execution = {
        'authority_epoch': authority.epoch,
        'admission_id': row['admission_id'],
        'execution_generation': row['generation'],
    }
    return row, parent


def _assert_i11_child_live(authority, coordinator):
    assert authority._preservation_operations
    active = set(coordinator.status()['active_owner_classes'])
    assert {'I04_fifo_turn_execution', 'I11_session_mutations_finalizers'} <= active


@pytest.mark.asyncio
async def test_i03_direct_http_run_keeps_task_custody_until_terminal_finalizer(api, owner):
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    coordinator = _install_custody(owner)
    started, release = asyncio.Event(), asyncio.Event()

    async def handle(event):
        from gateway.session_results import execution_result
        started.set()
        await release.wait()
        execution_result.get()['result'] = {'final_response': 'direct answer', 'messages': []}
        return 'direct answer'

    owner.runner._handle_message = handle
    app = web.Application()
    app.router.add_post('/v1/runs', api._handle_runs)
    task = None
    async with TestClient(TestServer(app)) as client:
        try:
            response = await client.post('/v1/runs', json={'input': 'hello', 'session_id': 'direct-http'})
            assert response.status == 202
            run_id = (await response.json())['run_id']
            await asyncio.wait_for(started.wait(), 5)
            task = api._active_run_tasks[run_id]
            # Assert the real live HTTP run is owned before inspecting additive private bookkeeping.
            assert 'I03_api_run_direct' in set(coordinator.status()['active_owner_classes'])
            assert run_id in api._run_preservation_handles
        finally:
            release.set()
            if task is not None:
                await asyncio.wait_for(task, 10)
        assert api._run_statuses[run_id]['status'] == 'completed'
        assert run_id not in api._run_preservation_handles
        assert 'I03_api_run_direct' not in set(coordinator.status()['active_owner_classes'])


@pytest.mark.asyncio
async def test_i05_http_clarify_during_drain_and_stale_generation_keep_exact_custody(
    api, owner, monkeypatch
):
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from types import SimpleNamespace

    from gateway.platforms import api_server as api_server_module
    from gateway.platforms.api_server_authority_runs import run_projection
    from gateway.platforms.api_server_runs import _RunLaunch, _run_custody_start, _execute_run
    from gateway.session_api_turn import admit_api_turn
    from tests.waiting import async_wait_until
    from tools import clarify_gateway

    coordinator = _install_custody(owner)
    admitted = admit_api_turn(
        api, session_id='control-http', request_id='run-control', user_message='hello',
        conversation_history=[])
    _, ref, _row = admitted
    queue = asyncio.Queue()
    api._run_streams['run-control'] = queue
    api._run_statuses['run-control'] = {}
    api._run_owners['run-control'] = api._run_idempotency_scope(SimpleNamespace(headers={}))
    launch = _RunLaunch(
        api, 'run-control', queue, ref.session_id, None, False, 'hello', [], False,
        agent_kwargs={}, request_profile=None, browser_control_principal=None,
        browser_control_transport_family=None, admission=admitted)
    _run_custody_start(api, launch)

    async def handle(event):
        from gateway.run_turn_runner import TurnRunner
        from gateway.session_results import execution_result
        turn = object.__new__(TurnRunner)
        turn._ctx = SimpleNamespace(
            _native_slack_task_cards=False, _voice_ack_guild=[None], progress_mode='off')
        # The API admission row is pre-claim state. The running FIFO claim stamps its exact
        # generation on the live handle; register the prompt against that claimed generation.
        claimed_generation = owner._handle(ref).execution_generation
        assert claimed_generation == owner.sessions[ref.session_id].event_stream.execution['execution_generation']
        turn._approval_owner = (owner, ref.session_id, claimed_generation)
        entry = clarify_gateway.register('clarify-http', 'api', 'Which one?', ['a', 'b'])
        owner.register_clarify(ref.session_id, claimed_generation, entry)
        await asyncio.to_thread(entry.event.wait, 10)
        execution_result.get()['result'] = {'final_response': entry.response or 'unanswered'}
        return entry.response

    owner.runner._handle_message = handle
    run = asyncio.create_task(_execute_run(api, launch, _api_server=api_server_module))
    await async_wait_until(
        lambda: (run_projection(api, 'run-control') or {}).get('pending_controls'),
        timeout=10, what='the clarify prompt projected on the run')
    prompt = run_projection(api, 'run-control')['pending_controls'][0]
    close = coordinator.begin_close(owner_id='i03-i05-test', scope='process', deadline_s=30)
    assert close.state == 'closing'
    body = {
        'request_id': prompt['prompt_id'],
        'execution_generation': prompt['execution_generation'],
        'answer': 'b',
    }
    app = web.Application()
    app.router.add_post('/v1/runs/{run_id}/clarify', api._handle_run_clarify)
    async with TestClient(TestServer(app)) as client:
        answered = await client.post('/v1/runs/run-control/clarify', json=body)
        assert answered.status == 200, await answered.text()
        assert (await answered.json())['status'] == 'resolved'
        active = set(coordinator.status()['active_owner_classes'])
        assert {'I03_api_run_direct', 'I05_controls_and_unknown'} <= active
        stale = dict(body, execution_generation=prompt['execution_generation'] - 1)
        refused = await client.post('/v1/runs/run-control/clarify', json=stale)
        assert refused.status == 409
        assert (await refused.json())['error']['code'] == 'stale_generation'
        assert 'I05_controls_and_unknown' in set(coordinator.status()['active_owner_classes'])
    await asyncio.wait_for(run, 10)
    assert api._run_statuses['run-control']['output'] == 'b'
    assert 'I03_api_run_direct' not in set(coordinator.status()['active_owner_classes'])
    assert 'I05_controls_and_unknown' not in set(coordinator.status()['active_owner_classes'])


@pytest.mark.asyncio
async def test_i05_duplicate_http_control_keeps_first_child(api, owner, monkeypatch):
    """A duplicate HTTP control cannot replace the first live callback's lease."""
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from types import SimpleNamespace

    from gateway.platforms import api_server as api_server_module
    from gateway.platforms import api_server_authority_runs
    from gateway.platforms.api_server_authority_runs import run_projection
    from gateway.platforms.api_server_runs import _RunLaunch, _run_custody_start, _execute_run
    from gateway.session_api_turn import admit_api_turn
    from tests.waiting import async_wait_until
    from tools import clarify_gateway

    coordinator = _install_custody(owner)
    run_id = 'duplicate-control-http'
    admitted = admit_api_turn(
        api, session_id=run_id, request_id=run_id, user_message='hello', conversation_history=[])
    _, ref, _row = admitted
    queue = asyncio.Queue()
    api._run_streams[run_id] = queue
    api._run_statuses[run_id] = {}
    api._run_owners[run_id] = api._run_idempotency_scope(SimpleNamespace(headers={}))
    launch = _RunLaunch(
        api, run_id, queue, ref.session_id, None, False, 'hello', [], False,
        agent_kwargs={}, request_profile=None, browser_control_principal=None,
        browser_control_transport_family=None, admission=admitted)
    _run_custody_start(api, launch)

    async def handle(event):
        from gateway.run_turn_runner import TurnRunner
        from gateway.session_results import execution_result
        turn = object.__new__(TurnRunner)
        turn._ctx = SimpleNamespace(
            _native_slack_task_cards=False, _voice_ack_guild=[None], progress_mode='off')
        generation = owner._handle(ref).execution_generation
        turn._approval_owner = (owner, ref.session_id, generation)
        entry = clarify_gateway.register('duplicate-clarify', 'api', 'Which one?', ['a', 'b'])
        owner.register_clarify(ref.session_id, generation, entry)
        await asyncio.to_thread(entry.event.wait, 10)
        execution_result.get()['result'] = {'final_response': entry.response or 'unanswered'}
        return entry.response

    owner.runner._handle_message = handle
    run = asyncio.create_task(_execute_run(api, launch, _api_server=api_server_module))
    await async_wait_until(
        lambda: (run_projection(api, run_id) or {}).get('pending_controls'),
        timeout=10, what='the duplicate-control clarify prompt')
    prompt = run_projection(api, run_id)['pending_controls'][0]
    close = coordinator.begin_close(owner_id='i03-i05-duplicate-test', scope='process', deadline_s=30)
    assert close.state == 'closing'
    body = {
        'request_id': prompt['prompt_id'],
        'execution_generation': prompt['execution_generation'],
        'answer': 'b',
    }
    app = web.Application()
    app.router.add_post('/v1/runs/{run_id}/clarify', api._handle_run_clarify)
    first_entered, release_first = asyncio.Event(), asyncio.Event()
    original_respond = api_server_authority_runs.respond_run

    async def hold_first(*args, **kwargs):
        first_entered.set()
        await release_first.wait()
        return await original_respond(*args, **kwargs)

    monkeypatch.setattr(api_server_authority_runs, 'respond_run', hold_first)
    first = None
    try:
        async with TestClient(TestServer(app)) as client:
            first = asyncio.create_task(client.post(f'/v1/runs/{run_id}/clarify', json=body))
            await asyncio.wait_for(first_entered.wait(), 5)
            controls = api._run_preservation_handles[run_id]['controls']
            assert len(controls) == 1
            key, first_control = next(iter(controls.items()))
            duplicate = await client.post(f'/v1/runs/{run_id}/clarify', json=body)
            assert duplicate.status == 409, await duplicate.text()
            assert (await duplicate.json())['error']['code'] == 'admission_conflict'
            assert controls.get(key) is first_control
            assert coordinator.status()['active_owner_classes'].count('I05_controls_and_unknown') == 1
            release_first.set()
            answered = await asyncio.wait_for(first, 10)
            assert answered.status == 200, await answered.text()
        await asyncio.wait_for(run, 10)
    finally:
        release_first.set()
        if first is not None:
            await asyncio.gather(first, return_exceptions=True)
        await asyncio.gather(run, return_exceptions=True)
    assert api._run_statuses[run_id]['output'] == 'b'
    assert not api._run_preservation_handles.get(run_id, {}).get('controls')


@pytest.mark.asyncio
async def test_i05_control_stale_generation_and_foreign_home_release_without_lease(owner):
    from gateway import session_busy_controls
    from gateway.session_authority import LiveSession
    from hermes_state_runtime import RuntimeStoreError

    coordinator = _install_custody(owner)
    owner.db.create_session('control-session', source='test')
    connection = _connection(owner)
    owner.sessions['control-session'] = LiveSession(
        SessionSource(Platform.LOCAL, 'control-session', user_id=connection.actor.subject), 'control-route')
    ref = SessionRef(owner.profile_id, 'control-session')
    try:
        with pytest.raises(RuntimeStoreError, match='stale_generation'):
            await session_busy_controls.correct(
                connection, ref, {
                    'session_id': ref.session_id, 'text': 'bounded steer',
                    'execution_generation': 99,
                }, verb='steer')
        assert coordinator.status()['active_owner_classes'] == []

        with pytest.raises(RuntimeStoreError, match='profile_mismatch'):
            await session_busy_controls.correct(
                connection, SessionRef('foreign-home', ref.session_id), {
                    'session_id': ref.session_id, 'text': 'foreign route',
                    'execution_generation': 99,
                }, verb='steer')
        assert coordinator.status()['active_owner_classes'] == []
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_i06_busy_observer_holds_custody_until_real_waiter_delivery(owner, monkeypatch):
    import gateway.session_ingress as ingress
    from gateway.session_authority import LiveSession
    from hermes_state_runtime import admit_session_input, claim_session_input, settle_session_input

    coordinator = _install_custody(owner)
    owner.db.create_session('busy-route', source='test')
    owner.sessions['busy-route'] = LiveSession(
        SessionSource(Platform.LOCAL, 'busy-route', user_id='fixture-owner'), 'busy-route')
    principal_id = 'messaging:' + json.dumps(
        [None, Platform.TELEGRAM.value, 'busy-chat', None, 'fixture-owner'], separators=(',', ':'))

    async def admit_native(_event):
        row = admit_session_input(
            owner.db, epoch=owner.epoch, principal_id=principal_id, session_id='busy-route',
            request_id='busy-message', payload={'text': 'busy follow-up'})
        return SimpleNamespace(
            status=row['status'], admission_id=row['admission_id'],
            ref=SessionRef(owner.profile_id, 'busy-route'))

    owner.admit_native = admit_native
    delivery = AsyncMock()
    monkeypatch.setattr(ingress, 'deliver_response', delivery)
    adapter = SimpleNamespace(
        gateway_runner=owner.runner, _background_tasks=set(),
        _message_handler=lambda event: ingress.admit_message(owner, event),
        _release_turn_marker=AsyncMock(), _notify_turn_error=AsyncMock(), name='fixture')
    event = MessageEvent(
        text='busy follow-up', source=SessionSource(
            Platform.TELEGRAM, 'busy-chat', user_id='fixture-owner'), message_id='busy-message')
    receive = asyncio.create_task(ingress.dispatch_shared_busy(adapter, event, 'busy-route'))
    admission_id = None
    try:
        for _ in range(20):
            if owner.waiters:
                break
            await asyncio.sleep(0)
        await asyncio.wait_for(receive, 5)
        assert coordinator.status()['active_owner_classes'] == ['I06_busy_followup_delivery']
        admission_id = next(iter(owner.waiters))
        claimed = await asyncio.to_thread(
            claim_session_input, owner.db, epoch=owner.epoch, session_id='busy-route', head=admission_id)
        await asyncio.to_thread(
            settle_session_input, owner.db, epoch=owner.epoch, admission_id=admission_id,
            generation=claimed['generation'], outcome='completed',
            result={'final_response': 'busy answer', 'messages': []})
        owner.waiters[admission_id].set_result('busy answer')
        await asyncio.gather(*tuple(adapter._background_tasks))
        await asyncio.sleep(0)
        delivery.assert_awaited_once()
        assert coordinator.status()['active_owner_classes'] == []
    finally:
        if admission_id is not None:
            waiter = owner.waiters.get(admission_id)
            if waiter is not None and not waiter.done():
                waiter.set_result('cleanup')
        await asyncio.gather(*tuple(adapter._background_tasks), return_exceptions=True)


@pytest.mark.asyncio
async def test_i11_cancelled_mutation_reconciles_exact_retry_receipt(owner, monkeypatch):
    import threading
    import gateway.session_mutations as mutations

    from gateway.session_authority import LiveSession

    coordinator = _install_custody(owner)
    owner.db.create_session('mutation-session', source='test')
    connection = _connection(owner)
    owner.sessions['mutation-session'] = LiveSession(
        SessionSource(Platform.LOCAL, 'mutation-session', user_id=connection.actor.subject), 'mutation-route')
    ref = SessionRef(owner.profile_id, 'mutation-session')
    admission, parent = _bind_i04_parent(
        owner, ref.session_id, principal_id=connection.actor.subject, request_id='mutation-retry',
        payload={'text': 'mutation parent'})
    assert admission['principal_id'] == connection.actor.subject
    assert admission['target_session_id'] == ref.session_id
    assert owner._preservation_handles[admission['admission_id']] is parent
    entered, release, completed = threading.Event(), threading.Event(), threading.Event()
    original = mutations.mutate_runtime_session

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        try:
            return original(*args, **kwargs)
        finally:
            completed.set()

    monkeypatch.setattr(mutations, 'mutate_runtime_session', blocked)
    handle = owner._handle(ref)
    request = {
        'session_id': ref.session_id, 'request_id': 'mutation-retry',
        'expected_revision': handle.revision, 'operation': 'rename',
        'payload': {'title': 'reconciled'},
    }
    try:
        pending = asyncio.create_task(mutations.mutate_session(owner, connection.actor, ref, dict(request)))
        await asyncio.sleep(0)
        if pending.done():
            pending.result()
        assert await asyncio.to_thread(entered.wait, 5)
        _assert_i11_child_live(owner, coordinator)
        pending.cancel()
        await asyncio.gather(pending, return_exceptions=True)
        release.set()
        assert await asyncio.to_thread(completed.wait, 5)
        await asyncio.sleep(0)
        retried = await mutations.mutate_session(owner, connection.actor, ref, dict(request))
        await asyncio.sleep(0)
        assert retried['revision'] == handle.revision + 1
        assert owner._preservation_operations == {}
        owner._preservation_release_terminal(admission['admission_id'])
        assert coordinator.status()['active_owner_classes'] == []
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
        await asyncio.to_thread(completed.wait, 5)
        await connection.close()


@pytest.mark.asyncio
async def test_i11_completion_note_finalizer_releases_after_real_tracked_write(owner, monkeypatch):
    import json
    import gateway.session_runtime_workers as workers
    from gateway.session_authority import LiveSession
    from gateway.session_completion_notes import note_if_unviewed

    coordinator = _install_custody(owner)
    owner.db.create_session('completion-session', source='test')
    owner.sessions['completion-session'] = LiveSession(
        SessionSource(Platform.LOCAL, 'completion-session', user_id='fixture-owner'), 'completion-route')
    principal_id = 'automation:completion-route'
    row, _parent = _bind_i04_parent(
        owner, 'completion-session', principal_id=principal_id, request_id='completion-note',
        payload={
            'text': 'completion note',
            'local_automation_v1': {
                'identity': json.dumps(['completion', 'notice']),
                'identities': [], 'display_kind': 'internal_notification',
            },
        })
    entered = asyncio.Event()
    release = asyncio.Event()
    original = workers.tracked_write

    async def blocked(*args, **kwargs):
        entered.set()
        await release.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(workers, 'tracked_write', blocked)
    pending = asyncio.create_task(note_if_unviewed(
        owner, SessionRef(owner.profile_id, 'completion-session'), row))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        _assert_i11_child_live(owner, coordinator)
        release.set()
        result = await pending
        assert result == ''
        assert owner.pending_results[row['admission_id']]['result']['final_response'] == ''
        assert owner._preservation_operations == {}
        owner._preservation_release_terminal(row['admission_id'])
        assert coordinator.status()['active_owner_classes'] == []
    finally:
        release.set()
        await asyncio.gather(pending, return_exceptions=True)
        owner._preservation_release_terminal(row['admission_id'])


@pytest.mark.asyncio
async def test_i11_direct_authenticated_mutation_owns_a_root_while_open(owner):
    from gateway.session_authority import LiveSession
    from gateway.session_mutations import mutate_session

    coordinator = _install_custody(owner)
    owner.db.create_session('standalone-mutation', source='test')
    connection = _connection(owner)
    owner.sessions['standalone-mutation'] = LiveSession(
        SessionSource(Platform.LOCAL, 'standalone-mutation', user_id=connection.actor.subject),
        'standalone-route')
    ref = SessionRef(owner.profile_id, 'standalone-mutation')
    initial_revision = owner._handle(ref).revision
    try:
        result = await mutate_session(owner, connection.actor, ref, {
            'session_id': ref.session_id, 'request_id': 'standalone-rename',
            'expected_revision': initial_revision,
            'expected_generation': owner._handle(ref).execution_generation,
            'operation': 'rename', 'payload': {'title': 'Standalone'},
        })
        await asyncio.sleep(0)
        assert result['revision'] == initial_revision + 1
        assert owner._preservation_operations == {}
        assert coordinator.status()['active_owner_classes'] == []
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_i11_direct_mutation_refuses_a_new_root_during_close(owner):
    from gateway import session_mutations
    from gateway.session_authority import LiveSession
    from gateway.session_contract import Principal
    from hermes_state_runtime import RuntimeStoreError

    coordinator = _install_custody(owner)
    owner.db.create_session('closing-mutation', source='test')
    actor = Principal('fixture-owner', owner.profile_id,
                      frozenset({'session:read', 'session:control'}), 'socket')
    owner.sessions['closing-mutation'] = LiveSession(
        SessionSource(Platform.LOCAL, 'closing-mutation', user_id=actor.subject), 'closing-route')
    ref = SessionRef(owner.profile_id, 'closing-mutation')
    close = coordinator.begin_close(owner_id='direct-mutation-close', deadline_s=30)
    assert close.state == 'closing'
    with pytest.raises(RuntimeStoreError, match='runtime_draining'):
        await session_mutations.mutate_session(owner, actor, ref, {
            'session_id': ref.session_id, 'request_id': 'late-rename',
            'expected_revision': owner._handle(ref).revision,
            'expected_generation': owner._handle(ref).execution_generation,
            'operation': 'rename', 'payload': {'title': 'Late'},
        })
    assert owner._preservation_operations == {}
    assert coordinator.status()['active_owner_classes'] == []


@pytest.mark.asyncio
async def test_i11_completion_finalizer_without_parent_cannot_mint_a_root(owner):
    import json
    from gateway.session_completion_notes import note_if_unviewed
    from hermes_state_runtime import RuntimeStoreError

    coordinator = _install_custody(owner)
    row = {
        'admission_id': 'orphan-completion', 'principal_id': 'completion',
        'target_session_id': 'orphan-session', 'generation': 0,
        'payload': {'text': 'done', 'local_automation_v1': {
            'identity': json.dumps(['completion', 'orphan']),
        }},
    }
    with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
        await note_if_unviewed(owner, SessionRef(owner.profile_id, 'orphan-session'), row)
    assert coordinator.status()['active_owner_classes'] == []


@pytest.mark.asyncio
async def test_i11_mutation_with_stale_execution_parent_cannot_remint_a_root(owner):
    from gateway import session_mutations
    from gateway.session_authority import LiveSession
    from gateway.session_contract import Principal
    from hermes_state_runtime import RuntimeStoreError

    coordinator = _install_custody(owner)
    owner.db.create_session('stale-mutation', source='test')
    actor = Principal('fixture-owner', owner.profile_id,
                      frozenset({'session:read', 'session:control'}), 'socket')
    live = LiveSession(
        SessionSource(Platform.LOCAL, 'stale-mutation', user_id=actor.subject), 'stale-route')
    live.event_stream.execution = {'admission_id': 'missing-parent', 'execution_generation': 0}
    owner.sessions['stale-mutation'] = live
    ref = SessionRef(owner.profile_id, 'stale-mutation')
    with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
        await session_mutations.mutate_session(owner, actor, ref, {
            'session_id': ref.session_id, 'request_id': 'stale-rename',
            'expected_revision': owner._handle(ref).revision,
            'expected_generation': 0,
            'operation': 'rename', 'payload': {'title': 'Stale'},
        })
    assert coordinator.status()['active_owner_classes'] == []

