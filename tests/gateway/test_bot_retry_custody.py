"""Managed Bot retry custody follows the derived admission, not only the mailbox identity.

The ordinary peer-DM retry tests exercise the API-session path and the finite executor seam.  This
file drives the real Bot Chat FIFO through ``session_ingress.execute_admission`` so the retry must
authenticate its own I07 root and I06 delivery child before the turn is allowed to run.  The
context-overflow daemon fixture remains unchanged: these focused controls isolate the custody
regression without changing its model, timing, or durable transcript assertions.
"""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest
import pytest_asyncio

from gateway.session_contract import Principal
from hermes_state_runtime import list_session_admissions

KEY = 'a' * 32


@pytest_asyncio.fixture
async def managed_bot(tmp_path, monkeypatch):
    from gateway import run
    from gateway.config import GatewayConfig
    from gateway.runtime_ownership import process_ownership
    from gateway.session import SessionStore
    from gateway.session_authorities import SessionAuthorities
    from gateway.session_authority import initialize_session_authority
    from gateway.session_local import create_local_session
    from gateway.session_local_title import title_new_session
    from hermes_state import SessionDB

    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setattr(run, '_load_gateway_config', lambda: {'platform_toolsets': {'cli': []}})
    home = tmp_path.resolve()
    process_ownership.reserve([home])
    store = SessionStore(home / 'sessions', GatewayConfig())
    store._db = SessionDB(db_path=home / 'state.db')
    outcomes = [
        {'final_response': '', 'failed': True, 'completed': False,
         'error': 'Error code: 429 - rate limit exceeded', 'messages': []},
        {'final_response': 'pong', 'completed': True, 'messages': []},
    ]
    retry_started = asyncio.Event()
    release_retry = asyncio.Event()
    control = {'block_retry': False}

    async def handle_message(_event):
        result = outcomes.pop(0)
        if result.get('final_response') == 'pong' and control['block_retry']:
            retry_started.set()
            await release_retry.wait()
        from gateway.session_results import execution_result
        execution_result.get()['result'] = dict(result)
        return result.get('final_response', '')

    runner = SimpleNamespace(
        session_store=store, _session_db=store._db, adapters={}, _draining=False,
        _evict_cached_agent=lambda route: None, _handle_message=handle_message,
    )
    runner._adapter_for_source = lambda source: runner.adapters.get(source.platform)
    authority = await initialize_session_authority(runner, profile_id=str(home), instance_id='fixture')
    authorities = SessionAuthorities(home)
    authorities.add(home, authority)
    runner.session_authorities = authorities
    from tests.gateway.conftest import bind_authority_runtime_custody
    bind_authority_runtime_custody(runner, authority)
    owner = Principal('uid:1000', str(home),
                      frozenset({'session:create', 'session:read', 'session:submit'}), 'native')
    chat = create_local_session(authority, owner, {
        'request_id': 'bot-custody', 'source': 'gui', 'cwd': str(tmp_path),
        'model': 'fixture', 'toolsets': [],
    })
    title_new_session(authority, chat, 'Bot Chat')
    state = SimpleNamespace(
        authority=authority, chat=chat.session_id, home=tmp_path, connection=SimpleNamespace(
            authority=authority, actor=owner,
        ), control=control, retry_started=retry_started, release_retry=release_retry,
    )
    try:
        yield state
    finally:
        release_retry.set()
        from tests.waiting import async_wait_until
        await async_wait_until(
            lambda: all(live.task is None or live.task.done() for live in authority.sessions.values()),
            timeout=10, what='managed Bot retry drain to stop',
        )
        store._db.close()
        assert process_ownership.release(home) is True


def _admissions(bot):
    return list_session_admissions(bot.authority.db, session_id=bot.chat, pending_only=False)


async def _wait_terminal(bot, count):
    from tests.waiting import async_wait_until
    await async_wait_until(
        lambda: len(_admissions(bot)) >= count
        and all(row['status'] == 'terminal' for row in _admissions(bot)),
        timeout=10, what=f'{count} managed Bot admissions to settle',
    )


async def _wait_custody_released(bot, admission_ids):
    from tests.waiting import async_wait_until
    await async_wait_until(
        lambda: (
            not admission_ids.intersection(bot.authority._preservation_handles)
            and not admission_ids.intersection(bot.authority._preservation_automation_children)
            and all(
                operation.get('admission_id') not in admission_ids
                for operation in bot.authority._preservation_operations.values()
            )
        ),
        timeout=10, what='managed Bot I07/I06 custody to release after terminal readback',
    )


def _prepare_bound_rehome(bot, suffix, new_session_id):
    from gateway.session_local import create_local_session
    from hermes_state_runtime import admit_session_input, rehome_queued_inputs

    live = bot.authority.sessions[bot.chat]
    target = create_local_session(bot.authority, bot.connection.actor, {
        'request_id': f'rehome-target:{new_session_id}', 'source': 'gui',
        'cwd': str(bot.home), 'model': 'fixture', 'toolsets': [],
    })
    new_session_id = target.session_id
    identity = f'bot:{KEY}:{suffix}'
    principal_id = 'automation:' + live.route
    source_identity = 'test-source'
    root = bot.authority._preservation_automation_root(
        request_id=identity, principal_id=principal_id, session_id=bot.chat,
        source_identity=source_identity, delivery_id=identity[4:],
    )
    descriptor = bot.authority._preservation_automation_descriptor(
        root, request_id=identity, principal_id=principal_id, session_id=bot.chat,
        source_identity=source_identity, delivery_id=identity[4:],
    )
    row = admit_session_input(
        bot.authority.db, epoch=bot.authority.epoch,
        principal_id=principal_id, session_id=bot.chat, request_id=identity,
        payload={'text': suffix, 'native_text_v1': {'automation': {
            'identity': identity, 'owner': bot.chat, 'route': live.route,
            'target': bot.chat, 'custody': descriptor,
        }}}, intent='queue',
    )
    bot.authority._preservation_bind_or_release(row, root)
    rehome = bot.authority._preservation_prepare_automation_rehome(
        row['admission_id'], row, new_session_id,
    )
    assert rehome_queued_inputs(
        bot.authority.db, epoch=bot.authority.epoch, old_session_id=bot.chat,
        new_session_id=new_session_id, principal_prefix=principal_id,
    ) == [row['admission_id']]
    return root, row, rehome


@pytest.mark.asyncio
async def test_managed_retry_binds_distinct_i07_i06_parent_before_classic_ingress(
    managed_bot, monkeypatch,
):
    from gateway.session_bot import deliver
    from tools.bot_live_delivery import read_delivery_result

    captured = {}
    original = managed_bot.authority._preservation_bind_automation_child

    def capture(admission_id, child_key):
        root = managed_bot.authority._preservation_handles.get(admission_id)
        operation = managed_bot.authority._preservation_operations.get(child_key)
        captured.setdefault(admission_id, (root, child_key, dict(operation or {})))
        return original(admission_id, child_key)

    monkeypatch.setattr(managed_bot.authority, '_preservation_bind_automation_child', capture)
    first = await deliver(managed_bot.connection, {'id': KEY, 'profile': 'default', 'message': 'ping'})
    await _wait_terminal(managed_bot, 2)
    rows = _admissions(managed_bot)
    admission_ids = {row['admission_id'] for row in rows}
    await _wait_custody_released(managed_bot, admission_ids)
    receipt = await asyncio.to_thread(read_delivery_result, managed_bot.home, KEY)

    assert first['admission_id']
    assert (receipt['status'], receipt['reply']) == ('settled', 'pong'), receipt
    assert [row['request_id'] for row in rows] == [f'bot:{KEY}', f'bot:{KEY}:retry']
    assert len(captured) == 2
    by_request = {
        operation['request_id']: (root, child_key, operation)
        for root, child_key, operation in captured.values()
    }
    initial = by_request[f'bot:{KEY}']
    retry = by_request[f'bot:{KEY}:retry']
    assert initial[0] is not retry[0]
    for root, child_key, operation in (initial, retry):
        assert root is not None and operation['parent_handle'] is root
        assert operation['request_id'] == operation['admission_request_id']
        assert operation['owner_class'] == 'I06_busy_followup_delivery'
        assert child_key


@pytest.mark.asyncio
@pytest.mark.parametrize('corruption', ('missing_child', 'copied_child', 'stale_parent'))
async def test_managed_retry_rejects_missing_or_foreign_child_witness(
    managed_bot, monkeypatch, corruption,
):
    from gateway.session_bot import deliver
    from gateway.session_managed_worker import ManagedExecutionUnknown
    from gateway.session_bot_custody import bot_execution_binding_for_admission

    managed_bot.control['block_retry'] = True
    captured = {}
    original = managed_bot.authority._preservation_bind_automation_child

    def capture(admission_id, child_key):
        root = managed_bot.authority._preservation_handles.get(admission_id)
        captured.setdefault(admission_id, (root, child_key))
        return original(admission_id, child_key)

    monkeypatch.setattr(managed_bot.authority, '_preservation_bind_automation_child', capture)
    await deliver(managed_bot.connection, {'id': KEY, 'profile': 'default', 'message': 'ping'})
    await managed_bot.retry_started.wait()
    retry = next(row for row in _admissions(managed_bot) if row['request_id'].endswith(':retry'))
    root, child_key = captured[retry['admission_id']]
    operation = managed_bot.authority._preservation_operations[child_key]
    if corruption == 'missing_child':
        managed_bot.authority._preservation_automation_children.pop(retry['admission_id'])
    elif corruption == 'copied_child':
        copied_handle = replace(operation['handle'])
        managed_bot.authority._preservation_operations[child_key] = {
            **operation, 'handle': copied_handle,
        }
    else:
        managed_bot.authority._preservation_operations[child_key] = {
            **operation, 'parent_handle': object(),
        }
    with pytest.raises(ManagedExecutionUnknown, match='bot_owner_unknown'):
        bot_execution_binding_for_admission(managed_bot.authority, retry)
    assert managed_bot.authority._preservation_operations[child_key].get('unknown') is True
    assert root is not None
    managed_bot.release_retry.set()


@pytest.mark.asyncio
async def test_fresh_automation_root_seals_initial_pending_tuple(managed_bot):
    live = managed_bot.authority.sessions[managed_bot.chat]
    identity = f'bot:{KEY}:fresh-root'
    principal_id = 'automation:' + live.route
    root = managed_bot.authority._preservation_automation_root(
        request_id=identity,
        principal_id=principal_id,
        session_id=managed_bot.chat,
        source_identity='test-source',
        delivery_id=identity[4:],
    )

    assert managed_bot.authority._preservation_pending_roots[id(root)] == {
        'handle': root,
        'owner_class': root.owner_class,
        'principal_id': principal_id,
        'session_id': managed_bot.chat,
        'request_id': identity,
        'source_identity': 'test-source',
        'delivery_id': identity[4:],
    }
    assert managed_bot.authority._preservation_automation_roots[identity[4:]] is root
    assert managed_bot.authority._preservation_release_automation_unaccepted(None, root) is True


@pytest.mark.asyncio
async def test_retry_pre_admission_duplicate_reuses_exact_pair(managed_bot):
    from gateway.session_bot_custody import prepare_bot_retry_custody

    live = managed_bot.authority.sessions[managed_bot.chat]
    identity = f'bot:{KEY}:retry'
    principal_id = 'automation:' + live.route
    first = prepare_bot_retry_custody(
        managed_bot.authority, identity=identity, principal_id=principal_id,
        session_id=managed_bot.chat, source_identity='test-source',
    )
    second = prepare_bot_retry_custody(
        managed_bot.authority, identity=identity, principal_id=principal_id,
        session_id=managed_bot.chat, source_identity='test-source',
    )
    managed_bot.authority._preservation_pending_root(
        identity, first[0], principal_id=principal_id, session_id=managed_bot.chat,
        source_identity='test-source',
    )

    assert second == first
    assert (
        managed_bot.authority._preservation_pending_roots[id(first[0])]['delivery_id']
        == identity[4:]
    )
    assert [
        key for key, operation in managed_bot.authority._preservation_operations.items()
        if operation.get('request_id') == identity
    ] == [first[1]]
    assert managed_bot.authority._preservation_release_automation_unaccepted(
        first[1], first[0],
    ) is True


@pytest.mark.asyncio
@pytest.mark.parametrize('field', ('principal_id', 'source_identity'))
async def test_retry_root_reuse_refuses_changed_retained_identity(managed_bot, field):
    from gateway.session_bot_custody import prepare_bot_retry_custody
    from hermes_state_runtime import RuntimeStoreError

    live = managed_bot.authority.sessions[managed_bot.chat]
    identity = f'bot:{KEY}:retry'
    principal_id = 'automation:' + live.route
    first = prepare_bot_retry_custody(
        managed_bot.authority, identity=identity, principal_id=principal_id,
        session_id=managed_bot.chat, source_identity='test-source',
    )
    roots_before = dict(managed_bot.authority._preservation_automation_roots)
    operations_before = dict(managed_bot.authority._preservation_operations)
    changed = principal_id if field == 'source_identity' else 'automation:other-route'
    kwargs = {
        'identity': identity, 'principal_id': principal_id,
        'session_id': managed_bot.chat, 'source_identity': 'test-source',
    }
    kwargs[field] = changed
    with pytest.raises(RuntimeStoreError, match='preservation_owner_mismatch'):
        prepare_bot_retry_custody(managed_bot.authority, **kwargs)
    assert managed_bot.authority._preservation_automation_roots == roots_before
    assert managed_bot.authority._preservation_operations == operations_before
    assert managed_bot.authority._preservation_handles == {}
    assert first[0] is roots_before[identity[4:]]


@pytest.mark.asyncio
async def test_retry_pending_rejects_copied_root_before_child_allocation(managed_bot):
    from dataclasses import replace

    from hermes_state_runtime import RuntimeStoreError

    live = managed_bot.authority.sessions[managed_bot.chat]
    identity = f'bot:{KEY}:copied'
    principal_id = 'automation:' + live.route
    root = managed_bot.authority._preservation_automation_root(
        request_id=identity, principal_id=principal_id, session_id=managed_bot.chat,
        source_identity='test-source', delivery_id=identity[4:],
    )
    copied = replace(root)
    with pytest.raises(RuntimeStoreError, match='preservation_owner_mismatch'):
        managed_bot.authority._preservation_pending_root(
            identity, copied, principal_id=principal_id, session_id=managed_bot.chat,
            source_identity='test-source', delivery_id=identity[4:],
        )
    assert managed_bot.authority._preservation_automation_roots[identity[4:]] is root
    assert managed_bot.authority._preservation_pending_roots[id(root)]['handle'] is root
    assert not managed_bot.authority._preservation_operations


@pytest.mark.asyncio
async def test_retry_accepted_root_reuse_refuses_terminal_race_identity_change(
        managed_bot,
):
    from gateway.session_bot import deliver
    from gateway.session_bot_custody import prepare_bot_retry_custody
    from hermes_state_runtime import RuntimeStoreError

    managed_bot.control['block_retry'] = True
    await deliver(managed_bot.connection, {'id': KEY, 'profile': 'default', 'message': 'ping'})
    await managed_bot.retry_started.wait()
    retry = next(row for row in _admissions(managed_bot) if row['request_id'].endswith(':retry'))
    root = managed_bot.authority._preservation_handles[retry['admission_id']]
    roots_before = dict(managed_bot.authority._preservation_automation_roots)
    operations_before = dict(managed_bot.authority._preservation_operations)
    with pytest.raises(RuntimeStoreError, match='preservation_owner_mismatch'):
        prepare_bot_retry_custody(
            managed_bot.authority, identity=retry['request_id'],
            principal_id='automation:foreign-principal', session_id=managed_bot.chat,
            source_identity='foreign-source',
        )
    assert managed_bot.authority._preservation_automation_roots == roots_before
    assert managed_bot.authority._preservation_operations == operations_before
    assert managed_bot.authority._preservation_handles[retry['admission_id']] is root
    managed_bot.release_retry.set()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('field', 'changed'),
    (('principal_id', 'automation:foreign-route'), ('source_identity', 'foreign-source')),
)
async def test_bound_root_pending_overwrite_refuses_changed_tuple(
        managed_bot, field, changed,
):
    """A bound opaque root cannot be retagged after its pending tuple was consumed."""
    from gateway.session_bot_custody import prepare_bot_retry_custody
    from hermes_state_runtime import RuntimeStoreError, admit_session_input

    live = managed_bot.authority.sessions[managed_bot.chat]
    identity = f'bot:{KEY}:bound'
    principal_id = 'automation:' + live.route
    source_identity = 'test-source'
    root, _child = prepare_bot_retry_custody(
        managed_bot.authority, identity=identity, principal_id=principal_id,
        session_id=managed_bot.chat, source_identity=source_identity,
    )
    descriptor = managed_bot.authority._preservation_automation_descriptor(
        root, request_id=identity, principal_id=principal_id, session_id=managed_bot.chat,
        source_identity=source_identity, delivery_id=identity[4:],
    )
    row = admit_session_input(
        managed_bot.authority.db, epoch=managed_bot.authority.epoch,
        principal_id=principal_id, session_id=managed_bot.chat, request_id=identity,
        payload={'text': 'bound', 'local_automation_v1': {
            'identity': identity, 'owner': managed_bot.chat, 'route': live.route,
            'target': managed_bot.chat, 'custody': descriptor,
        }}, intent='queue',
    )
    managed_bot.authority._preservation_bind_or_release(row, root)
    assert id(root) not in managed_bot.authority._preservation_pending_roots
    binding_before = dict(
        managed_bot.authority._preservation_automation_bindings[row['admission_id']]
    )
    roots_before = dict(managed_bot.authority._preservation_automation_roots)
    kwargs = {
        'request_id': identity, 'principal_id': principal_id,
        'session_id': managed_bot.chat, 'source_identity': source_identity,
        'delivery_id': identity[4:],
    }
    kwargs[field] = changed
    with pytest.raises(RuntimeStoreError, match='preservation_owner_mismatch'):
        managed_bot.authority._preservation_pending_root(handle=root, **kwargs)
    assert id(root) not in managed_bot.authority._preservation_pending_roots
    assert (
        managed_bot.authority._preservation_automation_bindings[row['admission_id']]
        == binding_before
    )
    assert managed_bot.authority._preservation_automation_roots == roots_before


@pytest.mark.asyncio
async def test_retry_replaced_root_record_refuses_without_minting_successor(
        managed_bot, monkeypatch,
):
    from dataclasses import replace

    from gateway.session_bot_custody import prepare_bot_retry_custody
    from hermes_state_runtime import RuntimeStoreError

    live = managed_bot.authority.sessions[managed_bot.chat]
    identity = f'bot:{KEY}:replaced-root'
    principal_id = 'automation:' + live.route
    root, _child = prepare_bot_retry_custody(
        managed_bot.authority, identity=identity, principal_id=principal_id,
        session_id=managed_bot.chat, source_identity='test-source',
    )
    coordinator = managed_bot.authority._preservation_coordinator()
    lease_id = root.lease.lease_id
    original_record = coordinator._records[lease_id]
    monkeypatch.setitem(
        coordinator._records, lease_id, replace(original_record, handle=replace(root))
    )
    roots_before = dict(managed_bot.authority._preservation_automation_roots)
    operations_before = dict(managed_bot.authority._preservation_operations)
    with pytest.raises(RuntimeStoreError, match='preservation_custody_unknown'):
        prepare_bot_retry_custody(
            managed_bot.authority, identity=identity, principal_id=principal_id,
            session_id=managed_bot.chat, source_identity='test-source',
        )
    assert managed_bot.authority._preservation_automation_roots == roots_before
    assert managed_bot.authority._preservation_automation_roots[identity[4:]] is root
    assert managed_bot.authority._preservation_operations == operations_before


@pytest.mark.asyncio
async def test_bound_child_release_refuses_after_terminal_row_race(managed_bot):
    from gateway.session_bot_custody import prepare_bot_retry_custody
    from hermes_state_runtime import admit_session_input, cancel_session_input

    live = managed_bot.authority.sessions[managed_bot.chat]
    identity = f'bot:{KEY}:bound-cancel'
    principal_id = 'automation:' + live.route
    source_identity = 'test-source'
    root, child = prepare_bot_retry_custody(
        managed_bot.authority, identity=identity, principal_id=principal_id,
        session_id=managed_bot.chat, source_identity=source_identity,
    )
    descriptor = managed_bot.authority._preservation_automation_descriptor(
        root, request_id=identity, principal_id=principal_id, session_id=managed_bot.chat,
        source_identity=source_identity, delivery_id=identity[4:],
    )
    row = admit_session_input(
        managed_bot.authority.db, epoch=managed_bot.authority.epoch,
        principal_id=principal_id, session_id=managed_bot.chat, request_id=identity,
        payload={'text': 'bound-cancel', 'local_automation_v1': {
            'identity': identity, 'owner': managed_bot.chat, 'route': live.route,
            'target': managed_bot.chat, 'custody': descriptor,
        }}, intent='queue',
    )
    managed_bot.authority._preservation_bind_or_release(row, root)
    assert managed_bot.authority._preservation_operation_bind_admission(
        child, row['admission_id'], expected_parent=root,
    ) is True
    managed_bot.authority._preservation_bind_automation_child(row['admission_id'], child)
    cancelled = cancel_session_input(
        managed_bot.authority.db, epoch=managed_bot.authority.epoch,
        admission_id=row['admission_id'],
    )
    assert cancelled['outcome'] == 'cancelled'

    assert managed_bot.authority._preservation_operation_release_unaccepted(child) is False
    operation = managed_bot.authority._preservation_operations[child]
    assert operation['admission_id'] == row['admission_id']
    assert operation['unknown'] is True
    assert managed_bot.authority._preservation_coordinator()._records[
        operation['handle'].lease.lease_id
    ].state == 'unknown'


@pytest.mark.asyncio
async def test_rehome_installs_pending_tuple_before_cleanup(managed_bot):
    from hermes_state_runtime import get_session_admission

    root, row, rehome = _prepare_bound_rehome(
        managed_bot, 'rehome-positive', f'{managed_bot.chat}:successor',
    )
    admission_id = row['admission_id']
    canonical = get_session_admission(managed_bot.authority.db, admission_id=admission_id)
    assert canonical['target_session_id'] == rehome['new_session_id']

    assert managed_bot.authority._preservation_finish_automation_rehome(admission_id) is root
    pending = managed_bot.authority._preservation_pending_roots[id(root)]
    assert pending == {
        'handle': root,
        'owner_class': root.owner_class,
        'principal_id': rehome['principal_id'],
        'session_id': rehome['new_session_id'],
        'request_id': rehome['request_id'],
        'source_identity': rehome['source_identity'],
        'delivery_id': rehome['delivery_id'],
    }
    assert managed_bot.authority._preservation_automation_rehomes.get(admission_id) is None
    assert managed_bot.authority._preservation_handles.get(admission_id) is None
    assert managed_bot.authority._preservation_automation_bindings.get(admission_id) is None
    assert managed_bot.authority._preservation_automation_roots[rehome['delivery_id']] is root


@pytest.mark.asyncio
async def test_rehome_validation_failure_retains_exact_indexes_as_unknown(managed_bot):
    from hermes_state_runtime import RuntimeStoreError

    root, row, rehome = _prepare_bound_rehome(
        managed_bot, 'rehome-failure', f'{managed_bot.chat}:failure',
    )
    admission_id = row['admission_id']
    coordinator = managed_bot.authority._preservation_coordinator()
    lease_id = root.lease.lease_id
    handles_before = dict(managed_bot.authority._preservation_handles)
    bindings_before = dict(managed_bot.authority._preservation_automation_bindings)

    rehome['source_identity'] = ''
    with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
        managed_bot.authority._preservation_finish_automation_rehome(admission_id)

    assert managed_bot.authority._preservation_automation_rehomes[admission_id] is rehome
    assert managed_bot.authority._preservation_handles == handles_before
    assert managed_bot.authority._preservation_automation_bindings == bindings_before
    assert id(root) not in managed_bot.authority._preservation_pending_roots
    assert coordinator._records[lease_id].handle is root
    assert coordinator._records[lease_id].state == 'unknown'


@pytest.mark.asyncio
@pytest.mark.parametrize('field,value', [
    ('principal_id', 'automation:other-route'),
    ('request_id', f'bot:{KEY}:rehome-different-request'),
    ('source_identity', 'different-source'),
    ('delivery_id', 'different-delivery'),
    ('new_session_id', 'different-target'),
])
async def test_rehome_valid_but_different_tuple_retains_exact_indexes(
        managed_bot, field, value,
):
    from hermes_state_runtime import RuntimeStoreError

    root, row, rehome = _prepare_bound_rehome(
        managed_bot, 'rehome-different-tuple-' + field, f'{managed_bot.chat}:{field}',
    )
    admission_id = row['admission_id']
    coordinator = managed_bot.authority._preservation_coordinator()
    lease_id = root.lease.lease_id
    handles_before = dict(managed_bot.authority._preservation_handles)
    bindings_before = dict(managed_bot.authority._preservation_automation_bindings)
    rehome[field] = value

    with pytest.raises(RuntimeStoreError, match='preservation_owner_'):
        managed_bot.authority._preservation_finish_automation_rehome(admission_id)

    assert managed_bot.authority._preservation_automation_rehomes[admission_id] is rehome
    assert managed_bot.authority._preservation_handles == handles_before
    assert managed_bot.authority._preservation_automation_bindings == bindings_before
    assert id(root) not in managed_bot.authority._preservation_pending_roots
    assert coordinator._records[lease_id].handle is root
    assert coordinator._records[lease_id].state == 'unknown'


@pytest.mark.asyncio
async def test_rehome_copied_handle_marks_original_root_unknown(managed_bot):
    from hermes_state_runtime import RuntimeStoreError

    root, row, rehome = _prepare_bound_rehome(
        managed_bot, 'rehome-copied-handle', f'{managed_bot.chat}:copied',
    )
    admission_id = row['admission_id']
    coordinator = managed_bot.authority._preservation_coordinator()
    lease_id = root.lease.lease_id
    handles_before = dict(managed_bot.authority._preservation_handles)
    bindings_before = dict(managed_bot.authority._preservation_automation_bindings)
    rehome['handle'] = replace(root)

    with pytest.raises(RuntimeStoreError, match='preservation_owner_'):
        managed_bot.authority._preservation_finish_automation_rehome(admission_id)

    assert managed_bot.authority._preservation_automation_rehomes[admission_id] is rehome
    assert managed_bot.authority._preservation_handles == handles_before
    assert managed_bot.authority._preservation_automation_bindings == bindings_before
    assert id(root) not in managed_bot.authority._preservation_pending_roots
    assert coordinator._records[lease_id].handle is root
    assert coordinator._records[lease_id].state == 'unknown'


@pytest.mark.asyncio
async def test_retry_terminal_cancellation_releases_only_unaccepted_pair(managed_bot):
    from gateway.session_bot_custody import (
        _bind_or_release_bot_child, prepare_bot_retry_custody,
    )
    from hermes_state_runtime import admit_session_input, cancel_session_input

    live = managed_bot.authority.sessions[managed_bot.chat]
    identity = f'bot:{KEY}:retry'
    principal_id = 'automation:' + live.route
    source_identity = 'test-source'
    root, child = prepare_bot_retry_custody(
        managed_bot.authority, identity=identity, principal_id=principal_id,
        session_id=managed_bot.chat, source_identity=source_identity,
    )
    descriptor = managed_bot.authority._preservation_automation_descriptor(
        root, request_id=identity, principal_id=principal_id, session_id=managed_bot.chat,
        source_identity=source_identity, delivery_id=identity[4:],
    )
    row = admit_session_input(
        managed_bot.authority.db, epoch=managed_bot.authority.epoch,
        principal_id=principal_id, session_id=managed_bot.chat, request_id=identity,
        payload={'text': 'retry', 'local_automation_v1': {
            'identity': identity, 'owner': managed_bot.chat, 'route': live.route,
            'target': managed_bot.chat, 'custody': descriptor,
        }}, intent='queue',
    )
    cancelled = cancel_session_input(
        managed_bot.authority.db, epoch=managed_bot.authority.epoch,
        admission_id=row['admission_id'],
    )
    assert cancelled['outcome'] == 'cancelled'

    receipt = SimpleNamespace(admission_id=row['admission_id'])
    assert _bind_or_release_bot_child(
        managed_bot.authority, child_key=child, root_handle=root, receipt=receipt,
    ) is False
    assert managed_bot.authority._preservation_release_automation_unaccepted(
        None, root,
    ) is True
    assert child not in managed_bot.authority._preservation_operations
    assert row['admission_id'] not in managed_bot.authority._preservation_automation_children
    assert identity[4:] not in managed_bot.authority._preservation_automation_roots


@pytest.mark.asyncio
@pytest.mark.parametrize('reason', ('lease_release_failed', 'stale_lease'))
async def test_retry_release_failure_persists_unknown_pair(managed_bot, monkeypatch, reason):
    from gateway.runtime_custody import CustodyRefusal
    from gateway.session_bot_custody import prepare_bot_retry_custody
    from gateway.session_bot import _retain_retry_custody_unknown
    from gateway.session_bot_mailbox import read_receipt

    live = managed_bot.authority.sessions[managed_bot.chat]
    identity = f'bot:{KEY}:retry'
    root, child = prepare_bot_retry_custody(
        managed_bot.authority, identity=identity, principal_id='automation:' + live.route,
        session_id=managed_bot.chat, source_identity='test-source',
    )
    coordinator = managed_bot.authority._preservation_coordinator()
    monkeypatch.setattr(
        coordinator, 'release_unaccepted',
        lambda _handle: CustodyRefusal(reason),
    )

    assert managed_bot.authority._preservation_release_automation_unaccepted(
        child, root,
    ) is False
    operation = managed_bot.authority._preservation_operations[child]
    root_record = coordinator._records[root.lease.lease_id]
    assert operation['unknown'] is True
    assert root_record.state == 'unknown'
    assert managed_bot.authority._preservation_automation_roots[identity[4:]] is root
    record = {
        'delivery_id': KEY,
        'profile_home': str(managed_bot.home.resolve()),
        'session_id': managed_bot.chat,
        'principal_id': 'uid:1000',
        'message': 'retry',
        'status': 'failed',
    }
    await _retain_retry_custody_unknown(managed_bot.home, record, identity)
    _, refreshed = await read_receipt(managed_bot.home, KEY)
    assert refreshed['status'] == 'ambiguous'
    assert refreshed['retry'] == {'identity': identity, 'unknown': True}


@pytest.mark.asyncio
async def test_retry_unknown_projection_survives_owner_refresh_without_refusal(
        managed_bot, monkeypatch,
):
    from gateway.session_bot import (
        _result, _retry_eligible, _scan_owner_receipts, deliver,
    )
    from gateway import session_bot_custody
    from gateway.session_bot_mailbox import read_receipt
    from hermes_state_runtime import RuntimeStoreError

    async def fail_retry(*_args, **_kwargs):
        raise RuntimeStoreError('preservation_custody_unknown')

    monkeypatch.setattr(session_bot_custody, 'commit_bot_retry_admission', fail_retry)
    await deliver(
        managed_bot.connection, {'id': 'b' * 32, 'profile': 'default', 'message': 'ping'},
    )
    from tests.waiting import async_wait_until
    from tools.bot_live_delivery import read_delivery_result

    def ambiguous_projection():
        result = read_delivery_result(managed_bot.home, 'b' * 32)
        return result if result and result.get('status') == 'ambiguous' else None

    result = await async_wait_until(
        ambiguous_projection, timeout=10, what='retry UNKNOWN mailbox projection',
    )
    assert result['status'] == 'ambiguous'
    _, refreshed = await read_receipt(managed_bot.home, 'b' * 32)

    assert refreshed.get('admission_id')
    assert refreshed.get('admission_principal_id')
    assert isinstance(refreshed.get('automation_custody'), dict)
    identity = 'bot:' + ('b' * 32) + ':retry'
    _owner_root, refreshed_records, barrier = await _scan_owner_receipts(
        managed_bot.authority, managed_bot.home,
    )

    assert refreshed['status'] == 'ambiguous'
    assert refreshed['retry'] == {'identity': identity, 'unknown': True}
    assert barrier is None
    assert any(item.get('retry') == refreshed['retry'] for _path, item in refreshed_records)
    assert _retry_eligible(managed_bot.authority, refreshed) is False
    assert _result(managed_bot.authority, refreshed)['status'] == 'ambiguous'
