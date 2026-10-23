"""Bot delivery binds the real mailbox admission to its I07 root and I06 child."""

import asyncio

import pytest

pytest_plugins = ['tests.gateway.test_session_bot_retry']


@pytest.mark.asyncio
async def test_bot_queued_generation_is_pinned_only_by_its_real_fifo_claim(bot, monkeypatch):
    from gateway import session_finite
    from gateway.session_bot import deliver, _result
    from gateway.session_bot_mailbox import read_receipt
    from hermes_state_runtime import get_session_admission, list_session_admissions, RuntimeStoreError
    from tests.gateway.test_session_bot_retry import _settle_quietly

    authority = bot.authority
    coordinator = _install_custody(authority)
    scheduled = []
    original_schedule = authority._schedule_admitted
    original_execute = session_finite.execute_finite_admission
    started, finish = asyncio.Event(), asyncio.Event()

    async def held_execute(authority, ref, row):
        started.set()
        await finish.wait()
        return await original_execute(authority, ref, row)

    monkeypatch.setattr(authority, '_schedule_admitted', lambda ref, _receipt=None: scheduled.append(ref))
    monkeypatch.setattr(session_finite, 'execute_finite_admission', held_execute)
    key = '9' * 32
    params = {'id': key, 'profile': 'default', 'message': 'queued custody input'}
    try:
        queued = await deliver(bot.connection, params)
        admission_id = queued['admission_id']
        row = get_session_admission(authority.db, admission_id=admission_id)
        assert row['status'] == 'queued' and row['generation'] is None
        assert queued['status'] == 'queued' and queued.get('execution_generation') is None
        root = authority._preservation_handles[admission_id]
        child_key = authority._preservation_automation_children[admission_id]
        child = authority._preservation_operations[child_key]['handle']
        assert root.owner_class == 'I07_automation_route_wakes'
        assert child.owner_class == 'I06_busy_followup_delivery'
        again = await deliver(bot.connection, params)
        assert again['admission_id'] == admission_id
        assert authority._preservation_handles[admission_id] is root
        assert authority._preservation_operations[child_key]['handle'] is child
        assert len(list_session_admissions(authority.db, session_id=bot.chat, pending_only=False)) == 1
        original_schedule(scheduled[0])
        await asyncio.wait_for(started.wait(), timeout=5)
        row = get_session_admission(authority.db, admission_id=admission_id)
        assert row['status'] == 'started' and type(row['generation']) is int and row['generation'] > 0
        claimed = await deliver(bot.connection, params)
        assert claimed['admission_id'] == admission_id
        assert claimed['execution_generation'] == row['generation']
        _, record = await read_receipt(bot.home, key)
        for field, value in [('execution_generation', row['generation'] + 1),
                             ('admission_principal_id', 'foreign-principal'),
                             ('admission_request_id', 'foreign-request'),
                             ('authority_epoch', authority.epoch + 1)]:
            with pytest.raises(RuntimeStoreError):
                _result(authority, {**record, field: value})
    finally:
        finish.set()
    await _settle_quietly(bot)
    _, saved = await read_receipt(bot.home, key)
    assert saved['status'] == 'settled' and saved['reply'] == 'pong'
    assert saved['admission_id'] == admission_id
    assert saved['execution_generation'] == row['generation']
    assert coordinator.status()['active_owner_classes'] == [], coordinator.status()


def _install_custody(authority):
    # Reinstall through the canonical runtime binder so this legacy Bot fixture carries the
    # same real profile registry, DB epoch, graph identity, and retained process-ownership
    # witness as a booted authority.  A bare coordinator would make the strict I07 source
    # reader correctly refuse at the physical-owner boundary.
    from tests.gateway.conftest import bind_authority_runtime_custody

    return bind_authority_runtime_custody(authority.runner, authority)


async def _start_held_bot_admission(bot, monkeypatch, key):
    """Claim one real Bot row and hold it before the model/handler boundary."""
    from gateway import session_finite
    from gateway.session_bot import deliver
    from hermes_state_runtime import get_session_admission

    authority = bot.authority
    coordinator = _install_custody(authority)
    scheduled = []
    original_schedule = authority._schedule_admitted
    original_execute = session_finite.execute_finite_admission
    started, finish = asyncio.Event(), asyncio.Event()
    executed = []

    async def held_execute(authority, ref, row):
        started.set()
        await finish.wait()
        executed.append(row['admission_id'])
        return await original_execute(authority, ref, row)

    monkeypatch.setattr(authority, '_schedule_admitted',
                        lambda ref, _receipt=None: scheduled.append(ref))
    monkeypatch.setattr(session_finite, 'execute_finite_admission', held_execute)
    receipt = await deliver(
        bot.connection,
        {'id': key, 'profile': 'default', 'message': 'held Bot custody input'},
    )
    assert scheduled, 'the real Bot admission did not reach its FIFO scheduler'
    original_schedule(scheduled[0])
    await asyncio.wait_for(started.wait(), timeout=5)
    row = get_session_admission(authority.db, admission_id=receipt['admission_id'])
    assert row['status'] == 'started' and type(row['generation']) is int and row['generation'] >= 1
    child_key = authority._preservation_automation_children[receipt['admission_id']]
    return {
        'authority': authority,
        'coordinator': coordinator,
        'receipt': receipt,
        'row': row,
        'root': authority._preservation_handles[receipt['admission_id']],
        'child_key': child_key,
        'record': authority._preservation_operations[child_key],
        'finish': finish,
        'executed': executed,
    }


@pytest.mark.asyncio
async def test_started_bot_admission_binds_without_a_classic_bridge(bot, monkeypatch):
    from gateway.session_automation import restore_local_automation
    from gateway.session_contract import SessionRef
    from gateway.session_ingress import _bind_classic_execution_event
    from tests.gateway.test_session_bot_retry import _settle_quietly

    held = await _start_held_bot_admission(bot, monkeypatch, 'e' * 32)
    authority, row = held['authority'], held['row']
    event = restore_local_automation(
        authority, SessionRef(authority.profile_id, bot.chat), row,
    )
    try:
        _bind_classic_execution_event(authority, row, event)
        assert getattr(event, '_classic_execution_bridge', None) is None
        assert held['executed'] == []
        binding = authority._preservation_operations[held['child_key']]
        assert binding.get('producer_object') is None
        assert binding['parent_handle'] is held['root']
        assert authority._preservation_handles[row['admission_id']] is held['root']
    finally:
        held['finish'].set()
    await _settle_quietly(bot)


@pytest.mark.asyncio
@pytest.mark.parametrize('tamper', ['copied_child', 'foreign_source', 'missing_child_index'])
async def test_started_bot_tamper_keeps_exact_handles_unknown_before_handler(
    bot, monkeypatch, tamper,
):
    from copy import copy

    from gateway.session_automation import restore_local_automation
    from gateway.session_contract import SessionRef
    from gateway.session_ingress import _bind_classic_execution_event
    from gateway.session_managed_worker import ManagedExecutionUnknown
    from tests.gateway.test_session_bot_retry import _settle_quietly

    held = await _start_held_bot_admission(bot, monkeypatch, ('d' if tamper == 'copied_child' else 'a') * 32)
    authority, row = held['authority'], held['row']
    child_key, original_record = held['child_key'], held['record']
    original_child = original_record['handle']
    if tamper == 'copied_child':
        original_record['handle'] = copy(original_record['handle'])
    elif tamper == 'foreign_source':
        original_record['source_identity'] = 'foreign-source-identity'
    else:
        authority._preservation_automation_children.pop(row['admission_id'])
    event = restore_local_automation(
        authority, SessionRef(authority.profile_id, bot.chat), row,
    )
    try:
        with pytest.raises(ManagedExecutionUnknown, match='bot_owner_unknown'):
            _bind_classic_execution_event(authority, row, event)
        assert getattr(event, '_classic_execution_bridge', None) is None
        assert held['executed'] == []
        assert authority._preservation_operations[child_key]['unknown'] is True
        root_record = held['coordinator']._records[held['root'].lease.lease_id]
        child_record = held['coordinator']._records[original_child.lease.lease_id]
        assert root_record.handle is held['root'] and root_record.state == 'unknown'
        assert child_record.handle is original_child
        assert child_record.state == 'unknown'
    finally:
        held['finish'].set()
    await _settle_quietly(bot)


@pytest.mark.asyncio
async def test_bot_settlement_cancellation_retains_the_exact_claim_unknown(bot, monkeypatch):
    from gateway.session_bot import deliver
    from hermes_state_runtime import get_session_admission

    authority = bot.authority
    coordinator = _install_custody(authority)
    entered, hold = asyncio.Event(), asyncio.Event()

    async def held_settlement(ref, live, row, response, outcome):
        entered.set()
        await hold.wait()

    monkeypatch.setattr(authority, '_settle_claimed', held_settlement)
    receipt = await deliver(bot.connection, {'id': '8' * 32, 'profile': 'default', 'message': 'cancel settlement'})
    await asyncio.wait_for(entered.wait(), timeout=5)
    admission_id = receipt['admission_id']
    root = authority._preservation_handles[admission_id]
    child = authority._preservation_children[admission_id]
    task = authority.sessions[bot.chat].task
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert authority._preservation_handles[admission_id] is root
    assert authority._preservation_children[admission_id] is child
    root_record = coordinator._records[root.lease.lease_id]
    child_record = coordinator._records[child.lease.lease_id]
    assert root_record.handle is root and root_record.state == 'unknown'
    assert child_record.handle is child and child_record.state == 'unknown'
    row = get_session_admission(authority.db, admission_id=admission_id)
    assert row['status'] == 'started' and row['generation'] > 0
    from gateway.run_runtime import _seal_integrated_preservation_owners
    receipt = coordinator.begin_close(owner_id='bot-unknown-readiness')
    try:
        _seal_integrated_preservation_owners(authority.runner, coordinator, receipt)
        assert coordinator.status()['owner_states']['I07_automation_route_wakes'] != 'sealed'
        assert coordinator._records[root.lease.lease_id].handle is root
        assert coordinator._records[root.lease.lease_id].state == 'unknown'
    finally:
        assert coordinator.abort_before_handoff(receipt) is None


@pytest.mark.asyncio
async def test_managed_bot_projection_requires_its_authority_handle_registry(bot, monkeypatch):
    from gateway.session_bot import deliver
    from gateway.session_bot_custody import finish_bot_projection
    from gateway.session_bot_mailbox import read_receipt
    from hermes_state_runtime import RuntimeStoreError
    from tests.gateway.test_session_bot_retry import _settle_quietly

    _install_custody(bot.authority)
    key = '7' * 32
    await deliver(bot.connection, {'id': key, 'profile': 'default', 'message': 'managed projection'})
    await _settle_quietly(bot)
    _, record = await read_receipt(bot.home, key)
    assert record['status'] == 'settled'
    assert record['automation_custody']['owner_class'] == 'I07_automation_route_wakes'
    monkeypatch.delattr(bot.authority, '_preservation_handles')
    with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
        await finish_bot_projection(bot.authority, bot.home, record, True)


@pytest.mark.asyncio
async def test_bot_delivery_stamps_durable_automation_custody(bot):
    from gateway.session_bot import deliver
    from hermes_state_runtime import list_session_admissions

    coordinator = _install_custody(bot.authority)
    key = 'f' * 32

    receipt = await deliver(bot.connection, {'id': key, 'profile': 'default', 'message': 'ping'})
    for _ in range(200):
        rows = list_session_admissions(bot.authority.db, session_id=bot.chat, pending_only=False)
        if rows and rows[0]['status'] == 'terminal':
            break
        await asyncio.sleep(0.02)
    else:
        raise AssertionError(f'admission did not settle: {rows!r}')

    row = rows[0]
    custody = row['payload']['local_automation_v1']['custody']
    assert receipt['admission_id'] == row['admission_id']
    assert custody['owner_class'] == 'I07_automation_route_wakes'
    assert custody['request_id'] == 'bot:' + key
    assert custody['session_id'] == row['target_session_id']
    assert custody['principal_id'] == row['principal_id']
    assert custody['source_identity']
    assert row['generation'] is not None
    from tests.waiting import async_wait_until
    await async_wait_until(
        lambda: coordinator.status()['active_owner_classes'] == [],
        timeout=10,
        what='Bot delivery custody finalizers',
    )
    assert coordinator.status()['active_owner_classes'] == []
    from gateway.run_runtime import _seal_integrated_preservation_owners
    authority = bot.authority
    assert authority.preservation_owner_ready('I07_automation_route_wakes') is True
    receipt = coordinator.begin_close(owner_id='bot-terminal-readiness')
    try:
        _seal_integrated_preservation_owners(authority.runner, coordinator, receipt)
        assert coordinator.status()['owner_states']['I07_automation_route_wakes'] == 'sealed'
    finally:
        assert coordinator.abort_before_handoff(receipt) is None
