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
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_cli.backend_retirement import RetirementFence

    authority.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id,
        authority_epoch=authority.epoch,
        fence=RetirementFence(),
    )
    return authority.runner.runtime_custody


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
