"""Causal I05 prompt-control leases over a real API admission and FIFO claim."""

import pytest

from tests.gateway.test_preservation_admission_consumers import _install_custody


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['queued', 'started', 'claimed_before_control', 'claim_during_cancel'])
async def test_api_stop_preserves_exact_claim_races(api, owner, monkeypatch, phase):
    from gateway import session_authority
    from gateway.platforms import api_server_authority_runs as controls
    from gateway.session_api_turn import admit_api_turn
    from hermes_state_runtime import claim_session_input, get_session_admission
    from tests.gateway.test_canonical_stop_hard_cancel import Agent

    coordinator = _install_custody(owner)
    monkeypatch.setattr(owner, '_schedule', lambda _ref: None)
    _, ref, row = admit_api_turn(
        api, session_id='stop-race', request_id='stop-race',
        user_message='stop this admission', conversation_history=[])
    agent = Agent()
    owner.runner._cached_agent_for = lambda _route: agent
    if phase == 'started':
        await owner._claim_next(ref, owner.sessions[ref.session_id])
    elif phase == 'claimed_before_control':
        real_lookup = controls.run_admission

        def lookup_then_claim(adapter, run_id):
            snapshot = real_lookup(adapter, run_id)
            if snapshot[1]['status'] == 'queued':
                claim_session_input(owner.db, epoch=owner.epoch, session_id=ref.session_id)
            return snapshot

        monkeypatch.setattr(controls, 'run_admission', lookup_then_claim)
    elif phase == 'claim_during_cancel':
        real_cancel = session_authority.cancel_session_input

        def claim_then_cancel(db, **kwargs):
            claim_session_input(db, epoch=owner.epoch, session_id=ref.session_id)
            return real_cancel(db, **kwargs)

        monkeypatch.setattr(session_authority, 'cancel_session_input', claim_then_cancel)

    result = await controls.stop_run(api, row['request_id'])
    actual = get_session_admission(owner.db, admission_id=row['admission_id'])
    if phase == 'queued':
        assert actual['status'] == 'terminal' and actual['outcome'] == 'cancelled'
        assert result['status'] == 'cancelled'
        assert agent.stops == []
    else:
        assert actual['status'] == 'started'
        assert result['status'] == 'stopping'
        assert agent.stops == ['hard']
    assert not owner._preservation_operations
    assert 'I05_controls_and_unknown' not in coordinator.status()['active_owner_classes']


@pytest.mark.asyncio
async def test_api_stop_cancels_its_queued_follower_without_interrupting_the_live_head(api, owner, monkeypatch):
    from gateway.platforms.api_server_authority_runs import stop_run
    from gateway.session_api_turn import admit_api_turn
    from hermes_state_runtime import get_session_admission
    from tests.gateway.test_canonical_stop_hard_cancel import Agent

    coordinator = _install_custody(owner)
    monkeypatch.setattr(owner, '_schedule', lambda _ref: None)
    _, ref, head = admit_api_turn(
        api, session_id='queued-stop', request_id='head', user_message='running', conversation_history=[])
    _, _, follower = admit_api_turn(
        api, session_id=ref.session_id, request_id='follower', user_message='queued', conversation_history=[])
    await owner._claim_next(ref, owner.sessions[ref.session_id])
    agent = Agent()
    owner.runner._cached_agent_for = lambda _route: agent
    head_handle = owner._preservation_children[head['admission_id']]
    result = await stop_run(api, follower['request_id'])
    assert result['status'] == 'cancelled'
    assert get_session_admission(owner.db, admission_id=head['admission_id'])['status'] == 'started'
    assert get_session_admission(owner.db, admission_id=follower['admission_id'])['outcome'] == 'cancelled'
    assert owner._preservation_children[head['admission_id']] is head_handle
    assert agent.stops == []
    assert not owner._preservation_operations
    assert 'I05_controls_and_unknown' not in coordinator.status()['active_owner_classes']


@pytest.mark.asyncio
async def test_restart_unknown_control_uses_durable_recovery_without_a_live_old_parent(api, owner, monkeypatch):
    from gateway.session_authority import LiveSession, initialize_session_authority
    from gateway.session_api_turn import admit_api_turn
    from gateway.session_contract import Submission
    from gateway.session_controls import AuthorityConnection
    from hermes_state_runtime import claim_session_input, get_session_admission
    from tests.gateway.test_authority_unknown_resolution import Peer

    _install_custody(owner)
    monkeypatch.setattr(owner, '_schedule', lambda _ref: None)
    # Create the real route, then submit the lost turn under the authenticated viewer's identity.
    _, ref, api_row = admit_api_turn(
        api, session_id='restart-control', request_id='route-only',
        user_message='route initializer', conversation_history=[])
    from gateway.session_contract import Principal
    api_actor = Principal('api', owner.profile_id, frozenset({'session:submit'}), 'api-test')
    await owner.cancel_queued(api_actor, ref, api_row['admission_id'])
    first = AuthorityConnection(owner, Peer(), {'user_id': 'recovery-user'})
    lost = await owner.submit(first.actor, Submission('lost-head', ref, {'text': 'lost turn'}, 'queue'))
    follower = await owner.submit(first.actor, Submission('next-turn', ref, {'text': 'next turn'}, 'queue'))
    claimed = claim_session_input(owner.db, epoch=owner.epoch, session_id=ref.session_id)
    assert claimed['admission_id'] == lost.admission_id
    old_epoch = owner.epoch
    source = owner.sessions[ref.session_id].source
    restarted = await initialize_session_authority(
        owner.runner, profile_id=owner.profile_id, instance_id='restarted-control-owner', db=owner.db)
    restarted.sessions[ref.session_id] = LiveSession(source, ref.session_id)
    monkeypatch.setattr(restarted, '_schedule', lambda _ref: None)
    coordinator = _install_custody(restarted)
    connection = AuthorityConnection(restarted, Peer(), {'user_id': 'recovery-user'})
    params = {'session_id': ref.session_id, 'admission_id': lost.admission_id,
              'execution_generation': claimed['generation']}
    unknown = get_session_admission(restarted.db, admission_id=lost.admission_id)
    assert unknown['status'] == 'unknown' and unknown['owner_epoch'] == old_epoch
    assert old_epoch < restarted.epoch
    try:
        stale = await connection.dispatch({'id': 1, 'method': 'prompt.resolve_unknown',
                                          'params': {**params, 'execution_generation': claimed['generation'] + 1}})
        assert 'error' in stale
        assert stale['error']['data']['reason'] == 'stale_generation'
        assert not restarted._preservation_operations
        foreign = AuthorityConnection(restarted, Peer(), {'user_id': 'different-user'})
        try:
            denied = await foreign.dispatch({'id': 2, 'method': 'prompt.resolve_unknown', 'params': params})
            assert 'error' in denied
            assert not restarted._preservation_operations
        finally:
            await foreign.close()
        restarted.db._execute_write(lambda conn: conn.execute(
            "CREATE TRIGGER refuse_resolution BEFORE UPDATE OF status ON session_admissions "
            "WHEN NEW.admission_id=OLD.admission_id AND NEW.status='terminal' "
            "BEGIN SELECT RAISE(ABORT,'resolution failed'); END"))
        failed = await connection.dispatch({'id': 'failed-write', 'method': 'prompt.resolve_unknown', 'params': params})
        assert 'error' in failed
        retained = next(iter(restarted._preservation_operations.values()))
        original_handle = retained['handle']
        assert coordinator._records[original_handle.lease.lease_id].state == 'unknown'
        assert get_session_admission(restarted.db, admission_id=lost.admission_id)['status'] == 'unknown'
        restarted.db._execute_write(lambda conn: conn.execute('DROP TRIGGER refuse_resolution'))
        from dataclasses import replace
        retained['handle'] = replace(original_handle)
        copied = await connection.dispatch({'id': 'copied-handle', 'method': 'prompt.resolve_unknown', 'params': params})
        assert 'error' in copied
        assert coordinator._records[original_handle.lease.lease_id].state == 'unknown'
        retained['handle'] = original_handle
        identity = retained['recovery_identity']
        retained['recovery_identity'] = {**identity, 'owner_epoch': restarted.epoch}
        replaced = await connection.dispatch({'id': 'replaced-identity', 'method': 'prompt.resolve_unknown', 'params': params})
        assert 'error' in replaced
        assert get_session_admission(restarted.db, admission_id=lost.admission_id)['status'] == 'unknown'
        retained['recovery_identity'] = identity
        resolved = await connection.dispatch({'id': 3, 'method': 'prompt.resolve_unknown', 'params': params})
        assert 'error' not in resolved, resolved
        assert resolved['result']['status'] == 'terminal'
        assert resolved['result']['outcome'] == 'interrupted'
        assert not restarted._preservation_operations
        assert coordinator.status()['active_owner_classes'] == []
        assert original_handle.lease.lease_id not in coordinator._records
        retried = await connection.dispatch({'id': 4, 'method': 'prompt.resolve_unknown', 'params': params})
        assert retried['result'] == resolved['result']
        next_row = claim_session_input(restarted.db, epoch=restarted.epoch, session_id=ref.session_id)
        assert next_row['admission_id'] == follower.admission_id
        assert get_session_admission(restarted.db, admission_id=lost.admission_id)['owner_epoch'] == old_epoch
    finally:
        await connection.close()
        await first.close()


@pytest.mark.asyncio
async def test_prompt_control_binds_started_admission_and_settles_exact_child(api, owner):
    from gateway.session_api_turn import admit_api_turn

    coordinator = _install_custody(owner)
    _, ref, row = admit_api_turn(
        api,
        session_id='i05-prompt-control',
        request_id='i05-prompt-control',
        user_message='prompt control',
        conversation_history=[],
    )
    started, claimed = await owner._claim_next(ref, owner.sessions[ref.session_id])
    assert claimed['admission_id'] == row['admission_id']
    assert started['status'] == 'started'

    key = owner._preservation_prompt_operation_start(
        session_id=ref.session_id,
        generation=started['generation'],
        operation='approval',
        request_id='prompt-1',
    )
    assert owner._preservation_operations[key]['admission_id'] == row['admission_id']
    assert coordinator.status()['active_owner_classes'] == [
        'I02_api_session_turn', 'I04_fifo_turn_execution', 'I05_controls_and_unknown',
    ]

    owner._preservation_operation_terminal(key, 'prompt:approval:prompt-1')
    assert coordinator.status()['active_owner_classes'] == [
        'I02_api_session_turn', 'I04_fifo_turn_execution',
    ]


@pytest.mark.asyncio
async def test_prompt_control_rejects_stale_generation_before_reserving_child(api, owner):
    from gateway.session_api_turn import admit_api_turn
    from hermes_state_runtime import RuntimeStoreError

    coordinator = _install_custody(owner)
    _, ref, _row = admit_api_turn(
        api,
        session_id='i05-stale-prompt',
        request_id='i05-stale-prompt',
        user_message='stale prompt control',
        conversation_history=[],
    )
    started, _claimed = await owner._claim_next(ref, owner.sessions[ref.session_id])
    with pytest.raises(RuntimeStoreError, match='stale_generation'):
        owner._preservation_prompt_operation_start(
            session_id=ref.session_id,
            generation=started['generation'] + 1,
            operation='clarify',
            request_id='prompt-stale',
        )
    assert coordinator.status()['active_owner_classes'] == [
        'I02_api_session_turn', 'I04_fifo_turn_execution',
    ]


@pytest.mark.asyncio
async def test_prompt_control_unknown_reopens_only_the_same_owned_handle(api, owner):
    from gateway.session_api_turn import admit_api_turn
    from hermes_state_runtime import RuntimeStoreError

    coordinator = _install_custody(owner)
    _, ref, row = admit_api_turn(
        api,
        session_id='i05-unknown-prompt',
        request_id='i05-unknown-prompt',
        user_message='unknown prompt control',
        conversation_history=[],
    )
    started, _claimed = await owner._claim_next(ref, owner.sessions[ref.session_id])

    async def failed_control():
        raise RuntimeStoreError('read_failed')

    with pytest.raises(RuntimeStoreError, match='read_failed'):
        await owner._preservation_control_call(
            principal_id=row['principal_id'], session_id=ref.session_id,
            operation='approval', request_id='prompt-unknown',
            admission_id=row['admission_id'], generation=started['generation'],
            source_identity='prompt', call=failed_control)
    operation = next(iter(owner._preservation_operations.values()))
    assert coordinator._records[operation['handle'].lease.lease_id].state == 'unknown'
    async def successful_retry():
        return {'accepted': True}

    with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
        await owner._preservation_control_call(
            principal_id='foreign-principal', session_id=ref.session_id,
            operation='approval', request_id='prompt-unknown',
            admission_id=row['admission_id'], generation=started['generation'],
            source_identity='prompt', call=successful_retry)
    with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
        await owner._preservation_control_call(
            principal_id=row['principal_id'], session_id=ref.session_id,
            operation='approval', request_id='prompt-unknown',
            admission_id=row['admission_id'], generation=started['generation'] + 1,
            source_identity='prompt', call=successful_retry)

    result = await owner._preservation_control_call(
        principal_id=row['principal_id'], session_id=ref.session_id,
        operation='approval', request_id='prompt-unknown', admission_id=row['admission_id'],
        generation=started['generation'], source_identity='prompt', call=successful_retry)
    assert result == {'accepted': True}
    assert coordinator.status()['active_owner_classes'] == [
        'I02_api_session_turn', 'I04_fifo_turn_execution',
    ]


@pytest.mark.asyncio
async def test_api_approval_uses_durable_principal_and_prompt_lease(api, owner):
    from gateway.platforms.api_server_authority_runs import respond_run
    from gateway.session_api_turn import admit_api_turn

    coordinator = _install_custody(owner)
    _, ref, row = admit_api_turn(
        api, session_id='i05-api-respond', request_id='i05-api-respond',
        user_message='api prompt', conversation_history=[])
    started, _claimed = await owner._claim_next(ref, owner.sessions[ref.session_id])
    prompt_id = 'api-prompt-1'
    key = owner._preservation_prompt_operation_start(
        session_id=ref.session_id, generation=started['generation'],
        operation='approval', request_id=prompt_id)
    route = owner.sessions[ref.session_id].route
    owner.register_approval(ref.session_id, started['generation'], route, {
        'request_id': prompt_id, 'command': 'echo ok', 'description': 'test',
    })
    from tools.approval import _gateway_queues
    from tools.approval_gateway_wait import _ApprovalEntry
    _gateway_queues.setdefault(route, []).append(
        _ApprovalEntry({'request_id': prompt_id, 'command': 'echo ok'}))

    try:
        result = await respond_run(
            api, row['request_id'],
            {'request_id': prompt_id, 'execution_generation': started['generation'], 'choice': 'once'},
            kind='approval')
    finally:
        _gateway_queues.pop(route, None)
    assert result is not None
    # The API acknowledgement is a distinct control handle. The producer remains owned until
    # the approval waiter reports its actual settle boundary.
    assert key in owner._preservation_operations
    assert coordinator.status()['active_owner_classes'] == [
        'I02_api_session_turn', 'I04_fifo_turn_execution', 'I05_controls_and_unknown',
    ]
    owner._preservation_operation_terminal(key, 'prompt:approval:api-prompt-1')
    assert key not in owner._preservation_operations
    assert coordinator.status()['active_owner_classes'] == [
        'I02_api_session_turn', 'I04_fifo_turn_execution',
    ]


@pytest.mark.asyncio
async def test_tui_approval_uses_the_started_admission_identity(api, owner):
    from gateway.session_controls import AuthorityConnection
    from gateway.session_api_turn import admit_api_turn

    coordinator = _install_custody(owner)
    _, ref, _row = admit_api_turn(
        api, session_id='i05-tui-respond', request_id='i05-tui-respond',
        user_message='tui prompt', conversation_history=[])
    started, _claimed = await owner._claim_next(ref, owner.sessions[ref.session_id])
    prompt_id = 'tui-prompt-1'
    key = owner._preservation_prompt_operation_start(
        session_id=ref.session_id, generation=started['generation'],
        operation='approval', request_id=prompt_id)
    owner.register_approval(ref.session_id, started['generation'], 'tui', {
        'request_id': prompt_id, 'command': 'echo ok', 'description': 'test',
    })
    from tools.approval import _gateway_queues
    from tools.approval_gateway_wait import _ApprovalEntry
    route = owner.sessions[ref.session_id].route
    _gateway_queues.setdefault(route, []).append(
        _ApprovalEntry({'request_id': prompt_id, 'command': 'echo ok'}))
    connection = AuthorityConnection(owner, object(), {
        'user_id': 'tui-owner', 'instance_id': owner.instance_id,
        'profile_id': owner.profile_id,
    })
    await connection.resume(ref, {})
    try:
        result = await connection.respond(ref, {
            'session_id': ref.session_id, 'execution_generation': started['generation'],
            'prompt_id': prompt_id, 'choice': 'allow',
        })
        assert result is not None
        assert coordinator.status()['active_owner_classes'] == [
            'I02_api_session_turn', 'I04_fifo_turn_execution', 'I05_controls_and_unknown',
        ]
        owner._preservation_operation_terminal(key, 'prompt:approval:tui-prompt-1')
        assert coordinator.status()['active_owner_classes'] == [
            'I02_api_session_turn', 'I04_fifo_turn_execution',
        ]
    finally:
        _gateway_queues.pop(route, None)
        await connection.close()
