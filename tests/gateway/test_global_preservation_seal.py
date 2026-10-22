"""Causal regressions for the global preservation close source graph."""

import pytest

from tests.gateway.test_preservation_admission_consumers import _install_custody


def test_quiescent_i05_and_i11_sources_seal_from_global_close(owner):
    from gateway.run_runtime import _seal_integrated_preservation_owners

    coordinator = _install_custody(owner)
    receipt = coordinator.begin_close(owner_id='global-i05-i11-seal')
    try:
        assert owner.preservation_owner_ready('I05_controls_and_unknown') is True
        assert owner.preservation_owner_ready('I11_session_mutations_finalizers') is True
        _seal_integrated_preservation_owners(owner.runner, coordinator, receipt)
        states = coordinator.status()['owner_states']
        assert states['I05_controls_and_unknown'] == 'sealed'
        assert states['I11_session_mutations_finalizers'] == 'sealed'
    finally:
        assert coordinator.abort_before_handoff(receipt) is None


@pytest.mark.parametrize('missing_position', [0, 1])
def test_one_missing_profile_observation_cannot_seal_global_owner(
    owner, monkeypatch, missing_position,
):
    from gateway import run_runtime

    coordinator = _install_custody(owner)
    receipt = coordinator.begin_close(owner_id='global-all-profile-evidence')
    other = object()
    authorities = [owner, other]
    missing = authorities[missing_position]
    monkeypatch.setattr(run_runtime, '_authorities', lambda _runner: authorities)
    monkeypatch.setattr(
        run_runtime, '_authority_owner_ready',
        lambda _authority, owner_class, _coordinator: owner_class == 'I05_controls_and_unknown',
    )
    original = run_runtime._AUTHORITY_OWNER_EVIDENCE_HANDLERS[
        'I05_controls_and_unknown'
    ]

    def source_evidence(runner, authority, close, owner_class, owner_id):
        if authority is missing:
            return None
        return original(runner, owner, close, owner_class, owner_id)

    monkeypatch.setitem(
        run_runtime._AUTHORITY_OWNER_EVIDENCE_HANDLERS,
        'I05_controls_and_unknown', source_evidence,
    )
    try:
        run_runtime._seal_integrated_preservation_owners(owner.runner, coordinator, receipt)
        assert coordinator.status()['owner_states']['I05_controls_and_unknown'] != 'sealed'
    finally:
        assert coordinator.abort_before_handoff(receipt) is None


@pytest.mark.parametrize(
    ('owner_class', 'attribute', 'mode'),
    [
        ('I05_controls_and_unknown', 'pending_stops', 'delete'),
        ('I05_controls_and_unknown', 'pending_stops', 'wrong_type'),
        ('I11_session_mutations_finalizers', '_local_adoptions', 'delete'),
        ('I11_session_mutations_finalizers', '_mutation_tasks', 'wrong_type'),
    ],
)
def test_global_seal_refuses_missing_or_replaced_source_index(
    owner, monkeypatch, owner_class, attribute, mode,
):
    from gateway.run_runtime import _seal_integrated_preservation_owners

    coordinator = _install_custody(owner)
    receipt = coordinator.begin_close(owner_id='global-source-index-negative')
    try:
        if mode == 'delete':
            monkeypatch.delattr(owner, attribute)
        else:
            monkeypatch.setattr(owner, attribute, [])
        assert owner.preservation_owner_ready(owner_class) is False
        _seal_integrated_preservation_owners(owner.runner, coordinator, receipt)
        assert coordinator.status()['owner_states'][owner_class] != 'sealed'
    finally:
        assert coordinator.abort_before_handoff(receipt) is None


@pytest.mark.parametrize('attribute', ['_mutation_tasks', '_local_adoptions'])
def test_global_seal_refuses_unknown_task_values(attribute, owner):
    from gateway.run_runtime import _seal_integrated_preservation_owners

    coordinator = _install_custody(owner)
    receipt = coordinator.begin_close(owner_id='global-unknown-task-negative')
    index = getattr(owner, attribute)
    if type(index) is dict:
        before = dict(index)
        index['unknown-task'] = object()
    else:
        before = set(index)
        index.add(object())
    try:
        assert owner.preservation_owner_ready('I11_session_mutations_finalizers') is False
        _seal_integrated_preservation_owners(owner.runner, coordinator, receipt)
        assert coordinator.status()['owner_states']['I11_session_mutations_finalizers'] != 'sealed'
    finally:
        if type(index) is dict:
            index.clear()
            index.update(before)
        else:
            index.clear()
            index.update(before)
        assert coordinator.abort_before_handoff(receipt) is None


def test_i09_global_seal_uses_hosted_source_evidence(started_mux):
    from gateway.run_runtime import _seal_integrated_preservation_owners
    from gateway.session_hosted_service import (
        abort_i09_source_close,
        await_i09_source_close,
        begin_i09_source_close,
    )

    runner, _homes, _loop, call = started_mux
    coordinator = runner.runtime_custody

    async def invoke(function, *args, **kwargs):
        result = function(*args, **kwargs)
        if hasattr(result, '__await__'):
            return await result
        return result

    receipt = call(invoke(coordinator.begin_close, owner_id='global-i09-seal'))
    try:
        assert call(invoke(begin_i09_source_close, runner, receipt.close_generation))
        assert call(invoke(await_i09_source_close, runner, receipt.close_generation, 10))
        call(invoke(_seal_integrated_preservation_owners, runner, coordinator, receipt))
        assert coordinator.status()['owner_states']['I09_a2a_hosted_kanban'] == 'sealed'
    finally:
        assert call(invoke(abort_i09_source_close, runner, receipt.close_generation))
        assert call(invoke(coordinator.abort_before_handoff, receipt)) is None


@pytest.mark.asyncio
async def test_session_prune_retains_i11_lease_until_owner_task_finishes(owner, monkeypatch):
    import asyncio

    from gateway import session_prune
    from gateway.session_controls import AuthorityConnection
    from tests.waiting import async_wait_until

    coordinator = _install_custody(owner)
    entered, release = asyncio.Event(), asyncio.Event()

    async def held_apply(*_args):
        entered.set()
        await release.wait()
        return {'deleted': 0}

    monkeypatch.setattr(session_prune, '_apply', held_apply)
    identity = {
        'user_id': 'local-operator', 'provider': 'local', 'profile_id': owner.profile_id,
        'instance_id': owner.instance_id, 'native_bootstrap': True,
        'capabilities': ['session:create', 'session:read', 'session:submit', 'session:control'],
    }
    connection = AuthorityConnection(owner, object(), identity, operator=True)
    task = asyncio.create_task(connection.dispatch({
        'id': 'session-prune-custody',
        'method': 'session.prune',
        'params': {'filters': {'older_than_days': 0}},
    }))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        records = [
            record for record in owner._preservation_operations.values()
            if record.get('owner_class') == 'I11_session_mutations_finalizers'
            and record.get('operation') == 'session-prune'
        ]
        assert len(records) == 1
        assert coordinator.status()['active_owner_classes'] == [
            'I11_session_mutations_finalizers',
        ]
        release.set()
        frame = await asyncio.wait_for(task, 5)
        assert frame['result'] == {'deleted': 0}
        await async_wait_until(
            lambda: not owner._preservation_operations,
            timeout=5,
            what='session.prune custody finalizer',
        )
        assert coordinator.status()['active_owner_classes'] == []
    finally:
        release.set()
        if not task.done():
            await task
        await connection.close()


@pytest.mark.asyncio
async def test_session_prune_failure_retains_exact_i11_unknown(owner, monkeypatch):
    import asyncio

    from gateway import session_prune
    from gateway.session_controls import AuthorityConnection
    from hermes_state_runtime import RuntimeStoreError
    from tests.waiting import async_wait_until

    coordinator = _install_custody(owner)
    entered = asyncio.Event()

    async def failed_apply(*_args):
        entered.set()
        raise RuntimeStoreError('read_failed')

    monkeypatch.setattr(session_prune, '_apply', failed_apply)
    identity = {
        'user_id': 'local-operator', 'provider': 'local', 'profile_id': owner.profile_id,
        'instance_id': owner.instance_id, 'native_bootstrap': True,
        'capabilities': ['session:create', 'session:read', 'session:submit', 'session:control'],
    }
    connection = AuthorityConnection(owner, object(), identity, operator=True)
    try:
        task = asyncio.create_task(connection.dispatch({
            'id': 'session-prune-unknown',
            'method': 'session.prune',
            'params': {'filters': {'older_than_days': 0}},
        }))
        await asyncio.wait_for(entered.wait(), 5)
        frame = await asyncio.wait_for(task, 5)
        assert frame['error']['data']['reason'] == 'read_failed'
        await async_wait_until(
            lambda: bool(owner._preservation_operations),
            timeout=5,
            what='retained session.prune unknown lease',
        )
        record = next(iter(owner._preservation_operations.values()))
        assert record['owner_class'] == 'I11_session_mutations_finalizers'
        assert record['operation'] == 'session-prune'
        assert record['unknown'] is True
        lease = coordinator._records[record['handle'].lease.lease_id]
        assert lease.state == 'unknown'
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_session_prune_refuses_a_fresh_preservation_drain(owner):
    from gateway import session_prune
    from gateway.session_contract import SessionRef
    from gateway.session_controls import AuthorityConnection
    from hermes_state_runtime import RuntimeStoreError

    _install_custody(owner)
    owner.runner._preservation_drain_active = True
    identity = {
        'user_id': 'local-operator', 'provider': 'local', 'profile_id': owner.profile_id,
        'instance_id': owner.instance_id, 'native_bootstrap': True,
        'capabilities': ['session:create', 'session:read', 'session:submit', 'session:control'],
    }
    connection = AuthorityConnection(owner, object(), identity, operator=True)
    try:
        with pytest.raises(RuntimeStoreError, match='runtime_draining'):
            await session_prune.prune(
                connection, SessionRef(owner.profile_id, ''),
                {'filters': {'older_than_days': 0}},
            )
        assert not owner._preservation_operations
    finally:
        await connection.close()
