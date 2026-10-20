"""Real SQLite automation recovery preserves the row-to-epoch custody fence."""

from types import SimpleNamespace

import pytest

from gateway.config import GatewayConfig, Platform
from gateway.runtime_custody import RuntimeCustodyCoordinator
from gateway.session import SessionSource, SessionStore
from gateway.session_authority import SessionAuthority
from hermes_cli.backend_retirement import RetirementFence
from hermes_state import SessionDB
from hermes_state_runtime import (
    admit_session_input,
    begin_runtime_epoch,
    claim_session_input,
    settle_session_input,
)


def _base_native_row(db, *, epoch, session_id, route):
    admitted = admit_session_input(
        db,
        epoch=epoch,
        principal_id='messaging:recovery-fixture',
        session_id=session_id,
        request_id='native-base',
        payload={'native_text_v1': {'route': route, 'source': {}}},
    )
    started = claim_session_input(db, epoch=epoch, session_id=session_id)
    settle_session_input(
        db,
        epoch=epoch,
        admission_id=admitted['admission_id'],
        generation=started['generation'],
        outcome='completed',
    )


def _recovery_fixture(tmp_path, *, descriptor_epoch=None, row_epoch=None, custody=True):
    db = SessionDB(tmp_path / 'state.db')
    store = SessionStore(tmp_path / 'sessions', GatewayConfig())
    store._db = db
    runner = SimpleNamespace(_draining=False, session_store=store)
    source = SessionSource(
        platform=Platform.TELEGRAM, chat_id='recovery-chat', user_id='recovery-user',
        profile='default',
    )
    entry = store.get_or_create_session(source)
    session_id, route = entry.session_id, entry.session_key
    old_epoch = begin_runtime_epoch(db, instance_id='previous-owner')
    _base_native_row(db, epoch=old_epoch, session_id=session_id, route=route)
    descriptor_epoch = old_epoch if descriptor_epoch is None else descriptor_epoch
    row_epoch = old_epoch if row_epoch is None else row_epoch
    descriptor = {
        'owner_class': 'I07_automation_route_wakes',
        'owner_id': f'automation:default:{session_id}:wake-1',
        'profile_id': 'default',
        'profile_home': str((tmp_path).resolve()),
        'authority_epoch': descriptor_epoch,
        'process_instance': 'previous-owner',
        'close_generation': 0,
        'coordinator_id': 'previous-coordinator',
        'session_id': session_id,
        'principal_id': 'automation:' + route,
        'request_id': 'wake-1',
        'source_identity': 'source-1',
    }
    automation = {
        'identity': 'wake-1', 'owner': session_id, 'route': route, 'target': session_id,
    }
    if custody:
        automation['custody'] = descriptor
    payload = {
        'text': 'RECOVER WAKE',
        'native_text_v1': {
            'route': route, 'source': {}, 'automation': automation,
        },
    }
    row = admit_session_input(
        db,
        epoch=old_epoch,
        principal_id='automation:' + route,
        session_id=session_id,
        request_id='wake-1',
        payload=payload,
    )
    if row_epoch != old_epoch:
        db._execute_write(lambda conn: conn.execute(
            'UPDATE session_admissions SET owner_epoch=? WHERE admission_id=?',
            (row_epoch, row['admission_id']),
        ))
    new_epoch = begin_runtime_epoch(db, instance_id='current-owner')
    recovered = SessionAuthority(
        runner, profile_id='default', instance_id='current-owner', db=db, epoch=new_epoch,
    )
    recovered.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance='current-owner', authority_epoch=new_epoch, fence=RetirementFence(),
    )
    scheduled = []
    recovered._schedule = lambda ref: scheduled.append(ref.session_id)
    return db, runner, source, session_id, route, old_epoch, new_epoch, row, descriptor, recovered, scheduled


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('case', 'descriptor_epoch', 'row_epoch', 'custody'),
    [
        ('missing_descriptor', 1, 1, False),
        ('zero_row_epoch', 1, 0, True),
        ('mismatched_row_epoch', 1, 2, True),
        ('mismatched_descriptor_epoch', 2, 1, True),
    ],
)
async def test_recover_native_sessions_refuses_unbound_automation_before_schedule(
        monkeypatch, tmp_path, case, descriptor_epoch, row_epoch, custody):
    db, _runner, source, session_id, route, _old_epoch, _new_epoch, _row, _descriptor, authority, scheduled = (
        _recovery_fixture(
            tmp_path, descriptor_epoch=descriptor_epoch, row_epoch=row_epoch, custody=custody,
        )
    )
    route_checks = []

    async def check_native_route(_runner, payload, target, available_source, adapter):
        route_checks.append(payload)
        return source, route

    monkeypatch.setattr('gateway.session_envelope.check_native_route', check_native_route)
    from gateway.runtime_ownership import process_ownership
    from pathlib import Path
    home = Path(db.db_path).parent.resolve()
    process_ownership.reserve([home])
    try:
        result = await authority.recover_native_sessions([(session_id, source, object())])
    finally:
        process_ownership.release(home)
        db.close()

    assert result == {session_id: 'preservation_owner_unmapped'}, case
    assert scheduled == []
    assert session_id not in authority.sessions
    assert route_checks == [], 'custody refusal precedes native route validation'


@pytest.mark.asyncio
async def test_recover_native_sessions_binds_real_row_and_allows_current_epoch_claim(
        monkeypatch, tmp_path):
    db, _runner, source, session_id, route, old_epoch, new_epoch, row, _descriptor, authority, scheduled = (
        _recovery_fixture(tmp_path)
    )
    async def check_native_route(_runner, _payload, _target, _available_source, _adapter):
        return source, route

    monkeypatch.setattr('gateway.session_envelope.check_native_route', check_native_route)
    from gateway.runtime_ownership import process_ownership
    from pathlib import Path
    home = Path(db.db_path).parent.resolve()
    process_ownership.reserve([home])
    try:
        result = await authority.recover_native_sessions([(session_id, source, object())])
        assert result == {session_id: 'ready'}
        assert scheduled == [session_id]
        handle = authority._preservation_handles[row['admission_id']]
        assert handle.authority_epoch == new_epoch
        binding = authority._preservation_automation_bindings[row['admission_id']]
        assert binding['descriptor']['authority_epoch'] == old_epoch
        assert binding['row']['owner_epoch'] == old_epoch

        # This is the actual SQLite queued -> started writer transition.  The old producer
        # descriptor remains the witness while the current authority stamps its own epoch.
        claimed = claim_session_input(db, epoch=new_epoch, session_id=session_id)
        authority._preservation_bind_claim(row['admission_id'], claimed)
        rebound = authority._preservation_automation_bindings[row['admission_id']]
        assert rebound['row']['owner_epoch'] == new_epoch
        assert rebound['row']['generation'] == claimed['generation']
    finally:
        process_ownership.release(home)
        db.close()
