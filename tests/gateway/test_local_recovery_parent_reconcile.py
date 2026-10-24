"""Causal controls for the queued LOCAL I10 parent-reconciliation proposal.

The positive test delegates to the existing managed daemon journey so its original assertions,
restart boundary, three-follower FIFO, provider requests, and durable transcript checks remain
unchanged.  The smaller controls use the real managed authority, real SQLite rows, real
``ensure_recovery_root`` leases, and the real coordinator; the row edits only model the durable
crash/resolve boundary so each refusal can be checked without a second scheduler implementation.
"""
import copy
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace

import pytest


@pytest.mark.platforms('linux')
def test_resolved_unknown_reconciles_three_real_local_followers(tmp_path):
    """The unchanged journey remains the end-to-end positive and original oracle bank."""
    from tests.gateway.test_journey_compress_restart_fifo import (
        test_followers_survive_compression_and_restart_in_order_exactly_once,
    )

    test_followers_survive_compression_and_restart_in_order_exactly_once(tmp_path, 'crash')


@asynccontextmanager
async def _managed_local_state(tmp_path, monkeypatch):
    """Build the actual managed profile boundary used by the direct refusal controls."""
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from tests.fakes.fake_llm_provider import FakeLLMServer, write_hermes_home

    home = (tmp_path / 'state').resolve()
    user = (tmp_path / 'user').resolve()
    user.mkdir()
    monkeypatch.setenv('HOME', str(user))
    monkeypatch.setenv('USERPROFILE', str(user))
    monkeypatch.setenv('HERMES_HOME', str(home))
    import hermes_state
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', hermes_state._IMPORT_DEFAULT_DB_PATH)
    home_token = None
    runner = None
    reserved = False
    with FakeLLMServer(default_text='reconcile-fixture') as provider:
        write_hermes_home(home, provider.base_url)
        process_ownership.reserve([home])
        reserved = True
        home_token = set_hermes_home_override(home)
        try:
            runner = GatewayRunner(GatewayConfig(sessions_dir=home / 'sessions'))
            await initialize_gateway_runtime(runner)
            yield runner.session_authority
        finally:
            if runner is not None:
                runner.session_store.close_all_db_handles()
                runner.close_all_session_db_handles()
            if reserved:
                process_ownership.release(home)
            if home_token is not None:
                reset_hermes_home_override(home_token)


def _seed_unknown_head(authority, *, unknown_follower=False):
    from gateway.session_contract import Principal
    from gateway.session_local import create_local_session
    from gateway.session_recovery_custody import ensure_recovery_root
    from hermes_state_runtime import admit_session_input

    actor = Principal(
        'local-owner', authority.profile_id,
        frozenset({'session:create', 'session:read', 'session:submit', 'session:control'}),
        'proposal-fixture',
    )
    ref = create_local_session(authority, actor, {
        'request_id': 'reconcile-root', 'source': 'gui', 'cwd': str(authority.db.db_path.parent),
        'model': 'frozen', 'toolsets': [],
    })
    rows = [admit_session_input(
        authority.db, epoch=authority.epoch, principal_id=actor.subject,
        session_id=ref.session_id, request_id=request_id,
        payload={'text': request_id},
    ) for request_id in ('RESOLVED_HEAD', 'FOLLOWER_1', 'FOLLOWER_2', 'FOLLOWER_3')]
    handles = tuple(
        ensure_recovery_root(authority, row, consumer='local_recovery') for row in rows
    )
    coordinator = authority._preservation_coordinator()
    for handle in handles:
        coordinator.mark_unknown(handle, 'crash_boundary')
    authority.db._execute_write(lambda conn: conn.execute(
        "UPDATE session_admissions SET status='terminal', outcome='interrupted' WHERE admission_id=?",
        (rows[0]['admission_id'],),
    ))
    if unknown_follower:
        authority.db._execute_write(lambda conn: conn.execute(
            "UPDATE session_admissions SET status='unknown' WHERE admission_id=?",
            (rows[1]['admission_id'],),
        ))
    return SimpleNamespace(
        authority=authority, ref=ref, rows=rows, handles=handles, coordinator=coordinator,
        resolved_id=rows[0]['admission_id'], follower_ids=tuple(row['admission_id'] for row in rows[1:]),
    )


@pytest.mark.asyncio
async def test_other_unknown_keeps_all_queued_local_parents_unknown(tmp_path, monkeypatch):
    from gateway.session_local_recovery import reconcile_local_parents_after_unknown

    async with _managed_local_state(tmp_path, monkeypatch) as authority:
        state = _seed_unknown_head(authority, unknown_follower=True)
        assert reconcile_local_parents_after_unknown(
            authority, state.ref.session_id, state.resolved_id,
        ) is False
        assert all(
            state.coordinator._records[handle.lease.lease_id].state == 'unknown'
            for handle in state.handles[1:]
        )


def _copy_local_handle(state):
    admission_id = state.follower_ids[0]
    state.authority._preservation_handles[admission_id] = copy.copy(state.handles[1])


def _replace_local_binding(state):
    state.authority._i10_recovery_bindings[state.follower_ids[0]]['handle'] = state.handles[2]


def _stale_local_binding(state):
    binding = state.authority._i10_recovery_bindings[state.follower_ids[0]]
    stale = copy.copy(binding['identity'])
    binding['identity'] = stale.__class__(**{**vars(stale), 'authority_epoch': stale.authority_epoch - 1})


def _foreign_local_payload(state):
    from hermes_state_runtime import get_session_admission
    admission_id = state.follower_ids[0]
    row = get_session_admission(state.authority.db, admission_id=admission_id)
    payload = dict(row['payload'])
    payload['native_text_v1'] = {'route': row['target_session_id'], 'source': {}, 'event': {}}
    state.authority.db._execute_write(lambda conn: conn.execute(
        'UPDATE session_admissions SET payload_json=? WHERE admission_id=?',
        (json.dumps(payload), admission_id),
    ))


def _close_local_source(state):
    state.authority.pending_closes = {state.ref.session_id: object()}


_RECONCILIATION_MUTATIONS = {
    'copied_handle': _copy_local_handle,
    'replaced_binding': _replace_local_binding,
    'stale_binding': _stale_local_binding,
    'foreign_payload': _foreign_local_payload,
    'closed_source': _close_local_source,
}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'mutation',
    ['copied_handle', 'replaced_binding', 'stale_binding', 'foreign_payload', 'closed_source'],
)
async def test_local_parent_reconcile_refuses_identity_and_source_mutations(
        tmp_path, monkeypatch, mutation):
    from gateway.session_local_recovery import reconcile_local_parents_after_unknown

    async with _managed_local_state(tmp_path, monkeypatch) as authority:
        state = _seed_unknown_head(authority)
        _RECONCILIATION_MUTATIONS[mutation](state)
        assert reconcile_local_parents_after_unknown(
            authority, state.ref.session_id, state.resolved_id,
        ) is False
        assert all(
            state.coordinator._records[handle.lease.lease_id].state == 'unknown'
            for handle in state.handles[1:]
        )
