"""Real SQLite caller routes bind I08/I09 roots before the FIFO can claim them."""

from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_i08_automation_row_keeps_root_for_the_real_fifo_claim(owner, monkeypatch):
    from gateway.config import Platform
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session import SessionSource
    from gateway.session_contract import Principal
    from gateway.session_local import LocalSessionAdapter, create_local_session
    from gateway.run_heartbeat_acceptance import admit_heartbeat
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_cli.heartbeat import HeartbeatState, save_heartbeat
    from hermes_state_runtime import list_session_admissions

    owner.runner.adapters = {Platform.LOCAL: LocalSessionAdapter(owner)}
    owner.runner._adapter_for_source = lambda _source: owner.runner.adapters[Platform.LOCAL]
    owner.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=owner.instance_id, authority_epoch=owner.epoch, fence=RetirementFence())
    owner._schedule = lambda _ref: None
    source = SessionSource(platform=Platform.LOCAL, chat_id='caller-i08', user_id='operator')

    actor = Principal('operator', owner.profile_id,
                      frozenset({'session:create', 'session:read', 'session:submit'}), 'caller-test')
    import pathlib
    monkeypatch.setenv('HERMES_HOME', str(pathlib.Path(owner.db.db_path).parent))
    ref = create_local_session(owner, actor, {
        'request_id': 'caller-i08-session', 'source': 'cli', 'model': 'fixture',
        'toolsets': [], 'cwd': str(owner.db.db_path.parent),
    })
    live = owner.sessions[ref.session_id]
    source = live.source
    save_heartbeat(ref.session_id, HeartbeatState(prompt='I08 caller wake', interval_seconds=60, created_at=1))
    await admit_heartbeat(owner.runner, owner.runner.adapters[Platform.LOCAL], source,
                          ref.session_id, live.route)

    [row] = list_session_admissions(owner.db, session_id=ref.session_id, pending_only=False)
    assert row['payload']['local_automation_v1']['custody']['owner_class'] == 'I08_goals_loops_heartbeat'

    claim_next = owner._claim_next

    async def observe_claim(claim_ref, claim_live):
        claimed, first = await claim_next(claim_ref, claim_live)
        if claimed:
            assert first['admission_id'] == row['admission_id']
            assert row['admission_id'] in owner._preservation_children
        return claimed, first

    async def execute(_authority, _ref, _row):
        return 'heartbeat completed'

    monkeypatch.setattr(owner, '_claim_next', observe_claim)
    monkeypatch.setattr('gateway.session_finite.execute_finite_admission', execute)
    await owner._drain(ref)
    [settled] = list_session_admissions(owner.db, session_id=ref.session_id, pending_only=False)
    assert settled['status'] == 'terminal'
    assert owner.preservation_owner_ready('I02_api_session_turn') is True
    owner._preservation_release_terminal(row['admission_id'])
    assert owner._preservation_coordinator().status()['active_owner_classes'] == []
    from gateway.run_runtime import _seal_integrated_preservation_owners
    coordinator = owner._preservation_coordinator()
    assert owner.preservation_owner_ready('I07_automation_route_wakes') is False
    close = coordinator.begin_close(owner_id='heartbeat-is-not-route-wake')
    try:
        _seal_integrated_preservation_owners(owner.runner, coordinator, close)
        assert coordinator.status()['owner_states']['I07_automation_route_wakes'] != 'sealed'
    finally:
        assert coordinator.abort_before_handoff(close) is None


@pytest.mark.asyncio
@pytest.mark.parametrize('missing_field', ['process_instance', 'coordinator_id'])
async def test_i08_claim_refuses_an_incomplete_custody_descriptor(owner, monkeypatch, missing_field):
    """The live heartbeat producer cannot claim a row whose durable owner tuple is incomplete."""
    import hashlib
    import json
    import pathlib

    from gateway.config import Platform
    from gateway.run_heartbeat_acceptance import admit_heartbeat
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session_contract import Principal
    from gateway.session_local import LocalSessionAdapter, create_local_session
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_cli.heartbeat import HeartbeatState, save_heartbeat
    from hermes_state_runtime import RuntimeStoreError, get_session_admission

    adapter = LocalSessionAdapter(owner)
    owner.runner.adapters = {Platform.LOCAL: adapter}
    owner.runner._adapter_for_source = lambda _source: adapter
    owner.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=owner.instance_id, authority_epoch=owner.epoch, fence=RetirementFence())
    owner._schedule = lambda _ref: None
    monkeypatch.setenv('HERMES_HOME', str(pathlib.Path(owner.db.db_path).parent))
    actor = Principal('operator', owner.profile_id,
                      frozenset({'session:create', 'session:read', 'session:submit'}), 'caller-i08-incomplete')
    ref = create_local_session(owner, actor, {
        'request_id': 'caller-i08-incomplete-session', 'source': 'cli', 'model': 'fixture',
        'toolsets': [], 'cwd': str(owner.db.db_path.parent),
    })
    live = owner.sessions[ref.session_id]
    save_heartbeat(ref.session_id, HeartbeatState(
        prompt='I08 incomplete custody', interval_seconds=60, created_at=1))
    await admit_heartbeat(owner.runner, adapter, live.source, ref.session_id, live.route)
    [row] = owner.db._read_all(
        'SELECT admission_id,payload_json FROM session_admissions WHERE target_session_id=?',
        (ref.session_id,))
    payload = json.loads(row['payload_json'])
    del payload['local_automation_v1']['custody'][missing_field]
    encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'))
    owner.db._execute_write(lambda conn: conn.execute(
        'UPDATE session_admissions SET payload_json=?, payload_digest=? WHERE admission_id=?',
        (encoded, hashlib.sha256(encoded.encode()).hexdigest(), row['admission_id'])))

    with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
        await owner._claim_next(ref, live)
    claimed = get_session_admission(owner.db, admission_id=row['admission_id'])
    # The SQLite claim is already committed when the execution stamp performs its typed
    # descriptor check.  Its durable status therefore remains started; the exact parent and
    # child leases carry the sticky unknown state and no execution event was published.
    assert claimed['status'] == 'started'
    assert claimed['generation'] >= 1
    assert owner.sessions[ref.session_id].event_stream.execution == {}
    coordinator = owner._preservation_coordinator()
    child = owner._preservation_children[row['admission_id']]
    parent = owner._preservation_handles[row['admission_id']]
    assert coordinator._records[child.lease.lease_id].state == 'unknown'
    assert coordinator._records[parent.lease.lease_id].state == 'unknown'


@pytest.mark.asyncio
async def test_i09_a2a_retry_reuses_one_durable_row_and_same_root(tmp_path, monkeypatch):
    from gateway import run
    from gateway.config import GatewayConfig
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session import SessionStore
    from gateway.session_authority import initialize_session_authority
    from gateway.session_controls import AuthorityConnection
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_state_runtime import get_session_admission

    monkeypatch.setattr(run, '_load_gateway_config', lambda: {
        'model': {'default': 'fixture'}, 'platform_toolsets': {'cli': []},
    })
    monkeypatch.setattr(run, '_resolve_gateway_model', lambda _config: 'fixture')
    store = SessionStore(tmp_path / 'sessions', GatewayConfig())
    runner = run.GatewayRunner.__new__(run.GatewayRunner)
    runner.session_store = store
    runner._session_db = store._db
    runner.adapters = {}
    runner._draining = False
    authority = await initialize_session_authority(runner, profile_id='target', instance_id='caller-i09')
    runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id, authority_epoch=authority.epoch, fence=RetirementFence())
    authority._schedule = lambda _ref: None
    conn = AuthorityConnection(authority, SimpleNamespace(write=lambda _frame: None), {'user_id': 'producer'})
    params = {
        'agent': 'dev', 'tenant': 'team', 'peer': 'alice', 'context_id': 'caller-i09',
        'input_id': 'same-input', 'text': 'hello',
    }
    try:
        first = await conn.dispatch({'id': 1, 'method': 'a2a.forward', 'params': params})
        second = await conn.dispatch({'id': 2, 'method': 'a2a.forward', 'params': params})
        assert 'result' in first and 'result' in second
        assert second['result']['admission_id'] == first['result']['admission_id']
        admission_id = first['result']['admission_id']
        row = get_session_admission(authority.db, admission_id=admission_id)
        descriptor = row['payload']['_automation_custody_v1']
        binding = authority._preservation_automation_bindings[admission_id]
        assert descriptor['owner_class'] == 'I09_a2a_hosted_kanban'
        assert descriptor['principal_id'] == row['principal_id']
        assert descriptor['session_id'] == row['target_session_id']
        assert descriptor['request_id'] == 'same-input'
        assert binding['handle'].owner_class == 'I09_a2a_hosted_kanban'
        assert binding['descriptor'] == descriptor
        assert binding['row']['request_id'] == 'same-input'
        from gateway.session_contract import SessionRef
        ref = SessionRef(authority.profile_id, row['target_session_id'])
        claim_next = authority._claim_next

        async def observe_claim(claim_ref, claim_live):
            claimed, first_row = await claim_next(claim_ref, claim_live)
            if claimed:
                assert first_row['admission_id'] == admission_id
            return claimed, first_row

        async def execute(_authority, _ref, _row):
            return 'a2a completed'

        monkeypatch.setattr(authority, '_claim_next', observe_claim)
        monkeypatch.setattr('gateway.session_finite.execute_finite_admission', execute)
        await authority._drain(ref)
        assert get_session_admission(authority.db, admission_id=admission_id)['status'] == 'terminal'
        assert authority.preservation_owner_ready('I02_api_session_turn') is True
        authority._preservation_release_terminal(admission_id)
        assert authority._preservation_coordinator().status()['active_owner_classes'] == []
        from gateway.run_runtime import _seal_integrated_preservation_owners
        coordinator = authority._preservation_coordinator()
        assert authority.preservation_owner_ready('I07_automation_route_wakes') is False
        close = coordinator.begin_close(owner_id='a2a-is-not-route-wake')
        try:
            _seal_integrated_preservation_owners(authority.runner, coordinator, close)
            assert coordinator.status()['owner_states']['I07_automation_route_wakes'] != 'sealed'
        finally:
            assert coordinator.abort_before_handoff(close) is None
    finally:
        await conn.close()
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_i09_a2a_refuses_before_generation_without_runtime_coordinator(tmp_path, monkeypatch):
    from gateway import run
    from gateway.config import GatewayConfig
    from gateway.session import SessionStore
    from gateway.session_authority import initialize_session_authority
    from gateway.session_controls import AuthorityConnection
    from hermes_state_runtime import RuntimeStoreError

    monkeypatch.setattr(run, '_load_gateway_config', lambda: {
        'model': {'default': 'fixture'}, 'platform_toolsets': {'cli': []},
    })
    monkeypatch.setattr(run, '_resolve_gateway_model', lambda _config: 'fixture')
    store = SessionStore(tmp_path / 'sessions', GatewayConfig())
    runner = run.GatewayRunner.__new__(run.GatewayRunner)
    runner.session_store = store
    runner._session_db = store._db
    runner.adapters = {}
    runner._draining = False
    authority = await initialize_session_authority(runner, profile_id='target', instance_id='caller-i09-missing')
    conn = AuthorityConnection(
        authority, SimpleNamespace(write=lambda _frame: None),
        {'user_id': 'producer', 'instance_id': authority.instance_id},
    )
    try:
        response = await conn.dispatch({'id': 1, 'method': 'a2a.forward', 'params': {
            'agent': 'dev', 'tenant': 'team', 'peer': 'alice', 'context_id': 'missing',
            'input_id': 'missing-input', 'text': 'hello',
        }})
        assert response['error']['message'] == RuntimeStoreError('runtime_custody_unavailable').reason
        assert authority.sessions == {}
    finally:
        await conn.close()
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_i08_heartbeat_refuses_before_admission_without_runtime_coordinator(owner, monkeypatch):
    from gateway.config import Platform
    from gateway.run_heartbeat_acceptance import admit_heartbeat
    from gateway.session_contract import Principal
    from gateway.session_local import LocalSessionAdapter, create_local_session
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_cli.heartbeat import HeartbeatState, save_heartbeat
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_state_runtime import RuntimeStoreError, list_session_admissions

    adapter = LocalSessionAdapter(owner)
    owner.runner.adapters = {Platform.LOCAL: adapter}
    owner.runner._adapter_for_source = lambda _source: adapter
    owner.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=owner.instance_id, authority_epoch=owner.epoch, fence=RetirementFence())
    actor = Principal('operator', owner.profile_id,
                      frozenset({'session:create', 'session:read', 'session:submit'}), 'caller-i08-missing')
    ref = create_local_session(owner, actor, {
        'request_id': 'caller-i08-missing-session', 'source': 'cli', 'model': 'fixture',
        'toolsets': [], 'cwd': str(owner.db.db_path.parent),
    })
    live = owner.sessions[ref.session_id]
    save_heartbeat(ref.session_id, HeartbeatState(
        prompt='I08 missing coordinator', interval_seconds=60, created_at=1))
    del owner.runner.runtime_custody
    with pytest.raises(RuntimeStoreError, match='runtime_custody_unavailable'):
        await admit_heartbeat(owner.runner, adapter, live.source, ref.session_id, live.route)
    assert list_session_admissions(owner.db, session_id=ref.session_id, pending_only=False) == []


@pytest.mark.asyncio
async def test_i08_automation_refuses_before_snapshot_without_runtime_coordinator(owner):
    from gateway.config import Platform
    from gateway.platforms.event import MessageEvent
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session_automation import admit_automation
    from gateway.session_contract import Principal
    from gateway.session_local import LocalSessionAdapter, create_local_session
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_state_runtime import RuntimeStoreError, list_session_admissions

    adapter = LocalSessionAdapter(owner)
    owner.runner.adapters = {Platform.LOCAL: adapter}
    owner.runner._adapter_for_source = lambda _source: adapter
    owner.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=owner.instance_id, authority_epoch=owner.epoch, fence=RetirementFence())
    actor = Principal('operator', owner.profile_id,
                      frozenset({'session:create', 'session:read', 'session:submit'}), 'caller-automation-missing')
    ref = create_local_session(owner, actor, {
        'request_id': 'caller-automation-missing-session', 'source': 'cli', 'model': 'fixture',
        'toolsets': [], 'cwd': str(owner.db.db_path.parent),
    })
    live = owner.sessions[ref.session_id]
    event = MessageEvent(
        text='automation without coordinator', source=live.source, internal=True,
        message_id='caller-automation-missing',
    )
    del owner.runner.runtime_custody
    with pytest.raises(RuntimeStoreError, match='runtime_custody_unavailable'):
        await admit_automation(owner, adapter, event, 'caller-automation-missing')
    assert list_session_admissions(owner.db, session_id=ref.session_id, pending_only=False) == []


@pytest.mark.asyncio
async def test_i09_kanban_refuses_before_session_creation_without_runtime_coordinator(owner):
    from gateway.session_controls import AuthorityConnection
    from gateway.session_kanban import run_task
    from hermes_state_runtime import RuntimeStoreError

    connection = AuthorityConnection(
        owner, SimpleNamespace(write=lambda _frame: None),
        {'user_id': 'operator', 'instance_id': owner.instance_id, 'native_bootstrap': True},
    )
    del owner.runner.runtime_custody
    with pytest.raises(RuntimeStoreError, match='runtime_custody_unavailable'):
        await run_task(connection, {
            'board': 'missing-board', 'task_id': 'missing-task', 'run_id': 1,
            'claim_lock': 'missing-lock',
        })
    assert owner.sessions == {}
    await connection.close()


@pytest.mark.asyncio
async def test_i07_plugin_refuses_before_admission_without_runtime_coordinator(monkeypatch):
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.run import GatewayRunner
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session_authority import initialize_session_authority
    from hermes_cli.backend_retirement import RetirementFence
    from plugins.platforms.discord.adapter import DiscordAdapter
    from hermes_state_runtime import list_session_admissions

    monkeypatch.setenv('DISCORD_ALLOWED_USERS', '42')
    runner = GatewayRunner(GatewayConfig())
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token='fixture-token', typing_indicator=False))
    adapter.gateway_runner = runner
    runner.adapters = {Platform.DISCORD: adapter}
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='caller-i07')
    runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id, authority_epoch=authority.epoch, fence=RetirementFence())
    source = adapter.build_source(chat_id='42', chat_type='dm', user_id='42')
    key = runner.session_store._generate_session_key(source)
    entry = runner.session_store.get_or_create_session(source)
    assert entry is not None
    before = list_session_admissions(authority.db, session_id=entry.session_id, pending_only=False)
    del runner.runtime_custody
    assert await runner._admit_plugin_injection(
        adapter, source, session_key=key, session_id=entry.session_id,
        content='plugin without coordinator', plugin_id='desk') is False
    after = list_session_admissions(authority.db, session_id=entry.session_id, pending_only=False)
    assert after == before


@pytest.mark.asyncio
async def test_i09_private_carrier_is_rejected_by_public_submission(owner):
    """Only the typed A2A/Kanban writer may place the private custody carrier in a row."""
    from gateway.session_contract import Principal, Submission
    from gateway.session_local import create_local_session
    from hermes_state_runtime import RuntimeStoreError, list_session_admissions

    owner.runner.adapters = {}
    actor = Principal('operator', owner.profile_id,
                      frozenset({'session:create', 'session:read', 'session:submit'}), 'caller-i09-public')
    ref = create_local_session(owner, actor, {
        'request_id': 'caller-i09-public-session', 'source': 'cli', 'model': 'fixture',
        'toolsets': [], 'cwd': str(owner.db.db_path.parent),
    })
    before = list_session_admissions(owner.db, session_id=ref.session_id, pending_only=False)
    payload = {'text': 'private carrier', '_automation_custody_v1': {'owner_class': 'I09_a2a_hosted_kanban'}}
    with pytest.raises(RuntimeStoreError, match='invalid_params'):
        await owner.submit(actor, Submission('caller-i09-public', ref, payload, 'queue'))
    assert list_session_admissions(owner.db, session_id=ref.session_id, pending_only=False) == before
