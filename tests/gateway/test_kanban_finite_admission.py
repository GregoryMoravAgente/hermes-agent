"""Regression for #138992: dispatched cards retain headless approval semantics."""
import asyncio
from contextlib import closing
import json
from types import SimpleNamespace

import pytest


def test_kanban_submission_enters_single_query_approval_context(tmp_path, monkeypatch):
    from gateway.session_contract import AdmissionReceipt, Principal, SessionRef
    from gateway.session_finite import execute_finite_admission, finite_turn_required, unattended_turn
    from gateway.session_kanban import run_task
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import connect
    from tools.approval_context import _is_single_query_approval_context

    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    for name in ('HERMES_KANBAN_DB', 'HERMES_DELEGATED_CHILD', 'HERMES_SINGLE_QUERY_SESSION'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr('hermes_cli.profiles.resolve_profile_env', lambda name: str(tmp_path))
    workspace = tmp_path / 'workspace'
    workspace.mkdir()
    kb.create_board('owned')
    with closing(connect(board='owned')) as conn:
        task_id = kb.create_task(conn, title='Headless work', assignee='default',
                                workspace_kind='dir', workspace_path=str(workspace))
        kb.recompute_ready(conn)
        task = kb.claim_task(conn, task_id)
    config = {'model': {'default': 'loop-model', 'provider': 'custom'},
              'platform_toolsets': {'cli': ['terminal']}}
    monkeypatch.setattr('gateway.run._load_gateway_config', lambda: config)
    from gateway.session_local_recovery import local_identity
    request_id = 'kanban:' + json.dumps(
        ['owned', task.id, task.current_run_id], separators=(',', ':'))
    ref = SessionRef(str(tmp_path), local_identity(str(tmp_path), 'owner', request_id))
    policies = []

    def create(authority, actor, params, *, trusted_policy, trusted_secrets):
        policies.append(trusted_policy)
        return ref

    monkeypatch.setattr('gateway.session_local.create_local_session', create)
    observed = []

    async def execute(authority, ref, row):
        observed.append((_is_single_query_approval_context(), finite_turn_required(), unattended_turn()))

    monkeypatch.setattr('gateway.session_ingress.execute_admission', execute)

    async def submit(actor, submission, **kwargs):
        await execute_finite_admission(authority, ref, {'admission_id': 'admission', 'payload': submission.payload})
        return AdmissionReceipt('admission', ref, 1, 'queued', None, 1, None)

    async def resume(ref, params):
        pass

    authority = SimpleNamespace(profile_id=str(tmp_path), submit=submit,
                                db=SimpleNamespace(get_session=lambda sid: None, db_path=str(tmp_path / 'state.db')))
    actor = Principal('owner', str(tmp_path), frozenset({'session:create'}), 'transport')
    connection = SimpleNamespace(authority=authority, actor=actor, native_owner=True, resume=resume)
    params = dict(board='owned', task_id=task.id, run_id=task.current_run_id, claim_lock=task.claim_lock)
    result = asyncio.run(run_task(connection, params))
    assert result['session_id'] == ref.session_id
    assert policies[0].source == 'kanban'
    assert observed == [(True, True, False)], 'card turns must be finite without granting unattended auto-approval'
    assert finite_turn_required() is None


@pytest.fixture
def kanban_owner(tmp_path, monkeypatch):
    """A real owner authority and a claimed card on its board; call it with the profile config."""
    import json
    from gateway import run
    from gateway.session_controls import AuthorityConnection
    from hermes_cli import kanban_db as kb
    from hermes_cli.kanban_db_connect import connect
    from tests.gateway.test_session_follow_profile_config import _owner

    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setenv('HERMES_KANBAN_HOME', str(tmp_path))
    for name in ('HERMES_KANBAN_DB', 'HERMES_DELEGATED_CHILD', 'HERMES_SINGLE_QUERY_SESSION', 'HERMES_YOLO_MODE'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(run, '_hermes_home', tmp_path)
    monkeypatch.setattr('hermes_cli.profiles.resolve_profile_env', lambda name: str(tmp_path))

    def build(config):
        (tmp_path / 'config.yaml').write_text(json.dumps(config))
        kb.create_board('owned')
        with closing(connect(board='owned')) as conn:
            task_id = kb.create_task(conn, title='Headless work', assignee='default',
                                    workspace_kind='dir', workspace_path=str(tmp_path))
            kb.recompute_ready(conn)
            task = kb.claim_task(conn, task_id)
        store, runner, authority, dashboard = _owner(tmp_path)
        from gateway.runtime_custody import RuntimeCustodyCoordinator
        from hermes_cli.backend_retirement import RetirementFence
        runner.runtime_custody = RuntimeCustodyCoordinator(
            process_instance=authority.instance_id, authority_epoch=authority.epoch,
            fence=RetirementFence())
        authority.profile_id = str(tmp_path)
        monkeypatch.setattr(authority, '_schedule', lambda ref: None)
        connection = AuthorityConnection(authority, object(), {
            'user_id': 'owner', 'provider': 'local', 'profile_id': authority.profile_id,
            'instance_id': authority.instance_id, 'native_bootstrap': True,
            'capabilities': ['session:create', 'session:read', 'session:submit', 'session:control']})

        async def close():
            await connection.close()
            await dashboard.close()
            store.close_all_db_handles()

        params = dict(board='owned', task_id=task.id, run_id=task.current_run_id, claim_lock=task.claim_lock)
        return SimpleNamespace(runner=runner, authority=authority, connection=connection, params=params, close=close)
    return build


_CONFIG = {'model': {'default': 'loop-model', 'provider': 'custom'}, 'platform_toolsets': {'cli': ['terminal']}}


def _rewrite_kanban_admission_payload(db, row, payload):
    from hermes_state_keys import admission_fingerprint

    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    digest = admission_fingerprint(
        canonical_target=row['target_session_id'],
        payload={'input': payload, 'intent': row['intent']},
    )
    db._execute_write(lambda conn: conn.execute(
        'UPDATE session_admissions SET payload_json=?, payload_digest=? WHERE admission_id=?',
        (encoded, digest, row['admission_id'])))


def _kanban_admission_snapshot(db, admission_id):
    with db._read_ctx() as conn:
        row = conn.execute(
            'SELECT payload_json, payload_digest, status, outcome FROM session_admissions WHERE admission_id=?',
            (admission_id,),
        ).fetchone()
    return tuple(row) if row is not None else None


@pytest.mark.parametrize('mode', ['deny', 'approve'])
def test_durable_kanban_admission_preserves_headless_policy_in_worker(kanban_owner, mode):
    from gateway.session_contract import SessionRef
    from gateway.session_kanban import run_task
    from gateway.session_managed_worker import _bootstrap, worker_turn_scope
    from gateway.session_policy import policy_for_source
    from hermes_state_runtime import list_session_admissions
    from tools.approval import request_tool_approval
    from tools.approval_context import _is_single_query_approval_context

    owner = kanban_owner({**_CONFIG, 'approvals': {'mode': 'smart', 'single_query_mode': mode}})
    authority = owner.authority

    async def scenario():
        try:
            first = await run_task(owner.connection, owner.params)
            retry = await run_task(owner.connection, owner.params)
            assert retry == first
            sid = first['session_id']
            rows = list_session_admissions(authority.db, session_id=sid)
            assert len(rows) == 1
            policy = policy_for_source(owner.runner, authority.sessions[sid].source)
            assert policy.source == 'kanban'
            frame = _bootstrap(authority, SessionRef(authority.profile_id, sid), rows[0], policy,
                               {'execution_id': 'probe'})
            with worker_turn_scope(frame):
                assert _is_single_query_approval_context(), 'persisted card must reach worker as single-query'
                result = request_tool_approval('execute_code', 'headless test',
                    approval_callback=lambda *args: (_ for _ in ()).throw(AssertionError('must not prompt')))
                assert result['approved'] is (mode == 'approve')
                if mode == 'deny':
                    assert 'single' in result['message'].lower()
        finally:
            await owner.close()

    asyncio.run(scenario())


def test_retry_of_pre_finite_kanban_admission_returns_original_receipt(kanban_owner, monkeypatch):
    """An attempt admitted before cards were finite keeps its committed payload on retry: the
    upgraded run_task must return the original receipt, not admission_conflict. The retry reads
    that payload by its exact admission key, never by listing the session's admissions."""
    import hermes_state_runtime
    from gateway.session_kanban import run_task
    from hermes_state_runtime import list_session_admissions

    owner = kanban_owner(_CONFIG)
    authority = owner.authority
    real_submit_i09 = authority._submit_i09_automation

    async def pre_finite_submit_i09(actor, ref, request_id, payload, handle, **kwargs):
        # The payload run_task committed before this change: no finite flag.
        payload = {k: v for k, v in payload.items() if k != 'finite'}
        return await real_submit_i09(actor, ref, request_id, payload, handle, **kwargs)

    def no_listing(*args, **kwargs):
        raise AssertionError('retry must use the exact-key admission lookup')

    async def scenario():
        try:
            monkeypatch.setattr(authority, '_submit_i09_automation', pre_finite_submit_i09)
            first = await run_task(owner.connection, owner.params)
            monkeypatch.setattr(authority, '_submit_i09_automation', real_submit_i09)
            monkeypatch.setattr(hermes_state_runtime, 'list_session_admissions', no_listing)
            retry = await run_task(owner.connection, owner.params)
            assert retry == first
            rows = list_session_admissions(authority.db, session_id=first['session_id'], pending_only=False)
            assert len(rows) == 1 and 'finite' not in rows[0]['payload']
        finally:
            await owner.close()

    asyncio.run(scenario())


def test_kanban_retry_replays_the_same_authenticated_descriptor(kanban_owner):
    """A lost delivery acknowledgement replays the real row and never mints a second root."""
    from gateway.session_kanban import run_task
    from hermes_state_runtime import list_session_admissions

    owner = kanban_owner(_CONFIG)

    async def scenario():
        try:
            first = await run_task(owner.connection, owner.params)
            [row] = list_session_admissions(owner.authority.db, session_id=first['session_id'],
                                             pending_only=False)
            descriptor = row['payload']['_automation_custody_v1']
            assert descriptor['owner_class'] == 'I09_a2a_hosted_kanban'
            assert descriptor['session_id'] == row['target_session_id']
            assert descriptor['principal_id'] == row['principal_id']
            assert descriptor['request_id'] == row['request_id']
            replay = await run_task(owner.connection, owner.params)
            assert replay == first
            rows = list_session_admissions(owner.authority.db, session_id=first['session_id'],
                                            pending_only=False)
            assert len(rows) == 1
            assert rows[0]['payload']['_automation_custody_v1'] == descriptor
        finally:
            await owner.close()

    asyncio.run(scenario())


def test_terminal_kanban_replay_requires_exact_terminal_witness(kanban_owner):
    """A real terminal row replays without reopening custody; copied/stale/unknown witnesses refuse."""
    from dataclasses import replace

    from gateway.session_kanban import run_task
    from hermes_state_runtime import (
        RuntimeStoreError,
        claim_session_input,
        get_session_admission,
        list_session_admissions,
        settle_session_input,
    )

    owner = kanban_owner(_CONFIG)
    authority = owner.authority

    async def scenario():
        try:
            first = await run_task(owner.connection, owner.params)
            queued = get_session_admission(
                authority.db, admission_id=first['receipt']['admission_id'],
            )
            assert queued['status'] == 'queued'
            claimed = claim_session_input(
                authority.db, epoch=authority.epoch,
                session_id=queued['target_session_id'], head=queued['admission_id'],
            )
            assert claimed['status'] == 'started'
            terminal = settle_session_input(
                authority.db, epoch=authority.epoch,
                admission_id=claimed['admission_id'], generation=claimed['generation'],
                outcome='completed', result={'text': 'terminal replay witness'},
            )
            assert terminal['status'] == 'terminal'
            before = _kanban_admission_snapshot(authority.db, terminal['admission_id'])
            handle = authority._preservation_handles[terminal['admission_id']]
            assert authority._preservation_automation_row_matches(
                terminal, handle, allow_terminal=True,
            )

            # All three are typed copies/replacements, not caller-supplied custody booleans.  The
            # row matcher must reject them before any retry effect, even though their fields look
            # close to the exact source-owned handle.
            for invalid_handle in (
                replace(handle, owner_id=f'{handle.owner_id}:replaced'),
                replace(handle, authority_epoch=authority.epoch + 1),
                replace(handle, profile_id=f'{handle.profile_id}:foreign'),
            ):
                assert not authority._preservation_automation_row_matches(
                    terminal, invalid_handle, allow_terminal=True,
                )

            expected_source = terminal['payload']['_automation_custody_v1']['source_identity']
            foreign = dict(terminal)
            foreign['payload'] = dict(terminal['payload'])
            foreign['payload']['_automation_custody_v1'] = dict(
                terminal['payload']['_automation_custody_v1'],
                source_identity='kanban:foreign-terminal',
            )
            with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
                authority._preservation_i09_descriptor_for_row(
                    foreign, source_identity=expected_source,
                )

            stale = dict(terminal, owner_epoch=authority.epoch + 1)
            with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
                authority._preservation_i09_descriptor_for_row(
                    stale, source_identity=expected_source,
                )

            unknown = dict(terminal, status='unknown', generation=None)
            with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
                authority._preservation_i09_descriptor_for_row(
                    unknown, source_identity=expected_source,
                )

            # This is the public retry path over the exact source-produced terminal row.  It must
            # return the existing admission, not create a new root or execute the card again.
            replay = await run_task(owner.connection, owner.params)
            assert replay['session_id'] == first['session_id']
            assert replay['receipt']['admission_id'] == first['receipt']['admission_id']
            assert replay['receipt']['status'] == 'terminal'
            assert _kanban_admission_snapshot(authority.db, terminal['admission_id']) == before
            assert len(list_session_admissions(
                authority.db, session_id=terminal['target_session_id'], pending_only=False,
            )) == 1
        finally:
            await owner.close()

    asyncio.run(scenario())


def test_i09_admission_stamps_the_existing_managed_worker_descriptor(kanban_owner):
    """The trusted I09 producer uses the same managed route marker as local submission."""
    from gateway.session_kanban import run_task
    from gateway.session_operator import MANAGED_WORKER_DESCRIPTOR, MANAGED_WORKER_FIELD
    from hermes_state_runtime import list_session_admissions

    owner = kanban_owner(_CONFIG)

    async def scenario():
        try:
            first = await run_task(owner.connection, owner.params)
            [row] = list_session_admissions(owner.authority.db, session_id=first['session_id'],
                                             pending_only=False)
            assert row['payload'][MANAGED_WORKER_FIELD] == MANAGED_WORKER_DESCRIPTOR
            assert '_automation_custody_v1' in row['payload']
        finally:
            await owner.close()

    asyncio.run(scenario())


def test_i09_missing_managed_descriptor_stays_refused_before_worker_route(kanban_owner):
    """A copied/missing route marker cannot turn an I09 row into a managed worker admission."""
    from gateway.runtime_service_custody import ServiceCustodyError
    from gateway.session_contract import SessionRef
    from gateway.session_ingress import _bind_process_route
    from gateway.session_kanban import run_task
    from gateway.session_operator import MANAGED_WORKER_FIELD
    from hermes_state_runtime import RuntimeStoreError, list_session_admissions

    owner = kanban_owner(_CONFIG)

    async def scenario():
        try:
            first = await run_task(owner.connection, owner.params)
            [row] = list_session_admissions(owner.authority.db, session_id=first['session_id'],
                                             pending_only=False)
            missing = dict(row, payload={key: value for key, value in row['payload'].items()
                                         if key != MANAGED_WORKER_FIELD})
            with pytest.raises(ServiceCustodyError, match='managed_producer_binding_missing'):
                _bind_process_route(owner.authority,
                                    SessionRef(owner.authority.profile_id, first['session_id']),
                                    missing, require_managed=True)
            with pytest.raises(RuntimeStoreError, match='invalid_params'):
                await owner.authority._submit_i09_automation(
                    owner.connection.actor,
                    SessionRef(owner.authority.profile_id, first['session_id']),
                    'kanban:forged',
                    {'text': 'x', MANAGED_WORKER_FIELD: {'producer_process_mode': 'managed_turn'}},
                    None,
                    source_identity='kanban:forged',
                )
        finally:
            await owner.close()

    asyncio.run(scenario())


def test_pre_r27_markerless_i09_replay_preserves_canonical_payload(kanban_owner):
    """A legacy row retries by its original digest; managed routing is not a payload migration."""
    from gateway.session_kanban import run_task
    from gateway.session_operator import MANAGED_WORKER_FIELD
    from hermes_state_runtime import list_session_admissions

    owner = kanban_owner(_CONFIG)

    async def scenario():
        try:
            first = await run_task(owner.connection, owner.params)
            [row] = list_session_admissions(owner.authority.db, session_id=first['session_id'],
                                             pending_only=False)
            legacy_payload = dict(row['payload'])
            legacy_payload.pop(MANAGED_WORKER_FIELD, None)
            _rewrite_kanban_admission_payload(owner.authority.db, row, legacy_payload)
            before = _kanban_admission_snapshot(owner.authority.db, row['admission_id'])
            retry = await run_task(owner.connection, owner.params)
            after = _kanban_admission_snapshot(owner.authority.db, row['admission_id'])
            assert retry == first
            assert after == before
            [replayed] = list_session_admissions(owner.authority.db, session_id=first['session_id'],
                                                 pending_only=False)
            assert MANAGED_WORKER_FIELD not in replayed['payload']
        finally:
            await owner.close()

    asyncio.run(scenario())


def test_pre_r27_markerless_i09_replay_refuses_foreign_source(kanban_owner):
    """A markerless legacy row with a copied source identity never becomes a managed retry."""
    from gateway.session_kanban import run_task
    from gateway.session_operator import MANAGED_WORKER_FIELD
    from hermes_state_runtime import RuntimeStoreError, list_session_admissions

    owner = kanban_owner(_CONFIG)

    async def scenario():
        try:
            first = await run_task(owner.connection, owner.params)
            [row] = list_session_admissions(owner.authority.db, session_id=first['session_id'],
                                             pending_only=False)
            legacy_payload = dict(row['payload'])
            legacy_payload.pop(MANAGED_WORKER_FIELD, None)
            legacy_payload['_automation_custody_v1'] = dict(
                legacy_payload['_automation_custody_v1'], source_identity='kanban:foreign-row')
            _rewrite_kanban_admission_payload(owner.authority.db, row, legacy_payload)
            with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
                await run_task(owner.connection, owner.params)
            [retained] = list_session_admissions(owner.authority.db, session_id=first['session_id'],
                                                 pending_only=False)
            assert retained['payload'] == legacy_payload
        finally:
            await owner.close()

    asyncio.run(scenario())


def test_kanban_refuses_a_copied_descriptor_before_retry_effect(kanban_owner):
    """A copied source witness stays unknown instead of becoming a new Kanban admission."""
    import hashlib
    import json

    from gateway.session_kanban import run_task
    from hermes_state_runtime import RuntimeStoreError, list_session_admissions

    owner = kanban_owner(_CONFIG)

    async def scenario():
        try:
            first = await run_task(owner.connection, owner.params)
            [row] = list_session_admissions(owner.authority.db, session_id=first['session_id'],
                                             pending_only=False)
            payload = dict(row['payload'])
            payload['_automation_custody_v1'] = dict(
                payload['_automation_custody_v1'], source_identity='copied-from-foreign-row')
            encoded = json.dumps(payload, sort_keys=True, separators=(',', ':'))
            owner.authority.db._execute_write(lambda conn: conn.execute(
                'UPDATE session_admissions SET payload_json=?, payload_digest=? WHERE admission_id=?',
                (encoded, hashlib.sha256(encoded.encode()).hexdigest(), row['admission_id'])))
            with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
                await run_task(owner.connection, owner.params)
            assert len(list_session_admissions(owner.authority.db, session_id=first['session_id'],
                                               pending_only=False)) == 1
        finally:
            await owner.close()

    asyncio.run(scenario())


def test_admission_lookup_matches_only_the_exact_durable_key(kanban_owner):
    """The shared lookup is the UNIQUE (principal, target, request) key: it returns the same row
    the session listing holds, and nothing for another principal, target or request id."""
    from gateway.session_kanban import run_task
    from hermes_state_runtime import find_session_admission, list_session_admissions

    owner = kanban_owner(_CONFIG)
    db, principal = owner.authority.db, owner.connection.actor.subject

    async def scenario():
        try:
            sid = (await run_task(owner.connection, owner.params))['session_id']
            [listed] = list_session_admissions(db, session_id=sid, pending_only=False)
            key = dict(principal_id=principal, session_id=sid, request_id=listed['request_id'])
            assert find_session_admission(db, **key) == listed
            for wrong in ({'principal_id': 'someone-else'}, {'session_id': 'other-session'},
                          {'request_id': 'kanban:other'}):
                assert find_session_admission(db, **{**key, **wrong}) is None
        finally:
            await owner.close()

    asyncio.run(scenario())


def _lose_across_restart(authority, sid):
    """Claim the card's admission under this owner, then a new owner epoch boots on the same store
    and runs the real boot recovery (fence ``unknown``, then settle the unattended ones)."""
    from gateway.session_unattended_unknown import settle_recovered_unattended
    from hermes_state_runtime import (begin_runtime_epoch, claim_session_input, list_session_admissions,
                                      recover_session_inputs)
    db = authority.db
    claimed = claim_session_input(db, epoch=authority.epoch, session_id=sid)
    assert claimed and claimed['status'] == 'started'
    epoch = begin_runtime_epoch(db, instance_id='owner-2')
    assert recover_session_inputs(db, epoch=epoch) == 1
    settle_recovered_unattended(db, epoch)
    [row] = list_session_admissions(db, session_id=sid, pending_only=False)
    return row


def test_kanban_card_fenced_unknown_across_owner_epochs_settles_interrupted(kanban_owner):
    """#134889 KB-8: a card attempt lost across an owner restart has nobody to resend it; the
    dispatcher's trusted marker makes boot recovery settle it interrupted instead of pausing."""
    from gateway.session_kanban import run_task

    owner = kanban_owner(_CONFIG)

    async def scenario():
        try:
            sid = (await run_task(owner.connection, owner.params))['session_id']
            row = _lose_across_restart(owner.authority, sid)
            assert (row['status'], row['outcome']) == ('terminal', 'interrupted'), row
            notices = [m['content'] for m in owner.authority.db.get_messages(sid)
                       if m.get('display_kind') == 'failed_turn']
            assert notices and 'automat' in notices[0].lower(), notices
        finally:
            await owner.close()

    asyncio.run(scenario())


def test_card_admitted_before_the_marker_keeps_pausing_and_retries_cleanly(kanban_owner, monkeypatch):
    """Upgrade compat: an attempt main committed without the marker resubmits its exact payload on
    retry (original receipt, no admission_conflict) and keeps today's pause when lost."""
    from gateway.session_kanban import run_task

    owner = kanban_owner(_CONFIG)
    authority = owner.authority
    real_submit = authority._submit_i09_automation

    async def unmarked_submit(actor, ref, request_id, payload, handle, **kwargs):
        kwargs.pop('kanban_card', None)  # the admission main committed: no producer marker
        return await real_submit(actor, ref, request_id, payload, handle, **kwargs)

    async def scenario():
        try:
            monkeypatch.setattr(authority, '_submit_i09_automation', unmarked_submit)
            first = await run_task(owner.connection, owner.params)
            monkeypatch.setattr(authority, '_submit_i09_automation', real_submit)
            assert await run_task(owner.connection, owner.params) == first
            row = _lose_across_restart(authority, first['session_id'])
            assert row['status'] == 'unknown', row
        finally:
            await owner.close()

    asyncio.run(scenario())


def test_a_client_cannot_mint_the_kanban_card_marker(kanban_owner):
    """The marker is the dispatcher's, never a client payload field: session.submit and the
    authority's public payload both refuse it, so a person's turn can't opt out of the pause."""
    from gateway.session_contract import Submission
    from gateway.session_kanban import run_task
    from hermes_state_runtime import RuntimeStoreError

    owner = kanban_owner(_CONFIG)
    authority = owner.authority

    async def scenario():
        try:
            sid = (await run_task(owner.connection, owner.params))['session_id']
            from gateway.session_contract import SessionRef
            ref = SessionRef(authority.profile_id, sid)
            forged = {'text': 'x', 'kanban_card_v1': {'task_id': owner.params['task_id'], 'run_id': owner.params['run_id']}}
            with pytest.raises(RuntimeStoreError) as refused:
                await authority.submit(owner.connection.actor, Submission('forged', ref, forged, 'queue'))
            assert refused.value.reason == 'invalid_params'
            with pytest.raises(RuntimeStoreError) as refused:
                owner.connection.subscriptions[sid] = object()
                owner.connection.submission(ref, {'session_id': sid, 'submission_id': 'f2', **forged})
            assert refused.value.reason == 'invalid_params'
        finally:
            await owner.close()

    asyncio.run(scenario())
