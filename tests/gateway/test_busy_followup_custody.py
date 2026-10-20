"""Busy follow-ups must be children of the exact automation admission lease."""

import pytest


def _install_custody(authority):
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_cli.backend_retirement import RetirementFence

    authority.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id,
        authority_epoch=authority.epoch,
        fence=RetirementFence(),
    )
    return authority.runner.runtime_custody


def test_busy_followup_uses_the_exact_automation_parent_and_rejects_a_copy(owner):
    coordinator = _install_custody(owner)
    root = owner._preservation_automation_root(
        request_id='wake-1', principal_id='automation:route', session_id='session-1',
        source_identity='source-1', delivery_id='delivery-1',
    )
    child = owner._preservation_operation_start(
        'I06_busy_followup_delivery', principal_id='automation:route', session_id='session-1',
        operation='follow-up', request_id='wake-1', generation='pre-admission',
        source_identity='source-1', parent_handle=root,
    )

    assert owner._preservation_operations[child]['parent_handle'] is root
    assert coordinator.status()['active_owner_classes'] == [
        'I06_busy_followup_delivery', 'I07_automation_route_wakes',
    ]

    from dataclasses import replace
    from hermes_state_runtime import RuntimeStoreError
    with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
        owner._preservation_operation_start(
            'I06_busy_followup_delivery', principal_id='automation:route', session_id='session-1',
            operation='copied-parent', request_id='wake-2', generation='pre-admission',
            source_identity='source-1', parent_handle=replace(root, owner_id='copied'),
        )

    owner._preservation_operation_release_unaccepted(child)
    owner._preservation_release_automation_unaccepted(None, root)
    assert coordinator.status()['active_owner_classes'] == []


@pytest.mark.asyncio
async def test_real_claim_pins_preadmission_generation_and_reuses_exact_operation(tmp_path, monkeypatch):
    from hermes_state_runtime import get_session_admission
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = await fixture.claim('pre-admission-bind')
        authority = fixture.authority
        row = get_session_admission(authority.db, admission_id=binding.admission_id)
        key = authority._preservation_operation_start(
            'I06_busy_followup_delivery', principal_id=row['principal_id'],
            session_id=binding.session_id, operation='bind-real-follow-up',
            request_id=row['request_id'], generation='pre-admission',
            source_identity='native-follow-up', parent_handle=binding.parent_handle,
        )
        record = authority._preservation_operations[key]
        handle = record['handle']
        assert authority._preservation_operation_bind_row(key, row) is True
        assert record['generation'] == binding.execution_generation
        assert authority._preservation_operation_bind_row(key, row) is True
        assert authority._preservation_operation_row(key)['generation'] == binding.execution_generation
        assert authority._preservation_operation_start(
            'I06_busy_followup_delivery', principal_id=row['principal_id'],
            session_id=binding.session_id, operation='bind-real-follow-up',
            request_id=row['request_id'], generation='pre-admission',
            source_identity='native-follow-up', parent_handle=binding.parent_handle,
        ) == key
        assert authority._preservation_operations[key] is record
        assert record['handle'] is handle
        assert record['generation'] == binding.execution_generation
        arguments = {
            'principal_id': row['principal_id'], 'session_id': binding.session_id,
            'operation': 'bind-real-follow-up', 'request_id': row['request_id'],
            'generation': 'pre-admission', 'source_identity': 'native-follow-up',
            'parent_handle': binding.parent_handle,
        }
        authority._preservation_operation_unknown(key, 'read_failed')
        assert record['unknown'] is True
        assert authority._preservation_operation_start(
            'I06_busy_followup_delivery', **arguments,
        ) == key
        assert record['unknown'] is False
        assert authority._preservation_operation_start(
            'I06_busy_followup_delivery', **arguments,
        ) == key
        assert authority._preservation_operations[key] is record
        assert record['handle'] is handle
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize('wrong_field', ['owner_epoch', 'generation'])
async def test_real_claim_rejects_foreign_row_without_rebinding_owner(tmp_path, monkeypatch, wrong_field):
    from hermes_state_runtime import get_session_admission
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = await fixture.claim('foreign-bind-' + wrong_field)
        authority = fixture.authority
        row = get_session_admission(authority.db, admission_id=binding.admission_id)
        key = authority._preservation_operation_start(
            'I06_busy_followup_delivery', principal_id=row['principal_id'],
            session_id=binding.session_id, operation='reject-foreign-row',
            request_id=row['request_id'], admission_id=binding.admission_id,
            generation=binding.execution_generation, source_identity='native-follow-up',
            parent_handle=binding.parent_handle,
        )
        record = authority._preservation_operations[key]
        handle = record['handle']
        assert authority._preservation_operation_bind_row(key, row) is True
        wrong = {**row, wrong_field: row[wrong_field] + 1}
        assert authority._preservation_operation_bind_row(key, wrong) is False
        assert record['generation'] == binding.execution_generation
        assert record['admission_id'] == binding.admission_id
        assert record['handle'] is handle
        assert record['unknown'] is True
        retained = authority.runner.runtime_custody._records[handle.lease.lease_id]
        assert retained.handle is handle
        assert retained.state == 'unknown'
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_real_claim_existing_operation_rejects_a_copied_parent(tmp_path, monkeypatch):
    from dataclasses import replace
    from hermes_state_runtime import RuntimeStoreError, get_session_admission
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = await fixture.claim('copied-same-key')
        authority = fixture.authority
        row = get_session_admission(authority.db, admission_id=binding.admission_id)
        arguments = {
            'principal_id': row['principal_id'], 'session_id': binding.session_id,
            'operation': 'same-key-parent', 'request_id': row['request_id'],
            'admission_id': binding.admission_id, 'generation': binding.execution_generation,
            'source_identity': 'native-follow-up',
        }
        key = authority._preservation_operation_start(
            'I06_busy_followup_delivery', **arguments, parent_handle=binding.parent_handle,
        )
        handle = authority._preservation_operations[key]['handle']
        with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
            authority._preservation_operation_start(
                'I06_busy_followup_delivery', **arguments, parent_handle=replace(binding.parent_handle),
            )
        assert authority._preservation_operations[key]['handle'] is handle
        assert authority._preservation_operations[key]['parent_handle'] is binding.parent_handle
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_classic_commit_refusal_retains_accepted_row_before_publication(tmp_path, monkeypatch):
    from gateway.platforms.event import MessageEvent
    from hermes_state_runtime import RuntimeStoreError, get_session_admission
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        authority = fixture.authority
        source = fixture.adapter.build_source(
            chat_id='commit-refusal@s.whatsapp.net', user_id='15551234567@s.whatsapp.net', chat_type='dm',
        )
        store = fixture.runner.session_store
        session_key = store._generate_session_key(source)
        store.get_or_create_session(source, touch_activity=False)
        event = MessageEvent(text='retained commit', source=source, message_id='commit-refusal')
        assert authority._classic_begin(event, session_key, 'fifo')
        key = event._classic_custody_key
        record = authority._preservation_operations[key]
        handle = record['handle']
        original_row = authority._preservation_operation_row
        monkeypatch.setattr(
            authority, '_preservation_operation_row',
            lambda candidate: None if candidate == key else original_row(candidate),
        )
        publications, schedules = [], []
        monkeypatch.setattr(authority, '_publish_pending', publications.append)
        monkeypatch.setattr(authority, '_schedule', schedules.append)
        with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
            await authority.admit_native(event)
        row = get_session_admission(authority.db, admission_id=record['admission_id'])
        assert row is not None and row['status'] == 'queued'
        assert row['request_id'] == event.message_id
        assert event._gateway_accepted is True
        assert publications == []
        assert schedules == []
        assert authority._preservation_operations[key] is record
        assert record['handle'] is handle
        retained = authority.runner.runtime_custody._records[handle.lease.lease_id]
        assert retained.handle is handle
        assert retained.state == 'unknown'
    finally:
        fixture.cleanup()
