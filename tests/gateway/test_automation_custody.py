"""Automation wakes retain a durable producer identity across retries and route moves."""

import json

import pytest


def test_route_wake_identity_is_stable_and_keeps_the_producer_envelope():
    from gateway.session_automation_custody import route_wake_identity

    first = route_wake_identity('kanban-wake', 'agent:default:local:dm:42', 'WAKE', {'board': 'b'})
    retry = route_wake_identity('kanban-wake', 'agent:default:local:dm:42', 'WAKE', {'board': 'b'})
    other = route_wake_identity('kanban-wake', 'agent:default:local:dm:42', 'OTHER', {'board': 'b'})
    envelope = json.loads(first)

    assert envelope[0] == 'kanban-wake'
    assert envelope[1] == 'agent:default:local:dm:42'
    assert first == retry
    assert first != other


def test_kanban_occurrence_compacts_a_complete_claim_without_truncating_it():
    from gateway.session_automation_custody import (
        kanban_occurrence_metadata, normalize_kanban_occurrence, route_wake_identity,
    )

    event_ids = list(range(1, 513))
    occurrence = {
        'schema': 'kanban_task_events.v1', 'board': 'ops', 'task_id': 'task-42',
        'old_cursor': 100, 'claimed_cursor': 700, 'event_ids': event_ids,
    }
    compact = normalize_kanban_occurrence(occurrence)

    assert set(compact) == {
        'schema', 'board', 'task_id', 'old_cursor', 'claimed_cursor',
        'event_count', 'event_ids_digest',
    }
    assert compact['event_count'] == len(event_ids)
    assert len(compact['event_ids_digest']) == 64
    assert normalize_kanban_occurrence(compact) == compact
    assert kanban_occurrence_metadata({'kanban_occurrence': occurrence}) == {
        'kanban_occurrence': compact,
    }
    assert route_wake_identity('kanban-wake', 'route-42', 'same wake',
                               {'kanban_occurrence': occurrence}) == route_wake_identity(
                                   'kanban-wake', 'route-42', 'same wake',
                                   {'kanban_occurrence': compact})


def test_kanban_occurrence_rejects_malformed_compact_claim_and_generation_state():
    from gateway.session_automation_custody import _automation_generation_valid, normalize_kanban_occurrence
    from hermes_state_runtime import RuntimeStoreError

    compact = {
        'schema': 'kanban_task_events.v1', 'board': 'ops', 'task_id': 'task-42',
        'old_cursor': 100, 'claimed_cursor': 700, 'event_count': 512,
        'event_ids_digest': '0' * 64,
    }
    with pytest.raises(RuntimeStoreError, match='invalid_params'):
        normalize_kanban_occurrence({**compact, 'event_ids_digest': 'bad'})

    assert _automation_generation_valid({'status': 'queued', 'generation': None})
    assert not _automation_generation_valid({'status': 'queued', 'generation': 1})
    assert _automation_generation_valid({'status': 'started', 'generation': 1})
    assert _automation_generation_valid({'status': 'unknown', 'generation': 2})
    assert not _automation_generation_valid({'status': 'started', 'generation': None})
    assert not _automation_generation_valid({'status': 'unknown', 'generation': 0})


def test_automation_row_descriptor_reads_the_existing_durable_payload():
    from gateway.session_automation_custody import row_descriptor

    descriptor = {'owner_class': 'I07_automation_route_wakes', 'request_id': 'wake-1'}
    row = {'payload': {'native_text_v1': {'automation': {'custody': descriptor}}}}

    assert row_descriptor(row) is descriptor
    assert row_descriptor({'payload': {'text': 'legacy'}}) is None


def test_restart_recovery_requires_the_mailbox_projection_to_match_the_fifo_row(owner):
    from pathlib import Path

    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session_automation_custody import row_descriptor
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_state_runtime import RuntimeStoreError

    coordinator = RuntimeCustodyCoordinator(
        process_instance=owner.instance_id, authority_epoch=owner.epoch, fence=RetirementFence(),
    )
    owner.runner.runtime_custody = coordinator
    descriptor = {
        'owner_class': 'I07_automation_route_wakes',
        'owner_id': 'automation:default:session-1:wake-1',
        'profile_id': owner.profile_id,
        'profile_home': str(Path(owner.db.db_path).parent.resolve()),
        'authority_epoch': owner.epoch,
        'process_instance': 'previous-process',
        'close_generation': 0,
        'coordinator_id': 'previous-coordinator',
        'session_id': 'session-1',
        'principal_id': 'automation:route',
        'request_id': 'wake-1',
        'source_identity': 'source-1',
    }
    row = {
        'admission_id': 'admission-1', 'target_session_id': 'session-1', 'owner_epoch': owner.epoch,
        'principal_id': 'automation:route', 'request_id': 'wake-1', 'status': 'queued',
        'payload': {'native_text_v1': {'automation': {'custody': descriptor}}},
    }
    bad_receipt = {'automation_custody': {**descriptor, 'request_id': 'foreign-wake'}}

    assert row_descriptor(row) is descriptor
    with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
        owner._preservation_recover_automation(row, bad_receipt)
    assert coordinator.status()['active_owner_classes'] == []


def test_restart_recovery_requires_the_current_profile_lock_and_epoch(owner):
    from pathlib import Path

    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.runtime_ownership import process_ownership
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_state_runtime import RuntimeStoreError, begin_runtime_epoch

    # A genuine restart has a committed predecessor epoch, rather than epoch zero.
    owner.epoch = begin_runtime_epoch(owner.db, instance_id=owner.instance_id)

    coordinator = RuntimeCustodyCoordinator(
        process_instance=owner.instance_id, authority_epoch=owner.epoch, fence=RetirementFence(),
    )
    owner.runner.runtime_custody = coordinator
    home = str(Path(owner.db.db_path).parent.resolve())
    descriptor = {
        'owner_class': 'I07_automation_route_wakes',
        'owner_id': 'automation:default:session-1:wake-1',
        'profile_id': owner.profile_id,
        'profile_home': home,
        # This is the prior process witness. Recovery must not require it to equal the new epoch.
        'authority_epoch': owner.epoch - 1,
        'process_instance': 'previous-process',
        'close_generation': 0,
        'coordinator_id': 'previous-coordinator',
        'session_id': 'session-1',
        'principal_id': 'automation:route',
        'request_id': 'wake-1',
        'source_identity': 'source-1',
    }
    row = {
        'admission_id': 'admission-1', 'target_session_id': 'session-1', 'owner_epoch': owner.epoch - 1,
        'principal_id': 'automation:route', 'request_id': 'wake-1', 'status': 'queued',
        'payload': {'native_text_v1': {'automation': {'custody': descriptor}}},
    }
    process_ownership.reserve([Path(home)])
    try:
        recovered = owner._preservation_recover_automation(row)
        assert recovered.owner_class == 'I07_automation_route_wakes'
        assert recovered.authority_epoch == owner.epoch
    finally:
        process_ownership.release(Path(home))

    with pytest.raises(RuntimeStoreError, match='foreign_custody'):
        owner._preservation_recover_automation({**row, 'admission_id': 'admission-2'})

    owner.db._execute_write(lambda conn: conn.execute(
        'UPDATE runtime_epoch SET instance_id=? WHERE singleton=1', ('other-process',)))
    process_ownership.reserve([Path(home)])
    try:
        with pytest.raises(RuntimeStoreError, match='stale_epoch'):
            owner._preservation_recover_automation({**row, 'admission_id': 'admission-3'})
    finally:
        process_ownership.release(Path(home))
