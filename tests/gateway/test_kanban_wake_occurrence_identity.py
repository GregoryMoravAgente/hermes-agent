"""Kanban wake identity keeps distinct claimed event occurrences distinct."""
from types import SimpleNamespace

import pytest


def _occurrence(event_ids, *, old_cursor=4, claimed_cursor=7):
    return {
        'schema': 'kanban_task_events.v1',
        'board': 'ops',
        'task_id': 'task-42',
        'old_cursor': old_cursor,
        'claimed_cursor': claimed_cursor,
        'event_ids': event_ids,
    }


def test_same_rendered_wake_has_distinct_identity_for_distinct_event_occurrences():
    from gateway.session_automation_custody import route_wake_identity

    first = route_wake_identity('kanban-wake', 'route-42', 'same rendered wake',
                                {'kanban_occurrence': _occurrence([11])})
    second = route_wake_identity('kanban-wake', 'route-42', 'same rendered wake',
                                 {'kanban_occurrence': _occurrence([12])})
    retry = route_wake_identity('kanban-wake', 'route-42', 'same rendered wake',
                                {'kanban_occurrence': _occurrence([11])})

    assert first == retry
    assert first != second


def test_notifier_occurrence_is_copied_from_exact_claimed_events_and_cursors():
    from gateway.kanban_watchers_notifier import _kanban_wake_occurrence

    delivery = {'board': 'ops', 'old_cursor': 8, 'cursor': 10,
                'sub': {'task_id': 'task-42'}}
    occurrence = _kanban_wake_occurrence(
        delivery, [SimpleNamespace(id=9), SimpleNamespace(id=10)])

    assert occurrence == _occurrence([9, 10], old_cursor=8, claimed_cursor=10)


def test_occurrence_rejects_missing_or_reordered_source_event_ids():
    from gateway.kanban_watchers_notifier import _kanban_wake_occurrence

    delivery = {'board': 'ops', 'old_cursor': 8, 'cursor': 10,
                'sub': {'task_id': 'task-42'}}
    with pytest.raises(RuntimeError, match='exact ordered occurrence'):
        _kanban_wake_occurrence(delivery, [SimpleNamespace(id=10), SimpleNamespace(id=9)])
    with pytest.raises(RuntimeError, match='exact ordered occurrence'):
        _kanban_wake_occurrence(delivery, [SimpleNamespace(id=None)])


def test_occurrence_cannot_be_attached_to_a_non_kanban_producer():
    from gateway.session_automation_custody import route_wake_identity
    from hermes_state_runtime import RuntimeStoreError

    with pytest.raises(RuntimeStoreError, match='invalid_params'):
        route_wake_identity('loop-tick', 'route-42', 'same rendered wake',
                            {'kanban_occurrence': _occurrence([11])})


def test_compact_occurrence_retry_keeps_ack_loss_identity_stable():
    from gateway.session_automation_custody import route_wake_identity

    compact = {
        'schema': 'kanban_task_events.v1', 'board': 'ops', 'task_id': 'task-42',
        'old_cursor': 8, 'claimed_cursor': 400,
        'event_count': 392, 'event_ids_digest': 'a' * 64,
    }
    first = route_wake_identity('kanban-wake', 'route-42', 'same rendered wake',
                                {'kanban_occurrence': compact})
    retry = route_wake_identity('kanban-wake', 'route-42', 'same rendered wake',
                                {'kanban_occurrence': dict(compact)})
    different = route_wake_identity(
        'kanban-wake', 'route-42', 'same rendered wake',
        {'kanban_occurrence': {**compact, 'event_ids_digest': 'b' * 64}},
    )

    assert first == retry
    assert first != different
