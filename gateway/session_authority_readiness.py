"""Session-authority readiness predicate extracted from the facade.

The predicate is source-owned observation only: the coordinator and canonical
durable store remain the authorities for ownership and settlement.
"""
from __future__ import annotations

import sqlite3

from gateway.session_authority_operation_graph import PreservationProducerGraph
from hermes_state_runtime import get_session_admission


_MISSING = object()


def _task_done(task):
    """Read one task's completion bit, or return ``None`` for an unknown value."""
    done = getattr(task, 'done', _MISSING)
    if not callable(done):
        return None
    try:
        result = done()
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return None
    return result if type(result) is bool else None


def _unfinished_task_count(values):
    """Count a typed task map; missing, malformed, or unknown values return ``None``."""
    if type(values) is not dict:
        return None
    count = 0
    for task in values.values():
        finished = _task_done(task)
        if finished is None:
            return None
        count += not finished
    return count


def _unfinished_task_set(values):
    """Count a typed task set; an unregistered task can never prove quiescence."""
    if type(values) is not set:
        return None
    count = 0
    for task in values:
        finished = _task_done(task)
        if finished is None:
            return None
        count += not finished
    return count


def _unfinished_task_union(*values):
    """Count the distinct live tasks in multiple registered task sets."""
    merged = set()
    for value in values:
        if type(value) is not set:
            return None
        merged.update(value)
    return _unfinished_task_set(merged)


def _watcher_count(values):
    """Validate reply watcher tuples and count every retained watcher as pending."""
    if type(values) is not dict:
        return None
    count = 0
    for key, entry in values.items():
        if not isinstance(key, str) or not key or type(entry) is not tuple or len(entry) != 3:
            return None
        admission_id, task, future = entry
        if not isinstance(admission_id, str) or not admission_id:
            return None
        if _task_done(task) is None or _task_done(future) is None:
            return None
        count += 1
    return count


def _producer_graph(authority, coordinator):
    """Return the current bootstrap graph only when its identity binding is still exact."""
    graph = getattr(authority, '_preservation_producer_graph', _MISSING)
    if not isinstance(graph, PreservationProducerGraph):
        return None
    try:
        return graph if graph.matches(authority, coordinator) else None
    except (AttributeError, TypeError, ValueError):
        return None


def _source_operation_i05_snapshot(authority):
    """Observe the typed control indexes and their terminalizer."""
    pending_controls = 0
    remote_responders = 0
    sessions = getattr(authority, 'sessions', _MISSING)
    if type(sessions) is not dict:
        return None
    from gateway.session_pending_controls import PendingControls
    for session_id, live in sessions.items():
        if not isinstance(session_id, str) or not session_id:
            return None
        controls = getattr(live, 'controls', _MISSING)
        if type(controls) is not PendingControls:
            return None
        pending = getattr(controls, 'pending', _MISSING)
        remote = getattr(controls, 'remote_responders', _MISSING)
        if type(pending) is not dict or type(remote) is not dict:
            return None
        for prompt_id, entry in pending.items():
            if (not isinstance(prompt_id, str) or not prompt_id
                    or type(entry) is not tuple or len(entry) != 2
                    or not isinstance(entry[1], dict)):
                return None
        for prompt_id, responder in remote.items():
            if (not isinstance(prompt_id, str) or not prompt_id
                    or prompt_id not in pending or not callable(responder)):
                return None
        pending_controls += len(pending)
        remote_responders += len(remote)
    indexes = {
        'cancel_obligations': getattr(authority, 'cancel_obligations', _MISSING),
        'steer_transfers': getattr(authority, 'steer_transfers', _MISSING),
        'pending_stops': getattr(authority, 'pending_stops', _MISSING),
        'pending_steers': getattr(authority, 'pending_steers', _MISSING),
        'pending_closes': getattr(authority, 'pending_closes', _MISSING),
    }
    if (type(indexes['cancel_obligations']) is not set
            or any(type(indexes[name]) is not dict for name in (
                'steer_transfers', 'pending_stops', 'pending_steers', 'pending_closes'))):
        return None
    if any(
        any(not isinstance(key, str) or not key for key in value)
        for value in indexes.values()
    ):
        return None
    counts = {
        'cancel_obligations': len(indexes['cancel_obligations']),
        'steer_transfers': len(indexes['steer_transfers']),
        'pending_stops': len(indexes['pending_stops']),
        'pending_steers': len(indexes['pending_steers']),
        'pending_closes': len(indexes['pending_closes']),
    }
    if any((pending_controls, remote_responders, *counts.values())):
        return None
    if (not callable(getattr(authority, '_preservation_control_call', None))
            or not callable(getattr(authority, '_preservation_control_terminal', None))):
        return None
    return {
        'pending_controls': pending_controls,
        **counts,
        'producer_graph': 'SessionAuthority._register_preservation_producer_graph',
        'producer_entry': 'SessionAuthority._preservation_control_call',
        'terminalizer': 'SessionAuthority._preservation_control_terminal',
    }


def _source_operation_i11_snapshot(authority):
    """Observe the typed mutation indexes and their terminalizer."""
    task_indexes = {
        'tracked_workers': _unfinished_task_union(
            getattr(authority, '_mutation_tasks', _MISSING),
            getattr(authority, '_bot_mailbox_operations', _MISSING),
        ),
        'receipt_tasks': _unfinished_task_set(
            getattr(authority, '_bot_receipt_tasks', _MISSING),
        ),
        'local_adoptions': _unfinished_task_count(
            getattr(authority, '_local_adoptions', _MISSING),
        ),
        'policy_refreshes': _unfinished_task_count(
            getattr(authority, '_policy_refreshes', _MISSING),
        ),
        'receipt_refreshes': _unfinished_task_count(
            getattr(authority, '_bot_receipt_refreshes', _MISSING),
        ),
    }
    pending_adoptions_index = getattr(authority, '_pending_adoptions', _MISSING)
    if (type(pending_adoptions_index) is not set
            or any(not isinstance(value, str) or not value for value in pending_adoptions_index)):
        task_indexes['pending_adoptions'] = None
    else:
        task_indexes['pending_adoptions'] = len(pending_adoptions_index)
    deferred_resets_index = getattr(authority, 'deferred_resets', _MISSING)
    if (type(deferred_resets_index) is not dict
            or any(not isinstance(key, str) or not key or not callable(value)
                   for key, value in deferred_resets_index.items())):
        task_indexes['deferred_resets'] = None
    else:
        task_indexes['deferred_resets'] = len(deferred_resets_index)
    receipt_index = getattr(authority, '_bot_receipt_index', _MISSING)
    receipt_dirty_index = getattr(authority, '_bot_receipt_dirty', _MISSING)
    if (type(receipt_index) is not dict or type(receipt_dirty_index) is not set
            or any(not isinstance(key, str) or not key or type(value) is not tuple or len(value) != 2
                   for key, value in receipt_index.items())
            or any(not isinstance(key, str) or not key for key in receipt_dirty_index)):
        task_indexes['bot_receipts'] = task_indexes['bot_receipt_dirty'] = None
    else:
        task_indexes['bot_receipts'] = len(receipt_index)
        task_indexes['bot_receipt_dirty'] = len(receipt_dirty_index)
    task_indexes['reply_watchers'] = _watcher_count(
        getattr(authority, '_bot_reply_watchers', _MISSING),
    )
    if any(value is None for value in task_indexes.values()) or any(task_indexes.values()):
        return None
    if not callable(getattr(authority, '_preservation_operation_terminal', None)):
        return None
    return {
        **task_indexes,
        'producer_graph': 'SessionAuthority._register_preservation_producer_graph',
        'producer_entry': 'gateway.session_runtime_workers.track_mutation',
        'terminalizer': 'SessionAuthority._preservation_operation_terminal',
    }


_SOURCE_OPERATION_SNAPSHOT_HANDLERS = {
    'I05_controls_and_unknown': _source_operation_i05_snapshot,
    'I11_session_mutations_finalizers': _source_operation_i11_snapshot,
}


def _source_operation_snapshot(authority, owner_class):
    """Return source-index evidence for one local global-close seam."""
    coordinator = authority._preservation_coordinator()
    if _producer_graph(authority, coordinator) is None:
        return None
    handler = _SOURCE_OPERATION_SNAPSHOT_HANDLERS.get(owner_class)
    return handler(authority) if handler is not None else None


def _owner_ready_special_source(authority, owner_class, custody):
    if owner_class == 'I08_goals_loops_heartbeat':
        from gateway.session_goal_tick_custody import owner_ready
        return owner_ready(authority, close_generation=custody.get('close_generation'))
    if owner_class == 'I06_busy_followup_delivery' and not authority._classic_sources_quiescent():
        return False
    if owner_class in {'I01_native_inbound', 'I06_busy_followup_delivery'}:
        # A registered source close is stronger than an activity bit: every callback accepted
        # before this exact close generation is retained until native_callback exits, while a
        # new callback is refused before the adapter can acknowledge it.
        from gateway.preservation_source_close import source_owner_ready
        close_generation = custody.get('close_generation')
        if not isinstance(close_generation, int) or not source_owner_ready(
                authority.runner, close_generation):
            return False
    if owner_class == 'I07_automation_route_wakes' and not authority._route_wake_inbound_seen:
        return False
    if owner_class == 'I01_native_inbound':
        teams_pending = authority._preservation_teams_job_pending()
        if teams_pending is None or teams_pending:
            return False
    return True


def _owner_ready_i02_handles(authority, owner_class):
    handles = authority._preservation_handles
    if owner_class != 'I02_api_session_turn':
        return handles
    # Exempt a retained automation handle only after current durable identity reread;
    # stale or foreign rows remain blocking.
    handles = dict(authority._preservation_handles)
    for admission_id, handle in authority._preservation_handles.items():
        binding = authority._preservation_automation_bindings.get(admission_id)
        bound_row = binding.get('row') if isinstance(binding, dict) else None
        if (not isinstance(binding, dict)
                or binding.get('handle') is not handle
                or not isinstance(bound_row, dict)
                or binding.get('terminal_pending') is not True
                or getattr(handle, 'owner_class', None) not in {
                    'I07_automation_route_wakes', 'I08_goals_loops_heartbeat',
                    'I09_a2a_hosted_kanban',
                }):
            continue
        try:
            current = get_session_admission(authority.db, admission_id=admission_id)
        except (sqlite3.Error, RuntimeError, TypeError, ValueError):
            continue
        if (isinstance(current, dict) and current.get('admission_id') == admission_id
                and current.get('status') == 'terminal'
                and authority._preservation_automation_row_matches(
                    current, handle, allow_terminal=True,
                )):
            handles.pop(admission_id, None)
    return handles


def _owner_ready_local_maps_empty(authority, handles):
    return not (
        handles or authority._preservation_children
        or authority._preservation_pending_roots or authority._preservation_claiming
        or authority._preservation_scheduler_handles
        or authority._preservation_scheduler_retry_handles
        or authority._preservation_marker_watchers or authority._preservation_sidecars
        or authority._preservation_automation_rehomes or authority._preservation_operations
        or any((authority.executing, authority._external_process_finalizers))
        or authority.pending_results or authority.pending_deliveries
        or authority.native_waiters or authority.waiters
        or getattr(authority, 'marker_adopters', None)
        or getattr(authority, 'held_turn_markers', None)
        or getattr(authority, 'unsettled_deliveries', None)
        or getattr(authority, 'waiter_replies', None)
    )


def _owner_ready_durable_admissions(authority):
    with authority.db._read_ctx() as conn:
        return conn.execute(
            "SELECT 1 FROM session_admissions WHERE status IN ('queued','started','unknown') LIMIT 1"
        ).fetchone() is None


def preservation_owner_ready(authority, owner_class):
    """Report quiescence for the authority-owned preservation source seams.

    This is a source-owned observation used by handoff preparation after natural settlement; it
    does not seal the other 17 owner classes or infer cross-process ownership.
    """
    allowed = {
        'I01_native_inbound', 'I02_api_session_turn', 'I04_fifo_turn_execution',
        'I06_busy_followup_delivery', 'I07_automation_route_wakes',
        'I08_goals_loops_heartbeat', 'I05_controls_and_unknown',
        'I11_session_mutations_finalizers',
    }
    if owner_class not in allowed:
        return False
    coordinator = authority._preservation_coordinator()
    if coordinator is None:
        # Readiness is never proven by empty local maps while the authoritative ledger is absent.
        return False
    graph_owners = {'I05_controls_and_unknown', 'I11_session_mutations_finalizers'}
    if owner_class in graph_owners and _producer_graph(authority, coordinator) is None:
        return False
    custody = coordinator.status()
    if not _owner_ready_special_source(authority, owner_class, custody):
        return False
    if (owner_class in custody.get('active_owner_classes', ())
            or owner_class in custody.get('unknown_owner_classes', ())):
        return False
    handles = _owner_ready_i02_handles(authority, owner_class)
    if not _owner_ready_local_maps_empty(authority, handles):
        return False
    if not _owner_ready_durable_admissions(authority):
        return False
    return owner_class not in graph_owners or _source_operation_snapshot(authority, owner_class) is not None


def preservation_owner_evidence(
    authority, owner_class, *, process_instance, authority_epoch, close_generation, owner_id,
):
    """Build source-backed evidence for local I05/I11 global sealing."""
    if owner_class not in {'I05_controls_and_unknown', 'I11_session_mutations_finalizers'}:
        return None
    coordinator = authority._preservation_coordinator()
    if coordinator is None:
        return None
    if _producer_graph(authority, coordinator) is None:
        return None
    custody = coordinator.status()
    if (process_instance != getattr(coordinator, 'process_instance', None)
            or authority_epoch != getattr(coordinator, 'authority_epoch', None)
            or custody.get('close_generation') != close_generation
            or not preservation_owner_ready(authority, owner_class)):
        return None
    snapshot = _source_operation_snapshot(authority, owner_class)
    if snapshot is None:
        return None
    from gateway.runtime_custody import OWNER_SPECS
    return {
        'source': OWNER_SPECS[owner_class].source,
        'process_instance': process_instance,
        'authority_epoch': authority_epoch,
        'close_generation': close_generation,
        'owner_id': owner_id,
        'observed': True,
        'source_snapshot': {
            'profile_id': authority.profile_id,
            'profile_authority_epoch': authority.epoch,
            **snapshot,
        },
    }
