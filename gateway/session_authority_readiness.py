"""Session-authority readiness predicate extracted from the facade.

The predicate is source-owned observation only: the coordinator and canonical
durable store remain the authorities for ownership and settlement.
"""
from __future__ import annotations

import sqlite3

from hermes_state_runtime import get_session_admission


def preservation_owner_ready(authority, owner_class):
    """Report quiescence for the API/FIFO/native source seams wired by this proposal.

    This is a source-owned observation used by handoff preparation after natural settlement; it
    does not seal the other 17 owner classes or infer cross-process ownership.
    """
    if owner_class not in {
        'I01_native_inbound', 'I02_api_session_turn', 'I04_fifo_turn_execution',
        'I06_busy_followup_delivery', 'I07_automation_route_wakes',
        'I08_goals_loops_heartbeat',
    }:
        return False
    coordinator = authority._preservation_coordinator()
    if coordinator is None:
        # Readiness is never proven by empty local maps while the authoritative ledger is absent.
        # Bare legacy adapters remain functional, but they cannot report managed custody ready.
        return False
    custody = coordinator.status()
    if owner_class == 'I08_goals_loops_heartbeat':
        from gateway.session_goal_tick_custody import owner_ready
        if not owner_ready(authority, close_generation=custody.get('close_generation')):
            return False
    if owner_class == 'I06_busy_followup_delivery' and not authority._classic_sources_quiescent():
        return False
    # The coordinator is the authoritative lease ledger while a producer is between callback
    # admission phases.  Its active set also includes an exact lease retained as unknown after a
    # failed identity/read reconciliation, even though no durable admission id can be bound in
    # ``_preservation_handles``.  Do not turn either an in-flight root or an unknown root into a
    # sealed class merely because this authority's local maps/queue happen to be empty.
    if (owner_class in custody.get('active_owner_classes', ())
            or owner_class in custody.get('unknown_owner_classes', ())):
        return False
    if owner_class in {'I01_native_inbound', 'I06_busy_followup_delivery'}:
        # A registered source close is stronger than an activity bit: every callback accepted
        # before this exact close generation is retained until native_callback exits, while a
        # new callback is refused before the adapter can acknowledge it. No event is required
        # to make this proof, but an empty/unregistered source graph remains unknown.
        from gateway.preservation_source_close import source_owner_ready
        close_generation = custody.get('close_generation')
        if not isinstance(close_generation, int) or not source_owner_ready(
                authority.runner, close_generation):
            return False
    if owner_class == 'I07_automation_route_wakes':
        # A class is sealed only after its real source ingress has been observed. Empty local
        # maps or an empty FIFO cannot prove that an external route-wake producer never existed;
        # route-wake sources are separate from the adapter callback close above.
        if not authority._route_wake_inbound_seen:
            return False
    if owner_class == 'I01_native_inbound':
        teams_pending = authority._preservation_teams_job_pending()
        if teams_pending is None or teams_pending:
            return False
    i02_handles = authority._preservation_handles
    if owner_class == 'I02_api_session_turn':
        # Exempt a retained automation handle only after current durable identity reread;
        # stale or foreign rows remain blocking.
        i02_handles = dict(authority._preservation_handles)
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
                i02_handles.pop(admission_id, None)
    if (i02_handles or authority._preservation_children
            or authority._preservation_pending_roots or authority._preservation_claiming
            or authority._preservation_scheduler_handles
            or authority._preservation_scheduler_retry_handles
            or authority._preservation_marker_watchers or authority._preservation_sidecars
            or authority._preservation_automation_rehomes
            or authority._preservation_operations
            or any((authority.executing, authority._external_process_finalizers))
            or authority.pending_results or authority.pending_deliveries
            or authority.native_waiters or authority.waiters
            or getattr(authority, 'marker_adopters', None)
            or getattr(authority, 'held_turn_markers', None)
            or getattr(authority, 'unsettled_deliveries', None)
            or getattr(authority, 'waiter_replies', None)):
        return False
    with authority.db._read_ctx() as conn:
        return conn.execute(
            "SELECT 1 FROM session_admissions WHERE status IN ('queued','started','unknown') LIMIT 1"
        ).fetchone() is None
