"""Accounting at completion of an exact heartbeat admission attempt.

Execution means entering the gateway's agent runner after turn preparation,
not reserving an adapter slot, successful model completion, or outbound delivery.
The done callback retains the watch's profile ContextVars and manager claim.
"""
import logging

from hermes_state_runtime import RuntimeStoreError

logger = logging.getLogger("gateway.run")


async def resolve_heartbeat_owner(runner, event, entry):
    """Keep normal reset/topic resolution, then admit only the original lineage."""
    expected = getattr(event, "_heartbeat_session_id", None)
    if not expected:
        return True
    resolved = entry.session_id
    if resolved != expected:
        def compression_tip():
            return runner.session_store._db_for_key(entry.session_key).get_compression_tip(expected)

        tip = await runner._run_in_executor_with_context(compression_tip)
        if tip != resolved:
            return False
    # Keep a value, not the mutable routing entry: preparation and hooks can yield
    # to /new or /stop before the agent runner starts.
    event._heartbeat_resolved_session_id = resolved
    return heartbeat_owner_is_current(runner, event, entry.session_key)


def heartbeat_owner_is_current(runner, event, session_key):
    expected = getattr(event, "_heartbeat_resolved_session_id", None)
    if not expected:
        return True
    current = runner.session_store.lookup_by_session_key(session_key)
    return current is not None and not current.suspended and current.session_id == expected


def settle_heartbeat_attempt(event, manager):
    if not getattr(event, "_heartbeat_execution_started", False):
        try:
            manager.abandon_fire()
        except Exception:
            logger.warning("Failed to refund unexecuted heartbeat", exc_info=True)


async def admit_heartbeat(runner, adapter, source, session_id, route):
    """Coalesce overdue ticks behind the canonical FIFO, not an adapter-local slot."""
    import json
    from gateway.platforms.event import MessageEvent
    from hermes_cli.heartbeat import HeartbeatManager
    from hermes_state_runtime import list_session_admissions

    from gateway.session_authorities import active_authority
    from gateway.session_goal_tick_custody import reserve_tick, release_unaccepted
    authority = active_authority(runner)
    if authority is None:
        return
    require_coordinator = getattr(authority, '_require_preservation_coordinator', None)
    coordinator = require_coordinator() if callable(require_coordinator) else None
    # Admissions live on the route's logical owner (the compression root), not its transcript tip;
    # a LOCAL route's heartbeat rides local_automation_v1.
    pending = list_session_admissions(authority.db, session_id=authority.logical_owner(session_id))
    if any(row['payload'].get('native_text_v1', {}).get('automation', {}).get('heartbeat')
           or row['payload'].get('local_automation_v1', {}).get('heartbeat') for row in pending):
        return
    manager = HeartbeatManager(session_id)
    state = manager.state
    if state is None:
        return
    owner = authority.logical_owner(session_id)
    identity = json.dumps(
        ['heartbeat', session_id, state.created_at, state.fire_count + 1], separators=(',', ':'))
    principal_id = 'automation:' + route
    if coordinator is not None and not callable(getattr(authority, '_preservation_automation_root', None)):
        raise RuntimeStoreError('runtime_custody_unavailable')
    tick = (reserve_tick(
        authority, request_id=identity, principal_id=principal_id, session_id=owner,
        source_identity='heartbeat:' + route, kind='heartbeat', watcher_key=route,
    ) if coordinator is not None else None)
    handle = tick.handle if tick is not None else None
    prompt = manager.due_prompt()
    if prompt is None:
        if tick is not None:
            release_unaccepted(tick)
        return
    event = MessageEvent(text=prompt, source=source, internal=True, message_id=identity,
        metadata={'gateway_session_key': route, 'gateway_session_id': session_id})
    event._heartbeat_session_id = session_id
    from gateway.session_goal_tick_custody import attach_tick_handle
    attach_tick_handle(event, handle, 'heartbeat')
    event._preservation_effect_started = True
    try:
        await authority.admit_automation(
            adapter, event, identity, _preservation_handle=handle)
    except BaseException:
        coordinator = authority._preservation_coordinator()
        if coordinator is not None:
            coordinator.mark_unknown(handle, 'join_failed')
        # Readiness is not a delivery attempt, and cancellation before commit is not ACK.
        if getattr(event, '_gateway_accepted', False) is not True:
            manager.abandon_fire()
        raise
