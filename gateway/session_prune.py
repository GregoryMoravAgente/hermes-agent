"""``session.prune``: the owner runs `hermes sessions prune` for its own store.

The CLI computes the filters and shows the preview from a read-only view; the delete itself is
this owner's write, through the same ``prune_sessions`` / ``retire_prunable`` path housekeeping
uses (busy sessions and the logical owner of a retained reset child are skipped). A CLI writing
the store under a live gateway would be a second writer, so it never does.
"""
import asyncio
from pathlib import Path
import uuid

from hermes_state_runtime import RuntimeStoreError

_SCALARS = (type(None), bool, int, float, str)


def _filters(raw):
    from hermes_state_maintenance import _PRUNE_FILTER_NAMES
    if (not isinstance(raw, dict) or set(raw) - (_PRUNE_FILTER_NAMES | {'older_than_days'})
            or not all(isinstance(value, _SCALARS) for value in raw.values())):
        raise RuntimeStoreError('invalid_params')
    return dict(raw)


async def prune(connection, ref, params):
    authority, actor = connection.authority, connection.actor
    # Store-wide deletion across every principal's rows: the local operator only (a ticket from
    # this host's private control socket), never a remote or browser grant.
    if not connection.native_owner or 'session:operator' not in actor.capabilities:
        raise RuntimeStoreError('permission_denied')
    if actor.profile_id != authority.profile_id:
        raise RuntimeStoreError('profile_mismatch')
    never_active = params.get('never_active_days')
    if ('filters' in params) == (never_active is not None) or (
            never_active is not None and (isinstance(never_active, bool) or not isinstance(never_active, (int, float))
                                          or never_active < 0)):
        raise RuntimeStoreError('invalid_params')
    filters = _filters(params['filters']) if 'filters' in params else None
    # Store-wide pruning creates a new mutation root; it cannot join an external preservation
    # drain the way an already accepted FIFO continuation can.
    authority._require_fresh_admission_open()
    from gateway.session_runtime_workers import track_mutation
    request_id = f'session-prune:{uuid.uuid4().hex}'
    preservation_key = authority._preservation_operation_start_direct_mutation(
        principal_id=actor.subject,
        session_id=None,
        operation='session-prune',
        request_id=request_id,
        source_identity='session_prune',
    )
    operation = _apply(authority, filters, never_active)
    try:
        task = track_mutation(authority, operation)
    except RuntimeError:
        operation.close()
        authority._preservation_operation_unknown(preservation_key, 'join_failed')
        raise
    if preservation_key is not None:
        def finish(done):
            if done.cancelled():
                authority._preservation_operation_unknown(preservation_key, 'join_failed')
                return
            error = done.exception()
            if error is not None:
                authority._preservation_operation_unknown(preservation_key, 'join_failed')
                return
            authority._preservation_operation_terminal(
                preservation_key, f'session-prune:{request_id}',
            )
        task.add_done_callback(finish)
    return await asyncio.shield(task)


async def _apply(authority, filters, never_active):
    from hermes_constants import get_hermes_home
    from gateway.session_mutations import retire_live_sessions
    # Same transcript dir the CLI's local prune unlinks under (dispatch runs in the owner's scope).
    sessions_dir = Path(get_hermes_home()) / 'sessions'
    deleted_ids = []
    if filters is None:
        deleted, routing, skipped = await asyncio.to_thread(
            authority.db.prune_never_active_keyed_sessions, older_than_days=float(never_active),
            sessions_dir=sessions_dir, deleted_ids=deleted_ids)
        result = {'deleted': deleted, 'routing_deleted': routing, 'skipped': skipped}
    else:
        deleted = await asyncio.to_thread(
            authority.db.prune_sessions, sessions_dir=sessions_dir, exclude_active_write_guards=True,
            deleted_ids=deleted_ids, **filters)
        result = {'deleted': deleted}
    retire_live_sessions(authority, deleted_ids)
    return result
