"""Exact I04 parent validation for a queued native I06 child."""

from __future__ import annotations

import sqlite3


def _parent_mode_valid(self, parent_mode, parent_row, parent_admission_id, terminal_parent):
    if parent_mode == 'classic_busy':
        # A first public classic row is durably marked before the authority creates this I04
        # claim.  Authenticate that marker through the original source-owned I06 record; the
        # descriptor alone is never a mode allowlist or a substitute for its producer object.
        try:
            from gateway.session_classic_custody import (
                _classic_admission_row_matches, classic_direct_route_marker,
                classic_row_is_direct,
            )
            if not classic_row_is_direct(parent_row):
                return False
            if terminal_parent:
                from gateway.session_classic_delivery import classic_marker_release_witness_valid
                parent_owner = getattr(self, 'marker_adopters', {}).get(parent_admission_id)
                bridge = getattr(parent_owner, '_classic_execution_bridge', None)
                owner_key = getattr(bridge, 'operation_key', None)
                parent_record = getattr(bridge, 'retired_operation_record', None)
                if (
                    getattr(parent_owner, '_classic_custody_authority', None) is not self
                    or not isinstance(owner_key, str)
                    or getattr(bridge, 'owner_event', None) is not parent_owner
                    or getattr(bridge, 'admission_id', None) != parent_admission_id
                    or not isinstance(parent_record, dict)
                    or parent_record.get('owner_class') != 'I06_busy_followup_delivery'
                    or parent_record.get('producer_object') is not parent_owner
                    or parent_record.get('parent_handle') is not None
                    or parent_record.get('admission_id') != parent_admission_id
                    or parent_record.get('unknown') is True
                    or not _classic_admission_row_matches(
                        self, parent_record, parent_row, allow_terminal=True)
                    or not classic_marker_release_witness_valid(
                        self, owner_key, parent_record, parent_owner)
                ):
                    return False
            else:
                resolver = getattr(self, '_classic_execution_owner_for_admission', None)
                if not callable(resolver):
                    return False
                parent_owner, parent_error = resolver(parent_row)
                owner_key = getattr(parent_owner, '_classic_custody_key', None)
                records = getattr(self, '_preservation_operations', None)
                parent_record = records.get(owner_key) if isinstance(records, dict) else None
                if (
                    parent_error is not None
                    or parent_owner is None
                    or classic_direct_route_marker(self, parent_owner) != 'classic_busy'
                    or getattr(parent_owner, '_classic_custody_authority', None) is not self
                    or not isinstance(owner_key, str)
                    or not isinstance(parent_record, dict)
                    or parent_record.get('owner_class') != 'I06_busy_followup_delivery'
                    or parent_record.get('producer_object') is not parent_owner
                    or parent_record.get('parent_handle') is not None
                    or parent_record.get('admission_id') != parent_admission_id
                ):
                    return False
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            return False
    elif parent_mode != 'managed_turn':
        return False
    return True


def _completed_terminal_parent_edge_valid(self, record, parent_row, parent, coordinator):
    if parent_row.get('outcome') != 'completed':
        return False
    # A later session claim can advance the session generation while this
    # child is queued. Bind the completed row to the generation captured
    # when this exact child reserved its parent lease instead.
    if (type(record.get('parent_generation')) is not int
            or record['parent_generation'] != parent_row['generation']):
        return False
    child = record.get('handle')
    parent_lease_id = getattr(getattr(parent, 'lease', None), 'lease_id', None)
    child_lease = getattr(child, 'lease', None)
    child_lease_id = getattr(child_lease, 'lease_id', None)
    lock = getattr(coordinator, '_lock', None)
    if (lock is None or not isinstance(parent_lease_id, str)
            or not isinstance(child_lease_id, str)
            or getattr(child_lease, 'parent_lease_id', None) != parent_lease_id):
        return False
    with lock:
        records = getattr(coordinator, '_records', None)
        children = getattr(coordinator, '_children_by_parent', None)
        parent_live = records.get(parent_lease_id) if isinstance(records, dict) else None
        child_live = records.get(child_lease_id) if isinstance(records, dict) else None
        if (parent_live is None or parent_live.handle is not parent
                or parent_live.state not in {'accepted', 'finalizing'}
                or child_live is None or child_live.handle is not child
                or child_live.state not in {'accepted', 'finalizing'}
                or not isinstance(children, dict)
                or child_lease_id not in children.get(parent_lease_id, ())):
            return False
    return True


def parented_native_row_live_matches(
    self, record, row, admission_id, row_generation, expected_parent,
):
    """Validate a queued native child against its exact live I04 parent.

    A classic I06 child is accepted while the parent I04 is still executing.  Its own native
    row is therefore queued and cannot satisfy the generic current-admission check until the
    FIFO claims it.  This narrow path authenticates the existing parent handle and the actual
    current I04 row before allowing the exact queued child row to bind; it is never a general
    queued-row or I06 live-check bypass.
    """
    if (
        not isinstance(record, dict)
        or record.get('owner_class') != 'I06_busy_followup_delivery'
        or record.get('operation') not in {'running-slot', 'fifo', 'debounce'}
        or record.get('parent_handle') is not expected_parent
        or not isinstance(row, dict)
        or row.get('status') != 'queued'
        or row_generation is not None
        or record.get('generation') not in (None, 'pre-admission')
        or not isinstance(admission_id, str)
        or not admission_id
        or row.get('admission_id') != admission_id
    ):
        return False
    payload = row.get('payload')
    descriptor = payload.get('native_text_v1') if isinstance(payload, dict) else None
    if (not isinstance(descriptor, dict)
            or descriptor.get('producer_process_mode') != 'managed_turn'):
        return False
    coordinator = self._preservation_coordinator()
    if coordinator is None or not self._preservation_operation_expected_parent_valid(
            record, expected_parent):
        return False
    parent = expected_parent
    if parent.owner_class != 'I04_fifo_turn_execution':
        return False
    owner_id = parent.owner_id
    prefix = 'claim:'
    if type(owner_id) is not str or not owner_id.startswith(prefix):
        return False
    parent_admission_id = owner_id[len(prefix):]
    if not parent_admission_id or parent_admission_id == admission_id:
        return False
    children = getattr(self, '_preservation_children', None)
    if not isinstance(children, dict) or children.get(parent_admission_id) is not parent:
        return False
    from hermes_state_runtime import get_session_admission
    try:
        parent_row = get_session_admission(self.db, admission_id=parent_admission_id)
    except (OSError, RuntimeError, sqlite3.Error, KeyError, TypeError, ValueError):
        return False
    if (isinstance(parent_row, dict)
            and parent_row.get('principal_id') != record.get('principal_id')):
        live = self.sessions.get(record.get('session_id'))
        parent_source = getattr(live, 'source', None)
        child_source = getattr(record.get('producer_object'), 'source', None)
        config = getattr(getattr(self, 'runner', None), 'config', None)
        shared_group_route = (
            getattr(parent_source, 'chat_type', None) == 'group'
            and getattr(child_source, 'chat_type', None) == 'group'
            and getattr(parent_source, 'platform', None) == getattr(child_source, 'platform', None)
            and getattr(parent_source, 'profile', None) == getattr(child_source, 'profile', None)
            and getattr(parent_source, 'chat_id', None) == getattr(child_source, 'chat_id', None)
            and getattr(parent_source, 'thread_id', None) == getattr(child_source, 'thread_id', None)
            and getattr(config, 'group_sessions_per_user', True) is False
        )
        if not shared_group_route:
            return False
    if (
        not isinstance(parent_row, dict)
        or parent_row.get('admission_id') != parent_admission_id
        or parent_row.get('status') not in {'started', 'terminal'}
        or parent_row.get('owner_epoch') != self.epoch
        or parent_row.get('target_session_id') != record.get('session_id')
        or type(parent_row.get('generation')) is not int
        or parent_row.get('generation') < 1
    ):
        return False
    terminal_parent = parent_row['status'] == 'terminal'
    if terminal_parent and not _completed_terminal_parent_edge_valid(
            self, record, parent_row, parent, coordinator):
        return False
    parent_payload = parent_row.get('payload')
    parent_descriptor = (
        parent_payload.get('native_text_v1') if isinstance(parent_payload, dict) else None
    )
    if not isinstance(parent_descriptor, dict):
        return False
    parent_mode = parent_descriptor.get('producer_process_mode')
    if not _parent_mode_valid(
            self, parent_mode, parent_row, parent_admission_id, terminal_parent):
        return False
    if terminal_parent:
        return True
    live = self.sessions.get(record.get('session_id'))
    event_stream = getattr(live, 'event_stream', None) if live is not None else None
    lock = getattr(event_stream, 'lock', None)
    if live is None or event_stream is None or lock is None:
        return False
    with lock:
        execution = getattr(event_stream, 'execution', None)
        executing = getattr(self, 'executing', None)
        return (
            type(execution) is dict
            and execution.get('authority_epoch') == self.epoch
            and execution.get('admission_id') == parent_admission_id
            and execution.get('execution_generation') == parent_row.get('generation')
            and isinstance(executing, dict)
            and executing.get(record.get('session_id')) == parent_row.get('generation')
        )
