"""Retry exact terminal I04 release after its classic I06 child closes."""

import sqlite3


def retry_completed_parent_release(authority, retired, parent_validator):
    # The prior I04 may have reached terminal while this exact I06 child held
    # its lease. Re-enter only after the child's own terminal receipt exists.
    parent = retired.get('parent_handle')
    parent_owner_id = getattr(parent, 'owner_id', None)
    if (getattr(parent, 'owner_class', None) != 'I04_fifo_turn_execution'
            or type(parent_owner_id) is not str
            or not parent_owner_id.startswith('claim:')):
        return
    parent_admission_id = parent_owner_id[len('claim:'):]
    if not parent_validator(authority, parent, parent_admission_id):
        return
    coordinator = authority._preservation_coordinator()
    child = retired.get('handle')
    child_lease_id = getattr(getattr(child, 'lease', None), 'lease_id', None)
    lock = getattr(coordinator, '_lock', None)
    if lock is None or not isinstance(child_lease_id, str):
        return
    with lock:
        completed = getattr(coordinator, '_completed', None)
        records = getattr(coordinator, '_records', None)
        child_terminal = (
            isinstance(completed, dict) and completed.get(child_lease_id) == 'terminal'
            and isinstance(records, dict) and child_lease_id not in records
        )
    if not child_terminal:
        return
    try:
        from hermes_state_runtime import get_session_admission
        parent_row = get_session_admission(authority.db, admission_id=parent_admission_id)
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return
    if (not isinstance(parent_row, dict)
            or parent_row.get('admission_id') != parent_admission_id
            or parent_row.get('status') != 'terminal'
            or parent_row.get('outcome') != 'completed'
            or parent_row.get('owner_epoch') != authority.epoch
            or parent_row.get('target_session_id') != retired.get('session_id')
            or parent_row.get('principal_id') != retired.get('principal_id')):
        return
    authority._preservation_release_terminal(parent_admission_id)
