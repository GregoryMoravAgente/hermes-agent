"""Pin the exact I04 generation before a classic I06 child reserves its lease."""

from hermes_state_runtime import RuntimeStoreError, get_session_admission


def classic_parent_generation(authority, parent, session_id):
    if getattr(parent, 'owner_class', None) != 'I04_fifo_turn_execution':
        return None
    owner_id = getattr(parent, 'owner_id', None)
    if type(owner_id) is not str or not owner_id.startswith('claim:'):
        raise RuntimeStoreError('preservation_owner_unmapped')
    try:
        parent_row = get_session_admission(authority.db, admission_id=owner_id[len('claim:'):])
    except Exception as exc:
        # A failed read cannot establish that the I04 owner is still live.  Keep the
        # classic child out of the coordinator rather than letting a store error
        # escape the admission boundary.
        raise RuntimeStoreError('preservation_owner_unmapped') from exc
    if (not isinstance(parent_row, dict)
            or parent_row.get('status') != 'started'
            or parent_row.get('owner_epoch') != authority.epoch
            or parent_row.get('target_session_id') != session_id
            or type(parent_row.get('generation')) is not int
            or parent_row.get('generation') < 1):
        raise RuntimeStoreError('preservation_owner_unmapped')
    return parent_row['generation']
