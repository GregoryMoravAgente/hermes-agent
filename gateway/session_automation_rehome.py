"""Source-owned rehome validation for the automation custody mixin.

The coordinator remains the only lease authority.  These helpers keep the
mutable rehome buffer behind the retained binding and canonical admission row
before the existing pending-root seam is allowed to publish a successor.
"""

from hermes_state_runtime import RuntimeStoreError


def prepare_automation_rehome(authority, admission_id, row, new_session_id):
    """Hold one queued wake's I07 root while its durable row moves to a fresh route."""
    from gateway.session_automation_custody import _AUTOMATION_OWNER_CLASSES

    coordinator = authority._preservation_coordinator()
    handle = authority._preservation_handles.get(admission_id)
    descriptor = authority._preservation_automation_row_descriptor(row)
    if (
        coordinator is None
        or handle is None
        or getattr(handle, 'owner_class', None) not in _AUTOMATION_OWNER_CLASSES
        or not isinstance(row, dict)
        or row.get('status') != 'queued'
        or not isinstance(new_session_id, str)
        or not new_session_id
        or not authority._preservation_automation_row_matches(row, handle)
        or not isinstance(descriptor, dict)
    ):
        raise RuntimeStoreError('preservation_owner_unmapped')
    if admission_id in authority._preservation_children:
        raise RuntimeStoreError('preservation_owner_unmapped')
    rehome = {
        'handle': handle,
        'principal_id': row.get('principal_id'),
        'request_id': row.get('request_id'),
        'source_identity': descriptor.get('source_identity'),
        'delivery_id': descriptor.get('delivery_id'),
        'new_session_id': new_session_id,
    }
    authority._preservation_automation_rehomes[admission_id] = rehome
    return rehome


def abort_automation_rehome(authority, admission_id):
    authority._preservation_automation_rehomes.pop(admission_id, None)


def rehome_source_matches(authority, admission_id, handle, rehome):
    """Validate a rehome against the retained binding and canonical admission row."""
    from gateway.session_automation_custody import (
        _automation_descriptor_complete,
        _automation_generation_valid,
    )

    bindings = getattr(authority, '_preservation_automation_bindings', {})
    binding = bindings.get(admission_id) if isinstance(bindings, dict) else None
    if (
        not isinstance(binding, dict)
        or binding.get('handle') is not handle
        or rehome.get('handle') is not handle
    ):
        return None
    descriptor = binding.get('descriptor')
    bound_row = binding.get('row')
    if not _automation_descriptor_complete(descriptor) or not isinstance(bound_row, dict):
        return None
    stable_fields = ('principal_id', 'request_id', 'source_identity', 'delivery_id')
    if any(rehome.get(field) != descriptor.get(field) for field in stable_fields):
        return None
    if any(
        bound_row.get(field) != expected
        for field, expected in {
            'admission_id': admission_id,
            'principal_id': descriptor.get('principal_id'),
            'request_id': descriptor.get('request_id'),
        }.items()
    ):
        return None
    if not isinstance(rehome.get('new_session_id'), str) or not rehome['new_session_id']:
        return None
    try:
        from hermes_state_runtime import get_session_admission

        row = get_session_admission(authority.db, admission_id=admission_id)
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if (
        not isinstance(row, dict)
        or row.get('admission_id') != admission_id
        or row.get('status') != 'queued'
        or not _automation_generation_valid(row)
        or row.get('principal_id') != descriptor.get('principal_id')
        or row.get('request_id') != descriptor.get('request_id')
        or row.get('target_session_id') != rehome.get('new_session_id')
    ):
        return None
    row_descriptor = authority._preservation_automation_row_descriptor(row)
    if not _automation_descriptor_complete(row_descriptor):
        return None
    descriptor_fields = (
        'owner_class', 'owner_id', 'profile_id', 'profile_home', 'authority_epoch',
        'process_instance', 'close_generation', 'coordinator_id', 'principal_id',
        'request_id', 'source_identity', 'delivery_id',
    )
    if any(row_descriptor.get(field) != descriptor.get(field) for field in descriptor_fields):
        return None
    return row


def finish_automation_rehome(authority, admission_id):
    """Move a validated I07 root into the pending tuple for its new durable row."""
    rehome = authority._preservation_automation_rehomes.get(admission_id)
    if rehome is None:
        return None
    bindings = getattr(authority, '_preservation_automation_bindings', {})
    binding = bindings.get(admission_id) if isinstance(bindings, dict) else None
    # The rehome buffer is mutable.  The binding's exact accepted handle is the only
    # coordinator identity allowed to enter validation or UNKNOWN marking.
    handle = binding.get('handle') if isinstance(binding, dict) else None
    coordinator = authority._preservation_coordinator()
    if coordinator is None:
        require = getattr(authority, '_require_preservation_coordinator', None)
        coordinator = require() if callable(require) else None
        if coordinator is None:
            raise RuntimeStoreError('runtime_custody_unavailable')
    try:
        from gateway.session_automation_custody import _AUTOMATION_OWNER_CLASSES

        if (
            not isinstance(rehome, dict)
            or handle is None
            or getattr(handle, 'owner_class', None) not in _AUTOMATION_OWNER_CLASSES
            or getattr(handle, 'profile_id', None) != authority.profile_id
            or getattr(handle, 'authority_epoch', None) != authority.epoch
            or getattr(handle, 'process_instance', None)
            != getattr(coordinator, 'process_instance', None)
            or getattr(handle, 'process_instance', None) != authority.instance_id
            or getattr(handle, 'coordinator_id', None)
            != getattr(coordinator, '_coordinator_id', None)
            or getattr(coordinator, '_parent', lambda _handle: None)(handle) is None
            or rehome_source_matches(authority, admission_id, handle, rehome) is None
        ):
            raise RuntimeStoreError('preservation_owner_unmapped')
        # Install the validated successor tuple while the exact rehome descriptor, handle
        # index, accepted binding, and canonical retarget row are still present.  A strict
        # pending-root check must never run after cleanup has made its source witness unreachable.
        authority._preservation_pending_root(
            rehome['request_id'], handle,
            principal_id=rehome['principal_id'], session_id=rehome['new_session_id'],
            source_identity=rehome['source_identity'], delivery_id=rehome['delivery_id'],
        )
    except RuntimeStoreError:
        # Preserve every original index for same-handle reconciliation; the exact root is
        # UNKNOWN rather than eligible for a replacement lease.
        if handle is not None:
            coordinator.mark_unknown(handle, 'read_failed')
        raise
    authority._preservation_automation_rehomes.pop(admission_id, None)
    authority._preservation_handles.pop(admission_id, None)
    authority._preservation_automation_bindings.pop(admission_id, None)
    return handle
