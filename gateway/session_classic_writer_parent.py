"""Authenticated classic writer-parent and carrier joins.

The delivery module owns callback effects; this topical module owns the existing
writer-parent proof joins so the facade stays below its health-size cap.
"""

from __future__ import annotations


def _classic_claimed_child_operation_match(authority, record, admission_id):
    """Join an active or exact settled retired I06 record to its producer bridge."""
    operations = getattr(authority, '_preservation_operations', None)
    matches = [(key, candidate) for key, candidate in (
        operations.items() if isinstance(operations, dict) else ()) if candidate is record]
    if len(matches) == 1:
        key, candidate = matches[0]
        return key, candidate.get('producer_object'), False
    if matches:
        return None
    producer = record.get('producer_object') if isinstance(record, dict) else None
    bridge = getattr(producer, '_classic_execution_bridge', None)
    key = getattr(bridge, 'operation_key', None)
    if not (
        isinstance(key, str)
        and getattr(bridge, 'owner_event', None) is producer
        and getattr(bridge, 'admission_id', None) == admission_id
        and getattr(bridge, 'retired_operation_record', None) is record
        and getattr(producer, '_classic_custody_authority', None) is authority
        and getattr(bridge, 'post_delivery_callback_state', None) == 'settled'
        and getattr(bridge, 'post_delivery_callback_settled', False) is True
    ):
        return None
    return key, producer, True


def _classic_claimed_child_handles(authority, binding, admission_id):
    try:
        from gateway.runtime_custody import CustodyHandle
        from gateway.runtime_service_custody import TurnCustodyBinding
    except (ImportError, AttributeError):
        return None
    if not isinstance(binding, TurnCustodyBinding):
        return None
    getter = getattr(authority, '_preservation_coordinator', None)
    if not callable(getter):
        return None
    try:
        coordinator = getter()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None
    handles, children = getattr(authority, '_preservation_handles', None), getattr(authority, '_preservation_children', None)
    if coordinator is None or not isinstance(handles, dict) or not isinstance(children, dict):
        return None
    root, child = handles.get(admission_id), children.get(admission_id)
    if not isinstance(root, CustodyHandle) or not isinstance(child, CustodyHandle):
        return None
    if (
        getattr(binding, 'authority', None) is not authority
        or getattr(binding, 'coordinator', None) is not coordinator
        or getattr(binding, 'parent_handle', None) is not child
        or getattr(binding, 'admission_id', None) != admission_id
    ):
        return None
    return coordinator, root, child


def _classic_claimed_child_edge_valid(authority, binding, admission_id):
    """Validate A->H1->C without conflating C with the classic outer parent."""
    parts = _classic_claimed_child_handles(authority, binding, admission_id)
    if parts is None:
        return False
    coordinator, root, child = parts
    from gateway.session_classic_custody import (
        _classic_claimed_child_edge_identity_valid,
        _classic_claimed_child_records_valid,
    )
    ids = _classic_claimed_child_edge_identity_valid(
        authority, coordinator, root, child, admission_id,
    )
    if ids is None:
        return False
    return _classic_claimed_child_records_valid(
        coordinator, root, child, ids[0], ids[1],
    )


def _classic_claimed_child_writer_parent_valid(authority, record, identity, binding) -> bool:
    from gateway.session_classic_custody import _classic_writer_binding_valid
    from gateway.session_classic_delivery import _classic_source_parent_valid

    admission_id = record.get('admission_id') if isinstance(record, dict) else None
    parent = getattr(binding, 'parent_handle', None)
    coordinator_getter = getattr(authority, '_preservation_coordinator', None)
    coordinator = coordinator_getter() if callable(coordinator_getter) else None
    if (
        not isinstance(record, dict)
        or not isinstance(identity, dict)
        or not isinstance(admission_id, str)
        or not admission_id
        or coordinator is None
        or binding.authority is not authority
        or binding.coordinator is not coordinator
        or binding.admission_id != admission_id
    ):
        return False
    if not _classic_source_parent_valid(authority, parent, admission_id):
        return False
    for field in (
        'session_id', 'profile_id', 'authority_epoch', 'process_instance',
        'principal_id', 'source_identity',
    ):
        expected_field = (
            'principal_id' if field == 'source_identity' else field
        )
        if getattr(binding, field, None) != identity.get(expected_field):
            return False
    if type(binding.execution_generation) is not int or binding.execution_generation < 1:
        return False
    return _classic_writer_binding_valid(
        binding, identity, record, allow_terminal=True,
    )


def _classic_claimed_child_carrier_writer_parent_valid(authority, record, identity, binding) -> bool:
    from gateway.session_classic_custody import _classic_claimed_child_binding_valid
    return _classic_claimed_child_binding_valid(authority, record, identity, binding)


def _classic_execution_writer_parent_valid(authority, record, identity, binding) -> bool:
    """Validate one explicit inline-parent or separately claimed-child writer route."""
    from gateway.session_classic_custody import (
        classic_inline_parent_binding_valid,
        classic_writer_parent_relation,
    )

    relation = classic_writer_parent_relation(record, binding, authority)
    if relation is None or not isinstance(identity, dict):
        return False
    if relation[0] == 'inline_parent':
        return classic_inline_parent_binding_valid(
            authority, record, identity, binding,
        )
    if relation[0] == 'claimed_child':
        return _classic_claimed_child_writer_parent_valid(
            authority, record, identity, binding,
        )
    if relation[0] == 'claimed_child_carrier':
        return _classic_claimed_child_carrier_writer_parent_valid(
            authority, record, identity, binding,
        )
    return False
