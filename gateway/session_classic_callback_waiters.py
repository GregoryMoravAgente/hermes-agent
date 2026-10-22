"""Source-owned exact classic callback waiter index and completion boundary."""

from contextlib import suppress


def _classic_callback_waiter_registry(authority, *, create=False):
    """Return the source-owned waiter index without replacing a present foreign holder."""
    missing = object()
    registry = getattr(authority, '_classic_callback_waiters', missing)
    if isinstance(registry, dict):
        return registry
    if not create or registry is not missing:
        return None
    registry = {}
    try:
        authority._classic_callback_waiters = registry
    except (AttributeError, TypeError):
        return None
    return registry


def _classic_callback_retired_record(authority, owner, bridge):
    """Return the exact retired I06 record when its source identity is still bound."""
    record = getattr(bridge, 'retired_operation_record', None)
    if (
        getattr(owner, '_classic_custody_key', None) is not None
        or getattr(owner, '_classic_custody_authority', None) is not authority
        or not isinstance(record, dict)
        or record.get('owner_class') != 'I06_busy_followup_delivery'
        or record.get('producer_object') is not owner
        or record.get('admission_id') != getattr(bridge, 'admission_id', None)
        or record.get('unknown') is True
    ):
        return None
    identity = record.get('classic_identity')
    if not isinstance(identity, dict):
        return None
    if (
        identity.get('owner_class') != record.get('owner_class')
        or identity.get('profile_id') != getattr(authority, 'profile_id', None)
        or identity.get('process_instance') != getattr(authority, 'instance_id', None)
        or identity.get('authority_epoch') != getattr(authority, 'epoch', None)
        or not isinstance(identity.get('source_identity'), str)
        or not identity.get('source_identity')
        or any(
            identity.get(field) != getattr(bridge, field, None)
            for field in ('session_id', 'principal_id', 'request_id', 'occurrence_id')
        )
    ):
        return None
    return record


def _classic_callback_retired_handle_valid(authority, record):
    handle = record.get('handle')
    return (
        handle is not None
        and getattr(handle, 'owner_class', None) == record.get('owner_class')
        and getattr(handle, 'authority_epoch', None) == getattr(authority, 'epoch', None)
        and getattr(handle, 'profile_id', None) == getattr(authority, 'profile_id', None)
        and getattr(handle, 'process_instance', None) == getattr(authority, 'instance_id', None)
    )


def _classic_callback_retired_state_valid(bridge):
    return (
        getattr(bridge, 'post_delivery_callback_expected', False) is True
        and getattr(bridge, 'post_delivery_callback_pending', False) is False
        and getattr(bridge, 'post_delivery_callback_executed', False) is True
        and getattr(bridge, 'post_delivery_callback_settled', False) is True
        and getattr(bridge, 'post_delivery_callback_state', None) == 'settled'
        and getattr(bridge, 'post_delivery_callback_settlement', None) is not None
        and getattr(bridge, 'post_delivery_callback_delivery_witness', None) is not None
    )


def _classic_callback_retired_profile_valid(authority, record):
    try:
        from gateway.session_classic_custody import _profile_home
        profile_home = _profile_home(authority)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False
    identity = record.get('classic_identity')
    return isinstance(identity, dict) and identity.get('profile_home') == profile_home


def _classic_callback_retired_identity_valid(authority, owner, bridge):
    """Validate the one-use bridge witness after its live operation row is retired."""
    record = _classic_callback_retired_record(authority, owner, bridge)
    return (
        record is not None
        and _classic_callback_retired_handle_valid(authority, record)
        and _classic_callback_retired_state_valid(bridge)
        and _classic_callback_retired_profile_valid(authority, record)
    )


def _classic_callback_bridge_identity_valid(
    authority, owner, bridge, *, operation_key=None, admission_id=None, expectation=None,
    allow_retired=False,
):
    from gateway.session_classic_custody import ClassicExecutionEventBridge

    if not isinstance(bridge, ClassicExecutionEventBridge) or owner is None:
        return False
    owner_key = getattr(owner, '_classic_custody_key', None)
    key = owner_key
    if operation_key is not None:
        key = operation_key
    if (
        not isinstance(key, str)
        or getattr(owner, '_classic_custody_authority', None) is not authority
        or bridge.owner_event is not owner
        or bridge.operation_key != key
        or not isinstance(bridge.admission_id, str)
        or not bridge.admission_id
        or (admission_id is not None and bridge.admission_id != admission_id)
    ):
        return False
    if owner_key != key and (
        not allow_retired
        or not _classic_callback_retired_identity_valid(authority, owner, bridge)
    ):
        return False
    if expectation is None:
        return True
    return (
        getattr(expectation, 'authority', None) is authority
        and getattr(expectation, 'operation_key', None) == key
        and getattr(expectation, 'admission_id', None) == bridge.admission_id
        and getattr(expectation, 'owner_event', None) is owner
        and getattr(bridge, 'post_delivery_callback_expectation', None) is expectation
    )


def _classic_callback_waiter_entry(authority, *, operation_key=None, admission_id=None):
    """Find one exact source-owned pending bridge without trusting the mutable owner attribute."""
    registry = _classic_callback_waiter_registry(authority)
    if not isinstance(registry, dict):
        return None
    entries = []
    for registered_admission, entry in registry.items():
        if admission_id is not None and registered_admission != admission_id:
            continue
        if not isinstance(entry, tuple) or len(entry) != 4:
            continue
        key, owner, bridge, expectation = entry
        if operation_key is not None and key != operation_key:
            continue
        if (
            not _classic_callback_bridge_identity_valid(
                authority, owner, bridge, operation_key=key, admission_id=registered_admission,
                expectation=expectation, allow_retired=True,
            )
            or getattr(bridge, 'post_delivery_callback_expected', False) is not True
        ):
            continue
        entries.append((owner, bridge))
    return entries[0] if len(entries) == 1 else None


def _classic_callback_waiter_slot(authority, bridge_owner, bridge, expectation):
    """Return the exact registry slot, refusing present malformed or foreign holders."""
    admission_id = getattr(expectation, 'admission_id', None)
    operation_key = getattr(expectation, 'operation_key', None)
    if (
        not isinstance(admission_id, str)
        or not admission_id
        or not isinstance(operation_key, str)
        or not operation_key
    ):
        return None
    registry = _classic_callback_waiter_registry(authority, create=True)
    if not isinstance(registry, dict):
        return None
    if admission_id in registry:
        prior_waiter = registry[admission_id]
        if (
            not isinstance(prior_waiter, tuple)
            or len(prior_waiter) != 4
            or prior_waiter[0] != operation_key
            or prior_waiter[1] is not bridge_owner
            or prior_waiter[2] is not bridge
            or prior_waiter[3] is not expectation
        ):
            return None
    return registry


def _classic_callback_bridge_for_event(event, *, allow_retired=False):
    from gateway.session_classic_custody import classic_execution_owner_for_event

    owner = classic_execution_owner_for_event(event)
    authority = getattr(owner, '_classic_custody_authority', None)
    operation_key = getattr(owner, '_classic_custody_key', None)
    bridge = getattr(owner, '_classic_execution_bridge', None)
    if _classic_callback_bridge_identity_valid(
        authority, owner, bridge, operation_key=operation_key,
    ):
        return owner, bridge
    if allow_retired and _classic_callback_bridge_identity_valid(
        authority,
        owner,
        bridge,
        operation_key=getattr(bridge, 'operation_key', None),
        admission_id=getattr(bridge, 'admission_id', None),
        allow_retired=True,
    ):
        return owner, bridge
    registered = _classic_callback_waiter_entry(authority, operation_key=operation_key)
    if registered is not None and registered[0] is owner:
        return registered
    return owner, None


def _classic_callback_bridge_for_admission(authority, admission_id, *, allow_retired=False):
    from gateway.session_classic_custody import classic_execution_owner_for_event

    records = getattr(authority, '_preservation_operations', None)
    if not isinstance(admission_id, str) or not admission_id:
        return None
    matches = []
    candidate_seen = False
    for key, record in (records.items() if isinstance(records, dict) else ()):
        if (
            not isinstance(record, dict)
            or record.get('owner_class') != 'I06_busy_followup_delivery'
            or record.get('admission_id') != admission_id
        ):
            continue
        candidate_seen = True
        owner = record.get('producer_object')
        bridge = getattr(owner, '_classic_execution_bridge', None)
        if _classic_callback_bridge_identity_valid(
            authority, owner, bridge, operation_key=key, admission_id=admission_id,
        ):
            matches.append((owner, bridge))
    if len(matches) == 1:
        return matches[0]
    registered = _classic_callback_waiter_entry(authority, admission_id=admission_id)
    if registered is not None:
        return registered
    if matches or candidate_seen:
        return None
    adopter = (getattr(authority, 'marker_adopters', None) or {}).get(admission_id)
    owner = classic_execution_owner_for_event(adopter) if adopter is not None else None
    bridge = getattr(owner, '_classic_execution_bridge', None)
    if _classic_callback_bridge_identity_valid(
        authority,
        owner,
        bridge,
        operation_key=getattr(bridge, 'operation_key', None),
        admission_id=admission_id,
        allow_retired=allow_retired,
    ):
        return owner, bridge
    return None


def _classic_callback_done(bridge, *, failed=False):
    if bridge is None:
        return
    bridge.post_delivery_callback_pending = False
    bridge.post_delivery_callback_authority = False
    bridge.post_delivery_callback_executed = True
    if failed:
        bridge.post_delivery_callback_state = 'failed'
        bridge.post_delivery_callback_settled = False
    done = getattr(bridge, 'post_delivery_callback_done', None)
    if done is not None:
        with suppress(Exception):
            done.set()
    expected = getattr(bridge, 'post_delivery_callback_expectation', None)
    authority = getattr(expected, 'authority', None)
    owner = getattr(bridge, 'owner_event', None)
    if authority is None:
        authority = getattr(owner, '_classic_custody_authority', None)
    registry = _classic_callback_waiter_registry(authority)
    if isinstance(registry, dict):
        for registered_admission, entry in tuple(registry.items()):
            if isinstance(entry, tuple) and len(entry) == 4 and entry[2] is bridge:
                registry.pop(registered_admission, None)
