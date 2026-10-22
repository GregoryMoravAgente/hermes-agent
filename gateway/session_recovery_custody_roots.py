"""Exact source-root adoption helpers for managed recovery."""

from __future__ import annotations

from hermes_state_runtime import RuntimeStoreError, get_session_admission


def _custody_module():
    import gateway.session_recovery_custody as custody
    return custody


def adopt_local_api_root(authority, row, existing):
    """Transfer one queued LOCAL API producer root to the current I10 recovery owner."""
    custody = _custody_module()
    identity = custody._identity(row, authority)
    if (
        identity is None
        or row.get("status") != "queued"
        or getattr(existing, "owner_class", None) != "I02_api_session_turn"
        or getattr(existing, "owner_id", None)
        != f"local:{identity.target_session_id}:{identity.request_id}"
        or custody._validate_handle(authority, existing) is None
    ):
        raise RuntimeStoreError("preservation_owner_unmapped")
    coordinator = getattr(authority.runner, "runtime_custody", None)
    if coordinator is None or coordinator.has_active_bound_child(existing, "I04_fifo_turn_execution"):
        raise RuntimeStoreError("unknown_execution")
    custody._current_epoch(authority)
    custody._physical_owner(authority)
    from gateway.runtime_custody import CustodyRefusal

    replacement = coordinator.reserve_root(
        custody.OWNER_CLASS,
        f"recovery:local_recovery:{identity.admission_id}:{identity.request_id}",
        profile_id=authority.profile_id,
        authority_epoch=authority.epoch,
    )
    if isinstance(replacement, CustodyRefusal):
        raise RuntimeStoreError(replacement.code)
    current = get_session_admission(authority.db, admission_id=identity.admission_id)
    current_identity = custody._identity(current, authority)
    if (
        not isinstance(current, dict)
        or current.get("status") != "queued"
        or not custody._same_full_identity(identity, current_identity)
    ):
        coordinator.mark_unknown(replacement, "unmapped_writer")
        raise RuntimeStoreError("preservation_owner_unmapped")
    refusal = coordinator.mark_terminal(existing)
    if refusal is not None:
        coordinator.mark_unknown(replacement, "recovery_handoff_failed")
        raise RuntimeStoreError(refusal.code)
    authority._preservation_handles[identity.admission_id] = replacement
    custody._bindings(authority)[identity.admission_id] = {
        "handle": replacement,
        "identity": current_identity,
        "consumer": "local_recovery",
        "consumers": {"local_recovery"},
        "profile_home": str(custody._home(authority)),
        "process_instance": authority.instance_id,
        "authority_epoch": authority.epoch,
        "state": "accepted",
        "receipts": (),
    }
    authority._i10_recovery_seen = True
    return replacement
