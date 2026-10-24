"""Strict I10 binding checks shared by native recovery consumers.

The classic producer discriminator is not an ownership proof.  Startup recovery may legitimately
resume a row after the original I06 event has disappeared, but only the source-owned I10 binding
can authorize that route.  This helper deliberately returns no binding object: callers must keep
using their existing I10 child/finalizer paths for the actual release proof.
"""
from __future__ import annotations

from hermes_state_runtime import RuntimeStoreError

from gateway.session_recovery_custody import (
    CONSUMERS,
    OWNER_CLASS,
    _binding_for_row,
    _validate_handle,
)


def _retain_unknown(authority, admission_id, reason):
    marker = getattr(authority, "_preservation_mark_unknown", None)
    if callable(marker):
        marker(admission_id, reason)


def i10_native_recovery_row_is_bound(authority, row, *, consumer):
    """Return true only for a current source-owned I10 row/handle/consumer binding.

    A missing binding is an ordinary non-I10 case and returns false so the caller can retain its
    existing classifier.  A present but malformed, foreign, replaced, or unknown binding is also
    false, while preserving the exact admission in UNKNOWN custody through the authority's normal
    marker.  No payload mode, empty-map observation, caller marker, or synthetic child is accepted
    as a substitute for the typed I10 coordinator handle.
    """
    if consumer not in CONSUMERS:
        raise RuntimeStoreError("preservation_owner_mismatch")
    if not isinstance(row, dict):
        return False
    admission_id = row.get("admission_id")
    if not isinstance(admission_id, str):
        return False
    bindings = getattr(authority, "_i10_recovery_bindings", None)
    if not isinstance(bindings, dict) or admission_id not in bindings:
        return False
    try:
        binding = _binding_for_row(authority, row)
    except RuntimeStoreError:
        _retain_unknown(authority, admission_id, "classic_recovery_binding_unknown")
        return False
    if not isinstance(binding, dict):
        _retain_unknown(authority, admission_id, "classic_recovery_binding_unmapped")
        return False
    handle = binding.get("handle")
    if (
        getattr(handle, "owner_class", None) != OWNER_CLASS
        or _validate_handle(authority, handle) is None
    ):
        _retain_unknown(authority, admission_id, "classic_recovery_binding_unknown")
        return False
    consumers = binding.get("consumers")
    if not (
        binding.get("consumer") == consumer
        or isinstance(consumers, set)
        and consumer in consumers
    ):
        _retain_unknown(authority, admission_id, "classic_recovery_consumer_mismatch")
        return False
    return True
