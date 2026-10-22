"""Authenticate a retry of the same managed native process finalizer."""

from __future__ import annotations

from typing import Any


def _managed_parent_matches(receipt: Any) -> bool:
    """Authenticate the producer view of this same managed service and physical owner."""
    from gateway.runtime_ownership import process_ownership
    from gateway.runtime_service_operations import (
        ExternalProcessParent,
        _persistent_service_identity_matches,
    )

    service = receipt.service
    binding = service.binding
    parent = service.external_parent
    if parent is None:
        return True
    if (
        not isinstance(parent, ExternalProcessParent)
        or parent.mode != "managed_turn"
        or parent.state == "terminal"
        or parent.turn_binding is not binding
        or parent._service is not service
        or parent.root_handle is not service.handle
        or service.physical is not receipt
        or (parent._physical is not None and parent._physical is not receipt)
        or (parent.finalizer is not service and parent.finalizer is not receipt)
        or not _persistent_service_identity_matches(parent, binding)
        or not parent.physical_owner_epoch
        or parent.physical_owner_epoch != service.handle.physical_owner_epoch
        or parent.physical_owner_epoch != receipt.handle.physical_owner_epoch
    ):
        return False
    try:
        if not process_ownership.reservation_is_current(
            parent.profile_home, parent.physical_owner_epoch,
        ):
            return False
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False
    coordinator = service.coordinator
    with coordinator._lock:
        for handle in (service.handle, receipt.handle):
            record = coordinator._records.get(handle.lease.lease_id)
            if (record is None or record.handle is not handle
                    or record.state not in {"accepted", "finalizing", "unknown"}):
                return False
    return True


def can_retry_managed_native_close(receipt: Any) -> bool:
    """Only a retained managed transaction may reconcile native close uncertainty.

    Reader, publication, foreign identity and unavailable-query failures stay unknown.
    The caller must still prove EOF, joins, durable delivery, exact Job emptiness and
    native handle closure before reconciling the original coordinator handle.
    """
    from gateway.runtime_service_custody import TurnCustodyBinding

    transaction = receipt.transaction
    return bool(
        receipt.unknown_reason in {
            "termination_failed", "native_job_nonempty", "native_close_failed",
        }
        and isinstance(receipt.service.binding, TurnCustodyBinding)
        and _managed_parent_matches(receipt)
        and transaction is not None
        and transaction.custody is receipt
        and transaction.process is receipt.process
        and transaction.matches(receipt.process, receipt.pid, receipt.process_started_at)
    )
