"""Authenticate a retry of the same managed native process finalizer."""

from __future__ import annotations

from typing import Any


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
        and receipt.service.external_parent is None
        and transaction is not None
        and transaction.custody is receipt
        and transaction.process is receipt.process
        and transaction.matches(receipt.process, receipt.pid, receipt.process_started_at)
    )
