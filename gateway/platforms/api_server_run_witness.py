"""Read-only source witnesses for one API-run custody handle.

The adapter owns the original launch, task, stream, writer receipt, and producer result.  This
module only reads those objects at reconciliation/sealing time.  It does not retain a second
ledger, manufacture a receipt, or turn a map/counter into proof.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from gateway.runtime_custody import CustodyRefusal

_TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled", "interrupted"})
_TERMINAL_DELIVERY_STATUSES = frozenset({"settled", "failed", "cancelled"})


def _refusal(code: str) -> CustodyRefusal:
    return CustodyRefusal(code, "I03_api_run_direct")


def _launch_for(adapter: Any, run_id: str):
    launches = getattr(adapter, "_run_launches", None)
    launch = launches.get(run_id) if isinstance(launches, dict) else None
    if launch is None:
        return None, _refusal("run_source_launch_missing")
    if getattr(launch, "run_id", None) != run_id:
        return None, _refusal("run_source_launch_mismatch")
    return launch, None


def _canonical_status(adapter: Any, custody: Any, launch: Any):
    """Read either the exact keyed status row or the canonical SessionDB result."""
    if custody.reservation_key is not None:
        store = getattr(adapter, "_run_idempotency_store", None)
        reader = getattr(store, "status_for_run", None)
        if not callable(reader):
            return None
        try:
            record = reader(custody.reservation_scope, custody.run_id)
        except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError,
                ValueError, sqlite3.Error):
            return None
        if not isinstance(record, dict):
            return None
        if (
            record.get("scope") != custody.reservation_scope
            or record.get("idempotency_key") != custody.reservation_key
            or record.get("fingerprint") != custody.reservation_fingerprint
            or record.get("owner_pid") != custody.reservation_owner_pid
            or record.get("owner_started") != custody.reservation_owner_started
        ):
            return None
        status = record.get("status")
        if not isinstance(status, dict) or status.get("status") not in _TERMINAL_RUN_STATUSES:
            return None
        return {
            "committed": True,
            "storage": "run_idempotency",
            "rowcount": 1,
            "run_id": custody.run_id,
            "scope": record["scope"],
            "idempotency_key": record["idempotency_key"],
            "fingerprint": record["fingerprint"],
            "owner_pid": record["owner_pid"],
            "owner_started": record["owner_started"],
            "readback": True,
            "status": status,
        }

    admission = getattr(launch, "admission", None)
    launch_authority = admission[0] if isinstance(admission, tuple) and len(admission) == 3 else None
    delivery = getattr(launch, "delivery_record", None)
    if launch_authority is None and isinstance(delivery, dict):
        launch_authority = delivery.get("_api_admitting_authority")
    authority = getattr(custody, "admission_authority", None)
    if authority is None:
        authority = launch_authority
    if authority is None or (
            launch_authority is not None and launch_authority is not authority):
        return None
    from gateway.platforms.api_server_run_custody import _session_status_receipt
    return _session_status_receipt(
        authority, custody, expected_session_id=getattr(launch, "session_id", None))


def _delivery_outcome(launch: Any, status_receipt: dict[str, Any]):
    """Return a source-owned delivery/finalizer outcome when the producer supplied one."""
    delivery = getattr(launch, "delivery_record", None)
    if isinstance(delivery, dict):
        status = delivery.get("status")
        if status not in _TERMINAL_DELIVERY_STATUSES:
            return None
        delivery_id = delivery.get("delivery_id")
        if not isinstance(delivery_id, str) or not delivery_id:
            return None
        admission_id = delivery.get("retry_admission_id") or delivery.get("admission_id")
        custody = getattr(launch, "custody", None)
        if getattr(custody, "producer_phase", None) == "bot_i02":
            generation = (delivery.get("retry_execution_generation")
                          if delivery.get("retry_admission_id")
                          else delivery.get("execution_generation"))
            if (
                admission_id != getattr(custody, "admission_id", None)
                or type(generation) is not int
                or generation != getattr(custody, "execution_generation", None)
                or delivery.get("profile_id") != getattr(custody, "profile_id", None)
                or delivery.get("authority_epoch") != getattr(custody, "authority_epoch", None)
                or delivery.get("principal_id") != getattr(custody, "producer_principal", None)
                or delivery.get("_api_producer_phase") != "bot_i02"
                or delivery.get("_api_producer_source") != "bot_chat"
            ):
                return None
            canonical_outcome = status_receipt.get("terminal_outcome")
            expected_outcomes = {
                "settled": {"completed"}, "failed": {"failed", "rejected"},
                "cancelled": {"cancelled"},
            }[status]
            if canonical_outcome not in expected_outcomes:
                return None
        return {
            "kind": "bot_delivery",
            "delivery_id": delivery_id,
            "admission_id": admission_id,
            "generation": (delivery.get("retry_execution_generation")
                           if delivery.get("retry_admission_id")
                           else delivery.get("execution_generation")),
            "principal_id": delivery.get("principal_id"),
            "profile_id": delivery.get("profile_id"),
            "authority_epoch": delivery.get("authority_epoch"),
            "status": status,
            "source_readback": True,
        }

    admission_id = status_receipt.get("admission_id")
    if not isinstance(admission_id, str) or not admission_id:
        # A direct API run has no session-ingress delivery receipt; its canonical result is the
        # producer boundary.  Managed sidecar entries must expose their own finalizer instead.
        return {"kind": "api_result", "source_readback": True}
    authority = getattr(launch, "admission", (None, None, None))[0]
    has_sidecar = getattr(authority, "_preservation_has_sidecar", None)
    if callable(has_sidecar):
        try:
            if has_sidecar(admission_id):
                return None
        except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError):
            return None
    return {"kind": "session_result", "admission_id": admission_id, "source_readback": True}


def _status_receipt_matches(source_receipt: Any, readback: dict[str, Any]) -> bool:
    if not isinstance(source_receipt, dict):
        return False
    if source_receipt.get("committed") is not True or source_receipt.get("run_id") != readback.get("run_id"):
        return False
    if source_receipt.get("storage") != readback.get("storage"):
        return False
    if source_receipt.get("storage") == "run_idempotency":
        return all(source_receipt.get(key) == readback.get(key) for key in (
            "scope", "idempotency_key", "fingerprint", "owner_pid", "owner_started"))
    if source_receipt.get("storage") == "session_admission":
        return all(source_receipt.get(key) == readback.get(key) for key in (
            "admission_id", "generation", "principal_id", "target_session_id"))
    return False


def read_run_witness(adapter: Any, custody: Any):
    """Read one source-owned run; return ``(witness, refusal)`` without mutating ownership."""
    launch, refusal = _launch_for(adapter, custody.run_id)
    if refusal is not None:
        return None, refusal
    task = getattr(launch, "execution_task", None)
    if task is None or not callable(getattr(task, "done", None)) or task.done() is not True:
        return None, _refusal("run_worker_pending")
    if getattr(custody, "worker_done", False) is not True:
        return None, _refusal("run_worker_unsettled")
    if getattr(launch, "control_finalizer_receipt", None) != ("retired", custody.run_id):
        return None, _refusal("run_control_finalizer_unproven")
    terminal_event = getattr(launch, "terminal_event_outcome", None)
    stream_close = getattr(launch, "stream_close_outcome", None)
    transport = getattr(launch, "transport_retirement_receipt", None)
    if transport != ("retired", custody.run_id) and (
        terminal_event is not True or stream_close is not True
    ):
        return None, _refusal("run_transport_unsettled")
    status_receipt = _canonical_status(adapter, custody, launch)
    if status_receipt is None:
        return None, _refusal("status_commit_unproven")
    if getattr(launch, "status_receipt", None) is None:
        return None, _refusal("status_writer_receipt_missing")
    if not _status_receipt_matches(launch.status_receipt, status_receipt):
        return None, _refusal("status_writer_receipt_mismatch")
    delivery = _delivery_outcome(launch, status_receipt)
    if delivery is None:
        return None, _refusal("delivery_finalizer_unproven")
    if delivery.get("kind") == "bot_delivery" and not _status_receipt_matches(
            status_receipt, {
                "run_id": custody.run_id,
                "storage": "session_admission",
                "admission_id": delivery.get("admission_id"),
                "generation": delivery.get("generation"),
                "principal_id": delivery.get("principal_id"),
                "target_session_id": getattr(custody, "admission_session_id", None),
        }):
        return None, _refusal("delivery_status_receipt_mismatch")
    process_receipt = custody.source_process_receipt()
    if process_receipt is None:
        return None, _refusal("process_receipt_unproven")
    return {
        "run_id": custody.run_id,
        "status_receipt": status_receipt,
        "delivery": delivery,
        "worker_task_done": True,
        "terminal_event": terminal_event,
        "stream_close": stream_close,
        "transport_retired": transport,
        "control_finalizer": launch.control_finalizer_receipt,
        "process_receipt": process_receipt,
    }, None


def owner_evidence(adapter: Any, *, process_instance: str, authority_epoch: int,
                   close_generation: int, owner_id: str):
    """Read every retained launch for one adapter and return source-owned quiescence evidence."""
    if getattr(adapter, "_api_run_owner_registered", False) is not True:
        return None
    descriptor = getattr(getattr(adapter, "gateway_runner", None), "session_runtime_descriptor", None) or {}
    if descriptor.get("instance_id") != process_instance or descriptor.get("authority_epoch") != authority_epoch:
        return None
    launches = getattr(adapter, "_run_launches", None)
    custody_table = getattr(adapter, "_run_custody", None)
    if not isinstance(launches, dict) or not launches or not isinstance(custody_table, dict):
        return None
    # A reservation that has no retained original launch, or an original launch whose custody
    # record was dropped, means the source graph is incomplete for this close generation.
    if set(launches) != set(custody_table):
        return None
    witnesses = []
    for run_id, launch in tuple(launches.items()):
        custody = custody_table.get(run_id)
        if (custody is None or getattr(launch, "owner", None) is not adapter
                or getattr(launch, "custody", None) is not custody
                or getattr(custody, "released", False) is not True):
            return None
        if getattr(custody, "unknown_reason", None):
            return None
        if (
            custody.process_instance != process_instance
            or custody.authority_epoch != authority_epoch
            or custody.handle.close_generation != close_generation
        ):
            return None
        witness, refusal = read_run_witness(adapter, custody)
        if refusal is not None:
            return None
        witnesses.append(witness)
    process_receipts = tuple(witness["process_receipt"] for witness in witnesses)
    if not all(isinstance(receipt, dict) and receipt.get("observed") is True
               and receipt.get("kind") in {
                   "parent_handle", "I15_process_registry_terminal", "I03_direct_executor"
               }
               for receipt in process_receipts):
        return None
    return {
        "process_instance": process_instance,
        "authority_epoch": authority_epoch,
        "close_generation": close_generation,
        "owner_id": owner_id,
        "observed": True,
        "cross_process_proof": all(
            isinstance(receipt, dict) and receipt.get("cross_process_proof") is True
            for receipt in process_receipts),
        "run_ids": tuple(sorted(launches)),
        "witnesses": tuple(witnesses),
        "process_receipts": process_receipts,
    }
