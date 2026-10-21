"""Typed finalizer completion for an authenticated API session run.

The runtime coordinator remains the only lease ledger.  This module keeps one exact admission
completion object alive between the API worker's durable result and the authority's final delivery;
it is an index of that object, not a second ownership map.

The API drain has two source-owned phases. ``stage_delivery`` consumes the typed transport and
result readbacks before the API waiter is released. ``settle_after_child`` runs only after the
bound I03 child has produced its own terminal release evidence; it authenticates the parent
receipt before asking the authority operations mixin to retire that parent. This ordering keeps a
missing or stale parent recoverable and avoids making the API observer await a child finalizer that
can only run after the observer's waiter is released.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any, Optional

from gateway.runtime_custody import CustodyHandle, CustodyRefusal


_REPLAY_AUDIT_TTL_SECONDS = 300.0
_REPLAY_AUDIT_MAX_ENTRIES = 256


def _replay_now() -> float:
    return time.monotonic()


@dataclass(frozen=True)
class ApiFinalizerCompletionReceipt:
    admission_id: str
    run_id: Optional[str]
    state: str
    profile_id: Optional[str]
    authority_epoch: Optional[int]
    process_instance: Optional[str]
    close_generation: Optional[int]
    execution_generation: Optional[int]
    session_id: Optional[str]
    principal: Optional[str]
    child_owner_id: Optional[str]
    parent_owner_id: Optional[str]
    finalizer_ok: bool
    release_ok: bool
    waiter_delivered: bool
    delivery_attempted: bool
    delivery_delivered: bool
    delivery_effect_state: Optional[str]
    delivery_admission_id: Optional[str]
    reason: Optional[str] = None

    def as_wire(self) -> dict[str, Any]:
        return {
            "admission_id": self.admission_id,
            "run_id": self.run_id,
            "state": self.state,
            "profile_id": self.profile_id,
            "authority_epoch": self.authority_epoch,
            "process_instance": self.process_instance,
            "close_generation": self.close_generation,
            "execution_generation": self.execution_generation,
            "session_id": self.session_id,
            "principal": self.principal,
            "child_owner_id": self.child_owner_id,
            "parent_owner_id": self.parent_owner_id,
            "finalizer_ok": self.finalizer_ok,
            "release_ok": self.release_ok,
            "waiter_delivered": self.waiter_delivered,
            "delivery_attempted": self.delivery_attempted,
            "delivery_delivered": self.delivery_delivered,
            "delivery_effect_state": self.delivery_effect_state,
            "delivery_admission_id": self.delivery_admission_id,
            **({"reason": self.reason} if self.reason else {}),
        }


@dataclass(frozen=True)
class ApiFinalizerRecoveryReceipt:
    """A typed, linked reconciliation result for an earlier UNKNOWN receipt.

    The original completion receipt remains immutable on the completion object.  This separate
    receipt is only issued after the source-owned launch, durable result, process, transport and
    child-release witnesses have been read back again for the same identity.
    """

    admission_id: str
    run_id: Optional[str]
    original_unknown: ApiFinalizerCompletionReceipt
    recovered_completion: ApiFinalizerCompletionReceipt
    identity: tuple[tuple[str, Any], ...]
    source_evidence: tuple[tuple[str, Any], ...]
    child_evidence: tuple[tuple[str, Any], ...]

    def as_wire(self) -> dict[str, Any]:
        return {
            "admission_id": self.admission_id,
            "run_id": self.run_id,
            "status": "reconciled",
            "original_unknown": self.original_unknown.as_wire(),
            "completion": self.recovered_completion.as_wire(),
            "identity": dict(self.identity),
            "source_evidence": dict(self.source_evidence),
            "child_evidence": dict(self.child_evidence),
        }


def _optional_int(value: Any) -> Optional[int]:
    return value if type(value) is int and value >= 0 else None


def _optional_str(value: Any) -> Optional[str]:
    return value if isinstance(value, str) and value else None


def _json_commitment(value: Any) -> Optional[str]:
    try:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        return None
    return hashlib.sha256(encoded).hexdigest()


def _delivery_commitment(receipt: Any) -> Optional[str]:
    fields = {
        "admission_id": getattr(receipt, "admission_id", None),
        "attempted": getattr(receipt, "attempted", None),
        "delivered": getattr(receipt, "delivered", None),
        "reason": getattr(receipt, "reason", None),
        "effect_state": getattr(receipt, "effect_state", None),
    }
    if type(fields["admission_id"]) is not str or not fields["admission_id"]:
        return None
    if type(fields["attempted"]) is not bool or type(fields["delivered"]) is not bool:
        return None
    if not isinstance(fields["effect_state"], str):
        return None
    return _json_commitment(fields)


def _retained_custody_owner_is_exact(custody: Any) -> bool:
    """Check the retained child and parent tuples against this coordinator."""
    coordinator = getattr(custody, "coordinator", None)
    handle = getattr(custody, "handle", None)
    coordinator_id = getattr(coordinator, "_coordinator_id", None)
    if (
        not isinstance(handle, CustodyHandle)
        or not isinstance(coordinator_id, str)
        or handle.coordinator_id != coordinator_id
        or handle.profile_id != getattr(custody, "profile_id", None)
        or handle.authority_epoch != getattr(custody, "authority_epoch", None)
        or handle.process_instance != getattr(custody, "process_instance", None)
        or getattr(coordinator, "process_instance", None)
        != getattr(custody, "process_instance", None)
        or getattr(coordinator, "authority_epoch", None)
        != getattr(custody, "authority_epoch", None)
    ):
        return False
    if getattr(custody, "parent_bound", None) is True:
        parent = getattr(custody, "parent_handle", None)
        if (
            not isinstance(parent, CustodyHandle)
            or parent.owner_class not in {"I02_api_session_turn", "I04_fifo_turn_execution"}
            or parent.coordinator_id != coordinator_id
            or parent.profile_id != getattr(custody, "profile_id", None)
            or parent.authority_epoch != getattr(custody, "authority_epoch", None)
            or parent.process_instance != getattr(custody, "process_instance", None)
            or parent.close_generation != handle.close_generation
        ):
            return False
    return True


def _retained_custody_status_is_exact(custody: Any) -> bool:
    """Read and validate the current durable terminal identity before a repeat."""
    try:
        status_receipt = custody.authoritative_status_receipt()
    except (AttributeError, ImportError, KeyError, LookupError, OSError, RuntimeError,
            TypeError, ValueError, sqlite3.Error):
        return False
    if not isinstance(status_receipt, dict) or status_receipt.get("readback") is not True:
        return False
    if getattr(custody, "reservation_key", None) is None:
        from gateway.platforms.api_server_run_custody import _TERMINAL_RUN_STATUSES
        status = status_receipt.get("status")
        if (
            not isinstance(status, dict)
            or status.get("status") not in _TERMINAL_RUN_STATUSES
            or status_receipt.get("admission_id") != getattr(custody, "admission_id", None)
            or status_receipt.get("generation")
            != getattr(custody, "execution_generation", None)
        ):
            return False
    validator = getattr(custody, "mark_status_receipt", None)
    return callable(validator) and validator(
        status_receipt, require_store=getattr(custody, "reservation_key", None) is not None,
    ) is None


def _retained_terminal_release_is_exact(custody: Any) -> bool:
    """Accept a repeat only with exact ownership, readback, and release evidence."""
    receipt = getattr(custody, "terminal_release_receipt", None)
    if (
        getattr(custody, "released", None) is not True
        or getattr(custody, "unknown_reason", None) is not None
        or not isinstance(receipt, dict)
        or getattr(custody, "status_persisted", None) is not True
        or getattr(custody, "admission_pending", None) is not False
        or getattr(custody, "worker_done", None) is not True
        or getattr(custody, "control_finalizers", None) is not True
        or not (
            getattr(custody, "transport_retired", None) is True
            or (getattr(custody, "event_enqueued", None) is True
                and getattr(custody, "stream_closed", None) is True)
        )
    ):
        return False
    if not _retained_custody_owner_is_exact(custody):
        return False
    if not _retained_custody_status_is_exact(custody):
        return False
    evidence = getattr(custody, "_terminal_release_evidence", None)
    if not callable(evidence):
        return False
    expected = evidence()
    return isinstance(expected, dict) and set(receipt) == set(expected) and all(
        receipt[key] == value for key, value in expected.items()
    )


def _recovery_terminal_release_is_exact(custody: Any) -> bool:
    """Validate a released child while retaining its historical UNKNOWN reason."""
    if (
        getattr(custody, "released", None) is not True
        or getattr(custody, "admission_pending", None) is not False
        or getattr(custody, "worker_done", None) is not True
        or getattr(custody, "control_finalizers", None) is not True
        or not (
            getattr(custody, "transport_retired", None) is True
            or (
                getattr(custody, "event_enqueued", None) is True
                and getattr(custody, "stream_closed", None) is True
            )
        )
    ):
        return False
    if not _retained_custody_owner_is_exact(custody):
        return False
    if not _retained_custody_status_is_exact(custody):
        return False
    evidence = getattr(custody, "_terminal_release_evidence", None)
    receipt = getattr(custody, "terminal_release_receipt", None)
    if not callable(evidence) or not isinstance(receipt, dict):
        return False
    expected = evidence()
    return isinstance(expected, dict) and set(receipt) == set(expected) and all(
        receipt[key] == value for key, value in expected.items()
    )


def _recovery_evidence_projection(source: dict[str, Any], custody: Any, delivery_receipt: Any):
    """Keep typed source/child evidence in a replay receipt, never result payloads."""
    status = source.get("status_receipt")
    process = source.get("process_receipt")
    child = getattr(custody, "terminal_release_receipt", None)
    if not isinstance(status, dict) or not isinstance(process, dict) or not isinstance(child, dict):
        return None
    result_commitment = _json_commitment({
        "outcome": status.get("terminal_outcome"),
        "result": status.get("terminal_result"),
        "usage": status.get("terminal_usage"),
    })
    transport_commitment = _delivery_commitment(delivery_receipt)
    if result_commitment is None or transport_commitment is None:
        return None
    source_fields = {
        "status_readback": status.get("readback"),
        "status_storage": status.get("storage"),
        "status_rowcount": status.get("rowcount"),
        "status_run_id": status.get("run_id"),
        "status_admission_id": status.get("admission_id"),
        "status_generation": status.get("generation"),
        "status_principal_id": status.get("principal_id"),
        "status_session_id": status.get("target_session_id"),
        "status_name": (status.get("status") or {}).get("status")
            if isinstance(status.get("status"), dict) else None,
        "process_kind": process.get("kind"),
        "process_instance": process.get("process_instance"),
        "process_authority_epoch": process.get("authority_epoch"),
        "process_close_generation": process.get("close_generation"),
        "process_owner_id": process.get("owner_id"),
        "process_task_id": process.get("task_id"),
        "process_observed": process.get("observed"),
        "process_cross_process_proof": process.get("cross_process_proof"),
        "worker_done": source.get("worker_done"),
        "control_finalizers": source.get("control_finalizers"),
        "transport_retired": source.get("transport_retired"),
        "event_enqueued": source.get("event_enqueued"),
        "stream_closed": source.get("stream_closed"),
        "result_commitment": result_commitment,
        "transport_commitment": transport_commitment,
    }
    child_fields = {
        "child_owner_id": child.get("child_owner_id"),
        "parent_owner_id": child.get("parent_owner_id"),
        "profile_id": child.get("profile_id"),
        "authority_epoch": child.get("authority_epoch"),
        "process_instance": child.get("process_instance"),
        "close_generation": child.get("close_generation"),
        "admission_id": child.get("admission_id"),
        "released": child.get("released"),
    }
    return tuple(sorted(source_fields.items())), tuple(sorted(child_fields.items()))


def _read_exact_recovery_evidence(
    custody: Any, adapter: Any, expected_identity: dict[str, Any], delivery_receipt: Any,
):
    """Re-read the same source witnesses for both first recovery and lost-ACK replay."""
    try:
        source = custody.source_recovery_receipt(adapter)
    except (AttributeError, ImportError, KeyError, LookupError, OSError, RuntimeError,
            TypeError, ValueError, sqlite3.Error):
        return None
    if not isinstance(source, dict) or any(
        source.get(key) != value for key, value in expected_identity.items()
    ):
        return None
    if source.get("worker_done") is not True or source.get("control_finalizers") is not True:
        return None
    if source.get("transport_retired") is not True and not (
        source.get("event_enqueued") is True and source.get("stream_closed") is True
    ):
        return None
    try:
        process_receipt = custody.source_process_receipt()
    except (AttributeError, ImportError, KeyError, LookupError, OSError, RuntimeError,
            TypeError, ValueError, sqlite3.Error):
        return None
    if source.get("process_receipt") != process_receipt:
        return None
    evidence = _recovery_evidence_projection(source, custody, delivery_receipt)
    return (source, evidence) if evidence is not None else None


class ApiFinalizerCompletion:
    """One source-owned completion fence for one exact API admission."""

    def __init__(self, *, authority: Any, custody: Any, parent: CustodyHandle,
                 admission_id: str, future: asyncio.Future):
        self.authority = authority
        self.custody = custody
        self.parent = parent
        self.admission_id = admission_id
        self.future = future
        self._receipt: Optional[ApiFinalizerCompletionReceipt] = None
        self._parent_release_receipt: Optional[dict[str, Any]] = None
        self._parent_release_candidate: Optional[dict[str, Any]] = None
        self._delivery_receipt: Any = None
        self._waiter_delivered: Optional[bool] = None
        self._finalizer_ok = False
        self._delivery_staged = False
        self._recovery_receipt: Optional[ApiFinalizerRecoveryReceipt] = None
        self._recovery_in_progress = False
        self._recovery_adapter: Any = None
        self._recovery_identity: Optional[dict[str, Any]] = None
        self._recovery_source_evidence: Any = None
        self._replay_retained_at: Optional[float] = None
        self._replay_expires_at: Optional[float] = None

    @classmethod
    def begin(cls, authority: Any, custody: Any, parent: Any, admission_id: Any):
        if not isinstance(parent, CustodyHandle) or not isinstance(admission_id, str) or not admission_id:
            return None, CustodyRefusal("finalizer_completion_identity_missing", "I02_api_session_turn")
        if getattr(custody, "producer_phase", None) != "api_i02":
            return None, CustodyRefusal("finalizer_completion_phase_mismatch", "I02_api_session_turn")
        if getattr(custody, "parent_kind", None) != "I02_api_session_turn":
            return None, CustodyRefusal("finalizer_completion_parent_mismatch", "I02_api_session_turn")
        if getattr(custody, "admission_id", None) != admission_id:
            return None, CustodyRefusal("finalizer_completion_admission_mismatch", "I02_api_session_turn")
        handles = getattr(authority, "_preservation_handles", None)
        current = handles.get(admission_id) if isinstance(handles, dict) else None
        if current is not parent:
            return None, CustodyRefusal("finalizer_completion_parent_replaced", "I02_api_session_turn")
        bound_parent, refusal = custody.bound_parent_handle()
        if refusal is not None or bound_parent is not parent:
            return None, CustodyRefusal("finalizer_completion_parent_unmapped", "I02_api_session_turn")
        coordinator = getattr(custody, "coordinator", None)
        bound_parent_id = getattr(parent.lease, "lease_id", None)
        if (coordinator is None or not callable(getattr(coordinator, "bound_parent", None))
                or coordinator.bound_parent(custody.handle) != bound_parent_id):
            return None, CustodyRefusal("finalizer_completion_edge_missing", "I02_api_session_turn")
        generation = getattr(custody, "execution_generation", None)
        if generation is not None and (type(generation) is not int or generation < 0):
            return None, CustodyRefusal("finalizer_completion_generation_unknown", "I02_api_session_turn")
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return None, CustodyRefusal("finalizer_completion_loop_missing", "I02_api_session_turn")
        registry = getattr(authority, "_api_finalizer_completions", None)
        if registry is None:
            registry = {}
            authority._api_finalizer_completions = registry
        elif not isinstance(registry, dict):
            return None, CustodyRefusal("finalizer_completion_registry_invalid", "I02_api_session_turn")
        prune_replay_registry(registry)
        existing = registry.get(admission_id)
        if existing is not None:
            if isinstance(existing, cls) and existing.custody is custody and existing.parent is parent:
                return existing, None
            return None, CustodyRefusal("finalizer_completion_collision", "I02_api_session_turn")
        completion = cls(
            authority=authority,
            custody=custody,
            parent=parent,
            admission_id=admission_id,
            future=loop.create_future(),
        )
        registry[admission_id] = completion
        return completion, None

    def _child_release_identity_matches(self) -> bool:
        if getattr(self.custody, "released", False) is not True:
            return False
        child_receipt = getattr(self.custody, "terminal_release_receipt", None)
        if not isinstance(child_receipt, dict) or child_receipt.get("released") is not True:
            return False
        expected = {
            "child_owner_id": getattr(self.custody.handle, "owner_id", None),
            "parent_owner_id": getattr(self.parent, "owner_id", None),
            "profile_id": getattr(self.custody, "profile_id", None),
            "authority_epoch": getattr(self.custody, "authority_epoch", None),
            "process_instance": getattr(self.custody, "process_instance", None),
            "close_generation": getattr(self.custody.handle, "close_generation", None),
            "admission_id": self.admission_id,
            "released": True,
        }
        return all(child_receipt.get(key) == value for key, value in expected.items())

    def _identity_matches(self) -> bool:
        if getattr(self.custody, "admission_id", None) != self.admission_id:
            return False
        if getattr(self.custody, "parent_handle", None) is not self.parent:
            return False
        handles = getattr(self.authority, "_preservation_handles", None)
        current_parent = handles.get(self.admission_id) if isinstance(handles, dict) else None
        if current_parent is not None and current_parent is not self.parent:
            return False
        bound_parent, refusal = self.custody.bound_parent_handle()
        if refusal is None and bound_parent is self.parent:
            return True
        return self._child_release_identity_matches()

    def _parent_process_receipt(self) -> Optional[dict[str, Any]]:
        if not self._child_release_identity_matches() or not self._identity_matches():
            return None
        process_receipt = self.custody.source_process_receipt()
        if (
            not isinstance(process_receipt, dict)
            or process_receipt.get("kind") != "parent_handle"
            or process_receipt.get("owner_id") != self.parent.owner_id
            or process_receipt.get("task_id") is not None
            or process_receipt.get("process_instance") != self.custody.process_instance
            or process_receipt.get("authority_epoch") != self.custody.authority_epoch
            or process_receipt.get("close_generation") != self.custody.handle.close_generation
            or process_receipt.get("observed") is not True
            or process_receipt.get("cross_process_proof") is not False
        ):
            return None
        return process_receipt

    def prepare_parent_release(self) -> bool:
        """Authenticate the child and parent witness before the coordinator can delete the parent."""
        if self._receipt is not None or getattr(self.custody, "unknown_reason", None):
            return False
        process_receipt = self._parent_process_receipt()
        if process_receipt is None:
            return False
        self._parent_release_candidate = {
            "parent_owner_id": self.parent.owner_id,
            "parent_lease_id": self.parent.lease.lease_id,
            "coordinator_id": self.parent.coordinator_id,
            "profile_id": self.custody.profile_id,
            "authority_epoch": self.custody.authority_epoch,
            "process_instance": self.custody.process_instance,
            "close_generation": self.custody.handle.close_generation,
            "admission_id": self.admission_id,
            "process_receipt": dict(process_receipt),
            "released": False,
        }
        return True

    def confirm_parent_release(self) -> bool:
        candidate = self._parent_release_candidate
        if not isinstance(candidate, dict) or candidate.get("released") is not False:
            return False
        self._parent_release_receipt = {**candidate, "released": True}
        return True

    def _build_receipt(self, *, state: str, finalizer_ok: bool, release_ok: bool,
                 waiter_delivered: bool, delivery_receipt: Any, reason: Optional[str] = None):
        return ApiFinalizerCompletionReceipt(
            admission_id=self.admission_id,
            run_id=_optional_str(getattr(self.custody, "run_id", None)),
            state=state,
            profile_id=_optional_str(getattr(self.custody, "profile_id", None)),
            authority_epoch=_optional_int(getattr(self.custody, "authority_epoch", None)),
            process_instance=_optional_str(getattr(self.custody, "process_instance", None)),
            close_generation=_optional_int(getattr(self.custody.handle, "close_generation", None)),
            execution_generation=_optional_int(getattr(self.custody, "execution_generation", None)),
            session_id=_optional_str(getattr(self.custody, "admission_session_id", None)),
            principal=_optional_str(getattr(self.custody, "producer_principal", None)),
            child_owner_id=_optional_str(getattr(self.custody.handle, "owner_id", None)),
            parent_owner_id=_optional_str(getattr(self.parent, "owner_id", None)),
            finalizer_ok=finalizer_ok,
            release_ok=release_ok,
            waiter_delivered=waiter_delivered,
            delivery_attempted=(type(getattr(delivery_receipt, "attempted", None)) is bool
                                and delivery_receipt.attempted),
            delivery_delivered=(type(getattr(delivery_receipt, "delivered", None)) is bool
                                and delivery_receipt.delivered),
            delivery_effect_state=(getattr(delivery_receipt, "effect_state", None)
                                   if isinstance(getattr(delivery_receipt, "effect_state", None), str)
                                   else None),
            delivery_admission_id=_optional_str(getattr(delivery_receipt, "admission_id", None)),
            reason=reason,
        )

    def _settle(self, receipt: ApiFinalizerCompletionReceipt):
        if self._receipt is not None:
            return self._receipt
        self._receipt = receipt
        self.custody.finalizer_completion_receipt = receipt
        if not self.future.done():
            self.future.set_result(receipt)
        return receipt

    def fail(self, reason: str):
        if self._receipt is not None:
            return self._receipt
        if not isinstance(reason, str) or not reason:
            reason = "finalizer_completion_unknown"
        marker = getattr(self.authority, "_preservation_mark_unknown", None)
        marker_refused = False
        if callable(marker):
            try:
                marker(self.admission_id, reason)
            except BaseException:  # health: allow BLE001 -- opaque marker retains UNKNOWN
                marker_refused = True
        if marker_refused:
            reason = f"{reason}:marker_refused"
        receipt = self._build_receipt(
            state="unknown", finalizer_ok=False, release_ok=False,
            waiter_delivered=(self._waiter_delivered is True),
            delivery_receipt=self._delivery_receipt,
            reason=reason,
        )
        return self._settle(receipt)

    @staticmethod
    def _valid_delivery_receipt(delivery_receipt: Any, admission_id: str) -> bool:
        try:
            from gateway.session_ingress import DeliveryReceipt
        except (ImportError, RuntimeError):
            return False
        if type(delivery_receipt) is not DeliveryReceipt:
            return False
        attempted = delivery_receipt.attempted
        delivered = delivery_receipt.delivered
        effect = delivery_receipt.effect_state
        if (
            type(attempted) is not bool
            or type(delivered) is not bool
            or not isinstance(effect, str)
            or effect not in {"none", "delivered", "ambiguous"}
            or delivery_receipt.admission_id != admission_id
            or (delivered and not attempted)
            or (effect == "delivered" and (not attempted or not delivered or delivery_receipt.reason is not None))
            or (effect == "ambiguous" and delivered)
            or (effect == "none" and delivery_receipt.reason == "delivery_ambiguous")
        ):
            return False
        return True

    def stage_delivery(self, *, delivery_receipt: Any, waiter_delivered: bool,
                       finalizer_ok: bool, reason: Optional[str] = None) -> bool:
        """Consume transport/result evidence before the API waiter can unblock its I03 child."""
        if self._receipt is not None:
            return self._receipt.state == "complete"
        if self._delivery_staged:
            return True
        if not self._identity_matches():
            self.fail("finalizer_completion_identity_changed")
            return False
        generation_refusal = self.custody.refresh_admission_generation()
        if generation_refusal is not None:
            self.fail("finalizer_completion_generation_unknown")
            return False
        if type(waiter_delivered) is not bool or type(finalizer_ok) is not bool:
            self.fail("finalizer_completion_flag_invalid")
            return False
        if not self._valid_delivery_receipt(delivery_receipt, self.admission_id):
            self.fail("finalizer_completion_receipt_invalid")
            return False
        source_receipt = self.custody.authoritative_status_receipt()
        # A /v1/runs stream also owns a keyed idempotency reservation, but that writer is
        # finalized after the API observer returns.  The settled API admission is the earlier
        # canonical result witness; consume it here rather than mistaking a running keyed row
        # for the API result or waiting on the stream task that needs this waiter to return.
        if (not isinstance(source_receipt, dict)
                or source_receipt.get("result_readback") is not True):
            authority = getattr(self.custody, "admission_authority", None)
            try:
                from gateway.platforms.api_server_run_custody import _session_status_receipt
                source_receipt = _session_status_receipt(
                    authority, self.custody,
                    expected_session_id=getattr(self.custody, "admission_session_id", None),
                )
            except (AttributeError, ImportError, KeyError, LookupError, OSError, RuntimeError,
                    TypeError, ValueError):
                source_receipt = None
        if (
            not isinstance(source_receipt, dict)
            or source_receipt.get("readback") is not True
            or source_receipt.get("result_readback") is not True
            or source_receipt.get("admission_id") != self.admission_id
        ):
            self.fail("finalizer_completion_source_readback_missing")
            return False
        if not finalizer_ok:
            self.fail(reason or "finalizer_completion_refused")
            return False
        self._delivery_receipt = delivery_receipt
        self._waiter_delivered = waiter_delivered
        self._finalizer_ok = True
        self._delivery_staged = True
        return True

    def note_waiter_delivery(self, delivered: bool) -> bool:
        if type(delivered) is not bool or not self._delivery_staged or self._receipt is not None:
            return False
        self._waiter_delivered = delivered
        return True

    def settle_after_child(self):
        """Release the authenticated I02 parent only after the bound I03 child is terminal."""
        if self._receipt is not None:
            return self._receipt
        if not self._delivery_staged or self._waiter_delivered is not True:
            return self.fail("finalizer_completion_delivery_pending")
        if not (self._recovery_in_progress and isinstance(self._parent_release_receipt, dict)):
            if not self.prepare_parent_release():
                return self.fail("api_parent_release_receipt_missing")
            release = getattr(self.authority, "_preservation_release_terminal", None)
            if not callable(release):
                return self.fail("api_parent_release_unavailable")
            try:
                release_ok = release(self.admission_id)
            except BaseException:
                self.fail("api_parent_release_error")
                raise
            if release_ok is not True or not self.confirm_parent_release():
                return self.fail("api_parent_release_pending")
        released = getattr(self.authority, "_preservation_i02_parent_released", None)
        if not callable(released) or not released(
            self.admission_id, self.parent, self._parent_release_receipt,
        ):
            return self.fail("api_parent_release_readback_missing")
        if self._recovery_in_progress:
            reread = _read_exact_recovery_evidence(
                self.custody, self._recovery_adapter, self._recovery_identity or {},
                self._delivery_receipt,
            )
            if reread is None:
                return self.fail("finalizer_recovery_readback_missing")
            self._recovery_source_evidence = reread
        commit = getattr(self.authority, "_preservation_commit_i02_parent_release", None)
        if not callable(commit) or not commit(
            self.admission_id, self.parent, self._parent_release_receipt,
        ):
            return self.fail("api_parent_release_retirement_pending")
        if not callable(released) or not released(
            self.admission_id, self.parent, self._parent_release_receipt,
        ):
            return self.fail("api_parent_release_readback_missing")
        return self._settle(self._build_receipt(
            state="complete", finalizer_ok=True, release_ok=True,
            waiter_delivered=True, delivery_receipt=self._delivery_receipt,
        ))

    def _restore_recovery_unknown(self, original, reason: str) -> None:
        """Put the exact original UNKNOWN back when a recovery boundary refuses."""
        self._receipt = original
        self.custody.finalizer_completion_receipt = original
        if not getattr(self.custody, "unknown_reason", None):
            self.custody.unknown_reason = original.reason or reason
        marker = getattr(self.authority, "_preservation_mark_unknown", None)
        handles = getattr(self.authority, "_preservation_handles", None)
        if (
            callable(marker)
            and isinstance(handles, dict)
            and handles.get(self.admission_id) is self.parent
        ):
            try:
                marker(self.admission_id, self.custody.unknown_reason)
            except BaseException:  # health: allow BLE001 -- preserve the original recovery error
                coordinator_resolver = getattr(self.authority, "_preservation_coordinator", None)
                coordinator = coordinator_resolver() if callable(coordinator_resolver) else None
                if coordinator is not None:
                    try:
                        coordinator.mark_unknown(self.parent, self.custody.unknown_reason)
                    except BaseException:  # health: allow BLE001 -- retain the first error
                        coordinator = None
        self._recovery_in_progress = False
        self._recovery_adapter = None
        self._recovery_identity = None
        self._recovery_source_evidence = None
        self.custody._finalizer_recovery_in_progress = False

    def recover_unknown(self, adapter: Any, expected_identity: dict[str, Any]):
        """Run the original finalizer/release boundary for one authenticated UNKNOWN."""
        expected = tuple(sorted(expected_identity.items()))
        if self._recovery_in_progress:
            return None
        if self._recovery_receipt is not None:
            if (
                self._recovery_receipt.identity == expected
                and self.receipt is not None
                and self.receipt.state == "complete"
                and self._identity_matches()
                and _recovery_terminal_release_is_exact(self.custody)
                and self._valid_delivery_receipt(self._delivery_receipt, self.admission_id)
                and self._waiter_delivered is True
            ):
                reread = _read_exact_recovery_evidence(
                    self.custody, adapter, expected_identity, self._delivery_receipt,
                )
                if reread is not None and reread[1] == (
                    self._recovery_receipt.source_evidence,
                    self._recovery_receipt.child_evidence,
                ):
                    return self._recovery_receipt
            return None
        original = self._receipt
        if (
            not isinstance(original, ApiFinalizerCompletionReceipt)
            or original.state != "unknown"
            or not getattr(self.custody, "unknown_reason", None)
            or expected != tuple(sorted(self.custody.recovery_identity().items()))
            or completion_for_custody(self.custody) is not self
            or not self._identity_matches()
            or not _recovery_terminal_release_is_exact(self.custody)
            or not self._valid_delivery_receipt(self._delivery_receipt, self.admission_id)
            or self._waiter_delivered is not True
        ):
            return None
        committed_release = isinstance(self._parent_release_receipt, dict)
        if committed_release:
            released = getattr(self.authority, "_preservation_i02_parent_released", None)
            if not callable(released) or not released(
                self.admission_id, self.parent, self._parent_release_receipt,
            ):
                return None
        else:
            reopen = getattr(self.authority, "_preservation_recover_i02_unknown", None)
            if not callable(reopen):
                return None
            refusal = reopen(self.admission_id, self.parent)
            if refusal is not None:
                return None
        self._recovery_in_progress = True
        self._recovery_adapter = adapter
        self._recovery_identity = dict(expected_identity)
        self._recovery_source_evidence = None
        self.custody._finalizer_recovery_in_progress = True
        reread = _read_exact_recovery_evidence(
            self.custody, adapter, expected_identity, self._delivery_receipt,
        )
        if reread is None:
            self._restore_recovery_unknown(original, "finalizer_recovery_readback_missing")
            return None
        self._receipt = None
        self._parent_release_candidate = None
        self.custody.unknown_reason = None
        try:
            recovered = self.settle_after_child()
        except BaseException:
            self._restore_recovery_unknown(original, "finalizer_recovery_error")
            raise
        if getattr(recovered, "state", None) != "complete":
            self._restore_recovery_unknown(
                original,
                getattr(recovered, "reason", None) or "finalizer_recovery_unknown",
            )
            return None
        post = self._recovery_source_evidence
        if not isinstance(post, tuple) or len(post) != 2:
            self._restore_recovery_unknown(original, "finalizer_recovery_readback_missing")
            return None
        _source, evidence = post
        source_evidence, child_evidence = evidence
        recovery = ApiFinalizerRecoveryReceipt(
            admission_id=self.admission_id,
            run_id=_optional_str(getattr(self.custody, "run_id", None)),
            original_unknown=original,
            recovered_completion=recovered,
            identity=expected,
            source_evidence=source_evidence,
            child_evidence=child_evidence,
        )
        self._recovery_receipt = recovery
        self.custody.finalizer_recovery_receipt = recovery
        self._recovery_in_progress = False
        self._recovery_adapter = None
        self._recovery_identity = None
        self._recovery_source_evidence = None
        self.custody._finalizer_recovery_in_progress = False
        return recovery

    def retain_replay(self) -> bool:
        """Keep this completed recovery as an idempotent bounded lost-ACK audit."""
        registry = getattr(self.authority, "_api_finalizer_completions", None)
        if (
            self._recovery_receipt is None
            or self.receipt is None
            or self.receipt.state != "complete"
            or not isinstance(registry, dict)
            or registry.get(self.admission_id) is not self
        ):
            return False
        now = _replay_now()
        self._replay_retained_at = now
        self._replay_expires_at = now + _REPLAY_AUDIT_TTL_SECONDS
        prune_replay_registry(registry, now=now)
        return True

    def _replay_audit_eligible(self) -> bool:
        custody = self.custody
        return (
            self._recovery_receipt is not None
            and getattr(self.receipt, "state", None) == "complete"
            and getattr(custody, "released", False) is True
            and not getattr(custody, "unknown_reason", None)
            and self.replay_source_retired()
        )

    def _replay_expired(self, now: Optional[float] = None) -> bool:
        expires_at = self._replay_expires_at
        return expires_at is not None and (now if now is not None else _replay_now()) >= expires_at

    def consume_replay(self, expected_identity: dict[str, Any]):
        """Return the same exact replay while its bounded audit window remains."""
        registry = getattr(self.authority, "_api_finalizer_completions", None)
        if not isinstance(registry, dict):
            return None
        now = _replay_now()
        prune_replay_registry(registry, now=now)
        expected = tuple(sorted(expected_identity.items()))
        if (
            registry.get(self.admission_id) is not self
            or self._recovery_receipt is None
            or self._recovery_receipt.identity != expected
            or self._replay_expired(now)
            or not self._replay_audit_eligible()
        ):
            return None
        return self._recovery_receipt

    def replay_source_retired(self) -> bool:
        """Require exact source-map absence before consuming a retained response."""
        launch = getattr(self.custody, "source_launch", None)
        owner = getattr(launch, "owner", None)
        run_id = getattr(self.custody, "run_id", None)
        custody_table = getattr(owner, "_run_custody", None)
        launch_table = getattr(owner, "_run_launches", None)
        return (
            owner is not None
            and isinstance(run_id, str)
            and isinstance(custody_table, dict)
            and isinstance(launch_table, dict)
            and run_id not in custody_table
            and run_id not in launch_table
        )

    async def wait(self):
        try:
            return await asyncio.shield(self.future)
        except asyncio.CancelledError:
            self.fail("finalizer_completion_cancelled")
            raise

    def retire(self) -> bool:
        if self._receipt is None or self._receipt.state != "complete":
            return False
        registry = getattr(self.authority, "_api_finalizer_completions", None)
        if not isinstance(registry, dict) or registry.get(self.admission_id) is not self:
            return False
        prune_replay_registry(registry)
        if registry.get(self.admission_id) is not self:
            return False
        if self._replay_audit_eligible():
            return True
        registry.pop(self.admission_id, None)
        return True

    @property
    def receipt(self) -> Optional[ApiFinalizerCompletionReceipt]:
        return self._receipt

    @property
    def recovery_receipt(self) -> Optional[ApiFinalizerRecoveryReceipt]:
        return self._recovery_receipt


def prune_replay_registry(registry: Any, *, now: Optional[float] = None) -> int:
    """Prune only completed, source-retired replay audit entries opportunistically."""
    if not isinstance(registry, dict):
        return 0
    checked_now = _replay_now() if now is None else now
    removed = 0
    for key, completion in list(registry.items()):
        if (
            isinstance(completion, ApiFinalizerCompletion)
            and completion._replay_audit_eligible()
            and completion._replay_expired(checked_now)
            and registry.get(key) is completion
        ):
            registry.pop(key, None)
            removed += 1
    eligible = [
        (completion._replay_retained_at, key, completion)
        for key, completion in registry.items()
        if (
            isinstance(completion, ApiFinalizerCompletion)
            and completion._replay_retained_at is not None
            and completion._replay_audit_eligible()
        )
    ]
    eligible.sort(key=lambda item: (item[0], str(item[1])))
    for _, key, completion in eligible[:-_REPLAY_AUDIT_MAX_ENTRIES]:
        if registry.get(key) is completion:
            registry.pop(key, None)
            removed += 1
    return removed


def completion_for(authority: Any, admission_id: Any):
    registry = getattr(authority, "_api_finalizer_completions", None)
    if not isinstance(registry, dict) or not isinstance(admission_id, str):
        return None
    prune_replay_registry(registry)
    value = registry.get(admission_id)
    return value if isinstance(value, ApiFinalizerCompletion) else None


def completion_for_custody(custody: Any):
    """Return the registry entry only when it owns this exact I02 custody object."""
    if getattr(custody, "producer_phase", None) != "api_i02":
        return None
    authority = getattr(custody, "admission_authority", None)
    completion = completion_for(authority, getattr(custody, "admission_id", None))
    if completion is None or completion.custody is not custody:
        return None
    return completion


def _replay_admission_exact(completion: Any, custody: Any, authority: Any, run_id: Any) -> bool:
    if not isinstance(completion, ApiFinalizerCompletion):
        return False
    if (
        completion.authority is not authority
        or getattr(custody, "admission_authority", None) is not authority
        or getattr(custody, "run_id", None) != run_id
        or not isinstance(getattr(completion, "admission_id", None), str)
        or not completion.admission_id
        or getattr(custody, "admission_id", None) != completion.admission_id
    ):
        return False
    registry = getattr(authority, "_api_finalizer_completions", None)
    return isinstance(registry, dict) and registry.get(completion.admission_id) is completion


def _replay_authority_context_exact(completion: Any, custody: Any, authority: Any):
    resolver = getattr(authority, "_preservation_coordinator", None)
    try:
        coordinator = resolver() if callable(resolver) else None
    except (AttributeError, KeyError, LookupError, RuntimeError, TypeError, ValueError):
        return None
    profile_id = getattr(authority, "profile_id", None)
    authority_epoch = getattr(authority, "epoch", None)
    process_instance = getattr(authority, "instance_id", None)
    coordinator_id = getattr(coordinator, "_coordinator_id", None)
    if coordinator is None:
        return None
    if (
        not isinstance(profile_id, str)
        or not profile_id
        or type(authority_epoch) is not int
        or authority_epoch < 1
        or not isinstance(process_instance, str)
        or not process_instance
        or not isinstance(coordinator_id, str)
        or not coordinator_id
    ):
        return None
    if (
        getattr(custody, "coordinator", None) is not coordinator
        or getattr(coordinator, "process_instance", None) != process_instance
        or getattr(coordinator, "authority_epoch", None) != authority_epoch
        or getattr(custody, "profile_id", None) != profile_id
        or getattr(custody, "authority_epoch", None) != authority_epoch
        or getattr(custody, "process_instance", None) != process_instance
    ):
        return None
    if not _retained_custody_owner_is_exact(custody):
        return None
    if (
        getattr(custody, "parent_bound", None) is not True
        or getattr(custody, "parent_handle", None) is not completion.parent
    ):
        return None
    return coordinator_id, profile_id, authority_epoch, process_instance


def _replay_source_exact(
    completion: Any, custody: Any, adapter: Any, run_id: Any, coordinator_id: Any,
):
    handle = getattr(custody, "handle", None)
    launch = getattr(custody, "source_launch", None)
    owner = getattr(launch, "owner", None)
    if not isinstance(handle, CustodyHandle):
        return None
    if (
        getattr(handle, "coordinator_id", None) != coordinator_id
        or owner is not adapter
        or getattr(launch, "custody", None) is not custody
        or getattr(launch, "run_id", None) != run_id
    ):
        return None
    if (
        getattr(launch, "transport_retirement_receipt", None) != ("retired", run_id)
        or getattr(launch, "control_finalizer_receipt", None) != ("retired", run_id)
    ):
        return None
    idempotency_ids = getattr(owner, "_run_idempotency_ids", None)
    if not isinstance(idempotency_ids, set) or run_id in idempotency_ids:
        return None
    if not completion.replay_source_retired():
        return None
    return handle


def _replay_receipts_exact(
    completion: Any, custody: Any, run_id: Any, profile_id: Any, authority_epoch: Any,
    process_instance: Any, handle: CustodyHandle,
) -> bool:
    receipt = completion.receipt
    if not isinstance(receipt, ApiFinalizerCompletionReceipt):
        return False
    if (
        receipt.state != "complete"
        or receipt.admission_id != completion.admission_id
        or receipt.run_id != run_id
        or receipt.profile_id != profile_id
        or receipt.authority_epoch != authority_epoch
        or receipt.process_instance != process_instance
        or receipt.close_generation != getattr(handle, "close_generation", None)
        or receipt.execution_generation != getattr(custody, "execution_generation", None)
        or receipt.child_owner_id != getattr(handle, "owner_id", None)
        or receipt.parent_owner_id != getattr(completion.parent, "owner_id", None)
    ):
        return False
    recovery = completion.recovery_receipt
    if not isinstance(recovery, ApiFinalizerRecoveryReceipt):
        return False
    try:
        identity = tuple(sorted(custody.recovery_identity().items()))
    except (AttributeError, KeyError, LookupError, RuntimeError, TypeError, ValueError):
        return False
    if (
        recovery.admission_id != completion.admission_id
        or recovery.run_id != run_id
        or recovery.identity != identity
        or recovery.recovered_completion is not receipt
        or getattr(recovery.original_unknown, "state", None) != "unknown"
        or not recovery.source_evidence
        or not recovery.child_evidence
    ):
        return False
    if (
        getattr(custody, "finalizer_completion_receipt", None) is not receipt
        or getattr(custody, "finalizer_recovery_receipt", None) is not recovery
    ):
        return False
    return completion._replay_audit_eligible()


def _replay_completion_for_authority(
    completion: Any, authority: Any, adapter: Any, run_id: Any,
) -> bool:
    custody = getattr(completion, "custody", None)
    if not _replay_admission_exact(completion, custody, authority, run_id):
        return False
    context = _replay_authority_context_exact(completion, custody, authority)
    if context is None:
        return False
    coordinator_id, profile_id, authority_epoch, process_instance = context
    handle = _replay_source_exact(completion, custody, adapter, run_id, coordinator_id)
    if handle is None:
        return False
    return _replay_receipts_exact(
        completion, custody, run_id, profile_id, authority_epoch, process_instance, handle,
    )


def api_i02_completion_required(authority: Any, admission_id: Any) -> bool:
    """Identify a bound I02 parent even when its completion registry entry was lost."""
    if not isinstance(admission_id, str):
        return False
    handles = getattr(authority, "_preservation_handles", None)
    parent = handles.get(admission_id) if isinstance(handles, dict) else None
    return isinstance(parent, CustodyHandle) and parent.owner_class == "I02_api_session_turn"


def begin_api_finalizer_completion(authority: Any, custody: Any, parent: Any, admission_id: Any):
    return ApiFinalizerCompletion.begin(authority, custody, parent, admission_id)


def stage_api_finalizer_delivery(authority: Any, admission_id: Any, **kwargs):
    completion = completion_for(authority, admission_id)
    if completion is None:
        return False
    return completion.stage_delivery(**kwargs)


def settle_api_finalizer_completion(custody: Any):
    completion = completion_for_custody(custody)
    if completion is None:
        return None
    return completion.settle_after_child()


def fail_api_finalizer_completion(authority: Any, admission_id: Any, reason: str):
    completion = completion_for(authority, admission_id)
    return completion.fail(reason) if completion is not None else None
