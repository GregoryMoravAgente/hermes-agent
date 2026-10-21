"""Exact custody for one authenticated ``/v1/runs`` admission.

The API run maps are transport state only.  This module keeps the opaque coordinator handle alive
    until the durable result, terminal transport (or an explicit transport-retirement receipt),
    control callbacks, worker/launch barrier and (for an unparented direct run) exact process-registry
    evidence all settle.  Missing evidence is unknown and remains retained.
"""
from __future__ import annotations

import asyncio
import hashlib
import sqlite3
import time
from dataclasses import dataclass, field
from contextlib import suppress
from typing import Any, Optional

from gateway.runtime_custody import CustodyHandle, CustodyRefusal
from gateway.platforms.api_server_run_peer import (
    peer_binding_identity as _peer_binding_identity,
    peer_binding_source as _peer_binding_source,
)


class ApiRunCustodyError(RuntimeError):
    """Typed fail-closed refusal for an API run without an authenticated owner."""

    def __init__(self, refusal: CustodyRefusal):
        self.refusal = refusal
        super().__init__(refusal.code)


def _process_receipt_matches(custody: ApiRunCustody, receipt: Any) -> bool:
    required = {
        "kind", "process_instance", "authority_epoch", "close_generation", "owner_id",
        "task_id", "observed", "cross_process_proof",
    }
    if not isinstance(receipt, dict) or set(receipt) != required:
        return False
    if (
        receipt["process_instance"] != custody.process_instance
        or receipt["authority_epoch"] != custody.authority_epoch
        or receipt["close_generation"] != custody.handle.close_generation
        or receipt["observed"] is not True
        or type(receipt["cross_process_proof"]) is not bool
    ):
        return False
    if receipt["kind"] == "parent_handle":
        parent, refusal = custody.bound_parent_handle()
        if refusal is not None or parent is None:
            return False
        exact = (
            receipt["owner_id"] == parent.owner_id
            and receipt["task_id"] is None
            and receipt["cross_process_proof"] is False
        )
        if exact:
            return True
        # The pre-R6 private recovery shape carried the child owner id.  It is retained only as
        # an internal compatibility reader; the source accessor below emits the exact parent id
        # and every production recovery request is identity-only.
        return (
            receipt["owner_id"] == custody.handle.owner_id
            and receipt["task_id"] is None
            and receipt["cross_process_proof"] is False
        )
    if receipt["kind"] == "I15_process_registry_terminal":
        owner_task_id = custody._process_owner_task_id()
        return (
            isinstance(receipt["task_id"], str)
            and bool(receipt["task_id"])
            and isinstance(owner_task_id, str)
            and bool(owner_task_id)
            and receipt["owner_id"] == f"{custody.process_instance}:I15_process_registry_terminal"
            and receipt["task_id"] == owner_task_id
            and receipt["cross_process_proof"] is True
        )
    if receipt["kind"] == "I03_direct_executor":
        direct = custody._direct_executor_receipt()
        return direct is not None and receipt == direct
    return False


def _authoritative_admission(authority: Any, admission_id: str):
    """Read the canonical row before binding an I03 child to an admission parent."""
    db = getattr(authority, "db", None)
    if db is None:
        return None, CustodyRefusal("peer_durable_row_missing", "I03_api_run_direct")
    try:
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(db, admission_id=admission_id)
    except Exception:  # health: allow BLE001 -- unreadable authority state stays retained
        return None, CustodyRefusal("admission_durable_row_unavailable", "I03_api_run_direct")
    if not isinstance(row, dict):
        return None, CustodyRefusal("admission_durable_row_missing", "I03_api_run_direct")
    return row, None


def _preclaim_cancel_matches(custody: Any, row: Any) -> bool:
    """Validate the canonical no-generation cancellation boundary for an API observer."""
    authority = getattr(custody, "admission_authority", None)
    return (
        isinstance(row, dict)
        and row.get("status") == "terminal"
        and row.get("outcome") == "cancelled"
        and row.get("generation") is None
        and row.get("principal_id") == "api"
        and row.get("target_session_id") == getattr(custody, "admission_session_id", None)
        and getattr(custody, "producer_phase", None) == "api_i02"
        and getattr(custody, "producer_principal", None) == "api"
        and getattr(custody, "execution_generation", None) is None
        and row.get("owner_epoch") == getattr(custody, "authority_epoch", None)
        and getattr(authority, "epoch", None) == getattr(custody, "authority_epoch", None)
    )


def _session_status_receipt(authority: Any, custody: Any, *, expected_session_id: Optional[str] = None):
    """Read one terminal admission and its stored result from the owning SessionDB.

    The local run-status map is a poll view only. A legacy API turn may seal its custody handle
    only after this exact admission row and result blob have both been read back.
    """
    admission_id = getattr(custody, "admission_id", None)
    db = getattr(authority, "db", None)
    if db is None or not isinstance(admission_id, str) or not admission_id:
        return None
    try:
        from hermes_state_runtime import get_session_admission
        from gateway.session_results import admission_result
        canonical = get_session_admission(db, admission_id=admission_id)
        result = admission_result(db, admission_id)
    except (AttributeError, ImportError, KeyError, LookupError, OSError, RuntimeError,
            TypeError, ValueError, sqlite3.Error):
        return None
    if not isinstance(canonical, dict) or canonical.get("status") != "terminal":
        return None
    if canonical.get("admission_id") != admission_id:
        return None
    if (expected_session_id is not None
            and canonical.get("target_session_id") != expected_session_id):
        return None
    if canonical.get("target_session_id") != getattr(custody, "admission_session_id", None):
        return None
    # A terminal replay is a read-only receipt from an earlier owner epoch.  The exact durable
    # admission id, principal, target and stored result remain the identity proof; requiring the
    # successor epoch here would turn a lost response into a false ownership refusal.  Live I02
    # completion still requires its current epoch.
    if (canonical.get("owner_epoch") != custody.authority_epoch
            and getattr(custody, "producer_phase", None) != "api_terminal_replay"):
        return None
    preclaim_cancel = _preclaim_cancel_matches(custody, canonical)
    generation = canonical.get("generation")
    if (not preclaim_cancel
            and (type(generation) is not int or generation < 0
                 or generation != custody.execution_generation)):
        return None
    principal = canonical.get("principal_id")
    if (not isinstance(principal, str) or not principal
            or principal != custody.producer_principal):
        return None
    if not isinstance(result, dict) and not preclaim_cancel:
        return None
    return {
        "committed": True,
        "storage": "session_admission",
        "rowcount": 1,
        "run_id": custody.run_id,
        "admission_id": admission_id,
        "generation": generation,
        "principal_id": principal,
        "target_session_id": canonical["target_session_id"],
        "status": canonical,
        "readback": True,
        "terminal_outcome": canonical.get("outcome"),
        "terminal_result": result.get("result") if isinstance(result, dict) else None,
        "terminal_usage": result.get("usage", {}) if isinstance(result, dict) else {},
        "result_readback": True,
        "preclaim_cancel": preclaim_cancel,
    }


def _status_receipt_from_source(adapter: Any, custody: ApiRunCustody):
    """Build a status receipt from the adapter's actual store/projection, never request JSON."""
    if custody.reservation_key is not None:
        store = getattr(adapter, "_run_idempotency_store", None)
        status_for_run = getattr(store, "status_for_run", None)
        if not callable(status_for_run):
            return None
        try:
            record = status_for_run(custody.reservation_scope, custody.run_id)
        except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError,
                ValueError, sqlite3.Error):
            return None
        if not isinstance(record, dict):
            return None
        if (record.get("scope") != custody.reservation_scope
                or record.get("idempotency_key") != custody.reservation_key
                or record.get("fingerprint") != custody.reservation_fingerprint
                or record.get("owner_pid") != custody.reservation_owner_pid
                or record.get("owner_started") != custody.reservation_owner_started):
            return None
        status = record.get("status")
        if not isinstance(status, dict) or status.get("status") not in _TERMINAL_RUN_STATUSES:
            return None
        return {
            "committed": True, "storage": "run_idempotency", "rowcount": 1,
            "run_id": custody.run_id, "scope": record["scope"],
            "idempotency_key": record["idempotency_key"],
            "fingerprint": record["fingerprint"],
            "owner_pid": record["owner_pid"], "owner_started": record["owner_started"],
            "readback": True,
        }
    # An unkeyed run is accepted only when its original canonical SessionDB admission/result is
    # still available. A local status projection is deliberately not a recovery proof.
    authority = getattr(custody, "admission_authority", None)
    if authority is not None:
        return _session_status_receipt(
            authority, custody, expected_session_id=getattr(custody, "admission_session_id", None))
    from gateway.platforms.api_server_run_witness import _canonical_status
    launch = (getattr(adapter, "_run_launches", {}) or {}).get(custody.run_id)
    return _canonical_status(adapter, custody, launch) if launch is not None else None


_TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled", "interrupted"})


async def await_original_work(custody: Any) -> None:
    """Fence a legacy route's source-owned Task/Future before its wrapper finalizes."""
    work = getattr(custody, "worker_task", None) or getattr(custody, "worker_future", None)
    if work is None or not callable(getattr(work, "done", None)) or work.done():
        return
    with suppress(asyncio.CancelledError, Exception):
        await asyncio.shield(work)


_LEGACY_AUTHORITY_KEYS = frozenset({
    "user_message", "conversation_history", "ephemeral_system_prompt", "session_id",
    "stream_delta_callback", "tool_progress_callback", "tool_start_callback",
    "tool_complete_callback", "interim_assistant_callback", "reasoning_callback",
    "status_callback", "walked_from", "agent_ref", "active_run_id", "gateway_session_key",
    "requested_model", "requested_provider", "model_options", "route", "session_model",
    "requested_runtime", "route_source", "confirmed_runtime_lock", "bind_declared_conversation",
    "request_id", "run_owner_scope", "history_from_session", "session_history_delivery",
    "turn_author", "resume_unanswered_turn", "relay_metadata", "approval_notify_callback",
    "approval_session_key", "notification_category",
})


def legacy_authority_kwargs(values: dict[str, Any]) -> dict[str, Any]:
    """Select the exact run_api_turn call shape from the legacy adapter's locals."""
    return {key: values[key] for key in _LEGACY_AUTHORITY_KEYS if key in values}


def complete_legacy_source(custody: Any, *, active_run_id: Optional[str]):
    """Consume the source receipt, then settle the typed I02 parent after the I03 child closes."""
    completion = None
    if getattr(custody, "producer_phase", None) == "api_i02":
        from gateway.platforms.api_server_run_completion import completion_for_custody
        completion = completion_for_custody(custody)
        if completion is None:
            stale = getattr(custody, "finalizer_completion", None)
            if callable(getattr(stale, "fail", None)):
                stale.fail("finalizer_completion_registry_missing")
            refusal = CustodyRefusal(
                "finalizer_completion_registry_missing", "I02_api_session_turn")
            custody.mark_unknown(refusal.code)
            return refusal
    source_receipt = custody.authoritative_status_receipt()
    # A session stream has both an outer keyed run-status row and the canonical I02 admission
    # result.  The outer row is deliberately still ``running`` until the stream emits its
    # terminal frame, while the authority waiter can only return after the I02 result is
    # terminal/readable.  Consume that exact admission readback here; treating the outer
    # projection as the source would reject a successful canonical turn as ``status_commit_unproven``.
    if (getattr(custody, "producer_phase", None) == "api_i02"
            and (not isinstance(source_receipt, dict)
                 or source_receipt.get("result_readback") is not True)):
        source_receipt = _session_status_receipt(
            getattr(custody, "admission_authority", None), custody,
            expected_session_id=getattr(custody, "admission_session_id", None),
        )
    if source_receipt is None:
        if completion is not None:
            completion.fail("status_commit_unproven")
        return custody.mark_unknown("status_commit_unproven")
    preclaim_cancel = source_receipt.get("preclaim_cancel") is True
    if completion is not None and preclaim_cancel and not active_run_id:
        from gateway.session_ingress import DeliveryReceipt
        if not completion.stage_delivery(
                delivery_receipt=DeliveryReceipt(
                    custody.admission_id, attempted=False, delivered=False,
                    reason="no_delivery", effect_state="none",
                ),
                waiter_delivered=True, finalizer_ok=True):
            reason = "finalizer_completion_cancelled_unknown"
            completion.fail(reason)
            custody.mark_unknown(reason)
            return CustodyRefusal(reason, "I02_api_session_turn")
    if active_run_id:
        refusal = custody.mark_status_receipt(source_receipt)
        if refusal is not None:
            custody.mark_unknown(getattr(refusal, "code", "status_commit_unproven"))
        return refusal
    refusal = custody.complete_from_source(source_receipt)
    if refusal is not None:
        reason = getattr(refusal, "code", "source_completion_unknown")
        if completion is not None:
            completion.fail(reason)
        custody.mark_unknown(reason)
        return refusal
    if completion is None:
        retire_legacy_source(custody)
        return None
    receipt = completion.settle_after_child()
    if receipt is None and getattr(completion, '_pending_parent_release', False):
        return None
    if getattr(receipt, "state", None) != "complete":
        reason = getattr(receipt, "reason", None) or "finalizer_completion_unknown"
        custody.mark_completion_unknown(reason)
        return CustodyRefusal(reason, "I02_api_session_turn")
    # The completion registry and launch index are retired by the owning task after its final
    # receipt is complete; the drain itself never drops either source-owned reference early.
    return None


def retire_legacy_source(custody: Any) -> bool:
    """Retire a settled legacy launch after its canonical source receipt was consumed.

    Unknown or still-live custody remains indexed for reconciliation.  A released legacy turn
    has no active transport controls left, so retaining its launch map entry would turn a closed
    source into a permanent owner set member without adding recovery evidence.
    """
    if not getattr(custody, "released", False) or getattr(custody, "unknown_reason", None):
        return False
    launch = getattr(custody, "source_launch", None)
    owner = getattr(launch, "owner", None)
    run_id = getattr(custody, "run_id", None)
    if owner is None or not isinstance(run_id, str) or not run_id:
        return False
    custody_table = getattr(owner, "_run_custody", None)
    launch_table = getattr(owner, "_run_launches", None)
    if isinstance(custody_table, dict) and custody_table.get(run_id) is custody:
        custody_table.pop(run_id, None)
    if isinstance(launch_table, dict) and launch_table.get(run_id) is launch:
        launch_table.pop(run_id, None)
    idempotency_ids = getattr(owner, "_run_idempotency_ids", None)
    if isinstance(idempotency_ids, set):
        idempotency_ids.discard(run_id)
    if (isinstance(custody_table, dict) and not custody_table
            and isinstance(launch_table, dict) and not launch_table):
        owner._api_run_owner_registered = False
    return True


@dataclass(frozen=True)
class _LegacySourceCompletionPreflight:
    completion: Any
    registry: dict[str, Any]
    admission_id: str
    launch: Any
    owner: Any
    run_id: str
    custody_table: dict[str, Any]
    launch_table: dict[str, Any]
    custody: Any
    retire_completion: Any
    idempotency_ids: Any
    had_idempotency: bool
    owner_registered: Any


def _preflight_legacy_source_completion(
    custody: Any, completion: Any,
) -> Optional[_LegacySourceCompletionPreflight]:
    receipt = getattr(completion, "receipt", None)
    if (not getattr(custody, "released", False)
            or getattr(custody, "unknown_reason", None)
            or getattr(receipt, "state", None) != "complete"):
        return None
    if getattr(completion, "custody", None) is not custody:
        return None
    authority = getattr(custody, "admission_authority", None)
    registry = getattr(authority, "_api_finalizer_completions", None)
    admission_id = getattr(custody, "admission_id", None)
    if (not isinstance(registry, dict)
            or not isinstance(admission_id, str)
            or registry.get(admission_id) is not completion):
        return None
    launch = getattr(custody, "source_launch", None)
    owner = getattr(launch, "owner", None)
    run_id = getattr(custody, "run_id", None)
    custody_table = getattr(owner, "_run_custody", None)
    launch_table = getattr(owner, "_run_launches", None)
    if (owner is None or not isinstance(run_id, str) or not run_id
            or not isinstance(custody_table, dict)
            or custody_table.get(run_id) is not custody
            or not isinstance(launch_table, dict)
            or launch_table.get(run_id) is not launch):
        return None
    retire_completion = getattr(completion, "retire", None)
    if not callable(retire_completion):
        return None
    idempotency_ids = getattr(owner, "_run_idempotency_ids", None)
    return _LegacySourceCompletionPreflight(
        completion=completion, registry=registry, admission_id=admission_id,
        launch=launch, owner=owner, run_id=run_id, custody_table=custody_table,
        launch_table=launch_table, custody=custody, retire_completion=retire_completion,
        idempotency_ids=idempotency_ids,
        had_idempotency=isinstance(idempotency_ids, set) and run_id in idempotency_ids,
        owner_registered=getattr(owner, "_api_run_owner_registered", None),
    )


def _restore_legacy_source_completion(
    preflight: _LegacySourceCompletionPreflight,
) -> bool:
    restored = True
    for table, key, value in (
        (preflight.registry, preflight.admission_id, preflight.completion),
        (preflight.custody_table, preflight.run_id, preflight.custody),
        (preflight.launch_table, preflight.run_id, preflight.launch),
    ):
        if key not in table:
            table[key] = value
        elif table.get(key) is not value:
            restored = False
    if (preflight.had_idempotency and isinstance(preflight.idempotency_ids, set)
            and preflight.run_id not in preflight.idempotency_ids):
        preflight.idempotency_ids.add(preflight.run_id)
    if (preflight.owner_registered is True
            and getattr(preflight.owner, "_api_run_owner_registered", None) is False):
        preflight.owner._api_run_owner_registered = True
    return restored


def _mark_source_completion_retirement_failure(custody: Any, rollback_ok: bool) -> None:
    marker = getattr(custody, "mark_completion_unknown", None)
    if callable(marker):
        marker(
            "stream_completion_retire_rollback_failed"
            if not rollback_ok else "stream_completion_retire_failed"
        )


def retire_legacy_source_with_completion(custody: Any) -> bool:
    """Retire a stream only after exact source and completion evidence agree.

    Source maps and the typed completion registry are source-owned references.  The synchronous
    close boundary retires source first, then completion, and restores only missing exact entries.
    """
    completion = getattr(custody, "finalizer_completion", None)
    if completion is None:
        return retire_legacy_source(custody)
    preflight = _preflight_legacy_source_completion(custody, completion)
    if preflight is None:
        return False
    try:
        if not retire_legacy_source(custody):
            _restore_legacy_source_completion(preflight)
            return False
        if not preflight.retire_completion():
            rollback_ok = _restore_legacy_source_completion(preflight)
            if not rollback_ok:
                _mark_source_completion_retirement_failure(custody, False)
            return False
    except BaseException:  # health: allow BLE001 -- rollback must not replace first close error
        try:
            rollback_ok = _restore_legacy_source_completion(preflight)
        except BaseException:  # health: allow BLE001 -- retain first close error
            rollback_ok = False
        try:
            _mark_source_completion_retirement_failure(custody, rollback_ok)
        except BaseException:  # health: allow BLE001 -- marker must not replace first close error
            rollback_ok = False
        raise
    return True


def _legacy_store_scope(adapter: Any, profile_id: Optional[str]) -> str:
    """Return the same opaque profile/auth namespace used by the public run store."""
    profile = profile_id or "default"
    expected_key = ""
    resolver = getattr(adapter, "_expected_api_key", None)
    if callable(resolver):
        expected_key = resolver() or ""
    return hashlib.sha256(
        "\0".join((str(profile), expected_key or "unauthenticated-test-listener")).encode()
    ).hexdigest()


def prepare_legacy_store(adapter: Any, custody: Any, *, profile_id: Optional[str]):
    """Bind a no-authority legacy turn to the existing durable status writer.

    The pre-authority API path is still a supported in-process route.  Its agent has no I02
    admission row to read back, so it gets a private, server-minted reservation in the same
    authenticated ``RunIdempotencyStore`` used by ``/v1/runs``.  The key is never client supplied;
    the row is only a terminal status witness for this exact custody handle.
    """
    if custody.reservation_key is not None:
        return None
    from gateway.runtime_custody import CustodyRefusal

    store = getattr(adapter, "_run_idempotency_store", None)
    if store is None or getattr(store, "durable", False) is not True:
        return CustodyRefusal("legacy_status_store_unavailable", "I03_api_run_direct")
    scope = _legacy_store_scope(adapter, profile_id)
    key = f"legacy:{custody.run_id}"
    fingerprint = hashlib.sha256(
        "\0".join((key, custody.process_instance, str(custody.authority_epoch),
                   str(custody.handle.close_generation))).encode()
    ).hexdigest()
    initial = {
        "object": "hermes.run", "run_id": custody.run_id, "status": "queued",
        "created_at": time.time(),
    }
    try:
        outcome, _record = store.reserve(
            scope, key, fingerprint, custody.run_id, initial,
            owner_pid=getattr(adapter, "_run_owner_pid", 0),
            owner_started=getattr(adapter, "_run_owner_started", 0),
        )
    except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return CustodyRefusal("legacy_status_reservation_unavailable", "I03_api_run_direct")
    if outcome != "created":
        return CustodyRefusal("legacy_status_reservation_conflict", "I03_api_run_direct")
    refusal = custody.bind_idempotency_identity(
        scope, key, fingerprint,
        getattr(adapter, "_run_owner_pid", 0), getattr(adapter, "_run_owner_started", 0),
    )
    if refusal is not None:
        with suppress(Exception):
            store.forget(scope, key)
        return refusal
    idempotency_ids = getattr(adapter, "_run_idempotency_ids", None)
    if not isinstance(idempotency_ids, set):
        with suppress(Exception):
            store.forget(scope, key)
        return CustodyRefusal("legacy_status_reservation_unavailable", "I03_api_run_direct")
    idempotency_ids.add(custody.run_id)
    return None


def record_legacy_status(adapter: Any, custody: Any, status: str, **fields: Any):
    """Commit and read back one no-authority legacy status before it can be consumed."""
    from gateway.runtime_custody import CustodyRefusal

    _current, receipt = adapter._set_run_status(
        custody.run_id, status, _return_receipt=True, **fields)
    refusal = custody.mark_status_receipt(receipt, require_store=True)
    if refusal is not None:
        return refusal
    custody.status_receipt = receipt
    launch = getattr(custody, "source_launch", None)
    if launch is not None:
        launch.status_receipt = receipt
    return None


def record_legacy_failure(
    adapter: Any, custody: Any, status: str, *, active_run_id: Optional[str], **fields: Any,
) -> Any:
    """Persist an exception only after preserving a completed direct worker outcome."""
    if custody.reservation_key is None:
        return
    work = getattr(custody, "worker_task", None) or getattr(custody, "worker_future", None)
    if callable(getattr(work, "done", None)) and work.done() and not work.cancelled():
        exception_fn = getattr(work, "exception", None)
        worker_exception = (
            exception_fn() if callable(exception_fn) else RuntimeError("worker exception unavailable"))
        if worker_exception is None:
            outcome = work.result()
            if isinstance(outcome, tuple) and len(outcome) == 2:
                try:
                    from gateway.platforms.api_server_run_execution import record_legacy_direct_result
                    record_legacy_direct_result(
                        adapter, custody, outcome[0], outcome[1], active_run_id=active_run_id)
                except ApiRunCustodyError as exc:
                    custody.mark_unknown(getattr(exc.refusal, "code", "status_commit_unproven"))
                    return exc.refusal
                except RuntimeError:
                    custody.mark_unknown("status_commit_unproven")
                    return CustodyRefusal("status_commit_unproven", "I03_api_run_direct")
                return None
    refusal = record_legacy_status(adapter, custody, status, **fields)
    if refusal is not None:
        custody.mark_unknown(getattr(refusal, "code", "status_commit_unproven"))
    return refusal


def _legacy_reservation_readback(custody: Any):
    """Read the exact durable reservation before closing a direct no-worker boundary."""
    if custody.reservation_key is None:
        return None, CustodyRefusal("legacy_status_reservation_missing", "I03_api_run_direct")
    owner = getattr(getattr(custody, "source_launch", None), "owner", None)
    store = getattr(owner, "_run_idempotency_store", None)
    reader = getattr(store, "status_for_run", None)
    if not callable(reader):
        return None, CustodyRefusal("legacy_status_store_unavailable", "I03_api_run_direct")
    try:
        record = reader(custody.reservation_scope, custody.run_id)
    except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return None, CustodyRefusal("status_commit_unproven", "I03_api_run_direct")
    if not isinstance(record, dict):
        return None, CustodyRefusal("status_commit_unproven", "I03_api_run_direct")
    expected = (
        ("scope", custody.reservation_scope),
        ("idempotency_key", custody.reservation_key),
        ("fingerprint", custody.reservation_fingerprint),
        ("owner_pid", custody.reservation_owner_pid),
        ("owner_started", custody.reservation_owner_started),
    )
    if any(record.get(key) != value for key, value in expected):
        return None, CustodyRefusal("status_identity_mismatch", "I03_api_run_direct")
    status = record.get("status")
    if not isinstance(status, dict) or status.get("status") not in {"queued", "running"}:
        return None, CustodyRefusal("status_commit_unproven", "I03_api_run_direct")
    return record, None


def _legacy_source_is_unindexed(custody: Any) -> bool:
    owner = getattr(getattr(custody, "source_launch", None), "owner", None)
    custody_table = getattr(owner, "_run_custody", {})
    launch_table = getattr(owner, "_run_launches", {})
    return (
        not isinstance(custody_table, dict)
        or custody_table.get(custody.run_id) is not custody
        or not isinstance(launch_table, dict)
        or launch_table.get(custody.run_id) is not custody.source_launch
    )


async def finalize_legacy_cancelled(custody: Any, *, finalize_source: bool = False) -> None:
    """Retain cancellation without waiting on the source worker in the handler task.

    The handler can be cancelled by a disconnect or shutdown while its executor/authority
    task still owns the turn.  Waiting here would keep the cancellation handler suspended
    until that task exits and would hide the worker from the shutdown drain.  The retained
    worker reference remains the reconciliation boundary; terminalization still requires
    its own completion and durable receipts.
    """
    if not custody.worker_done:
        custody.mark_unknown("legacy_turn_cancelled")
    if finalize_source:
        custody.mark_transport_retired()
        custody.mark_control_finalizers()


async def finalize_legacy_failed(custody: Any, *, finalize_source: bool = False) -> None:
    """Keep an exception unknown until the original worker/admission boundary is settled."""
    await await_original_work(custody)
    if not custody.worker_submitted and not custody.worker_done:
        if custody.reservation_key is None and _legacy_source_is_unindexed(custody):
            return
        if custody.reservation_key is not None:
            _record, reservation_refusal = _legacy_reservation_readback(custody)
            if reservation_refusal is not None:
                custody.mark_unknown(getattr(reservation_refusal, "code", "status_commit_unproven"))
            else:
                custody.request_no_worker()
        else:
            custody.request_no_worker()
        if custody.admission_pending:
            custody.mark_unknown("admission_interrupted")
        elif not custody.unknown_reason:
            custody.mark_unknown("legacy_turn_failed")
    elif not custody.worker_done:
        custody.mark_unknown("worker_unsettled")
    else:
        custody.mark_unknown("legacy_turn_failed")
    if finalize_source:
        custody.mark_transport_retired()
        custody.mark_control_finalizers()


async def run_legacy_authority_turn(
    adapter: Any, custody: Any, *, active_run_id: Optional[str], run_kwargs: dict[str, Any],
):
    """Run one legacy authority turn while retaining its exact Task and canonical result."""
    from gateway.session_api_turn import _settled_api_result, run_api_turn

    run_kwargs = legacy_authority_kwargs(run_kwargs)
    run_kwargs["active_run_id"] = active_run_id
    authority_task = asyncio.create_task(run_api_turn(adapter, custody=custody, **run_kwargs))
    custody.worker_task = authority_task
    custody.execution_task = authority_task
    if custody.source_launch is not None:
        custody.source_launch.execution_task = authority_task
        custody.source_launch.worker_future = authority_task
    try:
        result = await asyncio.shield(authority_task)
        if custody.producer_phase == "api_terminal_replay":
            custody.request_no_worker()
            source_receipt = custody.authoritative_status_receipt()
            if source_receipt is None:
                custody.mark_unknown("terminal_replay_unknown")
                raise RuntimeError("terminal API admission replay could not be read back")
            refusal = complete_legacy_source(custody, active_run_id=active_run_id)
            if refusal is not None:
                custody.mark_unknown("terminal_replay_unknown")
                raise RuntimeError("terminal API admission replay could not be sealed")
            return result
        custody.mark_worker_done()
        generation_refusal = custody.refresh_admission_generation()
        if generation_refusal is not None:
            custody.mark_unknown(getattr(generation_refusal, "code", "admission_generation_missing"))
        refusal = complete_legacy_source(custody, active_run_id=active_run_id)
        if refusal is not None:
            from gateway.platforms.api_server_run_completion import failed_api_response_or_raise
            return failed_api_response_or_raise(custody, refusal, active_run_id=active_run_id)
        completion = getattr(custody, "finalizer_completion", None)
        if completion is not None and not active_run_id:
            completion_receipt = await completion.wait()
            if getattr(completion_receipt, "state", None) != "complete":
                reason = getattr(completion_receipt, "reason", None) or "finalizer_completion_unknown"
                custody.mark_completion_unknown(reason)
                raise ApiRunCustodyError(
                    CustodyRefusal("finalizer_completion_unknown", "I02_api_session_turn")
                )
            if not retire_legacy_source(custody):
                custody.mark_completion_unknown("finalizer_completion_retire_failed")
                raise ApiRunCustodyError(
                    CustodyRefusal("finalizer_completion_retire_failed", "I02_api_session_turn")
                )
            if not completion.retire():
                custody.mark_completion_unknown("finalizer_completion_registry_missing")
                raise ApiRunCustodyError(
                    CustodyRefusal("finalizer_completion_registry_missing", "I02_api_session_turn")
                )
        return result
    except asyncio.CancelledError:
        completion = getattr(custody, "finalizer_completion", None)
        if completion is not None:
            completion.fail("api_turn_cancelled")
        await finalize_legacy_cancelled(custody, finalize_source=active_run_id is None)
        raise
    except BaseException:
        if custody.producer_phase == "api_terminal_replay":
            await await_original_work(custody)
            custody.request_no_worker()
            source_receipt = custody.authoritative_status_receipt()
            replay_result = None
            if source_receipt is not None:
                try:
                    replay_result = _settled_api_result(
                        custody.admission_authority, custody.admission_id)
                except (KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
                    replay_result = None
            if replay_result is not None:
                refusal = complete_legacy_source(custody, active_run_id=active_run_id)
                if refusal is not None:
                    custody.mark_unknown("terminal_replay_unknown")
                    raise RuntimeError("terminal API admission replay could not be sealed")
                return replay_result
            custody.mark_unknown("terminal_replay_unknown")
            raise
        completion = getattr(custody, "finalizer_completion", None)
        if completion is not None:
            completion.fail("api_turn_failed")
        await finalize_legacy_failed(custody, finalize_source=active_run_id is None)
        raise


def preservation_owner_evidence(adapters: Any, runner: Any, *, process_instance: str,
                                 authority_epoch: int, close_generation: int, owner_id: str):
    """Return I03 quiescence only from each adapter's retained source-owned run witnesses."""
    if not isinstance(adapters, (list, tuple)) or not adapters:
        return None
    descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
    if (descriptor.get("instance_id") != process_instance
            or descriptor.get("authority_epoch") != authority_epoch):
        return None
    from pathlib import Path
    from gateway.runtime_ownership import process_ownership
    from gateway.session_authorities import all_authorities
    from gateway.platforms.api_server_run_witness import owner_evidence
    authorities = all_authorities(runner)
    homes = [getattr(authority, "profile_id", None) for authority in authorities]
    if (not authorities or any(not isinstance(home, str) or not home for home in homes)
            or any(not process_ownership.owns(Path(home)) for home in homes)):
        return None
    adapter_evidence = []
    for adapter in adapters:
        evidence = owner_evidence(
            adapter, process_instance=process_instance, authority_epoch=authority_epoch,
            close_generation=close_generation, owner_id=owner_id)
        if evidence is None:
            return None
        adapter_evidence.append(evidence)
    witnesses = tuple(
        witness for evidence in adapter_evidence for witness in evidence.get("witnesses", ()))
    process_receipts = tuple(
        receipt for evidence in adapter_evidence for receipt in evidence.get("process_receipts", ()))
    if not witnesses or len(process_receipts) != len(witnesses):
        return None
    return {
        "process_instance": process_instance, "authority_epoch": authority_epoch,
        "close_generation": close_generation, "owner_id": owner_id, "observed": True,
        "cross_process_proof": all(
            isinstance(receipt, dict) and receipt.get("observed") is True
            and receipt.get("kind") in {
                "parent_handle", "I15_process_registry_terminal", "I03_direct_executor"
            }
            and receipt.get("cross_process_proof") is True
            for receipt in process_receipts),
        "profile_ids": tuple(sorted(str(authority.profile_id) for authority in authorities)),
        "registered_adapters": len(adapters),
        "run_witnesses": witnesses,
        "process_receipts": process_receipts,
    }


@dataclass
class ApiRunCustody:
    coordinator: Any
    handle: CustodyHandle
    run_id: str
    profile_id: str
    authority_epoch: int
    process_instance: str
    parent_kind: Optional[str] = None
    parent_handle: Optional[CustodyHandle] = None
    parent_bound: bool = False
    status_persisted: bool = False
    event_enqueued: bool = False
    stream_closed: bool = False
    transport_retired: bool = False
    worker_done: bool = False
    worker_submitted: bool = False
    no_worker_finalized: bool = False
    launch_phase: str = "reserved"
    control_finalizers: bool = False
    process_proof: bool = False
    process_task_id: Optional[str] = None
    process_owner_task_id: Optional[str] = None
    reservation_scope: Optional[str] = None
    reservation_key: Optional[str] = None
    reservation_fingerprint: Optional[str] = None
    reservation_owner_pid: Optional[int] = None
    reservation_owner_started: Optional[int] = None
    admission_id: Optional[str] = None
    initial_admission_id: Optional[str] = None
    execution_generation: Optional[int] = None
    admission_authority: Any = field(default=None, repr=False, compare=False)
    # Exact I02 completion fence; RuntimeCustodyCoordinator remains the only lease ledger.
    finalizer_completion: Any = field(default=None, repr=False, compare=False)
    finalizer_completion_receipt: Any = field(default=None, repr=False, compare=False)
    admission_session_id: Optional[str] = None
    producer_phase: str = "I15_direct"
    producer_principal: Optional[str] = None
    producer_source: Optional[str] = None
    admission_pending: bool = False
    no_worker_requested: bool = False
    unknown_reason: Optional[str] = None
    released: bool = False
    status_receipt: Any = field(default=None, repr=False, compare=False)
    execution_task: Any = field(default=None, repr=False, compare=False)
    worker_task: Any = field(default=None, repr=False, compare=False)
    worker_future: Any = field(default=None, repr=False, compare=False)
    source_launch: Any = field(default=None, repr=False, compare=False)
    process_scope: Any = field(default=None, repr=False, compare=False)
    process_scope_receipt: Any = field(default=None, repr=False, compare=False)
    terminal_release_receipt: Any = field(default=None, repr=False, compare=False)
    _terminal_attempted: bool = field(default=False, init=False)

    @classmethod
    def reserve(cls, adapter: Any, run_id: str, profile_id: Optional[str]):
        runner = getattr(adapter, "gateway_runner", None)
        coordinator = getattr(runner, "runtime_custody", None)
        descriptor = getattr(runner, "session_runtime_descriptor", None)
        if coordinator is None or not isinstance(descriptor, dict):
            return None, CustodyRefusal("runtime_custody_unavailable", "I03_api_run_direct")
        process_instance = descriptor.get("instance_id")
        authority_epoch = descriptor.get("authority_epoch")
        if not isinstance(process_instance, str) or not process_instance:
            return None, CustodyRefusal("process_identity_missing", "I03_api_run_direct")
        if type(authority_epoch) is not int or authority_epoch < 1:
            return None, CustodyRefusal("authority_epoch_missing", "I03_api_run_direct")
        from gateway.session_authorities import active_authority
        authority = active_authority(runner)
        if authority is not None:
            authoritative_profile = getattr(authority, "profile_id", None)
            authoritative_instance = getattr(authority, "instance_id", None)
            authoritative_epoch = getattr(authority, "epoch", None)
            if (not isinstance(authoritative_profile, str) or not authoritative_profile
                    or authoritative_instance != process_instance
                    or authoritative_epoch != authority_epoch):
                return None, CustodyRefusal("runtime_authority_mismatch", "I03_api_run_direct")
            from gateway.session_authorities import served_profile_name
            if (profile_id is not None
                    and profile_id not in (authoritative_profile, served_profile_name(authoritative_profile))):
                return None, CustodyRefusal("profile_identity_mismatch", "I03_api_run_direct")
            profile_id = authoritative_profile
        elif getattr(runner, "session_authorities", None) is not None:
            return None, CustodyRefusal("runtime_authority_missing", "I03_api_run_direct")
        if not isinstance(profile_id, str) or not profile_id:
            profile_id = descriptor.get("profile_id")
        if not isinstance(profile_id, str) or not profile_id:
            served = descriptor.get("served_profiles")
            if isinstance(served, (list, tuple)) and len(served) == 1:
                only_served = served[0]
                if isinstance(only_served, dict):
                    only_served = only_served.get("profile_id")
                if isinstance(only_served, str):
                    profile_id = only_served
        if not isinstance(profile_id, str) or not profile_id:
            return None, CustodyRefusal("profile_identity_missing", "I03_api_run_direct")
        handle = coordinator.reserve_root(
            "I03_api_run_direct", run_id, profile_id=profile_id,
            authority_epoch=authority_epoch)
        if isinstance(handle, CustodyRefusal):
            return None, handle
        # This is the first source-owned producer registration. The sealer still requires a
        # retained run/status/stream record and settled custody, so this marker cannot prove an
        # empty operation map or release an unaccepted reservation.
        adapter._api_run_owner_registered = True
        return cls(
            coordinator, handle, run_id, profile_id, authority_epoch, process_instance), None

    def bind_parent(self, parent: Any, parent_kind: str):
        if not isinstance(parent, CustodyHandle):
            return CustodyRefusal("parent_handle_missing", "I03_api_run_direct")
        if parent.close_generation != self.handle.close_generation:
            return CustodyRefusal("parent_generation_mismatch", "I03_api_run_direct")
        refusal = self.coordinator.bind_parent(self.handle, parent)
        if refusal is None:
            self.parent_kind = parent_kind
            self.parent_handle = parent
            self.parent_bound = True
        return refusal

    def bound_parent_handle(self):
        """Return the exact accepted parent after checking the coordinator's live edge."""
        parent = self.parent_handle
        if not self.parent_bound or not isinstance(parent, CustodyHandle):
            return None, CustodyRefusal("parent_handle_missing", "I03_api_run_direct")
        if (
            parent.coordinator_id != self.handle.coordinator_id
            or parent.profile_id != self.profile_id
            or parent.authority_epoch != self.authority_epoch
            or parent.process_instance != self.process_instance
            or parent.close_generation != self.handle.close_generation
        ):
            return None, CustodyRefusal("parent_identity_mismatch", "I03_api_run_direct")
        if self.coordinator.bound_parent(self.handle) != parent.lease.lease_id:
            return None, CustodyRefusal("parent_owner_unmapped", "I03_api_run_direct")
        return parent, None

    @staticmethod
    def _parent_from_authority(authority: Any, admission_id: Any):
        if not isinstance(admission_id, str) or not admission_id:
            return None, None
        handles = getattr(authority, "_preservation_handles", {})
        parent = handles.get(admission_id) if isinstance(handles, dict) else None
        if isinstance(parent, CustodyHandle):
            return parent, "I02_api_session_turn"
        children = getattr(authority, "_preservation_children", {})
        parent = children.get(admission_id) if isinstance(children, dict) else None
        if isinstance(parent, CustodyHandle):
            return parent, "I04_fifo_turn_execution"
        return None, None

    def bind_admission(self, admission: Any):
        if not isinstance(admission, tuple) or len(admission) != 3:
            return CustodyRefusal("admission_identity_missing", "I03_api_run_direct")
        authority, session_ref, supplied_row = admission
        if not isinstance(supplied_row, dict):
            return CustodyRefusal("admission_row_missing", "I03_api_run_direct")
        if (getattr(authority, "profile_id", None) != self.profile_id
                or getattr(authority, "epoch", None) != self.authority_epoch):
            return CustodyRefusal("admission_authority_mismatch", "I03_api_run_direct")
        admission_id = supplied_row.get("admission_id")
        if not isinstance(admission_id, str) or not admission_id:
            return CustodyRefusal("admission_identity_missing", "I03_api_run_direct")
        row, refusal = _authoritative_admission(authority, admission_id)
        if refusal is not None:
            return refusal
        identity_keys = (
            "admission_id", "target_session_id", "generation", "owner_epoch", "principal_id")
        if any(row.get(key) != supplied_row.get(key) for key in identity_keys):
            return CustodyRefusal("admission_identity_mismatch", "I03_api_run_direct")
        target_session_id = row.get("target_session_id")
        if (not isinstance(target_session_id, str) or not target_session_id
                or getattr(session_ref, "profile_id", None) != self.profile_id
                or getattr(session_ref, "session_id", None) != target_session_id
                or row.get("principal_id") != "api"):
            return CustodyRefusal("admission_identity_mismatch", "I03_api_run_direct")
        session = getattr(getattr(authority, "db", None), "get_session", lambda _id: None)(target_session_id)
        source = session.get("source") if isinstance(session, dict) else None
        if (supplied_row.get("profile_id", self.profile_id) != self.profile_id
                or supplied_row.get("authority_epoch", self.authority_epoch) != self.authority_epoch
                or (row.get("status") != "terminal"
                    and row.get("owner_epoch") != self.authority_epoch)):
            return CustodyRefusal("admission_identity_mismatch", "I03_api_run_direct")
        generation = row.get("generation")
        if generation is not None and (type(generation) is not int or generation < 0):
            return CustodyRefusal("admission_generation_missing", "I03_api_run_direct")
        if generation is None and row.get("status") != "queued":
            return CustodyRefusal("admission_generation_missing", "I03_api_run_direct")
        if row.get("status") == "terminal":
            # A terminal API admission is a durable read-only replay.  Its session may have
            # been deleted after the result committed, so the live session source is no longer
            # available; the exact row/principal/result readback below remains the ownership
            # proof and this path never creates a new worker or parent edge.
            self.admission_id = admission_id
            self.execution_generation = generation
            self.admission_authority = authority
            self.admission_session_id = target_session_id
            self.producer_phase = "api_terminal_replay"
            self.producer_principal = "api"
            self.producer_source = source if source in {"api_server", "bot_room"} else None
            self.admission_pending = False
            # A terminal row is an exact canonical read-only replay. Do not attach this new
            # I03 reservation to the retired admission's old parent edge.
            return CustodyRefusal("terminal_replay_readonly", "I03_api_run_direct")
        if source not in {"api_server", "bot_room"}:
            return CustodyRefusal("admission_source_mismatch", "I03_api_run_direct")
        parent, parent_kind = self._parent_from_authority(authority, admission_id)
        if parent is None:
            return CustodyRefusal("parent_owner_unmapped", "I03_api_run_direct")
        self.admission_id = admission_id
        self.execution_generation = generation
        self.admission_authority = authority
        self.admission_session_id = target_session_id
        self.producer_phase = "api_i02"
        self.producer_principal = "api"
        self.producer_source = source
        if self.source_launch is not None:
            self.source_launch.admission = admission
            self.source_launch.session_id = target_session_id
        self.admission_pending = False
        refusal = self.bind_parent(parent, parent_kind)
        if refusal is not None:
            return refusal
        # The caller may have proven before admission completed that no nested worker will run.
        # Refuse at that pre-acceptance boundary before allocating an I02 finalizer completion;
        # a missing event loop must not replace the original late-submission reason.
        if self.no_worker_requested or self.no_worker_finalized:
            self.mark_unknown("late_worker_submission")
            return CustodyRefusal("late_worker_submission", "I03_api_run_direct")
        if parent_kind == "I02_api_session_turn":
            from gateway.platforms.api_server_run_completion import begin_api_finalizer_completion
            completion, completion_refusal = begin_api_finalizer_completion(
                authority, self, parent, admission_id,
            )
            if completion_refusal is not None:
                self.mark_unknown(getattr(completion_refusal, "code", "finalizer_completion_unknown"))
                return completion_refusal
            self.finalizer_completion = completion
        return None

    def refresh_admission_generation(self):
        """Read the claimed generation before consuming a terminal admission result."""
        authority = self.admission_authority
        admission_id = self.admission_id
        if authority is None or not isinstance(admission_id, str) or not admission_id:
            return CustodyRefusal("admission_generation_missing", "I03_api_run_direct")
        row, refusal = _authoritative_admission(authority, admission_id)
        if refusal is not None:
            return refusal
        if _preclaim_cancel_matches(self, row):
            return None
        if (
            row.get("target_session_id") != self.admission_session_id
            or row.get("owner_epoch") != self.authority_epoch
            or row.get("principal_id") != self.producer_principal
        ):
            return CustodyRefusal("admission_identity_mismatch", "I03_api_run_direct")
        generation = row.get("generation")
        if type(generation) is not int or generation < 0:
            return CustodyRefusal("admission_generation_missing", "I03_api_run_direct")
        if self.execution_generation is not None and self.execution_generation != generation:
            return CustodyRefusal("admission_generation_mismatch", "I03_api_run_direct")
        self.execution_generation = generation
        return None

    def bind_peer_record(self, record: Any, authority: Any):
        identity, refusal = _peer_binding_identity(self, record, authority)
        if refusal is not None:
            return refusal
        admission_id, binding_id, generation = identity
        source_data, refusal = _peer_binding_source(self, record, authority, binding_id, generation)
        if refusal is not None:
            return refusal
        row, source, parent, parent_kind = source_data
        self.admission_id = binding_id
        if self.initial_admission_id is None:
            self.initial_admission_id = admission_id
        self.execution_generation = generation
        self.admission_authority = authority
        self.admission_session_id = record.get("session_id")
        self.producer_phase = "bot_i02"
        self.producer_principal = row["principal_id"]
        self.producer_source = source
        if self.source_launch is not None:
            self.source_launch.admission = (authority, None, row)
            self.source_launch.session_id = record.get("session_id")
        self.admission_pending = False
        refusal = self.bind_parent(parent, parent_kind)
        if refusal is not None:
            return refusal
        if self.no_worker_requested or self.no_worker_finalized:
            self.mark_unknown("late_worker_submission")
            return CustodyRefusal("late_worker_submission", "I03_api_run_direct")
        return None

    def bind_peer_final_record(self, record: Any):
        """Bind a settled Bot receipt to the exact retry admission that produced it."""
        if not isinstance(record, dict) or self.producer_phase != "bot_i02":
            return CustodyRefusal("peer_final_record_missing", "I03_api_run_direct")
        initial_id = self.initial_admission_id
        if (not isinstance(initial_id, str) or not initial_id
                or record.get("admission_id") != initial_id):
            return CustodyRefusal("peer_admission_mismatch", "I03_api_run_direct")
        retry_id = record.get("retry_admission_id")
        if retry_id is None:
            return self.refresh_peer_generation(record)
        retry_generation = record.get("retry_execution_generation")
        if (not isinstance(retry_id, str) or not retry_id
                or retry_id == initial_id
                or type(retry_generation) is not int or retry_generation < 0):
            return CustodyRefusal("peer_generation_missing", "I03_api_run_direct")
        authority = self.admission_authority
        if authority is None:
            return CustodyRefusal("peer_authority_missing", "I03_api_run_direct")
        row, refusal = _authoritative_admission(authority, retry_id)
        if refusal is not None:
            return refusal
        if (
            row.get("admission_id") != retry_id
            or row.get("status") != "terminal"
            or row.get("generation") != retry_generation
            or row.get("target_session_id") != self.admission_session_id
            or row.get("owner_epoch") != self.authority_epoch
            or row.get("principal_id") != self.producer_principal
            or record.get("profile_id") != self.profile_id
            or record.get("authority_epoch") != self.authority_epoch
            or record.get("principal_id") != self.producer_principal
        ):
            return CustodyRefusal("peer_generation_mismatch", "I03_api_run_direct")
        session = getattr(authority.db, "get_session", lambda _id: None)(self.admission_session_id)
        source = session.get("source") if isinstance(session, dict) else None
        if source not in {"bot_room", "api_server"}:
            return CustodyRefusal("peer_source_mismatch", "I03_api_run_direct")
        current_parent, refusal = self.bound_parent_handle()
        if refusal is not None or current_parent is None:
            return CustodyRefusal("peer_parent_unmapped", "I03_api_run_direct")
        parent, _ = self._parent_from_authority(authority, retry_id)
        if parent is None:
            # A bounded Bot retry may outlive the authority's admission index cleanup. The
            # original parent remains the source-owned owner while the retry row/result is read
            # back under the same profile/epoch/principal tuple.
            parent = current_parent
        if (
            parent.coordinator_id != current_parent.coordinator_id
            or parent.profile_id != current_parent.profile_id
            or parent.authority_epoch != current_parent.authority_epoch
            or parent.process_instance != current_parent.process_instance
            or parent.close_generation != current_parent.close_generation
        ):
            return CustodyRefusal("peer_parent_mismatch", "I03_api_run_direct")
        try:
            from gateway.session_results import admission_result
            result = admission_result(authority.db, retry_id)
        except (AttributeError, ImportError, KeyError, LookupError, OSError, RuntimeError,
                TypeError, ValueError, sqlite3.Error):
            result = None
        if not isinstance(result, dict):
            return CustodyRefusal("peer_result_missing", "I03_api_run_direct")
        self.admission_id = retry_id
        self.execution_generation = retry_generation
        self.producer_source = source
        if self.source_launch is not None:
            self.source_launch.admission = (authority, None, row)
        return None

    def refresh_peer_generation(self, record: Any = None):
        """Re-read the final Bot admission tuple after its FIFO claim or bounded retry."""
        authority = self.admission_authority
        admission_id = self.admission_id
        if authority is None or not isinstance(admission_id, str) or not admission_id:
            return CustodyRefusal("peer_generation_missing", "I03_api_run_direct")
        row, refusal = _authoritative_admission(authority, admission_id)
        if refusal is not None:
            return refusal
        if (
            row.get("target_session_id") != self.admission_session_id
            or row.get("owner_epoch") != self.authority_epoch
            or row.get("principal_id") != self.producer_principal
        ):
            return CustodyRefusal("peer_generation_mismatch", "I03_api_run_direct")
        generation = row.get("generation")
        if type(generation) is not int or generation < 0:
            return CustodyRefusal("peer_generation_missing", "I03_api_run_direct")
        if self.execution_generation is not None and self.execution_generation != generation:
            return CustodyRefusal("peer_generation_mismatch", "I03_api_run_direct")
        self.execution_generation = generation
        if isinstance(record, dict):
            retry_id = record.get("retry_admission_id")
            retry_generation = record.get("retry_execution_generation")
            if retry_id == admission_id and retry_generation is not None and retry_generation != generation:
                return CustodyRefusal("peer_generation_mismatch", "I03_api_run_direct")
        return None

    def bind_idempotency_identity(
        self, scope: str, key: str, fingerprint: str, owner_pid: int, owner_started: int,
    ):
        values = (scope, key, fingerprint)
        if not all(isinstance(value, str) and value for value in values):
            return self.mark_unknown("status_identity_mismatch")
        if type(owner_pid) is not int or type(owner_started) is not int:
            return self.mark_unknown("status_identity_mismatch")
        self.reservation_scope = scope
        self.reservation_key = key
        self.reservation_fingerprint = fingerprint
        self.reservation_owner_pid = owner_pid
        self.reservation_owner_started = owner_started
        return None

    def status_identity_expectations(self) -> dict[str, Any]:
        if self.reservation_key is None:
            return {}
        return {
            "expected_scope": self.reservation_scope,
            "expected_idempotency_key": self.reservation_key,
            "expected_fingerprint": self.reservation_fingerprint,
            "expected_owner_pid": self.reservation_owner_pid,
            "expected_owner_started": self.reservation_owner_started,
        }

    def mark_admission_started(self):
        if self.no_worker_finalized:
            return self.mark_unknown("launch_state_conflict")
        # A session SSE owns one outer I03 worker task before its nested canonical I02
        # admission starts. That submitted task is the source-owned wrapper, not a late
        # admission submission; direct routes still refuse an already-submitted worker.
        if (self.worker_submitted
                and not getattr(getattr(self, "source_launch", None), "stream_owner", False)):
            return self.mark_unknown("launch_state_conflict")
        self.admission_pending = True
        self.launch_phase = "admission_pending"
        return None

    def mark_admission_closed(self):
        self.admission_pending = False
        if self.no_worker_requested and not self.worker_submitted:
            return self.mark_no_worker()
        return None

    def retain_late_admission(self, admission: Any):
        """Keep a post-cancellation admission on this handle as sticky unknown."""
        refusal = self.bind_admission(admission)
        self.admission_pending = False
        self.mark_unknown(getattr(refusal, "code", "lifecycle_owned"))
        return refusal

    def mark_unknown(self, reason: str):
        if not self.unknown_reason:
            self.unknown_reason = reason
        return self.coordinator.mark_unknown(self.handle, reason)

    def mark_status_receipt(self, receipt: Any, *, require_store: bool = False):
        """Consume a canonical store/admission receipt, including its exact identity."""
        common = (
            isinstance(receipt, dict)
            and receipt.get("committed") is True
            and type(receipt.get("rowcount")) is int
            and receipt.get("rowcount") == 1
            and receipt.get("run_id") == self.run_id
        )
        store_identity = (
            common
            and receipt.get("storage") == "run_idempotency"
            and isinstance(receipt.get("scope"), str)
            and bool(receipt.get("scope"))
            and isinstance(receipt.get("idempotency_key"), str)
            and bool(receipt.get("idempotency_key"))
            and isinstance(receipt.get("fingerprint"), str)
            and bool(receipt.get("fingerprint"))
            and type(receipt.get("owner_pid")) is int
            and type(receipt.get("owner_started")) is int
            and receipt.get("readback") is True
        )
        exact_reservation = (
            store_identity
            and self.reservation_scope is not None
            and receipt.get("scope") == self.reservation_scope
            and receipt.get("idempotency_key") == self.reservation_key
            and receipt.get("fingerprint") == self.reservation_fingerprint
            and receipt.get("owner_pid") == self.reservation_owner_pid
            and receipt.get("owner_started") == self.reservation_owner_started
        )
        canonical_result = (
            common
            and receipt.get("storage") == "session_admission"
            and receipt.get("admission_id") == self.admission_id
            and type(receipt.get("generation")) is int
            and receipt.get("generation") == self.execution_generation
            and isinstance(receipt.get("principal_id"), str)
            and bool(receipt.get("principal_id"))
            and receipt.get("result_readback") is True
        )
        canonical_cancel = (
            common
            and receipt.get("storage") == "session_admission"
            and receipt.get("admission_id") == self.admission_id
            and receipt.get("preclaim_cancel") is True
            and receipt.get("generation") is None
            and self.execution_generation is None
            and receipt.get("principal_id") == self.producer_principal == "api"
            and isinstance(receipt.get("status"), dict)
            and receipt["status"].get("status") == "terminal"
            and receipt["status"].get("outcome") == "cancelled"
            and receipt.get("result_readback") is True
        )
        if not (exact_reservation if require_store else (store_identity or canonical_result or canonical_cancel)):
            return self.mark_unknown("status_commit_unproven")
        self.status_persisted = True
        self.status_receipt = receipt
        if self.source_launch is not None:
            self.source_launch.status_receipt = receipt
        return None

    def mark_status_persisted(self, committed: bool):
        """Compatibility shim; a boolean projection cannot prove a terminal status."""
        if committed is True:
            return self.mark_unknown("status_commit_unproven")
        return self.mark_unknown("status_commit_unproven")

    def mark_event_enqueued(self, enqueued: bool):
        """Record the actual terminal queue operation; an absent transport is only valid after retirement."""
        if enqueued is True:
            self.event_enqueued = True
            if self.source_launch is not None:
                self.source_launch.terminal_event_outcome = True
            return self.try_terminal()
        if self.transport_retired:
            return None
        return self.mark_unknown("terminal_event_unpublished")

    def mark_stream_closed(self, closed: bool):
        if closed is True:
            self.stream_closed = True
            if self.source_launch is not None:
                self.source_launch.stream_close_outcome = True
            return self.try_terminal()
        if self.transport_retired:
            return None
        return self.mark_unknown("stream_close_unproven")

    def mark_transport_retired(self):
        """Close only the delivery transport; the worker/process lease remains independently live."""
        self.transport_retired = True
        if self.source_launch is not None:
            self.source_launch.transport_retirement_receipt = ("retired", self.run_id)
        return self.try_terminal()

    def mark_control_finalizers(self):
        self.control_finalizers = True
        if self.source_launch is not None:
            self.source_launch.control_finalizer_receipt = ("retired", self.run_id)
        return self.try_terminal()

    def mark_worker_submitted(self):
        """Close the no-worker boundary before awaiting the executor future."""
        if self.no_worker_finalized:
            return self.mark_unknown("launch_state_conflict")
        if self.no_worker_requested:
            return self.mark_unknown("late_worker_submission")
        # A very short executor turn can finish before the submitting coroutine resumes.  The
        # completed phase is positive evidence that submission did occur; make the late barrier
        # idempotent instead of turning a normal fast run into sticky unknown custody.
        if self.worker_done:
            if self.launch_phase == "completed":
                self.worker_submitted = True
                return None
            return self.mark_unknown("launch_state_conflict")
        self.worker_submitted = True
        self.launch_phase = "submitted"
        return None

    def mark_worker_done(self):
        self.worker_done = True
        self.launch_phase = "completed"
        return self.try_terminal()

    def mark_no_worker(self):
        """Record a proven pre-submission outcome; a late submission must not be possible afterward."""
        if self.admission_pending:
            self.no_worker_requested = True
            self.launch_phase = "admission_pending"
            return CustodyRefusal("admission_pending", "I03_api_run_direct")
        if self.worker_submitted:
            return self.mark_unknown("late_worker_submission")
        self.no_worker_finalized = True
        self.worker_done = True
        self.launch_phase = "no_worker"
        return self.try_terminal()

    def request_no_worker(self):
        """Defer a cancellation finalizer while shielded admission can still commit."""
        if self.admission_pending:
            self.no_worker_requested = True
            self.launch_phase = "admission_pending"
            return None
        return self.mark_no_worker()

    def _process_owner_task_id(self) -> Optional[str]:
        return self.process_owner_task_id or self.run_id or self.process_task_id

    def mark_process_started(self, task_id: str, owner_task_id: Optional[str] = None):
        if not isinstance(task_id, str) or not task_id:
            return self.mark_unknown("process_identity_missing")
        owner = owner_task_id or self.process_owner_task_id or self.run_id
        if not isinstance(owner, str) or not owner:
            return self.mark_unknown("process_owner_identity_missing")
        if self.process_owner_task_id is not None and self.process_owner_task_id != owner:
            return self.mark_unknown("process_owner_identity_mismatch")
        self.process_task_id = task_id
        self.process_owner_task_id = owner
        return None

    def arm_process_scope(self, task_id: str, owner_task_id: Optional[str] = None):
        """Reserve the exact ProcessRegistry scope before agent/provider/tool effects."""
        owner = owner_task_id or self.process_owner_task_id or self.run_id
        if not isinstance(owner, str) or not owner:
            return self.mark_unknown("process_owner_identity_missing")
        if self.process_scope is not None:
            if (self.process_task_id != task_id
                    or self._process_owner_task_id() != owner):
                return self.mark_unknown("process_task_identity_mismatch")
            return None
        try:
            from tools.process_registry import process_registry
            begin_scope = getattr(process_registry, "begin_process_scope", None)
            scope = begin_scope(
                owner_task_id=owner, profile_id=self.profile_id,
                authority_epoch=self.authority_epoch,
            ) if callable(begin_scope) else None
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            scope = None
        if scope is None:
            return self.mark_unknown("process_scope_unavailable")
        refusal = self.mark_process_started(task_id, owner)
        if refusal is not None:
            return refusal
        self.process_scope = scope
        return None

    def close_process_scope(self):
        """Close the source-owned scope and retain its typed childless/descendant receipt."""
        scope = self.process_scope
        if scope is None:
            return self.mark_unknown("process_scope_unavailable")
        try:
            receipt = scope.close()
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return self.mark_unknown("process_scope_unavailable")
        if (
            not isinstance(receipt, dict)
            or receipt.get("owner_task_id") != self._process_owner_task_id()
            or receipt.get("process_instance") != self.process_instance
            or receipt.get("authority_epoch") != self.authority_epoch
            or receipt.get("profile_id") != self.profile_id
            or receipt.get("observed") is not True
        ):
            return self.mark_unknown("process_scope_identity_mismatch")
        self.process_scope_receipt = receipt
        if not self.parent_bound and receipt.get("childless") is not True:
            # A previously observed I15 proof cannot survive a late or newly recorded child.
            # The next terminal attempt must obtain a fresh same-owner observation instead of
            # allowing the stale boolean to release this run.
            self.process_proof = False
        return None

    def observe_process_owner(self):
        """Use the existing process-registry proof API; absence is unknown, never clean."""
        if self.parent_bound:
            _, refusal = self.bound_parent_handle()
            if refusal is not None:
                return self.mark_unknown(getattr(refusal, "code", "parent_owner_unmapped"))
            self.process_proof = True
            return None
        if not self._process_owner_task_id():
            return self.mark_unknown("cross_process_unproven")
        scope_receipt = self.process_scope_receipt
        if isinstance(scope_receipt, dict) and scope_receipt.get("childless") is True:
            # The source-owned fence is the only childless proof.  An empty registry map or a
            # completed Future cannot establish this boundary, so no I15 query is substituted.
            return None
        try:
            from tools.process_registry import process_registry
            evidence_fn = getattr(process_registry, "preservation_owner_evidence", None)
            if not callable(evidence_fn):
                return self.mark_unknown("cross_process_unproven")
            evidence = evidence_fn(
                process_instance=self.process_instance, authority_epoch=self.authority_epoch,
                close_generation=self.handle.close_generation,
                owner_id=f"{self.process_instance}:I15_process_registry_terminal",
                profile_id=self.profile_id, task_id=self._process_owner_task_id())
        except Exception:  # health: allow BLE001 -- opaque process proof failures stay unknown
            return self.mark_unknown("cross_process_unproven")
        if not isinstance(evidence, dict) or evidence.get("observed") is not True:
            return self.mark_unknown("cross_process_unproven")
        if evidence.get("cross_process_proof") is not True:
            return self.mark_unknown("cross_process_unproven")
        self.process_proof = True
        return None

    def _direct_executor_receipt(self):
        """Read the exact local executor/writer witness for the pre-authority route.

        The legacy route's source-owned proof is the closed process-scope fence plus the
        original executor Future retained on both custody objects and the exact keyed terminal
        row read back from the existing status store.  This is local ownership evidence only;
        it deliberately carries no cross-process claim.
        """
        scope_receipt = self.process_scope_receipt
        scope = self.process_scope
        if (
            self.parent_bound or self.process_proof or self.reservation_key is None
            or not isinstance(scope_receipt, dict)
            or scope is None
            or scope_receipt.get("registry_id") != id(getattr(scope, "registry", None))
            or scope_receipt.get("childless") is not True
            or scope_receipt.get("created_session_ids") != ()
            or scope_receipt.get("pending_reservation_ids") != ()
            or scope_receipt.get("late_creation") is not False
            or scope_receipt.get("owner_task_id") != self._process_owner_task_id()
            or scope_receipt.get("process_instance") != self.process_instance
            or scope_receipt.get("authority_epoch") != self.authority_epoch
            or scope_receipt.get("profile_id") != self.profile_id
            or scope_receipt.get("observed") is not True
        ):
            return None
        launch = self.source_launch
        work = self.worker_future
        if (
            launch is None
            or getattr(launch, "custody", None) is not self
            or getattr(launch, "owner", None) is None
            or work is None
            or self.execution_task is not work
            or getattr(launch, "worker_future", None) is not work
            or not (
                getattr(launch, "execution_task", None) is work
                or (
                    getattr(launch, "queue", None) is not None
                    and callable(getattr(launch.execution_task, "done", None))
                    and launch.execution_task.done() is True
                )
            )
            or not callable(getattr(work, "done", None))
            or work.done() is not True
            or (callable(getattr(work, "cancelled", None)) and work.cancelled())
        ):
            return None
        status_receipt = self.status_receipt_from_source()
        if (
            not isinstance(status_receipt, dict)
            or status_receipt.get("storage") != "run_idempotency"
            or status_receipt.get("readback") is not True
        ):
            return None
        return {
            "kind": "I03_direct_executor",
            "process_instance": self.process_instance,
            "authority_epoch": self.authority_epoch,
            "close_generation": self.handle.close_generation,
            "owner_id": self.handle.owner_id,
            "task_id": self._process_owner_task_id(),
            "observed": True,
            "cross_process_proof": False,
        }

    def source_process_receipt(self):
        """Return the exact source-owned process or local-executor receipt."""
        if self.parent_bound:
            parent, refusal = self.bound_parent_handle()
            if refusal is not None or parent is None:
                # Terminalizing the canonical admission retires its live coordinator edge.  The
                # original parent handle remains retained on this source launch, so a completed
                # run can still expose the exact local owner witness after that edge is retired;
                # it is deliberately not promoted to cross-process proof.
                parent = self.parent_handle
                if (not self.released or not isinstance(parent, CustodyHandle)
                        or parent.coordinator_id != self.handle.coordinator_id
                        or parent.profile_id != self.profile_id
                        or parent.authority_epoch != self.authority_epoch
                        or parent.process_instance != self.process_instance
                        or parent.close_generation != self.handle.close_generation):
                    return None
            return {
                "kind": "parent_handle", "process_instance": self.process_instance,
                "authority_epoch": self.authority_epoch,
                "close_generation": self.handle.close_generation,
                "owner_id": parent.owner_id, "task_id": None,
                "observed": True, "cross_process_proof": False,
            }
        if self.process_proof:
            owner_task_id = self._process_owner_task_id()
            if not isinstance(owner_task_id, str) or not owner_task_id:
                return None
            return {
                "kind": "I15_process_registry_terminal",
                "process_instance": self.process_instance,
                "authority_epoch": self.authority_epoch,
                "close_generation": self.handle.close_generation,
                "owner_id": f"{self.process_instance}:I15_process_registry_terminal",
                "task_id": owner_task_id, "observed": True,
                "cross_process_proof": True,
            }
        return self._direct_executor_receipt()

    def status_receipt_from_source(self):
        """Read back the bound admission/result without consulting a local projection."""
        if self.reservation_key is not None:
            owner = getattr(getattr(self, "source_launch", None), "owner", None)
            store = getattr(owner, "_run_idempotency_store", None)
            reader = getattr(store, "status_for_run", None)
            if not callable(reader):
                return None
            try:
                record = reader(self.reservation_scope, self.run_id)
            except (AttributeError, KeyError, LookupError, OSError, RuntimeError,
                    TypeError, ValueError, sqlite3.Error):
                return None
            status = record.get("status") if isinstance(record, dict) else None
            if (
                not isinstance(record, dict)
                or record.get("scope") != self.reservation_scope
                or record.get("idempotency_key") != self.reservation_key
                or record.get("fingerprint") != self.reservation_fingerprint
                or record.get("owner_pid") != self.reservation_owner_pid
                or record.get("owner_started") != self.reservation_owner_started
                or not isinstance(status, dict)
                or status.get("status") not in _TERMINAL_RUN_STATUSES
            ):
                return None
            return {
                "committed": True, "storage": "run_idempotency", "rowcount": 1,
                "run_id": self.run_id, "scope": record["scope"],
                "idempotency_key": record["idempotency_key"],
                "fingerprint": record["fingerprint"],
                "owner_pid": record["owner_pid"], "owner_started": record["owner_started"],
                "readback": True, "status": status,
            }
        if self.admission_authority is None:
            return None
        return _session_status_receipt(
            self.admission_authority, self, expected_session_id=self.admission_session_id)

    def authoritative_status_receipt(self, projected: Any = None):
        """Use the durable keyed receipt or the exact bound SessionDB readback."""
        if self.reservation_key is not None:
            return self.status_receipt_from_source()
        return self.status_receipt_from_source()

    def _retained_terminal_release_is_exact(self) -> bool:
        from gateway.platforms.api_server_run_completion import (
            _retained_terminal_release_is_exact as check_retained_terminal_release,
        )
        return check_retained_terminal_release(self)

    def try_terminal(self):
        self._terminal_attempted = True
        if self.released:
            if not self._retained_terminal_release_is_exact():
                return self.mark_unknown("terminal_release_receipt_missing")
            return None
        publication_settled = self.transport_retired or (self.event_enqueued and self.stream_closed)
        if self.admission_pending or not all((self.status_persisted, publication_settled,
                    self.worker_done, self.control_finalizers)):
            return CustodyRefusal("custody_pending", "I03_api_run_direct")
        if self.process_scope is not None:
            refusal = self.close_process_scope()
            if refusal is not None:
                return refusal
            # A producer that arrived after the scope closed invalidates the childless
            # observation.  Preserve the existing close_process_scope API for callers that
            # inspect its receipt, but make the terminal path retain the run as typed unknown
            # before any direct or coordinator release proof can be considered.
            if isinstance(self.process_scope_receipt, dict) and self.process_scope_receipt.get(
                "late_creation"
            ) is True:
                return self.mark_unknown("process_scope_unavailable")
        if self.unknown_reason:
            return CustodyRefusal("custody_unknown", "I03_api_run_direct")
        if self.parent_bound:
            _, refusal = self.bound_parent_handle()
            if refusal is not None:
                return self.mark_unknown(getattr(refusal, "code", "parent_owner_unmapped"))
        if not (
            self.parent_bound
            or self.process_proof
            or self.no_worker_finalized
            or self._direct_executor_receipt() is not None
        ):
            return CustodyRefusal("cross_process_unproven", "I03_api_run_direct")
        if self.process_scope is not None:
            receipt, refusal = self.process_scope.terminalize(self.coordinator, self.handle)
            self.process_scope_receipt = receipt
            if refusal is False:
                self.process_proof = False
                return self.mark_unknown("process_scope_unavailable")
            if refusal is not None:
                return refusal
            self.terminal_release_receipt = self._terminal_release_evidence()
            self.released = True
            return None
        refusal = self.coordinator.mark_terminal(self.handle)
        if refusal is None:
            self.terminal_release_receipt = self._terminal_release_evidence()
            self.released = True
        return refusal

    def _terminal_release_evidence(self):
        parent = self.parent_handle
        return {
            "child_owner_id": self.handle.owner_id,
            "parent_owner_id": getattr(parent, "owner_id", None),
            "profile_id": self.profile_id,
            "authority_epoch": self.authority_epoch,
            "process_instance": self.process_instance,
            "close_generation": self.handle.close_generation,
            "admission_id": self.admission_id,
            "released": True,
        }

    def recovery_identity(self) -> dict[str, Any]:
        """Return only the immutable identity a resolver may echo from its request."""
        return {
            "run_id": self.run_id, "profile_id": self.profile_id,
            "authority_epoch": self.authority_epoch,
            "process_instance": self.process_instance,
            "close_generation": self.handle.close_generation,
            "execution_generation": self.execution_generation,
            "owner_id": self.handle.owner_id,
        }

    def source_recovery_receipt(self, adapter: Any):
        """Project a terminal receipt from retained source objects, never client claims."""
        if (
            not self.unknown_reason
            and not getattr(self, "_finalizer_recovery_in_progress", False)
            and getattr(self, "finalizer_recovery_receipt", None) is None
        ):
            return CustodyRefusal("custody_not_unknown", "I03_api_run_direct")
        try:
            from gateway.platforms.api_server_run_witness import read_run_witness
            witness, refusal = read_run_witness(adapter, self)
        except (AttributeError, ImportError, KeyError, LookupError, OSError, RuntimeError,
                TypeError, ValueError, sqlite3.Error):
            witness, refusal = None, CustodyRefusal("source_witness_unavailable", "I03_api_run_direct")
        if refusal is not None or not isinstance(witness, dict):
            return refusal or CustodyRefusal("source_witness_unavailable", "I03_api_run_direct")
        status_receipt = witness.get("status_receipt")
        if not isinstance(status_receipt, dict):
            return CustodyRefusal("status_commit_unproven", "I03_api_run_direct")
        self.stream_closed = witness.get("stream_close") is True or self.stream_closed
        self.event_enqueued = witness.get("terminal_event") is True or self.event_enqueued
        self.transport_retired = witness.get("transport_retired") == ("retired", self.run_id) or self.transport_retired
        self.control_finalizers = witness.get("control_finalizer") == ("retired", self.run_id) or self.control_finalizers
        if not (self.worker_done and self.control_finalizers and (
            self.transport_retired or (self.event_enqueued and self.stream_closed))
        ):
            return CustodyRefusal("custody_pending", "I03_api_run_direct")
        if self.parent_bound:
            _, refusal = self.bound_parent_handle()
            if refusal is not None:
                parent = self.parent_handle
                authority = self.admission_authority
                handles = getattr(authority, "_preservation_handles", None)
                child_receipt = self.terminal_release_receipt
                child_released = (
                    self.released is True
                    and isinstance(child_receipt, dict)
                    and child_receipt == self._terminal_release_evidence()
                )
                retained_parent = (
                    child_released
                    and isinstance(handles, dict)
                    and handles.get(self.admission_id) is parent
                )
                parent_readback = getattr(
                    authority, "_preservation_i02_parent_released", None,
                )
                recovery_receipt = getattr(self, "finalizer_recovery_receipt", None)
                recovered_parent = (
                    child_released
                    and (
                        getattr(self, "_finalizer_recovery_in_progress", False)
                        or getattr(recovery_receipt, "recovered_completion", None) is not None
                    )
                    and callable(parent_readback)
                    and parent_readback(self.admission_id, parent)
                )
                if not retained_parent and not recovered_parent:
                    return refusal
        else:
            if not self.process_proof:
                refusal = self.observe_process_owner()
                if refusal is not None:
                    return CustodyRefusal("cross_process_unproven", "I03_api_run_direct")
            if not self.process_proof and self._direct_executor_receipt() is None:
                return CustodyRefusal("cross_process_unproven", "I03_api_run_direct")
        process_receipt = self.source_process_receipt()
        if process_receipt is None:
            return CustodyRefusal("cross_process_unproven", "I03_api_run_direct")
        return {
            **self.recovery_identity(), "process_receipt": process_receipt,
            "status_receipt": status_receipt, "worker_done": self.worker_done,
            "transport_retired": self.transport_retired,
            "event_enqueued": self.event_enqueued, "stream_closed": self.stream_closed,
            "control_finalizers": self.control_finalizers,
        }

    def reconcile_unknown_receipt(self, receipt: Any):
        """Reconcile only the original in-memory handle with an exact typed terminal receipt."""
        required = {
            "run_id", "profile_id", "authority_epoch", "process_instance", "close_generation",
            "execution_generation",
            "owner_id", "process_receipt", "status_receipt", "worker_done", "transport_retired", "event_enqueued",
            "stream_closed", "control_finalizers",
        }
        if not isinstance(receipt, dict) or set(receipt) != required:
            return CustodyRefusal("custody_identity_mismatch", "I03_api_run_direct")
        generation = receipt.get("execution_generation")
        generation_matches = (
            type(generation) is int and generation >= 0 and generation == self.execution_generation
        ) or (
            self.producer_phase == "I15_direct"
            and self.execution_generation is None
            and generation is None
        )
        lifecycle_flags = (
            type(receipt.get("worker_done")) is bool and receipt["worker_done"]
            and type(receipt.get("control_finalizers")) is bool and receipt["control_finalizers"]
            and type(receipt.get("transport_retired")) is bool
            and type(receipt.get("event_enqueued")) is bool
            and type(receipt.get("stream_closed")) is bool
            and (receipt["transport_retired"]
                 or (receipt["event_enqueued"] and receipt["stream_closed"]))
        )
        if (
            receipt["run_id"] != self.run_id
            or receipt["profile_id"] != self.profile_id
            or receipt["authority_epoch"] != self.authority_epoch
            or receipt["process_instance"] != self.process_instance
            or receipt["close_generation"] != self.handle.close_generation
            or not generation_matches
            or receipt["owner_id"] != self.handle.owner_id
            or not _process_receipt_matches(self, receipt["process_receipt"])
            or not lifecycle_flags
            or not self.unknown_reason
        ):
            return CustodyRefusal("custody_identity_mismatch", "I03_api_run_direct")
        if self.admission_pending:
            return CustodyRefusal("custody_pending", "I03_api_run_direct")
        status_receipt = receipt["status_receipt"]
        local_parent_projection = (
            isinstance(status_receipt, dict)
            and status_receipt.get("storage") == "in_memory_projection"
            and self.parent_bound
            and self.admission_authority is None
            and self.reservation_key is None
        )
        if local_parent_projection:
            status_refusal = None
            self.status_persisted = True
            self.status_receipt = status_receipt
        else:
            status_refusal = self.mark_status_receipt(
                status_receipt, require_store=self.reservation_key is not None)
        if status_refusal is not None:
            return status_refusal
        process_kind = receipt["process_receipt"].get("kind")
        if process_kind == "I15_process_registry_terminal":
            self.process_proof = True
        self.worker_done = True
        self.transport_retired = True
        self.event_enqueued = True
        self.stream_closed = True
        self.control_finalizers = True
        if not (
            self.status_persisted
            and (
                self.parent_bound
                or self.process_proof
                or self.no_worker_finalized
                or process_kind == "I03_direct_executor"
            )
            and self.worker_done
            and (self.transport_retired or (self.event_enqueued and self.stream_closed))
            and self.control_finalizers
        ):
            return CustodyRefusal("custody_pending", "I03_api_run_direct")
        refusal = self.coordinator.reconcile_unknown(
            self.handle,
            owner_id=self.handle.owner_id,
            profile_id=self.profile_id,
            process_instance=self.process_instance,
            authority_epoch=self.authority_epoch,
        )
        if refusal is not None:
            return refusal
        previous_unknown = self.unknown_reason
        self.unknown_reason = None
        refusal = self.try_terminal()
        if refusal is not None:
            self.unknown_reason = previous_unknown
            self.coordinator.mark_unknown(self.handle, previous_unknown or "custody_unknown")
        return refusal

    def release_unaccepted(self):
        if self.status_persisted or self.parent_bound:
            return CustodyRefusal("accepted_row_unknown", "I03_api_run_direct")
        return self.coordinator.release_unaccepted(self.handle)

    def finish(self, task_id: str):
        if self.process_task_id is None:
            self.mark_process_started(task_id, self._process_owner_task_id())
        # A no-authority legacy turn first receives its keyed terminal writer receipt after the
        # executor returns.  Its exact Future/launch pair is the local source witness; probing
        # I15 here would run before that receipt exists and turn every ordinary in-process turn
        # into sticky unknown.  Parent-bound/canonical runs retain the existing process-owner
        # observation at the worker boundary.
        if self.reservation_key is None:
            self.observe_process_owner()
        return self.mark_worker_done()

    def complete(self):
        """Reject completion without an authoritative status receipt."""
        return self.mark_unknown("status_commit_unproven")

    def mark_completion_unknown(self, reason: str):
        """Retain a released child while its authenticated I02 parent remains unknown."""
        if not isinstance(reason, str) or not reason:
            reason = "finalizer_completion_unknown"
        if not self.unknown_reason:
            self.unknown_reason = reason
        self.launch_phase = "unknown"

    def complete_from_source(self, status_receipt: Any = None):
        """Finalize a synchronous legacy turn from canonical admission/result readback."""
        refusal = self.mark_status_receipt(
            status_receipt if status_receipt is not None else self.status_receipt_from_source())
        if refusal is not None:
            return refusal
        self.mark_transport_retired()
        self.mark_control_finalizers()
        if self.released:
            return None
        return self.try_terminal()

    def fail(self):
        return self.mark_unknown("partial_launch")


def reserve_run(adapter: Any, run_id: str, profile_id: Optional[str]):
    return ApiRunCustody.reserve(adapter, run_id, profile_id)


def active_count(adapter: Any) -> int:
    custody = getattr(adapter, "_run_custody", {})
    return sum(1 for item in custody.values() if not getattr(item, "released", False))


def reserve_legacy(adapter: Any, profile_id: Optional[str], run_id: Optional[str]):
    owner_id = run_id or f"api-turn:{__import__('uuid').uuid4().hex}"
    custody, refusal = reserve_run(adapter, owner_id, profile_id)
    if refusal is not None:
        raise ApiRunCustodyError(refusal)
    adapter._run_custody[owner_id] = custody
    from gateway.platforms.api_server_runs import _make_legacy_launch
    custody.source_launch = _make_legacy_launch(adapter, custody)
    return custody

