"""Small, source-owned predicates used by async handoff recovery.

This module contains no second ledger.  It only normalizes the authenticated owner
scope already carried by the runtime worker claim, classifies durable rows, and
selects rows that are safe to retire from the canonical ``async_delegations`` table.
Unknown or malformed evidence is deliberately retained.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Optional

OWNER_SCOPE_KEYS = ("profile_id", "session_id", "generation", "epoch")
_CUSTODY_EXCEPTIONS = (Exception, KeyboardInterrupt, SystemExit, GeneratorExit)
_ROUTINE_EXCEPTIONS = (Exception,)


def _current_origin_session_id() -> str:
    """Raw session id of the ORIGINATING api_server request, or ``""``. ``HERMES_SESSION_ID``
    is unsafe here: building the child agent calls ``set_current_session_id(child.session_id)``
    just before dispatch, so the wake would self-post into the subagent's own session. The
    request-scoped ``HERMES_SESSION_CHAT_ID`` (raw X-Hermes-Session-Id on api_server) survives
    child construction; on push platforms chat_id is a chat, not a session => ``""``."""
    try:
        from gateway.session_context import get_session_env
        is_api = get_session_env("HERMES_SESSION_PLATFORM", "") == "api_server"
        return (get_session_env("HERMES_SESSION_CHAT_ID", "") or "") if is_api else ""
    except _ROUTINE_EXCEPTIONS:
        return ""


def _single_crash(error: str, duration: float) -> dict[str, Any]:
    return {"status": "error", "summary": None, "error": error, "api_calls": 0, "duration_seconds": duration}


def _batch_crash(error: str, duration: float) -> dict[str, Any]:
    return {"results": [], "error": error, "total_duration_seconds": duration}


def _batch_status(combined: dict[str, Any]) -> str:
    """Batch status: completed unless every child errored/was interrupted."""
    child_results = combined.get("results") or []
    ok = ("completed", "success")
    return "error" if child_results and all(r.get("status") not in ok for r in child_results) else "completed"




@dataclass(frozen=True)
class _SettlementReceipt:
    """Source-owned acknowledgement of the two terminal delivery edges."""

    durable: bool
    queued: bool
    event: Optional[dict[str, Any]] = None
    result: Optional[dict[str, Any]] = None

    @property
    def settled(self) -> bool:
        return self.durable and self.queued


def normalize_owner_scope(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != set(OWNER_SCOPE_KEYS):
        return None
    if (not isinstance(value["profile_id"], str) or not value["profile_id"]
            or not isinstance(value["session_id"], str) or not value["session_id"]
            or type(value["generation"]) is not int or value["generation"] < 0
            or type(value["epoch"]) is not int or value["epoch"] < 1):
        return None
    return {key: value[key] for key in OWNER_SCOPE_KEYS}


def capture_owner_scope_state(session_id: Any) -> tuple[dict[str, Any] | None, bool]:
    """Read a real active authority handle and report whether the route is managed.

    Direct CLI/tool dispatches can legitimately run outside a gateway authority.  They
    still carry the authenticated local PID/birth and retirement lease, but there is no
    profile epoch to invent.  Once a live runner exposes an active authority, a missing
    session handle is a managed-scope failure and must remain a refusal.
    """
    if not isinstance(session_id, str) or not session_id:
        return None, False
    try:
        from gateway.run import _gateway_runner_ref
        from gateway.session_authorities import active_authority
        from gateway.session_contract import SessionRef
        runner = _gateway_runner_ref()
        if runner is None:
            return None, False
        authority = active_authority(runner)
        if authority is None:
            return None, False
        if not isinstance(getattr(authority, "profile_id", None), str):
            return None, True
        handle = authority._handle(SessionRef(authority.profile_id, session_id))
        scope = normalize_owner_scope({
            "profile_id": authority.profile_id,
            "session_id": session_id,
            "generation": handle.execution_generation,
            "epoch": authority.epoch,
        })
        return scope, True
    except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError):
        return None, True


def capture_owner_scope(session_id: Any) -> dict[str, Any] | None:
    """Read the active authority's typed handle, never profile/epoch from request data."""
    return capture_owner_scope_state(session_id)[0]


def owner_fence_matches(expected: Any, saved: Any, *, require_scope: bool = True) -> bool:
    """Compare the entire persisted admission identity, including local process birth."""
    if not isinstance(expected, dict) or not isinstance(saved, dict):
        return False
    for key in ("handoff_id", "owner_execution_id", "owner_pid", "owner_started_at"):
        if saved.get(key) != expected.get(key):
            return False
    expected_scope = normalize_owner_scope(expected.get("owner_scope"))
    saved_scope = normalize_owner_scope(saved.get("owner_scope"))
    if expected.get("owner_scope") is not None and expected_scope is None:
        return False
    if saved.get("owner_scope") is not None and saved_scope is None:
        return False
    # Managed routes require the complete profile/session/generation/epoch fence.
    # Standalone/direct-tool routes have no authority scope to mint; their physical
    # PID/birth and execution identity above remain the complete local fence.
    if expected_scope is None or saved_scope is None:
        return not require_scope and expected_scope is None and saved_scope is None
    return expected_scope == saved_scope


def recovered_results(task: dict[str, Any], result_json: str | None, error: str) -> list[dict[str, Any]] | None:
    partial = json.loads(result_json or "{}") or {}
    if not (task.get("is_batch") and partial.get("partial") and partial.get("results")):
        return None
    recorded = {r["task_index"]: r for r in partial["results"] if isinstance(r.get("task_index"), int)}
    indexes = task.get("task_indexes") or list(range(len(task.get("goals") or [])))
    return [recorded.get(i) or {"task_index": i, "status": "unknown", "summary": None, "error": error}
            for i in indexes]


def owner_liveness() -> Optional[Callable[[Any, Any], bool]]:
    """Return the shared drift-tolerant PID/birth comparator, or no probe."""
    try:
        from gateway.status import _pid_exists, get_process_start_time, start_time_fingerprints_match
    except (AttributeError, ImportError, LookupError, OSError, RuntimeError, TypeError, ValueError):
        return None

    def alive(pid, started) -> Optional[bool]:
        if type(pid) is not int or pid <= 0 or type(started) is not int:
            return None
        try:
            if not _pid_exists(pid):
                return False
            observed = get_process_start_time(pid)
            return start_time_fingerprints_match(started, observed) if observed is not None else None
        except (AttributeError, LookupError, OSError, RuntimeError, TypeError, ValueError):
            return None
    return alive


def _strict_execution_claim(execution: Any, delegation_id: Any) -> bool:
    """Reuse the authenticated delegation validator before a row can be retired."""
    try:
        from gateway.session_worker_delegation import _execution
        _execution(execution, delegation_id)
    except (AttributeError, ImportError, KeyError, RuntimeError, TypeError, ValueError):
        return False
    return True


def _integral_owner_birth(value: Any) -> bool:
    """Accept SQLite's REAL affinity for an otherwise canonical integer birth."""
    if type(value) not in (int, float) or value < 0:
        return False
    try:
        return int(value) == value
    except (OverflowError, ValueError):
        return False


def _execution_is_prunable(execution_json: Any, state: Any, delivery_state: Any,
                           *, delegation_id: Any, parent_session_id: Any, owner_pid: Any,
                           owner_started_at: Any, owner_execution_id: Any) -> bool:
    """Only a typed terminal claim matching every durable owner column can be evicted."""
    if delivery_state != "delivered" or state in {"running", "finalizing"}:
        return False
    try:
        execution = json.loads(execution_json or "")
    except (TypeError, ValueError):
        return False
    if not isinstance(execution, dict):
        return False
    if (execution.get("handoff_id") != delegation_id
            or execution.get("owner_execution_id") != owner_execution_id
            or execution.get("owner_pid") != owner_pid
            or execution.get("owner_started_at") != owner_started_at
            or not isinstance(delegation_id, str) or not delegation_id
            or not isinstance(owner_execution_id, str) or not owner_execution_id
            or type(owner_pid) is not int or owner_pid <= 0
            or not _integral_owner_birth(owner_started_at)):
        return False
    if (not _integral_owner_birth(execution.get("owner_started_at"))
            or not _strict_execution_claim(execution, delegation_id)):
        return False
    scope = normalize_owner_scope(execution.get("owner_scope"))
    if (scope is None or not isinstance(parent_session_id, str) or not parent_session_id
            or scope["session_id"] != parent_session_id):
        return False
    if execution.get("accepted") == "no":
        return execution.get("state") == "rejected_before_enqueue" and execution.get("worker_exited") is True
    return (execution.get("accepted") == "yes"
            and execution.get("worker_exited") is True
            and execution.get("state") in {"runner_exited", "cancelled_before_start"})


def prunable_ids(conn: Any, cutoff: float, max_retained: int, protected_ids: tuple[str, ...]) -> tuple[str, ...]:
    """Choose only exact delivered terminal rows; pending/unknown rows never enter caps."""
    protected = set(protected_ids)
    rows = conn.execute(
        "SELECT delegation_id,parent_session_id,state,delivery_state,updated_at,execution_json,"
        "owner_pid,owner_started_at,owner_execution_id "
        "FROM async_delegations WHERE state NOT IN ('running','finalizing')"
    ).fetchall()
    eligible = [row for row in rows if row[0] not in protected and _execution_is_prunable(
        row[5], row[2], row[3], delegation_id=row[0], parent_session_id=row[1],
        owner_pid=row[6], owner_started_at=row[7], owner_execution_id=row[8])]
    aged = {row[0] for row in eligible if row[3] == "delivered" and row[4] < cutoff}
    excess = max(0, len(eligible) - max_retained)
    by_age = sorted(eligible, key=lambda row: (0 if row[3] == "delivered" else 1, row[4]))
    selected = set(aged)
    selected.update(row[0] for row in by_age[:excess])
    return tuple(sorted(selected))


def reconcile_persist_failure(
    *, record: dict[str, Any], delegation_id: str, label: str, error: str,
    failure_result: Callable[[dict[str, Any], str], dict[str, Any]],
    release_lease: Callable[[], bool], mark_settlement_pending: Callable[[], None],
    mark_admission_recovery: Callable[[str], None], release_after_settlement: Callable[[], None],
    read_admission_row: Callable[[str], tuple[bool, Optional[dict[str, Any]]]],
    admission_row_matches: Callable[[dict[str, Any], Optional[dict[str, Any]]], bool],
    reject_before_enqueue: Callable[[dict[str, Any], str], bool],
    finalize: Callable[..., Any], records_lock: Any, records: dict[str, Any],
    logger: logging.Logger,
) -> dict[str, Any]:
    """Reconcile a durable-admission write failure before any executor submit."""
    readback_known, saved = read_admission_row(delegation_id)
    if readback_known and saved is None:
        released = release_lease()
        if released:
            with records_lock:
                records.pop(delegation_id, None)
            return {"status": "rejected", "error": f"{error}; no admission row exists and no worker was submitted"}
        mark_admission_recovery(f"{error}; admission row absent but exact lease release was refused")
        return {"status": "rejected", "error": f"{error}; admission row absent but custody is retained"}
    if readback_known and admission_row_matches(record, saved):
        if not reject_before_enqueue(record, error):
            mark_admission_recovery(
                f"{error}; authenticated admission could not close as no-submit; exact custody retained"
            )
            return {"status": "rejected", "error": f"{error}; no-submit transition is unknown and custody is retained"}
        try:
            finalize(delegation_id, lambda snapshot: failure_result(snapshot, error), "error")
        except _CUSTODY_EXCEPTIONS:
            logger.exception("Async delegation%s admission reconciliation failed for %s", label, delegation_id)
            mark_settlement_pending()
        finally:
            release_after_settlement()
        return {"status": "rejected", "error": error}
    reason = ("durable admission readback unavailable" if not readback_known
              else "durable admission row failed ownership/state authentication")
    mark_admission_recovery(f"{error}; {reason}; exact custody retained")
    return {"status": "rejected", "error": f"{error}; {reason}; retry reconciliation before cleanup"}
