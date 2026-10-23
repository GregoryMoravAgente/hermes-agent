#!/usr/bin/env python3
"""Async (background) delegation registry behind ``delegate_task(background=true)``.

The parent dispatches a subagent on a module-level daemon executor and returns a handle
immediately. On completion a ``type="async_delegation"`` event (self-contained task-source
block) is pushed onto the SHARED ``process_registry.completion_queue`` the CLI/gateway drain
while idle, so results surface as a NEW turn (never mid-turn) and inherit its de-dup and
crash-recovery wiring. Only the async lifecycle lives here; the child run is an injected ``runner``."""

from __future__ import annotations

import contextvars
import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional

from hermes_constants import get_hermes_home, hermes_home_key
from tools.daemon_pool import DaemonThreadPoolExecutor
from tools.thread_context import propagate_context_to_thread
from tools.async_delegation_worker import worker_ledger, require_ledger_owner
from tools.delegation_redaction import redact_child_result, redact_completion_event
from tools.async_delegation_recovery import (
    capture_owner_scope_state,
    normalize_owner_scope,
    owner_fence_matches,
    owner_liveness as _owner_liveness,
    prunable_ids,
    reconcile_persist_failure,
    recovered_results as _recovered_results,
    _SettlementReceipt,
    _batch_crash,
    _batch_status,
    _current_origin_session_id,
    _single_crash,
)

logger = logging.getLogger(__name__)

# Keep exception boundaries explicit for the custody state machine.  The
# named tuples preserve the previous Exception/BaseException behavior while
# making each opaque persistence/worker boundary visible to the health gate.
_ROUTINE_EXCEPTIONS = (Exception,)
_CUSTODY_EXCEPTIONS = (Exception, KeyboardInterrupt, SystemExit, GeneratorExit)

# ── Module-level state ──────────────────────────────────────────────────────
# Persistent daemon executor (never a `with ThreadPoolExecutor()` block, which
# would join on exit and defeat async); daemon workers can't hang a hard exit.
_executor: Optional[ThreadPoolExecutor] = None
_executor_lock = threading.Lock()
_executor_max_workers: int = 0

_records_lock = threading.Lock()
# delegation_id -> record dict; kept for the run plus a short completed tail.
_records: dict[str, dict[str, Any]] = {}
# A closed session's token -> the (field, value) selectors a new dispatch is refused for (``refuse_for_session``),
# and each selector -> the tokens refusing it, so a dispatch checks its own selectors, not every fence.
_refused: dict[Any, frozenset] = {}
_refused_by: dict[tuple, set] = {}
_FENCED_FIELDS = ("session_key", "parent_session_id")

_DEFAULT_MAX_ASYNC_CHILDREN = 3
# Completed records retained (in memory and in the ledger) for status queries.
_MAX_RETAINED_COMPLETED = 50
_DURABLE_RETENTION_SECONDS = 7 * 24 * 60 * 60
_MAX_DURABLE_PENDING = 1000
# Cap retried deliveries so an unroutable row converges to terminal 'dropped'.
_MAX_DELIVERY_ATTEMPTS = 8
# Pending completions older than this are dropped on restart replay instead of
# re-run as a full-context turn; 48h keeps weekend results deliverable.
_MAX_COMPLETION_REPLAY_AGE_S = 48 * 3600.0
# A delivery claim older than this is abandoned and may be re-claimed.
_CLAIM_LEASE_S = 300.0


_DB_LOCK = threading.Lock()

# ── Orphaned-completion sweep ────────────────────────────────────────────────
# Startup replay runs once per process, so a completion whose owner died while THIS process was
# already running (a desktop reload) would wait for the next restart (#97202). Delivery loops (gateway
# watcher, TUI poller) sweep each home they serve at most once per interval.
ORPHAN_SWEEP_INTERVAL_S = 30.0
# Idle time before a dead owner's pending row is re-offered; keeps the sweep off a row just touched.
_ORPHAN_STALE_S = 60.0
_orphan_lock = threading.Lock()
# (home key, delegation_id) put on this process's queue by replay or sweep and not re-offered while
# that copy is alive. A consumer that discards its copy with the row still pending hands it back
# (``return_completion_offer``); the delivery claim stays the only thing that settles the row.
_offered: set = set()
# Delegation ids this process first offered from its startup replay. Their copies carry
# ``startup_replay`` on the sweep's later re-offers too, so a refused one stays gated.
_startup_replayed: set = set()
_last_orphan_sweep: dict[str, float] = {}

# ── Stale-delegation detection (progress-based, on by default) ──────────────
# A runner wedged before returning never reaches its finalizer, so it would show
# "dispatched" forever. No wall-clock timeout (heavy work must never be killed for
# taking long): one monitor thread samples per-dispatch PROGRESS via an injected
# ``progress_fn``; a frozen child is interrupted, given a grace window to unwind via
# the normal finalize path, and only force-finalized (terminal ``stalled`` event) if
# it never returns. Thresholds mirror delegate_tool's sync heartbeat monitor.
_STALE_CHECK_INTERVAL = 30.0
_STALE_IDLE_SECONDS = 450.0
_STALE_IN_TOOL_SECONDS = 1200.0
_STALL_GRACE_SECONDS = 120.0

_monitor_lock = threading.Lock()
_monitor_thread: Optional[threading.Thread] = None
_monitor_stop = threading.Event()

_LIVE_STATES = {"running", "stalling", "finalizing"}
_ACTIVE_STATES = ("running", "stalling")
# Routing origin persisted at dispatch so a restart-recovered completion can
# reconstruct a full SessionSource (scope_id drives relay tenant egress).
_ROUTING_KEYS = ("scope_id", "user_id", "user_name")
# Structured stall metadata — additive, present only on stall finalizations.
_STALL_META_KEYS = ("stalled_after_quiet_seconds", "stall_threshold_seconds", "stall_phase", "stall_grace_seconds")
# Private stall bookkeeping on the record -> public field in list_async_delegations().
_STALL_FIELD_MAP = (("_stall_quiet_seconds", "stalled_after_quiet_seconds"),
                    ("_stall_threshold_seconds", "stall_threshold_seconds"), ("_stall_in_tool", "stall_in_tool"))


# ── Durable ledger (state.db / async_delegations) ───────────────────────────
def _db_path():
    return get_hermes_home() / "state.db"


def _connect() -> sqlite3.Connection:
    from agent.runtime_session_store import WorkerPersistenceError, is_worker_process
    if is_worker_process():
        raise WorkerPersistenceError('worker_delegation_ledger_unavailable')
    from hermes_cli.sqlite_util import open_db
    # Same state.db as hermes_state.SessionDB -- reuse its owner-only (0600)
    # hardening so this writer doesn't create/leave the file (and its WAL
    # sidecars) at the process umask. See hermes_state._secure_state_db_files.
    from hermes_constants import mkdir_under_hermes_home
    from hermes_state import _secure_state_db_files
    path = _db_path()
    # A late replay or writer must not resurrect a removed named profile (#123265).
    mkdir_under_hermes_home(path.parent)
    _secure_state_db_files(path, create_main=True)
    # wal=False: SessionDB owns state.db's journal mode (_initialize_schema applies the barriers).
    conn = open_db(path, db_label="state.db (async_delegation)", busy_timeout_ms=10_000,
                   wal=False, row_factory=None, initialize=_initialize_schema)
    _secure_state_db_files(path)
    return conn


def _initialize_schema(conn: sqlite3.Connection) -> None:
    from hermes_state_repair import apply_durability_barriers
    from hermes_state_schema import reconcile_state_schema
    # Preserve the journal mode SessionDB configured on state.db: forcing WAL from
    # every short-lived connection collides with live transcript/FTS writers.
    apply_durability_barriers(conn)
    # Single durable-shape authority: the canonical SCHEMA_SQL drives both
    # table creation and column backfill (reconcile_state_schema replays the
    # canonical DDL and reuses SessionDB's declarative reconciliation). This
    # module previously carried its own CREATE TABLE + ALTER column list,
    # which drifted from SCHEMA_SQL — same-name columns with different
    # nullability/defaults depending on which authority touched the database
    # first (#94691).
    reconcile_state_schema(conn)


def _transaction():
    from hermes_cli.sqlite_util import transaction
    return transaction(_connect())


def _capture_routing_origin() -> dict[str, Any]:
    """Snapshot scope_id/user_id/user_name on the PARENT thread (the daemon worker
    has no contextvars) so a restart-replayed completion can rebuild a SessionSource.
    Best-effort: empty values are omitted."""
    try:
        from gateway.session_context import get_session_env
        return {k: v for k in _ROUTING_KEYS if (v := get_session_env(f"HERMES_SESSION_{k.upper()}", ""))}
    except _ROUTINE_EXCEPTIONS:
        return {}


_EXECUTION_STATES = {
    "prepared", "queued", "started", "runner_exited", "unknown",
    "cancelled_before_start", "rejected_before_enqueue",
}
_EXECUTION_TRANSITIONS = {
    "prepared": {"prepared", "queued", "unknown", "rejected_before_enqueue"},
    "queued": {"queued", "started", "unknown", "cancelled_before_start"},
    "started": {"started", "runner_exited", "unknown"},
    "unknown": {"unknown", "started", "runner_exited", "cancelled_before_start", "rejected_before_enqueue"},
    "runner_exited": {"runner_exited"},
    "cancelled_before_start": {"cancelled_before_start"},
    "rejected_before_enqueue": {"rejected_before_enqueue"},
}


def _execution_claim(record: dict[str, Any]) -> dict[str, Any]:
    claim = record.get("execution")
    if not isinstance(claim, dict) or claim.get("handoff_id") != record.get("delegation_id"):
        raise RuntimeError("async delegation execution claim is not authenticated")
    state = claim.get("state")
    if state not in _EXECUTION_STATES or claim.get("accepted") not in {"yes", "no", "unknown"}:
        raise RuntimeError("async delegation execution state is invalid")
    if type(claim.get("worker_started")) is not bool or type(claim.get("worker_exited")) is not bool:
        raise RuntimeError("async delegation execution flags are invalid")
    if type(claim.get("owner_pid")) is not int or claim["owner_pid"] <= 0:
        raise RuntimeError("async delegation execution owner is invalid")
    if not isinstance(claim.get("owner_execution_id"), str) or not claim["owner_execution_id"]:
        raise RuntimeError("async delegation execution owner identity is invalid")
    started = claim.get("owner_started_at")
    if started is not None and (type(started) is not int or started < 0):
        raise RuntimeError("async delegation execution birth is invalid")
    scope = claim.get("owner_scope")
    if scope is not None:
        invalid = (not isinstance(scope, dict) or set(scope) != {"profile_id", "session_id", "generation", "epoch"}
                   or not isinstance(scope.get("profile_id"), str) or not scope["profile_id"]
                   or not isinstance(scope.get("session_id"), str) or not scope["session_id"]
                   or type(scope.get("generation")) is not int or type(scope.get("epoch")) is not int
                   or scope["generation"] < 0 or scope["epoch"] < 1)
        if invalid: raise RuntimeError("async delegation execution owner scope is invalid")
    if state == "prepared" and (claim["accepted"] != "unknown" or claim["worker_started"] or claim["worker_exited"]):
        raise RuntimeError("prepared async execution claim is inconsistent")
    if state == "queued" and (claim["accepted"] != "yes" or claim["worker_started"] or claim["worker_exited"]):
        raise RuntimeError("queued async execution claim is inconsistent")
    if state == "started" and (claim["accepted"] != "yes" or not claim["worker_started"] or claim["worker_exited"]):
        raise RuntimeError("started async execution claim is inconsistent")
    if state == "runner_exited" and (claim["accepted"] != "yes" or not claim["worker_started"] or not claim["worker_exited"]):
        raise RuntimeError("exited async execution claim is inconsistent")
    if state == "cancelled_before_start" and (claim["accepted"] != "yes" or claim["worker_started"] or not claim["worker_exited"]):
        raise RuntimeError("cancelled async execution claim is inconsistent")
    if state == "rejected_before_enqueue" and (claim["accepted"] != "no" or claim["worker_started"] or not claim["worker_exited"]):
        raise RuntimeError("rejected async execution claim is inconsistent")
    for key in ("error", "exception"):
        if claim.get(key) is not None:
            if not isinstance(claim[key], dict) or len(json.dumps(claim[key], separators=(",", ":"), allow_nan=False)) > 2048:
                raise RuntimeError("async execution error metadata is invalid")
    return dict(claim)


def _execution_update(record: dict[str, Any], state: str, **fields) -> dict[str, Any]:
    """Write a monotonic typed execution claim; a failed write leaves custody unknown."""
    with _records_lock:
        current = _records.get(record["delegation_id"], record)
        claim = _execution_claim(current)
        if state not in _EXECUTION_TRANSITIONS.get(claim.get("state"), set()):
            raise RuntimeError(f"stale async execution transition: {claim.get('state')} -> {state}")
        if ((claim.get("worker_started") and fields.get("worker_started") is False)
                or (claim.get("worker_exited") and fields.get("worker_exited") is False)):
            raise RuntimeError("async execution claim would regress a worker phase")
        claim.update(fields, state=state)
        current["execution"] = claim
    try:
        if remote := worker_ledger():
            remote.execution(record["delegation_id"], claim)
        else:
            with _DB_LOCK, _transaction() as conn:
                changed = conn.execute(
                    "UPDATE async_delegations SET execution_json=?,updated_at=? WHERE delegation_id=?",
                    (json.dumps(claim, sort_keys=True), time.time(), record["delegation_id"])).rowcount
            if changed != 1:
                raise RuntimeError(f"async delegation {record['delegation_id']} execution row is missing")
    except _CUSTODY_EXCEPTIONS:
        with _records_lock:
            current = _records.get(record["delegation_id"], record)
            fallback = _execution_claim(current)
            fallback.update(error={"kind": "execution_readback_unknown"}, state="unknown")
            current["execution"] = fallback
        raise
    return claim


def _persist_dispatch(record: dict[str, Any]) -> None:
    if remote := worker_ledger():
        return remote.dispatch(record)
    now = time.time()
    try:
        from gateway.status import get_process_start_time
        owner_started_at = get_process_start_time(os.getpid())
    except _ROUTINE_EXCEPTIONS:
        owner_started_at = None
    if type(owner_started_at) is not int:
        raise RuntimeError("owner_birth_unavailable")
    with _records_lock:
        claim = _execution_claim(record)
        if (record.get("_owner_scope_required")
                and normalize_owner_scope(claim.get("owner_scope")) is None):
            raise RuntimeError("owner_scope_unavailable")
        claim["owner_started_at"] = owner_started_at
        record["execution"] = claim
    task_payload = {
        key: record.get(key)
        for key in ("goal", "goals", "context", "toolsets", "role", "model", "is_batch", "task_indexes", "task_transcripts", *_ROUTING_KEYS)
        if key in record}
    try:  # where the children's terminals started; lets recovery add a git-state hint
        task_payload["owner_cwd"] = os.getcwd()
    except OSError:
        pass
    with _DB_LOCK, _transaction() as conn:
        conn.execute("""INSERT OR REPLACE INTO async_delegations
               (delegation_id, origin_session, origin_ui_session_id,
                parent_session_id, state, dispatched_at, updated_at,
                delivery_state, delivery_attempts, owner_pid,
                owner_started_at, task_json, origin_session_id, execution_json)
               VALUES (?, ?, ?, ?, 'running', ?, ?, 'pending', 0, ?, ?, ?, ?, ?)""",
            (record["delegation_id"], record.get("session_key", ""), record.get("origin_ui_session_id", ""),
             record.get("parent_session_id"), record["dispatched_at"], now, os.getpid(), owner_started_at,
             json.dumps(task_payload), record.get("origin_session_id", ""),
             json.dumps(_execution_claim(record), sort_keys=True)))
    _prune_durable_records()


def _read_admission_row(delegation_id: str) -> tuple[bool, Optional[dict[str, Any]]]:
    """Read back a pre-submit admission without treating an I/O error as absence."""
    try:
        if remote := worker_ledger():
            return True, remote.read(delegation_id)
        with _DB_LOCK, _transaction() as conn:
            row = conn.execute("""SELECT delegation_id, origin_session, origin_ui_session_id,
                          parent_session_id, state, dispatched_at, origin_session_id, execution_json
                   FROM async_delegations WHERE delegation_id=?""", (delegation_id,)).fetchone()
        if row is None:
            return True, None
        return True, {
            "delegation_id": row[0], "origin_session": row[1],
            "origin_ui_session_id": row[2], "parent_session_id": row[3],
            "state": row[4], "dispatched_at": row[5], "origin_session_id": row[6],
            "execution": json.loads(row[7]) if row[7] else None,
        }
    except _ROUTINE_EXCEPTIONS:
        logger.warning("Async delegation %s admission readback failed", delegation_id, exc_info=True)
        return False, None


def _admission_row_matches(record: dict[str, Any], saved: Optional[dict[str, Any]]) -> bool:
    """Accept only this dispatch's authenticated, still-live admission row.

    ``owner_fence_matches`` rejects absent scope; a missing authority proof is
    UNKNOWN and keeps the durable row and retirement lease for reconciliation.
    """
    if not isinstance(saved, dict) or saved.get("delegation_id") != record.get("delegation_id"):
        return False
    for saved_key, record_key in (("origin_session", "session_key"),
                                  ("origin_ui_session_id", "origin_ui_session_id"),
                                  ("parent_session_id", "parent_session_id"),
                                  ("origin_session_id", "origin_session_id")):
        if saved_key in saved and (saved.get(saved_key) or "") != (record.get(record_key) or ""):
            return False
    if "dispatched_at" in saved and saved.get("dispatched_at") != record.get("dispatched_at"):
        return False
    saved_execution = saved.get("execution")
    expected_execution = record.get("execution")
    if not owner_fence_matches(
            expected_execution, saved_execution,
            require_scope=bool(record.get("_owner_scope_required"))):
        return False
    return saved.get("state") in {"running", "finalizing"}


def _reject_before_enqueue(record: dict[str, Any], error: BaseException | str) -> bool:
    """Close a proven no-submit admission; unknown evidence keeps exact custody."""
    with _records_lock:
        current = _records.get(record["delegation_id"], record)
        state = (current.get("execution") or {}).get("state")
    if state == "rejected_before_enqueue": return True
    if state not in {"prepared", "unknown"}: return False
    metadata = ({"kind": type(error).__name__, "message": str(error)[:240]} if isinstance(error, BaseException)
                else {"kind": "admission_reconcile", "message": str(error)[:240]})
    try:
        _execution_update(record, "rejected_before_enqueue", accepted="no", worker_exited=True, error=metadata)
    except _CUSTODY_EXCEPTIONS:
        with _records_lock:
            current = _records.get(record["delegation_id"], record)
            claim = _execution_claim(current)
            claim.update(state="unknown", accepted="unknown", worker_exited=False, error={"kind": "admission_reconcile_unknown"})
            current["execution"] = claim
        return False
    return True


def _unschedule_dispatch(delegation_id: str) -> None:
    """Remove a dispatch row only after the executor proved no Future was accepted.
    A returned Future is the handoff boundary. Callers must never use this helper after
    that boundary: a worker may already be running, queued, or cancelled and its durable
    row is then the recovery record.
    """
    if remote := worker_ledger():
        remote.unscheduled(delegation_id)
        return
    with _DB_LOCK, _transaction() as conn:
        changed = conn.execute(
            "DELETE FROM async_delegations WHERE delegation_id=?", (delegation_id,)
        ).rowcount
    if changed != 1:
        raise RuntimeError(f"async delegation {delegation_id} admission row was not unscheduled")


def _prune_durable_records() -> None:
    """Retire only exact delivered terminal rows; accepted custody is never cap-evicted."""
    require_ledger_owner()
    cutoff = time.time() - _DURABLE_RETENTION_SECONDS
    with _records_lock:
        protected_ids = tuple(
            delegation_id for delegation_id, record in _records.items()
            if record.get("_settlement_pending")
        )
    with _DB_LOCK, _transaction() as conn:
        for delegation_id in prunable_ids(
            conn, cutoff, _MAX_RETAINED_COMPLETED, protected_ids
        ):
            conn.execute("DELETE FROM async_delegations WHERE delegation_id=?", (delegation_id,))


def _persist_completion(event: dict[str, Any], result: dict[str, Any]) -> None:
    event, result = redact_completion_event(event), redact_child_result(result)
    if remote := worker_ledger():
        return remote.complete(event, result)
    now = time.time()
    with _DB_LOCK, _transaction() as conn:
        changed = conn.execute("""UPDATE async_delegations SET state=?, completed_at=?, updated_at=?,
               event_json=?, result_json=?, delivery_state='pending'
               WHERE delegation_id=?""",
            (event.get("status", "completed"), event.get("completed_at", now), now,
             json.dumps(event), json.dumps(result), event["delegation_id"])).rowcount
        if changed != 1:
            raise RuntimeError(f"async delegation {event['delegation_id']} completion row is missing")


def record_unit_child(delegation_id: str, entry: dict[str, Any]) -> None:
    """Durably record ONE finished child of a still-running multi-child unit on the unit's own row, so a crash before
    the unit joins loses only the children that had not finished. Stored in ``result_json`` (overwritten by the real
    result at finalize); ``recover_abandoned_delegations`` replays it. Best-effort: a failed write costs recovery
    fidelity, never the live result."""
    entry = redact_child_result(entry)
    if remote := worker_ledger():
        return remote.child(delegation_id, entry)
    try:
        with _DB_LOCK, _transaction() as conn:
            row = conn.execute("SELECT result_json FROM async_delegations WHERE delegation_id=? AND state='running'",
                               (delegation_id,)).fetchone()
            if row is None:
                return
            partial = redact_child_result(json.loads(row[0] or "{}") or {})
            results = [r for r in partial.get("results") or [] if r.get("task_index") != entry.get("task_index")]
            results.append(entry)
            conn.execute("UPDATE async_delegations SET result_json=?, updated_at=? WHERE delegation_id=? AND state='running'",
                         (json.dumps({"results": results, "partial": True}), time.time(), delegation_id))
    except _ROUTINE_EXCEPTIONS:
        logger.warning("Async delegation %s: could not record finished child %s", delegation_id, entry.get("task_index"), exc_info=True)


def recover_abandoned_delegations() -> int:
    """Classify records whose owning process disappeared as outcome unknown; children a multi-child unit had already
    recorded (``record_unit_child``) are replayed with their real results."""
    require_ledger_owner()
    alive = _owner_liveness()
    if alive is None:
        return 0
    now, recovered = time.time(), 0
    with _DB_LOCK, _transaction() as conn:
        rows = conn.execute("""SELECT delegation_id, origin_session, origin_ui_session_id,
                      parent_session_id, dispatched_at, owner_pid,
                      owner_started_at, task_json, origin_session_id, result_json, state, execution_json
               FROM async_delegations WHERE state IN ('running','finalizing')""").fetchall()
        for row in rows:
            delegation_id, session_key, origin_ui, parent_id, dispatched_at, pid, started, task_json, origin_sid, result_json, last_state, execution_json = row
            owner_alive = alive(pid, started)
            if owner_alive is not False:
                continue
            execution = json.loads(execution_json or "{}") or {}
            task = json.loads(task_json or "{}")
            error = "Delegation owner exited before recording a terminal result; outcome unknown."
            if execution.get("state") in {"queued", "started", "unknown"}:
                error = "Delegation owner exited with an unsettled executor handoff; outcome unknown."
            recovered_results = _recovered_results(task, result_json, error)
            if recovered_results:
                done = sum(1 for r in recovered_results if r.get("status") != "unknown")
                error = (f"Delegation owner exited before the unit finished; {done}/{len(recovered_results)} child "
                         "results were recorded and are included below, the rest are unknown.")
            diagnostics = {"last_known_status": last_state, "task_transcripts": task.get("task_transcripts") or {}}
            # Verbatim transcript tails and a git snapshot let the parent re-dispatch from the event (#116000).
            from tools.async_delegation_recovery_hints import git_state_hint, transcript_tails
            if tails := transcript_tails(diagnostics["task_transcripts"]):
                diagnostics["transcript_tails"] = tails
            if hint := git_state_hint(task.get("owner_cwd")):
                diagnostics["git_state_hint"] = hint
            event = {
                "type": "async_delegation", "delegation_id": delegation_id, "session_key": session_key,
                "origin_ui_session_id": origin_ui, "origin_session_id": origin_sid or "",
                "parent_session_id": parent_id, "goal": task.get("goal", ""), "goals": task.get("goals"),
                "context": task.get("context"), "toolsets": task.get("toolsets"), "role": task.get("role"),
                "model": task.get("model"), "is_batch": bool(task.get("is_batch")),
                "status": "unknown", "summary": None, "error": error, **diagnostics,
                **({"results": recovered_results} if recovered_results else {}),
                "dispatched_at": dispatched_at, "completed_at": now,
                **{k: task[k] for k in _ROUTING_KEYS if task.get(k)}}
            result = {"status": "unknown", "summary": None, "error": event["error"], **diagnostics,
                      **({"results": recovered_results} if recovered_results else {})}
            event, result = redact_completion_event(event), redact_child_result(result)
            execution.update(
                state="unknown", accepted=execution.get("accepted", "unknown"),
                worker_exited=bool(execution.get("worker_exited")),
                error={"kind": "owner_exit_before_settlement"})
            conn.execute("""UPDATE async_delegations SET state='unknown', completed_at=?,
                   updated_at=?, event_json=?, result_json=?, execution_json=?, delivery_state='pending'
                   WHERE delegation_id=?""", (now, now, json.dumps(event), json.dumps(result),
                                                json.dumps(execution, sort_keys=True), delegation_id))
            recovered += 1
    return recovered


def restore_undelivered_completions(target_queue) -> int:
    """Enqueue durable pending completions as fresh turns after process start.
    Restored events are stamped ``restored=True`` in memory only: they came from a PREVIOUS
    process, so drains without an ownership filter must leave them for a consumer that can
    prove ownership. Rows older than ``_MAX_COMPLETION_REPLAY_AGE_S`` are terminally dropped
    instead of replaying a turn nobody is waiting on.
    Every restored event is stamped ``restored=True`` (in-memory only — the stamp is added after the durable
    payload is deserialized and is never persisted). Restored events originate from a *previous* process, so
    no consumer in THIS process implicitly owns them: drain paths that run without an ownership filter (the
    legacy single-session behavior) must leave them queued for a consumer that can positively prove
    ownership, otherwise a brand-new session adopts a dead session's delegation results seconds after boot
    (#64484).
    """
    from agent.runtime_session_store import is_worker_process
    if is_worker_process():
        # The ordinary owner restores delivery-only results; compute imports never reap or steal them.
        return 0
    if not _db_path().exists():
        return 0  # nothing to replay; a replay must not create (or migrate) the ledger (#123265)
    recover_abandoned_delegations()
    now = time.time()
    with _DB_LOCK, _transaction() as conn:
        rows = conn.execute("""SELECT delegation_id, event_json, completed_at, dispatched_at
               FROM async_delegations
               WHERE state != 'running' AND delivery_state='pending' AND event_json IS NOT NULL
               ORDER BY completed_at, delegation_id""").fetchall()
        return _replay_pending(conn, rows, target_queue, now, startup=True)


def _replay_pending(conn, rows, target_queue, now: float, *, startup: bool = False) -> int:
    """Put each pending ``(delegation_id, event_json, completed_at, dispatched_at)`` row on ``target_queue``
    stamped ``restored``, or terminally drop it past ``_MAX_COMPLETION_REPLAY_AGE_S``. Records the offer so
    the orphan sweep skips the row until the copy is handed back (``return_completion_offer``)."""
    home, restored = hermes_home_key(get_hermes_home()), 0
    for delegation_id, payload, completed_at, dispatched_at in rows:
        age_basis = completed_at or dispatched_at
        if age_basis and (now - age_basis) > _MAX_COMPLETION_REPLAY_AGE_S:
            conn.execute("""UPDATE async_delegations SET delivery_state='dropped',
                          delivery_claim=NULL, delivery_claimed_at=NULL,
                          updated_at=?
                   WHERE delegation_id=? AND delivery_state='pending'""", (now, delegation_id))
            logger.warning("Async delegation %s: pending completion is %.1fh old "
                           "(cap %.1fh); terminally dropping the replay (result remains queryable).",
                           delegation_id, (now - age_basis) / 3600.0, _MAX_COMPLETION_REPLAY_AGE_S / 3600.0)
            continue
        with _orphan_lock:
            if (home, delegation_id) in _offered:
                # Startup and first-consume replay may both run; offer once.
                continue
        evt = json.loads(payload)
        if isinstance(evt, dict):
            evt = redact_completion_event(evt)
        with _orphan_lock:
            if startup:
                _startup_replayed.add(delegation_id)
            if isinstance(evt, dict):
                evt["restored"] = True
                if delegation_id in _startup_replayed:
                    evt["startup_replay"] = True
        target_queue.put(evt)
        with _orphan_lock:
            _offered.add((home, delegation_id))
        restored += 1
    return restored


def sweep_orphaned_completions(target_queue, *, now: Optional[float] = None) -> int:
    """Offer this home's completions whose owner died after THIS process started (#97202).
    Startup replay (``restore_undelivered_completions``) covers owners that died before the process
    started; this covers the rest while it runs. Abandoned in-flight rows are first classified by
    ``recover_abandoned_delegations``. A terminal row qualifies when it is pending with an event, idle
    past ``_ORPHAN_STALE_S``, not under a live delivery claim, and its owner fails the shared liveness
    check. A row is offered once per live in-memory copy: a consumer that discards the copy with the row
    still pending hands it back for the next sweep. The consumer's ``claim_completion_delivery`` stays
    the atomic cross-process gate, so two processes offering one row never both deliver it. Rows past
    the delivery budget or the replay age converge to ``dropped``. Reads the current profile's ledger:
    callers bind the owning profile first."""
    alive = _owner_liveness()
    if alive is None or not _db_path().exists():
        return 0  # never create a ledger just to sweep it
    recover_abandoned_delegations()
    now = time.time() if now is None else now
    home = hermes_home_key(get_hermes_home())
    with _orphan_lock:
        offered = {delegation_id for key, delegation_id in _offered if key == home}
    with _DB_LOCK, _transaction() as conn:
        rows = conn.execute("""SELECT delegation_id, event_json, completed_at, dispatched_at,
                      owner_pid, owner_started_at, delivery_attempts
               FROM async_delegations
               WHERE state NOT IN ('running','finalizing') AND delivery_state='pending'
                 AND event_json IS NOT NULL AND updated_at < ?
                 AND (delivery_claim IS NULL OR delivery_claimed_at < ?)
               ORDER BY completed_at, delegation_id""", (now - _ORPHAN_STALE_S, now - _CLAIM_LEASE_S)).fetchall()
        orphans = []
        for delegation_id, payload, completed_at, dispatched_at, pid, started, attempts in rows:
            if delegation_id in offered or alive(pid, started) is not False:
                continue
            if (attempts or 0) >= _MAX_DELIVERY_ATTEMPTS:
                # Its last claimant died holding the final attempt; converge like release_completion_delivery.
                conn.execute("""UPDATE async_delegations SET delivery_state='dropped',
                              delivery_claim=NULL, delivery_claimed_at=NULL, updated_at=?
                       WHERE delegation_id=? AND delivery_state='pending'""", (now, delegation_id))
                logger.warning("Async delegation %s exhausted its %d delivery attempts; "
                               "marking terminally dropped (result remains queryable).",
                               delegation_id, _MAX_DELIVERY_ATTEMPTS)
                continue
            orphans.append((delegation_id, payload, completed_at, dispatched_at))
        return _replay_pending(conn, orphans, target_queue, now)


def maybe_sweep_orphaned_completions(target_queue, *, now: Optional[float] = None) -> int:
    """``sweep_orphaned_completions`` at most once per ``ORPHAN_SWEEP_INTERVAL_S`` per home (``now`` is
    monotonic), for delivery loops that tick far more often. Never raises into the loop."""
    home = hermes_home_key(get_hermes_home())
    now = time.monotonic() if now is None else now
    with _orphan_lock:
        last = _last_orphan_sweep.get(home)
        if last is not None and now - last < ORPHAN_SWEEP_INTERVAL_S:
            return 0
        _last_orphan_sweep[home] = now
    try:
        return sweep_orphaned_completions(target_queue)
    except _ROUTINE_EXCEPTIONS:
        logger.debug("Orphaned async delegation sweep failed", exc_info=True)
        return 0


def _update_delivery(sql: str, params: tuple) -> bool:
    """Run one UPDATE on the ledger; True iff exactly one row changed."""
    with _DB_LOCK, _transaction() as conn:
        return conn.execute(sql, params).rowcount == 1


def mark_completion_delivered(delegation_id: str) -> bool:
    """Atomically acknowledge successful injection of a durable completion."""
    require_ledger_owner()
    now = time.time()
    return _update_delivery(
        """UPDATE async_delegations SET delivery_state='delivered', delivered_at=?, updated_at=?
           WHERE delegation_id=? AND delivery_state!='delivered'""", (now, now, delegation_id))


def claim_completion_delivery(delegation_id: str, claim_id: str) -> bool:
    """Claim one pending completion across competing consumers/processes."""
    if remote := worker_ledger():
        return remote.claim(delegation_id, claim_id)
    now = time.time()
    with _DB_LOCK, _transaction() as conn:
        row = conn.execute(
            "SELECT delivery_state FROM async_delegations WHERE delegation_id=?", (delegation_id,)).fetchone()
        if row is None:
            return True  # legacy event created before durable dispatch
        cur = conn.execute("""UPDATE async_delegations SET delivery_claim=?, delivery_claimed_at=?,
                      delivery_attempts=delivery_attempts+1, updated_at=?
               WHERE delegation_id=? AND delivery_state='pending'
                 AND (delivery_claim IS NULL OR delivery_claimed_at < ?)""",
            (claim_id, now, now, delegation_id, now - _CLAIM_LEASE_S))
        return cur.rowcount == 1


def is_interim_delegation_event(evt: dict[str, Any]) -> bool:
    """An early per-task notice for a batch that is still running. It shares the batch's
    ``delegation_id`` but is NOT the durable completion: it must never claim, acknowledge or
    dedup against the final result's row (independent review reproduced exactly that loss)."""
    return evt.get("type") == "async_delegation" and bool(evt.get("task_failure_notice"))


def claim_event_delivery(evt: dict[str, Any], consumer: str) -> Optional[str]:
    """Claim a durable delegation event; non-durable events (and interim notices) need no token."""
    if is_interim_delegation_event(evt):
        return ""
    delegation_id = str(evt.get("delegation_id") or "") if evt.get("type") == "async_delegation" else ""
    if not delegation_id:
        return ""
    claim_id = f"{consumer}:{os.getpid()}:{uuid.uuid4().hex}"
    return claim_id if claim_completion_delivery(delegation_id, claim_id) else None


def release_completion_delivery(delegation_id: str, claim_id: str) -> bool:
    """Release a failed delivery claim so another consumer may retry. Attempts are
    counted at claim time; once the budget is exhausted the row converges to
    terminal ``dropped`` (only pending rows replay on restart)."""
    if remote := worker_ledger():
        return remote.release(delegation_id, claim_id)
    now = time.time()
    with _DB_LOCK, _transaction() as conn:
        capped = conn.execute("""UPDATE async_delegations SET delivery_state='dropped',
                      delivery_claim=NULL, delivery_claimed_at=NULL, updated_at=?
               WHERE delegation_id=? AND delivery_state='pending'
                 AND delivery_claim=? AND delivery_attempts>=?""",
            (now, delegation_id, claim_id, _MAX_DELIVERY_ATTEMPTS))
        if capped.rowcount == 1:
            logger.warning("Async delegation %s exhausted its %d delivery attempts; "
                           "marking terminally dropped (result remains queryable).",
                           delegation_id, _MAX_DELIVERY_ATTEMPTS)
            return True
        cur = conn.execute("""UPDATE async_delegations SET delivery_claim=NULL,
                      delivery_claimed_at=NULL, updated_at=?
               WHERE delegation_id=? AND delivery_state='pending'
                 AND delivery_claim=?""", (now, delegation_id, claim_id))
        return cur.rowcount == 1


def defer_completion_delivery(delegation_id: str, claim_id: str) -> bool:
    """Return an unadmitted completion to pending without spending a delivery attempt."""
    if remote := worker_ledger():
        return remote.defer(delegation_id, claim_id)
    return _update_delivery("""UPDATE async_delegations SET delivery_claim=NULL,
                  delivery_claimed_at=NULL, delivery_attempts=MAX(0, delivery_attempts-1),
                  updated_at=?
           WHERE delegation_id=? AND delivery_state='pending' AND delivery_claim=?""",
        (time.time(), delegation_id, claim_id))


def drop_completion_delivery(delegation_id: str, claim_id: str) -> bool:
    """Terminally drop a claimed completion whose target is permanently gone (the
    spawning session ended at an explicit user boundary such as /new or reset).
    ``dropped`` — not ``delivered`` — keeps the ack honest; not ``pending`` keeps
    restart recovery from replaying it into a fail-closed drop forever."""
    if remote := worker_ledger():
        return remote.drop(delegation_id, claim_id)
    return _update_delivery("""UPDATE async_delegations SET delivery_state='dropped',
                  updated_at=?, delivery_claim=NULL,
                  delivery_claimed_at=NULL
           WHERE delegation_id=? AND delivery_state='pending'
             AND delivery_claim=?""", (time.time(), delegation_id, claim_id))


def complete_completion_delivery(delegation_id: str, claim_id: str) -> bool:
    """Acknowledge acceptance for the consumer holding this claim."""
    if remote := worker_ledger():
        return remote.ack(delegation_id, claim_id)
    now = time.time()
    return _update_delivery("""UPDATE async_delegations SET delivery_state='delivered',
                  delivered_at=?, updated_at=?, delivery_claim=NULL,
                  delivery_claimed_at=NULL
           WHERE delegation_id=? AND delivery_state='pending'
             AND delivery_claim=?""", (now, now, delegation_id, claim_id))


def requeue_admitted_completion(delegation_id: str, target_queue) -> bool:
    """Offer an acknowledged completion again when its receiving turn could not confirm the owner.
    The row returns to pending and its event goes back on ``target_queue``, so the next delivery
    re-runs the pre-flight. A row past the delivery budget converges to ``dropped`` instead, which
    bounds the loop; a row without a stored event is left as it is."""
    now = time.time()
    with _DB_LOCK, _transaction() as conn:
        exhausted = conn.execute("""UPDATE async_delegations SET delivery_state='dropped', updated_at=?
               WHERE delegation_id=? AND delivery_state='delivered' AND delivery_attempts>=?""",
            (now, delegation_id, _MAX_DELIVERY_ATTEMPTS))
        if exhausted.rowcount == 1:
            logger.warning("Async delegation %s exhausted its %d delivery attempts; "
                           "marking terminally dropped (result remains queryable).",
                           delegation_id, _MAX_DELIVERY_ATTEMPTS)
            return False
        reopened = conn.execute("""UPDATE async_delegations SET delivery_state='pending',
                      delivered_at=NULL, updated_at=?
               WHERE delegation_id=? AND delivery_state='delivered' AND event_json IS NOT NULL""",
            (now, delegation_id))
        if reopened.rowcount != 1:
            return False
        rows = conn.execute("""SELECT delegation_id, event_json, completed_at, dispatched_at
               FROM async_delegations WHERE delegation_id=?""", (delegation_id,)).fetchall()
        with _orphan_lock:  # an admitted copy already had its consumer: requeue keeps live routing
            _startup_replayed.discard(delegation_id)
        return _replay_pending(conn, rows, target_queue, now) == 1


def complete_event_delivery(evt: dict[str, Any], claim_id: str) -> None:
    _event_delivery(complete_completion_delivery, evt, claim_id)


def release_event_delivery(evt: dict[str, Any], claim_id: str) -> None:
    """Release a failed claim for a consumer that discards its copy (the TUI poller): the row is pending
    again, so it must stay eligible for the orphan sweep."""
    _event_delivery(release_completion_delivery, evt, claim_id)
    return_completion_offer(evt)


def return_completion_offer(evt: dict[str, Any]) -> None:
    """Hand an offered completion back to the orphan sweep after its in-memory copy was discarded while
    the durable row stays pending, e.g. a TUI session that cannot prove it owns the event drops it (every
    session poller drains one process-wide queue). The next sweep may offer the row again. Delegation ids
    are unique across profiles, so this clears the offer in every home."""
    delegation_id = str(evt.get("delegation_id") or "") if evt.get("type") == "async_delegation" else ""
    if not delegation_id or is_interim_delegation_event(evt):
        return
    with _orphan_lock:
        _offered.difference_update({key for key in _offered if key[1] == delegation_id})


def _event_delivery(fn, evt: dict[str, Any], claim_id: str) -> None:
    if claim_id and evt.get("type") == "async_delegation":
        fn(str(evt.get("delegation_id") or ""), claim_id)


def get_durable_delegation(delegation_id: str) -> Optional[dict[str, Any]]:
    if remote := worker_ledger():
        saved = remote.read(delegation_id)
        if saved is not None and saved.get("result") is not None:
            return {**saved, "result": redact_child_result(saved["result"])}
        return saved
    with _DB_LOCK, _transaction() as conn:
        row = conn.execute("""SELECT origin_session, state, dispatched_at, completed_at,
                      result_json, delivery_state, delivery_attempts,
                      origin_session_id
               FROM async_delegations WHERE delegation_id=?""", (delegation_id,)).fetchone()
    return None if row is None else {
        "delegation_id": delegation_id, "origin_session": row[0], "state": row[1], "dispatched_at": row[2],
        "completed_at": row[3], "result": redact_child_result(json.loads(row[4])) if row[4] else None, "delivery_state": row[5],
        "delivery_attempts": row[6], "origin_session_id": row[7] or ""}


_FAILED_TASK_STATES = frozenset({"error", "failed", "failure", "timeout", "stalled", "unknown", "interrupted"})
_FAILURE_SURFACE_WINDOW_S = 24 * 3600.0


def _json_object(raw: Optional[str]) -> dict[str, Any]:
    try:
        value = json.loads(raw) if raw else {}
    except ValueError:
        return {}
    return value if isinstance(value, dict) else {}


def failed_delegations_for_session(
    origin_ui_session_id: str = "", parent_session_id: str = "", *, limit: int = 20, now: Optional[float] = None,
) -> list[dict[str, Any]]:
    """Recently failed async delegation tasks owned by a session, newest first.
    The live roster forgets a child once it ends and does not survive a renderer reload, so a failed
    delegation had nowhere to show (#97202). This reads the durable row instead: one entry per failed
    task (a batch unit that "completed" can still carry failed tasks) with ``delegation_id``,
    ``task_index``, ``goal``, ``status``, ``error``, ``dispatched_at`` and ``completed_at``. Either selector claims a row:
    the UI session id at dispatch, or the spawner's durable session id (survives a reload re-mint)."""
    selectors = [(col, val) for col, val in (
        ("origin_ui_session_id", origin_ui_session_id), ("parent_session_id", parent_session_id)) if val]
    if not selectors:
        return []
    cutoff = (now if now is not None else time.time()) - _FAILURE_SURFACE_WINDOW_S
    owner_sql = " OR ".join(f"{col}=?" for col, _ in selectors)
    with _DB_LOCK, _transaction() as conn:
        rows = conn.execute(
            f"""SELECT delegation_id, state, dispatched_at, completed_at, task_json, result_json FROM async_delegations
                WHERE ({owner_sql}) AND state NOT IN ('running','finalizing') AND completed_at >= ?
                ORDER BY completed_at DESC LIMIT ?""",
            (*(val for _, val in selectors), cutoff, limit)).fetchall()
    failed: list[dict[str, Any]] = []
    for delegation_id, state, dispatched_at, completed_at, task_json, result_json in rows:
        task, result = _json_object(task_json), redact_child_result(_json_object(result_json))
        goals = task.get("goals") if isinstance(task.get("goals"), list) and task["goals"] else [task.get("goal") or ""]
        goal_for = dict(zip(task.get("task_indexes") or range(len(goals)), goals))
        tasks = result["results"] if isinstance(result.get("results"), list) else [] if task.get("is_batch") else [result]
        if not tasks and str(state).lower() in _FAILED_TASK_STATES:
            tasks = [{"task_index": 0, "error": result.get("error")}]
        for entry in tasks:
            status = str(entry.get("status") or state or "").lower()
            if status not in _FAILED_TASK_STATES:
                continue
            index = entry.get("task_index") if isinstance(entry.get("task_index"), int) else 0
            error = entry.get("error") or result.get("error")
            failed.append({
                "delegation_id": delegation_id, "task_index": index, "status": status,
                "goal": str(goal_for.get(index, goals[0]) or ""), "error": str(error) if error else None,
                "dispatched_at": dispatched_at, "completed_at": completed_at})
    return failed[:limit]


# ── In-memory registry queries ──────────────────────────────────────────────
def _get_executor(max_workers: int) -> ThreadPoolExecutor:
    """Lazily create (or grow in place, never shrink) the shared daemon executor. Raising
    ``_max_workers`` is enough: the next ``submit`` spawns threads up to the new cap."""
    global _executor, _executor_max_workers
    with _executor_lock:
        if _executor is None:
            _executor = DaemonThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="async-delegate")
            _executor_max_workers = max_workers
        elif max_workers > _executor_max_workers:
            _executor._max_workers = max_workers
            _executor_max_workers = max_workers
        return _executor


def active_count() -> int:
    """Number of live async delegation UNITS (one per completion message: a task group or an ungrouped task)."""
    with _records_lock:
        return sum(1 for r in _records.values() if r.get("status") in _LIVE_STATES)


def active_task_count() -> int:
    """Number of running child subagents (a batch of N contributes N; a batch with
    no goal list counts 1) — the truthful observability figure, unlike slots."""
    with _records_lock:
        return sum(
            len(r.get("task_indexes") or r["goals"])
            if r.get("is_batch") and isinstance(r.get("goals"), (list, tuple)) and r["goals"] else 1
            for r in _records.values() if r.get("status") in {"running", "finalizing"})


def _session_records(statuses, session_key: str, origin_ui_session_id: str, parent_session_id: str) -> list:
    """Records in ``statuses`` owned by a session: any non-empty selector claims the
    record — ``origin_ui_session_id`` (TUI tab), ``session_key`` (routing key at
    dispatch), or ``parent_session_id`` (spawner's durable id — the right one for
    gateway chats, whose session_key survives ``/new`` while the session id rotates)."""
    selectors = [(field, wanted) for field, wanted in (
        ("origin_ui_session_id", origin_ui_session_id), ("session_key", session_key),
        ("parent_session_id", parent_session_id)) if wanted]
    if not selectors:
        return []
    with _records_lock:
        return [r for r in _records.values() if r.get("status") in statuses
                and any(str(r.get(field) or "") == wanted for field, wanted in selectors)]


def has_live_for_session(session_key: str = "", origin_ui_session_id: str = "", parent_session_id: str = "") -> bool:
    """Whether a session still owns any live (running/stalling/finalizing) delegation."""
    return bool(_session_records(_LIVE_STATES, session_key, origin_ui_session_id, parent_session_id))


def _new_delegation_id() -> str:
    return f"deleg_{uuid.uuid4().hex[:8]}"


def _prune_completed_locked() -> None:
    """Drop the oldest completed records beyond the cap. Caller holds ``_records_lock``.
    ``stalling``/``finalizing`` are still live: evicting one makes the late runner return hit
    ``_finalize``'s missing-record path and silently drop a real result."""
    completed = [(rid, r) for rid, r in _records.items()
                 if r.get("status") not in _LIVE_STATES and not r.get("_settlement_pending")]
    completed.sort(key=lambda kv: kv[1].get("completed_at") or kv[1].get("dispatched_at") or 0)
    for rid, _ in completed[: max(0, len(completed) - _MAX_RETAINED_COMPLETED)]:
        _records.pop(rid, None)




# ── Dispatch ────────────────────────────────────────────────────────────────
def _obtain_submission(
    *, delegation_id: str, label: str, max_workers: int, record: dict[str, Any],
    mark_queued: Callable[[str], None], mark_started: Callable[[str], None],
    mark_exited: Callable[[str], None], mark_execution_unknown: Callable[[BaseException], None],
    failure_result: Callable[[dict[str, Any], str], dict[str, Any]],
    release_lease: Callable[[], bool], mark_settlement_pending: Callable[[], None],
    release_after_settlement: Callable[[], None],
) -> tuple[Any, Optional[dict[str, Any]]]:
    """Cross the executor queue boundary and classify only pre-submit failures."""
    submit_invoked = False
    try:
        executor = _get_executor(max_workers)
        if executor is None:
            raise RuntimeError("async executor unavailable")
        submit_invoked = True
        submission = executor.submit_owned(
            delegation_id, propagate_context_to_thread(record["_leased_worker"]),
            on_queued=mark_queued, on_started=mark_started, on_exited=mark_exited)
    except _CUSTODY_EXCEPTIONS as exc:  # pragma: no cover — pool handoff failures are rare
        if submit_invoked:
            mark_execution_unknown(exc)
            logger.error(
                "Async delegation%s %s submission outcome is unknown; retaining accepted custody: %s",
                label, delegation_id, exc,
            )
            return None, {"status": "dispatched", "delegation_id": delegation_id, "acceptance": "unknown"}
        error = f"Failed to obtain async executor{label}: {exc}"
        try:
            _unschedule_dispatch(delegation_id)
        except _ROUTINE_EXCEPTIONS as unschedule_exc:
            terminal_error = (
                f"{error}; durable admission could not be unscheduled safely: "
                f"{type(unschedule_exc).__name__}: {unschedule_exc}"
            )
            try:
                _finalize(delegation_id, lambda snapshot: failure_result(snapshot, terminal_error), "error")
            except _ROUTINE_EXCEPTIONS:
                logger.exception("Async delegation%s failed to settle rejected admission %s", label, delegation_id)
                mark_settlement_pending()
            finally:
                release_after_settlement()
            return None, {"status": "rejected", "error": terminal_error}
        with _records_lock:
            _records.pop(delegation_id, None)
        release_lease()
        return None, {"status": "rejected", "error": error}
    return submission, None


def _finish_submission(
    *, record: dict[str, Any], delegation_id: str, label: str, submission: Any,
    progress_fn: Optional[Callable[[], tuple]], cancelled_worker: Callable[[], None],
    observe_future_cancellation: Callable[[Any], bool],
    mark_execution_unknown: Callable[[BaseException], None],
    release_lease: Callable[[], bool],
    mark_admission_recovery: Callable[[str], None],
) -> dict[str, Any]:
    """Install the accepted Future and preserve unknown/cancelled handoffs."""
    with _records_lock:
        current = _records.get(delegation_id)
        if current is not None:
            current["_submission_state"] = submission.accepted
    if submission.accepted == "no":
        try:
            _execution_update(
                record, "rejected_before_enqueue", accepted="no", worker_exited=True,
                error={"kind": type(submission.error).__name__ if submission.error else "rejected"},
            )
            _unschedule_dispatch(delegation_id)
            try:
                released = release_lease()
            except _CUSTODY_EXCEPTIONS as release_exc:
                mark_admission_recovery(
                    f"{label} executor refusal lease release is unknown: "
                    f"{type(release_exc).__name__}: {release_exc}"
                )
                return {
                    "status": "rejected",
                    "error": f"{submission.error or 'executor rejected handoff'}; exact custody retained",
                }
            if released:
                with _records_lock:
                    _records.pop(delegation_id, None)
                return {"status": "rejected", "error": str(submission.error or "executor rejected handoff")}
            mark_admission_recovery(
                f"{label} executor refusal exact lease release was refused; exact custody retained"
            )
            return {
                "status": "rejected",
                "error": f"{submission.error or 'executor rejected handoff'}; exact custody retained",
            }
        except _CUSTODY_EXCEPTIONS as exc:
            mark_execution_unknown(exc)
            logger.error("Async delegation%s %s no-enqueue cleanup is unknown", label, delegation_id, exc_info=True)
            return {"status": "dispatched", "delegation_id": delegation_id, "acceptance": "unknown"}
    if submission.accepted == "unknown":
        with _records_lock:
            current = _records.get(delegation_id)
            if current is not None:
                current["_future"] = submission.future
                current["_cancel_handoff"] = cancelled_worker
        if submission.future is not None:
            observe_future_cancellation(submission.future)
        mark_execution_unknown(submission.error or RuntimeError("executor enqueue outcome unknown"))
        return {"status": "dispatched", "delegation_id": delegation_id, "acceptance": "unknown"}
    if submission.error is not None:
        mark_execution_unknown(submission.error)
        logger.error(
            "Async delegation%s %s accepted queue handoff has a start error: %s",
            label, delegation_id, submission.error,
        )
    future = submission.future
    with _records_lock:
        current = _records.get(delegation_id)
        if current is not None:
            current["_future"] = future
            current["_cancel_handoff"] = cancelled_worker
    observed = observe_future_cancellation(future) if future is not None else False
    if not observed:
        logger.error(
            "Async delegation%s %s cancellation observer unavailable; keeping accepted work",
            label, delegation_id,
        )
    reconcile_pending_handoff(delegation_id)
    if progress_fn is not None:
        try:
            _ensure_stale_monitor()
        except _ROUTINE_EXCEPTIONS:
            logger.exception("Async delegation%s %s stale monitor could not start", label, delegation_id)
    return {
        "status": "dispatched", "delegation_id": delegation_id,
        **({"acceptance": "unknown"} if submission.error is not None else {}),
    }


def _dispatch(**kwargs) -> dict[str, Any]:
    from hermes_cli.backend_retirement import retirement
    with retirement.work() as admitted:
        if not admitted:
            return {"status": "rejected", "error": "backend is retiring; reconnect to continue"}
        return _dispatch_admitted(**kwargs)


def _dispatch_admitted(
    *, delegation_id: str, goal: str, goals: Optional[list[str]], context: Optional[str],
    toolsets: Optional[list[str]], role: str, model: Optional[str], session_key: str,
    parent_session_id: Optional[str], runner: Callable[[], dict[str, Any]], origin_ui_session_id: str,
    origin_session_id: str, interrupt_fn: Optional[Callable[[], None]], max_async_children: int,
    progress_fn: Optional[Callable[[], tuple]], capacity_error: str, slot_key: Optional[str] = None,
    task_indexes: Optional[list[int]] = None,
    task_transcripts: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Admit one async unit and retain its exact retirement lease through settlement.
    A submit exception after the executor call begins is an unknown handoff, even when
    private executor flags happen to say ``shutdown`` or ``broken``. Only failure before
    ``executor.submit`` is invoked permits unscheduling. Terminal UI state is published
    independently from durable/queue settlement; an unresolved settlement keeps its exact
    lease and in-memory recovery record without counting as an active slot.
    """
    worker_ledger()  # refuse unbound workers before publishing a live record
    is_batch = goals is not None
    label = " batch" if is_batch else ""
    classify = _batch_status if is_batch else (lambda r: r.get("status") or "completed")
    crash_result = _batch_crash if is_batch else _single_crash
    dispatched_at = time.time()
    owner_execution_id = f"async:{os.getpid()}:{delegation_id}:{uuid.uuid4().hex[:12]}"
    owner_scope, owner_scope_required = capture_owner_scope_state(parent_session_id)
    record: dict[str, Any] = {
        "delegation_id": delegation_id, "goal": goal, **({"goals": list(goals)} if is_batch else {}),
        "context": context, "toolsets": list(toolsets) if toolsets else None, "role": role, "model": model,
        "session_key": session_key, "origin_ui_session_id": origin_ui_session_id,
        "origin_session_id": origin_session_id, "parent_session_id": parent_session_id,
        **_capture_routing_origin(),
        "status": "running", "dispatched_at": dispatched_at, "completed_at": None,
        "interrupt_fn": interrupt_fn, **({"is_batch": True} if is_batch else {}), "progress_fn": progress_fn,
        "slot_key": slot_key or delegation_id,
        **({"task_transcripts": dict(task_transcripts)} if task_transcripts else {}),
        **({"task_indexes": list(task_indexes)} if task_indexes is not None else {}),
        "_context": contextvars.copy_context(),
        "_owner_scope_required": owner_scope_required,
        "_progress_token": None, "_progress_ts": dispatched_at, "_interrupted_at": None,
        "_finalization_event": threading.Event(), "_settlement_pending": False,
        "_settlement_retrying": False,
        "execution": {
            "handoff_id": delegation_id, "state": "prepared", "accepted": "unknown",
            "worker_started": False, "worker_exited": False, "owner_pid": os.getpid(),
            "owner_started_at": None, "owner_execution_id": owner_execution_id,
            "owner_scope": owner_scope, "error": None, "exception": None}}
    from hermes_cli.backend_retirement import retirement
    lease = retirement.acquire(owner_kind="async-delegation", owner_id=delegation_id)
    if lease is None:
        return {"status": "rejected", "error": "backend is retiring; reconnect to continue"}
    lease_owned = True
    lease_release_lock = threading.Lock()
    def _release_lease() -> bool:
        nonlocal lease_owned
        with lease_release_lock:
            if not lease_owned:
                return True
            if retirement.release(lease):
                lease_owned = False
                return True
            logger.error("Async delegation %s exact retirement lease release was refused", delegation_id)
            return False
    record["_retirement_lease"] = lease
    record["_release_lease"] = _release_lease
    with _records_lock:
        refused = any((field, record[field]) in _refused_by for field in _FENCED_FIELDS)
        active_slots = {r.get("slot_key") or r["delegation_id"] for r in _records.values()
                        if r.get("status") in _ACTIVE_STATES}
        at_capacity = record["slot_key"] not in active_slots and len(active_slots) >= max_async_children
        if not refused and not at_capacity:
            _records[delegation_id] = record
            live_units = sum(1 for r in _records.values() if r.get("status") in _LIVE_STATES)
        else:
            live_units = 0
    if refused:
        _release_lease()
        return {"status": "rejected", "error": "This session was closed (exit or /new): no new background work."}
    if at_capacity:
        _release_lease()
        return {"status": "rejected", "error": capacity_error}
    def _failure_result(snapshot: dict[str, Any], error: str) -> dict[str, Any]:
        completed_at = snapshot.get("completed_at") or time.time()
        started_at = snapshot.get("dispatched_at") or completed_at
        return crash_result(error, round(completed_at - started_at, 2))
    def _mark_settlement_pending() -> None:
        with _records_lock:
            current = _records.get(delegation_id)
            if current is not None:
                current["_settlement_pending"] = True
    def _release_after_settlement(*, settled: bool = False) -> None:
        with _records_lock:
            current = _records.get(delegation_id)
            finalizing = bool(current and current.get("status") == "finalizing")
            execution = dict(current.get("execution") or {}) if current else {}
            receipt = current.get("_settlement_receipt") if current else None
            worker_exited = execution.get("state") in {"runner_exited", "cancelled_before_start", "rejected_before_enqueue"}
            ready = settled or bool(receipt and receipt.settled)
        if current is not None and worker_exited and not finalizing and ready:
            released = _release_lease()
            if current is not None:
                with _records_lock:
                    current = _records.get(delegation_id)
                    if current is not None:
                        if released:
                            current["_settlement_pending"] = False
                            for key in ("_settlement_event", "_settlement_result", "_settlement_receipt"):
                                current.pop(key, None)
                        else:
                            current["_settlement_pending"] = True
    def _mark_admission_recovery(error: str) -> None:
        with _records_lock:
            current = _records.get(delegation_id)
            if current is None:
                return
            completed_at = time.time()
            snapshot = dict(current)
            snapshot["completed_at"] = completed_at
            current.update(
                status="error", completed_at=completed_at, _settlement_pending=True,
                _admission_recovery_pending=True, _admission_error=error,
                _admission_result=_failure_result(snapshot, error))
    record["_release_after_settlement"] = _release_after_settlement
    try:
        _persist_dispatch(record)
    except _ROUTINE_EXCEPTIONS as exc:
        error = f"Failed to persist async delegation{label} before worker submission: {type(exc).__name__}: {exc}"
        return reconcile_persist_failure(
            record=record, delegation_id=delegation_id, label=label, error=error,
            failure_result=_failure_result, release_lease=_release_lease,
            mark_settlement_pending=_mark_settlement_pending,
            mark_admission_recovery=_mark_admission_recovery,
            release_after_settlement=_release_after_settlement,
            read_admission_row=_read_admission_row,
            admission_row_matches=_admission_row_matches,
            reject_before_enqueue=_reject_before_enqueue,
            finalize=_finalize, records_lock=_records_lock, records=_records,
            logger=logger,
        )
    def _mark_execution_unknown(error: BaseException) -> None:
        metadata = {"kind": type(error).__name__, "message": str(error)[:240]}
        with _records_lock:
            current = _records.get(delegation_id)
            if current is None:
                return
            state = (current.get("execution") or {}).get("state")
        target = state if state in {"runner_exited", "cancelled_before_start", "rejected_before_enqueue"} else "unknown"
        fields = {"accepted": "unknown", "worker_exited": False, "error": metadata}
        if target == "cancelled_before_start":
            fields.update(accepted="yes", worker_exited=True)
        elif target == "rejected_before_enqueue":
            fields.update(accepted="no", worker_exited=True)
        elif target == "runner_exited":
            fields.update(accepted="yes", worker_exited=True)
        try:
            _execution_update(record, target, **fields)
        except _CUSTODY_EXCEPTIONS:
            with _records_lock:
                current = _records.get(delegation_id)
                if current is None:
                    return
                claim = _execution_claim(current)
                claim.update(state="unknown", accepted="unknown", worker_exited=False, error=metadata)
                current["execution"] = claim

    def _mark_queued(handoff_id: str) -> None:
        try:
            with _records_lock:
                current = _records.get(handoff_id)
                if current is None or current["execution"].get("state") != "prepared":
                    return
            _execution_update(record, "queued", accepted="yes")
        except _CUSTODY_EXCEPTIONS as exc:
            _mark_execution_unknown(exc)

    def _mark_started(handoff_id: str) -> None:
        try:
            with _records_lock:
                current = _records.get(handoff_id)
                if current is not None:
                    current.update(_started=True, _progress_ts=time.time())
            _execution_update(record, "started", accepted="yes", worker_started=True)
        except _CUSTODY_EXCEPTIONS as exc:
            _mark_execution_unknown(exc)

    def _mark_exited(handoff_id: str) -> None:
        try:
            _execution_update(record, "runner_exited", worker_exited=True)
        except _CUSTODY_EXCEPTIONS as exc:
            _mark_execution_unknown(exc)
            logger.error("Async delegation%s %s runner-exit receipt is unknown: %s", label, handoff_id, exc)
        finally:
            _release_after_settlement()

    def _worker() -> None:
        result: dict[str, Any] = {}
        status = "error"
        original_base_exception: BaseException | None = None
        try:
            result = runner() or {}
            status = classify(result)
        except _CUSTODY_EXCEPTIONS as exc:
            logger.exception(f"Async delegation{label} %s crashed", delegation_id)
            result = crash_result(f"{type(exc).__name__}: {exc}", round(time.time() - dispatched_at, 2))
            result["exception"] = {"type": type(exc).__name__, "message": str(exc)[:240], "base": True}
            original_base_exception = exc
        finally:
            try:
                _finalize(delegation_id, result, status)
            except _CUSTODY_EXCEPTIONS:
                logger.exception("Async delegation%s %s finalization crashed; retaining settlement custody",
                                 label, delegation_id)
                _mark_settlement_pending()
            finally:
                _release_after_settlement()
        if original_base_exception is not None:
            raise original_base_exception

    def _leased_worker() -> None:
        _worker()

    def _cancelled_worker() -> None:
        try:
            _execution_update(record, "cancelled_before_start", accepted="yes", worker_exited=True)
        except _CUSTODY_EXCEPTIONS as exc:
            _mark_execution_unknown(exc)
        try:
            _finalize(
                delegation_id,
                lambda snapshot: _failure_result(snapshot, "Async delegation was cancelled before its worker started."),
                "interrupted",
            )
        except _CUSTODY_EXCEPTIONS:
            logger.exception("Async delegation%s %s cancellation finalization failed; retaining custody", label, delegation_id)
            _mark_settlement_pending()
        finally:
            _release_after_settlement()

    def _observe_future_cancellation(future) -> bool:
        """Observe only pre-start cancellation; exact release follows settlement."""
        cancel_lock = threading.Lock()
        cancel_settled = False

        def cancel_once() -> None:
            nonlocal cancel_settled
            with cancel_lock:
                if cancel_settled:
                    return
                cancel_settled = True
            _cancelled_worker()

        def done(completed) -> None:
            try:
                if completed.cancelled():
                    cancel_once()
            except _CUSTODY_EXCEPTIONS:
                logger.warning("Async delegation%s %s cancellation observation failed; retaining custody",
                               label, delegation_id, exc_info=True)

        try:
            future.add_done_callback(done)
        except _CUSTODY_EXCEPTIONS:
            logger.warning("Async delegation%s %s cancellation observer attachment failed; retaining custody",
                           label, delegation_id, exc_info=True)
            try:
                if future.cancelled():
                    cancel_once()
            except _CUSTODY_EXCEPTIONS:
                logger.warning("Async delegation%s %s cancellation status was unavailable; retaining custody",
                               label, delegation_id, exc_info=True)
            return False
        return True

    record["_leased_worker"] = _leased_worker
    submission, early_result = _obtain_submission(
        delegation_id=delegation_id, label=label,
        max_workers=max(max_async_children, live_units), record=record,
        mark_queued=_mark_queued, mark_started=_mark_started, mark_exited=_mark_exited,
        mark_execution_unknown=_mark_execution_unknown, failure_result=_failure_result,
        release_lease=_release_lease, mark_settlement_pending=_mark_settlement_pending,
        release_after_settlement=_release_after_settlement,
    )
    if early_result is not None:
        return early_result
    return _finish_submission(
        record=record, delegation_id=delegation_id, label=label, submission=submission,
        progress_fn=progress_fn, cancelled_worker=_cancelled_worker,
        observe_future_cancellation=_observe_future_cancellation,
        mark_execution_unknown=_mark_execution_unknown,
        release_lease=_release_lease, mark_admission_recovery=_mark_admission_recovery,
    )


def dispatch_async_delegation(
    *, goal: str, context: Optional[str], toolsets: Optional[list[str]], role: str, model: Optional[str],
    session_key: str, parent_session_id: Optional[str] = None, runner: Callable[[], dict[str, Any]],
    origin_ui_session_id: str = "", origin_session_id: str = "", interrupt_fn: Optional[Callable[[], None]] = None,
    max_async_children: int = _DEFAULT_MAX_ASYNC_CHILDREN, progress_fn: Optional[Callable[[], tuple]] = None,
) -> dict[str, Any]:
    """Spawn ``runner`` on the daemon executor and return a handle immediately.
    ``session_key``/``parent_session_id`` are captured on the parent thread (the worker carries
    no contextvars) and route the completion back to the spawning session.
    ``progress_fn() -> (token, in_tool)`` enables stale monitoring; omitted = unmonitored.
    Returns ``{"status": "dispatched", "delegation_id"}`` or ``{"status": "rejected", "error"}``."""
    delegation_id = _new_delegation_id()
    handle = _dispatch(
        delegation_id=delegation_id, goal=goal, goals=None, context=context,
        toolsets=toolsets, role=role, model=model, session_key=session_key,
        parent_session_id=parent_session_id, runner=runner,
        origin_ui_session_id=origin_ui_session_id, origin_session_id=origin_session_id,
        interrupt_fn=interrupt_fn, max_async_children=max_async_children, progress_fn=progress_fn,
        capacity_error=(
            f"Async delegation capacity reached ({max_async_children} running). Wait for one to finish "
            "(its result will re-enter the chat), or run this task synchronously (background=false). "
            "Raise delegation.max_concurrent_children in config.yaml to allow more concurrent background subagents."))
    if handle["status"] == "dispatched":
        logger.info("Dispatched async delegation %s (session_key=%s): %s",
                    delegation_id, session_key or "<cli>", (goal or "")[:80])
    return handle


def dispatch_async_delegation_batch(
    *, goals: list[str], context: Optional[str], toolsets: Optional[list[str]], role: str, model: Optional[str],
    session_key: str, parent_session_id: Optional[str] = None, runner: Callable[[], dict[str, Any]],
    origin_ui_session_id: str = "", origin_session_id: str = "", interrupt_fn: Optional[Callable[[], None]] = None,
    max_async_children: int = _DEFAULT_MAX_ASYNC_CHILDREN, delegation_id: Optional[str] = None,
    progress_fn: Optional[Callable[[], tuple]] = None, slot_key: Optional[str] = None,
    task_indexes: Optional[list[int]] = None,
    task_transcripts: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """Dispatch a fan-out unit (a whole batch, or one ``group`` of a delegate_task call) as ONE
    background unit: ``runner`` runs its tasks and returns the combined ``{"results": [...],
    "total_duration_seconds": N}`` dict. The unit occupies ONE async slot — or joins the slot named
    by ``slot_key`` (in-unit parallelism is bounded separately) — and produces a SINGLE completion
    event carrying per-task ``results``."""
    delegation_id = delegation_id or _new_delegation_id()
    # ``goals`` is the whole call (result task_index indexes it); the unit's own goals label the record.
    unit_goals = [goals[i] for i in task_indexes] if task_indexes is not None else list(goals)
    n = len(unit_goals)
    combined_goal = unit_goals[0] if n == 1 else f"{n} parallel subagents: " + "; ".join(g[:40] for g in unit_goals)
    handle = _dispatch(
        delegation_id=delegation_id, goal=combined_goal, goals=goals, context=context,
        toolsets=toolsets, role=role, model=model, session_key=session_key,
        parent_session_id=parent_session_id, runner=runner,
        origin_ui_session_id=origin_ui_session_id, origin_session_id=origin_session_id,
        interrupt_fn=interrupt_fn, max_async_children=max_async_children, progress_fn=progress_fn, slot_key=slot_key,
        task_indexes=task_indexes, task_transcripts=task_transcripts,
        capacity_error=(
            f"Async delegation capacity reached ({max_async_children} running). Wait for one to finish "
            "(its result will re-enter the chat), or raise delegation.max_concurrent_children in "
            "config.yaml to allow more concurrent background units."))
    if handle["status"] == "dispatched":
        logger.info("Dispatched async delegation batch %s (%d task(s), session_key=%s)",
                    delegation_id, n, session_key or "<cli>")
    return handle


def reconcile_pending_handoff(delegation_id: str) -> dict[str, Any] | None:
    """Read the exact in-process Future/claim without resubmitting or inferring success.

    A completed Future without the wrapper's durable ``runner_exited`` claim is still
    unknown; owner restart recovery uses the same durable claim rather than this hint.
    """
    with _records_lock:
        record = _records.get(delegation_id)
        if record is None:
            return None
        future = record.get("_future")
        claim = dict(record.get("execution") or {})
        cancel = record.get("_cancel_handoff")
    if future is not None and future.cancelled() and callable(cancel):
        cancel()
    elif future is not None and future.done() and claim.get("state") not in {
        "runner_exited", "cancelled_before_start"
    }:
        _mark = {"kind": "future_done_without_runner_exit"}
        try:
            _execution_update(record, "unknown", accepted="unknown", worker_exited=False, error=_mark)
        except _CUSTODY_EXCEPTIONS:
            pass
    with _records_lock:
        current = _records.get(delegation_id)
        return dict(current.get("execution") or {}) if current else None


# ── Finalization + completion events ────────────────────────────────────────
def _finalize(delegation_id: str, result: Any, status: str) -> Optional[_SettlementReceipt]:
    """Publish one terminal receipt while retaining exact custody until both edges settle."""
    owner = False
    wait_event = None
    with _records_lock:
        record = _records.get(delegation_id)
        if record is None:
            return None
        wait_event = record.setdefault("_finalization_event", threading.Event())
        current_status = record.get("status")
        if current_status == "finalizing":
            pass
        elif current_status in _ACTIVE_STATES or (
                current_status == "error" and record.get("_admission_recovery_pending")):
            wait_event.clear()
            record["status"] = "finalizing"
            record["completed_at"] = time.time()
            record["interrupt_fn"] = None
            record["progress_fn"] = None
            snapshot = dict(record)
            owner = True
        else:
            existing = record.get("_settlement_receipt")
            return existing if isinstance(existing, _SettlementReceipt) else None
    if not owner:
        wait_event.wait()
        with _records_lock:
            current = _records.get(delegation_id)
            existing = current.get("_settlement_receipt") if current is not None else None
        return existing if isinstance(existing, _SettlementReceipt) else None

    try:
        terminal_result = result(snapshot) if callable(result) else result
        prepared_event, prepared_result = _build_completion_event(snapshot, terminal_result, status)
        prepared_receipt = _SettlementReceipt(False, False, prepared_event, prepared_result)
        with _records_lock:
            current = _records.get(delegation_id)
            if current is not None:
                current.update(_settlement_pending=True, _settlement_event=prepared_event,
                               _settlement_result=prepared_result, _settlement_receipt=prepared_receipt)
        # Preserve the established three-argument hook; the typed receipt is staged above.
        receipt = _push_completion_event(snapshot, terminal_result, status)
        with _records_lock:
            current = _records.get(delegation_id)
            if current is not None:
                current["status"] = status
                current.pop("_admission_recovery_pending", None)
                current.pop("_admission_result", None)
                current.update(_settlement_pending=True, _settlement_event=receipt.event,
                               _settlement_result=receipt.result, _settlement_receipt=receipt)
                _prune_completed_locked()
        return receipt
    except _CUSTODY_EXCEPTIONS:
        with _records_lock:
            current = _records.get(delegation_id)
            if current is not None:
                current["_settlement_pending"] = True
        raise
    finally:
        wait_event.set()


def _enqueue_completion_event(event: dict[str, Any], label: str) -> bool:
    try:
        from tools.process_registry import process_registry
        process_registry.completion_queue.put(event)
        return True
    except _ROUTINE_EXCEPTIONS as exc:  # pragma: no cover — queue failures are injected by focused tests
        logger.error("Async delegation%s %s: completion queue publication failed; retaining custody: %s",
                     label, event.get("delegation_id"), exc)
        return False


def _build_completion_event(record: dict[str, Any], result: dict[str, Any], status: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build the typed terminal payload without performing either fallible settlement edge."""
    result = redact_child_result(result, force=False)
    is_batch = bool(record.get("is_batch"))
    dispatched_at = record.get("dispatched_at") or time.time()
    completed_at = record.get("completed_at") or time.time()
    if is_batch:
        payload = {
            "is_batch": True, "results": result.get("results") or [],
            "live_transcripts": result.get("live_transcripts"), "error": result.get("error"),
            "total_duration_seconds": result.get("total_duration_seconds"),
            **({"group": result["group"]} if result.get("group") is not None else {})}
    else:
        payload = {
            "summary": result.get("summary"), "error": result.get("error"), "api_calls": result.get("api_calls", 0),
            "duration_seconds": result.get("duration_seconds", round(completed_at - dispatched_at, 2))}
    evt = {
        "type": "async_delegation", "delegation_id": record.get("delegation_id"),
        "session_key": record.get("session_key", ""),
        "origin_ui_session_id": record.get("origin_ui_session_id", ""),
        "origin_session_id": record.get("origin_session_id", ""),
        "parent_session_id": record.get("parent_session_id"),
        "goal": record.get("goal", ""), **({"goals": record.get("goals")} if is_batch else {}),
        "context": record.get("context"), "toolsets": record.get("toolsets"), "role": record.get("role"),
        "model": record.get("model") if is_batch else (result.get("model") or record.get("model")),
        "status": status, **payload, "dispatched_at": dispatched_at, "completed_at": completed_at,
        **({} if is_batch else {"exit_reason": result.get("exit_reason")}),
        **{k: record[k] for k in _ROUTING_KEYS if record.get(k)},
        **{k: result[k] for k in _STALL_META_KEYS if k in result}}
    return evt, result


def _push_completion_event(record: dict[str, Any], result: dict[str, Any], status: str,
                           *, prepared: Optional[tuple[dict[str, Any], dict[str, Any]]] = None) -> _SettlementReceipt:
    """Acknowledge durable persistence and queue publication separately."""
    evt, result = prepared or _build_completion_event(record, result, status)
    is_batch = bool(record.get("is_batch"))
    label = " batch" if is_batch else ""
    durable = False
    try:
        _persist_completion(evt, result)
        durable = True
    except _ROUTINE_EXCEPTIONS as exc:
        logger.error("Async delegation%s %s: durable completion write failed; retaining settlement custody: %s",
                     label, record.get("delegation_id"), exc)
    queued = _enqueue_completion_event(evt, label)
    return _SettlementReceipt(durable=durable, queued=queued, event=evt, result=result)


def _retry_admission_recovery(
    delegation_id: str, record: dict[str, Any], release: Any, release_after: Any,
) -> bool:
    if (remote := worker_ledger()) is not None:
        remote.retry_pending_settlement()
    readback_known, saved = _read_admission_row(delegation_id)
    if readback_known and saved is None:
        if not callable(release) or not release():
            return False
        with _records_lock:
            _records.pop(delegation_id, None)
        return True
    if not readback_known or not _admission_row_matches(record, saved):
        return False
    if not _reject_before_enqueue(record, "admission retry readback proved no submission"):
        return False
    with _records_lock:
        current = _records.get(delegation_id)
        admission_result = current.get("_admission_result") if current is not None else None
    try:
        receipt = _finalize(delegation_id, admission_result or {"status": "error", "summary": None}, "error")
    except _CUSTODY_EXCEPTIONS:
        logger.exception("Async delegation %s admission retry reconciliation failed", delegation_id)
        return False
    if not isinstance(receipt, _SettlementReceipt) or not callable(release_after):
        return False
    release_after()
    with _records_lock:
        current = _records.get(delegation_id)
        return current is not None and not current.get("_settlement_pending")


def _retry_settlement_receipt(
    delegation_id: str, receipt: Any, event: Any, result: Any, release_after: Any,
) -> bool:
    if not isinstance(receipt, _SettlementReceipt):
        logger.error("Async delegation %s settlement recovery lacks a typed receipt; retaining custody", delegation_id)
        return False
    event = event or receipt.event
    result = result or receipt.result
    if not isinstance(event, dict) or not isinstance(result, dict):
        logger.error("Async delegation %s settlement recovery lacks a typed receipt; retaining custody", delegation_id)
        return False
    durable, queued = receipt.durable, receipt.queued
    if not durable:
        try:
            remote = worker_ledger()
            if remote is not None:
                durable = bool(remote.retry_pending_settlement())
                queued = durable or queued
            else:
                _persist_completion(event, result)
                durable = True
        except _CUSTODY_EXCEPTIONS as exc:
            logger.error("Async delegation %s settlement durable/outbox retry failed; retaining custody: %s",
                         delegation_id, exc)
    if not queued:
        queued = _enqueue_completion_event(event, "")
    recovered = _SettlementReceipt(durable=durable, queued=queued, event=event, result=result)
    with _records_lock:
        current = _records.get(delegation_id)
        if current is None:
            return False
        current.update(_settlement_receipt=recovered, _settlement_pending=not recovered.settled,
                       status=event.get("status", "error"))
    if not recovered.settled or not callable(release_after):
        return False
    release_after(settled=True)
    with _records_lock:
        current = _records.get(delegation_id)
        return current is not None and not current.get("_settlement_pending")


def retry_pending_settlement(delegation_id: str) -> bool:
    """Claim one settlement retry owner and release exact custody only after both edges settle."""
    with _records_lock:
        record = _records.get(delegation_id)
        if record is None or not record.get("_settlement_pending"):
            return False
        if record.get("_settlement_retrying"):
            return False
        record["_settlement_retrying"] = True
        admission_recovery = bool(record.get("_admission_recovery_pending"))
        receipt = record.get("_settlement_receipt")
        event = record.get("_settlement_event")
        result = record.get("_settlement_result")
        release = record.get("_release_lease")
        release_after = record.get("_release_after_settlement")

    try:
        if admission_recovery:
            return _retry_admission_recovery(delegation_id, record, release, release_after)
        return _retry_settlement_receipt(delegation_id, receipt, event, result, release_after)
    finally:
        with _records_lock:
            current = _records.get(delegation_id)
            if current is not None:
                current["_settlement_retrying"] = False


def push_task_failure_notice(delegation_id: str, entry: dict[str, Any], *, n_tasks: int) -> None:
    """Surface ONE failed child of a still-running detached batch to the parent now, instead of
    when the slowest sibling finishes. In a 1,393-agent run every wave-1 child died in a 401 storm
    at 08:29 and the parent learned of it at 09:36, when the batch's "unknown outcome" block finally
    arrived: 66 minutes of a dead wave with nothing running. The notice rides the same
    ``type="async_delegation"`` event shape as the batch result (so every drain/route/format path
    treats it identically) with ``task_failure_notice=True`` and a single-entry ``results`` list; the
    batch record is NOT finalized and its consolidated result still arrives as before. In-process
    queue only (never persisted), so the parent's copy honours ``security.redact_secrets: false``."""
    entry = redact_child_result(entry, force=False)
    with _records_lock:
        record = _records.get(delegation_id)
        if record is None or record.get("status") not in _ACTIVE_STATES:
            return
        snapshot = dict(record)
    try:
        from tools.process_registry import process_registry
    except _ROUTINE_EXCEPTIONS as exc:  # pragma: no cover
        logger.error("Async delegation batch %s: task failure notice dropped (process_registry import): %s", delegation_id, exc)
        return
    evt = {
        "type": "async_delegation", "task_failure_notice": True, "is_batch": True, "n_tasks": n_tasks,
        "delegation_id": delegation_id, "results": [entry],
        "session_key": snapshot.get("session_key", ""),
        "origin_ui_session_id": snapshot.get("origin_ui_session_id", ""),
        "origin_session_id": snapshot.get("origin_session_id", ""),
        "parent_session_id": snapshot.get("parent_session_id"),
        "goal": snapshot.get("goal", ""), "goals": snapshot.get("goals"), "context": snapshot.get("context"),
        "toolsets": snapshot.get("toolsets"), "role": snapshot.get("role"), "model": snapshot.get("model"),
        "status": "running", "dispatched_at": snapshot.get("dispatched_at") or time.time(), "completed_at": time.time(),
        **{k: snapshot[k] for k in _ROUTING_KEYS if snapshot.get(k)}}
    try:
        process_registry.completion_queue.put(evt)
    except _ROUTINE_EXCEPTIONS as exc:  # pragma: no cover
        logger.error("Async delegation batch %s: failed to enqueue task failure notice: %s", delegation_id, exc)


# ── Stale monitor ───────────────────────────────────────────────────────────
def _ensure_stale_monitor() -> None:
    """Start (once) the stale-delegation monitor thread. One daemon thread serves
    every dispatch; it exits when no monitorable records remain and is restarted
    by the next dispatch with a ``progress_fn``."""
    global _monitor_thread
    with _monitor_lock:
        if _monitor_thread is not None and _monitor_thread.is_alive():
            return
        _monitor_stop.clear()
        _monitor_thread = threading.Thread(
            target=_stale_monitor_loop, name="async-delegate-stale-monitor", daemon=True)
        _monitor_thread.start()


def _sweep_stale_locked(now: float):
    """One monitor pass over ``_records``; caller holds ``_records_lock``. Returns
    ``(stalled, expired, any_monitorable)``: newly-stalling ``(delegation_id, quiet_for, in_tool)``
    tuples, stalling ids past the grace window, and whether anything is left to monitor."""
    stalled, expired, any_monitorable = [], [], False  # (delegation_id, quiet_for, in_tool) / ids past grace
    for record in _records.values():
        status = record.get("status")
        if status == "stalling":
            any_monitorable = True
            if now - (record.get("_interrupted_at") or now) >= _STALL_GRACE_SECONDS:
                expired.append(record["delegation_id"])
            continue
        progress_fn = record.get("progress_fn")
        if status != "running" or progress_fn is None:
            continue
        any_monitorable = True
        if not record.get("_started"):
            continue  # queued behind a full pool: not stalled, but keep the monitor alive for when it starts
        try:
            token, in_tool = progress_fn()
        except _ROUTINE_EXCEPTIONS:
            # An unreadable child keeps the last timestamp; it never looks permanently healthy.
            token, in_tool = record.get("_progress_token"), False
        if token != record.get("_progress_token"):
            record.update(_progress_token=token, _progress_ts=now)
            continue
        quiet_for = now - (record.get("_progress_ts") or now)
        limit = _STALE_IN_TOOL_SECONDS if in_tool else _STALE_IDLE_SECONDS
        if quiet_for >= limit:
            # Stall context feeds the terminal event and listings.
            record.update(
                status="stalling", _interrupted_at=now, _stall_quiet_seconds=round(quiet_for, 2),
                _stall_threshold_seconds=limit, _stall_in_tool=bool(in_tool))
            stalled.append((record["delegation_id"], quiet_for, in_tool))
    return stalled, expired, any_monitorable


def _call_interrupt(fn, msg: str, *args) -> bool:
    """Invoke an ``interrupt_fn``; True on success, else debug-log ``msg`` (+ exc)."""
    if not callable(fn):
        return False
    try:
        fn()
        return True
    except _ROUTINE_EXCEPTIONS as exc:
        logger.debug(msg, *args, exc)
        return False


def _stale_monitor_loop() -> None:
    """Sweep running delegations for stalled progress. A changed progress token refreshes the
    record's timestamp; a frozen token past the idle/in-tool threshold marks the record
    ``stalling`` and calls ``interrupt_fn``; a ``stalling`` record still unreturned after the
    grace window is force-finalized with a terminal ``stalled`` event."""
    while not _monitor_stop.wait(_STALE_CHECK_INTERVAL):
        now = time.time()
        with _records_lock:
            stalled, expired, any_monitorable = _sweep_stale_locked(now)
        for delegation_id, quiet_for, in_tool in stalled:
            logger.warning("Async delegation %s made no progress for %.0fs "
                           "(in_tool=%s) — interrupting; grace window %.0fs",
                           delegation_id, quiet_for, in_tool, _STALL_GRACE_SECONDS)
            with _records_lock:
                fn = (_records.get(delegation_id) or {}).get("interrupt_fn")
            _call_interrupt(fn, "Async delegation %s stall interrupt failed: %s", delegation_id)
        for delegation_id in expired:
            with _records_lock:
                current = _records.get(delegation_id) or {}
                ctx = current.get("_context") or contextvars.copy_context()
                release_after = current.get("_release_after_settlement")
            try:
                ctx.run(_finalize, delegation_id, lambda rec, d=delegation_id: _stalled_result(d, rec), "stalled")
            finally:
                if callable(release_after):
                    release_after()
        if not any_monitorable:
            return


def _stalled_error_text(event_record: dict[str, Any]) -> str:
    """Human wording for a force-finalized stall. This string reaches the user (CLI timeline, Desktop
    async-result card), so it names the task, how long it was silent, and what to do — no issue
    numbers or worker internals (those stay in the log line and the stall_* metadata)."""
    goal = " ".join(str(event_record.get("goal") or "").split())
    label = f'Background task "{goal[:120]}"' if goal else "The background task"
    quiet = float(event_record.get("_stall_quiet_seconds") or 0)
    silence = f" after {round(quiet / 60)} min of no progress" if quiet >= 60 else ""
    return (f"{label} stopped responding{silence} and was cancelled. Nothing else was affected; "
            "ask me to run it again if you still need it.")


def _stalled_result(delegation_id: str, event_record: dict[str, Any]) -> dict[str, Any]:
    """Synthetic terminal result for a stalling delegation whose runner never returned."""
    completed_at = event_record.get("completed_at") or time.time()
    duration = round(completed_at - (event_record.get("dispatched_at") or completed_at), 2)
    error = _stalled_error_text(event_record)
    logger.error("Async delegation %s force-finalized as stalled after %.0fs", delegation_id, duration)
    # Structured stall metadata distinguishes a monitor kill without parsing the error string.
    stall_in_tool = event_record.get("_stall_in_tool")
    stall_meta = {
        "stalled_after_quiet_seconds": event_record.get("_stall_quiet_seconds"),
        "stall_threshold_seconds": event_record.get("_stall_threshold_seconds"),
        "stall_phase": "in_tool" if stall_in_tool else "idle" if stall_in_tool is not None else None,
        "stall_grace_seconds": _STALL_GRACE_SECONDS}
    if event_record.get("is_batch"):
        return {**_batch_crash(error, duration), **stall_meta}
    return {**_single_crash(error, duration), "status": "stalled", "exit_reason": "stalled", **stall_meta}


# ── Observability + control ─────────────────────────────────────────────────
def _children_activity_from_token(token: Any, now: float) -> Optional[list]:
    """Parse a progress token into per-child activity dicts (best-effort): delegate_tool
    emits one ``(api_call_count, current_tool, last_activity_ts)`` tuple per child;
    foreign token shapes degrade to ``None`` entries."""
    try:
        parts = list(token)
    except TypeError:
        return None
    out: list[Optional[dict[str, Any]]] = []
    for part in parts:
        if not (isinstance(part, (list, tuple)) and len(part) >= 2):
            out.append(None)
            continue
        entry: dict[str, Any] = {"api_calls": part[0], "current_tool": part[1]}
        if len(part) >= 3 and isinstance(part[2], (int, float)):
            entry["seconds_since_activity"] = round(max(0.0, now - float(part[2])), 1)
        out.append(entry)
    return out


def list_async_delegations() -> list[dict[str, Any]]:
    """Snapshot of async delegations (running + recently completed) without callables or private
    monitor bookkeeping; adds computed live fields for UIs (``seconds_since_progress``,
    ``children_activity``/``in_tool`` sampled from ``progress_fn``) and stall context once tripped.

    Safe to call from any thread. See #51690.
    """
    now = time.time()
    samplers: dict[str, Callable] = {}
    with _records_lock:
        items = []
        for r in _records.values():
            item = {k: v for k, v in r.items() if k not in {"interrupt_fn", "progress_fn"} and not k.startswith("_")}
            status = r.get("status")
            if status in _ACTIVE_STATES:
                if r.get("_progress_ts"):
                    item["seconds_since_progress"] = round(now - r["_progress_ts"], 1)
                if callable(r.get("progress_fn")):
                    samplers[r["delegation_id"]] = r["progress_fn"]
            if status in ("stalling", "stalled"):
                for src, dst in _STALL_FIELD_MAP:
                    if r.get(src) is not None:
                        item[dst] = r.get(src)
            items.append(item)
    # Sample OUTSIDE the lock so a slow sampler cannot block dispatch/finalize.
    for item in items:
        fn = samplers.get(item.get("delegation_id"))
        if fn is None:
            continue
        try:
            token, in_tool = fn()
        except _ROUTINE_EXCEPTIONS:
            continue
        activity = _children_activity_from_token(token, now)
        if activity is not None:
            item["children_activity"] = activity
        item["in_tool"] = bool(in_tool)
    return items


def _interrupt_records(targets: list[dict[str, Any]], caller: str, reason: str, msg: str) -> int:
    """Call ``interrupt_fn`` on each record; log ``msg`` once; returns how many succeeded."""
    count = sum(
        _call_interrupt(r.get("interrupt_fn"), "%s: %s interrupt failed: %s", caller, r.get("delegation_id"))
        for r in targets)
    if count:
        logger.info(msg, count, reason)
    return count


def interrupt_all(reason: str = "shutdown") -> int:
    """Signal every running async delegation to stop (``/stop``, shutdown). Returns how
    many. The child still emits a completion event (status='interrupted') via the
    normal finalize path."""
    with _records_lock:
        targets = [r for r in _records.values() if r.get("status") in _ACTIVE_STATES]
    return _interrupt_records(targets, "interrupt_all", reason, "Interrupted %d async delegation(s) (%s)")


def interrupt_for_session(
    session_key: str = "", origin_ui_session_id: str = "", parent_session_id: str = "", reason: str = "session_end",
) -> int:
    """Signal running async delegations owned by ONE ending session to stop (any
    selector matches, see ``_session_records``). Returns how many."""
    targets = _session_records(_ACTIVE_STATES, session_key, origin_ui_session_id, parent_session_id)
    return _interrupt_records(
        targets, "interrupt_for_session", reason, "Interrupted %d async delegation(s) for ending session (%s)")


def refuse_for_session(token: Any, *, session_key: str = "", parent_session_ids=()) -> None:
    """Refuse new dispatches for ONE closing session (any selector matches) until ``allow_for_session``.
    Set before ``interrupt_for_session``: a dispatch registered first is interrupted, a later one refused."""
    fence = {("session_key", session_key)} if session_key else set()
    fence.update(("parent_session_id", parent) for parent in parent_session_ids if parent)
    with _records_lock:
        _unfence(token)
        _refused[token] = frozenset(fence)
        for selector in fence:
            _refused_by.setdefault(selector, set()).add(token)


def allow_for_session(token: Any) -> None:
    """Lift ONE session's refusal: a viewer attached again, or the owner retired its live state."""
    with _records_lock:
        _unfence(token)


def _unfence(token: Any) -> None:
    """Drop *token*'s refusal; caller holds ``_records_lock``."""
    for selector in _refused.pop(token, ()):
        tokens = _refused_by[selector]
        tokens.discard(token)
        if not tokens:
            del _refused_by[selector]


def _reset_for_tests() -> None:
    """Test-only: clear all state and tear down the executor + monitor."""
    global _executor, _executor_max_workers, _monitor_thread
    with _executor_lock:
        if _executor is not None:
            _executor.shutdown(wait=False)
        _executor = None
        _executor_max_workers = 0
    _monitor_stop.set()
    with _monitor_lock:
        thread, _monitor_thread = _monitor_thread, None
    if thread is not None and thread.is_alive():
        thread.join(timeout=2)
    with _records_lock:
        _records.clear()
        _refused.clear()
        _refused_by.clear()
    with _orphan_lock:
        _offered.clear()
        _startup_replayed.clear()
        _last_orphan_sweep.clear()
