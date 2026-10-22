"""Source-owned custody helpers for goal, loop, and heartbeat producers.

The runtime coordinator remains the only lease ledger.  This module only keeps the
source's watcher task references and returns typed views over coordinator handles;
it never creates a second admission or completion store.
"""
from __future__ import annotations

from dataclasses import dataclass
import json
import sqlite3
from typing import Any

from hermes_state_runtime import RuntimeStoreError

I08_OWNER = "I08_goals_loops_heartbeat"


@dataclass(frozen=True)
class WatcherLease:
    """A source watcher identity backed by the runner's actual task."""

    authority: Any
    key: str
    kind: str
    profile_id: Any
    authority_epoch: int


@dataclass(frozen=True)
class TickLease:
    """A typed view over one coordinator handle reserved before a tick effect."""

    authority: Any
    handle: Any
    kind: str
    request_id: str
    watcher_key: str | None = None
    turn_lease: Any = None


def view_for_handle(authority, handle) -> TickLease | None:
    if authority is None or handle is None:
        return None
    # A missing pending-root entry is distinct from a malformed entry.  The former can still
    # be authenticated through the canonical automation binding; the latter must refuse closed.
    pending = getattr(authority, "_preservation_pending_roots", {}).get(id(handle))
    if getattr(handle, "owner_class", None) != I08_OWNER:
        return None
    if not isinstance(pending, dict):
        for admission_id, binding in getattr(authority, "_preservation_automation_bindings", {}).items():
            if isinstance(binding, dict) and binding.get("handle") is handle:
                row = binding.get("row")
                request_id = row.get("request_id") if isinstance(row, dict) else None
                if type(request_id) is str and request_id:
                    return TickLease(authority, handle, "loop", request_id, None)
        return None
    request_id = pending.get("request_id")
    kind = pending.get("i08_kind")
    if type(request_id) is not str or not request_id or type(kind) is not str or not kind:
        return None
    return TickLease(authority, handle, kind, request_id, pending.get("i08_watcher_key"))


def attach_tick_handle(event, tick_or_handle, kind: str | None = None):
    """Attach one real I08 lease to the event's delivery boundary without overwriting another."""
    if event is None or tick_or_handle is None:
        return None
    handle = getattr(tick_or_handle, "handle", tick_or_handle)
    if handle is None:
        return None
    handles = getattr(event, "_i08_tick_handles", None)
    if not isinstance(handles, list):
        handles = []
        prior = getattr(event, "_i08_tick_handle", None)
        if prior is not None:
            handles.append((getattr(event, "_i08_tick_kind", None), prior))
        event._i08_tick_handles = handles
    if not any(isinstance(item, tuple) and len(item) == 2 and item[1] is handle
               for item in handles):
        handles.append((kind, handle))
    if getattr(event, "_i08_tick_handle", None) is None:
        event._i08_tick_handle = handle
        event._i08_tick_kind = kind
    return handle


def _source_intake_open(authority) -> bool:
    getter = getattr(authority, "_preservation_coordinator", None)
    coordinator = getter() if callable(getter) else None
    if coordinator is None:
        return True
    try:
        return coordinator.status().get("state") == "open"
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return False


def mark_late_registration(authority) -> None:
    if authority is not None:
        _state(authority)["late_registration"] = True


def _state(authority) -> dict:
    state = getattr(authority, "_i08_source_state", None)
    if state is None:
        state = {
            "seen": False,
            "sources": set(),
            "watchers": {},
            "tasks": set(),
        }
        authority._i08_source_state = state
    return state


def mark_source_observed(authority, source: str = "producer") -> None:
    """Record that a canonical I08 producer/source hook was actually entered."""
    if authority is None:
        return
    state = _state(authority)
    state["seen"] = True
    if isinstance(source, str) and source:
        state["sources"].add(source)
    authority._i08_source_seen = True


def register_watcher(authority, key: str, kind: str, *, session_id=None) -> WatcherLease | None:
    """Register a persisted watcher route without creating a second ownership ledger."""
    if authority is None or type(key) is not str or not key or type(kind) is not str or not kind:
        return None
    if not _source_intake_open(authority):
        mark_late_registration(authority)
        return None
    mark_source_observed(authority, f"watcher:{kind}")
    state = _state(authority)
    state["watchers"][f"{kind}:{key}"] = {
        "key": key,
        "kind": kind,
        "session_id": session_id,
        "closed": False,
    }
    return WatcherLease(
        authority, key, kind, getattr(authority, "profile_id", None),
        getattr(authority, "epoch", 0),
    )


def unregister_watcher(authority, key: str, kind: str) -> None:
    state = getattr(authority, "_i08_source_state", None)
    if not isinstance(state, dict):
        return
    item = state.get("watchers", {}).get(f"{kind}:{key}")
    if isinstance(item, dict):
        item["closed"] = True
    state.get("watchers", {}).pop(f"{kind}:{key}", None)


def register_runner_watcher(runner, kind: str, task) -> None:
    """Attach a real long-lived watcher task to each currently served authority."""
    if task is None:
        return
    tasks = getattr(runner, "_i08_watcher_tasks", None)
    if tasks is None:
        tasks = runner._i08_watcher_tasks = set()
    tasks.add(task)
    task.add_done_callback(tasks.discard)
    authorities = getattr(runner, "session_authorities", None)
    values = list(authorities.values()) if isinstance(authorities, dict) else []
    active = getattr(runner, "session_authority", None)
    if active is not None and active not in values:
        values.append(active)
    for authority in values:
        if not _source_intake_open(authority):
            mark_late_registration(authority)
            continue
        mark_source_observed(authority, f"watcher-task:{kind}")
        _state(authority)["tasks"].add(task)


def _activate_child(authority, parent, owner_id):
    coordinator = authority._preservation_coordinator()
    if coordinator is None:
        return None
    reservation = coordinator.reserve_child(
        parent, I08_OWNER, owner_id,
        profile_id=authority.profile_id, authority_epoch=authority.epoch,
    )
    from gateway.runtime_custody import CustodyRefusal

    if isinstance(reservation, CustodyRefusal):
        raise RuntimeStoreError(reservation.code)
    handle = coordinator.activate_child(reservation)
    if isinstance(handle, CustodyRefusal):
        raise RuntimeStoreError(handle.code)
    return handle


def reserve_tick(
    authority, *, request_id: str, principal_id: str, session_id: str,
    source_identity: str, kind: str, watcher_key: str | None = None, parent=None,
    turn_lease=None,
) -> TickLease | None:
    """Reserve the exact I08 handle before a poll, tick, judge, or callback effect."""
    if authority is None:
        return None
    values = (request_id, principal_id, session_id, source_identity, kind)
    if any(type(value) is not str or not value for value in values):
        raise RuntimeStoreError("invalid_params")
    if parent is None and not _source_intake_open(authority):
        raise RuntimeStoreError("runtime_draining")
    mark_source_observed(authority, f"producer:{kind}")
    if parent is not None:
        owner_id = f"{I08_OWNER}:{authority.profile_id}:{session_id}:{request_id}"
        handle = _activate_child(authority, parent, owner_id)
    else:
        handle = authority._preservation_automation_root(
            request_id=request_id, principal_id=principal_id, session_id=session_id,
            source_identity=source_identity, owner_class=I08_OWNER,
        )
    if handle is None:
        return None
    authority._preservation_pending_root(
        request_id, handle, principal_id=principal_id, session_id=session_id,
        source_identity=source_identity,
    )
    pending = getattr(authority, "_preservation_pending_roots", {}).get(id(handle))
    if isinstance(pending, dict):
        pending.update({"i08_kind": kind, "i08_watcher_key": watcher_key})
    getattr(authority, "_preservation_automation_roots", {})[request_id] = handle
    return TickLease(authority, handle, kind, request_id, watcher_key, turn_lease)


def reserve_goal(authority, *, event, session_id: str, request_id: str,
                 principal_id: str, source_identity: str, turn_lease=None) -> TickLease | None:
    """Reserve a goal decision beneath the exact active I04 turn lease."""
    generation = getattr(event, "_hermes_run_generation", None)
    live = getattr(authority, "sessions", {}).get(session_id)
    execution = getattr(getattr(live, "event_stream", None), "execution", None) or {}
    admission_id = execution.get("admission_id")
    if type(generation) is not int:
        generation = execution.get("execution_generation")
    if (
        turn_lease is None
        or getattr(turn_lease, "released", True)
        or getattr(turn_lease, "session_id", None) != session_id
        or getattr(turn_lease, "generation", None) != generation
        or getattr(getattr(turn_lease, "lease", None), "holder", None) is not turn_lease
    ):
        raise RuntimeStoreError("preservation_owner_unmapped")
    if type(admission_id) is not str or not admission_id or type(generation) is not int:
        raise RuntimeStoreError("preservation_owner_unmapped")
    from hermes_state_runtime import get_session_admission

    row = get_session_admission(authority.db, admission_id=admission_id)
    if (not isinstance(row, dict) or row.get("status") != "started"
            or row.get("target_session_id") != session_id
            or row.get("generation") != generation
            or row.get("owner_epoch") != authority.epoch
            or row.get("principal_id") != principal_id):
        raise RuntimeStoreError("preservation_owner_unmapped")
    parent = authority._preservation_operation_parent(
        session_id, admission_id, generation=generation, principal_id=principal_id,
    )
    if parent is None:
        return None
    return reserve_tick(
        authority, request_id=request_id, principal_id=principal_id,
        session_id=session_id, source_identity=source_identity, kind="goal",
        parent=parent, turn_lease=turn_lease,
    )


def handle_for_admission(authority, admission_id):
    if authority is None or not isinstance(admission_id, str) or not admission_id:
        return None
    handle = getattr(authority, "_preservation_handles", {}).get(admission_id)
    if getattr(handle, "owner_class", None) == I08_OWNER:
        return handle
    handle = getattr(authority, "_preservation_children", {}).get(admission_id)
    return handle if getattr(handle, "owner_class", None) == I08_OWNER else None


def _bound_admission(authority, handle):
    for admission_id, candidate in getattr(authority, "_preservation_handles", {}).items():
        if candidate is handle:
            return admission_id
    for admission_id, candidate in getattr(authority, "_preservation_children", {}).items():
        if candidate is handle:
            return admission_id
    return None


def release_unaccepted(tick: TickLease | None, *, reason: str = "deferred") -> bool:
    """Release only an exact handle that has no durable row; ambiguity stays retained."""
    if tick is None:
        return False
    authority, handle = tick.authority, tick.handle
    if _bound_admission(authority, handle) is not None:
        return False
    coordinator = authority._preservation_coordinator()
    if coordinator is None:
        return False
    result = coordinator.release_unaccepted(handle)
    if result is not None and getattr(result, "code", None) != "stale_lease":
        coordinator.mark_unknown(handle, "read_failed")
        return False
    pending = getattr(authority, "_preservation_pending_roots", {})
    pending.pop(id(handle), None)
    roots = getattr(authority, "_preservation_automation_roots", {})
    for key, candidate in list(roots.items()):
        if candidate is handle:
            roots.pop(key, None)
    return True


def mark_unknown(tick: TickLease | None, reason: str = "join_failed") -> None:
    if tick is None:
        return
    coordinator = tick.authority._preservation_coordinator()
    if coordinator is not None:
        coordinator.mark_unknown(tick.handle, reason)


def finish_unbound(tick: TickLease | None, *, success: bool) -> bool:
    """Finish a no-child decision only after its source-owned durable decision returns."""
    if tick is None or _bound_admission(tick.authority, tick.handle) is not None:
        return False
    if success:
        return release_unaccepted(tick, reason="no_child")
    mark_unknown(tick, "join_failed")
    return False


def _payload_owner(payload):
    if not isinstance(payload, dict):
        return None
    for value in payload.values():
        if not isinstance(value, dict):
            continue
        custody = value.get("custody")
        if isinstance(custody, dict) and custody.get("owner_class") == I08_OWNER:
            return I08_OWNER
        found = _payload_owner(value)
        if found is not None:
            return found
    return None


def _durable_i08_pending(authority) -> bool:
    db = getattr(authority, "db", None)
    read_ctx = getattr(db, "_read_ctx", None)
    if not callable(read_ctx):
        return True
    try:
        with read_ctx() as conn:
            rows = conn.execute(
                "SELECT status, payload FROM session_admissions "
                "WHERE status IN ('queued','started','unknown')"
            ).fetchall()
    except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return True
    for status, payload in rows:
        try:
            decoded = json.loads(payload) if isinstance(payload, str) else payload
        except (TypeError, ValueError):
            return True
        if _payload_owner(decoded) == I08_OWNER:
            return True
    return False


def owner_ready(authority, *, close_generation: int | None = None) -> bool:
    """Return close readiness only from the source registry and canonical ledger/readback."""
    state = getattr(authority, "_i08_source_state", None)
    if not isinstance(state, dict) or state.get("seen") is not True:
        return False
    for task in tuple(state.get("tasks", ())):
        if task is not None and not task.done():
            return False
    for item in state.get("watchers", {}).values():
        if isinstance(item, dict) and not item.get("closed"):
            return False
    coordinator = authority._preservation_coordinator()
    if coordinator is None:
        return False
    custody = coordinator.status()
    expected_generation = custody.get("close_generation")
    if (custody.get("state") != "closing" or type(expected_generation) is not int
            or (close_generation is not None and close_generation != expected_generation)):
        return False
    if state.get("late_registration"):
        return False
    # The goal hook runs after the managed I04 turn has produced its response but before the
    # turn's source-owned post-turn hook reserves the I08 child.  A source that has already been
    # observed may therefore have no I08 handle yet while the canonical I04 parent is still live.
    # Keep I08 unwired until that exact coordinator owner settles; otherwise a close could seal the
    # goal source and make the legitimate child reservation fail as a late registration.
    active_owner_classes = custody.get("active_owner_classes", ())
    unknown_owner_classes = custody.get("unknown_owner_classes", ())
    if (
        "I04_fifo_turn_execution" in active_owner_classes
        or "I04_fifo_turn_execution" in unknown_owner_classes
    ):
        return False
    if I08_OWNER in active_owner_classes:
        return False
    if I08_OWNER in unknown_owner_classes:
        return False
    if any(getattr(item, "owner_class", None) == I08_OWNER
           for item in getattr(authority, "_preservation_handles", {}).values()):
        return False
    if any(getattr(item, "owner_class", None) == I08_OWNER
           for item in getattr(authority, "_preservation_children", {}).values()):
        return False
    for pending in getattr(authority, "_preservation_pending_roots", {}).values():
        if isinstance(pending, dict) and pending.get("owner_class") == I08_OWNER:
            return False
    for record in getattr(authority, "_preservation_operations", {}).values():
        if record.get("owner_class") == I08_OWNER:
            return False
    return not _durable_i08_pending(authority)


def source_owner_evidence(authority, *, process_instance, authority_epoch,
                          close_generation, owner_id):
    coordinator = authority._preservation_coordinator() if authority is not None else None
    if coordinator is None:
        return None
    custody = coordinator.status()
    if (type(process_instance) is not str
            or process_instance != custody.get("process_instance")
            or type(authority_epoch) is not int
            or authority_epoch != custody.get("authority_epoch")
            or type(close_generation) is not int
            or close_generation != custody.get("close_generation")
            or not owner_ready(authority, close_generation=close_generation)):
        return None
    from gateway.runtime_custody import OWNER_SPECS

    state = getattr(authority, "_i08_source_state", {})
    return {
        "source": OWNER_SPECS[I08_OWNER].source,
        "process_instance": process_instance,
        "authority_epoch": authority_epoch,
        "close_generation": close_generation,
        "owner_id": owner_id,
        "observed": True,
        "source_seen": True,
        "source_kinds": tuple(sorted(state.get("sources", ()))),
    }
