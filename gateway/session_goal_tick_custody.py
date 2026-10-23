"""Source-owned custody helpers for goal, loop, and heartbeat producers.

The runtime coordinator remains the only lease ledger.  This module only keeps the
source's watcher task references and returns typed views over coordinator handles;
it never creates a second admission or completion store.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
import json
from pathlib import Path
import sqlite3
from typing import Any

from hermes_state_runtime import RuntimeStoreError

I08_OWNER = "I08_goals_loops_heartbeat"


@dataclass(frozen=True)
class I08ProducerGraph:
    """Identity-bound registration for every real I08 source family.

    The coordinator remains the only lease ledger.  This graph records the existing authority
    state, runner watcher set, producer entrypoints, and profile/DB identity so an untouched
    source can prove zero work without treating an absent state dict or completed task bit as
    evidence.  Executor futures are tracked separately in the source state because a cancelled
    asyncio watcher does not cancel a native thread already running in its executor.
    """

    runner: Any
    authority: Any
    coordinator: Any
    profile_id: str
    instance_id: str
    authority_epoch: int
    database: Any
    database_path: str
    source_state: dict
    watcher_tasks: set
    producer_families: tuple[tuple[str, Any], ...]
    # The profile owner is captured at the source/coordinator bind.  Close-time reads may
    # revalidate this physical token, but must never bless a later lock incarnation.
    physical_owner_epoch: str | None = None
    native_descriptor: int | None = None
    physical_binding: tuple[str, str, int] | None = None

    def matches(self, runner, authority, coordinator) -> bool:
        if (
            self.runner is not runner
            or self.authority is not authority
            or self.coordinator is not coordinator
            or getattr(runner, "runtime_custody", None) is not coordinator
            or getattr(authority, "profile_id", None) != self.profile_id
            or getattr(authority, "instance_id", None) != self.instance_id
            or getattr(authority, "epoch", None) != self.authority_epoch
            or getattr(authority, "db", None) is not self.database
            or str(Path(getattr(getattr(authority, "db", None), "db_path", "")).resolve())
            != self.database_path
            or getattr(runner, "_i08_watcher_tasks", None) is not self.watcher_tasks
            or getattr(authority, "_i08_source_state", None) is not self.source_state
            or getattr(coordinator, "process_instance", None) != self.instance_id
        ):
            return False
        if self.coordinator is not None and (
            type(self.physical_owner_epoch) is not str
            or not self.physical_owner_epoch
            or type(self.native_descriptor) is not int
            or isinstance(self.native_descriptor, bool)
            or not isinstance(self.physical_binding, tuple)
            or self.physical_binding != (
                str(Path(self.profile_id).resolve()),
                self.physical_owner_epoch, self.native_descriptor,
            )
        ):
            return False
        return self.producer_families == _i08_producer_families()


def _i08_producer_families() -> tuple[tuple[str, Any], ...]:
    """Resolve the actual goal, heartbeat, loop, and typed reservation entrypoints."""
    from gateway.run_goals import GatewayGoalsMixin
    from gateway.run_heartbeat_acceptance import admit_heartbeat
    from gateway.session_goal_tick_runtime import reserve_goal_tick

    return (
        ("heartbeat_watch", GatewayGoalsMixin._register_heartbeat_watch),
        ("heartbeat_admission", admit_heartbeat),
        ("loop_watcher", GatewayGoalsMixin._loop_wakeup_watcher),
        ("goal_tick", reserve_goal_tick),
        ("typed_tick", reserve_tick),
    )


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
    state = getattr(authority, "_i08_source_state", None)
    if isinstance(state, dict) and state.get("phase", "open") != "open":
        return False
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
            "task_history": {},
            "task_families": set(),
            "executor_tasks": set(),
            "executor_history": {},
            "phase": "open",
            "close_generation": None,
            "close_tasks": (),
        }
        authority._i08_source_state = state
    else:
        # Older hand-built authorities may already have the source state.  Add only the typed
        # source-owned fields; do not infer readiness from the migration itself.
        state.setdefault("sources", set())
        state.setdefault("watchers", {})
        state.setdefault("tasks", set())
        state.setdefault("task_history", {})
        state.setdefault("task_families", set())
        state.setdefault("executor_tasks", set())
        state.setdefault("executor_history", {})
        state.setdefault("phase", "open")
        state.setdefault("close_generation", None)
        state.setdefault("close_tasks", ())
    return state


def register_i08_source_graph(runner, authority, coordinator=None) -> I08ProducerGraph:
    """Bind one served profile to the existing I08 source indexes before source effects."""
    if coordinator is None:
        coordinator = getattr(runner, "runtime_custody", None)
    database = getattr(authority, "db", None)
    database_path = getattr(database, "db_path", None)
    profile_id = getattr(authority, "profile_id", None)
    instance_id = getattr(authority, "instance_id", None)
    epoch = getattr(authority, "epoch", None)
    if (
        coordinator is None
        or getattr(runner, "runtime_custody", None) is not coordinator
        or database is None
        or not isinstance(database_path, (str, Path))
        or not isinstance(profile_id, str)
        or not isinstance(instance_id, str)
        or type(epoch) is not int
        or epoch < 1
        or getattr(coordinator, "process_instance", None) != instance_id
    ):
        raise RuntimeStoreError("i08_source_graph_invalid")
    watcher_tasks = getattr(runner, "_i08_watcher_tasks", None)
    if watcher_tasks is None:
        watcher_tasks = runner._i08_watcher_tasks = set()
    if type(watcher_tasks) is not set:
        raise RuntimeStoreError("i08_source_graph_invalid")
    state = _state(authority)
    graph = getattr(authority, "_i08_source_graph", None)
    if graph is not None:
        if not isinstance(graph, I08ProducerGraph) or graph.authority is not authority:
            raise RuntimeStoreError("i08_source_graph_invalid")
        if (
            graph.coordinator is coordinator
            and graph.physical_owner_epoch is None
            and graph.native_descriptor is None
            and graph.physical_binding is None
        ):
            # Legacy embedded authorities may not hold a process-owned profile lock.  Preserve
            # their existing source/admission behavior, but leave close readiness UNKNOWN.
            return graph
        if not graph.matches(runner, authority, coordinator):
            raise RuntimeStoreError("i08_source_graph_binding_mismatch")
        return graph
    home = Path(profile_id).resolve()
    reservation = _i08_native_reservation(home)
    graph = I08ProducerGraph(
        runner=runner,
        authority=authority,
        coordinator=coordinator,
        profile_id=profile_id,
        instance_id=instance_id,
        authority_epoch=epoch,
        database=database,
        database_path=str(Path(database_path).resolve()),
        source_state=state,
        watcher_tasks=watcher_tasks,
        producer_families=_i08_producer_families(),
        physical_owner_epoch=(reservation.get("owner_epoch") if reservation is not None else None),
        native_descriptor=(reservation.get("native_descriptor") if reservation is not None else None),
        physical_binding=(reservation.get("physical_binding") if reservation is not None else None),
    )
    authority._i08_source_graph = graph
    return graph


def _i08_graph(runner, authority, coordinator):
    graph = getattr(authority, "_i08_source_graph", None)
    if not isinstance(graph, I08ProducerGraph):
        return None
    try:
        return graph if graph.matches(runner, authority, coordinator) else None
    except (AttributeError, TypeError, ValueError):
        return None


def has_i08_source_graph(runner) -> bool:
    """Return whether this runtime has the authenticated per-profile I08 graph."""
    authorities = _i08_authorities(runner)
    if not authorities:
        return False
    coordinator = getattr(runner, "runtime_custody", None)
    return all(_i08_graph(runner, authority, coordinator) is not None for authority in authorities)


def i08_close_active(runner, close_generation: int) -> bool:
    """Return whether this exact source close has fenced at least one I08 authority."""
    if not has_i08_source_graph(runner) or type(close_generation) is not int:
        return False
    return any(
        isinstance(state := getattr(authority, "_i08_source_state", None), dict)
        and state.get("close_generation") == close_generation
        and state.get("phase") in {"closing", "closed", "resuming"}
        for authority in _i08_authorities(runner)
    )


def _i08_authorities(runner, authority=None) -> list:
    if authority is not None:
        return [authority]
    try:
        from gateway.session_authorities import all_authorities
        return [item for item in all_authorities(runner) if item is not None]
    except (AttributeError, TypeError, ValueError):
        active = getattr(runner, "session_authority", None)
        return [active] if active is not None else []


def _record_task_state(state: dict, task, *, family: str, executor: bool) -> None:
    history_name = "executor_history" if executor else "task_history"
    tasks_name = "executor_tasks" if executor else "tasks"
    history = state[history_name]
    record = history.setdefault(id(task), {
        "task": task, "family": family, "state": "running",
    })
    record["task"] = task
    record["family"] = family
    record["state"] = "running"
    state[tasks_name].add(task)

    def _done(done_task):
        current = history.get(id(done_task))
        if current is None:
            return
        if done_task.cancelled():
            current["state"] = "cancelled"
        else:
            try:
                current["state"] = "failed" if done_task.exception() is not None else "done"
            except (asyncio.InvalidStateError, RuntimeError):
                current["state"] = "unknown"
        state[tasks_name].discard(done_task)

    task.add_done_callback(_done)


def register_runner_watcher(
    runner, kind: str, task, *, resume_generation: int | None = None,
) -> None:
    """Attach one real watcher to every served authority, including multiplexed secondaries."""
    if task is None:
        return
    tasks = getattr(runner, "_i08_watcher_tasks", None)
    if tasks is None:
        tasks = runner._i08_watcher_tasks = set()
    if type(tasks) is not set:
        return
    tasks.add(task)
    task.add_done_callback(tasks.discard)
    for authority in _i08_authorities(runner):
        coordinator = getattr(authority, "_preservation_coordinator", lambda: None)()
        graph = _i08_graph(runner, authority, coordinator)
        state = _state(authority)
        resuming = resume_generation is not None
        if resuming:
            status = coordinator.status() if coordinator is not None else {}
            if graph is None or state.get("close_generation") != resume_generation:
                state["late_registration"] = True
                continue
            if status.get("state") == "closing" and status.get("close_generation") != resume_generation:
                state["late_registration"] = True
                continue
            if status.get("state") not in {"closing", "open"}:
                state["late_registration"] = True
                continue
        if not resuming and not _source_intake_open(authority):
            mark_late_registration(authority)
            continue
        mark_source_observed(authority, f"watcher-task:{kind}")
        state["task_families"].add(kind)
        _record_task_state(state, task, family=kind, executor=False)


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
                "SELECT status, payload_json FROM session_admissions "
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


def _i08_runtime_epoch_matches(authority) -> bool:
    try:
        with authority.db._read_ctx() as conn:
            row = conn.execute(
                "SELECT epoch, instance_id FROM runtime_epoch WHERE singleton=1"
            ).fetchone()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return False
    return (
        row is not None
        and type(row[0]) is int
        and row[0] == authority.epoch
        and row[1] == authority.instance_id
    )


def _i08_native_reservation(home):
    """Read the retained descriptor-bound native owner witness for one profile."""
    try:
        from gateway.runtime_ownership import process_ownership
        reservation = process_ownership.reservation_evidence(home)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if (
        not isinstance(reservation, dict)
        or reservation.get("profile_home") != str(home)
        or type(reservation.get("owner_epoch")) is not str
        or not reservation.get("owner_epoch")
        or reservation.get("observed") is not True
        or reservation.get("native_handle_retained") is not True
        or reservation.get("os_lock_owner") is not True
        or reservation.get("cross_process_proof") is not True
        or type(reservation.get("native_descriptor")) is not int
        or isinstance(reservation.get("native_descriptor"), bool)
        or reservation.get("physical_binding") != (
            str(home), reservation.get("owner_epoch"), reservation.get("native_descriptor"),
        )
    ):
        return None
    return reservation


def _i08_native_owner_snapshot(authority, coordinator):
    """Read the bound profile/native owner and reject a replacement incarnation."""
    runner = getattr(authority, "runner", None)
    graph = _i08_graph(runner, authority, coordinator)
    if graph is None:
        return None
    home = Path(authority.profile_id).resolve()
    if str(Path(authority.db.db_path).resolve().parent) != str(home):
        return None
    if not _i08_runtime_epoch_matches(authority):
        return None
    try:
        scope = coordinator.profile_scope_observation(
            authority.profile_id, authority.epoch,
            require_serving=False, allow_retiring=True,
        )
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return None
    if (
        not getattr(scope, "ok", False)
        or type(getattr(scope, "physical_owner_epoch", None)) is not str
        or not scope.physical_owner_epoch
    ):
        return None
    reservation = _i08_native_reservation(home)
    if reservation is None:
        return None
    if (
        reservation.get("owner_epoch") != graph.physical_owner_epoch
        or reservation.get("native_descriptor") != graph.native_descriptor
        or reservation.get("physical_binding") != graph.physical_binding
        or scope.physical_owner_epoch != graph.physical_owner_epoch
    ):
        return None
    return {
        "profile_home": str(home),
        "profile_id": authority.profile_id,
        "authority_epoch": authority.epoch,
        "process_instance": authority.instance_id,
        "physical_owner_epoch": graph.physical_owner_epoch,
        "database_path": str(Path(authority.db.db_path).resolve()),
        "database_identity": id(authority.db),
        "native_descriptor": graph.native_descriptor,
        "native_handle_retained": True,
        "os_lock_owner": True,
        "cross_process_proof": True,
    }


def _i08_history_terminal(state: dict) -> bool:
    for history_name in ("task_history", "executor_history"):
        for record in state.get(history_name, {}).values():
            status = record.get("state") if isinstance(record, dict) else None
            if status == "running":
                return False
            if status == "failed" or status == "unknown":
                return False
            if status == "cancelled" and not record.get("close_expected"):
                return False
    return True


def _i08_zero_work_snapshot(authority, close_generation: int | None):
    coordinator = authority._preservation_coordinator()
    runner = getattr(authority, "runner", None)
    graph = _i08_graph(runner, authority, coordinator) if runner is not None else None
    state = getattr(authority, "_i08_source_state", None)
    if graph is None or not isinstance(state, dict):
        return None
    status = coordinator.status()
    if (
        state.get("phase") != "closed"
        or status.get("state") != "closing"
        or type(close_generation) is not int
        or status.get("close_generation") != close_generation
        or state.get("close_generation") != close_generation
        or state.get("late_registration")
        or not _i08_history_terminal(state)
        or state.get("tasks")
        or state.get("executor_tasks")
    ):
        return None
    if (
        "I04_fifo_turn_execution" in status.get("active_owner_classes", ())
        or "I04_fifo_turn_execution" in status.get("unknown_owner_classes", ())
        or I08_OWNER in status.get("active_owner_classes", ())
        or I08_OWNER in status.get("unknown_owner_classes", ())
    ):
        return None
    if any(
        isinstance(item, dict) and not item.get("closed")
        for item in state.get("watchers", {}).values()
    ):
        return None
    if any(
        getattr(item, "owner_class", None) == I08_OWNER
        for item in (
            *getattr(authority, "_preservation_handles", {}).values(),
            *getattr(authority, "_preservation_children", {}).values(),
        )
    ):
        return None
    for pending in getattr(authority, "_preservation_pending_roots", {}).values():
        if isinstance(pending, dict) and pending.get("owner_class") == I08_OWNER:
            return None
    for record in getattr(authority, "_preservation_operations", {}).values():
        if isinstance(record, dict) and record.get("owner_class") == I08_OWNER:
            return None
    if _durable_i08_pending(authority):
        return None
    native = _i08_native_owner_snapshot(authority, coordinator)
    if native is None:
        return None
    return {
        "mode": "zero_work",
        "zero_work": True,
        "ingress_closed": True,
        "close_generation": close_generation,
        "profile_id": authority.profile_id,
        "authority_epoch": authority.epoch,
        "producer_families": tuple(name for name, _ in graph.producer_families),
        "watcher_families": tuple(sorted(state.get("task_families", ()))),
        "settled_watcher_count": len(state.get("task_history", {})),
        "settled_executor_count": len(state.get("executor_history", {})),
        "pending_admissions": 0,
        **native,
    }


async def run_tracked_executor(runner, func, *args, authority=None, kind="executor"):
    """Run source work while retaining the real executor future through watcher cancellation."""
    task = asyncio.create_task(runner._run_in_executor_with_context(func, *args))
    for target in _i08_authorities(runner, authority):
        _record_task_state(_state(target), task, family=kind, executor=True)
    return await asyncio.shield(task)


async def _await_i08_tasks(tasks, deadline: float | None) -> bool:
    pending = {task for task in tasks if task is not None and not task.done()}
    if deadline is None:
        return False
    while pending:
        left = deadline - asyncio.get_running_loop().time()
        if left <= 0:
            return False
        _done, pending = await asyncio.wait(pending, timeout=left)
    return True


async def begin_i08_source_close(runner, close_generation: int, remaining: float | None) -> bool:
    """Fence and join I08 watcher/future work without touching accepted coordinator leases."""
    authorities = _i08_authorities(runner)
    if (
        not authorities
        or type(close_generation) is not int
        or type(remaining) not in (int, float)
        or isinstance(remaining, bool)
        or remaining <= 0
    ):
        return False
    deadline = asyncio.get_running_loop().time() + float(remaining)
    graphs = []
    for authority in authorities:
        coordinator = authority._preservation_coordinator()
        graph = _i08_graph(runner, authority, coordinator)
        state = _state(authority)
        if graph is None or state.get("late_registration"):
            return False
        if state.get("phase") == "closed" and state.get("close_generation") == close_generation:
            graphs.append((authority, state, graph))
            continue
        if state.get("phase") not in {"open", "closing", "resuming"}:
            return False
        state["phase"] = "closing"
        state["close_generation"] = close_generation
        graphs.append((authority, state, graph))

    watcher_tasks = set(getattr(runner, "_i08_watcher_tasks", ()))
    for _authority, state, _graph in graphs:
        watcher_tasks.update(state.get("tasks", ()))
        for task in state.get("tasks", ()):
            record = state.get("task_history", {}).get(id(task))
            if isinstance(record, dict):
                record["close_expected"] = True
        state["close_tasks"] = tuple(watcher_tasks)
    for task in watcher_tasks:
        if task is not None and not task.done():
            task.cancel()
    if not await _await_i08_tasks(watcher_tasks, deadline):
        return False
    executor_tasks = set()
    for _authority, state, _graph in graphs:
        executor_tasks.update(state.get("executor_tasks", ()))
    if not await _await_i08_tasks(executor_tasks, deadline):
        return False
    for _authority, state, _graph in graphs:
        if state.get("tasks") or state.get("executor_tasks") or not _i08_history_terminal(state):
            return False
        state["phase"] = "closed"
    return True


async def resume_i08_source_close(runner, close_generation: int, remaining: float | None) -> bool:
    """Rebind only the watcher families that existed before this exact close generation."""
    authorities = _i08_authorities(runner)
    if (
        not authorities
        or type(close_generation) is not int
        or type(remaining) not in (int, float)
        or isinstance(remaining, bool)
        or remaining <= 0
    ):
        return False
    deadline = asyncio.get_running_loop().time() + float(remaining)
    coordinator = getattr(runner, "runtime_custody", None)
    if coordinator is None:
        return False
    families = set()
    for authority in authorities:
        graph = _i08_graph(runner, authority, coordinator)
        state = _state(authority)
        if graph is None or state.get("phase") not in {"closed", "closing", "resuming"}:
            return False
        coordinator_status = coordinator.status()
        if coordinator_status.get("state") == "closing":
            generation_matches = coordinator_status.get("close_generation") == close_generation
        else:
            # The coordinator is reopened before source tasks are restarted.  The source state
            # retains the exact close generation, so a stale receipt cannot revive a newer graph.
            generation_matches = state.get("close_generation") == close_generation
        if not generation_matches:
            return False
        state["phase"] = "resuming"
        families.update(state.get("task_families", ()))
    try:
        if asyncio.get_running_loop().time() >= deadline:
            return False
        if "heartbeat" in families:
            runner._start_heartbeat_poller(_i08_resume_generation=close_generation)
            if asyncio.get_running_loop().time() >= deadline:
                return False
        if "loop" in families:
            runner._spawn_supervised(
                lambda: runner._loop_wakeup_watcher(_i08_resume_generation=close_generation),
                "loop_wakeup_watcher", restart=True,
            )
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return False
    await asyncio.sleep(0)
    if asyncio.get_running_loop().time() >= deadline:
        return False
    for authority in authorities:
        state = _state(authority)
        if any(
            family not in state.get("task_families", ())
            or not any(
                isinstance(record, dict)
                and record.get("family") == family
                and record.get("state") == "running"
                for record in state.get("task_history", {}).values()
            )
            for family in families
        ):
            return False
    return True


def complete_i08_source_resume(runner, close_generation: int) -> bool:
    """Finish an exact abort rebind only after the coordinator has reopened."""
    coordinator = getattr(runner, "runtime_custody", None)
    if coordinator is None or coordinator.status().get("state") != "open":
        return False
    for authority in _i08_authorities(runner):
        if _i08_graph(runner, authority, coordinator) is None:
            return False
        state = _state(authority)
        if state.get("phase") != "resuming" or state.get("close_generation") != close_generation:
            return False
        state["phase"] = "open"
        state["close_generation"] = None
        state["close_tasks"] = ()
        state.pop("late_registration", None)
    return True


def owner_ready(authority, *, close_generation: int | None = None) -> bool:
    """Return close readiness only from the source registry and canonical ledger/readback."""
    state = getattr(authority, "_i08_source_state", None)
    if not isinstance(state, dict):
        return False
    # Authenticated runtime bootstrap may prove an untouched source without manufacturing a goal
    # or heartbeat.  Once that graph exists, the typed zero-work snapshot is the only readiness
    # path; the legacy observed-source path remains for hand-built single-profile active tests.
    runner = getattr(authority, "runner", None)
    coordinator = authority._preservation_coordinator()
    if _i08_graph(runner, authority, coordinator) is not None:
        return _i08_zero_work_snapshot(authority, close_generation) is not None
    if state.get("seen") is not True:
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
    snapshot = _i08_zero_work_snapshot(authority, close_generation)
    if getattr(authority, "_i08_source_graph", None) is not None and snapshot is None:
        return None
    return {
        "source": OWNER_SPECS[I08_OWNER].source,
        "process_instance": process_instance,
        "authority_epoch": authority_epoch,
        "close_generation": close_generation,
        "owner_id": owner_id,
        "observed": True,
        "source_seen": True,
        "source_kinds": tuple(sorted(state.get("sources", ()))),
        "source_snapshot": snapshot,
    }


@dataclass(frozen=True)
class I08AbortCapability:
    """Exact I08 participant retained across the coordinator's reversible OPEN transition."""

    runner: Any
    coordinator: Any
    receipt: Any
    graphs: tuple[I08ProducerGraph, ...]
    states: tuple[dict, ...]
    close_generation: int


def _same_i08_receipt(left, right) -> bool:
    return all(
        getattr(left, field, None) == getattr(right, field, None)
        for field in ("receipt_id", "process_instance", "authority_epoch", "close_generation")
    ) and left is not None and right is not None


def prepare_i08_abort(runner, coordinator, receipt) -> I08AbortCapability | None:
    """Capture the current I08 graph after source resume, before coordinator OPEN."""
    existing = getattr(runner, "_i08_post_open_capability", None)
    if isinstance(existing, I08AbortCapability):
        return existing if _same_i08_receipt(existing.receipt, receipt) else None
    if (
        getattr(coordinator, "state", None) != "closing"
        or not callable(getattr(coordinator, "_check_close_identity", None))
        or not coordinator._check_close_identity(receipt)
    ):
        return None
    graphs = []
    states = []
    for authority in _i08_authorities(runner):
        graph = _i08_graph(runner, authority, coordinator)
        state = getattr(authority, "_i08_source_state", None)
        if graph is None or not isinstance(state, dict):
            return None
        if state.get("phase") not in {"resuming", "open"}:
            return None
        if state.get("phase") == "resuming" and state.get("close_generation") != receipt.close_generation:
            return None
        graphs.append(graph)
        states.append(state)
    if not graphs:
        return None
    capability = I08AbortCapability(
        runner=runner, coordinator=coordinator, receipt=receipt,
        graphs=tuple(graphs), states=tuple(states), close_generation=receipt.close_generation,
    )
    runner._i08_post_open_capability = capability
    return capability


def i08_abort_participant(capability: I08AbortCapability):
    return (capability.runner, capability.receipt, capability)


def validate_i08_abort_participant(runner, coordinator, transition, entry) -> bool:
    if type(entry) is not tuple or len(entry) != 3:
        return False
    source, receipt, capability = entry
    if (
        not isinstance(capability, I08AbortCapability)
        or source is not capability.runner
        or capability.coordinator is not coordinator
        or capability.receipt is not receipt
        or capability.runner is not runner
        or not _same_i08_receipt(capability.receipt, getattr(transition, "receipt", None))
        or getattr(transition, "close_generation", None) != capability.close_generation
    ):
        return False
    current = getattr(runner, "_i08_post_open_capability", None)
    if current is not capability:
        return False
    for graph, state in zip(capability.graphs, capability.states):
        authority = graph.authority
        if _i08_graph(runner, authority, coordinator) is not graph:
            return False
        if getattr(authority, "_i08_source_state", None) is not state:
            return False
        if state.get("phase") == "resuming":
            if state.get("close_generation") != capability.close_generation:
                return False
        elif state.get("phase") != "open" or state.get("close_generation") is not None:
            return False
    return True


def complete_i08_abort(capability: I08AbortCapability, transition) -> bool:
    """Settle I08 only after coordinator OPEN; retain the exact capability on failure."""
    if not isinstance(capability, I08AbortCapability):
        return False
    coordinator = capability.coordinator
    if getattr(coordinator, "state", None) != "open":
        return False
    if not callable(getattr(coordinator, "check_post_open_transition", None)):
        return False
    if not coordinator.check_post_open_transition(transition):
        return False
    if all(
        state.get("phase") == "open" and state.get("close_generation") is None
        for state in capability.states
    ):
        return True
    if not complete_i08_source_resume(capability.runner, capability.close_generation):
        return False
    return all(
        state.get("phase") == "open" and state.get("close_generation") is None
        for state in capability.states
    )


def clear_i08_abort(capability: I08AbortCapability, transition) -> bool:
    if not isinstance(capability, I08AbortCapability):
        return False
    if getattr(capability.runner, "_i08_post_open_capability", None) is not capability:
        return False
    if not capability.coordinator.check_post_open_transition(transition):
        # The coordinator may already have consumed this exact token; callers use this only after
        # their own coordinator-completion check, so retain a strict identity fallback.
        completed = getattr(capability.coordinator, "completed_post_open_transition_for", None)
        if not callable(completed) or completed(capability.receipt) is None:
            return False
    capability.runner._i08_post_open_capability = None
    return True
