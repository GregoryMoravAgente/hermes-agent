"""Private proposal: public I12 scheduler producers with the managed owner runtime.

This file is intentionally additive.  The existing cron custody, occurrence, and identity
files remain the negative/order controls; these two cases supply the missing producer-to-I04
consumer and producer-to-script-child paths on one source-owned runtime.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import threading
import weakref
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path

import pytest


def _i13_digest(value):
    if value is None:
        return None
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:16]


def _i13_text(value):
    if value is None:
        return None
    if isinstance(value, str):
        return value if len(value) <= 80 else value[:79] + "…"
    return f"<{type(value).__name__}>"


def _i13_identity_equal(left, right):
    return left is not None and right is not None and type(left) is type(right) and left == right


def _i13_dispatch(value):
    if value is None:
        return "unavailable"
    if isinstance(value, str):
        return value if value in {"scoped", "degraded", "in_process"} else "other"
    return f"error:{type(value).__name__}"


def _i13_outcome(value):
    if value is True or value is False:
        return value
    if value is None:
        return "unavailable"
    return f"error:{type(value).__name__}"


def _i13_reconcile(value):
    if value in {"not_applicable", "unadopted", "terminalized", "unknown"}:
        return value
    if value is None:
        return "unavailable"
    return "other"


def _i13_process(process):
    if process is None:
        return {"pid": None, "returncode": None}
    pid = getattr(process, "pid", None)
    try:
        returncode = process.poll()
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
        returncode = f"error:{type(exc).__name__}"
    if returncode is not None and type(returncode) is not int:
        returncode = f"error:{type(returncode).__name__}"
    return {"pid": pid if type(pid) is int else None, "returncode": returncode}


def _i13_row(row, execution_id, job_id, acknowledgement):
    if row is None:
        return {"read": "missing"}
    if not isinstance(row, dict):
        return {"read": f"error:{type(row).__name__}"}
    ack = acknowledgement if isinstance(acknowledgement, dict) else {}
    pid = row.get("pid") if type(row.get("pid")) is int else None
    started = row.get("process_started_at") if type(row.get("process_started_at")) is int else None
    ack_pid = ack.get("pid") if type(ack.get("pid")) is int else None
    ack_started = (
        ack.get("process_started_at") if type(ack.get("process_started_at")) is int else None
    )
    status = row.get("status")
    return {
        "read": "ok",
        "id_digest": _i13_digest(row.get("id")),
        "id_eq": _i13_identity_equal(row.get("id"), execution_id),
        "job_id_digest": _i13_digest(row.get("job_id")),
        "job_id_eq": _i13_identity_equal(row.get("job_id"), job_id),
        "source_builtin": row.get("source") == "builtin",
        "status": status if status in {"claimed", "running", "completed", "failed", "unknown"} else "other",
        "handoff_pending": row.get("handoff_pending") if type(row.get("handoff_pending")) is bool else "other",
        "pid": pid,
        "process_started_at": started,
        "ack_pid_eq": _i13_identity_equal(pid, ack_pid),
        "ack_start_eq": _i13_identity_equal(started, ack_started),
    }


def _i13_binding(binding):
    try:
        child = getattr(binding, "child", None)
        process = getattr(child, "_process", None) if child is not None else None
        process_state = _i13_process(process)
        reaper = getattr(child, "_reaper", None) if child is not None else None
        try:
            reaper_alive = _i13_outcome(reaper.is_alive()) if reaper is not None else None
        except Exception as exc:
            reaper_alive = f"error:{type(exc).__name__}"
        return {
            "owner_class": _i13_text(getattr(binding, "owner_class", None)),
            "execution_id_digest": _i13_digest(getattr(binding, "execution_id", None)),
            "settled": getattr(binding, "settled", None),
            "unknown": getattr(binding, "_unknown", None),
            "child_owner_class": _i13_text(getattr(child, "owner_class", None)),
            "child_settled": getattr(child, "settled", None) if child is not None else None,
            "child_unknown": getattr(child, "_unknown", None) if child is not None else None,
            "bound_pid": getattr(child, "_bound_process_pid", None) if child is not None else None,
            "bound_started_at": getattr(child, "_bound_process_started_at", None) if child is not None else None,
            "worker_pid": getattr(child, "_worker_pid", None) if child is not None else None,
            "worker_started_at": getattr(child, "_worker_started_at", None) if child is not None else None,
            "process_pid": process_state["pid"],
            "process_returncode": process_state["returncode"],
            "native_job_present": getattr(child, "_native_job", None) is not None if child is not None else False,
            "native_job_proven": getattr(child, "_native_job_proven", None) if child is not None else None,
            "reaper_alive": reaper_alive,
        }
    except Exception as exc:
        return {"read": f"error:{type(exc).__name__}"}


def _install_i13_observer(monkeypatch, request):
    """Record scalar results at existing I13 seams; every wrapper calls the source once."""
    from cron import executions, runtime_custody, scheduler

    captured = []
    events = []
    event_lock = threading.Lock()
    thread_scope = threading.local()
    job_context = {}
    event_state = {"truncated": False}

    def record(kind, **fields):
        with event_lock:
            if len(events) >= 63:
                if not event_state["truncated"]:
                    events.append({"event": "truncated", "dropped": True})
                    event_state["truncated"] = True
                return None
            entry = {"event": kind, **fields}
            events.append(entry)
            return entry

    def current_scope():
        return getattr(thread_scope, "context", None)

    def context_fields(context):
        if context is None:
            return {"context": "unavailable"}
        return {
            "phase": context.get("phase"),
            "owner_execution_id_digest": context.get("execution_id_digest"),
            "owner_job_id_digest": context.get("job_id_digest"),
            "process_pid": context.get("process_pid"),
            "owner_process_started_at": context.get("process_started_at"),
            "job_token": context.get("job_token"),
        }

    def job_scope(native_job):
        if native_job is None:
            return None
        with event_lock:
            saved = job_context.get(id(native_job))
        if saved is None:
            return None
        reference, context = saved
        try:
            if reference() is not native_job:
                return None
        except Exception:
            return None
        return dict(context)

    def restore_scope(previous):
        if previous is None:
            try:
                del thread_scope.context
            except AttributeError:
                pass
        else:
            thread_scope.context = previous

    reserve = runtime_custody.reserve_execution
    bind = runtime_custody.CronOwnerBinding.bind_process
    membership = runtime_custody.CronOwnerBinding._native_job_contains_process
    empty = runtime_custody.CronOwnerBinding._native_job_is_empty
    activate = runtime_custody.CronOwnerBinding.activate_external_worker
    reconcile = runtime_custody.CronOwnerBinding.reconcile_unacknowledged
    get_execution = executions.get_execution
    scheduler_get_execution = scheduler.get_execution
    wait_body = scheduler._wait_for_external_cron_worker_body

    @wraps(reserve)
    def observe_reserve(*args, **kwargs):
        binding = reserve(*args, **kwargs)
        if kwargs.get("owner_class") == "I12_cron_fire" and binding is not None:
            with event_lock:
                captured.append(binding)
        return binding

    @wraps(membership)
    def observe_membership(native_job, process):
        context = current_scope() or job_scope(native_job)
        fields = context_fields(context)
        if fields.get("process_pid") is None:
            fields["process_pid"] = _i13_process(process)["pid"]
        try:
            result = membership(native_job, process)
        except Exception as exc:
            entry = record("job_membership", value=f"error:{type(exc).__name__}", **fields)
            if current_scope() is context and entry is not None:
                context.setdefault("event_refs", []).append(entry)
            raise
        entry = record("job_membership", value=_i13_outcome(result), **fields)
        if current_scope() is context and entry is not None:
            context.setdefault("event_refs", []).append(entry)
        return result

    @wraps(empty)
    def observe_empty(native_job):
        context = current_scope() or job_scope(native_job)
        try:
            result = empty(native_job)
        except Exception as exc:
            record("job_empty", value=f"error:{type(exc).__name__}", **context_fields(context))
            raise
        record("job_empty", value=_i13_outcome(result), **context_fields(context))
        return result

    @wraps(bind)
    def observe_bind(binding, process, *, native_job=None):
        p = _i13_process(process)
        previous = current_scope()
        context = {
            "phase": "bind",
            "execution_id_digest": _i13_digest(getattr(binding, "execution_id", None)),
            "job_id_digest": _i13_digest(getattr(binding, "job_id", None)),
            "job_id": getattr(binding, "job_id", None),
            "process_pid": p["pid"],
            "process_started_at": None,
            "job_token": _i13_digest(id(native_job)) if native_job is not None else None,
            "event_refs": [],
        }
        thread_scope.context = context
        bound = False
        try:
            result = bind(binding, process, native_job=native_job)
            bound = True
            if binding.owner_class == "I13_external_cron_worker":
                record("bind", **context_fields(context), native_job_present=native_job is not None,
                       native_job_proven=binding._native_job_proven)
            return result
        finally:
            started = getattr(binding, "_bound_process_started_at", None)
            context["process_started_at"] = started if type(started) is int else None
            with event_lock:
                for entry in context["event_refs"]:
                    entry["owner_process_started_at"] = context["process_started_at"]
                if bound and native_job is not None:
                    try:
                        reference = weakref.ref(native_job)
                    except TypeError:
                        reference = None
                    if reference is not None:
                        if len(job_context) >= 8:
                            job_context.pop(next(iter(job_context)))
                        job_context[id(native_job)] = (reference, {
                            key: context[key] for key in (
                                "phase", "execution_id_digest", "job_id_digest",
                                "process_pid", "process_started_at", "job_token",
                            )
                        })
            restore_scope(previous)

    def observe_execution_read(original, read_source, execution_id):
        context = current_scope()
        try:
            result = original(execution_id)
        except Exception as exc:
            if context is not None:
                record(
                    "ledger_read",
                    read_source=read_source,
                    read=f"error:{type(exc).__name__}",
                    **context_fields(context),
                )
            raise
        if context is not None:
            record(
                "ledger_read",
                read_source=read_source,
                **_i13_row(
                    result,
                    execution_id,
                    context.get("job_id"),
                    context.get("ack"),
                ),
                **context_fields(context),
            )
        return result

    @wraps(get_execution)
    def observe_get_execution(execution_id):
        return observe_execution_read(get_execution, "executions", execution_id)

    @wraps(scheduler_get_execution)
    def observe_scheduler_get_execution(execution_id):
        return observe_execution_read(scheduler_get_execution, "scheduler_alias", execution_id)

    @wraps(wait_body)
    def observe_wait_body(
        process, *, execution_id, stderr_path=None, custody=None, dispatch_mode=None,
    ):
        previous = current_scope()
        started = getattr(custody, "_worker_started_at", None) if custody is not None else None
        if type(started) is not int:
            started = getattr(custody, "_bound_process_started_at", None) if custody is not None else None
        native_job = getattr(custody, "_native_job", None) if custody is not None else None
        context = {
            "phase": "wait",
            "execution_id_digest": _i13_digest(execution_id),
            "job_id_digest": _i13_digest(getattr(custody, "job_id", None)),
            "job_id": getattr(custody, "job_id", None),
            "process_pid": _i13_process(process)["pid"],
            "process_started_at": started if type(started) is int else None,
            "job_token": _i13_digest(id(native_job)) if native_job is not None else None,
            "ack": None,
        }
        thread_scope.context = context
        try:
            return wait_body(
                process,
                execution_id=execution_id,
                stderr_path=stderr_path,
                custody=custody,
                dispatch_mode=dispatch_mode,
            )
        finally:
            restore_scope(previous)

    @wraps(activate)
    def observe_activate(binding, acknowledgement, process, *, dispatch_mode=None, scope_unit=None):
        previous = current_scope()
        p = _i13_process(process)
        parsed = isinstance(acknowledgement, dict)
        ack = acknowledgement if parsed else {}
        started = ack.get("process_started_at") if type(ack.get("process_started_at")) is int else None
        context = {
            "phase": "activation",
            "execution_id_digest": _i13_digest(getattr(binding, "execution_id", None)),
            "job_id_digest": _i13_digest(getattr(binding, "job_id", None)),
            "job_id": getattr(binding, "job_id", None),
            "process_pid": p["pid"],
            "process_started_at": started,
            "job_token": _i13_digest(id(getattr(binding, "_native_job", None))) if getattr(binding, "_native_job", None) is not None else None,
            "ack": acknowledgement if parsed else None,
        }
        thread_scope.context = context
        record(
            "ack",
            **context_fields(context),
            seen=parsed,
            parse="ok" if parsed else "error:not_dict",
            ack_execution_id_digest=_i13_digest(ack.get("execution_id")),
            ack_execution_id_eq=_i13_identity_equal(ack.get("execution_id"), getattr(binding, "execution_id", None)),
            ack_pid=ack.get("pid") if type(ack.get("pid")) is int else None,
            ack_process_started_at=started,
            dispatch_requested=_i13_dispatch(dispatch_mode),
            dispatch_effective=_i13_dispatch(ack.get("dispatch_mode")),
            dispatch_eq=_i13_identity_equal(ack.get("dispatch_mode"), dispatch_mode),
            scope_unit_eq=_i13_identity_equal(ack.get("scope_unit"), scope_unit),
            control_group_present=parsed and isinstance(ack.get("scope_control_group"), str),
        )
        try:
            result = activate(binding, acknowledgement, process, dispatch_mode=dispatch_mode, scope_unit=scope_unit)
        except Exception as exc:
            code = getattr(exc, "code", None)
            decision = f"refused:{_i13_text(code)}" if code is not None else f"error:{type(exc).__name__}"
            record("activation", decision=decision, **context_fields(context))
            raise
        else:
            record("activation", decision="returned", **context_fields(context))
            return result
        finally:
            restore_scope(previous)

    @wraps(reconcile)
    def observe_reconcile(binding, process=None, *, dispatch_mode=None):
        previous = current_scope()
        p = _i13_process(process)
        child = getattr(binding, "child", None)
        started = getattr(child, "_worker_started_at", None) if child is not None else None
        if type(started) is not int:
            started = getattr(child, "_bound_process_started_at", None) if child is not None else None
        context = {
            "phase": "reconcile",
            "execution_id_digest": _i13_digest(getattr(binding, "execution_id", None)),
            "job_id_digest": _i13_digest(getattr(binding, "job_id", None)),
            "job_id": getattr(binding, "job_id", None),
            "process_pid": p["pid"],
            "process_started_at": started if type(started) is int else None,
            "job_token": _i13_digest(id(getattr(child, "_native_job", None))) if child is not None and getattr(child, "_native_job", None) is not None else None,
            "ack": None,
        }
        thread_scope.context = context
        try:
            result = reconcile(binding, process, dispatch_mode=dispatch_mode)
        except Exception as exc:
            record("reconcile", decision=f"error:{type(exc).__name__}", **context_fields(context))
            raise
        else:
            record("reconcile", decision=_i13_reconcile(result), **context_fields(context))
            return result
        finally:
            restore_scope(previous)

    monkeypatch.setattr(runtime_custody, "reserve_execution", observe_reserve)
    monkeypatch.setattr(runtime_custody.CronOwnerBinding, "bind_process", observe_bind)
    monkeypatch.setattr(runtime_custody.CronOwnerBinding, "_native_job_contains_process", staticmethod(observe_membership))
    monkeypatch.setattr(runtime_custody.CronOwnerBinding, "_native_job_is_empty", staticmethod(observe_empty))
    monkeypatch.setattr(runtime_custody.CronOwnerBinding, "activate_external_worker", observe_activate)
    monkeypatch.setattr(runtime_custody.CronOwnerBinding, "reconcile_unacknowledged", observe_reconcile)
    monkeypatch.setattr(executions, "get_execution", observe_get_execution)
    monkeypatch.setattr(scheduler, "get_execution", observe_scheduler_get_execution)
    monkeypatch.setattr(scheduler, "_wait_for_external_cron_worker_body", observe_wait_body)

    def emit_observation():
        try:
            if sys.platform != "win32":
                return
            with event_lock:
                saved_events = [dict(entry) for entry in events]
                saved_bindings = list(captured)
                truncated = bool(event_state["truncated"])
            try:
                payload = {
                    "case": _i13_text(request.node.name),
                    "events": saved_events,
                    "events_truncated": truncated,
                    "bindings": [_i13_binding(binding) for binding in saved_bindings],
                }
                line = json.dumps(payload, sort_keys=True, separators=(",", ":"))
            except Exception as exc:
                line = json.dumps({
                    "case": "i13-diagnostic",
                    "events": [],
                    "events_truncated": True,
                    "bindings": [],
                    "diagnostic_error": f"error:{type(exc).__name__}",
                }, separators=(",", ":"))
            try:
                print("I13_DIAGNOSTIC_OBSERVATION=" + line)
            except Exception:
                return
        except Exception:
            return

    request.addfinalizer(emit_observation)
    return captured


def _i13_failure_snapshot(binding):
    """Show exact retained worker facts if a positive native handoff stays UNKNOWN."""
    child = binding.child
    if child is None:
        return {"child": None, "parent_unknown": binding._unknown}
    process = child._process
    return {
        "parent_unknown": binding._unknown,
        "child_unknown": child._unknown,
        "child_settled": child.settled,
        "dispatch_mode": child._dispatch_mode,
        "native_job_bound": child._native_job is not None,
        "native_job_proven": child._native_job_proven,
        "process_pid": process.pid if process is not None else None,
        "process_returncode": process.poll() if process is not None else None,
        "bound_pid": child._bound_process_pid,
        "bound_started_at": child._bound_process_started_at,
        "worker_pid": child._worker_pid,
        "worker_started_at": child._worker_started_at,
        "reaper_alive": child._reaper.is_alive() if child._reaper is not None else None,
    }


def _assert_terminal_i12(binding, job, executions, coordinator, owner_classes_before):
    from cron.runtime_custody import CronOwnerBinding

    assert type(binding) is CronOwnerBinding
    assert binding.owner_class == "I12_cron_fire"
    assert binding.execution_id == binding.owner_id
    assert binding.job_id == job["id"]
    assert binding.settled is True, _i13_failure_snapshot(binding)
    record = executions.get_execution(binding.owner_id)
    assert record is not None
    assert record["id"] == binding.owner_id
    assert record["job_id"] == job["id"]
    assert record["source"] == "builtin"
    assert record["status"] in {"completed", "failed"}
    assert record["delivery_outcome"] != "queued"
    active_owner_classes = set(coordinator.status()["active_owner_classes"])
    i12_classes = {"I12_cron_fire", "I13_external_cron_worker"}
    assert active_owner_classes.isdisjoint(i12_classes)
    assert active_owner_classes - i12_classes == set(owner_classes_before) - i12_classes
    return record


async def _await_public_fire_custody_settlement(binding, deadline):
    """Wait for the detached worker's real reaper callback within the test's deadline."""
    from tests.waiting import async_wait_until

    def settled():
        if binding.settled is not True:
            return False
        child = binding.child
        if child is None:
            return True
        if child.settled is not True:
            return False
        reaper = child._reaper
        return (
            reaper is not None
            and reaper.is_alive() is False
            and getattr(reaper, "_cron_reap_succeeded", False) is True
        )

    loop = asyncio.get_running_loop()
    await async_wait_until(
        settled,
        timeout=max(0.0, deadline - loop.time()),
        what="public fire_due custody and native reaper settlement",
    )


def _find_cron_admission(authority, job_id, execution_id):
    from gateway.session_results import admission_result
    from gateway.session_local_recovery import local_identity
    from hermes_state_keys import CRON_PRINCIPAL_ID, cron_request_id
    from hermes_state_runtime import find_session_admission

    request_id = cron_request_id(job_id, execution_id)
    session_id = local_identity(authority.profile_id, CRON_PRINCIPAL_ID, request_id)
    row = find_session_admission(
        authority.db,
        principal_id=CRON_PRINCIPAL_ID,
        session_id=session_id,
        request_id=request_id,
    )
    assert row is not None
    assert row["target_session_id"] == session_id
    saved = admission_result(authority.db, row["admission_id"])
    assert row["status"] == "terminal"
    assert isinstance(saved, dict)
    return row, saved


def _assert_external_worker_physical_ownership(binding):
    """Check the real external-worker topology selected by the source."""
    child = binding.child
    assert child is not None
    from cron.runtime_custody import CronOwnerBinding

    assert type(child) is CronOwnerBinding
    assert child.parent is binding
    assert child.owner_class == "I13_external_cron_worker"
    assert child.owner_id == f"{binding.execution_id}:external-worker"
    assert child.execution_id == binding.execution_id
    assert child.settled is True
    process = child._process
    assert process is not None and process.pid > 0 and process.poll() is not None
    assert child._bound_process_pid == process.pid
    assert type(child._worker_pid) is int and child._worker_pid > 0
    assert type(child._worker_started_at) is int
    # Preserve the direct-child contract; only a degraded Windows trampoline may split the
    # authenticated worker identity from the retained Popen/root identity.
    if child._dispatch_mode != "degraded" or child._worker_pid == process.pid:
        assert child._worker_pid == process.pid
    assert child._reaper is not None
    assert child._reaper.is_alive() is False
    assert getattr(child._reaper, "_cron_reap_succeeded", False) is True
    assert child._dispatch_mode in {"scoped", "degraded"}
    if child._dispatch_mode == "degraded" and child._worker_pid != process.pid:
        # A Windows trampoline split is accepted only with the retained native Job proof.
        assert child._native_job is not None

    if child._dispatch_mode == "scoped":
        proof = child._scope_proof
        assert isinstance(proof, dict)
        assert proof["pid"] == process.pid
        assert proof["process_started_at"] == child._worker_started_at
        assert proof["scope_empty"] is True
    elif child._native_job is not None:
        proof = child._scope_proof
        assert child._native_job_proven is True
        assert isinstance(proof, dict)
        assert proof["kind"] == "windows_job"
        assert proof["pid"] == child._worker_pid
        assert proof["process_started_at"] == child._worker_started_at
        assert proof["root_pid"] == process.pid
        assert proof["native_job_canonical"] is True
        assert proof["scope_empty"] is True
        if child._worker_pid == process.pid:
            # Existing direct Windows Job proof remains an exact root/worker handle proof.
            assert proof["worker_job_membership"] == "retained_root_handle"
        else:
            assert child._dispatch_mode == "degraded"
            assert proof["worker_job_membership"] == "transitive_no_breakaway_ancestry"


@pytest.mark.asyncio
async def test_public_scheduler_tick_runs_managed_agent_and_releases_i12(
    runtime_owned_api_owner, monkeypatch, request,
):
    """A real public tick reaches the owner AIAgent path before I12 finalization."""
    _api, authority = runtime_owned_api_owner
    from cron import executions, jobs, scheduler

    home = Path(authority.db.db_path).resolve().parent
    coordinator = authority.runner.runtime_custody
    owner_classes_before = set(coordinator.status()["active_owner_classes"])
    captured = _install_i13_observer(monkeypatch, request)
    with jobs.use_cron_store(home):
        job = jobs.create_job(
            prompt="Return the managed cron fixture result.",
            schedule=datetime.now(UTC).isoformat(),
            name="managed scheduler agent",
            deliver="local",
        )
        deadline = asyncio.get_running_loop().time() + 90.0
        assert await asyncio.wait_for(
            asyncio.to_thread(scheduler.tick, verbose=False, sync=True),
            timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
        ) == 1
        assert len(captured) == 1
        binding = captured[0]
        from tests.waiting import async_wait_until

        await async_wait_until(
            lambda: binding.settled,
            timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
            what="I12 scheduler custody finalizer",
        )
        record = _assert_terminal_i12(
            binding, job, executions, coordinator, owner_classes_before,
        )
        assert record["delivery_outcome"] == "suppressed"
        _row, saved = _find_cron_admission(authority, job["id"], binding.execution_id)
        assert saved["result"]["completed"] is True
        assert saved["result"]["cron_result"][0] is True
        assert saved["result"]["final_response"] == "runtime-custody-fixture-ok"


@pytest.mark.asyncio
async def test_public_fire_due_runs_real_script_child_and_releases_i12(
    runtime_owned_api_owner, monkeypatch, request,
):
    """The public provider fire_due path owns a real script child through pipe EOF."""
    _api, authority = runtime_owned_api_owner
    from cron import executions, jobs
    from cron.scheduler_provider import InProcessCronScheduler

    home = Path(authority.db.db_path).resolve().parent
    coordinator = authority.runner.runtime_custody
    owner_classes_before = set(coordinator.status()["active_owner_classes"])
    scripts = home / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    script = scripts / "managed-cron-child.py"
    script.write_text(
        "from pathlib import Path\n"
        "marker = str(Path(__file__))\n"
        "Path(marker + '.script-enter').write_text('1')\n"
        "import subprocess\n"
        "import sys\n"
        "child = subprocess.Popen([sys.executable, '-c', "
        "\"import sys; from pathlib import Path; "
        "Path(sys.argv[1] + '.child-enter').write_text('1'); "
        "print('cron-descendant-ok', flush=True)\", marker])\n"
        "Path(marker + '.child-popen').write_text('1')\n"
        "child.wait()\n"
        "Path(marker + '.child-wait').write_text('1')\n"
        "print('cron-script-ok', flush=True)\n",
        encoding="utf-8",
    )
    captured = _install_i13_observer(monkeypatch, request)
    with jobs.use_cron_store(home):
        job = jobs.create_job(
            prompt=None,
            schedule=datetime.now(UTC).isoformat(),
            name="managed scheduler script",
            script=str(script),
            no_agent=True,
            deliver="local",
        )
        provider = InProcessCronScheduler()
        deadline = asyncio.get_running_loop().time() + 90.0
        try:
            assert await asyncio.wait_for(
                asyncio.to_thread(provider.fire_due, job["id"]),
                timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
            ) is True
        finally:
            print("NATIVE_SCRIPT_MARKERS=" + json.dumps({
                name: Path(str(script) + '.' + name).is_file()
                for name in ('script-enter', 'child-popen', 'child-enter', 'child-wait')
            }, sort_keys=True), flush=True)
            execution_id = getattr(captured[0], 'execution_id', None) if len(captured) == 1 else None
            if (type(execution_id) is str and execution_id
                    and Path(execution_id).name == execution_id):
                stderr_file = home / 'cron' / 'external-workers' / f'{execution_id}.stderr'
                if stderr_file.is_file():
                    with stderr_file.open('rb') as stream:
                        stream.seek(0, 2)
                        stream.seek(max(0, stream.tell() - 65536))
                        snapshot = stream.read(65536).decode('utf-8', errors='replace')
                    for line in snapshot.splitlines():
                        if line.startswith('NATIVE_SCRIPT_STAGE='):
                            print(line, flush=True)

    assert len(captured) == 1
    binding = captured[0]
    await _await_public_fire_custody_settlement(binding, deadline)
    record = _assert_terminal_i12(
        binding, job, executions, coordinator, owner_classes_before,
    )
    assert record["delivery_outcome"] == "suppressed"
    if binding.child is None:
        process = binding._script_process
        assert process is not None and process.pid > 0 and process.poll() is not None
        assert binding._script_pipes_closed is True
    else:
        _assert_external_worker_physical_ownership(binding)
    outputs = list(jobs._job_output_dir(job["id"]).glob("*.md"))
    assert len(outputs) == 1
    output = outputs[0].read_text(encoding="utf-8")
    assert "cron-descendant-ok" in output
    assert "cron-script-ok" in output
