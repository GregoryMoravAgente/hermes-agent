"""Real I08 goal continuation controls through the public gateway producer.

The five controls in this file deliberately enter through ``WhatsAppAdapter.handle_message``
and the real ``GatewayRunner``/``AIAgent`` path.  The loopback FakeLLM server supplies both
the main response and the auxiliary goal judge, but no test calls a goal hook, installs a
custody handle, or replaces the gateway handler.  Durable rows, coordinator handles, and
the adapter delivery ledger are the only positive readback witnesses.
"""

from __future__ import annotations

import asyncio
from functools import wraps
import hashlib
import json
import os
from pathlib import Path
import threading

import pytest

from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding
from tests.waiting import async_wait_until


_I08_OWNER = "I08_goals_loops_heartbeat"


def _configure_public_agent(monkeypatch) -> None:
    """Use the normal configured OpenAI adapter against the test-owned provider."""
    monkeypatch.setattr(
        "hermes_cli.tools_config._get_platform_tools", lambda *_args, **_kwargs: {"terminal"},
    )
    monkeypatch.setattr("gateway.run._resolve_gateway_model", lambda *_args, **_kwargs: "fixture-model")
    monkeypatch.setattr("gateway.run._load_gateway_config", dict)
    monkeypatch.setattr(
        "gateway.run.GatewayRunner._load_reasoning_config",
        staticmethod(lambda model="": {}),
    )
    monkeypatch.setattr(
        "gateway.run.GatewayRunner._load_fallback_model",
        staticmethod(lambda: None),
    )
    monkeypatch.setattr("gateway.run._current_max_iterations", lambda: 6)


def _payload_has_i08(value) -> bool:
    if not isinstance(value, dict):
        return False
    custody = value.get("custody")
    if isinstance(custody, dict) and custody.get("owner_class") == _I08_OWNER:
        return True
    return any(_payload_has_i08(item) for item in value.values())


def _fake_llm_body(record: dict) -> dict:
    """Read the genuine FakeLLMServer request envelope without rebuilding it."""
    assert isinstance(record, dict)
    body = record.get("body")
    assert isinstance(body, dict), "FakeLLMServer request is missing its decoded body"
    return body


def _i08_rows(authority, session_id: str) -> list[dict]:
    from hermes_state_runtime import list_session_admissions

    return [
        row for row in list_session_admissions(authority.db, session_id=session_id, pending_only=False)
        if _payload_has_i08(row.get("payload"))
    ]


def _owner_records(authority) -> list[tuple[object, object]]:
    coordinator = authority._preservation_coordinator()
    assert coordinator is not None
    return [
        (record.handle, record)
        for record in coordinator._records.values()
        if getattr(record.handle, "owner_class", None) == _I08_OWNER
    ]


def _delivered_obligations(authority, session_key: str) -> list[tuple[str, str]]:
    with authority.db._read_ctx() as conn:
        rows = conn.execute(
            "SELECT obligation_id, state FROM delivery_obligations "
            "WHERE session_key=? ORDER BY obligation_id",
            (session_key,),
        ).fetchall()
    return [(str(row[0]), str(row[1])) for row in rows]


def _capture_session_tasks(monkeypatch, adapter, session_key: str) -> list[asyncio.Task]:
    """Capture source task callbacks and bounded pre-task routing diagnostics."""
    class _TaskCapture(list):
        def __init__(self):
            super().__init__()
            self.diagnostic = {
                "expected_session_key_sha256": hashlib.sha256(
                    session_key.encode("utf-8")
                ).hexdigest(),
                "track": [],
                "start": [],
                "busy": [],
                "classic": [],
            }

    captured = _TaskCapture()

    def key_scalar(candidate_session_key):
        return {
            "candidate_session_key_sha256": hashlib.sha256(
                str(candidate_session_key).encode("utf-8")
            ).hexdigest(),
            "expected_match": candidate_session_key == session_key,
        }

    def task_scalar(task):
        task_type = f"{type(task).__module__}.{type(task).__qualname__}"
        return {
            "task_type": task_type,
            "task_identity_sha256": hashlib.sha256(
                f"{task_type}:{id(task)}".encode()
            ).hexdigest(),
            "is_asyncio_task": isinstance(task, asyncio.Task),
        }

    def remember(task):
        if not any(existing is task for existing in captured):
            captured.append(task)

    original = adapter._track_session_task

    @wraps(original)
    def observe(candidate_session_key: str, task):
        scalar = key_scalar(candidate_session_key)
        scalar.update(task_scalar(task))
        captured.diagnostic["track"].append(scalar)
        if candidate_session_key == session_key:
            remember(task)
        return original(candidate_session_key, task)

    monkeypatch.setattr(adapter, "_track_session_task", observe)

    original_start = adapter._start_session_processing

    @wraps(original_start)
    def observe_start(event, candidate_session_key, **kwargs):
        from gateway.session_ingress_burst import holds_busy_burst

        scalar = key_scalar(candidate_session_key)
        scalar.update({
            "active_before": candidate_session_key in adapter._active_sessions,
            "shared_burst_before": holds_busy_burst(adapter, candidate_session_key),
            "event_classic_key_before": getattr(event, "_classic_custody_key", None) is not None,
        })
        try:
            result = original_start(event, candidate_session_key, **kwargs)
        except BaseException as exc:
            scalar.update({"returned": None, "raised": type(exc).__name__})
            captured.diagnostic["start"].append(scalar)
            raise
        scalar.update({"returned": result is True, "return_type": type(result).__name__})
        captured.diagnostic["start"].append(scalar)
        return result

    monkeypatch.setattr(adapter, "_start_session_processing", observe_start)

    authority = getattr(adapter.gateway_runner, "session_authority", None)
    if authority is not None:
        original_classic_begin = authority._classic_begin

        @wraps(original_classic_begin)
        def observe_classic_begin(event, candidate_session_key, producer, **kwargs):
            scalar = key_scalar(candidate_session_key)
            scalar.update({
                "producer": str(producer),
                "event_classic_key_before": getattr(
                    event, "_classic_custody_key", None
                ) is not None,
            })
            try:
                result = original_classic_begin(
                    event, candidate_session_key, producer, **kwargs
                )
            except BaseException as exc:
                scalar.update({"returned": None, "raised": type(exc).__name__})
                captured.diagnostic["classic"].append(scalar)
                raise
            scalar.update({
                "returned": result is True,
                "return_type": type(result).__name__,
                "event_classic_key_after": getattr(event, "_classic_custody_key", None) is not None,
            })
            captured.diagnostic["classic"].append(scalar)
            return result

        monkeypatch.setattr(authority, "_classic_begin", observe_classic_begin)

    import gateway.run_busy_custody as busy_custody
    original_busy_begin = busy_custody.begin_classic_event

    @wraps(original_busy_begin)
    def observe_busy_begin(runner, event, candidate_session_key, producer, **kwargs):
        bound = getattr(event, "_classic_custody_authority", None)
        scalar = key_scalar(candidate_session_key)
        scalar.update({
            "producer": str(producer),
            "bound_authority_present": bound is not None,
            "bound_runner_matches": getattr(bound, "runner", None) is runner,
            "runner_has_authority_surface": bool(
                runner is not None
                and (hasattr(runner, "session_authorities") or hasattr(runner, "session_authority"))
            ),
        })
        try:
            result = original_busy_begin(
                runner, event, candidate_session_key, producer, **kwargs
            )
        except BaseException as exc:
            scalar.update({"returned": None, "raised": type(exc).__name__})
            captured.diagnostic["busy"].append(scalar)
            raise
        scalar.update({"returned": result is True, "return_type": type(result).__name__})
        captured.diagnostic["busy"].append(scalar)
        return result

    monkeypatch.setattr(busy_custody, "begin_classic_event", observe_busy_begin)
    return captured


async def _join_session_tasks(captured: list[asyncio.Task], *, timeout: float = 90.0) -> None:
    """Join every task the source creates for one session under one existing time budget."""
    if not captured:
        diagnostic = getattr(captured, "diagnostic", None)
        if isinstance(diagnostic, dict):
            print("i08-session-task-diagnostic:" + json.dumps(diagnostic, sort_keys=True))
    assert captured, "the public admission did not create a source-owned session task"
    deadline = asyncio.get_running_loop().time() + timeout
    joined = 0
    while joined < len(captured):
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError("the public session task chain did not settle")
        await asyncio.wait_for(captured[joined], timeout=remaining)
        joined += 1


def _owner_records_for(authority, owner_class: str) -> list[tuple[object, object]]:
    coordinator = authority._preservation_coordinator()
    assert coordinator is not None
    return [
        (record.handle, record)
        for record in coordinator._records.values()
        if getattr(record.handle, "owner_class", None) == owner_class
    ]


async def _public_goal_turn(
    fixture,
    tmp_path: Path,
    monkeypatch,
    *,
    llm,
    goal: str,
    message_id: str,
    chat_id: str | None = None,
    expected_main: int = 1,
    send_success: bool = True,
    join_authority_tasks: bool = False,
):
    """Drive one real public event and return its durable routing context."""
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from hermes_cli import goals
    from tests.fakes.fake_llm_provider import write_hermes_home

    # real_runtime_binding freezes automatic scheduling for its lower-level controls.  Restore
    # the production class scheduler before entering the public adapter path.
    scheduler = getattr(type(fixture.authority), "_schedule", None)
    assert scheduler is not None
    monkeypatch.setattr(
        fixture.authority, "_schedule", scheduler.__get__(fixture.authority, type(fixture.authority)),
    )
    _configure_public_agent(monkeypatch)
    monkeypatch.setattr(
        "gateway.run._resolve_runtime_agent_kwargs",
        lambda llm=llm: {
            "provider": "openai", "api_key": "fixture-key", "base_url": llm.base_url,
            "api_mode": "chat_completions",
        },
    )

    sent: list[str] = []

    async def send(*args, content=None, **_kwargs):
        value = content
        if value is None and len(args) >= 2:
            value = args[1]
        sent.append(str(value or ""))
        if send_success:
            return SendResult(success=True, message_id=f"reply-{message_id}")
        return SendResult(success=False, error="owned-test-delivery-failure")

    monkeypatch.setattr(fixture.adapter, "send", send)
    source = fixture.adapter.build_source(
        chat_id=chat_id or f"{message_id}@s.whatsapp.net",
        user_id="15551234567@s.whatsapp.net",
        chat_type="dm",
    )
    session_key = fixture.runner.session_store._generate_session_key(source)
    entry = fixture.runner.session_store.get_or_create_session(source)
    assert entry.session_key == session_key
    goals.GoalManager(entry.session_id).set(goal, max_turns=4)
    event = MessageEvent(text=f"public I08 turn: {message_id}", source=source, message_id=message_id)
    write_hermes_home(Path(os.environ["HERMES_HOME"]), llm.base_url)

    captured_tasks = _capture_session_tasks(monkeypatch, fixture.adapter, session_key)
    authority_tasks: list[asyncio.Task] = []
    if join_authority_tasks:
        # Goal continuation is admitted through the authority FIFO.  Its real drain task is owned
        # by the authority session entry, so it is not necessarily visible through the adapter's
        # _track_session_task hook above.  Capture that exact source-created task after the
        # production scheduler returns; do not manufacture a task or read a task for another
        # session.
        original_schedule = fixture.authority._schedule

        @wraps(original_schedule)
        def observe_authority_schedule(ref):
            result = original_schedule(ref)
            if getattr(ref, "session_id", None) != entry.session_id:
                return result
            live = fixture.authority.sessions.get(entry.session_id)
            task = getattr(live, "task", None)
            if isinstance(task, asyncio.Task) and not any(
                existing is task for existing in captured_tasks
            ):
                captured_tasks.append(task)
                authority_tasks.append(task)
            return result

        monkeypatch.setattr(fixture.authority, "_schedule", observe_authority_schedule)
    await fixture.adapter.handle_message(event)
    join_failure = None
    try:
        await _join_session_tasks(captured_tasks)
    except BaseException as exc:
        join_failure = type(exc).__name__
        raise
    finally:
        if join_authority_tasks:
            _write_r8_done_diagnostic(tmp_path, [{
                "session_id_sha256": _r7_scalar_hash(entry.session_id),
                "task_type": f"{type(task).__module__}.{type(task).__qualname__}",
                "task_identity_sha256": _r7_scalar_hash(
                    f"{type(task).__module__}.{type(task).__qualname__}:{id(task)}",
                ),
                "done": task.done(), "cancelled": task.cancelled(),
            } for task in authority_tasks], join_failure, case_label="fixture-task")
    await async_wait_until(
        lambda: len(llm.main_requests()) >= expected_main,
        timeout=90,
        interval=0.02,
        what=f"{message_id} public main provider calls",
    )
    await async_wait_until(
        lambda: not any(
            session_key in mapping
            for mapping in (
                fixture.adapter._session_tasks,
                fixture.adapter._active_sessions,
                fixture.adapter._pending_messages,
            )
        ),
        timeout=90,
        interval=0.02,
        what=f"{message_id} public adapter drain",
    )
    return entry, source, session_key, sent


def _goal_aux_responder(goal_calls: list[dict], *, continue_once: bool = False):
    """Answer only actual goal-judge prompts; unrelated auxiliary calls finish immediately."""
    from tests.fakes.fake_llm_provider import Text

    def respond(request):
        body = _fake_llm_body(request)
        messages = body.get("messages")
        assert isinstance(messages, list), "goal-judge request body is missing messages"
        text = "\n".join(
            str(item.get("content") or "") for item in messages if isinstance(item, dict)
        )
        if "Is the goal satisfied" not in text:
            return Text('{"verdict":"done","reason":"non-goal auxiliary work complete"}')
        goal_calls.append(request)
        if continue_once and len(goal_calls) == 1:
            return Text('{"verdict":"continue","reason":"the next turn is still required"}')
        return Text('{"verdict":"done","reason":"the public continuation is complete"}')

    return respond


_R7_RESERVATION_FIELDS = (
    "owner_class", "profile_id", "authority_epoch", "process_instance",
    "physical_owner_epoch", "instance_id", "session_id", "admission_id",
    "generation", "request_id",
)


def _r7_scalar_hash(value):
    if value is None:
        return None
    if isinstance(value, (str, bytes, int, float, bool)):
        raw = f"{type(value).__name__}:{value}".encode("utf-8", "backslashreplace")
    else:
        value_type = f"{type(value).__module__}.{type(value).__qualname__}"
        raw = f"{value_type}:{id(value)}".encode()
    return hashlib.sha256(raw).hexdigest()


def _r7_ref(value, fields=()):
    if value is None:
        return {"present": False}
    result = {
        "present": True,
        "type": f"{type(value).__module__}.{type(value).__qualname__}",
        "identity_sha256": _r7_scalar_hash(value),
    }
    for field in fields:
        try:
            field_value = getattr(value, field)
        except BaseException:
            result[f"{field}_read"] = "error"
            continue
        if isinstance(field_value, (bool, int)):
            result[field] = field_value
        elif field_value is None:
            result[field] = None
        else:
            result[f"{field}_sha256"] = _r7_scalar_hash(field_value)
    return result


def _r7_execution(authority, session_id):
    sessions = getattr(authority, "sessions", {})
    live = sessions.get(session_id) if isinstance(sessions, dict) else None
    execution = getattr(getattr(live, "event_stream", None), "execution", None) or {}
    return {
        "session_id_sha256": _r7_scalar_hash(session_id),
        "admission_id_sha256": _r7_scalar_hash(execution.get("admission_id")),
        "execution_generation": execution.get("execution_generation")
        if isinstance(execution.get("execution_generation"), int) else None,
    }


def _r7_authority(authority):
    return _r7_ref(
        authority,
        ("profile_id", "epoch", "instance_id", "process_instance", "physical_owner_epoch"),
    )


def _r7_event(event):
    return {
        "generation": getattr(event, "_hermes_run_generation", None),
        "type": f"{type(event).__module__}.{type(event).__qualname__}",
    }


def _r7_exception(exc):
    result = {"type": f"{type(exc).__module__}.{type(exc).__qualname__}"}
    args = getattr(exc, "args", ())
    reason = args[0] if args and isinstance(args[0], str) else None
    if reason and all(char.isalnum() or char in "_:-" for char in reason):
        result["reason"] = reason
    else:
        result["reason_sha256"] = _r7_scalar_hash(reason)
    return result


def _r7_record(observations, record):
    try:
        observations.append(record)
    except BaseException:
        pass


def _install_r7_goal_reservation_observer(monkeypatch, fixture, observations):
    """Observe the real reservation calls without re-running any source predicate."""
    import gateway.session_goal_tick_custody as custody
    import gateway.session_goal_tick_runtime as runtime

    authority_hint = getattr(fixture, "authority", None)
    original_tick = runtime.reserve_goal_tick

    @wraps(original_tick)
    def observe_tick(runner, session_entry, source, event):
        record = {
            "function": "reserve_goal_tick",
            "before": {
                "authority": _r7_authority(authority_hint),
                "session_entry": _r7_ref(session_entry, ("session_id", "session_key")),
                "execution": _r7_execution(
                    authority_hint,
                    getattr(session_entry, "session_id", None),
                ),
                "event": _r7_event(event),
            },
        }
        try:
            result = original_tick(runner, session_entry, source, event)
        except BaseException as exc:
            record["exception"] = _r7_exception(exc)
            _r7_record(observations, record)
            raise
        authority, tick, custody_refused = (result + (None, None, None))[:3] \
            if isinstance(result, tuple) else (None, None, None)
        record["after"] = {
            "authority": _r7_authority(authority),
            "tick": _r7_ref(tick, ("kind", "request_id", "turn_lease")),
            "custody_refused": custody_refused is True,
            "result_type": type(result).__name__,
        }
        _r7_record(observations, record)
        return result

    original_goal = custody.reserve_goal

    @wraps(original_goal)
    def observe_goal(authority, *, event, session_id, request_id, principal_id,
                     source_identity, turn_lease=None):
        record = {
            "function": "reserve_goal",
            "before": {
                "authority": _r7_authority(authority),
                "execution": _r7_execution(authority, session_id),
                "event": _r7_event(event),
                "session_id_sha256": _r7_scalar_hash(session_id),
                "request_id_sha256": _r7_scalar_hash(request_id),
                "principal_id_sha256": _r7_scalar_hash(principal_id),
                "source_identity_sha256": _r7_scalar_hash(source_identity),
                "turn_lease": _r7_ref(
                    turn_lease,
                    ("session_id", "generation", "released", "authority_epoch"),
                ),
            },
        }
        try:
            result = original_goal(
                authority, event=event, session_id=session_id, request_id=request_id,
                principal_id=principal_id, source_identity=source_identity,
                turn_lease=turn_lease,
            )
        except BaseException as exc:
            record["exception"] = _r7_exception(exc)
            _r7_record(observations, record)
            raise
        record["after"] = {
            "tick": _r7_ref(result, ("kind", "request_id", "turn_lease")),
            "returned_none": result is None,
        }
        _r7_record(observations, record)
        return result

    original_reserve_tick = custody.reserve_tick

    @wraps(original_reserve_tick)
    def observe_reserve_tick(
        authority, *, request_id, principal_id, session_id, source_identity, kind,
        watcher_key=None, parent=None, turn_lease=None,
    ):
        record = {
            "function": "reserve_tick",
            "before": {
                "authority": _r7_authority(authority),
                "execution": _r7_execution(authority, session_id),
                "request_id_sha256": _r7_scalar_hash(request_id),
                "principal_id_sha256": _r7_scalar_hash(principal_id),
                "session_id_sha256": _r7_scalar_hash(session_id),
                "source_identity_sha256": _r7_scalar_hash(source_identity),
                "kind": kind,
                "parent": _r7_ref(parent, _R7_RESERVATION_FIELDS),
                "turn_lease": _r7_ref(
                    turn_lease,
                    ("session_id", "generation", "released", "authority_epoch"),
                ),
            },
        }
        try:
            result = original_reserve_tick(
                authority, request_id=request_id, principal_id=principal_id,
                session_id=session_id, source_identity=source_identity, kind=kind,
                watcher_key=watcher_key, parent=parent, turn_lease=turn_lease,
            )
        except BaseException as exc:
            record["exception"] = _r7_exception(exc)
            _r7_record(observations, record)
            raise
        record["after"] = {
            "tick": _r7_ref(result, ("kind", "request_id", "turn_lease")),
            "returned_none": result is None,
        }
        _r7_record(observations, record)
        return result

    monkeypatch.setattr(runtime, "reserve_goal_tick", observe_tick)
    monkeypatch.setattr(custody, "reserve_goal", observe_goal)
    monkeypatch.setattr(custody, "reserve_tick", observe_reserve_tick)

    original_debug = runtime.logger.debug

    @wraps(original_debug)
    def observe_refusal_log(message, *args, **kwargs):
        if message == "goal continuation custody reservation failed: %s" and args:
            _r7_record(observations, {
                "function": "reserve_goal_tick.refusal_log",
                "exception": _r7_exception(args[0]),
            })
        return original_debug(message, *args, **kwargs)

    monkeypatch.setattr(runtime.logger, "debug", observe_refusal_log)

    authority = authority_hint
    original_parent = getattr(authority, "_preservation_operation_parent", None)
    if callable(original_parent):
        @wraps(original_parent)
        def observe_parent(*args, **kwargs):
            record = {
                "function": "_preservation_operation_parent",
                "before": {
                    "session_id_sha256": _r7_scalar_hash(args[0] if args else kwargs.get("session_id")),
                    "admission_id_sha256": _r7_scalar_hash(
                        args[1] if len(args) > 1 else kwargs.get("admission_id")
                    ),
                    "generation": kwargs.get("generation"),
                    "principal_id_sha256": _r7_scalar_hash(kwargs.get("principal_id")),
                },
            }
            try:
                result = original_parent(*args, **kwargs)
            except BaseException as exc:
                record["exception"] = _r7_exception(exc)
                _r7_record(observations, record)
                raise
            record["after"] = {
                "parent": _r7_ref(result, _R7_RESERVATION_FIELDS),
                "returned_none": result is None,
            }
            _r7_record(observations, record)
            return result

        monkeypatch.setattr(authority, "_preservation_operation_parent", observe_parent)


def _r8_return(value):
    if value is None:
        return {"present": False}
    if isinstance(value, bool):
        return {"present": True, "type": "bool", "value": value}
    return _r7_ref(value, ("code", "owner_class", "reason"))


def _install_r8_done_observer(monkeypatch, fixture, observations):
    """Observe the real finalizer boundary without re-evaluating its custody predicates."""
    import gateway.platforms.base_delivery_callbacks as delivery_callbacks
    import gateway.session_goal_tick_custody as custody
    import gateway.session_goal_tick_runtime as runtime

    original_register = runtime.register_tick_delivery_finalizer

    @wraps(original_register)
    def observe_register(runner, source, event):
        record = {
            "function": "register_tick_delivery_finalizer",
            "event": _r7_ref(
                event, ("_hermes_run_generation", "_i08_delivery_finalizer_registered"),
            ),
            "source": _r7_ref(source, ("platform", "chat_id", "user_id")),
        }
        try:
            result = original_register(runner, source, event)
        except BaseException as exc:
            record["exception"] = _r7_exception(exc)
            _r7_record(observations, record)
            raise
        record["return"] = _r8_return(result)
        _r7_record(observations, record)
        return result

    monkeypatch.setattr(runtime, "register_tick_delivery_finalizer", observe_register)

    original_session_key = fixture.runner._session_key_for_source

    @wraps(original_session_key)
    def observe_session_key(*args, **kwargs):
        source = args[0] if args else kwargs.get("source")
        record = {
            "function": "_session_key_for_source",
            "source": _r7_ref(source, ("platform", "chat_id", "user_id")),
        }
        try:
            result = original_session_key(*args, **kwargs)
        except BaseException as exc:
            record["exception"] = _r7_exception(exc)
            _r7_record(observations, record)
            raise
        record["return"] = {
            "type": type(result).__name__,
            "value_sha256": _r7_scalar_hash(result),
        }
        _r7_record(observations, record)
        return result

    monkeypatch.setattr(fixture.runner, "_session_key_for_source", observe_session_key)

    original_outcome = delivery_callbacks._post_delivery_outcome

    @wraps(original_outcome)
    def observe_outcome():
        try:
            result = original_outcome()
        except BaseException as exc:
            _r7_record(observations, {
                "function": "_post_delivery_outcome",
                "exception": _r7_exception(exc),
            })
            raise
        record = {
            "function": "_post_delivery_outcome",
            "present": result is not None,
        }
        if result is not None:
            record.update({
                "type": f"{type(result).__module__}.{type(result).__qualname__}",
                "event": _r7_ref(getattr(result, "event", None), ("_hermes_run_generation",)),
                "session_key_sha256": _r7_scalar_hash(getattr(result, "session_key", None)),
                "processing_ok": getattr(result, "processing_ok", None),
            })
        _r7_record(observations, record)
        return result

    monkeypatch.setattr(delivery_callbacks, "_post_delivery_outcome", observe_outcome)

    original_finish = custody.finish_unbound

    @wraps(original_finish)
    def observe_finish(tick, *, success):
        record = {
            "function": "finish_unbound",
            "tick": _r7_ref(tick, ("kind", "request_id", "turn_lease")),
            "success": success,
        }
        try:
            result = original_finish(tick, success=success)
        except BaseException as exc:
            record["exception"] = _r7_exception(exc)
            _r7_record(observations, record)
            raise
        record["return"] = _r8_return(result)
        _r7_record(observations, record)
        return result

    monkeypatch.setattr(custody, "finish_unbound", observe_finish)

    original_mark_unknown = custody.mark_unknown

    @wraps(original_mark_unknown)
    def observe_mark_unknown(tick, reason="join_failed"):
        record = {
            "function": "mark_unknown",
            "tick": _r7_ref(tick, ("kind", "request_id", "turn_lease")),
            "reason": reason,
        }
        try:
            result = original_mark_unknown(tick, reason)
        except BaseException as exc:
            record["exception"] = _r7_exception(exc)
            _r7_record(observations, record)
            raise
        record["return"] = _r8_return(result)
        _r7_record(observations, record)
        return result

    monkeypatch.setattr(custody, "mark_unknown", observe_mark_unknown)

    coordinator = fixture.authority._preservation_coordinator()
    if coordinator is None:
        _r7_record(observations, {
            "function": "coordinator.release_unaccepted",
            "coordinator_present": False,
        })
    else:
        original_release = coordinator.release_unaccepted

        @wraps(original_release)
        def observe_release(handle):
            record = {
                "function": "coordinator.release_unaccepted",
                "handle": _r7_ref(handle, _R7_RESERVATION_FIELDS),
            }
            try:
                result = original_release(handle)
            except BaseException as exc:
                record["exception"] = _r7_exception(exc)
                _r7_record(observations, record)
                raise
            record["return"] = _r8_return(result)
            _r7_record(observations, record)
            return result

        monkeypatch.setattr(coordinator, "release_unaccepted", observe_release)


def _install_i08_origin_observer(monkeypatch, observations):
    """Observe source-created origin/receipt identities without replacing any producer seam."""
    import gateway.platforms.base_delivery_callbacks as delivery_callbacks
    import gateway.session_goal_tick_runtime as runtime

    original_register = runtime.register_tick_delivery_finalizer

    @wraps(original_register)
    def observe_register(runner, source, event):
        result = original_register(runner, source, event)
        capture = getattr(event, "_i08_delivery_origin", None)
        observations["registrations"].append((event, capture, result))
        return result

    original_outcome = delivery_callbacks._post_delivery_outcome

    @wraps(original_outcome)
    def observe_outcome():
        outcome = original_outcome()
        if outcome is not None:
            observations["outcomes"].append(outcome)
        return outcome

    monkeypatch.setattr(runtime, "register_tick_delivery_finalizer", observe_register)
    monkeypatch.setattr(delivery_callbacks, "_post_delivery_outcome", observe_outcome)
    return observations


def _install_r8_route_observer(monkeypatch, fixture, observations):
    """Observe normal and in-band goal routes without invoking either route a second time."""
    original_queued = fixture.runner._run_agent_queued_followup

    def queued_context_scalar(turn_ctx):
        return _r7_ref(
            turn_ctx,
            ("session_key", "session_id", "run_generation", "_interrupt_depth"),
        )

    @wraps(original_queued)
    async def observe_queued(*args, **kwargs):
        turn_ctx = args[0] if args else kwargs.get("turn_ctx")
        pending = args[2] if len(args) > 2 else kwargs.get("pending")
        pending_event = args[3] if len(args) > 3 else kwargs.get("pending_event")
        entry = {
            "phase": "queued_followup",
            "stage": "enter",
            "turn_context": queued_context_scalar(turn_ctx),
            "pending_present": pending is not None,
            "pending_event": _r7_ref(
                pending_event, ("_hermes_run_generation", "message_id", "internal"),
            ),
        }
        _r7_record(observations, entry)
        try:
            result = await original_queued(*args, **kwargs)
        except BaseException as exc:
            _r7_record(observations, {
                "phase": "queued_followup",
                "stage": "exit",
                "exception": _r7_exception(exc),
            })
            raise
        _r7_record(observations, {
            "phase": "queued_followup",
            "stage": "exit",
            "return": _r8_return(result),
        })
        return result

    monkeypatch.setattr(fixture.runner, "_run_agent_queued_followup", observe_queued)

    original_goal = fixture.runner._post_turn_goal_continuation

    @wraps(original_goal)
    async def observe_goal(*args, **kwargs):
        session_entry = args[0] if args else kwargs.get("session_entry")
        source = args[1] if len(args) > 1 else kwargs.get("source")
        final_response = args[2] if len(args) > 2 else kwargs.get("final_response")
        event = args[3] if len(args) > 3 else kwargs.get("event")
        _r7_record(observations, {
            "phase": "normal_goal_hook",
            "stage": "enter",
            "session_entry": _r7_ref(session_entry, ("session_id", "session_key")),
            "source": _r7_ref(source, ("platform", "chat_id", "user_id")),
            "event": _r7_ref(event, ("_hermes_run_generation", "message_id")),
            "final_response_present": bool(final_response),
        })
        try:
            result = await original_goal(*args, **kwargs)
        except BaseException as exc:
            _r7_record(observations, {
                "phase": "normal_goal_hook",
                "stage": "exit",
                "exception": _r7_exception(exc),
            })
            raise
        _r7_record(observations, {
            "phase": "normal_goal_hook",
            "stage": "exit",
            "return": _r8_return(result),
        })
        return result

    monkeypatch.setattr(fixture.runner, "_post_turn_goal_continuation", observe_goal)


def _write_r8_route_diagnostic(tmp_path: Path, observations, failure_type: str):
    path = tmp_path / "i08-queued-route-diagnostic.json"
    payload = {
        "failure_type": failure_type,
        "calls": observations,
        "call_count": len(observations),
    }
    try:
        configured = os.environ.get("HERMES_E2E_ARTIFACTS", "").strip()
        if configured:
            output_dir = Path(configured).expanduser() / "i08-goal-reservation"
            output_dir.mkdir(parents=True, exist_ok=True)
            path = output_dir / path.name
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        print("i08-queued-route-diagnostic:" + str(path))
    except BaseException:
        pass


def _write_r8_done_diagnostic(tmp_path: Path, observations, failure_type: str, case_label: str):
    path = tmp_path / f"i08-done-observer-{case_label}.json"
    payload = {
        "case": case_label,
        "failure_type": failure_type,
        "calls": observations,
        "call_count": len(observations),
    }
    try:
        configured = os.environ.get("HERMES_E2E_ARTIFACTS", "").strip()
        if configured:
            output_dir = Path(configured).expanduser() / "i08-goal-reservation"
            output_dir.mkdir(parents=True, exist_ok=True)
            path = output_dir / path.name
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        print(f"i08-done-observer-{case_label}:" + str(path))
    except BaseException:
        pass


def _write_r7_goal_reservation_diagnostic(tmp_path: Path, observations, failure_type: str):
    path = tmp_path / "i08-goal-reservation-diagnostic.json"
    payload = {
        "failure_type": failure_type,
        "calls": observations,
        "call_count": len(observations),
    }
    try:
        configured = os.environ.get("HERMES_E2E_ARTIFACTS", "").strip()
        if configured:
            output_dir = Path(configured).expanduser() / "i08-goal-reservation"
            output_dir.mkdir(parents=True, exist_ok=True)
            path = output_dir / path.name
        path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        print("i08-goal-reservation-diagnostic:" + str(path))
    except BaseException:
        pass


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_public_goal_continuation_claims_real_parent_and_drains_next_turn(tmp_path, monkeypatch):
    """A real public turn creates an I08 child beneath I04 and runs the next FIFO turn."""
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text
    from hermes_cli import goals

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    goal_calls: list[dict] = []
    reservation_observations = []
    done_observations = []
    route_observations = []
    _install_r7_goal_reservation_observer(monkeypatch, fixture, reservation_observations)
    _install_r8_done_observer(monkeypatch, fixture, done_observations)
    _install_r8_route_observer(monkeypatch, fixture, route_observations)
    failure_type = None
    try:
        with FakeLLMServer(
            [Text("first public response"), Text("second public response")],
            aux=_goal_aux_responder(goal_calls, continue_once=True),
        ) as llm:
            entry, _source, session_key, sent = await _public_goal_turn(
                fixture, tmp_path, monkeypatch, llm=llm,
                goal="finish the managed goal continuation", message_id="i08-public-followup",
                expected_main=2,
                join_authority_tasks=True,
            )
        rows = _i08_rows(fixture.authority, entry.session_id)
        assert len(goal_calls) >= 2
        assert len(llm.main_requests()) == 2
        assert any("first public response" in item for item in sent)
        assert any("second public response" in item for item in sent)
        assert rows and all(row["status"] == "terminal" for row in rows)
        assert all(type(row.get("generation")) is int for row in rows)
        assert all(str(row.get("principal_id", "")).startswith("automation:") for row in rows)
        assert any(row.get("outcome") not in {None, "cancelled"} for row in rows)
        from gateway.session_goal_tick_custody import I08_OWNER
        assert I08_OWNER not in fixture.authority._preservation_coordinator().status()["unknown_owner_classes"]
        assert goals.GoalManager(entry.session_id).state.status == "done"
        assert any(state == "delivered" for _oid, state in _delivered_obligations(fixture.authority, session_key))
    except BaseException as exc:
        failure_type = type(exc).__name__
        raise
    finally:
        if failure_type is not None:
            _write_r7_goal_reservation_diagnostic(
                tmp_path, reservation_observations, failure_type,
            )
            _write_r8_done_diagnostic(
                tmp_path, done_observations, failure_type, case_label="continuation",
            )
            _write_r8_route_diagnostic(tmp_path, route_observations, failure_type)
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_public_goal_done_releases_unbound_tick_at_real_delivery(tmp_path, monkeypatch):
    """A done judge releases the real unbound I08 tick only after adapter delivery."""
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text
    from hermes_cli import goals

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    goal_calls: list[dict] = []
    done_observations = []
    _install_r8_done_observer(monkeypatch, fixture, done_observations)
    failure_type = None
    try:
        with FakeLLMServer(
            [Text("one public response")],
            aux=_goal_aux_responder(goal_calls),
        ) as llm:
            entry, _source, session_key, sent = await _public_goal_turn(
                fixture, tmp_path, monkeypatch, llm=llm,
                goal="finish without a continuation", message_id="i08-public-done",
            )
        status = fixture.authority._preservation_coordinator().status()
        assert len(goal_calls) == 1
        assert any("one public response" in item for item in sent)
        assert _i08_rows(fixture.authority, entry.session_id) == []
        assert _owner_records(fixture.authority) == []
        assert _I08_OWNER not in status["active_owner_classes"]
        assert _I08_OWNER not in status["unknown_owner_classes"]
        assert goals.GoalManager(entry.session_id).state.status == "done"
        assert any(state == "delivered" for _oid, state in _delivered_obligations(fixture.authority, session_key))
    except BaseException as exc:
        failure_type = type(exc).__name__
        raise
    finally:
        if failure_type is not None:
            _write_r8_done_diagnostic(
                tmp_path, done_observations, failure_type, case_label="done",
            )
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_public_goal_close_race_retains_live_parent_until_public_turn_finishes(tmp_path, monkeypatch):
    """A close begun while the public I04 provider is live cannot seal the parent early."""
    from gateway.platforms.event import MessageEvent
    from hermes_cli import goals
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    provider_started = threading.Event()
    release_provider = threading.Event()
    coordinator = fixture.authority._preservation_coordinator()
    assert coordinator is not None
    close = None
    captured_tasks = None
    join_attempted = False
    tasks_settled = False

    def provider(request):
        if _fake_llm_body(request).get("tools"):
            provider_started.set()
            assert release_provider.wait(30), "the public provider did not receive the close-race release"
            return Text("close-race response")
        return Text('{"verdict":"done","reason":"close-race auxiliary complete"}')

    try:
        from tests.fakes.fake_llm_provider import write_hermes_home
        _configure_public_agent(monkeypatch)
        scheduler = getattr(type(fixture.authority), "_schedule", None)
        assert scheduler is not None
        monkeypatch.setattr(
            fixture.authority, "_schedule", scheduler.__get__(fixture.authority, type(fixture.authority)),
        )
        source = fixture.adapter.build_source(
            chat_id="i08-close-race@s.whatsapp.net",
            user_id="15551234567@s.whatsapp.net", chat_type="dm",
        )
        entry = fixture.runner.session_store.get_or_create_session(source)
        session_key = entry.session_key
        goals.GoalManager(entry.session_id).set("finish the held public turn", max_turns=2)
        event = MessageEvent(text="held public turn", source=source, message_id="i08-close-race")
        from gateway.platforms.base import SendResult

        async def send(*_args, content=None, **_kwargs):
            return SendResult(success=True, message_id="close-race-reply")

        monkeypatch.setattr(fixture.adapter, "send", send)
        write_hermes_home(Path(os.environ["HERMES_HOME"]), "http://127.0.0.1:1/v1")
        with FakeLLMServer(provider) as llm:
            write_hermes_home(Path(os.environ["HERMES_HOME"]), llm.base_url)
            monkeypatch.setattr(
                "gateway.run._resolve_runtime_agent_kwargs",
                lambda llm=llm: {
                    "provider": "openai", "api_key": "fixture-key", "base_url": llm.base_url,
                    "api_mode": "chat_completions",
                },
            )
            captured_tasks = _capture_session_tasks(monkeypatch, fixture.adapter, session_key)
            await fixture.adapter.handle_message(event)
            assert captured_tasks
            await async_wait_until(
                provider_started.is_set, timeout=30, interval=0.02,
                what="public close-race provider admission",
            )
            close = coordinator.begin_close(
                owner_id="i08-public-close-race", scope="process", deadline_s=30,
            )
            assert close is not None and close.close_generation >= 1
            status = fixture.authority._preservation_coordinator().status()
            assert "I04_fifo_turn_execution" in status["active_owner_classes"]
            i04_records = _owner_records_for(fixture.authority, "I04_fifo_turn_execution")
            assert len(i04_records) == 1
            i04_handle, _i04_record = i04_records[0]
            from gateway.session_goal_tick_custody import owner_ready
            assert owner_ready(fixture.authority, close_generation=close.close_generation) is False
            release_provider.set()
            join_attempted = True
            await _join_session_tasks(captured_tasks)
            tasks_settled = True
            await async_wait_until(
                lambda: not any(
                    session_key in mapping
                    for mapping in (
                        fixture.adapter._session_tasks,
                        fixture.adapter._active_sessions,
                        fixture.adapter._pending_messages,
                    )
                ),
                timeout=90, interval=0.02, what="close-race public task settlement",
            )
            # The close is intentionally still held: this successful fixture has a real durable
            # response and no I08 child, so the exact parent must settle through its canonical
            # terminal/readback path before the final readiness assertion.
            status = fixture.authority._preservation_coordinator().status()
            assert fixture.authority._preservation_coordinator().is_terminal_handle(
                i04_handle,
                owner_class="I04_fifo_turn_execution",
                profile_id=fixture.authority.profile_id,
                authority_epoch=fixture.authority.epoch,
            )
            assert "I04_fifo_turn_execution" not in status["active_owner_classes"]
            assert "I04_fifo_turn_execution" not in status["unknown_owner_classes"]
            assert any(state == "delivered" for _oid, state in _delivered_obligations(
                fixture.authority, session_key,
            ))
            assert owner_ready(fixture.authority, close_generation=close.close_generation) is False
    finally:
        release_provider.set()
        join_error = None
        if captured_tasks is not None and not join_attempted:
            join_attempted = True
            try:
                await _join_session_tasks(captured_tasks)
                tasks_settled = True
            except BaseException as exc:
                join_error = exc
                tasks_settled = all(task.done() for task in captured_tasks)
        if close is not None and getattr(close, "state", None) == "closing":
            if not tasks_settled:
                if join_error is not None:
                    raise RuntimeError(
                        "close-race task did not settle; retaining exact close receipt and fixture ownership"
                    ) from join_error
                raise RuntimeError(
                    "close-race task was not joined; retaining exact close receipt and fixture ownership"
                )
            assert coordinator.abort_before_handoff(close) is None
        fixture.cleanup()
        if join_error is not None:
            raise join_error


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_public_goal_refuses_retiring_authority_before_i08_tick(tmp_path, monkeypatch):
    """A public event against a retiring authority cannot create an I08 row or lease."""
    from gateway.platforms.event import MessageEvent
    from hermes_cli import goals
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text, write_hermes_home

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    try:
        _configure_public_agent(monkeypatch)
        scheduler = getattr(type(fixture.authority), "_schedule", None)
        assert scheduler is not None
        monkeypatch.setattr(
            fixture.authority, "_schedule", scheduler.__get__(fixture.authority, type(fixture.authority)),
        )
        source = fixture.adapter.build_source(
            chat_id="i08-retiring-authority@s.whatsapp.net",
            user_id="15551234567@s.whatsapp.net", chat_type="dm",
        )
        entry = fixture.runner.session_store.get_or_create_session(source)
        goals.GoalManager(entry.session_id).set("must not start while retiring", max_turns=1)
        event = MessageEvent(text="retiring authority", source=source, message_id="i08-retiring")
        fixture.authority.retiring = True
        with FakeLLMServer([Text("must not run")]) as llm:
            write_hermes_home(Path(os.environ["HERMES_HOME"]), llm.base_url)
            monkeypatch.setattr(
                "gateway.run._resolve_runtime_agent_kwargs",
                lambda llm=llm: {
                    "provider": "openai", "api_key": "fixture-key", "base_url": llm.base_url,
                    "api_mode": "chat_completions",
                },
            )
            await fixture.adapter.handle_message(event)
            task = fixture.adapter._session_tasks.get(entry.session_key)
            if task is not None:
                await asyncio.wait_for(task, timeout=45)
            assert llm.main_requests() == []
        assert _i08_rows(fixture.authority, entry.session_id) == []
        assert _owner_records(fixture.authority) == []
    finally:
        fixture.authority.retiring = False
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_public_goal_delivery_failure_retains_exact_unknown_i08_handle(tmp_path, monkeypatch):
    """A real outbound failure leaves the exact I08 handle UNKNOWN for owner recovery."""
    from hermes_cli import goals
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    goal_calls: list[dict] = []
    try:
        with FakeLLMServer(
            [Text("response whose delivery is refused")],
            aux=_goal_aux_responder(goal_calls),
        ) as llm:
            entry, _source, _session_key, _sent = await _public_goal_turn(
                fixture, tmp_path, monkeypatch, llm=llm,
                goal="retain the failed public delivery", message_id="i08-delivery-failure",
                send_success=False,
            )
        assert len(goal_calls) == 1
        records = _owner_records(fixture.authority)
        assert len(records) == 1
        handle, record = records[0]
        assert getattr(handle, "owner_class", None) == _I08_OWNER
        assert getattr(record, "state", None) == "unknown"
        assert getattr(record, "reason", None) in {
            "delivery_finalizer_refused", "delivery_finalizer_failed", "join_failed",
        }
        status = fixture.authority._preservation_coordinator().status()
        from gateway.session_goal_tick_custody import owner_ready
        assert _I08_OWNER in status["active_owner_classes"]
        assert _I08_OWNER not in status["unknown_owner_classes"]
        assert owner_ready(
            fixture.authority,
            close_generation=status.get("close_generation"),
        ) is False
        assert _i08_rows(fixture.authority, entry.session_id) == []
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_public_goal_done_uses_captured_ingress_origin_for_restored_event(tmp_path, monkeypatch):
    """The real Base finalizer consumes the original ingress object, not its restored copy."""
    from hermes_cli import goals
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    observations = {"registrations": [], "outcomes": []}
    _install_i08_origin_observer(monkeypatch, observations)
    goal_calls: list[dict] = []
    try:
        with FakeLLMServer(
            [Text("captured ingress response")],
            aux=_goal_aux_responder(goal_calls),
        ) as llm:
            entry, _source, session_key, _sent = await _public_goal_turn(
                fixture, tmp_path, monkeypatch, llm=llm,
                goal="finish through the captured ingress", message_id="i08-origin-positive",
            )
        assert len(goal_calls) == 1
        registrations = [item for item in observations["registrations"] if item[1] is not None]
        assert registrations, "the managed public reservation did not capture an ingress origin"
        restored_event, capture, registered = registrations[0]
        assert registered is True
        assert isinstance(capture, tuple) and len(capture) == 6
        admission_id, generation, _session_id, _principal_id, parent, origin = capture
        assert isinstance(admission_id, str) and admission_id
        assert type(generation) is int and generation >= 1
        assert getattr(parent, "owner_class", None) == "I04_fifo_turn_execution"
        assert origin is not None and origin is not restored_event
        outcomes = [
            outcome for outcome in observations["outcomes"]
            if getattr(outcome, "session_key", None) == session_key
            and getattr(outcome, "processing_ok", None) is True
        ]
        assert outcomes and any(outcome.event is origin for outcome in outcomes)
        assert all(outcome.event is not restored_event for outcome in outcomes)
        assert _i08_rows(fixture.authority, entry.session_id) == []
        assert _owner_records(fixture.authority) == []
        assert goals.GoalManager(entry.session_id).state.status == "done"
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_public_goal_successors_keep_distinct_source_origins(tmp_path, monkeypatch):
    """Two real public turns record distinct source-owned ingress origins."""
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    observations = {"registrations": [], "outcomes": []}
    _install_i08_origin_observer(monkeypatch, observations)
    goal_calls: list[dict] = []
    shared_chat = "i08-origin-successor@s.whatsapp.net"
    try:
        with FakeLLMServer(
            [Text("first successor response"), Text("second successor response")],
            aux=_goal_aux_responder(goal_calls),
        ) as llm:
            first, _source, session_key, _sent = await _public_goal_turn(
                fixture, tmp_path, monkeypatch, llm=llm,
                goal="finish the first successor turn", message_id="i08-origin-first",
                chat_id=shared_chat, expected_main=1,
            )
            second, _source, second_session_key, _sent = await _public_goal_turn(
                fixture, tmp_path, monkeypatch, llm=llm,
                goal="finish the second successor turn", message_id="i08-origin-second",
                chat_id=shared_chat, expected_main=2,
            )
        assert first.session_id == second.session_id
        assert second_session_key == session_key
        assert len(goal_calls) == 2
        registrations = [item for item in observations["registrations"] if item[1] is not None]
        assert len(registrations) >= 2
        first_origin = registrations[-2][1][5]
        second_origin = registrations[-1][1][5]
        assert first_origin is not None and second_origin is not None
        assert first_origin is not second_origin
        outcomes = [
            outcome for outcome in observations["outcomes"]
            if getattr(outcome, "session_key", None) == session_key
            and getattr(outcome, "processing_ok", None) is True
        ]
        assert any(outcome.event is first_origin for outcome in outcomes)
        assert any(outcome.event is second_origin for outcome in outcomes)
        assert _owner_records(fixture.authority) == []
    finally:
        fixture.cleanup()
