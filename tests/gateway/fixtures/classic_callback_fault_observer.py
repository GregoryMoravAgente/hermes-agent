"""Test-only observation for managed authority and standalone Base callback lanes.

This proposal copy is not imported or executed in the live worktree.  Managed fault
injection is synchronous and is installed only after the authority has authenticated
the exact occurrence slot; the standalone helper remains a separate Base contract.
"""


def replace_exact_authority_callback_with_fault(
    adapter, *, handle, session_key, generation, control, observation,
):
    """Bind a synchronous fault to the already-authenticated exact occurrence.

    The caller invokes this from the adapter's occurrence-consumer wrapper, immediately
    before delegating to the production occurrence seam.  The authority has already run
    its slot, identity, receipt, row and parent checks, so this preserves the production
    consumer and exact handle rather than replacing a predicate or creating a callback row.
    """
    import asyncio
    from gateway.platforms.base_delivery_callbacks import ensure_occurrence_registry

    registry = ensure_occurrence_registry(adapter)
    original = registry.pop(handle, session_key=session_key, generation=generation)
    if not callable(original):
        raise AssertionError("expected one live production callback for the exact handle")

    def fault_before_callback_return():
        observation["fault_task"] = asyncio.current_task()
        observation["fault_control"] = control
        observation["fault_exception_type"] = expected_authority_fault_type(control).__name__
        if control == "authority_callback_keyboardinterrupt":
            raise KeyboardInterrupt("fixture_authority_callback_keyboardinterrupt")
        if control == "authority_callback_cancelled":
            raise asyncio.CancelledError()
        if control == "authority_callback_runtime_error":
            raise RuntimeError("fixture_authority_callback_runtime_error")
        raise AssertionError(f"unknown authority callback control: {control}")

    registry.bind(handle, fault_before_callback_return)
    observation["production_callback"] = original
    observation["exact_handle"] = handle
    return handle


def expected_authority_fault_type(control):
    import asyncio

    return {
        "authority_callback_keyboardinterrupt": KeyboardInterrupt,
        "authority_callback_cancelled": asyncio.CancelledError,
        "authority_callback_runtime_error": RuntimeError,
    }[control]


def install_execute_admission_branch_observer(monkeypatch, observations):
    """Observe the real policy decision and its real delivery destination.

    The production function reads ``native_waiters`` after the policy helpers return and
    then writes exactly one of its existing delivery registries.  Wrapping the existing
    policy callable captures that predicate in the real execution Task; wrapping the outer
    admission call records the registry state immediately after the original function
    returns, before its caller can resume.  No source text, frame locals, predicate,
    waiter set, or destination mapping is changed.
    """
    import asyncio
    from gateway import session_ingress, session_managed_worker

    original_execute = session_ingress.execute_admission
    original_managed_policy = session_managed_worker.managed_policy
    policy_observations = []

    def observe_managed_policy(authority, ref):
        policy = original_managed_policy(authority, ref)
        if policy is None:
            policy_observations.append({
                "authority": authority,
                "task": asyncio.current_task(),
                "native_waiters": frozenset(getattr(authority, "native_waiters", ())),
            })
        return policy

    async def observe(authority, ref, row):
        task = asyncio.current_task()
        observation_start = len(policy_observations)
        admission_id = row.get("admission_id") if isinstance(row, dict) else None
        result = await original_execute(authority, ref, row)
        candidates = [
            item for item in policy_observations[observation_start:]
            if item["authority"] is authority and item["task"] is task
        ]
        policy_observation = candidates[-1] if candidates else None
        from gateway.session_ingress import waiter_replies
        waiter_map = waiter_replies(authority)
        if admission_id in waiter_map:
            destination = "waiter_replies"
        elif admission_id in getattr(authority, "pending_deliveries", {}):
            destination = "pending_deliveries"
        else:
            destination = None
        native_predicate = (
            admission_id in policy_observation["native_waiters"]
            if policy_observation is not None else None
        )
        observations.append({
            "kind": "outer_execute_return",
            "admission_id": admission_id,
            "native_predicate": native_predicate,
            "destination": destination,
            "task": task,
            "policy_task": policy_observation["task"] if policy_observation is not None else None,
            "policy_observed": policy_observation is not None,
        })
        return result

    monkeypatch.setattr(session_managed_worker, "managed_policy", observe_managed_policy)
    monkeypatch.setattr(session_ingress, "execute_admission", observe)
    return original_execute

def install_finish_post_delivery_observer(
    monkeypatch,
    control,
    callback_task_errors,
    cleanup_after_callback_fault,
    settlement_observations,
):
    """Observe the real standalone Base finalizer without relabeling authority faults."""
    from gateway.platforms import base as base_platform

    original = base_platform.finish_post_delivery_callback

    async def observe(*args, **kwargs):
        try:
            return await original(*args, **kwargs)
        except BaseException as exc:
            callback_task_errors.append(exc)
            settlement_observations.append(
                {
                    "kind": "base_finalizer_same_task",
                    "exception_object": exc,
                    "exception_type": type(exc).__name__,
                    "cleanup_completed": cleanup_after_callback_fault.is_set(),
                    "control": control,
                }
            )
            raise

    monkeypatch.setattr(base_platform, "finish_post_delivery_callback", observe)


def _write_isolated_authority_fault_proof(
    proof_path, *, error, authority, fault_observation, third_queue_observation,
    cleanup_observation, branch_observations, busy_finish_calls,
):
    """Persist sanitized custody facts before a real BaseException reaches the process boundary."""
    import json
    from pathlib import Path

    def scalar_mapping(mapping):
        return {
            key: value
            for key, value in mapping.items()
            if value is None or isinstance(value, (str, int, float, bool))
        }

    records = []
    for record in getattr(authority, "_preservation_operations", {}).values():
        if not isinstance(record, dict) or record.get("request_id") not in {
            "authority-inbound-2", "authority-inbound-3",
        }:
            continue
        records.append({
            "request_id": record.get("request_id"),
            "state": record.get("state"),
            "unknown": record.get("unknown"),
            "owner_class": record.get("owner_class"),
            "admission_id_present": bool(record.get("admission_id")),
            "handle_present": record.get("handle") is not None,
            "parent_present": record.get("parent_handle") is not None,
        })
    branches = [
        {
            "admission_id_present": bool(item.get("admission_id")),
            "native_predicate": item.get("native_predicate"),
            "destination": item.get("destination"),
            "policy_observed": item.get("policy_observed"),
        }
        for item in branch_observations
        if item.get("kind") == "outer_execute_return"
    ]
    fault_task = fault_observation.get("fault_task")
    occurrence_task = fault_observation.get("occurrence_task")
    payload = {
        "phase": "after_busy_finish_before_task_boundary",
        "process_error_type": error if isinstance(error, str) else type(error).__name__,
        "fault_exception_type": fault_observation.get("fault_exception_type"),
        "fault_before_production_occurrence": fault_observation.get(
            "fault_before_production_occurrence",
        ),
        "consumer_path_observed": fault_observation.get("consumer_path_observed"),
        "same_task_occurrence_and_fault": fault_task is occurrence_task,
        "third_queue": scalar_mapping(third_queue_observation),
        "cleanup": scalar_mapping(cleanup_observation),
        "branches": branches,
        "busy_finish_calls": list(busy_finish_calls),
        "records": records,
    }
    Path(proof_path).write_text(json.dumps(payload, sort_keys=True) + "\n")


async def _hold_first_callback(state, *args, **kwargs):
    candidate_event = args[0] if args else kwargs.get("event")
    if candidate_event is state["third"]:
        state["third_model_started"].set()
    if not state["first_processed"].is_set():
        state["first_processed"].set()
        await state["release_first"].wait()
    return await state["original_handle"](*args, **kwargs)


async def _observe_send_final_text_callback(state, *args, **kwargs):
    candidate_event = args[0] if args else kwargs.get("event")
    if candidate_event is state["third"]:
        state["third_delivery_started"].set()
    result = await state["original_send_final_text"](*args, **kwargs)
    if candidate_event is state["first"]:
        state["first_send_completed"].set()
    return result


async def _send_callback(state, *args, content=None, **_kwargs):
    from gateway.platforms.base import SendResult

    if content is None and len(args) >= 2:
        content = args[1]
    state["sent"].append(str(content))
    return SendResult(success=True, message_id=f"authority-reply-{len(state['sent'])}")


async def _observe_authority_callback_callback(state, *args, **kwargs):
    try:
        result = await state["original_fire_authority"](*args, **kwargs)
        state["fault_observation"]["authority_callback_result"] = result
        return result
    except BaseException as exc:
        state["fault_observation"]["authority_callback_exception_type"] = type(exc).__name__
        raise


async def _observe_reconcile_callback(state, *args, **kwargs):
    import asyncio

    event = args[1] if len(args) > 1 else kwargs.get("event")
    if getattr(event, "message_id", None) == "authority-inbound-2":
        state["fault_observation"].update({
            "reconcile_task": asyncio.current_task(),
            "consumer_path_observed": True,
        })
    return await state["original_busy_reconcile"](*args, **kwargs)


def _observe_finish_observer_callback(state, *args, **kwargs):
    import asyncio

    preservation_key = args[2] if len(args) > 2 else kwargs.get("preservation_key")
    if state["callback_fault"].is_set():
        record = state["authority"]._preservation_operations.get(preservation_key)
        state["cleanup_observation"].update({
            "third_model_started_before_reconciliation": state["third_model_started"].is_set(),
            "third_delivery_started_before_reconciliation": state["third_delivery_started"].is_set(),
            "reconciliation_task": asyncio.current_task(),
            "second_record_present": isinstance(record, dict),
        })
        state["cleanup_after_callback_fault"].set()
    return state["original_busy_finish"](*args, **kwargs)

async def _observe_occurrence_callback(state, *args, **kwargs):
    import asyncio

    fault_observation = state["fault_observation"]
    handle = args[2] if len(args) > 2 else kwargs.get("handle")
    execution_event = args[1] if len(args) > 1 else kwargs.get("execution_event")
    consume_generic = bool(kwargs.get("consume_generic", False))
    owner = getattr(handle, "_owner_event", None)
    if owner is None:
        owner = getattr(execution_event, "_classic_execution_owner_event", execution_event)
    fault_observation.setdefault("occurrence_calls", []).append({
        "consume_generic": consume_generic,
        "execution_message_id": getattr(execution_event, "message_id", None),
        "owner_message_id": getattr(owner, "message_id", None),
        "occurrence_task": asyncio.current_task(),
    })
    if (
        consume_generic
        and state["control"] in state["controls"]
        and getattr(owner, "message_id", None) == "authority-inbound-2"
    ):
        fault_observation.update({
            "consumer_path": "session_ingress_busy._busy_reconcile_callback -> fire_authority_post_delivery_callback -> adapter._fire_post_delivery_callback_occurrence",
            "occurrence_task": asyncio.current_task(),
            "fault_before_production_occurrence": True,
            "owner_event_is_second": owner is state["second"],
        })
        if not state["third_admission_done"]:
            state["third_admission_done"] = True
            state["third_queue_snapshot"]()
            state["third_queue_observed"].set()
        generation = getattr(handle, "generation", None)
        if generation is None:
            generation = getattr(execution_event, "_hermes_run_generation", None)
        state["replace_exact_authority_callback_with_fault"](
            state["adapter"],
            handle=handle,
            session_key=args[0] if args else kwargs["session_key"],
            generation=generation,
            control=state["control"],
            observation=fault_observation,
        )
        fault_task = asyncio.current_task()
        fault_observation["fault_task"] = fault_task

        def _observe_fault_task_done(task):
            try:
                error = task.exception()
            except BaseException as exc:
                error = exc
            fault_observation["fault_task_done"] = True
            fault_observation["fault_task_exception_type"] = (
                type(error).__name__ if error is not None else None
            )
            state["callback_task_done"].set()

        if fault_task is not None:
            # The managed busy observer deliberately re-raises its first BaseException after
            # all cleanup.  Retrieve that exact task result at the real producer boundary.
            fault_task.add_done_callback(_observe_fault_task_done)
        state["callback_fault"].set()

    def _capture_retained_identity():
        if not state["third_admission_done"]:
            return
        records = getattr(state["authority"], "_preservation_operations", {})
        latest = {}
        for record in records.values():
            if isinstance(record, dict) and record.get("request_id") in {
                "authority-inbound-2", "authority-inbound-3",
            }:
                latest[record["request_id"]] = record
        for request_id, prefix in (("authority-inbound-2", "second"), ("authority-inbound-3", "third")):
            record = latest.get(request_id)
            if record is None:
                continue
            state["third_queue_observation"][f"{prefix}_handle_same_after_fault"] = (
                state["third_queue_observation"].get(f"_{prefix}_handle_before")
                is record.get("handle")
            )
            state["third_queue_observation"][f"{prefix}_parent_same_after_fault"] = (
                state["third_queue_observation"].get(f"_{prefix}_parent_before")
                is record.get("parent_handle")
            )

    try:
        result = await state["original_fire_occurrence"](*args, **kwargs)
        _capture_retained_identity()
        return result
    except BaseException as exc:
        fault_observation["occurrence_exception_type"] = type(exc).__name__
        _capture_retained_identity()
        raise


def _observe_finish_busy_callback(state, authority_arg, key, accepted_arg, *, unknown=False):
    result = state["original_finish_busy"](authority_arg, key, accepted_arg, unknown=unknown)
    record = authority_arg._preservation_operations.get(key)
    if isinstance(record, dict) and record.get("request_id") == "authority-inbound-2":
        state["busy_finish_calls"].append({"accepted": accepted_arg, "unknown": unknown})
        state["busy_finish_done"].set()
        if state["proof_path"] is not None:
            _write_isolated_authority_fault_proof(
                state["proof_path"],
                error=state["fault_observation"].get("fault_exception_type") or "unknown",
                authority=authority_arg,
                fault_observation=state["fault_observation"],
                third_queue_observation=state["third_queue_observation"],
                cleanup_observation=state["cleanup_observation"],
                branch_observations=state["branch_observations"],
                busy_finish_calls=state["busy_finish_calls"],
            )
    return result

async def _execute_real_authority_callback_fault_case(case):
    import asyncio

    from tests.fakes.fake_llm_provider import FakeLLMServer, Text
    from tests.gateway.fixtures.classic_callback_fault_observer import expected_authority_fault_type

    fixture = case["fixture"]
    adapter = case["adapter"]
    authority = case["authority"]
    monkeypatch = case["monkeypatch"]
    control = case["control"]
    first = case["first"]
    second = case["second"]
    third = case["third"]
    session_key = case["session_key"]
    first_processed = case["first_processed"]
    release_first = case["release_first"]
    first_send_completed = case["first_send_completed"]
    callback_fault = case["callback_fault"]
    third_queue_observed = case["third_queue_observed"]
    cleanup_after_callback_fault = case["cleanup_after_callback_fault"]
    callback_task_done = case["callback_task_done"]
    busy_finish_done = case["busy_finish_done"]
    fault_observation = case["fault_observation"]
    third_queue_observation = case["third_queue_observation"]
    cleanup_observation = case["cleanup_observation"]
    branch_observations = case["branch_observations"]
    busy_finish_calls = case["busy_finish_calls"]
    sent = case["sent"]

    first_call = None
    second_call = None
    try:
        with FakeLLMServer([Text("authority reply 1"), Text("authority reply 2")]) as llm:
            # The real fixture resolves provider settings through the profile-scoped runtime
            # loader.  Bind that loader to this actual FakeLLMServer endpoint, as the existing
            # public managed fixture does; a global fake endpoint alone leaves the profile at
            # ``no_provider_configured`` before the I06 consumer can run.
            monkeypatch.setattr(
                "gateway.run._resolve_runtime_agent_kwargs",
                lambda: {
                    "provider": "openai",
                    "api_key": "fixture-key",
                    "base_url": llm.base_url,
                    "api_mode": "chat_completions",
                },
            )
            monkeypatch.setattr(
                "gateway.run._resolve_gateway_model",
                lambda *_args, **_kwargs: "fixture-model",
            )
            monkeypatch.setattr("gateway.run._load_gateway_config", dict)
            monkeypatch.setattr(
                "gateway.run.GatewayRunner._load_reasoning_config",
                staticmethod(lambda model="": {}),
            )
            monkeypatch.setattr(
                "gateway.run.GatewayRunner._load_fallback_model",
                staticmethod(lambda: None),
            )
            monkeypatch.setattr("gateway.run._current_max_iterations", lambda: 4)
            monkeypatch.setattr(
                "hermes_cli.tools_config._get_platform_tools",
                lambda *_: {"terminal"},
            )
            first_call = asyncio.create_task(adapter.handle_message(first))
            await asyncio.wait_for(first_processed.wait(), 30)
            # The public adapter route is the only source-faithful producer topology here.  It
            # observes the real claimed I04 parent, registers the native waiter, and lets the
            # authority drain create the I06 callback occurrence.  Direct authority admission
            # skips that waiter and is correctly rejected or stranded by the ownership guards.
            from hermes_state_runtime import get_session_admission
            entry = fixture.runner.session_store.lookup_by_session_key(session_key)
            assert entry is not None
            live = authority.sessions[entry.session_id]
            execution = live.event_stream.execution
            assert isinstance(execution, dict)
            parent_admission_id = execution.get("admission_id")
            generation = execution.get("execution_generation")
            assert isinstance(parent_admission_id, str) and parent_admission_id
            assert type(generation) is int and generation >= 1
            parent_row = get_session_admission(authority.db, admission_id=parent_admission_id)
            assert parent_row is not None
            assert parent_row["status"] == "started"
            assert parent_row["generation"] == generation
            parent_handle = authority._preservation_children.get(parent_admission_id)
            assert parent_handle is not None
            case["parent_holder"]["handle"] = parent_handle
            assert parent_handle.owner_class == "I04_fifo_turn_execution"
            second_call = asyncio.create_task(adapter.handle_message(second))
            await asyncio.wait_for(second_call, 10)
            assert second._gateway_accepted is True
            second_records = [
                (key, record) for key, record in authority._preservation_operations.items()
                if isinstance(record, dict) and record.get("request_id") == "authority-inbound-2"
            ]
            assert len(second_records) == 1
            second_key, second_record = second_records[0]
            assert second_record["state"] == "queued"
            assert second_record["handle"].owner_class == "I06_busy_followup_delivery"
            assert second_record["parent_handle"] is parent_handle
            # Admit the real third FIFO input while the claimed I04 parent still exists.  Its
            # durable queued row then survives until the second callback fault; creating it from
            # inside the later callback would be a rowless root after parent terminalization.
            third_call = asyncio.create_task(adapter.handle_message(third))
            await asyncio.wait_for(third_call, 10)
            assert third._gateway_accepted is True
            third_records = [
                record for record in authority._preservation_operations.values()
                if isinstance(record, dict) and record.get("request_id") == "authority-inbound-3"
            ]
            assert len(third_records) == 1
            assert third_records[0]["state"] == "queued"
            assert third_records[0]["parent_handle"] is parent_handle
            release_first.set()
            try:
                await asyncio.wait_for(callback_fault.wait(), 30)
            except TimeoutError as exc:
                from gateway.session_ingress import waiter_replies
                raise AssertionError(
                    "authority callback occurrence was not reached: "
                    + repr({
                        "authority_callback_result": fault_observation.get("authority_callback_result"),
                        "authority_callback_exception_type": fault_observation.get(
                            "authority_callback_exception_type"
                        ),
                        "occurrence_calls": [
                            {
                                "consume_generic": item.get("consume_generic"),
                                "execution_message_id": item.get("execution_message_id"),
                                "owner_message_id": item.get("owner_message_id"),
                            }
                            for item in fault_observation.get("occurrence_calls", [])
                        ],
                        "branches": branch_observations,
                        "second_record": {
                            "state": second_record.get("state"),
                            "admission_id": second_record.get("admission_id"),
                            "unknown": second_record.get("unknown"),
                            "parent_present": second_record.get("parent_handle") is not None,
                        },
                        "pending_deliveries": sorted(getattr(authority, "pending_deliveries", {})),
                        "waiter_replies": sorted(waiter_replies(authority)),
                        "active_sessions": sorted(getattr(adapter, "_active_sessions", {})),
                        "pending_messages": sorted(getattr(adapter, "_pending_messages", {})),
                        "first_send_completed": first_send_completed.is_set(),
                    })
                ) from exc
            await asyncio.wait_for(third_queue_observed.wait(), 30)
            await asyncio.wait_for(cleanup_after_callback_fault.wait(), 30)
            await asyncio.wait_for(busy_finish_done.wait(), 30)
            await asyncio.wait_for(callback_task_done.wait(), 30)
            await asyncio.wait_for(first_call, 30)
            if not cleanup_after_callback_fault.is_set():
                cleanup_after_callback_fault.set()
            fault_observation["expected_fault_type"] = expected_authority_fault_type(control).__name__
            fault_observation["fault_task_type"] = type(fault_observation.get("fault_task")).__name__
            fault_observation["second_operation_key"] = second_key
            fault_observation["second_admission_id"] = second_record.get("admission_id")
            fault_observation["same_task_occurrence_and_fault"] = (
                fault_observation.get("occurrence_task") is fault_observation.get("fault_task")
            )
            fault_observation["same_task_reconcile_and_occurrence"] = (
                fault_observation.get("reconcile_task") is fault_observation.get("occurrence_task")
            )
            second_branches = [
                item for item in branch_observations
                if item.get("admission_id") == second_record.get("admission_id")
                and item.get("kind") == "outer_execute_return"
            ]
            assert len(second_branches) == 1
            second_branch = second_branches[0]
            assert second_branch["policy_observed"] is True
            assert second_branch["policy_task"] is second_branch["task"]
            assert second_branch["destination"] in {"waiter_replies", "pending_deliveries"}
            assert second_branch["native_predicate"] is (
                second_branch["destination"] == "waiter_replies"
            )
            fault_observation["second_branch_observations"] = second_branches
            fault_observation["second_native_waiter_values"] = [
                second_branch["native_predicate"]
            ]
            fault_observation["second_delivery_destinations"] = [
                second_branch["destination"]
            ]
            assert fault_observation["fault_before_production_occurrence"] is True
            assert fault_observation["same_task_occurrence_and_fault"] is True
            assert fault_observation["same_task_reconcile_and_occurrence"] is True
            assert third_queue_observation["accepted"] is True
            assert third_queue_observation["durable_queued"] is True
            assert third_queue_observation["record_count"] == 1
            assert third_queue_observation["queued_state"] is True
            assert third_queue_observation["owner_class"] == "I06_busy_followup_delivery"
            assert third_queue_observation["unknown"] is False
            assert third_queue_observation["different_operation_key"] is True
            assert third_queue_observation["different_identity"] is True
            assert third_queue_observation["handle_present"] is True
            assert third_queue_observation["handle_distinct_from_second"] is True
            assert third_queue_observation["second_handle_retained"] is True
            assert third_queue_observation["third_parent_is_exact_i04_parent"] is True
            assert third_queue_observation["second_parent_is_exact_i04_parent"] is True
            assert third_queue_observation["same_parent_identity"] is True
            assert third_queue_observation["distinct_source_tuple"] is True
            assert cleanup_observation["third_model_started_before_reconciliation"] is False
            assert cleanup_observation["third_delivery_started_before_reconciliation"] is False
            assert len(busy_finish_calls) == 1
            second_after = authority._preservation_operations.get(second_key)
            assert isinstance(second_after, dict)
            assert second_after["unknown"] is True
        return (
            fixture, authority, adapter, fault_observation, third_queue_observation,
            cleanup_observation, branch_observations, sent,
        )
    except BaseException:
        release_first.set()
        if first_call is not None and not first_call.done():
            try:
                await first_call
            except BaseException:
                pass
        if second_call is not None and not second_call.done():
            try:
                await second_call
            except BaseException:
                pass
        if third_call is not None and not third_call.done():
            try:
                await third_call
            except BaseException:
                pass
        try:
            await adapter.cancel_background_tasks()
        except BaseException:
            pass
        fixture.cleanup()
        raise

async def _run_real_authority_callback_fault_case(
    tmp_path, monkeypatch, control, *, proof_path=None,
):
    """Exercise the actual managed authority consumer before the occurrence seam."""
    import asyncio

    from tests.gateway.test_preservation_native_admission_link import (
        _configure_whatsapp_home, _restore_production_scheduler,
    )
    from gateway import session_ingress, session_ingress_busy
    from gateway import session_classic_delivery
    from gateway.config import Platform
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text
    from tests.gateway.fixtures.classic_callback_fault_observer import (
        expected_authority_fault_type,
        install_execute_admission_branch_observer,
        replace_exact_authority_callback_with_fault,
    )
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    controls = {
        "authority_callback_keyboardinterrupt",
        "authority_callback_cancelled",
        "authority_callback_runtime_error",
    }
    assert control in controls
    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    adapter = fixture.runner.adapters[Platform.WHATSAPP]
    authority = fixture.authority
    _restore_production_scheduler(authority, monkeypatch)
    sent = []
    first_processed = asyncio.Event()
    release_first = asyncio.Event()
    first_send_completed = asyncio.Event()
    callback_fault = asyncio.Event()
    third_queue_observed = asyncio.Event()
    cleanup_after_callback_fault = asyncio.Event()
    callback_task_done = asyncio.Event()
    busy_finish_done = asyncio.Event()
    third_model_started = asyncio.Event()
    third_delivery_started = asyncio.Event()
    fault_observation = {"control": control}
    third_queue_observation = {}
    cleanup_observation = {}
    branch_observations = []
    busy_finish_calls = []
    third_admission_done = False
    parent_holder = {"handle": None}

    source = adapter.build_source(
        chat_id="15551234567@s.whatsapp.net",
        user_id="15551234567@s.whatsapp.net",
        chat_type="dm",
    )
    _configure_whatsapp_home(fixture, source)
    first = MessageEvent(text="authority first", source=source, message_id="authority-inbound-1")
    second = MessageEvent(text="authority second", source=source, message_id="authority-inbound-2")
    third = MessageEvent(text="authority third", source=source, message_id="authority-inbound-3")
    session_key = fixture.runner.session_store._generate_session_key(source)

    original_handle = fixture.runner._handle_message_with_agent

    original_send_final_text = adapter._send_final_text

    from gateway.platforms.base import SendResult
    original_fire_occurrence = adapter._fire_post_delivery_callback_occurrence
    original_fire_authority = session_classic_delivery.fire_authority_post_delivery_callback
    original_busy_reconcile = session_ingress_busy._busy_reconcile_callback
    original_busy_finish = session_ingress_busy._busy_finish_observer
    original_finish_busy = session_ingress.finish_busy_preservation

    def _third_queue_snapshot():
        from hermes_state_runtime import get_session_admission

        parent_handle = parent_holder["handle"]
        records = getattr(authority, "_preservation_operations", {})
        third_records = [
            (key, record) for key, record in records.items()
            if isinstance(record, dict) and record.get("request_id") == "authority-inbound-3"
        ]
        second_records = [
            (key, record) for key, record in records.items()
            if isinstance(record, dict) and record.get("request_id") == "authority-inbound-2"
        ]
        third_key, third_record = third_records[0] if len(third_records) == 1 else (None, {})
        second_key, second_record = second_records[0] if len(second_records) == 1 else (None, {})
        third_identity = third_record.get("classic_identity")
        second_identity = second_record.get("classic_identity")
        third_handle = third_record.get("handle")
        second_handle = second_record.get("handle")
        third_parent = third_record.get("parent_handle")
        second_parent = second_record.get("parent_handle")
        coordinator = authority._preservation_coordinator()

        def _owner_tuple(record):
            identity = record.get("classic_identity") if isinstance(record, dict) else None
            handle = record.get("handle") if isinstance(record, dict) else None
            return (
                authority.profile_id, authority.instance_id, authority.epoch,
                getattr(handle, "profile_id", None),
                getattr(handle, "process_instance", None),
                getattr(handle, "authority_epoch", None),
                record.get("admission_id") if isinstance(record, dict) else None,
                identity.get("session_id") if isinstance(identity, dict) else None,
                identity.get("principal_id") if isinstance(identity, dict) else None,
                identity.get("request_id") if isinstance(identity, dict) else None,
                identity.get("occurrence_id") if isinstance(identity, dict) else None,
            )

        def _row_matches(record, *, require_queued=False):
            if not isinstance(record, dict) or not isinstance(record.get("admission_id"), str):
                return False
            row = get_session_admission(authority.db, admission_id=record["admission_id"])
            identity = record.get("classic_identity")
            handle = record.get("handle")
            return (
                isinstance(row, dict)
                and row.get("status") in (
                    {"queued"} if require_queued
                    else {"queued", "started", "unknown", "terminal"}
                )
                and row.get("admission_id") == record.get("admission_id")
                and row.get("owner_epoch") == authority.epoch
                and isinstance(identity, dict)
                and row.get("target_session_id") == identity.get("session_id")
                and row.get("principal_id") == identity.get("principal_id")
                and row.get("request_id") == identity.get("request_id")
                and getattr(handle, "profile_id", None) == authority.profile_id
                and getattr(handle, "process_instance", None) == authority.instance_id
                and getattr(handle, "authority_epoch", None) == authority.epoch
            )

        def _parent_edge_matches(record):
            if not isinstance(record, dict):
                return False
            child = record.get("handle")
            parent = record.get("parent_handle")
            if child is None or parent is None or coordinator is None:
                return False
            parent_admission_ids = tuple(
                admission_id for admission_id, candidate in
                getattr(authority, "_preservation_children", {}).items()
                if candidate is parent
            )
            parent_row = (
                get_session_admission(authority.db, admission_id=parent_admission_ids[0])
                if len(parent_admission_ids) == 1 else None
            )
            return (
                getattr(parent, "owner_class", None) == "I04_fifo_turn_execution"
                and getattr(parent, "profile_id", None) == authority.profile_id
                and getattr(parent, "process_instance", None) == authority.instance_id
                and getattr(parent, "authority_epoch", None) == authority.epoch
                and coordinator._parent(parent) is parent.lease
                and getattr(child.lease, "parent_lease_id", None) == parent.lease.lease_id
                and getattr(coordinator, "_children_by_parent", {}).get(
                    parent.lease.lease_id, set()
                ).__contains__(child.lease.lease_id)
                and isinstance(parent_row, dict)
                and parent_row.get("status") in {"started", "unknown", "terminal"}
                and parent_row.get("owner_epoch") == authority.epoch
                and parent_row.get("admission_id") == parent_admission_ids[0]
            )

        third_owner_tuple = _owner_tuple(third_record)
        second_owner_tuple = _owner_tuple(second_record)
        third_queue_observation["_third_handle_before"] = third_handle
        third_queue_observation["_third_parent_before"] = third_parent
        third_queue_observation["_second_handle_before"] = second_handle
        third_queue_observation["_second_parent_before"] = second_parent
        third_source_tuple = (
            third_identity.get("request_id"),
            third_identity.get("occurrence_id"),
            third_identity.get("source_identity"),
        ) if isinstance(third_identity, dict) else None
        second_source_tuple = (
            second_identity.get("request_id"),
            second_identity.get("occurrence_id"),
            second_identity.get("source_identity"),
        ) if isinstance(second_identity, dict) else None
        third_queue_observation.update({
            "accepted": third._gateway_accepted is True,
            "pending_is_third": adapter._pending_messages.get(session_key) is third,
            "record_count": len(third_records),
            "queued_state": third_record.get("state") == "queued",
            "durable_queued": (
                third_record.get("state") == "queued"
                and isinstance(third_record.get("admission_id"), str)
                and third_record.get("parent_handle") is parent_handle
            ),
            "handle_present": third_handle is not None,
            "handle_distinct_from_second": (
                third_handle is not None and second_handle is not None
                and third_handle is not second_handle
            ),
            "second_handle_retained": second_handle is not None,
            "third_owner_tuple": third_owner_tuple,
            "second_owner_tuple": second_owner_tuple,
            "third_owner_tuple_exact": _row_matches(third_record, require_queued=True),
            "second_owner_tuple_exact": _row_matches(second_record),
            "third_parent_edge_authenticated": _parent_edge_matches(third_record),
            "second_parent_edge_authenticated": _parent_edge_matches(second_record),
            "third_parent_is_exact_i04_parent": third_parent is parent_handle,
            "second_parent_is_exact_i04_parent": second_parent is parent_handle,
            "same_parent_identity": (
                third_parent is parent_handle and second_parent is parent_handle
            ),
            "distinct_source_tuple": (
                third_source_tuple is not None
                and second_source_tuple is not None
                and third_source_tuple != second_source_tuple
            ),
            "owner_class": third_record.get("owner_class"),
            "unknown": third_record.get("unknown"),
            "operation_key": third_key,
            "admission_id": third_record.get("admission_id"),
            "event_key_matches": getattr(third, "_classic_custody_key", None) == third_key,
            "identity_request_id": (
                third_identity.get("request_id") if isinstance(third_identity, dict) else None
            ),
            "identity_occurrence_id": (
                third_identity.get("occurrence_id") if isinstance(third_identity, dict) else None
            ),
            "different_operation_key": third_key is not None and third_key != second_key,
            "different_identity": (
                isinstance(third_identity, dict)
                and isinstance(second_identity, dict)
                and third_identity.get("request_id") != second_identity.get("request_id")
                and third_identity.get("occurrence_id") != second_identity.get("occurrence_id")
                and third_identity.get("source_identity") != second_identity.get("source_identity")
            ),
        })
    from functools import partial
    observer_state = {
        "third_admission_done": third_admission_done,
        "fault_observation": fault_observation,
        "third_queue_observation": third_queue_observation,
        "third_queue_observed": third_queue_observed,
        "callback_task_done": callback_task_done,
        "callback_fault": callback_fault,
        "replace_exact_authority_callback_with_fault": replace_exact_authority_callback_with_fault,
        "original_fire_occurrence": original_fire_occurrence,
        "third_queue_snapshot": _third_queue_snapshot,
        "parent_holder": parent_holder,
        "session_key": session_key, "authority": authority, "adapter": adapter,
        "control": control, "controls": controls, "second": second,
        "third": third, "original_finish_busy": original_finish_busy,
        "busy_finish_calls": busy_finish_calls, "busy_finish_done": busy_finish_done,
        "proof_path": proof_path, "cleanup_observation": cleanup_observation,
        "branch_observations": branch_observations,
        "original_handle": original_handle,
        "third_model_started": third_model_started,
        "first_processed": first_processed,
        "release_first": release_first,
        "original_send_final_text": original_send_final_text,
        "third_delivery_started": third_delivery_started,
        "first": first, "first_send_completed": first_send_completed,
        "sent": sent,
        "original_fire_authority": original_fire_authority,
        "original_busy_reconcile": original_busy_reconcile,
        "original_busy_finish": original_busy_finish,
        "cleanup_after_callback_fault": cleanup_after_callback_fault,
    }
    hold_first = partial(_hold_first_callback, observer_state)
    observe_send_final_text = partial(_observe_send_final_text_callback, observer_state)
    send = partial(_send_callback, observer_state)
    observe_authority_callback = partial(_observe_authority_callback_callback, observer_state)
    observe_reconcile = partial(_observe_reconcile_callback, observer_state)
    observe_finish_observer = partial(_observe_finish_observer_callback, observer_state)
    observe_occurrence = partial(_observe_occurrence_callback, observer_state)
    observe_finish_busy = partial(_observe_finish_busy_callback, observer_state)
    install_execute_admission_branch_observer(monkeypatch, branch_observations)
    monkeypatch.setattr(fixture.runner, "_handle_message_with_agent", hold_first)
    monkeypatch.setattr(adapter, "_send_final_text", observe_send_final_text)
    monkeypatch.setattr(adapter, "send", send)
    monkeypatch.setattr(adapter, "_fire_post_delivery_callback_occurrence", observe_occurrence)
    monkeypatch.setattr(
        session_classic_delivery, "fire_authority_post_delivery_callback", observe_authority_callback,
    )
    monkeypatch.setattr(session_ingress_busy, "_busy_reconcile_callback", observe_reconcile)
    monkeypatch.setattr(session_ingress_busy, "_busy_finish_observer", observe_finish_observer)
    monkeypatch.setattr(session_ingress, "finish_busy_preservation", observe_finish_busy)

    return await _execute_real_authority_callback_fault_case({
        "fixture": fixture, "adapter": adapter, "authority": authority,
        "monkeypatch": monkeypatch, "control": control, "proof_path": proof_path,
        "first": first, "second": second, "third": third, "session_key": session_key,
        "first_processed": first_processed, "release_first": release_first,
        "first_send_completed": first_send_completed, "callback_fault": callback_fault,
        "third_queue_observed": third_queue_observed,
        "cleanup_after_callback_fault": cleanup_after_callback_fault,
        "callback_task_done": callback_task_done, "busy_finish_done": busy_finish_done,
        "fault_observation": fault_observation,
        "third_queue_observation": third_queue_observation,
        "cleanup_observation": cleanup_observation,
        "branch_observations": branch_observations,
        "busy_finish_calls": busy_finish_calls, "sent": sent,
        "parent_holder": parent_holder,
    })


def _run_isolated_authority_keyboardinterrupt():
    """Run the authentic KI control in its own process and preserve its real exit boundary."""
    import argparse
    import asyncio
    import pytest
    from pathlib import Path

    parser = argparse.ArgumentParser()
    parser.add_argument("--run-authority-keyboardinterrupt", action="store_true")
    parser.add_argument("--tmp-path", required=True)
    parser.add_argument("--proof", required=True)
    args = parser.parse_args()
    assert args.run_authority_keyboardinterrupt is True
    monkeypatch = pytest.MonkeyPatch()
    try:
        asyncio.run(
            _run_real_authority_callback_fault_case(
                Path(args.tmp_path),
                monkeypatch,
                "authority_callback_keyboardinterrupt",
                proof_path=Path(args.proof),
            )
        )
    finally:
        monkeypatch.undo()


if __name__ == "__main__":
    _run_isolated_authority_keyboardinterrupt()
