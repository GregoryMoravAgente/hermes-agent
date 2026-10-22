"""Dispatch helpers for ``POST /v1/runs``.

The adapter keeps the route name and authentication surface.  Parsing, exact target selection,
idempotency reservation, and launch handoff stay in separate helpers so each stage has one
custody boundary and remains reviewable.
"""

from __future__ import annotations

import asyncio
import sqlite3
import uuid
from typing import Any


async def _parse_request(adapter, request, *, api_server):
    runs = _runs()
    openai_error = api_server._openai_error
    gateway_session_key, key_error = adapter._parse_session_key_header(request)
    if key_error is not None:
        return key_error
    try:
        body = await request.json()
    except (LookupError, TypeError, ValueError):
        return runs._json_error(openai_error, "Invalid JSON", status=400)
    body, room_error = await adapter._normalize_room_dispatch(request, body)
    if room_error is not None:
        return room_error
    room_dispatch, room_execution_policy = (
        value if isinstance(value, dict) else None for value in (
            (body.get("hosted_room_dispatch"), body.get("_room_execution_policy"))
            if isinstance(body, dict) else (None, None)))
    idempotency_key = request.headers.get("Idempotency-Key", "").strip()
    if len(idempotency_key) > 255 or any(ord(ch) < 33 or ord(ch) > 126 for ch in idempotency_key):
        return runs._json_error(
            openai_error, "Idempotency-Key must be 1-255 visible ASCII characters",
            code="invalid_idempotency_key", status=400)
    scope, fingerprint = runs._run_idempotency_identity(
        adapter, request, body, gateway_session_key, idempotency_key)
    user_message, input_error = runs._run_user_message(body.get("input"), openai_error)
    if input_error is not None:
        return input_error
    try:
        turn_author = api_server._request_turn_author(body)
    except ValueError as exc:
        return runs._json_error(openai_error, str(exc), code="invalid_author", status=400)
    history, instructions, stored_session_id, history_error, walked_from = (
        await runs._resolve_chained_history(adapter, body, body.get("input"), _openai_error=openai_error))
    if history_error is not None:
        return history_error
    session_id = body.get("session_id") or stored_session_id
    route = adapter._resolve_route(body.get("model"))
    agent_overrides = api_server._request_agent_overrides(body, virtual_model=adapter._model_name)
    selection_error = adapter._request_route_conflict_error(
        session_id=session_id, gateway_session_key=gateway_session_key,
        requested_model=agent_overrides.get("requested_model"),
        requested_provider=agent_overrides.get("requested_provider"), route=route)
    if selection_error:
        return runs._json_error(openai_error, selection_error, status=400)
    return {
        "body": body, "gateway_session_key": gateway_session_key,
        "room_dispatch": room_dispatch, "room_execution_policy": room_execution_policy,
        "idempotency_key": idempotency_key, "idempotency_scope": scope,
        "idempotency_fingerprint": fingerprint, "user_message": user_message,
        "turn_author": turn_author, "conversation_history": history,
        "instructions": instructions, "stored_session_id": stored_session_id,
        "walked_from": walked_from, "previous_response_id": body.get("previous_response_id"),
        "session_id": session_id, "route": route, "agent_overrides": agent_overrides,
    }


async def _select_target(adapter, request, parsed, *, api_server):
    runs = _runs()
    openai_error = api_server._openai_error
    key = parsed["idempotency_key"]
    if key:
        outcome, record = adapter._run_idempotency_store.lookup(
            parsed["idempotency_scope"], key, parsed["idempotency_fingerprint"],
            retention_until=runs._room_retention_until(request))
        if outcome == "conflict" or (outcome == "reused" and record is not None):
            return runs._replay_or_conflict(
                adapter, request, outcome, record, parsed["gateway_session_key"], openai_error)
    limited = adapter._concurrency_limited_response()
    if limited is not None:
        return limited
    run_id = f"run_{uuid.uuid4().hex}"
    selected = parsed["session_id"]
    declared_selected = not selected and bool(parsed["gateway_session_key"])
    if declared_selected:
        selected = await asyncio.to_thread(
            adapter._declared_conversation_session, parsed["gateway_session_key"])
    if selected:
        selected = await runs._resolve_live_session_id(adapter, str(selected))
    session_id = selected or run_id
    history = parsed["conversation_history"]
    session_history_delivery = not parsed["previous_response_id"] and not history
    if not history and selected and not parsed["previous_response_id"]:
        history = await adapter._conversation_history_for_session(str(selected))
    parsed = dict(parsed)
    parsed.update({
        "run_id": run_id, "selected_session_id": selected, "session_id": session_id,
        "declared_selected": declared_selected, "conversation_history": history,
        "session_history_delivery": session_history_delivery,
    })
    return parsed


def _reserve(adapter, request, selected, *, api_server):
    runs = _runs()
    openai_error = api_server._openai_error
    run_id = selected["run_id"]
    from gateway.platforms.api_server_run_custody import reserve_run
    custody, refusal = reserve_run(adapter, run_id, api_server._api_request_profile.get())
    if refusal is not None:
        return runs._run_custody_error(openai_error, refusal)
    adapter._run_owners[run_id] = adapter._run_idempotency_scope(request)
    adapter._run_custody[run_id] = custody
    queue = adapter._run_streams[run_id] = runs._RunStream()
    created_at = adapter._run_streams_created[run_id] = runs.time.time()
    adapter._run_approval_sessions[run_id] = run_id
    initial_status = adapter._set_run_status(
        run_id, "queued", created_at=created_at, session_id=selected["session_id"],
        model=selected["body"].get("model", adapter._model_name))
    key = selected["idempotency_key"]
    if key:
        try:
            outcome, record = adapter._run_idempotency_store.reserve(
                selected["idempotency_scope"], key, selected["idempotency_fingerprint"], run_id,
                initial_status, owner_pid=adapter._run_owner_pid,
                owner_started=adapter._run_owner_started,
                retention_until=runs._room_retention_until(request))
        except (OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            runs.logger.exception("run idempotency reservation failed after custody admission run_id=%s", run_id)
            reconciliation = runs._reconcile_idempotency_reservation_failure(
                adapter, run_id, selected["idempotency_scope"], key,
                selected["idempotency_fingerprint"], runs._room_retention_until(request))
            if reconciliation is not None:
                return runs._run_custody_error(openai_error, reconciliation)
            runs._forget_run(
                adapter, run_id, adapter._run_streams, adapter._run_streams_created,
                adapter._run_approval_sessions, adapter._run_statuses, adapter._run_owners)
            return runs._json_error(
                openai_error, "Idempotency reservation could not be completed",
                code="idempotency_reservation_unavailable", status=503)
        if outcome != "created":
            runs._release_unaccepted_custody(adapter, run_id)
            runs._forget_run(
                adapter, run_id, adapter._run_streams, adapter._run_streams_created,
                adapter._run_approval_sessions, adapter._run_statuses, adapter._run_owners)
            return runs._replay_or_conflict(
                adapter, request, outcome, record, selected["gateway_session_key"], openai_error)
        identity_refusal = custody.bind_idempotency_identity(
            selected["idempotency_scope"], key, selected["idempotency_fingerprint"],
            adapter._run_owner_pid, adapter._run_owner_started)
        if identity_refusal is not None:
            return runs._run_custody_error(openai_error, identity_refusal)
        adapter._run_idempotency_ids.add(run_id)
    return selected | {"custody": custody, "queue": queue, "created_at": created_at}


async def _launch(adapter, request, selected, *, api_server):
    runs = _runs()
    openai_error = api_server._openai_error
    custody = selected["custody"]
    launch = runs._RunLaunch(
        adapter, selected["run_id"], selected["queue"], selected["session_id"],
        selected["gateway_session_key"], selected["declared_selected"], selected["user_message"],
        selected["conversation_history"], selected["session_history_delivery"],
        agent_kwargs={
            "ephemeral_system_prompt": selected["instructions"],
            "session_id": selected["session_id"],
            "gateway_session_key": selected["gateway_session_key"],
            "route": selected["route"],
            "room_dispatch": selected["room_dispatch"],
            "room_execution_policy": selected["room_execution_policy"],
            **{key: selected["agent_overrides"].get(key)
               for key in ("requested_model", "requested_provider", "model_options")},
        },
        request_profile=api_server._api_request_profile.get(),
        browser_control_principal=api_server._api_request_browser_control_principal.get(),
        browser_control_transport_family=api_server._api_request_browser_control_transport_family.get(),
        turn_author=selected["turn_author"],
        walked_from=selected["walked_from"] if selected["session_id"] == selected["stored_session_id"] else None,
        custody=custody)
    admitted = None
    if selected["selected_session_id"]:
        admitted = await adapter._admit_to_live_bot_chat(
            selected["session_id"], selected["user_message"], selected["turn_author"], custody=custody)
    if admitted is not None:
        peer_record = admitted[1]
        peer_authority = peer_record.get("_api_admitting_authority") if isinstance(peer_record, dict) else None
        refusal = custody.bind_peer_record(peer_record, peer_authority)
        if refusal is not None:
            custody.mark_unknown(getattr(refusal, "code", "peer_parent_unmapped"))
            return runs._run_custody_error(openai_error, refusal)
        # A displaceable API row may have been replaced by the Bot Chat owner during admission.
        # Publish the receipt's canonical LOCAL session on the run before the worker starts; the
        # launch object remains the source-owned status identity for all terminal projections.
        runs._retarget_to_delivered_chat(adapter, launch, admitted)
    if admitted is None and getattr(adapter.gateway_runner, "session_authority", None) is not None:
        refused = await runs._admit_run_to_authority(
            adapter, launch, selected["run_id"], selected["session_history_delivery"],
            selected["declared_selected"], selected["idempotency_scope"],
            selected["idempotency_key"], openai_error)
        if refused is not None:
            return refused
    if admitted is None and launch.admission is None and custody.reservation_key is None:
        # A direct /v1/runs worker has no SessionAuthority row.  Before creating the
        # executor task, establish the server-minted durable status witness used by the
        # direct custody finalizer; client input remains only request data.
        from gateway.platforms.api_server_run_execution import begin_legacy_direct_turn
        from gateway.platforms.api_server_run_custody import ApiRunCustodyError
        try:
            begin_legacy_direct_turn(
                adapter, custody,
                profile_id=launch.request_profile,
                session_id=launch.session_id)
        except ApiRunCustodyError as exc:
            return runs._run_custody_error(openai_error, exc.refusal)
    adapter._activate_admitted_request()
    if admitted is not None:
        task = adapter._active_run_tasks[selected["run_id"]] = asyncio.create_task(
            runs._execute_run_via_live_owner(adapter, launch, *admitted, _api_server=api_server))
    else:
        task = adapter._active_run_tasks[selected["run_id"]] = asyncio.create_task(
            runs._execute_run(adapter, launch, _api_server=api_server))
    # Retain the original source task before its first scheduling turn.  Shutdown, disconnect, and
    # recovery must observe this exact Task rather than a wrapper or a newly-created successor.
    launch.execution_task = task
    with runs.suppress(TypeError):
        adapter._background_tasks.add(task)
    if hasattr(task, "add_done_callback"):
        task.add_done_callback(adapter._background_tasks.discard)
    return runs._accepted_response(selected["run_id"], "started", selected["gateway_session_key"], replayed=False)


async def handle_runs(adapter, request, *, api_server):
    parsed = await _parse_request(adapter, request, api_server=api_server)
    if not isinstance(parsed, dict):
        return parsed
    selected = await _select_target(adapter, request, parsed, api_server=api_server)
    if not isinstance(selected, dict):
        return selected
    reserved = _reserve(adapter, request, selected, api_server=api_server)
    if not isinstance(reserved, dict):
        return reserved
    return await _launch(adapter, request, reserved, api_server=api_server)


def _runs():
    from gateway.platforms import api_server_runs
    return api_server_runs
