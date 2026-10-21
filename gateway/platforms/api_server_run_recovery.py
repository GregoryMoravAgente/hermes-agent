"""Exact-handle recovery for API custody that cannot prove terminal ownership."""

from __future__ import annotations


def _recovery_error(api_server, code: str):
    from gateway.platforms.api_server_runs import _json_error

    return _json_error(
        api_server._openai_error,
        "The exact API finalizer completion could not be reconciled.",
        code=code,
        status=409,
    )


def _replay_completion_for_run(adapter, run_id):
    runner = getattr(adapter, "gateway_runner", None)
    from gateway.platforms.api_server_run_completion import (
        _replay_completion_for_authority, prune_replay_registry,
    )
    from gateway.session_authorities import all_authorities
    registry_owner = getattr(runner, "session_authorities", None)
    if registry_owner is not None and not isinstance(registry_owner, (dict, tuple, list, set)):
        # The production SessionAuthorities object is iterable and owns every served profile;
        # never reduce it to the launch authority or a dict/list-only view.
        authorities = tuple(all_authorities(runner))
    elif isinstance(registry_owner, dict):
        authorities = tuple(registry_owner.values())
    elif isinstance(registry_owner, (tuple, list, set)):
        authorities = tuple(registry_owner)
    else:
        authorities = tuple(all_authorities(runner))
    if registry_owner is None and not authorities:
        authority = getattr(runner, "session_authority", None)
        authorities = (authority,) if authority is not None else ()
    matches = []
    conflict = False
    for authority in authorities:
        registry = getattr(authority, "_api_finalizer_completions", None)
        if not isinstance(registry, dict):
            continue
        prune_replay_registry(registry)
        for completion in tuple(registry.values()):
            custody = getattr(completion, "custody", None)
            if getattr(custody, "run_id", None) != run_id:
                continue
            if _replay_completion_for_authority(completion, authority, adapter, run_id):
                matches.append(completion)
            else:
                conflict = True
    return matches[0] if len(matches) == 1 and not conflict else None


async def _resolve_i02_finalizer_replay(adapter, request, *, api_server, completion):
    custody = getattr(completion, "custody", None)
    expected = custody.recovery_identity() if custody is not None else None
    try:
        body = await request.json()
    except (ValueError, LookupError):
        body = None
    if not isinstance(expected, dict) or not isinstance(body, dict) or set(body) != set(expected) or any(
        body.get(key) != value for key, value in expected.items()
    ):
        return _recovery_error(api_server, "api_finalizer_identity_mismatch")
    replay = completion.consume_replay(expected)
    if replay is None:
        return _recovery_error(api_server, "api_finalizer_replay_expired")
    from aiohttp import web

    return web.json_response({"run_id": custody.run_id, **replay.as_wire()})


async def _resolve_i02_finalizer_unknown(adapter, request, *, api_server, custody, completion):
    """Retry one authenticated I02 finalizer fence using its original source objects."""
    try:
        body = await request.json()
    except (ValueError, LookupError):
        body = None
    expected = custody.recovery_identity()
    if not isinstance(body, dict) or set(body) != set(expected) or any(
        body.get(key) != value for key, value in expected.items()
    ):
        return _recovery_error(api_server, "api_finalizer_identity_mismatch")
    recovered = completion.recover_unknown(adapter, expected)
    if recovered is None:
        return _recovery_error(api_server, "api_finalizer_recovery_unproven")
    # Retire the source maps before constructing the response while retaining a bounded exact
    # completion replay.  A lost first response can therefore be replayed without leaving live
    # custody indefinitely; any retirement fault restores the original source-owned references.
    from gateway.platforms.api_server_run_completion import prune_replay_registry
    from gateway.platforms.api_server_run_custody import retire_legacy_source_with_completion
    if not completion.retain_replay():
        return _recovery_error(api_server, "api_finalizer_replay_unavailable")
    if not retire_legacy_source_with_completion(custody):
        return _recovery_error(api_server, "api_finalizer_replay_retirement_pending")
    prune_replay_registry(getattr(completion.authority, "_api_finalizer_completions", None))
    from aiohttp import web

    return web.json_response({"run_id": custody.run_id, **recovered.as_wire()})


async def resolve_local_unknown(adapter, request, *, api_server):
    """Resolve only a still-live original I03 handle; return ``None`` for canonical I02 runs."""
    auth_error = adapter._check_run_auth(request, permission="stop")
    if auth_error is not None:
        return auth_error
    run_id = request.match_info["run_id"]
    from gateway.platforms.api_server_runs import _run_not_found, _request_owns_run
    if not _request_owns_run(adapter, request, run_id):
        return _run_not_found(api_server._openai_error, run_id)
    custody = getattr(adapter, "_run_custody", {}).get(run_id)
    if custody is None:
        replay = _replay_completion_for_run(adapter, run_id)
        if replay is not None:
            return await _resolve_i02_finalizer_replay(
                adapter, request, api_server=api_server, completion=replay,
            )
        return None
    from gateway.platforms.api_server_run_completion import (
        api_i02_completion_required,
        completion_for_custody,
    )
    if getattr(custody, "producer_phase", None) == "api_i02":
        completion = completion_for_custody(custody)
        if completion is None:
            if api_i02_completion_required(
                getattr(custody, "admission_authority", None), custody.admission_id
            ):
                return _recovery_error(api_server, "api_finalizer_completion_registry_missing")
        elif (
            getattr(getattr(completion, "receipt", None), "state", None) == "unknown"
            or getattr(completion, "recovery_receipt", None) is not None
        ):
            return await _resolve_i02_finalizer_unknown(
                adapter, request, api_server=api_server, custody=custody, completion=completion
            )
    if not getattr(custody, "unknown_reason", None):
        return None
    try:
        body = await request.json()
    except (ValueError, LookupError):
        body = None
    expected = custody.recovery_identity()
    if not isinstance(body, dict) or set(body) != set(expected) or any(
            body.get(key) != value for key, value in expected.items()):
        from gateway.platforms.api_server_runs import _json_error
        return _json_error(
            api_server._openai_error,
            "The exact original custody identity could not be reconciled.",
            code="run_custody_identity_mismatch", status=409)
    receipt = custody.source_recovery_receipt(adapter)
    if not isinstance(receipt, dict):
        from gateway.platforms.api_server_runs import _json_error
        return _json_error(
            api_server._openai_error,
            "The current source-owned terminal evidence is incomplete.",
            code=f"run_custody_{getattr(receipt, 'code', 'custody_pending')}", status=409)
    refusal = custody.reconcile_unknown_receipt(receipt)
    if refusal is not None:
        from gateway.platforms.api_server_runs import _json_error
        return _json_error(
            api_server._openai_error,
            "The exact original custody handle could not be reconciled.",
            code=f"run_custody_{getattr(refusal, 'code', 'identity_mismatch')}", status=409)
    if custody.released:
        adapter._run_custody.pop(run_id, None)
        adapter._release_run_owner_if_forgotten(run_id)
    from aiohttp import web
    return web.json_response({
        "run_id": run_id,
        "status": "resolved",
        "custody": "original_handle",
    })


def publish_unknown_status(adapter, run_id, custody, reason):
    """Persist the public unknown outcome when a terminal status receipt is not proven."""
    reason_code = getattr(reason, "code", None) or str(reason or "status_commit_unproven")
    fields = {
        "error": "API run terminal status could not be proven.",
        "reason": reason_code,
        "last_event": "run.unknown",
    }
    _, receipt = adapter._set_run_status(
        run_id, "unknown", _return_receipt=True, **fields)
    if custody is not None:
        custody.mark_status_receipt(
            receipt, require_store=run_id in getattr(adapter, "_run_idempotency_ids", set()))
    return fields
