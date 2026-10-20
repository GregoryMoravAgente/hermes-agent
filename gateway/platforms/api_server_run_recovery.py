"""Exact-handle recovery for API custody that cannot prove terminal ownership."""

from __future__ import annotations


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
    if custody is None or not getattr(custody, "unknown_reason", None):
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
