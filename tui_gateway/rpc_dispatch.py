"""JSON-RPC admission and worker dispatch. Rebound onto the server namespace."""

from __future__ import annotations

from .method_ctx import bind_module


def handle_request(req: dict) -> dict | None:
    from hermes_cli.backend_retirement import retirement

    with retirement.work() as admitted:
        if not admitted:
            return _err(req.get("id"), 5035, "backend is retiring; reconnect to continue")
        return _handle_admitted_request(req)


def _handle_admitted_request(req: dict) -> dict | None:
    normalized = _normalize_request(req)
    if isinstance(normalized, dict):
        return normalized
    rid, method, params = normalized
    if not (fn := _methods.get(method)):
        return _err(rid, -32601, f"unknown method: {method} — the client and the Hermes backend are out of sync "
                    "(different versions); run `hermes update` and restart both")
    # Test doubles register straight into ``_methods`` without a contract; every production
    # handler comes through ``register_method`` and therefore has one.
    contract = _contracts.METHODS.get(method)
    if contract is not None:
        params, problem = _contracts.validate_params(contract, params)
        if problem is not None:
            return _err(rid, 4000, problem)
    token = _current_rpc_method.set(method)
    try:
        response = fn(rid, params)
    except ProfileUnavailableError as exc:
        return _err(rid, 4064, str(exc))
    finally:
        _current_rpc_method.reset(token)
    if contract is not None and isinstance(response, dict) and isinstance(response.get("result"), dict):
        _contracts.check_params_accepted(contract, params)
        _contracts.check_result(contract, response["result"])
    return response


def dispatch(req: dict, transport: Optional[Transport] = None) -> dict | None:
    """Route inbound RPCs — long handlers to the pool (returns None; the worker writes its own
    response via the bound transport), everything else inline (returns the response dict).
    *transport* pins every write of this request — events included — to that transport;
    omitted → the module stdio transport (``tui_gateway.entry`` behaviour)."""
    t = transport or _stdio_transport
    token = bind_transport(t)
    try:
        from tui_gateway import server_requests
        if server_requests.is_response_frame(req):
            # The renderer answering one of OUR requests (clarify, approval, …): no response frame goes back.
            if not server_requests.resolve_response(req, t) and not _relay_compute_host_response(req):
                logger.debug("dropping response for unknown server request id=%r", req.get("id"))
            return None
        normalized = _normalize_request(req)
        if isinstance(normalized, dict):
            return normalized
        if normalized[1] not in _LONG_HANDLERS:
            return handle_request(req)
        from hermes_cli.backend_retirement import retirement

        # Reserve BEFORE enqueueing: a queued handler has accepted work even though no worker runs yet.
        lease = retirement.acquire()
        if lease is None:
            return _err(req.get("id"), 5035, "backend is retiring; reconnect to continue")
        from tui_gateway.hosted_tui_custody import rpc_admission, rpc_settle
        custody, custody_reason = rpc_admission(req)
        if custody_reason is not None:
            retirement.release(lease)
            return _err(req.get("id"), 5035, custody_reason)
        try:
            ctx = contextvars.copy_context()  # the pool worker must see the bound transport
            owner = normalized[2].get("owner")
            if normalized[1] in _CONNECTOR_RPC_METHODS and isinstance(owner, dict) and owner.get("type") == "session":
                ctx.run(_capture_connector_rpc_owner, normalized[2])

            def run():
                try:
                    resp = _handle_admitted_request(req)
                except Exception as exc:
                    resp = _err(req.get("id"), -32000, f"handler error: {exc}")
                if resp is not None:
                    t.write(resp)
                rpc_settle(custody, known=True)
            def run_with_custody():
                try:
                    return ctx.run(retirement.run_with_lease, lease, run)
                except BaseException:
                    rpc_settle(custody, known=False)
                    raise
        except BaseException:
            rpc_settle(custody, known=False)
            retirement.release(lease)
            raise
        # submit() may raise after accepting the callable. Its wrapper owns the
        # release; an ambiguous submission must keep custody until it unwinds.
        try:
            future = _pool.submit(run_with_custody)
        except BaseException:
            rpc_settle(custody, known=False)
            # Submission may have accepted the worker before raising. Its wrapper owns the
            # lease until output settles; an unobserved submission remains UNKNOWN.
            raise
        retirement.observe_future_cancellation(future, lease)
        future.add_done_callback(
            lambda done: rpc_settle(custody, known=False)
            if done.cancelled() or done.exception() is not None else None)
        return None
    finally:
        reset_transport(token)


def register(server):
    bind_module(globals(), server)
