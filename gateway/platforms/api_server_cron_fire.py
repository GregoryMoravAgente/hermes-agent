"""Chronos cron-fire admission and detached execution boundary.

This sibling owns the API-specific admission path so the facade remains a routing seam.
It deliberately late-imports the facade's verifier and pending-work helpers to avoid import cycles.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3
import uuid
from contextlib import nullcontext, suppress
from typing import Any, Optional

from aiohttp import web


logger = logging.getLogger(__name__)


async def _authenticate_fire(request: web.Request) -> Optional[dict[str, Any]]:
    """Verify the NAS token without admitting any request on verifier failure."""
    from gateway.platforms.api_server import _call_verifier
    from hermes_cli.config import cfg_get, load_config
    from plugins.cron_providers.chronos.verify import get_fire_verifier

    auth = request.headers.get("Authorization", "")
    token = auth[7:].strip() if auth.startswith("Bearer ") else ""
    cfg = await asyncio.to_thread(load_config)
    verifier = get_fire_verifier()
    verify_kwargs = {
        "token": token,
        "expected_audience": cfg_get(cfg, "cron", "chronos", "expected_audience", default=""),
        "jwks_or_key": cfg_get(cfg, "cron", "chronos", "nas_jwks_url", default="") or None,
        "issuer": cfg_get(cfg, "cron", "chronos", "portal_url", default="") or None,
    }
    try:
        return await _call_verifier(verifier, **verify_kwargs)
    except Exception:  # health: allow BLE001 -- verifier is an opaque auth boundary and must fail closed
        logger.exception("cron fire: verifier crashed; rejecting token")
        return None


def _paused_response(job_id: str) -> Optional[web.Response]:
    """Return the operator pause response, if the existing ESTOP gate is active."""
    with suppress(ImportError):
        from agent.estop import check_paused

        if check_paused("cron-webhook", logger):
            return web.json_response(
                {"error": "hermes is paused (ESTOP)", "job_id": job_id},
                status=503,
                headers={"Retry-After": "60"},
            )
    return None


def _reserve_api_custody(provider: Any, job_id: str, managed_runtime: bool):
    """Reserve I14 only when the provider can return the exact I12 receipt."""
    from cron.runtime_custody import (
        CronCustodyRefused,
        reserve_execution,
    )
    from cron.scheduler_provider import provider_supports_split_fire

    if managed_runtime and not provider_supports_split_fire(provider):
        return None, web.json_response(
            {"error": "cron provider has no custody receipt", "job_id": job_id}, status=503)
    if not managed_runtime:
        return None, None
    try:
        custody = reserve_execution(
            owner_class="I14_api_detached_cron",
            job_id=job_id,
            source="api-cron",
            required=True,
            owner_id=f"api:{uuid.uuid4().hex}",
        )
    except (CronCustodyRefused, RuntimeError) as exc:
        if isinstance(exc, CronCustodyRefused):
            logger.warning("cron fire custody refused for %s: %s", job_id, exc.code)
        else:
            logger.warning("cron fire custody unavailable for %s", job_id)
        return None, web.json_response(
            {"error": "cron fire custody unavailable", "job_id": job_id}, status=503)
    return custody, None


def _finish_detached(
    adapter: Any, reservation: dict[str, Any], api_custody: Any, fire_binding: Any,
) -> None:
    """Consume terminal custody before releasing the API pending-work reservation."""
    try:
        if fire_binding is not None:
            fire_binding.observe_terminal()
        if api_custody is not None:
            api_custody.observe_terminal()
    except (
        asyncio.CancelledError,
        AttributeError,
        KeyError,
        LookupError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
        sqlite3.Error,
    ):
        if fire_binding is not None:
            fire_binding.mark_unknown("read_failed")
        if api_custody is not None:
            api_custody.mark_unknown("read_failed")
    finally:
        from gateway.platforms.api_server import _release_pending_api_work

        _release_pending_api_work(adapter, reservation)


def _detach_fire(
    adapter: Any,
    reservation: dict[str, Any],
    api_custody: Any,
    fire_binding: Any,
    fire_fn: Any,
    *fire_args: Any,
    adapters: Any,
    loop: Any,
    job_id: str,
) -> web.Response:
    """Detach one provider call while retaining both exact custody bindings."""
    work = asyncio.to_thread(fire_fn, *fire_args, adapters=adapters, loop=loop)
    try:
        task = asyncio.create_task(work)
    except (RuntimeError, TypeError, ValueError):
        work.close()
        if fire_binding is not None:
            fire_binding.mark_unknown("read_failed")
        if api_custody is not None:
            api_custody.mark_unknown("read_failed")
        raise
    if api_custody is not None:
        api_custody.bind_future(task)
        api_custody.clear_claim_future()
    if fire_binding is not None:
        fire_binding.bind_future(task)
    reservation["detached"] = True
    task.add_done_callback(
        lambda _task: _finish_detached(adapter, reservation, api_custody, fire_binding))
    adapter._track_background_task(task, tolerate_missing=True)
    return web.json_response({"status": "accepted", "job_id": job_id}, status=202)


def _start_split_claim(provider: Any, job_id: str, api_custody: Any):
    from cron.runtime_custody import require_cron_custody

    def _claim_with_custody():
        with (require_cron_custody(api_custody) if api_custody is not None else nullcontext()):
            return provider.claim_fire(job_id)

    work = asyncio.to_thread(_claim_with_custody)
    try:
        task = asyncio.create_task(work)
    except (RuntimeError, TypeError, ValueError):
        work.close()
        if api_custody is not None:
            api_custody.mark_unknown("read_failed")
        raise
    if api_custody is not None:
        api_custody.bind_claim_future(task)
    return task


async def _await_split_claim(task: Any, api_custody: Any, job_id: str):
    try:
        return await asyncio.shield(task), None
    except asyncio.CancelledError:
        # The provider thread is still the authority for whether a durable claim committed. Keep
        # the exact task bound and let the cancellation continuation reconcile its real result;
        # marking the coordinator unknown here would make a later proven ``None`` impossible to
        # release and would also poison a valid claimed receipt before detach.
        raise
    except Exception:  # health: allow BLE001 -- provider failure is an opaque retryable boundary
        if api_custody is not None:
            api_custody.mark_unknown("read_failed")
        logger.exception("cron fire admission failed")
        return None, web.json_response(
            {"error": "cron fire admission failed", "job_id": job_id}, status=503)


def _release_cancelled_pending(adapter: Any, reservation: dict[str, Any]) -> None:
    from gateway.platforms.api_server import _release_pending_api_work

    _release_pending_api_work(adapter, reservation)


def _finish_cancelled_split_continuation(
    task: asyncio.Task, adapter: Any, reservation: dict[str, Any], api_custody: Any,
) -> None:
    """Release only API bookkeeping when shutdown cancels before continuation code starts.

    The claim task remains attached to the exact custody binding; an early-cancelled continuation
    has no opportunity to await or reconcile that task, so the binding stays sticky unknown.
    """
    if not task.cancelled():
        return
    try:
        if api_custody is not None:
            api_custody.mark_unknown("claim_cancelled")
    finally:
        _release_cancelled_pending(adapter, reservation)


async def _continue_cancelled_split_fire(
    adapter: Any,
    reservation: dict[str, Any],
    api_custody: Any,
    claim_task: Any,
    provider: Any,
    job_id: str,
    adapters: Any,
    loop: Any,
) -> None:
    """Finish a claim whose request task was cancelled, retaining the exact receipt or task."""
    try:
        claimed_job, claim_error = await _await_split_claim(claim_task, api_custody, job_id)
        if claim_error is not None:
            if api_custody is not None:
                api_custody.clear_claim_future(claim_task)
            _release_cancelled_pending(adapter, reservation)
            return
        if claimed_job is None:
            if api_custody is not None:
                api_custody.clear_claim_future(claim_task)
                api_custody.release_unaccepted()
            _release_cancelled_pending(adapter, reservation)
            return
        fire_binding, claim_error = _validate_claimed_fire(api_custody, job_id, claimed_job)
        if claim_error is not None:
            if api_custody is not None:
                api_custody.clear_claim_future(claim_task)
            _release_cancelled_pending(adapter, reservation)
            return
        _detach_fire(
            adapter, reservation, api_custody, fire_binding, provider.fire_claimed, claimed_job,
            adapters=adapters, loop=loop, job_id=job_id)
    except asyncio.CancelledError:
        # The claim task remains bound to ``api_custody``. A cancellation of this continuation is
        # itself ambiguous and therefore sticky; the original claim task is intentionally not
        # cancelled or released.
        if api_custody is not None:
            api_custody.mark_unknown("claim_cancelled")
        _release_cancelled_pending(adapter, reservation)
    except Exception:  # health: allow BLE001 -- retain ambiguous handoff custody; release only API bookkeeping
        if api_custody is not None:
            api_custody.mark_unknown("read_failed")
            api_custody.clear_claim_future(claim_task)
        _release_cancelled_pending(adapter, reservation)
        logger.exception("cron fire cancelled-claim continuation failed")


async def _dispatch_split_fire(
    adapter: Any,
    reservation: dict[str, Any],
    api_custody: Any,
    provider: Any,
    job_id: str,
    request: web.Request,
    received_at: float,
) -> web.Response:
    """Claim the split provider's durable attempt, then hand it to the existing detached path."""
    loop = asyncio.get_running_loop()
    from gateway.platforms.api_server_fire_startup import live_adapters_once_started
    from cron.scheduler_provider import provider_supports_split_fire

    try:
        refusal, adapters = await live_adapters_once_started(
            adapter, request, job_id, received_at=received_at)
    except asyncio.CancelledError:
        if api_custody is not None:
            api_custody.release_unaccepted()
        raise
    if refusal is not None:
        if api_custody is not None:
            api_custody.release_unaccepted()
        return refusal
    if not provider_supports_split_fire(provider):
        return _detach_fire(
            adapter, reservation, api_custody, None, provider.fire_due, job_id,
            adapters=adapters, loop=loop, job_id=job_id)

    claim_task = _start_split_claim(provider, job_id, api_custody)
    try:
        claimed_job, claim_error = await _await_split_claim(claim_task, api_custody, job_id)
    except asyncio.CancelledError:
        try:
            continuation = asyncio.create_task(_continue_cancelled_split_fire(
                adapter, reservation, api_custody, claim_task, provider, job_id, adapters, loop))
        except (RuntimeError, TypeError, ValueError):
            if api_custody is not None:
                api_custody.mark_unknown("read_failed")
            raise
        reservation["detached"] = True
        continuation.add_done_callback(
            lambda task: _finish_cancelled_split_continuation(
                task, adapter, reservation, api_custody))
        adapter._track_background_task(continuation, tolerate_missing=True)
        raise
    if claim_error is not None:
        if api_custody is not None:
            api_custody.clear_claim_future(claim_task)
        return claim_error
    if claimed_job is None:
        if api_custody is not None:
            api_custody.clear_claim_future(claim_task)
            api_custody.release_unaccepted()
        return web.json_response({"status": "duplicate", "job_id": job_id}, status=200)
    fire_binding, claim_error = _validate_claimed_fire(api_custody, job_id, claimed_job)
    if claim_error is not None:
        if api_custody is not None:
            api_custody.clear_claim_future(claim_task)
        return claim_error
    return _detach_fire(
        adapter, reservation, api_custody, fire_binding, provider.fire_claimed, claimed_job,
        adapters=adapters, loop=loop, job_id=job_id)


def _validate_claimed_fire(api_custody: Any, job_id: str, claimed_job: Any):
    from cron.runtime_custody import CronCustodyRefused

    if not isinstance(claimed_job, dict):
        if api_custody is not None:
            api_custody.mark_unknown("read_failed")
        return None, web.json_response(
            {"error": "cron fire admission unavailable", "job_id": job_id}, status=503)
    fire_binding = claimed_job.get("_cron_custody")
    if fire_binding is None and api_custody is not None:
        api_custody.mark_unknown("unsupported_external_owner")
        return None, web.json_response(
            {"error": "cron provider returned no custody receipt", "job_id": job_id}, status=503)
    if fire_binding is not None and api_custody is not None:
        try:
            api_custody.validate_child_receipt(
                fire_binding, job_id=job_id, execution_id=claimed_job.get("execution_id"))
        except CronCustodyRefused:
            api_custody.mark_unknown("foreign_custody")
            return None, web.json_response(
                {"error": "cron provider custody mismatch", "job_id": job_id}, status=503)
    return fire_binding, None


async def _handle_authenticated_fire(
    adapter: Any, request: web.Request, reservation: dict[str, Any], received_at: float,
) -> web.Response:
    """Run the authenticated API admission and handoff sequence."""
    body: Any = {}
    with suppress(Exception):
        body = await request.json()
    job_id = (body or {}).get("job_id")
    if not job_id:
        return web.json_response({"error": "missing job_id"}, status=400)
    paused = _paused_response(job_id)
    if paused is not None:
        return paused
    from cron.runtime_custody import managed_runtime_available
    from cron.scheduler_provider import resolve_cron_scheduler

    provider = resolve_cron_scheduler()
    managed_runtime = managed_runtime_available()
    api_custody, refusal = _reserve_api_custody(provider, job_id, managed_runtime)
    if refusal is not None:
        return refusal
    return await _dispatch_split_fire(
        adapter, reservation, api_custody, provider, job_id, request, received_at)


async def handle_cron_fire(adapter: Any, request: web.Request) -> web.Response:
    """POST /api/cron/fire — authenticated Chronos fire with detached durable custody."""
    from gateway.platforms.api_server import _reserve_pending_api_work

    received_at = asyncio.get_running_loop().time()
    claims = await _authenticate_fire(request)
    if claims is None:
        logger.warning(
            "cron fire: rejected invalid token: %s",
            adapter._request_audit_log_suffix(request),
        )
        return web.json_response(
            {"error": "invalid fire token"}, status=401)
    draining = adapter._draining_response()
    if draining is not None:
        return draining
    with _reserve_pending_api_work(adapter) as reservation:
        return await _handle_authenticated_fire(adapter, request, reservation, received_at)
