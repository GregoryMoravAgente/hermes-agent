"""Runtime-facing goal tick helpers.

The goal manager remains the source of the decision.  These helpers only keep the
typed I08 reservation and the existing adapter FIFO/post-delivery boundaries
together so the runner facade stays small and the original paths remain readable.
"""
from __future__ import annotations

import asyncio
import logging
import sqlite3

from gateway.config import Platform

logger = logging.getLogger("gateway.run")


def _event_tick_handles(event):
    if event is None:
        return ()
    handles = []
    entries = getattr(event, "_i08_tick_handles", ())
    if isinstance(entries, (list, tuple)):
        for item in entries:
            if isinstance(item, tuple) and len(item) == 2:
                kind, handle = item
            else:
                kind, handle = None, item
            if handle is not None and not any(existing is handle for _, existing in handles):
                handles.append((kind, handle))
    legacy = getattr(event, "_i08_tick_handle", None)
    if legacy is not None and not any(existing is legacy for _, existing in handles):
        handles.append((getattr(event, "_i08_tick_kind", None), legacy))
    return tuple(handles)


def reserve_goal_tick(runner, session_entry, source, event):
    """Reserve a goal decision beneath the live I04 admission, or refuse it closed."""
    if event is None:
        return None, None, False
    from gateway.session_authorities import active_authority
    from gateway.session_automation import automation_source_identity
    from gateway.session_goal_tick_custody import attach_tick_handle, reserve_goal

    authority = active_authority(runner)
    if authority is None:
        return None, None, False
    try:
        key = runner._session_key_for_source(source)
        source_event = runner._synthetic_prompt_event(source, "", internal=True)
        coordinator = authority._preservation_coordinator()
        if coordinator is None:
            return authority, None, False
        live = authority.sessions.get(session_entry.session_id)
        execution = getattr(getattr(live, "event_stream", None), "execution", None) or {}
        admission_id = execution.get("admission_id")
        generation = getattr(event, "_hermes_run_generation", None)
        request_id = f"goal:{admission_id}:{generation}"
        turn = getattr(runner._session_state(key), "turn", None)
        turn_lease = getattr(turn, "lease_tokens", {}).get(generation)
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(authority.db, admission_id=admission_id)
        principal_id = row.get("principal_id") if isinstance(row, dict) else None
        if type(principal_id) is not str or not principal_id:
            raise RuntimeError("preservation_owner_unmapped")
        tick = reserve_goal(
            authority, event=event, session_id=session_entry.session_id,
            request_id=request_id, principal_id=principal_id,
            source_identity=automation_source_identity(source_event, key),
            turn_lease=turn_lease,
        )
        if tick is not None:
            attach_tick_handle(event, tick, "goal")
    except (AttributeError, KeyError, OSError, RuntimeError, sqlite3.Error, TypeError, ValueError) as exc:
        logger.debug("goal continuation custody reservation failed: %s", exc)
        return authority, None, True
    # A legacy/session-test path may have no live I04 operation parent. It keeps the historical
    # FIFO behavior, but never presents that fallback as an I08 custody proof.
    return authority, tick, False


def _generic_callback_contains(callback, target) -> bool:
    if callback is target:
        return True
    chain = getattr(callback, "_post_delivery_callback_chain", None)
    return isinstance(chain, tuple) and any(
        _generic_callback_contains(item, target) for item in chain
    )


def register_tick_delivery_finalizer(runner, source, event) -> bool:
    """Settle an unbound I08 tick only at the real adapter delivery boundary.

    Managed child admissions already have the automation delivery/receipt finalizer.  This
    callback is therefore only for an explicit no-child decision (or a legacy tick that never
    became a durable child); it never releases a bound child or treats a missing callback as
    success.  The generic callback lane is used because the Base exact lane is owned by the
    classic writer occurrence for the same inbound event.
    """
    handles = _event_tick_handles(event)
    if not handles or getattr(event, "_i08_delivery_finalizer_registered", False):
        return not handles or bool(getattr(event, "_i08_delivery_finalizer_registered", False))
    from gateway.session_authorities import active_authority
    from gateway.session_goal_tick_custody import (
        _bound_admission, mark_unknown, view_for_handle, finish_unbound,
    )

    authority = active_authority(runner)
    ticks = []
    for _kind, handle in handles:
        tick = view_for_handle(authority, handle)
        if tick is not None and _bound_admission(authority, handle) is None:
            ticks.append(tick)
    if not ticks:
        return True
    getter = getattr(authority, "_preservation_coordinator", None)
    coordinator = getter() if callable(getter) else None
    if coordinator is not None:
        try:
            if coordinator.status().get("state") != "open":
                from gateway.session_goal_tick_custody import mark_late_registration
                mark_late_registration(authority)
                for tick in ticks:
                    mark_unknown(tick, "runtime_draining")
                return False
        except (AttributeError, RuntimeError, TypeError, ValueError):
            for tick in ticks:
                mark_unknown(tick, "delivery_callback_registration_failed")
            return False
    adapter = runner._delivery_adapter_for(source)
    session_key = runner._session_key_for_source(source)
    register = getattr(adapter, "register_post_delivery_callback", None) if adapter is not None else None
    if not isinstance(session_key, str) or not session_key or not callable(register):
        for tick in ticks:
            mark_unknown(tick, "delivery_callback_missing")
        return False

    async def _settle_after_delivery() -> None:
        try:
            for _kind, handle in _event_tick_handles(event):
                tick = view_for_handle(authority, handle)
                if tick is None or _bound_admission(authority, handle) is not None:
                    continue
                if not finish_unbound(tick, success=True):
                    mark_unknown(tick, "delivery_finalizer_refused")
        except BaseException:
            for tick in ticks:
                mark_unknown(tick, "delivery_finalizer_failed")
            raise

    generation = getattr(event, "_hermes_run_generation", None)
    try:
        register(session_key, _settle_after_delivery, generation=generation)
        entry = getattr(adapter, "_post_delivery_callbacks", {}).get(session_key)
        callback = entry[1] if isinstance(entry, tuple) and len(entry) == 2 else entry
        if not _generic_callback_contains(callback, _settle_after_delivery):
            for tick in ticks:
                mark_unknown(tick, "delivery_callback_unregistered")
            return False
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        for tick in ticks:
            mark_unknown(tick, "delivery_callback_registration_failed")
        return False
    event._i08_delivery_finalizer_registered = True
    return True


async def apply_goal_decision(runner, source, message, prompt, goal_tick) -> None:
    """Apply one manager decision through the existing delivery and FIFO seams."""
    if not prompt or source is None:
        if goal_tick is not None:
            if message and source is not None:
                await runner._defer_goal_status_notice_after_delivery(source, message)
        elif message and source is not None:
            await runner._defer_goal_status_notice_after_delivery(source, message)
        return

    if goal_tick is not None:
        from gateway.session_automation import admit_route_wake
        from gateway.session_goal_tick_custody import mark_unknown, release_unaccepted
        try:
            admitted = await admit_route_wake(
                runner, runner._delivery_adapter_for(source), source, prompt,
                "goal-continuation", _preservation_handle=goal_tick.handle,
            )
        except BaseException:
            mark_unknown(goal_tick, "join_failed")
            raise
        if not admitted:
            release_unaccepted(goal_tick)
        if message and source is not None:
            await runner._defer_goal_status_notice_after_delivery(source, message)
        return

    if getattr(source, "platform", None) == Platform.LOCAL:
        from gateway.session_automation import admit_route_wake
        try:
            await admit_route_wake(
                runner, runner._delivery_adapter_for(source), source, prompt, "goal-continuation",
            )
        except Exception:  # health: allow BLE001 -- best-effort local FIFO admission is logged below
            logger.debug("goal continuation: local admission failed", exc_info=True)
        return

    if message and source is not None:
        await runner._defer_goal_status_notice_after_delivery(source, message)
    try:
        adapter = runner._delivery_adapter_for(source)
        session_key = runner._session_key_for_source(source)
        if adapter and session_key:
            runner._enqueue_fifo(
                session_key, runner._synthetic_prompt_event(source, prompt), adapter,
            )
    except Exception as exc:  # health: allow BLE001 -- opaque adapter enqueue failure stays retryable and is logged
        logger.debug("goal continuation: enqueue failed: %s", exc)
