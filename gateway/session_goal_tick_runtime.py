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


_I04_PARENT = "I04_fifo_turn_execution"
_I08_CHILD = "I08_goals_loops_heartbeat"
_MISSING_ORIGIN = object()
_INVALID_ORIGIN = object()


def _authenticated_goal_parent(
    authority, session_id, admission_id, generation, principal_id, handle,
):
    """Return the exact active I04 parent for one newly reserved I08 handle."""
    values = (session_id, admission_id, principal_id)
    if (any(type(value) is not str or not value for value in values)
            or type(generation) is not int or generation < 1
            or getattr(handle, "owner_class", None) != _I08_CHILD):
        return None
    child_lease = getattr(handle, "lease", None)
    parent_lease_id = getattr(child_lease, "parent_lease_id", None)
    if type(parent_lease_id) is not str or not parent_lease_id:
        return None
    getter = getattr(authority, "_preservation_operation_parent", None)
    coordinator_getter = getattr(authority, "_preservation_coordinator", None)
    if not callable(getter) or not callable(coordinator_getter):
        return None
    try:
        parent = getter(
            session_id, admission_id, generation=generation, principal_id=principal_id,
        )
        coordinator = coordinator_getter()
        validator = getattr(authority, "_preservation_operation_validate_parent_handle", None)
        instance_id = getattr(authority, "instance_id", None)
        if (parent is None or getattr(parent, "owner_class", None) != _I04_PARENT
                or coordinator is None or not callable(validator)
                or type(instance_id) is not str or not instance_id):
            return None
        if validator(
            parent, coordinator, getattr(authority, "profile_id", None),
            getattr(authority, "epoch", None), instance_id,
        ) is not parent:
            return None
        parent_lease = getattr(parent, "lease", None)
        if getattr(parent_lease, "lease_id", None) != parent_lease_id:
            return None
        for candidate in (parent, handle):
            if (getattr(candidate, "profile_id", None) != getattr(authority, "profile_id", None)
                    or getattr(candidate, "authority_epoch", None) != getattr(authority, "epoch", None)
                    or getattr(candidate, "process_instance", None) != instance_id
                    or getattr(candidate, "coordinator_id", None)
                    != getattr(coordinator, "_coordinator_id", None)):
                return None
        records = getattr(coordinator, "_records", None)
        record = records.get(parent_lease_id) if isinstance(records, dict) else None
        if (record is None or getattr(record, "handle", None) is not parent
                or getattr(record, "state", None) not in {"accepted", "finalizing"}):
            return None
    except (AttributeError, KeyError, LookupError, OSError, RuntimeError,
            sqlite3.Error, TypeError, ValueError):
        return None
    return parent


def _capture_goal_delivery_origin(
    authority, session_id, admission_id, generation, principal_id, handle,
):
    """Capture the source-owned ingress event before the waiter cleanup removes its map entry."""
    parent = _authenticated_goal_parent(
        authority, session_id, admission_id, generation, principal_id, handle,
    )
    if parent is None:
        parent_lease_id = getattr(getattr(handle, "lease", None), "parent_lease_id", None)
        return _INVALID_ORIGIN if isinstance(parent_lease_id, str) and parent_lease_id else None
    adopters = getattr(authority, "marker_adopters", None)
    origin = adopters.get(admission_id) if isinstance(adopters, dict) else None
    # A parent without the exact already-installed marker is a real provenance failure.  Keep the
    # authenticated parent in the capture with origin=None so the later callback cannot fall back
    # to the restored event or treat an empty current map as proof.
    return (admission_id, generation, session_id, principal_id, parent, origin)


def _captured_goal_delivery_origin(authority, ticks, captured, registered_generation):
    """Validate the registration snapshot and every I08->I04 edge before delivery release."""
    if captured is _MISSING_ORIGIN:
        return _MISSING_ORIGIN
    capture = captured
    if not isinstance(capture, tuple) or len(capture) != 6:
        return None
    admission_id, generation, session_id, principal_id, parent, origin = capture
    if (type(admission_id) is not str or not admission_id
            or type(generation) is not int or generation < 1
            or type(session_id) is not str or not session_id
            or type(principal_id) is not str or not principal_id
            or origin is None or parent is None
            or registered_generation != generation):
        return None
    try:
        coordinator = getattr(authority, "_preservation_coordinator", lambda: None)()
    except (AttributeError, RuntimeError, TypeError, ValueError):
        return None
    instance_id = getattr(authority, "instance_id", None)
    if (coordinator is None or type(instance_id) is not str or not instance_id
            or getattr(parent, "owner_class", None) != _I04_PARENT
            or getattr(parent, "profile_id", None) != getattr(authority, "profile_id", None)
            or getattr(parent, "authority_epoch", None) != getattr(authority, "epoch", None)
            or getattr(parent, "process_instance", None) != instance_id
            or getattr(parent, "coordinator_id", None)
            != getattr(coordinator, "_coordinator_id", None)):
        return None
    parent_lease_id = getattr(getattr(parent, "lease", None), "lease_id", None)
    if type(parent_lease_id) is not str or not parent_lease_id:
        return None
    adopters = getattr(authority, "marker_adopters", None)
    if not isinstance(adopters, dict):
        return None
    if admission_id in adopters and adopters[admission_id] is not origin:
        return None
    for tick in ticks:
        handle = getattr(tick, "handle", None)
        lease = getattr(handle, "lease", None)
        if (getattr(handle, "owner_class", None) != _I08_CHILD
                or getattr(lease, "parent_lease_id", None) != parent_lease_id
                or getattr(handle, "profile_id", None) != getattr(authority, "profile_id", None)
                or getattr(handle, "authority_epoch", None) != getattr(authority, "epoch", None)
                or getattr(handle, "process_instance", None) != instance_id
                or getattr(handle, "coordinator_id", None)
                != getattr(coordinator, "_coordinator_id", None)):
            return None
    return origin


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
            origin = _capture_goal_delivery_origin(
                authority, session_entry.session_id, admission_id, generation,
                principal_id, tick.handle,
            )
            if origin is None:
                if hasattr(event, "_i08_delivery_origin"):
                    delattr(event, "_i08_delivery_origin")
            else:
                event._i08_delivery_origin = origin
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

    origin_snapshot = getattr(event, "_i08_delivery_origin", _MISSING_ORIGIN)
    registration_generation = getattr(event, "_hermes_run_generation", None)
    captured_ticks = tuple(ticks)

    async def _settle_after_delivery() -> None:
        try:
            from gateway.platforms.base_delivery_callbacks import _post_delivery_outcome
            outcome = _post_delivery_outcome()
            if outcome is None:
                for tick in ticks:
                    mark_unknown(tick, "join_failed")
                return
            origin = _captured_goal_delivery_origin(
                authority, captured_ticks, origin_snapshot, registration_generation,
            )
            event_matches = (
                outcome.event is event if origin is _MISSING_ORIGIN
                else origin is not None and outcome.event is origin
            )
            if not event_matches or outcome.session_key != session_key:
                for tick in ticks:
                    mark_unknown(tick, "join_failed")
                return
            if outcome.processing_ok is not True:
                for tick in ticks:
                    mark_unknown(tick, "join_failed")
                return
            for captured_tick in captured_ticks:
                handle = getattr(captured_tick, "handle", None)
                tick = view_for_handle(authority, handle)
                if tick is None or _bound_admission(authority, handle) is not None:
                    continue
                if not finish_unbound(tick, success=True):
                    mark_unknown(tick, "join_failed")
        except BaseException:
            for tick in ticks:
                mark_unknown(tick, "join_failed")
            raise

    try:
        register(session_key, _settle_after_delivery, generation=registration_generation)
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
