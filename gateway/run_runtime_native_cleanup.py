"""Source-owned native cleanup for a committed preservation handoff.

The coordinator owns the close and the opaque retry capability.  This module only obtains the
source participant snapshot and invokes each participant outside the coordinator lock.  It never
turns an absent registry, a copied receipt, or an empty caller payload into a zero-work proof.
"""
from __future__ import annotations

import asyncio
import inspect
import time
from typing import Any

from gateway.runtime_custody import (
    CustodyRefusal,
    DrainReceipt,
    NativeCleanupPlan,
    NativeCleanupResult,
    RuntimeCustodyCoordinator,
)
from gateway.runtime_custody_profiles import ProfileScopeResolver


def _source_plan_provider(runner):
    provider = getattr(runner, "_preservation_native_cleanup_plan", None)
    return provider if callable(provider) else None


async def _source_plan(runner, coordinator, receipt):
    provider = _source_plan_provider(runner)
    if provider is None:
        return None
    try:
        plan = provider(coordinator, receipt)
        if inspect.isawaitable(plan):
            plan = await plan
    except Exception:  # health: allow BLE001 -- opaque source failure retains native custody without exposing values
        return None
    return plan if isinstance(plan, NativeCleanupPlan) else None


async def prepare_native_cleanup_plan(runner, coordinator, receipt):
    """Capture and bind the source-owned native participant graph before ACK."""
    plan = await _source_plan(runner, coordinator, receipt)
    if plan is None:
        return CustodyRefusal("native_cleanup_missing")
    return coordinator.bind_native_cleanup_plan(receipt, plan)


async def _await_source_completion(value, timeout: float):
    if inspect.isawaitable(value):
        if timeout <= 0:
            raise TimeoutError
        return await asyncio.wait_for(value, timeout=timeout)
    return value


async def settle_native_cleanup(runner, coordinator, capability):
    """Run exact source finalizers and retain unresolved participants on every failure."""
    started = coordinator.begin_native_cleanup(capability)
    if isinstance(started, CustodyRefusal):
        return started
    result, participants = started
    if result.cleanup == "complete" or not participants:
        return result
    plan = capability.plan
    if plan is None:
        return result
    deadline = None
    close = getattr(capability, "close", None)
    if close is not None:
        deadline = close.deadline
    for source, source_receipt, source_capability in participants:
        remaining = coordinator.remaining_seconds()
        if deadline is not None:
            remaining = min(remaining if remaining is not None else 0.0,
                            max(0.0, deadline - time.monotonic()))
        if remaining is None or remaining <= 0:
            return coordinator.finish_native_cleanup(
                capability, (), reason="native_cleanup_deadline", unknown=True,
            )
        finish = getattr(source_capability, "finish_native_cleanup", None)
        if not callable(finish):
            return coordinator.finish_native_cleanup(
                capability, (), reason="native_cleanup_capability_missing", unknown=True,
            )
        try:
            completion = finish(
                coordinator=coordinator,
                capability=capability,
                source_receipt=source_receipt,
                native_fences=plan.native_fences,
                deadline=remaining,
            )
            completion = await _await_source_completion(completion, remaining)
            if deadline is not None and time.monotonic() > deadline:
                return await _mark_unknown(
                    coordinator, capability, "native_cleanup_deadline",
                )
        except TimeoutError:
            return await _mark_unknown(coordinator, capability, "native_cleanup_timeout")
        except Exception:  # health: allow BLE001 -- opaque source failure retains the exact participant
            return await _mark_unknown(coordinator, capability, "native_cleanup_unknown")
        applied = coordinator.finish_native_cleanup(capability, (completion,))
        if isinstance(applied, CustodyRefusal):
            return applied
        result = applied
        if result.cleanup == "unknown":
            return result
    return coordinator.native_cleanup_result(capability.closed_receipt)


async def _mark_unknown(coordinator, capability, reason: str):
    result = coordinator.finish_native_cleanup(
        capability, (), reason=reason, unknown=True,
    )
    return result


async def acknowledge_native_cleanup(runner, coordinator, receipt):
    """ACK a committed handoff, then settle its retained native source graph."""
    if receipt.state == "handoff_pending":
        bound = await prepare_native_cleanup_plan(runner, coordinator, receipt)
        if isinstance(bound, CustodyRefusal):
            return bound
        result = coordinator.acknowledge_native(receipt)
    elif receipt.state == "closed":
        result = coordinator.acknowledge_native(receipt)
        if not isinstance(result, CustodyRefusal) and result.cleanup != "complete":
            provider = _source_plan_provider(runner)
            if provider is not None:
                observed = await _source_plan(runner, coordinator, receipt)
                if observed is not None and observed is not result.capability.plan:
                    started = coordinator.begin_native_cleanup(result.capability)
                    if isinstance(started, CustodyRefusal):
                        return started
                    return coordinator.finish_native_cleanup(
                        result.capability, (), reason="native_cleanup_plan_replaced", unknown=True,
                    )
    else:
        return CustodyRefusal("native_ack_mismatch")
    if isinstance(result, CustodyRefusal):
        return result
    if not isinstance(result, NativeCleanupResult):
        return CustodyRefusal("native_cleanup_result_invalid")
    if result.cleanup == "complete":
        return result
    return await settle_native_cleanup(runner, coordinator, result.capability)


def pending_wire(runner, params):
    """Return the retained closed receipt for a timed-out private socket operation."""
    receipt = params.get("receipt") if isinstance(params, dict) else None
    if not isinstance(receipt, dict) or receipt.get("status") not in {"handoff_pending", "closed"}:
        return None
    try:
        parsed = DrainReceipt(
            receipt["receipt_id"], receipt["process_instance"],
            receipt["authority_epoch"], receipt["close_generation"], receipt["status"],
        )
    except (KeyError, TypeError, ValueError):
        return None
    coordinator = getattr(runner, "runtime_custody", None)
    if coordinator is None:
        return None
    if parsed.state == "handoff_pending":
        capability = coordinator.native_cleanup_capability(parsed)
        if isinstance(capability, CustodyRefusal):
            return None
        parsed = capability.closed_receipt
    result = coordinator.native_cleanup_result(parsed)
    return result.as_wire() if isinstance(result, NativeCleanupResult) else None


def install_runtime_custody(runner) -> RuntimeCustodyCoordinator:
    descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
    registry = getattr(runner, "session_authorities", None)
    coordinator = RuntimeCustodyCoordinator(
        process_instance=descriptor.get("instance_id", ""),
        authority_epoch=descriptor.get("authority_epoch", 0),
        profile_scope=ProfileScopeResolver(runner) if registry is not None else None,
    )
    runner.runtime_custody = coordinator
    authorities = list(registry) if registry is not None else [getattr(runner, "session_authority", None)]
    for authority in authorities:
        if authority is None:
            continue
        bind_graph = getattr(authority, "_register_preservation_producer_graph", None)
        if callable(bind_graph):
            bind_graph(coordinator)
        bind_automation_graph = getattr(authority, "_register_automation_source_graph", None)
        if callable(bind_automation_graph):
            bind_automation_graph(coordinator)
    return coordinator

