"""Transactional MCP source finalization for the runtime preservation abort."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from gateway.runtime_custody import PostOpenTransitionCapability


@dataclass(frozen=True)
class MCPFinishOutcome:
    """Typed result of the source/coordinator abort transaction."""

    status: Literal[
        "complete", "pre_open_failure", "committed_open_cleanup_pending",
    ]
    reason: str | None = None
    transition: Any = None

    @property
    def completed(self) -> bool:
        return self.status == "complete"

    @property
    def committed_open(self) -> bool:
        return self.status == "committed_open_cleanup_pending"


def _same_receipt(left, right) -> bool:
    return all(
        getattr(left, field, None) == getattr(right, field, None)
        for field in ("receipt_id", "process_instance", "authority_epoch", "close_generation")
    ) and left is not None and right is not None


def _pending_transition(coordinator, receipt):
    lookup = getattr(coordinator, "pending_post_open_transition_for", None)
    if callable(lookup):
        return lookup(receipt)
    candidate = getattr(coordinator, "pending_post_open_transition", lambda: None)()
    if (
        isinstance(candidate, PostOpenTransitionCapability)
        and candidate.coordinator is coordinator
        and _same_receipt(candidate.receipt, receipt)
    ):
        return candidate
    return None


def _completed_transition(coordinator, receipt):
    lookup = getattr(coordinator, "completed_post_open_transition_for", None)
    if callable(lookup):
        return lookup(receipt)
    return None


def _exact_transition(coordinator, receipt, candidate):
    return (
        isinstance(candidate, PostOpenTransitionCapability)
        and candidate.coordinator is coordinator
        and _same_receipt(candidate.receipt, receipt)
        and _pending_transition(coordinator, receipt) is candidate
    )


def _committed_open_outcome(coordinator, receipt, reason):
    """Classify an OPEN coordinator only through its exact post-open witness."""
    transition = _pending_transition(coordinator, receipt)
    if transition is None:
        transition = _completed_transition(coordinator, receipt)
    if transition is not None:
        status = "complete" if _completed_transition(coordinator, receipt) is transition else "committed_open_cleanup_pending"
        return MCPFinishOutcome(status, reason, transition)
    return MCPFinishOutcome("committed_open_cleanup_pending", reason)


def _prepare_mcp_capabilities(entries, coordinator, coordinator_receipt):
    from gateway.runtime_sidecar_factory import FactoryAbortCapability, RemoteSourceError

    capabilities = []
    try:
        for factory, receipt in tuple(entries or ()):
            capability = factory.prepared_finish_capability(
                receipt, coordinator=coordinator, coordinator_receipt=coordinator_receipt,
            )
            if not isinstance(capability, FactoryAbortCapability):
                raise RemoteSourceError("mcp_source_finalization_capability_mismatch")
            capabilities.append(capability)
    except (AttributeError, OSError, RemoteSourceError, RuntimeError, TypeError, ValueError):
        return None
    return tuple(capabilities)


def _rollback_mcp_capabilities(capabilities):
    from gateway.runtime_sidecar_remote import RemoteSourceError

    for capability in reversed(capabilities):
        try:
            capability.factory.rollback_finish_abort(capability)
        except (AttributeError, OSError, RemoteSourceError, RuntimeError, TypeError, ValueError):
            pass


def _commit_mcp_capabilities(capabilities, coordinator, coordinator_receipt, *, runner=None):
    """Commit private sources and release the coordinator, retaining its exact token.

    Source callbacks are deliberately outside the coordinator mutex.  They are allowed to
    inspect the coordinator (and, in particular, report their own status from a worker thread),
    so holding the mutex across a callback can deadlock a callback that is waiting for the same
    status snapshot.  The close identity is checked both before and after the callbacks; a
    callback never gets to publish a stale source result into a newer coordinator phase.
    """
    from gateway.runtime_sidecar_remote import RemoteSourceError

    touched = []
    from gateway.run_runtime_mcp_participants import participant_tokens

    participants = participant_tokens(tuple(capabilities), runner=runner, coordinator=coordinator)
    check_identity = getattr(coordinator, "_check_close_identity", None)
    if not callable(check_identity):
        return None, MCPFinishOutcome("pre_open_failure", "mcp_source_finalization_pending")

    def bind_transition(candidate):
        if not isinstance(candidate, PostOpenTransitionCapability):
            candidate = _pending_transition(coordinator, coordinator_receipt)
        if candidate is None:
            return None
        bind = getattr(coordinator, "bind_post_open_participants", None)
        if not callable(bind):
            return None
        return bind(candidate, participants)

    lock = getattr(coordinator, "_lock", None)

    def rollback_or_report_failure():
        # Read the phase under the lock, but perform source rollback outside it for the same
        # reason as commit/finalize: rollback is source code and may call back into the runtime.
        with lock:
            still_closing = (
                getattr(coordinator, "state", None) == "closing"
                and check_identity(coordinator_receipt)
            )
        if still_closing:
            _rollback_mcp_capabilities(touched)
            with lock:
                still_closing = (
                    getattr(coordinator, "state", None) == "closing"
                    and check_identity(coordinator_receipt)
                )
            if still_closing:
                return None, MCPFinishOutcome("pre_open_failure", "mcp_source_finalization_pending")
        return None, _committed_open_outcome(
            coordinator, coordinator_receipt, "mcp_source_finalization_pending",
        )

    try:
        # Snapshot the authenticated CLOSING phase before invoking any source callback.
        with lock:
            if (
                getattr(coordinator, "state", None) != "closing"
                or not check_identity(coordinator_receipt)
            ):
                return None, MCPFinishOutcome("pre_open_failure", "mcp_source_finalization_pending")

        # These callbacks may inspect coordinator.status() from another thread.  Do not hold the
        # coordinator mutex while they publish their source-side staged state.
        for capability in capabilities:
            touched.append(capability)
            if capability.factory.commit_finish_abort(capability) is not True:
                raise RemoteSourceError("mcp_source_finalization_pending")
        for capability in capabilities:
            if capability.factory.finalize_finish_abort(capability) is not True:
                raise RemoteSourceError("mcp_source_finalization_pending")

        # Revalidate the exact close after source callbacks, then make the one coordinator phase
        # transition while the lock is held.  No source callback runs in this critical section.
        needs_rollback = False
        with lock:
            if (
                getattr(coordinator, "state", None) != "closing"
                or not check_identity(coordinator_receipt)
            ):
                needs_rollback = True
            else:
                returned_transition = coordinator.abort_before_handoff(
                    coordinator_receipt, return_transition=True,
                )
                transition = bind_transition(returned_transition)
                if (
                    isinstance(returned_transition, PostOpenTransitionCapability)
                    and _exact_transition(coordinator, coordinator_receipt, transition)
                ):
                    return transition, None
                if getattr(coordinator, "state", None) != "closing":
                    return None, _committed_open_outcome(
                        coordinator, coordinator_receipt,
                        getattr(transition, "code", None) or "mcp_source_finalization_pending",
                    )
                needs_rollback = True
        if needs_rollback:
            return rollback_or_report_failure()
        return None, MCPFinishOutcome("pre_open_failure", "mcp_source_finalization_pending")
    except (AttributeError, OSError, RemoteSourceError, RuntimeError, TypeError, ValueError):
        return rollback_or_report_failure()


def _complete_mcp_capabilities(capabilities, coordinator, coordinator_receipt, transition):
    """Complete source cleanup after OPEN, with exact-token recovery after late faults.

    Completion is source-owned work and therefore runs without the coordinator mutex.  The exact
    post-open token is revalidated before publishing completion and before reporting a pending
    result, so a concurrent retry or phase change cannot be mistaken for a successful cleanup.
    """
    from gateway.runtime_sidecar_remote import RemoteSourceError

    lock = getattr(coordinator, "_lock", None)

    def pending_outcome():
        with lock:
            if _completed_transition(coordinator, coordinator_receipt) is transition:
                return MCPFinishOutcome("complete", transition=transition)
            if not _exact_transition(coordinator, coordinator_receipt, transition):
                return MCPFinishOutcome(
                    "committed_open_cleanup_pending", "mcp_source_finalization_pending", transition,
                )
        return MCPFinishOutcome(
            "committed_open_cleanup_pending", "mcp_source_finalization_pending", transition,
        )

    with lock:
        if not _exact_transition(coordinator, coordinator_receipt, transition):
            if _completed_transition(coordinator, coordinator_receipt) is transition:
                return MCPFinishOutcome("complete", transition=transition)
            return MCPFinishOutcome(
                "committed_open_cleanup_pending", "mcp_source_finalization_pending", transition,
            )

    for capability in capabilities:
        try:
            if capability.factory.complete_finish_abort(
                capability, transition=transition,
            ) is not True:
                return pending_outcome()
        except (AttributeError, OSError, RemoteSourceError, RuntimeError, TypeError, ValueError):
            return pending_outcome()
    # Source participants are completed after OPEN and before the token is consumed.
    from gateway.run_runtime_mcp_participants import complete_source_participant
    for _source, _receipt, capability in getattr(transition, "source_participants", ()):
        if not complete_source_participant(coordinator, transition, capability):
            return pending_outcome()
    # Source callbacks above ran without the coordinator lock.  Revalidate the exact token before
    # consuming it; only the coordinator transition itself belongs in this critical section.
    with lock:
        if not _exact_transition(coordinator, coordinator_receipt, transition):
            if _completed_transition(coordinator, coordinator_receipt) is transition:
                return MCPFinishOutcome("complete", transition=transition)
            return MCPFinishOutcome(
                "committed_open_cleanup_pending", "mcp_source_finalization_pending", transition,
            )
        outcome = _complete_coordinator_transition(coordinator, coordinator_receipt, transition)
    if not outcome.completed:
        return outcome
    # Clearing source-side opaque capabilities is also source work; do not hold the coordinator
    # lock while it calls back into source registries.
    from gateway.run_runtime_mcp_participants import clear_source_participant
    for _source, _receipt, capability in getattr(transition, "source_participants", ()):
        if not clear_source_participant(transition, capability):
            reason = (
                f"{capability.__class__.__name__.replace('AbortCapability', '').lower()}"
                "_source_finalization_pending"
            )
            return MCPFinishOutcome(
                "committed_open_cleanup_pending", reason, transition,
            )
    return outcome


def _complete_coordinator_transition(coordinator, coordinator_receipt, transition):
    """Consume the coordinator token, recovering a completion that raised after its effect."""
    try:
        completed = coordinator.complete_post_open_transition(transition)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        if _completed_transition(coordinator, coordinator_receipt) is None:
            return MCPFinishOutcome(
                "committed_open_cleanup_pending",
                "mcp_source_finalization_pending", transition,
            )
        return MCPFinishOutcome("complete", transition=transition)
    if completed is None or _completed_transition(coordinator, coordinator_receipt) is transition:
        return MCPFinishOutcome("complete", transition=transition)
    return MCPFinishOutcome(
        "committed_open_cleanup_pending", "mcp_source_finalization_pending", transition,
    )


def _finish_mcp_factories_atomically(
    entries, coordinator, coordinator_receipt, *, runner=None,
) -> MCPFinishOutcome:
    """Publish prepared MCP sources through one exact CLOSING->OPEN transaction."""
    lock = getattr(coordinator, "_lock", None)
    check_identity = getattr(coordinator, "_check_close_identity", None)
    if lock is None or not callable(check_identity):
        return MCPFinishOutcome("pre_open_failure", "mcp_source_finalization_pending")
    capabilities = _prepare_mcp_capabilities(entries, coordinator, coordinator_receipt)
    if capabilities is None:
        return MCPFinishOutcome("pre_open_failure", "mcp_source_finalization_pending")
    with lock:
        if (
            getattr(coordinator, "state", None) != "closing"
            or not check_identity(coordinator_receipt)
        ):
            return MCPFinishOutcome("pre_open_failure", "mcp_source_finalization_pending")
    # Both source phases deliberately run outside the coordinator mutex.  Each phase takes its
    # own lock snapshot and revalidates the exact receipt before publishing a coordinator change.
    transition, failure = _commit_mcp_capabilities(
        capabilities, coordinator, coordinator_receipt, runner=runner,
    )
    if failure is not None:
        return failure
    return _complete_mcp_capabilities(
        capabilities, coordinator, coordinator_receipt, transition,
    )


def _complete_post_open_participant(factory, capability, coordinator, transition) -> bool:
    """Complete one retained participant without dropping an opaque native receipt."""
    kind = capability.__class__.__name__
    if kind == "I08AbortCapability":
        from gateway.session_goal_tick_custody import complete_i08_abort
        return bool(complete_i08_abort(capability, transition))
    if kind == "CronAbortCapability":
        from gateway.runtime_cron_source_registry import complete_cron_abort
        return bool(complete_cron_abort(capability, transition))
    if kind == "I17ColdPostOpenCapability":
        from tui_gateway.hosted_tui_custody import complete_i17_cold_abort
        return bool(complete_i17_cold_abort(capability, coordinator, transition))
    pending = getattr(factory, "pending_finish_capability", lambda *_: None)(
        coordinator, transition,
    )
    if pending is None:
        return getattr(factory, "close_receipt", None) is None
    return factory.complete_finish_abort(pending, transition=transition) is True


def _clear_post_open_participant(capability, transition) -> bool:
    """Clear a source capability only after the coordinator has consumed its token."""
    kind = capability.__class__.__name__
    if kind == "I08AbortCapability":
        from gateway.session_goal_tick_custody import clear_i08_abort
        return bool(clear_i08_abort(capability, transition))
    if kind == "CronAbortCapability":
        from gateway.runtime_cron_source_registry import clear_cron_abort
        return bool(clear_cron_abort(capability, transition))
    if kind == "I17ColdPostOpenCapability":
        from tui_gateway.hosted_tui_custody import clear_i17_cold_abort
        return bool(clear_i17_cold_abort(capability, transition))
    return True


def _retry_mcp_factories_post_open(runner, coordinator, receipt) -> MCPFinishOutcome:
    """Retry only the coordinator-issued post-open token, never the consumed wire receipt."""
    lock = getattr(coordinator, "_lock", None)
    if lock is None:
        return MCPFinishOutcome(
            "committed_open_cleanup_pending", "mcp_source_finalization_pending", None,
        )
    with lock:
        transition = _pending_transition(coordinator, receipt)
        if transition is None:
            completed = _completed_transition(coordinator, receipt)
            if completed is not None:
                return MCPFinishOutcome("complete", transition=completed)
            return MCPFinishOutcome("pre_open_failure", "mcp_post_open_transition_missing")

    def pending_outcome(reason="mcp_source_finalization_pending"):
        with lock:
            completed = _completed_transition(coordinator, receipt)
            if completed is transition:
                return MCPFinishOutcome("complete", transition=transition)
            if _exact_transition(coordinator, receipt, transition):
                return MCPFinishOutcome("committed_open_cleanup_pending", reason, transition)
            return MCPFinishOutcome("committed_open_cleanup_pending", reason, transition)

    # Validation and every source callback are intentionally outside the coordinator mutex.  In
    # particular, source finalizers may report coordinator.status() from a worker thread.
    from gateway.run_runtime_mcp_participants import validate_post_open_participants

    if not validate_post_open_participants(runner, coordinator, transition):
        return pending_outcome()
    cleanup_failed = False
    for factory, _receipt, _capability in transition.source_participants:
        try:
            if not _complete_post_open_participant(
                factory, _capability, coordinator, transition,
            ):
                cleanup_failed = True
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            cleanup_failed = True
    if cleanup_failed:
        return pending_outcome()

    # Revalidate the token after callbacks, then consume it in the coordinator only.  The source
    # clear callbacks below remain outside the lock as well.
    with lock:
        if not _exact_transition(coordinator, receipt, transition):
            return pending_outcome()
        outcome = _complete_coordinator_transition(coordinator, receipt, transition)
    if not outcome.completed:
        return outcome
    for _source, _receipt, _capability in transition.source_participants:
        try:
            if not _clear_post_open_participant(_capability, transition):
                return MCPFinishOutcome(
                    "committed_open_cleanup_pending",
                    f"{_capability.__class__.__name__.replace('AbortCapability', '').lower()}_source_finalization_pending",
                    transition,
                )
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return MCPFinishOutcome(
                "committed_open_cleanup_pending", "mcp_source_finalization_pending", transition,
            )
    return outcome
