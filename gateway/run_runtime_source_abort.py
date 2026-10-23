"""Source participant composition for the reversible preservation abort.

This module is intentionally only an adapter around the existing source registries and the one
``RuntimeCustodyCoordinator`` transition.  It does not introduce another phase ledger.
"""
from __future__ import annotations

from gateway.run_runtime_mcp_abort import (
    MCPFinishOutcome,
    _finish_mcp_factories_atomically,
    _retry_mcp_factories_post_open,
)


async def resume_profile_sources_before_open(runner, coordinator, receipt) -> bool:
    """Restore the exact I18 manager leases before the coordinator can reopen."""
    if getattr(runner, "_profile_lifecycle_custody", None) is None:
        return True
    from gateway.profile_global_provider import resume_global_memory_providers

    remaining = coordinator.remaining_seconds()
    if remaining is None or not await resume_global_memory_providers(
        runner, receipt.close_generation, remaining=remaining,
    ):
        return False
    from gateway.profile_lifecycle_custody import abort_profile_lifecycle_close

    if abort_profile_lifecycle_close(runner, receipt.close_generation):
        return True
    from gateway.profile_global_provider import refence_global_memory_providers

    await refence_global_memory_providers(
        runner, receipt.close_generation, remaining=coordinator.remaining_seconds(),
    )
    return False


async def refence_profile_sources(runner, coordinator, receipt) -> bool:
    """Re-fence I18 after a pre-open source failure while retaining the same generation."""
    if getattr(runner, "_profile_lifecycle_custody", None) is None:
        return True
    from gateway.profile_global_provider import refence_global_memory_providers

    remaining = coordinator.remaining_seconds()
    if remaining is None or not await refence_global_memory_providers(
        runner, receipt.close_generation, remaining=remaining,
    ):
        return False
    from gateway.profile_lifecycle_custody import begin_profile_lifecycle_close

    return begin_profile_lifecycle_close(runner, receipt.close_generation)


def prepare_source_participants(
    runner, coordinator, receipt, *, include_i08: bool = True,
) -> tuple[bool, str | None]:
    """Capture I08/Cron participants while the canonical coordinator is still CLOSING."""
    from gateway.session_goal_tick_custody import (
        has_i08_source_graph, i08_close_active, prepare_i08_abort,
    )

    if include_i08 and has_i08_source_graph(runner) and i08_close_active(
            runner, receipt.close_generation):
        if prepare_i08_abort(runner, coordinator, receipt) is None:
            return False, "i08_source_finalization_pending"
    registry = getattr(coordinator, "_cron_source_registry", None)
    if (
        registry is not None
        and getattr(registry, "_state", None) == "closing"
        and getattr(registry, "_close_generation", None) == receipt.close_generation
    ):
        from gateway.runtime_cron_source_registry import prepare_cron_abort
        if prepare_cron_abort(coordinator, receipt) is None:
            return False, "cron_source_finalization_pending"
    return True, None


def finish_sources_atomically(runner, entries, coordinator, receipt) -> MCPFinishOutcome:
    """Commit all currently prepared source participants through one coordinator token."""
    from gateway.run_runtime import _prepare_mcp_factories, _reclose_mcp_factories
    if not _prepare_mcp_factories(entries, coordinator, receipt):
        _reclose_mcp_factories(entries)
        return MCPFinishOutcome("pre_open_failure", "mcp_source_finalization_pending")
    return _finish_mcp_factories_atomically(
        entries, coordinator, receipt, runner=runner,
    )


def retry_sources_post_open(runner, coordinator, receipt) -> MCPFinishOutcome:
    """Retry only the exact coordinator-issued post-open capability."""
    return _retry_mcp_factories_post_open(runner, coordinator, receipt)


def outcome_wire(outcome: MCPFinishOutcome, receipt_wire: dict | None = None) -> dict | None:
    """Translate a typed source result without hiding an already-published OPEN transition."""
    if outcome.completed:
        return None
    if outcome.committed_open:
        wire = {"status": "open", "pending_cleanup": {"source": "preservation_sources"}}
        if receipt_wire is not None:
            wire["receipt"] = receipt_wire
        return wire
    wire = {"status": "refused", "reason": outcome.reason or "source_finalization_pending"}
    if receipt_wire is not None:
        wire["receipt"] = receipt_wire
    return wire


async def prepare_global_provider_handoff(runner, coordinator, receipt) -> dict:
    """Seal the exact provider graph and publish its typed coordinator result."""
    from gateway.profile_global_provider import prepare_global_memory_providers
    from gateway.run_runtime import _seal_integrated_preservation_owners

    await prepare_global_memory_providers(
        runner, receipt.close_generation, remaining=coordinator.remaining_seconds(),
    )
    # Provider preparation must complete before I17 terminalizes the enclosing hosted
    # service.  This preserves the source-owned provider prepare/rollback boundary while
    # still requiring the real hosted-room stop/join receipt before profile sealing.
    from gateway.run_runtime_i17_terminal import terminalize_i17_rooms_before_seal
    blocked = await terminalize_i17_rooms_before_seal(
        runner, coordinator, receipt, coordinator.remaining_seconds(),
    )
    if blocked is not None:
        return blocked
    _seal_integrated_preservation_owners(runner, coordinator, receipt)
    result = coordinator.prepare_handoff(receipt)
    return result.as_wire() if hasattr(result, "as_wire") else {
        "status": "refused", "reason": getattr(result, "code", "custody_pending"),
    }
