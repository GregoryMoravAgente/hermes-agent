"""I17 hosted-room terminal phase for preservation preparation."""

from __future__ import annotations


async def terminalize_i17_rooms_before_seal(runner, coordinator, receipt, remaining):
    """Join exact hosted-room runtimes before sealing their profile roots."""
    from gateway import run_runtime
    from tui_gateway.hosted_tui_custody import terminalize_preservation_rooms

    if remaining is not None and await terminalize_preservation_rooms(runner, remaining):
        return None
    run_runtime._seal_integrated_preservation_owners(runner, coordinator, receipt)
    blocked = coordinator.prepare_handoff(receipt)
    if hasattr(blocked, "as_wire"):
        return blocked.as_wire()
    return {
        "status": "refused",
        "reason": getattr(blocked, "code", "custody_pending"),
    }
