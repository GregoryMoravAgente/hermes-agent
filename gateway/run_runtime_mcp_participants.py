"""Exact MCP participant custody for a post-open preservation retry."""
from __future__ import annotations

from typing import Any


def participant_tokens(
    capabilities: tuple[Any, ...], *, runner: Any = None, coordinator: Any = None,
) -> tuple[tuple[Any, Any, Any], ...]:
    """Retain each source object, close receipt, and source-issued capability together."""
    tokens = [(capability.factory, capability.receipt, capability) for capability in capabilities]
    if runner is not None:
        i08 = getattr(runner, "_i08_post_open_capability", None)
        if i08 is not None:
            from gateway.session_goal_tick_custody import i08_abort_participant
            tokens.append(i08_abort_participant(i08))
    if coordinator is not None:
        cron = getattr(getattr(coordinator, "_cron_source_registry", None), "_post_open_capability", None)
        if cron is not None:
            from gateway.runtime_cron_source_registry import cron_abort_participant
            tokens.append(cron_abort_participant(cron))
    if runner is not None:
        from tui_gateway.hosted_tui_custody import i17_cold_abort_participant
        tokens.extend(
            i17_cold_abort_participant(capability)
            for capability in getattr(runner, "_i17_post_open_capabilities", ())
        )
    return tuple(tokens)


def complete_source_participant(coordinator: Any, transition: Any, capability: Any) -> bool:
    """Complete one non-MCP source participant under the exact OPEN transition."""
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
    return True


def clear_source_participant(transition: Any, capability: Any) -> bool:
    """Clear one source participant only after the coordinator consumed its token."""
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


def _same_identity_set(expected: tuple[Any, ...], actual: tuple[Any, ...]) -> bool:
    if len(expected) != len(actual):
        return False
    return all(any(candidate is item for candidate in actual) for item in expected)


def _current_authority(runner: Any, authority: Any) -> bool:
    from gateway import run_runtime

    return any(candidate is authority for candidate in run_runtime._authorities(runner))


def _participant_is_current(runner: Any, coordinator: Any, transition: Any, entry: Any) -> bool:
    from gateway.runtime_sidecar_factory import FactoryAbortCapability
    from gateway.runtime_sidecar_remote import RemoteSourceError

    if type(entry) is not tuple or len(entry) != 3:
        return False
    factory, receipt, capability = entry
    if capability.__class__.__name__ == "I08AbortCapability":
        from gateway.session_goal_tick_custody import validate_i08_abort_participant
        return validate_i08_abort_participant(runner, coordinator, transition, entry)
    if capability.__class__.__name__ == "CronAbortCapability":
        from gateway.runtime_cron_source_registry import validate_cron_abort_participant
        return validate_cron_abort_participant(coordinator, transition, entry)
    if capability.__class__.__name__ == "I17ColdPostOpenCapability":
        from tui_gateway.hosted_tui_custody import validate_i17_cold_abort_participant
        return validate_i17_cold_abort_participant(runner, coordinator, transition, entry)
    if (
        not isinstance(capability, FactoryAbortCapability)
        or capability.factory is not factory
        or capability.receipt is not receipt
        or capability.coordinator is not coordinator
    ):
        return False
    authority = getattr(factory, "authority", None)
    if authority is None or not _current_authority(runner, authority):
        return False
    if getattr(authority, "_mcp_source_factory", None) is not factory:
        return False
    current_receipt = getattr(factory, "close_receipt", None)
    if current_receipt is not None and current_receipt is not receipt:
        return False
    pending = getattr(factory, "pending_finish_capability", None)
    if not callable(pending):
        return False
    try:
        current_capability = pending(coordinator, transition)
    except (AttributeError, OSError, RemoteSourceError, RuntimeError, TypeError, ValueError):
        return False
    if current_capability is not capability:
        return False
    current_identity = getattr(factory, "_close_identity", None)
    if not callable(current_identity):
        return False
    try:
        identity = current_identity()
    except (AttributeError, OSError, RemoteSourceError, RuntimeError, TypeError, ValueError):
        return False
    if identity != (
        capability.profile_id,
        capability.process_instance,
        capability.authority_epoch,
        capability.physical_owner_epoch,
    ):
        return False
    return True


def validate_post_open_participants(runner: Any, coordinator: Any, transition: Any) -> bool:
    """Require the original exact participant graph before consuming a post-open token."""
    from gateway import run_runtime

    participants = getattr(transition, "source_participants", None)
    if not isinstance(participants, tuple):
        return False
    factories = tuple(entry[0] for entry in participants if type(entry) is tuple and len(entry) == 3)
    if len(factories) != len(participants) or len({id(factory) for factory in factories}) != len(factories):
        return False
    if not all(_participant_is_current(runner, coordinator, transition, entry) for entry in participants):
        return False
    current = tuple(run_runtime._mcp_factory_participants(runner))
    i08 = getattr(runner, "_i08_post_open_capability", None)
    if i08 is not None:
        current += (i08.runner,)
    cron = getattr(getattr(coordinator, "_cron_source_registry", None), "_post_open_capability", None)
    if cron is not None:
        current += (cron.registry,)
    current += tuple(
        capability.authority
        for capability in getattr(runner, "_i17_post_open_capabilities", ())
    )
    return _same_identity_set(factories, current)
