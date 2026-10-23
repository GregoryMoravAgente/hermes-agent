"""Source-owned adapter reconnect readback for the I10 recovery close."""

from __future__ import annotations

from gateway.platforms.base import BasePlatformAdapter


def i10_source_snapshot(runner, authority) -> dict:
    """Read one profile's reconnect and adapter-owned inbound queues at close."""
    registry = getattr(runner, "session_authorities", None)
    if registry is None or not callable(getattr(registry, "profile_name", None)):
        raise RuntimeError("adapter registry authority binding is unavailable")
    profile_name = registry.profile_name(authority)
    if profile_name is None:
        try:
            reconnect = runner._failed_platforms
            adapters = runner.adapters
        except AttributeError as exc:
            raise RuntimeError("adapter registry is incomplete") from exc
    else:
        try:
            profile_reconnect = runner._profile_failed_platforms
            profile_adapters = runner._profile_adapters
        except AttributeError as exc:
            raise RuntimeError("adapter registry is incomplete") from exc
        if (
            not isinstance(profile_reconnect, dict)
            or not isinstance(profile_adapters, dict)
            or profile_name not in profile_reconnect
            or profile_name not in profile_adapters
        ):
            raise RuntimeError("adapter registry profile binding is incomplete")
        reconnect = profile_reconnect[profile_name]
        adapters = profile_adapters[profile_name]
    if not isinstance(reconnect, dict) or not isinstance(adapters, dict):
        raise RuntimeError("adapter registry has invalid indexes")
    if any(
            not isinstance(adapter, BasePlatformAdapter)
            or not isinstance(getattr(adapter, "_pending_messages", None), dict)
            for adapter in adapters.values()):
        raise RuntimeError("adapter registry inbound index is invalid")
    pending = {f"reconnect:{getattr(platform, 'value', platform)}" for platform in reconnect}
    pending.update(
        f"inbound:{getattr(platform, 'value', platform)}:{key}"
        for platform, adapter in adapters.items()
        for key in tuple(adapter._pending_messages.keys())
    )
    return {
        "pending_ids": tuple(sorted(str(item) for item in pending)),
        "registry": {
            "profile": profile_name or "default",
            "reconnect_platforms": tuple(sorted(
                str(getattr(platform, 'value', platform)) for platform in reconnect
            )),
            "pending_inbound": tuple(sorted(str(item) for item in pending)),
        },
    }


def register_i10_source(runner, authority):
    """Install adapter reconnect's canonical close-time probe for one authority."""
    registry = getattr(runner, "session_authorities", None)
    if registry is None or not callable(getattr(registry, "profile_name", None)):
        raise RuntimeError("adapter registry authority binding is unavailable")
    profile_name = registry.profile_name(authority)
    if profile_name is not None:
        profile_reconnect = getattr(runner, "_profile_failed_platforms", None)
        profile_adapters = getattr(runner, "_profile_adapters", None)
        if not isinstance(profile_reconnect, dict) or not isinstance(profile_adapters, dict):
            raise RuntimeError("adapter registry is incomplete")
        profile_reconnect.setdefault(profile_name, {})
        profile_adapters.setdefault(profile_name, {})
    else:
        if not isinstance(getattr(runner, "_failed_platforms", None), dict):
            raise RuntimeError("adapter registry is incomplete")
        if not isinstance(getattr(runner, "adapters", None), dict):
            raise RuntimeError("adapter registry is incomplete")
    from gateway.session_recovery_custody import register_i10_consumer_source

    return register_i10_consumer_source(
        authority, "adapter_reconnect", lambda current: i10_source_snapshot(runner, current),
    )
