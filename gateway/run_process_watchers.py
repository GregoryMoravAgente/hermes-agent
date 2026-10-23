"""Transfer pending watcher descriptors without replacing their source-owned index."""

from __future__ import annotations

import asyncio


def _check_source_index(runner, registry, pending) -> None:
    binding = getattr(runner, "_i10_notification_source_binding", None)
    if binding is not None and (
        not isinstance(binding, dict)
        or binding.get("registry") is not registry
        or binding.get("pending_watchers") is not pending
    ):
        raise RuntimeError("process watcher source index does not match its binding")
    factory = getattr(registry, "_i15_zero_factory", None)
    if factory is not None and (
        factory.registry is not registry
        or any(not factory._registration_identity_unchanged(item)
               for item in factory.registrations())
    ):
        raise RuntimeError("process watcher source index does not match its factory")
    if registry.pending_watchers is not pending:
        raise RuntimeError("process watcher source index was replaced")


async def drain_pending_process_watchers(runner, registry, spawn) -> int:
    """Keep descriptors visible until scheduling accepts each exact object.

    Concurrent appends remain in the canonical list for the next batch.  Serialize yielding
    drains so a second turn cannot schedule the remainder of an already accepted batch twice.
    Scheduling is only admission: existing watcher owners still prove native settlement.
    """
    lock = getattr(runner, "_process_watcher_drain_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        runner._process_watcher_drain_lock = lock
    async with lock:
        pending = registry.pending_watchers
        if not isinstance(pending, list):
            raise RuntimeError("process watcher source index is unavailable")
        _check_source_index(runner, registry, pending)
        batch = tuple(pending)
        for index, watcher in enumerate(batch):
            _check_source_index(runner, registry, pending)
            spawn(watcher)
            # Normal return means the existing scheduler accepted this exact descriptor.
            # Record that transfer on the original list even if a concurrent replacement
            # invalidated the source; retry must not schedule the accepted watcher twice.
            for pending_index, candidate in enumerate(pending):
                if candidate is watcher:
                    del pending[pending_index]
                    break
            else:
                raise RuntimeError("accepted process watcher descriptor is missing")
            _check_source_index(runner, registry, pending)
            if index % 100 == 99:
                await asyncio.sleep(0)
        return len(batch)
