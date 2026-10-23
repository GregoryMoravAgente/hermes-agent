"""Source watcher transfers conserve descriptors and the captured registry identity."""

import asyncio
from types import SimpleNamespace

import pytest


def test_real_ready_boot_preserves_registered_process_indexes(started_mux):
    from tools.process_registry import process_registry

    runner, _homes, _loop, _call = started_mux
    factory = process_registry._i15_zero_factory
    assert factory.coordinator is runner.runtime_custody
    assert len(factory.registrations()) == 3
    assert all(factory._registration_identity_unchanged(item) for item in factory.registrations())


@pytest.mark.asyncio
async def test_yielding_drains_conserve_concurrent_appends_without_double_scheduling():
    from gateway.run_process_watchers import drain_pending_process_watchers

    pending = [{"session_id": str(index)} for index in range(101)]
    appended = [{"session_id": "late-a"}, {"session_id": "late-b"}]
    registry = SimpleNamespace(pending_watchers=pending)
    runner = SimpleNamespace()
    accepted = []
    reached_yield = asyncio.Event()

    def spawn(watcher):
        accepted.append(watcher)
        if len(accepted) == 100:
            reached_yield.set()

    async def append_and_drain():
        await reached_yield.wait()
        pending.extend(appended)
        return await drain_pending_process_watchers(runner, registry, spawn)

    await asyncio.gather(
        drain_pending_process_watchers(runner, registry, spawn), append_and_drain(),
    )
    assert registry.pending_watchers is pending
    assert pending == []
    assert len(accepted) == 103
    assert len({id(watcher) for watcher in accepted}) == 103
    assert accepted[-2:] == appended


@pytest.mark.asyncio
async def test_failed_scheduling_retains_exact_unaccepted_descriptors():
    from gateway.run_process_watchers import drain_pending_process_watchers

    first, second, late = ({"session_id": name} for name in ("first", "second", "late"))
    pending = [first, second]
    registry = SimpleNamespace(pending_watchers=pending)
    error = RuntimeError("scheduling refused")

    def spawn(watcher):
        if watcher is second:
            pending.append(late)
            raise error

    with pytest.raises(RuntimeError) as observed:
        await drain_pending_process_watchers(SimpleNamespace(), registry, spawn)
    assert observed.value is error
    assert registry.pending_watchers is pending
    assert pending[0] is second and pending[1] is late


@pytest.mark.asyncio
async def test_mid_spawn_replacement_retains_unaccepted_work_and_retries_without_duplication():
    from gateway.run_process_watchers import drain_pending_process_watchers

    watcher, remaining = ({"session_id": name} for name in ("accepted", "retained"))
    pending = [watcher, remaining]
    replacement = [{"session_id": "foreign"}]
    registry = SimpleNamespace(pending_watchers=pending)
    runner = SimpleNamespace(_i10_notification_source_binding={
        "registry": registry, "pending_watchers": pending,
    })
    accepted = []

    def replace(item):
        accepted.append(item)
        registry.pending_watchers = replacement

    with pytest.raises(RuntimeError, match="source index"):
        await drain_pending_process_watchers(runner, registry, replace)
    assert pending == [remaining]
    assert registry.pending_watchers is replacement
    assert replacement[0]["session_id"] == "foreign"
    registry.pending_watchers = pending
    await drain_pending_process_watchers(runner, registry, accepted.append)
    assert accepted == [watcher, remaining]
    assert pending == []


@pytest.mark.asyncio
async def test_pre_drain_source_replacement_refuses_before_scheduling():
    from gateway.run_process_watchers import drain_pending_process_watchers

    original = [{"session_id": "original"}]
    replacement = [{"session_id": "foreign"}]
    registry = SimpleNamespace(pending_watchers=replacement)
    runner = SimpleNamespace(_i10_notification_source_binding={
        "registry": registry, "pending_watchers": original,
    })
    accepted = []
    with pytest.raises(RuntimeError, match="source index"):
        await drain_pending_process_watchers(runner, registry, accepted.append)
    assert accepted == []
    assert registry.pending_watchers is replacement
    assert original[0]["session_id"] == "original"
