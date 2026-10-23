"""AGC-1067 external two-profile provider-custody behavior probe.

This test stops at the real Gateway profile/global provider-custody boundary. It
uses two multiplexed profile authorities, real MemoryManager instances, real
RetainDB queues and SQLite state, and the public AIAgent prepare/resume/commit
facade. Only the RetainDB network client is synthetic; this does not claim the
full protected/native full20 acknowledgement path.
"""

from pathlib import Path

import pytest


async def _install_real_provider(runner, authority, profile_name, client):
    from agent.memory_manager import MemoryManager
    from gateway.session import _session_key_namespace
    from gateway.session_authorities import owner_scope
    from plugins.memory import retaindb
    from run_agent import AIAgent

    manager = MemoryManager()
    provider = retaindb.RetainDBMemoryProvider()
    session_id = f"agc1067-two-profile-{profile_name}"

    with owner_scope(authority):
        manager.add_provider(provider)
        manager.initialize_all(
            session_id=session_id,
            hermes_home=str(Path(authority.db.db_path).parent),
            platform="test",
        )

    agent = object.__new__(AIAgent)
    agent._memory_manager = manager
    agent._session_db = authority.db
    agent.session_id = session_id

    namespace = _session_key_namespace(profile_name)
    cache_key = f"{namespace}:agc1067-two-profile-{profile_name}"
    runner._agent_cache[cache_key] = (agent, "agc1067-two-profile")
    # Keep the synthetic client association explicit for the per-profile write
    # assertion; the provider itself owns the real queue/store objects.
    assert provider._client is client
    return cache_key, agent, manager, provider, provider._queue


async def _restore_profile_custody(runner, generation):
    """Restore reversible custody before the authority fixture is closed."""
    from gateway.profile_global_provider import (
        custody_for,
        discard_global_memory_providers,
        resume_global_memory_providers,
    )
    from gateway.profile_lifecycle_custody import abort_profile_lifecycle_close

    custody = custody_for(runner)
    if custody._generation != generation:
        return

    entries = tuple(custody._entries.values())
    if any(entry.lease is not None or entry.task is not None for entry in entries):
        await resume_global_memory_providers(runner, generation, remaining=10.0)
    abort_profile_lifecycle_close(runner, generation)
    discard_global_memory_providers(runner, generation)


@pytest.mark.asyncio
async def test_actual_two_profile_global_provider_scope_preserves_and_commits(
    tmp_path, monkeypatch,
):
    from gateway.profile_global_provider import (
        commit_global_memory_providers,
        prepare_global_memory_providers,
        resume_global_memory_providers,
    )
    from gateway.profile_lifecycle_custody import begin_profile_lifecycle_close
    from gateway.session_authorities import owner_scope
    from plugins.memory import retaindb
    from tests.gateway.test_runtime_custody_profile_epochs import _boot, _close_boot
    from tests.plugins.test_provider_native_retirement import _RetainClient
    from tests.waiting import wait_until

    provider_clients = [_RetainClient(), _RetainClient()]
    for client in provider_clients:
        client.release_ingest.set()
        client.release_prefetch.set()
        client.release_seed.set()
    factory_clients = list(provider_clients)

    class _ProviderClientFactory:
        def __new__(cls, *_args, **_kwargs):
            if not factory_clients:
                raise AssertionError("unexpected additional RetainDB client")
            return factory_clients.pop(0)

    monkeypatch.setenv("RETAINDB_API_KEY", "synthetic-local-client")
    monkeypatch.setattr(retaindb, "_Client", _ProviderClientFactory)

    runner, entries = await _boot(tmp_path, monkeypatch)
    cache_keys = []
    installed = []
    generation = 106701
    committed_generation = generation + 1
    try:
        assert runner.config.multiplex_profiles is True
        assert len(entries) == 2
        assert len(runner.session_authorities) == 2

        named = {}
        for authority in runner.session_authorities:
            profile_name = runner.session_authorities.profile_name(authority)
            named[profile_name or "default"] = authority
        assert set(named) == {"default", "alpha"}
        assert named["default"].epoch != named["alpha"].epoch

        for profile_name in ("default", "alpha"):
            authority = named[profile_name]
            client = provider_clients[0 if profile_name == "default" else 1]
            installed_item = await _install_real_provider(
                runner, authority, profile_name, client,
            )
            cache_keys.append(installed_item[0])
            installed.append((profile_name, authority, *installed_item[1:]))

        # A multiplexed direct call without the owning profile scope must be
        # refused. The global route must supply each cached agent's own scope.
        secondary = next(item for item in installed if item[0] == "alpha")
        secondary_report = secondary[2].prepare_memory_provider()
        assert secondary_report.status == "unknown"
        assert secondary_report.reason == "owner_scope_refused"

        assert begin_profile_lifecycle_close(runner, generation) is True
        outcome = await prepare_global_memory_providers(
            runner, generation, remaining=20.0,
        )
        assert outcome.ready is True, outcome
        assert outcome.reason == "prepared"

        original = {}
        for profile_name, authority, agent, manager, provider, queue in installed:
            original[profile_name] = (agent, manager, provider, queue)
            cache_key = next(
                key for key in cache_keys
                if key.endswith(f"-{profile_name}")
            )
            assert runner._agent_cache[cache_key][0] is agent
            assert manager._retirement_preparation_lease is not None
            assert manager._shutting_down is False
            assert provider._queue is queue
            assert queue._connections

        assert await resume_global_memory_providers(
            runner, generation, remaining=20.0,
        ) is True
        for profile_name, _authority, agent, manager, provider, queue in installed:
            assert original[profile_name][0] is agent
            assert original[profile_name][1] is manager
            assert original[profile_name][2] is provider
            assert original[profile_name][3] is queue
            assert manager._retirement_preparation_lease is None
            assert provider._queue is queue
            assert queue._connections

        # Abort the reversible phase, then prove the same provider objects can
        # accept ordinary profile-scoped writes before the final commit phase.
        await _restore_profile_custody(runner, generation)
        for profile_name, authority, _agent, manager, _provider, queue in installed:
            client = provider_clients[0 if profile_name == "default" else 1]
            before_calls = client.ingest_calls
            with owner_scope(authority):
                manager.sync_all(
                    f"agc1067 preserved write {profile_name}",
                    "ack",
                    session_id=f"agc1067-two-profile-{profile_name}",
                )
                assert manager.flush_pending(timeout=2.0) is True
            wait_until(
                lambda queue=queue: queue._pending_count() == 0,
                what=f"RetainDB queue drained for {profile_name}",
            )
            assert client.ingest_calls > before_calls

        # Run the irreversible phase through the same public AIAgent methods,
        # with a fresh global generation and the unchanged cached objects.
        assert begin_profile_lifecycle_close(runner, committed_generation) is True
        committed = await prepare_global_memory_providers(
            runner, committed_generation, remaining=20.0,
        )
        assert committed.ready is True, committed
        assert await commit_global_memory_providers(
            runner, committed_generation,
        ) is True

        for _profile_name, _authority, agent, manager, provider, queue in installed:
            assert provider._queue is queue
            assert manager.providers == []
            assert not queue._connections
            assert getattr(provider, "_released", False) is True
            assert agent._memory_manager is manager
    finally:
        # If an assertion interrupts either reversible generation, restore only
        # through source-owned custody before touching the real queues.
        await _restore_profile_custody(runner, committed_generation)
        await _restore_profile_custody(runner, generation)

        for key in cache_keys:
            runner._agent_cache.pop(key, None)

        for _profile_name, authority, _agent, manager, provider, _queue in installed:
            if not getattr(provider, "_released", False):
                with owner_scope(authority):
                    report = manager.retire_hard()
                assert report.released is True

        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_actual_two_profile_loaded_provider_native_ack_closes_before_profile_locks(
    tmp_path, monkeypatch,
):
    """Use the public native ACK path with real loaded providers and exact coordinator leases.

    This deliberately stops before the full20 owner graph: the provider participants are real
    MemoryManager/RetainDB objects, while profile lock participants remain owned and are asserted
    still retained.  It proves the ordering boundary without manufacturing an I17/I08 receipt.
    """
    from agent.memory_retirement import MemoryRetirementReport
    from gateway.profile_global_provider import (
        prepare_global_memory_providers,
        custody_for,
    )
    from gateway.profile_lifecycle_custody import (
        abort_profile_lifecycle_close,
        begin_profile_lifecycle_close,
    )
    from gateway.run_runtime_native_cleanup import acknowledge_native_cleanup
    from gateway.runtime_custody import RuntimeCustodyCoordinator, NativeCleanupPlan
    from gateway.runtime_native_cleanup_sources import _provider_participants
    from gateway.runtime_ownership import process_ownership
    from hermes_cli.backend_retirement import RetirementFence
    from plugins.memory import retaindb
    from tests.gateway.test_runtime_custody_profile_epochs import _boot, _close_boot
    from tests.plugins.test_provider_native_retirement import _RetainClient

    provider_clients = [_RetainClient(), _RetainClient()]
    for client in provider_clients:
        client.release_ingest.set()
        client.release_prefetch.set()
        client.release_seed.set()
    factory_clients = list(provider_clients)

    class _ProviderClientFactory:
        def __new__(cls, *_args, **_kwargs):
            if not factory_clients:
                raise AssertionError("unexpected additional RetainDB client")
            return factory_clients.pop(0)

    monkeypatch.setenv("RETAINDB_API_KEY", "synthetic-local-client")
    monkeypatch.setattr(retaindb, "_Client", _ProviderClientFactory)

    runner, entries = await _boot(tmp_path, monkeypatch)
    cache_keys = []
    installed = []
    close_generation = None
    try:
        named = {
            runner.session_authorities.profile_name(authority) or "default": authority
            for authority in runner.session_authorities
        }
        for profile_name in ("default", "alpha"):
            installed_item = await _install_real_provider(
                runner,
                named[profile_name],
                profile_name,
                provider_clients[0 if profile_name == "default" else 1],
            )
            cache_keys.append(installed_item[0])
            installed.append((profile_name, named[profile_name], *installed_item[1:]))

        descriptor = runner.session_runtime_descriptor
        coordinator = RuntimeCustodyCoordinator(
            process_instance=descriptor["instance_id"],
            authority_epoch=descriptor["authority_epoch"],
            fence=RetirementFence(),
        )
        closing = coordinator.begin_close(owner_id="loaded-provider-native-ack", deadline_s=30)
        assert not hasattr(closing, "code"), closing
        close_generation = closing.close_generation
        for spec in coordinator.owner_manifest:
            assert coordinator.seal_owner(
                spec.owner_class,
                {
                    "source": spec.source,
                    "process_instance": closing.process_instance,
                    "authority_epoch": closing.authority_epoch,
                    "close_generation": close_generation,
                    "owner_id": f"loaded-provider-{spec.owner_class}",
                    "observed": True,
                    "cross_process_proof": True,
                    "os_lock_owner": True,
                },
            ) is None
        handoff = coordinator.prepare_handoff(closing)
        assert not hasattr(handoff, "code"), handoff
        assert coordinator.state == "handoff_pending"

        assert begin_profile_lifecycle_close(runner, close_generation) is True
        prepared = await prepare_global_memory_providers(
            runner, close_generation, remaining=20.0,
        )
        assert prepared.ready is True, prepared
        source_entries = _provider_participants(runner, coordinator, handoff)
        assert source_entries is not None and len(source_entries) == 2
        plan = NativeCleanupPlan(
            handoff.process_instance,
            handoff.authority_epoch,
            close_generation,
            source_participants=tuple(source_entries),
            native_fences=tuple(item[1].lease for item in source_entries),
        )
        runner._preservation_native_cleanup_plan = lambda _coordinator, _receipt: plan

        closed = await acknowledge_native_cleanup(runner, coordinator, handoff)
        assert closed.cleanup == "complete", closed
        assert coordinator.state == "closed"
        custody = custody_for(runner)
        assert all(entry.status == "committed" for entry in custody._entries.values())
        for _profile_name, authority, agent, manager, provider, queue in installed:
            assert manager.providers == []
            assert not queue._connections
            assert getattr(provider, "_released", False) is True
            assert agent._memory_manager is manager
            # Native provider settlement precedes profile lock release; this test intentionally
            # keeps the exact two profile roots owned for the subsequent source-owned teardown.
            assert process_ownership.owns(authority.profile_id)

        retry = await acknowledge_native_cleanup(runner, coordinator, closed.closed_receipt)
        assert retry.cleanup == "complete"
    finally:
        if close_generation is not None:
            abort_profile_lifecycle_close(runner, close_generation)
        from gateway.profile_global_provider import discard_global_memory_providers
        discard_global_memory_providers(runner, close_generation or 0)
        for key in cache_keys:
            runner._agent_cache.pop(key, None)
        for _profile_name, authority, _agent, manager, provider, _queue in installed:
            if not getattr(provider, "_released", False):
                from gateway.session_authorities import owner_scope
                with owner_scope(authority):
                    report = manager.retire_hard()
                assert isinstance(report, MemoryRetirementReport)
                assert report.released is True
        assert await _close_boot(runner, entries) is True
