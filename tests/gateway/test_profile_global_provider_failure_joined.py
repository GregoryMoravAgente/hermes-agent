"""Bounded I18 native retry proof for two loaded multiplexed profiles.

This is intentionally narrower than the full preservation graph.  It uses the real
profile ownership descriptors, real ``MemoryManager``/RetainDB provider objects and
the real coordinator native participant retry path.  The coordinator owner manifest
is only the typed close harness needed to reach the I18 native boundary; this test
does not claim I17 or full20 readiness.
"""

import pytest

from tests.gateway.test_profile_global_provider_native import (
    _install_real_provider,
    _restore_profile_custody,
)


def _seal_typed_close_harness(coordinator, closing) -> None:
    """Close the coordinator harness without creating a zero-work native receipt."""
    for spec in coordinator.owner_manifest:
        assert coordinator.seal_owner(
            spec.owner_class,
            {
                "source": spec.source,
                "process_instance": closing.process_instance,
                "authority_epoch": closing.authority_epoch,
                "close_generation": closing.close_generation,
                "owner_id": f"i18-bounded-harness-{spec.owner_class}",
                "observed": True,
                "cross_process_proof": True,
                "os_lock_owner": True,
            },
        ) is None


def _profile_cleanup_participants(runner, coordinator, handoff):
    """Capture both real profile lock descriptors before native ACK."""
    from gateway.run_runtime import reserved_profile_homes
    from gateway.runtime_native_cleanup_sources import (
        ProfileNativeCleanupParticipant,
        ProfileNativeCleanupReceipt,
    )
    from gateway.runtime_ownership import canonical_home, process_ownership

    participants = []
    captured = {}
    for profile_name, home in reserved_profile_homes(runner):
        canonical = canonical_home(home)
        evidence = process_ownership.reservation_evidence(canonical)
        assert isinstance(evidence, dict)
        with process_ownership._mutex:
            handle = process_ownership._handles.get(canonical)
        assert handle is not None and not handle.closed
        snapshot = ProfileNativeCleanupReceipt(
            coordinator=coordinator,
            owner=process_ownership,
            home=canonical,
            handle=handle,
            evidence=evidence,
            process_instance=handoff.process_instance,
            authority_epoch=handoff.authority_epoch,
            close_generation=handoff.close_generation,
            # This bounded test deliberately isolates I18.  I17 is an explicit UNKNOWN/full20
            # boundary in the receiving packet, so no I17 readiness is manufactured here.
            source_ready=None,
        )
        participant = ProfileNativeCleanupParticipant(snapshot)
        participants.append((snapshot, snapshot, participant))
        captured[profile_name or "default"] = (canonical, handle, snapshot, participant)
    assert set(captured) == {"default", "alpha"}
    return tuple(participants), captured


@pytest.mark.asyncio
async def test_two_profile_provider_failure_retains_both_locks_and_retries_same_receipt(
    tmp_path, monkeypatch,
):
    """A post-effect provider failure retains both profile locks and retries exactly once.

    The first provider commit is allowed to perform its real provider release and then raises,
    modeling an ambiguous public result after a native effect.  The coordinator must retain the
    exact provider/profile participant tuple and closed wire receipt.  The retry must observe the
    already-released provider without releasing it twice, settle the sibling, and only then close
    both original profile descriptors.
    """
    from gateway.profile_global_provider import (
        custody_for,
        prepare_global_memory_providers,
    )
    from gateway.profile_lifecycle_custody import (
        abort_profile_lifecycle_close,
        begin_profile_lifecycle_close,
    )
    from gateway.run_runtime_native_cleanup import acknowledge_native_cleanup
    from gateway.runtime_custody import NativeCleanupPlan, RuntimeCustodyCoordinator
    from gateway.runtime_native_cleanup_sources import _provider_participants
    from gateway.runtime_ownership import process_ownership
    from gateway.session_authorities import owner_scope
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
    release_calls = {}
    commit_calls = {}
    provider_commit_calls = {}
    try:
        named = {
            runner.session_authorities.profile_name(authority) or "default": authority
            for authority in runner.session_authorities
        }
        assert set(named) == {"default", "alpha"}
        assert named["default"].epoch != named["alpha"].epoch

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
        closing = coordinator.begin_close(
            owner_id="i18-two-profile-provider-failure", deadline_s=30,
        )
        assert not hasattr(closing, "code"), closing
        close_generation = closing.close_generation
        _seal_typed_close_harness(coordinator, closing)
        handoff = coordinator.prepare_handoff(closing)
        assert not hasattr(handoff, "code"), handoff

        assert begin_profile_lifecycle_close(runner, close_generation) is True
        prepared = await prepare_global_memory_providers(
            runner, close_generation, remaining=20.0,
        )
        assert prepared.ready is True, prepared
        provider_entries = _provider_participants(runner, coordinator, handoff)
        assert provider_entries is not None and len(provider_entries) == 2
        profile_entries, captured_profiles = _profile_cleanup_participants(
            runner, coordinator, handoff,
        )
        assert len(profile_entries) == 2

        # Capture every exact opaque provider lease and wrap only the public methods for
        # observability.  The underlying manager/provider and profile owner remain real.
        custody = custody_for(runner)
        original_leases = {
            profile_name: next(
                entry.lease
                for profile_name_entry, _authority, agent, _manager, _provider, _queue in installed
                if profile_name_entry == profile_name
                for entry in custody._entries.values()
                if entry.agent is agent
            )
            for profile_name, _authority, _agent, _manager, _provider, _queue in installed
        }
        target_profile = "default"
        target_agent = next(item[2] for item in installed if item[0] == target_profile)
        target_provider = next(item[4] for item in installed if item[0] == target_profile)
        for profile_name, _authority, agent, _manager, provider, _queue in installed:
            commit_calls[profile_name] = 0
            provider_commit_calls[profile_name] = 0

            real_agent_commit = agent.commit_memory_provider

            def _tracked_agent_commit(
                lease, *, _profile=profile_name, _real=real_agent_commit,
            ):
                commit_calls[_profile] += 1
                report = _real(lease)
                if _profile == target_profile and commit_calls[_profile] == 1:
                    # The provider has actually closed its queue/client, but the public result is
                    # lost after the side effect.  This is the ambiguity that must retain the
                    # coordinator's exact participant and make the next retry idempotent.
                    raise RuntimeError("bounded post-effect provider result lost")
                return report

            def _tracked_provider_commit(
                lease, *, _profile=profile_name, _real=provider.commit_retirement,
            ):
                provider_commit_calls[_profile] += 1
                return _real(lease)

            monkeypatch.setattr(agent, "commit_memory_provider", _tracked_agent_commit)
            monkeypatch.setattr(provider, "commit_retirement", _tracked_provider_commit)

        profile_release_real = process_ownership.release_outcome

        def _tracked_profile_release(home):
            key = str(home)
            release_calls[key] = release_calls.get(key, 0) + 1
            return profile_release_real(home)

        monkeypatch.setattr(process_ownership, "release_outcome", _tracked_profile_release)

        plan = NativeCleanupPlan(
            handoff.process_instance,
            handoff.authority_epoch,
            close_generation,
            source_participants=tuple(provider_entries) + tuple(profile_entries),
            native_fences=tuple(
                item[1].lease for item in provider_entries
            ) + tuple(item[0].handle for item in profile_entries),
        )
        monkeypatch.setattr(
            runner,
            "_preservation_native_cleanup_plan",
            lambda _coordinator, _receipt: plan,
            raising=False,
        )

        first = await acknowledge_native_cleanup(runner, coordinator, handoff)
        assert first.cleanup == "unknown", first
        assert first.reason == "native_cleanup_unknown", first
        assert coordinator.state == "closed"
        assert coordinator.status()["native_cleanup_pending_participants"] == 4

        target_entry = next(
            entry for entry in custody._entries.values() if entry.agent is target_agent
        )
        assert target_entry.status == "commit_unknown"
        assert target_entry.lease is original_leases[target_profile]
        assert commit_calls == {"default": 1, "alpha": 0}
        assert provider_commit_calls == {"default": 1, "alpha": 0}
        assert getattr(target_provider, "_released", False) is True

        # The provider failed before the participant loop could reach either profile. Both exact
        # descriptors and kernel locks therefore remain owned, despite the target provider's real
        # post-effect release having already occurred.
        assert all(not handle.closed for _home, handle, _snapshot, _participant in captured_profiles.values())
        assert all(
            process_ownership.owns(home)
            for home, _handle, _snapshot, _participant in captured_profiles.values()
        )
        assert all(count == 0 for count in release_calls.values())
        assert next(item[5] for item in installed if item[0] == "alpha")._connections

        retry = await acknowledge_native_cleanup(runner, coordinator, first.closed_receipt)
        assert retry.cleanup == "complete", retry
        assert coordinator.status()["native_cleanup_pending_participants"] == 0
        assert commit_calls == {"default": 2, "alpha": 1}
        assert provider_commit_calls == {"default": 1, "alpha": 1}
        assert all(
            custody_entry.status == "committed"
            for custody_entry in custody._entries.values()
        )
        assert all(getattr(provider, "_released", False) is True for *_, provider, _ in installed)
        assert all(not queue._connections for *_, queue in installed)
        assert all(not process_ownership.owns(home) for home, *_ in captured_profiles.values())
        assert all(handle.closed for _home, handle, _snapshot, _participant in captured_profiles.values())
        assert all(count == 1 for count in release_calls.values())

        # Replaying the exact closed receipt is a no-op. It must not invoke either public provider
        # commit or profile release a second time.
        replay = await acknowledge_native_cleanup(runner, coordinator, retry.closed_receipt)
        assert replay.cleanup == "complete", replay
        assert commit_calls == {"default": 2, "alpha": 1}
        assert provider_commit_calls == {"default": 1, "alpha": 1}
        assert all(count == 1 for count in release_calls.values())
        assert all(
            custody_entry.lease is original_leases[profile_name]
            for profile_name, _authority, agent, _manager, _provider, _queue in installed
            for custody_entry in custody._entries.values()
            if custody_entry.agent is agent
        )
    finally:
        # A normal green path has already released all provider/profile custody. On an assertion
        # failure, restore source-owned reversible leases before the fixture closes the real homes.
        if close_generation is not None:
            await _restore_profile_custody(runner, close_generation)
            abort_profile_lifecycle_close(runner, close_generation)
        for key in cache_keys:
            runner._agent_cache.pop(key, None)
        for _profile_name, authority, _agent, manager, provider, _queue in installed:
            if not getattr(provider, "_released", False):
                with owner_scope(authority):
                    report = manager.retire_hard()
                assert report.released is True
        assert await _close_boot(runner, entries) is True
