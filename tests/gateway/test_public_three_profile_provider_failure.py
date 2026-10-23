"""Public three-profile provider failure/retry proof for AGC-1721.

This is an independent regression test for the real public preservation route.  The temporary
``authenticated_started_mux`` fixture is copied from C's joined public-handoff fixture until Root
composes that fixture into the candidate; it is intentionally test-only and does not alter C's
worktree.  The test does not build a coordinator, seal an owner manifest, or construct a native
cleanup plan: begin, prepare, native ACK, and same-receipt retry all use the public runtime APIs.

The provider client is synthetic, but MemoryManager, RetainDB, SQLite queues, profile lock
descriptors, coordinator, native participants, and profile authorities are real.  I17/full20
qualification remains outside this bounded provider-failure proof.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import threading

import pytest


@pytest.fixture
def authenticated_started_mux(tmp_path, monkeypatch, unused_tcp_port):
    """Temporary local copy of the joined public three-profile authenticated fixture."""
    from gateway.control_socket import GatewayControlServer
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.run import GatewayRunner
    from gateway.run_runtime import (
        drain_gateway_runtime,
        initialize_gateway_runtime,
        publish_gateway_runtime_ready,
        start_gateway_runtime_api,
    )
    from gateway.runtime_ownership import process_ownership
    from hermes_cli.config_home import initialize_home
    from hermes_cli.profiles import create_profile
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from hermes_state import _IMPORT_DEFAULT_DB_PATH
    import hermes_state
    from tests.fakes.fake_llm_provider import FakeLLMServer, write_hermes_home
    from tests.gateway.conftest import _runtime_owned_identify
    from tui_gateway.hosted_tui_custody import close_profile

    root = (tmp_path / ".hermes").resolve()
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(root))
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", _IMPORT_DEFAULT_DB_PATH)
    initialize_home(root, ("cron", "sessions", "logs", "memories"), set())
    homes = [("default", root)]
    for name in ("alpha", "beta"):
        home = create_profile(name, no_alias=True, no_skills=True)
        assert home == root / "profiles" / name
        homes.append((name, home))
    for _name, home in homes:
        config = {
            "model": {"default": "fixture-model"},
            "platform_toolsets": {"cli": []},
            "hosted_rooms": {"profiles": {n: str(h) for n, h in homes if h != home}},
        }
        (home / "config.yaml").write_text(json.dumps(config))

    api_key = "started-mux-api-key-0123456789"
    config = GatewayConfig(
        platforms={
            Platform.API_SERVER: PlatformConfig(
                enabled=True,
                extra={"key": api_key, "port": unused_tcp_port},
            ),
        },
        sessions_dir=root / "sessions",
    )
    config.multiplex_profiles = True
    config._runtime_profile_homes = tuple(homes)
    loop = asyncio.new_event_loop()
    thread = threading.Thread(target=loop.run_forever, name="started-mux-loop")
    thread.start()
    runner = None
    control_server = None
    home_token = set_hermes_home_override(root)
    reserved = False
    started = False

    def call(coroutine):
        return asyncio.run_coroutine_threadsafe(coroutine, loop).result(30)

    try:
        with FakeLLMServer(default_text="started-mux-fixture-ok") as provider:
            try:
                write_hermes_home(root, provider.base_url)
                process_ownership.reserve([home for _name, home in homes])
                reserved = True
                runner = GatewayRunner(config)
                call(initialize_gateway_runtime(runner))
                control_server = runner.session_control_server = GatewayControlServer(
                    root, verb_handlers={"identify": lambda: _runtime_owned_identify(runner)},
                )
                control_server.ticket_store = runner.session_ticket_store
                call(control_server.start())
                call(start_gateway_runtime_api(runner))
                assert call(runner.start()) is True
                started = True
                def call_sync(function, *args):
                    async def invoke():
                        return function(*args)
                    return call(invoke())
                call_sync(publish_gateway_runtime_ready, runner)
                assert runner._running is True
                assert runner.session_runtime_descriptor["state"] == "ready"
                yield runner, dict(homes), loop, call
            finally:
                if runner is not None:
                    if started:
                        call(runner.stop())
                    elif getattr(runner, "session_runtime_descriptor", None) is not None:
                        call(drain_gateway_runtime(runner))
                    if control_server is not None:
                        call(control_server.stop())
                    for authority in tuple(getattr(runner, "session_authorities", []) or ()):
                        assert close_profile(authority)
                        authority.db.close()
                    runner.session_store.close_all_db_handles()
                    runner.close_all_session_db_handles()
    finally:
        if reserved:
            process_ownership.close()
        loop.call_soon_threadsafe(loop.stop)
        thread.join(30)
        assert not thread.is_alive()
        loop.close()
        reset_hermes_home_override(home_token)


def test_public_three_profile_provider_post_effect_failure_retains_all_profile_locks(
    authenticated_started_mux, monkeypatch,
):
    """A real post-effect provider failure retains all profile handles for same-receipt retry."""
    from gateway import run_runtime
    from gateway import runtime_native_cleanup_sources
    from gateway.profile_global_provider import custody_for
    from gateway.run_bootstrap import _start_gateway_start_cron_and_housekeeping
    from gateway.run_runtime import recover_gateway_native_sessions, reserved_profile_homes
    from gateway.runtime_ownership import canonical_home, process_ownership
    from gateway.session_authorities import owner_scope
    from gateway.run import (
        _CRON_SHUTDOWN_DRAIN_TIMEOUT,
        _HOUSEKEEPING_SHUTDOWN_DRAIN_TIMEOUT,
        _await_thread_exit,
        _stop_cron_provider,
    )
    from plugins.memory import retaindb
    from tests.gateway.test_profile_global_provider_native import (
        _install_real_provider,
        _restore_profile_custody,
    )
    from tests.plugins.test_provider_native_retirement import _RetainClient

    clients = [_RetainClient() for _ in range(3)]
    for client in clients:
        client.release_ingest.set()
        client.release_prefetch.set()
        client.release_seed.set()
    factory_clients = list(clients)

    class _ProviderClientFactory:
        def __new__(cls, *_args, **_kwargs):
            if not factory_clients:
                raise AssertionError("unexpected additional RetainDB client")
            return factory_clients.pop(0)

    monkeypatch.setenv("RETAINDB_API_KEY", "synthetic-local-client")
    monkeypatch.setattr(retaindb, "_Client", _ProviderClientFactory)

    runner, _homes, _loop, call = authenticated_started_mux
    cron_stop = cron_provider = cron_thread = housekeeping_thread = None
    cache_keys = []
    installed = []
    generation = None
    release_calls = {}
    agent_calls = {}
    provider_calls = {}
    try:
        async def start_cron_boundary():
            return _start_gateway_start_cron_and_housekeeping(runner)

        cron_stop, cron_provider, cron_thread, housekeeping_thread = call(
            start_cron_boundary(),
        )
        call(recover_gateway_native_sessions(runner))
        authorities = tuple(runner.session_authorities)
        assert len(authorities) == 3
        names = {
            authority: runner.session_authorities.profile_name(authority) or "default"
            for authority in authorities
        }
        clients_by_profile = {
            "default": clients[0],
            "alpha": clients[1],
            "beta": clients[2],
        }
        authorities_by_name = {
            profile_name: authority for authority, profile_name in names.items()
        }
        for profile_name in ("default", "alpha", "beta"):
            authority = authorities_by_name[profile_name]
            installed_item = call(_install_real_provider(
                runner, authority, profile_name, clients_by_profile[profile_name],
            ))
            cache_keys.append(installed_item[0])
            installed.append((profile_name, authority, *installed_item[1:]))

        descriptor = runner.session_runtime_descriptor
        wire = call(run_runtime.begin_preservation_drain(
            runner,
            {
                "instance_id": descriptor["instance_id"],
                "authority_epoch": descriptor["authority_epoch"],
                "owner_id": "public-three-profile-provider-failure",
                "scope": "process",
                "deadline_s": 30,
            },
            "fixture-peer",
        ))
        assert wire["status"] == "closing", wire
        prepared = call(run_runtime.prepare_preservation_handoff(
            runner, {"receipt": wire}, "fixture-peer",
        ))
        assert prepared["status"] == "handoff_pending", prepared
        generation = prepared["close_generation"]

        homes = tuple(reserved_profile_homes(runner))
        assert len(homes) == 3
        profile_handles = {}
        profile_evidence = {}
        for profile_name, home in homes:
            canonical = canonical_home(home)
            handle = process_ownership._handles.get(canonical)
            evidence = process_ownership.reservation_evidence(canonical)
            assert handle is not None and evidence is not None
            profile_handles[profile_name] = handle
            profile_evidence[profile_name] = evidence

        # A served-home/participant divergence must refuse before ACK rather than omit the live
        # descriptor from the native plan.  Cover both an omitted home on the first capture and a
        # participant disappearing between the capture and the plan bind.
        real_bindings = runtime_native_cleanup_sources._reserved_profile_bindings

        def always_omit_one_participant(current_runner):
            return real_bindings(current_runner)[:-1]

        monkeypatch.setattr(
            runtime_native_cleanup_sources,
            "_reserved_profile_bindings",
            always_omit_one_participant,
        )
        initial_missing = call(run_runtime.acknowledge_preservation_native(
            runner, {"receipt": prepared}, "fixture-peer",
        ))
        assert initial_missing == {
            "status": "refused",
            "reason": "native_cleanup_missing",
        }
        assert runner.runtime_custody.state == "handoff_pending"
        for profile_name, home in homes:
            canonical = canonical_home(home)
            assert process_ownership._handles.get(canonical) is profile_handles[profile_name]
            assert process_ownership.owns(canonical)

        monkeypatch.setattr(
            runtime_native_cleanup_sources,
            "_reserved_profile_bindings",
            real_bindings,
        )
        binding_calls = [0]

        def omit_one_participant(current_runner):
            binding_calls[0] += 1
            bindings = real_bindings(current_runner)
            return bindings if binding_calls[0] == 1 else bindings[:-1]

        monkeypatch.setattr(
            runtime_native_cleanup_sources,
            "_reserved_profile_bindings",
            omit_one_participant,
        )
        missing_participant = call(run_runtime.acknowledge_preservation_native(
            runner, {"receipt": prepared}, "fixture-peer",
        ))
        assert missing_participant == {
            "status": "refused",
            "reason": "native_cleanup_missing",
        }
        assert runner.runtime_custody.state == "handoff_pending"
        for profile_name, home in homes:
            canonical = canonical_home(home)
            assert process_ownership._handles.get(canonical) is profile_handles[profile_name]
            assert process_ownership.owns(canonical)
        monkeypatch.setattr(
            runtime_native_cleanup_sources,
            "_reserved_profile_bindings",
            real_bindings,
        )

        custody = custody_for(runner)
        entries = {
            profile_name: next(
                entry for entry in custody._entries.values() if entry.agent is agent
            )
            for profile_name, _authority, agent, _manager, _provider, _queue in installed
        }
        assert len(entries) == 3
        target_profile = next(iter(entries))
        original_leases = {name: entry.lease for name, entry in entries.items()}

        original_release = process_ownership.release_outcome
        for profile_name in profile_handles:
            release_calls[profile_name] = 0

        def tracked_release(home):
            canonical = canonical_home(home)
            profile_name = next(
                name for name, candidate in profile_handles.items()
                if canonical == canonical_home(dict(homes)[name])
            )
            release_calls[profile_name] += 1
            return original_release(home)

        monkeypatch.setattr(process_ownership, "release_outcome", tracked_release)

        for profile_name, _authority, agent, _manager, provider, _queue in installed:
            agent_calls[profile_name] = 0
            provider_calls[profile_name] = 0
            real_agent_commit = agent.commit_memory_provider
            real_provider_commit = provider.commit_retirement

            def tracked_provider_commit(lease, *, _profile=profile_name, _real=real_provider_commit):
                provider_calls[_profile] += 1
                return _real(lease)

            def tracked_agent_commit(
                lease, *, _profile=profile_name, _real=real_agent_commit,
            ):
                agent_calls[_profile] += 1
                report = _real(lease)
                if _profile == target_profile and agent_calls[_profile] == 1:
                    # The real manager/provider has already released; only the public result is
                    # lost.  Native cleanup must retain the exact participant for retry.
                    raise RuntimeError("bounded post-effect provider result lost")
                return report

            monkeypatch.setattr(provider, "commit_retirement", tracked_provider_commit)
            monkeypatch.setattr(agent, "commit_memory_provider", tracked_agent_commit)

        first = call(run_runtime.acknowledge_preservation_native(
            runner, {"receipt": prepared}, "fixture-peer",
        ))
        assert first["status"] == "closed", first
        assert first["cleanup"] == "unknown", first
        assert entries[target_profile].status == "commit_unknown"
        assert entries[target_profile].lease is original_leases[target_profile]

        # The first provider has had its real side effect, but no profile participant may run
        # while any provider participant is UNKNOWN.  All three original descriptors remain held.
        for profile_name, home in homes:
            canonical = canonical_home(home)
            assert process_ownership._handles.get(canonical) is profile_handles[profile_name]
            assert process_ownership.owns(canonical)
            assert process_ownership.reservation_evidence(canonical) == profile_evidence[profile_name]
            assert release_calls[profile_name] == 0

        for profile_name, _authority, _agent, manager, provider, queue in installed:
            if profile_name == target_profile:
                assert provider._released is True
                assert manager.providers == []
                assert not queue._connections
            else:
                if agent_calls[profile_name] == 0:
                    assert provider._released is False
                    assert queue._connections
                else:
                    assert provider._released is True
                    assert not queue._connections

        retry = call(run_runtime.acknowledge_preservation_native(
            runner, {"receipt": first}, "fixture-peer",
        ))
        assert retry["status"] == "closed", retry
        assert retry["cleanup"] == "complete", retry
        assert all(entry.status == "committed" for entry in custody._entries.values())
        assert all(entry.lease is original_leases[name] for name, entry in entries.items())
        assert agent_calls[target_profile] == 2
        assert all(agent_calls[name] == 1 for name in agent_calls if name != target_profile)
        assert all(provider_calls[name] == 1 for name in provider_calls)
        assert all(release_calls[name] == 1 for name in release_calls)

        for profile_name, _authority, _agent, manager, provider, queue in installed:
            assert manager.providers == []
            assert provider._released is True
            assert not queue._connections

        for profile_name, home in homes:
            canonical = canonical_home(home)
            assert not process_ownership.owns(canonical)
            assert canonical not in process_ownership._handles
            assert profile_handles[profile_name].closed is True

    finally:
        if generation is not None:
            _restore_profile_custody_result = call(_restore_profile_custody(runner, generation))
            assert _restore_profile_custody_result is None
        for key in cache_keys:
            runner._agent_cache.pop(key, None)
        for _profile_name, authority, _agent, manager, provider, _queue in installed:
            if not getattr(provider, "_released", False):
                with owner_scope(authority):
                    report = manager.retire_hard()
                assert report.released is True
        if cron_stop is not None:
            cron_stop.set()
            _stop_cron_provider(cron_provider)
            assert call(_await_thread_exit(
                cron_thread, timeout=_CRON_SHUTDOWN_DRAIN_TIMEOUT,
            ))
            assert call(_await_thread_exit(
                housekeeping_thread, timeout=_HOUSEKEEPING_SHUTDOWN_DRAIN_TIMEOUT,
            ))
