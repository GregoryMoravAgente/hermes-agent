"""Real three-profile public preservation prepare/ACK proof."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import threading

import pytest


@pytest.fixture
def authenticated_started_mux(tmp_path, monkeypatch, unused_tcp_port):
    """Boot the supported three-profile route with real first-run provenance."""
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

    def call_sync(function, *args):
        async def invoke():
            return function(*args)
        return call(invoke())

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
                call_sync(publish_gateway_runtime_ready, runner)
                assert runner._running is True
                assert runner.session_runtime_descriptor["state"] == "ready"
                yield runner, dict(homes), loop, call
            finally:
                if runner is not None:
                    if started:
                        yuanbao = getattr(runner, "adapters", {}).get(Platform.YUANBAO)
                        if yuanbao is not None and getattr(
                            getattr(yuanbao, "_connection", None), "ws", None
                        ) is not None:
                            call(yuanbao.disconnect())
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


def test_public_three_profile_prepare_ack_releases_exact_native_plan(authenticated_started_mux):
    """Prepare and ACK the real READY route with all three profile lock descriptors retained."""
    from gateway import run_runtime
    from gateway.run_runtime import recover_gateway_native_sessions
    from gateway.run_bootstrap import _start_gateway_start_cron_and_housekeeping
    from gateway.run import (
        _CRON_SHUTDOWN_DRAIN_TIMEOUT,
        _HOUSEKEEPING_SHUTDOWN_DRAIN_TIMEOUT,
        _await_thread_exit,
        _stop_cron_provider,
    )
    from gateway.runtime_native_cleanup_sources import (
        ProfileNativeCleanupReceipt,
        build_native_cleanup_plan,
    )
    from gateway.runtime_custody import DrainReceipt
    from gateway.session_authorities import serving_authorities

    runner, _homes, _loop, call = authenticated_started_mux
    async def start_cron_boundary():
        return _start_gateway_start_cron_and_housekeeping(runner)

    cron_stop, cron_provider, cron_thread, housekeeping_thread = call(start_cron_boundary())
    try:
        call(recover_gateway_native_sessions(runner))
        authorities = tuple(serving_authorities(runner))
        assert len(authorities) == 3
        assert all(authority.hosted_room_service is not None for authority in authorities)

        descriptor = runner.session_runtime_descriptor
        params = {
            "instance_id": descriptor["instance_id"],
            "authority_epoch": descriptor["authority_epoch"],
            "owner_id": "public-three-profile-native-handoff",
            "scope": "process",
            "deadline_s": 30,
        }
        wire = call(run_runtime.begin_preservation_drain(runner, params, "fixture-peer"))
        assert wire["status"] == "closing", wire
        prepared = call(run_runtime.prepare_preservation_handoff(
            runner, {"receipt": wire}, "fixture-peer",
        ))
        assert prepared["status"] == "handoff_pending", prepared
        coordinator = runner.runtime_custody
        receipt = DrainReceipt(
            prepared["receipt_id"], prepared["process_instance"],
            prepared["authority_epoch"], prepared["close_generation"],
            prepared["status"],
        )
        plan = build_native_cleanup_plan(runner, coordinator, receipt)
        assert plan is not None
        profile_entries = [
            entry for entry in plan.source_participants
            if isinstance(entry[0], ProfileNativeCleanupReceipt)
        ]
        assert len(profile_entries) == len(authorities)
        handles = tuple(entry[0].handle for entry in profile_entries)
        assert len({id(handle) for handle in handles}) == len(authorities)
        assert all(isinstance(handle.fileno(), int) and handle.fileno() >= 0 for handle in handles)
        assert all(any(fence is handle for fence in plan.native_fences) for handle in handles)
        closed = call(run_runtime.acknowledge_preservation_native(
            runner, {"receipt": prepared}, "fixture-peer",
        ))
        assert closed["status"] == "closed", closed
        assert closed["cleanup"] == "complete", coordinator.status()
        assert coordinator.status()["state"] == "closed"
        assert all(getattr(handle, "closed", False) for handle in handles)
        for handle in handles:
            with pytest.raises((OSError, ValueError)):
                os.fstat(handle.fileno())
        retry = call(run_runtime.acknowledge_preservation_native(
            runner, {"receipt": closed}, "fixture-peer",
        ))
        assert retry["status"] == "closed", retry
        assert retry["cleanup"] == "complete", retry
    finally:
        cron_stop.set()
        _stop_cron_provider(cron_provider)
        assert call(_await_thread_exit(
            cron_thread, timeout=_CRON_SHUTDOWN_DRAIN_TIMEOUT,
        ))
        assert call(_await_thread_exit(
            housekeeping_thread, timeout=_HOUSEKEEPING_SHUTDOWN_DRAIN_TIMEOUT,
        ))
