"""Admitted heartbeat work belongs to a conversation, not a reusable route."""
import asyncio
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from evals.heartbeat_idle_wire import WireAdapter
from gateway.config import GatewayConfig, Platform, PlatformConfig
from gateway.platforms.event import MessageEvent, MessageType
from gateway.run import GatewayRunner
from gateway.session import SessionSource, SessionStore
from hermes_cli.heartbeat import HeartbeatState, migrate_heartbeat_to_session, save_heartbeat


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["reset", "suspend", "compression", "prepare-reset", "prepare-suspend", "unchanged"])
async def test_admitted_heartbeat_executes_only_in_own_conversation(tmp_path, boundary):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    runner._running_agents = {}
    runner._run_in_executor_with_context = asyncio.to_thread
    runner.session_store = SessionStore(tmp_path / "sessions", runner.config)
    adapter = WireAdapter(PlatformConfig(enabled=True, typing_indicator=False), Platform.TELEGRAM)
    adapter.wire = []
    runner._delivery_adapter_for = lambda source: adapter
    runner._is_telegram_topic_lane = lambda source: False
    runner._cache_session_source = lambda *args: None
    runner._clear_session_env = lambda tokens: None
    source = SessionSource(platform=Platform.TELEGRAM, chat_id="boundary", user_id="owner")
    entry = runner.session_store.get_or_create_session(source)
    key, old = entry.session_key, entry.session_id
    await runner._warm_goals_session_db("test")
    save_heartbeat(old, HeartbeatState(prompt="old work", interval_seconds=60, created_at=1))
    executions = []

    def change():
        if "reset" in boundary:
            runner.session_store.reset_session(key)
        elif "suspend" in boundary:
            runner.session_store.suspend_session(key)
        elif boundary == "compression":
            child = old + "-child"
            runner.session_store._db_for_key(key).publish_compression_child(
                parent_session_id=old, child_session_id=child, source="telegram",
                require_compression_lease=False, model="offline", model_config={},
                system_prompt="offline", messages=[{"role": "user", "content": "retained"}],
            )
            assert migrate_heartbeat_to_session(old, child)
            assert runner.session_store.advance_compression_session(key, old, child)

    async def prepare(*args):
        if boundary.startswith("prepare-"):
            change()
        return runner._PreparedTurn([], "", "old work", "old work", None, None), {}

    async def model(**kwargs):
        executions.append(kwargs["session_id"])
        raise asyncio.CancelledError  # stop before unrelated post-turn delivery

    runner._hmwa_prepare_turn = prepare
    runner._run_agent = model
    runner.hooks = SimpleNamespace(emit=AsyncMock())
    adapter.set_message_handler(lambda event: runner._handle_message_with_agent(event, source, key, 1))
    try:
        await runner._heartbeat_poll_once({key: (source, old)})
        assert key in adapter._session_tasks  # prove admission before changing ownership
        if not boundary.startswith("prepare-"):
            change()
        await asyncio.gather(*adapter._background_tasks, return_exceptions=True)
        await asyncio.sleep(0)
        expected = [old + "-child"] if boundary == "compression" else [old] if boundary == "unchanged" else []
        assert executions == expected
        if boundary == "suspend":
            assert runner.session_store.peek_session_id(key) != old  # normal reset policy still runs
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_restart_does_not_restore_suspended_heartbeat(tmp_path):
    from gateway.run_heartbeat_restore import restore_heartbeat_watches

    config = GatewayConfig()
    store = SessionStore(tmp_path / "sessions", config)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id="stopped", user_id="owner")
    entry = store.get_or_create_session(source)
    save_heartbeat(entry.session_id, HeartbeatState(prompt="stale work", interval_seconds=60, created_at=1))
    store.suspend_session(entry.session_key)
    store.close_all_db_handles()
    runner = GatewayRunner.__new__(GatewayRunner)
    runner.config = config
    runner.session_store = SessionStore(tmp_path / "sessions", config)
    runner._heartbeat_watch = {}
    runner._start_heartbeat_poller = lambda: None
    runner._run_in_executor_with_context = asyncio.to_thread
    try:
        await restore_heartbeat_watches(runner)
        assert runner._heartbeat_watch == {}
        restored = runner.session_store.lookup_by_session_key(entry.session_key)
        assert restored.suspended and restored.session_id == entry.session_id
    finally:
        runner.session_store.close_all_db_handles()


async def _real_goal_owner(tmp_path, monkeypatch, *, enable_drain=False, seed_native=True):
    """A real GatewayRunner/SessionAuthority/SQLite producer fixture for loop fallback controls."""
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session_authority import SessionAuthority, initialize_session_authority
    from gateway.session_ingress_context import native_callback
    from gateway.session_automation_custody import automation_source_identity
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_constants import get_hermes_home
    from plugins.platforms.discord.adapter import DiscordAdapter

    schedule = SessionAuthority._schedule
    monkeypatch.setattr(SessionAuthority, "_schedule", lambda self, ref: None)
    monkeypatch.setenv("DISCORD_ALLOWED_USERS", "42")
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    adapter = DiscordAdapter(
        PlatformConfig(enabled=True, token="fixture-token", typing_indicator=False),
    )
    runner.adapters = {Platform.DISCORD: adapter}
    authority = await initialize_session_authority(
        runner, profile_id="default", instance_id="goal-fallback-owner",
    )
    runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id,
        authority_epoch=authority.epoch,
        fence=RetirementFence(),
    )
    runner._wire_adapter_handlers(adapter)
    source = adapter.build_source(chat_id="42", chat_type="dm", user_id="42")
    if seed_native:
        human = MessageEvent(text="seed", source=source, message_id="human-seed")
        with native_callback(runner, human, get_hermes_home()):
            await authority.admit_native(human)
    else:
        authority._reserve_route(source)
    if enable_drain:
        monkeypatch.setattr(SessionAuthority, "_schedule", schedule)
    key = runner.session_store._generate_session_key(source)
    entry = runner.session_store.lookup_by_session_key(key)
    assert entry is not None
    request_id = "goal-fallback-real-request"
    event = MessageEvent(
        text="check the current state",
        message_type=MessageType.TEXT,
        source=source,
        message_id=request_id,
        internal=True,
        metadata={"gateway_session_key": key, "gateway_session_id": entry.session_id},
    )
    handle = authority._preservation_automation_root(
        request_id=request_id,
        principal_id="automation:" + key,
        session_id=authority.logical_owner(entry.session_id),
        source_identity=automation_source_identity(event, key),
        owner_class="I08_goals_loops_heartbeat",
    )
    from gateway.session_ingress import PreservedAutomationHandoff
    handoff = PreservedAutomationHandoff(
        authority=authority,
        handle=handle,
        owner_class="I08_goals_loops_heartbeat",
        request_id=request_id,
        principal_id="automation:" + key,
        session_id=authority.logical_owner(entry.session_id),
        source_identity=automation_source_identity(event, key),
    )
    return runner, adapter, authority, key, entry, event, handoff


@pytest.mark.asyncio
async def test_unbound_goal_tick_delivery_callback_releases_real_reservation(tmp_path, monkeypatch):
    """A no-child goal decision settles its source-owned tick at the real adapter boundary."""
    from gateway.platforms.base_delivery_callbacks import finish_post_delivery_callback
    from gateway.session_goal_tick_custody import (
        attach_tick_handle, reserve_tick, view_for_handle,
    )
    from gateway.session_goal_tick_runtime import register_tick_delivery_finalizer
    from gateway.session_automation_custody import automation_source_identity

    runner, adapter, authority, key, entry, event, _handoff = await _real_goal_owner(
        tmp_path, monkeypatch,
    )
    try:
        tick = reserve_tick(
            authority,
            request_id="goal-no-child-delivery",
            principal_id="automation:" + key,
            session_id=authority.logical_owner(entry.session_id),
            source_identity=automation_source_identity(event, key),
            kind="goal",
        )
        assert tick is not None
        attach_tick_handle(event, tick, "goal")
        assert register_tick_delivery_finalizer(runner, event.source, event) is True
        assert key in adapter._post_delivery_callbacks

        channel = SimpleNamespace(
            id=42,
            send=AsyncMock(return_value=SimpleNamespace(id="goal-delivery")),
        )
        adapter._client = SimpleNamespace(
            get_channel=lambda _channel_id: channel,
            fetch_channel=AsyncMock(return_value=channel),
        )
        result = await adapter.send(
            chat_id=event.source.chat_id,
            content="heartbeat delivery",
            metadata={},
        )
        assert result.success is True
        assert result.message_id == "goal-delivery"
        await finish_post_delivery_callback(
            adapter,
            event,
            key,
            asyncio.Event(),
            processing_ok=result.success,
            classic_receipt=None,
            thread_metadata=None,
        )

        assert view_for_handle(authority, tick.handle) is None
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_goal_tick_unknown_does_not_release_sibling_or_accept_stale_handle(tmp_path, monkeypatch):
    """UNKNOWN custody stays attached to its exact tick while a sibling can settle independently."""
    from gateway.session_automation_custody import automation_source_identity
    from gateway.session_goal_tick_custody import (
        finish_unbound, mark_unknown, reserve_tick, view_for_handle,
    )

    runner, _adapter, authority, key, entry, event, _handoff = await _real_goal_owner(
        tmp_path, monkeypatch,
    )
    try:
        source_identity = automation_source_identity(event, key)
        first = reserve_tick(
            authority, request_id="goal-unknown-first", principal_id="automation:" + key,
            session_id=authority.logical_owner(entry.session_id), source_identity=source_identity,
            kind="goal",
        )
        sibling = reserve_tick(
            authority, request_id="goal-sibling", principal_id="automation:" + key,
            session_id=authority.logical_owner(entry.session_id), source_identity=source_identity,
            kind="heartbeat",
        )
        assert first is not None and sibling is not None

        mark_unknown(first, "join_failed")
        assert finish_unbound(first, success=True) is False
        assert view_for_handle(authority, first.handle) is not None

        assert finish_unbound(sibling, success=True) is True
        assert view_for_handle(authority, sibling.handle) is None
        assert view_for_handle(authority, first.handle) is not None

        stale = replace(first.handle, owner_id="goal-stale-copy")
        refusal = authority._preservation_coordinator().release_unaccepted(stale)
        assert refusal is not None and refusal.code == "stale_lease"
        assert view_for_handle(authority, first.handle) is not None
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_goal_tick_close_generation_refuses_late_registration_and_callback(
    tmp_path, monkeypatch,
):
    """Close seals intake: late watchers/ticks and delivery callbacks stay UNKNOWN."""
    from hermes_state_runtime import RuntimeStoreError
    from gateway.session_automation_custody import automation_source_identity
    from gateway.session_goal_tick_custody import (
        attach_tick_handle, owner_ready, register_watcher, reserve_tick, view_for_handle,
    )
    from gateway.session_goal_tick_runtime import register_tick_delivery_finalizer

    runner, adapter, authority, key, entry, event, _handoff = await _real_goal_owner(
        tmp_path, monkeypatch,
    )
    try:
        source_identity = automation_source_identity(event, key)
        tick = reserve_tick(
            authority, request_id="goal-close-race", principal_id="automation:" + key,
            session_id=authority.logical_owner(entry.session_id), source_identity=source_identity,
            kind="goal",
        )
        assert tick is not None
        close = authority._preservation_coordinator().begin_close(
            owner_id="goal-close-owner", scope="process", deadline_s=30,
        )
        assert close is not None and close.close_generation >= 1

        assert register_watcher(
            authority, "late-goal-watcher", "goal", session_id=entry.session_id,
        ) is None
        with pytest.raises(RuntimeStoreError, match="runtime_draining"):
            reserve_tick(
                authority, request_id="goal-too-late", principal_id="automation:" + key,
                session_id=authority.logical_owner(entry.session_id), source_identity=source_identity,
                kind="goal",
            )

        attach_tick_handle(event, tick, "goal")
        assert register_tick_delivery_finalizer(runner, event.source, event) is False
        assert view_for_handle(authority, tick.handle) is not None
        status = authority._preservation_coordinator().status()
        record = authority._preservation_coordinator()._records[tick.handle.lease.lease_id]
        assert record.state == "unknown" and record.reason == "custody_unknown"
        assert "I08_goals_loops_heartbeat" in status["active_owner_classes"]
        assert owner_ready(authority, close_generation=close.close_generation) is False
        assert not adapter._post_delivery_callbacks
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_goal_owner_ready_retains_live_i04_parent_until_post_turn_hook(
    tmp_path, monkeypatch,
):
    """An observed goal source cannot seal while its real managed I04 parent is still live."""
    from gateway.session_goal_tick_custody import mark_source_observed, owner_ready
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    try:
        binding = await fixture.claim("goal-close-held-parent")
        authority = fixture.authority
        mark_source_observed(authority, "producer:goal")
        close = authority._preservation_coordinator().begin_close(
            owner_id="goal-close-held-parent-owner", scope="process", deadline_s=30,
        )
        assert close is not None and close.close_generation >= 1
        status = authority._preservation_coordinator().status()
        assert "I04_fifo_turn_execution" in status["active_owner_classes"]
        assert binding.parent_handle is authority._preservation_children[binding.admission_id]
        assert owner_ready(authority, close_generation=close.close_generation) is False
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_goal_fallback_consumes_real_sqlite_automation_writer(tmp_path, monkeypatch):
    """The fired-loop handoff reaches the production automation writer and durable FIFO row."""
    from gateway.session_ingress import admit_preserved_automation
    from hermes_state_runtime import list_session_admissions

    runner, adapter, authority, _key, entry, event, handoff = await _real_goal_owner(
        tmp_path, monkeypatch,
    )
    try:
        receipt = await admit_preserved_automation(authority, adapter, event, handoff)
        rows = list_session_admissions(
            authority.db, session_id=authority.logical_owner(entry.session_id), pending_only=False,
        )
        wake = next(row for row in rows if row["request_id"] == handoff.request_id)
        assert receipt is not None
        assert wake["status"] == "queued"
        descriptor = wake["payload"]["native_text_v1"]["automation"]["custody"]
        assert descriptor["owner_class"] == "I08_goals_loops_heartbeat"
        assert descriptor["request_id"] == handoff.request_id
        assert descriptor["source_identity"] == handoff.source_identity
        assert authority._preservation_handles[wake["admission_id"]] is handoff.handle
        assert handoff.state == "admitted"
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_goal_fallback_real_loop_tick_reaches_terminal_fifo(tmp_path, monkeypatch):
    """A real LoopManager fire reaches the runner's route wake admission and FIFO settlement."""
    import time

    from gateway import session_finite
    from hermes_cli import loops
    from hermes_state_runtime import list_session_admissions

    runner, _adapter, authority, _key, entry, event, _handoff = await _real_goal_owner(
        tmp_path, monkeypatch, enable_drain=True,
    )
    try:
        monkeypatch.setattr(loops, "_get_session_db", lambda: authority.db)
        monkeypatch.setattr(loops, "goal_blocks_loop_tick", lambda _sid: False)
        manager = loops.LoopManager(session_id=entry.session_id)
        manager.set(
            "check the current state", interval_seconds=30,
            route={"platform": "discord", "chat_id": "42", "chat_type": "dm", "user_id": "42"},
        )
        manager.state.next_due_at = time.time() - 1
        loops.save_loop(entry.session_id, manager.state)

        async def finite(_authority, _ref, _row):
            return "loop terminal response"

        monkeypatch.setattr(session_finite, "execute_finite_admission", finite)
        await runner._loop_wakeup_fire_one(
            entry.session_id, manager.state, time.time() + 1, set(),
        )
        live = authority.sessions[authority.logical_owner(entry.session_id)]
        assert live.task is not None
        await asyncio.wait_for(asyncio.shield(live.task), timeout=5)

        rows = list_session_admissions(
            authority.db, session_id=authority.logical_owner(entry.session_id), pending_only=False,
        )
        wake = next(row for row in rows if row["principal_id"].startswith("automation:"))
        assert wake["status"] == "terminal"
        await runner._post_turn_loop_completion(
            session_entry=entry, source=event.source, final_response="loop terminal response",
        )
        settled = loops.load_loop(entry.session_id)
        assert settled is not None and settled.awaiting_response is False
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_goal_fallback_adapter_path_retains_same_handle_on_unbound_route(
        tmp_path, monkeypatch):
    """A false pre-effect route admission reaches the real adapter and stays unknown."""
    import json
    import time

    from hermes_cli import loops
    from hermes_state_runtime import RuntimeStoreError

    runner, adapter, authority, key, entry, _event, _handoff = await _real_goal_owner(
        tmp_path, monkeypatch, enable_drain=True, seed_native=False,
    )
    observed = []

    async def receive(event):
        try:
            await runner._hm_authority_admission(event, event.source, key, True)
        except RuntimeStoreError as exc:
            observed.append(exc.reason)

    adapter.set_message_handler(receive)
    try:
        monkeypatch.setattr(loops, "_get_session_db", lambda: authority.db)
        monkeypatch.setattr(loops, "goal_blocks_loop_tick", lambda _sid: False)
        manager = loops.LoopManager(session_id=entry.session_id)
        manager.set(
            "check the current state", interval_seconds=30,
            route={"platform": "discord", "chat_id": "42", "chat_type": "dm", "user_id": "42"},
        )
        manager.state.next_due_at = time.time() - 1
        loops.save_loop(entry.session_id, manager.state)
        await runner._loop_wakeup_fire_one(
            entry.session_id, manager.state, time.time() + 1, set(),
        )
        await asyncio.gather(*list(adapter._background_tasks), return_exceptions=True)
        request_id = json.dumps(
            ["loop", entry.session_id, 1], separators=(",", ":"),
        )
        record = next(
            item for item in authority._preservation_coordinator()._records.values()
            if (getattr(item.handle, "owner_class", None) == "I08_goals_loops_heartbeat"
                and getattr(item.handle, "owner_id", None)
                == f"I08_goals_loops_heartbeat:{authority.profile_id}:{entry.session_id}:{request_id}")
        )
        assert observed == ["not_found"]
        assert record.state == "unknown"
        persisted = loops.load_loop(entry.session_id)
        assert persisted is not None and persisted.awaiting_response is True
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_goal_fallback_slash_keeps_command_route_and_retains_same_handle_unknown(
        tmp_path, monkeypatch):
    """Slash wakeups keep their command semantics while the fired handle stays explicitly unknown."""
    runner, _adapter, authority, key, _entry, event, handoff = await _real_goal_owner(
        tmp_path, monkeypatch,
    )
    try:
        event.text = "/goal status"
        event.message_id = handoff.request_id
        event.allow_gateway_control = True
        event._preservation_handoff = handoff
        owned, reply = await runner._hm_authority_admission(
            event, event.source, key, True,
        )
        record = authority._preservation_coordinator()._records[handoff.handle.lease.lease_id]
        assert (owned, reply) == (False, None)
        assert event.is_command()
        assert record.handle is handoff.handle
        assert record.state == "unknown"
        assert handoff.state == "unknown"
    finally:
        runner.session_store.close_all_db_handles()


@pytest.mark.asyncio
async def test_goal_tick_exact_callback_cancel_marks_same_handle_unknown_before_reraise(
    tmp_path, monkeypatch,
):
    """Exact cancellation still consumes the generic I08 finalizer outside its outcome scope."""
    from gateway.platforms.base_delivery_callbacks import finish_post_delivery_callback
    from gateway.session_goal_tick_custody import (
        attach_tick_handle, reserve_tick, view_for_handle,
    )
    from gateway.session_goal_tick_runtime import register_tick_delivery_finalizer
    from gateway.session_automation_custody import automation_source_identity

    runner, adapter, authority, key, entry, event, _handoff = await _real_goal_owner(
        tmp_path, monkeypatch,
    )
    try:
        tick = reserve_tick(
            authority,
            request_id="goal-exact-cancel",
            principal_id="automation:" + key,
            session_id=authority.logical_owner(entry.session_id),
            source_identity=automation_source_identity(event, key),
            kind="goal",
        )
        assert tick is not None
        attach_tick_handle(event, tick, "goal")
        assert register_tick_delivery_finalizer(runner, event.source, event) is True

        async def cancelled_exact_callback():
            raise asyncio.CancelledError()

        exact_handle = adapter.register_post_delivery_callback_occurrence(
            key,
            cancelled_exact_callback,
            generation=getattr(event, "_hermes_run_generation", None),
            owner_event=event,
        )
        assert exact_handle is not None
        event._classic_execution_bridge = SimpleNamespace(
            post_delivery_callback_handle=exact_handle,
        )

        with pytest.raises(asyncio.CancelledError):
            await finish_post_delivery_callback(
                adapter,
                event,
                key,
                asyncio.Event(),
                processing_ok=True,
                classic_receipt=None,
                thread_metadata=None,
            )

        record = authority._preservation_coordinator()._records[tick.handle.lease.lease_id]
        assert record.handle is tick.handle
        assert record.state == "unknown"
        assert record.reason == "join_failed"
        assert view_for_handle(authority, tick.handle) is not None
        assert key not in adapter._post_delivery_callbacks
    finally:
        runner.session_store.close_all_db_handles()

