"""``inject_message(origin=...)`` under the session authority answers ``False`` up front for a chat
the FIFO cannot admit it into (no human message admitted there yet), instead of ``True`` for a
message the dispatch then drops (PLG-6). Real GatewayRunner, DiscordAdapter and SessionAuthority.
"""
import asyncio

import pytest
import hermes_yaml as yaml

from hermes_cli.plugins import PluginContext, PluginManager, PluginManifest
from tests.waiting import async_wait_until

PLUGIN = "desk"


def _context(home) -> tuple[PluginContext, PluginManager]:
    (home / "config.yaml").write_text(yaml.safe_dump({
        "plugins": {"entries": {PLUGIN: {"allow_gateway_injection": True}}},
    }), encoding="utf-8")
    manager = PluginManager(scope_key=str(home))
    return PluginContext(PluginManifest(name=PLUGIN, key=PLUGIN, source="user"), manager), manager


def _publish(runner, manager) -> None:
    runner._gateway_loop = asyncio.get_running_loop()
    runner._running = True
    manager.set_gateway_message_injector(runner, runner._schedule_plugin_message_injection)


def _plugin_rows(authority, session_id, text):
    from hermes_state_runtime import list_session_admissions
    return [row for row in list_session_admissions(authority.db, session_id=session_id, pending_only=False)
            if row["payload"].get("text") == text]


@pytest.mark.asyncio
async def test_origin_injection_is_refused_up_front_until_the_chat_holds_a_human_turn(monkeypatch):
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from gateway.run import GatewayRunner
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session_authority import initialize_session_authority
    from gateway.session_results import execution_result
    from hermes_constants import get_hermes_home
    from hermes_cli.backend_retirement import RetirementFence
    from plugins.platforms.discord.adapter import DiscordAdapter

    monkeypatch.setenv("DISCORD_ALLOWED_USERS", "42")
    runner = GatewayRunner(GatewayConfig())
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="fixture-token", typing_indicator=False))

    async def send(chat_id, content, reply_to=None, metadata=None):
        return SendResult(success=True, message_id="sent")
    monkeypatch.setattr(adapter, "send", send)
    adapter.gateway_runner = runner
    runner.adapters = {Platform.DISCORD: adapter}
    authority = await initialize_session_authority(runner, profile_id="default", instance_id="owner")
    runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id, authority_epoch=authority.epoch, fence=RetirementFence())
    runner._wire_adapter_handlers(adapter)
    executed = []

    async def turn(event, source, quick_key, run_generation):
        executed.append(event.text)
        captured = execution_result.get()
        if captured is not None:
            captured["result"] = {"final_response": "reply", "messages": []}
        return "reply"
    monkeypatch.setattr(runner, "_handle_message_with_agent", turn)

    ctx, manager = _context(get_hermes_home())
    _publish(runner, manager)
    origin = {"platform": "discord", "chat_id": "42", "chat_type": "dm", "user_id": "42"}
    source = adapter.build_source(chat_id="42", chat_type="dm", user_id="42")
    key = runner.session_store._generate_session_key(source)

    # No session in that chat yet: the FIFO cannot admit a producer turn there.
    assert await asyncio.to_thread(ctx.inject_message, "reminder", origin=origin) is False
    assert runner.session_store.lookup_by_session_key(key) is None

    # A session row with no committed human turn has no connector provenance to borrow either.
    runner.session_store.get_or_create_session(source)
    assert await asyncio.to_thread(ctx.inject_message, "reminder", origin=origin) is False

    # Once a human message was admitted there, the plugin turn joins the chat's durable FIFO.
    await adapter.handle_message(MessageEvent(text="A", source=source, message_id="A"))
    await async_wait_until(lambda: "A" in executed, timeout=10, what="the human turn")
    assert await asyncio.to_thread(ctx.inject_message, "reminder", origin=origin) is True
    owner = authority.logical_owner(runner.session_store.lookup_by_session_key(key).session_id)
    await async_wait_until(lambda: _plugin_rows(authority, owner, "reminder"), timeout=10,
                           what="the plugin admission")


async def _plugin_custody_route(monkeypatch, *, human):
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.event import MessageEvent
    from gateway.run import GatewayRunner
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session_authority import initialize_session_authority
    from gateway.session_ingress_context import native_callback
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_constants import get_hermes_home
    from plugins.platforms.discord.adapter import DiscordAdapter

    monkeypatch.setenv("DISCORD_ALLOWED_USERS", "42")
    runner = GatewayRunner(GatewayConfig())
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="fixture-token", typing_indicator=False))
    adapter.gateway_runner = runner
    runner.adapters = {Platform.DISCORD: adapter}
    authority = await initialize_session_authority(runner, profile_id="default", instance_id="plugin-errors")
    runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id, authority_epoch=authority.epoch, fence=RetirementFence())
    runner._wire_adapter_handlers(adapter)
    monkeypatch.setattr(authority, "_schedule", lambda _ref: None)
    source = adapter.build_source(chat_id="42", chat_type="dm", user_id="42")
    key = runner.session_store._generate_session_key(source)
    entry = runner.session_store.get_or_create_session(source)
    if human:
        event = MessageEvent(text="human provenance", source=source, message_id="human-provenance")
        with native_callback(runner, event, get_hermes_home()):
            await authority.admit_native(event)
    return runner, authority, adapter, source, key, entry.session_id


@pytest.mark.asyncio
async def test_plugin_snapshot_refusal_releases_its_real_unadmitted_owner(monkeypatch):
    runner, authority, adapter, source, key, sid = await _plugin_custody_route(monkeypatch, human=False)
    assert await runner._admit_plugin_injection(
        adapter, source, session_key=key, session_id=sid, content="no human", plugin_id=PLUGIN) is False
    assert _plugin_rows(authority, sid, "no human") == []
    assert "I07_automation_route_wakes" not in runner.runtime_custody.status()["active_owner_classes"]


@pytest.mark.asyncio
async def test_plugin_cancel_before_submit_releases_the_original_reserved_handle(monkeypatch):
    runner, authority, adapter, source, key, sid = await _plugin_custody_route(monkeypatch, human=True)
    captured = {}

    async def cancel_before_submit(*_args, **kwargs):
        captured["handle"] = kwargs["_preservation_handle"]
        raise asyncio.CancelledError

    monkeypatch.setattr(authority, "admit_automation", cancel_before_submit)
    with pytest.raises(asyncio.CancelledError):
        await runner._admit_plugin_injection(
            adapter, source, session_key=key, session_id=sid, content="cancelled plugin", plugin_id=PLUGIN)
    handle = captured["handle"]
    assert _plugin_rows(authority, sid, "cancelled plugin") == []
    assert handle.lease.lease_id not in runner.runtime_custody._records


@pytest.mark.asyncio
async def test_plugin_lost_ack_and_failed_readback_retain_the_exact_committed_owner(monkeypatch):
    from hermes_state_runtime import RuntimeStoreError

    runner, authority, adapter, source, key, sid = await _plugin_custody_route(monkeypatch, human=True)
    original = authority.admit_automation
    captured = {}

    def unreadable_row(*_args, **_kwargs):
        raise RuntimeStoreError("read_failed")

    async def commit_then_lose_ack(*args, **kwargs):
        captured["handle"] = kwargs["_preservation_handle"]
        await original(*args, **kwargs)
        monkeypatch.setattr("gateway.session_authority.find_session_admission", unreadable_row)
        raise RuntimeStoreError("read_failed")

    monkeypatch.setattr(authority, "admit_automation", commit_then_lose_ack)
    assert await runner._admit_plugin_injection(
        adapter, source, session_key=key, session_id=sid, content="committed plugin", plugin_id=PLUGIN) is False
    rows = _plugin_rows(authority, sid, "committed plugin")
    assert len(rows) == 1
    handle = captured["handle"]
    assert authority._preservation_handles[rows[0]["admission_id"]] is handle
    record = runner.runtime_custody._records[handle.lease.lease_id]
    assert record.handle is handle
    assert record.state == "unknown"


@pytest.mark.asyncio
async def test_automation_descriptor_failure_releases_its_real_prewrite_owner(monkeypatch):
    from gateway.platforms.event import MessageEvent
    from hermes_state_runtime import RuntimeStoreError

    runner, authority, adapter, source, key, sid = await _plugin_custody_route(monkeypatch, human=True)

    def refuse_descriptor(*_args, **_kwargs):
        raise RuntimeStoreError("preservation_owner_unmapped")

    monkeypatch.setattr(authority, "_preservation_automation_descriptor", refuse_descriptor)
    event = MessageEvent(
        text="bad descriptor", source=source, message_id="bad-descriptor", internal=True,
        allow_gateway_control=False, metadata={"gateway_session_key": key, "gateway_session_id": sid})
    with pytest.raises(RuntimeStoreError, match="preservation_owner_unmapped"):
        await authority.admit_automation(adapter, event, "bad-descriptor")
    assert _plugin_rows(authority, sid, "bad descriptor") == []
    assert "I07_automation_route_wakes" not in runner.runtime_custody.status()["active_owner_classes"]
