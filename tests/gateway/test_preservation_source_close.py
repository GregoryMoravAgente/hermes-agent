"""Behavioral proof for the adapter source close generation.

The tests exercise the source-owned admission boundary directly: accepted callbacks are retained
until ``native_callback`` exits, new callbacks are refused during the exact close generation, and
abort restores admission without releasing the retained callback.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway.platforms.base_source_close import PreservationSourceAdapterMixin, preservation_source_entry
from gateway.preservation_source_close import (
    PreservationSourceClose,
    SourceCloseContract,
    SourcePendingSnapshot,
    register_source_adapter,
)
from gateway.session_ingress_context import native_callback
from hermes_state_runtime import RuntimeStoreError


def test_source_pending_snapshot_requires_typed_producer_readiness_when_present():
    fields = {
        "close_generation": 1,
        "queue_epoch": "queue",
        "bridge_instance": "bridge",
        "owner_epoch": 1,
        "validated": True,
        "drain_complete": True,
        "producer_generation": "generation",
    }
    assert SourcePendingSnapshot(**fields, producer_state="unknown").ready is False
    assert SourcePendingSnapshot(**fields, producer_state="disconnected").ready is False
    assert SourcePendingSnapshot(
        **fields, producer_state="connected", producer_receipt_digest="d" * 64,
    ).ready is True
    # Non-WhatsApp source adapters retain their existing queue-only contract.
    assert SourcePendingSnapshot(**fields).ready is True


class _Adapter(PreservationSourceAdapterMixin):
    platform = "native"

    def __init__(self, handler):
        self._background_tasks = set()
        self._pending_text_batch_tasks = {}
        self._session_tasks = {}
        self._active_sessions = {}
        self._pending_messages = {}
        self._pending_text_batches = {}
        self._text_debounce = {}
        self._shared_bursts = {}
        self._shared_burst_commits = {}
        self._preservation_source_close_generation = None
        self._preservation_source_pending_tokens = set()
        self._handler = handler

    def _preservation_source_registry(self):
        return getattr(self, "_preservation_source_close", None)

    def _preservation_source_contract(self):
        return SourceCloseContract.for_adapter(self, name="native-test-adapter")

    @preservation_source_entry
    async def handle_message(self, event, *, _source_handoff=(), source_token=None, source_tokens=None):
        try:
            return await self._handler(event)
        finally:
            self._preservation_source_producer_done(_source_handoff or event)


def _runner(adapter):
    return SimpleNamespace(
        _transport_owner=lambda source: (adapter, None),
        _preservation_source_close=None,
    )


def _event():
    return SimpleNamespace(source=SimpleNamespace(platform="native"), _gateway_accepted=False)


@pytest.mark.asyncio
async def test_no_event_source_close_refuses_new_callback_and_aborts(tmp_path):
    runner = _runner(None)
    adapter = _Adapter(lambda _event: asyncio.sleep(0))
    runner._transport_owner = lambda source: (adapter, None)
    registry = PreservationSourceClose(runner)
    runner._preservation_source_close = registry
    register_source_adapter(runner, adapter)

    assert registry.begin_close(11) is True
    late = _event()
    with pytest.raises(RuntimeStoreError, match="runtime_draining"):
        with native_callback(runner, late, Path(tmp_path)):
            pass
    assert late._gateway_accepted is False
    assert registry.owner_ready(11) is True

    assert registry.abort(11) is True
    resumed = _event()
    with native_callback(runner, resumed, Path(tmp_path)):
        pass
    assert registry.owner_ready(11) is False


@pytest.mark.asyncio
async def test_preclose_accepted_callback_is_retained_until_native_exit(tmp_path):
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(event):
        event._gateway_accepted = True
        started.set()
        await release.wait()

    adapter = _Adapter(handler)
    runner = _runner(adapter)
    runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, adapter)
    event = _event()
    public = asyncio.create_task(adapter.handle_message(event))
    await started.wait()

    registry = runner._preservation_source_close
    assert registry.begin_close(12) is True
    adapter._background_tasks.add(public)
    public.add_done_callback(adapter._background_tasks.discard)
    assert registry.owner_ready(12) is False

    with native_callback(runner, event, Path(tmp_path)):
        assert registry.active_leases[0].native_entered is True
    assert registry.owner_ready(12) is False

    release.set()
    await public
    assert registry.owner_ready(12) is True


@pytest.mark.asyncio
async def test_unknown_source_handle_blocks_until_abort_and_re_registration(tmp_path):
    adapter = _Adapter(lambda _event: asyncio.sleep(0))
    runner = _runner(adapter)
    registry = PreservationSourceClose(runner)
    runner._preservation_source_close = registry
    register_source_adapter(runner, adapter)
    assert registry.begin_close(13) is True

    runner._transport_owner = lambda source: None
    unknown = SimpleNamespace(source=SimpleNamespace(platform="other"), _gateway_accepted=False)
    with pytest.raises(RuntimeStoreError, match="runtime_draining"):
        with native_callback(runner, unknown, Path(tmp_path)):
            pass
    assert registry.owner_ready(13) is False

    assert registry.abort(13) is True
    replacement = _Adapter(lambda _event: asyncio.sleep(0))
    replacement.platform = "other"
    runner._transport_owner = lambda source: (replacement, None)
    register_source_adapter(runner, replacement)
    assert registry.begin_close(14) is True
    assert registry.owner_ready(14) is False


@pytest.mark.asyncio
async def test_gateway_runner_no_event_close_proves_native_and_busy_sources(tmp_path, monkeypatch):
    """A real runner and adapter can close both bounded source classes without activity."""
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.run import GatewayRunner
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session_authority import initialize_session_authority
    from hermes_cli.backend_retirement import RetirementFence
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from tests.gateway.fixtures.whatsapp_queue_http import WhatsAppQueueHttpFixture, reserve_port

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    adapter = WhatsAppAdapter(PlatformConfig(enabled=True, extra={
        "session_path": str(tmp_path / "wa"), "bridge_port": reserve_port(),
    }))
    adapter.gateway_runner = runner
    runner.adapters = {Platform.WHATSAPP: adapter}
    authority = await initialize_session_authority(
        runner, profile_id="default", instance_id="source-close-runner",
    )
    coordinator = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id,
        authority_epoch=authority.epoch,
        fence=RetirementFence(),
    )
    runner.runtime_custody = coordinator
    runner._wire_adapter_handlers(adapter)
    fixture = None
    try:
        fixture = await WhatsAppQueueHttpFixture.start(adapter, tmp_path, name="no-event")
        await fixture.open()
        receipt = coordinator.begin_close(owner_id="source-close-runner", deadline_s=30)
        registry = runner._preservation_source_close
        assert registry is not None
        assert registry.begin_close(receipt.close_generation) is True
        assert await registry.await_quiescence(receipt.close_generation, 5) is True
        assert authority.preservation_owner_ready("I01_native_inbound") is True
        assert authority.preservation_owner_ready("I06_busy_followup_delivery") is True
        assert authority.preservation_owner_ready("I07_automation_route_wakes") is False
    finally:
        try:
            if fixture is not None:
                await fixture.close()
        finally:
            authority.db.close()


@pytest.mark.asyncio
async def test_two_profile_runner_registers_same_whatsapp_class_as_distinct_sources(tmp_path):
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.run import GatewayRunner
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from tests.gateway.fixtures.whatsapp_queue_http import WhatsAppQueueHttpFixture, reserve_port

    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    primary = WhatsAppAdapter(
        PlatformConfig(enabled=True, extra={
            "session_path": str(tmp_path / "wa-primary"), "bridge_port": reserve_port(),
        })
    )
    secondary = WhatsAppAdapter(
        PlatformConfig(enabled=True, extra={
            "session_path": str(tmp_path / "wa-secondary"), "bridge_port": reserve_port(),
        })
    )
    primary.gateway_runner = secondary.gateway_runner = runner
    runner.adapters = {Platform.WHATSAPP: primary}
    runner._profile_adapters = {"alt": {Platform.WHATSAPP: secondary}}
    runner._wire_adapter_handlers(primary)
    runner._wire_adapter_handlers(secondary, preservation_profile="alt")
    registry = runner._preservation_source_close
    assert registry is not None
    assert {item.adapter for item in registry.registrations} == {primary, secondary}

    primary_fixture = None
    secondary_fixture = None
    try:
        primary_fixture = await WhatsAppQueueHttpFixture.start(primary, tmp_path, name="profile-primary")
        secondary_fixture = await WhatsAppQueueHttpFixture.start(secondary, tmp_path, name="profile-secondary")
        await primary_fixture.open()
        await secondary_fixture.open()
        assert await primary_fixture.foreign_capability_status(secondary_fixture) == 409

        first_event = SimpleNamespace(source=SimpleNamespace(platform="whatsapp"))
        second_event = SimpleNamespace(source=SimpleNamespace(platform="whatsapp", profile="alt"))
        first = registry.admit_source(primary, {"profile": "default"}, event=first_event)
        second = registry.admit_source(secondary, {"profile": "alt"}, profile="alt", event=second_event)
        assert first.registration is not second.registration
        primary._preservation_source_producer_done(first)
        secondary._preservation_source_producer_done(second)
        assert registry.begin_close(51) is True
        assert registry.owner_ready(51) is False
        with native_callback(runner, first_event, Path(tmp_path), source_token=first):
            pass
        with native_callback(runner, second_event, Path(tmp_path), "alt", source_token=second):
            pass
        assert await registry.await_quiescence(51, 5) is True
        assert registry.owner_ready(51) is True
    finally:
        try:
            if secondary_fixture is not None:
                await secondary_fixture.close()
        finally:
            if primary_fixture is not None:
                await primary_fixture.close()


@pytest.mark.asyncio
async def test_real_base_standalone_keeps_source_until_runner_native_callback(tmp_path, monkeypatch):
    """The Base scheduler returns before the standalone runner callback owns native custody."""
    from contextlib import nullcontext

    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.event import MessageEvent, MessageType
    from gateway.run import GatewayRunner
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from tests.gateway.fixtures.whatsapp_queue_http import WhatsAppQueueHttpFixture, reserve_port

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    adapter = WhatsAppAdapter(
        PlatformConfig(enabled=True, extra={
            "session_path": str(tmp_path / "wa"), "bridge_port": reserve_port(),
        })
    )
    adapter.gateway_runner = runner
    runner.adapters = {Platform.WHATSAPP: adapter}

    background_started = asyncio.Event()
    release_background = asyncio.Event()
    native_started = asyncio.Event()
    release_native = asyncio.Event()
    admitted = []

    original_admit = adapter._preservation_source_admit

    def _record_admit(marker, **kwargs):
        token = original_admit(marker, **kwargs)
        if token is not None:
            admitted.append(token)
        return token

    async def _processing_hook(name, *_args, **_kwargs):
        if name == "on_processing_start":
            background_started.set()
            await release_background.wait()

    async def _stop_typing(*_args, **_kwargs):
        return None

    async def _release_marker(*_args, **_kwargs):
        return None

    async def _runner_handler(_event):
        native_started.set()
        await release_native.wait()

    adapter._preservation_source_admit = _record_admit
    adapter._run_processing_hook = _processing_hook
    adapter._start_typing_refresh = lambda *_args, **_kwargs: None
    adapter._stop_typing_refresh = _stop_typing
    adapter._release_turn_marker = _release_marker
    runner._handle_message = _runner_handler
    runner._standalone_launch_scope = lambda: nullcontext()
    runner._wire_adapter_handlers(adapter)
    fixture = None
    background_task = None
    try:
        fixture = await WhatsAppQueueHttpFixture.start(adapter, tmp_path, name="standalone")
        await fixture.open()
        event = MessageEvent(
            text="hello", message_type=MessageType.TEXT, source=adapter.build_source("standalone-chat")
        )
        await adapter.handle_message(event)
        background_task = next(iter(adapter._background_tasks))
        await background_started.wait()

        assert len(admitted) == 1
        token = admitted[0]
        assert event._gateway_accepted is True
        assert token.producer_done is True
        assert token.native_entered is False
        assert token.native_exited is False
        assert token in runner._preservation_source_close.active_leases

        registry = runner._preservation_source_close
        assert registry.begin_close(61) is True
        assert registry.owner_ready(61) is False

        release_background.set()
        await native_started.wait()
        assert token.native_entered is True
        assert token.native_exited is False
        assert token in registry.active_leases

        release_native.set()
        await background_task
        assert token.native_exited is True
        assert token not in registry.active_leases
        assert await registry.await_quiescence(61, 5) is True
        assert registry.owner_ready(61) is True
    finally:
        release_background.set()
        release_native.set()
        pending_tasks = set(getattr(adapter, "_background_tasks", set()))
        if background_task is not None:
            pending_tasks.add(background_task)
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        if fixture is not None:
            await fixture.close()


@pytest.mark.asyncio
async def test_whatsapp_late_refusal_replays_through_real_runner_without_dangling_unknown(
    tmp_path, monkeypatch,
):
    """A late poll refusal must be replayable through the real Base/native path after abort."""
    from contextlib import nullcontext

    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.event import MessageEvent, MessageType
    from gateway.run import GatewayRunner
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from tests.gateway.fixtures.whatsapp_queue_http import WhatsAppQueueHttpFixture, reserve_port

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    adapter = WhatsAppAdapter(
        PlatformConfig(enabled=True, extra={
            "session_path": str(tmp_path / "wa"), "bridge_port": reserve_port(),
        })
    )
    adapter.gateway_runner = runner
    adapter._send_read_receipts = False
    runner.adapters = {Platform.WHATSAPP: adapter}

    background_started = asyncio.Event()
    release_background = asyncio.Event()
    native_started = asyncio.Event()
    release_native = asyncio.Event()

    async def _processing_hook(name, *_args, **_kwargs):
        if name == "on_processing_start":
            background_started.set()
            await release_background.wait()

    async def _stop_typing(*_args, **_kwargs):
        return None

    async def _release_marker(*_args, **_kwargs):
        return None

    async def _runner_handler(_event):
        native_started.set()
        await release_native.wait()

    async def _same_event(_data):
        return event

    adapter._run_processing_hook = _processing_hook
    adapter._start_typing_refresh = lambda *_args, **_kwargs: None
    adapter._stop_typing_refresh = _stop_typing
    adapter._release_turn_marker = _release_marker
    adapter._build_message_event = _same_event
    runner._handle_message = _runner_handler
    runner._standalone_launch_scope = lambda: nullcontext()
    runner._wire_adapter_handlers(adapter)
    fixture = None
    background_task = None
    registry = runner._preservation_source_close
    assert registry is not None

    try:
        fixture = await WhatsAppQueueHttpFixture.start(adapter, tmp_path, name="late-refusal")
        await fixture.open()
        event = MessageEvent(
            text="late photo", message_type=MessageType.PHOTO,
            source=adapter.build_source("late-chat"),
        )
        msg_data = {"messageId": "late-1"}
        assert registry.begin_close(71) is True
        await adapter._dispatch_polled_message(msg_data)
        assert registry.active_leases == ()
        assert registry.owner_ready(71) is False
        assert await registry.await_resume(71, 5) is True
        assert registry.abort(71) is True

        await adapter._dispatch_polled_message(msg_data)
        await background_started.wait()
        background_task = next(task for task in adapter._background_tasks if not task.done())
        token = registry.active_leases[0]
        assert token.event is event
        assert token.producer_done is True
        assert token.native_entered is False

        assert registry.begin_close(72) is True
        assert registry.owner_ready(72) is False

        release_background.set()
        await native_started.wait()
        assert token.native_entered is True
        assert token.native_exited is False

        release_native.set()
        await background_task
        assert token.native_exited is True
        assert token not in registry.active_leases
        assert await registry.await_quiescence(72, 5) is True
        assert registry.owner_ready(72) is True
        assert await registry.await_resume(72, 5) is True
        resumed_health = await fixture.health()
        assert resumed_health["queue"]["intake_state"] == "open"
        assert resumed_health["queue"]["close_generation"] is None
        assert registry.abort(72) is True
    finally:
        release_background.set()
        release_native.set()
        pending_tasks = set(getattr(adapter, "_background_tasks", set()))
        if background_task is not None:
            pending_tasks.add(background_task)
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        if fixture is not None:
            await fixture.close()


@pytest.mark.asyncio
async def test_authenticated_whatsapp_queue_binder_controls_restart_and_wire_identity(tmp_path):
    from gateway.config import PlatformConfig
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from tests.gateway.fixtures.whatsapp_queue_http import WhatsAppQueueHttpFixture, reserve_port

    primary = WhatsAppAdapter(PlatformConfig(enabled=True, extra={
        "session_path": str(tmp_path / "wa-primary"), "bridge_port": reserve_port(),
    }))
    secondary = WhatsAppAdapter(PlatformConfig(enabled=True, extra={
        "session_path": str(tmp_path / "wa-secondary"), "bridge_port": reserve_port(),
    }))
    primary_fixture = None
    secondary_fixture = None
    try:
        primary_fixture = await WhatsAppQueueHttpFixture.start(primary, tmp_path, name="controls-primary")
        secondary_fixture = await WhatsAppQueueHttpFixture.start(secondary, tmp_path, name="controls-secondary")
        initial = await primary_fixture.health()
        assert initial["queue"]["producer_state"] == "unknown"
        connected = await primary_fixture.open()
        assert connected["queue"]["producer_state"] == "connected"
        assert connected["queue"]["producer_receipt_digest"]

        wire = await primary_fixture.enqueue_wire(
            {"messageId": "wire-1", "chatId": "chat-1", "body": "hello"},
            {"messageId": "wire-1", "chatId": "chat-1", "source": "messages.upsert"},
        )
        assert wire["status"] in {"queued", "enqueued"}
        assert wire["item_id"]
        assert wire["wire_identity"]["messageId"] == "wire-1"

        disconnected = await primary_fixture.disconnect()
        assert disconnected["queue"]["producer_state"] == "disconnected"
        assert disconnected["queue"]["producer_receipt_digest"] is None

        old_owner_epoch = connected["queue"]["owner_epoch"]
        restarted = await primary_fixture.restart()
        assert restarted["queue"]["owner_epoch"] > old_owner_epoch
        assert restarted["queue"]["owner_handoff"]["state"] == "adopted"
        assert restarted["queue"]["producer_state"] == "unknown"
        reopened = await primary_fixture.open()
        assert reopened["queue"]["producer_state"] == "connected"
        assert reopened["queue"]["owner_epoch"] == restarted["queue"]["owner_epoch"]

        await secondary_fixture.open()
        assert await primary_fixture.foreign_capability_status(secondary_fixture) == 409
    finally:
        try:
            if secondary_fixture is not None:
                await secondary_fixture.close()
        finally:
            if primary_fixture is not None:
                await primary_fixture.close()


@pytest.mark.asyncio
async def test_real_whatsapp_child_http_response_loss_replays_through_adapter(tmp_path):
    from gateway.config import PlatformConfig
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from tests.gateway.fixtures.whatsapp_queue_http import WhatsAppQueueHttpFixture, reserve_port

    adapter = WhatsAppAdapter(PlatformConfig(enabled=True, extra={
        "session_path": str(tmp_path / "wa"), "bridge_port": reserve_port(),
    }))
    fixture = None
    payload = {
        "messageId": "response-loss-child",
        "chatId": "chat@s.whatsapp.net",
        "body": "response-loss",
    }
    identity = {
        "messageId": "response-loss-child",
        "chatId": "chat@s.whatsapp.net",
        "source": "messages.upsert",
    }
    try:
        fixture = await WhatsAppQueueHttpFixture.start(adapter, tmp_path, name="response-loss-child")
        connected = await fixture.open()
        assert connected["queue"]["producer_state"] == "connected"
        assert connected["queue"]["producer_receipt_digest"]
        assert await fixture.enqueue_http_raw(
            payload, identity, request_id="response-loss-child-request",
        ) is None
        replayed = await fixture.enqueue_http(
            payload, identity, request_id="response-loss-child-request",
        )
        assert replayed["status"] in {"queued", "enqueued"}
        assert replayed["item"]["wire_identity"]["messageId"] == "response-loss-child"
        assert (await fixture.health())["queue"]["pending"] == 1
    finally:
        if fixture is not None:
            await fixture.close()


@pytest.mark.asyncio
async def test_real_whatsapp_child_http_response_loss_recovers_inside_one_adapter_operation(tmp_path):
    from gateway.config import PlatformConfig
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from tests.gateway.fixtures.whatsapp_queue_http import WhatsAppQueueHttpFixture, reserve_port

    adapter = WhatsAppAdapter(PlatformConfig(enabled=True, extra={
        "session_path": str(tmp_path / "wa"), "bridge_port": reserve_port(),
    }))
    fixture = None
    payload = {
        "messageId": "response-loss-truncated",
        "chatId": "chat@s.whatsapp.net",
        "body": "response-loss-truncated",
    }
    identity = {
        "messageId": "response-loss-truncated",
        "chatId": "chat@s.whatsapp.net",
        "source": "messages.upsert",
    }
    request_id = "response-loss-truncated-request"
    try:
        fixture = await WhatsAppQueueHttpFixture.start(adapter, tmp_path, name="response-loss-truncated")
        connected = await fixture.open()
        assert connected["queue"]["producer_state"] == "connected"
        assert connected["queue"]["producer_receipt_digest"]

        bridge_calls = []
        original_bridge_json = adapter._bridge_json

        async def observed_bridge_json(method, path, **kwargs):
            response = await original_bridge_json(method, path, **kwargs)
            bridge_calls.append((method, path, response))
            return response

        adapter._bridge_json = observed_bridge_json
        recovered = await fixture.enqueue_http(
            payload, identity, request_id=request_id, truncate_response=True,
        )
        assert bridge_calls[0] == ("post", "test/enqueue", (200, None))
        assert bridge_calls[1][0:2] == ("get", f"messages/requests/{request_id}")
        assert bridge_calls[1][2][0] == 200
        assert bridge_calls[1][2][1]["request_id"] == request_id
        assert recovered["status"] == "enqueued"
        assert recovered["request_id"] == request_id
        assert recovered["item"]["wire_identity"] == identity

        replayed = await fixture.enqueue_http(payload, identity, request_id=request_id)
        assert replayed == recovered

        conflict = await fixture.enqueue_http(
            {**payload, "body": "foreign-body"}, identity, request_id=request_id,
        )
        assert conflict is None
        assert (await fixture.health())["queue"]["pending"] == 1
    finally:
        if fixture is not None:
            await fixture.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", [["malformed"], "malformed"])
async def test_whatsapp_adapter_rejects_malformed_success_without_request_lookup(tmp_path, malformed):
    from gateway.config import PlatformConfig
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter

    adapter = WhatsAppAdapter(PlatformConfig(enabled=True, extra={
        "session_path": str(tmp_path / "wa"),
    }))
    calls = []

    async def malformed_bridge_json(method, path, **_kwargs):
        calls.append((method, path))
        return 200, malformed

    adapter._bridge_json = malformed_bridge_json
    result = await adapter._bridge_operation(
        "test/enqueue",
        {"request_id": "malformed-success", "payload": {}, "wire_identity": {}},
        timeout=5,
    )
    assert result is None
    assert calls == [("post", "test/enqueue")]


@pytest.mark.asyncio
async def test_real_whatsapp_child_wire_reaches_finalizer_settlement_and_idempotent_ack(
    tmp_path, monkeypatch,
):
    """The real child HTTP queue owns the wire item through finalizer and ACK replay."""
    from contextlib import nullcontext

    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.run import GatewayRunner
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from tests.gateway.fixtures.whatsapp_queue_http import WhatsAppQueueHttpFixture, reserve_port
    from tests.waiting import async_wait_until

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    adapter = WhatsAppAdapter(PlatformConfig(enabled=True, extra={
        "session_path": str(tmp_path / "wa"), "bridge_port": reserve_port(),
        "dm_policy": "allowlist", "allow_from": ["15550001111@s.whatsapp.net"],
    }))
    adapter.gateway_runner = runner
    adapter._send_read_receipts = False
    runner.adapters = {Platform.WHATSAPP: adapter}
    processed = asyncio.Event()
    admitted = []
    original_admit = adapter._preservation_source_admit

    def record_admit(marker, **kwargs):
        token = original_admit(marker, **kwargs)
        if token is not None:
            admitted.append(token)
        return token

    async def processing_hook(*_args, **_kwargs):
        return None

    async def stop_typing(*_args, **_kwargs):
        return None

    async def release_marker(*_args, **_kwargs):
        return None

    async def runner_handler(_event):
        processed.set()

    adapter._preservation_source_admit = record_admit
    adapter._run_processing_hook = processing_hook
    adapter._start_typing_refresh = lambda *_args, **_kwargs: None
    adapter._stop_typing_refresh = stop_typing
    adapter._release_turn_marker = release_marker
    runner._handle_message = runner_handler
    runner._standalone_launch_scope = lambda: nullcontext()
    runner._wire_adapter_handlers(adapter)

    fixture = None
    try:
        fixture = await WhatsAppQueueHttpFixture.start(adapter, tmp_path, name="finalizer-chain")
        await fixture.open()
        original_bridge_json = adapter._bridge_json
        response_loss = {"fired": False}
        conflict_responses = []

        async def controlled_bridge_json(method, path, **kwargs):
            status, data = await original_bridge_json(method, path, **kwargs)
            payload = kwargs.get("payload")
            if (
                method == "post"
                and path == "messages/transition"
                and isinstance(payload, dict)
                and payload.get("op") == "ack"
                and isinstance(payload.get("settlement"), dict)
                and payload["settlement"].get("producer_done") is True
                and not response_loss["fired"]
            ):
                assert status == 200
                assert isinstance(data, dict) and data.get("status") == "transitioned"
                response_loss["fired"] = True
                raise ConnectionError("controlled committed ACK response loss")
            if (
                method == "post"
                and path == "messages/transition"
                and isinstance(payload, dict)
                and payload.get("op") == "ack"
                and isinstance(payload.get("settlement"), dict)
                and payload["settlement"].get("producer_done") is False
            ):
                conflict_responses.append((status, data))
            return status, data

        monkeypatch.setattr(adapter, "_bridge_json", controlled_bridge_json)
        wire = await fixture.enqueue_wire(
            {"messageId": "finalizer-wire", "chatId": "chat@s.whatsapp.net", "senderId": "15550001111@s.whatsapp.net", "body": "hello"},
            {"messageId": "finalizer-wire", "chatId": "chat@s.whatsapp.net", "source": "messages.upsert"},
            request_id="finalizer-wire-request",
        )
        assert wire["item_id"]
        replay_wire = await fixture.enqueue_wire(
            {"messageId": "finalizer-wire", "chatId": "chat@s.whatsapp.net", "senderId": "15550001111@s.whatsapp.net", "body": "hello"},
            {"messageId": "finalizer-wire", "chatId": "chat@s.whatsapp.net", "source": "messages.upsert"},
            request_id="finalizer-wire-request",
        )
        assert replay_wire == wire
        conflict_wire = await fixture.enqueue_wire(
            {"messageId": "finalizer-wire", "chatId": "chat@s.whatsapp.net", "senderId": "15550001111@s.whatsapp.net", "body": "different"},
            {"messageId": "finalizer-wire", "chatId": "chat@s.whatsapp.net", "source": "messages.upsert"},
            request_id="finalizer-wire-request",
        )
        assert conflict_wire == {"status": "error", "error": "request_conflict"}
        await asyncio.wait_for(processed.wait(), timeout=5)

        def settled_token():
            if not admitted:
                return None
            token = admitted[0]
            return token if (
                token.finalizer_receipt is not None
                and token.transport_settled
                and token not in runner._preservation_source_close.active_leases
            ) else None

        token = await async_wait_until(settled_token, timeout=5, what="WhatsApp finalizer receipt")
        assert response_loss["fired"] is True
        assert token.finalizer_receipt.status == "success"
        assert token.finalizer_receipt.native_occurrence_id == token.native_occurrence_id
        assert token not in runner._preservation_source_close.active_leases

        delivery = token.transport_receipt
        assert isinstance(delivery, dict)
        assert delivery.get("ack_request_id")
        replay = await adapter._bridge_transition(
            "ack", delivery, expected_state="accepted", custody_ref=token.custody_ref,
        )
        assert isinstance(replay, dict)
        assert replay["status"] == "transitioned"
        conflicting_replay = await adapter._bridge_transition(
            "ack", delivery, expected_state="accepted", custody_ref=token.custody_ref,
            settlement={"producer_done": False},
        )
        assert conflicting_replay is None
        assert len(conflict_responses) == 1
        conflict_status, conflict_body = conflict_responses[0]
        assert conflict_status == 409
        assert isinstance(conflict_body, dict)
        assert conflict_body.get("error") == "request_conflict"
        health = await fixture.health()
        assert health["queue"]["acked"] == 1

        restarted = await fixture.restart()
        assert restarted["queue"]["acked"] == 1
        assert restarted["queue"]["owner_epoch"] > health["queue"]["owner_epoch"]
    finally:
        pending = set(getattr(adapter, "_background_tasks", set()))
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if fixture is not None:
            await fixture.close()



def _r22_projection_token(*, profile="profile-r22", generation=7):
    from gateway.preservation_source_close import SourceAdmissionToken

    adapter = object()
    transport_owner = object()
    registration = SimpleNamespace(
        adapter=adapter,
        profile=profile,
        generation=generation,
        transport_owner=transport_owner,
    )
    source = SimpleNamespace(
        to_dict=lambda: {"platform": "whatsapp", "messageId": "projection-r22"},
    )
    event = SimpleNamespace(source=source)
    token = SourceAdmissionToken(
        registration=registration,
        marker=("whatsapp-poll", "queue-r22", "item-r22"),
        phase="whatsapp-poll-callback",
        event=event,
        close_generation=3,
        identity=(adapter, profile, transport_owner, generation, 3),
        invocation_id=9,
    )
    return token, adapter, transport_owner


def _r22_projection_delivery():
    return {
        "queue_epoch": "queue-r22",
        "bridge_instance": "bridge-r22",
        "owner_epoch": 4,
        "consumer_id": "consumer-r22",
        "consumer_epoch": 2,
        "close_generation": 3,
        "producer_generation": "producer-r22",
        "item_id": "item-r22",
        "sequence": 9,
        "claim_id": "claim-r22",
        "custody_ref": "custody-r22",
        "settlement_challenge": "challenge-r22",
        "settle_request_id": "settle-r22-request",
        "wire_identity": {"messageId": "projection-r22", "chatId": "chat-r22"},
    }


def test_whatsapp_identity_projection_keeps_opaque_fences_and_shared_digests():
    import json

    from gateway.whatsapp_bridge_witness import (
        digest,
        settlement_fields,
        source_identity_projection,
    )

    token, adapter, transport_owner = _r22_projection_token()
    delivery = _r22_projection_delivery()
    token.capture_bridge_delivery(delivery)
    projection = source_identity_projection(token, delivery)
    assert json.dumps(projection, sort_keys=True)
    assert projection["source_identity"]["profile"] == "profile-r22"
    assert projection["source_identity"]["registration_generation"] == 7
    assert projection["source_identity"]["native_occurrence_id"] == "7:9"
    assert token.identity[0] is adapter
    assert token.identity[2] is transport_owner
    assert adapter not in projection["source_identity"].values()
    assert transport_owner not in projection["transport_owner"].values()
    assert token.source_identity_digest == digest(projection["source_identity"])
    assert token.source_event_digest == digest(projection["source_event"])
    assert token.transport_owner_digest == digest(projection["transport_owner"])

    del token._settlement_fields
    token.source_identity_digest = None
    token.source_event_digest = None
    token.transport_owner_digest = None
    fallback = settlement_fields(token, delivery, {})
    assert fallback["source_identity_digest"] == digest(projection["source_identity"])
    assert fallback["source_event_digest"] == digest(projection["source_event"])
    assert fallback["transport_owner_digest"] == digest(projection["transport_owner"])

    foreign, _foreign_adapter, _foreign_owner = _r22_projection_token(profile="foreign-profile")
    assert digest(source_identity_projection(foreign, delivery)["source_identity"]) != digest(projection["source_identity"])
    spoofed = {**delivery, "profile": "foreign-profile", "registration_generation": 999}
    assert source_identity_projection(token, spoofed)["source_identity"] == projection["source_identity"]
    changed_owner = source_identity_projection(token, {**delivery, "owner_epoch": 5})
    assert digest(changed_owner["transport_owner"]) != digest(projection["transport_owner"])
    with pytest.raises(TypeError, match="non-JSON"):
        source_identity_projection(token, {**delivery, "owner_epoch": object()})
