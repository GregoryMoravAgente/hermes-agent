"""Source-owned close contracts for real pre-handler adapter boundaries.

These tests stay at the contract seam: they do not start a provider or infer readiness from an
empty adapter map.  Every accepted item has an opaque token, an exact registration identity, and
an explicit terminal/replay reconciliation path.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
from collections import OrderedDict
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway.preservation_source_close import (
    PreservationSourceClose,
    SourceCloseContract,
    register_source_adapter,
)
from gateway.platforms.base import BasePlatformAdapter
from gateway.platforms.event import MessageEvent, MessageType
from gateway.session_ingress_context import native_callback
from hermes_state_runtime import RuntimeStoreError


class _ContractAdapter:
    platform = "contract"

    def __init__(self):
        self._preservation_source_close_generation = None
        self._preservation_source_pending_tokens = set()

    async def handle_message(self, event):
        event._gateway_accepted = True

    def _preservation_source_contract(self):
        return SourceCloseContract.for_adapter(self, name="contract-adapter")

    def _preservation_close_source(self, generation):
        self._preservation_source_close_generation = generation
        return True

    def _preservation_resume_source(self, generation):
        if self._preservation_source_close_generation != generation:
            return False
        self._preservation_source_close_generation = None
        return True

    def _preservation_source_pending(self, generation):
        return tuple(self._preservation_source_pending_tokens)

    def _preservation_source_producer_done(self, item):
        registry = getattr(self, "_preservation_source_close", None)
        if registry is not None:
            registry.mark_producer_done(item)


def _runner(adapter=None, profile=None):
    return SimpleNamespace(
        _preservation_source_close=None,
        _transport_owner=lambda _source: (adapter, profile) if adapter is not None else None,
        session_authority=None,
    )


def _event(platform="contract"):
    return SimpleNamespace(source=SimpleNamespace(platform=platform), _gateway_accepted=False)


def _provider_event(platform: str) -> SimpleNamespace:
    return SimpleNamespace(
        source=SimpleNamespace(platform=platform, chat_id="chat", chat_type="dm"),
        text="hello", media_urls=[], media_types=[], _gateway_accepted=False,
    )


def _register_provider(adapter, profile=None):
    adapter._preservation_source_close_generation = None
    adapter._preservation_source_pending_tokens = set()
    runner = _runner(adapter, profile)
    adapter.gateway_runner = runner
    registry = runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, adapter, profile)
    return runner, registry


async def _accept_source_event(event, *, source_tokens=()):
    event._gateway_accepted = True


def test_zero_callback_close_is_proven_by_registered_source_gate(tmp_path):
    adapter = _ContractAdapter()
    runner = _runner(adapter, "profile-a")
    registry = runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, adapter, "profile-a")

    assert registry.begin_close(31) is True
    assert registry.owner_ready(31) is True
    assert registry.abort(31) is True

    # A cold callback is accepted only after the exact source is reopened.
    event = _event()
    with native_callback(runner, event, Path(tmp_path), "profile-a"):
        pass
    assert registry.active_leases == ()


def test_prehandler_token_blocks_close_until_native_exit_and_generation_is_exact(tmp_path):
    adapter = _ContractAdapter()
    runner = _runner(adapter, "profile-a")
    registry = runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, adapter, "profile-a")

    event = _event()
    token = registry.admit_source(adapter, {"delivery": "accepted"}, profile="profile-a", phase="pre-handler")
    registry.bind_event(token, event)
    registry.mark_producer_done(token)
    assert registry.begin_close(32) is True
    assert registry.owner_ready(32) is False

    with native_callback(runner, event, Path(tmp_path), "profile-a"):
        assert token.native_entered is True
    assert registry.owner_ready(32) is True


def test_same_event_needs_explicit_handoff_and_late_admission_is_refused():
    adapter = _ContractAdapter()
    runner = _runner(adapter, "profile-a")
    registry = runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, adapter, "profile-a")
    event = _event()
    first = registry.admit_source(adapter, {"delivery": 1}, profile="profile-a", event=event)
    second = registry.admit_source(adapter, {"delivery": 2}, profile="profile-a", event=event)
    assert second is not first
    assert second.invocation_id != first.invocation_id
    assert registry.admit_source(
        adapter, {"handoff": True}, profile="profile-a", event=event, source_token=first,
    ) is first
    assert len(registry.active_leases) == 2
    assert registry.begin_close(34) is True
    with pytest.raises(RuntimeStoreError, match="runtime_draining"):
        registry.admit_source(adapter, {"delivery": 3}, profile="profile-a")
    assert registry.abort(34) is True
    registry.mark_unknown(first, "close_race_requires_reconcile")
    assert registry.owner_ready(34) is False
    assert registry.reconcile(first, "terminal") is True
    assert registry.reconcile(second, "terminal") is True


def test_empty_native_witness_reuses_only_one_event_token(tmp_path):
    adapter = _ContractAdapter()
    runner = _runner(adapter, "profile-a")
    registry = runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, adapter, "profile-a")
    event = _event()
    token = registry.admit_source(adapter, {"delivery": "one"}, profile="profile-a", event=event)
    registry.mark_producer_done(token)
    assert registry.begin_close(37) is True
    with native_callback(runner, event, Path(tmp_path), "profile-a", source_token=()):
        assert token.native_entered is True
    assert registry.owner_ready(37) is True


def test_ambiguous_event_tokens_fail_closed_without_arbitrary_reuse(tmp_path):
    adapter = _ContractAdapter()
    runner = _runner(adapter, "profile-a")
    registry = runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, adapter, "profile-a")
    event = _event()
    first = registry.admit_source(adapter, {"delivery": 1}, profile="profile-a", event=event)
    second = registry.admit_source(adapter, {"delivery": 2}, profile="profile-a", event=event)
    registry.mark_producer_done(first)
    registry.mark_producer_done(second)
    assert registry.begin_close(38) is True
    with pytest.raises(RuntimeStoreError, match="source_identity_ambiguous"):
        with native_callback(runner, event, Path(tmp_path), "profile-a"):
            pass
    assert first.state == "unknown"
    assert second.state == "unknown"
    assert registry.owner_ready(38) is False
    assert registry.abort(38) is True
    assert registry.reconcile(first, "terminal") is True
    assert registry.reconcile(second, "terminal") is True


def test_foreign_native_witness_fails_closed_before_callback_entry(tmp_path):
    adapter = _ContractAdapter()
    foreign = _ContractAdapter()
    runner = _runner(adapter, "profile-a")
    registry = runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, adapter, "profile-a")
    register_source_adapter(runner, foreign, "profile-b")
    token = registry.admit_source(foreign, {"delivery": "foreign"}, profile="profile-b")
    event = _event()
    assert registry.begin_close(39) is True
    with pytest.raises(RuntimeStoreError, match="source_identity_mismatch"):
        with native_callback(runner, event, Path(tmp_path), "profile-a", source_token=token):
            pass
    assert token.state == "unknown"
    assert registry.owner_ready(39) is False
    assert registry.abort(39) is True
    assert registry.reconcile(token, "terminal") is True


class _UnwiredSubclass:
    _preservation_source_contract = BasePlatformAdapter._preservation_source_contract

    async def handle_message(self, event):
        event._gateway_accepted = True


def test_subclass_without_source_contract_is_explicitly_unknown():
    adapter = object.__new__(_UnwiredSubclass)
    adapter.platform = "unwired"
    adapter._preservation_source_close_generation = None
    adapter._preservation_source_pending_tokens = set()
    runner = _runner(adapter, "profile-a")
    registry = runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, adapter, "profile-a")
    assert registry.begin_close(35) is False
    assert registry.owner_ready(35) is False


def test_replaced_handler_and_unregistered_instance_fail_closed():
    adapter = _ContractAdapter()
    runner = _runner(adapter)
    registry = runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, adapter)

    adapter.handle_message = lambda _event: None
    with pytest.raises(RuntimeStoreError, match="source_registration_missing"):
        registry.admit_public(adapter, _event())

    replacement = _ContractAdapter()
    replacement.platform = "other"
    with pytest.raises(RuntimeStoreError, match="source_registration_missing"):
        registry.admit_source(replacement, {"delivery": "unregistered"})


def test_zero_event_handler_replacement_blocks_close_and_owner_ready():
    adapter = _ContractAdapter()
    runner = _runner(adapter, "profile-a")
    registry = runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, adapter, "profile-a")

    adapter.handle_message = lambda _event: None
    closed = registry.begin_close(40)
    ready = registry.owner_ready(40)
    assert closed is False
    assert ready is False


def test_abort_keeps_unknown_until_explicit_reconcile_and_rehome_is_identity_bound():
    predecessor = _ContractAdapter()
    successor = _ContractAdapter()
    runner = _runner(predecessor, "profile-a")
    registry = runner._preservation_source_close = PreservationSourceClose(runner)
    register_source_adapter(runner, predecessor, "profile-a")
    register_source_adapter(runner, successor, "profile-b")

    token = registry.admit_source(predecessor, {"message": "held"}, profile="profile-a", phase="held")
    assert registry.begin_close(33) is True
    registry.mark_unknown(token, "disconnect_before_replay")
    assert registry.abort(33) is True
    assert registry.owner_ready(33) is False

    event = _event()
    successor_token = registry.rehome(token, successor, event, "profile-b")
    assert successor_token is not None
    assert successor_token.registration_generation != token.registration_generation
    successor._preservation_source_producer_done(successor_token)
    assert registry.reconcile(successor_token, "replayed") is True
    registry.release(successor_token)
    assert registry.begin_close(36) is True
    assert registry.owner_ready(36) is True


@pytest.mark.asyncio
async def test_real_telegram_batch_keeps_token_until_native_callback(tmp_path):
    from plugins.platforms.telegram.adapter import TelegramAdapter

    adapter = object.__new__(TelegramAdapter)
    adapter.platform = "telegram"
    adapter._should_drop_delayed_delivery = lambda: False
    adapter._hold_inbound_event = lambda *_args, **_kwargs: None
    adapter.handle_message = _accept_source_event
    runner, registry = _register_provider(adapter)
    event = _provider_event("telegram")
    token = adapter._preservation_source_admit(
        {"update": "batch"}, event=event, phase="telegram-batch"
    )
    pending = {"batch": event}
    tasks = {}
    task = asyncio.create_task(
        TelegramAdapter._flush_buffered(adapter, pending, tasks, "batch", 0, "text")
    )
    tasks["batch"] = task
    await task
    assert token.producer_done is True
    assert token in registry.active_leases
    assert registry.begin_close(101) is True
    assert registry.owner_ready(101) is False
    with native_callback(runner, event, Path(tmp_path), source_token=token):
        pass
    assert registry.owner_ready(101) is True


@pytest.mark.asyncio
async def test_real_feishu_media_batch_keeps_token_until_native_callback(tmp_path):
    from plugins.platforms.feishu.adapter import FeishuAdapter

    adapter = object.__new__(FeishuAdapter)
    adapter.platform = "feishu"
    adapter.handle_message = _accept_source_event
    adapter._handle_message_with_guards = _accept_source_event
    adapter._pending_media_batches = {}
    runner, registry = _register_provider(adapter)
    event = _provider_event("feishu")
    event.media_urls = ["/tmp/synthetic.jpg"]
    event._preservation_source_deferred = True
    token = adapter._preservation_source_admit(
        {"media": 1}, event=event, phase="feishu-media"
    )
    adapter._pending_media_batches["media"] = event
    await FeishuAdapter._flush_media_batch_now(adapter, "media")
    assert token.producer_done is True
    assert token in registry.active_leases
    assert registry.begin_close(102) is True
    with native_callback(runner, event, Path(tmp_path), source_token=token):
        pass
    assert registry.owner_ready(102) is True


@pytest.mark.asyncio
async def test_real_wecom_early_ack_queue_keeps_token_until_native_callback(tmp_path):
    from plugins.platforms.wecom.callback_adapter import WecomCallbackAdapter

    class _OneShotQueue:
        def __init__(self, event):
            self.event = event
            self.used = False

        async def get(self):
            if not self.used:
                self.used = True
                return self.event
            raise asyncio.CancelledError

    adapter = object.__new__(WecomCallbackAdapter)
    adapter.platform = "wecom_callback"
    adapter.handle_message = _accept_source_event
    adapter._background_tasks = set()
    event = _provider_event("wecom_callback")
    runner, registry = _register_provider(adapter)
    token = adapter._preservation_source_admit(
        {"callback": "early-ack"}, event=event, phase="wecom-ack"
    )
    adapter._message_queue = _OneShotQueue(event)
    poll = asyncio.create_task(WecomCallbackAdapter._poll_loop(adapter))
    await asyncio.sleep(0)
    with suppress(asyncio.CancelledError):
        await poll
    await asyncio.sleep(0)
    for task in tuple(adapter._background_tasks):
        await task
    assert token.producer_done is True
    assert token in registry.active_leases
    assert registry.begin_close(103) is True
    with native_callback(runner, event, Path(tmp_path), source_token=token):
        pass
    assert registry.owner_ready(103) is True


@pytest.mark.asyncio
async def test_real_yuanbao_push_ack_and_debounce_keep_token_until_native_callback(tmp_path, monkeypatch):
    import plugins.platforms.yuanbao.adapter as yuanbao_module
    from plugins.platforms.yuanbao.adapter import ConnectionManager, InboundContext

    class _WebSocket:
        def __init__(self):
            self.sent = []

        async def send(self, value):
            self.sent.append(value)

    adapter = object.__new__(yuanbao_module.YuanbaoAdapter)
    adapter.platform = SimpleNamespace(value="yuanbao")
    adapter.handle_message = _accept_source_event
    adapter._group_queues = {}
    adapter._active_sessions = {}
    adapter._inbound_tasks = set()
    adapter._processing_msg_ids = {}
    adapter._processing_msg_texts = {}
    adapter._msg_content_cache = {}
    adapter._source_session_key = lambda _source: "group-key"
    adapter._inbound_pipeline = SimpleNamespace()
    runner, registry = _register_provider(adapter)
    admitted = []
    original_admit = adapter._preservation_source_admit

    def _record_admit(marker, **kwargs):
        token = original_admit(marker, **kwargs)
        if token is not None:
            admitted.append(token)
        return token

    adapter._preservation_source_admit = _record_admit

    async def execute(context):
        event = _provider_event("yuanbao")
        adapter._preservation_source_bind(context.source_tokens[0], event)

    adapter._inbound_pipeline.execute = execute

    def track(task):
        adapter._inbound_tasks.add(task)
        task.add_done_callback(adapter._inbound_tasks.discard)
        return task

    adapter._track_task = track
    connection = object.__new__(ConnectionManager)
    connection._adapter = adapter
    connection._ws = _WebSocket()
    connection._pending_acks = {}
    connection._pending_pong = None
    connection._inbound_buffer = {}
    connection._inbound_timers = {}
    monkeypatch.setattr(
        yuanbao_module, "decode_conn_msg",
        lambda _raw: {"head": {"cmd_type": yuanbao_module.CMD_TYPE["Push"], "cmd": "push", "msg_id": "", "need_ack": True}, "data": b"{}"},
    )
    monkeypatch.setattr(yuanbao_module, "encode_push_ack", lambda _head: b"ack")
    await connection._handle_frame(b"synthetic-push")
    assert connection._ws.sent == [b"ack"]
    token = admitted[0]
    assert token.producer_done is True
    key = next(iter(connection._inbound_buffer))
    connection._inbound_timers[key].cancel()
    connection._flush_inbound_buffer(key)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert token.event is not None
    assert token in registry.active_leases

    group_token = adapter._preservation_source_admit({"group": 1}, phase="yuanbao-group")
    source = SimpleNamespace(platform="yuanbao", chat_id="group", chat_type="group")
    context = InboundContext(
        adapter=adapter, source=source, chat_type="group", raw_text="group message",
        msg_type=MessageType.TEXT, msg_id="group-1", source_tokens=[group_token],
    )
    await yuanbao_module.DispatchMiddleware().handle(context, lambda: asyncio.sleep(0))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert group_token.event is not None
    assert group_token.producer_done is True
    assert registry.begin_close(104) is True
    with native_callback(runner, token.event, Path(tmp_path), source_token=token):
        pass
    with native_callback(runner, group_token.event, Path(tmp_path), source_token=group_token):
        pass
    assert registry.owner_ready(104) is True


@pytest.mark.asyncio
async def test_real_a2a_task_dispatch_keeps_token_until_native_callback(tmp_path, monkeypatch):
    import plugins.platforms.a2a.adapter as a2a_module
    from plugins.platforms.a2a.adapter import A2AAdapter

    class _Tasks:
        def create(self, task_id, context_id, peer, slug, tenant, input_id=""):
            return {"task_id": task_id, "context_id": context_id, "created_iso": "synthetic"}

        def set_state(self, *_args):
            return None

    adapter = object.__new__(A2AAdapter)
    adapter.platform = "a2a"
    adapter._agents = {"": {"local": True, "slug": "", "tenant": ""}}
    adapter._forwarded_inputs = OrderedDict()
    adapter._message_handler = object()
    adapter._loop = asyncio.get_running_loop()
    adapter._pending = {}
    adapter._pending_order = {}
    adapter._active_tasks = set()
    adapter._pending_lock = __import__("threading").Lock()
    adapter.tasks = _Tasks()
    adapter._turns = SimpleNamespace(track=lambda _context: 0)
    adapter.handle_message = _accept_source_event
    adapter._note_forwarded_input = lambda _input_id: False
    adapter._forget_forwarded_input = lambda _input_id: None
    adapter._register_inline_push = lambda *_args, **_kwargs: None
    adapter._add_pending = lambda task_id, context_id: asyncio.get_running_loop().create_future()
    adapter.build_source = lambda **kwargs: SimpleNamespace(**kwargs, platform="a2a")
    runner, registry = _register_provider(adapter)
    monkeypatch.setattr(a2a_module.protocol, "extract_text", lambda _params: "hello")
    monkeypatch.setattr(a2a_module.protocol, "extract_context_id", lambda _params: "ctx")
    monkeypatch.setattr(a2a_module.protocol, "extract_message_id", lambda _params: "")
    monkeypatch.setattr(a2a_module.protocol, "new_task_id", lambda: "task-1")
    monkeypatch.setattr(a2a_module.protocol, "persist_message", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(a2a_module.security, "wrap_inbound", lambda _peer, text: text)
    monkeypatch.setattr(a2a_module.security, "audit", lambda *_args: None)
    terminal, pending = adapter._prepare_task({"text": "hello", "contextId": "ctx"}, "peer")
    assert terminal is None
    assert pending is not None
    await asyncio.sleep(0)
    token = next(iter(registry.active_leases))
    assert token.event is not None
    assert token.producer_done is True
    assert registry.begin_close(105) is True
    with native_callback(runner, token.event, Path(tmp_path), source_token=token):
        pass
    assert registry.owner_ready(105) is True


@pytest.mark.asyncio
async def test_real_whatsapp_background_batch_keeps_token_until_native_callback(tmp_path):
    from gateway.config import PlatformConfig
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from tests.gateway.fixtures.whatsapp_queue_http import WhatsAppQueueHttpFixture, reserve_port

    adapter = WhatsAppAdapter(PlatformConfig(enabled=True, extra={
        "session_path": str(tmp_path / "wa"), "bridge_port": reserve_port(),
    }))
    adapter.handle_message = _accept_source_event
    runner, registry = _register_provider(adapter)
    fixture = None
    try:
        fixture = await WhatsAppQueueHttpFixture.start(adapter, tmp_path, name="contract-batch")
        await fixture.open()
        event = _provider_event("whatsapp")
        event._preservation_source_deferred = True
        token = adapter._preservation_source_admit(
            {"poll": 1}, event=event, phase="whatsapp-poll"
        )
        await WhatsAppAdapter._dispatch_text_batch(adapter, event)
        assert token.producer_done is True
        assert token in registry.active_leases
        assert registry.begin_close(106) is True
        with native_callback(runner, event, Path(tmp_path), source_token=token):
            pass
        assert await registry.await_quiescence(106, 5) is True
        assert registry.owner_ready(106) is True
    finally:
        if fixture is not None:
            await fixture.close()


@pytest.mark.asyncio
async def test_real_base_multiplex_profiles_keep_tokens_isolated_until_native_callbacks(
    tmp_path, monkeypatch,
):
    """Each same-class profile reaches its own runner native wrapper after Base scheduling."""
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.run import GatewayRunner
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from tests.gateway.fixtures.whatsapp_queue_http import WhatsAppQueueHttpFixture, reserve_port

    default_home = tmp_path / "hermes"
    alt_home = default_home / "profiles" / "alt"
    alt_home.mkdir(parents=True)
    monkeypatch.setenv("HERMES_HOME", str(default_home))

    @asynccontextmanager
    async def _profile_scope(_home):
        yield

    import gateway.run as gateway_run

    monkeypatch.setattr(gateway_run, "_async_profile_runtime_scope", _profile_scope)
    runner = GatewayRunner(
        GatewayConfig(
            multiplex_profiles=True,
            sessions_dir=tmp_path / "sessions",
        )
    )
    runner._primary_profile_name = "default"
    runner.config._runtime_profile_homes = [("default", default_home), ("alt", alt_home)]
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
    secondary.set_owner_profile("alt")
    runner.adapters = {Platform.WHATSAPP: primary}
    runner._profile_adapters = {"alt": {Platform.WHATSAPP: secondary}}

    probes = {
        "default": {
            "background_started": asyncio.Event(),
            "release_background": asyncio.Event(),
            "native_started": asyncio.Event(),
            "release_native": asyncio.Event(),
            "admitted": [],
        },
        "alt": {
            "background_started": asyncio.Event(),
            "release_background": asyncio.Event(),
            "native_started": asyncio.Event(),
            "release_native": asyncio.Event(),
            "admitted": [],
        },
    }

    def _install_probe(adapter, profile):
        probe = probes[profile]
        original_admit = adapter._preservation_source_admit

        def _record_admit(marker, **kwargs):
            token = original_admit(marker, **kwargs)
            if token is not None:
                probe["admitted"].append(token)
            return token

        async def _processing_hook(name, *_args, **_kwargs):
            if name == "on_processing_start":
                probe["background_started"].set()
                await probe["release_background"].wait()

        async def _stop_typing(*_args, **_kwargs):
            return None

        async def _release_marker(*_args, **_kwargs):
            return None

        adapter._preservation_source_admit = _record_admit
        adapter._run_processing_hook = _processing_hook
        adapter._start_typing_refresh = lambda *_args, **_kwargs: None
        adapter._stop_typing_refresh = _stop_typing
        adapter._release_turn_marker = _release_marker

    async def _runner_handler(event):
        profile = getattr(event.source, "profile", None) or "default"
        probe = probes[profile]
        probe["native_started"].set()
        await probe["release_native"].wait()

    _install_probe(primary, "default")
    _install_probe(secondary, "alt")
    runner._handle_message = _runner_handler
    runner._wire_adapter_handlers(primary)
    runner._wire_adapter_handlers(secondary, preservation_profile="alt")
    primary_fixture = None
    secondary_fixture = None
    tasks = []
    try:
        primary_fixture = await WhatsAppQueueHttpFixture.start(primary, tmp_path, name="multiplex-primary")
        secondary_fixture = await WhatsAppQueueHttpFixture.start(secondary, tmp_path, name="multiplex-secondary")
        await primary_fixture.open()
        await secondary_fixture.open()
        assert await primary_fixture.foreign_capability_status(secondary_fixture) == 409

        primary_event = MessageEvent(
            text="primary", message_type=MessageType.TEXT, source=primary.build_source("primary-chat")
        )
        secondary_event = MessageEvent(
            text="secondary", message_type=MessageType.TEXT, source=secondary.build_source("secondary-chat")
        )
        await primary.handle_message(primary_event)
        await secondary.handle_message(secondary_event)
        tasks = [next(iter(primary._background_tasks)), next(iter(secondary._background_tasks))]
        await probes["default"]["background_started"].wait()
        await probes["alt"]["background_started"].wait()

        registry = runner._preservation_source_close
        primary_token = probes["default"]["admitted"][0]
        secondary_token = probes["alt"]["admitted"][0]
        assert primary_token.registration is not secondary_token.registration
        assert primary_token.profile is None
        assert secondary_token.profile == "alt"
        assert primary_token.producer_done is True
        assert secondary_token.producer_done is True
        assert primary_token.native_entered is False
        assert secondary_token.native_entered is False
        assert primary_token.native_exited is False
        assert secondary_token.native_exited is False
        assert set(registry.active_leases) == {primary_token, secondary_token}

        assert registry.begin_close(62) is True
        assert registry.owner_ready(62) is False

        probes["default"]["release_background"].set()
        await probes["default"]["native_started"].wait()
        assert primary_token.native_entered is True
        assert primary_token.native_exited is False
        assert secondary_token.native_entered is False
        assert secondary_token in registry.active_leases

        probes["default"]["release_native"].set()
        await tasks[0]
        assert primary_token.native_exited is True
        assert primary_token not in registry.active_leases
        assert secondary_token in registry.active_leases

        probes["alt"]["release_background"].set()
        await probes["alt"]["native_started"].wait()
        assert secondary_token.native_entered is True
        assert secondary_token.native_exited is False
        probes["alt"]["release_native"].set()
        await tasks[1]
        assert secondary_token.native_exited is True
        assert secondary_token not in registry.active_leases
        assert await registry.await_quiescence(62, 5) is True
        assert registry.owner_ready(62) is True
    finally:
        for probe in probes.values():
            probe["release_background"].set()
            probe["release_native"].set()
        pending_tasks = set(tasks)
        pending_tasks.update(getattr(primary, "_background_tasks", set()))
        pending_tasks.update(getattr(secondary, "_background_tasks", set()))
        if pending_tasks:
            await asyncio.gather(*pending_tasks, return_exceptions=True)
        try:
            if secondary_fixture is not None:
                await secondary_fixture.close()
        finally:
            if primary_fixture is not None:
                await primary_fixture.close()


def test_source_finalizer_receipt_precedes_transport_settlement_and_preserves_occurrence_identity(tmp_path):
    adapter = _ContractAdapter()
    runner, registry = _register_provider(adapter, "profile-a")
    event = _event()
    token = registry.admit_source(
        adapter, {"wire": "finalizer-positive"}, profile="profile-a", event=event,
        phase="whatsapp-wire",
    )
    settlement_calls = []

    def settle(current, custody_ref):
        settlement_calls.append((current.native_occurrence_id, custody_ref))
        return {"settled": True, "status": "success"}

    custody_ref = registry.attach_transport(
        token, {"status": "success", "request_id": "finalizer-positive"}, settle,
        "custody-finalizer-positive",
    )
    assert custody_ref == "custody-finalizer-positive"
    registry.mark_producer_done(token)
    with native_callback(runner, event, Path(tmp_path), source_token=token):
        assert token.native_occurrence_id is not None
    occurrence_id = token.native_occurrence_id
    assert token.finalizer_receipt is None
    assert settlement_calls == []
    assert token in registry.active_leases

    receipt = token.record_finalizer_receipt(status="success", reason="actual-finalizer")
    assert receipt.protocol == "hermes.whatsapp.source-finalizer.v1"
    assert receipt.native_occurrence_id == occurrence_id
    assert receipt.transport_receipt_digest == token.transport_receipt_digest
    assert settlement_calls == [(occurrence_id, custody_ref)]
    assert token not in registry.active_leases


def test_transport_failure_after_native_finalizer_retains_unknown_token(tmp_path):
    adapter = _ContractAdapter()
    runner, registry = _register_provider(adapter, "profile-a")
    event = _event()
    token = registry.admit_source(
        adapter, {"wire": "finalizer-failure"}, profile="profile-a", event=event,
        phase="whatsapp-wire",
    )
    custody_ref = registry.attach_transport(
        token, {"status": "success", "request_id": "finalizer-failure"},
        lambda *_args: False, "custody-finalizer-failure",
    )
    assert custody_ref == "custody-finalizer-failure"
    registry.mark_producer_done(token)
    with native_callback(runner, event, Path(tmp_path), source_token=token):
        pass
    token.record_finalizer_receipt(status="success", reason="actual-finalizer")
    assert token.state == "unknown"
    assert token in registry.active_leases

