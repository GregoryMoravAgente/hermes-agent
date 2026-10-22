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
    SourceTransportReceipt,
    register_source_adapter,
)
from gateway.platforms.base import BasePlatformAdapter
from gateway.platforms.event import MessageEvent, MessageType
from gateway.session_ingress_context import native_callback
from hermes_state_runtime import RuntimeStoreError
from tests.gateway.yuanbao_loopback import RecordingWebSocket as _RecordingWebSocket, yuanbao_module, yuanbao_proto


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


def _typed_yuanbao_receipt(token, *, source_marker=None, producer_generation="connect-1"):
    return SourceTransportReceipt(
        protocol="hermes.yuanbao.push-transport.v1",
        producer_kind="yuanbao-websocket-push",
        source_marker=token.marker if source_marker is None else source_marker,
        registration_generation=token.registration_generation,
        invocation_id=token.invocation_id,
        close_generation=token.close_generation,
        producer_generation=producer_generation,
        source_event_digest="event-digest",
        wire_identity_digest="wire-digest",
        handoff_digest="handoff-digest",
        ack_required=True,
        ack_sent=True,
        ack_digest="ack-digest",
        handoff=object(),
    )


def _yuanbao_auth_response_wire(yuanbao_module, connect_id, *, msg_id="synthetic-yuanbao-auth"):
    """Build a protocol-valid synthetic BIND_ACK for the real adapter decoder."""

    auth_data = yuanbao_proto._encode_parts([
        (1, "n", 0),
        (3, "s", connect_id),
    ])
    return yuanbao_proto.encode_conn_msg_full(
        cmd_type=yuanbao_module.CMD_TYPE["Response"],
        cmd="auth-bind",
        seq_no=1,
        msg_id=msg_id,
        module="conn_access",
        data=auth_data,
    )


def _assert_yuanbao_auth_bind_and_reset(websocket, decode_conn_msg):
    """Observe the actual AUTH_BIND request through the production decoder, then start ACK phase."""
    auth_frames = []
    for raw in websocket.sent:
        if not isinstance(raw, (bytes, bytearray)):
            continue
        decoded = decode_conn_msg(raw)
        if decoded.get("head", {}).get("cmd") == "auth-bind":
            auth_frames.append((raw, decoded))
    assert len(auth_frames) == 1
    raw, decoded = auth_frames[0]
    assert isinstance(raw, bytes)
    assert decoded["head"]["cmd_type"] == 0
    assert decoded["head"]["module"] == "conn_access"
    assert decoded["data"]
    websocket.sent.clear()
    return decoded


def _configure_started_yuanbao(runner):
    """Attach one authenticated Yuanbao adapter to an already started GatewayRunner."""
    from gateway.config import Platform, PlatformConfig
    from plugins.platforms.yuanbao.adapter import YuanbaoAdapter

    adapter = YuanbaoAdapter(PlatformConfig(
        enabled=True,
        extra={
            "app_id": "synthetic-yuanbao-app",
            "app_secret": "synthetic-yuanbao-secret",
            "ws_url": "wss://synthetic.invalid/ws",
            "api_domain": "https://synthetic.invalid",
            "dm_policy": "allowlist",
            "dm_allow_from": "synthetic-yuanbao-user",
            "group_policy": "allowlist",
            "group_allow_from": "synthetic-group",
            "bot_id": "synthetic-yuanbao-bot",
        },
    ))
    adapter.gateway_runner = runner
    # Mirror normal GatewayRunner startup: authz reads the live platform policy from
    # the canonical runner config while Yuanbao's own middleware enforces the same
    # exact synthetic identity.
    runner.config.platforms[Platform.YUANBAO] = adapter.config
    runner.adapters[Platform.YUANBAO] = adapter
    runner._wire_adapter_handlers(adapter)
    return adapter, runner._preservation_source_close


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
    callback_done = asyncio.Event()
    original_dispatch_done = adapter._source_dispatch_done

    def _observe_dispatch_done(event_arg, task):
        try:
            return original_dispatch_done(event_arg, task)
        finally:
            callback_done.set()

    adapter._source_dispatch_done = _observe_dispatch_done
    adapter._message_queue = _OneShotQueue(event)
    poll = asyncio.create_task(WecomCallbackAdapter._poll_loop(adapter))
    try:
        # Preserve the historical scheduling yield as a yield only; completion is established by
        # the source-owned dispatch event above, never by this sleep.
        await asyncio.sleep(0)
        await asyncio.wait_for(callback_done.wait(), timeout=30)
    finally:
        if not poll.done():
            poll.cancel()
        with suppress(asyncio.CancelledError):
            await poll
        # Preserve the second historical yield without using it as a producer witness.
        await asyncio.sleep(0)
    assert token.producer_done is True
    assert token in registry.active_leases
    assert registry.begin_close(103) is True
    with native_callback(runner, event, Path(tmp_path), source_token=token):
        pass
    assert registry.owner_ready(103) is True


@pytest.mark.asyncio
async def test_real_yuanbao_push_ack_and_debounce_keep_token_until_native_callback(
    tmp_path, monkeypatch, started_mux,
):
    import json
    import threading

    runner, _homes, _loop, call = started_mux
    adapter, registry = _configure_started_yuanbao(runner)
    admitted = []
    producer_events = {}
    finalizer_events = {}
    original_admit = adapter._preservation_source_admit

    def _record_admit(marker, **kwargs):
        token = original_admit(marker, **kwargs)
        if token is not None and token not in producer_events:
            admitted.append(token)
            producer_events[token] = threading.Event()
            finalizer_events[token] = threading.Event()
            original_finalizer = token.record_finalizer_or_unknown

            def _observe_finalizer(*args, _token=token, _original=original_finalizer, **kw):
                result = _original(*args, **kw)
                finalizer_events[_token].set()
                return result

            token.record_finalizer_or_unknown = _observe_finalizer
        return token

    adapter._preservation_source_admit = _record_admit
    original_done = adapter._preservation_source_producer_done

    def _observe_done(item):
        result = original_done(item)
        for token, event in tuple(producer_events.items()):
            if token.producer_done:
                event.set()
        return result

    adapter._preservation_source_producer_done = _observe_done
    connection = adapter._connection
    connection._ws = _RecordingWebSocket()
    dm_data = json.dumps({
        "from_account": "synthetic-yuanbao-user",
        "msg_id": "yuanbao-msg-1",
        "msg_body": [{"msg_type": "TIMTextElem", "msg_content": {"text": "hello"}}],
    }).encode()
    group_data = json.dumps({
        "from_account": "synthetic-yuanbao-user",
        "group_code": "synthetic-group",
        "msg_id": "yuanbao-group-1",
        "msg_body": [
            {
                "msg_type": "TIMCustomElem",
                "msg_content": {"data": json.dumps({
                    "elem_type": 1002, "user_id": "synthetic-yuanbao-bot",
                    "text": "@synthetic-yuanbao-bot",
                })},
            },
            {"msg_type": "TIMTextElem", "msg_content": {"text": "group request"}},
        ],
    }).encode()
    frames = {b"dm-wire": dm_data, b"group-wire": group_data}
    auth_connect_id = "synthetic-yuanbao-connection-1"
    auth_response = _yuanbao_auth_response_wire(yuanbao_module, auth_connect_id)
    connection._ws.auth_response = auth_response
    original_decode = yuanbao_module.decode_conn_msg

    def _decode(raw):
        if raw == auth_response or raw in connection._ws.response_frames:
            return original_decode(raw)
        return {
            "head": {
                "cmd_type": yuanbao_module.CMD_TYPE["Push"],
                "cmd": "push", "msg_id": "", "need_ack": True,
            },
            "data": frames[raw],
        }

    monkeypatch.setattr(yuanbao_module, "decode_conn_msg", _decode)
    monkeypatch.setattr(yuanbao_module, "encode_push_ack", lambda _head: b"ack")
    connection._ws.decode_outbound = original_decode

    async def _on_gateway(coro):
        return await asyncio.to_thread(call, coro)

    async def _flush(key):
        timer = connection._inbound_timers.pop(key)
        timer.cancel()
        before = set(adapter._background_tasks)
        before.update(adapter._inbound_tasks)
        connection._flush_inbound_buffer(key)
        created = {task for task in (set(adapter._background_tasks) | set(adapter._inbound_tasks)) if task not in before}
        if created:
            await asyncio.gather(*created, return_exceptions=True)

    async def _start_receive_loop():
        task = asyncio.create_task(connection._receive_loop(), name="yuanbao-test-recv")
        connection._recv_task = task
        return task

    receive_task = None
    async def _cleanup():
        nonlocal receive_task
        receive_task = connection._recv_task or receive_task
        if receive_task is not None and not receive_task.done():
            receive_task.cancel()
        if receive_task is not None:
            await asyncio.gather(receive_task, return_exceptions=True)
        connection._recv_task = None
        tasks = set(adapter._background_tasks) | set(adapter._inbound_tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    try:
        assert await _on_gateway(connection._authenticate({
            "bot_id": "synthetic-yuanbao-bot", "token": "synthetic-yuanbao-token",
        })) is True
        assert connection._connect_id == auth_connect_id
        receive_task = await _on_gateway(_start_receive_loop())
        auth_bind = _assert_yuanbao_auth_bind_and_reset(connection._ws, original_decode)
        assert auth_bind["head"]["cmd"] == "auth-bind"
        await _on_gateway(connection._handle_frame(b"dm-wire"))
        assert connection._ws.sent == [b"ack"]
        token = admitted[0]
        assert token.producer_done is False
        assert token in registry.active_leases
        key = next(iter(connection._inbound_buffer))
        await _on_gateway(_flush(key))
        # This yield is retained for the R4 scheduling oracle; the event below is the only
        # completion witness.
        await asyncio.sleep(0)
        assert await asyncio.to_thread(producer_events[token].wait, 30)
        assert token.event is not None
        assert token.producer_done is True
        assert token in registry.active_leases
        assert token.native_entered is True
        assert token.transport_receipt is not None
        assert isinstance(token.transport_receipt, SourceTransportReceipt)
        assert token.transport_receipt.native_occurrence_id == token.native_occurrence_id
        assert token.transport_receipt.protocol == "hermes.yuanbao.push-transport.v1"
        assert token.transport_receipt.source_marker is token.marker
        assert token.transport_receipt.producer_generation == connection._connect_id
        assert token.transport_receipt.handoff.connection_marker is connection._ws
        assert token.transport_receipt.custody_ref == token.custody_ref
        assert token.transport_receipt.ack_required is True
        assert token.transport_receipt.ack_sent is True
        assert await asyncio.to_thread(finalizer_events[token].wait, 30)
        assert token.finalizer_receipt is not None
        assert token.finalizer_receipt.status == "success"
        assert token.finalizer_receipt.protocol == "hermes.yuanbao.push-transport.v1"
        assert token.finalizer_receipt.native_occurrence_id == token.native_occurrence_id

        await _on_gateway(connection._handle_frame(b"group-wire"))
        group_token = admitted[1]
        group_key = next(iter(connection._inbound_buffer))
        await _on_gateway(_flush(group_key))
        await asyncio.sleep(0)
        assert await asyncio.to_thread(producer_events[group_token].wait, 30)
        assert group_token.event is not None
        assert group_token.producer_done is True
        assert group_token in registry.active_leases
        assert group_token.native_entered is True
        assert group_token.transport_receipt is not None
        assert isinstance(group_token.transport_receipt, SourceTransportReceipt)
        assert group_token.transport_receipt.source_marker is group_token.marker
        assert group_token.transport_receipt.producer_generation == connection._connect_id
        assert await asyncio.to_thread(finalizer_events[group_token].wait, 30)
        assert group_token.finalizer_receipt is not None
        assert group_token.finalizer_receipt.status == "success"
        assert group_token.finalizer_receipt.protocol == "hermes.yuanbao.push-transport.v1"

        # The real Gateway handler has already entered/exited the native callback and the real
        # background finalizer has published both receipts.  Close/quiesce now observes the
        # source-owned settlement; no test-created callback context or registry reconciliation is
        # allowed to manufacture release.
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        async def _close_and_check_source_on_gateway():
            assert registry.begin_close(104) is True
            assert await registry.await_quiescence(104, 30) is True
            assert registry.owner_ready(104) is True

        await _on_gateway(_close_and_check_source_on_gateway())
        assert token not in registry.active_leases
        assert group_token not in registry.active_leases
    finally:
        await _on_gateway(_cleanup())


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


def test_typed_yuanbao_receipt_rejects_copied_foreign_and_stale_identity(tmp_path):
    adapter = _ContractAdapter()
    _runner, registry = _register_provider(adapter, "profile-a")

    copied_marker_token = registry.admit_source(
        adapter, {"wire": "copied-marker"}, profile="profile-a", event=_event(),
        phase="yuanbao-push",
    )
    copied_receipt = _typed_yuanbao_receipt(
        copied_marker_token, source_marker=dict(copied_marker_token.marker),
    )
    assert registry.attach_transport(
        copied_marker_token, copied_receipt, lambda *_args: True,
    ) is None
    assert copied_marker_token.state == "unknown"
    assert copied_marker_token in registry.active_leases

    source_token = registry.admit_source(
        adapter, {"wire": "foreign-source"}, profile="profile-a", event=_event(),
        phase="yuanbao-push",
    )
    target_token = registry.admit_source(
        adapter, {"wire": "foreign-target"}, profile="profile-a", event=_event(),
        phase="yuanbao-push",
    )
    foreign_receipt = _typed_yuanbao_receipt(source_token)
    assert registry.attach_transport(target_token, foreign_receipt, lambda *_args: True) is None
    assert target_token.state == "unknown"
    assert target_token in registry.active_leases

    stale_token = registry.admit_source(
        adapter, {"wire": "stale-generation"}, profile="profile-a", event=_event(),
        phase="yuanbao-push",
    )
    stale_receipt = _typed_yuanbao_receipt(stale_token, producer_generation="")
    assert registry.attach_transport(stale_token, stale_receipt, lambda *_args: True) is None
    assert stale_token.state == "unknown"
    assert stale_token in registry.active_leases


def test_typed_yuanbao_receipt_keeps_occurrence_until_exact_settlement(tmp_path):
    adapter = _ContractAdapter()
    runner, registry = _register_provider(adapter, "profile-a")
    event = _event()
    token = registry.admit_source(
        adapter, {"wire": "typed-positive"}, profile="profile-a", event=event,
        phase="yuanbao-push",
    )
    settlement_calls = []

    def settle(current, custody_ref):
        settlement_calls.append((current, custody_ref))
        return {"settled": True, "status": "success"}

    receipt = _typed_yuanbao_receipt(token)
    custody_ref = registry.attach_transport(
        token, receipt, settle, "custody-typed-positive",
    )
    assert custody_ref == "custody-typed-positive"
    registry.mark_producer_done(token)
    with native_callback(runner, event, Path(tmp_path), source_token=token):
        pass
    occurrence_id = token.native_occurrence_id
    assert occurrence_id is not None
    assert token.finalizer_receipt is None
    assert token in registry.active_leases

    finalizer = token.record_finalizer_receipt(status="success", reason="typed-finalizer")
    assert finalizer.protocol == "hermes.yuanbao.push-transport.v1"
    assert finalizer.producer_kind == "yuanbao-websocket-push"
    assert finalizer.native_occurrence_id == occurrence_id
    assert finalizer.source_event_digest == receipt.source_event_digest
    assert finalizer.handoff_digest == receipt.handoff_digest
    assert settlement_calls == [(token, custody_ref)]
    assert token not in registry.active_leases


def test_typed_yuanbao_receipt_tracks_close_generation_without_rehome(tmp_path):
    adapter = _ContractAdapter()
    _runner, registry = _register_provider(adapter, "profile-a")
    token = registry.admit_source(
        adapter, {"wire": "typed-close-generation"}, profile="profile-a", event=_event(),
        phase="yuanbao-push",
    )
    receipt = _typed_yuanbao_receipt(token)
    registry.attach_transport(token, receipt, lambda *_args: True)
    assert token.transport_receipt.close_generation is None
    assert registry.begin_close(104) is True
    assert token.transport_receipt.close_generation == 104
    assert token.transport_receipt.matches_token(token)
    assert registry.abort(104) is True
    assert token.transport_receipt.close_generation is None
    assert token.transport_receipt.matches_token(token)


def test_typed_yuanbao_finalizer_failure_retains_same_unknown_occurrence(tmp_path):
    adapter = _ContractAdapter()
    runner, registry = _register_provider(adapter, "profile-a")
    event = _event()
    token = registry.admit_source(
        adapter, {"wire": "typed-finalizer-failure"}, profile="profile-a", event=event,
        phase="yuanbao-push",
    )
    registry.attach_transport(token, _typed_yuanbao_receipt(token), lambda *_args: True)
    registry.mark_producer_done(token)
    with native_callback(runner, event, Path(tmp_path), source_token=token):
        pass
    occurrence_id = token.native_occurrence_id
    token.record_finalizer_receipt(status="failed", reason="typed-finalizer-failure")
    assert token.native_occurrence_id == occurrence_id
    assert token.state == "unknown"
    assert token in registry.active_leases


def test_typed_yuanbao_settlement_readback_failure_retains_same_unknown_occurrence(tmp_path):
    adapter = _ContractAdapter()
    runner, registry = _register_provider(adapter, "profile-a")
    event = _event()
    token = registry.admit_source(
        adapter, {"wire": "typed-settlement-failure"}, profile="profile-a", event=event,
        phase="yuanbao-push",
    )
    registry.attach_transport(
        token, _typed_yuanbao_receipt(token), lambda *_args: {"settled": False, "status": "unknown"},
    )
    registry.mark_producer_done(token)
    with native_callback(runner, event, Path(tmp_path), source_token=token):
        pass
    occurrence_id = token.native_occurrence_id
    token.record_finalizer_receipt(status="success", reason="typed-settlement-failure")
    assert token.native_occurrence_id == occurrence_id
    assert token.state == "unknown"
    assert token in registry.active_leases




@asynccontextmanager
async def _canonical_provider_runtime(tmp_path, monkeypatch, adapter):
    """Use the existing managed runtime bootstrap, then wire one real provider adapter."""
    from tests.gateway.conftest import runtime_owned_api

    async with runtime_owned_api(tmp_path, monkeypatch, cleanup_readback=True) as api_adapter:
        runner = api_adapter.gateway_runner
        runner.adapters[api_adapter.platform] = api_adapter
        adapter.gateway_runner = runner
        runner.adapters[adapter.platform] = adapter
        runner._wire_adapter_handlers(adapter)
        try:
            yield runner, runner.session_authority, runner._preservation_source_close
        finally:
            if getattr(adapter, "_running", False):
                await adapter.disconnect()


def _finish_canonical_tokens(runner, registry, tokens, tmp_path):
    """Drive exact registered tokens through the native finalizer boundary."""
    tokens = tuple(tokens)
    assert tokens
    for token in tokens:
        assert token.event is not None
        with native_callback(runner, token.event, Path(tmp_path), source_token=token):
            pass
        assert token not in registry.active_leases


async def _await_provider_tasks(adapter):
    """Await tasks already created by the provider's real dispatch path."""
    tasks = set(getattr(adapter, "_inbound_tasks", ()))
    tasks.update(getattr(adapter, "_background_tasks", ()))
    pending = tuple(task for task in tasks if not task.done())
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


@pytest.mark.asyncio
async def test_canonical_gateway_wecom_queue_failure_retains_exact_unknown(tmp_path, monkeypatch):
    """The authenticated WeCom callback retains a token when its real queue handoff fails."""
    from xml.etree import ElementTree as ET

    from gateway.config import PlatformConfig
    from plugins.platforms.wecom.callback_adapter import WecomCallbackAdapter

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    app = {
        "name": "source-contract-app",
        "corp_id": "ww-source-contract",
        "corp_secret": "synthetic-secret",
        "agent_id": "1000002",
        "token": "source-contract-token",
        "encoding_aes_key": "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG",
    }
    adapter = WecomCallbackAdapter(PlatformConfig(
        enabled=True,
        extra={"mode": "callback", "host": "127.0.0.1", "port": 0, "apps": [app]},
    ))

    async with _canonical_provider_runtime(tmp_path, monkeypatch, adapter) as (_runner, _authority, registry):
        crypt = adapter._crypt_for_app(app)

        class _Request:
            def __init__(self, body, query):
                self._body = body
                self.query = query

            async def read(self):
                return self._body.encode("utf-8") if isinstance(self._body, str) else self._body

        def _request(message_id: str, nonce: str):
            encrypted = crypt.encrypt(
                "<xml><ToUserName>ww-source-contract</ToUserName><FromUserName>alice</FromUserName>"
                "<CreateTime>1710000000</CreateTime><MsgType>text</MsgType><Content>hello</Content>"
                f"<MsgId>{message_id}</MsgId></xml>",
                nonce=nonce, timestamp="1710000000",
            )
            root = ET.fromstring(encrypted)
            query = {
                "msg_signature": root.findtext("MsgSignature", default=""),
                "timestamp": root.findtext("TimeStamp", default="1710000000"),
                "nonce": root.findtext("Nonce", default=nonce),
            }
            return _Request(encrypted, query)

        accepted = await adapter._handle_callback(_request("source-msg-1", "source-nonce"))
        assert accepted.status == 200
        accepted_event = adapter._message_queue.get_nowait()
        accepted_token = registry.tokens_for_event(accepted_event)[0]
        assert accepted_token.producer_done is True
        _finish_canonical_tokens(_runner, registry, (accepted_token,), tmp_path)

        captured = []

        async def _fail_queue(event):
            captured.append(event)
            raise RuntimeError("canonical queue handoff failed")

        monkeypatch.setattr(adapter._message_queue, "put", _fail_queue)
        refused = await adapter._handle_callback(_request("source-msg-2", "source-nonce-2"))
        assert refused.status == 400
        assert len(captured) == 1
        failed_token = registry.tokens_for_event(captured[0])[0]
        assert failed_token.state == "unknown"
        assert failed_token in registry.active_leases


@pytest.mark.asyncio
async def test_canonical_gateway_yuanbao_push_task_failure_retains_exact_unknown(tmp_path, monkeypatch):
    """The real Yuanbao push is admitted before debounce and UNKNOWN follows pipeline failure."""
    import plugins.platforms.yuanbao.adapter as yuanbao_module
    from gateway.config import PlatformConfig
    from plugins.platforms.yuanbao.adapter import YuanbaoAdapter

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))

    class _WebSocket:
        def __init__(self):
            self.sent = []
            self.auth_response = None

        async def send(self, value):
            self.sent.append(value)

        async def recv(self):
            return self.auth_response

    adapter = YuanbaoAdapter(PlatformConfig(
        enabled=True, extra={"app_id": "source-app", "app_secret": "source-secret"},
    ))
    async with _canonical_provider_runtime(tmp_path, monkeypatch, adapter) as (_runner, _authority, registry):
        connection = adapter._connection
        websocket = _WebSocket()
        connection._ws = websocket
        connect_id = "synthetic-yuanbao-push-task"
        auth_response = _yuanbao_auth_response_wire(yuanbao_module, connect_id)
        websocket.auth_response = auth_response
        original_decode = yuanbao_module.decode_conn_msg

        def _decode(raw):
            if raw == auth_response:
                return original_decode(raw)
            return {
                "head": {
                    "cmd_type": yuanbao_module.CMD_TYPE["Push"], "cmd": "push",
                    "msg_id": "", "need_ack": True,
                },
                "data": b"canonical-yuanbao-push",
            }

        monkeypatch.setattr(yuanbao_module, "decode_conn_msg", _decode)
        monkeypatch.setattr(yuanbao_module, "encode_push_ack", lambda _head: b"ack")
        assert await connection._authenticate({
            "bot_id": "synthetic-yuanbao-bot", "token": "synthetic-yuanbao-token",
        }) is True
        assert connection._connect_id == connect_id
        auth_bind = _assert_yuanbao_auth_bind_and_reset(websocket, original_decode)
        assert auth_bind["head"]["cmd"] == "auth-bind"
        try:
            await connection._handle_frame(b"source-push")
            assert connection._ws.sent == [b"ack"]
            key = next(iter(connection._inbound_buffer))
            entries = connection._inbound_buffer[key]
            assert len(entries) == 1
            token = entries[0][1]
            assert token is not None
            assert token.event is None
            assert token.producer_done is False
            assert token.state != "unknown"

            timer = connection._inbound_timers.pop(key)
            timer.cancel()
            before = set(adapter._background_tasks)
            connection._flush_inbound_buffer(key)
            pipeline_tasks = tuple(set(adapter._background_tasks) - before)
            assert len(pipeline_tasks) == 1
            await asyncio.gather(*pipeline_tasks, return_exceptions=True)
            assert token.state == "unknown"
            assert token in registry.active_leases
        finally:
            for timer in connection._inbound_timers.values():
                timer.cancel()
            for task in tuple(adapter._background_tasks):
                if not task.done():
                    task.cancel()
            if adapter._background_tasks:
                await asyncio.gather(*tuple(adapter._background_tasks), return_exceptions=True)


@pytest.mark.asyncio
async def test_canonical_gateway_yuanbao_direct_and_group_handoffs_finish_real_tokens(tmp_path, monkeypatch):
    """The actual Yuanbao dispatch middleware binds and completes both scheduled source lanes."""
    from gateway.config import PlatformConfig
    from gateway.platforms.event import MessageType
    from plugins.platforms.yuanbao.adapter import DispatchMiddleware, InboundContext, YuanbaoAdapter

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    adapter = YuanbaoAdapter(PlatformConfig(
        enabled=True,
        extra={
            "app_id": "source-app",
            "app_secret": "source-secret",
            "dm_policy": "allowlist",
            "dm_allow_from": "yuanbao-user",
            "group_policy": "allowlist",
            "group_allow_from": "yuanbao-group",
        },
    ))
    async with _canonical_provider_runtime(tmp_path, monkeypatch, adapter) as (_runner, _authority, registry):
        direct_source = adapter.build_source(chat_id="yuanbao-direct", chat_type="dm", user_id="yuanbao-user")
        group_source = adapter.build_source(chat_id="yuanbao-group", chat_type="group", user_id="yuanbao-user")
        direct_token = adapter._preservation_source_admit({"lane": "direct"}, phase="yuanbao-direct")
        group_token = adapter._preservation_source_admit({"lane": "group"}, phase="yuanbao-group")
        assert direct_token is not None
        assert group_token is not None
        direct_context = InboundContext(
            adapter=adapter, source=direct_source, chat_type="dm", raw_text="direct",
            msg_type=MessageType.TEXT, msg_id="yuanbao-direct", source_tokens=[direct_token],
        )
        group_context = InboundContext(
            adapter=adapter, source=group_source, chat_type="group", raw_text="group",
            msg_type=MessageType.TEXT, msg_id="yuanbao-group", source_tokens=[group_token],
        )
        try:
            middleware = DispatchMiddleware()
            await middleware.handle(direct_context, lambda: asyncio.sleep(0))
            await middleware.handle(group_context, lambda: asyncio.sleep(0))
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            await _await_provider_tasks(adapter)
            assert direct_context._preservation_source_dispatch_scheduled is True
            assert group_context._preservation_source_dispatch_scheduled is True
            assert direct_token.event is not None
            assert group_token.event is not None
            assert direct_token.producer_done is True
            assert group_token.producer_done is True
            assert direct_token.state != "unknown"
            assert group_token.state != "unknown"
            # DispatchMiddleware reaches the runner's installed message handler, whose real
            # native wrapper owns the occurrence.  The test-created native_callback used by R9
            # concealed source identity mismatches and is intentionally absent here.
            assert direct_token.native_entered is True
            assert group_token.native_entered is True
            assert direct_token.native_exited is True
            assert group_token.native_exited is True
            assert direct_token not in registry.active_leases
            assert group_token not in registry.active_leases
        finally:
            for task in tuple(adapter._inbound_tasks):
                if not task.done():
                    task.cancel()
            if adapter._inbound_tasks:
                await asyncio.gather(*tuple(adapter._inbound_tasks), return_exceptions=True)


@pytest.mark.asyncio
async def test_canonical_gateway_a2a_owner_future_failure_retains_exact_unknown(tmp_path, monkeypatch):
    """A real A2A admission resolves its pending waiter when the owner-loop dispatch fails."""
    from gateway.config import PlatformConfig
    from gateway.preservation_source_close import register_source_adapter
    from plugins.platforms.a2a.adapter import A2AAdapter, protocol

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    adapter = A2AAdapter(PlatformConfig(enabled=True, extra={"port": 0}))
    async with _canonical_provider_runtime(tmp_path, monkeypatch, adapter) as (_runner, _authority, registry):
        adapter._loop = asyncio.get_running_loop()

        async def _fail_owner_dispatch(_event, **_kwargs):
            raise RuntimeError("canonical owner dispatch failed")

        # This is a fault at the public adapter handoff.  Re-register the actual current
        # handler through the production source gate; do not bypass registration identity.
        monkeypatch.setattr(adapter, "handle_message", _fail_owner_dispatch)
        register_source_adapter(_runner, adapter)
        # A2A's public admission runs in a ThreadingHTTPServer worker.  Enter that actual
        # worker boundary so run_coroutine_threadsafe() and its owner-loop callback are exercised.
        terminal, pending = await asyncio.to_thread(
            adapter._prepare_task,
            {"message": protocol.text_message(protocol.ROLE_USER, "hello", context_id="ctx-source")},
            "peer-source",
        )
        assert terminal is None
        assert pending is not None
        owner_future = pending["owner_future"]
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        # Consume the actual concurrent owner future instead of assuming two loop turns imply
        # completion.  No extra timer or retry is introduced; the future is already the source
        # of truth for the worker-to-owner handoff.
        try:
            await asyncio.wrap_future(owner_future)
        except RuntimeError:
            pass
        assert owner_future.done() is True
        assert isinstance(owner_future.exception(), RuntimeError)
        state, reply = adapter._await_reply(pending)
        assert state == protocol.STATE_FAILED
        assert reply == "[agent processing failed]"
        adapter._finalize_task(pending, state, reply)
        task_record = adapter.tasks.get(pending["task_id"], "", "")
        assert task_record is not None
        assert task_record["state"] == protocol.STATE_FAILED
        token = next(iter(registry.active_leases))
        assert token.event is not None
        assert token.state == "unknown"
        assert token in registry.active_leases


@pytest.mark.asyncio
async def test_canonical_gateway_yuanbao_push_ack_failure_retains_exact_unknown(tmp_path, monkeypatch, started_mux):
    """A real admitted push remains UNKNOWN when the provider ACK send fails."""
    import json
    import plugins.platforms.yuanbao.adapter as yuanbao_module

    runner, _homes, _loop, call = started_mux
    adapter, registry = _configure_started_yuanbao(runner)
    admitted = []
    original_admit = adapter._preservation_source_admit

    def _record_admit(marker, **kwargs):
        token = original_admit(marker, **kwargs)
        if token is not None:
            admitted.append(token)
        return token

    adapter._preservation_source_admit = _record_admit

    class _FailingWebSocket:
        def __init__(self):
            self.sent = []
            self.auth_response = None
            self.fail_send = False

        async def send(self, value):
            if self.fail_send:
                raise OSError("synthetic PushAck failure")
            self.sent.append(value)

        async def recv(self):
            return self.auth_response

    websocket = _FailingWebSocket()
    adapter._connection._ws = websocket
    data = json.dumps({
        "from_account": "synthetic-yuanbao-user",
        "msg_id": "yuanbao-ack-failure",
        "msg_body": [{"msg_type": "TIMTextElem", "msg_content": {"text": "ack failure"}}],
    }).encode()
    connect_id = "synthetic-yuanbao-ack-failure"
    auth_response = _yuanbao_auth_response_wire(yuanbao_module, connect_id)
    websocket.auth_response = auth_response
    original_decode = yuanbao_module.decode_conn_msg

    def _decode(raw):
        if raw == auth_response:
            return original_decode(raw)
        return {
            "head": {
                "cmd_type": yuanbao_module.CMD_TYPE["Push"], "cmd": "push",
                "msg_id": "", "need_ack": True,
            },
            "data": data,
        }

    monkeypatch.setattr(yuanbao_module, "decode_conn_msg", _decode)
    monkeypatch.setattr(yuanbao_module, "encode_push_ack", lambda _head: b"ack")

    async def _on_gateway(coro):
        return await asyncio.to_thread(call, coro)

    async def _shutdown():
        adapter._preservation_source_shutdown()

    async def _cleanup():
        adapter._preservation_source_shutdown()
        tasks = set(adapter._background_tasks) | set(adapter._inbound_tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    assert await _on_gateway(adapter._connection._authenticate({
        "bot_id": "synthetic-yuanbao-bot", "token": "synthetic-yuanbao-token",
    })) is True
    assert adapter._connection._connect_id == connect_id
    auth_bind = _assert_yuanbao_auth_bind_and_reset(websocket, original_decode)
    assert auth_bind["head"]["cmd"] == "auth-bind"
    websocket.fail_send = True

    try:
        await _on_gateway(adapter._connection._handle_frame(b"ack-failure-wire"))
        token = admitted[0]
        assert token.state == "unknown"
        assert registry._unknown_sources.get(token) == "yuanbao_push_ack_failed"
        assert token.producer_done is False
        assert token in registry.active_leases
        assert await _on_gateway(_shutdown()) is None
        assert token.state == "unknown"
        assert token in registry.active_leases
    finally:
        await _on_gateway(_cleanup())


@pytest.mark.asyncio
async def test_canonical_gateway_yuanbao_pending_shutdown_retains_exact_unknown(
    tmp_path, monkeypatch, started_mux,
):
    """Shutdown retains UNKNOWN custody for an admitted token blocked before debounce ownership."""
    import json
    import threading
    import plugins.platforms.yuanbao.adapter as yuanbao_module

    runner, _homes, _loop, call = started_mux
    adapter, registry = _configure_started_yuanbao(runner)
    admitted = []
    original_admit = adapter._preservation_source_admit

    def _record_admit(marker, **kwargs):
        token = original_admit(marker, **kwargs)
        if token is not None:
            admitted.append(token)
        return token

    adapter._preservation_source_admit = _record_admit

    class _BlockingWebSocket:
        def __init__(self):
            self.entered = threading.Event()
            self.release = threading.Event()
            self.sent = []
            self.auth_response = None
            self.block_send = False

        async def send(self, value):
            if not self.block_send:
                self.sent.append(value)
                return
            self.entered.set()
            await asyncio.to_thread(self.release.wait, 30)

        async def recv(self):
            return self.auth_response

    websocket = _BlockingWebSocket()
    adapter._connection._ws = websocket
    data = json.dumps({
        "from_account": "synthetic-yuanbao-user",
        "msg_id": "yuanbao-pending-shutdown",
        "msg_body": [{"msg_type": "TIMTextElem", "msg_content": {"text": "shutdown"}}],
    }).encode()
    connect_id = "synthetic-yuanbao-pending-shutdown"
    auth_response = _yuanbao_auth_response_wire(yuanbao_module, connect_id)
    websocket.auth_response = auth_response
    original_decode = yuanbao_module.decode_conn_msg

    def _decode(raw):
        if raw == auth_response:
            return original_decode(raw)
        return {
            "head": {
                "cmd_type": yuanbao_module.CMD_TYPE["Push"], "cmd": "push",
                "msg_id": "", "need_ack": True,
            },
            "data": data,
        }

    monkeypatch.setattr(yuanbao_module, "decode_conn_msg", _decode)
    monkeypatch.setattr(yuanbao_module, "encode_push_ack", lambda _head: b"ack")
    holder = {}

    async def _on_gateway(coro):
        return await asyncio.to_thread(call, coro)

    async def _shutdown():
        adapter._preservation_source_shutdown()

    async def _launch():
        holder["task"] = asyncio.create_task(adapter._connection._handle_frame(b"pending-shutdown-wire"))

    async def _finish():
        await holder["task"]

    async def _cleanup():
        websocket.release.set()
        task = holder.get("task")
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        adapter._preservation_source_shutdown()

    assert await _on_gateway(adapter._connection._authenticate({
        "bot_id": "synthetic-yuanbao-bot", "token": "synthetic-yuanbao-token",
    })) is True
    assert adapter._connection._connect_id == connect_id
    auth_bind = _assert_yuanbao_auth_bind_and_reset(websocket, original_decode)
    assert auth_bind["head"]["cmd"] == "auth-bind"
    websocket.block_send = True

    try:
        await _on_gateway(_launch())
        assert await asyncio.to_thread(websocket.entered.wait, 30)
        token = admitted[0]
        assert token in adapter._preservation_source_pending_tokens
        await _on_gateway(_shutdown())
        assert token.state == "unknown"
        assert registry._unknown_sources.get(token) == "yuanbao_pending_shutdown"
        assert token in registry.active_leases
        websocket.release.set()
        await _on_gateway(_finish())
        assert token.state == "unknown"
        assert token in registry.active_leases
    finally:
        await _on_gateway(_cleanup())


@pytest.mark.asyncio
async def test_canonical_gateway_yuanbao_ack_cancellation_retains_exact_unknown(
    tmp_path, monkeypatch, started_mux,
):
    """Cancellation while the real PushAck is pending retains the exact admitted token."""
    import json
    import threading
    import plugins.platforms.yuanbao.adapter as yuanbao_module

    runner, _homes, _loop, call = started_mux
    adapter, registry = _configure_started_yuanbao(runner)
    admitted = []
    original_admit = adapter._preservation_source_admit

    def _record_admit(marker, **kwargs):
        token = original_admit(marker, **kwargs)
        if token is not None:
            admitted.append(token)
        return token

    adapter._preservation_source_admit = _record_admit

    class _BlockingWebSocket:
        def __init__(self):
            self.entered = threading.Event()
            self.release = threading.Event()

        async def send(self, _value):
            self.entered.set()
            await asyncio.to_thread(self.release.wait, 30)

    websocket = _BlockingWebSocket()
    adapter._connection._ws = websocket
    data = json.dumps({
        "from_account": "synthetic-yuanbao-user",
        "msg_id": "yuanbao-ack-cancel",
        "msg_body": [{"msg_type": "TIMTextElem", "msg_content": {"text": "cancel"}}],
    }).encode()
    monkeypatch.setattr(
        yuanbao_module,
        "decode_conn_msg",
        lambda _raw: {
            "head": {
                "cmd_type": yuanbao_module.CMD_TYPE["Push"], "cmd": "push",
                "msg_id": "", "need_ack": True,
            },
            "data": data,
        },
    )
    monkeypatch.setattr(yuanbao_module, "encode_push_ack", lambda _head: b"ack")
    holder = {}

    async def _on_gateway(coro):
        return await asyncio.to_thread(call, coro)

    async def _launch():
        holder["task"] = asyncio.create_task(adapter._connection._handle_frame(b"ack-cancel-wire"))

    async def _cancel():
        task = holder["task"]
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task

    async def _cleanup():
        websocket.release.set()
        task = holder.get("task")
        if task is not None and not task.done():
            task.cancel()
        if task is not None:
            await asyncio.gather(task, return_exceptions=True)
        adapter._preservation_source_shutdown()

    try:
        await _on_gateway(_launch())
        assert await asyncio.to_thread(websocket.entered.wait, 30)
        token = admitted[0]
        assert token in adapter._preservation_source_pending_tokens
        await _on_gateway(_cancel())
        websocket.release.set()
        assert token.state == "unknown"
        assert registry._unknown_sources.get(token) == "yuanbao_push_ack_cancelled"
        assert token in registry.active_leases
    finally:
        await _on_gateway(_cleanup())


@pytest.mark.asyncio
async def test_canonical_gateway_telegram_flush_failure_retains_exact_unknown(tmp_path, monkeypatch):
    """A real Telegram photo batch retains the token when the post-pop handler fails."""
    from gateway.config import PlatformConfig
    from plugins.platforms.telegram.adapter import TelegramAdapter

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    adapter = TelegramAdapter(PlatformConfig(enabled=True, token="source-token"))
    async with _canonical_provider_runtime(tmp_path, monkeypatch, adapter) as (_runner, _authority, registry):
        adapter._media_batch_delay_seconds = 0
        event = MessageEvent(
            text="photo", message_type=MessageType.PHOTO,
            source=adapter.build_source(chat_id="telegram-source", user_id="telegram-user", chat_type="dm"),
            media_urls=[str(tmp_path / "source-photo.jpg")], media_types=["image/jpeg"],
        )

        async def _fail_handler(_event, **_kwargs):
            raise RuntimeError("canonical Telegram handler failed")

        monkeypatch.setattr(adapter, "handle_message", _fail_handler)
        from gateway.preservation_source_close import register_source_adapter
        register_source_adapter(_runner, adapter)
        try:
            adapter._enqueue_photo_event("source-photo-batch", event)
            task = adapter._pending_photo_batch_tasks["source-photo-batch"]
            with pytest.raises(RuntimeError, match="canonical Telegram handler failed"):
                await task
            token = registry.tokens_for_event(event)[0]
            assert token.state == "unknown"
            assert token in registry.active_leases
        finally:
            for task in tuple(adapter._pending_photo_batch_tasks.values()):
                if not task.done():
                    task.cancel()


@pytest.mark.asyncio
async def test_canonical_gateway_feishu_batch_failure_retains_exact_unknown(tmp_path, monkeypatch):
    """A real Feishu media batch retains its source token when setup/flush fails."""
    from gateway.config import PlatformConfig
    from plugins.platforms.feishu.adapter import FeishuAdapter

    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "hermes"))
    adapter = FeishuAdapter(PlatformConfig(enabled=True))
    async with _canonical_provider_runtime(tmp_path, monkeypatch, adapter) as (_runner, _authority, registry):
        adapter._media_batch_delay_seconds = 0
        event = MessageEvent(
            text="media", message_type=MessageType.PHOTO,
            source=adapter.build_source(chat_id="feishu-source", user_id="feishu-user", chat_type="dm"),
            media_urls=[str(tmp_path / "source-media.jpg")], media_types=["image/jpeg"],
        )

        async def _fail_guarded_handler(_event):
            raise RuntimeError("canonical Feishu handler failed")

        monkeypatch.setattr(adapter, "_handle_message_with_guards", _fail_guarded_handler)
        try:
            await adapter._enqueue_media_event(event)
            key = next(iter(adapter._pending_media_batch_tasks))
            task = adapter._pending_media_batch_tasks[key]
            with pytest.raises(RuntimeError, match="canonical Feishu handler failed"):
                await task
            token = registry.tokens_for_event(event)[0]
            assert token.state == "unknown"
            assert token in registry.active_leases
        finally:
            for task in tuple(adapter._pending_media_batch_tasks.values()):
                if not task.done():
                    task.cancel()


async def _run_yuanbao_auth_bound_push(adapter, runner_call, monkeypatch, admitted, *, suffix):
    """Drive AUTH_BIND, one real PushAck, debounce insertion, and one real pipeline task."""
    import json
    connection = adapter._connection
    websocket = _RecordingWebSocket()
    connection._ws = websocket
    data = json.dumps({
        "from_account": "synthetic-yuanbao-user",
        "msg_id": f"yuanbao-r12-{suffix}",
        "msg_body": [{"msg_type": "TIMTextElem", "msg_content": {"text": "r12"}}],
    }).encode()
    connect_id = f"synthetic-yuanbao-r12-{suffix}"
    auth_response = _yuanbao_auth_response_wire(
        yuanbao_module, connect_id, msg_id=f"synthetic-yuanbao-auth-{suffix}",
    )
    websocket.auth_response = auth_response
    original_decode = yuanbao_module.decode_conn_msg

    def _decode(raw):
        if raw == auth_response or raw in websocket.response_frames:
            return original_decode(raw)
        return {
            "head": {
                "cmd_type": yuanbao_module.CMD_TYPE["Push"],
                "cmd": "push", "msg_id": "", "need_ack": True,
            },
            "data": data,
        }

    monkeypatch.setattr(yuanbao_module, "decode_conn_msg", _decode)
    monkeypatch.setattr(yuanbao_module, "encode_push_ack", lambda _head: b"r12-ack")
    websocket.decode_outbound = original_decode

    async def _on_gateway(coro):
        return await asyncio.to_thread(runner_call, coro)

    async def _start_receive_loop():
        task = asyncio.create_task(connection._receive_loop(), name="yuanbao-test-recv")
        connection._recv_task = task
        return task

    assert await _on_gateway(connection._authenticate({
        "bot_id": "synthetic-yuanbao-bot", "token": "synthetic-yuanbao-token",
    })) is True
    assert connection._connect_id == connect_id
    receive_task = await _on_gateway(_start_receive_loop())
    auth_bind = _assert_yuanbao_auth_bind_and_reset(websocket, original_decode)
    assert auth_bind["head"]["cmd"] == "auth-bind"
    await _on_gateway(connection._handle_frame(b"yuanbao-r12-push-wire"))
    assert connection._ws.sent == [b"r12-ack"]

    async def _flush_and_join():
        key = next(iter(connection._inbound_buffer))
        timer = connection._inbound_timers.pop(key)
        timer.cancel()
        before = set(adapter._background_tasks) | set(adapter._inbound_tasks)
        connection._flush_inbound_buffer(key)
        created = tuple(
            task for task in (set(adapter._background_tasks) | set(adapter._inbound_tasks))
            if task not in before
        )
        assert len(created) == 1
        await asyncio.gather(*created, return_exceptions=True)
        await _await_provider_tasks(adapter)
        return created

    created = await _on_gateway(_flush_and_join())
    assert admitted

    async def _cleanup():
        if receive_task is not None and not receive_task.done():
            receive_task.cancel()
        if receive_task is not None:
            await asyncio.gather(receive_task, return_exceptions=True)
        connection._recv_task = None
        for timer in connection._inbound_timers.values():
            timer.cancel()
        adapter._preservation_source_shutdown()
        tasks = set(adapter._background_tasks) | set(adapter._inbound_tasks)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    return connection, _on_gateway, created, _cleanup


@pytest.mark.asyncio
async def test_canonical_gateway_yuanbao_finalizer_failure_retains_auth_bound_unknown(
    tmp_path, monkeypatch, started_mux,
):
    """The real AUTH_BIND/push/native route retains one occurrence on finalizer failure."""
    import threading

    runner, _homes, _loop, call = started_mux
    adapter, registry = _configure_started_yuanbao(runner)
    admitted = []
    finalizer_done = threading.Event()
    original_admit = adapter._preservation_source_admit

    def _record_admit(marker, **kwargs):
        token = original_admit(marker, **kwargs)
        if token is not None:
            admitted.append(token)
            original_finalizer = token.record_finalizer_or_unknown

            def _failed_finalizer(*_args, _original=original_finalizer, **_kwargs):
                result = _original(
                    status="failed",
                    reason="yuanbao_r12_finalizer_failure",
                    unknown_reason=_kwargs.get(
                        "unknown_reason", "yuanbao_r12_finalizer_failure",
                    ),
                )
                finalizer_done.set()
                return result

            token.record_finalizer_or_unknown = _failed_finalizer
        return token

    adapter._preservation_source_admit = _record_admit
    _connection, _on_gateway, _created, _cleanup = await _run_yuanbao_auth_bound_push(
        adapter, call, monkeypatch, admitted, suffix="finalizer-failure",
    )
    try:
        assert await asyncio.to_thread(finalizer_done.wait, 30)
        token = admitted[0]
        assert token.native_entered is True
        assert token.native_exited is True
        assert token.finalizer_receipt is not None
        assert token.finalizer_receipt.status == "failed"
        assert token.state == "unknown"
        assert token in registry.active_leases
        assert token.native_occurrence_id is not None
    finally:
        await _on_gateway(_cleanup())


@pytest.mark.asyncio
async def test_canonical_gateway_yuanbao_settlement_readback_failure_retains_unknown(
    tmp_path, monkeypatch, started_mux,
):
    """A real Yuanbao occurrence remains UNKNOWN when its source settlement readback fails."""
    import threading

    runner, _homes, _loop, call = started_mux
    adapter, registry = _configure_started_yuanbao(runner)
    admitted = []
    settlement_done = threading.Event()
    original_admit = adapter._preservation_source_admit

    def _record_admit(marker, **kwargs):
        token = original_admit(marker, **kwargs)
        if token is not None:
            admitted.append(token)
        return token

    adapter._preservation_source_admit = _record_admit
    original_settle = adapter._preservation_settle_yuanbao_transport

    async def _failed_readback(token, custody_ref):
        settled = await original_settle(token, custody_ref)
        assert isinstance(settled, dict) and settled.get("settled") is True
        return {
            "settled": False,
            "status": "unknown",
            "reason": "yuanbao_r12_settlement_readback_failure",
        }

    monkeypatch.setattr(adapter, "_preservation_settle_yuanbao_transport", _failed_readback)

    original_finish_settlement = registry._finish_transport_settlement

    def _observe_settlement_finish(token, result):
        outcome = original_finish_settlement(token, result)
        if token in admitted:
            settlement_done.set()
        return outcome

    registry._finish_transport_settlement = _observe_settlement_finish
    _connection, _on_gateway, _created, _cleanup = await _run_yuanbao_auth_bound_push(
        adapter, call, monkeypatch, admitted, suffix="settlement-readback",
    )
    try:
        assert await asyncio.to_thread(settlement_done.wait, 30)
        token = admitted[0]
        assert token.native_entered is True
        assert token.native_exited is True
        assert token.finalizer_receipt is not None
        assert token.finalizer_receipt.status == "success"
        assert token.state == "unknown"
        assert token in registry.active_leases
        assert registry._unknown_sources.get(token) == "transport_settlement_failed"
    finally:
        await _on_gateway(_cleanup())
