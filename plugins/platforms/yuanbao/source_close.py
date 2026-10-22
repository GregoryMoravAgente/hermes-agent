"""Yuanbao source-close ownership helpers kept outside the adapter facade."""
from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
import hashlib
import json
import logging
from typing import Any, Optional

from gateway.platforms.base import SendResult

logger = logging.getLogger(__name__)


def _digest_json(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _set_group_consumer_tokens(consumer_task, tokens) -> None:
    if consumer_task is not None:
        consumer_task._preservation_source_tokens = tuple(tokens or ())


async def _consume_yuanbao_group_queue(adapter, session_key: str) -> None:
    """Drain one group queue while retaining every accepted source token."""
    queue = adapter._group_queues.get(session_key)
    if not queue:
        return
    current_tokens = ()
    consumer_task = asyncio.current_task()
    try:
        while True:
            try:
                dispatch_fn = await asyncio.wait_for(queue.get(), timeout=2.0)
            except TimeoutError:
                break
            current_tokens = tuple(getattr(dispatch_fn, "_preservation_source_tokens", ()) or ())
            _set_group_consumer_tokens(consumer_task, current_tokens)
            logger.debug(
                "[%s] Group queue: dispatching for %s (remaining=%d)",
                adapter.name, (session_key or "")[:50], queue.qsize(),
            )
            try:
                await dispatch_fn()
                while session_key in adapter._active_sessions:
                    await asyncio.sleep(0.1)
                current_tokens = ()
                _set_group_consumer_tokens(consumer_task, ())
            except Exception:
                logger.exception("[%s] Group queue consumer error", adapter.name)
    except asyncio.CancelledError:
        # A queued closure has already crossed the provider admission boundary.  If the
        # group consumer is cancelled before it can invoke that closure, retain every exact
        # token as UNKNOWN instead of dropping the queue with accepted work inside it.
        for token in current_tokens:
            adapter._preservation_source_unknown(token, "yuanbao_group_consumer_cancelled")
        while True:
            try:
                dispatch_fn = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            for token in tuple(getattr(dispatch_fn, "_preservation_source_tokens", ()) or ()):
                adapter._preservation_source_unknown(token, "yuanbao_group_consumer_cancelled")
        _set_group_consumer_tokens(consumer_task, ())
        raise
    finally:
        _set_group_consumer_tokens(consumer_task, ())
        adapter._group_queues.pop(session_key, None)


@dataclass(frozen=True)
class YuanbaoPushAckObservation:
    """Result of the actual push ACK write; no caller-supplied success flag."""

    required: bool
    sent: bool
    frame_digest: str


@dataclass
class YuanbaoSourceHandoff:
    """The real debounce insertion and timer identity for one admitted push."""

    key: str
    payload_digest: str
    connection_generation: str
    connection_marker: object = field(repr=False, compare=False)
    source_token: object = field(repr=False, compare=False)
    timer: object = field(repr=False, compare=False)
    producer_task: object | None = field(default=None, repr=False, compare=False)

    @property
    def handoff_digest(self) -> str:
        return _digest_json({
            "key": self.key,
            "payload_digest": self.payload_digest,
            "connection_generation": self.connection_generation,
        })


async def _execute_yuanbao_pipeline(adapter, data_list, source_tokens) -> None:
    from plugins.platforms.yuanbao.adapter import InboundContext

    context = InboundContext(adapter=adapter, raw_frames=data_list, source_tokens=source_tokens)
    try:
        await adapter._inbound_pipeline.execute(context)
    except asyncio.CancelledError:
        for token in source_tokens:
            adapter._preservation_source_unknown(token, "yuanbao_pipeline_cancelled")
        raise
    except Exception:
        for token in source_tokens:
            adapter._preservation_source_unknown(token, "yuanbao_pipeline_failed")
        logger.exception("[%s] Yuanbao inbound pipeline failed", adapter.name)
        raise
    if not getattr(context, "_preservation_source_dispatch_scheduled", False):
        for token in source_tokens:
            adapter._preservation_source_unknown(token, "yuanbao_pipeline_no_dispatch")


def _observe_yuanbao_pipeline(adapter, source_tokens, done) -> None:
    try:
        if done.cancelled():
            reason = "yuanbao_pipeline_task_cancelled"
        else:
            reason = "yuanbao_pipeline_task_failed" if done.exception() is not None else None
    except asyncio.CancelledError:
        reason = "yuanbao_pipeline_task_cancelled"
    if reason:
        for token in source_tokens:
            adapter._preservation_source_unknown(token, reason)


class YuanbaoConnectionSourceCloseMixin:
    """Own the websocket debounce buffer's source-token transfer and failure paths."""

    def _extract_sender_key(self, raw_data: bytes) -> str:
        from plugins.platforms.yuanbao.adapter import DecodeMiddleware, decode_inbound_push

        with contextlib.suppress(Exception):
            parsed = json.loads(raw_data.decode("utf-8"))
            if isinstance(parsed, dict):
                from_account, group_code = DecodeMiddleware.json_sender_fields(parsed)
                if from_account:
                    return f"{from_account}:{group_code}"
        with contextlib.suppress(Exception):
            push = decode_inbound_push(raw_data)
            if push:
                return f"{push.get('from_account', '')}:{push.get('group_code', '')}"
        return f"__unknown_{id(raw_data)}"

    def _push_to_inbound(self, raw_data: bytes, *, source_token=None) -> YuanbaoSourceHandoff:
        """Debounce frames while retaining each accepted push token."""
        connection_generation = getattr(self, "_connect_id", None)
        connection_marker = getattr(self, "_ws", None)
        if (
            not isinstance(connection_generation, str)
            or not connection_generation
            or connection_marker is None
        ):
            raise RuntimeError("yuanbao_source_connection_generation_missing")
        key = self._extract_sender_key(raw_data)
        existing_timer = self._inbound_timers.pop(key, None)
        if existing_timer:
            existing_timer.cancel()
        self._inbound_buffer.setdefault(key, []).append((raw_data, source_token))
        logger.debug(
            "[%s] Debounce: buffered frame for key=%s, count=%d",
            self._adapter.name, key, len(self._inbound_buffer[key]),
        )
        timer = asyncio.get_running_loop().call_later(
            self._DEBOUNCE_WINDOW, self._flush_inbound_buffer, key,
        )
        self._inbound_timers[key] = timer
        return YuanbaoSourceHandoff(
            key=key,
            payload_digest=hashlib.sha256(raw_data).hexdigest(),
            connection_generation=connection_generation,
            connection_marker=connection_marker,
            source_token=source_token,
            timer=timer,
        )

    async def _send_source_push_ack(self, head, source_token) -> YuanbaoPushAckObservation | None:
        """Send a PushAck while preserving exact source-token failure custody."""
        ack_required = bool(head.get("need_ack", False))
        if not ack_required:
            return YuanbaoPushAckObservation(
                required=False, sent=False, frame_digest=_digest_json(head),
            )
        if self._ws is None:
            if source_token is not None:
                self._adapter._preservation_source_unknown(
                    source_token, "yuanbao_push_ack_transport_missing",
                )
            return None
        from plugins.platforms.yuanbao.adapter import encode_push_ack

        adapter = self._adapter
        try:
            encoded = encode_push_ack(head)
            await self._ws.send(encoded)
            return YuanbaoPushAckObservation(
                required=True, sent=True, frame_digest=hashlib.sha256(encoded).hexdigest(),
            )
        except asyncio.CancelledError:
            if source_token is not None:
                adapter._preservation_source_unknown(source_token, "yuanbao_push_ack_cancelled")
            raise
        except Exception as ack_exc:
            if source_token is not None:
                adapter._preservation_source_unknown(source_token, "yuanbao_push_ack_failed")
            logger.exception("[%s] Failed to send PushAck: %s", adapter.name, ack_exc)
            return None

    @staticmethod
    def _preservation_bind_yuanbao_handoff_task(source_tokens, task) -> None:
        for token in source_tokens:
            receipt = getattr(token, "transport_receipt", None)
            handoff = getattr(receipt, "handoff", None)
            if isinstance(handoff, YuanbaoSourceHandoff) and handoff.source_token is token:
                handoff.producer_task = task

    def _attach_source_transport(self, source_token, ack_observation, handoff) -> None:
        adapter = self._adapter
        try:
            receipt = adapter._preservation_build_yuanbao_transport_receipt(
                source_token, ack_observation, handoff,
            )
            custody_ref = adapter._preservation_source_attach_transport(
                source_token,
                receipt,
                adapter._preservation_settle_yuanbao_transport,
            )
            if not custody_ref and getattr(source_token, "state", None) != "unknown":
                adapter._preservation_source_unknown(
                    source_token, "yuanbao_transport_custody_missing",
                )
        except (AttributeError, RuntimeError, TypeError, ValueError):
            # An ACK write can have already recorded the first causal UNKNOWN.
            # Receipt construction must retain that reason instead of replacing it.
            if getattr(source_token, "state", None) != "unknown":
                adapter._preservation_source_unknown(
                    source_token, "yuanbao_transport_receipt_failed",
                )


    def _flush_inbound_buffer(self, key: str) -> None:
        """Run the pipeline over a buffered push batch and retain unresolved tokens."""
        self._inbound_timers.pop(key, None)
        entries = self._inbound_buffer.pop(key, [])
        if not entries:
            return
        adapter = self._adapter
        data_list = [entry[0] if isinstance(entry, tuple) else entry for entry in entries]
        source_tokens = [
            entry[1] for entry in entries
            if isinstance(entry, tuple) and entry[1] is not None
        ]
        logger.info(
            "[%s] Debounce flush: key=%s, aggregated %d frames",
            adapter.name, key, len(data_list),
        )

        try:
            task = asyncio.create_task(
                _execute_yuanbao_pipeline(adapter, data_list, source_tokens),
                name=f"yuanbao-pipeline-{key}",
            )
            self._preservation_bind_yuanbao_handoff_task(source_tokens, task)
            # Keep the exact accepted source set attached to this task until its done callback
            # runs.  Disconnect must await this pipeline before releasing the platform lock;
            # task disappearance alone is never a terminal receipt.
            task._preservation_source_tokens = tuple(source_tokens)

            task.add_done_callback(
                lambda done: _observe_yuanbao_pipeline(adapter, source_tokens, done),
            )
            adapter._track_task(task)
        except asyncio.CancelledError:
            for token in source_tokens:
                adapter._preservation_source_unknown(token, "yuanbao_pipeline_task_cancelled")
            raise
        except Exception:
            for token in source_tokens:
                adapter._preservation_source_unknown(token, "yuanbao_pipeline_task_failed")
            raise


class YuanbaoAdapterSourceCloseMixin:
    """Own the adapter-level contract and shutdown treatment for accepted source work."""

    def _preservation_build_yuanbao_transport_receipt(
        self, token, ack: YuanbaoPushAckObservation, handoff: YuanbaoSourceHandoff,
    ):
        from gateway.preservation_source_close import SourceAdmissionToken, SourceTransportReceipt

        if not isinstance(token, SourceAdmissionToken) or handoff.source_token is not token:
            raise ValueError("yuanbao_source_handoff_token_mismatch")
        if not isinstance(token.marker, dict):
            raise ValueError("yuanbao_source_marker_invalid")
        if not isinstance(ack, YuanbaoPushAckObservation):
            raise ValueError("yuanbao_push_ack_observation_missing")
        if not isinstance(ack.frame_digest, str) or not ack.frame_digest:
            raise ValueError("yuanbao_push_ack_digest_missing")
        if ack.required and not ack.sent:
            raise ValueError("yuanbao_push_ack_not_sent")
        if (
            not handoff.connection_generation
            or handoff.connection_marker is None
            or handoff.timer is None
        ):
            raise ValueError("yuanbao_source_handoff_incomplete")
        return SourceTransportReceipt(
            protocol="hermes.yuanbao.push-transport.v1",
            producer_kind="yuanbao-websocket-push",
            source_marker=token.marker,
            registration_generation=token.registration_generation,
            invocation_id=token.invocation_id,
            close_generation=token.close_generation,
            producer_generation=handoff.connection_generation,
            source_event_digest=_digest_json({
                "marker": token.marker,
                "phase": token.phase,
                "handoff_digest": handoff.handoff_digest,
            }),
            wire_identity_digest=handoff.payload_digest,
            handoff_digest=handoff.handoff_digest,
            ack_required=ack.required,
            ack_sent=ack.sent,
            ack_digest=ack.frame_digest,
            native_occurrence_id=token.native_occurrence_id,
            handoff=handoff,
        )

    def _preservation_source_tasks_for_token(self, token) -> tuple[asyncio.Task, ...]:
        tasks = set(getattr(self, "_inbound_tasks", ()))
        tasks.update(getattr(self, "_background_tasks", ()))
        return tuple(
            task for task in tasks
            if token in tuple(getattr(task, "_preservation_source_tokens", ()) or ())
        )

    def _preservation_yuanbao_receipt(self, token, custody_ref):
        from gateway.preservation_source_close import SourceAdmissionToken, SourceTransportReceipt

        if not isinstance(token, SourceAdmissionToken):
            return None
        receipt = getattr(token, "transport_receipt", None)
        if not isinstance(receipt, SourceTransportReceipt) or not receipt.matches_token(token):
            return None
        if (
            receipt.protocol != "hermes.yuanbao.push-transport.v1"
            or receipt.producer_kind != "yuanbao-websocket-push"
        ):
            return None
        if (
            token.custody_ref != custody_ref
            or receipt.custody_ref != custody_ref
            or receipt.receipt_digest != token.transport_receipt_digest
            or token.state == "unknown"
        ):
            return None
        if not token.native_entered or not token.native_exited or not token.producer_done:
            return None
        finalizer = getattr(token, "finalizer_receipt", None)
        if getattr(finalizer, "status", None) != "success":
            return None
        if (
            finalizer.protocol != receipt.protocol
            or finalizer.producer_kind != receipt.producer_kind
            or finalizer.native_occurrence_id != str(token.native_occurrence_id)
            or finalizer.producer_generation != receipt.producer_generation
            or finalizer.close_generation != receipt.close_generation
            or finalizer.transport_receipt_digest != token.transport_receipt_digest
        ):
            return None
        return receipt

    def _preservation_yuanbao_handoff_task(self, token, receipt):
        handoff = receipt.handoff
        connection = self._connection
        if not isinstance(handoff, YuanbaoSourceHandoff):
            return None
        if (
            handoff.source_token is not token
            or handoff.connection_marker is not connection._ws
            or connection._connect_id != receipt.producer_generation
            or receipt.handoff_digest != handoff.handoff_digest
            or receipt.wire_identity_digest != handoff.payload_digest
            or receipt.source_event_digest != _digest_json({
                "marker": token.marker,
                "phase": token.phase,
                "handoff_digest": handoff.handoff_digest,
            })
        ):
            return None
        producer_task = handoff.producer_task
        if not isinstance(producer_task, asyncio.Task):
            return None
        if token not in tuple(getattr(producer_task, "_preservation_source_tokens", ()) or ()):
            return None
        return handoff, producer_task, connection

    async def _preservation_wait_yuanbao_tasks(self, token, producer_task) -> bool:
        current_task = asyncio.current_task()
        source_tasks = self._preservation_source_tasks_for_token(token)
        pending_tasks = tuple(
            task for task in source_tasks if task is not current_task and not task.done()
        )
        if pending_tasks:
            results = await asyncio.gather(*pending_tasks, return_exceptions=True)
            if any(isinstance(result, BaseException) for result in results):
                return False
        if producer_task is not current_task and not producer_task.done():
            await producer_task
        if (
            not producer_task.done()
            or producer_task.cancelled()
            or producer_task.exception() is not None
        ):
            return False
        return token.state != "unknown"

    def _preservation_yuanbao_work_remaining(self, token, handoff, connection) -> bool:
        for entries in getattr(connection, "_inbound_buffer", {}).values():
            if any(
                isinstance(entry, tuple) and len(entry) == 2 and entry[1] is token
                for entry in entries
            ):
                return True
        if getattr(connection, "_inbound_timers", {}).get(handoff.key) is handoff.timer:
            return True
        for queue in getattr(self, "_group_queues", {}).values():
            for dispatch_fn in tuple(getattr(queue, "_queue", ())):
                if token in tuple(getattr(dispatch_fn, "_preservation_source_tokens", ()) or ()):
                    return True
        return any(
            not task.done() for task in self._preservation_source_tasks_for_token(token)
        )

    async def _preservation_settle_yuanbao_transport(self, token, custody_ref):
        receipt = self._preservation_yuanbao_receipt(token, custody_ref)
        if receipt is None:
            return False
        binding = self._preservation_yuanbao_handoff_task(token, receipt)
        if binding is None:
            return False
        handoff, producer_task, connection = binding
        if not await self._preservation_wait_yuanbao_tasks(token, producer_task):
            return False
        if self._preservation_yuanbao_work_remaining(token, handoff, connection):
            return False
        return {
            "settled": True,
            "status": "success",
            "protocol": receipt.protocol,
            "producer_generation": receipt.producer_generation,
            "native_occurrence_id": token.native_occurrence_id,
            "transport_receipt_digest": token.transport_receipt_digest,
        }

    def _preservation_source_contract(self):
        from gateway.preservation_source_close import SourceCloseContract

        return SourceCloseContract.for_adapter(self, name="yuanbao-push-source")

    def _preservation_source_shutdown(self) -> None:
        connection = self._connection
        for timer in tuple(getattr(connection, "_inbound_timers", {}).values()):
            timer.cancel()
        getattr(connection, "_inbound_timers", {}).clear()
        inbound_buffer = getattr(connection, "_inbound_buffer", {})
        for entries in tuple(inbound_buffer.values()):
            for entry in entries:
                token = entry[1] if isinstance(entry, tuple) and len(entry) == 2 else None
                if token is not None:
                    self._preservation_source_unknown(token, "yuanbao_debounce_shutdown")
        inbound_buffer.clear()
        for queue in self._group_queues.values():
            while True:
                try:
                    dispatch_fn = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                for token in getattr(dispatch_fn, "_preservation_source_tokens", ()):
                    self._preservation_source_unknown(token, "yuanbao_group_queue_shutdown")
        # Admission happens before ACK and debounce. A cancellation or disconnect can therefore
        # leave an accepted token in the adapter's exact base-owned pending view without a buffer,
        # queue, or task handle. Mark that same opaque token UNKNOWN and retain registry custody.
        pending_tokens = getattr(self, "_preservation_source_pending_tokens", None)
        if isinstance(pending_tokens, set):
            for token in tuple(pending_tokens):
                self._preservation_source_unknown(token, "yuanbao_pending_shutdown")

    async def _preservation_await_source_tasks(self) -> None:
        """Cancel and await every inbound or source-tagged task before transport release.

        Inbound tasks are source-owned by definition.  Background tasks are included only when
        the source pipeline attached its exact token tuple; unrelated notifier/owner tasks stay
        outside this close barrier.  A cancellation or exception is handled by each task's
        existing UNKNOWN path, so this helper never turns task absence into successful custody.
        """
        tasks = set(getattr(self, "_inbound_tasks", ()))
        tasks.update(
            task for task in getattr(self, "_background_tasks", ())
            if getattr(task, "_preservation_source_tokens", ())
        )
        pending = tuple(task for task in tasks if not task.done())
        for task in pending:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _process_message_background(self, event, session_key: str) -> None:
        """Wrap base processing with the Yuanbao slow-response notifier."""
        chat_id = event.source.chat_id
        await self._outbound.slow_notifier.start(chat_id)
        try:
            await super()._process_message_background(event, session_key)
        finally:
            self._outbound.slow_notifier.cancel(chat_id)
            msg_id = event.message_id
            if msg_id and self._processing_msg_ids.get(session_key) == msg_id:
                self._processing_msg_ids.pop(session_key, None)
                self._processing_msg_texts.pop(session_key, None)

    def _track_task(self, task: asyncio.Task) -> asyncio.Task:
        """Register a fire-and-forget task so it won't be GC'd prematurely."""
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    @property
    def enforces_own_access_policy(self) -> bool:
        """Intake gating lives in ``AccessPolicy`` (composed, not inherited), so the flag stays here."""
        return True

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        ok = await self._connection.open()
        if ok:
            self._wire_plugin_handlers(None)  # plugin-registered native handlers
        return ok

    async def disconnect(self) -> None:
        """Cancel background tasks and close the WebSocket connection."""
        from plugins.platforms.yuanbao.adapter import YuanbaoAdapter

        if YuanbaoAdapter._active_instance is self:
            YuanbaoAdapter._active_instance = None
        for home_key, active in list(YuanbaoAdapter._active_instances.items()):
            if active is self:
                del YuanbaoAdapter._active_instances[home_key]
        self._running = False
        self._mark_disconnected()
        self._preservation_source_shutdown()
        await self._preservation_await_source_tasks()
        self._release_platform_lock()
        await self._connection.close()
        await self._outbound.close()
        for task in list(self._inbound_tasks):
            if not task.done():
                task.cancel()
        self._inbound_tasks.clear()
        self._group_queues.clear()
        logger.info("[%s] Disconnected", self.name)

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[dict[str, Any]] = None, group_code: str = "") -> SendResult:
        return await self._outbound.sender.send_text(chat_id, content, reply_to, group_code=group_code)

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        return {"name": chat_id, "type": "group" if chat_id.startswith("group:") else "dm"}

    async def send_typing(self, chat_id: str, metadata: Optional[dict] = None) -> None:
        """Start the RUNNING heartbeat (best effort)."""
        with contextlib.suppress(Exception):
            await self._outbound.heartbeat.start(chat_id)

    async def stop_typing(self, chat_id: str) -> None:
        """Stop RUNNING without FINISH — send() emits FINISH after delivery so ordering is
        RUNNING… → message → FINISH."""
        with contextlib.suppress(Exception):
            await self._outbound.heartbeat.stop(chat_id, send_finish=False)
