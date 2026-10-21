"""Yuanbao source-close ownership helpers kept outside the adapter facade."""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging

logger = logging.getLogger(__name__)


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

    def _push_to_inbound(self, raw_data: bytes, *, source_token=None) -> None:
        """Debounce frames while retaining each accepted push token."""
        key = self._extract_sender_key(raw_data)
        existing_timer = self._inbound_timers.pop(key, None)
        if existing_timer:
            existing_timer.cancel()
        self._inbound_buffer.setdefault(key, []).append((raw_data, source_token))
        logger.debug(
            "[%s] Debounce: buffered frame for key=%s, count=%d",
            self._adapter.name, key, len(self._inbound_buffer[key]),
        )
        self._inbound_timers[key] = asyncio.get_running_loop().call_later(
            self._DEBOUNCE_WINDOW, self._flush_inbound_buffer, key,
        )

    def _flush_inbound_buffer(self, key: str) -> None:
        """Run the pipeline over a buffered push batch and retain unresolved tokens."""
        from plugins.platforms.yuanbao.adapter import InboundContext

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

        async def _execute() -> None:
            context = InboundContext(adapter=adapter, raw_frames=data_list, source_tokens=source_tokens)
            try:
                await adapter._inbound_pipeline.execute(context)
            except asyncio.CancelledError:
                for token in source_tokens:
                    adapter._preservation_source_unknown(token, "yuanbao_pipeline_cancelled")
                raise
            except Exception:
                for token in source_tokens:
                    if getattr(token, "event", None) is None:
                        adapter._preservation_source_unknown(token, "yuanbao_pipeline_failed")
                logger.exception("[%s] Yuanbao inbound pipeline failed", adapter.name)
                raise
            for token in source_tokens:
                if getattr(token, "event", None) is None:
                    adapter._preservation_source_unknown(token, "yuanbao_pipeline_no_dispatch")

        try:
            adapter._track_task(asyncio.create_task(_execute(), name=f"yuanbao-pipeline-{key}"))
        except asyncio.CancelledError:
            for token in source_tokens:
                adapter._preservation_source_unknown(token, "yuanbao_pipeline_task_cancelled")
            raise
        except (RuntimeError, TypeError, ValueError):
            for token in source_tokens:
                adapter._preservation_source_unknown(token, "yuanbao_pipeline_task_failed")
            raise


class YuanbaoAdapterSourceCloseMixin:
    """Own the adapter-level contract and shutdown treatment for accepted source work."""

    def _preservation_source_contract(self):
        from gateway.preservation_source_close import SourceCloseContract

        return SourceCloseContract.for_adapter(self, name="yuanbao-push-source")

    def _preservation_source_shutdown(self) -> None:
        for entries in getattr(self._connection, "_inbound_buffer", {}).values():
            for entry in entries:
                token = entry[1] if isinstance(entry, tuple) and len(entry) == 2 else None
                if token is not None:
                    self._preservation_source_unknown(token, "yuanbao_debounce_shutdown")
        for queue in self._group_queues.values():
            while True:
                try:
                    dispatch_fn = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                for token in getattr(dispatch_fn, "_preservation_source_tokens", ()):
                    self._preservation_source_unknown(token, "yuanbao_group_queue_shutdown")

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
