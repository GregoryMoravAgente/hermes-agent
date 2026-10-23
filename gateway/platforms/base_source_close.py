"""Source-close helpers for the common platform-adapter lifecycle."""
from __future__ import annotations

import asyncio
from functools import wraps
import logging

logger = logging.getLogger(__name__)


class PreservationSourceAdapterMixin:
    """Keep source admission and batching custody out of the base adapter facade."""

    def _preservation_source_contract(self):
        from gateway.platforms.base import BasePlatformAdapter
        from gateway.preservation_source_close import SourceCloseContract

        supported = type(self).handle_message is BasePlatformAdapter.handle_message
        return SourceCloseContract.for_adapter(
            self, name=f"base:{getattr(self.platform, 'value', self.platform)}", supported=supported,
        )

    def _preservation_close_source(self, close_generation: int) -> bool:
        current = getattr(self, "_preservation_source_close_generation", None)
        if current is not None and current != close_generation:
            return False
        self._preservation_source_close_generation = close_generation
        return True

    def _preservation_resume_source(self, close_generation: int) -> bool:
        if getattr(self, "_preservation_source_close_generation", None) != close_generation:
            return False
        self._preservation_source_close_generation = None
        return True

    def _preservation_source_pending(self, close_generation: int) -> tuple:
        if getattr(self, "_preservation_source_close_generation", None) != close_generation:
            return ("source_close_generation_mismatch",)
        return tuple(getattr(self, "_preservation_source_pending_tokens", set()))

    async def _preservation_await_source_close(self, close_generation: int, timeout: float) -> bool:
        """Optional provider hook; the durable WhatsApp adapter overrides this seam."""
        return True

    async def _preservation_await_source_resume(self, close_generation: int, timeout: float) -> bool:
        """Optional provider hook; the durable WhatsApp adapter overrides this seam."""
        return True

    def _preservation_attach_transport(self, token, receipt, settlement, custody_ref=None):
        registry = self._preservation_source_registry()
        return registry.attach_transport(token, receipt, settlement, custody_ref) if registry is not None else None

    def _preservation_source_attach_transport(self, token, receipt, settlement, custody_ref=None):
        return self._preservation_attach_transport(token, receipt, settlement, custody_ref)

    def _preservation_source_registry(self):
        runner = getattr(self, "gateway_runner", None)
        if runner is None:
            runner = getattr(getattr(self, "_message_handler", None), "__self__", None)
        return getattr(runner, "_preservation_source_close", None)

    def _preservation_source_admit(
        self, marker, *, event=None, phase: str = "source", profile=None, source_token=None,
    ):
        """Issue or explicitly hand off the registered adapter's opaque token."""
        registry = self._preservation_source_registry()
        if registry is None:
            return None
        return registry.admit_source(
            self, marker, profile=profile, event=event, phase=phase, source_token=source_token,
        )

    def _preservation_source_handoff(self, source_tokens, event=None, profile=None):
        registry = self._preservation_source_registry()
        if registry is None:
            return ()
        if source_tokens:
            return registry.handoff_source(self, source_tokens, profile=profile, event=event)
        bound = registry.tokens_for_event(event) if event is not None else ()
        if len(bound) > 1:
            for token in bound:
                registry.mark_unknown(token, "ambiguous_event_source_tokens")
            return registry._refuse_new_callback(event, "source_identity_ambiguous")
        return bound

    def _preservation_source_context(self, source_tokens):
        from gateway.preservation_source_close import source_handoff_context

        return source_handoff_context(source_tokens)

    def _preservation_source_tokens(self, event):
        registry = self._preservation_source_registry()
        if registry is None:
            return ()
        return registry.tokens_for_event(event)

    def _preservation_source_bind(self, token, event):
        registry = self._preservation_source_registry()
        return registry.bind_event(token, event) if registry is not None else None

    def _preservation_source_unknown(self, item, reason: str) -> None:
        registry = self._preservation_source_registry()
        if registry is not None:
            registry.mark_unknown(item, reason)

    def _preservation_source_refused(self, event, *, marker, reason: str) -> None:
        registry = self._preservation_source_registry()
        if registry is not None:
            registry.record_source_refusal(self, event, marker=marker, reason=reason)

    def _preservation_source_producer_done(self, item) -> None:
        registry = self._preservation_source_registry()
        if registry is not None:
            registry.mark_producer_done(item)

    def _preservation_source_release(self, item) -> None:
        registry = self._preservation_source_registry()
        if registry is not None:
            from gateway.preservation_source_close import SourceAdmissionToken

            if isinstance(item, SourceAdmissionToken):
                registry.release(item)
            else:
                registry.release_event(item)

    def _preservation_mark_unknown(self, event, reason: str) -> None:
        handoff = getattr(event, "_preservation_handoff", None)
        if handoff is not None:
            handoff.mark_unknown(reason)
        self._preservation_source_unknown(event, reason)

    def _enqueue_text_event(self, event) -> None:
        """Buffer a text event while retaining every source token until its flush."""
        from gateway.platforms.base import _append_text

        if self._drop_unresolved(event):
            self._preservation_mark_unknown(event, "no_consumer")
            return
        from hermes_state_runtime import RuntimeStoreError

        source_tokens = self._preservation_source_tokens(event)
        try:
            self._preservation_source_admit(
                event, event=event, phase="text-batch", source_token=source_tokens or None,
            )
        except RuntimeStoreError as exc:
            logger.debug("[%s] Source-close refused text batch: %s", self.name, exc)
            return
        event._preservation_source_deferred = True
        key = self._text_batch_key(event)
        existing = self._pending_text_batches.get(key)
        if existing is None:
            existing = self._pending_text_batches[key] = event
            deliveries = getattr(event, "_preservation_bridge_deliveries", None)
            if deliveries:
                existing._preservation_bridge_deliveries = list(deliveries)
        else:
            if event.text:
                existing.text = _append_text(existing.text, event.text)
            if event.media_urls:
                existing.media_urls.extend(event.media_urls)
                existing.media_types.extend(event.media_types)
            existing.absorb_reply_expected(event)
            deliveries = getattr(event, "_preservation_bridge_deliveries", None)
            if deliveries:
                current = getattr(existing, "_preservation_bridge_deliveries", None)
                existing._preservation_bridge_deliveries = list(current or ()) + list(deliveries)
            registry = self._preservation_source_registry()
            if registry is not None:
                registry.merge_events(existing, event)
        existing._last_chunk_len = len(event.text or "")
        prior_task = self._pending_text_batch_tasks.get(key)
        if prior_task and not prior_task.done():
            prior_task.cancel()
        self._pending_text_batch_tasks[key] = asyncio.create_task(self._flush_text_batch(key))

    async def _dispatch_text_batch(self, event) -> None:
        """Hand a flushed batch to the pipeline with its explicit source handoff."""
        await self.handle_message(event, source_tokens=self._preservation_source_tokens(event))


def preservation_source_entry(method):
    """Decorate the common handler with admission and exact producer handoff custody."""
    @wraps(method)
    async def _wrapped(self, event, *, source_token=None, source_tokens=None):
        explicit_tokens = source_tokens if source_tokens is not None else source_token
        source_handoff = self._preservation_source_handoff(explicit_tokens, event=event)
        admitted = self._preservation_source_admit(
            event, event=event, phase="handle-message", source_token=source_handoff or None,
        )
        if not source_handoff and admitted is not None:
            source_handoff = (admitted,) if not isinstance(admitted, tuple) else admitted
        return await method(self, event, _source_handoff=source_handoff)

    return _wrapped
