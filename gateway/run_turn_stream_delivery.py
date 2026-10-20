"""Gateway stream-final delivery phase for the turn runner.

The facade keeps turn orchestration and inherits these concrete delivery methods so stream
finalization remains on the same GatewayRunner instance and uses the existing consumer/adapter
state.
"""

from __future__ import annotations

import hashlib
import logging
from contextlib import suppress
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from gateway.turn_context import TurnContext

logger = logging.getLogger("gateway.run")


class GatewayTurnStreamDeliveryMixin:
    async def _run_agent_edit_streamed_message(
        self, _sc, source, response, content, *, _sk, ok, fail_result: str, fail_exc: str,
        run_generation: Optional[int] = None,
    ) -> None:
        """Edit the stream consumer's message in place with ``content``; on success mark
        ``response["already_sent"]`` and log ``ok``. A returned failure logs ``fail_result`` as
        ``(session, error)`` and an exception logs ``fail_exc`` as ``(session, exc)``; either way
        ``already_sent`` stays unset so the normal final send delivers the content."""
        try:
            _res = await _sc.adapter.edit_message(
                chat_id=source.chat_id, message_id=_sc.message_id, content=content, finalize=True,
            )
        except Exception as _edit_err:
            logger.warning(fail_exc, _sk, _edit_err, exc_info=True)
            return
        if not getattr(_res, "success", True):
            logger.warning(fail_result, _sk, getattr(_res, "error", None))
            return
        response["already_sent"] = True
        from gateway.session_classic_custody import ClassicStreamDeliveryWitness
        response["_classic_stream_delivery_witness"] = ClassicStreamDeliveryWitness(
            session_key=_sk,
            run_generation=run_generation,
            message_id=str(getattr(_sc, "message_id", "") or "") or None,
            payload_digest=hashlib.sha256(str(content).encode("utf-8", "surrogatepass")).hexdigest(),
        )
        logger.info(*ok)

    async def _run_agent_mark_streamed_delivery(self, response: Any, turn_ctx: TurnContext) -> None:
        """Set the final-send suppression only after the actual stream consumer confirms delivery."""
        from gateway.session_finite import finite_turn_required
        _sc = turn_ctx.stream_consumer_holder[0]
        if not isinstance(response, dict) or response.get("failed") or finite_turn_required() is True:
            return
        _final = response.get("final_response") or ""
        _is_empty_sentinel = not _final or _final == "(empty)"
        _previewed = bool(response.get("response_previewed"))
        _content_delivered = bool(_sc and getattr(_sc, "final_content_delivered", False))
        _content_delivered, _stale_finalized = self._stream_delivery_readback(
            _sc, _final, _content_delivered, _is_empty_sentinel,
        )
        _transformed = bool(response.get("response_transformed"))
        _streamed = self._run_agent_stream_confirmed_final_delivery(
            _sc, _final, previewed=_previewed,
        )
        if _is_empty_sentinel:
            return
        await self._finish_streamed_delivery(
            response, turn_ctx, _sc, _final, _previewed, _content_delivered,
            _stale_finalized, _transformed, _streamed,
        )

    @staticmethod
    def _stream_delivery_readback(_sc: Any, final_text: str, content_delivered: bool, empty: bool) -> tuple[bool, bool]:
        """Revalidate the consumer's recorded final payload before suppressing the normal send."""
        stale = False
        if content_delivered and not empty:
            matcher = getattr(_sc, "delivered_final_matches", None)
            if callable(matcher):
                with suppress(Exception):
                    stale = matcher(final_text) is False
            if stale:
                content_delivered = False
        return content_delivered, stale

    async def _finish_streamed_delivery(
        self, response: Any, turn_ctx: Any, _sc: Any, final_text: str, previewed: bool,
        content_delivered: bool, stale_finalized: bool, transformed: bool, streamed: bool,
    ) -> None:
        """Apply the existing edit-or-send reconciliation once for a completed stream."""
        session_key = turn_ctx.session_key or "?"
        source = turn_ctx.source
        if not transformed and (streamed or content_delivered):
            logger.info(
                "Suppressing normal final send for session %s: final delivery already confirmed "
                "(streamed=%s previewed=%s content_delivered=%s).",
                session_key, streamed, previewed, content_delivered,
            )
            if streamed:
                from gateway.session_classic_custody import ClassicStreamDeliveryWitness
                response["_classic_stream_delivery_witness"] = ClassicStreamDeliveryWitness(
                    session_key=turn_ctx.session_key,
                    run_generation=turn_ctx.run_generation,
                    message_id=str(getattr(_sc, "message_id", "") or "") or None,
                    payload_digest=hashlib.sha256(str(final_text).encode("utf-8", "surrogatepass")).hexdigest(),
                )
            response["already_sent"] = True
            return
        if not transformed and stale_finalized and _sc is not None:
            await self._reconcile_stale_streamed_delivery(
                _sc, source, response, final_text, session_key, turn_ctx,
            )
            return
        if transformed and _sc is not None:
            await self._reconcile_transformed_streamed_delivery(
                _sc, source, response, session_key, turn_ctx,
            )
            return
        if _sc is not None and getattr(_sc, "stream_deltas_enabled", True):
            logger.warning(
                "Normal final-send NOT suppressed despite active stream consumer for session %s: "
                "streamed=%s previewed=%s content_delivered=%s transformed=%s final_len=%d — "
                "possible duplicate send (see wecom ack-timeout RCA).",
                session_key, streamed, previewed, content_delivered, transformed, len(final_text),
            )

    async def _reconcile_stale_streamed_delivery(
        self, _sc: Any, source: Any, response: Any, final_text: str, session_key: str, turn_ctx: TurnContext,
    ) -> None:
        """Edit a stale single-message stream; split delivery keeps the normal final-send path."""
        if getattr(_sc, "_turn_split_delivery", False):
            logger.info(
                "Stale streamed finalize detected for session %s on a multi-message split; "
                "skipping the in-place reconciliation edit and delivering the complete response "
                "via normal final send (#78541).", session_key,
            )
            return
        message_id = getattr(_sc, "message_id", None)
        adapter = getattr(_sc, "adapter", None)
        if not message_id or message_id == "__no_edit__" or adapter is None:
            logger.info(
                "Stale streamed finalize detected for session %s with no editable message; "
                "delivering complete response via normal final send (#71643).", session_key,
            )
            return
        await self._run_agent_edit_streamed_message(
            _sc, source, response, final_text, _sk=session_key,
            run_generation=turn_ctx.run_generation,
            ok=("Reconciled stale streamed finalize for session %s: edited message %s with the complete response (#71643).", session_key, message_id),
            fail_result="Stale-finalize reconciliation edit failed for session %s (%s); sending complete response via normal final send.",
            fail_exc="Stale-finalize reconciliation edit failed for session %s: %s; sending complete response via normal final send.",
        )

    async def _reconcile_transformed_streamed_delivery(
        self, _sc: Any, source: Any, response: Any, session_key: str, turn_ctx: TurnContext,
    ) -> None:
        """Edit the one streamed message after a plugin transforms the final response."""
        message_id = getattr(_sc, "message_id", None)
        if not message_id:
            return
        await self._run_agent_edit_streamed_message(
            _sc, source, response, response["final_response"], _sk=session_key,
            run_generation=turn_ctx.run_generation,
            ok=("Edited streamed message %s for session %s to include plugin-transformed content.", message_id, session_key),
            fail_result="Transformed-final edit failed for session %s (%s); sending transformed response via normal final send.",
            fail_exc="Failed to edit streamed message for session %s: %s",
        )
