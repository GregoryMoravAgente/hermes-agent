"""Tell the chat when an exec-approval prompt times out (messaging platforms).

``tools.approval_gateway_wait._await_gateway_decision`` calls ``entry.settle(reason)`` once the
wait ends. Only the TUI registered such a hook, so on Telegram / Slack / WhatsApp a card whose
timer ran out kept live buttons and the user never learned the command did NOT run. The turn
runner registers the hook here right after the prompt was delivered.

Best-effort by design: a failed notice is logged at debug — the approval already resolved as
"no", and nothing here may block the agent thread.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Optional

from agent.i18n import t
from gateway.platforms.base_exec_approval import approval_timeout_seconds, format_approval_timed_out_notice

logger = logging.getLogger(__name__)


def register_timeout_notice(
    runner, approval_data: dict, *, command: str, card_message_id: Optional[str], on_settle=None) -> bool:
    """Arm a settle hook that posts the timed-out notice for ``approval_data['request_id']``.

    ``runner`` is the ``TurnRunner`` (for ``_ctx`` and ``_schedule``); ``card_message_id`` is the
    delivered BUTTON card's id when the adapter returned one, so the card itself is edited in place
    (which also drops its buttons). The plain-text prompt passes ``None``: it has no buttons to
    drop and rewriting it would erase the record of what was asked. ``command`` is the
    already-redacted command shown to the user. The notice is skipped when the run is no longer
    current (``ctx._run_still_current``).
    Return ``True`` when the live approval entry accepted the hook; otherwise the caller receives
    ``notify_failed`` through ``on_settle`` and can retain the custody record as unknown.
    """
    from tools.approval import register_gateway_settle

    request_id = approval_data.get("request_id")
    session_key = runner._ctx.session_key or ""
    if not request_id or not session_key:
        if on_settle is not None:
            on_settle('notify_failed')
        return False
    timeout_s = approval_timeout_seconds()

    def finish(outcome: str) -> None:
        if on_settle is None:
            return
        try:
            on_settle(outcome)
        except Exception:
            logger.debug("Approval custody settle hook failed", exc_info=True)

    def finish_notice(done) -> None:
        """Settle the producer only after the actual timeout notice future finishes."""
        try:
            delivered = bool(done.result())
        except BaseException:
            logger.debug("Approval notice future failed; custody retained", exc_info=True)
            delivered = False
        finish("resolved" if delivered else "notify_failed")

    def settle(reason: str) -> None:
        if reason != "timeout":
            # Answered / interrupted / notify_failed have no later notice send. The queue's
            # settle callback is the producer's actual completion boundary for these outcomes.
            finish(reason)
            return
        # Same guard as every other late notice in TurnRunner: after /stop, /new or a restart the
        # turn is over and this chat belongs to a newer run — do not edit or post into it.
        still_current = getattr(runner._ctx, "_run_still_current", None)
        if callable(still_current) and not still_current():
            finish("notify_failed")
            return
        try:
            future = runner._schedule(
                _post_timeout_notice(runner._ctx, command, card_message_id, timeout_s),
                "Approval timeout notice scheduling error")
        except BaseException:
            logger.debug("Approval notice scheduling failed; custody retained", exc_info=True)
            finish("notify_failed")
            return
        if future is None:
            finish("notify_failed")
            return
        add_done_callback = getattr(future, "add_done_callback", None)
        if callable(add_done_callback):
            add_done_callback(finish_notice)
            return
        # Small synchronous test doubles and legacy schedulers may expose only result().
        try:
            delivered = bool(future.result())
        except BaseException:
            logger.debug("Approval notice future failed; custody retained", exc_info=True)
            delivered = False
        finish("resolved" if delivered else "notify_failed")

    armed = register_gateway_settle(session_key, request_id, settle)
    if not armed and on_settle is not None:
        on_settle('notify_failed')
    return armed


async def _post_timeout_notice(ctx, command: str, card_message_id: Optional[str], timeout_s: int) -> bool:
    from gateway.run import _interim_metadata

    adapter = ctx._status_adapter
    notice = format_approval_timed_out_notice(timeout_s)
    metadata = _interim_metadata(ctx._status_thread_metadata)
    try:
        # Plain markdown, not the card's platform markup: ``edit_message`` re-formats it itself.
        if card_message_id and await _edit_card(
                adapter, ctx._status_chat_id, card_message_id,
                t("gateway.exec_approval.timed_out_card_body", notice=notice, command=command)):
            return True
        result = await adapter.send(ctx._status_chat_id, notice, metadata=metadata)
        # Existing adapters historically returned None after a successful send; preserve that
        # compatibility while treating an explicit SendResult(success=False) as an unknown send.
        return bool(getattr(result, "success", True))
    except Exception:
        logger.debug("Approval timeout notice failed", exc_info=True)
        return False


async def _edit_card(adapter, chat_id: str, message_id: str, content: str) -> bool:
    """Edit the card in place (drops the buttons on platforms whose edit replaces the markup)."""
    edit: Optional[Callable[..., Any]] = getattr(adapter, "edit_message", None)
    if edit is None:
        return False
    try:
        result = await edit(chat_id, message_id, content)
    except Exception:
        logger.debug("Approval card edit failed; sending the notice as a new message", exc_info=True)
        return False
    return bool(getattr(result, "success", False))
