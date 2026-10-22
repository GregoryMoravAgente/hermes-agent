"""Adapter-side bridges for the classic busy custody boundary."""
from __future__ import annotations


def begin_adapter_event(adapter, event, session_key, producer, *, state='queued'):
    from gateway.run_busy_custody import begin_classic_event
    return begin_classic_event(
        getattr(adapter, 'gateway_runner', None), event, session_key, producer, state=state)


def mark_adapter_event(adapter, event, state):
    from gateway.run_busy_custody import mark_classic_event
    mark_classic_event(getattr(adapter, 'gateway_runner', None), event, state)


def finish_adapter_event(adapter, event, *, success, reason='join_failed', receipt=None):
    from gateway.run_busy_custody import finish_classic_event
    finish_classic_event(
        getattr(adapter, 'gateway_runner', None), event, success=success, reason=reason,
        receipt=receipt)


def release_unaccepted_adapter_event(adapter, event):
    from gateway.run_busy_custody import release_unaccepted_classic_event
    release_unaccepted_classic_event(getattr(adapter, 'gateway_runner', None), event)


def attach_adapter_event(adapter, aggregate, event):
    runner = getattr(adapter, 'gateway_runner', None)
    if runner is None:
        return
    from gateway.run_busy_custody import attach_classic_event
    attach_classic_event(runner, aggregate, event)

async def _dispatch_busy_bypass(adapter, event, session_key):
    """Dispatch shared-runtime, command, and clarify inputs before busy-slot admission."""
    self = adapter
    if self._shared_runtime_owns_input(event):
        from gateway.session_ingress_burst import dispatch_busy_burst
        await dispatch_busy_burst(self, event, session_key)
        return True
    self._canonicalize(event.source)
    cmd = event.get_command()
    from hermes_cli.commands import is_interrupt_then_dispatch, should_bypass_active_session
    if should_bypass_active_session(cmd):
        try:
            if cmd and is_interrupt_then_dispatch(cmd):
                self._discard_text_debounce(session_key)
                await self._dispatch_active_session_command(event, session_key, cmd)
            else:
                import logging
                logging.getLogger("gateway.platforms.base").debug(
                    "[%s] Command '/%s' bypassing active-session guard for %s",
                    self.name, cmd, session_key,
                )
                await self._dispatch_inline_reply(event)
        except Exception as exc:
            import logging
            logging.getLogger("gateway.platforms.base").error(
                "[%s] Command '/%s' dispatch failed: %s", self.name, cmd, exc, exc_info=True,
            )
        return True
    if not cmd and event.allow_gateway_control:
        try:
            from tools import clarify_gateway as _clarify_mod
            has_text_clarify = _clarify_mod.get_pending_for_session(
                session_key, include_choice_prompts=True,
            ) is not None
        except (AttributeError, ImportError, KeyError, RuntimeError, TypeError, ValueError):
            has_text_clarify = False
        if has_text_clarify:
            import logging
            logger = logging.getLogger("gateway.platforms.base")
            logger.debug("[%s] Routing message to clarify text-intercept for %s", self.name, session_key)
            try:
                await self._dispatch_inline_reply(event)
            except Exception as exc:
                logger.error("[%s] Clarify text-intercept dispatch failed: %s", self.name, exc, exc_info=True)
            return True
    return False


async def _queue_busy_followup(adapter, event, session_key):
    """Run the profile-aware busy handler and admit the remaining event to its real slot."""
    self = adapter
    if self._busy_session_handler is not None:
        try:
            handled = await self._busy_session_handler(event, session_key)
        except Exception as exc:
            import logging
            logging.getLogger("gateway.platforms.base").error(
                "[%s] Busy-session handler failed: %s", self.name, exc, exc_info=True,
            )
            handled = event._gateway_accepted is True
        if session_key not in self._active_sessions:
            orphan = None
            for owner in self._pending_recovery_owners(event, session_key):
                orphan = owner.get_pending_message(session_key)
                if orphan is not None:
                    owner._start_session_processing(orphan, session_key)
                    break
            if orphan is None and not handled:
                event._gateway_accepted = self._start_session_processing(event, session_key)
                return
        if handled:
            return
    if event.internal and session_key in self._pending_messages:
        return
    if event.message_type == _message_type_photo():
        import logging
        logging.getLogger("gateway.platforms.base").debug(
            "[%s] Queuing photo follow-up for session %s without interrupt", self.name, session_key,
        )
        if not begin_adapter_event(self, event, session_key, "fifo", state="queued"):
            return
        from gateway.platforms.base import merge_pending_message_event
        existing = self._pending_messages.get(session_key)
        merge_pending_message_event(self._pending_messages, session_key, event)
        if existing is not None:
            attach_adapter_event(self, existing, event)
        event._gateway_accepted = True
        return
    if self._is_queue_text_debounce_candidate(event):
        import logging
        logging.getLogger("gateway.platforms.base").debug(
            "[%s] New text message while session %s is active — debouncing follow-up (busy_text_mode=queue, window=%.2fs)",
            self.name, session_key, self._busy_text_debounce_seconds,
        )
        await self._queue_text_debounce(session_key, event)
        return
    import logging
    logging.getLogger("gateway.platforms.base").debug(
        "[%s] New message while session %s is active — queuing follow-up (no interrupt, will cascade after current turn)",
        self.name, session_key,
    )
    if not begin_adapter_event(self, event, session_key, "fifo", state="queued"):
        return
    from gateway.platforms.base import merge_pending_message_event
    existing = self._pending_messages.get(session_key)
    merge_pending_message_event(
        self._pending_messages, session_key, event, merge_text=event.message_type == _message_type_text(),
    )
    if existing is not None:
        attach_adapter_event(self, existing, event)
    event._gateway_accepted = True


def _message_type_photo():
    from gateway.platforms.event import MessageType
    return MessageType.PHOTO


def _message_type_text():
    from gateway.platforms.event import MessageType
    return MessageType.TEXT


async def route_active_message(adapter, event, session_key):
    """Route a busy message through bypass handling, then the durable busy queue."""
    if await _dispatch_busy_bypass(adapter, event, session_key):
        return
    await _queue_busy_followup(adapter, event, session_key)


def requeue_pending_followup(adapter, session_key, pending_event, pending):
    """Keep a depth-capped follow-up in the adapter's real pending slot."""
    if adapter and pending_event:
        from gateway.platforms.base import merge_pending_message_event
        existing = adapter._pending_messages.get(session_key)
        merge_pending_message_event(adapter._pending_messages, session_key, pending_event)
        if existing is not None:
            attach_adapter_event(adapter, existing, pending_event)
    elif adapter and hasattr(adapter, 'queue_message'):
        adapter.queue_message(session_key, pending)
