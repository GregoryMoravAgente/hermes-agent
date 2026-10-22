"""Exact occurrence handles for Base-owned post-delivery callback consumption.

The legacy ``_post_delivery_callbacks`` mapping remains owned by Base for generic
same-session callbacks.  Classic delivery uses this separate occurrence registry so
same-key recursive turns never share a mutable dispatch owner or overwrite one another.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import inspect
import logging
from typing import Any, Awaitable, Callable


logger = logging.getLogger("gateway.platforms.base")


@dataclass(frozen=True)
class _PostDeliveryOutcome:
    """Source-owned outcome visible only while Base consumes one callback occurrence."""

    event: Any
    session_key: str
    processing_ok: bool


_POST_DELIVERY_OUTCOME: ContextVar[_PostDeliveryOutcome | None] = ContextVar(
    "gateway_post_delivery_outcome", default=None,
)


def _post_delivery_outcome() -> _PostDeliveryOutcome | None:
    return _POST_DELIVERY_OUTCOME.get()


@contextmanager
def _post_delivery_outcome_scope(event: Any, session_key: str, processing_ok: bool):
    token = _POST_DELIVERY_OUTCOME.set(
        _PostDeliveryOutcome(
            event=event, session_key=session_key, processing_ok=processing_ok,
        ),
    )
    try:
        yield
    finally:
        _POST_DELIVERY_OUTCOME.reset(token)


def _legacy_occurrence_marker() -> None:
    """Compatibility-only no-op retained in Base's legacy slot while an exact occurrence is live."""


_legacy_occurrence_marker._post_delivery_occurrence_marker = True


def is_legacy_occurrence_marker(callback: Any) -> bool:
    return callback is _legacy_occurrence_marker or bool(
        getattr(callback, "_post_delivery_occurrence_marker", False)
    )


def contains_legacy_occurrence_marker(callback: Any) -> bool:
    if is_legacy_occurrence_marker(callback):
        return True
    chain = getattr(callback, "_post_delivery_callback_chain", None)
    return bool(chain) and any(contains_legacy_occurrence_marker(item) for item in chain)


class PostDeliveryCallbackHandle:
    """Opaque one-use identity for one registered post-delivery occurrence."""

    __slots__ = ("_owner_event", "_registry", "generation", "session_key")

    def __init__(
        self, registry: PostDeliveryCallbackRegistry, session_key: str, generation: int | None,
        owner_event: Any = None,
    ):
        self._registry = registry
        self.session_key = session_key
        self.generation = generation
        self._owner_event = owner_event


class PostDeliveryCallbackContext:
    """Compatibility context passed through Base's existing two-argument fire seam."""

    __slots__ = ("consume_generic", "handle", "interrupt_event")

    def __init__(
        self, interrupt_event: Any, handle: PostDeliveryCallbackHandle, *, consume_generic: bool = False,
    ):
        self.interrupt_event = interrupt_event
        self.handle = handle
        self.consume_generic = bool(consume_generic)

    def __getattr__(self, name: str) -> Any:
        if name == "_classic_execution_owner_event" and self.handle._owner_event is not None:
            return self.handle._owner_event
        return getattr(self.interrupt_event, name)


class PostDeliveryCallbackRegistry:
    """Base-owned registry keyed by an exact occurrence handle."""

    __slots__ = ("_entries",)

    def __init__(self) -> None:
        self._entries: dict[PostDeliveryCallbackHandle, tuple[str, int | None, Callable]] = {}

    @staticmethod
    def _generation(value: Any) -> int | None:
        if value is None:
            return None
        if type(value) is not int:
            raise TypeError("post-delivery generation must be an int or None")
        return value

    def reserve(
        self, session_key: str, generation: Any = None, *, owner_event: Any = None,
    ) -> PostDeliveryCallbackHandle:
        if not isinstance(session_key, str) or not session_key:
            raise ValueError("post-delivery session key is required")
        return PostDeliveryCallbackHandle(
            self, session_key, self._generation(generation), owner_event=owner_event,
        )

    def bind(self, handle: PostDeliveryCallbackHandle, callback: Callable) -> None:
        if not isinstance(handle, PostDeliveryCallbackHandle) or handle._registry is not self:
            raise ValueError("post-delivery handle belongs to another registry")
        if not callable(callback):
            raise TypeError("post-delivery callback must be callable")
        if handle in self._entries:
            raise ValueError("post-delivery handle already bound")
        self._entries[handle] = (handle.session_key, handle.generation, callback)

    def contains(
        self,
        handle: PostDeliveryCallbackHandle,
        *,
        session_key: str | None = None,
        generation: Any = None,
        callback: Callable | None = None,
    ) -> bool:
        entry = self._entries.get(handle) if isinstance(handle, PostDeliveryCallbackHandle) else None
        if entry is None or handle._registry is not self:
            return False
        stored_key, stored_generation, stored_callback = entry
        if session_key is not None and stored_key != session_key:
            return False
        if generation is not None and stored_generation != self._generation(generation):
            return False
        return callback is None or stored_callback is callback

    def pop(
        self,
        handle: PostDeliveryCallbackHandle,
        *,
        session_key: str | None = None,
        generation: Any = None,
    ) -> Callable | None:
        if not self.contains(handle, session_key=session_key, generation=generation):
            return None
        _stored_key, _stored_generation, callback = self._entries.pop(handle)
        return callback

    def discard(self, handle: PostDeliveryCallbackHandle) -> None:
        if isinstance(handle, PostDeliveryCallbackHandle) and handle._registry is self:
            self._entries.pop(handle, None)

    def has_live_for(self, session_key: str, generation: int | None = None) -> bool:
        return any(
            stored_key == session_key
            and (generation is None or stored_generation == generation)
            for stored_key, stored_generation, _callback in self._entries.values()
        )


def ensure_occurrence_registry(adapter: Any) -> PostDeliveryCallbackRegistry:
    registry = getattr(adapter, "_post_delivery_callback_registry", None)
    if not isinstance(registry, PostDeliveryCallbackRegistry):
        registry = PostDeliveryCallbackRegistry()
        adapter._post_delivery_callback_registry = registry
    return registry


def callback_context_for(
    interrupt_event: Any, handle: PostDeliveryCallbackHandle, *, consume_generic: bool = False,
) -> PostDeliveryCallbackContext:
    return PostDeliveryCallbackContext(
        interrupt_event, handle, consume_generic=consume_generic,
    )


def strict_post_delivery_callback_chain(callback: Callable) -> tuple[Callable, ...]:
    """Flatten callback chains without applying their normal exception isolation."""
    chain = getattr(callback, "_post_delivery_callback_chain", None)
    if not isinstance(chain, tuple):
        return (callback,)
    flattened: list[Callable] = []
    for item in chain:
        flattened.extend(strict_post_delivery_callback_chain(item))
    return tuple(flattened)


def unpack_callback_context(value: Any) -> tuple[Any, PostDeliveryCallbackHandle | None]:
    if isinstance(value, PostDeliveryCallbackContext):
        return value.interrupt_event, value.handle
    return value, None


def callback_handle_for_event(event: Any) -> PostDeliveryCallbackHandle | None:
    """Find the terminal occurrence before the outer/owner bridge occurrence."""
    if event is None:
        return None
    terminal = getattr(event, "_classic_terminal_event", None)
    owner = getattr(event, "_classic_execution_owner_event", None)
    bridge = getattr(event, "_classic_execution_bridge", None)
    execution = getattr(bridge, "execution_event", None)
    candidates: list[Any] = []
    for candidate in (terminal, event, owner, execution, bridge):
        if candidate is None or any(candidate is prior for prior in candidates):
            continue
        candidates.append(candidate)
    def _live(handle: Any) -> PostDeliveryCallbackHandle | None:
        if not isinstance(handle, PostDeliveryCallbackHandle):
            return None
        return handle if handle._registry.contains(handle) else None

    for candidate in candidates:
        direct = _live(getattr(candidate, "post_delivery_callback_handle", None))
        if direct is not None:
            return direct
        candidate_bridge = getattr(candidate, "_classic_execution_bridge", None)
        bridge_handle = _live(getattr(candidate_bridge, "post_delivery_callback_handle", None))
        if bridge_handle is not None:
            return bridge_handle
    return None


async def finish_post_delivery_callback(
    adapter: Any, event: Any, session_key: str, interrupt_event: Any, *,
    processing_ok: bool, classic_receipt: Any, thread_metadata: Any,
) -> None:
    """Consume delivery callbacks, then finish custody and always unwind the session.

    The callback is the producer-owned witness boundary.  A failed or cancelled callback
    therefore reaches ``finish_adapter_event`` as ``success=False`` and keeps the existing
    operation UNKNOWN.  Every later cleanup phase is attempted even when an earlier phase
    raises; the first exception is re-raised after all four phases.
    """
    from gateway.platforms.base_busy_custody import finish_adapter_event

    callback_completed = False
    first_error = None
    post_delivery_handle = None
    generic_callback_attempted = False

    try:
        with _post_delivery_outcome_scope(event, session_key, processing_ok):
            post_delivery_handle = callback_handle_for_event(event)
            if post_delivery_handle is None:
                generic_callback_attempted = True
                await adapter._fire_post_delivery_callback(session_key, interrupt_event)
            else:
                await adapter._fire_post_delivery_callback_occurrence(
                    session_key, interrupt_event, post_delivery_handle,
                )
                # Generic callbacks retain their legacy same-key lane after the exact occurrence.
                generic_callback_attempted = True
                await adapter._fire_post_delivery_callback(session_key, interrupt_event)
            callback_completed = True
    except BaseException as exc:  # health: allow BLE001 -- opaque callback cancellation is re-raised after all ownership cleanup phases
        first_error = exc
        if post_delivery_handle is not None and not generic_callback_attempted:
            # The outcome scope has reset before this retry, so a retained I08 callback can only
            # observe the failed exact occurrence and mark its original handle UNKNOWN.
            generic_callback_attempted = True
            try:
                await adapter._fire_post_delivery_callback(session_key, interrupt_event)
            except BaseException:
                logger.exception("post-delivery generic callback cleanup failed")

    try:
        finish_adapter_event(
            adapter, event,
            success=processing_ok and callback_completed,
            reason="" if processing_ok and callback_completed else "join_failed",
            receipt=classic_receipt,
        )
    except BaseException as exc:  # health: allow BLE001 -- opaque custody finalizer failure is deferred until remaining cleanup; first error is re-raised
        if first_error is None:
            first_error = exc

    try:
        await adapter._stop_typing_refresh(
            event.source.chat_id, None, metadata=thread_metadata, stop_attempts=1,
        )
    except BaseException as exc:  # health: allow BLE001 -- opaque typing cleanup failure is deferred until session cleanup; first error is re-raised
        if first_error is None:
            first_error = exc

    try:
        adapter._finish_session_task(session_key, interrupt_event)
    except BaseException as exc:  # health: allow BLE001 -- opaque session cleanup failure is deferred; first error is re-raised
        if first_error is None:
            first_error = exc

    if first_error is not None:
        raise first_error


def _split_post_delivery_entry(entry: Any) -> tuple[int | None, Any]:
    """``(generation, callback)`` from a post-delivery slot; legacy bare callbacks have no
    generation."""
    return entry if isinstance(entry, tuple) and len(entry) == 2 else (None, entry)


class PostDeliveryCallbackMixin:
    """Base-compatible registration and consumption facade for post-delivery callbacks."""

    async def _consume_post_delivery_callbacks(
        self, session_key: str, interrupt_event: asyncio.Event, *, timeout: float,
    ) -> None:
        """Run one generic or exact callback at the real Base delivery boundary."""
        underlying_event, occurrence_handle = unpack_callback_context(interrupt_event)
        consume_generic = (
            isinstance(interrupt_event, PostDeliveryCallbackContext)
            and interrupt_event.consume_generic
        )
        generation = getattr(underlying_event, "_hermes_run_generation", None)
        if occurrence_handle is not None and generation is None:
            generation = occurrence_handle.generation
        _post_cb = self.pop_post_delivery_callback(
            session_key, generation=generation, occurrence_handle=occurrence_handle,
        )
        exact_consumed = occurrence_handle is not None and callable(_post_cb)
        if exact_consumed:
            self._remove_legacy_occurrence_marker(session_key, occurrence_handle)

        async def _run_callback(callback, *, propagate: bool = False) -> None:
            if not callable(callback):
                return
            callback_to_run = callback
            if propagate and getattr(callback, "_post_delivery_callback_chain", None) is not None:
                chain = strict_post_delivery_callback_chain(callback)

                async def _run_chain_without_suppression() -> None:
                    for chained_callback in chain:
                        chained_result = chained_callback()
                        if inspect.isawaitable(chained_result):
                            await chained_result

                callback_to_run = _run_chain_without_suppression
            try:
                callback_result = callback_to_run()
                if inspect.isawaitable(callback_result):
                    await asyncio.wait_for(
                        callback_result, timeout=timeout,
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                if propagate:
                    raise

        await _run_callback(_post_cb, propagate=consume_generic)

        if (
            consume_generic
            and exact_consumed
            and type(generation) is int
            and generation >= 1
            and not ensure_occurrence_registry(self).has_live_for(session_key)
        ):
            generic_callback = self.pop_post_delivery_callback(
                session_key, generation=generation,
            )
            await _run_callback(generic_callback, propagate=True)

    def register_post_delivery_callback(
        self, session_key: str, callback: Callable, *, generation: int | None = None,
        occurrence_handle: PostDeliveryCallbackHandle | None = None,
    ) -> Any:
        """Register a generic or exact post-delivery callback.

        The legacy same-key mapping remains the generic lane.  An occurrence handle selects the
        Base-owned exact lane and never changes that mapping, allowing recursive classic turns to
        share a session key without overwriting or dispatching through mutable owner state.
        """
        if occurrence_handle is not None:
            try:
                ensure_occurrence_registry(self).bind(occurrence_handle, callback)
            except (TypeError, ValueError):
                return None
            return occurrence_handle
        if not session_key or not callable(callback):
            return None
        existing = self._post_delivery_callbacks.get(session_key)
        if existing is not None:
            existing_gen, existing_cb = _split_post_delivery_entry(existing)
            if existing_gen is not None and generation is not None and int(generation) < int(existing_gen):
                return None
            # Same-or-newer generation: chain so both fire in registration order.
            if callable(existing_cb) and (
                existing_gen is None or generation is None or int(existing_gen) == int(generation)):
                callback = self._chain_callbacks(existing_cb, callback)
        self._post_delivery_callbacks[session_key] = (
            callback if generation is None else (int(generation), callback))
        return None

    def register_post_delivery_callback_occurrence(
        self, session_key: str, callback: Callable, *, generation: int | None = None,
        owner_event: Any = None,
    ) -> PostDeliveryCallbackHandle | None:
        """Reserve and bind one callback occurrence in the exact Base-owned registry."""
        if not session_key or not callable(callback):
            return None
        registry = ensure_occurrence_registry(self)
        try:
            handle = registry.reserve(session_key, generation, owner_event=owner_event)
            self.register_post_delivery_callback(
                session_key, callback, generation=generation, occurrence_handle=handle,
            )
        except (TypeError, ValueError):
            return None
        if not registry.contains(
            handle, session_key=session_key, generation=generation, callback=callback,
        ):
            registry.discard(handle)
            return None
        if owner_event is not None:
            try:
                self.register_post_delivery_callback(
                    session_key, _legacy_occurrence_marker, generation=generation,
                )
                entry = self._post_delivery_callbacks.get(session_key)
                marker_present = contains_legacy_occurrence_marker(
                    _split_post_delivery_entry(entry)[1] if entry is not None else None
                )
            except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
                marker_present = False
            if not marker_present:
                registry.discard(handle)
                return None
        return handle

    @staticmethod
    def _chain_callbacks(*callbacks: Callable) -> Callable[[], Awaitable[None]]:
        """Async wrapper running ``callbacks`` in order with per-callback exception isolation;
        async so coroutines returned by async hooks are awaited, not dropped."""
        async def _chained() -> None:
            for _cb in callbacks:
                try:
                    _result = _cb()
                    if inspect.isawaitable(_result):
                        await _result
                except Exception:
                    logger.debug("Post-delivery callback failed", exc_info=True)
        _chained._post_delivery_callback_chain = callbacks
        return _chained

    def has_post_delivery_callback_occurrence_marker(self, session_key: str) -> bool:
        entry = self._post_delivery_callbacks.get(session_key) if session_key else None
        if entry is None:
            return False
        _entry_generation, callback = _split_post_delivery_entry(entry)
        return contains_legacy_occurrence_marker(callback)

    @staticmethod
    def _without_legacy_occurrence_marker(callback: Callable):
        chain = getattr(callback, "_post_delivery_callback_chain", None)
        if chain is None:
            return [] if is_legacy_occurrence_marker(callback) else [callback]
        remaining = []
        for item in chain:
            remaining.extend(BasePlatformAdapter._without_legacy_occurrence_marker(item))
        return remaining

    def _remove_legacy_occurrence_marker(
        self, session_key: str, handle: PostDeliveryCallbackHandle,
    ) -> None:
        registry = ensure_occurrence_registry(self)
        if registry.has_live_for(session_key):
            return
        entry = self._post_delivery_callbacks.get(session_key)
        if entry is None:
            return
        entry_generation, callback = _split_post_delivery_entry(entry)
        remaining = self._without_legacy_occurrence_marker(callback)
        if not remaining:
            self._post_delivery_callbacks.pop(session_key, None)
        elif len(remaining) == 1:
            self._post_delivery_callbacks[session_key] = (
                remaining[0] if entry_generation is None else (entry_generation, remaining[0])
            )
        else:
            chained = self._chain_callbacks(*remaining)
            self._post_delivery_callbacks[session_key] = (
                chained if entry_generation is None else (entry_generation, chained)
            )

    def pop_post_delivery_callback(
        self, session_key: str, *, generation: int | None = None,
        occurrence_handle: PostDeliveryCallbackHandle | None = None,
    ) -> Callable | None:
        """Pop a generic callback or one exact occurrence, optionally by generation."""
        if occurrence_handle is not None:
            registry = ensure_occurrence_registry(self)
            return registry.pop(
                occurrence_handle, session_key=session_key or None, generation=generation,
            )
        entry = self._post_delivery_callbacks.get(session_key) if session_key else None
        if entry is None:
            return None
        entry_generation, callback = _split_post_delivery_entry(entry)
        if generation is not None and (entry_generation is None or int(entry_generation) != int(generation)):
            return None
        self._post_delivery_callbacks.pop(session_key, None)
        return callback if callable(callback) else None

    def pop_post_delivery_callback_occurrence(
        self, handle: PostDeliveryCallbackHandle, *, session_key: str | None = None,
        generation: int | None = None,
    ) -> Callable | None:
        """Consume one exact callback handle without consulting the generic session slot."""
        return self.pop_post_delivery_callback(
            session_key or getattr(handle, 'session_key', ''),
            generation=generation,
            occurrence_handle=handle,
        )

    def has_post_delivery_callback_occurrence(
        self, handle: PostDeliveryCallbackHandle, *, session_key: str | None = None,
        generation: int | None = None, callback: Callable | None = None,
    ) -> bool:
        """Return whether an exact callback is still live without consuming it."""
        try:
            return ensure_occurrence_registry(self).contains(
                handle, session_key=session_key or None, generation=generation, callback=callback,
            )
        except (TypeError, ValueError):
            return False


# The moved recursive helper retains its original qualified-name lookup.
BasePlatformAdapter = PostDeliveryCallbackMixin
