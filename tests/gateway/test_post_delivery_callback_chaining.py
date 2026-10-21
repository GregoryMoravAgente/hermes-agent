"""Tests for ``BasePlatformAdapter.register_post_delivery_callback`` chaining.

When two features want to run after the final response lands on the same
session (e.g. background-review release + temporary-progress cleanup), the
registration API chains them rather than clobbering. Per-callback
exceptions are swallowed so one bad callback can't sabotage the others.
Stale-generation registrations are rejected.

The chained wrapper is ``async`` so it transparently supports sync or async
callbacks — the outer invoker in ``_handle_message`` awaits awaitable
callbacks, and a sync wrapper would silently drop coroutine results from
async callbacks chained behind it.
"""
import asyncio
import inspect

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult


class _MinAdapter(BasePlatformAdapter):
    async def connect(self, *, is_reconnect: bool = False) -> bool:
        return True

    async def disconnect(self) -> None:
        return None

    async def send(self, chat_id, content, reply_to=None, metadata=None) -> SendResult:
        return SendResult(success=True, message_id="1")

    async def get_chat_info(self, chat_id):
        return {"id": chat_id}


@pytest.fixture
def adapter():
    return _MinAdapter(PlatformConfig(enabled=True), Platform.TELEGRAM)


def _invoke(cb):
    """Invoke a popped callback, awaiting if it returns a coroutine.

    Single-registration callbacks are returned as the raw user callable
    (sync). Chained callbacks (two or more registrations on the same
    session) are wrapped in an async helper. Tests use this helper so
    they don't have to care which case they're exercising.
    """
    result = cb()
    if inspect.isawaitable(result):
        asyncio.run(result)


class TestPostDeliveryCallbackChaining:
    def test_single_callback_fires(self, adapter):
        fired = []
        adapter.register_post_delivery_callback("s", lambda: fired.append("A"))
        cb = adapter.pop_post_delivery_callback("s")
        _invoke(cb)
        assert fired == ["A"]

    def test_two_callbacks_chain_in_order(self, adapter):
        fired = []
        adapter.register_post_delivery_callback("s", lambda: fired.append("A"))
        adapter.register_post_delivery_callback("s", lambda: fired.append("B"))
        cb = adapter.pop_post_delivery_callback("s")
        _invoke(cb)
        assert fired == ["A", "B"]

    def test_three_callbacks_chain_in_order(self, adapter):
        """Chain composes over an already-chained callback."""
        fired = []
        for label in ("A", "B", "C"):
            adapter.register_post_delivery_callback(
                "s", lambda x=label: fired.append(x)
            )
        cb = adapter.pop_post_delivery_callback("s")
        _invoke(cb)
        assert fired == ["A", "B", "C"]


class TestPostDeliveryCallbackAsyncChaining:
    """When an async callback is chained, the wrapper must await it.

    Regression test for a bug where the sync ``_chained`` wrapper called
    async callbacks without awaiting, silently dropping the returned
    coroutine. This broke ``/goal`` continuations (Discord etc.) where
    the continuation injection is an async ``_deliver()`` coroutine.
    """

    def test_async_callback_in_chain_is_awaited(self, adapter):
        fired = []

        async def async_cb():
            await asyncio.sleep(0)
            fired.append("async")

        adapter.register_post_delivery_callback("s", lambda: fired.append("sync"))
        adapter.register_post_delivery_callback("s", async_cb)
        cb = adapter.pop_post_delivery_callback("s")
        _invoke(cb)
        assert fired == ["sync", "async"]



class TestPostDeliveryCallbackOccurrences:
    def test_same_key_occurrences_have_independent_exact_handles(self, adapter):
        fired = []
        first = adapter.register_post_delivery_callback_occurrence(
            "s", lambda: fired.append("first"), generation=7,
        )
        second = adapter.register_post_delivery_callback_occurrence(
            "s", lambda: fired.append("second"), generation=7,
        )
        assert first is not None and second is not None and first is not second
        _invoke(adapter.pop_post_delivery_callback("s", generation=7, occurrence_handle=first))
        assert fired == ["first"]
        _invoke(adapter.pop_post_delivery_callback("s", generation=7, occurrence_handle=second))
        assert fired == ["first", "second"]

    def test_wrong_generation_or_foreign_handle_cannot_consume(self, adapter):
        fired = []
        handle = adapter.register_post_delivery_callback_occurrence(
            "s", lambda: fired.append("exact"), generation=3,
        )
        assert handle is not None
        assert adapter.pop_post_delivery_callback(
            "s", generation=4, occurrence_handle=handle,
        ) is None
        other = _MinAdapter(PlatformConfig(enabled=True), Platform.TELEGRAM)
        assert other.pop_post_delivery_callback(
            "s", generation=3, occurrence_handle=handle,
        ) is None
        assert adapter.has_post_delivery_callback_occurrence(
            handle, session_key="s", generation=3,
        )
        _invoke(adapter.pop_post_delivery_callback("s", generation=3, occurrence_handle=handle))
        assert fired == ["exact"]

    def test_exact_occurrence_does_not_consume_generic_same_key_callback(self, adapter):
        fired = []
        adapter.register_post_delivery_callback("s", lambda: fired.append("generic"), generation=5)
        handle = adapter.register_post_delivery_callback_occurrence(
            "s", lambda: fired.append("exact"), generation=5,
        )
        assert handle is not None
        _invoke(adapter.pop_post_delivery_callback("s", generation=5, occurrence_handle=handle))
        assert fired == ["exact"]
        _invoke(adapter.pop_post_delivery_callback("s", generation=5))
        assert fired == ["exact", "generic"]


class TestPostDeliveryCallbackAuthorityOptIn:
    @pytest.mark.asyncio
    async def test_opt_in_consumes_exact_and_current_generation_generic_once(self, adapter):
        fired = []

        async def generic():
            fired.append("generic")

        adapter.register_post_delivery_callback("s", generic, generation=7)
        handle = adapter.register_post_delivery_callback_occurrence(
            "s", lambda: fired.append("exact"), generation=7, owner_event=object(),
        )
        assert handle is not None
        event = asyncio.Event()
        event._hermes_run_generation = 7

        await adapter._fire_post_delivery_callback_occurrence(
            "s", event, handle, consume_generic=True,
        )

        assert fired == ["exact", "generic"]
        assert "s" not in adapter._post_delivery_callbacks
        assert not adapter.has_post_delivery_callback_occurrence(handle)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure", (RuntimeError("callback failure"), TimeoutError()),
    )
    async def test_opt_in_propagates_generic_failure(self, adapter, failure):
        async def generic():
            raise failure

        adapter.register_post_delivery_callback("s", generic, generation=7)
        handle = adapter.register_post_delivery_callback_occurrence(
            "s", lambda: None, generation=7, owner_event=object(),
        )
        assert handle is not None
        event = asyncio.Event()
        event._hermes_run_generation = 7

        with pytest.raises(type(failure)):
            await adapter._fire_post_delivery_callback_occurrence(
                "s", event, handle, consume_generic=True,
            )
        assert "s" not in adapter._post_delivery_callbacks

    @pytest.mark.asyncio
    async def test_opt_in_propagates_generic_cancellation(self, adapter):
        async def generic():
            raise asyncio.CancelledError()

        adapter.register_post_delivery_callback("s", generic, generation=7)
        handle = adapter.register_post_delivery_callback_occurrence(
            "s", lambda: None, generation=7, owner_event=object(),
        )
        assert handle is not None
        event = asyncio.Event()
        event._hermes_run_generation = 7

        with pytest.raises(asyncio.CancelledError):
            await adapter._fire_post_delivery_callback_occurrence(
                "s", event, handle, consume_generic=True,
            )

    @pytest.mark.asyncio
    async def test_opt_in_preserves_foreign_and_newer_generation_generic(self, adapter):
        fired = []
        handle = adapter.register_post_delivery_callback_occurrence(
            "s", lambda: fired.append("exact"), generation=7, owner_event=object(),
        )
        assert handle is not None
        adapter.register_post_delivery_callback(
            "s", lambda: fired.append("newer"), generation=8,
        )
        event = asyncio.Event()
        event._hermes_run_generation = 7

        await adapter._fire_post_delivery_callback_occurrence(
            "s", event, handle, consume_generic=True,
        )

        assert fired == ["exact"]
        assert "s" in adapter._post_delivery_callbacks
        _invoke(adapter.pop_post_delivery_callback("s", generation=8))
        assert fired == ["exact", "newer"]

    @pytest.mark.asyncio
    async def test_opt_in_rejects_foreign_occurrence_without_touching_generic(self, adapter):
        fired = []
        adapter.register_post_delivery_callback(
            "s", lambda: fired.append("generic"), generation=7,
        )
        foreign_adapter = _MinAdapter(PlatformConfig(enabled=True), Platform.TELEGRAM)
        foreign_handle = foreign_adapter.register_post_delivery_callback_occurrence(
            "s", lambda: fired.append("foreign"), generation=7, owner_event=object(),
        )
        assert foreign_handle is not None
        event = asyncio.Event()
        event._hermes_run_generation = 7

        await adapter._fire_post_delivery_callback_occurrence(
            "s", event, foreign_handle, consume_generic=True,
        )

        assert fired == []
        assert "s" in adapter._post_delivery_callbacks
        assert foreign_adapter.has_post_delivery_callback_occurrence(foreign_handle)

    @pytest.mark.asyncio
    async def test_default_occurrence_keeps_generic_for_normal_base_finalizer(self, adapter):
        fired = []
        adapter.register_post_delivery_callback(
            "s", lambda: fired.append("generic"), generation=7,
        )
        handle = adapter.register_post_delivery_callback_occurrence(
            "s", lambda: fired.append("exact"), generation=7, owner_event=object(),
        )
        assert handle is not None
        event = asyncio.Event()
        event._hermes_run_generation = 7

        await adapter._fire_post_delivery_callback_occurrence("s", event, handle)
        assert fired == ["exact"]
        assert "s" in adapter._post_delivery_callbacks

        await adapter._fire_post_delivery_callback("s", event)
        assert fired == ["exact", "generic"]
        assert "s" not in adapter._post_delivery_callbacks

    @pytest.mark.asyncio
    async def test_opt_in_preserves_generic_while_another_exact_is_live(self, adapter):
        fired = []
        adapter.register_post_delivery_callback(
            "s", lambda: fired.append("generic"), generation=7,
        )
        first = adapter.register_post_delivery_callback_occurrence(
            "s", lambda: fired.append("first"), generation=7, owner_event=object(),
        )
        second = adapter.register_post_delivery_callback_occurrence(
            "s", lambda: fired.append("second"), generation=7, owner_event=object(),
        )
        assert first is not None and second is not None
        event = asyncio.Event()
        event._hermes_run_generation = 7

        await adapter._fire_post_delivery_callback_occurrence(
            "s", event, first, consume_generic=True,
        )

        assert fired == ["first"]
        assert "s" in adapter._post_delivery_callbacks
        assert adapter.has_post_delivery_callback_occurrence(second)

        await adapter._fire_post_delivery_callback_occurrence(
            "s", event, second, consume_generic=True,
        )
        assert fired == ["first", "second", "generic"]
        assert "s" not in adapter._post_delivery_callbacks
