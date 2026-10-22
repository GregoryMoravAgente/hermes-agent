"""Classic busy state remains part of the real I06 handoff boundary.

These tests use the real Discord adapter/FIFO implementation and the canonical owner fixture. They
keep the production quiescence decision visible: a drained preservation map cannot claim readiness
while a classic slot, bounded overflow, active loop, or text debounce still owns work.
"""

from pathlib import Path

import pytest


def _real_classic_runner():
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.run import GatewayRunner
    from gateway.session import SessionSource, build_session_key
    from plugins.platforms.discord.adapter import DiscordAdapter

    runner = GatewayRunner.__new__(GatewayRunner)
    runner.config = GatewayConfig()
    runner.adapters = {}
    runner._profile_adapters = {}
    runner._profile_failed_platforms = {}
    runner._primary_profile_name = "default"
    runner._sessions = {}
    adapter = DiscordAdapter(
        PlatformConfig(enabled=True, token="fixture-token", typing_indicator=False),
    )
    adapter.gateway_runner = runner
    runner.adapters = {Platform.DISCORD: adapter}
    source = SessionSource(
        platform=Platform.DISCORD, chat_id="42", chat_type="dm", user_id="42",
    )
    return runner, adapter, source, build_session_key(source)


@pytest.mark.asyncio
async def test_preservation_readiness_refuses_a_real_classic_slot_with_debounce(owner):
    """A running classic agent and a buffered queue-mode text are not quiescent."""
    from gateway.platforms.event import MessageEvent

    runner, adapter, source, session_key = _real_classic_runner()
    runner._session_state(session_key).turn.agent = object()
    assert runner._is_session_running(session_key)

    adapter._busy_text_mode = "queue"
    await adapter._queue_text_debounce(
        session_key,
        MessageEvent(text="queued while busy", source=source, message_id="busy-1"),
    )
    assert session_key in adapter._text_debounce

    owner.runner = runner
    owner._busy_inbound_seen = True
    try:
        # Current R2 is RED here: preservation_owner_ready does not consume the classic slot or
        # adapter debounce state before reporting I06 quiescence.
        assert owner.preservation_owner_ready("I06_busy_followup_delivery") is False
    finally:
        adapter._discard_text_debounce(session_key)


def test_classic_running_slot_preserves_fifo_head_and_bounded_overflow():
    """The real busy FIFO keeps every admitted item through its hard cap and drops only excess."""
    from gateway.platforms.event import MessageEvent

    runner, adapter, source, session_key = _real_classic_runner()
    runner._session_state(session_key).turn.agent = object()
    capacity = runner._BUSY_QUEUE_MAX_PENDING

    for index in range(capacity + 2):
        runner._queue_or_replace_pending_event(
            session_key,
            MessageEvent(
                text=f"follow-up-{index}", source=source, message_id=f"follow-up-{index}",
            ),
        )

    assert runner._is_session_running(session_key)
    assert runner._queue_depth(session_key, adapter=adapter) == capacity
    assert adapter._pending_messages[session_key].text == "follow-up-0"
    assert [event.text for event in runner._overflow_queue(session_key)] == [
        f"follow-up-{index}" for index in range(1, capacity)
    ]
    assert "follow-up-" + str(capacity) not in {
        event.text for event in runner._overflow_queue(session_key)
    }


def test_active_loop_state_is_not_classic_quiescence(owner, monkeypatch):
    """A durable active LoopState keeps the I06 owner open until its wake settles."""
    from hermes_cli import goals, loops

    home = Path(owner.db.db_path).parent.resolve()
    monkeypatch.setenv("HERMES_HOME", str(home))
    goals._DB_CACHE.clear()
    manager = loops.LoopManager("classic-loop-session")
    manager.set("watch the queue", interval_seconds=30, route={"platform": "api_server"})
    owner._busy_inbound_seen = True
    try:
        assert manager.is_active()
        # Current R2 is RED here: owner readiness only inspects admission/local maps and does not
        # consume the persisted LoopState/active-loop watcher as a classic producer.
        assert owner.preservation_owner_ready("I06_busy_followup_delivery") is False
    finally:
        manager.clear()
        goals._DB_CACHE.clear()
