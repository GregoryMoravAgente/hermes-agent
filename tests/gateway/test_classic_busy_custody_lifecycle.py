"""Real classic producer custody controls.

The tests use the shared ``owner`` SessionAuthority fixture and its real SQLite/session routing
store.  The coordinator is the only lease authority; the event objects exercise the same source
identity and occurrence binding used by the adapter bridges.
"""
from copy import copy

import pytest


def _install_custody(owner):
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_cli.backend_retirement import RetirementFence

    coordinator = RuntimeCustodyCoordinator(
        process_instance=owner.instance_id,
        authority_epoch=owner.epoch,
        fence=RetirementFence(),
    )
    owner.runner.runtime_custody = coordinator
    return coordinator


def _event(owner, message_id):
    from gateway.platforms.event import MessageEvent
    from gateway.session import SessionSource, build_session_key
    from gateway.config import Platform

    source = SessionSource(
        platform=Platform.DISCORD, chat_id="classic-chat", chat_type="dm", user_id="classic-user",
    )
    key = build_session_key(source)
    owner.runner.session_store.get_or_create_session(source, touch_activity=False)
    return MessageEvent(text="queued", source=source, message_id=message_id), key


def test_distinct_classic_occurrences_keep_separate_handles_and_retry_same_owner(owner):
    coordinator = _install_custody(owner)
    first, session_key = _event(owner, "classic-1")
    second, _ = _event(owner, "classic-2")

    assert owner._classic_begin(first, session_key, "fifo")
    assert owner._classic_begin(second, session_key, "fifo")
    first_key = first._classic_custody_key
    second_key = second._classic_custody_key
    assert first_key != second_key
    assert coordinator.status()["active_owner_classes"] == ["I06_busy_followup_delivery"]

    owner._classic_finish(first, success=True)
    owner._classic_finish(second, success=False, reason="join_failed")
    assert second_key in owner._preservation_operations
    assert owner._preservation_operations[second_key]["unknown"] is True

    # The same source object can reconcile the retained opaque handle. A copied request cannot.
    copied_second = copy(second)
    assert owner._classic_begin(second, session_key, "fifo")
    assert owner._classic_begin(copied_second, session_key, "fifo") is False
    owner._classic_finish(second, success=True)
    assert coordinator.status()["active_owner_classes"] == ["I06_busy_followup_delivery"]
    assert owner._preservation_operations[second_key]["unknown"] is True


def test_classic_foreign_occurrence_cannot_reuse_retained_handle(owner):
    _install_custody(owner)
    first, session_key = _event(owner, "classic-stale")
    foreign, _ = _event(owner, "classic-foreign")
    assert owner._classic_begin(first, session_key, "running-slot")
    owner._classic_finish(first, success=False, reason="join_failed")

    # A different occurrence must receive its own lease, even when its text/source are identical.
    assert owner._classic_begin(foreign, session_key, "running-slot")
    assert foreign._classic_custody_key != first._classic_custody_key
    owner._classic_finish(foreign, success=False, reason="join_failed")


@pytest.mark.asyncio
async def test_debounce_custody_follows_merge_and_cancel(tmp_path):
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.event import MessageEvent
    from plugins.platforms.discord.adapter import DiscordAdapter
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_cli.backend_retirement import RetirementFence

    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="fixture", typing_indicator=False))
    adapter.gateway_runner = runner
    runner.adapters = {Platform.DISCORD: adapter}
    authority = await initialize_session_authority(runner, profile_id="default", instance_id="classic")
    runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id, authority_epoch=authority.epoch, fence=RetirementFence(),
    )
    runner._wire_adapter_handlers(adapter, busy_text_mode="queue")
    source = adapter.build_source(chat_id="debounce", chat_type="dm", user_id="u")
    session_key = runner.session_store._generate_session_key(source)
    runner.session_store.get_or_create_session(source, touch_activity=False)
    try:
        first = MessageEvent(text="one", source=source, message_id="debounce-1")
        second = MessageEvent(text="two", source=source, message_id="debounce-2")
        await adapter._queue_text_debounce(session_key, first)
        await adapter._queue_text_debounce(session_key, second)
        assert len(first._classic_custody_events) == 2
        adapter._discard_text_debounce(session_key)
        assert any(
            record.get("unknown") is True
            for record in authority._preservation_operations.values()
        )
    finally:
        authority.db.close()
