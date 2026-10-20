"""A control acknowledgement must not release its still-live prompt producer."""

import asyncio
from types import SimpleNamespace

import pytest

from tests.gateway.test_preservation_admission_consumers import _install_custody


async def _approval_prompt(api, owner, *, request_id, adapter=None):
    from gateway.run_turn_runner import TurnRunner
    from gateway.session_api_turn import admit_api_turn
    from tools.approval import _gateway_queues
    from tools.approval_gateway_wait import _ApprovalEntry

    coordinator = _install_custody(owner)
    _, ref, row = admit_api_turn(
        api, session_id=request_id, request_id=request_id,
        user_message="prompt lifecycle", conversation_history=[])
    started, _claimed = await owner._claim_next(ref, owner.sessions[ref.session_id])
    route = owner.sessions[ref.session_id].route
    prompt_id = request_id + "-approval"
    data = {"request_id": prompt_id, "command": "echo synthetic", "description": "test"}
    entry = _ApprovalEntry(data)
    _gateway_queues.setdefault(route, []).append(entry)
    turn = object.__new__(TurnRunner)
    turn._approval_owner = (owner, ref.session_id, started["generation"])
    turn._ctx = SimpleNamespace(
        session_key=route, _run_still_current=lambda: True,
        _status_adapter=adapter, _status_chat_id="synthetic-chat",
        _status_thread_metadata={}, _loop_for_step=asyncio.get_running_loop())
    # The transport has finished sending. Keep the real producer registration, control,
    # approval queue and settle callback; no provider or external destination is contacted.
    turn._render_approval_sync = lambda _data: ("echo synthetic", None)
    turn._approval_notify_sync(data)
    record = next(r for r in owner._preservation_operations.values()
                  if r["owner_class"] == "I05_controls_and_unknown")
    return coordinator, ref, row, started, prompt_id, entry, record


@pytest.mark.asyncio
async def test_api_answer_ack_keeps_prompt_producer_until_real_settle(api, owner):
    from gateway.platforms.api_server_authority_runs import respond_run
    from tools.approval import _gateway_queues

    coordinator, ref, row, started, prompt_id, entry, record = await _approval_prompt(
        api, owner, request_id="prompt-answer-lifetime")
    handle = record["handle"]
    try:
        await respond_run(api, row["request_id"], {
            "request_id": prompt_id, "execution_generation": started["generation"],
            "choice": "once"}, kind="approval")
        # The queue's waiter has not consumed the answer or run its settle callback yet.
        assert entry.result is not None
        assert any(r["handle"] is handle for r in owner._preservation_operations.values())
        assert coordinator._records[handle.lease.lease_id].state == "accepted"
        entry.settle("resolved")
        assert not any(r["handle"] is handle for r in owner._preservation_operations.values())
    finally:
        _gateway_queues.pop(owner.sessions[ref.session_id].route, None)


@pytest.mark.asyncio
async def test_timeout_notice_keeps_prompt_until_actual_send_finishes(api, owner):
    from tools.approval import _gateway_queues

    entered = asyncio.Event()
    release = asyncio.Event()
    sent = asyncio.Event()

    class Adapter:
        async def send(self, *_args, **_kwargs):
            entered.set()
            await release.wait()
            sent.set()

    coordinator, ref, _row, _started, _prompt_id, entry, record = await _approval_prompt(
        api, owner, request_id="prompt-timeout-lifetime", adapter=Adapter())
    handle = record["handle"]
    try:
        entry.settle("timeout")
        await asyncio.wait_for(entered.wait(), timeout=3)
        assert any(r["handle"] is handle for r in owner._preservation_operations.values())
        assert coordinator._records[handle.lease.lease_id].state == "accepted"
        release.set()
        await asyncio.wait_for(sent.wait(), timeout=3)
        from tests.waiting import async_wait_until
        await async_wait_until(
            lambda: not any(r["handle"] is handle for r in owner._preservation_operations.values()),
            timeout=3)
    finally:
        release.set()
        _gateway_queues.pop(owner.sessions[ref.session_id].route, None)


@pytest.mark.asyncio
async def test_timeout_notice_late_failure_retains_prompt_as_unknown(api, owner):
    from tools.approval import _gateway_queues

    entered = asyncio.Event()
    release = asyncio.Event()
    failed = asyncio.Event()

    class Adapter:
        async def send(self, *_args, **_kwargs):
            entered.set()
            await release.wait()
            failed.set()
            raise RuntimeError("synthetic late send failure")

    coordinator, ref, _row, _started, _prompt_id, entry, record = await _approval_prompt(
        api, owner, request_id="prompt-timeout-late-failure", adapter=Adapter())
    handle = record["handle"]
    try:
        entry.settle("timeout")
        await asyncio.wait_for(entered.wait(), timeout=3)
        assert coordinator._records[handle.lease.lease_id].state == "accepted"
        release.set()
        await asyncio.wait_for(failed.wait(), timeout=3)
        from tests.waiting import async_wait_until
        await async_wait_until(
            lambda: coordinator._records[handle.lease.lease_id].state == "unknown",
            timeout=3)
        assert any(r["handle"] is handle for r in owner._preservation_operations.values())
    finally:
        release.set()
        _gateway_queues.pop(owner.sessions[ref.session_id].route, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "refused", "raised"])
async def test_clarify_producer_tracks_real_card_retirement_future(api, owner, outcome):
    from gateway.platforms.base import SendResult
    from gateway.run_turn_runner import TurnRunner
    from gateway.session_api_turn import admit_api_turn
    from tests.waiting import async_wait_until
    from tools import clarify_gateway

    entered = asyncio.Event()
    release = asyncio.Event()

    class Adapter:
        async def retire_clarify_card(self, _clarify_id, _notice):
            entered.set()
            await release.wait()
            if outcome == "raised":
                raise RuntimeError("synthetic card retirement failure")
            return SendResult(success=outcome == "success")

    coordinator = _install_custody(owner)
    name = "clarify-retirement-" + outcome
    _, ref, _row = admit_api_turn(
        api, session_id=name, request_id=name, user_message="clarify lifetime",
        conversation_history=[])
    started, _claimed = await owner._claim_next(ref, owner.sessions[ref.session_id])
    route = owner.sessions[ref.session_id].route
    turn = object.__new__(TurnRunner)
    turn._approval_owner = (owner, ref.session_id, started["generation"])
    turn._ctx = SimpleNamespace(
        session_key=route, _status_adapter=Adapter(),
        _loop_for_step=asyncio.get_running_loop())
    turn._register_i05_clarify(
        clarify_gateway, clarify_id=name, session_key=route,
        question="Synthetic choice?", choices=["yes", "no"], multi_select=False)
    record = next(r for r in owner._preservation_operations.values()
                  if r["owner_class"] == "I05_controls_and_unknown")
    handle = record["handle"]
    try:
        turn._finish_i05_clarify(
            clarify_id=name, response="[user did not respond]", answered=False,
            rearm=False, ctx=turn._ctx)
        await asyncio.wait_for(entered.wait(), timeout=3)
        assert any(r["handle"] is handle for r in owner._preservation_operations.values())
        assert coordinator._records[handle.lease.lease_id].state == "accepted"
        release.set()
        if outcome == "success":
            await async_wait_until(
                lambda: not any(r["handle"] is handle for r in owner._preservation_operations.values()),
                timeout=3)
        else:
            await async_wait_until(
                lambda: coordinator._records[handle.lease.lease_id].state == "unknown",
                timeout=3)
            assert any(r["handle"] is handle for r in owner._preservation_operations.values())
    finally:
        release.set()
        clarify_gateway.clear_session(route)
