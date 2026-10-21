"""A detached API result must have an addressable consumer and one durable row."""
import asyncio
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.platforms.api_server import APIServerAdapter
from gateway.session_context import clear_session_vars
from gateway.wake import persist_delegation_delivery
from hermes_state import SessionDB
from tests.gateway.conftest import runtime_owned_api
from tools.delegate_tool_dispatch import _resolve_async_wake_sid


@pytest.mark.asyncio
async def test_detached_dispatch_requires_a_declared_consumer(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setenv("HERMES_SESSION_HISTORY_DELIVERY", "1")
    for capability in (None, "", "1"):
        kw = dict(chat_id="api-parent", session_id="api-parent")
        import inspect
        if capability is not None and "session_history_delivery" in inspect.signature(APIServerAdapter._bind_api_server_session).parameters:
            kw["session_history_delivery"] = capability
        tokens = APIServerAdapter._bind_api_server_session(**kw)
        try:
            args = ["api-parent"]
            if len(inspect.signature(_resolve_async_wake_sid).parameters) > 1:
                from gateway.session_context import session_history_delivery_supported
                args.append(session_history_delivery_supported())
            target = _resolve_async_wake_sid(*args)
            assert target == ("api-parent" if capability == "1" else None)
        finally:
            clear_session_vars(tokens)
    from evals.api_delegation_http_probe import probe
    result = await probe()
    for request in result["requests"]:
        assert request["status"] == 200
        runtime = request["runtime"]
        assert runtime["target"] == ("child" if request["explicit"] else None)
        if request["explicit"]:
            assert runtime["session_id"] == "child"
            assert request["header"] == "parent"
    assert result["unsolicited_calls"] == 0
    assert result["durable_child_rows"] == 1
    raw_delivery_rows = [
        message for message in result["session_run_raw_history"]
        if message.get("content") == "DELIVERY_RESULT"
    ]
    assert len(raw_delivery_rows) == 1
    raw_delivery = raw_delivery_rows[0]
    assert raw_delivery["role"] == "user"
    assert raw_delivery["display_kind"] == "async_delegation_complete"
    assert raw_delivery["display_metadata"] == {
        "delegation_id": "unit-http",
        "task_count": 1,
        "completed_count": 1,
        "failed_count": 0,
    }
    delivery_uid = raw_delivery["message_uid"]
    assert sum(message.get("role") == "session_meta" for message in result["session_run_raw_history"]) == 1
    assert sum(m["content"] == "DELIVERY_RESULT" for m in result["resumed_history"]) == 1
    runs = {r["name"]: r for r in result["runs"]}
    assert all(r["status"] == 202 for r in runs.values())
    for name in ("caller_history", "response_chain"):
        assert runs[name]["runtime"]["target"] is None
        assert runs[name]["runtime"]["history"] == [{"role": "user", "content": "caller snapshot"}]
    assert runs["session"]["runtime"]["target"] == "child"
    assert runs["session"]["runtime"]["history"] == result["resumed_history"]
    runtime_delivery_rows = [
        message for message in runs["session"]["runtime"]["history"]
        if message.get("content") == raw_delivery["content"]
    ]
    assert len(runtime_delivery_rows) == 1
    assert runtime_delivery_rows[0]["message_uid"] == delivery_uid
    assert sum(message.get("message_uid") == delivery_uid for message in runs["session"]["runtime"]["history"]) == 1
    substantive_raw_rows = [
        (message.get("role"), message.get("content"), message.get("message_uid"))
        for message in result["session_run_raw_history"]
        if message.get("role") in {"user", "assistant"}
    ]
    substantive_expected_rows = [
        (message.get("role"), message.get("content"), message.get("message_uid"))
        for message in result["session_run_expected_history"]
        if message.get("role") in {"user", "assistant"}
    ]
    assert substantive_expected_rows == substantive_raw_rows
    assert [
        (message.get("role"), message.get("content"), message.get("message_uid"))
        for message in runs["session"]["runtime"]["history"]
        if message.get("role") in {"user", "assistant"}
    ] == substantive_expected_rows
    assert runs["declared_key"]["runtime"]["target"] == "declared"
    assert result["session_run_resolved_session_id"] == "child"
    assert result["session_run_task_count"] > 0
    assert result["session_run_task_joined"] is True
    assert runs["session"]["runtime"]["history"] == result["session_run_expected_history"]
    chat_history = result["chat_resumed_history"]
    assert isinstance(chat_history, list)
    assert sum("DELIVERY_RESULT" in str(message.get("content", "")) for message in chat_history) == 1
    chat_delivery_rows = [
        message for message in chat_history
        if raw_delivery["content"] in str(message.get("content", ""))
    ]
    assert len(chat_delivery_rows) == 1
    assert chat_delivery_rows[0]["message_uid"] == delivery_uid
    assert sum(message.get("message_uid") == delivery_uid for message in chat_history) == 1
    accepted_session_rows = [
        index for index, message in enumerate(chat_history)
        if message.get("role") == "user"
        and result["session_run_input"] in str(message.get("content", ""))
    ]
    assert len(accepted_session_rows) == 1
    assert any(
        message.get("role") == "assistant"
        and message.get("content") == result["session_run_reply"]
        for message in chat_history[accepted_session_rows[0] + 1:]
    )
    assert runs["declared_key"]["runtime"]["history"][0]["content"] == "DECLARED_HISTORY"


@pytest.mark.asyncio
async def test_delivery_replay_is_atomic_across_continuation_and_busy_turn(tmp_path):
    db = SessionDB(db_path=tmp_path / "state.db")
    peer = SessionDB(db_path=tmp_path / "state.db")
    try:
        db.create_session("parent", source="api_server")
        db.create_session("other", source="api_server")
        adapters = [SimpleNamespace(_ensure_session_db=lambda: db), SimpleNamespace(_ensure_session_db=lambda: peer)]
        evt = {"type": "async_delegation", "delegation_id": "unique-unit"}
        async def send(adapter, event=evt):
            await persist_delegation_delivery(adapter, text="RESULT", session_id="parent", evt=event)
        await asyncio.gather(*(send(a) for a in adapters))
        assert len(db.get_messages("parent")) == 1
        db.end_session("parent", "compression")
        db.create_session("child", source="api_server", parent_session_id="parent")
        await send(adapters[0])
        assert db.get_messages("child") == []  # old event was already recorded in the lineage
        from hermes_state_errors import SessionTurnLeaseLostError
        assert db.acquire_session_turn_lease("child", "client-turn", wait_seconds=0)
        later = {**evt, "delegation_id": "later-unit"}
        try:
            with pytest.raises(SessionTurnLeaseLostError):
                await send(adapters[0], later)
            assert db.get_messages("child") == []
        finally:
            db.release_session_turn_lease("child", "client-turn")
        await send(adapters[0], later)
        assert len(db.get_messages("child")) == 1
        notice = {**later, "task_failure_notice": True, "results": [{"task_index": 0, "status": "failed"}]}
        await send(adapters[0], notice)
        await send(adapters[1], notice)
        assert len(db.get_messages("child")) == 2  # interim notice cannot consume the final's identity
        assert db.get_messages("other") == []
    finally:
        peer.close()
        db.close()


@pytest.mark.asyncio
async def test_partial_error_header_controls_are_sanitized_without_changing_body(
    tmp_path, monkeypatch
):
    """The real HTTP route keeps JSON error text while making its header wire-safe."""
    async with runtime_owned_api(tmp_path, monkeypatch, api_key="header-test-key") as adapter:
        async def partial_error(**_kwargs):
            return ({
                "final_response": "partial answer",
                "completed": False,
                "partial": True,
                "failed": False,
                "error": "provider failed\r\nX-Injected: yes\x01",
                "messages": [],
            }, {})

        adapter._run_agent = partial_error
        app = web.Application()
        app.router.add_post("/v1/chat/completions", adapter._handle_chat_completions)
        async with TestClient(TestServer(app)) as client:
            response = await client.post(
                "/v1/chat/completions",
                headers={"Authorization": "Bearer header-test-key"},
                json={"messages": [{"role": "user", "content": "header probe"}]},
            )
            assert response.status == 200
            error_header = response.headers["X-Hermes-Error"]
            assert "\r" not in error_header
            assert "\n" not in error_header
            assert "\x01" not in error_header
            body = await response.json()
            assert body["choices"][0]["message"]["content"] == "partial answer"
            assert body["hermes"]["error"] == "provider failed\r\nX-Injected: yes\x01"
