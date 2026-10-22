"""Canonical authority notifications preserve source outcomes and exact retries."""
import copy

import pytest


def _install_loopback_provider(monkeypatch, text):
    """Make the real AIAgent consume a scripted response from the real loopback HTTP server."""
    from tests.fakes.fake_llm_provider import FakeLLMServer as RealFakeLLMServer, Text

    servers = []

    class ScriptedFakeLLMServer(RealFakeLLMServer):
        def __init__(self, *args, **kwargs):
            kwargs["default_text"] = text
            kwargs["aux"] = lambda _request: Text(
                text, chunk_chars=4096, prompt_tokens=3, completion_tokens=4)
            super().__init__(*args, **kwargs)
            self._script = [
                Text(text, chunk_chars=4096, prompt_tokens=3, completion_tokens=4)
                for _ in range(16)
            ]
            servers.append(self)

    monkeypatch.setattr("tests.fakes.fake_llm_provider.FakeLLMServer", ScriptedFakeLLMServer)
    return servers


def _assert_loopback_provider_used(servers):
    assert len(servers) == 1
    assert servers[0].main_requests()


def _install_canonical_agent(monkeypatch, result):
    """Use the production AIAgent and overlay only the requested source flags after its real turn."""
    from run_agent import AIAgent as RealAIAgent

    created = []

    class ObservedAIAgent(RealAIAgent):
        def __init__(self, *args, **kwargs):
            self._canonical_source = copy.deepcopy(result)
            self.callback_history = []
            super().__init__(*args, **kwargs)
            created.append(self)

        def run_conversation(self, *args, **kwargs):
            self.callback_history.append(getattr(self, "stream_delta_callback", None))
            outcome = super().run_conversation(*args, **kwargs)
            if isinstance(outcome, dict):
                outcome = dict(outcome)
                # The loopback provider and real persistence/finalizer own messages, usage, and proof.
                # These source flags are the test's authenticated model-result boundary.
                for key in ("completed", "failed", "partial", "error"):
                    if key in self._canonical_source:
                        outcome[key] = copy.deepcopy(self._canonical_source[key])
            return outcome

    monkeypatch.setattr("run_agent.AIAgent", ObservedAIAgent)
    return created


def _forbid_legacy_factory(adapter, monkeypatch):
    def unexpected_legacy_factory(*_args, **_kwargs):
        raise AssertionError("canonical authority turn used APIServerAdapter._create_agent")

    monkeypatch.setattr(adapter, "_create_agent", unexpected_legacy_factory)


def _settled_admission(adapter, session_id, request_id):
    from gateway.session_results import admission_result
    from hermes_state_runtime import list_session_admissions

    authority = adapter.gateway_runner.session_authority
    rows = list_session_admissions(authority.db, session_id=session_id, pending_only=False)
    row = next(row for row in rows if row["request_id"] == request_id)
    return row, admission_result(authority.db, row["admission_id"])


@pytest.mark.asyncio
@pytest.mark.parametrize(("category", "muted"), [("result", False), ("diagnostic", True)])
async def test_authority_notification_roundtrip_uses_canonical_factory(
    tmp_path, monkeypatch, category, muted,
):
    from tests.gateway.conftest import runtime_owned_api

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    (tmp_path / "config.yaml").write_text("display: {suppress_warning_notifications: true}\n")
    source = {
        "final_response": "canonical notification evidence",
        "completed": False,
        "failed": True,
        "error": "canonical failure details",
        "messages": [{"role": "assistant", "content": "canonical notification evidence"}],
    }
    request_id = f"canonical-notification-{category}"
    observed = []
    servers = _install_loopback_provider(monkeypatch, source["final_response"])

    async with runtime_owned_api(tmp_path, monkeypatch) as adapter:
        created = _install_canonical_agent(monkeypatch, source)
        _forbid_legacy_factory(adapter, monkeypatch)
        result, usage = await adapter._run_agent(
            "canonical event", [], session_id="canonical-notification",
            request_id=request_id, notification_category=category,
            stream_delta_callback=observed.append,
        )

        _assert_loopback_provider_used(servers)
        assert len(created) == 1
        assert result["final_response"] == source["final_response"]
        assert result["failed"] is True and result["error"] == source["error"]
        assert bool(result.get("_notification_presentation_suppressed")) is muted
        assert usage["input_tokens"] == 3 and usage["output_tokens"] == 4
        assert observed == ([] if muted else [source["final_response"]])
        assert source["messages"] == [{"role": "assistant", "content": source["final_response"]}]

        row, stored = _settled_admission(adapter, "canonical-notification", request_id)
        assert row["status"] == "terminal" and row["outcome"] == "failed"
        assert row["payload"]["api_turn_v1"]["settings"]["notification_category"] == category
        assert row["payload"]["api_turn_v1"]["producer_process_mode"] == "managed_turn"
        assert stored["result"]["failed"] is True
        assert stored["result"]["error"] == source["error"]
        assert bool(stored["result"].get("_notification_presentation_suppressed")) is muted


@pytest.mark.asyncio
async def test_authority_notification_retry_replays_terminal_row_without_new_agent(
    tmp_path, monkeypatch,
):
    from tests.gateway.conftest import runtime_owned_api

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    (tmp_path / "config.yaml").write_text("display: {suppress_warning_notifications: true}\n")
    source = {
        "final_response": "replayed canonical evidence",
        "completed": True,
        "failed": False,
        "messages": [{"role": "assistant", "content": "replayed canonical evidence"}],
    }
    request_id = "canonical-notification-replay"
    servers = _install_loopback_provider(monkeypatch, source["final_response"])

    async with runtime_owned_api(tmp_path, monkeypatch) as adapter:
        created = _install_canonical_agent(monkeypatch, source)
        _forbid_legacy_factory(adapter, monkeypatch)
        first = await adapter._run_agent(
            "replayable canonical event", [], session_id="canonical-replay",
            request_id=request_id, notification_category="diagnostic",
        )
        second = await adapter._run_agent(
            "replayable canonical event", [], session_id="canonical-replay",
            request_id=request_id, notification_category="diagnostic",
        )

        assert first == second
        _assert_loopback_provider_used(servers)
        assert len(created) == 1
        row, stored = _settled_admission(adapter, "canonical-replay", request_id)
        assert row["status"] == "terminal"
        assert row["payload"]["api_turn_v1"]["settings"]["notification_category"] == "diagnostic"
        assert stored["result"]["final_response"] == source["final_response"]
        assert stored["result"].get("_notification_presentation_suppressed") is True



def _canonical_http_app(adapter):
    from aiohttp import web

    app = web.Application()
    app.router.add_post("/v1/chat/completions", adapter._handle_chat_completions)
    return app


def _canonical_wire_frames(wire):
    import json

    return [
        json.loads(line[6:])
        for line in wire.splitlines()
        if line.startswith("data: ") and line != "data: [DONE]"
    ]


def _settled_category_admission(adapter, session_id, category):
    from gateway.session_results import admission_result
    from hermes_state_runtime import list_session_admissions

    authority = adapter.gateway_runner.session_authority
    rows = list_session_admissions(authority.db, session_id=session_id, pending_only=False)
    matches = [
        row
        for row in rows
        if (row.get("payload") or {}).get("api_turn_v1", {}).get("settings", {}).get(
            "notification_category"
        )
        == category
    ]
    assert len(matches) == 1
    row = matches[0]
    return row, admission_result(authority.db, row["admission_id"])


@pytest.mark.asyncio
@pytest.mark.parametrize(("category", "muted"), [("result", False), ("diagnostic", True)])
async def test_authority_notification_json_wire_uses_canonical_factory(
    monkeypatch, tmp_path, category, muted
):
    import json
    from aiohttp.test_utils import TestClient, TestServer
    from tests.gateway.conftest import runtime_owned_api

    source = {
        "final_response": "canonical JSON wire result",
        "failed": True,
        "partial": True,
        "completed": False,
        "error": "canonical JSON diagnostic detail",
        "messages": [{"role": "assistant", "content": "canonical JSON wire result"}],
    }
    session_id = f"canonical-wire-json-{category}"
    (tmp_path / "config.yaml").write_text(
        "display: {suppress_warning_notifications: true}\n")
    servers = _install_loopback_provider(monkeypatch, source["final_response"])
    async with runtime_owned_api(tmp_path, monkeypatch, api_key="test-local-key") as adapter:
        created = _install_canonical_agent(monkeypatch, source)
        _forbid_legacy_factory(adapter, monkeypatch)
        async with TestClient(TestServer(_canonical_http_app(adapter))) as client:
            response = await client.post(
                "/v1/chat/completions",
                headers={
                    "Authorization": "Bearer test-local-key",
                    "X-Hermes-Session-Id": session_id,
                    "Idempotency-Key": f"{session_id}-request",
                },
                json={
                    "messages": [{"role": "user", "content": "canonical JSON request"}],
                    "stream": False,
                    "hermes_notification_category": category,
                },
            )
            wire = await response.text()

        assert response.status == 200, wire
        _assert_loopback_provider_used(servers)
        body = json.loads(wire)
        assert body["choices"][0]["finish_reason"] == "error"
        assert body["choices"][0]["message"]["content"] == (
            "" if muted else source["final_response"]
        )
        assert body["usage"]["total_tokens"] == 7
        assert body["hermes"]["failed"] is True
        assert body["hermes"]["partial"] is True
        assert (source["error"] not in wire) is muted
        assert len(created) == 1

        row, stored = _settled_category_admission(adapter, session_id, category)
        assert row["status"] == "terminal"
        assert stored["result"]["failed"] is True
        assert stored["result"]["partial"] is True
        assert stored["result"]["_notification_presentation_suppressed"] is muted
        assert stored["result"]["_notification_category"] == category


@pytest.mark.asyncio
@pytest.mark.parametrize(("category", "muted"), [("result", False), ("diagnostic", True)])
async def test_authority_notification_sse_wire_preserves_canonical_outcome(
    monkeypatch, tmp_path, category, muted
):
    from tests.gateway.conftest import runtime_owned_api
    from aiohttp.test_utils import TestClient, TestServer

    source = {
        "final_response": "canonical SSE wire result",
        "failed": True,
        "partial": True,
        "completed": False,
        "error": "canonical SSE diagnostic detail",
        "messages": [{"role": "assistant", "content": "canonical SSE wire result"}],
    }
    session_id = f"canonical-wire-sse-{category}"
    (tmp_path / "config.yaml").write_text(
        "display: {suppress_warning_notifications: true}\n")
    servers = _install_loopback_provider(monkeypatch, source["final_response"])
    async with runtime_owned_api(tmp_path, monkeypatch, api_key="test-local-key") as adapter:
        created = _install_canonical_agent(monkeypatch, source)
        _forbid_legacy_factory(adapter, monkeypatch)
        async with TestClient(TestServer(_canonical_http_app(adapter))) as client:
            response = await client.post(
                "/v1/chat/completions",
                headers={
                    "Authorization": "Bearer test-local-key",
                    "X-Hermes-Session-Id": session_id,
                    "Idempotency-Key": f"{session_id}-request",
                },
                json={
                    "messages": [{"role": "user", "content": "canonical SSE request"}],
                    "stream": True,
                    "hermes_notification_category": category,
                },
            )
            wire = await response.text()

        assert response.status == 200, wire
        _assert_loopback_provider_used(servers)
        frames = _canonical_wire_frames(wire)
        assert frames
        terminal = frames[-1]
        assert terminal["choices"][0]["finish_reason"] == "error"
        assert terminal["usage"]["total_tokens"] == 7
        assert terminal["hermes"]["failed"] is True
        assert terminal["hermes"]["partial"] is True
        assert (source["final_response"] not in wire) is muted
        assert len(created) == 1

        row, stored = _settled_category_admission(adapter, session_id, category)
        assert row["status"] == "terminal"
        assert stored["result"]["_notification_presentation_suppressed"] is muted
        assert stored["result"]["_notification_category"] == category


@pytest.mark.asyncio
async def test_authority_notification_sse_rebinds_callback_after_settlement(
    monkeypatch, tmp_path
):
    from tests.gateway.conftest import runtime_owned_api
    from aiohttp.test_utils import TestClient, TestServer

    source = {
        "final_response": "canonical callback rebind evidence",
        "failed": False,
        "partial": False,
        "completed": True,
        "messages": [{"role": "assistant", "content": "canonical callback rebind evidence"}],
    }
    session_id = "canonical-wire-callback-rebind"
    servers = _install_loopback_provider(monkeypatch, source["final_response"])
    async with runtime_owned_api(tmp_path, monkeypatch, api_key="test-local-key") as adapter:
        created = _install_canonical_agent(monkeypatch, source)
        _forbid_legacy_factory(adapter, monkeypatch)
        async with TestClient(TestServer(_canonical_http_app(adapter))) as client:
            for turn in (1, 2):
                response = await client.post(
                    "/v1/chat/completions",
                    headers={
                        "Authorization": "Bearer test-local-key",
                        "X-Hermes-Session-Id": session_id,
                        "Idempotency-Key": f"{session_id}-request-{turn}",
                    },
                    json={
                        "messages": [{"role": "user", "content": f"rebind turn {turn}"}],
                        "stream": True,
                        "hermes_notification_category": "result",
                    },
                )
                wire = await response.text()
                assert response.status == 200, wire
                frames = _canonical_wire_frames(wire)
                assert frames[-1]["choices"][0]["finish_reason"] == "stop"
                assert source["final_response"] in wire
                assert adapter._shutdown_interruptible_agents == {}

        _assert_loopback_provider_used(servers)
        callbacks = [
            callback
            for agent in created
            for callback in agent.callback_history
            if callback is not None
        ]
        assert len(callbacks) == 2
        assert callbacks[0] is not callbacks[1]


@pytest.mark.asyncio
@pytest.mark.parametrize("stream", [False, True])
async def test_authority_notification_diagnostic_wire_visible_when_policy_false(
    monkeypatch, tmp_path, stream
):
    """A diagnostic category is still visible when the canonical profile policy is false."""
    import json
    from aiohttp.test_utils import TestClient, TestServer
    from tests.gateway.conftest import runtime_owned_api

    source = {
        "final_response": "visible diagnostic response",
        "failed": True,
        "partial": True,
        "completed": False,
        "error": "visible diagnostic failure",
        "messages": [{"role": "assistant", "content": "visible diagnostic response"}],
    }
    session_id = f"canonical-wire-diagnostic-visible-{stream}"
    (tmp_path / "config.yaml").write_text(
        "display: {suppress_warning_notifications: false}\n")
    servers = _install_loopback_provider(monkeypatch, source["final_response"])

    async with runtime_owned_api(tmp_path, monkeypatch, api_key="test-local-key") as adapter:
        created = _install_canonical_agent(monkeypatch, source)
        _forbid_legacy_factory(adapter, monkeypatch)
        async with TestClient(TestServer(_canonical_http_app(adapter))) as client:
            response = await client.post(
                "/v1/chat/completions",
                headers={
                    "Authorization": "Bearer test-local-key",
                    "X-Hermes-Session-Id": session_id,
                    "Idempotency-Key": f"{session_id}-request",
                },
                json={
                    "messages": [{"role": "user", "content": "visible diagnostic request"}],
                    "stream": stream,
                    "hermes_notification_category": "diagnostic",
                },
            )
            wire = await response.text()

        assert response.status == 200, wire
        _assert_loopback_provider_used(servers)
        assert len(created) == 1
        assert source["final_response"] in wire
        assert source["error"] in wire

        if stream:
            frames = _canonical_wire_frames(wire)
            assert frames
            terminal = frames[-1]
            assert terminal["choices"][0]["finish_reason"] == "error"
            assert terminal["usage"]["total_tokens"] == 7
            assert terminal["hermes"] == {
                "completed": False,
                "partial": True,
                "failed": True,
                "error": source["error"],
                "error_code": "agent_error",
            }
        else:
            body = json.loads(wire)
            assert body["choices"][0]["message"]["content"] == source["final_response"]
            assert body["choices"][0]["finish_reason"] == "error"
            assert body["usage"]["total_tokens"] == 7
            assert body["hermes"] == {
                "completed": False,
                "partial": True,
                "failed": True,
                "error": source["error"],
                "error_code": "agent_error",
            }

        row, stored = _settled_category_admission(adapter, session_id, "diagnostic")
        assert row["status"] == "terminal"
        assert stored["result"]["failed"] is True
        assert stored["result"]["partial"] is True
        assert stored["result"]["error"] == source["error"]
        assert stored["result"]["_notification_category"] == "diagnostic"
        assert stored["result"]["_notification_presentation_suppressed"] is False
