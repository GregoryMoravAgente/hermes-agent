"""Production AIAgent stream consumers for managed hook context transport."""

from __future__ import annotations

import threading
import time
import asyncio
import json
import os
import sys
from pathlib import Path

import pytest
from aiohttp.test_utils import TestClient, TestServer

from tests.agent.test_copilot_acp_managed_production import (
    HERMES_SESSION_ID,
    MODEL,
    _api_app,
    _cached_agent,
    _create_session,
)
from tests.fakes.providers import copilot_acp as acp
from tests.agent.test_shell_hook_custody import shell_runtime as _shell_runtime_fixture
from tests.waiting import async_wait_until

globals()["shell_runtime"] = _shell_runtime_fixture


def _callbacks(callbacks_by_hook):
    return lambda name: tuple(callbacks_by_hook.get(name, ()))


async def _wait_for(predicate, timeout: float = 1.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and not predicate():
        await asyncio.sleep(0.01)
    assert predicate()


def _configure_real_stream_hooks(
    runtime, tmp_path: Path, *, blocking: bool = False, actions: list[dict] | None = None,
    turns: list[list[dict]] | None = None,
):
    """Configure shell consumers on the live fixture for an actual ACP route."""
    from agent import shell_hooks

    script_path = runtime["fake"].state_dir / "script.json"
    script = json.loads(script_path.read_text(encoding="utf-8"))
    if actions is None:
        messages = ["stream one", "stream two", "stream three"] if blocking else ["streamed managed output"]
        actions = [*[acp.message(message) for message in messages], acp.result()]
    script["turns"] = turns if turns is not None else [actions]
    script_path.write_text(json.dumps(script), encoding="utf-8")

    log_path = runtime["hook_log"]
    if not blocking:
        hook_script = tmp_path / "stream-hook.py"
        hook_script.write_text(
            "import json, pathlib, sys\n"
            "payload = json.load(sys.stdin)\n"
            "with pathlib.Path(sys.argv[1]).open('a', encoding='utf-8') as log:\n"
            "    log.write(json.dumps(payload) + '\\n')\n"
            "print('{}')\n",
            encoding="utf-8",
        )
        command = f"{sys.executable} {hook_script} {log_path}"
        shell_hooks.register_from_config(
            {"hooks": {name: [{"command": command}] for name in (
                "on_stream_start", "on_stream_delta", "on_stream_end",
            )}},
            accept_hooks=True,
        )
        return None, None

    fifo = tmp_path / "stream-gate"
    marker = tmp_path / "stream-gate.started"
    os.mkfifo(fifo)
    hook_script = tmp_path / "stream-blocking-hook.py"
    hook_script.write_text(
        "import json, pathlib, sys\n"
        "payload = json.load(sys.stdin)\n"
        "with pathlib.Path(sys.argv[1]).open('a', encoding='utf-8') as log:\n"
        "    log.write(json.dumps(payload) + '\\n')\n"
        "if payload.get('hook_event_name') == 'on_stream_delta':\n"
        "    marker = pathlib.Path(sys.argv[3])\n"
        "    try:\n"
        "        marker.open('x').close()\n"
        "    except FileExistsError:\n"
        "        pass\n"
        "    with pathlib.Path(sys.argv[2]).open('r', encoding='utf-8') as gate:\n"
        "        gate.read(1)\n"
        "print('{}')\n",
        encoding="utf-8",
    )
    command = f"{sys.executable} {hook_script} {log_path} {fifo} {marker}"
    shell_hooks.register_from_config(
        {"hooks": {name: [{"command": command}] for name in (
            "on_stream_start", "on_stream_delta", "on_stream_end",
        )}},
        accept_hooks=True,
    )
    return fifo, marker


def _assert_real_managed_completion(runtime, prompt: str, reply: str) -> None:
    """Require the real admission/result/owner fences after a public ACP turn."""
    from gateway.session_results import admission_result
    from hermes_state_runtime import list_session_admissions

    db = runtime["runner"].session_authority.db
    admissions = list_session_admissions(db, session_id=HERMES_SESSION_ID, pending_only=False)
    matches = [row for row in admissions if row.get("payload", {}).get("text") == prompt]
    assert len(matches) == 1, admissions
    admission = matches[0]
    assert admission["status"] == "terminal", admission
    assert admission["outcome"] == "completed", admission
    generation = admission.get("generation")
    assert type(generation) is int and generation > 0, admission
    stored = admission_result(db, admission["admission_id"])
    assert stored["result"]["final_response"] == reply, stored

    agent = _cached_agent(runtime["runner"], HERMES_SESSION_ID)
    acp_client = agent._managed_acp_client
    parent = acp_client._producer_policy.parent
    assert acp_client._managed_service_is_reusable() is True
    assert acp_client.is_closed is False
    assert parent._owner_agent is agent
    assert parent.state == "request_reserved"
    assert parent._service.external_parent is parent
    authority = runtime["runner"].session_authority
    assert parent.profile_id == authority.profile_id
    assert parent.authority_epoch == authority.epoch
    assert parent._physical is None


@pytest.mark.asyncio
async def test_public_agent_stream_lifecycle_delivers_real_stream_events(monkeypatch, shell_runtime):
    runtime = shell_runtime
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "prepare stream consumer", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": "hermes-api-key-managed-20261019",
            },
        )
        assert response.status == 200, await response.text()
    from agent.plugin_stream_hooks import shutdown_plugin_stream_hook_dispatcher

    shutdown_plugin_stream_hook_dispatcher()
    events = []
    monkeypatch.setattr(
        "hermes_cli.plugins.iter_hook_callbacks",
        _callbacks({
            "on_stream_start": [lambda **payload: events.append(("start", payload))],
            "on_stream_delta": [lambda **payload: events.append(("delta", payload))],
            "on_stream_end": [lambda **payload: events.append(("end", payload))],
        }),
    )
    agent = _cached_agent(runtime["runner"], HERMES_SESSION_ID)
    agent._emit_stream_start()
    agent._fire_stream_delta("streamed result")
    agent._emit_stream_end(final_text="streamed result", finished=True, error=None)
    await _wait_for(lambda: {name for name, _payload in events} == {"start", "delta", "end"})
    shutdown_plugin_stream_hook_dispatcher()

    assert [payload["delta"] for name, payload in events if name == "delta"] == ["streamed result"]
    assert all("admission_id" not in payload and "parent_handle" not in payload for _name, payload in events)


@pytest.mark.asyncio
async def test_public_agent_stream_queue_drop_and_shutdown_keep_callback_bounded(monkeypatch, shell_runtime):
    runtime = shell_runtime
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "prepare bounded stream", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": "hermes-api-key-managed-20261019",
            },
        )
        assert response.status == 200, await response.text()
    from agent.plugin_stream_hooks import shutdown_plugin_stream_hook_dispatcher

    shutdown_plugin_stream_hook_dispatcher()
    monkeypatch.setattr("agent.plugin_stream_hooks._QUEUE_SIZE", 1)
    delivered = []
    started = threading.Event()
    release = threading.Event()

    def on_stream_delta(**payload):
        delivered.append(payload["delta"])
        started.set()
        release.wait(timeout=1.0)

    monkeypatch.setattr(
        "hermes_cli.plugins.iter_hook_callbacks",
        _callbacks({"on_stream_delta": [on_stream_delta]}),
    )
    agent = _cached_agent(runtime["runner"], HERMES_SESSION_ID)
    agent._fire_stream_delta("first")
    assert started.wait(timeout=1.0)
    agent._fire_stream_delta("second")
    agent._fire_stream_delta("third")
    release.set()
    await _wait_for(lambda: delivered == ["first", "third"])
    shutdown_plugin_stream_hook_dispatcher(timeout=0.2)

    assert delivered == ["first", "third"]


@pytest.mark.asyncio
async def test_public_agent_stream_lifecycle_consumes_real_shell_hooks(shell_runtime, tmp_path):
    """ACP updates invoke configured shell callbacks during the managed turn."""
    runtime = shell_runtime
    _configure_real_stream_hooks(runtime, tmp_path)
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "exercise live stream consumer", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": "hermes-api-key-managed-20261019",
            },
        )
        body = await response.text()

    from agent.plugin_stream_hooks import shutdown_plugin_stream_hook_dispatcher

    shutdown_plugin_stream_hook_dispatcher()
    events = [json.loads(line) for line in runtime["hook_log"].read_text().splitlines()]
    names = {event["hook_event_name"] for event in events}
    assert {"on_stream_start", "on_stream_delta", "on_stream_end"} <= names, events
    assert response.status == 200, body
    assert all("admission_id" not in event and "parent_handle" not in event for event in events)


@pytest.mark.asyncio
async def test_public_agent_stream_queue_drop_retains_live_owner(shell_runtime, monkeypatch, tmp_path):
    """A full managed queue drops an event without poisoning the active turn owner."""
    runtime = shell_runtime
    monkeypatch.setattr("agent.plugin_stream_hooks._QUEUE_SIZE", 1)
    stream_gate, stream_gate_marker = _configure_real_stream_hooks(runtime, tmp_path, blocking=True)
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        request = asyncio.create_task(client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "exercise bounded stream drop", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": "hermes-api-key-managed-20261019",
            },
        ))
        await async_wait_until(
            lambda: stream_gate_marker.exists(),
            timeout=10,
            what="the first real stream shell consumer to block",
        )

        def release_gate() -> None:
            with stream_gate.open("w", encoding="utf-8") as gate:
                gate.write("release")

        await asyncio.to_thread(release_gate)
        response = await request
        body = await response.text()

    from agent.plugin_stream_hooks import shutdown_plugin_stream_hook_dispatcher

    shutdown_plugin_stream_hook_dispatcher(timeout=1.0)
    events = [json.loads(line) for line in runtime["hook_log"].read_text().splitlines()]
    delta_events = [event for event in events if event["hook_event_name"] == "on_stream_delta"]
    assert delta_events, events
    assert len(delta_events) < 3, "queue-full route should drop at least one live delta"
    assert response.status == 200, body
    agent = _cached_agent(runtime["runner"], HERMES_SESSION_ID)
    parent = agent._managed_acp_client._producer_policy.parent
    assert getattr(agent, "_managed_persistence_failed", False) is False
    assert parent.state == "request_reserved"
    assert parent._physical is None


@pytest.mark.asyncio
async def test_public_agent_acp_xml_tool_envelopes_are_filtered_once(shell_runtime, tmp_path):
    """The real managed stream reads a bounded workspace file and completes its second model turn."""
    runtime = shell_runtime
    read_path = Path(__file__).resolve()
    xml = acp.hermes_tool_call("call_xml", "read_file", {"path": str(read_path)})
    first_turn = [
        acp.fs_read(str(read_path)),
        acp.message("before"),
        acp.message(xml[:11]),
        acp.message(xml[11:]),
        acp.message("middle"),
        acp.message("after"),
        acp.result(),
    ]
    second_turn = [acp.message("xml tool result complete"), acp.result()]
    _configure_real_stream_hooks(runtime, tmp_path, turns=[first_turn, second_turn])
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "exercise ACP tool envelope filtering", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": "hermes-api-key-managed-20261019",
            },
        )
        body = await response.text()

    from agent.plugin_stream_hooks import shutdown_plugin_stream_hook_dispatcher

    shutdown_plugin_stream_hook_dispatcher()
    events = [json.loads(line) for line in runtime["hook_log"].read_text().splitlines()]
    stream_events = [
        event for event in events if event["hook_event_name"].startswith("on_stream_")
    ]
    payload = json.dumps(stream_events)
    records = runtime["fake"].records()
    fs_results = [record for record in records if record.get("kind") == "fs_read_result"]
    prompts = runtime["fake"].main_prompts()
    rows = runtime["runner"].session_authority.db.get_messages(HERMES_SESSION_ID)
    assert response.status == 200, body
    assert "xml tool result complete" in body
    assert len(prompts) == 2, records
    assert fs_results and fs_results[0]["msg"]["result"]["content"], records
    assert any(row.get("role") == "tool" and row.get("tool_call_id") == "call_xml" for row in rows), rows
    assert "<tool_call>" not in payload
    assert "call_xml" not in payload
    assert str(read_path) not in payload
    assert json.dumps({"path": str(read_path)}, sort_keys=True) not in payload
    assert [event["hook_event_name"] for event in events].count("on_stream_start") == 1
    assert [event["hook_event_name"] for event in events].count("on_stream_end") == 1
    assert any(event["hook_event_name"] == "on_stream_delta" for event in events)
    _assert_real_managed_completion(runtime, "exercise ACP tool envelope filtering", "xml tool result complete")


@pytest.mark.asyncio
async def test_public_agent_acp_bare_tool_envelopes_are_filtered_once(shell_runtime, tmp_path):
    """The real managed stream reads a bounded workspace file and completes its bare-call turn."""
    runtime = shell_runtime
    read_path = Path(__file__).resolve()
    xml = acp.hermes_tool_call("call_bare", "read_file", {"path": str(read_path)})
    bare = xml[len("<tool_call>"):-len("</tool_call>")]
    first_turn = [
        acp.fs_read(str(read_path)),
        acp.message("before"),
        acp.message(bare[:9]),
        acp.message(bare[9:] + "after"),
        acp.result(),
    ]
    second_turn = [acp.message("bare tool result complete"), acp.result()]
    _configure_real_stream_hooks(
        runtime,
        tmp_path,
        turns=[first_turn, second_turn],
    )
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "exercise ACP bare tool envelope filtering", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": "hermes-api-key-managed-20261019",
            },
        )
        body = await response.text()

    from agent.plugin_stream_hooks import shutdown_plugin_stream_hook_dispatcher

    shutdown_plugin_stream_hook_dispatcher()
    events = [json.loads(line) for line in runtime["hook_log"].read_text().splitlines()]
    stream_events = [
        event for event in events if event["hook_event_name"].startswith("on_stream_")
    ]
    payload = json.dumps(stream_events)
    records = runtime["fake"].records()
    fs_results = [record for record in records if record.get("kind") == "fs_read_result"]
    prompts = runtime["fake"].main_prompts()
    rows = runtime["runner"].session_authority.db.get_messages(HERMES_SESSION_ID)
    assert response.status == 200, body
    assert "bare tool result complete" in body
    assert len(prompts) == 2, records
    assert fs_results and fs_results[0]["msg"]["result"]["content"], records
    assert any(row.get("role") == "tool" and row.get("tool_call_id") == "call_bare" for row in rows), rows
    assert "call_bare" not in payload
    assert '"type":"function"' not in payload
    assert str(read_path) not in payload
    assert json.dumps({"path": str(read_path)}, sort_keys=True) not in payload
    assert [event["hook_event_name"] for event in events].count("on_stream_start") == 1
    assert [event["hook_event_name"] for event in events].count("on_stream_end") == 1
    _assert_real_managed_completion(runtime, "exercise ACP bare tool envelope filtering", "bare tool result complete")


@pytest.mark.asyncio
async def test_public_agent_acp_mixed_tool_envelopes_fail_without_success_projection(shell_runtime, tmp_path):
    """An irreconcilable mixed XML/bare response fails closed without raw end text."""
    runtime = shell_runtime
    xml = acp.hermes_tool_call("call_mixed", "read_file", {"path": "README.md"})
    bare = xml[len("<tool_call>"):-len("</tool_call>")]
    _configure_real_stream_hooks(
        runtime,
        tmp_path,
        actions=[acp.message("before"), acp.message(xml), acp.message(bare), acp.result()],
    )
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "exercise mixed ACP envelope refusal", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": "hermes-api-key-managed-20261019",
            },
        )
        body = await response.text()

    from agent.plugin_stream_hooks import shutdown_plugin_stream_hook_dispatcher

    shutdown_plugin_stream_hook_dispatcher()
    events = [json.loads(line) for line in runtime["hook_log"].read_text().splitlines()]
    payload = json.dumps(events)
    ends = [event for event in events if event["hook_event_name"] == "on_stream_end"]
    assert response.status == 200, body
    assert json.loads(body)["status"] == "failed"
    assert len(ends) == 1
    assert ends[0]["finished"] is False
    assert "call_mixed" not in payload
    assert '"type":"function"' not in payload


@pytest.mark.asyncio
async def test_public_agent_acp_stream_route_is_active_and_once_only(shell_runtime, tmp_path):
    """The public outer SSE route drives ACP callbacks inside an active admission.

    ACP's provider policy intentionally remains non-streaming; this control does not
    claim direct ``CopilotACPClient`` ``stream=True`` coverage.
    """
    runtime = shell_runtime
    _configure_real_stream_hooks(
        runtime, tmp_path,
        actions=[acp.message("ordinary producer response"), acp.result()],
    )
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat/stream",
            json={"message": "ordinary producer lifecycle", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": "hermes-api-key-managed-20261019",
            },
        )
        body = await response.text()

    from agent.plugin_stream_hooks import shutdown_plugin_stream_hook_dispatcher

    shutdown_plugin_stream_hook_dispatcher()
    events = [json.loads(line) for line in runtime["hook_log"].read_text().splitlines()]
    first_delta = [
        event["extra"]["delta"]
        for event in events if event["hook_event_name"] == "on_stream_delta"
    ]
    assert response.status == 200, body
    assert "ordinary producer response" in body
    assert first_delta == ["ordinary producer response"]
    assert [event["hook_event_name"] for event in events].count("on_stream_start") == 1
    assert [event["hook_event_name"] for event in events].count("on_stream_end") == 1
    assert len(runtime["fake"].main_prompts()) == 1
    _assert_real_managed_completion(runtime, "ordinary producer lifecycle", "ordinary producer response")


@pytest.mark.asyncio
async def test_public_agent_acp_direct_stream_true_uses_active_managed_turn(monkeypatch, shell_runtime, tmp_path):
    """Exercise the supported direct-client stream=True branch under one real admission."""
    runtime = shell_runtime
    prompt = "exercise explicit ACP direct stream mode"
    reply = "explicit ACP direct stream response"
    _configure_real_stream_hooks(
        runtime, tmp_path,
        actions=[acp.message(reply), acp.result()],
    )

    from agent import turn_api_call
    from agent.copilot_acp_client import CopilotACPClient

    original_should_stream = turn_api_call._should_stream
    original_create = CopilotACPClient._create_chat_completion
    decisions = []
    forced_agents = []
    observed_stream_calls = []

    def delegate_stream_decision(candidate):
        default = original_should_stream(candidate)
        if str(getattr(candidate, "provider", "")).lower() == "copilot-acp":
            decisions.append({
                "default": default,
                "base_url": str(getattr(candidate, "base_url", "")),
                "session_id": getattr(candidate, "session_id", None),
            })
            if not forced_agents:
                forced_agents.append(candidate)
                return True
        return default

    def observe_direct_create(client, **kwargs):
        if kwargs.get("stream") is True:
            policy = getattr(client, "_producer_policy", None)
            parent = getattr(policy, "parent", None)
            service = getattr(parent, "_service", None)
            binding = getattr(parent, "turn_binding", None)
            observed_stream_calls.append({
                "client": client,
                "parent": parent,
                "mode": getattr(parent, "mode", None),
                "state": getattr(parent, "state", None),
                "binding": binding,
                "service": service,
                "request_present": bool(
                    service is not None and binding is not None
                    and binding.admission_id in getattr(service, "requests", {})
                ),
                "service_unknown": getattr(service, "unknown", None),
                "physical_before_effect": getattr(service, "physical", None) is not None,
                "owner_present": getattr(parent, "_owner_agent", None) is not None,
            })
        return original_create(client, **kwargs)

    monkeypatch.setattr(turn_api_call, "_should_stream", delegate_stream_decision)
    monkeypatch.setattr(CopilotACPClient, "_create_chat_completion", observe_direct_create)

    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": prompt, "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": "hermes-api-key-managed-20261019",
            },
        )
        body = await response.text()

    from agent.plugin_stream_hooks import shutdown_plugin_stream_hook_dispatcher

    shutdown_plugin_stream_hook_dispatcher()
    events = [json.loads(line) for line in runtime["hook_log"].read_text().splitlines()]
    assert response.status == 200, body
    assert reply in body
    assert decisions, "the real turn_api_call decision hook was not reached"
    assert decisions[0]["default"] is False, decisions
    assert len(forced_agents) == 1, decisions
    assert len(observed_stream_calls) == 1, observed_stream_calls
    observed = observed_stream_calls[0]
    assert observed["mode"] == "managed_turn", observed
    assert observed["state"] == "request_reserved", observed
    assert observed["binding"] is not None, observed
    assert observed["request_present"] is True, observed
    assert observed["service_unknown"] is False, observed
    assert observed["physical_before_effect"] is False, observed
    assert observed["owner_present"] is True, observed
    assert len(runtime["fake"].main_prompts()) == 1
    assert runtime["fake"].events("spawn"), runtime["fake"].records()
    assert runtime["fake"].events("exit"), runtime["fake"].records()
    assert [event["hook_event_name"] for event in events].count("on_stream_start") == 1
    assert [event["hook_event_name"] for event in events].count("on_stream_end") == 1
    _assert_real_managed_completion(runtime, prompt, reply)


@pytest.mark.asyncio
async def test_public_agent_acp_error_ends_stream_once(shell_runtime, tmp_path):
    """A real ACP error retains the failed end fence and never emits success."""
    runtime = shell_runtime
    _configure_real_stream_hooks(
        runtime,
        tmp_path,
        actions=[acp.message("before"), acp.rpc_error(-32000, "synthetic ACP failure")],
    )
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "exercise ACP error lifecycle", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": "hermes-api-key-managed-20261019",
            },
        )
        body = await response.text()

    from agent.plugin_stream_hooks import shutdown_plugin_stream_hook_dispatcher

    shutdown_plugin_stream_hook_dispatcher()
    events = [json.loads(line) for line in runtime["hook_log"].read_text().splitlines()]
    ends = [event for event in events if event["hook_event_name"] == "on_stream_end"]
    assert response.status == 200, body
    assert json.loads(body)["status"] == "failed"
    assert len(ends) == 1
    assert ends[0]["finished"] is False
