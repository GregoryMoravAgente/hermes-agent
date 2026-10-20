"""Public managed-turn consumers for shell hooks.

These tests enter through the real API server, GatewayRunner, SessionAuthority,
SessionDB and AIAgent.  The shell script is the only substituted executable;
the custody/writer/finalizer path is the production path under test.
"""

from __future__ import annotations

import json
import sys

import pytest
from aiohttp.test_utils import TestClient, TestServer

from tests.agent.test_copilot_acp_managed_production import (
    HERMES_SESSION_ID,
    HERMES_SESSION_KEY,
    MODEL,
    _api_app,
    _cached_agent,
    _create_session,
    _json_response,
)
from tests.agent.test_shell_hook_custody import shell_runtime as _shell_runtime_fixture

globals()["shell_runtime"] = _shell_runtime_fixture


@pytest.mark.asyncio
async def test_managed_shell_hook_uses_canonical_turn_writer_and_finalizer(monkeypatch, shell_runtime):
    runtime = shell_runtime
    from gateway.runtime_service_custody import ServiceProcessReceipt

    durable_calls = []
    original_record = ServiceProcessReceipt.record_result_durable

    def capture_record(self, binding, request, proof):
        if str(getattr(self, "owner_id", "")).startswith("shell-hook:"):
            durable_calls.append((self, binding, request, proof))
        return original_record(self, binding, request, proof)

    # This is a spy on the canonical production writer handoff, not a replacement
    # receipt or a manually constructed proof.  The original method still performs
    # all validation and physical-finalizer work.
    monkeypatch.setattr(ServiceProcessReceipt, "record_result_durable", capture_record)
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "managed writer proof", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": HERMES_SESSION_KEY,
            },
        )
        body = await _json_response(response)

    assert response.status == 200, body
    agent = _cached_agent(runtime["runner"], HERMES_SESSION_ID)
    assert getattr(agent, "_managed_persistence_failed", False) is False
    rows = runtime["runner"].session_authority.db.get_messages(HERMES_SESSION_ID)
    assert any(row.get("role") == "tool" for row in rows), rows
    assert durable_calls, "the canonical SessionDB proof never reached the shell-hook receipt"
    _receipt, binding, _request, proof = durable_calls[-1]
    assert proof.session_id == HERMES_SESSION_ID
    assert proof.admission_id == binding.admission_id
    assert proof.execution_generation == binding.execution_generation
    assert proof.row_ids and proof.result_row_ids
    assert tuple(proof.row_ids[-len(proof.result_row_ids):]) == proof.result_row_ids
    stored_ids = {row.get("_row_id") or row.get("id") for row in rows}
    assert set(proof.row_ids) <= stored_ids
    events = [json.loads(line) for line in runtime["hook_log"].read_text().splitlines()]
    assert {event["hook_event_name"] for event in events} == {"pre_tool_call", "post_tool_call"}


@pytest.mark.asyncio
async def test_managed_shell_hook_retires_exact_operation_after_public_completion(shell_runtime):
    runtime = shell_runtime
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "managed retry identity", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": HERMES_SESSION_KEY,
            },
        )
        body = await _json_response(response)

    assert response.status == 200, body
    agent = _cached_agent(runtime["runner"], HERMES_SESSION_ID)
    assert getattr(agent, "_shell_hook_custody_operations", {}) == {}
    rows = runtime["runner"].session_authority.db.get_messages(HERMES_SESSION_ID)
    tool_rows = [row for row in rows if row.get("role") == "tool"]
    assert len(tool_rows) == 1, rows


@pytest.mark.asyncio
async def test_sequential_transform_shell_hook_consumes_same_managed_turn(shell_runtime):
    runtime = shell_runtime
    from agent import shell_hooks

    transform_script = runtime["hook_log"].parent / "transform-hook.py"
    transform_script.write_text(
        "import json, pathlib, sys\n"
        "payload = json.load(sys.stdin)\n"
        "with pathlib.Path(sys.argv[1]).open('a', encoding='utf-8') as log:\n"
        "    log.write(json.dumps(payload) + '\\n')\n"
        "print('{}')\n",
        encoding="utf-8",
    )
    shell_hooks.register_from_config(
        {"hooks": {"transform_tool_result": [{
            "matcher": "read_file",
            "command": f"{sys.executable} {transform_script} {runtime['hook_log']}",
        }]}},
        accept_hooks=True,
    )
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "managed sequential transform", "provider": "copilot-acp", "model": MODEL},
            headers={
                "Authorization": "Bearer api-test-key-managed-acp-20261019",
                "X-Hermes-Session-Key": HERMES_SESSION_KEY,
            },
        )
        body = await _json_response(response)

    assert response.status == 200, body
    events = [json.loads(line) for line in runtime["hook_log"].read_text().splitlines()]
    assert {event["hook_event_name"] for event in events} >= {
        "pre_tool_call", "post_tool_call", "transform_tool_result",
    }
    assert all(event["tool_name"] == "read_file" for event in events if "tool_name" in event)
