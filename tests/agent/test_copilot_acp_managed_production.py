"""Production ACP custody consumer controls.

This module is intentionally a source proposal.  It exercises the public API server route and the
normal GatewayRunner/AIAgent path; the only substituted boundary is the external ACP executable,
which is the repository's schema-valid ``AcpFake`` subprocess.  It never constructs a service
binding, an AIAgent, a persistence writer, or a delivery receipt by hand.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from tests.fakes.providers import copilot_acp as acp
from tests.waiting import async_wait_until

API_KEY = "api-test-key-managed-acp-20261019"
MODEL = "fake-model-managed-acp"
HERMES_SESSION_ID = "hermes-api-session-managed-20261019"
HERMES_SESSION_KEY = "hermes-api-key-managed-20261019"
FIRST_PROMPT = "managed production turn one ACP-ID boundary"
SECOND_PROMPT = "managed production turn two ACP-ID boundary"
FIRST_REPLY = "managed production reply one"
SECOND_REPLY = "managed production reply two"


@dataclass
class RuntimeFixture:
    root: Path
    home: Path
    beta_home: Path | None
    runner: Any
    adapter: Any
    authority: Any
    fake: acp.AcpFake
    api_key: str


def _write_profile(home: Path, fake: acp.AcpFake) -> None:
    home.mkdir(parents=True, exist_ok=True)
    (home / "config.yaml").write_text(
        json.dumps(
            {
                "model": {"provider": "copilot-acp", "default": MODEL},
                "agent": {"api_max_retries": 0},
                "updates": {"check": False},
                "auxiliary": {"title_generation": {"enabled": False}},
                "memory": {"memory_enabled": False, "user_profile_enabled": False},
                "telemetry": {"shared_metrics": {"enabled": False, "send": False}},
            }
        ),
        encoding="utf-8",
    )
    env = {**fake.env(), "API_SERVER_KEY": API_KEY}
    (home / ".env").write_text(
        "".join(f"{key}={value}\n" for key, value in env.items()), encoding="utf-8"
    )


def _api_app(adapter: Any, runner: Any) -> web.Application:
    """Mirror APIServerAdapter.connect(), including every native /p/<profile> route."""
    app = web.Application(middlewares=[adapter._make_profile_prefix_middleware()])
    app["api_server_adapter"] = adapter
    app["gateway_runner"] = runner
    for method, path, handler in adapter._http_route_table():
        app.router.add_route(method, path, handler)
        app.router.add_route(method, f"/p/{{profile}}{path}", handler)
    return app


async def _json_response(response: Any) -> dict[str, Any]:
    body = await response.text()
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        raise AssertionError(f"expected JSON response, got {body!r}") from None


async def _create_session(client: TestClient, session_id: str, *, path: str = "/api/sessions") -> None:
    response = await client.post(
        path,
        json={
            "id": session_id,
            "source": "api_server",
            "provider": "copilot-acp",
            "model": MODEL,
        },
        headers={"Authorization": f"Bearer {API_KEY}"},
    )
    assert response.status == 201, await response.text()


async def _chat(
    client: TestClient, session_id: str, prompt: str, *, path_prefix: str = "",
    extra_body: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, Any]]:
    body = {"message": prompt, "provider": "copilot-acp", "model": MODEL}
    if extra_body:
        body.update(extra_body)
    response = await client.post(
        f"{path_prefix}/api/sessions/{session_id}/chat",
        json=body,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "X-Hermes-Session-Key": HERMES_SESSION_KEY,
        },
    )
    return response, await _json_response(response)


def _cached_agent(runner: Any, session_id: str) -> Any:
    cache = getattr(runner, "_agent_cache", {})
    matches = [
        entry[0]
        for entry in cache.values()
        if isinstance(entry, tuple) and len(entry) >= 4 and entry[3] == session_id
    ]
    assert len(matches) == 1, f"expected one cached production AIAgent for {session_id!r}: {cache!r}"
    return matches[0]


def _acp_session_ids(fake: acp.AcpFake) -> set[str]:
    result: set[str] = set()
    for record in fake.records():
        message = record.get("msg")
        if not isinstance(message, dict):
            continue
        payload = message.get("result")
        if isinstance(payload, dict) and isinstance(payload.get("sessionId"), str):
            result.add(payload["sessionId"])
    return result


def _spawn_pids(fake: acp.AcpFake) -> set[int]:
    return {int(record["pid"]) for record in fake.events("spawn")}


def _exit_pids(fake: acp.AcpFake) -> set[int]:
    return {int(record["pid"]) for record in fake.events("exit")}


def _row_id(row: dict[str, Any]) -> int:
    value = row.get("_row_id") or row.get("id")
    assert type(value) is int and value > 0, row
    return value


def _durable_turn_row_ids(rows: list[dict[str, Any]], prompt: str, reply: str) -> tuple[int, int]:
    user_rows = [row for row in rows if row.get("role") == "user" and row.get("content") == prompt]
    assert len(user_rows) == 1, {"prompt": prompt, "matches": user_rows, "rows": rows}
    user_id = _row_id(user_rows[0])
    assistant_rows = [
        row for row in rows
        if row.get("role") == "assistant" and row.get("content") == reply and _row_id(row) > user_id
    ]
    assert len(assistant_rows) == 1, {"reply": reply, "after_user": user_id, "matches": assistant_rows, "rows": rows}
    assistant_id = _row_id(assistant_rows[0])
    return user_id, assistant_id


def _assert_durable_turn_rows(rows: list[dict[str, Any]], prompt: str, reply: str) -> tuple[int, int]:
    return _durable_turn_row_ids(rows, prompt, reply)


def _assert_durable_turn(
    admission: dict[str, Any], stored_result: dict[str, Any], rows: list[dict[str, Any]],
    prompt: str, reply: str,
) -> tuple[int, int]:
    assert admission.get("payload", {}).get("text") == prompt, admission
    result = stored_result.get("result")
    assert isinstance(result, dict), stored_result
    assert result.get("final_response") == reply, stored_result
    return _assert_durable_turn_rows(rows, prompt, reply)


@pytest_asyncio.fixture
async def production_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Real single-profile GatewayRunner and APIServerAdapter with only the ACP process replaced."""
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.api_server import APIServerAdapter
    from gateway.run import GatewayRunner
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership

    root = tmp_path / "hermes"
    home = root / ".hermes"
    fake = acp.AcpFake(
        root / "acp",
        [
            [acp.message(FIRST_REPLY), acp.result()],
            [acp.message(SECOND_REPLY), acp.result()],
        ],
        models=[MODEL],
    )
    _write_profile(home, fake)
    monkeypatch.setenv("HOME", str(root))
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_STATE_DB_GUARD_BYPASS", "1")
    monkeypatch.setattr(Path, "home", lambda: root)
    # gateway.run may already be imported by the test environment; pin its cached home
    # before constructing the runner so config/model fallback reads this fixture profile.
    from gateway import run as gateway_run
    monkeypatch.setattr(gateway_run, "_hermes_home", home)
    import hermes_state
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", hermes_state._IMPORT_DEFAULT_DB_PATH)

    process_ownership.reserve([home])
    runner = GatewayRunner(GatewayConfig(sessions_dir=home / "sessions"))
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": API_KEY}))
    runner.adapters[Platform.API_SERVER] = adapter
    adapter.gateway_runner = runner
    runner._wire_adapter_handlers(adapter)
    try:
        await initialize_gateway_runtime(runner)
        yield RuntimeFixture(root, home, None, runner, adapter, runner.session_authority, fake, API_KEY)
    finally:
        await runner.stop()
        if process_ownership.owns(home):
            process_ownership.release(home)


@pytest.mark.asyncio
async def test_public_tool_round_keeps_gateway_writer_pending_until_terminal_proof(
    production_runtime, tmp_path, monkeypatch,
):
    """A real intermediate child cannot become an ordinary Gateway final writer."""
    from agent import session_persistence_proof as proofs
    from agent.session_persistence import SessionPersistenceMixin

    runtime = production_runtime
    target = tmp_path / 'pending-writer-input.txt'
    target.write_text('synthetic pending writer input', encoding='utf-8')
    reply = 'pending writer terminal reply'
    script_path = runtime.fake.state_dir / 'script.json'
    script = json.loads(script_path.read_text(encoding='utf-8'))
    script['turns'] = [
        [acp.message(acp.hermes_tool_call('pending-read', 'read_file', {'path': str(target)})), acp.result()],
        [acp.message(reply), acp.result()],
    ]
    script_path.write_text(json.dumps(script), encoding='utf-8')
    original_flush = SessionPersistenceMixin._flush_messages_to_session_db
    original_publish = proofs.publish_gateway_writer_proof
    observations = []
    published = []

    def observe_flush(agent, messages, conversation_history=None):
        client = getattr(agent, '_managed_acp_client', None)
        parent = getattr(getattr(client, '_producer_policy', None), 'parent', None)
        binding = getattr(parent, 'turn_binding', None)
        probe = bool(
            not observations and not getattr(agent, '_managed_proof_capture', False)
            and client is not None and binding is not None
            and client.has_pending_managed_witness()
            and client._managed_intermediate_message(messages) is not None
        )
        armed = proofs.request_gateway_writer_proof(agent) if probe else None
        result = original_flush(agent, messages, conversation_history)
        if probe:
            fence = getattr(agent, '_classic_writer_intermediate_fence', None)
            observations.append({
                'agent': agent, 'binding': binding, 'armed': armed, 'persisted': result,
                'fence': fence,
                'second_request': proofs.request_gateway_writer_proof(agent),
                'premature_handoff': proofs.publish_gateway_writer_proof(
                    agent, messages, getattr(fence, 'writer_proof', None), agent._current_turn_id,
                ),
            })
        return result

    def observe_publish(agent, messages, proof, turn_id):
        handoff = original_publish(agent, messages, proof, turn_id)
        if handoff is not None:
            published.append(handoff)
        return handoff

    monkeypatch.setattr(SessionPersistenceMixin, '_flush_messages_to_session_db', observe_flush)
    monkeypatch.setattr(proofs, 'publish_gateway_writer_proof', observe_publish)
    async with TestClient(TestServer(_api_app(runtime.adapter, runtime.runner))) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response, body = await _chat(client, HERMES_SESSION_ID, 'read the input with a pending writer')
    assert len(observations) == 1
    [observed] = observations
    assert observed['armed'] is True and observed['persisted'] is True
    assert observed['second_request'] is False
    assert observed['premature_handoff'] is None
    assert observed['fence'] is not None
    assert response.status == 200, body
    assert body.get('message', {}).get('content') == reply, body
    assert len(_spawn_pids(runtime.fake)) == 2
    assert len(published) == 1
    assert published[0].binding is observed['binding']
    assert getattr(observed['agent'], '_classic_writer_intermediate_fence', None) is None


@pytest.mark.asyncio
async def test_public_tool_round_advances_the_same_physical_receipt_before_second_acp_child(
    production_runtime, tmp_path, monkeypatch,
):
    """An actual tool envelope is durable before the next child; the final proof covers both."""
    from gateway.runtime_service_custody import ServiceLease
    from gateway.session_results import admission_result
    from hermes_state_runtime import list_session_admissions

    runtime = production_runtime
    target = tmp_path / 'bounded-input.txt'
    target.write_text('synthetic tool input', encoding='utf-8')
    reply = 'tool round and second child complete'
    script_path = runtime.fake.state_dir / 'script.json'
    script = json.loads(script_path.read_text(encoding='utf-8'))
    script['turns'] = [
        [acp.message(acp.hermes_tool_call('bounded-read', 'read_file', {'path': str(target)})), acp.result()],
        [acp.message(reply), acp.result()],
    ]
    script_path.write_text(json.dumps(script), encoding='utf-8')
    original = ServiceLease.prepare_next_managed_child
    original_durable = ServiceLease.record_result_durable
    transitions = []
    durable_proofs = []

    def observe_durable(service, binding, request, proof):
        original_durable(service, binding, request, proof)
        durable_proofs.append((service.physical, proof))

    def observe_transition(service, binding, request):
        physical = service.physical
        before = {
            'physical': physical, 'handle': physical.handle,
            'process_exited': physical.process.poll() is not None,
            'eof': set(physical.eof_streams), 'joined': set(physical.joined_streams),
            'durable': dict(physical.durable_proofs),
        }
        result = original(service, binding, request)
        before.update(result=result, same_physical=service.physical is physical,
                      same_handle=physical.handle is before['handle'],
                      next_process=physical.process, next_state=physical.state)
        transitions.append(before)
        return result

    monkeypatch.setattr(ServiceLease, 'prepare_next_managed_child', observe_transition)
    monkeypatch.setattr(ServiceLease, 'record_result_durable', observe_durable)
    async with TestClient(TestServer(_api_app(runtime.adapter, runtime.runner))) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response, body = await _chat(client, HERMES_SESSION_ID, 'read the bounded input and finish')
    assert response.status == 200, body
    assert body.get('message', {}).get('content') == reply, body
    assert len(transitions) == 1
    [transition] = transitions
    assert transition['process_exited'] is True
    assert transition['eof'] == transition['joined'] == {'stdout', 'stderr'}
    assert transition['durable']
    assert transition['result'] is True and transition['same_physical'] and transition['same_handle']
    assert transition['next_process'] is None and transition['next_state'] == 'reserved'
    assert len(_spawn_pids(runtime.fake)) == 2
    await async_wait_until(lambda: _spawn_pids(runtime.fake) <= _exit_pids(runtime.fake),
                           timeout=10, what='both real ACP children to exit')
    [row] = list_session_admissions(runtime.authority.db, session_id=HERMES_SESSION_ID, pending_only=False)
    assert (row['status'], row['outcome']) == ('terminal', 'completed')
    saved = admission_result(runtime.authority.db, row['admission_id'])
    assert saved['result']['final_response'] == reply
    messages = saved['result']['messages']
    assert any(message.get('role') == 'tool' and message.get('tool_call_id') == 'bounded-read' for message in messages)
    physical = transition['physical']
    assert len(durable_proofs) >= 2
    proof_physical, proof = durable_proofs[-1]
    assert proof_physical is physical
    assert tuple(message['_row_id'] for message in messages) == proof.result_row_ids
    assert not runtime.authority._preservation_has_sidecar(row['admission_id'])


@pytest.mark.asyncio
async def test_public_three_child_tool_round_retains_one_receipt_and_strict_prefix(
    production_runtime, tmp_path, monkeypatch,
):
    """Two real tool rounds extend the same durable owner before the final ACP child."""
    from gateway.runtime_service_custody import ServiceLease
    from gateway.session_results import admission_result
    from hermes_state_runtime import list_session_admissions

    runtime = production_runtime
    target = tmp_path / "three-child-input.txt"
    target.write_text("synthetic three-child input", encoding="utf-8")
    reply = "three child terminal reply"
    script_path = runtime.fake.state_dir / "script.json"
    script = json.loads(script_path.read_text(encoding="utf-8"))
    script["turns"] = [
        [acp.message(acp.hermes_tool_call("first-read", "read_file", {"path": str(target)})), acp.result()],
        [acp.message(acp.hermes_tool_call("second-read", "read_file", {"path": str(target)})), acp.result()],
        [acp.message(reply), acp.result()],
    ]
    script_path.write_text(json.dumps(script), encoding="utf-8")
    original = ServiceLease.prepare_next_managed_child
    transitions = []

    def observe_transition(service, binding, request):
        physical = service.physical
        agent = physical.managed_client._producer_policy.parent._owner_agent
        fence = getattr(agent, "_classic_writer_intermediate_fence", None)
        before = {
            "physical": physical, "handle": physical.handle, "binding": binding,
            "agent": agent, "fence": fence,
            "eof": set(physical.eof_streams), "joined": set(physical.joined_streams),
            "exited": physical.process.poll() is not None,
        }
        before["result"] = original(service, binding, request)
        before["same_physical"] = service.physical is physical
        before["same_handle"] = physical.handle is before["handle"]
        transitions.append(before)
        return before["result"]

    monkeypatch.setattr(ServiceLease, "prepare_next_managed_child", observe_transition)
    async with TestClient(TestServer(_api_app(runtime.adapter, runtime.runner))) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response, body = await _chat(client, HERMES_SESSION_ID, "read twice before finishing")
    assert response.status == 200, body
    assert body.get("message", {}).get("content") == reply, body
    assert len(transitions) == 2
    first, second = transitions
    assert all(t["result"] is True and t["same_physical"] and t["same_handle"] for t in transitions)
    assert all(t["exited"] and t["eof"] == t["joined"] == {"stdout", "stderr"} for t in transitions)
    assert first["physical"] is second["physical"] and first["handle"] is second["handle"]
    assert first["binding"] is second["binding"]
    assert all(t["fence"] is not None and t["fence"].state == "pending" for t in transitions)
    first_rows = first["fence"].writer_proof.row_ids
    second_rows = second["fence"].writer_proof.row_ids
    assert len(first_rows) < len(second_rows) and second_rows[:len(first_rows)] == first_rows
    assert getattr(first["agent"], "_classic_writer_intermediate_fence", None) is None
    assert len(_spawn_pids(runtime.fake)) == 3
    await async_wait_until(lambda: _spawn_pids(runtime.fake) <= _exit_pids(runtime.fake),
                           timeout=10, what="all three ACP children to exit")
    [row] = list_session_admissions(runtime.authority.db, session_id=HERMES_SESSION_ID, pending_only=False)
    assert (row["status"], row["outcome"]) == ("terminal", "completed")
    saved = admission_result(runtime.authority.db, row["admission_id"])
    assert saved["result"]["final_response"] == reply
    tool_ids = [m.get("tool_call_id") for m in saved["result"]["messages"] if m.get("role") == "tool"]
    assert tool_ids == ["first-read", "second-read"]
    assert not runtime.authority._preservation_has_sidecar(row["admission_id"])


@pytest.mark.asyncio
async def test_public_child_failure_reconciles_retained_intermediate_prefix(
    production_runtime, tmp_path, monkeypatch,
):
    """A retryable real ACP child failure reuses the exact prefix and same physical receipt."""
    from agent.copilot_acp_client import CopilotACPClient
    from gateway.runtime_service_custody import ServiceLease
    from gateway.session_results import admission_result
    from hermes_state_runtime import list_session_admissions

    runtime = production_runtime
    config_path = runtime.home / "config.yaml"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["agent"]["api_max_retries"] = 2
    config_path.write_text(json.dumps(config), encoding="utf-8")
    target = tmp_path / "retry-prefix-input.txt"
    target.write_text("synthetic retry-prefix input", encoding="utf-8")
    reply = "recovered after one child failure"
    script_path = runtime.fake.state_dir / "script.json"
    script = json.loads(script_path.read_text(encoding="utf-8"))
    script["turns"] = [
        [acp.message(acp.hermes_tool_call("retry-read", "read_file", {"path": str(target)})), acp.result()],
        [acp.crash(stderr="provider server error")],
        [acp.message(reply), acp.result()],
    ]
    script_path.write_text(json.dumps(script), encoding="utf-8")
    original_reconcile = ServiceLease.reconcile_managed_child_failure
    original_capture = CopilotACPClient.capture_managed_durable_receipt_witness
    recoveries = []
    captured_witnesses = []

    def observe_reconcile(service, binding, request):
        physical = service.physical
        parent = service.external_parent
        agent = parent._owner_agent
        prefix = getattr(physical, "_classic_writer_intermediate_prefix_handoff", None)
        durable_before = dict(physical.durable_proofs)
        handle = physical.handle
        failed_process = physical.process
        managed_witness_before = getattr(physical, "managed_witness", None)
        result = original_reconcile(service, binding, request)
        recoveries.append({
            "result": result,
            "physical": physical,
            "handle": physical.handle,
            "parent": parent,
            "agent": agent,
            "prefix": prefix,
            "failed_process": failed_process,
            "managed_witness_before": managed_witness_before,
            "durable_before": durable_before,
            "managed_witness_after": getattr(physical, "managed_witness", None),
            "prefix_after": getattr(physical, "_classic_writer_intermediate_prefix_handoff", None),
            "durable_after": dict(physical.durable_proofs),
            "service_unknown_after": service.unknown,
            "parent_state_after": parent.state,
            "physical_state_after": physical.state,
            "same_physical": service.physical is physical,
            "same_service": physical.service is service,
            "same_request": service.requests.get(binding.admission_id) is request,
            "same_binding": service.binding is binding,
            "same_parent": service.external_parent is parent,
            "same_handle": physical.handle is handle,
        })
        return result

    def observe_capture(client, agent, proof):
        witness = original_capture(client, agent, proof)
        if witness is not None:
            captured_witnesses.append(witness)
        return witness

    monkeypatch.setattr(ServiceLease, "reconcile_managed_child_failure", observe_reconcile)
    monkeypatch.setattr(CopilotACPClient, "capture_managed_durable_receipt_witness", observe_capture)
    async with TestClient(TestServer(_api_app(runtime.adapter, runtime.runner))) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response, body = await _chat(client, HERMES_SESSION_ID, "recover the failed child and finish")

    assert response.status == 200, body
    assert body.get("message", {}).get("content") == reply, body
    assert len(recoveries) == 1, recoveries
    [recovery] = recoveries
    assert recovery["result"] is True
    assert recovery["prefix"] is not None
    assert recovery["prefix"].state == "unknown"
    assert recovery["failed_process"] is not None
    assert recovery["managed_witness_before"] is not None
    assert recovery["managed_witness_after"] is None
    assert recovery["prefix_after"] is recovery["prefix"]
    assert recovery["durable_after"] == recovery["durable_before"]
    assert recovery["service_unknown_after"] is False
    assert recovery["parent_state_after"] == "request_reserved"
    assert recovery["physical_state_after"] == "reserved"
    assert recovery["same_physical"] is True
    assert recovery["same_service"] is True
    assert recovery["same_request"] is True
    assert recovery["same_binding"] is True
    assert recovery["same_parent"] is True
    assert recovery["same_handle"] is True
    assert len(_spawn_pids(runtime.fake)) == 3, runtime.fake.records()
    assert recovery["failed_process"].poll() is not None
    failed_pid = recovery["failed_process"].pid
    assert failed_pid in _spawn_pids(runtime.fake)
    await async_wait_until(lambda: _spawn_pids(runtime.fake) - {failed_pid} <= _exit_pids(runtime.fake),
                           timeout=10, what="successful ACP children to exit after the failed child was reaped")

    assert len(captured_witnesses) >= 2, captured_witnesses
    first_witness, final_witness = captured_witnesses[0], captured_witnesses[-1]
    assert first_witness is not final_witness
    assert first_witness.receipt is final_witness.receipt is recovery["physical"]
    assert first_witness.process is not final_witness.process
    assert first_witness.provider_identity[:5] == final_witness.provider_identity[:5]
    assert getattr(recovery["agent"], "_classic_writer_intermediate_fence", None) is None

    admissions = list_session_admissions(
        runtime.authority.db, session_id=HERMES_SESSION_ID, pending_only=False,
    )
    assert len(admissions) == 1, admissions
    [row] = admissions
    assert (row["status"], row["outcome"]) == ("terminal", "completed")
    saved = admission_result(runtime.authority.db, row["admission_id"])
    assert saved["result"]["final_response"] == reply
    assert any(
        message.get("role") == "tool" and message.get("tool_call_id") == "retry-read"
        for message in saved["result"]["messages"]
    )
    assert not runtime.authority._preservation_has_sidecar(row["admission_id"])


@pytest.mark.asyncio
async def test_public_child_failure_rejects_copied_prefix_alias_without_new_child(
    production_runtime, tmp_path, monkeypatch,
):
    """A copied prefix alias is refused while the original custody stays unknown."""
    from dataclasses import replace

    from gateway.runtime_service_custody import ServiceLease
    from hermes_state_runtime import list_session_admissions

    runtime = production_runtime
    config_path = runtime.home / "config.yaml"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["agent"]["api_max_retries"] = 2
    config_path.write_text(json.dumps(config), encoding="utf-8")
    target = tmp_path / "copied-prefix-input.txt"
    target.write_text("synthetic copied-prefix input", encoding="utf-8")
    script_path = runtime.fake.state_dir / "script.json"
    script = json.loads(script_path.read_text(encoding="utf-8"))
    script["turns"] = [
        [
            acp.message(acp.hermes_tool_call("copy-read", "read_file", {"path": str(target)})),
            acp.result(),
        ],
        [acp.crash(stderr="provider server error")],
        [acp.message("must not publish after copied prefix refusal"), acp.result()],
    ]
    script_path.write_text(json.dumps(script), encoding="utf-8")
    original_reconcile = ServiceLease.reconcile_managed_child_failure
    attempts = []

    def inject_copied_prefix(service, binding, request):
        physical = service.physical
        parent = service.external_parent
        original_prefix = getattr(physical, "_classic_writer_intermediate_prefix_handoff", None)
        assert original_prefix is not None
        aliases_before = {
            "parent_fence": getattr(parent, "_classic_writer_intermediate_fence", None),
            "parent_seal": getattr(parent, "_classic_writer_intermediate_fence_seal", None),
            "receipt_fence": getattr(physical, "_classic_writer_intermediate_fence", None),
            "receipt_seal": getattr(physical, "_classic_writer_intermediate_fence_seal", None),
        }
        copied_prefix = replace(original_prefix)
        # This is the one negative-control fault: only the receipt prefix alias is copied.
        physical._classic_writer_intermediate_prefix_handoff = copied_prefix
        failed_process = physical.process
        result = original_reconcile(service, binding, request)
        attempts.append({
            "result": result, "failed_process": failed_process,
            "physical": physical,
            "service": service,
            "parent": parent,
            "original_prefix": original_prefix,
            "copied_prefix": copied_prefix,
            "prefix_after": getattr(physical, "_classic_writer_intermediate_prefix_handoff", None),
            "aliases_before": aliases_before,
            "aliases_after": {
                "parent_fence": getattr(parent, "_classic_writer_intermediate_fence", None),
                "parent_seal": getattr(parent, "_classic_writer_intermediate_fence_seal", None),
                "receipt_fence": getattr(physical, "_classic_writer_intermediate_fence", None),
                "receipt_seal": getattr(physical, "_classic_writer_intermediate_fence_seal", None),
            },
            "handle": physical.handle,
            "binding": binding,
            "request": request,
        })
        return result

    monkeypatch.setattr(ServiceLease, "reconcile_managed_child_failure", inject_copied_prefix)
    async with TestClient(TestServer(_api_app(runtime.adapter, runtime.runner))) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response, body = await _chat(
            client, HERMES_SESSION_ID, "refuse copied prefix and retain custody",
        )

    assert response.status == 200, body
    assert body.get("status") == "failed", body
    message = body.get("message")
    assert not isinstance(message, dict) or message.get("content") != (
        "must not publish after copied prefix refusal"
    ), body
    assert len(attempts) == 1, attempts
    [attempt] = attempts
    assert attempt["result"] is False
    assert attempt["original_prefix"] is not attempt["copied_prefix"]
    assert attempt["prefix_after"] is attempt["copied_prefix"]
    assert attempt["aliases_after"] == attempt["aliases_before"]
    assert all(
        attempt["aliases_after"][key] is attempt["aliases_before"][key]
        for key in attempt["aliases_before"]
    )
    assert attempt["service"].unknown is True
    assert attempt["physical"].state == "unknown"
    coordinator = attempt["service"].coordinator
    for handle in (attempt["service"].handle, attempt["physical"].handle):
        record = coordinator._records[handle.lease.lease_id]
        assert record.handle is handle and record.state == "unknown"
    assert attempt["physical"].handle is attempt["handle"]
    assert len(_spawn_pids(runtime.fake)) == 2, runtime.fake.records()
    failed_process = attempt["failed_process"]
    assert failed_process.poll() is not None
    failed_pid = failed_process.pid
    assert failed_pid in _spawn_pids(runtime.fake)
    await async_wait_until(
        lambda: _spawn_pids(runtime.fake) - {failed_pid} <= _exit_pids(runtime.fake),
        timeout=10,
        what="the two original ACP children to exit after copied-prefix refusal",
    )
    admissions = list_session_admissions(
        runtime.authority.db, session_id=HERMES_SESSION_ID, pending_only=False,
    )
    assert len(admissions) == 1, admissions
    assert not (
        admissions[0]["status"] == "terminal" and admissions[0].get("outcome") == "completed"
    ), admissions[0]



@pytest.mark.asyncio
async def test_public_child_failure_rejects_residual_prefix_queue_entry_without_new_child(
    production_runtime, tmp_path, monkeypatch,
):
    """A stale same-receipt prefix process cannot be broad-deleted by retry."""
    from agent.copilot_acp_client import CopilotACPClient

    runtime = production_runtime
    config_path = runtime.home / "config.yaml"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["agent"]["api_max_retries"] = 2
    config_path.write_text(json.dumps(config), encoding="utf-8")
    target = tmp_path / "residual-prefix-input.txt"
    target.write_text("synthetic residual-prefix input", encoding="utf-8")
    script_path = runtime.fake.state_dir / "script.json"
    script = json.loads(script_path.read_text(encoding="utf-8"))
    script["turns"] = [
        [
            acp.message(acp.hermes_tool_call("residual-read", "read_file", {"path": str(target)})),
            acp.result(),
        ],
        [acp.crash(stderr="provider server error")],
        [acp.message("must not spawn after residual queue refusal"), acp.result()],
    ]
    script_path.write_text(json.dumps(script), encoding="utf-8")
    original_retry = CopilotACPClient._retry_managed_process_failure
    attempts = []

    def inject_residual_entry(client, parent, service, binding, request):
        receipt = parent._physical
        current_process = receipt.process
        if current_process is None:
            current_process = next(
                item["_process"]
                for item in client._pending_managed_witnesses
                if item.get("receipt") is receipt
                and item.get("_failure_reason")
                and item.get("_process") is not None
            )
        assert current_process is not None
        history = tuple(getattr(receipt, "managed_child_history", ()))
        earlier = next(
            item for item in history
            if item.get("receipt") is receipt
            and item.get("_process") is not None
            and item.get("_process") is not current_process
            and not item.get("_failure_reason")
        )
        earlier_process = earlier["_process"]
        assert earlier_process is not current_process
        snapshot = earlier.get("binding_snapshot")
        assert isinstance(snapshot, dict)
        snapshot_before = dict(snapshot)
        actual_request = service.requests.get(binding.admission_id)
        assert actual_request is request
        residual = dict(earlier)
        residual["_failure_reason"] = "injected_residual_prefix_child"
        assert residual["receipt"] is receipt
        assert residual["parent"] is parent
        assert residual["binding_snapshot"] is snapshot
        assert snapshot["operation_id"] == parent.operation_id
        assert snapshot["profile_id"] == parent.profile_id
        assert snapshot["process_instance"] == parent.process_instance
        assert snapshot["authority_epoch"] == parent.authority_epoch
        assert snapshot["source_identity"] == parent.source_identity
        assert snapshot["admission_id"] == binding.admission_id
        assert snapshot["execution_generation"] == binding.execution_generation
        assert snapshot["hermes_session_id"] == binding.session_id
        client._pending_managed_witnesses.append(residual)
        queued_before = tuple(client._pending_managed_witnesses)
        result = original_retry(client, parent, service, binding, request)
        queued_after = tuple(client._pending_managed_witnesses)
        attempts.append({
            "result": result,
            "residual": residual,
            "residual_process": earlier_process,
            "current_process": current_process,
            "queued_before": queued_before,
            "queued_after": queued_after,
            "receipt": receipt,
            "service": service,
            "parent": parent,
            "physical": receipt,
            "handle": receipt.handle,
            "binding": binding,
            "request": request,
            "request_from_service": actual_request,
            "binding_snapshot": snapshot,
            "binding_snapshot_before": snapshot_before,
        })
        return result

    monkeypatch.setattr(CopilotACPClient, "_retry_managed_process_failure", inject_residual_entry)
    async with TestClient(TestServer(_api_app(runtime.adapter, runtime.runner))) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response, body = await _chat(
            client, HERMES_SESSION_ID, "refuse residual prefix queue and retain custody",
        )

    assert response.status == 200, body
    assert body.get("status") == "failed", body
    message = body.get("message")
    assert not isinstance(message, dict) or message.get("content") != (
        "must not spawn after residual queue refusal"
    ), body
    assert len(attempts) == 1, attempts
    [attempt] = attempts
    assert attempt["result"] is False
    assert attempt["residual_process"] is not attempt["current_process"]
    assert attempt["residual"]["receipt"] is attempt["receipt"]
    assert attempt["residual"]["parent"] is attempt["parent"]
    assert attempt["residual"]["binding_snapshot"] is attempt["binding_snapshot"]
    assert attempt["request_from_service"] is attempt["request"]
    snapshot = attempt["binding_snapshot"]
    assert snapshot == attempt["binding_snapshot_before"]
    assert snapshot["operation_id"] == attempt["parent"].operation_id
    assert snapshot["profile_id"] == attempt["parent"].profile_id
    assert snapshot["process_instance"] == attempt["parent"].process_instance
    assert snapshot["authority_epoch"] == attempt["parent"].authority_epoch
    assert snapshot["source_identity"] == attempt["parent"].source_identity
    assert snapshot["admission_id"] == attempt["binding"].admission_id
    assert snapshot["execution_generation"] == attempt["binding"].execution_generation
    assert snapshot["hermes_session_id"] == attempt["binding"].session_id
    assert any(item is attempt["residual"] for item in attempt["queued_after"])
    assert all(
        any(item is original for item in attempt["queued_after"])
        for original in attempt["queued_before"]
    )
    assert attempt["service"].unknown is True
    assert attempt["physical"].state == "unknown"
    coordinator = attempt["service"].coordinator
    for handle in (attempt["service"].handle, attempt["physical"].handle):
        record = coordinator._records[handle.lease.lease_id]
        assert record.handle is handle and record.state == "unknown"
    assert attempt["parent"]._physical is attempt["physical"]
    assert attempt["physical"].handle is attempt["handle"]
    assert len(_spawn_pids(runtime.fake)) == 2, runtime.fake.records()
    failed_process = attempt["current_process"]
    assert failed_process.poll() is not None
    failed_pid = failed_process.pid
    assert failed_pid in _spawn_pids(runtime.fake)
    await async_wait_until(
        lambda: _spawn_pids(runtime.fake) - {failed_pid} <= _exit_pids(runtime.fake),
        timeout=10,
        what="the two original ACP children to exit after residual queue refusal",
    )



@pytest.mark.asyncio
async def test_public_interrupt_after_tool_checkpoint_finalizes_without_another_child(
    production_runtime, tmp_path, monkeypatch,
):
    """The real interrupt/finalizer closes a tool tail on the exited child's receipt."""
    from agent.session_persistence import SessionPersistenceMixin
    from hermes_state_runtime import list_session_admissions

    runtime = production_runtime
    target = tmp_path / "interrupt-input.txt"
    target.write_text("synthetic interrupted input", encoding="utf-8")
    script_path = runtime.fake.state_dir / "script.json"
    script = json.loads(script_path.read_text(encoding="utf-8"))
    script["turns"] = [
        [acp.message(acp.hermes_tool_call("interrupt-read", "read_file", {"path": str(target)})), acp.result()],
        [acp.message("must not spawn after interruption"), acp.result()],
    ]
    script_path.write_text(json.dumps(script), encoding="utf-8")
    original = SessionPersistenceMixin._flush_messages_to_session_db
    observations = []

    def interrupt_after_checkpoint(agent, messages, conversation_history=None):
        outer = not getattr(agent, "_managed_proof_capture", False)
        result = original(agent, messages, conversation_history)
        if (outer and not observations and messages and messages[-1].get("role") == "tool"
                and getattr(agent, "_classic_writer_intermediate_fence", None) is not None):
            observations.append((agent, agent.interrupt(hard_cancel=True)))
        return result

    monkeypatch.setattr(SessionPersistenceMixin, "_flush_messages_to_session_db", interrupt_after_checkpoint)
    async with TestClient(TestServer(_api_app(runtime.adapter, runtime.runner))) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response, body = await _chat(client, HERMES_SESSION_ID, "read once then stop")
    assert len(observations) == 1
    agent, interrupted = observations[0]
    assert interrupted is True
    assert len(_spawn_pids(runtime.fake)) == 1
    await async_wait_until(lambda: _spawn_pids(runtime.fake) <= _exit_pids(runtime.fake),
                           timeout=10, what="interrupted ACP child to exit")
    assert getattr(agent, "_managed_persistence_failed", False) is not True
    assert getattr(agent, "_classic_writer_intermediate_fence", None) is None
    rows = [row for row in runtime.authority.db.get_messages(HERMES_SESSION_ID)
            if row.get("role") in {"user", "assistant", "tool"}]
    assert any(row.get("role") == "tool" and row.get("tool_call_id") == "interrupt-read" for row in rows)
    assert rows[-1].get("role") == "assistant" and not rows[-1].get("tool_calls")
    [row] = list_session_admissions(runtime.authority.db, session_id=HERMES_SESSION_ID, pending_only=False)
    assert row["status"] == "terminal", {"admission": row, "response_status": response.status, "body": body}
    assert not runtime.authority._preservation_has_sidecar(row["admission_id"])


@pytest.mark.asyncio
async def test_real_gateway_managed_acp_two_turns_persist_and_finalize(production_runtime):
    """Two public API turns use the cached AIAgent, separate ACP sessions, real DB rows, and final delivery."""
    runtime = production_runtime
    app = _api_app(runtime.adapter, runtime.runner)
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        first_response, first_body = await _chat(client, HERMES_SESSION_ID, FIRST_PROMPT)
        assert first_response.status == 200, first_body
        assert first_body.get("message", {}).get("content") == FIRST_REPLY, first_body
        first_agent = _cached_agent(runtime.runner, HERMES_SESSION_ID)
        second_response, second_body = await _chat(client, HERMES_SESSION_ID, SECOND_PROMPT)
        second_agent = _cached_agent(runtime.runner, HERMES_SESSION_ID)

    assert first_response.status == 200, first_body
    assert second_response.status == 200, second_body
    assert first_body["session_id"] == HERMES_SESSION_ID
    assert second_body["session_id"] == HERMES_SESSION_ID
    assert first_body["message"]["content"] == FIRST_REPLY
    assert second_body["message"]["content"] == SECOND_REPLY
    assert second_agent is first_agent, "the second turn must reuse the production cached AIAgent"

    # ACP session/new is a provider-process namespace.  It must never be made equal to the
    # Hermes SessionDB/session-authority namespace to satisfy a managed witness lookup.
    acp_ids = _acp_session_ids(runtime.fake)
    assert len(acp_ids) >= 2, runtime.fake.records()
    assert all(acp_id != HERMES_SESSION_ID for acp_id in acp_ids)
    assert len(_spawn_pids(runtime.fake)) >= 2
    await async_wait_until(
        lambda: _spawn_pids(runtime.fake) <= _exit_pids(runtime.fake),
        timeout=10,
        what="all managed ACP children to emit their natural exit record",
    )
    assert all(record.get("main") for record in runtime.fake.main_prompts())

    from gateway.session_results import admission_result
    from hermes_state_runtime import list_session_admissions

    admissions = list_session_admissions(runtime.authority.db, session_id=HERMES_SESSION_ID, pending_only=False)
    assert len(admissions) == 2, admissions
    assert [row["status"] for row in admissions] == ["terminal", "terminal"]
    assert [row["outcome"] for row in admissions] == ["completed", "completed"]
    admission_ids = [row["admission_id"] for row in admissions]
    assert len(set(admission_ids)) == 2, admissions
    execution_generations = [row["generation"] for row in admissions]
    assert all(type(generation) is int and generation > 0 for generation in execution_generations), admissions
    assert len(set(execution_generations)) == 2, admissions
    assert [row.get("payload", {}).get("text") for row in admissions] == [FIRST_PROMPT, SECOND_PROMPT]
    assert [
        row.get("payload", {}).get("api_turn_v1", {}).get("producer_process_mode")
        for row in admissions
    ] == ["managed_turn", "managed_turn"], admissions
    stored_results = [admission_result(runtime.authority.db, admission_id) for admission_id in admission_ids]
    assert all(isinstance(result, dict) for result in stored_results), stored_results
    assert [result["result"]["final_response"] for result in stored_results] == [FIRST_REPLY, SECOND_REPLY]
    assert all(
        not runtime.authority._preservation_has_sidecar(row["admission_id"])
        for row in admissions
    )

    # The raw SessionDB API returns ordered rows with ``_row_id``; no unsupported
    # include_row_ids keyword is used on ``get_messages``.
    rows = runtime.authority.db.get_messages(HERMES_SESSION_ID)
    turn_row_ids = []
    for admission, stored_result, prompt, reply in zip(
        admissions, stored_results, (FIRST_PROMPT, SECOND_PROMPT), (FIRST_REPLY, SECOND_REPLY), strict=True,
    ):
        turn_row_ids.append(_assert_durable_turn(admission, stored_result, rows, prompt, reply))
    first_user_id, first_reply_id = turn_row_ids[0]
    second_user_id, second_reply_id = turn_row_ids[1]
    assert first_user_id < first_reply_id < second_user_id < second_reply_id, {
        "turn_row_ids": turn_row_ids, "rows": rows,
    }
    assert any(row["role"] == "user" and FIRST_PROMPT in row["content"] for row in rows)
    assert any(row["role"] == "user" and SECOND_PROMPT in row["content"] for row in rows)
    assert any(row["role"] == "assistant" and FIRST_REPLY in row["content"] for row in rows)
    assert any(row["role"] == "assistant" and SECOND_REPLY in row["content"] for row in rows)
    relevant = [row for row in rows if row.get("role") in {"user", "assistant"}]
    assert all(row.get("_row_id") or row.get("id") for row in relevant)


@pytest.mark.asyncio
async def test_api_route_owns_managed_marker_over_client_body(production_runtime):
    """A client-supplied route marker cannot select a producer mode for the persisted API turn."""
    runtime = production_runtime
    app = _api_app(runtime.adapter, runtime.runner)
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response, body = await _chat(
            client, HERMES_SESSION_ID, "client marker must not select direct route",
            extra_body={"producer_process_mode": "direct"},
        )

    assert response.status == 200, body
    assert body["message"]["content"] == FIRST_REPLY
    from hermes_state_runtime import list_session_admissions

    admissions = list_session_admissions(runtime.authority.db, session_id=HERMES_SESSION_ID, pending_only=False)
    assert len(admissions) == 1, admissions
    assert admissions[0]["payload"]["api_turn_v1"]["producer_process_mode"] == "managed_turn"


@pytest_asyncio.fixture
async def multiplex_runtime(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    """Two real profile authorities sharing one mirrored API listener."""
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.api_server import APIServerAdapter
    from gateway.run import GatewayRunner
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership

    root = tmp_path / "multiplex"
    default_home = root / ".hermes"
    beta_home = default_home / "profiles" / "beta"
    fake = acp.AcpFake(root / "acp", [[acp.message("must not spawn"), acp.result()]], models=[MODEL])
    _write_profile(default_home, fake)
    _write_profile(beta_home, fake)
    monkeypatch.setenv("HOME", str(root))
    monkeypatch.setenv("HERMES_HOME", str(default_home))
    monkeypatch.setenv("HERMES_STATE_DB_GUARD_BYPASS", "1")
    monkeypatch.setattr(Path, "home", lambda: root)
    import hermes_state
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", hermes_state._IMPORT_DEFAULT_DB_PATH)
    monkeypatch.setattr(
        "hermes_cli.profiles.profiles_to_serve",
        lambda multiplex: [("default", default_home), ("beta", beta_home)],
    )
    monkeypatch.setattr(
        "hermes_cli.profiles.get_profile_dir",
        lambda name: default_home if name == "default" else beta_home,
    )

    process_ownership.reserve([default_home, beta_home])
    config = GatewayConfig(multiplex_profiles=True, sessions_dir=default_home / "sessions")
    config._runtime_profile_homes = (("default", default_home), ("beta", beta_home))
    runner = GatewayRunner(config)
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={"key": API_KEY}))
    runner.adapters[Platform.API_SERVER] = adapter
    adapter.gateway_runner = runner
    runner._wire_adapter_handlers(adapter)
    try:
        await initialize_gateway_runtime(runner)
        yield RuntimeFixture(
            root, default_home, beta_home, runner, adapter,
            runner.session_authorities.launch, fake, API_KEY,
        )
    finally:
        await runner.stop()
        if process_ownership.owns(beta_home):
            process_ownership.release(beta_home)
        if process_ownership.owns(default_home):
            process_ownership.release(default_home)


@pytest.mark.asyncio
async def test_foreign_profile_target_refuses_before_any_acp_spawn(multiplex_runtime):
    """A real /p/beta request cannot borrow the default SessionDB or start a provider child."""
    runtime = multiplex_runtime
    app = _api_app(runtime.adapter, runtime.runner)
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        before = len(runtime.fake.events("spawn"))
        response, body = await _chat(client, HERMES_SESSION_ID, "foreign profile must refuse", path_prefix="/p/beta")

    assert response.status in {404, 409}, body
    assert len(runtime.fake.events("spawn")) == before
    assert runtime.authority.db.get_session(HERMES_SESSION_ID) is not None
    beta_authority = runtime.runner.session_authorities.for_home(runtime.beta_home)
    assert beta_authority is not None
    assert beta_authority.db.get_session(HERMES_SESSION_ID) is None


@pytest.mark.asyncio
async def test_retired_session_refuses_before_any_acp_spawn(production_runtime):
    """The public API's retired-session fence is exercised before provider construction."""
    runtime = production_runtime
    app = _api_app(runtime.adapter, runtime.runner)
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        retired = await client.patch(
            f"/api/sessions/{HERMES_SESSION_ID}",
            json={"end_reason": "test-retired-target"},
            headers={"Authorization": f"Bearer {API_KEY}"},
        )
        assert retired.status == 200, await retired.text()
        before = len(runtime.fake.events("spawn"))
        response, body = await _chat(client, HERMES_SESSION_ID, "retired target must refuse")

    assert response.status in {400, 404, 409}, body
    assert len(runtime.fake.events("spawn")) == before
