"""Production shell-hook custody consumers.

The fixture below uses the real GatewayRunner, APIServerAdapter, SessionAuthority,
SessionDB, and AIAgent path.  Only the provider executable is the repository's
schema-valid ACP fake; shell hooks themselves are real local child processes.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from tests.fakes.providers import copilot_acp as acp
from tests.agent.test_copilot_acp_managed_production import (
    API_KEY,
    HERMES_SESSION_ID,
    HERMES_SESSION_KEY,
    MODEL,
    _api_app,
    _cached_agent,
    _create_session,
    _json_response,
    _write_profile,
)


@pytest_asyncio.fixture
async def shell_runtime(request: pytest.FixtureRequest, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Any:
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.api_server import APIServerAdapter
    from gateway.run import GatewayRunner
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership
    from hermes_cli import plugins
    from agent import shell_hooks

    mode = getattr(request, "param", "success")
    root = tmp_path / "hermes"
    home = root / ".hermes"
    hook_log = root / "hook-events.jsonl"
    read_path = Path(__file__).resolve()
    hook_script = root / "hook.py"
    hook_script.parent.mkdir(parents=True, exist_ok=True)
    hook_script.write_text(
        "import json, pathlib, sys\n"
        "payload = json.load(sys.stdin)\n"
        "with pathlib.Path(sys.argv[1]).open('a', encoding='utf-8') as log:\n"
        "    log.write(json.dumps(payload) + '\\n')\n"
        "if payload.get('hook_event_name') == 'pre_tool_call':\n"
        + ({
            "success": "    print('{}')\n",
            "block": "    print(json.dumps({'action': 'block', 'message': 'blocked by production shell hook'}))\n",
            "modify": "    print(json.dumps({'action': 'modify', 'args': {'path': " + repr(str(read_path)) + "}}))\n",
        }[mode])
        + "else:\n    print('{}')\n",
        encoding="utf-8",
    )
    fake = acp.AcpFake(
        root / "acp",
        [[acp.message(acp.hermes_tool_call("call-shell", "read_file", {"path": str(read_path)})), acp.result()]],
        models=[MODEL],
    )
    _write_profile(home, fake)
    monkeypatch.setenv("HOME", str(root))
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_STATE_DB_GUARD_BYPASS", "1")
    monkeypatch.setattr(Path, "home", lambda: root)
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
    shell_hooks.reset_for_tests()
    plugins._plugin_manager = plugins.PluginManager()
    command = f"{sys.executable} {hook_script} {hook_log}"
    shell_hooks.register_from_config(
        {
            "hooks": {
                "pre_tool_call": [{"matcher": "read_file", "command": command}],
                "post_tool_call": [{"matcher": "read_file", "command": command}],
            },
        },
        accept_hooks=True,
    )
    try:
        await initialize_gateway_runtime(runner)
        yield {"runner": runner, "adapter": adapter, "fake": fake, "home": home, "hook_log": hook_log, "mode": mode}
    finally:
        await runner.stop()
        shell_hooks.reset_for_tests()
        if process_ownership.owns(home):
            process_ownership.release(home)


@pytest.mark.asyncio
@pytest.mark.parametrize("shell_runtime", ["success", "block", "modify"], indirect=True)
async def test_public_gateway_shell_hook_custody_matrix(shell_runtime):
    runtime = shell_runtime
    app = _api_app(runtime["adapter"], runtime["runner"])
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": "exercise production shell hook", "provider": "copilot-acp", "model": MODEL},
            headers={"Authorization": f"Bearer {API_KEY}", "X-Hermes-Session-Key": HERMES_SESSION_KEY},
        )
        body = await _json_response(response)

    events = [json.loads(line) for line in runtime["hook_log"].read_text().splitlines()]
    assert {event["hook_event_name"] for event in events} == {"pre_tool_call", "post_tool_call"}
    assert all(event["tool_name"] == "read_file" for event in events)
    if runtime["mode"] == "block":
        assert response.status == 200, body
        assert any(event["hook_event_name"] == "pre_tool_call" for event in events)
        assert not runtime["fake"].main_prompts()[0].get("tool_result")
    else:
        assert response.status == 200, body
        assert body.get("session_id") == HERMES_SESSION_ID
        assert len(runtime["fake"].events("spawn")) >= 2

    admissions = runtime["runner"].session_authority.db
    rows = admissions.get_messages(HERMES_SESSION_ID)
    assert any(row.get("role") == "tool" and row.get("tool_call_id") == "call-shell" for row in rows) or runtime["mode"] == "block"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_point", ["after_request", "after_physical"])
async def test_public_shell_reservation_refusal_reconciles_exact_owner_to_completion(
    monkeypatch, shell_runtime, failure_point,
):
    """A public fault/retry completes through the canonical writer on one exact tuple."""
    runtime = shell_runtime
    authority = runtime["runner"].session_authority
    coordinator = authority._preservation_coordinator()
    from agent import shell_hook_custody
    from gateway import runtime_service_custody
    from gateway.runtime_service_custody import ServiceProcessReceipt
    from gateway.runtime_custody import CustodyRefusal

    refused = False
    original_terminalize = coordinator.mark_terminal_with_secondary_parents

    def refuse_once(*args, **kwargs):
        nonlocal refused
        if not refused:
            refused = True
            return CustodyRefusal("shell_reservation_cleanup_refused")
        return original_terminalize(*args, **kwargs)

    monkeypatch.setattr(coordinator, "mark_terminal_with_secondary_parents", refuse_once)

    reserve_calls: list[tuple[str, Any]] = []
    original_reserve_service = runtime_service_custody.reserve_service

    def capture_reserve(binding, owner_id):
        service = original_reserve_service(binding, owner_id)
        reserve_calls.append((owner_id, service))
        return service

    # This observes root allocation only; the real reserve_service still performs all
    # coordinator validation and allocation.
    monkeypatch.setattr(runtime_service_custody, "reserve_service", capture_reserve)

    constructed: list[tuple[Any, Any, Any, Any]] = []
    original_operation = shell_hook_custody.ShellHookOperation
    operation_failed = False

    def construct_operation(*args, **kwargs):
        nonlocal operation_failed
        constructed.append((kwargs["service"], kwargs["request"], kwargs["receipt"], kwargs["binding"]))
        if failure_point == "after_physical" and not operation_failed:
            operation_failed = True
            raise RuntimeError("shell_operation_publication_boundary")
        return original_operation(*args, **kwargs)

    monkeypatch.setattr(shell_hook_custody, "ShellHookOperation", construct_operation)

    binder_calls: list[tuple[Any, Any, Any]] = []
    if failure_point == "after_request":
        original_bind = authority._preservation_bind_sidecar
        bind_failed = False

        def bind_then_fail(admission_id, parent, service, request):
            nonlocal bind_failed
            original_bind(admission_id, parent, service, request)
            binder_calls.append((service, request, parent))
            if not bind_failed:
                bind_failed = True
                raise RuntimeError("shell_reservation_publication_boundary")

        monkeypatch.setattr(authority, "_preservation_bind_sidecar", bind_then_fail)

    durable_calls: list[tuple[Any, Any, Any, Any]] = []
    original_record = ServiceProcessReceipt.record_result_durable

    def capture_durable(self, binding, request, proof):
        if str(getattr(self, "owner_id", "")).startswith("shell-hook:"):
            durable_calls.append((self, binding, request, proof))
        return original_record(self, binding, request, proof)

    # The production writer/finalizer remains authoritative; this is only a witness on
    # the existing durable handoff, never a replacement receipt or proof.
    monkeypatch.setattr(ServiceProcessReceipt, "record_result_durable", capture_durable)

    app = _api_app(runtime["adapter"], runtime["runner"])
    prompt = "exercise reservation rollback boundary"
    async with TestClient(TestServer(app)) as client:
        await _create_session(client, HERMES_SESSION_ID)
        response = await client.post(
            f"/api/sessions/{HERMES_SESSION_ID}/chat",
            json={"message": prompt, "provider": "copilot-acp", "model": MODEL},
            headers={"Authorization": f"Bearer {API_KEY}", "X-Hermes-Session-Key": HERMES_SESSION_KEY},
        )
        body = await response.text()

    assert response.status == 200, body
    assert refused
    if failure_point == "after_physical":
        assert operation_failed
    else:
        assert bind_failed
        # The first binder call committed the canonical sidecar before raising its
        # publication-boundary error.  Retry must observe that same entry and never bind it
        # a second time.
        assert len(binder_calls) == 1, binder_calls

    # The failed event gets one root allocation.  The retry path may allocate the
    # independent post event, but it may never allocate a successor for this owner.
    if failure_point == "after_request":
        target_service, target_request, target_parent = binder_calls[0]
    else:
        target_service, target_request, target_parent = constructed[0][:3]
        target_parent = constructed[0][3].parent_handle
    target_owner = target_service.owner_id
    assert sum(owner_id == target_owner for owner_id, _service in reserve_calls) == 1, reserve_calls
    carriers = [
        parent for parent in authority.external_process_finalizers()
        if parent.operation_id == target_owner
    ]
    assert len(carriers) == 1
    assert carriers[0]._service is target_service
    assert carriers[0].root_handle is target_service.handle
    target_constructions = [item for item in constructed if item[0] is target_service]
    assert target_constructions
    target_receipt = target_constructions[0][2]
    for service, request, receipt, binding in target_constructions:
        assert service is target_service
        assert request is target_request
        assert binding.parent_handle is target_parent
        for handle in (service.handle, request, receipt.handle, binding.parent_handle):
            assert handle.profile_id == binding.profile_id
            assert handle.process_instance == binding.process_instance
            assert handle.authority_epoch == binding.authority_epoch
    if failure_point == "after_physical":
        assert len(target_constructions) == 2
        assert target_constructions[0][2] is target_constructions[1][2]

    # Canonical admission/result and durable delivery are the primary proof.  The private
    # operation/sidecar maps below are intentionally not used as completion evidence.
    from gateway.session_results import admission_result
    from hermes_state_runtime import list_session_admissions

    db = runtime["runner"].session_authority.db
    admissions = list_session_admissions(db, session_id=HERMES_SESSION_ID, pending_only=False)
    matches = [row for row in admissions if row.get("payload", {}).get("text") == prompt]
    assert len(matches) == 1, admissions
    admission = matches[0]
    assert admission["status"] == "terminal", admission
    assert admission["outcome"] == "completed", admission
    stored = admission_result(db, admission["admission_id"])
    assert isinstance(stored.get("result", {}).get("final_response"), str)
    assert stored["result"]["final_response"]
    assert durable_calls
    assert any(
        receipt is target_receipt and request is target_request
        for receipt, _binding, request, _proof in durable_calls
    )

    agent = _cached_agent(runtime["runner"], HERMES_SESSION_ID)
    assert getattr(agent, "_managed_persistence_failed", False) is False
    parent = agent._managed_acp_client._producer_policy.parent
    assert agent._managed_acp_client._managed_service_is_reusable() is True
    assert parent.state == "request_reserved"
    assert parent._physical is None

