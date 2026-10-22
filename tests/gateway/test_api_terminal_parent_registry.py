"""Real HTTP/FakeLLM controls for the post-I03-terminal completion boundary.

These tests fault only the existing authority completion registry after the real I03 source
custody has terminalized. They do not mint handles, receipts, providers, or a second ledger.
"""

import json

import pytest


@pytest.mark.parametrize("mutation", ["missing_registry", "foreign_registry"])
@pytest.mark.asyncio
async def test_real_http_terminal_i03_retains_i02_parent_on_registry_loss(
    mutation, tmp_path, monkeypatch,
):
    """A real I03 terminal child must not turn a managed I02 into a direct observer."""
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms.api_server_run_completion import ApiFinalizerCompletion
    from tests.gateway.conftest import runtime_owned_api
    from tests.gateway.test_api_finalizer_completion_current import (
        _active_session_stream_app,
        _sse_run_id,
    )
    from tests.gateway.test_api_notification_authority_outcomes import (
        _assert_loopback_provider_used,
        _install_canonical_agent,
        _install_loopback_provider,
    )

    source = {
        "final_response": "post-I03 terminal registry boundary",
        "completed": True,
        "failed": False,
        "messages": [{
            "role": "assistant",
            "content": "post-I03 terminal registry boundary",
        }],
    }
    servers = _install_loopback_provider(monkeypatch, source["final_response"])
    foreign_session_id = "foreign-terminal-registry"
    target_session_id = "target-terminal-registry"
    foreign = {}
    target = {}
    original_settle = ApiFinalizerCompletion.settle_after_child

    def observe_settlement(completion):
        custody = completion.custody
        session_id = getattr(custody, "admission_session_id", None)
        if session_id == foreign_session_id and "completion" not in foreign:
            # Let the real completion settle first.  This callback runs before
            # the source-retirement step, so every identity assertion below is
            # made against the live source-owned maps rather than a reinserted
            # or synthetic registry entry.
            result = original_settle(completion)
            assert result is not None
            assert result.state == "complete"
            assert completion.receipt is not None
            assert completion.receipt.state == "complete"
            assert completion.receipt is result
            assert completion.custody is custody
            assert custody.released is True
            assert custody.unknown_reason is None
            assert custody.parent_handle is not None
            assert custody.parent_handle is completion.parent
            launch = custody.source_launch
            owner = launch.owner
            registry = custody.admission_authority._api_finalizer_completions
            assert owner._run_custody[custody.run_id] is custody
            assert owner._run_launches[custody.run_id] is launch
            assert registry[completion.admission_id] is completion
            foreign.update(
                completion=completion,
                custody=custody,
                launch=launch,
                parent=custody.parent_handle,
            )
            foreign_run_id = custody.run_id
            assert adapter._run_custody[foreign_run_id] is foreign["custody"]
            assert adapter._run_launches[foreign_run_id] is foreign["launch"]
            assert adapter._run_launches[foreign_run_id] is foreign["custody"].source_launch
            assert registry[foreign["completion"].admission_id] is foreign["completion"]
            return result

        if session_id == target_session_id and "registry" not in target:
            authority = custody.admission_authority
            registry = authority._api_finalizer_completions
            parent = custody.parent_handle
            assert registry[custody.admission_id] is completion
            assert custody.released is True
            assert custody.coordinator.has_active_bound_child(
                parent, "I03_api_run_direct"
            ) is False
            target["registry"] = registry
            target["completion"] = completion
            target["custody"] = custody
            target["parent"] = parent
            target["launch"] = custody.source_launch
            if mutation == "missing_registry":
                registry.pop(custody.admission_id)
            else:
                assert foreign.get("completion") is not None
                assert foreign["completion"].receipt is not None
                assert foreign["completion"].receipt.state == "complete"
                assert foreign["completion"].admission_id != custody.admission_id
                registry[custody.admission_id] = foreign["completion"]

        return original_settle(completion)

    # The foreign object is captured after real settlement and before normal retirement.
    monkeypatch.setattr(ApiFinalizerCompletion, "settle_after_child", observe_settlement)

    contract_verified = False
    runtime_context = runtime_owned_api(tmp_path, monkeypatch, cleanup_readback=True)
    adapter = await runtime_context.__aenter__()
    try:
        _install_canonical_agent(monkeypatch, source)
        app = _active_session_stream_app(adapter)
        async with TestClient(TestServer(app)) as client:
            if mutation == "foreign_registry":
                foreign_session = adapter._ensure_session_db().create_session(
                    foreign_session_id, "api_server",
                )
                foreign_response = await client.post(
                    f"/api/sessions/{foreign_session}/chat/stream",
                    json={"message": "create real foreign completion"},
                )
                foreign_body = await foreign_response.text()
                assert foreign_response.status == 200, foreign_body
                assert source["final_response"] in foreign_body
                foreign_run_id = _sse_run_id(foreign_body)
                assert foreign["completion"].custody.run_id == foreign_run_id
                assert foreign["completion"].receipt.state == "complete"
                assert foreign["parent"] is foreign["completion"].parent

            target_session = adapter._ensure_session_db().create_session(
                target_session_id, "api_server",
            )
            response = await client.post(
                f"/api/sessions/{target_session}/chat/stream",
                json={"message": "lose completion registry"},
            )
            body = await response.text()
            assert response.status == 200, body
            assert "event: run.completed" in body, body
            assert source["final_response"] in body
            run_id = _sse_run_id(body)
            status_response = await client.get(f"/v1/runs/{run_id}")
            status_body = await status_response.text()
            assert status_response.status == 200, status_body
            assert json.loads(status_body)["run_id"] == run_id
            _assert_loopback_provider_used(servers)

            custody = target["custody"]
            completion = target["completion"]
            parent = target["parent"]
            authority = custody.admission_authority
            registry = target["registry"]
            coordinator = custody.coordinator
            parent_lease_id = parent.lease.lease_id
            assert custody.run_id == run_id
            assert custody.released is True
            assert custody.terminal_release_receipt["released"] is True
            assert coordinator.has_active_bound_child(
                parent, "I03_api_run_direct"
            ) is False
            assert completion.receipt.state == "unknown"
            assert completion.receipt.reason == "finalizer_completion_registry_missing"
            assert custody.unknown_reason == "finalizer_completion_registry_missing"
            assert authority._preservation_handles[custody.admission_id] is parent
            assert coordinator._records[parent_lease_id].handle is parent
            assert adapter._run_custody[run_id] is custody
            assert adapter._run_launches[run_id] is target["launch"]
            assert authority._preservation_i02_parent_released(
                custody.admission_id, parent
            ) is False

            if mutation == "missing_registry":
                assert custody.admission_id not in registry
            else:
                assert registry[custody.admission_id] is foreign["completion"]
                assert foreign["completion"].custody is foreign["custody"]

            identity = custody.recovery_identity()
            resolve_response = await client.post(
                f"/v1/runs/{run_id}/resolve-unknown", json=identity,
            )
            resolve_body = await resolve_response.text()
            assert resolve_response.status == 409, resolve_body
            assert authority._preservation_handles[custody.admission_id] is parent
            assert adapter._run_custody[run_id] is custody
            assert adapter._run_launches[run_id] is target["launch"]
            contract_verified = True

    finally:
        with pytest.raises(RuntimeError, match="api_runtime_cleanup_unproven"):
            await runtime_context.__aexit__(None, None, None)
    # Keep an expected cleanup refusal from replacing a failed contract assertion in the body.
    # This is deliberately outside pytest.raises so the cleanup exception cannot mask the body.
    assert contract_verified is True
