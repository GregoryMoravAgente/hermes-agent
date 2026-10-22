"""Current-source API finalizer custody controls.

The positive exercises the real GatewayRunner, AIAgent subclass, loopback provider, SessionDB
admission/result, and source-owned cleanup. The small negative controls exercise only typed receipt
and registry refusal boundaries; they never mint a lease or replace the production runner.
"""
from types import SimpleNamespace
import json

import pytest


@pytest.mark.asyncio
async def test_api_finalizer_completion_uses_real_loopback_and_cleanup_readback(
    tmp_path, monkeypatch,
):
    from tests.gateway.conftest import runtime_owned_api
    from tests.gateway.test_api_notification_authority_outcomes import (
        _assert_loopback_provider_used,
        _install_canonical_agent,
        _install_loopback_provider,
        _settled_admission,
    )

    source = {
        "final_response": "current API finalizer completion",
        "completed": True,
        "failed": False,
        "messages": [{"role": "assistant", "content": "current API finalizer completion"}],
    }
    servers = _install_loopback_provider(monkeypatch, source["final_response"])
    from gateway.platforms import api_server_run_completion as completion_module
    settled = []
    original_settle = completion_module.ApiFinalizerCompletion._settle

    def capture_settle(completion, receipt):
        settled.append(receipt)
        return original_settle(completion, receipt)

    monkeypatch.setattr(completion_module.ApiFinalizerCompletion, "_settle", capture_settle)
    async with runtime_owned_api(tmp_path, monkeypatch, cleanup_readback=True) as adapter:
        created = _install_canonical_agent(monkeypatch, source)
        result, usage = await adapter._run_agent(
            "exercise the current owned API run", [],
            session_id="api-finalizer-current",
            request_id="api-finalizer-current-1",
            notification_category="result",
        )

        _assert_loopback_provider_used(servers)
        assert len(created) == 1
        assert result["final_response"] == source["final_response"]
        assert usage["input_tokens"] == 3 and usage["output_tokens"] == 4
        row, stored = _settled_admission(
            adapter, "api-finalizer-current", "api-finalizer-current-1",
        )
        assert row["status"] == "terminal"
        assert stored["result"]["final_response"] == source["final_response"]
        assert adapter._run_custody == {}
        assert adapter._run_launches == {}

    assert len(settled) == 1
    assert settled[0].state == "complete"
    assert settled[0].finalizer_ok is True and settled[0].release_ok is True
    assert settled[0].delivery_effect_state in {"none", "delivered"}


def test_api_delivery_receipt_contract_preserves_effect_states():
    from gateway.platforms.api_server_run_completion import ApiFinalizerCompletion
    from gateway.session_ingress import DeliveryReceipt

    assert ApiFinalizerCompletion._valid_delivery_receipt(
        DeliveryReceipt("api", True, True, effect_state="none"), "api",
    )
    assert ApiFinalizerCompletion._valid_delivery_receipt(
        DeliveryReceipt("api", True, True, effect_state="delivered"), "api",
    )
    assert ApiFinalizerCompletion._valid_delivery_receipt(
        DeliveryReceipt("api", True, False, "delivery_ambiguous", "ambiguous"), "api",
    )
    assert not ApiFinalizerCompletion._valid_delivery_receipt(
        DeliveryReceipt("api", True, True, "delivery_ambiguous", "ambiguous"), "api",
    )
    assert not ApiFinalizerCompletion._valid_delivery_receipt(
        DeliveryReceipt("api", True, False, None, "delivered"), "api",
    )


@pytest.mark.asyncio
async def test_completion_failure_resolves_future_and_retains_unknown():
    import asyncio
    from gateway.platforms.api_server_run_completion import ApiFinalizerCompletion

    future = asyncio.get_running_loop().create_future()
    authority = SimpleNamespace(_preservation_mark_unknown=lambda *_args: (_ for _ in ()).throw(RuntimeError("marker")))
    custody = SimpleNamespace(
        run_id="api-run", profile_id="default", authority_epoch=1,
        process_instance="instance", admission_session_id="session",
        producer_principal="api", handle=SimpleNamespace(close_generation=1, owner_id="child"),
    )
    parent = SimpleNamespace(owner_id="parent")
    completion = ApiFinalizerCompletion(
        authority=authority, custody=custody, parent=parent,
        admission_id="api-admission", future=future,
    )
    receipt = completion.fail("api_turn_cancelled")
    assert receipt.state == "unknown"
    assert receipt.reason == "api_turn_cancelled:marker_refused"
    assert future.done() and future.result() is receipt


def test_completion_registry_non_dict_is_typed_unknown_boundary():
    from gateway.platforms.api_server_run_completion import completion_for

    authority = SimpleNamespace(_api_finalizer_completions=[])
    assert completion_for(authority, "api-finalizer-current-1") is None


def _active_session_stream_app(adapter):
    from aiohttp import web

    app = web.Application()
    app.router.add_post("/api/sessions/{session_id}/chat/stream", adapter._handle_session_chat_stream)
    app.router.add_get("/v1/runs/{run_id}", adapter._handle_get_run)
    app.router.add_post("/v1/runs/{run_id}/resolve-unknown", adapter._handle_resolve_unknown_run)
    return app


def _canonical_runs_app(adapter):
    from aiohttp import web

    app = web.Application()
    app.router.add_post("/v1/runs", adapter._handle_runs)
    app.router.add_get("/v1/runs/{run_id}", adapter._handle_get_run)
    app.router.add_get("/v1/runs/{run_id}/events", adapter._handle_run_events)
    return app


def _sse_run_id(body):
    import json

    for block in body.split("\n\n"):
        for line in block.splitlines():
            if line.startswith("data: "):
                payload = json.loads(line[6:])
                if isinstance(payload, dict) and isinstance(payload.get("run_id"), str):
                    return payload["run_id"]
    raise AssertionError("active stream did not expose a run_id")


def _completion_registries(runner):
    authorities = getattr(runner, "session_authorities", None)
    if not authorities:
        authority = getattr(runner, "session_authority", None)
        authorities = [authority] if authority is not None else []
    registries = [
        getattr(authority, "_api_finalizer_completions", None)
        for authority in authorities
        if authority is not None
    ]
    if not registries or any(not isinstance(registry, dict) for registry in registries):
        raise AssertionError("real runtime did not expose a concrete completion registry")
    return registries


@pytest.mark.asyncio
async def test_active_session_stream_retires_completion_after_real_close(tmp_path, monkeypatch):
    """The real SSE session stream retires its completion after child and parent settlement."""
    from aiohttp.test_utils import TestClient, TestServer
    from tests.gateway.conftest import runtime_owned_api
    from tests.gateway.test_api_notification_authority_outcomes import (
        _assert_loopback_provider_used,
        _install_canonical_agent,
        _install_loopback_provider,
    )

    source = {
        "final_response": "active stream completion",
        "completed": True,
        "failed": False,
        "messages": [{"role": "assistant", "content": "active stream completion"}],
    }
    servers = _install_loopback_provider(monkeypatch, source["final_response"])
    async with runtime_owned_api(tmp_path, monkeypatch, cleanup_readback=True) as adapter:
        from gateway.platforms import api_server_run_custody
        captured = {}
        original_retire_with_completion = api_server_run_custody.retire_legacy_source_with_completion

        def capture_retire_with_completion(custody):
            captured["custody"] = custody
            return original_retire_with_completion(custody)

        monkeypatch.setattr(
            api_server_run_custody,
            "retire_legacy_source_with_completion",
            capture_retire_with_completion,
        )
        created = _install_canonical_agent(monkeypatch, source)
        session_id = adapter._ensure_session_db().create_session("active-stream", "api_server")
        app = _active_session_stream_app(adapter)
        async with TestClient(TestServer(app)) as client:
            response = await client.post(
                f"/api/sessions/{session_id}/chat/stream", json={"message": "stream please"},
            )
            body = await response.text()
            assert response.status == 200, body
            assert "event: run.completed" in body, body
            assert "active stream completion" in body
            run_id = _sse_run_id(body)
            status_response = await client.get(f"/v1/runs/{run_id}")
            status_body = await status_response.text()
            assert status_response.status == 200, status_body
            status_payload = json.loads(status_body)
            assert status_payload.get("run_id") == run_id
            assert status_payload.get("status") in {
                "completed", "failed", "cancelled", "interrupted",
            }
            _assert_loopback_provider_used(servers)
            assert created
            assert adapter._run_custody == {}
            assert adapter._run_launches == {}
            assert all(registry == {} for registry in _completion_registries(adapter.gateway_runner))
            custody = captured["custody"]
            assert custody.mark_worker_done() is None
            assert custody.mark_transport_retired() is None
            assert custody.mark_control_finalizers() is None
            assert custody.unknown_reason is None
            import copy
            for mutate in (
                lambda receipt: None,
                lambda receipt: {**receipt, "parent_owner_id": "foreign-parent"},
                lambda receipt: {**receipt, "close_generation": receipt["close_generation"] + 1},
            ):
                candidate = copy.copy(custody)
                candidate.terminal_release_receipt = mutate(custody.terminal_release_receipt)
                refusal = candidate.try_terminal()
                assert getattr(refusal, "code", None) == "stale_lease"
                assert candidate.unknown_reason == "terminal_release_receipt_missing"


@pytest.mark.asyncio
async def test_canonical_run_events_reader_consumes_real_completion(tmp_path, monkeypatch):
    """The canonical POST /v1/runs transport remains readable through its event surface."""
    import asyncio

    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms.api_server_run_witness import read_run_witness
    from tests.gateway.conftest import runtime_owned_api
    from tests.gateway.test_api_notification_authority_outcomes import (
        _assert_loopback_provider_used,
        _forbid_legacy_factory,
        _install_canonical_agent,
        _install_loopback_provider,
    )

    source = {
        "final_response": "canonical run event completion",
        "completed": True,
        "failed": False,
        "messages": [{"role": "assistant", "content": "canonical run event completion"}],
    }
    servers = _install_loopback_provider(monkeypatch, source["final_response"])
    async with runtime_owned_api(tmp_path, monkeypatch) as adapter:
        created = _install_canonical_agent(monkeypatch, source)
        _forbid_legacy_factory(adapter, monkeypatch)
        async with TestClient(TestServer(_canonical_runs_app(adapter))) as client:
            response = await client.post(
                "/v1/runs",
                json={"input": "canonical event completion", "session_id": "canonical-events"},
            )
            assert response.status == 202, await response.text()
            started = await response.json()
            run_id = started["run_id"]
            launch = adapter._run_launches.get(run_id)
            assert launch is not None and launch.execution_task is not None
            await asyncio.shield(launch.execution_task)

            status_response = await client.get(f"/v1/runs/{run_id}")
            status_body = await status_response.text()
            assert status_response.status == 200, status_body
            status_payload = json.loads(status_body)
            assert status_payload["run_id"] == run_id
            assert status_payload["status"] == "completed"

            events_response = await client.get(f"/v1/runs/{run_id}/events")
            events_body = await events_response.text()
            assert events_response.status == 200, events_body
            assert run_id in events_body
            assert "run.completed" in events_body

        _assert_loopback_provider_used(servers)
        assert len(created) == 1
        custody = adapter._run_custody[run_id]
        witness, refusal = read_run_witness(adapter, custody)
        assert refusal is None, getattr(refusal, "code", None)
        assert witness["run_id"] == run_id
        assert witness["status_receipt"]["readback"] is True
        assert witness["delivery"]["source_readback"] is True
        assert launch.control_finalizer_receipt == ("retired", run_id)
        assert launch.terminal_event_outcome is True
        assert launch.stream_close_outcome is True
        assert custody.released is True


@pytest.mark.asyncio
async def test_active_stream_finalizer_refusal_retains_completion_and_custody(
    tmp_path, monkeypatch,
):
    """A stream finalizer refusal keeps the exact completion registry recoverable."""
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.runtime_custody import CustodyRefusal
    from tests.gateway.conftest import runtime_owned_api
    from tests.gateway.test_api_notification_authority_outcomes import (
        _install_canonical_agent,
        _install_loopback_provider,
    )

    source = {
        "final_response": "stream refusal retention",
        "completed": True,
        "failed": False,
        "messages": [{"role": "assistant", "content": "stream refusal retention"}],
    }
    _install_loopback_provider(monkeypatch, source["final_response"])
    with pytest.raises(RuntimeError, match="api_runtime_cleanup_unproven"):
        async with runtime_owned_api(tmp_path, monkeypatch, cleanup_readback=True) as adapter:
            _install_canonical_agent(monkeypatch, source)
            session_id = adapter._ensure_session_db().create_session("stream-refusal", "api_server")
            from gateway.platforms import api_server_run_execution
            monkeypatch.setattr(
                api_server_run_execution,
                "finalize_legacy_stream_custody",
                lambda *_args: CustodyRefusal(
                    "stream_control_receipt_missing", "I03_api_run_direct"),
            )
            app = _active_session_stream_app(adapter)
            async with TestClient(TestServer(app)) as client:
                response = await client.post(
                    f"/api/sessions/{session_id}/chat/stream", json={"message": "refuse close"},
                )
                body = await response.text()
                assert response.status == 200, body
                assert "stream refusal retention" in body
                run_id = _sse_run_id(body)
                status_response = await client.get(f"/v1/runs/{run_id}")
                status_body = await status_response.text()
                assert status_response.status == 200, status_body
                status_payload = json.loads(status_body)
                assert status_payload.get("run_id") == run_id
                assert status_payload.get("status") in {
                    "completed", "failed", "cancelled", "interrupted",
                }
                assert any(registry for registry in _completion_registries(adapter.gateway_runner))
                assert adapter._run_custody
                assert any(getattr(item, "unknown_reason", None) for item in adapter._run_custody.values())


@pytest.mark.asyncio
async def test_public_unknown_finalizer_retry_requires_exact_source_witness(
    tmp_path, monkeypatch,
):
    """The public retry reconciles one real UNKNOWN only after its source witnesses re-read."""
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms.api_server_run_completion import ApiFinalizerCompletion
    from tests.gateway.conftest import runtime_owned_api
    from tests.gateway.test_api_notification_authority_outcomes import (
        _install_canonical_agent,
        _install_loopback_provider,
    )

    source = {
        "final_response": "public finalizer retry",
        "completed": True,
        "failed": False,
        "messages": [{"role": "assistant", "content": "public finalizer retry"}],
    }
    _install_loopback_provider(monkeypatch, source["final_response"])
    original_settle = ApiFinalizerCompletion.settle_after_child
    injected = {"run_id": None}

    def force_one_unknown(completion):
        if injected["run_id"] is None:
            injected["run_id"] = completion.custody.run_id
            return completion.fail("transient_finalizer_fault")
        return original_settle(completion)

    monkeypatch.setattr(ApiFinalizerCompletion, "settle_after_child", force_one_unknown)
    async with runtime_owned_api(tmp_path, monkeypatch) as adapter:
        _install_canonical_agent(monkeypatch, source)
        session_id = adapter._ensure_session_db().create_session(
            "public-finalizer-retry", "api_server",
        )
        async with TestClient(TestServer(_active_session_stream_app(adapter))) as client:
            response = await client.post(
                f"/api/sessions/{session_id}/chat/stream", json={"message": "retry me"},
            )
            body = await response.text()
            assert response.status == 200, body
            assert "public finalizer retry" in body
            run_id = _sse_run_id(body)
            custody = adapter._run_custody[run_id]
            completion = custody.finalizer_completion
            assert injected["run_id"] == run_id
            assert completion.receipt.state == "unknown"
            assert custody.unknown_reason == "transient_finalizer_fault"
            identity = custody.recovery_identity()
            endpoint = f"/v1/runs/{run_id}/resolve-unknown"

            foreign = {**identity, "owner_id": "foreign-owner"}
            foreign_response = await client.post(endpoint, json=foreign)
            assert foreign_response.status == 409

            stale = {**identity, "close_generation": identity["close_generation"] + 1}
            stale_response = await client.post(endpoint, json=stale)
            assert stale_response.status == 409

            retained_release = custody.terminal_release_receipt
            custody.terminal_release_receipt = {
                **retained_release, "parent_owner_id": "foreign-parent",
            }
            unavailable_response = await client.post(endpoint, json=identity)
            assert unavailable_response.status == 409
            assert completion.receipt.state == "unknown"
            assert adapter._run_custody[run_id] is custody
            custody.terminal_release_receipt = retained_release
            from gateway.platforms import api_server_run_completion as completion_module
            registry = custody.admission_authority._api_finalizer_completions
            assert completion_module.prune_replay_registry(registry) == 0
            assert registry[custody.admission_id] is completion

            resolved_response = await client.post(endpoint, json=identity)
            resolved = await resolved_response.json()
            assert resolved_response.status == 200, resolved
            assert resolved["status"] == "reconciled"
            assert resolved["original_unknown"]["state"] == "unknown"
            assert resolved["completion"]["state"] == "complete"
            assert resolved["identity"] == identity
            assert resolved["source_evidence"]["status_readback"] is True
            assert resolved["source_evidence"]["worker_done"] is True
            assert isinstance(resolved["source_evidence"]["result_commitment"], str)
            assert len(resolved["source_evidence"]["result_commitment"]) == 64
            assert isinstance(resolved["source_evidence"]["transport_commitment"], str)
            assert len(resolved["source_evidence"]["transport_commitment"]) == 64
            assert resolved["child_evidence"]["released"] is True
            assert custody.unknown_reason is None
            assert custody.admission_authority._preservation_handles.get(
                custody.admission_id
            ) is None
            assert custody.admission_authority._preservation_i02_parent_released(
                custody.admission_id, completion.parent
            ) is True
            assert adapter._run_custody.get(run_id) is None
            assert custody.admission_authority._api_finalizer_completions[
                custody.admission_id
            ] is completion
            assert (
                completion.recovery_receipt.original_unknown.reason
                == "transient_finalizer_fault"
            )

            owner = custody.source_launch.owner
            foreign_launch = object()
            owner._run_launches[run_id] = foreign_launch
            try:
                foreign_replay_response = await client.post(endpoint, json=identity)
                assert foreign_replay_response.status == 409
            finally:
                if owner._run_launches.get(run_id) is foreign_launch:
                    owner._run_launches.pop(run_id, None)

            replay_response = await client.post(endpoint, json=identity)
            replay = await replay_response.json()
            assert replay_response.status == 200
            assert replay == resolved

            repeated_replay_response = await client.post(endpoint, json=identity)
            repeated_replay = await repeated_replay_response.json()
            assert repeated_replay_response.status == 200
            assert repeated_replay == resolved
            assert adapter._run_custody.get(run_id) is None
            assert custody.source_launch.owner._run_launches.get(run_id) is None
            stable_replay_response = await client.post(endpoint, json=identity)
            stable_replay = await stable_replay_response.json()
            assert stable_replay_response.status == 200
            assert stable_replay == resolved
            assert registry[custody.admission_id] is completion
            retained_at = completion._replay_retained_at
            assert retained_at is not None
            monkeypatch.setattr(
                completion_module,
                "_replay_now",
                lambda: retained_at + completion_module._REPLAY_AUDIT_TTL_SECONDS + 1,
            )
            assert completion_module.prune_replay_registry(registry) == 1
            assert registry == {}


@pytest.mark.asyncio
async def test_public_unknown_retry_recovers_after_post_release_readback_fault(
    tmp_path, monkeypatch,
):
    """A readback fault after coordinator release retains the exact retry boundary."""
    from aiohttp.test_utils import TestClient, TestServer
    from tests.gateway.conftest import runtime_owned_api
    from tests.gateway.test_api_notification_authority_outcomes import (
        _install_canonical_agent,
        _install_loopback_provider,
    )

    source = {
        "final_response": "post release readback retry",
        "completed": True,
        "failed": False,
        "messages": [{"role": "assistant", "content": "post release readback retry"}],
    }
    _install_loopback_provider(monkeypatch, source["final_response"])
    async with runtime_owned_api(tmp_path, monkeypatch, cleanup_readback=True) as adapter:
        _install_canonical_agent(monkeypatch, source)
        authority = adapter.gateway_runner.session_authority
        original_readback = authority._preservation_i02_parent_released
        injected = {"done": False}

        def fail_after_release(admission_id, parent, release_receipt=None):
            handles = authority._preservation_handles
            if (
                not injected["done"]
                and isinstance(release_receipt, dict)
                and handles.get(admission_id) is None
            ):
                injected["done"] = True
                return False
            return original_readback(admission_id, parent, release_receipt)

        monkeypatch.setattr(authority, "_preservation_i02_parent_released", fail_after_release)
        session_id = adapter._ensure_session_db().create_session(
            "post-release-readback", "api_server",
        )
        async with TestClient(TestServer(_active_session_stream_app(adapter))) as client:
            response = await client.post(
                f"/api/sessions/{session_id}/chat/stream", json={"message": "retry after release"},
            )
            body = await response.text()
            assert response.status == 200, body
            run_id = _sse_run_id(body)
            custody = adapter._run_custody[run_id]
            completion = custody.finalizer_completion
            assert injected["done"] is True
            assert completion.receipt.state == "unknown"
            assert custody.unknown_reason == "api_parent_release_readback_missing"
            assert custody.admission_authority._preservation_handles.get(
                custody.admission_id
            ) is None
            identity = custody.recovery_identity()
            endpoint = f"/v1/runs/{run_id}/resolve-unknown"
            resolved_response = await client.post(endpoint, json=identity)
            resolved = await resolved_response.json()
            assert resolved_response.status == 200, resolved
            assert resolved["completion"]["state"] == "complete"
            assert completion.recovery_receipt is not None
            assert custody.unknown_reason is None
            assert adapter._run_custody.get(run_id) is None

            replay_response = await client.post(endpoint, json=identity)
            replay = await replay_response.json()
            assert replay_response.status == 200
            assert replay == resolved

            repeated_replay_response = await client.post(endpoint, json=identity)
            repeated_replay = await repeated_replay_response.json()
            assert repeated_replay_response.status == 200
            assert repeated_replay == resolved
            assert adapter._run_custody.get(run_id) is None
            assert custody.source_launch.owner._run_launches.get(run_id) is None
            stable_replay_response = await client.post(endpoint, json=identity)
            stable_replay = await stable_replay_response.json()
            assert stable_replay_response.status == 200
            assert stable_replay == resolved
            from gateway.platforms import api_server_run_completion as completion_module
            registry = custody.admission_authority._api_finalizer_completions
            assert registry[custody.admission_id] is completion
            retained_at = completion._replay_retained_at
            assert retained_at is not None
            monkeypatch.setattr(
                completion_module,
                "_replay_now",
                lambda: retained_at + completion_module._REPLAY_AUDIT_TTL_SECONDS + 1,
            )
            assert completion_module.prune_replay_registry(registry) == 1
            assert registry == {}


@pytest.mark.asyncio
async def test_unknown_replay_scans_real_secondary_profile_authority(tmp_path, monkeypatch):
    """The iterable scanner rejects a fabricated secondary entry without source ownership proof."""
    import asyncio

    from gateway.platforms.api_server_run_completion import (
        ApiFinalizerCompletion,
        ApiFinalizerCompletionReceipt,
        ApiFinalizerRecoveryReceipt,
    )
    from gateway.platforms.api_server_run_recovery import _replay_completion_for_run
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership
    from tests.gateway.test_session_authorities_multiplex import _reserve_homes, _runner

    root, homes = _reserve_homes(tmp_path, monkeypatch, names=("secondary",))
    process_ownership.reserve([home for _, home in homes])
    runner = None
    try:
        runner = _runner(root, homes)
        await initialize_gateway_runtime(runner)
        secondary = runner.session_authorities.for_home(homes[1][1])
        assert secondary is not None
        owner = SimpleNamespace(_run_custody={}, _run_launches={})
        launch = SimpleNamespace(owner=owner)
        custody = SimpleNamespace(
            run_id="secondary-replay-run", released=True, unknown_reason=None,
            source_launch=launch,
        )
        admission_id = "secondary-replay-admission"
        receipt = ApiFinalizerCompletionReceipt(
            admission_id=admission_id, run_id=custody.run_id, state="complete",
            profile_id=custody.run_id, authority_epoch=1, process_instance="process",
            close_generation=1, execution_generation=1, session_id="session",
            principal="api", child_owner_id="child", parent_owner_id="parent",
            finalizer_ok=True, release_ok=True, waiter_delivered=True,
            delivery_attempted=True, delivery_delivered=True,
            delivery_effect_state="delivered", delivery_admission_id=admission_id,
        )
        completion = ApiFinalizerCompletion(
            authority=secondary, custody=custody, parent=object(),
            admission_id=admission_id, future=asyncio.get_running_loop().create_future(),
        )
        completion._receipt = receipt
        completion._recovery_receipt = ApiFinalizerRecoveryReceipt(
            admission_id=admission_id, run_id=custody.run_id,
            original_unknown=receipt, recovered_completion=receipt,
            identity=(), source_evidence=(), child_evidence=(),
        )
        secondary._api_finalizer_completions = {admission_id: completion}
        adapter = SimpleNamespace(gateway_runner=runner)

        assert len(tuple(runner.session_authorities)) == len(homes)
        assert _replay_completion_for_run(adapter, custody.run_id) is None
        assert _replay_completion_for_run(adapter, "launch-only-run") is None
    finally:
        if runner is not None:
            for authority in list(getattr(runner, "session_authorities", []) or []):
                authority.db.close()
        for _, home in homes:
            process_ownership.release(home)


@pytest.mark.asyncio
async def test_secondary_profile_api_replay_uses_real_authority_and_rejects_foreign_registry(
    tmp_path, monkeypatch,
):
    """A secondary profile reaches the real finalizer/replay route with exact authority binding."""
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from agent.secret_scope import build_profile_secret_scope
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.api_server import _api_request_profile, APIServerAdapter
    from gateway.platforms.api_server_run_completion import ApiFinalizerCompletion
    from gateway.run import _profile_runtime_scope
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from tests.fakes.fake_llm_provider import FakeLLMServer, write_hermes_home
    from tests.gateway.test_api_notification_authority_outcomes import (
        _forbid_legacy_factory, _install_canonical_agent,
    )
    from tests.gateway.test_session_authorities_multiplex import _reserve_homes, _runner

    source = {
        "final_response": "secondary profile finalizer replay",
        "completed": True,
        "failed": False,
        "messages": [{"role": "assistant", "content": "secondary profile finalizer replay"}],
    }
    root, homes = _reserve_homes(tmp_path, monkeypatch, names=("secondary",))
    process_ownership.reserve([home for _, home in homes])
    home_token = set_hermes_home_override(root)
    secondary_home = homes[1][1]
    secondary_api_key = "secondary-test-api-key-123456"
    auth_headers = {"Authorization": f"Bearer {secondary_api_key}"}
    runner = None
    adapter = None
    try:
        with FakeLLMServer(default_text=source["final_response"]) as provider:
            for _, home in homes:
                write_hermes_home(home, provider.base_url)
            (secondary_home / ".env").write_text(
                "OPENAI_API_KEY=sk-fake-e2e\n"
                f"API_SERVER_KEY={secondary_api_key}\n",
                encoding="utf-8",
            )
            secondary_secrets = build_profile_secret_scope(secondary_home)
            runner = _runner(root, homes)
            adapter = APIServerAdapter(PlatformConfig(enabled=True))
            adapter.gateway_runner = runner
            runner.adapters = {Platform.API_SERVER: adapter}
            await initialize_gateway_runtime(runner)
            runner._wire_adapter_handlers(adapter)
            created = _install_canonical_agent(monkeypatch, source)
            _forbid_legacy_factory(adapter, monkeypatch)
            original_settle = ApiFinalizerCompletion.settle_after_child
            injected = {"run_id": None}

            def force_secondary_unknown(completion):
                if injected["run_id"] is None:
                    injected["run_id"] = completion.custody.run_id
                    return completion.fail("secondary_profile_transient_fault")
                return original_settle(completion)

            monkeypatch.setattr(
                ApiFinalizerCompletion, "settle_after_child", force_secondary_unknown,
            )
            with _profile_runtime_scope(secondary_home, secondary_secrets):
                session_id = adapter._ensure_session_db().create_session(
                    "secondary-finalizer", "api_server",
                )

            @web.middleware
            async def secondary_scope(request, handler):
                profile_token = _api_request_profile.set("secondary")
                try:
                    with _profile_runtime_scope(secondary_home, secondary_secrets):
                        return await handler(request)
                finally:
                    _api_request_profile.reset(profile_token)

            app = web.Application(middlewares=[secondary_scope])
            app.router.add_post(
                "/api/sessions/{session_id}/chat/stream", adapter._handle_session_chat_stream,
            )
            app.router.add_post(
                "/v1/runs/{run_id}/resolve-unknown", adapter._handle_resolve_unknown_run,
            )
            async with TestClient(TestServer(app)) as client:
                response = await client.post(
                    f"/api/sessions/{session_id}/chat/stream",
                    json={"message": "retry secondary"},
                    headers=auth_headers,
                )
                body = await response.text()
                assert response.status == 200, body
                assert provider.requests
                assert len(created) == 1
                run_id = _sse_run_id(body)
                custody = adapter._run_custody[run_id]
                completion = custody.finalizer_completion
                authority = runner.session_authorities.for_home(secondary_home)
                assert authority is custody.admission_authority
                assert injected["run_id"] == run_id
                assert completion.receipt.state == "unknown"
                identity = custody.recovery_identity()
                endpoint = f"/v1/runs/{run_id}/resolve-unknown"

                resolved_response = await client.post(endpoint, json=identity, headers=auth_headers)
                resolved = await resolved_response.json()
                assert resolved_response.status == 200, resolved
                assert resolved["status"] == "reconciled"
                assert custody.unknown_reason is None
                assert adapter._run_custody.get(run_id) is None

                launch_authority = runner.session_authority
                foreign_registry = dict(getattr(launch_authority, "_api_finalizer_completions", {}))
                foreign_registry[custody.admission_id] = completion
                with monkeypatch.context() as foreign_scope:
                    foreign_scope.setattr(
                        launch_authority, "_api_finalizer_completions", foreign_registry,
                        raising=False,
                    )
                    foreign_response = await client.post(
                        endpoint, json=identity, headers=auth_headers,
                    )
                    assert foreign_response.status == 409

                original_profile = custody.profile_id
                custody.profile_id = "foreign-profile"
                try:
                    assert (await client.post(
                        endpoint, json=identity, headers=auth_headers,
                    )).status == 409
                finally:
                    custody.profile_id = original_profile

                original_epoch = custody.authority_epoch
                custody.authority_epoch += 1
                try:
                    assert (await client.post(
                        endpoint, json=identity, headers=auth_headers,
                    )).status == 409
                finally:
                    custody.authority_epoch = original_epoch

                replay_response = await client.post(
                    endpoint, json=identity, headers=auth_headers,
                )
                replay = await replay_response.json()
                assert replay_response.status == 200
                assert replay == resolved
    finally:
        if adapter is not None:
            adapter._response_store.close()
            adapter._run_idempotency_store.close()
        if runner is not None:
            runner.session_store.close_all_db_handles()
            runner.close_all_session_db_handles()
            for authority in list(getattr(runner, "session_authorities", []) or []):
                authority.db.close()
        reset_hermes_home_override(home_token)
        for _, home in homes:
            process_ownership.release(home)



def _completion_retirement_case():
    from gateway.platforms.api_server_run_completion import ApiFinalizerCompletionReceipt
    authority = SimpleNamespace(_api_finalizer_completions={})
    owner = SimpleNamespace(
        _run_custody={}, _run_launches={}, _run_idempotency_ids={"run"},
        _api_run_owner_registered=True,
    )
    launch = SimpleNamespace(owner=owner)
    handle = SimpleNamespace(close_generation=1, owner_id="child")
    custody = SimpleNamespace(
        finalizer_completion=None, released=True, unknown_reason=None,
        admission_authority=authority, admission_id="admission", run_id="run",
        source_launch=launch, handle=handle, _completion_unknown=None,
        mark_completion_unknown=lambda reason: setattr(custody, "_completion_unknown", reason),
    )
    completion = SimpleNamespace(
        custody=custody,
        receipt=ApiFinalizerCompletionReceipt(
            admission_id="admission", run_id="run", state="complete",
            profile_id="default", authority_epoch=1, process_instance="proc",
            close_generation=1, execution_generation=1, session_id="session",
            principal="api", child_owner_id="child", parent_owner_id="parent",
            finalizer_ok=True, release_ok=True, waiter_delivered=True,
            delivery_attempted=True, delivery_delivered=True,
            delivery_effect_state="delivered", delivery_admission_id="admission",
            reason=None,
        ),
        retire=lambda: authority._api_finalizer_completions.pop("admission", None) is not None,
    )
    custody.finalizer_completion = completion
    authority._api_finalizer_completions["admission"] = completion
    owner._run_custody["run"] = custody
    owner._run_launches["run"] = launch
    return custody, completion, authority, owner, launch


@pytest.mark.parametrize(
    "mutation",
    ["stale_completion", "foreign_registry", "missing_registry", "foreign_source", "missing_source"],
)
def test_stream_completion_retirement_refuses_stale_foreign_or_missing_exact_entries(mutation):
    from gateway.platforms import api_server_run_custody

    custody, completion, authority, owner, launch = _completion_retirement_case()
    from dataclasses import replace
    mutation_actions = {
        "stale_completion": lambda: setattr(
            completion, "receipt", replace(completion.receipt, state="unknown")),
        "foreign_registry": lambda: authority._api_finalizer_completions.__setitem__(
            "admission", object()),
        "missing_registry": lambda: authority._api_finalizer_completions.pop("admission"),
        "foreign_source": lambda: owner._run_custody.__setitem__("run", object()),
        "missing_source": lambda: owner._run_custody.pop("run"),
    }
    mutation_actions[mutation]()
    assert api_server_run_custody.retire_legacy_source_with_completion(custody) is False
    assert owner._run_launches.get("run") is launch
    assert owner._run_idempotency_ids == {"run"}
    assert owner._api_run_owner_registered is True
    if mutation == "foreign_registry":
        assert authority._api_finalizer_completions["admission"] is not completion
    elif mutation == "missing_registry":
        assert "admission" not in authority._api_finalizer_completions
    else:
        assert authority._api_finalizer_completions.get("admission") is completion


def test_stream_completion_retirement_rolls_back_exact_refs_after_sync_source_failure(monkeypatch):
    from gateway.platforms import api_server_run_custody

    custody, completion, authority, owner, launch = _completion_retirement_case()

    def fail_after_source_mutation(value):
        assert value is custody
        owner._run_custody.pop("run")
        owner._run_launches.pop("run")
        owner._run_idempotency_ids.discard("run")
        owner._api_run_owner_registered = False
        raise RuntimeError("synchronous source retirement fault")

    monkeypatch.setattr(api_server_run_custody, "retire_legacy_source", fail_after_source_mutation)
    with pytest.raises(RuntimeError, match="synchronous source retirement fault"):
        api_server_run_custody.retire_legacy_source_with_completion(custody)
    assert authority._api_finalizer_completions.get("admission") is completion
    assert owner._run_custody.get("run") is custody
    assert owner._run_launches.get("run") is launch
    assert owner._run_idempotency_ids == {"run"}
    assert owner._api_run_owner_registered is True
    assert completion.receipt.state == "complete"


def _peer_custody_index_case(*, custody_entry=True, launch_entry=True, status_persisted=False):
    from gateway.platforms.api_server_live_bot import release_peer_custody

    release_calls = []
    owner = SimpleNamespace(
        _run_custody={}, _run_launches={},
        _release_run_owner_if_forgotten=lambda run_id: release_calls.append(run_id),
    )
    run_id = "chat-peer-index"
    custody = SimpleNamespace(
        run_id=run_id, released=False, unknown_reason=None, parent_bound=False,
        status_persisted=status_persisted,
        release_unaccepted=lambda: None,
        mark_unknown=lambda reason: setattr(custody, "unknown_reason", reason),
    )
    launch = SimpleNamespace(owner=owner, run_id=run_id, custody=custody, queue=None)
    custody.source_launch = launch
    if custody_entry:
        owner._run_custody[run_id] = custody
    if launch_entry:
        owner._run_launches[run_id] = launch
    return owner, custody, launch, release_calls, release_peer_custody


@pytest.mark.parametrize(
    "mutation",
    [
        "foreign_custody", "foreign_launch", "status_persisted", "parent_bound", "unknown",
        "released_unknown",
    ],
)
def test_peer_custody_release_retains_ambiguous_or_accepted_indexes(mutation):
    owner, custody, launch, release_calls, release_peer_custody = _peer_custody_index_case(
        status_persisted=mutation == "status_persisted",
    )
    foreign_custody = object()
    foreign_launch = object()
    mutation_actions = {
        "foreign_custody": lambda: owner._run_custody.__setitem__(custody.run_id, foreign_custody),
        "foreign_launch": lambda: owner._run_launches.__setitem__(custody.run_id, foreign_launch),
        "status_persisted": lambda: None,
        "parent_bound": lambda: setattr(custody, "parent_bound", True),
        "unknown": lambda: setattr(custody, "unknown_reason", "peer_delivery_pending"),
        "released_unknown": lambda: (
            setattr(custody, "released", True),
            setattr(custody, "unknown_reason", "terminal_release_receipt_missing"),
        ),
    }
    mutation_actions[mutation]()

    release_peer_custody(owner, custody.run_id, custody)

    expected_entries = {
        "foreign_custody": (foreign_custody, launch),
        "foreign_launch": (custody, foreign_launch),
        "status_persisted": (custody, launch),
        "parent_bound": (custody, launch),
        "unknown": (custody, launch),
        "released_unknown": (custody, launch),
    }[mutation]
    assert owner._run_custody[custody.run_id] is expected_entries[0]
    assert owner._run_launches[custody.run_id] is expected_entries[1]
    assert release_calls == []
    expected_unknown = {
        "unknown": "peer_delivery_pending",
        "released_unknown": "terminal_release_receipt_missing",
    }.get(mutation)
    assert custody.unknown_reason == expected_unknown


def test_peer_custody_release_removes_only_exact_unaccepted_pair():
    owner, custody, _launch, release_calls, release_peer_custody = _peer_custody_index_case()

    release_peer_custody(owner, custody.run_id, custody)

    assert owner._run_custody == {}
    assert owner._run_launches == {}
    assert release_calls == [custody.run_id]


@pytest.mark.parametrize("mutation", ["foreign_registry", "foreign_source"])
@pytest.mark.asyncio
async def test_active_stream_postcompletion_refusal_retains_real_receipt(
    mutation, tmp_path, monkeypatch,
):
    """The actual stream boundary retains a typed completion after exact-map replacement."""
    from aiohttp.test_utils import TestClient, TestServer
    from tests.gateway.conftest import runtime_owned_api
    from tests.gateway.test_api_notification_authority_outcomes import (
        _install_canonical_agent,
        _install_loopback_provider,
    )
    from gateway.platforms import api_server_run_custody

    source = {
        "final_response": "postcompletion refusal retention",
        "completed": True,
        "failed": False,
        "messages": [{"role": "assistant", "content": "postcompletion refusal retention"}],
    }
    _install_loopback_provider(monkeypatch, source["final_response"])
    original_complete = api_server_run_custody.complete_legacy_source
    captured = {}

    def mutate_after_completion(custody, *, active_run_id):
        refusal = original_complete(custody, active_run_id=active_run_id)
        completion = custody.finalizer_completion
        if (active_run_id is None and refusal is None
                and getattr(getattr(completion, "receipt", None), "state", None) == "complete"):
            completion = custody.finalizer_completion
            assert completion.receipt is not None
            captured["custody"] = custody
            captured["completion"] = completion
            if mutation == "foreign_registry":
                registry = custody.admission_authority._api_finalizer_completions
                registry[custody.admission_id] = object()
            else:
                owner = custody.source_launch.owner
                owner._run_launches[custody.run_id] = object()
        return refusal

    monkeypatch.setattr(api_server_run_custody, "complete_legacy_source", mutate_after_completion)
    with pytest.raises(RuntimeError, match="api_runtime_cleanup_unproven"):
        async with runtime_owned_api(tmp_path, monkeypatch, cleanup_readback=True) as adapter:
            _install_canonical_agent(monkeypatch, source)
            session_id = adapter._ensure_session_db().create_session(
                "postcompletion-refusal", "api_server",
            )
            async with TestClient(TestServer(_active_session_stream_app(adapter))) as client:
                response = await client.post(
                    f"/api/sessions/{session_id}/chat/stream", json={"message": "retain"},
                )
                body = await response.text()
                assert response.status == 200, body
                assert "postcompletion refusal retention" in body
                run_id = _sse_run_id(body)
                status_response = await client.get(f"/v1/runs/{run_id}")
                status_body = await status_response.text()
                assert status_response.status == 200, status_body
                assert json.loads(status_body).get("run_id") == run_id

            custody = captured["custody"]
            completion = captured["completion"]
            assert completion.receipt.state == "complete"
            assert custody.unknown_reason == "stream_completion_retire_pending"
            assert custody.run_id == run_id
            if mutation == "foreign_registry":
                assert custody.admission_authority._api_finalizer_completions[
                    custody.admission_id
                ] is not completion
                assert adapter._run_custody.get(run_id) is custody
            else:
                assert adapter._run_custody.get(run_id) is custody
                assert custody.source_launch.owner._run_launches[run_id] is not custody.source_launch


@pytest.mark.asyncio
async def test_active_stream_repeated_finalizer_rejects_foreign_release_receipt(
    tmp_path, monkeypatch,
):
    """A live stream retains the exact custody when a repeat sees a foreign child receipt."""
    from aiohttp.test_utils import TestClient, TestServer
    from tests.gateway.conftest import runtime_owned_api
    from tests.gateway.test_api_notification_authority_outcomes import (
        _install_canonical_agent,
        _install_loopback_provider,
    )
    from gateway.platforms.api_server_run_custody import ApiRunCustody

    source = {
        "final_response": "repeated finalizer foreign receipt",
        "completed": True,
        "failed": False,
        "messages": [{"role": "assistant", "content": "repeated finalizer foreign receipt"}],
    }
    _install_loopback_provider(monkeypatch, source["final_response"])
    captured = {}
    original_try_terminal = ApiRunCustody.try_terminal

    def inject_foreign_release_receipt(custody):
        if (
            not captured
            and custody.producer_phase == "api_i02"
            and custody.parent_bound is True
            and custody.parent_handle is not None
            and custody.finalizer_completion is not None
            and custody.released is True
            and isinstance(custody.terminal_release_receipt, dict)
        ):
            captured["custody"] = custody
            captured["completion"] = custody.finalizer_completion
            captured["parent"] = custody.parent_handle
            custody.terminal_release_receipt = {
                **custody.terminal_release_receipt,
                "parent_owner_id": "foreign-parent",
            }
        return original_try_terminal(custody)

    monkeypatch.setattr(ApiRunCustody, "try_terminal", inject_foreign_release_receipt)
    with pytest.raises(RuntimeError, match="api_runtime_cleanup_unproven"):
        async with runtime_owned_api(tmp_path, monkeypatch, cleanup_readback=True) as adapter:
            _install_canonical_agent(monkeypatch, source)
            session_id = adapter._ensure_session_db().create_session(
                "repeated-finalizer-foreign", "api_server",
            )
            async with TestClient(TestServer(_active_session_stream_app(adapter))) as client:
                response = await client.post(
                    f"/api/sessions/{session_id}/chat/stream",
                    json={"message": "retain foreign receipt"},
                )
                body = await response.text()
                assert response.status == 200, body
                assert "repeated finalizer foreign receipt" in body
                run_id = _sse_run_id(body)
                status_response = await client.get(f"/v1/runs/{run_id}")
                status_body = await status_response.text()
                assert status_response.status == 200, status_body
                assert json.loads(status_body)["run_id"] == run_id

            custody = captured["custody"]
            completion = captured["completion"]
            parent = captured["parent"]
            assert custody.run_id == run_id
            assert custody.parent_handle is parent
            assert custody.terminal_release_receipt["parent_owner_id"] == "foreign-parent"
            assert custody.unknown_reason == "terminal_release_receipt_missing"
            assert completion is custody.finalizer_completion
            assert completion.receipt.state == "unknown"
            assert adapter._run_custody.get(run_id) is custody
            assert custody.source_launch.owner._run_launches.get(run_id) is custody.source_launch
            identity = custody.recovery_identity()
            assert identity["owner_id"] == custody.handle.owner_id
            assert identity["profile_id"] == custody.profile_id
            assert identity["authority_epoch"] == custody.authority_epoch
            assert identity["process_instance"] == custody.process_instance
            assert identity["close_generation"] == custody.handle.close_generation
            assert custody.admission_authority._api_finalizer_completions[
                custody.admission_id
            ] is completion
