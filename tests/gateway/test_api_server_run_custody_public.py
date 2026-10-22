"""First public I03 consumer slice for API-run custody.

Each case uses a real GatewayRunner, profile ownership, SessionAuthority, APIServerAdapter, and
its aiohttp routes.  The loopback provider is the only fake boundary; admissions, canonical rows,
I03/I02 leases, status writer, event stream, and recovery route remain production paths.
Bot, late shielded admission, and the wider SSE/disconnect matrix are separate follow-up cases.
"""

import asyncio

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer


def _public_app(adapter):
    from gateway.platforms.api_server import cors_middleware, security_headers_middleware

    app = web.Application(
        middlewares=[mw for mw in (cors_middleware, security_headers_middleware) if mw is not None]
    )
    app["api_server_adapter"] = adapter
    app.router.add_post("/v1/runs", adapter._handle_runs)
    app.router.add_post("/v1/chat/completions", adapter._handle_chat_completions)
    app.router.add_get("/v1/runs/{run_id}", adapter._handle_get_run)
    app.router.add_get("/v1/runs/{run_id}/events", adapter._handle_run_events)
    app.router.add_post("/v1/runs/{run_id}/resolve-unknown", adapter._handle_resolve_unknown_run)
    return app


@pytest_asyncio.fixture
async def actual_api_run(tmp_path, monkeypatch):
    """One initialized profile owner with the real API adapter and public route app."""
    import hermes_state
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.api_server import APIServerAdapter
    from gateway.run import GatewayRunner
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from tests.fakes.fake_llm_provider import FakeLLMServer, write_hermes_home

    home = (tmp_path / "profile").resolve()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr("gateway.run._hermes_home", home)
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", hermes_state._IMPORT_DEFAULT_DB_PATH)
    home_token = set_hermes_home_override(home)
    reserved = False
    runner = None
    adapter = None
    with FakeLLMServer(default_text="public-custody-ok") as provider:
        write_hermes_home(home, provider.base_url)
        try:
            process_ownership.reserve([home])
            reserved = True
            runner = GatewayRunner(GatewayConfig(sessions_dir=home / "sessions"))
            adapter = APIServerAdapter(PlatformConfig(enabled=True))
            adapter.gateway_runner = runner
            runner.adapters = {Platform.API_SERVER: adapter}
            await initialize_gateway_runtime(runner)
            runner._wire_adapter_handlers(adapter)
            yield {
                "adapter": adapter,
                "authority": runner.session_authority,
                "app": _public_app(adapter),
                "provider": provider,
                "runner": runner,
            }
        finally:
            if adapter is not None:
                adapter._response_store.close()
                adapter._run_idempotency_store.close()
            if runner is not None:
                runner.session_store.close_all_db_handles()
                runner.close_all_session_db_handles()
            if reserved:
                process_ownership.close()
            reset_hermes_home_override(home_token)


async def _await_original_run(adapter, run_id):
    """Await the exact retained dispatch Task; no polling or replacement task is accepted."""
    launch = adapter._run_launches.get(run_id)
    assert launch is not None
    assert launch.execution_task is not None
    await asyncio.shield(launch.execution_task)
    return launch


async def _completed_run(client, adapter, *, headers=None, session_id=None):
    payload = {"input": "return a deterministic custody result"}
    if session_id is not None:
        payload["session_id"] = session_id
    response = await client.post("/v1/runs", json=payload, headers=headers or {})
    assert response.status == 202, await response.text()
    started = await response.json()
    run_id = started["run_id"]
    await _await_original_run(adapter, run_id)
    status_response = await client.get(f"/v1/runs/{run_id}", headers=headers or {})
    assert status_response.status == 200, await status_response.text()
    return run_id, await status_response.json()


async def _wait_for_unknown(adapter, run_id):
    launch = await _await_original_run(adapter, run_id)
    custody = adapter._run_custody.get(run_id)
    assert launch.control_finalizer_receipt == ("retired", run_id)
    assert custody is not None and custody.unknown_reason
    return custody


async def _wait_for_launch_finalizer(adapter, run_id):
    launch = await _await_original_run(adapter, run_id)
    assert launch.control_finalizer_receipt == ("retired", run_id)
    return launch


@pytest.mark.asyncio
async def test_public_api_positive_consumes_original_launch_witness(actual_api_run):
    """A real POST/GET route leaves source-owned launch and terminal receipts readable."""
    from gateway.platforms.api_server_run_witness import read_run_witness

    adapter, app = actual_api_run["adapter"], actual_api_run["app"]
    provider = actual_api_run["provider"]
    async with TestClient(TestServer(app)) as client:
        run_id, status = await _completed_run(client, adapter, session_id="public-positive")
        launch = await _wait_for_launch_finalizer(adapter, run_id)
    assert provider.main_requests()
    assert status["status"] == "completed"
    custody = adapter._run_custody[run_id]
    witness, refusal = read_run_witness(adapter, custody)
    assert refusal is None, getattr(refusal, "code", None)
    assert witness["run_id"] == run_id
    assert witness["status_receipt"]["readback"] is True
    assert witness["delivery"]["source_readback"] is True
    assert launch.owner is adapter and launch.custody is custody
    assert launch.execution_task.done() is True
    assert launch.status_receipt is not None
    assert launch.control_finalizer_receipt == ("retired", run_id)
    assert launch.terminal_event_outcome is True
    assert launch.stream_close_outcome is True
    assert custody.released is True


@pytest.mark.asyncio
async def test_public_api_forged_recovery_identity_is_refused(actual_api_run, monkeypatch):
    """A copied close identity cannot replace the original source-owned recovery handle."""
    from gateway.platforms.api_server_runs import _RunLaunch

    original_put_event = _RunLaunch.put_event

    def refuse_terminal_sentinel(run, event):
        if event is None:
            return False
        return original_put_event(run, event)

    monkeypatch.setattr(_RunLaunch, "put_event", refuse_terminal_sentinel)
    adapter, app = actual_api_run["adapter"], actual_api_run["app"]
    async with TestClient(TestServer(app)) as client:
        run_id, _status = await _completed_run(client, adapter, session_id="forged-recovery")
        custody = await _wait_for_unknown(adapter, run_id)
        await _wait_for_launch_finalizer(adapter, run_id)
        forged = custody.recovery_identity()
        forged["close_generation"] += 1
        response = await client.post(f"/v1/runs/{run_id}/resolve-unknown", json=forged)
        body = await response.json()
    assert response.status == 409
    assert body["error"]["code"] == "run_custody_identity_mismatch"
    assert adapter._run_custody[run_id] is custody
    assert custody.released is False


@pytest.mark.asyncio
async def test_public_api_terminal_idempotent_replay_reads_same_store_row(actual_api_run):
    """A keyed retry replays the existing run before reserving another I03/I02 parent."""
    adapter, app = actual_api_run["adapter"], actual_api_run["app"]
    headers = {"Idempotency-Key": "r7-public-terminal-replay"}
    async with TestClient(TestServer(app)) as client:
        run_id, status = await _completed_run(
            client, adapter, headers=headers, session_id="terminal-replay")
        assert status["status"] == "completed"
        launches_before = dict(adapter._run_launches)
        custody_before = dict(adapter._run_custody)
        replay = await client.post(
            "/v1/runs", json={"input": "return a deterministic custody result", "session_id": "terminal-replay"},
            headers=headers)
        replay_body = await replay.json()
    assert replay.status == 202
    assert replay_body == {"run_id": run_id, "status": "completed", "replayed": True}
    assert adapter._run_launches == launches_before
    assert adapter._run_custody == custody_before


@pytest.mark.asyncio
async def test_public_legacy_request_id_replays_canonical_session_result(actual_api_run):
    """Chat Completions' stable request_id reuses the canonical terminal admission."""
    app = actual_api_run["app"]
    provider = actual_api_run["provider"]
    headers = {"Idempotency-Key": "r7-legacy-request-replay"}
    body = {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "return a legacy custody result"}],
    }
    async with TestClient(TestServer(app)) as client:
        first = await client.post("/v1/chat/completions", json=body, headers=headers)
        assert first.status == 200, await first.text()
        first_body = await first.json()
        second = await client.post("/v1/chat/completions", json=body, headers=headers)
        assert second.status == 200, await second.text()
        second_body = await second.json()
    assert first_body["choices"][0]["message"]["content"] == "public-custody-ok"
    assert second_body["choices"][0]["message"]["content"] == first_body["choices"][0]["message"]["content"]
    assert len(provider.main_requests()) == 1


@pytest.mark.asyncio
async def test_public_legacy_no_authority_uses_durable_status_writer(actual_api_run, monkeypatch):
    """The supported pre-authority Chat route seals its real worker with a durable receipt."""
    from gateway.platforms import api_server_run_custody
    from gateway.platforms.api_server_run_witness import read_run_witness

    adapter, app, provider = (
        actual_api_run["adapter"], actual_api_run["app"], actual_api_run["provider"])
    runner = actual_api_run["runner"]
    # This is the legacy runtime topology exercised by api_server_openai_routes: no authority
    # or profile registry owns the turn, while the initialized runner, DB and provider remain real.
    runner.session_authority = None
    runner.session_authorities = None
    receipts = []
    original_update = adapter._run_idempotency_store.update_status_receipt

    def observe_update(*args, **kwargs):
        receipt = original_update(*args, **kwargs)
        receipts.append(receipt)
        return receipt

    monkeypatch.setattr(adapter._run_idempotency_store, "update_status_receipt", observe_update)
    original_retire = api_server_run_custody.retire_legacy_source
    retired = {}

    def observe_retire(custody):
        witness, refusal = read_run_witness(adapter, custody)
        retired.update({"custody": custody, "launch": custody.source_launch,
                        "witness": witness, "witness_refusal": refusal})
        return original_retire(custody)

    monkeypatch.setattr(api_server_run_custody, "retire_legacy_source", observe_retire)
    body = {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "return a legacy custody result"}],
    }
    async with TestClient(TestServer(app)) as client:
        response = await client.post("/v1/chat/completions", json=body)
        response_body = await response.json()
    assert response.status == 200, response_body
    assert response_body["choices"][0]["message"]["content"] == "public-custody-ok"
    assert provider.main_requests()
    assert len(receipts) >= 2
    terminal = next(receipt for receipt in reversed(receipts)
                     if receipt["status"]["status"] == "completed")
    assert terminal["storage"] == "run_idempotency"
    assert terminal["rowcount"] == 1
    assert terminal["readback"] is True
    assert terminal["idempotency_key"].startswith("legacy:")
    stored = adapter._run_idempotency_store.status_for_run(
        terminal["scope"], terminal["run_id"])
    assert stored is not None
    assert stored["scope"] == terminal["scope"]
    assert stored["idempotency_key"] == terminal["idempotency_key"]
    assert stored["fingerprint"] == terminal["fingerprint"]
    assert stored["status"]["status"] == "completed"
    custody = retired["custody"]
    launch = retired["launch"]
    assert retired["witness_refusal"] is None
    assert launch.worker_future is custody.worker_future
    assert launch.execution_task is custody.execution_task
    assert launch.worker_future is launch.execution_task
    assert launch.execution_task.done() is True
    assert custody.unknown_reason is None
    assert custody.released is True


@pytest.mark.asyncio
async def test_public_legacy_writer_refusal_closes_exact_no_worker_boundary(actual_api_run, monkeypatch):
    """A running receipt refusal retains the exact reservation and finalizes no-worker custody."""
    adapter, app = actual_api_run["adapter"], actual_api_run["app"]
    runner = actual_api_run["runner"]
    runner.session_authority = None
    runner.session_authorities = None
    original_update = adapter._run_idempotency_store.update_status_receipt
    calls = []

    def refuse_running(run_id, status, **kwargs):
        calls.append(status.get("status"))
        if len(calls) == 1:
            return None
        return original_update(run_id, status, **kwargs)

    monkeypatch.setattr(adapter._run_idempotency_store, "update_status_receipt", refuse_running)
    body = {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "writer refusal should retain custody"}],
    }
    before = set(adapter._run_custody)
    async with TestClient(TestServer(app)) as client:
        response = await client.post("/v1/chat/completions", json=body)
        response_body = await response.json()
    assert response.status == 500, response_body
    assert calls[0] == "running"
    run_ids = set(adapter._run_custody) - before
    assert len(run_ids) == 1
    run_id = run_ids.pop()
    custody = adapter._run_custody[run_id]
    launch = adapter._run_launches[run_id]
    assert custody.worker_submitted is False
    assert custody.no_worker_finalized is True
    assert custody.worker_done is True
    assert custody.unknown_reason == "status_commit_unproven"
    assert custody.transport_retired is True
    assert custody.control_finalizers is True
    assert launch.worker_future is None
    stored = adapter._run_idempotency_store.status_for_run(custody.reservation_scope, run_id)
    assert stored is not None
    assert stored["status"]["status"] == "failed"


@pytest.mark.asyncio
async def test_public_legacy_worker_failure_still_consumes_original_launch(actual_api_run, monkeypatch):
    """A real provider failure settles through the same direct launch and source finalizers."""
    from gateway.platforms import api_server_run_custody
    from gateway.platforms.api_server_run_witness import read_run_witness
    from tests.fakes.fake_llm_provider import Error

    adapter, app, provider = (
        actual_api_run["adapter"], actual_api_run["app"], actual_api_run["provider"])
    runner = actual_api_run["runner"]
    runner.session_authority = None
    runner.session_authorities = None
    # A permanent 4xx avoids the client's transient recovery cycle and drives the real worker
    # exception into the direct source finalizer.
    monkeypatch.setattr(
        provider, "_responder", lambda _request: Error(status=400, message="synthetic provider failure")
    )
    original_retire = api_server_run_custody.retire_legacy_source
    retired = {}

    def observe_retire(custody):
        witness, refusal = read_run_witness(adapter, custody)
        retired.update({"custody": custody, "launch": custody.source_launch,
                        "witness": witness, "witness_refusal": refusal})
        return original_retire(custody)

    monkeypatch.setattr(api_server_run_custody, "retire_legacy_source", observe_retire)
    body = {
        "model": "fake-model",
        "messages": [{"role": "user", "content": "provider failure remains source-owned"}],
    }
    async with TestClient(TestServer(app)) as client:
        response = await client.post("/v1/chat/completions", json=body)
        await response.text()
    assert response.status in (200, 502)
    custody = retired["custody"]
    launch = retired["launch"]
    assert retired["witness_refusal"] is None
    assert retired["witness"]["status_receipt"]["status"]["status"] == "failed"
    assert launch.worker_future is custody.worker_future
    assert launch.execution_task is custody.execution_task
    assert launch.execution_task.done() is True
    assert custody.released is True
    assert custody.unknown_reason is None


@pytest.mark.asyncio
async def test_public_api_same_handle_unknown_retry_reconciles_after_transport_retirement(
    actual_api_run, monkeypatch
):
    """After source transport retirement, only the original unknown handle can recover."""
    from gateway.platforms.api_server_runs import _RunLaunch

    original_put_event = _RunLaunch.put_event

    def refuse_terminal_sentinel(run, event):
        if event is None:
            return False
        return original_put_event(run, event)

    monkeypatch.setattr(_RunLaunch, "put_event", refuse_terminal_sentinel)
    adapter, app = actual_api_run["adapter"], actual_api_run["app"]
    async with TestClient(TestServer(app)) as client:
        run_id, _status = await _completed_run(client, adapter, session_id="same-handle")
        custody = await _wait_for_unknown(adapter, run_id)
        await _wait_for_launch_finalizer(adapter, run_id)
        identity = custody.recovery_identity()
        wrong_owner = dict(identity, owner_id="copied-owner")
        refused = await client.post(f"/v1/runs/{run_id}/resolve-unknown", json=wrong_owner)
        assert refused.status == 409
        adapter._run_streams_created[run_id] -= adapter._RUN_STREAM_TTL + 1
        adapter._sweep_orphaned_runs_once()
        resolved = await client.post(f"/v1/runs/{run_id}/resolve-unknown", json=identity)
        resolved_body = await resolved.json()
    assert resolved.status == 200
    assert resolved_body == {"run_id": run_id, "status": "resolved", "custody": "original_handle"}
    assert run_id not in adapter._run_custody
