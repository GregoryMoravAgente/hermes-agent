"""HTTP run owners can acknowledge an exact unknown API admission after restart."""
import asyncio
import json
import time
from contextlib import suppress
from contextvars import ContextVar
from pathlib import Path
from types import SimpleNamespace

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.session_api import restore_api_session
from hermes_state_runtime import claim_session_input, list_session_admissions
from tests.gateway.conftest import bind_authority_runtime_custody, runtime_owned_api


_OWNER = ContextVar("test_api_unknown_owner", default="owner")
_API_KEYS = {"owner": "checkpoint-two-api-key", "other": "other-valid-api-key"}
_AUTH_HEADERS = {"Authorization": f"Bearer {_API_KEYS['owner']}", "X-Test-Owner": "owner"}
_OTHER_AUTH_HEADERS = {
    "Authorization": f"Bearer {_API_KEYS['other']}", "X-Test-Owner": "other"
}


_PHASE_FILE = "api-unknown-resolution-phases.jsonl"


def _write_phase(tmp_path, phase, ordinal):
    record = {"phase": phase, "ordinal": ordinal, "monotonic_ns": time.monotonic_ns()}
    with (Path(tmp_path) / _PHASE_FILE).open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, separators=(",", ":")) + "\n")


def _write_adapter_phase(adapters, phase, ordinal):
    authority = getattr(adapters[-1].gateway_runner, "session_authority", None)
    db = getattr(authority, "db", None)
    db_path = getattr(db, "db_path", None)
    if db_path is not None:
        _write_phase(Path(db_path).resolve().parent.parent, phase, ordinal)


def _runs_client(adapter):
    @web.middleware
    async def select_owner(request, handler):
        token = _OWNER.set(request.headers.get("X-Test-Owner", "owner"))
        try:
            return await handler(request)
        finally:
            _OWNER.reset(token)

    adapter._expected_api_key = lambda: _API_KEYS[_OWNER.get()]
    app = web.Application(middlewares=[select_owner])
    for method, path, handler in adapter._http_route_table():
        if path.startswith("/v1/runs"):
            app.router.add_route(method, path, handler)
    return TestClient(TestServer(app))


def _adapter(runner, db):
    adapter = APIServerAdapter(PlatformConfig(enabled=True))
    adapter.gateway_runner = runner
    adapter._session_db = db
    return adapter


async def _restarted_api_runs(tmp_path, monkeypatch, *, idempotent=True):
    scope = runtime_owned_api(tmp_path, monkeypatch, api_key=_API_KEYS["owner"])
    _write_phase(tmp_path, "scope_enter_before", 1)
    adapter = await scope.__aenter__()
    _write_phase(tmp_path, "scope_enter_after", 2)
    adapter._runtime_owned_scope = scope
    runner = adapter.gateway_runner
    first = runner.session_authority
    db = first.db
    store = runner.session_store
    executed = []

    async def answer(event):
        from gateway.session_results import execution_result

        executed.append(event.text)
        result = {"final_response": "ACK_" + event.text, "messages": []}
        execution_result.get()["result"] = result
        return result["final_response"]

    runner._handle_message = answer
    runner._wire_adapter_handlers(adapter)
    runner._adapter_for_source = lambda source: adapter

    # Exercise the public admission handler so run ownership and the canonical
    # principal/request-id binding are the same ones the resolution route sees.
    first._schedule = lambda ref: None
    client = _runs_client(adapter)
    _write_phase(tmp_path, "client_start_before", 10)
    await client.start_server()
    _write_phase(tmp_path, "client_start_after", 11)
    tasks = []
    try:
        admitted = []
        inputs = (("run-a", "HEAD"), ("run-b", "FOLLOWER")) if idempotent else (
            ("run-a", "SAME BODY"), ("run-b", "SAME BODY"))
        for run_id, text in inputs:
            headers = dict(_AUTH_HEADERS)
            if idempotent:
                headers["Idempotency-Key"] = run_id
            _write_phase(tmp_path, "admission_post_before", len(admitted))
            response = await client.post(
                "/v1/runs",
                json={"input": text, "session_id": "api-session"},
                headers=headers,
            )
            _write_phase(tmp_path, "admission_post_after", len(admitted))
            assert response.status == 202, await response.text()
            admitted.append(await response.json())
        assert admitted[0]["run_id"] != admitted[1]["run_id"]
        tasks = list(adapter._active_run_tasks.values())
        await asyncio.sleep(0)

        _write_phase(tmp_path, "claim_before", 20)
        rows = list_session_admissions(db, session_id="api-session")
        head = claim_session_input(db, epoch=first.epoch, session_id="api-session")
        _write_phase(tmp_path, "claim_after", 21)
        assert head["admission_id"] == rows[0]["admission_id"]

        from gateway.session_authority import initialize_session_authority
        _write_phase(tmp_path, "bootstrap_before", 30)
        restarted = await initialize_session_authority(
            runner, profile_id=first.profile_id, instance_id="api-owner-2"
        )
        _write_phase(tmp_path, "bootstrap_after", 31)
        bind_authority_runtime_custody(runner, restarted)
        _write_phase(tmp_path, "custody_bind_after", 32)
        # Model a new owner process for this single reserved profile: publish the new
        # authority in a fresh registry before restoring the durable session.
        from gateway.session_authorities import SessionAuthorities
        registry = SessionAuthorities(db.db_path.parent)
        registry.add(db.db_path.parent, restarted)
        runner.session_authorities = registry
        runner.session_authority = restarted
        _write_phase(tmp_path, "restore_before", 33)
        restore_api_session(restarted, "api-session")
        _write_phase(tmp_path, "restore_after", 34)
        rows = list_session_admissions(db, session_id="api-session")
        assert [row["status"] for row in rows] == ["unknown", "queued"]
        _write_phase(tmp_path, "old_client_close_before", 35)
        await client.close()
        _write_phase(tmp_path, "old_client_close_after", 36)

        # A process restart constructs a new API adapter.  Keeping the original
        # adapter here would preserve _run_owners and conceal the recovery gap.
        fresh = _adapter(runner, db)
        runner._wire_adapter_handlers(fresh)
        fresh._runtime_owned_scope = scope
        runner._adapter_for_source = lambda source: fresh
        client = _runs_client(fresh)
        _write_phase(tmp_path, "fresh_client_start_before", 40)
        await client.start_server()
        _write_phase(tmp_path, "fresh_client_start_after", 41)
        _write_phase(tmp_path, "fixture_ready", 42)
        return client, (adapter, fresh), store, restarted, rows[0], rows[1], executed, tasks
    except Exception:
        _write_phase(tmp_path, "fixture_exception", 90)
        await client.close()
        adapter._response_store.close()
        adapter._run_idempotency_store.close()
        _write_phase(tmp_path, "scope_exit_before", 91)
        await scope.__aexit__(None, None, None)
        _write_phase(tmp_path, "scope_exit_after", 92)
        raise


async def _close_fixture(client, adapters, store, tasks):
    _write_adapter_phase(adapters, "cleanup_begin", 100)
    for task in tasks:
        task.cancel()
    for task in tasks:
        with suppress(asyncio.CancelledError):
            await task
    _write_adapter_phase(adapters, "cleanup_client_before", 101)
    await client.close()
    _write_adapter_phase(adapters, "cleanup_client_after", 102)
    for adapter in adapters:
        adapter._response_store.close()
        adapter._run_idempotency_store.close()
    scope = getattr(adapters[0], "_runtime_owned_scope", None)
    if scope is None:
        adapters[-1].gateway_runner.session_authority.db.close()
        store.close_all_db_handles()
    else:
        _write_adapter_phase(adapters, "cleanup_scope_before", 103)
        await scope.__aexit__(None, None, None)
        _write_adapter_phase(adapters, "cleanup_scope_after", 104)


@pytest.mark.asyncio
async def test_http_resolve_unknown_releases_follower_once_without_replaying_head(
    tmp_path, monkeypatch
):
    client, adapters, store, owner, unknown, follower, executed, tasks = (
        await _restarted_api_runs(tmp_path, monkeypatch)
    )
    try:
        _write_phase(tmp_path, "case_request_before", 110)
        body = {
            "admission_id": unknown["admission_id"],
            "execution_generation": unknown["generation"],
        }
        path = "/v1/runs/" + unknown["request_id"] + "/resolve-unknown"
        unauthenticated = await client.post(path, json=body)
        assert unauthenticated.status == 401
        assert (await unauthenticated.json())["error"]["code"] == "gateway_auth_failed"

        capabilities = json.loads((await adapters[-1]._handle_capabilities(
            SimpleNamespace(headers=_AUTH_HEADERS, remote="127.0.0.1"))).text)
        assert capabilities["features"]["run_unknown_resolution"] is True
        assert capabilities["endpoints"]["run_unknown_resolution"]["path"] == "/v1/runs/{run_id}/resolve-unknown"

        response = await client.post(path, json=body, headers=_AUTH_HEADERS)
        assert response.status == 200, await response.text()
        result = await response.json()
        assert result["run_id"] == unknown["request_id"]
        assert result["admission_id"] == unknown["admission_id"]
        assert result["status"] == "terminal" and result["outcome"] == "interrupted"

        await owner.sessions["api-session"].task
        assert executed == ["FOLLOWER"]
        statuses = {
            row["admission_id"]: (row["status"], row["outcome"])
            for row in list_session_admissions(owner.db, session_id="api-session", pending_only=False)
        }
        assert statuses[unknown["admission_id"]] == ("terminal", "interrupted")
        assert statuses[follower["admission_id"]] == ("terminal", "completed")

        # A lost-ack retry of the exact resolution replays its committed receipt; nothing runs twice.
        again = await client.post(path, json=body, headers=_AUTH_HEADERS)
        assert again.status == 200, await again.text()
        assert await again.json() == result
        await owner.sessions["api-session"].task
        assert executed == ["FOLLOWER"]
        stale = await client.post(path, json=body | {"execution_generation": unknown["generation"] + 1},
                                  headers=_AUTH_HEADERS)
        assert stale.status == 409 and (await stale.json())["error"]["code"] == "stale_generation"
    finally:
        await _close_fixture(client, adapters, store, tasks)


@pytest.mark.asyncio
async def test_http_resolve_unknown_refuses_stale_or_foreign_admission(
    tmp_path, monkeypatch
):
    client, adapters, store, owner, unknown, follower, executed, tasks = (
        await _restarted_api_runs(tmp_path, monkeypatch)
    )
    try:
        _write_phase(tmp_path, "case_request_before", 110)
        run_path = "/v1/runs/" + unknown["request_id"] + "/resolve-unknown"
        stop = await client.post(
            "/v1/runs/" + unknown["request_id"] + "/stop",
            headers=_AUTH_HEADERS,
        )
        assert stop.status == 409
        assert (await stop.json())["error"]["code"] == "unknown_execution"
        attempts = (
            ({
                "admission_id": unknown["admission_id"],
                "execution_generation": unknown["generation"] + 1,
            }, "stale_generation"),
            ({
                "admission_id": follower["admission_id"],
                "execution_generation": unknown["generation"],
            }, "not_found"),
            ({
                "admission_id": unknown["admission_id"],
                "execution_generation": False,
            }, "stale_generation"),
            ({
                "admission_id": unknown["admission_id"],
                "execution_generation": unknown["generation"],
                "extra": True,
            }, "invalid_params"),
            ([unknown["admission_id"], unknown["generation"]], "invalid_params"),
        )
        for body, code in attempts:
            response = await client.post(run_path, json=body, headers=_AUTH_HEADERS)
            assert response.status == (400 if code == "invalid_params" else 409)
            assert (await response.json())["error"]["code"] == code

        # A refused resolution must not release the FIFO: let any drain it scheduled run to the end
        # (the restored drain parks on the unknown head) before reading what it left behind.
        drain = owner.sessions["api-session"].task
        if drain is not None:
            await asyncio.wait_for(drain, 10)
        rows = {
            row["admission_id"]: row["status"]
            for row in list_session_admissions(owner.db, session_id="api-session")
        }
        assert rows == {
            unknown["admission_id"]: "unknown",
            follower["admission_id"]: "queued",
        }
        assert executed == []
    finally:
        await _close_fixture(client, adapters, store, tasks)


@pytest.mark.asyncio
async def test_fresh_adapter_recovers_non_idempotent_owner_but_refuses_other_credential(
    tmp_path, monkeypatch
):
    client, adapters, store, _owner, unknown, _follower, _executed, tasks = (
        await _restarted_api_runs(tmp_path, monkeypatch, idempotent=False)
    )
    try:
        _write_phase(tmp_path, "case_request_before", 110)
        path = "/v1/runs/" + unknown["request_id"] + "/resolve-unknown"
        body = {
            "admission_id": unknown["admission_id"],
            "execution_generation": unknown["generation"],
        }

        foreign = await client.post(path, json=body, headers=_OTHER_AUTH_HEADERS)
        assert foreign.status == 404
        assert (await foreign.json())["error"]["code"] == "run_not_found"

        response = await client.post(path, json=body, headers=_AUTH_HEADERS)
        assert response.status == 200, await response.text()
        assert (await response.json())["outcome"] == "interrupted"
    finally:
        await _close_fixture(client, adapters, store, tasks)


@pytest.mark.asyncio
async def test_http_poll_reports_unknown_head_as_non_terminal_until_resolved(
    tmp_path, monkeypatch
):
    """The documented discovery path: a client polls GET /v1/runs/{id}, learns the head is
    unknown (not finished), reads admission_id/execution_generation from that body and
    resolves it. ``interrupted`` is in TERMINAL_STATUSES and documented as terminal, so a
    poller cannot tell a paused head from a finished run if the projection reuses it."""
    from gateway.platforms.api_server_run_idempotency import TERMINAL_STATUSES

    client, adapters, store, owner, unknown, _follower, executed, tasks = (
        await _restarted_api_runs(tmp_path, monkeypatch)
    )
    try:
        _write_phase(tmp_path, "case_request_before", 110)
        path = "/v1/runs/" + unknown["request_id"]
        polled = await client.get(path, headers=_AUTH_HEADERS)
        assert polled.status == 200, await polled.text()
        body = await polled.json()
        print("POLL_BEFORE_RESOLVE", json.dumps({k: body.get(k) for k in (
            "status", "admission_id", "execution_generation")}, sort_keys=True))
        assert body["status"] not in TERMINAL_STATUSES, body
        assert body["status"] == "unknown", body
        assert body["admission_id"] == unknown["admission_id"]
        assert body["execution_generation"] == unknown["generation"]

        resolved = await client.post(path + "/resolve-unknown", headers=_AUTH_HEADERS, json={
            "admission_id": body["admission_id"],
            "execution_generation": body["execution_generation"],
        })
        assert resolved.status == 200, await resolved.text()
        await owner.sessions["api-session"].task
        assert executed == ["FOLLOWER"]

        after = await (await client.get(path, headers=_AUTH_HEADERS)).json()
        print("POLL_AFTER_RESOLVE", json.dumps({"status": after.get("status")}))
        assert after["status"] in TERMINAL_STATUSES, after
    finally:
        await _close_fixture(client, adapters, store, tasks)
