"""HTTP run owners can acknowledge an exact unknown API admission after a secondary epoch successor."""
import asyncio
import json
import socket
import sys
import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import Platform, PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.session_api import restore_api_session
from hermes_state_runtime import RuntimeStoreError, list_session_admissions


_OWNER = ContextVar("test_api_unknown_owner", default="owner")
_PROFILE_NAME = "secondary"
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


async def _retire_registered_authorities(runner):
    """Attempt every registered retirement; retain stores and locks if any refuses."""
    from gateway.run_runtime import _retire_profile_authority

    registry = getattr(runner, "session_authorities", None)
    errors = []
    for authority in tuple(registry) if registry is not None else ():
        if authority is None or getattr(authority, "retired", False):
            continue
        try:
            if not await _retire_profile_authority(authority):
                raise RuntimeError("profile authority retirement failed; ownership retained")
        except BaseException as error:
            errors.append(error)
    if errors:
        raise errors[0]


async def _cancel_join_observer_tasks(tasks, adapters=()):
    """Join every exact task even when one already failed; preserve its first error."""
    retained = list(tasks)
    for adapter in adapters:
        for task in getattr(adapter, "_active_run_tasks", {}).values():
            if not any(task is previous for previous in retained):
                retained.append(task)
    from gateway.run_runtime import TURN_SETTLE_SECONDS

    for task in retained:
        task.cancel()
    if retained:
        _done, pending = await asyncio.wait(retained, timeout=TURN_SETTLE_SECONDS)
        if pending:
            raise RuntimeError("observer join incomplete; ownership retained")
    errors = []
    for task in retained:
        try:
            task.result()
        except asyncio.CancelledError:
            continue
        except BaseException as error:
            errors.append(error)
    return errors


def _release_scope_reservations(homes):
    from gateway.runtime_ownership import ProfileReleaseSummary, process_ownership

    summary = ProfileReleaseSummary({home: process_ownership.release_outcome(home) for home in homes})
    if summary.failed:
        raise RuntimeError("secondary API profile release incomplete: " + summary.describe())


async def _close_owned_runtime(runner, reserved, homes):
    """One teardown owner, ordered after proven retirement and before lock release."""
    from gateway.platforms.api_server_runs import _active_run_custody_count

    if runner is None:
        if reserved:
            _release_scope_reservations(homes)
        return
    await _retire_registered_authorities(runner)
    adapters = tuple(runner._r24b_adapters)
    errors = await _cancel_join_observer_tasks(runner._r24b_tasks, adapters)
    if any(_active_run_custody_count(adapter) for adapter in adapters):
        raise RuntimeError("accepted API custody remains; stores and ownership retained")
    close_errors = []
    for client in runner._r24b_clients:
        try:
            await client.close()
        except BaseException as error:
            close_errors.append(error)
    for adapter in adapters:
        try:
            await adapter.disconnect()
        except BaseException as error:
            close_errors.append(error)
    for close in (runner.session_store.close_all_db_handles, runner.close_all_session_db_handles):
        try:
            close()
        except BaseException as error:
            close_errors.append(error)
    if reserved and not close_errors:
        _release_scope_reservations(homes)
    errors.extend(close_errors)
    if errors:
        raise errors[0]


def _runtime_owned_secondary_api(tmp_path, monkeypatch, *, api_key=None):
    """Real multiplex bootstrap with a launch authority and one served secondary authority."""

    @asynccontextmanager
    async def owned():
        import hermes_state
        from gateway.config import GatewayConfig
        from gateway.run import GatewayRunner
        from gateway.run_runtime import initialize_gateway_runtime
        from gateway.runtime_ownership import process_ownership
        from hermes_constants import reset_hermes_home_override, set_hermes_home_override
        from tests.fakes.fake_llm_provider import FakeLLMServer, write_hermes_home

        launch_home = (tmp_path / "api-profile").resolve()
        secondary_home = (launch_home / "profiles" / "secondary").resolve()
        requested_config = (tmp_path / "config.yaml").read_text() if (tmp_path / "config.yaml").exists() else None
        monkeypatch.setenv("HERMES_HOME", str(launch_home))
        monkeypatch.setattr("gateway.run._hermes_home", launch_home)
        monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", hermes_state._IMPORT_DEFAULT_DB_PATH)
        home_token = set_hermes_home_override(launch_home)
        reserved = False
        runner = None
        adapter = None
        primary_error = None
        try:
            with FakeLLMServer(default_text="runtime-custody-fixture-ok") as provider:
                extra_config = requested_config or ""
                if extra_config.strip() == "{}":
                    extra_config = ""
                write_hermes_home(launch_home, provider.base_url, extra_config=extra_config)
                write_hermes_home(secondary_home, provider.base_url)
                process_ownership.reserve([launch_home, secondary_home])
                reserved = True
                config = GatewayConfig(sessions_dir=launch_home / "sessions")
                config.multiplex_profiles = True
                config._runtime_profile_homes = (("default", launch_home), (_PROFILE_NAME, secondary_home))
                runner = GatewayRunner(config)
                runner._r24b_adapters = []
                runner._r24b_tasks = []
                runner._r24b_clients = []
                extra = {"key": api_key} if api_key else {}
                adapter = APIServerAdapter(PlatformConfig(enabled=True, extra=extra))
                adapter.gateway_runner = runner
                runner.adapters = {Platform.API_SERVER: adapter}
                runner._r24b_adapters = [adapter]
                await initialize_gateway_runtime(runner)
                runner._wire_adapter_handlers(adapter)
                yield adapter
        except BaseException as error:
            primary_error = error
            raise
        finally:
            try:
                await _close_owned_runtime(runner, reserved, (launch_home, secondary_home))
            except BaseException as error:
                if primary_error is None:
                    raise
                primary_error.add_note("Secondary API fixture teardown also failed: " + type(error).__name__)
            finally:
                reset_hermes_home_override(home_token)

    return owned()


def _runs_client(adapter, profile=None):
    @web.middleware
    async def select_owner(request, handler):
        token = _OWNER.set(request.headers.get("X-Test-Owner", "owner"))
        try:
            return await handler(request)
        finally:
            _OWNER.reset(token)

    adapter._expected_api_key = lambda: _API_KEYS[_OWNER.get()]
    app = web.Application(middlewares=[adapter._make_profile_prefix_middleware(), select_owner])
    prefix = "/p/{profile}" if profile else ""
    for method, path, handler in adapter._http_route_table():
        if path.startswith("/v1/runs"):
            app.router.add_route(method, f"{prefix}{path}", handler)
    return TestClient(TestServer(app))


def _profile_path(profile, path):
    return f"/p/{profile}{path}"


def _adapter(runner):
    adapter = APIServerAdapter(PlatformConfig(enabled=True))
    adapter.gateway_runner = runner
    return adapter


async def _admit_and_kill_secondary_daemon(
    root, launch_home, env, api_origin, headers, peer, idempotent, daemon
):
    async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=30)) as client:
        with daemon(root, launch_home, env, barrier=False) as (proc, _desc):
            admission_inputs = (
                ("run-a", "BLOCK_STARTED"),
                ("run-b", "FOLLOWER" if idempotent else "BLOCK_STARTED"),
            )
            admitted = []
            for run_id, text in admission_inputs:
                request_headers = dict(headers)
                if idempotent:
                    request_headers["Idempotency-Key"] = run_id
                deadline = asyncio.get_running_loop().time() + 20
                while True:
                    try:
                        response = await client.post(
                            api_origin + "/p/secondary/v1/runs",
                            json={"input": text, "session_id": "api-session"},
                            headers=request_headers,
                        )
                        break
                    except aiohttp.ClientConnectorError:
                        if asyncio.get_running_loop().time() >= deadline:
                            raise
                        await asyncio.sleep(0.05)
                data = await response.json()
                assert response.status == 202, data
                admitted.append(data)
                if len(admitted) == 1:
                    assert await asyncio.to_thread(peer.blocked.wait, 20)
            assert admitted[0]["run_id"] != admitted[1]["run_id"]
            proc.kill()
            proc.wait(timeout=10)


async def _crash_old_secondary_api(tmp_path, *, idempotent=True):
    """Create the admissions in a real daemon, then kill that owner.

    The successor fixture below is intentionally in-process so the HTTP assertions can
    keep using the existing TestClient.  The owner that creates the unknown row must be
    a genuine OS process, though: a same-process unserve/serve cannot transfer an
    unresolved I03/I05 admission while the typed finalizer is still unsettled.
    """
    from tests.gateway.fixtures.local_recovery_probe import Model, child_env, daemon

    root = Path(__file__).resolve().parents[2]
    launch_home = (tmp_path / "api-profile").resolve()
    secondary_home = (launch_home / "profiles" / _PROFILE_NAME).resolve()
    user_home = (tmp_path / "api-old-user").resolve()
    secondary_home.mkdir(parents=True, exist_ok=True)
    user_home.mkdir(parents=True, exist_ok=True)

    peer = ThreadingHTTPServer(("127.0.0.1", 0), Model)
    peer.requests = []
    import threading

    peer.blocked, peer.release = threading.Event(), threading.Event()
    peer_thread = threading.Thread(target=peer.serve_forever, daemon=True)
    peer_thread.start()
    try:
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            api_port = sock.getsockname()[1]
        model_url = f"http://127.0.0.1:{peer.server_port}/v1"
        api_origin = f"http://127.0.0.1:{api_port}"
        launch_config = {
            "model": {"provider": "custom", "default": "local-wire-stub", "base_url": model_url},
            "platforms": {
                "api_server": {"enabled": True, "port": api_port, "host": "127.0.0.1",
                                "extra": {"key": _API_KEYS["owner"]}},
            },
            "multiplex_profiles": True,
            "auxiliary": {"title_generation": {"enabled": False}},
            "terminal": {"cwd": str(launch_home)},
        }
        secondary_config = {
            "model": {"provider": "custom", "default": "local-wire-stub", "base_url": model_url},
            "platforms": {
                "api_server": {"enabled": True, "extra": {"key": _API_KEYS["owner"]}},
            },
            "auxiliary": {"title_generation": {"enabled": False}},
            "terminal": {"cwd": str(secondary_home)},
        }
        launch_home.mkdir(parents=True, exist_ok=True)
        (launch_home / "config.yaml").write_text(json.dumps(launch_config), encoding="utf-8")
        (secondary_home / "config.yaml").write_text(json.dumps(secondary_config), encoding="utf-8")
        (secondary_home / ".env").write_text(
            f"API_SERVER_KEY={_API_KEYS['owner']}\n", encoding="utf-8"
        )
        env = child_env()
        env.update(
            HOME=str(user_home),
            USERPROFILE=str(user_home),
            HERMES_HOME=str(launch_home),
            PYTHONPATH=str(root),
            OPENAI_API_KEY="loopback-only",
            OPENAI_BASE_URL=model_url,
            API_SERVER_KEY=_API_KEYS["owner"],
            API_SERVER_ENABLED="true",
            API_SERVER_PORT=str(api_port),
            PYTHONUNBUFFERED="1",
        )
        headers = {"Authorization": f"Bearer {_API_KEYS['owner']}"}
        await _admit_and_kill_secondary_daemon(
            root, launch_home, env, api_origin, headers, peer, idempotent, daemon
        )
    finally:
        peer.release.set()
        peer.shutdown()
        peer.server_close()
        peer_thread.join(timeout=5)


async def _restarted_api_runs(tmp_path, monkeypatch, *, idempotent=True):
    await _crash_old_secondary_api(tmp_path, idempotent=idempotent)
    scope = _runtime_owned_secondary_api(tmp_path, monkeypatch, api_key=_API_KEYS["owner"])
    _write_phase(tmp_path, "scope_enter_before", 1)
    adapter = await scope.__aenter__()
    runner = adapter.gateway_runner
    tasks = []
    adapters = [adapter]
    runner._r24b_adapters = adapters
    runner._r24b_tasks = tasks
    client = None
    try:
        _write_phase(tmp_path, "scope_enter_after", 2)
        adapter._runtime_owned_scope = scope
        profile_name = _PROFILE_NAME
        secondary_home = (Path(runner.session_authority.db.db_path).resolve().parent / "profiles" / profile_name).resolve()
        first = runner.session_authorities.for_home(secondary_home)
        assert first is not None
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

        # The rows were admitted by the killed daemon above.  The fresh authority
        # recovers that durable boundary and parks on its unknown head.
        client = _runs_client(adapter, profile_name)
        runner._r24b_clients.append(client)
        _write_phase(tmp_path, "client_start_before", 10)
        await client.start_server()
        _write_phase(tmp_path, "client_start_after", 11)
        rows = list_session_admissions(db, session_id="api-session")
        old_home = Path(db.db_path).parent
        assert len(rows) == 2
        tasks = []
        runner._r24b_tasks = tasks
        restarted = first
        _write_phase(tmp_path, "restore_before", 33)
        from gateway.run import _profile_runtime_scope
        with _profile_runtime_scope(old_home, hydrate_secrets=False):
            restore_api_session(restarted, "api-session")
        _write_phase(tmp_path, "restore_after", 34)
        rows = list_session_admissions(db, session_id="api-session")
        assert [row["status"] for row in rows] == ["unknown", "queued"]
        assert rows[0]["owner_epoch"] < restarted.epoch
        assert restarted.profile_id == first.profile_id

        # A successor adapter is built only after the source-owned old profile retired.
        # Keeping the original adapter here would preserve _run_owners and conceal the recovery gap.
        await adapter.disconnect()
        fresh = _adapter(runner)
        runner._wire_adapter_handlers(fresh)
        fresh._runtime_owned_scope = scope
        # The source-owned primary publication helper is the canonical API adapter registration;
        # the shared listener serves the secondary through /p/{profile}/ mirrors.
        runner._publish_primary_adapter(Platform.API_SERVER, fresh)
        assert runner.adapters[Platform.API_SERVER] is fresh
        adapters.append(fresh)
        client = _runs_client(fresh, profile_name)
        runner._r24b_clients.append(client)
        _write_phase(tmp_path, "fresh_client_start_before", 40)
        await client.start_server()
        _write_phase(tmp_path, "fresh_client_start_after", 41)
        _write_phase(tmp_path, "fixture_ready", 42)
        return client, (adapter, fresh), store, restarted, rows[0], rows[1], executed, tasks
    except BaseException:
        await scope.__aexit__(*sys.exc_info())
        raise


async def _close_fixture(client, adapters, store, tasks):
    primary_error = sys.exc_info()
    scope = getattr(adapters[0], "_runtime_owned_scope", None)
    if scope is None:
        raise RuntimeError("secondary API fixture has no teardown owner")
    await scope.__aexit__(*primary_error)


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
        path = _profile_path(_PROFILE_NAME, "/v1/runs/" + unknown["request_id"] + "/resolve-unknown")
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

        # A current-epoch terminal row is not a successor restart proof; durable presence alone
        # cannot enter the old-owner recovery seam.
        current_follower = next(
            row for row in list_session_admissions(
                owner.db, session_id="api-session", pending_only=False
            )
            if row["admission_id"] == follower["admission_id"]
        )
        current_epoch = await client.post(
            _profile_path(_PROFILE_NAME, "/v1/runs/" + follower["request_id"] + "/resolve-unknown"),
            json={"admission_id": follower["admission_id"],
                  # The real successor increments the queued follower's generation when it
                  # executes. Authenticate the current durable row so this is the intended
                  # current-epoch refusal rather than a stale-generation check.
                  "execution_generation": current_follower["generation"]},
            headers=_AUTH_HEADERS,
        )
        assert current_epoch.status == 409
        assert (await current_epoch.json())["error"]["code"] == "preservation_owner_unmapped"

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
async def test_http_resolve_unknown_retains_current_restart_control_on_readback_failure(
    tmp_path, monkeypatch
):
    """A real control failure retains current I05 custody until the exact operation retries."""
    client, adapters, store, owner, unknown, follower, executed, tasks = (
        await _restarted_api_runs(tmp_path, monkeypatch)
    )
    try:
        body = {
            "admission_id": unknown["admission_id"],
            "execution_generation": unknown["generation"],
        }
        path = _profile_path(_PROFILE_NAME, "/v1/runs/" + unknown["request_id"] + "/resolve-unknown")
        original_resolve = owner.resolve_unknown

        async def fail_readback(*args, **kwargs):
            raise RuntimeStoreError("read_failed")

        owner.resolve_unknown = fail_readback
        failed = await client.post(path, json=body, headers=_AUTH_HEADERS)
        assert failed.status == 409
        assert (await failed.json())["error"]["code"] == "read_failed"
        rows = {
            row["admission_id"]: row["status"]
            for row in list_session_admissions(owner.db, session_id="api-session")
        }
        assert rows == {
            unknown["admission_id"]: "unknown",
            follower["admission_id"]: "queued",
        }
        retained = [
            record for record in owner._preservation_operations.values()
            if record.get("operation") == "resolve_unknown"
            and record.get("admission_id") == unknown["admission_id"]
        ]
        assert len(retained) == 1
        record = retained[0]
        assert record["owner_class"] == "I05_controls_and_unknown"
        assert record["unknown"] is True
        handle = record["handle"]
        assert handle.profile_id == owner.profile_id
        assert handle.authority_epoch == owner.epoch
        assert handle.process_instance == owner.instance_id
        assert isinstance(handle.physical_owner_epoch, str) and handle.physical_owner_epoch
        assert unknown["request_id"] not in adapters[-1]._run_custody

        owner.resolve_unknown = original_resolve
        retried = await client.post(path, json=body, headers=_AUTH_HEADERS)
        assert retried.status == 200, await retried.text()
        receipt = await retried.json()
        assert receipt["outcome"] == "interrupted"
        await owner.sessions["api-session"].task
        assert executed == ["FOLLOWER"]
        again = await client.post(path, json=body, headers=_AUTH_HEADERS)
        assert again.status == 200, await again.text()
        assert await again.json() == receipt
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
        run_path = _profile_path(_PROFILE_NAME, "/v1/runs/" + unknown["request_id"] + "/resolve-unknown")
        stop = await client.post(
            _profile_path(_PROFILE_NAME, "/v1/runs/" + unknown["request_id"] + "/stop"),
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

        # The refusal oracle above intentionally leaves the real unknown head and follower
        # retained. Resolve that exact head once, after the oracle, so fixture teardown can
        # retire the genuine I02/I04 owners instead of force-clearing retained custody.
        cleanup = await client.post(
            run_path,
            json={
                "admission_id": unknown["admission_id"],
                "execution_generation": unknown["generation"],
            },
            headers=_AUTH_HEADERS,
        )
        assert cleanup.status == 200, await cleanup.text()
        await owner.sessions["api-session"].task
        cleanup_statuses = {
            row["admission_id"]: (row["status"], row["outcome"])
            for row in list_session_admissions(
                owner.db, session_id="api-session", pending_only=False
            )
        }
        assert cleanup_statuses[unknown["admission_id"]] == ("terminal", "interrupted")
        assert cleanup_statuses[follower["admission_id"]] == ("terminal", "completed")
        assert executed == ["FOLLOWER"]
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
        path = _profile_path(_PROFILE_NAME, "/v1/runs/" + unknown["request_id"] + "/resolve-unknown")
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
        path = _profile_path(_PROFILE_NAME, "/v1/runs/" + unknown["request_id"])
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
