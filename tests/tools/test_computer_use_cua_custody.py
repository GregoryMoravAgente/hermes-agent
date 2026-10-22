"""Focused custody tests for the ordinary host CUA stdio finalizer."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import sys
import uuid

import pytest

from gateway.runtime_custody import CustodyRefusal, RuntimeCustodyCoordinator
from gateway.runtime_service_operations import ExternalProcessParent
from gateway.runtime_sidecar_cua import CuaStdioOwner


pytestmark = pytest.mark.platforms("posix")


def _parent() -> ExternalProcessParent:
    coordinator = RuntimeCustodyCoordinator(
        process_instance="cua-test-process", authority_epoch=1,
    )
    root = coordinator.reserve_root(
        "I16_turn_sidecars", "cua-test-operation",
        profile_id="cua-test-profile", authority_epoch=1,
    )
    assert not isinstance(root, CustodyRefusal)
    return ExternalProcessParent(
        mode="standalone_maintenance",
        profile_home=Path("/tmp/cua-test-profile"),
        profile_id="cua-test-profile",
        operation_id="cua-test-operation",
        source_identity="cua-driver:test",
        principal_id=None,
        process_instance="cua-test-process",
        authority_epoch=1,
        coordinator=coordinator,
        root_handle=root,
        physical_owner_epoch=None,
    )


@asynccontextmanager
async def _managed_owned_process(tmp_path: Path, monkeypatch):
    """Run the positive receipt through a real admitted API turn and writer fence."""
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from functools import wraps
    import gateway.platforms.api_server_run_dispatch as run_dispatch
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    from gateway.runtime_service_custody import current_turn_binding
    from gateway.runtime_sidecar_cua import captured_cua_stdio, prepare_cua_session
    from gateway.session_results import admission_result
    from hermes_state_runtime import get_session_admission
    from tests.gateway.conftest import runtime_owned_api

    server_code = (
        "import json,sys\n"
        "for line in sys.stdin:\n"
        " try: request=json.loads(line)\n"
        " except Exception: continue\n"
        " method=request.get('method')\n"
        " if method == 'notifications/initialized': continue\n"
        " if method == 'initialize': result={'protocolVersion':'2025-03-26','capabilities':{'tools':{}},'serverInfo':{'name':'cua-managed-fixture','version':'1'}}\n"
        " elif method == 'tools/list': result={'tools':[]}\n"
        " elif method == 'ping': result={}\n"
        " else: result={}\n"
        " if 'id' in request: print(json.dumps({'jsonrpc':'2.0','id':request['id'],'result':result}),flush=True)\n"
    )
    owner_holder = {}
    claimed_holder = {}
    operation_holder = {}

    async def run_public_api_turn(api, authority):
        """Submit through the real /v1/runs producer and consume its finalizer response."""
        fixture_key = "cua-r16-fixture-api-key-0123456789"
        api._api_key = fixture_key
        session_id = "cua-managed-custody"
        db = api._ensure_session_db()
        if db.get_session(session_id) is None:
            db.create_session(session_id=session_id, source="api_server", model="hermes-agent")
        app = web.Application()
        app["api_server_adapter"] = api
        app.router.add_post("/v1/runs", api._handle_runs)
        app.router.add_get("/v1/runs/{run_id}", api._handle_get_run)
        original_launch = run_dispatch._launch

        @wraps(original_launch)
        async def observing_launch(adapter, request, selected, *, api_server):
            response = await original_launch(adapter, request, selected, api_server=api_server)
            run_id = selected["run_id"]
            launch = adapter._run_launches[run_id]
            operation_task = launch.execution_task
            assert isinstance(operation_task, asyncio.Task)
            assert adapter._active_run_tasks[run_id] is operation_task
            operation_holder["task"] = operation_task
            operation_holder["launch"] = launch
            return response

        async with TestClient(TestServer(app)) as client:
            run_dispatch._launch = observing_launch
            try:
                response = await client.post(
                    "/v1/runs",
                    json={"model": "hermes-agent", "input": "run the managed CUA custody control",
                          "session_id": session_id},
                    headers={"Authorization": f"Bearer {fixture_key}",
                             "Idempotency-Key": "cua-managed-positive"},
                )
            finally:
                run_dispatch._launch = original_launch
            raw = await response.read()
            assert response.status in (200, 202), raw.decode(errors="replace")
            accepted = json.loads(raw)
            run_id = accepted.get("run_id")
            assert isinstance(run_id, str) and run_id
            operation_task = operation_holder["task"]
            assert operation_task is operation_holder["launch"].execution_task
            await operation_task
            drain_task = claimed_holder["drain_task"]
            drain_result = await drain_task
            assert drain_result is None or isinstance(drain_result, str)
            status_response = await client.get(
                f"/v1/runs/{run_id}", headers={"Authorization": f"Bearer {fixture_key}"},
            )
            status_raw = await status_response.read()
            assert status_response.status == 200, status_raw.decode(errors="replace")
            status = json.loads(status_raw)
            assert status.get("run_id") == run_id
            assert status.get("status") == "completed"
            return accepted

    async with runtime_owned_api(tmp_path, monkeypatch) as api:
        authority = api.gateway_runner.session_authority
        original_handler = authority.runner._handle_message

        async def actual_handler(event):
            binding = current_turn_binding()
            assert binding is not None
            claimed = get_session_admission(authority.db, admission_id=binding.admission_id)
            assert claimed is not None and claimed['status'] == 'started'
            assert claimed['generation'] == binding.execution_generation
            live = authority.sessions[binding.session_id]
            drain_task = asyncio.current_task()
            assert drain_task is not None and live.task is drain_task
            claimed_holder['row'] = claimed
            claimed_holder['session_id'] = binding.session_id
            claimed_holder['drain_task'] = drain_task
            owner = prepare_cua_session(
                session_id='cua-managed-custody', permission_mode='standard',
            )
            owner_holder['owner'] = owner
            params = StdioServerParameters(
                command=sys.executable, args=['-c', server_code],
            )
            async def drive_sdk():
                async with captured_cua_stdio(stdio_client, params, owner) as streams:
                    async with owner.observe_session(ClientSession(*streams)) as session:
                        await session.initialize()
                        tools = await session.list_tools()
                        assert getattr(tools, 'tools', None) == []

            sdk_task = asyncio.create_task(drive_sdk())
            await sdk_task
            return await original_handler(event)

        authority.runner._handle_message = actual_handler
        try:
            accepted = await run_public_api_turn(api, authority)
        finally:
            authority.runner._handle_message = original_handler

        assert isinstance(accepted.get('run_id'), str)
        owner = owner_holder['owner']
        parent = owner.parent
        service = getattr(parent, '_service', None)
        physical = owner.receipt.physical
        assert parent.mode == 'managed_turn'
        assert service is not None and service.external_parent is parent
        assert physical is not None and physical.service is service
        assert parent._physical is physical
        assert not service.requests
        assert not service.durable_requests
        assert physical.completed_requests
        claimed = claimed_holder['row']
        terminal = get_session_admission(authority.db, admission_id=claimed['admission_id'])
        assert terminal is not None and terminal['status'] == 'terminal'
        assert terminal['owner_epoch'] == claimed['owner_epoch']
        assert terminal['generation'] == claimed['generation']
        saved = admission_result(authority.db, claimed['admission_id'])
        assert isinstance(saved, dict) and isinstance(saved.get('result'), dict)
        yield owner


async def _run_owned_process(owner: CuaStdioOwner, *, fail_session: bool = False) -> None:
    async def lifecycle() -> None:
        owner.bind_task(asyncio.current_task())
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-c", "import sys; sys.exit(0)",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        owner.receipt.bind_process(process, (process.stdout, process.stdin))
        owner.receipt.bind_session(object(), asyncio.current_task())
        if fail_session:
            owner.receipt.close_session(RuntimeError("session-body-failed"), None)
        else:
            owner.receipt.close_session(None, None)
        await owner.receipt.finish_transport(None)

    task = asyncio.create_task(lifecycle())
    await task


@pytest.mark.asyncio
async def test_real_process_context_closes_exact_root(tmp_path, monkeypatch) -> None:
    async with _managed_owned_process(tmp_path, monkeypatch) as owner:
        parent = owner.parent

        assert owner.receipt.process_exited is True
        assert owner.receipt.reader_eof is True
        assert owner.receipt.reader_joined is True
        assert owner.receipt.task.done() is True
        assert owner.finish() is True
        assert parent.state == "terminal"
        assert owner.receipt.readback() is not None


@pytest.mark.asyncio
async def test_session_failure_keeps_exact_parent_unknown() -> None:
    parent = _parent()
    owner = CuaStdioOwner(
        parent, "cua_stdio", "cua-driver:test", "session-2", "standard",
    )
    parent._cua_receipt = owner.receipt

    await _run_owned_process(owner, fail_session=True)

    assert owner.receipt.state == "unknown"
    assert owner.finish() is False
    assert parent.state == "unknown"
    record = parent.coordinator._records[parent.root_handle.lease.lease_id]
    assert record.handle is parent.root_handle
    assert record.state == "unknown"


@pytest.mark.asyncio
async def test_replaced_receipt_cannot_publish_terminal_readback() -> None:
    parent = _parent()
    owner = CuaStdioOwner(
        parent, "cua_stdio", "cua-driver:test", "session-3", "standard",
    )
    parent._cua_receipt = owner.receipt
    replacement = CuaStdioOwner(
        parent, "cua_stdio", "cua-driver:replacement", "session-3", "standard",
    )
    parent._cua_receipt = replacement.receipt

    await _run_owned_process(owner)

    assert owner.finish() is False
    assert parent.state == "unknown"


# ---------------------------------------------------------------------------
# Public producer proof
# ---------------------------------------------------------------------------
# The three tests above intentionally remain small compatibility controls for
# the typed receipt.  They do not claim that computer_use reaches that receipt.
# This bank is opt-in because it starts the pinned native CUA driver and the
# host's real Bot Desktop.  Root's sealed runner supplies the driver through
# AGC_CUA_PUBLIC_DRIVER; an arbitrary HERMES_CUA_DRIVER_CMD from a developer
# shell is never enough to opt this proof in.


@pytest.fixture
def _public_cua_surface(monkeypatch):
    driver = os.environ.get("AGC_CUA_PUBLIC_DRIVER", "").strip()
    if not driver:
        pytest.skip("Root sealed CUA public-driver surface is not configured")

    monkeypatch.setenv("HERMES_CUA_DRIVER_CMD", driver)
    from plugins.computer_use import get_active_provider
    from tools.computer_use.cua_backend_driver import cua_driver_runtime_contract_status

    provider = get_active_provider()
    assert getattr(provider, "name", None) == "cua", (
        "the public custody bank must select the built-in CuaDriverProvider"
    )
    contract = cua_driver_runtime_contract_status(driver)
    assert contract.get("ready") is True, contract
    return driver


def _public_backend(session_id: str):
    from tools.computer_use import tool
    from tools.computer_use.cua_backend import CuaDriverBackend

    sid = tool._scoped_sid(session_id)
    backend = tool._backends.get(sid)
    assert isinstance(backend, CuaDriverBackend), (
        "handle_computer_use did not install the built-in CuaDriverBackend"
    )
    owner = getattr(backend, "_cua_owner", None)
    assert owner is not None, "the public backend was not bound to a CUA source owner"
    receipt = owner.receipt
    assert getattr(owner.parent, "_cua_receipt", None) is receipt
    return tool, sid, backend, owner, receipt


def _public_list_apps(session_id: str) -> dict:
    from tools.computer_use import tool

    response = json.loads(
        tool.handle_computer_use({"action": "list_apps"}, session_id=session_id)
    )
    assert "error" not in response, response
    assert isinstance(response.get("apps"), list)
    assert response.get("count") == len(response["apps"])
    return response


def test_public_handle_computer_use_owns_real_sdk_child_and_closes_exact_receipt(
    _public_cua_surface,
) -> None:
    """The public tool must bind the real MCP SDK child before publishing its result."""
    from tools.computer_use import tool

    session_id = f"public-cua-{uuid.uuid4().hex}"
    try:
        _public_list_apps(session_id)
        _tool, sid, backend, owner, receipt = _public_backend(session_id)

        # These are observations of the child that the canonical stdio SDK
        # created.  No test object is inserted into the receipt.
        assert receipt.transport_entered is True
        assert receipt.session_entered is True
        assert receipt.process is not None
        assert type(receipt.pid) is int and receipt.pid > 0
        assert receipt.streams is not None and len(receipt.streams) == 2
        assert receipt.session is not None
        assert receipt.session.__class__.__name__ == "ClientSession"
        assert isinstance(receipt.task, asyncio.Task)
        assert receipt.task.done() is False
        assert receipt.process_exited is False
        assert receipt.reader_joined is False
        assert sid in tool._backends and tool._backends[sid] is backend

        assert tool.release_computer_use_session(session_id) is True
        assert receipt.state == "closed"
        assert receipt.session_closed is True
        assert receipt.process_exited is True
        assert receipt.reader_eof is True
        assert receipt.reader_joined is True
        assert receipt.task.done() is True
        readback = receipt.readback()
        assert readback is not None
        assert readback["source_identity"] == owner.source_identity
        assert readback["operation_id"] == owner.parent.operation_id
        assert readback["generation"] == receipt.generation
        assert sid not in tool._backends
    finally:
        tool.release_computer_use_session(session_id)


def test_public_foreign_session_release_cannot_retire_real_owner(_public_cua_surface) -> None:
    """A release for another public session cannot close the first session's source owner."""
    from tools.computer_use import tool

    session_id = f"public-cua-{uuid.uuid4().hex}"
    missing_session_id = f"public-cua-missing-{uuid.uuid4().hex}"
    foreign_session_id = f"public-cua-foreign-{uuid.uuid4().hex}"
    try:
        _public_list_apps(session_id)
        _tool, sid, backend, _owner, receipt = _public_backend(session_id)
        assert tool.release_computer_use_session(missing_session_id) is False
        assert tool.release_computer_use_session(foreign_session_id) is False
        assert sid in tool._backends and tool._backends[sid] is backend
        assert receipt.state == "running"
        assert receipt.process_exited is False
        assert tool.release_computer_use_session(session_id) is True
        assert receipt.state == "closed"
    finally:
        tool.release_computer_use_session(session_id)
        tool.release_computer_use_session(missing_session_id)
        tool.release_computer_use_session(foreign_session_id)


def test_public_unknown_lifecycle_retains_same_handle_and_refuses_new_root(
    _public_cua_surface,
) -> None:
    """A real lifecycle-task failure keeps the old owner and prevents silent replacement."""
    from tools.computer_use import tool

    session_id = f"public-cua-unknown-{uuid.uuid4().hex}"
    try:
        _public_list_apps(session_id)
        _tool, sid, backend, owner, receipt = _public_backend(session_id)
        original_operation_id = owner.parent.operation_id
        lifecycle_future = getattr(backend._session, "_lifecycle_future", None)
        assert lifecycle_future is not None

        # Cancel the actual lifecycle task that owns stdio_client and
        # ClientSession.  This is a physical finalizer fault, not a receipt
        # state injection; the production observer records UNKNOWN during its
        # real same-task context exit.
        assert lifecycle_future.cancel() is True
        assert tool.release_computer_use_session(session_id) is False
        assert sid in tool._backends and tool._backends[sid] is backend
        assert backend._cua_owner is owner
        assert owner.receipt is receipt
        assert owner.parent.operation_id == original_operation_id
        assert receipt.state == "unknown"
        assert owner.parent.state == "unknown"

        retry = json.loads(
            tool.handle_computer_use({"action": "list_apps"}, session_id=session_id)
        )
        assert "error" in retry
        assert sid in tool._backends and tool._backends[sid] is backend
        assert owner.parent.operation_id == original_operation_id
        assert receipt is owner.receipt
    finally:
        tool.release_computer_use_session(session_id)
