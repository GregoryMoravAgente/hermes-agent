"""Public MCP HTTP/SSE consumers of the profile-owned I16 remote receipt."""

from __future__ import annotations

import asyncio
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Empty, Queue
import threading
from urllib.parse import parse_qs, urlsplit

import pytest

from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding


class _LoopbackMCP:
    """A real loopback JSON-RPC endpoint consumed by the installed MCP SDK."""

    def __init__(
        self,
        *,
        sse: bool = False,
        close_sse_after_endpoint: bool = False,
        streamable_status: int | None = None,
    ):
        self.sse = sse
        self.close_sse_after_endpoint = close_sse_after_endpoint
        self.streamable_status = streamable_status
        self.stop = threading.Event()
        self._messages: dict[str, Queue] = {}
        self.sse_gets = 0
        owner = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_args):
                return

            def _json(
                self, status: int, payload: dict, *, session: str = "loopback-session",
                allow: str | None = None,
            ):
                body = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Mcp-Session-Id", session)
                if allow is not None:
                    self.send_header("Allow", allow)
                self.end_headers()
                if body:
                    self.wfile.write(body)
                    self.wfile.flush()

            def _answer(self, request: dict) -> dict:
                method = request.get("method")
                params = request.get("params") or {}
                if method == "initialize":
                    return {
                        "protocolVersion": params.get("protocolVersion", "2025-03-26"),
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "loopback-mcp", "version": "1"},
                    }
                if method == "tools/list":
                    return {"tools": [{
                        "name": "loopback_probe",
                        "description": "real loopback probe",
                        "inputSchema": {"type": "object", "properties": {}},
                    }]}
                return {}

            def _read_json(self) -> dict:
                length = int(self.headers.get("Content-Length", "0"))
                return json.loads(self.rfile.read(length) or b"{}")

            def do_HEAD(self):
                if not owner.sse or urlsplit(self.path).path != "/sse":
                    self._json(404, {"error": "unknown path"})
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_POST(self):
                request = self._read_json()
                path = urlsplit(self.path).path
                if owner.sse and path == "/messages":
                    query = parse_qs(urlsplit(self.path).query)
                    session = (query.get("session_id") or [""])[0]
                    queue = owner._messages.get(session)
                    if queue is None:
                        self._json(404, {"error": "unknown session"})
                        return
                    if "id" in request:
                        queue.put({"jsonrpc": "2.0", "id": request["id"], "result": self._answer(request)})
                    self.send_response(202)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                if owner.sse and path == "/sse":
                    status = owner.streamable_status or 405
                    self._json(
                        status,
                        {"error": "use GET" if status == 405 else f"rejection-{status}"},
                        allow="GET" if status == 405 else None,
                    )
                    return
                if not owner.sse and path != "/mcp":
                    self._json(404, {"error": "unknown path"})
                    return
                if "id" in request:
                    self._json(200, {"jsonrpc": "2.0", "id": request["id"], "result": self._answer(request)})
                else:
                    self.send_response(202)
                    self.send_header("Content-Length", "0")
                    self.end_headers()

            def do_GET(self):
                path = urlsplit(self.path).path
                if not owner.sse and path == "/mcp":
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Connection", "keep-alive")
                    self.send_header("Mcp-Session-Id", "loopback-session")
                    self.end_headers()
                    try:
                        while not owner.stop.wait(0.2):
                            pass
                    except (BrokenPipeError, ConnectionResetError, OSError):
                        pass
                    return
                if not owner.sse or path != "/sse":
                    self._json(404, {"error": "unknown path"})
                    return
                owner.sse_gets += 1
                session = f"sse-{id(self)}"
                queue: Queue = Queue()
                owner._messages[session] = queue
                endpoint = (
                    f"http://127.0.0.1:{owner.httpd.server_port}/messages?session_id={session}"
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.end_headers()
                self.wfile.write(f"event: endpoint\ndata: {endpoint}\n\n".encode())
                self.wfile.flush()
                if owner.close_sse_after_endpoint:
                    self.close_connection = True
                    return
                try:
                    while not owner.stop.is_set():
                        try:
                            payload = queue.get(timeout=0.2)
                        except Empty:
                            continue
                        encoded = json.dumps(payload)
                        self.wfile.write(f"event: message\ndata: {encoded}\n\n".encode())
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                finally:
                    owner._messages.pop(session, None)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, name="i16-loopback-mcp", daemon=True)

    def start(self):
        self.thread.start()
        return f"http://127.0.0.1:{self.httpd.server_port}{'/sse' if self.sse else '/mcp'}"

    def close(self):
        self.stop.set()
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)


def _capture_server(monkeypatch):
    from tools import mcp_tool_discovery

    captured = []
    original = mcp_tool_discovery._adopt_server

    def observe(name, server):
        captured.append(server)
        return original(name, server)

    monkeypatch.setattr(mcp_tool_discovery, "_adopt_server", observe)
    return captured


@pytest.mark.asyncio
async def test_public_mcp_http_binds_real_remote_receipt_and_shutdown(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    pytest.importorskip("mcp.client.streamable_http")
    from gateway.runtime_sidecar_remote import RemoteSidecarOwner
    from tools import mcp_tool_discovery, mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP()
    captured = _capture_server(monkeypatch)
    url = loopback.start()
    try:
        names = await asyncio.wait_for(asyncio.to_thread(
            mcp_tool_discovery.register_mcp_servers,
            {"i16-http": {"url": url, "connect_timeout": 5}},
        ), timeout=20)
        assert "mcp__i16_http__loopback_probe" in names
        assert len(captured) == 1
        server = captured[0]
        owner = server._i16_source_owner
        assert isinstance(owner, RemoteSidecarOwner)
        assert owner.receipt is not None
        assert owner.receipt.accepted is True
        assert owner.receipt.session is not None
        assert owner.receipt.task is server._task
        assert owner.receipt.resolved_identity == server._resolved_identity
        session = owner.receipt.session
        transport = owner.receipt.transport
        assert owner.parent.profile_authority is fixture.authority
        assert owner.parent.profile_id == fixture.authority.profile_id
        assert owner.parent.authority_epoch == fixture.authority.epoch
        assert owner.parent.physical_owner_epoch
        parent = owner.parent

        mcp_tool_lifecycle.shutdown_mcp_servers(names={"i16-http"}, timeout=15)
        assert owner.receipt.state == "closed"
        assert owner.receipt.transport is None
        assert owner.receipt.session is None
        assert owner.receipt.source_object is session or owner.receipt.source_object is transport
        assert owner.receipt.readback() is not None
        assert parent.state == "terminal"
        assert parent not in fixture.authority.external_process_finalizers()
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"i16-http"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_public_mcp_sse_uses_same_profile_factory_and_real_session(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    pytest.importorskip("mcp.client.sse")
    from gateway.runtime_sidecar_remote import RemoteSidecarOwner
    from tools import mcp_tool_discovery, mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP(sse=True)
    captured = _capture_server(monkeypatch)
    url = loopback.start()
    try:
        names = await asyncio.wait_for(asyncio.to_thread(
            mcp_tool_discovery.register_mcp_servers,
            {"i16-sse": {"url": url, "transport": "sse", "connect_timeout": 5}},
        ), timeout=20)
        assert "mcp__i16_sse__loopback_probe" in names
        assert len(captured) == 1
        owner = captured[0]._i16_source_owner
        assert isinstance(owner, RemoteSidecarOwner)
        assert owner.receipt.accepted is True
        assert owner.receipt.session is not None
        assert owner.receipt.task is captured[0]._task
        assert owner.receipt.resolved_identity == captured[0]._resolved_identity
        parent = owner.parent

        mcp_tool_lifecycle.shutdown_mcp_servers(names={"i16-sse"}, timeout=15)
        assert owner.receipt.readback() is not None
        assert parent.state == "terminal"
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"i16-sse"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_streamable_rejection_falls_back_to_sse_on_same_remote_parent(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    pytest.importorskip("mcp.client.streamable_http")
    pytest.importorskip("mcp.client.sse")
    from gateway.runtime_sidecar_remote import RemoteSidecarOwner
    from tools import mcp_tool_discovery, mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP(sse=True)
    captured = _capture_server(monkeypatch)
    url = loopback.start()
    try:
        names = await asyncio.wait_for(asyncio.to_thread(
            mcp_tool_discovery.register_mcp_servers,
            {"i16-fallback": {"url": url, "connect_timeout": 5}},
        ), timeout=20)
        assert "mcp__i16_fallback__loopback_probe" in names
        owner = captured[0]._i16_source_owner
        assert isinstance(owner, RemoteSidecarOwner)
        assert owner.receipt.accepted is True
        parent = owner.parent
        # The first rejected context exited before the SSE context was
        # admitted; both belong to this one object-bound parent.
        assert owner.receipt._transport_closes >= 1
        assert parent.profile_authority is fixture.authority

        mcp_tool_lifecycle.shutdown_mcp_servers(names={"i16-fallback"}, timeout=15)
        assert owner.receipt.readback() is not None
        assert parent.state == "terminal"
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"i16-fallback"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_streamable_rejection_with_real_sse_eof_retains_same_remote_root(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """A real fallback stream EOF leaves its reserved parent UNKNOWN for retry/readback."""
    pytest.importorskip("mcp.client.streamable_http")
    pytest.importorskip("mcp.client.sse")
    from tools import mcp_tool_discovery, mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP(sse=True, close_sse_after_endpoint=True)
    url = loopback.start()
    try:
        names = await asyncio.wait_for(asyncio.to_thread(
            mcp_tool_discovery.register_mcp_servers,
            {"i16-fallback-eof": {"url": url, "connect_timeout": 5}},
        ), timeout=20)
        assert names == []
        parents = tuple(
            parent for parent in fixture.authority.external_process_finalizers()
            if parent.operation_id.startswith("mcp-remote:i16-fallback-eof:")
        )
        assert len(parents) == 1
        parent = parents[0]
        receipt = parent._remote_receipt
        assert receipt is not None
        assert receipt.state == "unknown"
        assert parent.state == "unknown"
        record = fixture.authority._preservation_coordinator()._records[
            parent.root_handle.lease.lease_id
        ]
        assert record.handle is parent.root_handle
        assert record.state == "unknown"
        assert receipt.readback() is None
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"i16-fallback-eof"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_public_reregister_refuses_retained_unknown_remote_root(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """A public re-registration cannot mint a second root while the first route is UNKNOWN."""
    pytest.importorskip("mcp.client.streamable_http")
    pytest.importorskip("mcp.client.sse")
    from tools import mcp_tool_discovery, mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP(sse=True, close_sse_after_endpoint=True)
    url = loopback.start()
    config = {"i16-reregister": {"url": url, "connect_timeout": 5}}
    try:
        names = await asyncio.wait_for(asyncio.to_thread(
            mcp_tool_discovery.register_mcp_servers, config,
        ), timeout=20)
        assert names == []
        parents = tuple(
            parent for parent in fixture.authority.external_process_finalizers()
            if parent.operation_id.startswith("mcp-remote:i16-reregister:")
        )
        assert len(parents) == 1
        parent = parents[0]
        receipt = parent._remote_receipt
        assert receipt is not None
        assert receipt.state == "unknown"
        coordinator = fixture.authority._preservation_coordinator()
        record = coordinator._records[parent.root_handle.lease.lease_id]
        assert record.handle is parent.root_handle
        assert record.state == "unknown"

        mcp_tool_lifecycle.shutdown_mcp_servers(names={"i16-reregister"}, timeout=15)
        names = await asyncio.wait_for(asyncio.to_thread(
            mcp_tool_discovery.register_mcp_servers, config,
        ), timeout=20)
        assert names == []
        retained = tuple(
            candidate for candidate in fixture.authority.external_process_finalizers()
            if candidate.operation_id.startswith("mcp-remote:i16-reregister:")
        )
        assert retained == (parent,)
        assert parent._remote_receipt is receipt
        assert receipt.state == "unknown"
        assert coordinator._records[parent.root_handle.lease.lease_id] is record
        assert record.handle is parent.root_handle
        assert record.state == "unknown"
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"i16-reregister"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_remote_factory_closes_ingress_and_retains_configured_unaccepted_route(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """A close generation never turns an empty live map into authenticated no-route."""
    from gateway.runtime_sidecar_remote import RemoteSourceError
    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    try:
        factory = fixture.authority._mcp_source_factory
        factory.observe_config({"configured": {"url": "http://127.0.0.1:1/mcp"}})
        receipt = factory.close_ingress(0)
        assert receipt.outcome == "unknown"
        assert receipt.candidate_count == 1
        with pytest.raises(RemoteSourceError) as excinfo:
            factory.reserve("configured", {"url": "http://127.0.0.1:1/mcp"})
        assert getattr(excinfo.value, "code", None) == "mcp_ingress_closed"
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [401, 500])
async def test_initial_auth_or_server_error_does_not_fallback_to_sse(
    tmp_path, monkeypatch, require_mcp_2_sdk, status,
):
    """Only a recorded initial transport mismatch may reuse the existing remote root."""
    pytest.importorskip("mcp.client.streamable_http")
    pytest.importorskip("mcp.client.sse")
    from tools import mcp_tool_discovery

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP(sse=True, streamable_status=status)
    url = loopback.start()
    try:
        names = await asyncio.wait_for(asyncio.to_thread(
            mcp_tool_discovery.register_mcp_servers,
            {f"i16-reject-{status}": {"url": url, "connect_timeout": 5}},
        ), timeout=20)
        assert names == []
        assert loopback.sse_gets == 0
        parents = tuple(
            parent for parent in fixture.authority.external_process_finalizers()
            if parent.operation_id.startswith(f"mcp-remote:i16-reject-{status}:")
        )
        assert len(parents) == 1
        parent = parents[0]
        receipt = parent._remote_receipt
        assert receipt is not None
        assert receipt.state == "unknown"
        assert parent.state == "unknown"
        assert receipt.readback() is None
    finally:
        from tools import mcp_tool_lifecycle
        mcp_tool_lifecycle.shutdown_mcp_servers(names={f"i16-reject-{status}"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_initial_missing_endpoint_does_not_fallback_to_sse(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """A missing initial endpoint is not an authenticated transport mismatch."""
    pytest.importorskip("mcp.client.streamable_http")
    pytest.importorskip("mcp.client.sse")
    from tools import mcp_tool_discovery

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP(sse=True, streamable_status=404)
    url = loopback.start()
    try:
        names = await asyncio.wait_for(asyncio.to_thread(
            mcp_tool_discovery.register_mcp_servers,
            {"i16-reject-404": {"url": url, "connect_timeout": 5}},
        ), timeout=20)
        assert names == []
        assert loopback.sse_gets == 0
        parents = tuple(
            parent for parent in fixture.authority.external_process_finalizers()
            if parent.operation_id.startswith("mcp-remote:i16-reject-404:")
        )
        assert len(parents) == 1
        parent = parents[0]
        receipt = parent._remote_receipt
        assert receipt is not None
        assert receipt.state == "unknown"
        assert parent.state == "unknown"
        assert receipt.readback() is None
    finally:
        from tools import mcp_tool_lifecycle
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"i16-reject-404"}, timeout=15)
        loopback.close()
        fixture.cleanup()


async def _real_registered_server(
    monkeypatch, server_name, url, config=None,
):
    """Register an actual MCPServerTask and return its source-owned receipt."""
    from tools import mcp_tool_discovery

    captured = _capture_server(monkeypatch)
    rendered = dict(config or {})
    rendered.setdefault("url", url)
    rendered.setdefault("connect_timeout", 5)
    names = await asyncio.wait_for(asyncio.to_thread(
        mcp_tool_discovery.register_mcp_servers,
        {server_name: rendered},
    ), timeout=20)
    assert f"mcp__{server_name.replace('-', '_')}__loopback_probe" in names
    assert len(captured) == 1
    server = captured[0]
    owner = server._i16_source_owner
    assert owner is not None
    assert owner.receipt is not None
    assert owner.receipt.task is server._task
    return server, owner


async def _actual_registered_server_allowing_refusal(
    monkeypatch, server_name, url, config=None,
):
    """Register one real source task while allowing its deliberate fault to refuse tools."""
    from tools import mcp_tool_discovery
    from tools.mcp_tool_common import _core

    # A deliberate source refusal happens before _adopt_server, so observe the
    # actual producer claim that receives the MCPServerTask before start().
    # Preserve the discovery claim's original call exactly once; this records
    # the source-created task without manufacturing an adoption or registry row.
    captured = []
    original_connect = mcp_tool_discovery._connect_server

    async def observe_connect(name, rendered_config):
        original_claim = _core._connect_server_claim.get()
        if original_claim is None:
            return await original_connect(name, rendered_config)

        def observe_claim(server):
            captured.append(server)
            return original_claim(server)

        claim_token = _core._connect_server_claim.set(observe_claim)
        try:
            return await original_connect(name, rendered_config)
        finally:
            _core._connect_server_claim.reset(claim_token)

    monkeypatch.setattr(mcp_tool_discovery, "_connect_server", observe_connect)
    rendered = dict(config or {})
    rendered.setdefault("url", url)
    rendered.setdefault("connect_timeout", 5)
    names = await asyncio.wait_for(asyncio.to_thread(
        mcp_tool_discovery.register_mcp_servers,
        {server_name: rendered},
    ), timeout=20)
    assert len(captured) == 1
    server = captured[0]
    owner = server._i16_source_owner
    assert owner is not None
    assert owner.receipt is not None
    assert owner.receipt.task is server._task
    return names, server, owner


def _capture_source_attempts(monkeypatch):
    """Observe only source-created attempt objects for stale/foreign negatives."""
    from gateway.runtime_sidecar_remote import RemoteSourceReceipt

    attempts = []
    original = RemoteSourceReceipt._source_begin_http_attempt

    def observe(receipt, server, task):
        result = original(receipt, server, task)
        attempts.append(receipt._http_attempt)
        return result

    monkeypatch.setattr(RemoteSourceReceipt, "_source_begin_http_attempt", observe)
    return attempts


@pytest.mark.asyncio
async def test_remote_receipt_rejects_replaced_session_and_preserves_unknown_custody(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """A second source scope cannot replace the session on exact active streams."""
    pytest.importorskip("mcp.client.streamable_http")
    from gateway.runtime_sidecar_remote import RemoteSourceError, RemoteSourceReceipt
    from tools.mcp_tool_common import _core
    from tools import mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP()
    url = loopback.start()
    observed = {}
    original_bind = RemoteSourceReceipt._bind_transport_scope_session

    def observe_bind(receipt, scope):
        result = original_bind(receipt, scope)
        try:
            task = scope.task
            server = receipt._server
            owner = receipt._owner
            transport = receipt.transport
            session = scope.session
            observed.update(
                receipt=receipt,
                owner=owner,
                server=server,
                task=task,
                callback_task=asyncio.current_task(),
                transport=transport,
                session=session,
            )
            assert server is not None
            assert task is server._task
            assert task is asyncio.current_task()
            assert receipt.task is task
            assert transport is not None
            assert owner.receipt.session is session
            replacement = _core.streamable_http_client(url)
            observed["replacement"] = replacement
            with pytest.raises(RemoteSourceError) as excinfo:
                receipt._source_open_transport_scope(
                    server,
                    replacement,
                    session_kwargs={},
                    task=task,
                )
            assert excinfo.value.code == "remote_transport_scope_unavailable"
        except BaseException as callback_error:
            observed["callback_error"] = callback_error
        return result

    monkeypatch.setattr(
        RemoteSourceReceipt,
        "_bind_transport_scope_session",
        observe_bind,
    )
    try:
        _names, server, owner = await _actual_registered_server_allowing_refusal(
            monkeypatch, "replaced", url,
        )
        assert observed.get("callback_error") is None
        assert observed.get("server") is server
        assert observed.get("owner") is owner
        assert observed.get("callback_task") is observed.get("task")
        assert observed.get("transport") is not None
        assert observed.get("session") is not None
        transport = owner.receipt.transport
        session = owner.receipt.session
        assert transport is not None
        assert session is not None
        assert owner.receipt.state == "unknown"
        assert transport is observed["transport"]
        assert session is observed["session"]
        assert owner.receipt.transport is transport
        assert owner.receipt.session is session
        assert owner.receipt.source_object is session or owner.receipt.source_object is transport
        assert owner.receipt.readback() is None
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"replaced"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_remote_receipt_rejects_forged_exit_witness_without_clearing_transport(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """A caller-side close attempt cannot prove the source scope has exited."""
    pytest.importorskip("mcp.client.streamable_http")

    from tools import mcp_tool_lifecycle
    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP()
    url = loopback.start()
    try:
        _server, owner = await _real_registered_server(monkeypatch, "forged", url)
        transport = owner.receipt.transport
        session = owner.receipt.session
        assert transport is not None
        assert session is not None
        assert owner.receipt.close_after_shutdown(task=asyncio.current_task()) is False
        assert owner.receipt.state == "unknown"
        assert owner.receipt.transport is transport
        assert owner.receipt.session is session
        assert owner.receipt.source_object is session or owner.receipt.source_object is transport
        assert owner.receipt.readback() is None
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"forged"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_remote_receipt_rejects_copied_http_rejection_record_from_foreign_task(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """A copied HTTP response from a foreign task cannot authenticate an event."""
    pytest.importorskip("mcp.client.streamable_http")
    from tools import mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP()
    url = loopback.start()
    try:
        _server, owner = await _real_registered_server(monkeypatch, "copied", url)
        attempt = owner.receipt._http_attempt
        assert attempt is not None
        wire_request = attempt.transport_request
        wire_response = attempt.transport_response
        assert wire_request is not None
        assert wire_response is not None
        copied_request = type(wire_request)(wire_request.method, str(wire_request.url))
        copied_response = type(wire_response)(405, request=copied_request)
        async def invoke_from_foreign_task():
            attempt.record_response(copied_response, "copied")

        foreign_task = asyncio.create_task(invoke_from_foreign_task())
        await foreign_task
        assert owner.receipt.state == "unknown"
        assert owner.receipt.unknown_reason == "remote_http_response_foreign"
        assert owner.receipt.state == "unknown"
        assert owner.receipt.source_object is not None
        assert owner.receipt.readback() is None
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"copied"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_remote_receipt_rejects_copied_http_rejection_record(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """The actual source HTTP rejection cannot be replaced with a copied SDK error."""
    pytest.importorskip("mcp.client.streamable_http")
    from gateway.runtime_sidecar_remote import (
        RemoteSourceError,
        RemoteSourceReceipt,
        _RemoteTransportScope,
    )
    from tools.mcp_tool_common import _core
    from tools import mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP(sse=True, streamable_status=405)
    url = loopback.start()
    observed = {}

    original_begin = RemoteSourceReceipt._source_begin_http_attempt

    def observe_begin(receipt, server, task):
        result = original_begin(receipt, server, task)
        observed.update(receipt=receipt, server=server, source_task=task)
        return result

    original_open = RemoteSourceReceipt._source_open_transport_scope

    def observe_open(receipt, server, transport, *, session_kwargs, task):
        scope = original_open(
            receipt,
            server,
            transport,
            session_kwargs=session_kwargs,
            task=task,
        )
        observed["scope"] = scope
        return scope

    original_exit = _RemoteTransportScope.__aexit__

    async def observe_exit(scope, exc_type, exc, traceback):
        observed["scope_exit_task"] = asyncio.current_task()
        observed["scope_exit_error"] = exc
        try:
            return await original_exit(scope, exc_type, exc, traceback)
        finally:
            body_error = scope.body_error
            copied_error = observed.get("copied_error")
            assert body_error is copied_error
            observed["scope_body_error"] = body_error
            observed["body_error_is_copied"] = body_error is copied_error

    original_initialize = _core.ClientSession.initialize

    async def observe_initialize(session, *args, **kwargs):
        observed["initialize_task"] = asyncio.current_task()
        try:
            return await original_initialize(session, *args, **kwargs)
        except BaseException as actual_error:
            observed["sdk_error_type"] = type(actual_error).__name__
            receipt = observed["receipt"]
            event = receipt._http_event
            response = None if event is None else event.response
            assert response is not None
            assert response.status_code == 405
            transport = receipt.transport
            assert transport is not None
            observed["transport"] = transport
            httpx = _core.sdk_httpx()
            copied_request = httpx.Request("POST", str(response.request.url))
            copied_response = httpx.Response(405, request=copied_request)
            copied_error = httpx.HTTPStatusError(
                "copied response",
                request=copied_request,
                response=copied_response,
            )
            observed["copied_error"] = copied_error
            observed["response_status"] = response.status_code
            raise copied_error

    original_witness = RemoteSourceReceipt.http_rejection_witness

    def observe_witness(receipt, exception):
        observed["witness_task"] = asyncio.current_task()
        observed["witness_exception"] = exception
        with pytest.raises(RemoteSourceError) as excinfo:
            original_witness(receipt, exception)
        assert excinfo.value.code == "remote_http_rejection_unproven"
        observed["witness_code"] = excinfo.value.code
        observed["witness_error_is_copied"] = (
            exception is observed.get("copied_error")
        )
        raise excinfo.value

    monkeypatch.setattr(RemoteSourceReceipt, "_source_begin_http_attempt", observe_begin)
    monkeypatch.setattr(RemoteSourceReceipt, "_source_open_transport_scope", observe_open)
    monkeypatch.setattr(_RemoteTransportScope, "__aexit__", observe_exit)
    monkeypatch.setattr(_core.ClientSession, "initialize", observe_initialize)
    monkeypatch.setattr(RemoteSourceReceipt, "http_rejection_witness", observe_witness)
    try:
        _names, server, owner = await _actual_registered_server_allowing_refusal(
            monkeypatch, "copied", url,
        )
        receipt = owner.receipt
        assert observed.get("server") is server
        assert observed.get("receipt") is receipt
        assert observed.get("source_task") is server._task
        assert observed.get("initialize_task") is observed.get("source_task")
        assert observed.get("scope_exit_task") is observed.get("source_task")
        assert observed.get("response_status") == 405
        assert observed.get("scope_exit_error") is observed.get("copied_error")
        assert observed.get("scope_body_error") is observed.get("copied_error")
        assert observed.get("body_error_is_copied") is True
        assert observed.get("witness_task") is observed.get("source_task")
        assert observed.get("witness_exception") is observed.get("copied_error")
        assert observed.get("witness_error_is_copied") is True
        assert observed.get("witness_code") == "remote_http_rejection_unproven"
        assert receipt.state == "unknown"
        assert receipt.source_object is not None
        assert receipt.readback() is None
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"copied"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_remote_receipt_rejects_inherited_context_foreign_task_and_copied_wire_objects(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """Copied request extensions and response markers cannot authenticate an inherited context."""
    pytest.importorskip("mcp.client.streamable_http")
    from gateway.runtime_sidecar_remote import _REMOTE_SCOPE_CONTEXT
    from tools import mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP()
    url = loopback.start()
    try:
        attempts = _capture_source_attempts(monkeypatch)
        _server, owner = await _real_registered_server(monkeypatch, "inherited", url)
        attempt = attempts[-1]
        assert attempt is owner.receipt._http_attempt
        wire_request = attempt.transport_request
        wire_response = attempt.transport_response
        assert wire_request is not None
        assert wire_response is not None
        copied_request = type(wire_request)(
            wire_request.method,
            str(wire_request.url),
            extensions=dict(getattr(wire_request, "extensions", {})),
        )
        copied_response = type(wire_response)(
            405,
            request=copied_request,
            extensions=dict(getattr(wire_response, "extensions", {})),
        )
        token = _REMOTE_SCOPE_CONTEXT.set(attempt.context)
        try:
            async def invoke_from_inherited_context():
                attempt.record_response(copied_response, "copied")

            await asyncio.create_task(invoke_from_inherited_context())
        finally:
            _REMOTE_SCOPE_CONTEXT.reset(token)
        assert owner.receipt.state == "unknown"
        assert owner.receipt.unknown_reason == "remote_http_response_foreign"
        assert owner.receipt.readback() is None
        assert owner.parent.root_handle is not None
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"inherited"}, timeout=15)
        loopback.close()
        fixture.cleanup()

@pytest.mark.asyncio
async def test_remote_receipt_rejects_inherited_context_foreign_task_and_exact_wire_objects(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """Exact source wire objects cannot authenticate an inherited foreign task."""
    pytest.importorskip("mcp.client.streamable_http")
    from gateway.runtime_sidecar_remote import _REMOTE_SCOPE_CONTEXT
    from tools import mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP()
    url = loopback.start()
    try:
        attempts = _capture_source_attempts(monkeypatch)
        _server, owner = await _real_registered_server(monkeypatch, "inherited", url)
        attempt = attempts[-1]
        assert attempt is owner.receipt._http_attempt
        wire_request = attempt.transport_request
        wire_response = attempt.transport_response
        assert wire_request is not None
        assert wire_response is not None
        token = _REMOTE_SCOPE_CONTEXT.set(attempt.context)
        try:
            async def invoke_from_inherited_context():
                attempt.mark_transport_response(wire_response)
                attempt.record_response(wire_response, "exact-wire")

            await asyncio.create_task(invoke_from_inherited_context())
        finally:
            _REMOTE_SCOPE_CONTEXT.reset(token)
        assert owner.receipt.state == "unknown"
        assert owner.receipt.unknown_reason == "remote_http_response_foreign"
        assert owner.receipt.readback() is None
        assert owner.parent.root_handle is not None
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"inherited"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_remote_receipt_rejects_inherited_context_private_source_hook(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """An inherited foreign task cannot invoke the actual source hook with exact wire objects."""
    pytest.importorskip("mcp.client.streamable_http")
    from gateway.runtime_sidecar_remote import _REMOTE_SCOPE_CONTEXT
    from tools import mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP()
    url = loopback.start()
    try:
        attempts = _capture_source_attempts(monkeypatch)
        _server, owner = await _real_registered_server(monkeypatch, "hookforeign", url)
        attempt = attempts[-1]
        wire_response = attempt.transport_response
        wire_request = attempt.transport_request
        assert wire_request is not None
        assert wire_response is not None
        assert getattr(wire_response, "request", None) is wire_request
        source_hook = attempt.source_response_hook()
        token = _REMOTE_SCOPE_CONTEXT.set(attempt.context)
        try:
            async def invoke_from_inherited_context():
                source_hook(wire_response, "exact-source-wire")

            await asyncio.create_task(invoke_from_inherited_context())
        finally:
            _REMOTE_SCOPE_CONTEXT.reset(token)
        assert owner.receipt.state == "unknown"
        assert owner.receipt.unknown_reason == "remote_http_response_foreign"
        assert owner.receipt.readback() is None
        assert owner.parent.root_handle is not None
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"hookforeign"}, timeout=15)
        loopback.close()
        fixture.cleanup()


@pytest.mark.asyncio
async def test_remote_receipt_rejects_stale_event_from_replaced_source_attempt(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """An old fallback attempt cannot publish a second event after same-root replacement."""
    pytest.importorskip("mcp.client.streamable_http")
    pytest.importorskip("mcp.client.sse")
    from gateway.runtime_sidecar_remote import _REMOTE_SCOPE_CONTEXT
    from tools import mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP(sse=True)
    url = loopback.start()
    try:
        attempts = _capture_source_attempts(monkeypatch)
        _server, owner = await _real_registered_server(monkeypatch, "stale", url)
        assert len(attempts) >= 2
        stale_attempt = attempts[0]
        assert owner.receipt._http_attempt is attempts[-1]
        assert stale_attempt is not owner.receipt._http_attempt
        assert stale_attempt.response is not None
        assert owner.receipt.state == "accepted"
        token = _REMOTE_SCOPE_CONTEXT.set(stale_attempt.context)
        try:
            stale_attempt.record_response(stale_attempt.response, "stale")
        finally:
            _REMOTE_SCOPE_CONTEXT.reset(token)
        assert owner.receipt.state == "unknown"
        assert owner.receipt.unknown_reason in {
            "remote_http_response_foreign",
            "remote_http_response_replaced",
        }
        assert owner.receipt.readback() is None
        assert owner.parent.root_handle is not None
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={"stale"}, timeout=15)
        loopback.close()
        fixture.cleanup()





@pytest.mark.asyncio
async def test_remote_receipt_rejects_foreign_scope_task_and_preserves_unknown_custody(
    tmp_path, monkeypatch, require_mcp_2_sdk,
):
    """A foreign caller task cannot enter the actual active source scope."""
    pytest.importorskip("mcp.client.streamable_http")
    from gateway.runtime_sidecar_remote import RemoteSourceError
    from tools.mcp_tool_common import _core
    from tools import mcp_tool_lifecycle

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    loopback = _LoopbackMCP()
    url = loopback.start()
    server_name = "foreign-scope"
    try:
        server, owner = await _real_registered_server(monkeypatch, server_name, url)
        transport = owner.receipt.transport
        session = owner.receipt.session
        assert transport is not None
        assert session is not None
        replacement = _core.streamable_http_client(url)
        with pytest.raises(RemoteSourceError) as excinfo:
            owner._source_open_transport_scope(
                server,
                replacement,
                session_kwargs={},
                task=asyncio.current_task(),
            )
        assert excinfo.value.code == "remote_server_capability_unavailable"
        assert owner.receipt.state == "unknown"
        assert owner.receipt.transport is transport
        assert owner.receipt.session is session
        assert owner.receipt.source_object is session or owner.receipt.source_object is transport
        assert owner.receipt.readback() is None
    finally:
        mcp_tool_lifecycle.shutdown_mcp_servers(names={server_name}, timeout=15)
        loopback.close()
        fixture.cleanup()
