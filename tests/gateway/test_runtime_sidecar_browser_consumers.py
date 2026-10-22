"""Real public browser consumers over the existing I04/I16 custody boundary.

These controls use the sealed agent-browser/Chromium tool store when the guarded
runtime bank runs them.  The gateway admission is real, the page is a loopback
HTTP server owned by the test, and the connected Chrome uses a fresh temporary
profile.  No browser result is accepted from a fake process or a copied custody
object.
"""

import asyncio
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
from types import SimpleNamespace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import urlopen

import pytest

from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding
from tests.waiting import wait_until


class _LoopbackPage(BaseHTTPRequestHandler):
    def do_GET(self):
        body = (
            b"<!doctype html><html><head><title>Hermes browser custody</title></head>"
            b"<body><main><h1>Browser custody page</h1>"
            b"<button id='probe'>Loopback probe</button></main></body></html>"
        )
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


class _CacheLoopbackPage(BaseHTTPRequestHandler):
    request_count = 0

    def do_GET(self):
        type(self).request_count += 1
        body = (
            b"<!doctype html><html><head><title>Hermes Lightpanda cache</title></head>"
            b"<body><main><h1>Lightpanda cache custody</h1>"
            b"<p id='turn'>The managed public route is alive.</p></main></body></html>"
        )
        self.send_response(200)
        self.send_header("Cache-Control", "public, max-age=120")
        self.send_header("ETag", '"hermes-lightpanda-cache-v1"')
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        return


class _PortSquatter(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(404)
        self.end_headers()

    def log_message(self, *_args):
        return


def _start_http(handler):
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, name="browser-custody-loopback", daemon=True)
    thread.start()
    return server, thread


def _wait_for_cdp(port: int, timeout: float = 15.0) -> str:
    url = f"http://127.0.0.1:{port}/json/version"
    def ready():
        try:
            with urlopen(url, timeout=1.0) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if isinstance(payload, dict) and isinstance(payload.get("webSocketDebuggerUrl"), str):
                return True
        except Exception:
            return False
        return False

    wait_until(ready, timeout=timeout, what="test-owned Chromium CDP")
    return url


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _start_test_chromium(tmp_path: Path) -> tuple[subprocess.Popen, str]:
    from hermes_cli.browser_runtime import chromium_executable

    executable = chromium_executable()
    assert executable and Path(executable).exists()
    port = _free_port()
    profile = tmp_path / "fresh-chrome-profile"
    argv = [
        executable,
        "--headless=new",
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--no-first-run",
        "--no-default-browser-check",
        "--disable-extensions",
        "--password-store=basic",
        "--use-mock-keychain",
        "--remote-debugging-address=127.0.0.1",
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile}",
    ]
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        argv.extend(("--no-sandbox", "--disable-dev-shm-usage"))
    argv.append("about:blank")
    process = subprocess.Popen(
        argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        return process, _wait_for_cdp(port)
    except BaseException:
        process.terminate()
        process.wait(timeout=5)
        raise


def _configure_public_lightpanda(tmp_path, monkeypatch):
    """Select the real local Browser Use/Lightpanda route for one owned profile."""
    home = Path(os.environ["HERMES_HOME"])
    (home / "config.yaml").write_text(
        "browser:\n"
        "  backend: browser-use\n"
        "  cloud_provider: local\n"
        "  engine: lightpanda\n"
        "  allow_private_urls: true\n"
        "  use_real_profile: false\n",
        encoding="utf-8",
    )
    monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
    monkeypatch.delenv("BROWSER_CDP_WS", raising=False)
    monkeypatch.delenv("AGENT_BROWSER_ENGINE", raising=False)
    monkeypatch.setenv("AGENT_BROWSER_PROFILE", str(tmp_path / "agent-browser-profile"))


def _assert_frozen_lightpanda_binary():
    """Require Root's sealed official binary instead of resolving ambient PATH state."""
    from tools import browser_lightpanda

    selected = browser_lightpanda.find_lightpanda_binary()
    assert selected, "the guarded runner must select the frozen Lightpanda on PATH"
    path = Path(selected).resolve()
    assert path.is_file() and os.access(path, os.X_OK)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert digest == "d4b66ac8484f49d7a49e05b6f9342716c747129527fcbe8ffe36679ffa18dd60"
    from tools import browser_lightpanda

    resolved = browser_lightpanda.find_lightpanda_binary()
    assert resolved is not None
    assert Path(resolved).resolve() == path
    assert hashlib.sha256(Path(resolved).read_bytes()).hexdigest() == digest
    return path


def _lightpanda_session_state(task_id: str):
    from tools import browser_tool
    from tools.browser_lightpanda import _record_path, get_server

    with browser_tool._cleanup_lock:
        session_info = browser_tool._active_sessions.get(task_id)
    assert isinstance(session_info, dict)
    session_name = session_info.get("session_name")
    assert isinstance(session_name, str) and session_name
    receipt = session_info.get("native_backend_receipt")
    assert receipt is not None
    server = get_server(session_name)
    assert server is not None
    assert server.receipt is receipt
    return session_info, session_name, receipt, server, _record_path(session_name)


def _assert_terminal_browser_admission(fixture, accepted, captured):
    from gateway.session_results import admission_result
    from hermes_state_runtime import get_session_admission

    row = get_session_admission(fixture.authority.db, admission_id=accepted.admission_id)
    assert row is not None
    assert row["status"] == "terminal"
    assert row["admission_id"] == accepted.admission_id
    assert row["target_session_id"] == captured["binding"].session_id
    saved = admission_result(fixture.authority.db, accepted.admission_id)
    assert isinstance(saved, dict)
    stored_result = saved.get("result")
    assert isinstance(stored_result, dict)
    final_response = stored_result.get("final_response")
    assert isinstance(final_response, str)
    expected_title = captured["navigation"]["title"]
    assert isinstance(expected_title, str) and expected_title
    assert expected_title in final_response
    return row, saved


async def _run_public_browser_turn(
    fixture, *, task_id: str, url: str, message_id: str | None = None,
):
    """Component-level custody control; public producer coverage is below."""
    from gateway.platforms.event import MessageEvent
    from gateway.runtime_service_custody import current_turn_binding
    from hermes_state_runtime import get_session_admission
    from tools.browser_tool import browser_navigate, browser_snapshot

    source = fixture.adapter.build_source(
        chat_id=f"{task_id}@s.whatsapp.net", user_id="15551234567@s.whatsapp.net", chat_type="dm",
    )
    message_id = message_id or task_id
    event = MessageEvent(
        text=f"browser-public-turn:{message_id}", source=source, message_id=message_id,
    )
    accepted = await fixture.admit_native_with_source_handoff(event)
    assert accepted.status == "queued"
    fixture.authority.native_waiters.add(accepted.admission_id)
    fixture.authority.waiters[accepted.admission_id] = asyncio.get_running_loop().create_future()
    captured = {}
    original_handler = fixture.runner._handle_message

    async def handle_message(turn_event):
        binding = current_turn_binding()
        assert binding is not None
        captured["binding"] = binding
        navigation = json.loads(browser_navigate(url, task_id=task_id))
        snapshot = json.loads(browser_snapshot(task_id=task_id))
        captured["navigation"] = navigation
        captured["snapshot"] = snapshot
        return json.dumps({"navigation": navigation, "snapshot": snapshot}, sort_keys=True)

    fixture.runner._handle_message = handle_message
    try:
        await fixture.authority._drain(accepted.ref)
    finally:
        fixture.runner._handle_message = original_handler

    row = get_session_admission(fixture.authority.db, admission_id=accepted.admission_id)
    assert row is not None and row["status"] == "terminal"
    return accepted, captured


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass  # the real agent-browser daemon may reparent
async def test_public_browser_navigate_snapshot_and_cdp_connect_use_real_custody(tmp_path, monkeypatch):
    """The public tools cross a real admission, browser connect, and fresh Chrome profile."""
    from tui_gateway import server as tui_server
    from gateway.runtime_sidecar_browser import factory_for_authority
    from tools.browser_tool_lifecycle import cleanup_browser

    page_server, page_thread = _start_http(_LoopbackPage)
    chrome = None
    fixture = None
    task_id = "browser-custody-public"
    monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
    if os.environ.get("HERMES_RUNTIME_DIR"):
        monkeypatch.setenv("HERMES_RUNTIME_DIR", os.environ["HERMES_RUNTIME_DIR"])
    else:
        monkeypatch.delenv("HERMES_RUNTIME_DIR", raising=False)
    monkeypatch.setenv("AGENT_BROWSER_PROFILE", str(tmp_path / "agent-browser-profile"))
    try:
        fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
        (Path(os.environ["HERMES_HOME"]) / "config.yaml").write_text(
            "browser:\n  cloud_provider: local\n  allow_private_urls: true\n",
            encoding="utf-8",
        )
        chrome, cdp_discovery = _start_test_chromium(tmp_path)
        connected = tui_server.handle_request({
            "id": "browser-connect", "method": "browser.manage",
            "params": {"action": "connect", "url": cdp_discovery},
        })
        assert connected["result"]["connected"] is True
        assert os.environ["BROWSER_CDP_URL"].startswith("http://127.0.0.1:")

        accepted, captured = await _run_public_browser_turn(
            fixture, task_id=task_id, url=f"http://127.0.0.1:{page_server.server_port}/page",
        )
        assert captured["navigation"]["success"] is True
        assert captured["navigation"]["title"] == "Hermes browser custody"
        assert captured["snapshot"]["success"] is True
        assert "Browser custody page" in captured["snapshot"]["snapshot"]
        assert factory_for_authority(fixture.authority) is fixture.authority.browser_source_factory()
        assert not any(
            entry.get("admission_id") == accepted.admission_id
            for entry in fixture.authority._preservation_sidecars.values()
        )
    finally:
        monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
        if fixture is not None:
            cleanup_browser(task_id)
            fixture.cleanup()
        if chrome is not None:
            chrome.terminate()
            chrome.wait(timeout=5)
        page_server.shutdown()
        page_server.server_close()
        page_thread.join(timeout=5)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_public_browser_agent_turn_uses_real_provider_tool_calls(tmp_path, monkeypatch):
    """The public adapter/AIAgent path must produce the browser calls and readback."""
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text, ToolCall

    page_server, page_thread = _start_http(_LoopbackPage)
    chrome = None
    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    authority = fixture.authority
    adapter = fixture.adapter
    task_id = "browser-agent-public"
    session_key = None
    sent: list[str] = []

    scheduler = getattr(type(authority), "_schedule", None)
    assert scheduler is not None
    monkeypatch.setattr(authority, "_schedule", scheduler.__get__(authority, type(authority)))
    monkeypatch.setattr("hermes_cli.tools_config._get_platform_tools", lambda *_: {"browser"})
    monkeypatch.setattr("gateway.run._resolve_gateway_model", lambda *_args, **_kwargs: "fixture-model")
    monkeypatch.setattr("gateway.run._load_gateway_config", dict)
    monkeypatch.setattr(
        "gateway.run.GatewayRunner._load_reasoning_config", staticmethod(lambda model="": {}),
    )
    monkeypatch.setattr("gateway.run.GatewayRunner._load_fallback_model", staticmethod(lambda: None))
    monkeypatch.setattr("gateway.run._current_max_iterations", lambda: 6)

    async def _send(*args, content=None, **_kwargs):
        if content is None and len(args) >= 2:
            content = args[1]
        sent.append(str(content))
        return SendResult(success=True, message_id="browser-public-reply")

    monkeypatch.setattr(adapter, "send", _send)
    monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
    if os.environ.get("HERMES_RUNTIME_DIR"):
        monkeypatch.setenv("HERMES_RUNTIME_DIR", os.environ["HERMES_RUNTIME_DIR"])
    else:
        monkeypatch.delenv("HERMES_RUNTIME_DIR", raising=False)

    try:
        chrome, cdp_discovery = _start_test_chromium(tmp_path)
        from tui_gateway import server as tui_server
        connected = tui_server.handle_request({
            "id": "browser-agent-connect", "method": "browser.manage",
            "params": {"action": "connect", "url": cdp_discovery},
        })
        assert connected["result"]["connected"] is True
        url = f"http://127.0.0.1:{page_server.server_port}/page"
        script = [
            ToolCall("browser_navigate", {"url": url}),
            ToolCall("browser_snapshot", {}),
            Text("browser public turn complete"),
        ]
        source = adapter.build_source(
            chat_id=f"{task_id}@s.whatsapp.net", user_id="15551234567@s.whatsapp.net", chat_type="dm",
        )
        event = MessageEvent(text="use the browser", source=source, message_id=task_id)
        session_key = fixture.runner.session_store._generate_session_key(source)
        with FakeLLMServer(script) as llm:
            monkeypatch.setattr(
                "gateway.run._resolve_runtime_agent_kwargs",
                lambda llm=llm: {
                    "provider": "openai", "api_key": "fixture-key", "base_url": llm.base_url,
                    "api_mode": "chat_completions",
                },
            )
            await adapter.handle_message(event)
            task = adapter._session_tasks.get(session_key)
            assert task is not None
            await asyncio.wait_for(task, 45)
            requests = llm.main_requests()
            tool_names = [
                call.get("function", {}).get("name")
                for request in requests
                for message in request.get("messages", [])
                for call in message.get("tool_calls", [])
                if isinstance(call, dict)
            ]
            assert "browser_navigate" in tool_names
            assert "browser_snapshot" in tool_names
            assert any("browser public turn complete" in text for text in sent)
    finally:
        from tools.browser_tool_lifecycle import cleanup_browser
        cleanup_browser(task_id)
        fixture.cleanup()
        if chrome is not None:
            chrome.terminate()
            chrome.wait(timeout=5)
        page_server.shutdown()
        page_server.server_close()
        page_thread.join(timeout=5)


@pytest.mark.asyncio
async def test_browser_source_refuses_retiring_authority_and_portsquatter(tmp_path, monkeypatch):
    """A stale profile and a non-CDP loopback occupant never become browser custody."""
    from gateway.runtime_sidecar_browser import BrowserSourceError, factory_for_authority

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    try:
        factory = factory_for_authority(fixture.authority)
        assert factory is fixture.authority.browser_source_factory()
        fixture.authority.retiring = True
        with pytest.raises(BrowserSourceError, match="retiring"):
            factory.reserve_command("stale-browser", {"session_name": "stale-session"})
        fixture.authority.retiring = False

        squatter, thread = _start_http(_PortSquatter)
        try:
            from tui_gateway import server as tui_server
            monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
            result = tui_server.handle_request({
                "id": "browser-squatter", "method": "browser.manage",
                "params": {"action": "connect", "url": f"http://127.0.0.1:{squatter.server_port}"},
            })
            assert result["error"]["code"] == 5031
            assert "BROWSER_CDP_URL" not in os.environ
        finally:
            squatter.shutdown()
            squatter.server_close()
            thread.join(timeout=5)
    finally:
        fixture.cleanup()


def test_lightpanda_endpoint_readback_requires_exact_owned_listener(monkeypatch):
    """A responsive loopback port is insufficient without the exact child listener."""
    from gateway import runtime_browser_native_backends as native
    from tools.process_registry import ProcessRegistry

    process = SimpleNamespace(pid=4242)
    connection = SimpleNamespace(
        status="LISTEN", laddr=SimpleNamespace(ip="127.0.0.1", port=43111), fd=9,
    )

    class _Psutil:
        CONN_LISTEN = "LISTEN"

        @staticmethod
        def Process(pid):
            assert pid == process.pid
            return SimpleNamespace(net_connections=lambda kind: [connection])

    monkeypatch.setitem(sys.modules, "psutil", _Psutil)
    monkeypatch.setattr(ProcessRegistry, "_safe_host_start_time", staticmethod(lambda pid: 77))
    assert native._owned_listener_identity(process, 43111, 77) == (
        4242, 77, "127.0.0.1", 43111, 9,
    )
    assert native._owned_listener_identity(process, 43112, 77) is None
    assert native._owned_listener_identity(process, 43111, 78) is None


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
@pytest.mark.integration
async def test_public_lightpanda_two_turns_reuse_managed_receipt_and_cache(tmp_path, monkeypatch):
    """The public Browser Use route reuses one authenticated native session across turns."""
    from tools.browser_lightpanda import _record_path
    from tools.browser_tool_lifecycle import cleanup_browser

    page_server, page_thread = _start_http(_CacheLoopbackPage)
    fixture = None
    task_id = "browser-lightpanda-public"
    _CacheLoopbackPage.request_count = 0
    try:
        fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
        _configure_public_lightpanda(tmp_path, monkeypatch)
        _assert_frozen_lightpanda_binary()
        url = f"http://127.0.0.1:{page_server.server_port}/page"

        first, first_captured = await _run_public_browser_turn(
            fixture, task_id=task_id, message_id=f"{task_id}-turn-1", url=url,
        )
        assert first_captured["navigation"]["success"] is True
        assert first_captured["navigation"]["title"] == "Hermes Lightpanda cache"
        assert first_captured["snapshot"]["success"] is True
        assert "Lightpanda cache custody" in first_captured["snapshot"]["snapshot"]
        _assert_terminal_browser_admission(fixture, first, first_captured)
        _session_info, session_name, receipt, server, record_path = _lightpanda_session_state(task_id)
        assert record_path == _record_path(session_name)
        process = receipt.process
        physical = receipt.physical
        endpoint = receipt.endpoint_identity
        assert process is not None
        assert physical is not None
        assert endpoint is not None
        assert receipt.cdp_readback_digest
        publication = physical.external_publication_proof
        assert publication is not None and publication.cache_readback is True
        assert receipt.operation_id in physical.completed_requests
        record = json.loads(record_path.read_text(encoding="utf-8"))
        assert tuple(record["endpoint_identity"]) == tuple(endpoint)
        assert record["pid"] == process.pid

        second, second_captured = await _run_public_browser_turn(
            fixture, task_id=task_id, message_id=f"{task_id}-turn-2", url=url,
        )
        assert second_captured["navigation"]["success"] is True
        assert second_captured["snapshot"]["success"] is True
        _assert_terminal_browser_admission(fixture, second, second_captured)
        _session_info2, session_name2, receipt2, server2, record_path2 = _lightpanda_session_state(task_id)
        assert first.admission_id != second.admission_id
        assert first.ref.session_id == second.ref.session_id
        assert first_captured["binding"].session_id == second_captured["binding"].session_id
        assert session_name2 == session_name
        assert receipt2 is receipt
        assert server2 is server
        assert record_path2 == record_path
        assert receipt2.process is process
        assert receipt2.physical is physical
        assert receipt2.endpoint_identity == endpoint
        assert _CacheLoopbackPage.request_count >= 1
    finally:
        cleanup_browser(task_id)
        if fixture is not None:
            fixture.cleanup()
        page_server.shutdown()
        page_server.server_close()
        page_thread.join(timeout=5)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
@pytest.mark.integration
async def test_public_lightpanda_reader_and_finalizer_failure_retain_same_receipt(
    tmp_path, monkeypatch,
):
    """Reader, finalizer, and record faults retain one exact receipt through retries."""
    from gateway import runtime_service_operations
    from tools.browser_lightpanda import get_server
    from tools.browser_tool_lifecycle import cleanup_browser

    page_server, page_thread = _start_http(_LoopbackPage)
    fixture = None
    receipt = None
    original_close_after_finalizers = runtime_service_operations.close_after_finalizers
    from gateway.runtime_sidecar_browser import BrowserOutputReader

    original_read = BrowserOutputReader._read_after_exit
    reader_release = threading.Event()
    reader_entered = threading.Event()

    def gated_read(reader):
        if reader.path.endswith(".log"):
            reader_entered.set()
            if not reader_release.wait(30):
                raise RuntimeError("test-owned stderr reader gate was not released")
        return original_read(reader)

    monkeypatch.setattr(BrowserOutputReader, "_read_after_exit", gated_read)
    task_id = "browser-lightpanda-finalizer-failure"
    try:
        fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
        _configure_public_lightpanda(tmp_path, monkeypatch)
        _assert_frozen_lightpanda_binary()
        accepted, captured = await _run_public_browser_turn(
            fixture, task_id=task_id, message_id=f"{task_id}-turn-1",
            url=f"http://127.0.0.1:{page_server.server_port}/page",
        )
        _assert_terminal_browser_admission(fixture, accepted, captured)
        _session_info, session_name, receipt, server, record_path = _lightpanda_session_state(task_id)
        process = receipt.process
        physical = receipt.physical
        reader = receipt.stderr_reader
        assert process is not None and physical is not None and reader is not None

        assert reader_entered.wait(5), "real stderr reader did not enter its owned gate"
        cleanup_browser(task_id)
        assert get_server(session_name) is server
        assert receipt.state == "unknown"
        assert receipt.unknown_reason
        assert receipt.unknown_reason in {
            "browser_native_process_close_unknown", "browser_native_reader_join_failed",
        }
        assert receipt.parent.state == "unknown"
        assert receipt.process is process
        assert receipt.physical is physical
        # The first UNKNOWN is allowed to be an in-flight SIGTERM.  Wait on the
        # exact retained Popen, then retry the same receipt so the real gated
        # reader join failure is reached without fabricating native closure.
        if receipt.unknown_reason == "browser_native_process_close_unknown":
            process.wait(timeout=5)
            cleanup_browser(task_id)
        assert receipt.unknown_reason == "browser_native_reader_join_failed"
        assert process.poll() is not None
        assert record_path.exists()

        reader_release.set()
        reader.join(timeout=5)
        assert not reader.is_alive()
        assert reader._error is None
        finalizer_calls = 0

        def refuse_finalizer(parent):
            nonlocal finalizer_calls
            finalizer_calls += 1
            return False

        monkeypatch.setattr(runtime_service_operations, "close_after_finalizers", refuse_finalizer)
        cleanup_browser(task_id)
        assert finalizer_calls == 1
        assert get_server(session_name) is server
        assert receipt.process is process
        assert receipt.physical is physical
        assert receipt.state == "unknown"
        assert receipt.unknown_reason
        assert getattr(receipt, "_readback", None) is not None
        assert receipt.parent.state == "unknown"
        assert process.poll() is not None
        assert record_path.exists()

        monkeypatch.setattr(
            runtime_service_operations, "close_after_finalizers", original_close_after_finalizers,
        )
        cleanup_browser(task_id)
        assert get_server(session_name) is None
        assert receipt.state == "closed"
        assert receipt.readback() is not None
        assert receipt.parent.state == "terminal"
        assert receipt.process is process
        assert not record_path.exists()
    finally:
        reader_release.set()
        monkeypatch.setattr(
            runtime_service_operations, "close_after_finalizers", original_close_after_finalizers,
        )
        cleanup_browser(task_id)
        if fixture is not None:
            fixture.cleanup()
        page_server.shutdown()
        page_server.server_close()
        page_thread.join(timeout=5)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
@pytest.mark.integration
async def test_public_lightpanda_record_unlink_failure_retains_terminal_receipt(tmp_path, monkeypatch):
    """Record removal is retried only after the same native receipt has terminalized."""
    from tools.browser_lightpanda import get_server
    from tools.browser_tool_lifecycle import cleanup_browser

    page_server, page_thread = _start_http(_LoopbackPage)
    fixture = None
    real_unlink = Path.unlink
    task_id = "browser-lightpanda-unlink-failure"
    try:
        fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
        _configure_public_lightpanda(tmp_path, monkeypatch)
        _assert_frozen_lightpanda_binary()
        accepted, captured = await _run_public_browser_turn(
            fixture, task_id=task_id, message_id=f"{task_id}-turn-1",
            url=f"http://127.0.0.1:{page_server.server_port}/page",
        )
        _assert_terminal_browser_admission(fixture, accepted, captured)
        _session_info, session_name, receipt, server, record_path = _lightpanda_session_state(task_id)
        process = receipt.process
        physical = receipt.physical
        assert process is not None and physical is not None
        failed = False

        def fail_record_unlink(path, *args, **kwargs):
            nonlocal failed
            if path == record_path and not failed:
                failed = True
                raise OSError("test-owned record unlink failure")
            return real_unlink(path, *args, **kwargs)

        monkeypatch.setattr(Path, "unlink", fail_record_unlink)
        cleanup_browser(task_id)
        if not failed:
            # A source-owned close can retain UNKNOWN until the exact native
            # process observes exit.  Reap that same Popen, then retry once;
            # only the retry may reach the injected record unlink failure.
            assert get_server(session_name) is server
            assert receipt.state == "unknown"
            assert receipt.unknown_reason == "browser_native_process_close_unknown"
            process.wait(timeout=5)
            cleanup_browser(task_id)
        assert failed is True
        assert get_server(session_name) is server
        assert receipt.state == "closed"
        assert receipt.readback() is not None
        assert receipt.parent.state == "terminal"
        assert receipt.process is process
        assert receipt.physical is physical
        assert record_path.exists()

        monkeypatch.setattr(Path, "unlink", real_unlink)
        cleanup_browser(task_id)
        assert get_server(session_name) is None
        assert receipt.state == "closed"
        assert receipt.readback() is not None
        assert receipt.parent.state == "terminal"
        assert receipt.process is process
        assert not record_path.exists()
    finally:
        monkeypatch.setattr(Path, "unlink", real_unlink)
        cleanup_browser(task_id)
        if fixture is not None:
            fixture.cleanup()
        page_server.shutdown()
        page_server.server_close()
        page_thread.join(timeout=5)


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
@pytest.mark.integration
async def test_public_lightpanda_foreign_loopback_listener_refuses_before_acceptance(tmp_path, monkeypatch):
    """A foreign listener occupying the selected port cannot become a managed endpoint."""
    from gateway.runtime_browser_native_backends import reserve_lightpanda_backend
    from tools import browser_lightpanda
    from tools.browser_lightpanda import _record_path, get_server, launch_lightpanda

    fixture = None
    squatter = None
    thread = None
    task_id = "browser-lightpanda-foreign-listener"
    session_name = "lp-foreign-listener"
    try:
        fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
        _configure_public_lightpanda(tmp_path, monkeypatch)
        _assert_frozen_lightpanda_binary()
        receipt = reserve_lightpanda_backend(task_id, session_name)
        assert receipt is not None
        squatter, thread = _start_http(_PortSquatter)
        monkeypatch.setattr(browser_lightpanda, "_pick_free_loopback_port", lambda: squatter.server_port)
        server, error = launch_lightpanda(session_name, source_receipt=receipt)
        assert server is None
        assert isinstance(error, str) and error
        assert get_server(session_name) is None
        assert not _record_path(session_name).exists()
        assert receipt.state == "unknown"
        assert receipt.unknown_reason
        assert receipt.parent.state == "unknown"
    finally:
        if squatter is not None:
            squatter.shutdown()
            squatter.server_close()
        if thread is not None:
            thread.join(timeout=5)
        if fixture is not None:
            fixture.cleanup()
