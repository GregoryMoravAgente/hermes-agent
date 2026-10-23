"""Real managed CUA admission/delivery acceptance contract.

The fixture drives the public ``handle_computer_use`` entrypoint from a genuine
canonical authority drain. Its driver, process, reader, writer handoff, and
durable result all come from the sealed runtime; this module never constructs a
custody parent or substitutes a result object.
"""
from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
from dataclasses import replace
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import threading
import time

import pytest
from tests.waiting import wait_until


pytestmark = [pytest.mark.integration, pytest.mark.platforms("linux")]

_SESSION = "cua-r7-managed"


def _require_sealed_driver() -> None:
    if not os.environ.get("HERMES_CUA_DRIVER_CMD", "").strip():
        pytest.fail("HERMES_CUA_DRIVER_CMD is required for the real managed CUA acceptance run")


def _find_cua_parent(authority, session_id: str):
    matches = []
    for parent in authority.external_process_finalizers():
        receipt = getattr(parent, "_cua_receipt", None)
        owner = getattr(receipt, "owner", None)
        if getattr(owner, "session_id", None) == session_id:
            matches.append((parent, receipt, owner))
    assert len(matches) == 1, "the public CUA action must leave one exact registered owner"
    return matches[0]


def _image_dimensions(raw: bytes, mime: str) -> tuple[int, int]:
    """Decode dimensions with stdlib readers; do not trust a caller-supplied flag."""
    if mime == "image/png":
        assert raw.startswith(b"\x89PNG\r\n\x1a\n") and len(raw) >= 24
        return struct.unpack(">II", raw[16:24])
    assert mime == "image/jpeg" and raw[:2] == b"\xff\xd8"
    offset = 2
    sof = {0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF}
    while offset < len(raw):
        while offset < len(raw) and raw[offset] == 0xFF:
            offset += 1
        assert offset < len(raw)
        marker = raw[offset]
        offset += 1
        if marker in {0xD8, 0xD9}:
            continue
        assert offset + 2 <= len(raw)
        length = struct.unpack(">H", raw[offset:offset + 2])[0]
        assert length >= 2 and offset + length <= len(raw)
        if marker in sof:
            assert length >= 7
            return struct.unpack(">HH", raw[offset + 3:offset + 7])[::-1]
        offset += length
    raise AssertionError("JPEG has no SOF dimension marker")


def _assert_capture_success(payload: dict, target: dict) -> None:
    """Require the actual public multimodal envelope and source-produced image bytes."""
    assert isinstance(payload, dict), payload
    assert payload.get("_multimodal") is True, payload
    assert "error" not in payload, payload
    content = payload.get("content")
    assert isinstance(content, list)
    image = next(
        (part for part in content
         if isinstance(part, dict) and isinstance(part.get("image_url"), dict)
         and isinstance(part["image_url"].get("url"), str)),
        None,
    )
    assert image is not None, payload
    url = image["image_url"]["url"]
    prefix, encoded = url.split(";base64,", 1)
    mime = prefix.removeprefix("data:")
    assert mime in {"image/png", "image/jpeg"}
    raw = base64.b64decode(encoded, validate=True)
    width, height = _image_dimensions(raw, mime)
    assert width > 0 and height > 0
    meta = payload.get("meta")
    assert isinstance(meta, dict)
    assert meta.get("mode") == "vision"
    assert (meta.get("width"), meta.get("height")) == (width, height)
    summary = payload.get("text_summary")
    assert isinstance(summary, str) and summary.startswith("capture mode=vision")
    if target.get("app_name"):
        assert target["app_name"] in summary
    target["capture_dimensions"] = (width, height)
    target["capture_mime"] = mime


def _decode_public_json(value):
    payload = json.loads(value) if isinstance(value, str) else value
    assert isinstance(payload, dict), payload
    return payload


def _xprop_target(target: dict) -> str:
    result = subprocess.run(
        ["xprop", "-id", str(target["window_id"]), "_NET_WM_PID", "WM_CLASS"],
        env=target["env"], capture_output=True, text=True, timeout=3, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert str(target["pid"]) in result.stdout, result.stdout
    return result.stdout


def _find_owned_window(raw, target: dict) -> dict:
    payload = _decode_public_json(raw)
    windows = payload.get("windows")
    assert isinstance(windows, list), payload
    matches = [
        row for row in windows
        if isinstance(row, dict)
        and row.get("pid") == target["pid"]
        and row.get("title") == target["title"]
    ]
    assert len(matches) == 1, {"target": target["title"], "matches": matches, "windows": windows}
    row = matches[0]
    assert isinstance(row.get("window_id"), (int, str)) and str(row["window_id"])
    assert str(row.get("app_name") or "")
    target["window_id"] = int(row["window_id"])
    target["app_name"] = str(row["app_name"])
    target["xprop"] = _xprop_target(target)
    return row


def _capture_owned_window(raw, target: dict) -> None:
    last = None
    def observe():
        nonlocal last
        try:
            return _find_owned_window(raw(), target)
        except AssertionError as exc:
            last = exc
            return None
    try:
        return wait_until(
            observe, timeout=15.0,
            what="owned terminal window in the public list_windows result",
        )
    except AssertionError as exc:
        raise AssertionError(
            "owned terminal window did not appear in the public list_windows result",
        ) from last or exc


def _assert_same_owned_window(raw, target: dict) -> None:
    before = (target["pid"], target["window_id"], target["app_name"], target["title"])
    row = _find_owned_window(raw(), target)
    assert (target["pid"], target["window_id"], target["app_name"], target["title"]) == before
    assert int(row["window_id"]) == before[1]
    target["xprop_after"] = _xprop_target(target)


@contextlib.contextmanager
def _owned_linux_target(tmp_path: Path):
    """Own one real Xfce terminal target through Bot Desktop's published environment."""
    if not sys.platform.startswith("linux"):
        raise RuntimeError("the managed CUA target is a Linux Bot Desktop control")
    from tools.bot_desktop import runtime

    started_here = False
    if not runtime.is_running():
        runtime.start(wait_seconds=20.0)
        started_here = True
    published = runtime.published_env()
    assert published.get("DISPLAY") and published.get("XAUTHORITY")
    executable = shutil.which("xfce4-terminal") or "/usr/bin/xfce4-terminal"
    if not Path(executable).is_file():
        if started_here:
            runtime.stop()
        pytest.fail("the sealed Linux Bot Desktop must provide xfce4-terminal")
    title = f"AGC-CUA-owned-target-{os.getpid()}-{time.monotonic_ns()}"
    env = {**os.environ, **published}
    process = subprocess.Popen(
        [executable, "--disable-server", "--title", title, "--geometry=70x12+100+100",
         "--command=/bin/sleep 90"],
        env=env, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    target = {"process": process, "pid": process.pid, "title": title, "env": env}
    try:
        yield target
    finally:
        with contextlib.suppress(ProcessLookupError):
            process.terminate()
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=5)
        if process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            process.wait(timeout=5)
        if started_here:
            runtime.stop()


async def _execute_public_turn(api, authority, *, request_id: str, session_id: str,
                               action_session: str, target: dict):
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer
    from functools import wraps
    import gateway.platforms.api_server_run_dispatch as run_dispatch
    from gateway.runtime_service_custody import current_turn_binding
    from gateway.session_results import admission_result
    from hermes_state_runtime import get_session_admission
    from tools.computer_use.tool import handle_computer_use

    observed: list[dict] = []
    claimed_holder = {}
    operation_holder = {}
    original_handler = authority.runner._handle_message

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

    async def actual_handler(event):
        binding = current_turn_binding()
        assert binding is not None, "the public CUA call must run inside the real admitted I04 binding"
        claimed = get_session_admission(authority.db, admission_id=binding.admission_id)
        assert claimed is not None and claimed["status"] == "started"
        assert claimed["generation"] == binding.execution_generation
        live = authority.sessions[binding.session_id]
        drain_task = asyncio.current_task()
        assert drain_task is not None and live.task is drain_task
        claimed_holder["row"] = claimed
        claimed_holder["session_id"] = binding.session_id
        claimed_holder["drain_task"] = drain_task
        target["binding"] = binding

        def list_windows():
            return handle_computer_use({"action": "list_windows"}, session_id=action_session)

        _capture_owned_window(list_windows, target)
        payload = handle_computer_use(
            {"action": "capture", "mode": "vision", "app": target["app_name"],
             "pid": target["pid"], "window_id": target["window_id"]},
            session_id=action_session,
        )
        payload = _decode_public_json(payload) if isinstance(payload, str) else payload
        _assert_capture_success(payload, target)
        observed.append(payload)
        _assert_same_owned_window(list_windows, target)
        return await original_handler(event)

    authority.runner._handle_message = actual_handler
    try:
        fixture_key = "cua-r16-fixture-api-key-0123456789"
        api._api_key = fixture_key
        db = api._ensure_session_db()
        if db.get_session(session_id) is None:
            db.create_session(session_id=session_id, source="api_server", model="hermes-agent")
        app = web.Application()
        app["api_server_adapter"] = api
        app.router.add_post("/v1/runs", api._handle_runs)
        app.router.add_get("/v1/runs/{run_id}", api._handle_get_run)
        async with TestClient(TestServer(app)) as client:
            run_dispatch._launch = observing_launch
            try:
                response = await client.post(
                    "/v1/runs",
                    json={"model": "hermes-agent", "input": "capture the owned test display",
                          "session_id": session_id},
                    headers={"Authorization": f"Bearer {fixture_key}",
                             "Idempotency-Key": request_id},
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
    finally:
        authority.runner._handle_message = original_handler
    assert len(observed) == 1
    claimed = claimed_holder["row"]
    terminal = get_session_admission(authority.db, admission_id=claimed["admission_id"])
    assert terminal is not None and terminal["status"] == "terminal"
    assert terminal["owner_epoch"] == claimed["owner_epoch"]
    assert terminal["generation"] == claimed["generation"]
    saved = admission_result(authority.db, claimed["admission_id"])
    assert isinstance(saved, dict)
    assert isinstance(saved.get("result"), dict)
    return claimed, observed[0], terminal, saved


@pytest.mark.asyncio
async def test_public_cua_managed_admission_records_two_delivery_results_and_reuses_physical(
    runtime_owned_api_owner, tmp_path,
):
    """Two genuine I04 turns use one CUA physical owner and distinct requests."""
    _require_sealed_driver()
    from tools.computer_use.tool import release_computer_use_session

    api, authority = runtime_owned_api_owner
    with _owned_linux_target(tmp_path) as target:
        row1, _payload1, terminal1, saved1 = await _execute_public_turn(
            api, authority, request_id="cua-r7-1", session_id="cua-managed-session",
            action_session=_SESSION, target=target,
        )
        parent, receipt, cua_owner = _find_cua_parent(authority, _SESSION)
        assert parent.mode == "managed_turn"
        assert parent._service is not None
        assert parent._service.external_parent is parent
        assert parent._service.physical is receipt.physical
        assert receipt.physical is not None
        assert receipt.physical.service is parent._service
        root = parent.root_handle
        physical = receipt.physical
        first_request_ids = set(physical.completed_requests)
        assert first_request_ids, "the first real writer/delivery must clear one physical request"
        assert terminal1["admission_id"] == row1["admission_id"]
        assert isinstance(saved1["result"].get("final_response"), str)

        row2, _payload2, terminal2, saved2 = await _execute_public_turn(
            api, authority, request_id="cua-r7-2", session_id="cua-managed-session",
            action_session=_SESSION, target=target,
        )
        parent2, receipt2, cua_owner2 = _find_cua_parent(authority, _SESSION)
        assert parent2 is parent
        assert receipt2 is receipt
        assert cua_owner2 is cua_owner
        assert parent2.root_handle is root
        assert receipt2.physical is physical
        assert parent2._service.physical is physical
        second_request_ids = set(physical.completed_requests)
        assert second_request_ids > first_request_ids
        assert row1["admission_id"] != row2["admission_id"]
        assert terminal2["admission_id"] == row2["admission_id"]
        assert isinstance(saved2["result"].get("final_response"), str)

        # Keep the stale-binding assertion in the original managed acceptance
        # function. It is a real source rejection on this exact owner; the expected
        # UNKNOWN retention is asserted before cleanup, with no replacement root.
        binding = parent.turn_binding
        assert binding is not None
        stale = replace(binding, authority_epoch=binding.authority_epoch + 1)
        assert cua_owner.prepare_current_binding(stale) is False
        assert parent.root_handle is root
        assert getattr(parent, "_cua_receipt", None) is receipt
        assert parent.state == "unknown"
        assert receipt.physical is physical
        assert release_computer_use_session(_SESSION) is False


@pytest.mark.asyncio
async def test_public_cua_transport_failure_retains_physical_and_recovers_same_handle(
    runtime_owned_api_owner, tmp_path,
):
    """An actual reader/lifecycle failure retains one owner and recovers its same handle."""
    _require_sealed_driver()
    from tools.computer_use import tool
    from tools.computer_use.tool import release_computer_use_session

    api, authority = runtime_owned_api_owner
    with _owned_linux_target(tmp_path) as target:
        row1, _payload1, terminal1, _saved1 = await _execute_public_turn(
            api, authority, request_id="cua-r7-transport-1", session_id="cua-transport-session",
            action_session=_SESSION + "-transport", target=target,
        )
        parent, receipt, owner = _find_cua_parent(authority, _SESSION + "-transport")
        root = parent.root_handle
        physical = receipt.physical
        assert physical is not None
        assert receipt.task is not None and receipt.reader_task is receipt.task
        assert receipt.process is not None and receipt.streams is not None
        backend = tool._get_backend(_SESSION + "-transport")
        driver_session = getattr(backend, "_session", None)
        lifecycle = getattr(driver_session, "_lifecycle_future", None)
        assert lifecycle is not None and lifecycle.cancel() is True
        assert release_computer_use_session(_SESSION + "-transport") is False
        assert parent.root_handle is root
        assert receipt.physical is physical
        assert getattr(parent, "_cua_receipt", None) is receipt
        assert receipt.state == "unknown"
        assert receipt.unknown_reason
        assert parent.state == "unknown"
        assert row1["admission_id"] == terminal1["admission_id"]
        first_wait_task = receipt.process_wait_task
        assert first_wait_task is not None

        row2, _payload2, terminal2, _saved2 = await _execute_public_turn(
            api, authority, request_id="cua-r7-transport-2", session_id="cua-transport-session",
            action_session=_SESSION + "-transport", target=target,
        )
        parent2, receipt2, owner2 = _find_cua_parent(authority, _SESSION + "-transport")
        assert parent2 is parent
        assert receipt2 is receipt
        assert owner2 is owner
        assert receipt2.physical is physical
        assert row2["admission_id"] != row1["admission_id"]
        assert terminal2["admission_id"] == row2["admission_id"]
        assert receipt.process_wait_task is not first_wait_task
        assert release_computer_use_session(_SESSION + "-transport") is True


@pytest.mark.asyncio
async def test_public_cua_transport_wait_timeout_retains_same_task_until_terminal_retry(
    runtime_owned_api_owner, monkeypatch, tmp_path,
):
    """A timed-out wait stays UNKNOWN until that exact task finishes, then retries one owner."""
    _require_sealed_driver()
    from tools.computer_use import tool
    from tools.computer_use.tool import release_computer_use_session

    api, authority = runtime_owned_api_owner
    release_wait = threading.Event()
    wait_started = threading.Event()
    session_id = _SESSION + "-wait"
    with _owned_linux_target(tmp_path) as target:
        row1, _payload1, terminal1, _saved1 = await _execute_public_turn(
            api, authority, request_id="cua-r8-wait-1", session_id="cua-wait-session",
            action_session=session_id, target=target,
        )
        parent, receipt, owner = _find_cua_parent(authority, session_id)
        root = parent.root_handle
        assert receipt.process is not None
        process_type = type(receipt.process)
        original_wait = process_type.wait

        async def gated_wait(process):
            wait_started.set()
            await asyncio.to_thread(release_wait.wait)
            return await original_wait(process)

        monkeypatch.setattr(process_type, "wait", gated_wait)
        try:
            backend = tool._get_backend(session_id)
            driver_session = getattr(backend, "_session", None)
            lifecycle = getattr(driver_session, "_lifecycle_future", None)
            assert lifecycle is not None
            with driver_session._lock:
                driver_session._signal_shutdown_locked()
            assert wait_until(wait_started.is_set, timeout=15.0, what="CUA process wait task start")
            first_wait_task = receipt.process_wait_task
            assert first_wait_task is not None
            assert wait_until(
                lambda: receipt.state == "unknown" and receipt.close_error is not None,
                timeout=15.0, what="retained CUA UNKNOWN after wait timeout",
            )
            assert not first_wait_task.done()
            assert receipt.process_wait_done is False
            assert owner.prepare_transport_retry() is False
            assert parent.root_handle is root
            assert row1["admission_id"] == terminal1["admission_id"]

            release_wait.set()
            assert wait_until(
                lambda: first_wait_task.done() and receipt.process_wait_done,
                timeout=15.0, what="retained CUA wait task terminal readback",
            )
            row2, _payload2, terminal2, _saved2 = await _execute_public_turn(
                api, authority, request_id="cua-r8-wait-2", session_id="cua-wait-session",
                action_session=session_id, target=target,
            )
            parent2, receipt2, owner2 = _find_cua_parent(authority, session_id)
            assert parent2 is parent
            assert receipt2 is receipt
            assert owner2 is owner
            assert row2["admission_id"] != row1["admission_id"]
            assert terminal2["admission_id"] == row2["admission_id"]
            assert receipt.process_wait_task is not first_wait_task
            assert release_computer_use_session(session_id) is True
        finally:
            release_wait.set()


@pytest.mark.asyncio
async def test_public_cua_transport_cancellation_retains_same_task_until_terminal_retry(
    runtime_owned_api_owner, monkeypatch, tmp_path,
):
    """Cancellation during finish_transport keeps UNKNOWN until the real wait task ends."""
    _require_sealed_driver()
    from tools.computer_use import tool
    from tools.computer_use.tool import release_computer_use_session

    api, authority = runtime_owned_api_owner
    release_wait = threading.Event()
    wait_started = threading.Event()
    session_id = _SESSION + "-cancel"
    with _owned_linux_target(tmp_path) as target:
        row1, _payload1, terminal1, _saved1 = await _execute_public_turn(
            api, authority, request_id="cua-r9b-cancel-1", session_id="cua-cancel-session",
            action_session=session_id, target=target,
        )
        parent, receipt, owner = _find_cua_parent(authority, session_id)
        root = parent.root_handle
        physical = receipt.physical
        assert physical is not None
        process_type = type(receipt.process)
        original_wait = process_type.wait

        async def gated_wait(process):
            wait_started.set()
            await asyncio.to_thread(release_wait.wait)
            return await original_wait(process)

        monkeypatch.setattr(process_type, "wait", gated_wait)
        try:
            backend = tool._get_backend(session_id)
            driver_session = getattr(backend, "_session", None)
            lifecycle = getattr(driver_session, "_lifecycle_future", None)
            assert lifecycle is not None
            with driver_session._lock:
                driver_session._signal_shutdown_locked()
            assert wait_until(wait_started.is_set, timeout=15.0, what="CUA cancellation wait task start")
            first_wait_task = receipt.process_wait_task
            assert first_wait_task is not None
            assert lifecycle.cancel() is True
            assert wait_until(
                lambda: receipt.state == "unknown" and receipt.close_error is not None,
                timeout=15.0, what="retained CUA UNKNOWN after cancellation",
            )
            assert not first_wait_task.done()
            assert receipt.process_wait_done is False
            assert owner.prepare_transport_retry() is False
            assert parent.root_handle is root
            assert receipt.physical is physical
            assert row1["admission_id"] == terminal1["admission_id"]

            release_wait.set()
            assert wait_until(
                lambda: first_wait_task.done() and receipt.process_wait_done,
                timeout=15.0, what="retained CUA cancelled wait task terminal readback",
            )
            row2, _payload2, terminal2, _saved2 = await _execute_public_turn(
                api, authority, request_id="cua-r9b-cancel-2", session_id="cua-cancel-session",
                action_session=session_id, target=target,
            )
            parent2, receipt2, owner2 = _find_cua_parent(authority, session_id)
            assert parent2 is parent
            assert receipt2 is receipt
            assert owner2 is owner
            assert receipt2.physical is physical
            assert row2["admission_id"] != row1["admission_id"]
            assert terminal2["admission_id"] == row2["admission_id"]
            assert receipt.process_wait_task is not first_wait_task
            assert release_computer_use_session(session_id) is True
        finally:
            release_wait.set()


@pytest.mark.asyncio
@pytest.mark.live_system_guard_bypass
async def test_public_cua_agent_turn_uses_real_provider_tool_calls(
    runtime_owned_api_owner, monkeypatch, tmp_path,
):
    """The public adapter/AIAgent loop emits CUA calls and receives the real image result."""
    from gateway.platforms.base import SendResult
    from gateway.platforms.event import MessageEvent
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text, ToolCall

    _require_sealed_driver()
    api, _authority = runtime_owned_api_owner
    runner = api.gateway_runner
    task_id = "cua-r7-agent-public"
    sent: list[str] = []
    target_holder: dict = {}

    monkeypatch.setattr("hermes_cli.tools_config._get_platform_tools", lambda *_: {"computer_use"})
    monkeypatch.setattr("gateway.run._resolve_gateway_model", lambda *_args, **_kwargs: "fixture-model")
    monkeypatch.setattr("gateway.run._load_gateway_config", dict)
    monkeypatch.setattr(
        "gateway.run.GatewayRunner._load_reasoning_config", staticmethod(lambda model="": {}),
    )
    monkeypatch.setattr("gateway.run.GatewayRunner._load_fallback_model", staticmethod(lambda: None))
    monkeypatch.setattr("gateway.run._current_max_iterations", lambda: 6)

    async def send(*args, content=None, **_kwargs):
        if content is None and len(args) >= 2:
            content = args[1]
        sent.append(str(content))
        return SendResult(success=True, message_id="cua-r7-agent-reply")

    monkeypatch.setattr(api, "send", send)

    def message_text(message):
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return " ".join(
                part.get("text", "") for part in content
                if isinstance(part, dict) and isinstance(part.get("text"), str)
            )
        return json.dumps(content, sort_keys=True) if content is not None else ""

    def json_objects(text):
        decoder = json.JSONDecoder()
        for index, char in enumerate(text):
            if char != "{":
                continue
            try:
                value, _end = decoder.raw_decode(text[index:])
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                yield value

    def responder(record):
        messages = record["body"].get("messages", [])
        tool_messages = [m for m in messages if isinstance(m, dict) and m.get("role") == "tool"]
        if not tool_messages:
            return ToolCall("computer_use", {"action": "list_windows"})
        for message in reversed(tool_messages):
            text = message_text(message)
            for value in json_objects(text):
                windows = value.get("windows")
                if not isinstance(windows, list):
                    continue
                matches = [
                    row for row in windows
                    if isinstance(row, dict) and row.get("pid") == target_holder.get("pid")
                    and target_holder.get("title") in str(row.get("title") or "")
                ]
                if len(matches) == 1:
                    row = matches[0]
                    target_holder.update(window_id=int(row["window_id"]), app_name=str(row["app_name"]))
                    return ToolCall("computer_use", {
                        "action": "capture", "mode": "vision", "app": target_holder["app_name"],
                        "pid": target_holder["pid"], "window_id": target_holder["window_id"],
                    })
        if any("image_url" in message_text(m) or "_multimodal" in message_text(m) for m in tool_messages):
            return Text("managed CUA public turn complete")
        return ToolCall("computer_use", {"action": "list_windows"})

    source = api.build_source(
        chat_id=f"{task_id}@s.whatsapp.net", user_id="15551234567@s.whatsapp.net", chat_type="dm",
    )
    event = MessageEvent(text="use computer use", source=source, message_id=task_id)
    session_key = runner.session_store._generate_session_key(source)
    with _owned_linux_target(tmp_path) as target:
        target_holder.update(pid=target["pid"], title=target["title"])
        with FakeLLMServer(responder) as llm:
            monkeypatch.setattr(
                "gateway.run._resolve_runtime_agent_kwargs",
                lambda llm=llm: {
                    "provider": "openai", "api_key": "fixture-key", "base_url": llm.base_url,
                    "api_mode": "chat_completions",
                },
            )
            await api.handle_message(event)
            task = api._session_tasks.get(session_key)
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
            assert tool_names.count("computer_use") >= 2
            assert target_holder.get("window_id")
            assert any("capture mode=vision" in json.dumps(request, sort_keys=True)
                       for request in requests)
            assert any("managed CUA public turn complete" in text for text in sent)


@pytest.mark.asyncio
async def test_public_cua_late_finalizer_retains_closed_physical_for_same_owner_retry(
    runtime_owned_api_owner, monkeypatch, tmp_path,
):
    """A late registry refusal retains the closed tuple and retries that fence."""
    _require_sealed_driver()
    from tools.computer_use.tool import release_computer_use_session

    api, authority = runtime_owned_api_owner
    with _owned_linux_target(tmp_path) as target:
        await _execute_public_turn(
            api, authority, request_id="cua-r7-late", session_id="cua-late-session",
            action_session=_SESSION + "-late", target=target,
        )
        parent, receipt, _owner = _find_cua_parent(authority, _SESSION + "-late")
        root, physical = parent.root_handle, receipt.physical
        assert physical is not None
        original_forget = parent.profile_authority.forget_external_process_finalizer
        calls = []

        def fail_once(operation_id, candidate):
            calls.append((operation_id, candidate))
            if len(calls) == 1:
                return False
            return original_forget(operation_id, candidate)

        monkeypatch.setattr(parent.profile_authority, "forget_external_process_finalizer", fail_once)
        assert release_computer_use_session(_SESSION + "-late") is False
        assert calls and calls[0][1] is parent
        assert parent.root_handle is root
        assert parent._physical is physical
        assert receipt.physical is physical
        assert physical.state in {"closed", "released"}
        assert parent.state == "unknown"
        monkeypatch.setattr(parent.profile_authority, "forget_external_process_finalizer", original_forget)
        assert release_computer_use_session(_SESSION + "-late") is True
        assert parent.root_handle is root
        assert parent._cua_receipt is None
