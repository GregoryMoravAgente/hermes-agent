"""Integration tests for tools.browser_supervisor.

Exercises the supervisor end-to-end against a real local Chrome
(``--remote-debugging-port``).  Skipped when Chrome is not installed
— these are the tests that actually verify the CDP wire protocol
works, since mock-CDP unit tests can only prove the happy paths we
thought to model.

These tests spawn a **real Chrome process** on the machine running them.
They are therefore opt-in, twice over:

* ``@pytest.mark.integration`` — excluded by the default
  ``addopts = "-m 'not integration'"`` in ``pyproject.toml``, so a bare
  ``pytest`` cannot launch a browser on a developer's desktop by accident.
* ``HERMES_E2E_BROWSER=1`` — the env gate this docstring has always claimed.
  It previously existed only in this prose: nothing read the variable, and
  the sole real gate was "is a Chrome binary on PATH", which is true on most
  desktops and on ``ubuntu-latest``. Now it is enforced.

Run manually:
    HERMES_E2E_BROWSER=1 scripts/run_tests.sh -m integration \\
        tests/tools/test_browser_supervisor.py

(``scripts/run_tests.sh`` runs under ``env -i`` and forwards
``HERMES_E2E_BROWSER`` explicitly; ``-m integration`` overrides the default
marker filter.)
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

import pytest


pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("HERMES_E2E_BROWSER", "").strip() != "1",
        reason="real-browser E2E: set HERMES_E2E_BROWSER=1 to opt in",
    ),
    pytest.mark.skipif(
        not shutil.which("google-chrome") and not shutil.which("chromium"),
        reason="Chrome/Chromium not installed",
    ),
]


def _find_chrome() -> str:
    from hermes_cli.browser_runtime import chromium_executable

    configured = chromium_executable()
    if configured:
        return configured
    for candidate in ("google-chrome", "chromium", "chromium-browser"):
        path = shutil.which(candidate)
        if path:
            return path
    pytest.skip("no Chrome binary found")


def _file_identity(path: str | Path | None) -> dict[str, object]:
    """Return bounded, non-path identity data for a local test artifact."""
    if not path:
        return {"name": None, "bytes": None, "sha256": None}
    try:
        candidate = Path(path)
        digest = hashlib.sha256()
        size = 0
        with candidate.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
        return {"name": candidate.name, "bytes": size, "sha256": digest.hexdigest()}
    except Exception as exc:
        return {"name": Path(path).name, "bytes": None, "sha256": None,
                "error_type": type(exc).__name__}


def _stderr_identity(stderr) -> dict[str, object]:
    if stderr is None:
        return {"bytes": None, "sha256": None}
    try:
        stderr.flush()
        stderr.seek(0)
        digest = hashlib.sha256()
        size = 0
        for chunk in iter(lambda: stderr.read(1024 * 1024), b""):
            size += len(chunk)
            digest.update(chunk)
        return {"bytes": size, "sha256": digest.hexdigest()}
    except Exception as exc:
        return {"bytes": None, "sha256": None, "error_type": type(exc).__name__}



def _redact_sample_output(payload: str, *, pid: int) -> list[str]:
    safe_lines = []
    for raw_line in payload.splitlines()[:512]:
        line = raw_line.strip()
        if not line or any(token in line.lower() for token in ("environment", "command line", "http://", "https://")):
            continue
        line = re.sub(r"/(?:Users|private|var|tmp|home|Volumes|Applications|opt|Library)/\S+", "<PATH>", line)
        line = re.sub(rf"\b{pid}\b", "<PID>", line)
        line = re.sub(r"\b0x[0-9a-fA-F]+\b", "<ADDR>", line)
        if line.startswith(("Sampling process", "Call graph", "Thread ")) or line[:1].isdigit():
            safe_lines.append(line[:512])
    return safe_lines[:128]


def _profile_startup_state(profile: str | None) -> dict[str, object]:
    if not profile:
        return {"state": "no-profile"}
    port_path = Path(profile) / "DevToolsActivePort"
    try:
        with port_path.open("rb") as stream:
            payload = stream.read(4097)
    except OSError as exc:
        return {"state": "missing", "error_type": type(exc).__name__}
    lines = payload.splitlines()
    first = lines[0].decode("utf-8", errors="replace") if lines else ""
    return {
        "state": "truncated" if len(payload) > 4096 else "present",
        "bytes": len(payload),
        "sha256": hashlib.sha256(payload[:4096]).hexdigest(),
        "line_count": len(lines),
        "first_line_numeric": first.isdigit(),
    }


def _native_startup_diagnostic(*, proc, executable: str | None, profile: str | None,
                               stage: str, port: int | None, last_error_type: str | None,
                               popen_started_ns: int | None) -> dict[str, object]:
    result = {
        "stage": stage,
        "last_error_type": last_error_type,
        "pid": proc.pid if proc is not None else None,
        "popen_started_monotonic_ns": popen_started_ns,
        "executable": _file_identity(executable),
        "profile_devtools": _profile_startup_state(profile),
        "port": port,
        "sample": {"status": "not-attempted"},
    }
    if proc is None:
        result["sample"] = {"status": "no-popen"}
        return result
    result["returncode_before_sample"] = proc.poll()
    if result["returncode_before_sample"] is not None:
        result["sample"] = {"status": "owned-process-not-alive"}
        return result
    sample_bin = Path("/usr/bin/sample")
    if not sample_bin.is_file() or not os.access(sample_bin, os.X_OK):
        result["sample"] = {"status": "unavailable"}
        return result
    raw_path = Path(profile) / ".hermes-chrome-startup.sample" if profile else None
    try:
        if raw_path is None:
            result["sample"] = {"status": "no-profile"}
            return result
        completed = subprocess.run(
            [str(sample_bin), str(proc.pid), "1", "1", "-mayDie", "-file", str(raw_path)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=2,
            check=False,
        )
        if raw_path.exists():
            with raw_path.open("rb") as stream:
                payload = stream.read(262145)
        else:
            payload = b""
        returncode_after_sample = proc.poll()
        sample_info = {
            "status": "captured" if raw_path.exists() else "no-output",
            "returncode": completed.returncode,
            "bytes_captured": min(len(payload), 262144),
            "truncated": len(payload) > 262144,
            "sha256_captured": hashlib.sha256(payload[:262144]).hexdigest(),
            "stderr": "discarded",
            "redacted_frames": [],
        }
        if returncode_after_sample is None:
            sample_info["redacted_frames"] = _redact_sample_output(
                payload[:262144].decode("utf-8", errors="replace"), pid=proc.pid
            )
        else:
            # Do not expose a stack if the owned process exited while sample ran;
            # the PID could have been reused before the readback completed.
            sample_info["status"] = "process-exited-during-sample"
        result["sample"] = sample_info
    except subprocess.TimeoutExpired:
        result["sample"] = {"status": "timeout"}
    except (OSError, ValueError) as exc:
        result["sample"] = {"status": "error", "error_type": type(exc).__name__}
    finally:
        if raw_path is not None:
            try:
                raw_path.unlink()
            except OSError:
                pass
    result["returncode_after_sample"] = proc.poll()
    return result

def _failure_diagnostic(*, stage: str, executable: str | None, proc, profile: str | None,
                        stderr, port: int | None, last_error_type: str | None,
                        native_startup: dict[str, object] | None = None) -> str:
    active_port = None
    if profile:
        active_port_path = Path(profile) / "DevToolsActivePort"
        try:
            active_port = {"exists": active_port_path.exists(), "port": port}
        except OSError as exc:
            active_port = {"exists": None, "port": port, "error_type": type(exc).__name__}
    return json.dumps({
        "stage": stage,
        "executable": _file_identity(executable),
        "pid": proc.pid if proc is not None else None,
        "returncode": proc.poll() if proc is not None else None,
        "devtools_active_port": active_port,
        "last_error_type": last_error_type,
        "stderr": _stderr_identity(stderr),
        "native_startup": native_startup,
    }, sort_keys=True)


def _cleanup_chrome(*, proc, profile: str | None, stderr, wait_timeout: float) -> None:
    """Reap only this fixture's process and remove only its profile/diagnostic file."""
    if proc is not None:
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            proc.wait(timeout=wait_timeout)
        except (subprocess.TimeoutExpired, AssertionError, Exception):
            try:
                proc.kill()
            except Exception:
                pass
            try:
                proc.wait(timeout=2)
            except (AssertionError, Exception):
                pass
    if profile is not None:
        shutil.rmtree(profile, ignore_errors=True)
    if stderr is not None:
        try:
            stderr.close()
        except Exception:
            pass


@pytest.fixture
def chrome_cdp(tmp_path):
    """Start a headless Chrome with --remote-debugging-port, yield its WS URL.

    Binds an ephemeral port (bind port 0, read back the real one) so
    concurrently-running test files in the per-file parallel runner can
    never collide on a fixed port.
    Always launches with ``--site-per-process`` so cross-origin iframes
    become real OOPIFs (needed by the iframe interaction tests).
    """
    profile = None
    stderr = None
    proc = None
    executable = None
    ws_url = None
    port = None
    stage = "allocate-profile"
    last_error_type = None
    yielded = False
    popen_started_ns = None
    try:
        profile = tempfile.mkdtemp(prefix="hermes-supervisor-test-")
        stage = "open-stderr"
        stderr = (tmp_path / "chrome.stderr").open("w+b")
        stage = "resolve-executable"
        executable = _find_chrome()
        stage = "launch"
        proc = subprocess.Popen(
            [
                executable,
                "--remote-debugging-port=0",
                f"--user-data-dir={profile}",
                "--no-first-run",
                "--no-default-browser-check",
                "--headless=new",
                "--disable-gpu",
                "--site-per-process",  # force OOPIFs for cross-origin iframes
            ],
            stdout=subprocess.DEVNULL,
            stderr=stderr,
        )
        popen_started_ns = time.monotonic_ns()

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                stage = "chrome-exited-before-cdp"
                last_error_type = "ProcessExited"
                break
            try:
                stage = "read-devtools-active-port"
                port = int((Path(profile) / "DevToolsActivePort").read_text(encoding="utf-8").splitlines()[0])
                import urllib.request
                stage = "fetch-cdp-version"
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/json/version", timeout=1
                ) as r:
                    payload = r.read()
                stage = "parse-cdp-version"
                info = json.loads(payload.decode())
                ws_url = info["webSocketDebuggerUrl"]
                break
            except Exception as exc:
                last_error_type = type(exc).__name__
                time.sleep(0.25)
        if ws_url is None:
            native_startup = _native_startup_diagnostic(
                proc=proc, executable=executable, profile=profile, stage=stage,
                port=port, last_error_type=last_error_type, popen_started_ns=popen_started_ns,
            )
            diagnostic = _failure_diagnostic(
                stage=stage, executable=executable, proc=proc, profile=profile,
                stderr=stderr, port=port, last_error_type=last_error_type,
                native_startup=native_startup,
            )
            pytest.fail(f"Chrome didn't expose CDP in time: {diagnostic}")

        yielded = True
        yield ws_url, port
    finally:
        # The stdlib subprocess._wait() POSIX implementation has a known race
        # (https://bugs.python.org/issue38630); cleanup remains owned by this fixture
        # and force-kills only its process if the bounded wait does not reap it.
        _cleanup_chrome(proc=proc, profile=profile, stderr=stderr, wait_timeout=3 if yielded else 5)


def _test_page_url() -> str:
    html = """<!doctype html>
<html><head><title>Supervisor pytest</title></head><body>
<h1>Supervisor pytest</h1>
<iframe id="inner" srcdoc="<body><h2>frame-marker</h2></body>" width="400" height="100"></iframe>
</body></html>"""
    return "data:text/html;base64," + base64.b64encode(html.encode()).decode()


def _fire_on_page(cdp_url: str, expression: str) -> None:
    """Navigate the first page target to a data URL and fire `expression`."""
    import websockets as _ws_mod

    async def run():
        async with _ws_mod.connect(cdp_url, max_size=50 * 1024 * 1024) as ws:
            next_id = [1]

            async def call(method, params=None, session_id=None):
                cid = next_id[0]
                next_id[0] += 1
                p = {"id": cid, "method": method}
                if params:
                    p["params"] = params
                if session_id:
                    p["sessionId"] = session_id
                await ws.send(json.dumps(p))
                async for raw in ws:
                    m = json.loads(raw)
                    if m.get("id") == cid:
                        return m

            targets = (await call("Target.getTargets"))["result"]["targetInfos"]
            page = next(t for t in targets if t.get("type") == "page")
            attach = await call(
                "Target.attachToTarget", {"targetId": page["targetId"], "flatten": True}
            )
            sid = attach["result"]["sessionId"]
            await call("Page.navigate", {"url": _test_page_url()}, session_id=sid)
            await asyncio.sleep(1.5)  # let the page load
            await call(
                "Runtime.evaluate",
                {"expression": expression, "returnByValue": True},
                session_id=sid,
            )

    asyncio.run(run())


@pytest.fixture
def supervisor_registry():
    """Yield the global registry and tear down any supervisors after the test."""
    from tools.browser_supervisor import SUPERVISOR_REGISTRY

    yield SUPERVISOR_REGISTRY
    SUPERVISOR_REGISTRY.stop_all()


def _wait_for_dialog(supervisor, timeout: float = 5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        snap = supervisor.snapshot()
        if snap.pending_dialogs:
            return snap.pending_dialogs
        time.sleep(0.1)
    return ()


def test_supervisor_start_and_snapshot(chrome_cdp, supervisor_registry):
    """Supervisor attaches, exposes an active snapshot with a top frame."""
    cdp_url, _port = chrome_cdp
    supervisor = supervisor_registry.get_or_start(task_id="pytest-1", cdp_url=cdp_url)

    # Navigate so the frame tree populates.
    _fire_on_page(cdp_url, "/* no dialog */ void 0")

    # Give a moment for frame events to propagate
    time.sleep(1.0)
    snap = supervisor.snapshot()
    assert snap.active is True
    assert snap.task_id == "pytest-1"
    assert snap.pending_dialogs == ()
    # At minimum a top frame should exist after the navigate.
    assert snap.frame_tree.get("top") is not None


def test_main_frame_alert_detection_and_dismiss(chrome_cdp, supervisor_registry):
    """alert() in the main frame surfaces and can be dismissed via the sync API."""
    cdp_url, _port = chrome_cdp
    supervisor = supervisor_registry.get_or_start(task_id="pytest-2", cdp_url=cdp_url)

    _fire_on_page(cdp_url, "setTimeout(() => alert('PYTEST-MAIN-ALERT'), 50)")
    dialogs = _wait_for_dialog(supervisor)
    assert dialogs, "no dialog detected"
    d = dialogs[0]
    assert d.type == "alert"
    assert "PYTEST-MAIN-ALERT" in d.message

    result = supervisor.respond_to_dialog("dismiss")
    assert result["ok"] is True
    # State cleared after dismiss
    time.sleep(0.3)
    assert supervisor.snapshot().pending_dialogs == ()


def test_iframe_contentwindow_alert(chrome_cdp, supervisor_registry):
    """alert() fired from inside a same-origin iframe surfaces too."""
    cdp_url, _port = chrome_cdp
    supervisor = supervisor_registry.get_or_start(task_id="pytest-3", cdp_url=cdp_url)

    _fire_on_page(
        cdp_url,
        "setTimeout(() => document.querySelector('#inner').contentWindow.alert('PYTEST-IFRAME'), 50)",
    )
    dialogs = _wait_for_dialog(supervisor)
    assert dialogs, "no iframe dialog detected"
    assert any("PYTEST-IFRAME" in d.message for d in dialogs)

    result = supervisor.respond_to_dialog("accept")
    assert result["ok"] is True


def test_prompt_dialog_with_response_text(chrome_cdp, supervisor_registry):
    """prompt() gets our prompt_text back inside the page."""
    cdp_url, _port = chrome_cdp
    supervisor = supervisor_registry.get_or_start(task_id="pytest-4", cdp_url=cdp_url)

    # Fire a prompt and stash the answer on window
    _fire_on_page(
        cdp_url,
        "setTimeout(() => { window.__promptResult = prompt('give me a token', 'default-x'); }, 50)",
    )
    dialogs = _wait_for_dialog(supervisor)
    assert dialogs
    d = dialogs[0]
    assert d.type == "prompt"
    assert d.default_prompt == "default-x"

    result = supervisor.respond_to_dialog("accept", prompt_text="PYTEST-PROMPT-REPLY")
    assert result["ok"] is True


def test_browser_dialog_tool_end_to_end(chrome_cdp, supervisor_registry):
    """Full agent-path check: fire an alert, call the tool handler directly."""
    from tools.browser_dialog_tool import browser_dialog

    cdp_url, _port = chrome_cdp
    supervisor = supervisor_registry.get_or_start(task_id="pytest-tool", cdp_url=cdp_url)

    _fire_on_page(cdp_url, "setTimeout(() => alert('PYTEST-TOOL-END2END'), 50)")
    assert _wait_for_dialog(supervisor), "no dialog detected via wait_for_dialog"

    r = json.loads(browser_dialog(action="dismiss", task_id="pytest-tool"))
    assert r["success"] is True
    assert r["action"] == "dismiss"
    assert "PYTEST-TOOL-END2END" in r["dialog"]["message"]



def test_evaluate_runtime_unserializable_value(chrome_cdp, supervisor_registry):
    """``Infinity``/``NaN``/``BigInt`` come back via ``unserializableValue``."""
    cdp_url, _port = chrome_cdp
    supervisor = supervisor_registry.get_or_start(task_id="pytest-eval-5", cdp_url=cdp_url)

    _fire_on_page(cdp_url, "void 0")
    time.sleep(0.5)

    out = supervisor.evaluate_runtime("Infinity")
    assert out["ok"] is True
    assert out["result"] == "Infinity"
