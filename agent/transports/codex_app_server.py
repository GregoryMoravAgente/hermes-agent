"""Codex app-server JSON-RPC client (newline-delimited JSON-RPC 2.0 over stdio, codex 0.125+).

``initialize`` handshake, then ``thread/start`` + ``turn/start`` with streaming
``item/*`` notifications until ``turn/completed``. Wire-level speaker only —
projection, approvals and transcript handling live in sibling modules.
"""

from __future__ import annotations

import contextlib
import json
import os
import queue
import re
import subprocess
import sys
import threading
from dataclasses import dataclass
from typing import Any, Optional

from agent.transports.hermes_tools_mcp_server import HERMES_TOOLS_MCP_SERVER_NAME
from tools.environments.local import hermes_subprocess_env
from hermes_cli._subprocess_compat import kill_process_tree

MIN_CODEX_VERSION = (0, 125, 0)


@dataclass
class CodexAppServerError(RuntimeError):
    """Raised on JSON-RPC errors from the app-server."""

    code: int
    message: str
    data: Optional[Any] = None

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"codex app-server error {self.code}: {self.message}"


class CodexAppServerTransportError(CodexAppServerError):
    """The JSON-RPC transport is gone: a write failed or close() drained the request.

    Distinct from server-reported errors so session boundaries can retire the
    session without swallowing unrelated ``RuntimeError`` programming defects.
    """

    def __str__(self) -> str:  # pragma: no cover - trivial
        return self.message


_TRANSPORT_LOST_CODE = -32000


def _snapshot_descendants(pid: int) -> list[Any]:
    """psutil handles for ``pid``'s current descendants ([] when psutil is unavailable)."""
    try:
        import psutil
        return psutil.Process(int(pid)).children(recursive=True)
    except Exception:
        return []


def _reap_snapshotted(descendants: list[Any]) -> None:
    """SIGTERM the snapshotted descendants (deepest first), then SIGKILL survivors after a
    bounded wait. psutil identity checks make a recycled PID a no-op."""
    if not descendants:
        return
    import psutil
    live = []
    for child in reversed(descendants):
        with contextlib.suppress(Exception):
            if child.is_running():
                child.terminate()
                live.append(child)
    try:
        _, alive = psutil.wait_procs(live, timeout=1.0)
    except Exception:
        alive = live
    for child in alive:
        with contextlib.suppress(Exception):
            child.kill()


def _managed_physical(client: Any) -> bool:
    return (
        getattr(client, "_physical_receipt", None) is not None
        or getattr(getattr(client, "_producer_policy", None), "parent", None) is not None
    )


def _close_descendants(client: Any, managed: bool) -> list[Any]:
    if getattr(client, "_closing_descendants", None) is not None:
        return client._closing_descendants
    if managed or client._proc.poll() is not None:
        descendants = []
    else:
        descendants = _snapshot_descendants(client._proc.pid)
    client._closing_descendants = descendants
    return descendants


def _force_close_process(client: Any) -> None:
    policy = getattr(client, "_producer_policy", None)
    legacy_cleanup = (
        getattr(client, "_custody", None) is None and policy is None
    ) or (policy is not None and getattr(policy, "legacy_tree_kill_allowed", False) is True)
    if legacy_cleanup:
        kill_process_tree(client._proc)
    else:
        client._proc.kill()
    client._proc.wait(timeout=1.0)


def _settle_physical_receipt(client: Any, timeout: float) -> None:
    reader = getattr(client, "_reader", None)
    stderr_reader = getattr(client, "_stderr_reader", None)
    join_timeout = max(0.0, min(float(timeout), 1.0))
    if reader is not None:
        reader.join(timeout=join_timeout)
    if stderr_reader is not None:
        stderr_reader.join(timeout=join_timeout)
    receipt = getattr(client, "_physical_receipt", None)
    if receipt is None:
        return
    if reader is None or stderr_reader is None or reader.is_alive() or stderr_reader.is_alive():
        receipt.mark_unknown("reader_join_failed")
        raise CodexAppServerTransportError(
            code=_TRANSPORT_LOST_CODE, message="codex app-server reader finalizer did not settle",
        )
    receipt.reader_joined("stdout", reader)
    receipt.reader_joined("stderr", stderr_reader)
    if receipt.close_after_delivery() is not True:
        raise CodexAppServerTransportError(
            code=_TRANSPORT_LOST_CODE, message="codex app-server physical custody did not settle",
        )


def _launch_managed_process(client: Any, cmd: list[str], spawn_env: dict[str, str], hide_flags: int) -> None:
    client._physical_receipt = client._custody.reserve_physical()
    try:
        if sys.platform == "win32":
            from tools.process_registry_windows import begin_windows_launch
            client._launch_transaction = begin_windows_launch(client._physical_receipt)
            if (
                client._physical_receipt is None
                or client._physical_receipt.retain_launch_transaction(client._launch_transaction) is not True
            ):
                raise RuntimeError("Codex launch transaction could not be retained")
            client._proc = client._launch_transaction.launch(
                cmd, text=False, env=spawn_env, cwd=client._cwd,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                encoding="utf-8", errors="replace", start_new_session=True,
                creationflags=hide_flags,
            )
            client._custody.bind_physical_launch(client._proc, client._launch_transaction)
        else:
            client._proc = subprocess.Popen(
                cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                bufsize=0, env=spawn_env, cwd=client._cwd, creationflags=hide_flags,
            )
            if not client._physical_receipt.bind_process(client._proc.pid, None, "posix_direct", "posix_direct"):
                client._physical_receipt.mark_unknown("process_identity_rejected")
                raise RuntimeError("posix process identity rejected")
            client._custody.bind_physical_launch(client._proc)
    except FileNotFoundError:
        if client._launch_transaction is not None and client._launch_transaction.release_before_effect():
            client._physical_receipt = None
        raise
    except BaseException:
        receipt = client._physical_receipt
        transaction = client._launch_transaction
        process = getattr(client, "_proc", None)
        if process is None and transaction is not None:
            process = getattr(transaction, "process", None)
        if receipt is not None:
            receipt.retain_partial_launch(process, transaction, reason="partial_launch")
        raise


def _launch_external_process(client: Any, cmd: list[str], spawn_env: dict[str, str], hide_flags: int) -> None:
    from gateway.runtime_service_operations import bind_physical, bind_windows_launch, release_before_effect, reserve_physical
    parent = client._producer_policy.parent
    client._physical_receipt = reserve_physical(parent)
    try:
        if sys.platform == "win32":
            from tools.process_registry_windows import begin_windows_launch
            client._launch_transaction = begin_windows_launch(client._physical_receipt)
            if (
                client._physical_receipt is None
                or client._physical_receipt.retain_launch_transaction(client._launch_transaction) is not True
            ):
                raise RuntimeError("Codex launch transaction could not be retained")
            client._proc = client._launch_transaction.launch(
                cmd, text=False, env=spawn_env, cwd=client._cwd,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                encoding="utf-8", errors="replace", start_new_session=True,
                creationflags=hide_flags,
            )
            if client._proc is False or client._proc is None:
                raise RuntimeError("external process launch did not return a process")
            bind_windows_launch(parent, client._launch_transaction, client._proc)
        else:
            client._proc = subprocess.Popen(
                cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                bufsize=0, env=spawn_env, cwd=client._cwd, creationflags=hide_flags,
            )
            bind_physical(
                parent, None, client._proc, pid=client._proc.pid, process_started_at=None,
                scope="posix_direct", scope_identity="posix_direct",
            )
    except FileNotFoundError:
        if (
            (client._launch_transaction is not None and client._launch_transaction.release_before_effect())
            or release_before_effect(parent)
        ):
            client._physical_receipt = None
        raise
    except BaseException:
        receipt = client._physical_receipt
        transaction = client._launch_transaction
        process = getattr(client, "_proc", None)
        if process is None and transaction is not None:
            process = getattr(transaction, "process", None)
        if receipt is not None:
            receipt.retain_partial_launch(process, transaction, reason="partial_launch")
        raise


def _validate_process_policy(policy: Any) -> None:
    if policy is not None:
        from gateway.runtime_service_operations import validate_producer_policy
        validate_producer_policy(policy, spawning=True)


def _launch_process(client: Any, cmd: list[str], spawn_env: dict[str, str], hide_flags: int) -> None:
    if client._custody is not None:
        _launch_managed_process(client, cmd, spawn_env, hide_flags)
    elif client._producer_policy is not None:
        _launch_external_process(client, cmd, spawn_env, hide_flags)
    else:
        client._proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            bufsize=0, env=spawn_env, cwd=client._cwd, creationflags=hide_flags,
        )


class CodexAppServerClient:
    """Minimal synchronous JSON-RPC 2.0 client for ``codex app-server`` over stdio.

    One reader thread routes replies to pending queues and notifications / server
    requests to queues; another captures stderr. Deliberately NOT async:
    AIAgent.run_conversation() is synchronous and cancels via ``turn/interrupt``.
    """

    def __init__(
        self, codex_bin: str = "codex", codex_home: Optional[str] = None,
        extra_args: Optional[list[str]] = None, env: Optional[dict[str, str]] = None,
        cwd: Optional[str] = None, custody: Any = None, require_custody: bool = False,
        producer_policy: Any = None,
    ) -> None:
        self._codex_bin = codex_bin
        self._cwd = cwd or os.getcwd()
        self._custody = custody
        self._require_custody = require_custody
        self._producer_policy = producer_policy
        self._physical_receipt = None
        self._launch_transaction = None
        if self._require_custody and self._custody is None:
            from gateway.runtime_service_custody import ServiceCustodyError
            raise ServiceCustodyError("preservation_owner_unmapped")
        if self._producer_policy is not None:
            from gateway.runtime_service_operations import validate_producer_policy
            validate_producer_policy(self._producer_policy, spawning=True)
        # codex needs LLM provider creds but must not receive Tier-1 Hermes secrets (gateway/GitHub/infra tokens).
        # codex app-server is a model-driving CLI executor: it runs a model-chosen agentic loop that
        # executes shell commands, so it legitimately needs LLM provider credentials
        # (inherit_credentials=True) to authenticate against the model endpoint. But the previous
        # `os.environ.copy()` also handed it every Tier-1 Hermes secret — gateway bot tokens, GitHub auth,
        # Modal/Daytona infra tokens, the dashboard session token, AUXILIARY_* side-LLM keys,
        # GATEWAY_RELAY_* auth — none of which a coding subprocess has any use for. Route through the
        # centralized helper so Tier-1 + dynamic-internal secrets are always stripped while provider creds
        # still flow, matching copilot_acp_client (#29157 sibling spawn-site gap).
        spawn_env = hermes_subprocess_env(inherit_credentials=True)
        if env:
            spawn_env.update(env)
        if codex_home:
            spawn_env["CODEX_HOME"] = codex_home

        cmd = [codex_bin, "app-server", *(extra_args or [])]
        from agent.delegation_context import (
            DELEGATED_CHILD_ENV_MARKER, KANBAN_ENV_KEYS,
            delegated_child_subprocess_env, is_dispatcher_owned_worker_context,
        )
        # Native shell children remain unowned. Only Hermes' managed MCP tool
        # endpoint acts for this worker; grant it scope via its existing per-server
        # environment (the entry the runtime migration registers), never by granting
        # the whole executor process ownership.
        owned_task = os.environ.get("HERMES_KANBAN_TASK") and is_dispatcher_owned_worker_context()
        if owned_task:
            for key in (*KANBAN_ENV_KEYS, "HERMES_KANBAN_DB", "HERMES_KANBAN_BOARD"):
                if key in os.environ:
                    cmd += ["-c", f"mcp_servers.{HERMES_TOOLS_MCP_SERVER_NAME}.env.{key}={json.dumps(os.environ[key])}"]
            cmd += ["-c", f'mcp_servers.{HERMES_TOOLS_MCP_SERVER_NAME}.env.{DELEGATED_CHILD_ENV_MARKER}=""']
        spawn_env = delegated_child_subprocess_env(spawn_env)
        # Kanban workers must write handoff/status to the board DB outside the
        # workspace: keep the sandbox on, add the Kanban root as writable.
        if owned_task:
            kanban_db = spawn_env.get("HERMES_KANBAN_DB")
            default_root = os.path.join(spawn_env.get("HERMES_HOME", os.path.expanduser("~/.hermes")), "kanban")
            kanban_root = os.path.dirname(kanban_db) if kanban_db else spawn_env.get("HERMES_KANBAN_ROOT", default_root)
            cmd += [
                "-c", 'sandbox_mode="workspace-write"',
                "-c", f'sandbox_workspace_write.writable_roots=["{kanban_root}"]',
                "-c", "sandbox_workspace_write.network_access=false",
            ]
        # Codex emits tracing to stderr; default WARN keeps it quiet for users.
        spawn_env.setdefault("RUST_LOG", "warn")

        # Hide the console the codex child would otherwise flash on Windows (#56747).
        # Hide-only — stdio pipes stay intact for the app-server wire.
        # See #56747.
        from hermes_cli._subprocess_compat import windows_hide_flags

        _validate_process_policy(self._producer_policy)
        _launch_process(self, cmd, spawn_env, windows_hide_flags())
        self._next_id = 1
        self._pending: dict[int, queue.Queue] = {}  # request id -> single-slot reply queue
        self._pending_lock = threading.Lock()
        self._notifications: queue.Queue = queue.Queue()
        self._server_requests: queue.Queue = queue.Queue()
        self._stderr_lines: list[str] = []
        self._stderr_lock = threading.Lock()
        self._closed = False
        self._close_lock = threading.Lock()
        self._close_complete = False
        self._closing_descendants = None
        self._initialized = False

        from agent.memory_provider import spawn_context_thread
        self._reader = spawn_context_thread(
            self._read_stdout, name="codex-app-server-stdout",
        )
        self._stderr_reader = spawn_context_thread(
            self._read_stderr, name="codex-app-server-stderr",
        )
        if self._physical_receipt is not None:
            self._physical_receipt.bind_reader("stdout", self._reader)
            self._physical_receipt.bind_reader("stderr", self._stderr_reader)
        self._reader.start()
        self._stderr_reader.start()

    def initialize(
        self, client_name: str = "hermes", client_title: str = "Hermes Agent",
        client_version: str = "0.1", capabilities: Optional[dict] = None, timeout: float = 10.0,
    ) -> dict:
        """Send ``initialize`` + ``initialized``; return the server's InitializeResponse."""
        if self._initialized:
            raise RuntimeError("already initialized")
        params = {
            "clientInfo": {"name": client_name, "title": client_title, "version": client_version},
            "capabilities": capabilities or {},
        }
        result = self.request("initialize", params, timeout=timeout)
        self.notify("initialized")
        self._initialized = True
        return result

    def close(self, timeout: float = 3.0) -> None:
        """Confirm root subprocess exit, then best-effort reap snapshotted descendants.

        Codex app-server owns stdio MCP descendants that may sit in their own process
        groups (``setsid``). Once the root exits they reparent and a parent walk can no
        longer find them, so descendants are snapshotted BEFORE the root is retired and
        the proven identities (PID + create time) are swept afterwards."""
        close_lock = getattr(self, "_close_lock", None) or threading.Lock()
        self._close_lock = close_lock
        if not close_lock.acquire(timeout=30):
            raise TimeoutError("codex app-server close is still in progress")
        try:
            if getattr(self, "_close_complete", False):
                return
            self._closed = True  # reject new requests, even if termination needs a retry
            self._fail_pending_requests("codex app-server client is closing")
            managed_physical = _managed_physical(self)
            descendants = _close_descendants(self, managed_physical)
            with contextlib.suppress(Exception):
                if self._proc.stdin and not self._proc.stdin.closed:
                    self._proc.stdin.close()
            try:
                if self._proc.poll() is None:
                    self._proc.terminate()
                self._proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                # Popen retains the child identity and guards signaling after exit. Do not
                # rediscover a process tree from a PID that may have been recycled on retry.
                _force_close_process(self)
            finally:
                if not managed_physical:
                    _reap_snapshotted(descendants)
            _settle_physical_receipt(self, timeout)
            self._close_complete = True
        finally:
            close_lock.release()

    def _fail_pending_requests(self, reason: str) -> None:
        """Unblock every thread currently sitting in request() instead of
        leaving them to ride out their own per-call timeout (up to 30s by
        default) after the transport they're waiting on has already died.
        Mirrors _read_stdout's own pop-then-deliver dispatch under the same
        lock, so a reply that lands at the exact same moment still wins the
        race cleanly instead of being dropped or double-delivered."""
        pending_lock = getattr(self, "_pending_lock", None)
        pending = getattr(self, "_pending", {})
        if pending_lock is None:
            pending_items = list(pending.items())
            pending.clear()
        else:
            with pending_lock:
                pending_items = list(pending.items())
                pending.clear()
        if not pending_items:
            return
        synthetic = {"error": {"code": _TRANSPORT_LOST_CODE, "message": reason}, "transportLost": True}
        for _rid, pending in pending_items:
            pending.put_nowait(synthetic)

    def __enter__(self) -> CodexAppServerClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def request(self, method: str, params: Optional[dict] = None, timeout: float = 30.0) -> dict:
        """Send a request and block for ``result``; raise CodexAppServerError on ``error``."""
        rid, self._next_id = self._next_id, self._next_id + 1
        q: queue.Queue = queue.Queue(maxsize=1)
        with self._pending_lock:
            self._pending[rid] = q
        try:
            self._send({"id": rid, "method": method, "params": params or {}})
        except CodexAppServerTransportError:
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise
        try:
            msg = q.get(timeout=timeout)
        except queue.Empty:
            with self._pending_lock:
                self._pending.pop(rid, None)
            raise TimeoutError(f"codex app-server method {method!r} timed out after {timeout}s")
        if "error" in msg:
            err = msg["error"]
            cls = CodexAppServerTransportError if msg.get("transportLost") else CodexAppServerError
            raise cls(code=err.get("code", -1), message=err.get("message", ""), data=err.get("data"))
        return msg.get("result", {})

    def notify(self, method: str, params: Optional[dict] = None) -> None:
        """Send a JSON-RPC notification (no id, no response expected)."""
        self._send({"method": method, "params": params or {}})

    def respond(self, request_id: Any, result: dict) -> None:
        """Reply to a server-initiated request (e.g. approval prompts)."""
        self._send({"id": request_id, "result": result})

    def respond_error(self, request_id: Any, code: int, message: str, data: Optional[Any] = None) -> None:
        """Reply to a server-initiated request with an error."""
        err: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            err["data"] = data
        self._send({"id": request_id, "error": err})

    @staticmethod
    def _take(q: queue.Queue, timeout: float) -> Optional[dict]:
        try:
            return q.get_nowait() if timeout <= 0 else q.get(timeout=timeout)
        except queue.Empty:
            return None

    def take_notification(self, timeout: float = 0.0) -> Optional[dict]:
        """Pop the next streaming notification, or None on timeout (0 = non-blocking)."""
        return self._take(self._notifications, timeout)

    def take_server_request(self, timeout: float = 0.0) -> Optional[dict]:
        """Pop the next server-initiated request (e.g. exec/applyPatch approval)."""
        return self._take(self._server_requests, timeout)

    def stderr_tail(self, n: int = 20) -> list[str]:
        """Return last n lines of codex's stderr (for error reports)."""
        if self._proc.poll() is not None:
            # The reader can still hold the crash text when the exit is first observed (#121467).
            self._stderr_reader.join(timeout=1.0)
        with self._stderr_lock:
            return list(self._stderr_lines[-n:])

    def is_alive(self) -> bool:
        return self._proc.poll() is None

    def _send(self, obj: dict) -> None:
        if self._closed:
            raise CodexAppServerTransportError(code=_TRANSPORT_LOST_CODE, message="codex app-server client is closed")
        if self._proc.stdin is None:
            raise CodexAppServerTransportError(code=_TRANSPORT_LOST_CODE, message="codex app-server stdin not available")
        try:
            self._proc.stdin.write((json.dumps(obj) + "\n").encode("utf-8"))
            self._proc.stdin.flush()
        except (OSError, ValueError) as exc:  # BrokenPipe, EINVAL on a torn-down pipe, write on closed file
            raise CodexAppServerTransportError(
                code=_TRANSPORT_LOST_CODE, message=f"codex app-server stdin closed unexpectedly: {exc}",
            ) from exc

    def _append_stderr(self, line: str) -> None:
        with self._stderr_lock:
            self._stderr_lines.append(line)
            if len(self._stderr_lines) > 500:  # bound memory
                self._stderr_lines = self._stderr_lines[-500:]

    def _read_stdout(self) -> None:
        receipt = getattr(self, "_physical_receipt", None)
        if self._proc.stdout is None:
            if receipt is not None:
                receipt.reader_failed("stdout_pipe_missing")
            return
        try:
            for line in iter(self._proc.stdout.readline, b""):
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line)
                except json.JSONDecodeError:
                    # Non-JSON stdout is unexpected; surface it via the stderr buffer.
                    self._append_stderr(f"<non-json on stdout> {line[:200]!r}")
                    continue
                self._dispatch(msg)
        except Exception as exc:
            if receipt is not None:
                receipt.reader_failed("stdout_reader_error")
            self._append_stderr(f"<stdout reader error> {exc}")
        finally:
            if receipt is not None and receipt.reader_error is None:
                receipt.reader_eof("stdout", getattr(self, "_reader", None))
            # EOF (codex died) or a reader failure: nobody will ever answer the
            # requests still waiting, so fail them now instead of letting each
            # ride out its per-call timeout.
            self._fail_pending_requests("codex app-server stdout closed")

    def _dispatch(self, msg: dict) -> None:
        if "id" in msg and ("result" in msg or "error" in msg):  # reply
            with self._pending_lock:
                pending = self._pending.pop(msg["id"], None)
            if pending is not None:
                with contextlib.suppress(queue.Full):  # pragma: no cover - defensive
                    pending.put_nowait(msg)
        elif "method" in msg:  # server-initiated request (has id) or notification
            (self._server_requests if "id" in msg else self._notifications).put(msg)

    def _read_stderr(self) -> None:
        receipt = getattr(self, "_physical_receipt", None)
        if self._proc.stderr is None:
            if receipt is not None:
                receipt.reader_failed("stderr_pipe_missing")
            return
        try:
            for line in iter(self._proc.stderr.readline, b""):
                self._append_stderr(line.decode("utf-8", "replace").rstrip())
        except Exception:
            if receipt is not None:
                receipt.reader_failed("stderr_reader_error")
            raise
        finally:
            if receipt is not None and receipt.reader_error is None:
                receipt.reader_eof("stderr", getattr(self, "_stderr_reader", None))


def parse_codex_version(output: str) -> Optional[tuple[int, int, int]]:
    """Parse ``codex --version`` output ("codex-cli 0.130.0 ...") into (major, minor, patch)."""
    match = re.search(r"(\d+)\.(\d+)\.(\d+)", output or "")
    return tuple(int(g) for g in match.groups()) if match else None


def check_codex_binary(
    codex_bin: str = "codex", min_version: tuple[int, int, int] = MIN_CODEX_VERSION
) -> tuple[bool, str]:
    """Verify codex CLI is installed and meets minimum version. Returns (ok, message)."""
    try:
        proc = subprocess.run(
            [codex_bin, "--version"], capture_output=True, text=True, encoding='utf-8', errors='replace',
            timeout=10, stdin=subprocess.DEVNULL,
            check=False,
        )
    except FileNotFoundError:
        return False, f"codex CLI not found at {codex_bin!r}. Install with: npm i -g @openai/codex"
    except subprocess.TimeoutExpired:
        return False, "codex --version timed out"
    if proc.returncode != 0:
        return False, f"codex --version exited {proc.returncode}: {proc.stderr.strip()}"
    version = parse_codex_version(proc.stdout)
    if version is None:
        return False, f"could not parse codex version from: {proc.stdout!r}"
    have = ".".join(map(str, version))
    if version < min_version:
        return False, f"codex {have} is older than required {'.'.join(map(str, min_version))}. Run: npm i -g @openai/codex"
    return True, have
