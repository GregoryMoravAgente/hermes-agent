"""ByteRover memory plugin — MemoryProvider interface.

Persistent memory via the ByteRover CLI (``brv``): hierarchical context tree with tiered retrieval
(fuzzy text → LLM-driven search), local-first with optional cloud sync (BRV_API_KEY). Requires the
``brv`` CLI (npm install -g byterover-cli, or byterover.dev/install.sh). Working directory is
$HERMES_HOME/byterover/ (profile-scoped); ``memory.byterover.auto_extract: false`` disables curate hooks.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import threading
import time
from pathlib import Path
from time import monotonic as _monotonic
from typing import Any, Dict, List, Optional

from agent.memory_provider import MemoryProvider, spawn_context_thread
from plugins.memory._retirement import (
    ProviderIdentity,
    new_identity,
    receipt_reason,
    retirement_result,
    thread_is_closed,
    watch_thread_until_closed,
)
from tools.registry import tool_error

logger = logging.getLogger(__name__)

_QUERY_TIMEOUT = 10    # brv query — should be fast
_CURATE_TIMEOUT = 120  # brv curate — may involve LLM processing
_MIN_QUERY_LEN = 10    # noise filters
_MIN_OUTPUT_LEN = 20
_RETIREMENT_WAIT_S = 10.0

_BOOL_WORDS = {**dict.fromkeys(("1", "true", "yes", "on"), True), **dict.fromkeys(("0", "false", "no", "off"), False)}


def _coerce_bool(value: Any, default: bool = False) -> bool:
    """bool/number -> truthiness; common true/false words parsed; anything else -> default."""
    if isinstance(value, (bool, int, float)):
        return bool(value)
    return _BOOL_WORDS.get(value.strip().lower(), default) if isinstance(value, str) else default


def _load_plugin_config() -> dict[str, Any]:
    """Read ``memory.byterover``; fall back to legacy ``memory.provider_config`` (early docs used it)."""
    try:
        from hermes_cli.config import load_config
        memory_config = load_config().get("memory", {})
    except Exception:
        return {}
    for key in ("byterover", "provider_config") if isinstance(memory_config, dict) else ():
        block = memory_config.get(key, {})
        if isinstance(block, dict) and (block or key == "provider_config"):
            return dict(block)
    return {}


def _get_brv_cwd() -> Path:
    """Profile-scoped working directory for the brv context tree."""
    from hermes_constants import get_hermes_home
    return get_hermes_home() / "byterover"


# brv binary resolution (cached, thread-safe): None = unresolved, "" = resolved-missing
_brv_path_lock = threading.Lock()
_cached_brv_path: Optional[str] = None


def _resolve_brv_path() -> Optional[str]:
    """Find the brv binary on PATH or well-known install locations (resolved once; lookup is outside the lock)."""
    global _cached_brv_path
    with _brv_path_lock:
        if _cached_brv_path is not None:
            return _cached_brv_path or None
    candidates = (Path.home() / ".brv-cli/bin/brv", Path("/usr/local/bin/brv"), Path.home() / ".npm-global/bin/brv")
    found = shutil.which("brv") or next((str(c) for c in candidates if c.exists()), None)
    with _brv_path_lock:
        if _cached_brv_path is None:
            _cached_brv_path = found or ""
        return _cached_brv_path or None


def _brv_child_env(brv_path: str) -> dict[str, str]:
    """Env for the ``brv`` child. Under gateway.multiplex_profiles ``os.environ`` holds the
    LAUNCH profile's ``.env``; the served profile's ``BRV_*`` (cloud identity) live only in its
    secret scope, so they are resolved through the scope — a miss means no cloud key, never
    another profile's — and the launch profile's ``.env`` residue is stripped. Outside multiplex
    the process env IS this profile's own and is passed through unchanged."""
    from agent.secret_scope import UnscopedSecretError, get_secret, is_multiplex_active
    from hermes_constants import get_hermes_home_override
    from tools.environments.local import build_subprocess_env, strip_launch_profile_env

    env = build_subprocess_env(scrub_secrets=False)
    if is_multiplex_active():
        env = strip_launch_profile_env(env, get_hermes_home_override())
        for key in [k for k in env if k.startswith("BRV_")]:
            env.pop(key, None)
        try:
            api_key = get_secret("BRV_API_KEY", "")
        except UnscopedSecretError:
            api_key = ""
        if api_key:
            env["BRV_API_KEY"] = api_key
    env["PATH"] = str(Path(brv_path).parent) + os.pathsep + env.get("PATH", "")
    return env


def _run_brv(args: list[str], timeout: int = _QUERY_TIMEOUT, cwd: str | None = None) -> dict:
    """Run a brv CLI command. Returns {success, output, error}."""
    global _cached_brv_path
    brv_path = _resolve_brv_path()
    if not brv_path:
        return {"success": False, "error": "brv CLI not found. Install: npm install -g byterover-cli"}
    effective_cwd = cwd or str(_get_brv_cwd())
    Path(effective_cwd).mkdir(parents=True, exist_ok=True)
    env = _brv_child_env(brv_path)
    try:
        result = subprocess.run(
            [brv_path] + args, capture_output=True, text=True, encoding='utf-8', errors='replace',
            timeout=timeout, cwd=effective_cwd, env=env, stdin=subprocess.DEVNULL,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return {"success": False, "error": f"brv timed out after {timeout}s"}
    except FileNotFoundError:
        with _brv_path_lock:
            _cached_brv_path = None  # binary vanished; re-resolve next call
        return {"success": False, "error": "brv CLI not found"}
    except Exception as e:
        return {"success": False, "error": str(e)}
    stdout, stderr = result.stdout.strip(), result.stderr.strip()
    if result.returncode == 0:
        return {"success": True, "output": stdout}
    return {"success": False, "error": stderr or stdout or f"brv exited {result.returncode}"}


def _schema(name: str, description: str, arg: str = "", arg_desc: str = "") -> dict:
    props = {arg: {"type": "string", "description": arg_desc}} if arg else {}
    return {"name": name, "description": description, "parameters": {"type": "object", "properties": props, "required": [arg] if arg else []}}


QUERY_SCHEMA = _schema(
    "brv_query", "Search ByteRover's persistent knowledge tree for relevant context. Returns memories, project knowledge, "
    "architectural decisions, and patterns from previous sessions. Use for any question where past context would help.",
    "query", "What to search for.")
CURATE_SCHEMA = _schema(
    "brv_curate", "Store important information in ByteRover's persistent knowledge tree. Use for architectural decisions, bug fixes, "
    "user preferences, project patterns — anything worth remembering across sessions. ByteRover's LLM automatically "
    "categorizes and organizes the memory.", "content", "The information to remember.")
STATUS_SCHEMA = _schema("brv_status", "Check ByteRover status — CLI version, context tree stats, cloud sync state.")


class ByteRoverMemoryProvider(MemoryProvider):
    """ByteRover persistent memory via the brv CLI."""

    def __init__(self, config: Optional[dict[str, Any]] = None):
        self._config = dict(config) if config is not None else _load_plugin_config()
        self._auto_extract = _coerce_bool(self._config.get("auto_extract"), True)
        self._cwd, self._session_id, self._turn_count = "", "", 0
        self._sync_thread: Optional[threading.Thread] = None
        self._retirement_condition = threading.Condition()
        self._retirement_mutex = threading.Lock()
        self._retirement_local = threading.local()
        self._retirement_identity = ProviderIdentity("", "", "")
        self._retirement_active_calls = 0
        self._retirement_threads: set[threading.Thread] = set()
        self._retirement_failures = 0
        self._retiring = False
        self._released = False
        self._retirement_in_progress = False
        self._retirement_initializing = False
        self._retirement_initialization_failed = False

    @property
    def name(self) -> str:
        return "byterover"

    def is_available(self) -> bool:
        """Check if brv CLI is installed. No network calls."""
        return _resolve_brv_path() is not None

    def get_config_schema(self):
        return [
            {"key": "api_key", "description": "ByteRover API key (optional, for cloud sync)", "secret": True,
             "env_var": "BRV_API_KEY", "url": "https://app.byterover.dev"},
            {"key": "auto_extract", "description": "Automatically curate completed turns and compression/memory hooks",
             "default": "true", "choices": ["true", "false"]},
        ]

    def initialize(self, session_id: str, **kwargs) -> None:
        # Bind to the profile the manager names, not the ambient one (multiplexing contract, MemoryProvider).
        home = kwargs.get("hermes_home")
        cwd = Path(home) / "byterover" if home else _get_brv_cwd()
        with self._retirement_condition:
            if self._retirement_active_calls or self._retirement_in_progress or self._retiring or self._released:
                raise RuntimeError("ByteRover cannot be reinitialized while work is active")
            self._retirement_identity = new_identity("byterover", Path(home) if home else cwd.parent)
            # Initialization owns a real filesystem admission until the profile directory exists.
            # Publish the incarnation now so observers can retain it, but do not let retirement
            # release it while the native startup work is still in flight.
            self._retirement_initializing = True
            self._retirement_active_calls += 1
            self._retiring = False
            self._released = False
            self._retirement_failures = 0
            self._retirement_initialization_failed = False
        try:
            self._cwd, self._session_id, self._turn_count = str(cwd), session_id, 0
            Path(self._cwd).mkdir(parents=True, exist_ok=True)
        except BaseException:
            with self._retirement_condition:
                self._retirement_initialization_failed = True
            raise
        finally:
            with self._retirement_condition:
                self._retirement_initializing = False
                self._retirement_active_calls = max(0, self._retirement_active_calls - 1)
                self._retirement_condition.notify_all()

    def retirement_receipt(self) -> dict[str, str]:
        """Return the source-owned profile and provider incarnation identity."""
        with self._retirement_condition:
            return self._retirement_identity.as_mapping()

    def _begin_call(self) -> bool:
        with self._retirement_condition:
            if self._retiring or self._released:
                return False
            self._retirement_active_calls += 1
            return True

    def _finish_call(self, *, failed: bool = False, thread: threading.Thread | None = None) -> None:
        watch = False
        with self._retirement_condition:
            if failed:
                self._retirement_failures += 1
            self._retirement_active_calls = max(0, self._retirement_active_calls - 1)
            if thread is not None:
                if thread_is_closed(thread):
                    self._retirement_threads.discard(thread)
                else:
                    watch = True
            self._retirement_condition.notify_all()
        if watch and thread is not None:
            def _drop_thread() -> None:
                with self._retirement_condition:
                    self._retirement_threads.discard(thread)
                    self._retirement_condition.notify_all()
            watch_thread_until_closed(thread, _drop_thread)

    def system_prompt_block(self) -> str:
        if not _resolve_brv_path():
            return ""
        return ("# ByteRover Memory\nActive. Persistent knowledge tree with hierarchical context.\n"
                "Use brv_query to search past knowledge, brv_curate to store important facts, brv_status to check state.")

    def _query(self, query: str) -> dict:
        if not self._begin_call():
            return {"success": False, "error": "ByteRover retirement is in progress"}
        try:
            return _run_brv(["query", "--", query.strip()[:5000]], timeout=_QUERY_TIMEOUT, cwd=self._cwd)
        # health: allow BLE001 -- optional CLI boundary must fail closed while preserving the call lease
        except Exception as exc:
            logger.debug("ByteRover query failed: %s", exc)
            return {"success": False, "error": str(exc)}
        finally:
            self._finish_call()

    def _curate(self, content: str) -> dict:
        admitted = bool(getattr(self._retirement_local, "admitted", False))
        if not admitted and not self._begin_call():
            return {"success": False, "error": "ByteRover retirement is in progress"}
        failed = False
        try:
            result = _run_brv(["curate", "--", content], timeout=_CURATE_TIMEOUT, cwd=self._cwd)
            failed = not bool(result.get("success"))
            return result
        # health: allow BLE001 -- optional CLI boundary must fail closed while preserving accepted writes
        except Exception as exc:
            failed = True
            logger.debug("ByteRover curate failed: %s", exc)
            return {"success": False, "error": str(exc)}
        finally:
            if not admitted:
                self._finish_call(failed=failed)

    def _status(self) -> dict:
        if not self._begin_call():
            return {"success": False, "error": "ByteRover retirement is in progress"}
        try:
            return _run_brv(["status"], timeout=15, cwd=self._cwd)
        # health: allow BLE001 -- optional CLI boundary must fail closed while preserving the call lease
        except Exception as exc:
            logger.debug("ByteRover status failed: %s", exc)
            return {"success": False, "error": str(exc)}
        finally:
            self._finish_call()

    def _curate_in_background(self, content: str, *, name: str, what: str, on_done: str = "") -> Optional[threading.Thread]:
        """Spawn a daemon thread that curates ``content``; failures are logged at debug, never raised."""
        with self._retirement_condition:
            if self._retiring or self._released:
                return None
            self._retirement_active_calls += 1

        finish_lock = threading.Lock()
        finished = False
        worker_started = threading.Event()

        def _finish_once(*, failed: bool, thread: threading.Thread | None = None) -> None:
            nonlocal finished
            with finish_lock:
                if finished:
                    return
                finished = True
            self._finish_call(failed=failed, thread=thread)

        t: threading.Thread | None = None

        def _work():
            failed = False
            try:
                worker_started.set()
                self._retirement_local.admitted = True
                result = self._curate(content)
                failed = isinstance(result, dict) and not bool(result.get("success"))
                if on_done and not failed:
                    logger.info(on_done)
                if failed and isinstance(result, dict):
                    logger.debug("ByteRover %s failed: %s", what, result.get("error", "unknown error"))
            except Exception as e:
                failed = True
                logger.debug("ByteRover %s failed: %s", what, e)
            finally:
                self._retirement_local.admitted = False
                _finish_once(failed=failed, thread=t if t is not None else threading.current_thread())

        try:
            t = spawn_context_thread(_work, name=name)
            with self._retirement_condition:
                self._retirement_threads.add(t)
            t.start()
        except BaseException:
            # A custom start wrapper may raise after launching the target. In that ambiguous
            # case the worker's finally owns the lease; only an unstarted object is released here.
            if t is None or (not worker_started.is_set() and getattr(t, "ident", None) is None):
                _finish_once(failed=True, thread=t)
            raise
        return t

    def _join_retirement_threads(self, timeout: float) -> bool:
        """Join every admitted ByteRover worker within one bounded custody window.

        The sync thread is only one of several accepted workers: memory mirrors and
        pre-compression flushes use their own native CLI calls.  A failed or ambiguous worker
        therefore keeps its exact admission lease until its own finish callback runs.
        """
        deadline = _monotonic() + max(0.0, timeout)
        current = threading.current_thread()
        while True:
            with self._retirement_condition:
                threads = tuple(self._retirement_threads)
                if not threads:
                    return True
            for thread in threads:
                if thread is current:
                    return False
                if thread_is_closed(thread):
                    with self._retirement_condition:
                        self._retirement_threads.discard(thread)
                    continue
                remaining = deadline - _monotonic()
                if remaining <= 0:
                    return False
                try:
                    thread.join(timeout=remaining)
                except RuntimeError as exc:
                    # An admitted but unusable thread cannot be treated as settled by dropping
                    # its provider lease.
                    logger.warning("ByteRover worker join failed: %s", exc)
                    return False
                if thread_is_closed(thread):
                    with self._retirement_condition:
                        self._retirement_threads.discard(thread)
                else:
                    return False
            remaining = deadline - _monotonic()
            if remaining <= 0:
                with self._retirement_condition:
                    return not self._retirement_threads
            with self._retirement_condition:
                if not self._retirement_threads:
                    return True
                self._retirement_condition.wait(min(0.05, remaining))

    def _auto_extract_enabled(self, what: str) -> bool:
        if not self._auto_extract:
            logger.debug("ByteRover %s skipped (auto_extract disabled)", what)
        return self._auto_extract

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        """Run brv query synchronously (blocks up to _QUERY_TIMEOUT) so context is ready before the first LLM call."""
        if not query or len(query.strip()) < _MIN_QUERY_LEN:
            return ""
        result = self._query(query)
        output = (result.get("output") or "").strip() if result["success"] else ""
        return f"## ByteRover Context\n{output}" if len(output) > _MIN_OUTPUT_LEN else ""

    def queue_prefetch(self, query: str, *, session_id: str = "") -> None:
        """No-op: prefetch() runs synchronously at turn start."""

    def sync_turn(self, user_content: str, assistant_content: str, *, session_id: str = "") -> None:
        """Curate the conversation turn in background (non-blocking); only substantive turns."""
        self._turn_count += 1
        if not self._auto_extract_enabled("sync_turn") or len(user_content.strip()) < _MIN_QUERY_LEN:
            return
        if self._sync_thread and self._sync_thread.is_alive():  # wait for the previous sync so curates don't pile up
            self._sync_thread.join(timeout=5.0)
        thread = self._curate_in_background(
            f"User: {user_content[:2000]}\nAssistant: {assistant_content[:2000]}",
            name="brv-sync", what="sync",
        )
        if thread is not None:
            self._sync_thread = thread

    def on_memory_write(self, action: str, target: str, content: str) -> None:
        """Mirror built-in memory writes to ByteRover."""
        if self._auto_extract_enabled("memory mirror") and action in {"add", "replace"} and content:
            label = "User profile" if target == "user" else "Agent memory"
            self._curate_in_background(f"[{label}] {content}", name="brv-memwrite", what="memory mirror")

    def on_pre_compress(self, messages: list[dict[str, Any]]) -> str:
        """Extract insights from the last 10 user/assistant messages before compression discards them."""
        if not self._auto_extract_enabled("pre-compression flush") or not messages:
            return ""
        parts = [f"{msg.get('role', '')}: {msg.get('content', '')[:500]}" for msg in messages[-10:]
                 if msg.get("role", "") in {"user", "assistant"} and isinstance(msg.get("content", ""), str) and msg.get("content", "").strip()]
        if parts:
            self._curate_in_background(
                "[Pre-compression context]\n" + "\n".join(parts),
                name="brv-flush", what="pre-compression flush",
                on_done=f"ByteRover pre-compression flush: {len(parts)} messages",
            )
        return ""

    def get_tool_schemas(self) -> list[dict[str, Any]]:
        return [QUERY_SCHEMA, CURATE_SCHEMA, STATUS_SCHEMA]

    def handle_tool_call(self, tool_name: str, args: dict, **kwargs) -> str:
        if tool_name not in _TOOLS:
            return tool_error(f"Unknown tool: {tool_name}")
        arg, fail_msg, run, on_ok = _TOOLS[tool_name]
        value = args.get(arg, "") if arg else None
        if arg and not value:
            return tool_error(f"{arg} is required")
        result = run(self, value)
        return json.dumps(on_ok(result.get("output", ""))) if result["success"] else tool_error(result.get("error", fail_msg))

    def retire(self, receipt: dict[str, str] | None = None) -> dict[str, object]:
        """Drain accepted CLI work and retain the provider on any ambiguous failure."""
        with self._retirement_condition:
            current = self._retirement_identity
        requested, reason = receipt_reason(receipt, current)
        if reason is not None:
            return retirement_result(requested, status="unknown", reason=reason)

        if not self._retirement_mutex.acquire(blocking=False):
            return retirement_result(current, status="unknown", reason="retirement_in_progress")
        try:
            with self._retirement_condition:
                # receipt_reason ran before taking the mutex so a concurrent initializer can
                # proceed. Re-read the source-owned incarnation under the retirement fence;
                # an old receipt must never retire the replacement instance.
                current = self._retirement_identity
                if current != requested:
                    mismatch = "foreign_receipt" if (
                        current.provider != requested.provider or current.home_id != requested.home_id
                    ) else "stale_receipt"
                    return retirement_result(requested, status="unknown", reason=mismatch)
                if self._retirement_initialization_failed:
                    return retirement_result(current, status="unknown", reason="store_open_failed")
                if self._released:
                    return retirement_result(current, status="released", reason="released")
                self._retiring = True
                self._retirement_in_progress = True
                deadline = time.monotonic() + _RETIREMENT_WAIT_S
                while self._retirement_active_calls:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return retirement_result(
                            current, status="unknown", reason="pending_flush",
                            pending_flush_count=self._retirement_active_calls,
                        )
                    self._retirement_condition.wait(remaining)
            remaining = max(0.0, deadline - time.monotonic())
            if not self._join_retirement_threads(remaining):
                with self._retirement_condition:
                    pending = max(1, len(self._retirement_threads))
                return retirement_result(
                    current, status="unknown", reason="pending_flush",
                    pending_flush_count=pending,
                )
            with self._retirement_condition:
                failures = self._retirement_failures
            if failures:
                return retirement_result(
                    current, status="unknown", reason="flush_failed",
                    pending_flush_count=failures,
                )
            with self._retirement_condition:
                self._released = True
            return retirement_result(current, status="released", reason="released")
        finally:
            with self._retirement_condition:
                self._retirement_in_progress = False
                self._retirement_condition.notify_all()
            self._retirement_mutex.release()

    def shutdown(self) -> None:
        if not self._join_retirement_threads(_RETIREMENT_WAIT_S):
            logger.warning("ByteRover provider shutdown remains unresolved; workers retained")
            return
        self._sync_thread = None


def _format_query_output(output: str) -> dict:
    output = output.strip()
    if len(output) < _MIN_OUTPUT_LEN:
        return {"result": "No relevant memories found."}
    return {"result": output[:8000] + "\n\n[... truncated]" if len(output) > 8000 else output}


# tool name -> (required arg or None, failure message, run(provider, arg_value) -> brv result, on_ok(output) -> JSON payload)
_TOOLS = {
    "brv_query": ("query", "Query failed", lambda p, q: p._query(q), _format_query_output),
    "brv_curate": ("content", "Curate failed", lambda p, c: p._curate(c), lambda _: {"result": "Memory curated successfully."}),
    "brv_status": (None, "Status check failed", lambda p, _: p._status(), lambda out: {"status": out}),
}


def register(ctx) -> None:
    """Register ByteRover as a memory provider plugin."""
    ctx.register_memory_provider(ByteRoverMemoryProvider())
