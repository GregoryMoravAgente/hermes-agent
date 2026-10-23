"""RetainDB memory plugin — MemoryProvider interface.

Cross-session memory via the RetainDB cloud API: durable SQLite write-behind queue, semantic
search + profile, context overlay, dialectic/agent self-model prefetch, shared file store tools.
Config: RETAINDB_API_KEY (required, scoped secret), RETAINDB_BASE_URL (default https://api.retaindb.com),
RETAINDB_PROJECT (optional; defaults to "default"); the non-secret two also read config.yaml ``memory.retaindb``.
"""

from __future__ import annotations

import json
import logging
import os
import queue
import re
import sqlite3
import threading
import time
from contextlib import suppress
from datetime import datetime, timezone, UTC
from pathlib import Path
from time import monotonic as _monotonic
from typing import Any, Callable
from urllib.parse import quote

from agent.memory_provider import MemoryProvider, spawn_context_thread
from agent.secret_scope import get_secret
from agent.file_safety import raise_if_read_blocked
from plugins.memory._retirement import (
    ProviderIdentity,
    ProviderRetirementLease,
    new_identity,
    preparation_result,
    receipt_reason,
    retirement_result,
    thread_is_closed,
    watch_thread_until_closed,
)
from tools.registry import tool_error

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://api.retaindb.com"
_ASYNC_SHUTDOWN = object()
_TEXT_EXTS = (".txt", ".md", ".json", ".csv", ".yaml", ".yml", ".xml", ".html")
_RETIREMENT_WAIT_S = 10.0


def _load_retaindb_config() -> dict[str, Any]:
    """``memory.retaindb`` block from config.yaml (empty on error): Dashboard-persisted base_url/project; api_key stays in scoped secrets."""
    try:
        from hermes_cli.config import load_config_readonly
        block = load_config_readonly().get("memory", {}).get("retaindb", {})
    except Exception:
        block = None
    return dict(block) if isinstance(block, dict) else {}


def _q(s: str) -> str:
    return quote(s, safe="")


def _quiet(label: str, fn: Callable[[], Any]) -> Any:
    """Run *fn*; on any exception log "RetainDB <label> failed" at debug and return None."""
    try:
        return fn()
    except Exception as exc:
        logger.debug("RetainDB %s failed: %s", label, exc)
        return None


def _schema(name: str, description: str, properties: dict | None = None, required: tuple = ()) -> dict:
    return {"name": name, "description": description,
            "parameters": {"type": "object", "properties": properties or {}, "required": list(required)}}


def _p(description: str, type_: str = "string", **extra) -> dict:
    return {"type": type_, **extra, "description": description}


_SCHEMAS = (
    _schema("retaindb_profile", "Get the user's stable profile — preferences, facts, and patterns recalled from long-term memory."),
    _schema("retaindb_search", "Semantic search across stored memories. Returns ranked results with relevance scores.",
            {"query": _p("What to search for."), "top_k": _p("Max results (default: 8, max: 20).", "integer")}, ("query",)),
    _schema("retaindb_context", "Synthesized context block — what matters most for the current task, pulled from long-term memory.",
            {"query": _p("Current task or question.")}, ("query",)),
    _schema("retaindb_remember", "Persist an explicit fact, preference, or decision to long-term memory.",
            {"content": _p("The fact to remember."),
             "memory_type": _p("Category (default: factual).", enum=["factual", "preference", "goal", "instruction", "event", "opinion"]),
             "importance": _p("Importance 0-1 (default: 0.7).", "number")}, ("content",)),
    _schema("retaindb_forget", "Delete a specific memory by ID.", {"memory_id": _p("Memory ID to delete.")}, ("memory_id",)),
    _schema("retaindb_upload_file", "Upload a file to the shared RetainDB file store. Returns an rdb:// URI any agent can reference.",
            {"local_path": _p("Local file path to upload."), "remote_path": _p("Destination path, e.g. /reports/q1.pdf"),
             "scope": _p("Access scope (default: PROJECT).", enum=["USER", "PROJECT", "ORG"]),
             "ingest": _p("Also extract memories from file after upload (default: false).", "boolean")}, ("local_path",)),
    _schema("retaindb_list_files", "List files in the shared file store.",
            {"prefix": _p("Path prefix to filter by, e.g. /reports/"), "limit": _p("Max results (default: 50).", "integer")}),
    _schema("retaindb_read_file", "Read the text content of a stored file by its file ID.",
            {"file_id": _p("File ID returned from upload or list.")}, ("file_id",)),
    _schema("retaindb_ingest_file", "Chunk, embed, and extract memories from a stored file. Makes its contents searchable.",
            {"file_id": _p("File ID to ingest.")}, ("file_id",)),
    _schema("retaindb_delete_file", "Delete a stored file.", {"file_id": _p("File ID to delete.")}, ("file_id",)),
)


class _Client:
    """Thin HTTP client over the RetainDB REST API (lazy ``requests`` import)."""

    def __init__(self, api_key: str, base_url: str, project: str):
        self.api_key, self.base_url, self.project = api_key, re.sub(r"/+$", "", base_url), project

    def _headers(self, path: str, json_body: bool = True) -> dict:
        token = self.api_key.replace("Bearer ", "").strip()
        return {"Authorization": f"Bearer {token}", "x-sdk-runtime": "hermes-plugin",
                **({"Content-Type": "application/json"} if json_body else {}),
                **({"X-API-Key": token} if path.startswith(("/v1/memory", "/v1/context")) else {})}  # memory/context also accept X-API-Key

    def _http(self, method: str, path: str, *, json_body: bool = True, timeout: float = 30, **kwargs):
        import requests
        return requests.request(method, f"{self.base_url}{path}", headers=self._headers(path, json_body), timeout=timeout, **kwargs)

    def request(self, method: str, path: str, *, params=None, json_body=None, timeout: float = 8.0) -> Any:
        """JSON request; raises RuntimeError carrying the server message on a non-2xx response."""
        method = method.upper()
        resp = self._http(method, path, params=params, json=json_body if method not in {"GET", "DELETE"} else None, timeout=timeout)
        try:
            payload = resp.json()
        except Exception:
            payload = resp.text
        if not resp.ok:
            msg = str(payload.get("message") or payload.get("error") or "") if isinstance(payload, dict) else ""
            raise RuntimeError(f"RetainDB {method} {path} failed ({resp.status_code}): {msg or payload}")
        return payload

    def _raw(self, method: str, path: str, **kwargs) -> Any:
        """Non-JSON request (multipart upload / binary download); raises on HTTP error."""
        resp = self._http(method, path, json_body=False, **kwargs)
        return resp.raise_for_status() or resp

    @staticmethod
    def _with_fallback(primary: Callable[[], dict], fallback: Callable[[], dict]) -> dict:
        """Try the current API route; on any error retry via the legacy route."""
        try:
            return primary()
        except Exception:
            return fallback()

    def _scoped(self, user_id: str, session_id: str, **extra) -> dict:
        return {"project": self.project, "user_id": user_id, "session_id": session_id, **extra}

    # Memory routes (one endpoint per method; bodies are the wire payloads)
    def query_context(self, user_id: str, session_id: str, query: str, max_tokens: int = 1200) -> dict:
        return self.request("POST", "/v1/context/query", json_body=self._scoped(user_id, session_id, query=query, include_memories=True, max_tokens=max_tokens))
    def search(self, user_id: str, session_id: str, query: str, top_k: int = 8) -> dict:
        return self.request("POST", "/v1/memory/search", json_body=self._scoped(user_id, session_id, query=query, top_k=top_k, include_pending=True))
    def get_profile(self, user_id: str) -> dict:
        return self._with_fallback(
            lambda: self.request("GET", f"/v1/memory/profile/{_q(user_id)}", params={"project": self.project, "include_pending": "true"}),
            lambda: self.request("GET", "/v1/memories", params={"project": self.project, "user_id": user_id, "limit": "200"}))
    def add_memory(self, user_id: str, session_id: str, content: str, memory_type: str = "factual", importance: float = 0.7) -> dict:
        body = self._scoped(user_id, session_id, content=content, memory_type=memory_type, importance=importance)
        return self._with_fallback(
            lambda: self.request("POST", "/v1/memory", json_body={**body, "write_mode": "sync"}, timeout=5.0),
            lambda: self.request("POST", "/v1/memories", json_body=body, timeout=5.0))
    def delete_memory(self, memory_id: str) -> dict:
        return self._with_fallback(
            lambda: self.request("DELETE", f"/v1/memory/{_q(memory_id)}", timeout=5.0),
            lambda: self.request("DELETE", f"/v1/memories/{_q(memory_id)}", timeout=5.0))
    def ingest_session(self, user_id: str, session_id: str, messages: list, timeout: float = 15.0) -> dict:
        return self.request("POST", "/v1/memory/ingest/session", json_body=self._scoped(user_id, session_id, messages=messages, write_mode="sync"), timeout=timeout)
    def ask_user(self, user_id: str, query: str, reasoning_level: str = "low") -> dict:
        return self.request("POST", f"/v1/memory/profile/{_q(user_id)}/ask", json_body={"project": self.project, "query": query, "reasoning_level": reasoning_level}, timeout=8.0)
    def get_agent_model(self, agent_id: str) -> dict:
        return self.request("GET", f"/v1/memory/agent/{_q(agent_id)}/model", params={"project": self.project}, timeout=4.0)
    def seed_agent_identity(self, agent_id: str, content: str, source: str = "soul_md") -> dict:
        return self.request("POST", f"/v1/memory/agent/{_q(agent_id)}/seed", json_body={"project": self.project, "content": content, "source": source}, timeout=20.0)

    # File routes
    def upload_file(self, data: bytes, filename: str, remote_path: str, mime_type: str, scope: str, project_id: str | None) -> dict:
        import io
        fields = {"path": remote_path, "scope": scope.upper(), **({"project_id": project_id} if project_id else {})}
        return self._raw("POST", "/v1/files", files={"file": (filename, io.BytesIO(data), mime_type)}, data=fields).json()
    def list_files(self, prefix: str | None = None, limit: int = 50) -> dict:
        return self.request("GET", "/v1/files", params={"limit": limit, **({"prefix": prefix} if prefix else {})})
    def get_file(self, file_id: str) -> dict:
        return self.request("GET", f"/v1/files/{_q(file_id)}")
    def read_file_content(self, file_id: str) -> bytes:
        return self._raw("GET", f"/v1/files/{_q(file_id)}/content", allow_redirects=True).content
    def ingest_file(self, file_id: str, user_id: str | None = None, agent_id: str | None = None) -> dict:
        body = {k: v for k, v in (("user_id", user_id), ("agent_id", agent_id)) if v}
        return self.request("POST", f"/v1/files/{_q(file_id)}/ingest", json_body=body, timeout=60.0)
    def delete_file(self, file_id: str) -> dict:
        return self.request("DELETE", f"/v1/files/{_q(file_id)}", timeout=5.0)


class _WriteQueue:
    """SQLite-backed async write queue. Survives crashes — pending rows replay on startup."""

    def __init__(self, client: _Client, db_path: Path,
                 *, on_constructed: Callable[[_WriteQueue], None] | None = None):
        self._client, self._db_path, self._q = client, db_path, queue.Queue()
        self._thread: threading.Thread | None = None
        self._local = threading.local()  # one cached connection per thread, all tracked in _connections
        self._connections: set[sqlite3.Connection] = set()
        self._connections_lock, self._shutdown_lock, self._shutdown = threading.Lock(), threading.Lock(), False
        self._condition = threading.Condition()
        self._retiring = False
        self._retirement_stop_requested = False
        self._hard_released = False
        self._active_flushes = 0
        self._failed_rows: set[int] = set()
        # A reversible resume may be requested while one native ingest is still in flight.  Do
        # not put that row back into the queue until the current flush has settled; otherwise the
        # same durable row can be admitted twice.  The settled worker then replays every exact
        # failed row on this queue before accepting later writes.
        self._replay_failed_after_active = False
        # A writer launch can fail before a thread exists or after a wrapper has published an
        # unstarted thread. Keep the exact queue object and its SQLite handles so hard retirement
        # can report UNKNOWN and retry on the same source-owned queue.
        self._worker_start_failure: str | None = None
        # Publish before any filesystem, SQLite, or worker operation can fail. The owning provider
        # must retain this exact object and every native handle it acquired for cleanup/retry.
        if on_constructed is not None:
            on_constructed(self)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._execute("CREATE TABLE IF NOT EXISTS pending (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, "
                             "session_id TEXT, messages_json TEXT, created_at TEXT, last_error TEXT)").connection
        self._start_worker(name="retaindb-writer", raise_on_failure=True)
        replay = conn.execute("SELECT id, user_id, session_id, messages_json FROM pending ORDER BY id ASC LIMIT 200").fetchall()
        for row_id, user_id, session_id, msgs_json in replay:  # rows left from a previous crash
            self._q.put((row_id, user_id, session_id, json.loads(msgs_json)))

    def _start_worker(self, *, name: str, raise_on_failure: bool) -> threading.Thread | None:
        """Start one exact queue worker and classify pre-start versus ambiguous failures.

        ``Thread.start`` may raise after the native target has begun. In that case the target
        owns the queue admission and the caller must join/reconcile it from source state. A
        factory or pre-start failure has no worker custody, so the queue retains a structured
        failure and a later hard-retire attempt may retry on this same queue.
        """
        worker_started = threading.Event()
        thread: threading.Thread | None = None
        # A previous retry may have recorded a pre-start failure.  Every new launch is a
        # fresh source observation: clear that marker before attempting it, then restore it
        # only when this attempt is proven pre-start.  An ambiguous start must be reconciled
        # from the exact worker/row state rather than inheriting an old failure.
        self._worker_start_failure = None

        def _run_worker() -> None:
            worker_started.set()
            self._loop()

        try:
            thread = spawn_context_thread(_run_worker, name=name)
            self._thread = thread
            thread.start()
        except BaseException:
            launched = thread is not None and (
                worker_started.is_set() or getattr(thread, "ident", None) is not None
            )
            if not launched:
                self._worker_start_failure = "worker_start_failed"
            if raise_on_failure:
                raise
            return thread
        self._worker_start_failure = None
        return thread

    def _get_conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = self._local.conn = sqlite3.connect(str(self._db_path), timeout=30, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            with self._connections_lock:
                self._connections.add(conn)
        return conn

    def _execute(self, sql: str, params: tuple = ()) -> sqlite3.Cursor:
        cur = self._get_conn().execute(sql, params)
        cur.connection.commit()
        return cur

    def _close(self, *conns: sqlite3.Connection, strict: bool = False) -> int:
        failures = 0
        for conn in conns:
            try:
                conn.close()
            # health: allow BLE001 -- SQLite handles may expose driver-specific close failures; hard custody retains them
            except Exception:
                failures += 1
                if not strict:
                    with self._connections_lock:
                        self._connections.discard(conn)
                continue
            with self._connections_lock:
                self._connections.discard(conn)
            if getattr(self._local, "conn", None) is conn:
                self._local.conn = None
        return failures

    def _close_thread_conn(self, *, strict: bool = False) -> None:
        conn, self._local.conn = getattr(self._local, "conn", None), None
        self._close(*([conn] if conn is not None else []), strict=strict)

    def enqueue(self, user_id: str, session_id: str, messages: list) -> bool:
        now = datetime.now(UTC).isoformat()
        with self._shutdown_lock:
            if self._shutdown or self._retiring:
                return False
            cur = self._execute("INSERT INTO pending (user_id, session_id, messages_json, created_at) VALUES (?,?,?,?)",
                                (user_id, session_id, json.dumps(messages, ensure_ascii=False), now))
            self._q.put((cur.lastrowid, user_id, session_id, messages))
            return True

    def _flush_row(self, row_id: int, user_id: str, session_id: str, messages: list) -> None:
        try:
            self._client.ingest_session(user_id, session_id, messages)
            self._execute("DELETE FROM pending WHERE id = ?", (row_id,))
            with self._condition:
                self._failed_rows.discard(row_id)
        # health: allow BLE001 -- SQLite/JSON state inspection must fail closed during hard retirement
        except Exception as exc:
            logger.warning("RetainDB ingest failed (will retry): %s", exc)
            with self._condition:
                self._failed_rows.add(row_id)
            with suppress(Exception):
                self._execute("UPDATE pending SET last_error = ? WHERE id = ?", (str(exc), row_id))
            time.sleep(2)

    def _requeue_failed_rows_locked(self) -> bool:
        """Replay exact failed durable rows after an active flush fence has settled.

        The shutdown lock is held by callers.  A failed row has already been removed from the
        in-memory queue by ``Queue.get`` while its SQLite row remains durable.  Only those source
        row ids are requeued, so a current in-flight item or an unrelated accepted row is never
        duplicated.  A failed durable read leaves the rows and failure ids retained for a later
        same-queue retry.
        """
        if self._shutdown or self._hard_released:
            return False
        with self._condition:
            if self._active_flushes:
                return False
            failed_ids = set(self._failed_rows)
        if not failed_ids:
            return True
        rows = self._pending_rows()
        if rows is None:
            return False
        replay = [row for row in rows if row[0] in failed_ids]
        replay_ids = {row[0] for row in replay}
        for row in replay:
            self._q.put(row)
        with self._condition:
            self._failed_rows.difference_update(replay_ids)
        return True

    def _loop(self) -> None:
        try:
            while (item := self._q.get()) is not _ASYNC_SHUTDOWN:
                with self._condition:
                    self._active_flushes += 1
                replay_failed = False
                try:
                    self._flush_row(*item)
                except Exception as exc:
                    logger.error("RetainDB writer error: %s", exc)
                finally:
                    with self._condition:
                        self._active_flushes = max(0, self._active_flushes - 1)
                        replay_failed = self._replay_failed_after_active and self._active_flushes == 0
                        if replay_failed:
                            self._replay_failed_after_active = False
                        self._condition.notify_all()
                    if replay_failed:
                        with self._shutdown_lock:
                            if not self._retiring:
                                self._requeue_failed_rows_locked()
        finally:
            # Keep a failed native close in the shared census so provider shutdown can retry the
            # exact handle from the owner; dropping it here would fabricate a clean close.
            self._close_thread_conn(strict=True)  # sqlite3 connections must close on their owning thread
            with self._condition:
                self._condition.notify_all()

    def _pending_rows(self) -> list[tuple[int, str, str, list]] | None:
        try:
            rows = self._get_conn().execute(
                "SELECT id, user_id, session_id, messages_json FROM pending ORDER BY id ASC LIMIT 200"
            ).fetchall()
            return [(row_id, user_id, session_id, json.loads(messages_json))
                    for row_id, user_id, session_id, messages_json in rows]
        # health: allow BLE001 -- SQLite state inspection must fail closed during hard retirement
        except Exception as exc:
            logger.warning("RetainDB pending-row inspection failed: %s", exc)
            return None

    def _pending_count(self) -> int | None:
        try:
            return int(self._get_conn().execute("SELECT COUNT(*) FROM pending").fetchone()[0])
        # health: allow BLE001 -- optional RetainDB seed boundary must retain provider custody on any failure
        except Exception as exc:
            logger.warning("RetainDB pending-row count failed: %s", exc)
            return None

    def _connection_count(self) -> int:
        with self._connections_lock:
            return len(self._connections)

    @staticmethod
    def _thread_is_alive(thread: threading.Thread | None) -> bool:
        checker = getattr(thread, "is_alive", None)
        return bool(checker is not None and checker())

    def begin_retirement(self) -> None:
        """Close new queue admission while allowing already queued rows to drain."""
        with self._shutdown_lock:
            if self._shutdown or self._hard_released:
                return
            self._retiring = True
            if not self._retirement_stop_requested:
                self._q.put(_ASYNC_SHUTDOWN)
                self._retirement_stop_requested = True

    def prepare_retirement(self, timeout: float = _RETIREMENT_WAIT_S) -> dict[str, object]:
        """Fence queue admission and drain durable rows while retaining all handles open."""
        deadline = _monotonic() + max(0.0, timeout)
        with self._shutdown_lock:
            if self._hard_released:
                return {"status": "unknown", "pending_write_count": 0,
                        "pending_flush_count": 0, "close_failure_count": 0,
                        "active_store_count": 0, "reason": "released"}
            if self._shutdown:
                return {"status": "unknown", "pending_write_count": 0,
                        "pending_flush_count": 0, "close_failure_count": 1,
                        "active_store_count": max(1, self._connection_count()), "reason": "close_failed"}
            self._retiring = True
            thread = getattr(self, "_thread", None)
            if self._worker_start_failure and thread_is_closed(thread):
                self._worker_start_failure = None
            if not self._thread_is_alive(thread) and self._pending_count() not in (0, None):
                self._start_retry_worker_locked()
                thread = getattr(self, "_thread", None)
            launch_failure = self._worker_start_failure

        if launch_failure:
            pending = self._pending_count()
            return {
                "status": "unknown",
                "pending_write_count": pending if pending is not None else 0,
                "pending_flush_count": 1,
                "close_failure_count": 0,
                "active_store_count": max(1, self._connection_count()),
                "reason": launch_failure,
            }

        while True:
            pending = self._pending_count()
            with self._condition:
                active = self._active_flushes
            if pending is None:
                return {"status": "unknown", "pending_write_count": 0,
                        "pending_flush_count": max(1, active), "close_failure_count": 0,
                        "active_store_count": max(1, self._connection_count()), "reason": "pending_catch_up"}
            if pending == 0 and active == 0:
                return {"status": "prepared", "pending_write_count": 0,
                        "pending_flush_count": 0, "close_failure_count": 0,
                        "active_store_count": max(1, self._connection_count()), "reason": "prepared"}
            remaining = deadline - _monotonic()
            if remaining <= 0:
                return {"status": "unknown", "pending_write_count": pending,
                        "pending_flush_count": max(1, active), "close_failure_count": 0,
                        "active_store_count": max(1, self._connection_count()), "reason": "pending_catch_up"}
            thread = getattr(self, "_thread", None)
            if not self._thread_is_alive(thread) and pending:
                with self._shutdown_lock:
                    self._start_retry_worker_locked()
            with self._condition:
                self._condition.wait(min(0.05, remaining))

    def resume_admission(self) -> bool:
        """Reopen this exact queue after a reversible preparation fence."""
        with self._shutdown_lock:
            if self._shutdown or self._hard_released:
                return False
            thread = getattr(self, "_thread", None)
            thread_was_alive = self._thread_is_alive(thread)
            active = 0
            if not thread_was_alive:
                rows = self._pending_rows()
                if rows is None:
                    return False
                self._q = queue.Queue()
                for row in rows:
                    self._q.put(row)
                self._retirement_stop_requested = False
                self._start_worker(name="retaindb-writer-resume", raise_on_failure=False)
                if self._worker_start_failure:
                    return False
            else:
                with self._condition:
                    active = self._active_flushes
                if active:
                    # The current native call remains the sole owner of its queue item.  Its
                    # completion path will replay any failed durable rows after decrementing the
                    # active count, so resume never duplicates an in-flight row.
                    self._replay_failed_after_active = True
            self._retiring = False
            if thread_was_alive and not active:
                if not self._requeue_failed_rows_locked():
                    self._retiring = True
                    return False
            self._retirement_stop_requested = False
            return True

    def _start_retry_worker_locked(self) -> None:
        """Replay durable rows on the same queue object after an unresolved attempt."""
        if self._shutdown or self._hard_released:
            return
        thread = getattr(self, "_thread", None)
        if self._thread_is_alive(thread):
            return
        rows = self._pending_rows()
        if rows is None or not rows:
            self._retirement_stop_requested = False
            return
        self._q = queue.Queue()
        for row in rows:
            self._q.put(row)
        self._retirement_stop_requested = False
        self._start_worker(name="retaindb-writer-retry", raise_on_failure=False)

    def hard_retire(self, timeout: float = _RETIREMENT_WAIT_S) -> dict[str, object]:
        """Drain this exact queue and close handles only after durable work is settled."""
        with self._shutdown_lock:
            if self._hard_released:
                return {"status": "released", "pending_write_count": 0,
                        "pending_flush_count": 0, "close_failure_count": 0,
                        "active_store_count": 0, "reason": "released"}
            if self._shutdown:
                return {"status": "unknown", "pending_write_count": 0,
                        "pending_flush_count": 0, "close_failure_count": 1,
                        "active_store_count": max(1, self._connection_count()), "reason": "close_failed"}
            self._retiring = True
            thread = getattr(self, "_thread", None)
            if not self._thread_is_alive(thread):
                self._start_retry_worker_locked()
                thread = getattr(self, "_thread", None)
            launch_failure = self._worker_start_failure
            if self._thread_is_alive(thread) and not self._retirement_stop_requested:
                self._q.put(_ASYNC_SHUTDOWN)
                self._retirement_stop_requested = True

        if launch_failure:
            pending = self._pending_count()
            return {
                "status": "unknown",
                "pending_write_count": pending if pending is not None else 0,
                "pending_flush_count": 1,
                "close_failure_count": 0,
                "active_store_count": max(1, self._connection_count()),
                "reason": launch_failure,
            }

        if self._thread_is_alive(thread):
            thread.join(timeout=max(0.0, timeout))
            if self._thread_is_alive(thread):
                pending = self._pending_count()
                return {
                    "status": "unknown",
                    "pending_write_count": pending if pending is not None else 0,
                    "pending_flush_count": max(1, self._active_flushes),
                    "close_failure_count": 0,
                    "active_store_count": max(1, self._connection_count()),
                    "reason": "pending_flush",
                }

        pending = self._pending_count()
        if pending is None:
            return {"status": "unknown", "pending_write_count": 0,
                    "pending_flush_count": 0, "close_failure_count": 0,
                    "active_store_count": max(1, self._connection_count()), "reason": "pending_catch_up"}
        if pending:
            with self._shutdown_lock:
                self._retirement_stop_requested = False
            return {"status": "unknown", "pending_write_count": pending,
                    "pending_flush_count": 0, "close_failure_count": 0,
                    "active_store_count": max(1, self._connection_count()), "reason": "pending_catch_up"}

        with self._connections_lock:
            connections = list(self._connections)
        close_failures = self._close(*connections, strict=True)
        if close_failures:
            return {"status": "unknown", "pending_write_count": 0,
                    "pending_flush_count": 0, "close_failure_count": close_failures,
                    "active_store_count": max(1, self._connection_count()), "reason": "close_failed"}

        with self._shutdown_lock:
            self._hard_released = True
            self._shutdown = True
        return {"status": "released", "pending_write_count": 0,
                "pending_flush_count": 0, "close_failure_count": 0,
                "active_store_count": 0, "reason": "released"}

    def shutdown(self) -> bool:
        with self._shutdown_lock:
            if self._hard_released:
                return True
            if self._retiring and not self._hard_released:
                return False
            if not self._shutdown:
                self._shutdown = True
                self._q.put(_ASYNC_SHUTDOWN)
        # Keep a caller-owned handle in the exact queue when native close fails.  Dropping it
        # from the census would let a failed initializer or ambiguous worker look closed.
        self._close_thread_conn(strict=True)  # caller thread owns the connection opened in __init__
        thread = self._thread
        if thread is not None and thread.ident is not None:
            thread.join(timeout=10)
        if self._thread_is_alive(thread):
            # The sentinel was accepted, but the native client may still be in flight.  Keep
            # the exact queue and every handle for the next bounded cleanup attempt.
            return False
        if thread is None or thread.ident is None or not thread.is_alive():
            # A constructor/start failure may leave a published partial queue with no started
            # worker. Its exact connection set still belongs to this queue and must be closed here.
            with self._connections_lock:
                stragglers = list(self._connections)
            self._close(*stragglers, strict=True)
        return self._connection_count() == 0


def _compact(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()[:320]


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9 ]", "", _compact(s).lower())


def _build_overlay(profile: dict, query_result: dict, local_entries: list[str] | None = None) -> str:
    """Profile + query memories (5 each, compacted, deduped against each other and *local_entries*)."""
    seen = {_norm(e) for e in (local_entries or []) if _norm(e)}

    def _dedupe(items) -> list[str]:
        out: list[str] = []
        for m in list(items or [])[:5]:
            c = _compact((m or {}).get("content") or "")
            if c and _norm(c) not in seen:
                seen.add(_norm(c))
                out.append(c)
        return out

    profile_items, query_items = _dedupe((profile or {}).get("memories")), _dedupe((query_result or {}).get("results"))
    if not profile_items and not query_items:
        return ""
    return "\n".join(["[RetainDB Context]", "Profile:"] + ([f"- {i}" for i in profile_items] or ["- None"])
                     + ["Relevant memories:"] + ([f"- {i}" for i in query_items] or ["- None"]))


# Agent self-model keys -> prefetch line formatter, in display order.
_AGENT_MODEL_FIELDS = (
    ("persona", lambda v: f"Persona: {v}"),
    ("persistent_instructions", lambda v: "Instructions:\n" + "\n".join(f"- {i}" for i in v)),
    ("working_style", lambda v: f"Working style: {v}"),
)


class RetainDBMemoryProvider(MemoryProvider):
    """RetainDB cloud memory — durable queue, semantic search, dialectic synthesis, shared files."""

    def __init__(self):
        self._client: _Client | None = None
        self._queue: _WriteQueue | None = None
        self._user_id, self._session_id, self._agent_id = "default", "", "hermes"
        self._lock = threading.Lock()  # guards the prefetch caches below
        self._context_result, self._dialectic_result, self._agent_model = "", "", {}
        self._prefetch_threads: list[threading.Thread] = []  # tracked so rapid turns don't pile up threads
        self._soul_thread: threading.Thread | None = None
        self._retirement_condition = threading.Condition()
        self._retirement_mutex = threading.Lock()
        self._retirement_identity = ProviderIdentity("", "", "")
        self._retirement_active_calls = 0
        self._retirement_threads: set[threading.Thread] = set()
        self._retirement_failures = 0
        self._retiring = False
        self._released = False
        self._retirement_in_progress = False
        self._retirement_initializing = False
        self._retirement_initialization_failed = False
        self._retirement_lease: ProviderRetirementLease | None = None

    @property
    def name(self) -> str:
        return "retaindb"

    def is_available(self) -> bool:
        return bool(get_secret("RETAINDB_API_KEY"))

    def get_config_schema(self) -> list[dict[str, Any]]:
        return [
            {"key": "api_key", "description": "RetainDB API key", "secret": True, "required": True, "env_var": "RETAINDB_API_KEY", "url": "https://retaindb.com"},
            {"key": "base_url", "description": "API endpoint", "default": _DEFAULT_BASE_URL},
            {"key": "project", "description": "Project identifier (optional — uses 'default' project if not set)", "default": ""},
        ]

    def initialize(self, session_id: str, **kwargs) -> None:
        # Non-secret fields resolve env (profile-scoped) -> config.yaml (written by the Dashboard) -> default.
        cfg = {k: v.strip() for k, v in _load_retaindb_config().items() if isinstance(v, str)}
        base_url = re.sub(r"/+$", "", get_secret("RETAINDB_BASE_URL", "") or cfg.get("base_url") or _DEFAULT_BASE_URL)
        # Project: RETAINDB_PROJECT > config.yaml > hermes-<profile> > "default" (API auto-creates "default").
        # The project is the data partition: read through the secret scope so a multiplexed secondary's
        # memories never land in the default profile's project.
        project = get_secret("RETAINDB_PROJECT", "") or cfg.get("project")
        if not project:
            profile_name = os.path.basename(str(kwargs.get("hermes_home", "")))
            project = f"hermes-{profile_name}" if profile_name not in {"", ".hermes"} else "default"
        # Bind to the profile the manager names, not the ambient one (multiplexing contract, MemoryProvider).
        from hermes_constants import get_hermes_home
        home = Path(kwargs["hermes_home"]) if kwargs.get("hermes_home") else get_hermes_home()
        with self._retirement_condition:
            if (self._retirement_active_calls or self._retirement_in_progress or self._retiring or self._released
                    or self._client is not None or self._queue is not None):
                raise RuntimeError("RetainDB cannot be reinitialized while its provider state is owned")
            self._retirement_identity = new_identity("retaindb", home)
            # Keep the source-owned incarnation visible for custody while client, SQLite queue,
            # and optional SOUL startup work are still being constructed.
            self._retirement_initializing = True
            self._retirement_active_calls += 1
            self._retirement_failures = 0
            self._retirement_initialization_failed = False
        try:
            self._client = _Client(get_secret("RETAINDB_API_KEY", "") or "", base_url, project)
            self._session_id, self._user_id = session_id, kwargs.get("user_id", "default") or "default"
            self._agent_id = kwargs.get("agent_id", "hermes") or "hermes"
            def _publish_queue(queue_obj: _WriteQueue) -> None:
                with self._retirement_condition:
                    self._queue = queue_obj

            self._queue = _WriteQueue(
                self._client, home / "retaindb_queue.db", on_constructed=_publish_queue,
            )
            soul = (home / "SOUL.md").read_text(encoding="utf-8-sig", errors="replace").strip() if (home / "SOUL.md").exists() else ""
            if soul:  # seed agent identity from SOUL.md in background
                seed = lambda: self._client.seed_agent_identity(self._agent_id, soul, source="soul_md")
                self._soul_thread = self._spawn_provider_thread(
                    lambda: self._run_seed(seed), name="retaindb-soul-seed",
                )
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

    def _run_seed(self, seed: Callable[[], Any]) -> bool:
        try:
            result = seed()
            return result is not None
        # health: allow BLE001 -- optional RetainDB seed boundary must retain provider custody on any failure
        except Exception as exc:
            logger.debug("RetainDB soul seed failed: %s", exc)
            return False

    def _spawn_provider_thread(self, target: Callable[[], Any], *, name: str) -> threading.Thread | None:
        """Admit a provider-owned worker before starting it so retirement cannot miss the race."""
        with self._retirement_condition:
            # A worker created by an initializer is part of that already-admitted startup
            # operation. Once startup has begun, retirement may close new external admission
            # but must not silently skip the initializer's own SOUL seed.
            if (self._retiring and not self._retirement_initializing) or self._released:
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

        thread: threading.Thread | None = None

        def _work() -> None:
            failed = False
            try:
                worker_started.set()
                failed = target() is False
            # health: allow BLE001 -- provider worker boundary must retain accepted worker custody on any failure
            except Exception as exc:
                failed = True
                logger.debug("RetainDB worker %s failed: %s", name, exc)
            finally:
                _finish_once(failed=failed, thread=thread if thread is not None else threading.current_thread())

        try:
            thread = spawn_context_thread(_work, name=name)
            with self._retirement_condition:
                self._retirement_threads.add(thread)
            thread.start()
        except BaseException:
            # A custom start wrapper may raise after launching the target. In that ambiguous
            # case the worker's finally owns the lease; only an unstarted object is released here.
            if thread is None or (not worker_started.is_set() and getattr(thread, "ident", None) is None):
                _finish_once(failed=True, thread=thread)
            raise
        return thread

    def _join_retirement_threads(self, timeout: float) -> bool:
        """Join every admitted provider worker within one bounded custody window.

        Initialization can admit a SOUL seed before the provider has a usable queue, and
        prefetch workers can be admitted after the queue exists.  Both workers retain the
        provider's native client and must settle before shutdown drops that client.  A worker
        that is still live, or whose finish callback has not released its lease, keeps the
        provider reachable for a later cleanup attempt.
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
                    # An unstarted or otherwise unusable worker remains admitted and therefore
                    # cannot be treated as settled by dropping the provider client.
                    logger.warning("RetainDB provider worker join failed: %s", exc)
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

    def system_prompt_block(self) -> str:
        project = self._client.project if self._client else "retaindb"
        return (f"# RetainDB Memory\nActive. Project: {project}.\nUse retaindb_search to find memories, retaindb_remember to store facts, "
                "retaindb_profile for a user overview, retaindb_context for current-task context.")

    def queue_prefetch(self, query: str, *, session_id: str = "") -> None:
        """Fire context + dialectic + agent model prefetches in background (turn-end); prefetch() consumes them next turn."""
        with self._retirement_condition:
            if self._retiring or self._released or not self._client:
                return
        if not self._client:
            return
        for t in self._prefetch_threads:  # wait for the previous batch so threads don't accumulate on rapid turns
            t.join(timeout=2.0)
        if any(t.is_alive() for t in self._prefetch_threads):
            logger.debug("RetainDB prefetch still running; skipping new batch")
            return
        jobs = (  # (thread name, log label, cache attr, fetch) — fetch returns None to leave the cache untouched
            ("retaindb-ctx", "context", "_context_result", lambda: self._context_overlay(query)["context"]),
            ("retaindb-dialectic", "dialectic", "_dialectic_result", lambda: str(
                self._client.ask_user(self._user_id, query, reasoning_level=self._reasoning_level(query)).get("answer") or "") or None),
            ("retaindb-agent-model", "agent model", "_agent_model", lambda: self._agent_model_or_none(self._client.get_agent_model(self._agent_id))),
        )
        threads = []
        for name, label, attr, fetch in jobs:
            thread = self._spawn_provider_thread(
                lambda label=label, attr=attr, fetch=fetch: self._store(label, attr, fetch),
                name=name,
            )
            if thread is not None:
                threads.append(thread)
        self._prefetch_threads = threads

    def _context_overlay(self, query: str) -> dict:
        query_result = self._client.query_context(self._user_id, self._session_id, query)
        return {"context": _build_overlay(self._client.get_profile(self._user_id), query_result), "raw": query_result}

    @staticmethod
    def _agent_model_or_none(model: dict) -> dict | None:
        return model if model.get("memory_count", 0) > 0 else None

    def _store(self, label: str, attr: str, fetch: Callable[[], Any]) -> bool:
        """Run one prefetch job; cache its value under the lock unless None (failures log at debug)."""
        try:
            value = fetch()
        # health: allow BLE001 -- optional RetainDB prefetch boundary must retain provider custody on any failure
        except Exception as exc:
            logger.debug("RetainDB %s prefetch failed: %s", label, exc)
            return False
        if value is not None:
            with self._lock:
                setattr(self, attr, value)
        return True

    @staticmethod
    def _reasoning_level(query: str) -> str:
        return "low" if len(query) < 120 else "medium" if len(query) < 400 else "high"

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        """Consume prefetched results and return them as a context block."""
        with self._lock:
            context, dialectic, agent_model = self._context_result, self._dialectic_result, self._agent_model
            self._context_result, self._dialectic_result, self._agent_model = "", "", {}
        model_lines = [fmt(agent_model[k]) for k, fmt in _AGENT_MODEL_FIELDS if agent_model.get(k)] if agent_model.get("memory_count", 0) > 0 else []
        parts = [context, dialectic and f"[RetainDB User Synthesis]\n{dialectic}",
                 model_lines and "[RetainDB Agent Self-Model]\n" + "\n".join(model_lines)]
        return "\n\n".join(p for p in parts if p)

    def sync_turn(self, user_content: str, assistant_content: str, *, session_id: str = "") -> None:
        """Queue turn for async ingest. Returns immediately."""
        if not self._queue or not user_content or not self._begin_call():
            return
        now = datetime.now(UTC).isoformat()
        failed = False
        try:
            self._queue.enqueue(self._user_id, session_id or self._session_id,
                                [{"role": "user", "content": user_content, "timestamp": now},
                                 {"role": "assistant", "content": assistant_content, "timestamp": now}])
        except Exception:
            failed = True
            raise
        finally:
            self._finish_call(failed=failed)

    def get_tool_schemas(self) -> list[dict[str, Any]]:
        return list(_SCHEMAS)

    def handle_tool_call(self, tool_name: str, args: dict, **kwargs) -> str:
        if not self._client:
            return tool_error("RetainDB not initialized")
        if not self._begin_call():
            return tool_error("RetainDB retirement is in progress")
        failed = False
        try:
            return json.dumps(self._dispatch(tool_name, args))
        # health: allow BLE001 -- optional RetainDB write boundary must retain provider custody on any failure
        except Exception as exc:
            failed = True
            return tool_error(str(exc))
        finally:
            self._finish_call(failed=failed)

    def _dispatch(self, tool_name: str, args: dict) -> Any:
        required, handler = _TOOLS.get(tool_name, (None, None))
        if handler is None:
            return {"error": f"Unknown tool: {tool_name}"}
        value = args.get(required, "") if required else None
        return {"error": f"{required} is required"} if required and not value else handler(self, args, value)

    def prepare_retirement(self, receipt: dict[str, str] | None = None) -> dict[str, object]:
        """Reversibly fence this provider after workers and durable rows have settled."""
        with self._retirement_condition:
            current = self._retirement_identity
        requested, reason = receipt_reason(receipt, current)
        if reason is not None:
            return preparation_result(current, status="unknown", lease=None, reason=reason)

        if not self._retirement_mutex.acquire(blocking=False):
            with self._retirement_condition:
                lease = self._retirement_lease
                current = self._retirement_identity
            return preparation_result(current, status="unknown", lease=lease, reason="retirement_in_progress")
        lease: ProviderRetirementLease | None = None
        try:
            with self._retirement_condition:
                current = self._retirement_identity
                if current != requested:
                    return preparation_result(current, status="unknown", lease=None, reason="stale_receipt")
                if self._retirement_initialization_failed:
                    return preparation_result(current, status="unknown", lease=None, reason="store_open_failed")
                if self._released:
                    return preparation_result(current, status="unknown", lease=self._retirement_lease, reason="released")
                lease = self._retirement_lease
                if lease is None:
                    lease = ProviderRetirementLease(current)
                    self._retirement_lease = lease
                elif lease._resumed:
                    return preparation_result(current, status="unknown", lease=None, reason="stale_lease")
                elif lease._commit_started:
                    return preparation_result(current, status="unknown", lease=lease, reason="commit_started")
                self._retiring = True
                self._retirement_in_progress = True
                deadline = _monotonic() + _RETIREMENT_WAIT_S
                while self._retirement_active_calls:
                    remaining = deadline - _monotonic()
                    if remaining <= 0:
                        return preparation_result(current, status="unknown", lease=lease, reason="pending_flush")
                    self._retirement_condition.wait(remaining)
                queue_obj = self._queue
            if queue_obj is None:
                return preparation_result(current, status="unknown", lease=lease, reason="not_initialized")
            remaining = max(0.0, deadline - _monotonic())
            if not self._join_retirement_threads(remaining):
                return preparation_result(current, status="unknown", lease=lease, reason="pending_flush")
            with self._retirement_condition:
                failures = self._retirement_failures
            if failures:
                return preparation_result(current, status="unknown", lease=lease, reason="flush_failed")
            queue_report = queue_obj.prepare_retirement(timeout=max(0.0, deadline - _monotonic()))
            if queue_report["status"] != "prepared":
                return preparation_result(current, status="unknown", lease=lease, reason=str(queue_report["reason"]))
            with self._retirement_condition:
                lease._prepared = True
            return preparation_result(current, status="prepared", lease=lease, reason="prepared")
        finally:
            with self._retirement_condition:
                self._retirement_in_progress = False
                self._retirement_condition.notify_all()
            self._retirement_mutex.release()

    def resume_retirement(self, lease: object) -> bool:
        """Reopen admission for the exact uncommitted provider lease and queue object."""
        with self._retirement_condition:
            current = self._retirement_identity
            queue_obj = self._queue
            if (
                not isinstance(lease, ProviderRetirementLease)
                or self._retirement_lease is not lease
                or lease._identity != current
                or lease._commit_started
                or lease._resumed
                or self._released
                or self._retirement_in_progress
                or queue_obj is None
            ):
                return False
            if not queue_obj.resume_admission():
                return False
            self._retiring = False
            lease._resumed = True
            self._retirement_lease = None
            self._retirement_condition.notify_all()
            return True

    def commit_retirement(self, lease: object) -> dict[str, object]:
        """Begin irreversible queue/client retirement only for the exact prepared lease."""
        with self._retirement_condition:
            current = self._retirement_identity
            if (
                not isinstance(lease, ProviderRetirementLease)
                or self._retirement_lease is not lease
                or lease._identity != current
                or lease._resumed
            ):
                return retirement_result(current, status="unknown", reason="foreign_receipt")
            if not lease._prepared and not lease._commit_started:
                return retirement_result(current, status="unknown", reason="pending_flush")
            lease._commit_started = True
        return self._retire_exact_lease(lease)

    def _retire_exact_lease(self, lease: ProviderRetirementLease) -> dict[str, object]:
        """Run the irreversible path only after the exact opaque lease was admitted."""
        with self._retirement_condition:
            current = self._retirement_identity
            if (
                self._retirement_lease is not lease
                or lease._identity != current
                or lease._resumed
                or not lease._commit_started
            ):
                return retirement_result(current, status="unknown", reason="foreign_receipt")
            receipt = current.as_mapping()
        return self._retire_impl(receipt, lease=lease)

    def _tool_upload_file(self, args: dict, local_path: str) -> Any:
        path_obj = Path(local_path)
        if not path_obj.exists():
            return {"error": f"File not found: {local_path}"}
        try:
            raise_if_read_blocked(str(path_obj))
        except ValueError as exc:
            return {"error": str(exc)}
        import mimetypes
        result = self._client.upload_file(path_obj.read_bytes(), path_obj.name, args.get("remote_path") or f"/{path_obj.name}",
                                          mimetypes.guess_type(path_obj.name)[0] or "application/octet-stream", args.get("scope", "PROJECT"), None)
        if args.get("ingest") and result.get("file", {}).get("id"):
            result["ingest"] = self._ingest(result["file"]["id"])
        return result

    def _tool_read_file(self, args: dict, file_id: str) -> Any:
        file_info = self._client.get_file(file_id).get("file") or {}
        raw = self._client.read_file_content(file_id)
        out = {"file_id": file_id, "rdb_uri": file_info.get("rdb_uri"), "name": file_info.get("name")}
        if not ((file_info.get("mime_type") or "").lower().startswith("text/") or file_info.get("name", "").endswith(_TEXT_EXTS)):
            return {**out, "content": None, "note": "Binary file — use retaindb_ingest_file to extract text into memory."}
        text = raw.decode("utf-8", errors="replace")
        return {**out, "content": text[:32000], "truncated": len(text) > 32000}

    def _ingest(self, file_id: str) -> Any:
        return self._client.ingest_file(file_id, user_id=self._user_id, agent_id=self._agent_id)

    def on_memory_write(self, action: str, target: str, content: str) -> None:
        """Mirror built-in memory writes to RetainDB."""
        if action != "add" or not content or not self._client or not self._begin_call():
            return
        failed = False
        try:
            self._client.add_memory(
                self._user_id, self._session_id, content,
                memory_type="preference" if target == "user" else "factual",
            )
        # health: allow BLE001 -- optional RetainDB write boundary must retain provider custody on any failure
        except Exception as exc:
            failed = True
            logger.debug("RetainDB memory mirror failed: %s", exc)
        finally:
            self._finish_call(failed=failed)

    def retire(self, receipt: dict[str, str] | None = None) -> dict[str, object]:
        """Drain accepted RetainDB workers and durable rows on this exact provider instance."""
        return self._retire_impl(receipt)

    def _retire_impl(
        self,
        receipt: dict[str, str] | None = None,
        *,
        lease: ProviderRetirementLease | None = None,
    ) -> dict[str, object]:
        """Run receipt retirement, with the lease-only hard path kept private."""
        with self._retirement_condition:
            current = self._retirement_identity
        requested, reason = receipt_reason(receipt, current)
        if reason is not None:
            return retirement_result(requested, status="unknown", reason=reason)

        if not self._retirement_mutex.acquire(blocking=False):
            return retirement_result(current, status="unknown", reason="retirement_in_progress")
        try:
            with self._retirement_condition:
                # Initialization may replace the provider identity while the first receipt
                # check is outside this mutex. Revalidate under the fence before touching
                # _retiring, the queue, or the client for this attempt.
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
                if self._retirement_lease is not None:
                    if lease is None:
                        return retirement_result(current, status="unknown", reason="retirement_in_progress")
                    if (
                        self._retirement_lease is not lease
                        or lease._identity != current
                        or lease._resumed
                        or not lease._commit_started
                    ):
                        return retirement_result(current, status="unknown", reason="foreign_receipt")
                self._retiring = True
                self._retirement_in_progress = True
                deadline = time.monotonic() + _RETIREMENT_WAIT_S
                queue_obj = None
                while self._retirement_active_calls:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return retirement_result(
                            current, status="unknown", reason="pending_flush",
                            pending_flush_count=self._retirement_active_calls,
                        )
                    self._retirement_condition.wait(remaining)
                queue_obj = self._queue
            if queue_obj is None:
                return retirement_result(current, status="unknown", reason="not_initialized")
            remaining = max(0.0, deadline - time.monotonic())
            if not self._join_retirement_threads(remaining):
                with self._retirement_condition:
                    pending = max(1, len(self._retirement_threads))
                return retirement_result(
                    current, status="unknown", reason="pending_flush",
                    pending_flush_count=pending,
                )
            queue_obj.begin_retirement()

            with self._retirement_condition:
                remaining = max(0.0, deadline - time.monotonic())
                failures = self._retirement_failures

            queue_report = queue_obj.hard_retire(timeout=remaining)
            if queue_report["status"] != "released":
                return retirement_result(
                    current, status="unknown", reason=str(queue_report["reason"]),
                    active_store_count=int(queue_report.get("active_store_count", 0)),
                    pending_write_count=int(queue_report["pending_write_count"]),
                    pending_flush_count=int(queue_report["pending_flush_count"]),
                    close_failure_count=int(queue_report["close_failure_count"]),
                )
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
        with self._retirement_condition:
            if self._retirement_lease is not None and not self._retirement_lease._commit_started:
                logger.warning("RetainDB shutdown deferred while reversible retirement lease is active")
                return
            if self._retiring and not self._released:
                return
            queue_obj = self._queue
        if not self._join_retirement_threads(_RETIREMENT_WAIT_S):
            logger.warning("RetainDB provider shutdown remains unresolved; workers retained")
            return
        if queue_obj is not None and not queue_obj.shutdown():
            logger.warning("RetainDB provider shutdown remains unresolved; queue retained")
            return
        with self._retirement_condition:
            # The exact queue/client remain source-owned until every admitted worker and native
            # queue handle reports settled.  Only this successful close path clears them.
            if self._retirement_threads:
                return
            self._prefetch_threads = []
            self._soul_thread = None
            if self._queue is queue_obj:
                self._queue = None
                self._client = None


# tool name -> (required arg or None, handler(provider, args, required_value)); missing arg -> "<arg> is required"
_TOOLS: dict[str, tuple[str | None, Callable[..., Any]]] = {
    "retaindb_profile": (None, lambda p, a, _: p._client.get_profile(p._user_id)),
    "retaindb_search": ("query", lambda p, a, q: p._client.search(p._user_id, p._session_id, q, top_k=min(int(a.get("top_k", 8)), 20))),
    "retaindb_context": ("query", lambda p, a, q: p._context_overlay(q)),
    "retaindb_remember": ("content", lambda p, a, c: p._client.add_memory(
        p._user_id, p._session_id, c, memory_type=a.get("memory_type", "factual"), importance=float(a.get("importance", 0.7)))),
    "retaindb_forget": ("memory_id", lambda p, a, m: p._client.delete_memory(m)),
    "retaindb_upload_file": ("local_path", RetainDBMemoryProvider._tool_upload_file),
    "retaindb_list_files": (None, lambda p, a, _: p._client.list_files(prefix=a.get("prefix"), limit=int(a.get("limit", 50)))),
    "retaindb_read_file": ("file_id", RetainDBMemoryProvider._tool_read_file),
    "retaindb_ingest_file": ("file_id", lambda p, a, f: p._ingest(f)),
    "retaindb_delete_file": ("file_id", lambda p, a, f: p._client.delete_file(f)),
}


def register(ctx) -> None:
    """Register RetainDB as a memory provider plugin."""
    ctx.register_memory_provider(RetainDBMemoryProvider())
