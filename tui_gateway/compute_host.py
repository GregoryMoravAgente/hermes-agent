"""Persistent dashboard compute-host child: owns live AIAgent objects when
``dashboard.turn_isolation`` is enabled; frames are line-JSON over stdin/stdout."""

from __future__ import annotations

# First, like every entry point: stdio, import-path and environ-lifetime fixes (hermes_bootstrap).
# Only as ``python -m``: tests import this module, and the bootstrap's TMPDIR/scratch exports
# must not fire in a library importer.
if __name__ == "__main__":
    import hermes_bootstrap

import argparse
import concurrent.futures
import contextlib
import hashlib
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Collection

from tui_gateway.host_supervisor import MUTATOR_ROUTE_TABLE, _build_sha
from tui_gateway.windows_producer_identity import build_producer_startup_record


def now_ns() -> int:
    return time.perf_counter_ns()


class _HostTransport:
    def __init__(self, emit: Callable[[dict[str, Any]], None]) -> None:
        self._emit = emit

    def write(self, obj: dict) -> bool:
        sid = ""
        with contextlib.suppress(Exception):
            if obj.get("method") == "event":
                sid = str(((obj.get("params") or {}).get("session_id")) or "")
        self._emit({"type": "rpc", "sid": sid, "message": obj})
        return True

    def close(self) -> None:
        return None


# Slice of ``ComputeHost.shutdown``'s budget held back for the post-drain finalize: the
# supervisor SIGKILLs the host 10s (= default ``wait``) after SIGTERM, so a drain that ate
# the whole budget would leave the flush racing that kill and persist nothing.
_FLUSH_RESERVE_SECS = 1.0

# Fallback control.error text when a routed server method returns an error without a message.
_CONTROL_FAILURES = {
    "session.save": "session save failed", "session.compress": "session compression failed"}
_PRODUCER_COMPLETION_ENV = "HERMES_COMPUTE_HOST_PRODUCER_COMPLETION_FD"
_PRODUCER_COMPLETION_TYPE = "producer.complete"
_PRODUCER_COMPLETION_SCHEMA = 1
_PRODUCER_COMPLETION_MAX_BYTES = 4096
_PRODUCER_COMPLETION_KEYS = frozenset({
    "type", "schema", "host_pid", "host_boot_id", "sid", "request_id", "turn_id",
    "session_key", "origin_session_key", "profile_home", "terminal_sha256",
})
_EXCEPTION_PRODUCER_JOIN_SECS = 1.0


def _terminal_digest(terminal: dict[str, Any]) -> str:
    """Digest the complete finalized terminal wire object with stable key ordering."""
    encoded = json.dumps(
        terminal, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_completion_bytes(
    fd: int, data: bytes, *, write_fn: Callable[[int, Any], int] | None = None,
) -> None:
    """Write one bounded completion record, refusing short/zero/non-progress writes."""
    writer = write_fn or os.write
    view = memoryview(data)
    while view:
        written = writer(fd, view)
        if type(written) is not int or written <= 0 or written > len(view):
            raise OSError("producer completion pipe made no progress")
        view = view[written:]


class ComputeHost:
    # frame ``type`` -> handler method name (resolved per call so monkeypatches take effect).
    _FRAME_HANDLERS: dict[str, str] = {
        "turn.start": "_handle_turn_start", "interrupt": "_handle_interrupt",
        "respond": "_handle_respond", "reload_mcp": "_handle_reload_mcp",
        "control": "_handle_control", "shutdown": "_handle_shutdown"}

    def __init__(
        self, *, stdout: Any = None, max_workers: int | None = None,
        heartbeat_secs: float | None = None, completion_fd: int | None = None) -> None:
        self._stdout = stdout or sys.stdout
        self._write_lock = threading.Lock()
        self._completion_fd = completion_fd
        self._completion_lock = threading.Lock()
        self._completion_close_requested = threading.Event()
        self._completion_closed = threading.Event()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers or _default_workers(), thread_name_prefix="compute-host-turn")
        self._closed = threading.Event()
        self._shutdown_lock = threading.RLock()
        self._shutdown_requested = threading.Event()
        self._shutdown_complete = threading.Event()
        # A parsed turn can already be inside the admission handler when a
        # signal/orphan shutdown wins the lifecycle lock. Keep that handler alive
        # until its typed refusal and source-bound receipt have been published.
        self._turn_admission_condition = threading.Condition()
        self._turn_admissions = 0
        self._turn_admission_closed = False
        self._turn_admission_expired = False
        self._parent_pid = os.getppid()
        self._boot_id = uuid.uuid4().hex
        self._progress_counter = 0
        self._progress_lock = threading.Lock()
        # Future -> the ``sid`` whose turn it runs; ``shutdown`` leaves live sids unfinalized.
        self._turn_futures: dict[concurrent.futures.Future, str] = {}
        self._turn_futures_lock = threading.Lock()
        self._transport = _HostTransport(self.emit)
        self._heartbeat_secs = (
            float(heartbeat_secs) if heartbeat_secs is not None
            else float(os.environ.get("HERMES_COMPUTE_HOST_HEARTBEAT_SECS") or "15"))
        if self._heartbeat_secs > 0:
            for target, name in (
                (self._heartbeat_loop, "compute-host-heartbeat"),
                (self._parent_guard_loop, "compute-host-ppid-guard")):
                threading.Thread(target=target, name=name, daemon=True).start()

    def emit(self, frame: dict[str, Any]) -> None:
        frame.setdefault("host_ns", now_ns())
        data = json.dumps(frame, separators=(",", ":"), ensure_ascii=False)
        with self._write_lock:
            print(data, file=self._stdout, flush=True)

    def _reply(self, kind: str, sid: str, request_id: Any, **extra: Any) -> None:
        """Emit a per-session frame keyed by the request it answers."""
        self.emit({"type": kind, "sid": sid, "request_id": request_id,
                   "host_boot_id": self._boot_id, **extra})

    def _reply_and_complete(
        self, kind: str, sid: str, request_id: Any, **extra: Any,
    ) -> None:
        """Emit a terminal frame, then mark the joined producer boundary."""
        self._reply_terminal(kind, sid, request_id, producer_complete=True, **extra)

    def _reply_terminal(
        self, kind: str, sid: str, request_id: Any, *, producer_complete: bool,
        **extra: Any,
    ) -> None:
        """Emit one finalized terminal object and optionally publish its producer receipt."""
        terminal = {
            "type": kind, "sid": sid, "request_id": request_id,
            "host_boot_id": self._boot_id, **extra,
        }
        terminal.setdefault("host_ns", now_ns())
        self.emit(terminal)
        if producer_complete:
            self._emit_producer_completion(terminal)

    def _emit_producer_completion(self, terminal: dict[str, Any]) -> None:
        """Record the real producer boundary after terminal-frame finalization.

        This channel is a child-owned control handle passed only to this
        ComputeHost. It is closed before tool/descendant creation, so a late
        inherited stdout writer cannot forge this receipt. A failed write leaves
        the parent in its existing unknown-custody state.
        """
        record = {
            "type": _PRODUCER_COMPLETION_TYPE,
            "schema": _PRODUCER_COMPLETION_SCHEMA,
            "host_pid": os.getpid(),
            "host_boot_id": self._boot_id,
            "sid": str(terminal.get("sid") or ""),
            "request_id": str(terminal.get("request_id") or ""),
            "turn_id": str(terminal.get("turn_id") or terminal.get("request_id") or ""),
            "session_key": str(terminal.get("session_key") or ""),
            "origin_session_key": str(terminal.get("origin_session_key") or ""),
            "profile_home": str(terminal.get("profile_home") or ""),
            "terminal_sha256": _terminal_digest(terminal),
        }
        data = (json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
        if len(data) > _PRODUCER_COMPLETION_MAX_BYTES:
            logging.getLogger(__name__).warning("compute host producer completion record too large")
            return
        try:
            with self._completion_lock:
                fd = self._completion_fd
                if fd is None or self._completion_closed.is_set():
                    return
                _write_completion_bytes(fd, data)
        except OSError:
            logging.getLogger(__name__).warning(
                "compute host producer completion write failed")

    def _emit_producer_startup(self) -> None:
        """Bind the real child PID/boot before public hello or tool imports."""
        record = build_producer_startup_record(pid=os.getpid(), boot_id=self._boot_id)
        data = (json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n").encode()
        try:
            with self._completion_lock:
                fd = self._completion_fd
                if fd is None or self._completion_closed.is_set():
                    return
                _write_completion_bytes(fd, data)
        except OSError:
            logging.getLogger(__name__).warning(
                "compute host producer startup write failed")

    def close(self, *, deadline: float | None = None) -> bool:
        """Close transport after a bounded admission seal.

        A direct close has no lifecycle budget and therefore uses an immediate
        deadline.  It must never wait forever for a reader that may be stuck in
        a terminal/refusal path; an expired seal leaves custody unresolved.
        """
        self._shutdown_requested.set()
        self._closed.set()
        admissions_settled = self._seal_turn_admissions(deadline=deadline)
        self._executor.shutdown(wait=False, cancel_futures=True)
        self._completion_close_requested.set()
        with self._completion_lock:
            fd, self._completion_fd = self._completion_fd, None
            self._completion_closed.set()
            if fd is not None:
                with contextlib.suppress(OSError):
                    os.close(fd)
        # A foreground command in its own process group would outlive the closing host.
        with contextlib.suppress(Exception):
            from tools.environments.base import kill_live_foreground_processes
            kill_live_foreground_processes()
        self._shutdown_complete.set()
        return admissions_settled

    def _drain_and_flush(
        self, *, reason: str, wait: float, deadline: float | None = None,
    ) -> bool:
        """Drain accepted turns and flush idle sessions without closing the host transport."""
        budget = max(0.0, wait)
        reserve = min(_FLUSH_RESERVE_SECS, budget / 2.0)
        drain_deadline = time.monotonic() + budget - reserve
        if deadline is not None:
            drain_deadline = min(drain_deadline, deadline - reserve)
        while True:
            remaining = drain_deadline - time.monotonic()
            if remaining <= 0 or not self._live_turns():
                break
            # Bounded by ``remaining``: a flat sleep would eat the reserve it protects.
            time.sleep(min(0.05, remaining))
        with self._turn_futures_lock:
            live_sids = {sid for f, sid in self._turn_futures.items() if sid and not f.done()}
        self.flush_all_sessions(reason=reason, skip_sids=live_sids)
        return not self._live_turns()

    def shutdown(self, *, reason: str = "shutdown", wait: float = 10.0) -> None:
        """Drain in-flight turns, then close the host for non-command shutdown paths.

        The explicit shutdown command uses ``_drain_and_flush`` first so its ACK can be
        serialized while the host and completion transport are still alive. Signal, orphan,
        and stdin-EOF paths retain the existing closed-before-drain behavior. The same
        monotonic deadline bounds admission sealing; expiry leaves custody unknown.
        """
        deadline = time.monotonic() + max(0.0, wait)
        with self._shutdown_lock:
            if self._shutdown_complete.is_set():
                return
            self._shutdown_requested.set()
            self._closed.set()
        # Let a reader that already entered turn admission acquire the lifecycle
        # lock, observe the shutdown boundary, and publish its refusal. Waiting
        # while holding that lock would deadlock the reader before it can write.
        self._drain_and_flush(reason=reason, wait=wait, deadline=deadline)
        self._seal_turn_admissions(deadline=deadline)
        with self._shutdown_lock:
            if self._shutdown_complete.is_set():
                return
            self.close(deadline=deadline)

    def flush_all_sessions(
        self, *, reason: str = "shutdown", skip_sids: Collection[str] | None = None) -> None:
        """Finalize every server session except ``skip_sids`` (turn still live)."""
        try:
            from tui_gateway import server
        except Exception:
            return
        skip = set(skip_sids or ())
        for sid, session in list(server._sessions.items()):
            if sid in skip:
                continue
            with contextlib.suppress(Exception):
                server._finalize_session(session, end_reason=f"compute_host_{reason}")

    def handle_frame(self, frame: dict[str, Any]) -> None:
        kind = str(frame.get("type") or "")
        handler = self._FRAME_HANDLERS.get(kind)
        if handler is None:
            self.emit({
                "type": "error", "request_id": frame.get("request_id"),
                "message": f"unknown frame type: {kind}"})
        else:
            if kind == "turn.start":
                # A frame that entered before shutdown remains counted even while
                # it waits on the admission RLock. If the barrier was already
                # sealed, drop it: stdout alone must never become an authenticated
                # terminal or release a parent lease.
                if not self._begin_turn_admission():
                    return
                try:
                    getattr(self, handler)(frame)
                finally:
                    self._end_turn_admission()
            else:
                getattr(self, handler)(frame)

    def _handle_shutdown(self, frame: dict[str, Any]) -> None:
        # The exact ACK must be serialized before close marks the host terminal or closes
        # the completion channel. The parent refuses clean capture unless this receipt is
        # observed and matches the current process identity. An expired admission seal
        # deliberately emits no ACK, leaving the parent in unknown custody.
        with self._shutdown_lock:
            if self._shutdown_complete.is_set():
                return
            self._shutdown_requested.set()
            self._closed.set()
        deadline = time.monotonic() + 10.0
        drained = self._drain_and_flush(reason="shutdown", wait=10.0, deadline=deadline)
        admissions_settled = self._seal_turn_admissions(deadline=deadline)
        with self._shutdown_lock:
            if self._shutdown_complete.is_set():
                return
            try:
                if admissions_settled:
                    self.emit({
                        "type": "shutdown.ack", "request_id": frame.get("request_id"),
                        "host_pid": os.getpid(), "host_boot_id": self._boot_id,
                        "drained": drained,
                    })
            finally:
                self.close(deadline=deadline)

    def _begin_turn_admission(self) -> bool:
        """Register a parsed turn until its terminal/refusal path returns."""
        with self._turn_admission_condition:
            if self._turn_admission_closed:
                return False
            self._turn_admissions += 1
            return True

    def _end_turn_admission(self) -> None:
        with self._turn_admission_condition:
            self._turn_admissions -= 1
            if self._turn_admissions == 0:
                self._turn_admission_condition.notify_all()

    def _seal_turn_admissions(self, *, deadline: float | None = None) -> bool:
        """Bound the parsed-turn admission/refusal wait before closing custody."""
        if deadline is None:
            # A direct close has no caller-owned lifecycle budget.  Fail closed
            # immediately rather than inventing an unbounded grace period.
            deadline = time.monotonic()
        with self._turn_admission_condition:
            if self._turn_admission_expired:
                self._turn_admission_closed = True
                return False
            while self._turn_admissions:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._turn_admission_closed = True
                    self._turn_admission_expired = True
                    return False
                self._turn_admission_condition.wait(timeout=remaining)
            self._turn_admission_closed = True
            return True

    def _track_turn_future(self, future: concurrent.futures.Future, sid: str) -> None:
        """Track an in-flight turn; the done callback pops it or the map grows forever."""
        with self._turn_futures_lock:
            self._turn_futures[future] = sid
        future.add_done_callback(self._untrack_turn_future)

    def _untrack_turn_future(self, future: concurrent.futures.Future) -> None:
        with self._turn_futures_lock:
            self._turn_futures.pop(future, None)

    def _handle_turn_start(self, frame: dict[str, Any]) -> None:
        sid = str(frame.get("sid") or "")
        request_id = frame.get("request_id")
        refused_by_shutdown = False
        submit_error: str | None = None
        # The child reader and signal/orphan shutdown share this boundary.  Hold it
        # only through submit+track: response/interrupt handlers remain outside the
        # lock so already-accepted work can drain and publish its terminal frame.
        with self._shutdown_lock:
            if self._shutdown_requested.is_set() or self._closed.is_set():
                refused_by_shutdown = True
            else:
                try:
                    future = self._executor.submit(self._run_real_turn, dict(frame))
                except RuntimeError as exc:
                    submit_error = str(exc)
                else:
                    self._track_turn_future(future, sid)
        if refused_by_shutdown:
            self._reply_and_complete(
                "turn.error", sid, request_id,
                turn_id=str(frame.get("turn_id") or request_id or ""),
                session_key=str(frame.get("session_key") or ""),
                profile_home=str(frame.get("profile_home") or ""),
                origin_session_key=str(frame.get("session_key") or ""),
                reason="shutdown_requested", message="compute host is shutting down")
            return
        if submit_error is not None:
            self._reply_and_complete(
                "turn.error", sid, request_id,
                turn_id=str(frame.get("turn_id") or request_id or ""),
                session_key=str(frame.get("session_key") or ""),
                profile_home=str(frame.get("profile_home") or ""),
                origin_session_key=str(frame.get("session_key") or ""),
                reason="submit_refused", message=submit_error)

    def _guarded(
        self, frame: dict[str, Any], error_kind: str, body: Callable, *,
        on_error: Callable[[str], None] | None = None, **error_extra: Any) -> None:
        """Run ``body(server, sid, request_id)``; any exception becomes an ``error_kind`` reply."""
        sid = str(frame.get("sid") or "")
        request_id = frame.get("request_id")
        try:
            from tui_gateway import server
            body(server, sid, request_id)
        except Exception as exc:
            if on_error is not None:
                on_error(sid)
            self._reply(error_kind, sid, request_id, **error_extra, message=str(exc))

    def _handle_interrupt(self, frame: dict[str, Any]) -> None:
        def body(server: Any, sid: str, request_id: Any) -> None:
            session = server._sessions.get(sid)
            if session is None:
                self._reply("interrupt.ack", sid, request_id, applied=False)
                return
            # In the child the shared helper interrupts the local agent and releases this
            # process's pending clarify Event (the parent only has a metadata mirror).
            server._interrupt_session_turn(sid, session)
            self._reply("interrupt.ack", sid, request_id, applied=True, applied_ns=now_ns())
        self._guarded(frame, "interrupt.ack", body, applied=False)

    def _handle_respond(self, frame: dict[str, Any]) -> None:
        """Resolve a server→client request this host owns: ``params.frame`` is the client's JSON-RPC response
        frame relayed by the parent; ``params.lock`` is one batch-clarify lock (answered with ``clarify.lock``'s
        result so the parent can ack the client); ``params.cancel`` withdraws this session's open requests of
        one ``method`` (the parent's own cancel never reaches a wait owned here)."""
        def body(server: Any, sid: str, request_id: Any) -> None:
            params = frame.get("params")
            error = ("session not found" if sid not in server._sessions
                     else None if isinstance(params, dict) else "response params must be an object")
            if error:
                self._reply("respond.error", sid, request_id, message=error)
                return
            from tui_gateway import server_requests
            if isinstance(params.get("lock"), dict):
                response = server._methods["clarify.lock"](request_id, params["lock"])
            elif isinstance(cancel := params.get("cancel"), dict):
                withdrawn = server_requests.cancel(sid, str(cancel.get("reason") or "resolved"), method=str(cancel["method"]))
                response = {"jsonrpc": "2.0", "id": request_id, "result": {"cancelled": withdrawn}}
            else:
                response_frame = params.get("frame") if isinstance(params.get("frame"), dict) else params
                resolved = server_requests.resolve_response(response_frame)
                response = {"jsonrpc": "2.0", "id": request_id, "result": {"status": "ok" if resolved else "expired"}}
            self._reply("respond.ack", sid, request_id, response=response)
        self._guarded(frame, "respond.error", body)

    def _run_real_turn(self, frame: dict[str, Any]) -> None:
        sid = str(frame.get("sid") or "")
        request_id = str(frame.get("request_id") or uuid.uuid4().hex)
        turn_id = str(frame.get("turn_id") or request_id)
        session_key = str(frame.get("session_key") or "")
        origin_session_key = session_key
        profile_home = str(frame.get("profile_home") or "")
        session: dict[str, Any] | None = None
        run_thread: Any = None
        dispatch_started = False
        server_module: Any = None
        if not sid:
            self._reply_and_complete(
                "turn.error", sid, request_id, turn_id=turn_id,
                session_key=session_key, origin_session_key=origin_session_key,
                profile_home=profile_home,
                reason="invalid_request", message="sid required")
            return
        try:
            from tui_gateway import server
            server_module = server
            session = self._ensure_server_session(server, frame)
            session_key = str(session.get("session_key") or session_key)
            profile_home = str(session.get("profile_home") or profile_home)
            # #101416: the parent already holds this session's active-session lease (claimed in
            # prompt.submit before routing here). Install the inert borrow BEFORE the turn runs, or
            # _admit_prompt_turn re-claims from this child pid and is fenced out by the parent's own
            # registry entry ("already has a live owner"). Unknown flag (parent predates the field):
            # no borrow, legacy self-claim path, unchanged behaviour.
            server._install_borrowed_lease(sid, session, frame)
            text = frame["text"] if "text" in frame else frame.get("prompt", "")
            inflight = frame["text"] if "text" in frame else frame.get("prompt")
            with session["history_lock"]:
                queued_gen = frame.get("queued_prompt_generation")
                current_gen = int(session.get("_queued_prompt_generation", 0))
                if queued_gen is not None and current_gen != int(queued_gen):
                    self._reply_and_complete(
                        "turn.end", sid, request_id, turn_id=turn_id,
                        session_key=session_key, origin_session_key=origin_session_key,
                        profile_home=profile_home,
                        interrupted=True, ended_ns=now_ns())
                    return
                if session.get("running"):
                    self._reply_and_complete(
                        "turn.error", sid, request_id, turn_id=turn_id,
                        session_key=session_key, origin_session_key=origin_session_key,
                        profile_home=profile_home,
                        reason="session_busy", message="session busy")
                    return
                session.update(running=True, _turn_cancel_requested=False, last_active=time.time())
                server._start_inflight_turn(session, inflight)
                turn_started_at = time.time()
            self._reply("turn.started", sid, request_id, turn_id=turn_id,
                        session_key=session_key, origin_session_key=origin_session_key,
                        profile_home=profile_home,
                        started_ns=now_ns())
            with contextlib.suppress(Exception):
                server._ensure_session_db_row(session)
                # #85303: the parent's prompt.submit reopened this row before dispatching here (or
                # the turn would not have been admitted), but an old parent predating that fix --
                # or a synthesized re-entry -- must not strand the child's transcript writes in a
                # row still marked ended_at. Same best-effort shape as the row binding above.
                with server._session_db(session) as db:
                    if db is not None:
                        server._reopen_if_finalized(db, str(session.get("session_key") or ""))
            with contextlib.suppress(Exception):
                import hermes_undo
                hermes_undo.on_user_message_appended(session["session_key"])
            with contextlib.suppress(Exception):
                server._persist_branch_seed(session)
            dispatch_started = True
            accepted = server._run_prompt_submit(
                request_id, sid, session, text, display_kind=frame.get("display_kind") or None,
                display_metadata=(frame.get("display_metadata")
                                  if isinstance(frame.get("display_metadata"), dict) else None))
            if not accepted:
                # No worker was admitted; the parent must receive a refused turn.
                self._reply_and_complete(
                    "turn.error", sid, request_id, turn_id=turn_id,
                    session_key=session_key, origin_session_key=origin_session_key,
                    profile_home=profile_home,
                    reason="dispatch_refused", message="prompt dispatch refused")
                return
            run_thread = session.get("_run_thread")
            if run_thread is not None and hasattr(run_thread, "join"):
                while run_thread.is_alive():
                    run_thread.join(timeout=1.0)
                    if run_thread.is_alive() and frame.get("turn_id"):
                        self._emit_turn_activity(sid, session, frame["turn_id"], turn_started_at)
            with session["history_lock"]:
                meta = _history_meta(session)
                interrupted = bool(session.get("_turn_cancel_requested"))
            session_info = server._session_info(session.get("agent"), session)
            with self._progress_lock:
                self._progress_counter += 1
            self._reply_and_complete(
                "turn.end", sid, request_id, turn_id=turn_id, origin_session_key=origin_session_key,
                profile_home=profile_home,
                **meta, interrupted=interrupted, ended_ns=now_ns(),
                session_info=session_info, session_info_emitted=True)
        except Exception as exc:
            # Keep the exact accepted session and worker references. The lifecycle may have
            # popped this session from ``server._sessions`` while the worker is still running;
            # looking it up again can otherwise turn a live worker into a false completion.
            owned_session = session
            if owned_session is not None and run_thread is None:
                run_thread = owned_session.get("_run_thread")
            joined = not dispatch_started
            worker_live = False
            if dispatch_started:
                if run_thread is not None and hasattr(run_thread, "join"):
                    try:
                        if run_thread is not threading.current_thread():
                            run_thread.join(timeout=_EXCEPTION_PRODUCER_JOIN_SECS)
                            worker_live = bool(run_thread.is_alive())
                            joined = not worker_live
                    except Exception:  # health: allow BLE001 -- an opaque worker cannot prove join; preserve unknown custody
                        logging.getLogger(__name__).warning(
                            "compute host worker join failed; retaining unknown custody"
                        )
                        joined = False
                        worker_live = False
                else:
                    joined = False
            if joined and owned_session is not None and server_module is not None:
                try:
                    with owned_session.get("history_lock", threading.Lock()):
                        owned_session["running"] = False
                        server_module._clear_inflight_turn(owned_session)
                except Exception:  # health: allow BLE001 -- cleanup can be session/provider-specific; failure keeps custody unknown
                    logging.getLogger(__name__).warning(
                        "compute host inflight cleanup failed; retaining unknown custody"
                    )
                    joined = False
            if joined:
                self._reply_terminal(
                    "turn.error", sid, request_id, turn_id=turn_id,
                    session_key=session_key, origin_session_key=origin_session_key,
                    profile_home=profile_home,
                    reason="exception", message=str(exc), producer_complete=True)
            elif worker_live and owned_session is not None and server_module is not None:
                # Publish one bounded public observation while the accepted worker is
                # still live. This is only an unknown-custody candidate: it must not
                # invoke the callback, publish the private producer receipt, or release
                # the exact turn lease. The watcher retains the original session/thread
                # references and is the only path that may later publish producer_complete.
                self._reply_terminal(
                    "turn.error", sid, request_id, turn_id=turn_id,
                    session_key=session_key, origin_session_key=origin_session_key,
                    profile_home=profile_home,
                    reason="exception", message=str(exc), producer_complete=False)
                if self._watch_exception_worker(
                    server_module, owned_session, run_thread, sid=sid, request_id=request_id,
                    turn_id=turn_id, session_key=session_key,
                    origin_session_key=origin_session_key, profile_home=profile_home,
                    message=str(exc),
                ):
                    return
                # The bounded unknown frame above is already the public observation.
                # If watcher startup fails, leave custody pending rather than emitting
                # a duplicate frame or manufacturing a settled receipt.
            else:
                # No positive proof of a completed worker exists. Keep the parent in
                # its existing unknown-custody path rather than publishing a receipt.
                self._reply_terminal(
                    "turn.error", sid, request_id, turn_id=turn_id,
                    session_key=session_key, origin_session_key=origin_session_key,
                    profile_home=profile_home,
                    reason="exception", message=str(exc), producer_complete=False)

    def _watch_exception_worker(
        self, server: Any, session: dict[str, Any], run_thread: Any, *, sid: str,
        request_id: str, turn_id: str, session_key: str, origin_session_key: str,
        profile_home: str, message: str,
    ) -> bool:
        """Publish the exception receipt only after the accepted worker actually exits."""
        def wait_for_worker() -> None:
            try:
                run_thread.join()
                if run_thread.is_alive():
                    raise RuntimeError("producer worker remained live after join")
                with session.get("history_lock", threading.Lock()):
                    session["running"] = False
                    server._clear_inflight_turn(session)
            except Exception:  # health: allow BLE001 -- worker join and session cleanup are opaque custody boundaries; failure keeps the lease unresolved
                logging.getLogger(__name__).warning(
                    "compute host exception worker cleanup failed; retaining unknown custody")
                # The live-worker branch already emitted the single bounded
                # producer_complete=false observation before starting this watcher.
                # Keep that candidate and the exact lease pending on join/cleanup
                # failure; a second frame would create a distinct digest and could
                # obscure which custody state is still unresolved.
                return
            self._reply_terminal(
                "turn.error", sid, request_id, turn_id=turn_id,
                session_key=session_key, origin_session_key=origin_session_key,
                profile_home=profile_home,
                reason="exception", message=message, producer_complete=True)

        from agent.memory_provider import spawn_context_thread
        try:
            spawn_context_thread(
                wait_for_worker, name="compute-host-exception-worker",
            ).start()
        except RuntimeError:
            return False
        return True

    def _emit_turn_activity(self, sid: str, session: dict, turn_id: str, started_at: float) -> None:
        # Observe the agent clock, never the host heartbeat. A reused agent's last
        # turn must not lend its activity to a new turn that has not made progress.
        activity_ns = None
        try:
            summary = session["agent"].get_activity_summary()
            stamped_at = summary.get("last_activity_at")
            elapsed = summary.get("seconds_since_activity")
            if stamped_at is not None and stamped_at >= started_at and elapsed is not None and elapsed >= 0:
                activity_ns = now_ns() - int(elapsed * 1_000_000_000)
        except Exception:
            logging.getLogger(__name__).debug("compute host activity unavailable sid=%s", sid, exc_info=True)
        # perf_counter is shared across local processes; queued frames and cached
        # samples age without requiring synchronized wall clocks in the parent.
        self._transport.write({"jsonrpc": "2.0", "method": "compute_host.activity", "params": {
            "session_id": sid, "turn_id": turn_id, "activity_ns": activity_ns}})

    def _ensure_server_session(self, server: Any, frame: dict[str, Any]) -> dict:
        sid = str(frame.get("sid") or "")
        session = server._sessions.get(sid)
        if session is not None:
            session["transport"] = self._transport
            if frame.get("cols") is not None:
                session["cols"] = int(frame.get("cols") or 80)
            for key in ("cwd", "profile_home"):
                if frame.get(key):
                    session[key] = str(frame[key])
        else:
            session = self._build_server_session(server, frame, sid)
        if isinstance(frame.get("attached_images"), list):
            session["attached_images"] = list(frame.get("attached_images") or [])
        # A model switch queued on the host side while this session was busy rides
        # the turn frame; adopt it here so the child's turn thread applies it via
        # _apply_pending_model_switch — in the child the live agent exists.
        if frame.get("pending_model_switch"):
            session["pending_model_switch"] = dict(frame["pending_model_switch"])
        return session

    def _build_server_session(self, server: Any, frame: dict[str, Any], sid: str) -> dict:
        """Build the agent under the frame's profile scope and register the session."""
        key = str(frame.get("session_key") or sid)
        history = frame.get("history") if isinstance(frame.get("history"), list) else []
        profile_home = str(frame.get("profile_home") or "")
        session_db = home_token = secret_token = None
        owns_db = False
        try:
            if profile_home:
                from hermes_constants import set_hermes_home_override
                from agent.secret_scope import build_profile_secret_scope, set_secret_scope
                from hermes_cli.env_loader import hydrate_profile_secret_sources
                from hermes_state_registry import acquire
                home_token = set_hermes_home_override(profile_home)
                # External sources first (1Password / Bitwarden / secrets.command): this isolated
                # turn process never ran the launch dotenv path for the routed profile, so without
                # hydration the scope is built on an empty external snapshot and a vault-only
                # provider key fails closed. Same order as gateway/run.py::_load_profile_secret_scope
                # and tui_gateway/model_switch.py::_profile_runtime_scope_tokens (#119521).
                hydrate_profile_secret_sources(Path(profile_home))
                secret_token = set_secret_scope(
                    build_profile_secret_scope(Path(profile_home)), profile_home=profile_home)
                # DEDICATED handle — ours only until _make_agent succeeds, then the agent owns
                # it. A RAISING _make_agent is the one path where nothing takes it (``owns_db``).
                session_db = acquire(Path(profile_home) / "state.db")
                owns_db = True
            agent = server._make_agent(
                sid, key, session_id=key, model_override=frame.get("model_override"),
                reasoning_config_override=frame.get("reasoning_config_override"),
                service_tier_override=frame.get("service_tier_override"),
                platform_override=frame.get("source"),
                cwd_override=str(frame.get("cwd") or "") or None,
                context_cwd_is_launch_artifact=bool(
                    frame.get("context_cwd_is_launch_artifact", False)),
                session_db=session_db, auth_user_id=frame.get("auth_user_id"))
            if server._transfer_db_to_agent(agent, session_db):
                owns_db = False
        finally:
            if owns_db and session_db is not None:
                with contextlib.suppress(Exception):
                    from hermes_state_registry import release_or_close
                    release_or_close(session_db)
            if home_token is not None:
                with contextlib.suppress(Exception):
                    from hermes_constants import reset_hermes_home_override
                    from agent.secret_scope import reset_secret_scope
                    reset_hermes_home_override(home_token)
                    reset_secret_scope(secret_token)
        try:
            from tui_gateway.transport import bind_transport, reset_transport
            token = bind_transport(self._transport)
            try:
                server._init_session(
                    sid, key, agent, list(history), cols=int(frame.get("cols") or 80),
                    cwd=str(frame.get("cwd") or "") or None, session_db=session_db,
                    source=frame.get("source"))
            finally:
                reset_transport(token)
        except Exception:
            # _init_session's side machinery (slash worker, approval notify) unavailable: keep a
            # minimal host-owned session rather than failing after the expensive agent build.
            server._sessions[sid] = {
                "agent": agent, "session_key": key, "history": list(history),
                "history_lock": threading.Lock(),
                "history_version": int(frame.get("history_version") or 0), "inflight_turn": None,
                "created_at": time.time(), "last_active": time.time(), "running": False,
                "attached_images": [], "image_counter": 0,
                "cwd": str(frame.get("cwd") or os.getcwd()), "cols": int(frame.get("cols") or 80),
                "slash_worker": None, "show_reasoning": server._load_show_reasoning(),
                "tool_progress_mode": server._load_tool_progress_mode(), "edit_snapshots": {},
                "tool_started_at": {}, "model_override": frame.get("model_override"),
                "source": server._sanitize_client_source(frame.get("source")),
                "transport": self._transport}
        session = server._sessions[sid]
        session["transport"] = self._transport
        # The host pipe names no login; the record carries the one the gateway stamped at creation.
        session["auth_user_id"] = frame.get("auth_user_id")
        session["profile_home"] = profile_home or session.get("profile_home")
        if frame.get("model_override") is not None:
            session["model_override"] = frame.get("model_override")
        # See _ensure_server_session's existing-session branch — a queued model
        # switch crosses the process boundary and applies at this child's turn start.
        if frame.get("pending_model_switch"):
            session["pending_model_switch"] = dict(frame["pending_model_switch"])
        return session

    def _handle_reload_mcp(self, frame: dict[str, Any]) -> None:
        def body(server: Any, sid: str, request_id: Any) -> None:
            resp = server.handle_request({
                "id": request_id, "method": "reload.mcp",
                "params": {"session_id": sid, "confirm": True}})
            self._reply("reload_mcp.ack", sid, request_id, response=resp)
        self._guarded(frame, "control.error", body)

    def _handle_control(self, frame: dict[str, Any]) -> None:
        route_name = str(frame.get("route_name") or "")

        def body(server: Any, sid: str, request_id: Any) -> None:
            route = MUTATOR_ROUTE_TABLE.get(route_name)
            session = server._sessions.get(sid)
            error = (f"unclassified route: {route_name}" if route is None
                     else "session not found" if session is None
                     else "session busy" if route == "idle-gated" and session.get("running")
                     else None)
            if error:
                self._reply("control.error", sid, request_id, message=error)
            elif route_name == "reload.mcp":
                self._handle_reload_mcp({**frame, "type": "reload_mcp"})
            else:
                ack = self._control_ack(server, frame, session)
                if "error" in ack:
                    self._reply("control.error", sid, request_id, message=ack["error"],
                                **({"code": c} if (c := ack.get("code")) else {}))
                else:
                    self._reply("control.ack", sid, request_id, route_name=route_name, **ack)

        def on_error(sid: str) -> None:
            if route_name in {"session.compress", "slash.compress"}:
                # The compress mirror defers the context-engine boundary notification until the
                # host commits; discard it so it can't fire against a rejected boundary later
                # (finalize is exactly-once, so a no-op if the mirror already emitted it).
                with contextlib.suppress(Exception):
                    from tui_gateway import server as _server
                    from agent.conversation_compression import (
                        finalize_context_engine_compression_notification as _finalize)
                    _agent = (_server._sessions.get(sid) or {}).get("agent")
                    if _agent is not None:
                        _finalize(_agent, committed=False)
        self._guarded(frame, "control.error", body, on_error=on_error)

    def _control_ack(self, server: Any, frame: dict[str, Any], session: dict) -> dict:
        """control.ack payload for one classified route, or ``{"error": message}``."""
        sid = str(frame.get("sid") or "")
        route_name = str(frame.get("route_name") or "")
        command = str(frame.get("command") or "")
        if route_name in {"session.save", "session.compress"}:
            params = {"session_id": sid}
            if route_name == "session.compress":
                focus_topic = command.removeprefix("/compress").strip()
                if focus_topic:
                    params["focus_topic"] = focus_topic
            response = server._methods[route_name](frame.get("request_id"), params)
            if "error" in response:
                failure = _CONTROL_FAILURES[route_name]
                return {"error": str(response["error"].get("message") or failure), "code": response["error"].get("code")}
            ack = {"result": response.get("result") or {}}
            if route_name == "session.save":
                return ack
            with session["history_lock"]:
                ack.update(_history_meta(session))
        else:
            if route_name == "slash.refine":
                parts = command.lstrip("/").split(maxsplit=1)
                focus = parts[1] if len(parts) > 1 else ""
                output = server._live_slash_command_output(sid, session, "refine", focus) or ""
            else:
                output = server._mirror_slash_side_effects(sid, session, command) if command else ""
            with session["history_lock"]:
                messages = server._history_to_messages(list(session.get("history") or []), profile_home=session.get("profile_home"))
                ack = {"output": output, **_history_meta(session), "messages": messages}
        ack["session_info"] = server._session_info(session.get("agent"), session)
        return ack

    def _live_turns(self) -> list[concurrent.futures.Future]:
        with self._turn_futures_lock:
            return [f for f in self._turn_futures if not f.done()]

    def _heartbeat_loop(self) -> None:
        while not self._closed.wait(self._heartbeat_secs):
            if self._shutdown_requested.is_set():
                return
            active_turns = len(self._live_turns())
            with self._progress_lock:
                counter = self._progress_counter
            self.emit({
                "type": "hb", "active_turns": active_turns, "progress_counter": counter,
                "rss_mb": _rss_mb(os.getpid())})

    def _parent_guard_loop(self) -> None:
        while not self._closed.wait(1.0):
            if self._shutdown_requested.is_set():
                return
            ppid = os.getppid()
            if ppid in {0, 1} or (self._parent_pid and ppid != self._parent_pid):
                self.emit({"type": "orphan", "old_ppid": self._parent_pid, "ppid": ppid})
                self.shutdown(reason="orphan")
                os._exit(0)


def _history_meta(session: dict) -> dict[str, Any]:
    """Transcript identity for turn.end / control.ack frames; caller holds history_lock."""
    return {
        "session_key": str(session.get("session_key") or ""),
        "history_version": int(session.get("history_version", 0)),
        "message_count": len(session.get("history") or [])}


def _rss_mb(pid: int) -> float:
    try:
        out = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(pid)], text=True, encoding="utf-8", errors="replace",
            stdin=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=2).strip()
        return int(out.splitlines()[-1].strip()) / 1024.0 if out else 0.0
    except Exception:
        return 0.0


def _default_workers() -> int:
    try:
        return max(2, int(os.environ.get("HERMES_TUI_RPC_POOL_WORKERS") or "8"))
    except (TypeError, ValueError):
        return 8


def _open_producer_completion_fd() -> int | None:
    """Claim the one inherited completion handle before importing tool/server code."""
    raw = os.environ.pop(_PRODUCER_COMPLETION_ENV, "")
    if not raw:
        return None
    fd: int | None = None
    try:
        if os.name == "nt":
            import msvcrt

            handle = int(raw)
            fd = msvcrt.open_osfhandle(handle, os.O_WRONLY | os.O_BINARY)
            assert fd is not None
            try:
                os.set_handle_inheritable(handle, False)
            except Exception:
                with contextlib.suppress(OSError):
                    os.close(fd)
                fd = None
                raise
            return fd
        fd = int(raw)
        assert fd is not None
        try:
            os.set_inheritable(fd, False)
        except Exception:
            with contextlib.suppress(OSError):
                os.close(fd)
            fd = None
            raise
        return fd
    except (AssertionError, OSError, ValueError):
        logging.getLogger(__name__).warning(
            "compute host producer completion handle setup failed")
        if fd is not None:
            with contextlib.suppress(OSError):
                os.close(fd)
        return None


def run_host(stdin: Any = None, stdout: Any = None) -> None:
    os.environ["HERMES_COMPUTE_HOST_CHILD"] = "1"
    # JSONL framing is byte-oriented; avoid text-stream read-ahead on Windows pipes.
    stdin = stdin if stdin is not None else getattr(sys.stdin, "buffer", sys.stdin)
    completion_fd = _open_producer_completion_fd()
    host = ComputeHost(stdout=stdout or sys.stdout, completion_fd=completion_fd)
    # This is the only source-authenticated startup identity. It must precede
    # public hello and the server/tool import path, whose output is untrusted.
    host._emit_producer_startup()
    shutting_down = threading.Event()
    # No client is connected to this process: session-less broadcasts (``broadcast_plugin_event``
    # from a plugin tool/hook running in the isolated turn) ride the host pipe to the parent
    # gateway, which fans them out to its clients (compute_host_bridge._relay_compute_host_rpc).
    from tui_gateway import server
    server.register_live_transport(host._transport)

    def _signal_handler(_signum, _frame) -> None:
        if shutting_down.is_set():
            return
        shutting_down.set()
        host.shutdown(reason="sigterm")
        raise SystemExit(0)
    with contextlib.suppress(Exception):
        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)
    host.emit({
        "type": "hello", "host_pid": os.getpid(), "boot_id": host._boot_id,
        "build_sha": _build_sha(), "cwd": os.getcwd(),
        "hermes_home": os.environ.get("HERMES_HOME", "")})

    shutdown_command_started = threading.Event()

    def _reader() -> None:
        for raw in stdin:
            if host._closed.is_set():
                break
            try:
                frame = json.loads(raw)
            except json.JSONDecodeError as exc:
                host.emit({"type": "error", "message": f"invalid json: {exc}"})
                continue
            if not isinstance(frame, dict):
                host.emit({"type": "error", "message": "frame must be an object"})
                continue
            is_shutdown = frame.get("type") == "shutdown"
            if is_shutdown:
                shutdown_command_started.set()
            host.handle_frame(frame)
            if is_shutdown:
                # The handler has emitted and flushed the ACK before close(). The main
                # thread joins this reader before running any fallback finalizer.
                return
            if host._closed.is_set():
                break
    reader = threading.Thread(target=_reader, name="compute-host-control-reader", daemon=True)
    reader.start()
    try:
        while not host._closed.wait(0.2):
            if not reader.is_alive():
                break
    finally:
        if shutdown_command_started.is_set():
            # A command handler owns the shutdown lock and has a bounded drain. Joining
            # here prevents the main thread from exiting while the ACK write is in flight.
            reader.join()
        server.unregister_live_transport(host._transport)
        if not host._shutdown_complete.is_set():
            host.shutdown(reason="stdin_closed", wait=2.0)


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="Dashboard compute-host process").parse_args(argv)
    run_host()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
