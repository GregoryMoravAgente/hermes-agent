"""Supervisor for the dashboard compute-host child: with ``dashboard.turn_isolation``
agent turns run in one persistent ``python -m tui_gateway.compute_host`` child so heavy
agent threads do not contend with the serving process' event loop for the GIL."""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
import hashlib
import json
import logging
import os
import queue
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from hermes_cli.local_runtime.processes import spawn_server
from hermes_constants import get_hermes_home
from tools.environments.local import hermes_subprocess_env
from tui_gateway.host_supervisor_streams import drain_producer_completion, readers_settled
from tui_gateway.windows_owner_recovery import (
    new_windows_job_name,
    owner_record_key,
    owner_state,
    persist_owner_record,
    remove_registry_record,
    RegistryLease,
    reconcile_startup_orphan,
    release_owner,
    try_close_owner,
)
from tui_gateway.windows_producer_identity import (
    accept_producer_startup,
    cancel_producer_startup,
    try_close_producer_handle,
    valid_producer_startup_record,
)

logger = logging.getLogger(__name__)

MUTATOR_ROUTE_TABLE: dict[str, str] = {
    "prompt.submit": "turn-path", "session.interrupt": "turn-path", "reload.mcp": "run-concurrent",
    "session.save": "run-concurrent", "session.compress": "idle-gated",
    "prompt.submit.truncate": "idle-gated", "slash.model": "idle-gated",
    "slash.personality": "idle-gated", "slash.prompt": "idle-gated", "slash.compress": "idle-gated",
    "slash.refine": "idle-gated",
    "session.reset": "idle-gated", "session.history.reload": "idle-gated",
    "slash.retry": "idle-gated"}

_REGISTRY_NAME = "dashboard-compute-host.json"
_RESPAWN_WINDOW_SECS = 300.0
_SHUTDOWN_TIMEOUT_SECS = 10.0
# A cold host imports the agent stack before its hello (~1.3s idle, 5s+ on a loaded box).
_HELLO_TIMEOUT_SECS = 10.0
# Late control-ack handlers: a compress that outlives its RPC waiter can run for the full
# compression ceiling plus a stall-fallback retry, so keep registrations past that — bounded.
# See #97948.
_LATE_CONTROL_TTL_SECS = 1800.0
_LATE_CONTROL_MAX = 64
# Host frames whose ``request_id`` resolves a pending/late control waiter.
_CONTROL_REPLY_TYPES = frozenset({
    "control.ack", "control.error", "respond.ack", "respond.error", "interrupt.ack",
    "reload_mcp.ack", "shutdown.ack"})
_PRODUCER_COMPLETION_ENV = "HERMES_COMPUTE_HOST_PRODUCER_COMPLETION_FD"
_PRODUCER_COMPLETION_TYPE = "producer.complete"
_PRODUCER_COMPLETION_SCHEMA = 1
_PRODUCER_COMPLETION_MAX_RECORD_BYTES = 4096
_PRODUCER_COMPLETION_KEYS = frozenset({
    "type", "schema", "host_pid", "host_boot_id", "sid", "request_id", "turn_id",
    "session_key", "origin_session_key", "profile_home", "terminal_sha256",
})
_MAX_TERMINAL_CANDIDATES = 8
# Public turn frames can contain bounded transcript/session metadata, but the parent
# must refuse an oversized line before parsing or retaining an inherited writer's copy.
_MAX_TERMINAL_FRAME_BYTES = 1 * 1024 * 1024


class _FrameSendError(RuntimeError):
    """A host-frame send failed; ``ambiguous`` means bytes may have reached the child."""

    def __init__(self, message: str, *, ambiguous: bool, proc=None) -> None:
        super().__init__(message)
        self.ambiguous = ambiguous
        self.proc = proc


class _AdmissionRefusedError(RuntimeError):
    """The turn never crossed the child transport, but exact lease settlement is unresolved."""

    admission_refused = True


def _producer_completion_pipe() -> tuple[int, int, str, dict[str, Any]]:
    """Create one source-bound completion pipe and the child-spawn handoff.

    The write end is made inheritable only for the exact ComputeHost Popen. POSIX
    uses ``pass_fds`` and Windows uses the kernel handle allow-list; the child
    closes inheritance before importing or spawning any tools. Shared stdout is
    intentionally not used as a completion signal because inherited writers can
    produce indistinguishable JSON after the producer has returned.
    """
    read_fd: int | None = None
    write_fd: int | None = None
    try:
        read_fd, write_fd = os.pipe()
        assert read_fd is not None and write_fd is not None
        os.set_inheritable(read_fd, False)
        if os.name == "nt":
            import msvcrt

            handle = msvcrt.get_osfhandle(write_fd)
            os.set_handle_inheritable(handle, True)
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.lpAttributeList = {"handle_list": [handle]}
            return read_fd, write_fd, str(handle), {
                "close_fds": True,
                "startupinfo": startupinfo,
            }
        os.set_inheritable(write_fd, False)
        return read_fd, write_fd, str(write_fd), {
            "close_fds": True,
            "pass_fds": (write_fd,),
        }
    except Exception:
        _close_fd(read_fd)
        _close_fd(write_fd)
        raise


def _terminal_digest(frame: dict[str, Any]) -> str:
    """Digest the complete finalized terminal wire object with stable key ordering."""
    encoded = json.dumps(
        frame, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _valid_producer_completion_record(record: dict[str, Any]) -> bool:
    """Reject schema drift before any source-bound state is changed."""
    if set(record) != _PRODUCER_COMPLETION_KEYS:
        return False
    if record.get("type") != _PRODUCER_COMPLETION_TYPE:
        return False
    if type(record.get("schema")) is not int or record["schema"] != _PRODUCER_COMPLETION_SCHEMA:
        return False
    if type(record.get("host_pid")) is not int or record["host_pid"] <= 0:
        return False
    for key in (
        "host_boot_id", "sid", "request_id", "turn_id", "session_key",
        "origin_session_key",
    ):
        value = record.get(key)
        if type(value) is not str or not value:
            return False
    if type(record.get("profile_home")) is not str:
        return False
    digest = record.get("terminal_sha256")
    return (
        type(digest) is str and len(digest) == 64
        and all(char in "0123456789abcdef" for char in digest)
    )


def _valid_producer_startup_record(record: dict[str, Any]) -> bool:
    """Validate the source-bound startup identity before accepting public hello."""
    return valid_producer_startup_record(record, is_windows=os.name == "nt")


def _close_fd(fd: int | None) -> None:
    if fd is None:
        return
    with contextlib.suppress(OSError):
        os.close(fd)


def _read_bounded_stdout_line(stream: Any) -> tuple[str | None, bool]:
    """Read one complete stdout line with an explicit byte budget.

    The Popen stream is text-wrapped, so prefer its binary buffer for an exact
    UTF-8 byte budget. A small in-memory test stream may expose only text; the
    fallback encodes each bounded chunk before accounting. Oversized content is
    drained through the newline and returned as ``(None, True)`` without parsing.
    """
    reader = getattr(stream, "buffer", None) or stream
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = reader.readline(_MAX_TERMINAL_FRAME_BYTES + 1)
        if not chunk:
            break
        encoded = chunk if isinstance(chunk, bytes) else chunk.encode("utf-8", errors="replace")
        total += len(encoded)
        if total <= _MAX_TERMINAL_FRAME_BYTES:
            chunks.append(encoded)
        if encoded.endswith(b"\n"):
            break
    if total == 0:
        return None, False
    if total > _MAX_TERMINAL_FRAME_BYTES:
        return None, True
    return b"".join(chunks).decode("utf-8", errors="replace"), False


@dataclass
class _PendingTurn:
    """Parent-owned custody for one transport handoff.

    The exact RetirementLease is carried by the completion owner supplied by the
    bridge. The supervisor records it beside the reader-owned host generation so
    a stale stdout reader or a respawn can never settle a later turn.
    """

    sid: str
    session_key: str
    profile_home: str
    turn_id: str
    host_generation: int
    host_boot_id: str
    host_proc: subprocess.Popen[str] | None
    callback: Callable[[dict], object] | None
    retirement_lease: object | None = None
    terminal: dict | None = None
    retry_terminal: dict | None = None
    terminal_candidates: dict[str, dict] = field(default_factory=dict)
    producer_terminal_digest: str | None = None
    finalizing: bool = False
    callback_succeeded: bool = False
    unknown_notified: bool = False
    producer_settled: bool = False
    producer_completion: dict | None = None
    owner_settled: bool = False
    lease_release_pending: bool = False
    pre_submit: bool = False


@dataclass
class _StreamState:
    """One reader's terminal state; EOF is valid only on the no-exception path."""

    eof: bool = False
    failed: bool = False
    error: str = ""
    done: threading.Event = field(default_factory=threading.Event)


def append_log_record(path: str | Path, record: str) -> None:
    """Append one log record using O_APPEND and exactly one os.write call."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    text = record if record.endswith("\n") else f"{record}\n"
    if os.name == "nt":
        # CRT O_APPEND is seek+write, not an atomic append across handles.
        # FILE_APPEND_DATA without FILE_WRITE_DATA makes the kernel append.
        import win32con
        import win32file
        from ntsecuritycon import FILE_APPEND_DATA
        handle = win32file.CreateFile(
            str(path), FILE_APPEND_DATA,
            win32con.FILE_SHARE_READ | win32con.FILE_SHARE_WRITE | win32con.FILE_SHARE_DELETE,
            None, win32con.OPEN_ALWAYS, win32con.FILE_ATTRIBUTE_NORMAL, None,
        )
        try:
            win32file.WriteFile(handle, text.encode("utf-8", errors="replace"))
        finally:
            handle.Close()
        return
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    try:
        os.write(fd, text.encode("utf-8", errors="replace"))
    finally:
        os.close(fd)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _check_output(argv: list[str], **kwargs: Any) -> str:
    """Stripped stdout of a short subprocess, or ``""`` on any failure."""
    with contextlib.suppress(Exception):
        return subprocess.check_output(
            argv, stdin=subprocess.DEVNULL, text=True, encoding="utf-8", errors="replace", stderr=subprocess.DEVNULL,
            timeout=2, **kwargs).strip()
    return ""


def _build_sha() -> str:
    """HEAD sha or ``"unknown"``; shared with ``compute_host`` so the hello handshake agrees."""
    return _check_output(["git", "rev-parse", "HEAD"], cwd=str(_repo_root())) or "unknown"


def _call_logged(cb: Callable[[dict], None], frame: dict, failure: str) -> None:
    """Invoke a host-frame callback; a raising callback is logged, never propagated."""
    try:
        cb(frame)
    except Exception:
        logger.exception(failure)


def _release_retirement_lease(lease: object | None) -> bool:
    """Release only the exact lease owned by a terminal pending-turn record."""
    if lease is None:
        return True
    from hermes_cli.backend_retirement import retirement
    return bool(retirement.release(lease))


def _merge_terminal(current: dict | None, incoming: dict) -> dict:
    """Keep one terminal receipt, preferring a known receipt over an unknown one."""
    if current is None:
        return dict(incoming)
    if current.get("custody") == "unknown" and incoming.get("custody") != "unknown":
        return dict(incoming)
    return dict(current)


def _legacy_pending_fields(pending):
    """Read old two-tuples kept by narrow test/compatibility callers.

    Production accepted turns always use ``_PendingTurn``. The tuple path has no
    custody token and therefore cannot release one; it only preserves the old
    callback behavior for callers that have not opted into this seam.
    """
    if isinstance(pending, _PendingTurn):
        return pending.sid, pending.callback
    try:
        return pending[0], pending[1]
    except (IndexError, KeyError, TypeError):
        return "", None


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except Exception as exc:
        return isinstance(exc, PermissionError)


def _signal_pid(pid: int, sig: int, label: str) -> bool:
    """Send ``sig``; False when the pid is gone or the signal failed (logged)."""
    try:
        os.kill(pid, sig)
        return True
    except ProcessLookupError:
        return False
    except Exception:
        logger.debug("failed to %s compute host pid=%s", label, pid, exc_info=True)
        return False


def _pid_command(pid: int) -> str:
    if pid <= 0:
        return ""
    with contextlib.suppress(Exception):  # Linux fast path
        data = (Path("/proc") / str(pid) / "cmdline").read_bytes()
        if data:
            return data.replace(b"\x00", b" ").decode("utf-8", errors="replace")
    return _check_output(["ps", "-p", str(pid), "-o", "command="])


def is_compute_host_identity(pid: int) -> bool:
    return "tui_gateway.compute_host" in _pid_command(pid)


class HostSupervisor:
    """Own one persistent compute-host child and relay its frames."""

    def __init__(
        self, *, registry_path: str | Path | None = None, argv: list[str] | None = None,
        cwd: str | Path | None = None, env: dict[str, str] | None = None,
        rpc_sink: Callable[[dict], None] | None = None, respawn_max: int = 3,
        heartbeat_secs: int = 15, expected_build_sha: str | None = None,
        expected_hermes_home: str | None = None, autostart: bool = True) -> None:
        self.registry_path = (
            Path(registry_path) if registry_path is not None
            else get_hermes_home() / "state" / _REGISTRY_NAME)
        self.argv = argv or [sys.executable, "-m", "tui_gateway.compute_host"]
        self.cwd = Path(cwd) if cwd is not None else _repo_root()
        self.env = env
        self.rpc_sink = rpc_sink or (lambda _obj: None)
        self.respawn_max = max(0, int(respawn_max))
        self.heartbeat_secs = max(1, int(heartbeat_secs))
        self.expected_build_sha = _build_sha() if expected_build_sha is None else expected_build_sha
        self.expected_hermes_home = (
            str(get_hermes_home()) if expected_hermes_home is None else expected_hermes_home)
        self._lock = threading.RLock()
        # Startup waits for hello while the caller owns ``_lock``.  Keep the
        # reader's one-time hello CAS on a separate mutex: taking the parent
        # lifecycle lock from the stdout reader would deadlock every spawn.
        self._hello_lock = threading.Lock()
        self._proc: subprocess.Popen[str] | None = None
        self._owner: Any | None = None
        # The Windows Job name is the recoverable owner credential.  It is only
        # persisted after startup identity validation; no PID is ever adopted.
        self._owner_job_name: str | None = None
        self._registry_owner_key: tuple[str, int, str, int, str | None] | None = None
        self._registry_record_removed = False
        # A query-only handle for a live named Job is retained while duplicate
        # startup is refused.  Closing the last handle on KILL_ON_JOB_CLOSE can
        # terminate members, so an active recovery handle is never closed merely
        # to return an error.
        self._recovery_owner: Any | None = None
        self._registry_lease: RegistryLease | None = None
        self._owner_closed = False
        # The identity is deliberately unset until the exact child has passed hello
        # validation. It is (host generation, validated boot id, Popen pid), never a
        # random metadata token that could bind a different process.
        self._owner_identity: tuple[int, str, int] | None = None
        # The private startup receipt binds the real ComputeHost identity separately
        # from the Popen/owner identity.  A Windows venv launcher may have a
        # different PID from the real child; completion and shutdown ACKs use this
        # source-bound pair, while owner custody remains bound to Popen and its Job.
        self._producer_identity: tuple[int, str] | None = None
        self._producer_process_handle: Any | None = None
        self._producer_creation_filetime: int | None = None
        self._startup_event = threading.Event()
        self._startup_identity: tuple[int, str] | None = None
        self._startup_error = ""
        self._startup_seen = False
        self._startup_binding: tuple[Any, int] | None = None
        self._startup_cancelled_generations: set[int] = set()
        self._startup_orphan_handles: dict[int, Any] = {}
        self._pending_shutdown_receipt: dict[str, Any] | None = None
        self._shutdown_request_id: str | None = None
        self._shutdown_ack: dict[str, Any] | None = None
        self._shutdown_admission_closed = False
        self._stream_states: dict[int, tuple[_StreamState, _StreamState, _StreamState]] = {}
        self._exit_settled: dict[int, threading.Event] = {}
        self._shutdown_sent = False
        self._hello_event = threading.Event()
        self._hello: dict[str, Any] = {}
        self._closing = False
        self._stopped_respawning = False
        self._restart_times: list[float] = []
        self._host_generation = 0
        self._host_boot_id = ""
        # Gate 1 proves only the producer turn boundary. Retained host/resource
        # custody is deliberately still unknown, so capture publication remains
        # fail-closed until the later process-owner gate supplies a real receipt.
        self._host_resource_custody_known = False
        self._pending_turns: dict[str, _PendingTurn | tuple[str, Callable[[dict], None] | None]] = {}
        self._pending_controls: dict[str, queue.Queue[dict]] = {}
        # request_id -> (registered_at, handler) for control waiters that timed out while their
        # host work still runs, so the eventual control.ack is not silently dropped.
        # The host emits its control.ack whenever it finishes; without this the ack matched no queue and was
        # silently dropped. See #97948.
        self._late_control_handlers: dict[
            str, tuple[float, Callable[[dict], None] | None]
        ] = {}
        self._late_control_uncertain = False
        self._stderr_tail: list[str] = []
        self._last_progress_counter = 0
        if autostart:
            self.start()

    @property
    def pid(self) -> int:
        proc = self._proc
        return int(proc.pid or 0) if proc is not None else 0

    def is_running(self) -> bool:
        proc = self._proc
        return proc is not None and proc.poll() is None and not self._stopped_respawning

    @property
    def capture_ready(self) -> bool:
        """Whether retained host/resource custody is proven for capture publication.

        Producer completion is intentionally insufficient. This remains false in
        the gate-1 composition; a later owner/descendant proof must extend the
        guarded state without changing the turn callback contract.
        """
        with self._lock:
            return bool(self._host_resource_custody_known)

    def start(self) -> None:
        with self._lock:
            if self._closing:
                raise RuntimeError("compute host is closing")
            if self.is_running():
                return
            if self._owner is not None:
                raise RuntimeError("compute host owner is still retained after stop")
            if self._registry_lease is None and not self._acquire_registry_lease():
                raise RuntimeError("compute host registry is already owned")
            self._closing = False
            self._shutdown_admission_closed = False
            self._pending_shutdown_receipt = None
            self._shutdown_sent = False
            self._shutdown_request_id = None
            self._shutdown_ack = None
            outcome = self.reconcile_startup_orphan(keep_lease=True)
            if outcome == "owner-unavailable":
                raise RuntimeError("compute host owner is still live or not proven absent")
            self._spawn_locked(reason="startup")

    def _acquire_registry_lease(self) -> bool:
        self._registry_lease = RegistryLease.acquire(self.registry_path)
        return self._registry_lease is not None

    def _release_registry_lease(self) -> bool:
        if self._registry_lease is None:
            return True
        released = self._registry_lease.release()
        self._registry_lease = None if released else self._registry_lease
        return released

    def _owner_state(self) -> str:
        with self._lock:
            owner = self._owner
        return owner_state(owner)

    def _release_owner(self) -> bool:
        return release_owner(self)

    @staticmethod
    def _try_close_owner(owner: Any | None) -> bool:
        return try_close_owner(owner)

    def _try_close_producer_handle(self) -> bool:
        return try_close_producer_handle(self)

    @staticmethod
    def _readers_settled(
        states: tuple[_StreamState, _StreamState, _StreamState] | None,
        timeout: float,
    ) -> bool:
        return readers_settled(states, timeout)

    @staticmethod
    def _stop_result(
        *, shutdown_sent: bool, process_exited: bool, natural_exit: bool,
        stdout: _StreamState | None, stderr: _StreamState | None,
        completion: _StreamState | None, owner_state: str, error: str = "",
        identity: tuple[int, str, int] | None = None,
        shutdown_ack: bool = True, exit_settled: bool = True,
        pending_turns: int = 0, pending_controls: int = 0,
        late_controls: int = 0, late_control_uncertain: bool = False,
    ) -> dict[str, Any]:
        streams = (stdout, stderr, completion)
        reader_done = all(state is not None and state.done.is_set() for state in streams)
        reader_eof = all(state is not None and state.eof for state in streams)
        reader_failed = any(state is not None and state.failed for state in streams)
        clean = bool(
            shutdown_sent and process_exited and natural_exit and reader_done and reader_eof
            and not reader_failed and owner_state == "empty" and identity is not None
            and shutdown_ack and exit_settled
            and not pending_turns and not pending_controls and not late_controls
            and not late_control_uncertain
        )
        if pending_turns or pending_controls or late_controls or late_control_uncertain:
            status = "pending_work"
        elif not shutdown_ack:
            status = "shutdown_ack_missing"
        elif not exit_settled:
            status = "exit_settlement_pending"
        elif clean:
            status = "clean_stopped"
        elif owner_state == "unavailable":
            status = "owner_unavailable"
        elif owner_state == "unknown":
            status = "unknown"
        else:
            status = "incomplete"
        errors = [state.error for state in streams if state is not None and state.error]
        result: dict[str, Any] = {
            "status": status,
            "clean": clean,
            "ack_required": clean,
            "shutdown_sent": shutdown_sent,
            "process_exited": process_exited,
            "natural_exit": natural_exit,
            "stdout_done": bool(stdout is not None and stdout.done.is_set()),
            "stdout_eof": bool(stdout is not None and stdout.eof),
            "stdout_failed": bool(stdout is not None and stdout.failed),
            "stderr_done": bool(stderr is not None and stderr.done.is_set()),
            "stderr_eof": bool(stderr is not None and stderr.eof),
            "stderr_failed": bool(stderr is not None and stderr.failed),
            "completion_done": bool(completion is not None and completion.done.is_set()),
            "completion_eof": bool(completion is not None and completion.eof),
            "completion_failed": bool(completion is not None and completion.failed),
            "owner_state": owner_state,
            "shutdown_ack": bool(shutdown_ack),
            "exit_settled": bool(exit_settled),
            "pending_turns": int(pending_turns),
            "pending_controls": int(pending_controls),
            "late_controls": int(late_controls),
            "late_control_uncertain": bool(late_control_uncertain),
        }
        if identity is not None:
            result.update({
                "host_generation": identity[0],
                "host_boot_id": identity[1],
                "host_pid": identity[2],
            })
        if error or errors:
            result["error"] = ";".join(part for part in (error, *errors) if part)
        return result

    def shutdown_clean(self, *, timeout: float = _SHUTDOWN_TIMEOUT_SECS) -> dict[str, Any]:
        """Observe a clean stop without releasing the native owner.

        A ready receipt is only an observation.  The owner handle and registry stay
        live until the exact receipt is acknowledged by ``acknowledge_clean_stop``;
        this prevents a capture consumer from racing a close that would terminate a
        still-retained descendant.  Reader errors are refusals even when the streams
        later reach EOF.
        """
        with self._lock:
            if self._pending_shutdown_receipt is not None:
                return dict(self._pending_shutdown_receipt)
            self._closing = True
            proc = self._proc
            states = self._stream_states.get(id(proc)) if proc is not None else None
            shutdown_sent = self._shutdown_sent
            pending_turns = len(self._pending_turns)
            pending_controls = len(self._pending_controls)
            late_controls = len(self._late_control_handlers)
            late_control_uncertain = self._late_control_uncertain
            identity = self._owner_identity
            shutdown_ack = self._shutdown_ack is not None
            exit_event = self._exit_settled.get(id(proc)) if proc is not None else None

            # Freeze new admission before checking pending state. The lock makes this
            # one atomic boundary with submit_turn: no new turn can cross the check
            # below, while an already accepted turn may still use the explicit
            # respond/interrupt drain path.
            self._shutdown_admission_closed = True
            if not (pending_turns or pending_controls or late_controls or late_control_uncertain):
                if not shutdown_sent:
                    self._shutdown_request_id = f"shutdown-{uuid.uuid4().hex}"
                    self._shutdown_ack = None
                shutdown_request_id = self._shutdown_request_id
            else:
                shutdown_request_id = None

        stdout, stderr, completion = states or (None, None, None)
        if pending_turns or pending_controls or late_controls or late_control_uncertain:
            result = self._stop_result(
                shutdown_sent=shutdown_sent,
                process_exited=bool(proc is not None and proc.poll() is not None),
                natural_exit=False,
                stdout=stdout, stderr=stderr, completion=completion,
                owner_state=self._owner_state(), error="pending_work",
                identity=identity, shutdown_ack=shutdown_ack,
                exit_settled=bool(exit_event is not None and exit_event.is_set()),
                pending_turns=pending_turns, pending_controls=pending_controls,
                late_controls=late_controls, late_control_uncertain=late_control_uncertain,
            )
            result["admission_closed"] = True
            result["retry_after_settlement"] = True
            return result
        if proc is None:
            return self._stop_result(
                shutdown_sent=shutdown_sent, process_exited=False, natural_exit=False,
                stdout=stdout, stderr=stderr, completion=completion,
                owner_state=self._owner_state(), error="no_process", identity=identity,
                shutdown_ack=shutdown_ack,
                exit_settled=bool(exit_event is not None and exit_event.is_set()),
            )
        deadline = time.monotonic() + max(0.0, float(timeout))
        error = ""
        if not shutdown_sent:
            try:
                if proc.poll() is None and proc.stdin is not None:
                    self._send_frame({
                        "type": "shutdown", "request_id": shutdown_request_id,
                    }, allow_shutdown=True)
                    shutdown_sent = True
                    with self._lock:
                        self._shutdown_sent = True
                else:
                    error = "host_not_running"
            except _FrameSendError as exc:
                error = "shutdown_send_ambiguous" if exc.ambiguous else f"shutdown_send_failed:{type(exc).__name__}"
            except Exception as exc:  # health: allow BLE001 -- failed shutdown delivery cannot prove clean custody; retain the exception type only
                error = f"shutdown_send_failed:{type(exc).__name__}"

        if proc.poll() is None:
            with contextlib.suppress(Exception):
                proc.wait(timeout=max(0.0, deadline - time.monotonic()))
        process_exited = proc.poll() is not None
        natural_exit = bool(shutdown_sent and process_exited and proc.returncode == 0)
        for state in (stdout, stderr, completion):
            if state is not None:
                state.done.wait(timeout=max(0.0, deadline - time.monotonic()))
        exit_settled = bool(
            exit_event is not None
            and exit_event.wait(timeout=max(0.0, deadline - time.monotonic()))
        )
        with self._lock:
            shutdown_ack = (
                self._shutdown_ack is not None
                and self._shutdown_request_id == shutdown_request_id
            )
            pending_turns = len(self._pending_turns)
            pending_controls = len(self._pending_controls)
            late_controls = len(self._late_control_handlers)
            late_control_uncertain = self._late_control_uncertain
            identity = self._owner_identity
        result = self._stop_result(
            shutdown_sent=shutdown_sent, process_exited=process_exited,
            natural_exit=natural_exit, stdout=stdout, stderr=stderr,
            completion=completion, owner_state=self._owner_state(), error=error,
            identity=identity, shutdown_ack=shutdown_ack, exit_settled=exit_settled,
            pending_turns=pending_turns, pending_controls=pending_controls,
            late_controls=late_controls, late_control_uncertain=late_control_uncertain,
        )
        if result["clean"]:
            with self._lock:
                # The process/owner identity must still be the one observed by all
                # readers; otherwise this receipt is stale and cannot be acknowledged.
                if self._proc is proc and self._owner_identity == (
                    result["host_generation"], result["host_boot_id"], result["host_pid"]
                ):
                    self._pending_shutdown_receipt = dict(result)
                else:
                    result["clean"] = False
                    result["ack_required"] = False
                    result["status"] = "stale_identity"
        result["admission_closed"] = self._shutdown_admission_closed
        return result

    def acknowledge_clean_stop(self, receipt: dict[str, Any]) -> bool:
        """Release the exact retained owner after a clean-stop receipt is consumed."""
        with self._lock:
            expected = self._pending_shutdown_receipt
            owner = self._owner
            proc = self._proc
            identity = self._owner_identity
            if expected is None or dict(receipt) != expected or owner is None or proc is None:
                return False
            exit_event = self._exit_settled.get(id(proc))
            if exit_event is None or not exit_event.is_set():
                return False
            if (
                self._pending_turns or self._pending_controls or self._late_control_handlers
                or self._late_control_uncertain
            ):
                return False
            expected_identity = (
                expected.get("host_generation"), expected.get("host_boot_id"),
                expected.get("host_pid"),
            )
            if identity != expected_identity or int(proc.pid or 0) != expected["host_pid"]:
                return False
            if not self._owner_closed:
                try:
                    if owner.empty() is not True:
                        return False
                    owner.close()
                except Exception:  # health: allow BLE001 -- native owner release is an opaque boundary; retain the receipt without exception values
                    logger.warning("compute host owner acknowledgement failed; custody retained")
                    return False
                self._owner_closed = True
            if not self._try_close_producer_handle():
                return False
            # Keep the exact receipt and closed owner retryable when registry
            # deletion fails; capture readiness is published only after removal.
            if not self._remove_registry(self._registry_owner_key):
                return False
            # The lock is part of the same custody boundary as the owner record.
            # Do not clear the receipt or publish capture readiness until its
            # fallible close has succeeded under the lifecycle lock.
            if not self._release_registry_lease():
                return False
            self._owner = None
            self._owner_job_name = None
            self._owner_closed = False
            self._owner_identity = None
            self._producer_identity = None
            self._registry_owner_key = None
            self._registry_record_removed = False
            self._proc = None
            self._host_resource_custody_known = True
            self._pending_shutdown_receipt = None
            self._shutdown_request_id = None
            self._shutdown_ack = None
            self._shutdown_admission_closed = False
            self._closing = False
            self._exit_settled.pop(id(proc), None)
            self._stream_states.pop(id(proc), None)
        return True

    def shutdown(self) -> dict[str, Any]:
        """Use the legacy cleanup path, explicitly acknowledging a clean receipt."""
        result = self.shutdown_clean(timeout=_SHUTDOWN_TIMEOUT_SECS)
        if result.get("clean"):
            if self.acknowledge_clean_stop(result):
                return result
            # A clean receipt that cannot yet be acknowledged still owns the
            # process/owner/registry custody. Do not fall through to destructive
            # cleanup; the caller must retry the exact acknowledgement.
            return {**result, "clean": False, "ack_required": True, "status": "ack_failed"}
        if (
            result.get("pending_turns")
            or result.get("pending_controls")
            or result.get("late_controls")
            or result.get("late_control_uncertain")
        ):
            # A destructive fallback would turn accepted work into unknown custody.
            # The caller must settle the exact records and retry shutdown_clean().
            return result
        with self._lock:
            proc = self._proc
            owner = self._owner
            recovery_owner = self._recovery_owner
        if recovery_owner is not None or (owner is None and self.registry_path.exists()):
            # Destructive cleanup cannot erase unresolved recovery custody or its registry.
            return {
                **result, "clean": False, "ack_required": False,
                "status": "owner_unavailable", "owner_state": "unavailable",
            }
        if proc is not None and proc.poll() is None:
            self._terminate_process(proc)
        self._fail_pending_turns(reason="shutdown", message="compute host shutdown")
        with self._lock:
            producer_closed = self._try_close_producer_handle()
            owner_closed = self._owner_closed or self._try_close_owner(owner)
            registry_removed = (
                owner_closed and producer_closed
                and self._remove_registry(self._registry_owner_key)
            )
            if owner_closed and self._owner is owner:
                self._owner_closed = True
            lease_released = registry_removed and self._release_registry_lease()
            cleanup_complete = registry_removed and lease_released
            if cleanup_complete and self._owner is owner:
                self._owner = None
                self._owner_job_name = None
                self._owner_closed = False
                self._owner_identity = None
                self._producer_identity = None
                self._registry_owner_key = None
                self._registry_record_removed = False
                self._proc = None
                self._pending_shutdown_receipt = None
                self._shutdown_request_id = None
                self._shutdown_ack = None
                self._shutdown_admission_closed = False
                self._closing = False
                self._host_resource_custody_known = False
                if proc is not None:
                    self._exit_settled.pop(id(proc), None)
                    self._stream_states.pop(id(proc), None)
        if not cleanup_complete:
            status = (
                "owner_close_failed" if not owner_closed else
                "producer_close_failed" if not producer_closed else
                "registry_remove_failed" if not registry_removed else
                "registry_lease_release_failed"
            )
            result = {**result, "clean": False, "ack_required": False, "status": status}
        return result

    def _legacy_shutdown(self) -> None:
        """Compatibility name for callers that only need destructive cleanup."""
        self.shutdown()

    def reconcile_startup_orphan(self, *, keep_lease: bool = False) -> str:
        """Reconcile one persisted owner without adopting by PID."""
        acquired = self._registry_lease is None
        if acquired and not self._acquire_registry_lease():
            return "owner-unavailable"
        with self._lock:
            recovery_owner = self._recovery_owner
        outcome, recovery_owner = reconcile_startup_orphan(
            registry_path=self.registry_path,
            recovery_owner=recovery_owner,
            remove_registry=self._remove_registry,
            pid_alive=_pid_alive,
            pid_matches_compute_host=self._pid_matches_compute_host,
            terminate_pid=self._terminate_pid,
            shutdown_timeout_secs=_SHUTDOWN_TIMEOUT_SECS,
        )
        with self._lock:
            self._recovery_owner = recovery_owner
        if acquired and not keep_lease and outcome in {
            "none", "owner-unavailable-absent", "not-running", "pid-reuse-ignored", "terminated",
        }:
            if not self._release_registry_lease():
                return "owner-unavailable"
        return outcome

    def _make_pending_turn(
        self, request_id: str, payload: dict[str, Any], *, callback, retirement_lease,
        host_proc: subprocess.Popen[str] | None, host_generation: int, host_boot_id: str,
    ) -> _PendingTurn:
        return _PendingTurn(
            sid=str(payload.get("sid") or ""),
            session_key=str(payload.get("session_key") or ""),
            profile_home=str(payload.get("profile_home") or ""),
            turn_id=str(payload.get("turn_id") or request_id),
            host_generation=host_generation,
            host_boot_id=host_boot_id,
            host_proc=host_proc,
            callback=callback,
            retirement_lease=retirement_lease)

    def _record_pre_submit_refusal(
        self, request_id: str, pending: _PendingTurn, *, reason: str, message: str,
    ) -> bool:
        """Keep an exact lease when no child bytes were accepted and release is not confirmed."""
        pending.pre_submit = True
        pending.terminal = {
            "type": "turn.error", "sid": pending.sid, "request_id": request_id,
            "turn_id": pending.turn_id, "session_key": pending.session_key,
            "profile_home": pending.profile_home, "reason": reason,
            "message": message, "custody": "known"}
        pending.callback_succeeded = True
        pending.owner_settled = True  # no child was accepted; only exact release remains
        pending.lease_release_pending = True
        with self._lock:
            self._pending_turns[request_id] = pending
        return self._attempt_pending_release(request_id)

    def retain_pre_submit_lease(
        self, frame: dict[str, Any], lease: object, *, reason: str, message: str,
    ) -> bool:
        """Record a lease that lost the admission race before any child bytes were sent."""
        request_id = str(frame.get("request_id") or frame.get("turn_id") or uuid.uuid4().hex)
        with self._lock:
            pending = self._make_pending_turn(
                request_id, {**frame, "request_id": request_id}, callback=None,
                retirement_lease=lease, host_proc=self._proc,
                host_generation=self._host_generation, host_boot_id=self._host_boot_id)
        return self._record_pre_submit_refusal(request_id, pending, reason=reason, message=message)

    def submit_turn(self, frame: dict[str, Any], *, on_complete: Callable[[dict], object] | None = None) -> str:
        retirement_lease = getattr(on_complete, "_retirement_lease", None)
        request_id = str(frame.get("request_id") or uuid.uuid4().hex)
        sid = str(frame.get("sid") or "")
        payload = {**frame, "type": "turn.start", "request_id": request_id}

        def _refuse_start(exc: BaseException) -> None:
            if retirement_lease is None:
                raise exc
            with self._lock:
                pending = self._make_pending_turn(
                    request_id, payload, callback=on_complete, retirement_lease=retirement_lease,
                    host_proc=self._proc, host_generation=self._host_generation,
                    host_boot_id=self._host_boot_id)
            released = self._record_pre_submit_refusal(
                request_id, pending, reason="admission_refused", message=str(exc))
            if released:
                raise _AdmissionRefusedError(str(exc)) from exc
            unresolved = _AdmissionRefusedError(
                f"compute-host admission refused with unresolved lease release: {exc}")
            unresolved.lease_release_pending = True
            raise unresolved from exc

        try:
            self.start()
        except (OSError, RuntimeError, ValueError) as exc:
            _refuse_start(exc)
        with self._lock:
            host_boot_id = self._host_boot_id
            host_generation = self._host_generation
            host_proc = self._proc
            if self._closing or self._shutdown_admission_closed:
                _refuse_start(RuntimeError("compute host is closing; new turn refused"))
        if retirement_lease is not None and not host_boot_id:
            _refuse_start(RuntimeError("compute host did not provide a boot identity for a leased turn"))
        with self._lock:
            if self._closing or self._shutdown_admission_closed:
                _refuse_start(RuntimeError("compute host is closing; new turn refused"))
            pending = self._make_pending_turn(
                request_id, payload, callback=on_complete, retirement_lease=retirement_lease,
                host_proc=host_proc, host_generation=host_generation, host_boot_id=host_boot_id)
            self._pending_turns[request_id] = pending if retirement_lease is not None else (sid, on_complete)
        try:
            self._send_frame(payload)
        except _FrameSendError as exc:
            if retirement_lease is not None and exc.ambiguous:
                frame_unknown = {
                    "type": "turn.error", "sid": sid, "request_id": request_id,
                    "turn_id": pending.turn_id, "session_key": pending.session_key,
                    "profile_home": pending.profile_home, "host_boot_id": pending.host_boot_id,
                    "reason": "transport_unknown",
                    "message": str(exc), "custody": "unknown"}
                self._deliver_pending_terminal(
                    request_id, frame_unknown, proc=exc.proc,
                    host_generation=pending.host_generation)
                exc.custody_unknown = True
                raise
            with self._lock:
                removed = self._pending_turns.get(request_id)
            if isinstance(removed, _PendingTurn):
                self._record_pre_submit_refusal(
                    request_id, removed, reason="admission_refused", message=str(exc))
                refused = _AdmissionRefusedError(str(exc))
                refused.lease_release_pending = request_id in self._pending_turns
                raise refused from exc
            elif on_complete is not None:
                _call_logged(on_complete, {"type": "turn.error", "sid": sid,
                                           "request_id": request_id,
                                           "reason": "send_failed", "message": str(exc)},
                             "compute host turn refusal callback failed")
            raise
        except Exception as exc:
            with self._lock:
                removed = self._pending_turns.get(request_id)
            if isinstance(removed, _PendingTurn):
                self._record_pre_submit_refusal(
                    request_id, removed, reason="admission_refused", message=str(exc))
                refused = _AdmissionRefusedError(str(exc))
                refused.lease_release_pending = request_id in self._pending_turns
                raise refused from exc
            elif on_complete is not None:
                _call_logged(on_complete, {"type": "turn.error", "sid": sid,
                                           "request_id": request_id,
                                           "reason": "send_failed", "message": str(exc)},
                             "compute host turn refusal callback failed")
            raise
        return request_id

    def _start_for_existing_sid(self, sid: str) -> None:
        with self._lock:
            if self._closing:
                if (
                    self._proc is None
                    or self._proc.poll() is not None
                    or not self._has_pending_sid_locked(sid)
                ):
                    raise RuntimeError("compute host is closing; existing response refused")
                return
        self.start()

    def interrupt(self, sid: str, *, request_id: str | None = None) -> None:
        self._start_for_existing_sid(sid)
        self._send_frame(
            {"type": "interrupt", "sid": sid, "request_id": request_id or uuid.uuid4().hex},
            allow_existing_sid=sid,
        )

    def _has_pending_sid_locked(self, sid: str) -> bool:
        return any(_legacy_pending_fields(pending)[0] == sid
                   for pending in self._pending_turns.values())

    def has_pending_turn(self, sid: str) -> bool:
        """True when a host completion callback is still registered for ``sid``.

        Read by the serving process to tell "the host still owes this session a
        terminal frame" from "nothing will ever call back for it". Entries are
        added in :meth:`submit_turn` and removed by ``turn.end``/``turn.error``
        handling or :meth:`_fail_pending_turns`, all under ``self._lock``.
        """
        with self._lock:
            return any(_legacy_pending_fields(pending)[0] == sid
                       for pending in self._pending_turns.values())

    def _await_reply(
        self, frame: dict[str, Any], request_id: str, timeout: float,
        *, late_handler: Callable[[dict], None] | None = None,
        allow_existing_sid: str | None = None,
    ) -> dict:
        """Send ``frame`` and block for the host reply carrying ``request_id``."""
        q: queue.Queue[dict] = queue.Queue(maxsize=1)
        with self._lock:
            if (
                self._shutdown_admission_closed
                and (allow_existing_sid is None or not self._has_pending_sid_locked(allow_existing_sid))
            ):
                raise _AdmissionRefusedError("compute host shutdown admission is closed")
            self._pending_controls[request_id] = q
        timed_out = False
        try:
            if allow_existing_sid is None:
                self._send_frame(frame)
            else:
                self._send_frame(frame, allow_existing_sid=allow_existing_sid)
            return q.get(timeout=timeout)
        except queue.Empty:
            timed_out = True
            raise
        finally:
            with self._lock:
                self._pending_controls.pop(request_id, None)
                if timed_out:
                    self._register_late_control_handler_locked(request_id, late_handler)

    def respond(self, sid: str, params: dict[str, Any], *, timeout: float = 15.0) -> dict:
        """Deliver an interactive prompt response to the host that owns it."""
        self._start_for_existing_sid(sid)
        request_id = uuid.uuid4().hex
        frame = {"type": "respond", "sid": sid, "request_id": request_id, "params": dict(params)}
        return self._await_reply(frame, request_id, timeout, allow_existing_sid=sid)

    def reload_mcp(self, sid: str, *, request_id: str | None = None) -> dict:
        payload = {"type": "reload_mcp", "sid": sid, "request_id": request_id or uuid.uuid4().hex}
        return self.control(sid, route_name="reload.mcp", wait=True, payload=payload)

    def control(
        self, sid: str, *, route_name: str, payload: dict[str, Any] | None = None,
        wait: bool = True, timeout: float = 30.0, on_late_ack: Callable[[dict], None] | None = None,
    ) -> dict:
        """Send a control frame; with ``wait`` block up to ``timeout`` for its ack. ``on_late_ack``
        (only with ``wait``) keeps the request adoptable after the waiter gives up: the host's
        eventual ``control.ack``/``control.error``/``error`` fires it once (bounded by
        ``_LATE_CONTROL_TTL_SECS``/``_MAX``) instead of being dropped."""
        if route_name not in MUTATOR_ROUTE_TABLE:
            raise ValueError(f"unclassified host mutator route: {route_name}")
        self.start()
        payload = payload or {}
        request_id = str(payload.get("request_id") or uuid.uuid4().hex)
        frame = {"type": "control", **payload, "sid": sid, "route_name": route_name,
                 "request_id": request_id}
        if not wait:
            self._send_frame(frame)
            return {"status": "sent", "request_id": request_id}
        return self._await_reply(frame, request_id, timeout, late_handler=on_late_ack)

    def _register_late_control_handler_locked(
        self, request_id: str, handler: Callable[[dict], None] | None,
    ) -> None:
        now = time.monotonic()
        handlers = self._late_control_handlers
        for rid in [r for r, (at, _cb) in handlers.items() if now - at > _LATE_CONTROL_TTL_SECS]:
            handlers.pop(rid, None)
            self._late_control_uncertain = True
        while len(handlers) >= _LATE_CONTROL_MAX:
            handlers.pop(min(handlers, key=lambda rid: handlers[rid][0]), None)
            self._late_control_uncertain = True
        handlers[request_id] = (now, handler)

    def _register_late_control_handler(
        self, request_id: str, handler: Callable[[dict], None] | None,
    ) -> None:
        with self._lock:
            self._register_late_control_handler_locked(request_id, handler)

    def _deliver_control_frame(
        self, request_id: str, frame: dict[str, Any], *,
        proc: subprocess.Popen[str] | None = None, host_generation: int | None = None,
    ) -> None:
        with self._lock:
            if frame.get("type") == "shutdown.ack":
                identity = self._owner_identity
                producer_identity = self._producer_identity
                if (
                    request_id
                    and request_id == self._shutdown_request_id
                    and proc is self._proc
                    and host_generation == self._host_generation
                    and identity is not None
                    and producer_identity is not None
                    and (os.name != "nt" or (
                        self._producer_process_handle is not None
                        and self._producer_process_handle.is_open
                        and self._producer_creation_filetime is not None
                    ))
                    and frame.get("host_pid") == producer_identity[0]
                    and str(frame.get("host_boot_id") or "") == producer_identity[1]
                    and frame.get("drained") is True
                ):
                    self._shutdown_ack = dict(frame)
                else:
                    logger.warning("ignored foreign or incomplete compute-host shutdown ack")
                return
            q = self._pending_controls.get(request_id)
            late = None if q is not None else self._late_control_handlers.pop(request_id, None)
        if q is not None:
            with contextlib.suppress(queue.Full):
                q.put_nowait(frame)
        elif late is not None:
            if late[1] is not None:
                _call_logged(late[1], frame, f"compute host late control ack handler failed (request_id={request_id})")

    def _accept_producer_startup(
        self, record: dict[str, Any], *, proc: subprocess.Popen[str], host_generation: int,
    ) -> bool:
        return accept_producer_startup(
            self, record, proc=proc, host_generation=host_generation)

    def _spawn_locked(self, *, reason: str) -> None:
        if self._stopped_respawning:
            raise RuntimeError("compute host respawn disabled after crash loop")
        with self._hello_lock:
            if self._startup_binding is not None or self._startup_orphan_handles:
                raise RuntimeError("compute host startup custody is still retained")
        env = {**hermes_subprocess_env(inherit_credentials=True), **os.environ, **(self.env or {})}
        env["HERMES_COMPUTE_HOST_HEARTBEAT_SECS"] = str(self.heartbeat_secs)
        root = str(_repo_root())
        env.setdefault("PYTHONPATH", root)
        if root not in env["PYTHONPATH"].split(os.pathsep):
            env["PYTHONPATH"] = root + os.pathsep + env["PYTHONPATH"]
        completion_read_fd, completion_write_fd, completion_child_fd, completion_spawn = (
            _producer_completion_pipe()
        )
        env[_PRODUCER_COMPLETION_ENV] = completion_child_fd
        # Lossy UTF-8 decode: a locale-mismatched byte must not raise inside the drain threads.
        # Hold the handshake mutex across reset and publication of the new
        # Popen/generation pair.  A late old reader therefore cannot win the
        # hello CAS in the gap before the new process is published.
        with self._hello_lock:
            self._hello_event.clear()
            self._hello = {}
            self._host_boot_id = ""
            owner_job_name = new_windows_job_name() if os.name == "nt" else None
            try:
                proc, owner = spawn_server(
                    self.argv, cwd=str(self.cwd), env=env, stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                    encoding="utf-8", errors="replace", bufsize=1,
                    start_new_session=True, owner_job_name=owner_job_name,
                    **completion_spawn)
            except Exception:
                _close_fd(completion_read_fd)
                _close_fd(completion_write_fd)
                raise
            # The parent never writes this channel. Closing its copy is also
            # required for EOF to mean that the exact child generation stopped.
            _close_fd(completion_write_fd)
            self._host_generation += 1
            host_generation = self._host_generation
            self._proc = proc
            self._owner = owner
            self._owner_job_name = getattr(owner, "name", None) or owner_job_name
            self._registry_owner_key = None
            self._registry_record_removed = False
            self._owner_closed = False
            self._owner_identity = None
            self._producer_identity = None
            self._producer_process_handle = None
            self._producer_creation_filetime = None
            self._startup_event.clear()
            self._startup_identity = None
            self._startup_error = ""
            self._startup_seen = False
            self._startup_binding = None
            self._host_resource_custody_known = False
            self._shutdown_sent = False
            self._shutdown_request_id = None
            self._shutdown_ack = None
            self._shutdown_admission_closed = False
            self._pending_shutdown_receipt = None
            self._late_control_uncertain = False
            stdout_state = _StreamState()
            stderr_state = _StreamState()
            completion_state = _StreamState()
            self._stream_states[id(proc)] = (stdout_state, stderr_state, completion_state)
            exit_settled = threading.Event()
            self._exit_settled[id(proc)] = exit_settled
        from agent.memory_provider import spawn_context_thread
        spawn_context_thread(
            self._drain_stdout, name="compute-host-stdout",
            args=(proc, host_generation, stdout_state),
        ).start()
        spawn_context_thread(
            self._drain_producer_completion,
            args=(completion_read_fd, proc, host_generation, completion_state),
            name="compute-host-producer-completion",
        ).start()
        spawn_context_thread(
            self._drain_stderr, args=(proc, stderr_state), name="compute-host-stderr",
        ).start()
        spawn_context_thread(
            self._wait_for_exit, args=(proc, host_generation, exit_settled), name="compute-host-wait",
        ).start()
        deadline = time.monotonic() + _HELLO_TIMEOUT_SECS
        if not self._hello_event.wait(timeout=max(0.0, deadline - time.monotonic())):
            cancel_producer_startup(self, proc=proc, host_generation=host_generation)
            self._terminate_process(proc)
            self._release_owner()
            raise RuntimeError(f"compute host did not send hello; stderr={self._stderr_tail[-5:]}")
        if not self._startup_event.wait(timeout=max(0.0, deadline - time.monotonic())):
            cancel_producer_startup(self, proc=proc, host_generation=host_generation)
            self._terminate_process(proc)
            self._release_owner()
            raise RuntimeError("compute host did not send source-bound startup identity")
        try:
            self._validate_hello()
            with self._hello_lock:
                startup_identity = self._startup_identity
                startup_error = self._startup_error
                hello_pid = self._hello.get("host_pid")
                hello_boot_id = str(self._hello.get("boot_id") or "")
                if startup_error:
                    raise RuntimeError(startup_error)
                if startup_identity is None:
                    raise RuntimeError("startup:missing_identity")
                if hello_pid != startup_identity[0] or hello_boot_id != startup_identity[1]:
                    raise RuntimeError("startup:hello_identity_mismatch")
                self._host_boot_id = startup_identity[1]
                self._producer_identity = startup_identity
                self._owner_identity = (host_generation, startup_identity[1], int(proc.pid or 0))
        except BaseException:
            cancel_producer_startup(self, proc=proc, host_generation=host_generation)
            self._terminate_process(proc)
            self._release_owner()
            raise
        self._persist_registry()
        logger.info("compute host started pid=%s reason=%s", proc.pid, reason)

    def _validate_hello(self) -> None:
        hello = self._hello
        if not hello:
            raise RuntimeError("compute host missing hello")
        got_home = str(hello.get("hermes_home") or "")
        if got_home and got_home != self.expected_hermes_home:
            raise RuntimeError(
                f"compute host HERMES_HOME mismatch: {got_home} != {self.expected_hermes_home}")
        got_sha = str(hello.get("build_sha") or "")
        expected = self.expected_build_sha
        if expected != "unknown" and got_sha not in {"", "unknown", expected}:
            raise RuntimeError(f"compute host build mismatch: {got_sha} != {expected}")
        if type(hello.get("host_pid")) is not int or hello["host_pid"] <= 0:
            raise RuntimeError("compute host missing validated host_pid")
        if not str(hello.get("boot_id") or ""):
            raise RuntimeError("compute host missing validated boot_id")

    def _persist_registry(self) -> None:
        identity = self._owner_identity
        if identity is None:
            raise RuntimeError("cannot persist a host owner before hello validation")
        owner_kind = "windows_job" if self._owner is not None else "unowned"
        persist_owner_record(
            registry_path=self.registry_path, owner_identity=identity,
            owner_kind=owner_kind, build_sha=self._hello.get("build_sha") or "",
            argv=self.argv, owner_job_name=self._owner_job_name,
        )
        self._registry_owner_key = owner_record_key(
            owner_identity=identity, owner_kind=owner_kind, owner_job_name=self._owner_job_name)
        self._registry_record_removed = False

    def _remove_registry(self, expected: object | None = None) -> bool:
        if self._registry_lease is None and self.registry_path.exists():
            return False
        key = expected if expected is not None else self._registry_owner_key
        if key is not None and key == self._registry_owner_key and self._registry_record_removed:
            return not self.registry_path.exists()
        removed = remove_registry_record(self.registry_path, key)
        if removed and key is not None and key == self._registry_owner_key:
            self._registry_record_removed = True
        return removed

    def _send_frame(
        self, frame: dict[str, Any], *, allow_shutdown: bool = False,
        allow_existing_sid: str | None = None,
    ) -> None:
        try:
            payload = json.dumps(frame, separators=(",", ":"), ensure_ascii=False) + "\n"
        except Exception as exc:
            # No transport operation ran, so this is a proven pre-submit refusal.
            raise _FrameSendError(str(exc), ambiguous=False, proc=None) from exc
        with self._lock:
            existing_allowed = bool(
                allow_existing_sid is not None
                and frame.get("sid") == allow_existing_sid
                and frame.get("type") in {"respond", "interrupt"}
                and self._has_pending_sid_locked(allow_existing_sid)
            )
            if self._shutdown_admission_closed and not (allow_shutdown or existing_allowed):
                raise _FrameSendError(
                    "compute host shutdown admission is closed", ambiguous=False, proc=self._proc)
            proc = self._proc
            if proc is None or proc.poll() is not None or proc.stdin is None:
                raise _FrameSendError("compute host is not running", ambiguous=False, proc=proc)
            try:
                written = proc.stdin.write(payload)
                if written != len(payload):
                    raise _FrameSendError(
                        f"compute host short write: {written!r}/{len(payload)} characters",
                        ambiguous=True, proc=proc)
                proc.stdin.flush()
            except _FrameSendError:
                raise
            except Exception as exc:
                raise _FrameSendError(str(exc), ambiguous=True, proc=proc) from exc

    def _drain_stdout(
        self, proc: subprocess.Popen[str], host_generation: int | None = None,
        stream_state: _StreamState | None = None,
    ) -> None:
        state = stream_state or _StreamState()
        try:
            assert proc.stdout is not None
            while True:
                raw, oversized = _read_bounded_stdout_line(proc.stdout)
                if raw is None:
                    if oversized:
                        logger.warning("compute host stdout frame exceeded byte budget")
                        continue
                    break
                try:
                    frame = json.loads(raw)
                except json.JSONDecodeError:
                    logger.warning("compute host emitted invalid json")
                    continue
                if isinstance(frame, dict):
                    self._handle_host_frame(frame, proc=proc, host_generation=host_generation)
        except BaseException as exc:  # health: allow BLE001 -- arbitrary RPC sink failures must withhold EOF and preserve custody without logging values
            state.failed = True
            state.error = f"stdout:{type(exc).__name__}"
            logger.warning("compute host stdout reader failed; custody retained")
        else:
            state.eof = True
        finally:
            state.done.set()

    def _drain_producer_completion(
        self, read_fd: int, proc: subprocess.Popen[str], host_generation: int,
        stream_state: _StreamState | None = None,
    ) -> None:
        drain_producer_completion(
            self, read_fd, proc, host_generation, stream_state or _StreamState(),
            _PRODUCER_COMPLETION_MAX_RECORD_BYTES,
        )

    def _producer_completion_matches(
        self, request_id: str, pending: _PendingTurn, record: dict[str, Any],
        *, proc: subprocess.Popen[str], host_generation: int,
    ) -> bool:
        """Bind a completion record to the exact child Popen and accepted turn."""
        if not _valid_producer_completion_record(record):
            return False
        producer_identity = self._producer_identity
        if producer_identity is None:
            return False
        if os.name == "nt" and (
            self._producer_process_handle is None
            or not self._producer_process_handle.is_open
            or self._producer_creation_filetime is None
        ):
            return False
        if record.get("host_pid") != producer_identity[0]:
            return False
        if str(record.get("request_id") or "") != request_id:
            return False
        if str(record.get("sid") or "") != pending.sid:
            return False
        if str(record.get("turn_id") or "") != pending.turn_id:
            return False
        if str(record.get("session_key") or "") != pending.session_key:
            if str(record.get("origin_session_key") or "") != pending.session_key:
                return False
        if str(record.get("profile_home") or "") != pending.profile_home:
            return False
        if str(record.get("host_boot_id") or "") != producer_identity[1]:
            return False
        if pending.host_boot_id != producer_identity[1]:
            return False
        if pending.host_proc is not proc or pending.host_generation != host_generation:
            return False
        if self._proc is not None and self._proc is not proc:
            return False
        return True

    def _handle_producer_completion(
        self, record: dict[str, Any], *, proc: subprocess.Popen[str], host_generation: int,
    ) -> bool:
        """Open the turn callback gate only after the child producer has finalized."""
        request_id = str(record.get("request_id") or "")
        with self._lock:
            pending = self._pending_turns.get(request_id)
            if not isinstance(pending, _PendingTurn) or not self._producer_completion_matches(
                request_id, pending, record, proc=proc, host_generation=host_generation
            ):
                logger.warning("ignored invalid, stale, or foreign compute-host producer completion")
                return False
            pending.producer_settled = True
            pending.producer_completion = dict(record)
            pending.producer_terminal_digest = str(record["terminal_sha256"])
            terminal = pending.terminal_candidates.pop(pending.producer_terminal_digest, None)
            # Every other public candidate arrived before the source-bound receipt and
            # is quarantined. A later public frame must still match the exact digest.
            pending.terminal_candidates.clear()
            if terminal is None and pending.terminal is not None:
                if _terminal_digest(pending.terminal) == pending.producer_terminal_digest:
                    terminal = dict(pending.terminal)
                else:
                    pending.terminal = None
            if terminal is not None:
                pending.terminal = dict(terminal)
        if terminal is None:
            # The public turn.end can arrive after the control record on a
            # separately scheduled reader; the identity-bound state is enough
            # for that later stdout frame to open the callback gate.
            return True
        # This bool acknowledges the authenticated producer receipt, not the
        # downstream callback outcome. A failed callback retains the exact pending
        # record and lease while this channel stays open for a source-bound retry.
        self._deliver_pending_terminal(
            request_id, terminal, proc=proc, host_generation=host_generation,
            source_bound=True)
        return True

    def _drain_stderr(
        self, proc: subprocess.Popen[str], stream_state: _StreamState | None = None,
    ) -> None:
        state = stream_state or _StreamState()
        try:
            assert proc.stderr is not None
            for raw in proc.stderr:
                if text := raw.rstrip("\n"):
                    self._stderr_tail = (self._stderr_tail + [text])[-80:]
                    logger.warning("compute host stderr: %s", text)
        except BaseException as exc:  # health: allow BLE001 -- a failed stderr reader cannot prove EOF; retain custody without logging exception values
            state.failed = True
            state.error = f"stderr:{type(exc).__name__}"
            logger.warning("compute host stderr reader failed; custody retained")
        else:
            state.eof = True
        finally:
            state.done.set()

    def _pending_matches_frame(
        self, request_id: str, pending: _PendingTurn, frame: dict[str, Any],
        *, proc: subprocess.Popen[str] | None = None, host_generation: int | None = None,
    ) -> bool:
        if str(frame.get("request_id") or "") != request_id:
            return False
        if str(frame.get("sid") or "") != pending.sid:
            return False
        if proc is None or pending.host_proc is not proc:
            return False
        if host_generation is None or pending.host_generation != host_generation:
            return False
        # The wait thread may clear _proc just before the stdout reader drains
        # the child's final line. The pending record's exact Popen object plus
        # generation still authenticates that tail; a respawned current proc is rejected.
        if self._proc is not None and self._proc is not proc:
            return False
        if pending.turn_id and str(frame.get("turn_id") or "") != pending.turn_id:
            return False
        terminal_session_key = str(frame.get("session_key") or "")
        if pending.session_key and terminal_session_key != pending.session_key:
            # Context compression may rotate A -> B during this same accepted
            # turn. The child must carry the original dispatch key as an
            # authenticated predecessor; an arbitrary replacement key is foreign.
            if str(frame.get("origin_session_key") or "") != pending.session_key:
                return False
        if pending.profile_home and str(frame.get("profile_home") or "") != pending.profile_home:
            return False
        if not pending.host_boot_id or str(frame.get("host_boot_id") or "") != pending.host_boot_id:
            return False
        return True

    def _attempt_pending_release(self, request_id: str) -> bool:
        """Release a settled exact lease once; retain the record on false/raise for retry."""
        with self._lock:
            pending = self._pending_turns.get(request_id)
            if not isinstance(pending, _PendingTurn) or not pending.callback_succeeded:
                return False
            if pending.finalizing or pending.terminal is None:
                return False
            if not pending.pre_submit:
                if pending.terminal.get("custody") == "unknown" or not pending.producer_settled:
                    return False
            pending.finalizing = True
            lease = pending.retirement_lease
        try:
            released = _release_retirement_lease(lease)
        except Exception:
            logger.exception("compute host retirement lease release failed")
            released = False
        with self._lock:
            current = self._pending_turns.get(request_id)
            if current is not pending:
                return released
            pending.finalizing = False
            pending.lease_release_pending = not released
            if released:
                self._pending_turns.pop(request_id, None)
                return True
        return False

    def retry_pending_release(
        self, request_id: str, *, sid: str, turn_id: str, session_key: str, profile_home: str,
        host_generation: int,
    ) -> bool:
        """Retry one exact record after a release failure; identity is checked under the lock."""
        with self._lock:
            pending = self._pending_turns.get(request_id)
            if (not isinstance(pending, _PendingTurn)
                    or pending.sid != sid or pending.turn_id != turn_id
                    or pending.session_key != session_key or pending.profile_home != profile_home
                    or pending.host_generation != host_generation):
                return False
        return self._attempt_pending_release(request_id)

    def settle_process_owner(
        self, request_id: str, *, proc: subprocess.Popen[str], host_generation: int,
        sid: str, turn_id: str, session_key: str, profile_home: str,
    ) -> bool:
        """Accept a later real process-owner settlement, never a terminal frame.

        The current ab6 compute-host producer has no authenticated OS/process-owner receipt for
        inherited descendants. Until an existing owner supplies this exact Popen/generation/turn
        binding after joining or accounting for its descendants, this method is intentionally
        never called by Gate 1's production path. An explicit real owner receipt
        remains a stronger legacy compatibility path for this turn, but it does
        not settle the host-wide capture/resource state.
        """
        with self._lock:
            pending = self._pending_turns.get(request_id)
            if (not isinstance(pending, _PendingTurn)
                    or pending.host_proc is not proc or pending.host_generation != host_generation
                    or pending.sid != sid or pending.turn_id != turn_id
                    or pending.session_key != session_key or pending.profile_home != profile_home):
                return False
            if pending.terminal is not None and pending.terminal.get("custody") == "unknown":
                return False
            pending.owner_settled = True
            # A real owner receipt is stronger than the producer record and
            # remains a valid legacy way to open this turn. It does not mark
            # the host-wide resource/capture state settled.
            pending.producer_settled = True
            terminal = dict(pending.terminal) if pending.terminal is not None else None
        if terminal is None:
            return True
        # Re-enter the same identity-checked delivery path so the callback and
        # queued handoff happen only after the owner receipt, never merely because
        # the child wrote a matching JSON frame.
        return self._deliver_pending_terminal(
            request_id, terminal, proc=proc, host_generation=host_generation,
            source_bound=True)

    @staticmethod
    def _remember_terminal_candidate(
        pending: _PendingTurn, frame: dict[str, Any], frame_digest: str,
    ) -> None:
        if not frame_digest:
            return
        pending.terminal_candidates[frame_digest] = dict(frame)
        while len(pending.terminal_candidates) > _MAX_TERMINAL_CANDIDATES:
            pending.terminal_candidates.pop(next(iter(pending.terminal_candidates)))

    @staticmethod
    def _producer_digest_matches(pending: _PendingTurn, frame_digest: str) -> bool:
        return (
            pending.pre_submit
            or pending.producer_terminal_digest is None
            or pending.producer_terminal_digest == frame_digest
        )

    @staticmethod
    def _replace_unknown_terminal(
        pending: _PendingTurn, frame: dict[str, Any],
    ) -> bool:
        if (
            not pending.pre_submit
            and pending.terminal is not None
            and pending.terminal.get("custody") == "unknown"
        ):
            # A source-bound receipt proves producer finalization only; it does
            # not convert an unresolved transport/process-owner handoff into a
            # callback-safe terminal.
            pending.terminal = dict(frame)
            pending.unknown_notified = True
            return True
        return False

    def _stage_pending_terminal(
        self, request_id: str, frame: dict[str, Any], *, frame_digest: str,
        proc: subprocess.Popen[str] | None, host_generation: int | None,
        source_bound: bool,
    ) -> dict[str, Any]:
        """Stage one identity-checked terminal under the pending-turn lock."""
        with self._lock:
            pending = self._pending_turns.get(request_id)
            if pending is None:
                return {"kind": "missing"}
            if not isinstance(pending, _PendingTurn):
                self._pending_turns.pop(request_id, None)
                return {"kind": "legacy", "callback": _legacy_pending_fields(pending)[1]}
            if not self._pending_matches_frame(
                request_id, pending, frame, proc=proc, host_generation=host_generation,
            ):
                logger.warning("ignored stale or foreign compute-host terminal request_id=%s", request_id)
                return {"kind": "rejected"}
            if pending.finalizing:
                if source_bound and (
                    pending.producer_terminal_digest is None
                    or frame_digest == pending.producer_terminal_digest
                ):
                    pending.retry_terminal = dict(frame)
                return {"kind": "rejected"}
            if not pending.pre_submit and not pending.producer_settled:
                # Public frames are only candidates until the child-owned receipt
                # supplies the digest of the actual finalized terminal.
                self._remember_terminal_candidate(pending, frame, frame_digest)
                return {"kind": "rejected"}
            if not self._producer_digest_matches(pending, frame_digest):
                logger.warning("ignored terminal with non-matching producer digest")
                return {"kind": "rejected"}
            if self._replace_unknown_terminal(pending, frame):
                return {"kind": "rejected"}
            if pending.producer_settled and pending.terminal is not None and not source_bound:
                # A public duplicate after the source-bound producer receipt cannot
                # retry a failed callback or release. Only the same dedicated child
                # channel (or the explicit owner receipt) may replay that exact terminal.
                return {"kind": "rejected"}
            if pending.callback_succeeded:
                if pending.producer_terminal_digest != frame_digest:
                    return {"kind": "rejected"}
                pending.retry_terminal = None
                return {"kind": "retry_release", "pending": pending}
            # The pre-submit candidate check above intentionally waits for the
            # producer receipt. Once a digest exists, the final callback path must
            # still enforce it even for a pre-submit frame.
            if (
                pending.producer_terminal_digest is not None
                and pending.producer_terminal_digest != frame_digest
            ):
                return {"kind": "rejected"}
            pending.terminal = dict(frame)
            pending.finalizing = True
            return {
                "kind": "callback", "pending": pending,
                "callback": pending.callback, "terminal": dict(pending.terminal),
            }

    def _deliver_pending_terminal(
        self, request_id: str, frame: dict[str, Any], *,
        proc: subprocess.Popen[str] | None = None, host_generation: int | None = None,
        source_bound: bool = False,
    ) -> bool:
        """Deliver one correlated terminal frame, retaining unknown or failed custody."""
        replayed_retry = False
        frame_digest = _terminal_digest(frame)
        while True:
            decision = self._stage_pending_terminal(
                request_id, frame, frame_digest=frame_digest,
                proc=proc, host_generation=host_generation, source_bound=source_bound,
            )
            kind = decision["kind"]
            if kind == "missing":
                return False
            if kind == "legacy":
                if decision["callback"] is not None:
                    _call_logged(
                        decision["callback"], frame,
                        "compute host turn completion callback failed",
                    )
                return True
            if kind == "rejected":
                return False
            if kind == "retry_release":
                return self._attempt_pending_release(request_id)
            pending = decision["pending"]
            callback = decision["callback"]
            terminal = decision["terminal"]
            callback_succeeded = False
            if callback is not None:
                try:
                    callback_succeeded = callback(terminal) is True
                except Exception:  # health: allow BLE001 -- provider/session callback is opaque; failure leaves terminal custody unresolved
                    logger.warning("compute host turn completion callback failed; retaining custody")
            with self._lock:
                current = self._pending_turns.get(request_id)
                if current is not pending:
                    return False
                pending.finalizing = False
                if callback_succeeded:
                    pending.callback_succeeded = True
                if pending.terminal and pending.terminal.get("custody") == "unknown":
                    pending.unknown_notified = True
                retry_available = pending.retry_terminal is not None
                if retry_available:
                    pending.terminal = dict(pending.retry_terminal)
                    pending.retry_terminal = None
            if callback_succeeded:
                if pending.terminal and pending.terminal.get("custody") == "unknown":
                    return False
                # The turn lease is settled at the producer boundary. The host/resource
                # owner is deliberately not marked settled by this gate.
                return self._attempt_pending_release(request_id)
            if retry_available and not replayed_retry:
                replayed_retry = True
                continue
            return False

    def _handle_host_frame(
        self, frame: dict[str, Any], *, proc: subprocess.Popen[str] | None = None,
        host_generation: int | None = None,
    ) -> None:
        ftype = str(frame.get("type") or "")
        request_id = str(frame.get("request_id") or "")
        if ftype in _CONTROL_REPLY_TYPES or (ftype == "error" and request_id):
            self._deliver_control_frame(
                request_id, frame, proc=proc, host_generation=host_generation)
        elif ftype == "hello":
            # ``_spawn_locked`` waits for this event while holding ``_lock``;
            # never acquire that lifecycle lock from the reader thread.
            with self._hello_lock:
                # Only the reader that belongs to the current spawn may complete the
                # handshake. A late old reader must never replace the new boot identity.
                if (proc is None or host_generation is None
                        or proc is not self._proc or host_generation != self._host_generation
                        or self._hello_event.is_set()):
                    logger.warning("ignored stale or foreign compute-host hello")
                    return
                self._hello = dict(frame)
                self._hello_event.set()
        elif ftype == "hb":
            self._last_progress_counter = int(frame.get("progress_counter") or self._last_progress_counter)
            logger.debug("compute host heartbeat: %s", frame)
        elif ftype == "rpc":
            if isinstance(frame.get("message"), dict):
                self.rpc_sink(frame["message"])
        elif ftype in ("turn.end", "turn.error"):
            self._deliver_pending_terminal(request_id, frame, proc=proc,
                                            host_generation=host_generation)

    def _wait_for_exit(
        self, proc: subprocess.Popen[str], host_generation: int | None = None,
        exit_settled: threading.Event | None = None,
    ) -> None:
        event = exit_settled or self._exit_settled.get(id(proc))
        try:
            while True:
                try:
                    code = proc.wait(timeout=1.0)
                    break
                except subprocess.TimeoutExpired:
                    continue
            closing = False
            owner = None
            with self._lock:
                if self._proc is not proc:
                    return
                with self._hello_lock:
                    # Pair the process clear with the hello CAS so a stale reader
                    # cannot publish after this generation is retired.
                    if self._proc is not proc:
                        return
                    closing = self._closing
                    if not closing:
                        owner = self._owner
            if not closing:
                cancel_producer_startup(
                    self, proc=proc, host_generation=host_generation or self._host_generation)
            owner_closed = True
            producer_closed = True
            if not closing:
                stream_states = self._stream_states.get(id(proc))
                readers_settled = self._readers_settled(
                    stream_states, _SHUTDOWN_TIMEOUT_SECS)
                if readers_settled:
                    producer_closed = self._try_close_producer_handle()
                    owner_closed = self._owner_closed or self._try_close_owner(owner)
                else:
                    owner_closed = producer_closed = False
                registry_removed = (
                    owner_closed and producer_closed
                    and self._remove_registry(self._registry_owner_key)
                )
                with self._lock:
                    if owner_closed:
                        self._owner_closed = True
                    lease_released = registry_removed and self._release_registry_lease()
                    cleanup_complete = registry_removed and lease_released
                    if self._proc is proc and cleanup_complete:
                        self._proc = None
                        self._owner = None
                        self._owner_job_name = None
                        self._owner_closed = False
                        self._owner_identity = None
                        self._producer_identity = None
                        self._registry_owner_key = None
                        self._registry_record_removed = False
                        self._host_resource_custody_known = False
                        self._stream_states.pop(id(proc), None)
                if not cleanup_complete:
                    owner_closed = False
            reason = "shutdown" if closing else "crash"
            self._fail_pending_turns(
                reason=reason, message=f"compute host exited with code {code}",
                proc=proc, host_generation=host_generation)
            if not closing and owner_closed and producer_closed:
                self._maybe_respawn_after_crash()
        finally:
            if event is not None:
                event.set()

    def _fail_pending_turns(
        self, *, reason: str, message: str, proc: subprocess.Popen[str] | None = None,
        host_generation: int | None = None,
    ) -> None:
        with self._lock:
            pending = list(self._pending_turns.items())
        failure = {"reason": reason, "message": message}
        for request_id, record in pending:
            sid, cb = _legacy_pending_fields(record)
            self.rpc_sink({"jsonrpc": "2.0", "method": "event",
                           "params": {"type": "error", "session_id": sid, "payload": dict(failure)}})
            if isinstance(record, _PendingTurn):
                frame = {
                    "type": "turn.error", "sid": sid, "request_id": request_id,
                    "turn_id": record.turn_id, "session_key": record.session_key,
                    "profile_home": record.profile_home, "host_boot_id": record.host_boot_id, **failure,
                    "custody": "unknown"}
                self._deliver_pending_terminal(
                    request_id, frame, proc=proc or record.host_proc,
                    host_generation=host_generation if host_generation is not None else record.host_generation)
            else:
                with self._lock:
                    self._pending_turns.pop(request_id, None)
                if cb is not None:
                    _call_logged(cb, {"type": "turn.error", "sid": sid,
                                      "request_id": request_id, **failure},
                                 "compute host error callback failed")
        # A crashed host never emits the late acks timed-out control waiters still expect; fail
        # them too so the client's "still running" notice can't hang.
        with self._lock:
            late = self._late_control_handlers
            self._late_control_handlers = {}
        for request_id, (_registered_at, handler) in late.items():
            frame = {"type": "control.error", "request_id": request_id, **failure}
            if handler is not None:
                _call_logged(handler, frame, "compute host late control error handler failed")

    def _maybe_respawn_after_crash(self) -> None:
        now = time.monotonic()
        self._restart_times = [t for t in self._restart_times if now - t <= _RESPAWN_WINDOW_SECS]
        if len(self._restart_times) >= self.respawn_max:
            self._stopped_respawning = True
            logger.error(
                "compute host crash loop: max %s restarts per 5min reached; not respawning",
                self.respawn_max)
            return
        self._restart_times.append(now)
        # Small bounded backoff; tests and first recovery stay quick.
        delay = min(5.0, 0.25 * (2 ** max(0, len(self._restart_times) - 1)))

        def _respawn() -> None:
            time.sleep(delay)
            with self._lock:
                if self._closing or self._stopped_respawning or self._proc is not None:
                    return
                if self._registry_lease is None and not self._acquire_registry_lease():
                    return
                try:
                    self._spawn_locked(reason="crash")
                except Exception:
                    logger.exception("compute host respawn failed")
        threading.Thread(target=_respawn, name="compute-host-respawn", daemon=True).start()

    _pid_matches_compute_host = staticmethod(is_compute_host_identity)

    def _terminate_pid(self, pid: int, *, timeout: float = _SHUTDOWN_TIMEOUT_SECS) -> None:
        if not _signal_pid(pid, signal.SIGTERM, "SIGTERM"):
            return
        deadline = time.monotonic() + timeout
        while _pid_alive(pid):
            if time.monotonic() >= deadline:
                _signal_pid(pid, signal.SIGKILL, "SIGKILL")
                return
            time.sleep(0.05)

    def _terminate_process(self, proc: subprocess.Popen[str]) -> None:
        if proc.poll() is not None:
            return
        with contextlib.suppress(Exception):
            proc.terminate()
            proc.wait(timeout=_SHUTDOWN_TIMEOUT_SECS)
            return
        for step in (proc.kill, lambda: proc.wait(timeout=2)):
            with contextlib.suppress(Exception):
                step()


__all__ = ["MUTATOR_ROUTE_TABLE", "HostSupervisor", "append_log_record", "is_compute_host_identity"]
