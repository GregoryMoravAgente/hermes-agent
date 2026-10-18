"""Bounded stream custody helpers for HostSupervisor."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import logging
import os
import threading
import time
from typing import Any

from agent.memory_provider import spawn_context_thread
from tui_gateway.windows_producer_identity import PRODUCER_STARTUP_TYPE

logger = logging.getLogger(__name__)


@dataclass
class ShutdownWriterState:
    """One process/request-bound shutdown write that may remain in the OS pipe."""

    proc: Any
    request_id: str
    done: threading.Event = field(default_factory=threading.Event)
    sent: bool = False
    error_type: str | None = None
    ambiguous: bool = True
    thread: threading.Thread | None = None


@dataclass(frozen=True)
class ShutdownWriterObservation:
    sent: bool = False
    pending: bool = False
    error_type: str | None = None
    ambiguous: bool = True


@dataclass(frozen=True)
class FrameWriteObservation:
    sent: bool = False
    message: str = ""
    ambiguous: bool = False
    proc: Any = None


def transport_write_pending(supervisor: Any, proc: Any) -> bool:
    with supervisor._lock:
        return supervisor._transport_write_identity == (proc, supervisor._host_generation)


def send_host_frame(
    supervisor: Any, frame: dict[str, Any], *, allow_shutdown: bool = False,
    allow_existing_sid: str | None = None, expected_proc: Any = None,
) -> FrameWriteObservation:
    """Serialize and write one frame under the transport lock, outside lifecycle state."""
    try:
        payload = json.dumps(frame, separators=(",", ":"), ensure_ascii=False) + "\n"
    except (TypeError, ValueError) as exc:
        return FrameWriteObservation(message=str(exc))
    with supervisor._transport_write_lock:
        with supervisor._lock:
            existing_allowed = bool(
                allow_existing_sid is not None
                and frame.get("sid") == allow_existing_sid
                and frame.get("type") in {"respond", "interrupt"}
                and supervisor._has_pending_sid_locked(allow_existing_sid)
            )
            if supervisor._shutdown_admission_closed and not (allow_shutdown or existing_allowed):
                return FrameWriteObservation(
                    message="compute host shutdown admission is closed", proc=supervisor._proc,
                )
            proc = supervisor._proc
            if expected_proc is not None and proc is not expected_proc:
                return FrameWriteObservation(
                    message="compute host process changed", proc=proc,
                )
            if proc is None or proc.poll() is not None or proc.stdin is None:
                return FrameWriteObservation(
                    message="compute host is not running", proc=proc,
                )
            stdin = proc.stdin
            write_identity = (proc, supervisor._host_generation)
            supervisor._transport_write_identity = write_identity
        try:
            written = stdin.write(payload)
            if written != len(payload):
                return FrameWriteObservation(
                    message=f"compute host short write: {written!r}/{len(payload)} characters",
                    ambiguous=True, proc=proc,
                )
            stdin.flush()
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            return FrameWriteObservation(
                message=str(exc), ambiguous=True, proc=proc,
            )
        finally:
            with supervisor._lock:
                if supervisor._transport_write_identity == write_identity:
                    supervisor._transport_write_identity = None
    return FrameWriteObservation(sent=True, proc=proc)


def _matching_shutdown_writer(
    supervisor: Any, *, proc: Any, request_id: str | None,
) -> ShutdownWriterState | None:
    with supervisor._lock:
        state = supervisor._shutdown_send_state
        if (
            state is None or state.proc is not proc
            or state.request_id != request_id
        ):
            return None
        return state


def shutdown_writer_pending(
    supervisor: Any, *, proc: Any, request_id: str | None,
) -> bool:
    """Tell callers whether the exact process/request writer still owns the pipe."""
    state = _matching_shutdown_writer(supervisor, proc=proc, request_id=request_id)
    return bool(state is not None and not state.done.is_set())


def shutdown_writer_window(
    supervisor: Any, *, proc: Any, request_id: str | None, timeout: float,
) -> tuple[bool, float]:
    """Use no second process wait when the same writer is already unresolved."""
    deadline = time.monotonic() + max(0.0, float(timeout))
    pending = shutdown_writer_pending(supervisor, proc=proc, request_id=request_id)
    return pending, time.monotonic() if pending else deadline


def shutdown_writer_result(
    supervisor: Any, *, proc: Any, request_id: str | None, error: str,
) -> dict[str, bool]:
    """Classify the exact writer state without losing a late successful ACK."""
    state = _matching_shutdown_writer(supervisor, proc=proc, request_id=request_id)
    if state is None:
        return {
            "transport_writer_pending": False,
            "transport_writer_sent": False,
            "transport_writer_ambiguous": error == "shutdown_send_ambiguous",
        }
    if not state.done.is_set():
        return {
            "transport_writer_pending": True,
            "transport_writer_sent": False,
            "transport_writer_ambiguous": False,
        }
    if state.sent:
        return {
            "transport_writer_pending": False,
            "transport_writer_sent": True,
            "transport_writer_ambiguous": False,
        }
    return {
        "transport_writer_pending": False,
        "transport_writer_sent": False,
        "transport_writer_ambiguous": bool(
            state.ambiguous or error == "shutdown_send_ambiguous"
        ),
    }


def send_shutdown_frame_until(
    supervisor: Any, frame: dict[str, Any], *, proc: Any, request_id: str, deadline: float,
) -> ShutdownWriterObservation:
    """Start or observe one exact shutdown write without dropping its custody.

    A second observer never starts a second writer while the first is unresolved. The
    writer validates the same Popen object again immediately before the OS write, so a
    late old generation cannot publish a shutdown frame into a replacement process.
    """
    created = False
    with supervisor._lock:
        state = supervisor._shutdown_send_state
        if state is not None:
            same_writer = state.proc is proc and state.request_id == request_id
            if not state.done.is_set():
                return ShutdownWriterObservation(pending=True)
            if same_writer and state.sent:
                return ShutdownWriterObservation(sent=True)
            # A completed ambiguous writer may have crossed the OS boundary. Retain
            # that refusal instead of starting a duplicate shutdown frame. Only a
            # proven pre-submit thread-start failure is retryable.
            if state.ambiguous:
                return ShutdownWriterObservation(
                    error_type=state.error_type or "ShutdownWriterError", ambiguous=True,
                )
            state = None
        if state is None:
            state = ShutdownWriterState(proc=proc, request_id=request_id)

            def send() -> None:
                try:
                    supervisor._send_frame(
                        frame, allow_shutdown=True, expected_proc=proc,
                    )
                except (OSError, RuntimeError, TypeError, ValueError) as exc:
                    state.error_type = type(exc).__name__
                    state.ambiguous = bool(getattr(exc, "ambiguous", True))
                else:
                    state.sent = True
                    # A complete write/flush is no longer ambiguous.  ACK and
                    # registry settlement remain separate custody gates, but a
                    # cleanly completed writer must not permanently block the
                    # next generation after that receipt is acknowledged.
                    state.ambiguous = False
                    with supervisor._lock:
                        if (supervisor._proc is proc
                                and supervisor._shutdown_request_id == request_id):
                            supervisor._shutdown_sent = True
                finally:
                    state.done.set()

            state.thread = spawn_context_thread(
                send, name="compute-host-shutdown-writer", daemon=False,
            )
            supervisor._shutdown_send_state = state
            created = True
            try:
                state.thread.start()
            except RuntimeError as exc:  # failed thread creation never crossed the pipe
                state.error_type = type(exc).__name__
                state.ambiguous = False
                state.done.set()

    if not created and not state.done.is_set():
        return ShutdownWriterObservation(pending=True)
    if not state.done.wait(timeout=max(0.0, deadline - time.monotonic())):
        return ShutdownWriterObservation(pending=True)
    if state.sent:
        return ShutdownWriterObservation(sent=True)
    return ShutdownWriterObservation(
        error_type=state.error_type or "ShutdownWriterError",
        ambiguous=state.ambiguous,
    )


def readers_settled(
    states: tuple[Any, Any, Any] | None, timeout: float,
) -> bool:
    """Prove every required reader reached EOF without a failure."""
    if states is None:
        return False
    deadline = time.monotonic() + max(0.0, timeout)
    for state in states:
        if not state.done.wait(timeout=max(0.0, deadline - time.monotonic())):
            return False
    return all(state.eof and not state.failed for state in states)


def _consume_producer_completion_record(
    supervisor: Any, record: dict[str, Any], *, proc: Any, host_generation: int,
) -> str | None:
    if record.get("type") == PRODUCER_STARTUP_TYPE:
        if supervisor._accept_producer_startup(
            record, proc=proc, host_generation=host_generation,
        ):
            return None
        with supervisor._hello_lock:
            return supervisor._startup_error or "startup:rejected"
    if supervisor._handle_producer_completion(
        record, proc=proc, host_generation=host_generation
    ):
        return None
    return "completion:unmatched_record"


def _consume_record_line(
    supervisor: Any, raw: bytes, *, proc: Any, host_generation: int,
    max_record_bytes: int,
) -> str | None:
    if len(raw) > max_record_bytes:
        logger.warning("compute host producer completion record too large")
        return "completion:record_too_large"
    try:
        record = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError):
        logger.warning("compute host emitted invalid producer completion")
        return "completion:invalid_record"
    if not isinstance(record, dict):
        logger.warning("compute host producer completion must be an object")
        return "completion:non_object"
    return _consume_producer_completion_record(
        supervisor, record, proc=proc, host_generation=host_generation)


def _consume_buffer(
    supervisor: Any, buffer: bytearray, *, proc: Any, host_generation: int,
    max_record_bytes: int,
) -> tuple[bytearray, str | None]:
    while b"\n" in buffer:
        raw, _, remainder = buffer.partition(b"\n")
        buffer = bytearray(remainder)
        record_error = _consume_record_line(
            supervisor, raw, proc=proc, host_generation=host_generation,
            max_record_bytes=max_record_bytes)
        if record_error is not None:
            logger.warning("compute host producer record was rejected")
            return buffer, record_error
    return buffer, None


def drain_producer_completion(
    supervisor: Any, read_fd: int, proc: Any, host_generation: int,
    stream_state: Any, max_record_bytes: int,
) -> None:
    """Read one exact child completion channel and retain custody on any failure."""
    state = stream_state
    try:
        with os.fdopen(read_fd, "rb", buffering=0) as stream:
            buffer = bytearray()
            while True:
                chunk = stream.read(4096)
                if not chunk:
                    break
                buffer.extend(chunk)
                buffer, record_error = _consume_buffer(
                    supervisor, buffer, proc=proc, host_generation=host_generation,
                    max_record_bytes=max_record_bytes)
                if record_error is not None:
                    state.failed = True
                    state.error = record_error
                    return
                if len(buffer) > max_record_bytes:
                    logger.warning("compute host producer completion record too large")
                    state.failed = True
                    state.error = "completion:partial_record_too_large"
                    return
            if buffer:
                logger.warning("compute host producer completion ended with partial record")
                state.failed = True
                state.error = "completion:partial_record"
                return
    except (OSError, RuntimeError, TypeError, ValueError, LookupError, AttributeError) as exc:
        state.failed = True
        state.error = f"completion:{type(exc).__name__}"
        logger.warning("compute host producer completion reader failed; custody retained")
    else:
        with supervisor._hello_lock:
            if supervisor._proc is proc and not supervisor._startup_event.is_set():
                supervisor._startup_error = "startup:missing_record"
                supervisor._startup_event.set()
        state.eof = True
    finally:
        with supervisor._hello_lock:
            if supervisor._proc is proc and not supervisor._startup_event.is_set():
                supervisor._startup_error = "startup:missing_record"
                supervisor._startup_event.set()
        state.done.set()
