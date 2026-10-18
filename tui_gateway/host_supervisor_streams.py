"""Bounded stream custody helpers for HostSupervisor."""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from tui_gateway.windows_producer_identity import PRODUCER_STARTUP_TYPE

logger = logging.getLogger(__name__)


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
    except BaseException as exc:  # health: allow BLE001 -- opaque completion reader failure withholds EOF and values
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
