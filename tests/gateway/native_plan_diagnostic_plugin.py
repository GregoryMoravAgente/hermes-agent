"""Test-only, single-thread I10/native-plan observations for R12.

This plugin is deliberately external to the frozen R11 worktree.  It wraps only the
two non-producer entrypoints that are under diagnosis and calls the saved functions
unchanged.  The trace is installed only on the thread executing one wrapper and is
restored in ``finally``; it never installs a process-wide/threading trace.
"""

from __future__ import annotations

from contextlib import contextmanager
from functools import wraps
import json
import sys
import threading
from typing import Any, Iterator

import pytest


_TRACE_STATE = threading.local()

# Do not add source producer functions here.  Registry/probe callables must retain
# their exact identity; the trace observes their return/exception events without
# replacing them.
_TRACE_TARGETS = {
    "run_runtime.py": {"_seal_i10_preservation_owner"},
    "session_recovery_custody.py": {
        "_current_epoch", "_physical_owner", "refresh_i10_source_evidence",
        "observe_consumer", "observe_consumer_rows", "owner_ready", "owner_evidence",
        "unused_consumer",
    },
    "session_recovery_custody_sources.py": {
        "source_registry", "close_fence_current", "close_witness",
        "refresh_source_evidence", "attach_close_witness", "valid_close_witness",
    },
    "run_startup.py": {"i10_source_snapshot"},
    "session_local_recovery.py": {"i10_source_snapshot"},
    "webhook_ingress.py": {"i10_source_snapshot"},
    "run_adapters_recovery.py": {"i10_source_snapshot"},
    "run_adapters.py": {"i10_source_snapshot"},
    "run_notifications.py": {"i10_source_snapshot"},
    "run_notifications_recovery.py": {"i10_source_snapshot"},
    "session_bot_mailbox.py": {"i10_source_snapshot"},
    "runtime_native_cleanup_profile.py": {
        "profile_bindings_match", "profile_participants_match_witness",
        "_witness_homes_match_bindings", "_source_evidence_valid", "_participant_bound",
        "_records_valid", "_record_identity_valid", "_source_profile_valid",
        "_provider_entries_valid", "_provider_entry_valid",
        "profile_native_cleanup_witness_ready", "_header_valid",
    },
    "runtime_native_cleanup_sources.py": {
        "build_native_cleanup_plan", "_hosted_tui_prerequisite",
        "_capture_profile_native_cleanup_witness", "_source_prerequisites",
        "_valid_source_evidence", "_profile_host_evidence", "_host_producer_evidence",
        "_room_producer_evidence", "_room_terminal_identity_matches",
        "_registry_identity_valid", "_provider_participants", "_profile_participants",
        "_reserved_profile_bindings", "_profile_handle",
        "_profile_lifecycle_prerequisite", "_owner_is_sealed", "_cron_participant",
        "_profile_evidence_matches", "_profile_native_cleanup_witness_ready",
        "_identity_matches", "_validate_finish_context",
    },
}

_KNOWN_CONSUMERS = frozenset({
    "startup", "local_recovery", "webhook_replay", "adapter_reconnect",
    "completion_notification", "bot_mailbox",
})
_PROBE_FILE_CONSUMER = {
    "run_startup.py": "startup",
    "session_local_recovery.py": "local_recovery",
    "webhook_ingress.py": "webhook_replay",
    "run_adapters_recovery.py": "adapter_reconnect",
    "run_adapters.py": "adapter_reconnect",
    "run_notifications.py": "completion_notification",
    "run_notifications_recovery.py": "completion_notification",
    "session_bot_mailbox.py": "bot_mailbox",
}


def _basename(filename: str) -> str:
    """Cheap cross-platform basename for the hot trace callback."""
    return filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]


def _consumer_label(frame) -> str | None:
    filename = _basename(frame.f_code.co_filename)
    label = _PROBE_FILE_CONSUMER.get(filename)
    if label in _KNOWN_CONSUMERS:
        return label
    # ``consumer`` is a reviewed enum-like label in the shared close helpers.  Do
    # not emit any other local value (especially IDs, paths, or receipt contents).
    candidate = frame.f_locals.get("consumer")
    return candidate if candidate in _KNOWN_CONSUMERS else None


def _pending_count(value: Any) -> int | None:
    if not isinstance(value, dict):
        return None
    pending = value.get("pending_ids")
    if isinstance(pending, (tuple, list, set, frozenset)):
        return len(pending)
    pending = value.get("pending_count")
    return pending if type(pending) is int and pending >= 0 else None


def _result_kind(value: Any) -> str:
    if value is None:
        return "none"
    if value is True:
        return "true"
    if value is False:
        return "false"
    # Structured receipts/evidence are intentionally represented only by shape.
    return "structured"


def _emit(frame, *, event: str, value: Any = None, exception: BaseException | None = None):
    row = {
        "file": _basename(frame.f_code.co_filename),
        "function": frame.f_code.co_name,
        "event": event,
        "line": frame.f_lineno,
    }
    if exception is not None:
        row["exception_class"] = type(exception).__name__
    else:
        row["result"] = _result_kind(value)
        pending = _pending_count(value)
        if pending is not None:
            row["pending_count"] = pending
    consumer = _consumer_label(frame)
    if consumer is not None:
        row["consumer"] = consumer
    print("I10_DIAGNOSTIC_OBSERVATION=" + json.dumps(row, sort_keys=True), flush=True)


def _trace_factory():
    def trace(frame, event, arg):
        filename = _basename(frame.f_code.co_filename)
        functions = _TRACE_TARGETS.get(filename)
        if not functions or frame.f_code.co_name not in functions:
            return None
        if event == "return":
            _emit(frame, event="return", value=arg)
        elif event == "exception" and isinstance(arg, tuple) and len(arg) >= 2:
            _emit(frame, event="exception", exception=arg[1])
        return trace

    return trace


@contextmanager
def _observe_current_thread() -> Iterator[None]:
    """Install one recursive-safe trace on this thread only."""
    depth = getattr(_TRACE_STATE, "depth", 0)
    if depth:
        _TRACE_STATE.depth = depth + 1
        try:
            yield
        finally:
            _TRACE_STATE.depth = depth
        return

    previous = sys.gettrace()
    if previous is not None:
        raise RuntimeError("i10 diagnostic refuses to replace an existing current-thread tracer")
    _TRACE_STATE.depth = 1
    trace = _trace_factory()
    sys.settrace(trace)
    try:
        yield
    finally:
        sys.settrace(previous)
        _TRACE_STATE.depth = 0


@pytest.fixture(autouse=True)
def native_plan_return_observer(monkeypatch):
    """Observe actual frozen I10/native-plan functions without changing producers."""
    from gateway import run_runtime
    import gateway.runtime_native_cleanup_sources as cleanup_sources

    monkeypatch.setenv("_AGC_NATIVE_DIAGNOSTIC", "1")
    original_seal = run_runtime._seal_i10_preservation_owner
    original_plan = cleanup_sources.build_native_cleanup_plan

    @wraps(original_seal)
    def wrapped_seal(*args, **kwargs):
        with _observe_current_thread():
            return original_seal(*args, **kwargs)

    @wraps(original_plan)
    def wrapped_plan(*args, **kwargs):
        with _observe_current_thread():
            return original_plan(*args, **kwargs)

    # Only these two private non-producer callables are replaced.  In particular,
    # no registry entry or probe callable is wrapped or rebound.
    monkeypatch.setattr(run_runtime, "_seal_i10_preservation_owner", wrapped_seal)
    monkeypatch.setattr(cleanup_sources, "build_native_cleanup_plan", wrapped_plan)
    yield
