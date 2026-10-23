"""Read-only checks for retained process results at an I15 coordinator boundary."""

from __future__ import annotations

from typing import Any


_UNSET = object()


def _completion_event_set(session: Any) -> bool:
    event = getattr(session, "_completion_event", None)
    is_set = getattr(event, "is_set", None)
    if not callable(is_set):
        return False
    try:
        return bool(is_set())
    except (AttributeError, RuntimeError, TypeError):
        return False


def _reader_settled(session: Any) -> bool:
    reader = getattr(session, "_reader_thread", None)
    if reader is None:
        return True
    is_alive = getattr(reader, "is_alive", None)
    if not callable(is_alive):
        return False
    try:
        return not is_alive()
    except (AttributeError, RuntimeError, TypeError):
        return False


def _foreground_transfer_settled(custody: Any) -> bool:
    transfer = getattr(custody, "foreground_transfer", None)
    if transfer is None:
        return True
    if getattr(transfer, "phase", None) != "terminal":
        return False
    for name in ("finalizer_thread", "finish_thread"):
        thread = getattr(transfer, name, None)
        is_alive = getattr(thread, "is_alive", None)
        if thread is not None and not callable(is_alive):
            return False
        try:
            if callable(is_alive) and is_alive():
                return False
        except (AttributeError, RuntimeError, TypeError):
            return False
    return True


def _custody_terminal(custody: Any) -> bool:
    if custody is None or getattr(custody, "state", None) != "terminal":
        return False
    if getattr(custody, "handle", None) is not None:
        return False
    return all(
        getattr(custody, name, None) is True
        for name in (
            "process_bound", "reader_eof", "scope_settled", "cross_process_proven",
            "durable", "streams_closed", "handle_closed", "watcher_registered",
            "delivery_acked",
        )
    )


def _session_custody_identity_matches(session: Any, custody: Any) -> bool:
    return all(
        getattr(session, session_name, None) == getattr(custody, custody_name, None)
        for session_name, custody_name in (
            ("profile_id", "profile_id"),
            ("process_instance", "process_instance"),
            ("authority_epoch", "authority_epoch"),
            ("custody_owner_id", "owner_id"),
            ("pid", "pid"),
            ("host_start_time", "host_start_time"),
        )
    )


def _native_identity_matches(session: Any, custody: Any, native: Any) -> bool:
    process = getattr(session, "process", None)
    if (
        getattr(native, "custody", None) is not custody
        or process is None
        or getattr(native, "process", None) is not process
        or getattr(native, "pid", None) != getattr(session, "pid", None)
        or getattr(native, "host_start_time", None) != getattr(session, "host_start_time", None)
        or not _session_custody_identity_matches(session, custody)
        or getattr(native, "scope_observation_process", None) is not process
        or getattr(native, "scope_observation_pid", None) != getattr(session, "pid", None)
        or getattr(native, "scope_observation_start_time", None)
        != getattr(session, "host_start_time", None)
        or getattr(native, "scope_observation_job", None) is not getattr(native, "job", None)
        or getattr(native, "close_job", None) is not getattr(native, "job", None)
        or getattr(native, "close_job_handle", None)
        is not getattr(native, "scope_observation_job_handle", None)
    ):
        return False
    matches = getattr(native, "matches", None)
    retained_matches = getattr(native, "_retained_scope_matches", None)
    if not callable(matches) or not callable(retained_matches):
        return False
    try:
        return bool(
            matches(process, session.pid, session.host_start_time)
            and retained_matches()
        )
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def _delivery_settled(session: Any, *, completion_tokens: Any, pending_watchers: Any) -> bool:
    session_id = getattr(session, "id", None)
    if not isinstance(session_id, str) or not session_id:
        return False
    try:
        has_token = any(
            isinstance(binding, (tuple, list))
            and bool(binding)
            and binding[0] == session_id
            for binding in completion_tokens.values()
        )
        has_watcher = any(
            isinstance(watcher, dict) and watcher.get("session_id") == session_id
            for watcher in pending_watchers
        )
    except (AttributeError, RuntimeError, TypeError):
        return False
    return not has_token and not has_watcher


def _native_receipt_closed(session: Any, custody: Any, native: Any) -> bool:
    if native is None:
        return True
    if not _native_identity_matches(session, custody, native):
        return False
    return all(
        (
            getattr(native, "state", None) == "closed",
            getattr(native, "native_empty_observed", None) is True,
            getattr(native, "scope_observation", None) in {"empty", "empty_nonmember"},
            getattr(native, "process_handle_closed", None) is True,
            getattr(native, "job_handle_closed", None) is True,
            getattr(native, "close_retryable", None) is not True,
        )
    )


def finished_session_is_terminal_history(
    session: Any, *, completion_tokens: Any, pending_watchers: Any,
    live_coordinator: Any = _UNSET,
) -> bool:
    """Return whether a retained result is safe to ignore at a new close fence.

    ``ProcessRegistry._finished`` is intentionally a display/history index, so a result can
    outlive the runtime coordinator that created it.  History is not custody proof: only an
    exact terminal ``ProcessCustody`` plus its native close receipt can make it irrelevant to
    the next coordinator.  This helper reads existing source-owned state; it does not clear
    history, acknowledge a delivery, or manufacture a native observation.
    """
    custody = getattr(session, "custody", None)
    if live_coordinator is not _UNSET:
        original_coordinator = getattr(custody, "coordinator", None)
        if original_coordinator is None or original_coordinator is live_coordinator:
            return False
    return all((
        getattr(session, "exited", False) is True,
        _completion_event_set(session),
        getattr(session, "_scope_stop_pending", False) is not True,
        _reader_settled(session),
        getattr(session, "reader_phase", None) in {"eof", "not_started"},
        _foreground_transfer_settled(custody),
        _custody_terminal(custody),
        _delivery_settled(
            session,
            completion_tokens=completion_tokens,
            pending_watchers=pending_watchers,
        ),
        _native_receipt_closed(session, custody, getattr(custody, "native_scope", None)),
    ))
