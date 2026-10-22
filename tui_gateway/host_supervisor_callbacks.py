"""Callback and legacy-registry seams for :mod:`host_supervisor`.

These small lifecycle helpers live outside the supervisor facade so the
process/reader implementation stays within the repository's per-file health
limit.  They deliberately receive the owner object instead of importing the
facade, which keeps the callback seam late-bound and testable.
"""

from __future__ import annotations

import json
import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


def call_logged(callback: Callable[[dict], object], frame: dict, failure: str) -> None:
    """Invoke an opaque compatibility callback without escaping its exception."""
    try:
        callback(frame)
    except Exception:
        logger.exception(failure)


def legacy_pending_fields(pending: Any) -> tuple[str, Callable[[dict], object] | None]:
    """Read the old two-tuple pending callback shape without importing the facade."""
    if hasattr(pending, "sid") and hasattr(pending, "callback"):
        return pending.sid, pending.callback
    try:
        return pending[0], pending[1]
    except (IndexError, KeyError, TypeError):
        return "", None


def legacy_registry_outcome(
    supervisor: Any,
    *,
    pid_alive: Callable[[int], bool],
    pid_matches: Callable[[int], bool],
) -> str | None:
    """Handle the pre-typed two-field registry without adopting its PID."""
    try:
        record = json.loads(supervisor.registry_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return None
    if not isinstance(record, dict) or not set(record) <= {"host_pid", "boot_id"}:
        return None
    try:
        pid = int(record.get("host_pid") or 0)
    except (TypeError, ValueError):
        pid = 0
    if pid <= 0 or not pid_alive(pid):
        supervisor.registry_path.unlink(missing_ok=True)
        return "not-running"
    if not pid_matches(pid):
        supervisor.registry_path.unlink(missing_ok=True)
        return "pid-reuse-ignored"
    return "owner-unavailable"


def has_pending_work(supervisor: Any) -> bool:
    """Keep a detached target alive while any callback or transport can settle."""
    with supervisor._lock:
        return bool(
            supervisor._pending_turns or supervisor._pending_controls
            or supervisor._late_control_handlers or supervisor._late_control_uncertain
            or supervisor._active_callbacks
            or supervisor._transport_write_lock.locked()
            or supervisor._startup_binding is not None
            or supervisor._startup_orphan_handles
        )


def transport_writer_unresolved(result: dict[str, Any]) -> bool:
    """Keep custody unknown until a sent writer has a complete ownerless receipt."""
    if result.get("transport_writer_pending") or result.get("transport_writer_ambiguous"):
        return True
    if not result.get("transport_writer_sent") or result.get("clean"):
        return False
    return not (
        result.get("owner_state") == "unavailable"
        and result.get("natural_exit") and result.get("shutdown_ack")
        and result.get("exit_settled") and result.get("stdout_eof")
        and result.get("stderr_eof") and result.get("completion_eof")
    )


def normalize_refused_terminal(pending: Any, terminal: dict[str, Any]) -> dict[str, Any]:
    """Expose a typed pre-admission refusal as an error to leased callbacks."""
    if (
        getattr(pending, "retirement_lease", None) is not None
        and terminal.get("type") == "turn.end"
        and terminal.get("reason") == "dispatch_refused"
    ):
        pending.terminal = {**terminal, "type": "turn.error"}
        return pending.terminal
    return terminal


def invoke_callback(
    supervisor: Any, callback: Callable[[dict], object], frame: dict[str, Any], failure: str,
    *, reserved: bool = False,
) -> object | None:
    """Count an opaque callback as live custody until it returns."""
    if not reserved:
        with supervisor._lock:
            supervisor._active_callbacks += 1
    try:
        try:
            return callback(frame)
        except Exception:
            logger.exception(failure)
            return None
    finally:
        on_idle = None
        with supervisor._lock:
            supervisor._active_callbacks = max(0, supervisor._active_callbacks - 1)
            if supervisor._active_callbacks == 0 and not supervisor._pending_turns \
                    and not supervisor._pending_controls \
                    and not supervisor._late_control_handlers:
                on_idle = supervisor._on_callbacks_idle
        if on_idle is not None:
            try:
                on_idle()
            except Exception:
                logger.exception("compute host callbacks-idle hook failed")
