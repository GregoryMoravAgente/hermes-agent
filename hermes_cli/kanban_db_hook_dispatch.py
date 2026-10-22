"""Kanban lifecycle hook consumers kept outside the board facade.

These callbacks run only after the board transaction has committed.  They are
observer notifications; a plugin failure never changes the durable transition.
The helpers import the board's current-resolution functions lazily to avoid a
facade/module cycle while keeping the dispatch behavior in this sibling.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any, Iterable, Optional


_log = logging.getLogger("hermes_cli.kanban_db")


def _hook_profile_name() -> str:
    """Active profile for hook payloads; ``"default"`` when it cannot be resolved."""
    from hermes_cli.profiles import get_active_profile_name

    try:
        return get_active_profile_name()
    except Exception:
        return "default"


def _kanban_observer_consumed(event: str) -> bool:
    """Skip payload assembly when no lifecycle observer is registered."""
    try:
        from hermes_cli.lifecycle import has_hook

        return has_hook(event)
    except Exception:  # pragma: no cover - defensive
        return False


def _fire_kanban_lifecycle_hook(event: str, task_id: str, **fields: Any) -> None:
    """Fire a post-commit lifecycle observer without holding the board write lock."""
    try:
        from hermes_cli.lifecycle import invoke_hook
        from agent.shell_hook_scope import managed_hook_route

        with managed_hook_route():
            invoke_hook(event, task_id=task_id, profile_name=_hook_profile_name(), **fields)
    except Exception as exc:  # pragma: no cover - defensive
        _log.debug("kanban lifecycle hook %s failed: %s", event, exc)


def _fire_task_hook(
    event: str, task: Any, task_id: str, run_id: Optional[int], **fields: Any,
) -> None:
    """Notify a task transition using the committed board and assignee row."""
    from hermes_cli.kanban_db import get_current_board

    _fire_kanban_lifecycle_hook(
        event, task_id, board=get_current_board(),
        assignee=task.assignee if task else None, run_id=run_id, **fields,
    )


def _fire_worker_spawned_hook(
    conn: sqlite3.Connection, task: Any, workspace_path: str, pid: Optional[int], *,
    board: Optional[str] = None,
) -> None:
    """Notify after the worker PID is durably persisted."""
    if not _kanban_observer_consumed("on_kanban_worker_spawned"):
        return
    try:
        from hermes_cli.kanban_db import _current_run_id, get_current_board

        _fire_kanban_lifecycle_hook(
            "on_kanban_worker_spawned", task.id, board=board or get_current_board(),
            assignee=task.assignee, run_id=_current_run_id(conn, task.id),
            worker_pid=int(pid) if pid else None, workspace_path=str(workspace_path),
        )
    except Exception as exc:  # pragma: no cover - defensive
        _log.debug("kanban worker spawned hook failed: %s", exc)


def notify_task_updated(
    conn: sqlite3.Connection, task_id: str, changed_fields: Iterable[str], *,
    board: Optional[str] = None,
) -> None:
    """Notify after a non-lifecycle task mutation commits."""
    if not _kanban_observer_consumed("on_kanban_task_updated"):
        return
    try:
        from hermes_cli.kanban_db import get_current_board

        row = conn.execute(
            "SELECT assignee, current_run_id FROM tasks WHERE id = ?", (task_id,),
        ).fetchone()
        _fire_kanban_lifecycle_hook(
            "on_kanban_task_updated", task_id, board=board or get_current_board(),
            assignee=row["assignee"] if row else None,
            run_id=row["current_run_id"] if row else None, changed_fields=list(changed_fields),
        )
    except Exception as exc:  # pragma: no cover - defensive
        _log.debug("kanban task updated hook failed: %s", exc)


_TICK_ACTIVITY_FIELDS = (
    "spawned", "reclaimed", "promoted", "reconciled_orphans", "reaped_terminal_workers", "crashed", "stale",
    "timed_out", "auto_blocked", "rate_limited", "auto_assigned_default",
    "respawn_guarded", "skipped_per_profile_capped", "skipped_unassigned",
    "skipped_nonspawnable",
)


def _fire_dispatch_tick_hook(
    result: Any, *, board: Optional[str] = None, dry_run: bool = False,
) -> None:
    """Notify after the dispatch tick lock is released."""
    if not _kanban_observer_consumed("on_kanban_dispatch_tick"):
        return
    try:
        from hermes_cli.kanban_db import get_current_board
        from hermes_cli.lifecycle import invoke_hook
        from agent.shell_hook_scope import managed_hook_route

        profile_name = _hook_profile_name()
        if board is None:
            try:
                board = get_current_board()
            except Exception:
                board = None
        outcome = "ok"
        if result.skipped_locked:
            outcome = "skipped_locked"
        elif not any(getattr(result, field) for field in _TICK_ACTIVITY_FIELDS):
            outcome = "idle"
        with managed_hook_route():
            invoke_hook(
                "on_kanban_dispatch_tick", board=board, profile_name=profile_name,
                dry_run=bool(dry_run), outcome=outcome, result=result,
            )
    except Exception as exc:  # pragma: no cover - defensive
        _log.debug("kanban dispatch tick hook failed: %s", exc)
