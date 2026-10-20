"""Typed I15 evidence query kept outside the process registry facade."""

from __future__ import annotations

from dataclasses import dataclass, field
import threading
from typing import Any, Optional


@dataclass
class ProcessScopeFence:
    """Source-owned admission fence for one API task's process-creation scope.

    This is an event token, not a second custody ledger.  The registry records every session
    creation observed through the token before a process effect; a closed token can therefore
    distinguish a proven childless scope from a scope that created a real child.  A late or
    mismatched producer invalidates the childless result instead of becoming an absence proof.
    """

    registry: Any
    owner_task_id: str
    process_instance: str
    authority_epoch: int
    profile_id: str
    _created_session_ids: list[str] = field(default_factory=list, repr=False)
    _pending_reservation_ids: set[str] = field(default_factory=set, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)
    _late_creation: bool = field(default=False, init=False, repr=False)
    _receipt: Optional[dict[str, Any]] = field(default=None, init=False, repr=False)
    _lock: Any = field(default_factory=threading.Lock, init=False, repr=False)

    def record_session(self, session_id: str, owner_task_id: str) -> bool:
        """Record a source-owned session before its Popen/backend effect."""
        with self._lock:
            if self._closed or owner_task_id != self.owner_task_id:
                self._late_creation = True
                self._receipt = None
                return False
            if not isinstance(session_id, str) or not session_id:
                self._late_creation = True
                self._receipt = None
                return False
            self._created_session_ids.append(session_id)
            return True

    def reserve_foreground(self, reservation_id: str, owner_task_id: str) -> bool:
        """Record a foreground custody reservation before the launcher can create a child."""
        with self._lock:
            if self._closed or owner_task_id != self.owner_task_id:
                self._late_creation = True
                self._receipt = None
                return False
            if not isinstance(reservation_id, str) or not reservation_id:
                self._late_creation = True
                self._receipt = None
                return False
            self._pending_reservation_ids.add(reservation_id)
            return True

    def release_foreground(self, reservation_id: str) -> bool:
        """Clear a reservation only after its exact owner has proved pre-effect release."""
        with self._lock:
            if self._closed or reservation_id not in self._pending_reservation_ids:
                self._late_creation = True
                self._receipt = None
                return False
            self._pending_reservation_ids.remove(reservation_id)
            return True

    def close(self) -> dict[str, Any]:
        """Return the current source-owned observation; late producers invalidate prior receipts."""
        with self._lock:
            return self._close_locked()

    def _close_locked(self) -> dict[str, Any]:
        if self._receipt is not None:
            return dict(self._receipt)
        self._closed = True
        self._receipt = {
            "source": "tools.process_registry_custody_evidence.ProcessScopeFence",
            "registry_id": id(self.registry),
            "owner_task_id": self.owner_task_id,
            "process_instance": self.process_instance,
            "authority_epoch": self.authority_epoch,
            "profile_id": self.profile_id,
            "created_session_ids": tuple(self._created_session_ids),
            "pending_reservation_ids": tuple(sorted(self._pending_reservation_ids)),
            "childless": (
                not self._created_session_ids
                and not self._pending_reservation_ids
                and not self._late_creation
            ),
            "late_creation": self._late_creation,
            "observed": True,
        }
        return dict(self._receipt)

    def terminalize(self, coordinator: Any, handle: Any):
        """Claim the current fence and coordinator lease in one synchronization interval."""
        with self._lock:
            receipt = self._close_locked()
            if self._late_creation:
                return receipt, False
            return receipt, coordinator.mark_terminal(handle)


def preservation_owner_evidence(
    self, *, process_instance: str, authority_epoch: int, close_generation: int,
    owner_id: str, profile_id: str, task_id: Optional[str] = None,
) -> Optional[dict[str, Any]]:
    """Return I15 evidence only after a real process custody cycle is closed.

    When *task_id* is supplied, the proof is scoped to sessions whose raw spawning owner is
    exactly that task.  The legacy omitted-task form remains available to existing lifecycle
    consumers; API-run custody must always use the exact-task form so a terminal sibling in
    the same profile cannot prove a different run.
    """
    self._retry_foreground_records()
    with self._lock:
        all_sessions = [*self._running.values(), *self._finished.values()]
        if task_id is not None:
            if not isinstance(task_id, str) or not task_id:
                return None
            sessions = [
                session for session in all_sessions
                if getattr(session, "owner_task_id", "") == task_id
            ]
            if not sessions:
                return None
        else:
            sessions = all_sessions
        if owner_id != f"{process_instance}:I15_process_registry_terminal":
            return None
        if not isinstance(profile_id, str) or not profile_id:
            return None
        if self._runtime_custody is None or not self._custody_observed:
            return None
        if self._custody_unbound_sessions or not self._custody_terminal_count or not sessions:
            return None
        if any(session.custody_state not in {"terminal", "not_accepted"} for session in sessions):
            return None
        if any(
            session.profile_id != profile_id
            for session in sessions
            if session.custody_state in {"terminal", "not_accepted"}
        ):
            return None
        terminal = [session for session in sessions if session.custody_state == "terminal"]
        if not terminal or any(
            session.process_instance != process_instance
            or session.authority_epoch != authority_epoch
            or session.profile_id != profile_id
            or not session.custody_process_bound
            or not session.custody_reader_eof
            or not session.custody_scope_settled
            or not session.custody_durable
            or not session.custody_handle_closed
            or not session.custody_delivery_acked
            or not session.custody_owner_id
            or not session.custody_owner_id.startswith(f"{process_instance}:process:")
            for session in terminal
        ):
            return None
        if any(session.custody is not None and not session.custody.cross_process_proven for session in terminal):
            return None
        return {
            "source": (
                "tools/process_registry_custody_evidence.py:9-83;"
                "tools/process_registry.py:1175-1315,1563-1632,1901-1997,2233-2300,2495-2569;"
                "tools/terminal_tool_background.py:107-240,251-306;gateway/run_notifications.py:1677-1715"
            ),
            "process_instance": process_instance,
            "authority_epoch": authority_epoch,
            "close_generation": close_generation,
            "owner_id": owner_id,
            "profile_id": profile_id,
            **({"task_id": task_id} if task_id is not None else {}),
            "observed": True,
            "cross_process_proof": terminal[-1].custody.cross_process_proven,
            "settled_processes": len(terminal),
            "terminal_session_ids": tuple(session.id for session in terminal),
        }


def open_process_scope(
    registry, *, owner_task_id: str, profile_id: str, authority_epoch: int,
) -> Optional[ProcessScopeFence]:
    """Open an exact API-task process scope before any child-producing effect."""
    coordinator = registry._runtime_custody
    if (
        coordinator is None
        or not isinstance(owner_task_id, str) or not owner_task_id
        or not isinstance(profile_id, str) or not profile_id
        or profile_id != registry._process_profile_id()
        or authority_epoch != coordinator.authority_epoch
    ):
        return None
    return ProcessScopeFence(
        registry, owner_task_id, coordinator.process_instance, authority_epoch, profile_id,
    )

