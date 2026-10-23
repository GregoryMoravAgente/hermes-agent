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

    def contains_session(self, session_id: str) -> bool:
        """Return whether this exact source admission is still retained."""
        with self._lock:
            return session_id in self._created_session_ids

    def contains_foreground(self, reservation_id: str) -> bool:
        """Return whether this exact foreground reservation is still pending."""
        with self._lock:
            return reservation_id in self._pending_reservation_ids

    def release_session(self, session_id: str, owner_task_id: str) -> bool:
        """Release one admission only after its caller proved no native effect occurred."""
        with self._lock:
            if (
                self._closed
                or owner_task_id != self.owner_task_id
                or session_id not in self._created_session_ids
            ):
                self._late_creation = True
                self._receipt = None
                return False
            self._created_session_ids.remove(session_id)
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

    def transfer_foreground(
        self, reservation_id: str, session_id: str, owner_task_id: str,
    ) -> bool:
        """Transfer one admitted foreground reservation to its exact registry session."""
        with self._lock:
            if (
                self._closed
                or owner_task_id != self.owner_task_id
                or reservation_id not in self._pending_reservation_ids
                or not isinstance(session_id, str)
                or not session_id
            ):
                self._late_creation = True
                self._receipt = None
                return False
            self._pending_reservation_ids.remove(reservation_id)
            self._created_session_ids.append(session_id)
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
        if not self._runtime_custody.profile_scope_valid(
            profile_id, authority_epoch, require_serving=False, allow_retiring=True,
        ):
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
            or not self._runtime_custody.profile_scope_valid(
                profile_id, session.authority_epoch,
                require_serving=False, allow_retiring=True,
                physical_owner_epoch=getattr(
                    getattr(session, "custody", None), "physical_owner_epoch", None,
                ),
            )
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
            "physical_owner_epochs": tuple(
                session.custody.physical_owner_epoch for session in terminal
            ),
        }




def _session_profile_identity(session, process_instance: str):
    profile_id = getattr(session, "profile_id", None)
    authority_epoch = getattr(session, "authority_epoch", None)
    if (
        getattr(session, "process_instance", None) != process_instance
        or not isinstance(profile_id, str)
        or not profile_id
        or type(authority_epoch) is not int
        or authority_epoch < 1
    ):
        return None
    return profile_id, authority_epoch



def _terminal_custody_observations_valid(session, custody) -> bool:
    for field_name in (
        "custody_process_bound", "custody_reader_eof", "custody_scope_settled",
        "custody_durable", "custody_handle_closed", "custody_delivery_acked",
    ):
        if getattr(session, field_name, False) is not True:
            return False
    for field_name in (
        "process_bound", "reader_eof", "scope_settled", "durable", "streams_closed",
        "handle_closed", "delivery_acked", "cross_process_proven",
    ):
        if getattr(custody, field_name, False) is not True:
            return False
    return True


def _terminal_profile_receipt(registry, session, process_instance: str) -> Optional[dict[str, Any]]:
    """Return the source-owned closed receipt for one terminal session."""
    custody = getattr(session, "custody", None)
    coordinator = getattr(registry, "_runtime_custody", None)
    identity = _session_profile_identity(session, process_instance)
    if (
        coordinator is None
        or identity is None
        or custody is None
        or getattr(session, "custody_state", None) != "terminal"
        or getattr(custody, "state", None) != "terminal"
        or getattr(custody, "coordinator", None) is not coordinator
        or getattr(custody, "process_instance", None) != process_instance
    ):
        return None
    profile_id, authority_epoch = identity
    physical_owner_epoch = getattr(custody, "physical_owner_epoch", None)
    owner_id = getattr(session, "custody_owner_id", None)
    if (
        getattr(custody, "profile_id", None) != profile_id
        or getattr(custody, "authority_epoch", None) != authority_epoch
        or not isinstance(physical_owner_epoch, str)
        or not physical_owner_epoch
        or not isinstance(owner_id, str)
        or not owner_id.startswith(f"{process_instance}:process:")
        or getattr(custody, "owner_id", None) != owner_id
        or getattr(session, "custody_error", "")
        or getattr(custody, "reason", "")
        or not _terminal_custody_observations_valid(session, custody)
    ):
        return None
    return {
        "session_id": getattr(session, "id", ""),
        "profile_id": profile_id,
        "authority_epoch": authority_epoch,
        "physical_owner_epoch": physical_owner_epoch,
        "owner_id": owner_id,
        "cross_process_proof": True,
        "closed_receipt": True,
    }


def _not_accepted_profile_receipt(session, process_instance: str) -> Optional[dict[str, Any]]:
    """Return the no-effect receipt for an authenticated pre-effect session."""
    identity = _session_profile_identity(session, process_instance)
    custody = getattr(session, "custody", None)
    if (
        identity is None
        or getattr(session, "custody_state", None) != "not_accepted"
        or getattr(session, "custody_error", "")
        or (custody is not None and getattr(custody, "state", None) != "not_accepted")
    ):
        return None
    profile_id, authority_epoch = identity
    return {
        "session_id": getattr(session, "id", ""),
        "profile_id": profile_id,
        "authority_epoch": authority_epoch,
        "physical_owner_epoch": None,
        "owner_id": getattr(session, "custody_owner_id", ""),
        "cross_process_proof": True,
        "closed_receipt": False,
    }


def _mixed_close_inputs(
    registry, *, process_instance: str, authority_epoch: int, close_generation: int,
    owner_id: str, launch_profile_id: str,
):
    coordinator = getattr(registry, "_runtime_custody", None)
    if coordinator is None or not registry._custody_observed or registry._custody_unbound_sessions:
        return None
    if not isinstance(process_instance, str) or not process_instance:
        return None
    if getattr(coordinator, "process_instance", None) != process_instance:
        return None
    if type(authority_epoch) is not int or authority_epoch < 1:
        return None
    if getattr(coordinator, "authority_epoch", None) != authority_epoch:
        return None
    if type(close_generation) is not int or close_generation < 1:
        return None
    if owner_id != f"{process_instance}:I15_process_registry_terminal":
        return None
    if not isinstance(launch_profile_id, str) or not launch_profile_id:
        return None
    if launch_profile_id != registry._process_profile_id():
        return None
    return coordinator


def _collect_mixed_profile_sessions(registry, process_instance: str):
    sessions = [*registry._running.values(), *registry._finished.values()]
    if not sessions or not registry._custody_terminal_count:
        return None
    grouped: dict[str, dict[str, Any]] = {}
    terminal_count = 0
    for session in sessions:
        state = getattr(session, "custody_state", None)
        if state == "terminal":
            proof = _terminal_profile_receipt(registry, session, process_instance)
            terminal_count += 1
        elif state == "not_accepted":
            proof = _not_accepted_profile_receipt(session, process_instance)
        else:
            return None
        if proof is None:
            return None
        profile_id = proof["profile_id"]
        group = grouped.setdefault(
            profile_id,
            {"terminal": [], "not_accepted": [], "authority_epochs": set(),
             "physical_owner_epochs": set()},
        )
        group[state].append(proof)
        group["authority_epochs"].add(proof["authority_epoch"])
        if proof["physical_owner_epoch"] is not None:
            group["physical_owner_epochs"].add(proof["physical_owner_epoch"])
    return (grouped, terminal_count) if grouped and terminal_count else None


def _build_mixed_profile_evidence(
    grouped: dict[str, dict[str, Any]], *, process_instance: str, authority_epoch: int,
    close_generation: int, owner_id: str, launch_profile_id: str,
) -> dict[str, Any]:
    profile_evidence = []
    terminal_session_ids = []
    physical_owner_epochs = []
    terminal_count = 0
    for profile_id in sorted(grouped):
        group = grouped[profile_id]
        terminal = group["terminal"]
        terminal_count += len(terminal)
        terminal_session_ids.extend(proof["session_id"] for proof in terminal)
        physical_owner_epochs.extend(proof["physical_owner_epoch"] for proof in terminal)
        profile_evidence.append({
            "profile_id": profile_id,
            "authority_epochs": tuple(sorted(group["authority_epochs"])),
            "physical_owner_epochs": tuple(sorted(group["physical_owner_epochs"])),
            "terminal_session_ids": tuple(proof["session_id"] for proof in terminal),
            "not_accepted_session_ids": tuple(
                proof["session_id"] for proof in group["not_accepted"]
            ),
            "terminal_receipts": tuple(terminal),
            "observed": True,
            "cross_process_proof": (
                all(proof["cross_process_proof"] for proof in terminal)
                if terminal else True
            ),
        })
    return {
        "source": (
            "tools.process_registry_custody_evidence.py:107-439;"
            "tools.process_registry_custody.py:119-312,1517-1586,1615-1630,1750-1780;"
            "tools.process_registry.py:545-627,1572-1613"
        ),
        "process_instance": process_instance,
        "authority_epoch": authority_epoch,
        "close_generation": close_generation,
        "owner_id": owner_id,
        "profile_id": launch_profile_id,
        "profile_ids": tuple(sorted(grouped)),
        "profile_evidence": tuple(profile_evidence),
        "observed": True,
        "cross_process_proof": True,
        "settled_processes": terminal_count,
        "terminal_session_ids": tuple(terminal_session_ids),
        "physical_owner_epochs": tuple(physical_owner_epochs),
    }


def preservation_owner_evidence_for_profiles(
    registry, *, process_instance: str, authority_epoch: int, close_generation: int,
    owner_id: str, launch_profile_id: str,
) -> Optional[dict[str, Any]]:
    """Return one I15 close proof covering every authenticated profile in this process.

    A process close has one launch epoch at the coordinator envelope, but multiplexed profiles
    may have older, already-retired authority epochs.  A terminal ProcessCustody object retains
    the source-owned physical incarnation and all native/durable/reader/delivery observations,
    so a closed historical profile can contribute without pretending that the current serving
    epoch was its terminal epoch.  Active, unknown, unbound, foreign, malformed, or incomplete
    sessions remain refusal evidence; absence is never completion.
    """
    registry._retry_foreground_records()
    with registry._lock:
        _mixed_close_inputs_result = _mixed_close_inputs(
            registry,
            process_instance=process_instance,
            authority_epoch=authority_epoch,
            close_generation=close_generation,
            owner_id=owner_id,
            launch_profile_id=launch_profile_id,
        )
        if _mixed_close_inputs_result is None:
            return None
        collected = _collect_mixed_profile_sessions(registry, process_instance)
        if collected is None:
            return None
        grouped, _terminal_count = collected
        return _build_mixed_profile_evidence(
            grouped,
            process_instance=process_instance,
            authority_epoch=authority_epoch,
            close_generation=close_generation,
            owner_id=owner_id,
            launch_profile_id=launch_profile_id,
        )


def open_process_scope(
    registry, *, owner_task_id: str, profile_id: str, authority_epoch: int,
) -> Optional[ProcessScopeFence]:
    """Open an exact API-task process scope before any child-producing effect."""
    coordinator = registry._runtime_custody
    if (
        coordinator is None
        or not isinstance(owner_task_id, str) or not owner_task_id
        or not isinstance(profile_id, str) or not profile_id
        or not coordinator.profile_scope_valid(
            profile_id, authority_epoch, require_serving=True, allow_retiring=False,
        )
    ):
        return None
    return ProcessScopeFence(
        registry, owner_task_id, coordinator.process_instance, authority_epoch, profile_id,
    )
