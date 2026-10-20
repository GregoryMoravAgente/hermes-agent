"""Runtime-custody binding for the process registry.

The process registry owns the OS reader and process handles; the gateway custody
coordinator owns the profile-scoped lease.  This small adapter keeps those two
lifetimes joined without introducing a second registry or a process-wide proof
flag.  A binding is accepted before a child is started and becomes terminal only
after the real process identity, reader EOF, durable result, handle close, and
completion delivery have all been observed.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field
import logging
import os
import platform
from pathlib import Path
import secrets
import sqlite3
import subprocess
import threading
import time
import uuid
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from tools.process_registry import ProcessSession


from tools.process_registry_notifications import (
    format_process_notification, is_child_process_notification, should_surface_notification,
)
from tools.process_registry_foreground_retry import (
    can_reconcile_unknown, foreground_delivery_close_generation,
    foreground_record, managed_retry_complete, managed_retry_identity_valid,
    mark_native_scope_unknown, native_scope_pending_reason,
    reconcile_native_custody, reconcile_native_foreground_retry,
)
from tools.process_registry_custody_evidence import ProcessScopeFence

logger = logging.getLogger("tools.process_registry")

OWNER_CLASS = "I15_process_registry_terminal"
_IS_WINDOWS = platform.system() == "Windows"
_PROOF_EXCEPTIONS = (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error)


@dataclass(frozen=True)
class ForegroundParentBinding:
    """The authenticated parent turn that owns a foreground launch."""

    session_id: str
    session_key: str
    tool_call_id: str
    turn_id: str
    run_generation: int


@dataclass
class ForegroundCustodyRecord:
    """Mutable state kept on the original native transaction until delivery settles."""

    transaction: Any
    binding: Optional[ForegroundParentBinding] = None
    raw_result: Any = None
    finalized_result: Any = None
    evidence: Optional[dict[str, Any]] = None
    durable_proof: Any = None
    session: Any = None
    phase: str = "reserved"
    delivery_token: str = ""
    command: str = ""
    cwd: Optional[str] = None
    task_id: str = ""
    owner_task_id: str = ""
    delivery_close_generation: Optional[int] = None


@dataclass
class ForegroundReaderTransfer:
    """Typed in-memory ownership record for one live foreground reader.

    Native handles, threads and the collector never enter a checkpoint.  The phase and the
    authenticated transaction identity do: a retry may reuse only these exact objects.
    """

    transaction: Any
    process: Any
    reader_thread: Any
    reader_stop: Any
    output: Any
    evidence: dict[str, Any]
    deadline: Optional[float]
    stdin_thread: Any = None
    stdin_errors: Any = None
    phase: str = "staging"
    collector_token: Any = field(default=None, repr=False, compare=False)
    initial_output: str = ""
    initial_total_chars: int = 0
    spill_path: Optional[str] = None
    output_total_chars: int = 0
    callback_started: bool = False
    routing_committed: bool = False
    finalizer_thread: Any = field(default=None, repr=False, compare=False)
    finalizer_started: bool = False
    finish_claimed: bool = False
    finish_thread: Any = field(default=None, repr=False, compare=False)
    kill_requested: bool = False
    kill_source: str = ""
    kill_consume_output: bool = False
    lock: Any = field(default_factory=threading.RLock, repr=False, compare=False)

    def blocks_finish(self) -> bool:
        return self.phase != "terminal"


@dataclass
class ProcessCustody:
    coordinator: Any
    handle: Any
    owner_id: str
    profile_id: str
    process_instance: str
    authority_epoch: int
    state: str = "accepted"
    reason: str = ""
    process_bound: bool = False
    reader_eof: bool = False
    scope_settled: bool = False
    cross_process_proven: bool = False
    durable: bool = False
    handle_closed: bool = False
    watcher_registered: bool = False
    delivery_acked: bool = False
    pid: Optional[int] = None
    host_start_time: Optional[int] = None
    pid_scope: str = "host"
    scope_identity: str = ""
    process_group_id: Optional[int] = None
    # The exact native Job/process transaction is intentionally in-memory only.  A checkpoint
    # may carry the authenticated identity, never a guessed PID or a reconstructed handle.
    native_scope: Any = field(default=None, repr=False, compare=False)
    delivery_token: str = field(default="", repr=False, compare=False)
    streams_closed: bool = False
    foreground_binding: Optional[ForegroundParentBinding] = field(default=None, repr=False, compare=False)
    foreground_transfer: Optional[ForegroundReaderTransfer] = field(default=None, repr=False, compare=False)
    parent_handle: Any = field(default=None, repr=False, compare=False)
    scope_fence: Optional[ProcessScopeFence] = field(default=None, repr=False, compare=False)
    foreground_reserved: bool = field(default=False, repr=False, compare=False)

    @classmethod
    def reserve(
        cls, coordinator, owner_id: str, profile_id: str, *,
        parent_handle: Any = None, scope_fence: Optional[ProcessScopeFence] = None,
    ) -> ProcessCustody:
        if not isinstance(profile_id, str) or not profile_id:
            raise ValueError("process custody requires an authenticated profile")
        handle = coordinator.reserve_root(
            OWNER_CLASS,
            owner_id,
            profile_id=profile_id,
            authority_epoch=coordinator.authority_epoch,
            parent=parent_handle,
        )
        if (
            not hasattr(handle, "lease")
            or getattr(handle, "owner_class", None) != OWNER_CLASS
            or getattr(handle, "owner_id", None) != owner_id
            or getattr(handle, "profile_id", None) != profile_id
            or getattr(handle, "process_instance", None) != coordinator.process_instance
            or getattr(handle, "authority_epoch", None) != coordinator.authority_epoch
        ):
            if hasattr(handle, "lease"):
                try:
                    coordinator.mark_unknown(handle, "foreign_custody")
                except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                    pass
            raise RuntimeError(
                f"process custody reservation refused: {getattr(handle, 'code', 'unknown')}"
            )
        return cls(
            coordinator=coordinator,
            handle=handle,
            owner_id=owner_id,
            profile_id=profile_id,
            process_instance=handle.process_instance,
            authority_epoch=handle.authority_epoch,
            parent_handle=parent_handle,
            scope_fence=scope_fence,
        )

    def bind_process(
        self, pid: Optional[int], host_start_time: Optional[int], pid_scope: str, scope_identity: str = "",
    ) -> bool:
        self.pid, self.host_start_time, self.pid_scope = pid, host_start_time, pid_scope
        self.scope_identity = scope_identity
        self.process_group_id = None
        if pid_scope == "host" and type(pid) is int and pid > 0 and platform.system() != "Windows":
            try:
                self.process_group_id = os.getpgid(pid)
            except OSError:
                self.process_group_id = None
        if not self.scope_identity and self.process_group_id is not None:
            self.scope_identity = f"posix_pgrp:{self.process_group_id}"
        self.process_bound = (
            type(pid) is int and pid > 0
            and type(host_start_time) is int and host_start_time >= 0
            and isinstance(pid_scope, str) and bool(pid_scope)
        )
        if not self.process_bound:
            self.mark_unknown("partial_launch")
        return self.process_bound

    def mark_unknown(self, reason: str) -> None:
        if self.state in {"terminal", "not_accepted"}:
            return
        self.reason = reason
        self.state = "unknown"
        if self.handle is not None:
            self.coordinator.mark_unknown(self.handle, reason)

    def release_unaccepted(self) -> bool:
        if self.state != "accepted":
            return self.state == "not_accepted"
        if self.native_scope is not None and self.native_scope.has_process_effect:
            self.mark_unknown("partial_launch")
            return False
        refusal = self.coordinator.release_unaccepted(self.handle)
        if refusal is not None:
            self.mark_unknown(getattr(refusal, "code", "accepted_row_unknown"))
            return False
        self.state = "not_accepted"
        self.handle = None
        return True

    def ready(self) -> bool:
        return (
            self.state == "accepted"
            and self.process_bound
            and self.reader_eof
            and self.scope_settled
            and self.cross_process_proven
            and self.durable
            and self.streams_closed
            and self.handle_closed
            and self.watcher_registered
            and self.delivery_acked
        )

    def reconcile_unknown(self) -> bool:
        """Reopen only an unknown record whose exact native owner is still observable."""
        return reconcile_native_custody(self)

    def close_native(self) -> bool:
        """Close the exact Job/process handles after the native empty proof."""
        if self.native_scope is None:
            return self.handle_closed
        if (
            not self.delivery_acked
            or not self.reader_eof
            or not self.durable
            or not self.streams_closed
            or not self.scope_settled
        ):
            return False
        closed = self.native_scope.close_after_observed_empty()
        if closed:
            self.handle_closed = True
        return closed

    def settle(self) -> bool:
        if self.state in {"terminal", "not_accepted"}:
            return self.state == "terminal"
        if self.state == "unknown" or not self.ready():
            return False
        refusal = self.coordinator.mark_finalizing(self.handle, f"process:{self.owner_id}:complete")
        if refusal is not None:
            self.mark_unknown(getattr(refusal, "code", "finalizer_identity_missing"))
            return False
        refusal = self.coordinator.mark_terminal(self.handle)
        if refusal is not None:
            self.mark_unknown(getattr(refusal, "code", "lease_release_failed"))
            return False
        self.state = "terminal"
        self.handle = None
        return True

    def evidence(self, *, close_generation: int) -> dict[str, Any]:
        if self.state != "terminal":
            raise ValueError("process custody is not terminal")
        return {
            "owner_id": self.owner_id,
            "profile_id": self.profile_id,
            "process_instance": self.process_instance,
            "authority_epoch": self.authority_epoch,
            "close_generation": close_generation,
            "observed": True,
            "cross_process_proof": self.cross_process_proven,
            "process_identity": {
                "pid": self.pid,
                "host_start_time": self.host_start_time,
                "pid_scope": self.pid_scope,
                "scope_identity": self.scope_identity,
                "process_group_id": self.process_group_id,
            },
        }


class ProcessRegistryCustodyMixin:
    def begin_process_scope(
        self, *, owner_task_id: str, profile_id: str, authority_epoch: int,
    ) -> Optional[ProcessScopeFence]:
        from tools.process_registry_custody_evidence import open_process_scope
        return open_process_scope(
            self, owner_task_id=owner_task_id, profile_id=profile_id, authority_epoch=authority_epoch,
        )

    """I15 lifecycle methods kept beside the typed custody adapter.

    The process registry remains the owner of sessions and OS readers; this mixin only joins
    those existing callbacks to the one runtime coordinator.  It intentionally has no registry
    of its own and uses the host registry's existing lock, running/finished tables, and descendant
    probe.
    """

    def bind_runtime_custody(self, coordinator) -> None:
        """Bind the authenticated runtime ledger and quarantine pre-existing sessions."""
        with self._lock:
            self._runtime_custody = coordinator
            for session in (*self._running.values(), *self._finished.values()):
                if session.custody_state == "unbound":
                    session.custody_state = "unknown"
                    session.custody_error = "preexisting_process"
                    self._custody_unbound_sessions.add(session.id)

    @staticmethod
    def _foreground_record(transaction) -> ForegroundCustodyRecord:
        record = getattr(transaction, "_foreground_custody_record", None)
        if record is None:
            record = ForegroundCustodyRecord(transaction=transaction)
            transaction._foreground_custody_record = record
        return record

    def retain_foreground_custody(
        self, transaction, *, binding: Optional[ForegroundParentBinding] = None,
        evidence: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Keep one exact native transaction reachable until its authenticated consumer adopts it."""
        custody = getattr(transaction, "custody", None)
        owner_id = getattr(custody, "owner_id", "")
        if not owner_id:
            return False
        record = self._foreground_record(transaction)
        binding = binding or getattr(custody, "foreground_binding", None)
        if binding is not None:
            if record.binding is not None and record.binding != binding:
                if custody is not None:
                    custody.mark_unknown("foreign_foreground_binding")
                return False
            record.binding = binding
            if custody is not None:
                custody.foreground_binding = binding
            close_generation = foreground_delivery_close_generation(custody, record)
            if type(close_generation) is not int:
                custody.mark_unknown(close_generation)
                return False
            record.delivery_close_generation = close_generation
            if not record.delivery_token:
                record.delivery_token = secrets.token_urlsafe(32)
        with self._lock:
            current = self._foreground_custody_transactions.get(owner_id)
            if current is not None and current is not transaction:
                if custody is not None:
                    custody.mark_unknown("duplicate_foreground_transaction")
                return False
            if isinstance(evidence, dict):
                record.evidence = evidence if evidence.get("reader_lease") is True else dict(evidence)
            self._foreground_custody_transactions[owner_id] = transaction
            self._custody_unbound_sessions.add(owner_id)
        return True

    def consume_foreground_custody(self, custody) -> bool:
        """Remove a transaction only when the map still contains that exact object."""
        transaction = custody if hasattr(custody, "custody") else getattr(custody, "native_scope", None)
        transaction = transaction or custody
        transaction_custody = getattr(transaction, "custody", None)
        owner_id = getattr(transaction_custody, "owner_id", "") or getattr(custody, "owner_id", "")
        if not owner_id:
            return False
        with self._lock:
            if self._foreground_custody_transactions.get(owner_id) is not transaction:
                return False
            self._foreground_custody_transactions.pop(owner_id, None)
            self._custody_unbound_sessions.discard(owner_id)
        return True

    def stage_foreground_result(
        self, transaction, *, result: Any, finalized_result: Any,
        evidence: Optional[dict[str, Any]], command: str = "", cwd: Optional[str] = None,
        task_id: str = "", owner_task_id: str = "", session_key: str = "",
    ) -> bool:
        """Stage a real terminal result; the parent DB proof must commit it later."""
        record = self._foreground_record(transaction)
        custody = getattr(transaction, "custody", None)
        if custody is None or record.binding is None:
            return False
        with self._lock:
            owner_id = custody.owner_id
            if self._foreground_custody_transactions.get(owner_id) is not transaction:
                return False
            record.raw_result = result
            record.finalized_result = finalized_result
            record.evidence = dict(evidence) if isinstance(evidence, dict) else None
            record.phase = "delivery_pending"
            record.command = command
            record.cwd = cwd
            record.task_id = task_id
            record.owner_task_id = owner_task_id or task_id
        return True

    def retain_foreground_finalization_failure(
        self, transaction, *, result: Any = None, evidence: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Retain the original transaction when finalization failed before DB proof."""
        record = self._foreground_record(transaction)
        custody = getattr(transaction, "custody", None)
        if custody is None:
            return False
        record.raw_result = result
        record.evidence = dict(evidence) if isinstance(evidence, dict) else record.evidence
        record.phase = "finalizer_failed"
        custody.mark_unknown("foreground_finalizer_failed")
        return self.retain_foreground_custody(transaction)

    @staticmethod
    def _proof_is_exact(
        proof, binding: ForegroundParentBinding, tool_call_id: str, *, message=None,
    ) -> bool:
        return (
            proof is not None
            and getattr(proof, "raw_flush_result", None) is True
            and getattr(proof, "session_id", None) == binding.session_id
            and getattr(proof, "tool_call_id", None) == tool_call_id == binding.tool_call_id
            and type(getattr(proof, "row_id", None)) is int
            and getattr(proof, "row_id", 0) >= 0
            and getattr(proof, "message", None) is not None
            and isinstance(getattr(proof, "readback", None), Mapping)
            and isinstance(getattr(proof, "readback_digest", None), str)
            and bool(getattr(proof, "readback_digest", ""))
            and isinstance(getattr(proof, "message_digest", None), str)
            and bool(getattr(proof, "message_digest", ""))
            and (message is None or getattr(proof, "message", None) is message)
            and getattr(proof, "database", None) is not None
        )

    def _proof_is_current(
        self, proof, binding: ForegroundParentBinding, tool_call_id: str, transaction, *, message=None,
    ) -> bool:
        """Revalidate the original row and exact native custody before every commit/retry."""
        if not self._proof_is_exact(proof, binding, tool_call_id, message=message):
            return False
        try:
            from agent.context_compressor import _DB_PERSISTED_MARKER
            from agent.tool_result_custody import _payload_digest, read_managed_tool_row

            proof_message = proof.message
            get_role = getattr(proof.database, "get_message_role", None)
            custody = getattr(transaction, "custody", None)
            coordinator = getattr(self, "_runtime_custody", None)
            handle = getattr(custody, "handle", None)
            record = self._foreground_record(transaction)
            if (
                not isinstance(proof_message, dict)
                or _payload_digest(proof_message) != proof.message_digest
                or proof_message.get(_DB_PERSISTED_MARKER) is not True
                or proof_message.get("tool_call_id") != tool_call_id
                or not callable(get_role)
                or custody is None
                or coordinator is None
                or custody.coordinator is not coordinator
                or custody.state not in {"accepted", "finalizing", "unknown"}
                or (
                    custody.state == "unknown"
                    and not (
                        record.durable_proof is proof and custody.durable is True
                    )
                    and custody.reason != "unmapped_writer"
                )
                or record.binding != binding
                or custody.foreground_binding != binding
                or handle is None
                or custody.native_scope is not transaction
                or handle.owner_id != custody.owner_id
                or handle.owner_class != OWNER_CLASS
                or handle.coordinator_id != getattr(coordinator, "_coordinator_id", None)
                or handle.process_instance != custody.process_instance
                or handle.profile_id != custody.profile_id
                or handle.authority_epoch != custody.authority_epoch
                or type(record.delivery_close_generation) is not int
                or handle.close_generation != record.delivery_close_generation
                or custody.process_instance != coordinator.process_instance
                or custody.authority_epoch != coordinator.authority_epoch
            ):
                return False
            if get_role(proof.session_id, proof.row_id) != "tool":
                return False
            if read_managed_tool_row(
                proof.database,
                proof.session_id,
                proof.row_id,
                tool_call_id,
                proof_message,
                expected_readback=proof.readback,
                expected_digest=proof.readback_digest,
            ) is None:
                return False
        except _PROOF_EXCEPTIONS:
            return False
        return True

    def _foreground_transaction_for_binding(self, binding: ForegroundParentBinding):
        with self._lock:
            matches = []
            for transaction in self._foreground_custody_transactions.values():
                record = getattr(transaction, "_foreground_custody_record", None)
                if record is not None and record.binding == binding:
                    matches.append(transaction)
            return matches[0] if len(matches) == 1 else None

    def foreground_binding_active(
        self, *, session_id: str, session_key: str, turn_id: str,
        run_generation: int, tool_call_id: str,
    ) -> bool:
        """Check whether this exact parent call owns a retained foreground transaction."""
        binding = ForegroundParentBinding(session_id, session_key, tool_call_id, turn_id, run_generation)
        return self._foreground_transaction_for_binding(binding) is not None

    def retain_foreground_binding_unknown(
        self, *, session_id: str, session_key: str, turn_id: str,
        run_generation: int, tool_call_id: str, reason: str,
    ) -> bool:
        """Retain the exact binding when its managed lookup is indeterminate."""
        binding = ForegroundParentBinding(session_id, session_key, tool_call_id, turn_id, run_generation)
        transaction = self._foreground_transaction_for_binding(binding)
        if transaction is None:
            return False
        custody = getattr(transaction, "custody", None)
        if custody is None:
            return False
        custody.mark_unknown(reason)
        return self.retain_foreground_custody(transaction, binding=binding)

    def foreground_delivery_token(
        self, *, session_id: str, session_key: str, turn_id: str,
        run_generation: int, tool_call_id: str,
    ) -> str:
        binding = ForegroundParentBinding(session_id, session_key, tool_call_id, turn_id, run_generation)
        transaction = self._foreground_transaction_for_binding(binding)
        if transaction is None:
            return ""
        return self._foreground_record(transaction).delivery_token

    @staticmethod
    def _refresh_foreground_reader_evidence(record: ForegroundCustodyRecord) -> None:
        """Consume a completed reader lease without joining or closing from its reader thread."""
        evidence = record.evidence
        if not isinstance(evidence, dict):
            return
        if evidence.get("deadline_expired") is True:
            return
        reader_state = evidence.get("reader_state")
        if not isinstance(reader_state, dict) or reader_state.get("done") is not True:
            return
        reader_thread = evidence.get("reader_thread") or reader_state.get("thread")
        is_alive = getattr(reader_thread, "is_alive", None)
        if reader_thread is None or not callable(is_alive) or is_alive():
            return
        status = reader_state.get("status") or evidence.get("status")
        if not isinstance(status, str):
            return
        evidence["status"] = status
        evidence["reader_pending"] = False
        evidence["reader_done"] = True
        evidence["drain_joined"] = True
        evidence["true_eof"] = status == "true_eof"
        if reader_state.get("eof_kind") is not None:
            evidence["eof_kind"] = reader_state["eof_kind"]
        process = evidence.get("process")
        stdout = getattr(process, "stdout", None)
        if stdout is None or evidence.get("stdout_closed") is True:
            return
        try:
            stdout.close()
        except (OSError, ValueError):
            evidence["stdout_closed"] = False
        else:
            evidence["stdout_closed"] = True

    def commit_foreground_delivery(
        self, proof, *, session_id: str, session_key: str, turn_id: str,
        run_generation: int, tool_call_id: str, delivery_token: str, message=None,
    ) -> bool:
        """Consume one exact DB proof and finish the already-created native transaction."""
        binding = ForegroundParentBinding(session_id, session_key, tool_call_id, turn_id, run_generation)
        transaction = self._foreground_transaction_for_binding(binding)
        if transaction is None:
            return False
        record = self._foreground_record(transaction)
        self._refresh_foreground_reader_evidence(record)
        custody = getattr(transaction, "custody", None)
        if (
            custody is None
            or not isinstance(delivery_token, str)
            or not delivery_token
            or record.delivery_token != delivery_token
            or not self._proof_is_current(proof, binding, tool_call_id, transaction, message=message)
        ):
            if custody is not None:
                custody.mark_unknown("foreground_durable_proof_missing")
            return False
        if record.durable_proof is not None:
            if record.durable_proof is not proof:
                custody.mark_unknown("foreign_foreground_proof")
                return False
        elif record.phase != "delivery_pending":
            custody.mark_unknown("foreground_delivery_not_pending")
            return False
        record.durable_proof = record.durable_proof or proof
        record.phase = "delivery_committed"
        custody.durable = True
        custody.delivery_acked = True
        if custody.state == "unknown" and not custody.reconcile_unknown():
            self.retain_foreground_custody(transaction)
            return False
        if record.session is None:
            raw = record.raw_result if isinstance(record.raw_result, dict) else {}
            evidence = record.evidence
            try:
                accepted = self.admit_completed_foreground(
                    transaction,
                    command=record.command or str(raw.get("command") or raw.get("cmd") or "foreground-command"),
                    cwd=record.cwd if record.cwd is not None else raw.get("cwd"),
                    task_id=record.task_id or binding.session_key,
                    owner_task_id=record.owner_task_id or binding.session_key,
                    session_key=binding.session_key,
                    output=str(raw.get("output", "")),
                    returncode=raw.get("returncode"),
                    evidence=evidence,
                )
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                accepted = False
            if not accepted:
                custody.mark_unknown("foreground_delivery_settlement_failed")
                self.retain_foreground_custody(transaction)
                return False
        else:
            self._custody_settle_if_ready(record.session)
        if record.session is not None and record.session.custody_state == "terminal":
            record.phase = "terminal"
            custody.delivery_acked = True
            return self.consume_foreground_custody(transaction)
        self.retain_foreground_custody(transaction)
        return False

    def _retry_foreground_records(self) -> None:
        """Retry only records that retain the same exact durable proof and binding."""
        with self._lock:
            records = [
                getattr(transaction, "_foreground_custody_record", None)
                for transaction in self._foreground_custody_transactions.values()
            ]
        for record in records:
            if record is None or record.durable_proof is None or record.binding is None:
                continue
            self._refresh_foreground_reader_evidence(record)
            binding = record.binding
            self.commit_foreground_delivery(
                record.durable_proof,
                session_id=binding.session_id,
                session_key=binding.session_key,
                turn_id=binding.turn_id,
                run_generation=binding.run_generation,
                tool_call_id=binding.tool_call_id,
                delivery_token=record.delivery_token,
                message=record.durable_proof.message,
            )

    def admit_completed_foreground(
        self, transaction, *, command: str, cwd: Optional[str], task_id: str,
        owner_task_id: str, session_key: str, output: str, returncode: Any,
        evidence: Optional[dict[str, Any]],
    ) -> bool:
        """Admit a finished foreground child without reopening its pipe or PID.

        ``adopt_local`` is intentionally reserved for a still-running yield handoff.  A
        foreground wait has already reaped the child and closed its streams, so this seam
        accepts only BaseEnvironment's out-of-band true-EOF witness and the exact transaction
        object that created the child.  Missing evidence leaves the native lease unknown.
        """
        custody = getattr(transaction, "custody", None)
        process = getattr(transaction, "process", None)
        pid = getattr(transaction, "pid", None)
        host_start_time = getattr(transaction, "host_start_time", None)
        reader_state = evidence.get("reader_state") if isinstance(evidence, dict) else None
        reader_thread = (
            evidence.get("reader_thread") if isinstance(evidence, dict) else None
        ) or (reader_state.get("thread") if isinstance(reader_state, dict) else None)
        if (
            custody is None
            or process is None
            or not isinstance(evidence, dict)
            or not isinstance(reader_state, dict)
            or reader_state.get("done") is not True
            or reader_thread is None
            or not callable(getattr(reader_thread, "is_alive", None))
            or reader_thread.is_alive()
            or evidence.get("reader_pending") is True
            or evidence.get("deadline_expired") is True
            or evidence.get("status") != "true_eof"
            or evidence.get("true_eof") is not True
            or evidence.get("eof_kind") != "win32_broken_pipe"
            or evidence.get("natural_exit") is not True
            or evidence.get("drain_joined") is not True
            or evidence.get("stdin_writer_joined") is not True
            or evidence.get("stdout_closed") is not True
            or evidence.get("process") is not process
            or evidence.get("pid") != pid
            or type(pid) is not int
            or pid <= 0
            or type(host_start_time) is not int
            or host_start_time < 0
            or not transaction.matches(process, pid, host_start_time)
            or process.poll() is None
            or returncode is None
            or returncode != process.returncode
            or custody.state != "accepted"
            or custody.native_scope is not transaction
            or transaction.observe_empty() is not True
        ):
            if custody is not None:
                custody.mark_unknown("foreground_evidence_unavailable")
                self.retain_foreground_custody(transaction)
            return False

        session = self._new_session(
            command, task_id, owner_task_id, session_key, cwd,
            custody=custody, reserve_custody=False, notify_on_complete=False,
        )
        session.process = process
        session.pid = pid
        session.host_start_time = host_start_time
        session.pid_scope = "windows_job"
        session.notify_on_complete = False
        if output:
            session.append_output(output)
        self._custody_bind_process(session)
        if session.custody is None or session.custody.state != "accepted":
            if custody is not None:
                custody.mark_unknown("foreground_identity_mismatch")
                self.retain_foreground_custody(transaction)
            return False
        custody.reader_eof = True
        custody.scope_settled = True
        custody.cross_process_proven = custody.process_bound
        record = self._foreground_record(transaction)
        with self._lock:
            self._running[session.id] = session
            record.session = session
        try:
            self._finish_exited(session, returncode, "exited", "foreground")
        except Exception:  # health: allow BLE001 -- finalizer failure retains exact custody
            custody.mark_unknown("foreground_finalizer_failed")
            self.retain_foreground_custody(transaction)
            return False
        return session.custody_state == "terminal"

    @staticmethod
    def _process_profile_id() -> str:
        from hermes_constants import get_hermes_home
        return str(get_hermes_home().resolve())

    def _reserve_process_custody(
        self, session, *, parent_handle: Any = None,
        scope_fence: Optional[ProcessScopeFence] = None,
    ) -> None:
        coordinator = self._runtime_custody
        if coordinator is None:
            return
        custody = ProcessCustody.reserve(
            coordinator,
            f"{coordinator.process_instance}:process:{session.id}",
            self._process_profile_id(),
            parent_handle=parent_handle,
            scope_fence=scope_fence,
        )
        session.custody = custody
        session.profile_id = custody.profile_id
        session.process_instance = custody.process_instance
        session.authority_epoch = custody.authority_epoch
        session.custody_owner_id = custody.owner_id
        session.custody_state = custody.state
        self._custody_observed = True

    def reserve_foreground_custody(
        self, *, profile_id: Optional[str] = None,
        binding: Optional[ForegroundParentBinding] = None,
        parent_handle: Any = None,
        scope_fence: Optional[ProcessScopeFence] = None,
        owner_task_id: Optional[str] = None,
    ):
        """Reserve the lease a foreground launcher must pass into ``adopt_local``.

        The current local environment invokes its yield callback after Popen. A caller that can
        reserve before that side effect passes this handle; an omitted handle is retained as
        unknown instead of receiving a late reservation.
        """
        coordinator = self._runtime_custody
        if coordinator is None:
            return None
        custody = ProcessCustody.reserve(
            coordinator,
            f"{coordinator.process_instance}:process:foreground:{uuid.uuid4().hex}",
            profile_id or self._process_profile_id(),
            parent_handle=parent_handle,
            scope_fence=scope_fence,
        )
        custody.foreground_binding = binding
        if scope_fence is not None:
            if not scope_fence.reserve_foreground(custody.owner_id, owner_task_id or ""):
                custody.mark_unknown("process_scope_unavailable")
            else:
                custody.foreground_reserved = True
        return custody

    @staticmethod
    def _foreground_reader_identity_valid(transaction, custody, proc) -> bool:
        if custody is None or not getattr(custody, "owner_id", ""):
            return False
        if getattr(transaction, "process", None) is not proc:
            return False
        if getattr(transaction, "assigned", False) is not True:
            return False
        if getattr(transaction, "resumed", False) is not True:
            return False
        if getattr(custody, "native_scope", None) is not transaction:
            return False
        if getattr(custody, "state", "") != "accepted":
            return False
        try:
            return transaction.matches(
                proc, getattr(transaction, "pid", None),
                getattr(transaction, "host_start_time", None),
            )
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return False

    @staticmethod
    def _foreground_reader_evidence_valid(reader_thread, reader_stop, evidence) -> bool:
        if reader_thread is None or not callable(getattr(reader_thread, "is_alive", None)):
            return False
        if reader_stop is None or not callable(getattr(reader_stop, "is_set", None)):
            return False
        if reader_stop.is_set() or not reader_thread.is_alive():
            return False
        if not isinstance(evidence, dict) or evidence.get("reader_lease") is not True:
            return False
        return evidence.get("reader_thread") is reader_thread

    def _rollback_foreground_reader_transfer(
        self, transaction, session, record, owner_id, previous_phase,
        previous_evidence, previous_raw_result, previous_finalized_result,
    ) -> None:
        with self._lock:
            self._running.pop(session.id, None)
            if record.session is session:
                record.session = None
                record.phase = previous_phase
                record.evidence = previous_evidence
                record.raw_result = previous_raw_result
                record.finalized_result = previous_finalized_result
            if self._foreground_custody_transactions.get(owner_id) is None:
                self._foreground_custody_transactions[owner_id] = transaction
                self._custody_unbound_sessions.add(owner_id)

    @staticmethod
    def _prepare_foreground_reader_handoff(output):
        begin = getattr(output, "begin_reader_handoff", None)
        commit = getattr(output, "commit_reader_handoff", None)
        abort = getattr(output, "abort_reader_handoff", None)
        if not all(callable(method) for method in (begin, commit, abort)):
            return None
        try:
            receipt = begin()
            token = getattr(receipt, "token", None)
            snapshot = getattr(receipt, "snapshot", None)
            total_chars = getattr(receipt, "total_chars", None)
            if token is None or not isinstance(snapshot, str) or type(total_chars) is not int:
                raise TypeError("reader_handoff_receipt_invalid")
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            logger.warning("foreground reader handoff staging refused: %s", exc, exc_info=True)
            return None
        return (
            receipt,
            lambda sink: commit(token, sink),
            lambda: abort(token),
        )

    def _new_foreground_reader_session(
        self, command, task_id, owner_task_id, session_key, cwd, custody,
        notify_on_complete, abort_handoff,
    ):
        try:
            binding = getattr(custody, "foreground_binding", None)
            if binding is not None and not isinstance(binding, ForegroundParentBinding):
                raise RuntimeError("foreground parent binding is not authenticated")
            if binding is not None and (
                not isinstance(binding.session_id, str) or not binding.session_id
            ):
                raise RuntimeError("foreground parent session identity is unavailable")
            return self._new_session(
                command, task_id, owner_task_id, session_key, cwd,
                custody=custody, reserve_custody=False,
                parent_session_id=binding.session_id if binding is not None else None,
                notify_on_complete=notify_on_complete,
            )
        except Exception as exc:
            logger.warning("foreground reader session creation failed: %s", exc, exc_info=True)
            with suppress(Exception):
                abort_handoff()
            raise

    @staticmethod
    def _seed_foreground_snapshot(session, receipt) -> None:
        """Install the collector's presentation snapshot without counting its notice as raw data."""
        snapshot = receipt.snapshot[-session.max_output_chars:]
        with session._lock:
            session.output_buffer = snapshot
            session.total_output_chars = receipt.total_chars

    @staticmethod
    def _foreground_transfer(session) -> Optional[ForegroundReaderTransfer]:
        custody = getattr(session, "custody", None)
        transfer = getattr(custody, "foreground_transfer", None)
        if isinstance(transfer, ForegroundReaderTransfer):
            return transfer
        return None

    def _foreground_reader_blocks_finish(self, session) -> bool:
        transfer = self._foreground_transfer(session)
        if transfer is not None:
            return transfer.blocks_finish()
        return bool(getattr(session, "_foreground_reader_transfer", False))

    def _foreground_reader_reconcile_guard(self, session) -> bool:
        transfer = self._foreground_transfer(session)
        if transfer is not None and transfer.phase in {"finalizer_start_failed", "scope_pending"}:
            self._retry_transferred_foreground_reader(session)
        return self._foreground_reader_blocks_finish(session)

    def _foreground_reader_finish_authorized(self, session) -> bool:
        transfer = self._foreground_transfer(session)
        return bool(
            transfer is not None
            and transfer.finish_claimed
            and transfer.finish_thread is threading.current_thread()
        )

    def _foreground_reader_finish_guard(self, session) -> bool:
        return (
            self._foreground_reader_blocks_finish(session)
            and not self._foreground_reader_finish_authorized(session)
        )

    def _foreground_reader_blocked_result(self, session) -> dict[str, Any]:
        return {
            "status": "error",
            "error": "managed foreground reader still owns the process transaction",
            "session_id": session.id,
            "process_running": not session.exited,
        }

    def _request_foreground_reader_termination(
        self, session, *, source: str, consume_output: bool,
    ) -> Optional[dict[str, Any]]:
        """Request a stop through the same retained native transaction.

        ``None`` means this is not the typed managed-reader path and the caller may keep
        its ordinary PTY/Popen/backend kill behavior.  A managed transfer always returns a
        result, including a refusal: its exact Job/reader owner must never fall through to a
        PID or newly-created Job path.  A successful request leaves the reader, stdin writer,
        collector, and finisher in place; only that finisher may publish completion.
        """
        transfer = self._foreground_transfer(session)
        if transfer is None:
            return None
        custody = getattr(session, "custody", None)
        transaction = getattr(transfer, "transaction", None)
        if (
            custody is None
            or transaction is None
            or getattr(custody, "native_scope", None) is not transaction
            or getattr(transaction, "process", None) is not session.process
        ):
            self._custody_unknown(session, "foreign_managed_kill_owner")
            return {
                "status": "error",
                "error": "managed_foreground_termination_refused",
                "session_id": session.id,
                "process_running": not session.exited,
            }
        with transfer.lock:
            retry_identity = managed_retry_identity_valid(
                transfer, custody, session, source, consume_output,
            )
            retry_pending = (
                transaction.state == "termination_failed"
                and transaction.termination_retryable is True
            )
            if retry_pending and not retry_identity:
                if source == transfer.kill_source and consume_output == transfer.kill_consume_output:
                    transaction.termination_retryable = False
                return {
                    "status": "error",
                    "error": "managed_foreground_termination_retry_refused",
                    "session_id": session.id,
                    "process_running": not session.exited,
                }
            if not retry_identity and not self._foreground_reader_identity_valid(
                transaction, custody, session.process,
            ):
                if custody.state != "unknown":
                    self._custody_unknown(session, "foreign_managed_kill_owner")
                return {
                    "status": "error",
                    "error": "managed_foreground_termination_refused",
                    "session_id": session.id,
                    "process_running": not session.exited,
                }
            if transfer.phase == "terminal":
                return {
                    "status": "already_exited",
                    "session_id": session.id,
                    "process_running": False,
                }
            if transfer.kill_requested:
                if source != transfer.kill_source:
                    return {
                        "status": "error",
                        "error": "managed_foreground_termination_intent_mismatch",
                        "session_id": session.id,
                        "process_running": not session.exited,
                    }
                transfer.kill_consume_output = transfer.kill_consume_output or consume_output
                return {
                    "status": "kill_requested",
                    "session_id": session.id,
                    "completion_pending": True,
                    "completion_reason": "killed",
                    "termination_source": transfer.kill_source,
                    "process_running": not session.exited,
                }
            retry = retry_identity
            if not retry and (transfer.phase == "unknown" or custody.state != "accepted"):
                return {
                    "status": "error",
                    "error": "managed_foreground_custody_unknown",
                    "session_id": session.id,
                    "process_running": not session.exited,
                }
            if not retry:
                transfer.kill_source = source
                transfer.kill_consume_output = consume_output
            try:
                terminate = transaction.terminate_owned_tree
                terminate()
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                transfer.phase = "unknown"
                self._custody_unknown(session, "managed_termination_failed")
                logger.warning(
                    "managed foreground termination retained exact custody: %s",
                    exc,
                    exc_info=True,
                )
                return {
                    "status": "error",
                    "error": "managed_foreground_termination_unknown",
                    "session_id": session.id,
                    "process_running": not session.exited,
                }
            transfer.kill_requested = True
            return {
                "status": "kill_requested",
                "session_id": session.id,
                "completion_pending": True,
                "completion_reason": "killed",
                "termination_source": transfer.kill_source,
                "process_running": not session.exited,
            }

    def _release_finished_handles(self, session: ProcessSession):
        """Close a finished session's OS handles (Popen pipes / PTY master).

        Best-effort and idempotent: the session may have no local Popen (env
        backends, detached recovery), or the handles may already be closed by
        the reader loop / kill path. Closing a Popen's stream objects does not
        kill anything — the child has already exited — it only releases the
        parent's pipe FDs, which is exactly the retained-resource leak.
        """
        if self._foreground_reader_finish_guard(session):
            return False
        closed = True
        proc = session.process
        if proc is not None:
            for stream in (proc.stdout, proc.stderr, proc.stdin):
                if stream is not None:
                    try:
                        stream.close()
                    except (OSError, ValueError):  # a stdin flush can hit EPIPE
                        closed = False
        if session._pty is not None:
            # A live ptyprocess reader sits in a blocking read holding the PTY file
            # object's buffer lock, and that read only ends once every holder of
            # the slave side is gone. A descendant that setsid()s past the kill
            # keeps it open, so close() here would block forever (under _lock on
            # the prune path). pywinpty reads don't block, so Windows closes here as before.
            reader = session._reader_thread
            if (not _IS_WINDOWS and reader is not None and reader.is_alive()
                    and reader is not threading.current_thread()):
                return False
            # ptyprocess/pywinpty close() is idempotent (``closed`` flag) and
            # closes the master fd exactly once; it raises only if the child
            # ignores SIGKILL, which we don't want to surface on the finish path.
            try:
                session._pty.close()
            except (OSError, ValueError):
                closed = False
        return closed

    def _retry_transferred_foreground_reader(self, session) -> bool:
        transfer = self._foreground_transfer(session)
        if transfer is None or transfer.phase not in {"finalizer_start_failed", "scope_pending"}:
            return False
        with transfer.lock:
            if transfer.finalizer_thread is not None and transfer.finalizer_thread.is_alive():
                return False
        if transfer.phase == "scope_pending":
            custody = getattr(session, "custody", None)
            if custody is None or not reconcile_native_foreground_retry(
                custody,
                transfer,
                session,
                reader_eof=self._foreground_reader_eof_valid(session),
            ):
                return False
        with transfer.lock:
            if transfer.finalizer_thread is not None and transfer.finalizer_thread.is_alive():
                return False
            transfer.phase = "streaming"
            transfer.finalizer_started = True
        try:
            from agent.memory_provider import spawn_context_thread
            finisher = spawn_context_thread(
                self._finish_transferred_foreground_reader,
                args=(session,),
                name=f"proc-foreground-finisher-{session.id}",
            )
            transfer.finalizer_thread = finisher
            session._reader_finish_thread = finisher
            finisher.start()
        except Exception as exc:
            with transfer.lock:
                transfer.phase = "finalizer_start_failed"
            self._custody_unknown(session, "foreground_reader_handoff_failed")
            logger.warning("foreground reader finisher retry failed: %s", exc, exc_info=True)
            self.retain_foreground_custody(transfer.transaction)
            return False
        return True

    def _start_foreground_reader_finisher(self, session, transfer, before_finisher) -> None:
        if not callable(before_finisher):
            raise RuntimeError("foreground_routing_hook_required")
        with transfer.lock:
            if transfer.phase != "registered":
                raise RuntimeError("foreground_transfer_phase_invalid")
            transfer.phase = "routing"
        before_finisher(session)
        transfer.routing_committed = True
        transfer.output.commit_reader_handoff(
            transfer.collector_token, lambda chunk: self._ingest_output(session, chunk),
        )
        with transfer.lock:
            transfer.callback_started = True
            transfer.phase = "streaming"
            transfer.finalizer_started = True
        from agent.memory_provider import spawn_context_thread
        finisher = spawn_context_thread(
            self._finish_transferred_foreground_reader,
            args=(session,),
            name=f"proc-foreground-finisher-{session.id}",
        )
        transfer.finalizer_thread = finisher
        session._reader_finish_thread = finisher
        try:
            finisher.start()
        except (OSError, RuntimeError, TypeError, ValueError):
            with transfer.lock:
                transfer.phase = "finalizer_start_failed"
            raise

    def _build_foreground_reader_session(
        self, transaction, custody, proc, reader_thread, reader_stop, output, evidence,
        receipt, command, task_id, owner_task_id, session_key, cwd, notify_on_complete,
        abort_handoff,
    ) -> Optional[tuple[ProcessSession, ForegroundReaderTransfer]]:
        transfer = None
        try:
            session = self._new_foreground_reader_session(
                command, task_id, owner_task_id, session_key, cwd, custody,
                notify_on_complete, abort_handoff,
            )
            session.process = proc
            session.pid = transaction.pid
            session.host_start_time = transaction.host_start_time
            session.pid_scope = "windows_job"
            transfer = ForegroundReaderTransfer(
                transaction=transaction, process=proc, reader_thread=reader_thread,
                reader_stop=reader_stop, output=output, evidence=evidence,
                deadline=evidence.get("deadline"),
                stdin_thread=evidence.get("stdin_thread"),
                stdin_errors=evidence.get("stdin_errors"),
                initial_output=receipt.snapshot, initial_total_chars=receipt.total_chars,
                output_total_chars=receipt.total_chars, collector_token=receipt.token,
            )
            custody.foreground_transfer = transfer
            self._seed_foreground_snapshot(session, receipt)
            # Compatibility aliases are views of the typed custody transfer; custody remains
            # the sole lifecycle state and native-object owner.
            session._reader_thread = reader_thread
            session._reader_output = output
            session._reader_evidence = evidence
            session._reader_stop = reader_stop
            session._foreground_reader_transfer = True
            session._foreground_handoff_committed = False
            self._custody_bind_process(session)
            if session.custody is None or session.custody.state != "accepted":
                abort_handoff()
                custody.foreground_transfer = None
                return None
            return session, transfer
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            logger.warning("foreground reader session setup refused: %s", exc, exc_info=True)
            if transfer is not None and getattr(custody, "foreground_transfer", None) is transfer:
                custody.foreground_transfer = None
            with suppress(Exception):
                abort_handoff()
            return None

    def _register_foreground_reader_transfer(
        self, transaction, owner_id, session, transfer, record, evidence,
        command, cwd, task_id, owner_task_id,
    ) -> bool:
        with self._lock:
            if self._foreground_custody_transactions.get(owner_id) is not transaction:
                return False
            self._prune_if_needed()
            self._running[session.id] = session
            self._foreground_custody_transactions.pop(owner_id, None)
            self._custody_unbound_sessions.discard(owner_id)
            record.session = session
            record.evidence = evidence
            record.phase = "reader_transferred"
            record.command = command
            record.cwd = cwd
            record.task_id = task_id
            record.owner_task_id = owner_task_id or task_id
            transfer.phase = "registered"
        return True

    def _launch_foreground_reader_finisher(self, session, transfer, transaction, before_finisher) -> bool:
        try:
            self._start_foreground_reader_finisher(session, transfer, before_finisher)
        except Exception as exc:
            with transfer.lock:
                if transfer.phase != "finalizer_start_failed":
                    transfer.phase = "unknown"
            self._custody_unknown(session, "foreground_reader_handoff_failed")
            self.retain_foreground_custody(transaction)
            logger.warning("foreground reader handoff retained as unknown: %s", exc, exc_info=True)
            return False
        return True

    def _checkpoint_foreground_reader_transfer(self, session, transfer, transaction) -> None:
        with transfer.lock:
            if transfer.phase == "terminal":
                return
        try:
            self._write_checkpoint()
        except Exception as exc:
            with transfer.lock:
                if transfer.phase == "terminal":
                    return
                transfer.phase = "unknown"
            self._custody_unknown(session, "foreground_reader_checkpoint_failed")
            self.retain_foreground_custody(transaction)
            logger.warning("foreground reader checkpoint failed; custody retained: %s", exc, exc_info=True)

    def transfer_foreground_reader(
        self, transaction, *, proc, reader_thread, reader_stop, output, evidence,
        command: str, cwd: Optional[str], task_id: str = "", owner_task_id: str = "",
        session_key: str = "", notify_on_complete: bool = True, before_finisher=None,
    ) -> Optional[ProcessSession]:
        """Install one exact live reader under one registry transaction.

        After the foreground map is removed, every failure retains the same session/transaction
        as unknown; no callback, PID, reader or native Job is recreated.
        """
        custody = getattr(transaction, "custody", None)
        owner_id = getattr(custody, "owner_id", "")
        if not self._foreground_reader_identity_valid(transaction, custody, proc):
            return None
        if not self._foreground_reader_evidence_valid(reader_thread, reader_stop, evidence):
            return None
        with self._lock:
            if self._foreground_custody_transactions.get(owner_id) is not transaction:
                return None
        handoff = self._prepare_foreground_reader_handoff(output)
        if handoff is None:
            return None
        receipt, _, abort_handoff = handoff
        built = self._build_foreground_reader_session(
            transaction, custody, proc, reader_thread, reader_stop, output, evidence,
            receipt, command, task_id, owner_task_id, session_key, cwd,
            notify_on_complete, abort_handoff,
        )
        if built is None:
            return None
        session, transfer = built
        record = self._foreground_record(transaction)
        if not self._register_foreground_reader_transfer(
            transaction, owner_id, session, transfer, record, evidence,
            command, cwd, task_id, owner_task_id,
        ):
            with suppress(Exception):
                abort_handoff()
            custody.foreground_transfer = None
            return None
        if not self._launch_foreground_reader_finisher(session, transfer, transaction, before_finisher):
            return session
        self._checkpoint_foreground_reader_transfer(session, transfer, transaction)
        return session

    @staticmethod
    def _foreground_reader_eof_valid(session) -> bool:
        transfer = getattr(getattr(session, "custody", None), "foreground_transfer", None)
        if not isinstance(transfer, ForegroundReaderTransfer):
            return False
        evidence = transfer.evidence
        state = evidence.get("reader_state") if isinstance(evidence, dict) else None
        if not isinstance(state, dict):
            return False
        return (
            evidence.get("reader_lease") is True
            and evidence.get("reader_thread") is transfer.reader_thread
            and state.get("done") is True
            and state.get("thread") is transfer.reader_thread
            and state.get("status") == "true_eof"
            and state.get("eof_kind") == "win32_broken_pipe"
            and evidence.get("deadline_expired") is not True
            and evidence.get("process") is transfer.process
            and evidence.get("pid") == session.pid
            and evidence.get("host_start_time") == session.host_start_time
            and transfer.reader_stop is not None
            and not transfer.reader_stop.is_set()
            and getattr(getattr(transfer.output, "_reader_handoff", None), "token", None)
            is transfer.collector_token
            and getattr(getattr(transfer.output, "_reader_handoff", None), "phase", None) == "committed"
            and not getattr(getattr(transfer.output, "_reader_handoff", None), "pending_dropped", False)
            and not getattr(getattr(transfer.output, "_reader_handoff", None), "uncertain_chars", 0)
        )

    @staticmethod
    def _close_transferred_streams(session) -> bool:
        proc = session.process
        if proc is None:
            return False
        closed = True
        for stream in (getattr(proc, "stdout", None), getattr(proc, "stderr", None), getattr(proc, "stdin", None)):
            if stream is None or getattr(stream, "closed", False):
                continue
            try:
                stream.close()
            except (OSError, ValueError):
                closed = False
        return closed

    def _await_foreground_reader_completion(self, session, transfer):
        reader = transfer.reader_thread
        proc = transfer.process
        if reader is None or proc is None:
            raise RuntimeError("foreground_reader_or_process_missing")
        # The foreground deadline is provenance for admission only. Once the exact reader has
        # been transferred, it follows the ordinary background lifetime and may finish later.
        reader.join()
        if reader.is_alive():
            raise RuntimeError("foreground_reader_join_incomplete")
        if not self._foreground_reader_eof_valid(session):
            raise RuntimeError("foreground_reader_evidence_unavailable")
        proc.wait()
        if proc.returncode is None:
            raise RuntimeError("foreground_exit_unknown")
        return proc

    def _settle_foreground_reader_completion(self, session, transfer) -> None:
        stdin_thread = transfer.stdin_thread
        if stdin_thread is not None:
            stdin_thread.join()
            if stdin_thread.is_alive():
                raise RuntimeError("foreground_stdin_writer_join_incomplete")
        if transfer.stdin_errors:
            raise RuntimeError("foreground_stdin_write_failed")
        if not self._close_transferred_streams(session):
            raise RuntimeError("foreground_stream_close_failed")
        transfer.spill_path = transfer.output.close_spill()
        transfer.output_total_chars = transfer.output.total_chars
        session.output_log = transfer.spill_path or session.output_log
        session.total_output_chars = transfer.output_total_chars
        self._custody_process_finished(session)
        if (
            session.custody is None
            or (
                session.custody.state != "accepted"
                and not managed_retry_complete(
                    transfer, session.custody, require_delivery=False, pre_finish=True,
                )
            )
            or not session.custody.scope_settled
        ):
            raise RuntimeError("foreground_reader_scope_unavailable")

    def _foreground_reader_finish_outcome(self, session, transfer, exit_code):
        """Select the public exit reason only after the original reader proved completion."""
        with transfer.lock:
            kill_requested = transfer.kill_requested
            kill_source = transfer.kill_source
            consume_output = transfer.kill_consume_output
        if not kill_requested:
            return exit_code, "exited", "foreground_reader_transfer"
        if consume_output:
            self._completion_consumed.add(session.id)
        return -15, "killed", kill_source or "process.kill"

    def _finish_transferred_foreground_reader(self, session: ProcessSession) -> None:
        """Finish the same Popen/reader only after bounded native, stdin and stream gates."""
        transfer = self._foreground_transfer(session)
        if transfer is None:
            return
        with transfer.lock:
            if transfer.phase == "terminal":
                return
            transfer.phase = "finalizing"
        try:
            proc = self._await_foreground_reader_completion(session, transfer)
            self._settle_foreground_reader_completion(session, transfer)
            with transfer.lock:
                transfer.phase = "finishing"
                transfer.finish_claimed = True
                transfer.finish_thread = threading.current_thread()
            finish_code, finish_reason, finish_source = self._foreground_reader_finish_outcome(
                session, transfer, proc.returncode,
            )
            moved = self._finish_exited(session, finish_code, finish_reason, finish_source)
            if not self._foreground_reader_finish_complete(session, transfer, moved):
                raise RuntimeError("foreground_reader_finish_unconfirmed")
            with transfer.lock:
                transfer.phase = "terminal"
                transfer.finish_claimed = False
                transfer.finish_thread = None
            session._foreground_reader_transfer = False
        except Exception as exc:
            custody = getattr(session, "custody", None)
            scope_pending = custody is not None and native_scope_pending_reason(
                getattr(custody, "reason", "")
            )
            with transfer.lock:
                transfer.phase = "scope_pending" if scope_pending else "unknown"
                transfer.finish_claimed = False
                transfer.finish_thread = None
            if not scope_pending:
                self._custody_unknown(session, "foreground_reader_finalizer_failed")
            self.retain_foreground_custody(transfer.transaction)
            logger.warning("foreground reader finalizer retained exact custody: %s", exc, exc_info=True)

    def _foreground_reader_finish_complete(self, session, transfer, moved) -> bool:
        custody = getattr(session, "custody", None)
        return bool(
            moved is True
            and session.exited
            and session.id in self._finished
            and custody is not None
            and (
                custody.state not in {"unknown", "not_accepted"}
                or managed_retry_complete(transfer, custody, require_delivery=False)
            )
            and custody.reader_eof
            and custody.durable
            and custody.streams_closed
            and session.total_output_chars == transfer.output_total_chars
        )

    @staticmethod
    def _sync_process_custody(session) -> None:
        custody = session.custody
        if custody is None:
            return
        session.custody_state = custody.state
        session.custody_error = custody.reason
        session.custody_process_bound = custody.process_bound
        session.custody_reader_eof = custody.reader_eof
        session.custody_scope_settled = custody.scope_settled
        session.custody_durable = custody.durable
        session.custody_handle_closed = custody.handle_closed
        session.custody_watcher_registered = custody.watcher_registered
        session.custody_delivery_acked = custody.delivery_acked

    def _custody_bind_process(self, session) -> None:
        if session.custody is None:
            return
        native_scope = session.custody.native_scope
        if native_scope is not None:
            if not native_scope.matches(session.process, session.pid, session.host_start_time):
                session.custody.mark_unknown("foreign_custody")
                self._sync_process_custody(session)
                return
            # A matching Popen/PID/start is identity only.  Job assignment and exact
            # scope capture must be complete before this adapter publishes windows_job;
            # resume remains a separate transaction phase.
            if not getattr(native_scope, "can_bind_process", False):
                session.custody.mark_unknown("native_scope_partial")
                self._sync_process_custody(session)
                return
        session.custody.bind_process(
            session.pid,
            session.host_start_time,
            "windows_job" if native_scope is not None else session.pid_scope,
            session.custody.scope_identity if native_scope is not None else session.systemd_unit,
        )
        self._sync_process_custody(session)

    def _custody_release_unaccepted(self, session) -> bool:
        if session.custody is None:
            return True
        session.custody.release_unaccepted()
        self._sync_process_custody(session)
        return session.custody.state == "not_accepted"

    def _custody_unknown(self, session, reason: str) -> None:
        if session.custody is None:
            return
        session.custody.mark_unknown(reason)
        self._sync_process_custody(session)

    def _custody_settle_if_ready(self, session) -> bool:
        if session.custody is None:
            return False
        if session.custody.state == "unknown" and not session.custody.reconcile_unknown():
            self._sync_process_custody(session)
            return False
        if session.custody.native_scope is not None and not session.custody.handle_closed:
            # KILL_ON_JOB_CLOSE remains armed until the exact Job is observed empty and the
            # delivery witness has been accepted.
            session.custody.close_native()
        was_terminal = session.custody.state == "terminal"
        settled = session.custody.settle()
        self._sync_process_custody(session)
        if settled and not was_terminal:
            self._custody_terminal_count += 1
            if session.custody.delivery_token:
                self._completion_tokens.pop(session.custody.delivery_token, None)
        return settled

    @staticmethod
    def _systemd_scope_observed(unit: str) -> Optional[bool]:
        """Return the observed state of an authenticated transient systemd scope.

        ``None`` means that the unit or its cgroup could not be observed.  It is never
        treated as an empty scope because an unavailable systemd query is an ownership
        failure, not evidence that descendants are gone.
        """
        if not unit or platform.system() != "Linux":
            return None
        try:
            from tools.process_registry import systemd_user_bus_env
            result = subprocess.run(
                ["systemctl", "--user", "show", unit, "--property=ActiveState",
                 "--property=SubState", "--property=ControlGroup"],
                check=True, capture_output=True, text=True, timeout=2,
                env=systemd_user_bus_env(),
            )
        except (OSError, subprocess.SubprocessError, ValueError):
            return None
        values = {}
        for line in result.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator:
                values[key] = value.strip()
        if not {"ActiveState", "SubState", "ControlGroup"}.issubset(values):
            return None
        control_group = values.get("ControlGroup", "")
        if not control_group.startswith("/"):
            return None
        cgroup = Path("/sys/fs/cgroup") / control_group.lstrip("/")
        try:
            members = cgroup.joinpath("cgroup.procs").read_bytes().split()
        except OSError:
            return None
        return not members

    @staticmethod
    def _posix_scope_observed(process_group_id: Optional[int]) -> Optional[bool]:
        """Observe a process-group snapshot without treating an absent target as empty."""
        if type(process_group_id) is not int or process_group_id <= 0:
            return None
        try:
            result = subprocess.run(
                ["ps", "-axo", "pid=,pgid=,stat="],
                check=True, capture_output=True, text=True, timeout=2,
            )
        except (OSError, subprocess.SubprocessError, ValueError):
            return None
        saw_target = False
        for line in result.stdout.splitlines():
            if not line.strip():
                continue
            fields = line.split()
            if len(fields) < 3:
                return None
            try:
                pid, pgid = int(fields[0]), int(fields[1])
            except ValueError:
                return None
            if pid <= 0 or pgid <= 0 or not fields[2]:
                return None
            if pgid == process_group_id:
                saw_target = True
                if not fields[2].startswith("Z"):
                    return False
        return True if saw_target else None

    def _custody_scope_observed(self, session) -> Optional[bool]:
        """Observe a real OS scope; unavailable observations remain unknown."""
        native_scope = getattr(session.custody, "native_scope", None) if session.custody is not None else None
        if native_scope is not None:
            if not native_scope.matches(session.process, session.pid, session.host_start_time):
                return None
            return native_scope.observe_empty()
        if session.pid_scope != "host" or session.pid is None:
            return None
        if session.host_start_time is None:
            return None
        if platform.system() == "Windows":
            return None
        if session.wsl_chain:
            return None
        if session.process is not None:
            if session.process.poll() is None:
                return False
        elif session._pty is None or not hasattr(session._pty, "isalive") or session._pty.isalive():
            return False
        if session.systemd_unit:
            return self._systemd_scope_observed(session.systemd_unit)
        # A host-wide PGID listing cannot establish the complete descendant boundary.  An
        # authenticated systemd scope or native Windows Job is required for a clean proof.
        return None

    def _custody_process_finished(self, session) -> None:
        if session.custody is None:
            return
        custody = session.custody
        custody.reader_eof = True
        scope_observed = self._custody_scope_observed(session)
        custody.scope_settled = scope_observed is True
        if scope_observed is None:
            mark_native_scope_unknown(custody)
        custody.cross_process_proven = custody.process_bound and custody.scope_settled and custody.reader_eof
        self._sync_process_custody(session)
    def _custody_refresh_scope(self, session) -> None:
        """Retry scope observation after durable completion/handle release."""
        if session.custody is None:
            return
        if session.custody.state == "unknown":
            if not session.custody.reconcile_unknown():
                self._sync_process_custody(session)
                return
            if getattr(session.custody.native_scope, "scope_observation_retries", 0) == 1:
                self._sync_process_custody(session)
                return
        scope_observed = self._custody_scope_observed(session)
        if scope_observed is None:
            session.custody.mark_unknown("read_failed")
        elif scope_observed:
            session.custody.scope_settled = True
            session.custody.cross_process_proven = session.custody.process_bound and session.custody.reader_eof
        self._sync_process_custody(session)

    def mark_completion_watcher_registered(self, session_id: str) -> None:
        session = self.get(session_id)
        if session is None or session.custody is None:
            return
        session.custody.watcher_registered = True
        session.custody.delivery_acked = False
        self._sync_process_custody(session)

    def _issue_completion_delivery_token(self, session) -> str:
        if session.custody is None:
            return ""
        handle = session.custody.handle
        close_generation = getattr(handle, "close_generation", None)
        binding = (
            session.id,
            session.custody.owner_id,
            session.custody.profile_id,
            session.custody.process_instance,
            session.custody.authority_epoch,
            close_generation,
        )
        if (
            not isinstance(binding[0], str) or not binding[0]
            or not isinstance(binding[1], str) or not binding[1]
            or not isinstance(binding[2], str) or not binding[2]
            or not isinstance(binding[3], str) or not binding[3]
            or type(binding[4]) is not int
            or type(binding[5]) is not int
        ):
            return ""
        with self._lock:
            token = session.custody.delivery_token
            if not token:
                token = secrets.token_urlsafe(32)
                session.custody.delivery_token = token
                self._completion_tokens[token] = binding
            return token

    def acknowledge_completion_delivery(self, delivery_token: str) -> None:
        """Commit the delivery witness after a queue/watcher consumer accepts the result."""
        if not isinstance(delivery_token, str) or not delivery_token:
            return
        with self._lock:
            binding = self._completion_tokens.get(delivery_token)
            session_id = binding[0] if binding is not None else None
            session = (
                self._running.get(session_id) or self._finished.get(session_id)
                if session_id else None
            )
            if (
                session is None
                or session.custody is None
                or session.custody.delivery_token != delivery_token
                or binding != (
                    session.id,
                    session.custody.owner_id,
                    session.custody.profile_id,
                    session.custody.process_instance,
                    session.custody.authority_epoch,
                    getattr(session.custody.handle, "close_generation", None),
                )
            ):
                return
        if session is None or session.custody is None:
            return
        session.custody.delivery_acked = True
        self._sync_process_custody(session)
        self._custody_settle_if_ready(session)

    def preservation_owner_evidence(
        self, *, process_instance: str, authority_epoch: int, close_generation: int,
        owner_id: str, profile_id: str, task_id: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        from tools.process_registry_custody_evidence import preservation_owner_evidence
        return preservation_owner_evidence(
            self, process_instance=process_instance, authority_epoch=authority_epoch,
            close_generation=close_generation, owner_id=owner_id, profile_id=profile_id,
            task_id=task_id)

    def is_completion_consumed(self, session_id: str) -> bool:
        """Check if a completion notification was already consumed via wait/log."""
        return session_id in self._completion_consumed

    def is_session_waiting(self, session_id: str) -> bool:
        """Whether a goal loop (``hermes_cli.goals`` wait barrier) should stay parked on
        this session: still running AND, with ``watch_patterns``, none matched yet (a
        long-lived watcher unblocks on its trigger, not on exit). Unknown/exited/
        already-fired sessions return False so a stale barrier can never wedge the loop."""
        with self._lock:
            session = (self._running.get(session_id) or self._finished.get(session_id)) if session_id else None
        if session is None:
            return False
        with suppress(Exception):
            refreshed = self._refresh_detached_session(session)
            if refreshed is None:
                return False
            session = refreshed
        return not session.exited and not (
            session.watch_patterns and not session._watch_disabled and session._watch_hits > 0)

    def wait_for_pending_completions(
        self, task_id: Optional[str] = None, *, timeout: float | None = None, poll_interval: float = 1.0,
    ) -> dict:
        """Bounded linger for ``notify_on_complete`` background processes at one-shot exit.
        A one-shot CLI run (``hermes -q/-Q/-z``) exits when its turn ends; a background
        process it spawned still holds a stdout pipe owned by the dying parent and dies of
        SIGPIPE seconds later (Bot Mode handoff replies were the visible casualty). Only
        ``notify_on_complete`` processes carry a completion contract — servers/daemons/
        watchers aren't the parent's to wait for. ``task_id=None`` waits on every tracked
        process; ``timeout=None`` reads ``terminal.oneshot_completion_wait_seconds`` (``<= 0``
        disables). Each pass re-reconciles child state so an orphaned-pipe exit can't wedge
        the linger. Returns ``{"waited", "completed", "timed_out"}`` id lists.

        Bot Mode handoff REPLIES are the visible casualty (#90879): a recipient invoked as ``hermes -p <bot>
        chat -Q --query-file ...`` dispatches its reply via ``message_agent`` / ``bot_relay`` exactly this
        way, then exits, and the reply process is destroyed ~3s later. The sender waits forever for a reply
        that was already killed.
        See #17327.
        """
        if timeout is None:
            timeout = self._oneshot_completion_wait_seconds()
        result: dict = {"waited": [], "completed": [], "timed_out": []}
        with self._lock:
            # `_finished` too: `_move_to_finished` pops a session from `_running` and enqueues its completion
            # only after releasing handles and writing the checkpoint. A parent whose turn ends inside that
            # window would otherwise see nothing pending, drain nothing and exit without the follow-up turn.
            pending = [
                s for store in (self._running, self._finished) for s in store.values()
                if s.notify_on_complete and not s._completion_event.is_set()
                and (task_id is None or s.owner_task_id == task_id)
            ]
        if not pending or timeout <= 0:
            return result
        result["waited"] = [s.id for s in pending]
        logger.info(
            "One-shot exit lingering (bounded %ss) for %d notify_on_complete "
            "background process(es): %s",
            timeout, len(pending), ", ".join(s.id for s in pending))
        deadline = time.monotonic() + max(float(timeout), 0.0)
        interval = max(float(poll_interval), 0.05)
        try:
            from tools.interrupt import is_interrupted as _is_interrupted
        except Exception:
            _is_interrupted = lambda: False
        interrupted = False
        for session in pending:
            try:
                while not session._completion_event.is_set():
                    if interrupted or _is_interrupted():
                        interrupted = True
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    # Reconcile first so orphaned-pipe and detached exits fire the event.
                    with suppress(Exception):
                        # Reconcile first: catches direct-child exits whose reader is blocked on a pipe held
                        # open by a descendant (#17327) and detached/env sessions, so the event actually
                        # fires.
                        # Reconcile against real child state before reading session.exited. Guards against
                        # orphaned-pipe reader hangs (issue #17327).
                        # Reconcile against real child state — guards against orphaned- pipe reader hangs
                        # where the reader is blocked but the direct child has already exited (issue
                        # #17327).
                        self._reconcile_local_exit(session)
                        if self._refresh_detached_session(session) is None:
                            break
                    if session._completion_event.is_set():
                        break
                    session._completion_event.wait(min(remaining, interval))
            except KeyboardInterrupt:
                # Stop waiting, but never let the interrupt skip the caller's durable
                # teardown (session flush, end_session) that follows.
                interrupted = True
            result["completed" if session._completion_event.is_set() else "timed_out"].append(session.id)
        if result["timed_out"]:
            logger.warning(
                "One-shot exit linger timed out after %ss with %d background "
                "process(es) still running: %s — they may be killed when this "
                "process exits.",
                timeout, len(result["timed_out"]), ", ".join(result["timed_out"]))
        return result

    def _drain_should_skip(self, session_id: str, *, skip_poll_observed: bool = True) -> bool:
        """Skip a completion the CLI agent already has this turn — consumed via wait/log
        or observed inline via poll(). Gateway/tui watchers check only
        ``is_completion_consumed`` so a read-only poll never suppresses their turn.

        Skips when the agent has either truly consumed the output (wait/log → ``_completion_consumed``) or
        observed the exit inline via poll() (``_poll_observed``). In both cases the CLI agent already has
        the result this turn, so injecting a [SYSTEM: ...] completion would be a duplicate (#8228).
        """
        return session_id in self._completion_consumed or (skip_poll_observed and session_id in self._poll_observed)

    @staticmethod
    def _owns_event(evt: dict, session_key: str, owns_event, is_async_delegation: bool) -> bool:
        """Routing verdict for one drained event (see drain_notifications); False = requeue."""
        evt_session_key = str(evt.get("session_key") or "")
        requires_positive_proof = is_async_delegation or bool(evt_session_key or evt.get("origin_ui_session_id"))
        if owns_event is not None and requires_positive_proof:
            try:
                return bool(owns_event(evt))
            except Exception:
                return False  # fail closed — never leak on a broken check
        if session_key and requires_positive_proof:
            return evt_session_key == session_key
        # Restored payloads from a previous process: an unfiltered drain cannot prove
        # ownership, so leave them for the owner.
        return not (is_async_delegation and evt.get("restored"))

    def restore_completions(self) -> int:
        """Rehydrate durable delegation completions from the LAUNCH profile's ledger, once per
        process. Called by the first consumer that drains the queue (CLI/TUI drain, gateway boot,
        TUI poller) so a mere ``import model_tools`` never touches state.db (#123265). The replay
        always runs in the launch scope: the TUI poller / prompt_turn drain call this under the
        session's profile binding, and a once-per-process replay taken under a secondary's scope
        would leave the launch ledger unreplayed for the life of the process. Secondaries are
        replayed by the gateway's ``_restore_secondary_completion_ledgers``."""
        if self._completions_restored:
            return 0
        self._completions_restored = True
        from hermes_constants import reset_hermes_home_override, set_hermes_home_override
        token = set_hermes_home_override(None)
        try:
            from tools.async_delegation import restore_undelivered_completions
            return restore_undelivered_completions(self.completion_queue)
        except Exception as exc:
            logger.warning("Could not restore async delegation completions: %s", exc)
            return 0
        finally:
            reset_hermes_home_override(token)

    def drain_notifications(
        self, session_key: str = "", owns_event=None, *, skip_poll_observed: bool = True,
    ) -> list[tuple[dict, str]]:
        """Pop all pending events and return ``(raw_event, formatted_text)`` pairs.
        Skips completions per ``_drain_should_skip`` (gateway/TUI pass
        ``skip_poll_observed=False``). Routing (``_owns_event``): async-delegation events
        always need ownership proof, ordinary events once they carry ``session_key`` or
        ``origin_ui_session_id``; ``owns_event(evt)`` (strongest; the TUI passes a
        compression-chain-aware check) consumes ONLY on True, ``session_key`` uses plain
        equality; non-owned events are re-queued for their owner. No filter consumes
        everything (legacy single-session) except restored delegation payloads (fail-closed)."""
        self.restore_completions()
        results: list[tuple[dict, str]] = []
        requeue: list[dict] = []
        # delegation.surface_child_process_notifications, read at most once per drain
        # and only when an sa- event shows up.
        surface_child: bool | None = None
        while not self.completion_queue.empty():
            try:
                evt = self.completion_queue.get_nowait()
            except Exception:
                break
            is_async_delegation = evt.get("type") == "async_delegation"
            if not self._owns_event(evt, session_key, owns_event, is_async_delegation):
                requeue.append(evt)
                continue
            # Routing happened first so a foreign session cannot drop the owner's
            # event via its own consumed/observed state.
            _evt_sid = evt.get("session_id", "")
            if evt.get("type") == "completion" and self._drain_should_skip(
                _evt_sid, skip_poll_observed=skip_poll_observed):
                continue
            # Subagent-owned process notifications are suppressed by default — the
            # child's delegation result is the deliverable. Judge ownership on
            # owner_task_id (RAW spawning id; task_id is the container key, collapsed
            # by _resolve_container_task_id). Dropped, NOT requeued: children never
            # drain, so a requeue would pin the event forever. 'async_delegation'
            # is the result itself and is NEVER suppressed.
            _evt_task_id = str(evt.get("owner_task_id") or evt.get("task_id") or "")
            if is_child_process_notification(evt):
                if surface_child is None:
                    surface_child = self._surface_child_process_notifications()
                if not should_surface_notification(evt, surface_child=surface_child):
                    logger.debug(
                        "Suppressed subagent-owned process notification "
                        "(delegation.surface_child_process_notifications=false): "
                        "type=%s session_id=%s task_id=%s",
                        evt.get("type", "completion"), _evt_sid, _evt_task_id)
                    continue
            if text := format_process_notification(evt):
                # Formatting and queue removal are not downstream admission. The TUI/CLI or
                # gateway consumer must acknowledge only after it accepts this completion.
                results.append((evt, text))
        for evt in requeue:
            self.completion_queue.put(evt)
        return results
