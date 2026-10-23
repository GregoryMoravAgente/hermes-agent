"""Native process and reader receipt for one canonical I16 service lease.

This sibling keeps the physical witness beside the service custody contract without
creating a second ledger.  The source module re-exports ``ServiceProcessReceipt``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import subprocess
import threading
import time
from typing import TYPE_CHECKING, Any

from gateway.runtime_service_custody import (
    CustodyHandle,
    DurableResultProof,
    ServiceCustodyError,
    TurnCustodyBinding,
    _EXTERNAL_CLOSE_RETRY_REASONS,
    _MANAGED_PROCESS_RETRY_REASONS,
    _authoritative_admission_row,
    _canonical_store_path,
    _require_authoritative_row,
    _require_binding_shape,
    _require_live_execution,
    _require_terminal_binding,
)
from gateway.runtime_sidecar_transport import required_reader_streams

if TYPE_CHECKING:
    from gateway.runtime_service_custody import ServiceLease


_PROCESS_RETRY_CLEANUP_BUDGET_SECONDS = 2.0


@dataclass
class ServiceProcessReceipt:
    """One exact Popen/reader/Job receipt below a persistent service lease."""

    service: ServiceLease
    handle: CustodyHandle
    owner_id: str
    state: str = "reserved"
    # A receipt reset by an exact same-handle failure reconciliation may be reused
    # by the next spawn; an ordinary reserved receipt still belongs to its first bind.
    retry_ready: bool = False
    transaction: Any = None

    @property
    def native_scope(self) -> Any:
        """Expose this receipt's exact native transaction to its Windows owner."""
        return self.transaction

    # The ACP setup operation owns this exact fence from before either reader pump is created.
    # A same-handle retry cannot replace the receipt while the fence is still in flight.
    setup_fence: threading.Event | None = field(default=None, repr=False)
    process: Any = None
    pid: int | None = None
    process_started_at: int | None = None
    scope: str | None = None
    scope_identity: str | None = None
    # Shell hooks own a POSIX process group, so leader exit alone is not a physical
    # close proof.  This identity remains on this receipt until the group is empty.
    process_group_id: int | None = None
    stdout_eof: bool = False
    stderr_eof: bool = False
    stdout_joined: bool = False
    stderr_joined: bool = False
    reader_error: str | None = None
    unknown_reason: str | None = None
    # These legacy aliases remain diagnostic only; acceptance uses the keyed maps below.
    durable_proof: DurableResultProof | None = None
    delivery_complete: bool = False
    # A source-owned transport may supply a typed native close witness when it has no
    # durable result map (standalone CUA is the current case).  This is attached only
    # after the same source receipt has observed its exact process/context close.
    source_receipt: Any = field(default=None, repr=False)
    readers: dict[str, Any] = field(default_factory=dict)
    # Reader construction is separate from reader start.  A start exception is
    # ambiguous: retain that exact reader as unknown instead of treating an
    # absent thread ident as proof that no reader effect occurred.
    reader_start_states: dict[str, str] = field(default_factory=dict)
    eof_streams: set[str] = field(default_factory=set)
    joined_streams: set[str] = field(default_factory=set)
    durable_proofs: dict[str, DurableResultProof] = field(default_factory=dict)
    durable_proof_history: list[DurableResultProof] = field(default_factory=list, repr=False)
    delivery_requests: set[str] = field(default_factory=set)
    completed_requests: set[str] = field(default_factory=set)
    external_publication_proof: Any = None
    # ACP prompt children are one-request processes; Codex app-server receipts remain
    # persistent across admissions and must not be closed at each delivery fence.
    ephemeral_process: bool = False
    # Native close is two-phase for managed delivery: the process/Job is closed while the
    # exact custody maps remain, then the physical coordinator handle is committed.
    native_close_committed: bool = False
    # Managed ACP keeps the immutable child witness on the physical receipt until the
    # writer, delivery, and native close fences have all committed.  This is one
    # receipt-local witness, not a second custody ledger.
    managed_witness: dict[str, Any] | None = None
    # A managed turn may need more than one short-lived provider child (for example, a
    # tool-call response followed by the post-tool answer).  Keep the completed child
    # identities on this same physical receipt while the admission remains open; the
    # coordinator handle and the canonical proof map remain singular.
    managed_child_history: list[dict[str, Any]] = field(default_factory=list, repr=False)
    managed_child_ready: bool = False
    managed_client: Any = field(default=None, repr=False)
    # A Codex client can fail after native launch but before the client object is
    # published to its session.  Keep that partial construction owner attached to
    # this exact receipt until native cleanup and retry reconciliation finish.
    construction_owner: Any = field(default=None, repr=False)

    def retain_construction_owner(self, owner: Any) -> bool:
        """Attach the exact transport object while its constructor is still in flight."""
        if (
            owner is None
            or self.state not in {"reserved", "identity_bound"}
            or self.process is not None
            or (self.construction_owner is not None and self.construction_owner is not owner)
        ):
            self.mark_unknown("construction_owner_mismatch")
            return False
        self.construction_owner = owner
        return True

    def clear_construction_owner(self, owner: Any) -> bool:
        """Clear only the owner that completed this exact construction."""
        if self.construction_owner is not owner:
            self.mark_unknown("construction_owner_mismatch")
            return False
        self.construction_owner = None
        return True

    def retain_launch_transaction(self, transaction: Any) -> bool:
        """Attach one native launch transaction before it can create a process effect."""
        if (
            transaction is None
            or self.state not in {"reserved", "identity_bound"}
            or self.process is not None
            or getattr(transaction, "custody", None) is not self
            or getattr(transaction, "process", None) is not None
            or getattr(transaction, "state", None) != "reserved"
        ):
            self.mark_unknown("launch_transaction_identity_mismatch")
            return False
        existing = self.transaction
        if existing is not None and existing is not transaction:
            self.mark_unknown("launch_transaction_replaced")
            return False
        self.transaction = transaction
        self.retry_ready = False
        return True

    def retain_partial_launch(
        self, process: Any, transaction: Any = None, *, reason: str = "partial_launch",
        scope: str | None = None, scope_identity: str | None = None,
        process_group_id: int | None = None,
    ) -> None:
        """Keep the exact process/transaction objects when a later bind step fails."""
        retained_transaction = transaction if transaction is not None else self.transaction
        if transaction is not None:
            if (
                getattr(transaction, "custody", None) is not self
                or (
                    self.transaction is not None
                    and self.transaction is not transaction
                )
            ):
                self.mark_unknown("launch_transaction_identity_mismatch")
            else:
                self.transaction = transaction
        if process is not None:
            transaction_process = getattr(retained_transaction, "process", None)
            if transaction_process is not None and transaction_process is not process:
                self.mark_unknown("launch_process_identity_mismatch")
            else:
                self.process = process
                process_pid = getattr(retained_transaction, "pid", None)
                if type(process_pid) is not int or process_pid <= 0:
                    process_pid = getattr(process, "pid", None)
                if type(process_pid) is int and process_pid > 0:
                    self.pid = process_pid
                process_start = getattr(retained_transaction, "host_start_time", None)
                if type(process_start) is int and process_start > 0:
                    self.process_started_at = process_start
                if retained_transaction is not None:
                    self.scope = "windows_job"
                    self.scope_identity = f"windows_job:{self.owner_id}:{self.pid or 'unknown'}"
                else:
                    self.scope = scope if scope is not None else "posix_direct"
                    self.scope_identity = scope_identity if scope_identity is not None else "posix_direct"
                    self.process_group_id = process_group_id
        self.retry_ready = False
        self.mark_unknown(reason)

    def bind_process(
        self, pid: int, process_started_at: int | None, scope: str, scope_identity: str,
        *, process_group_id: int | None = None,
    ) -> bool:
        if self.state not in {"reserved", "identity_bound"}:
            return False
        if type(pid) is not int or pid <= 0 or type(scope) is not str or not scope:
            return False
        if scope == "windows_job":
            if type(process_started_at) is not int or process_started_at <= 0:
                return False
            if type(scope_identity) is not str or not scope_identity.startswith("windows_job:"):
                return False
        elif scope == "posix_process_group":
            if (
                type(process_group_id) is not int
                or process_group_id <= 0
                or scope_identity != f"posix_process_group:{pid}:{process_group_id}"
            ):
                return False
        elif scope != "posix_direct" or scope_identity != "posix_direct":
            return False
        self.pid = pid
        self.process_started_at = process_started_at
        self.scope = scope
        self.scope_identity = scope_identity
        self.process_group_id = process_group_id if scope == "posix_process_group" else None
        self.retry_ready = False
        self.managed_child_ready = False
        self.state = "identity_bound"
        return True

    def bind_launch(self, process: Any, transaction: Any = None) -> None:
        if self.state not in {"reserved", "identity_bound"} or process is None:
            self.mark_unknown("launch_identity_mismatch")
            raise ServiceCustodyError("physical_launch_identity_mismatch")
        if transaction is not None:
            if (
                (
                    self.scope == "windows_job"
                    and getattr(transaction, "custody", None) is not self
                )
                or (self.transaction is not None and self.transaction is not transaction)
                or getattr(transaction, "process", None) is not process
                or getattr(transaction, "pid", None) != self.pid
                or getattr(transaction, "host_start_time", None) != self.process_started_at
                or getattr(transaction, "assigned", False) is not True
                or getattr(transaction, "resumed", False) is not True
            ):
                self.mark_unknown("launch_identity_mismatch")
                raise ServiceCustodyError("physical_launch_identity_mismatch")
        elif self.scope not in {"posix_direct", "posix_process_group"} or self.transaction is not None:
            self.mark_unknown("launch_transaction_missing")
            raise ServiceCustodyError("physical_launch_transaction_missing")
        else:
            process_pid = getattr(process, "pid", None)
            if type(process_pid) is not int or process_pid != self.pid:
                # POSIX has no native transaction object to authenticate the Popen instance.  The
                # exact process object's pid must therefore match the identity already admitted on
                # this receipt; a copied/stale pid is insufficient even when the process is stopped.
                self.mark_unknown("launch_identity_mismatch")
                raise ServiceCustodyError("physical_launch_identity_mismatch")
        self.process = process
        self.transaction = transaction
        self.retry_ready = False
        self.managed_child_ready = False
        self.state = "process_bound"

    def can_retry_external_close(self) -> bool:
        """Return whether the existing external receipt may retry its same-handle close."""
        return self.state == "unknown" and self.unknown_reason in _EXTERNAL_CLOSE_RETRY_REASONS

    def release_unaccepted(self) -> bool:
        """Adapter name consumed by WindowsLaunchTransaction on a verified no-Popen refusal."""
        transaction = self.transaction
        if transaction is None:
            return self.release_before_effect()
        if (
            self.process is not None
            or getattr(transaction, "process", None) is not None
            or getattr(transaction, "assigned", False) is True
            or getattr(transaction, "no_effect_verified", False) is not True
            or getattr(transaction, "state", None) != "released"
            or getattr(transaction, "job_handle_closed", False) is not True
        ):
            self.mark_unknown("partial_launch")
            return False
        result = self.service.coordinator.release_unaccepted(self.handle)
        if result is not None:
            self.mark_unknown(getattr(result, "code", "release_failed"))
            return False
        self.transaction = None
        self.state = "released"
        self.service.physical = None
        return True

    def release_before_effect(self) -> bool:
        transaction = self.transaction
        if self.ephemeral_process and (self.durable_proofs or self.managed_child_history):
            # A later child may have no effect, but the same physical receipt already
            # carries a committed prefix.  Do not release that handle; retain it for an
            # exact same-owner retry and the eventual full-turn delivery fence.
            self.mark_unknown("spawn_not_started")
            return False
        if transaction is not None:
            if self.state not in {"reserved", "identity_bound"} or self.process is not None:
                self.mark_unknown("partial_launch")
                return False
            if getattr(transaction, "no_effect_verified", False) is not True:
                self.mark_unknown("native_launch_ambiguous")
                return False
            if transaction.release_before_effect() is not True:
                return False
            return (
                self.state == "released"
                and self.service.physical is None
                and getattr(transaction, "job_handle_closed", False) is True
            )
        if self.state != "reserved" or self.process is not None:
            self.mark_unknown("partial_launch")
            return False
        result = self.service.coordinator.release_unaccepted(self.handle)
        if result is not None:
            self.mark_unknown(getattr(result, "code", "release_failed"))
            return False
        self.state = "released"
        self.service.physical = None
        return True

    def prepare_process_retry(self) -> bool:
        """Close retained native objects only after the original setup fence is terminal."""
        deadline = time.monotonic() + _PROCESS_RETRY_CLEANUP_BUDGET_SECONDS
        if not self.stop_process_for_retry(deadline=deadline):
            return False
        return self.close_native_after_process()

    def _posix_process_group_empty(self) -> bool:
        """Read back a contained POSIX scope; a bare PGID is never proof."""
        if self.scope != "posix_process_group":
            return False
        provider = getattr(self, "contained_scope_provider", None)
        observe = getattr(provider, "observe_empty", None) if provider is not None else None
        return callable(observe) and observe() is True

    def _stop_posix_process_group(self) -> bool:
        """Terminate only through an authenticated contained-scope provider."""
        if self.scope != "posix_process_group":
            return False
        provider = getattr(self, "contained_scope_provider", None)
        terminate = getattr(provider, "terminate", None) if provider is not None else None
        if not callable(terminate):
            return False
        try:
            terminate()
        except (OSError, RuntimeError, TypeError, ValueError):
            return False
        return self._posix_process_group_empty()

    def _posix_delivery_ready(self) -> bool:
        if self.scope == "posix_process_group":
            return self._posix_process_group_empty()
        return self.scope == "posix_direct" and self.process is not None and self.process.poll() is not None

    @staticmethod
    def _drain_windows_transaction(transaction: Any, deadline: float) -> bool:
        """Drain the retained Job once, within the shared retry deadline."""
        drain = getattr(transaction, "drain_after_process_exit", None)
        if not callable(drain):
            return False
        remaining = max(0.0, deadline - time.monotonic())
        if remaining <= 0.0:
            return False
        if drain(remaining) is not True:
            return False
        return time.monotonic() <= deadline

    def stop_process_for_retry(self, *, deadline: float | None = None) -> bool:
        """Stop the retained process while leaving readers and native handles reachable."""
        setup_fence = self.setup_fence
        if setup_fence is not None and not setup_fence.is_set():
            return False
        if deadline is None:
            deadline = time.monotonic() + _PROCESS_RETRY_CLEANUP_BUDGET_SECONDS
        process = self.process
        transaction = self.transaction
        if process is None:
            return transaction is None
        try:
            if transaction is not None:
                if getattr(transaction, "process", None) is not process:
                    return False
                if process.poll() is None:
                    terminate = getattr(transaction, "terminate_owned_tree", None)
                    if not callable(terminate):
                        return False
                    terminate()
                    remaining = max(0.0, deadline - time.monotonic())
                    if remaining <= 0.0:
                        return False
                    process.wait(timeout=remaining)
                if process.poll() is None:
                    return False
                return self._drain_windows_transaction(transaction, deadline)
            if self.scope == "posix_process_group":
                return self._stop_posix_process_group()
            if self.scope != "posix_direct":
                return False
            if process.poll() is None:
                process.terminate()
                remaining = max(0.0, deadline - time.monotonic())
                if remaining <= 0.0:
                    return False
                process.wait(timeout=remaining)
            return process.poll() is not None
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, subprocess.TimeoutExpired):
            return False

    def close_native_after_process(self) -> bool:
        """Observe and close native handles after the exact process has exited."""
        process = self.process
        transaction = self.transaction
        if process is None:
            return transaction is None
        try:
            if process.poll() is None:
                return False
            if transaction is not None:
                if getattr(transaction, "process", None) is not process:
                    return False
                if not getattr(transaction, "native_empty_observed", False):
                    if transaction.observe_empty() is not True:
                        return False
                if transaction.close_after_observed_empty() is not True:
                    return False
                return (
                    getattr(transaction, "state", None) == "closed"
                    and getattr(transaction, "process_handle_closed", False) is True
                    and getattr(transaction, "job_handle_closed", False) is True
                )
            if self.scope == "posix_process_group":
                return self._posix_process_group_empty()
            return self.scope == "posix_direct"
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return False

    def bind_reader(self, stream: str, reader: Any) -> None:
        if stream not in {"stdout", "stderr"} or reader is None or not callable(getattr(reader, "is_alive", None)):
            self.mark_unknown("reader_identity_missing")
            raise ServiceCustodyError("reader_identity_missing")
        existing = self.readers.get(stream)
        if existing is not None and existing is not reader:
            self.mark_unknown("reader_identity_replaced")
            raise ServiceCustodyError("reader_identity_replaced")
        self.readers[stream] = reader
        self.reader_start_states.setdefault(stream, "bound")

    def reader_started(self, stream: str, reader: Any = None) -> bool:
        expected = self.readers.get(stream)
        if (
            stream not in {"stdout", "stderr"}
            or expected is None
            or (reader is not None and reader is not expected)
            or self.reader_start_states.get(stream) != "bound"
        ):
            self.reader_start_failed(stream, reader, "reader_start_state_invalid")
            return False
        self.reader_start_states[stream] = "started"
        return True

    def reader_start_failed(
        self, stream: str, reader: Any = None, reason: str = "reader_start_unknown",
    ) -> bool:
        expected = self.readers.get(stream)
        if (
            stream not in {"stdout", "stderr"}
            or expected is None
            or (reader is not None and reader is not expected)
        ):
            self.mark_unknown("reader_state_invalid")
            return False
        self.reader_start_states[stream] = "unknown"
        self.reader_error = reason
        self.mark_unknown(reason)
        return False

    def reader_eof(self, stream: str, reader: Any = None) -> None:
        expected = self.readers.get(stream)
        if (stream not in {"stdout", "stderr"} or expected is None
                or (reader is not None and reader is not expected)
                or self.state in {"closed", "released"}):
            self.mark_unknown("reader_state_invalid")
            return
        self.eof_streams.add(stream)
        setattr(self, f"{stream}_eof", True)
        if self.state == "process_bound":
            self.state = "readers_active"

    def reader_joined(self, stream: str, reader: Any = None) -> None:
        expected = self.readers.get(stream)
        if (stream not in {"stdout", "stderr"} or expected is None
                or (reader is not None and reader is not expected)
                or not callable(getattr(expected, "is_alive", None))):
            self.mark_unknown("reader_state_invalid")
            return
        try:
            alive = expected.is_alive()
        except (OSError, RuntimeError, TypeError, ValueError):
            self.mark_unknown("reader_state_unknown")
            return
        if alive is not False:
            self.mark_unknown("reader_still_alive")
            return
        self.joined_streams.add(stream)
        setattr(self, f"{stream}_joined", True)

    def reader_failed(self, reason: str) -> None:
        self.reader_error = reason if isinstance(reason, str) and reason else "reader_failed"
        self.mark_unknown("reader_failed")

    def mark_unknown(self, reason: str) -> None:
        if self.state in {"closed", "released"}:
            return
        if self.state == "unknown":
            return
        self.state = "unknown"
        self.unknown_reason = reason
        self.service.unknown = True
        self.service.coordinator.mark_unknown(self.handle, reason)

    def bind_source_receipt(self, receipt: Any) -> None:
        """Retain one exact source transport witness for a result-less close path."""
        owner = getattr(receipt, "owner", None)
        parent = getattr(owner, "parent", None)
        service = self.service
        if (
            receipt is None
            or getattr(receipt, "physical", None) is not self
            or getattr(receipt, "physical_process", None) is not self.process
            or getattr(receipt, "state", None) != "closed"
            or not callable(getattr(receipt, "readback", None))
            or receipt.readback() is None
            or getattr(parent, "_service", None) is not service
            or getattr(parent, "_physical", None) is not self
            or getattr(parent, "_cua_receipt", None) is not receipt
            or getattr(service, "external_parent", None) is not parent
        ):
            self.mark_unknown("source_receipt_identity_mismatch")
            raise ServiceCustodyError("source_receipt_identity_mismatch")
        current = self.source_receipt
        if current is not None and current is not receipt:
            self.mark_unknown("source_receipt_replaced")
            raise ServiceCustodyError("source_receipt_replaced")
        self.source_receipt = receipt

    def record_result_durable(
        self, binding: TurnCustodyBinding, request_id: str, proof: DurableResultProof,
    ) -> None:
        if self.state in {"unknown", "closed", "released"}:
            raise ServiceCustodyError("physical_custody_unknown")
        if type(request_id) is not str or not request_id:
            self.mark_unknown("durable_request_identity_missing")
            raise ServiceCustodyError("durable_request_identity_missing")
        from gateway.session_turn_physical_tip import physical_proof_session
        physical_session_id = physical_proof_session(binding, proof)
        if physical_session_id is None:
            self.mark_unknown("durable_result_identity_mismatch")
            raise ServiceCustodyError("durable_result_identity_mismatch")
        if (
            proof.store_path != _canonical_store_path(binding.authority)
            or proof.session_id != physical_session_id
            or proof.admission_id != binding.admission_id
            or proof.execution_generation != binding.execution_generation
            or type(proof.row_ids) is not tuple
            or not proof.row_ids
            or any(type(row_id) is not int or row_id <= 0 for row_id in proof.row_ids)
            or type(proof.row_snapshots) is not tuple
            or len(proof.row_snapshots) != len(proof.row_ids)
            or any(type(snapshot) is not str or not snapshot for snapshot in proof.row_snapshots)
            or type(proof.row_sessions) is not tuple
            or len(proof.row_sessions) != len(proof.row_ids)
            or any(session != physical_session_id for session in proof.row_sessions)
            or type(proof.row_roles) is not tuple
            or len(proof.row_roles) != len(proof.row_ids)
            or any(type(role) is not str or not role for role in proof.row_roles)
            or type(proof.payload_digest) is not str
            or not proof.payload_digest
            or type(proof.result_row_ids) is not tuple
            or not proof.result_row_ids
            or any(type(row_id) is not int or row_id <= 0 for row_id in proof.result_row_ids)
            or len(set(proof.result_row_ids)) != len(proof.result_row_ids)
            or any(row_id not in proof.row_ids for row_id in proof.result_row_ids)
            or len(proof.result_row_ids) >= len(proof.row_ids)
            or tuple(proof.row_ids[-len(proof.result_row_ids):]) != proof.result_row_ids
            or proof.row_roles[-len(proof.result_row_ids) - 1] != "user"
            or any(role not in {"assistant", "tool"} for role in proof.row_roles[-len(proof.result_row_ids):])
            or hashlib.sha256(
                "\n".join(
                    f"{row_id}:{session}:{role}:{snapshot}"
                    for row_id, session, role, snapshot in zip(
                        proof.row_ids, proof.row_sessions, proof.row_roles, proof.row_snapshots)
                ).encode("utf-8")
            ).hexdigest() != proof.payload_digest
        ):
            self.mark_unknown("durable_result_identity_mismatch")
            raise ServiceCustodyError("durable_result_identity_mismatch")
        prior = self.durable_proofs.get(request_id)
        if prior is not None and prior != proof:
            from gateway.runtime_service_managed_children import accepts_managed_proof_update
            if not accepts_managed_proof_update(self, prior, proof):
                self.mark_unknown("durable_result_replaced")
                raise ServiceCustodyError("durable_result_replaced")
            self.durable_proof_history.append(prior)
        elif prior is None:
            self.durable_proof_history.append(proof)
        self.durable_proofs[request_id] = proof
        self.durable_proof = proof
        if self.ephemeral_process:
            witness = self.managed_witness
            if not isinstance(witness, dict):
                self.mark_unknown("managed_witness_missing")
                raise ServiceCustodyError("managed_witness_missing")
            self.managed_witness = {**witness, "durable_proof": proof}

    def prepare_next_managed_child(self) -> bool:
        from gateway.runtime_service_managed_children import prepare_next_managed_child
        return prepare_next_managed_child(self)

    def reset_managed_child_after_retry(self) -> bool:
        from gateway.runtime_service_managed_children import reset_managed_child_after_retry
        return reset_managed_child_after_retry(self)

    def mark_delivery_complete(self, request_id: str) -> None:
        if (request_id not in self.durable_proofs
                or self.state in {"unknown", "closed", "released"}):
            self.mark_unknown("delivery_before_durable_result")
            raise ServiceCustodyError("delivery_before_durable_result")
        self.delivery_requests.add(request_id)
        self.delivery_complete = True

    def clear_request(self, request_id: str) -> None:
        self.durable_proofs.pop(request_id, None)
        self.delivery_requests.discard(request_id)
        self.completed_requests.add(request_id)
        if not self.durable_proofs:
            self.durable_proof = None
        self.delivery_complete = bool(self.delivery_requests)

    def reopen_external_catalog_retry(self) -> bool:
        """Reset only this completed child receipt for a same-owner discovery retry."""
        if self.state != "unknown" or self.unknown_reason not in {
            "catalog_discovery_failed", "cache_readback_unknown",
        }:
            return False
        # A cache-readback failure may already have a durable/publication record.  That
        # record is an immutable terminal candidate, not permission to erase state and
        # spawn another child; only an owner with no committed proof may be reopened.
        if self.external_publication_proof is not None or self.durable_proofs or self.delivery_requests:
            return False
        if (
            self.process is None or self.process.poll() is None
            or self.reader_error is not None
            or self.eof_streams != required_reader_streams(self)
            or self.joined_streams != required_reader_streams(self)
            or set(self.readers) != required_reader_streams(self)
        ):
            return False
        if self.transaction is not None:
            if getattr(self.transaction, "process", None) is not self.process:
                return False
            if not getattr(self.transaction, "native_empty_observed", False):
                if self.transaction.observe_empty() is not True:
                    return False
            if self.transaction.close_after_observed_empty() is not True:
                return False
        restored = self.service.coordinator.reconcile_unknown(
            self.handle, owner_id=self.handle.owner_id, profile_id=self.handle.profile_id,
            process_instance=self.handle.process_instance, authority_epoch=self.handle.authority_epoch,
        )
        if restored is not None:
            return False
        self.state = "reserved"
        self.transaction = None
        self.process = None
        self.pid = None
        self.process_started_at = None
        self.scope = None
        self.scope_identity = None
        self.process_group_id = None
        self.stdout_eof = self.stderr_eof = False
        self.stdout_joined = self.stderr_joined = False
        self.reader_error = self.unknown_reason = None
        self.readers.clear()
        self.reader_start_states.clear()
        self.eof_streams.clear()
        self.joined_streams.clear()
        self.external_publication_proof = None
        self.completed_requests.clear()
        self.service.unknown = False
        return True

    def _delivery_finalize_unknown(self, reason: str = "delivery_finalize_failed") -> bool:
        self.state = "unknown"
        self.unknown_reason = reason
        self.service.unknown = True
        # Local state alone cannot support a later same-handle reconciliation.  Move the
        # coordinator record to unknown at the same boundary, even when the caller will
        # subsequently retain the request/I04 tuple with its typed delivery reason.
        self.service.coordinator.mark_unknown(self.handle, reason)
        return False

    def prepare_delivery_close(self, request_id: str) -> bool:
        """Close native process custody while retaining proof/request maps for commit or retry."""
        if (
            type(request_id) is not str
            or request_id not in self.durable_proofs
            or request_id not in self.delivery_requests
            or self.state in {"closed", "released"}
        ):
            return self._delivery_finalize_unknown()
        if self.native_close_committed:
            self.state = "finalizing"
            return True
        if (
            self.reader_error is not None
            or self.eof_streams != required_reader_streams(self)
            or self.joined_streams != required_reader_streams(self)
            or set(self.readers) != required_reader_streams(self)
            or self.process is None
        ):
            return self._delivery_finalize_unknown("physical_receipt_incomplete")
        if (
            self.ephemeral_process
            and self.service.binding is not None
            and not isinstance(self.managed_witness, dict)
        ):
            return self._delivery_finalize_unknown("managed_witness_missing")
        if self.transaction is not None:
            process = self.process
            if getattr(self.transaction, "process", None) is not process or process.poll() is None:
                return self._delivery_finalize_unknown("physical_process_live")
            if not getattr(self.transaction, "native_empty_observed", False):
                if self.transaction.observe_empty() is not True:
                    return self._delivery_finalize_unknown("native_job_state_unknown")
            if self.transaction.close_after_observed_empty() is not True:
                return self._delivery_finalize_unknown()
        elif self.scope == "posix_process_group":
            if not self._posix_process_group_empty():
                return self._delivery_finalize_unknown("posix_process_group_live")
        elif self.scope != "posix_direct" or self.process.poll() is None:
            return self._delivery_finalize_unknown("physical_process_state_unknown")
        self.native_close_committed = True
        self.state = "finalizing"
        return True

    def commit_delivery_close(self) -> bool:
        """Commit the physical coordinator handle after I04 request terminalization."""
        if not self.native_close_committed or self.state not in {"finalizing", "unknown"}:
            return False
        if self.state == "unknown":
            restored = self.service.coordinator.reconcile_unknown(
                self.handle, owner_id=self.handle.owner_id, profile_id=self.handle.profile_id,
                process_instance=self.handle.process_instance, authority_epoch=self.handle.authority_epoch,
            )
            if restored is not None:
                return False
            self.state = "finalizing"
            self.unknown_reason = None
            self.service.unknown = False
        result = self.service.coordinator.mark_terminal(self.handle)
        if result is not None:
            # Preserve the same physical handle as retryable unknown custody even when the
            # coordinator refuses for a reason other than a release failure.  Reconciliation
            # must never attempt to reopen an accepted record through a local-only marker.
            self.service.coordinator.mark_unknown(self.handle, "physical_release_failed")
            return self._delivery_finalize_unknown()
        managed_client = self.managed_client
        self.state = "closed"
        self.managed_witness = None
        self.managed_client = None
        self.service.physical = None
        if managed_client is not None:
            refresh = getattr(managed_client, "_refresh_closed_state", None)
            if callable(refresh):
                refresh()
        return True

    def close_after_delivery(self) -> bool:
        from gateway.runtime_service_native_close import can_retry_managed_native_close
        managed_native_retry = can_retry_managed_native_close(self)
        external_unknown = self.unknown_reason in (
            {"catalog_discovery_failed", "cache_readback_unknown"}
            | _EXTERNAL_CLOSE_RETRY_REASONS
        ) and not managed_native_retry
        if external_unknown:
            proof = self.external_publication_proof
            if (
                not isinstance(getattr(proof, "operation_id", None), str)
                or not isinstance(getattr(proof, "profile_id", None), str)
                or not isinstance(getattr(proof, "process_instance", None), str)
                or type(getattr(proof, "authority_epoch", None)) is not int
                or not isinstance(getattr(proof, "source_identity", None), str)
                or not (getattr(proof, "durable_result", False)
                        or getattr(proof, "cache_readback", False))
                or self.owner_id not in {
                    f"{proof.operation_id}:physical", f"external:{proof.operation_id}:physical",
                }
                or proof.profile_id != self.handle.profile_id
                or proof.process_instance != self.handle.process_instance
                or proof.authority_epoch != self.handle.authority_epoch
            ):
                return False
        if (
            self.durable_proofs
            or self.delivery_requests
            or self.reader_error is not None
            or self.eof_streams != required_reader_streams(self)
            or self.joined_streams != required_reader_streams(self)
            or set(self.readers) != required_reader_streams(self)
            or not self.completed_requests
            or self.process is None
        ):
            self.mark_unknown("physical_receipt_incomplete")
            return False
        if (
            self.ephemeral_process
            and self.service.binding is not None
            and not isinstance(self.managed_witness, dict)
        ):
            self.mark_unknown("managed_witness_missing")
            return False
        if self.transaction is not None:
            process = self.process
            if getattr(self.transaction, "process", None) is not process or process.poll() is None:
                self.mark_unknown("physical_process_live")
                return False
            if not getattr(self.transaction, "native_empty_observed", False):
                if self.transaction.observe_empty() is not True:
                    self.mark_unknown("native_job_state_unknown")
                    return False
            if self.transaction.close_after_observed_empty() is not True:
                self.state = "unknown"
                self.unknown_reason = "native_close_failed"
                self.service.unknown = True
                self.service.coordinator.mark_unknown(self.handle, "native_close_failed")
                return False
            if self.state == "unknown":
                # A native close can fail after the exact empty proof while the retained
                # handle and generation remain valid. Re-open only this same handle after
                # the close retry succeeds; identity/reader uncertainty stays sticky.
                if self.unknown_reason not in _EXTERNAL_CLOSE_RETRY_REASONS and not managed_native_retry:
                    return False
                reconciled = self.service.coordinator.reconcile_unknown(
                    self.handle, owner_id=self.handle.owner_id, profile_id=self.handle.profile_id,
                    process_instance=self.handle.process_instance, authority_epoch=self.handle.authority_epoch,
                )
                if reconciled is not None:
                    return False
                self.state = "process_bound"
                self.service.unknown = False
        elif not self._posix_delivery_ready():
            self.mark_unknown("physical_process_state_unknown")
            return False
        if self.state == "unknown":
            if self.unknown_reason not in (
                {"catalog_discovery_failed", "cache_readback_unknown"}
                | _EXTERNAL_CLOSE_RETRY_REASONS
            ) and not managed_native_retry:
                return False
            reconciled = self.service.coordinator.reconcile_unknown(
                self.handle, owner_id=self.handle.owner_id, profile_id=self.handle.profile_id,
                process_instance=self.handle.process_instance, authority_epoch=self.handle.authority_epoch,
            )
            if reconciled is not None:
                return False
            self.state = "process_bound"
            self.service.unknown = False
        result = self.service.coordinator.mark_terminal(self.handle)
        if result is not None:
            self.mark_unknown(getattr(result, "code", "physical_release_failed"))
            return False
        managed_client = self.managed_client
        self.state = "closed"
        self.managed_witness = None
        self.managed_client = None
        self.service.physical = None
        if managed_client is not None:
            refresh = getattr(managed_client, "_refresh_closed_state", None)
            if callable(refresh):
                refresh()
        return True


