"""Typed admission bindings for long-lived Codex sidecar services.

The runtime coordinator remains the sole custody ledger.  This module only carries the
authenticated admission identity from the FIFO executor to a reused service and its exact
request children; it never infers ownership from a session id, a PID, or an empty map.
"""

from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass, field
import sqlite3
from typing import Any

from gateway.runtime_custody import CustodyHandle, CustodyRefusal, RuntimeCustodyCoordinator
from gateway.runtime_sidecar_transport import required_reader_streams


class ServiceCustodyError(RuntimeError):
    """A typed refusal before a sidecar can create work without an authenticated parent."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)
_MANAGED_PROCESS_RETRY_REASONS = frozenset({
    "reader_join_failed", "reader_setup_failed", "acp_session_failed",
    "native_cleanup_interrupted", "reader_cleanup_interrupted",
    "spawn_not_started", "spawn_bind_failed", "pipe_missing",
    "partial_launch", "launch_identity_mismatch",
    "launch_transaction_missing",
    "process_identity_rejected", "physical_launch_identity_mismatch",
    "physical_launch_transaction_missing", "reader_identity_missing",
    "reader_identity_replaced", "reader_state_invalid", "reader_failed",
    "reader_still_alive", "reader_start_unknown", "posix_process_group_live",
})
_EXTERNAL_CLOSE_RETRY_REASONS = frozenset({
    "join_failed", "native_close_failed", "physical_close_failed",
    "physical_release_failed", "lease_release_failed",
})


@dataclass(frozen=True)
class TurnCustodyBinding:
    """The exact active FIFO claim a sidecar request is allowed to extend."""

    authority: Any
    coordinator: RuntimeCustodyCoordinator
    parent_handle: CustodyHandle
    admission_id: str
    execution_generation: int
    session_id: str
    profile_id: str
    authority_epoch: int
    process_instance: str
    # Empty is retained only for the old low-level synthetic fixture; every
    # authenticated admission binding is populated from the authoritative row.
    principal_id: str = ""
    # Canonical source identity is carried separately from the messaging
    # principal.  The current admission schema stores the same authenticated
    # value in principal_id; keeping both fields makes a source-only tamper
    # observable before a request or process effect.
    source_identity: str = ""


_CURRENT_BINDING: ContextVar[TurnCustodyBinding | None] = ContextVar(
    "codex_turn_custody_binding", default=None,
)


def current_turn_binding() -> TurnCustodyBinding | None:
    """Return the current executor binding; deferred work must receive it explicitly."""
    return _CURRENT_BINDING.get()


def bind_turn(binding: TurnCustodyBinding):
    return _CURRENT_BINDING.set(binding)


def reset_turn(token) -> None:
    _CURRENT_BINDING.reset(token)


def _string(value: Any, field: str) -> str:
    if type(value) is not str or not value:
        raise ServiceCustodyError(f"invalid_{field}")
    return value


def _require_binding_shape(binding: TurnCustodyBinding, *, allow_unknown_parent: bool = False) -> None:
    if not isinstance(binding, TurnCustodyBinding):
        raise ServiceCustodyError("preservation_owner_unmapped")
    parent = binding.parent_handle
    if (
        not isinstance(binding.coordinator, RuntimeCustodyCoordinator)
        or not isinstance(parent, CustodyHandle)
        or type(binding.admission_id) is not str
        or not binding.admission_id
        or type(binding.execution_generation) is not int
        or binding.execution_generation < 1
        or type(binding.session_id) is not str
        or not binding.session_id
        or type(binding.profile_id) is not str
        or not binding.profile_id
        or type(binding.authority_epoch) is not int
        or binding.authority_epoch < 1
        or type(binding.process_instance) is not str
        or not binding.process_instance
        or type(binding.principal_id) is not str
        or type(binding.source_identity) is not str
        or parent.coordinator_id != getattr(binding.coordinator, "_coordinator_id", None)
        or parent.owner_class != "I04_fifo_turn_execution"
        or parent.profile_id != binding.profile_id
        or parent.authority_epoch != binding.authority_epoch
        or parent.process_instance != binding.process_instance
    ):
        raise ServiceCustodyError("preservation_owner_mismatch")
    if binding.coordinator._parent(parent) is not None:
        return
    if not allow_unknown_parent:
        raise ServiceCustodyError("preservation_owner_mismatch")
    lock = getattr(binding.coordinator, "_lock", None)
    records = getattr(binding.coordinator, "_records", None)
    lease_id = getattr(getattr(parent, "lease", None), "lease_id", None)
    if lock is None or not isinstance(records, dict) or not isinstance(lease_id, str):
        raise ServiceCustodyError("preservation_owner_mismatch")
    with lock:
        record = records.get(lease_id)
        if (
            record is None
            or getattr(record, "handle", None) is not parent
            or getattr(record, "state", None) not in {"accepted", "finalizing", "unknown"}
        ):
            raise ServiceCustodyError("preservation_owner_mismatch")


def _authoritative_admission_row(authority: Any, admission_id: str) -> dict[str, Any]:
    db = getattr(authority, "db", None)
    if db is None:
        raise ServiceCustodyError("preservation_owner_unknown")
    from hermes_state_runtime import get_session_admission
    try:
        row = get_session_admission(db, admission_id=admission_id)
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as exc:
        raise ServiceCustodyError("preservation_owner_unknown") from exc
    if not isinstance(row, dict):
        raise ServiceCustodyError("preservation_owner_unknown")
    return row


def _require_authoritative_row(
    binding: TurnCustodyBinding, row: dict[str, Any], *, status: str,
) -> None:
    if (
        row.get("admission_id") != binding.admission_id
        or row.get("target_session_id") != binding.session_id
        or row.get("status") != status
        or type(row.get("owner_epoch")) is not int
        or row.get("owner_epoch") != binding.authority_epoch
        or type(row.get("generation")) is not int
        or row.get("generation") != binding.execution_generation
        or type(row.get("principal_id")) is not str
        or not row.get("principal_id")
        or (binding.principal_id and row.get("principal_id") != binding.principal_id)
        or (binding.source_identity and row.get("principal_id") != binding.source_identity)
    ):
        raise ServiceCustodyError("preservation_owner_mismatch")


def _require_authority_parent_identity(binding: TurnCustodyBinding) -> None:
    authority = binding.authority
    parent = binding.parent_handle
    if authority is None:
        raise ServiceCustodyError("preservation_owner_unmapped")
    if type(getattr(authority, "profile_id", None)) is not str or authority.profile_id != binding.profile_id:
        raise ServiceCustodyError("preservation_owner_mismatch")
    if type(getattr(authority, "epoch", None)) is not int or authority.epoch != binding.authority_epoch:
        raise ServiceCustodyError("preservation_owner_mismatch")
    coordinator_getter = getattr(authority, "_preservation_coordinator", None)
    if not callable(coordinator_getter) or coordinator_getter() is not binding.coordinator:
        raise ServiceCustodyError("preservation_owner_mismatch")
    children = getattr(authority, "_preservation_children", None)
    if not isinstance(children, dict) or children.get(binding.admission_id) is not parent:
        raise ServiceCustodyError("preservation_owner_mismatch")
    if type(getattr(authority, "instance_id", None)) is not str:
        raise ServiceCustodyError("preservation_owner_mismatch")
    if authority.instance_id != binding.process_instance:
        raise ServiceCustodyError("preservation_owner_mismatch")
    sessions = getattr(authority, "sessions", None)
    if not isinstance(sessions, dict) or binding.session_id not in sessions:
        raise ServiceCustodyError("preservation_owner_mismatch")


def _require_live_execution(binding: TurnCustodyBinding) -> None:
    _require_authority_parent_identity(binding)
    authority = binding.authority
    sessions = getattr(authority, "sessions", None)
    if not isinstance(sessions, dict):
        raise ServiceCustodyError("preservation_owner_unmapped")
    live = sessions.get(binding.session_id)
    event_stream = getattr(live, "event_stream", None) if live is not None else None
    lock = getattr(event_stream, "lock", None)
    if live is None or event_stream is None or lock is None:
        raise ServiceCustodyError("preservation_owner_mismatch")
    with lock:
        execution = getattr(event_stream, "execution", None)
        executing = getattr(authority, "executing", None)
        if (
            type(execution) is not dict
            or execution.get("authority_epoch") != binding.authority_epoch
            or execution.get("admission_id") != binding.admission_id
            or type(execution.get("execution_generation")) is not int
            or execution.get("execution_generation") != binding.execution_generation
            or not isinstance(executing, dict)
            or type(executing.get(binding.session_id)) is not int
            or executing.get(binding.session_id) != binding.execution_generation
        ):
            raise ServiceCustodyError("preservation_owner_mismatch")


def _require_binding(binding: TurnCustodyBinding, row: dict[str, Any] | None = None) -> None:
    _require_binding_shape(binding)
    authoritative = row if row is not None else _authoritative_admission_row(
        binding.authority, binding.admission_id,
    )
    _require_authoritative_row(binding, authoritative, status="started")
    _require_live_execution(binding)


def _require_terminal_binding(
    binding: TurnCustodyBinding, *, allow_unknown_parent: bool = False,
) -> dict[str, Any]:
    """Validate immutable ownership after settlement cleared process-local execution state."""
    _require_binding_shape(binding, allow_unknown_parent=allow_unknown_parent)
    _require_authority_parent_identity(binding)
    row = _authoritative_admission_row(binding.authority, binding.admission_id)
    _require_authoritative_row(binding, row, status="terminal")
    if row.get("outcome") not in {"completed", "interrupted", "rejected", "failed"}:
        raise ServiceCustodyError("preservation_result_unknown")
    return row


def admission_binding(authority: Any, ref: Any, row: dict[str, Any]) -> TurnCustodyBinding:
    """Bind a claimed row to its live I04 lease before the handler can spawn a sidecar."""
    if not isinstance(row, dict):
        raise ServiceCustodyError("admission_row_missing")
    admission_id = _string(row.get("admission_id"), "admission_id")
    session_id = _string(getattr(ref, "session_id", None), "session_id")
    ref_profile_id = _string(getattr(ref, "profile_id", None), "profile_id")
    generation = row.get("generation")
    if type(generation) is not int or generation < 1:
        raise ServiceCustodyError("admission_generation_missing")
    if row.get("target_session_id") != session_id:
        raise ServiceCustodyError("preservation_owner_mismatch")
    profile_id = _string(getattr(authority, "profile_id", None), "profile_id")
    if ref_profile_id != profile_id:
        raise ServiceCustodyError("preservation_owner_mismatch")
    epoch = getattr(authority, "epoch", None)
    if type(epoch) is not int or epoch < 1:
        raise ServiceCustodyError("authority_epoch_missing")
    coordinator_getter = getattr(authority, "_preservation_coordinator", None)
    coordinator = coordinator_getter() if callable(coordinator_getter) else None
    children = getattr(authority, "_preservation_children", None)
    parent = children.get(admission_id) if isinstance(children, dict) else None
    if not isinstance(coordinator, RuntimeCustodyCoordinator) or not isinstance(parent, CustodyHandle):
        raise ServiceCustodyError("preservation_owner_unmapped")
    if (
        parent.owner_class != "I04_fifo_turn_execution"
        or parent.owner_id != f"claim:{admission_id}"
        or parent.profile_id != profile_id
        or parent.authority_epoch != epoch
        or parent.process_instance != coordinator.process_instance
        or coordinator._parent(parent) is None
    ):
        raise ServiceCustodyError("preservation_owner_mismatch")
    authoritative = _authoritative_admission_row(authority, admission_id)
    if any(row.get(field) != authoritative.get(field)
           for field in ("admission_id", "target_session_id", "generation", "owner_epoch", "principal_id")):
        raise ServiceCustodyError("preservation_owner_mismatch")
    principal_id = _string(authoritative.get("principal_id"), "principal_id")
    binding = TurnCustodyBinding(
        authority=authority,
        coordinator=coordinator,
        parent_handle=parent,
        admission_id=admission_id,
        execution_generation=generation,
        session_id=session_id,
        profile_id=profile_id,
        authority_epoch=epoch,
        process_instance=parent.process_instance,
        principal_id=principal_id,
        source_identity=principal_id,
    )
    _require_binding(binding, authoritative)
    return binding


def validate_turn_binding(binding: TurnCustodyBinding) -> None:
    """Validate the live authority and exact I04 handle before a service effect."""
    _require_binding(binding)


@dataclass(frozen=True)
class DurableResultProof:
    """Typed witness produced by the same SessionDB transaction as a result write."""

    store_path: str
    session_id: str
    admission_id: str
    execution_generation: int
    row_ids: tuple[int, ...]
    row_snapshots: tuple[str, ...]
    row_sessions: tuple[str, ...]
    row_roles: tuple[str, ...]
    payload_digest: str
    result_row_ids: tuple[int, ...] = ()


def _canonical_store_path(authority: Any) -> str:
    from gateway.session_turn_physical_tip import canonical_store_path
    path = canonical_store_path(authority)
    if path is None:
        raise ServiceCustodyError("durable_store_identity_missing")
    return path


from gateway.runtime_service_physical import ServiceProcessReceipt


@dataclass
class ServiceLease:
    """One persistent service root plus exact request and physical child leases."""

    coordinator: RuntimeCustodyCoordinator
    handle: CustodyHandle
    binding: TurnCustodyBinding | None
    owner_id: str
    requests: dict[str, CustodyHandle]
    # The source class is carried on the same lease object; it is not a second ownership map.
    # Older low-level fixtures may omit it, but a bootstrapped runtime registry refuses to seal
    # an unclassified service.
    producer_key: str | None = None
    unknown: bool = False
    physical: ServiceProcessReceipt | None = None
    durable_requests: set[str] = field(default_factory=set)
    # RuntimeCustodyCoordinator is intentionally unchanged in this bounded proposal; this
    # exact lease-id map carries the typed delivery reason alongside its unknown records.
    delivery_unknown_reasons: dict[str, str] = field(default_factory=dict, init=False, repr=False)
    # Admission ids whose I04 request/secondary parent terminalization committed before a
    # physical close retry.  This is a same-service transition marker, not a second ledger.
    delivery_terminalized: set[str] = field(default_factory=set, init=False, repr=False)
    external_parent: Any = field(default=None, init=False, repr=False)

    def validate_binding(self, binding: TurnCustodyBinding) -> None:
        _require_binding(binding)
        self._validate_service_identity(binding)

    def validate_spawn_binding(
        self, binding: TurnCustodyBinding, request: CustodyHandle | None = None,
    ) -> CustodyHandle:
        """Validate the live admission and request before reserving physical custody."""
        self.validate_binding(binding)
        if self.unknown or self.binding is not binding:
            raise ServiceCustodyError("preservation_owner_mismatch")
        current = self.requests.get(binding.admission_id)
        if current is None or (request is not None and current is not request):
            raise ServiceCustodyError("service_request_missing")
        if self.physical is not None and not (
            (self.physical.retry_ready or self.physical.managed_child_ready)
            and self.physical.state == "reserved"
            and self.physical.process is None
        ):
            raise ServiceCustodyError("physical_custody_pending")
        return current

    def prepare_next_managed_child(
        self, binding: TurnCustodyBinding, request: CustodyHandle,
    ) -> bool:
        from gateway.runtime_service_managed_children import prepare_service_next_managed_child
        return prepare_service_next_managed_child(self, binding, request)

    def _retry_record_is_exact(self, handle: CustodyHandle, binding: TurnCustodyBinding, owner_id: str) -> bool:
        lock = getattr(self.coordinator, "_lock", None)
        records = getattr(self.coordinator, "_records", None)
        lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
        if lock is None or not isinstance(records, dict) or not isinstance(lease_id, str):
            return False
        with lock:
            record = records.get(lease_id)
            return (
                record is not None
                and getattr(record, "handle", None) is handle
                and getattr(record, "state", None) == "unknown"
                and handle.coordinator_id == self.coordinator._coordinator_id
                and handle.owner_id == owner_id
                and handle.profile_id == binding.profile_id
                and handle.process_instance == binding.process_instance
                and handle.authority_epoch == binding.authority_epoch
            )

    @staticmethod
    def _readers_stopped(receipt: ServiceProcessReceipt) -> bool:
        if receipt.readers and set(receipt.readers) != required_reader_streams(receipt):
            return False
        for reader in receipt.readers.values():
            checker = getattr(reader, "is_alive", None)
            if not callable(checker):
                return False
            try:
                if checker():
                    return False
            except (OSError, RuntimeError, TypeError, ValueError):
                return False
        return True

    def _reset_process_for_retry(self, receipt: ServiceProcessReceipt) -> bool:
        setup_fence = receipt.setup_fence
        if setup_fence is not None and not setup_fence.is_set():
            return False
        receipt.state = "reserved"
        receipt.retry_ready = True
        receipt.transaction = None
        receipt.setup_fence = None
        receipt.construction_owner = None
        receipt.process = None
        receipt.pid = None
        receipt.process_started_at = None
        receipt.scope = None
        receipt.scope_identity = None
        receipt.process_group_id = None
        receipt.stdout_eof = receipt.stderr_eof = False
        receipt.stdout_joined = receipt.stderr_joined = False
        receipt.reader_error = receipt.unknown_reason = None
        receipt.readers.clear()
        receipt.reader_start_states.clear()
        receipt.eof_streams.clear()
        receipt.joined_streams.clear()
        receipt.durable_proofs.clear()
        receipt.delivery_requests.clear()
        receipt.completed_requests.clear()
        receipt.durable_proof = None
        receipt.delivery_complete = False
        receipt.native_close_committed = False
        self.unknown = False
        owner = self.external_parent
        if owner is not None and owner.state == "unknown":
            owner.state = "request_reserved"
        return True

    def _codex_constructor_receipt_is_exact(
        self, binding: TurnCustodyBinding, request: CustodyHandle, owner: Any,
    ) -> ServiceProcessReceipt | None:
        receipt = self.physical
        if (
            not self.unknown
            or self.binding is not binding
            or self.requests.get(binding.admission_id) is not request
            or receipt is None
            or receipt.service is not self
            or receipt.construction_owner is not owner
            or receipt.state != "unknown"
            or receipt.unknown_reason not in _MANAGED_PROCESS_RETRY_REASONS
            or receipt.ephemeral_process
            or receipt.durable_proofs
            or receipt.delivery_requests
            or receipt.completed_requests
            or receipt.external_publication_proof is not None
            or self.durable_requests
            or self.delivery_unknown_reasons
            or self.delivery_terminalized
            or receipt.process is None
            or (
                receipt.transaction is not None
                and getattr(receipt.transaction, "process", None) is not receipt.process
            )
            or not self._retry_record_is_exact(binding.parent_handle, binding, binding.parent_handle.owner_id)
            or not self._retry_record_is_exact(request, binding, request.owner_id)
            or not self._retry_record_is_exact(receipt.handle, binding, receipt.owner_id)
        ):
            return None
        return receipt

    def _codex_external_constructor_receipt_is_exact(
        self, parent: Any, owner: Any,
    ) -> ServiceProcessReceipt | None:
        """Authenticate one producer-policy parent before touching its child."""
        receipt = self.physical
        if (
            receipt is None
            or getattr(parent, "_service", None) is not self
            or getattr(parent, "_physical", None) is not receipt
            or getattr(parent, "root_handle", None) is not self.handle
            or getattr(parent, "coordinator", None) is not self.coordinator
            or getattr(parent, "profile_id", None) != self.handle.profile_id
            or getattr(parent, "process_instance", None) != self.handle.process_instance
            or getattr(parent, "authority_epoch", None) != self.handle.authority_epoch
            or getattr(parent, "state", None) in {"terminal", "released"}
            or receipt.service is not self
            or receipt.construction_owner is not owner
            or receipt.state != "unknown"
            or receipt.unknown_reason not in _MANAGED_PROCESS_RETRY_REASONS
            or receipt.ephemeral_process
            or receipt.external_publication_proof is not None
            or receipt.durable_proofs
            or receipt.delivery_requests
            or receipt.completed_requests
            or self.durable_requests
            or self.delivery_unknown_reasons
            or self.delivery_terminalized
            or receipt.process is None
            or (
                receipt.transaction is not None
                and getattr(receipt.transaction, "process", None) is not receipt.process
            )
        ):
            return None
        records = getattr(self.coordinator, "_records", None)
        lock = getattr(self.coordinator, "_lock", None)
        lease_id = getattr(getattr(receipt.handle, "lease", None), "lease_id", None)
        root_lease_id = getattr(getattr(self.handle, "lease", None), "lease_id", None)
        if lock is None or not isinstance(records, dict) or not isinstance(lease_id, str) or not isinstance(root_lease_id, str):
            return None
        with lock:
            physical_record = records.get(lease_id)
            root_record = records.get(root_lease_id)
            if (
                physical_record is None
                or getattr(physical_record, "handle", None) is not receipt.handle
                or getattr(physical_record, "state", None) != "unknown"
                or root_record is None
                or getattr(root_record, "handle", None) is not self.handle
                or getattr(root_record, "state", None) not in {"accepted", "unknown"}
            ):
                return None
        return receipt

    def prepare_codex_external_constructor_failure(self, parent: Any, owner: Any) -> bool:
        """Authenticate and stop a producer-policy child before reader settlement."""
        receipt = self._codex_external_constructor_receipt_is_exact(parent, owner)
        return receipt is not None and receipt.stop_process_for_retry()

    def finish_codex_external_constructor_failure(self, parent: Any, owner: Any) -> bool:
        """Close and reopen only the exact producer-policy child for a same-owner retry."""
        receipt = self._codex_external_constructor_receipt_is_exact(parent, owner)
        if receipt is None or receipt.reader_error is not None:
            return False
        if (
            set(receipt.readers) != required_reader_streams(receipt)
            or receipt.reader_start_states != dict.fromkeys(required_reader_streams(receipt), "started")
            or receipt.joined_streams != required_reader_streams(receipt)
        ):
            return False
        if not receipt.close_native_after_process():
            return False
        if self._codex_external_constructor_receipt_is_exact(parent, owner) is None:
            return False
        restored = self.coordinator.reconcile_unknown(
            receipt.handle, owner_id=receipt.handle.owner_id,
            profile_id=receipt.handle.profile_id,
            process_instance=receipt.handle.process_instance,
            authority_epoch=receipt.handle.authority_epoch,
        )
        if restored is not None or not self._reset_process_for_retry(receipt):
            return False
        parent._physical = receipt
        parent.state = "request_reserved"
        return True

    def prepare_codex_constructor_failure(
        self, binding: TurnCustodyBinding, request: CustodyHandle, owner: Any,
    ) -> bool:
        """Authenticate the retained owner and stop its native process before reader joins."""
        receipt = self._codex_constructor_receipt_is_exact(binding, request, owner)
        if receipt is None:
            return False
        return receipt.stop_process_for_retry()

    def finish_codex_constructor_failure(
        self, binding: TurnCustodyBinding, request: CustodyHandle, owner: Any,
    ) -> bool:
        """Close the stopped native receipt and reopen only the same live admission."""
        receipt = self._codex_constructor_receipt_is_exact(binding, request, owner)
        if receipt is None:
            return False
        if (
            receipt.reader_error is not None
            or set(receipt.readers) != required_reader_streams(receipt)
            or receipt.joined_streams != required_reader_streams(receipt)
        ):
            return False
        if not receipt.close_native_after_process():
            return False
        try:
            _require_binding_shape(binding, allow_unknown_parent=True)
            row = _authoritative_admission_row(binding.authority, binding.admission_id)
            _require_authoritative_row(binding, row, status="started")
            _require_live_execution(binding)
        except ServiceCustodyError:
            # The authenticated old owner is already stopped and remains retained.  A
            # stale admission may refuse retry, but it must never authorize a successor.
            return False
        restored_handles: list[CustodyHandle] = []
        # The persistent Codex service root remains the accepted owner of this
        # session.  Constructor failure makes only the request and physical
        # child uncertain; reopening the root would make the next admission
        # fail its authoritative service identity check.
        for handle in (receipt.handle, request, binding.parent_handle):
            restored = self.coordinator.reconcile_unknown(
                handle, owner_id=handle.owner_id, profile_id=binding.profile_id,
                process_instance=binding.process_instance, authority_epoch=binding.authority_epoch,
            )
            if restored is not None:
                for restored_handle in restored_handles:
                    self.coordinator.mark_unknown(restored_handle, "codex_constructor_reconcile_failed")
                return False
            restored_handles.append(handle)
        return self._reset_process_for_retry(receipt)

    def reconcile_managed_child_failure(
        self, binding: TurnCustodyBinding, request: CustodyHandle,
    ) -> bool:
        from gateway.runtime_service_managed_children import reconcile_service_managed_child_failure
        return reconcile_service_managed_child_failure(self, binding, request)

    def reconcile_process_failure(
        self, binding: TurnCustodyBinding, request: CustodyHandle,
    ) -> bool:
        """Reopen one failed managed child receipt for an exact same-handle retry."""
        _require_binding_shape(binding, allow_unknown_parent=True)
        row = _authoritative_admission_row(binding.authority, binding.admission_id)
        _require_authoritative_row(binding, row, status="started")
        _require_live_execution(binding)
        receipt = self.physical
        if (
            not self.unknown
            or self.binding is not binding
            or self.requests.get(binding.admission_id) is not request
            or receipt is None
            or receipt.service is not self
            or receipt.state != "unknown"
            or receipt.unknown_reason not in _MANAGED_PROCESS_RETRY_REASONS
            or not receipt.ephemeral_process
            or receipt.durable_proofs
            or receipt.delivery_requests
            or receipt.completed_requests
            or receipt.external_publication_proof is not None
            or self.durable_requests
            or self.delivery_unknown_reasons
            or self.delivery_terminalized
            or (
                receipt.transaction is not None
                and getattr(receipt.transaction, "process", None) is not receipt.process
            )
            or not self._retry_record_is_exact(self.handle, binding, self.owner_id)
            or not self._retry_record_is_exact(receipt.handle, binding, receipt.owner_id)
        ):
            return False
        if not receipt.prepare_process_retry() or not self._readers_stopped(receipt):
            return False
        restored_handles: list[CustodyHandle] = []
        for handle in (receipt.handle, self.handle):
            restored = self.coordinator.reconcile_unknown(
                handle, owner_id=handle.owner_id, profile_id=binding.profile_id,
                process_instance=binding.process_instance, authority_epoch=binding.authority_epoch,
            )
            if restored is not None:
                for restored_handle in restored_handles:
                    self.coordinator.mark_unknown(restored_handle, "managed_retry_reconcile_failed")
                return False
            restored_handles.append(handle)
        if not self._reset_process_for_retry(receipt):
            return False
        return True

    def _validate_service_identity(self, binding: TurnCustodyBinding) -> None:
        active_record = None
        lock = getattr(self.coordinator, "_lock", None)
        checker = getattr(self.coordinator, "_active_handle_locked", None)
        if lock is not None and callable(checker):
            with lock:
                active_record = checker(
                    self.handle, "I16_turn_sidecars", binding.profile_id, binding.authority_epoch,
                )
        if (
            binding.authority is not self.binding.authority
            or binding.coordinator is not self.coordinator
            or binding.profile_id != self.binding.profile_id
            or binding.authority_epoch != self.binding.authority_epoch
            or binding.process_instance != self.binding.process_instance
            or binding.session_id != self.binding.session_id
            or (self.binding.principal_id and binding.principal_id != self.binding.principal_id)
            or (self.binding.source_identity and binding.source_identity != self.binding.source_identity)
            or active_record is None
            or getattr(active_record, "handle", None) is not self.handle
            or getattr(active_record, "state", None) not in {"accepted", "finalizing"}
            or self.handle.owner_class != "I16_turn_sidecars"
            or self.handle.profile_id != binding.profile_id
            or self.handle.authority_epoch != binding.authority_epoch
            or self.handle.process_instance != binding.process_instance
        ):
            raise ServiceCustodyError("preservation_owner_mismatch")

    def validate_terminal_delivery(self, binding: TurnCustodyBinding, request: CustodyHandle) -> str:
        """Validate exact durable delivery ownership after the admission is terminal."""
        lease_id = self._request_for_admission(binding.admission_id, request)
        physical = self.physical
        retryable_unknown = (
            physical is not None
            and physical.service is self
            and physical.unknown_reason in {"delivery_failed", "delivery_finalize_failed"}
        )
        _require_terminal_binding(binding, allow_unknown_parent=retryable_unknown)
        self._validate_service_identity(binding)
        if (
            physical is None
            or physical.service is not self
            or (
                not retryable_unknown
                and self.coordinator._parent(physical.handle) is not physical.handle.lease
            )
            or (
                retryable_unknown
                and not self._exact_physical_record_is_retained(physical.handle)
            )
            or getattr(getattr(request, "lease", None), "parent_lease_id", None)
            != getattr(getattr(self.handle, "lease", None), "lease_id", None)
            or getattr(self.coordinator, "_secondary_parents_by_child", {}).get(
                getattr(getattr(request, "lease", None), "lease_id", None),
            ) != {getattr(getattr(binding.parent_handle, "lease", None), "lease_id", None)}
            or getattr(getattr(physical.handle, "lease", None), "parent_lease_id", None)
            != getattr(getattr(self.handle, "lease", None), "lease_id", None)
            or physical.process is None
            or physical.state in {"reserved", "released", "closed"}
            or (
                physical.transaction is not None
                and getattr(physical.transaction, "process", None) is not physical.process
            )
            or (
                physical.ephemeral_process
                and not isinstance(physical.managed_witness, dict)
            )
        ):
            raise ServiceCustodyError("physical_receipt_identity_mismatch")
        proof = physical.durable_proofs.get(lease_id)
        from gateway.session_turn_physical_tip import physical_proof_session
        physical_session_id = physical_proof_session(binding, proof)
        if (
            proof is None
            or physical_session_id is None
            or proof.session_id != physical_session_id
            or proof.admission_id != binding.admission_id
            or proof.execution_generation != binding.execution_generation
            or proof.store_path != _canonical_store_path(binding.authority)
        ):
            raise ServiceCustodyError("durable_result_identity_mismatch")
        try:
            from gateway.session_results import admission_result
            result = admission_result(binding.authority.db, binding.admission_id)
        except (OSError, RuntimeError, TypeError, ValueError, KeyError, sqlite3.Error) as exc:
            raise ServiceCustodyError("preservation_result_unknown") from exc
        if not isinstance(result, dict) or not isinstance(result.get("result"), dict):
            raise ServiceCustodyError("preservation_result_unknown")
        stored_messages = result["result"].get("messages")
        if (
            not isinstance(stored_messages, list)
            or not stored_messages
            or any(not isinstance(message, dict) or type(message.get("_row_id")) is not int
                   or message.get("_row_id") <= 0 for message in stored_messages)
            or tuple(message["_row_id"] for message in stored_messages) != proof.result_row_ids
        ):
            raise ServiceCustodyError("durable_result_identity_mismatch")
        return lease_id

    def _exact_physical_record_is_retained(self, handle: CustodyHandle) -> bool:
        lock = getattr(self.coordinator, "_lock", None)
        records = getattr(self.coordinator, "_records", None)
        lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
        if lock is None or not isinstance(records, dict) or not isinstance(lease_id, str):
            return False
        with lock:
            record = records.get(lease_id)
            return (
                record is not None
                and getattr(record, "handle", None) is handle
                and getattr(record, "state", None) in {"accepted", "finalizing", "unknown"}
            )

    def begin_request(self, binding: TurnCustodyBinding) -> CustodyHandle:
        if self.unknown:
            raise ServiceCustodyError("custody_unknown")
        self.validate_binding(binding)
        request_id = binding.admission_id
        if request_id in self.requests:
            raise ServiceCustodyError("duplicate_service_request")
        if self.requests:
            raise ServiceCustodyError("service_request_in_flight")
        # A close may still drain an already accepted request/physical child, but it must not
        # create a new sidecar admission after the global owner entered closing.
        if self.coordinator.state in {"closing", "handoff_pending", "closed"}:
            raise ServiceCustodyError("runtime_draining")
        request = self.coordinator.reserve_root_with_secondary_parent(
            "I16_turn_sidecars", f"{self.owner_id}:request:{request_id}",
            profile_id=binding.profile_id, authority_epoch=binding.authority_epoch,
            parent=self.handle, secondary_parent=binding.parent_handle,
        )
        if isinstance(request, CustodyRefusal):
            raise ServiceCustodyError(request.code)
        self.binding = binding
        self.requests[request_id] = request
        registry = getattr(self.coordinator, "_i16_source_registry", None)
        if registry is not None:
            try:
                registry.register_request(self, request)
            except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
                self.coordinator.mark_unknown(request, "unmapped_writer")
                self.unknown = True
                raise ServiceCustodyError("i16_source_unregistered") from exc
        return request

    def begin_hook_request(self, parent: Any, invocation_id: str) -> CustodyHandle:
        """Reserve a non-turn hook child under an existing source-owned parent."""
        if self.unknown or parent is None:
            raise ServiceCustodyError("custody_unknown")
        if (
            getattr(parent, "coordinator", None) is not self.coordinator
            or getattr(parent, "parent_handle", None) is not self.handle
        ):
            raise ServiceCustodyError("preservation_owner_mismatch")
        if type(invocation_id) is not str or not invocation_id or self.requests:
            raise ServiceCustodyError("duplicate_hook_request")
        if self.coordinator.state in {"closing", "handoff_pending", "closed"}:
            raise ServiceCustodyError("runtime_draining")
        profile_id = getattr(parent, "profile_id", None)
        authority_epoch = getattr(parent, "authority_epoch", None)
        if type(profile_id) is not str or not profile_id or type(authority_epoch) is not int or authority_epoch < 1:
            raise ServiceCustodyError("preservation_owner_mismatch")
        request = self.coordinator.reserve_root(
            "I16_turn_sidecars", f"{self.owner_id}:hook:{invocation_id}",
            profile_id=profile_id, authority_epoch=authority_epoch, parent=self.handle,
        )
        if isinstance(request, CustodyRefusal):
            raise ServiceCustodyError(request.code)
        self.requests[invocation_id] = request
        self.external_parent = parent.owner
        registry = getattr(self.coordinator, "_i16_source_registry", None)
        if registry is not None:
            try:
                registry.register_request(self, request)
            except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
                self.coordinator.mark_unknown(request, "unmapped_writer")
                self.unknown = True
                raise ServiceCustodyError("i16_source_unregistered") from exc
        return request

    def begin_standalone_request(self, parent: Any, operation_id: str) -> CustodyHandle:
        """Reserve one request child for an authenticated standalone source parent."""
        if (
            self.unknown
            or self.binding is not None
            or getattr(parent, "mode", None) != "standalone_maintenance"
            or getattr(parent, "coordinator", None) is not self.coordinator
            or getattr(parent, "root_handle", None) is not self.handle
            or getattr(parent, "_service", None) is not self
            or (
                getattr(self, "external_parent", None) is not None
                and getattr(self, "external_parent", None) is not parent
            )
            or type(operation_id) is not str
            or not operation_id
            or self.requests
        ):
            raise ServiceCustodyError("standalone_request_identity_mismatch")
        if self.coordinator.state in {"closing", "handoff_pending", "closed"}:
            raise ServiceCustodyError("runtime_draining")
        request = self.coordinator.reserve_root(
            "I16_turn_sidecars", f"{self.owner_id}:request:{operation_id}",
            profile_id=self.handle.profile_id, authority_epoch=self.handle.authority_epoch,
            parent=self.handle,
        )
        if isinstance(request, CustodyRefusal):
            raise ServiceCustodyError(request.code)
        self.requests[operation_id] = request
        self.external_parent = parent
        registry = getattr(self.coordinator, "_i16_source_registry", None)
        if registry is not None:
            try:
                registry.register_request(self, request)
            except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
                self.coordinator.mark_unknown(request, "unmapped_writer")
                self.unknown = True
                raise ServiceCustodyError("i16_source_unregistered") from exc
        return request

    def reserve_standalone_physical(
        self, parent: Any, request: CustodyHandle,
    ) -> ServiceProcessReceipt:
        """Reserve the physical child below one exact standalone request."""
        if (
            self.unknown
            or self.binding is not None
            or getattr(parent, "mode", None) != "standalone_maintenance"
            or getattr(parent, "coordinator", None) is not self.coordinator
            or getattr(parent, "root_handle", None) is not self.handle
            or getattr(parent, "_service", None) is not self
            or getattr(self, "external_parent", None) is not parent
            or not isinstance(request, CustodyHandle)
            or self.requests.get(getattr(parent, "operation_id", None)) is not request
            or self.physical is not None
        ):
            raise ServiceCustodyError("standalone_physical_identity_mismatch")
        handle = self.coordinator.reserve_root(
            "I16_turn_sidecars", f"{self.owner_id}:physical",
            profile_id=self.handle.profile_id, authority_epoch=self.handle.authority_epoch,
            parent=request,
        )
        if isinstance(handle, CustodyRefusal):
            raise ServiceCustodyError(handle.code)
        receipt = ServiceProcessReceipt(self, handle, f"{self.owner_id}:physical")
        self.physical = receipt
        registry = getattr(self.coordinator, "_i16_source_registry", None)
        if registry is not None:
            try:
                registry.register_receipt(self, receipt)
            except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
                receipt.mark_unknown("unmapped_writer")
                raise ServiceCustodyError("i16_source_unregistered") from exc
        return receipt

    def reserve_hook_physical(self, parent: Any) -> ServiceProcessReceipt:
        """Reserve one physical child for a non-turn source-owned hook."""
        if (
            getattr(parent, "coordinator", None) is not self.coordinator
            or getattr(parent, "parent_handle", None) is not self.handle
        ):
            raise ServiceCustodyError("preservation_owner_mismatch")
        if self.unknown or self.physical is not None or not self.requests:
            if self.physical is not None:
                return self.physical
            raise ServiceCustodyError("physical_custody_pending")
        profile_id = getattr(parent, "profile_id", None)
        authority_epoch = getattr(parent, "authority_epoch", None)
        request = next(iter(self.requests.values()))
        handle = self.coordinator.reserve_root(
            "I16_turn_sidecars", f"{self.owner_id}:physical",
            profile_id=profile_id, authority_epoch=authority_epoch, parent=request,
        )
        if isinstance(handle, CustodyRefusal):
            raise ServiceCustodyError(handle.code)
        self.physical = ServiceProcessReceipt(self, handle, f"{self.owner_id}:physical")
        registry = getattr(self.coordinator, "_i16_source_registry", None)
        if registry is not None:
            try:
                registry.register_receipt(self, self.physical)
            except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
                self.physical.mark_unknown("unmapped_writer")
                raise ServiceCustodyError("i16_source_unregistered") from exc
        return self.physical

    def reconcile_hook_failure(self, parent: Any, request: CustodyHandle) -> bool:
        """Reconcile only the exact retained non-turn request and physical child."""
        key = getattr(parent, "invocation_id", None)
        if type(key) is not str or self.requests.get(key) is not request or self.physical is None:
            return False
        physical = self.physical
        if physical.state != "unknown":
            return False
        result = self.coordinator.reconcile_unknown(
            physical.handle, owner_id=physical.handle.owner_id,
            profile_id=getattr(parent, "profile_id", None),
            process_instance=getattr(parent, "process_instance", None),
            authority_epoch=getattr(parent, "authority_epoch", None),
        )
        if result is not None:
            return False
        physical.state = "reserved"
        physical.unknown_reason = None
        physical.retry_ready = True
        self.unknown = False
        return True

    def reserve_physical(self, binding: TurnCustodyBinding | None = None) -> ServiceProcessReceipt:
        binding = binding or self.binding
        self.validate_binding(binding)
        if self.unknown:
            raise ServiceCustodyError("custody_unknown")
        if self.physical is not None:
            return self.physical
        handle = self.coordinator.reserve_root(
            "I16_turn_sidecars", f"{self.owner_id}:physical",
            profile_id=binding.profile_id, authority_epoch=binding.authority_epoch, parent=self.handle,
        )
        if isinstance(handle, CustodyRefusal):
            raise ServiceCustodyError(handle.code)
        self.physical = ServiceProcessReceipt(self, handle, f"{self.owner_id}:physical")
        registry = getattr(self.coordinator, "_i16_source_registry", None)
        if registry is not None:
            try:
                registry.register_receipt(self, self.physical)
            except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
                self.physical.mark_unknown("unmapped_writer")
                raise ServiceCustodyError("i16_source_unregistered") from exc
        return self.physical

    def request_is_active(self, request: CustodyHandle | None) -> bool:
        """Return true only when this exact request handle is still in the admission map."""
        return request is not None and any(candidate is request for candidate in self.requests.values())

    def _request_for_admission(self, admission_id: str, request: CustodyHandle) -> str:
        current = self.requests.get(admission_id)
        if current is not request:
            raise ServiceCustodyError("service_request_missing")
        lease_id = getattr(getattr(request, "lease", None), "lease_id", None)
        if type(lease_id) is not str or not lease_id:
            raise ServiceCustodyError("service_request_identity_missing")
        return lease_id

    def record_result_durable(
        self, binding: TurnCustodyBinding, request: CustodyHandle, proof: DurableResultProof,
    ) -> None:
        self.validate_binding(binding)
        lease_id = self._request_for_admission(binding.admission_id, request)
        if self.physical is None:
            raise ServiceCustodyError("physical_receipt_missing")
        self.physical.record_result_durable(binding, lease_id, proof)
        self.durable_requests.add(binding.admission_id)

    def mark_delivery_unknown(
        self, binding: TurnCustodyBinding, request: CustodyHandle, reason: str,
    ) -> None:
        """Retain the exact physical/request/I04 tuple with one typed delivery reason."""
        if reason not in {"delivery_failed", "delivery_finalize_failed"}:
            raise ServiceCustodyError("delivery_reason_not_retryable")
        if binding.admission_id in self.delivery_terminalized:
            self._validate_service_identity(binding)
            if self.requests.get(binding.admission_id) is not request:
                raise ServiceCustodyError("service_request_missing")
            physical = self.physical
            if physical is None or physical.service is not self:
                raise ServiceCustodyError("physical_receipt_missing")
            physical.mark_unknown(reason)
            lease_id = getattr(getattr(physical.handle, "lease", None), "lease_id", None)
            if not isinstance(lease_id, str) or not lease_id:
                raise ServiceCustodyError("delivery_reconcile_failed")
            self.delivery_unknown_reasons[lease_id] = reason
            self.unknown = True
            return
        lease_id = self.validate_terminal_delivery(binding, request)
        physical = self.physical
        if physical is None:
            raise ServiceCustodyError("physical_receipt_missing")
        if physical.state == "unknown" and physical.unknown_reason != reason:
            raise ServiceCustodyError("delivery_reconcile_failed")
        handles = (physical.handle, request, binding.parent_handle)
        lock = getattr(self.coordinator, "_lock", None)
        records = getattr(self.coordinator, "_records", None)
        if lock is None or not isinstance(records, dict):
            raise ServiceCustodyError("delivery_reconcile_failed")
        for handle in handles:
            lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
            with lock:
                record = records.get(lease_id)
                if (
                    record is None
                    or getattr(record, "handle", None) is not handle
                    or (getattr(record, "state", None) == "unknown"
                        and self.delivery_unknown_reasons.get(lease_id) != reason)
                ):
                    raise ServiceCustodyError("delivery_reconcile_failed")
            result = self.coordinator.mark_unknown(handle, "join_failed")
            if result is not None and getattr(result, "code", None) not in {
                "custody_unknown", "join_failed",
            }:
                raise ServiceCustodyError("delivery_reconcile_failed")
            lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
            if not isinstance(lease_id, str) or not lease_id:
                raise ServiceCustodyError("delivery_reconcile_failed")
            self.delivery_unknown_reasons[lease_id] = reason
        physical.mark_unknown(reason)

    def reconcile_delivery(self, binding: TurnCustodyBinding, request: CustodyHandle) -> None:
        lease_id = self.validate_terminal_delivery(binding, request)
        physical = self.physical
        reason = None if physical is None else physical.unknown_reason
        if reason not in {"delivery_failed", "delivery_finalize_failed"}:
            raise ServiceCustodyError("physical_custody_unknown")
        if self.delivery_unknown_reasons.get(lease_id) != reason:
            raise ServiceCustodyError("delivery_reconcile_failed")
        handles = (physical.handle, request, binding.parent_handle)
        # The request and I04 parent are part of the same retained retry tuple. An accepted
        # record is never reopened merely because the physical receipt is unknown.
        for handle in handles:
            if not self._exact_delivery_record(handle, binding, reason):
                self.unknown = True
                raise ServiceCustodyError("delivery_reconcile_failed")
        for handle in handles:
            result = self._reconcile_exact_delivery_handle(handle, binding, reason)
            if result is not None:
                for candidate in handles:
                    self.coordinator.mark_unknown(candidate, "delivery_reconcile_failed")
                raise ServiceCustodyError(getattr(result, "code", "delivery_reconcile_failed"))
        self.unknown = False
        physical.state = "process_bound"
        physical.unknown_reason = None
        for handle in handles:
            self.delivery_unknown_reasons.pop(
                getattr(getattr(handle, "lease", None), "lease_id", None), None,
            )

    def _exact_delivery_record(
        self, handle: CustodyHandle, binding: TurnCustodyBinding, reason: str,
    ) -> bool:
        lock = getattr(self.coordinator, "_lock", None)
        records = getattr(self.coordinator, "_records", None)
        lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
        if lock is None or not isinstance(records, dict) or not isinstance(lease_id, str):
            return False
        with lock:
            record = records.get(lease_id)
            return (
                record is not None
                and getattr(record, "handle", None) is handle
                and handle.coordinator_id == self.coordinator._coordinator_id
                and handle.profile_id == binding.profile_id
                and handle.process_instance == binding.process_instance
                and handle.authority_epoch == binding.authority_epoch
                and getattr(record, "state", None) == "unknown"
                and getattr(record, "reason", None) in {"join_failed", "custody_unknown"}
                and self.delivery_unknown_reasons.get(lease_id) == reason
            )

    def _reconcile_exact_delivery_handle(
        self, handle: CustodyHandle, binding: TurnCustodyBinding, reason: str,
    ):
        lock = getattr(self.coordinator, "_lock", None)
        records = getattr(self.coordinator, "_records", None)
        lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
        if lock is None or not isinstance(records, dict) or not isinstance(lease_id, str):
            return CustodyRefusal("delivery_reconcile_failed")
        with lock:
            record = records.get(lease_id)
            if (
                record is None
                or getattr(record, "handle", None) is not handle
                or handle.coordinator_id != self.coordinator._coordinator_id
                or handle.profile_id != binding.profile_id
                or handle.process_instance != binding.process_instance
                or handle.authority_epoch != binding.authority_epoch
                or getattr(record, "state", None) != "unknown"
                or getattr(record, "reason", None) not in {"join_failed", "custody_unknown"}
                or self.delivery_unknown_reasons.get(lease_id) != reason
            ):
                return CustodyRefusal("delivery_reconcile_failed")
        return self.coordinator.reconcile_unknown(
            handle, owner_id=handle.owner_id, profile_id=binding.profile_id,
            process_instance=binding.process_instance, authority_epoch=binding.authority_epoch,
        )

    def _retry_terminalized_delivery(self, binding: TurnCustodyBinding, request: CustodyHandle) -> None:
        """Retry only the physical phase after the exact I04 terminalization already committed."""
        self._validate_service_identity(binding)
        if self.requests.get(binding.admission_id) is not request or self.physical is None:
            raise ServiceCustodyError("delivery_reconcile_failed")
        physical = self.physical
        if physical.service is not self or physical.state not in {"closed", "finalizing", "unknown"}:
            raise ServiceCustodyError("delivery_reconcile_failed")
        managed_client = physical.managed_client if physical.ephemeral_process else None
        if physical.state == "unknown":
            if physical.unknown_reason != "delivery_finalize_failed":
                raise ServiceCustodyError("delivery_reconcile_failed")
            restored = self.coordinator.reconcile_unknown(
                physical.handle, owner_id=physical.handle.owner_id, profile_id=binding.profile_id,
                process_instance=binding.process_instance, authority_epoch=binding.authority_epoch,
            )
            if restored is not None:
                raise ServiceCustodyError("delivery_reconcile_failed")
            physical.state = "finalizing"
            physical.unknown_reason = None
            self.unknown = False
        if physical.state != "closed":
            if physical.commit_delivery_close() is not True:
                self.unknown = True
                raise ServiceCustodyError("delivery_finalize_failed")
        lease_id = getattr(getattr(request, "lease", None), "lease_id", None)
        if not isinstance(lease_id, str):
            raise ServiceCustodyError("service_request_identity_missing")
        if physical.ephemeral_process:
            from gateway.runtime_service_operations import retire_per_invocation_parent
            retire_per_invocation_parent(self, binding, request, physical)
            self.unknown = False
        physical.clear_request(lease_id)
        self.durable_requests.discard(binding.admission_id)
        self.requests.pop(binding.admission_id, None)
        self.delivery_terminalized.discard(binding.admission_id)
        self.physical = None
        owner = self.external_parent
        if owner is not None and getattr(owner, "_physical", None) is physical:
            owner._physical = None
            if owner.state != "terminal":
                owner.state = "request_reserved"
        if managed_client is not None:
            refresh = getattr(managed_client, "_refresh_closed_state", None)
            if callable(refresh):
                refresh()

    def complete_delivery(self, binding: TurnCustodyBinding, request: CustodyHandle) -> None:
        if binding.admission_id in self.delivery_terminalized:
            self._retry_terminalized_delivery(binding, request)
            return
        lease_id = self.validate_terminal_delivery(binding, request)
        if binding.admission_id not in self.durable_requests or self.physical is None:
            self.unknown = True
            raise ServiceCustodyError("delivery_before_durable_result")
        if self.physical.state == "unknown":
            self.reconcile_delivery(binding, request)
        physical = self.physical
        managed_client = physical.managed_client if physical.ephemeral_process else None
        physical.mark_delivery_complete(lease_id)
        # Native close is prepared while proof/request maps remain.  The exact I04 request
        # is terminalized next; only then is the physical coordinator handle committed.
        if physical.ephemeral_process and physical.prepare_delivery_close(lease_id) is not True:
            self.unknown = True
            raise ServiceCustodyError("delivery_finalize_failed")
        result = self.coordinator.mark_terminal_with_secondary_parents(request, (binding.parent_handle,))
        if result is not None:
            physical._delivery_finalize_unknown()
            self.unknown = True
            raise ServiceCustodyError("delivery_finalize_failed")
        if physical.ephemeral_process:
            self.delivery_terminalized.add(binding.admission_id)
            if physical.commit_delivery_close() is not True:
                self.unknown = True
                raise ServiceCustodyError("delivery_finalize_failed")
            try:
                from gateway.runtime_service_operations import retire_per_invocation_parent
                retire_per_invocation_parent(self, binding, request, physical)
            except ServiceCustodyError:
                # The native receipt is already closed, but the one-shot parent must remain
                # attached so the same finalizer can retry its exact root/registry fence.
                self.physical = physical
                self.unknown = True
                raise
        physical.clear_request(lease_id)
        self.durable_requests.discard(binding.admission_id)
        self.requests.pop(binding.admission_id, None)
        if physical.ephemeral_process:
            self.delivery_terminalized.discard(binding.admission_id)
            self.physical = None
            owner = self.external_parent
            if owner is not None and getattr(owner, "_physical", None) is physical:
                owner._physical = None
                if owner.state != "terminal":
                    owner.state = "request_reserved"
            if managed_client is not None:
                refresh = getattr(managed_client, "_refresh_closed_state", None)
                if callable(refresh):
                    refresh()
        else:
            self.physical = physical

    def finish_request(self, binding: TurnCustodyBinding, *, durable: bool) -> None:
        self.validate_binding(binding)
        request = self.requests.get(binding.admission_id)
        if request is None:
            raise ServiceCustodyError("service_request_missing")
        if durable:
            raise ServiceCustodyError("delivery_receipt_required")
        self.coordinator.mark_unknown(request, "join_failed")
        self.unknown = True

    def forget_request(self, admission_id: str) -> None:
        self.requests.pop(admission_id, None)
        self.durable_requests.discard(admission_id)

    def close(self) -> None:
        if self.physical is not None:
            if self.physical.state == "closed":
                self.physical = None
            elif not self.physical.close_after_delivery():
                self.unknown = True
                raise ServiceCustodyError("physical_custody_pending")
            else:
                self.physical = None
        if self.requests or self.durable_requests:
            self.unknown = True
            for request in self.requests.values():
                self.coordinator.mark_unknown(request, "join_failed")
            return
        result = self.coordinator.mark_terminal(self.handle)
        if result is not None:
            self.unknown = True
            raise ServiceCustodyError(getattr(result, "code", "service_release_failed"))


def reserve_service(
    binding: TurnCustodyBinding, owner_id: str, *, producer_key: str | None = None,
) -> ServiceLease:
    """Reserve a service root after validating the exact active parent admission."""
    _require_binding(binding)
    owner_id = _string(owner_id, "service_owner_id")
    registry = getattr(binding.coordinator, "_i16_source_registry", None)
    if registry is not None:
        try:
            if producer_key is None:
                raise ServiceCustodyError("i16_source_unregistered")
            registry.require_producer(producer_key)
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            raise ServiceCustodyError("i16_source_unregistered") from exc
    handle = binding.coordinator.reserve_root(
        "I16_turn_sidecars", owner_id, profile_id=binding.profile_id,
        authority_epoch=binding.authority_epoch,
    )
    if isinstance(handle, CustodyRefusal):
        raise ServiceCustodyError(handle.code)
    service = ServiceLease(
        binding.coordinator, handle, binding, owner_id, {}, producer_key=producer_key,
    )
    if registry is not None:
        try:
            registry.register_service(service, producer_key)
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            binding.coordinator.release_unaccepted(handle)
            raise ServiceCustodyError("i16_source_unregistered") from exc
    return service


def service_for_external_parent(
    coordinator: RuntimeCustodyCoordinator, handle: CustodyHandle, owner_id: str,
    *, producer_key: str | None = None,
) -> ServiceLease:
    """Adapt one existing I16 root without inventing an I04 binding."""
    if (
        not isinstance(coordinator, RuntimeCustodyCoordinator)
        or not isinstance(handle, CustodyHandle)
        or handle.owner_class != "I16_turn_sidecars"
        or handle.coordinator_id != getattr(coordinator, "_coordinator_id", None)
    ):
        raise ServiceCustodyError("preservation_owner_mismatch")
    owner_id = _string(owner_id, "service_owner_id")
    registry = getattr(coordinator, "_i16_source_registry", None)
    if registry is not None:
        try:
            if producer_key is None:
                raise ServiceCustodyError("i16_source_unregistered")
            registry.require_producer(producer_key)
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            raise ServiceCustodyError("i16_source_unregistered") from exc
    service = ServiceLease(coordinator, handle, None, owner_id, {}, producer_key=producer_key)
    if registry is not None:
        try:
            registry.register_service(service, producer_key)
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            raise ServiceCustodyError("i16_source_unregistered") from exc
    return service
