"""Codex app-server adapter for the gateway's shared service custody ledger."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from gateway.runtime_service_custody import (
    DurableResultProof,
    ServiceCustodyError,
    ServiceLease,
    TurnCustodyBinding,
    reserve_service,
    validate_turn_binding,
)


@dataclass
class CodexAppServerCustody:
    """Keep one service lease and exact per-admission request children."""

    binding: TurnCustodyBinding
    owner_id: str
    service: ServiceLease | None = None
    request: Any = None

    def _binding_matches(self, binding: TurnCustodyBinding) -> bool:
        return (
            isinstance(binding, TurnCustodyBinding)
            and binding.coordinator is self.binding.coordinator
            and binding.authority is self.binding.authority
            and binding.profile_id == self.binding.profile_id
            and binding.authority_epoch == self.binding.authority_epoch
            and binding.process_instance == self.binding.process_instance
            and binding.parent_handle is self.binding.parent_handle
            and binding.admission_id == self.binding.admission_id
            and binding.execution_generation == self.binding.execution_generation
            and binding.session_id == self.binding.session_id
            and (not self.binding.principal_id or binding.principal_id == self.binding.principal_id)
            and (not self.binding.source_identity or binding.source_identity == self.binding.source_identity)
        )

    def _check_binding(self, binding: TurnCustodyBinding) -> None:
        validate_turn_binding(binding)
        if not self._binding_matches(binding):
            raise ServiceCustodyError("preservation_owner_mismatch")
        if self.service is not None:
            self.service.validate_binding(binding)

    def _service_identity_matches(self, binding: TurnCustodyBinding) -> bool:
        """Match only persistent service identity during a completed-to-new transition."""
        return (
            isinstance(binding, TurnCustodyBinding)
            and binding.coordinator is self.binding.coordinator
            and binding.authority is self.binding.authority
            and binding.profile_id == self.binding.profile_id
            and binding.authority_epoch == self.binding.authority_epoch
            and binding.process_instance == self.binding.process_instance
            and binding.session_id == self.binding.session_id
            and (not self.binding.principal_id or binding.principal_id == self.binding.principal_id)
            and (
                not self.binding.source_identity
                or binding.source_identity == self.binding.source_identity
            )
        )

    def _check_next_admission_binding(
        self, binding: TurnCustodyBinding, completed_request: Any,
    ) -> None:
        service = self.service
        physical = None if service is None else service.physical
        request_id = getattr(getattr(completed_request, "lease", None), "lease_id", None)
        if (
            service is None
            or service.unknown
            or service.requests
            or service.durable_requests
            or service.delivery_unknown_reasons
            or physical is None
            or physical.service is not service
            or physical.state in {"unknown", "released", "closed"}
            or physical.unknown_reason is not None
            or type(request_id) is not str
            or request_id not in physical.completed_requests
        ):
            raise ServiceCustodyError("custody_unknown")
        if not self._service_identity_matches(binding):
            raise ServiceCustodyError("preservation_owner_mismatch")

    def _retained_unknown(self, binding: TurnCustodyBinding, request: Any) -> bool:
        physical = None if self.service is None else self.service.physical
        return (
            self._binding_matches(binding)
            and self.service is not None
            and self.request is request
            and self.service.request_is_active(request)
            and (self.service.unknown or (physical is not None and physical.state == "unknown"))
        )

    def begin_turn(self, binding: TurnCustodyBinding) -> None:
        """Reserve the request child and bind it before Codex subprocess construction."""
        completed_request = self.request
        reusing_completed = False
        if completed_request is None:
            self._check_binding(binding)
        elif self.service is None or self.service.request_is_active(completed_request):
            self._check_binding(binding)
            raise ServiceCustodyError("service_request_in_flight")
        else:
            # A stale pointer is reusable only after the exact request was completed and
            # removed; active/unknown requests remain under the exact admission binding.
            self._check_next_admission_binding(binding, completed_request)
            reusing_completed = True
        if self.service is None:
            self.service = reserve_service(binding, self.owner_id)
        try:
            request = self.service.begin_request(binding)
        except Exception:
            if reusing_completed:
                # A refused next admission must not close or replace the persistent root.
                raise
            try:
                self.service.close()
            except ServiceCustodyError:
                self.service.unknown = True
            binding.authority._preservation_mark_unknown(binding.admission_id, "join_failed")
            raise
        try:
            binding.authority._preservation_bind_sidecar(
                binding.admission_id, binding.parent_handle, self.service, request,
            )
        except Exception:
            self.service.finish_request(binding, durable=False)
            binding.authority._preservation_mark_unknown(binding.admission_id, "join_failed")
            raise
        self.binding = binding
        self.request = request

    def reserve_physical(self, binding: TurnCustodyBinding | None = None):
        binding = binding or self.binding
        self._check_binding(binding)
        if self.service is None or self.request is None:
            raise ServiceCustodyError("service_request_missing")
        return self.service.reserve_physical(binding)

    def bind_physical_launch(self, process, transaction=None) -> None:
        if self.service is None or self.service.physical is None:
            raise ServiceCustodyError("physical_receipt_missing")
        self.service.physical.bind_launch(process, transaction)

    def record_result_durable(self, binding: TurnCustodyBinding, proof: DurableResultProof) -> None:
        self._check_binding(binding)
        if self.service is None or self.request is None:
            raise ServiceCustodyError("service_request_missing")
        self.service.record_result_durable(binding, self.request, proof)

    def finish_turn(
        self, binding: TurnCustodyBinding, *, durable: bool, proof: DurableResultProof | None = None,
    ) -> None:
        """Stage a typed durable result; terminalization waits for delivery completion."""
        self._check_binding(binding)
        request = self.request
        if request is None or self.service is None:
            raise ServiceCustodyError("service_request_missing")
        if not durable:
            self.service.finish_request(binding, durable=False)
            return
        if proof is None:
            self.service.unknown = True
            binding.authority._preservation_mark_unknown(binding.admission_id, "durable_result_proof_missing")
            raise ServiceCustodyError("durable_result_proof_missing")
        self.service.record_result_durable(binding, request, proof)
        binding.authority._preservation_release_sidecar(binding.admission_id, request, durable=True)

    def mark_unknown(self, binding: TurnCustodyBinding, reason: str = "join_failed") -> None:
        """Retain the request and parent when the physical or durable boundary is ambiguous."""
        request = self.request
        if request is None:
            return
        try:
            self._check_binding(binding)
        except ServiceCustodyError:
            # A transport close can report the same failure after the first unknown transition
            # already retained this exact request.  Preserve that original transport error and
            # never reopen or replace the retained tuple.
            if self._retained_unknown(binding, request):
                return
            raise
        if not self.service.request_is_active(request):
            self.request = None
            return
        if self.service.physical is not None:
            self.service.physical.mark_unknown(reason)
        self.service.coordinator.mark_unknown(request, reason)
        self.service.unknown = True
        binding.authority._preservation_release_sidecar(
            binding.admission_id, request, durable=False,
        )

    def close(self) -> None:
        """Close a service only after all request children are terminal."""
        if self.service is None:
            return
        if self.request is not None:
            if self.service.request_is_active(self.request):
                self.service.unknown = True
                self.service.coordinator.mark_unknown(self.request, "join_failed")
                return
            # A completed request is removed by the authenticated delivery finalizer.
            # Do not mark that old handle unknown while closing the persistent root.
            self.request = None
        self.service.close()
        self.service = None
