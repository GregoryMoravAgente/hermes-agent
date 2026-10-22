"""Managed ACP child transitions for one authenticated service receipt."""

from __future__ import annotations

from typing import Any


_MANAGED_PREFIX_HANDOFF = "_classic_writer_intermediate_prefix_handoff"
_MANAGED_INTERMEDIATE_FENCE = "_classic_writer_intermediate_fence"
_MANAGED_INTERMEDIATE_SEAL = "_classic_writer_intermediate_fence_seal"


def _managed_prefix_aliases_valid(
    receipt: Any, service: Any, binding: Any, request: Any, handoff: Any,
) -> bool:
    parent = getattr(service, "external_parent", None)
    client = getattr(handoff, "client", None)
    agent = getattr(handoff, "agent", None)
    return bool(
        handoff is not None
        and getattr(handoff, "state", None) == "unknown"
        and getattr(receipt, _MANAGED_PREFIX_HANDOFF, None) is handoff
        and getattr(handoff, "agent", None) is agent
        and getattr(handoff, "parent", None) is parent
        and getattr(handoff, "service", None) is service
        and getattr(handoff, "binding", None) is binding
        and getattr(handoff, "request", None) is request
        and getattr(handoff, "receipt", None) is receipt
        and getattr(parent, "_owner_agent", None) is agent
        and getattr(parent, "_service", None) is service
        and getattr(parent, "_physical", None) is receipt
        and getattr(parent, "turn_binding", None) is binding
        and getattr(parent, "_catalog_client", None) is client
        and getattr(receipt, "service", None) is service
        and getattr(receipt, "managed_client", None) is client
    )


def _managed_prefix_fence_aliases_valid(receipt: Any, parent: Any, handoff: Any) -> bool:
    agent = getattr(handoff, "agent", None)
    seal = getattr(handoff, "identity_seal", None)
    return bool(
        getattr(agent, _MANAGED_INTERMEDIATE_FENCE, None) is handoff
        and getattr(agent, _MANAGED_INTERMEDIATE_SEAL, None) is seal
        and getattr(parent, _MANAGED_INTERMEDIATE_FENCE, None) is handoff
        and getattr(parent, _MANAGED_INTERMEDIATE_SEAL, None) is seal
        and getattr(receipt, _MANAGED_INTERMEDIATE_FENCE, None) is handoff
        and getattr(receipt, _MANAGED_INTERMEDIATE_SEAL, None) is seal
    )


def _managed_prefix_witness_valid(
    receipt: Any, service: Any, binding: Any, request: Any, handoff: Any,
) -> bool:
    parent = getattr(service, "external_parent", None)
    client = getattr(handoff, "client", None)
    witness = getattr(handoff, "physical_witness", None)
    return bool(
        getattr(witness, "writer_proof", None) is getattr(handoff, "writer_proof", None)
        and getattr(witness, "client", None) is client
        and getattr(witness, "parent", None) is parent
        and getattr(witness, "service", None) is service
        and getattr(witness, "request", None) is request
        and getattr(witness, "binding", None) is binding
        and getattr(witness, "receipt", None) is receipt
        and getattr(witness, "binding_identity", None) == getattr(handoff, "binding_identity", None)
    )


def _managed_prefix_durable_valid(
    receipt: Any, service: Any, binding: Any, request: Any, handoff: Any,
) -> bool:
    witness = getattr(handoff, "physical_witness", None)
    durable = getattr(witness, "durable_proof", None)
    request_id = getattr(getattr(request, "lease", None), "lease_id", None)
    durable_proofs = getattr(receipt, "durable_proofs", None)
    durable_requests = getattr(service, "durable_requests", None)
    return bool(
        type(request_id) is str
        and isinstance(durable_proofs, dict)
        and durable_proofs.get(request_id) is durable
        and getattr(receipt, "durable_proof", None) is durable
        and isinstance(durable_requests, set)
        and getattr(binding, "admission_id", None) in durable_requests
        and durable is not None
    )


def _managed_prefix_handoff_valid(
    receipt: Any, service: Any, binding: Any, request: Any, handoff: Any,
) -> bool:
    """Validate one exact retained prefix before resetting its failed child."""
    parent = getattr(service, "external_parent", None)
    return bool(
        _managed_prefix_aliases_valid(receipt, service, binding, request, handoff)
        and _managed_prefix_fence_aliases_valid(receipt, parent, handoff)
        and _managed_prefix_witness_valid(receipt, service, binding, request, handoff)
        and _managed_prefix_durable_valid(receipt, service, binding, request, handoff)
    )


def accepts_managed_proof_update(receipt: Any, prior: Any, proof: Any) -> bool:
    """Allow only a strict same-admission prefix extension of a writer proof."""
    return bool(
        getattr(receipt, "ephemeral_process", False)
        and len(proof.row_ids) > len(prior.row_ids)
        and tuple(proof.row_ids[:len(prior.row_ids)]) == prior.row_ids
        and tuple(proof.row_snapshots[:len(prior.row_snapshots)]) == prior.row_snapshots
        and tuple(proof.row_sessions[:len(prior.row_sessions)]) == prior.row_sessions
        and tuple(proof.row_roles[:len(prior.row_roles)]) == prior.row_roles
        and len(proof.result_row_ids) > len(prior.result_row_ids)
        and tuple(proof.result_row_ids[:len(prior.result_row_ids)]) == prior.result_row_ids
    )


def prepare_next_managed_child(receipt: Any) -> bool:
    """Retain one physical handle while resetting only a settled managed child."""
    ready = not (
        not getattr(receipt, "ephemeral_process", False)
        or getattr(receipt, "state", None) not in {"process_bound", "readers_active"}
        or getattr(receipt, "process", None) is None
        or receipt.process.poll() is None
        or getattr(receipt, "reader_error", None) is not None
        or set(getattr(receipt, "readers", {})) != {"stdout", "stderr"}
        or getattr(receipt, "eof_streams", set()) != {"stdout", "stderr"}
        or getattr(receipt, "joined_streams", set()) != {"stdout", "stderr"}
        or not getattr(receipt, "durable_proofs", None)
        or getattr(receipt, "delivery_requests", set())
    )
    if not ready:
        if getattr(receipt, "ephemeral_process", False):
            receipt.mark_unknown("managed_child_not_ready")
        return False
    if not receipt.close_native_after_process():
        receipt.mark_unknown("physical_close_failed")
        return False
    witness = getattr(receipt, "managed_witness", None)
    if isinstance(witness, dict):
        receipt.managed_child_history.append(dict(witness))
    _reset_child_fields(receipt, ready=True)
    receipt.state = "reserved"
    return True


def reset_managed_child_after_retry(receipt: Any, *, retained_prefix: Any = None) -> bool:
    """Reopen one exact child while optionally carrying its authenticated prefix token."""
    setup_fence = getattr(receipt, "setup_fence", None)
    if setup_fence is not None and not setup_fence.is_set():
        return False
    existing_prefix = getattr(receipt, _MANAGED_PREFIX_HANDOFF, None)
    if retained_prefix is None:
        if existing_prefix is not None:
            return False
    elif existing_prefix is not retained_prefix:
        return False
    if retained_prefix is not None and not _managed_prefix_handoff_valid(
        receipt, getattr(receipt, "service", None),
        getattr(retained_prefix, "binding", None), getattr(retained_prefix, "request", None),
        retained_prefix,
    ):
        return False
    witness = getattr(receipt, "managed_witness", None)
    if getattr(receipt, "ephemeral_process", False) and isinstance(witness, dict):
        receipt.managed_child_history.append(dict(witness))
    _reset_child_fields(receipt, ready=True)
    receipt.managed_witness = None
    if retained_prefix is not None:
        setattr(receipt, _MANAGED_PREFIX_HANDOFF, retained_prefix)
    receipt.service.unknown = False
    return True


def _reset_child_fields(receipt: Any, *, ready: bool) -> None:
    receipt.retry_ready = False
    receipt.managed_child_ready = ready
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
    receipt.native_close_committed = False
    if ready:
        receipt.state = "reserved"


def prepare_service_next_managed_child(service: Any, binding: Any, request: Any) -> bool:
    """Authenticate and advance one child without releasing its service admission."""
    service.validate_binding(binding)
    current = service.requests.get(binding.admission_id)
    receipt = service.physical
    if (
        service.unknown
        or service.binding is not binding
        or current is not request
        or receipt is None
        or receipt.service is not service
        or not receipt.ephemeral_process
        or binding.admission_id not in service.durable_requests
    ):
        return False
    if not prepare_next_managed_child(receipt):
        return False
    owner = service.external_parent
    if owner is not None and getattr(owner, "_physical", None) is receipt:
        owner.state = "request_reserved"
    return True


def reconcile_service_managed_child_failure(service: Any, binding: Any, request: Any) -> bool:
    """Reconcile a later-child failure while retaining the prior prefix proof."""
    from gateway.runtime_service_custody import (
        _MANAGED_PROCESS_RETRY_REASONS,
        _authoritative_admission_row,
        _require_authoritative_row,
        _require_binding_shape,
        _require_live_execution,
    )

    _require_binding_shape(binding, allow_unknown_parent=True)
    row = _authoritative_admission_row(binding.authority, binding.admission_id)
    _require_authoritative_row(binding, row, status="started")
    _require_live_execution(binding)
    receipt = service.physical
    if (
        not service.unknown
        or service.binding is not binding
        or service.requests.get(binding.admission_id) is not request
        or receipt is None
        or receipt.service is not service
        or receipt.state != "unknown"
        or receipt.unknown_reason not in _MANAGED_PROCESS_RETRY_REASONS | {"spawn_not_started"}
        or not receipt.ephemeral_process
        or not receipt.durable_proofs
        or receipt.delivery_requests
        or receipt.completed_requests
        or service.delivery_unknown_reasons
        or service.delivery_terminalized
        or not service._retry_record_is_exact(service.handle, binding, service.owner_id)
        or not service._retry_record_is_exact(receipt.handle, binding, receipt.owner_id)
    ):
        return False
    retained_prefix = getattr(receipt, _MANAGED_PREFIX_HANDOFF, None)
    if retained_prefix is not None and not _managed_prefix_handoff_valid(
        receipt, service, binding, request, retained_prefix,
    ):
        return False
    if receipt.process is not None:
        if (
            receipt.transaction is not None
            and getattr(receipt.transaction, "process", None) is not receipt.process
        ):
            return False
        if not receipt.prepare_process_retry() or not service._readers_stopped(receipt):
            return False
    elif receipt.transaction is not None:
        return False
    restored_handles: list[Any] = []
    for handle in (receipt.handle, service.handle):
        restored = service.coordinator.reconcile_unknown(
            handle, owner_id=handle.owner_id, profile_id=binding.profile_id,
            process_instance=binding.process_instance, authority_epoch=binding.authority_epoch,
        )
        if restored is not None:
            for restored_handle in restored_handles:
                service.coordinator.mark_unknown(restored_handle, "managed_retry_reconcile_failed")
            return False
        restored_handles.append(handle)
    if not reset_managed_child_after_retry(receipt, retained_prefix=retained_prefix):
        return False
    service.unknown = False
    owner = service.external_parent
    if owner is not None and getattr(owner, "_physical", None) is receipt:
        owner.state = "request_reserved"
    return True
