# health: allow FILE_LINES -- lossless merged-prefix proof keeps carrier lookup, exact row readback,
# and full-content reconstruction together so no suffix can be claimed without durable evidence.
"""Typed SessionDB write witnesses for managed Codex custody.

The legacy flush API remains a bool/None API.  This sibling opts the one managed
caller into a capture of the exact writer batch, then reads every intended row back
from the same SessionDB after the writer has committed.  It never writes a second
time, accepts a truthy legacy result, or treats a live marker as the durable row.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
from itertools import pairwise
from pathlib import Path
import sqlite3
from typing import Any

from agent.message_metadata import DB_ROW_SNAPSHOT
from hermes_state_common import _placeholders
from agent.session_persistence_physical_tip import (
    _gateway_writer_physical_witness_valid,
    _is_direct_physical_tip_witness,
    _is_managed_receipt_witness,
    _managed_physical_tip_valid,
    _physical_tip_value,
)

@dataclass(frozen=True)
class SessionDBWriteProof:
    store_path: str
    session_id: str
    row_ids: tuple[int, ...]
    row_snapshots: tuple[str, ...]
    row_sessions: tuple[str, ...]
    row_roles: tuple[str, ...]
    payload_digest: str
    # The managed Codex turn supplies the exact persisted result suffix after the
    # writer has committed.  A low-level writer proof may leave this empty, but
    # such a proof cannot cross the managed custody terminal fence.
    result_row_ids: tuple[int, ...] = ()

@dataclass(frozen=True)
class GatewayWriterProofHandoff:
    """Private one-turn handoff of the canonical writer proof to Gateway.

    The proof remains the immutable value produced by the existing SessionDB writer.  This
    envelope carries only the already-authenticated turn binding across the in-process result
    seam; it is consumed once before any public result projection.
    """

    writer_proof: SessionDBWriteProof
    binding: Any
    turn_id: str
    physical_tip_witness: Any = None

@dataclass(frozen=True)
class ManagedIntermediateWriterFence:
    """The one retained managed-child prefix and its same-receipt continuation token.

    The fence is an in-process continuation token, not a second custody ledger.  Object
    references and an opaque seal are retained so a copied proof, replacement fence, new
    client, or new admission cannot consume the prefix.  ``unknown`` blocks ordinary writer
    fallback, but an exact same-owner retry may revalidate and continue this token.
    """

    writer_proof: SessionDBWriteProof
    physical_witness: Any
    agent: Any
    client: Any
    parent: Any
    service: Any
    request: Any
    receipt: Any
    binding: Any
    turn_id: str
    lease_id: str
    owner_identity: tuple[Any, ...]
    binding_identity: tuple[Any, ...]
    identity_seal: Any
    state: str = "pending"
    reason: str = ""

_GATEWAY_WRITER_PROOF_REQUEST = "_classic_writer_proof_requested"
_GATEWAY_WRITER_PROOF_HANDOFF = "_classic_writer_proof_handoff"
_GATEWAY_WRITER_PROOF_BINDING = "_classic_writer_proof_binding"
_GATEWAY_WRITER_PROOF_TURN_ID = "_classic_writer_proof_turn_id"
_GATEWAY_WRITER_PROOF_BOUND_BINDING = "_classic_writer_proof_bound_binding"
_GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS = "_classic_writer_proof_physical_witness"
_GATEWAY_WRITER_PROOF_PHYSICAL_REQUIRED = "_classic_writer_proof_physical_required"
_GATEWAY_WRITER_PROOF_UNKNOWN = "_classic_writer_proof_unknown"
_GATEWAY_WRITER_INTERMEDIATE_FENCE = "_classic_writer_intermediate_fence"
_GATEWAY_WRITER_INTERMEDIATE_SEAL = "_classic_writer_intermediate_fence_seal"
_GATEWAY_WRITER_INTERMEDIATE_PREFIX_HANDOFF = "_classic_writer_intermediate_prefix_handoff"
_GATEWAY_WRITER_UNKNOWN = object()
_GATEWAY_WRITER_PREFIX_REPAIR = object()
_UNSET = object()

def _set_managed_intermediate_fence_aliases(
    agent: Any, parent: Any, receipt: Any, fence: ManagedIntermediateWriterFence,
) -> bool:
    """Publish one fence object on every already-owned object that carries its identity.

    The aliases are deliberately object references, rather than a registry or copied record.
    A later retry must observe the same token on the agent, the service parent, and the
    physical receipt before it can reopen or consume anything.
    """
    owners = (agent, parent, receipt)
    try:
        for owner in owners:
            if owner is None:
                return False
            setattr(owner, _GATEWAY_WRITER_INTERMEDIATE_FENCE, fence)
            setattr(owner, _GATEWAY_WRITER_INTERMEDIATE_SEAL, fence.identity_seal)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False
    return True


def _managed_intermediate_fence_aliases_match(
    agent: Any, parent: Any, receipt: Any, fence: ManagedIntermediateWriterFence,
) -> bool:
    try:
        return all(
            getattr(owner, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None) is fence
            and getattr(owner, _GATEWAY_WRITER_INTERMEDIATE_SEAL, None) is fence.identity_seal
            for owner in (agent, parent, receipt)
        )
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def _clear_managed_intermediate_fence_aliases(
    agent: Any, parent: Any, receipt: Any, fence: ManagedIntermediateWriterFence,
) -> None:
    """Clear only aliases that still point at this exact fence object."""
    for owner in (agent, parent, receipt):
        if owner is None:
            continue
        try:
            if getattr(owner, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None) is fence:
                delattr(owner, _GATEWAY_WRITER_INTERMEDIATE_FENCE)
            if getattr(owner, _GATEWAY_WRITER_INTERMEDIATE_SEAL, None) is fence.identity_seal:
                delattr(owner, _GATEWAY_WRITER_INTERMEDIATE_SEAL)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            # A mismatched/replaced alias remains visible and therefore fails closed on
            # the next read; never clear a replacement object by name alone.
            continue


def _managed_intermediate_context(agent: Any, client: Any):
    """Resolve the exact live managed owner for one intermediate fence."""
    if client is None or getattr(agent, "_managed_acp_client", None) is not client:
        return None
    parent = getattr(getattr(client, "_producer_policy", None), "parent", None)
    service = getattr(parent, "_service", None)
    receipt = getattr(parent, "_physical", None)
    binding = getattr(parent, "turn_binding", None)
    request = (
        (getattr(service, "requests", {}) or {}).get(getattr(binding, "admission_id", None))
        if service is not None and binding is not None else None
    )
    try:
        from gateway.runtime_service_custody import TurnCustodyBinding, validate_turn_binding
    except (ImportError, AttributeError):
        return None
    if (
        not isinstance(binding, TurnCustodyBinding)
        or getattr(parent, "mode", None) != "managed_turn"
        or getattr(parent, "_owner_agent", None) is not agent
        or getattr(parent, "_catalog_client", None) is not client
        or getattr(parent, "_request", None) is not request
        or getattr(parent, "_service", None) is not service
        or getattr(parent, "_physical", None) is not receipt
        or getattr(service, "physical", None) is not receipt
        or getattr(service, "binding", None) is not binding
        or getattr(receipt, "service", None) is not service
        or getattr(receipt, "managed_client", None) is not client
        or getattr(service, "unknown", True) is not False
        or getattr(parent, "state", None) in {"unknown", "terminal"}
        or getattr(receipt, "state", None) in {"unknown", "closed", "released"}
        or request is None
        or getattr(receipt, "ephemeral_process", False) is not True
    ):
        return None
    try:
        validate_turn_binding(binding)
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if not _managed_physical_tip_valid(agent, binding):
        return None
    lease_id = getattr(getattr(request, "lease", None), "lease_id", None)
    if type(lease_id) is not str or not lease_id:
        return None
    owner_identity = tuple(
        getattr(parent, field, None)
        for field in ("operation_id", "profile_id", "process_instance", "authority_epoch", "source_identity")
    )
    binding_identity = tuple(
        getattr(binding, field, None)
        for field in (
            "admission_id", "execution_generation", "session_id", "profile_id",
            "authority_epoch", "process_instance", "principal_id", "source_identity",
        )
    )
    if any(item is None or (type(item) is str and not item) for item in owner_identity + binding_identity):
        return None
    return parent, service, request, receipt, binding, lease_id, owner_identity, binding_identity


def _managed_intermediate_recovery_context(
    agent: Any, client: Any, fence: ManagedIntermediateWriterFence,
):
    """Authenticate the retained object tuple while its coordinator is marked unknown.

    Ordinary reads deliberately reject unknown service/receipt state.  The same-owner retry
    is the one exception: it must use the fence's original objects and the authoritative
    started admission row, never a fresh ContextVar lookup or a replacement lease.
    """
    if type(fence) is not ManagedIntermediateWriterFence:
        return None
    if not _managed_intermediate_fence_aliases_match(agent, fence.parent, fence.receipt, fence):
        return None
    parent, service, request, receipt, binding = (
        fence.parent, fence.service, fence.request, fence.receipt, fence.binding,
    )
    if (
        fence.agent is not agent
        or fence.client is not client
        or getattr(agent, "_managed_acp_client", None) is not client
        or getattr(client, "_producer_policy", None) is None
        or getattr(client._producer_policy, "parent", None) is not parent
        or getattr(parent, "_owner_agent", None) is not agent
        or getattr(parent, "_catalog_client", None) is not client
        or getattr(parent, "_service", None) is not service
        or getattr(parent, "_physical", None) is not receipt
        or getattr(parent, "turn_binding", None) is not binding
        or getattr(service, "physical", None) is not receipt
        or getattr(receipt, "service", None) is not service
        or getattr(receipt, "managed_client", None) is not client
        or (getattr(service, "requests", {}) or {}).get(getattr(binding, "admission_id", None)) is not request
        or getattr(parent, "state", None) in {"terminal", "released"}
        or getattr(receipt, "state", None) in {"closed", "released"}
    ):
        return None
    try:
        from gateway.runtime_service_custody import (
            _authoritative_admission_row, _require_authoritative_row,
            _require_binding_shape, _require_live_execution,
        )
        _require_binding_shape(binding, allow_unknown_parent=True)
        row = _authoritative_admission_row(binding.authority, binding.admission_id)
        _require_authoritative_row(binding, row, status="started")
        _require_live_execution(binding)
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if not _managed_physical_tip_valid(
        agent, binding, _physical_tip_value(fence.physical_witness),
    ):
        return None
    return parent, service, request, receipt, binding


def _managed_writer_result_ids(witness: Any) -> tuple[int, ...] | None:
    durable = getattr(witness, "durable_proof", None)
    result_ids = getattr(durable, "result_row_ids", None)
    if (
        type(result_ids) is not tuple
        or not result_ids
        or len(set(result_ids)) != len(result_ids)
        or any(type(row_id) is not int or row_id <= 0 for row_id in result_ids)
    ):
        return None
    return result_ids


def _managed_writer_proof_readback(agent: Any, proof: Any) -> bool:
    """Re-read one immutable proof from the authoritative SessionDB before reuse."""
    if type(proof) is not SessionDBWriteProof or proof.result_row_ids:
        return False
    expected = tuple(zip(proof.row_ids, proof.row_roles, proof.row_snapshots))
    if (
        not expected
        or proof.row_sessions != tuple(proof.session_id for _row_id, _role, _snapshot in expected)
        or _read_expected_rows(agent, proof.session_id, expected) != expected
    ):
        return False
    return _proof_from_validated_rows(proof.store_path, proof.session_id, expected) == proof


def _managed_provider_owner_prefix(witness: Any) -> tuple[Any, ...] | None:
    """Return only the stable owner identity shared by all children of one service lease."""
    identity = getattr(witness, "provider_identity", None)
    if type(identity) is not tuple or len(identity) < 5:
        return None
    prefix = identity[:5]
    if any(item is None or (type(item) is str and not item) for item in prefix):
        return None
    return prefix


def _managed_writer_prefix_extends(
    agent: Any, prior_proof: Any, prior_witness: Any, next_proof: Any, next_witness: Any,
) -> bool:
    """Require both proof generations to read back and strictly extend one receipt."""
    prior_result_ids = _managed_writer_result_ids(prior_witness)
    next_result_ids = _managed_writer_result_ids(next_witness)
    if (
        not _managed_writer_proof_readback(agent, prior_proof)
        or not _managed_writer_proof_readback(agent, next_proof)
        or prior_result_ids is None
        or next_result_ids is None
        or prior_proof.store_path != next_proof.store_path
        or prior_proof.session_id != next_proof.session_id
        or len(next_result_ids) <= len(prior_result_ids)
        or next_result_ids[:len(prior_result_ids)] != prior_result_ids
        or len(next_proof.row_ids) <= len(prior_proof.row_ids)
        or tuple(next_proof.row_ids[:len(prior_proof.row_ids)]) != prior_proof.row_ids
        or tuple(next_proof.row_snapshots[:len(prior_proof.row_snapshots)]) != prior_proof.row_snapshots
        or tuple(next_proof.row_sessions[:len(prior_proof.row_sessions)]) != prior_proof.row_sessions
        or tuple(next_proof.row_roles[:len(prior_proof.row_roles)]) != prior_proof.row_roles
    ):
        return False
    prior_provider_prefix = _managed_provider_owner_prefix(prior_witness)
    next_provider_prefix = _managed_provider_owner_prefix(next_witness)
    return all(
        getattr(next_witness, field, None) is getattr(prior_witness, field, None)
        for field in ("client", "parent", "service", "request", "binding", "receipt")
    ) and all(
        getattr(next_witness, field, None) == getattr(prior_witness, field, None)
        for field in ("binding_identity", "binding_snapshot_identity")
    ) and (
        # provider_session_id/result_identity/payload_digest are expected to change for a
        # legitimate next child.  The first five provider fields remain the authenticated
        # operation/profile/process/epoch/source owner prefix and must not change.
        prior_provider_prefix is not None
        and next_provider_prefix is not None
        and prior_provider_prefix == next_provider_prefix
    ) and (
        getattr(prior_witness, "writer_proof", None) is prior_proof
        and getattr(next_witness, "writer_proof", None) is next_proof
    )


def _managed_intermediate_witness_shape(fence: ManagedIntermediateWriterFence) -> bool:
    witness = fence.physical_witness
    return bool(
        getattr(witness, "writer_proof", None) is fence.writer_proof
        and getattr(witness, "client", None) is fence.client
        and getattr(witness, "parent", None) is fence.parent
        and getattr(witness, "service", None) is fence.service
        and getattr(witness, "request", None) is fence.request
        and getattr(witness, "binding", None) is fence.binding
        and getattr(witness, "receipt", None) is fence.receipt
        and getattr(witness, "binding_identity", None) == fence.binding_identity
        and getattr(witness, "durable_proof", None) is not None
        and _managed_writer_result_ids(witness) is not None
    )


def _managed_prefix_aliases_match(
    agent: Any, client: Any, fence: ManagedIntermediateWriterFence,
) -> bool:
    """Require the exact receipt-local continuation token and all owner aliases."""
    receipt = fence.receipt
    parent = fence.parent
    service = fence.service
    return bool(
        getattr(receipt, _GATEWAY_WRITER_INTERMEDIATE_PREFIX_HANDOFF, None) is fence
        and _managed_intermediate_fence_aliases_match(agent, parent, receipt, fence)
        and fence.agent is agent
        and fence.client is client
        and getattr(client, "_producer_policy", None) is not None
        and getattr(client._producer_policy, "parent", None) is parent
        and getattr(parent, "_owner_agent", None) is agent
        and getattr(parent, "_catalog_client", None) is client
        and getattr(parent, "_service", None) is service
        and getattr(parent, "_physical", None) is receipt
        and getattr(parent, "turn_binding", None) is fence.binding
        and getattr(service, "external_parent", None) is parent
        and getattr(service, "binding", None) is fence.binding
        and getattr(service, "physical", None) is receipt
        and (getattr(service, "requests", {}) or {}).get(
            getattr(fence.binding, "admission_id", None),
        ) is fence.request
        and getattr(receipt, "service", None) is service
        and getattr(receipt, "managed_client", None) is client
    )


def _managed_prefix_witness_matches(
    client: Any, fence: ManagedIntermediateWriterFence,
) -> bool:
    witness = fence.physical_witness
    return bool(
        getattr(witness, "writer_proof", None) is fence.writer_proof
        and getattr(witness, "client", None) is client
        and getattr(witness, "parent", None) is fence.parent
        and getattr(witness, "service", None) is fence.service
        and getattr(witness, "request", None) is fence.request
        and getattr(witness, "binding", None) is fence.binding
        and getattr(witness, "receipt", None) is fence.receipt
        and getattr(witness, "binding_identity", None) == fence.binding_identity
        and getattr(witness, "binding_snapshot_identity", None)
            == getattr(fence.physical_witness, "binding_snapshot_identity", None)
    )


def _managed_prefix_durable_matches(fence: ManagedIntermediateWriterFence) -> bool:
    receipt = fence.receipt
    witness = fence.physical_witness
    durable = getattr(witness, "durable_proof", None)
    request_id = getattr(getattr(fence.request, "lease", None), "lease_id", None)
    durable_proofs = getattr(receipt, "durable_proofs", None)
    durable_requests = getattr(fence.service, "durable_requests", None)
    return bool(
        type(request_id) is str
        and isinstance(durable_proofs, dict)
        and durable_proofs.get(request_id) is durable
        and getattr(receipt, "durable_proof", None) is durable
        and isinstance(durable_requests, set)
        and getattr(fence.binding, "admission_id", None) in durable_requests
        and durable is not None
    )


def _managed_intermediate_prefix_identity_matches(
    agent: Any, client: Any, fence: ManagedIntermediateWriterFence,
) -> bool:
    """Check owner, request, receipt, aliases, seal, and witness without requiring
    the durable map to remain unchanged.  The extension branch uses this before
    authenticating a new same-receipt durable suffix.
    """
    return bool(
        _managed_prefix_aliases_match(agent, client, fence)
        and _managed_prefix_witness_matches(client, fence)
    )


def _managed_intermediate_prefix_handoff_matches(
    agent: Any, client: Any, fence: ManagedIntermediateWriterFence,
) -> bool:
    """Validate the retained prefix after its failed child was reset."""
    return bool(
        _managed_intermediate_prefix_identity_matches(agent, client, fence)
        and _managed_prefix_durable_matches(fence)
        and _managed_writer_proof_readback(agent, fence.writer_proof)
    )


def managed_intermediate_writer_fence(agent: Any, client: Any):
    """Return a valid pending/unknown fence, no fence, or UNKNOWN on any identity mismatch."""
    fence = getattr(agent, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None)
    if fence is None:
        return None
    if (
        type(fence) is not ManagedIntermediateWriterFence
        or fence.state not in {"pending", "unknown"}
        or getattr(agent, _GATEWAY_WRITER_INTERMEDIATE_SEAL, None) is not fence.identity_seal
    ):
        return _GATEWAY_WRITER_UNKNOWN
    context = _managed_intermediate_context(agent, client)
    if context is None:
        return _GATEWAY_WRITER_UNKNOWN
    parent, service, request, receipt, binding, lease_id, owner_identity, binding_identity = context
    turn_id = str(getattr(agent, "_current_turn_id", "") or "")
    if (
        not turn_id
        or fence.agent is not agent
        or fence.client is not client
        or fence.parent is not parent
        or fence.service is not service
        or fence.request is not request
        or fence.receipt is not receipt
        or fence.binding is not binding
        or fence.turn_id != turn_id
        or fence.lease_id != lease_id
        or fence.owner_identity != owner_identity
        or fence.binding_identity != binding_identity
        or type(fence.writer_proof) is not SessionDBWriteProof
        or fence.writer_proof.session_id != getattr(agent, "session_id", None)
        or fence.writer_proof.store_path != _store_path(agent)
        or not _managed_intermediate_fence_aliases_match(agent, parent, receipt, fence)
        or not _managed_physical_tip_valid(
            agent, binding, _physical_tip_value(fence.physical_witness),
        )
        or not _managed_intermediate_prefix_identity_matches(agent, client, fence)
        or not _managed_intermediate_witness_shape(fence)
        or not _managed_writer_proof_readback(agent, fence.writer_proof)
    ):
        return _GATEWAY_WRITER_UNKNOWN
    return fence


def retain_managed_intermediate_writer_fence(
    agent: Any, client: Any, proof: Any, physical_witness: Any,
) -> bool:
    """Retain one prefix or its strict same-receipt extension without replacing its token."""
    if type(proof) is not SessionDBWriteProof or physical_witness is None:
        return False
    context = _managed_intermediate_context(agent, client)
    if context is None:
        return False
    parent, service, request, receipt, binding, lease_id, owner_identity, binding_identity = context
    validator = getattr(client, "validate_managed_durable_receipt_witness", None)
    if (
        not callable(validator)
        or getattr(physical_witness, "writer_proof", None) is not proof
        or validator(agent, proof, physical_witness) is not True
        or _managed_writer_result_ids(physical_witness) is None
        or not _managed_writer_proof_readback(agent, proof)
    ):
        return False
    previous = getattr(agent, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None)
    if previous is not None:
        current = managed_intermediate_writer_fence(agent, client)
        if current is _GATEWAY_WRITER_UNKNOWN or current is None:
            return False
        if not _managed_writer_prefix_extends(
            agent, current.writer_proof, current.physical_witness, proof, physical_witness,
        ):
            return False
        identity_seal = current.identity_seal
    else:
        # A stale parent/receipt alias is an ownership mismatch, even when the agent
        # attribute was cleared.  Never overwrite an alias from a prior token.
        if (
            getattr(parent, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None) is not None
            or getattr(receipt, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None) is not None
            or getattr(parent, _GATEWAY_WRITER_INTERMEDIATE_SEAL, None) is not None
            or getattr(receipt, _GATEWAY_WRITER_INTERMEDIATE_SEAL, None) is not None
        ):
            return False
        identity_seal = object()
    turn_id = str(getattr(agent, "_current_turn_id", "") or "")
    if not turn_id or proof.session_id != getattr(agent, "session_id", None) or proof.store_path != _store_path(agent):
        return False
    new_fence = ManagedIntermediateWriterFence(
        writer_proof=proof, physical_witness=physical_witness,
        agent=agent, client=client, parent=parent, service=service,
        request=request, receipt=receipt, binding=binding, turn_id=turn_id,
        lease_id=lease_id, owner_identity=owner_identity, binding_identity=binding_identity,
        identity_seal=identity_seal,
    )
    if not _set_managed_intermediate_fence_aliases(agent, parent, receipt, new_fence):
        _clear_managed_intermediate_fence_aliases(agent, parent, receipt, new_fence)
        return False
    try:
        setattr(receipt, _GATEWAY_WRITER_INTERMEDIATE_PREFIX_HANDOFF, new_fence)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        _clear_managed_intermediate_fence_aliases(agent, parent, receipt, new_fence)
        return False
    return True


def mark_managed_intermediate_writer_fence_unknown(
    agent: Any, fallback_acp: Any = None, reason: str = "managed_intermediate_unknown",
    *, mark_parent: bool = False,
) -> bool:
    """Retain the exact fence and allow only an authenticated same-owner recovery."""
    fence = getattr(agent, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None)
    if type(fence) is not ManagedIntermediateWriterFence:
        return False
    if not _managed_intermediate_fence_aliases_match(agent, fence.parent, fence.receipt, fence):
        return False
    unknown_fence = replace(fence, state="unknown", reason=reason)
    if not _set_managed_intermediate_fence_aliases(agent, fence.parent, fence.receipt, unknown_fence):
        return False
    try:
        setattr(fence.receipt, _GATEWAY_WRITER_INTERMEDIATE_PREFIX_HANDOFF, unknown_fence)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False
    if mark_parent:
        mark_gateway_writer_proof_unknown(agent, fallback_acp)
    return True


def _reconcile_managed_intermediate_child(agent: Any, client: Any, fence: ManagedIntermediateWriterFence) -> bool:
    """Validate the original prefix and failed queue before its native child reset."""
    recovery_context = _managed_intermediate_recovery_context(agent, client, fence)
    if recovery_context is None or not _managed_intermediate_prefix_identity_matches(
        agent, client, fence,
    ):
        return False
    _parent, service, request, receipt, binding = recovery_context
    failed_process = getattr(receipt, "process", None)
    if getattr(service, "unknown", False) or getattr(receipt, "state", None) == "unknown":
        queue_exact = getattr(client, "_managed_failure_queue_is_exact", None)
        if not callable(queue_exact) or queue_exact(receipt, failed_process) is not True:
            return False
        retry_child = getattr(service, "reconcile_managed_child_failure", None)
        try:
            retried = callable(retry_child) and retry_child(binding, request) is True
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            retried = False
        if not retried:
            return False
        # The existing same-owner retry must retain the exact prefix object and all
        # durable/readback fields.  A copied witness or replacement lease is refused.
        if not _managed_intermediate_prefix_handoff_matches(agent, client, fence):
            return False
        retire_failed = getattr(client, "_retire_managed_failure_witness_after_retry", None)
        if not callable(retire_failed) or retire_failed(receipt, failed_process) is not True:
            mark_managed_intermediate_writer_fence_unknown(
                agent, client, "managed_failure_queue_residual", mark_parent=True,
            )
            return False
    return True


def recover_managed_intermediate_writer_fence(agent: Any, client: Any) -> bool:
    """Reopen only an exact unknown fence after same-owner proof/readback validation."""
    fence = getattr(agent, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None)
    if type(fence) is not ManagedIntermediateWriterFence or fence.state != "unknown":
        return False
    current = managed_intermediate_writer_fence(agent, client)
    if current is _GATEWAY_WRITER_UNKNOWN or current is None:
        # A native close/readback failure can mark the exact coordinator unknown before the
        # existing same-owner retry has reconciled it.  Authenticate the retained object
        # tuple and the receipt-local prefix token before attempting that retry.
        if not _reconcile_managed_intermediate_child(agent, client, fence):
            return False
        current = managed_intermediate_writer_fence(agent, client)
        if current is _GATEWAY_WRITER_UNKNOWN or current is None:
            return False
    if not _managed_intermediate_prefix_identity_matches(agent, client, current):
        return False
    validator = getattr(client, "validate_managed_durable_receipt_witness", None)
    prefix_only_recovery = (
        current.state == "unknown"
        and getattr(current.receipt, "managed_witness", None) is None
        and _managed_intermediate_prefix_handoff_matches(agent, client, current)
    )
    if prefix_only_recovery:
        reopened = replace(current, state="pending", reason="")
        if not _set_managed_intermediate_fence_aliases(
            agent, current.parent, current.receipt, reopened,
        ):
            return False
        try:
            setattr(current.receipt, _GATEWAY_WRITER_INTERMEDIATE_PREFIX_HANDOFF, reopened)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return False
        return True
    if callable(validator) and validator(agent, current.writer_proof, current.physical_witness) is True:
        reopened = replace(current, state="pending", reason="")
        if not _set_managed_intermediate_fence_aliases(
            agent, current.parent, current.receipt, reopened,
        ):
            return False
        try:
            setattr(current.receipt, _GATEWAY_WRITER_INTERMEDIATE_PREFIX_HANDOFF, reopened)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return False
        return True
    # A terminal record may already have replaced the receipt's durable proof before the
    # first witness readback failed.  Rebind only that exact strict extension.
    receipt = current.receipt
    managed = getattr(receipt, "managed_witness", None)
    durable = managed.get("durable_proof") if isinstance(managed, dict) else None
    if durable is None:
        request_id = getattr(getattr(current.request, "lease", None), "lease_id", None)
        durable_map = getattr(receipt, "durable_proofs", None)
        if isinstance(durable_map, dict) and type(request_id) is str:
            durable = durable_map.get(request_id)
    capture = getattr(client, "capture_managed_durable_receipt_witness", None)
    if durable is None or not callable(capture) or not callable(validator):
        return False
    next_proof = SessionDBWriteProof(
        store_path=getattr(durable, "store_path", None), session_id=getattr(durable, "session_id", None),
        row_ids=getattr(durable, "row_ids", ()), row_snapshots=getattr(durable, "row_snapshots", ()),
        row_sessions=getattr(durable, "row_sessions", ()), row_roles=getattr(durable, "row_roles", ()),
        payload_digest=getattr(durable, "payload_digest", ""),
    )
    next_witness = capture(agent, next_proof)
    if (
        next_witness is None
        or validator(agent, next_proof, next_witness) is not True
        or not _managed_writer_prefix_extends(
            agent, current.writer_proof, current.physical_witness, next_proof, next_witness,
        )
    ):
        return False
    reopened = replace(
        current, writer_proof=next_proof, physical_witness=next_witness,
        state="pending", reason="",
    )
    if not _set_managed_intermediate_fence_aliases(
        agent, current.parent, current.receipt, reopened,
    ):
        return False
    try:
        setattr(current.receipt, _GATEWAY_WRITER_INTERMEDIATE_PREFIX_HANDOFF, reopened)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False
    return True


def consume_managed_intermediate_writer_fence(
    agent: Any, client: Any, proof: Any, physical_witness: Any, messages: Any = None,
) -> bool | None:
    """Consume only after a strict same-owner final row/result extension."""
    fence = getattr(agent, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None)
    if fence is None:
        return None
    current = managed_intermediate_writer_fence(agent, client)
    if current is _GATEWAY_WRITER_UNKNOWN or current is None or current.state != "pending":
        return False
    if not _managed_intermediate_prefix_identity_matches(agent, client, current):
        return False
    if type(proof) is not SessionDBWriteProof:
        return False
    validator = getattr(client, "validate_managed_durable_receipt_witness", None)
    final_message_reader = getattr(client, "_managed_final_message", None)
    result_rows_reader = getattr(client, "_managed_result_rows", None)
    if (
        type(messages) is not list
        or not callable(final_message_reader)
        or not callable(result_rows_reader)
        or not _gateway_proof_rows_match(agent, messages, proof, getattr(proof, "session_id", None))
    ):
        return False
    final_message = final_message_reader(messages)
    result_rows = result_rows_reader(proof, messages)
    durable_result_rows = _managed_writer_result_ids(physical_witness)
    if (
        not result_rows
        or final_message is None
        or type(final_message.get("_row_id")) is not int
        or final_message.get("_row_id") not in result_rows
        or durable_result_rows != tuple(result_rows)
    ):
        return False
    same_terminal = (
        proof is current.writer_proof
        and getattr(physical_witness, "writer_proof", None) is proof
        and getattr(physical_witness, "receipt", None) is current.receipt
    )
    prefix_handoff = _managed_intermediate_prefix_handoff_matches(agent, client, current)
    if (
        not callable(validator)
        or (
            validator(agent, proof, physical_witness) is not True
            and not (prefix_handoff and physical_witness is current.physical_witness)
        )
        or (
            not same_terminal
            and not _managed_writer_prefix_extends(
                agent, current.writer_proof, current.physical_witness, proof, physical_witness,
            )
        )
        or getattr(physical_witness, "client", None) is not client
        or getattr(physical_witness, "parent", None) is not current.parent
        or getattr(physical_witness, "service", None) is not current.service
        or getattr(physical_witness, "request", None) is not current.request
        or getattr(physical_witness, "binding", None) is not current.binding
        or getattr(physical_witness, "receipt", None) is not current.receipt
        or getattr(physical_witness, "binding_identity", None) != current.binding_identity
        or getattr(physical_witness, "binding_snapshot_identity", None)
        != getattr(current.physical_witness, "binding_snapshot_identity", None)
    ):
        return False
    try:
        if getattr(current.receipt, _GATEWAY_WRITER_INTERMEDIATE_PREFIX_HANDOFF, None) is not current:
            return False
        delattr(current.receipt, _GATEWAY_WRITER_INTERMEDIATE_PREFIX_HANDOFF)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False
    _clear_managed_intermediate_fence_aliases(
        agent, current.parent, current.receipt, current,
    )
    # A prior terminal readback failure may have left the generic marker set.  The
    # same-owner fence has now consumed that exact receipt, so it is safe to retire only
    # that marker; no unrelated writer proof is reintroduced.
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_UNKNOWN, None)
    return True


def request_gateway_writer_proof(agent: Any, binding: Any = _UNSET) -> bool:
    """Arm one Gateway turn for the existing typed SessionDB writer proof.

    A no-argument request is the existing managed I04 route and resolves its binding from the
    active service context at publication time.  An explicit binding is retained by identity;
    an explicit ``None`` is a refusal and must never borrow that ambient context.
    """
    if getattr(agent, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None) is not None:
        # The same physical receipt owns the turn until its typed prefix is consumed.
        return False
    if getattr(agent, _GATEWAY_WRITER_PROOF_UNKNOWN, False) is True:
        # Only the existing authenticated physical/finalizer retry path may clear this marker;
        # a writer request cannot turn an unknown parent into a fresh admission.
        return False
    elif (
        getattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_REQUIRED, False) is True
        and getattr(agent, "_managed_shell_writer_proof", None) is not None
    ):
        # A second request must not erase a retained managed receipt and open a fresh writer.
        return False
    else:
        discard_gateway_writer_proof(agent)
    setattr(agent, _GATEWAY_WRITER_PROOF_REQUEST, True)
    if binding is not _UNSET:
        setattr(agent, _GATEWAY_WRITER_PROOF_BINDING, binding)
    return True


def gateway_writer_proof_requested(agent: Any) -> bool:
    return getattr(agent, _GATEWAY_WRITER_PROOF_REQUEST, False) is True


def discard_gateway_writer_proof(agent: Any) -> None:
    """Drop an unconsumed transient handoff and its one-turn request."""
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_HANDOFF, None)
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_REQUEST, None)
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_BINDING, None)
    if getattr(agent, _GATEWAY_WRITER_PROOF_UNKNOWN, False) is True:
        # Finalizer cleanup may retire only the transient request.  The original proof and
        # physical token remain reachable until the authenticated same-owner retry succeeds.
        return
    forget_gateway_writer_proof(agent)


def forget_gateway_writer_proof(agent: Any) -> None:
    """Drop the retained proof without changing the active request token."""
    agent.__dict__.pop("_managed_shell_writer_proof", None)
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_TURN_ID, None)
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_BOUND_BINDING, None)
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, None)
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_PHYSICAL_REQUIRED, None)
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_UNKNOWN, None)


def mark_gateway_writer_proof_unknown(agent: Any, fallback_acp: Any = None) -> bool:
    """Mark the original ACP parent unknown while retaining its opaque retry token."""
    setattr(agent, _GATEWAY_WRITER_PROOF_UNKNOWN, True)
    witness = getattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, None)
    owner = getattr(witness, "client", None)
    marker = getattr(owner, "mark_managed_durable_receipt_unknown", None)
    if callable(marker):
        try:
            if marker(witness) is True:
                return True
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            pass
    if fallback_acp is not None:
        try:
            fallback = getattr(fallback_acp, "_producer_policy", None)
            parent = getattr(fallback, "parent", None)
            if parent is not None:
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, "managed_durable_witness_invalid")
                return True
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            pass
    return False


def retain_gateway_writer_proof_unknown(
    agent: Any, proof: Any, physical_witness: Any = None,
    requested: Any = _UNSET, explicit: bool = False,
) -> bool:
    """Retain a post-effect proof and its opaque witness while the owner is unknown.

    A failed durable handoff may occur after the canonical writer has committed.  The
    failed path must keep that exact proof/token reachable for the existing authenticated
    owner retry; dropping it would permit a later call to create a second writer.
    """
    if type(proof) is not SessionDBWriteProof:
        return False
    agent._managed_shell_writer_proof = proof
    if physical_witness is not None:
        setattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, physical_witness)
        setattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_REQUIRED, True)
    turn_id = str(getattr(agent, "_current_turn_id", "") or "")
    if turn_id:
        setattr(agent, _GATEWAY_WRITER_PROOF_TURN_ID, turn_id)
    binding = _active_gateway_binding(requested, explicit)
    if binding is not None:
        setattr(agent, _GATEWAY_WRITER_PROOF_BOUND_BINDING, binding)
    setattr(agent, _GATEWAY_WRITER_PROOF_UNKNOWN, True)
    return True


def prepare_gateway_writer_proof_recovery(
    agent: Any, client: Any, parent: Any, service: Any, binding: Any, request: Any,
) -> tuple[SessionDBWriteProof, Any, str] | bool | None:
    """Authenticate the retained writer proof before a typed managed-child retry."""
    if getattr(agent, _GATEWAY_WRITER_PROOF_UNKNOWN, False) is not True:
        return None
    proof = getattr(agent, "_managed_shell_writer_proof", None)
    receipt = getattr(parent, "_physical", None)
    turn_id = str(getattr(agent, "_current_turn_id", "") or "")
    if (
        type(proof) is not SessionDBWriteProof
        or not turn_id
        or proof.result_row_ids
        or getattr(parent, "_owner_agent", None) is not agent
        or getattr(agent, "_managed_acp_client", None) is not client
        or getattr(parent, "_service", None) is not service
        or getattr(parent, "turn_binding", None) is not binding
        or getattr(parent, "_physical", None) is not receipt
        or getattr(service, "physical", None) is not receipt
        or getattr(receipt, "service", None) is not service
        or getattr(receipt, "managed_client", None) is not client
        or not isinstance(getattr(receipt, "managed_witness", None), dict)
        or getattr(service, "requests", {}).get(getattr(binding, "admission_id", None)) is not request
        or getattr(agent, "session_id", None) != proof.session_id
        or proof.store_path != _store_path(agent)
        or getattr(agent, _GATEWAY_WRITER_PROOF_BOUND_BINDING, None) is not binding
        or getattr(receipt, "durable_proofs", None)
        or getattr(receipt, "delivery_requests", None)
        or getattr(receipt, "completed_requests", None)
        or getattr(receipt, "external_publication_proof", None) is not None
    ):
        return False
    if not _managed_physical_tip_valid(
        agent, binding, _physical_tip_value(
            getattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, None),
        ),
    ):
        return False
    expected = tuple(zip(proof.row_ids, proof.row_roles, proof.row_snapshots))
    if (
        not expected
        or len(expected) != len(proof.row_ids)
        or proof.row_sessions != tuple(proof.session_id for _row_id, _role, _snapshot in expected)
        or _read_expected_rows(agent, proof.session_id, expected) != expected
    ):
        return False
    pending = getattr(client, "_pending_managed_witnesses", ())
    if not any(
        item.get("parent") is parent
        and item.get("receipt") is receipt
        and item.get("_process") is getattr(receipt, "process", None)
        and item.get("_failure_reason")
        for item in pending
        if isinstance(item, dict)
    ):
        return False
    return proof, receipt, turn_id


def complete_gateway_writer_proof_recovery(
    agent: Any, client: Any, parent: Any, service: Any, binding: Any, request: Any,
    recovery: tuple[SessionDBWriteProof, Any, str],
) -> bool:
    """Retire a stale writer proof only after the exact same receipt was reset."""
    proof, receipt, turn_id = recovery
    if (
        getattr(agent, _GATEWAY_WRITER_PROOF_UNKNOWN, False) is not True
        or getattr(agent, "_managed_shell_writer_proof", None) is not proof
        or getattr(agent, "_current_turn_id", None) != turn_id
        or getattr(parent, "_owner_agent", None) is not agent
        or getattr(agent, "_managed_acp_client", None) is not client
        or getattr(parent, "_service", None) is not service
        or getattr(parent, "turn_binding", None) is not binding
        or getattr(parent, "_physical", None) is not receipt
        or getattr(service, "physical", None) is not receipt
        or getattr(service, "requests", {}).get(getattr(binding, "admission_id", None)) is not request
        or getattr(service, "unknown", True) is not False
        or getattr(parent, "state", None) != "request_reserved"
        or getattr(receipt, "state", None) != "reserved"
        or getattr(receipt, "retry_ready", False) is not True
        or getattr(receipt, "process", None) is not None
        or getattr(receipt, "transaction", None) is not None
        or getattr(receipt, "managed_witness", None) is not None
        or getattr(receipt, "managed_client", None) is not None
        or getattr(receipt, "durable_proofs", None)
        or getattr(receipt, "delivery_requests", None)
        or getattr(receipt, "completed_requests", None)
    ):
        return False
    forget_gateway_writer_proof(agent)
    agent._managed_persistence_failed = False
    return True


def prepare_gateway_writer_postcommit_recovery(
    agent: Any, client: Any, parent: Any, service: Any, binding: Any, request: Any,
    messages: Any,
) -> tuple[SessionDBWriteProof, Any, str, Any] | None:
    """Revalidate one retained post-commit proof before same-owner reuse.

    This path is deliberately separate from the pre-effect process retry above.  The
    writer has already committed both the SessionDB rows and the receipt-local durable
    result, so recovery must preserve those exact objects and only repair a failed
    witness read/capture.  It never resets the service or admits a new child.
    """
    if getattr(agent, _GATEWAY_WRITER_PROOF_UNKNOWN, False) is not True:
        return None
    proof = getattr(agent, "_managed_shell_writer_proof", None)
    receipt = getattr(parent, "_physical", None)
    turn_id = str(getattr(agent, "_current_turn_id", "") or "")
    if (
        type(proof) is not SessionDBWriteProof
        or not turn_id
        or proof.result_row_ids
        or getattr(parent, "mode", None) != "managed_turn"
        or getattr(parent, "_owner_agent", None) is not agent
        or getattr(agent, "_managed_acp_client", None) is not client
        or getattr(parent, "_service", None) is not service
        or getattr(parent, "turn_binding", None) is not binding
        or getattr(parent, "_physical", None) is not receipt
        or getattr(service, "physical", None) is not receipt
        or getattr(receipt, "service", None) is not service
        or getattr(service, "unknown", True) is not False
        or getattr(parent, "state", None) in {"unknown", "terminal"}
        or getattr(receipt, "state", None) in {"unknown", "closed", "released"}
        or getattr(receipt, "managed_client", None) is not client
        or getattr(parent, "_catalog_client", None) is not client
        or getattr(parent, "_request", None) is not request
        or (getattr(service, "requests", {}) or {}).get(getattr(binding, "admission_id", None)) is not request
        or getattr(agent, "session_id", None) != proof.session_id
        or proof.store_path != _store_path(agent)
        or getattr(agent, _GATEWAY_WRITER_PROOF_BOUND_BINDING, None) is not binding
    ):
        return None
    retained_witness = getattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, None)
    # A failed first capture has no immutable provider/physical identity to compare on a
    # later read.  Keep that case unknown rather than treating a fresh mutable receipt view
    # as proof of the original child.
    if retained_witness is None:
        return None
    lease_id = getattr(getattr(request, "lease", None), "lease_id", None)
    durable_proofs = getattr(receipt, "durable_proofs", None)
    durable = durable_proofs.get(lease_id) if isinstance(durable_proofs, dict) else None
    if (
        type(lease_id) is not str or not lease_id
        or not isinstance(durable_proofs, dict)
        or set(durable_proofs) != {lease_id}
        or durable is not getattr(receipt, "durable_proof", None)
        or getattr(service, "durable_requests", None) != {getattr(binding, "admission_id", None)}
        or getattr(receipt, "delivery_requests", None)
        or getattr(receipt, "completed_requests", None)
        or getattr(receipt, "external_publication_proof", None) is not None
    ):
        return None
    if not _gateway_proof_rows_match(agent, messages, proof, proof.session_id):
        return None
    capture = getattr(client, "capture_managed_durable_receipt_witness", None)
    if _is_direct_physical_tip_witness(retained_witness):
        if not _gateway_writer_physical_witness_valid(
            agent, binding, proof, retained_witness,
        ):
            return None
        physical_witness = retained_witness
    else:
        if not callable(capture):
            return None
        try:
            physical_witness = capture(agent, proof)
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            return None
        if (
            not _is_managed_receipt_witness(physical_witness)
            or getattr(physical_witness, "writer_proof", None) is not proof
            or not _gateway_writer_physical_witness_valid(
                agent, binding, proof, physical_witness,
            )
        ):
            return None
    try:
        if any(
            getattr(physical_witness, field, None) is not getattr(retained_witness, field, None)
            for field in (
                "client", "parent", "service", "request", "binding", "receipt", "process",
                "binding_snapshot", "writer_proof", "durable_proof", "managed_witness",
            )
        ):
            return None
        if any(
            getattr(physical_witness, field, None) != getattr(retained_witness, field, None)
            for field in (
                "provider_identity", "binding_snapshot_identity", "durable_identity", "binding_identity",
                "physical_tip_witness",
            )
        ):
            return None
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        return None
    return proof, receipt, turn_id, physical_witness


def complete_gateway_writer_postcommit_recovery(
    agent: Any, client: Any, parent: Any, service: Any, binding: Any, request: Any,
    recovery: tuple[SessionDBWriteProof, Any, str, Any], messages: Any,
) -> bool:
    """Clear only the sticky writer marker after exact post-commit revalidation."""
    proof, receipt, turn_id, physical_witness = recovery
    prepared = prepare_gateway_writer_postcommit_recovery(
        agent, client, parent, service, binding, request, messages,
    )
    if (
        prepared is None
        or prepared[0] is not proof
        or prepared[1] is not receipt
        or prepared[2] != turn_id
    ):
        return False
    physical_witness = prepared[3]
    setattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, physical_witness)
    setattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_REQUIRED, True)
    setattr(agent, _GATEWAY_WRITER_PROOF_REQUEST, True)
    setattr(agent, _GATEWAY_WRITER_PROOF_BOUND_BINDING, binding)
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_UNKNOWN, None)
    agent._managed_persistence_failed = False
    return True


def _clear_gateway_writer_proof_request(agent: Any) -> None:
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_REQUEST, None)
    agent.__dict__.pop(_GATEWAY_WRITER_PROOF_BINDING, None)


def _requested_gateway_binding(agent: Any) -> tuple[Any, bool]:
    """Return (binding, explicit) without consulting an ambient managed context."""
    if _GATEWAY_WRITER_PROOF_BINDING in agent.__dict__:
        return agent.__dict__[_GATEWAY_WRITER_PROOF_BINDING], True
    return _UNSET, False


def _active_gateway_binding(requested: Any, explicit: bool) -> Any:
    """Resolve the binding used for a proof without inventing an ambient owner."""
    if explicit:
        return requested
    try:
        from gateway.runtime_service_custody import current_turn_binding
        return current_turn_binding()
    except (ImportError, AttributeError, RuntimeError, TypeError, ValueError):
        return None


def remember_gateway_writer_proof(
    agent: Any, proof: Any, requested: Any, explicit: bool, physical_witness: Any = None,
) -> bool:
    """Retain a fresh proof only with the exact turn and binding that produced it."""
    turn_id = str(getattr(agent, "_current_turn_id", "") or "")
    binding = _active_gateway_binding(requested, explicit)
    session_id = getattr(agent, "session_id", None)
    if (
        type(proof) is not SessionDBWriteProof
        or not turn_id
        or not isinstance(session_id, str)
        or not session_id
        or proof.session_id != session_id
        or binding is None
    ):
        agent.__dict__.pop(_GATEWAY_WRITER_PROOF_TURN_ID, None)
        agent.__dict__.pop(_GATEWAY_WRITER_PROOF_BOUND_BINDING, None)
        agent.__dict__.pop(_GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, None)
        agent.__dict__.pop(_GATEWAY_WRITER_PROOF_PHYSICAL_REQUIRED, None)
        return False
    try:
        from gateway.runtime_service_custody import TurnCustodyBinding
    except (AttributeError, ImportError):
        TurnCustodyBinding = None
    if TurnCustodyBinding is not None and isinstance(binding, TurnCustodyBinding):
        try:
            from gateway.session_turn_physical_tip import resolve_physical_tip_witness
            current_witness = resolve_physical_tip_witness(binding, session_id, _store_path(agent))
        except (
            AttributeError, ImportError, KeyError, OSError, RuntimeError, TypeError, ValueError,
            sqlite3.Error,
        ):
            current_witness = None
        supplied_witness = _physical_tip_value(physical_witness)
        if current_witness is None or (
            physical_witness is not None and supplied_witness != current_witness
        ):
            agent.__dict__.pop(_GATEWAY_WRITER_PROOF_TURN_ID, None)
            agent.__dict__.pop(_GATEWAY_WRITER_PROOF_BOUND_BINDING, None)
            agent.__dict__.pop(_GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, None)
            agent.__dict__.pop(_GATEWAY_WRITER_PROOF_PHYSICAL_REQUIRED, None)
            return False
    setattr(agent, _GATEWAY_WRITER_PROOF_TURN_ID, turn_id)
    setattr(agent, _GATEWAY_WRITER_PROOF_BOUND_BINDING, binding)
    if explicit:
        # Preserve the exact binding for the later Gateway handoff even when the
        # finalizer runs outside the original ContextVar scope.
        setattr(agent, _GATEWAY_WRITER_PROOF_BINDING, binding)
    if TurnCustodyBinding is not None and isinstance(binding, TurnCustodyBinding):
        # Direct writers have no ACP receipt wrapper, so retain the raw canonical witness.
        # Managed ACP writers retain their receipt witness, whose physical-tip member was
        # checked against this same immutable value above.
        setattr(
            agent, _GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS,
            physical_witness if physical_witness is not None else current_witness,
        )
        setattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_REQUIRED, True)
    elif physical_witness is None:
        agent.__dict__.pop(_GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, None)
        agent.__dict__.pop(_GATEWAY_WRITER_PROOF_PHYSICAL_REQUIRED, None)
    else:
        setattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, physical_witness)
        setattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_REQUIRED, True)
    return True

def gateway_writer_proof_extends_prefix(previous: Any, proof: Any) -> bool:
    """Require a repaired writer proof to retain the prefix and add a terminal result suffix."""
    if type(previous) is not SessionDBWriteProof or type(proof) is not SessionDBWriteProof:
        return False
    prefix_len = len(previous.row_ids)
    suffix_roles = proof.row_roles[prefix_len:]
    return (
        prefix_len > 0
        and len(proof.row_ids) > prefix_len
        and bool(suffix_roles)
        and previous.row_roles[-1:] == ("user",)
        and all(role in {"assistant", "tool"} for role in suffix_roles)
        and (proof.store_path, proof.session_id) == (previous.store_path, previous.session_id)
        and (proof.row_ids[:prefix_len], proof.row_snapshots[:prefix_len],
             proof.row_sessions[:prefix_len], proof.row_roles[:prefix_len]) == (
                 previous.row_ids, previous.row_snapshots,
                 previous.row_sessions, previous.row_roles)
    )

def _classic_binding_is_current(binding: Any, session_id: str) -> bool:
    """Re-read a direct classic owner record before reusing its prior proof."""
    try:
        from gateway.session_classic_custody import ClassicWriterBinding, classic_writer_binding_valid
    except (ImportError, AttributeError):
        return False
    if not isinstance(binding, ClassicWriterBinding):
        return True
    try:
        return classic_writer_binding_valid(binding, expected_session_id=session_id) is True
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        return False

def _gateway_proof_reuse_context(
    agent: Any, proof: Any, requested: Any, explicit: bool,
) -> tuple[str, Any, str] | None:
    """Resolve the exact active turn/binding context for proof reuse."""
    if (
        type(proof) is not SessionDBWriteProof
        or getattr(agent, "_managed_persistence_failed", False) is True
        or proof.result_row_ids
    ):
        return None
    turn_id = str(getattr(agent, "_current_turn_id", "") or "")
    if not turn_id or getattr(agent, _GATEWAY_WRITER_PROOF_TURN_ID, None) != turn_id:
        return None
    binding = _active_gateway_binding(requested, explicit)
    retained_binding = getattr(agent, _GATEWAY_WRITER_PROOF_BOUND_BINDING, None)
    if binding is None or retained_binding is not binding:
        return None
    session_id = getattr(agent, "session_id", None)
    if (
        not isinstance(session_id, str)
        or not session_id
        or proof.session_id != session_id
        or proof.store_path != _store_path(agent)
        or not _classic_binding_is_current(binding, session_id)
        or not _managed_physical_tip_valid(
            agent, binding,
            getattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, None),
        )
    ):
        return None
    return turn_id, binding, session_id

def _gateway_proof_rows_match(
    agent: Any, messages: Any, proof: SessionDBWriteProof, session_id: str,
) -> bool:
    """Confirm unchanged current-turn rows and exact post-commit proof readback."""
    if _intended_messages(messages):
        return False
    expected = tuple(zip(proof.row_ids, proof.row_roles, proof.row_snapshots))
    if (
        len(expected) != len(proof.row_ids)
        or proof.row_sessions != tuple(session_id for _row_id, _role, _snapshot in expected)
        or any(type(row_id) is not int or row_id <= 0 for row_id, _role, _snapshot in expected)
        or any(type(role) is not str or type(snapshot) is not str or not snapshot
               for _row_id, role, snapshot in expected)
    ):
        return False
    try:
        checked = _read_expected_rows(agent, session_id, expected)
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return False
    if checked is None:
        return False
    validated = _proof_from_validated_rows(proof.store_path, session_id, checked)
    return validated == proof

def _gateway_writer_prefix_matches(
    agent: Any, messages: Any, proof: SessionDBWriteProof, binding: Any,
) -> bool:
    """Recognize the same I04 writer's staged prefix before its terminal result rows.

    The first user row may be flushed before the model answer exists.  That prefix is still
    owned by this exact managed turn; the terminal flush must extend it with the new assistant
    rows instead of classifying the expected suffix as a second writer or as UNKNOWN.
    """
    try:
        from gateway.runtime_service_custody import TurnCustodyBinding
    except (AttributeError, ImportError):
        return False
    if not isinstance(binding, TurnCustodyBinding):
        return False
    prefix = _managed_current_turn_prefix(agent, messages)
    if prefix is None:
        return False
    expected = tuple(_persisted_row_spec(message) for message in prefix)
    if any(row is None for row in expected):
        return False
    typed = tuple(row for row in expected if row is not None)
    if (
        proof.store_path != _store_path(agent)
        or proof.session_id != getattr(agent, "session_id", None)
        or proof.row_ids != tuple(row_id for row_id, _role, _snapshot in typed)
        or proof.row_roles != tuple(role for _row_id, role, _snapshot in typed)
        or proof.row_snapshots != tuple(snapshot for _row_id, _role, snapshot in typed)
        or proof.row_sessions != tuple(proof.session_id for _row_id, _role, _snapshot in typed)
        or not typed
    ):
        return False
    try:
        return _read_expected_rows(agent, proof.session_id, typed) == typed
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return False


# health: allow CC -- fail-closed validation keeps carrier identity, witness shape, exact readback, and full-content reconstruction in one security boundary.
def _gateway_writer_merged_prefix_matches(
    agent: Any, messages: Any, proof: SessionDBWriteProof, binding: Any,
) -> bool:
    """Recognize a current user row folded by the canonical alternation-repair pass.

    A detached delivery is a durable user row.  When the next client prompt is loaded, the
    repair belt may merge that delivery row with the newly staged user row.  The repair removes
    the live persisted marker and carries the staged row's physical id in its existing merge
    witness.  Continue only when that witness, the exact active DB rows, and their durable UIDs
    all identify the retained proof; a copied or marker-less message cannot take this path.
    """
    try:
        from agent.context_compressor import _DB_PERSISTED_MARKER
        from agent.conversation_compression_archive import ABSORBED_ROW_IDS, MERGED_DURABLE_ROWS
        from agent.message_metadata import ABSORBED_MESSAGE_UIDS, MERGED_TURN_PREFIX, MESSAGE_UID
        from gateway.runtime_service_custody import TurnCustodyBinding
    except (AttributeError, ImportError):
        return False
    if not isinstance(binding, TurnCustodyBinding):
        return False
    session_id = getattr(agent, "session_id", None)
    if (
        type(proof) is not SessionDBWriteProof
        or proof.store_path != _store_path(agent)
        or not isinstance(session_id, str)
        or not session_id
        or proof.session_id != session_id
        or not proof.row_ids
        or any(role != "user" for role in proof.row_roles)
        or len(proof.row_ids) != len(proof.row_roles)
        or len(proof.row_ids) != len(proof.row_snapshots)
        or proof.row_sessions != tuple(session_id for _row_id in proof.row_ids)
        or proof.result_row_ids
    ):
        return False
    anchor_index = _latest_user_index(messages) if isinstance(messages, list) else None
    if anchor_index is None:
        return False
    anchor = messages[anchor_index]
    if (
        not isinstance(anchor, dict)
        or anchor.get("role") != "user"
        or anchor.get(_DB_PERSISTED_MARKER) is True
        or type(anchor.get(MERGED_DURABLE_ROWS)) is not int
        or anchor.get(MERGED_DURABLE_ROWS) != len(proof.row_ids) + 1
        or type(anchor.get(MERGED_TURN_PREFIX)) is not str
        or (
            anchor.get(MESSAGE_UID) is not None
            and (type(anchor.get(MESSAGE_UID)) is not str or not anchor.get(MESSAGE_UID))
        )
    ):
        return False
    absorbed_ids = anchor.get(ABSORBED_ROW_IDS)
    if (
        type(absorbed_ids) is not list
        or any(type(row_id) is not int or row_id <= 0 for row_id in absorbed_ids)
        or tuple(absorbed_ids) != tuple(proof.row_ids)
    ):
        return False
    absorbed_uids = anchor.get(ABSORBED_MESSAGE_UIDS)
    if (
        type(absorbed_uids) is not list
        or len(absorbed_uids) != len(proof.row_ids)
        or any(type(uid) is not str or not uid for uid in absorbed_uids)
    ):
        return False
    tail = messages[anchor_index:]
    if (
        len(tail) < 2
        or any(not isinstance(message, dict) for message in tail)
        or any(message.get(_DB_PERSISTED_MARKER) is True for message in tail)
        or any(message.get("role") not in {"user", "assistant", "tool"} for message in tail)
        or any(message.get("role") == "user" for message in tail[1:])
    ):
        return False
    intended = _intended_messages(messages)
    if not intended or intended[0] is not anchor or any(
        id(message) in {id(item) for item in intended} for message in messages[:anchor_index]
    ):
        return False
    anchor_row_id = anchor.get("_row_id")
    if type(anchor_row_id) is not int or anchor_row_id <= 0:
        anchor_row_id = None
        try:
            if anchor.get(MESSAGE_UID):
                candidates = agent._session_db._read_all(
                    "SELECT * FROM messages WHERE session_id = ? AND active = 1 "
                    "AND role = 'user' AND message_uid = ? ORDER BY id DESC",
                    (session_id, anchor[MESSAGE_UID]),
                )
            else:
                candidates = [
                    row for row in agent._session_db._read_all(
                        "SELECT * FROM messages WHERE session_id = ? AND active = 1 "
                        "AND role = 'user' ORDER BY id DESC", (session_id,)
                    )
                    if row["id"] not in proof.row_ids
                    and agent._session_db._decode_content(row["content"]) == anchor[MERGED_TURN_PREFIX]
                ]
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            return False
        if not anchor.get(MESSAGE_UID) and len(candidates) == 0:
            # An API caller may supply a durable-looking history snapshot that is intentionally
            # external to this SessionDB.  The retained proof still authenticates the current
            # user row; the snapshot prefix must stay out of the child transcript.
            candidates = []
        elif len(candidates) != 1:
            return False
        if candidates:
            anchor_row_id = candidates[0]["id"]
    if (
        anchor_row_id is not None
        and (type(anchor_row_id) is not int or anchor_row_id <= 0 or anchor_row_id in proof.row_ids)
    ):
        return False
    expected = tuple(zip(proof.row_ids, proof.row_roles, proof.row_snapshots))
    try:
        stored_ids = (*proof.row_ids,) if anchor_row_id is None else (anchor_row_id, *proof.row_ids)
        stored = _read_committed_rows(agent._session_db, session_id, stored_ids)
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return False
    if len(stored) != len(expected) + (1 if anchor_row_id is not None else 0):
        return False
    if anchor_row_id is not None:
        anchor_row, *proof_rows = stored
        try:
            anchor_content = agent._session_db._decode_content(anchor_row["content"])
            if (
                anchor_row["id"] != anchor_row_id
                or anchor_row["session_id"] != session_id
                or anchor_row["role"] != "user"
                or anchor_row["active"] != 1
                or (
                    anchor.get(MESSAGE_UID) is not None
                    and anchor_row["message_uid"] != anchor[MESSAGE_UID]
                )
                or anchor_content != anchor[MERGED_TURN_PREFIX]
            ):
                return False
        except (AttributeError, IndexError, KeyError, TypeError, ValueError):
            return False
    else:
        anchor_content = anchor[MERGED_TURN_PREFIX]
        proof_rows = stored
    proof_contents: list[Any] = []
    proof_uids: list[str] = []
    for row, (row_id, role, snapshot) in zip(proof_rows, expected):
        try:
            from agent.transcript_repair import transcript_row_snapshot
            if (
                row["id"] != row_id
                or row["session_id"] != session_id
                or row["role"] != role
                or row["active"] != 1
                or transcript_row_snapshot(row) != snapshot
                or type(row["message_uid"]) is not str
                or row["message_uid"] != absorbed_uids[len(proof_uids)]
            ):
                return False
            proof_contents.append(agent._session_db._decode_content(row["content"]))
            proof_uids.append(row["message_uid"])
        except (AttributeError, IndexError, KeyError, TypeError, ValueError):
            return False
    merged_content = anchor_content
    for content in proof_contents:
        if not isinstance(content, str):
            return False
        merged_content = f"{merged_content}\n\n{content}" if merged_content and content else (merged_content or content)
    return merged_content == anchor.get("content")

def reuse_gateway_writer_proof(
    agent: Any, messages: Any, proof: Any, requested: Any, explicit: bool,
) -> SessionDBWriteProof | None:
    """Validate an idempotent no-new-row continuation of one exact writer proof.

    The retained value is reusable only while the request, current turn, exact binding, message
    suffix, and authoritative SessionDB rows are unchanged.  Empty intended rows alone never
    authorize success.
    """
    if getattr(agent, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None) is not None:
        return _GATEWAY_WRITER_UNKNOWN
    managed_witness_required = getattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_REQUIRED, False) is True
    if getattr(agent, _GATEWAY_WRITER_PROOF_UNKNOWN, False) is True:
        return _GATEWAY_WRITER_UNKNOWN if managed_witness_required else None
    context = _gateway_proof_reuse_context(agent, proof, requested, explicit)
    if context is None:
        return _GATEWAY_WRITER_UNKNOWN if managed_witness_required else None
    turn_id, _binding, session_id = context
    if managed_witness_required:
        physical_witness = getattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, None)
        if not _gateway_writer_physical_witness_valid(
            agent, _binding, proof, physical_witness,
        ):
            return _GATEWAY_WRITER_UNKNOWN
    # Reuse the existing handoff validator for current-turn row identity, exact binding type, and
    # active I04 validation.  Its result is intentionally discarded: publication must still add
    # the result suffix once at the original terminal boundary.
    if _gateway_writer_handoff(agent, messages, proof, turn_id) is None:
        # Managed ACP retains a physical receipt across its staged prefix.  Only its canonical
        # terminal finalizer may extend that proof; direct/classic writers retain the existing
        # prefix-repair route used by the I04 delivery boundary.
        prefix_repair_allowed = (
            getattr(agent, "_managed_acp_client", None) is None
            or getattr(agent, "_managed_finalize_shell_turn", False) is True
        )
        _prefix_match = _gateway_writer_prefix_matches(agent, messages, proof, _binding)
        _merged_match = _gateway_writer_merged_prefix_matches(agent, messages, proof, _binding)
        if prefix_repair_allowed and _prefix_match:
            # The initial user-prefix proof is the same authenticated writer occurrence.  Let
            # the staged writer append the terminal rows and remember the combined readback.
            return _GATEWAY_WRITER_PREFIX_REPAIR
        if prefix_repair_allowed and _merged_match:
            # The canonical alternation repair folded the same authenticated user row into its
            # current delivery carrier.  Let the source writer rewrite that carrier and append
            # the terminal rows under the same binding instead of opening a second writer.
            return _GATEWAY_WRITER_PREFIX_REPAIR
        return _GATEWAY_WRITER_UNKNOWN if managed_witness_required else None
    if _gateway_proof_rows_match(agent, messages, proof, session_id):
        return proof
    return _GATEWAY_WRITER_UNKNOWN if managed_witness_required else None

def _current_turn_row_specs(messages: Any) -> tuple[tuple[int, str, str], ...] | None:
    """Return the exact final user-to-result suffix, without an agent index hint."""
    if type(messages) is not list:
        return None
    anchor = next(
        (
            index for index in range(len(messages) - 1, -1, -1)
            if isinstance(messages[index], dict) and messages[index].get("role") == "user"
        ),
        None,
    )
    if anchor is None:
        return None
    specs: list[tuple[int, str, str]] = []
    for offset, message in enumerate(messages[anchor:]):
        if not isinstance(message, dict):
            return None
        role = message.get("role")
        if (offset == 0 and role != "user") or (offset > 0 and role not in {"assistant", "tool"}):
            return None
        row_id = message.get("_row_id")
        marker = message.get(DB_ROW_SNAPSHOT)
        if type(row_id) is not int or row_id <= 0 or type(marker) is not str or not marker:
            return None
        from agent.context_compressor import _DB_PERSISTED_MARKER
        if message.get(_DB_PERSISTED_MARKER) is not True:
            return None
        specs.append((row_id, role, marker))
    return tuple(specs) if len(specs) >= 2 else None


def _gateway_writer_handoff(
    agent: Any, messages: Any, proof: Any, turn_id: Any,
) -> GatewayWriterProofHandoff | None:
    """Bind one exact writer proof to current-turn rows and the authenticated owner."""
    if not gateway_writer_proof_requested(agent) or type(proof) is not SessionDBWriteProof:
        return None
    if type(turn_id) is not str or not turn_id:
        return None
    if str(getattr(agent, "_current_turn_id", "") or "") != turn_id:
        return None
    session_id = getattr(agent, "session_id", None)
    if type(session_id) is not str or not session_id or proof.session_id != session_id:
        return None
    specs = _current_turn_row_specs(messages)
    if specs is None:
        return None
    row_ids = tuple(row_id for row_id, _role, _marker in specs)
    row_roles = tuple(role for _row_id, role, _marker in specs)
    row_snapshots = tuple(marker for _row_id, _role, marker in specs)
    if (
        proof.row_ids != row_ids
        or proof.row_sessions != tuple(session_id for _ in row_ids)
        or proof.row_roles != row_roles
        or proof.row_snapshots != row_snapshots
        or proof.result_row_ids
    ):
        return None
    requested, explicit = _requested_gateway_binding(agent)
    if explicit:
        # An explicit absence is a typed refusal.  It must never borrow I04 ContextVar state.
        binding = requested
        if binding is None:
            return None
    else:
        try:
            from gateway.runtime_service_custody import current_turn_binding
            binding = current_turn_binding()
        except (ImportError, AttributeError, RuntimeError, TypeError, ValueError):
            return None
        if binding is None:
            return None
    try:
        from gateway.runtime_service_custody import TurnCustodyBinding, validate_turn_binding
        from gateway.session_classic_custody import ClassicWriterBinding
    except (ImportError, AttributeError):
        return None
    if isinstance(binding, TurnCustodyBinding):
        try:
            # Validate the real I04 parent, authoritative admission row, and live execution before
            # the writer proof can be published. Scalar fields are never an ownership proof.
            validate_turn_binding(binding)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            return None
    elif not isinstance(binding, ClassicWriterBinding):
        return None
    physical_tip_witness = None
    if isinstance(binding, TurnCustodyBinding):
        try:
            from gateway.session_turn_physical_tip import resolve_physical_tip_witness
            physical_tip_witness = resolve_physical_tip_witness(
                binding, session_id, _store_path(agent),
            )
        except (
            AttributeError, ImportError, KeyError, OSError, RuntimeError, TypeError, ValueError,
            sqlite3.Error,
        ):
            return None
        retained_tip = _physical_tip_value(
            getattr(agent, _GATEWAY_WRITER_PROOF_PHYSICAL_WITNESS, None),
        )
        if physical_tip_witness is None or retained_tip is None or retained_tip != physical_tip_witness:
            return None
    if (getattr(binding, "session_id", None) != session_id
            and not isinstance(binding, TurnCustodyBinding)):
        return None
    result_row_ids = row_ids[1:]
    if not result_row_ids:
        return None
    bound_proof = replace(proof, result_row_ids=result_row_ids)
    return GatewayWriterProofHandoff(bound_proof, binding, turn_id, physical_tip_witness)


def publish_gateway_writer_proof(
    agent: Any, messages: Any, proof: Any, turn_id: Any,
) -> GatewayWriterProofHandoff | None:
    """Publish one validated proof for Gateway, never as a public result field."""
    if getattr(agent, _GATEWAY_WRITER_INTERMEDIATE_FENCE, None) is not None:
        # Only the terminal same-receipt consumer may cross this fence.
        return None
    handoff = _gateway_writer_handoff(agent, messages, proof, turn_id)
    if handoff is None:
        return None
    setattr(agent, _GATEWAY_WRITER_PROOF_HANDOFF, handoff)
    _clear_gateway_writer_proof_request(agent)
    return handoff

def peek_gateway_writer_proof(agent: Any) -> GatewayWriterProofHandoff | None:
    handoff = getattr(agent, _GATEWAY_WRITER_PROOF_HANDOFF, None)
    return handoff if type(handoff) is GatewayWriterProofHandoff else None


def acknowledge_gateway_writer_proof(agent: Any, handoff: Any) -> bool:
    current = peek_gateway_writer_proof(agent)
    if current is None or current is not handoff:
        return False
    discard_gateway_writer_proof(agent)
    return True


def _store_path(agent: Any) -> str | None:
    db = getattr(agent, "_session_db", None)
    raw = getattr(db, "db_path", None)
    if raw is None or isinstance(raw, bool):
        return None
    try:
        return str(Path(raw).expanduser().resolve())
    except (OSError, TypeError, ValueError):
        return None

def _intended_messages(
    messages: list[dict[str, Any]], conversation_history: list[dict[str, Any]] | None = None,
    *, replay_history: bool = False,
) -> tuple[dict[str, Any], ...]:
    """Select rows the canonical writer will consume, honoring its history identity boundary."""
    from agent.context_compressor import _DB_PERSISTED_MARKER
    from agent.session_persistence import _is_ephemeral_scaffolding
    history_ids = ({id(item) for item in conversation_history if isinstance(item, dict)}
                   if conversation_history is not None and not replay_history else set())
    return tuple(
        message for message in messages
        if isinstance(message, dict)
        and not _is_ephemeral_scaffolding(message)
        and not message.get(_DB_PERSISTED_MARKER)
        and id(message) not in history_ids
    )

def _restore_capture_attribute(agent: Any, name: str, sentinel: object, previous: Any) -> None:
    if previous is sentinel:
        agent.__dict__.pop(name, None)
    else:
        setattr(agent, name, previous)

def _read_committed_rows(db: Any, session_id: str, row_ids: tuple[int, ...]) -> list[Any]:
    """Read the exact writer-issued rows through SessionDB's canonical raw read guard."""
    selected: dict[int, Any] = {}
    for start in range(0, len(row_ids), 900):
        chunk = row_ids[start:start + 900]
        rows = db._read_all(
            f"SELECT * FROM messages WHERE session_id = ? AND id IN ({_placeholders(chunk)})",
            (session_id, *chunk),
        )
        for row in rows:
            try:
                row_id = row["id"]
            except (IndexError, KeyError, TypeError):
                continue
            if row_id in chunk:
                selected[row_id] = row
    return [selected[row_id] for row_id in row_ids if row_id in selected]

def _captured_writer_row(item: Any) -> tuple[int, str, str] | None:
    if type(item) is not tuple or len(item) != 3:
        return None
    _message_identity, writer_row, writer_message = item
    if not isinstance(writer_row, dict) or not isinstance(writer_message, dict):
        return None
    row_id = writer_message.get("_row_id")
    if type(row_id) is not int or row_id <= 0 or writer_row.get("_row_id") not in {None, row_id}:
        return None
    role = writer_row.get("role", writer_message.get("role"))
    if type(role) is not str or not role:
        return None
    marker = writer_message.get(DB_ROW_SNAPSHOT)
    writer_marker = writer_row.get(DB_ROW_SNAPSHOT)
    if (
        type(marker) is not str or not marker
        or (writer_marker is not None and (type(writer_marker) is not str or not writer_marker))
        or (writer_marker is not None and writer_marker != marker)
    ):
        return None
    return row_id, role, marker

def _captured_writer_rows(
    captured: Any, intended: tuple[dict[str, Any], ...],
) -> tuple[tuple[int, str, str], ...] | None:
    if type(captured) is not tuple or len(captured) != len(intended):
        return None
    if tuple(item[0] for item in captured if type(item) is tuple and item) != tuple(id(message) for message in intended):
        return None
    rows = tuple(_captured_writer_row(item) for item in captured)
    if any(row is None for row in rows):
        return None
    typed_rows = tuple(row for row in rows if row is not None)
    if len({row_id for row_id, _role, _marker in typed_rows}) != len(typed_rows):
        return None
    return typed_rows

def _capture_writer_batch(
    agent: Any, messages: list[dict[str, Any]], conversation_history: list[dict[str, Any]] | None,
) -> tuple[Any, Any]:
    sentinel = object()
    previous_capture = getattr(agent, "_managed_proof_capture", sentinel)
    previous_batch = getattr(agent, "_managed_proof_batch", sentinel)
    agent._managed_proof_capture = True
    agent._managed_proof_batch = ()
    try:
        result = agent._flush_messages_to_session_db(messages, conversation_history)
        return result, getattr(agent, "_managed_proof_batch", ())
    finally:
        _restore_capture_attribute(agent, "_managed_proof_capture", sentinel, previous_capture)
        _restore_capture_attribute(agent, "_managed_proof_batch", sentinel, previous_batch)


def _latest_user_index(messages: list[dict[str, Any]]) -> int | None:
    return next(
        (index for index in range(len(messages) - 1, -1, -1)
         if isinstance(messages[index], dict) and messages[index].get("role") == "user"),
        None,
    )


def managed_current_turn_is_staged(messages: list[dict[str, Any]]) -> bool:
    """Select the explicit managed path only after this turn's user row was durably stamped."""
    from agent.context_compressor import _DB_PERSISTED_MARKER

    index = _latest_user_index(messages)
    return index is not None and messages[index].get(_DB_PERSISTED_MARKER) is True


def _same_identity_slice(
    values: Any, expected: tuple[dict[str, Any], ...], start: int,
) -> bool:
    return (
        isinstance(values, list)
        and start >= 0
        and start + len(expected) <= len(values)
        and all(values[start + offset] is message for offset, message in enumerate(expected))
    )


def _persisted_row_spec(message: dict[str, Any]) -> tuple[int, str, str] | None:
    row_id = message.get("_row_id")
    role = message.get("role")
    marker = message.get(DB_ROW_SNAPSHOT)
    if (
        type(row_id) is not int or row_id <= 0
        or type(role) is not str or role not in {"user", "assistant", "tool"}
        or type(marker) is not str or not marker
    ):
        return None
    return row_id, role, marker


def _managed_current_turn_prefix(
    agent: Any, messages: list[dict[str, Any]],
) -> tuple[dict[str, Any], ...] | None:
    from agent.context_compressor import _DB_PERSISTED_MARKER
    from agent.session_persistence import _is_ephemeral_scaffolding

    anchor = _latest_user_index(messages)
    if anchor is None:
        return None
    turn = messages[anchor:]
    persisted: list[dict[str, Any]] = []
    for message in turn:
        if not isinstance(message, dict) or _is_ephemeral_scaffolding(message):
            return None
        if message.get(_DB_PERSISTED_MARKER) is not True:
            break
        if _persisted_row_spec(message) is None:
            return None
        persisted.append(message)
    if not persisted or persisted[0] is not turn[0] or persisted[0].get("role") != "user":
        return None
    if any(
        isinstance(message, dict) and message.get(_DB_PERSISTED_MARKER) is True
        for message in turn[len(persisted):]
    ):
        return None
    intended = _intended_messages(messages)
    intended_ids = {id(item) for item in intended}
    if not intended or any(id(message) in intended_ids for message in messages[:anchor]):
        return None
    flushed_prefix = getattr(agent, "_db_flush_scan_prefix", None)
    start = next((index for index, item in enumerate(flushed_prefix or []) if item is persisted[0]), None)
    if start is None or not _same_identity_slice(flushed_prefix, tuple(persisted), start):
        return None
    return tuple(persisted)


def _read_expected_rows(
    agent: Any, session_id: str, expected: tuple[tuple[int, str, str], ...],
) -> tuple[tuple[int, str, str], ...] | None:
    try:
        stored = _read_committed_rows(
            agent._session_db, session_id, tuple(row_id for row_id, _role, _marker in expected),
        )
    except (OSError, RuntimeError, TypeError, ValueError, KeyError, AttributeError, sqlite3.Error):
        return None
    if len(stored) != len(expected):
        return None
    checked: list[tuple[int, str, str]] = []
    for row, (row_id, expected_role, expected_marker) in zip(stored, expected):
        try:
            if (
                type(row["id"]) is not int
                or row["id"] != row_id
                or row["session_id"] != session_id
                or row["role"] != expected_role
            ):
                return None
            from agent.transcript_repair import transcript_row_snapshot
            snapshot = transcript_row_snapshot(row)
        except (IndexError, KeyError, TypeError, ValueError):
            return None
        if type(snapshot) is not str or not snapshot or snapshot != expected_marker:
            return None
        checked.append((row_id, expected_role, snapshot))
    return tuple(checked)


def _proof_from_validated_rows(
    store_path: str, session_id: str, rows: tuple[tuple[int, str, str], ...],
) -> SessionDBWriteProof:
    digest_input = "\n".join(
        f"{row_id}:{session_id}:{role}:{snapshot}"
        for row_id, role, snapshot in rows
    )
    return SessionDBWriteProof(
        store_path=store_path,
        session_id=session_id,
        row_ids=tuple(row_id for row_id, _role, _snapshot in rows),
        row_snapshots=tuple(snapshot for _row_id, _role, snapshot in rows),
        row_sessions=tuple(session_id for _row_id, _role, _snapshot in rows),
        row_roles=tuple(role for _row_id, role, _snapshot in rows),
        payload_digest=hashlib.sha256(digest_input.encode("utf-8")).hexdigest(),
    )

def _proof_from_writer_rows(
    agent: Any, store_path: str, session_id: str, writer_rows: tuple[tuple[int, str, str], ...],
) -> SessionDBWriteProof | None:
    checked = _read_expected_rows(agent, session_id, writer_rows)
    return None if checked is None else _proof_from_validated_rows(store_path, session_id, checked)


def _managed_writer_history(
    messages: list[dict[str, Any]], conversation_history: list[dict[str, Any]] | None,
    merged_prefix: SessionDBWriteProof | None,
) -> tuple[bool, list[dict[str, Any]] | None]:
    if merged_prefix is None:
        return True, conversation_history
    anchor_index = _latest_user_index(messages)
    if anchor_index is None:
        return False, None
    if conversation_history is None:
        return True, None
    anchor = messages[anchor_index]
    return True, [item for item in conversation_history if item is not anchor]


def _restore_merged_prefix_anchor(
    agent: Any, messages: list[dict[str, Any]], previous: Any, binding: Any,
) -> SessionDBWriteProof | None:
    if not _gateway_writer_merged_prefix_matches(agent, messages, previous, binding):
        return None
    merged_prefix = previous
    # The matcher authenticates the retained row before restoring its identity.
    anchor_index = _latest_user_index(messages)
    if (
        anchor_index is None
        or len(merged_prefix.row_ids) != 1
        or len(merged_prefix.row_snapshots) != 1
        or not isinstance(messages[anchor_index], dict)
    ):
        return None
    from agent.context_compressor import _DB_PERSISTED_MARKER
    anchor = messages[anchor_index]
    anchor["_row_id"] = merged_prefix.row_ids[0]
    anchor[DB_ROW_SNAPSHOT] = merged_prefix.row_snapshots[0]
    anchor[_DB_PERSISTED_MARKER] = True
    return merged_prefix


def _proof_after_merged_close(
    agent: Any, messages: list[dict[str, Any]], merged_prefix: SessionDBWriteProof | None,
    store_path: str, session_id: str,
) -> SessionDBWriteProof | None:
    """Prove a close-path suffix already committed after a retained merged prefix."""
    if merged_prefix is None:
        return None
    current = _current_turn_row_specs(messages)
    prefix_specs = tuple(zip(merged_prefix.row_ids, merged_prefix.row_roles, merged_prefix.row_snapshots))
    if current is None or current[:len(prefix_specs)] != prefix_specs:
        return None
    checked = _read_expected_rows(agent, session_id, current)
    return _proof_from_validated_rows(store_path, session_id, checked) if checked == current else None


def _proof_after_managed_batch(
    agent: Any, messages: list[dict[str, Any]], merged_prefix: SessionDBWriteProof | None,
    typed_prefix: tuple[tuple[int, str, str], ...],
    typed_writer: tuple[tuple[int, str, str], ...], store_path: str, session_id: str,
) -> SessionDBWriteProof | None:
    if merged_prefix is not None:
        current = _current_turn_row_specs(messages)
        prefix_specs = tuple(zip(merged_prefix.row_ids, merged_prefix.row_roles, merged_prefix.row_snapshots))
        if current != prefix_specs + typed_writer:
            return None
        checked = _read_expected_rows(agent, session_id, current)
        return _proof_from_validated_rows(store_path, session_id, checked) if checked == current else None
    checked_prefix = _read_expected_rows(agent, session_id, typed_prefix)
    if checked_prefix is None:
        return None
    prefix_id_set = {row_id for row_id, _role, _marker in checked_prefix}
    writer_ids = tuple(row_id for row_id, _role, _marker in typed_writer)
    if prefix_id_set.intersection(writer_ids):
        return None
    combined_ids = tuple(row_id for row_id, _role, _marker in checked_prefix + typed_writer)
    if any(left >= right for left, right in pairwise(combined_ids)):
        return None
    return _proof_from_validated_rows(store_path, session_id, checked_prefix + typed_writer)


def flush_messages_with_proof(
    agent: Any, messages: list[dict[str, Any]], conversation_history: list[dict[str, Any]] | None = None,
) -> SessionDBWriteProof | None:
    """Flush once, then prove every intended writer-issued row through SessionDB readback."""
    if getattr(agent, "_persist_disabled", False) or getattr(agent, "_session_db", None) is None:
        return None
    replay_history = getattr(agent, "_session_row_replay_pending", None) == getattr(agent, "session_id", None)
    intended = _intended_messages(messages, conversation_history, replay_history=replay_history)
    if not intended:
        return None
    result, captured = _capture_writer_batch(agent, messages, conversation_history)
    if result is not True:
        return None
    writer_rows = _captured_writer_rows(captured, intended)
    if writer_rows is None:
        return None
    store_path = _store_path(agent)
    session_id = getattr(agent, "session_id", None)
    if store_path is None or type(session_id) is not str or not session_id:
        return None
    return _proof_from_writer_rows(agent, store_path, session_id, writer_rows)
def flush_messages_with_managed_current_turn_proof(
    agent: Any, messages: list[dict[str, Any]], conversation_history: list[dict[str, Any]] | None = None,
) -> SessionDBWriteProof | None:
    """Read back a staged prefix, then flush and prove one fresh writer batch.

    A canonical user repair may carry the authenticated prior row as a markerless merge carrier;
    remove that carrier from the history identity set so the DB writer emits the repaired user
    row before the terminal assistant row.
    """
    if getattr(agent, "_persist_disabled", False) or getattr(agent, "_session_db", None) is None:
        return None
    prefix = _managed_current_turn_prefix(agent, messages)
    merged_prefix: SessionDBWriteProof | None = None
    if prefix is None:
        previous = getattr(agent, "_managed_shell_writer_proof", None)
        requested, explicit = _requested_gateway_binding(agent)
        binding = _active_gateway_binding(requested, explicit)
        merged_prefix = _restore_merged_prefix_anchor(agent, messages, previous, binding)
        if merged_prefix is None:
            return None
    store_path = _store_path(agent)
    session_id = getattr(agent, "session_id", None)
    if store_path is None or type(session_id) is not str or not session_id:
        return None
    typed_prefix: tuple[tuple[int, str, str], ...] = ()
    if merged_prefix is None:
        prefix_rows = tuple(_persisted_row_spec(message) for message in prefix)
        if any(row is None for row in prefix_rows):
            return None
        typed_prefix = tuple(row for row in prefix_rows if row is not None)
        prefix_ids = tuple(row_id for row_id, _role, _marker in typed_prefix)
        if len(set(prefix_ids)) != len(prefix_ids):
            return None
        if _read_expected_rows(agent, session_id, typed_prefix) is None:
            return None
    writer_valid, writer_history = _managed_writer_history(messages, conversation_history, merged_prefix)
    if not writer_valid:
        return None
    replay_history = getattr(agent, "_session_row_replay_pending", None) == session_id
    intended = _intended_messages(messages, writer_history, replay_history=replay_history)
    if not intended:
        # A close-path flush may have committed terminal rows before this proof capture runs.
        return _proof_after_merged_close(agent, messages, merged_prefix, store_path, session_id)
    result, captured = _capture_writer_batch(agent, messages, writer_history)
    if result is not True:
        return None
    writer_rows = _captured_writer_rows(captured, intended)
    if writer_rows is None:
        return None
    typed_writer = _read_expected_rows(agent, session_id, writer_rows)
    if typed_writer is None:
        return None
    return _proof_after_managed_batch(
        agent, messages, merged_prefix, typed_prefix, typed_writer, store_path, session_id,
    )
