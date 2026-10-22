"""Typed managed ACP receipt witness capture and validation.

This module is a topical extraction from the ACP client facade. It does not own a ledger or
create a second proof type; it only binds the existing SessionDBWriteProof to the existing
receipt-local DurableResultProof and owner identities.
"""

from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Any


@dataclass(frozen=True)
class _ManagedDurableReceiptWitness:
    """Opaque identity witness for one managed ACP durable-result receipt."""

    client: Any
    parent: Any
    service: Any
    request: Any
    binding: Any
    receipt: Any
    managed_witness: dict[str, Any]
    process: Any
    binding_snapshot: dict[str, Any]
    writer_proof: Any
    durable_proof: Any
    provider_identity: tuple[Any, ...]
    binding_snapshot_identity: tuple[Any, ...]
    durable_identity: tuple[Any, ...]
    binding_identity: tuple[Any, ...]
    physical_tip_witness: Any = None


_MANAGED_PROVIDER_FIELDS = (
    "operation_id", "profile_id", "process_instance", "authority_epoch", "source_identity",
    "admission_id", "execution_generation", "hermes_session_id", "provider_session_id",
    "result_identity", "payload_digest",
)
_MANAGED_DURABLE_FIELDS = (
    "store_path", "session_id", "admission_id", "execution_generation", "row_ids",
    "row_snapshots", "row_sessions", "row_roles", "payload_digest", "result_row_ids",
)
_MANAGED_SNAPSHOT_FIELDS = (
    "operation_id", "profile_id", "process_instance", "authority_epoch", "source_identity",
    "admission_id", "execution_generation", "hermes_session_id",
)
_MANAGED_BINDING_FIELDS = (
    "admission_id", "execution_generation", "session_id", "profile_id", "authority_epoch",
    "process_instance", "principal_id", "source_identity",
)


def _managed_identity_values(value: Any, fields: tuple[str, ...], *, mapping: bool = False):
    values = tuple(
        value.get(field) if mapping else getattr(value, field, None)
        for field in fields
    )
    if any(item is None or (type(item) is str and not item) for item in values):
        return None
    return values


def _managed_writer_proof_matches(
    writer_proof: Any, durable_proof: Any,
) -> bool:
    return all(
        getattr(writer_proof, field, None) == getattr(durable_proof, field, None)
        for field in (
            "store_path", "session_id", "row_ids", "row_snapshots", "row_sessions",
            "row_roles", "payload_digest",
        )
    )


def _managed_physical_tip(binding: Any, physical_session_id: Any, store_path: Any):
    try:
        from gateway.runtime_service_custody import TurnCustodyBinding
        from gateway.session_turn_physical_tip import resolve_physical_tip_witness
    except (AttributeError, ImportError):
        return None
    if not isinstance(binding, TurnCustodyBinding):
        return None
    try:
        return resolve_physical_tip_witness(binding, physical_session_id, store_path)
    except (
        AttributeError, ImportError, KeyError, OSError, RuntimeError, TypeError, ValueError,
        sqlite3.Error,
    ):
        return None


def _managed_result_suffix_matches(writer_proof: Any, durable_proof: Any) -> bool:
    """Require the durable result to be the exact non-empty suffix written for this turn."""
    writer_rows = getattr(writer_proof, "row_ids", None)
    result_rows = getattr(durable_proof, "result_row_ids", None)
    if (
        type(writer_rows) is not tuple or len(writer_rows) < 2
        or type(result_rows) is not tuple or not result_rows
        or result_rows != writer_rows[1:]
        or len(set(result_rows)) != len(result_rows)
        or any(type(row_id) is not int or row_id <= 0 for row_id in result_rows)
    ):
        return False
    return True


def capture_managed_durable_receipt_witness(
    client: Any, agent: Any, writer_proof: Any,
) -> _ManagedDurableReceiptWitness | None:
    """Capture the exact ACP/provider/receipt proof before a writer cache can be reused."""
    try:
        from agent.session_persistence_proof import SessionDBWriteProof
        from gateway.runtime_service_custody import DurableResultProof, TurnCustodyBinding
    except (ImportError, AttributeError):
        return None
    if type(writer_proof) is not SessionDBWriteProof:
        return None
    parent = getattr(getattr(client, "_producer_policy", None), "parent", None)
    service = getattr(parent, "_service", None)
    receipt = getattr(parent, "_physical", None)
    binding = getattr(parent, "turn_binding", None)
    witness = getattr(receipt, "managed_witness", None)
    durable_proof = witness.get("durable_proof") if isinstance(witness, dict) else None
    request = (
        (getattr(service, "requests", {}) or {}).get(getattr(binding, "admission_id", None))
        if service is not None and binding is not None else None
    )
    if (
        getattr(parent, "mode", None) != "managed_turn"
        or getattr(parent, "state", None) in {"unknown", "terminal"}
        or getattr(service, "unknown", True) is not False
        or getattr(receipt, "state", None) in {"unknown", "closed", "released"}
        or binding is None or request is None
        or getattr(parent, "_service", None) is not service
        or getattr(parent, "_request", None) is not request
        or getattr(parent, "_physical", None) is not receipt
        or getattr(service, "physical", None) is not receipt
        or getattr(service, "binding", None) is not binding
        or getattr(receipt, "service", None) is not service
        or getattr(receipt, "managed_client", None) is not client
        or getattr(parent, "_catalog_client", None) is not client
        or getattr(parent, "_owner_agent", None) is not agent
        or getattr(agent, "_managed_acp_client", None) is not client
        or not isinstance(witness, dict)
        or witness.get("parent") is not parent
        or witness.get("receipt") is not receipt
        or witness.get("_process") is None
        or not isinstance(witness.get("binding_snapshot"), dict)
        or type(durable_proof) is not DurableResultProof
    ):
        return None
    if (
        not _managed_writer_proof_matches(writer_proof, durable_proof)
        or not _managed_result_suffix_matches(writer_proof, durable_proof)
    ):
        return None
    if isinstance(binding, TurnCustodyBinding):
        validator = getattr(service, "_validate_service_identity", None)
        if not callable(validator):
            return None
        try:
            # The physical witness is useful only while the canonical I16 sidecar lease
            # still owns this exact retained receipt; fields on the receipt/row are not
            # a substitute for the existing coordinator validator.
            validator(binding)
        except (
            AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError,
            sqlite3.Error,
        ):
            return None
    provider_identity = _managed_identity_values(
        witness, _MANAGED_PROVIDER_FIELDS, mapping=True,
    )
    binding_snapshot = witness["binding_snapshot"]
    binding_snapshot_identity = _managed_identity_values(
        binding_snapshot, _MANAGED_SNAPSHOT_FIELDS, mapping=True,
    )
    durable_identity = _managed_identity_values(durable_proof, _MANAGED_DURABLE_FIELDS)
    binding_identity = _managed_identity_values(binding, _MANAGED_BINDING_FIELDS)
    if (
        provider_identity is None or binding_snapshot_identity is None
        or durable_identity is None or binding_identity is None
    ):
        return None
    if (
        witness.get("profile_id") != getattr(parent, "profile_id", None)
        or witness.get("process_instance") != getattr(parent, "process_instance", None)
        or witness.get("authority_epoch") != getattr(parent, "authority_epoch", None)
        or witness.get("source_identity") != getattr(parent, "source_identity", None)
        or witness.get("admission_id") != getattr(binding, "admission_id", None)
        or witness.get("execution_generation") != getattr(binding, "execution_generation", None)
        or witness.get("hermes_session_id") != getattr(binding, "session_id", None)
        or durable_proof.admission_id != binding.admission_id
        or durable_proof.execution_generation != binding.execution_generation
    ):
        return None
    physical_tip_witness = _managed_physical_tip(
        binding, writer_proof.session_id, writer_proof.store_path,
    )
    if (
        isinstance(binding, TurnCustodyBinding) and physical_tip_witness is None
    ) or (
        not isinstance(binding, TurnCustodyBinding)
        and writer_proof.session_id != binding.session_id
        and physical_tip_witness is None
    ):
        return None
    return _ManagedDurableReceiptWitness(
        client=client, parent=parent, service=service, request=request, binding=binding,
        receipt=receipt, managed_witness=witness, process=witness["_process"],
        binding_snapshot=binding_snapshot, writer_proof=writer_proof,
        durable_proof=durable_proof,
        provider_identity=provider_identity, durable_identity=durable_identity,
        binding_snapshot_identity=binding_snapshot_identity,
        binding_identity=binding_identity,
        physical_tip_witness=physical_tip_witness,
    )


def validate_managed_durable_receipt_witness(
    client: Any, agent: Any, writer_proof: Any, witness: Any,
) -> bool:
    """Revalidate the original managed physical/provider witness before idempotent reuse."""
    if not isinstance(witness, _ManagedDurableReceiptWitness):
        return False
    try:
        from agent.session_persistence_proof import SessionDBWriteProof
        from gateway.runtime_service_custody import TurnCustodyBinding
        if type(writer_proof) is not SessionDBWriteProof or writer_proof is not witness.writer_proof:
            return False
        parent = getattr(getattr(client, "_producer_policy", None), "parent", None)
        service = getattr(parent, "_service", None)
        receipt = getattr(parent, "_physical", None)
        binding = getattr(parent, "turn_binding", None)
        current_request = (
            (getattr(service, "requests", {}) or {}).get(getattr(binding, "admission_id", None))
            if service is not None and binding is not None else None
        )
        current_managed = getattr(receipt, "managed_witness", None)
        if (
            witness.client is not client
            or witness.parent is not parent
            or witness.service is not service
            or witness.binding is not binding
            or witness.receipt is not receipt
            or witness.request is not current_request
            or getattr(parent, "mode", None) != "managed_turn"
            or getattr(parent, "state", None) in {"unknown", "terminal"}
            or getattr(service, "unknown", True) is not False
            or getattr(parent, "_owner_agent", None) is not agent
            or getattr(agent, "_managed_acp_client", None) is not client
            or getattr(parent, "_catalog_client", None) is not client
            or getattr(parent, "_service", None) is not service
            or getattr(parent, "_request", None) is not current_request
            or getattr(parent, "_physical", None) is not receipt
            or getattr(service, "physical", None) is not receipt
            or getattr(service, "binding", None) is not binding
            or getattr(receipt, "service", None) is not service
            or getattr(receipt, "managed_client", None) is not client
            or getattr(receipt, "state", None) in {"unknown", "closed", "released"}
            or current_managed is not witness.managed_witness
            or not isinstance(current_managed, dict)
            or current_managed.get("_process") is not witness.process
            or current_managed.get("binding_snapshot") is not witness.binding_snapshot
            or current_managed.get("durable_proof") is not witness.durable_proof
            or not _managed_writer_proof_matches(writer_proof, witness.durable_proof)
            or not _managed_result_suffix_matches(writer_proof, witness.durable_proof)
        ):
            return False
        if isinstance(binding, TurnCustodyBinding):
            validator = getattr(service, "_validate_service_identity", None)
            if not callable(validator):
                return False
            try:
                validator(binding)
            except (
                AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError,
                sqlite3.Error,
            ):
                return False
        provider_identity = _managed_identity_values(
            current_managed, _MANAGED_PROVIDER_FIELDS, mapping=True,
        )
        durable_identity = _managed_identity_values(
            witness.durable_proof, _MANAGED_DURABLE_FIELDS,
        )
        binding_snapshot_identity = _managed_identity_values(
            current_managed.get("binding_snapshot"), _MANAGED_SNAPSHOT_FIELDS, mapping=True,
        )
        binding_identity = _managed_identity_values(binding, _MANAGED_BINDING_FIELDS)
        physical_tip_witness = _managed_physical_tip(
            binding, writer_proof.session_id, writer_proof.store_path,
        )
        if isinstance(binding, TurnCustodyBinding) or writer_proof.session_id != binding.session_id:
            if physical_tip_witness is None or physical_tip_witness != witness.physical_tip_witness:
                return False
        elif witness.physical_tip_witness is not None:
            return False
        return (
            provider_identity == witness.provider_identity
            and binding_snapshot_identity == witness.binding_snapshot_identity
            and durable_identity == witness.durable_identity
            and binding_identity == witness.binding_identity
            and current_managed.get("parent") is parent
            and current_managed.get("receipt") is receipt
        )
    except (
        AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError,
        sqlite3.Error,
    ):
        return False


def mark_managed_durable_receipt_unknown(_client: Any, witness: Any) -> bool:
    """Keep the original managed parent recoverable when its cached witness is tampered with."""
    if not isinstance(witness, _ManagedDurableReceiptWitness):
        return False
    parent = witness.parent
    if parent is None or getattr(parent, "state", None) in {"unknown", "terminal"}:
        return False
    try:
        from gateway.runtime_service_operations import mark_unknown
        mark_unknown(parent, "managed_durable_witness_invalid")
        return True
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        return False

