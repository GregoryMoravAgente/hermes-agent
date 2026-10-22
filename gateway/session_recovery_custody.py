"""Source-owned custody for profile-scoped startup/recovery handoffs.

I10 is an adapter around the existing coordinator and canonical admission store.  It does not add
a durable table or a second lease ledger.  Every binding below keeps the original coordinator
handle and a source-owned typed readback until the shared terminal release boundary consumes it.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sqlite3
from itertools import pairwise
from typing import Any, Callable

from hermes_state_runtime import RuntimeStoreError, get_session_admission


OWNER_CLASS = "I10_startup_recovery_handoff"
CONSUMERS = (
    "startup",
    "local_recovery",
    "webhook_replay",
    "adapter_reconnect",
    "completion_notification",
    "bot_mailbox",
)

_SOURCE_BY_CONSUMER = {
    "startup": "gateway/run_startup.py",
    "local_recovery": "gateway/session_local_recovery.py",
    "webhook_replay": "gateway/platforms/webhook_ingress.py",
    "adapter_reconnect": "gateway/run_adapters.py",
    "completion_notification": "gateway/run_notifications.py",
    "bot_mailbox": "gateway/session_bot_mailbox.py",
}


@dataclass(frozen=True)
class RecoveryBindingIdentity:
    """The immutable source identity carried by one recovery occurrence.

    ``owner_epoch`` and ``generation`` may advance once at the source-owned claim transition.  The
    transition is explicit in ``refresh_binding``; a caller cannot update either scalar directly.
    ``handle`` remains a separate object identity and is never reconstructed from this value.
    """

    admission_id: str
    target_session_id: str
    principal_id: str
    request_id: str
    owner_epoch: int | None
    generation: int | None
    source_digest: str
    route_digest: str
    occurrence_digest: str
    profile_id: str
    authority_epoch: int
    process_instance: str


@dataclass(frozen=True)
class RecoveryConsumerAbsence:
    """A source-owned no-work observation for an unused consumer.

    This is produced only by ``unused_consumer`` after its caller has performed the consumer's
    own canonical intake check.  It is not inferred from an empty local map or a zero count.
    """

    schema: str
    consumer: str
    source: str
    process_instance: str
    authority_epoch: int
    profile_home: str
    reason: str
    close_witness: RecoveryConsumerCloseWitness | None = None


@dataclass(frozen=True)
class RecoveryConsumerCloseWitness:
    """Typed source snapshot attached at the reversible close boundary.

    A source absence is admissible only after its own canonical registry/queue has been read while
    the process ingress fence is closed.  The witness retains the exact profile, process, authority
    epoch and close generation that produced that readback; a later generic empty-map check cannot
    manufacture one.
    """

    schema: str
    consumer: str
    source: str
    profile_id: str
    profile_home: str
    process_instance: str
    authority_epoch: int
    close_generation: int
    source_generation: int
    ingress_state: str
    registry_digest: str
    pending_ids: tuple[str, ...]


@dataclass(frozen=True)
class RecoveryConsumerReceipt:
    """A typed source readback attached to the exact I10 handle.

    The source receipt tuple contains only the existing typed receipt classes.  The wrapper carries
    the immutable row identity and object-bound handle so a copied row, lease id, or dict cannot
    satisfy close-time proof.
    """

    schema: str
    consumer: str
    source: str
    state: str
    identity: RecoveryBindingIdentity | None
    handle: object | None
    source_receipts: tuple[object, ...]
    finalizer_digest: str | None = None
    reason: str | None = None
    close_witness: RecoveryConsumerCloseWitness | None = None


@dataclass(frozen=True)
class LocalPolicyLineageWitness:
    """Source-authenticated persisted local policy and canonical lineage readback."""

    schema: str
    source: str
    session_id: str
    route: str
    profile_id: str
    authority_epoch: int
    process_instance: str
    principal_id: str
    request_id: str
    physical_session_id: str
    policy_digest: str
    lineage: tuple[str, ...]
    lineage_digest: str
    expected_physical_session_id: str
    expected_lineage: tuple[str, ...]
    effect_state: str = "committed"


@dataclass(frozen=True)
class LocalRecoverySourceReceipt:
    """The local producer's typed source witness for one recovered admission."""

    schema: str
    source: str
    admission_id: str
    logical_session_id: str
    route: str
    physical_session_id: str
    profile_id: str
    authority_epoch: int
    process_instance: str
    policy_principal_id: str
    policy_request_id: str
    admission_principal_id: str
    admission_request_id: str
    policy_digest: str
    lineage: tuple[str, ...]
    lineage_digest: str
    worker_status: str


@dataclass(frozen=True)
class RecoveryRetirementProof:
    """The exact source and coordinator proof needed before an I10 root retires."""

    admission_id: str
    handle: object
    identity: RecoveryBindingIdentity
    row: dict
    terminal_receipt: object
    receipt_digest: str
    child: object | None = None


_RECEIPT_SCHEMA = "hermes.gateway.i10-consumer-receipt.v1"
_ABSENCE_SCHEMA = "hermes.gateway.i10-consumer-absence.v1"
_LOCAL_POLICY_SCHEMA = "hermes.gateway.local-policy-lineage.v1"
_LOCAL_SOURCE_SCHEMA = "hermes.gateway.local-recovery-source.v1"
_CLOSE_WITNESS_SCHEMA = "hermes.gateway.i10-consumer-close-witness.v1"
_LOCAL_POLICY_SOURCES = frozenset({'cli', 'tui', 'gui', 'acp', 'tool', 'oneshot'})
_ACTIVE_STATES = {"accepted", "active", "unknown", "finalizing"}
_TERMINAL_STATES = {"terminal", "failed", "cancelled"}
I10_NOT_BOUND = object()


def _json_digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _home(authority):
    return Path(authority.db.db_path).parent.resolve()


def _identity(row, authority=None):
    if not isinstance(row, dict):
        return None
    values = tuple(row.get(field) for field in ("admission_id", "target_session_id", "principal_id", "request_id"))
    if not all(isinstance(value, str) and value for value in values):
        return None
    owner_epoch = row.get("owner_epoch")
    generation = row.get("generation")
    if owner_epoch is not None and (type(owner_epoch) is not int or owner_epoch < 1):
        return None
    if generation is not None and (type(generation) is not int or generation < 0):
        return None
    payload = row.get("payload")
    if not isinstance(payload, dict):
        return None
    native = payload.get("native_text_v1")
    if native is not None and not isinstance(native, dict):
        return None
    source = native.get("source") if isinstance(native, dict) else payload.get("source")
    route = native.get("route") if isinstance(native, dict) else payload.get("route")
    event = native.get("event") if isinstance(native, dict) else payload.get("event")
    if source is None:
        source = row.get("source") or row.get("principal_id")
    if route is None:
        route = row.get("target_session_id")
    occurrence = {
        "admission_id": row.get("admission_id"),
        "request_id": row.get("request_id"),
        "event": event,
        "message_id": event.get("message_id") if isinstance(event, dict) else None,
    }
    profile_id = getattr(authority, "profile_id", None)
    authority_epoch = getattr(authority, "epoch", None)
    process_instance = getattr(authority, "instance_id", None)
    if not all(isinstance(value, str) and value for value in (profile_id, process_instance)):
        return None
    if type(authority_epoch) is not int or authority_epoch < 1:
        return None
    return RecoveryBindingIdentity(
        admission_id=values[0], target_session_id=values[1], principal_id=values[2], request_id=values[3],
        owner_epoch=owner_epoch, generation=generation,
        source_digest=_json_digest(source), route_digest=_json_digest(route),
        occurrence_digest=_json_digest(occurrence), profile_id=profile_id,
        authority_epoch=authority_epoch, process_instance=process_instance,
    )


def _same_source_identity(left, right):
    if not isinstance(left, RecoveryBindingIdentity) or not isinstance(right, RecoveryBindingIdentity):
        return False
    return (
        left.admission_id == right.admission_id
        and left.target_session_id == right.target_session_id
        and left.principal_id == right.principal_id
        and left.request_id == right.request_id
        and left.source_digest == right.source_digest
        and left.route_digest == right.route_digest
        and left.occurrence_digest == right.occurrence_digest
        and left.profile_id == right.profile_id
        and left.authority_epoch == right.authority_epoch
        and left.process_instance == right.process_instance
    )


def _same_full_identity(left, right):
    return _same_source_identity(left, right) and (
        left.owner_epoch == right.owner_epoch and left.generation == right.generation
    )


def _current_epoch(authority):
    try:
        with authority.db._read_ctx() as conn:
            row = conn.execute(
                "SELECT epoch, instance_id FROM runtime_epoch WHERE singleton=1"
            ).fetchone()
    except BaseException as exc:
        raise RuntimeStoreError("read_failed") from exc
    if row is None or type(row[0]) is not int or row[0] != authority.epoch:
        raise RuntimeStoreError("stale_epoch")
    if row[1] != authority.instance_id:
        raise RuntimeStoreError("stale_epoch")
    return row


def _physical_owner(authority):
    from gateway.runtime_ownership import process_ownership

    evidence = process_ownership.reservation_evidence(_home(authority))
    if not isinstance(evidence, dict):
        raise RuntimeStoreError("foreign_custody")
    expected = str(_home(authority))
    if (
        evidence.get("profile_home") != expected
        or evidence.get("os_lock_owner") is not True
        or evidence.get("cross_process_proof") is not True
        or evidence.get("native_handle_retained") is not True
        or evidence.get("observed") is not True
    ):
        raise RuntimeStoreError("foreign_custody")
    return evidence


def _bindings(authority):
    bindings = getattr(authority, "_i10_recovery_bindings", None)
    if bindings is None:
        bindings = authority._i10_recovery_bindings = {}
    return bindings


def _coordinator_record(authority, handle):
    coordinator = getattr(authority.runner, "runtime_custody", None)
    lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
    records = getattr(coordinator, "_records", None) if coordinator is not None else None
    lock = getattr(coordinator, "_lock", None) if coordinator is not None else None
    if coordinator is None or not isinstance(lease_id, str) or not isinstance(records, dict) or lock is None:
        return None
    with lock:
        record = records.get(lease_id)
        if (
            record is None
            or getattr(record, "handle", None) is not handle
            or getattr(handle, "coordinator_id", None) != getattr(coordinator, "_coordinator_id", None)
        ):
            return None
        return record


def _validate_handle(authority, handle, *, allow_unknown=False):
    record = _coordinator_record(authority, handle)
    if record is None:
        return None
    if getattr(handle, "profile_id", None) != authority.profile_id:
        return None
    if getattr(handle, "authority_epoch", None) != authority.epoch:
        return None
    if getattr(handle, "process_instance", None) != authority.instance_id:
        return None
    allowed = {"accepted", "finalizing"}
    if allow_unknown:
        allowed.add("unknown")
    return record if getattr(record, "state", None) in allowed else None


def _source_receipt_types():
    from gateway.delivery_ledger import DeliveryObligationProof
    from gateway.preservation_source_close import SourceFinalizerReceipt
    from gateway.session_contract import AdmissionReceipt
    from gateway.session_ingress import DeliveryReceipt
    return (
        AdmissionReceipt, DeliveryReceipt, DeliveryObligationProof, SourceFinalizerReceipt,
        LocalRecoverySourceReceipt,
    )


def _validate_source_receipts(receipts, *, consumer=None):
    if not isinstance(receipts, tuple) or not receipts:
        return False
    accepted = _source_receipt_types()
    if not all(isinstance(receipt, accepted) for receipt in receipts):
        return False
    return all(
        not isinstance(receipt, LocalRecoverySourceReceipt) or consumer == 'local_recovery'
        for receipt in receipts
    )


def _validate_local_lineage_prefix(conn, saved, lineage):
    """Validate the persisted chain while allowing the real untracked tail to remain external."""
    from hermes_state_compression import _CHAIN_STEP_SQL
    from hermes_state_keys import local_identity
    from hermes_state_local_migration import legacy_lineage_root

    if ('legacy_session_id' not in saved
            and saved['session_id'] != local_identity(
                saved['profile_id'], saved['principal_id'], saved['request_id'])):
        raise RuntimeStoreError("storage_unavailable")
    root = legacy_lineage_root(conn, saved)
    physical = saved['entry']['session_id']
    if (not isinstance(lineage, list) or not lineage or len(set(lineage)) != len(lineage)
            or lineage[0] != root or lineage[-1] != physical):
        raise RuntimeStoreError("storage_unavailable")
    for parent_id, child_id in pairwise(lineage):
        parent = conn.execute(
            'SELECT end_reason FROM sessions WHERE id=?', (parent_id,)).fetchone()
        child = conn.execute(
            'SELECT parent_session_id,model_config FROM sessions WHERE id=?', (child_id,)
        ).fetchone()
        if parent is None or child is None or child['parent_session_id'] != parent_id:
            raise RuntimeStoreError("storage_unavailable")
        if parent['end_reason'] == 'session_reset':
            valid = json.loads(child['model_config'] or '{}').get('_reset_from') == parent_id
        else:
            tip = conn.execute(_CHAIN_STEP_SQL, (parent_id,)).fetchone()
            valid = (parent['end_reason'] == 'compression'
                     and tip is not None and tip['id'] == child_id)
        if not valid:
            raise RuntimeStoreError("storage_unavailable")


def _validated_local_policy(authority, sid, policy_receipt, *, allow_untracked=False):
    from hermes_state_local import local_receipt
    from hermes_state_local_lineage import validate_local_lineage
    from hermes_state_local_migration import legacy_lineage_root

    _current_epoch(authority)
    saved = local_receipt(authority.db, sid)
    if policy_receipt is not None and saved != policy_receipt:
        raise RuntimeStoreError("stale_generation")
    entry = saved.get("entry")
    policy = saved.get("policy")
    values = (saved.get("session_id"), saved.get("profile_id"), saved.get("principal_id"),
              saved.get("request_id"), saved.get("route"))
    physical = entry.get("session_id") if isinstance(entry, dict) else None
    if (not isinstance(entry, dict) or not isinstance(policy, dict)
            or not all(isinstance(value, str) and value for value in values)
            or saved.get("session_id") != sid or not isinstance(physical, str) or not physical):
        raise RuntimeStoreError("preservation_receipt_unmapped")
    with authority.db._read_ctx() as conn:
        persisted_lineage = saved.get("lineage")
        lineage = ([legacy_lineage_root(conn, saved)]
                   if persisted_lineage is None else persisted_lineage)
        if allow_untracked:
            _validate_local_lineage_prefix(conn, saved, lineage)
            canonical_tip = physical
        else:
            canonical_tip = validate_local_lineage(conn, saved)
    if (canonical_tip != physical or not isinstance(lineage, list) or not lineage
            or len(set(lineage)) != len(lineage)
            or not all(isinstance(value, str) and value for value in lineage)
            or lineage[-1] != physical):
        raise RuntimeStoreError("preservation_receipt_unmapped")
    return saved, entry, policy, tuple(lineage)


def local_policy_lineage_witness(authority, sid, policy_receipt=None, *, expected_children=()):
    """Read a committed local witness, or a planned witness when a real tail is supplied."""
    if not isinstance(expected_children, (tuple, list)) or not all(
            isinstance(child, str) and child for child in expected_children):
        raise RuntimeStoreError("preservation_receipt_unmapped")
    if expected_children:
        return local_policy_lineage_plan(
            authority, sid, policy_receipt, expected_children=expected_children,
        )
    saved, entry, policy, lineage = _validated_local_policy(authority, sid, policy_receipt)
    return LocalPolicyLineageWitness(
        schema=_LOCAL_POLICY_SCHEMA, source="gateway/session_local_recovery.py", session_id=sid,
        route=saved["route"], profile_id=saved["profile_id"], authority_epoch=authority.epoch,
        process_instance=authority.instance_id, principal_id=saved["principal_id"],
        request_id=saved["request_id"], physical_session_id=entry["session_id"],
        policy_digest=_json_digest(policy), lineage=lineage,
        lineage_digest=_json_digest(list(lineage)), expected_physical_session_id=entry["session_id"],
        expected_lineage=lineage, effect_state="committed",
    )


def local_policy_lineage_plan(authority, sid, policy_receipt=None, *, expected_children=()):
    """Authenticate the persisted prefix and the actual adoptable tail before I11 reservation."""
    from hermes_state_local_lineage import adoptable_compression

    if not isinstance(expected_children, (tuple, list)) or not all(
            isinstance(child, str) and child for child in expected_children):
        raise RuntimeStoreError("preservation_receipt_unmapped")
    saved, entry, policy, lineage = _validated_local_policy(
        authority, sid, policy_receipt, allow_untracked=True,
    )
    with authority.db._read_ctx() as conn:
        adoptable = adoptable_compression(conn, saved)
    if adoptable is None:
        raise RuntimeStoreError("stale_generation")
    children, routed = adoptable
    if (tuple(children) != tuple(expected_children) or not isinstance(routed, dict)
            or routed.get("session_id") != entry["session_id"]):
        raise RuntimeStoreError("stale_generation")
    expected_lineage = lineage + tuple(expected_children)
    return LocalPolicyLineageWitness(
        schema=_LOCAL_POLICY_SCHEMA, source="gateway/session_local_recovery.py", session_id=sid,
        route=saved["route"], profile_id=saved["profile_id"], authority_epoch=authority.epoch,
        process_instance=authority.instance_id, principal_id=saved["principal_id"],
        request_id=saved["request_id"], physical_session_id=entry["session_id"],
        policy_digest=_json_digest(policy), lineage=lineage,
        lineage_digest=_json_digest(list(lineage)), expected_physical_session_id=expected_lineage[-1],
        expected_lineage=expected_lineage, effect_state="planned",
    )


def local_recovery_source_receipt(authority, row, policy_receipt, *, worker_status):
    """Build a source-bound witness from the persisted policy and canonical lineage."""
    identity = _identity(row, authority)
    if (identity is None or worker_status not in {"mounted", "compression_catchup"}
            or not isinstance(policy_receipt, dict)):
        raise RuntimeStoreError("preservation_receipt_unmapped")
    witness = local_policy_lineage_witness(authority, identity.target_session_id, policy_receipt)
    policy = policy_receipt.get('policy')
    if not isinstance(policy, dict) or policy.get('source') not in _LOCAL_POLICY_SOURCES:
        raise RuntimeStoreError("preservation_receipt_unmapped")
    if (witness.effect_state != "committed"
            or witness.profile_id != authority.profile_id
            or witness.session_id != identity.target_session_id):
        raise RuntimeStoreError("preservation_receipt_unmapped")
    return LocalRecoverySourceReceipt(
        _LOCAL_SOURCE_SCHEMA, witness.source, identity.admission_id, witness.session_id,
        witness.route, witness.physical_session_id, witness.profile_id, witness.authority_epoch,
        witness.process_instance, witness.principal_id, witness.request_id,
        identity.principal_id, identity.request_id, witness.policy_digest, witness.lineage,
        witness.lineage_digest, worker_status,
    )


def _local_source_identity_shape(identity, receipt):
    return (
        isinstance(identity, RecoveryBindingIdentity)
        and isinstance(receipt.admission_id, str) and bool(receipt.admission_id)
        and receipt.admission_id == identity.admission_id
        and receipt.logical_session_id == identity.target_session_id
        and isinstance(receipt.route, str) and bool(receipt.route)
        and receipt.admission_principal_id == identity.principal_id
        and receipt.admission_request_id == identity.request_id
    )


def _local_source_lineage_shape(receipt):
    return (
        isinstance(receipt.physical_session_id, str)
        and bool(receipt.physical_session_id)
        and isinstance(receipt.lineage, tuple)
        and bool(receipt.lineage)
        and len(set(receipt.lineage)) == len(receipt.lineage)
        and receipt.lineage[-1] == receipt.physical_session_id
        and receipt.physical_session_id in receipt.lineage
        and receipt.lineage_digest == _json_digest(list(receipt.lineage))
        and isinstance(receipt.policy_digest, str)
        and bool(receipt.policy_digest)
    )


def _local_source_shape_matches(authority, identity, receipt):
    return (
        isinstance(receipt, LocalRecoverySourceReceipt)
        and receipt.schema == _LOCAL_SOURCE_SCHEMA
        and receipt.source == "gateway/session_local_recovery.py"
        and receipt.profile_id == authority.profile_id
        and receipt.authority_epoch == authority.epoch
        and receipt.process_instance == authority.instance_id
        and receipt.worker_status in {"mounted", "compression_catchup"}
        and _local_source_identity_shape(identity, receipt)
        and _local_source_lineage_shape(receipt)
    )


def _local_source_policy_matches(receipt, witness):
    return (
        isinstance(witness, LocalPolicyLineageWitness)
        and witness.schema == _LOCAL_POLICY_SCHEMA
        and witness.source == receipt.source
        and witness.session_id == receipt.logical_session_id
        and witness.route == receipt.route
        and witness.profile_id == receipt.profile_id
        and witness.authority_epoch == receipt.authority_epoch
        and witness.process_instance == receipt.process_instance
        and witness.principal_id == receipt.policy_principal_id
        and witness.request_id == receipt.policy_request_id
        and witness.physical_session_id == receipt.physical_session_id
        and witness.policy_digest == receipt.policy_digest
        and witness.lineage == receipt.lineage
        and witness.lineage_digest == receipt.lineage_digest
        and witness.effect_state == "committed"
    )


def _local_source_matches(authority, identity, receipt):
    """Match a local witness to the current row, policy, profile, epoch and canonical lineage."""
    if not _local_source_shape_matches(authority, identity, receipt):
        return False
    try:
        current = get_session_admission(authority.db, admission_id=identity.admission_id)
        witness = local_policy_lineage_witness(authority, identity.target_session_id)
    except (AttributeError, KeyError, LookupError, OSError, RuntimeStoreError,
            sqlite3.Error, TypeError, ValueError):
        return False
    current_identity = _identity(current, authority)
    return (
        isinstance(current, dict)
        and current.get("status") == "terminal"
        and _same_full_identity(identity, current_identity)
        and _local_source_policy_matches(receipt, witness)
    )


def _bound_receipt(binding, receipt):
    receipts = binding.get("receipts") if isinstance(binding, dict) else None
    return isinstance(receipts, tuple) and any(stored is receipt for stored in receipts)


def _source_delivery_receipt(binding, receipt):
    return (
        isinstance(binding, dict)
        and binding.get("source_delivery_receipt") is receipt
    )


def record_source_delivery_receipt(authority, admission_id, receipt):
    """Bind the exact receipt object emitted by ``deliver_settled`` to one I10 occurrence."""
    from gateway.session_ingress import DeliveryReceipt

    if not isinstance(receipt, DeliveryReceipt) or receipt.admission_id != admission_id:
        return False
    binding = _bindings(authority).get(admission_id)
    if not isinstance(binding, dict) or binding.get("handle") is None:
        return False
    receipts = binding.get("receipts")
    if not isinstance(receipts, tuple):
        return False
    binding["source_delivery_receipt"] = receipt
    if not any(stored is receipt for stored in receipts):
        binding["receipts"] = receipts + (receipt,)
    observations = getattr(authority, "_i10_consumer_evidence", {})
    for consumer, stored in tuple(observations.items()):
        observed = stored if isinstance(stored, tuple) else (stored,)
        updated = []
        changed = False
        for item in observed:
            if not (
                isinstance(item, RecoveryConsumerReceipt)
                and item.identity is not None
                and item.identity.admission_id == admission_id
            ):
                updated.append(item)
                continue
            source_receipts = item.source_receipts
            if not any(existing is receipt for existing in source_receipts):
                source_receipts = source_receipts + (receipt,)
            updated.append(RecoveryConsumerReceipt(
                item.schema, item.consumer, item.source, item.state, item.identity,
                item.handle, source_receipts, item.finalizer_digest, item.reason,
                item.close_witness,
            ))
            changed = True
        if changed:
            observations[consumer] = updated[0] if len(updated) == 1 else tuple(updated)
    return True


def begin_i10_retry(authority, admission_id, reason, effect_state):
    """Arm one source-owned retry before its delivery worker is re-entered."""
    binding = _bindings(authority).get(admission_id)
    if (
        not isinstance(binding, dict)
        or binding.get("retry_attempt") is not None
        or reason != "delivery_failed"
        or effect_state != "none"
    ):
        return False
    binding["retry_attempt"] = (reason, effect_state)
    return True


def finish_i10_retry(authority, admission_id, receipt, reason, effect_state):
    """Attach the source-produced retry receipt without accepting caller-shaped evidence."""
    from gateway.session_ingress import DeliveryReceipt

    binding = _bindings(authority).get(admission_id)
    if (
        not isinstance(receipt, DeliveryReceipt)
        or receipt.admission_id != admission_id
        or receipt.attempted is not True
        or receipt.delivered is not True
        or receipt.reason is not None
        or receipt.effect_state != "delivered"
        or not isinstance(binding, dict)
        or binding.get("retry_attempt") != (reason, effect_state)
        or not _source_delivery_receipt(binding, receipt)
    ):
        return False
    binding["retry_receipt"] = receipt
    binding["retry_reason"] = reason
    binding["retry_effect_state"] = effect_state
    return True


def _current_identity_matches(authority, binding, current):
    identity = _identity(current, authority)
    return (
        isinstance(current, dict)
        and isinstance(identity, RecoveryBindingIdentity)
        and isinstance(binding, dict)
        and _same_full_identity(binding.get("identity"), identity)
    )


def bind_terminal_receipt(authority, admission_id, receipt):
    """Bind the actual transport receipt before the shared terminal release boundary."""
    from gateway.session_ingress import DeliveryReceipt

    if not isinstance(receipt, DeliveryReceipt) or receipt.admission_id != admission_id:
        return False
    binding = _bindings(authority).get(admission_id)
    try:
        current = get_session_admission(authority.db, admission_id=admission_id)
    except (AttributeError, KeyError, LookupError, OSError, RuntimeError, sqlite3.Error,
            TypeError, ValueError):
        return False
    handle = binding.get("handle") if isinstance(binding, dict) else None
    if (
        not isinstance(binding, dict)
        or not isinstance(current, dict)
        or current.get("status") != "terminal"
        or not _current_identity_matches(authority, binding, current)
        or _validate_handle(authority, handle) is None
        or not _source_delivery_receipt(binding, receipt)
    ):
        return False
    receipts = binding.get("receipts")
    if not isinstance(receipts, tuple):
        return False
    if not any(stored is receipt for stored in receipts):
        binding["receipts"] = receipts + (receipt,)
    return True


def _rollback_reopened(authority, reopened):
    coordinator = getattr(authority.runner, "runtime_custody", None)
    if coordinator is None:
        return False
    success = True
    for lease in reversed(reopened):
        coordinator.mark_unknown(lease, "delivery_reconcile_failed")
        if getattr(_coordinator_record(authority, lease), "state", None) != "unknown":
            success = False
    return success


def retain_i10_unknown_after_reconcile(authority, admission_id):
    """Return a reconciled I10 occurrence to UNKNOWN when a later gate refuses it."""
    binding = _bindings(authority).get(admission_id)
    handle = binding.get("handle") if isinstance(binding, dict) else None
    child = getattr(authority, "_preservation_children", {}).get(admission_id)
    reopened = []
    for lease in (handle, child):
        record = _coordinator_record(authority, lease) if lease is not None else None
        if getattr(record, "state", None) in {"accepted", "finalizing"}:
            reopened.append(lease)
    return bool(reopened) and _rollback_reopened(authority, reopened)


def _reconcile_i10_leases(authority, admission_id, receipt):
    """Reopen exact unknown leases and retain them unknown if any later step refuses."""
    from gateway.session_ingress import DeliveryReceipt

    if not (
        isinstance(receipt, DeliveryReceipt)
        and receipt.admission_id == admission_id
        and receipt.attempted is True
        and receipt.delivered is True
        and receipt.reason is None
        and receipt.effect_state == "delivered"
    ):
        return None
    binding = _bindings(authority).get(admission_id)
    current = get_session_admission(authority.db, admission_id=admission_id)
    handle = binding.get("handle") if isinstance(binding, dict) else None
    if (
        not isinstance(binding, dict)
        or not isinstance(current, dict)
        or current.get("status") != "terminal"
        or getattr(handle, "owner_class", None) != OWNER_CLASS
        or _validate_handle(authority, handle, allow_unknown=True) is None
        or not _current_identity_matches(authority, binding, current)
    ):
        return None
    if not _source_delivery_receipt(binding, receipt):
        return None
    retry_attempt = binding.get("retry_attempt")
    if retry_attempt is not None and (
        binding.get("retry_receipt") is not receipt
        or retry_attempt != ("delivery_failed", "none")
        or binding.get("retry_reason") != "delivery_failed"
        or binding.get("retry_effect_state") != "none"
    ):
        return None
    try:
        _current_epoch(authority)
        _physical_owner(authority)
    except (RuntimeStoreError, AttributeError, TypeError, ValueError):
        return None
    child = getattr(authority, "_preservation_children", {}).get(admission_id)
    if child is None or not _child_matches_binding(authority, admission_id, handle, child):
        return None
    coordinator = getattr(authority.runner, "runtime_custody", None)
    if coordinator is None:
        return None
    leases = ((handle, getattr(handle, "owner_id", None)), (child, f"claim:{admission_id}"))
    reopened = []
    for lease, owner_id in leases:
        if not isinstance(owner_id, str) or not owner_id:
            _rollback_reopened(authority, reopened)
            return None
        record = _coordinator_record(authority, lease)
        if record is None or getattr(record, "state", None) not in {"accepted", "finalizing", "unknown"}:
            _rollback_reopened(authority, reopened)
            return None
        if getattr(record, "state", None) == "unknown":
            refusal = coordinator.reconcile_unknown(
                lease,
                owner_id=owner_id,
                profile_id=authority.profile_id,
                process_instance=authority.instance_id,
                authority_epoch=authority.epoch,
            )
            if refusal is not None:
                _rollback_reopened(authority, reopened)
                return None
            reopened.append(lease)
    return reopened


def reconcile_binding_after_delivery(authority, admission_id, receipt):
    """Reopen only the same I10 leases after a source-owned pre-effect retry succeeds."""
    return _reconcile_i10_leases(authority, admission_id, receipt) is not None


def reconcile_and_bind_i10_delivery(authority, admission_id, receipt):
    """Bind a source receipt only after exact lease reconciliation, rolling back partial reopen."""
    reopened = _reconcile_i10_leases(authority, admission_id, receipt)
    if reopened is None:
        return False
    if bind_terminal_receipt(authority, admission_id, receipt):
        return True
    _rollback_reopened(authority, reopened)
    return False


def callback_record_for_recovery(authority, row, admission_id, handle):
    """Build the callback witness for one authenticated managed-native I10 occurrence."""
    if not isinstance(row, dict) or row.get("admission_id") != admission_id:
        return None
    payload = row.get("payload")
    native = payload.get("native_text_v1") if isinstance(payload, dict) else None
    if row.get("status") != "terminal" or not isinstance(native, dict):
        return None
    if native.get("producer_process_mode") != "managed_turn":
        return None
    binding = _bindings(authority).get(admission_id)
    try:
        current = get_session_admission(authority.db, admission_id=admission_id)
    except (AttributeError, KeyError, LookupError, OSError, RuntimeError, sqlite3.Error,
            TypeError, ValueError):
        return None
    identity = _identity(current, authority)
    if (
        not isinstance(binding, dict)
        or binding.get("handle") is not handle
        or getattr(handle, "owner_class", None) != OWNER_CLASS
        or not isinstance(current, dict)
        or current.get("status") != "terminal"
        or identity is None
        or not _same_full_identity(binding.get("identity"), identity)
        or not _same_full_identity(_identity(row, authority), identity)
    ):
        return None
    try:
        _current_epoch(authority)
        _physical_owner(authority)
    except (RuntimeStoreError, AttributeError, OSError, sqlite3.Error, TypeError, ValueError):
        return None
    if _validate_handle(authority, handle) is None:
        return None
    child = getattr(authority, "_preservation_children", {}).get(admission_id)
    child_record = _coordinator_record(authority, child) if child is not None else None
    if (
        child is None
        or not _child_matches_binding(authority, admission_id, handle, child)
        or child_record is None
        or getattr(child_record, "state", None) not in {"accepted", "finalizing"}
    ):
        return None
    operation = getattr(handle, "owner_id", None)
    if not isinstance(operation, str) or not operation:
        return None
    if not isinstance(identity.source_digest, str) or not identity.source_digest:
        return None
    return {
        "handle": handle,
        "owner_class": OWNER_CLASS,
        "owner_id": operation,
        "profile_id": authority.profile_id,
        "authority_epoch": authority.epoch,
        "process_instance": authority.instance_id,
        "coordinator_id": getattr(handle, "coordinator_id", None),
        "session_id": current["target_session_id"],
        "principal_id": current["principal_id"],
        "admission_id": admission_id,
        "request_id": current["request_id"],
        "admission_request_id": current["request_id"],
        "generation": current.get("generation"),
        "operation": operation,
        "source_identity": identity.source_digest,
        "child_handle": child,
    }


def i10_pre_effect_retry_pending(authority, admission_id):
    """Recognize only the retained source-owned window before an I10 safe retry."""
    from gateway.session_ingress import DeliveryReceipt

    binding = _bindings(authority).get(admission_id)
    receipt = binding.get("source_delivery_receipt") if isinstance(binding, dict) else None
    if (
        not isinstance(receipt, DeliveryReceipt)
        or receipt.admission_id != admission_id
        or receipt.attempted is not True
        or receipt.delivered is not False
        or receipt.reason != "delivery_failed"
        or receipt.effect_state != "none"
        or not isinstance(binding, dict)
        or binding.get("retry_attempt") is not None
        or getattr(authority, "_preservation_delivery_retries", {}).get(admission_id, 0)
    ):
        return False
    try:
        row = get_session_admission(authority.db, admission_id=admission_id)
    except (AttributeError, KeyError, LookupError, OSError, RuntimeError, sqlite3.Error,
            TypeError, ValueError):
        return False
    identity = _identity(row, authority)
    handle = binding.get("handle")
    if (
        not isinstance(row, dict)
        or row.get("status") != "terminal"
        or identity is None
        or not _same_full_identity(binding.get("identity"), identity)
        or getattr(handle, "owner_class", None) != OWNER_CLASS
        or _validate_handle(authority, handle, allow_unknown=True) is None
    ):
        return False
    try:
        _current_epoch(authority)
        _physical_owner(authority)
    except (RuntimeStoreError, AttributeError, OSError, sqlite3.Error, TypeError, ValueError):
        return False
    child = getattr(authority, "_preservation_children", {}).get(admission_id)
    child_record = _coordinator_record(authority, child) if child is not None else None
    if (
        child is None
        or not _child_matches_binding(authority, admission_id, handle, child)
        or child_record is None
        or getattr(child_record, "state", None) != "unknown"
        or admission_id not in getattr(authority, "pending_deliveries", {})
        or admission_id not in getattr(authority, "held_turn_markers", {})
    ):
        return False
    payload = row.get("payload")
    native = payload.get("native_text_v1") if isinstance(payload, dict) else None
    return isinstance(native, dict) and native.get("producer_process_mode") == "managed_turn"


def i10_pre_effect_retry_marker_is_exact(authority, admission_id):
    """Allow the retry phase only while its own event still owns the retained marker."""
    if not i10_pre_effect_retry_pending(authority, admission_id):
        return False
    pending = getattr(authority, "pending_deliveries", {}).get(admission_id)
    held = getattr(authority, "held_turn_markers", {}).get(admission_id)
    if (not isinstance(pending, tuple) or len(pending) != 5
            or held is None or pending[1] is not held):
        return False
    adopters = getattr(authority, "marker_adopters", {})
    watchers = getattr(authority, "_preservation_marker_watchers", {})
    if not isinstance(adopters, dict) or not isinstance(watchers, dict):
        return False
    if adopters.get(admission_id) is not None or admission_id in watchers:
        return False
    return not any(
        getattr(held, field, None) is not None
        for field in (
            "_classic_custody_authority", "_classic_custody_key", "_classic_execution_bridge",
        )
    )


async def finish_no_sidecar_delivery(
        authority, admission_id, delivery_receipt, waiter_delivered, *,
        automation_binding, callback_requirement):
    """Finish direct delivery while keeping I10 reconciliation in this topical module."""
    from gateway.session_classic_delivery import (
        CLASSIC_CALLBACK_NOT_REQUIRED,
        CLASSIC_CALLBACK_UNKNOWN,
        classic_callback_requirement,
        wait_for_classic_post_delivery_callback,
    )
    from gateway.session_ingress import DeliveryReceipt, retry_preservation_delivery

    physical_delivery = (
        isinstance(delivery_receipt, DeliveryReceipt)
        and delivery_receipt.admission_id == admission_id
        and delivery_receipt.attempted is True
        and delivery_receipt.delivered is True
        and delivery_receipt.reason is None
        and delivery_receipt.effect_state == "delivered"
    )
    i10_binding = admission_id in getattr(authority, "_i10_recovery_bindings", {})
    reconciled_i10 = False
    if automation_binding:
        if not physical_delivery:
            return False
        if not authority._preservation_record_automation_terminal_witness(
                admission_id, delivery_receipt=delivery_receipt,
                waiter_delivered=waiter_delivered, finalizer_outcome="direct_delivery"):
            return False
        callback_requirement = classic_callback_requirement(authority, admission_id)
        if callback_requirement == CLASSIC_CALLBACK_UNKNOWN:
            return False
    if (
        not physical_delivery
        and i10_binding
        and getattr(delivery_receipt, "reason", None) == "delivery_failed"
        and getattr(delivery_receipt, "effect_state", None) == "none"
    ):
        retry_receipt = await retry_preservation_delivery(
            authority, admission_id, delivery_receipt.reason,
            effect_state=delivery_receipt.effect_state,
        )
        if not (
            isinstance(retry_receipt, DeliveryReceipt)
            and retry_receipt.delivered is True
            and retry_receipt.effect_state == "delivered"
        ):
            return False
        delivery_receipt = retry_receipt
        physical_delivery = True
        if not reconcile_and_bind_i10_delivery(authority, admission_id, delivery_receipt):
            return False
        reconciled_i10 = True
        callback_requirement = classic_callback_requirement(authority, admission_id)
        if callback_requirement == CLASSIC_CALLBACK_UNKNOWN:
            retain_i10_unknown_after_reconcile(authority, admission_id)
            return False
    if callback_requirement != CLASSIC_CALLBACK_NOT_REQUIRED:
        callback_settled = await wait_for_classic_post_delivery_callback(authority, admission_id)
        if not callback_settled:
            if reconciled_i10:
                retain_i10_unknown_after_reconcile(authority, admission_id)
            return False
    if physical_delivery and i10_binding and not reconciled_i10 and not reconcile_and_bind_i10_delivery(
            authority, admission_id, delivery_receipt):
        return False
    authority._preservation_release_terminal(admission_id, terminal_receipt=delivery_receipt)
    return True


def _child_matches_binding(authority, admission_id, parent, child):
    lease = getattr(child, "lease", None)
    parent_lease = getattr(parent, "lease", None)
    return (
        lease is not None
        and parent_lease is not None
        and getattr(child, "owner_class", None) == "I04_fifo_turn_execution"
        and getattr(child, "owner_id", None) == f"claim:{admission_id}"
        and getattr(child, "profile_id", None) == authority.profile_id
        and getattr(child, "authority_epoch", None) == authority.epoch
        and getattr(child, "process_instance", None) == authority.instance_id
        and getattr(lease, "parent_lease_id", None) == getattr(parent_lease, "lease_id", None)
        and _coordinator_record(authority, child) is not None
    )


def _child_terminalized(authority, child):
    coordinator = getattr(authority.runner, "runtime_custody", None)
    lease_id = getattr(getattr(child, "lease", None), "lease_id", None)
    records = getattr(coordinator, "_records", None)
    completed = getattr(coordinator, "_completed", None)
    lock = getattr(coordinator, "_lock", None)
    if coordinator is None or not isinstance(lease_id, str) or not isinstance(records, dict):
        return False
    if not isinstance(completed, dict) or lock is None:
        return False
    with lock:
        return lease_id not in records and completed.get(lease_id) == "terminal"


def _terminal_receipt_matches(receipt, admission_id, *, authority, binding, current):
    """Accept only an existing typed delivery/finalizer transition for this occurrence."""
    from gateway.delivery_ledger import DeliveryObligationProof
    from gateway.preservation_source_close import SourceFinalizerReceipt
    from gateway.session_contract import AdmissionReceipt
    from gateway.session_ingress import DeliveryReceipt

    if not _current_identity_matches(authority, binding, current):
        return False
    if isinstance(receipt, DeliveryReceipt):
        return (
            _bound_receipt(binding, receipt)
            and _source_delivery_receipt(binding, receipt)
            and receipt.admission_id == admission_id
            and receipt.attempted is True
            and receipt.delivered is True
            and receipt.reason is None
            and receipt.effect_state == "delivered"
        )
    if isinstance(receipt, SourceFinalizerReceipt):
        return (
            _bound_receipt(binding, receipt)
            and receipt.status == "success"
            and isinstance(receipt.native_occurrence_id, str)
            and bool(receipt.native_occurrence_id)
            and isinstance(receipt.finalizer_digest, str)
            and bool(receipt.finalizer_digest)
            and receipt.reason == ""
        )
    if isinstance(receipt, DeliveryObligationProof):
        return (
            _bound_receipt(binding, receipt)
            and receipt.state == "delivered"
            and bool(receipt.obligation_id)
        )
    # A canonical cancellation is a source-owned terminal refusal before producer effect.  A
    # completed admission row without that explicit refusal remains insufficient by itself.
    return (
        isinstance(receipt, AdmissionReceipt)
        and _bound_receipt(binding, receipt)
        and receipt.admission_id == admission_id
        and receipt.ref.profile_id == authority.profile_id
        and receipt.ref.session_id == current.get("target_session_id")
        and receipt.authority_epoch == (current.get("owner_epoch") or authority.epoch)
        and receipt.execution_generation in (None, current.get("generation"))
        and receipt.status == "terminal"
        and receipt.outcome in {"cancelled", "refused", "rejected"}
    )


def prepare_binding_retirement(authority, admission_id, *, terminal_receipt=None):
    """Re-read the exact canonical row and typed terminal proof before releasing an I10 root."""
    binding = _bindings(authority).get(admission_id)
    if not isinstance(binding, dict):
        return None
    handle = binding.get("handle")
    identity = binding.get("identity")
    if not isinstance(identity, RecoveryBindingIdentity) or _validate_handle(authority, handle) is None:
        return None
    current = get_session_admission(authority.db, admission_id=admission_id)
    current_identity = _identity(current, authority)
    consumers = binding.get("consumers")
    if ((isinstance(consumers, set) and "local_recovery" in consumers)
            or binding.get("consumer") == "local_recovery") and not any(
                _local_source_matches(authority, identity, receipt)
                for receipt in (binding.get("receipts") or ())):
        return None
    if (
        not isinstance(current, dict)
        or current.get("status") != "terminal"
        or not _same_full_identity(identity, current_identity)
        or getattr(handle, "owner_class", None) != OWNER_CLASS
    ):
        return None
    child = getattr(authority, "_preservation_children", {}).get(admission_id)
    if child is not None and not _child_matches_binding(authority, admission_id, handle, child):
        return None
    candidates = []
    if terminal_receipt is not None:
        candidates.append(terminal_receipt)
    candidates.extend(binding.get("receipts") or ())
    for receipt in candidates:
        if _terminal_receipt_matches(
                receipt, admission_id, authority=authority, binding=binding, current=current):
            return RecoveryRetirementProof(
                admission_id=admission_id,
                handle=handle,
                identity=current_identity,
                row=dict(current),
                terminal_receipt=receipt,
                receipt_digest=_json_digest(receipt),
                child=child,
            )
    return None


def validate_binding_retirement(authority, proof):
    """Recheck the proof immediately before coordinator terminalization."""
    if not isinstance(proof, RecoveryRetirementProof):
        return False
    binding = _bindings(authority).get(proof.admission_id)
    current = get_session_admission(authority.db, admission_id=proof.admission_id)
    current_identity = _identity(current, authority)
    if (
        not isinstance(binding, dict)
        or binding.get("handle") is not proof.handle
        or not isinstance(current, dict)
        or current.get("status") != "terminal"
        or not _same_full_identity(proof.identity, current_identity)
        or not _terminal_receipt_matches(
            proof.terminal_receipt, proof.admission_id,
            authority=authority, binding=binding, current=current,
        )
    ):
        return False
    children = getattr(authority, "_preservation_children", {})
    return (
        proof.child is None or _child_terminalized(authority, proof.child)
    ) and proof.admission_id not in children


def commit_binding_retirement(authority, proof):
    """Consume only the binding proven by ``prepare_binding_retirement``."""
    if not validate_binding_retirement(authority, proof):
        return False
    observations = getattr(authority, "_i10_consumer_evidence", {})
    for consumer, stored in tuple(observations.items()):
        receipts = stored if isinstance(stored, tuple) else (stored,)
        updated = []
        for receipt in receipts:
            if (
                isinstance(receipt, RecoveryConsumerReceipt)
                and receipt.identity is not None
                and receipt.identity.admission_id == proof.admission_id
            ):
                source_receipts = receipt.source_receipts
                if proof.terminal_receipt not in source_receipts:
                    source_receipts = source_receipts + (proof.terminal_receipt,)
                receipt = RecoveryConsumerReceipt(
                    receipt.schema, receipt.consumer, receipt.source, "terminal", receipt.identity,
                    receipt.handle, source_receipts, proof.receipt_digest, receipt.reason,
                    receipt.close_witness,
                )
            updated.append(receipt)
        if updated:
            observations[consumer] = updated[0] if len(updated) == 1 else tuple(updated)
    _bindings(authority).pop(proof.admission_id, None)
    return True


def prepare_i10_terminal_release(authority, admission_id, terminal_receipt):
    """Prepare one I10 proof, retaining UNKNOWN when the source proof is absent."""
    bindings = getattr(authority, "_i10_recovery_bindings", {})
    if not isinstance(bindings, dict) or admission_id not in bindings:
        return I10_NOT_BOUND
    proof = prepare_binding_retirement(
        authority, admission_id, terminal_receipt=terminal_receipt,
    )
    if proof is not None:
        return proof
    binding = _bindings(authority).get(admission_id)
    handle = binding.get("handle") if isinstance(binding, dict) else None
    coordinator = getattr(authority, "_preservation_coordinator", lambda: None)()
    if coordinator is not None and handle is not None:
        coordinator.mark_unknown(handle, "terminal_proof_missing")
    return None


def validate_i10_terminal_release(authority, proof):
    """Validate an I10 proof after child terminalization without changing generic release paths."""
    if proof is I10_NOT_BOUND:
        return True
    if validate_binding_retirement(authority, proof):
        return True
    coordinator = getattr(authority, "_preservation_coordinator", lambda: None)()
    if coordinator is not None and getattr(proof, "handle", None) is not None:
        coordinator.mark_unknown(proof.handle, "terminal_proof_missing")
    return False


def commit_i10_terminal_release(authority, proof):
    """Commit only a proof that survived the shared child-before-parent release fence."""
    if proof is I10_NOT_BOUND:
        return True
    if commit_binding_retirement(authority, proof):
        return True
    binding = _bindings(authority).get(proof.admission_id)
    if isinstance(binding, dict):
        binding["state"] = "unknown"
    return False


def _binding_for_row(authority, row, *, consumer=None):
    identity = _identity(row, authority)
    if identity is None:
        raise RuntimeStoreError("preservation_owner_unmapped")
    admission_id = identity.admission_id
    binding = _bindings(authority).get(admission_id)
    if not isinstance(binding, dict):
        return None
    handle = binding.get("handle")
    if _validate_handle(authority, handle, allow_unknown=True) is None:
        raise RuntimeStoreError("preservation_owner_unmapped")
    stored = binding.get("identity")
    if not _same_source_identity(stored, identity):
        raise RuntimeStoreError("preservation_owner_unmapped")
    if consumer is not None:
        consumers = binding.get("consumers")
        if isinstance(consumers, set):
            if consumer not in consumers:
                consumers.add(consumer)
        elif binding.get("consumer") not in {None, consumer}:
            raise RuntimeStoreError("preservation_owner_mismatch")
    return binding


def _adopt_native_inbound_root(authority, row, existing, *, consumer):
    """Transfer a committed queued I01 root to recovery before any execution effect."""
    if (
        getattr(existing, "owner_class", None) != "I01_native_inbound"
        or row.get("status") != "queued"
    ):
        raise RuntimeStoreError("preservation_owner_unmapped")
    identity = _identity(row, authority)
    if _validate_handle(authority, existing) is None:
        raise RuntimeStoreError("preservation_owner_unmapped")
    _current_epoch(authority)
    _physical_owner(authority)
    coordinator = getattr(authority.runner, "runtime_custody", None)
    if identity is None or coordinator is None:
        raise RuntimeStoreError("preservation_owner_unmapped")
    from gateway.runtime_custody import CustodyRefusal

    replacement = coordinator.reserve_root(
        OWNER_CLASS,
        f"recovery:{consumer}:{identity.admission_id}:{identity.request_id}",
        profile_id=authority.profile_id,
        authority_epoch=authority.epoch,
    )
    if isinstance(replacement, CustodyRefusal):
        raise RuntimeStoreError(replacement.code)
    current = get_session_admission(authority.db, admission_id=identity.admission_id)
    current_identity = _identity(current, authority)
    if (
        current_identity is None
        or not _same_full_identity(identity, current_identity)
        or not isinstance(current, dict)
        or current.get("status") != "queued"
    ):
        coordinator.mark_unknown(replacement, "unmapped_writer")
        raise RuntimeStoreError("preservation_owner_unmapped")
    refusal = coordinator.mark_terminal(existing)
    if refusal is not None:
        coordinator.mark_unknown(replacement, "recovery_handoff_failed")
        raise RuntimeStoreError("recovery_handoff_failed")
    authority._preservation_handles[identity.admission_id] = replacement
    _bindings(authority)[identity.admission_id] = {
        "handle": replacement,
        "identity": current_identity,
        "consumer": consumer,
        "consumers": {consumer},
        "profile_home": str(_home(authority)),
        "process_instance": authority.instance_id,
        "authority_epoch": authority.epoch,
        "state": "accepted",
        "receipts": (),
    }
    authority._i10_recovery_seen = True
    return replacement

def _reserve_recovery_root(authority, row, *, consumer):
    identity = _identity(row, authority)
    if identity is None:
        raise RuntimeStoreError("preservation_owner_unmapped")
    admission_id = identity.admission_id
    existing = getattr(authority, "_preservation_handles", {}).get(admission_id)
    if existing is not None:
        binding = _binding_for_row(authority, row, consumer=consumer)
        if binding is None:
            if (
                consumer == "local_recovery"
                and getattr(existing, "owner_class", None) == "I02_api_session_turn"
            ):
                from gateway.session_recovery_custody_roots import adopt_local_api_root
                return adopt_local_api_root(authority, row, existing)
            return _adopt_native_inbound_root(
                authority, row, existing, consumer=consumer,
            )
        if getattr(existing, "owner_class", None) != OWNER_CLASS:
            raise RuntimeStoreError("preservation_owner_unmapped")
        binding.setdefault("consumers", set()).add(consumer)
        binding["consumer"] = consumer
        return existing
    if row.get("status") != "queued":
        raise RuntimeStoreError("unknown_execution")
    _current_epoch(authority)
    _physical_owner(authority)
    coordinator = getattr(authority.runner, "runtime_custody", None)
    if coordinator is None:
        raise RuntimeStoreError("runtime_custody_unavailable")
    from gateway.runtime_custody import CustodyRefusal

    handle = coordinator.reserve_root(
        OWNER_CLASS,
        f"recovery:{consumer}:{identity.admission_id}:{identity.request_id}",
        profile_id=authority.profile_id,
        authority_epoch=authority.epoch,
    )
    if isinstance(handle, CustodyRefusal):
        raise RuntimeStoreError(handle.code)
    current = get_session_admission(authority.db, admission_id=admission_id)
    current_identity = _identity(current, authority)
    if (
        current_identity is None
        or not _same_full_identity(identity, current_identity)
        or current.get("status") != "queued"
    ):
        coordinator.release_unaccepted(handle)
        raise RuntimeStoreError("preservation_owner_unmapped")
    authority._preservation_handles[admission_id] = handle
    _bindings(authority)[admission_id] = {
        "handle": handle,
        "identity": current_identity,
        "consumer": consumer,
        "consumers": {consumer},
        "profile_home": str(_home(authority)),
        "process_instance": authority.instance_id,
        "authority_epoch": authority.epoch,
        "state": "accepted",
        "receipts": (),
    }
    authority._i10_recovery_seen = True
    return handle


def ensure_recovery_root(authority, row, *, consumer):
    """Reserve one exact I10 root for a public recovery consumer before its effect."""
    if consumer not in CONSUMERS:
        raise RuntimeStoreError("preservation_owner_mismatch")
    return _reserve_recovery_root(authority, row, consumer=consumer)


def observe_consumer_rows(authority, consumer, *, rows=None, reason="canonical recovery scan"):
    """Attach one consumer's real canonical admissions to the existing I10 handles.

    The caller invokes this after its producer/recovery operation.  A quiet source records an
    explicit unused observation; an accepted row without a bound handle is left unobserved so
    close remains UNKNOWN instead of turning a missing consumer into a clean result.
    """
    if (
        consumer not in CONSUMERS
        or authority is None
        or not isinstance(getattr(authority, "instance_id", None), str)
        or not isinstance(getattr(authority, "profile_id", None), str)
        or not isinstance(getattr(authority, "epoch", None), int)
        or not hasattr(authority, "db")
    ):
        return False
    if rows is None:
        with authority.db._read_ctx() as conn:
            admission_ids = [row[0] for row in conn.execute(
                "SELECT admission_id FROM session_admissions "
                "WHERE status IN ('queued','started','unknown') ORDER BY seq"
            )]
        rows = [
            row for admission_id in admission_ids
            if (row := get_session_admission(authority.db, admission_id=admission_id)) is not None
        ]
    relevant_rows = [
        row for row in rows
        if isinstance(row, dict)
        and isinstance(row.get("payload"), dict)
        and ("native_text_v1" in row["payload"] or "local_automation_v1" in row["payload"])
    ]
    active_rows = [row for row in relevant_rows if row.get("status") in {"queued", "started", "unknown"}]
    if relevant_rows and not active_rows:
        return False
    if not active_rows:
        return observe_consumer(authority, consumer, unused_consumer(authority, consumer, reason=reason))
    observed = False
    for row in active_rows:
        admission_id = row.get("admission_id")
        try:
            handle = getattr(authority, "_preservation_handles", {}).get(admission_id)
            if row.get("status") == "queued" and handle is None:
                handle = ensure_recovery_root(authority, row, consumer=consumer)
            if handle is None:
                continue
            admission_receipt = authority._receipt(row)
            state = "unknown" if row.get("status") == "unknown" else "active"
            receipt = consumer_receipt(
                authority, consumer, row=row, handle=handle,
                source_receipts=(admission_receipt,), state=state,
            )
        except (RuntimeStoreError, AttributeError, KeyError, TypeError, ValueError):
            continue
        observed = observe_consumer(authority, consumer, receipt) or observed
    return observed


def refresh_binding(authority, row):
    """Advance a bound row through its source-owned claim transition on the same handle."""
    identity = _identity(row, authority)
    if identity is None:
        raise RuntimeStoreError("preservation_owner_unmapped")
    binding = _bindings(authority).get(identity.admission_id)
    if not isinstance(binding, dict):
        return False
    if not _same_source_identity(binding.get("identity"), identity):
        handle = binding.get("handle")
        coordinator = getattr(authority.runner, "runtime_custody", None)
        if coordinator is not None and handle is not None:
            coordinator.mark_unknown(handle, "unmapped_writer")
        raise RuntimeStoreError("preservation_owner_unmapped")
    if row.get("status") not in {"started", "unknown", "terminal", "queued"}:
        raise RuntimeStoreError("preservation_owner_unmapped")
    binding["identity"] = identity
    binding["row"] = dict(row)
    return True


def consumer_receipt(
    authority, consumer, *, row=None, handle=None, source_receipts=(), state="active",
    finalizer_digest=None, reason=None, close_witness=None,
):
    """Build a receipt only from an existing typed source result and exact bound handle."""
    if consumer not in CONSUMERS or state not in _ACTIVE_STATES | _TERMINAL_STATES:
        raise RuntimeStoreError("preservation_owner_mismatch")
    if not isinstance(source_receipts, tuple):
        raise RuntimeStoreError("preservation_receipt_unmapped")
    if not _validate_source_receipts(source_receipts, consumer=consumer):
        raise RuntimeStoreError("preservation_receipt_unmapped")
    if row is None or handle is None:
        raise RuntimeStoreError("preservation_owner_unmapped")
    identity = _identity(row, authority)
    binding = _binding_for_row(authority, row, consumer=consumer)
    if identity is None or binding is None or binding.get("handle") is not handle:
        raise RuntimeStoreError("preservation_owner_unmapped")
    if _validate_handle(authority, handle, allow_unknown=state == "unknown") is None:
        raise RuntimeStoreError("preservation_owner_unmapped")
    if state in _TERMINAL_STATES and not finalizer_digest:
        raise RuntimeStoreError("preservation_receipt_unmapped")
    if close_witness is not None and not _valid_close_witness(authority, consumer, close_witness):
        raise RuntimeStoreError("preservation_receipt_unmapped")
    return RecoveryConsumerReceipt(
        _RECEIPT_SCHEMA, consumer, _SOURCE_BY_CONSUMER[consumer], state, identity, handle,
        source_receipts, finalizer_digest, reason, close_witness,
    )


def unused_consumer(authority, consumer, *, reason, close_witness=None):
    """Record source-owned absence, never inferred from a generic empty map."""
    if consumer not in CONSUMERS or not isinstance(reason, str) or not reason:
        raise RuntimeStoreError("preservation_owner_mismatch")
    return RecoveryConsumerAbsence(
        _ABSENCE_SCHEMA, consumer, _SOURCE_BY_CONSUMER[consumer], authority.instance_id,
        authority.epoch, str(_home(authority)), reason, close_witness,
    )



def _valid_close_witness(authority, consumer, witness, *, close_generation=None):
    from gateway.session_recovery_custody_sources import valid_close_witness
    return valid_close_witness(
        authority, consumer, witness, close_generation=close_generation,
    )


def _i10_close_fence_current(authority, close_generation):
    from gateway.session_recovery_custody_sources import close_fence_current
    return close_fence_current(authority, close_generation)


def register_i10_consumer_source(authority, consumer, probe: Callable):
    from gateway.session_recovery_custody_sources import register_consumer_source
    return register_consumer_source(authority, consumer, probe)


def _i10_source_registry(authority):
    from gateway.session_recovery_custody_sources import source_registry
    return source_registry(authority)


def _close_witness(authority, consumer, close_generation, source_generation, snapshot):
    from gateway.session_recovery_custody_sources import close_witness
    return close_witness(authority, consumer, close_generation, source_generation, snapshot)


def _attach_close_witness(authority, consumer, witness):
    from gateway.session_recovery_custody_sources import attach_close_witness
    return attach_close_witness(authority, consumer, witness)


def refresh_i10_source_evidence(authority, close_generation):
    from gateway.session_recovery_custody_sources import refresh_source_evidence
    return refresh_source_evidence(authority, close_generation)

def ensure_native_recovery_root(authority, row):
    """Retain one current I10 root for a queued ordinary native admission.

    The old row owner epoch is only a witness.  A current root requires the live profile lock and
    current runtime epoch; a started/unknown row can only reuse an already retained exact handle.
    """
    identity = _identity(row, authority)
    payload = row.get("payload") if isinstance(row, dict) else None
    if identity is None or not isinstance(payload, dict) or "native_text_v1" not in payload:
        raise RuntimeStoreError("preservation_owner_unmapped")
    admission_id = identity.admission_id
    existing = getattr(authority, "_preservation_handles", {}).get(admission_id)
    if existing is not None:
        binding = _bindings(authority).get(admission_id)
        if (
            binding is None
            and getattr(existing, "owner_class", None) == "I01_native_inbound"
            and row.get("status") == "queued"
        ):
            return _adopt_native_inbound_root(
                authority, row, existing, consumer="startup",
            )
        if not isinstance(binding, dict) or binding.get("handle") is not existing:
            raise RuntimeStoreError("preservation_owner_unmapped")
        if getattr(existing, "owner_class", None) not in {OWNER_CLASS, "I01_native_inbound"}:
            raise RuntimeStoreError("preservation_owner_unmapped")
        _current_epoch(authority)
        _physical_owner(authority)
        current = get_session_admission(authority.db, admission_id=admission_id)
        current_identity = _identity(current, authority)
        if current_identity is None or not _same_source_identity(binding.get("identity"), current_identity):
            coordinator = getattr(authority.runner, "runtime_custody", None)
            if coordinator is not None:
                coordinator.mark_unknown(existing, "unmapped_writer")
            raise RuntimeStoreError("preservation_owner_unmapped")
        if not _validate_handle(authority, existing, allow_unknown=True):
            raise RuntimeStoreError("preservation_owner_unmapped")
        binding["identity"] = current_identity
        binding["row"] = dict(current)
        authority._i10_recovery_seen = True
        return existing
    return _reserve_recovery_root(authority, row, consumer="startup")


def claim_requires_parent(authority, row):
    if "native_text_v1" in (row.get("payload") or {}):
        return True
    descriptor = authority._preservation_automation_row_descriptor(row)
    return isinstance(descriptor, dict) and descriptor.get("owner_class") in {
        "I07_automation_route_wakes", "I09_a2a_hosted_kanban",
    }


def recover_claim_parent(authority, row, parent):
    """Resolve the exact parent for the existing FIFO claim path."""
    if parent is not None:
        return parent
    if "native_text_v1" in (row.get("payload") or {}):
        ensure_native_recovery_root(authority, row)
    else:
        descriptor = authority._preservation_automation_row_descriptor(row)
        if not isinstance(descriptor, dict) or descriptor.get("owner_class") not in {
            "I07_automation_route_wakes", "I09_a2a_hosted_kanban",
        }:
            return None
        authority._preservation_recover_automation(row)
    return authority._preservation_handles.get(row.get("admission_id"))


def _bind_local_automation_route(authority, sid, source, route):
    """Publish a LOCAL route only when an existing live source and route match."""
    from gateway.session_authority import LiveSession

    live = authority.sessions.get(sid)
    if live is None:
        authority.sessions[sid] = LiveSession(source, route)
        return
    try:
        same_source = live.source is not None and live.source.to_dict() == source.to_dict()
    except (AttributeError, TypeError, ValueError):
        same_source = False
    if not same_source or live.route != route:
        raise RuntimeStoreError("admission_conflict")


def _local_automation_route(authority, sid, available_source):
    """Resolve one persisted LOCAL automation row through its canonical session entry."""
    route = authority.runner.session_store._generate_session_key(available_source)
    entry = authority.runner.session_store.lookup_by_session_key(route)
    if (
        entry is None
        or entry.origin is None
        or entry.origin.to_dict() != available_source.to_dict()
        or authority.logical_owner(entry.session_id) != sid
    ):
        raise RuntimeStoreError("admission_conflict")
    _bind_local_automation_route(authority, sid, entry.origin, route)
    return entry.origin, route


def _queued_local_automation_identity(authority, row):
    """Read the current queued identity and physical owner before route effects."""
    status = row.get("status")
    if status != "queued":
        reason = "unknown_execution" if status in {"started", "unknown"} else "preservation_owner_unmapped"
        raise RuntimeStoreError(reason)
    current = get_session_admission(authority.db, admission_id=row.get("admission_id"))
    current_identity = _identity(current, authority)
    if (not isinstance(current, dict) or current.get("status") != "queued"
            or not _same_full_identity(_identity(row, authority), current_identity)):
        raise RuntimeStoreError("preservation_owner_unmapped")
    return current_identity


def _existing_local_automation_owner(authority, row, handle):
    """Reuse an exact existing automation root; this path never mints a successor."""
    if (getattr(handle, "owner_class", None) not in {
            "I07_automation_route_wakes", "I08_goals_loops_heartbeat", "I09_a2a_hosted_kanban"}
            or _validate_handle(authority, handle) is None):
        raise RuntimeStoreError("preservation_owner_unmapped")
    if authority._preservation_recover_existing_automation(row) is not handle:
        raise RuntimeStoreError("preservation_owner_unmapped")


def _validate_i10_local_binding(authority, row, binding, handle, current_identity):
    """Require the canonical binding and original accepted opaque I10 handle."""
    if (not isinstance(binding, dict) or binding.get("handle") is not handle
            or getattr(handle, "owner_class", None) != OWNER_CLASS
            or binding.get("profile_home") != str(_home(authority))
            or binding.get("process_instance") != authority.instance_id
            or binding.get("authority_epoch") != authority.epoch):
        raise RuntimeStoreError("preservation_owner_unmapped")
    if (_binding_for_row(authority, row) is not binding
            or _validate_handle(authority, handle) is None
            or not _same_full_identity(binding.get("identity"), current_identity)):
        raise RuntimeStoreError("preservation_owner_unmapped")


def validate_local_automation_recovery(authority, row):
    """Authenticate a LOCAL automation owner before route/store publication."""
    payload = row.get("payload") if isinstance(row, dict) else None
    if not isinstance(payload, dict) or "local_automation_v1" not in payload:
        return None
    current_identity = _queued_local_automation_identity(authority, row)
    admission_id = row.get("admission_id")
    bindings = getattr(authority, "_i10_recovery_bindings", None)
    handles = getattr(authority, "_preservation_handles", None)
    binding = bindings.get(admission_id) if isinstance(bindings, dict) else None
    handle = handles.get(admission_id) if isinstance(handles, dict) else None
    if binding is None:
        _existing_local_automation_owner(authority, row, handle)
        owner = "existing"
    else:
        _validate_i10_local_binding(authority, row, binding, handle, current_identity)
        owner = "i10"
    _current_epoch(authority)
    _physical_owner(authority)
    authority._require_admission_open()
    return owner


def _is_i10_local_automation(authority, row):
    """Recognize I10 only after the same current-owner validation used before restoration."""
    return validate_local_automation_recovery(authority, row) == "i10"


async def recover_native_sessions(authority, bindings):
    """Run the public native-recovery producer through the exact I10 root."""
    from collections import Counter
    from gateway.session_contract import SessionRef
    from gateway.session_envelope import check_native_route
    from hermes_state_runtime import list_session_admissions

    bindings = list(bindings)
    counts = Counter(sid for sid, _, _ in bindings)
    results = {}
    for sid, available_source, adapter in bindings:
        try:
            if counts[sid] != 1:
                raise RuntimeStoreError("admission_conflict")
            rows = list_session_admissions(authority.db, session_id=sid, pending_only=False)

            def recognized_automation(row):
                payload = row.get("payload") or {}
                principal = row.get("principal_id")
                return (
                    row.get("status") == "queued"
                    and isinstance(principal, str)
                    and principal.startswith("automation:")
                    and ("local_automation_v1" in payload or "native_text_v1" in payload)
                )

            native = [row for row in rows if "native_text_v1" in row.get("payload", {})]
            local_automation = [
                row for row in rows if "local_automation_v1" in row.get("payload", {})
            ]
            if not native and not local_automation:
                raise RuntimeStoreError("not_found")
            if any(row["status"] in {"started", "unknown"} for row in rows):
                raise RuntimeStoreError("unknown_execution")
            target = authority.physical_target(SessionRef(authority.profile_id, sid)) if native else None
            last_native = native[-1] if native else local_automation[-1]
            for row in rows:
                if row["status"] != "queued":
                    continue
                local_owner = validate_local_automation_recovery(authority, row)
                if recognized_automation(row) and local_owner not in {"i10", "existing"}:
                    recovered = authority._preservation_recover_automation(row)
                    if recovered is None:
                        raise RuntimeStoreError("preservation_owner_unmapped")
            if native:
                source, route = await check_native_route(
                    authority.runner, last_native["payload"], target, available_source, adapter,
                )
            else:
                source, route = _local_automation_route(authority, sid, available_source)
            for row in rows:
                if row["status"] != "queued":
                    continue
                payload = row["payload"]
                if "native_text_v1" in payload:
                    if not recognized_automation(row):
                        ensure_native_recovery_root(authority, row)
                    await check_native_route(
                        authority.runner, row["payload"], target, available_source, adapter,
                    )
                elif "local_automation_v1" in payload:
                    from gateway.session_automation import check_local_automation
                    check_local_automation(authority, SessionRef(authority.profile_id, sid), row, route=route)
                else:
                    from gateway.session_operator import check_local_input
                    check_local_input(authority, SessionRef(authority.profile_id, sid), row, source=source)
            authority._require_admission_open()
            from gateway.session_authority import LiveSession
            authority.sessions.setdefault(sid, LiveSession(source, route))
            if any(row["status"] == "unknown" for row in rows):
                raise RuntimeStoreError("unknown_execution")
            authority._schedule(SessionRef(authority.profile_id, sid))
            observe_consumer_rows(
                authority, "startup", rows=rows,
                reason="native recovery accepted no pending row",
            )
            results[sid] = "ready"
        except RuntimeStoreError as exc:
            results[sid] = exc.reason
    return results


def observe_consumer(authority, consumer, receipt):
    """Record a validated source receipt or source-owned unused observation.

    The historical call shape is retained for callers, but an arbitrary dict is deliberately
    rejected.  A caller must provide ``RecoveryConsumerReceipt`` or ``RecoveryConsumerAbsence``
    created by the source path above.
    """
    if authority is None or consumer not in CONSUMERS:
        return False
    if isinstance(receipt, RecoveryConsumerAbsence):
        if (
            receipt.schema != _ABSENCE_SCHEMA
            or receipt.consumer != consumer
            or receipt.source != _SOURCE_BY_CONSUMER[consumer]
            or receipt.process_instance != authority.instance_id
            or receipt.authority_epoch != authority.epoch
            or receipt.profile_home != str(_home(authority))
        ):
            return False
        if receipt.close_witness is not None and not _valid_close_witness(
                authority, consumer, receipt.close_witness):
            return False
    elif isinstance(receipt, RecoveryConsumerReceipt):
        if (
            receipt.schema != _RECEIPT_SCHEMA
            or receipt.consumer != consumer
            or receipt.source != _SOURCE_BY_CONSUMER[consumer]
            or receipt.identity is None
            or receipt.identity.profile_id != authority.profile_id
            or receipt.identity.authority_epoch != authority.epoch
            or receipt.identity.process_instance != authority.instance_id
            or not _validate_source_receipts(receipt.source_receipts, consumer=receipt.consumer)
        ):
            return False
        if receipt.close_witness is not None and not _valid_close_witness(
                authority, consumer, receipt.close_witness):
            return False
        binding = _bindings(authority).get(receipt.identity.admission_id)
        if (
            not isinstance(binding, dict)
            or binding.get("handle") is not receipt.handle
            or not _same_full_identity(binding.get("identity"), receipt.identity)
            or _validate_handle(authority, receipt.handle, allow_unknown=receipt.state == "unknown") is None
        ):
            return False
        if receipt.state in _TERMINAL_STATES and not receipt.finalizer_digest:
            return False
    else:
        return False
    observations = getattr(authority, "_i10_consumer_evidence", None)
    if observations is None:
        observations = authority._i10_consumer_evidence = {}
    previous = observations.get(consumer)
    if previous is None:
        observations[consumer] = receipt
    elif isinstance(previous, tuple):
        observations[consumer] = previous + (receipt,)
    elif previous is not receipt:
        observations[consumer] = (previous, receipt)
    authority._i10_recovery_seen = True
    if isinstance(receipt, RecoveryConsumerReceipt):
        binding = _bindings(authority).get(receipt.identity.admission_id)
        if isinstance(binding, dict):
            binding.setdefault("consumers", set()).add(receipt.consumer)
            binding["state"] = receipt.state
            existing = binding.get("receipts")
            retained = existing if isinstance(existing, tuple) else ()
            for source_receipt in receipt.source_receipts:
                if not any(stored is source_receipt for stored in retained):
                    retained += (source_receipt,)
            binding["receipts"] = retained
            binding["finalizer_digest"] = receipt.finalizer_digest
    return True


def owner_ready(authority):
    """Return true only after every source-owned I10 consumer has settled or recorded absence."""
    coordinator = getattr(authority.runner, "runtime_custody", None)
    if coordinator is None:
        return False
    try:
        status = coordinator.status()
        if OWNER_CLASS in status.get("active_owner_classes", ()):
            return False
        if OWNER_CLASS in status.get("unknown_owner_classes", ()):
            return False
        _physical_owner(authority)
        _current_epoch(authority)
        pending = authority.db._read_one(
            "SELECT 1 FROM session_admissions WHERE status IN ('queued','started','unknown') LIMIT 1"
        )
    except BaseException:  # health: allow BLE001 -- failed close-time I10 observation retains custody
        return False
    if pending is not None or _bindings(authority):
        return False
    observations = getattr(authority, "_i10_consumer_evidence", None)
    if not isinstance(observations, dict):
        return False
    for consumer in CONSUMERS:
        if consumer not in observations:
            # A missing source observation is unknown.  An empty map or a never-set seen flag is
            # not an absence receipt: every registered consumer must report its own intake scan.
            return False
        stored = observations[consumer]
        receipts = stored if isinstance(stored, tuple) else (stored,)
        if not receipts or any(receipt is None for receipt in receipts):
            return False
        for receipt in receipts:
            if isinstance(receipt, RecoveryConsumerAbsence):
                if (
                    receipt.schema != _ABSENCE_SCHEMA
                    or receipt.consumer != consumer
                    or receipt.source != _SOURCE_BY_CONSUMER[consumer]
                    or receipt.process_instance != authority.instance_id
                    or receipt.authority_epoch != authority.epoch
                    or receipt.profile_home != str(_home(authority))
                    or not isinstance(receipt.reason, str)
                    or not receipt.reason
                ):
                    return False
                if receipt.close_witness is not None and not _valid_close_witness(
                        authority, consumer, receipt.close_witness):
                    return False
                continue
            if (
                not isinstance(receipt, RecoveryConsumerReceipt)
                or receipt.schema != _RECEIPT_SCHEMA
                or receipt.consumer != consumer
                or receipt.source != _SOURCE_BY_CONSUMER[consumer]
                or not isinstance(receipt.identity, RecoveryBindingIdentity)
                or receipt.identity.profile_id != authority.profile_id
                or receipt.identity.authority_epoch != authority.epoch
                or receipt.identity.process_instance != authority.instance_id
                or receipt.state not in _TERMINAL_STATES
            ):
                return False
            if receipt.close_witness is not None and not _valid_close_witness(
                    authority, consumer, receipt.close_witness):
                return False
            terminal_check = getattr(coordinator, "is_terminal_handle", None)
            if receipt.handle is None or not callable(terminal_check):
                return False
            try:
                same_handle = terminal_check(
                    receipt.handle,
                    owner_class=OWNER_CLASS,
                    profile_id=authority.profile_id,
                    authority_epoch=authority.epoch,
                )
            except (AttributeError, RuntimeError, TypeError, ValueError):
                return False
            if same_handle is not True:
                return False
            if not receipt.finalizer_digest or not _validate_source_receipts(
                    receipt.source_receipts, consumer=receipt.consumer):
                return False
    return True



def owner_evidence(
        authority, *, process_instance, authority_epoch, close_generation, owner_id,
        require_close_witness=False,
):
    from gateway.session_recovery_custody_sources import owner_evidence as build_owner_evidence
    return build_owner_evidence(
        authority,
        process_instance=process_instance,
        authority_epoch=authority_epoch,
        close_generation=close_generation,
        owner_id=owner_id,
        require_close_witness=require_close_witness,
    )
