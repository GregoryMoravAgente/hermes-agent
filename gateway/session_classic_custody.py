"""Custody seams for classic adapter producers.

The classic adapter predates the durable FIFO.  This module binds its in-process slot, bounded
FIFO, and text debounce objects to the already authoritative ``RuntimeCustodyCoordinator``.  The
small records kept on ``SessionAuthority._preservation_operations`` are indexes of those opaque
handles; they are not a second ledger and never stand in for a durable admission row.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Optional
import uuid

from hermes_state_runtime import RuntimeStoreError


_CLASSIC_OWNER = 'I06_busy_followup_delivery'
_CLASSIC_PRODUCERS = frozenset({'running-slot', 'fifo', 'debounce'})
_CLASSIC_STATES = frozenset({'reserved', 'running', 'queued', 'debounced', 'handoff', 'unknown'})
_PARENT_UNAVAILABLE = object()
_CLASSIC_IDENTITY_FIELDS = (
    'owner_class', 'profile_id', 'profile_home', 'process_instance', 'authority_epoch',
    'session_id', 'principal_id', 'request_id', 'source_identity', 'producer', 'occurrence_id',
)


@dataclass(frozen=True)
class ClassicStreamDeliveryWitness:
    """Typed witness from the stream consumer after it matched the exact final payload."""

    session_key: str
    run_generation: Optional[int]
    message_id: Optional[str]
    payload_digest: str


@dataclass(frozen=True)
class ClassicWriterBinding:
    """Exact direct-I06 writer carrier; it is a reference to the existing custody record."""

    authority: Any
    coordinator: Any
    handle: Any
    operation_key: str
    parent_handle: Any
    owner_class: str
    owner_id: str
    profile_id: str
    profile_home: str
    process_instance: str
    authority_epoch: int
    session_id: str
    principal_id: str
    request_id: str
    occurrence_id: str
    source_identity: str
    producer: str
    session_key: str


@dataclass(frozen=True)
class ClassicExecutionReceipt:
    """Durable transcript/writer and delivery witnesses for one classic occurrence."""

    session_id: str
    session_key: str
    identity: tuple[Any, ...]
    transcript_rows: tuple[Any, ...] = ()
    writer_proof: Any = None
    writer_binding: Any = None
    delivery_proof: Any = None
    stream_witness: Optional[ClassicStreamDeliveryWitness] = None


@dataclass
class ClassicExecutionEventBridge:
    """Private one-use carrier linking the admitted owner event to its restored execution event.

    This is an identity witness, not a ledger: durable row/proof and delivery evidence remain
    owned by their existing writers.  The bridge is never placed in an agent result or row.
    """

    owner_event: Any
    execution_event: Any
    admission_id: str
    generation: Optional[int]
    session_id: str
    principal_id: str
    request_id: str
    occurrence_id: str
    operation_key: Optional[str] = None
    terminal_inbound_id: Optional[str] = None
    post_delivery_callback: Any = None
    # Exact Base-owned occurrence handle; never a mutable adapter dispatch owner.
    post_delivery_callback_handle: Any = None
    post_delivery_callback_expected: bool = False
    post_delivery_callback_pending: bool = False
    post_delivery_callback_authority: bool = False
    post_delivery_callback_done: Any = None
    post_delivery_callback_executed: bool = False
    post_delivery_callback_settled: bool = False
    post_delivery_callback_state: str = 'unclassified'
    post_delivery_callback_expectation: Any = None
    post_delivery_callback_delivery_witness: Any = None
    post_delivery_callback_settlement: Any = None
    # A terminal I06 row may retire from the operation index before the existing busy finalizer
    # performs its durable readback. Keep that exact authenticated record on the one-use bridge;
    # this is a witness carrier, not a second ownership index.
    retired_operation_record: Any = None


def _profile_home(authority):
    try:
        value = Path(authority.db.db_path).parent.resolve()
    except (AttributeError, TypeError, ValueError, OSError):
        raise RuntimeStoreError('preservation_owner_unmapped') from None
    return str(value)


def _source_payload(source):
    if source is None:
        return None
    to_dict = getattr(source, 'to_dict', None)
    value = to_dict() if callable(to_dict) else vars(source)
    if not isinstance(value, dict):
        return None
    return {str(key): value[key] for key in sorted(value) if key != 'message_id'}


def _canonical_session_key(authority, source):
    store = getattr(authority.runner, 'session_store', None)
    if store is None or source is None:
        return None
    try:
        return store._generate_session_key(source)
    except (AttributeError, KeyError, TypeError, ValueError, OSError):
        return None


def _classic_session_id(authority, session_key, event):
    store = getattr(authority.runner, 'session_store', None)
    if store is None or not isinstance(session_key, str) or not session_key:
        return None
    try:
        entry = store.lookup_by_session_key(session_key)
    except (AttributeError, KeyError, TypeError, ValueError):
        entry = None
    session_id = getattr(entry, 'session_id', None)
    if entry is not None and getattr(entry, 'session_key', session_key) != session_key:
        return None
    return session_id if isinstance(session_id, str) and session_id else None


def _classic_principal(source):
    if source is None:
        return None
    principal = getattr(source, 'user_id', None) or getattr(source, 'chat_id', None)
    return str(principal).strip() if principal is not None and str(principal).strip() else None


def _classic_admission_principal(source):
    """Return the server-owned principal namespace used by a native admission row."""
    return 'messaging:' + json.dumps([
        getattr(source, 'profile', None),
        getattr(getattr(source, 'platform', None), 'value', getattr(source, 'platform', None)),
        getattr(source, 'chat_id', None), getattr(source, 'thread_id', None),
        getattr(source, 'user_id', None),
    ], separators=(',', ':'))


def _classic_request_id(event, source_payload):
    message_id = getattr(event, 'message_id', None)
    if isinstance(message_id, str) and message_id.strip():
        return message_id.strip()
    if source_payload is None:
        return None
    occurrence = getattr(event, '_classic_occurrence_nonce', None)
    if not isinstance(occurrence, str) or not occurrence:
        occurrence = uuid.uuid4().hex
        event._classic_occurrence_nonce = occurrence
    return 'classic-' + occurrence


def _classic_source_identity(authority, event, session_key, producer, session_id):
    if producer not in _CLASSIC_PRODUCERS:
        raise RuntimeStoreError('invalid_params')
    source = getattr(event, 'source', None)
    source_payload = _source_payload(source)
    principal = _classic_admission_principal(source)
    request_id = _classic_request_id(event, source_payload)
    if source_payload is None or principal is None or request_id is None:
        raise RuntimeStoreError('preservation_owner_unmapped')
    profile_id = authority.profile_id
    instance_id = authority.instance_id
    epoch = authority.epoch
    home = _profile_home(authority)
    source_profile = getattr(source, 'profile', None)
    registry = getattr(authority.runner, 'session_authorities', None)
    served_name = registry.profile_name(authority) if registry is not None else None
    accepted_profiles = {profile_id}
    if isinstance(served_name, str) and served_name:
        accepted_profiles.add(served_name)
    if source_profile is not None and source_profile not in accepted_profiles:
        raise RuntimeStoreError('preservation_owner_unmapped')
    if (not isinstance(profile_id, str) or not profile_id or not isinstance(instance_id, str)
            or not instance_id or type(epoch) is not int or epoch < 1):
        raise RuntimeStoreError('preservation_owner_unmapped')
    if session_id is not None and (not isinstance(session_id, str) or not session_id):
        raise RuntimeStoreError('preservation_owner_unmapped')
    source_identity = hashlib.sha256(json.dumps({
        'producer': producer,
        'profile': profile_id,
        'home': home,
        'instance': instance_id,
        'epoch': epoch,
        'session': session_id,
        'principal': principal,
        'request': request_id,
        'source': source_payload,
    }, sort_keys=True, separators=(',', ':'), default=str).encode('utf-8')).hexdigest()
    return {
        'owner_class': _CLASSIC_OWNER,
        'profile_id': profile_id,
        'profile_home': home,
        'process_instance': instance_id,
        'authority_epoch': epoch,
        'session_id': session_id,
        'principal_id': principal,
        'request_id': request_id,
        'source_identity': source_identity,
        'producer': producer,
        'occurrence_id': request_id,
        'session_key': session_key,
    }


def classic_identity(authority, event, session_key, producer):
    """Build and validate the source-owned identity for one classic occurrence.

    ``event.metadata`` is deliberately excluded.  It is ingress data and cannot authenticate a
    profile, parent, epoch, or producer occurrence.  The active authority and the event's real
    source supply those values; a source message id or process-local occurrence nonce makes a
    distinct occurrence distinct.
    """
    session_id = _classic_session_id(authority, session_key, event)
    if session_id is None:
        raise RuntimeStoreError('preservation_owner_unmapped')
    return _classic_source_identity(authority, event, session_key, producer, session_id)


def _valid_parent(authority, parent):
    coordinator = authority._preservation_coordinator()
    return bool(
        coordinator is not None and parent is not None
        and getattr(parent, 'profile_id', None) == authority.profile_id
        and getattr(parent, 'authority_epoch', None) == authority.epoch
        and getattr(parent, 'process_instance', None) == authority.instance_id
        and getattr(parent, 'coordinator_id', None) == getattr(coordinator, '_coordinator_id', None)
        and getattr(parent, 'owner_class', None) in {
            'I04_fifo_turn_execution', 'I07_automation_route_wakes',
        }
        and coordinator._parent(parent) is not None
    )


def _exact_parent(authority, identity):
    """Use a real I04/I07 parent when the current execution exposes one."""
    live = authority.sessions.get(identity['session_id'])
    execution = getattr(getattr(live, 'event_stream', None), 'execution', None) or {}
    admission_id = execution.get('admission_id')
    if not admission_id:
        return None
    # A shared session's live source is the currently executing sender, not necessarily the sender
    # whose new busy occurrence is being admitted.  Use the already authenticated occurrence
    # principal and retain the durable row/epoch/profile checks in _preservation_operation_parent.
    principal_id = identity.get('principal_id')
    if not isinstance(principal_id, str) or not principal_id:
        return _PARENT_UNAVAILABLE
    parent = authority._preservation_operation_parent(
        identity['session_id'], admission_id=admission_id,
        generation=execution.get('execution_generation'),
        principal_id=principal_id,
        shared_group_principal=(
            getattr(getattr(live, 'source', None), 'chat_type', None) == 'group'
            and getattr(getattr(authority, 'runner', None), 'config', None) is not None
            and getattr(authority.runner.config, 'group_sessions_per_user', True) is False
        ))
    return parent if _valid_parent(authority, parent) else _PARENT_UNAVAILABLE


def _classic_parent_for_identity(authority, identity):
    """Return a real parent, ``None`` for an idle route, or a refusal sentinel.

    An active execution whose parent cannot be read must never become a fresh classic root.  That
    would let a stale/foreign I04 or I07 owner race the same session with an unrelated I06 lease.
    """
    parent = _exact_parent(authority, identity)
    if parent is _PARENT_UNAVAILABLE:
        raise RuntimeStoreError('preservation_owner_unmapped')
    return parent


def _identity_matches(record, identity):
    stored = record.get('classic_identity') if isinstance(record, dict) else None
    return isinstance(stored, dict) and all(stored.get(key) == identity.get(key) for key in _CLASSIC_IDENTITY_FIELDS)


def _classic_record_ref(event):
    authority = getattr(event, '_classic_custody_authority', None)
    key = getattr(event, '_classic_custody_key', None)
    records = getattr(authority, '_preservation_operations', None)
    if authority is None or not isinstance(key, str) or not isinstance(records, dict):
        return None
    record = records.get(key)
    return (authority, key, record) if isinstance(record, dict) else None


def _classic_record_identity(event) -> Optional[dict[str, Any]]:
    event = classic_execution_owner_for_event(event)
    reference = _classic_record_ref(event)
    record = reference[2] if reference is not None else None
    identity = record.get('classic_identity') if isinstance(record, dict) else None
    return dict(identity) if isinstance(identity, dict) else None


def _active_classic_handle(coordinator, handle):
    """Return the live coordinator record only for the original opaque handle."""
    records = getattr(coordinator, '_records', None)
    lock = getattr(coordinator, '_lock', None)
    lease_id = getattr(getattr(handle, 'lease', None), 'lease_id', None)
    if not isinstance(records, dict) or lock is None or not isinstance(lease_id, str):
        return None
    with lock:
        live = records.get(lease_id)
        if (live is None or getattr(live, 'handle', None) is not handle
                or getattr(live, 'state', None) not in {'accepted', 'finalizing'}):
            return None
        return live


def _classic_direct_writer_binding_valid(
    binding, identity: dict[str, Any], expected_record=None, *, allow_terminal=False,
) -> bool:
    if not isinstance(binding, ClassicWriterBinding) or not isinstance(identity, dict):
        return False
    authority = binding.authority
    coordinator_getter = getattr(authority, '_preservation_coordinator', None)
    coordinator = coordinator_getter() if callable(coordinator_getter) else None
    if coordinator is None or coordinator is not binding.coordinator:
        return False
    records = getattr(authority, '_preservation_operations', None)
    record = records.get(binding.operation_key) if isinstance(records, dict) else None
    if (not isinstance(record, dict) or (expected_record is not None and record is not expected_record)
            or record.get('owner_class') != _CLASSIC_OWNER
            or record.get('handle') is not binding.handle
            or record.get('parent_handle') is not binding.parent_handle
            or record.get('unknown') is True
            or record.get('state') not in _CLASSIC_STATES - {'unknown'}
            or not _identity_matches(record, identity)):
        return False
    admission_id = record.get('admission_id')
    if admission_id is not None:
        try:
            from hermes_state_runtime import get_session_admission
            row = get_session_admission(authority.db, admission_id=admission_id)
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            row = None
        if row is None or not _classic_admission_row_matches(
            authority, record, row, allow_terminal=allow_terminal,
        ):
            return False
    try:
        authority_home = _profile_home(authority)
    except RuntimeStoreError:
        return False
    if (getattr(authority, 'profile_id', None) != binding.profile_id
            or getattr(authority, 'instance_id', None) != binding.process_instance
            or getattr(authority, 'epoch', None) != binding.authority_epoch
            or authority_home != binding.profile_home
            or not isinstance(binding.operation_key, str)
            or not isinstance(binding.owner_id, str)
            or not binding.owner_id):
        return False
    handle = binding.handle
    if (getattr(handle, 'owner_class', None) != _CLASSIC_OWNER
            or getattr(handle, 'owner_id', None) != binding.owner_id
            or getattr(handle, 'profile_id', None) != binding.profile_id
            or getattr(handle, 'authority_epoch', None) != binding.authority_epoch
            or getattr(handle, 'process_instance', None) != binding.process_instance
            or getattr(handle, 'coordinator_id', None) != getattr(coordinator, '_coordinator_id', None)):
        return False
    live = _active_classic_handle(coordinator, handle)
    if live is None:
        return False
    expected_fields = (
        'owner_class', 'profile_id', 'profile_home', 'process_instance', 'authority_epoch',
        'session_id', 'principal_id', 'request_id', 'occurrence_id', 'source_identity',
        'producer', 'session_key',
    )
    for field in expected_fields:
        value = identity.get(field)
        if field == 'authority_epoch':
            if type(value) is not int or value < 1:
                return False
        elif not isinstance(value, str) or not value:
            return False
        if getattr(binding, field, None) != value:
            return False
    return True


def classic_writer_binding_for_event(event) -> Optional[ClassicWriterBinding]:
    """Carry a direct classic root's real custody handle into the canonical writer.

    A valid I04/I07 parent deliberately returns ``None`` so the existing managed ContextVar
    binding remains authoritative.  A malformed or stale classic record raises a typed refusal;
    it must not fall through to an ambient managed binding.
    """
    event = classic_execution_owner_for_event(event)
    reference = _classic_record_ref(event)
    if reference is None:
        return None
    authority, key, record = reference
    identity = record.get('classic_identity')
    if (not isinstance(identity, dict) or record.get('producer_object') is not event
            or not _identity_matches(record, identity)):
        raise RuntimeStoreError('preservation_owner_unmapped')
    coordinator_getter = getattr(authority, '_preservation_coordinator', None)
    coordinator = coordinator_getter() if callable(coordinator_getter) else None
    handle = record.get('handle')
    if coordinator is None or _active_classic_handle(coordinator, handle) is None:
        raise RuntimeStoreError('custody_unknown')
    parent = record.get('parent_handle')
    if parent is not None:
        if not _valid_parent(authority, parent):
            raise RuntimeStoreError('preservation_owner_unmapped')
        return None
    binding = ClassicWriterBinding(
        authority=authority, coordinator=coordinator, handle=handle, operation_key=key,
        parent_handle=None, owner_class=record.get('owner_class'),
        owner_id=getattr(handle, 'owner_id', None), profile_id=identity.get('profile_id'),
        profile_home=identity.get('profile_home'), process_instance=identity.get('process_instance'),
        authority_epoch=identity.get('authority_epoch'), session_id=identity.get('session_id'),
        principal_id=identity.get('principal_id'), request_id=identity.get('request_id'),
        occurrence_id=identity.get('occurrence_id'), source_identity=identity.get('source_identity'),
        producer=identity.get('producer'), session_key=identity.get('session_key'),
    )
    if not _classic_direct_writer_binding_valid(binding, identity, record):
        raise RuntimeStoreError('preservation_owner_unmapped')
    return binding


def classic_writer_binding_valid(binding, *, expected_session_id=None) -> bool:
    """Re-read a direct binding's exact authority record before consuming its proof."""
    if not isinstance(binding, ClassicWriterBinding):
        return False
    authority = binding.authority
    records = getattr(authority, '_preservation_operations', None)
    record = records.get(binding.operation_key) if isinstance(records, dict) else None
    identity = record.get('classic_identity') if isinstance(record, dict) else None
    if expected_session_id is not None and (not isinstance(identity, dict)
            or identity.get('session_id') != expected_session_id):
        return False
    return _classic_direct_writer_binding_valid(binding, identity, record)


def _classic_writer_handoff(event, _agent_result):
    """Read the one-use typed handoff from the owner/execution event pair.

    ``agent_result`` is intentionally not inspected: a handoff in a returned/captured/public result
    would cross the settlement and JSON boundaries. The caller clears both event holders on every exit.
    """
    owner, execution = _classic_event_pair(event)
    terminal = getattr(execution, '_classic_terminal_event', None)
    handoff = next(
        (getattr(holder, '_classic_writer_handoff', None)
         for holder in (owner, execution, terminal)
         if holder is not None and getattr(holder, '_classic_writer_handoff', None) is not None),
        None,
    )
    if handoff is None:
        return None, None
    try:
        from agent.session_persistence_proof import GatewayWriterProofHandoff
    except (ImportError, AttributeError):
        return None, None
    if type(handoff) is not GatewayWriterProofHandoff:
        return None, None
    return handoff.writer_proof, handoff.binding


def _classic_transcript_rows_valid(rows, session_id: str) -> bool:
    try:
        from gateway.session_transcript import TranscriptWriteProof
        if type(rows) is not tuple or not rows:
            return False
        row_ids = []
        roles = []
        for proof in rows:
            if not isinstance(proof, TranscriptWriteProof) or proof.session_id != session_id:
                return False
            if type(proof.row_id) is not int or proof.row_id <= 0 or not proof.row_snapshot:
                return False
            if proof.role not in {'session_meta', 'user', 'assistant', 'tool'}:
                return False
            row_ids.append(proof.row_id)
            roles.append(proof.role)
        return (
            tuple(row_ids) == tuple(sorted(set(row_ids)))
            and 'user' in roles
            and any(role in {'assistant', 'tool'} for role in roles)
        )
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        return False


def _classic_writer_proof_valid(proof, session_id: str) -> bool:
    if proof is None or getattr(proof, 'session_id', None) != session_id:
        return False
    row_ids = getattr(proof, 'row_ids', None)
    row_sessions = getattr(proof, 'row_sessions', None)
    row_roles = getattr(proof, 'row_roles', None)
    row_snapshots = getattr(proof, 'row_snapshots', None)
    result_row_ids = getattr(proof, 'result_row_ids', None)
    payload_digest = getattr(proof, 'payload_digest', None)
    store_path = getattr(proof, 'store_path', None)
    if (not isinstance(store_path, str) or not store_path
            or not isinstance(payload_digest, str) or len(payload_digest) != 64):
        return False
    if any(type(value) is not tuple or not value for value in (
        row_ids, row_sessions, row_roles, row_snapshots, result_row_ids,
    )):
        return False
    if row_ids != tuple(sorted(set(row_ids))) or any(type(row_id) is not int or row_id <= 0 for row_id in row_ids):
        return False
    if row_sessions != tuple(session_id for _ in row_ids):
        return False
    if len(row_roles) != len(row_ids) or len(row_snapshots) != len(row_ids):
        return False
    if any(role not in {'session_meta', 'user', 'assistant', 'tool'} for role in row_roles):
        return False
    if any(type(snapshot) is not str or not snapshot for snapshot in row_snapshots):
        return False
    result_ids = tuple(result_row_ids)
    suffix_ok = result_ids == row_ids[-len(result_ids):]
    user_index = len(row_ids) - len(result_ids) - 1
    return (
        'user' in row_roles
        and any(role in {'assistant', 'tool'} for role in row_roles)
        and suffix_ok
        and user_index >= 0
        and row_roles[user_index] == 'user'
        and all(row_id in row_ids for row_id in result_ids)
    )


def _classic_writer_binding_valid(
    binding, identity: dict[str, Any], expected_record=None, *, allow_terminal=False,
) -> bool:
    if isinstance(binding, ClassicWriterBinding):
        return _classic_direct_writer_binding_valid(
            binding, identity, expected_record, allow_terminal=allow_terminal,
        )
    try:
        from gateway.runtime_service_custody import TurnCustodyBinding, validate_turn_binding
    except (ImportError, AttributeError):
        return False
    if not isinstance(binding, TurnCustodyBinding):
        return False
    try:
        # This rereads the authoritative I04 admission row and exact parent/live execution. The
        # scalar comparisons below are only redundant identity checks for the classic receipt.
        validate_turn_binding(binding)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False
    fields = {
        'session_id': 'session_id',
        'profile_id': 'profile_id',
        'authority_epoch': 'authority_epoch',
        'process_instance': 'process_instance',
        'principal_id': 'principal_id',
        'source_identity': 'source_identity',
    }
    for binding_field, identity_field in fields.items():
        actual = getattr(binding, binding_field, None)
        expected = identity.get(identity_field)
        if actual is None or expected is None or actual != expected:
            return False
    admission_id = getattr(binding, 'admission_id', None)
    execution_generation = getattr(binding, 'execution_generation', None)
    if not isinstance(admission_id, str) or not admission_id:
        return False
    if type(execution_generation) is not int or execution_generation < 1:
        return False
    return True


def _classic_delivery_proof_valid(proof, session_key: str) -> bool:
    if proof is None:
        return False
    try:
        from gateway.delivery_ledger import DeliveryObligationProof
        return (
            isinstance(proof, DeliveryObligationProof)
            and proof.session_key == session_key
            and proof.state == 'delivered'
            and bool(proof.content_digest)
        )
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        return False


def _classic_delivery_witness_receipt_valid(receipt, witness) -> bool:
    proof = getattr(receipt, 'delivery_proof', None)
    witness_proof = getattr(witness, 'proof', None)
    expected = getattr(witness, 'expectation', None)
    try:
        from gateway.delivery_ledger import DeliveryObligationProof
        proof_matches = (
            isinstance(proof, DeliveryObligationProof)
            and isinstance(witness_proof, DeliveryObligationProof)
            and all(
                getattr(proof, field, None) == getattr(witness_proof, field, None)
                for field in (
                    'obligation_id', 'session_key', 'platform', 'chat_id', 'thread_id',
                    'content_digest', 'state', 'adapter_profile',
                )
            )
            and proof.state == 'delivered'
        )
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        proof_matches = False
    return (
        witness is not None
        and proof_matches
        and getattr(witness, 'obligation_id', None) == getattr(witness_proof, 'obligation_id', None)
        and getattr(expected, 'owner_event', None) is not None
        and getattr(expected, 'execution_event', None) is not None
        and getattr(expected, 'operation_key', None)
        and getattr(expected, 'admission_id', None)
    )


def _classic_callback_contract_valid(
    authority, record, owner, bridge, key, *, bridge_present, expected, callback_expected, witness,
) -> bool:
    """Require the exact typed callback fence before consuming one classic receipt."""
    if not isinstance(record, dict) or record.get('producer_object') is not owner:
        return False
    parented = record.get('parent_handle') is not None
    if bridge is None:
        # A bare direct root predates the callback bridge and may finish from its own receipt. Any
        # managed/parented occurrence, or an explicitly present but missing bridge, stays unknown.
        return not bridge_present and not parented
    if not isinstance(bridge, ClassicExecutionEventBridge):
        return False
    identity = record.get('classic_identity')
    execution = getattr(bridge, 'execution_event', None)
    if (
        bridge.owner_event is not owner
        or bridge.operation_key != key
        or getattr(owner, '_classic_custody_authority', None) is not authority
        or getattr(owner, '_classic_custody_key', None) != key
        or bridge.admission_id != record.get('admission_id')
        or not isinstance(identity, dict)
        or bridge.session_id != identity.get('session_id')
        or bridge.principal_id != identity.get('principal_id')
        or bridge.request_id != identity.get('request_id')
        or bridge.occurrence_id != identity.get('occurrence_id')
        or type(bridge.generation) is not int
        or bridge.generation < 1
        or record.get('generation') not in (None, 'pre-admission', bridge.generation)
    ):
        return False
    if expected is None:
        return False
    try:
        from gateway.session_classic_delivery import ClassicCallbackExpectation
    except (ImportError, AttributeError):
        return False
    if not isinstance(expected, ClassicCallbackExpectation):
        return False
    if (
        getattr(expected, 'authority', None) is not authority
        or getattr(expected, 'owner_event', None) is not owner
        or getattr(expected, 'execution_event', None) is not execution
        or getattr(expected, 'operation_key', None) != key
        or getattr(expected, 'admission_id', None) != bridge.admission_id
        or getattr(expected, 'admission_generation', None) != bridge.generation
        or getattr(expected, 'profile_id', None) != identity.get('profile_id')
        or getattr(expected, 'profile_home', None) != identity.get('profile_home')
        or getattr(expected, 'process_instance', None) != identity.get('process_instance')
        or getattr(expected, 'authority_epoch', None) != identity.get('authority_epoch')
        or getattr(expected, 'parent_handle', None) is not record.get('parent_handle')
    ):
        return False
    state = getattr(bridge, 'post_delivery_callback_state', 'unclassified')
    if callback_expected:
        return (
            bool(getattr(bridge, 'post_delivery_callback_expected', False))
            and getattr(bridge, 'post_delivery_callback_executed', False)
            and state in {'proof_recorded', 'settled'}
            and witness is not None
            and getattr(bridge, 'post_delivery_callback_delivery_witness', None) is witness
        )
    return (
        not getattr(bridge, 'post_delivery_callback_expected', False)
        and state == 'not_required'
        and getattr(bridge, 'post_delivery_callback_settled', False)
    )


def build_classic_execution_receipt(runner, event, session_entry, agent_result):
    """Assemble exact witnesses from the authenticated owner/execution event pair."""
    sid = getattr(session_entry, 'session_id', None)
    session_key = getattr(session_entry, 'session_key', None)
    if not isinstance(sid, str) or not sid or not isinstance(session_key, str) or not session_key:
        return None
    owner, execution = _classic_event_pair(event)
    identity = _classic_record_identity(owner)
    if identity is None:
        return None
    terminal = getattr(execution, '_classic_terminal_event', None)
    holders = (owner, execution, terminal)
    transcript_rows = next(
        (tuple(getattr(holder, '_classic_transcript_write_proofs', ()))
         for holder in holders
         if holder is not None and getattr(holder, '_classic_transcript_write_proofs', ())),
        (),
    )
    transcript_unknown = bool(
        getattr(execution, '_classic_transcript_write_unknown', False)
        or getattr(owner, '_classic_transcript_write_unknown', False)
        or getattr(terminal, '_classic_transcript_write_unknown', False)
    )
    writer_proof, writer_binding = _classic_writer_handoff(owner, agent_result)
    agent_persisted = bool(
        getattr(execution, '_classic_agent_persisted', False)
        or getattr(owner, '_classic_agent_persisted', False)
        or getattr(terminal, '_classic_agent_persisted', False)
    )
    if transcript_unknown or (agent_persisted and writer_proof is None):
        writer_proof = writer_binding = None
    if writer_proof is None:
        existing = next(
            (
                getattr(holder, '_classic_execution_receipt', None)
                for holder in (
                    owner, execution, getattr(execution, '_classic_terminal_event', None),
                )
                if isinstance(getattr(holder, '_classic_execution_receipt', None), ClassicExecutionReceipt)
            ),
            None,
        )
        existing_identity = tuple(identity.get(key) for key in _CLASSIC_IDENTITY_FIELDS)
        reference = _classic_record_ref(owner)
        record = reference[2] if reference is not None else None
        existing_writer_ok = _classic_writer_proof_valid(
            getattr(existing, 'writer_proof', None), sid,
        ) if existing is not None else False
        if existing_writer_ok:
            existing_writer_ok = _classic_writer_binding_valid(
                getattr(existing, 'writer_binding', None), identity, record,
                allow_terminal=True,
            )
        existing_transcript_ok = (
            existing is not None
            and _classic_transcript_rows_valid(getattr(existing, 'transcript_rows', ()), sid)
        )
        if (
            existing is not None
            and not transcript_unknown
            and existing.session_id == sid
            and existing.session_key == session_key
            and existing.identity == existing_identity
            and (existing_transcript_ok or existing_writer_ok)
            and (record is None or (
                record.get('producer_object') is owner
                and _identity_matches(record, identity)
            ))
        ):
            delivery = _classic_delivery_proof_for_event(event)
            if not _classic_delivery_proof_valid(delivery, session_key):
                delivery = existing.delivery_proof
            stream = next(
                (getattr(holder, '_classic_stream_delivery_witness', None)
                 for holder in holders
                 if holder is not None and getattr(holder, '_classic_stream_delivery_witness', None) is not None),
                existing.stream_witness,
            )
            return replace(existing, delivery_proof=delivery, stream_witness=stream)
    if transcript_unknown or (agent_persisted and writer_proof is None):
        return None
    if not agent_persisted and not transcript_rows and writer_proof is None:
        return None
    return ClassicExecutionReceipt(
        session_id=sid,
        session_key=session_key,
        identity=tuple(identity.get(key) for key in _CLASSIC_IDENTITY_FIELDS),
        transcript_rows=transcript_rows,
        writer_proof=writer_proof,
        writer_binding=writer_binding,
        delivery_proof=_classic_delivery_proof_for_event(event),
        stream_witness=next(
            (getattr(holder, '_classic_stream_delivery_witness', None)
             for holder in holders
             if holder is not None and getattr(holder, '_classic_stream_delivery_witness', None) is not None),
            None,
        ),
    )


def attach_classic_execution_receipt(runner, event, session_entry, agent_result):
    """Attach one private receipt to every exact owner/execution/terminal holder."""
    owner, execution = _classic_event_pair(event)
    terminal = getattr(execution, '_classic_terminal_event', None)
    holders = []
    for holder in (owner, execution, terminal):
        if holder is not None and not any(holder is existing for existing in holders):
            holders.append(holder)
    try:
        receipt = build_classic_execution_receipt(
            runner, execution, session_entry, agent_result,
        )
        for holder in holders:
            holder._classic_execution_receipt = receipt
    except Exception as exc:
        import logging
        logging.getLogger(__name__).debug(
            "Classic execution receipt readback unavailable: %s", exc, exc_info=True,
        )
        return None
    finally:
        for holder in holders:
            with suppress(AttributeError):
                delattr(holder, '_classic_writer_handoff')
    return receipt


def handoff_classic_execution_receipt(event, candidate) -> bool:
    """Forward a typed receipt only between the exact accepted event holders.

    The accepted busy event and restored execution event can cross different delivery helpers.
    The candidate must be the exact owner, execution, or source-owned terminal event. Receipt
    lookup is deliberately one-hop on that matched holder: a nested terminal holder cannot donate
    a foreign receipt. This helper never creates a receipt, callback, or durable terminal result.
    """
    owner, execution = _classic_event_pair(event)
    bridge = getattr(owner, '_classic_execution_bridge', None)
    reference = _classic_record_ref(owner)
    if (not isinstance(bridge, ClassicExecutionEventBridge)
            or bridge.owner_event is not owner
            or bridge.execution_event is not execution
            or reference is None
            or candidate is None
            or bridge.operation_key != reference[1]):
        return False
    record = reference[2]
    identity = record.get('classic_identity') if isinstance(record, dict) else None
    terminal = getattr(execution, '_classic_terminal_event', None)
    if candidate is not owner and candidate is not execution and candidate is not terminal:
        return False
    if (not isinstance(identity, dict)
            or bridge.admission_id != record.get('admission_id')
            or bridge.session_id != identity.get('session_id')
            or bridge.principal_id != identity.get('principal_id')
            or bridge.request_id != identity.get('request_id')
            or bridge.occurrence_id != identity.get('occurrence_id')
            or type(bridge.generation) is not int
            or bridge.generation < 1
            or record.get('generation') != bridge.generation):
        return False
    receipt = getattr(candidate, '_classic_execution_receipt', None)
    if not isinstance(receipt, ClassicExecutionReceipt):
        return False
    expected_identity = (
        tuple(identity.get(key) for key in _CLASSIC_IDENTITY_FIELDS)
        if isinstance(identity, dict) else None
    )
    if (expected_identity is None
            or receipt.identity != expected_identity
            or receipt.session_id != identity.get('session_id')
            or receipt.session_key != identity.get('session_key')):
        return False
    holders = []
    for holder in (owner, execution, terminal):
        if holder is not None and not any(holder is prior for prior in holders):
            holders.append(holder)
    for holder in holders:
        holder._classic_execution_receipt = receipt
    return True


def _classic_receipt_terminal(
    receipt, expected_identity=None, expected_record=None, delivery_witness=None,
):
    if not isinstance(receipt, ClassicExecutionReceipt):
        return False
    if not receipt.session_id or not receipt.session_key or not receipt.identity:
        return False
    if isinstance(expected_identity, dict):
        expected = tuple(expected_identity.get(key) for key in _CLASSIC_IDENTITY_FIELDS)
        if receipt.identity != expected:
            return False
    transcript_ok = _classic_transcript_rows_valid(receipt.transcript_rows, receipt.session_id)
    writer_ok = _classic_writer_proof_valid(receipt.writer_proof, receipt.session_id)
    if receipt.writer_proof is not None:
        identity = dict(zip(_CLASSIC_IDENTITY_FIELDS, receipt.identity))
        if expected_record is None and isinstance(receipt.writer_binding, ClassicWriterBinding):
            records = getattr(receipt.writer_binding.authority, '_preservation_operations', None)
            candidate = records.get(receipt.writer_binding.operation_key) if isinstance(records, dict) else None
            if isinstance(candidate, dict):
                expected_record = candidate
        record_identity = expected_record.get('classic_identity') if isinstance(expected_record, dict) else None
        if isinstance(record_identity, dict):
            identity['session_key'] = record_identity.get('session_key')
        writer_ok = writer_ok and _classic_writer_binding_valid(
            receipt.writer_binding, identity, expected_record, allow_terminal=True,
        )
    durable_ok = (
        writer_ok
        if isinstance(expected_record, dict) and expected_record.get('parent_handle') is not None
        else transcript_ok or writer_ok
    )
    delivery_ok = _classic_delivery_proof_valid(receipt.delivery_proof, receipt.session_key)
    if delivery_witness is not None:
        delivery_ok = delivery_ok and _classic_delivery_witness_receipt_valid(
            receipt, delivery_witness,
        )
    stream = receipt.stream_witness
    stream_ok = (
        isinstance(stream, ClassicStreamDeliveryWitness)
        and stream.session_key == receipt.session_key
        and isinstance(stream.session_key, str)
        and type(stream.run_generation) is int
        and stream.run_generation >= 1
        and isinstance(stream.payload_digest, str)
        and len(stream.payload_digest) == 64
    )
    return durable_ok and (
        delivery_ok if delivery_witness is not None else delivery_ok or stream_ok
    )


class ClassicCustodyMixin:
    """Source-owned classic producer lifecycle using the existing operation index/coordinator."""

    def _classic_operation_start(
        self, identity, *, parent=None, event=None, state='reserved', pre_reserved=None,
    ):
        if state not in _CLASSIC_STATES or not isinstance(identity, dict):
            raise RuntimeStoreError('invalid_params')
        if (identity.get('owner_class') != _CLASSIC_OWNER
                or identity.get('profile_id') != self.profile_id
                or identity.get('profile_home') != _profile_home(self)
                or identity.get('process_instance') != self.instance_id
                or identity.get('authority_epoch') != self.epoch):
            raise RuntimeStoreError('preservation_owner_unmapped')
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            raise RuntimeStoreError('runtime_custody_unavailable')
        key = self._preservation_operation_identity(
            _CLASSIC_OWNER, profile_id=self.profile_id,
            authority_epoch=self.epoch, session_id=identity['session_id'],
            principal_id=identity['principal_id'], operation=identity['producer'],
            request_id=identity['request_id'], generation=None,
            source_identity=identity['source_identity'])
        existing = self._preservation_operations.get(key)
        if existing is not None:
            if not _identity_matches(existing, identity):
                raise RuntimeStoreError('preservation_owner_unmapped')
            if event is not None and existing.get('producer_object') is not event:
                raise RuntimeStoreError('preservation_owner_unmapped')
            if existing.get('unknown'):
                refusal = coordinator.reconcile_unknown(
                    existing['handle'], owner_id=existing['handle'].owner_id,
                    profile_id=self.profile_id, process_instance=self.instance_id,
                    authority_epoch=self.epoch)
                if refusal is not None:
                    raise RuntimeStoreError(refusal.code)
                existing['unknown'] = False
            existing['state'] = state
            if event is not None:
                existing['producer_object'] = event
            return key
        if parent is None and pre_reserved is None:
            parent = _classic_parent_for_identity(self, identity)
        from gateway.runtime_custody import CustodyRefusal
        if parent is not None and not _valid_parent(self, parent):
            raise RuntimeStoreError('preservation_owner_unmapped')
        owner_id = f"classic:{self.profile_id}:{identity['session_id']}:{identity['request_id']}"
        if pre_reserved is not None:
            handle = pre_reserved
            if (getattr(handle, 'owner_class', None) != _CLASSIC_OWNER
                    or getattr(handle, 'profile_id', None) != self.profile_id
                    or getattr(handle, 'authority_epoch', None) != self.epoch
                    or getattr(handle, 'process_instance', None) != self.instance_id):
                raise RuntimeStoreError('preservation_owner_unmapped')
        elif parent is None:
            handle = coordinator.reserve_root(
                _CLASSIC_OWNER, owner_id, profile_id=self.profile_id, authority_epoch=self.epoch)
        else:
            reservation = coordinator.reserve_child(
                parent, _CLASSIC_OWNER, owner_id, profile_id=self.profile_id,
                authority_epoch=self.epoch)
            handle = (coordinator.activate_child(reservation)
                      if not isinstance(reservation, CustodyRefusal) else reservation)
        if isinstance(handle, CustodyRefusal):
            raise RuntimeStoreError(handle.code)
        self._busy_inbound_seen = True
        self._preservation_operations[key] = {
            'handle': handle,
            'owner_class': _CLASSIC_OWNER,
            'principal_id': identity['principal_id'],
            'session_id': identity['session_id'],
            'operation': identity['producer'],
            'request_id': identity['request_id'],
            'admission_id': None,
            'generation': None,
            'source_identity': identity['source_identity'],
            'parent_handle': parent,
            'classic_identity': dict(identity),
            'producer_object': event,
            'state': state,
            'unknown': False,
        }
        return key

    def _classic_reserve_missing_session(self, event, session_key, producer, *, state):
        """Admit a first-ever source occurrence before creating its routing row.

        The routing row is a durable source-owned effect, so it is created only after the opaque
        coordinator lease exists and is read back under the exact canonical key.  A failed create or
        readback keeps the lease unknown; it is never released as if no effect had happened.
        """
        source = getattr(event, 'source', None)
        if _canonical_session_key(self, source) != session_key:
            raise RuntimeStoreError('preservation_owner_unmapped')
        identity = _classic_source_identity(self, event, session_key, producer, None)
        coordinator = self._preservation_coordinator()
        if coordinator is None:
            raise RuntimeStoreError('runtime_custody_unavailable')
        owner_id = f"classic-pending:{self.profile_id}:{hashlib.sha256(session_key.encode()).hexdigest()}:{identity['request_id']}"
        from gateway.runtime_custody import CustodyRefusal
        handle = coordinator.reserve_root(
            _CLASSIC_OWNER, owner_id, profile_id=self.profile_id, authority_epoch=self.epoch,
        )
        if isinstance(handle, CustodyRefusal):
            raise RuntimeStoreError(handle.code)
        store = getattr(self.runner, 'session_store', None)
        try:
            store.get_or_create_session(source, touch_activity=False)
            entry = store.lookup_by_session_key(session_key)
            session_id = getattr(entry, 'session_id', None)
            if (entry is None or entry.session_key != session_key
                    or not isinstance(session_id, str) or not session_id):
                raise RuntimeStoreError('preservation_owner_unmapped')
            identity = _classic_source_identity(self, event, session_key, producer, session_id)
            return self._classic_operation_start(
                identity, event=event, state=state, pre_reserved=handle,
            )
        except Exception as exc:
            # The row may have been created even when readback failed.  Preserve the lease as
            # unknown so a retry can reconcile the exact source occurrence safely.
            coordinator.mark_unknown(handle, reason='session_binding_readback_failed')
            if isinstance(exc, RuntimeStoreError):
                raise
            raise RuntimeStoreError('session_binding_readback_failed') from exc

    def _classic_begin(self, event, session_key, producer, *, state='reserved', parent=None):
        if event is None:
            return False
        existing = getattr(event, '_classic_custody_key', None)
        if existing is not None:
            record = self._preservation_operations.get(existing)
            store = getattr(self.runner, 'session_store', None)
            try:
                entry = store.lookup_by_session_key(session_key) if store is not None else None
            except (AttributeError, KeyError, TypeError, ValueError):
                entry = None
            if (not isinstance(record, dict) or record.get('producer_object') is not event
                    or entry is None or entry.session_id != record.get('session_id')):
                return False
            try:
                current_identity = classic_identity(self, event, session_key, producer)
            except RuntimeStoreError:
                return False
            stored_identity = record.get('classic_identity')
            stored_producer = (
                stored_identity.get('producer') if isinstance(stored_identity, dict) else None
            )
            if (
                not isinstance(stored_identity, dict)
                or stored_identity.get('session_key') != session_key
                or stored_producer not in _CLASSIC_PRODUCERS
                or current_identity.get('producer') != producer
            ):
                return False
            try:
                identity = classic_identity(self, event, session_key, stored_producer)
                expected_key = self._preservation_operation_identity(
                    _CLASSIC_OWNER, profile_id=self.profile_id,
                    authority_epoch=self.epoch, session_id=identity['session_id'],
                    principal_id=identity['principal_id'], operation=identity['producer'],
                    request_id=identity['request_id'], generation=None,
                    source_identity=identity['source_identity'])
            except RuntimeStoreError:
                return False
            handle = record.get('handle')
            coordinator = self._preservation_coordinator()
            parent_handle = record.get('parent_handle')
            if (
                existing != expected_key
                or not _identity_matches(record, identity)
                or stored_identity != {
                    **identity, 'session_key': session_key,
                }
                or record.get('owner_class') != _CLASSIC_OWNER
                or record.get('principal_id') != identity['principal_id']
                or record.get('session_id') != identity['session_id']
                or record.get('operation') != identity['producer']
                or record.get('request_id') != identity['request_id']
                or record.get('source_identity') != identity['source_identity']
                or coordinator is None
                or handle is None
                or not isinstance(getattr(handle, 'owner_id', None), str)
                or not getattr(handle, 'owner_id', None)
                or getattr(handle, 'owner_class', None) != _CLASSIC_OWNER
                or getattr(handle, 'profile_id', None) != self.profile_id
                or getattr(handle, 'process_instance', None) != self.instance_id
                or getattr(handle, 'authority_epoch', None) != self.epoch
                or getattr(handle, 'coordinator_id', None) != getattr(coordinator, '_coordinator_id', None)
                or (parent is not None and parent_handle is not parent)
                or (parent_handle is not None and not _valid_parent(self, parent_handle))
            ):
                return False
            try:
                key = self._classic_operation_start(
                    identity, parent=parent_handle, event=event, state=state,
                )
            except RuntimeStoreError:
                return False
            return key == existing
        try:
            source = getattr(event, 'source', None)
            if _canonical_session_key(self, source) != session_key:
                return False
            if _classic_session_id(self, session_key, event) is None:
                key = self._classic_reserve_missing_session(event, session_key, producer, state=state)
            else:
                identity = classic_identity(self, event, session_key, producer)
                key = self._classic_operation_start(identity, parent=parent, event=event, state=state)
        except RuntimeStoreError:
            return False
        if key is None:
            return True
        event._classic_custody_key = key
        event._classic_custody_authority = self
        event._classic_custody_events = [event]
        return True

    def _classic_admission_committed(self, event, row):
        """Bind a direct classic owner inside the admission transaction callback.

        ``SessionAuthority._admitted`` runs this before it publishes pending work or invokes its
        ``after`` scheduler callback.  The durable row and the existing operation record are the
        authorities; ``marker_adopters`` is only a short-lived pointer for the original event.
        Managed I04/I07 children intentionally return without acquiring a direct classic writer.
        """
        reference = _classic_record_ref(event)
        if reference is None:
            # API, automation, and graph admissions do not carry a classic record.
            return None
        authority, key, record = reference
        if authority is not self or getattr(event, '_classic_custody_authority', None) is not self:
            return None
        if record.get('owner_class') != _CLASSIC_OWNER:
            return None
        # A classic record attached to a live I04/I07 parent keeps the existing managed
        # TurnCustodyBinding writer.  It still needs the exact durable admission id bound before
        # restore, otherwise the later occurrence cannot recover its bridge/receipt.  The parent
        # is validated before that bind so this never creates a second writer owner.
        parent_handle = record.get('parent_handle')
        if parent_handle is not None and not _valid_parent(self, parent_handle):
            self._preservation_operation_unknown(key, 'classic_parent_unknown')
            event._classic_admission_owner_unknown = True
            return False
        identity = record.get('classic_identity')
        if not isinstance(row, dict) or row.get('status') in {'terminal', 'unknown'}:
            # Exact terminal/unknown replay is handled by the admission ledger; it is not a new
            # owner binding and must not turn a finished or retained operation into a false mismatch.
            return None
        admission_id = row.get('admission_id')
        valid = (
            isinstance(identity, dict)
            and record.get('producer_object') is event
            and isinstance(admission_id, str)
            and admission_id
            and _classic_admission_row_matches(self, record, row)
            and _identity_matches(record, identity)
        )
        if not valid:
            self._preservation_operation_unknown(key, 'unmapped_writer')
            event._classic_admission_owner_unknown = True
            return False
        try:
            self._preservation_operation_bind_admission(
                key, admission_id, expected_parent=parent_handle,
            )
            bound = self._preservation_operation_row(key, expected_parent=parent_handle)
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            bound = None
        if bound is None or not _classic_admission_row_matches(self, record, bound):
            self._preservation_operation_unknown(key, 'admission_owner_readback_failed')
            event._classic_admission_owner_unknown = True
            return False
        event._classic_admission_id = admission_id
        if getattr(event, '_turn_marker_handoff', False):
            adopters = getattr(self, 'marker_adopters', None)
            if adopters is None:
                adopters = self.marker_adopters = {}
            prior = adopters.get(admission_id)
            if prior is not None and prior is not event:
                self._preservation_operation_unknown(key, 'foreign_marker_owner')
                event._classic_admission_owner_unknown = True
                return False
            adopters[admission_id] = event
        return True

    def _classic_execution_owner_for_admission(self, row):
        """Resolve one direct classic admission to its original event before agent effects.

        The operation index is authoritative and already retains ``producer_object``.  We do not
        reconstruct private fields from the serialized envelope and do not use text, request maps,
        or a second owner ledger.  A missing or conflicting owner is returned as a typed unknown so
        the scheduler can retain the exact admission for same-owner retry.
        """
        if not isinstance(row, dict) or not isinstance(row.get('admission_id'), str):
            return None, None
        admission_id = row['admission_id']
        candidates = []
        for key, record in list(getattr(self, '_preservation_operations', {}).items()):
            if (not isinstance(record, dict)
                    or record.get('owner_class') != _CLASSIC_OWNER
                    or record.get('admission_id') != admission_id):
                continue
            parent = record.get('parent_handle')
            if parent is not None and not _valid_parent(self, parent):
                self._preservation_operation_unknown(key, 'classic_parent_unknown')
                return None, 'classic_owner_unknown'
            candidates.append((key, record))
        if not candidates:
            if classic_row_is_direct(row):
                return None, 'classic_owner_unknown'
            return None, None
        if len(candidates) != 1:
            for key, _record in candidates:
                self._preservation_operation_unknown(key, 'ambiguous_owner')
            return None, 'classic_owner_ambiguous'
        key, record = candidates[0]
        owner = record.get('producer_object')
        if (record.get('unknown') is True or owner is None
                or getattr(owner, '_classic_custody_authority', None) is not self
                or getattr(owner, '_classic_custody_key', None) != key
                or not _classic_admission_row_matches(self, record, row)):
            self._preservation_operation_unknown(key, 'classic_owner_unmapped')
            return None, 'classic_owner_unknown'
        try:
            reread = self._preservation_operation_row(
                key, expected_parent=record.get('parent_handle'),
            )
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            reread = None
        if reread is None or not _classic_admission_row_matches(self, record, reread):
            self._preservation_operation_unknown(key, 'classic_owner_readback_failed')
            return None, 'classic_owner_unknown'
        adopters = getattr(self, 'marker_adopters', {})
        adopter = adopters.get(admission_id)
        if adopter is not None and adopter is not owner:
            self._preservation_operation_unknown(key, 'foreign_marker_owner')
            return None, 'classic_owner_unknown'
        return owner, None

    def _classic_mark_state(self, event, state):
        if event is None:
            return
        for item in getattr(event, '_classic_custody_events', [event]):
            key = getattr(item, '_classic_custody_key', None)
            if key is None:
                continue
            record = self._preservation_operations.get(key)
            if record is not None and record.get('classic_identity', {}).get('owner_class') == _CLASSIC_OWNER:
                record['state'] = state

    def _classic_attach_event(self, aggregate, event):
        if aggregate is None or event is None:
            return
        events = list(getattr(aggregate, '_classic_custody_events', [aggregate]))
        for item in getattr(event, '_classic_custody_events', [event]):
            if not any(item is existing for existing in events):
                events.append(item)
        aggregate._classic_custody_events = events
        self._classic_mark_state(aggregate, 'handoff')

    def _classic_finish_item(
        self, item, *, success, reason, receipt, callback_key, callback_owner, callback_bridge,
        callback_bridge_present, callback_expected, callback_witness, callback_failure_reason,
    ):
        """Apply the existing authenticated terminal/UNKNOWN decision to one custody event."""
        key = getattr(item, '_classic_custody_key', None)
        if key is None or key not in self._preservation_operations:
            return False
        record = self._preservation_operations.get(key)
        expected_identity = record.get('classic_identity') if isinstance(record, dict) else None
        parent_ok = True
        if isinstance(record, dict) and record.get('parent_handle') is not None:
            from gateway.session_classic_delivery import classic_parent_admission_proof
            parent_ok = classic_parent_admission_proof(self, record) is not None
        receipt_ok = _classic_receipt_terminal(
            receipt, expected_identity, record,
            callback_witness if key == callback_key else None,
        )
        callback_ok = (
            key != callback_key
            or _classic_callback_contract_valid(
                self,
                record,
                callback_owner,
                callback_bridge,
                key,
                bridge_present=callback_bridge_present,
                expected=getattr(callback_bridge, 'post_delivery_callback_expectation', None),
                callback_expected=callback_expected,
                witness=callback_witness,
            )
        )
        if success and parent_ok and receipt_ok and callback_ok:
            self._preservation_operation_terminal(key, f'classic:{item._classic_custody_key}')
            callback_terminal = key == callback_key
        elif success:
            self._preservation_operation_unknown(
                key, reason or callback_failure_reason
                or ('post_delivery_callback_unsettled' if key == callback_key and callback_expected
                    else 'completion_receipt_missing'))
            callback_terminal = False
            if key == callback_key and callback_expected:
                with suppress(AttributeError):
                    callback_bridge.post_delivery_callback_state = 'failed'
        else:
            self._preservation_operation_unknown(key, reason)
            callback_terminal = False
            if key == callback_key and callback_expected:
                with suppress(AttributeError):
                    callback_bridge.post_delivery_callback_state = 'failed'
        if key not in self._preservation_operations:
            item._classic_custody_key = None
        return callback_terminal

    def _classic_finish(self, event, *, success, reason='join_failed', receipt=None):
        if event is None or getattr(event, '_classic_custody_authority', self) is not self:
            return False
        callback_owner, _callback_execution = _classic_event_pair(event)
        callback_owner_key = getattr(callback_owner, '_classic_custody_key', None)
        callback_bridge = getattr(callback_owner, '_classic_execution_bridge', None)
        callback_bridge_present = hasattr(callback_owner, '_classic_execution_bridge')
        callback_key = callback_owner_key
        if isinstance(callback_bridge, ClassicExecutionEventBridge):
            if callback_bridge.operation_key == callback_owner_key:
                callback_key = callback_bridge.operation_key
        callback_expected = bool(getattr(callback_bridge, 'post_delivery_callback_expected', False))
        callback_witness = None
        if callback_expected:
            from gateway.session_classic_delivery import classic_delivery_proof_witness_for_event
            callback_witness = classic_delivery_proof_witness_for_event(event)
        callback_failure_reason = (
            getattr(callback_owner, '_classic_delivery_failure_reason', None)
            or getattr(event, '_classic_delivery_failure_reason', None)
        )
        if receipt is None:
            from gateway.session_classic_delivery import _classic_receipt_for_event
            receipt = _classic_receipt_for_event(event)
        if success and isinstance(receipt, ClassicExecutionReceipt) and receipt.delivery_proof is None:
            delivery_proof = getattr(callback_witness, 'proof', None) or _classic_delivery_proof_for_event(event)
            if delivery_proof is not None:
                receipt = replace(receipt, delivery_proof=delivery_proof)
        events = list(getattr(event, '_classic_custody_events', [event]))
        callback_terminal = False
        for item in events:
            callback_terminal = self._classic_finish_item(
                item,
                success=success,
                reason=reason,
                receipt=receipt,
                callback_key=callback_key,
                callback_owner=callback_owner,
                callback_bridge=callback_bridge,
                callback_bridge_present=callback_bridge_present,
                callback_expected=callback_expected,
                callback_witness=callback_witness,
                callback_failure_reason=callback_failure_reason,
            ) or callback_terminal
        settled = bool(
            success
            and callback_key is not None
            and callback_key not in self._preservation_operations
            and (not callback_expected or callback_terminal)
        )
        if settled:
            with suppress(AttributeError):
                callback_bridge.post_delivery_callback_settled = True
                if callback_expected:
                    with suppress(AttributeError):
                        callback_bridge.post_delivery_callback_state = 'settled'
                        from gateway.session_classic_delivery import ClassicCallbackSettlement
                        callback_bridge.post_delivery_callback_settlement = ClassicCallbackSettlement(
                            expectation=callback_bridge.post_delivery_callback_expectation,
                            delivery=callback_witness,
                        )
        if getattr(callback_bridge, 'post_delivery_callback_pending', False):
            callback_bridge.post_delivery_callback_pending = False
            callback_bridge.post_delivery_callback_authority = False
            callback_bridge.post_delivery_callback_executed = True
            done = getattr(callback_bridge, 'post_delivery_callback_done', None)
            if done is not None:
                with suppress(Exception):
                    done.set()
        return settled

    def _classic_release_unaccepted(self, event):
        if event is None:
            return
        for item in getattr(event, '_classic_custody_events', [event]):
            key = getattr(item, '_classic_custody_key', None)
            if key is not None:
                self._preservation_operation_release_unaccepted(key)
                if key not in self._preservation_operations:
                    item._classic_custody_key = None

    def _classic_sources_quiescent(self):
        """Read live adapter ownership; empty custody maps alone never establish quiescence."""
        runner = self.runner
        adapters = list((getattr(runner, 'adapters', {}) or {}).values())
        registry = getattr(runner, 'session_authorities', None)
        if registry is not None:
            profile_name = registry.profile_name(self)
            adapters.extend((getattr(runner, '_profile_adapters', {}) or {}).get(profile_name, {}).values())
        adapters = list({id(adapter): adapter for adapter in adapters}.values())
        for adapter in adapters:
            if getattr(adapter, '_active_sessions', None):
                return False
            if getattr(adapter, '_pending_messages', None):
                return False
            debounce = getattr(adapter, '_text_debounce', None)
            if debounce:
                return False
        sessions = getattr(runner, '_sessions', {}) or {}
        if any(getattr(getattr(state, 'conversation', None), 'queued_events', None)
               for state in sessions.values()):
            return False
        try:
            from hermes_cli.loops import store_has_active_loop
            return not store_has_active_loop(self.db)
        except (OSError, RuntimeError, sqlite3.Error, TypeError, ValueError, KeyError):
            # An unreadable loop namespace is unknown custody, never an empty/ready state.
            return False



def _classic_admission_row_matches(authority, record, row, *, allow_terminal=False):
    """Check only server-owned row identity; claim generation is bound by the claim writer."""
    if not isinstance(record, dict) or not isinstance(row, dict):
        return False
    if record.get('admission_id') not in (None, row.get('admission_id')):
        return False
    identity = record.get('classic_identity')
    if not isinstance(identity, dict):
        return False
    if any(row.get(field) != record.get(record_field) for field, record_field in (
        ('principal_id', 'principal_id'), ('target_session_id', 'session_id'),
        ('request_id', 'request_id'),
    )):
        return False
    if row.get('owner_epoch') != getattr(authority, 'epoch', None):
        return False
    status = row.get('status')
    if status not in ({'queued', 'started'} | ({'terminal'} if allow_terminal else set())):
        return False
    generation = row.get('generation')
    stored_generation = record.get('generation')
    if status == 'terminal':
        return (
            row.get('outcome') == 'completed'
            and type(generation) is int
            and generation >= 1
            and stored_generation == generation
        )
    if row.get('status') == 'queued':
        return generation is None and stored_generation in (None, 'pre-admission')
    if type(generation) is not int or generation < 1:
        return False
    return stored_generation in (None, 'pre-admission', generation)


def classic_execution_owner_for_event(event):
    """Return the original process-local owner carried by a restored admission event."""
    owner = getattr(event, '_classic_execution_owner_event', None)
    return owner if owner is not None else event


def classic_terminal_inbound_fallback_allowed(event) -> bool:
    """Allow legacy ledger fallback only for events outside the classic custody bridge."""
    owner = classic_execution_owner_for_event(event)
    return not any(getattr(candidate, field, None) is not None for candidate, field in (
        (event, '_classic_execution_owner_event'), (event, '_classic_execution_bridge'),
        (owner, '_classic_custody_authority'), (event, '_classic_custody_key'),
    ))


def _classic_event_pair(event):
    owner = classic_execution_owner_for_event(event)
    bridge = getattr(owner, '_classic_execution_bridge', None)
    if (isinstance(bridge, ClassicExecutionEventBridge)
            and bridge.owner_event is owner
            and bridge.execution_event is not None
            and (event is owner or event is bridge.execution_event)):
        return owner, bridge.execution_event
    return owner, event


def _classic_delivery_proof_for_event(event):
    """Read the one delivery proof already attached to the exact event bridge."""
    owner, execution = _classic_event_pair(event)
    terminal = getattr(execution, '_classic_terminal_event', None)
    seen = set()
    for holder in (owner, execution, terminal):
        if holder is None or id(holder) in seen:
            continue
        seen.add(id(holder))
        proof = getattr(holder, '_classic_delivery_proof', None)
        if proof is not None:
            return proof
    return None


def bind_classic_execution_event(owner, event, row) -> bool:
    """Bind the restored event only to the authenticated direct owner and admission row."""
    reference = _classic_record_ref(owner)
    if reference is None or owner is None or event is None or not isinstance(row, dict):
        return False
    authority, key, record = reference
    identity = record.get('classic_identity')
    admission_id = row.get('admission_id')
    parent = record.get('parent_handle')
    if (authority is not getattr(owner, '_classic_custody_authority', None)
            or record.get('producer_object') is not owner
            or (parent is not None and not _valid_parent(authority, parent))
            or record.get('admission_id') != admission_id
            or not isinstance(admission_id, str)
            or not _classic_admission_row_matches(authority, record, row)):
        return False
    if not isinstance(identity, dict):
        return False
    prior = getattr(owner, '_classic_execution_bridge', None)
    if isinstance(prior, ClassicExecutionEventBridge):
        if (prior.owner_event is not owner or prior.admission_id != admission_id
                or prior.operation_key != key
                or (prior.execution_event is not owner and prior.execution_event is not event)):
            return False
        if prior.execution_event is event:
            event._classic_execution_owner_event = owner
            return True
    bridge = ClassicExecutionEventBridge(
        owner_event=owner, execution_event=event, admission_id=admission_id,
        generation=row.get('generation'), session_id=identity.get('session_id'),
        principal_id=identity.get('principal_id'), request_id=identity.get('request_id'),
        occurrence_id=identity.get('occurrence_id'), operation_key=key,
    )
    if not all(isinstance(value, str) and value for value in (
        bridge.session_id, bridge.principal_id, bridge.request_id, bridge.occurrence_id,
    )):
        return False
    owner._classic_execution_bridge = bridge
    event._classic_execution_bridge = bridge
    event._classic_execution_owner_event = owner
    return True


def set_classic_terminal_inbound_id(event, inbound_id) -> bool:
    """Carry the exact terminal FIFO member id across the owner/execution event pair."""
    if not isinstance(inbound_id, str) or not inbound_id:
        return False
    owner, execution = _classic_event_pair(event)
    if owner is not event and not isinstance(getattr(owner, '_classic_execution_bridge', None), ClassicExecutionEventBridge):
        return False
    reference = _classic_record_ref(owner)
    if reference is None:
        return False
    authority, _owner_key, owner_record = reference
    owner_identity = owner_record.get('classic_identity')
    coordinator_getter = getattr(authority, '_preservation_coordinator', None)
    coordinator = coordinator_getter() if callable(coordinator_getter) else None
    if (owner_record.get('owner_class') != _CLASSIC_OWNER
            or owner_record.get('unknown') is True
            or owner_record.get('producer_object') is not owner
            or not isinstance(owner_identity, dict)
            or not _identity_matches(owner_record, owner_identity)
            or coordinator is None
            or _active_classic_handle(coordinator, owner_record.get('handle')) is None):
        return False
    candidates = []
    terminal_event = getattr(event, '_classic_terminal_event', None)
    for candidate in (
        owner, execution, terminal_event, *getattr(owner, '_classic_custody_events', ())
    ):
        if candidate is None or any(candidate is prior for prior in candidates):
            continue
        candidate_ref = _classic_record_ref(candidate)
        if candidate_ref is None:
            continue
        candidate_authority, candidate_key, candidate_record = candidate_ref
        candidate_identity = candidate_record.get('classic_identity')
        if (candidate_authority is not authority
                or candidate_record.get('owner_class') != _CLASSIC_OWNER
                or candidate_record.get('unknown') is True
                or candidate_record.get('producer_object') is not candidate
                or not isinstance(candidate_identity, dict)
                or not _identity_matches(candidate_record, candidate_identity)
                or any(candidate_identity.get(field) != owner_identity.get(field) for field in (
                    'owner_class', 'profile_id', 'profile_home', 'process_instance',
                    'authority_epoch', 'session_id', 'principal_id', 'source_identity',
                ))
                or candidate_record.get('request_id') != inbound_id
                or _active_classic_handle(coordinator, candidate_record.get('handle')) is None):
            continue
        admission_id = candidate_record.get('admission_id')
        if not isinstance(admission_id, str) or not admission_id:
            continue
        try:
            row = authority._preservation_operation_row(candidate_key)
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            row = None
        if row is not None and _classic_admission_row_matches(authority, candidate_record, row):
            candidates.append(candidate)
    if len(candidates) != 1:
        return False
    owner.ledger_message_id = inbound_id
    execution.ledger_message_id = inbound_id
    if terminal_event is not None:
        terminal_event.ledger_message_id = inbound_id
    bridge = getattr(owner, '_classic_execution_bridge', None)
    if isinstance(bridge, ClassicExecutionEventBridge):
        bridge.terminal_inbound_id = inbound_id
    return True


def classic_terminal_inbound_id(event):
    owner, execution = _classic_event_pair(event)
    bridge = getattr(owner, '_classic_execution_bridge', None)
    if isinstance(bridge, ClassicExecutionEventBridge) and bridge.terminal_inbound_id:
        return bridge.terminal_inbound_id
    return (getattr(execution, 'ledger_message_id', None)
            or getattr(owner, 'ledger_message_id', None)
            or getattr(execution, 'message_id', None)
            or getattr(owner, 'message_id', None))


def classic_direct_route_marker(authority, event):
    """Return the durable direct-classic marker only for an authenticated root owner."""
    reference = _classic_record_ref(event)
    if reference is None:
        return None
    bound_authority, _key, record = reference
    if (bound_authority is not authority
            or record.get('owner_class') != _CLASSIC_OWNER
            or record.get('producer_object') is not event
            or record.get('parent_handle') is not None):
        return None
    return 'classic_busy'


def classic_row_is_direct(row) -> bool:
    payload = row.get('payload') if isinstance(row, dict) else None
    descriptor = payload.get('native_text_v1') if isinstance(payload, dict) else None
    return isinstance(descriptor, dict) and descriptor.get('producer_process_mode') == 'classic_busy'


