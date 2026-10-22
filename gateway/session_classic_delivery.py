"""Typed classic delivery witnesses and post-delivery custody boundaries."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from contextlib import suppress
from dataclasses import dataclass, replace
from pathlib import Path

from gateway.session_classic_callback_waiters import (
    _classic_callback_bridge_for_admission,
    _classic_callback_bridge_for_event,
    _classic_callback_bridge_identity_valid,
    _classic_callback_done,
    _classic_callback_waiter_entry,
    _classic_callback_waiter_slot,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClassicMarkerAdoptionWitness:
    """Source-owned proof that one exact event adopted one exact turn marker."""

    authority: object
    operation_key: str
    handle: object
    admission_id: str
    marker_event: object


@dataclass(frozen=True)
class ClassicCallbackExpectation:
    """Exact callback contract for one admitted classic occurrence."""

    authority: object
    operation_key: str
    admission_id: str
    admission_generation: int
    run_generation: object
    profile_id: object
    profile_home: object
    process_instance: object
    authority_epoch: object
    parent_handle: object
    owner_event: object
    execution_event: object
    callback: object
    delivery_obligation_id: object = None
    delivery_platform: object = None
    delivery_chat_id: object = None
    delivery_thread_id: object = None
    delivery_adapter_profile: object = None


@dataclass(frozen=True)
class ClassicDeliveryProofWitness:
    """One exact delivery-ledger readback carried by its callback expectation."""

    expectation: ClassicCallbackExpectation
    terminal_event: object
    obligation_id: str
    proof: object
    session_key: str
    platform: str
    chat_id: str
    thread_id: object
    adapter_profile: str
    marker_event: object


@dataclass(frozen=True)
class ClassicCallbackSettlement:
    """Typed terminal callback witness retained after the operation row is retired."""

    expectation: ClassicCallbackExpectation
    delivery: ClassicDeliveryProofWitness


@dataclass(frozen=True)
class ClassicRetiredCallbackWitness:
    """Authenticated I06 evidence after its operation index entry has been consumed."""

    authority: object
    operation_key: str
    admission_id: str
    owner: object
    bridge: object
    record: dict
    row: dict
    parent_handle: object
    parent_proof: object
    settlement: ClassicCallbackSettlement
    delivery: ClassicDeliveryProofWitness


def bind_classic_marker_adoption(authority, operation_key, admission_id, event) -> bool:
    """Retain the authenticated marker adoption after the short-lived adopter map is cleared."""
    records = getattr(authority, '_preservation_operations', None)
    record = records.get(operation_key) if isinstance(records, dict) else None
    adopters = getattr(authority, 'marker_adopters', None)
    if (
        not isinstance(operation_key, str)
        or not isinstance(admission_id, str)
        or not isinstance(record, dict)
        or record.get('owner_class') != 'I06_busy_followup_delivery'
        or record.get('producer_object') is not event
        or record.get('admission_id') != admission_id
        or getattr(event, '_classic_custody_authority', None) is not authority
        or getattr(event, '_classic_custody_key', None) != operation_key
        or not getattr(event, '_turn_marker_handoff', False)
        or not isinstance(adopters, dict)
        or adopters.get(admission_id) is not event
        or record.get('handle') is None
    ):
        return False
    prior = record.get('_classic_marker_adoption')
    if prior is not None:
        return (
            isinstance(prior, ClassicMarkerAdoptionWitness)
            and prior.authority is authority
            and prior.operation_key == operation_key
            and prior.handle is record.get('handle')
            and prior.admission_id == admission_id
            and prior.marker_event is event
        )
    record['_classic_marker_adoption'] = ClassicMarkerAdoptionWitness(
        authority=authority,
        operation_key=operation_key,
        handle=record['handle'],
        admission_id=admission_id,
        marker_event=event,
    )
    return True


def clear_classic_marker_adopter(authority, admission_id):
    adopters = getattr(authority, 'marker_adopters', None)
    if isinstance(adopters, dict):
        adopters.pop(admission_id, None)


def classic_marker_release_witness_valid(authority, operation_key, record, marker_event) -> bool:
    """Validate the exact adopted event and its adapter-owned released marker."""
    witness = record.get('_classic_marker_adoption') if isinstance(record, dict) else None
    released = getattr(marker_event, '_gateway_marker_released', None)
    return (
        isinstance(witness, ClassicMarkerAdoptionWitness)
        and witness.authority is authority
        and witness.operation_key == operation_key
        and witness.handle is record.get('handle')
        and witness.admission_id == record.get('admission_id')
        and witness.marker_event is marker_event
        and record.get('producer_object') is marker_event
        and isinstance(released, asyncio.Event)
        and released.is_set()
    )


def _classic_receipt_for_event(event):
    from gateway.session_classic_custody import ClassicExecutionReceipt, classic_execution_owner_for_event

    owner = classic_execution_owner_for_event(event)
    bridge = getattr(owner, '_classic_execution_bridge', None)
    execution_event = getattr(bridge, 'execution_event', None)
    terminal_event = getattr(execution_event, '_classic_terminal_event', None)
    holders = (owner, execution_event, terminal_event)
    return next(
        (
            getattr(holder, '_classic_execution_receipt', None)
            for holder in holders
            if isinstance(getattr(holder, '_classic_execution_receipt', None), ClassicExecutionReceipt)
        ),
        None,
    )


def _classic_source_parent_valid(authority, parent, parent_admission_id) -> bool:
    """Validate the retained I04 parent carried by the accepted I06 source record."""
    from gateway.runtime_custody import CustodyHandle

    coordinator_getter = getattr(authority, '_preservation_coordinator', None)
    coordinator = coordinator_getter() if callable(coordinator_getter) else None
    children = getattr(authority, '_preservation_children', None)
    parent_edge = getattr(coordinator, '_parent', None)
    return (
        isinstance(parent, CustodyHandle)
        and isinstance(parent_admission_id, str)
        and bool(parent_admission_id)
        and parent.owner_class == 'I04_fifo_turn_execution'
        and parent.owner_id == f'claim:{parent_admission_id}'
        and parent.profile_id == getattr(authority, 'profile_id', None)
        and parent.authority_epoch == getattr(authority, 'epoch', None)
        and parent.process_instance == getattr(authority, 'instance_id', None)
        and coordinator is not None
        and parent.coordinator_id == getattr(coordinator, '_coordinator_id', None)
        and isinstance(children, dict)
        and children.get(parent_admission_id) is parent
        and callable(parent_edge)
        and parent_edge(parent) is not None
    )


def _classic_terminal_parent_receipt_valid(authority, record, parent, parent_admission_id) -> bool:
    from gateway.session_classic_custody import (
        _CLASSIC_IDENTITY_FIELDS,
        _classic_writer_proof_valid,
    )
    from gateway.session_classic_writer_parent import _classic_execution_writer_parent_valid

    identity = record.get('classic_identity') if isinstance(record, dict) else None
    receipt = _classic_receipt_for_event(record.get('producer_object'))
    binding = getattr(receipt, 'writer_binding', None)
    if (
        not isinstance(identity, dict)
        or receipt is None
        or receipt.identity != tuple(identity.get(field) for field in _CLASSIC_IDENTITY_FIELDS)
        or receipt.session_id != identity.get('session_id')
        or receipt.session_key != identity.get('session_key')
        or not _classic_writer_proof_valid(receipt.writer_proof, receipt.session_id)
        or getattr(binding, 'authority', None) is not authority
        or not _classic_source_parent_valid(authority, parent, parent_admission_id)
        or not _classic_execution_writer_parent_valid(authority, record, identity, binding)
    ):
        return False
    children = getattr(authority, '_preservation_children', None)
    coordinator = authority._preservation_coordinator()
    return (
        isinstance(children, dict)
        and children.get(parent_admission_id) is parent
        and coordinator is not None
        and coordinator._parent(parent) is not None
    )


def classic_parent_admission_proof(authority, record):
    """Authenticate the direct classic parent through its row and the live I04 custody graph."""
    parent = record.get('parent_handle') if isinstance(record, dict) else None
    parent_owner_id = getattr(parent, 'owner_id', None)
    if (
        parent is None
        or getattr(parent, 'owner_class', None) != 'I04_fifo_turn_execution'
        or type(parent_owner_id) is not str
        or not parent_owner_id.startswith('claim:')
    ):
        return None
    parent_admission_id = parent_owner_id[len('claim:'):]
    if not parent_admission_id:
        return None
    if (
        parent_admission_id == record.get('admission_id')
        or not _classic_source_parent_valid(authority, parent, parent_admission_id)
    ):
        return None
    try:
        from hermes_state_runtime import get_session_admission
        parent_row = get_session_admission(authority.db, admission_id=parent_admission_id)
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return None
    if not isinstance(parent_row, dict):
        return None
    records = getattr(authority, '_preservation_operations', None)
    candidates = [
        (key, candidate)
        for key, candidate in (records.items() if isinstance(records, dict) else ())
        if isinstance(candidate, dict)
        and candidate.get('owner_class') == 'I06_busy_followup_delivery'
        and candidate.get('admission_id') == parent_admission_id
        and candidate.get('parent_handle') is None
    ]
    if len(candidates) > 1:
        return None
    parent_key, parent_record = candidates[0] if candidates else (None, None)
    parent_owner = parent_record.get('producer_object') if parent_record is not None else None
    from gateway.session_classic_custody import (
        _classic_admission_row_matches,
        classic_direct_route_marker,
        classic_row_is_direct,
    )
    if (
        not classic_row_is_direct(parent_row)
        or parent_row.get('admission_id') != parent_admission_id
        or parent_row.get('target_session_id') != record.get('session_id')
        or parent_row.get('principal_id') != record.get('principal_id')
        or parent_row.get('owner_epoch') != authority.epoch
        or parent_row.get('status') not in {'started', 'terminal'}
        or type(parent_row.get('generation')) is not int
        or parent_row.get('generation') < 1
        or type(parent_row.get('request_id')) is not str
        or not parent_row.get('request_id')
    ):
        return None
    if parent_record is None:
        if (
            parent_row.get('status') != 'terminal'
            or parent_row.get('outcome') != 'completed'
            or not _classic_terminal_parent_receipt_valid(
                authority, record, parent, parent_admission_id,
            )
        ):
            return None
    elif (
        parent_owner is None
        or getattr(parent_owner, '_classic_custody_authority', None) is not authority
        or getattr(parent_owner, '_classic_custody_key', None) != parent_key
        or parent_record.get('unknown') is True
        or classic_direct_route_marker(authority, parent_owner) != 'classic_busy'
        or parent_record.get('generation') != parent_row.get('generation')
        or not _classic_admission_row_matches(
            authority, parent_record, parent_row, allow_terminal=parent_row.get('status') == 'terminal',
        )
    ):
        return None
    return parent, parent_row, parent_record, parent_owner



def release_busy_retired_callback(authority, key, admission_id):
    """Re-enter child/root release only with the exact retired I06 callback witness."""
    matched = _classic_callback_bridge_for_admission(
        authority, admission_id, allow_retired=True,
    )
    retired_owner, retired_bridge = (
        matched if isinstance(matched, tuple) and len(matched) == 2
        else (None, None)
    )
    retired = getattr(retired_bridge, 'retired_operation_record', None)
    if (
        isinstance(admission_id, str)
        and admission_id
        and isinstance(retired, dict)
        and getattr(retired_bridge, 'operation_key', None) == key
        and getattr(retired_bridge, 'admission_id', None) == admission_id
        and getattr(retired_owner, '_classic_custody_authority', None) is authority
        and retired.get('admission_id') == admission_id
        and retired.get('owner_class') == 'I06_busy_followup_delivery'
        and retired.get('producer_object') is retired_owner
        and retired.get('unknown') is not True
    ):
        authority._preservation_release_terminal(admission_id)
        from gateway.session_classic_parent_release import retry_completed_parent_release
        retry_completed_parent_release(authority, retired, _classic_source_parent_valid)

def _classic_callback_parent_witness_valid(authority, owner, record, row, bridge):
    """Require the registered R->P witness and the live opaque parent edge before effects."""
    expected = getattr(bridge, 'post_delivery_callback_expectation', None)
    if (
        not isinstance(expected, ClassicCallbackExpectation)
        or expected.authority is not authority
        or expected.owner_event is not owner
        or expected.execution_event is not getattr(bridge, 'execution_event', None)
        or expected.operation_key != getattr(bridge, 'operation_key', None)
        or expected.admission_id != (record.get('admission_id') if isinstance(record, dict) else None)
        or not isinstance(record, dict)
        or not isinstance(row, dict)
    ):
        return False
    parent = record.get('parent_handle')
    if parent is not expected.parent_handle:
        return False
    if not _callback_record_identity_matches(
        authority, row, record.get('handle'), record, record.get('admission_id'),
    ):
        return False
    return parent is None or classic_parent_admission_proof(authority, record) is not None


def _callback_identity_reason(authority, bound_authority, key, record, owner, session_key, admission_id):
    if bound_authority is not authority or not isinstance(key, str):
        return 'post_delivery_callback_owner_mismatch', None
    identity = record.get('classic_identity') if isinstance(record, dict) else None
    if (
        not isinstance(record, dict)
        or record.get('owner_class') != 'I06_busy_followup_delivery'
        or record.get('producer_object') is not owner
        or record.get('unknown') is True
        or not isinstance(identity, dict)
        or (admission_id is not None and record.get('admission_id') != admission_id)
    ):
        return 'post_delivery_callback_owner_mismatch', None
    if not isinstance(record.get('admission_id'), str) or not record['admission_id']:
        return 'post_delivery_callback_admission_missing', None
    if identity.get('session_key') != session_key:
        return 'post_delivery_callback_session_mismatch', None
    if any(identity.get(field) != getattr(authority, authority_field, None) for field, authority_field in (
        ('profile_id', 'profile_id'), ('process_instance', 'instance_id'),
        ('authority_epoch', 'epoch'),
    )):
        return 'post_delivery_callback_authority_mismatch', None
    if admission_id is not None and record.get('admission_id') != admission_id:
        return 'post_delivery_callback_admission_mismatch', None
    return None, identity


def _callback_row_reason(authority, key, record):
    from gateway.session_classic_custody import _classic_admission_row_matches

    try:
        row = authority._preservation_operation_row(
            key, expected_parent=record.get('parent_handle'))
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
        row = None
    if (
        row is None
        or row.get('admission_id') != record.get('admission_id')
        or not _classic_admission_row_matches(authority, record, row, allow_terminal=True)
        or type(row.get('generation')) is not int
        or row.get('generation') < 1
        or record.get('generation') != row.get('generation')
    ):
        return 'post_delivery_callback_row_mismatch', None
    return None, row


def _callback_bridge_reason(
    authority, adapter, owner, record, identity, row, *, callback_bridge=None,
):
    expected_adapter = None
    with suppress(Exception):
        expected_adapter = authority.runner._delivery_adapter_for(getattr(owner, 'source', None))
    if expected_adapter is not adapter:
        return 'post_delivery_callback_adapter_mismatch', None, None
    from gateway.session_classic_custody import (
        ClassicExecutionEventBridge,
        classic_execution_owner_for_event,
    )

    bridge = callback_bridge if callback_bridge is not None else getattr(owner, '_classic_execution_bridge', None)
    execution_event = getattr(bridge, 'execution_event', None)
    if (
        not isinstance(bridge, ClassicExecutionEventBridge)
        or bridge.owner_event is not owner
        or execution_event is None
        or classic_execution_owner_for_event(execution_event) is not owner
        or bridge.admission_id != record.get('admission_id')
        or bridge.session_id != identity.get('session_id')
        or bridge.principal_id != identity.get('principal_id')
        or bridge.request_id != identity.get('request_id')
        or bridge.occurrence_id != identity.get('occurrence_id')
        or type(bridge.generation) is not int
        or bridge.generation < 1
        or bridge.generation != row.get('generation')
    ):
        return 'post_delivery_callback_execution_mismatch', None, None
    return None, bridge, execution_event


def _callback_receipt_valid(
    execution_event, owner, identity, record, session_key, *, authority=None,
):
    from gateway.session_classic_custody import (
        ClassicExecutionReceipt,
        _classic_writer_proof_valid,
        classic_record_writer_binding_valid,
    )

    receipt = next(
        (
            getattr(holder, '_classic_execution_receipt', None)
            for holder in (owner, execution_event, getattr(execution_event, '_classic_terminal_event', None))
            if isinstance(getattr(holder, '_classic_execution_receipt', None), ClassicExecutionReceipt)
        ),
        None,
    )
    return (
        receipt is not None
        and receipt.session_id == identity.get('session_id')
        and receipt.session_key == session_key
        and receipt.identity == tuple(
            identity.get(field) for field in (
                'owner_class', 'profile_id', 'profile_home', 'process_instance',
                'authority_epoch', 'session_id', 'principal_id', 'request_id',
                'source_identity', 'producer', 'occurrence_id',
            )
        )
        and _classic_writer_proof_valid(receipt.writer_proof, receipt.session_id)
        and classic_record_writer_binding_valid(
            authority, receipt.writer_binding, identity, record,
        )
    )


def _callback_live_reason(authority, identity, row):
    live = authority.sessions.get(identity.get('session_id'))
    execution = getattr(getattr(live, 'event_stream', None), 'execution', None) or {}
    if not execution:
        return None
    executing = getattr(authority, 'executing', None)
    if (
        not isinstance(execution, dict)
        or execution.get('authority_epoch') != authority.epoch
        or execution.get('admission_id') != row.get('admission_id')
        or execution.get('execution_generation') != row.get('generation')
        or not isinstance(executing, dict)
        or executing.get(identity.get('session_id')) != row.get('generation')
    ):
        return 'post_delivery_callback_live_execution_mismatch'
    return None


def _callback_slot_reason(authority, adapter, bridge, execution_event, session_key):
    callback_generation = getattr(execution_event, '_hermes_run_generation', None)
    if type(callback_generation) is not int or callback_generation < 1:
        return 'post_delivery_callback_generation_missing'
    is_current = getattr(authority.runner, '_is_session_run_current', None)
    if not callable(is_current) or not is_current(session_key, callback_generation):
        return 'post_delivery_callback_generation_stale'
    handle = getattr(bridge, 'post_delivery_callback_handle', None)
    expected_callback = getattr(bridge, 'post_delivery_callback', None)
    has_exact = getattr(adapter, 'has_post_delivery_callback_occurrence', None)
    has_marker = getattr(adapter, 'has_post_delivery_callback_occurrence_marker', None)
    if (
        handle is None
        or not callable(expected_callback)
        or not callable(has_exact)
        or not callable(has_marker)
        or not has_exact(
            handle,
            session_key=session_key,
            generation=callback_generation,
            callback=expected_callback,
        )
        or not has_marker(session_key)
    ):
        return 'post_delivery_callback_generation_mismatch'
    if not callable(getattr(type(adapter), '_fire_post_delivery_callback_occurrence', None)):
        return 'post_delivery_callback_consumer_missing'
    return None


def _classic_callback_expectation(
    authority, owner, bridge, callback, run_generation, delivery=None,
):
    from gateway.session_classic_custody import ClassicExecutionEventBridge, _profile_home

    records = getattr(authority, '_preservation_operations', None)
    key = getattr(bridge, 'operation_key', None)
    record = records.get(key) if isinstance(records, dict) and isinstance(key, str) else None
    identity = record.get('classic_identity') if isinstance(record, dict) else None
    execution = getattr(bridge, 'execution_event', None)
    try:
        authority_home = _profile_home(authority)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if (
        not isinstance(bridge, ClassicExecutionEventBridge)
        or not isinstance(key, str)
        or not isinstance(record, dict)
        or record.get('owner_class') != 'I06_busy_followup_delivery'
        or record.get('producer_object') is not owner
        or getattr(owner, '_classic_custody_authority', None) is not authority
        or getattr(owner, '_classic_custody_key', None) != key
        or record.get('unknown') is True
        or not isinstance(identity, dict)
        or bridge.owner_event is not owner
        or bridge.execution_event is None
        or execution is not bridge.execution_event
        or not isinstance(bridge.admission_id, str)
        or bridge.admission_id != record.get('admission_id')
        or type(bridge.generation) is not int
        or bridge.generation < 1
        or record.get('generation') not in (None, 'pre-admission', bridge.generation)
        or identity.get('profile_id') != getattr(authority, 'profile_id', None)
        or identity.get('profile_home') != authority_home
        or identity.get('process_instance') != getattr(authority, 'instance_id', None)
        or identity.get('authority_epoch') != getattr(authority, 'epoch', None)
        or (
            run_generation is not None
            and (type(run_generation) is not int or run_generation < 1)
        )
        or (callback is not None and not callable(callback))
    ):
        return None
    return ClassicCallbackExpectation(
        authority=authority,
        operation_key=key,
        admission_id=bridge.admission_id,
        admission_generation=bridge.generation,
        run_generation=run_generation,
        profile_id=identity.get('profile_id'),
        profile_home=identity.get('profile_home'),
        process_instance=identity.get('process_instance'),
        authority_epoch=identity.get('authority_epoch'),
        parent_handle=record.get('parent_handle'),
        owner_event=owner,
        execution_event=execution,
        callback=callback,
        delivery_obligation_id=(delivery[0] if delivery is not None else None),
        delivery_platform=(delivery[1] if delivery is not None else None),
        delivery_chat_id=(delivery[2] if delivery is not None else None),
        delivery_thread_id=(delivery[3] if delivery is not None else None),
        delivery_adapter_profile=(delivery[4] if delivery is not None else None),
    )


def _classic_expectation_valid(authority, owner, bridge, expected) -> bool:
    if not isinstance(expected, ClassicCallbackExpectation):
        return False
    try:
        from gateway.session_classic_custody import _profile_home
        current_home = _profile_home(authority)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False
    if (
        expected.authority is not authority
        or expected.owner_event is not owner
        or expected.execution_event is not getattr(bridge, 'execution_event', None)
        or getattr(owner, '_classic_custody_authority', None) is not authority
        or expected.operation_key != getattr(bridge, 'operation_key', None)
        or expected.admission_id != getattr(bridge, 'admission_id', None)
        or expected.admission_generation != getattr(bridge, 'generation', None)
        or expected.profile_id != getattr(authority, 'profile_id', None)
        or expected.profile_home != current_home
        or expected.process_instance != getattr(authority, 'instance_id', None)
        or expected.authority_epoch != getattr(authority, 'epoch', None)
    ):
        return False
    actual_run = getattr(expected.execution_event, '_hermes_run_generation', None)
    if actual_run is not None and expected.run_generation is not None and actual_run != expected.run_generation:
        return False
    records = getattr(authority, '_preservation_operations', None)
    record = records.get(expected.operation_key) if isinstance(records, dict) else None
    if record is None:
        return True
    identity = record.get('classic_identity') if isinstance(record, dict) else None
    return (
        isinstance(record, dict)
        and record.get('owner_class') == 'I06_busy_followup_delivery'
        and record.get('producer_object') is owner
        and record.get('admission_id') == expected.admission_id
        and record.get('unknown') is not True
        and record.get('parent_handle') is expected.parent_handle
        and isinstance(identity, dict)
        and identity.get('profile_id') == expected.profile_id
        and identity.get('profile_home') == expected.profile_home
        and identity.get('process_instance') == expected.process_instance
        and identity.get('authority_epoch') == expected.authority_epoch
        and record.get('generation') in (None, 'pre-admission', expected.admission_generation)
    )


def classic_delivery_proof_witness_for_event(event, *, allow_retired=False):
    """Return the exact callback delivery witness, rejecting arbitrary holder projections."""
    owner, bridge = _classic_callback_bridge_for_event(event, allow_retired=allow_retired)
    witness = getattr(bridge, 'post_delivery_callback_delivery_witness', None)
    expected = getattr(bridge, 'post_delivery_callback_expectation', None)
    if (
        bridge is None
        or not isinstance(witness, ClassicDeliveryProofWitness)
        or witness.expectation is not expected
        or not _classic_expectation_valid(
            getattr(owner, '_classic_custody_authority', None), owner, bridge, expected,
        )
        or witness.terminal_event is not getattr(bridge.execution_event, '_classic_terminal_event', None)
        or witness.proof is None
    ):
        return None
    try:
        from gateway.delivery_ledger import DeliveryObligationProof
        proof = witness.proof
        if not isinstance(proof, DeliveryObligationProof) or proof.state != 'delivered':
            return None
        if any(getattr(proof, field, None) != getattr(witness, field, None) for field in (
            'obligation_id', 'session_key', 'platform', 'chat_id', 'thread_id', 'adapter_profile',
        )) or not getattr(proof, 'content_digest', None):
            return None
        if (
            expected.delivery_obligation_id is not None
            and (
                proof.obligation_id != expected.delivery_obligation_id
                or proof.platform != expected.delivery_platform
                or proof.chat_id != expected.delivery_chat_id
                or proof.thread_id != expected.delivery_thread_id
                or proof.adapter_profile != expected.delivery_adapter_profile
            )
        ):
            return None
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        return None
    records = getattr(expected.authority, '_preservation_operations', None)
    record = records.get(expected.operation_key) if isinstance(records, dict) else None
    if record is None:
        return witness if allow_retired and getattr(bridge, 'post_delivery_callback_state', None) == 'settled' else None
    adoption = record.get('_classic_marker_adoption')
    if adoption is not None and getattr(adoption, 'marker_event', None) is not witness.marker_event:
        return None
    return witness


def _retired_classic_callback_witness(authority, row, admission_id, matched):
    """Join the exact retired I06 record to its terminal row and settled callback proof."""
    if not isinstance(row, dict) or row.get('status') != 'terminal':
        return None
    if not isinstance(matched, tuple) or len(matched) != 2:
        return None
    owner, bridge = matched
    from gateway.session_classic_custody import (
        ClassicExecutionEventBridge, _classic_admission_row_matches,
        _profile_home, classic_execution_owner_for_event,
    )
    if (
        not isinstance(bridge, ClassicExecutionEventBridge)
        or bridge.owner_event is not owner
        or getattr(owner, '_classic_custody_authority', None) is not authority
        or getattr(bridge, 'admission_id', None) != admission_id
        or not isinstance(getattr(bridge, 'operation_key', None), str)
        or getattr(bridge, 'execution_event', None) is None
        or classic_execution_owner_for_event(bridge.execution_event) is not owner
    ):
        return None
    operation_key = bridge.operation_key
    record = getattr(bridge, 'retired_operation_record', None)
    if (
        not isinstance(record, dict)
        or record.get('owner_class') != 'I06_busy_followup_delivery'
        or record.get('producer_object') is not owner
        or record.get('admission_id') != admission_id
        or record.get('unknown') is True
        or record.get('handle') is None
        or record.get('classic_identity') is None
    ):
        return None
    adopters = getattr(authority, 'marker_adopters', None)
    adopter = adopters.get(admission_id) if isinstance(adopters, dict) else None
    if adopter is None or classic_execution_owner_for_event(adopter) is not owner:
        return None
    identity = record.get('classic_identity')
    retired_handle = record['handle']
    try:
        profile_home = _profile_home(authority)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if (
        not isinstance(identity, dict)
        or identity.get('profile_id') != getattr(authority, 'profile_id', None)
        or identity.get('profile_home') != profile_home
        or identity.get('process_instance') != getattr(authority, 'instance_id', None)
        or identity.get('authority_epoch') != getattr(authority, 'epoch', None)
        or not isinstance(identity.get('source_identity'), str)
        or not identity.get('source_identity')
        or getattr(retired_handle, 'owner_class', None) != 'I06_busy_followup_delivery'
        or getattr(retired_handle, 'authority_epoch', None) != getattr(authority, 'epoch', None)
        or getattr(retired_handle, 'profile_id', None) != getattr(authority, 'profile_id', None)
        or getattr(retired_handle, 'process_instance', None) != getattr(authority, 'instance_id', None)
    ):
        return None
    if (
        row.get('admission_id') != admission_id
        or not _classic_admission_row_matches(authority, record, row, allow_terminal=True)
    ):
        return None
    parent = record.get('parent_handle')
    parent_proof = None
    if parent is not None:
        try:
            parent_proof = classic_parent_admission_proof(authority, record)
        except (AttributeError, KeyError, LookupError, OSError, RuntimeError,
                sqlite3.Error, TypeError, ValueError):
            return None
        if parent_proof is None or parent_proof[0] is not parent:
            return None
    expected = getattr(bridge, 'post_delivery_callback_expectation', None)
    settlement = getattr(bridge, 'post_delivery_callback_settlement', None)
    delivery = classic_delivery_proof_witness_for_event(owner, allow_retired=True)
    if (
        not isinstance(expected, ClassicCallbackExpectation)
        or not isinstance(settlement, ClassicCallbackSettlement)
        or settlement.expectation is not expected
        or settlement.delivery is not delivery
        or delivery is None
        or not classic_post_delivery_callback_settled(authority, admission_id)
    ):
        return None
    return ClassicRetiredCallbackWitness(
        authority=authority, operation_key=operation_key, admission_id=admission_id,
        owner=owner, bridge=bridge, record=record, row=row, parent_handle=parent,
        parent_proof=parent_proof, settlement=settlement, delivery=delivery,
    )


CLASSIC_CALLBACK_REQUIRED = 'required'
CLASSIC_CALLBACK_NOT_REQUIRED = 'not_required'
CLASSIC_CALLBACK_UNKNOWN = 'unknown'

_NONCLASSIC_CALLBACK_OWNER_CLASSES = frozenset({
    'I01_native_inbound',
    'I02_api_session_turn',
    'I04_fifo_turn_execution',
    'I05_controls_and_unknown',
    'I07_automation_route_wakes',
    'I08_goals_loops_heartbeat',
    'I09_a2a_hosted_kanban',
    'I10_startup_recovery_handoff',
    'I11_session_mutations_finalizers',
})
_AUTOMATION_CALLBACK_OWNER_CLASSES = frozenset({
    'I07_automation_route_wakes',
    'I08_goals_loops_heartbeat',
    'I09_a2a_hosted_kanban',
})


def _recovered_native_callback_owner(authority, admission_id):
    bindings = getattr(authority, '_i10_recovery_bindings', {})
    binding = bindings.get(admission_id) if isinstance(bindings, dict) else None
    handle = binding.get('handle') if isinstance(binding, dict) else None
    if getattr(handle, 'owner_class', None) == 'I10_startup_recovery_handoff':
        return 'I10_startup_recovery_handoff'
    return 'I01_native_inbound'


def _canonical_callback_row(authority, admission_id):
    if not isinstance(admission_id, str) or not admission_id:
        return None
    try:
        from hermes_state_runtime import get_session_admission
        return get_session_admission(authority.db, admission_id=admission_id)
    except (AttributeError, KeyError, LookupError, OSError, RuntimeError,
            sqlite3.Error, TypeError, ValueError):
        return None


def _callback_handle_is_live(authority, handle):
    coordinator_getter = getattr(authority, '_preservation_coordinator', None)
    coordinator = coordinator_getter() if callable(coordinator_getter) else None
    lease = getattr(handle, 'lease', None)
    lease_id = getattr(lease, 'lease_id', None)
    records = getattr(coordinator, '_records', None) if coordinator is not None else None
    if (coordinator is None or not isinstance(lease_id, str)
            or not isinstance(records, dict) or getattr(coordinator, '_lock', None) is None):
        return False
    with coordinator._lock:
        record = records.get(lease_id)
        return (
            record is not None
            and record.handle is handle
            and record.state in {'accepted', 'finalizing'}
            and getattr(handle, 'profile_id', None) == getattr(authority, 'profile_id', None)
            and getattr(handle, 'authority_epoch', None) == getattr(authority, 'epoch', None)
            and getattr(handle, 'process_instance', None) == getattr(authority, 'instance_id', None)
            and getattr(handle, 'coordinator_id', None) == getattr(coordinator, '_coordinator_id', None)
        )


def _callback_parent_edge_is_live_locked(authority, handle, record, coordinator):
    """Validate the exact coordinator parent edge while its lock is held.

    A child operation and its row are not an independent terminal witness.  The parent lease,
    profile, epoch, process, and coordinator must still be the same opaque objects that admitted
    the child.  In particular, a copied ``parent_handle`` or a map entry with a matching id is not
    enough to exempt the child from the callback boundary.  The caller owns ``coordinator._lock``.
    """
    parent = record.get('parent_handle') if isinstance(record, dict) else None
    if parent is None:
        return True
    child_lease = getattr(getattr(handle, 'lease', None), 'parent_lease_id', None)
    parent_lease = getattr(getattr(parent, 'lease', None), 'lease_id', None)
    if not isinstance(child_lease, str) or child_lease != parent_lease:
        return False
    parent_record = getattr(coordinator, '_records', {}).get(parent_lease)
    return (
        parent_record is not None
        and parent_record.handle is parent
        and parent_record.state in {'accepted', 'finalizing'}
        and parent.profile_id == authority.profile_id
        and parent.authority_epoch == authority.epoch
        and parent.process_instance == authority.instance_id
        and parent.coordinator_id == getattr(coordinator, '_coordinator_id', None)
    )


def _callback_record_identity_matches(authority, row, handle, record, admission_id):
    """Compare the operation record, row, and opaque lease as one source-owned identity."""
    if not isinstance(record, dict) or record.get('handle') is not handle:
        return False
    # Classic custody keeps the complete immutable identity in its source-owned nested record;
    # unlike generic operation indexes, its compatibility projection intentionally does not copy
    # profile_id/authority_epoch/process_instance to the top level.  Read that canonical identity
    # without weakening the generic operation path or accepting a classic record that lacks it.
    classic_identity = (
        record.get('classic_identity')
        if record.get('owner_class') == 'I06_busy_followup_delivery'
        else None
    )
    if (
        record.get('owner_class') == 'I06_busy_followup_delivery'
        and not isinstance(classic_identity, dict)
    ):
        return False
    identity = classic_identity if isinstance(classic_identity, dict) else record
    if (identity.get('owner_class') != getattr(handle, 'owner_class', None)
            or identity.get('profile_id') != getattr(handle, 'profile_id', None)
            or identity.get('authority_epoch') != getattr(handle, 'authority_epoch', None)
            or identity.get('authority_epoch') != authority.epoch
            or (classic_identity is not None
                and identity.get('process_instance') != getattr(handle, 'process_instance', None))
            or record.get('session_id') != row.get('target_session_id')
            or record.get('principal_id') != row.get('principal_id')
            or record.get('admission_id') != admission_id):
        return False
    request_id = record.get('admission_request_id', record.get('request_id'))
    if request_id is not None and row.get('request_id') != request_id:
        return False
    generation = record.get('generation')
    if generation is not None and row.get('generation') != generation:
        return False
    coordinator_getter = getattr(authority, '_preservation_coordinator', None)
    coordinator = coordinator_getter() if callable(coordinator_getter) else None
    if coordinator is None:
        return False
    lease = getattr(handle, 'lease', None)
    lease_id = getattr(lease, 'lease_id', None)
    if getattr(coordinator, '_lock', None) is None:
        return False
    with coordinator._lock:
        live = getattr(coordinator, '_records', {}).get(lease_id)
        if (live is None or live.handle is not handle
                or live.state not in {'accepted', 'finalizing'}):
            return False
        return _callback_parent_edge_is_live_locked(authority, handle, record, coordinator)


def _callback_class_for_row(authority, row, operations):
    """Resolve the producer class from the authenticated durable payload.

    This is deliberately narrower than the presence of an arbitrary operation record.  A row
    with an API, native, or automation producer has a server-written discriminator; only a
    single operation owner is accepted when the payload has no such discriminator.
    """
    payload = row.get('payload') if isinstance(row, dict) else None
    if not isinstance(payload, dict):
        return None
    try:
        descriptor = authority._preservation_automation_row_descriptor(row)
    except (AttributeError, KeyError, TypeError, ValueError):
        descriptor = None
    if isinstance(descriptor, dict) and descriptor.get('owner_class') in _AUTOMATION_CALLBACK_OWNER_CLASSES:
        return descriptor['owner_class']
    api_payload = payload.get('api_turn_v1')
    if isinstance(api_payload, dict) and api_payload.get('producer_process_mode') == 'managed_turn':
        return 'I02_api_session_turn'
    native_payload = payload.get('native_text_v1')
    if isinstance(native_payload, dict):
        mode = native_payload.get('producer_process_mode')
        if mode == 'classic_busy':
            return 'I06_busy_followup_delivery'
        if mode == 'managed_turn':
            return _recovered_native_callback_owner(authority, row.get('admission_id'))
    owners = {
        record.get('owner_class') for record in (operations.values() if isinstance(operations, dict) else ())
        if isinstance(record, dict) and record.get('admission_id') == row.get('admission_id')
        and record.get('owner_class') in _NONCLASSIC_CALLBACK_OWNER_CLASSES
    }
    return next(iter(owners)) if len(owners) == 1 else None


def _callback_classic_marker_signal(authority, admission_id):
    """Return true when a classic marker/watcher is still capable of owning delivery."""
    for name in ('held_turn_markers', '_preservation_marker_watchers'):
        registry = getattr(authority, name, None)
        if isinstance(registry, dict) and admission_id in registry:
            return True
    adopters = getattr(authority, 'marker_adopters', None)
    adopter = adopters.get(admission_id) if isinstance(adopters, dict) else None
    return bool(
        adopter is not None
        and (
            getattr(adopter, '_classic_custody_authority', None) is authority
            or getattr(adopter, '_classic_execution_bridge', None) is not None
            or getattr(adopter, '_classic_custody_key', None) is not None
        )
    )


def _authenticated_local_operator_input(authority, row):
    """Recognize the exact server-authored viewer envelope after classic signals are absent."""
    if not isinstance(row, dict) or row.get('status') != 'terminal':
        return False
    if row.get('outcome') not in {'completed', 'failed', 'interrupted', 'rejected', 'cancelled', 'steered'}:
        return False
    payload = row.get('payload')
    marker = payload.get('local_operator_v1') if isinstance(payload, dict) else None
    if marker is not None and not isinstance(marker, dict):
        return False
    try:
        from gateway.session_operator import is_viewer_input
        if not is_viewer_input(row):
            return False
        if marker is not None:
            return marker == {
                'profile_id': authority.profile_id,
                'session_id': row.get('target_session_id'),
                'principal_id': row.get('principal_id'),
            }
        live = getattr(authority, 'sessions', {}).get(row.get('target_session_id'))
        source = getattr(live, 'source', None)
        return isinstance(getattr(source, 'user_id', None), str) and (
            row.get('principal_id') == source.user_id
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def _callback_typed_native_witness(row, handle, record):
    owner_class = getattr(handle, 'owner_class', None)
    payload = row.get('payload')
    if not isinstance(payload, dict):
        return False
    if owner_class in {'I02_api_session_turn', 'I01_native_inbound'}:
        field = 'api_turn_v1' if owner_class == 'I02_api_session_turn' else 'native_text_v1'
        descriptor = payload.get(field)
        return isinstance(descriptor, dict) and descriptor.get('producer_process_mode') == 'managed_turn'
    if owner_class != 'I04_fifo_turn_execution':
        return False
    descriptor = payload.get('native_text_v1')
    return (
        isinstance(record, dict) and record.get('handle') is handle
        and isinstance(record.get('source_identity'), str) and bool(record['source_identity'])
        and record.get('parent_handle') is not None
        and isinstance(descriptor, dict)
        and descriptor.get('producer_process_mode') == 'managed_turn'
    )


def _callback_typed_terminal_witness(authority, row, handle, record, admission_id):
    """Require the producer-specific terminal receipt before declaring callback-free delivery."""
    if row.get('outcome') not in {'completed', 'failed', 'interrupted', 'rejected', 'cancelled', 'steered'}:
        return False
    owner_class = getattr(handle, 'owner_class', None)
    if owner_class in {'I01_native_inbound', 'I02_api_session_turn', 'I04_fifo_turn_execution'}:
        return _callback_typed_native_witness(row, handle, record)
    if owner_class in _AUTOMATION_CALLBACK_OWNER_CLASSES:
        return _callback_descriptor_matches(authority, row, handle, admission_id)
    if owner_class in {'I05_controls_and_unknown', 'I10_startup_recovery_handoff',
                       'I11_session_mutations_finalizers'}:
        return (
            isinstance(record, dict) and record.get('handle') is handle
            and isinstance(record.get('operation'), str) and bool(record['operation'])
            and isinstance(record.get('source_identity'), str) and bool(record['source_identity'])
        )
    return False


def _callback_descriptor_matches(authority, row, handle, admission_id):
    descriptor = authority._preservation_automation_row_descriptor(row)
    if not isinstance(descriptor, dict):
        return False
    binding = getattr(authority, '_preservation_automation_bindings', {}).get(admission_id)
    if isinstance(binding, dict) and binding.get('handle') is handle:
        bound_descriptor = binding.get('descriptor')
        bound_row = binding.get('row')
        if (binding.get('successor') and descriptor == bound_descriptor
                and isinstance(bound_row, dict)
                and all(row.get(field) == bound_row.get(field) for field in (
                    'admission_id', 'target_session_id', 'principal_id', 'request_id',
                ))):
            return True
    expected = {
        'owner_class': getattr(handle, 'owner_class', None),
        'owner_id': getattr(handle, 'owner_id', None),
        'profile_id': getattr(handle, 'profile_id', None),
        'process_instance': getattr(handle, 'process_instance', None),
        'authority_epoch': getattr(handle, 'authority_epoch', None),
        'close_generation': getattr(handle, 'close_generation', None),
        'coordinator_id': getattr(handle, 'coordinator_id', None),
        'session_id': row.get('target_session_id'),
        'principal_id': row.get('principal_id'),
        'request_id': row.get('request_id'),
    }
    if any(descriptor.get(field) != value for field, value in expected.items()):
        return False
    profile_home = descriptor.get('profile_home')
    try:
        expected_home = str(Path(authority.db.db_path).parent.resolve())
    except (AttributeError, OSError, TypeError, ValueError):
        return False
    return (
        isinstance(profile_home, str)
        and profile_home == expected_home
        and isinstance(descriptor.get('source_identity'), str)
        and bool(descriptor['source_identity'])
    )


def _callback_bot_automation_child(authority, row, admission_id, record):
    """Return the exact I07 root witness for one authenticated bot-delivery child.

    Bot delivery owns an I06 child because the mailbox projection is still a delivery occurrence,
    but its terminal callback obligation belongs to the already-bound I07 automation root.  This
    path is deliberately narrower than the owner class: a classic I06 record, a foreign/stale
    parent, an absent child index, or a row without the real automation descriptor remains UNKNOWN.
    """
    if (not isinstance(record, dict)
            or record.get('owner_class') != 'I06_busy_followup_delivery'
            or record.get('operation') != 'bot-delivery'
            or record.get('admission_id') != admission_id
            or record.get('unknown') is True):
        return None
    child = record.get('handle')
    parent = record.get('parent_handle')
    if (child is None or parent is None
            or getattr(parent, 'owner_class', None) != 'I07_automation_route_wakes'
            or getattr(child, 'owner_class', None) != 'I06_busy_followup_delivery'
            or not _callback_record_identity_matches(authority, row, child, record, admission_id)):
        return None
    handles = getattr(authority, '_preservation_handles', None)
    if not isinstance(handles, dict) or handles.get(admission_id) is not parent:
        return None
    children = getattr(authority, '_preservation_automation_children', None)
    operations = getattr(authority, '_preservation_operations', None)
    if not isinstance(children, dict) or not isinstance(operations, dict):
        return None
    child_key = children.get(admission_id)
    if (not isinstance(child_key, str)
            or operations.get(child_key) is not record):
        return None
    try:
        from gateway.session_automation import callback_record_for_automation
        root_record = callback_record_for_automation(authority, row, admission_id, parent)
    except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError):
        root_record = None
    if (not isinstance(root_record, dict)
            or root_record.get('owner_class') != 'I07_automation_route_wakes'
            or root_record.get('handle') is not parent
            or not _callback_row_matches(authority, row, parent, root_record, admission_id)):
        return None
    return root_record


def _callback_row_matches(authority, row, handle, record, admission_id):
    if (not isinstance(row, dict) or row.get('admission_id') != admission_id
            or row.get('status') != 'terminal'
            or not _callback_handle_is_live(authority, handle)):
        return False
    expected = {
        'owner_epoch': getattr(handle, 'authority_epoch', None),
        'target_session_id': record.get('session_id'),
        'principal_id': record.get('principal_id'),
    }
    request_id = record.get('admission_request_id', record.get('request_id'))
    if request_id is not None:
        expected['request_id'] = request_id
    if any(row.get(field) != value for field, value in expected.items()):
        return False
    generation = record.get('generation')
    if generation is not None and row.get('generation') != generation:
        return False
    owner_class = getattr(handle, 'owner_class', None)
    if owner_class == 'I06_busy_followup_delivery':
        # The classic branch has already resolved the exact bridge; keep this check tied to that
        # source-owned occurrence instead of treating the I06 class as a generic nonclassic owner.
        return _classic_callback_bridge_for_admission(authority, admission_id) is not None
    if owner_class not in _NONCLASSIC_CALLBACK_OWNER_CLASSES:
        return False
    if isinstance(record, dict) and record.get('handle') is handle:
        if not _callback_record_identity_matches(authority, row, handle, record, admission_id):
            return False
    elif owner_class not in {'I01_native_inbound', 'I02_api_session_turn'}:
        # A map-only automation/I04/control handle is not a source-owned terminal witness.  The
        # producer record or typed automation descriptor must be available for exact recovery.
        return False
    return _callback_typed_terminal_witness(authority, row, handle, record, admission_id)


def _callback_operation_candidates(authority, row, admission_id, expected_owner, seen):
    candidates = []
    operations = getattr(authority, '_preservation_operations', None)
    if not isinstance(operations, dict):
        return candidates
    for key, record in operations.items():
        if (not isinstance(record, dict)
                or record.get('admission_id') != admission_id):
            continue
        handle = record.get('handle')
        if id(handle) in seen:
            continue
        seen.add(id(handle))
        owner_class = record.get('owner_class')
        if (record.get('unknown')
                or owner_class == 'I03_api_run_direct'
                or owner_class not in _NONCLASSIC_CALLBACK_OWNER_CLASSES):
            return None
        # The canonical row's producer identifies the root.  An I04 child may accompany that
        # root, but it is only admissible when the coordinator carries the exact parent edge.
        if expected_owner is not None and owner_class != expected_owner:
            if (owner_class != 'I04_fifo_turn_execution'
                    or not _callback_handle_is_live(authority, handle)
                    or not _callback_record_identity_matches(authority, row, handle, record, admission_id)):
                return None
            candidates.append((handle, record))
            continue
        try:
            current = authority._preservation_operation_row(
                key, expected_parent=record.get('parent_handle'))
        except (AttributeError, KeyError, LookupError, OSError, RuntimeError,
                sqlite3.Error, TypeError, ValueError):
            return None
        if not isinstance(current, dict) or not _callback_row_matches(
                authority, current, handle, record, admission_id):
            return None
        candidates.append((handle, record))
    return candidates


def _callback_map_candidates(authority, row, admission_id, expected_owner, seen):
    candidates = []
    for handles in (
        getattr(authority, '_preservation_children', None),
        getattr(authority, '_preservation_handles', None),
    ):
        if not isinstance(handles, dict):
            continue
        handle = handles.get(admission_id)
        if handle is None or id(handle) in seen:
            continue
        seen.add(id(handle))
        owner_class = getattr(handle, 'owner_class', None)
        if (owner_class == 'I03_api_run_direct'
                or owner_class not in _NONCLASSIC_CALLBACK_OWNER_CLASSES):
            return None
        if expected_owner is not None and owner_class != expected_owner:
            if (owner_class != 'I04_fifo_turn_execution'
                    or not _callback_handle_is_live(authority, handle)):
                return None
            candidates.append((handle, None))
            continue
        if owner_class in _AUTOMATION_CALLBACK_OWNER_CLASSES:
            try:
                from gateway.session_automation import callback_record_for_automation
                record = callback_record_for_automation(authority, row, admission_id, handle)
            except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError):
                record = None
            if record is None:
                return None
        elif owner_class == 'I10_startup_recovery_handoff':
            try:
                from gateway.session_recovery_custody import callback_record_for_recovery
                record = callback_record_for_recovery(authority, row, admission_id, handle)
            except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError):
                record = None
            if record is None:
                return None
        else:
            record = {
                'admission_id': admission_id,
                'session_id': row.get('target_session_id'),
                'principal_id': row.get('principal_id'),
                'request_id': row.get('request_id'),
                'generation': row.get('generation'),
                'admission_request_id': row.get('request_id'),
            }
        if not _callback_row_matches(authority, row, handle, record, admission_id):
            return None
        candidates.append((handle, record))
    return candidates


def _callback_nested_candidates_valid(authority, candidates, expected_owner):
    roots = [candidate for candidate in candidates
             if expected_owner is None or getattr(candidate[0], 'owner_class', None) == expected_owner]
    if len(roots) != 1:
        return None
    root_handle, _root_record = roots[0]
    coordinator_getter = getattr(authority, '_preservation_coordinator', None)
    coordinator = coordinator_getter() if callable(coordinator_getter) else None
    if coordinator is None or getattr(coordinator, '_lock', None) is None:
        return None
    root_id = getattr(getattr(root_handle, 'lease', None), 'lease_id', None)
    for handle, record in candidates:
        if handle is root_handle:
            continue
        if getattr(handle, 'owner_class', None) != 'I04_fifo_turn_execution':
            return None
        parent_id = getattr(getattr(handle, 'lease', None), 'parent_lease_id', None)
        if parent_id != root_id:
            return None
        if isinstance(record, dict) and record.get('parent_handle') is not root_handle:
            return None
        with coordinator._lock:
            root_record = getattr(coordinator, '_records', {}).get(root_id)
            child_record = getattr(coordinator, '_records', {}).get(
                getattr(getattr(handle, 'lease', None), 'lease_id', None))
            if (root_record is None or root_record.handle is not root_handle
                    or child_record is None or child_record.handle is not handle):
                return None
    return roots


def _nonclassic_callback_candidates(authority, row, admission_id):
    operations = getattr(authority, '_preservation_operations', None)
    expected_owner = _callback_class_for_row(authority, row, operations)
    seen = set()
    candidates = _callback_operation_candidates(authority, row, admission_id, expected_owner, seen)
    if candidates is None:
        return None
    mapped = _callback_map_candidates(authority, row, admission_id, expected_owner, seen)
    if mapped is None:
        return None
    candidates.extend(mapped)
    if not candidates:
        return []
    return _callback_nested_candidates_valid(authority, candidates, expected_owner)


def classic_callback_requirement(authority, admission_id, *, delivery_receipt=None):
    """Classify the exact admission's classic callback obligation.

    ``delivery_receipt`` is supplied only by the real settled-delivery finalizer.  The legacy
    lightweight exemption therefore cannot be reached by an untrusted direct classifier call.

    A required callback is proven by the authenticated classic bridge.  A nonclassic route is
    exempt only when its live opaque lease and terminal canonical row agree.  Missing rows,
    bridges, descriptors, or cross-process I03 evidence stay unknown so the existing terminalizer
    retains custody instead of treating an empty map as proof of no callback.
    """
    row = _canonical_callback_row(authority, admission_id)
    if not isinstance(row, dict):
        return CLASSIC_CALLBACK_UNKNOWN
    records = getattr(authority, '_preservation_operations', None)
    classic = [
        (key, record) for key, record in (records.items() if isinstance(records, dict) else ())
        if (isinstance(record, dict)
            and record.get('owner_class') == 'I06_busy_followup_delivery'
            and record.get('admission_id') == admission_id)
    ]
    if len(classic) > 1:
        return CLASSIC_CALLBACK_UNKNOWN
    matched = _classic_callback_bridge_for_admission(
        authority, admission_id, allow_retired=True,
    )
    if classic:
        _key, record = classic[0]
        bot_root = _callback_bot_automation_child(authority, row, admission_id, record)
        if bot_root is not None:
            # A bot child has no classic callback only when its exact automation root is the sole
            # typed terminal witness. A simultaneously present bridge/marker is an unresolved
            # obligation, never an exemption.
            if matched is not None or _callback_classic_marker_signal(authority, admission_id):
                return CLASSIC_CALLBACK_UNKNOWN
            return CLASSIC_CALLBACK_NOT_REQUIRED
        if record.get('unknown') or matched is None:
            return CLASSIC_CALLBACK_UNKNOWN
        handle = record.get('handle')
        if not _callback_row_matches(authority, row, handle, record, admission_id):
            return CLASSIC_CALLBACK_UNKNOWN
        return CLASSIC_CALLBACK_REQUIRED
    from gateway.session_classic_custody import classic_row_is_direct
    if classic_row_is_direct(row):
        return CLASSIC_CALLBACK_REQUIRED if matched is not None else CLASSIC_CALLBACK_UNKNOWN
    if matched is not None:
        if _retired_classic_callback_witness(authority, row, admission_id, matched) is not None:
            return CLASSIC_CALLBACK_REQUIRED
        # A bridge without a corresponding canonical classic operation is not a nonclassic
        # exemption; preserve the existing fail-closed callback behavior.
        return CLASSIC_CALLBACK_UNKNOWN
    if _callback_classic_marker_signal(authority, admission_id):
        # A marker/adopter/watcher is a source-owned delivery obligation even when the short-lived
        # I06 operation record has already been retired.  Empty nonclassic maps cannot prove its
        # absence, so retain the lease until the marker is settled or an exact bridge is recovered.
        return CLASSIC_CALLBACK_UNKNOWN
    from gateway.session_classic_legacy_callback import legacy_lightweight_no_delivery_callback
    if legacy_lightweight_no_delivery_callback(authority, row, admission_id, delivery_receipt):
        return CLASSIC_CALLBACK_NOT_REQUIRED
    if _authenticated_local_operator_input(authority, row):
        return CLASSIC_CALLBACK_NOT_REQUIRED
    candidates = _nonclassic_callback_candidates(authority, row, admission_id)
    return CLASSIC_CALLBACK_NOT_REQUIRED if candidates else CLASSIC_CALLBACK_UNKNOWN


def classic_post_delivery_callback_settled(authority, admission_id) -> bool:
    """Return whether the exact classic occurrence has passed its authenticated finalizer."""
    records = getattr(authority, '_preservation_operations', None)
    if records is None:
        return False
    if not isinstance(records, dict) or not isinstance(admission_id, str) or not admission_id:
        return False
    candidates = [
        record for record in records.values()
        if (
            isinstance(record, dict)
            and record.get('owner_class') == 'I06_busy_followup_delivery'
            and record.get('admission_id') == admission_id
        )
    ]
    if len(candidates) > 1:
        return False
    matched = _classic_callback_bridge_for_admission(
        authority, admission_id, allow_retired=True,
    )
    if matched is None:
        return False
    owner, bridge = matched
    expected = getattr(bridge, 'post_delivery_callback_expectation', None)
    if not _classic_expectation_valid(authority, owner, bridge, expected):
        return False
    state = getattr(bridge, 'post_delivery_callback_state', 'unclassified')
    if state == 'not_required':
        return not getattr(bridge, 'post_delivery_callback_expected', False)
    if state != 'settled' or not getattr(bridge, 'post_delivery_callback_settled', False):
        return False
    settlement = getattr(bridge, 'post_delivery_callback_settlement', None)
    witness = classic_delivery_proof_witness_for_event(owner, allow_retired=True)
    return (
        isinstance(settlement, ClassicCallbackSettlement)
        and settlement.expectation is expected
        and settlement.delivery is witness
    )


def _complete_classic_callback_refusal(
    authority, key, callback_bridge, result, refusal_reason, source_owned_record,
    prior_error=None,
):
    refusal_error = prior_error
    if source_owned_record:
        try:
            authority._preservation_operation_unknown(key, refusal_reason)
        except BaseException as exc:  # health: allow BLE001 -- preserve authenticated refusal fault
            if refusal_error is None:
                refusal_error = exc
    try:
        _classic_callback_done(callback_bridge, failed=True)
    except BaseException as wake_exc:  # health: allow BLE001 -- retain exact first fault
        if refusal_error is None:
            refusal_error = wake_exc
    if refusal_error is not None:
        raise refusal_error.with_traceback(refusal_error.__traceback__)
    return result


def _classic_callback_refusal_preflight(
    authority, adapter, owner, bound_authority, key, operation_key, record,
    session_key, admission_id, callback_bridge,
):
    if (
        bound_authority is not authority
        or getattr(owner, '_classic_custody_authority', None) is not authority
        or getattr(owner, '_classic_custody_key', None) != key
        or key != operation_key
        or not isinstance(record, dict)
        or record.get('unknown') is True
        or (admission_id is not None and record.get('admission_id') != admission_id)
    ):
        return False
    if (
        callback_bridge is None
        or not _classic_callback_bridge_identity_valid(
            authority, owner, callback_bridge, operation_key=key,
            admission_id=record.get('admission_id'),
        )
        or getattr(callback_bridge, 'post_delivery_callback_expected', False) is not True
        or getattr(callback_bridge, 'post_delivery_callback_pending', False) is not True
        or getattr(callback_bridge, 'post_delivery_callback_executed', False) is True
        or getattr(callback_bridge, 'post_delivery_callback_state', None) in {'settled', 'failed'}
    ):
        return False
    reason, identity = _callback_identity_reason(
        authority, bound_authority, key, record, owner, session_key, admission_id,
    )
    if reason is not None:
        return False
    try:
        from gateway.session_classic_custody import _profile_home
        current_home = _profile_home(authority)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False
    if (
        identity.get('owner_class') != 'I06_busy_followup_delivery'
        or identity.get('profile_home') != current_home
    ):
        return False
    reason, row = _callback_row_reason(authority, key, record)
    if reason is not None:
        return False
    if not _classic_callback_parent_witness_valid(
            authority, owner, record, row, callback_bridge):
        return False
    reason, bridge, execution_event = _callback_bridge_reason(
        authority, adapter, owner, record, identity, row, callback_bridge=callback_bridge,
    )
    if reason is not None or bridge is not callback_bridge:
        return False
    if _callback_live_reason(authority, identity, row) is not None:
        return False
    return _callback_slot_reason(authority, adapter, bridge, execution_event, session_key) is None


def refuse_authority_post_delivery_callback(
    authority, adapter, event, session_key, *, operation_key, reason, admission_id=None,
):
    """Authenticate and refuse one exact callback occurrence without executing its callback.

    A missing receipt handoff is an UNKNOWN custody result, not a successful terminal callback.
    The authority-owned pending-bridge index lets a stale or foreign mutable event attribute wake
    only the bridge that registered this exact operation; it never mutates a foreign holder or
    treats a missing witness as a successful terminal receipt.
    """
    if (
        authority is None
        or adapter is None
        or event is None
        or not isinstance(session_key, str)
        or not session_key
        or not isinstance(operation_key, str)
        or not operation_key
        or not isinstance(reason, str)
        or not reason
    ):
        return False
    from gateway.session_classic_custody import _classic_record_ref

    owner, callback_bridge = _classic_callback_bridge_for_event(event)
    if callback_bridge is None:
        registered = _classic_callback_waiter_entry(
            authority, operation_key=operation_key,
        )
        if registered is not None and registered[0] is owner:
            callback_bridge = registered[1]
    authenticated_admission_id = getattr(callback_bridge, 'admission_id', None)
    if (
        isinstance(authenticated_admission_id, str)
        and authenticated_admission_id
        and admission_id in (None, authenticated_admission_id)
    ):
        admission_id = authenticated_admission_id
    reference = _classic_record_ref(owner)
    if reference is None:
        bound_authority, key, record = authority, operation_key, None
    else:
        bound_authority, key, record = reference

    source_owned_record = (
        bound_authority is authority
        and key == operation_key
        and isinstance(record, dict)
        and getattr(owner, '_classic_custody_authority', None) is authority
        and getattr(owner, '_classic_custody_key', None) == key
        and record.get('producer_object') is owner
        and (
            not isinstance(getattr(callback_bridge, 'admission_id', None), str)
            or record.get('admission_id') == callback_bridge.admission_id
        )
    )
    refusal_reason = reason
    try:
        preflight_ok = _classic_callback_refusal_preflight(
            authority, adapter, owner, bound_authority, key, operation_key, record,
            session_key, admission_id, callback_bridge,
        )
    except BaseException as preflight_error:  # health: allow BLE001 -- wake the exact waiter first
        return _complete_classic_callback_refusal(
            authority, key, callback_bridge, False, refusal_reason, source_owned_record,
            prior_error=preflight_error,
        )
    if not preflight_ok:
        return _complete_classic_callback_refusal(
            authority, key, callback_bridge, False, refusal_reason, source_owned_record,
        )
    return _complete_classic_callback_refusal(
        authority, key, callback_bridge, True, refusal_reason, source_owned_record,
    )


async def wait_for_classic_post_delivery_callback(authority, admission_id):
    """Keep the exact claimed I04 live until its registered classic callback finishes."""
    missing = object()
    registry = getattr(authority, '_classic_callback_waiters', missing)
    if registry is not missing and not isinstance(registry, dict):
        return False
    matched = _classic_callback_waiter_entry(authority, admission_id=admission_id)
    if (
        isinstance(registry, dict)
        and admission_id in registry
        and matched is None
    ):
        return False
    if matched is not None:
        _owner, bridge = matched
        if getattr(bridge, 'post_delivery_callback_state', 'unclassified') == 'not_required':
            return classic_post_delivery_callback_settled(authority, admission_id)
        if getattr(bridge, 'post_delivery_callback_expected', False) is not True:
            return False
        if getattr(bridge, 'post_delivery_callback_pending', False):
            done = getattr(bridge, 'post_delivery_callback_done', None)
            if done is None:
                return False
            await done.wait()
        return classic_post_delivery_callback_settled(authority, admission_id)
    requirement = classic_callback_requirement(authority, admission_id)
    if requirement == CLASSIC_CALLBACK_NOT_REQUIRED:
        return True
    if requirement == CLASSIC_CALLBACK_UNKNOWN:
        return False
    if registry is missing:
        return False
    return classic_post_delivery_callback_settled(authority, admission_id)

async def wait_for_classic_marker_release(authority, admission_id, marker):
    """Wait for the marker and exact callback before the authority's existing terminal release."""
    try:
        await marker.wait()
    except BaseException:
        authority._preservation_marker_watchers.pop(admission_id, None)
        authority._preservation_mark_unknown(admission_id, 'join_failed')
        raise
    authority._preservation_marker_watchers.pop(admission_id, None)
    try:
        requirement = classic_callback_requirement(authority, admission_id)
        if requirement == CLASSIC_CALLBACK_UNKNOWN:
            authority._preservation_mark_unknown(admission_id, 'unmapped_writer')
            return
        settled = (
            True if requirement == CLASSIC_CALLBACK_NOT_REQUIRED
            else await wait_for_classic_post_delivery_callback(authority, admission_id)
        )
    except BaseException:
        authority._preservation_mark_unknown(admission_id, 'join_failed')
        raise
    if settled:
        authority._preservation_release_terminal(admission_id)


def _finish_authority_classic_callback(authority, event) -> bool:
    from gateway.session_classic_custody import _classic_record_ref

    owner, bridge = _classic_callback_bridge_for_event(event)
    if bridge is None or not getattr(bridge, 'post_delivery_callback_authority', False):
        return False
    reference = _classic_record_ref(owner)
    if reference is None:
        _classic_callback_done(bridge, failed=True)
        return False
    receipt = _classic_receipt_for_event(owner)
    proof_witness = classic_delivery_proof_witness_for_event(owner)
    proof = getattr(proof_witness, 'proof', None)
    failure_reason = getattr(owner, '_classic_delivery_failure_reason', None)
    if failure_reason:
        authority._classic_finish(event, success=False, reason=failure_reason)
    elif receipt is None or proof_witness is None or proof is None:
        authority._classic_finish(event, success=False, reason='delivery_proof_missing')
    else:
        finished = authority._classic_finish(
            event, success=True, receipt=replace(receipt, delivery_proof=proof),
        )
        return bool(finished)
    return True


def _delivery_marker_reason(authority, key, record, adapter, event, session_key,
                            delivery_event, delivery_state, marker_event):
    from gateway.session_classic_custody import _classic_delivery_proof_valid

    if delivery_state != 'delivered':
        return 'post_delivery_callback_delivery_failed'
    delivery_event = delivery_event if delivery_event is not None else event
    marker_event = marker_event if marker_event is not None else event
    if not _classic_delivery_proof_valid(
        getattr(delivery_event, '_classic_delivery_proof', None), session_key,
    ):
        return 'post_delivery_callback_delivery_proof_missing'
    if not classic_marker_release_witness_valid(authority, key, record, marker_event):
        return 'post_delivery_callback_marker_missing'
    return None


async def fire_authority_post_delivery_callback(
    authority, adapter, event, session_key, *, admission_id=None, operation_key=None,
    delivery_event=None, delivery_state=None, marker_event=None,
):
    """Consume one authenticated classic callback after the authority-owned send settles."""
    if (
        authority is None
        or adapter is None
        or event is None
        or not isinstance(session_key, str)
        or not session_key
    ):
        return None
    from gateway.session_classic_custody import (
        _classic_record_ref,
        classic_execution_owner_for_event,
    )

    owner = classic_execution_owner_for_event(event)
    caller_bridge = getattr(owner, '_classic_execution_bridge', None)
    reference = _classic_record_ref(owner)
    if reference is None:
        return None
    bound_authority, key, record = reference
    _callback_owner, callback_bridge = _classic_callback_bridge_for_event(event)

    def refuse(reason):
        _classic_callback_done(callback_bridge, failed=True)
        if bound_authority is authority and isinstance(key, str):
            with suppress(Exception):
                authority._preservation_operation_unknown(key, reason)
        return False

    if callback_bridge is None or caller_bridge is not callback_bridge:
        return refuse('post_delivery_callback_bridge_mismatch')
    if operation_key is not None and operation_key != key:
        return refuse('post_delivery_callback_owner_mismatch')
    reason = _delivery_marker_reason(
        authority, key, record, adapter, event, session_key,
        delivery_event, delivery_state, marker_event,
    )
    if reason is not None:
        return refuse(reason)
    reason, identity = _callback_identity_reason(
        authority, bound_authority, key, record, owner, session_key, admission_id,
    )
    if reason is not None:
        return refuse(reason)
    reason, row = _callback_row_reason(authority, key, record)
    if reason is not None:
        return refuse(reason)
    reason, bridge, execution_event = _callback_bridge_reason(
        authority, adapter, owner, record, identity, row, callback_bridge=callback_bridge,
    )
    if reason is not None:
        return refuse(reason)
    if not _classic_callback_parent_witness_valid(
            authority, owner, record, row, bridge):
        return refuse('post_delivery_callback_parent_mismatch')
    if not _callback_receipt_valid(
        execution_event, owner, identity, record, session_key, authority=authority,
    ):
        return refuse('post_delivery_callback_writer_mismatch')
    reason = _callback_live_reason(authority, identity, row)
    if reason is not None:
        return refuse(reason)
    reason = _callback_slot_reason(authority, adapter, bridge, execution_event, session_key)
    if reason is not None:
        return refuse(reason)
    handle = getattr(bridge, 'post_delivery_callback_handle', None)
    fire_occurrence = getattr(adapter, '_fire_post_delivery_callback_occurrence', None)
    if handle is None or not callable(fire_occurrence):
        return refuse('post_delivery_callback_consumer_missing')
    if callback_bridge is not None:
        callback_bridge.post_delivery_callback_authority = True
    try:
        await fire_occurrence(
            session_key, execution_event, handle, consume_generic=True,
        )
        if not _finish_authority_classic_callback(authority, event):
            return refuse('post_delivery_callback_failed')
    except asyncio.CancelledError:
        refuse('post_delivery_callback_cancelled')
        raise
    except Exception as exc:
        logger.debug("Classic post-delivery callback failed: %s", exc, exc_info=True)
        return refuse('post_delivery_callback_failed')
    return True


def _register_classic_callback_occurrence(adapter, session_key, run_generation, owner, callback):
    """Register one classic callback through the Base-owned exact occurrence API."""
    register = getattr(adapter, 'register_post_delivery_callback_occurrence', None)
    if (
        not isinstance(session_key, str)
        or not session_key
        or not callable(register)
        or not callable(callback)
    ):
        return None
    try:
        return register(session_key, callback, generation=run_generation, owner_event=owner)
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
        logger.debug("Classic callback registration failed: %s", exc, exc_info=True)
        return None


def register_classic_post_delivery_callback(
    runner, authority, adapter, event, source, session_key, text, receipt, run_generation,
):
    """Register the existing direct classic receipt callback at the normal turn boundary."""
    from gateway.session_classic_custody import (
        ClassicExecutionEventBridge,
        classic_execution_owner_for_event,
    )

    bridge_owner = classic_execution_owner_for_event(event) if event is not None else None
    bridge = getattr(bridge_owner, '_classic_execution_bridge', None)
    if not isinstance(bridge, ClassicExecutionEventBridge):
        return

    def _set_unregistered():
        bridge.post_delivery_callback = None
        bridge.post_delivery_callback_handle = None
        bridge.post_delivery_callback_expected = True
        bridge.post_delivery_callback_pending = False
        bridge.post_delivery_callback_settled = False
        bridge.post_delivery_callback_state = 'unregistered'
        bridge.post_delivery_callback_expectation = None
        bridge.post_delivery_callback_settlement = None
        bridge.post_delivery_callback_delivery_witness = None
        bridge.post_delivery_callback_done = asyncio.Event()
        bridge.post_delivery_callback_done.set()

    if authority is None or receipt is None or not text or adapter is None:
        if receipt is not None and not text and authority is not None:
            expected = _classic_callback_expectation(authority, bridge_owner, bridge, None, None)
            if expected is not None:
                bridge.post_delivery_callback_expected = False
                bridge.post_delivery_callback_pending = False
                bridge.post_delivery_callback_settled = True
                bridge.post_delivery_callback_state = 'not_required'
                bridge.post_delivery_callback_expectation = expected
                bridge.post_delivery_callback_settlement = None
                bridge.post_delivery_callback_delivery_witness = None
                bridge.post_delivery_callback_done = asyncio.Event()
                bridge.post_delivery_callback_done.set()
                bridge.post_delivery_callback_executed = True
                return
        _set_unregistered()
        return
    if not callable(getattr(adapter, 'register_post_delivery_callback_occurrence', None)):
        _set_unregistered()
        return

    bridge.post_delivery_callback = None
    bridge.post_delivery_callback_handle = None
    bridge.post_delivery_callback_expected = True
    bridge.post_delivery_callback_pending = True
    bridge.post_delivery_callback_authority = False
    bridge.post_delivery_callback_done = asyncio.Event()
    bridge.post_delivery_callback_executed = False
    bridge.post_delivery_callback_settled = False
    bridge.post_delivery_callback_state = 'registering'
    bridge.post_delivery_callback_expectation = None
    bridge.post_delivery_callback_settlement = None
    bridge.post_delivery_callback_delivery_witness = None
    bridge_owner._classic_delivery_failure_reason = None

    def _record_delivery_state(*, proof=None, failure_reason=None, witness=None):
        owner, current_bridge = _classic_callback_bridge_for_event(event)
        execution = getattr(current_bridge, 'execution_event', None)
        terminal = getattr(execution, '_classic_terminal_event', None)
        for holder in (owner, execution, terminal):
            if holder is None:
                continue
            if proof is not None:
                holder._classic_delivery_proof = proof
            if failure_reason is not None:
                holder._classic_delivery_failure_reason = failure_reason
        if current_bridge is not None:
            if witness is not None:
                current_bridge.post_delivery_callback_delivery_witness = witness
                current_bridge.post_delivery_callback_state = 'proof_recorded'
            elif failure_reason is not None:
                current_bridge.post_delivery_callback_state = 'failed'
        return current_bridge

    async def _finish_classic_after_delivery() -> None:
        current_bridge = _classic_callback_bridge_for_event(event)[1]
        if current_bridge is not None:
            current_bridge.post_delivery_callback_executed = True
        try:
            from gateway import delivery_ledger
            from gateway.session_classic_custody import classic_terminal_inbound_id

            message_ref = classic_terminal_inbound_id(event)
            delivery_proof = None
            platform = str(getattr(source.platform, 'value', source.platform))
            thread_id = getattr(source, 'thread_id', None)
            adapter_profile = getattr(adapter, '_owner_profile', None) or 'default'
            if message_ref:
                obligation_id = delivery_ledger.compute_obligation_id(
                    session_key, str(message_ref), text,
                )
                with runner._media_delivery_scope_for_source(source):
                    delivery_proof = await asyncio.to_thread(
                        delivery_ledger.read_obligation,
                        obligation_id,
                        session_key=session_key,
                        platform=platform,
                        chat_id=source.chat_id,
                        thread_id=thread_id,
                        content=text,
                        adapter_profile=adapter_profile,
                        expected_state='delivered',
                    )
            current_bridge = _classic_callback_bridge_for_event(event)[1]
            expected = getattr(current_bridge, 'post_delivery_callback_expectation', None)
            if (
                delivery_proof is None
                or not isinstance(expected, ClassicCallbackExpectation)
                or expected.authority is not authority
                or expected.owner_event is not _classic_callback_bridge_for_event(event)[0]
                or expected.run_generation != run_generation
                or getattr(current_bridge, 'post_delivery_callback_state', None) != 'registered'
            ):
                _record_delivery_state(failure_reason='delivery_proof_missing')
                return
            from gateway.delivery_ledger import DeliveryObligationProof
            if (
                not isinstance(delivery_proof, DeliveryObligationProof)
                or delivery_proof.obligation_id != obligation_id
                or delivery_proof.session_key != session_key
                or delivery_proof.platform != platform
                or delivery_proof.chat_id != str(source.chat_id)
                or delivery_proof.thread_id != (str(thread_id) if thread_id is not None else None)
                or delivery_proof.adapter_profile != adapter_profile
                or delivery_proof.state != 'delivered'
            ):
                _record_delivery_state(failure_reason='delivery_proof_mismatch')
                return
            witness = ClassicDeliveryProofWitness(
                expectation=expected,
                terminal_event=getattr(current_bridge.execution_event, '_classic_terminal_event', None),
                obligation_id=obligation_id,
                proof=delivery_proof,
                session_key=session_key,
                platform=platform,
                chat_id=str(source.chat_id),
                thread_id=(str(thread_id) if thread_id is not None else None),
                adapter_profile=adapter_profile,
                marker_event=(
                    getattr(
                        (getattr(authority, '_preservation_operations', {}) or {}).get(
                            expected.operation_key, {}
                        ).get('_classic_marker_adoption'),
                        'marker_event',
                        expected.owner_event,
                    )
                ),
            )
            _record_delivery_state(proof=delivery_proof, witness=witness)
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error) as exc:
            logger.debug("Classic post-delivery custody reconciliation failed: %s", exc, exc_info=True)
            _record_delivery_state(failure_reason='delivery_proof_readback_failed')

    try:
        delivery_expectation = None
        try:
            from gateway import delivery_ledger
            from gateway.session_classic_custody import classic_terminal_inbound_id

            message_ref = classic_terminal_inbound_id(event)
            if message_ref:
                delivery_expectation = (
                    delivery_ledger.compute_obligation_id(session_key, str(message_ref), text),
                    str(getattr(source.platform, 'value', source.platform)),
                    str(source.chat_id),
                    (str(getattr(source, 'thread_id', None))
                     if getattr(source, 'thread_id', None) is not None else None),
                    getattr(adapter, '_owner_profile', None) or 'default',
                )
        except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError):
            delivery_expectation = None
        expected = _classic_callback_expectation(
            authority, bridge_owner, bridge, _finish_classic_after_delivery, run_generation,
            delivery=delivery_expectation,
        )
        callback_waiters = _classic_callback_waiter_slot(
            authority, bridge_owner, bridge, expected,
        )
        if expected is None or callback_waiters is None:
            _set_unregistered()
            return
        bridge.post_delivery_callback_expectation = expected
        bridge.post_delivery_callback_state = 'registered'
        bridge.post_delivery_callback = _finish_classic_after_delivery
        handle = _register_classic_callback_occurrence(
            adapter, session_key, run_generation, bridge_owner, _finish_classic_after_delivery,
        )
        if handle is None:
            _set_unregistered()
            return
        bridge.post_delivery_callback_handle = handle
        callback_waiters[expected.admission_id] = (
            bridge.operation_key, bridge_owner, bridge, expected,
        )
    except (AttributeError, KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
        logger.debug("Classic post-delivery callback registration failed: %s", exc, exc_info=True)
        _classic_callback_done(bridge, failed=True)
