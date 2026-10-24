"""Managed-route compatibility for source-owned I09 admission replays.

The I09 row is the idempotency record.  A pre-r27 row may not carry the managed-worker
route descriptor, so a validated retry may use that descriptor only as an in-memory route
view; the canonical payload and digest remain unchanged.
"""

import asyncio
import json

from hermes_state_runtime import RuntimeStoreError, find_session_admission
from gateway.session_operator import MANAGED_WORKER_DESCRIPTOR, MANAGED_WORKER_FIELD


_PAYLOAD_FIELDS = frozenset({
    'text', 'finite', 'kanban_card_v1', '_automation_custody_v1', MANAGED_WORKER_FIELD,
})

_BUSINESS_PAYLOAD_FIELDS = ('text', 'finite', 'kanban_card_v1')


def _business_payload(payload):
    return {
        key: payload[key]
        for key in _BUSINESS_PAYLOAD_FIELDS
        if key in payload
    }


def _check_kanban_card(source_identity, request_id, card):
    if card is None:
        return
    if (not isinstance(source_identity, str) or not source_identity.startswith('kanban:')
            or not isinstance(card, dict)
            or set(card) != {'board', 'task_id', 'run_id'}
            or not isinstance(card['board'], str)
            or not isinstance(card['task_id'], str)
            or type(card['run_id']) is not int):
        raise RuntimeStoreError('invalid_params')
    try:
        parts = json.loads(request_id[len('kanban:'):])
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeStoreError('invalid_params') from exc
    if (not isinstance(parts, list) or len(parts) != 3
            or not isinstance(parts[0], str) or not isinstance(parts[1], str)
            or type(parts[2]) is not int
            or card != {'board': parts[0], 'task_id': parts[1], 'run_id': parts[2]}):
        raise RuntimeStoreError('invalid_params')


def _validate_replay_payload(authority, ref, row, *, descriptor, handle,
                             source_identity, delivery_id):
    if not isinstance(row, dict) or row.get('status') not in {'queued', 'started', 'unknown', 'terminal'}:
        raise RuntimeStoreError('preservation_owner_unmapped')
    payload = row.get('payload')
    row_request_id = row.get('request_id')
    stored = authority._preservation_automation_row_descriptor(row)
    if (not isinstance(payload, dict)
            or set(payload) - _PAYLOAD_FIELDS
            or not isinstance(payload.get('text'), str)
            or ('finite' in payload and type(payload['finite']) is not bool)
            or stored != descriptor
            or not isinstance(row_request_id, str)
            or row.get('target_session_id') != ref.session_id
            or row.get('principal_id') != descriptor.get('principal_id')
            or row_request_id != descriptor.get('request_id')):
        raise RuntimeStoreError('preservation_owner_unmapped')
    marker = payload.get(MANAGED_WORKER_FIELD)
    if marker is not None and marker != MANAGED_WORKER_DESCRIPTOR:
        raise RuntimeStoreError('producer_policy_invalid')
    _check_kanban_card(source_identity, row_request_id, payload.get('kanban_card_v1'))
    authority._preservation_validate_i09_descriptor(
        stored, handle=handle, request_id=row_request_id,
        principal_id=row['principal_id'], session_id=ref.session_id,
        source_identity=source_identity, delivery_id=delivery_id)
    if handle is not None and not authority._preservation_automation_row_matches(
            row, handle, allow_terminal=True):
        raise RuntimeStoreError('preservation_owner_unmapped')
    return dict(payload)


async def prepare_i09_payload(authority, ref, request_id, payload, handle, *,
                              descriptor, source_identity, delivery_id=None,
                              kanban_card=None):
    """Build a new I09 payload or replay the exact existing canonical payload."""
    _check_kanban_card(source_identity, request_id, kanban_card)
    existing = await asyncio.to_thread(
        find_session_admission, authority.db,
        principal_id=descriptor['principal_id'], session_id=ref.session_id,
        request_id=request_id,
    )
    from gateway.session_managed_worker import managed_policy
    managed = (callable(getattr(authority.runner, '_adapter_for_source', None))
               and managed_policy(authority, ref) is not None)
    if existing is not None:
        replay = _validate_replay_payload(
            authority, ref, existing, descriptor=descriptor, handle=handle,
            source_identity=source_identity, delivery_id=delivery_id)
        requested = dict(payload)
        if kanban_card is not None:
            requested['kanban_card_v1'] = dict(kanban_card)
        if _business_payload(replay) != _business_payload(requested):
            raise RuntimeStoreError('admission_conflict')
        return replay
    durable_payload = dict(payload)
    if kanban_card is not None:
        durable_payload['kanban_card_v1'] = dict(kanban_card)
    if managed:
        durable_payload[MANAGED_WORKER_FIELD] = dict(MANAGED_WORKER_DESCRIPTOR)
    durable_payload['_automation_custody_v1'] = descriptor
    return durable_payload


def managed_i09_route_row(authority, ref, row):
    """Return a transient managed marker for an authenticated legacy I09 row."""
    from gateway.runtime_service_custody import ServiceCustodyError
    from gateway.session_managed_worker import managed_policy
    if (not callable(getattr(authority.runner, '_adapter_for_source', None))
            or managed_policy(authority, ref) is None):
        raise ServiceCustodyError('managed_producer_binding_missing')
    payload = row.get('payload') if isinstance(row, dict) else None
    if (not isinstance(payload, dict)
            or '_automation_custody_v1' not in payload
            or row.get('status') != 'started'
            or type(row.get('generation')) is not int):
        raise ServiceCustodyError('managed_producer_binding_missing')
    from gateway.session_automation import check_i09_automation
    # This validates the exact retained I09 handle, row, profile, principal and source before
    # the marker is introduced into this transient route view.
    check_i09_automation(authority, ref, row)
    routed = dict(row)
    routed['payload'] = dict(payload)
    routed['payload'][MANAGED_WORKER_FIELD] = dict(MANAGED_WORKER_DESCRIPTOR)
    return routed
