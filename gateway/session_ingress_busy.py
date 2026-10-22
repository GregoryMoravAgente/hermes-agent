"""Source-owned busy-turn observer phases used by the serial ingress boundary."""

from __future__ import annotations

import asyncio
import logging


logger = logging.getLogger("gateway.session_ingress")


async def _busy_note_error(adapter, delivery_event, accepted, error):
    if isinstance(error, Exception) and not isinstance(error, asyncio.CancelledError) and accepted.done():
        try:
            logger.error('[%s] Queued busy follow-up failed: %s', adapter.name, error, exc_info=True)
            await adapter._notify_turn_error(delivery_event, error)
        except BaseException as notify_error:  # health: allow BLE001 -- retain notification failure
            return notify_error
        return None
    return error


async def _busy_observe_work(adapter, event, delivery_event, session_key, accepted):
    from gateway.session_ingress import _move_marker, deliver_response
    event._turn_marker_handoff = getattr(adapter, 'gateway_runner', None) is not None
    failed = False
    delivery_state = None
    handoff_ok = False
    first_error = None
    try:
        response = await adapter._message_handler(event)
        _move_marker(event, delivery_event)
        delivery_event._turn_marker_handoff = event._turn_marker_handoff
        delivery_state = await deliver_response(adapter, delivery_event, session_key, response)
        from gateway.session_classic_custody import handoff_classic_execution_receipt
        classic_bridge = getattr(event, '_classic_execution_bridge', None)
        classic_execution = getattr(classic_bridge, 'execution_event', None)
        handoff_ok = (
            classic_execution is not None
            and handoff_classic_execution_receipt(event, classic_execution)
        )
        if not handoff_ok or delivery_state != 'delivered':
            failed = True
    except BaseException as exc:  # health: allow BLE001 -- preserve the first cleanup exception
        failed = True
        first_error = await _busy_note_error(adapter, delivery_event, accepted, exc)
    return failed, delivery_state, handoff_ok, first_error


async def _busy_release_markers(adapter, delivery_event, event, first_error):
    cleanup_failed = False
    for marker_event in (delivery_event, event):
        try:
            await adapter._release_turn_marker(marker_event)
        except BaseException as exc:  # health: allow BLE001 -- preserve first cleanup fault
            cleanup_failed = True
            if first_error is None:
                first_error = exc
    return cleanup_failed, first_error


async def _busy_reconcile_callback(
    adapter, event, session_key, authority, preservation_key, delivery_event,
    delivery_state, handoff_ok, accepted, first_error,
):
    if authority is None or not accepted.done():
        return False, first_error
    if not handoff_ok:
        try:
            from gateway.session_classic_delivery import refuse_authority_post_delivery_callback
            refuse_authority_post_delivery_callback(
                authority, adapter, event, session_key,
                operation_key=preservation_key,
                admission_id=getattr(
                    getattr(event, '_classic_execution_bridge', None), 'admission_id', None,
                ),
                reason='post_delivery_callback_receipt_handoff_missing',
            )
        except BaseException as exc:  # health: allow BLE001 -- retain first refusal fault
            if first_error is None:
                first_error = exc
        return True, first_error
    try:
        from gateway.session_classic_delivery import fire_authority_post_delivery_callback
        callback_status = await fire_authority_post_delivery_callback(
            authority, adapter, event, session_key, operation_key=preservation_key,
            delivery_event=delivery_event, delivery_state=delivery_state,
            marker_event=event,
        )
        return callback_status is not True, first_error
    except BaseException as exc:  # health: allow BLE001 -- retain first callback fault
        if first_error is None:
            first_error = exc
        return True, first_error


def _busy_finish_observer(
    finish_busy_preservation, authority, preservation_key, accepted,
    failed, cleanup_failed, callback_failed, first_error,
):
    try:
        finish_busy_preservation(
            authority, preservation_key, accepted.done(),
            unknown=failed or cleanup_failed or callback_failed,
        )
    except BaseException as exc:  # health: allow BLE001 -- finish is still exactly once
        if first_error is None:
            first_error = exc
    return first_error


async def observe_shared_busy(
    adapter, event, delivery_event, session_key, authority, preservation_key,
    accepted, finish_busy_preservation,
):
    failed, delivery_state, handoff_ok, first_error = await _busy_observe_work(
        adapter, event, delivery_event, session_key, accepted,
    )
    cleanup_failed, first_error = await _busy_release_markers(
        adapter, delivery_event, event, first_error,
    )
    callback_failed, first_error = await _busy_reconcile_callback(
        adapter, event, session_key, authority, preservation_key, delivery_event,
        delivery_state, handoff_ok, accepted, first_error,
    )
    first_error = _busy_finish_observer(
        finish_busy_preservation, authority, preservation_key, accepted,
        failed, cleanup_failed, callback_failed, first_error,
    )
    if first_error is not None:
        raise first_error.with_traceback(first_error.__traceback__)
