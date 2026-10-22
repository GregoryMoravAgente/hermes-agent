"""Compatibility custody for legacy lightweight classic runners."""

from __future__ import annotations


def legacy_lightweight_no_delivery_callback(
    authority, row, admission_id, delivery_receipt,
) -> bool:
    """Recognize the one typed no-delivery terminal boundary of an old lightweight runner."""
    native = row.get('payload', {}).get('native_text_v1') if isinstance(row.get('payload'), dict) else None
    records = getattr(authority, '_preservation_operations', None)
    runner = getattr(authority, 'runner', None)
    lightweight_runner = (
        runner is not None
        and not hasattr(runner, 'session_runtime_descriptor')
        and not hasattr(runner, 'session_authorities')
        and not hasattr(runner, 'runtime_custody')
    )
    try:
        from gateway.session_ingress import DeliveryReceipt
        typed_no_delivery = (
            type(delivery_receipt) is DeliveryReceipt
            and delivery_receipt.admission_id == admission_id
            and delivery_receipt.attempted is False
            and delivery_receipt.delivered is False
            and delivery_receipt.reason == 'no_delivery'
            and delivery_receipt.effect_state == 'none'
        )
    except (ImportError, AttributeError, TypeError):
        typed_no_delivery = False
    return (
        row.get('status') == 'terminal'
        and lightweight_runner
        and getattr(authority, '_preservation_coordinator', lambda: None)() is None
        and isinstance(records, dict)
        and not records
        and isinstance(native, dict)
        and 'producer_process_mode' not in native
        and typed_no_delivery
    )
