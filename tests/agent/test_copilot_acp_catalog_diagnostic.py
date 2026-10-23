"""Windows-only observation of the retained external ACP close retry.

The diagnostic delegates the scenario and all assertions to the existing catalog retry
test.  It wraps only the existing receipt, native transaction, and coordinator methods so
the native implementation runs unchanged while its phase and identity evidence is printed
as one JSON record for the host-native RCA.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from gateway.runtime_service_custody import RuntimeCustodyCoordinator, ServiceProcessReceipt
from tests.agent.test_copilot_acp_failure_retention import (
    test_external_catalog_physical_close_unknown_retries_same_receipt as _run_original_catalog_test,
)
from tools.process_registry_windows import WindowsLaunchTransaction


def _poll(process: Any) -> int | str | None:
    if process is None:
        return None
    try:
        return process.poll()
    except BaseException as exc:  # diagnostic observation must not replace the test result
        return f"error:{type(exc).__name__}"


def _result_code(result: Any) -> str | None:
    code = getattr(result, "code", None)
    return code if isinstance(code, str) else None


def _native_snapshot(transaction: Any, receipt: Any) -> dict[str, Any] | None:
    if transaction is None:
        return None
    process = getattr(receipt, "process", None) if receipt is not None else None
    transaction_process = getattr(transaction, "process", None)
    return {
        "state": getattr(transaction, "state", None),
        "assigned": getattr(transaction, "assigned", None),
        "assignment_attempted": getattr(transaction, "assignment_attempted", None),
        "assignment_observation": getattr(transaction, "assignment_observation", None),
        "scope_captured": getattr(transaction, "scope_captured", None),
        "scope_observation": getattr(transaction, "scope_observation", None),
        "native_empty_observed": getattr(transaction, "native_empty_observed", None),
        "process_poll": _poll(transaction_process),
        "pid": getattr(transaction, "pid", None),
        "host_start_time": getattr(transaction, "host_start_time", None),
        "process_handle_closed": getattr(transaction, "process_handle_closed", None),
        "job_handle_closed": getattr(transaction, "job_handle_closed", None),
        "close_attempts": getattr(transaction, "close_attempts", None),
        "close_failures": getattr(transaction, "close_failures", None),
        "close_retryable": getattr(transaction, "close_retryable", None),
        "identity_match": bool(
            transaction_process is process
            and getattr(transaction, "pid", None) == getattr(receipt, "pid", None)
            and getattr(transaction, "host_start_time", None)
            == getattr(receipt, "process_started_at", None)
        ) if receipt is not None else None,
    }


def _receipt_snapshot(receipt: Any) -> dict[str, Any] | None:
    if receipt is None:
        return None
    handle = getattr(receipt, "handle", None)
    proof = getattr(receipt, "external_publication_proof", None)
    transaction = getattr(receipt, "transaction", None)
    owner_id = getattr(receipt, "owner_id", None)
    handle_owner_id = getattr(handle, "owner_id", None)
    proof_operation_id = getattr(proof, "operation_id", None)
    process = getattr(receipt, "process", None)
    transaction_process = getattr(transaction, "process", None)
    return {
        "owner_id": owner_id,
        "handle_owner_id": handle_owner_id,
        "proof_operation_id": proof_operation_id,
        "state": getattr(receipt, "state", None),
        "unknown_reason": getattr(receipt, "unknown_reason", None),
        "process_poll": _poll(process),
        "native_transaction": _native_snapshot(transaction, receipt),
        "binding": {
            "receipt_owner_matches_handle": owner_id == handle_owner_id,
            "proof_operation_matches_owner": owner_id in {
                f"{proof_operation_id}:physical",
                f"external:{proof_operation_id}:physical",
            } if isinstance(proof_operation_id, str) else False,
            "proof_profile_matches_handle": (
                getattr(proof, "profile_id", None) == getattr(handle, "profile_id", None)
            ),
            "proof_process_matches_handle": (
                getattr(proof, "process_instance", None)
                == getattr(handle, "process_instance", None)
            ),
            "proof_epoch_matches_handle": (
                getattr(proof, "authority_epoch", None)
                == getattr(handle, "authority_epoch", None)
            ),
            "transaction_custody_is_receipt": (
                getattr(transaction, "custody", None) is receipt
            ),
            "transaction_process_is_receipt_process": transaction_process is process,
            "transaction_pid_matches_receipt": (
                getattr(transaction, "pid", None) == getattr(receipt, "pid", None)
            ),
            "transaction_start_matches_receipt": (
                getattr(transaction, "host_start_time", None)
                == getattr(receipt, "process_started_at", None)
            ),
        },
    }


def _handle_snapshot(coordinator: Any, handle: Any) -> dict[str, Any]:
    lease = getattr(handle, "lease", None)
    lease_id = getattr(lease, "lease_id", None)
    record = getattr(coordinator, "_records", {}).get(lease_id)
    return {
        "owner_id": getattr(handle, "owner_id", None),
        "profile_id": getattr(handle, "profile_id", None),
        "process_instance": getattr(handle, "process_instance", None),
        "authority_epoch": getattr(handle, "authority_epoch", None),
        "owner_class": getattr(handle, "owner_class", None),
        "record_state": getattr(record, "state", None),
        "record_reason": getattr(record, "reason", None),
    }


@pytest.mark.platforms("windows")
def test_windows_external_catalog_close_retry_diagnostic(tmp_path, monkeypatch, record_property):
    """Run the original scenario and emit only observational native phase evidence."""

    events: list[dict[str, Any]] = []

    original_receipt_close = ServiceProcessReceipt.close_after_delivery

    def observe_receipt_close(receipt: Any, *args: Any, **kwargs: Any):
        before = _receipt_snapshot(receipt)
        try:
            result = original_receipt_close(receipt, *args, **kwargs)
        except BaseException as exc:
            events.append({
                "stage": "receipt.close_after_delivery",
                "before": before,
                "raised": type(exc).__name__,
                "after": _receipt_snapshot(receipt),
            })
            raise
        events.append({
            "stage": "receipt.close_after_delivery",
            "before": before,
            "result": result,
            "after": _receipt_snapshot(receipt),
        })
        return result

    original_observe_empty = WindowsLaunchTransaction.observe_empty

    def observe_native_empty(transaction: Any, *args: Any, **kwargs: Any):
        receipt = getattr(transaction, "custody", None)
        before = _receipt_snapshot(receipt)
        try:
            result = original_observe_empty(transaction, *args, **kwargs)
        except BaseException as exc:
            events.append({
                "stage": "native.observe_empty",
                "before": before,
                "raised": type(exc).__name__,
                "after": _receipt_snapshot(receipt),
            })
            raise
        events.append({
            "stage": "native.observe_empty",
            "before": before,
            "result": result,
            "after": _receipt_snapshot(receipt),
        })
        return result

    original_native_close = WindowsLaunchTransaction.close_after_observed_empty

    def observe_native_close(transaction: Any, *args: Any, **kwargs: Any):
        receipt = getattr(transaction, "custody", None)
        before = _receipt_snapshot(receipt)
        try:
            result = original_native_close(transaction, *args, **kwargs)
        except BaseException as exc:
            events.append({
                "stage": "native.close_after_observed_empty",
                "before": before,
                "raised": type(exc).__name__,
                "after": _receipt_snapshot(receipt),
            })
            raise
        events.append({
            "stage": "native.close_after_observed_empty",
            "before": before,
            "result": result,
            "after": _receipt_snapshot(receipt),
        })
        return result

    original_reconcile_unknown = RuntimeCustodyCoordinator.reconcile_unknown

    def observe_reconcile_unknown(coordinator: Any, handle: Any, *args: Any, **kwargs: Any):
        before = _handle_snapshot(coordinator, handle)
        try:
            result = original_reconcile_unknown(coordinator, handle, *args, **kwargs)
        except BaseException as exc:
            events.append({
                "stage": "coordinator.reconcile_unknown",
                "before": before,
                "raised": type(exc).__name__,
                "after": _handle_snapshot(coordinator, handle),
            })
            raise
        events.append({
            "stage": "coordinator.reconcile_unknown",
            "before": before,
            "result": _result_code(result),
            "accepted": result is None,
            "after": _handle_snapshot(coordinator, handle),
        })
        return result

    original_mark_terminal = RuntimeCustodyCoordinator.mark_terminal

    def observe_mark_terminal(coordinator: Any, handle: Any, *args: Any, **kwargs: Any):
        before = _handle_snapshot(coordinator, handle)
        try:
            result = original_mark_terminal(coordinator, handle, *args, **kwargs)
        except BaseException as exc:
            events.append({
                "stage": "coordinator.mark_terminal",
                "before": before,
                "raised": type(exc).__name__,
                "after": _handle_snapshot(coordinator, handle),
            })
            raise
        events.append({
            "stage": "coordinator.mark_terminal",
            "before": before,
            "result": _result_code(result),
            "accepted": result is None,
            "after": _handle_snapshot(coordinator, handle),
        })
        return result

    monkeypatch.setattr(ServiceProcessReceipt, "close_after_delivery", observe_receipt_close)
    monkeypatch.setattr(WindowsLaunchTransaction, "observe_empty", observe_native_empty)
    monkeypatch.setattr(
        WindowsLaunchTransaction, "close_after_observed_empty", observe_native_close,
    )
    monkeypatch.setattr(RuntimeCustodyCoordinator, "reconcile_unknown", observe_reconcile_unknown)
    monkeypatch.setattr(RuntimeCustodyCoordinator, "mark_terminal", observe_mark_terminal)

    outcome: dict[str, Any] = {"status": "passed"}
    try:
        _run_original_catalog_test(tmp_path, monkeypatch)
    except BaseException as exc:
        outcome = {"status": "failed", "exception": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        diagnostic = json.dumps({
            "diagnostic": "external_catalog_physical_close_unknown_retry",
            "original_test": "test_external_catalog_physical_close_unknown_retries_same_receipt",
            "outcome": outcome,
            "events": events,
        }, sort_keys=True)
        record_property("native_catalog_retry_diagnostic", diagnostic)
        print(diagnostic, flush=True)
