"""Contract tests for the optional public provider retirement consumer."""

from __future__ import annotations

from typing import Any

import pytest

from agent.memory_retirement import (
    MemoryRetirementCallError,
    MemoryRetirementCapabilityError,
    MemoryRetirementValidationError,
    RetirementReceipt,
    capture_retirement_receipt,
    retire_memory_provider,
    validate_retirement_result,
)


RECEIPT = {"provider": "holographic", "home_id": "home-a", "incarnation": "inc-a"}
_MISSING = object()


def result(**updates: Any) -> dict[str, Any]:
    value: dict[str, Any] = {
        "status": "released",
        "provider": "holographic",
        "home_id": "home-a",
        "incarnation": "inc-a",
        "active_store_count": 0,
        "pending_write_count": 0,
        "pending_flush_count": 0,
        "close_failure_count": 0,
        "reason": "released",
    }
    value.update(updates)
    return value


class PublicProvider:
    name = "holographic"

    def __init__(self, *, receipt: Any = RECEIPT, result_value: Any = _MISSING, error: Exception | None = None):
        self.receipt_value = receipt
        self.result_value = result() if result_value is _MISSING else result_value
        self.error = error
        self.receipts: list[dict[str, str]] = []

    def retirement_receipt(self) -> Any:
        return self.receipt_value

    def retire(self, receipt: dict[str, str]) -> Any:
        self.receipts.append(receipt)
        if self.error is not None:
            raise self.error
        return self.result_value


def test_success_uses_provider_receipt_and_preserves_retry_history() -> None:
    provider = PublicProvider(result_value=result(close_failure_count=2))

    observed = retire_memory_provider(provider)

    assert observed.released
    assert observed.close_failure_count == 2
    assert provider.receipts == [RECEIPT]


@pytest.mark.parametrize(
    "raw",
    [
        None,
        {"provider": "holographic", "home_id": "home-a"},
        {**RECEIPT, "extra": "no"},
        {**RECEIPT, "incarnation": 1},
        {"provider": "", "home_id": "home-a", "incarnation": "inc-a"},
    ],
)
def test_receipt_shape_is_exact_and_typed(raw: Any) -> None:
    with pytest.raises(MemoryRetirementValidationError):
        capture_retirement_receipt(PublicProvider(receipt=raw))


@pytest.mark.parametrize(
    "reason",
    ["foreign_receipt", "stale_receipt", "pending_catch_up", "pending_flush", "close_failed", "store_open_failed"],
)
def test_provider_refusals_remain_unknown(reason: str) -> None:
    provider = PublicProvider(result_value=result(status="unknown", reason=reason, active_store_count=1))

    observed = retire_memory_provider(provider)

    assert observed.status == "unknown"
    assert not observed.released
    assert observed.reason == reason


@pytest.mark.parametrize(
    "updates",
    [
        {"status": "absent", "reason": "released"},
        {"status": "unknown", "reason": "released"},
        {"active_store_count": 1},
        {"pending_write_count": 1},
        {"pending_flush_count": 1},
        {"close_failure_count": True},
        {"reason": "legacy_unknown"},
        {"extra": "no"},
        {"provider": "foreign-provider"},
        {"home_id": "foreign-home"},
        {"incarnation": "stale-incarnation"},
    ],
)
def test_malformed_or_false_released_result_is_refused(updates: dict[str, Any]) -> None:
    receipt = RetirementReceipt(**RECEIPT)
    with pytest.raises(MemoryRetirementValidationError):
        validate_retirement_result(result(**updates), receipt)


def test_empty_uninitialized_receipt_can_only_return_unknown() -> None:
    receipt = RetirementReceipt(provider="", home_id="", incarnation="")
    observed = validate_retirement_result(
        {
            "status": "unknown",
            "provider": "",
            "home_id": "",
            "incarnation": "",
            "active_store_count": 0,
            "pending_write_count": 0,
            "pending_flush_count": 0,
            "close_failure_count": 0,
            "reason": "not_initialized",
        },
        receipt,
    )
    assert observed.status == "unknown"

    with pytest.raises(MemoryRetirementValidationError):
        validate_retirement_result(result(), receipt)


def test_missing_legacy_capability_refuses_without_inventing_release() -> None:
    class LegacyProvider:
        name = "legacy"

    with pytest.raises(MemoryRetirementCapabilityError):
        retire_memory_provider(LegacyProvider())


def test_partial_legacy_capability_refuses_without_inventing_release() -> None:
    class ReceiptOnlyProvider:
        name = "holographic"

        def retirement_receipt(self) -> dict[str, str]:
            return RECEIPT

    with pytest.raises(MemoryRetirementCapabilityError):
        retire_memory_provider(ReceiptOnlyProvider())


def test_provider_exception_is_not_converted_to_release() -> None:
    with pytest.raises(MemoryRetirementCallError):
        retire_memory_provider(PublicProvider(error=RuntimeError("provider failed")))


def test_missing_result_is_not_converted_to_release() -> None:
    with pytest.raises(MemoryRetirementValidationError):
        retire_memory_provider(PublicProvider(result_value=None))


@pytest.mark.parametrize("attribute", ["retirement_receipt", "name", "retire"])
def test_raising_public_property_lookup_is_typed_and_redacted(attribute: str) -> None:
    class RaisingProxy(PublicProvider):
        def __getattribute__(self, requested: str) -> Any:
            if requested == attribute:
                raise RuntimeError("opaque provider failure")
            return super().__getattribute__(requested)

    with pytest.raises(MemoryRetirementCallError) as error:
        retire_memory_provider(RaisingProxy())

    assert str(error.value) == "provider public capability lookup was unavailable"
    assert "opaque provider failure" not in str(error.value)
