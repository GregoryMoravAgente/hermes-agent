"""Strict consumer boundary for an optional provider-owned memory retirement API.

The provider owns the home and incarnation identity.  Callers obtain that identity
through the provider's public receipt method and cannot supply a replacement home
or generation to make a release succeed.  This module deliberately has no import
dependency on an optional provider package.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final, Literal


RETIREMENT_RECEIPT_KEYS: Final[frozenset[str]] = frozenset({"provider", "home_id", "incarnation"})
RETIREMENT_RESULT_KEYS: Final[tuple[str, ...]] = (
    "status",
    "provider",
    "home_id",
    "incarnation",
    "active_store_count",
    "pending_write_count",
    "pending_flush_count",
    "close_failure_count",
    "reason",
)
RETIREMENT_STATUSES: Final[frozenset[str]] = frozenset({"released", "unknown"})
PREPARATION_RESULT_KEYS: Final[frozenset[str]] = frozenset({"status", "receipt", "lease", "reason"})
PREPARATION_STATUSES: Final[frozenset[str]] = frozenset({"prepared", "unknown"})
RETIREMENT_REASONS: Final[frozenset[str]] = frozenset({
    "accepted_write_failed",
    "not_initialized",
    "no_identity",
    "foreign_receipt",
    "stale_receipt",
    "retirement_in_progress",
    "pending_flush",
    "pending_catch_up",
    "flush_failed",
    "close_failed",
    "stale_store",
    "store_open_failed",
    "released",
})


class MemoryRetirementError(Exception):
    """Base class for a fail-closed retirement refusal."""


class MemoryRetirementValidationError(MemoryRetirementError, ValueError):
    """A public receipt or result does not match the provider contract."""


class MemoryRetirementCapabilityError(MemoryRetirementError):
    """The loaded provider is legacy or does not expose the optional capability."""


class MemoryRetirementCallError(MemoryRetirementError):
    """The provider call did not produce an observable public result."""


@dataclass(frozen=True, slots=True)
class RetirementReceipt:
    """Provider-owned identity captured before invoking ``retire``."""

    provider: str
    home_id: str
    incarnation: str

    @property
    def initialized(self) -> bool:
        return bool(self.provider and self.home_id and self.incarnation)

    def as_mapping(self) -> dict[str, str]:
        """Return the exact JSON-compatible shape accepted by the provider."""
        return {
            "provider": self.provider,
            "home_id": self.home_id,
            "incarnation": self.incarnation,
        }


@dataclass(frozen=True, slots=True)
class RetirementResult:
    """Validated public provider result; ``released`` is never inferred locally."""

    status: Literal["released", "unknown"]
    provider: str
    home_id: str
    incarnation: str
    active_store_count: int
    pending_write_count: int
    pending_flush_count: int
    close_failure_count: int
    reason: str

    @property
    def released(self) -> bool:
        return self.status == "released"


@dataclass(frozen=True, slots=True)
class RetirementAttempt:
    """One provider attempt recorded by the Core manager."""

    provider: str
    status: Literal["released", "unknown"]
    reason: str
    result: RetirementResult | None = None


@dataclass(frozen=True, slots=True)
class MemoryRetirementReport:
    """Host-owned aggregate result; release is true only when every provider released."""

    status: Literal["released", "unknown"]
    reason: str
    attempts: tuple[RetirementAttempt, ...] = ()

    @property
    def released(self) -> bool:
        return self.status == "released"


@dataclass(frozen=True, slots=True)
class ProviderRetirementPreparation:
    """Typed result of a reversible provider admission fence.

    ``lease`` is intentionally opaque to Core.  A provider may return ``None`` for an
    unknown result only when it refused before taking its own fence; once it has accepted
    the fence it must retain the same source-owned object for resume/commit retry.
    """

    status: Literal["prepared", "unknown"]
    receipt: RetirementReceipt
    lease: Any | None
    reason: str

    @property
    def prepared(self) -> bool:
        return self.status == "prepared"


@dataclass(slots=True, eq=False)
class ManagerRetirementLease:
    """Opaque manager lease binding one exact provider graph to one preparation fence."""

    manager: Any = field(repr=False, compare=False)
    providers: tuple[Any, ...] = field(repr=False, compare=False)
    receipts: tuple[RetirementReceipt, ...]
    provider_leases: list[Any | None] = field(repr=False)
    provider_prepared: list[bool] = field(repr=False)
    provider_attempts: list[RetirementAttempt | None] = field(repr=False)
    provider_reasons: list[str] = field(repr=False)
    scope: tuple[Any, Any]
    setup_generation: int
    provider_preparation_calls: list[Any | None] = field(repr=False)
    preparation_deadline: float | None = field(default=None, repr=False)
    manager_work_drained: bool = False
    phase: str = "draining"
    reason: str = "preparation_in_progress"


@dataclass(frozen=True, slots=True)
class ManagerPreparationReport:
    """Manager-owned reversible preparation result retained until resume or commit."""

    status: Literal["prepared", "unknown"]
    reason: str
    lease: ManagerRetirementLease | None

    @property
    def prepared(self) -> bool:
        return self.status == "prepared"


def has_retirement_capability(provider: Any) -> bool:
    """Return whether *provider* overrides both optional retirement hooks.

    The hooks are optional on :class:`MemoryProvider` so legacy plugins remain loadable.  A
    legacy inherited default is therefore not a capability, even though it is callable.
    """
    try:
        receipt_fn = _public_attribute(provider, "retirement_receipt")
        retire_fn = _public_attribute(provider, "retire")
        if not callable(receipt_fn) or not callable(retire_fn):
            return False
        from agent.memory_provider import MemoryProvider
        return (
            getattr(type(provider), "retirement_receipt", None) is not MemoryProvider.retirement_receipt
            and getattr(type(provider), "retire", None) is not MemoryProvider.retire
        )
    except MemoryRetirementCallError:
        raise
    except Exception:  # health: allow BLE001 -- optional provider proxies can raise arbitrary lookup errors; fail closed
        return False


def has_two_phase_retirement_capability(provider: Any) -> bool:
    """Return whether the loaded provider exposes the complete reversible contract."""
    if not has_retirement_capability(provider):
        return False
    try:
        from agent.memory_provider import MemoryProvider

        return all(
            callable(_public_attribute(provider, name))
            and getattr(type(provider), name, None) is not getattr(MemoryProvider, name)
            for name in ("prepare_retirement", "resume_retirement", "commit_retirement")
        )
    except MemoryRetirementCallError:
        raise
    except (AttributeError, TypeError):
        return False


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise MemoryRetirementValidationError(f"{label} must be a mapping")
    return value


def _text(value: Any, field: str) -> str:
    if type(value) is not str:
        raise MemoryRetirementValidationError(f"{field} must be a string")
    return value


def _count(value: Any, field: str) -> int:
    if type(value) is not int or value < 0:
        raise MemoryRetirementValidationError(f"{field} must be a non-negative integer")
    return value


def validate_retirement_receipt(value: Any) -> RetirementReceipt:
    """Validate the exact receipt shape returned by the public provider method."""
    receipt = _mapping(value, "retirement receipt")
    if set(receipt) != RETIREMENT_RECEIPT_KEYS:
        raise MemoryRetirementValidationError("retirement receipt keys are not exact")
    parsed = RetirementReceipt(
        provider=_text(receipt["provider"], "receipt.provider"),
        home_id=_text(receipt["home_id"], "receipt.home_id"),
        incarnation=_text(receipt["incarnation"], "receipt.incarnation"),
    )
    if any((parsed.provider, parsed.home_id, parsed.incarnation)) and not parsed.initialized:
        raise MemoryRetirementValidationError("receipt identity must be wholly empty or initialized")
    return parsed


def validate_retirement_result(value: Any, receipt: RetirementReceipt) -> RetirementResult:
    """Validate a result against the exact source-owned receipt used for the call."""
    if not isinstance(receipt, RetirementReceipt):
        raise MemoryRetirementValidationError("result validation requires a captured receipt")
    result = _mapping(value, "retirement result")
    if set(result) != set(RETIREMENT_RESULT_KEYS):
        raise MemoryRetirementValidationError("retirement result keys are not exact")

    status = _text(result["status"], "result.status")
    provider = _text(result["provider"], "result.provider")
    home_id = _text(result["home_id"], "result.home_id")
    incarnation = _text(result["incarnation"], "result.incarnation")
    reason = _text(result["reason"], "result.reason")
    if status not in RETIREMENT_STATUSES:
        raise MemoryRetirementValidationError("retirement status is unsupported")
    if reason not in RETIREMENT_REASONS:
        raise MemoryRetirementValidationError("retirement reason is unsupported")
    if (provider, home_id, incarnation) != (receipt.provider, receipt.home_id, receipt.incarnation):
        raise MemoryRetirementValidationError("retirement result identity does not match its receipt")

    parsed = RetirementResult(
        status=status,  # type: ignore[arg-type]
        provider=provider,
        home_id=home_id,
        incarnation=incarnation,
        active_store_count=_count(result["active_store_count"], "result.active_store_count"),
        pending_write_count=_count(result["pending_write_count"], "result.pending_write_count"),
        pending_flush_count=_count(result["pending_flush_count"], "result.pending_flush_count"),
        close_failure_count=_count(result["close_failure_count"], "result.close_failure_count"),
        reason=reason,
    )
    if parsed.status == "released":
        if parsed.reason != "released" or not receipt.initialized:
            raise MemoryRetirementValidationError("released requires an initialized source-owned receipt")
        if any((parsed.active_store_count, parsed.pending_write_count, parsed.pending_flush_count)):
            raise MemoryRetirementValidationError("released requires zero active and pending work")
    elif parsed.reason == "released":
        raise MemoryRetirementValidationError("unknown cannot carry the released reason")
    return parsed


def _public_attribute(provider: Any, name: str) -> Any:
    """Read an optional public provider attribute without exposing proxy errors."""
    try:
        return getattr(provider, name, None)
    except Exception as exc:  # Optional provider boundaries fail closed without exposing errors.
        raise MemoryRetirementCallError("provider public capability lookup was unavailable") from exc


def capture_retirement_receipt(provider: Any) -> RetirementReceipt:
    """Capture and validate the provider-owned receipt without accepting caller identity."""
    if not has_retirement_capability(provider):
        raise MemoryRetirementCapabilityError("provider has no retirement capability")
    receipt_fn = _public_attribute(provider, "retirement_receipt")
    if not callable(receipt_fn):
        raise MemoryRetirementCapabilityError("provider has no retirement receipt capability")
    try:
        raw_receipt = receipt_fn()
    except Exception as exc:  # Optional provider boundaries fail closed without exposing errors.
        raise MemoryRetirementCallError("provider retirement receipt was unavailable") from exc
    receipt = validate_retirement_receipt(raw_receipt)
    provider_name = _public_attribute(provider, "name")
    if receipt.provider and provider_name is not None and provider_name != receipt.provider:
        raise MemoryRetirementValidationError("provider name does not match its retirement receipt")
    return receipt


def validate_retirement_preparation(value: Any, receipt: RetirementReceipt) -> ProviderRetirementPreparation:
    """Validate the exact four-key provider preparation result."""
    if not isinstance(receipt, RetirementReceipt):
        raise MemoryRetirementValidationError("preparation validation requires a captured receipt")
    result = _mapping(value, "retirement preparation")
    if set(result) != PREPARATION_RESULT_KEYS:
        raise MemoryRetirementValidationError("retirement preparation keys are not exact")
    status = _text(result["status"], "preparation.status")
    if status not in PREPARATION_STATUSES:
        raise MemoryRetirementValidationError("retirement preparation status is unsupported")
    returned_receipt = validate_retirement_receipt(result["receipt"])
    if returned_receipt != receipt:
        raise MemoryRetirementValidationError("retirement preparation receipt does not match its request")
    reason = _text(result["reason"], "preparation.reason")
    lease = result["lease"]
    if status == "prepared" and (not receipt.initialized or lease is None):
        raise MemoryRetirementValidationError("prepared requires an initialized receipt and opaque lease")
    if status == "unknown" and reason == "prepared":
        raise MemoryRetirementValidationError("unknown preparation cannot carry the prepared reason")
    return ProviderRetirementPreparation(status=status, receipt=receipt, lease=lease, reason=reason)  # type: ignore[arg-type]


def prepare_memory_provider(provider: Any, receipt: RetirementReceipt) -> ProviderRetirementPreparation:
    """Acquire a provider-owned reversible fence using the exact captured receipt."""
    if not has_two_phase_retirement_capability(provider):
        raise MemoryRetirementCapabilityError("provider has no two-phase retirement capability")
    if not isinstance(receipt, RetirementReceipt):
        raise MemoryRetirementValidationError("provider preparation requires a captured receipt")
    prepare_fn = _public_attribute(provider, "prepare_retirement")
    try:
        raw_result = prepare_fn(receipt.as_mapping())
    except Exception as exc:  # Optional provider boundary: retain exact custody.
        raise MemoryRetirementCallError("provider retirement preparation was unavailable") from exc
    return validate_retirement_preparation(raw_result, receipt)


def resume_memory_provider(provider: Any, lease: Any) -> bool:
    """Resume an exact provider-owned preparation lease; only literal ``True`` succeeds."""
    if not has_two_phase_retirement_capability(provider):
        raise MemoryRetirementCapabilityError("provider has no two-phase retirement capability")
    if lease is None:
        raise MemoryRetirementValidationError("provider resume requires an opaque lease")
    resume_fn = _public_attribute(provider, "resume_retirement")
    try:
        result = resume_fn(lease)
    except Exception as exc:  # Optional provider boundary: fail closed.
        raise MemoryRetirementCallError("provider retirement resume was unavailable") from exc
    if type(result) is not bool:
        raise MemoryRetirementValidationError("provider retirement resume must return bool")
    return result


def commit_memory_provider(
    provider: Any, lease: Any, *, receipt: RetirementReceipt,
) -> RetirementResult:
    """Commit one exact provider lease and validate its source-owned terminal result."""
    if not has_two_phase_retirement_capability(provider):
        raise MemoryRetirementCapabilityError("provider has no two-phase retirement capability")
    if lease is None:
        raise MemoryRetirementValidationError("provider commit requires an opaque lease")
    commit_fn = _public_attribute(provider, "commit_retirement")
    try:
        raw_result = commit_fn(lease)
    except Exception as exc:  # Optional provider boundary: retain exact custody.
        raise MemoryRetirementCallError("provider retirement commit was unavailable") from exc
    return validate_retirement_result(raw_result, receipt)


def retire_memory_provider(provider: Any, *, receipt: RetirementReceipt | None = None) -> RetirementResult:
    """Run the public provider retirement operation with source-owned identity binding.

    The caller supplies only the provider object on the first attempt.  A retry may pass
    the exact receipt captured from that same object; the provider is never re-imported or
    asked for a replacement identity.  A missing legacy capability, malformed result,
    pending work, or unknown owner therefore cannot become ``released``.
    """
    if receipt is None:
        receipt = capture_retirement_receipt(provider)
    elif not isinstance(receipt, RetirementReceipt):
        raise MemoryRetirementValidationError("retirement retry requires a captured receipt")
    retire_fn = _public_attribute(provider, "retire")
    if not callable(retire_fn):
        raise MemoryRetirementCapabilityError("provider has no retirement capability")
    try:
        raw_result = retire_fn(receipt.as_mapping())
    except Exception as exc:  # Optional provider boundaries fail closed without exposing errors.
        raise MemoryRetirementCallError("provider retirement result was unavailable") from exc
    return validate_retirement_result(raw_result, receipt)
