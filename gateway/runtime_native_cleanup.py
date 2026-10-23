"""Coordinator-owned native cleanup types and the post-ACK settlement mixin.

The runtime custody coordinator keeps the close fence and exact receipt.  This topical sibling
keeps the native participant graph and its bounded retry state out of the coordinator's large
source module; all methods still execute against the same coordinator lock and fields.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
from typing import Any


def _refusal(code: str, owner_class: str | None = None):
    # Import lazily because runtime_custody re-exports these types while it defines the
    # coordinator.  Calls happen only after both modules have finished importing.
    from gateway.runtime_custody import CustodyRefusal

    return CustodyRefusal(code, owner_class)


def _is_refusal(value: object) -> bool:
    from gateway.runtime_custody import CustodyRefusal

    return isinstance(value, CustodyRefusal)


@dataclass(frozen=True)
class NativeCleanupZeroWorkReceipt:
    """Typed source-owned proof that a committed handoff has no native participants."""

    source: str
    process_instance: str
    authority_epoch: int
    close_generation: int
    owner_id: str
    outcome: str = "no_work"
    observed: bool = True


@dataclass(frozen=True)
class NativeCleanupPlan:
    """Exact native participant graph captured before the irreversible ACK."""

    process_instance: str
    authority_epoch: int
    close_generation: int
    source_participants: tuple[tuple[Any, Any, Any], ...] = ()
    native_fences: tuple[Any, ...] = ()
    zero_work: NativeCleanupZeroWorkReceipt | None = None


@dataclass(frozen=True)
class NativeCleanupCapability:
    """Coordinator-issued capability spanning handoff_pending to closed cleanup."""

    coordinator: Any = field(repr=False, compare=False)
    receipt: Any = field(repr=False, compare=False)
    closed_receipt: Any = field(repr=False, compare=False)
    close: Any = field(repr=False, compare=False)
    transition: object = field(repr=False, compare=False)
    process_instance: str
    authority_epoch: int
    close_generation: int
    plan: NativeCleanupPlan | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class NativeCleanupCompletionReceipt:
    coordinator: Any = field(repr=False, compare=False)
    capability: NativeCleanupCapability = field(repr=False, compare=False)
    participant: Any = field(repr=False, compare=False)
    source_receipt: Any = field(repr=False, compare=False)
    native_fence: Any = field(repr=False, compare=False)
    process_instance: str
    authority_epoch: int
    close_generation: int
    outcome: str = "complete"


@dataclass(frozen=True)
class NativeCleanupResult:
    capability: NativeCleanupCapability
    cleanup: str
    reason: str | None = None

    @property
    def closed_receipt(self) -> Any:
        return self.capability.closed_receipt

    @property
    def state(self) -> str:
        return self.closed_receipt.state

    def as_wire(self) -> dict[str, Any]:
        wire = self.closed_receipt.as_wire()
        wire["cleanup"] = self.cleanup
        if self.reason is not None:
            wire["reason"] = self.reason
        return wire


class NativeCleanupMixin:
    """Native cleanup operations executed by ``RuntimeCustodyCoordinator``."""

    def _native_plan_identity_refusal_locked(self, plan: NativeCleanupPlan):
        if not isinstance(plan, NativeCleanupPlan):
            return _refusal("native_cleanup_plan_invalid")
        if (
            plan.process_instance != self.process_instance
            or plan.authority_epoch != self.authority_epoch
            or self._close is None
            or plan.close_generation != self._close.close_generation
            or type(plan.source_participants) is not tuple
            or type(plan.native_fences) is not tuple
        ):
            return _refusal("native_cleanup_plan_mismatch")
        fence_ids = set()
        for native_fence in plan.native_fences:
            if native_fence is None or id(native_fence) in fence_ids:
                return _refusal("native_cleanup_fence_invalid")
            fence_ids.add(id(native_fence))
        if plan.source_participants:
            if plan.zero_work is not None or not plan.native_fences:
                return _refusal("native_cleanup_plan_incomplete")
        else:
            zero = plan.zero_work
            if (
                not isinstance(zero, NativeCleanupZeroWorkReceipt)
                or zero.source == ""
                or zero.owner_id == ""
                or zero.process_instance != self.process_instance
                or zero.authority_epoch != self.authority_epoch
                or self._close is None
                or zero.close_generation != self._close.close_generation
                or zero.outcome != "no_work"
                or zero.observed is not True
            ):
                return _refusal("native_cleanup_zero_work_missing")
        participant_ids = set()
        for entry in plan.source_participants:
            if type(entry) is not tuple or len(entry) != 3:
                return _refusal("native_cleanup_participant_invalid")
            source, source_receipt, capability = entry
            if source is None or source_receipt is None or capability is None:
                return _refusal("native_cleanup_participant_invalid")
            if id(capability) in participant_ids:
                return _refusal("native_cleanup_participant_duplicate")
            participant_ids.add(id(capability))
            if not callable(getattr(capability, "finish_native_cleanup", None)):
                return _refusal("native_cleanup_capability_missing")
            for witness in (source, source_receipt, capability):
                if getattr(witness, "coordinator", self) is not self:
                    return _refusal("native_cleanup_foreign_source")
                for name, expected in (
                    ("process_instance", self.process_instance),
                    ("authority_epoch", self.authority_epoch),
                    ("close_generation", self._close.close_generation),
                ):
                    observed = getattr(witness, name, expected)
                    if observed != expected:
                        return _refusal("native_cleanup_identity_mismatch")
        return None

    def _native_capability_valid_locked(self, capability: NativeCleanupCapability) -> bool:
        return (
            isinstance(capability, NativeCleanupCapability)
            and capability.coordinator is self
            and self._native_cleanup_capability is capability
            and capability.close is self._close
            and capability.transition is self._source_rebind_transition
            and capability.process_instance == self.process_instance
            and capability.authority_epoch == self.authority_epoch
            and self._close is not None
            and capability.close_generation == self._close.close_generation
            and capability.receipt.state == "handoff_pending"
            and capability.closed_receipt.state == "closed"
            and capability.receipt.receipt_id == self._receipt_id
            and capability.closed_receipt.receipt_id == self._receipt_id
        )

    def _native_result_locked(self) -> NativeCleanupResult | Any:
        capability = self._native_cleanup_capability
        if capability is None or not self._native_capability_valid_locked(capability):
            return _refusal("native_cleanup_capability_missing")
        return NativeCleanupResult(
            capability,
            self._native_cleanup_state,
            self._native_cleanup_reason,
        )

    def bind_native_cleanup_plan(self, receipt: Any, plan: NativeCleanupPlan):
        with self._lock:
            if (
                self._state != "handoff_pending"
                or not self._check_close_identity(receipt)
                or self._native_cleanup_capability is None
            ):
                return _refusal("native_cleanup_plan_mismatch")
            refusal = self._native_plan_identity_refusal_locked(plan)
            if refusal is not None:
                return refusal
            if self._native_cleanup_plan is not None:
                return (None if self._native_cleanup_plan is plan
                        else _refusal("native_cleanup_plan_replaced"))
            self._native_cleanup_plan = plan
            self._native_cleanup_capability = replace(self._native_cleanup_capability, plan=plan)
            return None

    def native_cleanup_capability(self, receipt: Any):
        with self._lock:
            capability = self._native_cleanup_capability
            if capability is None or not self._native_capability_valid_locked(capability):
                return _refusal("native_cleanup_capability_missing")
            if receipt == capability.receipt or receipt == capability.closed_receipt:
                return capability
            return _refusal("native_cleanup_mismatch")

    def native_cleanup_result(self, receipt: Any):
        with self._lock:
            capability = self._native_cleanup_capability
            if (
                capability is None
                or not self._native_capability_valid_locked(capability)
                or receipt != capability.closed_receipt
            ):
                return _refusal("native_cleanup_mismatch")
            return self._native_result_locked()

    def begin_native_cleanup(self, capability: NativeCleanupCapability):
        with self._lock:
            if not self._native_capability_valid_locked(capability) or self._state != "closed":
                return _refusal("native_cleanup_mismatch")
            current = self._native_result_locked()
            if _is_refusal(current):
                return current
            if current.cleanup == "complete":
                return current, ()
            if self._native_cleanup_plan is None:
                self._native_cleanup_state = "unknown"
                self._native_cleanup_reason = "native_cleanup_missing"
                return self._native_result_locked(), ()
            if self._native_cleanup_in_progress:
                return current, ()
            if not self._native_cleanup_pending_participants:
                self._native_cleanup_pending_participants = self._native_cleanup_plan.source_participants
            self._native_cleanup_in_progress = True
            return self._native_result_locked(), self._native_cleanup_pending_participants

    def finish_native_cleanup(
        self,
        capability: NativeCleanupCapability,
        completed_entries=(),
        *,
        reason: str | None = None,
        unknown: bool = False,
    ):
        with self._lock:
            if not self._native_capability_valid_locked(capability) or self._state != "closed":
                return _refusal("native_cleanup_mismatch")
            plan = self._native_cleanup_plan
            if plan is None:
                self._native_cleanup_state = "unknown"
                self._native_cleanup_reason = reason or "native_cleanup_missing"
                self._native_cleanup_in_progress = False
                return self._native_result_locked()
            try:
                entries = tuple(completed_entries)
            except TypeError:
                self._native_cleanup_in_progress = False
                self._native_cleanup_state = "unknown"
                self._native_cleanup_reason = "native_cleanup_receipt_invalid"
                return _refusal("native_cleanup_receipt_invalid")
            pending = list(self._native_cleanup_pending_participants or plan.source_participants)
            completed_ids = set()
            for completion in entries:
                if (
                    not isinstance(completion, NativeCleanupCompletionReceipt)
                    or completion.coordinator is not self
                    or completion.capability is not capability
                    or completion.process_instance != self.process_instance
                    or completion.authority_epoch != self.authority_epoch
                    or completion.close_generation != plan.close_generation
                    or completion.outcome != "complete"
                ):
                    self._native_cleanup_in_progress = False
                    self._native_cleanup_state = "unknown"
                    self._native_cleanup_reason = "native_cleanup_receipt_invalid"
                    return _refusal("native_cleanup_receipt_invalid")
                match = next(
                    (entry for entry in pending
                     if entry[2] is completion.participant
                     and entry[1] is completion.source_receipt),
                    None,
                )
                if match is None or not any(
                    completion.native_fence is fence for fence in plan.native_fences
                ):
                    self._native_cleanup_in_progress = False
                    self._native_cleanup_state = "unknown"
                    self._native_cleanup_reason = "native_cleanup_receipt_foreign"
                    return _refusal("native_cleanup_receipt_foreign")
                if id(match[2]) in completed_ids:
                    self._native_cleanup_in_progress = False
                    self._native_cleanup_state = "unknown"
                    self._native_cleanup_reason = "native_cleanup_receipt_duplicate"
                    return _refusal("native_cleanup_receipt_duplicate")
                completed_ids.add(id(match[2]))
                pending.remove(match)
            self._native_cleanup_pending_participants = tuple(pending)
            self._native_cleanup_in_progress = False
            if unknown:
                self._native_cleanup_state = "unknown"
                self._native_cleanup_reason = reason or "native_cleanup_unknown"
            elif not pending:
                self._native_cleanup_state = "complete"
                self._native_cleanup_reason = None
            else:
                self._native_cleanup_state = "pending"
                self._native_cleanup_reason = reason or "native_cleanup_pending"
            return self._native_result_locked()
