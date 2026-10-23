"""Live native cleanup participants for the committed runtime handoff.

The coordinator's native cleanup plan is deliberately assembled at the ACK boundary.  This
module adapts the source-owned native leases that are already held by the gateway runtime: the
per-profile cron jobs fences and the retained profile gateway-lock descriptors.  I17 hosted-room
and I18 profile/provider evidence are mandatory prerequisites; they are not inferred from a
current-authority map or an empty provider cache.  It does not create a second ledger or infer
absence from counters, tasks, or an empty mapping.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial
import math
from pathlib import Path
from typing import Any

from gateway.runtime_custody import (
    DrainReceipt,
    NativeCleanupCapability,
    NativeCleanupCompletionReceipt,
    NativeCleanupPlan,
)
from gateway.runtime_cron_source_registry import CronSourceRegistry


def _identity_matches(value: Any, coordinator: Any, receipt: DrainReceipt) -> bool:
    return (
        getattr(value, "coordinator", None) is coordinator
        and getattr(value, "process_instance", None) == receipt.process_instance
        and getattr(value, "authority_epoch", None) == receipt.authority_epoch
        and getattr(value, "close_generation", None) == receipt.close_generation
    )


def _validate_finish_context(
    *, coordinator: Any, capability: Any, source_receipt: Any, expected_source: Any,
    participant: Any, native_fences: Any, required_fences: tuple[Any, ...], deadline: Any,
    error_prefix: str,
) -> None:
    """Validate the coordinator-owned close before any source finalizer can run.

    ``finish_native_cleanup`` is the irreversible source boundary.  The coordinator's opaque
    capability and the source's captured fences therefore have to be checked before a registry
    transition, descriptor close, or other native effect.  A copied tuple is harmless, but every
    retained fence must still be the exact object captured by this participant and carried by the
    coordinator-owned plan.
    """
    if getattr(coordinator, "state", None) != "closed":
        raise RuntimeError(f"{error_prefix}_coordinator_not_closed")
    if not isinstance(capability, NativeCleanupCapability):
        raise RuntimeError(f"{error_prefix}_capability_invalid")
    if capability.coordinator is not coordinator or source_receipt is not expected_source:
        raise RuntimeError(f"{error_prefix}_identity_mismatch")
    if not _identity_matches(expected_source, coordinator, capability.closed_receipt):
        raise RuntimeError(f"{error_prefix}_identity_mismatch")
    if getattr(capability, "close_generation", None) != getattr(expected_source, "close_generation", None):
        raise RuntimeError(f"{error_prefix}_identity_mismatch")
    current_capability = getattr(coordinator, "native_cleanup_capability", None)
    if not callable(current_capability) or current_capability(capability.closed_receipt) is not capability:
        raise RuntimeError(f"{error_prefix}_capability_foreign")
    plan = capability.plan
    if not isinstance(plan, NativeCleanupPlan):
        raise RuntimeError(f"{error_prefix}_plan_missing")
    if type(native_fences) is not tuple:
        raise RuntimeError(f"{error_prefix}_fence_payload_invalid")
    if (
        len(native_fences) != len(plan.native_fences)
        or any(
            all(candidate is not expected for expected in plan.native_fences)
            for candidate in native_fences
        )
        or any(
            all(expected is not candidate for candidate in native_fences)
            for expected in plan.native_fences
        )
    ):
        raise RuntimeError(f"{error_prefix}_fence_payload_mismatch")
    if type(deadline) not in (int, float) or isinstance(deadline, bool):
        raise RuntimeError(f"{error_prefix}_deadline_invalid")
    if not math.isfinite(float(deadline)) or deadline <= 0:
        raise RuntimeError(f"{error_prefix}_deadline_expired")
    if not all(
        any(fence is candidate for candidate in native_fences)
        for fence in required_fences
    ):
        raise RuntimeError(f"{error_prefix}_fence_missing")
    if not all(
        any(fence is candidate for candidate in plan.native_fences)
        for fence in required_fences
    ):
        raise RuntimeError(f"{error_prefix}_plan_fence_missing")
    if not any(
        type(entry) is tuple and len(entry) == 3 and entry[1] is expected_source and entry[2] is participant
        for entry in plan.source_participants
    ):
        raise RuntimeError(f"{error_prefix}_participant_unbound")


@dataclass(frozen=True)
class CronNativeCleanupReceipt:
    coordinator: Any = field(repr=False, compare=False)
    registry: CronSourceRegistry = field(repr=False, compare=False)
    process_instance: str
    authority_epoch: int
    close_generation: int
    graph_digest: str
    fences: tuple[Any, ...] = field(repr=False, compare=False)
    source: str = "gateway.runtime_cron_source_registry"


@dataclass(frozen=True)
class ProfileNativeCleanupReceipt:
    coordinator: Any = field(repr=False, compare=False)
    owner: Any = field(repr=False, compare=False)
    home: Path
    handle: Any = field(repr=False, compare=False)
    evidence: dict[str, Any] = field(repr=False, compare=False)
    process_instance: str
    authority_epoch: int
    close_generation: int
    source: str = "gateway.runtime_ownership"
    source_ready: Any = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class ProviderNativeCleanupReceipt:
    """Exact public manager lease retained until the coordinator is CLOSED."""

    coordinator: Any = field(repr=False, compare=False)
    custody: Any = field(repr=False, compare=False)
    entry: Any = field(repr=False, compare=False)
    lease: Any = field(repr=False, compare=False)
    process_instance: str
    authority_epoch: int
    close_generation: int
    source: str = "gateway.profile_global_provider"


class ProviderNativeCleanupParticipant:
    """Commit one exact prepared manager lease after native coordinator closure."""

    def __init__(self, receipt: ProviderNativeCleanupReceipt):
        self.coordinator = receipt.coordinator
        self.process_instance = receipt.process_instance
        self.authority_epoch = receipt.authority_epoch
        self.close_generation = receipt.close_generation
        self._receipt = receipt

    async def finish_native_cleanup(
        self, *, coordinator, capability, source_receipt, native_fences, deadline,
    ):
        _validate_finish_context(
            coordinator=coordinator,
            capability=capability,
            source_receipt=source_receipt,
            expected_source=self._receipt,
            participant=self,
            native_fences=native_fences,
            required_fences=(self._receipt.lease,),
            deadline=deadline,
            error_prefix="provider_native_cleanup",
        )
        if not await self._receipt.custody.commit_native_entry(
            self.close_generation, self._receipt.entry,
        ):
            raise RuntimeError("provider_native_cleanup_unknown")
        return NativeCleanupCompletionReceipt(
            coordinator=coordinator,
            capability=capability,
            participant=self,
            source_receipt=self._receipt,
            native_fence=self._receipt.lease,
            process_instance=self.process_instance,
            authority_epoch=self.authority_epoch,
            close_generation=self.close_generation,
        )


class CronNativeCleanupParticipant:
    """Release exactly the cron fence captured for this close generation."""

    def __init__(self, receipt: CronNativeCleanupReceipt):
        self.coordinator = receipt.coordinator
        self.process_instance = receipt.process_instance
        self.authority_epoch = receipt.authority_epoch
        self.close_generation = receipt.close_generation
        self._receipt = receipt

    def finish_native_cleanup(
        self, *, coordinator, capability, source_receipt, native_fences, deadline,
    ):
        _validate_finish_context(
            coordinator=coordinator,
            capability=capability,
            source_receipt=source_receipt,
            expected_source=self._receipt,
            participant=self,
            native_fences=native_fences,
            required_fences=self._receipt.fences,
            deadline=deadline,
            error_prefix="cron_native_cleanup",
        )
        registry = self._receipt.registry
        with registry._lock:
            if (
                registry.coordinator is not coordinator
                or registry.close_generation != self.close_generation
                or registry._runner is None
                or registry._profile_graph_digest_locked() != self._receipt.graph_digest
            ):
                raise RuntimeError("cron_native_cleanup_graph_replaced")
            current = tuple(registry._fences.values())
            if not current or any(
                all(item is not captured for captured in self._receipt.fences)
                or all(item is not candidate for candidate in native_fences)
                for item in current
            ):
                raise RuntimeError("cron_native_cleanup_fence_replaced")
            if registry.state == "closing" and not registry.mark_handoff_pending(
                self.close_generation,
            ):
                raise RuntimeError("cron_native_cleanup_handoff_refused")
            outcome = registry.acknowledge_native_outcome(self.close_generation)
        if outcome.status != "closed":
            raise RuntimeError(f"cron_native_cleanup_{outcome.status}")
        fence = self._receipt.fences[0]
        return NativeCleanupCompletionReceipt(
            coordinator=coordinator,
            capability=capability,
            participant=self,
            source_receipt=self._receipt,
            native_fence=fence,
            process_instance=self.process_instance,
            authority_epoch=self.authority_epoch,
            close_generation=self.close_generation,
        )


class ProfileNativeCleanupParticipant:
    """Close one exact retained ``gateway.lock`` descriptor after native ACK."""

    def __init__(self, receipt: ProfileNativeCleanupReceipt):
        self.coordinator = receipt.coordinator
        self.process_instance = receipt.process_instance
        self.authority_epoch = receipt.authority_epoch
        self.close_generation = receipt.close_generation
        self._receipt = receipt

    def finish_native_cleanup(
        self, *, coordinator, capability, source_receipt, native_fences, deadline,
    ):
        _validate_finish_context(
            coordinator=coordinator,
            capability=capability,
            source_receipt=source_receipt,
            expected_source=self._receipt,
            participant=self,
            native_fences=native_fences,
            required_fences=(self._receipt.handle,),
            deadline=deadline,
            error_prefix="profile_native_cleanup",
        )
        owner = self._receipt.owner
        home = self._receipt.home
        source_ready = self._receipt.source_ready
        if callable(source_ready) and source_ready() is not True:
            raise RuntimeError("profile_native_cleanup_source_unknown")
        with owner._mutex:
            current_handle = owner._handles.get(home)
            if current_handle is not self._receipt.handle:
                raise RuntimeError("profile_native_cleanup_owner_replaced")
            current = owner.reservation_evidence(home)
            if not _profile_evidence_matches(self._receipt.evidence, current):
                raise RuntimeError("profile_native_cleanup_owner_unknown")
            outcome = owner.release_outcome(home)
        if getattr(outcome, "value", None) != "released":
            raise RuntimeError(f"profile_native_cleanup_{getattr(outcome, 'value', 'unknown')}")
        return NativeCleanupCompletionReceipt(
            coordinator=coordinator,
            capability=capability,
            participant=self,
            source_receipt=self._receipt,
            native_fence=self._receipt.handle,
            process_instance=self.process_instance,
            authority_epoch=self.authority_epoch,
            close_generation=self.close_generation,
        )


def _profile_evidence_matches(expected: dict[str, Any], current: Any) -> bool:
    if not isinstance(current, dict):
        return False
    fields = (
        "profile_home", "owner_epoch", "record", "physical_binding", "record_binding",
        "native_descriptor", "native_handle_retained", "os_lock_owner", "cross_process_proof",
        "observed",
    )
    return all(current.get(field) == expected.get(field) for field in fields)


def _profile_handle(owner: Any, home: Path, evidence: dict[str, Any]) -> Any:
    try:
        from gateway.runtime_ownership import canonical_home

        key = canonical_home(home)
        with owner._mutex:
            binding = owner._owner_record_bindings.get(key)
            handle = owner._handles.get(key)
            if (
                not isinstance(binding, tuple)
                or len(binding) != 4
                or binding[0] is not handle
                or handle is None
                or getattr(handle, "closed", False)
                or handle.fileno() != evidence.get("native_descriptor")
            ):
                return None
            return handle
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None


def _cron_participant(runner: Any, coordinator: Any, receipt: DrainReceipt):
    registry = getattr(coordinator, "_cron_source_registry", None)
    if not isinstance(registry, CronSourceRegistry):
        return None
    with registry._lock:
        if (
            registry.state != "closing"
            or registry.close_generation != receipt.close_generation
            or not registry._evidence_graph_ready_locked()
            or not registry._validate_graph_locked()
        ):
            return None
        fences = tuple(registry._fences.values())
        if not fences:
            return None
        snapshot = CronNativeCleanupReceipt(
            coordinator=coordinator,
            registry=registry,
            process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
            graph_digest=registry._profile_graph_digest_locked(),
            fences=fences,
        )
    participant = CronNativeCleanupParticipant(snapshot)
    return snapshot, participant, fences


def _profile_participants(runner: Any, coordinator: Any, receipt: DrainReceipt):
    try:
        from gateway.run_runtime import reserved_profile_homes
        from gateway.runtime_ownership import canonical_home, process_ownership

        homes = tuple(home for _name, home in reserved_profile_homes(runner))
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if not homes:
        return None
    entries = []
    for home in homes:
        canonical = canonical_home(home)
        try:
            evidence = process_ownership.reservation_evidence(canonical)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return None
        if not isinstance(evidence, dict):
            return None
        handle = _profile_handle(process_ownership, canonical, evidence)
        if handle is None:
            return None
        snapshot = ProfileNativeCleanupReceipt(
            coordinator=coordinator,
            owner=process_ownership,
            home=canonical,
            handle=handle,
            evidence=evidence,
            process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
            source_ready=partial(_source_prerequisites, runner, coordinator, receipt),
        )
        entries.append((snapshot, ProfileNativeCleanupParticipant(snapshot), handle))
    return tuple(entries)


def _provider_participants(runner: Any, coordinator: Any, receipt: DrainReceipt):
    """Capture exact prepared public manager leases before the irreversible ACK."""
    try:
        from gateway.profile_global_provider import custody_for

        custody = custody_for(runner)
        entries = custody.native_entries(receipt.close_generation)
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if entries is None:
        return None
    participants = []
    for entry in entries:
        lease = getattr(entry, "lease", None)
        if lease is None or getattr(entry, "status", None) != "prepared":
            return None
        snapshot = ProviderNativeCleanupReceipt(
            coordinator=coordinator,
            custody=custody,
            entry=entry,
            lease=lease,
            process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
        )
        participants.append((snapshot, snapshot, ProviderNativeCleanupParticipant(snapshot)))
    return tuple(participants)


def _owner_is_sealed(coordinator: Any, owner_class: str) -> bool:
    states = getattr(coordinator, "_owner_state", None)
    return isinstance(states, dict) and states.get(owner_class) == "sealed"


def _valid_source_evidence(
    evidence: Any, *, source: str, coordinator: Any, receipt: DrainReceipt,
) -> bool:
    return (
        isinstance(evidence, dict)
        and evidence.get("source") == source
        and evidence.get("process_instance") == receipt.process_instance
        and evidence.get("authority_epoch") == receipt.authority_epoch
        and evidence.get("close_generation") == receipt.close_generation
        and evidence.get("observed") is True
        and evidence.get("cross_process_proof") is True
        and isinstance(evidence.get("owner_id"), str)
        and bool(evidence["owner_id"])
        and getattr(coordinator, "process_instance", None) == receipt.process_instance
        and getattr(coordinator, "authority_epoch", None) == receipt.authority_epoch
    )


def _hosted_tui_prerequisite(runner: Any, coordinator: Any, receipt: DrainReceipt) -> bool:
    """Re-read I17's source-owned terminal graph before profile fences can be released."""
    if not _owner_is_sealed(coordinator, "I17_hosted_tui_services"):
        return False
    try:
        from gateway.runtime_custody import OWNER_SPECS
        from tui_gateway.hosted_tui_custody import owner_evidence

        evidence = owner_evidence(runner, receipt)
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        return False
    if not _valid_source_evidence(
        evidence,
        source=OWNER_SPECS["I17_hosted_tui_services"].source,
        coordinator=coordinator,
        receipt=receipt,
    ):
        return False
    graph = evidence.get("producer_graph")
    if not isinstance(graph, (tuple, list)) or not graph:
        return False
    return all(
        isinstance(item, dict)
        and item.get("host") in {"absent", "terminal"}
        and item.get("room") in {"absent", "terminal"}
        for item in graph
    )


def _profile_lifecycle_prerequisite(runner: Any, coordinator: Any, receipt: DrainReceipt) -> bool:
    """Require I18's exact profile/provider graph, not a current-authority map or empty cache."""
    if not _owner_is_sealed(coordinator, "I18_multiplex_profile_retirement"):
        return False
    try:
        from gateway.profile_lifecycle_custody import profile_lifecycle_owner_evidence
        from gateway.runtime_custody import OWNER_SPECS

        descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
        evidence = profile_lifecycle_owner_evidence(
            runner,
            process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
            owner_id=f'{descriptor.get("instance_id", receipt.process_instance)}:I18',
        )
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        return False
    if not _valid_source_evidence(
        evidence,
        source=OWNER_SPECS["I18_multiplex_profile_retirement"].source,
        coordinator=coordinator,
        receipt=receipt,
    ):
        return False
    profiles = evidence.get("profiles")
    if not isinstance(profiles, (tuple, list)) or not profiles:
        return False
    if not all(
        isinstance(item, dict)
        and item.get("source_zero_work_witness") is True
        and isinstance(item.get("profile_home"), str)
        and isinstance(item.get("authority_epoch"), int)
        and isinstance(item.get("physical_owner_epoch"), str)
        and item.get("close_generation") == receipt.close_generation
        for item in profiles
    ):
        return False
    # A loaded manager is part of I18's source graph.  Its exact prepared lease is a participant
    # below; it must settle after coordinator CLOSED and before the profile-lock participants.
    # Requiring ``provider_retirement_settled`` here would make a loaded provider impossible to
    # acknowledge because the source intentionally marks it pending until this native phase.
    custody = getattr(runner, "_profile_lifecycle_custody", None)
    try:
        from gateway.session_authorities import all_authorities

        authorities = tuple(all_authorities(runner))
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        return False
    if not authorities:
        return False
    profile_ids = {item.get("profile_id") for item in profiles}
    authority_ids = {getattr(authority, "profile_id", None) for authority in authorities}
    if None in profile_ids or profile_ids != authority_ids:
        return False
    record_for = getattr(custody, "_record_for", None)
    if not callable(record_for):
        return False
    for authority in authorities:
        # A source evidence row with no live custody record is not a provider settlement; it is
        # an enumeration gap that must retain the profile lock.
        if record_for(authority) is None:
            return False
    return True


def _source_prerequisites(runner: Any, coordinator: Any, receipt: DrainReceipt) -> bool:
    """Require actual I17/I18 source closure before constructing the final profile stage."""
    return (
        _hosted_tui_prerequisite(runner, coordinator, receipt)
        and _profile_lifecycle_prerequisite(runner, coordinator, receipt)
    )


def build_native_cleanup_plan(runner: Any, coordinator: Any, receipt: Any):
    """Capture a source-owned native plan, or return ``None`` before irreversible ACK."""
    if (
        isinstance(receipt, DrainReceipt)
        and receipt.state == "closed"
        and getattr(coordinator, "state", None) == "closed"
    ):
        # A retry must use the exact coordinator-owned plan.  Rebuilding provider/host/profile
        # snapshots would create a new source graph and could silently adopt a replacement.
        current = getattr(coordinator, "_native_cleanup_plan", None)
        return current if isinstance(current, NativeCleanupPlan) else None
    if (
        isinstance(receipt, DrainReceipt)
        and receipt.state == "handoff_pending"
        and getattr(coordinator, "state", None) == "handoff_pending"
    ):
        # Once a plan is bound, a retry in the same close must return the exact coordinator-owned
        # object.  Re-snapshotting source participants would let bind_native_cleanup_plan reject
        # a harmless retry as a replacement and could adopt a changed provider/profile handle.
        current = getattr(coordinator, "_native_cleanup_plan", None)
        if isinstance(current, NativeCleanupPlan):
            if (
                current.process_instance == receipt.process_instance
                and current.authority_epoch == receipt.authority_epoch
                and current.close_generation == receipt.close_generation
            ):
                return current
            return None
    if (
        not isinstance(receipt, DrainReceipt)
        or receipt.state != "handoff_pending"
        or getattr(coordinator, "state", None) != "handoff_pending"
        or receipt.process_instance != getattr(coordinator, "process_instance", None)
        or receipt.authority_epoch != getattr(coordinator, "authority_epoch", None)
    ):
        return None
    if not _source_prerequisites(runner, coordinator, receipt):
        return None
    cron = _cron_participant(runner, coordinator, receipt)
    profiles = _profile_participants(runner, coordinator, receipt)
    providers = _provider_participants(runner, coordinator, receipt)
    if profiles is None or providers is None:
        return None
    participants = []
    native_fences = []
    if cron is not None:
        cron_snapshot, cron_participant, cron_fences = cron
        participants.append((cron_snapshot, cron_snapshot, cron_participant))
        native_fences.extend(cron_fences)
    # Provider leases settle before profile gateway-lock participants.  A profile lock is never
    # released while an exact manager lease can still report UNKNOWN or retry.
    participants.extend(providers)
    native_fences.extend(item[1].lease for item in providers)
    participants.extend((snapshot, snapshot, participant) for snapshot, participant, _ in profiles)
    native_fences.extend(handle for _, _, handle in profiles)
    if not participants:
        return None
    return NativeCleanupPlan(
        process_instance=receipt.process_instance,
        authority_epoch=receipt.authority_epoch,
        close_generation=receipt.close_generation,
        source_participants=tuple(participants),
        native_fences=tuple(native_fences),
    )
