"""Manager-side orchestration for receipt-bound memory-provider retirement.

The public contract lives in :mod:`agent.memory_retirement`; this module owns the
manager's snapshot, drain, retry and per-provider result bookkeeping.  Provider
callbacks are deliberately invoked outside the manager lock.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional

from agent.memory_retirement import (
    MemoryRetirementCallError,
    MemoryRetirementCapabilityError,
    ManagerPreparationReport,
    ManagerRetirementLease,
    MemoryRetirementReport,
    MemoryRetirementValidationError,
    ProviderRetirementPreparation,
    RetirementAttempt,
    RetirementReceipt,
    capture_retirement_receipt,
    commit_memory_provider,
    has_retirement_capability,
    has_two_phase_retirement_capability,
    prepare_memory_provider,
    resume_memory_provider,
)
from agent.memory_caller_scope import in_owner_home
from agent.memory_provider import spawn_context_thread

logger = logging.getLogger(__name__)


@dataclass
class HardRetirementState:
    """Exact provider/receipt custody retained across unresolved attempts."""

    providers: tuple[Any, ...]
    receipts: tuple[RetirementReceipt, ...]
    scope: tuple[Any, Any]
    setup_generation: int
    active: bool = False
    report: Optional[MemoryRetirementReport] = None
    attempts: list[Optional[RetirementAttempt]] = field(default_factory=list)
    mode: str = "hard"

    def __post_init__(self) -> None:
        if not self.attempts:
            self.attempts = [None] * len(self.providers)


@dataclass
class _ProviderPreparationCall:
    """Manager-owned custody for one provider preparation invocation."""

    provider: Any
    receipt: RetirementReceipt
    entered: threading.Event = field(default_factory=threading.Event)
    done: threading.Event = field(default_factory=threading.Event)
    result: ProviderRetirementPreparation | None = None
    error: Exception | None = None
    thread: threading.Thread | None = None


def _refused(manager: Any, providers: tuple[Any, ...], reason: str) -> MemoryRetirementReport:
    return manager._retirement_refusal(providers, reason)


def _reserve(manager: Any):
    """Reserve a snapshot or return an already-final/refused report."""
    if not manager._owner_allows("retire_hard"):
        return None, _refused(manager, tuple(manager._providers), "owner_scope_refused"), False

    refusal = None
    with manager._sync_executor_lock:
        providers = tuple(manager._providers)
        state = manager._hard_retirement_state
        if state is not None and state.report is not None and state.report.released:
            return state, state.report, False
        if state is not None and state.mode != "hard":
            refusal = (providers, "retirement_preparation_in_progress")
        elif state is not None and state.active:
            refusal = (providers, "retirement_in_progress")
        elif state is not None and (
            not manager._same_provider_objects(providers, state.providers)
            or manager._retirement_scope() != state.scope
        ):
            refusal = (providers, "provider_binding_changed")
        elif state is None and (manager._teardown_claim is not None or manager._shutting_down):
            refusal = (providers, "teardown_in_progress")
        elif state is None and manager._provider_setup_inflight:
            refusal = (providers, "provider_setup_in_progress")
        elif not providers:
            state = HardRetirementState(
                providers, (), manager._retirement_scope(), manager._provider_setup_generation,
                active=False,
                report=MemoryRetirementReport(status="released", reason="no_provider"),
            )
            manager._hard_retirement_state = state
            manager._shutting_down = True
            return state, state.report, False
        else:
            refusal = None

        if refusal is None and state is None:
            state = HardRetirementState(
                providers, (), manager._retirement_scope(), manager._provider_setup_generation, active=True,
            )
            manager._hard_retirement_state = state
            return state, None, True
        if refusal is None:
            state.active = True
    if refusal is not None:
        return state, _refused(manager, *refusal), False
    return state, None, False


def _capture(manager: Any, state: HardRetirementState) -> Optional[MemoryRetirementReport]:
    """Capture capabilities/receipts outside the manager lock, then commit the snapshot."""
    providers = state.providers
    try:
        missing = tuple(provider for provider in providers if not has_retirement_capability(provider))
        if missing:
            receipts = None
            reason = "missing_capability"
        else:
            receipts = tuple(capture_retirement_receipt(provider) for provider in providers)
            reason = None
    except MemoryRetirementCapabilityError:
        missing, receipts, reason = providers, None, "missing_capability"
    except MemoryRetirementValidationError as error:
        logger.warning("Memory provider hard retirement receipt was invalid: %s", error)
        missing, receipts, reason = providers, None, "invalid_receipt"
    except MemoryRetirementCallError as error:
        logger.warning("Memory provider hard retirement receipt was unavailable: %s", error)
        missing, receipts, reason = providers, None, "receipt_unavailable"
    except Exception as error:  # health: allow BLE001 -- optional provider boundary remains retained
        logger.warning("Memory provider hard retirement capability lookup failed: %s", error, exc_info=True)
        missing, receipts, reason = providers, None, "receipt_unavailable"

    with manager._sync_executor_lock:
        valid = (
            manager._hard_retirement_state is state
            and state.active
            and manager._same_provider_objects(tuple(manager._providers), state.providers)
            and manager._retirement_scope() == state.scope
            and manager._provider_setup_generation == state.setup_generation
            and manager._provider_setup_inflight == 0
        )
        if valid and receipts is not None:
            state.receipts = receipts
            manager._shutting_down = True
        elif valid or manager._hard_retirement_state is state:
            manager._hard_retirement_state = None
            state.active = False

    if not valid:
        return _refused(manager, tuple(manager._providers), "provider_binding_changed")
    if receipts is None:
        return _refused(manager, missing, reason or "receipt_unavailable")
    return None


def _ready_for_drain(manager: Any, state: HardRetirementState) -> Optional[MemoryRetirementReport]:
    refusal = None
    with manager._sync_executor_lock:
        if manager._hard_retirement_state is not state or not state.active:
            refusal = (tuple(manager._providers), "retirement_in_progress")
        elif (
            not manager._same_provider_objects(tuple(manager._providers), state.providers)
            or manager._retirement_scope() != state.scope
            or manager._provider_setup_generation != state.setup_generation
            or manager._provider_setup_inflight
        ):
            state.active = False
            refusal = (tuple(manager._providers), "provider_binding_changed")
        else:
            manager._shutting_down = True
    if refusal is not None:
        return _refused(manager, *refusal)
    return None


def _record_drain_timeout(manager: Any, state: HardRetirementState) -> MemoryRetirementReport:
    from agent import memory_manager as memory_manager_module

    logger.warning(
        "Memory provider hard retirement drain_timeout after %.2fs; custody remains retained",
        memory_manager_module._SYNC_DRAIN_TIMEOUT_S,
    )
    labels = [manager._provider_label(provider) for provider in state.providers]
    with manager._sync_executor_lock:
        for index, attempt in enumerate(state.attempts):
            if attempt is None or not attempt.result or not attempt.result.released:
                state.attempts[index] = RetirementAttempt(
                    provider=labels[index], status="unknown", reason="drain_timeout",
                )
        report = _retirement_report(state)
        state.report, state.active = report, False
    return report


def _pending(manager: Any, state: HardRetirementState):
    refusal = None
    with manager._sync_executor_lock:
        if (
            not manager._same_provider_objects(tuple(manager._providers), state.providers)
            or manager._retirement_scope() != state.scope
            or manager._provider_setup_generation != state.setup_generation
        ):
            state.active = False
            refusal = (tuple(manager._providers), "provider_binding_changed")
        else:
            pending = []
            for index, (provider, receipt, attempt) in enumerate(
                zip(state.providers, state.receipts, state.attempts)
            ):
                if attempt is None or not attempt.result or not attempt.result.released:
                    pending.append((index, provider, receipt))
            failures = {
                id(provider): reason
                for provider, reason in manager._provider_call_failures.values()
                if any(provider is pending_provider for _, pending_provider, _ in pending)
            }
    if refusal is not None:
        return None, _refused(manager, *refusal)
    return (pending, failures), None


def _run_pending(manager: Any, state: HardRetirementState, pending, failures) -> None:
    for index, provider, receipt in pending:
        failure = failures.get(id(provider))
        prior = state.attempts[index]
        if failure is not None and prior is None:
            attempt = RetirementAttempt(
                provider=manager._provider_label(provider), status="unknown", reason=failure,
            )
        else:
            # A prior accepted-write failure is retained as UNKNOWN until this exact loaded
            # provider returns its own release result.  The retry itself is the provider-owned
            # recovery/completion observation; merely reporting the failure once never clears it.
            attempt = manager._retire_provider_attempt(provider, receipt)
        with manager._sync_executor_lock:
            if manager._hard_retirement_state is state and state.active:
                state.attempts[index] = attempt


def _retirement_report(state: HardRetirementState) -> MemoryRetirementReport:
    attempts = tuple(attempt for attempt in state.attempts if attempt is not None)
    unresolved = [
        attempt for attempt in state.attempts
        if attempt is None or not attempt.result or not attempt.result.released
    ]
    if not unresolved and attempts and len(attempts) == len(state.providers):
        return MemoryRetirementReport(status="released", reason="released", attempts=attempts)
    reason = next((attempt.reason for attempt in unresolved if attempt is not None), "retirement_in_progress")
    return MemoryRetirementReport(status="unknown", reason=reason, attempts=attempts)


def _finish(manager: Any, state: HardRetirementState) -> MemoryRetirementReport:
    refusal = None
    with manager._sync_executor_lock:
        if (
            manager._hard_retirement_state is not state
            or not manager._same_provider_objects(tuple(manager._providers), state.providers)
            or manager._retirement_scope() != state.scope
            or manager._provider_setup_generation != state.setup_generation
        ):
            state.active = False
            refusal = (tuple(manager._providers), "provider_binding_changed")
        else:
            report = _retirement_report(state)
            if report.released:
                for provider in state.providers:
                    manager._drop_provider(provider)
                    manager._provider_call_failures.pop(id(provider), None)
            state.report, state.active = report, False
    if refusal is not None:
        return _refused(manager, *refusal)
    return report


def retire_hard(manager: Any) -> MemoryRetirementReport:
    """Run one receipt-bound retirement attempt and preserve unresolved custody."""
    state, report, capture_needed = _reserve(manager)
    if report is not None:
        return report
    if capture_needed:
        report = _capture(manager, state)
        if report is not None:
            return report
    report = _ready_for_drain(manager, state)
    if report is not None:
        return report
    if not manager._hard_retirement_drain():
        return _record_drain_timeout(manager, state)
    pending, report = _pending(manager, state)
    if report is not None:
        return report
    _run_pending(manager, state, *pending)
    return _finish(manager, state)


def _preparation_refusal(manager: Any, reason: str) -> ManagerPreparationReport:
    logger.warning("Memory provider reversible retirement refused (%s); custody remains retained", reason)
    return ManagerPreparationReport(status="unknown", reason=reason, lease=None)


def _preparation_report(lease: ManagerRetirementLease) -> ManagerPreparationReport:
    return ManagerPreparationReport(
        status="prepared" if lease.phase == "prepared" else "unknown",
        reason=lease.reason,
        lease=lease,
    )


def _same_preparation_binding(manager: Any, lease: ManagerRetirementLease) -> bool:
    state = manager._hard_retirement_state
    return (
        manager._retirement_preparation_lease is lease
        and lease.manager is manager
        and state is not None
        and state.mode == "prepare"
        and manager._same_provider_objects(tuple(manager._providers), lease.providers)
        and manager._retirement_scope() == lease.scope
        and manager._provider_setup_generation == lease.setup_generation
        and manager._provider_setup_inflight == 0
    )


def _new_preparation_lease(manager: Any):
    """Capture exact provider identities before installing the reversible admission fence."""
    if not manager._owner_allows("prepare_retirement"):
        return None, _preparation_refusal(manager, "owner_scope_refused"), False

    with manager._sync_executor_lock:
        current = manager._retirement_preparation_lease
        if current is not None:
            return current, None, False
        state = manager._hard_retirement_state
        if state is not None:
            reason = "retirement_already_committed" if state.report is not None else "retirement_in_progress"
            return None, _preparation_refusal(manager, reason), False
        if manager._teardown_claim is not None or manager._shutting_down:
            return None, _preparation_refusal(manager, "teardown_in_progress"), False
        if manager._provider_setup_inflight:
            return None, _preparation_refusal(manager, "provider_setup_in_progress"), False
        providers = tuple(manager._providers)
        scope = manager._retirement_scope()
        setup_generation = manager._provider_setup_generation

    if providers:
        missing = tuple(provider for provider in providers if not has_two_phase_retirement_capability(provider))
        if missing:
            return None, _preparation_refusal(manager, "missing_capability"), False
        try:
            receipts = tuple(capture_retirement_receipt(provider) for provider in providers)
        except MemoryRetirementCapabilityError:
            return None, _preparation_refusal(manager, "missing_capability"), False
        except MemoryRetirementValidationError:
            return None, _preparation_refusal(manager, "invalid_receipt"), False
        except MemoryRetirementCallError:
            return None, _preparation_refusal(manager, "receipt_unavailable"), False
        except Exception:  # health: optional provider boundary remains retained
            logger.warning("Memory provider reversible retirement receipt lookup failed", exc_info=True)
            return None, _preparation_refusal(manager, "receipt_unavailable"), False
    else:
        receipts = ()

    lease = ManagerRetirementLease(
        manager=manager,
        providers=providers,
        receipts=receipts,
        provider_leases=[None] * len(providers),
        provider_prepared=[False] * len(providers),
        provider_attempts=[None] * len(providers),
        provider_reasons=["preparation_incomplete"] * len(providers),
        scope=scope,
        setup_generation=setup_generation,
        provider_preparation_calls=[None] * len(providers),
    )
    state = HardRetirementState(
        providers=providers,
        receipts=receipts,
        scope=scope,
        setup_generation=setup_generation,
        active=True,
        mode="prepare",
    )
    with manager._sync_executor_lock:
        if (
            manager._retirement_preparation_lease is not None
            or manager._hard_retirement_state is not None
            or not manager._same_provider_objects(tuple(manager._providers), providers)
            or manager._retirement_scope() != scope
            or manager._provider_setup_generation != setup_generation
            or manager._provider_setup_inflight
        ):
            return None, _preparation_refusal(manager, "provider_binding_changed"), False
        manager._retirement_preparation_lease = lease
        manager._hard_retirement_state = state
    return lease, None, True


def _start_provider_preparation_call(
    manager: Any, lease: ManagerRetirementLease, index: int,
) -> _ProviderPreparationCall:
    """Start one provider call while retaining its exact future-equivalent custody."""
    with manager._sync_executor_lock:
        existing = lease.provider_preparation_calls[index]
        if existing is not None:
            return existing
        call = _ProviderPreparationCall(lease.providers[index], lease.receipts[index])
        lease.provider_preparation_calls[index] = call

    def _run() -> None:
        call.entered.set()
        try:
            call.result = prepare_memory_provider(call.provider, call.receipt)
        except Exception as error:  # health: allow BLE001 -- opaque provider preparation remains exact lease custody
            call.error = error
        finally:
            call.done.set()

    try:
        call.thread = spawn_context_thread(
            in_owner_home(manager._bound_scope, _run),
            name=f"memory-retirement-prepare-{manager._provider_label(call.provider)}",
        )
        call.thread.start()
    except Exception as error:  # health: allow BLE001 -- thread start failure retains the accepted call record
        call.error = error
        # A factory failure before it returns a Thread proves that no native call was accepted.
        # Once a Thread object exists, ``Thread.start`` may have handed work to the native
        # runtime before raising; retain that exact call and thread rather than guessing from
        # ident/alive/entered observations and admitting a duplicate provider fence.
        if call.thread is None:
            call.done.set()
    return call


def _provider_preparation_attempt(
    manager: Any, lease: ManagerRetirementLease, index: int,
) -> tuple[ProviderRetirementPreparation | None, bool]:
    """Observe one call under the lease's shared deadline.

    A timeout deliberately leaves the call object in the lease.  A later retry observes
    that exact call rather than starting a duplicate provider fence or losing accepted
    provider work.
    """
    call = _start_provider_preparation_call(manager, lease, index)
    deadline = lease.preparation_deadline
    remaining = max(0.0, deadline - time.monotonic()) if deadline is not None else 0.0
    if not call.done.wait(remaining):
        with manager._sync_executor_lock:
            if manager._retirement_preparation_lease is lease:
                lease.provider_reasons[index] = "preparation_timeout"
        return None, True

    # The target sets ``done`` in its finally block before the native Thread.run finalizer
    # returns.  Join that same thread under the remaining shared budget before consuming the
    # call record; otherwise preparation can report success while native finalization still
    # owns provider resources.
    thread = call.thread
    if thread is not None:
        remaining = max(0.0, deadline - time.monotonic()) if deadline is not None else 0.0
        try:
            thread.join(remaining)
            thread_alive = thread.is_alive()
        except Exception:  # health: allow BLE001 -- opaque native thread observation must retain exact custody
            thread_alive = True
        if thread_alive:
            with manager._sync_executor_lock:
                if manager._retirement_preparation_lease is lease:
                    lease.provider_reasons[index] = "preparation_timeout"
            return None, True

    with manager._sync_executor_lock:
        if manager._retirement_preparation_lease is lease:
            lease.provider_preparation_calls[index] = None
    if call.result is not None:
        return call.result, False

    error = call.error
    if isinstance(error, MemoryRetirementCapabilityError):
        reason = "missing_capability"
    elif isinstance(error, MemoryRetirementValidationError):
        logger.warning("Memory provider '%s' reversible preparation was invalid: %s",
                       manager._provider_label(call.provider), error)
        reason = "invalid_preparation"
    elif isinstance(error, MemoryRetirementCallError):
        logger.warning("Memory provider '%s' reversible preparation failed: %s",
                       manager._provider_label(call.provider), error)
        reason = "provider_prepare_failed"
    else:
        logger.warning("Memory provider '%s' reversible preparation failed: %s",
                       manager._provider_label(call.provider), error, exc_info=error is not None)
        reason = "provider_prepare_failed"
    with manager._sync_executor_lock:
        if manager._retirement_preparation_lease is lease:
            lease.provider_leases[index] = None
            lease.provider_prepared[index] = False
            lease.provider_reasons[index] = reason
    return None, False


def _prepare_provider_leases(manager: Any, lease: ManagerRetirementLease) -> ManagerPreparationReport:
    """Drain accepted manager work, then acquire each source-owned provider lease.

    Provider preparation is opaque optional code.  All provider calls are admitted before
    observation and share one absolute budget; a stalled call remains attached to this lease
    for a later retry instead of being cancelled or replaced.
    """
    with manager._sync_executor_lock:
        if not _same_preparation_binding(manager, lease):
            return _preparation_refusal(manager, "provider_binding_changed")
        if lease.phase == "draining":
            lease.reason = "preparation_in_progress"

    if not lease.manager_work_drained:
        if not manager._prepare_retirement_drain():
            with manager._sync_executor_lock:
                if manager._retirement_preparation_lease is lease:
                    lease.phase = "prepare_unknown"
                    lease.reason = "drain_timeout"
            return _preparation_report(lease)
        with manager._sync_executor_lock:
            if not _same_preparation_binding(manager, lease):
                return _preparation_refusal(manager, "provider_binding_changed")
            lease.manager_work_drained = True

    with manager._sync_executor_lock:
        active_calls = any(
            call is not None and not call.done.is_set()
            for call in lease.provider_preparation_calls
        )
        if lease.preparation_deadline is None or (
            time.monotonic() >= lease.preparation_deadline and not active_calls
        ):
            from agent import memory_manager as memory_manager_module

            lease.preparation_deadline = time.monotonic() + memory_manager_module._SYNC_DRAIN_TIMEOUT_S

    pending_indexes: list[int] = []
    for index, _provider in enumerate(lease.providers):
        with manager._sync_executor_lock:
            if not _same_preparation_binding(manager, lease):
                return _preparation_refusal(manager, "provider_binding_changed")
            if lease.provider_prepared[index]:
                continue
            pending_indexes.append(index)

    # Admit every pending provider before waiting on any one provider, so the deadline is
    # shared across the provider graph rather than multiplied by provider count.
    for index in pending_indexes:
        _start_provider_preparation_call(manager, lease, index)

    for index in pending_indexes:
        observed, timed_out = _provider_preparation_attempt(manager, lease, index)
        with manager._sync_executor_lock:
            if manager._retirement_preparation_lease is not lease:
                return _preparation_refusal(manager, "provider_binding_changed")
            if observed is not None:
                lease.provider_leases[index] = observed.lease
                lease.provider_prepared[index] = observed.prepared
                lease.provider_reasons[index] = observed.reason
            elif timed_out:
                lease.provider_reasons[index] = "preparation_timeout"
            elif not lease.provider_reasons[index]:
                lease.provider_reasons[index] = "provider_prepare_failed"

    with manager._sync_executor_lock:
        if not _same_preparation_binding(manager, lease):
            return _preparation_refusal(manager, "provider_binding_changed")
        if all(lease.provider_prepared):
            lease.phase = "prepared"
            lease.reason = "no_provider" if not lease.providers else "prepared"
        else:
            lease.phase = "prepare_unknown"
            lease.reason = next(
                (reason for ready, reason in zip(lease.provider_prepared, lease.provider_reasons) if not ready),
                "preparation_incomplete",
            )
    return _preparation_report(lease)


def prepare_retirement(manager: Any) -> ManagerPreparationReport:
    """Acquire a reversible manager lease without setting shutdown or closing providers."""
    lease, report, created = _new_preparation_lease(manager)
    if report is not None:
        return report
    if lease is None:
        return _preparation_refusal(manager, "preparation_unavailable")
    if not created:
        if lease.phase == "prepared":
            return _preparation_report(lease)
        if lease.phase not in {"draining", "prepare_unknown"}:
            return _preparation_report(lease)
    return _prepare_provider_leases(manager, lease)


def _resume_provider_leases(manager: Any, lease: ManagerRetirementLease, indexes: list[int]) -> list[int]:
    resumed: list[int] = []
    for index in indexes:
        provider_lease = lease.provider_leases[index]
        if provider_lease is None:
            continue
        try:
            if resume_memory_provider(lease.providers[index], provider_lease):
                resumed.append(index)
                continue
        except (MemoryRetirementCapabilityError, MemoryRetirementValidationError, MemoryRetirementCallError):
            pass
        except Exception:  # Optional provider boundary: fail closed.
            logger.warning("Memory provider '%s' reversible resume failed", manager._provider_label(lease.providers[index]),
                           exc_info=True)
        break
    return resumed


def _refence_resumed_providers(manager: Any, lease: ManagerRetirementLease, indexes: list[int]) -> None:
    """Re-fence siblings that resumed before another provider refused resume."""
    with manager._sync_executor_lock:
        if not any(call is not None and not call.done.is_set() for call in lease.provider_preparation_calls):
            from agent import memory_manager as memory_manager_module

            lease.preparation_deadline = time.monotonic() + memory_manager_module._SYNC_DRAIN_TIMEOUT_S
    for index in indexes:
        observed, timed_out = _provider_preparation_attempt(manager, lease, index)
        with manager._sync_executor_lock:
            if observed is None or timed_out:
                lease.provider_leases[index] = None
                lease.provider_prepared[index] = False
                lease.provider_reasons[index] = "resume_refence_timeout" if timed_out else "resume_refence_failed"
            else:
                lease.provider_leases[index] = observed.lease
                lease.provider_prepared[index] = observed.prepared
                lease.provider_reasons[index] = observed.reason


def resume_retirement(manager: Any, lease: ManagerRetirementLease) -> bool:
    """Resume every exact provider lease, remaining closed if any resume cannot be proven."""
    if not isinstance(lease, ManagerRetirementLease):
        return False
    with manager._sync_executor_lock:
        if not _same_preparation_binding(manager, lease):
            return False
        if lease.phase not in {"draining", "prepare_unknown", "prepared", "resume_failed"}:
            return False
        # A timed-out provider preparation remains accepted work.  It must be observed
        # through the same lease before the manager can reopen admission; dropping it here
        # would let a late provider fence race with resumed calls.
        if any(call is not None for call in lease.provider_preparation_calls):
            return False
        lease.phase = "resuming"
        indexes = [index for index, provider_lease in enumerate(lease.provider_leases) if provider_lease is not None]

    resumed = _resume_provider_leases(manager, lease, indexes)
    if len(resumed) != len(indexes):
        _refence_resumed_providers(manager, lease, resumed)
        with manager._sync_executor_lock:
            if manager._retirement_preparation_lease is lease:
                lease.phase = "resume_failed"
                lease.reason = "provider_resume_failed"
        return False

    with manager._sync_executor_lock:
        if not _same_preparation_binding(manager, lease):
            lease.phase = "resume_failed"
            lease.reason = "provider_binding_changed"
            return False
        manager._retirement_preparation_lease = None
        manager._hard_retirement_state = None
        lease.phase = "resumed"
        lease.reason = "resumed"
    return True


def _commit_provider_attempt(manager: Any, lease: ManagerRetirementLease, index: int) -> RetirementAttempt:
    provider = lease.providers[index]
    try:
        result = commit_memory_provider(provider, lease.provider_leases[index], receipt=lease.receipts[index])
    except MemoryRetirementCapabilityError:
        return RetirementAttempt(provider=manager._provider_label(provider), status="unknown", reason="missing_capability")
    except MemoryRetirementValidationError as error:
        logger.warning("Memory provider '%s' reversible commit was invalid: %s",
                       manager._provider_label(provider), error)
        return RetirementAttempt(provider=manager._provider_label(provider), status="unknown", reason="invalid_result")
    except MemoryRetirementCallError as error:
        logger.warning("Memory provider '%s' reversible commit failed: %s",
                       manager._provider_label(provider), error)
        return RetirementAttempt(provider=manager._provider_label(provider), status="unknown", reason="provider_call_failed")
    except Exception as error:  # Optional provider boundary: retain exact custody.
        logger.warning("Memory provider '%s' reversible commit failed: %s",
                       manager._provider_label(provider), error, exc_info=True)
        return RetirementAttempt(provider=manager._provider_label(provider), status="unknown", reason="provider_call_failed")
    return RetirementAttempt(
        provider=manager._provider_label(provider), status=result.status, reason=result.reason, result=result,
    )


def commit_retirement(manager: Any, lease: ManagerRetirementLease) -> MemoryRetirementReport:
    """Commit exact prepared provider leases; UNKNOWN keeps the same manager lease for retry."""
    if not isinstance(lease, ManagerRetirementLease):
        return MemoryRetirementReport(status="unknown", reason="invalid_lease")
    with manager._sync_executor_lock:
        if manager._retirement_preparation_lease is not lease or lease.manager is not manager:
            return MemoryRetirementReport(status="unknown", reason="invalid_lease")
        if lease.phase == "released":
            state = manager._hard_retirement_state
            return state.report if state is not None and state.report is not None else MemoryRetirementReport(
                status="released", reason="released",
            )
        if lease.phase == "committing":
            return MemoryRetirementReport(status="unknown", reason="retirement_in_progress")
        if lease.phase not in {"prepared", "commit_unknown"}:
            return MemoryRetirementReport(status="unknown", reason=lease.reason)
        if not lease.manager_work_drained or not all(
            prepared and provider_lease is not None
            for prepared, provider_lease in zip(lease.provider_prepared, lease.provider_leases)
        ):
            return MemoryRetirementReport(status="unknown", reason="preparation_incomplete")
        if not _same_preparation_binding(manager, lease):
            return MemoryRetirementReport(status="unknown", reason="provider_binding_changed")
        lease.phase = "committing"
        lease.reason = "commit_in_progress"
        manager._shutting_down = True

    for index, attempt in enumerate(lease.provider_attempts):
        if attempt is not None and attempt.result is not None and attempt.result.released:
            continue
        observed = _commit_provider_attempt(manager, lease, index)
        with manager._sync_executor_lock:
            if manager._retirement_preparation_lease is lease:
                lease.provider_attempts[index] = observed

    with manager._sync_executor_lock:
        state = manager._hard_retirement_state
        if state is None or manager._retirement_preparation_lease is not lease:
            return MemoryRetirementReport(status="unknown", reason="provider_binding_changed")
        if not manager._same_provider_objects(tuple(manager._providers), lease.providers):
            report = MemoryRetirementReport(status="unknown", reason="provider_binding_changed")
            state.report, state.active = report, False
            lease.phase, lease.reason = "commit_unknown", report.reason
            return report
        attempts = tuple(attempt for attempt in lease.provider_attempts if attempt is not None)
        unresolved = [
            attempt for attempt in lease.provider_attempts
            if attempt is None or attempt.result is None or not attempt.result.released
        ]
        if not unresolved and len(attempts) == len(lease.providers):
            report = MemoryRetirementReport(status="released", reason="no_provider" if not lease.providers else "released",
                                            attempts=attempts)
            for provider in lease.providers:
                manager._drop_provider(provider)
            state.report, state.active = report, False
            lease.phase, lease.reason = "released", report.reason
            return report
        reason = next((attempt.reason for attempt in unresolved if attempt is not None), "retirement_in_progress")
        report = MemoryRetirementReport(status="unknown", reason=reason, attempts=attempts)
        state.report, state.active = report, False
        lease.phase, lease.reason = "commit_unknown", reason
        return report
