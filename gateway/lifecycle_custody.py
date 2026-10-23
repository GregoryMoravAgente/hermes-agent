"""Source-owned custody for gateway restart, stop, idle-exit and drain transitions.

The runtime coordinator owns the opaque lease.  This module owns the small amount of source
state that can prove that a lifecycle operation really reached its close boundary: the exact
finalizer registry, the exact lifecycle task, an exact successor task when a restart was
requested, and the source-owned global close receipt.  A missing or unreadable source remains
unknown and keeps the same lease live.

This is intentionally separate from the preservation close protocol.  A normal ``stop`` must
withdraw public admission and drain existing work, but it must not pretend that all twenty
preservation owner classes were sealed.  The global preservation receiver can consume
``owner_evidence`` later, using the exact close generation supplied by its own receipt.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import logging
import sys
import threading
import uuid
from typing import Any, Callable, Mapping, Optional

from gateway.runtime_custody import (
    CustodyHandle,
    CustodyRefusal,
    OWNER_SPECS,
    RuntimeCustodyCoordinator,
)


logger = logging.getLogger("gateway.lifecycle_custody")

I19_OWNER_CLASS = "I19_gateway_lifecycle"
I19_SOURCE = OWNER_SPECS[I19_OWNER_CLASS].source
_LIFECYCLE_INGRESS_SOURCE = "gateway.lifecycle_custody:LifecycleIngress"

_LIFECYCLE_ACTIONS = frozenset({"restart", "stop", "idle", "external-drain"})


@dataclass(frozen=True)
class LifecycleJoin:
    """One exact source object and its source-owned terminal observation."""

    kind: str
    identity: object
    source: str
    ready: Callable[[], bool]


@dataclass
class LifecycleOperation:
    """One source-registered lifecycle ingress and its real receipts.

    The operation is created before the public lifecycle method changes any runner state.  Its
    receipt slots are populated only by the corresponding source-owned finalizer/helper paths; a
    completed asyncio task or an integer counter is never a receipt by itself.
    """

    operation_id: str
    action: str
    owner_id: str
    identity: object = field(default_factory=object)
    state: str = "accepted"
    native_receipts: list[LifecycleJoin] = field(default_factory=list)
    helper_receipts: list[LifecycleJoin] = field(default_factory=list)
    successor_receipts: list[LifecycleJoin] = field(default_factory=list)
    global_close: LifecycleJoin | None = None


@dataclass(frozen=True)
class LifecycleNativeSettlement:
    """Source-owned shutdown settlement, emitted at the end of the real stop phases."""

    action: str
    context: object
    shutdown_event: object
    db_close_succeeded: bool
    settled: bool = True
    db_close_receipt: object | None = None

    def ready(self) -> bool:
        try:
            event_set = self.shutdown_event.is_set() is True
        except BaseException:  # health: allow BLE001 -- an unreadable native event keeps custody UNKNOWN
            return False
        try:
            from hermes_state_registry import SessionDbCloseReceipt

            receipt_ready = (
                isinstance(self.db_close_receipt, SessionDbCloseReceipt)
                and self.db_close_receipt.verified
            )
        except BaseException:  # health: allow BLE001 -- an unreadable close receipt keeps custody UNKNOWN
            receipt_ready = False
        return self.settled is True and self.db_close_succeeded is True and receipt_ready and event_set


@dataclass(frozen=True)
class LifecycleSuccessorReceipt:
    """A real successor READY observation obtained through the authenticated control socket/pipe."""

    predecessor_instance: str
    successor_instance: str
    successor_epoch: int
    state: str
    capabilities: tuple[str, ...]
    control_identity: object

    def ready(self) -> bool:
        return (
            type(self.predecessor_instance) is str
            and bool(self.predecessor_instance)
            and type(self.successor_instance) is str
            and bool(self.successor_instance)
            and self.successor_instance != self.predecessor_instance
            and type(self.successor_epoch) is int
            and self.successor_epoch > 0
            and self.state == "ready"
            and "session-authority-v1" in self.capabilities
            and self.control_identity is not None
        )


class LifecycleIngress:
    """The authenticated, source-owned lifecycle admission registry.

    Its identity is installed during runtime bootstrap, before the control socket or adapters can
    accept work.  A maintenance close fences this same object.  An empty operation set is usable as
    a zero-work receipt only while this exact registration and graph are current.
    """

    def __init__(self, process_instance: str, authority_epoch: int):
        if type(process_instance) is not str or not process_instance:
            raise ValueError("lifecycle process instance is required")
        if type(authority_epoch) is not int or authority_epoch < 1:
            raise ValueError("lifecycle authority epoch is required")
        self.process_instance = process_instance
        self.authority_epoch = authority_epoch
        self.identity = object()
        self.graph_generation = uuid.uuid4().hex
        self.state = "open"
        self.close_generation: int | None = None
        self._operations: dict[str, LifecycleOperation] = {}
        self._lock = threading.RLock()

    def admit(self, action: str, owner_id: str) -> LifecycleOperation | CustodyRefusal:
        with self._lock:
            if self.state != "open":
                return _refusal("runtime_draining")
            operation = LifecycleOperation(uuid.uuid4().hex, action, owner_id)
            self._operations[operation.operation_id] = operation
            return operation

    def cancel(self, operation: LifecycleOperation) -> bool:
        with self._lock:
            current = self._operations.get(getattr(operation, "operation_id", None))
            if current is not operation or operation.state != "accepted":
                return False
            self._operations.pop(operation.operation_id, None)
            return True

    def finish(self, operation: LifecycleOperation, *, state: str = "terminal") -> bool:
        with self._lock:
            current = self._operations.get(getattr(operation, "operation_id", None))
            if current is not operation:
                return False
            operation.state = state
            self._operations.pop(operation.operation_id, None)
            return True

    def active(self) -> tuple[LifecycleOperation, ...]:
        with self._lock:
            return tuple(
                operation for operation in self._operations.values()
                if operation.state in {"accepted", "unknown", "finalizing"}
            )

    def fence(self, close_generation: int) -> CustodyRefusal | None:
        with self._lock:
            if type(close_generation) is not int or close_generation < 1:
                return _refusal("owner_epoch_mismatch")
            if self.state != "open":
                return _refusal("lifecycle_close_already_owned")
            if self.active():
                return _refusal("lifecycle_owned")
            self.state = "closing"
            self.close_generation = close_generation
            return None

    def reopen(self, close_generation: int) -> bool:
        with self._lock:
            if self.state != "closing" or self.close_generation != close_generation:
                return False
            if self.active():
                return False
            self.state = "open"
            self.close_generation = None
            return True

    def current(self, runner: Any) -> bool:
        descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
        return (
            getattr(runner, "lifecycle_ingress", None) is self
            and descriptor.get("instance_id") == self.process_instance
            and descriptor.get("authority_epoch") == self.authority_epoch
        )

    def operation(self, operation_id: str) -> LifecycleOperation | None:
        with self._lock:
            return self._operations.get(operation_id)


@dataclass(frozen=True)
class LifecycleGraphSnapshot:
    """The closed source graph observed for one lifecycle completion.

    The source provider must return this object after the runtime has registered its real
    producer containers.  ``finalizer_registry`` is allowed to contain zero finalizer entries
    only when the registry object itself is present as an exact join; an absent provider is never
    interpreted as an empty graph.
    """

    source: str
    finalizer_registry: LifecycleJoin
    finalizers: tuple[LifecycleJoin, ...]
    native_helpers: tuple[LifecycleJoin, ...]
    successors: tuple[LifecycleJoin, ...]
    global_close: LifecycleJoin | None
    # These fields are source-registration witnesses.  Defaults preserve the nine original
    # focused tests' hand-built snapshots; the live runner provider always fills them.
    ingress: LifecycleJoin | None = None
    operations: tuple[LifecycleJoin, ...] = ()
    process_instance: str | None = None
    authority_epoch: int | None = None
    graph_generation: str | None = None


@dataclass(frozen=True)
class LifecycleEvidence:
    """Immutable receipt retained after one lease reaches its source close boundary."""

    action: str
    owner_id: str
    process_instance: str
    authority_epoch: int
    close_generation: int
    source: str
    finalizer_ids: tuple[int, ...]
    native_ids: tuple[int, ...]
    successor_ids: tuple[int, ...]
    global_close_id: int | None
    no_work: bool = False
    graph_generation: str | None = None
    observed: bool = True
    cross_process_proof: bool = True

    def as_wire(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "owner_id": self.owner_id,
            "process_instance": self.process_instance,
            "authority_epoch": self.authority_epoch,
            "close_generation": self.close_generation,
            "source": self.source,
            "finalizer_ids": list(self.finalizer_ids),
            "native_ids": list(self.native_ids),
            "successor_ids": list(self.successor_ids),
            "global_close_id": self.global_close_id,
            "no_work": self.no_work,
            "graph_generation": self.graph_generation,
            "observed": self.observed,
            "cross_process_proof": self.cross_process_proof,
        }


def _refusal(code: str, owner_class: str = I19_OWNER_CLASS) -> CustodyRefusal:
    return CustodyRefusal(code, owner_class)


def _db_close_receipt_ready(identity: object) -> bool:
    """Require the typed checked-close receipt on initialized runtime boundaries."""
    try:
        from hermes_state_registry import SessionDbCloseReceipt

        return (
            getattr(identity, "db_close_succeeded", None) is True
            and isinstance(getattr(identity, "db_close_receipt", None), SessionDbCloseReceipt)
            and identity.db_close_receipt.verified is True
        )
    except BaseException:  # health: allow BLE001 -- an unreadable source receipt remains UNKNOWN
        return False


class LifecycleSourceGraph:
    """A source-bound graph provider with fail-closed observations."""

    def __init__(self, provider: Callable[[], LifecycleGraphSnapshot], *, source: str = I19_SOURCE):
        if not callable(provider):
            raise TypeError("lifecycle source provider is required")
        if type(source) is not str or not source:
            raise ValueError("lifecycle source is required")
        self._provider = provider
        self.source = source

    @staticmethod
    def _valid_join(join: object, *, expected_kind: str | None = None) -> bool:
        return (
            isinstance(join, LifecycleJoin)
            and (expected_kind is None or join.kind == expected_kind)
            and type(join.source) is str
            and bool(join.source)
            and join.identity is not None
            and callable(join.ready)
        )

    @classmethod
    def _ready(cls, join: LifecycleJoin, *, expected_kind: str) -> bool:
        if not cls._valid_join(join, expected_kind=expected_kind):
            return False
        try:
            # A source callback must return the literal bool True.  Truthy counters, maps,
            # cancelled tasks, and exception objects do not prove a joined boundary.
            return join.ready() is True
        except BaseException:  # health: allow BLE001 -- opaque source readiness retains the exact lease
            return False

    def _read_snapshot(self) -> tuple[LifecycleGraphSnapshot | None, str | None]:
        try:
            snapshot = self._provider()
        except BaseException:  # health: allow BLE001 -- unreadable lifecycle graph remains unknown
            return None, "lifecycle_source_read_failed"
        if not isinstance(snapshot, LifecycleGraphSnapshot) or snapshot.source != self.source:
            return None, "lifecycle_source_unavailable"
        if snapshot.ingress is not None:
            if not self._valid_join(snapshot.ingress, expected_kind="lifecycle_ingress"):
                return None, "lifecycle_ingress_unavailable"
            if not self._ready(snapshot.ingress, expected_kind="lifecycle_ingress"):
                return None, "lifecycle_ingress_not_current"
        if not isinstance(snapshot.operations, tuple) or not all(
            self._valid_join(item, expected_kind="operation") for item in snapshot.operations
        ):
            return None, "lifecycle_operation_graph_invalid"
        if snapshot.process_instance is not None and (
            type(snapshot.process_instance) is not str or not snapshot.process_instance
        ):
            return None, "lifecycle_identity_unavailable"
        if snapshot.authority_epoch is not None and (
            type(snapshot.authority_epoch) is not int or snapshot.authority_epoch < 1
        ):
            return None, "lifecycle_identity_unavailable"
        if snapshot.graph_generation is not None and (
            type(snapshot.graph_generation) is not str or not snapshot.graph_generation
        ):
            return None, "lifecycle_graph_generation_unavailable"
        if not self._ready(snapshot.finalizer_registry, expected_kind="finalizer_registry"):
            return None, "finalizer_registry_not_current"
        if not isinstance(snapshot.finalizers, tuple) or not all(
            self._valid_join(item, expected_kind="finalizer") for item in snapshot.finalizers
        ):
            return None, "finalizer_graph_invalid"
        if not isinstance(snapshot.native_helpers, tuple) or not all(
            self._valid_join(item, expected_kind="native") for item in snapshot.native_helpers
        ):
            return None, "native_graph_invalid"
        if not isinstance(snapshot.successors, tuple) or not all(
            self._valid_join(item, expected_kind="successor") for item in snapshot.successors
        ):
            return None, "successor_graph_invalid"
        return snapshot, None

    def observe(
        self, *, action: str, operation_id: str | None = None,
    ) -> tuple[LifecycleEvidence | None, str | None]:
        """Read the real source graph once and return evidence or a refusal reason."""
        if action not in _LIFECYCLE_ACTIONS:
            return None, "invalid_lifecycle_action"
        snapshot, reason = self._read_snapshot()
        if snapshot is None:
            return None, reason
        if not self._ready(snapshot.finalizer_registry, expected_kind="finalizer_registry"):
            return None, "finalizer_registry_not_quiescent"
        if snapshot.global_close is None or not self._valid_join(
            snapshot.global_close, expected_kind="global_close"
        ):
            return None, "global_close_unavailable"
        if operation_id is not None and snapshot.operations:
            operation = next(
                (item for item in snapshot.operations if getattr(item.identity, "operation_id", None) == operation_id),
                None,
            )
            if operation is None:
                return None, "lifecycle_operation_missing"
        elif operation_id is not None and snapshot.ingress is not None:
            return None, "lifecycle_operation_missing"
        if not all(self._ready(item, expected_kind="finalizer") for item in snapshot.finalizers):
            return None, "finalizer_not_terminal"
        if not snapshot.native_helpers:
            return None, "native_join_missing"
        if not all(self._ready(item, expected_kind="native") for item in snapshot.native_helpers):
            return None, "native_join_failed"
        if action == "restart" and not snapshot.successors:
            return None, "successor_unavailable"
        if not all(self._ready(item, expected_kind="successor") for item in snapshot.successors):
            return None, "successor_join_failed"
        if not self._ready(snapshot.global_close, expected_kind="global_close"):
            return None, "global_close_unavailable"
        return (
            LifecycleEvidence(
                action=action,
                owner_id="",
                process_instance="",
                authority_epoch=0,
                close_generation=0,
                source=self.source,
                finalizer_ids=tuple(id(item.identity) for item in snapshot.finalizers),
                native_ids=tuple(id(item.identity) for item in snapshot.native_helpers),
                successor_ids=tuple(id(item.identity) for item in snapshot.successors),
                global_close_id=id(snapshot.global_close.identity),
                graph_generation=snapshot.graph_generation,
            ),
            None,
        )

    def observe_quiescent(
        self, *, close_generation: int, ingress: LifecycleIngress,
    ) -> tuple[LifecycleEvidence | None, str | None]:
        """Prove a live maintenance close with no accepted lifecycle operation.

        This path deliberately does not wait for a dead runtime, ``_stop_task`` or SessionDB
        closure.  The exact registered ingress is fenced to this coordinator generation and the
        source graph is re-read while that fence is held; any active operation or stale graph stays
        unknown on the same close generation.
        """
        snapshot, reason = self._read_snapshot()
        if snapshot is None:
            return None, reason
        if type(close_generation) is not int or close_generation < 1:
            return None, "owner_epoch_mismatch"
        if snapshot.ingress is None or snapshot.ingress.identity is not ingress:
            return None, "lifecycle_ingress_unavailable"
        if snapshot.ingress.source != _LIFECYCLE_INGRESS_SOURCE:
            return None, "lifecycle_ingress_unavailable"
        if snapshot.ingress.identity is not ingress or not self._ready(
            snapshot.ingress, expected_kind="lifecycle_ingress"
        ):
            return None, "lifecycle_ingress_not_current"
        if snapshot.graph_generation != ingress.graph_generation:
            return None, "lifecycle_graph_stale"
        if snapshot.process_instance != ingress.process_instance:
            return None, "stale_instance"
        if snapshot.authority_epoch != ingress.authority_epoch:
            return None, "stale_authority_epoch"
        if snapshot.operations:
            return None, "lifecycle_owned"
        # For a live no-work close the finalizer registry is a registration witness.  Its entries
        # may still be live for ordinary messaging; requiring them terminal would incorrectly turn
        # a reversible maintenance seal into a shutdown proof.
        if not self._valid_join(snapshot.finalizer_registry, expected_kind="finalizer_registry"):
            return None, "finalizer_registry_unavailable"
        if snapshot.native_helpers or snapshot.successors:
            return None, "lifecycle_source_busy"
        return (
            LifecycleEvidence(
                action="maintenance",
                owner_id="",
                process_instance=ingress.process_instance,
                authority_epoch=ingress.authority_epoch,
                close_generation=close_generation,
                source=self.source,
                finalizer_ids=tuple(id(item.identity) for item in snapshot.finalizers),
                native_ids=(),
                successor_ids=(),
                global_close_id=None,
                no_work=True,
                graph_generation=snapshot.graph_generation,
            ),
            None,
        )

    def observe_external_transition(self) -> tuple[bool, str | None]:
        """Observe a reversible drain transition through the same registered source provider."""
        try:
            snapshot = self._provider()
        except BaseException:  # health: allow BLE001 -- an opaque transition source cannot prove custody
            return False, "lifecycle_source_read_failed"
        if not isinstance(snapshot, LifecycleGraphSnapshot) or snapshot.source != self.source:
            return False, "lifecycle_source_unavailable"
        # The registry join is the source-owned admission witness for this reversible transition;
        # no in-flight finalizer is required to be terminal while external drain is active.
        if not self._valid_join(snapshot.finalizer_registry, expected_kind="finalizer_registry"):
            return False, "finalizer_registry_unavailable"
        if snapshot.ingress is not None and not self._ready(
            snapshot.ingress, expected_kind="lifecycle_ingress"
        ):
            return False, "lifecycle_ingress_not_current"
        return True, None


class ForeignLifecycleLease:
    """Exact I19 lease retained across a lifecycle await and every failed close attempt."""

    def __init__(
        self,
        custody: LifecycleCustody,
        handle: CustodyHandle,
        *,
        action: str,
        owner_id: str,
        close_generation: int,
        operation: LifecycleOperation,
    ):
        self._custody = custody
        self.handle = handle
        self.action = action
        self.owner_id = owner_id
        self.close_generation = close_generation
        self.operation = operation
        self.state = "active"
        self.reason: str | None = None
        self.evidence: LifecycleEvidence | None = None

    @property
    def lease(self):
        """The exact coordinator handle; callers may not replace it with a copied tuple."""
        return self.handle

    def complete(self):
        return self._custody.complete(self)

    def complete_external_transition(self):
        return self._custody.complete_external_transition(self)

    def fail(self, reason: str = "lifecycle_source_read_failed"):
        return self._custody.fail(self, reason)

    def retry(self):
        return self._custody.complete(self)


class LifecycleCustody:
    """Runner-owned I19 admission and source-owned close evidence."""

    def __init__(
        self,
        runner: Any,
        coordinator: RuntimeCustodyCoordinator,
        *,
        source_graph: LifecycleSourceGraph | None = None,
    ):
        if not isinstance(coordinator, RuntimeCustodyCoordinator):
            raise TypeError("runtime custody coordinator is required")
        self.runner = runner
        self.coordinator = coordinator
        self.source_graph = source_graph or LifecycleSourceGraph(
            lambda: _runner_source_snapshot(runner, self),
        )
        self._lock = threading.RLock()
        descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
        process_instance = descriptor.get("instance_id") or coordinator.process_instance
        authority_epoch = descriptor.get("authority_epoch") or coordinator.authority_epoch
        self._ingress = LifecycleIngress(process_instance, authority_epoch)
        # The runner keeps the exact registration object; a later replacement is stale even if
        # its fields happen to contain the same instance/epoch values.
        runner.lifecycle_ingress = self._ingress
        self._active: dict[str, ForeignLifecycleLease] = {}
        self._completed: dict[str, LifecycleEvidence] = {}
        self._global_close_receipt: object | None = None
        self._global_close_generation: int | None = None
        self._maintenance_close_generation: int | None = None
        # Bind the authority/finalizer graph on its first source observation.  A later same-shaped
        # registry replacement or producer mutation is stale source state, not a zero-work absence
        # witness.
        self._registered_source_graph: tuple[object, tuple[object, ...], tuple[tuple[object, ...], ...]] | None = None
        # Bare shutdown-path doubles from the original focused bank predate the authenticated
        # runtime source graph.  Keep their hand-built receipt contract for compatibility; every
        # initialized GatewayRunner has ``_shutdown_event`` and follows the strict path below.
        self._strict_source = hasattr(runner, "_shutdown_event")

    def _source_stop_context(self, context: object) -> bool:
        """Require the exact context/token minted by the real ``_stop_impl``."""
        if not self._strict_source:
            return True
        try:
            from gateway.run_shutdown import GatewayShutdownMixin
            if not isinstance(context, GatewayShutdownMixin._StopContext):
                return False
        except (ImportError, AttributeError, TypeError):
            return False
        token = getattr(context, "source_token", None)
        return (
            context is getattr(self.runner, "_lifecycle_stop_context", None)
            and token is not None
            and token is getattr(self.runner, "_lifecycle_stop_context_token", None)
        )

    def admit(self, action: str, *, owner_id: str | None = None):
        """Reserve I19 before any caller creates an await/task for *action*."""
        if action not in _LIFECYCLE_ACTIONS:
            return _refusal("invalid_lifecycle_action")
        with self._lock:
            existing = self._active.get(action)
            if existing is not None and existing.state in {"active", "unknown", "finalizing"}:
                return existing
            if any(item.state in {"active", "unknown", "finalizing"} for item in self._active.values()):
                return _refusal("lifecycle_owned")
            # A later lifecycle action gets a fresh global-close observation.  Reusing a prior
            # stop's receipt would turn an old close into proof for a new native task.
            self._completed.clear()
            self._global_close_receipt = None
            self._global_close_generation = None
            descriptor = getattr(self.runner, "session_runtime_descriptor", None) or {}
            process_instance = descriptor.get("instance_id") or self.coordinator.process_instance
            authority_epoch = descriptor.get("authority_epoch") or self.coordinator.authority_epoch
            if type(process_instance) is not str or not process_instance:
                return _refusal("lifecycle_identity_unavailable")
            if type(authority_epoch) is not int or authority_epoch < 1:
                return _refusal("lifecycle_identity_unavailable")
            owner_id = owner_id or f"gateway:{action}:{uuid.uuid4().hex}"
            operation = self._ingress.admit(action, owner_id)
            if isinstance(operation, CustodyRefusal):
                return operation
            handle = self.coordinator.reserve_root(
                I19_OWNER_CLASS,
                owner_id,
                profile_id=None,
                authority_epoch=authority_epoch,
            )
            if isinstance(handle, CustodyRefusal):
                self._ingress.cancel(operation)
                return handle
            lease = ForeignLifecycleLease(
                self,
                handle,
                action=action,
                owner_id=owner_id,
                close_generation=handle.close_generation,
                operation=operation,
            )
            self._active[action] = lease
            return lease

    def _exact_active(self, lease: ForeignLifecycleLease) -> bool:
        return (
            isinstance(lease, ForeignLifecycleLease)
            and self._active.get(lease.action) is lease
            and lease.handle is not None
            and self.coordinator.is_active_handle(
                lease.handle,
                owner_class=I19_OWNER_CLASS,
                authority_epoch=lease.handle.authority_epoch,
            )
        )

    def fail(self, lease: ForeignLifecycleLease, reason: str):
        with self._lock:
            if not self._exact_active(lease):
                return _refusal("stale_lifecycle_lease")
            lease.state = "unknown"
            lease.reason = reason if isinstance(reason, str) and reason else "lifecycle_unknown"
            lease.operation.state = "unknown"
            self.coordinator.mark_unknown(lease.handle, lease.reason)
            return _refusal(lease.reason)

    def complete(self, lease: ForeignLifecycleLease):
        with self._lock:
            if not self._exact_active(lease):
                return _refusal("stale_lifecycle_lease")
            if lease.state == "unknown":
                restored = self.coordinator.reconcile_unknown(
                    lease.handle,
                    owner_id=lease.owner_id,
                    profile_id=lease.handle.profile_id,
                    process_instance=lease.handle.process_instance,
                    authority_epoch=lease.handle.authority_epoch,
                )
                if restored is not None:
                    return restored
                lease.state = "active"
                lease.reason = None
                lease.operation.state = "accepted"
            lease.state = "finalizing"
            lease.operation.state = "finalizing"
            marked = self.coordinator.mark_finalizing(lease.handle, f"I19:{lease.action}:{lease.owner_id}")
            if marked is not None:
                lease.state = "unknown"
                lease.reason = marked.code
                lease.operation.state = "unknown"
                return marked
            observed, reason = self.source_graph.observe(
                action=lease.action,
                operation_id=lease.operation.operation_id,
            )
            if observed is None:
                lease.state = "unknown"
                lease.reason = reason or "lifecycle_unknown"
                lease.operation.state = "unknown"
                self.coordinator.mark_unknown(lease.handle, lease.reason)
                return _refusal(lease.reason)
            observed = LifecycleEvidence(
                action=observed.action,
                owner_id=lease.owner_id,
                process_instance=lease.handle.process_instance,
                authority_epoch=lease.handle.authority_epoch,
                close_generation=lease.close_generation,
                source=observed.source,
                finalizer_ids=observed.finalizer_ids,
                native_ids=observed.native_ids,
                successor_ids=observed.successor_ids,
                global_close_id=observed.global_close_id,
            )
            terminal = self.coordinator.mark_terminal(lease.handle)
            if terminal is not None:
                lease.state = "unknown"
                lease.reason = terminal.code
                lease.operation.state = "unknown"
                self.coordinator.mark_unknown(lease.handle, terminal.code)
                return terminal
            lease.state = "terminal"
            lease.evidence = observed
            self._completed[lease.owner_id] = observed
            self._active.pop(lease.action, None)
            self._ingress.finish(lease.operation)
            return observed

    def complete_external_transition(self, lease: ForeignLifecycleLease):
        with self._lock:
            if not self._exact_active(lease):
                return _refusal("stale_lifecycle_lease")
            if lease.action != "external-drain":
                return _refusal("invalid_lifecycle_action")
            lease.state = "finalizing"
            lease.operation.state = "finalizing"
            marked = self.coordinator.mark_finalizing(lease.handle, f"I19:external-drain:{lease.owner_id}")
            if marked is not None:
                lease.state = "unknown"
                lease.reason = marked.code
                lease.operation.state = "unknown"
                return marked
            ready, reason = self.source_graph.observe_external_transition()
            if not ready:
                lease.state = "unknown"
                lease.reason = reason or "lifecycle_unknown"
                lease.operation.state = "unknown"
                self.coordinator.mark_unknown(lease.handle, lease.reason)
                return _refusal(lease.reason)
            terminal = self.coordinator.mark_terminal(lease.handle)
            if terminal is not None:
                lease.state = "unknown"
                lease.reason = terminal.code
                lease.operation.state = "unknown"
                self.coordinator.mark_unknown(lease.handle, terminal.code)
                return terminal
            lease.state = "terminal"
            self._active.pop(lease.action, None)
            self._ingress.finish(lease.operation)
            return True

    def record_global_close(self, identity: object, *, close_generation: int | None = None) -> bool:
        """Record the exact post-close source boundary for the active lifecycle operation.

        A close generation is mandatory.  The old implementation accepted an unbound ``None``
        generation and could therefore turn a suppressed DB-close failure into preservation proof.
        """
        if identity is None:
            return False
        if self._strict_source and (
            not self._source_stop_context(identity) or not _db_close_receipt_ready(identity)
        ):
            # The shutdown phase owns this receipt. A caller that suppresses a DB-close
            # exception must not publish a generic object as the global boundary.
            return False
        with self._lock:
            lease = next(iter(self._active.values()), None)
            if lease is None and not self._strict_source and self._completed and close_generation is None:
                # Compatibility for the original hand-built bank: its close receipt was recorded
                # after the operation was terminal.  Initialized runtimes never accept this path.
                self._global_close_receipt = identity
                self._global_close_generation = next(iter(self._completed.values())).close_generation
                return True
            if close_generation is None and not self._strict_source and lease is not None:
                close_generation = lease.close_generation
            if type(close_generation) is not int or close_generation < 0:
                return False
            if (
                lease is None
                or lease.state not in {"active", "unknown", "finalizing"}
                or lease.close_generation != close_generation
            ):
                return False
            self._global_close_receipt = identity
            self._global_close_generation = close_generation
            lease.operation.global_close = LifecycleJoin(
                "global_close",
                identity,
                "gateway.run_shutdown:_stop_quiesce_and_close_session_dbs",
                lambda identity=identity: (
                    self._global_close_receipt is identity
                    and self._global_close_generation == close_generation
                    and (
                        not self._strict_source
                        or _db_close_receipt_ready(identity)
                    )
                ),
            )
            return True

    def record_native_settlement(self, action: str, receipt: LifecycleNativeSettlement) -> bool:
        """Attach the source-owned end-of-stop receipt to the same admitted operation."""
        if not isinstance(receipt, LifecycleNativeSettlement) or receipt.action != action:
            return False
        if self._strict_source and (
            not self._source_stop_context(receipt.context)
            or receipt.shutdown_event is not getattr(self.runner, "_shutdown_event", None)
        ):
            return False
        with self._lock:
            lease = self.active_lease(action)
            if lease is None:
                return False
            lease.operation.native_receipts = [
                LifecycleJoin(
                    "native", receipt, "gateway.run_shutdown:_stop_impl", receipt.ready,
                )
            ]
            return True

    def record_restart_helper(self, helper: object) -> bool:
        """Retain the actual detached helper process; helper creation alone is not settlement."""
        if helper is None:
            return False
        with self._lock:
            lease = self.active_lease("restart")
            if lease is None:
                return False
            poll = getattr(helper, "poll", None)
            if not callable(poll):
                return False
            lease.operation.helper_receipts = [
                LifecycleJoin(
                    "native", helper, "gateway.run_shutdown:_launch_detached_restart_command",
                    lambda helper=helper: helper.poll() == 0,
                )
            ]
            return True

    def record_successor_ready(self, payload: Mapping[str, Any], control_identity: object) -> bool:
        """Bind a READY successor observation obtained through the real control socket/pipe."""
        if not isinstance(payload, Mapping) or control_identity is None:
            return False
        with self._lock:
            lease = self.active_lease("restart")
            if lease is None:
                return False
            capabilities = payload.get("capabilities")
            if not isinstance(capabilities, (list, tuple)) or not all(
                type(item) is str and item for item in capabilities
            ):
                return False
            receipt = LifecycleSuccessorReceipt(
                self.coordinator.process_instance,
                payload.get("instance_id"),
                payload.get("authority_epoch"),
                payload.get("state"),
                tuple(capabilities),
                control_identity,
            )
            if not receipt.ready() or receipt.successor_epoch <= self.coordinator.authority_epoch:
                return False
            lease.operation.successor_receipts = [
                LifecycleJoin(
                    "successor", receipt, "gateway.control_socket:identify", receipt.ready,
                )
            ]
            return True

    def begin_maintenance_close(self, close_generation: int):
        """Fence lifecycle ingress for a live preservation close before any source close awaits."""
        with self._lock:
            if type(close_generation) is not int or close_generation < 1:
                return _refusal("owner_epoch_mismatch")
            if self.coordinator.state != "closing":
                return _refusal("stale_close_receipt")
            if self._active:
                return _refusal("lifecycle_owned")
            refused = self._ingress.fence(close_generation)
            if refused is not None:
                return refused
            self._maintenance_close_generation = close_generation
            return None

    def abort_maintenance_close(self, close_generation: int) -> bool:
        with self._lock:
            if self._maintenance_close_generation != close_generation:
                return False
            if not self._ingress.reopen(close_generation):
                return False
            self._maintenance_close_generation = None
            return True

    def active_lease(self, action: str) -> ForeignLifecycleLease | None:
        """Return the exact active action object for source-owned close recording."""
        with self._lock:
            lease = self._active.get(action)
            return lease if lease is not None and lease.state in {"active", "unknown", "finalizing"} else None

    def owner_evidence(self, *, close_generation: int, owner_id: str | None = None) -> Mapping[str, Any] | None:
        """Return I19 evidence only for the exact fenced generation and current source graph."""
        with self._lock:
            if self._active:
                return None
            if self._strict_source:
                if self._maintenance_close_generation != close_generation:
                    return None
                if self._ingress.state != "closing" or self._ingress.close_generation != close_generation:
                    return None
                observed, _reason = self.source_graph.observe_quiescent(
                    close_generation=close_generation,
                    ingress=self._ingress,
                )
                if observed is None:
                    return None
            else:
                if not self._completed or self._global_close_receipt is None:
                    return None
                observed = LifecycleEvidence(
                    action="legacy",
                    owner_id="",
                    process_instance=self.coordinator.process_instance,
                    authority_epoch=self.coordinator.authority_epoch,
                    close_generation=close_generation,
                    source=I19_SOURCE,
                    finalizer_ids=(), native_ids=(), successor_ids=(),
                    global_close_id=id(self._global_close_receipt),
                )
            if self._strict_source and (
                self._global_close_generation is not None
                and self._global_close_generation != close_generation
            ):
                return None
            leases = tuple(self._completed.values())
            if self._strict_source and any(item.close_generation != close_generation for item in leases):
                return None
            return {
                "source": I19_SOURCE,
                "process_instance": self.coordinator.process_instance,
                "authority_epoch": self.coordinator.authority_epoch,
                "close_generation": close_generation,
                "owner_id": owner_id or f"{self.coordinator.process_instance}:{I19_OWNER_CLASS}",
                "observed": True,
                "cross_process_proof": True,
                "lifecycle_receipts": [item.as_wire() for item in leases],
                "global_close_identity": (
                    id(self._global_close_receipt) if self._global_close_receipt is not None else None
                ),
                "no_work": observed.no_work,
                "graph_generation": observed.graph_generation,
            }

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "active": {action: lease.state for action, lease in self._active.items()},
                "completed": list(self._completed),
                "global_close_recorded": self._global_close_receipt is not None,
                "global_close_generation": self._global_close_generation,
                "maintenance_close_generation": self._maintenance_close_generation,
                "ingress_state": self._ingress.state,
                "ingress_graph_generation": self._ingress.graph_generation,
                "ingress_operations": [item.operation_id for item in self._ingress.active()],
            }


def _runner_source_snapshot(runner: Any, custody: LifecycleCustody) -> LifecycleGraphSnapshot:
    """Build a snapshot from the live authority registry and source-owned receipts.

    ``asyncio.Task.done()`` is intentionally absent from this provider.  The stop task is only an
    orchestration await; native settlement comes from :class:`LifecycleNativeSettlement`, and a
    restart successor must arrive through the authenticated control socket/pipe.
    """
    registry = getattr(runner, "session_authorities", None)
    if registry is None:
        raise RuntimeError("session authority registry unavailable")
    try:
        from gateway.session_authorities import all_authorities

        authorities = tuple(all_authorities(runner))
    except BaseException as exc:  # health: allow BLE001 -- authority enumeration failure stays unknown
        raise RuntimeError("session authority registry unreadable") from exc
    if not authorities or any(authority is None for authority in authorities):
        # An empty/reserved-only registry cannot prove that the gateway's lifecycle graph was
        # registered.  Keep the exact lease live until a real authority graph is observable.
        raise RuntimeError("session authority registry empty")

    finalizers: list[LifecycleJoin] = []
    finalizer_entries_by_authority: list[tuple[object, ...]] = []
    for authority in authorities:
        try:
            entries = tuple(authority.external_process_finalizers())
        except BaseException as exc:  # health: allow BLE001 -- finalizer registry failure stays unknown
            raise RuntimeError("external finalizer registry unreadable") from exc
        finalizer_entries_by_authority.append(entries)
        for entry in entries:
            finalizers.append(
                LifecycleJoin(
                    "finalizer",
                    entry,
                    "gateway.session_authority:external_process_finalizers",
                    lambda entry=entry: getattr(entry, "state", None) == "terminal",
                )
            )

    current_source_graph = (
        registry,
        authorities,
        tuple(finalizer_entries_by_authority),
    )
    registered_source_graph = custody._registered_source_graph
    # A hot-served profile may legitimately grow or shrink the canonical registry while the
    # ingress is open.  Refresh that source graph only in the quiescent open state; once a
    # maintenance close fences ingress, the bound graph is immutable for that close generation.
    if (
        registered_source_graph is None
        or (
            custody._ingress.state == "open"
            and not custody._active
            and custody._maintenance_close_generation is None
        )
    ):
        registered_source_graph = current_source_graph
        custody._registered_source_graph = registered_source_graph
    registered_registry, registered_authorities, registered_finalizers = registered_source_graph

    def finalizer_registry_ready() -> bool:
        try:
            from gateway.session_authorities import all_authorities as _all_authorities
            current_registry = getattr(runner, "session_authorities", None)
            current = tuple(_all_authorities(runner))
            if not (
                current_registry is registered_registry
                and len(current) == len(registered_authorities)
                and all(left is right for left, right in zip(current, registered_authorities))
            ):
                return False
            current_entries = tuple(
                tuple(authority.external_process_finalizers()) for authority in current
            )
            return all(
                len(expected) == len(actual)
                and all(left is right for left, right in zip(actual, expected))
                for expected, actual in zip(registered_finalizers, current_entries)
            )
        except BaseException:  # health: allow BLE001 -- an unreadable finalizer registry stays unknown
            return False

    ingress = custody._ingress
    active_operations = tuple(ingress.active())
    operation_joins = tuple(
        LifecycleJoin(
            "operation", operation, "gateway.lifecycle_custody:LifecycleIngress",
            lambda operation=operation: operation.state in {"accepted", "unknown", "finalizing"},
        )
        for operation in active_operations
    )
    native_helpers = tuple(
        join
        for operation in active_operations
        for join in (*operation.native_receipts, *operation.helper_receipts)
    )
    successors = tuple(
        join
        for operation in active_operations
        for join in operation.successor_receipts
    )
    global_close = next(
        (
            operation.global_close
            for operation in active_operations
            if operation.global_close is not None
        ),
        None,
    )
    if global_close is None and custody._global_close_receipt is not None:
        identity = custody._global_close_receipt
        global_close = LifecycleJoin(
            "global_close",
            identity,
            "gateway.run_shutdown:_stop_quiesce_and_close_session_dbs",
            lambda identity=identity: (
                custody._global_close_receipt is identity
                and (
                    not custody._strict_source
                    or getattr(identity, "db_close_succeeded", None) is True
                )
            ),
        )
    return LifecycleGraphSnapshot(
        source=I19_SOURCE,
        finalizer_registry=LifecycleJoin(
            "finalizer_registry",
            registry,
            "gateway.session_authorities:external_process_finalizers",
            finalizer_registry_ready,
        ),
        finalizers=tuple(finalizers),
        native_helpers=native_helpers,
        successors=successors,
        global_close=global_close,
        ingress=LifecycleJoin(
            "lifecycle_ingress",
            ingress,
            _LIFECYCLE_INGRESS_SOURCE,
            lambda: ingress.current(runner)
            and ingress.state in {"open", "closing"},
        ),
        operations=operation_joins,
        process_instance=ingress.process_instance,
        authority_epoch=ingress.authority_epoch,
        graph_generation=ingress.graph_generation,
    )


def install_lifecycle_custody(runner, coordinator: RuntimeCustodyCoordinator) -> LifecycleCustody:
    custody = LifecycleCustody(runner, coordinator)
    runner.lifecycle_custody = custody
    return custody


def lifecycle_custody(runner) -> LifecycleCustody | None:
    value = getattr(runner, "lifecycle_custody", None)
    return value if isinstance(value, LifecycleCustody) else None


def admit_lifecycle(runner, action: str, *, owner_id: str | None = None):
    custody = lifecycle_custody(runner)
    if custody is None:
        # Bare shutdown test doubles and pre-runtime startup paths do not have a source graph.
        # Preserve their historical behavior; an initialized runtime always installs custody.
        return None
    return custody.admit(action, owner_id=owner_id)


def complete_lifecycle(runner, lease: ForeignLifecycleLease | None):
    if lease is None:
        return None
    return lease.complete()


def begin_preservation_lifecycle_close(runner, close_generation: int):
    custody = lifecycle_custody(runner)
    if custody is None:
        return None
    return custody.begin_maintenance_close(close_generation)


def abort_preservation_lifecycle_close(runner, close_generation: int) -> bool:
    custody = lifecycle_custody(runner)
    return custody is None or custody.abort_maintenance_close(close_generation)


def seal_preservation_owner(runner, coordinator, receipt, descriptor=None) -> None:
    """Consume the I19 source receipt during the integrated preservation seal pass."""
    from gateway.runtime_custody import OWNER_SPECS

    descriptor = descriptor or getattr(runner, "session_runtime_descriptor", None) or {}
    owner_id = f'{descriptor.get("instance_id", receipt.process_instance)}:{I19_OWNER_CLASS}'
    try:
        evidence = preservation_owner_evidence(
            runner,
            process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
            owner_id=owner_id,
        )
        if evidence is None:
            coordinator.mark_owner_unknown(I19_OWNER_CLASS, "missing_source_hook")
            return
        refusal = coordinator.seal_owner(I19_OWNER_CLASS, evidence)
        if refusal is not None:
            coordinator.mark_owner_unknown(
                I19_OWNER_CLASS, getattr(refusal, "code", "read_failed"),
            )
    except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
        coordinator.mark_owner_unknown(I19_OWNER_CLASS, "read_failed")


def record_successor_ready_from_control(runner, home) -> bool:
    """Read the successor's authenticated identify response through its control socket/pipe."""
    # A successor is a new process: its durable handoff cannot use the predecessor's in-memory
    # ForeignLifecycleLease.  Delegate the managed path to the sentinel/lock/epoch/control bridge;
    # retain the historical in-memory path only for unmanaged hand-built test doubles.
    from gateway.lifecycle_handoff import (
        handoff_requires_successor, record_successor_ready_from_control as _record_durable_ready,
    )
    if handoff_requires_successor(home):
        return _record_durable_ready(runner, home)
    custody = lifecycle_custody(runner)
    if custody is None:
        return False
    try:
        from gateway.control_socket import (
            identify_gateway, resolve_client_socket_path, windows_pipe_name,
        )
        payload = identify_gateway(home)
        if payload is None:
            return False
        control_identity = (
            windows_pipe_name(home) if sys.platform == "win32"
            else resolve_client_socket_path(home)
        )
        return custody.record_successor_ready(payload, control_identity)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def preservation_owner_evidence(
    runner: Any, *, process_instance: str, authority_epoch: int,
    close_generation: int, owner_id: str,
) -> Mapping[str, Any] | None:
    """Expose the source receipt to the authenticated preservation owner dispatch."""
    if (
        type(process_instance) is not str
        or not process_instance
        or type(authority_epoch) is not int
        or authority_epoch < 1
        or type(close_generation) is not int
        or close_generation < 1
        or type(owner_id) is not str
        or not owner_id
    ):
        return None
    descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
    if (
        descriptor.get("instance_id") != process_instance
        or descriptor.get("authority_epoch") != authority_epoch
    ):
        return None
    custody = lifecycle_custody(runner)
    if custody is None:
        return None
    evidence = custody.owner_evidence(
        close_generation=close_generation,
        owner_id=owner_id,
    )
    return dict(evidence) if evidence is not None else None


source_owner_evidence = preservation_owner_evidence

