"""Authenticated owner context for shell-hook callbacks.

The shell-hook callback receives ordinary event data, but that data is not an
ownership boundary.  This module carries the already authenticated owner
object through synchronous, asynchronous, and worker callbacks.  It does not
create a ledger or copy identity fields into a mutable global map.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import uuid
from typing import Any, Callable, Iterator


class HookScopeError(RuntimeError):
    """A managed callback cannot prove the owner or its result consumer."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True)
class HookAckProof:
    """Typed proof returned by a source-owned result consumer."""

    owner_class: str
    owner_id: str
    profile_id: str
    authority_epoch: int
    process_instance: str
    source_identity: str
    invocation_id: str
    consumer_id: str
    result_identity: str
    committed: bool
    readback: bool

    def validate(self) -> None:
        values = (
            self.owner_class, self.owner_id, self.profile_id,
            self.process_instance, self.source_identity, self.invocation_id,
            self.consumer_id, self.result_identity,
        )
        if any(type(value) is not str or not value for value in values):
            raise HookScopeError("hook_result_identity_missing")
        if type(self.authority_epoch) is not int or self.authority_epoch < 1:
            raise HookScopeError("hook_result_epoch_invalid")
        if self.committed is not True or self.readback is not True:
            raise HookScopeError("hook_result_not_read_back")


@dataclass(frozen=True)
class HookResultConsumer:
    """Source-owned staged-result/finalizer callbacks.

    A shell callback can only stage a value.  The owner supplies these
    callbacks when it has a real durable writer or delivery fence.  A missing
    finalizer is an explicit refusal, never a successful proof.
    """

    consumer_id: str
    stage_callback: Callable[[str, Any], None] | None = None
    finalize_callback: Callable[[str], HookAckProof] | None = None
    unknown_callback: Callable[[str], None] | None = None
    pending_callback: Callable[[], tuple[str, ...]] | None = None

    def stage(self, invocation_id: str, value: Any) -> None:
        if not callable(self.stage_callback):
            raise HookScopeError("hook_result_consumer_missing")
        self.stage_callback(invocation_id, value)

    def finalize(self, invocation_id: str) -> HookAckProof:
        if not callable(self.finalize_callback):
            raise HookScopeError("hook_result_consumer_missing")
        proof = self.finalize_callback(invocation_id)
        if not isinstance(proof, HookAckProof):
            raise HookScopeError("hook_result_proof_missing")
        proof.validate()
        if proof.consumer_id != self.consumer_id or proof.invocation_id != invocation_id:
            raise HookScopeError("hook_result_identity_mismatch")
        return proof

    def mark_unknown(self, reason: str) -> None:
        if callable(self.unknown_callback):
            self.unknown_callback(reason)

    def pending_invocations(self) -> tuple[str, ...]:
        if not callable(self.pending_callback):
            return ()
        values = self.pending_callback()
        return values if isinstance(values, tuple) else ()


@dataclass(frozen=True)
class HookParentBinding:
    """Immutable view over one exact, source-owned parent object."""

    owner: Any
    event: str
    invocation_id: str
    consumer: HookResultConsumer | None = None

    @classmethod
    def from_turn(
        cls, binding: Any, event: str, invocation_id: str,
        consumer: HookResultConsumer | None = None,
    ) -> HookParentBinding:
        from gateway.runtime_service_custody import TurnCustodyBinding

        if not isinstance(binding, TurnCustodyBinding):
            raise HookScopeError("preservation_owner_unmapped")
        return cls(binding, _text(event, "hook_event"), _text(invocation_id, "hook_invocation"), consumer)

    @classmethod
    def from_external_parent(
        cls, parent: Any, event: str, invocation_id: str,
        consumer: HookResultConsumer | None = None,
    ) -> HookParentBinding:
        from gateway.runtime_service_operations import ExternalProcessParent

        if not isinstance(parent, ExternalProcessParent):
            raise HookScopeError("preservation_owner_unmapped")
        if getattr(parent, "state", None) in {"unknown", "terminal"}:
            raise HookScopeError("preservation_owner_unknown")
        return cls(parent, _text(event, "hook_event"), _text(invocation_id, "hook_invocation"), consumer)

    @property
    def is_turn(self) -> bool:
        from gateway.runtime_service_custody import TurnCustodyBinding
        return isinstance(self.owner, TurnCustodyBinding)

    @property
    def coordinator(self) -> Any:
        value = getattr(self.owner, "coordinator", None)
        if value is None:
            raise HookScopeError("preservation_coordinator_missing")
        return value

    @property
    def parent_handle(self) -> Any:
        value = getattr(self.owner, "parent_handle", None)
        if value is None:
            value = getattr(self.owner, "root_handle", None)
        if value is None:
            raise HookScopeError("preservation_parent_missing")
        return value

    @property
    def owner_class(self) -> str:
        return str(getattr(self.parent_handle, "owner_class", ""))

    @property
    def owner_id(self) -> str:
        value = getattr(self.parent_handle, "owner_id", None)
        if value is None:
            value = getattr(self.owner, "operation_id", None)
        return _text(value, "owner_id")

    @property
    def profile_id(self) -> str:
        return _text(getattr(self.owner, "profile_id", None), "profile_id")

    @property
    def authority_epoch(self) -> int:
        value = getattr(self.owner, "authority_epoch", None)
        if type(value) is not int or value < 1:
            raise HookScopeError("preservation_epoch_invalid")
        return value

    @property
    def process_instance(self) -> str:
        return _text(getattr(self.owner, "process_instance", None), "process_instance")

    @property
    def source_identity(self) -> str:
        value = getattr(self.owner, "source_identity", None)
        return _text(value, "source_identity")

    @property
    def principal_id(self) -> str | None:
        value = getattr(self.owner, "principal_id", None)
        return value if isinstance(value, str) and value else None

    @property
    def session_id(self) -> str | None:
        value = getattr(self.owner, "session_id", None)
        return value if isinstance(value, str) and value else None

    @property
    def admission_id(self) -> str | None:
        value = getattr(self.owner, "admission_id", None)
        return value if isinstance(value, str) and value else None

    @property
    def execution_generation(self) -> int | None:
        value = getattr(self.owner, "execution_generation", None)
        return value if type(value) is int else None

    def validate_live(self) -> None:
        if self.is_turn:
            from gateway.runtime_service_custody import validate_turn_binding
            validate_turn_binding(self.owner)
            return
        if getattr(self.owner, "state", None) in {"unknown", "terminal"}:
            raise HookScopeError("preservation_owner_unknown")
        coordinator = self.coordinator
        if getattr(self.parent_handle, "coordinator_id", None) != getattr(coordinator, "_coordinator_id", None):
            raise HookScopeError("preservation_owner_mismatch")

    def with_invocation(self, event: str, consumer: HookResultConsumer | None = None) -> HookParentBinding:
        return HookParentBinding(
            self.owner, _text(event, "hook_event"), uuid.uuid4().hex,
            self.consumer if consumer is None else consumer,
        )

    def mark_unknown(self, reason: str) -> None:
        if self.consumer is not None:
            self.consumer.mark_unknown(reason)


def _text(value: Any, field: str) -> str:
    if type(value) is not str or not value:
        raise HookScopeError(f"{field}_missing")
    return value


_CURRENT_HOOK_PARENT: ContextVar[HookParentBinding | None] = ContextVar(
    "current_shell_hook_parent", default=None,
)
_MANAGED_HOOK_ROUTE: ContextVar[bool] = ContextVar("managed_shell_hook_route", default=False)


def current_hook_parent() -> HookParentBinding | None:
    return _CURRENT_HOOK_PARENT.get()


def current_hook_is_managed() -> bool:
    """Whether the producer declared a managed boundary even before its owner adapter exists."""
    return _MANAGED_HOOK_ROUTE.get() or _CURRENT_HOOK_PARENT.get() is not None


@contextmanager
def managed_hook_route() -> Iterator[None]:
    """Mark a source-owned route so missing owner cannot fall through to raw Popen."""
    token = _MANAGED_HOOK_ROUTE.set(True)
    try:
        yield
    finally:
        _MANAGED_HOOK_ROUTE.reset(token)


@contextmanager
def bind_hook_parent(parent: HookParentBinding) -> Iterator[HookParentBinding]:
    """Bind one exact owner through a producer's callback boundary."""
    if not isinstance(parent, HookParentBinding):
        raise HookScopeError("preservation_owner_unmapped")
    try:
        parent.validate_live()
    except BaseException:
        # A copied worker may outlive the turn/profile that authenticated it.
        # Preserve that exact owner as UNKNOWN; never reconstruct a successor
        # binding from callback payload fields.
        parent.mark_unknown("hook_parent_unavailable")
        mark_current_hook_unknown("hook_parent_unavailable")
        raise
    token = _CURRENT_HOOK_PARENT.set(parent)
    try:
        yield parent
    finally:
        _CURRENT_HOOK_PARENT.reset(token)


@contextmanager
def hook_invocation_scope(
    event: str, *, parent: HookParentBinding | None = None,
    consumer: HookResultConsumer | None = None,
) -> Iterator[HookParentBinding | None]:
    """Give one callback one fresh invocation identity.

    When a real I04 binding is already active, it is adapted here.  No
    session/admission identity is reconstructed from event payloads.
    """
    if parent is None:
        parent = current_hook_parent()
    if parent is None:
        from gateway.runtime_service_custody import current_turn_binding

        turn = current_turn_binding()
        if turn is not None:
            parent = HookParentBinding.from_turn(turn, event, uuid.uuid4().hex, consumer)
    else:
        # A producer-level parent may be shared by many subscribers, but each
        # callback gets a distinct invocation identity for retry/drop fencing.
        parent = parent.with_invocation(event, consumer)
    if parent is None:
        yield None
        return
    parent.validate_live()
    token = _CURRENT_HOOK_PARENT.set(parent)
    try:
        yield parent
    except BaseException:
        parent.mark_unknown("hook_callback_failed")
        mark_current_hook_unknown("hook_callback_failed")
        raise
    finally:
        _CURRENT_HOOK_PARENT.reset(token)


def mark_current_hook_unknown(reason: str) -> None:
    """Mark the exact live dispatch operation, if one was already reserved."""
    parent = current_hook_parent()
    if parent is not None:
        parent.mark_unknown(reason)
    try:
        from agent.shell_hook_custody import mark_current_dispatch_unknown
        mark_current_dispatch_unknown(reason)
    except (ImportError, AttributeError, RuntimeError, TypeError, ValueError):
        return

