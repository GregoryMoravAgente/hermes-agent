"""Small source-owned adapters for the first I16 cold producer paths.

The runtime custody coordinator remains the only lease ledger.  ``SidecarOwner`` is a
typed view over the exact existing ``ExternalProcessParent``/``ServiceLease`` objects;
it does not index sources, infer ownership from a PID, or turn a missing object into a
successful no-op.  Managed turns use the real I04 binding.  Profile discovery uses the
existing served authority and profile lock path.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
import uuid


@dataclass
class SidecarOwner:
    """One source's exact I16 parent, request, and optional physical receipt."""

    family: str
    source_identity: str
    parent: Any
    service: Any
    request: Any = None
    physical: Any = None
    process: Any = None
    reader: Any = None
    effect_observed: bool = False

    @property
    def coordinator(self):
        return self.parent.coordinator

    @property
    def authority(self):
        return getattr(self.parent, "profile_authority", None)

    @property
    def handle(self):
        return self.parent.root_handle

    def identity(self) -> tuple[Any, ...]:
        """Return the immutable source/authority tuple from the real parent."""
        return (
            self.family,
            self.source_identity,
            self.parent.profile_id,
            self.parent.process_instance,
            self.parent.authority_epoch,
            self.parent.physical_owner_epoch,
            self.handle,
        )


def _require_text(value: Any, code: str) -> str:
    if type(value) is not str or not value:
        raise RuntimeError(code)
    return value


def reserve_managed_sidecar(
    binding: Any,
    *,
    family: str,
    source_identity: str,
    operation_id: str | None = None,
) -> SidecarOwner:
    """Reserve an I16 request under the live I04 admission before a producer effect."""
    from gateway.runtime_service_custody import (
        ServiceCustodyError,
        reserve_service,
    )
    from gateway.runtime_service_operations import parent_for_managed_service

    family = _require_text(family, "source_family_missing")
    source_identity = _require_text(source_identity, "source_identity_missing")
    operation_id = _require_text(
        operation_id or f"{family}:{source_identity}:{uuid.uuid4().hex}",
        "source_operation_missing",
    )
    service = None
    parent = None
    request = None
    try:
        service = reserve_service(binding, f"external:{operation_id}")
        parent = parent_for_managed_service(
            binding,
            service,
            operation_id=operation_id,
            delivery_lifecycle="per_invocation",
        )
        register = getattr(binding.authority, "register_external_process_finalizer", None)
        if not callable(register) or register(operation_id, parent) is not parent:
            raise ServiceCustodyError("finalizer_registry_missing")
        request = service.begin_request(binding)
        parent.turn_binding = binding
        parent._request = request
        binder = getattr(binding.authority, "_preservation_bind_sidecar", None)
        if not callable(binder):
            raise ServiceCustodyError("preservation_owner_unmapped")
        binder(binding.admission_id, binding.parent_handle, service, request)
        return SidecarOwner(family, source_identity, parent, service, request)
    except BaseException:
        # Before a request exists the exact root is definitely unaccepted.  After a
        # request/finalizer has been published, retain the same tuple as UNKNOWN; never
        # manufacture a replacement root while unwinding a constructor failure.
        if parent is not None:
            mark_unknown(
                SidecarOwner(family, source_identity, parent, service, request),
                "partial_launch",
            )
        elif service is not None:
            try:
                service.coordinator.release_unaccepted(service.handle)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                service.coordinator.mark_unknown(service.handle, "partial_launch")
        raise


def reserve_profile_sidecar(
    runner: Any,
    *,
    profile_home: Path,
    family: str,
    source_identity: str,
    operation_id: str | None = None,
) -> SidecarOwner:
    """Reserve a live served-profile source through the canonical authority/lock path."""
    from gateway.runtime_service_operations import (
        ExternalProcessOperationError,
        resolve_external_process_parent,
        reserve_physical,
    )

    family = _require_text(family, "source_family_missing")
    source_identity = _require_text(source_identity, "source_identity_missing")
    operation_id = _require_text(
        operation_id or f"mcp:{source_identity}:{uuid.uuid4().hex}",
        "source_operation_missing",
    )
    parent = resolve_external_process_parent(
        runner,
        profile_home=Path(profile_home),
        operation_id=operation_id,
        source_identity=source_identity,
        mode="live_profile_discovery",
    )
    try:
        service = parent._service
        if service is None:
            raise ExternalProcessOperationError("preservation_owner_unmapped")
        physical = reserve_physical(parent)
        return SidecarOwner(family, source_identity, parent, service, physical=physical)
    except BaseException:
        owner = SidecarOwner(family, source_identity, parent, parent._service)
        mark_unknown(owner, "partial_launch")
        raise


def require_physical(owner: SidecarOwner, *, streams: Iterable[str]) -> Any:
    """Set the receipt's source-required stream set before binding any effect."""
    physical = owner.physical
    if physical is None:
        from gateway.runtime_service_operations import reserve_physical
        physical = reserve_physical(owner.parent)
        owner.physical = physical
    try:
        required = frozenset(streams)
    except (TypeError, ValueError):
        physical.mark_unknown("required_streams_invalid")
        raise RuntimeError("required_streams_invalid")
    existing = getattr(physical, "required_streams", None)
    valid_existing = (
        isinstance(existing, frozenset)
        and bool(existing)
        and existing.issubset({"stdout", "stderr"})
    )
    if existing != required or not valid_existing:
        from gateway.runtime_sidecar_transport import configure_reader_streams
        configured = configure_reader_streams(physical, required)
    else:
        configured = True
    if not configured:
        physical.mark_unknown("required_streams_invalid")
        raise RuntimeError("required_streams_invalid")
    return physical


def bind_process(
    owner: SidecarOwner,
    process: Any,
    *,
    scope: str,
    scope_identity: str,
    process_group_id: int | None = None,
) -> Any:
    """Bind the exact source process to the already-reserved physical receipt."""
    from gateway.runtime_service_operations import ExternalProcessOperationError

    if process is None:
        mark_unknown(owner, "partial_launch")
        raise ExternalProcessOperationError("physical_launch_identity_mismatch")
    physical = require_physical(owner, streams=("stdout",))
    pid = getattr(process, "pid", None)
    if type(pid) is not int or pid <= 0:
        physical.mark_unknown("process_identity_rejected")
        raise ExternalProcessOperationError("physical_launch_identity_mismatch")
    if not physical.bind_process(
        pid,
        None,
        scope,
        scope_identity,
        process_group_id=process_group_id,
    ):
        physical.retain_partial_launch(process, reason="process_identity_rejected")
        raise ExternalProcessOperationError("physical_launch_identity_mismatch")
    try:
        physical.bind_launch(process)
    except BaseException:
        physical.retain_partial_launch(process, reason="partial_launch")
        raise
    owner.process = process
    owner.effect_observed = True
    owner.parent.state = "physical_bound"
    return physical


def bind_reader(owner: SidecarOwner, stream: str, reader: Any) -> None:
    physical = require_physical(owner, streams=("stdout",))
    physical.bind_reader(stream, reader)
    if not physical.reader_started(stream, reader):
        raise RuntimeError("reader_start_unknown")
    owner.reader = reader


def mark_unknown(owner: SidecarOwner, reason: str) -> None:
    """Retain the exact source objects and all existing coordinator records."""
    physical = getattr(owner, "physical", None)
    if physical is not None:
        physical.mark_unknown(reason)
    parent = getattr(owner, "parent", None)
    if parent is None:
        return
    from gateway.runtime_service_operations import mark_unknown as mark_parent_unknown
    try:
        mark_parent_unknown(parent, reason)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        parent.state = "unknown"
        parent.coordinator.mark_unknown(parent.root_handle, reason)
    authority = getattr(parent, "profile_authority", None)
    binding = getattr(parent, "turn_binding", None)
    if authority is not None and binding is not None:
        marker = getattr(authority, "_preservation_mark_unknown", None)
        if callable(marker):
            marker(binding.admission_id, reason)


def mark_process_finished(owner: SidecarOwner, *, reader: Any = None, reason: str) -> None:
    """Record EOF/join and retain the exact owner when source close proof is unavailable."""
    physical = owner.physical
    if physical is not None and reader is not None:
        physical.reader_eof("stdout", reader)
        physical.reader_joined("stdout", reader)
    mark_unknown(owner, reason)


def release_before_effect(owner: SidecarOwner) -> bool:
    """Release only a physical receipt that proves no source effect occurred."""
    physical = owner.physical
    if physical is None:
        mark_unknown(owner, "source_effect_unknown")
        return False
    if physical.process is not None or owner.effect_observed:
        mark_unknown(owner, "partial_launch")
        return False
    if physical.release_before_effect() is not True:
        mark_unknown(owner, "partial_launch")
        return False
    owner.physical = None
    return True


def source_evidence(owner: SidecarOwner) -> dict[str, Any]:
    """Expose only immutable identity/readback fields for source diagnostics/tests."""
    parent = owner.parent
    physical = owner.physical
    return {
        "family": owner.family,
        "source_identity": owner.source_identity,
        "profile_id": parent.profile_id,
        "process_instance": parent.process_instance,
        "authority_epoch": parent.authority_epoch,
        "physical_owner_epoch": parent.physical_owner_epoch,
        "root_handle": parent.root_handle,
        "request_handle": owner.request,
        "physical_handle": None if physical is None else physical.handle,
        "physical_state": None if physical is None else physical.state,
    }
