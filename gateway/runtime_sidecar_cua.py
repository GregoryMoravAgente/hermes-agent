"""Source-owned custody for the ordinary host CUA stdio transport.

The CUA SDK exposes a process only while entering its stdio context.  This module binds
that exact process, its streams, the lifecycle task, and the ``ClientSession`` to one
existing runtime parent before the driver can initialize.  It deliberately keeps CUA's
physical proof separate from the MCP ``ServiceProcessReceipt``: the CUA protocol has no
durable result map, while its context/task exit is still a real finalizer boundary.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import uuid
from typing import Any


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CuaFactoryReservation:
    """The live authority observation captured before a CUA producer is built."""

    authority: Any
    coordinator: Any
    process_instance: str
    authority_epoch: int
    close_generation: int


class CuaStdioSourceFactory:
    """Profile-owned CUA ingress fence.

    This is a capability registered by the serving authority during bootstrap.  It does
    not own leases; the authority finalizer index and the runtime coordinator remain the
    owners.  Its only job is to make the open/close-generation and retained-source checks
    at the producer construction boundary.
    """

    family = "cua_stdio"

    def __init__(self, authority: Any):
        if authority is None:
            raise RuntimeError("cua_factory_authority_missing")
        self.authority = authority

    def _coordinator(self) -> Any:
        getter = getattr(self.authority, "_preservation_coordinator", None)
        coordinator = getter() if callable(getter) else None
        if coordinator is None:
            raise RuntimeError("cua_factory_coordinator_missing")
        return coordinator

    def reserve_before_producer(self, *, source_identity: str) -> CuaFactoryReservation:
        """Refuse close/unknown ingress before an operation UUID or producer exists."""
        if type(source_identity) is not str or not source_identity:
            raise RuntimeError("cua_source_identity_missing")
        authority = self.authority
        if getattr(authority, "retiring", False) or getattr(authority, "retired", False):
            raise RuntimeError("cua_profile_retiring")
        coordinator = self._coordinator()
        status = coordinator.status()
        process_instance = getattr(authority, "instance_id", None)
        authority_epoch = getattr(authority, "epoch", None)
        if (
            type(process_instance) is not str or not process_instance
            or type(authority_epoch) is not int or authority_epoch < 1
            or status.get("process_instance") != process_instance
            or status.get("authority_epoch") != authority_epoch
            or status.get("state") != "open"
        ):
            raise RuntimeError("cua_close_generation_closed")
        for parent in authority.external_process_finalizers():
            receipt = getattr(parent, "_cua_receipt", None)
            owner = getattr(receipt, "owner", None)
            if (
                getattr(parent, "source_identity", None) == source_identity
                or (
                    isinstance(receipt, CuaStdioReceipt)
                    and getattr(owner, "source_identity", None) == source_identity
                )
            ):
                # A terminal source must already have retired its authority-finalizer
                # entry; presence in this registry means the old owner is retained.
                raise RuntimeError("cua_source_owner_retained")
        return CuaFactoryReservation(
            authority=authority,
            coordinator=coordinator,
            process_instance=process_instance,
            authority_epoch=authority_epoch,
            close_generation=int(status.get("close_generation") or 0),
        )

    def validate_existing_owner(self, owner: Any) -> bool:
        """Validate an already accepted owner while allowing retirement to close it."""
        if owner is None or getattr(owner, "parent", None) is None:
            return False
        parent = owner.parent
        service = getattr(parent, "_service", None)
        managed_parent = getattr(parent, "mode", None) == "managed_turn"
        # The parent mode is the authenticated admission classification.  A managed
        # parent whose service edge disappeared is still managed and must fail closed;
        # it may not be reinterpreted as a standalone owner during retry/retirement.
        if managed_parent and service is None:
            return False
        managed = managed_parent
        if (
            getattr(parent, "profile_authority", None) is not self.authority
            or getattr(owner, "family", None) != self.family
            or not any(candidate is parent for candidate in self.authority.external_process_finalizers())
            or getattr(parent, "_cua_receipt", None) is not getattr(owner, "receipt", None)
        ):
            return False
        if managed:
            # The parent carries the authenticated I04 source identity.  The CUA
            # driver identity is a child producer identity and must not be compared
            # to that admission string.  The service/binding object edges are the
            # ownership proof.
            if (
                getattr(service, "external_parent", None) is not parent
                or getattr(service, "binding", None) is not getattr(parent, "turn_binding", None)
                or getattr(service, "physical", None) is not getattr(parent, "_physical", None)
                or getattr(getattr(parent, "_physical", None), "service", None) is not service
            ):
                return False
        elif getattr(parent, "source_identity", None) != getattr(owner, "source_identity", None):
            return False
        coordinator = self._coordinator()
        status = coordinator.status()
        if (
            status.get("process_instance") != getattr(self.authority, "instance_id", None)
            or status.get("authority_epoch") != getattr(self.authority, "epoch", None)
        ):
            return False
        if status.get("state") == "open":
            return int(status.get("close_generation") or 0) == int(
                getattr(owner.receipt, "close_generation", 0) or 0
            )
        # A source accepted while open may finish during profile retirement.  A new
        # producer is still blocked by reserve_before_producer above.
        return status.get("state") == "closing" and int(
            getattr(owner.receipt, "close_generation", 0) or 0
        ) == 0


def register_cua_source_factory(runner: Any, authority: Any) -> CuaStdioSourceFactory:
    """Install the profile-scoped CUA factory before any producer discovery."""
    factory = CuaStdioSourceFactory(authority)
    register = getattr(authority, "register_cua_source_factory", None)
    if not callable(register) or register(factory) is not factory:
        raise RuntimeError("cua_factory_registry_missing")
    return factory


def _text(value: Any, code: str) -> str:
    if type(value) is not str or not value:
        from gateway.runtime_service_operations import ExternalProcessOperationError
        raise ExternalProcessOperationError(code)
    return value


def _identity_digest(values: dict[str, Any]) -> str:
    encoded = json.dumps(values, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


@dataclass
class CuaStdioReceipt:
    """One exact host stdio attempt below a canonical external parent."""

    owner: CuaStdioOwner
    state: str = "reserved"
    generation: int = 0
    close_generation: int = 0
    source_factory: Any = None
    task: Any = None
    process: Any = None
    pid: int | None = None
    process_group_id: int | None = None
    # The canonical ServiceProcessReceipt for managed CUA.  The CUA receipt is
    # only the SDK/context witness; durable result and delivery ownership stay
    # on this existing physical receipt.
    physical: Any = None
    physical_process: Any = None
    physical_reader: Any = None
    scope: str | None = None
    scope_identity: str | None = None
    streams: tuple[Any, ...] | None = None
    read_stream: Any = None
    write_stream: Any = None
    # Exact SDK objects captured by the source-owned stdio context.  These are
    # observations, never caller-supplied capability tokens.
    transport_context: Any = field(default=None, repr=False)
    reader_task: Any = field(default=None, repr=False)
    process_wait_task: Any = field(default=None, repr=False)
    process_wait_done: bool = field(default=False, repr=False)
    process_wait_error: BaseException | None = field(default=None, repr=False)
    process_wait_observer_registered: bool = field(default=False, repr=False)
    session: Any = None
    session_entered: bool = False
    session_closed: bool = False
    transport_entered: bool = False
    transport_closed: bool = False
    process_exited: bool = False
    reader_eof: bool = False
    reader_joined: bool = False
    returncode: Any = None
    body_error: BaseException | None = field(default=None, repr=False)
    close_error: BaseException | None = field(default=None, repr=False)
    unknown_reason: str | None = None
    prior_process_identity: str | None = None

    def _mark_unknown(self, reason: str) -> None:
        if self.state == "closed":
            return
        if self.state != "unknown":
            self.state = "unknown"
            self.unknown_reason = reason

    def _propagate_unknown(self, reason: str) -> None:
        self._mark_unknown(reason)
        try:
            self.owner.mark_unknown(reason)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.owner.parent.state = "unknown"
            self.owner.parent.coordinator.mark_unknown(self.owner.parent.root_handle, reason)

    def _upgrade_transport_unknown(self, reason: str) -> None:
        """Promote a stale-binding UNKNOWN only after the real child has closed."""
        if reason not in _CUA_PHYSICAL_RETRY_REASONS or self.unknown_reason != "cua_current_binding_mismatch":
            return
        self.unknown_reason = reason
        physical = self.physical
        if physical is not None and getattr(physical, "unknown_reason", None) == "cua_current_binding_mismatch":
            physical.unknown_reason = reason

    def bind_task(self, task: Any) -> None:
        if task is None or (self.task is not None and self.task is not task):
            self._mark_unknown("cua_task_identity_mismatch")
            raise RuntimeError("cua_task_identity_mismatch")
        self.task = task

    def bind_process(self, process: Any, streams: Any) -> None:
        """Bind the exact SDK child and stream pair before protocol initialization."""
        if self.state not in {"reserved", "unknown"} or process is None:
            self._mark_unknown("cua_process_identity_mismatch")
            raise RuntimeError("cua_process_identity_mismatch")
        if self.state == "unknown":
            raise RuntimeError("cua_retry_not_prepared")
        if not isinstance(streams, (tuple, list)) or len(streams) != 2:
            self._mark_unknown("cua_stream_identity_missing")
            raise RuntimeError("cua_stream_identity_missing")
        pid = getattr(process, "pid", None)
        if type(pid) is not int or pid <= 0:
            self._mark_unknown("cua_process_identity_missing")
            raise RuntimeError("cua_process_identity_missing")
        if os.name == "posix":
            try:
                pgid = os.getpgid(pid)
            except (AttributeError, OSError, ProcessLookupError) as exc:
                self._mark_unknown("cua_process_scope_unknown")
                raise RuntimeError("cua_process_scope_unknown") from exc
            if type(pgid) is not int or pgid <= 0:
                self._mark_unknown("cua_process_scope_unknown")
                raise RuntimeError("cua_process_scope_unknown")
            scope, scope_identity = "posix_process_group", f"posix_process_group:{pid}:{pgid}"
        elif os.name == "nt":
            native = getattr(process, "windows_job", None) or getattr(process, "native_scope", None)
            identity = getattr(native, "identity", None) if native is not None else None
            if type(identity) is not str or not identity.startswith("windows_job:"):
                self._mark_unknown("cua_windows_job_unavailable")
                raise RuntimeError("cua_windows_job_unavailable")
            scope, scope_identity, pgid = "windows_job", identity, None
        else:
            self._mark_unknown("cua_process_scope_unknown")
            raise RuntimeError("cua_process_scope_unknown")
        self.process, self.pid = process, pid
        self.process_group_id = pgid
        self.scope, self.scope_identity = scope, scope_identity
        self.streams = tuple(streams)
        self.read_stream, self.write_stream = streams[0], streams[1]
        # The long-lived lifecycle task is the SDK context owner.  It is retained
        # separately from the process so a copied process/stream pair cannot pass
        # the task/session identity fence.
        self.reader_task = self.task
        self.transport_entered = True
        self.state = "running"

    def bind_session(self, session: Any, task: Any) -> None:
        if session is None or task is None or task is not self.task:
            self._mark_unknown("cua_session_task_mismatch")
            raise RuntimeError("cua_session_task_mismatch")
        if self.session is not None and self.session is not session:
            self._mark_unknown("cua_session_replaced")
            raise RuntimeError("cua_session_replaced")
        self.session = session
        self.session_entered = True

    def close_session(self, body_error: BaseException | None, close_error: BaseException | None) -> None:
        if body_error is not None:
            self.body_error = body_error
        if close_error is not None:
            self.close_error = close_error
        self.session_closed = close_error is None
        if body_error is not None or close_error is not None:
            self._propagate_unknown("cua_session_close_unknown")

    def _record_process_wait_terminal(self, wait_task: Any) -> None:
        """Publish one terminal result for the retained wait task, exactly once."""
        if wait_task is not self.process_wait_task or self.process_wait_done:
            return
        try:
            self.returncode = wait_task.result()
        except BaseException as exc:
            logger.exception("CUA retained process wait failed; retaining UNKNOWN custody")
            self.process_wait_error = exc
            self.close_error = exc
            self._propagate_unknown("cua_process_exit_unknown")
            self._upgrade_transport_unknown("cua_process_exit_unknown")
            self.process_wait_done = True
            return
        self.process_exited = True
        self.transport_closed = True
        if self.body_error is not None:
            self.process_wait_done = True
            return
        # The stdio context's successful exit is the SDK-owned reader join boundary.
        self.reader_eof = True
        self.reader_joined = True
        physical = self.physical
        reader = self.physical_reader
        if physical is not None:
            try:
                if reader is None:
                    raise RuntimeError("cua_physical_reader_missing")
                reader.mark_joined()
                physical.reader_eof("stdout", reader)
                physical.reader_joined("stdout", reader)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                self.process_wait_error = exc
                self.close_error = exc
                self._propagate_unknown("cua_physical_readback_unknown")
                self._upgrade_transport_unknown("cua_physical_readback_unknown")
                self.process_wait_done = True
                return
        if self.state == "running":
            self.state = "closed_candidate"
        self.process_wait_done = True

    def _observe_process_wait(self, wait_task: Any) -> None:
        if self.process_wait_observer_registered:
            return
        wait_task.add_done_callback(self._record_process_wait_terminal)
        self.process_wait_observer_registered = True

    async def finish_transport(self, body_error: BaseException | None) -> None:
        if body_error is not None:
            self.body_error = body_error
            self._propagate_unknown("cua_transport_close_unknown")
            self._upgrade_transport_unknown("cua_transport_close_unknown")
        wait_task = None
        try:
            wait = getattr(self.process, "wait", None)
            if not callable(wait):
                raise RuntimeError("cua_process_wait_unavailable")
            wait_task = self.process_wait_task
            if wait_task is None:
                wait_task = asyncio.ensure_future(wait())
                self.process_wait_task = wait_task
            self._observe_process_wait(wait_task)
            await asyncio.wait_for(asyncio.shield(wait_task), timeout=5.0)
            self._record_process_wait_terminal(wait_task)
        except BaseException as exc:
            if wait_task is not None and wait_task.done():
                self._record_process_wait_terminal(wait_task)
            logger.exception("CUA transport process exit could not be observed")
            self.close_error = exc
            self._propagate_unknown("cua_process_exit_unknown")
            self._upgrade_transport_unknown("cua_process_exit_unknown")
            return

    def prepare_retry(self) -> bool:
        """Reconcile and reuse this same runtime handle for one exact transport retry."""
        parent = self.owner.parent
        factory = self.source_factory
        if factory is not None and not factory.validate_existing_owner(self.owner):
            return False
        if (
            self.state != "unknown"
            or self.unknown_reason not in _CUA_PHYSICAL_RETRY_REASONS
            or not self.transport_closed
            or not self.process_exited
            or not self.process_wait_done
            or self.process_wait_error is not None
            or getattr(parent, "_cua_receipt", None) is not self
            or parent.state != "unknown"
        ):
            return False
        coordinator = getattr(parent, "coordinator", None)
        reconcile = getattr(coordinator, "reconcile_unknown", None)
        if not callable(reconcile):
            return False
        service = getattr(parent, "_service", None)
        if service is not None:
            if not _prepare_cua_physical_retry(parent):
                return False
        else:
            restored = reconcile(
                parent.root_handle,
                owner_id=parent.root_handle.owner_id,
                profile_id=parent.profile_id,
                process_instance=parent.process_instance,
                authority_epoch=parent.authority_epoch,
            )
            if restored is not None:
                return False
        self.prior_process_identity = self.scope_identity
        self.generation += 1
        self.state = "reserved"
        self.task = self.process = self.session = None
        self.pid = self.process_group_id = None
        self.scope = self.scope_identity = None
        self.streams = None
        self.read_stream = self.write_stream = None
        self.physical_process = self.physical_reader = None
        self.session_entered = self.session_closed = False
        self.transport_entered = self.transport_closed = False
        self.process_exited = self.reader_eof = self.reader_joined = False
        self.returncode = None
        self.body_error = self.close_error = self.unknown_reason = None
        self.process_wait_task = None
        self.process_wait_done = False
        self.process_wait_error = None
        self.process_wait_observer_registered = False
        parent.state = "request_reserved"
        return True

    def owner_is_current(self) -> bool:
        parent = self.owner.parent
        if parent.state not in {"request_reserved", "physical_bound", "unknown"}:
            return False
        authority = getattr(parent, "profile_authority", None)
        if authority is not None:
            current = (
                getattr(authority, "profile_id", None) == parent.profile_id
                and getattr(authority, "epoch", None) == parent.authority_epoch
                and getattr(authority, "instance_id", None) == parent.process_instance
                and getattr(authority, "_preservation_coordinator", lambda: None)() is parent.coordinator
            )
            factory = self.source_factory
            return current and (factory is None or factory.validate_existing_owner(self.owner))
        return getattr(parent.coordinator, "_parent", lambda _handle: None)(parent.root_handle) is not None

    def close_after_shutdown(self) -> bool:
        if self.state == "closed":
            return True
        if (
            self.state not in {"running", "closed_candidate"}
            or self.task is None
            or not getattr(self.task, "done", lambda: False)()
            or self.reader_task is not self.task
            or not getattr(self.reader_task, "done", lambda: False)()
            or self.process is None
            or not self.process_exited
            or not self.transport_entered
            or not self.transport_closed
            or not self.session_closed
            or not self.reader_eof
            or not self.reader_joined
            or self.body_error is not None
            or self.close_error is not None
            or getattr(self.owner.parent, "_cua_receipt", None) is not self
            or (
                self.physical is not None
                and (
                    getattr(self.owner.parent, "_physical", None) is not self.physical
                    or getattr(getattr(self.owner.parent, "_service", None), "physical", None) is not self.physical
                    or getattr(self.physical, "state", None) in {"unknown", "closed", "released"}
                )
            )
            or not self.owner_is_current()
        ):
            self._propagate_unknown("cua_terminal_readback_unknown")
            return False
        self.state = "closed"
        physical = self.physical
        if physical is not None:
            try:
                physical.bind_source_receipt(self)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                self._propagate_unknown("cua_source_receipt_unknown")
                return False
        return True

    def readback(self) -> dict[str, Any] | None:
        if self.state != "closed":
            return None
        parent = self.owner.parent
        return {
            "family": self.owner.family,
            "source_identity": self.owner.source_identity,
            "operation_id": parent.operation_id,
            "profile_id": parent.profile_id,
            "authority_epoch": parent.authority_epoch,
            "process_instance": parent.process_instance,
            "physical_owner_epoch": parent.physical_owner_epoch,
            "session_id": self.owner.session_id,
            "permission_mode": self.owner.permission_mode,
            "display_identity": self.owner.display_identity,
            "scope": self.scope,
            "scope_identity": self.scope_identity or self.prior_process_identity,
            "pid": self.pid,
            "generation": self.generation,
            "close_generation": self.close_generation,
            "returncode": self.returncode,
            "identity_digest": self.owner.identity_digest,
        }


@dataclass
class CuaStdioOwner:
    """Typed CUA source parent attached to the existing authority finalizer registry."""

    parent: Any
    family: str
    source_identity: str
    session_id: str
    permission_mode: str
    display_identity: str = ""
    source_factory: CuaStdioSourceFactory | None = None
    receipt: CuaStdioReceipt = field(init=False)
    identity_digest: str = field(init=False)
    request_unknown_reason: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        self.family = _text(self.family, "cua_family_missing")
        self.source_identity = _text(self.source_identity, "cua_source_identity_missing")
        self.session_id = _text(self.session_id, "cua_session_identity_missing")
        self.permission_mode = _text(self.permission_mode, "cua_permission_mode_missing")
        self.display_identity = str(self.display_identity or "")
        self.identity_digest = self._digest()
        self.receipt = CuaStdioReceipt(self, source_factory=self.source_factory)

    def _digest(self) -> str:
        parent = self.parent
        return _identity_digest({
            "family": self.family,
            "source_identity": self.source_identity,
            "session_id": self.session_id,
            "permission_mode": self.permission_mode,
            "display_identity": self.display_identity,
            "profile_id": getattr(parent, "profile_id", None),
            "authority_epoch": getattr(parent, "authority_epoch", None),
            "process_instance": getattr(parent, "process_instance", None),
            "physical_owner_epoch": getattr(parent, "physical_owner_epoch", None),
        })

    def bind_display(self, display_identity: Any) -> None:
        display = str(display_identity or "")
        if self.receipt.process is not None or self.parent.state not in {"request_reserved", "unknown"}:
            self.mark_unknown("cua_display_bound_after_effect")
            raise RuntimeError("cua_display_bound_after_effect")
        if self.display_identity and display and self.display_identity != display:
            self.mark_unknown("cua_display_identity_replaced")
            raise RuntimeError("cua_display_identity_replaced")
        self.display_identity = display
        self.identity_digest = self._digest()

    def bind_task(self, task: Any) -> None:
        self.receipt.bind_task(task)

    def _retain_request_binding_unknown(self, binding: Any, request: Any, reason: str) -> None:
        """Keep one failed request edge until its exact binder can be retried."""
        parent = self.parent
        parent.turn_binding = binding
        parent._request = request
        parent.state = "unknown"
        self.request_unknown_reason = reason
        coordinator = getattr(parent, "coordinator", None)
        marker = getattr(coordinator, "mark_unknown", None)
        if not callable(marker):
            raise RuntimeError("cua_request_unknown_coordinator_missing")
        marker(parent.root_handle, reason)
        marker(request, reason)

    def _retry_request_binding(self, binding: Any) -> bool:
        """Rebind the retained request without allocating a replacement edge."""
        if self.request_unknown_reason is None:
            return True
        parent = self.parent
        service = getattr(parent, "_service", None)
        request = (getattr(service, "requests", {}) or {}).get(getattr(binding, "admission_id", None))
        if (
            service is None
            or request is None
            or request is not getattr(parent, "_request", None)
            or getattr(parent, "turn_binding", None) is not binding
            or getattr(service, "external_parent", None) is not parent
            or getattr(service, "binding", None) is not binding
        ):
            return False
        coordinator = getattr(parent, "coordinator", None)
        records = getattr(coordinator, "_records", None)
        lock = getattr(coordinator, "_lock", None)
        if not isinstance(records, dict) or lock is None:
            return False
        handles = (parent.root_handle, request)
        with lock:
            if any(
                records.get(getattr(getattr(handle, "lease", None), "lease_id", None)) is None
                or getattr(records.get(getattr(getattr(handle, "lease", None), "lease_id", None)), "handle", None) is not handle
                or getattr(records.get(getattr(getattr(handle, "lease", None), "lease_id", None)), "state", None) != "unknown"
                for handle in handles
            ):
                return False
        reconcile = getattr(coordinator, "reconcile_unknown", None)
        if not callable(reconcile):
            return False
        reconciled = []
        for handle in handles:
            try:
                restored = reconcile(
                    handle, owner_id=handle.owner_id, profile_id=parent.profile_id,
                    process_instance=parent.process_instance, authority_epoch=parent.authority_epoch,
                )
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                restored = handle
            if restored is not None:
                for completed in reconciled:
                    coordinator.mark_unknown(completed, "cua_request_retry_failed")
                coordinator.mark_unknown(handle, "cua_request_retry_failed")
                return False
            reconciled.append(handle)
        binder = getattr(binding.authority, "_preservation_bind_sidecar", None)
        if not callable(binder):
            return False
        try:
            binder(binding.admission_id, binding.parent_handle, service, request)
        except BaseException:
            logger.exception("CUA request binding retry failed; retaining UNKNOWN custody")
            self._retain_request_binding_unknown(binding, request, self.request_unknown_reason)
            return False
        self.request_unknown_reason = None
        parent.state = "request_reserved"
        return True

    def prepare_current_binding(self, binding: Any) -> bool:
        """Attach this cached owner to the exact current I04 request before an action."""
        parent = self.parent
        managed_parent = getattr(parent, "mode", None) == "managed_turn"
        if binding is None:
            # A managed parent may never fall back to a context-free cached
            # request; standalone maintenance remains an explicit capability.
            return (
                not managed_parent
                and getattr(parent, "_service", None) is None
                and self.receipt.state != "unknown"
            )
        if self.request_unknown_reason is not None and not self._retry_request_binding(binding):
            return False
        if self.receipt.state == "unknown" and not self.prepare_retry():
            return False
        service = getattr(parent, "_service", None)
        if managed_parent and service is None:
            self.mark_unknown("cua_managed_service_missing")
            return False
        if service is None or getattr(parent, "profile_authority", None) is not getattr(binding, "authority", None):
            self.mark_unknown("cua_current_binding_mismatch")
            return False
        try:
            if getattr(service, "external_parent", None) is not parent:
                raise RuntimeError("cua_service_parent_mismatch")
            current = getattr(service, "requests", {}).get(binding.admission_id)
            if current is getattr(parent, "_request", None) and getattr(parent, "turn_binding", None) is binding:
                return True
            if getattr(service, "requests", None):
                raise RuntimeError("cua_previous_request_still_active")
            # A cached CUA service keeps the same I16 root and physical receipt, but
            # every admitted Gateway turn gets a fresh authenticated request edge.
            # The existing ServiceLease is the only request allocator; no CUA map or
            # copied request handle is allowed here.
            from gateway.runtime_service_operations import begin_cua_request
            request = begin_cua_request(service, binding, parent)
            parent.turn_binding = binding
            parent._request = request
            binder = getattr(binding.authority, "_preservation_bind_sidecar", None)
            if not callable(binder):
                raise RuntimeError("cua_sidecar_binder_missing")
            binder(binding.admission_id, binding.parent_handle, service, request)
            self.request_unknown_reason = None
            parent.state = "request_reserved"
            return True
        except BaseException:
            logger.exception("CUA cached owner binding could not be prepared")
            if "request" in locals() and request is not None:
                self._retain_request_binding_unknown(binding, request, "cua_request_binding_unknown")
            else:
                self.mark_unknown("cua_current_binding_mismatch")
            return False

    def observe_session(self, context: Any) -> Any:
        from gateway.runtime_sidecar_cua_context import ObservedCuaContext
        return ObservedCuaContext(context, self)

    def mark_unknown(self, reason: str) -> None:
        self.receipt._mark_unknown(reason)
        parent = self.parent
        try:
            from gateway.runtime_service_operations import mark_unknown
            mark_unknown(parent, reason)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            parent.state = "unknown"
            parent.coordinator.mark_unknown(parent.root_handle, reason)
        authority = getattr(parent, "profile_authority", None)
        binding = getattr(parent, "turn_binding", None)
        marker = getattr(authority, "_preservation_mark_unknown", None)
        if binding is not None and callable(marker):
            marker(binding.admission_id, reason)

    def prepare_retry(self) -> bool:
        return self.receipt.prepare_retry()

    def prepare_transport_retry(self) -> bool:
        """Prepare the same owner after an actual lifecycle context has ended."""
        if self.receipt.state in {"running", "closed_candidate", "closed"}:
            self.receipt._propagate_unknown("cua_transport_close_unknown")
            self.receipt._upgrade_transport_unknown("cua_transport_close_unknown")
        return self.receipt.prepare_retry()

    def release_before_effect(self) -> bool:
        if self.receipt.process is not None or self.receipt.transport_entered:
            self.mark_unknown("cua_partial_launch")
            return False
        mode = getattr(self.parent, "mode", None)
        if mode == "managed_turn":
            return _release_managed_cua_before_effect(self)
        if mode == "standalone_maintenance":
            return _release_standalone_cua_before_effect(self)
        return _release_legacy_cua_before_effect(self)

    def finish(self) -> bool:
        if not self.receipt.close_after_shutdown():
            return False
        if close_cua_after_finalizers(self.parent) is not True:
            return False
        _finish_standalone_context(self.parent)
        return True


def _managed_cua_release_parts(owner: CuaStdioOwner) -> tuple[Any, Any, Any, Any] | None:
    parent = owner.parent
    service = getattr(parent, "_service", None)
    request = getattr(parent, "_request", None)
    binding = getattr(parent, "turn_binding", None)
    if (
        service is None
        or request is None
        or binding is None
        or getattr(parent, "_cua_receipt", None) is not owner.receipt
        or getattr(service, "external_parent", None) is not parent
        or getattr(service, "binding", None) is not binding
        or getattr(service, "requests", {}).get(getattr(binding, "admission_id", None)) is not request
    ):
        return None
    physical = getattr(service, "physical", None)
    if (
        physical is None
        or getattr(parent, "_physical", None) is not physical
        or getattr(physical, "service", None) is not service
        or (
            getattr(owner.receipt, "physical", None) is not None
            and getattr(owner.receipt, "physical", None) is not physical
        )
    ):
        return None
    return service, binding, request, physical


def _release_managed_cua_before_effect(owner: CuaStdioOwner) -> bool:
    parts = _managed_cua_release_parts(owner)
    if parts is None:
        owner.mark_unknown("cua_managed_owner_incomplete")
        return False
    service, binding, request, physical = parts
    try:
        from gateway.runtime_service_operations import release_cua_request
        release_cua_request(service, binding, request, owner.parent)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        owner.mark_unknown("cua_release_failed")
        return False
    if physical.state != "reserved" or physical.release_before_effect() is not True:
        owner.mark_unknown("cua_physical_release_failed")
        return False
    # Keep the exact released receipt attached until every root fence succeeds.
    service.physical = physical
    owner.parent._physical = physical
    try:
        _forget_cua_request(service, binding, request)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return _retain_cua_late_fence(
            owner.parent, service, physical, "cua_sidecar_retire_failed",
        )
    return _finish_managed_cua_release(owner, service, physical)


def _finish_managed_cua_release(owner: CuaStdioOwner, service: Any, physical: Any) -> bool:
    parent = owner.parent
    parent.state = "terminal"
    authority = getattr(parent, "profile_authority", None)
    forget = getattr(authority, "forget_external_process_finalizer", None)
    if not callable(forget):
        return _retain_cua_late_fence(parent, service, physical, "cua_finalizer_retire_failed")
    if not forget(parent.operation_id, parent):
        _restore_cua_finalizer(parent)
        return _retain_cua_late_fence(parent, service, physical, "cua_finalizer_retire_failed")
    if not _mark_cua_root_terminal(parent):
        parent.state = "unknown"
        _restore_cua_finalizer(parent)
        return _retain_cua_late_fence(parent, service, physical, "cua_release_failed")
    parent._request = None
    parent.turn_binding = None
    if getattr(service, "physical", None) is physical:
        service.physical = None
    parent._physical = None
    parent._cua_receipt = None
    _finish_standalone_context(parent)
    return True


def _release_standalone_cua_before_effect(owner: CuaStdioOwner) -> bool:
    parent = owner.parent
    if not _standalone_cua_edges_exact(owner) or owner.receipt.state != "reserved":
        owner.mark_unknown("cua_standalone_owner_incomplete")
        return False
    service = parent._service
    physical = parent._physical
    if physical.release_before_effect() is not True:
        owner.mark_unknown("cua_physical_release_failed")
        return False
    service.physical = None
    if parent.coordinator.release_unaccepted(parent._request) is not None:
        owner.mark_unknown("cua_request_release_failed")
        return False
    service.requests.pop(parent.operation_id, None)
    if parent.coordinator.release_unaccepted(parent.root_handle) is not None:
        owner.mark_unknown("cua_release_failed")
        return False
    authority = getattr(parent, "profile_authority", None)
    forget = getattr(authority, "forget_external_process_finalizer", None)
    parent.state = "terminal"
    if callable(forget) and not forget(parent.operation_id, parent):
        parent.state = "unknown"
        return False
    parent._request = None
    parent.turn_binding = None
    parent._physical = None
    parent._cua_receipt = None
    _finish_standalone_context(parent)
    return True


def _release_legacy_cua_before_effect(owner: CuaStdioOwner) -> bool:
    parent = owner.parent
    if parent.coordinator.release_unaccepted(parent.root_handle) is not None:
        owner.mark_unknown("cua_release_failed")
        return False
    parent.state = "terminal"
    if not _mark_cua_root_terminal(parent):
        parent.state = "unknown"
        return False
    authority = getattr(parent, "profile_authority", None)
    forget = getattr(authority, "forget_external_process_finalizer", None)
    if callable(forget) and not forget(parent.operation_id, parent):
        parent.state = "unknown"
        return False
    parent._cua_receipt = None
    _finish_standalone_context(parent)
    return True


def _finish_standalone_context(parent: Any) -> None:
    context = getattr(parent, "_maintenance_context", None)
    if context is None:
        return
    try:
        context.__exit__(None, None, None)
    finally:
        parent._maintenance_context = None
        if getattr(parent, "_cua_owned_profile_db", False):
            from hermes_state_registry import release_or_close
            release_or_close(getattr(parent, "_profile_db", None))
            parent._cua_owned_profile_db = False


def _restore_cua_finalizer(parent: Any) -> bool:
    """Re-register the same authority-owned parent after a root commit refusal."""
    authority = getattr(parent, "profile_authority", None)
    finals = getattr(authority, "external_process_finalizers", None)
    if callable(finals) and any(candidate is parent for candidate in finals()):
        return True
    register = getattr(authority, "register_external_process_finalizer", None)
    if not callable(register):
        return False
    return register(parent.operation_id, parent) is parent


def _retain_cua_late_fence(
    parent: Any, service: Any, physical: Any, reason: str,
) -> bool:
    """Retain a failed post-close fence without reopening the physical receipt.

    Once the canonical physical receipt is ``closed`` or ``released`` its state is
    an immutable native fact.  A late registry/request/root failure may leave the
    same object tuple reachable for a retry, but must never call physical
    ``mark_unknown``/``reconcile_unknown`` or create a replacement root.
    """
    if physical is None or getattr(physical, "state", None) not in {"closed", "released"}:
        return _retain_cua_cleanup_failure(parent, service, physical, reason)
    if (
        service is None
        or getattr(service, "external_parent", None) is not parent
        or getattr(parent, "_physical", None) is not physical
        or getattr(physical, "service", None) is not service
    ):
        # A missing or foreign edge cannot be repaired by late cleanup.  Keep the
        # exact parent state and surface a refusal without copying any object.
        parent.state = "unknown"
        raise RuntimeError("cua_late_owner_mismatch")
    service.physical = physical
    parent._physical = physical
    record = None
    records = getattr(parent.coordinator, "_records", None)
    lease_id = getattr(getattr(parent.root_handle, "lease", None), "lease_id", None)
    lock = getattr(parent.coordinator, "_lock", None)
    if isinstance(records, dict) and lease_id is not None and lock is not None:
        with lock:
            record = records.get(lease_id)
    if getattr(record, "state", None) == "terminal":
        parent.state = "terminal"
        return False
    parent.state = "unknown"
    marker = getattr(parent.coordinator, "mark_unknown", None)
    if not callable(marker):
        raise RuntimeError("cua_unknown_coordinator_missing")
    result = marker(parent.root_handle, reason)
    if result is not None:
        raise RuntimeError(
            f"cua_unknown_retention_failed:{getattr(result, 'code', 'unknown')}"
        )
    return False


def _retain_cua_cleanup_failure(
    parent: Any, service: Any, physical: Any, reason: str,
) -> bool:
    """Keep the exact CUA tuple reachable when a later cleanup fence refuses.

    The physical object may already have completed its native close.  Reattaching that
    same object to the same service/parent keeps the authority's finalizer and retry
    boundary observable; no replacement receipt or coordinator record is created.
    """
    service_physical = getattr(service, "physical", None) if service is not None else None
    if service_physical is not None and physical is not None and service_physical is not physical:
        # A mismatch is a foreign/replaced owner, not a gap that cleanup may repair.
        physical = getattr(parent, "_physical", None)
    if physical is not None:
        if service is not None:
            if service_physical is None:
                service.physical = physical
            elif service_physical is not physical:
                physical = getattr(parent, "_physical", None)
        if physical is not None:
            parent._physical = physical
    parent.state = "unknown"
    receipt = getattr(parent, "_cua_receipt", None)
    if receipt is not None:
        receipt._mark_unknown(reason)
    coordinator = getattr(parent, "coordinator", None)
    mark_unknown = getattr(coordinator, "mark_unknown", None)
    if not callable(mark_unknown):
        raise RuntimeError("cua_unknown_coordinator_missing")
    result = mark_unknown(parent.root_handle, reason)
    if result is not None:
        raise RuntimeError(
            f"cua_unknown_retention_failed:{getattr(result, 'code', 'unknown')}"
        )
    return False


_CUA_PHYSICAL_RETRY_REASONS = frozenset({
    "cua_transport_close_unknown",
    "cua_process_exit_unknown",
    "cua_session_close_unknown",
    "cua_physical_readback_unknown",
})


def _prepare_cua_physical_retry(parent: Any) -> bool:
    """Reopen only the retained CUA physical handle after a complete close readback."""
    service = getattr(parent, "_service", None)
    physical = getattr(service, "physical", None) if service is not None else None
    binding = getattr(parent, "turn_binding", None)
    if (
        service is None
        or physical is None
        or binding is None
        or getattr(service, "external_parent", None) is not parent
        or getattr(service, "binding", None) is not binding
        or getattr(parent, "_physical", None) is not physical
        or getattr(physical, "service", None) is not service
        or getattr(physical, "state", None) != "unknown"
        or getattr(physical, "unknown_reason", None) not in _CUA_PHYSICAL_RETRY_REASONS
        or getattr(service, "unknown", False) is not True
        or getattr(service, "durable_requests", None)
        or getattr(service, "delivery_unknown_reasons", None)
        or getattr(service, "delivery_terminalized", None)
        or getattr(physical, "durable_proofs", None)
        or getattr(physical, "delivery_requests", None)
    ):
        return False
    process = getattr(physical, "process", None)
    if process is None or getattr(process, "poll", lambda: None)() is None:
        return False
    required = {"stdout"}
    if (
        getattr(physical, "eof_streams", set()) != required
        or getattr(physical, "joined_streams", set()) != required
        or set(getattr(physical, "readers", {}) or {}) != required
        or getattr(physical, "reader_error", None) is not None
    ):
        return False
    reader = (getattr(physical, "readers", {}) or {}).get("stdout")
    if reader is None or getattr(reader, "is_alive", lambda: True)() is not False:
        return False
    scope = getattr(physical, "contained_scope_provider", None)
    observe_empty = getattr(scope, "observe_empty", None)
    if not callable(observe_empty) or observe_empty() is not True:
        return False
    coordinator = getattr(parent, "coordinator", None)
    lock = getattr(coordinator, "_lock", None)
    records = getattr(coordinator, "_records", None)
    handles = (getattr(parent, "root_handle", None), getattr(physical, "handle", None))
    if lock is None or not isinstance(records, dict) or any(handle is None for handle in handles):
        return False
    with lock:
        for handle in handles:
            lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
            record = records.get(lease_id)
            if (
                record is None
                or getattr(record, "handle", None) is not handle
                or getattr(record, "state", None) != "unknown"
                or getattr(handle, "profile_id", None) != getattr(binding, "profile_id", None)
                or getattr(handle, "process_instance", None) != getattr(binding, "process_instance", None)
                or getattr(handle, "authority_epoch", None) != getattr(binding, "authority_epoch", None)
            ):
                return False
    restored_root = coordinator.reconcile_unknown(
        parent.root_handle, owner_id=parent.root_handle.owner_id,
        profile_id=binding.profile_id, process_instance=binding.process_instance,
        authority_epoch=binding.authority_epoch,
    )
    if restored_root is not None:
        return False
    restored_physical = coordinator.reconcile_unknown(
        physical.handle, owner_id=physical.handle.owner_id,
        profile_id=binding.profile_id, process_instance=binding.process_instance,
        authority_epoch=binding.authority_epoch,
    )
    if restored_physical is not None:
        coordinator.mark_unknown(parent.root_handle, "cua_physical_retry_failed")
        return False
    physical.state = "reserved"
    physical.unknown_reason = None
    physical.transaction = None
    physical.process = None
    physical.pid = None
    physical.process_started_at = None
    physical.scope = None
    physical.scope_identity = None
    physical.process_group_id = None
    physical.stdout_eof = physical.stderr_eof = False
    physical.stdout_joined = physical.stderr_joined = False
    physical.reader_error = None
    physical.readers.clear()
    physical.reader_start_states.clear()
    physical.eof_streams.clear()
    physical.joined_streams.clear()
    physical.native_close_committed = False
    service.unknown = False
    return True


def _standalone_cua_edges_exact(owner: CuaStdioOwner) -> bool:
    parent = owner.parent
    service = getattr(parent, "_service", None)
    request = getattr(parent, "_request", None)
    physical = getattr(parent, "_physical", None)
    return (
        service is not None
        and getattr(parent, "mode", None) == "standalone_maintenance"
        and getattr(service, "binding", None) is None
        and getattr(service, "external_parent", None) is parent
        and getattr(service, "requests", {}).get(getattr(parent, "operation_id", None)) is request
        and request is not None
        and getattr(parent, "_request", None) is request
        and getattr(service, "physical", None) is physical
        and physical is not None
        and getattr(physical, "service", None) is service
        and (
            getattr(owner.receipt, "physical", None) is None
            or getattr(owner.receipt, "physical", None) is physical
        )
        and getattr(physical, "state", None) == "reserved"
    )


def _cua_physical_for_owner(owner: CuaStdioOwner) -> Any:
    parent = owner.parent
    service = getattr(parent, "_service", None)
    if service is None:
        return None
    mode = getattr(parent, "mode", None)
    if mode == "managed_turn":
        return getattr(service, "physical", None)
    if mode == "standalone_maintenance" and _standalone_cua_edges_exact(owner):
        return getattr(service, "physical", None)
    if mode == "standalone_maintenance":
        owner.mark_unknown("cua_standalone_owner_incomplete")
        raise RuntimeError("cua_standalone_owner_incomplete")
    owner.mark_unknown("cua_managed_parent_mode_missing")
    raise RuntimeError("cua_managed_parent_mode_missing")


def _bind_cua_native_scope(
    owner: CuaStdioOwner, physical: Any, process: Any, adapter: Any, pid: int,
) -> None:
    if os.name == "nt":
        transaction = getattr(process, "windows_job", None) or getattr(process, "native_scope", None)
        identity = getattr(transaction, "identity", None) if transaction is not None else None
        started_at = getattr(transaction, "host_start_time", None) if transaction is not None else None
        if (
            transaction is None
            or type(identity) is not str
            or not identity.startswith("windows_job:")
            or type(started_at) is not int
            or started_at <= 0
            or getattr(transaction, "custody", None) is not physical
            or getattr(transaction, "process", None) is not process
        ):
            owner.mark_unknown("cua_windows_job_unavailable")
            raise RuntimeError("cua_windows_job_unavailable")
        if not physical.bind_process(pid, started_at, "windows_job", identity):
            physical.mark_unknown("cua_physical_process_rejected")
            raise RuntimeError("cua_physical_process_rejected")
        physical.bind_launch(process, transaction)
        physical.contained_scope_provider = transaction
        return
    if os.name != "posix":
        owner.mark_unknown("cua_process_scope_unknown")
        raise RuntimeError("cua_process_scope_unknown")
    try:
        pgid = os.getpgid(pid)
    except (AttributeError, OSError, ProcessLookupError) as exc:
        owner.mark_unknown("cua_process_scope_unknown")
        raise RuntimeError("cua_process_scope_unknown") from exc
    if type(pgid) is not int or pgid <= 0 or pgid == os.getpgrp():
        owner.mark_unknown("cua_process_scope_unknown")
        raise RuntimeError("cua_process_scope_unknown")
    if not physical.bind_process(
        pid, None, "posix_process_group", f"posix_process_group:{pid}:{pgid}",
        process_group_id=pgid,
    ):
        physical.mark_unknown("cua_physical_process_rejected")
        raise RuntimeError("cua_physical_process_rejected")
    physical.bind_launch(adapter)
    from gateway.runtime_sidecar_browser import _BrowserPosixScope
    physical.contained_scope_provider = _BrowserPosixScope(adapter, pgid)


def _bind_cua_reader(owner: CuaStdioOwner, physical: Any, process: Any, streams: Any) -> Any:
    from tools.mcp_tool_custody import MCPStdioReaderWitness
    reader = getattr(owner.receipt, "physical_reader", None)
    if reader is None:
        reader = MCPStdioReaderWitness(streams[0], process)
        physical.bind_reader("stdout", reader)
        if not physical.reader_started("stdout", reader):
            owner.mark_unknown("cua_reader_start_unknown")
            raise RuntimeError("cua_reader_start_unknown")
    elif getattr(reader, "stream", None) is not streams[0]:
        owner.mark_unknown("cua_reader_identity_replaced")
        raise RuntimeError("cua_reader_identity_replaced")
    return reader


def _bind_cua_physical(owner: CuaStdioOwner, process: Any, streams: Any) -> Any:
    """Bind the actual CUA child to its source-owned physical receipt."""
    parent = owner.parent
    physical = _cua_physical_for_owner(owner)
    if physical is None and getattr(parent, "_service", None) is None:
        return None
    if not isinstance(streams, (tuple, list)) or not streams or streams[0] is None:
        owner.mark_unknown("cua_physical_reader_missing")
        raise RuntimeError("cua_physical_reader_missing")
    from gateway.runtime_service_operations import reserve_physical
    from gateway.runtime_sidecar_transport import configure_reader_streams
    from tools.mcp_tool_custody import MCPProcessAdapter
    physical = physical or reserve_physical(parent)
    required = getattr(physical, "required_streams", None)
    if required is None and not configure_reader_streams(physical, ("stdout",)):
        owner.mark_unknown("cua_required_streams_invalid")
        raise RuntimeError("cua_required_streams_invalid")
    if required is not None and required != frozenset({"stdout"}):
        owner.mark_unknown("cua_required_streams_replaced")
        raise RuntimeError("cua_required_streams_replaced")
    pid = getattr(process, "pid", None)
    if type(pid) is not int or pid <= 0:
        owner.mark_unknown("cua_process_identity_missing")
        raise RuntimeError("cua_process_identity_missing")
    adapter = MCPProcessAdapter(process)
    if physical.process is not None and physical.process is not adapter:
        owner.mark_unknown("cua_physical_process_replaced")
        raise RuntimeError("cua_physical_process_replaced")
    if physical.process is None:
        _bind_cua_native_scope(owner, physical, process, adapter, pid)
    reader = _bind_cua_reader(owner, physical, process, streams)
    owner.receipt.physical = physical
    owner.receipt.physical_process = adapter
    owner.receipt.physical_reader = reader
    return physical


@asynccontextmanager
async def captured_cua_stdio(stdio_client: Any, server_params: Any, owner: CuaStdioOwner):
    """Capture and close the actual host SDK process under *owner*."""
    owner.bind_task(asyncio.current_task())
    try:
        from tools.mcp_tool_custody import captured_stdio_process
        async with captured_stdio_process(
            stdio_client, server_params, include_context=True,
        ) as (streams, process, transport_context):
            owner.receipt.transport_context = transport_context
            owner.receipt.bind_process(process, streams)
            _bind_cua_physical(owner, process, streams)
            yield streams
    except BaseException as exc:
        await owner.receipt.finish_transport(exc)
        raise
    else:
        await owner.receipt.finish_transport(None)


def _reserve_managed_parent(binding: Any, operation_id: str, source_identity: str) -> Any:
    from gateway.runtime_sidecar_sources import mark_unknown, reserve_managed_sidecar
    owner = reserve_managed_sidecar(
        binding,
        family="cua_stdio",
        source_identity=source_identity,
        producer_key="cua",
        operation_id=operation_id,
    )
    # CUA keeps the same source parent across cached backend turns.  Each later
    # admission gets a fresh request edge through begin_cua_request; the I16 root
    # remains the same canonical ServiceLease owner.  Reserve the physical receipt
    # before the SDK/backend constructor so a later result cannot be accepted with
    # only the CUA context witness.
    try:
        owner.parent.delivery_lifecycle = "persistent"
        from gateway.runtime_service_operations import reserve_physical
        from gateway.runtime_sidecar_transport import configure_reader_streams
        physical = reserve_physical(owner.parent)
        if getattr(physical, "required_streams", None) is None and not configure_reader_streams(physical, ("stdout",)):
            physical.mark_unknown("cua_required_streams_invalid")
            raise RuntimeError("cua_required_streams_invalid")
        return owner.parent
    except BaseException:
        mark_unknown(owner, "cua_reservation_setup_failed")
        raise


def _forget_cua_request(service: Any, binding: Any, request: Any) -> None:
    forget = getattr(getattr(binding, "authority", None), "_preservation_forget_sidecar", None)
    if callable(forget):
        forget(binding.admission_id, request)
    service.forget_request(binding.admission_id)


def _prepare_standalone_cua_route(parent: Any) -> Any:
    """Attach the standalone CUA parent to its exact service/request/physical chain."""
    from gateway.runtime_service_operations import _parent_service

    try:
        service = _parent_service(parent)
        request = service.begin_standalone_request(parent, parent.operation_id)
        physical = service.reserve_standalone_physical(parent, request)
    except BaseException:
        parent.state = "unknown"
        parent.coordinator.mark_unknown(parent.root_handle, "cua_standalone_reservation_failed")
        raise
    parent._request = request
    parent._physical = physical
    parent.finalizer = physical
    parent.state = "request_reserved"
    return parent


def _reserve_standalone_parent(operation_id: str, source_identity: str) -> Any:
    from hermes_constants import get_hermes_home
    from hermes_state_registry import acquire
    from gateway.runtime_service_operations import reserve_standalone_external_process_parent
    db = acquire()
    context = reserve_standalone_external_process_parent(
        profile_home=Path(get_hermes_home()), profile_id=str(Path(get_hermes_home()).resolve()),
        operation_id=operation_id, source_identity=source_identity, profile_db=db,
        producer_key="cua",
    )
    parent = None
    try:
        parent = context.__enter__()
        parent._maintenance_context = context
        parent._cua_owned_profile_db = True
        return _prepare_standalone_cua_route(parent)
    except BaseException:
        if parent is None:
            from hermes_state_registry import release_or_close
            release_or_close(db)
        else:
            _finish_standalone_context(parent)
        raise


def reserve_cua_stdio(*, session_id: str, permission_mode: str, display_identity: str = "") -> CuaStdioOwner:
    """Reserve the canonical parent before CUA's SDK/provider/backend can spawn."""
    session_id = _text(session_id or f"anonymous-{uuid.uuid4().hex}", "cua_session_identity_missing")
    permission_mode = _text(permission_mode, "cua_permission_mode_missing")
    source_identity = f"cua-driver:{session_id}"
    from gateway.runtime_service_custody import current_turn_binding
    binding = current_turn_binding()
    factory = None
    reservation = None
    authority = None
    if binding is not None:
        factory = getattr(binding.authority, "cua_source_factory", lambda: None)()
        if factory is None:
            raise RuntimeError("cua_factory_unregistered")
    else:
        from gateway.run import _gateway_runner_ref
        runner = _gateway_runner_ref() if callable(_gateway_runner_ref) else None
        if runner is not None:
            from gateway.session_authorities import active_authority
            authority = active_authority(runner)
            if authority is not None:
                factory = getattr(authority, "cua_source_factory", lambda: None)()
                if factory is None:
                    raise RuntimeError("cua_factory_unregistered")
    if factory is not None:
        # This must precede UUID allocation and all provider/backend construction.
        reservation = factory.reserve_before_producer(source_identity=source_identity)
    operation_id = f"cua-stdio:{session_id}:{uuid.uuid4().hex}"
    if binding is not None:
        parent = _reserve_managed_parent(binding, operation_id, source_identity)
    else:
        if runner is not None:
            if authority is None:
                if getattr(runner, "session_authorities", None) is not None:
                    from gateway.runtime_service_operations import ExternalProcessOperationError
                    raise ExternalProcessOperationError("profile_mismatch")
                parent = _reserve_standalone_parent(operation_id, source_identity)
            else:
                from gateway.runtime_service_operations import resolve_external_process_parent
                parent = resolve_external_process_parent(
                    runner, profile_home=Path(authority.profile_id), operation_id=operation_id,
                    source_identity=source_identity, mode="live_profile_discovery", producer_key="cua",
                )
        else:
            parent = _reserve_standalone_parent(operation_id, source_identity)
    owner = CuaStdioOwner(
        parent, "cua_stdio", source_identity, session_id, permission_mode, display_identity,
        source_factory=factory,
    )
    if reservation is not None:
        owner.receipt.close_generation = reservation.close_generation
    parent._cua_receipt = owner.receipt
    return owner


def prepare_cua_session(
    *, session_id: str, permission_mode: str, existing_owner: Any = None,
) -> CuaStdioOwner:
    """Return the exact owner for public computer-use ingress.

    A cached managed backend advances through the current authenticated I04
    binding on the same service/physical root.  A new backend reserves its
    parent before the SDK constructor can start a child.  No caller object or
    empty registry is accepted as a substitute for either path.
    """
    from gateway.runtime_service_custody import current_turn_binding
    binding = current_turn_binding()
    if existing_owner is not None:
        parent = getattr(existing_owner, "parent", None)
        if binding is None:
            if getattr(parent, "mode", None) == "managed_turn":
                raise RuntimeError("cua_managed_binding_missing")
            return existing_owner
        if not existing_owner.prepare_current_binding(binding):
            raise RuntimeError("cua_current_binding_mismatch")
        return existing_owner
    return reserve_cua_stdio(
        session_id=session_id,
        permission_mode=permission_mode,
    )


def finish_cua_owner(owner: CuaStdioOwner) -> bool:
    """Close the exact CUA contexts, then retire the existing parent/root."""
    return owner.finish()


def _cua_writer_owner_for_binding(binding: Any):
    """Return the one registered CUA parent for this exact active I04 binding."""
    if binding is None:
        return ()
    authority = getattr(binding, "authority", None)
    finals = getattr(authority, "external_process_finalizers", None)
    if not callable(finals):
        return ()
    matches = []
    for parent in finals():
        receipt = getattr(parent, "_cua_receipt", None)
        service = getattr(parent, "_service", None)
        if (
            not isinstance(receipt, CuaStdioReceipt)
            or getattr(parent, "mode", None) != "managed_turn"
            or getattr(parent, "profile_authority", None) is not authority
            or getattr(parent, "turn_binding", None) is not binding
            or service is None
            or getattr(service, "external_parent", None) is not parent
            or getattr(service, "binding", None) is not binding
        ):
            continue
        request = (getattr(service, "requests", {}) or {}).get(getattr(binding, "admission_id", None))
        if (
            request is None
            or getattr(parent, "_request", None) is not request
            or getattr(parent, "_physical", None) is not getattr(service, "physical", None)
            or getattr(receipt, "physical", None) is not getattr(service, "physical", None)
        ):
            continue
        matches.append((parent, service, request, receipt))
    return tuple(matches)


def _cua_active_owner_for_binding(binding: Any):
    """Find retained CUA owners before validating a writer envelope's request edges."""
    if binding is None:
        return ()
    authority = getattr(binding, "authority", None)
    finals = getattr(authority, "external_process_finalizers", None)
    if not callable(finals):
        return ()
    matches = []
    for parent in finals():
        receipt = getattr(parent, "_cua_receipt", None)
        if (
            not isinstance(receipt, CuaStdioReceipt)
            or getattr(parent, "mode", None) != "managed_turn"
            or getattr(parent, "profile_authority", None) is not authority
            or getattr(parent, "turn_binding", None) is not binding
        ):
            continue
        matches.append((parent, receipt.owner))
    return tuple(matches)


def _cua_active_owners_for_authority(authority: Any):
    """Find retained managed CUA owners when a handoff has no usable binding."""
    finals = getattr(authority, "external_process_finalizers", None)
    if not callable(finals):
        return ()
    return tuple(
        (parent, receipt.owner)
        for parent in finals()
        for receipt in (getattr(parent, "_cua_receipt", None),)
        if isinstance(receipt, CuaStdioReceipt)
        and getattr(parent, "mode", None) == "managed_turn"
        and getattr(parent, "profile_authority", None) is authority
    )


def _cua_mark_handoff_unknown(binding: Any, reason: str) -> bool:
    """Mark the exact active CUA owner UNKNOWN after a handoff identity refusal."""
    from gateway.runtime_service_custody import current_turn_binding

    current = current_turn_binding()
    active = _cua_active_owner_for_binding(binding)
    if not active and current is not binding:
        active = _cua_active_owner_for_binding(current)
    if not active and current is not None:
        active = _cua_active_owners_for_authority(getattr(current, "authority", None))
    for _parent, _owner in active:
        _owner.mark_unknown(reason)
    return bool(active)


def consume_cua_writer_handoff(event: Any, handoff: Any) -> bool | None:
    """Consume the existing typed writer handoff only for an exact managed CUA owner.

    ``None`` means that this active I04 turn has no CUA parent and leaves the existing
    classic/API/native consumer untouched.  ``True`` means the canonical service has
    recorded the durable proof and staged its existing delivery sidecar.  ``False``
    means a CUA parent was present but one of its typed identity/readback fences failed;
    the caller must surface UNKNOWN rather than borrow a gateway row or retire a root.
    """
    try:
        from agent.session_persistence_proof import GatewayWriterProofHandoff, SessionDBWriteProof
        from gateway.runtime_service_custody import (
            DurableResultProof, TurnCustodyBinding, validate_turn_binding,
        )
        binding = getattr(handoff, "binding", None)
        if binding is None:
            from gateway.runtime_service_custody import current_turn_binding
            binding = current_turn_binding()
        if not isinstance(binding, TurnCustodyBinding):
            if _cua_mark_handoff_unknown(None, "cua_writer_handoff_invalid"):
                return False
            return None
        matches = _cua_writer_owner_for_binding(binding)
        if not matches:
            if _cua_mark_handoff_unknown(binding, "cua_writer_handoff_stale"):
                return False
            return None
        if len(matches) != 1:
            for parent, _service, _request, _receipt in matches:
                parent._cua_receipt.owner.mark_unknown("cua_writer_owner_ambiguous")
            return False
        parent, service, request, receipt = matches[0]
        def refuse(reason: str) -> bool:
            receipt.owner.mark_unknown(reason)
            return False
        if (
            type(handoff) is not GatewayWriterProofHandoff
            or getattr(handoff, "binding", None) is not binding
            or type(getattr(handoff, "writer_proof", None)) is not SessionDBWriteProof
            or getattr(handoff, "physical_tip_witness", None) is None
        ):
            return refuse("cua_writer_proof_missing")
        validate_turn_binding(binding)
        proof = handoff.writer_proof
        try:
            # The handoff already carries a typed immutable tip, but the CUA consumer must
            # re-read the current canonical tip at this boundary.  A copied envelope or stale
            # witness must not be able to publish a result merely because its binding object is
            # still present.
            from agent.session_persistence_physical_tip import _physical_tip_value
            from gateway.session_turn_physical_tip import resolve_physical_tip_witness
            current_tip = resolve_physical_tip_witness(
                binding, proof.session_id, proof.store_path,
            )
            retained_tip = _physical_tip_value(getattr(handoff, "physical_tip_witness", None))
        except (
            AttributeError, ImportError, KeyError, OSError, RuntimeError, TypeError, ValueError,
            sqlite3.Error,
        ):
            return refuse("cua_writer_physical_tip_unavailable")
        if current_tip is None or retained_tip is None or retained_tip != current_tip:
            return refuse("cua_writer_physical_tip_mismatch")
        result_ids = getattr(proof, "result_row_ids", None)
        if (
            type(result_ids) is not tuple
            or not result_ids
            or len(set(result_ids)) != len(result_ids)
            or any(type(row_id) is not int or row_id <= 0 for row_id in result_ids)
        ):
            return refuse("cua_writer_proof_result_missing")
        physical = getattr(service, "physical", None)
        if (
            physical is None
            or getattr(parent, "_physical", None) is not physical
            or getattr(receipt, "physical", None) is not physical
            or getattr(physical, "state", None) in {"unknown", "closed", "released"}
        ):
            return refuse("cua_physical_receipt_missing")
        durable = DurableResultProof(
            store_path=proof.store_path,
            session_id=proof.session_id,
            admission_id=binding.admission_id,
            execution_generation=binding.execution_generation,
            row_ids=proof.row_ids,
            row_snapshots=proof.row_snapshots,
            row_sessions=proof.row_sessions,
            row_roles=proof.row_roles,
            payload_digest=proof.payload_digest,
            result_row_ids=result_ids,
        )
        service.record_result_durable(binding, request, durable)
        release = getattr(binding.authority, "_preservation_release_sidecar", None)
        if not callable(release):
            return refuse("cua_sidecar_release_missing")
        release(binding.admission_id, request, durable=True)
        return True
    except Exception:
        logger.exception("CUA writer handoff could not be consumed")
        matches = locals().get("matches", ())
        for parent, _service, _request, _receipt in matches if isinstance(matches, tuple) else ():
            _receipt.owner.mark_unknown("cua_writer_handoff_unknown")
        return False if matches else None


def _cua_service_request_maps_exact(service: Any, binding: Any, request: Any, parent: Any) -> bool:
    requests = getattr(service, "requests", None)
    physical = getattr(service, "physical", None)
    receipt = getattr(parent, "_cua_receipt", None)
    return (
        isinstance(requests, dict)
        and len(requests) == 1
        and requests.get(binding.admission_id) is request
        and getattr(service, "external_parent", None) is parent
        and getattr(parent, "_request", None) is request
        and getattr(parent, "_physical", None) is physical
        and isinstance(receipt, CuaStdioReceipt)
        and getattr(receipt, "physical", None) is physical
        and physical is not None
        and getattr(physical, "state", None) not in {"unknown", "closed", "released"}
        and not getattr(service, "delivery_unknown_reasons", None)
        and not getattr(service, "delivery_terminalized", None)
    )


def _cua_service_quiescent(service: Any) -> bool:
    return (
        not getattr(service, "requests", None)
        and not getattr(service, "durable_requests", None)
        and not getattr(service, "delivery_unknown_reasons", None)
        and not getattr(service, "delivery_terminalized", None)
        and getattr(service, "physical", None) is None
        and getattr(service, "unknown", False) is False
    )


def _cua_sidecar_forgotten(binding: Any) -> bool:
    probe = getattr(binding.authority, "_preservation_has_sidecar", None)
    if not callable(probe):
        return False
    try:
        return probe(binding.admission_id) is False
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def _cua_sidecar_delivery_terminal(binding: Any, request: Any) -> bool:
    entries = getattr(binding.authority, "_preservation_sidecars", None)
    lease_id = getattr(getattr(request, "lease", None), "lease_id", None)
    entry = entries.get(lease_id) if isinstance(entries, dict) else None
    return (
        isinstance(entry, dict)
        and entry.get("admission_id") == binding.admission_id
        and entry.get("request") is request
        and entry.get("terminal") is True
        and entry.get("delivery_complete") is True
    )


def _cua_closed_request_forget_exact(
    service: Any, parent: Any, receipt: Any, binding: Any, request: Any,
) -> bool:
    """Allow only the surviving exact request to cross a post-close forget fence."""
    physical = getattr(service, "physical", None) or getattr(parent, "_physical", None)
    return (
        physical is not None
        and getattr(physical, "state", None) == "closed"
        and getattr(physical, "service", None) is service
        and getattr(parent, "_physical", None) is physical
        and getattr(receipt, "physical", None) is physical
        and getattr(service, "external_parent", None) is parent
        and getattr(service, "binding", None) is binding
        and getattr(parent, "_request", None) is request
        and getattr(service, "unknown", False) is False
        and not getattr(service, "delivery_unknown_reasons", None)
        and not getattr(service, "delivery_terminalized", None)
        and _cua_sidecar_delivery_terminal(binding, request)
    )


def _prepare_cua_request_close(
    parent: Any, receipt: Any, service: Any, request: Any, binding: Any,
) -> tuple[bool, bool] | None:
    managed_parent = getattr(parent, "mode", None) == "managed_turn"
    parts = (service is not None, request is not None, binding is not None)
    if any(parts) and not all(parts):
        return None
    if not all(parts):
        # A managed parent cannot silently fall through to standalone cleanup when
        # one owner edge disappeared.  Preserve the exact root as UNKNOWN.
        if managed_parent:
            return None
        return False, False
    if managed_parent and getattr(parent, "_cua_receipt", None) is not receipt:
        return None
    service_handle = getattr(service, "handle", None)
    request_parent = getattr(getattr(request, "lease", None), "parent_lease_id", None)
    service_lease = getattr(getattr(service_handle, "lease", None), "lease_id", None)
    if (
        getattr(service, "external_parent", None) is not parent
        or service_handle is not parent.root_handle
        or getattr(service, "binding", None) is not binding
        or request_parent != service_lease
    ):
        return None
    request_present = getattr(service, "requests", {}).get(binding.admission_id) is request
    if request_present:
        if not _cua_service_request_maps_exact(service, binding, request, parent):
            if _cua_closed_request_forget_exact(service, parent, receipt, binding, request):
                return True, False
            return None
        # Result durability and delivery are separate canonical fences.  CUA shutdown
        # may not retire an I04 request merely because its SDK context exited; the
        # existing preservation_delivery_outcome consumer must complete_delivery first.
        if binding.admission_id not in getattr(service, "durable_requests", set()):
            return None
        return True, True
    # A successful native close may have cleared the service slot before a later
    # registry/sidecar fence failed.  The parent-held receipt is the only valid
    # retry witness; never synthesize a replacement physical object.
    physical = getattr(service, "physical", None) or getattr(parent, "_physical", None)
    request_lease_id = getattr(getattr(request, "lease", None), "lease_id", None)
    if (
        not getattr(service, "requests", None)
        and not getattr(service, "durable_requests", None)
        and not getattr(service, "delivery_unknown_reasons", None)
        and not getattr(service, "delivery_terminalized", None)
        and getattr(service, "unknown", False) is False
        and physical is not None
        and getattr(physical, "service", None) is service
        and getattr(parent, "_physical", None) is physical
        and getattr(receipt, "physical", None) is physical
        and getattr(physical, "state", None) not in {"unknown", "released"}
        and (
            _cua_sidecar_delivery_terminal(binding, request)
            or (
                getattr(parent, "state", None) in {"request_reserved", "unknown"}
                and _cua_sidecar_forgotten(binding)
                and type(request_lease_id) is str
                and bool(request_lease_id)
                and request_lease_id in getattr(physical, "completed_requests", set())
                and getattr(parent, "_request", None) is request
            )
        )
    ):
        # A persistent CUA service keeps its physical receipt after each completed
        # delivery for the next cached turn.  Final backend shutdown closes this
        # exact receipt here; it is not quiescent while the receipt is still live.
        return True, False
    return None


def _mark_cua_root_terminal(parent: Any) -> bool:
    from gateway.runtime_service_operations import _cua_root_is_terminal
    if not _cua_root_is_terminal(parent):
        records = getattr(parent.coordinator, "_records", None)
        lease_id = getattr(getattr(parent.root_handle, "lease", None), "lease_id", None)
        record = records.get(lease_id) if isinstance(records, dict) else None
        if getattr(record, "state", None) == "unknown":
            reconcile = getattr(parent.coordinator, "reconcile_unknown", None)
            if not callable(reconcile):
                return False
            restored = reconcile(
                parent.root_handle,
                owner_id=parent.root_handle.owner_id,
                profile_id=parent.profile_id,
                process_instance=parent.process_instance,
                authority_epoch=parent.authority_epoch,
            )
            if restored is not None:
                return False
        result = parent.coordinator.mark_terminal(parent.root_handle)
        if result is not None:
            return False
    parent.state = "terminal"
    return True


def _standalone_cua_close_edges_exact(parent: Any, receipt: Any) -> bool:
    service = getattr(parent, "_service", None)
    request = getattr(parent, "_request", None)
    physical = getattr(parent, "_physical", None)
    return (
        getattr(parent, "mode", None) == "standalone_maintenance"
        and service is not None
        and getattr(service, "binding", None) is None
        and getattr(service, "external_parent", None) is parent
        and getattr(service, "requests", {}).get(getattr(parent, "operation_id", None)) is request
        and request is not None
        and getattr(parent, "_request", None) is request
        and getattr(service, "physical", None) is physical
        and physical is not None
        and getattr(physical, "service", None) is service
        and getattr(parent, "_physical", None) is physical
        and getattr(receipt, "physical", None) is physical
        and getattr(physical, "source_receipt", None) is receipt
        and getattr(receipt, "state", None) == "closed"
        and callable(getattr(receipt, "readback", None))
        and receipt.readback() is not None
    )


def _close_standalone_cua_route(parent: Any, receipt: Any) -> bool:
    """Close standalone CUA's physical, request, and service handles in order."""
    if not _standalone_cua_close_edges_exact(parent, receipt):
        parent.state = "unknown"
        return False
    from gateway.runtime_service_operations import _cua_request_is_terminal, _cua_root_is_terminal
    from gateway.runtime_sidecar_cua_standalone import native_close_ready, retry_close

    service = parent._service
    request = parent._request
    physical = parent._physical
    if physical.state != "closed":
        if physical.state == "unknown":
            if not retry_close(parent, physical):
                parent.state = "unknown"
                return False
        elif physical.state == "released" or not native_close_ready(physical):
            physical.mark_unknown("cua_physical_close_unknown")
            parent.coordinator.mark_unknown(parent.root_handle, "cua_physical_close_unknown")
            parent.state = "unknown"
            return False
        if physical.state != "closed":
            result = parent.coordinator.mark_terminal(physical.handle)
            if result is not None:
                physical.mark_unknown("cua_physical_release_failed")
                parent.coordinator.mark_unknown(parent.root_handle, "cua_physical_release_failed")
                parent.state = "unknown"
                return False
            physical.state = "closed"
    if not _cua_request_is_terminal(service, request):
        result = parent.coordinator.mark_terminal(request)
        if result is not None:
            parent.coordinator.mark_unknown(parent.root_handle, "cua_request_finalize_failed")
            parent.state = "unknown"
            return False
    if not _cua_root_is_terminal(parent):
        result = parent.coordinator.mark_terminal(parent.root_handle)
        if result is not None:
            parent.coordinator.mark_unknown(parent.root_handle, "cua_root_retire_failed")
            parent.state = "unknown"
            return False
    service.requests.pop(parent.operation_id, None)
    service.durable_requests.discard(parent.operation_id)
    parent.state = "terminal"
    parent._request = None
    parent.turn_binding = None
    parent._physical = None
    parent._cua_receipt = None
    return True


def close_cua_after_finalizers(parent: Any) -> bool:
    """Commit the exact CUA request and I16 root after the real context readback."""
    receipt = getattr(parent, "_cua_receipt", None)
    if getattr(getattr(receipt, "owner", None), "parent", None) is not parent:
        parent.state = "unknown"
        return False
    if getattr(receipt, "state", None) != "closed" or receipt.readback() is None:
        parent.state = "unknown"
        return False
    if getattr(parent, "mode", None) == "standalone_maintenance":
        return _close_standalone_cua_route(parent, receipt)
    service = getattr(parent, "_service", None)
    request = getattr(parent, "_request", None)
    binding = getattr(parent, "turn_binding", None)
    prepared = _prepare_cua_request_close(parent, receipt, service, request, binding)
    if prepared is None:
        parent.state = "unknown"
        return False
    managed, request_present = prepared
    if managed and request_present:
        # The exact request is still owned by the delivery consumer.  Retain the
        # closed SDK/physical witnesses and refuse root retirement until delivery
        # clears the request and records completed_requests.
        parent.state = "unknown"
        return False
    if managed:
        service = getattr(parent, "_service", None)
        service_physical = getattr(service, "physical", None) if service is not None else None
        parent_physical = getattr(parent, "_physical", None)
        if service_physical is not None and parent_physical is not None and service_physical is not parent_physical:
            return _retain_cua_cleanup_failure(
                parent, service, parent_physical, "cua_managed_owner_incomplete",
            )
        physical = service_physical if service_physical is not None else parent_physical
        if (
            service is None
            or physical is None
            or getattr(parent, "_physical", None) is not physical
            or getattr(physical, "service", None) is not service
        ):
            return _retain_cua_cleanup_failure(
                parent, service, physical, "cua_managed_owner_incomplete",
            )
        if physical is not None:
            if physical.state != "closed" and physical.close_after_delivery() is not True:
                return _retain_cua_cleanup_failure(
                    parent, service, physical, "cua_physical_close_unknown",
                )
            # ServiceProcessReceipt.close_after_delivery may clear its service slot
            # after a successful native close.  Keep the exact closed receipt attached
            # until the parent/root/registry fences below have all committed.
            service.physical = physical
            parent._physical = physical
        if request is not None:
            # A prior retry may already have completed this exact sidecar/request
            # forget fence before a later registry/root fence refused.  Repeating a
            # destructive map operation would turn an idempotent same-owner retry
            # into a false UNKNOWN.  Skip only when both canonical maps prove that
            # this exact request is already forgotten; any surviving edge is still
            # consumed through the source-owned forget helper and failures remain
            # retained.
            request_present = (
                getattr(service, "requests", {}).get(getattr(binding, "admission_id", None))
                is request
            )
            sidecar_forgotten = _cua_sidecar_forgotten(binding)
            if request_present or not sidecar_forgotten:
                try:
                    _forget_cua_request(service, binding, request)
                except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                    return _retain_cua_late_fence(
                        parent, service, physical, "cua_sidecar_retire_failed",
                    )
        # The authority registry consumes only a locally terminal parent.  This is
        # staging state; the coordinator root is still nonterminal until the next
        # fence below commits it.
        parent.state = "terminal"
        forget = getattr(parent.profile_authority, "forget_external_process_finalizer", None)
        if not callable(forget):
            parent.state = "unknown"
            return _retain_cua_late_fence(
                parent, service, physical, "cua_finalizer_retire_failed",
            )
        if not forget(parent.operation_id, parent):
            parent.state = "unknown"
            _restore_cua_finalizer(parent)
            return _retain_cua_late_fence(
                parent, service, physical, "cua_finalizer_retire_failed",
            )
        if not _mark_cua_root_terminal(parent):
            parent.state = "unknown"
            _restore_cua_finalizer(parent)
            return _retain_cua_late_fence(
                parent, service, physical, "cua_root_retire_failed",
            )
        service.physical = None
        parent._physical = None
        parent._request = None
        parent.turn_binding = None
        parent._cua_receipt = None
        return True
    parent.state = "terminal"
    forget = getattr(parent.profile_authority, "forget_external_process_finalizer", None)
    if callable(forget) and not forget(parent.operation_id, parent):
        parent.state = "unknown"
        _restore_cua_finalizer(parent)
        return False
    if not _mark_cua_root_terminal(parent):
        parent.state = "unknown"
        _restore_cua_finalizer(parent)
        return False
    parent._cua_receipt = None
    return True
