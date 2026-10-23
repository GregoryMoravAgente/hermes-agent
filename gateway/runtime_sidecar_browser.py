"""Source-owned browser command and CDP custody.

The browser tool has two real external effects: an ``agent-browser`` command
process and a CDP supervisor thread.  This module binds both effects to the
existing I04/I16 coordinator handles before they are started.  It deliberately
does not keep a second process registry: command owners live on the existing
``SessionAuthority`` finalizer index and the physical receipt is the existing
``ServiceProcessReceipt``.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import signal
import threading
import time
from typing import Any, Iterator
import uuid

from gateway.runtime_service_custody import ServiceLease


class BrowserSourceError(RuntimeError):
    """A browser source cannot prove the requested custody transition."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class BrowserResultProof:
    """Secret-free readback of one actual agent-browser command."""

    operation_id: str
    session_name: str
    command: str
    result_identity: str
    payload_digest: str
    success: bool
    raw_output_digest: str = ""
    process_identity: int = 0
    reader_identities: tuple[int, ...] = ()


class _BrowserPosixScope:
    """The source-owned process group attached to one real browser launch.

    ``ServiceProcessReceipt`` owns the lifecycle; this object only delegates its
    native observation and termination to the exact group created for this Popen.
    It never indexes processes or treats a missing group as a successful close.
    """

    __slots__ = ("pgid", "process")

    def __init__(self, process: Any, pgid: int):
        self.process = process
        self.pgid = pgid

    def observe_empty(self) -> bool | None:
        if self.process.poll() is None:
            return False
        killpg = getattr(os, "killpg", None)
        if killpg is None:
            return False
        try:
            killpg(self.pgid, 0)
        except ProcessLookupError:
            return True
        except (OSError, PermissionError):
            return False
        return False

    def terminate(self) -> None:
        killpg = getattr(os, "killpg", None)
        if killpg is None or os.name == "nt" or self.pgid == os.getpgrp():
            raise OSError("browser process group is not a safe source-owned scope")
        if self.observe_empty() is True:
            return
        killpg(self.pgid, signal.SIGTERM)


class BrowserOutputReader:
    """A real file reader joined against the exact command ``Popen`` object."""

    __slots__ = ("_error", "_thread", "data", "path", "process")

    def __init__(self, path: str, process: Any):
        self.path = path
        self.process = process
        self._thread = threading.Thread(
            target=self._read_after_exit,
            name=f"browser-output-{Path(path).name[:32]}",
            daemon=False,
        )
        self._error: BaseException | None = None
        self.data = ""

    def start(self) -> None:
        self._thread.start()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def join(self, timeout: float | None = None) -> None:
        self._thread.join(timeout)

    def _read_after_exit(self) -> None:
        try:
            while self.process.poll() is None:
                time.sleep(0.005)
            with open(self.path, "r", encoding="utf-8-sig") as stream:
                self.data = stream.read()
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            self._error = exc


@dataclass
class BrowserSourceOwner:
    """One source-owned browser command over the existing service/physical handles."""

    factory: BrowserSourceFactory
    parent: Any
    service: Any
    request: Any
    binding: Any
    operation_id: str
    task_id: str
    session_name: str
    managed: bool
    physical: Any = None
    process: Any = None
    readers: dict[str, BrowserOutputReader] = field(default_factory=dict)
    result_proof: BrowserResultProof | None = None
    _finished: bool = False

    @property
    def authority(self) -> Any:
        return getattr(self.parent, "profile_authority", None)

    @property
    def coordinator(self) -> Any:
        return self.parent.coordinator

    def _entry(self) -> dict[str, Any] | None:
        authority = self.authority
        request_id = getattr(getattr(self.request, "lease", None), "lease_id", None)
        entries = getattr(authority, "_preservation_sidecars", None)
        return entries.get(request_id) if isinstance(entries, dict) else None

    @property
    def result_durable(self) -> bool:
        entry = self._entry()
        return bool(entry and entry.get("browser_result_proof") is self.result_proof)

    def _mark_unknown(self, reason: str) -> None:
        if self.physical is not None:
            self.physical.mark_unknown(reason)
        try:
            from gateway.runtime_service_operations import mark_unknown
            mark_unknown(self.parent, reason)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.parent.state = "unknown"
            self.coordinator.mark_unknown(self.parent.root_handle, reason)
        authority = self.authority
        if self.binding is not None:
            marker = getattr(authority, "_preservation_mark_unknown", None)
            if callable(marker):
                try:
                    marker(self.binding.admission_id, reason)
                except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                    pass

    def _validate_parent(self) -> None:
        from gateway.runtime_service_operations import policy_for_parent
        policy_for_parent(self.parent, mode=self.parent.mode)

    def wait_for_output(self) -> bool:
        """Join source readers before the caller can consume or unlink their files."""
        for reader in self.readers.values():
            reader.join(timeout=10.0)
        return all(not reader.is_alive() and reader._error is None for reader in self.readers.values())

    def cleanup_output_files(self) -> None:
        """Remove only files belonging to this completed source-owned command."""
        for reader in self.readers.values():
            try:
                Path(reader.path).unlink(missing_ok=True)
            except (OSError, TypeError, ValueError):
                self._mark_unknown("browser_output_cleanup_failed")
                return

    def _source_result(self, command: str, observed: Any) -> tuple[str, str] | None:
        """Parse the exact bound stdout and require it to match the observed result."""
        stdout_reader = self.readers.get("stdout")
        stderr_reader = self.readers.get("stderr")
        if stdout_reader is None or stderr_reader is None or not self.wait_for_output():
            return None
        try:
            from tools.browser_tool_session import _interpret_browser_command_output, _unwrap_batch_result
            parsed = _interpret_browser_command_output(
                command,
                stdout_reader.data,
                stderr_reader.data,
                int(getattr(self.process, "returncode", 0) or 0),
            )
            canonical = _unwrap_batch_result(parsed, command)
            if canonical != observed:
                return None
            raw = f"{stdout_reader.data}\x00{stderr_reader.data}".encode()
            canonical_payload = json.dumps(
                canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            ).encode("utf-8")
            return hashlib.sha256(raw).hexdigest(), hashlib.sha256(canonical_payload).hexdigest()
        except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
            return None

    def proof_readback_valid(self) -> bool:
        """Re-read the exact source objects before the delivery fence releases them."""
        proof = self.result_proof
        if not isinstance(proof, BrowserResultProof) or self.process is None:
            return False
        readers = (self.readers.get("stdout"), self.readers.get("stderr"))
        if any(reader is None for reader in readers):
            return False
        if proof.process_identity != id(self.process) or proof.reader_identities != tuple(id(r) for r in readers):
            return False
        current = self._source_result(proof.command, self._canonical_result_from_readback(proof.command))
        if current is None:
            return False
        return current[0] == proof.raw_output_digest and current[1] == proof.payload_digest

    def _canonical_result_from_readback(self, command: str) -> Any:
        stdout_reader = self.readers.get("stdout")
        stderr_reader = self.readers.get("stderr")
        if stdout_reader is None or stderr_reader is None:
            return None
        try:
            from tools.browser_tool_session import _interpret_browser_command_output, _unwrap_batch_result
            return _unwrap_batch_result(
                _interpret_browser_command_output(
                    command, stdout_reader.data, stderr_reader.data,
                    int(getattr(self.process, "returncode", 0) or 0),
                ), command,
            )
        except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
            return None

    def bind_process(
        self, process: Any, stdout_path: str, stderr_path: str, *, transaction: Any = None,
    ) -> None:
        """Bind the actual command process and its two actual output readers."""
        if process is None or type(getattr(process, "pid", None)) is not int or process.pid <= 0:
            self._mark_unknown("browser_process_identity_missing")
            raise BrowserSourceError("browser_process_identity_missing")
        from gateway.runtime_service_operations import reserve_physical
        from gateway.runtime_sidecar_transport import configure_reader_streams

        try:
            self._validate_parent()
            physical = reserve_physical(self.parent)
            if self.physical is not None and self.physical is not physical:
                raise BrowserSourceError("browser_physical_receipt_mismatch")
            self.physical = physical
            if not configure_reader_streams(physical, {"stdout", "stderr"}):
                raise BrowserSourceError("browser_reader_streams_invalid")
            if os.name == "nt":
                if (
                    transaction is None
                    or getattr(transaction, "custody", None) is not physical
                    or getattr(transaction, "process", None) is not process
                ):
                    raise BrowserSourceError("browser_windows_launch_transaction_missing")
                physical.bind_launch(process, transaction)
            else:
                pgid = os.getpgid(process.pid)
                if type(pgid) is not int or pgid <= 0 or pgid == os.getpgrp():
                    raise BrowserSourceError("browser_process_scope_unknown")
                if not physical.bind_process(
                    process.pid, None, "posix_process_group",
                    f"posix_process_group:{process.pid}:{pgid}", process_group_id=pgid,
                ):
                    raise BrowserSourceError("browser_process_identity_mismatch")
                physical.contained_scope_provider = _BrowserPosixScope(process, pgid)
                physical.bind_launch(process)
            self.process = process
            for stream, path in (("stdout", stdout_path), ("stderr", stderr_path)):
                reader = BrowserOutputReader(path, process)
                physical.bind_reader(stream, reader)
                if not physical.reader_started(stream, reader):
                    raise BrowserSourceError("browser_reader_start_unknown")
                self.readers[stream] = reader
                reader.start()
        except BrowserSourceError:
            self._mark_unknown("browser_process_bind_failed")
            raise
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            self._mark_unknown("browser_process_bind_failed")
            raise BrowserSourceError("browser_process_bind_failed") from exc

    def _join_readers(self) -> bool:
        physical = self.physical
        if physical is None or self.process is None:
            return False
        for reader in self.readers.values():
            reader.join(timeout=10.0)
            if reader.is_alive() or reader._error is not None:
                return False
        for stream, reader in self.readers.items():
            physical.reader_eof(stream, reader)
            physical.reader_joined(stream, reader)
            if physical.state == "unknown":
                return False
        return (
            physical.eof_streams == {"stdout", "stderr"}
            and physical.joined_streams == {"stdout", "stderr"}
        )

    def finish(self, command: str, result: Any) -> None:
        """Publish one command readback, then close only the safe direct route."""
        if self._finished:
            return
        if not self._join_readers():
            self._mark_unknown("browser_reader_join_failed")
            return
        if not isinstance(result, dict) or result.get("success") is not True:
            self._mark_unknown("browser_command_failed")
            return
        readback = self._source_result(command, result)
        if readback is None:
            self._mark_unknown("browser_result_readback_unknown")
            return
        raw_digest, payload_digest = readback
        identity = f"{self.session_name}:{command}:{raw_digest}:{payload_digest}"
        self.result_proof = BrowserResultProof(
            operation_id=self.operation_id,
            session_name=self.session_name,
            command=command,
            result_identity=identity,
            payload_digest=payload_digest,
            success=True,
            raw_output_digest=raw_digest,
            process_identity=id(self.process),
            reader_identities=tuple(id(self.readers[name]) for name in ("stdout", "stderr")),
        )
        try:
            self._validate_parent()
            from gateway.runtime_service_operations import ExternalPublicationProof, publish_catalog
            publish_catalog(
                self.parent,
                ExternalPublicationProof(
                    operation_id=self.operation_id,
                    profile_id=self.parent.profile_id,
                    process_instance=self.parent.process_instance,
                    authority_epoch=self.parent.authority_epoch,
                    source_identity=self.parent.source_identity,
                    result_identity=identity,
                    cache_readback=True,
                    cache_identity=f"browser:{self.session_name}:{command}",
                    payload_digest=payload_digest,
                ),
            )
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            self._mark_unknown("browser_publication_unknown")
            raise BrowserSourceError("browser_publication_unknown") from exc
        entry = self._entry()
        if self.managed:
            # The existing sidecar entry is the delivery index.  The readback above is source-owned
            # and immutable; the canonical admission finalizer consumes it through BrowserServiceLease.
            if entry is None:
                self._mark_unknown("browser_sidecar_entry_missing")
                return
            entry["browser_owner"] = self
            entry["browser_result_proof"] = self.result_proof
            release = getattr(self.authority, "_preservation_release_sidecar", None)
            if not callable(release):
                self._mark_unknown("browser_sidecar_release_missing")
                return
            try:
                release(self.binding.admission_id, self.request, durable=True)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                self._mark_unknown("browser_sidecar_release_failed")
                raise BrowserSourceError("browser_sidecar_release_failed") from exc
            if not self.result_durable:
                self._mark_unknown("browser_sidecar_readback_unknown")
                return
            self._finished = True
            return
        if self._close_direct():
            self._finished = True

    def _close_direct(self) -> bool:
        if self.parent.state == "terminal":
            return True
        try:
            from gateway.runtime_service_operations import close_after_finalizers
            if close_after_finalizers(self.parent) is not True:
                self._mark_unknown("browser_close_failed")
                return False
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self._mark_unknown("browser_close_failed")
            return False
        return True

    def abort(self, reason: str = "browser_command_unsettled") -> None:
        if self._finished:
            return
        process = self.process
        if process is not None:
            try:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=2.0)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                self._mark_unknown("browser_process_close_unknown")
        self._mark_unknown(reason)


class BrowserServiceLease(ServiceLease):
    """A ServiceLease using the same coordinator handles with browser readback semantics."""

    def __init__(
        self, coordinator: Any, handle: Any, binding: Any, owner_id: str, requests: dict,
        *, producer_key: str = "browser",
    ):
        super().__init__(
            coordinator, handle, binding, owner_id, requests, producer_key=producer_key,
        )
        self.browser_owner: BrowserSourceOwner | None = None

    def complete_delivery(self, binding: Any, request: Any) -> None:
        from gateway.runtime_service_custody import ServiceCustodyError
        owner = self.browser_owner
        if owner is None or owner.binding is not binding or owner.request is not request:
            raise ServiceCustodyError("browser_owner_mismatch")
        current = self.requests.get(binding.admission_id)
        physical = self.physical
        if current is not request or physical is None or physical.service is not self:
            raise ServiceCustodyError("browser_physical_receipt_missing")
        parent = owner.parent
        request_lease = getattr(getattr(request, "lease", None), "lease_id", None)
        service_lease = getattr(getattr(self.handle, "lease", None), "lease_id", None)
        physical_lease = getattr(getattr(physical.handle, "lease", None), "lease_id", None)
        secondary = getattr(self.coordinator, "_secondary_parents_by_child", {}).get(request_lease)
        if (
            getattr(parent, "_service", None) is not self
            or getattr(parent, "turn_binding", None) is not binding
            or getattr(parent, "root_handle", None) is not self.handle
            or getattr(parent, "_request", None) is not request
            or physical.handle.profile_id != binding.profile_id
            or physical.handle.process_instance != binding.process_instance
            or physical.handle.authority_epoch != binding.authority_epoch
            or getattr(physical.handle.lease, "parent_lease_id", None) != service_lease
            or getattr(request.lease, "parent_lease_id", None) != service_lease
            or secondary != {getattr(binding.parent_handle.lease, "lease_id", None)}
            or physical_lease is None
            or request_lease is None
        ):
            physical.mark_unknown("browser_physical_identity_unknown")
            self.unknown = True
            raise ServiceCustodyError("browser_physical_identity_unknown")
        if binding.admission_id in self.delivery_terminalized:
            if physical.state != "closed":
                self.unknown = True
                raise ServiceCustodyError("browser_delivery_finalize_failed")
            try:
                from gateway.runtime_service_operations import retire_per_invocation_parent
                retire_per_invocation_parent(self, binding, request, physical)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
                owner = self.browser_owner
                if owner is not None:
                    owner._mark_unknown("browser_parent_retire_failed")
                self.unknown = True
                raise ServiceCustodyError("browser_parent_retire_failed") from exc
            self.physical = None
            self.requests.pop(binding.admission_id, None)
            self.durable_requests.discard(binding.admission_id)
            self.delivery_terminalized.discard(binding.admission_id)
            self.unknown = False
            return
        try:
            owner._validate_parent()
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            physical.mark_unknown("browser_physical_owner_unknown")
            self.unknown = True
            raise ServiceCustodyError("browser_physical_owner_unknown") from exc
        proof = owner.result_proof
        if not owner.result_durable or not isinstance(proof, BrowserResultProof):
            raise ServiceCustodyError("browser_result_unknown")
        if not owner.proof_readback_valid():
            physical.mark_unknown("browser_result_readback_unknown")
            self.unknown = True
            raise ServiceCustodyError("browser_result_readback_unknown")
        publication = physical.external_publication_proof
        if (
            getattr(publication, "operation_id", None) != owner.operation_id
            or getattr(publication, "profile_id", None) != owner.parent.profile_id
            or getattr(publication, "process_instance", None) != owner.parent.process_instance
            or getattr(publication, "authority_epoch", None) != owner.parent.authority_epoch
            or getattr(publication, "source_identity", None) != owner.parent.source_identity
            or getattr(publication, "result_identity", None) != proof.result_identity
            or getattr(publication, "payload_digest", None) != proof.payload_digest
            or getattr(publication, "cache_readback", None) is not True
            or owner.operation_id not in physical.completed_requests
        ):
            physical.mark_unknown("browser_publication_identity_unknown")
            self.unknown = True
            raise ServiceCustodyError("browser_publication_identity_unknown")
        row = None
        try:
            from hermes_state_runtime import get_session_admission
            row = get_session_admission(binding.authority.db, admission_id=binding.admission_id)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            row = None
        if (
            not isinstance(row, dict)
            or row.get("status") != "terminal"
            or row.get("admission_id") != binding.admission_id
            or row.get("target_session_id") != binding.session_id
            or row.get("generation") != binding.execution_generation
            or row.get("owner_epoch") != binding.authority_epoch
            or row.get("principal_id") not in {binding.principal_id, binding.source_identity}
        ):
            physical.mark_unknown("browser_terminal_readback_unknown")
            self.unknown = True
            raise ServiceCustodyError("browser_terminal_readback_unknown")
        if physical.state == "unknown":
            raise ServiceCustodyError("browser_physical_unknown")
        if physical.process is not owner.process or not physical.close_native_after_process():
            physical.mark_unknown("browser_close_failed")
            self.unknown = True
            raise ServiceCustodyError("browser_close_failed")
        if physical.eof_streams != {"stdout", "stderr"} or physical.joined_streams != {"stdout", "stderr"}:
            physical.mark_unknown("browser_reader_readback_unknown")
            self.unknown = True
            raise ServiceCustodyError("browser_reader_readback_unknown")
        result = self.coordinator.mark_terminal_with_secondary_parents(request, (binding.parent_handle,))
        if result is not None:
            physical.mark_unknown("browser_delivery_finalize_failed")
            self.unknown = True
            raise ServiceCustodyError("browser_delivery_finalize_failed")
        if self.coordinator.mark_terminal(physical.handle) is not None:
            physical.mark_unknown("browser_physical_release_failed")
            self.unknown = True
            raise ServiceCustodyError("browser_physical_release_failed")
        physical.state = "closed"
        self.delivery_terminalized.add(binding.admission_id)
        try:
            from gateway.runtime_service_operations import retire_per_invocation_parent
            retire_per_invocation_parent(self, binding, request, physical)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            owner._mark_unknown("browser_parent_retire_failed")
            raise ServiceCustodyError("browser_parent_retire_failed") from exc
        self.physical = None
        self.requests.pop(binding.admission_id, None)
        self.durable_requests.discard(binding.admission_id)
        self.delivery_terminalized.discard(binding.admission_id)
        self.unknown = False


def _service_handle(coordinator: Any, binding: Any, owner_id: str) -> Any:
    from gateway.runtime_custody import CustodyRefusal
    handle = coordinator.reserve_root(
        "I16_turn_sidecars", owner_id,
        profile_id=binding.profile_id, authority_epoch=binding.authority_epoch,
    )
    if isinstance(handle, CustodyRefusal):
        raise BrowserSourceError(handle.code)
    return handle


def _reserve_managed_browser(
    binding: Any, *, operation_id: str, task_id: str, session_name: str,
    factory: BrowserSourceFactory,
) -> BrowserSourceOwner:
    from gateway.runtime_service_operations import parent_for_managed_service, reserve_physical

    service = None
    parent = None
    try:
        handle = _service_handle(binding.coordinator, binding, f"browser:{operation_id}")
        service = BrowserServiceLease(
            binding.coordinator, handle, binding, f"browser:{operation_id}", {},
            producer_key="browser",
        )
        registry = getattr(binding.coordinator, "_i16_source_registry", None)
        if registry is not None:
            try:
                registry.register_service(service, "browser")
            except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
                binding.coordinator.release_unaccepted(handle)
                raise BrowserSourceError("browser_source_unregistered") from exc
        parent = parent_for_managed_service(
            binding, service, operation_id=operation_id, delivery_lifecycle="per_invocation",
        )
        register = getattr(binding.authority, "register_external_process_finalizer", None)
        if not callable(register) or register(operation_id, parent) is not parent:
            raise BrowserSourceError("browser_finalizer_registry_missing")
        request = service.begin_request(binding)
        # ``parent_for_managed_service`` creates the canonical parent before
        # the per-admission request exists.  The existing managed-service
        # path publishes that exact request on the parent before any delivery
        # or finalizer can consume it; browser sidecars must preserve the same
        # identity edge instead of relying on a copied request lookup.
        parent._request = request
        binder = getattr(binding.authority, "_preservation_bind_sidecar", None)
        if not callable(binder):
            raise BrowserSourceError("browser_sidecar_binder_missing")
        binder(binding.admission_id, binding.parent_handle, service, request)
        physical = reserve_physical(parent)
        owner = BrowserSourceOwner(
            factory=factory, parent=parent, service=service, request=request, binding=binding,
            operation_id=operation_id, task_id=task_id, session_name=session_name, managed=True,
            physical=physical,
        )
        service.browser_owner = owner
        entry = owner._entry()
        if entry is not None:
            entry["browser_owner"] = owner
        return owner
    except BaseException:
        if parent is not None:
            try:
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, "browser_reservation_failed")
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                pass
        elif service is not None:
            try:
                binding.coordinator.mark_unknown(service.handle, "browser_reservation_failed")
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                pass
        raise


class BrowserSupervisorReceipt:
    """Receipt for an actual CDP supervisor object, with no fabricated process proof."""

    def __init__(self, parent: Any, task_id: str, cdp_url: str):
        self.parent = parent
        self.task_id = task_id
        self.cdp_url = cdp_url
        self.supervisor: Any = None
        self.state = "reserved"
        self._readback: dict[str, Any] | None = None

    def bind(self, supervisor: Any) -> None:
        try:
            from tools.browser_supervisor import CDPSupervisor, SUPERVISOR_REGISTRY
        except (ImportError, RuntimeError, TypeError, ValueError) as exc:
            self.mark_unknown("browser_supervisor_type_unavailable")
            raise BrowserSourceError("browser_supervisor_type_unavailable") from exc
        if not isinstance(supervisor, CDPSupervisor) or SUPERVISOR_REGISTRY.get(self.task_id) is not supervisor:
            self.mark_unknown("browser_supervisor_identity_missing")
            raise BrowserSourceError("browser_supervisor_identity_missing")
        thread = getattr(supervisor, "_thread", None)
        loop = getattr(supervisor, "_loop", None)
        if supervisor is None or not callable(getattr(thread, "is_alive", None)) or not thread.is_alive() or loop is None:
            self.mark_unknown("browser_supervisor_identity_missing")
            raise BrowserSourceError("browser_supervisor_identity_missing")
        if getattr(supervisor, "task_id", None) != self.task_id or getattr(supervisor, "cdp_url", None) != self.cdp_url:
            self.mark_unknown("browser_supervisor_identity_mismatch")
            raise BrowserSourceError("browser_supervisor_identity_mismatch")
        existing = getattr(supervisor, "_i16_browser_receipt", None)
        if existing is not None and existing is not self:
            self.mark_unknown("browser_supervisor_receipt_replaced")
            raise BrowserSourceError("browser_supervisor_receipt_replaced")
        try:
            from gateway.runtime_service_operations import policy_for_parent
            policy_for_parent(self.parent, mode=self.parent.mode)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            self.mark_unknown("browser_supervisor_owner_unknown")
            raise BrowserSourceError("browser_supervisor_owner_unknown") from exc
        self.supervisor = supervisor
        self.state = "accepted"
        self.parent.state = "physical_bound"
        supervisor._i16_browser_receipt = self

    def mark_unknown(self, reason: str) -> None:
        self.state = "unknown"
        try:
            from gateway.runtime_service_operations import mark_unknown
            mark_unknown(self.parent, reason)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.parent.state = "unknown"

    def close_after_shutdown(self, supervisor: Any = None) -> bool:
        if self.state != "accepted" or self.supervisor is None:
            return False
        if supervisor is not None and supervisor is not self.supervisor:
            self.mark_unknown("browser_supervisor_replaced")
            return False
        try:
            from tools.browser_supervisor import SUPERVISOR_REGISTRY
            registered = SUPERVISOR_REGISTRY.get(self.task_id)
            if registered is not None and registered is not self.supervisor:
                self.mark_unknown("browser_supervisor_replaced")
                return False
        except (ImportError, RuntimeError, TypeError, ValueError):
            self.mark_unknown("browser_supervisor_registry_unknown")
            return False
        try:
            from gateway.runtime_service_operations import policy_for_parent
            policy_for_parent(self.parent, mode=self.parent.mode)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.mark_unknown("browser_supervisor_owner_unknown")
            return False
        thread = getattr(self.supervisor, "_thread", None)
        loop = getattr(self.supervisor, "_loop", None)
        if (
            thread is None
            or thread.is_alive()
            or loop is None
            or loop.is_running()
        ):
            self.mark_unknown("browser_supervisor_close_failed")
            return False
        self.state = "closed"
        self._readback = {
            "task_id": self.task_id,
            "cdp_url": self.cdp_url,
            "supervisor": self.supervisor,
        }
        return True

    def readback(self) -> dict[str, Any] | None:
        return self._readback if self.state == "closed" else None


class BrowserSourceFactory:
    """One browser source capability attached to one live SessionAuthority."""

    def __init__(self, runner: Any, authority: Any):
        if authority is None or getattr(authority, "runner", None) is not runner:
            raise BrowserSourceError("browser_factory_authority_mismatch")
        if not isinstance(getattr(authority, "profile_id", None), str) or not authority.profile_id:
            raise BrowserSourceError("browser_factory_profile_missing")
        self.runner = runner
        self.authority = authority
        self.family = "browser"

    def _current_identity(self) -> None:
        if getattr(self.authority, "retiring", False) or getattr(self.authority, "retired", False):
            raise BrowserSourceError("browser_authority_retiring")
        from gateway.session_authorities import authority_for_home
        if authority_for_home(self.runner, Path(self.authority.profile_id)) is not self.authority:
            raise BrowserSourceError("browser_authority_stale")
        try:
            from gateway.runtime_ownership import process_ownership
            if process_ownership.reservation_evidence(Path(self.authority.profile_id)) is None:
                raise BrowserSourceError("browser_physical_owner_unknown")
        except BrowserSourceError:
            raise
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            raise BrowserSourceError("browser_physical_owner_unknown") from exc

    def reserve_command(self, task_id: str, session_info: dict[str, Any]) -> BrowserSourceOwner:
        self._current_identity()
        if type(task_id) is not str or not task_id:
            raise BrowserSourceError("browser_task_identity_missing")
        session_name = session_info.get("session_name") if isinstance(session_info, dict) else None
        if type(session_name) is not str or not session_name:
            raise BrowserSourceError("browser_session_identity_missing")
        operation_id = f"browser:{task_id}:{session_name}:{uuid.uuid4().hex}"
        from gateway.runtime_service_custody import current_turn_binding
        binding = current_turn_binding()
        if binding is not None:
            if binding.authority is not self.authority:
                raise BrowserSourceError("browser_binding_authority_mismatch")
            return _reserve_managed_browser(
                binding, operation_id=operation_id, task_id=task_id,
                session_name=session_name, factory=self,
            )
        from gateway.runtime_service_operations import (
            resolve_external_process_parent, reserve_physical, reserve_request,
        )
        parent = resolve_external_process_parent(
            self.runner,
            profile_home=Path(self.authority.profile_id),
            operation_id=operation_id,
            source_identity=f"browser:{session_name}",
            mode="live_profile_discovery", producer_key="browser",
        )
        try:
            reserve_request(parent)
            physical = reserve_physical(parent)
            return BrowserSourceOwner(
                factory=self, parent=parent, service=parent._service,
                request=None, binding=None, operation_id=operation_id,
                task_id=task_id, session_name=session_name, managed=False, physical=physical,
            )
        except BaseException:
            try:
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, "browser_reservation_failed")
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                pass
            raise

    def reserve_supervisor(self, task_id: str, cdp_url: str) -> BrowserSupervisorReceipt | None:
        self._current_identity()
        if type(task_id) is not str or not task_id or type(cdp_url) is not str or not cdp_url:
            raise BrowserSourceError("browser_supervisor_identity_missing")
        operation_id = f"browser-cdp:{task_id}:{uuid.uuid4().hex}"
        from gateway.runtime_service_operations import resolve_external_process_parent, reserve_request
        parent = resolve_external_process_parent(
            self.runner,
            profile_home=Path(self.authority.profile_id),
            operation_id=operation_id,
            source_identity=f"browser-cdp:{task_id}",
            mode="live_profile_discovery", producer_key="browser",
        )
        try:
            reserve_request(parent)
            receipt = BrowserSupervisorReceipt(parent, task_id, cdp_url)
            parent._remote_receipt = receipt
            parent.finalizer = receipt
            return receipt
        except BaseException:
            try:
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, "browser_supervisor_reservation_failed")
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                pass
            raise


def register_browser_source_factory(runner: Any, authority: Any) -> BrowserSourceFactory:
    current = getattr(authority, "_browser_source_factory", None)
    if current is not None:
        if not isinstance(current, BrowserSourceFactory) or current.authority is not authority:
            raise BrowserSourceError("browser_factory_registry_collision")
        return current
    factory = BrowserSourceFactory(runner, authority)
    register = getattr(authority, "register_browser_source_factory", None)
    if not callable(register):
        raise BrowserSourceError("browser_factory_registry_missing")
    register(factory)
    return factory


def factory_for_authority(authority: Any) -> BrowserSourceFactory | None:
    factory = getattr(authority, "_browser_source_factory", None)
    if factory is None:
        return None
    if not isinstance(factory, BrowserSourceFactory) or factory.authority is not authority:
        raise BrowserSourceError("browser_factory_registry_mismatch")
    return factory


def _active_factory() -> BrowserSourceFactory | None:
    from gateway.runtime_service_custody import current_turn_binding
    binding = current_turn_binding()
    if binding is not None:
        return factory_for_authority(binding.authority)
    try:
        from gateway.run import _gateway_runner_ref
        runner = _gateway_runner_ref()
    except (AttributeError, TypeError):
        runner = None
    if runner is None:
        return None
    from gateway.session_authorities import active_authority
    authority = active_authority(runner)
    return None if authority is None else factory_for_authority(authority)


def begin_browser_command(task_id: str, session_info: dict[str, Any]) -> BrowserSourceOwner | None:
    factory = _active_factory()
    if factory is None:
        # Unit/standalone tool callers without a live gateway retain the historical direct path.
        # A live gateway always installs a factory before public browser ingress.
        return None
    return factory.reserve_command(task_id, session_info)


def finish_browser_command(owner: BrowserSourceOwner, command: str, result: Any) -> None:
    owner.finish(command, result)


def fail_browser_command(owner: BrowserSourceOwner, reason: str) -> None:
    owner.abort(reason)


def begin_browser_supervisor(task_id: str, cdp_url: str) -> BrowserSupervisorReceipt | None:
    factory = _active_factory()
    return None if factory is None else factory.reserve_supervisor(task_id, cdp_url)


def finish_browser_supervisor(supervisor: Any) -> bool:
    receipt = getattr(supervisor, "_i16_browser_receipt", None)
    if not isinstance(receipt, BrowserSupervisorReceipt):
        return True
    if not receipt.close_after_shutdown(supervisor):
        return False
    try:
        from gateway.runtime_service_operations import close_after_finalizers
        return close_after_finalizers(receipt.parent) is True and receipt.readback() is not None
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        receipt.mark_unknown("browser_supervisor_close_failed")
        return False


@contextmanager
def browser_command_scope(task_id: str, session_info: dict[str, Any]) -> Iterator[BrowserSourceOwner | None]:
    owner = begin_browser_command(task_id, session_info)
    try:
        yield owner
    except BaseException:
        if owner is not None:
            owner.abort("browser_command_exception")
        raise
    finally:
        if owner is not None and not owner._finished:
            owner.abort("browser_command_unsettled")


__all__ = [
    "BrowserOutputReader", "BrowserResultProof", "BrowserServiceLease", "BrowserSourceError",
    "BrowserSourceFactory", "BrowserSourceOwner", "begin_browser_command", "begin_browser_supervisor",
    "browser_command_scope", "factory_for_authority", "fail_browser_command", "finish_browser_command",
    "finish_browser_supervisor", "register_browser_source_factory",
]
