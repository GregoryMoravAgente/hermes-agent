"""Native custody for browser backends that own a long-lived local process.

The browser command receipt in :mod:`gateway.runtime_sidecar_browser` is intentionally
short lived.  A Lightpanda server has a different lifetime: it owns the CDP endpoint
until the browser session is cleaned up.  This module gives that server one typed view
over the existing external-process parent and ``ServiceProcessReceipt``.  It does not
create another registry, infer ownership from a PID, or turn an unverified close into a
terminal parent.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import logging
import os
from pathlib import Path
import subprocess
from typing import Any
import urllib.error
import urllib.request


logger = logging.getLogger(__name__)

def _cdp_version_digest(url: str) -> str | None:
    """Read the exact loopback CDP version response without retaining its payload."""
    if type(url) is not str or not url.startswith("http://127.0.0.1:"):
        return None
    request = urllib.request.Request(
        f"{url.rstrip('/')}/json/version", headers={"Accept": "application/json"},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout=0.5) as response:
            status = getattr(response, "status", 200)
            if type(status) is not int or not 200 <= status < 300:
                return None
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, TypeError, ValueError, urllib.error.URLError):
        return None
    if not isinstance(payload, dict) or type(payload.get("webSocketDebuggerUrl")) is not str:
        return None
    canonical = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _owned_listener_identity(process: Any, port: int, expected_start_time: int | None) -> tuple | None:
    """Return a listener tuple only when the exact child owns *port*.

    A live Popen and a responsive URL do not establish endpoint custody: another local
    process can answer after a port race.  ``psutil.Process.net_connections`` is scoped to
    the already authenticated child PID, and the start-time check prevents a recycled PID
    from satisfying the readback.  Missing/denied native inspection is deliberately
    indistinguishable from a failed association and therefore refuses closed custody.
    """
    pid = getattr(process, "pid", None)
    if type(pid) is not int or pid <= 0 or type(port) is not int or port <= 0:
        return None
    if type(expected_start_time) is not int:
        return None
    try:
        import psutil
    except ImportError:
        return None
    try:
        from tools.process_registry import ProcessRegistry

        observed_start_time = ProcessRegistry._safe_host_start_time(pid)
        if observed_start_time != expected_start_time:
            return None
        connections = psutil.Process(pid).net_connections(kind="tcp")
    except (psutil.Error, AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None
    if not isinstance(connections, (list, tuple)):
        return None
    listen = getattr(psutil, "CONN_LISTEN", "LISTEN")
    for connection in connections:
        if getattr(connection, "status", None) != listen:
            continue
        address = getattr(connection, "laddr", None)
        host = getattr(address, "ip", None)
        observed_port = getattr(address, "port", None)
        if host is None and isinstance(address, tuple) and len(address) >= 2:
            host, observed_port = address[0], address[1]
        if host not in {
            "127.0.0.1", "0.0.0.0", "::1", "::", "::ffff:127.0.0.1",
        } or observed_port != port:
            continue
        fd = getattr(connection, "fd", -1)
        return (pid, expected_start_time, str(host), port, fd if type(fd) is int else -1)
    return None


@dataclass
class BrowserNativeBackendReceipt:
    """One exact Lightpanda process and its source-owned output readers."""

    parent: Any
    family: str
    source_identity: str
    operation_id: str
    process: Any = None
    physical: Any = None
    server: Any = None
    stdout_reader: Any = None
    stderr_reader: Any = None
    state: str = "reserved"
    unknown_reason: str | None = None
    cdp_readback_digest: str | None = None
    endpoint_identity: tuple | None = None
    _readback: dict[str, Any] | None = field(default=None, repr=False)

    def readback(self) -> dict[str, Any] | None:
        return self._readback if self.state == "closed" else None

    def _mark_unknown(self, reason: str, *, physical_reason: str | None = None) -> None:
        if self.state == "closed":
            return
        self.state = "unknown"
        self.unknown_reason = reason
        physical = self.physical
        if physical is not None:
            try:
                physical.mark_unknown(physical_reason or reason)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                pass
        parent = self.parent
        try:
            from gateway.runtime_service_operations import mark_unknown
            mark_unknown(parent, reason)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            parent.state = "unknown"
            try:
                parent.coordinator.mark_unknown(parent.root_handle, reason)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                pass

    def _validate_parent(self) -> None:
        from gateway.runtime_service_operations import policy_for_parent
        policy_for_parent(self.parent, mode=self.parent.mode)

    def _reopen_retryable_parent(self) -> bool:
        """Reopen only this receipt's canonical UNKNOWN handle for a close retry."""
        physical = self.physical
        if self.state != "unknown" or physical is None:
            return self.state != "unknown"
        can_retry = getattr(physical, "can_retry_external_close", None)
        if not callable(can_retry) or not can_retry():
            return False
        parent = self.parent
        try:
            restored = parent.coordinator.reconcile_unknown(
                parent.root_handle, owner_id=parent.root_handle.owner_id,
                profile_id=parent.profile_id, process_instance=parent.process_instance,
                authority_epoch=parent.authority_epoch,
            )
            if restored is not None:
                return False
            service = getattr(parent, "_service", None)
            if service is None:
                return False
            parent._physical = physical
            service.physical = physical
            # The physical coordinator handle is still UNKNOWN.  Keep its state and
            # reason until ServiceProcessReceipt.close_after_delivery authenticates
            # and reconciles that same handle after the native/readback fences.
            parent.state = "physical_bound"
            return True
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return False

    def _reserve_physical(self) -> Any:
        from gateway.runtime_service_operations import reserve_physical
        physical = reserve_physical(self.parent)
        if self.physical is not None and self.physical is not physical:
            self._mark_unknown("browser_native_physical_replaced")
            raise RuntimeError("browser_native_physical_replaced")
        self.physical = physical
        return physical

    def _configure_readers(self, physical: Any) -> None:
        from gateway.runtime_sidecar_transport import configure_reader_streams
        if not configure_reader_streams(physical, {"stdout", "stderr"}):
            raise RuntimeError("browser_native_reader_streams_invalid")

    def bind_process(
        self, process: Any, stderr_path: str, *, stdout_path: str | None = None,
        transaction: Any = None,
    ) -> None:
        """Bind the exact Popen/Job and the real stderr file before readiness probing."""
        pid = getattr(process, "pid", None)
        if process is None or type(pid) is not int or pid <= 0:
            self._mark_unknown("browser_native_process_identity_missing")
            raise RuntimeError("browser_native_process_identity_missing")
        physical = self._reserve_physical()
        try:
            self._validate_parent()
            self._configure_readers(physical)
            if os.name == "nt":
                if (
                    transaction is None
                    or getattr(transaction, "custody", None) is not physical
                    or getattr(transaction, "process", None) is not process
                ):
                    raise RuntimeError("browser_native_windows_launch_transaction_missing")
                physical.bind_launch(process, transaction)
            else:
                from gateway.runtime_sidecar_browser import _BrowserPosixScope
                pgid = os.getpgid(pid)
                if type(pgid) is not int or pgid <= 0 or pgid == os.getpgrp():
                    raise RuntimeError("browser_native_process_scope_unknown")
                if not physical.bind_process(
                    pid, None, "posix_process_group",
                    f"posix_process_group:{pid}:{pgid}", process_group_id=pgid,
                ):
                    raise RuntimeError("browser_native_process_identity_mismatch")
                physical.contained_scope_provider = _BrowserPosixScope(process, pgid)
                physical.bind_launch(process)
            from gateway.runtime_sidecar_browser import BrowserOutputReader
            stdout_path = stdout_path or f"{stderr_path}.stdout"
            readers = {
                "stdout": BrowserOutputReader(stdout_path, process),
                "stderr": BrowserOutputReader(stderr_path, process),
            }
            # Publish the exact child/readers before starting either reader.  A start or
            # bind failure is ambiguous, so abort_process must still reach every object
            # that may have observed the native effect.
            self.process = process
            self.stdout_reader = readers["stdout"]
            self.stderr_reader = readers["stderr"]
            for stream, reader in readers.items():
                physical.bind_reader(stream, reader)
                if not physical.reader_started(stream, reader):
                    raise RuntimeError("browser_native_reader_start_unknown")
                reader.start()
            self.state = "accepted"
            self.parent.state = "physical_bound"
        except BaseException:  # health: retain the exact process on every bind ambiguity
            try:
                physical.retain_partial_launch(process, transaction, reason="partial_launch")
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                physical.mark_unknown("partial_launch")
            self._mark_unknown("browser_native_process_bind_failed")
            raise

    def bind_server(self, server: Any) -> None:
        if (
            server is None
            or getattr(server, "proc", None) is not self.process
            or type(getattr(server, "port", None)) is not int
            or getattr(server, "port", 0) <= 0
            or getattr(server, "cdp_url", None) != f"http://127.0.0.1:{server.port}"
        ):
            self._mark_unknown("browser_native_server_identity_missing")
            raise RuntimeError("browser_native_server_identity_missing")
        self.server = server

    def capture_cdp_readback(self) -> None:
        if self.state != "accepted" or self.server is None:
            self._mark_unknown("browser_native_cdp_owner_unknown")
            raise RuntimeError("browser_native_cdp_owner_unknown")
        digest = _cdp_version_digest(self.server.cdp_url)
        if digest is None:
            self._mark_unknown("browser_native_cdp_readback_unknown")
            raise RuntimeError("browser_native_cdp_readback_unknown")
        endpoint = _owned_listener_identity(self.process, self.server.port, self.server.start_time)
        if endpoint is None:
            self._mark_unknown("browser_native_endpoint_owner_unknown")
            raise RuntimeError("browser_native_endpoint_owner_unknown")
        self.cdp_readback_digest = digest
        self.endpoint_identity = endpoint
        try:
            from gateway.runtime_service_operations import ExternalPublicationProof, publish_catalog
            publish_catalog(
                self.parent,
                ExternalPublicationProof(
                    operation_id=self.operation_id,
                    profile_id=self.parent.profile_id,
                    process_instance=self.parent.process_instance,
                    authority_epoch=self.parent.authority_epoch,
                    source_identity=self.parent.source_identity,
                    result_identity=(
                        f"{self.operation_id}:{endpoint[0]}:{endpoint[1]}:{endpoint[3]}:{digest}"
                    ),
                    cache_readback=True,
                    cache_identity=f"browser-native:{self.operation_id}",
                    payload_digest=digest,
                ),
            )
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            self._mark_unknown("browser_native_publication_unknown")
            raise RuntimeError("browser_native_publication_unknown") from exc

    def _record_matches(self, record_path: str | Path) -> bool:
        try:
            record = json.loads(Path(record_path).read_text(encoding="utf-8-sig"))
        except (OSError, TypeError, ValueError):
            return False
        return (
            isinstance(record, dict)
            and record.get("owner_pid") == os.getpid()
            and record.get("pid") == getattr(self.process, "pid", None)
            and record.get("port") == getattr(self.server, "port", None)
            and record.get("start_time") == getattr(self.server, "start_time", None)
            and tuple(record.get("endpoint_identity", ())) == tuple(self.endpoint_identity or ())
        )

    def close_server(self, server: Any, record_path: str | Path) -> bool:
        """Stop the exact native scope, retaining the physical receipt until finalization."""
        if self.state not in {"accepted", "unknown"} or server is not self.server:
            self._mark_unknown("browser_native_server_replaced")
            return False
        try:
            retrying_retained_close = self.state == "unknown"
            if not self._reopen_retryable_parent():
                self._mark_unknown("browser_native_close_unknown")
                return False
            self._validate_parent()
            if not self._record_matches(record_path):
                raise RuntimeError("browser_native_record_identity_unknown")
            if type(self.cdp_readback_digest) is not str or not self.cdp_readback_digest:
                raise RuntimeError("browser_native_cdp_readback_missing")
            if (
                not retrying_retained_close
                and self.endpoint_identity != _owned_listener_identity(
                    self.process, server.port, server.start_time,
                )
            ):
                raise RuntimeError("browser_native_endpoint_owner_changed")
            physical = self.physical
            if physical is None or physical.process is not self.process:
                raise RuntimeError("browser_native_physical_identity_unknown")
            if not physical.stop_process_for_retry():
                self._mark_unknown(
                    "browser_native_process_close_unknown", physical_reason="physical_close_failed",
                )
                return False
            readers = (self.stdout_reader, self.stderr_reader)
            if any(reader is None for reader in readers):
                raise RuntimeError("browser_native_reader_missing")
            for stream, reader in zip(("stdout", "stderr"), readers):
                reader.join(timeout=10.0)
                if reader.is_alive() or reader._error is not None:
                    self._mark_unknown("browser_native_reader_join_failed", physical_reason="join_failed")
                    return False
                physical.reader_eof(stream, reader)
                physical.reader_joined(stream, reader)
            if not physical.close_native_after_process():
                self._mark_unknown("browser_native_native_close_unknown", physical_reason="native_close_failed")
                return False
            if _cdp_version_digest(server.cdp_url) is not None:
                raise RuntimeError("browser_native_cdp_still_live")
            self.state = "native_closed"
            self._readback = {
                "operation_id": self.operation_id,
                "source_identity": self.source_identity,
                "pid": self.process.pid,
                "port": server.port,
                "cdp_url": server.cdp_url,
                "start_time": server.start_time,
                "cdp_readback_digest": self.cdp_readback_digest,
                "endpoint_identity": self.endpoint_identity,
                "record_path": str(record_path),
            }
            return True
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self._mark_unknown("browser_native_close_unknown")
            return False

    def abort_process(self) -> bool:
        """Stop a partial launch while retaining this parent as UNKNOWN."""
        physical = self.physical
        process = self.process
        if physical is None or process is None:
            self._mark_unknown("browser_native_partial_launch")
            return False
        try:
            if not physical.stop_process_for_retry():
                self._mark_unknown(
                    "browser_native_partial_close_unknown", physical_reason="physical_close_failed",
                )
                return False
            reader = self.stderr_reader
            if reader is not None:
                reader.join(timeout=10.0)
                if reader.is_alive() or reader._error is not None:
                    self._mark_unknown("browser_native_reader_join_failed")
                    return False
                physical.reader_eof("stderr", reader)
                physical.reader_joined("stderr", reader)
            stdout_reader = self.stdout_reader
            if stdout_reader is not None:
                stdout_reader.join(timeout=10.0)
                if stdout_reader.is_alive() or stdout_reader._error is not None:
                    self._mark_unknown("browser_native_reader_join_failed", physical_reason="join_failed")
                    return False
                physical.reader_eof("stdout", stdout_reader)
                physical.reader_joined("stdout", stdout_reader)
            if not physical.close_native_after_process():
                self._mark_unknown(
                    "browser_native_partial_close_unknown", physical_reason="native_close_failed",
                )
                return False
            self._mark_unknown("browser_native_partial_launch")
            return True
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self._mark_unknown("browser_native_partial_close_unknown")
            return False

    def finish_after_record_removal(self) -> bool:
        """Retire the canonical parent after native/readback proof, before record untracking."""
        if self.state not in {"native_closed", "closed", "unknown"} or self._readback is None:
            self._mark_unknown("browser_native_readback_unknown")
            return False
        try:
            from gateway.runtime_service_operations import close_after_finalizers
            if close_after_finalizers(self.parent) is not True:
                self._mark_unknown(
                    "browser_native_parent_close_unknown", physical_reason="physical_release_failed",
                )
                return False
            self.state = "closed"
            return True
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self._mark_unknown(
                "browser_native_parent_close_unknown", physical_reason="physical_release_failed",
            )
            return False

    def launch(self, argv: list[str], *, log_path: str, env: dict[str, str]) -> Any:
        """Reserve physical custody and create the native child through the exact OS path."""
        physical = self._reserve_physical()
        self._configure_readers(physical)
        log_file = None
        stdout_file = None
        transaction = None
        try:
            log_file = open(log_path, "wb")
            stdout_file = open(f"{log_path}.stdout", "wb")
            if os.name == "nt":
                from hermes_cli._subprocess_compat import windows_hide_flags
                from tools.process_registry_windows import begin_windows_launch
                transaction = begin_windows_launch(physical)
                if not physical.retain_launch_transaction(transaction):
                    raise RuntimeError("browser_native_windows_launch_transaction_rejected")
                process = transaction.launch(
                    argv, text=False, env=env, cwd=os.getcwd(), stdin=subprocess.DEVNULL,
                    stdout=stdout_file, stderr=log_file, encoding=None, errors=None,
                    creationflags=windows_hide_flags(),
                )
            else:
                process = subprocess.Popen(
                    argv, stdin=subprocess.DEVNULL, stdout=stdout_file,
                    stderr=log_file, env=env, start_new_session=True,
                )
            self.bind_process(process, log_path, stdout_path=f"{log_path}.stdout", transaction=transaction)
            return process
        except BaseException:
            if transaction is not None and getattr(transaction, "process", None) is not None:
                physical.retain_partial_launch(transaction.process, transaction, reason="partial_launch")
            try:
                self.abort_process()
            except BaseException:
                logger.exception("Native browser launch cleanup failed; exact custody retained")
                self._mark_unknown("browser_native_launch_abort_unknown")
            self._mark_unknown("browser_native_launch_failed")
            raise
        finally:
            if log_file is not None:
                log_file.close()
            if stdout_file is not None:
                stdout_file.close()


def reserve_lightpanda_backend(task_id: str, session_name: str) -> BrowserNativeBackendReceipt | None:
    """Reserve one external profile parent when the live gateway is present."""
    if type(task_id) is not str or not task_id or type(session_name) is not str or not session_name:
        raise RuntimeError("browser_native_identity_missing")
    parent = None
    try:
        from gateway.run import _gateway_runner_ref
        from gateway.session_authorities import active_authority
        runner = _gateway_runner_ref()
        if runner is None:
            return None
        authority = active_authority(runner)
        if authority is None:
            raise RuntimeError("browser_native_authority_missing")
        from gateway.runtime_sidecar_browser import factory_for_authority
        if factory_for_authority(authority) is None:
            raise RuntimeError("browser_native_factory_missing")
        from gateway.runtime_sidecar_sources import reserve_profile_sidecar
        operation_id = f"browser-native:{task_id}:{session_name}"
        owner = reserve_profile_sidecar(
            runner, profile_home=Path(authority.profile_id), family="browser_native",
            source_identity=f"browser-native:{session_name}", operation_id=operation_id,
        )
        parent = owner.parent
        receipt = BrowserNativeBackendReceipt(
            parent=parent, family="browser_native", source_identity=parent.source_identity,
            operation_id=operation_id, physical=owner.physical,
        )
        return receipt
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        if parent is not None:
            try:
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, "browser_native_reservation_failed")
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                pass
        raise


__all__ = ["BrowserNativeBackendReceipt", "reserve_lightpanda_backend"]
