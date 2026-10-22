"""Custody adapter for shell-hook child processes.

The shell-hook wire contract stays in :mod:`agent.shell_hooks`.  This module only
connects a hook invocation to the existing managed-turn ServiceLease and to the
canonical tool-result writer.  It deliberately keeps live operation state scoped
to the owning agent; the runtime custody coordinator remains the only durable
custody ledger.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import hashlib
import logging
import os
import signal
import subprocess
import threading
import time
import uuid
from typing import Any, Iterator


logger = logging.getLogger(__name__)


class _ShellHookPosixScope:
    """Source-owned process group attached to one shell-hook ``Popen``.

    A bare PGID is only a termination hint.  The service receipt requires a
    contained-scope witness before it can close physical custody, so this
    adapter observes and terminates the exact group created for the hook.
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
            raise OSError("shell-hook process group is not a safe source-owned scope")
        if self.observe_empty() is True:
            return
        # A PGID can be recycled after the leader exits.  Revalidate the exact
        # leader immediately before signalling; an unverifiable identity stays
        # UNKNOWN rather than risking a foreign process group.
        try:
            pid = self.process.pid
            leader_live = pid > 0 and self.process.poll() is None
            leader_group = os.getpgid(pid) if leader_live else None
        except (AttributeError, OSError, ProcessLookupError, TypeError, ValueError):
            leader_live, leader_group = False, None
        if not leader_live or leader_group != self.pgid:
            raise OSError("shell-hook process group leader identity is no longer current")
        killpg(self.pgid, signal.SIGTERM)


_CURRENT_DISPATCH: ContextVar[ShellHookDispatch | None] = ContextVar(
    "shell_hook_dispatch", default=None,
)

_MANAGED_RESULT_EVENTS = frozenset({
    "pre_tool_call", "post_tool_call", "transform_tool_result", "transform_terminal_output",
    "on_stream_start", "on_stream_delta", "on_stream_end", "on_interim_message",
})

_RESERVATION_ACCEPTED_PHASES = frozenset({
    "root_reserved", "request_accepted", "physical_accepted", "published", "unknown",
})
_PREPUBLICATION_RETRY_REASON = "spawn_not_started"


@dataclass
class _ShellHookReservation:
    """Exact in-flight service/request/physical state before operation publication."""

    binding: Any
    owner_id: str
    phase: str = "none"
    service: Any | None = None
    request: Any | None = None
    receipt: Any | None = None
    # The existing ExternalProcessParent is the cross-dispatch carrier for this exact
    # ServiceLease.  It is an object-bound view over the same coordinator handles, never a
    # second reservation ledger.
    parent: Any | None = None
    operation: ShellHookOperation | None = None
    operation_key: tuple[str, ...] | None = None
    cleanup_error: str | None = None

    def accepted(self) -> bool:
        return self.phase in _RESERVATION_ACCEPTED_PHASES


@dataclass
class ShellHookDispatch:
    """Mutable live context shared with a bounded plugin callback worker."""

    agent: Any
    task_id: str
    tool_call_id: str
    turn_id: str
    # The binding is captured from the authenticated turn context at producer time.  A
    # queued stream item may be dropped before ``hook_invocation_scope`` admits a child,
    # so the copied ContextVar alone is not enough to retain that exact owner.
    binding: Any | None = None
    unknown_reason: str | None = None
    operations: dict[tuple[str, str, str], ShellHookOperation] = field(default_factory=dict)
    # Keep exact custody references until publication or proven terminal cleanup.  An empty
    # operation map therefore never becomes evidence that an admitted reservation did not exist.
    reservations: list[_ShellHookReservation] = field(default_factory=list)


@dataclass(eq=False)
class _PipeReader(threading.Thread):
    """One real reader thread retained by the physical receipt."""

    stream_name: str
    stream: Any
    receipt: Any
    output: list[str]

    def __post_init__(self) -> None:
        super().__init__(name=f"hermes-shell-hook-reader-{self.stream_name}", daemon=True)

    def run(self) -> None:
        try:
            value = self.stream.read()
            if isinstance(value, str):
                self.output.append(value)
            else:
                self.receipt.reader_failed(f"{self.stream_name}_reader_type_unknown")
                return
            self.receipt.reader_eof(self.stream_name, self)
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            self.receipt.reader_failed(f"{self.stream_name}_reader_failed:{type(exc).__name__}")


@dataclass
class ShellHookOperation:
    """One exact hook invocation and its existing I16 service children."""

    binding: Any
    service: Any
    request: Any
    receipt: Any
    event: str
    command_key: str
    owner_id: str
    call_id: str
    state: str = "reserved"
    last_error: str | None = None

    @property
    def tool_call_id(self) -> str:
        return self.call_id

    def bind_process(self, process: Any) -> bool:
        """Bind the actual process and native scope before any result is observed."""
        try:
            if self.receipt.state == "unknown" and not self.retry_same_handle():
                self.last_error = self.receipt.unknown_reason or "custody_unknown"
                return False
            transaction = getattr(self.receipt, "transaction", None)
            if transaction is not None:
                if (
                    getattr(transaction, "process", None) is not process
                    or getattr(self.receipt, "scope", None) != "windows_job"
                ):
                    self.receipt.retain_partial_launch(process, transaction, reason="spawn_bind_failed")
                    return False
                if not (
                    getattr(self.receipt, "state", None) == "process_bound"
                    and getattr(self.receipt, "process", None) is process
                ):
                    self.receipt.bind_launch(process, transaction)
            else:
                pid = process.pid
                pgid = os.getpgid(pid)
                if pgid != pid or not self.receipt.bind_process(
                    pid, None, "posix_process_group", f"posix_process_group:{pid}:{pgid}",
                    process_group_id=pgid,
                ):
                    self.receipt.retain_partial_launch(
                        process, reason="spawn_bind_failed", scope="posix_process_group",
                        scope_identity=f"posix_process_group:{pid}:{pgid}", process_group_id=pgid,
                    )
                    return False
                self.receipt.contained_scope_provider = _ShellHookPosixScope(process, pgid)
                self.receipt.bind_launch(process)
            self.receipt.ephemeral_process = True
            self.receipt.managed_witness = {
                "kind": "shell_hook_process",
                "pid": process.pid,
                "scope": self.receipt.scope,
                "reader_streams": ("stdout", "stderr"),
            }
            self.state = "process_bound"
            return True
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.receipt.retain_partial_launch(process, reason="spawn_bind_failed")
            return False
        except BaseException:
            self.receipt.retain_partial_launch(process, reason="spawn_bind_failed")
            self.state = "unknown"
            raise

    def start_readers(self, process: Any) -> tuple[_PipeReader, _PipeReader] | None:
        """Bind and start both actual pipe readers before the child can finish."""
        readers: list[_PipeReader] = []
        try:
            for stream_name in ("stdout", "stderr"):
                stream = getattr(process, stream_name)
                output: list[str] = []
                reader = _PipeReader(stream_name, stream, self.receipt, output)
                # Store the mutable output list on the reader for the collector;
                # the thread itself is the receipt identity.
                reader._output = output
                self.receipt.bind_reader(stream_name, reader)
                readers.append(reader)
            for stream_name, reader in zip(("stdout", "stderr"), readers):
                reader.start()
                if not self.receipt.reader_started(stream_name, reader):
                    return None
            self.state = "readers_active"
            return readers[0], readers[1]
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.receipt.mark_unknown("reader_setup_failed")
            return None

    def communicate(
        self, process: Any, stdin_json: str, timeout: int, kill_tree,
    ) -> dict[str, Any]:
        """Write stdin, wait with the historical timeout, and settle readers."""
        t0 = time.monotonic()
        result: dict[str, Any] = {
            "returncode": None, "stdout": "", "stderr": "", "timed_out": False,
            "elapsed_seconds": 0.0, "error": None,
        }
        readers = self.start_readers(process)
        if readers is None:
            try:
                self.terminate_process(process, kill_tree)
                process.wait(timeout=1)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError, subprocess.TimeoutExpired):
                self.receipt.mark_unknown("partial_launch")
            result["error"] = "shell hook reader setup failed"
            self.last_error = "reader_setup_failed"
            return result
        try:
            process.stdin.write(stdin_json)
            process.stdin.close()
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.receipt.mark_unknown("partial_launch")
            try:
                self.terminate_process(process, kill_tree)
                process.wait(timeout=1)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError, subprocess.TimeoutExpired):
                self.receipt.mark_unknown("partial_launch")
            result["timed_out"] = True
            self.last_error = "partial_launch"
        except BaseException as exc:
            try:
                self.terminate_process(process, kill_tree)
                process.wait(timeout=1)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError, subprocess.TimeoutExpired):
                self.receipt.mark_unknown("partial_launch")
            if not isinstance(exc, Exception):
                raise
            self.receipt.mark_unknown("reader_failed")
            result["error"] = str(exc)
            self.last_error = "reader_failed"
        finally:
            for stream_name, reader in zip(("stdout", "stderr"), readers):
                reader.join(timeout=1)
                if reader.is_alive():
                    self.receipt.mark_unknown("reader_still_alive")
                else:
                    self.receipt.reader_joined(stream_name, reader)
            result["elapsed_seconds"] = round(time.monotonic() - t0, 3)
            try:
                result["returncode"] = process.returncode
            except (AttributeError, OSError, TypeError, ValueError):
                result["returncode"] = None
            result["stdout"] = "".join(getattr(readers[0], "_output", []))
            result["stderr"] = "".join(getattr(readers[1], "_output", []))
            if self.receipt.state == "unknown" and self.last_error is None:
                self.last_error = self.receipt.unknown_reason or "reader_failed"
            elif self.receipt.state != "unknown":
                # A POSIX process group is only a termination hint.  killpg(pgid, 0)
                # cannot prove that a setsid descendant is gone and can address a
                # recycled group after the leader has been reaped.  Keep the output
                # and the original handle, but require a real contained-scope
                # provider before accepting native completion.
                if getattr(self.receipt, "scope", None) == "posix_process_group":
                    provider = getattr(self.receipt, "contained_scope_provider", None)
                    observed = getattr(provider, "observe_empty", None) if provider is not None else None
                    if not callable(observed) or observed() is not True:
                        self.receipt.mark_unknown("posix_containment_unproven")
                        self.last_error = "posix_containment_unproven"
                if self.receipt.state != "unknown":
                    self.receipt.state = "process_bound"
                    self.state = "process_bound"
        return result

    def terminate_process(self, process: Any, kill_tree) -> None:
        """Use the retained native owner for managed cleanup; legacy hooks keep tree-kill."""
        transaction = getattr(self.receipt, "transaction", None)
        if transaction is not None:
            terminate = getattr(transaction, "terminate_owned_tree", None)
            if not callable(terminate):
                self.receipt.mark_unknown("native_cleanup_interrupted")
                return
            try:
                terminate()
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                self.receipt.mark_unknown("native_cleanup_interrupted")
            return
        if getattr(self.receipt, "scope", None) == "posix_process_group":
            # Never signal a PGID after the original leader has disappeared:
            # the number may already belong to an unrelated process group.  A
            # real contained-scope provider is the only source allowed to
            # terminate descendants; without one retain UNKNOWN and terminate
            # only the exact leader while it is still live.
            provider = getattr(self.receipt, "contained_scope_provider", None)
            terminate = getattr(provider, "terminate", None) if provider is not None else None
            if callable(terminate):
                try:
                    terminate()
                except (OSError, RuntimeError, TypeError, ValueError):
                    self.receipt.mark_unknown("posix_containment_unproven")
                return
            try:
                if process.poll() is None:
                    process.terminate()
                    process.wait(timeout=2)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError, subprocess.TimeoutExpired):
                self.receipt.mark_unknown("posix_containment_unproven")
            self.receipt.mark_unknown("posix_containment_unproven")
            return
        kill_tree(process)

    def launch_windows(self, argv: list[str], stdin: Any, stdout: Any, stderr: Any, env: dict[str, str]) -> Any:
        """Launch through the existing suspended Job transaction before the child runs."""
        from hermes_cli._subprocess_compat import windows_hide_flags
        from tools.process_registry_windows import begin_windows_launch

        transaction = begin_windows_launch(self.receipt)
        if self.receipt.retain_launch_transaction(transaction) is not True:
            raise RuntimeError("managed shell-hook launch transaction could not be retained")
        process = transaction.launch(
            argv, text=True, env=env, cwd=None, stdin=stdin, stdout=stdout, stderr=stderr,
            encoding="utf-8", errors="replace", creationflags=windows_hide_flags(),
        )
        if process is None:
            raise RuntimeError("managed shell-hook launch did not return a process")
        try:
            self.receipt.bind_launch(process, transaction)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.receipt.retain_partial_launch(process, transaction, reason="spawn_bind_failed")
            raise
        return process

    def release_without_process(self) -> bool:
        """Terminalize only a reservation whose Popen had no effect."""
        try:
            if self.receipt.release_before_effect() is not True:
                self.last_error = "partial_launch"
                return False
            result = self.service.coordinator.mark_terminal_with_secondary_parents(
                self.request, (self.binding.parent_handle,),
            )
            if result is not None:
                self.last_error = getattr(result, "code", "request_release_failed")
                self.service.unknown = True
                return False
            # ``release_before_effect`` has already terminalized the exact physical
            # handle.  Detach it before closing the service root; otherwise
            # ``ServiceLease.close`` treats a released receipt as an incomplete
            # post-delivery child and turns a verified no-effect Popen failure into
            # sticky unknown custody.
            self.service.physical = None
            self.service.requests.pop(self.binding.admission_id, None)
            self.service.close()
            _retire_operation_parent(getattr(self.service, "external_parent", None), self.binding)
            self.state = "closed"
            return True
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.last_error = "request_release_failed"
            self.service.unknown = True
            return False

    def retry_same_handle(self) -> bool:
        """Reconcile this exact unknown child for a later same-owner hook retry."""
        try:
            return self.service.reconcile_process_failure(self.binding, self.request)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return False


class _MissingCustody(RuntimeError):
    pass


def _retire_operation_parent(parent: Any, binding: Any) -> None:
    """Retire the existing carrier only after its exact service root is terminal."""
    if parent is None:
        return
    parent._request = None
    parent._physical = None
    parent.state = "terminal"
    forget = getattr(getattr(binding, "authority", None), "forget_external_process_finalizer", None)
    if callable(forget) and not forget(parent.operation_id, parent):
        raise RuntimeError("finalizer_registry_retirement_refused")


def _agent_operation_map(agent: Any) -> tuple[threading.Lock, dict[tuple[str, str, str, str, str, str], ShellHookOperation]]:
    lock = getattr(agent, "_shell_hook_custody_lock", None)
    if lock is None:
        lock = threading.RLock()
        agent._shell_hook_custody_lock = lock
    operations = getattr(agent, "_shell_hook_custody_operations", None)
    if operations is None:
        operations = {}
        agent._shell_hook_custody_operations = operations
    return lock, operations


def _operation_key(
    spec: Any, event: str, tool_call_id: str, binding: Any,
) -> tuple[str, str, str, str, str, str]:
    return (
        binding.admission_id,
        str(binding.execution_generation),
        tool_call_id,
        event,
        str(getattr(spec, "command", "")),
        str(getattr(spec, "matcher", "") or ""),
    )


def _agent_turn_binding(agent: Any) -> Any | None:
    """Resolve the exact active owner when a provider callback lost ContextVar state.

    Provider reader/stream threads do not inherit the gateway executor context.  Their
    producer parent already carries the immutable binding that was authenticated before
    the child was started; using that object preserves the owner without deriving one from
    callback payloads.  The live ContextVar remains authoritative when present.
    """
    from gateway.runtime_service_custody import current_turn_binding

    binding = current_turn_binding()
    if binding is not None:
        return binding
    candidates = (
        getattr(getattr(agent, "_external_process_parent", None), "turn_binding", None),
        getattr(getattr(agent, "_codex_session", None), "_active_custody_binding", None),
    )
    return next((candidate for candidate in candidates if candidate is not None), None)


@contextmanager
def tool_dispatch(agent: Any, task_id: str, tool_call_id: str) -> Iterator[ShellHookDispatch]:
    """Expose exact live tool identity to shell callbacks, including bounded workers."""
    dispatch = ShellHookDispatch(
        agent=agent,
        task_id=task_id or "",
        tool_call_id=tool_call_id or "",
        turn_id=getattr(agent, "_current_turn_id", "") or "",
        binding=_agent_turn_binding(agent),
    )
    token = _CURRENT_DISPATCH.set(dispatch)
    try:
        yield dispatch
    finally:
        _CURRENT_DISPATCH.reset(token)


@contextmanager
def stream_dispatch(agent: Any, event: str) -> Iterator[ShellHookDispatch | None]:
    """Carry one live turn identity through a queued stream callback.

    The public stream payload remains unchanged.  An unbound standalone stream keeps the
    historical callback route; a managed turn gets a fresh invocation identity while retaining
    the same admission/generation in the copied context.
    """
    binding = _agent_turn_binding(agent)
    turn_id = getattr(agent, "_current_turn_id", "") or ""
    task_id = getattr(agent, "_current_task_id", "") or ""
    if event not in _MANAGED_RESULT_EVENTS or binding is None or not turn_id or not task_id:
        yield None
        return
    dispatch = ShellHookDispatch(
        agent=agent,
        task_id=task_id,
        tool_call_id=f"stream:{turn_id}:{uuid.uuid4().hex}",
        turn_id=turn_id,
        binding=binding,
    )
    token = _CURRENT_DISPATCH.set(dispatch)
    try:
        yield dispatch
    finally:
        _CURRENT_DISPATCH.reset(token)


def _current_dispatch() -> ShellHookDispatch | None:
    return _CURRENT_DISPATCH.get()


def managed_dispatch_active() -> bool:
    """Whether the copied callback context has an authenticated managed owner."""
    return _current_dispatch() is not None


def _mark_reservation_unknown(reservation: _ShellHookReservation, reason: str) -> None:
    """Retain every exact handle when reservation cleanup is refused or ambiguous."""
    prior_phase = reservation.phase
    reservation.phase = "unknown"
    reservation.cleanup_error = reason
    service = reservation.service
    if service is not None:
        service.unknown = True
    receipt = reservation.receipt
    if receipt is not None and getattr(receipt, "state", None) not in {"unknown", "closed", "released"}:
        try:
            # Only the source-owned pre-publication phase proves that no caller received an
            # operation from which Popen could occur.  Do not infer no-effect from a missing
            # process or an empty history on a reconstructed receipt.
            receipt_reason = reason
            if prior_phase == "physical_accepted" and reservation.operation is None:
                receipt_reason = _PREPUBLICATION_RETRY_REASON
            receipt.mark_unknown(receipt_reason)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, KeyboardInterrupt, SystemExit):
            logger.warning("managed shell-hook physical reservation could not be marked UNKNOWN", exc_info=True)
    operation_parent = reservation.parent
    if operation_parent is not None:
        # Keep the canonical cross-dispatch carrier UNKNOWN as well as the local reservation;
        # its existing exact root/physical references are the only retryable identity.
        try:
            from gateway.runtime_service_operations import mark_unknown as mark_parent_unknown
            mark_parent_unknown(operation_parent, reason)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, KeyboardInterrupt, SystemExit):
            operation_parent.state = "unknown"
            logger.warning("managed shell-hook operation parent could not be marked UNKNOWN", exc_info=True)
    binding = reservation.binding
    coordinator = getattr(service, "coordinator", None) or getattr(binding, "coordinator", None)
    if coordinator is not None:
        handles = (
            getattr(service, "handle", None),
            reservation.request,
            getattr(receipt, "handle", None),
        )
        seen: set[int] = set()
        for handle in handles:
            if handle is None or getattr(handle, "lease", None) is None or id(handle) in seen:
                continue
            seen.add(id(handle))
            try:
                coordinator.mark_unknown(handle, reason)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError, KeyboardInterrupt, SystemExit):
                logger.warning("managed shell-hook reservation handle could not be marked UNKNOWN", exc_info=True)
    # The admission may also contain a persistent provider sidecar (for example, the
    # ACP process that requested this hook).  The authority-level marker walks every
    # sidecar for the admission and would mark that unrelated provider receipt UNKNOWN,
    # making a same-owner shell publication retry fail with ``managed_process_unknown``.
    # The exact shell service/request/physical handles above already retain this child;
    # leave the shared admission carrier untouched until its own delivery fence reports
    # an affected result.


def _handle_record_state(binding: Any, handle: Any) -> str | None:
    """Read one exact coordinator record for reconciliation, never for absence proof."""
    coordinator = getattr(binding, "coordinator", None)
    lock = getattr(coordinator, "_lock", None)
    records = getattr(coordinator, "_records", None)
    lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
    if (
        coordinator is None or lock is None or not isinstance(records, dict)
        or not isinstance(lease_id, str)
        or getattr(handle, "coordinator_id", None) != getattr(coordinator, "_coordinator_id", None)
        or getattr(handle, "profile_id", None) != getattr(binding, "profile_id", None)
        or getattr(handle, "authority_epoch", None) != getattr(binding, "authority_epoch", None)
        or getattr(handle, "process_instance", None) != getattr(binding, "process_instance", None)
    ):
        return None
    with lock:
        record = records.get(lease_id)
        if record is None or getattr(record, "handle", None) is not handle:
            return None
        return getattr(record, "state", None)


def _reconcile_exact_unknown_handle(binding: Any, handle: Any) -> bool:
    """Reopen only the same coordinator handle after the full identity tuple matches."""
    state = _handle_record_state(binding, handle)
    if state in {"accepted", "finalizing"}:
        return True
    if state != "unknown":
        return False
    coordinator = binding.coordinator
    result = coordinator.reconcile_unknown(
        handle,
        owner_id=getattr(handle, "owner_id", None),
        profile_id=binding.profile_id,
        process_instance=binding.process_instance,
        authority_epoch=binding.authority_epoch,
    )
    return result is None and _handle_record_state(binding, handle) in {"accepted", "finalizing"}


def _handle_matches_binding(
    handle: Any, binding: Any, *, owner_class: str | None = None, owner_id: str | None = None,
) -> bool:
    return (
        handle is not None
        and getattr(handle, "coordinator_id", None) == getattr(binding.coordinator, "_coordinator_id", None)
        and getattr(handle, "profile_id", None) == binding.profile_id
        and getattr(handle, "authority_epoch", None) == binding.authority_epoch
        and getattr(handle, "process_instance", None) == binding.process_instance
        and (owner_class is None or getattr(handle, "owner_class", None) == owner_class)
        and (owner_id is None or getattr(handle, "owner_id", None) == owner_id)
    )


def _binding_matches_exact_owner(expected: Any, actual: Any) -> bool:
    """Authenticate a reconstructed binding against the original complete owner tuple."""
    if expected is actual:
        return True
    if expected is None or actual is None:
        return False
    return (
        getattr(expected, "authority", None) is getattr(actual, "authority", None)
        and getattr(expected, "coordinator", None) is getattr(actual, "coordinator", None)
        and getattr(expected, "parent_handle", None) is getattr(actual, "parent_handle", None)
        and getattr(expected, "admission_id", None) == getattr(actual, "admission_id", None)
        and getattr(expected, "execution_generation", None) == getattr(actual, "execution_generation", None)
        and getattr(expected, "session_id", None) == getattr(actual, "session_id", None)
        and getattr(expected, "profile_id", None) == getattr(actual, "profile_id", None)
        and getattr(expected, "authority_epoch", None) == getattr(actual, "authority_epoch", None)
        and getattr(expected, "process_instance", None) == getattr(actual, "process_instance", None)
        and getattr(expected, "principal_id", None) == getattr(actual, "principal_id", None)
        and getattr(expected, "source_identity", None) == getattr(actual, "source_identity", None)
    )


def _external_parent_matches_exact_owner(
    parent: Any, binding: Any, owner_id: str, service: Any | None = None,
) -> bool:
    """Authenticate the existing operation carrier before a new dispatch can reuse it."""
    carried_service = getattr(parent, "_service", None)
    return (
        parent is not None
        and getattr(parent, "mode", None) == "managed_turn"
        and getattr(parent, "operation_id", None) == owner_id
        and getattr(parent, "profile_authority", None) is getattr(binding, "authority", None)
        and getattr(parent, "coordinator", None) is getattr(binding, "coordinator", None)
        and getattr(parent, "profile_id", None) == getattr(binding, "profile_id", None)
        and getattr(parent, "authority_epoch", None) == getattr(binding, "authority_epoch", None)
        and getattr(parent, "process_instance", None) == getattr(binding, "process_instance", None)
        and getattr(parent, "principal_id", None) in {None, getattr(binding, "principal_id", None)}
        and getattr(parent, "source_identity", None) in {
            getattr(binding, "source_identity", None), getattr(binding, "principal_id", None),
        }
        and _binding_matches_exact_owner(getattr(parent, "turn_binding", None), binding)
        and _handle_matches_binding(
            getattr(parent, "root_handle", None), binding,
            owner_class="I16_turn_sidecars", owner_id=owner_id,
        )
        and (service is None or carried_service is service)
        and carried_service is not None
        and getattr(carried_service, "handle", None) is getattr(parent, "root_handle", None)
    )


def _sidecar_matches_exact(binding: Any, service: Any, request: Any) -> bool:
    """Return true only for the already-committed canonical sidecar, not a private flag."""
    sidecars = getattr(getattr(binding, "authority", None), "_preservation_sidecars", {})
    request_id = getattr(getattr(request, "lease", None), "lease_id", None)
    if not isinstance(sidecars, dict) or not isinstance(request_id, str):
        return False
    entry = sidecars.get(request_id)
    return bool(
        isinstance(entry, dict)
        and entry.get("admission_id") == getattr(binding, "admission_id", None)
        and entry.get("parent") is getattr(binding, "parent_handle", None)
        and entry.get("service") is service
        and entry.get("request") is request
    )


def _parent_is_unknown(binding: Any, parent: Any, service: Any) -> bool:
    """Detect a retained ambiguity from typed state; no empty-map or process-none inference."""
    physical = getattr(service, "physical", None)
    return bool(
        getattr(parent, "state", None) == "unknown"
        or getattr(service, "unknown", False)
        or _handle_record_state(binding, getattr(parent, "root_handle", None)) == "unknown"
        or _handle_record_state(binding, getattr(service, "handle", None)) == "unknown"
        or _handle_record_state(binding, getattr(physical, "handle", None)) == "unknown"
    )


def _reservation_matches_exact_owner(
    reservation: _ShellHookReservation, binding: Any, owner_id: str,
    operation_key: tuple[str, ...] | None = None,
) -> bool:
    """Authenticate a retained reservation before any retry can touch custody."""
    if not _binding_matches_exact_owner(reservation.binding, binding) or reservation.owner_id != owner_id:
        return False
    if reservation.operation_key is not None and operation_key is not None and reservation.operation_key != operation_key:
        return False
    service = reservation.service
    request = reservation.request
    receipt = reservation.receipt
    parent = getattr(binding, "parent_handle", None)
    operation_parent = reservation.parent
    if (
        service is None
        or getattr(service, "coordinator", None) is not binding.coordinator
        or not _binding_matches_exact_owner(getattr(service, "binding", None), binding)
        or not _handle_matches_binding(service.handle, binding, owner_class="I16_turn_sidecars", owner_id=owner_id)
        or not _handle_matches_binding(parent, binding, owner_class="I04_fifo_turn_execution")
        or not _external_parent_matches_exact_owner(operation_parent, binding, owner_id, service)
    ):
        return False
    if request is not None:
        expected_request_id = f"{owner_id}:request:{binding.admission_id}"
        if (
            getattr(service, "requests", {}).get(binding.admission_id) is not request
            or not _handle_matches_binding(request, binding, owner_class="I16_turn_sidecars", owner_id=expected_request_id)
            or getattr(getattr(request, "lease", None), "parent_lease_id", None)
            != getattr(getattr(service.handle, "lease", None), "lease_id", None)
        ):
            return False
    if receipt is not None:
        expected_physical_id = f"{owner_id}:physical"
        if (
            getattr(service, "physical", None) is not receipt
            or getattr(receipt, "service", None) is not service
            or not _handle_matches_binding(receipt.handle, binding, owner_class="I16_turn_sidecars", owner_id=expected_physical_id)
            or getattr(getattr(receipt, "handle", None), "lease", None) is None
            or getattr(getattr(receipt.handle, "lease", None), "parent_lease_id", None)
            != getattr(getattr(service.handle, "lease", None), "lease_id", None)
        ):
            return False
    elif getattr(service, "physical", None) is not None:
        return False
    return True


def _sidecar_is_unknown(binding: Any, entry: Any) -> bool:
    """Classify only an exact retained sidecar as unknown; no empty map is consulted."""
    if not isinstance(entry, dict) or entry.get("admission_id") != binding.admission_id:
        return False
    service = entry.get("service")
    request = entry.get("request")
    if service is None or request is None or entry.get("parent") is not binding.parent_handle:
        return False
    physical = getattr(service, "physical", None)
    return bool(
        getattr(service, "unknown", False)
        or _handle_record_state(binding, getattr(service, "handle", None)) == "unknown"
        or _handle_record_state(binding, request) == "unknown"
        or (physical is not None and getattr(physical, "state", None) == "unknown")
        or (physical is not None and _handle_record_state(binding, getattr(physical, "handle", None)) == "unknown")
    )


def _retained_from_dispatch(
    dispatch: ShellHookDispatch, binding: Any, owner_id: str, operation_key: tuple[str, ...],
) -> tuple[bool, _ShellHookReservation | None]:
    """Read the current callback's exact retained tuple before any cross-dispatch lookup."""
    unknown_dispatch_reservation = False
    for reservation in tuple(dispatch.reservations):
        if not _binding_matches_exact_owner(reservation.binding, binding) or reservation.phase != "unknown":
            continue
        unknown_dispatch_reservation = True
        if reservation.owner_id != owner_id:
            continue
        if not _reservation_matches_exact_owner(reservation, binding, owner_id, operation_key):
            raise _MissingCustody("shell_hook_reservation_owner_mismatch")
        return True, reservation
    return unknown_dispatch_reservation, None


def _registered_external_parents(binding: Any) -> tuple[Any, ...]:
    """Read the authority's exact carrier index, preserving registry failures as refusal."""
    finalizers = getattr(getattr(binding, "authority", None), "external_process_finalizers", None)
    if not callable(finalizers):
        return ()
    try:
        return tuple(finalizers())
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        raise _MissingCustody("shell_hook_reservation_registry_unavailable")


def _retained_from_external_parents(
    parents: tuple[Any, ...], binding: Any, owner_id: str, operation_key: tuple[str, ...],
) -> _ShellHookReservation | None:
    """Recover an exact UNKNOWN parent from the authority finalizer index."""
    for operation_parent in parents:
        carried_binding = getattr(operation_parent, "turn_binding", None)
        if not _binding_matches_exact_owner(carried_binding, binding):
            continue
        operation_id = getattr(operation_parent, "operation_id", None)
        if operation_id != owner_id:
            # A turn binding can legitimately carry provider-owned parents beside a
            # shell-hook operation.  Their UNKNOWN state does not authenticate a
            # shell-hook retry.  An untyped UNKNOWN carrier remains fail-closed because
            # there is no owner identity we can safely exclude.
            if getattr(operation_parent, "state", None) == "unknown" and not operation_id:
                raise _MissingCustody("shell_hook_reservation_owner_unknown")
            continue
        service = getattr(operation_parent, "_service", None)
        if not _external_parent_matches_exact_owner(operation_parent, binding, owner_id, service):
            raise _MissingCustody("shell_hook_reservation_owner_mismatch")
        if getattr(operation_parent, "state", None) == "terminal":
            continue
        if not _parent_is_unknown(binding, operation_parent, service):
            raise _MissingCustody("shell_hook_reservation_owner_busy")
        reservation = _ShellHookReservation(
            binding=carried_binding or binding,
            owner_id=owner_id,
            phase="unknown",
            service=service,
            request=getattr(operation_parent, "_request", None),
            receipt=getattr(operation_parent, "_physical", None) or getattr(service, "physical", None),
            parent=operation_parent,
            operation_key=operation_key,
            cleanup_error="retained_unknown",
        )
        if not _reservation_matches_exact_owner(reservation, binding, owner_id, operation_key):
            raise _MissingCustody("shell_hook_reservation_owner_mismatch")
        return reservation
    return None


def _retained_from_sidecars(
    parents: tuple[Any, ...], binding: Any, owner_id: str, operation_key: tuple[str, ...],
) -> _ShellHookReservation | None:
    """Use sidecars only as an index after an exact typed parent was found."""
    sidecars = getattr(getattr(binding, "authority", None), "_preservation_sidecars", {})
    if not isinstance(sidecars, dict):
        return None
    for entry in tuple(sidecars.values()):
        if not _sidecar_is_unknown(binding, entry):
            continue
        service = entry.get("service")
        service_owner_id = getattr(service, "owner_id", None)
        if service_owner_id != owner_id:
            # Other managed services share the admission sidecar index.  Only an
            # explicitly identified foreign owner can be ignored; an ownerless
            # UNKNOWN sidecar is ambiguous and must still refuse closed.
            if not isinstance(service_owner_id, str) or not service_owner_id:
                raise _MissingCustody("shell_hook_reservation_owner_unknown")
            continue
        operation_parent = next(
            (
                candidate for candidate in parents
                if getattr(candidate, "_service", None) is service
                and getattr(candidate, "operation_id", None) == owner_id
            ),
            None,
        )
        if operation_parent is None:
            raise _MissingCustody("shell_hook_reservation_carrier_missing")
        reservation = _ShellHookReservation(
            binding=getattr(operation_parent, "turn_binding", None) or binding,
            owner_id=owner_id,
            phase="unknown",
            service=service,
            request=entry.get("request"),
            receipt=getattr(service, "physical", None),
            parent=operation_parent,
            operation_key=operation_key,
            cleanup_error="retained_unknown",
        )
        if not _reservation_matches_exact_owner(reservation, binding, owner_id, operation_key):
            raise _MissingCustody("shell_hook_reservation_owner_mismatch")
        return reservation
    return None


def _retained_reservation_for_retry(
    dispatch: ShellHookDispatch, binding: Any, owner_id: str, operation_key: tuple[str, ...],
) -> _ShellHookReservation | None:
    """Find the canonical exact retained tuple, or refuse before a successor root."""
    found, reservation = _retained_from_dispatch(dispatch, binding, owner_id, operation_key)
    if found:
        return reservation
    parents = _registered_external_parents(binding)
    reservation = _retained_from_external_parents(parents, binding, owner_id, operation_key)
    return reservation or _retained_from_sidecars(parents, binding, owner_id, operation_key)


def _reconcile_physical_reservation(
    reservation: _ShellHookReservation, binding: Any, service: Any, request: Any,
    receipt: Any, operation_parent: Any,
) -> bool:
    """Reset only a pre-publication receipt whose exact sidecar is still present."""
    if (
        getattr(receipt, "state", None) != "unknown"
        or getattr(receipt, "unknown_reason", None) != _PREPUBLICATION_RETRY_REASON
        or getattr(receipt, "process", None) is not None
        or getattr(receipt, "transaction", None) is not None
        or getattr(receipt, "durable_proofs", {})
        or getattr(receipt, "managed_child_history", [])
        or not getattr(receipt, "ephemeral_process", False)
    ):
        return False
    if not _sidecar_matches_exact(binding, service, request):
        _mark_reservation_unknown(reservation, "preservation_owner_unmapped")
        return False
    retry_binding = getattr(service, "binding", None) or binding
    if not service.reconcile_process_failure(retry_binding, request):
        _mark_reservation_unknown(reservation, "reservation_reconcile_failed")
        return False
    if operation_parent is not None:
        operation_parent.turn_binding = binding
        operation_parent._request = request
        operation_parent._physical = receipt
        operation_parent.state = "physical_bound"
    return True


def _reconcile_root_reservation(
    reservation: _ShellHookReservation, binding: Any, service: Any, request: Any,
    operation_parent: Any,
) -> bool:
    """Reopen the exact accepted root and rebuild only its missing typed children."""
    for handle in (service.handle, request, binding.parent_handle):
        if handle is not None and not _reconcile_exact_unknown_handle(binding, handle):
            _mark_reservation_unknown(reservation, "reservation_reconcile_failed")
            return False
    service.unknown = False
    if request is None:
        request = service.begin_request(binding)
        reservation.request = request
        if operation_parent is not None:
            operation_parent.turn_binding = binding
            operation_parent._request = request
        reservation.phase = "request_accepted"
    elif operation_parent is not None:
        operation_parent.turn_binding = binding
        operation_parent._request = request
    if not _sidecar_matches_exact(binding, service, request):
        binder = getattr(binding.authority, "_preservation_bind_sidecar", None)
        if not callable(binder):
            _mark_reservation_unknown(reservation, "preservation_owner_unmapped")
            return False
        try:
            binder(binding.admission_id, binding.parent_handle, service, request)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            if not _sidecar_matches_exact(binding, service, request):
                _mark_reservation_unknown(reservation, "reservation_reconcile_failed")
                return False
    if not _sidecar_matches_exact(binding, service, request):
        _mark_reservation_unknown(reservation, "reservation_reconcile_failed")
        return False
    from gateway.runtime_service_operations import reserve_physical as reserve_external_physical
    if operation_parent is None:
        _mark_reservation_unknown(reservation, "shell_hook_reservation_carrier_missing")
        return False
    receipt = reserve_external_physical(operation_parent)
    receipt.ephemeral_process = True
    reservation.receipt = receipt
    operation_parent._physical = receipt
    operation_parent.state = "physical_bound"
    return True


def _reconcile_retained_reservation(
    reservation: _ShellHookReservation, binding: Any, owner_id: str,
    operation_key: tuple[str, ...],
) -> bool:
    """Reconcile the retained service/request/receipt tuple, never a newly allocated root."""
    if (
        reservation.phase != "unknown"
        or not _reservation_matches_exact_owner(reservation, binding, owner_id, operation_key)
    ):
        return False
    service, request, receipt, operation_parent = (
        reservation.service, reservation.request, reservation.receipt, reservation.parent,
    )
    try:
        if receipt is not None:
            if not _reconcile_physical_reservation(
                reservation, binding, service, request, receipt, operation_parent,
            ):
                return False
        elif not _reconcile_root_reservation(
            reservation, binding, service, request, operation_parent,
        ):
            return False
        request = reservation.request
        for handle in (request, binding.parent_handle):
            if handle is not None and not _reconcile_exact_unknown_handle(binding, handle):
                _mark_reservation_unknown(reservation, "reservation_reconcile_failed")
                return False
        service.unknown = False
        reservation.phase = "physical_accepted"
        reservation.cleanup_error = None
        return True
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        _mark_reservation_unknown(reservation, "reservation_reconcile_failed")
        return False
    except (KeyboardInterrupt, SystemExit):
        _mark_reservation_unknown(reservation, "reservation_reconcile_interrupted")
        return False


def _rollback_failed_reservation(reservation: _ShellHookReservation) -> bool:
    """Retire an exact no-effect tuple; retain every accepted handle on any refusal."""
    service = reservation.service
    if service is None:
        reservation.phase = "proven_closed"
        reservation.cleanup_error = None
        return True
    binding = reservation.binding
    try:
        # Request terminalization is attempted before physical release so a refusal retains
        # the exact request and receipt for the same-owner reconciliation path.
        if reservation.request is not None:
            result = service.coordinator.mark_terminal_with_secondary_parents(
                reservation.request, (binding.parent_handle,),
            )
            if result is not None:
                raise RuntimeError(getattr(result, "code", "request_release_failed"))
        receipt = reservation.receipt
        if receipt is not None:
            if receipt.release_before_effect() is not True:
                raise RuntimeError("partial_launch")
            # ``release_before_effect`` proved the exact physical handle had no effect.
            service.physical = None
        if reservation.request is not None:
            service.requests.pop(binding.admission_id, None)
        service.close()
        _retire_operation_parent(reservation.parent, binding)
    except BaseException as exc:
        reason = str(exc) or "reservation_cleanup_unknown"
        _mark_reservation_unknown(reservation, reason)
        logger.warning("managed shell-hook reservation cleanup retained UNKNOWN custody", exc_info=True)
        return False
    reservation.phase = "proven_closed"
    reservation.cleanup_error = None
    return True


def pre_tool_block_message(
    agent: Any, function_name: str, function_args: dict, effective_task_id: str,
    tool_call_id: str | None, middleware_trace: list,
) -> tuple[Any, dict]:
    """Run the legacy pre-tool hook resolver without weakening its fail-open policy."""
    try:
        from hermes_cli.plugins import _dispatch_pre_tool_call_hooks

        block_message, modified_args = _dispatch_pre_tool_call_hooks(
            function_name, function_args, task_id=effective_task_id or "",
            session_id=getattr(agent, "session_id", "") or "", tool_call_id=tool_call_id or "",
            turn_id=getattr(agent, "_current_turn_id", "") or "",
            api_request_id=getattr(agent, "_current_api_request_id", "") or "",
            middleware_trace=list(middleware_trace),
        )
        return block_message, (modified_args if modified_args is not None else function_args)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None, function_args


def _publish_shell_operation(
    reservation: _ShellHookReservation, dispatch: ShellHookDispatch,
    operations: dict[tuple[str, str, str, str, str, str], ShellHookOperation],
    full_key: tuple[str, ...], dispatch_key: tuple[str, str, str],
    event: str, digest: str, tool_call_id: str,
) -> ShellHookOperation:
    """Publish one exact retained tuple after every fallible admission step succeeded."""
    if reservation.service is None or reservation.request is None or reservation.receipt is None:
        raise _MissingCustody("shell_hook_reservation_incomplete")
    operation = ShellHookOperation(
        binding=reservation.binding, service=reservation.service,
        request=reservation.request, receipt=reservation.receipt,
        event=event, command_key=digest, owner_id=reservation.owner_id,
        call_id=tool_call_id,
    )
    reservation.operation = operation
    # Publication is the last fallible boundary.  Keep the exact reservation accepted
    # until both ephemeral maps contain this same operation.
    operations[full_key] = operation
    dispatch.operations[dispatch_key] = operation
    reservation.phase = "published"
    return operation


def _managed_operation_context(event: str) -> tuple[ShellHookDispatch | None, Any, Any]:
    """Resolve the authenticated producer context; legacy standalone hooks return empty."""
    from agent.shell_hook_scope import current_hook_is_managed, current_hook_parent
    from gateway.runtime_service_custody import current_turn_binding
    dispatch = _current_dispatch()
    parent = current_hook_parent()
    binding = (
        getattr(parent, "owner", None)
        if parent is not None and parent.is_turn
        else getattr(dispatch, "binding", None) or current_turn_binding()
    )
    if parent is None and binding is None:
        # No source-owned parent means this is the explicitly standalone legacy
        # route.  Its historical raw Popen semantics stay intact.
        if current_hook_is_managed():
            raise _MissingCustody("shell_hook_parent_missing")
        return None, None, None
    if dispatch is None:
        raise _MissingCustody("shell_hook_dispatch_missing")
    if parent is not None and not parent.is_turn:
        raise _MissingCustody("hook_service_owner_unmapped")
    if event not in _MANAGED_RESULT_EVENTS:
        raise _MissingCustody("hook_result_consumer_unmapped")
    if binding is None:
        raise _MissingCustody("shell_hook_parent_missing")
    return dispatch, binding, parent


def _operation_identity(
    spec: Any, event: str, dispatch: ShellHookDispatch, binding: Any,
) -> tuple[str, tuple[str, ...], tuple[str, ...], str, str]:
    """Build the operation identity solely from the authenticated dispatch context."""
    # The dispatch context is created by the actual tool executor.  Callback kwargs are
    # untrusted wire data and may never manufacture a custody identity.
    tool_call_id = dispatch.tool_call_id
    turn_id = dispatch.turn_id
    if not tool_call_id or not turn_id:
        raise _MissingCustody("shell_hook_identity_missing")
    key = _operation_key(spec, event, tool_call_id, binding)
    full_key = (*key,)
    digest = hashlib.sha256(
        f"{event}\n{getattr(spec, 'command', '')}\n{getattr(spec, 'matcher', '') or ''}".encode(),
    ).hexdigest()[:20]
    owner_id = f"shell-hook:{binding.admission_id}:{tool_call_id}:{digest}"
    return tool_call_id, key, full_key, owner_id, digest


def _existing_operation(
    dispatch: ShellHookDispatch, operations: dict, full_key: tuple[str, ...],
    dispatch_key: tuple[str, ...],
) -> ShellHookOperation | None:
    """Reuse one live operation, never a closed map entry."""
    existing = dispatch.operations.get(dispatch_key)
    if existing is not None:
        if dispatch.unknown_reason is not None:
            raise _MissingCustody(dispatch.unknown_reason)
        if existing.state not in {"closed", "released"}:
            return existing
        dispatch.operations.pop(dispatch_key, None)
    existing = operations.get(full_key)
    if existing is None:
        return None
    if dispatch.unknown_reason is not None:
        raise _MissingCustody(dispatch.unknown_reason)
    if existing.state not in {"closed", "released"}:
        dispatch.operations[dispatch_key] = existing
        return existing
    operations.pop(full_key, None)
    return None


def _retry_retained_operation(
    reservation: _ShellHookReservation | None, dispatch: ShellHookDispatch, operations: dict,
    binding: Any, owner_id: str, full_key: tuple[str, ...], dispatch_key: tuple[str, ...],
    event: str, digest: str, tool_call_id: str,
) -> ShellHookOperation | None:
    """Reconcile one exact retained carrier before any reserve_service call."""
    if reservation is None:
        return None
    if reservation not in dispatch.reservations:
        dispatch.reservations.append(reservation)
    if not _reconcile_retained_reservation(reservation, binding, owner_id, full_key):
        raise _MissingCustody("shell_hook_reservation_reconcile_failed")
    try:
        return _publish_shell_operation(
            reservation, dispatch, operations, full_key, dispatch_key,
            event, digest, tool_call_id,
        )
    except BaseException:
        operations.pop(full_key, None)
        dispatch.operations.pop(dispatch_key, None)
        _rollback_failed_reservation(reservation)
        raise


def _reserve_new_operation(
    dispatch: ShellHookDispatch, binding: Any, owner_id: str, full_key: tuple[str, ...],
    dispatch_key: tuple[str, ...], event: str, digest: str, tool_call_id: str,
    operations: dict,
) -> ShellHookOperation:
    """Reserve, bind, and publish one new exact shell operation."""
    from gateway.runtime_service_custody import ServiceCustodyError, reserve_service
    from gateway.runtime_service_operations import (
        parent_for_managed_service,
        reserve_physical as reserve_external_physical,
    )
    reservation = _ShellHookReservation(binding=binding, owner_id=owner_id, operation_key=full_key)
    dispatch.reservations.append(reservation)
    try:
        service = reserve_service(binding, owner_id)
        reservation.service = service
        reservation.phase = "root_reserved"
        operation_parent = parent_for_managed_service(
            binding, service, operation_id=owner_id,
            delivery_lifecycle="per_invocation",
        )
        register = getattr(binding.authority, "register_external_process_finalizer", None)
        if not callable(register) or register(owner_id, operation_parent) is not operation_parent:
            raise ServiceCustodyError("finalizer_registry_missing")
        reservation.parent = operation_parent
        request = service.begin_request(binding)
        reservation.request = request
        reservation.phase = "request_accepted"
        operation_parent.turn_binding = binding
        operation_parent._request = request
        binder = getattr(binding.authority, "_preservation_bind_sidecar", None)
        if not callable(binder):
            raise ServiceCustodyError("preservation_owner_unmapped")
        binder(binding.admission_id, binding.parent_handle, service, request)
        receipt = reserve_external_physical(operation_parent)
        reservation.receipt = receipt
        reservation.phase = "physical_accepted"
        receipt.ephemeral_process = True
        return _publish_shell_operation(
            reservation, dispatch, operations, full_key, dispatch_key,
            event, digest, tool_call_id,
        )
    except BaseException as first_error:
        operations.pop(full_key, None)
        dispatch.operations.pop(dispatch_key, None)
        retained = not _rollback_failed_reservation(reservation)
        if isinstance(first_error, Exception) and retained:
            try:
                if _reconcile_retained_reservation(reservation, binding, owner_id, full_key):
                    return _publish_shell_operation(
                        reservation, dispatch, operations, full_key, dispatch_key,
                        event, digest, tool_call_id,
                    )
            except BaseException as retry_error:
                operations.pop(full_key, None)
                dispatch.operations.pop(dispatch_key, None)
                _rollback_failed_reservation(reservation)
                raise first_error from retry_error
        raise


def begin_operation(spec: Any, event: str, kwargs: dict[str, Any]) -> ShellHookOperation | None:
    """Reserve or reconcile one exact managed operation; legacy routes return ``None``."""
    dispatch, binding, _parent = _managed_operation_context(event)
    if dispatch is None:
        return None
    tool_call_id, key, full_key, owner_id, digest = _operation_identity(spec, event, dispatch, binding)
    dispatch_key = key[3:]
    lock, operations = _agent_operation_map(dispatch.agent)
    with lock:
        existing = _existing_operation(dispatch, operations, full_key, dispatch_key)
        if existing is not None:
            return existing

        # A previous publication boundary may have left an UNKNOWN tuple in this same
        # dispatch or in the canonical sidecar registry.  Authenticate and reconcile that
        # tuple before considering reserve_service; absence is never permission for a root.
        reservation = _retained_reservation_for_retry(dispatch, binding, owner_id, full_key)
        retried = _retry_retained_operation(
            reservation, dispatch, operations, binding, owner_id, full_key, dispatch_key,
            event, digest, tool_call_id,
        )
        if retried is not None:
            return retried
        if dispatch.unknown_reason is not None:
            # This dispatch has already crossed a failure boundary, but no exact owner
            # tuple was authenticated.  Refuse before allocating any successor root.
            raise _MissingCustody(dispatch.unknown_reason)

        return _reserve_new_operation(
            dispatch, binding, owner_id, full_key, dispatch_key,
            event, digest, tool_call_id, operations,
        )


def _ops_for_tool(agent: Any, tool_call_id: str, binding: Any) -> list[ShellHookOperation]:
    lock, operations = _agent_operation_map(agent)
    with lock:
        selected: list[ShellHookOperation] = []
        for key, op in list(operations.items()):
            if op.state in {"closed", "released"}:
                operations.pop(key, None)
            elif (
                key[0] == binding.admission_id
                and key[1] == str(binding.execution_generation)
                and key[2] == tool_call_id
            ):
                selected.append(op)
        return selected


def _remove_operations(agent: Any, target: list[ShellHookOperation]) -> None:
    lock, operations = _agent_operation_map(agent)
    target_ids = {id(op) for op in target}
    with lock:
        for key, op in list(operations.items()):
            if id(op) in target_ids:
                operations.pop(key, None)


def _final_turn_proof(agent: Any, messages: list[dict[str, Any]], writer_proof: Any):
    """Read every committed row in the current turn and build its typed witness.

    The tool-result seam runs before the model's final assistant row exists.  The
    durable custody fence therefore belongs to the final session persistence
    boundary, where this function reads the complete user-to-final-response
    suffix back from SessionDB.
    """
    from agent.context_compressor import _DB_PERSISTED_MARKER
    from agent.session_persistence_proof import _proof_from_validated_rows, _read_expected_rows
    from agent.message_metadata import DB_ROW_SNAPSHOT
    from gateway.runtime_service_custody import DurableResultProof

    if writer_proof is None:
        return None
    try:
        anchor = max(i for i, item in enumerate(messages) if item.get("role") == "user")
    except (StopIteration, AttributeError, TypeError, ValueError):
        return None
    turn_messages = messages[anchor:]
    expected: list[tuple[int, str, str]] = []
    for message in turn_messages:
        if not isinstance(message, dict) or message.get(_DB_PERSISTED_MARKER) is not True:
            return None
        row_id = message.get("_row_id")
        role = message.get("role")
        snapshot = message.get(DB_ROW_SNAPSHOT)
        if type(row_id) is not int or row_id <= 0 or type(role) is not str or not isinstance(snapshot, str) or not snapshot:
            return None
        expected.append((row_id, role, snapshot))
    session_id = getattr(agent, "session_id", None)
    store_path = getattr(writer_proof, "store_path", None)
    if not isinstance(store_path, str):
        database = getattr(writer_proof, "database", None)
        raw_path = getattr(database, "db_path", None)
        if raw_path is not None and not isinstance(raw_path, bool):
            try:
                from pathlib import Path
                store_path = str(Path(raw_path).expanduser().resolve())
            except (OSError, TypeError, ValueError):
                store_path = None
    if not isinstance(session_id, str) or not session_id or not isinstance(store_path, str) or not store_path:
        return None
    checked = _read_expected_rows(agent, session_id, tuple(expected))
    if checked is None:
        return None
    proof = _proof_from_validated_rows(store_path, session_id, checked)
    result_row_ids = tuple(row_id for row_id, _role, _snapshot in checked[1:])
    if (
        not result_row_ids
        or checked[0][1] != "user"
        or any(role not in {"assistant", "tool"} for _row_id, role, _snapshot in checked[1:])
    ):
        return None
    return DurableResultProof(
        store_path=proof.store_path, session_id=proof.session_id,
        admission_id="", execution_generation=0,
        row_ids=proof.row_ids, row_snapshots=proof.row_snapshots,
        row_sessions=proof.row_sessions, row_roles=proof.row_roles,
        payload_digest=proof.payload_digest, result_row_ids=result_row_ids,
    )


def _fill_binding_fields(proof: Any, binding: Any) -> Any:
    from dataclasses import replace
    return replace(
        proof, admission_id=binding.admission_id,
        execution_generation=binding.execution_generation,
    )


def commit_tool_result(
    agent: Any, messages: list[dict[str, Any]], tool_message: dict[str, Any],
    *, tool_call_id: str, writer_proof: Any = None,
) -> bool | None:
    """Stage shell-hook receipts until the final assistant row is canonical.

    The managed writer callback has already read back the tool row here, but the
    public result still lacks the final assistant row.  Recording a terminal
    proof at this seam would make the later delivery check reject the exact
    complete result.  Keep the authenticated physical children retained and
    finish them from :func:`commit_turn_result`.
    """
    from gateway.runtime_service_custody import current_turn_binding
    binding = current_turn_binding()
    if binding is None:
        return None
    operations = _ops_for_tool(agent, tool_call_id, binding)
    if not operations:
        return None
    if any(op.state not in {"reserved", "process_bound", "readers_active"} or op.receipt.state != "process_bound" for op in operations):
        return False
    try:
        for operation in operations:
            operation.state = "delivery_staged"
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError, _MissingCustody) as exc:
        logger.warning("shell hook custody remains pending after result: %s:%s", type(exc).__name__, exc, exc_info=True)
        for operation in operations:
            if operation.state != "closed":
                operation.last_error = str(exc) or "durable_result_unknown"
                if operation.receipt.state not in {"unknown", "closed", "released"}:
                    operation.receipt.mark_unknown("durable_result_unknown")
                operation.state = "unknown"
        return False
    return True


def _pending_shell_operations(agent: Any, binding: Any) -> list[ShellHookOperation]:
    """Return exact shell children whose sidecar still needs the final proof."""
    lock, operations = _agent_operation_map(agent)
    sidecars = getattr(getattr(binding, "authority", None), "_preservation_sidecars", {})
    selected: list[ShellHookOperation] = []
    with lock:
        for key, operation in list(operations.items()):
            if (
                key[0] != binding.admission_id
                or key[1] != str(binding.execution_generation)
                or operation.binding.authority is not binding.authority
                or operation.binding.coordinator is not binding.coordinator
                or operation.binding.profile_id != binding.profile_id
                or operation.binding.authority_epoch != binding.authority_epoch
                or operation.binding.process_instance != binding.process_instance
                or operation.binding.session_id != binding.session_id
                or operation.state in {"closed", "released"}
            ):
                continue
            request_id = getattr(getattr(operation.request, "lease", None), "lease_id", None)
            entry = sidecars.get(request_id) if isinstance(sidecars, dict) else None
            if (
                isinstance(entry, dict)
                and entry.get("service") is operation.service
                and entry.get("request") is operation.request
                and entry.get("admission_id") == binding.admission_id
                and entry.get("terminal") is not True
                and entry.get("result_durable") is not True
            ):
                selected.append(operation)
    return selected


def has_pending_turn(agent: Any) -> bool:
    """Whether this exact live turn has a shell child awaiting final durability."""
    from gateway.runtime_service_custody import current_turn_binding
    binding = current_turn_binding()
    return binding is not None and bool(_pending_shell_operations(agent, binding))


def mark_pending_unknown(agent: Any, reason: str = "durable_result_unknown") -> None:
    """Keep every pending shell tuple sticky-unknown after a proof failure."""
    from gateway.runtime_service_custody import current_turn_binding
    binding = current_turn_binding()
    if binding is None:
        return
    operations = _pending_shell_operations(agent, binding)
    for operation in operations:
        if operation.receipt.state not in {"unknown", "closed", "released"}:
            operation.receipt.mark_unknown(reason)
        operation.last_error = reason
        operation.state = "unknown"
    try:
        binding.authority._preservation_mark_unknown(binding.admission_id, reason)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        logger.warning("shell-hook custody unknown marker failed", exc_info=True)


def commit_turn_result(agent: Any, messages: list[dict[str, Any]], writer_proof: Any) -> bool | None:
    """Attach the complete current-turn readback proof to every shell child."""
    from gateway.runtime_service_custody import current_turn_binding
    binding = current_turn_binding()
    if binding is None:
        return None
    operations = _pending_shell_operations(agent, binding)
    if not operations:
        return None
    try:
        durable = _final_turn_proof(agent, messages, writer_proof)
        if durable is None:
            raise _MissingCustody("durable_result_unknown")
        sidecars = getattr(binding.authority, "_preservation_sidecars", {})
        for operation in operations:
            request_id = getattr(getattr(operation.request, "lease", None), "lease_id", None)
            entry = sidecars.get(request_id) if isinstance(sidecars, dict) else None
            if not isinstance(entry, dict) or entry.get("service") is not operation.service:
                raise _MissingCustody("preservation_owner_unmapped")
            if entry.get("result_durable") is True:
                continue
            proof = _fill_binding_fields(durable, operation.binding)
            operation.service.record_result_durable(operation.binding, operation.request, proof)
            operation.binding.authority._preservation_release_sidecar(
                operation.binding.admission_id, operation.request, durable=True,
            )
            operation.state = "durable"
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError, _MissingCustody) as exc:
        logger.warning("shell hook custody remains pending after final persistence: %s", type(exc).__name__)
        mark_pending_unknown(agent, str(exc) or "durable_result_unknown")
        return False
    _remove_operations(agent, operations)
    return True


def commit_tool_result_delivery(
    agent: Any, messages: list[dict[str, Any]], tool_message: dict[str, Any], *,
    tool_call_id: str, function_result: Any, persisted_result: Any, stage: str,
) -> tuple[bool | None, bool]:
    """Share one managed writer proof with shell children before parent delivery."""
    from agent.tool_result_custody import commit_managed_tool_result_if_needed

    def _commit_child_after_writer(writer_proof: Any) -> bool | None:
        return commit_tool_result(
            agent, messages, tool_message, tool_call_id=tool_call_id, writer_proof=writer_proof,
        )

    managed = commit_managed_tool_result_if_needed(
        agent, messages, tool_message, tool_call_id=tool_call_id,
        function_result=function_result, persisted_result=persisted_result,
        stage=stage, before_settle=_commit_child_after_writer,
    )
    if managed is False:
        return False, False
    if managed is not None:
        return managed, False
    shell = commit_tool_result(
        agent, messages, tool_message, tool_call_id=tool_call_id,
    )
    if shell is False:
        return False, False
    return None, shell is True


def process_result(operation: ShellHookOperation, process: Any, stdin_json: str, timeout: int, kill_tree) -> dict[str, Any]:
    """Bind and run one already-reserved process; no caller may Popen first."""
    try:
        if not operation.bind_process(process):
            try:
                operation.terminate_process(process, kill_tree)
                process.wait(timeout=1)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError, subprocess.TimeoutExpired):
                pass
            operation.last_error = "spawn_bind_failed"
            return {
                "returncode": getattr(process, "returncode", None), "stdout": "", "stderr": "",
                "timed_out": False, "elapsed_seconds": 0.0, "error": "shell hook process bind failed",
            }
        return operation.communicate(process, stdin_json, timeout, kill_tree)
    except BaseException:
        operation.receipt.retain_partial_launch(process, reason="partial_launch")
        operation.state = "unknown"
        raise


def record_spawn_error(
    operation: ShellHookOperation, error: str, *, ambiguous: bool = False, process: Any = None,
) -> None:
    """Record a failed launch without mistaking an interrupted Popen for no effect."""
    if process is not None:
        operation.receipt.retain_partial_launch(process, reason="spawn_bind_failed")
        operation.last_error = error
        operation.state = "unknown"
        return
    if not ambiguous and operation.release_without_process():
        return
    operation.last_error = error
    operation.receipt.mark_unknown("spawn_bind_failed")
    operation.state = "unknown"


def current_dispatch_operation(spec: Any, event: str, kwargs: dict[str, Any]) -> ShellHookOperation | None:
    """Return/create operation for the callback without changing its wire result."""
    try:
        return begin_operation(spec, event, kwargs)
    except _MissingCustody:
        raise


def mark_current_dispatch_unknown(reason: str) -> None:
    """Retain accepted custody while keeping a no-effect observer failure local."""
    dispatch = _current_dispatch()
    if dispatch is None:
        return
    dispatch.unknown_reason = reason
    active_operations: list[ShellHookOperation] = []
    for operation in tuple(dispatch.operations.values()):
        if operation.state in {"closed", "released"}:
            continue
        active_operations.append(operation)
        if getattr(operation.receipt, "state", None) not in {"unknown", "closed", "released"}:
            operation.receipt.mark_unknown(reason)
        operation.last_error = reason
        operation.state = "unknown"
    # A reservation may have accepted service/request/physical custody before the
    # ShellHookOperation was published.  Its exact references, rather than an empty
    # operation map, decide whether the authenticated owner must become UNKNOWN.
    reservation_marked = False
    for reservation in tuple(dispatch.reservations):
        if not reservation.accepted():
            continue
        operation = reservation.operation
        if operation is not None and (
            operation.state in {"closed", "released"}
            or any(candidate is operation for candidate in active_operations)
        ):
            continue
        _mark_reservation_unknown(reservation, reason)
        reservation_marked = True
    if not active_operations:
        if not reservation_marked:
            # Observer-only queue drops have no accepted reservation and must not poison
            # the active I04 parent merely because the copied dispatch carried a binding.
            return
        # _mark_reservation_unknown already marked the exact authenticated parent.
        return
    binding = dispatch.binding
    marker = getattr(getattr(binding, "authority", None), "_preservation_mark_unknown", None)
    if callable(marker) and binding is not None:
        try:
            marker(binding.admission_id, reason)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, KeyboardInterrupt, SystemExit):
            logger.warning("shell-hook stream owner unknown marker failed", exc_info=True)
