"""Cron adapters for the existing gateway RuntimeCustodyCoordinator.

The coordinator and cron executions ledger remain the authorities.  This module only carries the
exact coordinator handles and real process/future references across the existing cron seams.  The
execution-keyed callback index is a wake-up index for delivery settlement; it is never consulted as
proof that a class is empty or ready.
"""
from __future__ import annotations

import os
import subprocess
import time
import threading
import sqlite3
import sys
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional


# A Windows venv/PM bootstrap may leave the Popen root waiting on a fresh worker
# interpreter.  Keep the ancestry proof bounded; an unbounded parent walk is not
# an ownership proof and can cross unrelated process lifetime boundaries.
MAX_CRON_WORKER_ANCESTRY_HOPS = 3


class CronCustodyUnavailable(RuntimeError):
    """The managed gateway has no installed coordinator; the caller must refuse the admission."""


class CronCustodyRefused(RuntimeError):
    """The exact coordinator lease could not be acquired or activated."""

    def __init__(self, code: str):
        self.code = code
        super().__init__(code)


_REQUIRED = ContextVar("cron_custody_required", default=False)
_PARENT = ContextVar("cron_custody_parent", default=None)
# The active owner is a propagation seam for the existing in-process script runner.  It is
# deliberately a binding object, never an execution id or an "empty" marker.
_ACTIVE_BINDING = ContextVar("cron_custody_active_binding", default=None)

# This is deliberately only a callback index.  RuntimeCustodyCoordinator owns every state
# transition; the execution ledger owns terminal/delivery facts.  Entries disappear after the
# coordinator handle is settled and are never counted as readiness.
_BINDINGS_LOCK = threading.RLock()
_BINDINGS_BY_EXECUTION: dict[str, list[CronOwnerBinding]] = {}
_SCOPE_PROOF_LOCK = threading.RLock()
_SETTLED_SCOPE_PROOFS: dict[str, dict[str, Any]] = {}


@contextmanager
def require_cron_custody(parent: Optional[CronOwnerBinding] = None):
    """Require an exact runtime handle for the current provider call.

    ``asyncio.to_thread`` copies this ContextVar, so a provider's ordinary claim seam can participate
    without a new provider authority or a changed abstract start contract.
    """
    required_token = _REQUIRED.set(True)
    parent_token = _PARENT.set(parent)
    try:
        yield
    finally:
        _PARENT.reset(parent_token)
        _REQUIRED.reset(required_token)


def custody_required() -> bool:
    return bool(_REQUIRED.get())


def custody_parent() -> Optional[CronOwnerBinding]:
    return _PARENT.get()


@contextmanager
def bind_cron_owner(binding: Optional[CronOwnerBinding]):
    """Bind one exact owner while the existing cron execution invokes a script.

    The ContextVar is only a call-context bridge; the binding's coordinator handle, durable
    execution row, process and pipe state remain the authorities.
    """
    token = _ACTIVE_BINDING.set(binding)
    try:
        yield
    finally:
        _ACTIVE_BINDING.reset(token)


def register_script_process(process: Any) -> None:
    binding = _ACTIVE_BINDING.get()
    if binding is not None:
        binding.bind_script_process(process)


def finish_script_process(process: Any) -> None:
    binding = _ACTIVE_BINDING.get()
    if binding is not None:
        binding.finish_script_process(process)


def managed_runtime_available() -> bool:
    """Return whether the live gateway has a descriptor-backed custody coordinator."""
    return _runtime() is not None


def _runtime():
    try:
        # Availability probing runs off-turn, so importing gateway.run here would execute the
        # launch-profile bootstrap (dotenv/config defaults) merely to ask whether a coordinator
        # exists.  A managed gateway has already loaded this module; an unloaded module is a
        # deliberate unavailable/refusal state.
        gateway_run = sys.modules.get("gateway.run")
        runner_ref = getattr(gateway_run, "_gateway_runner_ref", None)
        if not callable(runner_ref):
            return None
        runner = runner_ref()
        coordinator = getattr(runner, "runtime_custody", None) if runner is not None else None
        descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
        if coordinator is None or not descriptor.get("instance_id"):
            return None
        epoch = descriptor.get("authority_epoch")
        if type(epoch) is not int or epoch < 1:
            return None
        from hermes_constants import get_hermes_home

        return runner, coordinator, descriptor, Path(get_hermes_home()).resolve(), epoch
    except (AttributeError, ImportError, OSError, TypeError, ValueError):
        return None


def reserve_execution(
    *, owner_class: str, job_id: str, source: str, scheduled_instant: Optional[str] = None,
    parent: Optional[CronOwnerBinding] = None, required: Optional[bool] = None,
    owner_id: Optional[str] = None,
):
    """Reserve I12 or I14 before the existing durable claim/dispatch boundary.

    ``execution_id`` is generated once, becomes the coordinator owner identity, and is passed to
    ``create_execution`` by the caller.  If the ledger write fails after the lease is acquired the
    caller must mark the handle unknown; it may not infer that the row was absent.
    """
    runtime = _runtime()
    must_bind = custody_required() if required is None else bool(required)
    if runtime is None:
        if must_bind:
            raise CronCustodyUnavailable("runtime_custody_unavailable")
        return None
    _runner, coordinator, _descriptor, profile_home, epoch = runtime
    if owner_id is None:
        execution_id = uuid.uuid4().hex
    elif type(owner_id) is not str or not owner_id:
        raise CronCustodyRefused("invalid_owner_id")
    else:
        execution_id = owner_id
    handle = coordinator.reserve_root(
        owner_class,
        execution_id,
        profile_id=str(profile_home),
        authority_epoch=epoch,
        parent=parent.handle if parent is not None else None,
    )
    from gateway.runtime_custody import CustodyRefusal

    if isinstance(handle, CustodyRefusal):
        raise CronCustodyRefused(handle.code)
    binding = CronOwnerBinding(
        coordinator=coordinator,
        handle=handle,
        owner_class=owner_class,
        owner_id=execution_id,
        job_id=str(job_id),
        execution_id=execution_id if owner_class == "I12_cron_fire" else None,
        source=str(source),
        scheduled_instant=scheduled_instant,
        parent=parent,
    )
    if parent is not None:
        try:
            parent.attach_child(binding)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            binding.mark_unknown("foreign_custody")
            raise
    return binding


class CronOwnerBinding:
    """One exact coordinator handle plus the real completion objects for one cron admission."""

    def __init__(
        self, *, coordinator, handle, owner_class: str, owner_id: str, job_id: str,
        execution_id: Optional[str], source: str, scheduled_instant: Optional[str], parent=None,
    ):
        self.coordinator = coordinator
        self.handle = handle
        self.owner_class = owner_class
        self.owner_id = owner_id
        self.job_id = str(job_id)
        self.execution_id = execution_id
        self.source = source
        self.scheduled_instant = scheduled_instant
        self.parent = parent
        self.child: Optional[CronOwnerBinding] = None
        self._future = None
        # A split-fire provider claim is a durable boundary before the detached fire future
        # exists. Keep that real asyncio task visible until the API path either hands its exact
        # receipt to ``bind_future`` or proves the claim was unaccepted.
        self._claim_future = None
        self._process = None
        self._reaper = None
        self._reaper_expected = False
        self._worker_pid = None
        self._worker_started_at = None
        self._bound_process_pid = None
        self._bound_process_started_at = None
        self._dispatch_mode = None
        self._scope_isolated = None
        self._scope_unit = None
        self._scope_proof = None
        self._native_job = None
        self._native_job_identity = None
        self._native_job_proven = False
        # In-process script writers are tracked separately from detached worker processes.
        # A process exit alone is insufficient while stdout/stderr writers may still hold the
        # completion pipe; the runner records an explicit post-exit pipe-close observation.
        self._script_process = None
        self._script_pipes_closed = False
        self._lock = threading.RLock()
        self._settled = False
        self._unknown = False
        if execution_id:
            self._register_execution(execution_id)

    def _register_execution(self, execution_id: str) -> None:
        with _BINDINGS_LOCK:
            bucket = _BINDINGS_BY_EXECUTION.setdefault(str(execution_id), [])
            if self not in bucket:
                bucket.append(self)

    def _unregister(self) -> None:
        if not self.execution_id:
            return
        with _BINDINGS_LOCK:
            bucket = _BINDINGS_BY_EXECUTION.get(self.execution_id, [])
            if self in bucket:
                bucket.remove(self)
            if not bucket:
                _BINDINGS_BY_EXECUTION.pop(self.execution_id, None)

    def bind_execution_record(self, record: dict[str, Any]) -> None:
        if (
            not isinstance(record, dict)
            or str(record.get("id")) != self.owner_id
            or str(record.get("job_id")) != self.job_id
            or str(record.get("source")) != self.source
        ):
            self.mark_unknown("read_failed")
            raise CronCustodyRefused("execution_identity_mismatch")
        with self._lock:
            self.execution_id = self.owner_id
            self._register_execution(self.execution_id)

    def reserve_external_worker(self) -> CronOwnerBinding:
        if self.execution_id is None:
            raise CronCustodyRefused("execution_identity_missing")
        child = self.coordinator.reserve_child(
            self.handle,
            "I13_external_cron_worker",
            f"{self.execution_id}:external-worker",
            profile_id=self.handle.profile_id,
            authority_epoch=self.handle.authority_epoch,
        )
        from gateway.runtime_custody import CustodyRefusal

        if isinstance(child, CustodyRefusal):
            raise CronCustodyRefused(child.code)
        worker = CronOwnerBinding(
            coordinator=self.coordinator,
            handle=child,
            owner_class="I13_external_cron_worker",
            owner_id=f"{self.execution_id}:external-worker",
            job_id=self.job_id,
            execution_id=self.execution_id,
            source=self.source,
            scheduled_instant=self.scheduled_instant,
            parent=self,
        )
        # ChildReservation has no lease until the exact worker acknowledgement activates it.
        worker._child_reservation = child
        with self._lock:
            self.child = worker
        return worker

    @staticmethod
    def _systemd_control_group(scope_unit: str) -> Optional[str]:
        if sys.platform != "linux" or not isinstance(scope_unit, str) or not scope_unit.endswith(".scope"):
            return None
        try:
            from tools.process_registry import systemd_user_bus_env
            result = subprocess.run(
                ["systemctl", "--user", "show", scope_unit, "--property=ControlGroup",
                 "--value", "--no-pager"],
                stdin=subprocess.DEVNULL, capture_output=True, text=True,
                env=systemd_user_bus_env(), timeout=2, check=False,
            )
            control_group = result.stdout.strip()
            if result.returncode != 0 or not control_group.startswith("/") or control_group == "/":
                return None
            if ".." in Path(control_group).parts or "\x00" in control_group:
                return None
            return control_group
        except (FileNotFoundError, OSError, subprocess.SubprocessError, TypeError, ValueError):
            return None

    @classmethod
    def _capture_scope_proof(
        cls, scope_unit: str, pid: int, process_started_at: Optional[int] = None,
        control_group: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """Capture the live worker's exact systemd cgroup before it can exit.

        The parent Popen is the systemd-run wrapper, not the worker that publishes the ACK.  The
        ACK PID is therefore admitted only while its /proc cgroup and the manager's exact
        ControlGroup agree; the control-group path is retained for the post-reap descendant scan.
        """
        if type(pid) is not int or pid <= 0:
            return None
        manager_group = cls._systemd_control_group(scope_unit)
        if manager_group is not None and control_group is not None and manager_group != control_group:
            return None
        if control_group is None:
            control_group = manager_group
        if (
            not isinstance(control_group, str)
            or not control_group.startswith("/")
            or control_group == "/"
        ):
            return None
        if ".." in Path(control_group).parts or "\x00" in control_group:
            return None
        try:
            proc_groups = Path(f"/proc/{pid}/cgroup").read_text(encoding="utf-8")
        except (OSError, UnicodeError, ValueError):
            # The worker may have exited after publishing an authenticated ACK.  Its own
            # captured cgroup path remains usable only when the kernel/systemd drain proof below
            # corroborates that the exact scope has become empty.
            if not isinstance(scope_unit, str) or not cls._scope_is_empty({
                "kind": "linux_systemd_scope",
                "scope_unit": scope_unit,
                "control_group": control_group,
                "scope_empty": False,
            }) is True:
                return None
        else:
            if not any(
                line.rsplit(":", 1)[-1].strip() == control_group
                for line in proc_groups.splitlines()
            ):
                return None
        return {
            "kind": "linux_systemd_scope",
            "scope_unit": scope_unit,
            "control_group": control_group,
            "pid": pid,
            "process_started_at": process_started_at,
            "scope_empty": False,
        }

    @classmethod
    def _scope_process_matches(cls, scope_unit: str, pid: int) -> bool:
        """Prove the acknowledged PID is in the exact transient scope the parent requested.

        ``dispatch_mode == scoped`` is only a launch intent.  The ACK is accepted as isolated
        only after the kernel cgroup and the user systemd manager corroborate that intent.  An
        unavailable or unreadable scope is a refusal, never an empty-state proof.
        """
        return cls._capture_scope_proof(scope_unit, pid) is not None

    @staticmethod
    def _scope_is_empty(scope_proof: dict[str, Any]) -> Optional[bool]:
        """Return a kernel/systemd-backed descendant result for a captured Linux scope.

        ``systemd-run --collect`` removes a transient scope only after its cgroup is empty.  A
        vanished, previously captured cgroup is therefore a positive manager result; any other
        unreadable state is unknown and must not be treated as quiescence.
        """
        if scope_proof.get("kind") != "linux_systemd_scope":
            return None
        control_group = scope_proof.get("control_group")
        scope_unit = scope_proof.get("scope_unit")
        if not isinstance(control_group, str) or not control_group.startswith("/"):
            return None
        root = Path("/sys/fs/cgroup") / control_group.lstrip("/")
        try:
            if root.exists():
                observed_proc_files = 0
                walk_errors: list[OSError] = []
                for directory, _subdirectories, filenames in os.walk(
                    root, followlinks=False, onerror=walk_errors.append
                ):
                    if "cgroup.procs" not in filenames:
                        continue
                    observed_proc_files += 1
                    members = Path(directory, "cgroup.procs").read_text(
                        encoding="utf-8-sig").splitlines()
                    for member in members:
                        value = member.strip()
                        if not value:
                            continue
                        if not value.isdigit() or int(value) <= 0:
                            return None
                        return False
                # An existing cgroup root without a readable cgroup.procs file is an
                # observation failure, not evidence that the descendant set is empty.
                if walk_errors:
                    return None
                return True if observed_proc_files else None
            # --collect removes the unit only after systemd has observed an empty cgroup.  Check
            # the manager rather than equating a missing filesystem path with local process state.
            from tools.process_registry import systemd_user_bus_env
            result = subprocess.run(
                ["systemctl", "--user", "show", str(scope_unit), "--property=ControlGroup",
                 "--value", "--no-pager"],
                stdin=subprocess.DEVNULL, capture_output=True, text=True,
                env=systemd_user_bus_env(), timeout=2, check=False,
            )
            detail = f"{result.stdout}\n{result.stderr}".lower()
            if result.returncode != 0 and any(
                marker in detail for marker in ("not loaded", "not found", "does not exist")
            ):
                return True
        except (FileNotFoundError, OSError, subprocess.SubprocessError, TypeError, ValueError):
            return None
        return None

    @staticmethod
    def _native_job_is_canonical_owner(native_job: Any) -> bool:
        """Accept only the newly-created no-breakaway Job used by ``spawn_server``.

        Query-only recovery handles, replaced handles, and duck-typed test/foreign objects cannot
        supply the transitive split proof.  The private Job method pins the creation contract;
        root membership is checked separately through the retained Popen handle below.
        """
        if native_job is None or sys.platform != "win32":
            return False
        try:
            from hermes_cli.local_runtime.processes import _WindowsJob
            return (
                type(native_job) is _WindowsJob
                and native_job.is_canonical_owner_job() is True
            )
        except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
            return False

    @staticmethod
    def _native_job_contains_process(native_job: Any, process: Any) -> Optional[bool]:
        """Reuse the existing handle-based Windows Job membership proof.

        ``subprocess.Popen`` exposes its stable kernel process handle as ``_handle``; the shared
        local-runtime Job helper accepts the same handle through its ``raw_handle`` adapter.  A
        PID-only census is intentionally never used for this admission.
        """
        if native_job is None or sys.platform != "win32":
            return None
        try:
            raw_handle = getattr(process, "_handle", None)
            if raw_handle is None:
                return None
            return native_job.contains_process_handle(SimpleNamespace(raw_handle=raw_handle))
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return None

    def _native_job_binding_matches(
        self, native_job: Any, native_job_identity: Any, native_job_proven: bool,
        process: Any,
    ) -> bool:
        """Require the retained canonical Job and exact root handle for every Windows path.

        The same-PID direct path still owns a native Job when launched through ``spawn_server``.
        Replacing that object with a query-only handle, a foreign canonical Job, or a closed Job
        must fail closed just like a degraded split; terminal reconciliation cannot drain a
        different object and call it the original owner.
        """
        with self._lock:
            retained_process = self._process
            retained_job_identity = self._native_job_identity
            retained_job_proven = self._native_job_proven
        if process is not retained_process:
            return False
        if native_job is None:
            # A no-Job binding is valid only when the binding was originally created without
            # native Job provenance.  In particular, dropping the live Job object after
            # activation must not turn a retained canonical Job receipt into an unscoped path.
            return (
                native_job_identity is None
                and retained_job_identity is None
                and native_job_proven is True
                and retained_job_proven is True
            )
        return (
            native_job_identity is native_job
            and retained_job_identity is native_job
            and native_job_proven is True
            and retained_job_proven is True
            and self._native_job_is_canonical_owner(native_job) is True
            and self._native_job_contains_process(native_job, retained_process) is True
        )

    @staticmethod
    def _native_job_is_empty(native_job: Any) -> Optional[bool]:
        if native_job is None or sys.platform != "win32":
            return None
        try:
            return native_job.empty()
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return None

    def _wait_for_native_ownership_to_drain(self) -> bool:
        # The existing handoff grace is the lifecycle bound for this observation.  A zero
        # cron timeout means "unlimited" for job inactivity, but it must never turn physical
        # custody observation into an unbounded synchronous wait.  A live descendant simply
        # remains pending with the exact scope/Job handle retained for a later callback.
        from cron.executions import HANDOFF_ADOPTION_GRACE_SECONDS

        deadline = time.monotonic() + HANDOFF_ADOPTION_GRACE_SECONDS
        with self._lock:
            scope_proof = dict(self._scope_proof) if self._scope_proof is not None else None
            native_job = self._native_job
        if scope_proof is None and native_job is None:
            return True
        while True:
            if scope_proof is not None and scope_proof.get("kind") == "windows_job":
                # Windows custody is proved by the retained kernel Job handle.  The proof is
                # stored alongside the scope receipt so the same path can be retried, but it
                # must never be routed through the Linux cgroup reader.
                state = self._native_job_is_empty(native_job)
            elif scope_proof is not None:
                state = self._scope_is_empty(scope_proof)
            else:
                state = self._native_job_is_empty(native_job)
            if state is True:
                with self._lock:
                    if self._scope_proof is not None:
                        self._scope_proof["scope_empty"] = True
                return True
            if state is None:
                self.mark_unknown("worker_scope_unproven")
                return False
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # Keep the binding, retained Job/cgroup proof, and reaper available for the
                # next observation.  This is a pending drain, not a clean settlement.
                return False
            time.sleep(min(0.05, remaining))

    def _close_native_job(self) -> bool:
        with self._lock:
            native_job = self._native_job
        if native_job is None:
            return True
        try:
            native_job.close()
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.mark_unknown("worker_scope_unproven")
            return False
        return True

    def _windows_trampoline_worker_matches(
        self, record_pid: int, record_started_at: int, process: Any,
    ) -> bool:
        """Authenticate a live degraded Windows worker behind the retained Popen root.

        The ACK/ledger PID is authoritative only after the owner corroborates its exact birth
        fingerprint, liveness, non-zombie status, and bounded ancestry to the Popen handle.  The
        worker cannot supply or choose this relationship.  A dead root or unreadable ancestry is
        an ownership failure and remains UNKNOWN.
        """
        if sys.platform != "win32" or not isinstance(process, subprocess.Popen):
            return False
        try:
            if process.poll() is not None:
                return False
        except (OSError, RuntimeError, TypeError, ValueError):
            return False
        with self._lock:
            bound_pid = self._bound_process_pid
            bound_started_at = self._bound_process_started_at
        if (
            type(bound_pid) is not int
            or bound_pid <= 0
            or type(bound_started_at) is not int
            or process.pid != bound_pid
            or record_pid <= 0
            or type(record_started_at) is not int
            or record_pid == bound_pid
            or record_pid == os.getpid()
        ):
            return False
        try:
            import psutil
        except ImportError:
            return False
        try:
            launcher = psutil.Process(bound_pid)
            worker = psutil.Process(record_pid)
            if (
                launcher.ppid() != os.getpid()
                or not launcher.is_running()
                or launcher.status() == psutil.STATUS_ZOMBIE
                or not worker.is_running()
                or worker.status() == psutil.STATUS_ZOMBIE
            ):
                return False
            from gateway.status import get_process_start_time
            if (
                get_process_start_time(bound_pid) != bound_started_at
                or get_process_start_time(record_pid) != record_started_at
            ):
                return False
            # Do not use ``Process.parents()[:N]`` here.  psutil builds the complete
            # ancestry before slicing, so a long or hostile parent chain would turn a
            # bounded custody check into an unbounded process walk.  The retained Job
            # handle is the ownership proof; this short chain is only the corroborating
            # no-breakaway relationship from the ACK worker to that retained root.
            current = worker
            for _ in range(MAX_CRON_WORKER_ANCESTRY_HOPS):
                parent = current.parent()
                if parent is None:
                    return False
                if parent.pid == bound_pid:
                    # Re-check the retained root birth fingerprint immediately before
                    # accepting the PID relationship.  A reused PID cannot substitute
                    # for the Popen handle captured by the owner.
                    return get_process_start_time(bound_pid) == bound_started_at
                if parent.pid <= 0 or not parent.is_running():
                    return False
                if parent.status() == psutil.STATUS_ZOMBIE:
                    return False
                current = parent
            return False
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, psutil.Error):
            return False

    def _worker_identity_matches(self, record: dict[str, Any], process: Any, dispatch_mode: Optional[str]) -> bool:
        try:
            record_pid = int(record.get("pid", -1))
            record_started_at = record.get("process_started_at")
        except (TypeError, ValueError):
            return False
        if record_pid <= 0 or type(record_started_at) is not int:
            return False
        if dispatch_mode != "scoped":
            with self._lock:
                bound_pid = self._bound_process_pid
                bound_started_at = self._bound_process_started_at
            # A direct child may have exited before the ACK is consumed.  The parent captured
            # this exact Popen identity before waiting; never replace that receipt with a later
            # live-PID probe that cannot observe an exited process.
            if (
                record_pid == bound_pid
                and type(bound_started_at) is int
                and record_started_at == bound_started_at
            ):
                return True
            # Only degraded Windows dispatch may use the explicitly bounded launcher/worker
            # proof.  Direct equality remains the first and preferred admission path.
            return (
                dispatch_mode == "degraded"
                and self._windows_trampoline_worker_matches(record_pid, record_started_at, process)
            )
        with self._lock:
            proof = dict(self._scope_proof) if self._scope_proof is not None else None
        if proof is None:
            return False
        return (
            proof.get("pid") == record_pid
            and proof.get("process_started_at") == record_started_at
        )

    def _stored_active_worker_matches(
        self, record_pid: int, record_started_at: Any, dispatch_mode: Optional[str],
    ) -> bool:
        """Validate a terminal row against identity authenticated before process exit.

        Initial split admission requires live ancestry.  The terminal waiter may observe after both
        launcher and worker have exited, so it must consume the stored authenticated tuple and the
        retained Job provenance, then let ``observe_terminal`` perform Job-empty and handle-close
        proof.  It must never infer ownership from a fresh PID/ancestry lookup.
        """
        with self._lock:
            active_pid = self._worker_pid
            active_started_at = self._worker_started_at
            bound_pid = self._bound_process_pid
            process = self._process
            native_job = self._native_job
            native_job_proven = self._native_job_proven
            native_job_identity = self._native_job_identity
            scope_proof = dict(self._scope_proof) if self._scope_proof is not None else None
            expected_scope = self._scope_unit
        if active_pid != record_pid or active_started_at != record_started_at:
            return False
        if not self._native_job_binding_matches(
            native_job, native_job_identity, native_job_proven, process):
            return False
        if dispatch_mode == "scoped":
            return (
                expected_scope is not None
                and scope_proof is not None
                and scope_proof.get("pid") == record_pid
                and scope_proof.get("process_started_at") == record_started_at
            )
        windows_split = (
            sys.platform == "win32"
            and dispatch_mode == "degraded"
            and type(bound_pid) is int
            and record_pid != bound_pid
        )
        if not windows_split:
            return True
        return (
            native_job is not None
            and scope_proof is not None
            and scope_proof.get("kind") == "windows_job"
            and scope_proof.get("pid") == record_pid
            and scope_proof.get("process_started_at") == record_started_at
            and scope_proof.get("root_pid") == bound_pid
            and scope_proof.get("native_job_canonical") is True
            and scope_proof.get("worker_job_membership") == "transitive_no_breakaway_ancestry"
        )

    def _activate_child(
        self, process: Any, dispatch_mode: Optional[str], *, scope_proven: bool = False,
        scope_proof: Optional[dict[str, Any]] = None,
    ) -> None:
        from gateway.runtime_custody import CustodyRefusal

        active = self.coordinator.activate_child(self._child_reservation)
        if isinstance(active, CustodyRefusal):
            self.mark_unknown("foreign_custody")
            raise CronCustodyRefused(active.code)
        self.handle = active
        with self._lock:
            self._process = process
            self._worker_pid = getattr(process, "pid", None)
            self._dispatch_mode = dispatch_mode
            self._scope_isolated = bool(dispatch_mode == "scoped" and scope_proven)
            if self._scope_isolated and scope_proof is not None:
                self._scope_proof = dict(scope_proof)

    def bind_expected_scope(self, scope_unit: Optional[str]) -> None:
        if scope_unit is not None and (not isinstance(scope_unit, str) or not scope_unit.endswith(".scope")):
            raise CronCustodyRefused("worker_scope_identity_mismatch")
        with self._lock:
            self._scope_unit = scope_unit

    def activate_external_worker(
        self, acknowledgement: Any, process: Any, *, dispatch_mode: Optional[str] = None,
        scope_unit: Optional[str] = None,
    ) -> None:
        if self.owner_class != "I13_external_cron_worker":
            raise CronCustodyRefused("invalid_external_owner")
        if not isinstance(acknowledgement, dict) or acknowledgement.get("execution_id") != self.execution_id:
            self.mark_unknown("foreign_custody")
            raise CronCustodyRefused("worker_ack_mismatch")
        pid = acknowledgement.get("pid")
        if type(pid) is not int or pid <= 0:
            self.mark_unknown("foreign_custody")
            raise CronCustodyRefused("worker_pid_mismatch")
        acknowledged_started_at = acknowledgement.get("process_started_at")
        acknowledged_scope = acknowledgement.get("scope_unit")
        acknowledged_control_group = acknowledgement.get("scope_control_group")
        if dispatch_mode is not None and (
            acknowledgement.get("dispatch_mode") != dispatch_mode
            or type(acknowledged_started_at) is not int
            or (dispatch_mode == "scoped" and acknowledged_scope != scope_unit)
            or (dispatch_mode != "scoped" and acknowledged_scope is not None)
        ):
            self.mark_unknown("foreign_custody")
            raise CronCustodyRefused("worker_startup_identity_mismatch")
        with self._lock:
            expected_scope = self._scope_unit
        scope_proof = None
        if dispatch_mode == "scoped":
            if not isinstance(acknowledged_control_group, str):
                self.mark_unknown("foreign_custody")
                raise CronCustodyRefused("worker_scope_identity_unproven")
            scope_proof = self._capture_scope_proof(
                scope_unit, pid, acknowledged_started_at, acknowledged_control_group)
            if expected_scope != scope_unit or scope_proof is None:
                self.mark_unknown("foreign_custody")
                raise CronCustodyRefused("worker_scope_identity_unproven")
            with self._lock:
                self._scope_proof = dict(scope_proof)
        from cron.executions import get_execution

        try:
            record = get_execution(self.execution_id)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            self.mark_unknown("read_failed")
            raise CronCustodyRefused("worker_execution_read_failed")
        try:
            record_pid = int(record.get("pid", -1)) if record else -1
        except (TypeError, ValueError):
            record_pid = -1
        if (
            not isinstance(record, dict)
            or str(record.get("id")) != self.execution_id
            or str(record.get("job_id")) != self.job_id
            or record.get("source") != self.source
            or record.get("status") not in {"running", "completed", "failed"}
            or record_pid != pid
            or (
                dispatch_mode is not None
                and record.get("process_started_at") != acknowledged_started_at
            )
            or not self._worker_identity_matches(record, process, dispatch_mode)
        ):
            self.mark_unknown("foreign_custody")
            raise CronCustodyRefused("worker_execution_not_adopted")
        with self._lock:
            bound_pid = self._bound_process_pid
            bound_started_at = self._bound_process_started_at
            native_job = self._native_job
            native_job_proven = self._native_job_proven
            native_job_identity = self._native_job_identity
        windows_split = (
            sys.platform == "win32"
            and dispatch_mode == "degraded"
            and type(bound_pid) is int
            and record_pid != bound_pid
        )
        if not self._native_job_binding_matches(
            native_job, native_job_identity, native_job_proven, process):
            self.mark_unknown("worker_scope_unproven")
            raise CronCustodyRefused("worker_job_membership_unproven")
        # A split worker has no safe degraded custody without the exact canonical no-breakaway
        # Job that already proved the retained Popen root handle.  Bounded ancestry is transitive
        # Job membership only under that pinned creation contract; no PID census is accepted.
        if windows_split and (
            native_job is None
            or native_job_proven is not True
            or native_job_identity is not native_job
        ):
            self.mark_unknown("worker_scope_unproven")
            raise CronCustodyRefused("worker_job_membership_unproven")
        self._activate_child(
            process, dispatch_mode,
            scope_proven=dispatch_mode == "scoped" or native_job is not None,
            scope_proof=scope_proof,
        )
        with self._lock:
            # _process remains the retained Popen/root handle.  The ACK/ledger identity is the
            # authenticated worker identity used by terminal reconciliation and Job proof.
            self._worker_pid = record_pid
            self._worker_started_at = record.get("process_started_at")
            if native_job is not None and scope_proof is None:
                self._scope_proof = {
                    "kind": "windows_job",
                    "pid": record_pid,
                    "process_started_at": self._worker_started_at,
                    "root_pid": bound_pid,
                    "root_process_started_at": bound_started_at,
                    "native_job_canonical": True,
                    "worker_job_membership": (
                        "transitive_no_breakaway_ancestry" if windows_split
                        else "retained_root_handle"
                    ),
                    "scope_empty": False,
                }

    def bind_process(self, process: Any, *, native_job: Any = None) -> None:
        pid = getattr(process, "pid", None)
        started_at = None
        if type(pid) is int and pid > 0:
            try:
                from gateway.status import get_process_start_time
                started_at = get_process_start_time(pid)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                started_at = None
        with self._lock:
            self._process = process
            self._bound_process_pid = pid
            self._bound_process_started_at = started_at
            self._native_job = native_job
            self._native_job_identity = native_job
            self._reaper_expected = True
            self._native_job_proven = (
                native_job is None
                or (
                    self._native_job_is_canonical_owner(native_job) is True
                    and self._native_job_contains_process(native_job, process) is True
                )
            )

    def bind_script_process(self, process: Any) -> None:
        with self._lock:
            self._script_process = process
            self._script_pipes_closed = False

    def finish_script_process(self, process: Any) -> None:
        """Record script EOF only after the real child exited and its captured streams closed."""
        with self._lock:
            if process is not self._script_process:
                return
        try:
            if process.poll() is None:
                return
        except (OSError, ValueError, AttributeError):
            self.mark_unknown("read_failed")
            return
        for stream_name in ("stdout", "stderr"):
            stream = getattr(process, stream_name, None)
            if stream is not None:
                try:
                    stream.close()
                except (OSError, ValueError):
                    self.mark_unknown("read_failed")
                    return
        with self._lock:
            self._script_pipes_closed = True

    def reconcile_unacknowledged(
        self, process: Any = None, *, dispatch_mode: Optional[str] = None,
    ) -> str:
        """Classify a child whose process ended before a verifiable ready acknowledgement.

        The production waiter may also call this after activation when the worker publishes its
        terminal row before the retained root exits.  In that state the authenticated tuple is
        immutable: a mismatch is UNKNOWN and cannot re-enter the initial adoption path.
        """
        from cron.executions import get_execution

        with self._lock:
            pending_child_reservation = hasattr(self.handle, "reservation")
        if not self.execution_id:
            return "not_applicable"
        try:
            record = get_execution(self.execution_id)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            self.mark_unknown("read_failed")
            return "unknown"
        if (
            record is not None
            and record.get("status") == "claimed"
            and bool(record.get("handoff_pending"))
        ):
            if not pending_child_reservation:
                self.mark_unknown("foreign_custody")
                return "unknown"
            # The durable handoff never adopted; releasing this reservation is proven safe.
            self.cancel_unactivated_child()
            return "unadopted" if self.settled else "unknown"
        try:
            record_pid = int(record.get("pid", -1)) if record else -1
        except (TypeError, ValueError):
            record_pid = -1
        if (
            process is not None
            and isinstance(record, dict)
            and record.get("status") in {"completed", "failed"}
        ):
            record_started_at = record.get("process_started_at")
            with self._lock:
                previously_activated = not hasattr(self.handle, "reservation")
                already_active = (
                    self._worker_pid == record_pid
                    and self._worker_started_at == record_started_at
                )
            if previously_activated:
                # Once the child lease is active, a terminal row must match the exact tuple
                # authenticated during activation.  A mismatch is ownership uncertainty; never
                # reopen live ancestry and re-activate the same lease with a replacement tuple.
                if not already_active:
                    self.mark_unknown("foreign_custody")
                    return "unknown"
                # The split was authenticated before exit.  Do not re-run live ancestry here;
                # observe_terminal still has to prove Job empty and close the retained handle.
                if not self._stored_active_worker_matches(
                    record_pid, record_started_at, dispatch_mode):
                    self.mark_unknown("foreign_custody")
                    return "unknown"
                return "terminalized"
            if not self._worker_identity_matches(record, process, dispatch_mode):
                self.mark_unknown("foreign_custody")
                return "unknown"
            scope_proof = None
            if dispatch_mode == "scoped":
                with self._lock:
                    expected_scope = self._scope_unit
                    scope_proof = dict(self._scope_proof) if self._scope_proof is not None else None
                if (
                    expected_scope is None
                    or scope_proof is None
                    or scope_proof.get("pid") != record_pid
                    or scope_proof.get("process_started_at") != record_started_at
                ):
                    self.mark_unknown("foreign_custody")
                    return "unknown"
            with self._lock:
                bound_pid = self._bound_process_pid
                bound_started_at = self._bound_process_started_at
                native_job = self._native_job
                native_job_proven = self._native_job_proven
                native_job_identity = self._native_job_identity
            windows_split = (
                sys.platform == "win32"
                and dispatch_mode == "degraded"
                and type(bound_pid) is int
                and record_pid != bound_pid
            )
            if not self._native_job_binding_matches(
                native_job, native_job_identity, native_job_proven, process):
                self.mark_unknown("worker_scope_unproven")
                return "unknown"
            if windows_split and (
                native_job is None
                or native_job_proven is not True
                or native_job_identity is not native_job
            ):
                self.mark_unknown("worker_scope_unproven")
                return "unknown"
            try:
                self._activate_child(
                    process, dispatch_mode,
                    scope_proven=dispatch_mode == "scoped" or self._native_job is not None,
                    scope_proof=scope_proof if dispatch_mode == "scoped" else None,
                )
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
                self.mark_unknown("lease_release_failed")
                return "unknown"
            with self._lock:
                self._worker_pid = record_pid
                self._worker_started_at = record_started_at
                if self._native_job is not None and dispatch_mode != "scoped":
                    self._scope_proof = {
                        "kind": "windows_job",
                        "pid": record_pid,
                        "process_started_at": self._worker_started_at,
                        "root_pid": bound_pid,
                        "root_process_started_at": bound_started_at,
                        "native_job_canonical": True,
                        "worker_job_membership": (
                            "transitive_no_breakaway_ancestry" if windows_split
                            else "retained_root_handle"
                        ),
                        "scope_empty": False,
                    }
            return "terminalized"
        # The row may have been adopted or terminalized with an acknowledgement we could not
        # authenticate.  Keep the parent/child custody unknown rather than guessing release.
        self.mark_unknown("foreign_custody")
        return "unknown"

    def cancel_unactivated_child(self) -> None:
        """Release only when the caller proves no external worker was started or adopted."""
        from gateway.runtime_custody import ChildReservation, CustodyRefusal

        if not isinstance(getattr(self, "_child_reservation", None), ChildReservation):
            return
        try:
            result = self.coordinator.release_child_reservation(self._child_reservation)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            self.mark_unknown("lease_release_failed")
            return
        if isinstance(result, CustodyRefusal):
            self.mark_unknown("lease_release_failed")
        else:
            with self._lock:
                self._settled = True
            self._unregister()
            if self.parent is not None:
                try:
                    self.parent.observe_terminal()
                except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
                    self.parent.mark_unknown("read_failed")

    def bind_reaper(self, reaper: Any) -> None:
        with self._lock:
            self._reaper = reaper
            self._reaper_expected = False

    def expect_reaper(self) -> None:
        """Reserve the physical reaper slot before a fast child can finish."""
        with self._lock:
            self._reaper_expected = True

    def bind_future(self, future: Any) -> None:
        with self._lock:
            self._future = future
        if hasattr(future, "add_done_callback"):
            future.add_done_callback(lambda _future: self.observe_terminal())

    def bind_claim_future(self, future: Any) -> None:
        """Retain the provider's real pre-detach claim task without treating it as terminal work."""
        with self._lock:
            self._claim_future = future

    def clear_claim_future(self, future: Any = None) -> None:
        """Drop the claim-task reference only after its outcome has been handed off or released."""
        with self._lock:
            if future is None or self._claim_future is future:
                self._claim_future = None

    def attach_child(self, child: CronOwnerBinding) -> None:
        if child.parent is not self:
            self.mark_unknown("foreign_custody")
            raise CronCustodyRefused("child_parent_mismatch")
        with self._lock:
            self.child = child
            if child.execution_id:
                child._register_execution(child.execution_id)

    def validate_child_receipt(self, child: Any, *, job_id: str, execution_id: str) -> None:
        """Validate the exact I12 receipt before an I14 API caller returns 202.

        The returned provider object is untrusted until its parent, class, execution identity,
        job identity, source row and pre-dispatch state all agree.  This is intentionally a
        read-and-compare gate; it does not manufacture a receipt from the API request.
        """
        from cron.executions import get_execution

        if (
            not isinstance(child, CronOwnerBinding)
            or self.owner_class != "I14_api_detached_cron"
            or child.parent is not self
            or child.owner_class != "I12_cron_fire"
            or type(execution_id) is not str
            or not execution_id
            or child.execution_id != execution_id
            or child.job_id != str(job_id)
            or child.owner_id != execution_id
            or child.source == "api-cron"
        ):
            self.mark_unknown("foreign_custody")
            raise CronCustodyRefused("execution_identity_mismatch")
        try:
            record = get_execution(execution_id)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            self.mark_unknown("read_failed")
            raise CronCustodyRefused("execution_read_failed")
        if (
            not isinstance(record, dict)
            or str(record.get("id")) != execution_id
            or str(record.get("job_id")) != str(job_id)
            or record.get("source") != child.source
            or not isinstance(record.get("source"), str)
            or not record.get("source")
            or record.get("status") != "claimed"
            or bool(record.get("handoff_pending"))
        ):
            self.mark_unknown("foreign_custody")
            raise CronCustodyRefused("execution_not_admitted")
        self.attach_child(child)

    def release_unaccepted(self) -> None:
        try:
            result = self.coordinator.release_unaccepted(self.handle)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            self.mark_unknown("read_failed")
            return
        if result is not None:
            self.mark_unknown("read_failed")
        else:
            self._settled = True
            self._unregister()

    def mark_unknown(self, reason: str) -> None:
        with self._lock:
            if self._unknown or self._settled:
                return
            self._unknown = True
        # A child reservation has no active lease yet. Keeping the reservation
        # unresolved is the fail-closed state; asking the coordinator to mark a
        # reservation as an active handle would only manufacture a stale-lease
        # result and obscure the real custody uncertainty.
        if not hasattr(self.handle, "reservation"):
            self.coordinator.mark_unknown(self.handle, reason)
        if self.parent is not None:
            self.parent.mark_unknown(reason)

    def _terminal_record_ready(self) -> bool:
        if not self.execution_id:
            return True
        from cron.executions import get_execution

        try:
            record = get_execution(self.execution_id)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
            self.mark_unknown("read_failed")
            return False
        if not record or record.get("status") not in {"completed", "failed", "unknown"}:
            return False
        if record.get("status") == "unknown":
            self.mark_unknown("foreign_custody")
            return False
        # A queued outcome is a real durable delivery obligation, not a terminal publication.
        if record.get("delivery_outcome") == "queued":
            return False
        return True

    def _physical_ready(self) -> bool:
        # A ChildReservation has not been adopted by a real worker yet.  It is neither a
        # terminal lease nor evidence that the external handoff completed.
        if hasattr(self.handle, "reservation"):
            return False
        with self._lock:
            future, claim_future, process, reaper = (
                self._future, self._claim_future, self._process, self._reaper)
            reaper_expected = self._reaper_expected
            script_process, script_pipes_closed = self._script_process, self._script_pipes_closed
        if claim_future is not None:
            return False
        if reaper_expected and reaper is None:
            return False
        if future is not None and not future.done():
            return False
        if process is not None:
            try:
                if process.poll() is None:
                    return False
            except (OSError, ValueError, AttributeError):
                self.mark_unknown("read_failed")
                return False
        if reaper is not None:
            if reaper.is_alive() or getattr(reaper, "_cron_reap_succeeded", False) is not True:
                return False
        if script_process is not None:
            try:
                if script_process.poll() is None or not script_pipes_closed:
                    return False
            except (OSError, ValueError, AttributeError):
                self.mark_unknown("read_failed")
                return False
        return True

    def observe_terminal(self) -> bool:
        """Release only after ledger, delivery, child, process-group and reaper evidence agree."""
        with self._lock:
            if self._settled or self._unknown:
                return self._settled
            child = self.child
        if child is not None and not child.observe_terminal():
            return False
        if not self._terminal_record_ready() or not self._physical_ready():
            return False
        if child is not None and not child._settled:
            return False
        if not self._wait_for_native_ownership_to_drain():
            return False
        if not self._close_native_job():
            return False
        with self._lock:
            if self._settled or self._unknown:
                return self._settled
        finalizer_id = f"cron:{self.owner_class}:{self.owner_id}:terminal"
        refusal = self.coordinator.mark_finalizing(self.handle, finalizer_id)
        if refusal is not None:
            self.mark_unknown("lease_release_failed")
            return False
        refusal = self.coordinator.mark_terminal(self.handle)
        if refusal is not None:
            self.mark_unknown("lease_release_failed")
            return False
        with self._lock:
            self._settled = True
            scope_proof = dict(self._scope_proof) if self._scope_proof is not None else None
        if self.owner_class == "I13_external_cron_worker" and scope_proof is not None and self.execution_id:
            scope_proof["scope_empty"] = True
            with _SCOPE_PROOF_LOCK:
                _SETTLED_SCOPE_PROOFS[str(self.execution_id)] = scope_proof
        self._unregister()
        if self.parent is not None:
            self.parent.observe_terminal()
        return True

    @property
    def settled(self) -> bool:
        with self._lock:
            return self._settled


def _cron_bindings_are_quiet() -> bool:
    with _BINDINGS_LOCK:
        bindings = tuple(
            binding
            for bucket in _BINDINGS_BY_EXECUTION.values()
            for binding in bucket
            if binding.owner_class in {
                "I12_cron_fire", "I13_external_cron_worker", "I14_api_detached_cron",
            }
        )
    for binding in bindings:
        if not binding.settled:
            return False
        with binding._lock:
            if binding._unknown:
                return False
            if binding.owner_class == "I13_external_cron_worker":
                native_owned = (
                    binding._native_job is not None
                    and binding._native_job_proven is True
                )
                if binding._scope_isolated is not True and not native_owned:
                    return False
    return True


def _cron_profile_ledgers_are_quiet(runner: Any) -> bool:
    try:
        from gateway.run_runtime import reserved_profile_homes
        from cron.executions import list_unsettled_executions
        from cron import scheduler as cron_scheduler

        homes = tuple(home for _name, home in reserved_profile_homes(runner))
        if not homes or cron_scheduler.get_running_job_ids() or cron_scheduler.get_wedged_job_ids():
            return False
        for home in homes:
            with cron_scheduler._profile_cron_scope(home):
                records = list_unsettled_executions()
            external_workers = home / "cron" / "external-workers"
            if external_workers.exists() and any(external_workers.iterdir()):
                return False
            if records:
                return False
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return False
    return True


def _cron_external_scope_is_quiet(runner: Any, *, require_external_row: bool = False) -> bool:
    """Require a complete durable ledger scan and an observed exact worker proof.

    A missing binding, empty handoff directory, ``dispatch_mode`` flag, or empty local callback
    map is not a physical ownership result.  Rows adopted by a different process remain blocked
    unless this process captured the matching systemd cgroup or Windows Job proof, waited for the
    real descendant set to drain, and published the matching receipt before settlement.
    """
    try:
        from cron import executions
        from cron.executions import list_executions
        from cron import scheduler as cron_scheduler
        from gateway.run_runtime import reserved_profile_homes
        current_process_id = getattr(executions, "_PROCESS_ID", None)
        for _name, home in reserved_profile_homes(runner):
            rows: list[dict[str, Any]] = []
            external_seen = False
            before_claimed_at = None
            while True:
                with cron_scheduler._profile_cron_scope(home):
                    page = list_executions(
                        limit=500, before_claimed_at=before_claimed_at)
                if not page:
                    break
                if any(not isinstance(row, dict) for row in page):
                    return False
                rows.extend(page)
                if len(page) < 500:
                    break
                next_before = page[-1].get("claimed_at")
                if not isinstance(next_before, str) or next_before == before_claimed_at:
                    return False
                before_claimed_at = next_before
            if not rows:
                # A complete durable query is still required for an inactive profile.  A
                # cross-process owner cannot be sealed from an empty history: the absence of
                # rows is a durable negative observation, not physical proof that another
                # launcher or detached writer is absent.
                if require_external_row:
                    return False
                continue
            with _SCOPE_PROOF_LOCK:
                for row in rows:
                    process_id = row.get("process_id")
                    if not isinstance(process_id, str) or not process_id:
                        return False
                    if process_id == current_process_id:
                        continue
                    external_seen = True
                    proof = _SETTLED_SCOPE_PROOFS.get(str(row.get("id")))
                    if (
                        not isinstance(proof, dict)
                        or proof.get("kind") not in {"linux_systemd_scope", "windows_job"}
                        or proof.get("scope_empty") is not True
                    ):
                        return False
                    if (
                        proof.get("pid") != row.get("pid")
                        or proof.get("process_started_at") != row.get("process_started_at")
                    ):
                        return False
            if require_external_row and not external_seen:
                return False
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error):
        return False
    return True


def preservation_owner_evidence(
    owner_class: str, *, process_instance: str, authority_epoch: int,
    close_generation: int, owner_id: str,
) -> Optional[dict[str, Any]]:
    """Return source-backed quiescence for one cron preservation owner.

    The hook consumes the real execution ledger, scheduler in-flight sets, exact binding index,
    and external-worker handoff directory.  It deliberately refuses on any read error or any
    claimed/running/unknown/queued row; an empty local callback index alone is never enough.
    """
    if owner_class not in {
        "I12_cron_fire", "I13_external_cron_worker", "I14_api_detached_cron",
    }:
        return None
    if type(process_instance) is not str or not process_instance or type(authority_epoch) is not int:
        return None
    runtime = _runtime()
    if runtime is None:
        return None
    _runner, _coordinator, descriptor, _profile_home, live_epoch = runtime
    if (
        descriptor.get("instance_id") != process_instance
        or live_epoch != authority_epoch
        or type(close_generation) is not int
        or close_generation < 1
        or type(owner_id) is not str
        or not owner_id
    ):
        return None
    if not _cron_bindings_are_quiet() or not _cron_profile_ledgers_are_quiet(_runner):
        return None
    if owner_class in {"I12_cron_fire", "I13_external_cron_worker", "I14_api_detached_cron"}:
        # All three classes are marked CROSS_PROCESS by the coordinator.  Require the scan to
        # observe at least one foreign durable execution with a matching settled physical
        # receipt; local rows, an empty ledger, and an empty callback map cannot be relabeled as
        # cross-process proof for I12/I14.
        if not _cron_external_scope_is_quiet(_runner, require_external_row=True):
            return None
    from gateway.runtime_custody import OWNER_SPECS
    return {
        "source": OWNER_SPECS[owner_class].source,
        "process_instance": process_instance,
        "authority_epoch": authority_epoch,
        "close_generation": close_generation,
        "owner_id": owner_id,
        "observed": True,
        "cross_process_proof": True,
        "cross_process_proof_source": "foreign_durable_row+settled_physical_receipt",
    }


def notify_execution_delivery(execution_id: str) -> None:
    """Wake exact bindings after the existing delivery queue writes its terminal outcome."""
    with _BINDINGS_LOCK:
        bindings = tuple(_BINDINGS_BY_EXECUTION.get(str(execution_id), ()))
    for binding in bindings:
        binding.observe_terminal()
