"""Windows-only process-registry custody handoff.

The process registry must retain the exact native objects that were created before a
foreground command can be handed to the background registry.  This module is the
small adapter for that handoff.  It intentionally imports the existing ``_WindowsJob``
only when a Windows launch is requested; POSIX and unsupported launchers remain an
explicit unknown/refusal path.
"""
from __future__ import annotations

import ctypes
from dataclasses import dataclass
import logging
import sys
import subprocess
import time
import threading
from ctypes import wintypes
from typing import Any, Optional


CREATE_SUSPENDED = 0x00000004
logger = logging.getLogger("tools.process_registry_windows")


class WindowsCustodyUnavailable(RuntimeError):
    """The native Windows owner cannot be established for this launch."""


@dataclass
class _ProcessHandleRef:
    """The shape expected by ``_WindowsJob.contains_process_handle``."""

    raw_handle: Any


@dataclass
class WindowsLaunchTransaction:
    """One pre-effect lease and its exact process/Job objects.

    ``state`` is deliberately monotonic.  Once ``Popen`` returns, no exception path
    may call ``release_unaccepted`` because a child or a Job member may already exist.
    The transaction keeps both objects reachable until the registry has observed EOF,
    durable output, exact Job emptiness, and delivery acknowledgement.
    """

    custody: Any
    state: str = "reserved"
    process: Optional[subprocess.Popen] = None
    job: Any = None
    pid: Optional[int] = None
    host_start_time: Optional[int] = None
    resumed: bool = False
    assigned: bool = False
    assignment_attempted: bool = False
    assignment_ambiguous: bool = False
    # An assignment call that raised is unknown until the exact retained Job reports
    # membership.  An empty Job alone is never interpreted as process nonmembership.
    assignment_observation: str = "unobserved"  # member|nonmember|query_unavailable
    assignment_observation_failures: int = 0
    scope_capture_ambiguous: bool = False
    # Assignment and scope capture happen while the child is still suspended.  This
    # is an authenticated identity phase; it is deliberately distinct from resume.
    scope_captured: bool = False
    native_empty_observed: bool = False
    # Scope observation is deliberately separate from ``state``.  ``None`` from
    # observe_empty() used to conflate a live descendant, a foreign member, and
    # an unavailable native query, which made a retained transaction impossible
    # to reconcile without weakening custody.
    scope_observation: str = "unobserved"
    scope_observation_failures: int = 0
    scope_observation_retries: int = 0
    scope_observation_job: Any = None
    scope_observation_job_handle: Any = None
    scope_observation_process: Any = None
    scope_observation_pid: Optional[int] = None
    scope_observation_start_time: Optional[int] = None
    process_handle_closed: bool = False
    job_handle_closed: bool = False
    no_effect_verified: bool = False
    termination_attempts: int = 0
    termination_failures: int = 0
    termination_retryable: bool = False
    close_attempts: int = 0
    close_failures: int = 0
    close_retryable: bool = False
    termination_job: Any = None
    termination_job_handle: Any = None
    termination_process: Any = None
    close_job: Any = None
    close_job_handle: Any = None

    @property
    def can_bind_process(self) -> bool:
        """True for the exact assigned Job/process identity, even before resume."""
        return bool(
            self.assigned and self.scope_captured and self.matches(self.process, self.pid, self.host_start_time)
        )

    @property
    def scope_ready(self) -> bool:
        """True only after the captured exact scope also resumed successfully."""
        return bool(self.can_bind_process and self.resumed)

    def _record_unknown(self, reason: str) -> None:
        """Record failure custody without masking the originating exception."""
        try:
            self.custody.mark_unknown(reason)
        except BaseException as exc:  # health: secondary custody reporting cannot replace original failure
            logger.exception("Background process _record_unknown failed; retaining existing custody")
            logger.warning("native custody report failed during %s: %s", reason, type(exc).__name__)

    @property
    def has_process_effect(self) -> bool:
        return self.process is not None

    @property
    def process_ref(self) -> Optional[_ProcessHandleRef]:
        if self.process is None:
            return None
        try:
            raw = self.process._handle
        except (AttributeError, OSError, TypeError, ValueError):
            return None
        return _ProcessHandleRef(raw)

    def _native_job_type(self):
        if sys.platform != "win32":
            raise WindowsCustodyUnavailable("windows_job_requires_native_windows")
        try:
            from hermes_cli.local_runtime.processes import _WindowsJob
        except (ImportError, OSError, RuntimeError) as exc:
            raise WindowsCustodyUnavailable("windows_job_unavailable") from exc
        return _WindowsJob

    def launch(
        self,
        args: list[str],
        *,
        text: bool,
        env: dict[str, str],
        cwd: str,
        stdin: Any,
        stdout: Any,
        stderr: Any,
        encoding: Optional[str],
        errors: Optional[str],
        bufsize: int = -1,
        start_new_session: bool = True,
        close_fds: bool = True,
        creationflags: int = 0,
        on_process_bound=None,
    ) -> subprocess.Popen:
        """Create, bind, and resume one child without losing partial custody."""
        if self.state != "reserved":
            raise WindowsCustodyUnavailable("launch_transaction_already_used")
        if sys.platform != "win32":
            raise WindowsCustodyUnavailable("windows_job_requires_native_windows")
        from gateway.status import get_process_start_time
        import psutil

        try:
            job_type = self._native_job_type()
            self.job = job_type()
            self.state = "job_created"
        except BaseException:  # health: allow BLE001 -- native owner construction must retain unknown custody
            self.state = "unknown"
            self._record_unknown("native_owner_unavailable")
            raise
        try:
            self.process = subprocess.Popen(
                args,
                text=text,
                env=env,
                cwd=cwd,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                encoding=encoding,
                errors=errors,
                bufsize=bufsize,
                start_new_session=start_new_session,
                close_fds=close_fds,
                creationflags=creationflags | CREATE_SUSPENDED,
            )
            self.state = "popen_bound"
            self.pid = self.process.pid
            self.host_start_time = get_process_start_time(self.pid)
            if type(self.pid) is not int or self.pid <= 0 or type(self.host_start_time) is not int:
                raise WindowsCustodyUnavailable("windows_process_identity_unavailable")
            self._retain_launch_identity()
            # Publish the exact Popen/transaction before any fallible Job or custody
            # operation.  The callback never claims process_bound or resumed.
            if on_process_bound is not None:
                on_process_bound(self)
            self.assignment_attempted = True
            try:
                self.job.assign(self.process)
            except BaseException:  # health: retain the exact Job when assignment has ambiguous effect
                # The Job handle and exact Popen identity remain the only safe recovery
                # route when assignment may have taken effect before the API raised.
                self.assignment_ambiguous = True
                raise
            self.assigned = True
            self.state = "job_assigned"
            try:
                self._capture_scope_identity()
            except BaseException:  # health: retain the exact Job when scope capture is ambiguous
                # Assignment is authenticated, but scope capture is a separate phase.
                # Keep the exact Job for same-owner termination/reconciliation.
                self.scope_capture_ambiguous = True
                raise
            if not self.custody.bind_process(
                self.pid, self.host_start_time, "windows_job", self._scope_identity()
            ):
                raise WindowsCustodyUnavailable("windows_process_identity_rejected")
            # The native identity is valid while suspended; resume is a separate
            # phase and is never inferred from the bind.
            psutil.Process(self.pid).resume()
            self.resumed = True
            self.state = "resumed"
            return self.process
        except FileNotFoundError:
            if self.process is None:
                # CreateProcess reported that the executable/cwd was absent before returning
                # a process object.  This narrow no-effect result can release the reservation;
                # opaque constructor, access, signal, and post-create failures remain unknown.
                self.no_effect_verified = True
                self.state = "popen_refused"
            else:
                self.state = "unknown"
                self._record_unknown("partial_launch")
            raise
        except OSError:
            self.state = "unknown"
            self._record_unknown("partial_launch")
            raise
        except BaseException:  # health: allow BLE001 -- opaque post-launch interruption retains exact custody
            self.state = "unknown"
            self._record_unknown("partial_launch")
            raise

    def _scope_identity(self) -> str:
        return f"windows_job:{self.custody.owner_id}:{self.pid or 'unknown'}"

    def matches(self, process: Any, pid: Optional[int], start: Optional[int]) -> bool:
        return (
            process is self.process
            and type(pid) is int
            and pid == self.pid
            and type(start) is int
            and start == self.host_start_time
        )

    def _terminate_exact_job(self, *, require_scope: bool, exit_code: int = 1) -> bool:
        """Issue one guarded TerminateJobObject call on this exact retained Job.

        ``require_scope`` is false only for an assignment/capture ambiguity: the Job
        identity is retained, but no call here claims a successful scope bind or
        quiescence.  The same transaction and retry budget are used in both cases.
        """
        if sys.platform != "win32":
            raise WindowsCustodyUnavailable("windows_job_requires_native_windows")
        process = self.process
        job = self.job
        custody = self.custody
        assignment_proven = self.assigned is True or self.assignment_observation == "member"
        if (
            process is None
            or job is None
            or getattr(custody, "native_scope", None) is not self
            or not self.matches(process, self.pid, self.host_start_time)
            or not self._retained_scope_matches()
            or (require_scope and (self.assigned is not True or self.scope_captured is not True))
            or (not require_scope and (self.assignment_attempted is not True or not assignment_proven))
            or getattr(job, "_recovery_only", True) is True
        ):
            raise WindowsCustodyUnavailable("exact_windows_job_owner_required")
        if type(exit_code) is not int or not 0 <= exit_code <= 0xFFFFFFFF:
            raise ValueError("invalid_windows_job_exit_code")
        job_lock = getattr(job, "_lock", None)
        api = getattr(job, "_api", None)
        if job_lock is None or api is None:
            raise WindowsCustodyUnavailable("windows_job_api_unavailable")
        native_call_started = False
        try:
            with job_lock:
                if self.state == "termination_requested":
                    return False
                if self.state == "termination_failed":
                    if (
                        self.termination_retryable is not True
                        or self.termination_failures != 1
                        or self.termination_attempts != 1
                    ):
                        raise WindowsCustodyUnavailable("windows_job_retry_not_allowed")
                elif self.state not in {"resumed", "job_assigned", "unknown"} or self.termination_attempts != 0:
                    raise WindowsCustodyUnavailable("windows_job_not_terminable")
                if self.job_handle_closed:
                    raise WindowsCustodyUnavailable("windows_job_not_terminable")
                handle = getattr(job, "_handle", None)
                if handle is None:
                    raise WindowsCustodyUnavailable("windows_job_handle_closed")
                if self.termination_attempts == 0:
                    self.termination_job = job
                    self.termination_job_handle = handle
                elif self.termination_job is not job or self.termination_job_handle is not handle:
                    raise WindowsCustodyUnavailable("windows_job_identity_changed")
                terminate = api.TerminateJobObject
                terminate.argtypes = [wintypes.HANDLE, wintypes.UINT]
                terminate.restype = wintypes.BOOL
                self.termination_attempts += 1
                native_call_started = True
                if not terminate(handle, exit_code):
                    raise ctypes.WinError(ctypes.get_last_error())
                self.termination_retryable = False
                self.state = "termination_requested"
                return True
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            self.termination_failures += 1 if native_call_started else 0
            retryable = native_call_started and self.termination_failures == 1
            self.termination_retryable = retryable
            self.state = "termination_failed" if retryable else "unknown"
            try:
                self.custody.mark_unknown("termination_failed")
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as mark_exc:
                logger.warning(
                    "could not record native termination failure on retained custody: %s",
                    mark_exc, exc_info=True,
                )
            if isinstance(exc, WindowsCustodyUnavailable):
                raise
            raise WindowsCustodyUnavailable("windows_job_termination_failed") from exc

    def _observe_assignment_membership(self) -> Optional[bool]:
        """Observe ambiguous assignment through the original Job and process handles.

        ``Job.empty()`` answers only whether *some* member exists.  It cannot prove that the
        retained suspended child was never assigned.  The process-handle membership query is
        therefore the only accepted discriminator; unavailable or non-boolean results remain
        unknown and are retried against these same native objects.
        """
        if not self.assignment_ambiguous:
            return self.assigned if self.assigned else None
        process_ref = self.process_ref
        if self.job is None or process_ref is None or not self.matches(self.process, self.pid, self.host_start_time):
            self.assignment_observation = "query_unavailable"
            self.assignment_observation_failures += 1
            return None
        try:
            member = self.job.contains_process_handle(process_ref)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.assignment_observation = "query_unavailable"
            self.assignment_observation_failures += 1
            return None
        if member is True:
            self.assignment_observation = "member"
            return True
        if member is False:
            self.assignment_observation = "nonmember"
            return False
        self.assignment_observation = "query_unavailable"
        self.assignment_observation_failures += 1
        return None

    def _terminate_exact_process(self) -> bool:
        """Terminate an observed nonmember through the original Popen process handle."""
        if self.process is None or not self.matches(self.process, self.pid, self.host_start_time):
            raise WindowsCustodyUnavailable("exact_partial_process_required")
        if self.process_handle_closed:
            raise WindowsCustodyUnavailable("windows_process_handle_closed")
        if self.termination_attempts:
            if self.termination_process is not self.process:
                raise WindowsCustodyUnavailable("windows_process_identity_changed")
            if self.state == "termination_requested":
                return False
            if self.state != "termination_failed" or not self.termination_retryable:
                raise WindowsCustodyUnavailable("windows_process_retry_not_allowed")
        self.termination_process = self.process
        self.termination_attempts += 1
        try:
            # Popen.terminate() uses this exact process handle on Windows.  No PID reopen,
            # taskkill, or replacement Job is allowed after the assignment boundary.
            self.process.terminate()
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError) as exc:
            self.termination_failures += 1
            self.termination_retryable = self.termination_failures == 1
            self.state = "termination_failed" if self.termination_retryable else "unknown"
            self._record_unknown("partial_termination_failed")
            raise WindowsCustodyUnavailable("partial_process_termination_failed") from exc
        self.termination_retryable = False
        self.state = "termination_requested"
        self._record_unknown("assignment_nonmember_termination_requested")
        return True

    def terminate_owned_tree(self, exit_code: int = 1) -> bool:
        """Request termination through the retained exact Job handle.

        The process registry cannot use its PID/taskkill path after a foreground reader
        transfer: that path can race a recycled PID.  This method uses only the Job object
        created by this transaction and leaves its handles open for the original reader and
        normal EOF/durable/delivery/empty finalizer.
        """
        return self._terminate_exact_job(require_scope=True, exit_code=exit_code)

    def drain_after_process_exit(self, timeout: float) -> bool:
        """Drain this retained Job after root exit without closing reader-owned handles."""
        process = self.process
        if (
            process is None or process.poll() is None
            or self.custody.native_scope is not self
            or not self.matches(process, self.pid, self.host_start_time)
            or not self._retained_scope_matches()
            or (self.assigned and not self.scope_captured)
        ):
            return False
        if self.native_empty_observed:
            return True
        deadline = time.monotonic() + max(0.0, float(timeout))
        if not self.assigned:
            # No assignment was proven.  The process handle still has to be exited
            # and the exact Job must prove both nonmembership and emptiness before
            # the retained transaction can be retried.
            return self._observe_unassigned_empty() is True
        if self.observe_empty() is True:
            return True
        if self.scope_observation != "nonempty":
            return False
        try:
            self.terminate_owned_tree()
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return False
        wake = threading.Event()
        while True:
            if self.observe_empty() is True:
                return True
            remaining = deadline - time.monotonic()
            if self.scope_observation != "nonempty" or remaining <= 0:
                return False
            wake.wait(min(0.01, remaining))

    def _rebind_observed_member(self) -> None:
        """Re-capture and bind the original Job member before partial termination.

        ``contains_process_handle`` proves membership of this exact suspended Popen in
        this exact Job; it does not by itself populate the custody identity or scope
        fields.  Commit ``assigned`` only after the same Job/process/start tuple is
        captured and the existing coordinator lease is reopened.
        """
        if (
            self.assignment_observation != "member"
            or self.job is None
            or self.process is None
            or not self.matches(self.process, self.pid, self.host_start_time)
            or self.custody is None
        ):
            raise WindowsCustodyUnavailable("windows_member_rebind_identity_required")
        previous_assigned = self.assigned
        previous_scope = self.scope_captured
        self.assigned = True
        try:
            self._capture_scope_identity()
            if not self.custody.bind_process(
                self.pid, self.host_start_time, "windows_job", self._scope_identity()
            ):
                raise WindowsCustodyUnavailable("windows_member_identity_rejected")
            handle = getattr(self.custody, "handle", None)
            coordinator = getattr(self.custody, "coordinator", None)
            if handle is None or coordinator is None:
                raise WindowsCustodyUnavailable("windows_member_lease_unavailable")
            refusal = coordinator.reconcile_unknown(
                handle,
                owner_id=self.custody.owner_id,
                profile_id=self.custody.profile_id,
                process_instance=self.custody.process_instance,
                authority_epoch=self.custody.authority_epoch,
            )
            if refusal is not None:
                raise WindowsCustodyUnavailable("windows_member_lease_refused")
        except BaseException as exc:  # health: allow BLE001 -- rebind failures retain the exact native owner
            self.assigned = previous_assigned
            self.scope_captured = previous_scope
            self.scope_capture_ambiguous = True
            self.state = "unknown"
            self._record_unknown("assignment_member_rebind_failed")
            if isinstance(exc, WindowsCustodyUnavailable):
                raise
            raise WindowsCustodyUnavailable("windows_member_rebind_failed") from exc
        self.assigned = True
        self.assignment_ambiguous = False
        self.scope_capture_ambiguous = False
        self.state = "job_assigned"
        self.custody.state = "accepted"
        self.custody.reason = ""

    def terminate_partial_exact(self) -> bool:
        """Terminate the exact retained process or Job after a partial native phase.

        An ambiguous assignment first requires an exact Job membership observation.  A
        proven member re-captures the same Job/process/start identity and reopens the
        existing lease before using that Job; a proven nonmember uses the retained
        suspended-child handle.  A failed membership query or rebind remains unknown
        and never guesses from Job.empty().
        """
        if self.process is None or not self.matches(self.process, self.pid, self.host_start_time):
            raise WindowsCustodyUnavailable("exact_partial_process_required")
        if self.assigned:
            return self._terminate_exact_job(require_scope=False)
        if self.assignment_ambiguous:
            membership = self._observe_assignment_membership()
            if membership is True:
                self._rebind_observed_member()
                return self._terminate_exact_job(require_scope=False)
            if membership is None:
                raise WindowsCustodyUnavailable("windows_assignment_membership_unknown")
            return self._terminate_exact_process()
        if self.assignment_attempted:
            raise WindowsCustodyUnavailable("windows_assignment_effect_unknown")
        return self._terminate_exact_process()

    def _retain_launch_identity(self) -> None:
        """Anchor native objects before the assignment attempt can become ambiguous."""
        job_handle = getattr(self.job, "_handle", None)
        if (
            self.job is None or self.process is None
            or job_handle is None or not self.matches(self.process, self.pid, self.host_start_time)
        ):
            raise WindowsCustodyUnavailable("windows_scope_identity_unavailable")
        if self.scope_observation_job is not None:
            if not self._retained_scope_matches():
                raise WindowsCustodyUnavailable("windows_scope_identity_changed")
            return
        self.scope_observation_job = self.job
        self.scope_observation_job_handle = job_handle
        self.scope_observation_process = self.process
        self.scope_observation_pid = self.pid
        self.scope_observation_start_time = self.host_start_time

    def _retained_scope_matches(self) -> bool:
        job_handle_matches = (
            self.job_handle_closed is False
            and getattr(self.job, "_handle", None) is self.scope_observation_job_handle
        ) or (
            self.job_handle_closed is True
            and self.close_job is self.scope_observation_job
            and self.close_job_handle is self.scope_observation_job_handle
            and getattr(self.job, "_handle", None) is None
        )
        return bool(
            self.scope_observation_job is not None
            and self.scope_observation_job_handle is not None
            and self.job is self.scope_observation_job
            and self.process is self.scope_observation_process
            and self.matches(self.process, self.scope_observation_pid, self.scope_observation_start_time)
            and job_handle_matches
        )

    def _capture_scope_identity(self) -> None:
        """Mark assigned custody only for the original captured native objects."""
        if not self.assigned or not self._retained_scope_matches():
            raise WindowsCustodyUnavailable("windows_scope_identity_unavailable")
        self.scope_captured = True

    def _observe_unassigned_empty(self) -> Optional[bool]:
        """Close the assignment-ambiguous branch only after two exact observations.

        The original process handle must report exited, and the original Job must report both
        that this process is not a member and that the Job is empty.  A Job-empty result without
        the per-process nonmembership observation is deliberately insufficient.
        """
        if not self._retained_scope_matches():
            self.scope_observation = "unavailable"
            return None
        process_ref = self.process_ref
        if process_ref is None:
            self.scope_observation = "unavailable"
            return None
        try:
            member = self.job.contains_process_handle(process_ref)
            empty = self.job.empty()
            exited = self.process.poll() is not None
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.scope_observation = "query_unavailable"
            self.scope_observation_failures += 1
            return None
        if member is not False:
            self.scope_observation = "foreign" if member is True else "query_unavailable"
            if member is None:
                self.scope_observation_failures += 1
            return None
        if empty is not True:
            self.scope_observation = "nonempty" if empty is False else "query_unavailable"
            if empty is None:
                self.scope_observation_failures += 1
            return None
        if not exited:
            self.scope_observation = "process_alive"
            return None
        # A post-Popen failure can happen before the Job assignment call starts.  In
        # that branch ``assigned`` is deliberately still false, but the exact Job,
        # process handle, and process identity are retained.  Only this direct
        # membership + empty + exited observation may close that partial transaction;
        # a missing observation remains UNKNOWN and retryable on the same handles.
        self.assignment_observation = "nonmember"
        self.scope_observation = "empty_nonmember"
        self.native_empty_observed = True
        return True

    def observe_empty(self) -> Optional[bool]:
        """Return true only for the exact retained process handle and Job."""
        if self.assignment_observation == "nonmember":
            return self._observe_unassigned_empty()
        if self.job is None or self.process is None or not self.assigned or not self.scope_captured:
            self.scope_observation = "unavailable"
            return None
        process_ref = self.process_ref
        if process_ref is None or self.scope_observation_job is None:
            self.scope_observation = "unavailable"
            return None
        if (
            self.job is not self.scope_observation_job
            or getattr(self.job, "_handle", None) is not self.scope_observation_job_handle
            or self.process is not self.scope_observation_process
            or not self.matches(self.process, self.scope_observation_pid, self.scope_observation_start_time)
        ):
            self.scope_observation = "foreign"
            return None
        try:
            member = self.job.contains_process_handle(process_ref)
            empty = self.job.empty()
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.scope_observation = "query_unavailable"
            self.scope_observation_failures += 1
            return None
        if member is not True:
            self.scope_observation = "foreign" if member is False else "query_unavailable"
            if self.scope_observation == "query_unavailable":
                self.scope_observation_failures += 1
            return None
        if empty is not True:
            if empty is False:
                self.scope_observation = "nonempty"
            else:
                self.scope_observation = "query_unavailable"
                self.scope_observation_failures += 1
            return None
        self.scope_observation = "empty"
        self.native_empty_observed = True
        return True

    def close_after_observed_empty(self) -> bool:
        """Close the Job and retained process handle after the exact empty proof."""
        if not self._retained_scope_matches():
            self.scope_observation = "foreign"
            self._record_unknown("foreign_custody")
            return False
        if not self.native_empty_observed and self.observe_empty() is not True:
            return False
        if self.state == "closed":
            return True
        if self.state not in {"resumed", "job_assigned", "termination_requested", "close_failed", "unknown"}:
            return False
        if (
            not self.scope_captured or not self.assigned
        ) and self.assignment_observation != "nonmember":
            return False
        if self.state == "close_failed" and (
            self.close_retryable is not True
            or self.close_attempts != 1
            or self.close_failures != 1
        ):
            self.state = "unknown"
            self.custody.mark_unknown("join_failed")
            return False
        try:
            job = self.job
            job_handle = job._handle
            if self.close_attempts == 0:
                self.close_job = job
                self.close_job_handle = job_handle
            elif self.close_job is not job or self.close_job_handle is not job_handle:
                raise WindowsCustodyUnavailable("windows_job_close_identity_changed")
            if not self.process_handle_closed:
                ref = self.process_ref
                if ref is None or not hasattr(ref.raw_handle, "Close"):
                    return False
                self.close_attempts += 1
                ref.raw_handle.Close()
                self.process_handle_closed = True
            # Close the KILL_ON_JOB_CLOSE owner only after the exact process handle has
            # closed.  If either close raises, the other native object remains reachable
            # for a same-generation retry instead of leaving an unqueryable half-close.
            if not self.job_handle_closed:
                if self.close_attempts == 0:
                    self.close_attempts = 1
                job.close()
                self.job_handle_closed = True
            self.close_retryable = False
            self.state = "closed"
            return True
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.close_failures += 1
            self.close_retryable = self.close_attempts == 1 and self.close_failures == 1
            self.state = "close_failed" if self.close_retryable else "unknown"
            self.custody.mark_unknown("join_failed")
            return False

    def release_before_effect(self) -> bool:
        """Release only when no Popen object was created and the Job was never assigned."""
        if self.has_process_effect or self.assigned or not self.no_effect_verified:
            self.state = "unknown"
            self.custody.mark_unknown(
                "partial_launch" if self.has_process_effect or self.assigned else "native_launch_ambiguous"
            )
            return False
        try:
            if self.job is not None and not self.job_handle_closed:
                self.job.close()
                self.job_handle_closed = True
            self.state = "released"
            return self.custody.release_unaccepted()
        except (OSError, RuntimeError, TypeError, ValueError):
            self.state = "unknown"
            self.custody.mark_unknown("join_failed")
            return False


def begin_windows_launch(custody: Any) -> WindowsLaunchTransaction:
    """Create a transaction without importing or pretending to support another OS."""
    if sys.platform != "win32":
        raise WindowsCustodyUnavailable("windows_job_requires_native_windows")
    return WindowsLaunchTransaction(custody)
