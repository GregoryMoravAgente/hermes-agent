"""Process-session construction for the shared process registry.

The registry facade keeps its public ``ProcessSession`` and WSL patch seams.  This mixin owns
only the pre-effect session construction and delegates custody reservation to the existing
registry custody mixin.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
import time
import uuid
from contextlib import suppress
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from tools.process_registry import ProcessSession


logger = logging.getLogger("tools.process_registry")


class ProcessRegistrySpawnMixin:
    """Construct a session before any local process effect is allowed."""

    @staticmethod
    def _mark_i03_scope_unknown(custody, binding, reason: str) -> None:
        owner = custody if custody is not None else binding
        mark_unknown = getattr(owner, "mark_unknown", None)
        if callable(mark_unknown):
            mark_unknown(reason)

    def _resolve_i03_scope(self, owner_task_id, parent_handle, scope_fence):
        from gateway.session_context import get_i03_custody_binding

        binding = get_i03_custody_binding()
        if binding is not None:
            expected_owner_task_id = getattr(binding, "process_owner_task_id", None)
            if not isinstance(expected_owner_task_id, str) or not expected_owner_task_id:
                expected_owner_task_id = getattr(binding, "process_task_id", None)
            if owner_task_id != expected_owner_task_id:
                raise RuntimeError("I03 process task identity mismatch")
            if parent_handle is None:
                parent_handle = getattr(binding, "handle", None)
            if scope_fence is None:
                scope_fence = getattr(binding, "process_scope", None)
            if parent_handle is None or scope_fence is None:
                raise RuntimeError("I03 process custody binding is incomplete")
        if scope_fence is not None and scope_fence.registry is not self:
            raise RuntimeError("I03 process scope belongs to another registry")
        return binding, parent_handle, scope_fence

    def _record_i03_scope(self, session, owner_task_id, custody, scope_fence, binding):
        if getattr(custody, "foreground_reserved", False):
            if not scope_fence.release_foreground(getattr(custody, "owner_id", "")):
                self._mark_i03_scope_unknown(custody, binding, "process_scope_unavailable")
                raise RuntimeError("I03 foreground reservation is not current")
            custody.foreground_reserved = False
        if not scope_fence.record_session(session.id, owner_task_id):
            self._mark_i03_scope_unknown(custody, binding, "process_scope_unavailable")
            raise RuntimeError("I03 process scope is closed or mismatched")

    def _new_session(
        self, command, task_id, owner_task_id, session_key, cwd, *,
        custody=None, reserve_custody: bool = True, parent_session_id: Optional[str] = None,
        parent_handle=None,
        scope_fence=None, **extra,
    ) -> ProcessSession:
        from gateway.session_context import get_session_env

        from tools.process_registry import ProcessSession, _is_wsl_launcher_command

        binding, parent_handle, scope_fence = self._resolve_i03_scope(
            owner_task_id, parent_handle, scope_fence,
        )
        if custody is not None:
            if parent_handle is None:
                parent_handle = getattr(custody, "parent_handle", None)
            if scope_fence is None:
                scope_fence = getattr(custody, "scope_fence", None)

        session = ProcessSession(
            id=f"proc_{uuid.uuid4().hex[:12]}", command=command, task_id=task_id,
            owner_task_id=owner_task_id, session_key=session_key, cwd=cwd,
            parent_session_id=(
                parent_session_id if parent_session_id is not None
                else get_session_env("HERMES_SESSION_ID", "")
            ),
            wsl_chain=_is_wsl_launcher_command(command),
            started_at=time.time(), custody=custody, **extra)
        if custody is not None and scope_fence is not None:
            self._record_i03_scope(session, owner_task_id, custody, scope_fence, binding)
        if custody is not None:
            if self._runtime_custody is None or custody.coordinator is not self._runtime_custody:
                raise RuntimeError("foreground custody belongs to a different runtime coordinator")
            session.profile_id = custody.profile_id
            session.process_instance = custody.process_instance
            session.authority_epoch = custody.authority_epoch
            session.custody_owner_id = custody.owner_id
            session.custody_state = custody.state
            self._custody_observed = True
        elif reserve_custody:
            if scope_fence is not None and not scope_fence.record_session(session.id, owner_task_id):
                self._mark_i03_scope_unknown(custody, binding, "process_scope_unavailable")
                raise RuntimeError("I03 process scope is closed or mismatched")
            self._reserve_process_custody(
                session, parent_handle=parent_handle, scope_fence=scope_fence,
            )
        elif self._runtime_custody is not None:
            # The process already exists, but no caller supplied the pre-effect lease. Keep the
            # real child visible and block the I15 close rather than granting a late reservation.
            if scope_fence is not None and not scope_fence.record_session(session.id, owner_task_id):
                self._mark_i03_scope_unknown(custody, binding, "process_scope_unavailable")
                raise RuntimeError("I03 process scope is closed or mismatched")
            session.profile_id = self._process_profile_id()
            session.process_instance = self._runtime_custody.process_instance
            epoch_for_profile = getattr(
                self._runtime_custody, "authority_epoch_for_profile", None,
            )
            session.authority_epoch = (
                epoch_for_profile(session.profile_id, require_serving=False)
                if callable(epoch_for_profile) else self._runtime_custody.authority_epoch
            ) or 0
            session.custody_state = "unknown"
            session.custody_error = (
                "late_adoption" if session.authority_epoch else "foreign_custody"
            )
            self._custody_observed = True
            self._custody_unbound_sessions.add(session.id)
        return session


    def _publish_started_session(self, session: ProcessSession, *, process=None, transaction=None) -> None:
        """Publish the exact post-effect session before any fallible child setup.

        The registry index is the recovery root for both the reader and a native
        transaction.  It is intentionally written while the existing registry lock
        is held, before custody binding, reader start, checkpointing, or nonessential
        synchronization can fail.
        """
        if process is not None:
            session.process = process
            session.pid = getattr(process, "pid", None)
            session.host_start_time = self._safe_host_start_time(session.pid)
        if transaction is not None and session.custody is not None:
            # Prefer the transaction's authenticated birth observation over a second
            # best-effort host query; this keeps the callback bound to one identity.
            if type(getattr(transaction, "host_start_time", None)) is int:
                session.host_start_time = transaction.host_start_time
            if getattr(session.custody, "native_scope", None) not in (None, transaction):
                raise RuntimeError("native transaction identity changed")
            session.custody.native_scope = transaction
        with self._lock:
            existing = self._running.get(session.id)
            if existing is not None and existing is not session:
                raise RuntimeError("process session identity collision")
            self._prune_if_needed()
            self._running[session.id] = session

    def _recover_native_transaction(self, session: ProcessSession, original: BaseException, phase: str) -> None:
        """Best-effort retention boundary which never replaces the original failure."""
        secondary: list[str] = []
        try:
            self._publish_started_session(
                session, process=getattr(session.custody.native_scope, "process", None)
                if session.custody is not None and getattr(session.custody, "native_scope", None) is not None
                else getattr(session, "process", None),
                transaction=getattr(session.custody, "native_scope", None)
                if session.custody is not None else None,
            )
        except BaseException as exc:  # retention must not mask the launch exception
            logger.exception("Background process _recover_native_transaction failed; retaining existing custody")
            secondary.append(f"publish:{type(exc).__name__}")
        try:
            if session.custody is not None:
                self._custody_unknown(session, f"native_{phase}_failed")
        except BaseException as exc:  # health: preserve the original failure while recording secondary custody errors
            logger.exception("Background process _recover_native_transaction failed; retaining existing custody")
            secondary.append(f"custody:{type(exc).__name__}")
        try:
            self._write_checkpoint()
        except BaseException as exc:  # health: preserve the original failure while recording secondary custody errors
            logger.exception("Background process _recover_native_transaction failed; retaining existing custody")
            secondary.append(f"checkpoint:{type(exc).__name__}")
        if secondary:
            logger.warning(
                "native process retention had secondary failures during %s: %s",
                phase, ",".join(secondary), exc_info=True,
            )

    def _ensure_reader_registered(self, session: ProcessSession) -> bool:
        """Register exactly the original reader once when construction never began."""
        if session is None or session.exited or session.reader_phase != "not_started":
            return False
        reader = session._reader_thread
        if session.process is None or session.id not in self._running:
            return False
        if reader is None:
            if session._reader_target is None or session._reader_registration_retries:
                return False
            from contextvars import copy_context
            session._reader_registration_retries += 1
            try:
                reader = threading.Thread(
                    target=copy_context().run,
                    args=(session._reader_target, session, *session._reader_extra_args),
                    daemon=True, name=session._reader_name,
                )
            except BaseException:  # health: retain the exact session even when cleanup/checkpoint handling is interrupted
                logger.exception("Background process _ensure_reader_registered failed; retaining existing custody")
                session.reader_phase = "not_started"
                return False
            session._reader_thread = reader
        session.reader_phase = "starting"
        try:
            reader.start()
        except BaseException:  # health: retain the exact session even when cleanup/checkpoint handling is interrupted
            logger.exception("Background process _ensure_reader_registered failed; retaining existing custody")
            session.reader_phase = "ambiguous"
            try:
                self._write_checkpoint()
            except BaseException:  # health: retain the exact session even when cleanup/checkpoint handling is interrupted
                logger.exception("Background process _ensure_reader_registered failed; retaining existing custody")
            return False
        session.reader_phase = "started"
        try:
            self._write_checkpoint()
        except BaseException:  # health: retain the exact session even when cleanup/checkpoint handling is interrupted
            # The reader and exact session remain reachable; the checkpoint failure is
            # not permission to construct a second reader.
            raise
        return True


# Extracted local-producer lifecycle methods.  The facade keeps the public class through MRO.
    def _prepare_reader(self, session: ProcessSession, reader_target, reader_name: str, extra_args=()) -> None:
        """Record the one reader owner before any post-Popen launch step can fail."""
        session._reader_target = reader_target
        session._reader_name = reader_name
        session._reader_extra_args = tuple(extra_args)

    def _track_started(self, session: ProcessSession, reader_target, reader_name: str, extra_args=()) -> None:
        """Register the session before starting the one reader; retain ambiguous starts."""
        from contextvars import copy_context

        self._prepare_reader(session, reader_target, reader_name, extra_args)
        # A post-Popen child is published before reader construction.  This is the
        # recovery root even when Thread construction or start raises.
        with self._lock:
            self._prune_if_needed()
            self._running[session.id] = session
        try:
            reader = threading.Thread(
                target=copy_context().run, args=(reader_target, session, *extra_args),
                daemon=True, name=reader_name,
            )
        except BaseException:  # health: retain the exact session even when cleanup/checkpoint handling is interrupted
            session.reader_phase = "not_started"
            try:
                self._write_checkpoint()
            except BaseException:  # health: retain the exact session even when cleanup/checkpoint handling is interrupted
                logger.exception("Background process _track_started failed; retaining existing custody")
            raise
        session._reader_thread = reader
        session.reader_phase = "starting"
        try:
            reader.start()
        except BaseException:  # health: retain the exact session even when cleanup/checkpoint handling is interrupted
            session.reader_phase = "ambiguous"
            try:
                self._write_checkpoint()
            except BaseException:  # health: retain the exact session even when cleanup/checkpoint handling is interrupted
                logger.exception("Background process _track_started failed; retaining existing custody")
            raise
        session.reader_phase = "started"
        self._write_checkpoint()


    def _retain_untracked_after_custody_failure(self, session: ProcessSession) -> None:
        """Keep the exact session visible; secondary retention errors never mask the source error."""
        secondary: list[str] = []
        try:
            self._publish_started_session(session)
        except BaseException as exc:  # health: preserve the original failure; custody retention is secondary
            logger.exception("Background process _retain_untracked_after_custody_failure failed; retaining existing custody")
            secondary.append(f"publish:{type(exc).__name__}")
        try:
            self._write_checkpoint()
        except BaseException as exc:  # health: preserve the original failure; custody retention is secondary
            logger.exception("Background process _retain_untracked_after_custody_failure failed; retaining existing custody")
            secondary.append(f"checkpoint:{type(exc).__name__}")
        if secondary:
            logger.warning(
                "background custody retention had secondary failures: %s",
                ",".join(secondary), exc_info=True,
            )


    def _mark_sandbox_bind_failure(self, session: ProcessSession, original: BaseException) -> None:
        """Keep a marker-visible sandbox child unknown until its exact owner is rebound.

        The marker PID is only a lookup aid inside the already retained session.  It never
        proves custody.  The retry below must revalidate the existing coordinator handle,
        owner/profile/process/epoch tuple, and the same sandbox marker identity.
        """
        # ProcessSession is an existing public dataclass kept below the configured
        # size cap.  These two private attributes are ephemeral retry markers only;
        # they never enter a checkpoint or stand in for a custody receipt.
        session._sandbox_bind_retry_pending = True
        session._sandbox_bind_error_type = type(original).__name__
        marker_pid = getattr(session, "_sandbox_marker_pid", None)
        if marker_pid is not None and session.pid is not None and marker_pid != session.pid:
            self._custody_unknown(session, "process_identity_changed")
            return
        self._custody_unknown(session, "partial_launch")

    def _reconcile_sandbox_bind(self, session: ProcessSession) -> bool:
        """Reopen one exact sandbox lease after the same marker child binds successfully."""
        custody = session.custody
        if (
            custody is None
            or getattr(session, "_sandbox_bind_retry_pending", False) is not True
            or custody.state != "unknown"
            or not custody.process_bound
            or custody.pid != session.pid
        ):
            return False
        handle = getattr(custody, "handle", None)
        coordinator = getattr(custody, "coordinator", None)
        if handle is None or coordinator is None or not all((
            session.custody_owner_id == custody.owner_id,
            session.profile_id == custody.profile_id,
            session.process_instance == custody.process_instance,
            session.authority_epoch == custody.authority_epoch,
        )):
            return False
        try:
            refusal = coordinator.reconcile_unknown(
                handle,
                owner_id=custody.owner_id,
                profile_id=custody.profile_id,
                process_instance=custody.process_instance,
                authority_epoch=custody.authority_epoch,
            )
        except BaseException:
            logger.exception("Background process _reconcile_sandbox_bind failed; retaining existing custody")
            return False
        if refusal is not None:
            return False
        custody.state = "accepted"
        custody.reason = ""
        session._sandbox_bind_retry_pending = False
        session._sandbox_bind_error_type = ""
        self._sync_process_custody(session)
        return True

    def _sandbox_bind_attempt(self, session: ProcessSession, *, retry: bool = False) -> bool:
        """Attempt one bind on the existing marker session, optionally reopening its lease."""
        try:
            self._custody_bind_process(session)
        except BaseException as bind_exc:
            logger.exception("Background process _sandbox_bind_attempt failed; retaining existing custody")
            self._mark_sandbox_bind_failure(session, bind_exc)
            return False
        if session.custody is not None and not session.custody.process_bound:
            self._mark_sandbox_bind_failure(
                session, RuntimeError("sandbox custody bind incomplete"),
            )
            return False
        return not retry or self._reconcile_sandbox_bind(session)

    def _sandbox_bind_failure_or_unknown(self, session: ProcessSession, original: BaseException) -> None:
        if self._runtime_custody is not None and session.custody is not None:
            # An env.execute exception can follow a real child launch before its PID
            # response reaches this process.  Preserve the accepted lease and wait for
            # the durable marker; an empty response is never proof of no effect.
            self._mark_sandbox_bind_failure(session, original)
            return
        if session.pid is not None:
            self._mark_sandbox_bind_failure(session, original)
        else:
            self._custody_unknown(session, "partial_launch")

    def _sandbox_bind_wait(self, session: ProcessSession) -> bool:
        if not getattr(session, "_sandbox_bind_retry_pending", False):
            return False
        self._custody_unknown(session, "partial_launch")
        return True

    def _sandbox_bind_retry(self, session: ProcessSession) -> bool:
        return (
            not getattr(session, "_sandbox_bind_retry_pending", False)
            or self._sandbox_bind_attempt(session, retry=True)
        )

    def _sandbox_marker_observe(self, session: ProcessSession, observed_pid: int) -> bool:
        """Record one namespace-local marker without manufacturing host identity."""
        marker_pid = getattr(session, "_sandbox_marker_pid", None)
        if marker_pid is not None and marker_pid != observed_pid:
            self._custody_unknown(session, "process_identity_changed")
            return False
        if marker_pid is None:
            # This is only a durable marker lookup, never a custody receipt.  It
            # becomes immutable once the first PID is observed.
            session._sandbox_marker_pid = observed_pid
        if session.pid is None:
            session.pid = observed_pid
        elif session.pid != observed_pid:
            self._custody_unknown(session, "process_identity_changed")
            return False
        # Docker/sandbox PIDs are namespace-local.  _safe_host_start_time() is a
        # host-PID lookup and must never become a sandbox custody or release proof.
        session.host_start_time = None
        return True

    def _sandbox_terminal_observe(
        self, session: ProcessSession, observed_pid: Optional[int], exit_output: str,
    ) -> tuple[bool, Optional[int]]:
        """Accept one same-marker terminal record while custody remains explicit."""
        if not exit_output:
            return False, None
        if observed_pid is not None and not self._sandbox_marker_observe(session, observed_pid):
            return False, None
        marker_pid = getattr(session, "_sandbox_marker_pid", None)
        if marker_pid is None or session.pid is None or marker_pid != session.pid:
            # A terminal marker without the same launch marker is not identity proof.
            return False, None
        try:
            exit_code = int(exit_output.splitlines()[-1].strip())
        except (ValueError, IndexError):
            exit_code = -1
        return True, exit_code

    def _sandbox_finish_observed_exit(
        self, session: ProcessSession, observed_pid: Optional[int], exit_output: str,
    ) -> bool:
        observed, exit_code = self._sandbox_terminal_observe(session, observed_pid, exit_output)
        if not observed:
            return False
        # Log/exit observation is a terminal result, not a claim that the sandbox
        # process has an authenticated host Job or birth-time binding.
        self._custody_process_finished(session)
        self._finish_exited(session, exit_code if exit_code is not None else -1)
        return True

    def _sandbox_bind_observe(self, session: ProcessSession, observed_pid: int) -> bool:
        if not self._sandbox_marker_observe(session, observed_pid):
            return False
        return self._sandbox_bind_retry(session)


    def _retain_after_post_effect_failure(
        self, session: ProcessSession, original: BaseException, phase: str, *, process=None,
    ) -> None:
        """Retain one exact post-effect session while preserving its first failure.

        Every operation after Popen/PTY creation is recovery work.  A failed bind, reader
        construction/start, checkpoint, or nonessential sync must leave the original session
        in the registry and must never turn a secondary cleanup exception into the caller's
        error.  Native transactions use their exact retained Job/Popen path; portable sessions
        retain the same process/PTY object and only the no-custody path reaps that exact object.
        """
        native_scope = (
            getattr(session.custody, "native_scope", None)
            if session.custody is not None else None
        )
        if native_scope is not None:
            self._recover_native_transaction(session, original, phase)
            return

        secondary: list[str] = []

        def guarded(label: str, operation) -> None:
            try:
                operation()
            except BaseException as exc:  # retention is secondary to the source failure
                logger.exception("Background process guarded failed; retaining existing custody")
                secondary.append(f"{label}:{type(exc).__name__}")

        if process is not None and session.process is None:
            guarded("publish", lambda: self._publish_started_session(session, process=process))
        else:
            guarded("publish", lambda: self._publish_started_session(session))
        if session.custody is not None:
            guarded("custody", lambda: self._custody_unknown(session, f"{phase}_failed"))
        elif process is not None:
            # No authenticated custody exists on this path.  Reap only the exact Popen object
            # already retained above; do not reopen or signal a PID discovered later.
            guarded("reap", lambda: self._reap_untracked(session, process))
        guarded("checkpoint", self._write_checkpoint)
        if secondary:
            logger.warning(
                "post-effect process retention had secondary failures during %s: %s",
                phase, ",".join(secondary), exc_info=True,
            )


    def _spawn_local_pty(self, session: ProcessSession, safe_command: str, env_vars: dict) -> ProcessSession:
        """PTY spawn for interactive CLI tools (Codex, Claude Code, REPLs).
        Raises ImportError when no PTY backend is installed and re-raises any spawn
        failure; ``spawn_local`` falls back to pipe mode in both cases."""
        from tools.process_registry import _IS_WINDOWS, systemd_user_bus_env
        if _IS_WINDOWS:
            from winpty import PtyProcess as _PtyProcessCls
        else:
            from ptyprocess import PtyProcess as _PtyProcessCls
        pty_argv = self._scope_argv(session, safe_command, session.id, "PTY")
        pty_env = self._spawn_env(env_vars)
        if session.systemd_unit:
            pty_env = systemd_user_bus_env(pty_env)
        # A PTY is a real TTY, so pager-happy tools (git log/diff, man) WILL page and
        # hang waiting for `q` — default them to cat, honoring any pager the user set.
        pty_env.setdefault("GIT_PAGER", "cat")
        pty_env.setdefault("PAGER", "cat")
        self._prepare_reader(session, self._pty_reader_loop, f"proc-pty-reader-{session.id}")
        self._i15_before_process_effect(session)
        effect = False
        try:
            pty_proc = _PtyProcessCls.spawn(
                pty_argv, cwd=session.cwd, env=pty_env, dimensions=(30, 120),
            )
            effect = True
        finally:
            self._i15_after_process_effect(session, effect=effect)
        session.pid = pty_proc.pid
        session.host_start_time = self._safe_host_start_time(session.pid)
        session._pty = pty_proc
        self._publish_started_session(session)
        try:
            self._custody_bind_process(session)
            self._track_started(session, self._pty_reader_loop, f"proc-pty-reader-{session.id}")
        except BaseException as exc:
            self._retain_after_post_effect_failure(session, exc, "pty_post_effect")
            raise
        return session


    def spawn_local(
        self, command: str, cwd: str | None = None, task_id: str = "", session_key: str = "",
        env_vars: dict | None = None, use_pty: bool = False, owner_task_id: str = "",
        persist_on_release: bool = False, parent_handle=None, scope_fence=None,
        notify_on_complete: bool = False) -> ProcessSession:
        """Spawn a background process locally (TERMINAL_ENV=local; other backends use
        spawn_via_env()). ``use_pty`` requests a pseudo-terminal via ptyprocess/pywinpty
        for interactive CLIs, falling back to a plain pipe when unavailable or failing.
        ``persist_on_release`` keeps the process out of agent-lifecycle kill sweeps (#41225)."""
        from tools.process_registry import _resolve_safe_cwd

        session = self._new_session(
            command, task_id, owner_task_id, session_key, _resolve_safe_cwd(cwd or os.getcwd()),
            persist_on_release=persist_on_release, parent_handle=parent_handle,
            scope_fence=scope_fence, notify_on_complete=notify_on_complete,
        )
        safe_command, log = self._prepare_local_spawn(session, command)
        pty_session, pty_scope_attempted = self._try_local_pty(
            session, safe_command, env_vars, use_pty=use_pty, log=log,
        )
        if pty_session is not None:
            return pty_session
        return self._spawn_local_pipe(session, safe_command, env_vars, log, pty_scope_attempted)

    def _prepare_local_spawn(self, session, command):
        """Prepare command/output and release only proven no-effect custody on failure."""
        from tools.process_registry import worker_output_log
        from tools.terminal_tool_sudo import _rewrite_compound_background as _rewrite_bg

        try:
            safe_command = _rewrite_bg(command)
        except BaseException as exc:  # health: preserve the original failure; custody retention is secondary
            try:
                released = self._custody_release_unaccepted(session)
            except BaseException as release_exc:  # health: preserve the original failure if cleanup also fails
                logger.warning(
                    "pre-effect command preparation release failed: %s",
                    type(release_exc).__name__, exc_info=True,
                )
                released = False
            if not released:
                self._recover_native_transaction(session, exc, "pre_effect_preparation")
            raise
        # A worker's pipe or PTY dies with the worker (the child's next write is SIGPIPE): write to a log.
        try:
            log = worker_output_log(session.id)
        except BaseException as exc:  # health: preserve the original failure; custody retention is secondary
            try:
                released = self._custody_release_unaccepted(session)
            except BaseException as release_exc:  # health: preserve the original failure if cleanup also fails
                logger.warning(
                    "pre-effect output-log preparation release failed: %s",
                    type(release_exc).__name__, exc_info=True,
                )
                released = False
            if not released:
                self._recover_native_transaction(session, exc, "pre_effect_log")
            raise
        return safe_command, log

    def _try_local_pty(self, session, safe_command, env_vars, *, use_pty, log):
        """Fallback only after the original PTY attempt is proven effect-free."""
        from tools.process_registry import _stop_systemd_unit

        pty_scope_attempted = False
        if use_pty and log is None:
            try:
                return self._spawn_local_pty(session, safe_command, env_vars), False
            except ImportError:
                logger.warning("ptyprocess not installed, falling back to pipe mode")
            except Exception as e:
                logger.warning("PTY spawn failed (%s), falling back to pipe mode", e)
                process_effect = session.pid is not None or session._pty is not None
                if process_effect:
                    # Once the native PTY object or PID exists, a second pipe launch could
                    # duplicate the command. Retain unknown custody for reconciliation.
                    self._retain_after_post_effect_failure(session, e, "pty_partial")
                    raise
                if session.systemd_unit:
                    pty_scope_attempted = True
                    try:
                        stopped = _stop_systemd_unit(session.systemd_unit)
                    except BaseException as stop_exc:
                        logger.warning(
                            "PTY scope stop failed after setup error: %s",
                            type(stop_exc).__name__, exc_info=True,
                        )
                        stopped = False
                    if not stopped:
                        self._retain_after_post_effect_failure(session, e, "pty_scope_stop")
                        raise
                    try:
                        released = self._custody_release_unaccepted(session)
                    except BaseException as release_exc:
                        logger.warning(
                            "PTY custody release failed after setup error: %s",
                            type(release_exc).__name__, exc_info=True,
                        )
                        released = False
                    if not released:
                        self._retain_after_post_effect_failure(session, e, "pty_release")
                        raise
                    session.systemd_unit = ""
                    try:
                        self._reserve_process_custody(session)
                    except BaseException:
                        self._retain_after_post_effect_failure(session, e, "pty_fallback_reserve")
                        raise
                else:
                    try:
                        released = self._custody_release_unaccepted(session)
                    except BaseException as release_exc:
                        logger.warning(
                            "PTY custody release failed after setup error: %s",
                            type(release_exc).__name__, exc_info=True,
                        )
                        released = False
                    if not released:
                        self._retain_after_post_effect_failure(session, e, "pty_release")
                        raise
                    try:
                        self._reserve_process_custody(session)
                    except BaseException:
                        self._retain_after_post_effect_failure(session, e, "pty_fallback_reserve")
                        raise
            except BaseException as exc:  # health: allow BLE001 -- preserve the original PTY interruption
                # KeyboardInterrupt/SystemExit/other BaseException from PTY setup is
                # not an import-level no-effect fallback.  Preserve the original error
                # and exact session; never launch a duplicate pipe child.
                logger.warning("PTY setup interrupted (%s); retaining exact session", type(exc).__name__)
                self._retain_after_post_effect_failure(session, exc, "pty_baseexception")
                raise
        return None, pty_scope_attempted

    def _recover_pipe_launch_error(self, session, exc, transaction, *, known_pre_effect):
        """Retain the exact transaction unless its no-effect release is proven."""
        if known_pre_effect:
            if transaction is None:
                if session.process is not None or session.pid is not None:
                    self._recover_native_transaction(session, exc, "post_effect_setup")
                else:
                    try:
                        released = self._custody_release_unaccepted(session)
                    except BaseException as release_exc:  # retain exact session; preserve launch error
                        self._recover_native_transaction(session, exc, "pre_effect_release")
                        logger.warning(
                            "pre-effect custody release failed during %s: %s",
                            type(exc).__name__, type(release_exc).__name__, exc_info=True,
                        )
                        released = False
                    if not released:
                        self._recover_native_transaction(session, exc, "pre_effect_release")
            elif transaction.process is None and transaction.no_effect_verified:
                try:
                    released = transaction.release_before_effect()
                except BaseException as release_exc:  # preserve original launch/prepare error
                    logger.warning(
                        "native no-effect release failed: %s", type(release_exc).__name__,
                        exc_info=True,
                    )
                    released = False
                if not released:
                    self._recover_native_transaction(session, exc, "release")
            else:
                self._recover_native_transaction(session, exc, "launch")
            return
        if transaction is not None:
            self._recover_native_transaction(session, exc, "launch")
        elif session.process is not None or session.pid is not None:
            self._recover_native_transaction(session, exc, "post_effect_setup")
        else:
            try:
                released = self._custody_release_unaccepted(session)
            except BaseException as release_exc:  # health: preserve the original failure if cleanup also fails
                logger.warning(
                    "pre-effect custody release failed during %s: %s",
                    type(exc).__name__, type(release_exc).__name__, exc_info=True,
                )
                released = False
            if not released:
                self._recover_native_transaction(session, exc, "pre_effect_release")

    def _close_spawn_output(self, session, log, output, active_error, transaction):
        """Close parent output without replacing an earlier launch error."""
        if log and output is not None:
            try:
                os.close(output)
            except BaseException as close_exc:  # health: output close is secondary to launch/retention errors
                # A descriptor-close error is secondary to an active launch error.  If
                # launch succeeded, retain the exact process and mark custody unknown
                # before surfacing the close failure; never replace the original error.
                if active_error is not None:
                    logger.warning(
                        "output close failed after %s: %s",
                        type(active_error).__name__, type(close_exc).__name__, exc_info=True,
                    )
                else:
                    logger.warning("output close failed after native launch: %s", type(close_exc).__name__, exc_info=True)
                    if transaction is not None or session.process is not None or session.pid is not None:
                        self._recover_native_transaction(session, close_exc, "output_close")
                    else:
                        self._retain_untracked_after_custody_failure(session)
                    raise

    def _spawn_local_pipe(self, session, safe_command, env_vars, log, pty_scope_attempted):
        """Launch, publish and register exactly one owned pipe reader."""
        from tools.process_registry import (
            _IS_WINDOWS, open_output_log, record_exit_command, systemd_user_bus_env,
            windows_hide_flags,
        )

        # Pipe path (non-PTY or PTY fallback).  Keep all command/env/log preparation
        # inside the accepted custody boundary: a preparation error must release only
        # when no native effect exists, otherwise the exact session stays unknown.
        unit_suffix = f"{session.id}-pipe-fallback" if pty_scope_attempted else session.id
        reader_target = self._log_reader_loop if log else self._reader_loop
        self._prepare_reader(session, reader_target, f"proc-reader-{session.id}")
        transaction = None
        output = None
        active_error = None
        effect = False
        self._i15_before_process_effect(session)
        try:
            _popen_kwargs = {"creationflags": windows_hide_flags()} if _IS_WINDOWS else {}
            spawn_argv = self._scope_argv(
                session, record_exit_command(log, safe_command) if log else safe_command,
                unit_suffix, "Local",
            )
            spawn_env = self._spawn_env(env_vars)
            if session.systemd_unit:
                spawn_env = systemd_user_bus_env(spawn_env)
            # start_new_session is REQUIRED with systemd-run --scope too: the scope does not
            # give the worker a new session, so from an interactive TUI the worker would
            # share the foreground process group and background spawns would stop the whole
            # session (observed as dead TUIs in state T). Cgroup isolation is unaffected —
            # the scope attaches to the invoked process, not the spawning session.
            output = open_output_log(log) if log else subprocess.PIPE
            if _IS_WINDOWS and session.custody is not None:
                from tools.process_registry_windows import begin_windows_launch
                transaction = begin_windows_launch(session.custody)
                session.custody.native_scope = transaction

                def _publish_native_bound(tx):
                    self._publish_started_session(session, process=tx.process, transaction=tx)

                proc = transaction.launch(
                    spawn_argv, text=True, cwd=session.cwd, env=spawn_env, encoding="utf-8",
                    errors="replace", stdout=output, stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL, start_new_session=True,
                    creationflags=_popen_kwargs.get("creationflags", 0),
                    on_process_bound=_publish_native_bound,
                )
                effect = transaction.has_process_effect
            else:
                proc = subprocess.Popen(
                    spawn_argv, text=True, cwd=session.cwd, env=spawn_env, encoding="utf-8",
                    errors="replace", stdout=output, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                    start_new_session=True, **_popen_kwargs)
                self._publish_started_session(session, process=proc)
                effect = True
        except (OSError, TypeError, ValueError) as exc:
            active_error = exc
            self._recover_pipe_launch_error(session, exc, transaction, known_pre_effect=True)
            raise
        except BaseException as exc:
            active_error = exc
            self._recover_pipe_launch_error(session, exc, transaction, known_pre_effect=False)
            raise
        finally:
            self._i15_after_process_effect(session, effect=effect)
            self._close_spawn_output(session, log, output, active_error, transaction)
        session.output_log = str(log or "")
        try:
            if transaction is not None:
                # WindowsLaunchTransaction binds the coordinator directly before resume;
                # mirror that authenticated state onto the session before the reader starts.
                self._sync_process_custody(session)
            if transaction is None:
                session.process = proc
                session.pid = proc.pid
                session.host_start_time = self._safe_host_start_time(session.pid)
                # Publication precedes this bind; the exact object is now the recovery root.
                self._custody_bind_process(session)
            self._track_started(session, reader_target, f"proc-reader-{session.id}")
        except BaseException as exc:  # health: post-effect recovery must preserve the original failure
            phase = "reader" if session.reader_phase != "not_started" else "post_effect_setup"
            # The exact Popen/Job/identity/session remains the sole recovery root; no fallback
            # launch or PID re-open is permitted after this boundary.
            self._retain_after_post_effect_failure(
                session, exc, phase, process=proc if transaction is None else None,
            )
            raise
        return session


    def _reconcile_local_exit(self, session: ProcessSession) -> None:
        from tools.process_registry import _IS_WINDOWS, _READER_FINAL_DRAIN_WAIT_SECONDS
        """Reconcile ``session.exited`` against the real child state.
        The reader flips ``exited`` only at EOF; when the direct child has exited but a
        descendant (e.g. a daemon from ``hermes update``) holds the pipe open, poll()
        would report "running" forever. If ``Popen.poll()`` has an exit code, drain
        readable bytes non-blocking and flip ``exited``. No-op for env/PTY, exited and
        detached sessions.

        The reader thread (`_reader_loop`) sets `session.exited = True` only in its `finally` block, which
        runs when `stdout.read()` returns EOF. If the direct `Popen` child has exited but a descendant
        process (e.g. a daemon spawned by `hermes update` restarting the gateway) is still holding the
        stdout pipe open, the reader blocks forever and poll() keeps returning "running" indefinitely (issue
        #17327 — 74 polls over 7 minutes on Feishu).
        """
        if session is None or session.exited or self._foreground_reader_reconcile_guard(session):
            return
        if session.reader_phase == "not_started":
            self._ensure_reader_registered(session)
            if session.reader_phase in {"starting", "started"}:
                return
        proc = getattr(session, "process", None)
        if proc is None:
            return
        try:
            rc = proc.poll()
        except Exception:
            logger.exception("Background process _reconcile_local_exit failed; retaining existing custody")
            return
        if rc is None:
            return  # Direct child still running — reader block is legitimate.
        native_scope = getattr(session.custody, "native_scope", None) if session.custody is not None else None
        if native_scope is not None:
            # A process exit is not native EOF/durable/ACK/Job-empty proof.  Ask only
            # the original reader to finish; never move a managed session directly from
            # Popen.poll() to finished.
            if session.reader_phase == "not_started":
                self._ensure_reader_registered(session)
            if session.reader_phase in {"started", "starting"}:
                session._reader_finish_requested.set()
            return
        reader = session._reader_thread
        if (
            not _IS_WINDOWS
            and session._reader_selectable
            and reader is not None
            and reader.is_alive()
        ):
            # The reader owns the pipe and completion payload. Asking it to
            # finish avoids a competing TextIOWrapper read here racing the
            # reader, publishing an empty owner-stamped result, then closing
            # the pipe before the buffered tail is ingested. It wakes within
            # the reader's bounded select interval and drains what is buffered.
            session._reader_finish_requested.set()
            # Flipping ``exited`` before that drain lands would hand poll()/wait() a truncated
            # snapshot (and wait() marks it consumed), so give the reader a moment to publish.
            if session._completion_event.wait(_READER_FINAL_DRAIN_WAIT_SECONDS):
                return
            with session._lock:
                session.mark_exited(rc)
            logger.info(
                "Reconciled session %s: direct child exited with code %s; "
                "reader will publish the owned completion after its final drain.",
                session.id, rc)
            return
        # Best-effort non-blocking drain of whatever the reader hasn't consumed.
        stdout = getattr(proc, "stdout", None)
        if stdout is not None and not _IS_WINDOWS:
            try:
                import fcntl
                fd = stdout.fileno()
                flags = fcntl.fcntl(fd, fcntl.F_GETFL)
                fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)
                try:
                    with suppress(BlockingIOError, OSError, ValueError):
                        chunk = stdout.read()
                        if chunk:
                            session.append_output(chunk if isinstance(chunk, str) else chunk.decode("utf-8", errors="replace"))
                finally:
                    with suppress(Exception):
                        fcntl.fcntl(fd, fcntl.F_SETFL, flags)
            except Exception as e:
                logger.exception("Background process _reconcile_local_exit failed; retaining existing custody")
                logger.debug("Non-blocking drain failed for %s: %s", session.id, e)
        with session._lock:
            session.mark_exited(rc)
        logger.info(
            "Reconciled session %s: direct child exited with code %s but reader "
            "was still blocked (orphaned pipe). Flipped to exited.",
            session.id, rc)
        self._move_to_finished(session)
