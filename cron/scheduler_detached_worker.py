"""Cron: teardown of a worker that outlived its ``run_job``.

``ThreadPoolExecutor.shutdown(wait=False)`` after an inactivity timeout does not stop a
worker already inside ``run_conversation``. Finalizing its SessionDB from ``run_job``'s
``finally`` would close a handle the worker is still writing to — the checkpoint/WAL-unlink
overlap behind #102827. The worker's Future owns the teardown instead.
"""

from __future__ import annotations

import concurrent.futures
import contextlib
import logging
import subprocess
import threading
from typing import Optional


logger = logging.getLogger(__name__)


def _cron_worker_reaper_physical_fields(binding):
    return tuple(getattr(binding, name, default) for name, default in (
        ("_future", None), ("_claim_future", None), ("_reaper", None),
        ("_script_process", None), ("_script_pipes_closed", None),
        ("_native_job", None), ("_native_job_identity", None),
        ("_native_job_proven", None), ("_scope_proof", None)))


def _cron_worker_reaper_physical_matches(
    binding, expected, *, reaper_bound=True, expected_presence=None,
):
    actual = _cron_worker_reaper_physical_fields(binding)
    if expected is None or len(actual) != len(expected):
        return False
    if expected_presence is not None and len(expected_presence) != len(actual):
        return False
    if expected_presence is not None:
        actual_presence = tuple(value is not None for value in actual)
        if not reaper_bound:
            actual_presence = (*actual_presence[:2], expected_presence[2], *actual_presence[3:])
        if actual_presence != expected_presence:
            return False
    indices = (0, 1, 2, 3, 5, 6, 8) if reaper_bound else (0, 1, 3, 5, 6, 8)
    if not all(actual[index] is expected[index] for index in indices):
        return False
    for index in (4, 7):
        if type(expected[index]) is not bool or type(actual[index]) is not bool:
            return False
        if actual[index] is not expected[index]:
            return False
    return True


def _cron_finish_detached_worker_reaper_release(lease):
    with lease._reaper_lock:
        if not (
            lease._reaper_retained and lease._reaper_completed
            and lease._reaper_settlement_confirmed and lease._reaper_outer_exited
            and lease._reaper_release_requested and lease._reaper_retry_thread_done.is_set()
        ):
            return False
        retry_thread = lease._reaper_retry_thread
        if retry_thread is threading.current_thread():
            return False
    if retry_thread is not None:
        retry_thread.join()
    with lease._reaper_lock:
        if not (
            lease._reaper_retained and lease._reaper_completed
            and lease._reaper_settlement_confirmed and lease._reaper_outer_exited
            and lease._reaper_release_requested and lease._reaper_retry_thread_done.is_set()
            and (retry_thread is None or not retry_thread.is_alive())
        ):
            return False
        lease._reaper_retained = False
        lease._reaper_completed = False
        lease._reaper_release_requested = False
        lease._reaper_settlement_confirmed = False
        lease._reaper_retry_stop = False
        lease._reaper_retry_consumer_status = "finished"
        lease._reaper_retry_thread = None
        lease._reaper_retry_callback = None
        lease._accepted_worker_custody = None
        lease._accepted_worker_process = None
        lease._accepted_worker_identity = None
        lease._released = True
    if lease.registry is not None:
        lease.registry._leave_lease(lease)
    return True


def _cron_recover_reaper_retry(registry, lease):
    with registry._lock:
        thread = lease._reaper_retry_thread
        callback = lease._reaper_retry_callback
        started = getattr(thread, "_started", None)
        if (
            registry._active_leases.get(lease.lease_id) is not lease
            or lease._reaper_retry_consumer_status != "ambiguous"
            or thread is None or not lease._reaper_retry_thread_done.is_set()
            or thread.is_alive() or started is None or not started.is_set()
            or not callable(callback)
        ):
            return False
    try:
        thread.join()
    except RuntimeError:
        return False
    with registry._lock:
        if (registry._active_leases.get(lease.lease_id) is not lease or
            lease._reaper_retry_thread is not thread or thread.is_alive()):
            return False
        registry._reaper_retry_threads.pop(lease.lease_id, None)
        lease._reaper_retry_registered = False
        lease._reaper_retry_consumer_status = "not_registered"
        lease._reaper_retry_start_status = "recovered"
    return registry._register_reaper_retry(lease, callback)


def _close_late_session_db_result(future: concurrent.futures.Future) -> None:
    """Done-callback: close a SessionDB whose constructor finished after run_job's init timeout
    (worker abandoned via ``shutdown(wait=False)``), else its .db/WAL/SHM handles leak to EMFILE.

    If the constructor later completes inside that abandoned worker, the Future's result — an open
    SessionDB holding .db / WAL / SHM file handles — would be orphaned and never closed, leaking
    descriptors until EMFILE (#72782). This callback retrieves and closes that eventual late result.
    """
    with contextlib.suppress(Exception):
        db = future.result()
        if db is not None:
            from hermes_state_registry import release_or_close
            release_or_close(db)


def defer_teardown_to_running_worker(
    future: Optional[concurrent.futures.Future], session_db, agent, job_id: str, job_name: str,
    cron_session_id: str, workdir: Optional[str] = None,
) -> bool:
    """Return True when the worker is still running and its Future will finalize the session
    and tear the agent down on completion; False when the caller must do it now."""
    if future is None or future.done():
        return False
    from cron.scheduler import _finalize_cron_session, _teardown_cron_agent

    def _finish(_future) -> None:
        try:
            if session_db:
                _finalize_cron_session(session_db, agent, job_id, job_name, cron_session_id,
                                       workdir=workdir)
        finally:
            _teardown_cron_agent(agent, job_id)

    # Runs inline if the worker finished between done() and here — still exactly once.
    future.add_done_callback(_finish)
    return True


def _reap_terminal_worker_in_background_effect(
    process: subprocess.Popen, on_done=None, *, custody=None, admission=None,
):
    """Keep the reap contract and report its physical completion to custody.

    The ledger turning terminal lets ``_wait_for_external_cron_worker_body`` return while the
    worker is still in final teardown.  The callback is only a wakeup for the exact binding; it
    cannot release a lease without the ledger, process, reaper, and delivery checks agreeing.
    """
    reaped = threading.Event()
    settlement_lock = threading.Lock()

    def _retry_failed_native_wait() -> bool:
        """Retry one failed wait on the same authenticated native process."""
        if admission is None or getattr(thread, "_cron_reap_succeeded", False) is True:
            return admission is None
        if (
            getattr(thread, "_cron_reap_start_status", None) != "started"
            or not getattr(thread, "_cron_reap_wait_failed", False)
            or getattr(thread, "_cron_reap_wait_retry_attempted", False)
        ):
            return False
        if not admission._reaper_identity_matches(custody, process):
            custody.mark_unknown("foreign_custody")
            return False
        thread._cron_reap_wait_retry_attempted = True
        try:
            # The retry is a non-blocking native reap probe.  The existing
            # registry retry loop / runtime close deadline owns liveness; do
            # not invent a second per-process deadline here.  A still-live
            # process remains pending and must be retried by that owner.
            process.wait(timeout=0)
        except subprocess.TimeoutExpired:
            thread._cron_reap_wait_retry_attempted = False
            return False
        except (OSError, ValueError, subprocess.SubprocessError):
            custody.mark_unknown("reaper_wait_failed")
            return False
        if not admission.mark_detached_worker_reaper_terminal(custody, process):
            custody.mark_unknown("foreign_custody")
            return False
        thread._cron_reap_succeeded = True
        thread._cron_reap_wait_failed = False
        reaped.set()
        return True

    def _settle_after_reap() -> bool:
        """Run the source callback and exact receipt once, or retain for retry."""
        with settlement_lock:
            if admission is not None:
                if getattr(thread, "_cron_reap_start_status", None) == "ambiguous":
                    # A failed native thread start is not evidence that a
                    # consumer exists. Keep the lease UNKNOWN; no competing
                    # wait is permitted.
                    return False
                if thread._cron_reap_succeeded is not True and not _retry_failed_native_wait():
                    return False
                if not reaped.is_set() or thread._cron_reap_succeeded is not True:
                    return False
                parent = getattr(custody, "parent", None)
                if getattr(custody, "_unknown", False) or getattr(parent, "_unknown", False):
                    return False
                # Once the callback has terminalized either binding, its live records are
                # intentionally removed and the terminal completed-handle witness is the
                # authority.  Use the live-record check only before the first settlement.
                if not getattr(custody, "_settled", False) and not getattr(parent, "_settled", False):
                    if not admission._reaper_identity_matches(custody, process):
                        if custody is not None:
                            custody.mark_unknown("foreign_custody")
                        return False
            settlement_callback_ok = admission is None
            if on_done is not None:
                try:
                    settlement_callback_ok = on_done() is True
                except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError):
                    logger.warning(
                        "Cron external worker completion notification failed; custody remains pending",
                    )
                    return False
            elif admission is not None:
                logger.warning(
                    "Cron external worker reaper has no terminal custody observer; "
                    "source lease remains pending",
                )
                return False
            if admission is None:
                return settlement_callback_ok
            if not settlement_callback_ok or not admission.confirm_detached_worker_settlement(
                custody, process,
            ):
                logger.warning(
                    "Cron external worker settlement receipt was not exact; custody remains pending",
                )
                return False
            release = admission.release_detached_worker_reaper(custody, process)
            if getattr(release, "status", None) != "released":
                logger.warning(
                    "Cron external worker reaper could not release its exact source lease; "
                    "custody remains pending",
                )
                return False
            return True

    def _reap() -> None:
        try:
            # This thread is the detached reaper: a blocking wait is the native
            # Popen contract and performs the actual child reap.  Polling with a
            # short timeout here made unchanged fake/process adapters that raise
            # TimeoutExpired forever spin at full speed and starve the owning
            # test/process.  A live child still leaves custody pending until this
            # wait returns; the gateway caller never joins this daemon thread.
            # health: allow HX006 -- the detached daemon must block for native child exit; the caller never joins, and custody settles only after this wait returns.
            process.wait()
        except (OSError, ValueError, subprocess.SubprocessError):
            thread._cron_reap_succeeded = False
            thread._cron_reap_wait_failed = True
            reaped.set()
            logger.warning("Cron external worker could not be reaped; custody remains pending")
        else:
            if admission is not None and not admission.mark_detached_worker_reaper_terminal(
                custody, process,
            ):
                thread._cron_reap_succeeded = False
                logger.warning(
                    "Cron external worker reaper completed without an exact retained lease",
                )
            else:
                thread._cron_reap_succeeded = True
                thread._cron_reap_wait_failed = False
            reaped.set()

    from agent.memory_provider import spawn_context_thread

    thread = spawn_context_thread(
        _reap,
        name=f"cron-worker-reap-{getattr(process, 'pid', '?')}",
    )
    thread._cron_reap_succeeded = False
    thread._cron_reap_wait_failed = False
    thread._cron_reap_wait_retry_attempted = False
    thread._cron_reap_start_status = "pending"
    thread._cron_reap_done = reaped
    thread._cron_reap_joined = False
    if admission is not None:
        if not admission.register_detached_worker_reaper_retry(_settle_after_reap):
            logger.error(
                "Cron external worker could not register its exact settlement retry; "
                "custody remains retained",
            )
            thread._cron_reap_start_status = "no_consumer"
            if custody is not None:
                custody.mark_unknown("reaper_retry_unavailable")
            raise RuntimeError("external worker reaper settlement consumer unavailable")
    try:
        thread.start()
        thread._cron_reap_start_status = "started"
    except BaseException:
        # ``Thread.start`` does not prove no native work was accepted.  Keep the exact
        # retained lease and registered consumer; no second reaper or fresh admission is safe.
        thread._cron_reap_succeeded = False
        thread._cron_reap_start_status = "ambiguous"
        reaped.set()
        if admission is not None:
            admission.stop_detached_worker_reaper_retry("reaper_start")
            if custody is not None:
                custody.mark_unknown("reaper_start_unknown")
        raise
    if on_done is not None or admission is not None:
        def _notify_after_join() -> None:
            while thread.is_alive():
                thread.join(timeout=0.5)
            thread._cron_reap_joined = True
            _settle_after_reap()

        spawn_context_thread(
            _notify_after_join,
            name=f"cron-worker-reap-notify-{getattr(process, 'pid', '?')}",
        ).start()
    return thread


def reap_terminal_worker_in_background(
    process: subprocess.Popen, on_done=None, *, custody=None, admission=None,
):
    """Start the exact reaper continuation under its parent admission lease."""
    from cron.runtime_custody import admit_cron_source

    if admission is not None:
        if not admission.retain_detached_worker_reaper(custody, process):
            if custody is not None:
                custody.mark_unknown("foreign_custody")
            raise RuntimeError("external worker reaper continuation refused")
        # The original worker admission already detached the jobs lock.  The
        # reaper is its exact continuation and must not reacquire that profile
        # lock through a second worker_reaper admission.
        return _reap_terminal_worker_in_background_effect(
            process, on_done, custody=custody, admission=admission,
        )

    with admit_cron_source("worker_reaper", parent=custody):
        return _reap_terminal_worker_in_background_effect(
            process, on_done, custody=custody,
        )
