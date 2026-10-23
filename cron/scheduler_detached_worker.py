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
    process: subprocess.Popen, on_done=None, *, custody=None,
):
    """Keep the reap contract and report its physical completion to custody.

    The ledger turning terminal lets ``_wait_for_external_cron_worker_body`` return while the
    worker is still in final teardown.  The callback is only a wakeup for the exact binding; it
    cannot release a lease without the ledger, process, reaper, and delivery checks agreeing.
    """
    reaped = threading.Event()

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
            reaped.set()
            logger.warning("Cron external worker could not be reaped; custody remains pending")
        else:
            thread._cron_reap_succeeded = True
            reaped.set()

    from agent.memory_provider import spawn_context_thread

    thread = spawn_context_thread(
        _reap,
        name=f"cron-worker-reap-{getattr(process, 'pid', '?')}",
    )
    thread._cron_reap_succeeded = False
    thread._cron_reap_done = reaped
    thread._cron_reap_joined = False
    thread.start()
    if on_done is not None:
        def _notify_after_join() -> None:
            while thread.is_alive():
                thread.join(timeout=0.5)
            thread._cron_reap_joined = True
            if not reaped.is_set() or thread._cron_reap_succeeded is not True:
                return
            try:
                on_done()
            except (AttributeError, KeyError, LookupError, OSError, RuntimeError, TypeError, ValueError):
                logger.warning("Cron external worker completion notification failed; custody remains pending")

        spawn_context_thread(
            _notify_after_join,
            name=f"cron-worker-reap-notify-{getattr(process, 'pid', '?')}",
        ).start()
    return thread


def reap_terminal_worker_in_background(process: subprocess.Popen, on_done=None, *, custody=None):
    """Start the exact reaper continuation under its parent admission lease."""
    from cron.runtime_custody import admit_cron_source

    with admit_cron_source("worker_reaper", parent=custody):
        return _reap_terminal_worker_in_background_effect(
            process, on_done, custody=custody)
