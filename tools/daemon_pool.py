"""Shared daemon-thread ThreadPoolExecutor.

Stdlib workers are non-daemon AND registered in ``_threads_queues``, whose atexit
hook joins every worker even after ``shutdown(wait=False)`` — one wedged worker
(tool blocked on network I/O, hung provider, stuck subagent) blocks interpreter
exit forever. This variant spawns daemon workers and skips that registration.
Use it for best-effort/interruptible work that must never hold the process open;
NOT for work that must complete before exit (durable writes belong on foreground
threads with explicit bounded joins).
"""

from __future__ import annotations

import threading
import weakref
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures.thread import _WorkItem, _global_shutdown_lock, _worker
from contextvars import copy_context
from dataclasses import dataclass

__all__ = ["DaemonThreadPoolExecutor", "OwnedSubmission"]

_CUSTODY_EXCEPTIONS = (Exception, KeyboardInterrupt, SystemExit, GeneratorExit)


@dataclass(frozen=True)
class OwnedSubmission:
    """The executor's handoff result, including an accepted-but-failed start."""

    handoff_id: str
    future: Future | None
    accepted: str
    error: BaseException | None = None


def _phase(callback, handoff_id: str) -> BaseException | None:
    if callback is None:
        return None
    try:
        callback(handoff_id)
    except _CUSTODY_EXCEPTIONS as exc:  # metadata failure must never change runner semantics
        return exc
    return None


class DaemonThreadPoolExecutor(ThreadPoolExecutor):
    """ThreadPoolExecutor variant whose workers do not block process exit."""

    def submit(self, fn, /, *args, **kwargs):
        """Keep each task in its caller's profile scope, even on a reused worker.

        Thread-start context cannot track later submissions from other profiles
        (#54937). The stdlib worker context manages initialization, not contextvars.
        """
        ctx = copy_context()

        def _run_with_context(*call_args, **call_kwargs):
            return ctx.run(fn, *call_args, **call_kwargs)
        return super().submit(_run_with_context, *args, **kwargs)

    def submit_owned(self, handoff_id, fn, /, *args, on_queued=None, on_started=None,
                     on_exited=None, **kwargs):
        """Submit one handoff while retaining the exact queue acceptance boundary.

        ``ThreadPoolExecutor.submit`` queues its work item before starting a worker;
        mirroring that short stdlib transaction lets the caller distinguish a refusal
        before enqueue from an accepted queue whose worker-start path raised. The
        returned ``future`` and ``handoff_id`` are the only reconciliation identity.
        """
        ctx = copy_context()

        def run_with_context(*call_args, **call_kwargs):
            _phase(on_started, handoff_id)
            try:
                return ctx.run(fn, *call_args, **call_kwargs)
            finally:
                _phase(on_exited, handoff_id)

        with self._shutdown_lock, _global_shutdown_lock:
            if self._broken:
                return OwnedSubmission(handoff_id, None, "no", RuntimeError(self._broken))
            if self._shutdown:
                return OwnedSubmission(handoff_id, None, "no", RuntimeError("executor shutdown"))
            f = Future()
            resolver = getattr(self, "_resolve_work_item_task", None)
            if resolver is not None:
                # CPython 3.14 WorkerContext owns task resolution and _WorkItem is (future, task).
                work_item = _WorkItem(f, resolver(run_with_context, args, kwargs))
            else:
                # CPython 3.11-3.13 keep the older (future, fn, args, kwargs) private ABI.
                work_item = _WorkItem(f, run_with_context, args, kwargs)
            try:
                self._work_queue.put(work_item)
            except _CUSTODY_EXCEPTIONS as exc:
                return OwnedSubmission(handoff_id, f, "unknown", exc)
            queued_error = _phase(on_queued, handoff_id)
            try:
                self._adjust_thread_count()
            except _CUSTODY_EXCEPTIONS as exc:
                return OwnedSubmission(handoff_id, f, "yes", queued_error or exc)
            return OwnedSubmission(handoff_id, f, "yes", queued_error)

    def _adjust_thread_count(self) -> None:
        # Mirrors CPython's implementation with two changes:
        # daemon=True and no _threads_queues registration.
        if self._idle_semaphore.acquire(timeout=0):
            return

        def weakref_cb(_, q=self._work_queue):
            q.put(None)
        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = f'{self._thread_name_prefix or self}_{num_threads:d}'
            executor_ref = weakref.ref(self, weakref_cb)
            if hasattr(self, "_create_worker_context"):
                # Python 3.14 replaced _initializer/_initargs with a factory
                # that supplies the worker's initializer context.
                worker_args = (
                    executor_ref,
                    self._create_worker_context(),
                    self._work_queue,
                )
            else:
                worker_args = (
                    executor_ref,
                    self._work_queue,
                    self._initializer,
                    self._initargs,
                )
            t = threading.Thread(
                name=thread_name, target=_worker, daemon=True,
                args=worker_args,
            )
            t.start()
            self._threads.add(t)
