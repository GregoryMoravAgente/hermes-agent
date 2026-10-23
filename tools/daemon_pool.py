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

__all__ = ["DaemonThreadPoolExecutor", "OwnedFuture", "OwnedSubmission"]

_CUSTODY_EXCEPTIONS = (Exception, KeyboardInterrupt, SystemExit, GeneratorExit)


@dataclass(frozen=True)
class OwnedSubmission:
    """The executor's handoff result, including an accepted-but-failed start."""

    handoff_id: str
    future: Future | None
    accepted: str
    error: BaseException | None = None


class OwnedFuture(Future):
    """Future whose owned finalizer runs after ``Future`` callbacks.

    ``Future.set_result``/``set_exception`` invoke the callbacks registered by
    callers before returning.  A callback attached from the worker body is
    therefore too early to prove that accepted source-owned callbacks have
    settled.  The daemon pool owns this Future subclass and invokes the
    source-owned finalizer only after the stdlib callback dispatch has returned.
    A plain or foreign Future never gains this proof and remains UNKNOWN to its
    custody caller.
    """

    def __init__(self, on_finalized=None):
        super().__init__()
        self._owned_on_finalized = on_finalized
        self._owned_finalized = False
        self._owned_completion_dispatched = False
        self._owned_callbacks = set()
        self._owned_finalizer_lock = threading.Lock()

    def _run_owned_finalizer(self) -> None:
        with self._owned_finalizer_lock:
            if (
                self._owned_finalized
                or not self._owned_completion_dispatched
                or self._owned_callbacks
            ):
                return
            self._owned_finalized = True
            callback = self._owned_on_finalized
            self._owned_on_finalized = None
        if callback is not None:
            callback(self)

    def add_done_callback(self, fn):
        # Future invokes callbacks registered after FINISHED in the registrar's
        # thread. Such a callback can overlap the worker's callback dispatch;
        # retain its exact admission until it returns, too.
        token = object()
        with self._owned_finalizer_lock:
            retained = not self._owned_finalized
            if retained:
                self._owned_callbacks.add(token)
        if not retained:
            return super().add_done_callback(fn)

        def observed(done_future):
            try:
                fn(done_future)
            finally:
                with self._owned_finalizer_lock:
                    self._owned_callbacks.discard(token)
                self._run_owned_finalizer()

        return super().add_done_callback(observed)

    def set_result(self, result):
        super().set_result(result)
        with self._owned_finalizer_lock:
            self._owned_completion_dispatched = True
        self._run_owned_finalizer()

    def set_exception(self, exception):
        super().set_exception(exception)
        with self._owned_finalizer_lock:
            self._owned_completion_dispatched = True
        self._run_owned_finalizer()


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
                     on_exited=None, on_finalized=None, **kwargs):
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
            def run_owned_finalizer(done_future):
                # The OwnedFuture calls this only after every ordinary Future
                # callback has returned.  Keep the existing phase error
                # handling so metadata never changes the task result.
                _phase(on_finalized, handoff_id)

            f = OwnedFuture(run_owned_finalizer if on_finalized is not None else None)
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
