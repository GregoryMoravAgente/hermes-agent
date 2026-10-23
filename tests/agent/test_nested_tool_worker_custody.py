"""Causal contracts for nested daemon-tool worker custody."""

from __future__ import annotations

import concurrent.futures
import threading
from types import SimpleNamespace

import pytest


class _Parent:
    def __init__(self):
        self.children = {}
        self.allow = True
        self.lock = threading.Lock()

    def admit_tool_worker(self, child):
        if not self.allow:
            from agent.tool_worker_custody import ToolWorkerAdmissionError

            raise ToolWorkerAdmissionError("parent closed")
        with self.lock:
            self.children[child.handle_id] = child

    def settle_tool_worker(self, child):
        with self.lock:
            if self.children.get(child.handle_id) is child:
                del self.children[child.handle_id]


def test_accepted_worker_stays_with_parent_after_outer_timeout():
    from agent.tool_worker_custody import admit_tool_worker, bind_tool_worker_parent, submit_tool_worker
    from tools.daemon_pool import DaemonThreadPoolExecutor

    parent = _Parent()
    owner = object()
    started, release, completed = (threading.Event() for _ in range(3))

    def blocked_write():
        started.set()
        assert release.wait(5)
        completed.set()
        return "written"

    with bind_tool_worker_parent(parent):
        handle = admit_tool_worker(owner, "sequential")
        executor = DaemonThreadPoolExecutor(max_workers=1)
        future = submit_tool_worker(executor, handle, blocked_write)
    try:
        assert started.wait(5)
        assert handle.future is future
        assert handle.worker_tid is not None
        assert handle.pending
        assert parent.children[handle.handle_id] is handle
        # This is the outer-turn timeout boundary: the awaiter may return, but an
        # accepted worker is not cancelled or removed from the source-owned parent.
        executor.shutdown(wait=False, cancel_futures=False)
        assert not completed.is_set()
        assert parent.children
        release.set()
        assert future.result(timeout=5) == "written"
        assert completed.is_set()
        assert not handle.pending
        assert parent.children == {}
    finally:
        release.set()
        executor.shutdown(wait=True, cancel_futures=False)


def test_ambiguous_submit_retains_exact_handle():
    from agent.tool_worker_custody import (
        ToolWorkerSubmissionError,
        admit_tool_worker,
        bind_tool_worker_parent,
        submit_tool_worker,
    )

    class AmbiguousExecutor:
        def submit(self, *_args, **_kwargs):
            raise RuntimeError("queue state unavailable")

    parent = _Parent()
    owner = object()
    with bind_tool_worker_parent(parent):
        handle = admit_tool_worker(owner, "concurrent")
        with pytest.raises(ToolWorkerSubmissionError, match="queue state unavailable") as excinfo:
            submit_tool_worker(AmbiguousExecutor(), handle, lambda: None)

    assert excinfo.value.accepted == "unknown"
    assert handle.state == "unknown"
    assert handle.pending
    assert parent.children == {handle.handle_id: handle}


def test_owned_executor_refusal_settles_before_enqueue():
    from agent.tool_worker_custody import (
        ToolWorkerSubmissionError,
        admit_tool_worker,
        bind_tool_worker_parent,
        submit_tool_worker,
    )
    from tools.daemon_pool import DaemonThreadPoolExecutor

    parent = _Parent()
    owner = object()
    executor = DaemonThreadPoolExecutor(max_workers=1)
    executor.shutdown(wait=False, cancel_futures=False)
    try:
        with bind_tool_worker_parent(parent):
            handle = admit_tool_worker(owner, "sequential")
            with pytest.raises(ToolWorkerSubmissionError) as excinfo:
                submit_tool_worker(executor, handle, lambda: None)
        assert excinfo.value.accepted == "no"
        assert handle.state == "refused"
        assert not handle.pending
        assert parent.children == {}
    finally:
        executor.shutdown(wait=False, cancel_futures=False)


def test_owned_accepted_without_future_is_unknown_without_a_join():
    from agent.tool_worker_custody import (
        ToolWorkerSubmissionError,
        admit_tool_worker,
        bind_tool_worker_parent,
        submit_tool_worker,
    )
    from tools.daemon_pool import OwnedSubmission

    class AcceptedWithoutFuture:
        def __init__(self):
            self.waits = []

        def submit_owned(self, handoff_id, fn, *args, **kwargs):
            return OwnedSubmission(handoff_id, None, "unknown", RuntimeError("worker start ambiguous"))

        def shutdown(self, *, wait, cancel_futures):
            self.waits.append((wait, cancel_futures))

    parent = _Parent()
    owner = object()
    executor = AcceptedWithoutFuture()
    with bind_tool_worker_parent(parent):
        handle = admit_tool_worker(owner, "sequential")
        with pytest.raises(ToolWorkerSubmissionError) as excinfo:
            submit_tool_worker(executor, handle, lambda: None)
    assert excinfo.value.accepted == "unknown"
    assert handle.state == "unknown"
    assert handle.pending
    assert parent.children[handle.handle_id] is handle
    executor.shutdown(wait=False, cancel_futures=False)
    assert executor.waits == [(False, False)]


def test_foreign_submit_owned_result_stays_unknown_without_native_finalizer():
    from agent.tool_worker_custody import admit_tool_worker, bind_tool_worker_parent, submit_tool_worker
    from tools.daemon_pool import OwnedSubmission

    class ForeignExecutor:
        def __init__(self):
            self.thread = None
            self.received_kwargs = None

        def submit_owned(self, handoff_id, fn, *args, **kwargs):
            from concurrent.futures import Future

            self.received_kwargs = dict(kwargs)
            future = Future()

            def run():
                try:
                    future.set_result(fn(*args, **kwargs))
                except BaseException as exc:
                    future.set_exception(exc)

            self.thread = threading.Thread(target=run)
            self.thread.start()
            return OwnedSubmission(handoff_id, future, "yes")

    parent = _Parent()
    executor = ForeignExecutor()
    with bind_tool_worker_parent(parent):
        handle = admit_tool_worker(object(), "sequential")
        future = submit_tool_worker(executor, handle, lambda: "foreign result")

    try:
        assert future.result(timeout=5) == "foreign result"
        assert "on_finalized" not in executor.received_kwargs
        assert handle.state == "unknown"
        assert handle.pending
        assert parent.children == {handle.handle_id: handle}
    finally:
        executor.thread.join(5)
        assert not executor.thread.is_alive()


def test_legacy_prepared_future_retains_unknown_with_exact_identity():
    from agent.tool_worker_custody import (
        admit_tool_worker,
        bind_existing_tool_worker_future,
        bind_tool_worker_parent,
    )

    parent = _Parent()
    owner = object()
    future = concurrent.futures.Future()
    with bind_tool_worker_parent(parent):
        handle = admit_tool_worker(owner, "prepared-terminal")
        assert bind_existing_tool_worker_future(handle, future)
    assert handle.future is future
    assert handle.pending
    future.set_result("prepared result")
    assert handle.pending
    assert handle.state == "unknown"
    assert parent.children == {handle.handle_id: handle}


def test_legacy_prepared_future_retains_parent_during_other_callback():
    from agent.tool_worker_custody import (
        admit_tool_worker,
        bind_existing_tool_worker_future,
        bind_tool_worker_parent,
    )

    parent = _Parent()
    future = concurrent.futures.Future()
    entered, release = threading.Event(), threading.Event()
    with bind_tool_worker_parent(parent):
        handle = admit_tool_worker(object(), "prepared-terminal")
        assert bind_existing_tool_worker_future(handle, future)

    def accepted_callback(_future):
        entered.set()
        assert release.wait(5)

    future.add_done_callback(accepted_callback)
    worker = threading.Thread(target=lambda: future.set_result("written"))
    worker.start()
    try:
        assert entered.wait(5)
        assert future.done()
        assert handle.pending
        assert parent.children[handle.handle_id] is handle
    finally:
        release.set()
        worker.join(5)
        assert not worker.is_alive()


def test_factory_failure_happens_before_child_admission():
    from agent.tool_worker_custody import bind_tool_worker_parent, current_tool_worker_parent

    parent = _Parent()

    class BrokenFactory:
        def __init__(self):
            raise RuntimeError("factory failed")

    with bind_tool_worker_parent(parent):
        with pytest.raises(RuntimeError, match="factory failed"):
            BrokenFactory()
        assert current_tool_worker_parent() is parent
    assert parent.children == {}


def test_parent_refusal_prevents_submission():
    from agent.tool_worker_custody import ToolWorkerAdmissionError, admit_tool_worker, bind_tool_worker_parent

    parent = _Parent()
    parent.allow = False
    with bind_tool_worker_parent(parent), pytest.raises(ToolWorkerAdmissionError):
        admit_tool_worker(SimpleNamespace(), "prepared-terminal")
    assert parent.children == {}


def test_exceptional_worker_body_still_settles_finalizers():
    from agent.tool_worker_custody import admit_tool_worker, bind_tool_worker_parent, submit_tool_worker
    from tools.daemon_pool import DaemonThreadPoolExecutor

    parent = _Parent()
    with bind_tool_worker_parent(parent):
        handle = admit_tool_worker(object(), "concurrent")
        executor = DaemonThreadPoolExecutor(max_workers=1)
        future = submit_tool_worker(executor, handle, lambda: (_ for _ in ()).throw(ValueError("body")))
    try:
        with pytest.raises(ValueError, match="body"):
            future.result(timeout=5)
        assert not handle.pending
        assert handle.body_done.is_set() and handle.finalizers_done.is_set()
        assert parent.children == {}
    finally:
        executor.shutdown(wait=True, cancel_futures=False)


def test_owned_worker_stays_pending_until_future_callbacks_return():
    """A native owned Future settles only after its accepted callbacks finish."""
    from agent.tool_worker_custody import admit_tool_worker, bind_tool_worker_parent, submit_tool_worker
    from tools.daemon_pool import DaemonThreadPoolExecutor

    settled = threading.Event()

    class Parent(_Parent):
        def settle_tool_worker(self, child):
            super().settle_tool_worker(child)
            settled.set()

    parent = Parent()
    body_release = threading.Event()
    callback_started = threading.Event()
    callback_release = threading.Event()
    with bind_tool_worker_parent(parent):
        handle = admit_tool_worker(object(), "sequential")
        executor = DaemonThreadPoolExecutor(max_workers=1)
        future = submit_tool_worker(executor, handle, lambda: (body_release.wait(5), "written")[1])

    def callback(_done):
        callback_started.set()
        assert callback_release.wait(5)

    future.add_done_callback(callback)
    try:
        body_release.set()
        assert callback_started.wait(5)
        # Future.done() is already true while callback dispatch is blocked, but
        # the source-owned custody is still live.
        assert future.done()
        assert handle.pending
        assert parent.children[handle.handle_id] is handle
        callback_release.set()
        assert future.result(timeout=5) == "written"
        assert settled.wait(5)
        assert not handle.pending
        assert parent.children == {}
    finally:
        callback_release.set()
        executor.shutdown(wait=True, cancel_futures=False)


def test_callback_admitted_during_dispatch_keeps_parent():
    from agent.tool_worker_custody import admit_tool_worker, bind_tool_worker_parent, submit_tool_worker
    from tools.daemon_pool import DaemonThreadPoolExecutor

    class Parent:
        def __init__(self):
            self.children = {}
        def admit_tool_worker(self, child):
            self.children[child.handle_id] = child
        def settle_tool_worker(self, child):
            self.children.pop(child.handle_id)
    parent = Parent()
    body_release, first_entered, first_release = (threading.Event() for _ in range(3))
    late_entered, late_release, settled = (threading.Event() for _ in range(3))
    class ObservedParent(Parent):
        def settle_tool_worker(self, child):
            super().settle_tool_worker(child)
            settled.set()
    parent = ObservedParent()
    pool = DaemonThreadPoolExecutor(max_workers=1)
    with bind_tool_worker_parent(parent):
        handle = admit_tool_worker(object(), 'concurrent')
        future = submit_tool_worker(pool, handle, lambda: body_release.wait(5))
    def first(_done):
        first_entered.set()
        assert first_release.wait(5)
    def late(_done):
        late_entered.set()
        assert late_release.wait(5)
    future.add_done_callback(first)
    registrar = threading.Thread(target=lambda: future.add_done_callback(late))
    try:
        body_release.set()
        assert first_entered.wait(5)
        registrar.start()
        assert late_entered.wait(5)
        first_release.set()
        pool.shutdown(wait=True, cancel_futures=False)
        assert handle.pending, 'Concurrent completion callback is still running after worker callback dispatch returned'
        assert parent.children[handle.handle_id] is handle
        late_release.set()
        registrar.join(5)
        assert settled.wait(5)
    finally:
        body_release.set()
        first_release.set()
        late_release.set()
        if registrar.ident is not None:
            registrar.join(5)
        pool.shutdown(wait=True, cancel_futures=False)


def test_source_finalizer_boundary_allows_reentrant_late_observer():
    from tools.daemon_pool import OwnedFuture

    source_started = threading.Event()
    source_release = threading.Event()
    source_returned = threading.Event()
    observer_seen = threading.Event()
    future_holder = []

    def source_finalizer(done_future):
        source_started.set()
        # Future's public callback API remains re-entrant while the source
        # finalizer is in progress; this observer is after the linearized
        # finalizer boundary and must not deadlock or be silently dropped.
        future_holder[0].add_done_callback(lambda _done: observer_seen.set())
        assert source_release.wait(5)
        source_returned.set()

    future = OwnedFuture(source_finalizer)
    future_holder.append(future)
    setter = threading.Thread(target=lambda: future.set_result("done"))
    setter.start()
    try:
        assert source_started.wait(5)
        assert observer_seen.wait(5)
        assert not source_returned.is_set()
        assert future._owned_finalizer_in_progress
        assert not future._owned_finalized
        source_release.set()
        setter.join(5)
        assert not setter.is_alive()
        assert source_returned.is_set()
        assert future._owned_finalized
    finally:
        source_release.set()
        setter.join(5)
