"""Accepted workers retain their exact lease through output and ambiguous launch."""

from concurrent.futures import Future, ThreadPoolExecutor
from types import SimpleNamespace
import threading

import pytest


def _fence(monkeypatch):
    from hermes_cli import backend_retirement

    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    return fence


def _rpc(monkeypatch, pool, handler):
    from tui_gateway import server

    monkeypatch.setattr(server, "_pool", pool)
    monkeypatch.setattr(server, "_normalize_request", lambda req: (1, "lease.test", {}))
    monkeypatch.setattr(server, "_LONG_HANDLERS", {"lease.test"})
    monkeypatch.setattr(server, "_handle_admitted_request", handler)
    return server


def test_session_workers_release_only_their_own_lease(monkeypatch):
    from tui_gateway import server

    fence = _fence(monkeypatch)
    entered = [threading.Event(), threading.Event()]
    release = [threading.Event(), threading.Event()]
    threads = []

    def body(index):
        entered[index].set()
        assert release[index].wait(10)

    try:
        for index in range(2):
            thread = server._start_session_work(
                lambda index=index: body(index), name=f"exact-lease-{index}"
            )
            assert thread is not None
            threads.append(thread)
            assert entered[index].wait(10)
        assert fence.prepare() == {"ok": False, "idle": False}
        release[0].set()
        threads[0].join(10)
        assert not threads[0].is_alive()
        assert fence.prepare() == {"ok": False, "idle": False}
        release[1].set()
        threads[1].join(10)
        assert not threads[1].is_alive()
        assert fence.prepare()["ok"] is True
    finally:
        for event in release:
            event.set()
        for thread in threads:
            thread.join(10)


def test_session_start_exception_after_launch_retains_worker(monkeypatch):
    from agent import memory_provider
    from tui_gateway import server

    fence = _fence(monkeypatch)
    entered, release = threading.Event(), threading.Event()
    threads = []
    original = memory_provider.spawn_context_thread

    def body():
        entered.set()
        assert release.wait(10)

    def ambiguous_start(target, **kwargs):
        thread = original(target, **kwargs)
        threads.append(thread)

        def start():
            threading.Thread.start(thread)
            assert entered.wait(10)
            raise RuntimeError("synthetic post-launch error")

        return SimpleNamespace(start=start)

    monkeypatch.setattr(memory_provider, "spawn_context_thread", ambiguous_start)
    try:
        with pytest.raises(RuntimeError, match="post-launch"):
            server._start_session_work(body, name="ambiguous-exact-lease")
        assert fence.prepare() == {"ok": False, "idle": False}
        release.set()
        threads[0].join(10)
        assert not threads[0].is_alive()
        assert fence.prepare()["ok"] is True
    finally:
        release.set()
        for thread in threads:
            thread.join(10)


def test_cancelled_queued_rpc_releases_exact_lease(monkeypatch):
    fence = _fence(monkeypatch)
    future = Future()
    server = _rpc(monkeypatch, SimpleNamespace(submit=lambda fn: future), lambda req: None)
    transport = SimpleNamespace(write=lambda response: None)

    assert server.dispatch({"id": 1, "method": "lease.test"}, transport) is None
    assert fence.prepare() == {"ok": False, "idle": False}
    assert future.cancel()
    assert fence.prepare()["ok"] is True


def test_rpc_submit_exception_after_acceptance_retains_through_output(monkeypatch):
    fence = _fence(monkeypatch)
    entered, finish_handler = threading.Event(), threading.Event()
    writing, finish_write = threading.Event(), threading.Event()
    futures = []

    def handler(req):
        entered.set()
        assert finish_handler.wait(10)
        return {"id": 1, "result": {"ok": True}}

    def write(response):
        writing.set()
        assert finish_write.wait(10)

    with ThreadPoolExecutor(max_workers=1) as pool:
        def submit(fn):
            futures.append(pool.submit(fn))
            assert entered.wait(10)
            raise RuntimeError("synthetic accepted-submit error")

        server = _rpc(monkeypatch, SimpleNamespace(submit=submit), handler)
        try:
            with pytest.raises(RuntimeError, match="accepted-submit"):
                server.dispatch(
                    {"id": 1, "method": "lease.test"}, SimpleNamespace(write=write)
                )
            assert fence.prepare() == {"ok": False, "idle": False}
            finish_handler.set()
            assert writing.wait(10)
            assert fence.prepare() == {"ok": False, "idle": False}
            finish_write.set()
            futures[0].result(timeout=10)
            assert fence.prepare()["ok"] is True
        finally:
            finish_handler.set()
            finish_write.set()
