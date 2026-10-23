"""Causal provider-owned hard-retirement tests.

These tests exercise the real bundled provider classes with local fake backends.  They never
exercise the optional Core manager: the provider boundary must own its profile identity, accepted
workers, subprocess/HTTP calls, durable rows, and retry custody before Core can consume a report.
"""

from __future__ import annotations

import threading

import pytest

from tests.waiting import wait_until


@pytest.fixture(autouse=True)
def _isolated_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.delenv("BRV_API_KEY", raising=False)
    monkeypatch.setenv("RETAINDB_API_KEY", "local-test-key")
    monkeypatch.delenv("RETAINDB_BASE_URL", raising=False)
    monkeypatch.delenv("RETAINDB_PROJECT", raising=False)
    return home


def _assert_result_shape(result: dict):
    assert set(result) == {
        "status", "provider", "home_id", "incarnation",
        "active_store_count", "pending_write_count", "pending_flush_count",
        "close_failure_count", "reason",
    }


class _RetainClient:
    def __init__(self, project="local"):
        self.project = project
        self.ingest_started = threading.Event()
        self.release_ingest = threading.Event()
        self.ingest_calls = 0
        self.fail_ingest = False
        self.fail_after_release = False
        self.prefetch_started = threading.Event()
        self.release_prefetch = threading.Event()
        self.seed_started = threading.Event()
        self.release_seed = threading.Event()
        self.block_seed = False

    def ingest_session(self, *_args, **_kwargs):
        self.ingest_calls += 1
        self.ingest_started.set()
        if self.fail_after_release:
            if not self.release_ingest.wait(10):
                raise RuntimeError("synthetic ingest barrier timed out")
            raise RuntimeError("synthetic ingest failure after barrier")
        if self.fail_ingest:
            raise RuntimeError("synthetic ingest failure")
        if not self.release_ingest.wait(10):
            raise RuntimeError("synthetic ingest barrier timed out")
        return {"status": "ok"}

    def query_context(self, *_args, **_kwargs):
        self.prefetch_started.set()
        if not self.release_prefetch.wait(10):
            raise RuntimeError("synthetic prefetch barrier timed out")
        return {"results": []}

    def get_profile(self, *_args, **_kwargs):
        return {}

    def ask_user(self, *_args, **_kwargs):
        return {"answer": ""}

    def get_agent_model(self, *_args, **_kwargs):
        return {}

    def add_memory(self, *_args, **_kwargs):
        return {"id": "local-memory"}

    def seed_agent_identity(self, *_args, **_kwargs):
        if self.block_seed:
            self.seed_started.set()
            assert self.release_seed.wait(10)
        return {"status": "ok"}


def _initialized_retaindb(
    tmp_path, monkeypatch, client: _RetainClient | None = None, *, soul: str | None = None,
):
    import plugins.memory.retaindb as module

    client = client or _RetainClient()

    class _ClientFactory:
        def __init__(self, _api_key, _base_url, _project):
            self.__dict__.update(client.__dict__)
            self.project = client.project

        def __getattr__(self, name):
            return getattr(client, name)

    monkeypatch.setattr(module, "_Client", _ClientFactory)
    home = tmp_path / "retaindb-home"
    home.mkdir()
    if soul is not None:
        (home / "SOUL.md").write_text(soul, encoding="utf-8")
    provider = module.RetainDBMemoryProvider()
    provider.initialize("session-1", hermes_home=str(home))
    return provider, client, home


def test_byterover_retirement_receipt_is_source_owned(tmp_path):
    from plugins.memory.byterover import ByteRoverMemoryProvider

    provider = ByteRoverMemoryProvider()
    uninitialized = provider.retirement_receipt()
    assert uninitialized == {"provider": "", "home_id": "", "incarnation": ""}
    before = provider.retire()
    _assert_result_shape(before)
    assert before["status"] == "unknown"
    assert before["reason"] == "not_initialized"

    provider.initialize("session-1", hermes_home=str(tmp_path / "brv-home"))
    receipt = provider.retirement_receipt()
    assert receipt["provider"] == "byterover"
    assert receipt["home_id"] == str((tmp_path / "brv-home").resolve())
    assert receipt["incarnation"]

    foreign = dict(receipt, home_id=str((tmp_path / "other-home").resolve()))
    result = provider.retire(foreign)
    _assert_result_shape(result)
    assert result["status"] == "unknown"
    assert result["reason"] == "foreign_receipt"
    stale = dict(receipt, incarnation="old-incarnation")
    stale_result = provider.retire(stale)
    assert stale_result["status"] == "unknown"
    assert stale_result["reason"] == "stale_receipt"


def test_byterover_retirement_drains_admitted_curate_and_retries_same_receipt(tmp_path, monkeypatch):
    from plugins.memory import byterover

    provider = byterover.ByteRoverMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path / "brv-home"))
    entered = threading.Event()
    release = threading.Event()

    def fake_run(args, **_kwargs):
        if args[0] == "curate":
            entered.set()
            assert release.wait(10)
        return {"success": True, "output": ""}

    monkeypatch.setattr(byterover, "_run_brv", fake_run)
    provider.sync_turn("remember this local fact", "ack")
    assert entered.wait(10)
    receipt = provider.retirement_receipt()
    result_box = {}
    retiring = threading.Thread(target=lambda: result_box.setdefault("result", provider.retire(receipt)))
    retiring.start()
    wait_until(lambda: provider._retiring, what="ByteRover retirement admission fence")
    release.set()
    retiring.join(timeout=10)

    result = result_box["result"]
    _assert_result_shape(result)
    assert result["status"] == "released"
    assert result["reason"] == "released"
    assert provider.retire(receipt)["status"] == "released"


def test_byterover_failed_curate_stays_unknown_on_same_object(tmp_path, monkeypatch):
    from plugins.memory import byterover

    provider = byterover.ByteRoverMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path / "brv-home"))
    monkeypatch.setattr(
        byterover, "_run_brv", lambda args, **_kwargs: {"success": False, "error": "synthetic failure"},
    )
    provider.sync_turn("remember this failed fact", "ack")
    wait_until(lambda: provider._retirement_failures == 1, what="ByteRover failed curate observation")
    receipt = provider.retirement_receipt()

    first = provider.retire(receipt)
    second = provider.retire(receipt)
    assert first["status"] == second["status"] == "unknown"
    assert first["reason"] == second["reason"] == "flush_failed"
    assert provider._released is False


@pytest.mark.parametrize("entrypoint", ["memory", "pre_compress"])
def test_byterover_shutdown_retains_blocked_non_sync_worker(tmp_path, monkeypatch, entrypoint):
    from plugins.memory import byterover

    provider = byterover.ByteRoverMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path / "brv-home"))
    entered, release = threading.Event(), threading.Event()

    def fake_run(_args, **_kwargs):
        entered.set()
        assert release.wait(10)
        return {"success": True, "output": ""}

    monkeypatch.setattr(byterover, "_run_brv", fake_run)
    if entrypoint == "memory":
        provider.on_memory_write("add", "user", "accepted non-sync memory")
        expected_name = "brv-memwrite"
    else:
        provider.on_pre_compress([{"role": "user", "content": "accepted pre-compression context"}])
        expected_name = "brv-flush"
    assert entered.wait(10)
    assert provider._sync_thread is None
    worker = next(iter(provider._retirement_threads))
    assert worker.name == expected_name
    assert provider._retirement_active_calls == 1

    # Shutdown must account for accepted mirror/flush workers, not only the legacy sync slot.
    monkeypatch.setattr(byterover, "_RETIREMENT_WAIT_S", 0.05)
    provider.shutdown()
    assert worker.is_alive()
    assert worker in provider._retirement_threads
    assert provider._retirement_active_calls == 1

    release.set()
    worker.join(10)
    assert not worker.is_alive()
    wait_until(lambda: not provider._retirement_threads, what=f"{expected_name} lease release")
    provider.shutdown()
    assert provider._retirement_threads == set()
    assert provider._retirement_active_calls == 0
    assert provider._sync_thread is None


def test_byterover_retire_waits_for_live_native_finalizer(tmp_path, monkeypatch):
    """A finished curate body cannot release custody while its native wrapper is still live."""
    from plugins.memory import byterover

    monkeypatch.setattr(byterover, "_run_brv", lambda *args, **kwargs: {"success": True, "output": ""})
    monkeypatch.setattr(byterover, "_RETIREMENT_WAIT_S", 0.05)
    provider = byterover.ByteRoverMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path / "brv-home"))
    in_finalizer, release_finalizer = threading.Event(), threading.Event()
    original_spawn = byterover.spawn_context_thread
    workers = []

    def spawn_with_native_finalizer(target, **kwargs):
        def body():
            try:
                target()
            finally:
                in_finalizer.set()
                assert release_finalizer.wait(10)

        worker = original_spawn(body, **kwargs)
        workers.append(worker)
        return worker

    monkeypatch.setattr(byterover, "spawn_context_thread", spawn_with_native_finalizer)
    provider.on_memory_write("add", "user", "accepted real worker")
    assert in_finalizer.wait(10)
    worker = workers[0]
    assert worker.is_alive()
    assert provider._retirement_active_calls == 0

    result = provider.retire(provider.retirement_receipt())
    assert result["status"] == "unknown"
    assert result["reason"] == "pending_flush"
    assert worker in provider._retirement_threads

    release_finalizer.set()
    worker.join(10)
    wait_until(lambda: not provider._retirement_threads, what="ByteRover native finalizer lease release")
    assert provider.retire(provider.retirement_receipt())["status"] == "released"


def test_byterover_ambiguous_start_retains_worker_through_shutdown(tmp_path, monkeypatch):
    from plugins.memory import byterover

    provider = byterover.ByteRoverMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path / "brv-home"))
    entered, release = threading.Event(), threading.Event()

    def fake_run(_args, **_kwargs):
        entered.set()
        assert release.wait(10)
        return {"success": True, "output": ""}

    monkeypatch.setattr(byterover, "_run_brv", fake_run)
    real_spawn = byterover.spawn_context_thread
    wrappers = []

    def ambiguous_spawn(target, *, name, **kwargs):
        inner = real_spawn(target, name=name, **kwargs)
        if name != "brv-memwrite":
            return inner

        class _Ambiguous:
            def __init__(self):
                self.inner = inner

            @property
            def ident(self):
                return self.inner.ident

            def start(self):
                self.inner.start()
                assert entered.wait(10)
                raise RuntimeError("synthetic ambiguous ByteRover start")

            def is_alive(self):
                return self.inner.is_alive()

            def join(self, *args, **kwargs):
                return self.inner.join(*args, **kwargs)

        wrapper = _Ambiguous()
        wrappers.append(wrapper)
        return wrapper

    monkeypatch.setattr(byterover, "spawn_context_thread", ambiguous_spawn)
    with pytest.raises(RuntimeError, match="synthetic ambiguous ByteRover start"):
        provider.on_memory_write("add", "user", "accepted ambiguous memory")
    assert wrappers and wrappers[0].is_alive()
    assert provider._retirement_threads == {wrappers[0]}
    assert provider._retirement_active_calls == 1

    monkeypatch.setattr(byterover, "_RETIREMENT_WAIT_S", 0.05)
    provider.shutdown()
    assert wrappers[0].is_alive()
    assert provider._retirement_threads == {wrappers[0]}
    assert provider._retirement_active_calls == 1

    release.set()
    wrappers[0].join(10)
    assert not wrappers[0].is_alive()
    wait_until(lambda: not provider._retirement_threads, what="ambiguous ByteRover lease release")
    provider.shutdown()
    assert provider._retirement_threads == set()
    assert provider._retirement_active_calls == 0


@pytest.mark.parametrize("blocked_stage", ["client", "queue"])
def test_retaindb_cannot_release_while_native_initializer_is_pending(tmp_path, monkeypatch, blocked_stage):
    import plugins.memory.retaindb as module

    entered, release = threading.Event(), threading.Event()
    errors = []

    class _BlockingClient(_RetainClient):
        def __init__(self, _api_key, _base_url, project):
            super().__init__(project)
            if blocked_stage == "client":
                entered.set()
                assert release.wait(5)

    monkeypatch.setattr(module, "_Client", _BlockingClient)
    real_queue = module._WriteQueue

    def _blocking_queue(client, db_path, **kwargs):
        if blocked_stage == "queue":
            entered.set()
            assert release.wait(5)
        return real_queue(client, db_path, **kwargs)

    monkeypatch.setattr(module, "_WriteQueue", _blocking_queue)
    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 0)
    home = tmp_path / "retaindb-init-home"
    home.mkdir()
    provider = module.RetainDBMemoryProvider()

    def _initialize():
        try:
            provider.initialize("session-1", hermes_home=str(home))
        except BaseException as exc:  # pragma: no cover - the assertion below reports it
            errors.append(exc)

    worker = threading.Thread(target=_initialize)
    worker.start()
    try:
        assert entered.wait(5)
        receipt = provider.retirement_receipt()
        assert receipt["incarnation"]
        result = provider.retire(receipt)
        assert result["status"] == "unknown"
        assert result["reason"] == "pending_flush"
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    assert errors == []

    # The admitted initializer completed, so the same source-owned receipt can now settle the
    # real queue. Restore the normal bounded wait for cleanup after the causal refusal.
    module._RETIREMENT_WAIT_S = 10.0
    assert provider.retire(receipt)["status"] == "released"


def test_retaindb_partial_queue_initializer_retains_exact_queue_for_cleanup_and_retry(tmp_path, monkeypatch):
    import plugins.memory.retaindb as module

    class Client:
        def __init__(self, *_args):
            self.project = "local"

    monkeypatch.setattr(module, "_Client", Client)
    real_queue = module._WriteQueue
    created = []

    def fail_after_queue(client, path, **kwargs):
        queue = real_queue(client, path, **kwargs)
        created.append(queue)
        raise RuntimeError("synthetic post-queue initializer failure")

    monkeypatch.setattr(module, "_WriteQueue", fail_after_queue)
    home = tmp_path / "retaindb-partial-home"
    home.mkdir()
    provider = module.RetainDBMemoryProvider()
    with pytest.raises(RuntimeError, match="post-queue"):
        provider.initialize("session-1", hermes_home=str(home))

    assert len(created) == 1
    queue = created[0]
    assert provider._queue is queue
    assert queue._thread is not None and queue._thread.is_alive()
    result = provider.retire(provider.retirement_receipt())
    assert result["status"] == "unknown"
    assert result["reason"] == "store_open_failed"

    provider.shutdown()
    assert provider._queue is None
    monkeypatch.setattr(module, "_WriteQueue", real_queue)
    provider.initialize("session-2", hermes_home=str(home))
    provider.shutdown()


@pytest.mark.parametrize("failure_stage", ["factory", "start", "ambiguous"])
def test_retaindb_initial_writer_launch_failure_retains_exact_queue(tmp_path, monkeypatch, failure_stage):
    import plugins.memory.retaindb as module

    client = _RetainClient()

    class _ClientFactory:
        def __init__(self, _api_key, _base_url, _project):
            self.__dict__.update(client.__dict__)
            self.project = client.project

        def __getattr__(self, name):
            return getattr(client, name)

    monkeypatch.setattr(module, "_Client", _ClientFactory)
    real_spawn = module.spawn_context_thread
    wrappers = []

    class _Unstartable:
        ident = None

        def start(self):
            raise RuntimeError("synthetic writer start failure")

    def _ambiguous_spawn(target, *, name, **kwargs):
        inner = real_spawn(target, name=name, **kwargs)

        class _Ambiguous:
            def __init__(self):
                self.inner = inner

            @property
            def ident(self):
                return self.inner.ident

            def start(self):
                self.inner.start()
                assert self.inner.ident is not None
                raise RuntimeError("synthetic ambiguous writer start failure")

            def is_alive(self):
                return self.inner.is_alive()

            def join(self, *args, **kwargs):
                return self.inner.join(*args, **kwargs)

        wrapper = _Ambiguous()
        wrappers.append(wrapper)
        return wrapper

    if failure_stage == "factory":
        monkeypatch.setattr(
            module, "spawn_context_thread",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("synthetic writer factory failure")),
        )
    elif failure_stage == "start":
        monkeypatch.setattr(module, "spawn_context_thread", lambda *_args, **_kwargs: _Unstartable())
    else:
        monkeypatch.setattr(module, "spawn_context_thread", _ambiguous_spawn)

    home = tmp_path / f"retaindb-initial-{failure_stage}"
    home.mkdir()
    provider = module.RetainDBMemoryProvider()
    with pytest.raises(RuntimeError, match="synthetic .*writer (factory|start) failure"):
        provider.initialize("session-1", hermes_home=str(home))

    queue = provider._queue
    assert queue is not None
    assert provider.retire(provider.retirement_receipt())["reason"] == "store_open_failed"
    assert provider._queue is queue
    provider.shutdown()
    assert provider._queue is None
    assert not queue._connections
    if wrappers:
        assert not wrappers[0].is_alive()


@pytest.mark.parametrize("failure_stage", ["factory", "start", "ambiguous_success", "ambiguous_failure"])
def test_retaindb_retry_writer_launch_failure_is_structured_and_recoverable(
    tmp_path, monkeypatch, failure_stage,
):
    import plugins.memory.retaindb as module

    provider, client, _home = _initialized_retaindb(tmp_path, monkeypatch)
    client.fail_ingest = True
    provider.sync_turn("accepted durable content", "ack")
    # A second synchronous turn may be accepted while the first native flush is already in
    # flight. Both rows must remain on this exact queue through every launch failure.
    provider.sync_turn("accepted durable content 2", "ack")
    assert client.ingest_started.wait(10)
    queue = provider._queue
    assert queue is not None
    receipt = provider.retirement_receipt()

    # Force the first attempt to retain the durable row on the exact queue while the writer is
    # still completing its failed native call.
    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 0.0)
    first = provider.retire(receipt)
    assert first["status"] == "unknown"
    assert first["reason"] == "pending_flush"
    wait_until(lambda: queue._thread is not None and not queue._thread.is_alive(), what="failed writer stop")
    assert queue._pending_count() == 2
    assert queue._active_flushes == 0
    client.ingest_started.clear()

    real_spawn = module.spawn_context_thread
    wrappers = []

    class _Unstartable:
        ident = None

        def start(self):
            raise RuntimeError("synthetic retry writer start failure")

    def _ambiguous_spawn(target, *, name, **kwargs):
        inner = real_spawn(target, name=name, **kwargs)

        class _Ambiguous:
            def __init__(self):
                self.inner = inner

            @property
            def ident(self):
                return self.inner.ident

            def start(self):
                self.inner.start()
                assert client.ingest_started.wait(10)
                raise RuntimeError("synthetic ambiguous retry writer start failure")

            def is_alive(self):
                return self.inner.is_alive()

            def join(self, *args, **kwargs):
                return self.inner.join(*args, **kwargs)

        wrapper = _Ambiguous()
        wrappers.append(wrapper)
        return wrapper

    if failure_stage == "factory":
        monkeypatch.setattr(
            module, "spawn_context_thread",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("synthetic retry writer factory failure")),
        )
    elif failure_stage == "start":
        monkeypatch.setattr(module, "spawn_context_thread", lambda *_args, **_kwargs: _Unstartable())
    else:
        monkeypatch.setattr(module, "spawn_context_thread", _ambiguous_spawn)

    # The launch failure must become a structured UNKNOWN on the same queue; it must not escape
    # retire or erase the pending durable row. Ambiguous launch owns the worker if it really ran,
    # so its result is settled from the queue's native row state.
    if failure_stage == "ambiguous_success":
        client.fail_ingest = False
        client.release_ingest.set()
    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 10.0)
    second = provider.retire(receipt)
    if failure_stage in {"factory", "start"}:
        assert second["status"] == "unknown"
        assert second["reason"] == "worker_start_failed"
        assert second["pending_write_count"] == 2
        assert provider._queue is queue
    elif failure_stage == "ambiguous_success":
        assert second["status"] == "released"
        assert second["reason"] == "released"
        assert wrappers and not wrappers[0].is_alive()
    else:
        assert second["status"] == "unknown"
        assert second["reason"] == "pending_catch_up"
        assert second["pending_write_count"] == 2
        assert wrappers and not wrappers[0].is_alive()

    if second["status"] != "released":
        # Restore the real worker path and prove the same provider receipt can recover the exact
        # durable row after the opaque launch failure. This also checks no queue reset occurred.
        module.spawn_context_thread = real_spawn
        client.fail_ingest = False
        client.release_ingest.set()
        third = provider.retire(receipt)
        assert third["status"] == "released"
        assert third["reason"] == "released"
    assert provider._queue is queue
    assert not queue._connections


@pytest.mark.parametrize("prestart_failure", ["factory", "start"])
@pytest.mark.parametrize("ambiguous_outcome", ["success", "failure"])
def test_retaindb_sequential_retry_faults_reconcile_fresh_worker_state(
    tmp_path, monkeypatch, prestart_failure, ambiguous_outcome,
):
    """A retry sequence must clear old launch evidence without losing accepted rows.

    The sequence deliberately crosses a real failed flush, a pre-start factory/start failure,
    and then an ambiguous start whose worker either settles or leaves the durable rows pending.
    All attempts use the original queue and receipt; a stale ``worker_start_failed`` marker must
    not mask the later native worker result.
    """
    import plugins.memory.retaindb as module

    provider, client, _home = _initialized_retaindb(tmp_path, monkeypatch)
    client.fail_after_release = True
    provider.sync_turn("accepted sequence row 1", "ack")
    provider.sync_turn("accepted sequence row 2", "ack")
    assert client.ingest_started.wait(10)
    queue = provider._queue
    assert queue is not None
    receipt = provider.retirement_receipt()

    # Keep the first native call in flight so the initial retirement records pending work before
    # the worker naturally exits and leaves both accepted rows durable.
    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 0.0)
    first = provider.retire(receipt)
    assert first["status"] == "unknown"
    assert first["reason"] == "pending_flush"
    client.release_ingest.set()
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)
    wait_until(lambda: queue._thread is not None and not queue._thread.is_alive(), what="sequence writer stop")
    assert queue._pending_count() == 2

    real_spawn = module.spawn_context_thread

    class _Unstartable:
        ident = None

        def start(self):
            raise RuntimeError("synthetic sequence pre-start failure")

    if prestart_failure == "factory":
        monkeypatch.setattr(
            module,
            "spawn_context_thread",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("synthetic sequence factory failure")),
        )
    else:
        monkeypatch.setattr(module, "spawn_context_thread", lambda *_args, **_kwargs: _Unstartable())

    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 10.0)
    prestart = provider.retire(receipt)
    assert prestart["status"] == "unknown"
    assert prestart["reason"] == "worker_start_failed"
    assert prestart["pending_write_count"] == 2
    assert queue._worker_start_failure == "worker_start_failed"
    assert provider._queue is queue

    client.ingest_started.clear()
    wrappers = []

    def _ambiguous_spawn(target, *, name, **kwargs):
        inner = real_spawn(target, name=name, **kwargs)

        class _Ambiguous:
            def __init__(self):
                self.inner = inner

            @property
            def ident(self):
                return self.inner.ident

            def start(self):
                self.inner.start()
                assert client.ingest_started.wait(10)
                raise RuntimeError("synthetic sequence ambiguous failure")

            def is_alive(self):
                return self.inner.is_alive()

            def join(self, *args, **kwargs):
                return self.inner.join(*args, **kwargs)

        wrapper = _Ambiguous()
        wrappers.append(wrapper)
        return wrapper

    monkeypatch.setattr(module, "spawn_context_thread", _ambiguous_spawn)
    if ambiguous_outcome == "success":
        client.fail_after_release = False
        client.fail_ingest = False
    else:
        client.fail_after_release = False
        client.fail_ingest = True

    ambiguous = provider.retire(receipt)
    assert queue._worker_start_failure is None
    assert wrappers and not wrappers[0].is_alive()
    if ambiguous_outcome == "success":
        assert ambiguous["status"] == "released"
        assert ambiguous["reason"] == "released"
        assert ambiguous["pending_write_count"] == 0
    else:
        assert ambiguous["status"] == "unknown"
        assert ambiguous["reason"] == "pending_catch_up"
        assert ambiguous["pending_write_count"] == 2
        assert provider._queue is queue

        # The failed ambiguous worker did not erase either accepted row. A fresh real worker
        # on the same queue and receipt can now settle both rows.
        monkeypatch.setattr(module, "spawn_context_thread", real_spawn)
        client.fail_ingest = False
        recovered = provider.retire(receipt)
        assert recovered["status"] == "released"
        assert recovered["reason"] == "released"
    assert provider._queue is queue
    assert not queue._connections


def test_retaindb_failed_initialize_shutdown_retains_ambiguous_soul_worker(tmp_path, monkeypatch):
    """Failed startup shutdown must retain a live SOUL worker for a later exact cleanup."""
    import plugins.memory.retaindb as module

    client = _RetainClient()
    client.block_seed = True

    class _ClientFactory:
        def __init__(self, _api_key, _base_url, _project):
            self.__dict__.update(client.__dict__)
            self.project = client.project

        def __getattr__(self, name):
            return getattr(client, name)

    monkeypatch.setattr(module, "_Client", _ClientFactory)
    real_spawn = module.spawn_context_thread
    wrappers = []

    def _ambiguous_spawn(target, *, name, **kwargs):
        inner = real_spawn(target, name=name, **kwargs)
        if name != "retaindb-soul-seed":
            return inner

        class _Ambiguous:
            def __init__(self):
                self.inner = inner

            @property
            def ident(self):
                return self.inner.ident

            def start(self):
                self.inner.start()
                assert client.seed_started.wait(10)
                raise RuntimeError("synthetic ambiguous soul start")

            def is_alive(self):
                return self.inner.is_alive()

            def join(self, *args, **kwargs):
                return self.inner.join(*args, **kwargs)

        wrapper = _Ambiguous()
        wrappers.append(wrapper)
        return wrapper

    monkeypatch.setattr(module, "spawn_context_thread", _ambiguous_spawn)
    home = tmp_path / "retaindb-failed-init"
    home.mkdir()
    (home / "SOUL.md").write_text("synthetic soul", encoding="utf-8")
    provider = module.RetainDBMemoryProvider()
    with pytest.raises(RuntimeError, match="synthetic ambiguous soul start"):
        provider.initialize("session-1", hermes_home=str(home))
    assert wrappers and wrappers[0].is_alive()
    queue = provider._queue
    client_obj = provider._client
    assert queue is not None and client_obj is not None
    assert provider._retirement_initialization_failed is True
    assert provider._retirement_active_calls == 1
    assert len(provider._retirement_threads) == 1

    # A bounded shutdown attempt must not drop the client/queue or claim the ambiguous worker
    # closed while its native seed call is still blocked.
    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 0.05)
    provider.shutdown()
    assert wrappers[0].is_alive()
    assert provider._queue is queue
    assert provider._client is client_obj
    assert provider._retirement_active_calls == 1
    assert provider._retirement_threads == {wrappers[0]}

    client.release_seed.set()
    wrappers[0].join(10)
    assert not wrappers[0].is_alive()
    wait_until(lambda: not provider._retirement_threads, what="ambiguous soul worker lease release")

    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 10.0)
    provider.shutdown()
    assert provider._queue is None
    assert provider._client is None


@pytest.mark.parametrize("provider_name", ["byterover", "retaindb"])
@pytest.mark.parametrize("failure_stage", ["factory", "start"])
def test_provider_worker_launch_failure_releases_admission_once(
    tmp_path, monkeypatch, provider_name, failure_stage,
):
    if provider_name == "byterover":
        from plugins.memory import byterover as module

        provider = module.ByteRoverMemoryProvider()
        provider.initialize("session-1", hermes_home=str(tmp_path / "brv-home"))
    else:
        from plugins.memory import retaindb as module

        provider, _client, _home = _initialized_retaindb(tmp_path, monkeypatch)

    class _Unstartable:
        def start(self):
            raise RuntimeError("synthetic thread start failure")

    if failure_stage == "factory":
        def _fail_spawn(*_args, **_kwargs):
            raise RuntimeError("synthetic thread factory failure")
    else:
        def _fail_spawn(*_args, **_kwargs):
            return _Unstartable()

    monkeypatch.setattr(module, "spawn_context_thread", _fail_spawn)
    with pytest.raises(RuntimeError, match="synthetic thread (factory|start) failure"):
        if provider_name == "byterover":
            provider._curate_in_background("accepted content", name="provider-test", what="test")
        else:
            provider._spawn_provider_thread(lambda: None, name="provider-test")

    assert provider._retirement_active_calls == 0
    assert provider._retirement_failures == 1
    assert provider._retirement_threads == set()

    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 10.0)
    result = provider.retire(provider.retirement_receipt())
    assert result["status"] == "unknown"
    assert result["reason"] == "flush_failed"
    assert result["pending_flush_count"] == 1

    if provider_name == "retaindb" and provider._queue is not None:
        provider._queue.shutdown()


def test_retaindb_retirement_drains_worker_and_closes_real_sqlite(tmp_path, monkeypatch):
    provider, client, _home = _initialized_retaindb(tmp_path, monkeypatch)
    provider.sync_turn("durable local turn", "ack")
    assert client.ingest_started.wait(10)
    receipt = provider.retirement_receipt()
    result_box = {}
    retiring = threading.Thread(target=lambda: result_box.setdefault("result", provider.retire(receipt)))
    retiring.start()
    wait_until(lambda: provider._retiring, what="RetainDB retirement admission fence")
    client.release_ingest.set()
    retiring.join(timeout=10)

    result = result_box["result"]
    _assert_result_shape(result)
    assert result["status"] == "released"
    assert result["reason"] == "released"
    assert provider._queue is not None
    assert provider._client is not None
    assert not provider._queue._connections
    assert provider.retire(receipt)["status"] == "released"


def test_retaindb_pending_row_replays_on_same_queue_and_receipt(tmp_path, monkeypatch):
    import plugins.memory.retaindb as module

    provider, client, _home = _initialized_retaindb(tmp_path, monkeypatch)
    client.fail_ingest = True
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)
    provider.sync_turn("pending durable turn", "ack")
    wait_until(lambda: client.ingest_calls >= 1, what="RetainDB failed ingest")
    receipt = provider.retirement_receipt()

    first = provider.retire(receipt)
    _assert_result_shape(first)
    assert first["status"] == "unknown"
    assert first["reason"] == "pending_catch_up"
    assert first["pending_write_count"] == 1
    assert provider._queue is not None

    client.fail_ingest = False
    client.release_ingest.set()
    second = provider.retire(receipt)
    _assert_result_shape(second)
    assert second["status"] == "released"
    assert second["reason"] == "released"
    assert provider._queue is not None
    assert not provider._queue._connections


def test_retaindb_prefetch_worker_is_retirement_custody(tmp_path, monkeypatch):
    provider, client, _home = _initialized_retaindb(tmp_path, monkeypatch)
    provider.queue_prefetch("what do you know")
    assert client.prefetch_started.wait(10)
    receipt = provider.retirement_receipt()
    result_box = {}
    retiring = threading.Thread(target=lambda: result_box.setdefault("result", provider.retire(receipt)))
    retiring.start()
    wait_until(lambda: provider._retiring, what="RetainDB prefetch retirement fence")
    client.release_prefetch.set()
    retiring.join(timeout=10)

    result = result_box["result"]
    assert result["status"] == "released"
    assert result["reason"] == "released"


def test_retaindb_retire_waits_for_live_native_finalizer(tmp_path, monkeypatch):
    """RetainDB keeps an admitted provider worker until its native wrapper truly exits."""
    import plugins.memory.retaindb as module

    provider, _client, _home = _initialized_retaindb(tmp_path, monkeypatch)
    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 0.05)
    in_finalizer, release_finalizer = threading.Event(), threading.Event()
    original_spawn = module.spawn_context_thread
    workers = []

    def spawn_with_native_finalizer(target, **kwargs):
        def body():
            try:
                target()
            finally:
                in_finalizer.set()
                assert release_finalizer.wait(10)

        worker = original_spawn(body, **kwargs)
        workers.append(worker)
        return worker

    monkeypatch.setattr(module, "spawn_context_thread", spawn_with_native_finalizer)
    worker = provider._spawn_provider_thread(lambda: True, name="retaindb-finalizer")
    assert worker is not None
    assert in_finalizer.wait(10)
    assert worker.is_alive()
    assert provider._retirement_active_calls == 0

    result = provider.retire(provider.retirement_receipt())
    assert result["status"] == "unknown"
    assert result["reason"] == "pending_flush"
    assert worker in provider._retirement_threads

    release_finalizer.set()
    worker.join(10)
    wait_until(lambda: not provider._retirement_threads, what="RetainDB native finalizer lease release")
    assert provider.retire(provider.retirement_receipt())["status"] == "released"


def test_retaindb_soul_seed_worker_is_retirement_custody(tmp_path, monkeypatch):
    client = _RetainClient()
    client.block_seed = True
    provider, client, _home = _initialized_retaindb(tmp_path, monkeypatch, client, soul="synthetic soul")
    assert client.seed_started.wait(10)
    receipt = provider.retirement_receipt()
    result_box = {}
    retiring = threading.Thread(target=lambda: result_box.setdefault("result", provider.retire(receipt)))
    retiring.start()
    wait_until(lambda: provider._retiring, what="RetainDB soul retirement fence")
    client.release_seed.set()
    retiring.join(timeout=10)

    result = result_box["result"]
    assert result["status"] == "released"
    assert result["reason"] == "released"


def test_retaindb_close_failure_retains_queue_until_retry(tmp_path, monkeypatch):
    provider, _client, _home = _initialized_retaindb(tmp_path, monkeypatch)
    receipt = provider.retirement_receipt()
    queue_obj = provider._queue
    assert queue_obj is not None
    real_close = queue_obj._close
    monkeypatch.setattr(queue_obj, "_close", lambda *args, **kwargs: 1)

    first = provider.retire(receipt)
    assert first["status"] == "unknown"
    assert first["reason"] == "close_failed"
    assert first["close_failure_count"] == 1
    assert provider._queue is queue_obj
    assert provider._client is not None

    monkeypatch.setattr(queue_obj, "_close", real_close)
    second = provider.retire(receipt)
    assert second["status"] == "released"
    assert second["reason"] == "released"
