"""Exact retirement leases follow real async admission and terminal settlement."""

import json
import queue
import threading
import time

import pytest


def _clear_completion_queue(process_registry):
    while True:
        try:
            process_registry.completion_queue.get_nowait()
        except queue.Empty:
            return


def _event_for(process_registry, delegation_id, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            event = process_registry.completion_queue.get(timeout=0.1)
        except queue.Empty:
            continue
        if event.get("delegation_id") == delegation_id:
            return event
    return None


def _record_for(ad, delegation_id, timeout=5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with ad._records_lock:
            record = ad._records.get(delegation_id)
            if record is not None and record.get("status") not in {"running", "finalizing"}:
                return dict(record)
        time.sleep(0.01)
    return None


def _row(ad, delegation_id):
    with ad._DB_LOCK, ad._transaction() as conn:
        return conn.execute(
            "SELECT state, delivery_state, result_json FROM async_delegations WHERE delegation_id=?",
            (delegation_id,),
        ).fetchone()


class _RecordingExecutor:
    def __init__(self, max_workers):
        from tools.daemon_pool import DaemonThreadPoolExecutor
        self.pool = DaemonThreadPoolExecutor(max_workers=max_workers)
        self.futures = []

    def submit_owned(self, handoff_id, fn, /, *args, **kwargs):
        result = self.pool.submit_owned(handoff_id, fn, *args, **kwargs)
        if result.future is not None:
            self.futures.append(result.future)
        return result

    def shutdown(self, **kwargs):
        return self.pool.shutdown(**kwargs)


def _dispatch(ad, runner):
    return ad.dispatch_async_delegation(
        goal="exact lease", context=None, toolsets=None, role="leaf", model=None,
        session_key="exact-lease-test", runner=runner, max_async_children=2,
    )


def test_worker_lease_releases_only_after_real_runner_and_finalizer(tmp_path, monkeypatch):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad
    from tools.process_registry import process_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    _clear_completion_queue(process_registry)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    executor = _RecordingExecutor(max_workers=1)
    monkeypatch.setattr(ad, "_get_executor", lambda _n: executor)
    entered, release = threading.Event(), threading.Event()

    def runner():
        entered.set()
        assert release.wait(5)
        return {"summary": "settled", "status": "completed"}

    try:
        handle = _dispatch(ad, runner)
        assert handle["status"] == "dispatched"
        assert entered.wait(5)
        assert fence.active_count() == 1
        close = fence.begin_close(owner_id="lease-test")
        assert close is not None
        assert fence.commit_close(close) is False
        assert fence.release_close(close) is True
        release.set()
        event = _event_for(process_registry, handle["delegation_id"])
        assert event is not None and event["status"] == "completed"
        assert executor.futures[0].result(timeout=5) is None
        assert fence.active_count() == 0
        close = fence.begin_close(owner_id="lease-test-finished")
        assert close is not None and fence.commit_close(close) is True
    finally:
        release.set()
        executor.shutdown(wait=True)
        ad._reset_for_tests()
        _clear_completion_queue(process_registry)


def test_queued_future_cancellation_settles_real_record(tmp_path, monkeypatch):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad
    from tools.process_registry import process_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    _clear_completion_queue(process_registry)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    executor = _RecordingExecutor(max_workers=1)
    monkeypatch.setattr(ad, "_get_executor", lambda _n: executor)
    first_started, first_release, second_started = threading.Event(), threading.Event(), threading.Event()

    def first():
        first_started.set()
        assert first_release.wait(5)
        return {"summary": "first", "status": "completed"}

    def second():
        second_started.set()
        return {"summary": "second", "status": "completed"}

    try:
        first_handle = _dispatch(ad, first)
        assert first_started.wait(5)
        second_handle = _dispatch(ad, second)
        second_future = executor.futures[-1]
        assert second_future.cancel() is True
        second_event = _event_for(process_registry, second_handle["delegation_id"])
        assert second_event is not None and second_event["status"] == "interrupted"
        assert not second_started.is_set()
        assert _row(ad, second_handle["delegation_id"])[0:2] == ("interrupted", "pending")
        assert fence.active_count() == 1
        first_release.set()
        assert _event_for(process_registry, first_handle["delegation_id"]) is not None
        assert executor.futures[0].result(timeout=5) is None
        assert fence.active_count() == 0
    finally:
        first_release.set()
        executor.shutdown(wait=True, cancel_futures=True)
        ad._reset_for_tests()
        _clear_completion_queue(process_registry)


def test_executor_construction_failure_is_only_definitive_no_submit(tmp_path, monkeypatch):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    monkeypatch.setattr(ad, "_get_executor", lambda _n: (_ for _ in ()).throw(RuntimeError("construction failed")))
    entered = threading.Event()

    try:
        result = _dispatch(ad, lambda: entered.set() or {"status": "completed"})
        assert result["status"] == "rejected"
        assert not entered.is_set()
        with ad._records_lock:
            assert not ad._records
        with ad._DB_LOCK, ad._transaction() as conn:
            assert conn.execute("SELECT COUNT(*) FROM async_delegations").fetchone()[0] == 0
        assert fence.active_count() == 0
    finally:
        ad._reset_for_tests()


def test_definite_executor_refusal_releases_exact_lease_without_worker(tmp_path, monkeypatch):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad
    from tools.process_registry import process_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    _clear_completion_queue(process_registry)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    delegation_id = "deleg_executor_shutdown"
    monkeypatch.setattr(ad, "_new_delegation_id", lambda: delegation_id)
    executor = _RecordingExecutor(max_workers=1)
    executor.shutdown(wait=True)
    monkeypatch.setattr(ad, "_get_executor", lambda _n: executor)
    entered = threading.Event()

    try:
        handle = _dispatch(ad, lambda: entered.set() or {"status": "completed"})
        assert handle["status"] == "rejected"
        assert not entered.is_set()
        assert executor.futures == []
        with ad._records_lock:
            assert delegation_id not in ad._records
        assert _row(ad, delegation_id) is None
        assert fence.active_count() == 0
    finally:
        executor.shutdown(wait=True)
        ad._reset_for_tests()
        _clear_completion_queue(process_registry)


@pytest.mark.parametrize("late_flag", ["_shutdown", "_broken"])
def test_submit_raise_after_acceptance_retains_row_and_exact_lease(tmp_path, monkeypatch, late_flag):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad
    from tools.process_registry import process_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    _clear_completion_queue(process_registry)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)

    class PostAcceptRaiseExecutor(_RecordingExecutor):
        def submit_owned(self, handoff_id, fn, /, *args, **kwargs):
            result = super().submit_owned(handoff_id, fn, *args, **kwargs)
            setattr(self.pool, late_flag, True)
            return type(result)(result.handoff_id, result.future, "yes", RuntimeError(
                "submit raised after accepting the Future"))

    executor = PostAcceptRaiseExecutor(max_workers=1)
    monkeypatch.setattr(ad, "_get_executor", lambda _n: executor)
    entered, release = threading.Event(), threading.Event()

    def runner():
        entered.set()
        assert release.wait(5)
        return {"summary": "accepted", "status": "completed"}

    try:
        handle = _dispatch(ad, runner)
        assert handle["status"] == "dispatched"
        assert handle["acceptance"] == "unknown"
        assert entered.wait(5)
        assert _row(ad, handle["delegation_id"])[0] == "running"
        assert fence.active_count() == 1
        release.set()
        assert _event_for(process_registry, handle["delegation_id"]) is not None
        assert executor.futures[0].result(timeout=5) is None
        assert _row(ad, handle["delegation_id"])[0] == "completed"
        assert fence.active_count() == 0
    finally:
        release.set()
        executor.shutdown(wait=True)
        ad._reset_for_tests()
        _clear_completion_queue(process_registry)


def test_durable_settlement_failure_keeps_lease_until_owner_retry(tmp_path, monkeypatch):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad
    from tools.process_registry import process_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    _clear_completion_queue(process_registry)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    original_persist = ad._persist_completion

    def fail_once(event, result):
        raise RuntimeError("durable completion write failed")

    monkeypatch.setattr(ad, "_persist_completion", fail_once)
    try:
        handle = _dispatch(ad, lambda: {"summary": "durable pending", "status": "completed"})
        event = _event_for(process_registry, handle["delegation_id"])
        assert event is not None and event["status"] == "completed"
        record = _record_for(ad, handle["delegation_id"])
        assert record is not None and record["status"] == "completed"
        assert record["_settlement_pending"] is True
        assert ad.active_count() == 0
        assert fence.active_count() == 1
        monkeypatch.setattr(ad, "_persist_completion", original_persist)
        assert ad.retry_pending_settlement(handle["delegation_id"]) is True
        assert fence.active_count() == 0
        assert _row(ad, handle["delegation_id"])[0:2] == ("completed", "pending")
    finally:
        ad._reset_for_tests()
        _clear_completion_queue(process_registry)


class _FailingQueue:
    def put(self, _event):
        raise RuntimeError("completion queue unavailable")


def test_completion_queue_failure_keeps_lease_until_owner_retry(tmp_path, monkeypatch):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad
    from tools.process_registry import process_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    monkeypatch.setattr(process_registry, "completion_queue", _FailingQueue())

    try:
        handle = _dispatch(ad, lambda: {"summary": "queue pending", "status": "completed"})
        record = _record_for(ad, handle["delegation_id"])
        assert record is not None and record["status"] == "completed"
        assert record["_settlement_pending"] is True
        assert ad.active_count() == 0
        assert fence.active_count() == 1
        replacement = queue.Queue()
        monkeypatch.setattr(process_registry, "completion_queue", replacement)
        assert ad.retry_pending_settlement(handle["delegation_id"]) is True
        event = replacement.get(timeout=5)
        assert event["delegation_id"] == handle["delegation_id"]
        assert event["status"] == "completed"
        assert fence.active_count() == 0
    finally:
        ad._reset_for_tests()



def test_concurrent_finalizers_share_one_receipt_and_release_after_settlement(tmp_path, monkeypatch):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad
    from tools.process_registry import process_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    _clear_completion_queue(process_registry)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    executor = _RecordingExecutor(max_workers=1)
    monkeypatch.setattr(ad, "_get_executor", lambda _n: executor)
    entered, release = threading.Event(), threading.Event()
    push_entered, push_release = threading.Event(), threading.Event()

    def runner():
        entered.set()
        assert release.wait(5)
        return {"summary": "runner", "status": "completed"}

    try:
        handle = _dispatch(ad, runner)
        assert entered.wait(5)
        original_push = ad._push_completion_event
        pushed = []

        def blocked_push(record, result, status):
            pushed.append(record["delegation_id"])
            push_entered.set()
            assert push_release.wait(5)
            return original_push(record, result, status)

        monkeypatch.setattr(ad, "_push_completion_event", blocked_push)
        barrier = threading.Barrier(2)
        outcomes, errors = [], []

        def finalize_once():
            try:
                barrier.wait(5)
                outcomes.append(ad._finalize(handle["delegation_id"], {"summary": "race"}, "completed"))
            except BaseException as exc:
                errors.append(exc)

        first = threading.Thread(target=finalize_once)
        second = threading.Thread(target=finalize_once)
        first.start()
        second.start()
        assert push_entered.wait(5)
        assert fence.active_count() == 1
        push_release.set()
        release.set()
        assert executor.futures[0].result(timeout=5) is None
        first.join(5)
        second.join(5)
        assert not first.is_alive() and not second.is_alive()
        assert not errors
        assert len(outcomes) == 2 and all(outcomes)
        assert pushed == [handle["delegation_id"]]
        with ad._records_lock:
            release_after = ad._records[handle["delegation_id"]]["_release_after_settlement"]
        release_after()
        assert fence.active_count() == 0
        event = _event_for(process_registry, handle["delegation_id"])
        assert event is not None and event["status"] == "completed"
        try:
            process_registry.completion_queue.get_nowait()
        except queue.Empty:
            pass
        else:
            raise AssertionError("concurrent finalization published a duplicate event")
    finally:
        push_release.set()
        release.set()
        executor.shutdown(wait=True)
        ad._reset_for_tests()
        _clear_completion_queue(process_registry)


def test_concurrent_settlement_retry_claims_one_owner(tmp_path, monkeypatch):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad
    from tools.process_registry import process_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    _clear_completion_queue(process_registry)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    monkeypatch.setattr(process_registry, "completion_queue", _FailingQueue())

    try:
        handle = _dispatch(ad, lambda: {"summary": "retry race", "status": "completed"})
        record = _record_for(ad, handle["delegation_id"])
        assert record is not None and record["_settlement_pending"] is True
        replacement = queue.Queue()
        monkeypatch.setattr(process_registry, "completion_queue", replacement)
        original_enqueue = ad._enqueue_completion_event
        enqueue_entered, enqueue_release = threading.Event(), threading.Event()
        enqueues = []

        def blocked_enqueue(event, label):
            enqueues.append(event["delegation_id"])
            enqueue_entered.set()
            assert enqueue_release.wait(5)
            return original_enqueue(event, label)

        monkeypatch.setattr(ad, "_enqueue_completion_event", blocked_enqueue)
        results = []
        first = threading.Thread(target=lambda: results.append(ad.retry_pending_settlement(handle["delegation_id"])))
        first.start()
        assert enqueue_entered.wait(5)
        second = threading.Thread(target=lambda: results.append(ad.retry_pending_settlement(handle["delegation_id"])))
        second.start()
        second.join(5)
        assert not second.is_alive()
        assert results == [False]
        enqueue_release.set()
        first.join(5)
        assert not first.is_alive()
        assert results.count(True) == 1
        assert enqueues == [handle["delegation_id"]]
        event = replacement.get(timeout=5)
        assert event["delegation_id"] == handle["delegation_id"]
        try:
            replacement.get_nowait()
        except queue.Empty:
            pass
        else:
            raise AssertionError("settlement retry published a duplicate event")
        assert fence.active_count() == 0
    finally:
        if 'enqueue_release' in locals():
            enqueue_release.set()
        ad._reset_for_tests()
        _clear_completion_queue(process_registry)


def test_persist_dispatch_commit_then_raise_reconciles_once(tmp_path, monkeypatch):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad
    from tools.process_registry import process_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    _clear_completion_queue(process_registry)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    delegation_id = "deleg_commit_then_raise"
    monkeypatch.setattr(ad, "_new_delegation_id", lambda: delegation_id)
    original_persist = ad._persist_dispatch
    entered = threading.Event()
    runner_entered = threading.Event()

    def commit_then_raise(record):
        original_persist(record)
        entered.set()
        raise RuntimeError("commit acknowledged after the caller lost the return")

    monkeypatch.setattr(ad, "_persist_dispatch", commit_then_raise)

    try:
        handle = _dispatch(ad, lambda: runner_entered.set() or {"status": "completed"})
        assert entered.is_set()
        assert handle["status"] == "rejected"
        assert not runner_entered.is_set()
        event = _event_for(process_registry, delegation_id)
        assert event is not None and event["status"] == "error"
        assert _row(ad, delegation_id)[0:2] == ("error", "pending")
        with ad._DB_LOCK, ad._transaction() as conn:
            claim = conn.execute("SELECT execution_json FROM async_delegations WHERE delegation_id=?", (delegation_id,)).fetchone()[0]
        assert claim and json.loads(claim)["state"] == "rejected_before_enqueue"
        assert fence.active_count() == 0
    finally:
        ad._reset_for_tests()
        _clear_completion_queue(process_registry)


def test_persist_dispatch_missing_row_proves_no_submission_and_cleans_up(tmp_path, monkeypatch):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad
    from tools.process_registry import process_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    _clear_completion_queue(process_registry)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    delegation_id = "deleg_missing_admission"
    monkeypatch.setattr(ad, "_new_delegation_id", lambda: delegation_id)
    monkeypatch.setattr(ad, "_persist_dispatch", lambda _record: (_ for _ in ()).throw(RuntimeError("before commit")))
    runner_entered = threading.Event()

    try:
        handle = _dispatch(ad, lambda: runner_entered.set() or {"status": "completed"})
        assert handle["status"] == "rejected"
        assert not runner_entered.is_set()
        with ad._records_lock:
            assert delegation_id not in ad._records
        assert _row(ad, delegation_id) is None
        assert fence.active_count() == 0
    finally:
        ad._reset_for_tests()
        _clear_completion_queue(process_registry)


def test_owned_submit_retains_same_future_when_worker_start_raises():
    from tools.daemon_pool import DaemonThreadPoolExecutor

    entered, exited = threading.Event(), threading.Event()

    class StartAfterQueueRaises(DaemonThreadPoolExecutor):
        def _adjust_thread_count(self):
            super()._adjust_thread_count()
            raise RuntimeError("worker-start bookkeeping failed after queue acceptance")

    pool = StartAfterQueueRaises(max_workers=1)
    try:
        submission = pool.submit_owned(
            "handoff-start-raise", lambda: entered.set() or "one",
            on_started=lambda _id: None, on_exited=lambda _id: exited.set())
        assert submission.accepted == "yes"
        assert isinstance(submission.error, RuntimeError)
        assert submission.future is not None
        assert submission.future.result(timeout=5) == "one"
        assert entered.is_set() and exited.is_set()
    finally:
        pool.shutdown(wait=True)


def test_owned_submit_rejected_before_enqueue_has_no_future():
    from tools.daemon_pool import DaemonThreadPoolExecutor

    pool = DaemonThreadPoolExecutor(max_workers=1)
    pool.shutdown(wait=True)
    submission = pool.submit_owned("handoff-rejected", lambda: None)
    assert submission.accepted == "no"
    assert submission.future is None


def test_base_exception_keeps_typed_receipt_for_owner_retry(tmp_path, monkeypatch):
    from hermes_cli import backend_retirement
    from tools import async_delegation as ad
    from tools.process_registry import process_registry

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    ad._reset_for_tests()
    _clear_completion_queue(process_registry)
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    original_persist = ad._persist_completion
    monkeypatch.setattr(ad, "_persist_completion", lambda *_: (_ for _ in ()).throw(KeyboardInterrupt("provider stop")))

    try:
        handle = _dispatch(ad, lambda: (_ for _ in ()).throw(KeyboardInterrupt("runner stop")))
        deadline = time.monotonic() + 5
        record = None
        while time.monotonic() < deadline:
            with ad._records_lock:
                candidate = ad._records.get(handle["delegation_id"])
                if candidate is not None and candidate.get("_settlement_pending"):
                    record = dict(candidate)
                    break
            time.sleep(0.01)
        assert record is not None
        receipt = record["_settlement_receipt"]
        assert receipt.event["delegation_id"] == handle["delegation_id"]
        assert receipt.result["exception"]["type"] == "KeyboardInterrupt"
        assert record["execution"]["state"] == "runner_exited"
        future = record.get("_future")
        assert future is not None
        with pytest.raises(KeyboardInterrupt, match="runner stop"):
            future.result(timeout=5)
        monkeypatch.setattr(ad, "_persist_completion", original_persist)
        assert ad.retry_pending_settlement(handle["delegation_id"]) is True
        assert fence.active_count() == 0
    finally:
        monkeypatch.setattr(ad, "_persist_completion", original_persist)
        ad._reset_for_tests()
        _clear_completion_queue(process_registry)


def test_supported_executor_uses_real_future_and_context_task_contract():
    from tools.daemon_pool import DaemonThreadPoolExecutor

    pool = DaemonThreadPoolExecutor(max_workers=1)
    try:
        submission = pool.submit_owned("handoff-3-14", lambda: {"status": "completed"})
        assert submission.accepted == "yes"
        assert submission.future is not None and submission.future.result(timeout=5)["status"] == "completed"
    finally:
        pool.shutdown(wait=True)


def test_remote_outbox_retry_replays_the_store_journal_once():
    from tools.async_delegation_worker import WorkerDelegationLedger

    class Store:
        scope = {"pid": 1}
        def __init__(self):
            self.calls = 0
        def retry_pending(self):
            self.calls += 1
            return [{"value": None}]

    store = Store()
    ledger = WorkerDelegationLedger(store)
    assert ledger.retry_pending_settlement() == [{"value": None}]
    assert store.calls == 1


def test_remote_execution_rejects_wrong_authenticated_owner_scope():
    import json
    import sqlite3
    from gateway import session_worker_delegation as swd
    from hermes_state_runtime import RuntimeStoreError

    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    db.execute("CREATE TABLE async_delegations (delegation_id TEXT, parent_session_id TEXT, owner_execution_id TEXT, owner_pid INTEGER, owner_started_at REAL, execution_json TEXT, state TEXT)")
    claim = {"handoff_id": "d", "state": "prepared", "accepted": "unknown", "worker_started": False, "worker_exited": False,
             "owner_pid": 41, "owner_started_at": 4.1, "owner_execution_id": "e",
             "owner_scope": {"profile_id": "/p", "session_id": "s", "generation": 2, "epoch": 3}, "error": None, "exception": None}
    db.execute("INSERT INTO async_delegations VALUES (?,?,?,?,?,?,?)", ("d", "s", "e", 41, 4.1, json.dumps(claim), "running"))
    handlers = swd.worker_delegation_handlers("e", 41, 4.1, profile_id="/p", session_id="s", generation=2, epoch=3)
    claim["owner_pid"] = 99
    with pytest.raises(RuntimeStoreError):
        handlers["delegation.execution"](None, db, "s", {"delegation_id": "d", "execution": claim})


def test_missing_owner_birth_is_not_proof_of_dead_process(monkeypatch):
    from tools import async_delegation as ad

    alive = ad._owner_liveness()
    assert alive is not None and alive(123, None) is None


def test_settlement_pending_record_survives_inmemory_prune(monkeypatch):
    from tools import async_delegation as ad

    with ad._records_lock:
        saved = dict(ad._records)
        ad._records.clear()
        ad._records.update({
            "pending": {"status": "completed", "_settlement_pending": True, "completed_at": 1},
            "ordinary": {"status": "completed", "_settlement_pending": False, "completed_at": 2},
        })
        monkeypatch.setattr(ad, "_MAX_RETAINED_COMPLETED", 0)
        ad._prune_completed_locked()
        assert "pending" in ad._records and "ordinary" not in ad._records
        ad._records.clear()
        ad._records.update(saved)
