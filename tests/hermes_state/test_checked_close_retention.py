"""A failed physical close keeps the exact object available for a checked retry."""

import sqlite3
import threading
import types

import pytest

import hermes_state_registry as registry
from gateway.session_db_recovery import RecoverableHandleCache


@pytest.fixture(autouse=True)
def clean_registry():
    registry.close_all()
    registry._teardown_failure_count = 0
    yield
    registry.close_all()
    registry._teardown_failure_count = 0


def test_registry_checked_retry_closes_the_same_failed_generation(tmp_path, monkeypatch):
    db = registry.acquire(tmp_path / 'state.db')
    original_close = db.close
    attempts = []

    def close_once_failed():
        attempts.append(db)
        if len(attempts) == 1:
            raise OSError('synthetic close failure')
        return original_close()

    monkeypatch.setattr(db, 'close', close_once_failed)
    try:
        assert registry.close_all_checked().verified is False
        assert db._conn is not None
        second = registry.close_all_checked()
        assert attempts == [db, db]
        assert second.verified is True
        assert db._conn is None
    finally:
        original_close()


def test_checked_release_does_not_claim_nonfinal_reference_is_closed(tmp_path):
    db = registry.acquire(tmp_path / 'state.db')
    registry.acquire(tmp_path / 'state.db')

    receipt = registry.release_checked(db)

    assert receipt.verified is False
    assert receipt.pending == 1
    assert db._conn is not None
    assert registry.close_all_checked().verified


def test_unknown_teardown_result_cannot_prove_physical_close(tmp_path, monkeypatch):
    db = registry.acquire(tmp_path / 'state.db')
    original_close = db.close
    monkeypatch.setattr(registry, '_teardown', lambda _db: None)
    try:
        receipt = registry.close_all_checked()
        assert db._conn is not None
        assert receipt.verified is False
    finally:
        db._shared_registry_owned = False
        original_close()


def test_checked_receipt_covers_real_writer_reader_and_token_resources(tmp_path):
    db = registry.acquire(tmp_path / 'state.db')
    db.create_session('s', 'cli', model='m')
    db.queue_token_counts('s', input_tokens=1)
    reader = threading.Thread(target=lambda: db.get_session('s'))
    reader.start()
    reader.join(5)
    assert not reader.is_alive()
    assert db._read_pool.qsize() >= 1
    assert db._token_writer_thread is not None

    receipt = registry.close_all_checked()

    assert receipt.verified
    assert db._conn is None
    assert db._connection_pinned is False
    assert db._read_conns_closed is True
    assert db._read_pool.qsize() == 0
    assert db._failed_read_conns == []
    assert db._token_writer_thread is None
    assert db._token_writer_busy is False
    assert not db._token_queue


def test_checked_receipt_waits_for_an_inflight_wal_reader(tmp_path):
    db = registry.acquire(tmp_path / 'state.db')
    db.create_session('s', 'cli', model='m')
    entered = threading.Event()
    release_reader = threading.Event()

    def read_once():
        with db._read_ctx() as conn:
            conn.execute('SELECT 1').fetchone()
            entered.set()
            assert release_reader.wait(5)

    reader = threading.Thread(target=read_once)
    reader.start()
    try:
        assert entered.wait(5)
        first = registry.close_all_checked()
        assert first.verified is False
        assert db._active_read_contexts == 1

        release_reader.set()
        reader.join(5)
        assert not reader.is_alive()
        second = registry.close_all_checked()
        assert second.verified is True
    finally:
        release_reader.set()
        reader.join(5)


def test_blocked_token_writer_retains_native_connection_and_accepted_delta(
    tmp_path, monkeypatch,
):
    import sqlite3

    path = tmp_path / 'state.db'
    db = registry.acquire(path)
    db.create_session('blocked-token', 'cli', model='synthetic-model')
    native = db._conn
    entered, release_writer = threading.Event(), threading.Event()
    apply_batch, stop_writer = db._apply_token_batch, db._stop_token_writer

    def blocked_apply(batch):
        entered.set()
        assert release_writer.wait(5)
        apply_batch(batch)

    monkeypatch.setattr(db, '_apply_token_batch', blocked_apply)
    monkeypatch.setattr(db, '_stop_token_writer', lambda: stop_writer(join_timeout=0.01))
    db.queue_token_counts('blocked-token', input_tokens=7)
    writer = db._token_writer_thread
    try:
        assert entered.wait(5)
        first = registry.close_all_checked()
        retained = db._conn is native
        release_writer.set()
        writer.join(5)
        assert not writer.is_alive()
        second = registry.close_all_checked()
        with sqlite3.connect(path) as observer:
            count = observer.execute(
                'SELECT input_tokens FROM sessions WHERE id = ?', ('blocked-token',),
            ).fetchone()[0]
        assert first.verified is False
        assert retained is True, 'unsettled writer lost its exact native connection'
        assert count == 7, 'accepted token delta was lost during checked close'
        assert second.verified is True
    finally:
        release_writer.set()
        writer.join(5)
        registry.close_all_checked()


def test_checked_unregistered_fallback_retains_native_writer_until_token_batch_finishes(tmp_path):
    """Checked fallback close must not drop a real writer connection mid-apply."""
    from hermes_state import SessionDB

    path = tmp_path / "state.db"
    db = SessionDB(db_path=path)
    db.create_session("direct-fallback", "cli", model="synthetic-model")
    entered = threading.Event()
    release_writer = threading.Event()
    apply_batch = db._apply_token_batch
    stop_writer = db._stop_token_writer

    def blocked_apply(batch):
        entered.set()
        assert release_writer.wait(5)
        apply_batch(batch)

    def short_stop():
        stop_writer(join_timeout=0.01)

    db._apply_token_batch = blocked_apply
    db._stop_token_writer = short_stop
    db.queue_token_counts("direct-fallback", input_tokens=3)
    writer = db._token_writer_thread
    assert writer is not None
    native = db._conn
    try:
        assert entered.wait(5)
        first = registry.release_or_close_checked(db)
        assert first.verified is False
        # The checked fallback must retain the exact native writer while the accepted batch is live.
        assert db._conn is native

        release_writer.set()
        writer.join(5)
        assert not writer.is_alive()
        second = registry.release_or_close_checked(db)
        assert second.verified is True
        with sqlite3.connect(path) as observer:
            stored = observer.execute(
                "SELECT input_tokens FROM sessions WHERE id = ?", ("direct-fallback",)
            ).fetchone()[0]
        assert stored == 3
    finally:
        release_writer.set()
        writer.join(5)
        if db._conn is not None:
            db.close()


def test_native_read_close_failure_retains_exact_pooled_connection(tmp_path, monkeypatch):
    db = registry.acquire(tmp_path / 'state.db')
    db.create_session('s', 'cli', model='m')
    reader = threading.Thread(target=lambda: db.get_session('s'))
    reader.start()
    reader.join(5)
    assert not reader.is_alive()
    pooled = db._read_pool.get_nowait()
    db._read_pool.put_nowait(pooled)
    original_close = db._close_conn_logged
    attempts = []

    def fail_read_twice(conn, label):
        if conn is pooled and len(attempts) < 2:
            attempts.append(conn)
            return False
        attempts.append(conn)
        return original_close(conn, label)

    monkeypatch.setattr(db, '_close_conn_logged', fail_read_twice)
    first = registry.close_all_checked()
    assert first.verified is False
    assert db._failed_read_conns == [pooled]
    assert db._conn is None

    second = registry.close_all_checked()
    assert second.verified is True
    assert attempts == [pooled, pooled, pooled]
    assert db._failed_read_conns == []


def test_native_close_failure_retains_exact_writer_for_checked_retry(tmp_path, monkeypatch):
    db = registry.acquire(tmp_path / 'state.db')
    native = db._conn
    original_close = db._close_connection_quietly
    attempts = []

    def fail_native_once(conn):
        attempts.append(conn)
        if len(attempts) == 1:
            db._last_native_close_succeeded = False
            return False
        return original_close(conn)

    monkeypatch.setattr(db, '_close_connection_quietly', fail_native_once)
    try:
        first = registry.close_all_checked()
        assert first.verified is False
        assert db._conn is native
        assert db._checked_close_proven() is False

        second = registry.close_all_checked()
        assert second.verified is True
        assert attempts == [native, native]
        assert db._conn is None
    finally:
        db._shared_registry_owned = False
        if db._conn is not None:
            db.close()


def test_checked_receipt_rejects_actual_pinned_native_generation(tmp_path, monkeypatch):
    db = registry.acquire(tmp_path / 'state.db')
    native = db._conn
    pins = []
    monkeypatch.setattr(db, '_retire_connection', pins.append)
    monkeypatch.setattr(db, '_disable_close_time_checkpoint', lambda: False)
    monkeypatch.setattr(db, '_capture_retired_generation', lambda _trigger: tmp_path / 'captured')
    db._db_wal_generation_lost = True
    try:
        first = registry.close_all_checked()
        assert first.verified is False
        assert db._conn is None
        assert db._connection_pinned is True
        assert pins == [native]
        native.execute('SELECT 1').fetchone()

        second = registry.close_all_checked()
        assert second.verified is False
        assert second.failed == 1
        assert registry._failed_generations[id(db)].db is db
        with pytest.raises(RuntimeError, match='unverified'):
            registry.acquire(tmp_path / 'state.db')
        registry.close_all()
        assert registry._failed_generations[id(db)].db is db
    finally:
        db._shared_registry_owned = False
        native.close()
        db._connection_pinned = False


@pytest.mark.parametrize('first_result', [False, None])
def test_cache_checked_close_retains_exact_handle_until_retry(tmp_path, first_result):
    from hermes_state import SessionDB

    path = tmp_path / 'cache.db'
    db = SessionDB(db_path=path)
    cache = RecoverableHandleCache(handles={path: db})
    attempts = []

    def close(handle):
        attempts.append(handle)
        if len(attempts) == 1:
            return first_result
        handle.close()
        return True

    try:
        assert cache.close_all_checked(close).verified is False
        assert cache.handles.get(path) is db
        assert cache.close_all_checked(close).verified is True
        assert attempts == [db, db]
        assert path not in cache.handles
        assert db._conn is None
    finally:
        db.close()


def test_checked_cache_sweep_cannot_forget_an_admitted_inflight_open(tmp_path):
    from hermes_state import SessionDB

    path = tmp_path / 'opening.db'
    cache = RecoverableHandleCache()
    started, release = threading.Event(), threading.Event()
    opened = []

    def opener():
        started.set()
        assert release.wait(5)
        db = SessionDB(db_path=path)
        opened.append(db)
        return db

    def close(db):
        db.close()
        return True

    worker = threading.Thread(target=lambda: cache.get(path, opener))
    worker.start()
    try:
        assert started.wait(5)
        receipt = cache.close_all_checked(close)
        assert receipt.verified is False
        assert receipt.pending > 0
    finally:
        release.set()
        worker.join(5)
        assert not worker.is_alive()
        for db in opened:
            db.close()


def test_checked_cache_preserves_stale_real_handle_across_legacy_sweep(tmp_path):
    """A legacy cache sweep must not overwrite the checked callback for an admitted opener."""
    cache = RecoverableHandleCache()
    path = tmp_path / "cached.db"
    started = threading.Event()
    release_opener = threading.Event()
    opened = []
    result = []

    def opener():
        started.set()
        assert release_opener.wait(5)
        db = registry.acquire(path)
        opened.append(db)
        return db

    def checked_close(db):
        return registry.release_or_close_checked(db).verified

    worker = threading.Thread(target=lambda: result.append(cache.get(path, opener)))
    worker.start()
    try:
        assert started.wait(5)
        first = cache.close_all_checked(checked_close)
        assert first.verified is False
        assert first.pending > 0

        # This is the compatibility caller that currently replaces the checked callback/mode.
        cache.close_all(lambda _db: None)
        release_opener.set()
        worker.join(5)
        assert not worker.is_alive()
        assert result == [None]
        db = opened[0]
        assert db._conn is not None

        second = cache.close_all_checked(checked_close)
        assert second.verified is True
        assert db._conn is None
    finally:
        release_opener.set()
        worker.join(5)
        registry.close_all()
        for db in opened:
            if db._conn is not None:
                db._shared_registry_owned = False
                db.close()


def test_checked_registry_receipt_waits_for_real_read_open_admitted_before_close(tmp_path):
    """A read opener that passed admission before close must block checked settlement."""
    path = tmp_path / "read-race.db"
    db = registry.acquire(path)
    db.create_session("read-race", "cli", model="synthetic-model")
    started = threading.Event()
    release_open = threading.Event()
    outcomes = []
    original_connect = db._connect_read_only

    def blocked_connect(self, *, timeout):
        started.set()
        assert release_open.wait(5)
        conn = original_connect(timeout=timeout)
        outcomes.append(conn)
        return conn

    db._connect_read_only = types.MethodType(blocked_connect, db)

    def reader():
        try:
            with db._read_ctx() as conn:
                conn.execute("SELECT 1").fetchone()
        except BaseException as exc:  # keep the causal result visible to the assertion below
            outcomes.append(exc)

    worker = threading.Thread(target=reader)
    worker.start()
    try:
        assert started.wait(5)
        first = registry.close_all_checked()
        # The opener was admitted before close and must make the first receipt non-terminal.
        assert first.verified is False

        release_open.set()
        worker.join(5)
        assert not worker.is_alive()
        second = registry.close_all_checked()
        assert second.verified is True
    finally:
        release_open.set()
        worker.join(5)
        registry.close_all()

