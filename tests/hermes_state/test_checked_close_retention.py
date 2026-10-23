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


def test_checked_cache_upgrades_legacy_inflight_open(tmp_path):
    """A checked sweep must take custody of an opener first admitted by legacy close."""
    cache = RecoverableHandleCache()
    path = tmp_path / "legacy-first.db"
    started, release = threading.Event(), threading.Event()
    opened = []

    class Handle:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    def opener():
        started.set()
        assert release.wait(5)
        handle = Handle()
        opened.append(handle)
        return handle

    worker = threading.Thread(target=lambda: cache.get(path, opener))
    worker.start()
    try:
        assert started.wait(5)
        cache.close_all(lambda _handle: None)
        first = cache.close_all_checked(lambda handle: (handle.close() or True))
        assert first.verified is False
        assert first.pending == 1

        release.set()
        worker.join(5)
        assert not worker.is_alive()
        assert len(opened) == 1
        assert cache._failed_handles

        second = cache.close_all_checked(lambda handle: (handle.close() or True))
        assert second.verified is True
        assert opened[0].closed is True
    finally:
        release.set()
        worker.join(5)


def test_checked_cache_does_not_verify_during_stale_handle_retention(tmp_path, monkeypatch):
    """The receipt stays pending while a stale native handle is being retained."""
    cache = RecoverableHandleCache()
    path = tmp_path / "settling.db"
    started, release_open = threading.Event(), threading.Event()
    retain_entered, release_retain = threading.Event(), threading.Event()
    opened = []

    def opener():
        started.set()
        assert release_open.wait(5)
        handle = registry.acquire(path)
        opened.append(handle)
        return handle

    original_retain = cache._retain_failed_handle

    def blocking_retain(retain_path, handle):
        retain_entered.set()
        assert release_retain.wait(5)
        original_retain(retain_path, handle)

    monkeypatch.setattr(cache, "_retain_failed_handle", blocking_retain)
    close_checked = lambda handle: registry.release_or_close_checked(handle).verified
    worker = threading.Thread(target=lambda: cache.get(path, opener))
    worker.start()
    try:
        assert started.wait(5)
        first = cache.close_all_checked(close_checked)
        assert first.verified is False
        assert first.pending == 1

        release_open.set()
        assert retain_entered.wait(5)
        second = cache.close_all_checked(close_checked)
        assert second.verified is False
        assert second.pending == 1

        release_retain.set()
        worker.join(5)
        assert not worker.is_alive()
        third = cache.close_all_checked(close_checked)
        assert third.verified is True
        assert opened[0]._conn is None
    finally:
        release_open.set()
        release_retain.set()
        worker.join(5)
        registry.close_all()


def test_failed_generation_stays_in_process_holder_census(tmp_path):
    """A retained native handle remains visible to maintenance's same-process guard."""
    from hermes_state_holders import in_process_state_db_holders

    path = tmp_path / "failed-census.db"
    db = registry.acquire(path)
    native = db._conn
    original_close = db._close_connection_quietly

    def fail_close(_conn):
        db._last_native_close_succeeded = False
        return False

    db._close_connection_quietly = fail_close
    try:
        first = registry.close_all_checked()
        assert first.verified is False
        assert db._conn is native
        assert registry.other_generations_for_path(path)
        assert in_process_state_db_holders(path)
    finally:
        db._close_connection_quietly = original_close
        registry.close_all_checked()


def test_checked_cache_does_not_verify_during_checked_callback(tmp_path):
    """An overlapping checked sweep remains pending while the first native callback runs."""
    cache = RecoverableHandleCache()
    path = tmp_path / "overlapping-checked.db"
    db = cache.get(path, lambda: registry.acquire(path))
    entered, release = threading.Event(), threading.Event()
    receipts = []

    def blocked_close(handle):
        entered.set()
        assert release.wait(5)
        return registry.release_or_close_checked(handle).verified

    worker = threading.Thread(
        target=lambda: receipts.append(cache.close_all_checked(blocked_close)),
    )
    worker.start()
    try:
        assert entered.wait(5)
        assert db._conn is not None
        second = cache.close_all_checked(
            lambda handle: registry.release_or_close_checked(handle).verified,
        )
        assert second.verified is False
        assert second.pending >= 1
    finally:
        release.set()
        worker.join(5)
        registry.close_all_checked()
    assert not worker.is_alive()
    assert receipts[0].verified is True


def test_checked_cache_upgrades_overlapping_legacy_callback(tmp_path):
    """A checked sweep takes custody of a legacy callback already closing a cached handle."""
    cache = RecoverableHandleCache()
    path = tmp_path / "overlapping-legacy.db"
    db = cache.get(path, lambda: registry.acquire(path))
    entered, release = threading.Event(), threading.Event()

    def blocked_legacy(_handle):
        entered.set()
        assert release.wait(5)

    worker = threading.Thread(target=lambda: cache.close_all(blocked_legacy))
    worker.start()
    try:
        assert entered.wait(5)
        assert db._conn is not None
        second = cache.close_all_checked(
            lambda handle: registry.release_or_close_checked(handle).verified,
        )
        assert second.verified is False
        assert second.pending >= 1

        release.set()
        worker.join(5)
        assert not worker.is_alive()
        assert db._conn is not None
        assert cache._failed_handles

        third = cache.close_all_checked(
            lambda handle: registry.release_or_close_checked(handle).verified,
        )
        assert third.verified is True
        assert db._conn is None
    finally:
        release.set()
        worker.join(5)
        registry.close_all()


def test_second_checked_close_cannot_miss_earlier_callback_failure(tmp_path):
    cache = RecoverableHandleCache()
    first_db = cache.get(tmp_path / 'first.db', lambda: registry.acquire(tmp_path / 'first.db'))
    first_entered, second_entered = threading.Event(), threading.Event()
    first_release, second_release = threading.Event(), threading.Event()
    first_receipts, second_receipts = [], []
    def fail_first(handle):
        assert handle is first_db
        first_entered.set()
        assert first_release.wait(5)
        return False
    def close_second(handle):
        second_entered.set()
        assert second_release.wait(5)
        return registry.release_or_close_checked(handle).verified
    first = threading.Thread(target=lambda: first_receipts.append(cache.close_all_checked(fail_first)))
    second = threading.Thread(target=lambda: second_receipts.append(cache.close_all_checked(close_second)))
    first.start()
    try:
        assert first_entered.wait(5)
        cache.get(tmp_path / 'second.db', lambda: registry.acquire(tmp_path / 'second.db'))
        second.start()
        assert second_entered.wait(5)
        first_release.set()
        first.join(5)
        assert not first.is_alive()
        assert first_receipts[0].verified is False
        assert first_db._conn is not None
        second_release.set()
        second.join(5)
        assert not second.is_alive()
        assert second_receipts[0].verified is False, 'settled failed callback disappeared before receipt publication'
    finally:
        first_release.set()
        second_release.set()
        first.join(5)
        if second.ident is not None:
            second.join(5)
        registry.close_all_checked()


def test_failed_old_close_never_overwrites_newer_same_path_handle(tmp_path):
    from hermes_state import SessionDB

    cache = RecoverableHandleCache()
    path = tmp_path / 'same.db'
    old_db = cache.get(path, lambda: SessionDB(path))
    entered, release = threading.Event(), threading.Event()
    receipts = []
    def fail_old(handle):
        assert handle is old_db
        entered.set()
        assert release.wait(5)
        return False
    worker = threading.Thread(target=lambda: receipts.append(cache.close_all_checked(fail_old)))
    worker.start()
    new_db = None
    try:
        assert entered.wait(5)
        new_db = cache.get(path, lambda: SessionDB(path))
        assert new_db is not old_db
        release.set()
        worker.join(5)
        assert not worker.is_alive()
        assert receipts[0].verified is False
        assert cache.handles[path] is new_db, 'failed old close overwrote the newer native handle'
        assert any(handle is old_db for _, handle in cache._failed_handles.values())
    finally:
        release.set()
        worker.join(5)
        old_db.close()
        if new_db is not None:
            new_db.close()


def test_legacy_sweep_cannot_drop_a_failed_checked_handle(tmp_path):
    path = tmp_path / 'retained.db'
    cache = RecoverableHandleCache()
    db = cache.get(path, lambda: registry.acquire(path))
    try:
        assert cache.close_all_checked(lambda _handle: False).verified is False
        assert db._conn is not None
        cache.close_all(lambda _handle: None)
        assert db._conn is not None
        second = cache.close_all_checked(lambda handle: registry.release_or_close_checked(handle).verified)
        assert second.attempted == 1, 'legacy callback dropped a native handle requiring checked retry'
        assert second.verified is True
        assert db._conn is None
    finally:
        registry.close_all_checked()


def test_legacy_sessiondb_close_consumes_source_owned_proof(tmp_path):
    """A real SessionDB can leave the compatibility cache once its source proof is true."""
    path = tmp_path / 'compat-closed.db'
    cache = RecoverableHandleCache()
    db = cache.get(path, lambda: registry.acquire(path))
    try:
        cache.close_all(registry.release_or_close)
        assert db._conn is None

        receipt = cache.close_all_checked(
            lambda handle: registry.release_or_close_checked(handle).verified,
        )
        assert receipt.verified is True
        assert receipt.attempted == 0
    finally:
        registry.close_all_checked()


@pytest.mark.platforms('posix')
def test_failed_retired_close_keeps_current_generation_registered(tmp_path, monkeypatch):
    from tests.hermes_state.test_shared_session_db_registry import _replace_file_preserving_schema

    path = tmp_path / 'state.db'
    old = registry.acquire(path)
    native = old._conn
    new = None
    try:
        _replace_file_preserving_schema(path, path)
        new = registry.acquire(path)
        assert new is not old
        with monkeypatch.context() as patched:
            patched.setattr(old, 'close', lambda: None)
            assert registry.release_checked(old).verified is False
        assert old._conn is native
        assert registry._failed_generations[id(old)].db is old
        assert registry._generations.get(path) is not None, 'failed old close removed the live replacement'
        assert registry._generations[path].db is new
        assert new._conn is not None
    finally:
        if new is not None:
            new.close()
        old._shared_registry_owned = False
        native.close()
        old._connection_pinned = False
        registry.close_all_checked()


def test_empty_typed_legacy_receipt_cannot_close_an_admitted_bare_handle(tmp_path):
    from hermes_state import SessionDB

    path = tmp_path / 'bare.db'
    db = SessionDB(path)
    cache = RecoverableHandleCache(handles={path: db})
    try:
        cache.close_all(registry.release_checked)
        assert db._conn is not None
        second = cache.close_all_checked(lambda handle: registry.release_or_close_checked(handle).verified)
        assert second.attempted == 1, 'empty typed receipt hid an actually open admitted native handle'
        assert second.verified is True
        assert db._conn is None
    finally:
        db.close()
