"""Causal controls for the provider-owned reversible retirement contract."""

from __future__ import annotations

import threading
import sqlite3

import pytest

from tests.plugins.test_provider_native_retirement import _RetainClient, _initialized_retaindb
from tests.waiting import wait_until


@pytest.fixture(autouse=True)
def _isolated_provider_environment(monkeypatch):
    monkeypatch.delenv("BRV_API_KEY", raising=False)
    monkeypatch.setenv("RETAINDB_API_KEY", "local-test-key")
    monkeypatch.delenv("RETAINDB_BASE_URL", raising=False)
    monkeypatch.delenv("RETAINDB_PROJECT", raising=False)


def _assert_preparation_shape(result: dict[str, object]) -> None:
    assert set(result) == {"status", "receipt", "lease", "reason"}
    assert set(result["receipt"]) == {"provider", "home_id", "incarnation"}


def _assert_retirement_shape(result: dict[str, object]) -> None:
    assert set(result) == {
        "status", "provider", "home_id", "incarnation",
        "active_store_count", "pending_write_count", "pending_flush_count",
        "close_failure_count", "reason",
    }


def test_byterover_prepare_resume_reopens_same_provider_and_new_write(tmp_path, monkeypatch):
    from plugins.memory import byterover as module

    provider = module.ByteRoverMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path / "brv-home"))
    calls: list[list[str]] = []
    monkeypatch.setattr(
        module, "_run_brv", lambda args, **_kwargs: (calls.append(args) or {"success": True, "output": ""}),
    )
    receipt = provider.retirement_receipt()

    prepared = provider.prepare_retirement(receipt)
    _assert_preparation_shape(prepared)
    lease = prepared["lease"]
    assert prepared["status"] == "prepared"
    assert lease is not None
    assert provider._released is False
    assert provider._retiring is True
    assert provider.retire(receipt)["reason"] == "retirement_in_progress"
    assert provider._query("blocked while prepared")["success"] is False

    assert provider.resume_retirement(lease) is True
    assert provider._retiring is False
    provider.sync_turn("accepted after reversible resume", "ack")
    wait_until(lambda: provider._retirement_active_calls == 0, what="ByteRover resumed write")
    assert any(args[0] == "curate" for args in calls)

    second = provider.prepare_retirement(receipt)
    assert second["status"] == "prepared"
    committed = provider.commit_retirement(second["lease"])
    _assert_retirement_shape(committed)
    assert committed["status"] == "released"
    assert provider._released is True


def test_byterover_unknown_prepare_resume_preserves_live_work(tmp_path, monkeypatch):
    from plugins.memory import byterover as module

    provider = module.ByteRoverMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path / "brv-home"))
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 0.05)

    def blocked_run(args, **_kwargs):
        if args[0] == "curate":
            entered.set()
            assert release.wait(10)
        return {"success": True, "output": ""}

    monkeypatch.setattr(module, "_run_brv", blocked_run)
    provider.sync_turn("accepted before reversible fence", "ack")
    assert entered.wait(10)
    receipt = provider.retirement_receipt()

    pending = provider.prepare_retirement(receipt)
    _assert_preparation_shape(pending)
    assert pending["status"] == "unknown"
    assert pending["reason"] == "pending_flush"
    lease = pending["lease"]
    assert lease is not None
    assert provider._retiring is True
    assert provider.resume_retirement(lease) is True
    assert provider._retiring is False

    release.set()
    wait_until(lambda: provider._retirement_active_calls == 0, what="ByteRover resumed pending work")
    prepared = provider.prepare_retirement(receipt)
    assert prepared["status"] == "prepared"
    committed = provider.commit_retirement(prepared["lease"])
    assert committed["status"] == "released"


def test_retaindb_prepare_resume_reopens_same_queue_and_new_write(tmp_path, monkeypatch):
    import plugins.memory.retaindb as module

    provider, client, _home = _initialized_retaindb(tmp_path, monkeypatch, _RetainClient())
    client.release_ingest.set()
    receipt = provider.retirement_receipt()

    prepared = provider.prepare_retirement(receipt)
    _assert_preparation_shape(prepared)
    lease = prepared["lease"]
    assert prepared["status"] == "prepared"
    assert lease is not None
    queue_obj, client_obj = provider._queue, provider._client
    assert queue_obj is not None and client_obj is not None
    assert queue_obj._connections
    assert provider.resume_retirement(lease) is True
    assert provider._queue is queue_obj and provider._client is client_obj

    provider.sync_turn("accepted after reversible resume", "ack")
    wait_until(lambda: client.ingest_calls >= 1, what="RetainDB resumed write")
    prepared_again = provider.prepare_retirement(receipt)
    assert prepared_again["status"] == "prepared"
    committed = provider.commit_retirement(prepared_again["lease"])
    _assert_retirement_shape(committed)
    assert committed["status"] == "released"
    assert not queue_obj._connections


def test_retaindb_unknown_prepare_resume_preserves_durable_row(tmp_path, monkeypatch):
    import plugins.memory.retaindb as module

    client = _RetainClient()
    provider, client, _home = _initialized_retaindb(tmp_path, monkeypatch, client)
    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 0.05)
    provider.sync_turn("accepted durable row before fence", "ack")
    assert client.ingest_started.wait(10)
    receipt = provider.retirement_receipt()

    pending = provider.prepare_retirement(receipt)
    _assert_preparation_shape(pending)
    assert pending["status"] == "unknown"
    assert pending["reason"] == "pending_catch_up"
    lease = pending["lease"]
    assert lease is not None
    assert provider.resume_retirement(lease) is True
    assert provider._retiring is False

    client.release_ingest.set()
    wait_until(lambda: provider._queue is not None and provider._queue._pending_count() == 0, what="RetainDB resumed durable row")
    prepared = provider.prepare_retirement(receipt)
    assert prepared["status"] == "prepared"
    committed = provider.commit_retirement(prepared["lease"])
    assert committed["status"] == "released"


def test_retaindb_resume_replays_failed_row_before_new_write(tmp_path, monkeypatch):
    import plugins.memory.retaindb as module

    class _FailOnceClient(_RetainClient):
        def __init__(self):
            super().__init__()
            self.fail_next = True
            self.failed_calls = 0

        def ingest_session(self, *_args, **_kwargs):
            self.ingest_calls += 1
            self.ingest_started.set()
            if self.fail_next:
                self.fail_next = False
                self.failed_calls += 1
                if not self.release_ingest.wait(10):
                    raise RuntimeError("synthetic ingest barrier timed out")
                raise RuntimeError("synthetic ingest failure after resume fence")
            return {"status": "ok"}

    client = _FailOnceClient()
    provider, client, _home = _initialized_retaindb(tmp_path, monkeypatch, client)
    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 0.05)
    provider.sync_turn("accepted row whose first flush fails", "ack")
    assert client.ingest_started.wait(10)
    queue_obj = provider._queue
    assert queue_obj is not None

    receipt = provider.retirement_receipt()
    pending = provider.prepare_retirement(receipt)
    assert pending["status"] == "unknown"
    assert pending["reason"] == "pending_catch_up"
    lease = pending["lease"]
    assert lease is not None
    # Resume while the original native ingest is still active. The exact item must remain the
    # sole in-flight owner until it fails, then be replayed before another accepted write.
    assert provider.resume_retirement(lease) is True

    client.release_ingest.set()
    wait_until(lambda: queue_obj._pending_count() == 0, what="failed RetainDB row replay")
    assert client.failed_calls == 1
    assert client.ingest_calls == 2

    provider.sync_turn("new write after failed row replay", "ack")
    wait_until(lambda: queue_obj._pending_count() == 0, what="RetainDB new write after replay")
    assert client.ingest_calls == 3


def test_retaindb_resume_replays_already_failed_row_before_new_admission(tmp_path, monkeypatch):
    """A failed durable row remains first even when the original flush already returned."""
    import plugins.memory.retaindb as module

    class _FailThenBlockReplayClient(_RetainClient):
        def __init__(self):
            super().__init__()
            self.attempts: list[str] = []
            self.replay_started = threading.Event()
            self.allow_replay = threading.Event()

        def ingest_session(self, _user_id, _session_id, messages, **_kwargs):
            content = messages[0]["content"]
            self.attempts.append(content)
            self.ingest_calls += 1
            self.ingest_started.set()
            if len(self.attempts) == 1:
                raise RuntimeError("synthetic first ingest failure")
            if len(self.attempts) == 2:
                self.replay_started.set()
                assert self.allow_replay.wait(10)
            return {"status": "ok"}

    client = _FailThenBlockReplayClient()
    provider, client, _home = _initialized_retaindb(tmp_path, monkeypatch, client)
    monkeypatch.setattr(module.time, "sleep", lambda _seconds: None)
    provider.sync_turn("accepted row before failed retry", "ack")
    assert client.ingest_started.wait(10)
    queue_obj = provider._queue
    assert queue_obj is not None
    wait_until(
        lambda: queue_obj._active_flushes == 0 and queue_obj._pending_count() == 1,
        what="failed durable row retained before reversible fence",
    )

    monkeypatch.setattr(module, "_RETIREMENT_WAIT_S", 0.05)
    receipt = provider.retirement_receipt()
    pending = provider.prepare_retirement(receipt)
    assert pending["status"] == "unknown"
    assert pending["reason"] == "pending_catch_up"
    lease = pending["lease"]
    assert lease is not None
    assert provider.resume_retirement(lease) is True

    provider.sync_turn("accepted row after reversible resume", "ack")
    assert client.replay_started.wait(10)
    assert client.attempts == [
        "accepted row before failed retry",
        "accepted row before failed retry",
    ]
    client.allow_replay.set()
    wait_until(lambda: len(client.attempts) == 3, what="new row after failed-row replay")
    wait_until(lambda: queue_obj._pending_count() == 0, what="all durable rows replayed")
    assert client.attempts == [
        "accepted row before failed retry",
        "accepted row before failed retry",
        "accepted row after reversible resume",
    ]


def test_retaindb_commit_close_failure_retries_exact_prepared_lease(tmp_path, monkeypatch):
    import plugins.memory.retaindb as module

    provider, _client, _home = _initialized_retaindb(tmp_path, monkeypatch)
    receipt = provider.retirement_receipt()
    prepared = provider.prepare_retirement(receipt)
    assert prepared["status"] == "prepared"
    lease = prepared["lease"]
    queue_obj = provider._queue
    assert lease is not None and queue_obj is not None
    real_close = queue_obj._close
    monkeypatch.setattr(queue_obj, "_close", lambda *args, **kwargs: 1)

    first = provider.commit_retirement(lease)
    _assert_retirement_shape(first)
    assert first["status"] == "unknown"
    assert first["reason"] == "close_failed"
    assert provider._queue is queue_obj and provider._client is not None

    monkeypatch.setattr(queue_obj, "_close", real_close)
    second = provider.commit_retirement(lease)
    assert second["status"] == "released"
    assert provider._queue is queue_obj
    assert not queue_obj._connections


def test_byterover_receipt_cannot_bypass_failed_exact_commit(tmp_path, monkeypatch):
    from plugins.memory import byterover as module

    provider = module.ByteRoverMemoryProvider()
    provider.initialize("session-1", hermes_home=str(tmp_path / "brv-home"))
    monkeypatch.setattr(module, "_run_brv", lambda _args, **_kwargs: {"success": True, "output": ""})
    receipt = provider.retirement_receipt()
    prepared = provider.prepare_retirement(receipt)
    lease = prepared["lease"]
    assert prepared["status"] == "prepared" and lease is not None

    real_join = provider._join_retirement_threads
    attempts = 0

    def fail_once(timeout):
        nonlocal attempts
        attempts += 1
        return False if attempts == 1 else real_join(timeout)

    monkeypatch.setattr(provider, "_join_retirement_threads", fail_once)
    first = provider.commit_retirement(lease)
    assert first["status"] == "unknown" and first["reason"] == "pending_flush"
    bypass = provider.retire(receipt)
    assert bypass["status"] == "unknown" and bypass["reason"] == "retirement_in_progress"
    assert provider._released is False
    assert provider.commit_retirement(lease)["status"] == "released"


def test_retaindb_receipt_cannot_bypass_failed_exact_commit(tmp_path, monkeypatch):
    import plugins.memory.retaindb as module

    provider, _client, _home = _initialized_retaindb(tmp_path, monkeypatch)
    receipt = provider.retirement_receipt()
    prepared = provider.prepare_retirement(receipt)
    lease = prepared["lease"]
    queue_obj = provider._queue
    assert prepared["status"] == "prepared" and lease is not None and queue_obj is not None
    native = next(iter(queue_obj._connections))
    real_close = queue_obj._close
    fail_closes = True

    def fail_close(*connections, **kwargs):
        if fail_closes:
            return 1
        return real_close(*connections, **kwargs)

    monkeypatch.setattr(queue_obj, "_close", fail_close)
    first = provider.commit_retirement(lease)
    assert first["status"] == "unknown" and first["reason"] == "close_failed"
    assert native.execute("SELECT 1").fetchone()[0] == 1
    fail_closes = False
    bypass = provider.retire(receipt)
    assert bypass["status"] == "unknown" and bypass["reason"] == "retirement_in_progress"
    assert native.execute("SELECT 1").fetchone()[0] == 1
    assert provider.commit_retirement(lease)["status"] == "released"
    with pytest.raises(sqlite3.ProgrammingError):
        native.execute("SELECT 1")


def test_foreign_and_stale_provider_leases_cannot_commit_or_resume(tmp_path, monkeypatch):
    from plugins.memory import byterover as module

    first = module.ByteRoverMemoryProvider()
    second = module.ByteRoverMemoryProvider()
    first.initialize("session-1", hermes_home=str(tmp_path / "first"))
    second.initialize("session-2", hermes_home=str(tmp_path / "second"))
    monkeypatch.setattr(module, "_run_brv", lambda _args, **_kwargs: {"success": True, "output": ""})
    first_prepared = first.prepare_retirement(first.retirement_receipt())
    second_prepared = second.prepare_retirement(second.retirement_receipt())
    first_lease, second_lease = first_prepared["lease"], second_prepared["lease"]
    assert first_lease is not None and second_lease is not None

    foreign_commit = second.commit_retirement(first_lease)
    assert foreign_commit["status"] == "unknown"
    assert foreign_commit["reason"] == "foreign_receipt"
    assert second.resume_retirement(first_lease) is False
    assert first.resume_retirement(second_lease) is False

    assert first.resume_retirement(first_lease) is True
    stale_commit = first.commit_retirement(first_lease)
    assert stale_commit["status"] == "unknown"
    assert stale_commit["reason"] == "foreign_receipt"
    assert second.commit_retirement(second_lease)["status"] == "released"
