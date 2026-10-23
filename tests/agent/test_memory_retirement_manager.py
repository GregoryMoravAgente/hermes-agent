"""Core manager custody for the optional provider-owned retirement contract."""

from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from agent.memory_manager import MemoryManager
from agent.memory_provider import MemoryProvider
from hermes_constants import reset_hermes_home_override, set_hermes_home_override


class SQLiteRetirementProvider(MemoryProvider):
    """A real manager-loaded provider with a temporary SQLite-backed store."""

    def __init__(self, db_path: Path, *, name: str = "sqlite-retire", home_id: str = "home-a") -> None:
        self.db_path = db_path
        self._name = name
        self.home_id = home_id
        self.incarnation = f"inc-{name}"
        self.connection: sqlite3.Connection | None = None
        self.retire_calls: list[tuple[int, dict[str, str]]] = []
        self.retire_mode = "released"
        self.sync_started = threading.Event()
        self.sync_done = threading.Event()
        self.sync_release = threading.Event()
        self.block_sync = False
        self.fail_sync = False
        self.prefetch_started = threading.Event()
        self.prefetch_release = threading.Event()
        self.block_prefetch = False
        self.shutdown_calls = 0

    @property
    def name(self) -> str:
        return self._name

    def is_available(self) -> bool:
        return True

    def initialize(self, session_id: str, **kwargs: Any) -> None:
        self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
        self.connection.execute("CREATE TABLE IF NOT EXISTS events (kind TEXT NOT NULL)")
        self.connection.execute("INSERT INTO events VALUES (?)", (f"init:{session_id}",))
        self.connection.commit()

    def get_tool_schemas(self) -> list[dict[str, Any]]:
        return []

    def sync_turn(self, user_content: str, assistant_content: str, **kwargs: Any) -> None:
        try:
            if self.block_sync:
                self.sync_started.set()
                self.sync_release.wait(timeout=5)
            if self.fail_sync:
                raise RuntimeError("synthetic accepted write failure")
            if self.connection is not None:
                self.connection.execute("INSERT INTO events VALUES (?)", (f"turn:{user_content}",))
                self.connection.commit()
        finally:
            self.sync_done.set()

    def prefetch(self, query: str, *, session_id: str = "") -> str:
        if self.block_prefetch:
            self.prefetch_started.set()
            self.prefetch_release.wait(timeout=5)
        return "prefetched"

    def retirement_receipt(self) -> dict[str, str]:
        return {"provider": self.name, "home_id": self.home_id, "incarnation": self.incarnation}

    def retire(self, receipt: dict[str, str]) -> dict[str, Any]:
        self.retire_calls.append((id(self), dict(receipt)))
        if receipt != self.retirement_receipt():
            return self._result("unknown", "stale_receipt", active_store_count=1)
        if self.retire_mode == "raise":
            raise RuntimeError("synthetic provider failure")
        if self.retire_mode != "released":
            return self._result("unknown", self.retire_mode, active_store_count=1,
                                close_failure_count=1 if self.retire_mode == "close_failed" else 0)
        if self.connection is None:
            return self._result("unknown", "not_initialized", active_store_count=1)
        self.connection.execute("INSERT INTO events VALUES ('retired')")
        self.connection.commit()
        self.connection.close()
        self.connection = None
        return self._result("released", "released")

    def shutdown(self) -> None:
        self.shutdown_calls += 1
        if self.connection is not None:
            self.connection.close()
            self.connection = None

    def _result(self, status: str, reason: str, *, active_store_count: int = 0,
                close_failure_count: int = 0) -> dict[str, Any]:
        return {
            "status": status,
            "provider": self.name,
            "home_id": self.home_id,
            "incarnation": self.incarnation,
            "active_store_count": active_store_count,
            "pending_write_count": 0,
            "pending_flush_count": 0,
            "close_failure_count": close_failure_count,
            "reason": reason,
        }


class LegacyProvider(SQLiteRetirementProvider):
    """A provider which has only the legacy base-class retirement defaults."""

    retirement_receipt = MemoryProvider.retirement_receipt
    retire = MemoryProvider.retire


@contextmanager
def _profile_scope(home: Path):
    token = set_hermes_home_override(str(home))
    try:
        yield
    finally:
        reset_hermes_home_override(token)


def _manager(provider: MemoryProvider, home: Path) -> MemoryManager:
    manager = MemoryManager()
    with _profile_scope(home):
        manager.add_provider(provider)
        manager.initialize_all(session_id="session", hermes_home=str(home), platform="test")
    return manager


def test_hard_retirement_uses_loaded_provider_and_releases_sqlite_store(tmp_path):
    home = tmp_path / "home-a"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    manager = _manager(provider, home)

    with _profile_scope(home):
        report = manager.retire_hard()

    assert report.released
    assert report.reason == "released"
    assert provider.connection is None
    assert provider.retire_calls == [
        (id(provider), {
            "provider": "sqlite-retire",
            "home_id": "home-a",
            "incarnation": "inc-sqlite-retire",
        }),
    ]
    assert manager.providers == []


def test_unknown_close_failure_keeps_exact_provider_and_receipt_for_retry(tmp_path, caplog):
    home = tmp_path / "home-a"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    provider.retire_mode = "close_failed"
    manager = _manager(provider, home)

    with _profile_scope(home), caplog.at_level("WARNING", logger="agent.memory_manager"):
        first = manager.retire_hard()
    assert first.status == "unknown"
    assert first.reason == "close_failed"
    assert provider.connection is not None
    assert "hard retirement remains unknown: close_failed" in caplog.text
    first_call = provider.retire_calls[-1]

    provider.retire_mode = "released"
    with _profile_scope(home):
        second = manager.retire_hard()

    assert second.released
    assert provider.retire_calls[-1] == first_call
    assert len(provider.retire_calls) == 2
    assert manager.providers == []


def test_provider_exception_is_unknown_and_normal_shutdown_is_blocked(tmp_path, caplog):
    home = tmp_path / "home-a"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    provider.retire_mode = "raise"
    manager = _manager(provider, home)

    with _profile_scope(home), caplog.at_level("WARNING", logger="agent.memory_manager"):
        report = manager.retire_hard()

    assert report.status == "unknown"
    assert report.reason == "provider_call_failed"
    assert "hard retirement failed" in caplog.text
    manager.shutdown_all()
    assert provider.shutdown_calls == 0
    assert provider.connection is not None


def test_scope_refusal_happens_before_snapshot_or_provider_call(tmp_path):
    owner = tmp_path / "owner"
    foreign = tmp_path / "foreign"
    owner.mkdir()
    foreign.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    manager = _manager(provider, owner)

    with _profile_scope(foreign):
        report = manager.retire_hard()

    assert report.reason == "owner_scope_refused"
    assert provider.retire_calls == []
    assert provider.connection is not None
    assert manager._hard_retirement_state is None
    assert manager._shutting_down is False


def test_missing_legacy_capability_refuses_before_snapshot(tmp_path):
    home = tmp_path / "home-a"
    home.mkdir()
    provider = LegacyProvider(tmp_path / "memory.db", name="legacy")
    manager = _manager(provider, home)

    with _profile_scope(home):
        report = manager.retire_hard()

    assert report.reason == "missing_capability"
    assert provider.retire_calls == []
    assert provider.connection is not None
    assert manager._hard_retirement_state is None
    assert manager._shutting_down is False
    provider.shutdown()


def test_successor_binding_refusal_does_not_call_replacement(tmp_path, caplog):
    home = tmp_path / "home-a"
    home.mkdir()
    original = SQLiteRetirementProvider(tmp_path / "original.db")
    replacement = SQLiteRetirementProvider(tmp_path / "replacement.db", name="replacement")
    manager = _manager(original, home)
    original.retire_mode = "stale_receipt"

    with _profile_scope(home), caplog.at_level("WARNING", logger="agent.memory_manager"):
        first = manager.retire_hard()
    assert first.reason == "stale_receipt"
    assert "stale_receipt" in caplog.text

    manager._providers = [replacement]
    with _profile_scope(home), caplog.at_level("WARNING", logger="agent.memory_manager"):
        second = manager.retire_hard()

    assert second.reason == "provider_binding_changed"
    assert "provider_binding_changed" in caplog.text
    assert len(original.retire_calls) == 1
    assert replacement.retire_calls == []
    original.shutdown()


def test_sibling_manager_retirement_does_not_touch_other_sqlite_store(tmp_path):
    home_a = tmp_path / "home-a"
    home_b = tmp_path / "home-b"
    home_a.mkdir()
    home_b.mkdir()
    provider_a = SQLiteRetirementProvider(tmp_path / "a.db", home_id="home-a")
    provider_b = SQLiteRetirementProvider(tmp_path / "b.db", name="sqlite-retire-b", home_id="home-b")
    manager_a = _manager(provider_a, home_a)
    manager_b = _manager(provider_b, home_b)

    with _profile_scope(home_a):
        assert manager_a.retire_hard().released
    assert provider_a.connection is None
    assert provider_b.connection is not None
    provider_b.connection.execute("INSERT INTO events VALUES ('sibling-still-open')")
    provider_b.connection.commit()

    with _profile_scope(home_b):
        assert manager_b.retire_hard().released


def test_active_background_work_returns_unknown_then_retries_same_provider(tmp_path, monkeypatch, caplog):
    import agent.memory_manager as memory_manager_module

    home = tmp_path / "home-a"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    provider.block_sync = True
    manager = _manager(provider, home)
    with _profile_scope(home):
        manager.sync_all("active", "response", session_id="session")
    assert provider.sync_started.wait(timeout=2)
    monkeypatch.setattr(memory_manager_module, "_SYNC_DRAIN_TIMEOUT_S", 0.05)

    with _profile_scope(home), caplog.at_level("WARNING", logger="agent.memory_manager"):
        first = manager.retire_hard()
    assert first.reason == "drain_timeout"
    assert "drain_timeout" in caplog.text
    assert provider.retire_calls == []

    provider.sync_release.set()
    assert provider.sync_done.wait(timeout=2)
    provider.block_sync = False
    with _profile_scope(home):
        second = manager.retire_hard()

    assert second.released
    assert len(provider.retire_calls) == 1
    assert provider.retire_calls[0][1] == {
        "provider": "sqlite-retire", "home_id": "home-a", "incarnation": "inc-sqlite-retire",
    }


def test_prefetch_thread_is_retirement_custody_until_it_returns(tmp_path, monkeypatch):
    import agent.memory_manager as memory_manager_module

    home = tmp_path / "home-a"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    provider.block_prefetch = True
    manager = _manager(provider, home)

    def run_prefetch() -> None:
        with _profile_scope(home):
            manager.prefetch_all("query", session_id="session")

    prefetch_thread = threading.Thread(
        target=run_prefetch,
    )
    prefetch_thread.start()
    assert provider.prefetch_started.wait(timeout=2)
    monkeypatch.setattr(memory_manager_module, "_SYNC_DRAIN_TIMEOUT_S", 0.05)

    with _profile_scope(home):
        first = manager.retire_hard()
    assert first.reason == "drain_timeout"
    assert provider.retire_calls == []

    provider.prefetch_release.set()
    prefetch_thread.join(timeout=2)
    assert not prefetch_thread.is_alive()
    with _profile_scope(home):
        assert manager.retire_hard().released
    assert len(provider.retire_calls) == 1


def test_accepted_write_failure_is_visible_before_provider_release(tmp_path):
    home = tmp_path / "home-a"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    provider.fail_sync = True
    manager = _manager(provider, home)
    with _profile_scope(home):
        manager.sync_all("failed", "response", session_id="session")
    assert manager.flush_pending(timeout=2)

    with _profile_scope(home):
        first = manager.retire_hard()
    assert first.status == "unknown"
    assert first.reason == "accepted_write_failed"
    assert provider.retire_calls == []
    assert provider.connection is not None

    provider.fail_sync = False
    with _profile_scope(home):
        second = manager.retire_hard()
    assert second.released
    assert len(provider.retire_calls) == 1


def test_partial_multi_provider_release_retries_only_unresolved_provider(tmp_path):
    home = tmp_path / "home-a"
    home.mkdir()
    first = SQLiteRetirementProvider(tmp_path / "first.db", name="first")
    second = SQLiteRetirementProvider(tmp_path / "second.db", name="second")
    first.retire_mode = "close_failed"
    manager = _manager(first, home)
    with _profile_scope(home):
        second.initialize(session_id="session", hermes_home=str(home))
    # The production registry permits one external provider; this exercises the manager's
    # receipt/attempt bookkeeping with the provider tuple it already supports internally.
    manager._providers.append(second)

    with _profile_scope(home):
        first_report = manager.retire_hard()
    assert first_report.status == "unknown"
    assert first_report.attempts[0].reason == "close_failed"
    assert first_report.attempts[1].status == "released"
    assert first.connection is not None
    assert second.connection is None

    first.retire_mode = "released"
    with _profile_scope(home):
        second_report = manager.retire_hard()
    assert second_report.released
    assert len(first.retire_calls) == 2
    assert len(second.retire_calls) == 1
    assert manager.providers == []


def test_retirement_callbacks_can_reenter_manager_lock_probe(tmp_path):
    home = tmp_path / "home-a"
    home.mkdir()
    manager_ref: dict[str, MemoryManager] = {}

    class LockProbeProvider(SQLiteRetirementProvider):
        def retirement_receipt(self) -> dict[str, str]:
            manager = manager_ref["manager"]
            acquired = manager._sync_executor_lock.acquire(timeout=1)
            if not acquired:
                raise RuntimeError("manager lock held during provider callback")
            manager._sync_executor_lock.release()
            return super().retirement_receipt()

    provider = LockProbeProvider(tmp_path / "memory.db", name="lock-probe")
    manager = _manager(provider, home)
    manager_ref["manager"] = manager
    with _profile_scope(home):
        report = manager.retire_hard()
    assert report.released


def test_agent_hard_operation_is_explicit_and_surfaces_report():
    from unittest.mock import MagicMock

    from run_agent import AIAgent

    report = MagicMock(released=False)
    manager = MagicMock()
    manager.retire_hard.return_value = report
    agent = object.__new__(AIAgent)
    agent._memory_manager = manager

    assert agent.retire_memory_provider() is report
    manager.retire_hard.assert_called_once_with()
    assert not hasattr(agent, "_memory_provider_shutdown")


def test_hard_retirement_preserves_accepted_queued_writes_after_timeout(tmp_path, monkeypatch):
    import agent.memory_manager as memory_manager_module

    home = tmp_path / "home-a"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    provider.block_sync = True
    manager = _manager(provider, home)
    monkeypatch.setattr(memory_manager_module, "_SYNC_DRAIN_TIMEOUT_S", 0.05)
    with _profile_scope(home):
        manager.sync_all("first", "response", session_id="session")
        assert provider.sync_started.wait(timeout=2)
        manager.sync_all("queued", "response", session_id="session")
    with manager._sync_executor_lock:
        accepted = tuple(manager._background_futures)
    try:
        assert len(accepted) == 2
        with _profile_scope(home):
            first = manager.retire_hard()
        assert first.reason == "drain_timeout"
        assert provider.retire_calls == []
        assert not any(future.cancelled() for future in accepted)
    finally:
        provider.sync_release.set()
        for future in accepted:
            if not future.cancelled():
                future.result(timeout=2)
    with _profile_scope(home):
        assert manager.retire_hard().released
    with sqlite3.connect(provider.db_path) as connection:
        events = [row[0] for row in connection.execute("SELECT kind FROM events ORDER BY rowid")]
    assert events == ["init:session", "turn:first", "turn:queued", "retired"]


@pytest.mark.parametrize("session_end", [False, True])
def test_existing_teardown_claim_refuses_hard_retirement_before_snapshot(tmp_path, session_end):
    home = tmp_path / "home-a"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    manager = _manager(provider, home)
    try:
        assert manager.claim_teardown(session_end=session_end)
        with _profile_scope(home):
            report = manager.retire_hard()
        assert report.status == "unknown"
        assert report.reason == "teardown_in_progress"
        assert manager._hard_retirement_state is None
        assert provider.retire_calls == []
    finally:
        provider.shutdown()


@pytest.mark.parametrize("operation", ["initialize", "register"])
def test_unknown_retirement_keeps_provider_setup_closed(tmp_path, operation):
    home = tmp_path / "home-a"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    provider.retire_mode = "close_failed"
    manager = _manager(provider, home)
    with _profile_scope(home):
        assert manager.retire_hard().reason == "close_failed"
        retained_connection = provider.connection
        retained_state = manager._hard_retirement_state
        try:
            with pytest.raises(RuntimeError, match="retirement owns provider setup"):
                if operation == "initialize":
                    manager.initialize_all(session_id="successor", hermes_home=str(home))
                else:
                    manager.add_provider(SQLiteRetirementProvider(tmp_path / "new.db", name="builtin"))
            assert provider.connection is retained_connection
            assert manager.providers == [provider]
            assert manager._hard_retirement_state is retained_state
        finally:
            provider.retire_mode = "released"
            assert manager.retire_hard().released


def test_empty_manager_cannot_release_while_provider_registration_is_inflight(tmp_path, monkeypatch):
    home = tmp_path / "home-a"
    home.mkdir()
    manager = MemoryManager()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    entered, release = threading.Event(), threading.Event()

    def held_schemas():
        entered.set()
        assert release.wait(timeout=5)
        return []

    monkeypatch.setattr(provider, "get_tool_schemas", held_schemas)
    errors = []

    def register():
        try:
            with _profile_scope(home):
                manager.add_provider(provider)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=register)
    thread.start()
    try:
        assert entered.wait(timeout=2)
        with _profile_scope(home):
            report = manager.retire_hard()
        assert report.status == "unknown"
        assert report.reason == "provider_setup_in_progress"
        assert manager._hard_retirement_state is None
    finally:
        release.set()
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert errors == []
    with _profile_scope(home):
        manager.initialize_all(session_id="session", hermes_home=str(home))
        assert manager.retire_hard().released


def test_agent_hard_operation_marks_shutdown_only_after_release():
    from unittest.mock import MagicMock

    from agent.memory_retirement import MemoryRetirementReport
    from run_agent import AIAgent

    manager = MagicMock()
    manager.retire_hard.return_value = MemoryRetirementReport(status="released", reason="released")
    agent = object.__new__(AIAgent)
    agent._memory_manager = manager

    assert agent.retire_memory_provider().released
    assert agent._memory_provider_shutdown is True
