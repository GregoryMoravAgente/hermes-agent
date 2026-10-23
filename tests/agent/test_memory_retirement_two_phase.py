"""Causal tests for reversible manager/provider retirement preparation and commit."""

from __future__ import annotations

from typing import Any

import pytest

from agent.memory_manager import MemoryManager
from tests.agent.test_memory_retirement_manager import SQLiteRetirementProvider, _manager, _profile_scope


class TwoPhaseSQLiteProvider(SQLiteRetirementProvider):
    """A manager-loaded provider with a source-owned reversible lease."""

    def __init__(self, db_path, *, name: str = "two-phase", home_id: str = "home-a") -> None:
        super().__init__(db_path, name=name, home_id=home_id)
        self._preparation_lease: object | None = None
        self._lease_receipts: dict[object, dict[str, str]] = {}
        self.preparation_mode = "prepared"
        self.resume_result = True
        self.committed = False

    def prepare_retirement(self, receipt: dict[str, str]) -> dict[str, Any]:
        if receipt != self.retirement_receipt():
            return self._preparation_result(receipt, "unknown", None, "stale_receipt")
        if self._preparation_lease is None:
            self._preparation_lease = object()
            self._lease_receipts[self._preparation_lease] = dict(receipt)
        if self.preparation_mode != "prepared":
            return self._preparation_result(receipt, "unknown", self._preparation_lease, self.preparation_mode)
        return self._preparation_result(receipt, "prepared", self._preparation_lease, "prepared")

    def resume_retirement(self, lease: object) -> bool:
        if lease is not self._preparation_lease or self.committed or not self.resume_result:
            return False
        self._preparation_lease = None
        self.admissions_open = True
        return True

    def commit_retirement(self, lease: object) -> dict[str, Any]:
        if lease is not self._preparation_lease or self.committed:
            return self._result("unknown", "foreign_receipt", active_store_count=1)
        receipt = self._lease_receipts[lease]
        result = self.retire(receipt)
        if result["status"] == "released":
            self.committed = True
            self._preparation_lease = None
        return result

    def _preparation_result(
        self, receipt: dict[str, str], status: str, lease: object | None, reason: str,
    ) -> dict[str, Any]:
        return {"status": status, "receipt": dict(receipt), "lease": lease, "reason": reason}


class BlockingPreparationProvider(TwoPhaseSQLiteProvider):
    """Provider whose optional preparation callback remains accepted past the manager budget."""

    def __init__(self, db_path, **kwargs: Any) -> None:
        super().__init__(db_path, **kwargs)
        import threading

        self.prepare_started = threading.Event()
        self.prepare_release = threading.Event()
        self.prepare_calls = 0

    def prepare_retirement(self, receipt: dict[str, str]) -> dict[str, Any]:
        self.prepare_calls += 1
        self.prepare_started.set()
        self.prepare_release.wait(timeout=5)
        return super().prepare_retirement(receipt)


def test_prepare_is_reversible_and_keeps_same_loaded_store(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    provider = TwoPhaseSQLiteProvider(tmp_path / "memory.db")
    manager = _manager(provider, home)

    with _profile_scope(home):
        report = manager.prepare_retirement()

    assert report.prepared
    assert report.lease is manager._retirement_preparation_lease
    assert provider.connection is not None
    assert provider.retire_calls == []
    assert manager._shutting_down is False

    assert manager.resume_retirement(report.lease) is True
    assert manager._hard_retirement_state is None
    assert provider.connection is not None
    with _profile_scope(home):
        manager.sync_all("after resume", "response", session_id="session")
        assert manager.flush_pending(timeout=2)
    assert provider.connection is not None


def test_strict_shutdown_refuses_unresolved_preparation_without_releasing_store(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    provider = TwoPhaseSQLiteProvider(tmp_path / "memory.db")
    manager = _manager(provider, home)
    with _profile_scope(home):
        report = manager.prepare_retirement()
    assert report.prepared
    retained = manager._hard_retirement_state
    try:
        with _profile_scope(home):
            manager.shutdown_all()
            with pytest.raises(RuntimeError, match="retirement custody remains unresolved"):
                manager.shutdown_all(strict=True)
        assert manager._hard_retirement_state is retained
        assert manager._retirement_preparation_lease is report.lease
        assert provider.connection.execute("SELECT 1").fetchone() == (1,)
        assert provider.retire_calls == []
        assert manager._shutting_down is False
    finally:
        assert manager.resume_retirement(report.lease) is True
        provider.shutdown()


def test_missing_two_phase_capability_refuses_before_manager_fence(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    manager = _manager(provider, home)

    with _profile_scope(home):
        report = manager.prepare_retirement()

    assert report.status == "unknown"
    assert report.reason == "missing_capability"
    assert report.lease is None
    assert manager._hard_retirement_state is None
    assert manager._shutting_down is False
    assert provider.connection is not None
    provider.shutdown()


def test_prepare_drain_timeout_keeps_lease_without_destructive_shutdown(tmp_path, monkeypatch):
    import agent.memory_manager as memory_manager_module

    home = tmp_path / "home"
    home.mkdir()
    provider = TwoPhaseSQLiteProvider(tmp_path / "memory.db")
    provider.block_sync = True
    manager = _manager(provider, home)
    monkeypatch.setattr(memory_manager_module, "_SYNC_DRAIN_TIMEOUT_S", 0.05)
    with _profile_scope(home):
        manager.sync_all("blocked", "response", session_id="session")
    assert provider.sync_started.wait(timeout=2)

    with _profile_scope(home):
        first = manager.prepare_retirement()
    assert first.status == "unknown"
    assert first.reason == "drain_timeout"
    assert first.lease is manager._retirement_preparation_lease
    assert provider.retire_calls == []
    assert manager._shutting_down is False

    provider.sync_release.set()
    assert provider.sync_done.wait(timeout=2)
    provider.block_sync = False
    with _profile_scope(home):
        second = manager.prepare_retirement()
    assert second.prepared
    assert second.lease is first.lease
    assert manager.resume_retirement(second.lease) is True


def test_provider_prepare_timeout_retains_exact_call_and_uses_shared_budget(tmp_path, monkeypatch):
    import agent.memory_manager as memory_manager_module

    home = tmp_path / "home"
    home.mkdir()
    provider = BlockingPreparationProvider(tmp_path / "memory.db")
    manager = _manager(provider, home)
    monkeypatch.setattr(memory_manager_module, "_SYNC_DRAIN_TIMEOUT_S", 0.05)

    with _profile_scope(home):
        first = manager.prepare_retirement()

    assert provider.prepare_started.wait(timeout=2)
    assert first.status == "unknown"
    assert first.reason == "preparation_timeout"
    assert first.lease is manager._retirement_preparation_lease
    assert first.lease.provider_preparation_calls[0] is not None
    assert manager._shutting_down is False
    assert provider.connection is not None
    assert manager.resume_retirement(first.lease) is False

    provider.prepare_release.set()
    assert first.lease.provider_preparation_calls[0].done.wait(timeout=2)
    with _profile_scope(home):
        second = manager.prepare_retirement()

    assert second.prepared
    assert second.lease is first.lease
    assert second.lease.provider_preparation_calls[0] is None
    assert manager.resume_retirement(second.lease) is True
    assert provider.connection is not None


def test_partial_resume_refences_resumed_provider_without_losing_custody(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    first = TwoPhaseSQLiteProvider(tmp_path / "first.db", name="builtin")
    second = TwoPhaseSQLiteProvider(tmp_path / "second.db", name="two-phase")
    manager = MemoryManager()
    with _profile_scope(home):
        manager.add_provider(first)
        manager.add_provider(second)
        manager.initialize_all(session_id="session", hermes_home=str(home), platform="test")
        prepared = manager.prepare_retirement()

    assert prepared.prepared
    second.resume_result = False
    assert manager.resume_retirement(prepared.lease) is False
    assert prepared.lease.phase == "resume_failed"
    assert prepared.lease.provider_prepared[0] is True
    assert first._preparation_lease is not None

    second.resume_result = True
    assert manager.resume_retirement(prepared.lease) is True
    assert manager._retirement_preparation_lease is None
    first.shutdown()
    second.shutdown()


def test_definitely_unstarted_prepare_can_retry_without_provider_call(tmp_path, monkeypatch):
    import agent.memory_retirement_manager as retirement_manager_module

    home = tmp_path / "home"
    home.mkdir()
    provider = TwoPhaseSQLiteProvider(tmp_path / "memory.db")
    manager = _manager(provider, home)
    real_spawn = retirement_manager_module.spawn_context_thread
    spawn_calls = 0

    def fail_before_start_once(target, *, name, daemon=True, args=(), kwargs=None):
        nonlocal spawn_calls
        spawn_calls += 1
        if spawn_calls == 1:
            # No Thread object is returned: the factory failed before any native work could
            # have been accepted, which is the only start failure safe to retry.
            raise RuntimeError("synthetic thread factory failure")
        return real_spawn(target, name=name, daemon=daemon, args=args, kwargs=kwargs)

    monkeypatch.setattr(retirement_manager_module, "spawn_context_thread", fail_before_start_once)
    with _profile_scope(home):
        first = manager.prepare_retirement()

    assert first.status == "unknown"
    assert first.reason == "provider_prepare_failed"
    assert first.lease.provider_preparation_calls[0] is None
    assert provider._preparation_lease is None

    with _profile_scope(home):
        second = manager.prepare_retirement()

    assert second.prepared
    assert spawn_calls == 2
    assert manager.resume_retirement(second.lease) is True


def test_ambiguous_prepare_start_keeps_exact_call_without_duplicate(tmp_path, monkeypatch):
    import agent.memory_manager as memory_manager_module
    import agent.memory_retirement_manager as retirement_manager_module

    home = tmp_path / "home"
    home.mkdir()
    provider = BlockingPreparationProvider(tmp_path / "memory.db")
    manager = _manager(provider, home)
    monkeypatch.setattr(memory_manager_module, "_SYNC_DRAIN_TIMEOUT_S", 0.05)
    real_spawn = retirement_manager_module.spawn_context_thread

    def spawn_then_raise(target, *, name, daemon=True, args=(), kwargs=None):
        thread = real_spawn(target, name=name, daemon=daemon, args=args, kwargs=kwargs)
        real_start = thread.start

        def ambiguous_start():
            real_start()
            raise RuntimeError("synthetic start failed after target launch")

        thread.start = ambiguous_start
        return thread

    monkeypatch.setattr(retirement_manager_module, "spawn_context_thread", spawn_then_raise)
    with _profile_scope(home):
        first = manager.prepare_retirement()

    assert provider.prepare_started.wait(timeout=2)
    assert first.status == "unknown"
    assert first.reason == "preparation_timeout"
    call = first.lease.provider_preparation_calls[0]
    assert call is not None and call.entered.is_set()
    assert provider.prepare_calls == 1

    provider.prepare_release.set()
    assert call.done.wait(timeout=2)
    with _profile_scope(home):
        second = manager.prepare_retirement()

    assert second.prepared
    assert provider.prepare_calls == 1
    assert manager.resume_retirement(second.lease) is True


def test_unknown_commit_keeps_exact_lease_for_retry_and_cannot_resume(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    provider = TwoPhaseSQLiteProvider(tmp_path / "memory.db")
    provider.retire_mode = "close_failed"
    manager = _manager(provider, home)

    with _profile_scope(home):
        prepared = manager.prepare_retirement()
        first = manager.commit_retirement(prepared.lease)

    assert first.status == "unknown"
    assert first.reason == "close_failed"
    assert prepared.lease is manager._retirement_preparation_lease
    assert manager._shutting_down is True
    assert provider.connection is not None
    assert manager.resume_retirement(prepared.lease) is False

    provider.retire_mode = "released"
    with _profile_scope(home):
        second = manager.commit_retirement(prepared.lease)

    assert second.released
    assert manager.providers == []
    assert provider.connection is None
    assert len(provider.retire_calls) == 2


def test_foreign_manager_lease_cannot_commit_or_resume(tmp_path):
    home = tmp_path / "home"
    home.mkdir()
    provider = TwoPhaseSQLiteProvider(tmp_path / "memory.db")
    manager = _manager(provider, home)

    with _profile_scope(home):
        prepared = manager.prepare_retirement()

    assert manager.commit_retirement(object()).reason == "invalid_lease"
    assert manager.resume_retirement(object()) is False
    assert manager._retirement_preparation_lease is prepared.lease
    provider._preparation_lease = None
    provider.shutdown()


@pytest.mark.parametrize("resume_result", [False, True])
def test_agent_two_phase_facade_preserves_typed_manager_boundary(tmp_path, resume_result):
    from run_agent import AIAgent

    manager = MemoryManager()
    agent = object.__new__(AIAgent)
    agent._memory_manager = manager
    provider = TwoPhaseSQLiteProvider(tmp_path / "memory.db")
    home = tmp_path / "home"
    home.mkdir()
    with _profile_scope(home):
        manager.add_provider(provider)
        manager.initialize_all(session_id="session", hermes_home=str(home), platform="test")
        prepared = agent.prepare_memory_provider()
        provider.resume_result = resume_result
        resumed = agent.resume_memory_provider(prepared.lease)
    assert resumed is resume_result
