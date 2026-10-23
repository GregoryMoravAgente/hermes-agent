"""Hard retirement must close every actual provider call and setup admission."""

import threading

import pytest

from agent.memory_manager import MemoryManager
from tests.agent.test_memory_retirement_manager import SQLiteRetirementProvider, _manager, _profile_scope


def test_direct_session_end_call_keeps_provider_owned_until_it_finishes(tmp_path, monkeypatch):
    import agent.memory_manager as module

    home = tmp_path / "home"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    manager = _manager(provider, home)
    entered, release = threading.Event(), threading.Event()

    def session_end(_messages):
        entered.set()
        assert release.wait(timeout=5)

    monkeypatch.setattr(provider, "on_session_end", session_end)
    monkeypatch.setattr(module, "_SYNC_DRAIN_TIMEOUT_S", 0.05)

    def invoke():
        with _profile_scope(home):
            manager.on_session_end([])

    thread = threading.Thread(target=invoke)
    thread.start()
    try:
        assert entered.wait(timeout=2)
        with _profile_scope(home):
            report = manager.retire_hard()
        assert report.status == "unknown"
        assert report.reason == "drain_timeout"
        assert provider.retire_calls == []
        assert provider.connection is not None
    finally:
        release.set()
        thread.join(timeout=5)
        assert not thread.is_alive()
    with _profile_scope(home):
        assert manager.retire_hard().released


@pytest.mark.parametrize("operation", ["turn_start", "session_end", "pre_compress"])
def test_unknown_retirement_refuses_late_provider_callbacks(tmp_path, monkeypatch, operation):
    home = tmp_path / "home"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    provider.retire_mode = "close_failed"
    manager = _manager(provider, home)
    calls = []
    monkeypatch.setattr(provider, "on_turn_start", lambda *_a, **_k: calls.append("turn_start"))
    monkeypatch.setattr(provider, "on_session_end", lambda *_a, **_k: calls.append("session_end"))
    monkeypatch.setattr(provider, "on_pre_compress", lambda *_a, **_k: calls.append("pre_compress"))
    try:
        with _profile_scope(home):
            assert manager.retire_hard().reason == "close_failed"
            if operation == "turn_start":
                manager.on_turn_start(1, "late")
            elif operation == "session_end":
                manager.on_session_end([])
            else:
                manager.on_pre_compress([])
        assert calls == []
    finally:
        provider.retire_mode = "released"
        with _profile_scope(home):
            assert manager.retire_hard().released


@pytest.mark.parametrize("empty", [False, True])
def test_released_manager_cannot_admit_a_new_provider_incarnation(tmp_path, empty):
    home = tmp_path / "home"
    home.mkdir()
    original = SQLiteRetirementProvider(tmp_path / "original.db")
    manager = MemoryManager() if empty else _manager(original, home)
    with _profile_scope(home):
        assert manager.retire_hard().released
        replacement = SQLiteRetirementProvider(tmp_path / "replacement.db", name="replacement")
        with pytest.raises(RuntimeError, match="retirement owns provider setup"):
            manager.add_provider(replacement)
    assert manager.providers == []


def test_prefetch_admission_is_retained_before_its_thread_starts(tmp_path, monkeypatch):
    import agent.memory_manager as module

    home = tmp_path / "home"
    home.mkdir()
    provider = SQLiteRetirementProvider(tmp_path / "memory.db")
    manager = _manager(provider, home)
    pending_start, release_start = threading.Event(), threading.Event()
    real_spawn = module.spawn_context_thread
    monkeypatch.setattr(module, "_SYNC_DRAIN_TIMEOUT_S", 0.05)

    def deferred_spawn(*args, **kwargs):
        thread = real_spawn(*args, **kwargs)
        start = thread.start

        def deferred_start():
            pending_start.set()
            assert release_start.wait(timeout=5)
            start()

        monkeypatch.setattr(thread, "start", deferred_start)
        return thread

    monkeypatch.setattr(module, "spawn_context_thread", deferred_spawn)

    def invoke():
        with _profile_scope(home):
            manager.prefetch_all("accepted recall", session_id="session")

    thread = threading.Thread(target=invoke)
    thread.start()
    try:
        assert pending_start.wait(timeout=2)
        with _profile_scope(home):
            report = manager.retire_hard()
        assert report.reason == "drain_timeout"
        assert provider.retire_calls == []
    finally:
        release_start.set()
        thread.join(timeout=5)
        assert not thread.is_alive()
    with _profile_scope(home):
        assert manager.retire_hard().released
