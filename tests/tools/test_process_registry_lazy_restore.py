"""Importing the tool registry must not touch state.db; the durable completion replay runs when a
consumer asks for it (#123265)."""
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def test_model_tools_import_creates_no_state_db(tmp_path):
    typo_home = tmp_path / "profiles" / "typo"  # a missing named profile: a typo'd HERMES_HOME
    env = {**os.environ, "HERMES_HOME": str(typo_home), "PYTHONPATH": str(REPO)}
    env.pop("HERMES_DELEGATED_CHILD_CONTEXT", None)
    proc = subprocess.run([sys.executable, "-c", "import model_tools"], cwd=REPO, env=env,
                          text=True, capture_output=True, timeout=120, check=False)
    assert proc.returncode == 0, proc.stderr[-2000:]
    assert not (typo_home / "state.db").exists(), sorted(os.listdir(typo_home)) if typo_home.exists() else None


def test_restore_runs_once_on_first_drain(monkeypatch):
    from tools import async_delegation, process_registry as pr_mod
    calls = []
    monkeypatch.setattr(async_delegation, "restore_undelivered_completions", lambda q: calls.append(q) or 1)
    registry = pr_mod.ProcessRegistry()
    assert calls == []  # construction (module import) does not replay the ledger
    registry.drain_notifications("sess")
    registry.drain_notifications("sess")
    assert registry.restore_completions() == 0
    assert calls == [registry.completion_queue]  # first consumer restores, exactly once


def test_first_drain_under_secondary_scope_replays_the_launch_ledger(monkeypatch, tmp_path):
    """Under multi-profile ``hermes serve`` the first consumer is a session bound to a secondary
    profile (TUI poller / prompt_turn drain); the once-per-process replay must still read the LAUNCH
    ledger, or it is never replayed for the life of the process."""
    from hermes_constants import get_hermes_home, reset_hermes_home_override, set_hermes_home_override
    from tools import async_delegation, process_registry as pr_mod
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "launch"))
    ledgers = []
    monkeypatch.setattr(async_delegation, "restore_undelivered_completions",
                        lambda q: ledgers.append(async_delegation._db_path()) or 0)
    registry = pr_mod.ProcessRegistry()
    token = set_hermes_home_override(tmp_path / "launch" / "profiles" / "b")
    try:
        registry.drain_notifications("sess")
        assert get_hermes_home() == tmp_path / "launch" / "profiles" / "b"  # caller scope untouched
    finally:
        reset_hermes_home_override(token)
    assert ledgers == [tmp_path / "launch" / "state.db"]


def test_completed_result_restore_keeps_legacy_receipts_and_redacted_metadata(monkeypatch, tmp_path):
    import json

    from tools import process_registry_results as receipts

    parent = "sess-cold"
    path = tmp_path / "proc_cold"
    record = {
        "id": "proc_cold",
        "command": "printf legacy",
        "cwd": None,
        "task_id": "task",
        "owner_task_id": "task",
        "session_key": "key",
        "parent_session_id": parent,
        "started_at": 1.0,
        "exit_code": 0,
        "completion_reason": "exited",
        "termination_source": "",
        "notify_on_complete": True,
        "output": "legacy output",
    }
    path.write_text(json.dumps(record), encoding="utf-8")
    monkeypatch.setattr(receipts, "get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(receipts, "_result_paths", lambda: [path])
    monkeypatch.setattr(receipts, "_owns_result", lambda *_args: True)
    monkeypatch.setattr("gateway.session_context.get_session_env", lambda *_args: parent)

    restored = receipts.load_completed_results()
    legacy = restored["proc_cold"]
    assert legacy.output_log == ""
    assert legacy.total_output_chars == len(record["output"])
    assert legacy._completion_event.is_set()

    record.update(
        output_log="<HERMES_HOME>/logs/process-output/proc_cold.log",
        total_output_chars=987,
    )
    path.write_text(json.dumps(record), encoding="utf-8")
    restored = receipts.load_completed_results()
    current = restored["proc_cold"]
    assert current.output_log == str(tmp_path / "logs/process-output/proc_cold.log")
    assert current.total_output_chars == 987
