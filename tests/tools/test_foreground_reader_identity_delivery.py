"""Portable custody identity controls for foreground-reader session construction."""

from __future__ import annotations

from tools.process_registry import ProcessRegistry


def test_explicit_foreground_parent_identity_overrides_ambient_session(monkeypatch, tmp_path):
    """An authenticated foreground binding wins while ordinary sessions keep the fallback."""
    monkeypatch.setenv("HERMES_SESSION_ID", "ambient-session")
    registry = ProcessRegistry()

    bound = registry._new_session(
        "foreground-command",
        "foreground-task",
        "foreground-owner",
        "foreground-route",
        str(tmp_path),
        reserve_custody=False,
        parent_session_id="bound-session",
    )
    ordinary = registry._new_session(
        "ordinary-command",
        "ordinary-task",
        "ordinary-owner",
        "ordinary-route",
        str(tmp_path),
        reserve_custody=False,
    )

    assert bound.parent_session_id == "bound-session"
    assert ordinary.parent_session_id == "ambient-session"
