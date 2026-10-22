"""Consuming shell-hook route controls.

These tests exercise the registered shell callback itself.  A managed route
must have a source-owned parent before it can create a child; the explicit
standalone helper keeps its historical behavior.
"""

from __future__ import annotations

import sys
from pathlib import Path


def test_standalone_legacy_shell_hook_still_runs_real_child() -> None:
    from agent.shell_hooks import ShellHookSpec, run_once

    spec = ShellHookSpec(
        event="on_session_start",
        command=f"{sys.executable} -c \"print('{{}}')\"",
    )
    result = run_once(spec, {"session_id": "standalone-session"})

    assert result["returncode"] == 0
    assert result["error"] is None


def test_managed_event_without_source_owner_refuses_before_popen(tmp_path: Path) -> None:
    from agent.shell_hook_scope import managed_hook_route
    from agent.shell_hooks import ShellHookSpec, _make_callback

    marker = tmp_path / "must-not-run"
    spec = ShellHookSpec(
        event="on_session_start",
        command=f'{sys.executable} -c "from pathlib import Path; Path({str(marker)!r}).touch()"',
    )

    with managed_hook_route():
        result = _make_callback(spec)(session_id="managed-session")

    assert result is None
    assert not marker.exists()
