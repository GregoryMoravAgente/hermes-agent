"""Native and context boundaries for managed shell-hook custody."""

from __future__ import annotations

import signal
from types import SimpleNamespace

import pytest


def test_bare_posix_process_group_is_not_native_empty_proof() -> None:
    from gateway.runtime_service_physical import ServiceProcessReceipt

    service = SimpleNamespace(unknown=False, coordinator=SimpleNamespace())
    handle = SimpleNamespace(owner_id="shell", owner_class="I16_turn_sidecars")
    receipt = ServiceProcessReceipt(service, handle, "shell")
    receipt.scope = "posix_process_group"
    receipt.process_group_id = 1234
    receipt.process = SimpleNamespace(poll=lambda: 0)

    assert receipt._posix_process_group_empty() is False
    assert receipt._stop_posix_process_group() is False


def test_shell_posix_scope_refuses_recycled_group_before_signal(monkeypatch) -> None:
    from agent import shell_hook_custody

    process = SimpleNamespace(pid=321, poll=lambda: None)
    scope = shell_hook_custody._ShellHookPosixScope(process, 654)
    signals = []
    monkeypatch.setattr(shell_hook_custody.os, "getpgrp", lambda: 1)
    monkeypatch.setattr(shell_hook_custody.os, "getpgid", lambda _pid: 655)
    monkeypatch.setattr(shell_hook_custody.os, "killpg", lambda *args: signals.append(args))

    with pytest.raises(OSError, match="leader identity"):
        scope.terminate()

    assert signals == []


def test_shell_posix_scope_signals_authenticated_live_group(monkeypatch) -> None:
    from agent import shell_hook_custody

    process = SimpleNamespace(pid=321, poll=lambda: None)
    scope = shell_hook_custody._ShellHookPosixScope(process, 654)
    signals = []
    monkeypatch.setattr(shell_hook_custody.os, "getpgrp", lambda: 1)
    monkeypatch.setattr(shell_hook_custody.os, "getpgid", lambda _pid: 654)
    monkeypatch.setattr(shell_hook_custody.os, "killpg", lambda *args: signals.append(args))

    scope.terminate()

    assert signals == [(654, signal.SIGTERM)]
