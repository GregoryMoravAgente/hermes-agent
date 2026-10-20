"""Native and context boundaries for managed shell-hook custody."""

from __future__ import annotations

from types import SimpleNamespace


def test_bare_posix_process_group_is_not_native_empty_proof() -> None:
    from gateway.runtime_service_custody import ServiceProcessReceipt

    service = SimpleNamespace(unknown=False, coordinator=SimpleNamespace())
    handle = SimpleNamespace(owner_id="shell", owner_class="I16_turn_sidecars")
    receipt = ServiceProcessReceipt(service, handle, "shell")
    receipt.scope = "posix_process_group"
    receipt.process_group_id = 1234
    receipt.process = SimpleNamespace(poll=lambda: 0)

    assert receipt._posix_process_group_empty() is False
    assert receipt._stop_posix_process_group() is False


