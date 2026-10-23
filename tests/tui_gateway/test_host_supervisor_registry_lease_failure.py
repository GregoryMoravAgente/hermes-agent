"""Registry lease release failure must retain the exact clean-stop ACK retry."""

from pathlib import Path

from tests.tui_gateway.test_host_supervisor_owner_r4_causal import _seed_parent_state
from tui_gateway.host_supervisor import HostSupervisor


class _RetryLease:
    def __init__(self):
        self.calls = 0

    def release(self):
        self.calls += 1
        return self.calls > 1


def test_registry_lease_release_failure_keeps_receipt_and_capture_closed(tmp_path: Path):
    supervisor = HostSupervisor(autostart=False, registry_path=tmp_path / "owner.json")
    proc, owner = _seed_parent_state(supervisor, tmp_path, exited=True)
    receipt = {
        "status": "clean_stopped", "clean": True, "ack_required": True,
        "host_generation": 7, "host_boot_id": "boot-7", "host_pid": proc.pid,
    }
    lease = _RetryLease()
    supervisor._registry_lease = lease
    supervisor._pending_shutdown_receipt = dict(receipt)
    supervisor._remove_registry = lambda *_args, **_kwargs: True

    assert supervisor.acknowledge_clean_stop(receipt) is False
    assert supervisor.capture_ready is False
    assert supervisor._pending_shutdown_receipt == receipt
    assert supervisor._owner is owner
    assert supervisor._proc is proc
    assert supervisor._registry_lease is lease
    assert supervisor.acknowledge_clean_stop(receipt) is True
    assert supervisor.capture_ready is True
    assert owner.close_calls == 1


class _FailFirstClose:
    def __init__(self, handle):
        self.handle = handle
        self.calls = 0

    def __getattr__(self, name):
        return getattr(self.handle, name)

    def close(self):
        self.calls += 1
        if self.calls == 1:
            raise OSError("synthetic close refusal before actual descriptor close")
        self.handle.close()


def test_failed_registry_descriptor_close_keeps_actual_lock(tmp_path: Path):
    from tui_gateway.windows_owner_recovery import RegistryLease

    registry = tmp_path / "owner.json"
    lease = RegistryLease.acquire(registry)
    assert lease is not None
    lease._handle = _FailFirstClose(lease._handle)
    contender = None
    try:
        assert lease.release() is False
        contender = RegistryLease.acquire(registry)
        assert contender is None, "failed close released the real owner lock"
    finally:
        if contender is not None:
            assert contender.release() is True
        assert lease.release() is True
