"""Portable controller for the R7 startup-timeout CAS oracle.

The controller intentionally runs the normal POSIX branch. The test-owned
Popen-shaped object blocks only its ``pid`` property after the existing entry
CAS; it does not replace ``os.name`` or any Windows API.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import threading

from tests.waiting import wait_until
from tui_gateway.host_supervisor import HostSupervisor


@dataclass
class BlockingPidProcess:
    """Test-owned Popen-shaped seam that blocks the pre-publish PID read."""

    value: int = 4312
    pid_read_entered: threading.Event = field(default_factory=threading.Event)
    allow_pid_read: threading.Event = field(default_factory=threading.Event)

    @property
    def pid(self) -> int:
        self.pid_read_entered.set()
        if not self.allow_pid_read.wait(timeout=30):
            raise TimeoutError("startup PID-read barrier was not released")
        return self.value


@dataclass
class RecordingOwner:
    """Minimal owner seam used only by the existing release_owner path."""

    close_called: threading.Event = field(default_factory=threading.Event)

    def close(self) -> None:
        self.close_called.set()


def seed_startup_boundary(tmp_path: Path) -> tuple[HostSupervisor, BlockingPidProcess, RecordingOwner]:
    """Seed only the current HostSupervisor state needed by its real helpers."""
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
    )
    process = BlockingPidProcess()
    owner = RecordingOwner()
    with supervisor._lock:
        supervisor._proc = process
        supervisor._owner = owner
        supervisor._host_generation = 7
    return supervisor, process, owner


def posix_startup_record(pid: int) -> dict[str, object]:
    """Use the existing schema-1 record because this test does not patch platform."""
    return {
        "type": "producer.startup",
        "schema": 1,
        "host_pid": pid,
        "host_boot_id": "portable-r7-boot",
    }


def wait_for_pid_read(process: BlockingPidProcess) -> None:
    wait_until(
        process.pid_read_entered.is_set,
        timeout=10,
        what="startup reaches the existing Popen PID read",
    )


def release_and_join(
    process: BlockingPidProcess,
    worker: threading.Thread,
) -> None:
    """Release the blocked seam and bound cleanup; this is not an assertion oracle."""
    process.allow_pid_read.set()
    worker.join(timeout=10)
    if worker.is_alive():
        raise AssertionError("startup worker did not settle after PID-read release")
