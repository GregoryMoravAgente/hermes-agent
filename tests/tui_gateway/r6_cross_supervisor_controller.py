"""Private controller for the real R6 cross-supervisor owner-lease test.

This file is a test-only overlay. It deliberately uses the existing
HostSupervisor, ComputeHost fixture, Windows named Job, registry, and public
lifecycle. It does not replace any OS or Job primitive and does not inspect a
process census.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
from pathlib import Path
import sys
import threading
from typing import Any

from tests.waiting import wait_until
from tests.tui_gateway.host_supervisor_test_env import synthetic_host_env
from tui_gateway.host_supervisor import HostSupervisor


READINESS_TIMEOUT_SECONDS = 60.0
BARRIER_TIMEOUT_SECONDS = 60.0


@dataclass
class OwnerCloseBarrier:
    """Hold one real supervisor at its existing owner-release boundary."""

    supervisor: HostSupervisor
    entered: threading.Event = field(default_factory=threading.Event)
    release: threading.Event = field(default_factory=threading.Event)
    closed: threading.Event = field(default_factory=threading.Event)

    def install(self) -> None:
        """Install a barrier without changing the real close operation."""
        original = self.supervisor._try_close_owner

        def hold(owner: Any) -> bool:
            self.entered.set()
            if not self.release.wait(BARRIER_TIMEOUT_SECONDS):
                raise TimeoutError("R6-P1 owner-close barrier was not released")
            try:
                return bool(original(owner))
            finally:
                self.closed.set()

        # The instance-level wrapper is scoped to this test process only. The
        # wrapped method remains the production method and executes unchanged.
        self.supervisor._try_close_owner = hold  # type: ignore[method-assign]

    def wait_until_entered(self) -> None:
        wait_until(
            self.entered.is_set,
            timeout=READINESS_TIMEOUT_SECONDS,
            what="real supervisor A reaches owner-close boundary",
        )

    def wait_until_closed(self) -> None:
        wait_until(
            self.closed.is_set,
            timeout=READINESS_TIMEOUT_SECONDS,
            what="real supervisor A completes owner close",
        )


def make_supervisor(
    *,
    registry_path: Path,
    hermes_home: Path,
    fixture_path: Path,
) -> HostSupervisor:
    """Create one real supervisor using the existing production constructor."""
    return HostSupervisor(
        autostart=False,
        registry_path=registry_path,
        argv=[sys.executable, str(fixture_path), "--mode", "normal"],
        cwd=fixture_path.parents[2],
        env=synthetic_host_env(hermes_home, fixture_path.parents[2]),
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
        respawn_max=3,
    )


def read_registry(registry_path: Path) -> dict[str, Any]:
    return json.loads(registry_path.read_text(encoding="utf-8"))


def wait_for_registry(registry_path: Path) -> dict[str, Any]:
    wait_until(
        registry_path.is_file,
        timeout=READINESS_TIMEOUT_SECONDS,
        what="real host registry record",
    )
    return read_registry(registry_path)


def wait_for_respawn(
    *,
    supervisor: HostSupervisor,
    registry_path: Path,
    previous_pid: int,
) -> dict[str, Any]:
    """Wait for the actual old supervisor's crash respawn and new record."""

    def respawned() -> bool:
        if not supervisor.is_running() or not registry_path.is_file():
            return False
        try:
            record = read_registry(registry_path)
        except (OSError, ValueError):
            return False
        return record.get("host_pid") != previous_pid

    wait_until(
        respawned,
        timeout=READINESS_TIMEOUT_SECONDS,
        what="old supervisor A real crash respawn",
    )
    return read_registry(registry_path)


def request_natural_child_exit(
    supervisor: HostSupervisor,
    *,
    request_id: str,
) -> None:
    """Ask the real child to perform its existing shutdown path and exit."""
    supervisor._send_frame(
        {"type": "shutdown", "request_id": request_id},
        allow_shutdown=True,
    )


def stop_for_cleanup(supervisor: HostSupervisor) -> None:
    """Use only the public supervisor stop path during test cleanup."""
    try:
        supervisor.shutdown()
    except Exception:
        # The test's assertions retain the causal failure. Cleanup must not
        # blanket-kill a process or turn a native failure into a green result.
        pass
