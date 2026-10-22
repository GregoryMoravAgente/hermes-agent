"""Causal regression for duplicate run_host terminal cleanup."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import threading
import time

from tests.waiting import wait_until
from tests.tui_gateway.host_supervisor_test_env import synthetic_host_env
from tui_gateway.host_supervisor import HostSupervisor


def _boundary_supervisor(tmp_path: Path, paths: dict[str, Path]) -> HostSupervisor:
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_gate_child_r3.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    argv = [
        sys.executable, str(fixture), "--mode", "normal",
        "--shutdown-drain-ready", str(paths["drain-ready"]),
        "--shutdown-drain-release", str(paths["drain-release"]),
        "--shutdown-explicit-drain-returned", str(paths["explicit-drain-returned"]),
        "--shutdown-fallback-entered", str(paths["fallback-entered"]),
        "--shutdown-fallback-shutdown", str(paths["fallback-shutdown"]),
        "--shutdown-unregister", str(paths["unregister"]),
        "--shutdown-join-returned", str(paths["join-returned"]),
        "--shutdown-join-release", str(paths["join-release"]),
        "--reader-finished", str(paths["reader-finished"]),
    ]
    return HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=argv,
        cwd=root,
        env=synthetic_host_env(hermes_home, root),
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )


def test_reader_owns_explicit_shutdown_before_fallback_cleanup(tmp_path: Path):
    """R6 races fallback cleanup; R7 retains custody until the real reader returns."""
    paths = {
        "drain-ready": tmp_path / "drain-ready",
        "drain-release": tmp_path / "drain-release",
        "explicit-drain-returned": tmp_path / "explicit-drain-returned",
        "fallback-entered": tmp_path / "fallback-entered",
        "fallback-shutdown": tmp_path / "fallback-shutdown",
        "unregister": tmp_path / "unregister",
        "join-returned": tmp_path / "join-returned",
        "join-release": tmp_path / "join-release",
        "reader-finished": tmp_path / "reader-finished",
    }
    supervisor = _boundary_supervisor(tmp_path, paths)
    results: list[dict] = []
    errors: list[str] = []
    done = threading.Event()

    def stop() -> None:
        try:
            results.append(supervisor.shutdown_clean(timeout=30))
        except BaseException as exc:
            errors.append(type(exc).__name__)
        finally:
            done.set()

    stopper: threading.Thread | None = None
    try:
        supervisor.start()
        proc = supervisor._proc
        assert proc is not None
        stopper = threading.Thread(target=stop, name="r8-fallback-boundary-stop")
        stopper.start()

        wait_until(paths["drain-ready"].exists, timeout=15, what="real explicit drain gate")
        wait_until(paths["join-returned"].exists, timeout=10, what="real bounded reader join")
        join_state = json.loads(paths["join-returned"].read_text(encoding="utf-8"))
        assert join_state == {"alive": True, "timeout": 2.0}
        assert proc.poll() is None
        assert done.is_set() is False

        # Hold the main thread immediately after its real reader.join(2.0) returns.
        # This makes the observation boundary deterministic without changing production
        # deadlines or replacing the real reader/transport.
        assert not paths["fallback-entered"].exists()
        assert not paths["fallback-shutdown"].exists()
        assert not paths["unregister"].exists()
        paths["join-release"].write_text("release", encoding="utf-8")

        # On the R6 before tree this bounded observation sees the competing fallback
        # while the first real drain remains held, so the assertion below is the causal
        # RED. On R7 the deferred finalizer waits for the same reader and no marker
        # appears until the explicit drain is released.
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline and not paths["fallback-entered"].exists():
            time.sleep(0.05)
        assert not paths["fallback-entered"].exists()
        assert not paths["fallback-shutdown"].exists()
        assert not paths["unregister"].exists()
        assert proc.poll() is None
        assert paths["reader-finished"].exists() is False

        paths["drain-release"].write_text("release", encoding="utf-8")
        wait_until(
            paths["explicit-drain-returned"].exists, timeout=30,
            what="real explicit drain settlement",
        )
        wait_until(
            paths["reader-finished"].exists, timeout=30,
            what="real control reader settlement",
        )
        wait_until(done.is_set, timeout=30, what="parent shutdown result")
        stopper.join(timeout=2)
        assert not stopper.is_alive()
        assert not errors
        assert len(results) == 1
        assert results[0]["shutdown_ack"] is True
        assert results[0]["natural_exit"] is True
        assert results[0]["exit_settled"] is True
        assert not paths["fallback-entered"].exists()
        assert not paths["fallback-shutdown"].exists()
        assert paths["unregister"].exists()
    finally:
        paths["join-release"].write_text("release", encoding="utf-8")
        paths["drain-release"].write_text("release", encoding="utf-8")
        if stopper is not None:
            stopper.join(timeout=35)
        supervisor.shutdown()
