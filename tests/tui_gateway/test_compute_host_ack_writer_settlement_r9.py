"""Real ACK-writer custody proof for deferred host cleanup."""

from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import threading

import pytest

from tests.waiting import wait_until
from tui_gateway.host_supervisor import HostSupervisor


pytestmark = pytest.mark.platforms("posix", "windows")


def _ack_supervisor(tmp_path: Path, paths: dict[str, Path]) -> HostSupervisor:
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_gate_child_r3.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    return HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=[
            sys.executable, str(fixture), "--mode", "normal",
            "--shutdown-ack-ready", str(paths["ack-ready"]),
            "--shutdown-ack-release", str(paths["ack-release"]),
            "--shutdown-drain-ready", str(paths["drain-ready"]),
            "--shutdown-drain-release", str(paths["drain-release"]),
            "--shutdown-explicit-drain-returned", str(paths["explicit-drain-returned"]),
            "--shutdown-fallback-entered", str(paths["fallback-entered"]),
            "--shutdown-fallback-shutdown", str(paths["fallback-shutdown"]),
            "--shutdown-unregister", str(paths["unregister"]),
            "--shutdown-join-returned", str(paths["join-returned"]),
            "--shutdown-join-release", str(paths["join-release"]),
            "--shutdown-reader-settled", str(paths["reader-settled"]),
            "--shutdown-writer-join-entered", str(paths["writer-join-entered"]),
            "--shutdown-reader-finishes-first",
            "--shutdown-reader-fully-returned", str(paths["reader-fully-returned"]),
            "--reader-finished", str(paths["reader-finished"]),
        ],
        cwd=root,
        env={"HERMES_HOME": str(hermes_home), "PYTHONPATH": str(root)},
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )


def test_ack_writer_retains_transport_until_real_writer_settles(tmp_path: Path):
    """A blocked real ACK writer retains child and parent custody past reader return."""
    paths = {
        "ack-ready": tmp_path / "ack-ready",
        "ack-release": tmp_path / "ack-release",
        "drain-ready": tmp_path / "drain-ready",
        "drain-release": tmp_path / "drain-release",
        "explicit-drain-returned": tmp_path / "explicit-drain-returned",
        "fallback-entered": tmp_path / "fallback-entered",
        "fallback-shutdown": tmp_path / "fallback-shutdown",
        "unregister": tmp_path / "unregister",
        "join-returned": tmp_path / "join-returned",
        "join-release": tmp_path / "join-release",
        "reader-settled": tmp_path / "reader-settled",
        "writer-join-entered": tmp_path / "writer-join-entered",
        "reader-fully-returned": tmp_path / "reader-fully-returned",
        "reader-finished": tmp_path / "reader-finished",
    }
    supervisor = _ack_supervisor(tmp_path, paths)
    results: list[dict] = []
    errors: list[str] = []
    done = threading.Event()
    stopper: threading.Thread | None = None

    def stop() -> None:
        try:
            results.append(supervisor.shutdown_clean(timeout=30))
        except BaseException as exc:
            errors.append(type(exc).__name__)
        finally:
            done.set()

    try:
        supervisor.start()
        proc = supervisor._proc
        assert proc is not None
        stopper = threading.Thread(target=stop, name="r9-ack-writer-stop")
        stopper.start()

        # Let the real explicit drain reach the production path, then release only
        # that drain. The actual shutdown ACK writer is gated separately below.
        wait_until(paths["drain-ready"].exists, timeout=15, what="real explicit drain gate")
        paths["drain-release"].write_text("release", encoding="utf-8")
        wait_until(paths["join-returned"].exists, timeout=15, what="real bounded reader join")
        wait_until(paths["ack-ready"].exists, timeout=15, what="real shutdown ACK writer gate")
        assert proc.poll() is None
        assert done.is_set() is False

        # Force the actual reader thread to finish before releasing the
        # test-owned bounded-join barrier. This makes an inline finalizer
        # observable while the ACK writer is still gated.
        wait_until(
            paths["reader-fully-returned"].exists, timeout=20,
            what="real control reader thread exit while ACK is gated",
        )
        fully_returned = json.loads(
            paths["reader-fully-returned"].read_text(encoding="utf-8")
        )
        assert fully_returned["alive"] is False
        assert fully_returned["joined"] is True
        # Release the test-owned bounded reader-join observation only after the
        # writer is known to be blocked, then observe the deferred finalizer.
        paths["join-release"].write_text("release", encoding="utf-8")
        wait_until(
            paths["reader-finished"].exists, timeout=20,
            what="real control reader return after ACK deadline",
        )
        # These two markers come from the production deferred finalizer: the
        # first follows its exact reader.join(None), and the second is emitted
        # immediately before its exact lifecycle-writer join. The older
        # generator-finally marker above is intentionally not the custody boundary.
        wait_until(
            paths["reader-settled"].exists, timeout=20,
            what="real deferred reader join settlement",
        )
        wait_until(
            paths["writer-join-entered"].exists, timeout=15,
            what="real deferred ACK writer join entry",
        )
        assert json.loads(paths["reader-settled"].read_text(encoding="utf-8")) == {
            "thread": "compute-host-deferred-terminal-cleanup",
            "joined": True,
        }
        assert json.loads(paths["writer-join-entered"].read_text(encoding="utf-8")) == {
            "thread": "compute-host-deferred-terminal-cleanup",
            "timeout": None,
        }
        assert proc.poll() is None
        assert done.is_set() is False

        # An early fallback here would race the same child ACK writer while it is
        # still blocked. The production path must retain every custody boundary
        # until the real writer release.
        assert not paths["fallback-entered"].exists()
        assert not paths["fallback-shutdown"].exists()
        assert not paths["unregister"].exists()
        assert supervisor.capture_ready is False
        assert supervisor.registry_path.exists()
        with supervisor._lock:
            assert supervisor._registry_lease is not None
            assert supervisor._pending_shutdown_receipt is None
            if os.name == "nt":
                assert supervisor._owner is not None
                assert supervisor._owner_closed is False
                handle = supervisor._producer_process_handle
                assert handle is not None
                assert handle.is_open

        # Releasing the same real ACK gate lets the writer settle, after which the
        # existing exact ACK/natural-exit/owner acknowledgement path may proceed.
        paths["ack-release"].write_text("release", encoding="utf-8")
        wait_until(paths["unregister"].exists, timeout=15, what="post-ACK transport unregister")
        wait_until(done.is_set, timeout=30, what="exact natural shutdown result")
        assert stopper is not None
        stopper.join(timeout=2)
        assert not stopper.is_alive()
        assert not errors
        assert len(results) == 1
        result = results[0]
        assert result["shutdown_ack"] is True
        assert result["natural_exit"] is True
        assert result["exit_settled"] is True
        assert paths["fallback-shutdown"].exists()

        if os.name == "nt":
            assert result["clean"] is True
            assert supervisor.acknowledge_clean_stop(result) is True
            with supervisor._lock:
                assert supervisor._owner is None
                assert supervisor._producer_process_handle is None
                assert supervisor._registry_lease is None
        else:
            # POSIX has no native Job owner; retain the honest owner-unavailable
            # receipt and registry custody exactly as the existing native split does.
            assert result["clean"] is False
            assert result["status"] == "owner_unavailable"
            assert supervisor.acknowledge_clean_stop(result) is False
            with supervisor._lock:
                assert supervisor._registry_lease is not None
    finally:
        paths["ack-release"].write_text("release", encoding="utf-8")
        paths["join-release"].write_text("release", encoding="utf-8")
        paths["drain-release"].write_text("release", encoding="utf-8")
        if stopper is not None:
            stopper.join(timeout=35)
        supervisor.shutdown()
