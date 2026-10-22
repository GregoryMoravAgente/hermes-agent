"""Actual Windows causal test for R6-P1 cross-supervisor recovery."""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.tui_gateway.r6_cross_supervisor_controller import (
    OwnerCloseBarrier,
    make_supervisor,
    read_registry,
    request_natural_child_exit,
    stop_for_cleanup,
    wait_for_registry,
    wait_for_respawn,
)


pytestmark = pytest.mark.platforms("windows")


def test_real_cross_supervisor_owner_lease_serializes_recovery(tmp_path: Path) -> None:
    """R6 must RED; the planned R7 owner lease must GREEN.

    A's child exits through the real ComputeHost shutdown frame while A stays
    non-closing. The barrier is placed only when A's actual wait thread has
    observed child exit and is about to close the real named Job. At that
    instant the Job is empty but A still owns the registry/parent lifecycle.
    B must refuse without replacing A's exact record. After A closes and
    respawns through its normal path, a clean A ACK releases the lease and B
    may start and cleanly ACK.
    """
    registry_path = tmp_path / "compute-host-owner.json"
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    fixture_path = Path(__file__).with_name("compute_host_owner_child.py")

    supervisor_a = make_supervisor(
        registry_path=registry_path,
        hermes_home=hermes_home,
        fixture_path=fixture_path,
    )
    supervisor_b = make_supervisor(
        registry_path=registry_path,
        hermes_home=hermes_home,
        fixture_path=fixture_path,
    )
    owner_close = OwnerCloseBarrier(supervisor_a)
    owner_close.install()

    try:
        supervisor_a.start()
        record_before = wait_for_registry(registry_path)
        first_pid = int(record_before["host_pid"])
        record_before_bytes = registry_path.read_bytes()
        assert supervisor_a.is_running()

        # This is a real child shutdown request. It leaves A._closing false,
        # so A's existing crash/recovery path remains the exercised path.
        request_natural_child_exit(
            supervisor_a,
            request_id="r6-p1-natural-child-exit",
        )
        owner_close.wait_until_entered()

        assert supervisor_a._closing is False
        assert supervisor_a.is_running() is False
        assert supervisor_a._owner is not None
        assert supervisor_a._owner.active_processes() == 0

        # On old R6, B consumes the point-in-time empty Job, removes the
        # shared record, and starts. R7's owner lease must make this a typed
        # refusal while A's parent/lease is still alive.
        with pytest.raises(RuntimeError, match=r"owner|owned|lease"):
            supervisor_b.start()
        assert supervisor_b.is_running() is False
        assert registry_path.read_bytes() == record_before_bytes
        assert read_registry(registry_path) == record_before

        owner_close.release.set()
        owner_close.wait_until_closed()
        respawn_record = wait_for_respawn(
            supervisor=supervisor_a,
            registry_path=registry_path,
            previous_pid=first_pid,
        )
        assert int(respawn_record["host_pid"]) != first_pid
        assert supervisor_a.is_running()

        # A's real clean stop + ACK releases the R7 owner lease. The second
        # real supervisor is now allowed to own the same registry.
        clean_receipt_a = supervisor_a.shutdown_clean(timeout=30)
        assert clean_receipt_a["clean"] is True
        assert clean_receipt_a["shutdown_ack"] is True
        assert supervisor_a.acknowledge_clean_stop(clean_receipt_a) is True
        assert not registry_path.exists()

        supervisor_b.start()
        record_b = wait_for_registry(registry_path)
        assert int(record_b["host_pid"]) == supervisor_b.pid
        assert supervisor_b.is_running()
        clean_receipt_b = supervisor_b.shutdown_clean(timeout=30)
        assert clean_receipt_b["clean"] is True
        assert clean_receipt_b["shutdown_ack"] is True
        assert supervisor_b.acknowledge_clean_stop(clean_receipt_b) is True
        assert not registry_path.exists()
    finally:
        owner_close.release.set()
        stop_for_cleanup(supervisor_b)
        stop_for_cleanup(supervisor_a)
