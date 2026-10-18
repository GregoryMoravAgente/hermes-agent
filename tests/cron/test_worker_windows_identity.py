"""Typed checks for the Windows external-worker identity receipt."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from cron.scheduler_worker_identity import (
    WorkerIdentityUnavailable,
    WindowsWorkerReceipt,
    build_worker_ack_identity,
    capture_windows_worker_receipt,
    reuse_or_capture_windows_worker_receipt,
    retained_windows_worker_matches_ack,
    write_worker_ack,
    write_worker_acceptance,
    worker_receipt_matches_record,
)


def test_worker_ack_identity_is_platform_specific_and_positive_when_native():
    identity = build_worker_ack_identity()
    if os.name == "nt":
        assert type(identity.get("process_creation_filetime")) is int
        assert identity["process_creation_filetime"] > 0
    else:
        assert identity == {}


def test_worker_receipt_matches_only_exact_durable_pid_and_start():
    receipt = WindowsWorkerReceipt(
        pid=4312,
        process_started_at=9876,
        creation_filetime=134367929582396490,
        process_handle=SimpleNamespace(is_open=True),
    )
    assert worker_receipt_matches_record(
        receipt, {"pid": 4312, "process_started_at": 9876})
    assert not worker_receipt_matches_record(
        receipt, {"pid": 4313, "process_started_at": 9876})
    assert not worker_receipt_matches_record(
        receipt, {"pid": 4312, "process_started_at": 9877})
    assert not worker_receipt_matches_record(
        receipt, {"pid": True, "process_started_at": 9876})


def test_retained_worker_ack_retry_reuses_exact_handle_without_reopening():
    class Job:
        def __init__(self, handle):
            self.handle = handle
            self.calls = 0

        def contains_process_handle(self, handle):
            self.calls += 1
            return handle is self.handle

    handle = SimpleNamespace(is_open=True)
    job = Job(handle)
    acknowledgement = {
        "pid": 4312,
        "process_started_at": 9876,
        "process_creation_filetime": 134367929582396490,
    }
    record = {"pid": 4312, "process_started_at": 9876}
    assert retained_windows_worker_matches_ack(
        acknowledgement,
        record,
        job,
        handle=handle,
        retained_pid=4312,
        retained_started_at=9876,
        retained_creation_filetime=134367929582396490,
        job_closed=False,
    )
    receipt, already_active = reuse_or_capture_windows_worker_receipt(
        acknowledgement,
        record,
        job,
        retained_handle=handle,
        retained_pid=4312,
        retained_started_at=9876,
        retained_creation_filetime=134367929582396490,
        job_closed=False,
        already_active=True,
    )
    assert receipt is None
    assert already_active is True
    assert job.calls == 2


def test_worker_ack_writer_publishes_one_complete_atomic_record(tmp_path):
    ack_path = tmp_path / "execution.ready"

    write_worker_ack(
        ack_path,
        execution_id="execution-1",
        process_started_at=9876,
        dispatch_mode="direct",
        scope_unit=None,
        scope_control_group=None,
    )

    acknowledgement = json.loads(ack_path.read_text(encoding="utf-8"))
    assert acknowledgement["pid"] == os.getpid()
    assert acknowledgement["process_started_at"] == 9876
    assert acknowledgement["execution_id"] == "execution-1"
    assert acknowledgement["dispatch_mode"] == "direct"
    assert not list(tmp_path.glob("execution.ready.tmp*"))


def test_parent_acceptance_gate_is_real_subprocess_barrier(tmp_path):
    """A real child cannot publish its post-adoption marker until the exact parent decision is
    atomically written. This stays platform-neutral; Windows native Job proof is covered below."""
    ack = tmp_path / "execution.ready"
    accept = tmp_path / "execution.accept"
    marker = tmp_path / "side-effect.marker"
    child = textwrap.dedent(
        """
        import sys
        from pathlib import Path
        from cron.scheduler_worker_identity import write_worker_ack, wait_for_worker_acceptance

        ack = Path(sys.argv[1])
        accept = Path(sys.argv[2])
        marker = Path(sys.argv[3])
        acknowledgement = write_worker_ack(
            ack, execution_id="execution-1", process_started_at=1,
            dispatch_mode="direct", scope_unit=None, scope_control_group=None,
            handoff_nonce="child-only-nonce", custody_generation=None,
        )
        if not wait_for_worker_acceptance(accept, acknowledgement, timeout=3):
            raise SystemExit(2)
        marker.write_text("accepted", encoding="utf-8")
        """
    )
    process = subprocess.Popen(
        [sys.executable, "-c", child, str(ack), str(accept), str(marker)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 2
        while not ack.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert ack.exists()
        assert not marker.exists()
        acknowledgement = json.loads(ack.read_text(encoding="utf-8"))
        write_worker_acceptance(accept, acknowledgement, expected_generation=None)
        assert process.wait(timeout=5) == 0
        assert marker.read_text(encoding="utf-8") == "accepted"
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        if process.stderr is not None:
            process.stderr.close()


def test_parent_acceptance_rejects_stale_nonce_and_generation(tmp_path):
    acknowledgement = {
        "handoff_nonce": "current-nonce",
        "execution_id": "execution-1",
        "pid": 4312,
        "process_started_at": 9876,
        "process_creation_filetime": None,
        "dispatch_mode": "direct",
        "scope_unit": None,
        "scope_control_group": None,
        "custody_generation": {
            "process_instance": "instance-1",
            "authority_epoch": 4,
            "close_generation": 7,
        },
    }
    accept = tmp_path / "execution.accept"
    accept.write_text(json.dumps({
        **acknowledgement,
        "decision": "accepted",
        "handoff_nonce": "stale-nonce",
    }), encoding="utf-8")
    from cron.scheduler_worker_identity import wait_for_worker_acceptance

    assert not wait_for_worker_acceptance(accept, acknowledgement, timeout=0.1)
    assert not accept.exists()
    with pytest.raises(WorkerIdentityUnavailable, match="identity_mismatch"):
        write_worker_acceptance(
            accept, acknowledgement,
            expected_generation={
                "process_instance": "foreign-instance",
                "authority_epoch": 4,
                "close_generation": 7,
            },
        )


@pytest.mark.platforms("windows")
def test_windows_receipt_requires_exact_filetime_and_owned_job():
    from gateway.status import get_process_start_time
    from hermes_cli.local_runtime.processes import spawn_server
    from tui_gateway.windows_owner_recovery import new_windows_job_name
    from cron.scheduler_worker_identity import WorkerIdentityUnavailable

    child_code = textwrap.dedent(
        """
        import json
        import os
        from tui_gateway.windows_producer_identity import current_process_creation_filetime

        print(json.dumps({
            "pid": os.getpid(),
            "process_creation_filetime": current_process_creation_filetime(),
        }), flush=True)
        assert input() == "x"
        """
    )
    process, owned_job = spawn_server(
        [sys.executable, "-c", child_code],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        owner_job_name=new_windows_job_name(),
    )
    foreign_process = None
    foreign_job = None
    retained_handles = []
    try:
        assert process.stdout is not None
        acknowledgement = json.loads(process.stdout.readline())
        acknowledgement["process_started_at"] = get_process_start_time(acknowledgement["pid"])
        assert type(acknowledgement["process_started_at"]) is int

        receipt = capture_windows_worker_receipt(acknowledgement, owned_job)
        retained_handles.append(receipt.process_handle)
        assert receipt.process_handle.is_open is True
        assert owned_job.contains_process_handle(receipt.process_handle) is True

        with pytest.raises(WorkerIdentityUnavailable) as missing_filetime:
            capture_windows_worker_receipt(
                {key: value for key, value in acknowledgement.items()
                 if key != "process_creation_filetime"},
                owned_job,
            )
        assert missing_filetime.value.code == "worker_ack_identity_malformed"

        with pytest.raises(WorkerIdentityUnavailable) as wrong_filetime:
            capture_windows_worker_receipt(
                {**acknowledgement, "process_creation_filetime":
                    acknowledgement["process_creation_filetime"] + 1},
                owned_job,
            )
        assert wrong_filetime.value.code == "worker_creation_identity_mismatch"
        assert wrong_filetime.value.retained_handle is not None
        retained_handles.append(wrong_filetime.value.retained_handle)

        foreign_process, foreign_job = spawn_server(
            [sys.executable, "-c", child_code],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            owner_job_name=new_windows_job_name(),
        )
        assert foreign_process.stdout is not None
        foreign_acknowledgement = json.loads(foreign_process.stdout.readline())
        foreign_acknowledgement["process_started_at"] = get_process_start_time(
            foreign_acknowledgement["pid"])
        assert type(foreign_acknowledgement["process_started_at"]) is int

        # The PID is real and the ACK is complete, but the exact owned Job is different. This
        # drives the native capture boundary rather than testing only a PID/start predicate.
        with pytest.raises(WorkerIdentityUnavailable) as wrong_pid_membership:
            capture_windows_worker_receipt(foreign_acknowledgement, owned_job)
        assert wrong_pid_membership.value.code == "worker_job_membership_unproven"
        assert wrong_pid_membership.value.retained_handle is not None
        retained_handles.append(wrong_pid_membership.value.retained_handle)

        with pytest.raises(WorkerIdentityUnavailable) as foreign_membership:
            capture_windows_worker_receipt(acknowledgement, foreign_job)
        assert foreign_membership.value.code == "worker_job_membership_unproven"
        assert foreign_membership.value.retained_handle is not None
        retained_handles.append(foreign_membership.value.retained_handle)

        assert not worker_receipt_matches_record(
            receipt,
            {"pid": acknowledgement["pid"],
             "process_started_at": acknowledgement["process_started_at"] + 1},
        )
        assert not worker_receipt_matches_record(
            receipt,
            {"pid": acknowledgement["pid"] + 1,
             "process_started_at": acknowledgement["process_started_at"]},
        )
    finally:
        for handle in retained_handles:
            try:
                handle.close()
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                pass
        for child, job in ((process, owned_job), (foreign_process, foreign_job)):
            if child is not None:
                if child.poll() is None:
                    assert child.stdin is not None
                    child.stdin.write("x")
                    child.stdin.close()
                    child.wait(timeout=5)
                if child.stdout is not None:
                    child.stdout.close()
            if job is not None:
                job.close()
