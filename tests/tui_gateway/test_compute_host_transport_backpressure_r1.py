"""Causal real-pipe checks for bounded host shutdown and retained custody."""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
import signal
import sys
import threading
import time

import pytest

from hermes_cli import backend_retirement
from tests.waiting import wait_until
from tests.tui_gateway.host_supervisor_test_env import synthetic_host_env
from tui_gateway import host_supervisor_streams
from tui_gateway.host_supervisor import HostSupervisor, _PendingTurn


_SID = "pipe-backpressure-sid"
_SESSION_KEY = "pipe-backpressure-session"


class _ObservedStdin:
    """Observe entry into the real Popen text stream without replacing the pipe."""

    def __init__(self, stream, entered: threading.Event) -> None:
        self._stream = stream
        self._entered = entered

    def write(self, data: str) -> int:
        self._entered.set()
        return self._stream.write(data)

    def flush(self) -> None:
        self._stream.flush()

    def __getattr__(self, name):
        return getattr(self._stream, name)


def _frame(tmp_path: Path, request_id: str) -> dict[str, object]:
    return {
        "type": "turn.start",
        "sid": _SID,
        "request_id": request_id,
        "turn_id": request_id,
        "session_key": _SESSION_KEY,
        "profile_home": "",
        # The child has authenticated hello but is not reading stdin. This real
        # payload fills the OS pipe while the exact turn lease is already pending.
        "text": "pipe-fill:" + ("x" * (256 * 1024)),
        "history": [],
        "history_version": 0,
        "cols": 80,
        "cwd": str(tmp_path),
        "source": "desktop",
    }


def _supervisor(tmp_path: Path) -> tuple[HostSupervisor, Path, Path, Path]:
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_transport_backpressure_child_r1.py")
    hermes_home = tmp_path / "hermes-home"
    hermes_home.mkdir()
    ready = tmp_path / "stdin-reader-ready"
    release = tmp_path / "stdin-reader-release"
    reader_finished = tmp_path / "stdin-reader-finished"
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "compute-host.json",
        argv=[
            sys.executable, str(fixture), "--ready", str(ready), "--release", str(release),
            "--reader-finished", str(reader_finished),
        ],
        cwd=root,
        env=synthetic_host_env(hermes_home, root),
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )
    return supervisor, ready, release, reader_finished


def _completion_supervisor(tmp_path: Path):
    root = Path(__file__).resolve().parents[2]
    fixture = Path(__file__).with_name("compute_host_producer_gate_child_r3.py")
    hermes_home = tmp_path / "completion-hermes-home"
    hermes_home.mkdir()
    filled = tmp_path / "completion-filled"
    release = tmp_path / "completion-release"
    close_result = tmp_path / "completion-close-result"
    reader_finished = tmp_path / "completion-reader-finished"
    supervisor = HostSupervisor(
        autostart=False,
        registry_path=tmp_path / "completion-host.json",
        argv=[
            sys.executable, str(fixture), "--mode", "completion-backpressure",
            "--completion-filled", str(filled),
            "--completion-release", str(release),
            "--close-result", str(close_result),
            "--reader-finished", str(reader_finished),
            "--shutdown-wait", "0.25",
        ],
        cwd=root,
        env=synthetic_host_env(hermes_home, root),
        expected_build_sha="unknown",
        expected_hermes_home=str(hermes_home),
        heartbeat_secs=60,
    )
    return supervisor, filled, release, close_result, reader_finished


@pytest.mark.platforms("not windows")
def test_real_full_stdin_pipe_makes_shutdown_refuse_without_releasing_custody(
    monkeypatch, tmp_path: Path,
):
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    supervisor, ready, release, reader_finished = _supervisor(tmp_path)
    lease = fence.acquire(
        owner_kind="compute-host-turn", owner_id=f"{tmp_path}:pipe-fill", profile_id="",
    )
    callback_frames: list[dict] = []
    callback_done = threading.Event()
    submit_done = threading.Event()
    write_entered = threading.Event()
    errors: list[str] = []

    def completion(frame: dict) -> bool:
        callback_frames.append(dict(frame))
        callback_done.set()
        return True

    completion._retirement_lease = lease
    request_id = "pipe-fill-turn"
    submitter = None
    stopper = None
    proc = None
    try:
        supervisor.start()
        wait_until(ready.exists, timeout=15, what="real child stdin gate after hello")
        proc = supervisor._proc
        assert proc is not None
        proc.stdin = _ObservedStdin(proc.stdin, write_entered)

        def submit() -> None:
            try:
                supervisor.submit_turn(_frame(tmp_path, request_id), on_complete=completion)
            except BaseException as exc:
                errors.append(type(exc).__name__)
            finally:
                submit_done.set()

        submitter = threading.Thread(target=submit, name="real-pipe-turn-writer")
        submitter.start()
        wait_until(write_entered.is_set, timeout=15, what="real Popen stdin writer entry")
        wait_until(
            lambda: isinstance(supervisor._pending_turns.get(request_id), _PendingTurn),
            timeout=15,
            what="exact pending turn before blocked write",
        )

        refusal_box: list[dict] = []
        shutdown_done = threading.Event()

        def stop() -> None:
            refusal_box.append(supervisor.shutdown_clean(timeout=0.25))
            shutdown_done.set()

        started = time.monotonic()
        stopper = threading.Thread(target=stop, name="bounded-pipe-shutdown")
        stopper.start()
        assert shutdown_done.wait(2.5), "shutdown_clean must not wait on the blocked stdin writer"
        elapsed = time.monotonic() - started
        refusal = refusal_box[0]
        assert elapsed < 2.0
        assert refusal["clean"] is False
        assert refusal["status"] == "pending_work"
        assert refusal["pending_turns"] == 1
        assert refusal["admission_closed"] is True
        assert callback_frames == []
        assert fence.active_count() == 1
        assert supervisor.capture_ready is False
        assert supervisor._registry_lease is not None
        assert supervisor.registry_path.exists()

        # Release the same real reader barrier. The blocked write then reaches the
        # existing ComputeHost turn path and its source-bound completion receipt.
        release.write_text("release", encoding="utf-8")
        wait_until(submit_done.is_set, timeout=30, what="real pipe writer completion")
        stopper.join(timeout=5)
        assert not stopper.is_alive()
        assert errors == []
        wait_until(callback_done.is_set, timeout=30, what="real producer-bound callback")
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="exact turn lease release")
        wait_until(lambda: not supervisor.has_pending_turn(_SID), timeout=30,
                   what="exact pending turn removal")
        assert supervisor._pending_turns.get(request_id) is None
        assert fence.active_count() == 0
        final = supervisor.shutdown_clean(timeout=30)
        assert final["process_exited"] is True
        assert final["natural_exit"] is True
        assert final["shutdown_ack"] is True
        wait_until(reader_finished.exists, timeout=15, what="natural stdin reader settlement")
        # POSIX has no native Job owner. Natural process/reader/producer settlement
        # therefore remains an honest owner-unavailable refusal with custody retained.
        assert final["clean"] is False
        assert final["status"] == "owner_unavailable"
        assert final["owner_state"] == "unavailable"
        assert supervisor.acknowledge_clean_stop(final) is False
        assert supervisor._registry_lease is not None
        assert supervisor.registry_path.exists()
        assert supervisor.capture_ready is False
    finally:
        release.write_text("release", encoding="utf-8")
        if submitter is not None:
            submitter.join(timeout=35)
        if stopper is not None:
            stopper.join(timeout=35)
        proc = supervisor._proc
        if proc is not None and proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            with contextlib.suppress(Exception):
                proc.wait(timeout=10)
        supervisor.shutdown()


@pytest.mark.platforms("windows")
def test_windows_real_full_stdin_pipe_releases_native_owner_after_ack(
    monkeypatch, tmp_path: Path,
):
    """The same gated child reaches the real Windows Job/ACK/registry release path."""
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    supervisor, ready, release, reader_finished = _supervisor(tmp_path)
    lease = fence.acquire(
        owner_kind="compute-host-turn", owner_id=f"{tmp_path}:windows-pipe-fill", profile_id="",
    )
    callback_done = threading.Event()
    callback_frames: list[dict] = []

    def completion(frame: dict) -> bool:
        callback_frames.append(dict(frame))
        callback_done.set()
        return True

    completion._retirement_lease = lease
    submit_done = threading.Event()
    write_entered = threading.Event()
    errors: list[str] = []
    submitter = None
    try:
        supervisor.start()
        wait_until(ready.exists, timeout=15, what="native child stdin gate after hello")
        proc = supervisor._proc
        assert proc is not None
        proc.stdin = _ObservedStdin(proc.stdin, write_entered)

        def submit() -> None:
            try:
                supervisor.submit_turn(_frame(tmp_path, "windows-pipe-fill"), on_complete=completion)
            except BaseException as exc:
                errors.append(type(exc).__name__)
            finally:
                submit_done.set()

        submitter = threading.Thread(target=submit, name="native-pipe-turn-writer")
        submitter.start()
        wait_until(write_entered.is_set, timeout=15, what="native Popen stdin writer entry")
        wait_until(
            lambda: isinstance(supervisor._pending_turns.get("windows-pipe-fill"), _PendingTurn),
            timeout=15, what="native pending turn before pipe release",
        )
        release.write_text("release", encoding="utf-8")
        wait_until(submit_done.is_set, timeout=30, what="native pipe writer completion")
        wait_until(callback_done.is_set, timeout=30, what="native producer callback")
        wait_until(lambda: fence.active_count() == 0, timeout=30, what="native turn lease release")
        assert errors == []
        assert callback_frames
        receipt = supervisor.shutdown_clean(timeout=30)
        assert receipt["clean"] is True
        assert receipt["status"] == "clean_stopped"
        assert receipt["shutdown_ack"] is True
        assert receipt["owner_state"] == "empty"
        owner = supervisor._owner
        process_handle = supervisor._producer_process_handle
        assert owner is not None
        assert process_handle is not None and process_handle.is_open
        assert supervisor._producer_creation_filetime is not None
        assert owner.contains_process_handle(process_handle) is True
        assert (tmp_path / "compute-host.json").exists()
        assert supervisor.acknowledge_clean_stop(receipt) is True
        assert supervisor._owner is None
        assert process_handle.is_open is False
        assert supervisor._registry_lease is None
        assert not (tmp_path / "compute-host.json").exists()
        assert supervisor.capture_ready is True
        wait_until(reader_finished.exists, timeout=15, what="native stdin reader settlement")
    finally:
        release.write_text("release", encoding="utf-8")
        if submitter is not None:
            submitter.join(timeout=35)
        supervisor.shutdown()


@pytest.mark.platforms("windows")
def test_windows_bounded_shutdown_writer_timeout_retains_native_owner_until_ack(
    tmp_path: Path,
):
    """A blocked real shutdown writer retains the Windows Job until exact ACK custody settles."""
    supervisor, ready, release, reader_finished = _supervisor(tmp_path)
    writer_entered = threading.Event()
    writer_done = threading.Event()
    writer_errors: list[str] = []
    stopper_done = threading.Event()
    result_box: list[dict] = []
    writer = None
    stopper = None
    try:
        supervisor.start()
        wait_until(ready.exists, timeout=15, what="native child stdin gate after hello")
        proc = supervisor._proc
        assert proc is not None
        proc.stdin = _ObservedStdin(proc.stdin, writer_entered)
        owner = supervisor._owner
        process_handle = supervisor._producer_process_handle
        owner_identity = supervisor._owner_identity
        producer_identity = supervisor._producer_identity
        owner_job_name = supervisor._owner_job_name
        producer_creation_filetime = supervisor._producer_creation_filetime
        assert owner is not None
        assert process_handle is not None and process_handle.is_open
        assert owner_job_name
        assert producer_creation_filetime is not None
        assert owner_identity is not None
        assert producer_identity is not None
        assert owner.contains_process_handle(process_handle) is True
        assert supervisor._registry_lease is not None
        assert supervisor.registry_path.exists()

        def write_control() -> None:
            try:
                supervisor.control(
                    _SID, route_name="session.save", wait=False,
                    payload={"payload": "x" * (256 * 1024)},
                )
            except BaseException as exc:
                writer_errors.append(type(exc).__name__)
            finally:
                writer_done.set()

        writer = threading.Thread(target=write_control, name="native-real-pipe-owner-writer")
        writer.start()
        wait_until(writer_entered.is_set, timeout=15, what="native Popen stdin writer entry")

        def stop() -> None:
            result_box.append(supervisor.shutdown_clean(timeout=0.25))
            stopper_done.set()

        stopper = threading.Thread(target=stop, name="native-bounded-shutdown-writer-observer")
        started = time.monotonic()
        stopper.start()
        assert stopper_done.wait(2.5), "native shutdown_clean must not wait on blocked stdin writer"
        assert time.monotonic() - started < 2.0
        refusal = result_box[0]
        assert refusal["clean"] is False
        assert refusal["error"] == "shutdown_send_ambiguous"
        assert refusal["transport_writer_pending"] is True
        assert refusal["transport_writer_ambiguous"] is False
        assert refusal["shutdown_sent"] is False
        assert refusal["owner_state"] == "nonempty"
        assert refusal["host_generation"] == owner_identity[0]
        assert refusal["host_boot_id"] == owner_identity[1]
        assert refusal["host_pid"] == owner_identity[2]
        assert supervisor._owner is owner
        assert supervisor._producer_process_handle is process_handle
        assert process_handle.is_open
        assert owner.contains_process_handle(process_handle) is True
        assert supervisor._owner_job_name == owner_job_name
        assert supervisor._producer_identity == producer_identity
        assert supervisor._producer_creation_filetime == producer_creation_filetime
        assert supervisor._registry_lease is not None
        assert supervisor.registry_path.exists()
        assert supervisor.capture_ready is False

        release.write_text("release", encoding="utf-8")
        wait_until(writer_done.is_set, timeout=30, what="native blocked writer release")
        stopper.join(timeout=5)
        assert not stopper.is_alive()
        assert writer_errors == []
        wait_until(lambda: supervisor._shutdown_ack is not None, timeout=30,
                   what="native shutdown ACK after same writer")
        wait_until(lambda: proc.poll() is not None, timeout=30,
                   what="native natural shutdown exit")
        wait_until(reader_finished.exists, timeout=15, what="native stdin reader settlement")
        receipt = supervisor.shutdown_clean(timeout=30)
        assert receipt["clean"] is True
        assert receipt["status"] == "clean_stopped"
        assert receipt["shutdown_ack"] is True
        assert receipt["owner_state"] == "empty"
        assert receipt["host_generation"] == owner_identity[0]
        assert receipt["host_boot_id"] == producer_identity[1]
        assert receipt["host_pid"] == owner_identity[2]
        assert supervisor._owner is owner
        assert process_handle.is_open
        assert owner.contains_process_handle(process_handle) is True
        assert supervisor._owner_job_name == owner_job_name
        assert supervisor._producer_identity == producer_identity
        assert supervisor._producer_creation_filetime == producer_creation_filetime
        assert supervisor._registry_lease is not None
        assert supervisor.registry_path.exists()
        assert supervisor.capture_ready is False

        assert supervisor.acknowledge_clean_stop(receipt) is True
        assert supervisor._owner is None
        assert process_handle.is_open is False
        assert supervisor._registry_lease is None
        assert not supervisor.registry_path.exists()
        assert supervisor.capture_ready is True
    finally:
        release.write_text("release", encoding="utf-8")
        if writer is not None:
            writer.join(timeout=35)
        if stopper is not None:
            stopper.join(timeout=35)
        supervisor.shutdown()


@pytest.mark.platforms("not windows")
def test_bounded_shutdown_writer_timeout_retains_transport_owner(tmp_path: Path):
    """A shutdown frame blocked behind a real writer is ambiguous, never clean."""
    supervisor, ready, release, reader_finished = _supervisor(tmp_path)
    writer_entered = threading.Event()
    writer_done = threading.Event()
    writer_errors: list[str] = []
    stopper = None
    writer = None
    proc = None
    try:
        supervisor.start()
        wait_until(ready.exists, timeout=15, what="real child stdin gate after hello")
        proc = supervisor._proc
        assert proc is not None
        proc.stdin = _ObservedStdin(proc.stdin, writer_entered)

        def write_control() -> None:
            try:
                supervisor.control(
                    _SID, route_name="session.save", wait=False,
                    payload={"payload": "x" * (256 * 1024)},
                )
            except BaseException as exc:
                writer_errors.append(type(exc).__name__)
            finally:
                writer_done.set()

        writer = threading.Thread(target=write_control, name="real-pipe-owner-writer")
        writer.start()
        wait_until(writer_entered.is_set, timeout=15, what="real Popen stdin writer entry")

        result_box: list[dict] = []
        shutdown_done = threading.Event()

        def stop() -> None:
            result_box.append(supervisor.shutdown_clean(timeout=0.25))
            shutdown_done.set()

        stopper = threading.Thread(target=stop, name="bounded-shutdown-writer-observer")
        started = time.monotonic()
        stopper.start()
        assert shutdown_done.wait(2.5), "bounded shutdown must not wait on the lifecycle lock"
        assert time.monotonic() - started < 2.0
        result = result_box[0]
        assert result["clean"] is False
        assert result["error"] == "shutdown_send_ambiguous"
        assert supervisor.capture_ready is False
        assert supervisor._registry_lease is not None
        assert supervisor.registry_path.exists()
        started = time.monotonic()
        retained = supervisor.shutdown()
        assert time.monotonic() - started < 2.0
        assert retained["status"] == "transport_writer_pending"
        assert retained["ack_required"] is False
        assert retained["transport_writer_pending"] is True
        assert supervisor._registry_lease is not None

        release.write_text("release", encoding="utf-8")
        wait_until(writer_done.is_set, timeout=30, what="real blocked writer release")
        stopper.join(timeout=5)
        assert not stopper.is_alive()
        assert writer_errors == []
        wait_until(lambda: supervisor._shutdown_ack is not None, timeout=30,
                   what="exact shutdown ACK after same writer")
        wait_until(lambda: proc.poll() is not None, timeout=30, what="real post-writer shutdown")
        wait_until(reader_finished.exists, timeout=15, what="natural stdin reader settlement")
        assert proc.returncode == 0
        final = supervisor.shutdown_clean(timeout=30)
        assert final["shutdown_ack"] is True
        assert final["natural_exit"] is True
        # POSIX has no native Job owner even after the exact writer/reader receipt.
        # Keep the physical custody refusal explicit instead of treating transport
        # settlement as a clean owner acknowledgement.
        assert final["clean"] is False
        assert final["status"] == "owner_unavailable"
        assert final["owner_state"] == "unavailable"
        assert supervisor.acknowledge_clean_stop(final) is False
        assert supervisor._registry_lease is not None
        assert supervisor.registry_path.exists()
        assert supervisor.capture_ready is False
    finally:
        release.write_text("release", encoding="utf-8")
        if writer is not None:
            writer.join(timeout=35)
        if stopper is not None:
            stopper.join(timeout=35)
        proc = supervisor._proc
        if proc is not None and proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            with contextlib.suppress(Exception):
                proc.wait(timeout=10)
        supervisor.shutdown()


@pytest.mark.platforms("not windows")
def test_real_completion_pipe_backpressure_makes_child_close_refuse_unknown_custody(
    monkeypatch, tmp_path: Path,
):
    """A full completion pipe cannot close the child FD or release its turn lease."""
    fence = backend_retirement.RetirementFence()
    monkeypatch.setattr(backend_retirement, "retirement", fence)
    supervisor, filled, release, close_result, reader_finished = _completion_supervisor(tmp_path)
    reader_release = tmp_path / "completion-reader-release"
    stdout_release = tmp_path / "completion-stdout-release"
    reader_gate_reached = threading.Event()
    stdout_gate_reached = threading.Event()
    completion_padding_drained = threading.Event()
    reader_gate_used = False
    stdout_gate_used = False
    completion_seen = 0
    completion_results: list[bool] = []
    completion_target: list[int | None] = [None]
    original_consume = host_supervisor_streams._consume_buffer
    original_host_frame = supervisor._handle_host_frame
    original_completion = supervisor._handle_producer_completion

    def gated_consume(*args, **kwargs):
        nonlocal reader_gate_used
        result = original_consume(*args, **kwargs)
        if not reader_gate_used and supervisor._startup_event.is_set():
            reader_gate_used = True
            reader_gate_reached.set()
            wait_until(
                reader_release.exists,
                timeout=30,
                what="test-owned completion reader release after startup",
            )
        return result

    def gated_host_frame(frame: dict, *, proc=None, host_generation=None):
        nonlocal stdout_gate_used
        original_host_frame(frame, proc=proc, host_generation=host_generation)
        if not stdout_gate_used and frame.get("type") == "hello":
            stdout_gate_used = True
            stdout_gate_reached.set()
            wait_until(
                stdout_release.exists,
                timeout=30,
                what="test-owned stdout reader release after hello",
            )

    def observed_completion(record: dict, *, proc, host_generation) -> bool:
        nonlocal completion_seen
        result = original_completion(
            record, proc=proc, host_generation=host_generation,
        )
        if record.get("type") == "producer.complete":
            completion_seen += 1
            completion_results.append(result)
            target = completion_target[0]
            if target is not None and completion_seen >= target:
                completion_padding_drained.set()
        return result

    monkeypatch.setattr(host_supervisor_streams, "_consume_buffer", gated_consume)
    monkeypatch.setattr(supervisor, "_handle_host_frame", gated_host_frame)
    monkeypatch.setattr(supervisor, "_handle_producer_completion", observed_completion)
    lease = fence.acquire(
        owner_kind="compute-host-turn", owner_id=f"{tmp_path}:completion-fill", profile_id="",
    )
    callback_frames: list[dict] = []
    callback_done = threading.Event()

    def completion(frame: dict) -> bool:
        callback_frames.append(dict(frame))
        callback_done.set()
        return True

    completion._retirement_lease = lease
    proc = None
    try:
        supervisor.start()
        wait_until(reader_gate_reached.is_set, timeout=15, what="completion reader startup gate")
        wait_until(stdout_gate_reached.is_set, timeout=15, what="stdout reader startup gate")
        profile = tmp_path / "profile"
        profile.mkdir()
        supervisor.submit_turn(_frame(profile, "completion-fill-turn"), on_complete=completion)
        wait_until(filled.exists, timeout=30, what="real completion pipe fill")
        padding_count = int(filled.read_text(encoding="utf-8"))
        assert padding_count > 0
        completion_target[0] = padding_count + 1  # padding plus the original completion write
        pending = supervisor._pending_turns.get("completion-fill-turn")
        assert isinstance(pending, _PendingTurn)
        assert pending.producer_settled is False
        assert callback_frames == []
        assert fence.active_count() == 1
        assert supervisor.capture_ready is False
        assert supervisor._registry_lease is not None
        proc = supervisor._proc
        assert proc is not None
        proc.send_signal(signal.SIGTERM)
        wait_until(close_result.exists, timeout=3, what="bounded child close refusal")
        observed = json.loads(close_result.read_text(encoding="utf-8"))
        assert observed == {
            "result": False,
            "completion_closed": False,
            "completion_fd_present": True,
        }
        assert fence.active_count() == 1
        assert supervisor.capture_ready is False
        assert supervisor._registry_lease is not None
        assert supervisor._pending_turns.get("completion-fill-turn") is pending

        # Release the child writer and its real completion reader first. The valid
        # framed duplicates settle the producer identity while stdout remains gated,
        # so no callback can be opened by an unverified public terminal.
        release.write_text("release", encoding="utf-8")
        reader_release.write_text("release", encoding="utf-8")
        wait_until(
            completion_padding_drained.is_set,
            timeout=30,
            what="valid framed completion padding drained",
        )
        assert completion_results and all(completion_results)
        assert callback_done.is_set() is False

        # Only after the exact source-bound completion record is consumed may the
        # public terminal be admitted. This preserves callback/digest ordering.
        stdout_release.write_text("release", encoding="utf-8")
        wait_until(callback_done.is_set, timeout=30, what="real producer completion callback")
        wait_until(lambda: fence.active_count() == 0, timeout=30,
                   what="exact completion lease release")
        wait_until(lambda: supervisor._pending_turns.get("completion-fill-turn") is None,
                   timeout=30, what="exact completion pending removal")
        wait_until(lambda: proc.poll() is not None, timeout=30,
                   what="natural completion child exit")
        wait_until(reader_finished.exists, timeout=15, what="natural stdin reader settlement")
        assert callback_frames and callback_frames[-1]["type"] in {"turn.end", "turn.error"}
        assert pending.producer_settled is True
        assert pending.callback_succeeded is True
        assert pending.lease_release_pending is False
        wait_until(lambda: supervisor._registry_lease is None, timeout=30,
                   what="natural owner and registry settlement")
        assert proc.returncode == 0
    finally:
        release.write_text("release", encoding="utf-8")
        reader_release.write_text("release", encoding="utf-8")
        stdout_release.write_text("release", encoding="utf-8")
        proc = supervisor._proc
        if proc is not None and proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            with contextlib.suppress(Exception):
                proc.wait(timeout=10)
        supervisor.shutdown()

@pytest.mark.platforms("not windows")
def test_stdout_pipe_backpressure_refuses_deadline_close_until_writer_returns():
    """The public shutdown frame path retains a real stdout writer until the pipe drains."""
    read_fd, write_fd = os.pipe()
    os.set_blocking(write_fd, False)
    shutdown_thread = None
    stdout = None
    try:
        while True:
            try:
                os.write(write_fd, b"x" * 65536)
            except BlockingIOError:
                break
        os.set_blocking(write_fd, True)
        stdout = os.fdopen(write_fd, "w", buffering=1, encoding="utf-8")
        write_fd = -1
        from tui_gateway.compute_host import ComputeHost

        host = ComputeHost(stdout=stdout, completion_fd=None, heartbeat_secs=0)
        shutdown_thread = threading.Thread(
            target=host.handle_frame,
            args=({"type": "shutdown", "request_id": "stdout-fill"},),
            name="real-stdout-shutdown-handler",
        )
        shutdown_thread.start()

        def writer_holds_lock() -> bool:
            acquired = host._write_lock.acquire(blocking=False)
            if acquired:
                host._write_lock.release()
                return False
            return True

        wait_until(writer_holds_lock, timeout=15, what="real public shutdown writer entry")
        deadline = time.monotonic() + 0.25
        assert host.close(deadline=deadline) is False
        assert host._shutdown_complete.is_set() is False

        os.set_blocking(read_fd, False)
        while True:
            try:
                if not os.read(read_fd, 65536):
                    break
            except BlockingIOError:
                break
        shutdown_thread.join(timeout=5)
        assert not shutdown_thread.is_alive()
        assert host._shutdown_complete.is_set()
    finally:
        # Drain the test-owned pipe before joining the writer. Closing a BufferedIO
        # writer while it owns the pipe can block cleanup and hide the causal result.
        with contextlib.suppress(OSError, ValueError):
            os.set_blocking(read_fd, False)
        while read_fd >= 0:
            try:
                if not os.read(read_fd, 65536):
                    break
            except BlockingIOError:
                break
            except OSError:
                break
        if shutdown_thread is not None:
            shutdown_thread.join(timeout=5)
        if shutdown_thread is not None and shutdown_thread.is_alive():
            with contextlib.suppress(OSError):
                os.close(read_fd)
            read_fd = -1
            shutdown_thread.join(timeout=5)
        if stdout is not None:
            stdout.close()
        if write_fd >= 0:
            os.close(write_fd)
        if read_fd >= 0:
            os.close(read_fd)
