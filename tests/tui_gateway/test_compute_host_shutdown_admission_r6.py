"""Causal child admission races for the real ComputeHost executor boundary."""

from __future__ import annotations

import io
import json
from pathlib import Path
import threading

from tui_gateway.compute_host import ComputeHost


def _frame(tmp_path: Path, *, sid: str, request_id: str) -> dict[str, object]:
    return {
        "type": "turn.start",
        "sid": sid,
        "request_id": request_id,
        "turn_id": request_id,
        "session_key": "r6-session",
        "profile_home": str(tmp_path),
        "text": "r6 admission race",
        "history": [],
        "history_version": 0,
        "cols": 80,
        "cwd": str(tmp_path),
        "source": "desktop",
    }


def _frames(output: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in output.getvalue().splitlines() if line.strip()]


def test_shutdown_wins_before_child_turn_submit(monkeypatch, tmp_path: Path):
    """A shutdown flag observed under the child lock refuses a frame before submit."""
    output = io.StringIO()
    host = ComputeHost(stdout=output, max_workers=1, heartbeat_secs=0)
    submit_called = threading.Event()
    drain_entered = threading.Event()
    release_drain = threading.Event()
    shutdown_done = threading.Event()
    turn_done = threading.Event()
    errors: list[str] = []

    original_submit = host._executor.submit

    def observe_submit(*args, **kwargs):
        submit_called.set()
        return original_submit(*args, **kwargs)

    monkeypatch.setattr(host._executor, "submit", observe_submit)
    original_drain = host._drain_and_flush

    def gated_drain(*, reason: str, wait: float, **kwargs) -> bool:
        drain_entered.set()
        assert release_drain.wait(5)
        return original_drain(reason=reason, wait=wait, **kwargs)

    monkeypatch.setattr(host, "_drain_and_flush", gated_drain)

    def stop() -> None:
        try:
            host.shutdown(reason="sigterm", wait=5)
        except Exception as exc:  # test thread must surface a failed lifecycle call
            errors.append(type(exc).__name__)
        finally:
            shutdown_done.set()

    def submit_frame() -> None:
        try:
            host.handle_frame(_frame(tmp_path, sid="r6-refused", request_id="r6-refused"))
        except Exception as exc:  # test thread must surface a failed lifecycle call
            errors.append(type(exc).__name__)
        finally:
            turn_done.set()

    stopper = threading.Thread(target=stop, name="r6-shutdown-wins")
    submitter = threading.Thread(target=submit_frame, name="r6-submit-after-shutdown")
    stopper.start()
    try:
        assert drain_entered.wait(5)
        assert host._shutdown_requested.is_set()
        assert host._closed.is_set()
        submitter.start()
        release_drain.set()
        assert shutdown_done.wait(5)
        assert turn_done.wait(5)
        stopper.join(timeout=1)
        submitter.join(timeout=1)
        assert not stopper.is_alive()
        assert not submitter.is_alive()
        assert errors == []
        assert submit_called.is_set() is False
        frames = _frames(output)
        assert len(frames) == 1
        refusal = frames[0]
        assert refusal["type"] == "turn.error"
        assert refusal["sid"] == "r6-refused"
        assert refusal["request_id"] == "r6-refused"
        assert refusal["turn_id"] == "r6-refused"
        assert refusal["session_key"] == "r6-session"
        assert refusal["origin_session_key"] == "r6-session"
        assert refusal["profile_home"] == str(tmp_path)
        assert refusal["reason"] == "shutdown_requested"
        assert refusal["message"] == "compute host is shutting down"
        assert host._turn_futures == {}
    finally:
        release_drain.set()
        stopper.join(timeout=5)
        if submitter.ident is not None:
            submitter.join(timeout=5)
        host.close()


def test_shutdown_waits_for_tracked_future_after_submit(monkeypatch, tmp_path: Path):
    """A submitted real future is tracked before shutdown can drain or close."""
    output = io.StringIO()
    host = ComputeHost(stdout=output, max_workers=1, heartbeat_secs=0)
    worker_started = threading.Event()
    release_worker = threading.Event()
    worker_finished = threading.Event()
    submit_returned = threading.Event()
    track_entered = threading.Event()
    release_track = threading.Event()
    future_tracked = threading.Event()
    drain_entered = threading.Event()
    shutdown_invoked = threading.Event()
    shutdown_done = threading.Event()
    turn_done = threading.Event()
    errors: list[str] = []
    submitted = []

    original_run = host._run_real_turn

    def gated_run(frame: dict) -> None:
        worker_started.set()
        assert release_worker.wait(5)
        try:
            # Keep the production turn path after the deterministic executor barrier.
            original_run(frame)
        finally:
            worker_finished.set()

    monkeypatch.setattr(host, "_run_real_turn", gated_run)
    original_submit = host._executor.submit

    def observe_submit(*args, **kwargs):
        future = original_submit(*args, **kwargs)
        submitted.append(future)
        submit_returned.set()
        return future

    monkeypatch.setattr(host._executor, "submit", observe_submit)
    original_track = host._track_turn_future

    def gated_track(future, sid: str) -> None:
        track_entered.set()
        assert release_track.wait(5)
        original_track(future, sid)
        future_tracked.set()

    monkeypatch.setattr(host, "_track_turn_future", gated_track)
    original_drain = host._drain_and_flush

    def observe_drain(*, reason: str, wait: float, **kwargs) -> bool:
        drain_entered.set()
        return original_drain(reason=reason, wait=wait, **kwargs)

    monkeypatch.setattr(host, "_drain_and_flush", observe_drain)

    def submit_frame() -> None:
        try:
            # An empty sid exercises the production invalid-request terminal path after
            # the real executor has accepted the future; it avoids provider setup.
            host.handle_frame(_frame(tmp_path, sid="", request_id="r6-tracked"))
        except Exception as exc:  # test thread must surface a failed lifecycle call
            errors.append(type(exc).__name__)
        finally:
            turn_done.set()

    def stop() -> None:
        shutdown_invoked.set()
        try:
            host.shutdown(reason="sigterm", wait=5)
        except Exception as exc:  # test thread must surface a failed lifecycle call
            errors.append(type(exc).__name__)
        finally:
            shutdown_done.set()

    submitter = threading.Thread(target=submit_frame, name="r6-submit-wins")
    stopper = threading.Thread(target=stop, name="r6-drain-tracked-future")
    submitter.start()
    try:
        assert submit_returned.wait(5)
        assert track_entered.wait(5)
        assert worker_started.wait(5)
        assert submitted
        future = submitted[0]
        assert future.done() is False
        stopper.start()
        assert shutdown_invoked.wait(5)
        assert shutdown_done.is_set() is False
        assert host._shutdown_complete.is_set() is False

        release_track.set()
        assert future_tracked.wait(5)
        assert turn_done.wait(5)
        with host._turn_futures_lock:
            assert host._turn_futures.get(future) == ""
        assert future.done() is False
        assert drain_entered.wait(5)
        assert shutdown_done.is_set() is False
        assert host._shutdown_complete.is_set() is False

        release_worker.set()
        assert worker_finished.wait(5)
        assert future.result(timeout=5) is None
        assert shutdown_done.wait(5)
        stopper.join(timeout=1)
        submitter.join(timeout=1)
        assert not stopper.is_alive()
        assert not submitter.is_alive()
        assert errors == []
        assert future.done() is True
        with host._turn_futures_lock:
            assert future not in host._turn_futures
    finally:
        release_track.set()
        release_worker.set()
        if stopper.ident is not None:
            stopper.join(timeout=5)
        submitter.join(timeout=5)
        host.close()
