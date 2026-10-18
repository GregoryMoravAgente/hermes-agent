"""Causal tests for exact RetirementFence leases and reversible close custody."""

import threading
from dataclasses import replace

import pytest


class _CallbackFuture:
    """Small Future contract double for callback and cancellation custody tests."""

    def __init__(self, *, done=False, cancelled=False, attachment_failure=False):
        self._done = done
        self._cancelled = cancelled
        self._attachment_failure = attachment_failure
        self._callbacks = []

    def add_done_callback(self, callback):
        if self._attachment_failure:
            raise RuntimeError("callback attachment refused")
        self._callbacks.append(callback)
        if self._done:
            callback(self)

    def cancelled(self):
        return self._cancelled

    def cancel(self):
        if self._done:
            return False
        self._done = True
        self._cancelled = True
        for callback in list(self._callbacks):
            callback(self)
        return True


def _allow_idle(monkeypatch):
    from hermes_cli import web_server_idle_proof

    monkeypatch.setattr(web_server_idle_proof, "idle_proof", lambda: {"idle": True})


def _blocked_idle(monkeypatch):
    from hermes_cli import web_server_idle_proof

    entered = threading.Event()
    resume = threading.Event()

    def idle_proof():
        entered.set()
        assert resume.wait(10)
        return {"idle": True}

    monkeypatch.setattr(web_server_idle_proof, "idle_proof", idle_proof)
    return entered, resume


def test_callable_finally_releases_exact_lease_when_cleanup_raises():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    lease = fence.acquire(owner_kind="worker", owner_id="cleanup-error")

    def failing_worker():
        raise RuntimeError("worker cleanup failed")

    with pytest.raises(RuntimeError, match="worker cleanup failed"):
        fence.run_with_lease(lease, failing_worker)
    assert fence.active_count() == 0


def test_callback_attachment_failure_while_worker_blocked_keeps_custody_until_callable_terminal():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    lease = fence.acquire(owner_kind="worker", owner_id="attach-failure")
    future = _CallbackFuture(attachment_failure=True)
    assert fence.observe_future_cancellation(future, lease) is False
    assert fence.active_count() == 1

    started = threading.Event()
    resume = threading.Event()

    def worker():
        started.set()
        assert resume.wait(10)

    thread = threading.Thread(target=lambda: fence.run_with_lease(lease, worker))
    thread.start()
    assert started.wait(10)
    assert fence.active_count() == 1
    resume.set()
    thread.join(10)
    assert not thread.is_alive()
    assert fence.active_count() == 0


def test_queued_cancellation_releases_exact_lease_once():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    lease = fence.acquire(owner_kind="queued", owner_id="cancelled")
    future = _CallbackFuture()
    assert fence.observe_future_cancellation(future, lease) is True
    assert fence.active_count() == 1
    assert future.cancel() is True
    assert fence.active_count() == 0
    assert future.cancel() is False
    assert fence.release(lease) is False


def test_immediate_completion_uses_callable_terminal_not_future_done():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    lease = fence.acquire(owner_kind="worker", owner_id="immediate")
    future = _CallbackFuture(done=True)
    assert fence.observe_future_cancellation(future, lease) is True
    assert fence.active_count() == 1
    assert fence.run_with_lease(lease, lambda: "done") == "done"
    assert fence.active_count() == 0


def test_cancellation_cleanup_exception_still_releases_exact_lease():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    lease = fence.acquire(owner_kind="queued", owner_id="cleanup-error")
    future = _CallbackFuture()

    def failing_cancel_cleanup():
        raise RuntimeError("cancel cleanup failed")

    assert fence.observe_future_cancellation(
        future, lease, on_cancel=failing_cancel_cleanup
    ) is True
    assert future.cancel() is True
    assert fence.active_count() == 0


def test_callback_attachment_failure_for_queued_cancel_keeps_unknown_custody_held():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    lease = fence.acquire(owner_kind="queued", owner_id="unknown-cancel")
    future = _CallbackFuture(attachment_failure=True)
    assert fence.observe_future_cancellation(future, lease) is False
    future.cancel()
    # No observer existed, so cancellation is not a proof available to the
    # fence. Keep the exact lease held until an owner supplies terminal proof.
    assert fence.active_count() == 1
    assert fence.release(lease) is True


def test_refused_submission_never_creates_anonymous_custody():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    close = fence.begin_close(owner_kind="submission", owner_id="refused")
    assert close is not None
    assert fence.acquire(owner_kind="queued", owner_id="late") is None
    assert fence.active_count() == 0
    assert fence.commit_close(close) is True
    assert fence.acquire(owner_kind="queued", owner_id="after-commit") is None


def test_release_requires_the_exact_lease_and_never_decrements_a_foreign_holder():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    foreign = RetirementFence()
    lease = fence.acquire(owner_kind="test", owner_id="owner-a")
    foreign_lease = foreign.acquire(owner_kind="test", owner_id="owner-b")

    assert fence.active_count() == 1
    assert fence.release(foreign_lease) is False
    assert fence.release(object()) is False
    assert fence.active_count() == 1
    assert fence.release(lease) is True
    assert fence.release(lease) is False
    assert fence.active_count() == 0
    assert foreign.release(foreign_lease) is True


def test_legacy_prepare_refuses_pending_child_and_never_issues_idle_receipt(monkeypatch):
    from hermes_cli.backend_retirement import RetirementFence

    _allow_idle(monkeypatch)
    fence = RetirementFence()
    parent = fence.acquire(owner_kind="turn", owner_id="turn-pending")
    reservation = fence.reserve_child(parent, owner_kind="continuation", owner_id="turn-pending:child")
    assert reservation is not None
    assert fence.release(parent) is True

    prepared = fence.prepare()
    assert prepared == {"ok": False, "idle": False}
    assert fence.pending_child_count() == 1
    assert fence.commit(prepared.get("token")) == {"ok": False}
    assert fence.release_child_reservation(reservation) is True


def test_legacy_prepare_freezes_new_admission_while_idle_probe_is_paused(monkeypatch):
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    entered, resume = _blocked_idle(monkeypatch)
    result = {}

    def prepare():
        result["value"] = fence.prepare()

    thread = threading.Thread(target=prepare)
    thread.start()
    assert entered.wait(10)
    try:
        # Preparation owns a freeze while the external idle proof is in flight.
        assert fence.acquire(owner_kind="late-root", owner_id="late-root") is None
        assert fence.begin_close(owner_kind="late-close", owner_id="late-close") is None
        assert fence.active_count() == 0
        assert fence.pending_child_count() == 0
    finally:
        resume.set()
    thread.join(10)

    assert not thread.is_alive()
    prepared = result["value"]
    assert prepared["ok"] is True
    assert prepared["idle"] is True
    assert isinstance(prepared["token"], str) and prepared["token"]
    assert fence.commit(prepared["token"]) == {"ok": True}
    assert fence.active_count() == 0
    assert fence.pending_child_count() == 0


def test_reversible_close_is_barrier_with_live_work_and_child_settles_before_commit():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    parent = fence.acquire(owner_kind="turn", owner_id="turn-a")
    reservation = fence.reserve_child(parent, owner_kind="continuation", owner_id="turn-a:child")
    assert reservation is not None

    close = fence.begin_close(owner_kind="session", owner_id="session-a")
    assert close is not None
    assert fence.active_count() == 1
    assert fence.pending_child_count() == 1
    assert fence.acquire(owner_kind="late-root", owner_id="late-root") is None
    assert fence.reserve_child(parent, owner_kind="late-child", owner_id="late-child") is None

    child = fence.activate_child(reservation)
    assert child is not None
    assert child.parent_lease_id == parent.lease_id
    assert fence.active_count() == 2
    assert fence.commit_close(close) is False

    assert fence.release(parent) is True
    assert fence.release(child) is True
    assert fence.active_count() == 0
    assert fence.pending_child_count() == 0
    assert fence.commit_close(close) is True
    assert fence.acquire(owner_kind="new-root", owner_id="new-root") is None
    assert fence.release_close(close) is False


def test_reversible_commit_refuses_active_or_pending_custody():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    parent = fence.acquire(owner_kind="turn", owner_id="turn-held")
    reservation = fence.reserve_child(parent, owner_kind="continuation", owner_id="turn-held:child")
    assert reservation is not None
    close = fence.begin_close(owner_kind="session", owner_id="session-held")
    assert close is not None

    # The barrier may be held with accepted work, but commit cannot claim it is
    # complete while either active or pending child custody remains.
    assert fence.commit_close(close) is False
    assert fence.active_count() == 1
    assert fence.pending_child_count() == 1
    assert fence.release(parent) is True
    assert fence.commit_close(close) is False
    assert fence.pending_child_count() == 1
    assert fence.release_child_reservation(reservation) is True
    assert fence.commit_close(close) is True


def test_committed_reversible_close_replays_through_legacy_prepare_commit():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    receipt = fence.begin_close(owner_kind="session", owner_id="session-recovery")
    assert receipt is not None
    assert fence.commit_close(receipt) is True

    recovered = fence.prepare()
    assert recovered == {"ok": True, "idle": True, "token": receipt.token}
    assert fence.commit(recovered["token"]) == {"ok": True}
    assert fence.commit(receipt) == {"ok": True}
    assert fence.cancel(recovered["token"]) == {"ok": False}
    assert fence.acquire(owner_kind="late-root", owner_id="late-root") is None
    assert fence.active_count() == 0
    assert fence.pending_child_count() == 0


def test_uncommitted_reversible_receipt_cannot_bypass_legacy_prepare_or_commit():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    receipt = fence.begin_close(owner_kind="session", owner_id="session-uncommitted")
    assert receipt is not None
    assert fence.prepare() == {"ok": False, "idle": False}
    assert fence.commit(receipt.token) == {"ok": False}
    assert fence.commit(receipt) == {"ok": False}
    assert fence.release_close(receipt) is True
    assert fence.commit(receipt.token) == {"ok": False}


def test_committed_close_rejects_foreign_stale_and_copied_tokens():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    foreign = RetirementFence()
    stale = fence.begin_close(owner_kind="session", owner_id="stale")
    assert stale is not None
    assert fence.release_close(stale) is True
    current = fence.begin_close(owner_kind="session", owner_id="current")
    foreign_receipt = foreign.begin_close(owner_kind="session", owner_id="foreign")
    assert current is not None and foreign_receipt is not None
    assert fence.commit_close(current) is True

    assert fence.commit(stale.token) == {"ok": False}
    assert fence.commit(stale) == {"ok": False}
    assert fence.commit(replace(current)) == {"ok": False}
    assert fence.commit(foreign_receipt.token) == {"ok": False}
    assert fence.commit(foreign_receipt) == {"ok": False}
    assert fence.commit(current.token) == {"ok": True}
    assert foreign.release_close(foreign_receipt) is True


def test_close_release_rejects_stale_copied_and_double_receipts():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    first = fence.begin_close(owner_kind="session", owner_id="first")
    assert first is not None
    assert fence.release_close(first) is True
    second = fence.begin_close(owner_kind="session", owner_id="second")
    assert second is not None
    assert fence.release_close(first) is False
    assert fence.release_close(replace(second)) is False
    assert fence.release_close(second) is True
    assert fence.release_close(second) is False


def test_reversible_close_deadline_does_not_reopen_unknown_custody(monkeypatch):
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    live = fence.acquire(owner_kind="turn", owner_id="live")
    close = fence.begin_close(owner_kind="session", owner_id="deadline", deadline_s=0)
    assert close is not None
    fence._now = lambda: close.deadline + 1

    # The deadline bounds the close operation; it is not evidence that live work settled.
    assert fence.acquire(owner_kind="new-root", owner_id="blocked") is None
    assert fence.active_count() == 1
    assert fence.release_close(close) is True
    assert fence.active_count() == 1
    assert fence.release(live) is True
    reopened = fence.acquire(owner_kind="new-root", owner_id="after-abort")
    assert reopened is not None
    assert fence.release(reopened) is True


def test_close_owner_and_foreign_release_race_is_locked_and_stale_safe():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    foreign = RetirementFence()
    close = fence.begin_close(owner_kind="owner", owner_id="close-a")
    foreign_close = foreign.begin_close(owner_kind="owner", owner_id="close-b")
    assert close is not None and foreign_close is not None

    barrier = threading.Barrier(2)
    outcomes = {}

    def release_owner():
        barrier.wait(10)
        outcomes["owner"] = fence.release_close(close)

    def release_foreign():
        barrier.wait(10)
        outcomes["foreign"] = fence.release_close(foreign_close)

    threads = [threading.Thread(target=release_owner), threading.Thread(target=release_foreign)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    assert all(not thread.is_alive() for thread in threads)
    assert outcomes == {"owner": True, "foreign": False}
    assert fence.release_close(close) is False


def test_release_before_close_race_never_loses_the_close_transition():
    from hermes_cli.backend_retirement import RetirementFence

    fence = RetirementFence()
    parent = fence.acquire(owner_kind="turn", owner_id="turn-race")
    barrier = threading.Barrier(2)
    outcomes = {}

    def begin():
        barrier.wait(10)
        outcomes["close"] = fence.begin_close(owner_kind="session", owner_id="race")

    def release():
        barrier.wait(10)
        outcomes["release"] = fence.release(parent)

    threads = [threading.Thread(target=begin), threading.Thread(target=release)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    assert all(not thread.is_alive() for thread in threads)
    assert outcomes["release"] is True
    close = outcomes["close"] or fence.begin_close(owner_kind="session", owner_id="race")
    assert close is not None
    assert fence.release_close(close) is True
