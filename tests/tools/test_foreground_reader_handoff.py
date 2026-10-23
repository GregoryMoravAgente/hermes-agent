"""Host-independent custody/collector failure controls for foreground reader transfer.

Native Windows launch/Job/EOF and real LocalEnvironment delivery remain in
``test_process_registry_windows_custody.py``.  These controls exercise only the
existing collector and registry state machine so callback failures cannot be
silently turned into a successful handoff on any host.
"""
from __future__ import annotations

import threading
from types import SimpleNamespace

import pytest

from tools.environments.base_output import _BoundedOutputCollector
from tools.process_registry import ProcessRegistry, ProcessSession
from tools.process_registry_custody import ForegroundReaderTransfer
from tools.process_registry_foreground_retry import (
    can_reconcile_unknown, managed_retry_identity_valid,
)


def _transfer(collector, *, phase="streaming"):
    token = object()
    transfer = ForegroundReaderTransfer(
        transaction=object(),
        process=object(),
        reader_thread=object(),
        reader_stop=SimpleNamespace(is_set=lambda: False),
        output=collector,
        evidence={"reader_lease": True},
        deadline=1.0,
        phase=phase,
        collector_token=token,
    )
    return transfer


def test_collector_abort_drops_staged_chunks_without_callback():
    collector = _BoundedOutputCollector(1024)
    receipt = collector.begin_reader_handoff()
    collector.append("staged")
    delivered = []
    assert collector.abort_reader_handoff(receipt.token) is True
    collector.append("after-abort")
    assert delivered == []


def test_collector_callback_failure_is_sticky_and_keeps_unconsumed_chunks():
    collector = _BoundedOutputCollector(1024)
    receipt = collector.begin_reader_handoff()
    collector.append("first")
    collector.append("second")
    delivered = []

    def failing_sink(chunk):
        delivered.append(chunk)
        raise RuntimeError("sink failed after ownership began")

    with pytest.raises(RuntimeError, match="sink failed"):
        collector.commit_reader_handoff(receipt.token, failing_sink)
    collector.append("late")
    state = collector._reader_handoff
    assert state is not None
    assert state.phase == "unknown"
    assert delivered == ["first"]
    assert state.queue == []
    assert state.uncertain_chars == len("first")
    assert state.pending_chars == len("second") + len("late")
    assert state.pending_dropped is True
    assert collector.abort_reader_handoff(receipt.token) is False


def test_collector_reentrant_sink_does_not_deadlock_or_abort_generation():
    collector = _BoundedOutputCollector(1024)
    receipt = collector.begin_reader_handoff()
    collector.append("first")
    collector.append("second")
    delivered = []
    nested = {"done": False}

    def reentrant_sink(chunk):
        delivered.append(chunk)
        if not nested["done"]:
            nested["done"] = True
            collector.append("nested")

    collector.commit_reader_handoff(receipt.token, reentrant_sink)
    assert delivered == ["first", "second", "nested"]
    assert collector._reader_handoff is not None
    assert collector._reader_handoff.phase == "committed"


def test_collector_serializes_staged_flush_before_a_late_chunk():
    collector = _BoundedOutputCollector(1024)
    receipt = collector.begin_reader_handoff()
    collector.append("first")
    callback_started, release_callback = threading.Event(), threading.Event()
    delivered = []

    def sink(chunk):
        delivered.append(chunk)
        if chunk == "first":
            callback_started.set()
            assert release_callback.wait(5)

    commit = threading.Thread(
        target=lambda: collector.commit_reader_handoff(receipt.token, sink), daemon=True,
    )
    commit.start()
    assert callback_started.wait(5)
    late = threading.Thread(target=lambda: collector.append("late"), daemon=True)
    late.start()
    release_callback.set()
    commit.join(5)
    late.join(5)
    assert not commit.is_alive()
    assert not late.is_alive()
    assert delivered == ["first", "late"]


def test_registry_keeps_one_session_running_while_transfer_is_unresolved():
    collector = _BoundedOutputCollector(1024)
    transfer = _transfer(collector, phase="unknown")
    class NoPoll:
        def poll(self):
            raise AssertionError("reconcile must not poll an unresolved transferred process")

    session = ProcessSession(
        id="proc_transfer", command="true", process=NoPoll(),
        custody=SimpleNamespace(foreground_transfer=transfer),
    )
    registry = object.__new__(ProcessRegistry)
    assert registry._foreground_reader_reconcile_guard(session) is True
    registry._reconcile_local_exit(session)
    transfer.phase = "finalizer_start_failed"
    retried = []
    registry._retry_transferred_foreground_reader = lambda current: retried.append(current) or False
    assert registry._foreground_reader_reconcile_guard(session) is True
    assert retried == [session]
    transfer.phase = "terminal"
    assert registry._foreground_reader_blocks_finish(session) is False


def test_unresolved_reader_blocks_finish_release_and_kill_claimants():
    collector = _BoundedOutputCollector(1024)
    transfer = _transfer(collector, phase="unknown")

    class Stream:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    process = SimpleNamespace(stdout=Stream(), stderr=Stream(), stdin=Stream())
    session = ProcessSession(
        id="proc_transfer_guard", command="true", process=process,
        custody=SimpleNamespace(foreground_transfer=transfer),
    )
    registry = ProcessRegistry()
    registry._running[session.id] = session
    assert registry._move_to_finished(session) is False
    assert registry._release_finished_handles(session) is False
    assert process.stdout.closed is False
    assert registry.kill_process(session.id)["status"] == "error"
    assert session.id in registry._running


def test_unrelated_unknown_reason_stays_sticky_after_native_empty():
    """A native-empty observation cannot repair an unrelated finalizer failure."""
    custody = SimpleNamespace(
        state="unknown",
        reason="foreground_finalizer_failed",
        native_scope=SimpleNamespace(native_empty_observed=True),
    )
    assert can_reconcile_unknown(custody, None, None) is False
    close_only = SimpleNamespace(
        state="unknown",
        reason="join_failed",
        native_scope=SimpleNamespace(native_empty_observed=True, state="resumed"),
    )
    assert can_reconcile_unknown(close_only, None, None) is False


def _managed_retry_policy_fixture():
    process = object()
    job_handle = object()
    job = SimpleNamespace(_recovery_only=False, _handle=job_handle)
    handle = SimpleNamespace(
        owner_id="owner", profile_id="profile", process_instance="instance",
        authority_epoch=7, close_generation=2, coordinator_id="coordinator",
    )
    coordinator = SimpleNamespace(
        _coordinator_id="coordinator", process_instance="instance", authority_epoch=7,
    )
    binding = SimpleNamespace(
        session_id="conversation", session_key="session", tool_call_id="call",
        turn_id="turn", run_generation=3,
    )
    custody = SimpleNamespace(
        state="unknown", reason="managed_termination_failed", handle=handle,
        foreground_binding=binding, coordinator=coordinator, native_scope=None,
        owner_id="owner", profile_id="profile", process_instance="instance",
        authority_epoch=7,
    )

    def matches(candidate_process, pid, host_start_time):
        return candidate_process is process and pid == 4242 and host_start_time == 17

    transaction = SimpleNamespace(
        state="termination_failed", termination_retryable=True,
        termination_failures=1, termination_attempts=1, process=process,
        custody=custody, assigned=True, resumed=True, job=job, job_handle_closed=False,
        termination_job=job, termination_job_handle=job_handle, pid=4242,
        host_start_time=17, matches=matches,
    )
    transaction._foreground_custody_record = SimpleNamespace(binding=binding)
    custody.native_scope = transaction
    session = SimpleNamespace(
        id="proc_generated", session_key="session", parent_session_id="conversation",
        process=process, custody=custody,
    )
    transfer = SimpleNamespace(
        phase="unknown", transaction=transaction, process=process,
        kill_source="user.stop", kill_consume_output=True,
    )
    return transfer, custody, session


@pytest.mark.parametrize(
    "mutation", ["missing_binding", "wrong_parent_domain", "changed_handle", "second_failure"],
)
def test_managed_retry_rejects_missing_or_changed_retained_identity(mutation):
    """A retry cannot repair missing binding, parent-domain drift, handle drift, or a second failure."""
    transfer, custody, session = _managed_retry_policy_fixture()
    if mutation == "missing_binding":
        custody.foreground_binding = None
    elif mutation == "wrong_parent_domain":
        custody.foreground_binding.session_id = session.id
    elif mutation == "changed_handle":
        transfer.transaction.termination_job_handle = object()
    else:
        transfer.transaction.termination_failures = 2
        transfer.transaction.termination_attempts = 2

    assert managed_retry_identity_valid(
        transfer, custody, session, "user.stop", consume_output=True,
    ) is False
