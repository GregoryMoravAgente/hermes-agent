"""Causal I13 detached-reaper continuation tests over the existing custody graph."""
from __future__ import annotations

from contextlib import contextmanager
import json
import os
import subprocess
import sys
import textwrap
import threading
import time
from pathlib import Path
from threading import Event

import pytest

# Reuse the original real coordinator/DB fixture through pytest's plugin
# registration; importing the fixture symbol into this module would shadow the
# parameter name in every causal test and create an F811 health hit.
pytest_plugins = ("tests.cron.test_cron_owner_custody",)

from tests.cron.test_cron_owner_custody import (
    _windows_base_environment,
    _windows_base_executable,
)
@contextmanager
def _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch):
    """Real I12/I13 ownership graph with a child held at one event boundary."""
    from cron import executions
    from cron.runtime_custody import reserve_execution
    from gateway.runtime_cron_source_registry import CronAdmissionLease, CronSourceRegistry

    coordinator, _executions, db = custody_runtime
    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-r6-reaper-fault",
        source="builtin", required=True,
    )
    row = executions.create_execution(
        "job-r6-reaper-fault", source="builtin", execution_id=binding.owner_id,
    )
    binding.bind_execution_record(row)
    worker = binding.reserve_external_worker()
    assert executions.mark_execution_handoff_pending(binding.execution_id) is not None
    script = textwrap.dedent(
        f"""
        import json
        import os
        import sys
        from pathlib import Path
        import cron.executions as e

        e.EXECUTIONS_FILE = Path({str(db)!r})
        adopted = e.adopt_claimed_execution({binding.execution_id!r})
        assert adopted is not None
        print(json.dumps({{
            "execution_id": {binding.execution_id!r},
            "pid": os.getpid(),
            "process_started_at": adopted["process_started_at"],
        }}), flush=True)
        assert e.finish_execution(
            {binding.execution_id!r}, success=True, delivery_outcome="delivered",
        ) is not None
        assert sys.stdin.read(1) == "x"
        """
    )
    process = subprocess.Popen(
        [_windows_base_executable(), "-c", script],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        text=True, env=_windows_base_environment(),
    )
    worker.bind_process(process)
    assert process.stdout is not None
    acknowledgement = json.loads(process.stdout.readline())
    worker.activate_external_worker(acknowledgement, process)

    registry = CronSourceRegistry(coordinator)
    monkeypatch.setattr(registry, "_validate_admission_locked", lambda *_args: None)
    monkeypatch.setattr(registry, "_validate_graph_locked", lambda: True)
    admission = CronAdmissionLease(registry, "worker", tmp_path, parent=binding)
    entered = False
    try:
        admission.__enter__()
        entered = True
        worker.expect_reaper()
        assert admission.bind_external_worker_custody(worker, process) is True
        assert admission.detach_jobs_lock_after_handoff().detached is True
        yield {
            "admission": admission,
            "binding": binding,
            "coordinator": coordinator,
            "process": process,
            "registry": registry,
            "worker": worker,
        }
    finally:
        if (
            entered
            and admission._reaper_retry_registered
            and not admission._reaper_settlement_confirmed
            and not getattr(admission, "_r6_preserve_retry", False)
        ):
            admission.stop_detached_worker_reaper_retry("r6_test_cleanup")
            assert admission._reaper_retry_thread_done.wait(timeout=5)
        if process.poll() is None:
            if process.stdin is not None:
                try:
                    process.stdin.write("x")
                    process.stdin.flush()
                    process.stdin.close()
                except (BrokenPipeError, OSError, ValueError):
                    pass
            try:
                process.wait(timeout=5)
            except (OSError, subprocess.SubprocessError, ValueError):
                process.kill()
                process.wait(timeout=5)
        if entered:
            admission.__exit__(None, None, None)


def test_i13_reaper_retries_one_failed_wait_on_same_process(
    custody_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """A failed native wait gets one exact-Popen retry owned by the same lease."""
    from cron.scheduler_detached_worker import reap_terminal_worker_in_background
    from tests.waiting import wait_until

    first_failure = Event()
    retry_entered = Event()
    retry_pending = Event()
    callback_ready = Event()
    with _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch) as state:
        admission = state["admission"]
        process = state["process"]
        worker = state["worker"]
        original_wait = process.wait

        def wait_once(*args, **kwargs):
            if not first_failure.is_set():
                first_failure.set()
                raise OSError("r6 injected first wait failure")
            retry_entered.set()
            try:
                return original_wait(*args, **kwargs)
            except subprocess.TimeoutExpired:
                retry_pending.set()
                raise

        monkeypatch.setattr(process, "wait", wait_once)

        def observe_after_gate():
            callback_ready.wait(timeout=5)
            return worker.observe_terminal()

        reaper = reap_terminal_worker_in_background(
            process, on_done=observe_after_gate,
            custody=worker, admission=admission,
        )
        worker.bind_reaper(reaper)
        assert admission.bind_detached_worker_reaper(worker, process, reaper) is True
        callback_ready.set()
        assert first_failure.wait(timeout=5)
        assert retry_entered.wait(timeout=5)
        assert retry_pending.wait(timeout=5)
        assert worker._unknown is False
        assert admission.active is True
        assert process.poll() is None
        assert process.stdin is not None
        process.stdin.write("x")
        process.stdin.flush()
        process.stdin.close()
        assert first_failure.wait(timeout=5)
        assert retry_entered.wait(timeout=5)
        reaper.join(timeout=5)
        assert not reaper.is_alive()
        assert wait_until(
            lambda: admission._reaper_release_requested
            and admission._reaper_settlement_confirmed,
            timeout=5,
            what="same-Popen reaper settlement receipt",
        )

    # The retry callback can only request release while the outer lease is still
    # retained.  The registry close owner performs the actual external join and
    # exact-lease release; do not treat the pending typed outcome as inactive.
    assert state["registry"].begin_close(1, deadline=time.monotonic() + 5) is True
    assert admission.active is False
    assert worker.settled is True
    assert state["binding"].settled is True


def test_i13_second_failed_wait_keeps_unknown_exact_custody(
    custody_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """A second wait failure is UNKNOWN and cannot start another waiter."""
    from cron.scheduler_detached_worker import reap_terminal_worker_in_background
    from tests.waiting import wait_until

    wait_calls = []
    with _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch) as state:
        admission = state["admission"]
        process = state["process"]
        worker = state["worker"]
        original_wait = process.wait

        def fail_twice(*args, **kwargs):
            wait_calls.append(len(wait_calls) + 1)
            if len(wait_calls) <= 2:
                raise OSError("r6 injected ambiguous wait failure")
            return original_wait(*args, **kwargs)

        monkeypatch.setattr(process, "wait", fail_twice)
        reaper = reap_terminal_worker_in_background(
            process, on_done=lambda: worker.observe_terminal(),
            custody=worker, admission=admission,
        )
        worker.bind_reaper(reaper)
        assert admission.bind_detached_worker_reaper(worker, process, reaper) is True
        assert process.stdin is not None
        process.stdin.write("x")
        process.stdin.flush()
        process.stdin.close()
        assert wait_until(
            lambda: worker._unknown,
            timeout=5,
            what="unknown after second exact wait failure",
        )
        reaper.join(timeout=5)
        assert len(wait_calls) == 2
        assert admission.active is True
        assert admission._reaper_retained is True
        assert worker.settled is False
        assert admission._reaper_retry_registered is True
        admission.stop_detached_worker_reaper_retry("r6_second_wait_failure")
        assert wait_until(
            admission._reaper_retry_thread_done.is_set,
            timeout=5,
            what="failed reaper retry consumer stop",
        )


def test_i13_reaper_registration_failure_has_no_consumer_claim(
    custody_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """Missing registration retains UNKNOWN custody without starting a reaper."""
    from cron.scheduler_detached_worker import reap_terminal_worker_in_background

    registered = Event()
    with _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch) as state:
        admission = state["admission"]
        worker = state["worker"]
        process = state["process"]

        def reject_registration(_retry):
            registered.set()
            return False

        monkeypatch.setattr(admission, "register_detached_worker_reaper_retry", reject_registration)
        with pytest.raises(RuntimeError, match="settlement consumer unavailable"):
            reap_terminal_worker_in_background(
                process, custody=worker, admission=admission,
            )
        assert registered.is_set()
        assert worker._unknown is True
        assert admission._reaper_retained is True
        assert admission._reaper_retry_registered is False
        assert admission._reaper_retry_consumer_status == "not_registered"
        assert admission.registry._reaper_retry_threads == {}
        assert getattr(worker, "_reaper", None) is None


def test_i13_registry_rejection_leaves_custody_reaper_unbound(
    custody_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """Registry validation runs before custody mutation on a rejected bind."""
    from cron.scheduler import _start_external_worker_reaper

    with _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch) as state:
        admission = state["admission"]
        worker = state["worker"]
        process = state["process"]
        before = (getattr(worker, "_reaper", None), worker._reaper_expected)
        monkeypatch.setattr(admission, "bind_detached_worker_reaper", lambda *_args: False)
        with pytest.raises(RuntimeError, match="reaper identity could not be bound"):
            _start_external_worker_reaper(process, worker, admission)
        assert (getattr(worker, "_reaper", None), worker._reaper_expected) == before
        assert worker._unknown is True
        assert admission._reaper_retained is True


def test_i13_reaper_thread_start_ambiguity_retains_exact_lease(
    custody_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """A native reaper start exception retains UNKNOWN custody without a second waiter."""
    from agent import memory_provider
    from cron.scheduler_detached_worker import reap_terminal_worker_in_background
    from tests.waiting import wait_until

    start_failed = Event()
    original_spawn = memory_provider.spawn_context_thread

    def spawn_with_ambiguous_reaper_start(*args, **kwargs):
        thread = original_spawn(*args, **kwargs)
        if str(kwargs.get("name", "")).startswith("cron-worker-reap-") and not start_failed.is_set():
            def ambiguous_start(*_start_args, **_start_kwargs):
                start_failed.set()
                raise OSError("r6 injected native reaper start ambiguity")

            thread.start = ambiguous_start
        return thread

    monkeypatch.setattr(memory_provider, "spawn_context_thread", spawn_with_ambiguous_reaper_start)
    with _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch) as state:
        admission = state["admission"]
        worker = state["worker"]
        process = state["process"]
        with pytest.raises(OSError, match="native reaper start ambiguity"):
            reap_terminal_worker_in_background(
                process, custody=worker, admission=admission,
            )
        assert start_failed.is_set()
        assert worker._unknown is True
        assert admission._reaper_retained is True
        assert admission._reaper_retry_start_status == "unknown:reaper_start"
        assert getattr(worker, "_reaper", None) is None
        assert callable(admission.reconcile_detached_worker_reaper_retry)
        assert wait_until(
            admission._reaper_retry_thread_done.is_set,
            timeout=5,
            what="ambiguous native reaper retry consumer stop",
        )
        assert admission.registry._reaper_retry_threads == {}
        assert admission._reaper_retry_consumer_status == "finished"


def test_i13_ambiguous_retry_thread_start_has_no_live_consumer_claim(
    custody_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """A retry-thread start exception retains the exact lease but no live consumer claim."""
    from agent import memory_provider
    from cron.scheduler_detached_worker import reap_terminal_worker_in_background

    start_failed = Event()
    original_spawn = memory_provider.spawn_context_thread

    def spawn_with_one_ambiguous_start(*args, **kwargs):
        thread = original_spawn(*args, **kwargs)
        if kwargs.get("name") == "cron-reaper-settlement-retry" and not start_failed.is_set():
            def ambiguous_start(*_start_args, **_start_kwargs):
                start_failed.set()
                raise OSError("r6 injected retry-thread start ambiguity")

            thread.start = ambiguous_start
        return thread

    monkeypatch.setattr(memory_provider, "spawn_context_thread", spawn_with_one_ambiguous_start)
    with _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch) as state:
        admission = state["admission"]
        worker = state["worker"]
        process = state["process"]
        with pytest.raises(RuntimeError, match="settlement consumer unavailable"):
            reap_terminal_worker_in_background(
                process, custody=worker, admission=admission,
            )
        assert start_failed.is_set()
        assert worker._unknown is True
        assert admission._reaper_retained is True
        assert admission._reaper_retry_consumer_status == "ambiguous"
        assert admission._reaper_retry_registered is True
        assert admission._reaper_retry_thread_done.is_set()
        assert all(
            not thread.is_alive()
            for thread in admission.registry._reaper_retry_threads.values()
        )
        assert callable(admission.reconcile_detached_worker_reaper_retry)
        assert getattr(worker, "_reaper", None) is None


@pytest.mark.parametrize(
    ("target", "field"),
    [
        ("worker", "_future"),
        ("worker", "_claim_future"),
        ("worker", "_reaper"),
        ("worker", "_script_process"),
        ("worker", "_native_job"),
        ("worker", "_native_job_identity"),
        ("worker", "_scope_proof"),
        ("parent", "_future"),
        ("parent", "_native_job"),
        ("parent", "_scope_proof"),
    ],
)
def test_i13_reaper_rejects_replaced_physical_tuple(
    custody_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target, field,
):
    """A future/reaper/Job/scope replacement cannot pass the retained identity gate."""
    from cron.scheduler_detached_worker import reap_terminal_worker_in_background

    with _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch) as state:
        admission = state["admission"]
        binding = state["binding"]
        process = state["process"]
        worker = state["worker"]
        reaper = reap_terminal_worker_in_background(
            process, on_done=lambda: False,
            custody=worker, admission=admission,
        )
        worker.bind_reaper(reaper)
        assert admission.bind_detached_worker_reaper(worker, process, reaper) is True
        owner = worker if target == "worker" else binding
        original = getattr(owner, field)
        setattr(owner, field, object())
        try:
            assert admission._reaper_identity_matches(worker, process) is False
            assert admission.confirm_detached_worker_settlement(worker, process) is False
            assert admission.active is True
        finally:
            setattr(owner, field, original)
        admission.stop_detached_worker_reaper_retry("r6_physical_replacement")


@pytest.mark.parametrize(
    ("target", "field"),
    [
        ("worker", "_future"),
        ("worker", "_claim_future"),
        ("worker", "_script_process"),
        ("worker", "_scope_proof"),
        ("parent", "_future"),
        ("parent", "_scope_proof"),
    ],
)
def test_i13_reaper_rejects_missing_or_opaque_physical_witness(
    custody_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target, field,
):
    """An absent/default witness cannot be laundered as the captured source tuple."""
    with _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch) as state:
        admission = state["admission"]
        process = state["process"]
        worker = state["worker"]
        owner = worker if target == "worker" else state["binding"]
        original = getattr(owner, field)
        replacement = None if original is not None else object()
        setattr(owner, field, replacement)
        try:
            assert admission._reaper_identity_matches(worker, process) is False
            assert admission.confirm_detached_worker_settlement(worker, process) is False
            assert admission.active is True
        finally:
            setattr(owner, field, original)


@pytest.mark.parametrize(
    ("field", "mutation"),
    [
        ("_script_pipes_closed", "missing"),
        ("_script_pipes_closed", "opaque"),
        ("_native_job_proven", "missing"),
        ("_native_job_proven", "opaque"),
    ],
)
def test_i13_reaper_rejects_missing_or_opaque_boolean_witness(
    custody_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field, mutation,
):
    """Missing or non-boolean script/Job state cannot match explicit False."""
    with _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch) as state:
        admission = state["admission"]
        worker = state["worker"]
        original = getattr(worker, field)
        assert type(original) is bool
        if mutation == "missing":
            delattr(worker, field)
        else:
            setattr(worker, field, object())
        try:
            assert admission._reaper_identity_matches(worker, state["process"]) is False
            assert admission.active is True
        finally:
            setattr(worker, field, original)


def test_i13_reaper_preserves_explicit_false_boolean_witness(
    custody_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """An explicitly captured False remains a valid boolean physical witness."""
    from cron.scheduler_detached_worker import reap_terminal_worker_in_background

    with _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch) as state:
        admission = state["admission"]
        process = state["process"]
        worker = state["worker"]
        assert worker._script_pipes_closed is False
        # ``bind_process`` establishes the worker's native Job witness.  The
        # explicit False witness belongs to the legitimate I12 parent binding;
        # changing the worker flag would forge an active-worker proof.
        assert state["binding"]._native_job_proven is False
        reaper = reap_terminal_worker_in_background(
            process, on_done=lambda: False,
            custody=worker, admission=admission,
        )
        worker.bind_reaper(reaper)
        assert admission.bind_detached_worker_reaper(worker, process, reaper) is True
        assert admission._reaper_identity_matches(worker, process) is True
        admission.stop_detached_worker_reaper_retry("r8_boolean_witness_control")


def test_i13_reaper_release_waits_for_retry_finalizer_without_self_join(
    custody_runtime, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    """Release stays pending until the registry retry thread has finished its finalizer."""
    from cron.scheduler_detached_worker import reap_terminal_worker_in_background
    from tests.waiting import wait_until

    observer_entered = Event()
    allow_observer = Event()
    observations = []
    with _r6_reaper_fault_fixture(custody_runtime, tmp_path, monkeypatch) as state:
        admission = state["admission"]
        process = state["process"]
        worker = state["worker"]
        admission._r6_preserve_retry = True
        original_finish = admission._finish_detached_worker_reaper_release

        def finish_with_barrier():
            done_before = admission._reaper_retry_thread_done.is_set()
            result = original_finish()
            observations.append((done_before, result, threading.current_thread().name))
            return result

        monkeypatch.setattr(admission, "_finish_detached_worker_reaper_release", finish_with_barrier)
        observer_calls = 0

        def observer_after_reap():
            nonlocal observer_calls
            observer_calls += 1
            if observer_calls == 1:
                observer_entered.set()
                assert allow_observer.wait(timeout=5)
            return worker.observe_terminal()

        reaper = reap_terminal_worker_in_background(
            process, on_done=observer_after_reap,
            custody=worker, admission=admission,
        )
        worker.bind_reaper(reaper)
        assert admission.bind_detached_worker_reaper(worker, process, reaper) is True
        assert process.stdin is not None
        process.stdin.write("x")
        process.stdin.flush()
        process.stdin.close()
        assert observer_entered.wait(timeout=5)

    # The external context has closed, but the first callback is still blocked.
    assert admission.active is True
    allow_observer.set()
    assert wait_until(
        admission._reaper_retry_thread_done.is_set,
        timeout=5,
        what="reaper retry finalizer completion event",
    )
    # The external registry owner joins the finished retry consumer and then
    # releases the retained lease.  A direct lease-finalizer call would bypass
    # the production close barrier and misclassify ``reaper_pending``.
    assert state["registry"].begin_close(1, deadline=time.monotonic() + 5) is True
    assert admission.active is False
    assert worker.settled is True
    assert state["binding"].settled is True
    assert any(done is False and result is False for done, result, _name in observations)
    assert any(done is True and result is True for done, result, _name in observations)
    assert all(done or result is False for done, result, _name in observations)
