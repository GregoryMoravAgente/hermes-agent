"""Causal I12/I13/I14 custody tests over the existing cron ledger and gateway coordinator."""
from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from threading import Event

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence


@pytest.fixture
def custody_runtime(monkeypatch, tmp_path):
    from cron import executions
    from cron import runtime_custody
    import hermes_constants

    db = tmp_path / "executions.db"
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", db)
    monkeypatch.setattr(hermes_constants, "get_hermes_home", lambda: tmp_path)
    coordinator = RuntimeCustodyCoordinator(
        process_instance="cron-custody-test", authority_epoch=11, fence=RetirementFence())
    runner = SimpleNamespace(
        config=SimpleNamespace(
            multiplex_profiles=True,
            _runtime_profile_homes=[("cron-test", tmp_path)],
        ),
        _primary_profile_name="cron-test",
    )
    monkeypatch.setattr(
        runtime_custody,
        "_runtime",
        lambda: (
            runner, coordinator,
            {"instance_id": "cron-custody-test", "authority_epoch": 11},
            tmp_path,
            11,
        ),
    )
    try:
        yield coordinator, executions, db
    finally:
        # The birth-fingerprint negative case intentionally leaves an unknown binding. Clear only
        # this process-local index so a later test cannot inherit that refusal state.
        with runtime_custody._BINDINGS_LOCK:
            runtime_custody._BINDINGS_BY_EXECUTION.clear()
        with runtime_custody._SCOPE_PROOF_LOCK:
            runtime_custody._SETTLED_SCOPE_PROOFS.clear()


def _claimed(binding, executions, job_id="job-1"):
    record = executions.create_execution(
        job_id, source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(record)
    assert executions.mark_execution_running(binding.execution_id) is not None
    return record


def _windows_base_executable():
    """Return the real interpreter for a direct native-child fixture.

    uv-managed Windows environments commonly expose a launcher shim as ``sys.executable``;
    ``sys._base_executable`` is the interpreter that executes the child in the direct case.
    """
    if sys.platform == "win32":
        return getattr(sys, "_base_executable", None) or sys.executable
    return sys.executable


def _windows_base_environment():
    """Overlay the real PM dependency generation on the direct base interpreter.

    The Windows PM launcher can expose a ``Scripts/python.exe`` shim whose
    ``sys._base_executable`` is the store interpreter.  Launching that base
    executable directly keeps the same-PID/native-Job contract, but it drops the
    managed ``site-packages`` path unless the child receives the production cron
    overlay.  Use the source-owned helper so this fixture selects the committed
    generation rather than an ambient or stale in-tree environment.
    """
    environment = dict(os.environ)
    if sys.platform == "win32":
        from cron.scheduler_worker_env import pin_hermes_tree_on_pythonpath

        pin_hermes_tree_on_pythonpath(environment, Path(__file__).resolve().parents[2])
    return environment


def test_i12_holds_real_future_until_terminal_delivery_and_teardown(custody_runtime):
    coordinator, executions, _db = custody_runtime
    from cron.runtime_custody import reserve_execution

    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-1", source="builtin", required=True)
    _claimed(binding, executions)
    release = Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(release.wait)
        binding.bind_future(future)
        assert executions.finish_execution(
            binding.execution_id, success=True, delivery_outcome="delivered") is not None
        assert not binding.observe_terminal()
        release.set()
        assert future.result(timeout=2) is True
        assert binding.observe_terminal()
        assert binding.settled
    assert coordinator.status()["active_owner_classes"] == []


def test_i12_queued_delivery_wakes_only_after_existing_delivery_ledger_settles(custody_runtime):
    _coordinator, executions, _db = custody_runtime
    from cron.runtime_custody import notify_execution_delivery, reserve_execution

    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-queued", source="builtin", required=True)
    _claimed(binding, executions, "job-queued")
    assert executions.finish_execution(
        binding.execution_id, success=True, delivery_outcome="queued") is not None
    assert not binding.observe_terminal()
    assert not binding.settled
    assert executions.settle_delivery_outcome(binding.execution_id, "delivered")
    notify_execution_delivery(binding.execution_id)
    assert binding.settled


def test_i13_real_external_worker_ack_process_and_terminal_are_one_identity(custody_runtime):
    _coordinator, executions, db = custody_runtime
    from cron.runtime_custody import reserve_execution

    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-external", source="builtin", required=True)
    record = executions.create_execution(
        "job-external", source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(record)
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
        assert e.adopt_claimed_execution({binding.execution_id!r}) is not None
        print(json.dumps({{"execution_id": {binding.execution_id!r}, "pid": os.getpid()}}), flush=True)
        assert e.finish_execution({binding.execution_id!r}, success=True, delivery_outcome="delivered") is not None
        assert sys.stdin.read(1) == "x"
        """
    )
    process = subprocess.Popen(
        [_windows_base_executable(), "-c", script], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        text=True, env=_windows_base_environment())
    worker.bind_process(process)
    assert process.stdout is not None
    acknowledgement = json.loads(process.stdout.readline())
    worker.activate_external_worker(acknowledgement, process)
    assert process.stdin is not None
    process.stdin.write("x")
    process.stdin.close()
    from cron.scheduler_detached_worker import reap_terminal_worker_in_background

    reaper = reap_terminal_worker_in_background(process)
    worker.bind_reaper(reaper)
    reaper.join(timeout=5)
    assert not reaper.is_alive()
    assert worker.observe_terminal()
    assert binding.observe_terminal()
    assert binding.settled


def test_i13_terminal_row_before_ready_ack_keeps_exact_worker_identity(custody_runtime):
    """A real child may commit its terminal row before its ready file is consumed."""
    _coordinator, executions, db = custody_runtime
    from cron.runtime_custody import reserve_execution

    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-fast-worker", source="builtin", required=True)
    record = executions.create_execution(
        "job-fast-worker", source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(record)
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
        assert e.finish_execution({binding.execution_id!r}, success=True, delivery_outcome="delivered") is not None
        print(json.dumps({{
            "execution_id": {binding.execution_id!r},
            "pid": os.getpid(),
            "process_started_at": adopted["process_started_at"],
            "dispatch_mode": "direct",
        }}), flush=True)
        """
    )
    process = subprocess.Popen(
        [_windows_base_executable(), "-c", script], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        text=True, env=_windows_base_environment())
    worker.bind_process(process)
    try:
        assert process.stdout is not None
        acknowledgement = json.loads(process.stdout.readline())
        # The child has already committed its terminal row and exited. Activation must use the
        # Popen identity captured before exit; a fresh live-PID probe would reject this ordering.
        process.wait(timeout=5)
        worker.activate_external_worker(acknowledgement, process, dispatch_mode="direct")
        from cron.scheduler_detached_worker import reap_terminal_worker_in_background

        reaper = reap_terminal_worker_in_background(process)
        worker.bind_reaper(reaper)
        reaper.join(timeout=5)
        assert not reaper.is_alive()
        assert worker.observe_terminal()
        assert binding.observe_terminal()
        assert binding.settled
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)


def test_i13_reconcile_terminal_child_uses_parent_captured_identity(custody_runtime):
    """A terminal row can be recovered only from the Popen identity captured before exit."""
    _coordinator, executions, db = custody_runtime
    from cron.runtime_custody import reserve_execution

    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-reconcile", source="builtin", required=True)
    record = executions.create_execution(
        "job-reconcile", source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(record)
    worker = binding.reserve_external_worker()
    assert executions.mark_execution_handoff_pending(binding.execution_id) is not None
    script = textwrap.dedent(
        f"""
        import sys
        from pathlib import Path
        import cron.executions as e
        e.EXECUTIONS_FILE = Path({str(db)!r})
        assert e.adopt_claimed_execution({binding.execution_id!r}) is not None
        print("ready", flush=True)
        assert sys.stdin.read(1) == "x"
        assert e.finish_execution({binding.execution_id!r}, success=True, delivery_outcome="delivered") is not None
        """
    )
    process = subprocess.Popen(
        [_windows_base_executable(), "-c", script], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=_windows_base_environment())
    worker.bind_process(process)
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "ready"
        assert process.stdin is not None
        process.stdin.write("x")
        process.stdin.close()
        process.wait(timeout=5)
        assert worker.reconcile_unacknowledged(process, dispatch_mode="direct") == "terminalized"
        from cron.scheduler_detached_worker import reap_terminal_worker_in_background

        callback_done = Event()
        callback_ready = Event()

        def _on_reaped():
            callback_ready.wait(timeout=5)
            callback_done.set()
            worker.observe_terminal()

        reaper = reap_terminal_worker_in_background(process, on_done=_on_reaped)
        worker.bind_reaper(reaper)
        callback_ready.set()
        reaper.join(timeout=5)
        assert not reaper.is_alive()
        assert callback_done.wait(timeout=5)
        assert worker.observe_terminal()
        assert binding.observe_terminal()
        assert binding.settled
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)


@pytest.mark.platforms("windows")
def test_i13_windows_job_owns_real_cron_child_until_reap(custody_runtime):
    """Windows qualification uses the existing native Job helper, never a PID census or mock."""
    _coordinator, executions, db = custody_runtime
    from cron.runtime_custody import reserve_execution
    from hermes_cli.local_runtime.processes import spawn_server
    from tui_gateway.windows_owner_recovery import new_windows_job_name

    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-windows", source="builtin", required=True)
    record = executions.create_execution(
        "job-windows", source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(record)
    worker = binding.reserve_external_worker()
    assert executions.mark_execution_handoff_pending(binding.execution_id) is not None
    script = textwrap.dedent(
        f"""
        import sys
        from pathlib import Path
        import cron.executions as e
        e.EXECUTIONS_FILE = Path({str(db)!r})
        assert e.adopt_claimed_execution({binding.execution_id!r}) is not None
        print("ready", flush=True)
        assert sys.stdin.read(1) == "x"
        assert e.finish_execution({binding.execution_id!r}, success=True, delivery_outcome="delivered") is not None
        """
    )
    process, native_job = spawn_server(
        [_windows_base_executable(), "-c", script], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        env=_windows_base_environment(),
        owner_job_name=new_windows_job_name())
    worker.bind_process(process, native_job=native_job)
    try:
        assert process.stdout is not None
        assert process.stdout.readline().strip() == "ready"
        assert process.stdin is not None
        process.stdin.write("x")
        process.stdin.close()
        process.wait(timeout=5)
        assert worker.reconcile_unacknowledged(process, dispatch_mode="direct") == "terminalized"
        from cron.scheduler_detached_worker import reap_terminal_worker_in_background

        reaper = reap_terminal_worker_in_background(process)
        worker.bind_reaper(reaper)
        reaper.join(timeout=5)
        assert not reaper.is_alive()
        assert worker.observe_terminal()
        assert binding.observe_terminal()
        assert binding.settled
        assert worker._native_job_proven is True
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        if native_job is not None:
            native_job.close()


def _windows_split_fixture(custody_runtime, tmp_path, *, with_job, hold_after_terminal=False):
    """Start a real root/worker split; the root waits while the worker owns the ledger."""
    _coordinator, executions, db = custody_runtime
    from cron.runtime_custody import reserve_execution

    tmp_path.mkdir(parents=True, exist_ok=True)
    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-windows-split", source="builtin", required=True)
    executions.create_execution(
        "job-windows-split", source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(executions.get_execution(binding.owner_id))
    worker = binding.reserve_external_worker()
    assert executions.mark_execution_handoff_pending(binding.execution_id) is not None
    ack_path = tmp_path / "worker-ack.json"
    release_path = tmp_path / "worker-release"
    worker_code = textwrap.dedent(
        f"""
        import json, os, time
        from pathlib import Path
        import cron.executions as e
        e.EXECUTIONS_FILE = Path({str(db)!r})
        adopted = e.adopt_claimed_execution({binding.execution_id!r})
        assert adopted is not None
        Path({str(ack_path)!r}).write_text(json.dumps({{
            "execution_id": {binding.execution_id!r},
            "pid": os.getpid(),
            "process_started_at": adopted["process_started_at"],
            "dispatch_mode": "degraded",
            "scope_unit": None,
        }}), encoding="utf-8")
        if {hold_after_terminal!r}:
            assert e.finish_execution({binding.execution_id!r}, success=True, delivery_outcome="delivered") is not None
            while not Path({str(release_path)!r}).exists():
                time.sleep(0.01)
        else:
            while not Path({str(release_path)!r}).exists():
                time.sleep(0.01)
            assert e.finish_execution({binding.execution_id!r}, success=True, delivery_outcome="delivered") is not None
        """
    )
    launcher_code = textwrap.dedent(
        f"""
        import subprocess, sys
        worker = subprocess.Popen([sys.executable, "-c", {worker_code!r}])
        print(worker.pid, flush=True)
        raise SystemExit(worker.wait())
        """
    )
    native_job = None
    process = None
    try:
        if with_job:
            from hermes_cli.local_runtime.processes import spawn_server
            from tui_gateway.windows_owner_recovery import new_windows_job_name
            process, native_job = spawn_server(
                [sys.executable, "-c", launcher_code],
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, owner_job_name=new_windows_job_name(),
            )
        else:
            process = subprocess.Popen(
                [sys.executable, "-c", launcher_code],
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True,
            )
        worker.bind_process(process, native_job=native_job)
        from tests.waiting import wait_until
        wait_until(
            ack_path.exists,
            timeout=15,
            what="Windows trampoline worker acknowledgement",
        )
        acknowledgement = json.loads(ack_path.read_text(encoding="utf-8"))
        return binding, worker, process, native_job, acknowledgement, release_path
    except BaseException:
        _windows_split_cleanup(process, native_job, release_path)
        raise


def _windows_split_cleanup(process, native_job, release_path):
    release_path.touch(exist_ok=True)
    try:
        if process is not None and process.poll() is None:
            process.terminate()
        if process is not None:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
    finally:
        if native_job is not None and getattr(native_job, "_handle", None) is not None:
            native_job.close()


def _windows_direct_fixture(custody_runtime):
    """Start a real same-PID worker with a retained canonical Job.

    The native Windows PM environment may expose a ``Scripts/python.exe`` uv shim as
    ``sys.executable``.  Use the interpreter's base executable here so this fixture proves the
    direct contract; ``_windows_split_fixture`` deliberately uses ``sys.executable`` to prove
    the trampoline/degraded contract separately.
    """
    _coordinator, executions, db = custody_runtime
    from cron.runtime_custody import reserve_execution

    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-windows-direct", source="builtin", required=True)
    executions.create_execution(
        "job-windows-direct", source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(executions.get_execution(binding.owner_id))
    worker = binding.reserve_external_worker()
    assert executions.mark_execution_handoff_pending(binding.execution_id) is not None
    script = textwrap.dedent(
        f"""
        import json, os, sys
        from pathlib import Path
        import cron.executions as e
        e.EXECUTIONS_FILE = Path({str(db)!r})
        adopted = e.adopt_claimed_execution({binding.execution_id!r})
        assert adopted is not None
        print(json.dumps({{
            "execution_id": {binding.execution_id!r},
            "pid": os.getpid(),
            "process_started_at": adopted["process_started_at"],
            "dispatch_mode": "direct",
            "scope_unit": None,
        }}), flush=True)
        assert sys.stdin.read(1) == "x"
        e.finish_execution({binding.execution_id!r}, success=True, delivery_outcome="delivered")
        """
    )
    native_job = None
    process = None
    try:
        from hermes_cli.local_runtime.processes import spawn_server
        from tui_gateway.windows_owner_recovery import new_windows_job_name
        process, native_job = spawn_server(
            [_windows_base_executable(), "-c", script], stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
            env=_windows_base_environment(),
            owner_job_name=new_windows_job_name(),
        )
        worker.bind_process(process, native_job=native_job)
        assert process.stdout is not None
        acknowledgement = json.loads(process.stdout.readline())
        return binding, worker, process, native_job, acknowledgement
    except BaseException:
        _windows_direct_cleanup(process, native_job)
        raise


def _windows_direct_cleanup(process, native_job):
    try:
        if process is not None and process.poll() is None:
            stream = getattr(process, "stdin", None)
            if stream is not None:
                try:
                    stream.write("x")
                    stream.close()
                except (OSError, ValueError):
                    pass
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
    finally:
        if native_job is not None and getattr(native_job, "_handle", None) is not None:
            native_job.close()


@pytest.mark.platforms("windows")
def test_i13_windows_degraded_trampoline_authenticates_and_waiter_closes_job(custody_runtime, tmp_path):
    """A real split is admitted only through canonical Job+ancestry proof and settles via waiter."""
    _parent, child, process, native_job, acknowledgement, release_path = _windows_split_fixture(
        custody_runtime, tmp_path, with_job=True)
    try:
        child.activate_external_worker(acknowledgement, process, dispatch_mode="degraded")
        assert acknowledgement["pid"] != process.pid
        assert child._bound_process_pid == process.pid
        assert child._worker_pid == acknowledgement["pid"]
        assert child._worker_started_at == acknowledgement["process_started_at"]
        assert child._native_job_proven is True
        assert child._scope_proof["worker_job_membership"] == "transitive_no_breakaway_ancestry"
        assert child._scope_proof["native_job_canonical"] is True
        release_path.touch()
        from cron.scheduler import _wait_for_external_cron_worker_body
        assert _wait_for_external_cron_worker_body(
            process, execution_id=child.execution_id, custody=child, dispatch_mode="degraded") is True
        if child._reaper is not None:
            child._reaper.join(timeout=10)
        assert child.observe_terminal()
        assert child._scope_proof["scope_empty"] is True
        assert native_job._handle is None
        assert child.settled
    finally:
        _windows_split_cleanup(process, native_job, release_path)


@pytest.mark.platforms("windows")
def test_i13_windows_degraded_trampoline_without_job_refuses(custody_runtime, tmp_path):
    """A split without canonical native Job custody remains UNKNOWN, never settles."""
    _parent, child, process, native_job, acknowledgement, release_path = _windows_split_fixture(
        custody_runtime, tmp_path, with_job=False)
    try:
        from cron.runtime_custody import CronCustodyRefused
        with pytest.raises(CronCustodyRefused) as refusal:
            child.activate_external_worker(acknowledgement, process, dispatch_mode="degraded")
        assert refusal.value.code == "worker_job_membership_unproven"
        assert child._unknown is True
        assert child.settled is False
    finally:
        _windows_split_cleanup(process, native_job, release_path)


@pytest.mark.platforms("windows")
def test_i13_windows_degraded_trampoline_forged_worker_without_ancestor_refuses(
    custody_runtime, tmp_path,
):
    """A live sibling PID/start without the retained root ancestor cannot be adopted."""
    _parent, child, process, native_job, acknowledgement, release_path = _windows_split_fixture(
        custody_runtime, tmp_path, with_job=True)
    sibling = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(20)"],
        stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        from cron.runtime_custody import CronCustodyRefused
        from gateway.status import get_process_start_time
        forged = dict(
            acknowledgement,
            pid=sibling.pid,
            process_started_at=get_process_start_time(sibling.pid),
        )
        with pytest.raises(CronCustodyRefused):
            child.activate_external_worker(forged, process, dispatch_mode="degraded")
        assert child._unknown is True
    finally:
        sibling.terminate()
        sibling.wait(timeout=10)
        _windows_split_cleanup(process, native_job, release_path)


@pytest.mark.platforms("windows")
def test_i13_windows_degraded_trampoline_query_or_replaced_job_refuses(
    custody_runtime, tmp_path,
):
    """Query-only and replaced Jobs cannot cross the canonical owner boundary."""
    _parent, child, process, native_job, acknowledgement, release_path = _windows_split_fixture(
        custody_runtime, tmp_path, with_job=True)
    query_only = None
    replacement = None
    try:
        from cron.runtime_custody import CronCustodyRefused
        from hermes_cli.local_runtime.processes import _WindowsJob
        query_only = _WindowsJob.open_existing(native_job.name)
        child._native_job = query_only
        child._native_job_proven = False
        with pytest.raises(CronCustodyRefused):
            child.activate_external_worker(acknowledgement, process, dispatch_mode="degraded")
        assert child._unknown is True
    finally:
        if query_only is not None:
            query_only.close()
        _windows_split_cleanup(process, native_job, release_path)

    # A fresh canonical Job with a different handle is also foreign, even if a caller forges the
    # boolean proof. The retained object identity is part of the authenticated binding.
    _parent, child, process, native_job, acknowledgement, release_path = _windows_split_fixture(
        custody_runtime, tmp_path / "replacement", with_job=True)
    try:
        from cron.runtime_custody import CronCustodyRefused
        from tui_gateway.windows_owner_recovery import new_windows_job_name
        replacement = type(native_job)(name=new_windows_job_name())
        child._native_job = replacement
        child._native_job_proven = True
        with pytest.raises(CronCustodyRefused):
            child.activate_external_worker(acknowledgement, process, dispatch_mode="degraded")
        assert child._unknown is True
    finally:
        if replacement is not None and getattr(replacement, "_handle", None) is not None:
            replacement.close()
        _windows_split_cleanup(process, native_job, release_path)


@pytest.mark.platforms("windows")
def test_i13_windows_degraded_trampoline_closed_job_refuses(
    custody_runtime, tmp_path,
):
    """A closed native Job cannot be promoted to an owner after the worker is launched."""
    _parent, child, process, native_job, acknowledgement, release_path = _windows_split_fixture(
        custody_runtime, tmp_path, with_job=True)
    closed_job = None
    try:
        from cron.runtime_custody import CronCustodyRefused
        from tui_gateway.windows_owner_recovery import new_windows_job_name

        # Keep the real retained owner Job alive so the worker remains a live causal fixture;
        # close a separate canonical handle and make it the candidate binding.  This exercises
        # the closed-handle path without terminating the real root/worker tree through
        # KILL_ON_JOB_CLOSE.
        closed_job = type(native_job)(name=new_windows_job_name())
        closed_job.close()
        child._native_job = closed_job
        child._native_job_identity = closed_job
        child._native_job_proven = True
        with pytest.raises(CronCustodyRefused) as refusal:
            child.activate_external_worker(acknowledgement, process, dispatch_mode="degraded")
        assert refusal.value.code == "worker_job_membership_unproven"
        assert child._unknown is True
        assert child.settled is False
    finally:
        _windows_split_cleanup(process, native_job, release_path)


@pytest.mark.platforms("windows")
def test_i13_windows_degraded_trampoline_stale_worker_birth_refuses(
    custody_runtime, tmp_path, monkeypatch,
):
    """A live worker PID with a stale birth fingerprint cannot satisfy the split proof."""
    _parent, child, process, native_job, acknowledgement, release_path = _windows_split_fixture(
        custody_runtime, tmp_path, with_job=True)
    try:
        from cron.runtime_custody import CronCustodyRefused
        from gateway.status import get_process_start_time

        real_get_process_start_time = get_process_start_time

        def stale_worker_birth(pid):
            value = real_get_process_start_time(pid)
            if pid == acknowledgement["pid"] and isinstance(value, int):
                return value + 1
            return value

        monkeypatch.setattr("gateway.status.get_process_start_time", stale_worker_birth)
        with pytest.raises(CronCustodyRefused):
            child.activate_external_worker(acknowledgement, process, dispatch_mode="degraded")
        assert child._unknown is True
        assert child.settled is False
    finally:
        _windows_split_cleanup(process, native_job, release_path)


@pytest.mark.platforms("windows")
def test_i13_windows_degraded_terminal_uses_same_retained_job_after_exit(
    custody_runtime, tmp_path, monkeypatch,
):
    """After activation, terminalization uses the stored tuple and retained Job handle only."""
    _parent, child, process, native_job, acknowledgement, release_path = _windows_split_fixture(
        custody_runtime, tmp_path, with_job=True)
    try:
        child.activate_external_worker(acknowledgement, process, dispatch_mode="degraded")
        original_job = child._native_job
        original_identity = child._native_job_identity
        original_process = child._process
        release_path.touch()
        process.wait(timeout=10)

        # Once split admission succeeded, the child may finish after both live ancestry probes
        # are gone.  Reconciliation must not re-adopt a fresh PID or walk ancestry again.
        monkeypatch.setattr(
            child, "_windows_trampoline_worker_matches",
            lambda *_args, **_kwargs: pytest.fail("terminal reconciliation re-probed ancestry"),
        )
        assert child.reconcile_unacknowledged(process, dispatch_mode="degraded") == "terminalized"
        assert child._process is original_process
        assert child._native_job is original_job
        assert child._native_job_identity is original_identity
        assert child._worker_pid == acknowledgement["pid"]
        assert child._worker_started_at == acknowledgement["process_started_at"]

        from cron.scheduler_detached_worker import reap_terminal_worker_in_background

        reaper = reap_terminal_worker_in_background(process)
        child.bind_reaper(reaper)
        reaper.join(timeout=10)
        assert not reaper.is_alive()
        assert child.observe_terminal()
        assert child.settled
    finally:
        _windows_split_cleanup(process, native_job, release_path)


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("replacement_kind", ("query_only", "foreign_canonical", "dropped"))
def test_i13_windows_direct_terminal_replaced_or_query_job_refuses(
    custody_runtime, replacement_kind,
):
    """A direct same-PID terminal row cannot drain a replaced, dropped, or query-only Job."""
    _parent, child, process, native_job, acknowledgement = _windows_direct_fixture(custody_runtime)
    from cron import executions
    replacement = None
    active_handle = None
    try:
        child.activate_external_worker(acknowledgement, process, dispatch_mode="direct")
        assert acknowledgement["pid"] == process.pid
        active_handle = child.handle
        original_job = child._native_job
        if replacement_kind == "dropped":
            # Keep the authenticated identity while dropping only the live object.  The
            # retained Job receipt must still be required during terminal reconciliation.
            replacement = None
            child._native_job = None
            assert child._native_job_identity is original_job
            assert child._native_job_proven is True
        elif replacement_kind == "query_only":
            from hermes_cli.local_runtime.processes import _WindowsJob
            replacement = _WindowsJob.open_existing(native_job.name)
        else:
            from tui_gateway.windows_owner_recovery import new_windows_job_name
            replacement = type(native_job)(name=new_windows_job_name())
        if replacement_kind != "dropped":
            child._native_job = replacement
            child._native_job_proven = True
        assert executions.finish_execution(
            child.execution_id, success=True, delivery_outcome="delivered") is not None
        assert child.reconcile_unacknowledged(process, dispatch_mode="direct") == "unknown"
        assert child._unknown is True
        assert child.settled is False
        assert child.handle is active_handle
        assert child._native_job_identity is original_job
    finally:
        if replacement is not None and getattr(replacement, "_handle", None) is not None:
            replacement.close()
        _windows_direct_cleanup(process, native_job)


@pytest.mark.platforms("windows")
def test_i13_windows_degraded_terminal_tuple_mismatch_refuses_without_readoption(
    custody_runtime, tmp_path, monkeypatch,
):
    """An activated split with a stale terminal tuple stays UNKNOWN while both processes live."""
    _parent, child, process, native_job, acknowledgement, release_path = _windows_split_fixture(
        custody_runtime, tmp_path, with_job=True, hold_after_terminal=True)
    from cron import executions
    from tests.waiting import wait_until
    try:
        child.activate_external_worker(acknowledgement, process, dispatch_mode="degraded")
        active_handle = child.handle
        active_job = child._native_job
        wait_until(
            lambda: (
                (record := executions.get_execution(child.execution_id)) is not None
                and record["status"] == "completed"
            ),
            timeout=15,
            what="the child-owned terminal execution row",
        )
        child._worker_started_at += 1
        reactivation_calls = []

        def _unexpected_reactivation(*args, **kwargs):
            reactivation_calls.append((args, kwargs))
            raise AssertionError("activated binding must not re-adopt a mismatched tuple")

        monkeypatch.setattr(child, "_activate_child", _unexpected_reactivation)
        assert child.reconcile_unacknowledged(process, dispatch_mode="degraded") == "unknown"
        assert child._unknown is True
        assert child.settled is False
        assert reactivation_calls == []
        assert process.poll() is None
        assert child.handle is active_handle
        assert child._native_job is active_job
    finally:
        _windows_split_cleanup(process, native_job, release_path)


@pytest.mark.platforms("windows")
def test_i13_windows_degraded_trampoline_terminal_stored_identity_or_job_failure_is_unknown(
    custody_runtime, tmp_path, monkeypatch,
):
    """Terminal reconciliation uses stored identity and still refuses missing Job drain proof."""
    _parent, child, process, native_job, acknowledgement, release_path = _windows_split_fixture(
        custody_runtime, tmp_path, with_job=True)
    try:
        child.activate_external_worker(acknowledgement, process, dispatch_mode="degraded")
        original_started = child._worker_started_at
        child._worker_started_at = original_started + 1
        release_path.touch()
        from cron.scheduler import _ExternalWorkerPostHandoffError, _wait_for_external_cron_worker_body
        with pytest.raises(_ExternalWorkerPostHandoffError):
            _wait_for_external_cron_worker_body(
                process, execution_id=child.execution_id, custody=child, dispatch_mode="degraded")
        assert child._unknown is True
        assert child.settled is False
    finally:
        _windows_split_cleanup(process, native_job, release_path)


@pytest.mark.platforms("windows")
def test_i13_windows_degraded_trampoline_job_drain_failure_is_unknown(
    custody_runtime, tmp_path, monkeypatch,
):
    """An authenticated split cannot settle when the retained Job cannot prove empty/close."""
    _parent, child, process, native_job, acknowledgement, release_path = _windows_split_fixture(
        custody_runtime, tmp_path, with_job=True)
    try:
        child.activate_external_worker(acknowledgement, process, dispatch_mode="degraded")
        monkeypatch.setattr(native_job, "empty", lambda: None)
        release_path.touch()
        from cron.scheduler import _wait_for_external_cron_worker_body
        assert _wait_for_external_cron_worker_body(
            process, execution_id=child.execution_id, custody=child, dispatch_mode="degraded") is True
        assert child._unknown is True
        assert child.settled is False
        assert child._scope_proof["scope_empty"] is False
    finally:
        _windows_split_cleanup(process, native_job, release_path)


def test_i13_birth_fingerprint_mismatch_refuses_worker_adoption(custody_runtime, monkeypatch):
    _coordinator, executions, db = custody_runtime
    from cron.runtime_custody import CronCustodyRefused, reserve_execution

    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-birth-mismatch", source="builtin", required=True)
    record = executions.create_execution(
        "job-birth-mismatch", source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(record)
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
            "dispatch_mode": "direct",
        }}), flush=True)
        assert sys.stdin.read(1) == "x"
        """
    )
    process = subprocess.Popen(
        [_windows_base_executable(), "-c", script], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        text=True, env=_windows_base_environment())
    # Capture a deliberately stale parent fingerprint before bind_process. The child adopts the
    # real durable row and publishes its real fingerprint, so activation must refuse the mismatch
    # against the Popen identity already captured by the parent.
    monkeypatch.setattr("gateway.status.get_process_start_time", lambda _pid: -1)
    worker.bind_process(process)
    try:
        assert process.stdout is not None
        acknowledgement = json.loads(process.stdout.readline())
        with pytest.raises(CronCustodyRefused):
            worker.activate_external_worker(acknowledgement, process, dispatch_mode="direct")
    finally:
        if process.stdin is not None:
            process.stdin.write("x")
            process.stdin.close()
        if process.poll() is None:
            process.terminate()
        process.wait(timeout=5)


def test_i14_validates_exact_provider_execution_before_detach(custody_runtime):
    coordinator, executions, _db = custody_runtime
    from cron.runtime_custody import CronCustodyRefused, reserve_execution

    api_binding = reserve_execution(
        owner_class="I14_api_detached_cron", job_id="job-api", source="api-cron",
        required=True, owner_id="api:request-1")
    fire_binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-api", source="chronos",
        parent=api_binding, required=True, owner_id="execution-1")
    record = executions.create_execution("job-api", source="chronos", execution_id="execution-1")
    fire_binding.bind_execution_record(record)
    api_binding.validate_child_receipt(
        fire_binding, job_id="job-api", execution_id="execution-1")
    assert api_binding.child is fire_binding

    foreign = reserve_execution(
        owner_class="I12_cron_fire", job_id="other-job", source="chronos",
        required=True, owner_id="execution-foreign")
    foreign_record = executions.create_execution(
        "other-job", source="chronos", execution_id="execution-foreign")
    foreign.bind_execution_record(foreign_record)
    with pytest.raises(CronCustodyRefused):
        api_binding.validate_child_receipt(
            foreign, job_id="job-api", execution_id="execution-foreign")
    assert coordinator.status()["unknown_owner_classes"] == []


def test_i13_adopted_child_without_ready_ack_is_unknown(custody_runtime):
    _coordinator, executions, _db = custody_runtime
    from cron.runtime_custody import reserve_execution

    parent = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-adopted", source="builtin", required=True)
    record = executions.create_execution(
        "job-adopted", source="builtin", execution_id=parent.owner_id)
    parent.bind_execution_record(record)
    worker = parent.reserve_external_worker()
    assert executions.mark_execution_handoff_pending(parent.execution_id) is not None
    assert executions.adopt_claimed_execution(parent.execution_id) is not None
    assert worker.reconcile_unacknowledged() == "unknown"
    assert not worker.settled
    assert not parent.settled


def test_i13_script_descendant_writer_is_owned_until_real_pipe_close(
    custody_runtime, monkeypatch, tmp_path,
):
    _coordinator, executions, _db = custody_runtime
    from cron.runtime_custody import bind_cron_owner, reserve_execution
    from cron import scheduler_script

    scripts = tmp_path / "scripts"
    scripts.mkdir()
    script = scripts / "writer.py"
    script.write_text(
        "import subprocess, sys, time\n"
        "child = subprocess.Popen([sys.executable, '-c', 'import time; print(\"child\", flush=True); time.sleep(.1)'])\n"
        "print('parent', flush=True)\n"
        "child.wait()\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(scheduler_script._sched, "_get_hermes_home", lambda: tmp_path)
    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id="job-script", source="builtin", required=True)
    record = executions.create_execution(
        "job-script", source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(record)
    assert executions.mark_execution_running(binding.execution_id) is not None
    with bind_cron_owner(binding):
        success, output = scheduler_script._run_job_script(str(script))
    assert success and "parent" in output and "child" in output
    assert binding._script_process is not None
    assert binding._script_pipes_closed
    assert executions.finish_execution(
        binding.execution_id, success=True, delivery_outcome="delivered") is not None
    assert binding.observe_terminal()


def test_cron_preservation_evidence_consumes_ledger_and_physical_state(custody_runtime):
    _coordinator, executions, _db = custody_runtime
    from cron import runtime_custody

    binding = runtime_custody.reserve_execution(
        owner_class="I12_cron_fire", job_id="job-evidence", source="builtin", required=True)
    record = executions.create_execution(
        "job-evidence", source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(record)
    assert executions.mark_execution_running(binding.execution_id) is not None
    assert runtime_custody.preservation_owner_evidence(
        "I12_cron_fire", process_instance="cron-custody-test", authority_epoch=11,
        close_generation=1, owner_id="cron-custody-test:I12_cron_fire") is None
    assert executions.finish_execution(
        binding.execution_id, success=True, delivery_outcome="delivered") is not None
    assert binding.observe_terminal()
    evidence = runtime_custody.preservation_owner_evidence(
        "I12_cron_fire", process_instance="cron-custody-test", authority_epoch=11,
        close_generation=1, owner_id="cron-custody-test:I12_cron_fire")
    # This process-local settled row is a durable negative observation only.  The coordinator
    # classifies I12 as CROSS_PROCESS, so no evidence may claim external quiescence without a
    # foreign row plus a matching settled cgroup/Job receipt.
    assert evidence is None


def test_i12_provider_claim_fire_returns_real_execution_receipt(custody_runtime, monkeypatch):
    _coordinator, executions, _db = custody_runtime
    from cron import jobs
    from cron.scheduler_provider import InProcessCronScheduler

    monkeypatch.setattr(
        jobs, "claim_job_for_fire",
        lambda job_id, **_kwargs: {"id": job_id, "_scheduled_instant": "2026-10-18T06:00:00+00:00"},
    )
    claimed = InProcessCronScheduler().claim_fire("job-provider")
    assert claimed is not None
    binding = claimed["_cron_custody"]
    assert claimed["execution_id"] == binding.execution_id == binding.owner_id
    assert binding.owner_class == "I12_cron_fire"
    assert executions.finish_execution(
        binding.execution_id, success=True, delivery_outcome="delivered") is not None
    assert binding.observe_terminal()


def test_i12_provider_claim_fire_refuses_during_retirement_close(custody_runtime, monkeypatch):
    """A provider fire cannot create an I12 root after the managed close barrier is held."""
    coordinator, _executions, _db = custody_runtime
    from cron import jobs
    from cron.runtime_custody import CronCustodyRefused
    from cron.scheduler_provider import InProcessCronScheduler

    monkeypatch.setattr(
        jobs,
        "claim_job_for_fire",
        lambda *_args, **_kwargs: pytest.fail("provider claimed a job after retirement began"),
    )
    close = coordinator.begin_close(owner_id="provider-close")
    assert close is not None
    try:
        with pytest.raises(CronCustodyRefused, match="runtime_draining"):
            InProcessCronScheduler().claim_fire("job-provider-close")
    finally:
        assert coordinator.abort_before_handoff(close) is None


@pytest.mark.platforms("windows")
def test_i13_direct_popen_identity_probe():
    """Diagnostic only: compare real child identity with the parent Popen receipt."""
    script = textwrap.dedent(
        """
        import json
        import os
        import sys

        from gateway.status import get_process_start_time

        print(json.dumps({
            "pid": os.getpid(),
            "process_started_at": get_process_start_time(os.getpid()),
        }), flush=True)
        assert sys.stdin.read(1) == "x"
        """
    )
    process = subprocess.Popen(
        [_windows_base_executable(), "-c", script], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=_windows_base_environment())
    try:
        from gateway.status import get_process_start_time

        parent_identity = {
            "pid": process.pid,
            "process_started_at": get_process_start_time(process.pid),
        }
        assert process.stdout is not None
        child_identity = json.loads(process.stdout.readline())
        assert child_identity["pid"] == parent_identity["pid"], (
            "real-child PID differs from Popen PID; inspect the Windows launcher boundary: "
            f"child={child_identity['pid']} parent={parent_identity['pid']}"
        )
        assert type(child_identity["process_started_at"]) is int, child_identity
        assert type(parent_identity["process_started_at"]) is int, parent_identity
        assert child_identity["process_started_at"] == parent_identity["process_started_at"], (
            "real-child and parent start fingerprints differ: "
            f"child={child_identity['process_started_at']} "
            f"parent={parent_identity['process_started_at']}"
        )
    finally:
        if process.stdin is not None:
            try:
                process.stdin.write("x")
                process.stdin.flush()
                process.stdin.close()
            except (BrokenPipeError, OSError, ValueError):
                pass
        process.wait(timeout=5)
