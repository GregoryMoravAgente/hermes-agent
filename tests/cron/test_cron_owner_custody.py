"""Causal I12/I13/I14 custody tests over the existing cron ledger and gateway coordinator."""
from __future__ import annotations

import json
import subprocess
import sys
import textwrap
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from threading import Event, Thread

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
        [sys.executable, "-c", script], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        text=True)
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
        [sys.executable, "-c", script], stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
        text=True)
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
        [sys.executable, "-c", script], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
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
        [sys.executable, "-c", script], stdin=subprocess.PIPE,
        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
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
        [sys.executable, "-c", script], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        text=True)
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



def _i13_constant_reason(error):
    allowed = {
        "worker_ack_mismatch",
        "worker_execution_not_adopted",
        "worker_execution_read_failed",
        "worker_job_membership_unproven",
        "worker_scope_identity_unproven",
        "worker_startup_identity_mismatch",
        "foreign_custody",
    }
    detail = str(error)
    return detail if detail in allowed else type(error).__name__


def _i13_start_time(pid, get_process_start_time):
    if type(pid) is not int or pid <= 0:
        return None
    try:
        value = get_process_start_time(pid)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return None
    return value if type(value) is int else None


def _i13_job_member(native_job, process):
    raw_handle = getattr(process, "raw_handle", None)
    if native_job is None or raw_handle is None:
        return "unknown"
    try:
        result = native_job.contains_process_handle(SimpleNamespace(raw_handle=raw_handle))
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return "unknown"
    return result if result in (True, False) else "unknown"


@pytest.mark.platforms("windows")
def test_i13_windows_external_dispatch_identity_diagnostic(custody_runtime, monkeypatch, tmp_path):
    """Diagnostic only: observe the real Cron spawn/ACK/ledger identity boundary.

    This deliberately exercises ``_launch_external_cron_worker`` with a real no-agent script,
    real ``spawn_server`` and the real durable execution ACK. It keeps the existing I13 refusal
    contract fail-closed and emits a bounded synthetic receipt when the current identity check
    rejects the child.
    """
    _coordinator, executions, _db = custody_runtime
    from cron import jobs, scheduler
    from cron.runtime_custody import CronOwnerBinding, reserve_execution
    from gateway.status import get_process_start_time
    from hermes_cli.local_runtime import processes as local_processes
    from tui_gateway.windows_producer_identity import (
        ProducerIdentityError, WindowsProducerHandle)

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: tmp_path)
    scripts = tmp_path / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    started = tmp_path / "i13-diagnostic-started"
    release = tmp_path / "i13-diagnostic-release"
    script = scripts / "i13-native-diagnostic.py"
    script.write_text(
        "from pathlib import Path\n"
        "import time\n"
        f"started = Path({str(started)!r})\n"
        f"release = Path({str(release)!r})\n"
        "started.write_text('started', encoding='utf-8')\n"
        "deadline = time.monotonic() + 20\n"
        "while not release.exists() and time.monotonic() < deadline:\n"
        "    time.sleep(0.05)\n"
        "if not release.exists():\n"
        "    raise SystemExit('diagnostic release timeout')\n"
        "print('i13-diagnostic-complete', flush=True)\n",
        encoding="utf-8",
    )
    job = jobs.create_job(
        prompt=None, schedule="every 1h", name="I13 identity diagnostic",
        script=script.name, no_agent=True, deliver="local",
    )
    binding = reserve_execution(
        owner_class="I12_cron_fire", job_id=job["id"], source="builtin", required=True)
    record = executions.create_execution(
        job["id"], source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(record)
    job = dict(job, execution_id=binding.execution_id, _cron_custody=binding)

    receipt = {
        "platform": "win32",
        "launcher": {
            "popen_pid": None, "popen_start": None, "popen_start_known": False,
            "popen_handle_available": False, "popen_in_exact_job": "unknown",
        },
        "ack": {
            "pid": None, "start": None, "start_known": False, "execution_id_present": False,
            "dispatch_mode": "unknown", "scope_fields_present": False,
        },
        "durable_row": {
            "id_matches_execution": False, "pid": None, "start": None,
            "start_known": False, "status": "other", "matches_ack": False,
        },
        "native_child": {
            "open_handle": False, "creation_filetime_known": False,
            "creation_filetime": None,
            "in_exact_job_by_handle": "unknown",
        },
        "comparisons": {
            "pid_equal": False, "start_equal": False,
            "popen_job_membership": "unknown", "child_job_membership": "unknown",
            "identity_predicate": False,
        },
        "decision": "no_activation_receipt",
        "constant_reason": "not_observed",
    }
    captured = {"process": None, "native_job": None}
    query_handles = []
    spawn_seen = Event()
    activation_seen = Event()
    launch_done = Event()
    launch_state = {"returned_true": False, "error_type": None, "constant_reason": None}
    cleanup_state = {
        "forced_termination": False,
        "launch_thread_alive": True,
        "native_job_closed": False,
    }

    def capture_parent(process, native_job):
        pid = getattr(process, "pid", None)
        start = _i13_start_time(pid, get_process_start_time)
        raw_handle = getattr(process, "_handle", None)
        receipt["launcher"].update({
            "popen_pid": pid if type(pid) is int else None,
            "popen_start": start,
            "popen_start_known": type(start) is int,
            "popen_handle_available": raw_handle is not None,
            "popen_in_exact_job": _i13_job_member(
                native_job,
                SimpleNamespace(raw_handle=raw_handle),
            ) if raw_handle is not None else "unknown",
        })

    real_spawn_server = local_processes.spawn_server

    def capture_spawn_server(command, **kwargs):
        process, native_job = real_spawn_server(command, **kwargs)
        captured["process"] = process
        captured["native_job"] = native_job
        capture_parent(process, native_job)
        spawn_seen.set()
        return process, native_job

    monkeypatch.setattr(local_processes, "spawn_server", capture_spawn_server)

    real_bind_process = CronOwnerBinding.bind_process
    real_activate = CronOwnerBinding.activate_external_worker
    real_reconcile = CronOwnerBinding.reconcile_unacknowledged

    def capture_bind_process(self, process, *, native_job=None):
        result = real_bind_process(self, process, native_job=native_job)
        if self.owner_class == "I13_external_cron_worker":
            capture_parent(process, native_job)
        return result

    def capture_activate(self, acknowledgement, process, *, dispatch_mode=None, scope_unit=None):
        target = self.owner_class == "I13_external_cron_worker"
        if target:
            ack_pid = acknowledgement.get("pid") if isinstance(acknowledgement, dict) else None
            ack_start = (
                acknowledgement.get("process_started_at")
                if isinstance(acknowledgement, dict) else None
            )
            ack_mode = acknowledgement.get("dispatch_mode") if isinstance(acknowledgement, dict) else None
            receipt["ack"].update({
                "pid": ack_pid if type(ack_pid) is int else None,
                "start": ack_start if type(ack_start) is int else None,
                "start_known": type(ack_start) is int,
                "execution_id_present": (
                    isinstance(acknowledgement, dict)
                    and acknowledgement.get("execution_id") == self.execution_id
                ),
                "dispatch_mode": ack_mode if ack_mode in {"direct", "degraded", "scoped"} else "unknown",
                "scope_fields_present": bool(
                    isinstance(acknowledgement, dict)
                    and (
                        acknowledgement.get("scope_unit")
                        or acknowledgement.get("scope_control_group")
                    )
                ),
            })
            try:
                row = executions.get_execution(self.execution_id)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                row = None
            row_pid = row.get("pid") if isinstance(row, dict) else None
            row_start = row.get("process_started_at") if isinstance(row, dict) else None
            row_status = row.get("status") if isinstance(row, dict) else None
            receipt["durable_row"].update({
                "id_matches_execution": (
                    isinstance(row, dict) and str(row.get("id")) == self.execution_id
                ),
                "pid": row_pid if type(row_pid) is int else None,
                "start": row_start if type(row_start) is int else None,
                "start_known": type(row_start) is int,
                "status": row_status if row_status in {"claimed", "running", "completed", "failed"} else "other",
                "matches_ack": (
                    isinstance(row, dict)
                    and str(row.get("id")) == self.execution_id
                    and row_pid == ack_pid
                    and row_start == ack_start
                ),
            })
            receipt["comparisons"].update({
                "pid_equal": receipt["launcher"]["popen_pid"] == ack_pid,
                "start_equal": (
                    receipt["launcher"]["popen_start"] == ack_start
                    and type(ack_start) is int
                ),
                "popen_job_membership": receipt["launcher"]["popen_in_exact_job"],
            })
            child_handle = None
            if type(ack_pid) is int:
                try:
                    child_handle = WindowsProducerHandle.open(ack_pid)
                    query_handles.append(child_handle)
                    receipt["native_child"].update({
                        "open_handle": True,
                        "creation_filetime_known": type(child_handle.creation_filetime) is int,
                        "creation_filetime": (
                            child_handle.creation_filetime
                            if type(child_handle.creation_filetime) is int else None
                        ),
                        "in_exact_job_by_handle": _i13_job_member(
                            self._native_job, child_handle),
                    })
                except ProducerIdentityError as error:
                    if error.retained_handle is not None:
                        query_handles.append(error.retained_handle)
                    receipt["native_child"].update({
                        "open_handle": False,
                        "creation_filetime_known": False,
                        "in_exact_job_by_handle": "unknown",
                    })
                except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                    receipt["native_child"].update({
                        "open_handle": False,
                        "creation_filetime_known": False,
                        "in_exact_job_by_handle": "unknown",
                    })
            receipt["comparisons"]["child_job_membership"] = receipt["native_child"][
                "in_exact_job_by_handle"]
            receipt["comparisons"]["identity_predicate"] = bool(
                receipt["comparisons"]["pid_equal"]
                and receipt["comparisons"]["start_equal"]
                and receipt["durable_row"]["matches_ack"]
            )
        try:
            result = real_activate(
                self, acknowledgement, process,
                dispatch_mode=dispatch_mode, scope_unit=scope_unit)
        except Exception as error:
            if target:
                receipt["decision"] = "refused"
                receipt["constant_reason"] = _i13_constant_reason(error)
            raise
        else:
            if target:
                receipt["decision"] = "accepted"
                receipt["constant_reason"] = "accepted"
            return result
        finally:
            if target:
                activation_seen.set()

    def capture_reconcile(self, process=None, *, dispatch_mode=None):
        try:
            result = real_reconcile(self, process, dispatch_mode=dispatch_mode)
        except Exception:
            receipt["reconcile_result"] = "exception"
            raise
        if self.owner_class == "I13_external_cron_worker":
            receipt["reconcile_result"] = result if result in {
                "terminalized", "unknown", "unadopted", "not_applicable",
            } else "other"
        return result

    monkeypatch.setattr(CronOwnerBinding, "bind_process", capture_bind_process)
    monkeypatch.setattr(CronOwnerBinding, "activate_external_worker", capture_activate)
    monkeypatch.setattr(CronOwnerBinding, "reconcile_unacknowledged", capture_reconcile)

    def launch():
        try:
            launch_state["returned_true"] = scheduler._launch_external_cron_worker(job) is True
        except Exception as error:
            launch_state["error_type"] = type(error).__name__
            launch_state["constant_reason"] = _i13_constant_reason(error)
        finally:
            launch_done.set()

    thread = Thread(target=launch, name="i13-windows-diagnostic", daemon=True)
    thread.start()
    try:
        spawn_observed = spawn_seen.wait(timeout=30)
        activation_observed = activation_seen.wait(timeout=30) if spawn_observed else False
    finally:
        release.write_text("release", encoding="utf-8")
        if not launch_done.wait(timeout=30):
            process = captured["process"]
            if process is not None and process.poll() is None:
                cleanup_state["forced_termination"] = True
                process.terminate()
                process.wait(timeout=10)
            launch_done.wait(timeout=5)
        thread.join(timeout=5)
        cleanup_state["launch_thread_alive"] = thread.is_alive()
        process = captured["process"]
        if process is not None and process.stdin is not None:
            process.stdin.close()
        for handle in query_handles:
            handle.close()
        native_job = captured["native_job"]
        if (
            native_job is not None
            and not cleanup_state["launch_thread_alive"]
            and (process is None or process.poll() is not None)
        ):
            native_job.close()
            cleanup_state["native_job_closed"] = True

    final_row = executions.get_execution(binding.execution_id)
    if isinstance(final_row, dict):
        receipt["final_status"] = (
            final_row.get("status")
            if final_row.get("status") in {"claimed", "running", "completed", "failed", "unknown"}
            else "other"
        )
    receipt["launch"] = {
        "spawn_observed": spawn_observed,
        "activation_observed": activation_observed,
        "returned_true": launch_state["returned_true"],
        "error_type": launch_state["error_type"],
        "constant_reason": launch_state["constant_reason"],
        "script_started": started.exists(),
        "release_sent": release.exists(),
    }
    receipt["cleanup"] = cleanup_state
    print("I13_WINDOWS_EXTERNAL_DIAGNOSTIC=" + json.dumps(receipt, sort_keys=True))
    assert spawn_observed, json.dumps(receipt, sort_keys=True)
    assert activation_observed, json.dumps(receipt, sort_keys=True)
    assert receipt["launch"]["script_started"], json.dumps(receipt, sort_keys=True)
    assert not cleanup_state["forced_termination"], json.dumps(receipt, sort_keys=True)
    assert not cleanup_state["launch_thread_alive"], json.dumps(receipt, sort_keys=True)
    assert cleanup_state["native_job_closed"], json.dumps(receipt, sort_keys=True)
    assert receipt["decision"] == "accepted", json.dumps(receipt, sort_keys=True)

