"""Private proposal: public I12 scheduler producers with the managed owner runtime.

This file is intentionally additive.  The existing cron custody, occurrence, and identity
files remain the negative/order controls; these two cases supply the missing producer-to-I04
consumer and producer-to-script-child paths on one source-owned runtime.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path

import pytest


def _install_i12_observer(monkeypatch):
    """Observe the source-created I12 binding without replacing its constructor or lifecycle."""
    from cron import runtime_custody

    captured = []
    original = runtime_custody.reserve_execution

    @wraps(original)
    def observe(*args, **kwargs):
        binding = original(*args, **kwargs)
        if kwargs.get("owner_class") == "I12_cron_fire" and binding is not None:
            captured.append(binding)
        return binding

    monkeypatch.setattr(runtime_custody, "reserve_execution", observe)
    return captured


def _assert_terminal_i12(binding, job, executions, coordinator, owner_classes_before):
    from cron.runtime_custody import CronOwnerBinding

    assert type(binding) is CronOwnerBinding
    assert binding.owner_class == "I12_cron_fire"
    assert binding.execution_id == binding.owner_id
    assert binding.job_id == job["id"]
    assert binding.settled is True
    record = executions.get_execution(binding.owner_id)
    assert record is not None
    assert record["id"] == binding.owner_id
    assert record["job_id"] == job["id"]
    assert record["source"] == "builtin"
    assert record["status"] in {"completed", "failed"}
    assert record["delivery_outcome"] != "queued"
    active_owner_classes = set(coordinator.status()["active_owner_classes"])
    i12_classes = {"I12_cron_fire", "I13_external_cron_worker"}
    assert active_owner_classes.isdisjoint(i12_classes)
    assert active_owner_classes - i12_classes == set(owner_classes_before) - i12_classes
    return record


def _find_cron_admission(authority, job_id, execution_id):
    from gateway.session_results import admission_result
    from gateway.session_local_recovery import local_identity
    from hermes_state_keys import CRON_PRINCIPAL_ID, cron_request_id
    from hermes_state_runtime import find_session_admission

    request_id = cron_request_id(job_id, execution_id)
    session_id = local_identity(authority.profile_id, CRON_PRINCIPAL_ID, request_id)
    row = find_session_admission(
        authority.db,
        principal_id=CRON_PRINCIPAL_ID,
        session_id=session_id,
        request_id=request_id,
    )
    assert row is not None
    assert row["target_session_id"] == session_id
    saved = admission_result(authority.db, row["admission_id"])
    assert row["status"] == "terminal"
    assert isinstance(saved, dict)
    return row, saved


def _assert_external_worker_physical_ownership(binding):
    """Check the real external-worker topology selected by the source."""
    child = binding.child
    assert child is not None
    from cron.runtime_custody import CronOwnerBinding

    assert type(child) is CronOwnerBinding
    assert child.parent is binding
    assert child.owner_class == "I13_external_cron_worker"
    assert child.owner_id == f"{binding.execution_id}:external-worker"
    assert child.execution_id == binding.execution_id
    assert child.settled is True
    process = child._process
    assert process is not None and process.pid > 0 and process.poll() is not None
    assert child._bound_process_pid == process.pid
    assert child._worker_pid == process.pid
    assert child._reaper is not None
    assert child._reaper.is_alive() is False
    assert getattr(child._reaper, "_cron_reap_succeeded", False) is True
    assert child._dispatch_mode in {"scoped", "degraded"}

    if child._dispatch_mode == "scoped":
        proof = child._scope_proof
        assert isinstance(proof, dict)
        assert proof["pid"] == process.pid
        assert proof["process_started_at"] == child._worker_started_at
        assert proof["scope_empty"] is True
    elif child._native_job is not None:
        proof = child._scope_proof
        assert child._native_job_proven is True
        assert isinstance(proof, dict)
        assert proof["kind"] == "windows_job"
        assert proof["pid"] == process.pid
        assert proof["process_started_at"] == child._worker_started_at
        assert proof["scope_empty"] is True


@pytest.mark.asyncio
async def test_public_scheduler_tick_runs_managed_agent_and_releases_i12(
    runtime_owned_api_owner, monkeypatch,
):
    """A real public tick reaches the owner AIAgent path before I12 finalization."""
    _api, authority = runtime_owned_api_owner
    from cron import executions, jobs, scheduler

    home = Path(authority.db.db_path).resolve().parent
    coordinator = authority.runner.runtime_custody
    owner_classes_before = set(coordinator.status()["active_owner_classes"])
    captured = _install_i12_observer(monkeypatch)
    with jobs.use_cron_store(home):
        job = jobs.create_job(
            prompt="Return the managed cron fixture result.",
            schedule=datetime.now(UTC).isoformat(),
            name="managed scheduler agent",
            deliver="local",
        )
        deadline = asyncio.get_running_loop().time() + 90.0
        assert await asyncio.wait_for(
            asyncio.to_thread(scheduler.tick, verbose=False, sync=True),
            timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
        ) == 1
        assert len(captured) == 1
        binding = captured[0]
        from tests.waiting import async_wait_until

        await async_wait_until(
            lambda: binding.settled,
            timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
            what="I12 scheduler custody finalizer",
        )
        record = _assert_terminal_i12(
            binding, job, executions, coordinator, owner_classes_before,
        )
        assert record["delivery_outcome"] == "suppressed"
        _row, saved = _find_cron_admission(authority, job["id"], binding.execution_id)
        assert saved["result"]["completed"] is True
        assert saved["result"]["cron_result"][0] is True
        assert saved["result"]["final_response"] == "runtime-custody-fixture-ok"


@pytest.mark.asyncio
async def test_public_fire_due_runs_real_script_child_and_releases_i12(
    runtime_owned_api_owner, monkeypatch,
):
    """The public provider fire_due path owns a real script child through pipe EOF."""
    _api, authority = runtime_owned_api_owner
    from cron import executions, jobs
    from cron.scheduler_provider import InProcessCronScheduler

    home = Path(authority.db.db_path).resolve().parent
    coordinator = authority.runner.runtime_custody
    owner_classes_before = set(coordinator.status()["active_owner_classes"])
    scripts = home / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    script = scripts / "managed-cron-child.py"
    script.write_text(
        "import subprocess\n"
        "import sys\n"
        "child = subprocess.Popen([sys.executable, '-c', "
        "\"print('cron-descendant-ok', flush=True)\"])\n"
        "child.wait()\n"
        "print('cron-script-ok', flush=True)\n",
        encoding="utf-8",
    )
    captured = _install_i12_observer(monkeypatch)
    with jobs.use_cron_store(home):
        job = jobs.create_job(
            prompt=None,
            schedule=datetime.now(UTC).isoformat(),
            name="managed scheduler script",
            script=str(script),
            no_agent=True,
            deliver="local",
        )
        provider = InProcessCronScheduler()
        assert await asyncio.wait_for(
            asyncio.to_thread(provider.fire_due, job["id"]), timeout=90.0,
        ) is True

    assert len(captured) == 1
    binding = captured[0]
    record = _assert_terminal_i12(
        binding, job, executions, coordinator, owner_classes_before,
    )
    assert record["delivery_outcome"] == "suppressed"
    if binding.child is None:
        process = binding._script_process
        assert process is not None and process.pid > 0 and process.poll() is not None
        assert binding._script_pipes_closed is True
    else:
        _assert_external_worker_physical_ownership(binding)
    outputs = list(jobs._job_output_dir(job["id"]).glob("*.md"))
    assert len(outputs) == 1
    output = outputs[0].read_text(encoding="utf-8")
    assert "cron-descendant-ok" in output
    assert "cron-script-ok" in output
