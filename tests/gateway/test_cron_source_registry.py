"""Causal controls for the real gateway-owned cron producer graph and jobs-lock fence."""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
import threading
import time

import pytest


def _started_registry(started_mux):
    runner, homes, _loop, _call = started_mux
    coordinator = runner.runtime_custody
    registry = coordinator._cron_source_registry
    # ``started_mux`` exercises the real gateway READY path but intentionally does not launch the
    # periodic ticker.  Bind the same production provider/supervisor objects that run_bootstrap
    # would publish, without starting another background loop in the fixture.
    from cron.scheduler_provider import resolve_cron_scheduler, scheduler_for_profile_mode
    from cron.scheduler_thread import SupervisedTickerThread
    from gateway.runtime_cron_source_registry import bind_cron_scheduler_runtime

    provider = scheduler_for_profile_mode(
        resolve_cron_scheduler(), multiplex_profiles=len(homes) > 1)
    stop_event = threading.Event()
    ticker = SupervisedTickerThread(
        provider.start, args=(stop_event,), kwargs={}, stop_event=stop_event,
        name="cron-source-test-ticker",
    )
    bind_cron_scheduler_runtime(runner, coordinator, provider, ticker)
    return runner, coordinator, registry, homes


def test_zero_work_evidence_scans_real_profile_ledgers_and_native_locks(started_mux):
    """Real authorities/DBs and unequal runtime epochs can certify idle profiles."""
    _runner, coordinator, registry, homes = _started_registry(started_mux)
    assert len(homes) >= 2
    assert registry.begin_close(9)
    evidence = registry.source_owner_evidence(
        "I12_cron_fire",
        process_instance=coordinator.process_instance,
        authority_epoch=coordinator.authority_epoch,
        close_generation=9,
        owner_id=f"{coordinator.process_instance}:I12_cron_fire",
    )
    assert evidence is not None
    assert evidence["cross_process_proof_source"].startswith("cron_source_graph+")
    assert evidence["profile_count"] == len(homes)
    assert all(row["cross_process_proof"] is True for row in evidence["profiles"])
    assert all(row["authority_binding"]["database_path"].endswith("state.db") for row in evidence["profiles"])
    assert registry.abort_close(9)


def test_claim_and_execution_row_refuse_while_gateway_retains_jobs_fence(started_mux, monkeypatch):
    """A close-held jobs fence blocks both CAS fire and the durable row boundary."""
    _runner, _coordinator, registry, homes = _started_registry(started_mux)
    from cron import executions, jobs

    launch = homes["default"]
    monkeypatch.setattr(jobs, "_JOBS_LOCK_TIMEOUT_SECONDS", 0.1)
    with jobs.use_cron_store(launch):
        job = jobs.create_job(
            prompt="zero-work fence control",
            schedule=(datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
            name="fence control",
        )
        assert registry.begin_close(10)
        assert jobs.claim_job_for_fire(job["id"], manual=True, return_job=True) is False
        with pytest.raises(RuntimeError, match="admission refused|cron_admission_closed"):
            executions.create_execution(job["id"], source="manual")
        assert jobs.get_job(job["id"]).get("fire_claim") is None
        assert registry.abort_close(10)


def test_late_script_process_refuses_before_popen_while_gateway_retains_fence(started_mux):
    """The script Popen seam cannot create a child after source close begins."""
    _runner, _coordinator, registry, homes = _started_registry(started_mux)
    launch = homes["default"]
    scripts = launch / "scripts"
    scripts.mkdir(exist_ok=True)
    (scripts / "late.py").write_text("print('must not run')\n", encoding="utf-8")
    assert registry.begin_close(11)
    from cron.scheduler_script import _run_job_script

    success, output = _run_job_script("late.py")
    assert success is False
    assert "admission refused" in output.lower()
    assert registry.abort_close(11)


def test_profile_nested_jobs_lock_does_not_inherit_other_profile(started_mux):
    """Nested profile scopes acquire their own cross-process lock identity."""
    _runner, _coordinator, _registry, homes = _started_registry(started_mux)
    from cron import jobs

    with jobs.use_cron_store(homes["default"]):
        with jobs._jobs_lock(strict=True):
            with jobs.use_cron_store(homes["alpha"]):
                with jobs._jobs_lock(strict=True):
                    assert jobs._jobs_lock_key().endswith("profiles/alpha/cron/.jobs.lock")


def test_terminal_receipts_are_settled_history_not_active_work(started_mux):
    """A valid completed row does not turn an otherwise idle profile into active work."""
    _runner, _coordinator, registry, homes = _started_registry(started_mux)
    from cron import executions

    launch = homes["default"]
    previous = executions.EXECUTIONS_FILE
    executions.EXECUTIONS_FILE = launch / "cron" / "executions.db"
    try:
        record = executions.create_execution("terminal-history", source="test")
        executions.finish_execution(record["id"], success=True)
        assert registry._ledger_quiet(launch)["terminal_rows"] >= 1
    finally:
        executions.EXECUTIONS_FILE = previous


def test_replaced_producer_refuses_fresh_close_evidence(started_mux, monkeypatch):
    """A callable replacement after close cannot reuse the cached producer graph."""
    _runner, coordinator, registry, _homes = _started_registry(started_mux)
    assert registry.begin_close(12)
    from cron import scheduler_script

    original = scheduler_script._run_job_script
    monkeypatch.setattr(scheduler_script, "_run_job_script", lambda *args, **kwargs: (True, "foreign"))
    evidence = registry.source_owner_evidence(
        "I12_cron_fire",
        process_instance=coordinator.process_instance,
        authority_epoch=coordinator.authority_epoch,
        close_generation=12,
        owner_id=f"{coordinator.process_instance}:I12_cron_fire",
    )
    assert evidence is None
    monkeypatch.setattr(scheduler_script, "_run_job_script", original)
    assert registry.abort_close(12)


def test_admission_lease_covers_row_before_close_can_seal(started_mux):
    """A pre-close lease can finish its row effect while close waits; no gap is exposed."""
    _runner, _coordinator, registry, homes = _started_registry(started_mux)
    from cron import executions

    launch = homes["default"]
    close_started = threading.Event()
    close_done = threading.Event()

    admission = registry.admit("execution_row", home=launch)
    admission.__enter__()
    try:
        def _close() -> None:
            close_started.set()
            assert registry.begin_close(13)
            close_done.set()

        thread = threading.Thread(target=_close, name="cron-close-race")
        thread.start()
        assert close_started.wait(2)
        assert not close_done.wait(0.1)
        record = executions.create_execution(
            "lease-race", source="test", admission=admission)
        assert record["status"] == "claimed"
    finally:
        admission.__exit__(None, None, None)
    assert close_done.wait(2)
    thread.join(2)
    assert registry.abort_close(13)


def test_failed_fence_release_retains_exact_cleanup_capability(started_mux, monkeypatch):
    """A release fault remains pending with the same fence until an exact retry succeeds."""
    _runner, _coordinator, registry, _homes = _started_registry(started_mux)
    assert registry.begin_close(14)
    original = registry._release_fence
    monkeypatch.setattr(registry, "_release_fence", lambda _fence: False)
    pending = registry.abort_close_outcome(14)
    assert pending.status == "closing_cleanup_pending"
    assert pending.pending_homes
    assert registry.state == "closing"
    monkeypatch.setattr(registry, "_release_fence", original)
    assert registry.abort_close_outcome(14).status == "opened"


def test_async_close_yields_to_same_loop_accepted_lease(started_mux):
    """A close barrier must yield while an accepted coroutine lease finishes on its loop."""
    _runner, _homes, _loop, call = started_mux
    _runner, coordinator, registry, homes = _started_registry(started_mux)
    launch = homes["default"]
    from gateway.runtime_cron_source_registry import begin_cron_close_async

    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()

        async def accepted_producer():
            with registry.admit("execution_row", home=launch):
                entered.set()
                await release.wait()

        producer = asyncio.create_task(accepted_producer())
        await asyncio.wait_for(entered.wait(), timeout=2)
        close = asyncio.create_task(
            begin_cron_close_async(coordinator, 15, deadline=time.monotonic() + 5)
        )
        await asyncio.sleep(0)
        assert not close.done()
        release.set()
        await asyncio.wait_for(producer, timeout=2)
        assert await asyncio.wait_for(close, timeout=2) is True

    call(scenario())
    assert registry.abort_close(15)


def test_async_close_expired_deadline_refuses_before_fence_effect(started_mux):
    """An expired close deadline leaves the accepted lease and registry open and retryable."""
    _runner, _homes, _loop, call = started_mux
    _runner, coordinator, registry, homes = _started_registry(started_mux)
    launch = homes["default"]
    from gateway.runtime_cron_source_registry import begin_cron_close_async

    async def scenario():
        entered = asyncio.Event()
        release = asyncio.Event()

        async def accepted_producer():
            with registry.admit("execution_row", home=launch):
                entered.set()
                await release.wait()

        producer = asyncio.create_task(accepted_producer())
        await asyncio.wait_for(entered.wait(), timeout=2)
        assert await begin_cron_close_async(
            coordinator, 16, deadline=time.monotonic() - 1,
        ) is False
        assert registry.state == "open"
        assert registry.close_generation is None
        assert registry._fences == {}
        release.set()
        await asyncio.wait_for(producer, timeout=2)

    call(scenario())


def test_partial_fence_cleanup_failure_retains_exact_retry(started_mux, monkeypatch):
    """A late profile-lock failure never drops an earlier exact fence on cleanup failure."""
    _runner, _homes, _loop, _call = started_mux
    _runner, _coordinator, registry, _homes = _started_registry(started_mux)
    from cron import jobs

    calls = 0

    def acquire_once(_fd, _timeout):
        nonlocal calls
        calls += 1
        return calls == 1

    monkeypatch.setattr(jobs, "_acquire_flock", acquire_once)
    original_release = registry._release_fence
    monkeypatch.setattr(registry, "_release_fence", lambda _fence: False)
    assert registry.begin_close(17, deadline=time.monotonic() + 5) is False
    assert registry.state == "closing"
    assert registry.close_generation == 17
    assert len(registry._fences) == 1
    retained = next(iter(registry._fences.values()))
    monkeypatch.setattr(registry, "_release_fence", original_release)
    outcome = registry.abort_close_outcome(17)
    assert outcome.status == "opened"
    assert retained.fd.closed is True
