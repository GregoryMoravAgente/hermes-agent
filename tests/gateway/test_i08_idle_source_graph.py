"""Causal I08 source-graph close, zero-work, and reversible watcher controls."""

from __future__ import annotations

import asyncio

import pytest

from tests.gateway.test_runtime_custody_profile_epochs import _boot, _close_boot


async def _close_receipt(runner):
    coordinator = runner.runtime_custody
    receipt = coordinator.begin_close(
        owner_id="i08-source-graph-test",
        scope="process",
        deadline_s=30,
    )
    assert getattr(receipt, "state", None) == "closing"
    return receipt


@pytest.mark.asyncio
async def test_i08_real_two_profile_zero_work_uses_exact_graph_and_native_owner(
    tmp_path, monkeypatch,
):
    from gateway.session_goal_tick_custody import (
        _i08_zero_work_snapshot,
        has_i08_source_graph,
        owner_ready,
        reserve_tick,
    )
    from hermes_state_runtime import RuntimeStoreError

    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    try:
        authorities = list(runner.session_authorities)
        assert len(authorities) == 2
        assert authorities[0].epoch != authorities[1].epoch
        assert has_i08_source_graph(runner)
        runner._running = True
        receipt = await _close_receipt(runner)
        from gateway import run_runtime
        prepared = await run_runtime.prepare_preservation_handoff(
            runner, {"receipt": receipt.as_wire()}, "i08-test-peer",
        )
        assert prepared["status"] == "refused"
        assert runner.runtime_custody.status()["owner_states"][
            "I08_goals_loops_heartbeat"
        ] == "sealed"
        for authority in authorities:
            with pytest.raises(RuntimeStoreError, match="runtime_draining"):
                reserve_tick(
                    authority,
                    request_id="late-i08-request",
                    principal_id="automation:test",
                    session_id="late-i08-session",
                    source_identity="test:late-i08",
                    kind="heartbeat",
                )
            snapshot = _i08_zero_work_snapshot(authority, receipt.close_generation)
            assert snapshot is not None
            assert snapshot["zero_work"] is True
            assert snapshot["authority_epoch"] == authority.epoch
            assert owner_ready(authority, close_generation=receipt.close_generation)
    finally:
        if receipt is not None and runner.runtime_custody.status()["state"] == "closing":
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


def test_i08_legacy_graph_rebind_is_idempotent_and_config_changes_do_not_replace_it(
    tmp_path,
):
    """A legacy embedded authority has no optional native lock, but its source graph is real.

    Some desktop/session fixtures bind the same runner twice while profile config is edited between
    turns.  Reusing the exact open coordinator keeps the graph and its source state authenticated;
    a config change is not a source-owner replacement.
    """
    from tests.gateway.conftest import bind_authority_runtime_custody
    from tests.gateway.test_session_follow_profile_config import _owner
    from gateway.session_goal_tick_custody import has_i08_source_graph

    home = tmp_path / "legacy-home"
    home.mkdir()
    store, runner, authority, _owner_connection = _owner(home)
    try:
        first = runner.runtime_custody
        graph = authority._i08_source_graph
        assert graph.physical_owner_epoch is None
        assert graph.native_descriptor is None
        assert graph.physical_binding is None
        assert has_i08_source_graph(runner) is True

        (home / "config.yaml").write_text(
            '{"model": {"provider": "custom", "default": "before"}}',
            encoding="utf-8",
        )
        second = bind_authority_runtime_custody(runner, authority)

        assert second is first
        assert authority._i08_source_graph is graph
        assert has_i08_source_graph(runner) is True
        (home / "config.yaml").write_text(
            '{"model": {"provider": "custom", "default": "after"}}',
            encoding="utf-8",
        )
        assert has_i08_source_graph(runner) is True
    finally:
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_i08_close_cancels_real_watchers_and_resumes_same_families(
    tmp_path, monkeypatch,
):
    from gateway.session_goal_tick_custody import (
        begin_i08_source_close,
        complete_i08_source_resume,
        resume_i08_source_close,
    )

    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    watcher_tasks = []
    try:
        runner._running = True
        runner._start_heartbeat_poller()
        runner._spawn_supervised(
            lambda: runner._loop_wakeup_watcher(interval=30),
            "i08-test-loop-wakeup",
            restart=False,
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        histories = [
            authority._i08_source_state["task_history"]
            for authority in runner.session_authorities
        ]
        assert all(any(record["state"] == "running" for record in history.values()) for history in histories)
        watcher_tasks = list(runner._i08_watcher_tasks)
        assert watcher_tasks

        receipt = await _close_receipt(runner)
        assert await begin_i08_source_close(runner, receipt.close_generation, 10)
        assert all(
            not any(record["state"] == "running" for record in authority._i08_source_state["task_history"].values())
            for authority in runner.session_authorities
        )

        assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await resume_i08_source_close(runner, receipt.close_generation, 10)
        assert complete_i08_source_resume(runner, receipt.close_generation)
        assert all(
            authority._i08_source_state["phase"] == "open"
            for authority in runner.session_authorities
        )
    finally:
        runner._running = False
        for task in tuple(getattr(runner, "_i08_watcher_tasks", ())):
            task.cancel()
        for task in tuple(getattr(runner, "_background_tasks", ())):
            if task is not asyncio.current_task():
                task.cancel()
        if watcher_tasks:
            await asyncio.gather(*watcher_tasks, return_exceptions=True)
        if receipt is not None and runner.runtime_custody.status()["state"] == "closing":
            runner.runtime_custody.abort_before_handoff(receipt)
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i08_replaced_graph_cannot_prove_zero_work(
    tmp_path, monkeypatch,
):
    from dataclasses import replace

    from gateway.session_goal_tick_custody import (
        _i08_zero_work_snapshot,
        begin_i08_source_close,
    )

    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    try:
        authority = runner.session_authorities.launch
        graph = authority._i08_source_graph
        receipt = await _close_receipt(runner)
        assert await begin_i08_source_close(runner, receipt.close_generation, 10)
        authority._i08_source_graph = replace(graph, database=object())
        assert _i08_zero_work_snapshot(authority, receipt.close_generation) is None
    finally:
        if receipt is not None and runner.runtime_custody.status()["state"] == "closing":
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i08_hot_serve_registers_source_graph_before_recovery(
        tmp_path, monkeypatch,
):
    from gateway.run_runtime import serve_profile_runtime
    from gateway.runtime_ownership import process_ownership
    from gateway.session_goal_tick_custody import has_i08_source_graph

    runner, entries = await _boot(tmp_path, monkeypatch)
    entries = list(entries)
    try:
        home = entries[0][1] / "profiles" / "i08-hot"
        home.mkdir(parents=True, mode=0o700)
        (home / "config.yaml").write_text("{}")
        process_ownership.reserve([home])
        entries.append(("i08-hot", home))
        authority = await serve_profile_runtime(runner, "i08-hot", home)
        assert runner.session_authorities.for_home(home) is authority
        assert authority._i08_source_graph is not None
        assert has_i08_source_graph(runner) is True
    finally:
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i08_zero_work_retains_bound_native_owner_after_reacquire(
        tmp_path, monkeypatch,
):
    from gateway.runtime_ownership import process_ownership
    from gateway.session_goal_tick_custody import (
        _i08_zero_work_snapshot, begin_i08_source_close,
    )

    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    try:
        authority = runner.session_authorities.launch
        home = authority.profile_id
        bound = process_ownership.reservation_evidence(home)
        assert isinstance(bound, dict)
        receipt = await _close_receipt(runner)
        assert await begin_i08_source_close(runner, receipt.close_generation, 10)
        assert _i08_zero_work_snapshot(authority, receipt.close_generation) is not None

        assert process_ownership.release(home) is True
        process_ownership.reserve([home])
        replacement = process_ownership.reservation_evidence(home)
        assert isinstance(replacement, dict)
        assert replacement["owner_epoch"] != bound["owner_epoch"]
        assert _i08_zero_work_snapshot(authority, receipt.close_generation) is None
    finally:
        if receipt is not None and runner.runtime_custody.status()["state"] == "closing":
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i08_close_uses_one_absolute_deadline_and_rejects_zero_budget(
        tmp_path, monkeypatch,
):
    import gateway.session_goal_tick_custody as custody

    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    seen_deadlines = []
    original_wait = custody._await_i08_tasks

    async def record_deadline(tasks, deadline):
        seen_deadlines.append(deadline)
        return await original_wait(tasks, deadline)

    monkeypatch.setattr(custody, "_await_i08_tasks", record_deadline)
    try:
        receipt = await _close_receipt(runner)
        loop = asyncio.get_running_loop()
        started = loop.time()
        assert await custody.begin_i08_source_close(
            runner, receipt.close_generation, 0.25,
        )
        assert len(seen_deadlines) == 2
        assert seen_deadlines[0] == seen_deadlines[1]
        assert 0.20 < seen_deadlines[0] - started < 0.5
    finally:
        if receipt is not None and runner.runtime_custody.status()["state"] == "closing":
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i08_close_rejects_zero_budget_before_source_effects(tmp_path, monkeypatch):
    import gateway.session_goal_tick_custody as custody

    runner, entries = await _boot(tmp_path, monkeypatch)
    receipt = None
    try:
        receipt = await _close_receipt(runner)
        assert await custody.begin_i08_source_close(
            runner, receipt.close_generation, 0.0,
        ) is False
        assert all(
            authority._i08_source_state["phase"] == "open"
            for authority in runner.session_authorities
        )
    finally:
        if receipt is not None and runner.runtime_custody.status()["state"] == "closing":
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        assert await _close_boot(runner, entries) is True
