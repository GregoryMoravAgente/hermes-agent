"""Gateway parent/child identity and physical-drain contracts."""

from __future__ import annotations

import asyncio
import concurrent.futures
import threading
from types import SimpleNamespace

import pytest


def _authority():
    return SimpleNamespace(_turn_workers={}, retiring=False, retired=False)


def _worker(owner):
    worker = SimpleNamespace(worker_done=threading.Event())
    return worker, [owner]


def _real_authority_parent(tmp_path):
    """Build the production authority/parent boundary with a real state.db."""
    from gateway.session_authority import SessionAuthority
    from gateway.session_runtime_workers import _TurnToolWorkerParent
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch

    db = SessionDB(tmp_path / "state.db")
    runner = SimpleNamespace(_draining=False)
    authority = SessionAuthority(
        runner,
        profile_id=str(tmp_path),
        instance_id="nested-tool-test",
        db=db,
        epoch=begin_runtime_epoch(db, instance_id="nested-tool-test"),
    )
    owner = SimpleNamespace(
        _interrupt_requested=False,
        _tool_interrupt_reason=None,
        _tool_worker_threads=set(),
        _tool_worker_threads_lock=threading.Lock(),
        log_prefix="",
        _touch_activity=lambda *_args: None,
        _vprint=lambda *_args, **_kwargs: None,
    )
    worker = SimpleNamespace(worker_done=threading.Event())
    holder = [owner]
    parent = _TurnToolWorkerParent(authority, worker, holder)
    worker._tool_worker_parent = parent
    authority._turn_workers = {}
    authority._turn_workers[id(worker)] = (worker, holder)
    return db, authority, owner, worker, parent


def test_finished_outer_worker_remains_physical_until_nested_handle_settles():
    from agent.tool_worker_custody import ToolWorkerHandle
    from gateway.session_runtime_workers import _TurnToolWorkerParent, physical_workers

    authority = _authority()
    owner = object()
    worker, holder = _worker(owner)
    parent = _TurnToolWorkerParent(authority, worker, holder)
    worker._tool_worker_parent = parent
    authority._turn_workers[id(worker)] = (worker, holder)
    child = ToolWorkerHandle(route="sequential", owner=owner, parent=parent)

    parent.admit_tool_worker(child)
    worker.worker_done.set()
    assert list(physical_workers(authority)) == [(worker, holder)]
    assert id(worker) in authority._turn_workers

    child.mark_body_done()
    child.mark_finalizers_done()
    assert list(physical_workers(authority)) == []
    assert authority._turn_workers == {}


def test_replaced_parent_cannot_admit_or_settle_successor_children():
    from agent.tool_worker_custody import ToolWorkerAdmissionError, ToolWorkerHandle
    from gateway.session_runtime_workers import _TurnToolWorkerParent

    authority = _authority()
    owner_a, owner_b = object(), object()
    worker_a, holder_a = _worker(owner_a)
    parent_a = _TurnToolWorkerParent(authority, worker_a, holder_a)
    worker_a._tool_worker_parent = parent_a
    authority._turn_workers[id(worker_a)] = (worker_a, holder_a)

    child_a = ToolWorkerHandle(route="concurrent", owner=owner_a, parent=parent_a)
    parent_a.admit_tool_worker(child_a)

    worker_b, holder_b = _worker(owner_b)
    parent_b = _TurnToolWorkerParent(authority, worker_b, holder_b)
    worker_b._tool_worker_parent = parent_b
    authority._turn_workers[id(worker_a)] = (worker_b, holder_b)

    with pytest.raises(ToolWorkerAdmissionError, match="not_current"):
        parent_a.admit_tool_worker(ToolWorkerHandle(route="prepared-terminal", owner=owner_a, parent=parent_a))
    # The old child can settle its own exact parent, but cannot affect the successor
    # entry that now occupies the authority's worker key.
    child_a.mark_body_done()
    child_a.mark_finalizers_done()
    assert authority._turn_workers[id(worker_a)] == (worker_b, holder_b)
    assert parent_b.pending_tool_workers() == ()


def test_sibling_turns_have_isolated_child_sets():
    from agent.tool_worker_custody import ToolWorkerHandle
    from gateway.session_runtime_workers import _TurnToolWorkerParent

    authority = _authority()
    owner_a, owner_b = object(), object()
    worker_a, holder_a = _worker(owner_a)
    worker_b, holder_b = _worker(owner_b)
    parent_a = _TurnToolWorkerParent(authority, worker_a, holder_a)
    parent_b = _TurnToolWorkerParent(authority, worker_b, holder_b)
    worker_a._tool_worker_parent = parent_a
    worker_b._tool_worker_parent = parent_b
    authority._turn_workers[id(worker_a)] = (worker_a, holder_a)
    authority._turn_workers[id(worker_b)] = (worker_b, holder_b)

    child_a = ToolWorkerHandle(route="sequential", owner=owner_a, parent=parent_a)
    child_b = ToolWorkerHandle(route="concurrent", owner=owner_b, parent=parent_b)
    parent_a.admit_tool_worker(child_a)
    parent_b.admit_tool_worker(child_b)
    child_a.mark_body_done()
    child_a.mark_finalizers_done()

    assert parent_a.pending_tool_workers() == ()
    assert parent_b.pending_tool_workers() == (child_b,)


def test_real_delegated_child_uses_exact_parent_lineage_and_generation(tmp_path, monkeypatch):
    """A real built child is admitted; an unregistered sibling and late child are refused."""
    from agent.tool_worker_custody import ToolWorkerAdmissionError, admit_tool_worker, bind_tool_worker_parent
    from gateway.session_runtime_workers import _TurnToolWorkerParent
    from run_agent import AIAgent
    from tools.delegate_tool import _build_child_agent

    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("HERMES_RUNTIME_DIR", str(tmp_path / "runtime"))
    (tmp_path / "runtime").mkdir()
    outer = AIAgent(
        api_key="test-key", base_url="http://127.0.0.1:9/v1", provider="openai",
        model="test-model", max_iterations=1, quiet_mode=True,
        skip_context_files=True, skip_memory=True, load_soul_identity=False,
        session_id="outer-delegate-lineage", platform="cli",
    )
    authority = SimpleNamespace(
        _turn_workers={}, retiring=False, retired=False, profile_id=str(tmp_path), epoch="generation-1",
    )
    worker = SimpleNamespace(worker_done=threading.Event(), _tool_worker_parent=None)
    holder = [outer]
    parent = _TurnToolWorkerParent(authority, worker, holder)
    worker._tool_worker_parent = parent
    authority._turn_workers[id(worker)] = (worker, holder)
    child = sibling = None
    try:
        with bind_tool_worker_parent(parent):
            child = _build_child_agent(
                task_index=0, goal="real nested custody", context=None, toolsets=None,
                model="test-model", max_iterations=1, task_count=1, parent_agent=outer,
            )
            handle = admit_tool_worker(child, "sequential")
        assert handle.owner is child
        assert handle.parent is parent
        handle.mark_body_done()
        handle.mark_finalizers_done()

        # Build a genuine second AIAgent with the same lineage outside the
        # accepted context.  Its matching names do not make it an admitted
        # source-owned sibling.
        sibling = _build_child_agent(
            task_index=1, goal="unregistered sibling", context=None, toolsets=None,
            model="test-model", max_iterations=1, task_count=1, parent_agent=outer,
        )
        with bind_tool_worker_parent(parent), pytest.raises(
            ToolWorkerAdmissionError, match="nested_tool_worker_parent_not_current"
        ):
            admit_tool_worker(sibling, "sequential")

        authority.epoch = "generation-2"
        with bind_tool_worker_parent(parent), pytest.raises(
            ToolWorkerAdmissionError, match="nested_tool_worker_parent_not_current"
        ):
            admit_tool_worker(child, "sequential")

        authority.epoch = "generation-1"
        authority.profile_id = "foreign-profile"
        with bind_tool_worker_parent(parent), pytest.raises(
            ToolWorkerAdmissionError, match="nested_tool_worker_parent_not_current"
        ):
            admit_tool_worker(child, "sequential")
    finally:
        for agent in (sibling, child, outer):
            if agent is not None:
                try:
                    agent.close()
                except Exception:
                    pass


@pytest.mark.asyncio
async def test_turn_executor_propagates_exact_parent_into_nested_submission():
    from agent.tool_worker_custody import current_tool_worker_parent
    from gateway.session_runtime_workers import physical_workers, start_turn_worker

    authority = _authority()
    owner = object()
    worker, holder = _worker(owner)

    async def run_in_executor(fn):
        return await asyncio.to_thread(fn)

    runner = SimpleNamespace(
        session_authority=authority,
        _run_in_executor_with_context=run_in_executor,
    )
    authority.runner = runner
    nested_parent_seen = []

    def run_sync():
        nested_parent_seen.append(current_tool_worker_parent())
        worker.worker_done.set()
        return "turn complete"

    start_turn_worker(runner, worker, holder, run_sync)
    assert await worker.executor_task == "turn complete"
    assert nested_parent_seen == [worker._tool_worker_parent]
    assert list(physical_workers(authority)) == []


def test_real_sequential_route_retains_accepted_worker_after_deadline(tmp_path, monkeypatch):
    """The actual sequential middleware route retains its child on timeout."""
    import agent.tool_executor as te
    from agent.tool_executor import _ManagedToolResult, _ToolTimeoutResult
    from agent.tool_worker_custody import bind_tool_worker_parent

    db, _authority, agent, _worker, parent = _real_authority_parent(tmp_path)
    started = threading.Event()
    release = threading.Event()

    def blocked_middleware(_agent, **_kwargs):
        started.set()
        assert release.wait(5)
        return _ManagedToolResult(
            result="real sequential result", args={}, middleware_trace=[], blocked=False, dispatched=True,
        )

    monkeypatch.setattr(te, "_run_agent_tool_execution_middleware", blocked_middleware)
    monkeypatch.setattr(te, "_resolve_sequential_tool_timeout", lambda: 0.05)
    monkeypatch.setattr(te, "_emit_terminal_post_tool_call", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(te, "_interrupt_worker_tids", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(te, "_ra", lambda: SimpleNamespace(_set_interrupt=lambda *_a, **_k: None))

    try:
        with bind_tool_worker_parent(parent):
            managed = te._run_sequential_tool_execution_middleware(
                agent,
                function_name="blocked_tool",
                function_args={},
                effective_task_id="task",
                tool_call_id="seq-real",
                execute=lambda *_args, **_kwargs: "unused",
            )
        assert started.is_set()
        assert isinstance(managed.result, _ToolTimeoutResult)
        children = parent.pending_tool_workers()
        assert len(children) == 1
        child = children[0]
        assert child.future is not None and child.pending
        release.set()
        assert child.future.result(timeout=5).result == "real sequential result"
        assert parent.pending_tool_workers() == ()
    finally:
        release.set()
        db.close()


def test_real_concurrent_route_detaches_wedged_accepted_worker(tmp_path, monkeypatch):
    """The actual concurrent batch route never joins an accepted wedged child."""
    import agent.tool_executor as te
    from agent.tool_worker_custody import bind_tool_worker_parent

    db, _authority, agent, _worker, parent = _real_authority_parent(tmp_path)
    started = threading.Event()
    release = threading.Event()
    call = SimpleNamespace(id="conc-real", function=SimpleNamespace(name="blocked_tool", arguments="{}"))
    parsed = te._ParsedCall(call, "blocked_tool", {}, [], None, None)
    batch = te._ConcurrentBatch(agent, [], "task", [parsed], timeout_s=0.05)

    def blocked_run(_index, _order):
        started.set()
        assert release.wait(5)

    batch.run_worker = blocked_run
    monkeypatch.setattr(te, "_interrupt_worker_tids", lambda *_args, **_kwargs: None)

    try:
        with bind_tool_worker_parent(parent):
            batch.run()
        assert started.is_set()
        assert batch.timed_out_indices == {0}
        children = parent.pending_tool_workers()
        assert len(children) == 1
        child = children[0]
        assert child.future is not None and child.pending
        release.set()
        assert child.future.result(timeout=5) is None
        assert parent.pending_tool_workers() == ()
    finally:
        release.set()
        db.close()


def test_real_terminal_batch_route_binds_owned_future(tmp_path, monkeypatch):
    """Prepared terminal admission uses the executor's owned Future identity."""
    import agent.terminal_approval_batch as tab
    import agent.tool_executor as te
    from agent.tool_worker_custody import bind_tool_worker_parent

    db, _authority, agent, _worker, parent = _real_authority_parent(tmp_path)
    call = SimpleNamespace(id="terminal-real", function=SimpleNamespace(name="terminal", arguments="{}"))
    parsed = te._ParsedCall(call, "terminal", {}, [], None, None)
    batch = tab._TerminalBatch(agent, [], "task", [parsed])
    slot = batch.slots[0]

    def ready_run():
        slot.ready.set()
        return "prepared"

    slot.run = ready_run
    monkeypatch.setattr(te, "_resolve_sequential_tool_timeout", lambda: 1.0)
    try:
        with bind_tool_worker_parent(parent):
            batch.start()
        assert slot.future is not None
        assert slot.lease.future is slot.future
        assert slot.future.result(timeout=5) == "prepared"
        assert parent.pending_tool_workers() == ()
    finally:
        batch.close()
        db.close()


def test_real_sequential_route_binds_legacy_prepared_future(tmp_path, monkeypatch):
    """A legacy prepared slot is attached to its exact existing Future."""
    import agent.terminal_approval_batch as tab
    import agent.tool_executor as te
    from agent.tool_executor import _ManagedToolResult
    from agent.tool_worker_custody import bind_tool_worker_parent

    db, _authority, agent, _worker, parent = _real_authority_parent(tmp_path)
    future = concurrent.futures.Future()
    result = _ManagedToolResult(
        result="legacy prepared result", args={}, middleware_trace=[], blocked=False, dispatched=True,
    )
    future.set_result(result)
    prepared = SimpleNamespace(
        batch=SimpleNamespace(authorization_gate=te._ConcurrentToolAuthorizationGate(), executor=None),
        tids=[], future=future, lease=None,
    )
    monkeypatch.setattr(tab, "take_prepared_call", lambda _call_id: prepared)
    monkeypatch.setattr(te, "_resolve_sequential_tool_timeout", lambda: 1.0)
    monkeypatch.setattr(te, "_emit_terminal_post_tool_call", lambda *_args, **_kwargs: None)
    try:
        with bind_tool_worker_parent(parent):
            managed = te._run_sequential_tool_execution_middleware(
                agent,
                function_name="legacy_tool",
                function_args={},
                effective_task_id="task",
                tool_call_id="legacy-real",
                execute=lambda *_args, **_kwargs: "unused",
            )
        assert managed.result == "legacy prepared result"
        retained = parent.pending_tool_workers()
        assert len(retained) == 1
        assert retained[0].future is future
        assert retained[0].pending
        assert retained[0].state == "unknown"
    finally:
        db.close()


def test_real_sequential_route_detaches_when_owned_submit_has_no_future(tmp_path, monkeypatch):
    """An accepted/ambiguous queue handoff cannot make the sequential route join forever."""
    import agent.tool_executor as te
    from tools import daemon_pool
    from agent.tool_worker_custody import ToolWorkerSubmissionError, bind_tool_worker_parent
    from tools.daemon_pool import OwnedSubmission

    db, _authority, agent, _worker, parent = _real_authority_parent(tmp_path)

    class UnknownExecutor:
        def __init__(self, *args, **kwargs):
            self.waits = []
            created.append(self)

        def submit_owned(self, handoff_id, fn, *args, **kwargs):
            return OwnedSubmission(handoff_id, None, "unknown", RuntimeError("worker start ambiguous"))

        def shutdown(self, *, wait, cancel_futures):
            self.waits.append((wait, cancel_futures))

    created = []
    monkeypatch.setattr(daemon_pool, "DaemonThreadPoolExecutor", UnknownExecutor)
    try:
        with bind_tool_worker_parent(parent), pytest.raises(ToolWorkerSubmissionError) as excinfo:
            te._run_sequential_tool_execution_middleware(
                agent,
                function_name="blocked_tool",
                function_args={},
                effective_task_id="task",
                tool_call_id="seq-unknown",
                execute=lambda *_args, **_kwargs: "unused",
            )
        assert excinfo.value.accepted == "unknown"
        children = parent.pending_tool_workers()
        assert len(children) == 1 and children[0].state == "unknown"
        assert created and created[0].waits == [(False, False)]
    finally:
        db.close()


def test_real_concurrent_route_detaches_unknown_submission(tmp_path, monkeypatch):
    """An unknown submission without a Future retains its child and skips pool join."""
    import agent.tool_executor as te
    from agent.tool_worker_custody import ToolWorkerSubmissionError, bind_tool_worker_parent

    db, _authority, agent, _worker, parent = _real_authority_parent(tmp_path)
    call = SimpleNamespace(id="conc-unknown", function=SimpleNamespace(name="blocked_tool", arguments="{}"))
    parsed = te._ParsedCall(call, "blocked_tool", {}, [], None, None)
    batch = te._ConcurrentBatch(agent, [], "task", [parsed], timeout_s=0.05)

    def unknown_submit(executor, handle, fn, *args, **kwargs):
        handle.mark_submit_unknown()
        raise ToolWorkerSubmissionError(
            "worker start ambiguous", handle=handle, accepted="unknown", error=RuntimeError("worker start ambiguous"),
        )

    monkeypatch.setattr(te, "submit_tool_worker", unknown_submit)
    try:
        with bind_tool_worker_parent(parent):
            batch.run()
        assert batch.submission_abandoned
        children = parent.pending_tool_workers()
        assert len(children) == 1 and children[0].state == "unknown"
        assert batch.results[0] is not None
        assert "remained unknown" in str(batch.results[0].result)
    finally:
        db.close()
