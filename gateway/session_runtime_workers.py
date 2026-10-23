"""Physical worker liveness and cooperative Stop across every served authority."""
import asyncio
import os
import threading

from gateway.session_authorities import all_authorities


class _TurnToolWorkerParent:
    """Opaque gateway owner for nested tool workers of one exact turn worker.

    The agent executor only sees this object's two methods.  The parent retains
    object identity, profile authority and the submitting agent, so a late child
    from a superseded turn cannot settle or borrow a successor's worker slot.
    """

    def __init__(self, authority, worker, holder):
        self.authority = authority
        self.worker = worker
        self.holder = holder
        self._children = {}
        self._delegates = {}
        self._profile_id = getattr(authority, 'profile_id', None)
        self._authority_epoch = getattr(authority, 'epoch', None)
        self._lock = threading.Lock()

    def _is_current(self) -> bool:
        workers = getattr(self.authority, '_turn_workers', {})
        entry = workers.get(id(self.worker))
        return bool(
            entry is not None
            and entry[0] is self.worker
            and getattr(self.worker, '_tool_worker_parent', None) is self
            and not getattr(self.authority, 'retiring', False)
            and not getattr(self.authority, 'retired', False)
            and (
                self._authority_epoch is None
                or getattr(self.authority, 'epoch', None) == self._authority_epoch
            )
            and (
                self._profile_id is None
                or getattr(self.authority, 'profile_id', None) == self._profile_id
            )
        )

    @staticmethod
    def _parent_ref(agent):
        ref = getattr(agent, '_delegate_parent_ref', None)
        if ref is None or not callable(ref):
            return None
        try:
            return ref()
        except (ReferenceError, TypeError):
            return None

    @staticmethod
    def _profile_home(agent):
        db_path = getattr(getattr(agent, '_session_db', None), 'db_path', None)
        if not isinstance(db_path, (str, os.PathLike)):
            return None
        try:
            return os.path.normcase(os.path.dirname(os.fspath(db_path)))
        except (TypeError, ValueError):
            return None

    def _same_profile(self, parent_agent, child_agent) -> bool:
        parent_home = self._profile_home(parent_agent)
        child_home = self._profile_home(child_agent)
        if parent_home is not None and child_home is not None and parent_home != child_home:
            return False
        return True

    def _record_current(self, record, seen=None) -> bool:
        if seen is None:
            seen = set()
        marker = id(record)
        if marker in seen:
            return False
        seen.add(marker)
        if not self._is_current():
            return False
        if record.get('worker') is not self.worker:
            return False
        parent_agent = record.get('parent_agent')
        child_agent = record.get('agent')
        if parent_agent is None or child_agent is None:
            return False
        if self._parent_ref(child_agent) is not parent_agent:
            return False
        expected_parent_id = getattr(parent_agent, '_subagent_id', None)
        if getattr(child_agent, '_parent_subagent_id', None) != expected_parent_id:
            return False
        expected_depth = getattr(parent_agent, '_delegate_depth', 0) + 1
        if getattr(child_agent, '_delegate_depth', None) != expected_depth:
            return False
        expected_session = getattr(parent_agent, 'session_id', None)
        if getattr(child_agent, '_parent_session_id', None) != expected_session:
            return False
        if not self._same_profile(parent_agent, child_agent):
            return False
        if record.get('profile_id') != self._profile_id:
            return False
        if record.get('authority_epoch') != self._authority_epoch:
            return False
        if self.holder and parent_agent is self.holder[0]:
            return True
        parent_record = self._delegates.get(id(parent_agent))
        return bool(
            parent_record is not None
            and parent_record.get('agent') is parent_agent
            and self._record_current(parent_record, seen)
        )

    def register_delegate_agent(self, child_agent) -> bool:
        """Register a built delegate in this exact turn's lineage.

        ``AIAgent`` delegation intentionally creates a new object.  Admission
        accepts that object only after the builder has supplied its real weak
        parent, subagent lineage, depth and session binding while this parent
        is current.  The registration is retained for the parent worker's
        lifetime, so a late nested tool cannot fall through to a sibling.
        """
        from agent.tool_worker_custody import ToolWorkerAdmissionError

        with self._lock:
            if not self._is_current() or not self.holder or self.holder[0] is None:
                raise ToolWorkerAdmissionError('nested_tool_worker_parent_not_current')
            parent_agent = self._parent_ref(child_agent)
            if parent_agent is None:
                raise ToolWorkerAdmissionError('nested_tool_worker_delegate_parent_missing')
            root_agent = self.holder[0]
            if parent_agent is not root_agent:
                parent_record = self._delegates.get(id(parent_agent))
                if (
                    parent_record is None
                    or parent_record.get('agent') is not parent_agent
                    or not self._record_current(parent_record)
                ):
                    raise ToolWorkerAdmissionError('nested_tool_worker_delegate_lineage_not_current')
            expected_parent_id = getattr(parent_agent, '_subagent_id', None)
            if getattr(child_agent, '_parent_subagent_id', None) != expected_parent_id:
                raise ToolWorkerAdmissionError('nested_tool_worker_delegate_parent_id_mismatch')
            if getattr(child_agent, '_delegate_depth', None) != getattr(parent_agent, '_delegate_depth', 0) + 1:
                raise ToolWorkerAdmissionError('nested_tool_worker_delegate_depth_mismatch')
            if getattr(child_agent, '_parent_session_id', None) != getattr(parent_agent, 'session_id', None):
                raise ToolWorkerAdmissionError('nested_tool_worker_delegate_session_mismatch')
            if not self._same_profile(parent_agent, child_agent):
                raise ToolWorkerAdmissionError('nested_tool_worker_delegate_profile_mismatch')
            existing_parent = getattr(child_agent, '_tool_worker_parent', None)
            if existing_parent is not None and existing_parent is not self:
                raise ToolWorkerAdmissionError('nested_tool_worker_delegate_parent_rebind')
            record = {
                'agent': child_agent,
                'parent_agent': parent_agent,
                'worker': self.worker,
                'profile_id': self._profile_id,
                'authority_epoch': self._authority_epoch,
            }
            self._delegates[id(child_agent)] = record
            try:
                child_agent._tool_worker_parent = self
            except Exception as exc:
                self._delegates.pop(id(child_agent), None)
                raise ToolWorkerAdmissionError('nested_tool_worker_delegate_owner_unbindable') from exc
            return True

    def _registered_delegate_current(self, owner) -> bool:
        record = self._delegates.get(id(owner))
        return bool(record is not None and record.get('agent') is owner and self._record_current(record))

    def admit_tool_worker(self, child) -> None:
        from agent.tool_worker_custody import ToolWorkerAdmissionError

        with self._lock:
            owner = self.holder[0] if self.holder else None
            owner_is_current = child.owner is owner or self._registered_delegate_current(child.owner)
            if not self._is_current() or owner is None or not owner_is_current:
                raise ToolWorkerAdmissionError('nested_tool_worker_parent_not_current')
            if child.parent is not self:
                raise ToolWorkerAdmissionError('nested_tool_worker_parent_mismatch')
            self._children[child.handle_id] = child

    def settle_tool_worker(self, child) -> None:
        with self._lock:
            if self._children.get(child.handle_id) is child:
                del self._children[child.handle_id]

    def has_pending_tool_workers(self) -> bool:
        with self._lock:
            return any(child.pending for child in self._children.values())

    def pending_tool_workers(self):
        with self._lock:
            return tuple(self._children.values())


def _notify_write_failure(callback, error):
    """Invoke an ``on_failure`` callback with its supported arity.

    Older custody reconciliation callbacks are deliberately bound with ``functools.partial``
    and accept only their identity keywords; newer observers accept the worker exception.  The
    callback is an observation after the shielded write has finished, so supporting both shapes
    here keeps a signature mismatch from skipping reconciliation while still propagating an
    actual callback failure.
    """
    try:
        import inspect
        signature = inspect.signature(callback)
    except (TypeError, ValueError):
        # Preserve the historical error-aware contract for callables whose signature cannot be
        # inspected (for example some extension callables).
        callback(error)
        return
    try:
        signature.bind(error)
    except TypeError:
        callback()
    else:
        callback(error)


def executing_sessions(authority):
    for sid, live in getattr(authority, 'sessions', {}).items():
        execution = getattr(getattr(live, 'event_stream', None), 'execution', None) or {}
        task = live.task
        if execution.get('execution_generation') is not None and task is not None and not task.done():
            yield sid, live, execution['execution_generation']


def uncounted_runtime_work(runner):
    """Count claims absent from the ordinary agent map, including pre-bootstrap managed turns."""
    running = getattr(runner, '_running_agents', {})
    authorities = all_authorities(runner)
    managed = sum(not worker.closed.is_set() for authority in authorities
                  for worker in getattr(authority, '_managed_workers', {}).values())
    return (managed + sum(live.route not in running and sid not in getattr(authority, '_managed_workers', {})
                for authority in authorities for sid, live, _ in executing_sessions(authority))
            + sum(len(mutation_tasks(authority)) + len(side_agents(authority)) for authority in authorities))


def side_agents(authority):
    """``(thread, holder)`` for each detached side agent (/bg, /btw, preview restart) whose thread
    has not unwound (``gateway/session_tui_side.py`` ``_spawn``). A stopped ``/btw`` whose fork
    is gone (``_SideQuestionStop.abandoned``) is no longer owner work."""
    from gateway.session_tui_side import _abandoned
    return [(thread, holder) for thread, holder in list(getattr(authority, '_side_agents', {}).items())
            if not _abandoned(holder)]


def interrupt_side_agents(runner, reason):
    """Hard-interrupt every counted side agent's holder across served authorities (the stop
    drain waits for them, so its timed-out interrupt must reach them). Returns how many."""
    from agent.interrupt_compat import request_hard_interrupt
    count = 0
    for authority in all_authorities(runner):
        for _, holder in side_agents(authority):
            if holder and holder[0] is not None:
                count += bool(request_hard_interrupt(holder[0], reason))
    return count


def persist_work_count(authority, _task=None):
    """Re-publish the runner's in-flight count when owner work it counts (a drain, a tracked
    mutation, a mailbox operation) ends: the agent-slot release that last wrote
    ``gateway_state.json`` ran while that work was still live, so without this write the
    persisted count never returns to zero."""
    persist = getattr(getattr(authority, 'runner', None), '_persist_active_agents', None)
    if persist is not None:
        persist()


def mutation_tasks(authority):
    tasks = {*getattr(authority, '_mutation_tasks', ()), *getattr(authority, '_bot_mailbox_operations', ())}
    return [task for task in tasks if not task.done()]


def track_mutation(authority, operation):
    tasks = getattr(authority, '_mutation_tasks', None)
    if tasks is None:
        tasks = authority._mutation_tasks = set()
    task = asyncio.create_task(operation)
    tasks.add(task)
    def finished(done):
        tasks.discard(done)
        if not done.cancelled():
            done.exception()  # the observer may already have disconnected
        persist_work_count(authority)
    task.add_done_callback(finished)
    return task


async def tracked_write(authority, write, then=None, *, ordered=False, after=None,
                        on_cancel=None, on_failure=None):
    """Run a synchronous ledger write (``write()``) in a worker thread, off the owner loop.

    A held SQLite writer (``_WRITE_PATIENCE_S``) must not freeze every session, timer, socket,
    Stop and approval. The write and its loop-side follow-up (``then(result)``, sync or a
    coroutine: publication, scheduling, the claim's execution stamp) are one task tracked like ``track_mutation``, so
    retirement joins it, and shielded, so a cancelled caller cannot separate a commit from it.
    ``ordered`` writes (admissions) commit in call order, as they did on the loop: worker threads
    alone would let a later input take an earlier FIFO position.

    ``after(result)`` (an admission's drain scheduling) runs in the CALLER's own step once the
    write returns, as it did when admission was synchronous: the caller registers its delivery
    waiter before any drain it scheduled can claim, settle or pause that admission. A caller
    cancelled while the commit is in flight hands ``after`` to the commit's completion instead,
    so a committed row is never stranded without a drain. ``on_cancel`` and ``on_failure`` are
    late-only custody callbacks: they run after a shielded worker finishes when the caller had
    already been cancelled, and never authorize releasing a write that is still running."""
    order = getattr(authority, '_ordered_writes', None)
    if ordered and order is None:
        order = authority._ordered_writes = asyncio.Lock()

    async def run():
        if ordered:
            async with order:
                result = await asyncio.to_thread(write)
        else:
            result = await asyncio.to_thread(write)
        if then is None:
            return result
        followed = then(result)
        return await followed if asyncio.iscoroutine(followed) else followed
    task = track_mutation(authority, run())
    if after is None and on_cancel is None and on_failure is None:
        return await asyncio.shield(task)
    try:
        result = await asyncio.shield(task)
    except asyncio.CancelledError:
        def late(done):
            if done.cancelled():
                if on_failure is not None:
                    _notify_write_failure(on_failure, asyncio.CancelledError())
                return
            error = done.exception()
            if error is not None:
                if on_failure is not None:
                    _notify_write_failure(on_failure, error)
                return
            if on_cancel is not None:
                on_cancel(done.result())
            elif after is not None:
                after(done.result())
        task.add_done_callback(late)
        raise
    if after is not None:
        after(result)
    return result


def start_turn_worker(runner, worker, agent_holder, run_sync):
    authority = track_turn_worker(runner, worker, agent_holder)
    parent = getattr(worker, '_tool_worker_parent', None)
    if parent is None:
        worker.executor_task = asyncio.ensure_future(runner._run_in_executor_with_context(run_sync))
    else:
        # Bind the exact parent around the callable copied into the turn executor.  The
        # agent's own nested pools inherit this ContextVar through their existing context
        # propagation, without importing gateway into the generic tool executor.
        from agent.tool_worker_custody import bind_tool_worker_parent

        def run_with_parent():
            with bind_tool_worker_parent(parent):
                return run_sync()

        worker.executor_task = asyncio.ensure_future(runner._run_in_executor_with_context(run_with_parent))
    if authority is not None:
        # Normal settlement releases the retained agent promptly. A cancelled asyncio waiter is
        # not physical completion; leave that entry until worker_done proves the thread exited.
        worker.executor_task.add_done_callback(lambda _: list(physical_workers(authority)))


def track_turn_worker(runner, worker, agent_holder):
    from gateway.session_authorities import active_authority
    authority = active_authority(runner)
    if authority is not None:
        workers = getattr(authority, '_turn_workers', None)
        if workers is None:
            workers = authority._turn_workers = {}
        list(physical_workers(authority))
        worker._tool_worker_parent = _TurnToolWorkerParent(authority, worker, agent_holder)
        workers[id(worker)] = (worker, agent_holder)
    return authority


def physical_workers(authority):
    workers = getattr(authority, '_turn_workers', {})
    for key, (worker, holder) in list(workers.items()):
        parent = getattr(worker, '_tool_worker_parent', None)
        if worker.worker_done.is_set() and not (
            parent is not None and parent.has_pending_tool_workers()
        ):
            del workers[key]
        else:
            yield worker, holder


def stop_authority_work(authority):
    from gateway.run_runtime import stop_authority_turns
    from agent.interrupt_compat import request_hard_interrupt
    stop_authority_turns(authority, in_process=True)
    for _, holder in [*physical_workers(authority), *side_agents(authority)]:
        if holder and holder[0] is not None:
            request_hard_interrupt(holder[0], 'Profile runtime is stopping')
    for cancel in getattr(authority, '_cron_cancellations', {}).values():
        cancel.set()


async def join_authority_work(authority, timeout):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        stop_authority_work(authority)
        if (not list(physical_workers(authority)) and not list(executing_sessions(authority))
                and not mutation_tasks(authority) and not side_agents(authority)):
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError('Profile workers did not stop; ownership retained')
        await asyncio.sleep(.05)


async def await_authority_work(authority, timeout):
    """Wait for naturally accepted authority work without sending Stop or killing a turn.

    Preservation close uses this path.  A missed deadline raises and leaves the exact owner
    custody live; it is never converted into a successful zero result.
    """
    deadline = asyncio.get_running_loop().time() + max(0.0, float(timeout))
    while True:
        if not list(physical_workers(authority)) and not list(executing_sessions(authority)) and not mutation_tasks(authority):
            return
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError('Profile workers remain in preservation custody')
        await asyncio.sleep(.05)
