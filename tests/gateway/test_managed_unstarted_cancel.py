"""Cancellation before reservation closes any acquired child and settles the unexecuted claim."""
import asyncio
import os
import subprocess
import sys
import threading

import pytest

from hermes_state_runtime import get_session_admission
from tests.gateway.managed_cancellation_helpers import real_managed_queue
from tests.waiting import async_wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize('phase', ['environment', 'spawn', 'hello'])
async def test_cancel_before_worker_reservation_settles_interrupted(tmp_path, monkeypatch, phase):
    from gateway import session_managed_worker as managed
    from gateway.runtime_service_custody import admission_binding
    fixture, authority, receipt = await real_managed_queue(tmp_path, monkeypatch, 'cancelled-startup')
    db = authority.db
    ref = receipt.ref
    entered, release = threading.Event(), threading.Event()
    children = []
    original_spawn = subprocess.Popen
    def environment(owner):
        if phase == 'environment':
            entered.set()
            assert release.wait(10)
        return dict(os.environ)
    def spawn(args, **kwargs):
        if phase == 'spawn':
            entered.set()
            assert release.wait(10)
        child = original_spawn([sys.executable, '-c', 'import sys; sys.stdin.buffer.read()'], **kwargs)
        children.append(child)
        return child
    original_next = managed.ManagedWorker.next_frame
    async def next_frame(worker, *args, **kwargs):
        if phase == 'hello':
            entered.set()
        return await original_next(worker, *args, **kwargs)
    monkeypatch.setattr(managed, '_worker_env', environment)
    monkeypatch.setattr(managed.subprocess, 'Popen', spawn)
    monkeypatch.setattr(managed.ManagedWorker, 'next_frame', next_frame)
    async def execute(owner, ref, row):
        binding = admission_binding(owner, ref, row)
        return await managed.execute_managed(owner, ref, row, policy=None, binding=binding)
    monkeypatch.setattr('gateway.session_finite.execute_finite_admission', execute)
    try:
        with db:
            task = asyncio.create_task(authority._drain(ref))
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            release.set()
            await asyncio.wait_for(task, 10)
            row = get_session_admission(db, admission_id=receipt.admission_id)
            assert row['status'] == 'terminal' and row['outcome'] == 'interrupted'
            assert authority.sessions[ref.session_id].event_stream.execution == {}
            if phase != 'environment':
                await async_wait_until(lambda: children and all(child.poll() is not None for child in children),
                                       timeout=10, what='the managed worker child exited')
            assert db._read_one('SELECT COUNT(*) FROM worker_executions')[0] == 0
    finally:
        release.set()
        if 'task' in locals():
            await asyncio.gather(task, return_exceptions=True)
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait(timeout=5)
        # The fixture binds HERMES_HOME with a ContextVar.  Close it in this
        # coroutine, before the test task's context is torn down.
        fixture.cleanup()
