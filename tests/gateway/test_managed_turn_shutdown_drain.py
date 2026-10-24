"""R10: managed-worker turns are part of the bounded shutdown drain.

They run in child processes, outside ``_running_agents``: the drain must count them, the timed-out
path must send them the same Stop a user's Stop sends, and the final settlement wait must stay
bounded while keeping the admission's own fenced settlement (real subprocess workers)."""
import asyncio
import subprocess
import sys
from types import SimpleNamespace

import pytest

from tests.gateway.restart_test_helpers import make_restart_runner
from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding
from tests.waiting import async_wait_until


@pytest.mark.platforms("linux")
@pytest.mark.asyncio
async def test_drain_counts_managed_turns_and_the_timeout_path_stops_them(tmp_path):
    from gateway.session_managed_worker import ManagedWorker
    seen = tmp_path / 'controls'
    child = subprocess.Popen([sys.executable, '-c', 'import sys; open(sys.argv[1], "wb").write(sys.stdin.buffer.readline())',  # noqa: ASYNC220 -- Popen returns immediately; the test drives the child through its handle
                              str(seen)], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    worker = ManagedWorker(child)
    worker.writer.start()
    runner, _adapter = make_restart_runner()
    runner.session_authority = SimpleNamespace(sessions={}, _managed_workers={'s': worker})
    try:
        assert runner._active_work_count() == 1
        _snapshot, timed_out = await runner._drain_active_agents(0)
        assert timed_out is True
        runner._interrupt_running_agents('Gateway shutting down')
        await asyncio.to_thread(child.wait, 10)
        assert seen.read_bytes() == b'{"type":"stop"}\n'
    finally:
        await asyncio.to_thread(worker.close)
    assert runner._active_work_count() == 0


@pytest.mark.platforms("linux")
def test_settlement_stops_a_silent_managed_worker_instead_of_waiting_unbounded(tmp_path, monkeypatch):
    """The reviewer's probe: a started managed admission whose child never answers. Settlement sends
    Stop, the turn settles interrupted through its own path, and nothing waits on HELLO_SECONDS."""
    from gateway import run_runtime
    from gateway import session_managed_worker as managed
    from gateway.platforms.event import MessageEvent
    from gateway.session_policy import build_policy
    original = subprocess.Popen

    def silent_child(args, **kwargs):
        return original([sys.executable, '-c', 'import time\nwhile True: time.sleep(1)'], **kwargs)
    monkeypatch.setattr(managed.subprocess, 'Popen', silent_child)

    async def scenario():
        fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
        try:
            authority = fixture.authority
            runner = fixture.runner
            source = fixture.adapter.build_source(
                chat_id='15551234567@s.whatsapp.net',
                user_id='15551234567@s.whatsapp.net',
                chat_type='dm',
            )
            receipt = await fixture.admit_native_with_source_handoff(
                MessageEvent(text='silent-worker', source=source, message_id='silent-worker'),
            )
            assert receipt.status == 'queued'
            ref = receipt.ref
            admission_id = receipt.admission_id
            live = authority.sessions[ref.session_id]
            # Keep the real production admission wrapper while selecting the managed
            # route with a typed policy fixture; the WhatsApp source used to create
            # this binding is not itself a managed-worker launch surface.
            managed_policy = build_policy(
                {'cwd': str(tmp_path), 'model': 'm'},
                {'gateway': {'managed_workers': True}},
                profile_terminal=False,
            )
            monkeypatch.setattr(managed, 'managed_policy', lambda *_args: managed_policy)
            executed = {}
            original_execute_claimed = authority._execute_claimed

            async def capture_execute_claimed(execute_ref, execute_live, execute_row):
                result = await original_execute_claimed(execute_ref, execute_live, execute_row)
                executed['result'] = result
                executed['pending'] = authority.pending_results.get(execute_row['admission_id'])
                return result

            monkeypatch.setattr(authority, '_execute_claimed', capture_execute_claimed)
            # Exercise the production claim wrapper: it owns the execution stamp
            # finalizer while the shutdown path supervises the real managed child.
            # Calling execute_managed directly would require a test-only mirror of
            # the wrapper's lifecycle cleanup and would not prove the drain contract.
            live.task = asyncio.create_task(authority._drain(ref))
            await async_wait_until(lambda: ref.session_id in getattr(authority, '_managed_workers', {}),
                               what='the managed worker to register')
            async with asyncio.timeout(15):
                await run_runtime.settle_gateway_runtime(runner)
            task = live.task
            assert task.done() and not task.cancelled()
            assert executed['result'][0] == ''
            assert executed['pending']['result']['interrupted'] is True
            settled = authority.db._read_one(
                'SELECT status, outcome FROM session_admissions WHERE admission_id=?',
                (admission_id,),
            )
            assert settled is not None and tuple(settled) == ('terminal', 'interrupted')
            assert admission_id not in authority.pending_results
            assert authority._managed_workers == {}
        finally:
            fixture.cleanup()

    asyncio.run(scenario())
