"""Stop is never acknowledged into a queue nobody drains: a managed child that stays alive
without ever sending hello is terminated on Stop and the admission settles as interrupted."""
import asyncio
import subprocess
import sys
from types import SimpleNamespace

import psutil
import pytest

from gateway.session_contract import Principal
from tests.gateway.managed_cancellation_helpers import real_managed_claim, real_managed_queue
from tests.waiting import async_wait_until


@pytest.mark.platforms("linux")
def test_stop_before_hello_terminates_child_and_settles_interrupted(tmp_path, monkeypatch):
    from gateway import session_managed_worker as managed
    spawned = []
    original = subprocess.Popen

    def wedged_child(args, **kwargs):
        # An interpreter that starts but never introduces itself (loader/stdio stall).
        child = original([sys.executable, '-c', 'import time\nwhile True: time.sleep(1)'], **kwargs)
        spawned.append(child)
        return child
    monkeypatch.setattr(managed.subprocess, 'Popen', wedged_child)
    checks = []

    async def scenario():
        fixture, binding, authority, ref, row, _live = await real_managed_claim(
            tmp_path, monkeypatch, 'stop-before-hello'
        )
        actor = Principal('owner', authority.profile_id, frozenset({'session:control'}), 'transport')
        authority.authorize = lambda actor, ref, cap: checks.append(cap)
        authority.check_approval_generation = lambda session_id, generation: checks.append(generation)
        try:
            turn = asyncio.create_task(managed.execute_managed(authority, ref, row, policy=None, binding=binding))
            await async_wait_until(lambda: ref.session_id in getattr(authority, '_managed_workers', {}),
                                   timeout=10, what='the managed worker to be registered')
            assert managed.interrupt_managed(authority, actor, ref, row['generation']) is True
            async with asyncio.timeout(10):
                return await turn, fixture, authority, binding, ref, row
        finally:
            fixture.cleanup()
    # The fixture itself owns the profile home and native process lease; it is closed in scenario.
    result, _fixture, authority, binding, ref, row = asyncio.run(scenario())
    assert result == ''
    assert checks == ['session:control', row['generation']]
    assert authority.pending_results[binding.admission_id]['result']['interrupted'] is True
    assert ref.session_id not in authority._managed_workers
    child = spawned[0]
    assert child.poll() is not None and not psutil.pid_exists(child.pid) or psutil.Process(child.pid).status() == psutil.STATUS_ZOMBIE


CHILD = """import json, os, sys, psutil
me = psutil.Process()
out = os.fdopen(os.dup(sys.stdout.fileno()), 'wb', buffering=0)
def send(frame):
    out.write((json.dumps(frame) + '\\n').encode())
send({'type': 'hello', 'pid': me.pid, 'birth': me.create_time(), 'ancestors': [p.pid for p in me.parents()[:3]]})
for line in sys.stdin.buffer:
    frame = json.loads(line)
    with open(sys.argv[1], 'a') as log:
        log.write(json.dumps(frame.get('type', 'bootstrap')) + '\\n')
    if 'version' in frame:
        send({'type': 'result', 'result': {'final_response': 'LATE_TURN'}})
    elif frame == {'type': 'finish'}:
        send({'type': 'finished'})
        break
"""


@pytest.mark.platforms("linux")
def test_stop_acknowledged_during_spawn_never_bootstraps_the_worker(tmp_path, monkeypatch):
    """R2: a Stop the real interrupt RPC acknowledges while ``_worker_env``/``Popen`` is awaited
    (no worker registered yet, so the authority latches it for the generation) must keep the child
    from ever receiving its bootstrap; the admission settles interrupted, never as a late turn."""
    import threading
    from gateway import session_managed_worker as managed
    from gateway.session_controls import AuthorityConnection
    from gateway.session_policy import build_policy
    from gateway.runtime_service_custody import admission_binding
    from hermes_state_runtime import get_session_admission
    received = tmp_path / 'received.jsonl'

    class Child(subprocess.Popen):
        def __init__(self, args, **kw):
            super().__init__([sys.executable, '-c', CHILD, str(received)], **kw)
    monkeypatch.setattr(managed.subprocess, 'Popen', Child)
    entered, release = threading.Event(), threading.Event()

    def barrier(authority):
        entered.set()
        assert release.wait(10)
    monkeypatch.setattr(managed, '_worker_env', barrier)
    policy = build_policy({'cwd': str(tmp_path), 'model': 'm'}, {}, profile_terminal=False)
    captured = {}

    async def scenario():
        fixture, authority, receipt = await real_managed_queue(tmp_path, monkeypatch, 'stop-before-spawn')
        try:
            ref = receipt.ref

            async def execute(owner, claimed_ref, row):
                binding = admission_binding(owner, claimed_ref, row)
                response = await managed.execute_managed(owner, claimed_ref, row, policy, binding)
                captured['response'] = response
                captured['result'] = owner.pending_results[row['admission_id']]['result'].copy()
                return response

            monkeypatch.setattr('gateway.session_finite.execute_finite_admission', execute)
            turn = asyncio.create_task(authority._drain(ref))
            assert await asyncio.to_thread(entered.wait, 10)
            row = get_session_admission(authority.db, admission_id=receipt.admission_id)
            assert row is not None and row['status'] == 'started'
            # The control principal is the one that actually admitted this row;
            # a synthetic human principal is not a valid I05 owner.
            actor = Principal(row['principal_id'], authority.profile_id,
                              frozenset({'session:submit', 'session:control'}), 'cli')
            stopped = await AuthorityConnection.interrupt(
                SimpleNamespace(authority=authority, actor=actor), ref,
                {'execution_generation': row['generation']},
            )
            assert stopped['execution_state'] == 'running'
            release.set()
            async with asyncio.timeout(20):
                await turn
            return row, authority
        finally:
            release.set()
            if 'turn' in locals():
                await asyncio.gather(turn, return_exceptions=True)
            fixture.cleanup()

    row, authority = asyncio.run(scenario())
    assert captured['response'] == ''
    assert captured['result']['interrupted'] is True
    assert not received.exists() or 'bootstrap' not in received.read_text(), received.read_text()
    assert row['target_session_id'] not in authority.pending_stops
