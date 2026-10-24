"""The managed worker's reservation and loss writes run off the owner loop (B1-13): a state.db
writer held by another process (up to _WRITE_PATIENCE_S) must not freeze every other session,
timer, socket and Stop on the loop. Real child, real SQLite, a real competing writer."""
import asyncio
from itertools import pairwise
import sqlite3
import subprocess
import sys
import threading
import time

import pytest

from gateway.session_contract import SessionRef
from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding
from tests.waiting import wait_until

CHILD = """import json, os, sys, time, psutil
me = psutil.Process()
out = os.fdopen(os.dup(sys.stdout.fileno()), 'wb', buffering=0)
def send(frame):
    out.write((json.dumps(frame) + '\\n').encode())
send({'type': 'hello', 'pid': me.pid, 'birth': me.create_time(), 'ancestors': [p.pid for p in me.parents()[:3]]})
for line in sys.stdin.buffer:
    frame = json.loads(line)
    if 'version' in frame:
        open(sys.argv[2], 'w').close()
        time.sleep(.5)
        send({'type': 'bogus'})  # an invalid frame: the owner revokes the assignment
    elif frame == {'type': 'finish'}:
        break
"""
HOLD_S = 3.0


@pytest.mark.platforms("linux")
@pytest.mark.parametrize('held_during', ['reserve', 'lose'])
def test_reservation_and_loss_writes_never_block_the_loop(tmp_path, monkeypatch, held_during):
    from gateway import session_managed_worker as managed
    from gateway.session_policy import build_policy
    from hermes_state_runtime import get_session_admission

    held = threading.Event()

    async def scenario():
        fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
        try:
            binding = await fixture.claim(f'reservation-{held_during}')
            authority = fixture.authority
            db = authority.db
            ref = SessionRef(binding.profile_id, binding.session_id)
            row = get_session_admission(db, admission_id=binding.admission_id)
            assert row is not None
            bootstrapped = tmp_path / f'bootstrapped-{held_during}'

            class Child(subprocess.Popen):
                def __init__(self, args, **kw):
                    super().__init__([sys.executable, '-c', CHILD, held_during, str(bootstrapped)], **kw)

            monkeypatch.setattr(managed.subprocess, 'Popen', Child)
            policy = build_policy({'cwd': str(tmp_path), 'model': 'm'}, {}, profile_terminal=False)

            def competing_writer():
                if held_during == 'lose':
                    wait_until(bootstrapped.exists, timeout=30, what='the worker to receive its bootstrap')
                rival = sqlite3.connect(db.db_path, isolation_level=None)
                rival.execute('BEGIN IMMEDIATE')
                held.set()
                time.sleep(HOLD_S)  # health: allow HX013 -- the held writer's duration IS the behaviour under test
                rival.execute('ROLLBACK')
                rival.close()

            writer = threading.Thread(target=competing_writer, daemon=True)
            writer.start()
            if held_during == 'reserve':
                assert await asyncio.to_thread(held.wait, 10)
            ticks = []

            async def ticker():
                while True:
                    ticks.append(time.monotonic())
                    await asyncio.sleep(.05)  # health: allow HX013 -- the loop heartbeat probe; its gaps are the measurement
            beat = asyncio.create_task(ticker())
            try:
                async with asyncio.timeout(60):
                    await managed.execute_managed(authority, ref, row, policy, binding)
            except managed.ManagedExecutionUnknown:
                pass
            finally:
                beat.cancel()
            await asyncio.to_thread(writer.join, 10)
            status = db._read_one(
                'SELECT status FROM session_admissions WHERE admission_id=?',
                (row['admission_id'],),
            )[0]
            return status, max(b - a for a, b in pairwise(ticks))
        finally:
            fixture.cleanup()

    status, gap = asyncio.run(scenario())
    assert held.is_set()
    assert status == 'unknown'  # the bogus frame still revokes the assignment once the writer frees
    assert gap < HOLD_S / 2, f'the owner loop stalled {gap:.2f}s behind a held state.db writer'
