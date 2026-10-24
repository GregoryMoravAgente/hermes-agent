"""A lost managed worker's message.complete carries the contract's error TurnStatus next to
outcome 'unknown' (N117), so a client that renders by status never shows a normal finish."""
import asyncio
import subprocess
import sys
from types import SimpleNamespace

import pytest

CHILD = """import json, os, sys, psutil
me = psutil.Process()
out = os.fdopen(os.dup(sys.stdout.fileno()), 'wb', buffering=0)
out.write((json.dumps({'type': 'hello', 'pid': me.pid, 'birth': me.create_time(),
                       'ancestors': [p.pid for p in me.parents()[:3]]}) + '\\n').encode())
for line in sys.stdin.buffer:
    out.write(b'{"type": "bogus"}\\n')
    sys.stdin.buffer.read()
    break
"""


@pytest.mark.platforms("linux")
def test_unknown_worker_loss_publishes_error_status(tmp_path, monkeypatch):
    from gateway import session_managed_worker as managed
    from gateway.session_policy import build_policy
    published = []
    from hermes_state_runtime import get_session_admission
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    async def scenario():
        fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
        try:
            binding = await fixture.claim('unknown-status')
            authority = fixture.authority
            ref = SimpleNamespace(session_id=binding.session_id, profile_id=binding.profile_id)
            row = get_session_admission(authority.db, admission_id=binding.admission_id)
            assert row is not None
            live = authority.sessions[binding.session_id]
            original = live.event_stream.publish
            monkeypatch.setattr(live.event_stream, 'publish', lambda sid, payload, **kw: (
                published.append((kw.get('event_type', 'message.complete'), payload)),
                original(sid, payload, **kw))[1])

            class Child(subprocess.Popen):
                def __init__(self, args, **kw):
                    super().__init__([sys.executable, '-c', CHILD], **kw)
            monkeypatch.setattr(managed.subprocess, 'Popen', Child)
            policy = build_policy({'cwd': str(tmp_path), 'model': 'm'}, {}, profile_terminal=False)
            with pytest.raises(managed.ManagedExecutionUnknown):
                async with asyncio.timeout(30):
                    await managed.execute_managed(authority, ref, row, policy, binding)
        finally:
            fixture.cleanup()

    asyncio.run(scenario())
    complete = [p for kind, p in published if kind == 'message.complete' and p.get('outcome') == 'unknown']
    assert complete and complete[0]['status'] == 'error', published
