"""Managed-worker stderr (its tracebacks and redirected prints) lands in the owning profile's
private, redacted, size-capped ``logs/managed-worker.log`` instead of being discarded."""
import asyncio
import io
from pathlib import Path
import stat
import subprocess
import sys

import pytest

from gateway.session_contract import SessionRef
from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

SECRET = 'sk-proj-' + 'Q' * 40
CRASH = f"""import sys
sys.stderr.write('Traceback (most recent call last):\\n')
sys.stderr.write('RuntimeError: provider refused api_key={SECRET} Authorization: Bearer {SECRET}\\n')
sys.stderr.flush()
raise SystemExit(1)
"""


@pytest.mark.platforms("linux")
def test_worker_stderr_reaches_a_private_redacted_profile_log(tmp_path, monkeypatch):
    from gateway import session_managed_worker as managed
    from hermes_state_runtime import get_session_admission
    original = subprocess.Popen

    def crashing_child(args, **kwargs):
        return original([sys.executable, '-c', CRASH], **kwargs)
    monkeypatch.setattr(managed.subprocess, 'Popen', crashing_child)
    monkeypatch.setattr(managed, '_worker_env', lambda authority: None)

    async def scenario():
        fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
        try:
            binding = await fixture.claim('stderr-crash')
            authority = fixture.authority
            row = get_session_admission(authority.db, admission_id=binding.admission_id)
            assert row is not None
            with pytest.raises(EOFError):
                await asyncio.wait_for(
                    managed.execute_managed(
                        authority,
                        SessionRef(binding.profile_id, binding.session_id),
                        row,
                        None,
                        binding,
                    ),
                    20,
                )
            log = Path(authority.profile_id) / 'logs' / 'managed-worker.log'
            text = log.read_text()
            assert 'Traceback (most recent call last)' in text and 'RuntimeError: provider refused' in text
            assert SECRET not in text and 'QQQQQQQQQQ' not in text
            assert stat.S_IMODE(log.stat().st_mode) == 0o600
        finally:
            fixture.cleanup()

    asyncio.run(scenario())


def test_worker_log_is_bounded_and_scrubs_the_workers_exact_secrets(tmp_path):
    from gateway.session_managed_worker_log import LOG_NAME, MAX_LOG_BYTES, drain_worker_stderr
    path = tmp_path / 'logs' / LOG_NAME
    assignment = 'opaque-assignment-secret-value'
    noise = (f'line {assignment} ' + 'x' * 200 + '\n') * 12_000 + 'y' * 300_000 + '\nlast\n'
    drain_worker_stderr(io.BytesIO(noise.encode()), path, 7, lambda: [assignment])
    files = [path.with_name(LOG_NAME + '.1'), path]  # oldest first
    assert all(f.stat().st_size <= MAX_LOG_BYTES for f in files)
    assert not list(path.parent.glob(LOG_NAME + '.2'))
    text = ''.join(f.read_text() for f in files)
    assert assignment not in text and '[truncated]' in text and text.endswith('[pid 7] last\n')
