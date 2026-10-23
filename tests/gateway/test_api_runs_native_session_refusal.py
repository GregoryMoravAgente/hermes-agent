"""/v1/runs against a natively owned session is a bounded 409, never a 500 with a phantom reservation."""
import json
import os
import subprocess
import sys
from pathlib import Path


def test_runs_refuse_native_session_without_phantom_run(tmp_path):
    result = subprocess.run([sys.executable, __file__], cwd=Path(__file__).resolve().parents[2],
        env=dict(os.environ, HERMES_HOME=str(tmp_path)), capture_output=True, text=True, timeout=90, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    print((tmp_path / 'runs-receipt.json').read_text(encoding='utf-8'))


async def probe():
    import aiohttp
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.api_server import APIServerAdapter
    from gateway.run import GatewayRunner
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from gateway.session_contract import Principal
    from gateway.session_local import create_local_session

    home = Path(os.environ['HERMES_HOME']).resolve()
    home_token = set_hermes_home_override(home)
    process_ownership.reserve([home])
    runner = GatewayRunner(GatewayConfig(sessions_dir=home / 'sessions'))
    adapter = APIServerAdapter(PlatformConfig(enabled=True, extra={'key': 'runs-probe-owned-secret', 'port': 0}))
    adapter.gateway_runner = runner
    runner.adapters = {Platform.API_SERVER: adapter}
    await initialize_gateway_runtime(runner)
    runner._wire_adapter_handlers(adapter)
    authority = runner.session_authority
    assert await adapter.connect()
    port = adapter._site._server.sockets[0].getsockname()[1]
    actor = Principal('uid:test', authority.profile_id, frozenset({'session:create', 'session:read', 'session:submit', 'session:control'}), 'socket')
    native = create_local_session(authority, actor, {'request_id': 'native', 'source': 'cli', 'toolsets': []}).session_id
    headers = {'Authorization': 'Bearer runs-probe-owned-secret', 'Idempotency-Key': 'native-run'}
    body = {'session_id': native, 'input': 'RUN_ON_NATIVE'}
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as client:
            async with client.post(f'http://127.0.0.1:{port}/v1/runs', json=body, headers=headers) as response:
                first_status, first = response.status, json.loads(await response.text() or 'null') if response.content_type == 'application/json' else await response.text()
            async with client.post(f'http://127.0.0.1:{port}/v1/runs', json=body, headers=headers) as response:
                retry_status, retry = response.status, json.loads(await response.text() or 'null') if response.content_type == 'application/json' else await response.text()
        assert first_status == 409 and first['error']['code'] == 'permission_denied', (first_status, first)
        # The refused request must not leave a reservation that replays a run nobody executes.
        assert retry_status == 409 and retry['error']['code'] == 'permission_denied', (retry_status, retry)
        assert not adapter._run_statuses and not adapter._run_streams, adapter._run_statuses
        Path(os.environ['HERMES_HOME'], 'runs-receipt.json').write_text(encoding='utf-8', data=json.dumps(
            {'first': [first_status, first], 'retry': [retry_status, retry], 'native_session': native}))
    finally:
        await adapter.disconnect()
        adapter._response_store.close()
        adapter._run_idempotency_store.close()
        runner.session_store.close_all_db_handles()
        runner.close_all_session_db_handles()
        process_ownership.close()
        reset_hermes_home_override(home_token)


if __name__ == '__main__':
    import asyncio
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    # The refusal happens before any model call; a closed loopback port keeps this offline.
    os.environ.update(OPENAI_API_KEY='loopback-only', OPENAI_BASE_URL='http://127.0.0.1:9/v1')
    Path(os.environ['HERMES_HOME'], 'config.yaml').write_text(
        'model:\n  default: local-wire-stub\n  provider: custom\n  base_url: http://127.0.0.1:9/v1\n'
        'auxiliary:\n  title_generation:\n    enabled: false\n')
    asyncio.run(probe())
