"""A peer turn into the canonical Bot Chat under session authority is that chat's own admission.

``hermes peer dm`` / ``peer run`` reach the API edge; under the unified runtime the Bot Chat is
a LOCAL session whose only writer is the authority FIFO, so the turn goes through the
``bot_relay.deliver`` door (``gateway/session_bot.py``) and the reply is its admission's
receipt. Binding the chat as an API conversation would be a second writer (``permission_denied``).
"""
import asyncio
import functools
import hashlib
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
from gateway.session_contract import Principal

AUTHOR = {"id": "bot:cto", "name": "cto", "is_bot": True}


@pytest_asyncio.fixture
async def peer(tmp_path, monkeypatch):
    from gateway import run
    from gateway.config import GatewayConfig
    from gateway.session import SessionStore
    from gateway.session_authority import initialize_session_authority
    from gateway.session_local import create_local_session
    from gateway.session_local_title import title_new_session
    from tests.gateway.conftest import bind_authority_runtime_custody

    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setattr(run, '_load_gateway_config', lambda: {'platform_toolsets': {'cli': []}})
    monkeypatch.setattr('tools.bot_mode_dm._LIVE_WAIT_SECONDS', 2.0)
    from hermes_state import SessionDB
    store = SessionStore(tmp_path / 'sessions', GatewayConfig())
    store._db = SessionDB(db_path=tmp_path / 'state.db')
    runner = SimpleNamespace(session_store=store, _session_db=store._db, adapters={}, _draining=False,
                             _evict_cached_agent=lambda route: None)
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='fixture')
    bind_authority_runtime_custody(runner, authority)
    owner = Principal('uid:1000', 'default', frozenset({'session:create', 'session:read', 'session:submit'}), 'native')
    chat = create_local_session(authority, owner, {'request_id': 'bot', 'source': 'gui', 'cwd': str(tmp_path),
                                                    'model': 'fixture', 'toolsets': []})
    title_new_session(authority, chat, 'Bot Chat')
    authority.db.create_session('scratch', 'api_server')
    adapter = APIServerAdapter(PlatformConfig(enabled=True))
    adapter.gateway_runner = runner
    adapter._session_db = authority.db
    from gateway.config import Platform
    runner.adapters[Platform.API_SERVER] = adapter
    runner._adapter_for_source = lambda source: runner.adapters.get(source.platform)
    executed = []

    # Failure-only observer: the production helpers and bind call remain the original
    # call-through.  We collect only bounded scalar branch evidence, then read the
    # canonical admission row after the bind call actually returns a typed refusal.
    from gateway.platforms import api_server_run_custody as custody_module
    from gateway.platforms import api_server_run_peer as peer_module
    from gateway.platforms.api_server_run_custody import ApiRunCustody
    from hermes_state_runtime import get_session_admission

    source_branches = []

    def _digest(value):
        if value is None:
            return None
        if isinstance(value, (str, int, bool)):
            raw = str(value)
        else:
            raw = type(value).__name__
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:12]

    def _generation(value):
        return value if type(value) is int else None

    def _text(value):
        return value if isinstance(value, str) else type(value).__name__ if value is not None else None

    def _record_scalars(record):
        if not isinstance(record, dict):
            return {'type': type(record).__name__}
        return {
            'status': _text(record.get('status')),
            'admission_id': _digest(record.get('admission_id')),
            'retry_admission_id': _digest(record.get('retry_admission_id')),
            'execution_generation': _generation(record.get('execution_generation')),
            'retry_execution_generation': _generation(record.get('retry_execution_generation')),
            'session_id': _digest(record.get('session_id')),
            'producer_phase': _text(record.get('_api_producer_phase')),
            'producer_source': _text(record.get('_api_producer_source')),
        }

    def _custody_scalars(custody):
        return {
            'admission_id': _digest(getattr(custody, 'admission_id', None)),
            'execution_generation': _generation(getattr(custody, 'execution_generation', None)),
            'authority_epoch': _generation(getattr(custody, 'authority_epoch', None)),
            'profile_id': _digest(getattr(custody, 'profile_id', None)),
            'parent_bound': getattr(custody, 'parent_bound', None) is True,
            'parent_kind': _text(getattr(custody, 'parent_kind', None)),
        }

    def _row_scalars(row):
        if not isinstance(row, dict):
            return {'read': 'missing'}
        return {
            'read': 'ok',
            'admission_id': _digest(row.get('admission_id')),
            'status': _text(row.get('status')),
            'target_session_id': _digest(row.get('target_session_id')),
            'generation': _generation(row.get('generation')),
            'owner_epoch': _generation(row.get('owner_epoch')),
            'principal_present': isinstance(row.get('principal_id'), str) and bool(row.get('principal_id')),
        }

    def _refusal(result, tuple_result):
        if tuple_result:
            if not isinstance(result, tuple) or len(result) != 2:
                return None
            return result[1] if getattr(result[1], 'code', None) is not None else None
        return result if getattr(result, 'code', None) is not None else None

    def _wrap_peer_helper(branch, original, *, tuple_result):
        @functools.wraps(original)
        def observed(*args, **kwargs):
            result = original(*args, **kwargs)
            refusal = _refusal(result, tuple_result)
            if refusal is not None:
                source_branches.append({'branch': branch, 'code': refusal.code})
            return result
        return observed

    for name, tuple_result in (
        ('peer_binding_identity', True),
        ('peer_binding_source', True),
        ('peer_binding_replay', False),
    ):
        original = getattr(peer_module, name)
        observed = _wrap_peer_helper(name, original, tuple_result=tuple_result)
        monkeypatch.setattr(peer_module, name, observed)
        monkeypatch.setattr(custody_module, '_' + name, observed)

    original_bind_peer_record = ApiRunCustody.bind_peer_record

    @functools.wraps(original_bind_peer_record)
    def observed_bind_peer_record(custody, record, authority, **kwargs):
        before = {
            'record': _record_scalars(record),
            'custody': _custody_scalars(custody),
        }
        branch_start = len(source_branches)
        result = original_bind_peer_record(custody, record, authority, **kwargs)
        refusal = result if getattr(result, 'code', None) is not None else None
        if refusal is not None:
            try:
                binding_id = (
                    record.get('retry_admission_id') or record.get('admission_id')
                    if isinstance(record, dict) else None
                )
                db = getattr(authority, 'db', None)
                if db is None or not isinstance(binding_id, str) or not binding_id:
                    canonical = {'read': 'unavailable'}
                else:
                    try:
                        canonical = _row_scalars(get_session_admission(db, admission_id=binding_id))
                    except Exception as exc:
                        canonical = {'read': 'error', 'error_type': type(exc).__name__}
                branches = source_branches[branch_start:]
                if not branches:
                    branches = [{'branch': 'bind_peer_record', 'code': refusal.code}]
                print('peer-bind-refusal-observation: ' + json.dumps({
                    'source_branches': branches,
                    'refusal': refusal.code,
                    'before': before,
                    'after_custody': _custody_scalars(custody),
                    'canonical_row_after': canonical,
                }, sort_keys=True), flush=True)
            except BaseException:
                # A diagnostic read/print failure must not replace the original refusal.
                pass
        return result

    monkeypatch.setattr(ApiRunCustody, 'bind_peer_record', observed_bind_peer_record)

    async def execute(authority, ref, row):
        text = row['payload']['text']
        executed.append((ref.session_id, row['principal_id'], text, row['payload'].get('local_automation_v1', {}).get('turn_author')))
        if text == 'fail':
            raise RuntimeError('provider said 429')
        return 'pong'

    monkeypatch.setattr('gateway.session_finite.execute_finite_admission', execute)
    app = web.Application()
    app.router.add_post("/api/sessions/{session_id}/chat", adapter._handle_session_chat)
    app.router.add_post("/api/sessions/{session_id}/chat/stream", adapter._handle_session_chat_stream)
    app.router.add_post("/v1/runs", adapter._handle_runs)
    app.router.add_get("/v1/runs/{run_id}", adapter._handle_get_run)
    try:
        async with TestClient(TestServer(app)) as cli:
            yield SimpleNamespace(cli=cli, authority=authority, chat=chat.session_id, executed=executed,
                                  adapter=adapter, home=tmp_path)
    finally:
        adapter._response_store.close()
        adapter._run_idempotency_store.close()
        store._db.close()


@pytest.mark.asyncio
async def test_peer_dm_into_the_bot_chat_is_the_chats_own_admission(peer):
    resp = await peer.cli.post(f"/api/sessions/{peer.chat}/chat", json={"message": "ping", "author": AUTHOR})
    body = await resp.json()
    assert resp.status == 200, body
    assert body['message']['content'] == 'pong' and body['delivery_id']
    assert peer.executed == [(peer.chat, 'automation:' + peer.authority.sessions[peer.chat].route, 'ping', AUTHOR)]
    # The receipt is durable and exact: the same delivery reads back settled, no re-execution.
    record = json.loads((peer.home / 'runtime' / 'bot_live_delivery' / f"{body['delivery_id']}.json").read_text())
    assert record['status'] == 'settled' and record['session_id'] == peer.chat
    rows = peer.authority.db._read_all("SELECT principal_id FROM session_admissions WHERE principal_id='api'")
    assert rows == [], 'the Bot Chat was never bound as an API conversation'


@pytest.mark.asyncio
async def test_streamed_peer_dm_and_peer_run_ride_the_same_door(peer):
    resp = await peer.cli.post(f"/api/sessions/{peer.chat}/chat/stream", json={"message": "ping", "author": AUTHOR})
    assert resp.status == 200, await resp.text()
    assert resp.content_type == "text/event-stream"
    names = [line[7:] for line in (await resp.text()).splitlines() if line.startswith('event: ')]
    assert names[0] == 'run.started' and 'assistant.completed' in names and names[-1] == 'done'

    resp = await peer.cli.post("/v1/runs", json={"input": "fail", "session_id": peer.chat, "author": AUTHOR})
    response_body = await resp.json()
    print("peer-run-response-observation: " + json.dumps({
        "status": resp.status,
        "keys": sorted(response_body),
        "code": response_body.get("error", {}).get("code") if isinstance(response_body.get("error"), dict) else response_body.get("code"),
        "reason": response_body.get("reason"),
    }, sort_keys=True))
    run_id = response_body['run_id']
    for _ in range(100):
        status = await (await peer.cli.get(f"/v1/runs/{run_id}")).json()
        if status['status'] in ('completed', 'failed', 'cancelled'):
            break
        await asyncio.sleep(0.05)
    assert status['status'] == 'failed' and status['delivery_id'], status
    assert [text for _, _, text, _ in peer.executed] == ['ping', 'fail']
    # A turn into any other session still runs as an ordinary API admission.
    resp = await peer.cli.post("/api/sessions/scratch/chat", json={"message": "elsewhere"})
    assert resp.status == 200 and [text for _, _, text, _ in peer.executed][-1] == 'elsewhere'
    assert peer.authority.db._read_all("SELECT 1 FROM session_admissions WHERE principal_id='api'")
