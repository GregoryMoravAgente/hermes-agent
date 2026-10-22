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
    timeline = []
    peer_bindings = []
    live_peer_binding = {}
    execution_gate = {
        'hold_fail': False,
        'ready': asyncio.Event(),
        'release': asyncio.Event(),
    }

    # Source-observer only: the production helpers and bind call remain original call-throughs.
    # We collect bounded scalar chronology and read canonical rows only for typed refusals.
    from gateway.platforms import api_server_run_custody as custody_module
    from gateway.platforms import api_server_run_peer as peer_module
    from gateway.platforms.api_server_run_custody import ApiRunCustody
    from hermes_state_runtime import get_session_admission

    source_branches = []
    # Diagnostic synchronization only: the event is set by the real authority claim after the
    # canonical row receives its generation.  The commit wrapper below waits on this exact
    # source event before _admit takes its result snapshot; it never edits the row or custody.
    claim_event = asyncio.Event()
    claim_observation = {}

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
            'message': _text(record.get('message')),
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
            'text': _text((row.get('payload') or {}).get('text') if isinstance(row.get('payload'), dict) else None),
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
        peer_bindings.append((custody, dict(record) if isinstance(record, dict) else record))
        before = {
            'record': _record_scalars(record),
            'custody': _custody_scalars(custody),
        }
        timeline.append({'event': 'bind_before', **before})
        branch_start = len(source_branches)
        try:
            result = original_bind_peer_record(custody, record, authority, **kwargs)
        except BaseException as exc:
            timeline.append({
                'event': 'bind_error', 'record': _record_scalars(record),
                'error_type': type(exc).__name__,
            })
            raise
        refusal = result if getattr(result, 'code', None) is not None else None
        timeline.append({
            'event': 'bind_after', 'record': _record_scalars(record),
            'custody': _custody_scalars(custody),
            'parent_bound_before': before['custody']['parent_bound'],
            'refusal': getattr(refusal, 'code', None),
        })
        if (
            refusal is None
            and isinstance(record, dict)
            and record.get('message') == 'fail'
            and before['custody']['parent_bound'] is True
        ):
            live_peer_binding['value'] = (custody, dict(record))
            execution_gate['ready'].set()
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

    from gateway.session_authority import SessionAuthority
    original_stamp_claim = SessionAuthority._stamp_claim

    @functools.wraps(original_stamp_claim)
    def observed_stamp_claim(authority, ref, live, claim_child=None, claim_admission_id=None, row=None):
        result = original_stamp_claim(
            authority, ref, live, claim_child, claim_admission_id, row)
        timeline.append({
            'event': 'claim_after', 'row': _row_scalars(result),
        })
        return result

    monkeypatch.setattr(SessionAuthority, '_stamp_claim', observed_stamp_claim)

    original_claim_next = SessionAuthority._claim_next

    @functools.wraps(original_claim_next)
    async def observed_claim_next(authority, ref, live):
        result = await original_claim_next(authority, ref, live)
        row = result[0] if isinstance(result, tuple) and len(result) == 2 else None
        payload = row.get('payload') if isinstance(row, dict) else None
        if (isinstance(row, dict)
                and row.get('target_session_id') == chat.session_id
                and isinstance(payload, dict) and payload.get('text') == 'fail'):
            claim_observation.update({
                'admission_id': _digest(row.get('admission_id')),
                'generation': _generation(row.get('generation')),
                'target_session_id': _digest(row.get('target_session_id')),
            })
            timeline.append({'event': 'claim_gate_after', 'row': _row_scalars(row)})
            claim_event.set()
        return result

    monkeypatch.setattr(SessionAuthority, '_claim_next', observed_claim_next)

    from gateway import session_bot_custody
    original_commit_bot_admission = session_bot_custody.commit_bot_admission

    @functools.wraps(original_commit_bot_admission)
    async def observed_commit_bot_admission(*args, **kwargs):
        record = args[6] if len(args) > 6 else kwargs.get('record')
        receipt = await original_commit_bot_admission(*args, **kwargs)
        # This is deliberately after the real commit and before _admit calls _result.  The
        # authority's own drain is allowed to claim concurrently; the wait only prevents the
        # response projection from racing that canonical claim.
        if (receipt is not None and isinstance(record, dict)
                and record.get('message') == 'fail'):
            await claim_event.wait()
        return receipt

    monkeypatch.setattr(session_bot_custody, 'commit_bot_admission', observed_commit_bot_admission)

    async def execute(authority, ref, row):
        text = row['payload']['text']
        timeline.append({
            'event': 'execute_before', 'text': text,
            'provider_outcome': 'raise_429' if text == 'fail' else 'return_pong',
            'row': _row_scalars(row),
        })
        executed.append((ref.session_id, row['principal_id'], text, row['payload'].get('local_automation_v1', {}).get('turn_author')))
        if text == 'fail':
            if execution_gate['hold_fail']:
                await execution_gate['release'].wait()
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
                                  adapter=adapter, home=tmp_path, timeline=timeline,
                                  peer_bindings=peer_bindings, claim_observation=claim_observation,
                                  live_peer_binding=live_peer_binding, execution_gate=execution_gate,
                                  binding_id_digest=_digest)
    finally:
        execution_gate['release'].set()
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
    fail_binds = [
        item for item in peer.timeline
        if item['event'] == 'bind_after'
        and item['record'].get('message') == 'fail'
        and item['parent_bound_before'] is False
        and item['refusal'] is None
    ]
    fail_claims = [
        item for item in peer.timeline
        if item['event'] == 'claim_after' and item['row'].get('text') == 'fail'
    ]
    fail_executes = [
        item for item in peer.timeline
        if item['event'] == 'execute_before' and item['text'] == 'fail'
    ]
    fail_gate_claims = [
        item for item in peer.timeline
        if item['event'] == 'claim_gate_after' and item['row'].get('text') == 'fail'
    ]
    assert len(fail_binds) == 1 and fail_binds[0]['custody']['parent_bound'] is True
    assert len(fail_claims) == 1 and len(fail_executes) == 1
    assert len(fail_gate_claims) == 1
    assert peer.claim_observation == {
        'admission_id': fail_claims[0]['row']['admission_id'],
        'generation': fail_claims[0]['row']['generation'],
        'target_session_id': fail_claims[0]['row']['target_session_id'],
    }
    # The first parentless bind is the queued preclaim snapshot; promotion is observed separately.
    assert fail_binds[0]['custody']['execution_generation'] is None
    captured_binding = peer.live_peer_binding.get('value')
    live_bindings = [captured_binding[0]] if (
        captured_binding is not None
        and peer.binding_id_digest(captured_binding[1].get('admission_id'))
        == fail_claims[0]['row']['admission_id']
        and captured_binding[1].get('message') == 'fail'
    ) else []
    assert live_bindings
    assert live_bindings[0].execution_generation == fail_claims[0]['row']['generation']
    promoted_binds = [
        item for item in peer.timeline
        if item['event'] == 'bind_after'
        and item['record'].get('message') == 'fail'
        and item['parent_bound_before'] is True
        and item['refusal'] is None
    ]
    assert promoted_binds
    assert all(item['custody']['execution_generation'] == fail_claims[0]['row']['generation']
               for item in promoted_binds)
    assert fail_executes[0]['provider_outcome'] == 'raise_429'
    assert fail_binds[0]['record']['admission_id'] == fail_claims[0]['row']['admission_id']
    assert fail_binds[0]['custody']['authority_epoch'] == fail_claims[0]['row']['owner_epoch']
    assert fail_binds[0]['custody']['parent_kind'] == 'I02_api_session_turn'
    bind_index = next(index for index, item in enumerate(peer.timeline) if item is fail_binds[0])
    claim_index = next(index for index, item in enumerate(peer.timeline) if item is fail_claims[0])
    gate_index = next(index for index, item in enumerate(peer.timeline) if item is fail_gate_claims[0])
    execute_index = next(index for index, item in enumerate(peer.timeline) if item is fail_executes[0])
    assert bind_index < claim_index < execute_index
    assert claim_index < gate_index < execute_index
    terminal = {'event': 'terminal_failed_delivery', 'delivery_id': status['delivery_id'],
                'status': status['status'], 'delivery_present': bool(status['delivery_id'])}
    peer.timeline.append(terminal)
    assert terminal['status'] == 'failed' and terminal['delivery_present'] is True
    assert execute_index < len(peer.timeline) - 1
    # A turn into any other session still runs as an ordinary API admission.
    resp = await peer.cli.post("/api/sessions/scratch/chat", json={"message": "elsewhere"})
    assert resp.status == 200 and [text for _, _, text, _ in peer.executed][-1] == 'elsewhere'
    assert peer.authority.db._read_all("SELECT 1 FROM session_admissions WHERE principal_id='api'")


@pytest.mark.asyncio
async def test_peer_binding_rejects_copied_foreign_and_stale_projection(peer):
    peer.execution_gate['hold_fail'] = True
    response = await peer.cli.post(
        "/v1/runs", json={"input": "fail", "session_id": peer.chat, "author": AUTHOR})
    body = await response.json()
    assert response.status == 202, body
    assert body['status'] == 'started', body
    run_id = body['run_id']
    await peer.execution_gate['ready'].wait()
    custody, original_record = peer.live_peer_binding['value']
    bot_binding = next(
        (item for item in peer.peer_bindings
         if isinstance(item[1], dict)
         and item[1].get('_api_producer_phase') == 'bot_i02'),
        None,
    )
    assert bot_binding is not None
    identity_before = (custody.admission_id, custody.execution_generation,
                       custody.parent_bound, custody.parent_handle)

    try:
        # Negative controls copy the real source projection only to corrupt one identity field. No
        # copied projection is ever supplied as an accepted producer witness.
        canonical_generation = custody.execution_generation
        assert type(canonical_generation) is int and canonical_generation >= 0
        copied_foreign = dict(original_record)
        copied_foreign['execution_generation'] = canonical_generation
        copied_foreign.update(_api_producer_phase='api_i02', _api_producer_source='api_server')
        refusal = custody.bind_peer_record(copied_foreign, peer.authority,
                                           expected_parent=custody.parent_handle)
        assert getattr(refusal, 'code', None) == 'peer_source_mismatch'
        copied_stale = dict(original_record)
        copied_stale['execution_generation'] = canonical_generation
        copied_stale['authority_epoch'] = custody.authority_epoch - 1
        refusal = custody.bind_peer_record(copied_stale, peer.authority,
                                           expected_parent=custody.parent_handle)
        assert getattr(refusal, 'code', None) == 'peer_epoch_mismatch'
        copied_generation = dict(original_record)
        copied_generation['execution_generation'] = (custody.execution_generation or 0) + 1
        refusal = custody.bind_peer_record(copied_generation, peer.authority,
                                           expected_parent=custody.parent_handle)
        assert getattr(refusal, 'code', None) == 'peer_generation_mismatch'
        assert (custody.admission_id, custody.execution_generation,
                custody.parent_bound, custody.parent_handle) == identity_before
    finally:
        peer.execution_gate['release'].set()
    for _ in range(100):
        status = await (await peer.cli.get(f"/v1/runs/{run_id}")).json()
        if status['status'] in ('completed', 'failed', 'cancelled'):
            break
        await asyncio.sleep(0.05)
    assert status['status'] == 'failed' and status['delivery_id']



@pytest_asyncio.fixture
async def headless_peer(tmp_path, monkeypatch):
    """The canonical peer-created Bot Chat owner with a durable CLI source."""
    from gateway import run
    from gateway.config import GatewayConfig, Platform
    from gateway.session import SessionStore
    from gateway.session_authority import initialize_session_authority
    from tests.gateway.conftest import bind_authority_runtime_custody

    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setattr(run, '_load_gateway_config',
                        lambda: {'platform_toolsets': {'cli': []}, 'model': {'default': 'fixture'}})
    from hermes_state import SessionDB
    store = SessionStore(tmp_path / 'sessions', GatewayConfig())
    store._db = SessionDB(db_path=tmp_path / 'state.db')
    runner = SimpleNamespace(session_store=store, _session_db=store._db, adapters={}, _draining=False,
                             _evict_cached_agent=lambda route: None, _cached_agent_for=lambda route: None)
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='fixture')
    bind_authority_runtime_custody(runner, authority)
    adapter = APIServerAdapter(PlatformConfig(enabled=True))
    adapter.gateway_runner = runner
    adapter._session_db = authority.db
    runner.adapters[Platform.API_SERVER] = adapter
    runner._adapter_for_source = lambda source: runner.adapters.get(source.platform)
    executed = []
    peer_bindings = []

    async def execute(authority, ref, row):
        executed.append((ref.session_id, row['principal_id'], row['payload']['text']))
        return 'pong'

    monkeypatch.setattr('gateway.session_finite.execute_finite_admission', execute)
    from gateway.platforms.api_server_run_custody import ApiRunCustody
    original_bind_peer_record = ApiRunCustody.bind_peer_record

    @functools.wraps(original_bind_peer_record)
    def observe_bind_peer_record(custody, record, authority, **kwargs):
        if isinstance(record, dict):
            peer_bindings.append(dict(record))
        return original_bind_peer_record(custody, record, authority, **kwargs)

    monkeypatch.setattr(ApiRunCustody, 'bind_peer_record', observe_bind_peer_record)
    app = web.Application()
    app.router.add_post('/api/sessions', adapter._handle_create_session)
    app.router.add_post('/api/sessions/{session_id}/chat', adapter._handle_session_chat)
    app.router.add_post('/v1/runs', adapter._handle_runs)
    app.router.add_get('/v1/runs/{run_id}', adapter._handle_get_run)
    try:
        async with TestClient(TestServer(app)) as cli:
            yield SimpleNamespace(cli=cli, authority=authority, adapter=adapter, home=tmp_path,
                                  executed=executed, peer_bindings=peer_bindings)
    finally:
        adapter._response_store.close()
        adapter._run_idempotency_store.close()
        store._db.close()


async def _create_headless_peer_bot_chat(headless_peer):
    created = await headless_peer.cli.post(
        '/api/sessions', json={'title': 'Bot Chat', 'source': 'bot_peer_dm'})
    body = await created.json()
    assert created.status == 201, body
    session_id = body['session']['id']
    row = headless_peer.authority.db.get_session_by_title('Bot Chat')
    assert row['id'] == session_id and row['source'] == 'cli', row
    return session_id


async def _wait_for_peer_run(headless_peer, session_id, text):
    accepted_response = await headless_peer.cli.post(
        '/v1/runs', json={'input': text, 'session_id': session_id, 'author': AUTHOR})
    accepted = await accepted_response.json()
    assert accepted_response.status == 202, accepted
    assert accepted['status'] == 'started', accepted
    run_id = accepted['run_id']
    status = None
    for _ in range(100):
        status = await (await headless_peer.cli.get(f'/v1/runs/{run_id}')).json()
        if status['status'] in ('completed', 'failed', 'cancelled'):
            break
        await asyncio.sleep(0.05)
    assert status is not None and status['status'] == 'completed' and status['delivery_id'], status
    return status


@pytest.mark.asyncio
async def test_headless_cli_bot_chat_peer_run_uses_the_durable_owner(headless_peer):
    session_id = await _create_headless_peer_bot_chat(headless_peer)
    status = await _wait_for_peer_run(headless_peer, session_id, 'headless')
    assert [(sid, text) for sid, _, text in headless_peer.executed] == [(session_id, 'headless')]
    assert status['delivery_id']
    source_records = [record for record in headless_peer.peer_bindings
                      if record.get('session_id') == session_id
                      and record.get('_api_producer_phase') == 'bot_i02']
    assert source_records and all(record.get('_api_producer_source') == 'bot_chat'
                                  for record in source_records)


@pytest.mark.asyncio
async def test_plain_cli_projection_cannot_normalize_to_bot_room(headless_peer):
    session_id = await _create_headless_peer_bot_chat(headless_peer)
    await _wait_for_peer_run(headless_peer, session_id, 'projection')
    source_record = next(
        record for record in headless_peer.peer_bindings
        if record.get('session_id') == session_id
        and record.get('_api_producer_phase') == 'bot_i02'
        and record.get('_api_producer_source') == 'bot_chat')
    metadata_less = dict(source_record)
    metadata_less.pop('_api_producer_phase')
    metadata_less.pop('_api_producer_source')
    from gateway.platforms.api_server_run_peer import peer_source_kind
    assert peer_source_kind(headless_peer.authority, metadata_less) is None



async def _create_ordinary_cli_session(headless_peer):
    """Create an ordinary CLI session through the authenticated local owner path."""
    from gateway.runtime_bootstrap import _PURPOSE_CAPABILITIES
    from gateway.session_controls import AuthorityConnection

    owner = AuthorityConnection(headless_peer.authority, object(), {
        'user_id': 'uid:1000', 'provider': 'local',
        'profile_id': headless_peer.authority.profile_id,
        'instance_id': headless_peer.authority.instance_id,
        'capabilities': sorted(_PURPOSE_CAPABILITIES['interactive']),
        'native_bootstrap': True, 'profile_scope': 'profile'}, operator=True)
    response = await owner.dispatch({
        'id': 'ordinary-cli', 'method': 'session.create', 'params': {
            'request_id': 'ordinary-cli', 'source': 'cli',
            'cwd': str(headless_peer.home), 'model': 'fixture', 'toolsets': [],
            'title': 'Ordinary CLI'}})
    assert 'result' in response, response
    session_id = response['result']['session_id']
    row = headless_peer.authority.db.get_session(session_id)
    assert row['source'] == 'cli' and row['title'] == 'Ordinary CLI', row
    return session_id


@pytest.mark.asyncio
async def test_ordinary_cli_session_rejects_a_copied_bot_projection(headless_peer):
    bot_session_id = await _create_headless_peer_bot_chat(headless_peer)
    await _wait_for_peer_run(headless_peer, bot_session_id, 'ordinary-projection')
    ordinary_session_id = await _create_ordinary_cli_session(headless_peer)
    assert ordinary_session_id != bot_session_id
    source_record = next(
        record for record in headless_peer.peer_bindings
        if record.get('session_id') == bot_session_id
        and record.get('_api_producer_phase') == 'bot_i02'
        and record.get('_api_producer_source') == 'bot_chat')
    ordinary_projection = dict(source_record, session_id=ordinary_session_id)
    if 'target_session_id' in ordinary_projection:
        ordinary_projection['target_session_id'] = ordinary_session_id
    from gateway.platforms.api_server_bot_chat import bot_chat_tip
    from gateway.platforms.api_server_run_peer import peer_source_kind
    assert bot_chat_tip(headless_peer.authority, ordinary_session_id) is None
    assert peer_source_kind(headless_peer.authority, ordinary_projection) is None
