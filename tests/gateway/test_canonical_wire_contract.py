"""The canonical dispatcher speaks only the declared wire contract (``tui_gateway/contracts``)."""
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from gateway.session_authority import LiveSession, SessionAuthority
from gateway.session_controls import AuthorityConnection
from gateway.session_group_controls import GROUP_METHODS
from hermes_state import SessionDB
from hermes_state_runtime import begin_runtime_epoch
from tui_gateway.contracts.registry import CANONICAL_METHODS, METHODS

pytest_plugins = ['tests.gateway.test_bot_roster_fleet']


@pytest.fixture
def owner(tmp_path):
    with SessionDB(db_path=tmp_path / 'state.db') as db:
        db.create_session('s', source='test')
        epoch = begin_runtime_epoch(db, instance_id='current')
        authority = SessionAuthority(SimpleNamespace(_draining=False), profile_id='owned',
                                     instance_id='current', db=db, epoch=epoch)
        authority.sessions['s'] = LiveSession(None, 'route')
        yield AuthorityConnection(authority, object(), {'user_id': 'owner'}), db


def test_every_canonical_verb_has_a_declared_contract(owner):
    """Each name the authority answers is a ``METHODS`` (shared with the sidecar) or
    ``CANONICAL_METHODS`` declaration, so it is rendered into the generated TypeScript and
    OpenRPC; and no canonical declaration outlives its handler."""
    connection, _ = owner
    served = set(connection.handlers()) | set(GROUP_METHODS) | {'profiles.list'}
    assert sorted(served - set(METHODS) - set(CANONICAL_METHODS)) == []
    assert sorted(set(CANONICAL_METHODS) - served) == []


@pytest.mark.asyncio
async def test_declared_params_gate_canonical_verbs_before_their_handlers(owner):
    """An unknown or missing key is ``4001 invalid_params`` naming the field, including verbs
    whose handlers only ``.get`` their keys; a well-formed call is unaffected."""
    connection, db = owner
    revision = db.get_session('s')['runtime_revision']
    mutate = {'session_id': 's', 'request_id': 'r1', 'expected_revision': revision,
              'operation': 'rename', 'payload': {'title': 'Declared'}}
    try:
        for method, params, field in (
                ('prompt.receipt', {'session_id': 's', 'admission_id': 'a', 'bogus': 1}, 'bogus'),
                ('prompt.cancel', {'session_id': 's'}, 'admission_id'),
                ('session.mutate', {**mutate, 'expected_revison': revision}, 'expected_revison')):
            response = await connection.dispatch({'id': 1, 'method': method, 'params': params})
            assert response['error']['code'] == 4001, (method, response)
            assert response['error']['data']['reason'] == 'invalid_params'
            assert field in response['error']['data']['fields'], (method, response)
        applied = await connection.dispatch({'id': 2, 'method': 'session.mutate', 'params': mutate})
        CANONICAL_METHODS['session.mutate'].result.model_validate(applied['result'])
        assert db.get_session('s')['title'] == 'Declared'
    finally:
        await connection.close()


@pytest.mark.asyncio
async def test_real_bot_relay_result_carries_typed_metadata_and_rejects_unknown_fields(fleet, monkeypatch):
    from gateway.session_authorities import owner_scope
    from tests.waiting import async_wait_until
    from tools.bot_live_delivery import read_delivery_result

    connection = fleet.connect('alpha')
    authority = connection.authority
    errors = ['Error code: 429 - rate limit exceeded']

    async def execute(owner, _ref, row):
        if errors:
            owner.pending_results[row['admission_id']] = {
                'result': {'final_response': '', 'messages': [], 'failed': True,
                           'error': errors.pop()}, 'usage': {}}
            return ''
        return 'pong'

    monkeypatch.setattr('gateway.session_finite.execute_finite_admission', execute)
    params = {'id': 'a' * 32, 'profile': 'alpha', 'message': 'ping'}
    try:
        with owner_scope(authority):
            response = await connection.dispatch({
                'jsonrpc': '2.0',
                'id': 'canonical-bot-relay',
                'method': 'bot_relay.deliver',
                'params': params,
            })
        assert 'result' in response, response
        admission_id = response['result']['admission_id']
        await async_wait_until(
            lambda: (read_delivery_result(fleet.homes['alpha'], params['id']) or {}).get('status') == 'settled',
            timeout=10, what='real retry mailbox projection')
        with owner_scope(authority):
            response = await connection.dispatch({
                'id': 'canonical-bot-relay-retry', 'method': 'bot_relay.deliver', 'params': params,
            })
    finally:
        await connection.close()

    assert 'result' in response, response
    result = response['result']
    assert result['admission_id']
    assert result['profile_id'] == authority.profile_id
    assert result['authority_epoch'] == authority.epoch
    assert result['admission_id'] == admission_id
    assert result['retry_admission_id']
    assert result['execution_generation'] >= 1
    assert result['retry_execution_generation'] >= 1
    contract = CANONICAL_METHODS['bot_relay.deliver'].result
    contract.model_validate(result)
    with pytest.raises(ValidationError):
        contract.model_validate({**result, 'unexpected': True})
