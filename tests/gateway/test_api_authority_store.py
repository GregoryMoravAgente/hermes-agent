"""API storage selection borrows the runtime's store, never another profile's."""
import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from tests.gateway.conftest import runtime_owned_api


@pytest_asyncio.fixture
async def owned_api(tmp_path, monkeypatch):
    """Use the production profile selection after the real custody bootstrap."""
    async with runtime_owned_api(
        tmp_path, monkeypatch, api_key='owned-api-test-secret'
    ) as adapter:
        yield adapter, adapter.gateway_runner.session_authority.db


@pytest.mark.asyncio
async def test_bound_api_borrows_selected_authority_store(owned_api):
    adapter, db = owned_api
    db.create_session('owned-session', source='api_server')
    assert adapter._ensure_session_db() is db
    assert await adapter._ensure_session_db_async() is db
    app = web.Application()
    app.router.add_get('/api/sessions/{session_id}', adapter._handle_get_session)
    async with TestClient(TestServer(app)) as client:
        denied = await client.get('/api/sessions/owned-session')
        assert denied.status == 401
        reply = await client.get('/api/sessions/owned-session',
                                 headers={'Authorization': 'Bearer owned-api-test-secret'})
        assert reply.status == 200, await reply.text()
        assert (await reply.json())['session']['id'] == 'owned-session'
    adapter._close_cached_session_dbs()
    assert db.get_session('owned-session')['id'] == 'owned-session'


@pytest.mark.asyncio
async def test_bound_api_never_opens_unserved_home(owned_api, tmp_path, monkeypatch):
    adapter, db = owned_api
    other = tmp_path / 'unserved'
    monkeypatch.setenv('HERMES_HOME', str(other))
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    other_token = set_hermes_home_override(other)
    try:
        # Even an explicit legacy override cannot bypass runtime profile selection.
        adapter._session_db = db
        assert adapter._ensure_session_db() is None
        assert await adapter._ensure_session_db_async() is None
        assert not (other / 'state.db').exists()
    finally:
        reset_hermes_home_override(other_token)
    adapter.gateway_runner.session_authority = None
    adapter.gateway_runner.session_authorities = None
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    assert adapter._ensure_session_db() is None
    assert await adapter._ensure_session_db_async() is None
