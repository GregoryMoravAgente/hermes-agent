"""A successful profile retirement settles every observer still waiting on a queued admission:
API/webhook/Bot futures fail with runtime_draining instead of hanging, the native set is cleared,
and the durable row stays queued for the profile's next owner (never resent, never dropped)."""
import asyncio

import pytest

from gateway import run_runtime
from hermes_state_runtime import RuntimeStoreError, list_session_admissions
from tests.gateway.test_profile_retire_stops_turns import ACTOR, REF, _Agent, _authority
from gateway.session_contract import Submission
from tests.waiting import async_wait_until


@pytest.mark.asyncio
async def test_successful_retirement_fails_pending_waiters_and_keeps_rows_queued(tmp_path, monkeypatch):
    from gateway import session_finite
    agent = _Agent()
    db, authority, execute, _returned = _authority(tmp_path, agent)
    monkeypatch.setattr(session_finite, 'execute_finite_admission', execute)
    monkeypatch.setattr(run_runtime, 'TURN_SETTLE_SECONDS', 1.0, raising=False)
    monkeypatch.setattr('gateway.session_cron.unbind_owner', lambda authority: None)
    try:
        with db:
            await authority.submit(ACTOR, Submission(request_id='running', ref=REF, payload={'text': 'a'}, intent='queue'))
            await asyncio.wait_for(asyncio.to_thread(agent.started.wait, 5), 6)
            # A second session whose drain paused 'session_busy' (a live worker held its head):
            # its API/webhook/Bot observer deliberately keeps waiting, and no drain is left to
            # release it when retirement refuses the next claim.
            authority.sessions['t'] = type(authority.sessions['s'])(authority.sessions['s'].source, 'route-t')
            db.create_session('t', source='test')
            monkeypatch.setattr(authority, '_schedule', lambda ref: None)
            follower = await authority.submit(ACTOR, Submission(
                request_id='follower', ref=REF.__class__('owned', 't'), payload={'text': 'b'}, intent='queue'))
            waiter = authority.waiters.setdefault(follower.admission_id, asyncio.get_running_loop().create_future())
            authority.native_waiters.add('messaging-delivery')

            assert await run_runtime._retire_profile_authority(authority) is True

            assert waiter.done(), 'observer left pending after retirement'
            with pytest.raises(RuntimeStoreError, match='runtime_draining'):
                waiter.result()
            assert not authority.waiters and not authority.native_waiters
            rows = {r['request_id']: r for r in list_session_admissions(db, session_id='t', pending_only=False)}
            assert rows['follower']['status'] == 'queued'  # durable: the next owner runs it
    finally:
        agent.release.set()
        await async_wait_until(lambda: all(task.done() for task in run_runtime._authority_tasks(authority)),
                               timeout=10, what="the authority's tasks finished")
        db.close()


@pytest.mark.asyncio
async def test_api_observer_releases_before_authority_is_retired(tmp_path, monkeypatch):
    """The real API observer wakes while its profile authority is still canonical and retiring."""
    from gateway.session_api_turn import AdmissionQueued, observe_api_turn

    agent = _Agent()
    db, authority, _execute, _returned = _authority(tmp_path, agent)
    monkeypatch.setattr(run_runtime, 'TURN_SETTLE_SECONDS', 1.0, raising=False)
    # The observer is the consumer under test; leave its durable row queued rather than starting
    # an unrelated model turn. The admission and waiter still come from SessionAuthority.
    monkeypatch.setattr(authority, '_schedule', lambda _ref: None)
    observed = {}
    waiter = None

    def unbind(value):
        observed.update({
            'retiring': value.retiring,
            'retired': value.retired,
            'waiter_done': waiter.done(),
            'waiters_empty': not value.waiters,
        })

    monkeypatch.setattr('gateway.session_cron.unbind_owner', unbind)
    observer = None
    try:
        with db:
            receipt = await authority.submit(ACTOR, Submission(
                request_id='api-retirement', ref=REF, payload={'text': 'api'}, intent='queue'))
            row = next(
                row for row in list_session_admissions(
                    db, session_id=REF.session_id, pending_only=False)
                if row['admission_id'] == receipt.admission_id
            )
            observer = asyncio.create_task(observe_api_turn((authority, receipt.ref, row)))
            await async_wait_until(
                lambda: receipt.admission_id in authority.waiters,
                timeout=5, what='the API observer waiter registration',
            )
            waiter = authority.waiters[receipt.admission_id]
            authority.native_waiters.add(receipt.admission_id)

            assert await run_runtime._retire_profile_authority(authority) is True
            with pytest.raises(AdmissionQueued):
                await observer
            observer = None

            assert observed == {
                'retiring': True,
                'retired': False,
                'waiter_done': True,
                'waiters_empty': True,
            }
            assert authority.retired is True
            assert not authority.native_waiters
    finally:
        if observer is not None:
            observer.cancel()
            await asyncio.gather(observer, return_exceptions=True)
        db.close()


@pytest.mark.asyncio
async def test_api_completion_join_requires_real_released_receipt(tmp_path, monkeypatch):
    """The retirement join consumes a real API stream completion, not only a done Future."""
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms import api_server_run_custody
    from tests.gateway.conftest import runtime_owned_api
    from tests.gateway.test_api_finalizer_completion_current import (
        _active_session_stream_app, _sse_run_id,
    )
    from tests.gateway.test_api_notification_authority_outcomes import (
        _install_canonical_agent, _install_loopback_provider,
    )

    _install_loopback_provider(monkeypatch, 'retirement completion')
    captured = {}
    original_retire = api_server_run_custody.retire_legacy_source_with_completion

    def retain_for_join(custody):
        captured['custody'] = custody
        return True

    monkeypatch.setattr(api_server_run_custody, 'retire_legacy_source_with_completion', retain_for_join)
    async with runtime_owned_api(tmp_path, monkeypatch) as adapter:
        _install_canonical_agent(monkeypatch, {
            'final_response': 'retirement completion', 'completed': True, 'failed': False,
            'messages': [{'role': 'assistant', 'content': 'retirement completion'}],
        })
        session_id = adapter._ensure_session_db().create_session('retire-join', 'api_server')
        async with TestClient(TestServer(_active_session_stream_app(adapter))) as client:
            response = await client.post(
                f'/api/sessions/{session_id}/chat/stream', json={'message': 'join me'},
            )
            body = await response.text()
            assert response.status == 200 and 'retirement completion' in body
            assert _sse_run_id(body)
        authority = adapter.gateway_runner.session_authority
        registry = authority._api_finalizer_completions
        assert len(registry) == 1
        assert await run_runtime._wait_api_finalizer_completions(authority, 1.0) is True
        completion = next(iter(registry.values()))
        assert completion.receipt.state == 'complete'
        assert captured['custody'].released is True
        assert original_retire(captured['custody']) is True


@pytest.mark.asyncio
async def test_api_completion_join_retains_unknown_finalizer(tmp_path, monkeypatch):
    """A real API UNKNOWN completion remains retained instead of counting as retirement success."""
    from aiohttp.test_utils import TestClient, TestServer
    from gateway.platforms import api_server_run_completion
    from tests.gateway.conftest import runtime_owned_api
    from tests.gateway.test_api_finalizer_completion_current import _active_session_stream_app
    from tests.gateway.test_api_notification_authority_outcomes import (
        _install_canonical_agent, _install_loopback_provider,
    )

    _install_loopback_provider(monkeypatch, 'retirement unknown')
    original_settle = api_server_run_completion.ApiFinalizerCompletion.settle_after_child

    def force_unknown(completion):
        return completion.fail('retirement_unknown')

    monkeypatch.setattr(
        api_server_run_completion.ApiFinalizerCompletion, 'settle_after_child', force_unknown,
    )
    async with runtime_owned_api(tmp_path, monkeypatch) as adapter:
        _install_canonical_agent(monkeypatch, {
            'final_response': 'retirement unknown', 'completed': True, 'failed': False,
            'messages': [{'role': 'assistant', 'content': 'retirement unknown'}],
        })
        session_id = adapter._ensure_session_db().create_session('retire-unknown', 'api_server')
        async with TestClient(TestServer(_active_session_stream_app(adapter))) as client:
            response = await client.post(
                f'/api/sessions/{session_id}/chat/stream', json={'message': 'unknown me'},
            )
            body = await response.text()
            assert response.status == 200 and 'retirement unknown' in body
        authority = adapter.gateway_runner.session_authority
        registry = authority._api_finalizer_completions
        assert len(registry) == 1
        completion = next(iter(registry.values()))
        assert completion.receipt.state == 'unknown'
        assert await run_runtime._wait_api_finalizer_completions(authority, 1.0) is False
        assert registry[completion.admission_id] is completion
    _ = original_settle
