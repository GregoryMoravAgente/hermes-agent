"""Graph boundary uses the canonical internal queue, not a background ACK."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys


def test_graph_boundary_commits_only_valid_notifications(tmp_path):
    result = subprocess.run([sys.executable, __file__], env=dict(os.environ, HERMES_HOME=str(tmp_path)),
                            capture_output=True, text=True, timeout=90, check=False)
    assert result.returncode == 0, result.stdout + result.stderr
    print((tmp_path / 'graph-receipt.json').read_text())


async def probe(peer):
    import aiohttp
    import ipaddress
    import threading
    from gateway import run_runtime
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.msgraph_webhook import MSGraphWebhookAdapter
    from gateway.run import GatewayRunner
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.session_authority import initialize_session_authority
    from gateway.session_envelope import restore_native
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_state_runtime import list_session_admissions
    from tests.waiting import async_wait_until
    runner = GatewayRunner()
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='graph-test')
    adapter = MSGraphWebhookAdapter(PlatformConfig(enabled=True, extra={
        'host': '127.0.0.1', 'port': 0, 'client_state': 'owned-state',
        'accepted_resources': ['users/owned/messages'], 'prompt': 'BLOCK_FIFO'}))
    runner.adapters[Platform.MSGRAPH_WEBHOOK] = adapter
    runner._wire_adapter_handlers(adapter)
    assert await adapter.connect()
    port = next(iter(adapter._runner.sites))._server.sockets[0].getsockname()[1]
    url = f'http://127.0.0.1:{port}{adapter._webhook_path}'
    notification = {'id': 'graph-1', 'subscriptionId': 'owned-sub',
                    'resource': 'users/owned/messages/one', 'clientState': 'owned-state'}
    def rows():
        return [r for sid in authority.sessions for r in list_session_admissions(
            authority.db, session_id=sid, pending_only=False)]
    try:
        async with aiohttp.ClientSession() as client:
            # The Graph adapter retains a private authenticated automation envelope, so it cannot
            # simply switch to the generic producer helper.  Exercise its actual route while a
            # real to_thread route reservation is blocked: the I01 root must already be active,
            # and a close/handoff must refuse rather than seal an empty local map.
            coordinator = RuntimeCustodyCoordinator(
                process_instance=authority.instance_id,
                authority_epoch=authority.epoch,
                fence=RetirementFence(),
            )
            runner.runtime_custody = coordinator
            runner._running = True
            runner.session_runtime_descriptor = {
                'instance_id': authority.instance_id,
                'authority_epoch': authority.epoch,
            }
            runner._external_drain_active = False
            runner._preservation_drain_active = False
            runner._draining = False
            old_ownership_reason = run_runtime._preservation_ownership_reason
            old_authorities = run_runtime._authorities
            run_runtime._preservation_ownership_reason = lambda _runner, **_kwargs: None
            run_runtime._authorities = lambda _runner: [authority]
            route_entered, route_release = threading.Event(), threading.Event()
            original_reserve_route = authority._reserve_route

            def blocked_reserve_route(source, *, fresh=False):
                route_entered.set()
                if not route_release.wait(10):
                    raise RuntimeError('Graph route barrier expired')
                return original_reserve_route(source, fresh=fresh)

            authority._reserve_route = blocked_reserve_route
            racing_task = None
            try:
                racing_notification = {
                    'id': 'graph-preparation-race', 'subscriptionId': 'owned-sub',
                    'resource': 'users/owned/messages/race', 'clientState': 'owned-state',
                }
                racing_task = asyncio.create_task(client.post(
                    url, json={'value': [racing_notification]}))
                assert await asyncio.to_thread(route_entered.wait, 10)
                assert coordinator.status()['active_owner_classes'] == ['I01_native_inbound']
                close_params = {
                    'instance_id': authority.instance_id,
                    'authority_epoch': authority.epoch,
                    'owner_id': 'graph-preparation-race', 'scope': 'process', 'deadline_s': 30,
                }
                close_receipt = await run_runtime.begin_preservation_drain(
                    runner, close_params, 'test-peer')
                assert close_receipt['status'] == 'closing'
                pending = await run_runtime.prepare_preservation_handoff(
                    runner, {'receipt': close_receipt}, 'test-peer')
                assert pending == {
                    'status': 'refused', 'reason': 'unknown_owner',
                    'owner_class': 'I01_native_inbound',
                }
                route_release.set()
                raced = await racing_task
                assert raced.status == 503
                await raced.read()
                raced.release()
                assert coordinator.status()['active_owner_classes'] == []
                reopened = await run_runtime.abort_preservation_drain(
                    runner, {'receipt': close_receipt}, 'test-peer')
                assert reopened['status'] == 'open'
            finally:
                route_release.set()
                if racing_task is not None and not racing_task.done():
                    await racing_task
                authority._reserve_route = original_reserve_route

            for change, status in [({'clientState': 'wrong'}, 403), ({'resource': 'users/foreign'}, 400)]:
                response = await client.post(url, json={'value': [{**notification, **change}]})
                assert response.status == status and not rows()
            adapter._allowed_source_networks = [ipaddress.ip_network('192.0.2.0/24')]
            denied = await client.post(url, json={'value': [notification]})
            assert denied.status == 403 and not rows()
            adapter._allowed_source_networks = []
            authority.db._execute_write(lambda conn: conn.execute("CREATE TRIGGER fail_graph BEFORE INSERT ON session_admissions BEGIN SELECT RAISE(ABORT, 'owned storage failure'); END"))
            response = await client.post(url, json={'value': [notification]})
            assert response.status == 503, response.status
            assert not rows() and not peer.requests
            authority.db._execute_write(lambda conn: conn.execute('DROP TRIGGER fail_graph'))
            response = await client.post(url, json={'value': [notification]})
            assert response.status == 202 and len(rows()) == 1
            assert restore_native(rows()[0]['payload'], runner).internal is True
            assert await asyncio.to_thread(peer.blocked.wait, 10)
            adapter._seen_receipts.clear()
            duplicate = await client.post(url, json={'value': [notification]})
            assert duplicate.status == 202 and len(rows()) == 1
            peer.release.set()
            await async_wait_until(lambda: rows()[0]['status'] == 'terminal', timeout=25,
                                   what='the Graph notification turn to settle')
            assert len(peer.requests) == 1
            # The durable terminal row precedes final delivery, waiter resolution, and the exact
            # preservation release. Observe the coordinator after that source-owned finalizer too.
            await async_wait_until(
                lambda: coordinator.status()['active_owner_classes'] == [], timeout=25,
                what='the Graph delivery custody release')
            # A plugin scheduler owns this route exclusively; do not also infer.
            calls = []
            scheduler_started, scheduler_release = asyncio.Event(), asyncio.Event()
            async def scheduler(raw, event):
                calls.append(raw['id'])
                async def run_scheduler_job():
                    scheduler_started.set()
                    await scheduler_release.wait()
                task = asyncio.create_task(run_scheduler_job())
                adapter._background_tasks.add(task)
                task.add_done_callback(adapter._background_tasks.discard)
                return task
            adapter.set_notification_scheduler(scheduler)
            response = await client.post(url, json={'value': [{**notification, 'id': 'plugin-only'}]})
            assert response.status == 202 and calls == ['plugin-only']
            await scheduler_started.wait()
            assert coordinator.status()['active_owner_classes'] == ['I01_native_inbound']
            scheduler_close = await run_runtime.begin_preservation_drain(
                runner, {
                    'instance_id': authority.instance_id,
                    'authority_epoch': authority.epoch,
                    'owner_id': 'plugin-only', 'scope': 'process', 'deadline_s': 30,
                }, 'test-peer')
            scheduler_pending = await run_runtime.prepare_preservation_handoff(
                runner, {'receipt': scheduler_close}, 'test-peer')
            assert scheduler_pending == {
                'status': 'refused', 'reason': 'unknown_owner',
                'owner_class': 'I01_native_inbound',
            }
            scheduler_release.set()
            await async_wait_until(
                lambda: coordinator.status()['active_owner_classes'] == [], timeout=10,
                what='the scheduler-owned I01 task to settle')
            scheduler_reopened = await run_runtime.abort_preservation_drain(
                runner, {'receipt': scheduler_close}, 'test-peer')
            assert scheduler_reopened['status'] == 'open'
            assert len(rows()) == len(peer.requests) == 1

            # A real durable-write failure followed by an unavailable identity read retains an
            # unknown I01 lease.  Even after local queues are empty, the integrated owner must stay
            # unsealed until that custody is resolved.
            adapter.set_notification_scheduler(None)
            authority.db._execute_write(lambda conn: conn.execute(
                "CREATE TRIGGER fail_graph_unknown BEFORE INSERT ON session_admissions "
                "BEGIN SELECT RAISE(ABORT, 'owned identity read failure'); END"))
            original_current_owner = authority._current_owner

            def unavailable_current_owner(_session_id):
                raise RuntimeError('identity read unavailable')

            try:
                authority._current_owner = unavailable_current_owner
                unknown_response = await client.post(url, json={'value': [{
                    **notification, 'id': 'graph-unknown-custody'}]})
                assert unknown_response.status == 503
                await unknown_response.read()
            finally:
                authority._current_owner = original_current_owner
                authority.db._execute_write(lambda conn: conn.execute(
                    'DROP TRIGGER fail_graph_unknown'))
            assert coordinator.status()['active_owner_classes'] == ['I01_native_inbound']
            unknown_close = await run_runtime.begin_preservation_drain(
                runner, {
                    'instance_id': authority.instance_id,
                    'authority_epoch': authority.epoch,
                    'owner_id': 'graph-unknown-custody', 'scope': 'process', 'deadline_s': 30,
                }, 'test-peer')
            assert unknown_close['status'] == 'closing'
            unknown_pending = await run_runtime.prepare_preservation_handoff(
                runner, {'receipt': unknown_close}, 'test-peer')
            assert unknown_pending == {
                'status': 'refused', 'reason': 'unknown_owner',
                'owner_class': 'I01_native_inbound',
            }
            unknown_reopened = await run_runtime.abort_preservation_drain(
                runner, {'receipt': unknown_close}, 'test-peer')
            assert unknown_reopened['status'] == 'open'
            Path(os.environ['HERMES_HOME'], 'graph-receipt.json').write_text(json.dumps({
                'admissions': len(rows()), 'inferences': len(peer.requests), 'boundary_controls': True,
                'internal': True, 'storage_retry': True, 'scheduler_exclusive': True,
                'i01_graph_preparation_race': True, 'i01_unknown_custody_refused': True}))
    finally:
        if 'old_ownership_reason' in locals():
            run_runtime._preservation_ownership_reason = old_ownership_reason
            run_runtime._authorities = old_authorities
        peer.release.set()
        await adapter.disconnect()


if __name__ == '__main__':
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    sys.path.insert(1, str(Path(__file__).parent))
    import test_webhook_authority as harness
    import traceback
    harness.probe = probe
    status = 0
    try:
        harness.main()
    except BaseException:
        traceback.print_exc()
        status = 1
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(status)
