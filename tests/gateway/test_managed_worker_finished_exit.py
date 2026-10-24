"""A managed turn whose worker acknowledged its durable finish keeps its answer even when the
worker's interpreter is slow to exit or exits non-zero afterwards (interpreter teardown,
a lingering non-daemon thread, a Windows launcher): that is close()'s cleanup, not a lost
execution. Real child process, real SQLite authority."""
import asyncio
import subprocess
import sys

import pytest

from gateway.session_contract import SessionRef
from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

CHILD = """import json, os, sys, time, psutil
me = psutil.Process()
out = os.fdopen(os.dup(sys.stdout.fileno()), 'wb', buffering=0)
def send(frame):
    out.write((json.dumps(frame) + '\\n').encode())
send({'type': 'hello', 'pid': me.pid, 'birth': me.create_time(), 'ancestors': [p.pid for p in me.parents()[:3]]})
store = None
rpc = None
for line in sys.stdin.buffer:
    frame = json.loads(line)
    if 'version' in frame:
        from agent.managed_worker import outbox_dir
        from agent.runtime_session_store import RuntimeSessionStore, WorkerRPC
        scope = dict(frame['scope'])
        rpc = WorkerRPC(frame['home'])
        adopted = rpc('worker.adopt', **{key: value for key, value in scope.items() if key != 'epoch'})
        store = RuntimeSessionStore(
            rpc, scope, outbox_dir(frame['home'], scope['execution_id']), adopted,
            managed_custody=frame.get('managed_custody'),
        )
        if str(frame.get('text', '')).endswith('-True'):
            parent_session = scope['session_id']
            compression_holder = 'managed-fixture-compression'
            if not store.try_acquire_compression_lock(parent_session, compression_holder):
                raise RuntimeError('managed_fixture_compression_lock_unavailable')
            store.publish_compression_child(
                parent_session_id=parent_session,
                child_session_id=f'{parent_session}-c1',
                source='cli',
                messages=[
                    {'role': 'user', 'content': 'ROTATED_INPUT'},
                    {'role': 'assistant', 'content': 'ROTATED_OUTPUT'},
                ],
                compression_lock_holder=compression_holder,
            )
            store.queue_token_counts(
                store.scope['session_id'], input_tokens=100, output_tokens=10,
                api_call_count=1, model='m',
            )
        send({'type': 'result', 'result': {'final_response': 'FINISHED_ANSWER', 'model': 'm', 'failure_reason': 'billing',
              'billing_block': {'provider': 'custom', 'provider_label': 'Custom', 'model': 'm', 'billing_url': None,
                                'is_nous': False, 'message': 'Add credits'}}})
    elif frame == {'type': 'finish'}:
        messages = [
            {'role': 'user', 'content': 'FINISHED_INPUT'},
            {'role': 'assistant', 'content': 'FINISHED_ANSWER'},
        ]
        store.append_messages_batch(store.scope['session_id'], messages)
        finished = store.finish()
        send({'type': 'finished', 'managed_durable_proof': finished['managed_durable_proof']})
        store.close()
        rpc.close()
        break
if sys.argv[1] == 'nonzero':
    os._exit(3)
time.sleep(60)  # interpreter teardown that outlives the owner's exit wait
"""


@pytest.mark.platforms("linux")
@pytest.mark.parametrize(('exit_shape', 'rotate'), [('nonzero', False), ('lingering', False), ('nonzero', True)])
def test_finished_worker_keeps_its_result_on_slow_or_nonzero_exit(tmp_path, monkeypatch, exit_shape, rotate):
    from gateway import session_managed_worker as managed
    from gateway.session_policy import build_policy
    from hermes_state_runtime import get_session_admission
    spawned = []

    class Child(subprocess.Popen):
        def __init__(self, args, **kw):
            super().__init__([sys.executable, '-c', CHILD, exit_shape], **kw)
            spawned.append(self)
    monkeypatch.setattr(managed.subprocess, 'Popen', Child)

    async def scenario():
        fixture = await real_runtime_binding(
            tmp_path, monkeypatch, include_probe=False, include_worker_runtime=True,
        )
        authority = fixture.authority
        try:
            binding = await fixture.claim(f'finished-{exit_shape}-{rotate}')
            db = authority.db
            ref = SessionRef(binding.profile_id, binding.session_id)
            row = get_session_admission(db, admission_id=binding.admission_id)
            assert row is not None
            # The worker's per-call usage writes, already committed by the time it reports 'finished'.
            db.update_token_counts(
                binding.session_id, input_tokens=200, output_tokens=50,
                cache_read_tokens=800, api_call_count=3, model='m',
            )
            policy = build_policy({'cwd': str(tmp_path), 'model': 'm'}, {}, profile_terminal=False)
            async with asyncio.timeout(40):
                response = await managed.execute_managed(
                    authority, ref, row, policy, binding,
                )
            status = db._read_one(
                'SELECT status FROM session_admissions WHERE admission_id=?',
                (row['admission_id'],),
            )[0]
            assert response == 'FINISHED_ANSWER'
            captured = authority.pending_results[row['admission_id']]
            assert captured['result']['final_response'] == 'FINISHED_ANSWER'
            assert status == 'started', 'the finished turn was marked unknown instead of reaching settlement'
            assert spawned[0].poll() is not None, "close() did not reap the worker"
            # A local viewer's message.complete gets the billing banner and turn-end usage, as in process.
            from gateway.session_settlement_recovery import completion_payload
            complete = completion_payload(row, {'outcome': 'completed'}, response, captured)
            assert complete['billing']['message'] == 'Add credits' and complete['failure_reason'] == 'billing'
            if rotate:
                assert complete['usage'] == {'model': 'm', 'input': 300, 'output': 60, 'reasoning': 0, 'prompt': 1100,
                                             'completion': 60, 'total': 1160, 'calls': 4, 'cache_hit_pct': 73}
            else:
                assert complete['usage'] == {'model': 'm', 'input': 200, 'output': 50, 'reasoning': 0, 'prompt': 1000,
                                             'completion': 50, 'total': 1050, 'calls': 3, 'cache_hit_pct': 80}
        finally:
            # The production drain clears these per-generation runtime stamps around its
            # settlement wrapper; this direct fixture invokes execute_managed itself.  Do this
            # on both success and a causal custody failure so runner.stop cannot turn the original
            # result into a shutdown-ABI traceback.
            if 'binding' in locals():
                authority.executing.pop(binding.session_id, None)
                authority.adopted.pop(binding.session_id, None)
            await fixture.cleanup_async()

    asyncio.run(scenario())
