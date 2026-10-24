"""Managed-worker compression routes advance only for the exact worker child identity."""

import asyncio
import json
import subprocess
import sys

import pytest

from gateway.session_contract import SessionRef
from gateway.session_managed_worker import _advance_managed_compression_route
from gateway.session_managed_worker_custody import attach_bootstrap_metadata, reserve_managed_worker
from gateway.session_turn_physical_tip import canonical_store_path
from gateway.session_worker_reservation import reserve_admission_worker
from hermes_state_runtime import get_session_admission
from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding


WORKER = r"""import json, os, sys, time, psutil
me = psutil.Process()
out = os.fdopen(os.dup(sys.stdout.fileno()), 'wb', buffering=0)
def send(frame):
    out.write((json.dumps(frame) + '\n').encode())
send({'type': 'hello', 'pid': me.pid, 'birth': me.create_time(), 'ancestors': [p.pid for p in me.parents()[:3]]})
store = None
rpc = None
for line in sys.stdin.buffer:
    frame = json.loads(line)
    if frame.get('version') == 1:
        from agent.managed_worker import outbox_dir
        from agent.runtime_session_store import RuntimeSessionStore, WorkerRPC
        scope = dict(frame['scope'])
        rpc = WorkerRPC(frame['home'])
        adopted = rpc('worker.adopt', **{key: value for key, value in scope.items() if key != 'epoch'})
        store = RuntimeSessionStore(
            rpc, scope, outbox_dir(frame['home'], scope['execution_id']), adopted,
            managed_custody=frame.get('managed_custody'),
        )
        children = []
        for index in (1, 2):
            parent_session = store.scope['session_id']
            compression_holder = f'managed-route-identity-test-{index}'
            if not store.try_acquire_compression_lock(parent_session, compression_holder):
                raise RuntimeError('managed_route_compression_lock_unavailable')
            child_session = f'{parent_session}-compression-child-{index}'
            store.publish_compression_child(
                parent_session_id=parent_session,
                child_session_id=child_session,
                source='cli',
                messages=[
                    {'role': 'user', 'content': f'INPUT_{index}'},
                    {'role': 'assistant', 'content': f'SUMMARY_{index}'},
                ],
                compression_lock_holder=compression_holder,
            )
            children.append(child_session)
        send({'type': 'published', 'scope': store.scope, 'children': children})
    elif frame == {'type': 'proof'}:
        if store is not None:
            send({'type': 'proof', 'managed_durable_proof': store._managed_finish_proof()})
    elif frame == {'type': 'finish'}:
        if store is not None:
            store.close()
        if rpc is not None:
            rpc.close()
        break
time.sleep(60)
"""


def _route_proof(scope, admission, store_path, target, **overrides):
    """The managed child proof header required before the route CAS."""
    proof = {
        'session_id': target,
        'admission_id': admission['admission_id'],
        'execution_id': scope['execution_id'],
        'authority_epoch': scope['epoch'],
        'execution_generation': scope['generation'],
        'store_path': store_path,
    }
    return {**proof, **overrides}


@pytest.mark.platforms("linux")
def test_managed_worker_route_rejects_stale_and_foreign_children(tmp_path, monkeypatch):
    """A real worker execution may advance only to its own committed compression child."""
    async def read_frame(process):
        line = await asyncio.to_thread(process.stdout.readline)
        assert line, 'managed route worker exited before sending its receipt'
        return json.loads(line)

    async def scenario():
        fixture = await real_runtime_binding(
            tmp_path, monkeypatch, include_probe=False, include_worker_runtime=True,
        )
        child = None
        custody = None
        try:
            binding = await fixture.claim('compression-route-identity')
            authority = fixture.authority
            db = authority.db
            ref = SessionRef(binding.profile_id, binding.session_id)
            admission = get_session_admission(db, admission_id=binding.admission_id)
            assert admission is not None

            child = await asyncio.to_thread(
                subprocess.Popen,
                [sys.executable, '-c', WORKER],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            hello = await read_frame(child)
            assert hello['type'] == 'hello'
            scope = reserve_admission_worker(
                authority, admission_id=admission['admission_id'], process=child,
                principal_id=admission['principal_id'], hello=hello,
            )

            original_route = authority.sessions[ref.session_id].route
            original_entry = authority.runner.session_store.lookup_by_session_key(original_route)
            assert original_entry is not None
            assert original_entry.session_id == scope['session_id']
            store_path = canonical_store_path(authority)
            custody = await asyncio.to_thread(reserve_managed_worker, binding, store_path)
            custody.configure_streams({'stdout'})
            custody.bind_process(child)
            attach_bootstrap_metadata(custody, {}, child, scope)

            # The real worker is registered against the parent before it publishes its
            # compression child.  A foreign proof must remain UNKNOWN and leave the route
            # untouched at this boundary.
            with pytest.raises(ValueError, match='managed_compression_route_unknown'):
                _advance_managed_compression_route(
                    authority, ref, admission, scope,
                    _route_proof(scope, admission, store_path, 'foreign-child'),
                    binding=binding, custody=custody,
                )
            assert authority.runner.session_store.lookup_by_session_key(original_route).session_id == scope['session_id']

            # A target with the right durable shape but a copied generation is not
            # allowed to move the in-memory route before finish settlement.
            with pytest.raises(ValueError, match='managed_compression_route_unknown'):
                _advance_managed_compression_route(
                    authority, ref, admission, scope,
                    _route_proof(scope, admission, store_path, 'foreign-generation',
                                 execution_generation=scope['generation'] + 1),
                    binding=binding, custody=custody,
                )

            child.stdin.write(json.dumps({
                'version': 1, 'scope': scope, 'home': authority.profile_id,
                'managed_custody': {
                    'store_path': store_path,
                    'admission_id': admission['admission_id'],
                    'session_id': scope['session_id'],
                    'generation': scope['generation'],
                },
            }).encode() + b'\n')
            child.stdin.flush()
            published = await read_frame(child)
            assert published['type'] == 'published'
            children = published['children']
            assert len(children) == 2
            child_session = children[-1]
            assert published['scope']['session_id'] == child_session
            assert children[0].startswith(f"{scope['session_id']}-compression-child-1")
            assert children[1].startswith(f"{children[0]}-compression-child-2")

            child.stdin.write(b'{"type":"proof"}\n')
            child.stdin.flush()
            proof_frame = await read_frame(child)
            assert proof_frame['type'] == 'proof'
            source_proof = proof_frame['managed_durable_proof']
            assert source_proof['session_id'] == child_session
            assert source_proof['row_roles'] == ['user', 'assistant']
            assert source_proof['result_row_ids'] == [source_proof['row_ids'][1]]

            physical = custody.physical
            assert physical is not None
            initial_durable_proofs = dict(physical.durable_proofs)
            initial_durable_requests = set(custody.service.durable_requests)
            initial_state = physical.state

            def assert_rejected_without_receipt_effect(proof):
                with pytest.raises(ValueError, match='managed_compression_route_unknown'):
                    _advance_managed_compression_route(
                        authority, ref, admission, scope, proof,
                        binding=binding, custody=custody,
                    )
                assert authority.runner.session_store.lookup_by_session_key(original_route).session_id == scope['session_id']
                assert physical.durable_proofs == initial_durable_proofs
                assert custody.service.durable_requests == initial_durable_requests
                assert physical.state == initial_state

            missing_rows = dict(source_proof)
            missing_rows.pop('row_ids')
            assert_rejected_without_receipt_effect(missing_rows)
            tampered_rows = dict(source_proof)
            tampered_rows['row_snapshots'] = [*source_proof['row_snapshots'][:-1], 'tampered-row']
            assert_rejected_without_receipt_effect(tampered_rows)
            tampered_digest = dict(source_proof, payload_digest='0' * 64)
            assert_rejected_without_receipt_effect(tampered_digest)

            for invalid_state in ('unknown', 'closed'):
                physical.state = invalid_state
                with pytest.raises(ValueError, match='managed_compression_route_unknown'):
                    _advance_managed_compression_route(
                        authority, ref, admission, scope, source_proof,
                        binding=binding, custody=custody,
                    )
                assert authority.runner.session_store.lookup_by_session_key(original_route).session_id == scope['session_id']
                assert physical.durable_proofs == initial_durable_proofs
                assert custody.service.durable_requests == initial_durable_requests
                assert physical.state == invalid_state
                physical.state = initial_state

            custody.service.unknown = True
            assert_rejected_without_receipt_effect(source_proof)
            custody.service.unknown = False

            original_get_lineage = db.get_compression_lineage

            def mark_service_unknown(root):
                lineage = original_get_lineage(root)
                custody.service.unknown = True
                return lineage

            monkeypatch.setattr(db, 'get_compression_lineage', mark_service_unknown)
            assert_rejected_without_receipt_effect(source_proof)
            monkeypatch.setattr(db, 'get_compression_lineage', original_get_lineage)
            custody.service.unknown = False

            def replace_physical_after_lineage(root):
                lineage = original_get_lineage(root)
                custody.service.physical = object()
                return lineage

            monkeypatch.setattr(db, 'get_compression_lineage', replace_physical_after_lineage)
            assert_rejected_without_receipt_effect(source_proof)
            monkeypatch.setattr(db, 'get_compression_lineage', original_get_lineage)
            custody.service.physical = physical

            # The proof retains the owner-side reservation scope (parent), while the
            # durable worker assignment and lineage now identify the actual child.
            _advance_managed_compression_route(
                authority, ref, admission, scope,
                source_proof, binding=binding, custody=custody,
            )
            assert authority.runner.session_store.lookup_by_session_key(original_route).session_id == child_session

            # A replaced/stale child cannot replay the same route after the CAS has moved.
            with pytest.raises(ValueError, match='managed_compression_route_unknown'):
                _advance_managed_compression_route(
                    authority, ref, admission, scope,
                    dict(source_proof, session_id=f'{child_session}-stale'),
                    binding=binding, custody=custody,
                )
            assert authority.runner.session_store.lookup_by_session_key(original_route).session_id == child_session
        finally:
            if child is not None:
                try:
                    if child.stdin is not None:
                        child.stdin.write(b'{"type":"finish"}\n')
                        child.stdin.flush()
                except (BrokenPipeError, OSError):
                    pass
                if child.poll() is None:
                    child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)
            if custody is not None and custody.physical is not None and custody.physical.state not in {
                'unknown', 'closed', 'released',
            }:
                custody.mark_unknown('compression_route_test_cleanup')
            await fixture.cleanup_async()

    asyncio.run(scenario())


@pytest.mark.asyncio
async def test_managed_sidecar_no_delivery_requires_typed_same_admission_receipt(
    tmp_path, monkeypatch,
):
    """A real durable sidecar reaches finalization only for its exact typed admission receipt."""
    from gateway.runtime_service_custody import reserve_service
    from gateway.session_ingress import (
        DeliveryReceipt, _finish_preservation_sidecar_delivery,
    )

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    try:
        binding = await fixture.claim('managed-sidecar-no-delivery')
        authority = fixture.authority
        admission_id = binding.admission_id
        service = reserve_service(binding, 'managed-sidecar-no-delivery', producer_key='provider')
        request = service.begin_request(binding)
        authority._preservation_bind_sidecar(
            admission_id, binding.parent_handle, service, request,
        )
        authority._preservation_release_sidecar(admission_id, request, durable=True)
        from hermes_state_runtime import settle_session_input
        settle_session_input(
            authority.db, epoch=authority.epoch, admission_id=admission_id,
            generation=binding.execution_generation, outcome='completed',
        )

        receipt = DeliveryReceipt(
            admission_id, attempted=False, delivered=False,
            reason='no_delivery', effect_state='none',
        )
        _receipt, physical, outcome = await _finish_preservation_sidecar_delivery(
            authority, admission_id, receipt, False, False, False, False,
            lambda candidate: isinstance(candidate, DeliveryReceipt)
            and candidate.admission_id == admission_id
            and candidate.attempted is True
            and candidate.delivered is True,
        )
        assert physical is False
        assert outcome == 'physical_receipt_identity_mismatch'
        assert authority._preservation_sidecars[request.lease.lease_id]['terminal'] is False

        forged = DeliveryReceipt(
            'foreign-admission', attempted=False, delivered=False,
            reason='no_delivery', effect_state='none',
        )
        called = await _finish_preservation_sidecar_delivery(
            authority, admission_id, forged, False, False, False, False,
            lambda _candidate: False,
        )
        assert called[2] is None
        assert authority._preservation_sidecars[request.lease.lease_id]['terminal'] is False
    finally:
        fixture.cleanup()

