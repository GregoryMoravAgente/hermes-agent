"""Real API compression-tip writer custody controls.

The production API bootstrap and loopback model seam exercise the actual AIAgent writer.  The
writer handoff is observed while the I04 binding is still live; post-execution assertions only
read the proof captured at that handoff and the rows it wrote.
"""

from pathlib import Path

import pytest

from agent.session_persistence_proof import GatewayWriterProofHandoff, SessionDBWriteProof
from gateway.runtime_service_custody import current_turn_binding
from gateway.session_api_turn import admit_api_turn
from gateway.session_ingress import execute_admission
from gateway.session_turn_physical_tip import PhysicalTipWitness, resolve_physical_tip_witness


def _publish_real_compression(owner, parent, child, content):
    holder = f"compression-test:{parent}"
    assert owner.db.try_acquire_compression_lock(parent, holder)
    try:
        owner.db.publish_compression_child(
            parent_session_id=parent, child_session_id=child, source="api_server",
            messages=[{"role": "assistant", "content": content}],
            compression_lock_holder=holder,
        )
    finally:
        owner.db.release_compression_lock(parent, holder)


async def _prepare_compressed_api_admission(api, owner, *, session_id, request_id, child):
    _, ref, admitted = admit_api_turn(
        api, session_id=session_id, request_id=request_id,
        user_message="physical child", conversation_history=[],
    )
    _, retry_ref, retry = admit_api_turn(
        api, session_id=session_id, request_id=request_id,
        user_message="physical child", conversation_history=[],
    )
    assert retry_ref == ref
    assert retry["admission_id"] == admitted["admission_id"]
    _publish_real_compression(owner, ref.session_id, child, "summary")
    route = owner.sessions[ref.session_id].route
    assert owner.runner.session_store.advance_compression_session(route, ref.session_id, child)
    home = Path(owner.db.db_path).expanduser().resolve().parent
    store_path = str(Path(owner.db.db_path).expanduser().resolve())
    from gateway.runtime_ownership import process_ownership
    owner_evidence = process_ownership.reservation_evidence(home)
    assert isinstance(owner_evidence, dict)
    original_owner_epoch = owner_evidence["owner_epoch"]
    live = owner.sessions[ref.session_id]
    row, claim = await owner._claim_next(ref, live)
    assert row["admission_id"] == claim["admission_id"] == admitted["admission_id"]
    return ref, row, child, home, store_path, original_owner_epoch


@pytest.mark.asyncio
async def test_real_openai_api_writer_handoff_binds_current_tip_before_i04_release(
    runtime_owned_api_owner, monkeypatch,
):
    api, owner = runtime_owned_api_owner
    observed = {}
    home = Path(owner.db.db_path).expanduser().resolve().parent
    from gateway.runtime_ownership import process_ownership

    import agent.session_persistence_proof as proof_module

    original_publish = proof_module.publish_gateway_writer_proof

    def observe_publish(agent, messages, proof, turn_id):
        handoff = original_publish(agent, messages, proof, turn_id)
        assert isinstance(handoff, GatewayWriterProofHandoff)
        assert type(handoff.writer_proof) is SessionDBWriteProof
        binding = current_turn_binding()
        assert binding is handoff.binding
        witness = handoff.physical_tip_witness
        assert type(witness) is PhysicalTipWitness
        assert witness.physical_tip == child
        assert witness.logical_root == ref.session_id
        assert witness.lineage == (ref.session_id, child)
        assert witness.physical_owner_epoch == original_owner_epoch
        assert handoff.writer_proof.session_id == child
        assert handoff.writer_proof.row_sessions
        assert all(session_id == child for session_id in handoff.writer_proof.row_sessions)
        assert resolve_physical_tip_witness(binding, child, store_path) == witness
        observed["handoff"] = handoff
        observed["binding"] = binding
        return handoff

    monkeypatch.setattr(proof_module, "publish_gateway_writer_proof", observe_publish)

    try:
        ref, row, child, home, store_path, original_owner_epoch = await _prepare_compressed_api_admission(
            api, owner, session_id="compression-root", request_id="tip-turn",
            child="compression-root-child",
        )
        response = await execute_admission(owner, ref, row)
        assert response == "runtime-custody-fixture-ok"

        handoff = observed["handoff"]
        assert resolve_physical_tip_witness(
            observed["binding"], child, store_path,
        ) == handoff.physical_tip_witness
        placeholders = ", ".join("?" for _ in handoff.writer_proof.row_ids)
        rows = owner.db._read_all(
            f"SELECT id, session_id FROM messages WHERE id IN ({placeholders}) ORDER BY id",
            handoff.writer_proof.row_ids,
        )
        assert tuple(row["id"] for row in rows) == handoff.writer_proof.row_ids
        assert tuple(row["session_id"] for row in rows) == handoff.writer_proof.row_sessions
    finally:
        from gateway.run_runtime import _retire_profile_authority

        retired = await _retire_profile_authority(owner, timeout=5)
        assert retired is True
        if process_ownership.owns(home):
            assert process_ownership.release(home) is True


@pytest.mark.asyncio
async def test_real_openai_api_writer_refuses_reacquired_owner_and_retains_unknown(
    runtime_owned_api_owner, monkeypatch,
):
    api, owner = runtime_owned_api_owner
    observed = {}
    foreign_owner = None
    home = Path(owner.db.db_path).expanduser().resolve().parent
    from gateway.runtime_ownership import ProfileOwnership, process_ownership

    import agent.session_persistence_proof as proof_module

    original_publish = proof_module.publish_gateway_writer_proof

    def refuse_publish(agent, messages, proof, turn_id):
        nonlocal foreign_owner
        binding = current_turn_binding()
        assert resolve_physical_tip_witness(binding, ref.session_id, store_path) is None
        assert resolve_physical_tip_witness(binding, "missing-physical-tip", store_path) is None
        assert process_ownership.release(home) is True
        foreign_owner = ProfileOwnership()
        foreign_owner.reserve([home])
        assert foreign_owner.owns(home)
        assert resolve_physical_tip_witness(binding, child, store_path) is None
        assert foreign_owner.release(home) is True
        process_ownership.reserve([home])
        reacquired = process_ownership.reservation_evidence(home)
        assert isinstance(reacquired, dict)
        assert reacquired["owner_epoch"] != original_owner_epoch
        assert resolve_physical_tip_witness(binding, child, store_path) is None
        refused = original_publish(agent, messages, proof, turn_id)
        assert refused is None
        observed["agent"] = agent
        observed["refused"] = refused
        return refused

    monkeypatch.setattr(proof_module, "publish_gateway_writer_proof", refuse_publish)

    try:
        ref, row, child, home, store_path, original_owner_epoch = await _prepare_compressed_api_admission(
            api, owner, session_id="compression-root-negative", request_id="tip-turn-negative",
            child="compression-root-negative-child",
        )
        await execute_admission(owner, ref, row)
        assert observed["refused"] is None
        retained_agent = observed["agent"]
        assert getattr(retained_agent, "_classic_writer_proof_unknown", False) is True
        assert type(getattr(retained_agent, "_managed_shell_writer_proof", None)) is SessionDBWriteProof
    finally:
        from gateway.run_runtime import _retire_profile_authority

        if foreign_owner is not None and foreign_owner.owns(home):
            assert foreign_owner.release(home) is True
        retired = await _retire_profile_authority(owner, timeout=5)
        assert retired is True
        if process_ownership.owns(home):
            assert process_ownership.release(home) is True
