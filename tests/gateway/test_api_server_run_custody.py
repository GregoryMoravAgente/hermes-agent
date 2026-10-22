"""Causal I03 API-run custody controls.

The tests intentionally use the real RuntimeCustodyCoordinator lease indexes.  A copied handle,
empty transport map, task completion, or a durable row without the same parent never proves release.
"""
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch

from gateway.platforms.api_server_run_custody import ApiRunCustody
from gateway.runtime_custody import CustodyRefusal, RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence


def _coordinator():
    return RuntimeCustodyCoordinator(
        process_instance="i03-test-process", authority_epoch=4, fence=RetirementFence())


def _adapter(coordinator):
    runner = SimpleNamespace(
        runtime_custody=coordinator,
        session_runtime_descriptor={
            "instance_id": "i03-test-process", "authority_epoch": 4,
            "served_profiles": ["profile-a"],
        },
    )
    return SimpleNamespace(gateway_runner=runner)


def _ready(custody):
    custody.status_persisted = True
    custody.event_enqueued = True
    custody.stream_closed = True
    custody.worker_done = True
    custody.control_finalizers = True
    custody.process_proof = True


def test_run_is_bound_to_exact_i02_parent_before_terminalization():
    coordinator = _coordinator()
    parent = coordinator.reserve_root(
        "I02_api_session_turn", "admission-1", profile_id="profile-a", authority_epoch=4)
    child, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-1", "profile-a")
    assert refusal is None and child is not None
    assert child.bind_parent(parent, "I02_api_session_turn") is None
    assert isinstance(coordinator.mark_terminal(parent), CustodyRefusal)
    _ready(child)
    assert child.try_terminal() is None
    assert coordinator.mark_terminal(parent) is None


def test_copied_parent_and_foreign_profile_cannot_bind_or_release():
    coordinator = _coordinator()
    parent = coordinator.reserve_root(
        "I04_fifo_turn_execution", "fifo-1", profile_id="profile-a", authority_epoch=4)
    child, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-2", "profile-a")
    assert refusal is None
    assert isinstance(child.bind_parent(replace(parent, owner_id="copied"), "I04_fifo_turn_execution"), CustodyRefusal)
    assert child.bind_parent(parent, "I04_fifo_turn_execution") is None
    foreign = replace(parent, profile_id="profile-b")
    assert isinstance(child.coordinator.bind_parent(child.handle, foreign), CustodyRefusal)


def test_peer_binding_refuses_without_authoritative_durable_row():
    coordinator = _coordinator()
    parent = coordinator.reserve_root(
        "I02_api_session_turn", "admission-peer", profile_id="profile-a", authority_epoch=4)
    active_authority = SimpleNamespace(
        _preservation_handles={"admission-peer": parent}, _preservation_children={},
        profile_id="profile-a", epoch=4)
    child, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-peer", "profile-a")
    assert refusal is None
    peer = {
        "admission_id": "admission-peer", "execution_generation": 0,
        "profile_id": "profile-a", "authority_epoch": 4, "session_id": "session-peer",
    }
    refusal = child.bind_peer_record(peer, active_authority)
    assert isinstance(refusal, CustodyRefusal)
    assert refusal.code == "peer_durable_row_missing"
    foreign_authority = SimpleNamespace(
        _preservation_handles={}, _preservation_children={}, profile_id="profile-a", epoch=4)
    other, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-peer-foreign", "profile-a")
    assert refusal is None
    assert isinstance(
        other.bind_peer_record(peer, foreign_authority), CustodyRefusal)


def test_missing_process_evidence_retains_direct_run_unknown():
    coordinator = _coordinator()
    child, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-3", "profile-a")
    assert refusal is None
    _ready(child)
    child.process_proof = False
    child.process_task_id = "turn-3"
    child.mark_unknown("cross_process_unproven")
    assert isinstance(child.try_terminal(), CustodyRefusal)
    assert child.released is False
    assert coordinator.status()["active_owner_classes"] == ["I03_api_run_direct"]


def test_unaccepted_idempotency_race_can_release_only_before_parent_or_status():
    coordinator = _coordinator()
    child, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-4", "profile-a")
    assert refusal is None
    assert child.release_unaccepted() is None
    assert child.released is False  # release_unaccepted is coordinator-only; no terminal claim
    assert isinstance(child.try_terminal(), CustodyRefusal)


def test_run_stream_sentinel_marks_custody_closed_after_transport_retirement():
    from gateway.platforms.api_server_runs import _RunLaunch, _RunStream

    class _Custody:
        stream_closed = False
        attempts = 0

        def mark_stream_closed(self, closed):
            self.stream_closed = closed
            self.try_terminal()

        def try_terminal(self):
            self.attempts += 1

    stream, custody = _RunStream(), _Custody()
    owner = SimpleNamespace(_run_streams={"run-1": stream})
    launch = _RunLaunch(owner, "run-1", stream, "session-1", None, False, "hello", [], False, {},
                        "profile-a", None, None, custody=custody)
    launch.put_event(None)
    assert stream.terminal is True and custody.stream_closed is True and custody.attempts == 1


def test_disconnect_keeps_run_store_when_unknown_custody_exists():
    from gateway.platforms import api_server_runs as api_runs
    custody = SimpleNamespace(released=False, unknown_reason="cross_process_unproven")
    adapter = SimpleNamespace(_run_custody={"run-unknown": custody})
    assert api_runs._active_run_custody_count(adapter) == 1


def test_fast_worker_completion_does_not_race_the_submission_barrier():
    coordinator = _coordinator()
    child, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-fast", "profile-a")
    assert refusal is None
    child.mark_worker_done()
    assert child.mark_worker_submitted() is None
    assert child.worker_submitted is True
    assert child.unknown_reason is None


def test_transport_retirement_does_not_claim_worker_completion():
    coordinator = _coordinator()
    child, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-retired", "profile-a")
    assert refusal is None
    child.status_persisted = True
    child.control_finalizers = True
    assert isinstance(child.mark_transport_retired(), CustodyRefusal)
    assert child.transport_retired is True
    assert child.worker_done is False
    assert child.released is False


def test_failed_durable_status_receipt_is_sticky_unknown():
    coordinator = _coordinator()
    child, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-status", "profile-a")
    assert refusal is None
    assert isinstance(child.mark_status_persisted(False), CustodyRefusal)
    assert child.unknown_reason == "status_commit_unproven"
    assert child.released is False


def test_keyed_status_receipt_requires_store_identity_and_rowcount(tmp_path):
    from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore

    coordinator = _coordinator()
    store = RunIdempotencyStore(str(tmp_path / "status-receipt.db"))
    try:
        outcome, _ = store.reserve(
            "scope", "key", "fingerprint", "run-status-receipt", {"status": "queued"},
            owner_pid=0, owner_started=0)
        assert outcome == "created"
        child, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-status-receipt", "profile-a")
        assert refusal is None
        assert child.bind_idempotency_identity("scope", "key", "fingerprint", 0, 0) is None
        exact = store.update_status_receipt(
            "run-status-receipt", {"status": "completed"}, expected_scope="scope",
            expected_idempotency_key="key", expected_fingerprint="fingerprint",
            expected_owner_pid=0, expected_owner_started=0)
        assert exact is not None and exact["readback"] is True
        assert child.mark_status_receipt(exact, require_store=True) is None
        assert child.status_persisted is True

        other, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-status-mismatch", "profile-a")
        assert refusal is None
        wrong_storage = dict(exact, run_id="run-status-mismatch", storage="in_memory_projection")
        assert isinstance(other.mark_status_receipt(wrong_storage, require_store=True), CustodyRefusal)
        assert other.unknown_reason == "status_commit_unproven"
    finally:
        store.close()


def test_peer_record_generation_and_authority_are_exactly_bound(api, owner):
    from hermes_state_runtime import admit_session_input, claim_session_input

    from gateway.runtime_custody import RuntimeCustodyCoordinator

    owner.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance="first", authority_epoch=owner.epoch, fence=RetirementFence())
    owner.runner.session_runtime_descriptor = {
        "instance_id": "first", "authority_epoch": owner.epoch,
        "served_profiles": [owner.profile_id],
    }
    session_id = "peer-session"
    owner.db.create_session(session_id, "api_server")
    queued = admit_session_input(
        owner.db, epoch=owner.epoch, principal_id="api", session_id=session_id,
        request_id="peer-request", payload={"text": "hello"})
    claimed = claim_session_input(
        owner.db, epoch=owner.epoch, session_id=session_id, head=queued["admission_id"])
    assert isinstance(claimed, dict)
    parent = owner._preservation_root("I02_api_session_turn", claimed["admission_id"])
    owner._preservation_pending_root(
        claimed["request_id"], parent, principal_id="api", session_id=session_id)
    owner._preservation_bind_or_release(claimed, parent)
    child, refusal = ApiRunCustody.reserve(api, "run-generation", owner.profile_id)
    assert refusal is None
    peer = {
        "admission_id": claimed["admission_id"], "execution_generation": claimed["generation"],
        "profile_id": owner.profile_id, "authority_epoch": owner.epoch, "session_id": session_id,
    }
    assert child.bind_peer_record(peer, owner) is None
    assert child.execution_generation == claimed["generation"]
    assert isinstance(child.bind_peer_record({**peer, "execution_generation": 9}, owner), CustodyRefusal)
    _ready(child)
    assert child.try_terminal() is None
    owner._preservation_handles.pop(claimed["admission_id"], None)
    assert owner.runner.runtime_custody.mark_terminal(parent) is None


def test_peer_record_generation_is_checked_against_the_durable_admission():
    coordinator = _coordinator()
    parent = coordinator.reserve_root(
        "I02_api_session_turn", "admission-durable-generation", profile_id="profile-a", authority_epoch=4)
    authority = SimpleNamespace(
        _preservation_handles={"admission-durable-generation": parent}, _preservation_children={},
        profile_id="profile-a", epoch=4, db=object())
    child, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-durable-generation", "profile-a")
    assert refusal is None
    peer = {
        "admission_id": "admission-durable-generation", "execution_generation": 8,
        "profile_id": "profile-a", "authority_epoch": 4, "session_id": "session-peer",
    }
    with patch("hermes_state_runtime.get_session_admission", return_value={
        "admission_id": "admission-durable-generation", "target_session_id": "session-peer",
        "generation": 7, "owner_epoch": 4, "principal_id": "api",
    }):
        refusal = child.bind_peer_record(peer, authority)
    assert isinstance(refusal, CustodyRefusal)
    assert refusal.code == "peer_generation_mismatch"


def test_admission_cancellation_waits_for_late_shielded_commit(tmp_path):
    from hermes_state import SessionDB
    from hermes_state_runtime import admit_session_input, begin_runtime_epoch

    db = SessionDB(tmp_path / "state.db")
    try:
        db.create_session("session-late", "api_server")
        epoch = begin_runtime_epoch(db, instance_id="i03-test-process")
        coordinator = RuntimeCustodyCoordinator(
            process_instance="i03-test-process", authority_epoch=epoch, fence=RetirementFence())
        runner = SimpleNamespace(
            runtime_custody=coordinator,
            session_runtime_descriptor={
                "instance_id": "i03-test-process", "authority_epoch": epoch,
                "served_profiles": ["profile-a"],
            },
        )
        adapter = SimpleNamespace(gateway_runner=runner)
        queued = admit_session_input(
            db, epoch=epoch, principal_id="api", session_id="session-late",
            request_id="request-late", payload={"text": "late"})
        parent = coordinator.reserve_root(
            "I02_api_session_turn", queued["admission_id"],
            profile_id="profile-a", authority_epoch=epoch)
        authority = SimpleNamespace(
            _preservation_handles={queued["admission_id"]: parent}, _preservation_children={},
            profile_id="profile-a", epoch=epoch, db=db)
        child, refusal = ApiRunCustody.reserve(adapter, "run-late", "profile-a")
        assert refusal is None
        assert child.mark_admission_started() is None
        assert child.request_no_worker() is None
        admission = (
            authority, SimpleNamespace(profile_id="profile-a", session_id="session-late"), queued)
        refusal = child.bind_admission(admission)
        assert isinstance(refusal, CustodyRefusal)
        assert refusal.code == "late_worker_submission"
        assert child.unknown_reason == "late_worker_submission"
    finally:
        db.close()


def test_unknown_recovery_requires_the_exact_execution_generation():
    coordinator = _coordinator()
    parent = coordinator.reserve_root(
        "I02_api_session_turn", "admission-recovery", profile_id="profile-a", authority_epoch=4)
    child, refusal = ApiRunCustody.reserve(_adapter(coordinator), "run-recovery", "profile-a")
    assert refusal is None
    assert child.bind_parent(parent, "I02_api_session_turn") is None
    child.execution_generation = 5
    child.mark_unknown("status_commit_unproven")
    exact = {
        "run_id": "run-recovery", "profile_id": "profile-a", "authority_epoch": 4,
        "process_instance": "i03-test-process", "close_generation": child.handle.close_generation,
        "execution_generation": 5, "owner_id": child.handle.owner_id,
        "process_receipt": {
            "kind": "parent_handle", "process_instance": "i03-test-process",
            "authority_epoch": 4, "close_generation": child.handle.close_generation,
            "owner_id": child.handle.owner_id, "task_id": None,
            "observed": True, "cross_process_proof": False,
        },
        "status_receipt": {
            "committed": True, "storage": "in_memory_projection", "rowcount": 1,
            "run_id": "run-recovery",
        }, "worker_done": True, "transport_retired": True,
        "event_enqueued": True, "stream_closed": True, "control_finalizers": True,
    }
    assert child.reconcile_unknown_receipt({k: v for k, v in exact.items() if k != "execution_generation"})
    assert child.unknown_reason == "status_commit_unproven"
    assert child.reconcile_unknown_receipt(exact) is None
    assert child.released is True
