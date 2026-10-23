"""Causal I03 API custody authority and terminal-receipt controls.

These tests exercise the real runtime-custody coordinator and the real idempotency store.  A
transport map, copied parent, or in-memory terminal projection cannot satisfy the assertions.
"""

from types import SimpleNamespace

from gateway.platforms.api_server_live_bot import _status_projection
from gateway.platforms.api_server_run_custody import ApiRunCustody
from gateway.platforms.api_server_run_recovery import publish_unknown_status
from gateway.runtime_custody import CustodyRefusal
from tests.gateway.test_api_server_custody_fixtures import make_adapter, use_idempotency_db


def _store_receipt(adapter, run_id, tmp_path):
    use_idempotency_db(adapter, tmp_path / f"{run_id}.db")
    store = adapter._run_idempotency_store
    outcome, _ = store.reserve(
        "scope", run_id, "fingerprint", run_id, {"status": "queued"}, owner_pid=0,
        owner_started=0)
    assert outcome == "created"
    return store.update_status_receipt(
        run_id, {"status": "completed"}, expected_scope="scope",
        expected_idempotency_key=run_id, expected_fingerprint="fingerprint",
        expected_owner_pid=0, expected_owner_started=0)


def test_reserve_uses_the_active_authority_when_descriptor_omits_profile():
    adapter = make_adapter()
    authority = SimpleNamespace(profile_id="/tmp/profile-a", instance_id="test-api-server", epoch=1)
    adapter.gateway_runner.session_authority = authority
    adapter.gateway_runner.session_runtime_descriptor = {
        "instance_id": "test-api-server", "authority_epoch": 1,
    }
    custody, refusal = ApiRunCustody.reserve(adapter, "run-authority-profile", None)
    assert refusal is None and custody is not None
    assert custody.profile_id == authority.profile_id


def test_reserve_refuses_a_profile_outside_the_active_authority():
    adapter = make_adapter()
    adapter.gateway_runner.session_authority = SimpleNamespace(
        profile_id="/tmp/profile-a", instance_id="test-api-server", epoch=1)
    refusal = ApiRunCustody.reserve(adapter, "run-foreign-profile", "/tmp/profile-b")[1]
    assert isinstance(refusal, CustodyRefusal)
    assert refusal.code == "profile_identity_mismatch"


def test_final_delivery_receipts_retry_the_guarded_terminal_release(tmp_path):
    adapter = make_adapter()
    custody, refusal = ApiRunCustody.reserve(adapter, "run-receipts", "default")
    assert refusal is None and custody is not None
    parent = adapter.gateway_runner.runtime_custody.reserve_root(
        "I02_api_session_turn", "admission-receipts", profile_id="default", authority_epoch=1)
    assert custody.bind_parent(parent, "I02_api_session_turn") is None
    receipt = _store_receipt(adapter, "run-receipts", tmp_path)
    assert custody.bind_idempotency_identity("scope", "run-receipts", "fingerprint", 0, 0) is None
    assert custody.mark_status_receipt(receipt, require_store=True) is None
    assert custody.mark_worker_submitted() is None
    assert isinstance(custody.mark_worker_done(), CustodyRefusal)
    assert isinstance(custody.mark_control_finalizers(), CustodyRefusal)
    assert isinstance(custody.mark_event_enqueued(True), CustodyRefusal)
    assert custody.mark_stream_closed(True) is None
    assert custody.released is True


def test_unknown_recovery_retains_marker_until_original_handle_can_terminalize():
    adapter = make_adapter()
    custody, refusal = ApiRunCustody.reserve(adapter, "run-recovery", "default")
    assert refusal is None and custody is not None
    parent = adapter.gateway_runner.runtime_custody.reserve_root(
        "I02_api_session_turn", "admission-recovery", profile_id="default", authority_epoch=1)
    custody.admission_id = "admission-recovery"
    custody.execution_generation = 3
    assert custody.mark_admission_started() is None
    assert custody.bind_parent(parent, "I02_api_session_turn") is None
    custody.mark_unknown("terminal_event_unpublished")
    receipt = {
        "run_id": "run-recovery", "profile_id": "default", "authority_epoch": 1,
        "process_instance": "test-api-server", "close_generation": custody.handle.close_generation,
        "execution_generation": 3, "owner_id": custody.handle.owner_id,
        "process_receipt": {
            "kind": "parent_handle", "process_instance": "test-api-server",
            "authority_epoch": 1, "close_generation": custody.handle.close_generation,
            "owner_id": custody.handle.owner_id, "task_id": None,
            "observed": True, "cross_process_proof": False,
        },
        "status_receipt": {
            "committed": True, "storage": "in_memory_projection", "rowcount": 1,
            "run_id": "run-recovery",
        },
        "worker_done": True, "transport_retired": True, "event_enqueued": True,
        "stream_closed": True, "control_finalizers": True,
    }
    first = custody.reconcile_unknown_receipt(receipt)
    assert isinstance(first, CustodyRefusal)
    assert first.code == "custody_pending"
    assert custody.unknown_reason == "terminal_event_unpublished"
    assert custody.released is False
    assert custody.mark_admission_closed() is None
    assert custody.reconcile_unknown_receipt(receipt) is None
    assert custody.unknown_reason is None
    assert custody.released is True


def test_cross_process_receipt_requires_a_real_task_identity():
    adapter = make_adapter()
    custody, refusal = ApiRunCustody.reserve(adapter, "run-process", "default")
    assert refusal is None and custody is not None
    custody.execution_generation = 2
    custody.mark_unknown("cross_process_unproven")
    receipt = {
        "run_id": "run-process", "profile_id": "default", "authority_epoch": 1,
        "process_instance": "test-api-server", "close_generation": custody.handle.close_generation,
        "execution_generation": 2, "owner_id": custody.handle.owner_id,
        "process_receipt": {
            "kind": "I15_process_registry_terminal", "process_instance": "test-api-server",
            "authority_epoch": 1, "close_generation": custody.handle.close_generation,
            "owner_id": "test-api-server:I15_process_registry_terminal", "task_id": None,
            "observed": True, "cross_process_proof": True,
        },
        "status_receipt": {
            "committed": True, "storage": "in_memory_projection", "rowcount": 1,
            "run_id": "run-process",
        },
        "worker_done": True, "transport_retired": True, "event_enqueued": True,
        "stream_closed": True, "control_finalizers": True,
    }
    assert isinstance(custody.reconcile_unknown_receipt(receipt), CustodyRefusal)
    assert custody.unknown_reason == "cross_process_unproven"
    assert custody.released is False


def test_public_status_stays_unknown_when_terminal_commit_is_unproven():
    adapter = make_adapter()
    custody, refusal = ApiRunCustody.reserve(adapter, "run-public-unknown", "default")
    assert refusal is None and custody is not None
    adapter._run_custody["run-public-unknown"] = custody
    custody.mark_unknown("status_commit_unproven")
    fields = publish_unknown_status(adapter, "run-public-unknown", custody, "status_commit_unproven")
    assert fields["last_event"] == "run.unknown"
    assert adapter._run_statuses["run-public-unknown"]["status"] == "unknown"
    assert isinstance(_status_projection(custody), CustodyRefusal)
    assert custody.released is False


def test_peer_binding_refuses_without_the_authoritative_durable_row():
    adapter = make_adapter()
    custody, refusal = ApiRunCustody.reserve(adapter, "run-peer-missing-row", "default")
    assert refusal is None and custody is not None
    authority = SimpleNamespace(
        _preservation_handles={}, _preservation_children={}, profile_id="default", epoch=1, db=None)
    record = {
        "admission_id": "admission-peer", "execution_generation": 1,
        "profile_id": "default", "authority_epoch": 1, "session_id": "session-peer",
    }
    refusal = custody.bind_peer_record(record, authority)
    assert isinstance(refusal, CustodyRefusal)
    assert refusal.code == "peer_durable_row_missing"
