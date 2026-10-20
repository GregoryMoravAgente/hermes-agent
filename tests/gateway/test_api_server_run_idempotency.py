"""Durable receipt and reservation exception proofs for API run custody."""

from unittest.mock import patch

import pytest
from aiohttp.test_utils import TestClient, TestServer

from gateway.platforms.api_server_run_custody import ApiRunCustody
from gateway.platforms.api_server_run_idempotency import RunIdempotencyStore
from tests.gateway.test_api_server_custody_fixtures import (
    create_runs_app, make_adapter, use_idempotency_db,
)


def test_status_update_returns_exact_durable_row_receipt(tmp_path):
    store = RunIdempotencyStore(str(tmp_path / "status-receipt.db"))
    try:
        assert store.update_status("missing-run", {"status": "failed"}) is None
        assert store.update_status_receipt("missing-run", {"status": "failed"}) is None
        outcome, _ = store.reserve("scope", "key", "fingerprint", "run-1", {"status": "queued"})
        assert outcome == "created"
        receipt = store.update_status_receipt(
            "run-1", {"status": "completed"}, expected_scope="scope",
            expected_idempotency_key="key", expected_fingerprint="fingerprint",
            expected_owner_pid=0, expected_owner_started=0)
        assert receipt == {
            "committed": True, "storage": "run_idempotency", "rowcount": 1,
            "scope": "scope", "idempotency_key": "key", "fingerprint": "fingerprint",
            "run_id": "run-1", "owner_pid": 0, "owner_started": 0,
            "readback": True, "status": {"status": "completed"},
        }
        assert store.update_status_receipt(
            "run-1", {"status": "failed"}, expected_scope="wrong-scope") is None
        assert store.update_status("run-1", {"status": "failed"}) is None
    finally:
        store.close()


def test_status_store_exception_becomes_sticky_unknown(tmp_path):
    adapter = make_adapter()
    use_idempotency_db(adapter, tmp_path / "status-exception.db")
    run_id = "run-status-exception"
    initial = adapter._set_run_status(run_id, "queued")
    adapter._run_idempotency_store.reserve("scope", "key", "fingerprint", run_id, initial)
    adapter._run_idempotency_ids.add(run_id)
    custody, refusal = ApiRunCustody.reserve(adapter, run_id, "default")
    assert refusal is None and custody is not None
    adapter._run_custody[run_id] = custody
    with patch.object(
        adapter._run_idempotency_store, "update_status_receipt",
        side_effect=RuntimeError("commit failed"),
    ):
        _, receipt = adapter._set_run_status(run_id, "failed", _return_receipt=True)
    assert receipt is None
    assert custody.mark_status_receipt(receipt, require_store=True) is not None
    assert custody.unknown_reason == "status_commit_unproven"


@pytest.mark.asyncio
async def test_reservation_exception_reconciles_missing_row_before_release(tmp_path):
    adapter = make_adapter()
    use_idempotency_db(adapter, tmp_path / "idem.db")
    app = create_runs_app(adapter)
    with patch.object(adapter._run_idempotency_store, "reserve", side_effect=RuntimeError("commit failed")):
        async with TestClient(TestServer(app)) as client:
            response = await client.post(
                "/v1/runs", json={"input": "hello"}, headers={"Idempotency-Key": "commit-race"})
    assert response.status == 503
    assert adapter._run_custody == {}
    assert adapter._run_owners == {}
