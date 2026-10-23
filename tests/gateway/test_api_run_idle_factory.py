"""Actual two-profile API factory/native-owner controls without manufactured work."""
from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from tests.gateway.test_runtime_custody_profile_epochs import _boot, _close_boot
from tests.gateway.test_api_server_run_custody_public import actual_api_run


@asynccontextmanager
async def _factory(tmp_path, monkeypatch):
    from gateway.config import Platform, PlatformConfig
    from gateway.platforms.api_server import APIServerAdapter
    from gateway.preservation_source_close import begin_source_close, abort_source_close

    runner, entries = await _boot(tmp_path, monkeypatch)
    adapter = APIServerAdapter(PlatformConfig(enabled=True))
    receipt = None
    try:
        adapter.gateway_runner = runner
        runner.adapters = {Platform.API_SERVER: adapter}
        runner._wire_adapter_handlers(adapter)
        factory = adapter._api_run_factory
        assert factory.invalid is False
        assert factory.coordinator is runner.runtime_custody
        assert len({item.epoch for item in factory.profiles}) == 2
        assert adapter._api_run_owner_registered is False
        assert adapter._run_custody == adapter._run_launches == {}
        receipt = runner.runtime_custody.begin_close(owner_id="api-idle-factory-control")
        assert begin_source_close(runner, receipt.close_generation) is True
        yield runner, adapter, receipt, entries
    finally:
        if receipt is not None:
            assert abort_source_close(runner, receipt.close_generation) is True
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        adapter._response_store.close()
        adapter._run_idempotency_store.close()
        assert await _close_boot(runner, entries) is True


def _evidence(runner, adapter, receipt):
    from gateway.platforms.api_server_run_custody import preservation_owner_evidence

    return preservation_owner_evidence(
        [adapter], runner, process_instance=receipt.process_instance,
        authority_epoch=receipt.authority_epoch, close_generation=receipt.close_generation,
        owner_id=f"{receipt.process_instance}:I03_api_run_direct",
    )


@pytest.mark.asyncio
async def test_idle_api_factory_reads_real_two_profile_epochs_and_native_owners(tmp_path, monkeypatch):
    async with _factory(tmp_path, monkeypatch) as (runner, adapter, receipt, _entries):
        evidence = _evidence(runner, adapter, receipt)
        assert evidence is not None
        assert evidence["cross_process_proof"] is True
        assert evidence["run_witnesses"] == ()
        profiles = evidence["source_factory_proofs"][0]["profiles"]
        assert len(profiles) == 2
        assert len({row["authority_epoch"] for row in profiles}) == 2
        assert all(row["epoch_readback"] and row["physical_owner_epoch"] for row in profiles)
        from gateway.run_runtime import _seal_integrated_preservation_owners
        _seal_integrated_preservation_owners(runner, runner.runtime_custody, receipt)
        assert runner.runtime_custody.status()["owner_states"]["I03_api_run_direct"] == "sealed"


@pytest.mark.asyncio
@pytest.mark.platforms("windows")
async def test_windows_idle_api_factory_canonicalizes_db_home_alias_and_rejects_replacement(
        tmp_path, monkeypatch,
):
    """Physical DB aliases must not change the logical profile or owner incarnation."""
    from gateway.runtime_ownership import process_ownership

    async with _factory(tmp_path, monkeypatch) as (runner, adapter, receipt, entries):
        factory = adapter._api_run_factory
        logical_ids = tuple(item.home for item in factory.profiles)
        original_paths = tuple(item.db.db_path for item in factory.profiles)
        try:
            for item, original in zip(factory.profiles, original_paths):
                alias_parent = "".join(
                    char.swapcase() if char.isalpha() else char
                    for char in str(Path(original).parent)
                )
                item.db.db_path = Path(alias_parent) / Path(original).name
            assert _evidence(runner, adapter, receipt) is not None
            assert tuple(item.home for item in factory.profiles) == logical_ids
        finally:
            for item, original in zip(factory.profiles, original_paths):
                item.db.db_path = original

        secondary = entries[1][1]
        assert process_ownership.release(secondary) is True
        assert _evidence(runner, adapter, receipt) is None
        process_ownership.reserve([secondary])
        assert _evidence(runner, adapter, receipt) is None


def _replace_producer(changed, runner, adapter):
    changed.setattr("gateway.platforms.api_server_run_execution.finalize_api_run", lambda *a: None)


def _replace_index(changed, runner, adapter):
    changed.setattr(adapter, "_run_custody", {})


def _pending_request(changed, runner, adapter):
    changed.setattr(adapter, "_pending_agent_requests", 1)


def _missing_registration(changed, runner, adapter):
    changed.delitem(adapter._api_run_factory.registry._registrations, id(adapter))


def _failed_db_read(changed, runner, adapter):
    def unreadable():
        raise OSError("synthetic unreadable source DB")
    changed.setattr(runner.session_authorities.launch.db, "_read_ctx", unreadable)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", [
    _replace_producer, _replace_index, _pending_request, _missing_registration, _failed_db_read,
])
async def test_idle_factory_refuses_replaced_unreadable_or_pending_source(tmp_path, monkeypatch, mutation):
    async with _factory(tmp_path, monkeypatch) as (runner, adapter, receipt, _entries):
        assert _evidence(runner, adapter, receipt) is not None
        with monkeypatch.context() as changed:
            mutation(changed, runner, adapter)
            assert _evidence(runner, adapter, receipt) is None
        assert _evidence(runner, adapter, receipt) is not None


@pytest.mark.asyncio
async def test_idle_factory_refuses_current_db_epoch_drift(tmp_path, monkeypatch):
    async with _factory(tmp_path, monkeypatch) as (runner, adapter, receipt, _entries):
        assert _evidence(runner, adapter, receipt) is not None
        authority = runner.session_authorities.launch
        with authority.db._read_ctx() as conn:
            original = tuple(conn.execute(
                "SELECT epoch,instance_id FROM runtime_epoch WHERE singleton=1"
            ).fetchone())
        authority.db._execute_write(lambda conn: conn.execute(
            "UPDATE runtime_epoch SET epoch=epoch+1 WHERE singleton=1"
        ))
        try:
            assert _evidence(runner, adapter, receipt) is None
        finally:
            authority.db._execute_write(lambda conn: conn.execute(
                "UPDATE runtime_epoch SET epoch=?,instance_id=? WHERE singleton=1", original,
            ))
        assert _evidence(runner, adapter, receipt) is not None

@pytest.mark.asyncio
async def test_idle_factory_refuses_reacquired_profile_native_owner(tmp_path, monkeypatch):
    from gateway.runtime_ownership import process_ownership

    async with _factory(tmp_path, monkeypatch) as (runner, adapter, receipt, entries):
        assert _evidence(runner, adapter, receipt) is not None
        secondary = entries[1][1]
        assert process_ownership.release(secondary) is True
        assert _evidence(runner, adapter, receipt) is None
        process_ownership.reserve([secondary])
        assert _evidence(runner, adapter, receipt) is None


@pytest.mark.asyncio
async def test_activity_bit_cannot_replace_missing_source_factory(tmp_path, monkeypatch):
    async with _factory(tmp_path, monkeypatch) as (runner, adapter, receipt, _entries):
        assert _evidence(runner, adapter, receipt) is not None
        with monkeypatch.context() as changed:
            changed.delattr(adapter, "_api_run_factory")
            changed.setattr(adapter, "_api_run_owner_registered", True)
            assert _evidence(runner, adapter, receipt) is None


@pytest.mark.asyncio
async def test_post_close_api_admission_refuses_before_maps_or_rows(tmp_path, monkeypatch):
    from gateway.platforms.api_server_run_custody import reserve_run

    async with _factory(tmp_path, monkeypatch) as (runner, adapter, receipt, _entries):
        custody, refusal = reserve_run(adapter, "post-fence", None)
        assert custody is None
        assert refusal is not None
        assert adapter._run_custody == adapter._run_launches == {}
        assert _evidence(runner, adapter, receipt) is not None


@pytest.mark.asyncio
@pytest.mark.usefixtures("actual_api_run")
async def test_settled_public_api_run_joins_current_factory_close(request):
    runtime = request.getfixturevalue("actual_api_run")
    from gateway.preservation_source_close import begin_source_close, abort_source_close
    from tests.gateway.test_api_server_run_custody_public import (
        test_public_api_positive_consumes_original_launch_witness,
    )

    await test_public_api_positive_consumes_original_launch_witness(runtime)
    runner, adapter = runtime["runner"], runtime["adapter"]
    receipt = runner.runtime_custody.begin_close(owner_id="api-settled-factory")
    try:
        assert begin_source_close(runner, receipt.close_generation) is True
        evidence = _evidence(runner, adapter, receipt)
        assert evidence is not None
        assert len(evidence["run_witnesses"]) == 1
        assert evidence["cross_process_proof"] is True
    finally:
        assert abort_source_close(runner, receipt.close_generation) is True
        assert runner.runtime_custody.abort_before_handoff(receipt) is None


@pytest.mark.asyncio
async def test_secondary_api_original_replay_contract_with_unequal_real_epochs(tmp_path, monkeypatch):
    """Run the original public replay/refusal oracle after a genuine prior secondary boot."""
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch
    from tests.gateway import test_session_authorities_multiplex as multiplex
    from tests.gateway.test_api_finalizer_completion_current import (
        test_secondary_profile_api_replay_uses_real_authority_and_rejects_foreign_registry,
    )

    original = multiplex._reserve_homes
    def prior_secondary_boot(*args, **kwargs):
        root, homes = original(*args, **kwargs)
        db = SessionDB(homes[1][1] / "state.db")
        try:
            begin_runtime_epoch(db, instance_id="genuine-prior-secondary-boot")
        finally:
            db.close()
        return root, homes

    monkeypatch.setattr(multiplex, "_reserve_homes", prior_secondary_boot)
    await test_secondary_profile_api_replay_uses_real_authority_and_rejects_foreign_registry(
        tmp_path, monkeypatch,
    )
