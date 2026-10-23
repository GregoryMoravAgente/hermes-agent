"""I08 source-binding safety: legacy admission is not native close evidence."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.mark.asyncio
async def test_i08_unbound_legacy_graph_never_seals_native_zero_work(tmp_path):
    from tests.gateway.test_session_follow_profile_config import _owner
    from gateway.session_goal_tick_custody import (
        _i08_zero_work_snapshot,
        begin_i08_source_close,
        owner_ready,
        source_owner_evidence,
    )

    home = tmp_path / "legacy-home"
    home.mkdir()
    store, runner, authority, _owner_connection = _owner(home)
    receipt = None
    try:
        graph = authority._i08_source_graph
        assert graph.physical_owner_epoch is None
        assert graph.native_descriptor is None
        assert graph.physical_binding is None
        receipt = runner.runtime_custody.begin_close(
            owner_id="i08-legacy-close", scope="process", deadline_s=5,
        )
        assert receipt.state == "closing"
        assert await begin_i08_source_close(
            runner, receipt.close_generation, 5,
        ) is True
        assert _i08_zero_work_snapshot(authority, receipt.close_generation) is None
        assert owner_ready(authority, close_generation=receipt.close_generation) is False
        assert source_owner_evidence(
            authority,
            process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
            owner_id="i08-legacy-owner",
        ) is None
    finally:
        if receipt is not None and runner.runtime_custody.state == "closing":
            assert runner.runtime_custody.abort_before_handoff(receipt) is None
        store.close_all_db_handles()


def test_i08_partial_native_witness_stays_unknown_and_refuses_rebind(tmp_path):
    from tests.gateway.test_session_follow_profile_config import _owner
    from gateway.session_goal_tick_custody import (
        has_i08_source_graph,
        owner_ready,
        register_i08_source_graph,
    )
    from hermes_state_runtime import RuntimeStoreError

    home = tmp_path / "partial-home"
    home.mkdir()
    store, runner, authority, _owner_connection = _owner(home)
    graph = authority._i08_source_graph
    try:
        authority._i08_source_graph = replace(graph, physical_owner_epoch="partial-owner")
        assert has_i08_source_graph(runner) is False
        assert owner_ready(authority) is False
        with pytest.raises(RuntimeStoreError, match="i08_source_graph_binding_mismatch"):
            register_i08_source_graph(runner, authority, runner.runtime_custody)
    finally:
        authority._i08_source_graph = graph
        store.close_all_db_handles()


def test_i08_reuse_refuses_replaced_source_registry_authority_and_closed_coordinator(tmp_path):
    from tests.gateway.conftest import bind_authority_runtime_custody
    from tests.gateway.test_session_follow_profile_config import _owner
    from hermes_state_runtime import RuntimeStoreError

    home = tmp_path / "replacement-home"
    home.mkdir()
    store, runner, authority, _owner_connection = _owner(home)
    coordinator = runner.runtime_custody
    graph = authority._i08_source_graph
    try:
        authority._i08_source_graph = None
        with pytest.raises(RuntimeStoreError, match="runtime_custody_binding_collision"):
            bind_authority_runtime_custody(runner, authority)
        assert runner.runtime_custody is coordinator
        authority._i08_source_graph = graph

        runner.session_authorities = [authority]
        with pytest.raises(RuntimeStoreError, match="runtime_custody_binding_collision"):
            bind_authority_runtime_custody(runner, authority)
        del runner.session_authorities

        runner.session_authority = object()
        with pytest.raises(RuntimeStoreError, match="runtime_custody_binding_collision"):
            bind_authority_runtime_custody(runner, authority)
        runner.session_authority = authority

        receipt = coordinator.begin_close(
            owner_id="i08-closed-coordinator", scope="process", deadline_s=5,
        )
        assert receipt.state == "closing"
        with pytest.raises(RuntimeStoreError, match="runtime_custody_binding_closed"):
            bind_authority_runtime_custody(runner, authority)
        assert coordinator.abort_before_handoff(receipt) is None
    finally:
        authority._i08_source_graph = graph
        store.close_all_db_handles()


def test_i08_native_graph_uses_owner_ledger_canonical_case_for_path_identity(
    tmp_path, monkeypatch,
):
    """The graph must use the owner ledger's case-normalized path on Windows.

    This is a path-identity regression control, not native-platform qualification: the native
    reservation is supplied by the owner-ledger boundary, while graph registration and matching
    must preserve that exact canonical spelling instead of comparing ``Path.resolve()`` output.
    """
    from gateway import runtime_ownership
    from gateway.session_goal_tick_custody import register_i08_source_graph

    home = tmp_path / "MixedCaseProfile"
    database = home / "State.DB"
    canonical = Path(str(home).lower())
    canonical_database = Path(str(database).lower())
    owner_epoch = "owner-epoch"
    native_descriptor = 17
    reservation = {
        "profile_home": str(canonical),
        "owner_epoch": owner_epoch,
        "observed": True,
        "native_handle_retained": True,
        "os_lock_owner": True,
        "cross_process_proof": True,
        "native_descriptor": native_descriptor,
        "physical_binding": (str(canonical), owner_epoch, native_descriptor),
    }

    def canonical_home(value):
        return Path(str(value).lower())

    class FakeOwnership:
        def reservation_evidence(self, value):
            assert value == canonical
            return reservation

    monkeypatch.setattr(runtime_ownership, "canonical_home", canonical_home)
    monkeypatch.setattr(runtime_ownership, "process_ownership", FakeOwnership())
    authority = SimpleNamespace(
        profile_id=str(home),
        instance_id="instance",
        epoch=1,
        db=SimpleNamespace(db_path=str(database)),
    )
    coordinator = SimpleNamespace(process_instance="instance")
    runner = SimpleNamespace(runtime_custody=coordinator, _i08_watcher_tasks=set())

    graph = register_i08_source_graph(runner, authority, coordinator)

    assert graph.database_path == str(canonical_database)
    assert graph.physical_binding == reservation["physical_binding"]
    assert graph.matches(runner, authority, coordinator)
