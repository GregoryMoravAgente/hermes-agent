"""Profile-scoped custody consumes the live authority and physical-owner contracts."""

from __future__ import annotations

from dataclasses import replace
import shlex
import sys

import pytest


def _homes(tmp_path, monkeypatch):
    from tests.gateway.test_session_authorities_multiplex import _reserve_homes, _runner

    root, entries = _reserve_homes(tmp_path, monkeypatch, names=("alpha",))
    secondary = entries[1][1]
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch

    # Give the secondary a real prior DB epoch.  Bootstrap then creates two live authorities with
    # different epochs, while the coordinator retains the launch authority's close epoch.
    prior = SessionDB(secondary / "state.db")
    try:
        begin_runtime_epoch(prior, instance_id="prior-profile-boot")
    finally:
        prior.close()
    return root, entries, _runner(root, entries)


async def _boot(tmp_path, monkeypatch):
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership
    from tools import process_registry as process_registry_module
    from tools.process_registry import ProcessRegistry

    _, entries, runner = _homes(tmp_path, monkeypatch)
    registry = ProcessRegistry()
    monkeypatch.setattr(process_registry_module, "process_registry", registry)
    runner._profile_epoch_test_process_registry = registry
    process_ownership.reserve([home for _, home in entries])
    try:
        await initialize_gateway_runtime(runner)
    except BaseException:
        if await _close_boot(runner, entries) is not True:
            raise RuntimeError("profile authority retirement did not settle during fixture setup")
        raise
    return runner, entries


async def _close_boot(runner, entries):
    from gateway.run_runtime import _retire_profile_authority
    from gateway.runtime_ownership import process_ownership

    registry = getattr(runner, "session_authorities", None)
    authorities = []
    seen = set()
    for authority in list(registry or []):
        if authority is None or id(authority) in seen:
            continue
        seen.add(id(authority))
        authorities.append(authority)
    for authority in authorities:
        if await _retire_profile_authority(authority, timeout=5) is not True:
            return False
    if registry is not None:
        for authority in authorities:
            if authority is not registry.launch:
                registry.remove(authority.profile_id)
    # The real runner caches secondary SessionDB handles separately from each authority.  Sweep
    # those stores only after every authority has retired; a failed sweep/close leaves ownership
    # retained because the release loop is below it.
    store = getattr(runner, "session_store", None)
    close_store = getattr(store, "close_all_db_handles", None)
    if callable(close_store):
        close_store()
    close_runner_handles = getattr(runner, "close_all_session_db_handles", None)
    if callable(close_runner_handles):
        close_runner_handles()
    for authority in authorities:
        authority.db.close()
    for _, home in entries:
        if process_ownership.owns(home) and not process_ownership.release(home):
            return False
    return True


@pytest.mark.asyncio
async def test_live_two_profile_epochs_keep_process_close_epoch_and_reject_stale_root(
    tmp_path, monkeypatch,
):
    from gateway.runtime_custody import CustodyRefusal
    from gateway.runtime_ownership import process_ownership

    runner, entries = await _boot(tmp_path, monkeypatch)
    try:
        launch = runner.session_authorities.launch
        secondary = runner.session_authorities.for_home(entries[1][1])
        coordinator = runner.runtime_custody
        assert secondary is not None
        assert secondary.epoch != launch.epoch
        assert coordinator.authority_epoch == launch.epoch

        live = coordinator.reserve_root(
            "I02_api_session_turn", "profile-epoch-live",
            profile_id=secondary.profile_id, authority_epoch=secondary.epoch,
        )
        assert not isinstance(live, CustodyRefusal)
        assert live.authority_epoch == secondary.epoch
        assert isinstance(live.physical_owner_epoch, str)
        stale = coordinator.reserve_root(
            "I02_api_session_turn", "profile-epoch-stale",
            profile_id=secondary.profile_id, authority_epoch=launch.epoch,
        )
        assert isinstance(stale, CustodyRefusal)
        assert stale.code == "stale_authority_epoch"
        process_ownership.release(secondary.profile_id)
        missing = coordinator.mark_terminal(live)
        assert isinstance(missing, CustodyRefusal)
        assert missing.code == "foreign_custody"
        process_ownership.reserve([secondary.profile_id])
        evidence = process_ownership.reservation_evidence(secondary.profile_id)
        assert evidence["owner_epoch"] != live.physical_owner_epoch
        reacquired = coordinator.reconcile_unknown(
            live, owner_id=live.owner_id, profile_id=live.profile_id,
            process_instance=live.process_instance, authority_epoch=live.authority_epoch,
        )
        assert isinstance(reacquired, CustodyRefusal)
        assert reacquired.code == "foreign_custody"
        assert coordinator.status()["active_owner_classes"] == ["I02_api_session_turn"]

        receipt = coordinator.begin_close(owner_id="profile-epoch-close", deadline_s=5)
        assert receipt.authority_epoch == launch.epoch
        assert coordinator.abort_before_handoff(receipt) is None
    finally:
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_real_i16_edges_use_secondary_epoch_and_reject_copied_parent(
    tmp_path, monkeypatch,
):
    from gateway.runtime_custody import CustodyRefusal

    runner, entries = await _boot(tmp_path, monkeypatch)
    try:
        secondary = runner.session_authorities.for_home(entries[1][1])
        coordinator = runner.runtime_custody
        i04 = coordinator.reserve_root(
            "I04_fifo_turn_execution", "profile-i04",
            profile_id=secondary.profile_id, authority_epoch=secondary.epoch,
        )
        service = coordinator.reserve_root(
            "I16_turn_sidecars", "profile-i16-service",
            profile_id=secondary.profile_id, authority_epoch=secondary.epoch,
        )
        assert not isinstance(i04, CustodyRefusal)
        assert not isinstance(service, CustodyRefusal)
        request = coordinator.reserve_root_with_secondary_parent(
            "I16_turn_sidecars", "profile-i16-request",
            profile_id=secondary.profile_id, authority_epoch=secondary.epoch,
            parent=service, secondary_parent=i04,
        )
        assert not isinstance(request, CustodyRefusal)
        assert request.authority_epoch == secondary.epoch
        copied = replace(i04, owner_id="copied-i04")
        refused = coordinator.reserve_root_with_secondary_parent(
            "I16_turn_sidecars", "profile-i16-copied",
            profile_id=secondary.profile_id, authority_epoch=secondary.epoch,
            parent=service, secondary_parent=copied,
        )
        assert isinstance(refused, CustodyRefusal)
        assert refused.code == "secondary_parent_mismatch"
        launch = runner.session_authorities.launch
        foreign_parent = coordinator.reserve_root(
            "I04_fifo_turn_execution", "profile-i04-foreign",
            profile_id=launch.profile_id, authority_epoch=launch.epoch,
        )
        assert not isinstance(foreign_parent, CustodyRefusal)
        refused_foreign = coordinator.reserve_root_with_secondary_parent(
            "I16_turn_sidecars", "profile-i16-foreign-parent",
            profile_id=secondary.profile_id, authority_epoch=secondary.epoch,
            parent=service, secondary_parent=foreign_parent,
        )
        assert isinstance(refused_foreign, CustodyRefusal)
        assert refused_foreign.code == "secondary_parent_mismatch"
        assert coordinator.mark_terminal_with_secondary_parents(request, (i04,)) is None
        assert coordinator.mark_terminal(service) is None
        assert coordinator.mark_terminal(i04) is None
        assert coordinator.mark_terminal(foreign_parent) is None
    finally:
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_unserve_then_canonical_hotserve_retires_old_epoch_and_serves_successor(
    tmp_path, monkeypatch,
):
    from gateway.run_runtime import serve_profile_runtime, unserve_profile_runtime
    from gateway.runtime_custody import CustodyRefusal

    runner, entries = await _boot(tmp_path, monkeypatch)
    old_authority = runner.session_authorities.for_home(entries[1][1])
    coordinator = runner.runtime_custody
    old = coordinator.reserve_root(
        "I02_api_session_turn", "retire-old",
        profile_id=old_authority.profile_id, authority_epoch=old_authority.epoch,
    )
    try:
        assert not isinstance(old, CustodyRefusal)
        assert await unserve_profile_runtime(runner, entries[1][1]) is True
        assert runner.session_authorities.for_home(entries[1][1]) is None
        successor = await serve_profile_runtime(runner, "alpha", entries[1][1])
        assert successor.epoch > old_authority.epoch
        assert runner.session_authorities.for_home(entries[1][1]) is successor
        refused = coordinator.mark_terminal(old)
        assert isinstance(refused, CustodyRefusal)
        assert refused.code == "stale_authority_epoch"
        assert coordinator.status()["active_owner_classes"] == ["I02_api_session_turn"]
        fresh = coordinator.reserve_root(
            "I02_api_session_turn", "retire-new",
            profile_id=successor.profile_id, authority_epoch=successor.epoch,
        )
        assert not isinstance(fresh, CustodyRefusal)
        assert coordinator.mark_terminal(fresh) is None
    finally:
        if coordinator.status()["active_owner_classes"]:
            coordinator.mark_unknown(old, "foreign_custody")
        old_authority.db.close()
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_process_registry_consumes_secondary_profile_epoch_with_real_child(
    tmp_path, monkeypatch,
):
    from gateway.session_authorities import owner_scope
    from tools import process_registry as process_registry_module
    from tests.waiting import wait_until

    runner, entries = await _boot(tmp_path, monkeypatch)
    from tools.process_registry import process_registry
    secondary = runner.session_authorities.for_home(entries[1][1])
    session = None
    try:
        monkeypatch.setattr(process_registry_module, "_SYSTEMD_SCOPE_AVAILABLE", False, raising=False)
        child_code = 'print("profile-child", flush=True)'
        command = f"{shlex.quote(sys.executable)} -c {shlex.quote(child_code)}"
        with owner_scope(secondary):
            session = process_registry.spawn_local(
                command, cwd=str(entries[1][1]), task_id="profile-i15",
                owner_task_id="profile-i15", session_key="profile-i15",
            )
        assert session.custody is not None
        assert session.profile_id == secondary.profile_id
        assert session.authority_epoch == secondary.epoch
        assert session.custody.handle.authority_epoch == secondary.epoch
        assert isinstance(session.custody.handle.physical_owner_epoch, str)
        wait_until(lambda: session._completion_event.is_set(), what="profile-owned child EOF")
    finally:
        if session is not None and session.process is not None and session.process.poll() is None:
            process_registry.kill_process(session.id, source="profile-epoch-test", consume_output=False)
        assert await _close_boot(runner, entries) is True


def test_standalone_maintenance_keeps_fixed_epoch_without_session_authority(tmp_path):
    from gateway.runtime_service_operations import (
        close_after_finalizers, reserve_standalone_external_process_parent,
    )
    from hermes_state import SessionDB

    home = (tmp_path / "standalone").resolve()
    home.mkdir()
    db = SessionDB(home / "state.db")
    try:
        with reserve_standalone_external_process_parent(
            profile_home=home, profile_id=str(home), operation_id="standalone-epoch",
            source_identity="standalone-test", profile_db=db, producer_key="provider",
        ) as parent:
            assert parent.profile_authority is None
            assert parent.coordinator._profile_scope is None
            assert close_after_finalizers(parent) is True
    finally:
        db.close()


@pytest.mark.asyncio
async def test_foreign_registry_cannot_authorize_the_captured_profile_owner(tmp_path, monkeypatch):
    from gateway.runtime_custody import CustodyRefusal
    from gateway.session_authorities import SessionAuthorities

    runner, entries = await _boot(tmp_path, monkeypatch)
    try:
        original = runner.session_authorities
        secondary = original.for_home(entries[1][1])
        coordinator = runner.runtime_custody
        handle = coordinator.reserve_root(
            "I02_api_session_turn", "registry-owner",
            profile_id=secondary.profile_id, authority_epoch=secondary.epoch,
        )
        assert not isinstance(handle, CustodyRefusal)
        foreign = SessionAuthorities(entries[0][1], multiplexed=True)
        foreign.add(entries[0][1], original.launch)
        foreign.add(entries[1][1], secondary, name="alpha")
        with monkeypatch.context() as changed:
            changed.setattr(runner, "session_authorities", foreign)
            refused = coordinator.reserve_root(
                "I02_api_session_turn", "foreign-registry",
                profile_id=secondary.profile_id, authority_epoch=secondary.epoch,
            )
            assert isinstance(refused, CustodyRefusal)
            assert refused.code == "profile_unserved"
            retained = coordinator.mark_terminal(handle)
            assert isinstance(retained, CustodyRefusal)
            assert coordinator.status()["active_owner_classes"] == ["I02_api_session_turn"]
        assert coordinator.reconcile_unknown(
            handle, owner_id=handle.owner_id, profile_id=handle.profile_id,
            process_instance=handle.process_instance, authority_epoch=handle.authority_epoch,
        ) is None
        assert coordinator.mark_terminal(handle) is None
    finally:
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
@pytest.mark.platforms("windows")
async def test_windows_profile_paths_match_native_lock_canonicalization(tmp_path, monkeypatch):
    from gateway.runtime_custody import CustodyRefusal
    from gateway.runtime_ownership import canonical_home, process_ownership

    runner, entries = await _boot(tmp_path, monkeypatch)
    try:
        secondary = runner.session_authorities.for_home(entries[1][1])
        evidence = process_ownership.reservation_evidence(secondary.profile_id)
        assert evidence["profile_home"] == str(canonical_home(secondary.profile_id))
        handle = runner.runtime_custody.reserve_root(
            "I02_api_session_turn", "windows-profile-lock",
            profile_id=secondary.profile_id, authority_epoch=secondary.epoch,
        )
        assert not isinstance(handle, CustodyRefusal)
        assert handle.physical_owner_epoch == evidence["owner_epoch"]
        assert runner.runtime_custody.mark_terminal(handle) is None
    finally:
        assert await _close_boot(runner, entries) is True



def _finish_native_profile_child(registry, session):
    from tests.waiting import wait_until

    wait_until(
        lambda: session.process is not None and session.process.poll() is None,
        what="profile-owned native child start",
    )
    result = registry.kill_process(session.id, source="profile-epoch-mixed-close")
    assert result["status"] == "killing"
    assert result["completion_reason"] == "kill_requested"
    wait_until(lambda: session._completion_event.is_set(), what="profile-owned native EOF")
    delivered = registry.drain_notifications(
        session_key=session.session_key,
        skip_poll_observed=False,
    )
    assert delivered and delivered[0][0]["session_id"] == session.id
    token = delivered[0][0].get("delivery_token", "")
    assert token
    registry.acknowledge_completion_delivery(token)
    assert session.custody_state == "terminal"
    assert session.custody_reader_eof is True
    assert session.custody_durable is True
    assert session.custody_scope_settled is True
    assert session.custody_handle_closed is True
    assert session.custody.streams_closed is True
    assert session.custody.cross_process_proven is True


@pytest.mark.asyncio
@pytest.mark.platforms("windows")
async def test_i15_close_seals_real_terminal_children_from_mixed_profile_epochs(
    tmp_path, monkeypatch,
):
    from gateway.run_runtime import (
        _seal_integrated_preservation_owners, serve_profile_runtime, unserve_profile_runtime,
    )
    from gateway.session_authorities import owner_scope
    from tools.process_registry_custody_evidence import (
        preservation_owner_evidence_for_profiles,
    )

    runner, entries = await _boot(tmp_path, monkeypatch)
    registry = runner._profile_epoch_test_process_registry
    coordinator = runner.runtime_custody
    launch = runner.session_authorities.launch
    secondary = runner.session_authorities.for_home(entries[1][1])
    sessions = []
    try:
        assert secondary is not None and secondary.epoch != launch.epoch
        command = (
            f"{shlex.quote(sys.executable)} -c "
            f"{shlex.quote('import threading; threading.Event().wait()')}"
        )
        with owner_scope(secondary):
            secondary_session = registry.spawn_local(
                command,
                cwd=str(entries[1][1]),
                task_id="mixed-secondary",
                owner_task_id="mixed-secondary",
                session_key="mixed-secondary",
                notify_on_complete=True,
            )
        sessions.append(secondary_session)
        _finish_native_profile_child(registry, secondary_session)
        secondary_registration = next(
            registration for registration in registry._i15_zero_factory.registrations()
            if registration.profile_id == secondary.profile_id
        )
        # The native child is terminal, but the source fence has not crossed its close boundary
        # yet.  Retirement must not consume a terminal receipt from an open admission fence.
        assert secondary_registration.scope.terminal_history_retirement() is False
        secondary_registration.scope.close()
        assert secondary_registration.scope.terminal_history_retirement() is True
        original_profile_id = secondary_session.profile_id
        original_authority_epoch = secondary_session.authority_epoch
        original_coordinator = secondary_session.custody.coordinator
        secondary_session.profile_id = launch.profile_id
        assert secondary_registration.scope.terminal_history_retirement() is False
        secondary_session.profile_id = original_profile_id
        secondary_session.authority_epoch = launch.epoch
        assert secondary_registration.scope.terminal_history_retirement() is False
        secondary_session.authority_epoch = original_authority_epoch
        secondary_session.custody.coordinator = object()
        assert secondary_registration.scope.terminal_history_retirement() is False
        secondary_session.custody.coordinator = original_coordinator
        assert secondary_registration.scope.terminal_history_retirement() is True
        assert await unserve_profile_runtime(runner, entries[1][1]) is True
        successor = await serve_profile_runtime(runner, "alpha", entries[1][1])
        assert successor.epoch > secondary.epoch
        with owner_scope(successor):
            active_session = registry.spawn_local(
                command,
                cwd=str(entries[1][1]),
                task_id="mixed-successor-active",
                owner_task_id="mixed-successor-active",
                session_key="mixed-successor-active",
                notify_on_complete=True,
            )
        assert preservation_owner_evidence_for_profiles(
            registry,
            process_instance=coordinator.process_instance,
            authority_epoch=coordinator.authority_epoch,
            close_generation=1,
            owner_id=f"{coordinator.process_instance}:I15_process_registry_terminal",
            launch_profile_id=registry._process_profile_id(),
        ) is None
        sessions.append(active_session)
        _finish_native_profile_child(registry, active_session)
        with owner_scope(launch):
            launch_session = registry.spawn_local(
                command,
                cwd=str(entries[0][1]),
                task_id="mixed-launch",
                owner_task_id="mixed-launch",
                session_key="mixed-launch",
                notify_on_complete=True,
            )
        sessions.append(launch_session)
        _finish_native_profile_child(registry, launch_session)
        receipt = coordinator.begin_close(owner_id="profile-epoch-mixed-close", deadline_s=5)
        assert receipt.state == "closing"
        assert receipt.authority_epoch == launch.epoch
        evidence = preservation_owner_evidence_for_profiles(
            registry,
            process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            close_generation=receipt.close_generation,
            owner_id=f"{receipt.process_instance}:I15_process_registry_terminal",
            launch_profile_id=registry._process_profile_id(),
        )
        assert evidence is not None
        assert set(evidence["profile_ids"]) == {launch.profile_id, secondary.profile_id}
        profile_evidence = {item["profile_id"]: item for item in evidence["profile_evidence"]}
        assert profile_evidence[secondary.profile_id]["authority_epochs"] == (
            secondary.epoch, successor.epoch,
        )
        assert profile_evidence[secondary.profile_id]["physical_owner_epochs"]
        assert all(item["terminal_session_ids"] for item in profile_evidence.values())
        _seal_integrated_preservation_owners(runner, coordinator, receipt)
        assert coordinator.status()["owner_states"]["I15_process_registry_terminal"] == "sealed"
        assert "I15_process_registry_terminal" not in coordinator.status()["active_owner_classes"]
        assert coordinator.abort_before_handoff(receipt) is None
    finally:
        assert await _close_boot(runner, entries) is True
