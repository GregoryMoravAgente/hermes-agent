"""Native retry classification retains the real managed producer and profile lock identity."""

from dataclasses import replace

import pytest


@pytest.mark.asyncio
async def test_managed_native_parent_requires_exact_retained_source_and_physical_owner(
    tmp_path, monkeypatch,
):
    from gateway.runtime_service_custody import reserve_service
    from gateway.runtime_service_native_close import _managed_parent_matches
    from gateway.runtime_service_operations import parent_for_managed_service, reserve_physical
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    try:
        binding = fixture.binding
        service = reserve_service(binding, "native-retry-identity", producer_key="provider")
        parent = parent_for_managed_service(binding, service)
        physical = reserve_physical(parent)
        assert _managed_parent_matches(physical) is True

        for foreign in (
            replace(parent, mode="live_profile_discovery"),
            replace(parent, turn_binding=replace(binding)),
            replace(parent, root_handle=replace(service.handle)),
            replace(parent, physical_owner_epoch="foreign-incarnation"),
            replace(parent, finalizer=object()),
            replace(parent, _physical=object()),
            replace(parent, state="terminal"),
        ):
            monkeypatch.setattr(service, "external_parent", foreign)
            assert _managed_parent_matches(physical) is False
        monkeypatch.setattr(service, "external_parent", parent)
        assert _managed_parent_matches(physical) is True

        from gateway.runtime_ownership import process_ownership
        old_epoch = process_ownership.owner_epoch(parent.profile_home)
        process_ownership.release(parent.profile_home)
        process_ownership.reserve([parent.profile_home])
        assert process_ownership.owner_epoch(parent.profile_home) != old_epoch
        assert _managed_parent_matches(physical) is False
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_actual_managed_codex_reservation_retains_the_same_physical_service_view(
    tmp_path, monkeypatch,
):
    from agent.transports.codex_app_server_session import CodexAppServerSession
    from gateway.runtime_service_native_close import _managed_parent_matches
    from tests.agent.transports.test_codex_physical_custody import (
        _RealProofAgent,
        _codex_launcher,
        _complete_real_admission,
    )
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    session = None
    try:
        writer = _RealProofAgent(fixture.authority.db, "pending-session")
        session = CodexAppServerSession(
            cwd=str(tmp_path), codex_bin=str(_codex_launcher(tmp_path)),
            codex_home=str(tmp_path / "codex-home"), require_custody=True,
        )
        _request, physical = await _complete_real_admission(
            session, fixture, writer, "native-retry-real-managed-client",
        )
        service = session._custody.service
        parent = service.external_parent
        assert service.physical is physical
        assert parent is session._producer_policy.parent
        assert parent._physical is None
        assert parent.finalizer is service
        assert _managed_parent_matches(physical) is True
        monkeypatch.setattr(parent, "finalizer", object())
        assert _managed_parent_matches(physical) is False
        monkeypatch.setattr(parent, "finalizer", service)
        assert _managed_parent_matches(physical) is True
    finally:
        if session is not None:
            session.close()
        fixture.cleanup()
