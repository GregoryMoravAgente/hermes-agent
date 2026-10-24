"""Source-owned runtime-custody scope transitions."""

from types import SimpleNamespace

import pytest


def _runtime_with_detached_authority(home):
    from gateway.config import GatewayConfig
    from gateway.session import SessionStore
    from gateway.session_authority import SessionAuthority
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch

    db = SessionDB(home / "state.db")
    store = SessionStore(home / "sessions", GatewayConfig())
    epoch = begin_runtime_epoch(db, instance_id="first")
    runner = SimpleNamespace(
        _draining=False,
        session_store=store,
        _session_db=db,
        adapters={},
        session_runtime_descriptor={"instance_id": "first", "authority_epoch": epoch},
    )
    authority = SessionAuthority(
        runner, profile_id=str(home), instance_id="first", db=db, epoch=epoch,
    )
    return db, runner, authority


def test_empty_unscoped_coordinator_binds_one_live_profile_scope(
    reserved_gateway_profile_home,
):
    """An empty coordinator may capture the exact direct authority without changing epoch."""
    from gateway.runtime_custody import install_runtime_custody

    db, runner, authority = _runtime_with_detached_authority(reserved_gateway_profile_home)
    try:
        unscoped = install_runtime_custody(runner)
        assert unscoped._profile_scope is None
        runner.session_authority = authority
        scoped = install_runtime_custody(runner)
        assert scoped is unscoped
        assert scoped._profile_scope is not None
        assert scoped.authority_epoch == authority.epoch
    finally:
        db.close()


def test_active_unscoped_coordinator_refuses_late_profile_scope(
    reserved_gateway_profile_home,
):
    """A late direct authority cannot retrofit a coordinator that already owns a lease."""
    from gateway.runtime_custody import install_runtime_custody
    from hermes_state_runtime import RuntimeStoreError

    db, runner, authority = _runtime_with_detached_authority(reserved_gateway_profile_home)
    try:
        unscoped = install_runtime_custody(runner)
        handle = unscoped.reserve_root(
            "I02_api_session_turn",
            "scope-transition-test",
            profile_id=str(reserved_gateway_profile_home),
            authority_epoch=authority.epoch,
        )
        assert getattr(handle, "owner_class", None) == "I02_api_session_turn"
        assert unscoped.is_active_handle(
            handle,
            owner_class="I02_api_session_turn",
            profile_id=str(reserved_gateway_profile_home),
            authority_epoch=authority.epoch,
        )
        runner.session_authority = authority
        with pytest.raises(RuntimeStoreError, match="runtime_custody_binding_collision"):
            install_runtime_custody(runner)
        assert runner.runtime_custody is unscoped
        assert unscoped._profile_scope is None
        assert unscoped.is_active_handle(handle)
        assert unscoped.mark_terminal(handle) is None
        assert not unscoped.is_active_handle(handle)
        assert unscoped.is_terminal_handle(
            handle,
            owner_class="I02_api_session_turn",
            profile_id=str(reserved_gateway_profile_home),
            authority_epoch=authority.epoch,
        )
    finally:
        db.close()
