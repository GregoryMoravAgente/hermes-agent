"""Direct API run reservations use the live owning profile's durable epoch."""

from types import SimpleNamespace

import pytest

from gateway.platforms.api_server_run_custody import ApiRunCustody
from gateway.runtime_custody import CustodyRefusal
from gateway.session_authorities import owner_scope
from tests.gateway.test_runtime_custody_profile_epochs import _boot, _close_boot


@pytest.mark.asyncio
async def test_direct_api_reservation_uses_secondary_durable_epoch(tmp_path, monkeypatch):
    runner, entries = await _boot(tmp_path, monkeypatch)
    try:
        launch = runner.session_authorities.launch
        secondary = runner.session_authorities.for_home(entries[1][1])
        assert secondary.epoch != launch.epoch
        adapter = SimpleNamespace(gateway_runner=runner)
        with owner_scope(secondary):
            custody, refusal = ApiRunCustody.reserve(
                adapter, "secondary-api-reservation", secondary.profile_id,
            )
        assert refusal is None
        assert custody is not None
        assert custody.authority_epoch == secondary.epoch
        assert custody.handle.authority_epoch == secondary.epoch
        assert custody.handle.physical_owner_epoch
        assert runner.runtime_custody.release_unaccepted(custody.handle) is None
    finally:
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_direct_api_reservation_refuses_stale_foreign_and_released_profile(
    tmp_path, monkeypatch,
):
    from gateway.runtime_ownership import process_ownership

    runner, entries = await _boot(tmp_path, monkeypatch)
    try:
        launch = runner.session_authorities.launch
        secondary = runner.session_authorities.for_home(entries[1][1])
        adapter = SimpleNamespace(gateway_runner=runner)
        with owner_scope(secondary):
            custody, foreign = ApiRunCustody.reserve(
                adapter, "foreign-api-reservation", launch.profile_id,
            )
            assert custody is None
            assert isinstance(foreign, CustodyRefusal)
            assert foreign.code == "profile_identity_mismatch"
            stale_authority = SimpleNamespace(
                profile_id=secondary.profile_id,
                instance_id=secondary.instance_id,
                epoch=launch.epoch,
            )
            # A late callback may still carry an older authority view. The real
            # coordinator continues to read the current registered authority.
            with monkeypatch.context() as stale_patch:
                stale_patch.setattr(
                    "gateway.session_authorities.active_authority", lambda _runner: stale_authority,
                )
                custody, stale = ApiRunCustody.reserve(
                    adapter, "stale-api-reservation", secondary.profile_id,
                )
                assert custody is None
                assert isinstance(stale, CustodyRefusal)
                assert stale.code == "stale_authority_epoch"
            assert process_ownership.release(secondary.profile_id) is True
            custody, released = ApiRunCustody.reserve(
                adapter, "released-api-reservation", secondary.profile_id,
            )
            assert custody is None
            assert isinstance(released, CustodyRefusal)
            assert released.code == "foreign_custody"
            process_ownership.reserve([secondary.profile_id])
    finally:
        assert await _close_boot(runner, entries) is True
