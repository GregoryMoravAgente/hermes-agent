"""Runtime-owned gateway fixtures publish the custody identity required by guarded consumers."""

import pytest


@pytest.mark.asyncio
async def test_runtime_owned_api_uses_bootstrap_identity(tmp_path, monkeypatch):
    from tests.gateway.conftest import runtime_owned_api

    async with runtime_owned_api(tmp_path, monkeypatch) as adapter:
        runner = adapter.gateway_runner
        descriptor = runner.session_runtime_descriptor
        authority = runner.session_authority
        coordinator = runner.runtime_custody

        assert descriptor['instance_id'] == authority.instance_id
        assert descriptor['authority_epoch'] == authority.epoch
        assert descriptor['served_profiles'] == runner.session_authorities.served_profiles()
        assert coordinator.process_instance == authority.instance_id
        assert coordinator.authority_epoch == authority.epoch


def test_missing_descriptor_stays_fail_closed(api, owner, monkeypatch):
    from gateway.platforms.api_server_run_custody import ApiRunCustody

    monkeypatch.setattr(owner.runner, 'session_runtime_descriptor', None)
    custody, refusal = ApiRunCustody.reserve(api, 'missing-descriptor', owner.profile_id)

    assert custody is None
    assert refusal is not None and refusal.code == 'runtime_custody_unavailable'


def test_missing_coordinator_stays_fail_closed(api, owner, monkeypatch):
    from gateway.platforms.api_server_run_custody import ApiRunCustody

    monkeypatch.delattr(owner.runner, 'runtime_custody')
    custody, refusal = ApiRunCustody.reserve(api, 'missing-coordinator', owner.profile_id)

    assert custody is None
    assert refusal is not None and refusal.code == 'runtime_custody_unavailable'
