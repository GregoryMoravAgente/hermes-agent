"""I16 bootstrap and hot-serve evidence must follow the real profile registry."""

import pytest

from tests.gateway.test_runtime_custody_profile_epochs import _boot, _close_boot


@pytest.mark.asyncio
@pytest.mark.parametrize('hot_serve', [False, True])
async def test_real_profile_factory_graph_reaches_global_close(tmp_path, monkeypatch, hot_serve):
    from gateway.run_runtime import _seal_integrated_preservation_owners, serve_profile_runtime
    from gateway.runtime_ownership import process_ownership

    runner, entries = await _boot(tmp_path, monkeypatch)
    entries = list(entries)
    close = None
    try:
        if hot_serve:
            home = entries[0][1] / 'profiles' / 'beta'
            home.mkdir(parents=True, mode=0o700)
            (home / 'config.yaml').write_text('{}')
            process_ownership.reserve([home])
            entries.append(('beta', home))
            authority = await serve_profile_runtime(runner, 'beta', home)
            assert runner.session_authorities.for_home(home) is authority
        coordinator = runner.runtime_custody
        registry = coordinator._i16_source_registry
        authorities = tuple(runner.session_authorities)
        assert len(authorities) == (3 if hot_serve else 2)
        assert set(map(id, registry.authorities)) == set(map(id, authorities))
        for key, attribute in (
            ('mcp', '_mcp_source_factory'),
            ('browser', '_browser_source_factory'),
            ('cua', '_cua_source_factory'),
        ):
            bindings = registry.factory_bindings[key]
            assert len(bindings) == len(authorities)
            assert all(any(
                source is authority and factory is getattr(authority, attribute)
                for source, factory in bindings
            ) for authority in authorities)
        close = coordinator.begin_close(owner_id='live-profile-i16-close')
        _seal_integrated_preservation_owners(runner, coordinator, close)
        assert coordinator.status()['owner_states']['I16_turn_sidecars'] == 'sealed'
    finally:
        if close is not None:
            assert runner.runtime_custody.abort_before_handoff(close) is None
        assert await _close_boot(runner, entries) is True
