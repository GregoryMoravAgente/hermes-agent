"""Real profile-authority fixtures for plugin runtime custody tests."""

from pathlib import Path

import pytest_asyncio


@pytest_asyncio.fixture
async def teams_gateway(tmp_path, monkeypatch):
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership

    launch = tmp_path / '.hermes'
    secondary = launch / 'profiles' / 'served'
    secondary.mkdir(parents=True, mode=0o700)
    (launch / 'config.yaml').write_text('{}')
    (secondary / 'config.yaml').write_text('{}')
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    monkeypatch.setenv('HERMES_HOME', str(launch))
    import hermes_state
    monkeypatch.setattr(hermes_state, 'DEFAULT_DB_PATH', hermes_state._IMPORT_DEFAULT_DB_PATH)
    config = GatewayConfig(multiplex_profiles=True, sessions_dir=launch / 'sessions')
    config._runtime_profile_homes = (('served', secondary),)
    process_ownership.reserve([launch, secondary])
    runner = None
    try:
        runner = GatewayRunner(config)
        await initialize_gateway_runtime(runner)
        yield runner
    finally:
        if runner is not None:
            for authority in list(getattr(runner, 'session_authorities', []) or []):
                authority.db.close()
        process_ownership.release(secondary)
        process_ownership.release(launch)
