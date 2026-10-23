"""Profile-scoped helpers used by the ordinary gateway runtime lifecycle."""

from __future__ import annotations

from pathlib import Path


def store_open_failure(runner, home):
    """Return the cached reason for a failed profile store open without reopening it."""
    path = home / 'state.db'
    caches = (
        getattr(getattr(runner, 'session_store', None), '_db_handle_cache', None),
        getattr(runner, '_session_db_handle_cache', None),
    )
    error = next((e for e in (c.last_error(path) for c in caches if c is not None) if e), '')
    if error:
        return f'{path} did not open: {error}'
    return f'{path} is unavailable (its last open failed; retry pending)'


def retire_process_registry_profile(runner, authority) -> bool:
    """Drop one I15 profile registration only after its source fence is empty and closed."""
    from tools.process_registry import process_registry
    from hermes_cli.profile_provenance_bootstrap import ProfileProvenanceError, _safe_path

    factory = getattr(process_registry, '_i15_zero_factory', None)
    try:
        profile_id = str(_safe_path(Path(authority.profile_id), label='served profile home'))
    except ProfileProvenanceError:
        return False
    if factory is None or not factory.profile_registered(profile_id):
        return True
    return factory.retire_profile(profile_id, authority.epoch)
