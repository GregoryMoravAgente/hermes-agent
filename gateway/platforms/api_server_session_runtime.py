"""Runtime metadata and session-DB helpers for API session turns."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional


logger = logging.getLogger(__name__)


def _open_bare_session_db(adapter: Any, home) -> Optional[Any]:
    """Open the adapter-owned DB when a fixture runner has no SessionAuthority.

    ``selected_session_db`` is intentionally authority-only.  A bare adapter used by the
    session API still has a supported local store, so retain the same cache/close fence as
    ``APIServerAdapter._open_and_cache_session_db`` without treating a missing authority as
    proof of a live profile.
    """
    from hermes_state_registry import acquire

    key = str(home)
    with adapter._session_db_cache_lock:
        if adapter._session_db_cache_closed:
            return None
        db = adapter._cached_session_db_locked(key)
        if db is None:
            db = acquire(home / "state.db")
            adapter._session_dbs[key] = db
        return db


def effective_turn_runtime(self, runtime_request: dict[str, Any], result: Any, usage: Any) -> dict[str, Any]:
    """Sanitized runtime metadata for a finished session-chat turn."""
    runtime = self._result_runtime(result, usage)
    return self._sanitize_runtime_metadata(
        # Same shared ladder /api/status uses. Before this was unified, the two endpoints disagreed on
        # the same page load — the sidebar strip read "running" (it probed GATEWAY_HEALTH_URL and scoped
        # to the requested profile) while the Channels page rendered "The gateway is not running" (it
        # did neither). Cross-container, profile-scoped, and launch-service-managed deployments each hit
        # that split. profile_home is passed when the request was scoped to a named profile:
        # gateway/status readers resolve process-level paths and do NOT follow the HERMES_HOME
        # contextvar override (#56986 / #69143), so the profile's directory has to be handed over
        # explicitly or messaging silently reports another profile's gateway (#71211).
        runtime=runtime,
        requested_runtime=runtime_request.get("requested"),
        route_source=runtime_request.get("route_source") or "global",
        model_lock=self._model_lock_state(runtime_request, runtime))


def ensure_session_db(adapter: Any) -> Optional[Any]:
    """Resolve the active profile DB, retaining the bare-adapter fixture fallback."""
    if adapter.gateway_runner is not None:
        from hermes_constants import get_hermes_home
        from gateway.platforms.api_server_store import selected_session_db
        home = get_hermes_home()
        db = selected_session_db(adapter, home)
        if db is not None or "session_authorities" in vars(adapter.gateway_runner):
            return db
        if adapter._session_db is not None:
            return adapter._session_db
        try:
            return _open_bare_session_db(adapter, home)
        except Exception:
            logger.debug("SessionDB unavailable for API server", exc_info=True)
            return None
    if adapter._session_db is not None:
        return adapter._session_db
    try:
        from hermes_constants import get_hermes_home
        return adapter._open_and_cache_session_db(get_hermes_home())
    except Exception:
        logger.debug("SessionDB unavailable for API server", exc_info=True)
        return None


async def ensure_session_db_async(adapter: Any) -> Optional[Any]:
    """Async DB resolution; only blocking open work crosses into the worker thread."""
    if adapter.gateway_runner is not None:
        from hermes_constants import get_hermes_home
        from gateway.platforms.api_server_store import selected_session_db
        home = get_hermes_home()
        db = selected_session_db(adapter, home)
        if db is not None or "session_authorities" in vars(adapter.gateway_runner):
            return db
        if adapter._session_db is not None:
            return adapter._session_db
        try:
            return await asyncio.to_thread(_open_bare_session_db, adapter, home)
        except Exception:
            logger.debug("SessionDB unavailable for API server", exc_info=True)
            return None
    if adapter._session_db is not None:
        return adapter._session_db
    try:
        from hermes_constants import get_hermes_home
        home = get_hermes_home()
        key = str(home)
        with adapter._session_db_cache_lock:
            cached = adapter._cached_session_db_locked(key)
        if cached is not None:
            return cached
        if adapter._session_db_lock is None:
            adapter._session_db_lock = asyncio.Lock()
        async with adapter._session_db_lock:
            with adapter._session_db_cache_lock:
                cached = adapter._cached_session_db_locked(key)
            if cached is not None:
                return cached
            return await asyncio.to_thread(_open_bare_session_db, adapter, home)
    except Exception:
        logger.debug("SessionDB unavailable for API server", exc_info=True)
        return None
