"""Profile-scoped platform callback authorization for adapter lifecycle."""
from __future__ import annotations

import functools
import weakref as _weakref
from typing import Optional

from gateway.config import Platform
from gateway.session import SessionSource


def make_adapter_auth_check(lifecycle, platform: Platform, profile_name: Optional[str],
                            transport_home, profile_home):
    def check(
        user_id: str, chat_type: Optional[str] = None, chat_id: Optional[str] = None, *,
        is_bot: bool = False, thread_id: Optional[str] = None,
    ) -> bool:
        if not user_id:
            return False
        source = SessionSource(
            platform=platform, chat_id=chat_id or "", chat_type=chat_type or "group",
            user_id=user_id, thread_id=thread_id, is_bot=bool(is_bot), profile=profile_name,
        )
        # Same transport provenance as ``build_source``, so policy reads resolve the receiving adapter.
        registry = (
            (getattr(lifecycle, "_profile_adapters", None) or {}).get(profile_name)
            if profile_name else getattr(lifecycle, "adapters", None)
        ) or {}
        adapter = registry.get(platform)
        if adapter is not None:
            source._transport_adapter_ref = _weakref.ref(adapter)
        if transport_home is None:
            # Sync, on the adapter's event loop (per tap, per inline-query keystroke): never
            # hydrate external secret sources here — that takes the process-global source lock
            # (#99519). Startup and the message path hydrate off-loop; this reads their cache.
            from gateway.run import _profile_runtime_scope
            with lifecycle._scope_or_null(
                    functools.partial(_profile_runtime_scope, hydrate_secrets=False), profile_home):
                return lifecycle._is_user_authorized(source)
        # Canonicalize FIRST (callback sources never went through ``build_source``): the routed
        # profile's pairing store is consulted, allowlists read under the transport home.
        if lifecycle._canonicalize(source, primary_home=transport_home) is None:
            return False  # fail-closed, like the ``_handle_message`` ingress gate
        return lifecycle._is_user_authorized_for_source(source)
    return check
