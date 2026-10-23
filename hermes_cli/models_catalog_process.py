"""Managed external-process model discovery for the live catalog seam."""

from __future__ import annotations

import inspect
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def fetch_external_process_models(profile: Any, producer_policy: Any) -> Optional[list[str]]:
    if producer_policy is None:
        return None
    fetch = getattr(profile, "fetch_models", None)
    if not callable(fetch):
        return None
    try:
        parameters = inspect.signature(fetch).parameters
    except (TypeError, ValueError):
        parameters = None
    legacy_signature = parameters is not None and "producer_policy" not in parameters and not any(
        parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values())
    try:
        live = fetch() if legacy_signature else fetch(producer_policy=producer_policy)
        return list(live) if live is not None else None
    except Exception:
        parent = getattr(producer_policy, "parent", None)
        if parent is not None:
            try:
                from gateway.runtime_service_operations import mark_unknown
                mark_unknown(parent, "catalog_discovery_failed")
            except Exception:
                logger.debug("external-process catalog ownership marking failed", exc_info=True)
        logger.debug("external-process catalog fetch failed", exc_info=True)
        return None
