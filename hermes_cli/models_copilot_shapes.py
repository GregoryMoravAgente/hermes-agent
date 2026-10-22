"""Pure Copilot catalog shape and routing helpers.

The public import and monkeypatch surface remains ``hermes_cli.models``.  This
module owns only deterministic payload, id, and API-mode decisions; the origin
module supplies its live catalog loader and current runtime constants.
"""

from __future__ import annotations

from collections.abc import Callable
import re
from typing import Any, Optional

from agent.reasoning_effort import CODEX_ASTRA_EFFORTS, is_astra_model
from hermes_cli.models_catalog_static import _COPILOT_MODEL_ALIASES


COPILOT_CHAT_ENDPOINTS = {"/chat/completions", "/responses", "/v1/messages"}


def payload_items(payload: Any) -> list[dict[str, Any]]:
    data = payload.get("data", []) if isinstance(payload, dict) else payload
    return [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []


def catalog_item_is_text_model(
    item: dict[str, Any], *, ignore_picker_flag: bool = False,
    chat_endpoints: set[str] | frozenset[str] = COPILOT_CHAT_ENDPOINTS,
) -> bool:
    if not str(item.get("id") or "").strip():
        return False
    if not ignore_picker_flag and item.get("model_picker_enabled") is False:
        return False
    capabilities = item.get("capabilities")
    if isinstance(capabilities, dict):
        model_type = str(capabilities.get("type") or "").strip().lower()
        if model_type and model_type != "chat":
            return False
    supported_endpoints = item.get("supported_endpoints")
    if isinstance(supported_endpoints, list):
        endpoints = {e for endpoint in supported_endpoints if (e := str(endpoint).strip())}
        if endpoints and not endpoints & chat_endpoints:
            return False
    return True


def text_models(
    items: list[dict[str, Any]], *, ignore_picker_flag: bool = False,
    chat_endpoints: set[str] | frozenset[str] = COPILOT_CHAT_ENDPOINTS,
) -> list[dict[str, Any]]:
    """Chat-capable catalog rows, deduped by id, in catalog order."""
    models: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for item in items:
        model_id = str(item.get("id") or "").strip()
        if model_id in seen_ids:
            continue
        if not catalog_item_is_text_model(
            item, ignore_picker_flag=ignore_picker_flag, chat_endpoints=chat_endpoints,
        ):
            continue
        seen_ids.add(model_id)
        models.append(item)
    return models


def catalog_ids(
    catalog: Optional[list[dict[str, Any]]], api_key: Optional[str],
    fetch_catalog: Callable[..., Optional[list[dict[str, Any]]]],
) -> set[str]:
    if catalog is None and api_key:
        catalog = fetch_catalog(api_key=api_key)
    return {mid for item in (catalog or []) if (mid := str(item.get("id") or "").strip())}


def normalize_model_id(
    model_id: Optional[str], *, catalog: Optional[list[dict[str, Any]]] = None,
    api_key: Optional[str] = None,
    fetch_catalog: Callable[..., Optional[list[dict[str, Any]]]],
    aliases: dict[str, str] = _COPILOT_MODEL_ALIASES,
) -> str:
    raw = str(model_id or "").strip()
    if not raw:
        return ""

    catalog_id_set = catalog_ids(catalog, api_key, fetch_catalog)
    alias = aliases.get(raw)
    if alias:
        return alias

    candidates = [raw]
    if "/" in raw:
        candidates.append(raw.split("/", 1)[1].strip())
    if raw.endswith(("-mini", "-nano", "-chat")):
        candidates.append(raw[:-5])

    seen: set[str] = set()
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        if candidate in aliases:
            return aliases[candidate]
        if candidate in catalog_id_set:
            return candidate

    if "/" in raw:
        stripped = raw.split("/", 1)[1].strip()
        # Enterprise BYOK custom models expose ``owner/sub/model`` ids (two
        # slashes). A strip guess that still contains "/" cannot be a Copilot
        # id, so pass the input through untouched instead of corrupting it.
        if stripped and "/" not in stripped:
            return stripped
    return raw


def reasoning_efforts_for_model_id(
    model_id: str, *,
    normalize: Callable[..., str],
    gpt5_efforts: list[str],
    o_series_efforts: list[str],
    astra_efforts: list[str] = CODEX_ASTRA_EFFORTS,
) -> list[str]:
    raw = (model_id or "").strip().lower()
    if raw.startswith(("openai/o1", "openai/o3", "openai/o4", "o1", "o3", "o4")):
        return list(o_series_efforts)
    normalized = normalize(model_id).lower()
    if is_astra_model(normalized):
        return list(astra_efforts)
    if normalized.startswith("gpt-5"):
        return list(gpt5_efforts)
    return []


def should_use_responses_api(model_id: str) -> bool:
    """opencode's ``shouldUseCopilotResponsesApi`` decision."""
    match = re.match(r"^gpt-(\d+)", model_id)
    return bool(match) and int(match.group(1)) >= 5 and not model_id.startswith("gpt-5-mini")


def model_api_mode(
    model_id: Optional[str], *, catalog: Optional[list[dict[str, Any]]] = None,
    api_key: Optional[str] = None,
    fetch_catalog: Callable[..., Optional[list[dict[str, Any]]]],
    normalize: Callable[..., str],
) -> str:
    """Route a Copilot model by id while leaving live catalog ownership to the origin module."""
    if catalog is None and api_key:
        catalog = fetch_catalog(api_key=api_key)
    normalized = normalize(model_id, catalog=catalog, api_key=api_key)
    if normalized and should_use_responses_api(normalized):
        return "codex_responses"
    return "chat_completions"


def model_reasoning_efforts(
    model_id: Optional[str], *, catalog: Optional[list[dict[str, Any]]] = None,
    api_key: Optional[str] = None,
    fetch_catalog: Callable[..., Optional[list[dict[str, Any]]]],
    normalize: Callable[..., str],
    pattern_efforts: Callable[[str], list[str]],
) -> list[str]:
    normalized = normalize(model_id, catalog=catalog, api_key=api_key)
    if not normalized:
        return []

    if catalog is None and api_key:
        catalog = fetch_catalog(api_key=api_key)
    catalog_entry = next((item for item in catalog if item.get("id") == normalized), None) if catalog else None
    if catalog_entry is not None:
        capabilities = catalog_entry.get("capabilities")
        if isinstance(capabilities, dict):
            # Structured catalog: the advertised list is authoritative (empty when absent).
            supports = capabilities.get("supports")
            efforts = supports.get("reasoning_effort") if isinstance(supports, dict) else None
            if not isinstance(efforts, list):
                return []
            return list(dict.fromkeys(e for effort in efforts if (e := str(effort).strip().lower())))
        # Legacy list-shaped capabilities: only a "reasoning" tag unlocks pattern defaults.
        if "reasoning" not in {str(c).strip().lower() for c in catalog_entry.get("capabilities", [])}:
            return []
    return pattern_efforts(str(model_id or normalized))
