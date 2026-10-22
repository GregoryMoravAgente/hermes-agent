"""Source-owned legacy turn result shaping for the API adapter."""

from typing import Any, Optional


def finish_turn_result(
    adapter: Any, agent: Any, result: Any, session_id: Optional[str], *, route,
    requested_runtime, route_source, confirmed_runtime_lock: bool,
) -> tuple:
    """Attach usage, effective session id, compaction and runtime metadata to a finished turn."""
    usage = {"input_tokens": getattr(agent, "session_prompt_tokens", 0) or 0,
             "output_tokens": getattr(agent, "session_completion_tokens", 0) or 0,
             "total_tokens": getattr(agent, "session_total_tokens", 0) or 0}
    _eff_sid = getattr(agent, "session_id", session_id)
    if isinstance(_eff_sid, str) and _eff_sid:
        result["session_id"] = _eff_sid
    _session_rotated = isinstance(_eff_sid, str) and isinstance(session_id, str) and _eff_sid != session_id
    if getattr(agent, "_last_compaction_in_place", False) or _session_rotated:
        result["_compressed"] = True
    if requested_runtime or route or confirmed_runtime_lock or (route_source and route_source != "global"):
        runtime = adapter._turn_runtime_metadata(
            agent, route=route, requested_runtime=requested_runtime,
            route_source=route_source, confirmed_runtime_lock=confirmed_runtime_lock)
        if isinstance(result, dict):
            result["runtime"] = runtime
        usage["runtime"] = runtime
    return result, usage
