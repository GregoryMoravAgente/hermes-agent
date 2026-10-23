"""Test-only return-site observations for the frozen Windows native diagnosis.

No locals or receipt values are emitted. The complete native bank remains a
separate qualification gate; this plugin observes the existing source guards.
"""

import json
from pathlib import Path
import sys
import threading

import pytest


_FILES = {
    "runtime_native_cleanup_sources.py", "runtime_native_cleanup_profile.py",
    "hosted_tui_custody.py", "profile_lifecycle_custody.py",
}
_FUNCTIONS = {
    "build_native_cleanup_plan", "_hosted_tui_prerequisite",
    "_profile_lifecycle_prerequisite", "_capture_profile_native_cleanup_witness",
    "_reserved_profile_bindings", "_profile_handle", "_provider_participants",
    "_profile_participants", "profile_participants_match_witness",
    "profile_bindings_match", "_profile_host_evidence", "_host_producer_evidence",
    "_room_producer_evidence", "_room_terminal_identity_matches",
    "_registry_identity_valid", "_source_prerequisites", "_valid_source_evidence",
    "_owner_is_sealed", "_has_active_isolated_sessions", "_has_live_host",
}


@pytest.fixture(autouse=True)
def native_plan_return_observer(request):
    """Observe only selected return sites, retaining all original test oracles."""
    previous = sys.gettrace()
    previous_thread = threading.gettrace()
    if previous is not None or previous_thread is not None:
        raise RuntimeError("native diagnosis refuses to replace an existing tracer")
    lock = threading.RLock()

    def trace(frame, event, value):
        filename = Path(frame.f_code.co_filename).name
        function = frame.f_code.co_name
        if filename not in _FILES or function not in _FUNCTIONS:
            return None
        if event == "return":
            result = "none" if value is None else "true" if value is True else "false" if value is False else "value"
            row = {
                "case": request.node.nodeid, "file": filename,
                "function": function, "return_line": frame.f_lineno,
                "result": result,
            }
            if type(value) is tuple and value and type(value[0]) is bool:
                row["first_boolean"] = value[0]
            with lock:
                print("NATIVE_PLAN_RETURN=" + json.dumps(row, sort_keys=True), flush=True)
        return trace

    threading.settrace_all_threads(trace)
    sys.settrace(trace)
    try:
        yield
    finally:
        threading.settrace_all_threads(previous_thread)
        sys.settrace(previous)
