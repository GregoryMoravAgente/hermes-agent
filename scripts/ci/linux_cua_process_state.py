"""Bounded Linux process-state observer used by the hosted CUA custody script."""

from __future__ import annotations

import os
from pathlib import Path
import time
from typing import Any, Callable


PROCESS_STATE_MEMBER_LIMIT = 64


def read_process_state(
    pid: int,
    *,
    digest_bytes: Callable[[bytes], str],
) -> dict[str, Any]:
    """Read bounded state fields without collecting command lines or environments."""
    if pid <= 0:
        raise OSError("invalid process id")
    value = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8-sig")
    closing_parenthesis = value.rfind(")")
    if closing_parenthesis < 0:
        raise OSError("malformed Linux process stat")
    fields = value[closing_parenthesis + 2 :].split()
    if len(fields) < 20:
        raise OSError("incomplete Linux process stat")
    try:
        executable = Path(os.readlink(f"/proc/{pid}/exe")).name
    except (OSError, ValueError):
        executable = "state_unavailable"
    if executable not in {"chrome", "chromium"} and executable != "state_unavailable":
        executable = f"sha256:{digest_bytes(executable.encode('utf-8'))[:16]}"
    return {
        "pid": pid,
        "state": fields[0],
        "ppid": int(fields[1]),
        "pgid": int(fields[2]),
        "session": int(fields[3]),
        "start_ticks": int(fields[19]),
        "executable_label": executable or "state_unavailable",
    }


def capture_timeout_process_state(
    identity: dict[str, Any],
    deadline: float,
    *,
    group_snapshot: Callable[..., dict[str, Any]],
    state_reader: Callable[[int], dict[str, Any]],
) -> dict[str, Any]:
    """Capture one authenticated, bounded state snapshot before cleanup."""
    membership = group_snapshot(identity, deadline=deadline)
    members = membership.get("members", [])
    truncated = len(members) > PROCESS_STATE_MEMBER_LIMIT
    states: list[dict[str, Any]] = []
    errors = list(membership.get("scan_errors", []))
    deadline_exceeded = bool(membership.get("deadline_exceeded"))
    for member in members[:PROCESS_STATE_MEMBER_LIMIT]:
        if time.monotonic() >= deadline:
            deadline_exceeded = True
            errors.append("snapshot_deadline")
            break
        try:
            state = state_reader(member["pid"])
        except (OSError, ValueError) as exc:
            errors.append(type(exc).__name__)
            continue
        identity_matches = all(
            state.get(field) == member.get(field)
            for field in ("pid", "pgid", "session", "start_ticks")
        )
        state["identity_matches"] = identity_matches
        states.append(state)
        if not identity_matches:
            errors.append("member_identity_mismatch")
    root_state = next(
        (state for state in states if state["pid"] == identity["pid"] and state["identity_matches"]),
        None,
    )
    root_wait_channel: str | None = None
    root_wait_channel_readable = False
    if root_state is not None and time.monotonic() < deadline:
        try:
            root_wait_channel = Path(f"/proc/{identity['pid']}/wchan").read_text(
                encoding="utf-8"
            ).strip()[:128]
            root_wait_channel_readable = bool(root_wait_channel)
        except (OSError, UnicodeError):
            root_wait_channel = None
    elif root_state is not None:
        deadline_exceeded = True
        errors.append("snapshot_deadline")
    authenticated = (
        bool(membership.get("verified"))
        and not truncated
        and not deadline_exceeded
        and len(states) == len(members)
        and all(state["identity_matches"] for state in states)
    )
    return {
        "authenticated": authenticated,
        "member_limit": PROCESS_STATE_MEMBER_LIMIT,
        "member_count": len(members),
        "observed_member_count": len(states),
        "truncated": truncated,
        "membership_verified": bool(membership.get("verified")),
        "membership_root_present": bool(membership.get("root_present")),
        "membership_root_identity_matches": bool(membership.get("root_identity_matches")),
        "membership_member_identity_matches": bool(membership.get("member_identity_matches")),
        "members": states,
        "root_wait_channel": root_wait_channel,
        "root_wait_channel_readable": root_wait_channel_readable,
        "scan_errors": errors,
    }
