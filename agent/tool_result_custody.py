"""Managed tool-result durability and foreground-custody delivery.

The ordinary tool executor keeps its historical persistence path for calls that
have no foreground-custody binding. A managed call must pass through this
small, typed boundary: a literal database proof is produced first, then the
installed turn callback settles the original native transaction.
"""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import logging
import sqlite3
from types import MappingProxyType
from typing import Any, Optional


logger = logging.getLogger(__name__)
_CUSTODY_EXCEPTIONS = (AttributeError, OSError, RuntimeError, TypeError, ValueError, sqlite3.Error)


@dataclass(frozen=True)
class ManagedToolFlushProof:
    """A literal, row-bound SQLite write witness for managed foreground custody.

    ``readback`` is the recursively immutable public-API snapshot of the addressed row.
    The digests also fence the live message object before every retry.  A raw ``True``
    flush result and an in-memory marker are insufficient custody evidence.
    """

    database: Any
    session_id: str
    row_id: int
    tool_call_id: str
    message: dict[str, Any]
    raw_flush_result: bool
    readback: Mapping[str, Any]
    readback_digest: str = ""
    message_digest: str = ""


def _canonical_payload(value: Any) -> Any:
    """Return a stable JSON shape for the structured row proof."""
    if isinstance(value, Mapping):
        return [
            "mapping",
            [
                [str(key), _canonical_payload(item)]
                for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            ],
        ]
    if isinstance(value, (list, tuple)):
        return ["sequence", [_canonical_payload(item) for item in value]]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return ["opaque", type(value).__name__, repr(value)]


def _payload_digest(value: Any) -> str:
    try:
        encoded = json.dumps(
            _canonical_payload(value), ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        return ""
    return hashlib.sha256(encoded).hexdigest()


def _freeze_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze_payload(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_payload(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze_payload(item) for item in value)
    return value


def _payload_equal(left: Any, right: Any) -> bool:
    if isinstance(left, Mapping) and isinstance(right, Mapping):
        return (
            set(left) == set(right)
            and all(_payload_equal(left[key], right[key]) for key in left)
        )
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(
            _payload_equal(item, other) for item, other in zip(left, right)
        )
    return left == right


def read_managed_tool_row(
    database: Any, session_id: str, row_id: int, tool_call_id: str,
    message: Mapping[str, Any], *, expected_readback: Mapping[str, Any] | None = None,
    expected_digest: str | None = None,
) -> Mapping[str, Any] | None:
    """Read back and validate one exact durable tool row through the public conversation API."""
    get_messages = getattr(database, "get_messages_as_conversation", None)
    if not callable(get_messages):
        return None
    try:
        rows = get_messages(session_id, include_row_ids=True)
    except _CUSTODY_EXCEPTIONS:
        return None
    for row in rows if isinstance(rows, list) else ():
        if not isinstance(row, dict) or row.get("_row_id") != row_id:
            continue
        if row.get("role") != "tool" or row.get("tool_call_id") != tool_call_id:
            return None
        if row.get("content") != message.get("content"):
            return None
        for key in (
            "role", "content", "tool_call_id", "tool_name", "effect_disposition",
            "api_content", "display_kind", "display_metadata", "tool_calls",
            "timestamp", "message_id", "observed",
        ):
            if (key in row or key in message) and row.get(key) != message.get(key):
                return None
        try:
            snapshot = deepcopy(row)
        except (TypeError, ValueError):
            return None
        if expected_readback is not None and not _payload_equal(expected_readback, snapshot):
            return None
        if expected_digest is not None and _payload_digest(snapshot) != expected_digest:
            return None
        return _freeze_payload(snapshot)
    return None


def _flush_managed_tool_result(
    agent, messages: list, tool_message: dict[str, Any], *, tool_call_id: str, stage: str,
) -> Optional[ManagedToolFlushProof]:
    """Flush and prove one exact tool row; legacy callers keep their old contract."""
    from agent.conversation_loop import _maybe_inject_run_budget_wrapup
    from agent.turn_iteration_prep import _maybe_inject_iteration_budget_warning
    from agent.context_compressor import _DB_PERSISTED_MARKER

    _maybe_inject_run_budget_wrapup(agent, messages)
    _maybe_inject_iteration_budget_warning(agent, messages)
    if getattr(agent, "_persist_disabled", False) or getattr(agent, "_session_db", None) is None:
        return None
    try:
        raw = agent._flush_messages_to_session_db(messages)
    except _CUSTODY_EXCEPTIONS as exc:
        agent._incremental_persistence_failed = True
        from hermes_state import classify_persistence_error

        agent._last_persistence_error_cause = classify_persistence_error(exc)
        logger.warning("Managed tool-call persistence failed after %s: %s", stage, exc)
        return None
    if raw is not True:
        agent._incremental_persistence_failed = True
        if getattr(agent, "_last_persistence_error_cause", None) is None:
            agent._last_persistence_error_cause = "unknown"
        return None
    database = getattr(agent, "_session_db", None)
    session_id = getattr(agent, "session_id", None)
    row_id = tool_message.get("_row_id") if isinstance(tool_message, dict) else None
    if (
        database is None
        or not isinstance(session_id, str)
        or not session_id
        or not any(message is tool_message for message in messages)
        or type(row_id) is not int
        or row_id < 0
        or tool_message.get(_DB_PERSISTED_MARKER) is not True
        or tool_message.get("tool_call_id") != tool_call_id
    ):
        return None
    readback = read_managed_tool_row(database, session_id, row_id, tool_call_id, tool_message)
    if readback is None:
        return None
    return ManagedToolFlushProof(
        database=database,
        session_id=session_id,
        row_id=row_id,
        tool_call_id=tool_call_id,
        message=tool_message,
        raw_flush_result=True,
        readback=readback,
        readback_digest=_payload_digest(readback),
        message_digest=_payload_digest(tool_message),
    )


def _required_state(required_check, *, tool_call_id: str) -> Optional[bool]:
    """Return True/False/None for managed, known-unmanaged, or indeterminate."""
    try:
        required = required_check(tool_call_id=tool_call_id)
    except _CUSTODY_EXCEPTIONS as exc:
        logger.warning("Managed custody lookup failed: %s", type(exc).__name__)
        return None
    if required is True:
        return True
    if required is False:
        return False
    return None


def _invoke_refusal_callback(
    callback, *, tool_call_id: str, function_result: Any, persisted_result: Any,
    message: dict[str, Any], lookup_failed: bool,
) -> None:
    try:
        callback(
            proof=None,
            message=message,
            lookup_failed=lookup_failed,
            tool_call_id=tool_call_id,
            function_result=function_result,
            persisted_result=persisted_result,
        )
    except _CUSTODY_EXCEPTIONS as exc:
        logger.warning("Managed custody refusal could not be recorded: %s", type(exc).__name__)


def commit_managed_tool_result_if_needed(
    agent, messages: list, tool_message: dict[str, Any], *, tool_call_id: str,
    function_result: Any, persisted_result: Any, stage: str, before_settle=None,
) -> Optional[bool]:
    """Run the managed proof/commit path; None means known legacy/unmanaged."""
    callback = getattr(agent, "tool_result_committed_callback", None)
    required_check = getattr(agent, "tool_result_custody_required", None)
    if not callable(callback) and not callable(required_check):
        return None
    if not callable(callback) or not callable(required_check):
        # A partially installed managed boundary is indeterminate.  Falling through to the
        # legacy ``None-is-success`` flush would project a result without its custody owner.
        if callable(callback):
            _invoke_refusal_callback(
                callback,
                tool_call_id=tool_call_id,
                function_result=function_result,
                persisted_result=persisted_result,
                message=tool_message,
                lookup_failed=True,
            )
        return False
    required = _required_state(required_check, tool_call_id=tool_call_id)
    if required is False:
        return None
    if required is None:
        _invoke_refusal_callback(
            callback,
            tool_call_id=tool_call_id,
            function_result=function_result,
            persisted_result=persisted_result,
            message=tool_message,
            lookup_failed=True,
        )
        return False
    proof = _flush_managed_tool_result(
        agent, messages, tool_message, tool_call_id=tool_call_id, stage=stage,
    )
    if proof is None:
        _invoke_refusal_callback(
            callback,
            tool_call_id=tool_call_id,
            function_result=function_result,
            persisted_result=persisted_result,
            message=tool_message,
            lookup_failed=False,
        )
        return False
    if before_settle is not None:
        try:
            settled_children = before_settle(proof)
        except _CUSTODY_EXCEPTIONS as exc:
            logger.warning("Managed child custody callback failed: %s", type(exc).__name__)
            settled_children = False
        if settled_children is False:
            _invoke_refusal_callback(
                callback,
                tool_call_id=tool_call_id,
                function_result=function_result,
                persisted_result=persisted_result,
                message=tool_message,
                lookup_failed=False,
            )
            return False
    try:
        settled = callback(
            proof=proof,
            message=tool_message,
            lookup_failed=False,
            tool_call_id=tool_call_id,
            function_result=function_result,
            persisted_result=persisted_result,
        )
    except _CUSTODY_EXCEPTIONS as exc:
        logger.warning("Managed foreground custody callback failed: %s", type(exc).__name__)
        return False
    if settled is not True:
        logger.warning("Managed foreground custody remains pending after durable result")
        return False
    return True
