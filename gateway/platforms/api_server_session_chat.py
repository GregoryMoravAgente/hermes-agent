"""Canonical-target and input validation for the public session chat routes."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aiohttp import web


def retired_session_error(session: dict[str, Any]) -> web.Response | None:
    """A retired explicit target cannot start a replacement session or provider turn."""
    from gateway.platforms.api_server import _error_response

    if session.get("ended_at") is not None:
        return _error_response("Session is retired and cannot accept new turns", 409, code="session_retired")
    return None


def session_chat_user_message(body: dict[str, Any], *, param: str = "message") -> tuple[Any, web.Response | None]:
    """Parse and normalize session chat ``message`` / ``input`` like chat completions."""
    from gateway.platforms.api_server import (
        _content_has_visible_payload, _error_response,
        _multimodal_validation_error, _normalize_multimodal_content,
    )

    user_message = body.get("message") or body.get("input")
    if not _content_has_visible_payload(user_message):
        return None, _error_response("Missing 'message' field", 400, code="missing_message")
    try:
        return _normalize_multimodal_content(user_message), None
    except ValueError as exc:
        return None, _multimodal_validation_error(exc, param=param)
