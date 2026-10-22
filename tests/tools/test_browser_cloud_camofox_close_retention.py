"""Loopback consumer proofs for cloud and Camofox close-retention semantics.

These controls use the public cleanup/navigation entry points and a test-owned local
HTTP endpoint.  A failed remote close must leave the exact provider/session identity
available for the next cleanup attempt; only the acknowledged close retires it.
"""

from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen


@contextmanager
def _loopback_browser_server(
    outcomes: list[int], *, empty_body_statuses: set[int] = frozenset(),
    response_bodies: list[dict | None] | None = None,
):
    state = {
        "delete_paths": [],
        "outcomes": list(outcomes),
        "empty_body_statuses": set(empty_body_statuses),
        "response_bodies": list(response_bodies or []),
    }

    class Handler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, body: dict):
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            if self.path == "/health":
                self._send_json(200, {"status": "ok"})
            elif self.path.endswith("/snapshot"):
                self._send_json(200, {"snapshot": "", "refsCount": 0})
            else:
                self._send_json(404, {"error": "not found"})

        def do_POST(self):
            if self.path == "/tabs":
                self._send_json(200, {"tabId": "tab-loopback", "url": "http://127.0.0.1/"})
            else:
                self._send_json(404, {"error": "not found"})

        def do_DELETE(self):
            state["delete_paths"].append(self.path)
            status = state["outcomes"].pop(0) if state["outcomes"] else 204
            if status in state["empty_body_statuses"]:
                self.send_response(status)
                self.end_headers()
                return
            if state["response_bodies"]:
                body = state["response_bodies"].pop(0)
                if body is None:
                    self.send_response(status)
                    self.end_headers()
                    return
                self._send_json(status, body)
                return
            self._send_json(status, {"closed": status < 300})

        def log_message(self, *_args):
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", state
    finally:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()


class _LoopbackCloudProvider:
    def __init__(self, base_url: str, *, raise_once: bool = False):
        self.base_url = base_url
        self.raise_once = raise_once

    def close_session(self, session_id: str) -> bool:
        if self.raise_once:
            self.raise_once = False
            raise RuntimeError("test provider close failure")
        request = Request(f"{self.base_url}/sessions/{session_id}", method="DELETE")
        try:
            with urlopen(request, timeout=2) as response:
                return 200 <= response.status < 300
        except HTTPError:
            return False


def test_cloud_cleanup_retains_exact_session_until_loopback_provider_ack(monkeypatch):
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    with _loopback_browser_server([503, 204]) as (base_url, state):
        task_id = "cloud-retain"
        session_info = {"session_name": "", "bb_session_id": "cloud-loopback-1"}
        monkeypatch.setitem(browser_tool._active_sessions, task_id, session_info)
        monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: False)
        monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)
        monkeypatch.setattr(lifecycle._session, "_run_browser_command", lambda *_a, **_k: {"success": True})
        monkeypatch.setattr(lifecycle._cloud, "_get_cloud_provider", lambda: _LoopbackCloudProvider(base_url))

        assert lifecycle.cleanup_browser(task_id) is False
        assert browser_tool._active_sessions[task_id] is session_info

        assert lifecycle.cleanup_browser(task_id) is True
        assert task_id not in browser_tool._active_sessions
        assert state["delete_paths"] == ["/sessions/cloud-loopback-1", "/sessions/cloud-loopback-1"]


def test_cloud_cleanup_retains_exact_session_after_provider_exception(monkeypatch):
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    with _loopback_browser_server([204]) as (base_url, state):
        task_id = "cloud-exception"
        session_info = {"session_name": "", "bb_session_id": "cloud-loopback-exception"}
        provider = _LoopbackCloudProvider(base_url, raise_once=True)
        monkeypatch.setitem(browser_tool._active_sessions, task_id, session_info)
        monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: False)
        monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)
        monkeypatch.setattr(lifecycle._session, "_run_browser_command", lambda *_a, **_k: {"success": True})
        monkeypatch.setattr(lifecycle._cloud, "_get_cloud_provider", lambda: provider)

        assert lifecycle.cleanup_browser(task_id) is False
        assert browser_tool._active_sessions[task_id] is session_info

        assert lifecycle.cleanup_browser(task_id) is True
        assert task_id not in browser_tool._active_sessions
        assert state["delete_paths"] == ["/sessions/cloud-loopback-exception"]


def test_camofox_navigation_and_cleanup_retain_user_identity_after_failed_delete(monkeypatch):
    from tools import browser_camofox
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    previous_sessions = browser_camofox._sessions.copy()
    browser_camofox._sessions.clear()
    try:
        with _loopback_browser_server([503, 204]) as (base_url, state):
            task_id = "camofox-retain"
            monkeypatch.setenv("CAMOFOX_URL", base_url)
            monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
            monkeypatch.setattr(
                browser_camofox,
                "load_config",
                lambda: {"browser": {"camofox": {"managed_persistence": False}}},
            )
            monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: True)
            monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)

            navigation = json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))
            assert navigation["success"] is True
            session = browser_camofox._sessions[task_id]
            user_id = session["user_id"]

            assert lifecycle.cleanup_browser(task_id) is False
            assert browser_camofox._sessions[task_id] is session

            assert lifecycle.cleanup_browser(task_id) is True
            assert task_id not in browser_camofox._sessions
            assert state["delete_paths"] == [f"/sessions/{user_id}", f"/sessions/{user_id}"]
    finally:
        browser_camofox._sessions.clear()
        browser_camofox._sessions.update(previous_sessions)


def test_camofox_aggregate_cleanup_retries_retained_session_and_accepts_empty_204(monkeypatch):
    from tools import browser_camofox
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    previous_sessions = browser_camofox._sessions.copy()
    browser_camofox._sessions.clear()
    try:
        with _loopback_browser_server([503, 204], empty_body_statuses={204}) as (base_url, state):
            task_id = "camofox-aggregate-retain"
            monkeypatch.setenv("CAMOFOX_URL", base_url)
            monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
            monkeypatch.setattr(
                browser_camofox,
                "load_config",
                lambda: {"browser": {"camofox": {"managed_persistence": False}}},
            )
            monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: True)
            monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)

            assert json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))["success"]
            session = browser_camofox._sessions[task_id]

            assert lifecycle.cleanup_browser(task_id) is False
            assert browser_camofox._sessions[task_id] is session
            assert lifecycle.cleanup_all_browsers() is True
            assert task_id not in browser_camofox._sessions
            assert state["delete_paths"] == [f"/sessions/{session['user_id']}", f"/sessions/{session['user_id']}"]
    finally:
        browser_camofox._sessions.clear()
        browser_camofox._sessions.update(previous_sessions)


def test_camofox_emergency_cleanup_retries_retained_session(monkeypatch):
    from tools import browser_camofox
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    previous_sessions = browser_camofox._sessions.copy()
    browser_camofox._sessions.clear()
    try:
        with _loopback_browser_server([503, 204], empty_body_statuses={204}) as (base_url, state):
            task_id = "camofox-emergency-retain"
            monkeypatch.setenv("CAMOFOX_URL", base_url)
            monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
            monkeypatch.setattr(
                browser_camofox,
                "load_config",
                lambda: {"browser": {"camofox": {"managed_persistence": False}}},
            )
            monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: True)
            monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)
            monkeypatch.setattr(lifecycle._real_profile, "_terminate_real_profile_chrome", lambda: None)
            monkeypatch.setattr(lifecycle, "_stop_all_lightpanda", lambda: None)
            monkeypatch.setattr(lifecycle, "_reap_orphaned_browser_sessions", lambda: None)

            assert json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))["success"]
            session = browser_camofox._sessions[task_id]
            assert lifecycle.cleanup_browser(task_id) is False
            assert browser_camofox._sessions[task_id] is session

            monkeypatch.setattr(browser_tool, "_cleanup_done", False)
            lifecycle._emergency_cleanup_all_sessions()
            assert task_id not in browser_camofox._sessions
            assert state["delete_paths"] == [f"/sessions/{session['user_id']}", f"/sessions/{session['user_id']}"]
    finally:
        browser_camofox._sessions.clear()
        browser_camofox._sessions.update(previous_sessions)


def test_camofox_force_reap_runs_success_tail_after_ack(monkeypatch):
    from tools import browser_camofox
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    previous_sessions = browser_camofox._sessions.copy()
    previous_activity = browser_tool._session_last_activity.copy()
    previous_recording = set(browser_tool._recording_sessions)
    previous_bindings = browser_tool._last_active_session_key.copy()
    browser_camofox._sessions.clear()
    try:
        with _loopback_browser_server([503, 204], empty_body_statuses={204}) as (base_url, state):
            task_id = "camofox-force-reap"
            monkeypatch.setenv("CAMOFOX_URL", base_url)
            monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
            monkeypatch.setattr(
                browser_camofox,
                "load_config",
                lambda: {"browser": {"camofox": {"managed_persistence": False}}},
            )
            monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: True)
            monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)

            assert json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))["success"]
            session = browser_camofox._sessions[task_id]
            browser_tool._session_last_activity[task_id] = 1.0
            browser_tool._recording_sessions.add(task_id)
            browser_tool._last_active_session_key[task_id] = task_id

            assert lifecycle.cleanup_browser(task_id) is False
            assert lifecycle._force_reap_browser_session(task_id) is True
            assert task_id not in browser_camofox._sessions
            assert task_id not in browser_tool._session_last_activity
            assert task_id not in browser_tool._recording_sessions
            assert task_id not in browser_tool._last_active_session_key
            assert state["delete_paths"] == [f"/sessions/{session['user_id']}", f"/sessions/{session['user_id']}"]
    finally:
        browser_camofox._sessions.clear()
        browser_camofox._sessions.update(previous_sessions)
        browser_tool._session_last_activity.clear()
        browser_tool._session_last_activity.update(previous_activity)
        browser_tool._recording_sessions.clear()
        browser_tool._recording_sessions.update(previous_recording)
        browser_tool._last_active_session_key.clear()
        browser_tool._last_active_session_key.update(previous_bindings)


def test_camofox_same_public_task_id_isolated_by_profile_and_endpoint(monkeypatch, tmp_path):
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from tools import browser_camofox
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    previous_sessions = browser_camofox._sessions.copy()
    previous_active = browser_tool._active_sessions.copy()
    browser_camofox._sessions.clear()
    browser_tool._active_sessions.clear()
    try:
        with (
            _loopback_browser_server([204], empty_body_statuses={204}) as (url_a, state_a),
            _loopback_browser_server([204], empty_body_statuses={204}) as (url_b, state_b),
        ):
            monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
            monkeypatch.setattr(
                browser_camofox,
                "load_config",
                lambda: {"browser": {"camofox": {"managed_persistence": False}}},
            )
            monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: True)
            monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)

            task_id = "same-public-task"
            token_a = set_hermes_home_override(str(tmp_path / "profile-a"))
            try:
                monkeypatch.setenv("CAMOFOX_URL", url_a)
                assert json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))["success"]
                session_a = browser_camofox._sessions[task_id]

                token_b = set_hermes_home_override(str(tmp_path / "profile-b"))
                try:
                    monkeypatch.setenv("CAMOFOX_URL", url_b)
                    assert json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))["success"]
                    assert len(browser_camofox._sessions) == 2
                    session_b = next(
                        session for session in browser_camofox._sessions.values() if session is not session_a
                    )
                    assert session_a["owner_home"] != session_b["owner_home"]
                    assert session_a["endpoint"] == url_a
                    assert session_b["endpoint"] == url_b

                    assert lifecycle.cleanup_all_browsers() is True
                    assert state_a["delete_paths"] == []
                    assert state_b["delete_paths"] == [f"/sessions/{session_b['user_id']}"]
                    assert list(browser_camofox._sessions) == [task_id]
                    assert browser_camofox._sessions[task_id] is session_a
                    foreign_close = json.loads(browser_camofox.camofox_close(task_id))
                    assert foreign_close == {"success": False, "closed": False,
                                             "warning": "Camofox session belongs to another profile or endpoint"}
                    assert state_a["delete_paths"] == []
                finally:
                    reset_hermes_home_override(token_b)

                monkeypatch.setenv("CAMOFOX_URL", url_a)
                assert browser_camofox._has_session(task_id) is True
                assert lifecycle.cleanup_browser(task_id) is True
                assert state_a["delete_paths"] == [f"/sessions/{session_a['user_id']}"]
                assert browser_camofox._sessions == {}
            finally:
                reset_hermes_home_override(token_a)
    finally:
        browser_camofox._sessions.clear()
        browser_camofox._sessions.update(previous_sessions)
        browser_tool._active_sessions.clear()
        browser_tool._active_sessions.update(previous_active)


def test_camofox_close_retains_ambiguous_delete_responses_until_confirmation(monkeypatch):
    from tools import browser_camofox
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    previous_sessions = browser_camofox._sessions.copy()
    browser_camofox._sessions.clear()
    try:
        with _loopback_browser_server(
            [202, 200, 200, 204],
            empty_body_statuses={204},
            response_bodies=[{"closed": True}, {"closed": False}, {"message": "closed"}, None],
        ) as (base_url, state):
            task_id = "camofox-ambiguous-close"
            monkeypatch.setenv("CAMOFOX_URL", base_url)
            monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
            monkeypatch.setattr(
                browser_camofox,
                "load_config",
                lambda: {"browser": {"camofox": {"managed_persistence": False}}},
            )
            monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: True)
            monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)

            assert json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))["success"]
            session = browser_camofox._sessions[task_id]

            assert lifecycle.cleanup_browser(task_id) is False
            assert browser_camofox._sessions[task_id] is session
            assert lifecycle.cleanup_browser(task_id) is False
            assert browser_camofox._sessions[task_id] is session
            assert lifecycle.cleanup_browser(task_id) is False
            assert browser_camofox._sessions[task_id] is session
            assert lifecycle.cleanup_browser(task_id) is True
            assert task_id not in browser_camofox._sessions
            assert state["delete_paths"] == [
                f"/sessions/{session['user_id']}",
                f"/sessions/{session['user_id']}",
                f"/sessions/{session['user_id']}",
                f"/sessions/{session['user_id']}",
            ]
    finally:
        browser_camofox._sessions.clear()
        browser_camofox._sessions.update(previous_sessions)


def test_camofox_managed_aggregate_does_not_drop_foreign_profile(monkeypatch, tmp_path):
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from tools import browser_camofox
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    previous_sessions = browser_camofox._sessions.copy()
    previous_active = browser_tool._active_sessions.copy()
    browser_camofox._sessions.clear()
    browser_tool._active_sessions.clear()
    try:
        with (
            _loopback_browser_server([204], empty_body_statuses={204}) as (url_a, state_a),
            _loopback_browser_server([204], empty_body_statuses={204}) as (url_b, state_b),
        ):
            monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
            monkeypatch.setattr(
                browser_camofox,
                "load_config",
                lambda: {"browser": {"camofox": {"managed_persistence": True}}},
            )
            monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: True)
            monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)

            task_id = "same-managed-task"
            token_a = set_hermes_home_override(str(tmp_path / "managed-a"))
            try:
                monkeypatch.setenv("CAMOFOX_URL", url_a)
                assert json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))["success"]
                token_b = set_hermes_home_override(str(tmp_path / "managed-b"))
                try:
                    monkeypatch.setenv("CAMOFOX_URL", url_b)
                    assert json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))["success"]
                    session_b = next(
                        session for session in browser_camofox._sessions.values()
                        if session["endpoint"] == url_b
                    )
                finally:
                    reset_hermes_home_override(token_b)

                monkeypatch.setenv("CAMOFOX_URL", url_a)
                assert lifecycle.cleanup_all_browsers() is True
                assert state_a["delete_paths"] == []
                assert state_b["delete_paths"] == []
                assert len(browser_camofox._sessions) == 1
                foreign_close = json.loads(browser_camofox.camofox_close(task_id))
                assert foreign_close["success"] is False
                assert foreign_close["closed"] is False
                assert state_b["delete_paths"] == []

                token_b = set_hermes_home_override(str(tmp_path / "managed-b"))
                try:
                    monkeypatch.setenv("CAMOFOX_URL", url_b)
                    assert browser_camofox._has_session(task_id) is True
                    assert json.loads(browser_camofox.camofox_close(task_id))["closed"] is True
                    assert state_b["delete_paths"] == [f"/sessions/{session_b['user_id']}"]
                finally:
                    reset_hermes_home_override(token_b)
                assert browser_camofox._sessions == {}
            finally:
                reset_hermes_home_override(token_a)
    finally:
        browser_camofox._sessions.clear()
        browser_camofox._sessions.update(previous_sessions)
        browser_tool._active_sessions.clear()
        browser_tool._active_sessions.update(previous_active)


def test_camofox_close_uses_captured_endpoint_after_scope_lookup_flip(monkeypatch):
    """A URL change after owner lookup must not redirect the DELETE to another endpoint."""
    from tools import browser_camofox
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    previous_sessions = browser_camofox._sessions.copy()
    browser_camofox._sessions.clear()
    try:
        with (
            _loopback_browser_server([204], empty_body_statuses={204}) as (url_a, state_a),
            _loopback_browser_server([204], empty_body_statuses={204}) as (url_b, state_b),
        ):
            monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
            monkeypatch.setenv("CAMOFOX_URL", url_a)
            monkeypatch.setattr(
                browser_camofox,
                "load_config",
                lambda: {"browser": {"camofox": {"managed_persistence": False}}},
            )
            monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: True)
            monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)

            task_id = "captured-endpoint"
            assert json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))["success"]
            session = browser_camofox._sessions[task_id]

            endpoint_calls = []

            def _scope_endpoint():
                endpoint_calls.append(True)
                return url_a if len(endpoint_calls) == 1 else url_b

            monkeypatch.setattr(browser_camofox, "get_camofox_url", _scope_endpoint)
            result = json.loads(browser_camofox.camofox_close(task_id))

            assert result == {"success": True, "closed": True}
            assert endpoint_calls == [True]
            assert state_a["delete_paths"] == [f"/sessions/{session['user_id']}"]
            assert state_b["delete_paths"] == []
            assert task_id not in browser_camofox._sessions
    finally:
        browser_camofox._sessions.clear()
        browser_camofox._sessions.update(previous_sessions)


def test_camofox_cleanup_retains_identity_when_presence_lookup_fails(monkeypatch):
    """A registry read error must retain the exact session for a later retry."""
    from tools import browser_camofox
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    previous_sessions = browser_camofox._sessions.copy()
    browser_camofox._sessions.clear()
    try:
        with _loopback_browser_server([204], empty_body_statuses={204}) as (base_url, state):
            task_id = "camofox-presence-lookup-failure"
            monkeypatch.setenv("CAMOFOX_URL", base_url)
            monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
            monkeypatch.setattr(
                browser_camofox,
                "load_config",
                lambda: {"browser": {"camofox": {"managed_persistence": False}}},
            )
            monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: False)
            monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)

            assert json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))["success"]
            session = browser_camofox._sessions[task_id]
            original_has_session = browser_camofox._has_session
            lookup_calls = 0

            def _fail_first_presence_lookup(current_task_id=None):
                nonlocal lookup_calls
                lookup_calls += 1
                if lookup_calls == 1:
                    raise RuntimeError("test registry read failure")
                return original_has_session(current_task_id)

            monkeypatch.setattr(browser_camofox, "_has_session", _fail_first_presence_lookup)
            assert lifecycle.cleanup_browser(task_id) is False
            assert browser_camofox._sessions[task_id] is session
            assert state["delete_paths"] == []

            assert lifecycle.cleanup_browser(task_id) is True
            assert task_id not in browser_camofox._sessions
            assert state["delete_paths"] == [f"/sessions/{session['user_id']}"]
    finally:
        browser_camofox._sessions.clear()
        browser_camofox._sessions.update(previous_sessions)


def test_camofox_aggregate_cleanup_retains_identity_when_key_lookup_fails(monkeypatch):
    """Aggregate cleanup must fail closed when its source registry cannot be read."""
    from tools import browser_camofox
    from tools import browser_tool
    from tools import browser_tool_lifecycle as lifecycle

    previous_sessions = browser_camofox._sessions.copy()
    previous_active = browser_tool._active_sessions.copy()
    browser_camofox._sessions.clear()
    browser_tool._active_sessions.clear()
    try:
        with _loopback_browser_server([204], empty_body_statuses={204}) as (base_url, state):
            task_id = "camofox-key-lookup-failure"
            monkeypatch.setenv("CAMOFOX_URL", base_url)
            monkeypatch.delenv("BROWSER_CDP_URL", raising=False)
            monkeypatch.setattr(
                browser_camofox,
                "load_config",
                lambda: {"browser": {"camofox": {"managed_persistence": False}}},
            )
            monkeypatch.setattr(browser_tool, "_is_camofox_mode", lambda: False)
            monkeypatch.setattr(lifecycle._cdp, "_stop_cdp_supervisor", lambda _task_id: None)

            assert json.loads(browser_camofox.camofox_navigate("http://127.0.0.1/", task_id=task_id))["success"]
            session = browser_camofox._sessions[task_id]
            original_task_ids = browser_camofox._session_task_ids
            lookup_calls = 0

            def _fail_first_task_id_lookup():
                nonlocal lookup_calls
                lookup_calls += 1
                if lookup_calls == 1:
                    raise RuntimeError("test registry key read failure")
                return original_task_ids()

            monkeypatch.setattr(browser_camofox, "_session_task_ids", _fail_first_task_id_lookup)
            assert lifecycle.cleanup_all_browsers() is False
            assert browser_camofox._sessions[task_id] is session
            assert state["delete_paths"] == []

            assert lifecycle.cleanup_all_browsers() is True
            assert task_id not in browser_camofox._sessions
            assert state["delete_paths"] == [f"/sessions/{session['user_id']}"]
    finally:
        browser_camofox._sessions.clear()
        browser_camofox._sessions.update(previous_sessions)
        browser_tool._active_sessions.clear()
        browser_tool._active_sessions.update(previous_active)
