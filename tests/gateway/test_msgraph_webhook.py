"""Tests for the Microsoft Graph webhook adapter."""

import asyncio
import json

import pytest
import pytest_asyncio

from gateway.config import GatewayConfig, Platform, PlatformConfig, _apply_env_overrides
from gateway.platforms.msgraph_webhook import AIOHTTP_AVAILABLE, MSGraphWebhookAdapter


def _make_adapter(**extra_overrides) -> MSGraphWebhookAdapter:
    extra = {
        "host": "127.0.0.1",
        "client_state": "expected-client-state",
        "accepted_resources": ["communications/onlineMeetings"],
    }
    extra.update(extra_overrides)
    return MSGraphWebhookAdapter(PlatformConfig(enabled=True, extra=extra))


@pytest_asyncio.fixture
async def authority_backed_adapter():
    """Build the same runner-owned boundary that production ingress receives."""
    from pathlib import Path
    from gateway.run import GatewayRunner
    from gateway.run_runtime import _authority_tasks
    from gateway.session_authority import initialize_session_authority
    from gateway.session_cron import owner_for_home, unbind_owner

    runner = GatewayRunner()
    authority = None
    adapters: list[MSGraphWebhookAdapter] = []
    try:
        authority = await initialize_session_authority(
            runner, profile_id="default", instance_id="msgraph-webhook-fixture"
        )

        def build(**extra_overrides) -> MSGraphWebhookAdapter:
            adapter = _make_adapter(**extra_overrides)
            adapter.gateway_runner = runner
            runner.adapters[Platform.MSGRAPH_WEBHOOK] = adapter
            runner._wire_adapter_handlers(adapter)
            adapters.append(adapter)
            return adapter

        yield build
    finally:
        def _task_values(container):
            if container is None:
                return ()
            values = container.values() if hasattr(container, "values") else container
            return tuple(task for task in values if task is not None)

        def _adapter_tasks(adapter):
            tasks = []
            for name in (
                "_background_tasks", "_session_tasks", "_pending_text_batch_tasks",
                "_expected_cancelled_tasks",
            ):
                tasks.extend(_task_values(getattr(adapter, name, ())))
            return tuple(dict.fromkeys(tasks))

        def _live_tasks(tasks):
            return tuple(task for task in dict.fromkeys(tasks) if not task.done())

        def _task_failures(tasks):
            return tuple(
                task.exception()
                for task in dict.fromkeys(tasks)
                if task.done() and not task.cancelled() and task.exception() is not None
            )

        async def _cancel_and_join(tasks, label):
            failures = _task_failures(tasks)
            if failures:
                raise AssertionError(f"{label} task failed during teardown")
            live = _live_tasks(tasks)
            for task in live:
                task.cancel()
            if not live:
                return
            done, pending = await asyncio.wait(live, timeout=5.0)
            failures = [
                task.exception()
                for task in done
                if not task.cancelled() and task.exception() is not None
            ]
            if pending or failures:
                details = []
                if pending:
                    details.append(f"{len(pending)} live")
                if failures:
                    details.append(f"{len(failures)} failed")
                raise AssertionError(
                    f"{label} task settlement unproven: {', '.join(details)}"
                )

        adapter_tasks = tuple(
            task for adapter in adapters for task in _adapter_tasks(adapter)
        )
        for adapter in adapters:
            await adapter.cancel_background_tasks()
        # ``cancel_background_tasks`` may clear its tracking set after a bounded
        # timeout.  Retain the pre-cancel task objects and prove they settled before
        # allowing ownership or DB handles to be released.
        await _cancel_and_join(adapter_tasks, "adapter")

        # ``initialize_session_authority`` binds the process-global owner before it
        # returns.  Keep teardown authenticated through the public owner lookup so an
        # init failure after binding is retired too; never clear the registry directly.
        candidate = authority or getattr(runner, "session_authority", None)
        bound_authority = None
        owner_home = None
        if candidate is not None:
            owner_home = Path(candidate.db.db_path).resolve().parent
        else:
            db = getattr(runner.session_store, "_db", None)
            db_path = getattr(db, "db_path", None)
            if db_path is not None:
                owner_home = Path(db_path).resolve().parent
        if owner_home is not None:
            bound = owner_for_home(owner_home)
            bound_authority = bound[0] if bound is not None else None

        cleanup_authority = bound_authority or candidate
        if authority is not None and cleanup_authority is not authority:
            raise AssertionError("fixture authority is no longer canonically bound")

        if cleanup_authority is not None:
            cleanup_authority.retiring = True
            from gateway.session_runtime_workers import join_authority_work

            try:
                await join_authority_work(cleanup_authority, timeout=5.0)
            except TimeoutError as exc:
                raise AssertionError(
                    "canonical authority work did not settle before teardown"
                ) from exc

            # The canonical inventory includes live sessions, _bot_receipt_tasks,
            # _mutation_tasks, and _bot_mailbox_operations through _authority_tasks.
            # Cancel only after the cooperative join, then retain ownership if any
            # family remains live or reports a failure.
            for _ in range(2):
                authority_tasks = _authority_tasks(cleanup_authority)
                if _task_failures(authority_tasks):
                    raise AssertionError("canonical authority task failed during teardown")
                if not _live_tasks(authority_tasks):
                    break
                await _cancel_and_join(authority_tasks, "authority")
            remaining_authority_tasks = _authority_tasks(cleanup_authority)
            if _task_failures(remaining_authority_tasks) or _live_tasks(remaining_authority_tasks):
                raise AssertionError("canonical authority task settlement unproven")

            pending_fields = (
                "waiters", "events", "native_waiters", "pending_results",
                "pending_deliveries", "pending_stops", "pending_steers", "steer_transfers",
                "launch_sources", "delivered_stops", "adopted", "cancel_obligations",
                "executing", "pending_closes", "rotated_owners", "_preservation_handles",
                "_preservation_children", "_preservation_sidecars",
                "_preservation_delivery_retries",
                "_preservation_pending_roots", "_preservation_scheduler_handles",
                "_preservation_scheduler_retry_handles", "_preservation_claiming",
                "_preservation_marker_watchers", "_preservation_automation_roots",
                "_preservation_automation_children", "_preservation_automation_bindings",
                "_preservation_automation_rehomes", "_preservation_operations",
                "_external_process_finalizers",
            )
            pending = [name for name in pending_fields if getattr(cleanup_authority, name, None)]
            pending.extend(
                "live_session_tasks"
                for session in cleanup_authority.sessions.values()
                if getattr(session, "task", None) is not None and not session.task.done()
            )
            if pending:
                raise AssertionError(
                    "fixture authority still owns accepted work: " + ", ".join(pending)
                )

            unbind_owner(cleanup_authority)
            if owner_home is None or owner_for_home(owner_home) is not None:
                raise AssertionError("fixture authority owner remained bound after teardown")
            cleanup_authority.retired = True
        elif owner_home is not None:
            raise AssertionError("fixture authority owner could not be authenticated for teardown")

        runner.session_store.close_all_db_handles()
        runner.close_all_session_db_handles()


class _FakeRequest:
    def __init__(
        self,
        *,
        query=None,
        json_payload=None,
        raw_body: bytes | None = None,
        content_length: int | None = None,
        remote="127.0.0.1",
    ):
        self.query = query or {}
        self._json_payload = json_payload
        self._raw_body = raw_body
        self.content_length = content_length
        self.remote = remote

    async def json(self):
        if isinstance(self._json_payload, Exception):
            raise self._json_payload
        return self._json_payload

    async def read(self):
        if self._raw_body is not None:
            return self._raw_body
        return json.dumps(self._json_payload or {}).encode("utf-8")


class TestMSGraphWebhookConfig:
    def test_gateway_config_accepts_msgraph_webhook_platform(self):
        config = GatewayConfig.from_dict(
            {
                "platforms": {
                    "msgraph_webhook": {
                        "enabled": True,
                        "extra": {"client_state": "expected"},
                    }
                }
            }
        )

        assert Platform.MSGRAPH_WEBHOOK in config.platforms
        assert Platform.MSGRAPH_WEBHOOK in config.get_connected_platforms()


class TestMSGraphValidationHandshake:


    @pytest.mark.anyio
    async def test_connect_allows_loopback_without_source_allowlist(self):
        if not AIOHTTP_AVAILABLE:
            pytest.skip("aiohttp not installed")
        adapter = _make_adapter(host="127.0.0.1", port=0, allowed_source_cidrs=[])
        try:
            connected = await adapter.connect()
            assert connected is True
            assert adapter.is_connected is True
        finally:
            await adapter.disconnect()

    @pytest.mark.anyio
    async def test_validation_token_echo_on_get(self):
        adapter = _make_adapter()
        resp = await adapter._handle_validation(
            _FakeRequest(query={"validationToken": "abc123"})
        )
        assert resp.status == 200
        assert resp.text == "abc123"
        assert resp.content_type == "text/plain"


class TestMSGraphNotifications:
    @pytest.mark.anyio
    async def test_missing_client_state_is_auth_rejected(self):
        adapter = _make_adapter(client_state=None)
        payload = {
            "value": [
                {
                    "id": "notif-no-client-state",
                    "subscriptionId": "sub-1",
                    "changeType": "updated",
                    "resource": "communications/onlineMeetings/meeting-1",
                }
            ]
        }
        resp = await adapter._handle_notification(_FakeRequest(json_payload=payload))
        assert resp.status == 403

    @pytest.mark.anyio
    async def test_valid_notification_accepted_and_scheduled(self, authority_backed_adapter):
        adapter = authority_backed_adapter()
        scheduled: list[tuple[dict, object]] = []

        async def _capture(notification, event):
            scheduled.append((notification, event))

        adapter.set_notification_scheduler(_capture)
        payload = {
            "value": [
                {
                    "id": "notif-1",
                    "subscriptionId": "sub-1",
                    "changeType": "updated",
                    "resource": "communications/onlineMeetings/meeting-1",
                    "clientState": "expected-client-state",
                    "resourceData": {"id": "meeting-1"},
                }
            ]
        }

        resp = await adapter._handle_notification(_FakeRequest(json_payload=payload))
        # Success is 202 with empty body: internal counters must not leak to
        # the wire. Counters are still observable via /health.
        assert resp.status == 202
        assert resp.body is None or not resp.body

        await asyncio.sleep(0.05)

        assert len(scheduled) == 1
        notification, event = scheduled[0]
        assert notification["id"] == "notif-1"
        assert event.source.platform == Platform.MSGRAPH_WEBHOOK
        assert event.source.chat_type == "webhook"
        assert event.message_id == "id:notif-1"

    @pytest.mark.anyio
    async def test_scheduled_notification_without_authority_fails_closed(self):
        adapter = _make_adapter()
        assert adapter._message_handler is None
        adapter.set_notification_scheduler(lambda notification, event: None)
        payload = {
            "value": [
                {
                    "id": "notif-no-authority",
                    "subscriptionId": "sub-1",
                    "changeType": "updated",
                    "resource": "communications/onlineMeetings/meeting-1",
                    "clientState": "expected-client-state",
                }
            ]
        }

        resp = await adapter._handle_notification(_FakeRequest(json_payload=payload))
        assert resp.status == 503

    @pytest.mark.anyio
    async def test_oversized_notification_rejected_by_content_length(self):
        adapter = _make_adapter(max_body_bytes=100)
        payload = {
            "value": [
                {
                    "id": "notif-oversized",
                    "subscriptionId": "sub-1",
                    "changeType": "updated",
                    "resource": "communications/onlineMeetings/meeting-1",
                    "clientState": "expected-client-state",
                }
            ]
        }

        resp = await adapter._handle_notification(
            _FakeRequest(json_payload=payload, content_length=101)
        )

        assert resp.status == 413


    @pytest.mark.anyio
    async def test_non_ascii_client_state_rejected_without_raising(self):
        """A non-ASCII clientState (attacker-controlled request body) must be
        rejected with 403, not crash the handler: hmac.compare_digest raises
        TypeError on a str containing non-ASCII characters."""
        adapter = _make_adapter()
        payload = {
            "value": [
                {
                    "id": "notif-nonascii",
                    "subscriptionId": "sub-1",
                    "changeType": "updated",
                    "resource": "communications/onlineMeetings/meeting-x",
                    "clientState": "ské-not-the-secret",
                }
            ]
        }
        response = await adapter._handle_notification(
            _FakeRequest(json_payload=payload)
        )
        assert response.status == 403


    @pytest.mark.anyio
    async def test_resource_patterns_accept_leading_slash(self, authority_backed_adapter):
        adapter = authority_backed_adapter(accepted_resources=["/communications/onlineMeetings"])
        adapter.set_notification_scheduler(lambda notification, event: None)
        payload = {
            "value": [
                {
                    "id": "notif-slash",
                    "subscriptionId": "sub-1",
                    "changeType": "updated",
                    "resource": "communications/onlineMeetings/meeting-4",
                    "clientState": "expected-client-state",
                }
            ]
        }

        resp = await adapter._handle_notification(_FakeRequest(json_payload=payload))
        assert resp.status == 202


class TestMSGraphSourceIPAllowlist:
    @pytest.mark.anyio
    async def test_public_bind_without_allowlist_fails_closed(self):
        """Public binds must not accept requests until a source allowlist is configured."""
        adapter = _make_adapter(host="0.0.0.0", allowed_source_cidrs=[])
        payload = {
            "value": [
                {
                    "id": "notif-ip",
                    "resource": "communications/onlineMeetings/m",
                    "clientState": "expected-client-state",
                }
            ]
        }
        resp = await adapter._handle_notification(
            _FakeRequest(json_payload=payload, remote="203.0.113.99")
        )
        assert resp.status == 403

    @pytest.mark.anyio
    async def test_loopback_bind_without_allowlist_still_accepts_local_requests(
        self, authority_backed_adapter
    ):
        """Loopback-only listeners may rely on local proxying/tunnels instead of CIDRs."""
        adapter = authority_backed_adapter(host="127.0.0.1", allowed_source_cidrs=[])
        adapter.set_notification_scheduler(lambda notification, event: None)
        payload = {
            "value": [
                {
                    "id": "notif-ip-local",
                    "resource": "communications/onlineMeetings/m",
                    "clientState": "expected-client-state",
                }
            ]
        }
        resp = await adapter._handle_notification(
            _FakeRequest(json_payload=payload, remote="127.0.0.1")
        )
        assert resp.status == 202

    @pytest.mark.anyio
    async def test_post_from_disallowed_ip_rejected(self):
        adapter = _make_adapter(allowed_source_cidrs=["10.0.0.0/8"])
        payload = {
            "value": [
                {
                    "id": "notif-ip-bad",
                    "resource": "communications/onlineMeetings/m",
                    "clientState": "expected-client-state",
                }
            ]
        }
        resp = await adapter._handle_notification(
            _FakeRequest(json_payload=payload, remote="203.0.113.99")
        )
        assert resp.status == 403


