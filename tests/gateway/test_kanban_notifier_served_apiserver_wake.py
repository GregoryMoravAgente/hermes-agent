"""A served profile's multiplexed ``api_server`` Kanban wake delivers in-process, in its own scope.

A subscription whose destination is a RAW session id on the shared ``api_server`` cannot be
anchored by ``gateway.profile_routes`` (there is no chat/thread/guild discriminator), and a
platform-wide ``api_server`` route would deny the default profile's own ``api_server``
destinations. The notifier therefore authorized nothing for a served secondary profile, and — even
had it authorized — the HTTP wake self-post targets the unprefixed listener with the PRIMARY
adapter's key, so a served profile's wake turn would have resumed the session in the DEFAULT
profile's store (``/p/<profile>/`` would need that profile's own ``API_SERVER_KEY``, which a
route-only profile legitimately does not have).

Invariant: a completion for a shared-``api_server`` session wakes the served profile that owns
that exact session, in that profile's scope, without a second listener, without a secondary
credential, and without granting the profile the shared ``api_server`` generally.
"""

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway.config import Platform, PlatformConfig
from gateway.kanban_watchers_notifier import _adapter_for_subscription
from gateway.profile_routing import parse_profile_routes
from gateway.run import GatewayRunner, _profile_runtime_scope
from hermes_cli import kanban_db as kb
from hermes_cli import kanban_db_connect as kbc
from hermes_cli import kanban_db_notify as kbn
from hermes_constants import get_hermes_home
from hermes_state import SessionDB

SESSION = "20260918_033413_0665eb"      # the originating (Relay/web-UI) session id
WORKER_SESSION = "20260918_034333_945a5a"  # the dispatcher-spawned worker's own session


class RecordingApiServerAdapter:
    """Non-push (stateless) adapter double: records in-process wake turns, never sends."""

    supports_async_delivery = False

    def __init__(self):
        self.turns = []
        self.homes = []
        self.profiles = []

    async def run_internal_session_turn(self, *, session_id, text, profile, notification_category="result"):
        self.homes.append(str(get_hermes_home()))
        self.profiles.append(profile)
        self.turns.append({"session_id": session_id, "text": text, "category": notification_category})

    async def send(self, chat_id, text, metadata=None):
        from gateway.platforms.base import SendResult
        return SendResult(success=False, error="API server uses HTTP request/response, not send()")


class _FakeResponse:
    status = 200

    async def text(self):
        return ""

    async def read(self):
        return b""


class _FakePostCtx:
    def __init__(self, resp):
        self._resp = resp

    async def __aenter__(self):
        return self._resp

    async def __aexit__(self, *exc):
        return False


class _FakeHttpSession:
    """Records wake self-posts instead of sending them (proves which transport was used)."""

    calls: list = []

    def __init__(self, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def post(self, url, json=None, headers=None):
        type(self).calls.append({"url": url, "json": json, "headers": headers})
        return _FakePostCtx(_FakeResponse())


@pytest.fixture
def served(tmp_path, monkeypatch):
    """Default multiplex home serving a route-only secondary ``builder`` (no adapters, no key)."""
    root = tmp_path / ".hermes"
    (root / "profiles" / "builder").mkdir(parents=True)
    (root / "profiles" / "atlas").mkdir(parents=True)
    (root / "config.yaml").write_text("gateway:\n  multiplex_profiles: true\n", encoding="utf-8")
    (root / ".env").write_text("", encoding="utf-8")
    (root / "profiles" / "builder" / "config.yaml").write_text("{}\n", encoding="utf-8")
    # A route-only profile owns no API-server credential of its own.
    (root / "profiles" / "builder" / ".env").write_text("", encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(root))
    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "board.db"))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr("hermes_constants.get_default_hermes_root", lambda: root)
    return SimpleNamespace(root=root, builder=root / "profiles" / "builder", atlas=root / "profiles" / "atlas")


def _own_session(home: Path, session_id: str, profile_name: str) -> None:
    db = SessionDB(home / "state.db")
    try:
        db.create_session(session_id, source="webui", profile_name=profile_name)
    finally:
        db.close()


def _make_runner(*, adapter=None, routes=None, builder_adapters=None):
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._running = True
    runner.adapters = {Platform.API_SERVER: adapter or RecordingApiServerAdapter()}
    runner._profile_adapters = {"builder": builder_adapters or {}, "atlas": {}}
    runner._profile_failed_platforms = {}
    runner._primary_profile_name = "default"
    runner._kanban_notifier_profile = "default"
    runner._kanban_sub_fail_counts = {}
    runner._kanban_dispatcher_lock_handle = object()
    runner.config = SimpleNamespace(
        multiplex_profiles=True, profile_routes=parse_profile_routes(routes or []))
    return runner


def _subscription(*, chat_id=SESSION, profile="builder"):
    """One completed card whose origin is the raw session id, as the Relay origin creates it."""
    with kbc.connect() as conn:
        task = kb.create_task(conn, title="relay origin", assignee="builder", session_id=WORKER_SESSION)
        kbn.add_notify_sub(conn, task_id=task, platform="api_server", chat_id=chat_id,
                           chat_type="dm", notifier_profile=profile, delivery_mode="notify+wake")
        kb.complete_task(conn, task, summary="done once")
    return task


def _unseen(task, chat_id=SESSION):
    with kbc.connect() as conn:
        return kbn.unseen_events_for_sub(conn, task_id=task, platform="api_server", chat_id=chat_id,
                                         kinds=["completed"])[1]


async def _run_one_notifier_tick(monkeypatch, runner):
    real_sleep = asyncio.sleep

    async def fake_sleep(delay):
        if delay == 5:
            return
        runner._running = False
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    await runner._kanban_notifier_watcher(interval=1)


def _api_sub(chat_id=SESSION, profile="builder"):
    return {"task_id": "t_owned", "platform": "api_server", "chat_id": chat_id, "chat_type": "dm",
            "thread_id": "", "notifier_profile": profile, "delivery_metadata": None}


def test_served_profile_wake_runs_in_process_only_for_the_session_it_owns(served, monkeypatch):
    """Owned session → in-process wake in the owner's scope; everything else fails closed; the
    default profile keeps its HTTP self-post."""
    import aiohttp

    _own_session(served.builder, SESSION, "builder")
    _FakeHttpSession.calls = []
    monkeypatch.setattr(aiohttp, "ClientSession", _FakeHttpSession)
    kb.init_db()
    adapter = RecordingApiServerAdapter()
    runner = _make_runner(adapter=adapter)
    task = _subscription()

    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))

    assert [turn["session_id"] for turn in adapter.turns] == [SESSION]  # the origin, not the worker
    assert task in adapter.turns[0]["text"] and "done once" in adapter.turns[0]["text"]
    # Under the OWNING profile's scope, told which profile the authorization proved.
    assert adapter.homes == [str(served.builder)]
    assert adapter.profiles == ["builder"]
    # No HTTP self-post (so no secondary API_SERVER_KEY), one shared adapter, no borrowing.
    assert _FakeHttpSession.calls == []
    assert runner.adapters[Platform.API_SERVER] is adapter
    assert runner._profile_adapters["builder"] == {}
    assert _unseen(task) == []

    # Ownership of the exact session — not the platform — is the only proof.
    resolve = lambda sub, profile="builder", **kw: _adapter_for_subscription(
        _make_runner(**kw), Platform.API_SERVER, sub, profile)
    assert resolve(_api_sub(chat_id="20260918_051500_deadbe")) is None           # unknown session
    assert resolve(_api_sub(), profile="atlas") is None                          # not the owner
    assert resolve(_api_sub(), profile="ghost") is None                          # unserved profile
    # An adapter on ANOTHER platform is not a boundary for this one (#115460); an own api_server
    # adapter is — the primary never stands in for a credential the profile holds itself.
    assert resolve(_api_sub(), builder_adapters={Platform.DISCORD: object()}) is not None
    own_api = object()
    assert resolve(_api_sub(), builder_adapters={Platform.API_SERVER: own_api}) is own_api
    _own_session(served.atlas, "stamped-elsewhere", "builder")                   # foreign store
    assert resolve(_api_sub(chat_id="stamped-elsewhere")) is None
    # A row in the served store stamped for another profile is not ownership either.
    _own_session(served.builder, "atlas-row", "atlas")
    assert resolve(_api_sub(chat_id="atlas-row")) is None

    # Control: the default profile's own api_server subscription still HTTP self-posts.
    _own_session(served.root, "default-origin", "default")
    default_adapter = RecordingApiServerAdapter()
    default_adapter._api_key, default_adapter._host = "k" * 20, "127.0.0.1"
    default_adapter._port, default_adapter._model_name = 8642, "hermes"
    runner = _make_runner(adapter=default_adapter)
    task = _subscription(chat_id="default-origin", profile="default")
    asyncio.run(_run_one_notifier_tick(monkeypatch, runner))
    assert default_adapter.turns == []
    assert [c["headers"]["X-Hermes-Session-Id"] for c in _FakeHttpSession.calls] == ["default-origin"]
    assert _FakeHttpSession.calls[0]["url"].endswith("/v1/chat/completions")
    assert _unseen(task, chat_id="default-origin") == []


def test_internal_session_turn_targets_the_live_session_under_the_owner_profile(served, monkeypatch):
    """Adapter-level: the in-process turn binds the proven profile, adopts the compression tip,
    and never runs against a session the owner's store does not hold."""
    from gateway.platforms.api_server import APIServerAdapter, _api_request_profile

    parent, tip = "20260918_010000_parent", "20260918_020000_tip"
    db = SessionDB(served.builder / "state.db")
    try:
        db.create_session(parent, source="webui", profile_name="builder")
        db.append_message(parent, "user", "old turn")
        db.end_session(parent, "compression")
        db.create_session(tip, source="webui", profile_name="builder", parent_session_id=parent)
        db.append_message(tip, "user", "live turn")
        assert db.resolve_resume_session_id(parent) == tip, "fixture must produce a live tip"
    finally:
        db.close()

    adapter = APIServerAdapter(PlatformConfig(enabled=True))
    # A model_routes alias for the virtual model must apply to the wake turn as it does to the
    # HTTP self-post's /v1/chat/completions turn.
    aliased_route = {"provider": "custom", "model": "wake-model", "base_url": "http://r"}
    adapter._model_routes = {adapter._model_name: aliased_route}
    seen = {}

    async def fake_run_agent(**kwargs):
        seen.update(kwargs)
        seen["home"] = str(get_hermes_home())
        seen["request_profile"] = _api_request_profile.get()
        return {}, {}

    monkeypatch.setattr(adapter, "_run_agent", fake_run_agent)

    with _profile_runtime_scope(served.builder):
        asyncio.run(adapter.run_internal_session_turn(session_id=parent, text="wake", profile="builder"))

    assert seen["session_id"] == tip  # the live continuation, not the retired parent slice
    assert "live turn" in str(seen["conversation_history"])
    assert seen["user_message"] == "wake"
    assert seen["session_history_delivery"] == "1"
    assert seen["route"] == aliased_route  # resolved like the HTTP wake path, not route=None
    assert seen["home"] == str(served.builder)
    assert seen["request_profile"] == "builder"
    assert _api_request_profile.get() is None  # binding restored, never leaked

    seen.clear()
    with _profile_runtime_scope(served.builder):
        with pytest.raises(RuntimeError):  # not in the owner's store → caller rewinds
            asyncio.run(adapter.run_internal_session_turn(session_id="not-a-session", text="wake",
                                                          profile="builder"))
        with pytest.raises(ValueError):  # the proof is the caller's; never derived here
            asyncio.run(adapter.run_internal_session_turn(session_id=tip, text="wake", profile=""))
    assert seen == {}


@pytest.mark.asyncio
async def test_persisted_nonpush_wake_carries_occurrence_into_the_api_authority(served, monkeypatch):
    """A real claimed Kanban row must carry its occurrence through the stateless wake seam.

    The API adapter is the real non-push adapter and the subscription/event rows are real SQLite
    rows.  The final provider turn is deliberately stopped at the existing API-server wake seam:
    this test is the consuming contract for the source-owned occurrence/request identity that the
    next SessionAuthority admission must receive.  A test double is used only for the external
    HTTP/provider boundary, never for the persisted notifier or adapter route.
    """
    from gateway.kanban_watchers_notifier import _KanbanNotification, _kanban_wake_occurrence
    from gateway.platforms.api_server import APIServerAdapter
    from gateway.session_automation_custody import kanban_occurrence_metadata, route_wake_identity
    import gateway.wake as wake_mod

    monkeypatch.setenv("HERMES_KANBAN_DB", str(served.root / "board.db"))
    kb.init_db()
    with kbc.connect() as conn:
        task_id = kb.create_task(
            conn, title="persisted api wake", assignee="builder", session_id=WORKER_SESSION,
            initial_status="running",
        )
        kbn.add_notify_sub(
            conn, task_id=task_id, platform="api_server", chat_id=SESSION,
            chat_type="dm", delivery_mode="wake",
        )
        assert kb.complete_task(conn, task_id, summary="persisted handoff")
        sub = kbn.list_notify_subs(conn, task_id=task_id)[0]
        old_cursor, cursor, events = kbn.claim_unseen_events_for_sub(
            conn, task_id=task_id, platform="api_server", chat_id=SESSION,
            thread_id="", kinds=["completed"],
        )
        task = kb.get_task(conn, task_id)

    assert task is not None and events
    delivery = {
        "sub": sub, "old_cursor": old_cursor, "cursor": cursor,
        "events": events, "task": task, "board": "default",
    }
    raw_occurrence = _kanban_wake_occurrence(delivery, events)
    occurrence = kanban_occurrence_metadata(
        {"kanban_occurrence": raw_occurrence}
    )["kanban_occurrence"]
    adapter = APIServerAdapter(PlatformConfig(enabled=True))
    calls = []

    async def capture_self_post(adapter, *, text, session_id, wake_metadata=None, **kwargs):
        calls.append({
            "session_id": session_id, "text": text,
            "wake_metadata": wake_metadata, "extra": kwargs,
        })

    monkeypatch.setattr(wake_mod, "_self_post_chat_completion", capture_self_post)
    notification = _KanbanNotification(
        _make_runner(adapter=adapter), delivery, platform_cls=Platform, sub_fail_counts={},
    )
    notification.adapter = adapter
    notification.is_push_adapter = False
    notification.build_wake_text()
    notification.session_key = SESSION
    notification.synth = "same rendered persisted wake"
    notification.wake_occurrence = occurrence
    try:
        await notification.wake()
    finally:
        adapter._response_store.close()
        adapter._run_idempotency_store.close()

    assert len(calls) == 1
    assert calls[0]["session_id"] == SESSION
    assert calls[0]["wake_metadata"] == {"kanban_occurrence": occurrence}

    first = route_wake_identity(
        "kanban-wake", SESSION, "same rendered persisted wake",
        {"kanban_occurrence": occurrence},
    )
    retry = route_wake_identity(
        "kanban-wake", SESSION, "same rendered persisted wake",
        {"kanban_occurrence": occurrence},
    )
    distinct = route_wake_identity(
        "kanban-wake", SESSION, "same rendered persisted wake",
        {"kanban_occurrence": {**raw_occurrence, "event_ids": [events[0].id + 1]}},
    )
    assert first == retry
    assert first != distinct


def test_persisted_claimed_event_backlog_keeps_all_occurrence_ids_over_256(tmp_path, monkeypatch):
    """The source DB claim is one durable occurrence; identity must not truncate it at 256."""
    from gateway.kanban_watchers_notifier import _kanban_wake_occurrence
    from gateway.session_automation_custody import route_wake_identity

    monkeypatch.setenv("HERMES_KANBAN_DB", str(tmp_path / "backlog.db"))
    kb.init_db()
    with kbc.connect() as conn:
        task_id = kb.create_task(
            conn, title="large persisted backlog", assignee="builder", initial_status="running",
        )
        kbn.add_notify_sub(
            conn, task_id=task_id, platform="api_server", chat_id=SESSION,
            chat_type="dm", delivery_mode="wake",
        )
        for index in range(257):
            kb.add_comment(conn, task_id, "fixture", f"event {index}")
        old_cursor, cursor, events = kbn.claim_unseen_events_for_sub(
            conn, task_id=task_id, platform="api_server", chat_id=SESSION,
            thread_id="", kinds=["commented"],
        )

    assert len(events) == 257
    delivery = {
        "sub": {"task_id": task_id}, "old_cursor": old_cursor,
        "cursor": cursor, "board": "default",
    }
    occurrence = _kanban_wake_occurrence(delivery, events)
    assert occurrence["event_ids"] == [event.id for event in events]
    # This is deliberately the current RED boundary: the durable claim is complete, but the
    # current identity normalizer rejects the source-owned 257-event occurrence at 256.
    identity = route_wake_identity(
        "kanban-wake", SESSION, "large persisted backlog",
        {"kanban_occurrence": occurrence},
    )
    assert identity


def test_persisted_automation_recovery_refuses_missing_generation_witness(owner):
    """A started persisted automation row cannot mint a successor from an incomplete descriptor."""
    from pathlib import Path

    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.runtime_ownership import process_ownership
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_state_runtime import admit_session_input, claim_session_input, RuntimeStoreError

    coordinator = RuntimeCustodyCoordinator(
        process_instance=owner.instance_id, authority_epoch=owner.epoch, fence=RetirementFence(),
    )
    owner.runner.runtime_custody = coordinator
    session_id, request_id = "automation-session", "persisted-wake"
    owner.db.create_session(session_id, source="api_server")
    root = owner._preservation_automation_root(
        request_id=request_id, principal_id="automation:kanban", session_id=session_id,
        source_identity="kanban-source", delivery_id="delivery-1",
    )
    descriptor = owner._preservation_automation_descriptor(
        root, request_id=request_id, principal_id="automation:kanban", session_id=session_id,
        source_identity="kanban-source", delivery_id="delivery-1",
    )
    descriptor.pop("close_generation")
    queued = admit_session_input(
        owner.db, epoch=owner.epoch, principal_id="automation:kanban", session_id=session_id,
        request_id=request_id,
        payload={"native_text_v1": {"automation": {"custody": descriptor}}},
    )
    row = claim_session_input(owner.db, epoch=owner.epoch, session_id=session_id, head=queued["admission_id"])
    assert isinstance(row, dict) and row["status"] == "started" and isinstance(row["generation"], int)
    home = Path(owner.db.db_path).parent.resolve()
    process_ownership.reserve([home])
    try:
        with pytest.raises(RuntimeStoreError, match="preservation_owner_unmapped"):
            owner._preservation_recover_automation(row)
    finally:
        process_ownership.release(home)
        owner._preservation_release_automation_unaccepted(None, root)
