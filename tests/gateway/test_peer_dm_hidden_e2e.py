"""E2E: ``hermes peer dm`` against a REAL api_server gateway whose canonical
Bot Chat session is HIDDEN (issue #91583).

Two real HERMES homes in spirit: the "peer" side is a real
:class:`APIServerAdapter` bound to a real loopback TCP socket over a real
SQLite ``state.db`` (its own tmp HERMES_HOME) containing a hidden
``Bot Chat`` row — exactly what Bot Mode leaves behind. The "local" side is
the stock ``hermes peer dm`` client code (``hermes_cli.subcommands.peer``),
untouched, talking real HTTP with the real API key auth.

Only the model turn itself is stubbed (``_run_agent``); every HTTP handler,
auth check, and SQLite query in the resolution chain is the real thing.
"""

import asyncio
import json
from types import SimpleNamespace

import pytest
import pytest_asyncio
from aiohttp import web

from gateway.platforms.api_server import APIServerAdapter
from hermes_cli.subcommands import peer as peer_cmd

API_KEY = "sk-peer-e2e-key-123456"


def _build_app(adapter: APIServerAdapter) -> web.Application:
    app = web.Application()
    app.router.add_get("/api/sessions", adapter._handle_list_sessions)
    app.router.add_post("/api/sessions", adapter._handle_create_session)
    app.router.add_post("/api/sessions/{session_id}/chat", adapter._handle_session_chat)
    return app


@pytest_asyncio.fixture()
async def peer_gateway(tmp_path, monkeypatch):
    """A real runtime-owned api_server gateway (legacy session route) on a real socket."""
    from tests.gateway.conftest import runtime_owned_api

    async with runtime_owned_api(tmp_path, monkeypatch, api_key=API_KEY, legacy=True) as adapter:
        # The test covers the legacy API session-chat fallback: the runtime is still genuine
        # (process ownership, descriptor and custody coordinator), while no SessionAuthority
        # owns the synthetic Bot Mode row. Wire the runtime's real DB into the adapter so the
        # fallback does not reopen an unowned test database.
        db = adapter.gateway_runner.session_store._db
        adapter._session_db = db
        # ``runtime_owned_api(legacy=True)`` clears the authority registry to select this
        # fallback, but the DB resolver treats an explicitly present ``None`` registry as an
        # authority-only runtime. Remove that marker for the supported bare-store path.
        adapter.gateway_runner.__dict__.pop("session_authorities", None)

        # Bot Mode's footprint: the canonical Bot Chat exists and is HIDDEN.
        hidden_id = db.create_session("botchat_hidden_1", "gateway_botmode")
        assert db.set_session_title(hidden_id, "Bot Chat")
        assert db.set_session_hidden(hidden_id, True)
        # And a decoy visible session, so the listing isn't trivially empty.
        other_id = db.create_session("ordinary_1", "api_server")
        db.set_session_title(other_id, "Ordinary Chat")

        async def fake_run_agent(user_message, **kwargs):
            return (
                {
                    "final_response": f"e2e reply to: {user_message}",
                    "session_id": kwargs.get("session_id"),
                },
                {},
            )

        adapter._run_agent = fake_run_agent

        server = web.AppRunner(_build_app(adapter))
        await server.setup()
        site = web.TCPSite(server, "127.0.0.1", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        try:
            yield SimpleNamespace(
                url=f"http://127.0.0.1:{port}",
                db=db,
                hidden_id=hidden_id,
            )
        finally:
            await server.cleanup()


@pytest.mark.asyncio
async def test_peer_dm_reaches_hidden_canonical_bot_chat_e2e(peer_gateway, monkeypatch, capsys):
    """The stock peer dm client resolves the HIDDEN canonical Bot Chat on a
    real gateway, runs the chat turn on it, and creates no duplicate row."""
    monkeypatch.setattr(peer_cmd, "_load_peers", lambda: {"spark": {"url": peer_gateway.url}})
    monkeypatch.setattr(peer_cmd, "_peer_secret", lambda name: API_KEY)

    rc = await asyncio.to_thread(
        peer_cmd.cmd_peer,
        SimpleNamespace(peer_action="dm", target="spark", message="disk status?", json=True))

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["reply"] == "e2e reply to: disk status?"
    assert payload["session_id"] == peer_gateway.hidden_id

    # The real DB still holds exactly ONE Bot Chat row and it is still hidden.
    rows = peer_gateway.db.list_sessions_rich(limit=200, include_hidden=True)
    bot_chats = [r for r in rows if (r.get("title") or "").strip() == "Bot Chat"]
    assert [r["id"] for r in bot_chats] == [peer_gateway.hidden_id]
    assert bool(bot_chats[0].get("hidden"))


@pytest.mark.asyncio
async def test_hidden_lookup_requires_title_filter_e2e(peer_gateway):
    """Server-side bound: include_hidden without a title filter stays a
    visible-only listing (no blanket hidden exposure on this surface)."""
    import urllib.request

    req = urllib.request.Request(
        f"{peer_gateway.url}/api/sessions?limit=200&include_hidden=1",
        headers={"Authorization": f"Bearer {API_KEY}"},
    )
    def _read_listing():
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())

    listing = await asyncio.to_thread(_read_listing)
    ids = [s["id"] for s in listing["data"]]
    assert peer_gateway.hidden_id not in ids
    assert "ordinary_1" in ids


@pytest_asyncio.fixture()
async def peer_gateway_compressed_hidden(peer_gateway):
    """Aged Bot Mode footprint: the hidden canonical Bot Chat has a live
    compression tip (issue #106165). The tip is untitled and hidden via the
    lineage; the title lives only on the ended root."""
    db = peer_gateway.db
    tip_id = db.create_session(
        "botchat_tip_1", "gateway_botmode", parent_session_id=peer_gateway.hidden_id)
    db.end_session(peer_gateway.hidden_id, "compression")
    db.set_session_hidden(peer_gateway.hidden_id, True)  # lineage-hide root+tip
    peer_gateway.tip_id = tip_id
    return peer_gateway


@pytest.mark.asyncio
async def test_peer_dm_reaches_compressed_hidden_bot_chat_e2e(
        peer_gateway_compressed_hidden, monkeypatch, capsys):
    """Hidden canonical Bot Chat under compression: the peer lookup must still
    resolve (to the live tip) instead of missing it and colliding with
    UNIQUE(title) on create (HTTP 400, issue #106165)."""
    gw = peer_gateway_compressed_hidden
    monkeypatch.setattr(peer_cmd, "_load_peers", lambda: {"spark": {"url": gw.url}})
    monkeypatch.setattr(peer_cmd, "_peer_secret", lambda name: API_KEY)

    rc = await asyncio.to_thread(
        peer_cmd.cmd_peer,
        SimpleNamespace(peer_action="dm", target="spark", message="disk status?", json=True))

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["reply"] == "e2e reply to: disk status?"
    assert payload["session_id"] == gw.tip_id

    # No duplicate "Bot Chat" row was minted; the title still lives on the root.
    assert gw.db.get_session_by_title("Bot Chat")["id"] == gw.hidden_id
