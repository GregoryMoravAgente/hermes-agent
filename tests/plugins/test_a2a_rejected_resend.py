"""A forwarded message refused by an admission guard stays refused on its exact resend."""
import asyncio

from gateway.config import PlatformConfig
from plugins.platforms.a2a import protocol
from plugins.platforms.a2a.adapter import A2AAdapter


def test_resend_of_an_over_budget_message_is_rejected_again(monkeypatch):
    monkeypatch.setenv("A2A_MAX_PINGPONG_TURNS", "1")
    monkeypatch.setattr(protocol, "persist_message", lambda *a, **k: None)
    monkeypatch.setattr(protocol, "logged_input", lambda *a, **k: False)
    adapter = A2AAdapter(PlatformConfig(enabled=True, extra={
        "agents": {"dev": {"profile": "dev", "tenant": "dev"}}
    }))
    agent = adapter._agents["dev"]
    calls = []

    def fake_forward(agent_arg, peer, context_id, framed_text, *, input_id):
        calls.append(input_id)
        return "dev reply", protocol.STATE_COMPLETED

    adapter._forward_to_profile = fake_forward  # type: ignore[method-assign]

    def prepare(message):
        async def on_owner_loop():
            adapter._loop = asyncio.get_running_loop()
            adapter._message_handler = object()
            return adapter._prepare_task({"tenant": "dev", "message": dict(message)}, "peer-x", agent=agent)
        return asyncio.run(on_owner_loop())

    first = protocol.text_message(protocol.ROLE_USER, "hello", context_id="ctx-cap")
    over = protocol.text_message(protocol.ROLE_USER, "again", context_id="ctx-cap")
    states = [prepare(m)[0]["status"]["state"] for m in (first, over, over)]
    assert states == [protocol.STATE_COMPLETED, protocol.STATE_REJECTED, protocol.STATE_REJECTED]
    assert len(calls) == 1
    # An accepted message's resend is still a retry answered from its admission.
    retried, _ = prepare(first)
    assert retried["status"]["state"] == protocol.STATE_COMPLETED
    assert calls == [calls[0], calls[0]]
