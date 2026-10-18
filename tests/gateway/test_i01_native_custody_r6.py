"""Causal I01 controls for duplicate, cancelled, and late native admission writes.

These tests drive the existing WhatsApp adapter and SessionAuthority against a fresh SessionDB. The
only barriers are real worker-thread write boundaries; no coordinator map or fake future stands in
for the production admission path.
"""
import asyncio
from copy import copy
import threading

import pytest

from tests.waiting import async_wait_until


def _install_custody(authority):
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from hermes_cli.backend_retirement import RetirementFence

    coordinator = RuntimeCustodyCoordinator(
        process_instance=authority.instance_id,
        authority_epoch=authority.epoch,
        fence=RetirementFence(),
    )
    authority.runner.runtime_custody = coordinator
    return coordinator


def _event(adapter, message_id):
    from gateway.platforms.event import MessageEvent

    return MessageEvent(
        text="native I01 custody",
        source=adapter.build_source(
            chat_id="15551234567@s.whatsapp.net",
            user_id="15551234567@s.whatsapp.net",
            chat_type="dm",
        ),
        message_id=message_id,
    )


@pytest.mark.asyncio
async def test_direct_native_exact_duplicate_keeps_first_root(tmp_path, monkeypatch):
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from hermes_state_runtime import list_session_admissions

    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "15551234567")
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    adapter = WhatsAppAdapter(PlatformConfig(enabled=True, extra={"session_path": str(tmp_path / "wa")}))
    adapter.gateway_runner, runner.adapters = runner, {Platform.WHATSAPP: adapter}
    authority = await initialize_session_authority(runner, profile_id="default", instance_id="i01-dup")
    coordinator = _install_custody(authority)
    runner._wire_adapter_handlers(adapter)
    monkeypatch.setattr(authority, "_schedule", lambda _ref: None)

    first_write, release_write = threading.Event(), threading.Event()
    original_write = authority._admit_native_write
    calls = 0

    def held_write(**kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            first_write.set()
            assert release_write.wait(10)
        return original_write(**kwargs)

    monkeypatch.setattr(authority, "_admit_native_write", held_write)
    roots = []
    original_root = authority._preservation_root

    def record_root(owner_class, owner_id):
        handle = original_root(owner_class, owner_id)
        roots.append(handle)
        return handle

    monkeypatch.setattr(authority, "_preservation_root", record_root)
    canonical_event = _event(adapter, "same-native-request")
    first = asyncio.create_task(authority.admit_native(canonical_event))
    assert await asyncio.to_thread(first_write.wait, 10)
    # Both deliveries are the same canonical provider event. A fresh MessageEvent would mint a
    # different timestamp and correctly become an admission conflict, masking duplicate custody.
    second = asyncio.create_task(authority.admit_native(copy(canonical_event)))
    await async_wait_until(lambda: len(roots) == 2, timeout=10, what="both duplicate producer roots")
    release_write.set()
    receipts = await asyncio.gather(first, second)
    rows = list_session_admissions(authority.db, session_id=receipts[0].ref.session_id, pending_only=False)
    assert len(rows) == 1
    assert len(roots) == 2
    assert authority._preservation_handles[rows[0]["admission_id"]] is roots[0]
    assert len(coordinator._records) == 1
    authority.db.close()


@pytest.mark.asyncio
async def test_cancelled_native_write_reconciles_after_shielded_commit(tmp_path, monkeypatch):
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from plugins.platforms.whatsapp.adapter import WhatsAppAdapter
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from hermes_state_runtime import list_session_admissions

    monkeypatch.setenv("WHATSAPP_ALLOWED_USERS", "15551234567")
    runner = GatewayRunner(GatewayConfig(sessions_dir=tmp_path / "sessions"))
    adapter = WhatsAppAdapter(PlatformConfig(enabled=True, extra={"session_path": str(tmp_path / "wa")}))
    adapter.gateway_runner, runner.adapters = runner, {Platform.WHATSAPP: adapter}
    authority = await initialize_session_authority(runner, profile_id="default", instance_id="i01-cancel")
    coordinator = _install_custody(authority)
    runner._wire_adapter_handlers(adapter)
    monkeypatch.setattr(authority, "_schedule", lambda _ref: None)

    write_started, release_write = threading.Event(), threading.Event()
    original_write = authority._admit_native_write

    def held_write(**kwargs):
        write_started.set()
        assert release_write.wait(10)
        return original_write(**kwargs)

    monkeypatch.setattr(authority, "_admit_native_write", held_write)
    task = asyncio.create_task(authority.admit_native(_event(adapter, "cancelled-native-request")))
    assert await asyncio.to_thread(write_started.wait, 10)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    # Cancellation is not a refusal while the shielded worker is still inside the real writer.
    assert coordinator.status()["active_owner_classes"] == ["I01_native_inbound"]
    release_write.set()
    rows = await async_wait_until(
        lambda: list_session_admissions(authority.db, session_id=next(iter(authority.sessions)), pending_only=False),
        timeout=15, what="the late shielded native admission row")
    assert len(rows) == 1 and rows[0]["status"] == "queued"
    await async_wait_until(
        lambda: rows[0]["admission_id"] in authority._preservation_handles,
        timeout=10, what="the late native custody binding")
    assert coordinator.status()["active_owner_classes"] == ["I01_native_inbound"]
    authority.db.close()
