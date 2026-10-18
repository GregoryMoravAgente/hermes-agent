"""R11 causal controls for canonical store identity, lease loss, and runtime teardown."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

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


def _adapter():
    from gateway.config import Platform

    class Adapter:
        def __init__(self):
            self._background_tasks = set()
            self.scheduler = None

        def set_notification_scheduler(self, scheduler):
            self.scheduler = scheduler

    return Platform.MSGRAPH_WEBHOOK, Adapter()


@pytest.mark.platforms("linux", "macos")
def test_redirected_store_canonicalizes_alias_and_owner_bom(tmp_path):
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner

    target = tmp_path / "canonical" / "teams.json"
    target.parent.mkdir()
    target.write_text("{}", encoding="utf-8")
    alias = tmp_path / "alias.json"
    alias.symlink_to(target)
    owner = TeamsStoreOwner(
        profile_id=str(tmp_path / "profile-a"), authority_epoch="r11-epoch",
        instance_id="r11-owner", owner_id="teams:r11-owner",
    )
    owner_path = target.with_name("teams.json.owner")
    owner_path.write_bytes(b"\xef\xbb\xbf" + json.dumps(owner.record(target.resolve())).encode("utf-8"))

    store = TeamsPipelineStore(alias, owner=owner, lock=True)
    try:
        assert store.path == target.resolve()
        assert store._ownership.path == target.resolve()
        store.upsert_job("r11-alias", {"status": "received"})
        assert alias.is_symlink()
        assert not owner_path.read_bytes().startswith(b"\xef\xbb\xbf")
    finally:
        assert store.close() is True


def test_store_close_failure_retains_exact_handle_for_retry(tmp_path, monkeypatch):
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner

    owner = TeamsStoreOwner(
        profile_id=str(tmp_path / "profile-a"), authority_epoch="r11-epoch",
        instance_id="r11-close", owner_id="teams:r11-close",
    )
    store = TeamsPipelineStore(tmp_path / "teams.json", owner=owner, lock=True)
    ownership = store._ownership
    handle = ownership._handle

    original_release = ownership._release_file_lock_checked
    monkeypatch.setattr(ownership, "_release_file_lock_checked", lambda _handle: (_ for _ in ()).throw(OSError()))
    assert store.close() is False
    assert ownership._handle is handle
    store.upsert_job("r11-retained", {"status": "received"})

    monkeypatch.setattr(ownership, "_release_file_lock_checked", original_release)
    assert store.close() is True


def test_store_rejects_reads_writes_and_claims_after_close(tmp_path):
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner, TeamsStoreOwnershipError

    owner = TeamsStoreOwner(
        profile_id=str(tmp_path / "profile-a"), authority_epoch="r11-epoch",
        instance_id="r11-guard", owner_id="teams:r11-guard",
    )
    closed = TeamsPipelineStore(tmp_path / "closed.json", owner=owner, lock=True)
    assert closed.close() is True
    with pytest.raises(TeamsStoreOwnershipError, match="closed"):
        closed.list_jobs()
    with pytest.raises(TeamsStoreOwnershipError, match="closed"):
        closed.upsert_job("r11-closed", {"status": "received"})
    with pytest.raises(TeamsStoreOwnershipError, match="closed"):
        closed.claim_recoverable_jobs("teams:r11-guard", profile_id=owner.profile_id,
                                      authority_epoch=owner.authority_epoch, instance_id=owner.instance_id)


@pytest.mark.platforms("posix")
def test_store_rejects_reads_and_writes_after_inode_lease_loss(tmp_path):
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner, TeamsStoreOwnershipError

    owner = TeamsStoreOwner(
        profile_id=str(tmp_path / "profile-a"), authority_epoch="r11-epoch",
        instance_id="r11-guard", owner_id="teams:r11-guard",
    )
    live = TeamsPipelineStore(tmp_path / "lost.json", owner=owner, lock=True)
    try:
        live._ownership.lock_path.unlink()
        live._ownership.lock_path.write_bytes(b"replacement")
        with pytest.raises(TeamsStoreOwnershipError, match="owner_lost"):
            live.list_jobs()
        with pytest.raises(TeamsStoreOwnershipError, match="owner_lost"):
            live.upsert_job("r11-lost", {"status": "received"})
    finally:
        assert live.close() is True


@pytest.mark.asyncio
async def test_empty_rebind_validates_runtime_identity_before_store_snapshot(tmp_path, monkeypatch):
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    from gateway.runtime_ownership import process_ownership
    from gateway.session_authority import initialize_session_authority
    from hermes_state import SessionDB
    from plugins.teams_pipeline import runtime as runtime_module
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner

    platform, adapter = _adapter()
    home = tmp_path / "profile"
    home.mkdir()
    runner = GatewayRunner(GatewayConfig(platforms={}))
    authority = await initialize_session_authority(
        runner, profile_id=str(home), instance_id="r11-authority", db=SessionDB(home / "state.db")
    )
    owner = TeamsStoreOwner.for_authority(authority, owner_id="teams:r11-authority")
    store = TeamsPipelineStore(tmp_path / "empty.json", owner=owner, lock=True)
    runtime = SimpleNamespace(store=store)
    runner.adapters[platform] = adapter
    runner._teams_pipeline_runtime = runtime
    runner._teams_pipeline_runtime_error = None
    runner.session_runtime_descriptor = {
        "instance_id": "foreign-instance", "authority_epoch": authority.epoch,
    }
    monkeypatch.setattr(store, "list_jobs", lambda: pytest.fail("empty rebind read before identity validation"))
    process_ownership.reserve([home.resolve()])
    try:
        assert runtime_module.bind_gateway_runtime(runner) is False
        assert runner._teams_pipeline_runtime is None
    finally:
        process_ownership.release(home.resolve())
        store.close()
        authority.db.close()


@pytest.mark.asyncio
async def test_live_recovery_task_keeps_store_bound_during_failed_rebind(tmp_path):
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    from gateway.runtime_ownership import process_ownership
    from gateway.session_authority import initialize_session_authority
    from hermes_state import SessionDB
    from plugins.teams_pipeline import runtime as runtime_module
    from plugins.teams_pipeline.pipeline import TeamsMeetingPipeline
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner

    platform, adapter = _adapter()
    home = tmp_path / "profile"
    home.mkdir()
    runner = GatewayRunner(GatewayConfig(platforms={}))
    authority = await initialize_session_authority(
        runner, profile_id=str(home), instance_id="r11-live", db=SessionDB(home / "state.db")
    )
    _install_custody(authority)
    owner = TeamsStoreOwner.for_authority(authority, owner_id="teams:r11-live")
    store = TeamsPipelineStore(tmp_path / "live.json", owner=owner, lock=True)
    job_id = "r11-live-job"
    store.upsert_job(job_id, {"status": "received", "event_id": job_id})
    pipeline = TeamsMeetingPipeline(graph_client=object(), store=store, config={})
    started, release = asyncio.Event(), asyncio.Event()

    async def held_run(job_key):
        started.set()
        await release.wait()
        store.upsert_job(job_key, {"status": "completed"})
        return SimpleNamespace(job_id=job_key, status="completed")

    pipeline.run_job = held_run
    runner.adapters[platform] = adapter
    runner._teams_pipeline_runtime = pipeline
    runner._teams_pipeline_runtime_error = None
    runner.session_runtime_descriptor = {
        "instance_id": authority.instance_id, "authority_epoch": authority.epoch,
    }
    process_ownership.reserve([home.resolve()])
    try:
        assert runtime_module.bind_gateway_runtime(runner) is True
        await async_wait_until(started.is_set, timeout=10, what="the live Teams recovery task")
        runner.session_runtime_descriptor["instance_id"] = "r11-foreign"
        assert runtime_module.bind_gateway_runtime(runner) is False
        assert runner._teams_pipeline_runtime is pipeline
        assert store.get_job(job_id)["status"] == "received"

        release.set()
        await asyncio.gather(*list(adapter._background_tasks))
        assert store.get_job(job_id)["status"] == "completed"
        runner.session_runtime_descriptor["instance_id"] = authority.instance_id
        assert runtime_module.bind_gateway_runtime(runner) is True
    finally:
        release.set()
        if adapter._background_tasks:
            await asyncio.gather(*list(adapter._background_tasks))
        process_ownership.release(home.resolve())
        store.close()
        authority.db.close()


@pytest.mark.asyncio
async def test_done_recovery_task_stays_owned_through_source_finalizer():
    from plugins.teams_pipeline import runtime as runtime_module

    close_calls = []
    runtime = SimpleNamespace(store=SimpleNamespace(close=lambda: close_calls.append("close") or True))
    finalizer_seen = asyncio.Event()
    callback_state = {}
    task = asyncio.create_task(asyncio.sleep(0))

    def source_finalizer(done):
        callback_state["tracked"] = done in runtime_module._runtime_tasks(runtime)
        callback_state["close_refused"] = runtime_module._close_pipeline_runtime(runtime) is False
        finalizer_seen.set()

    runtime_module._track_runtime_task(runtime, task)
    task.add_done_callback(source_finalizer)
    await finalizer_seen.wait()

    assert callback_state == {"tracked": True, "close_refused": True}
    assert task.done()
    await async_wait_until(
        lambda: task not in runtime_module._runtime_tasks(runtime),
        timeout=10,
        what="deferred Teams task tracking cleanup",
    )
    assert runtime_module._close_pipeline_runtime(runtime) is True
    assert close_calls == ["close"]


@pytest.mark.asyncio
@pytest.mark.platforms("posix")
async def test_scheduler_callback_marks_real_store_lease_loss_unknown(tmp_path, monkeypatch):
    """A real scheduler task plus a lost Teams store lease retains sticky unknown custody."""
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from hermes_state import SessionDB
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner

    home = tmp_path / "profile"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    runner = GatewayRunner(GatewayConfig(platforms={}))
    authority = await initialize_session_authority(
        runner, profile_id=str(home), instance_id="r13-lease-loss",
        db=SessionDB(home / "state.db"),
    )
    coordinator = _install_custody(authority)
    owner = TeamsStoreOwner.for_authority(authority, owner_id="teams:r13-lease-loss")
    store = TeamsPipelineStore(tmp_path / "teams.json", owner=owner, lock=True)
    job_id = "r13-lease-loss-job"
    scheduler_id = "teams-recovery:r13-lease-loss-job"
    store.upsert_job(job_id, {"status": "completed", "event_id": job_id})
    runner._teams_pipeline_runtime = SimpleNamespace(store=store)
    handle = authority._preservation_root("I01_native_inbound", scheduler_id)
    authority._preservation_bind_scheduler(scheduler_id, handle, expected_job_id=job_id)

    # Replace the lock path while the real owner descriptor remains open. The store's next read
    # must fail its inode/lease check, rather than proving a terminal row from the wrong owner.
    store._ownership.lock_path.unlink()
    store._ownership.lock_path.write_bytes(b"replacement")
    task = asyncio.create_task(asyncio.sleep(0, result={
        "job_id": job_id, "status": "completed",
    }))
    task.add_done_callback(
        lambda done: authority._preservation_scheduler_done(scheduler_id, done)
    )
    try:
        await task
        await async_wait_until(
            lambda: (
                handle.lease.lease_id in coordinator._records
                and coordinator._records[handle.lease.lease_id].state == "unknown"
            ),
            timeout=10,
            what="real scheduler callback lease-loss custody",
        )
        record = coordinator._records[handle.lease.lease_id]
        assert record.reason == "read_failed"
        assert authority._preservation_scheduler_handles[scheduler_id]["handle"] is handle
        assert coordinator.status()["active_owner_classes"] == ["I01_native_inbound"]
    finally:
        store.close()
        authority.db.close()


@pytest.mark.asyncio
async def test_scheduler_callback_retains_unknown_after_real_lease_close(tmp_path, monkeypatch):
    """A real lease close is observable through the still-open store wrapper and keeps custody unknown."""
    from gateway.config import GatewayConfig
    from gateway.run import GatewayRunner
    from gateway.session_authority import initialize_session_authority
    from hermes_state import SessionDB
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner, TeamsStoreOwnershipError

    home = tmp_path / "profile"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    runner = GatewayRunner(GatewayConfig(platforms={}))
    authority = await initialize_session_authority(
        runner, profile_id=str(home), instance_id="r15-lease-close",
        db=SessionDB(home / "state.db"),
    )
    coordinator = _install_custody(authority)
    owner = TeamsStoreOwner.for_authority(authority, owner_id="teams:r15-lease-close")
    store = TeamsPipelineStore(tmp_path / "teams.json", owner=owner, lock=True)
    job_id = "r15-lease-close-job"
    scheduler_id = f"teams-recovery:{job_id}"
    store.upsert_job(job_id, {"status": "completed", "event_id": job_id})
    runner._teams_pipeline_runtime = SimpleNamespace(store=store)
    handle = authority._preservation_root("I01_native_inbound", scheduler_id)
    authority._preservation_bind_scheduler(scheduler_id, handle, expected_job_id=job_id)

    assert store._ownership.close() is True
    assert not store._closed
    with pytest.raises(TeamsStoreOwnershipError, match="not_acquired"):
        store.get_job(job_id)

    task = asyncio.create_task(asyncio.sleep(0, result={
        "job_id": job_id, "status": "completed",
    }))
    task.add_done_callback(
        lambda done: authority._preservation_scheduler_done(scheduler_id, done)
    )
    try:
        await task
        await async_wait_until(
            lambda: (
                handle.lease.lease_id in coordinator._records
                and coordinator._records[handle.lease.lease_id].state == "unknown"
            ),
            timeout=10,
            what="real closed-lease callback custody",
        )
        record = coordinator._records[handle.lease.lease_id]
        assert record.reason == "read_failed"
        assert authority._preservation_scheduler_handles[scheduler_id]["handle"] is handle
        assert coordinator.status()["active_owner_classes"] == ["I01_native_inbound"]
    finally:
        store.close()
        authority.db.close()


def test_store_close_retains_exact_lease_while_real_writer_is_inflight(tmp_path, monkeypatch):
    import threading

    from agent.memory_provider import spawn_context_thread
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner

    owner = TeamsStoreOwner(
        profile_id=str(tmp_path / "profile-a"), authority_epoch="root-writer-epoch",
        instance_id="root-writer", owner_id="teams:root-writer",
    )
    store = TeamsPipelineStore(tmp_path / "writer.json", owner=owner, lock=True)
    handle = store._ownership._handle
    entered, release = threading.Event(), threading.Event()
    original_persist = store._persist
    errors = []

    def held_persist():
        entered.set()
        assert release.wait(5)
        return original_persist()

    def write_job():
        try:
            store.upsert_job("held-writer", {"status": "received"})
        except (OSError, RuntimeError, ValueError) as exc:
            errors.append(type(exc).__name__)

    monkeypatch.setattr(store, "_persist", held_persist)
    writer = spawn_context_thread(write_job, name="root-held-store-writer", daemon=False)
    writer.start()
    try:
        assert entered.wait(5)
        result = store.close()
        assert store._ownership._handle is handle
        assert result is False
        release.set()
        writer.join(timeout=5)
        assert not writer.is_alive()
        assert errors == []
        assert store.get_job("held-writer")["status"] == "received"
        assert json.loads(store.path.read_text(encoding="utf-8-sig"))["jobs"]["held-writer"]["status"] == "received"
        assert store.close() is True
    finally:
        release.set()
        writer.join(timeout=5)
        assert not writer.is_alive()
        store.close()
