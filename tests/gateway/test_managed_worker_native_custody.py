"""Actual I04 managed-worker admission and I16 native producer boundaries."""

import asyncio
import hashlib
import os
import subprocess
import sys
from types import SimpleNamespace

import pytest

from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding


@pytest.mark.asyncio
async def test_server_owned_managed_descriptor_preserves_text_and_image_rows(tmp_path, monkeypatch):
    """Normal prompt.submit stamps the managed route outside the immutable media envelope."""
    from gateway import session_managed_worker
    from gateway.platforms.base import cache_image_from_bytes
    from gateway.session_contract import Principal, SessionRef, Submission
    from gateway.session_operator import check_local_input
    from gateway.session_queued_controls import queued_controls
    from hermes_state_runtime import get_session_admission
    from tests.gateway.test_prompt_attachments import _ONE_PX_PNG
    from tests.gateway.test_session_authority_cancel_settlement import _authority

    db, authority = _authority(tmp_path, monkeypatch)
    monkeypatch.setattr(authority, "_schedule", lambda _ref: None)
    authority.runner._adapter_for_source = lambda _source: None
    monkeypatch.setattr(session_managed_worker, "managed_policy", lambda *_args, **_kwargs: object())
    actor = Principal("human", "owned", frozenset({"session:submit", "session:control"}), "test")
    ref = SessionRef("owned", "s")
    try:
        with db:
            db._execute_write(lambda conn: conn.execute(
                "UPDATE sessions SET source=? WHERE id=?", ("gui", "s")
            ))
            text_receipt = await authority.submit(
                actor, Submission("managed-text", ref, {"text": "managed text"}, "queue")
            )
            staged = cache_image_from_bytes(_ONE_PX_PNG, ".png")
            image_receipt = await authority.submit(
                actor,
                Submission(
                    "managed-image", ref,
                    {"text": "managed image", "attachments": [{"path": staged, "mime": "image/png"}]},
                    "queue",
                ),
            )
            for receipt in (text_receipt, image_receipt):
                row = get_session_admission(db, admission_id=receipt.admission_id)
                assert row is not None
                assert row["payload"]["managed_worker_v1"] == {"producer_process_mode": "managed_turn"}
                assert row["payload"]["managed_worker_v1"] is not row["payload"].get("attachments_v1")
            image_row = get_session_admission(db, admission_id=image_receipt.admission_id)
            assert image_row["payload"]["attachments_v1"]["media_types"] == ["image/png"]
            assert set(image_row["payload"]["attachments_v1"]) == {"media", "media_types"}
            check_local_input(authority, ref, get_session_admission(db, admission_id=text_receipt.admission_id))
            check_local_input(authority, ref, image_row)
            assert queued_controls(authority, get_session_admission(
                db, admission_id=text_receipt.admission_id,
            )) == ["steer", "replace"]
            assert queued_controls(authority, image_row) == []
    finally:
        db.close()


@pytest.mark.asyncio
async def test_public_managed_route_passes_exact_i04_binding_before_spawn(tmp_path, monkeypatch):
    """The public admission route binds the claimed row before the managed producer runs."""
    from hermes_state_runtime import get_session_admission
    from gateway import session_completion_notes, session_ingress, session_managed_worker, session_policy

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    try:
        binding = await fixture.claim("managed-route-binding")
        ref = type("Ref", (), {
            "session_id": binding.session_id,
            "profile_id": binding.profile_id,
        })()
        row = get_session_admission(fixture.authority.db, admission_id=binding.admission_id)
        assert row is not None
        received = {}
        policy = object()

        async def admit(authority, received_ref, received_row, received_policy, received_binding=None):
            received.update(
                authority=authority, ref=received_ref, row=received_row,
                policy=received_policy, binding=received_binding,
            )
            return "managed-route-bound"

        async def no_note(*_args, **_kwargs):
            return None

        monkeypatch.setattr(session_completion_notes, "note_if_unviewed", no_note)
        monkeypatch.setattr(session_policy, "policy_for_source", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(session_managed_worker, "managed_policy", lambda *_args, **_kwargs: policy)
        monkeypatch.setattr(session_managed_worker, "admit_managed", admit)

        result = await session_ingress.execute_admission(fixture.authority, ref, row)

        assert result == "managed-route-bound"
        assert received["authority"] is fixture.authority
        assert received["ref"] is ref
        assert received["row"] is row
        assert received["policy"] is policy
        assert received["binding"] is not None
        assert received["binding"].admission_id == binding.admission_id
        assert received["binding"].parent_handle is binding.parent_handle
        assert received["binding"].execution_generation == binding.execution_generation
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
async def test_public_managed_route_refuses_legacy_row_without_producer_binding(tmp_path, monkeypatch):
    """A legacy durable row cannot silently spawn outside the I16 custody route."""
    from hermes_state_runtime import get_session_admission
    from gateway import session_completion_notes, session_ingress, session_managed_worker, session_policy
    from gateway.runtime_service_custody import ServiceCustodyError

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    try:
        binding = await fixture.claim("managed-legacy-row")
        ref = SimpleNamespace(session_id=binding.session_id, profile_id=binding.profile_id)
        row = get_session_admission(fixture.authority.db, admission_id=binding.admission_id)
        assert row is not None
        native = row["payload"].get("native_text_v1")
        assert isinstance(native, dict) and native.get("producer_process_mode") == "managed_turn"
        legacy_row = {
            **row,
            "payload": {
                **row["payload"],
                "native_text_v1": {
                    key: value for key, value in native.items() if key != "producer_process_mode"
                },
            },
        }
        policy = object()
        spawned = False

        async def no_note(*_args, **_kwargs):
            return None

        async def should_not_spawn(*_args, **_kwargs):
            nonlocal spawned
            spawned = True
            raise AssertionError("legacy managed row reached an untracked child route")

        monkeypatch.setattr(session_completion_notes, "note_if_unviewed", no_note)
        monkeypatch.setattr(session_policy, "policy_for_source", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(session_managed_worker, "managed_policy", lambda *_args, **_kwargs: policy)
        monkeypatch.setattr(session_managed_worker, "admit_managed", should_not_spawn)

        with pytest.raises(ServiceCustodyError, match="managed_producer_binding_missing"):
            await session_ingress.execute_admission(fixture.authority, ref, legacy_row)
        assert spawned is False
        assert getattr(fixture.authority, "_managed_workers", {}) == {}
    finally:
        fixture.cleanup()


@pytest.mark.platforms("posix")
@pytest.mark.asyncio
async def test_managed_worker_binds_real_process_group_and_reader_eof(tmp_path, monkeypatch):
    """A real child, reader, and process group remain one receipt until native close."""
    from gateway.session_managed_worker import ManagedWorker
    from gateway.session_managed_worker_custody import reserve_managed_worker
    from gateway.session_turn_physical_tip import canonical_store_path

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    process = None
    worker = None
    custody = None
    try:
        binding = await fixture.claim("managed-native-child")
        custody = reserve_managed_worker(binding, canonical_store_path(fixture.authority))
        custody.configure_streams({"stdout"})
        process = await asyncio.to_thread(
            subprocess.Popen,
            [sys.executable, "-c", "import json; print(json.dumps({'type': 'probe'}), flush=True)"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        custody.bind_process(process)
        worker = ManagedWorker(process)
        custody.attach_worker(worker)

        frame = await asyncio.wait_for(worker.next_frame(5, ack=0), timeout=10)
        assert frame == {"type": "probe"}
        assert process.wait(timeout=5) == 0
        await asyncio.to_thread(worker.close)

        physical = custody.physical
        assert physical.process is process
        assert physical.readers["stdout"] is worker.reader
        assert physical.eof_streams == {"stdout"}
        assert physical.joined_streams == {"stdout"}
        assert physical.close_native_after_process() is True
    finally:
        if worker is not None:
            await asyncio.to_thread(worker.close)
        elif process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if custody is not None and custody.physical is not None and custody.physical.state not in {"unknown", "closed", "released"}:
            custody.mark_unknown("native_probe_no_delivery")
        fixture.cleanup()


@pytest.mark.platforms("posix")
@pytest.mark.asyncio
async def test_partial_spawn_retains_same_process_and_marks_unknown(tmp_path, monkeypatch):
    """A cancellation after Popen keeps the exact process reachable instead of respawning."""
    from gateway.session_managed_worker_custody import reserve_managed_worker
    from gateway.session_turn_physical_tip import canonical_store_path

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    process = None
    custody = None
    try:
        binding = await fixture.claim("managed-partial-spawn")
        custody = reserve_managed_worker(binding, canonical_store_path(fixture.authority))
        process = await asyncio.to_thread(
            subprocess.Popen,
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        custody.cleanup_spawned_process(process)

        assert custody.physical.process is process
        assert custody.physical.state == "unknown"
        assert custody.physical.unknown_reason == "partial_launch"
        assert process.poll() is not None
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.platforms("posix")
@pytest.mark.asyncio
async def test_public_managed_execution_retains_native_process_on_bootstrap_failure(tmp_path, monkeypatch):
    """After the real I04 producer accepts a child, bootstrap failure is same-handle UNKNOWN."""
    from hermes_state_runtime import get_session_admission
    from gateway import session_context_refs, session_managed_worker
    from gateway.session_managed_worker import ManagedExecutionUnknown
    import gateway.session_managed_worker_custody as custody_module

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    captured = {}
    process = None
    try:
        binding = await fixture.claim("managed-bootstrap-failure")
        ref = SimpleNamespace(session_id=binding.session_id, profile_id=binding.profile_id)
        row = get_session_admission(fixture.authority.db, admission_id=binding.admission_id)
        assert row is not None

        async def managed_text(*_args, **_kwargs):
            return "native probe", None

        monkeypatch.setattr(session_context_refs, "managed_turn_text", managed_text)

        real_popen = subprocess.Popen
        worker_subprocess = SimpleNamespace(
            Popen=real_popen, PIPE=subprocess.PIPE, DEVNULL=subprocess.DEVNULL,
            TimeoutExpired=subprocess.TimeoutExpired,
        )
        hello_script = (
            "import json, os, psutil\n"
            "from gateway.status import get_process_start_time\n"
            "p = psutil.Process(os.getpid())\n"
            "print(json.dumps({'type':'hello', 'pid':os.getpid(), 'birth':get_process_start_time(os.getpid()), "
            "'ancestors':[parent.pid for parent in p.parents()]}), flush=True)\n"
            "import time; time.sleep(30)\n"
        )

        def probe_popen(_args, *args, **kwargs):
            return real_popen([sys.executable, "-c", hello_script], *args, **kwargs)

        worker_subprocess.Popen = probe_popen
        monkeypatch.setattr(session_managed_worker, "subprocess", worker_subprocess)

        def fail_bootstrap(*_args, **_kwargs):
            raise RuntimeError("intentional bootstrap failure")

        monkeypatch.setattr(session_managed_worker, "_bootstrap", fail_bootstrap)
        real_reserve = custody_module.reserve_managed_worker

        def capture_reserve(received_binding, store_path):
            custody = real_reserve(received_binding, store_path)
            captured["custody"] = custody
            return custody

        monkeypatch.setattr(custody_module, "reserve_managed_worker", capture_reserve)

        with pytest.raises(ManagedExecutionUnknown, match="managed_worker_unknown"):
            await session_managed_worker.execute_managed(
                fixture.authority, ref, row, object(), binding,
            )

        custody = captured["custody"]
        process = custody.physical.process
        assert process is not None
        assert custody.physical.state == "unknown"
        assert custody.physical.unknown_reason == "managed_worker_unknown"
        assert process.poll() is not None
        assert custody.physical.readers["stdout"] is not None
        assert custody.physical.eof_streams == custody.physical.required_streams
        assert custody.physical.joined_streams == custody.physical.required_streams
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.platforms("posix")
@pytest.mark.asyncio
async def test_physical_close_refusal_retries_same_managed_receipt(tmp_path, monkeypatch):
    """A native-close refusal keeps the real process receipt on the same retry path."""
    from gateway.runtime_service_custody import DurableResultProof
    from gateway.session_managed_worker_custody import reserve_managed_worker
    from gateway.session_turn_physical_tip import canonical_store_path

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    process = None
    custody = None
    try:
        binding = await fixture.claim("managed-native-close-retry")
        custody = reserve_managed_worker(binding, canonical_store_path(fixture.authority))
        process = await asyncio.to_thread(
            subprocess.Popen,
            [sys.executable, "-c", "import sys; sys.exit(0)"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        custody.bind_process(process)
        physical = custody.physical
        service = custody.service
        request = custody.request
        assert physical is not None and service is not None and request is not None
        assert process.wait(timeout=5) == 0

        db = fixture.authority.db
        user_id = db.append_message(binding.session_id, role="user", content="managed retry input")
        assistant_id = db.append_message(
            binding.session_id, role="assistant", content="managed retry output",
        )
        with db._lock:
            raw_rows = db._conn.execute(
                "SELECT * FROM messages WHERE session_id=? AND id IN (?,?)",
                (binding.session_id, user_id, assistant_id),
            ).fetchall()
        rows_by_id = {row["id"]: dict(row) for row in raw_rows}
        rows = [rows_by_id[user_id], rows_by_id[assistant_id]]
        assert [row["role"] for row in rows] == ["user", "assistant"]
        from agent.transcript_repair import transcript_row_snapshot
        row_ids = tuple(row["id"] for row in rows)
        row_snapshots = tuple(transcript_row_snapshot(row) for row in rows)
        digest_input = "\n".join(
            f"{row_id}:{binding.session_id}:{role}:{snapshot}"
            for row_id, role, snapshot in zip(
                row_ids, ("user", "assistant"), row_snapshots, strict=True,
            )
        )
        # This is the same typed proof shape the managed worker produces.  The retry
        # boundary does not reinterpret its contents or replace the owning receipt.
        proof = DurableResultProof(
            store_path=custody.store_path,
            session_id=binding.session_id,
            admission_id=binding.admission_id,
            execution_generation=binding.execution_generation,
            row_ids=row_ids, row_snapshots=row_snapshots,
            row_sessions=(binding.session_id, binding.session_id),
            row_roles=("user", "assistant"), payload_digest=hashlib.sha256(
                digest_input.encode()
            ).hexdigest(),
            result_row_ids=(row_ids[1],),
        )
        physical.managed_witness = {"kind": "managed_worker", "proof": proof}
        service.record_result_durable(binding, request, proof)

        close_calls = []
        original_close = physical.close_native_after_process

        def refuse_once():
            close_calls.append("refused" if not close_calls else "retry")
            if len(close_calls) == 1:
                return False
            return original_close()

        monkeypatch.setattr(physical, "close_native_after_process", refuse_once)
        receipt_handle = physical.handle
        process_before_retry = physical.process
        service_handle = service.handle

        assert physical.close_native_after_process() is False
        assert physical.state == "process_bound"
        custody.mark_unknown("physical_custody_pending")
        fixture.authority._preservation_mark_unknown(
            binding.admission_id, "physical_custody_pending",
        )
        assert physical.state == "unknown"
        assert physical.process is process_before_retry is process
        assert physical.handle is receipt_handle
        assert service.handle is service_handle

        assert service.reconcile_managed_child_failure(binding, request) is True
        assert close_calls == ["refused", "retry"]
        assert physical.handle is receipt_handle
        assert service.handle is service_handle
        assert physical.process is None
        assert physical.state == "reserved"
        assert physical.unknown_reason is None
        assert service.unknown is False
        assert service.requests[binding.admission_id] is request
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        fixture.cleanup()


def test_runtime_store_finish_emits_real_readback_proof(tmp_path):
    """The child-side store proves the committed user/result suffix through owner RPC readback."""
    from agent.runtime_session_store import RuntimeSessionStore
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch, mutate_worker_execution, register_worker_execution

    db_path = (tmp_path / "managed-proof.db").resolve()
    db = SessionDB(db_path)
    try:
        db.create_session("managed-session", source="cli")
        db._execute_write(
            lambda conn: conn.execute(
                "UPDATE sessions SET runtime_generation=1 WHERE id=?", ("managed-session",),
            )
        )
        epoch = begin_runtime_epoch(db, instance_id="managed-proof-owner")
        register_worker_execution(
            db, epoch=epoch, execution_id="managed-proof-execution", session_id="managed-session",
            generation=1, kind="compute", adoption_secret="managed-proof-secret",
        )
        scope = {
            "epoch": epoch, "execution_id": "managed-proof-execution",
            "session_id": "managed-session", "generation": 1,
        }

        def rpc(message, **params):
            assert message == "worker.persist"
            return mutate_worker_execution(
                db, epoch=params["epoch"], execution_id=params["execution_id"],
                session_id=params["session_id"], generation=params["generation"],
                sequence=params["sequence"], operation=params["operation"], payload=params["payload"],
            )

        store = RuntimeSessionStore(
            rpc, scope, tmp_path / "managed-proof-outbox",
            managed_custody={
                "store_path": str(db_path), "admission_id": "managed-proof-admission", "generation": 1,
            },
        )
        try:
            messages = [
                {"role": "user", "content": "managed input"},
                {"role": "assistant", "content": "managed output"},
            ]
            assert store.append_messages_batch("managed-session", messages) == 2
            finished = store.finish()
            proof = finished["managed_durable_proof"]
            assert proof["store_path"] == str(db_path)
            assert proof["session_id"] == "managed-session"
            assert proof["admission_id"] == "managed-proof-admission"
            assert proof["execution_generation"] == 1
            assert proof["row_roles"] == ["user", "assistant"]
            assert proof["result_row_ids"] == [messages[1]["_row_id"]]
            assert all(isinstance(snapshot, str) and snapshot for snapshot in proof["row_snapshots"])
            assert proof["payload_digest"]
        finally:
            store.close()
    finally:
        db.close()


def test_runtime_store_finish_reads_back_compression_rewrite_rows(tmp_path):
    """Archive publication rows are part of the same managed finish proof as appends."""
    from agent.runtime_session_store import RuntimeSessionStore
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch, mutate_worker_execution, register_worker_execution

    db_path = (tmp_path / "managed-compression-proof.db").resolve()
    db = SessionDB(db_path)
    try:
        db.create_session("managed-session", source="cli")
        db._execute_write(
            lambda conn: conn.execute(
                "UPDATE sessions SET runtime_generation=1 WHERE id=?", ("managed-session",),
            )
        )
        epoch = begin_runtime_epoch(db, instance_id="managed-compression-owner")
        register_worker_execution(
            db, epoch=epoch, execution_id="managed-compression-execution", session_id="managed-session",
            generation=1, kind="compute", adoption_secret="managed-compression-secret",
        )
        scope = {
            "epoch": epoch, "execution_id": "managed-compression-execution",
            "session_id": "managed-session", "generation": 1,
        }

        def rpc(message, **params):
            assert message == "worker.persist"
            return mutate_worker_execution(
                db, epoch=params["epoch"], execution_id=params["execution_id"],
                session_id=params["session_id"], generation=params["generation"],
                sequence=params["sequence"], operation=params["operation"], payload=params["payload"],
            )

        store = RuntimeSessionStore(
            rpc, scope, tmp_path / "managed-compression-proof-outbox",
            managed_custody={
                "store_path": str(db_path), "admission_id": "managed-compression-admission", "generation": 1,
            },
        )
        try:
            compacted = [
                {"role": "user", "content": "compressed input"},
                {"role": "assistant", "content": "compressed output"},
            ]
            assert store.archive_and_compact("managed-session", compacted, tail_count=0) == 2
            finished = store.finish()
            proof = finished["managed_durable_proof"]
            assert proof["session_id"] == "managed-session"
            assert proof["row_roles"] == ["user", "assistant"]
            assert proof["row_snapshots"]
            assert proof["result_row_ids"] == [compacted[1]["_row_id"]]
            assert proof["payload_digest"]
        finally:
            store.close()
    finally:
        db.close()


def test_runtime_store_finish_reads_back_published_compression_child(tmp_path):
    """A rotated compression child is recaptured under its new owner-scoped readback."""
    from agent.runtime_session_store import RuntimeSessionStore
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch, mutate_worker_execution, register_worker_execution

    db_path = (tmp_path / "managed-published-proof.db").resolve()
    db = SessionDB(db_path)
    try:
        db.create_session("managed-parent", source="cli")
        db._execute_write(
            lambda conn: conn.execute(
                "UPDATE sessions SET runtime_generation=1 WHERE id=?", ("managed-parent",),
            )
        )
        epoch = begin_runtime_epoch(db, instance_id="managed-published-owner")
        register_worker_execution(
            db, epoch=epoch, execution_id="managed-published-execution", session_id="managed-parent",
            generation=1, kind="compute", adoption_secret="managed-published-secret",
        )
        scope = {
            "epoch": epoch, "execution_id": "managed-published-execution",
            "session_id": "managed-parent", "generation": 1,
        }

        def rpc(message, **params):
            assert message == "worker.persist"
            return mutate_worker_execution(
                db, epoch=params["epoch"], execution_id=params["execution_id"],
                session_id=params["session_id"], generation=params["generation"],
                sequence=params["sequence"], operation=params["operation"], payload=params["payload"],
            )

        store = RuntimeSessionStore(
            rpc, scope, tmp_path / "managed-published-proof-outbox",
            managed_custody={
                "store_path": str(db_path), "admission_id": "managed-published-admission", "generation": 1,
            },
        )
        try:
            assert store.try_acquire_compression_lock("managed-parent", "managed-holder")
            published = [
                {"role": "user", "content": "published input"},
                {"role": "assistant", "content": "published output"},
            ]
            real_rpc = store.rpc

            def lost_ack(message, **params):
                real_rpc(message, **params)
                raise TimeoutError("compression publish lost acknowledgement")

            store.rpc = lost_ack
            with pytest.raises(TimeoutError, match="lost acknowledgement"):
                store.publish_compression_child(
                    parent_session_id="managed-parent", child_session_id="managed-child", source="cli",
                    messages=published, compression_lock_holder="managed-holder",
                )
            store.rpc = real_rpc
            assert store.retry_pending()
            assert store.scope["session_id"] == "managed-child"
            finished = store.finish()
            proof = finished["managed_durable_proof"]
            assert proof["session_id"] == "managed-child"
            assert proof["row_roles"] == ["user", "assistant"]
            assert proof["row_sessions"] == ["managed-child", "managed-child"]
            assert proof["result_row_ids"] == [proof["row_ids"][1]]
            assert proof["payload_digest"]
        finally:
            store.close()
    finally:
        db.close()


def test_runtime_store_finish_tracks_append_archive_publish_scope_rotation(tmp_path):
    """One managed turn keeps only the current SQL suffix across every real rotation step."""
    from agent.runtime_session_store import RuntimeSessionStore
    from hermes_state import SessionDB
    from hermes_state_runtime import begin_runtime_epoch, mutate_worker_execution, register_worker_execution

    db_path = (tmp_path / "managed-append-archive-publish.db").resolve()
    db = SessionDB(db_path)
    try:
        db.create_session("managed-parent", source="cli")
        db._execute_write(
            lambda conn: conn.execute(
                "UPDATE sessions SET runtime_generation=1 WHERE id=?", ("managed-parent",),
            )
        )
        epoch = begin_runtime_epoch(db, instance_id="managed-rotation-owner")
        register_worker_execution(
            db, epoch=epoch, execution_id="managed-rotation-execution", session_id="managed-parent",
            generation=1, kind="compute", adoption_secret="managed-rotation-secret",
        )
        scope = {
            "epoch": epoch, "execution_id": "managed-rotation-execution",
            "session_id": "managed-parent", "generation": 1,
        }

        def rpc(message, **params):
            assert message == "worker.persist"
            return mutate_worker_execution(
                db, epoch=params["epoch"], execution_id=params["execution_id"],
                session_id=params["session_id"], generation=params["generation"],
                sequence=params["sequence"], operation=params["operation"], payload=params["payload"],
            )

        store = RuntimeSessionStore(
            rpc, scope, tmp_path / "managed-append-archive-publish-outbox",
            managed_custody={
                "store_path": str(db_path), "admission_id": "managed-rotation-admission", "generation": 1,
            },
        )
        try:
            initial = [
                {"role": "user", "content": "initial input"},
                {"role": "assistant", "content": "initial output"},
            ]
            assert store.append_messages_batch("managed-parent", initial) == 2
            archived = [
                {"role": "user", "content": "archived input"},
                {"role": "assistant", "content": "archived output"},
            ]
            assert store.archive_and_compact("managed-parent", archived, tail_count=0) == 2
            assert store.try_acquire_compression_lock("managed-parent", "managed-rotation-holder")
            published = [
                {"role": "user", "content": "child input"},
                {"role": "assistant", "content": "child output"},
            ]
            store.publish_compression_child(
                parent_session_id="managed-parent", child_session_id="managed-child",
                source="cli", messages=published, compression_lock_holder="managed-rotation-holder",
            )
            assert store.scope["session_id"] == "managed-child"
            proof = store._managed_finish_proof()
            assert proof["session_id"] == "managed-child"
            assert proof["row_ids"] == [published[0]["_row_id"], published[1]["_row_id"]]
            assert proof["row_roles"] == ["user", "assistant"]
            assert proof["result_row_ids"] == [published[1]["_row_id"]]
            assert store._managed_written_rows == [
                (published[0]["_row_id"], "user", published[0]["_db_row_snapshot"]),
                (published[1]["_row_id"], "assistant", published[1]["_db_row_snapshot"]),
            ]
            with pytest.raises(Exception, match="permission_denied"):
                store.append_messages_batch("managed-parent", [{"role": "user", "content": "foreign"}])

            with db._read_ctx() as conn:
                original_content = conn.execute(
                    "SELECT content FROM messages WHERE id=?", (published[1]["_row_id"],)
                ).fetchone()[0]
            db._execute_write(
                lambda conn: conn.execute(
                    "UPDATE messages SET content=? WHERE id=?",
                    ('"tampered managed result"', published[1]["_row_id"]),
                )
            )
            assert store._managed_finish_proof() is None
            db._execute_write(
                lambda conn: conn.execute(
                    "UPDATE messages SET content=? WHERE id=?",
                    (original_content, published[1]["_row_id"]),
                )
            )
            assert store.finish()["managed_durable_proof"] == proof
            assert proof["session_id"] == "managed-child"
        finally:
            store.close()
    finally:
        db.close()


@pytest.mark.platforms("posix")
@pytest.mark.asyncio
async def test_managed_worker_reader_start_failure_keeps_same_unknown_receipt(tmp_path, monkeypatch):
    """A reader start exception is ambiguous and cannot release or respawn its receipt."""
    from gateway.session_managed_worker import ManagedWorker
    from gateway.session_managed_worker_custody import reserve_managed_worker
    from gateway.session_turn_physical_tip import canonical_store_path

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    process = None
    worker = None
    custody = None

    class StartRaises:
        def is_alive(self):
            return False

        def start(self):
            raise RuntimeError("reader-start-ambiguous")

        def join(self, timeout=None):
            return None

    try:
        binding = await fixture.claim("managed-reader-start")
        custody = reserve_managed_worker(binding, canonical_store_path(fixture.authority))
        custody.configure_streams({"stdout"})
        process = await asyncio.to_thread(
            subprocess.Popen,
            [sys.executable, "-c", "import time; time.sleep(30)"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        custody.bind_process(process)
        worker = ManagedWorker(process)
        custody.attach_worker(worker)
        monkeypatch.setattr(
            "agent.memory_provider.spawn_context_thread",
            lambda *args, **kwargs: StartRaises(),
        )
        with pytest.raises(RuntimeError, match="reader-start-ambiguous"):
            await worker.next_frame(1, ack=0)
        physical = custody.physical
        handle = physical.handle
        assert physical.state == "unknown"
        assert physical.unknown_reason == "reader_start_unknown"
        assert physical.handle is handle
        assert physical.process is process
    finally:
        if worker is not None:
            await asyncio.to_thread(worker.close)
        elif process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.platforms("posix")
@pytest.mark.asyncio
async def test_managed_worker_reader_error_and_descendant_exit_stay_same_unknown_receipt(tmp_path, monkeypatch):
    """Reader errors and a surviving process-group descendant retain the original handle."""
    from gateway.session_managed_worker import ManagedWorker
    from gateway.session_managed_worker_custody import reserve_managed_worker
    from gateway.session_turn_physical_tip import canonical_store_path

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    process = None
    worker = None
    custody = None
    try:
        binding = await fixture.claim("managed-reader-error")
        custody = reserve_managed_worker(binding, canonical_store_path(fixture.authority))
        custody.configure_streams({"stdout"})
        process = await asyncio.to_thread(
            subprocess.Popen,
            [sys.executable, "-c", "import sys; sys.stdout.write('not-json\\n'); sys.stdout.flush()"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        custody.bind_process(process)
        worker = ManagedWorker(process)
        custody.attach_worker(worker)
        with pytest.raises(ValueError, match="Expecting value"):
            await worker.next_frame(5, ack=0)
        physical = custody.physical
        assert physical.state == "unknown"
        assert physical.unknown_reason == "reader_failed"
        handle = physical.handle
        await asyncio.to_thread(worker.close)
        assert physical.handle is handle
        assert physical.process is process
        worker = None
    finally:
        if worker is not None:
            await asyncio.to_thread(worker.close)
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if custody is not None and custody.physical is not None:
            pgid = custody.physical.process_group_id
            if isinstance(pgid, int) and pgid > 0 and pgid != os.getpgrp():
                try:
                    os.killpg(pgid, 9)
                except (ProcessLookupError, PermissionError, OSError):
                    pass
        fixture.cleanup()


@pytest.mark.platforms("posix")
@pytest.mark.asyncio
async def test_managed_worker_descendant_exit_keeps_same_unknown_receipt(tmp_path, monkeypatch):
    """A natural leader exit does not prove the source-owned process group is empty."""
    from gateway.session_managed_worker import ManagedWorker
    from gateway.session_managed_worker_custody import reserve_managed_worker
    from gateway.session_turn_physical_tip import canonical_store_path

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    process = None
    worker = None
    custody = None
    try:
        binding = await fixture.claim("managed-descendant-exit")
        custody = reserve_managed_worker(binding, canonical_store_path(fixture.authority))
        custody.configure_streams({"stdout"})
        process = await asyncio.to_thread(
            subprocess.Popen,
            [sys.executable, "-c", (
                "import subprocess, sys, time; "
                "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(5)']); "
                "time.sleep(0.1)"
            )],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        custody.bind_process(process)
        worker = ManagedWorker(process)
        custody.attach_worker(worker)
        assert process.wait(timeout=5) == 0
        handle = custody.physical.handle
        await asyncio.to_thread(worker.close)
        assert custody.physical.close_native_after_process() is False
        custody.mark_unknown("physical_custody_pending")
        assert custody.physical.state == "unknown"
        assert custody.physical.unknown_reason == "physical_custody_pending"
        assert custody.physical.handle is handle
        assert custody.physical.process is process
        worker = None
    finally:
        if worker is not None:
            await asyncio.to_thread(worker.close)
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if custody is not None and custody.physical is not None:
            pgid = custody.physical.process_group_id
            if isinstance(pgid, int) and pgid > 0 and pgid != os.getpgrp():
                try:
                    os.killpg(pgid, 9)
                except (ProcessLookupError, PermissionError, OSError):
                    pass
        fixture.cleanup()


def _windows_safe_child_env(tmp_path):
    """Pass only non-secret process plumbing to a native Windows probe child."""
    env = {
        key: value for key in ("PATH", "SystemRoot", "WINDIR", "TEMP", "TMP")
        if (value := os.environ.get(key)) is not None
    }
    env["HERMES_HOME"] = str((tmp_path / "profile").resolve())
    return env


def _windows_child(custody, tmp_path, code):
    return custody.launch_windows(
        [sys.executable, "-c", code],
        text=False,
        env=_windows_safe_child_env(tmp_path),
        cwd=str(tmp_path),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        encoding=None,
        errors=None,
        bufsize=-1,
        start_new_session=True,
        close_fds=True,
        creationflags=0,
    )


async def _windows_custody_fixture(tmp_path, monkeypatch, request_id):
    from gateway.session_managed_worker_custody import reserve_managed_worker
    from gateway.session_turn_physical_tip import canonical_store_path

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    binding = await fixture.claim(request_id)
    custody = reserve_managed_worker(binding, canonical_store_path(fixture.authority))
    custody.configure_streams({"stdout"})
    return fixture, binding, custody


@pytest.mark.platforms("windows", arch="x64")
@pytest.mark.asyncio
async def test_windows_post_popen_failure_retains_exact_job_process_and_retries(
    tmp_path, monkeypatch,
):
    """A callback fault after Popen but before Job assignment is same-handle UNKNOWN."""
    fixture, binding, custody = await _windows_custody_fixture(
        tmp_path, monkeypatch, "windows-post-popen-failure",
    )
    try:
        physical = custody.physical
        handle = physical.handle

        def fail_after_popen(_transaction):
            raise KeyboardInterrupt("post-popen callback interruption")

        with pytest.raises(KeyboardInterrupt, match="post-popen"):
            custody.launch_windows(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                text=False,
                env=_windows_safe_child_env(tmp_path),
                cwd=str(tmp_path),
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                encoding=None,
                errors=None,
                bufsize=-1,
                start_new_session=True,
                close_fds=True,
                creationflags=0,
                on_process_bound=fail_after_popen,
            )

        transaction = physical.transaction
        process = transaction.process
        assert transaction.assignment_attempted is False
        assert transaction.assigned is False
        assert process is not None
        assert physical.process is process
        assert custody.owner.process is process
        assert custody.owner.effect_observed is True
        assert physical.state == "unknown"
        assert physical.unknown_reason == "partial_launch"
        assert process.poll() is not None

        assert custody.service.reconcile_process_failure(binding, custody.request) is True
        assert physical.handle is handle
        assert physical.process is None
        assert physical.transaction is None
        assert physical.state == "reserved"
    finally:
        fixture.cleanup()


@pytest.mark.platforms("windows", arch="x64")
@pytest.mark.asyncio
async def test_windows_pre_popen_refusal_releases_only_verified_no_effect(tmp_path, monkeypatch):
    """A CreateProcess refusal releases its Job only after the exact no-effect proof."""
    from tools import process_registry_windows

    fixture, _binding, custody = await _windows_custody_fixture(
        tmp_path, monkeypatch, "windows-pre-popen-refusal",
    )
    try:
        physical = custody.physical

        def refuse_popen(*_args, **_kwargs):
            raise FileNotFoundError("synthetic missing executable")

        monkeypatch.setattr(process_registry_windows.subprocess, "Popen", refuse_popen)
        with pytest.raises(FileNotFoundError, match="missing executable"):
            _windows_child(custody, tmp_path, "import time; time.sleep(30)")

        transaction = physical.transaction
        assert transaction is not None
        assert transaction.process is None
        assert transaction.no_effect_verified is True
        assert transaction.state == "popen_refused"
        assert physical.process is None
        assert physical.state == "reserved"
        assert transaction.release_before_effect() is True
        assert custody.owner.physical is None
        assert transaction.state == "released"
    finally:
        fixture.cleanup()


@pytest.mark.platforms("windows", arch="x64")
@pytest.mark.asyncio
async def test_windows_resume_failure_retains_assigned_job_and_retries_same_receipt(
    tmp_path, monkeypatch,
):
    """A resume fault after exact Job assignment retains the same suspended child."""
    import psutil

    fixture, binding, custody = await _windows_custody_fixture(
        tmp_path, monkeypatch, "windows-resume-failure",
    )
    calls = 0
    real_resume = psutil.Process.resume

    def fail_once(process):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise KeyboardInterrupt("resume interruption")
        return real_resume(process)

    monkeypatch.setattr(psutil.Process, "resume", fail_once)
    try:
        physical = custody.physical
        handle = physical.handle
        with pytest.raises(KeyboardInterrupt, match="resume"):
            _windows_child(custody, tmp_path, "import time; time.sleep(30)")

        transaction = physical.transaction
        process = transaction.process
        assert calls == 1
        assert transaction.assigned is True
        assert transaction.scope_captured is True
        assert transaction.resumed is False
        assert process is not None
        assert physical.process is process
        assert physical.state == "unknown"
        assert physical.unknown_reason == "partial_launch"
        assert process.poll() is not None

        assert custody.service.reconcile_process_failure(binding, custody.request) is True
        assert physical.handle is handle
        assert physical.process is None
        assert physical.transaction is None
        assert physical.state == "reserved"
    finally:
        fixture.cleanup()


@pytest.mark.platforms("windows", arch="x64")
@pytest.mark.asyncio
async def test_windows_worker_constructor_failure_retains_spawned_child_for_retry(
    tmp_path, monkeypatch,
):
    """A ManagedWorker constructor fault still leaves the exact child reconcilable."""
    from gateway import session_managed_worker
    from hermes_state_runtime import get_session_admission

    fixture, binding, custody = await _windows_custody_fixture(
        tmp_path, monkeypatch, "windows-worker-constructor-failure",
    )
    ref = SimpleNamespace(session_id=binding.session_id, profile_id=binding.profile_id)
    row = get_session_admission(fixture.authority.db, admission_id=binding.admission_id)
    assert row is not None
    original_worker = session_managed_worker.ManagedWorker
    calls = 0

    def fail_once(process):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise KeyboardInterrupt("worker constructor interruption")
        return original_worker(process)

    monkeypatch.setattr(session_managed_worker, "ManagedWorker", fail_once)
    try:
        with pytest.raises(KeyboardInterrupt, match="constructor"):
            await session_managed_worker._spawn_registered_worker(
                fixture.authority, ref, row, custody,
            )
        physical = custody.physical
        handle = physical.handle
        process = physical.process
        assert calls == 2
        assert process is not None
        assert process.poll() is not None
        assert physical.state == "unknown"
        assert physical.unknown_reason == "partial_launch"
        assert ref.session_id not in fixture.authority._managed_spawning

        assert custody.service.reconcile_process_failure(binding, custody.request) is True
        assert physical.handle is handle
        assert physical.process is None
        assert physical.state == "reserved"
    finally:
        fixture.cleanup()


@pytest.mark.platforms("windows", arch="x64")
@pytest.mark.asyncio
async def test_windows_reader_start_failure_retains_same_receipt_for_retry(tmp_path, monkeypatch):
    """An opaque reader-start fault is not treated as a missing reader effect."""
    from gateway.session_managed_worker import ManagedWorker

    class StartRaises:
        def is_alive(self):
            return False

        def start(self):
            raise KeyboardInterrupt("reader start interruption")

        def join(self, timeout=None):
            return None

    fixture, binding, custody = await _windows_custody_fixture(
        tmp_path, monkeypatch, "windows-reader-start-failure",
    )
    worker = None
    try:
        process = _windows_child(custody, tmp_path, "import time; time.sleep(30)")
        custody.bind_windows_transaction(custody.physical.transaction, process)
        worker = ManagedWorker(process)
        custody.attach_worker(worker)
        monkeypatch.setattr("agent.memory_provider.spawn_context_thread", lambda *a, **k: StartRaises())

        with pytest.raises(KeyboardInterrupt, match="reader start"):
            await worker.next_frame(1, ack=0)
        physical = custody.physical
        handle = physical.handle
        assert physical.reader_error == "reader_start_unknown"
        assert physical.unknown_reason == "reader_start_unknown"
        assert physical.process is process

        await asyncio.to_thread(worker.close)
        assert custody.service.reconcile_process_failure(binding, custody.request) is True
        assert physical.handle is handle
        assert physical.process is None
        assert physical.state == "reserved"
        worker = None
    finally:
        if worker is not None:
            await asyncio.to_thread(worker.close)
        fixture.cleanup()


@pytest.mark.platforms("windows", arch="x64")
@pytest.mark.asyncio
async def test_windows_reader_body_baseexception_marks_same_native_receipt_unknown(
    tmp_path, monkeypatch,
):
    """A reader-body BaseException is surfaced and never looks like clean EOF."""
    from gateway import session_managed_worker
    from gateway.session_managed_worker import ManagedWorker
    from hermes_state_runtime import RuntimeStoreError

    fixture, binding, custody = await _windows_custody_fixture(
        tmp_path, monkeypatch, "windows-reader-body-failure",
    )
    worker = None
    try:
        process = _windows_child(
            custody, tmp_path,
            "import json, time; print(json.dumps({'type':'probe'}), flush=True); time.sleep(30)",
        )
        custody.bind_windows_transaction(custody.physical.transaction, process)
        worker = ManagedWorker(process)
        custody.attach_worker(worker)

        def reader_fault(_stream):
            raise KeyboardInterrupt("reader body interruption")

        monkeypatch.setattr(session_managed_worker, "read_frame", reader_fault)
        with pytest.raises(RuntimeStoreError, match="managed_worker_reader_failed"):
            await worker.next_frame(5, ack=0)
        physical = custody.physical
        handle = physical.handle
        assert physical.reader_error == "reader_failed"
        assert physical.unknown_reason == "reader_failed"
        assert physical.process is process

        await asyncio.to_thread(worker.close)
        assert custody.service.reconcile_process_failure(binding, custody.request) is True
        assert physical.handle is handle
        assert physical.process is None
        assert physical.state == "reserved"
        worker = None
    finally:
        if worker is not None:
            await asyncio.to_thread(worker.close)
        fixture.cleanup()


@pytest.mark.platforms("windows", arch="x64")
@pytest.mark.asyncio
async def test_windows_reader_join_failure_retains_same_receipt_for_retry(tmp_path, monkeypatch):
    """A reader join fault retains the exact Job/process until the same retry closes it."""
    from gateway.session_managed_worker import ManagedWorker

    fixture, binding, custody = await _windows_custody_fixture(
        tmp_path, monkeypatch, "windows-reader-join-failure",
    )
    worker = None
    try:
        process = _windows_child(
            custody, tmp_path,
            "import json, time; print(json.dumps({'type':'probe'}), flush=True); time.sleep(30)",
        )
        custody.bind_windows_transaction(custody.physical.transaction, process)
        worker = ManagedWorker(process)
        custody.attach_worker(worker)
        assert await worker.next_frame(5, ack=0) == {"type": "probe"}
        reader = worker.reader
        real_join = reader.join

        def join_fault(timeout=None):
            raise KeyboardInterrupt("reader join interruption")

        monkeypatch.setattr(reader, "join", join_fault)
        with pytest.raises(KeyboardInterrupt, match="reader join"):
            await asyncio.to_thread(worker.close)
        physical = custody.physical
        handle = physical.handle
        assert physical.reader_error is None
        assert physical.unknown_reason == "reader_join_failed"
        assert physical.process is process
        assert process.poll() is not None

        monkeypatch.setattr(reader, "join", real_join)
        await asyncio.to_thread(worker.close)
        assert custody.service.reconcile_process_failure(binding, custody.request) is True
        assert physical.handle is handle
        assert physical.process is None
        assert physical.state == "reserved"
        worker = None
    finally:
        if worker is not None:
            await asyncio.to_thread(worker.close)
        fixture.cleanup()


@pytest.mark.platforms("windows", arch="x64")
@pytest.mark.asyncio
async def test_windows_stdout_close_failure_retains_same_receipt_for_retry(tmp_path, monkeypatch):
    """A native stdout-close fault is UNKNOWN, not a fabricated clean finalization."""
    fixture, binding, custody = await _windows_custody_fixture(
        tmp_path, monkeypatch, "windows-stdout-close-failure",
    )

    class CloseRaises:
        def __init__(self, stream):
            self.stream = stream

        def close(self):
            raise KeyboardInterrupt("stdout close interruption")

        def __getattr__(self, name):
            return getattr(self.stream, name)

    try:
        process = _windows_child(custody, tmp_path, "import sys; sys.exit(0)")
        custody.bind_windows_transaction(custody.physical.transaction, process)
        original_stdout = process.stdout
        process.stdout = CloseRaises(original_stdout)
        assert process.wait(timeout=5) == 0
        worker = __import__("gateway.session_managed_worker", fromlist=["ManagedWorker"]).ManagedWorker(process)
        custody.attach_worker(worker)

        with pytest.raises(KeyboardInterrupt, match="stdout close"):
            await asyncio.to_thread(worker.close)
        physical = custody.physical
        handle = physical.handle
        assert physical.unknown_reason == "reader_close_failed"
        assert physical.reader_error == "reader_close_failed"
        assert physical.process is process

        process.stdout = original_stdout
        original_stdout.close()
        await asyncio.to_thread(worker.close)
        assert custody.service.reconcile_process_failure(binding, custody.request) is True
        assert physical.handle is handle
        assert physical.process is None
        assert physical.state == "reserved"
    finally:
        fixture.cleanup()
