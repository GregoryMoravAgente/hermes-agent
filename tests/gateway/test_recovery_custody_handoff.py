"""Public native-recovery controls for the I10 current-owner handoff."""

import asyncio
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
import threading
from types import SimpleNamespace

import pytest

from gateway.config import GatewayConfig, HomeChannel, Platform, PlatformConfig
from gateway.runtime_custody import RuntimeCustodyCoordinator
from gateway.session import SessionSource, SessionStore
from gateway.session_authority import SessionAuthority
from gateway.session_contract import SessionRef
from hermes_cli.backend_retirement import RetirementFence
from hermes_state import SessionDB
from hermes_state_runtime import (
    admit_session_input,
    begin_runtime_epoch,
    claim_session_input,
    settle_session_input,
)


def _publish_untracked_local_compression(db, parent_session_id, child_session_id):
    """Use the real pre-0.22.2 publication shape whose receipt still needs catch-up."""
    from tests.gateway.test_local_receipt_untracked_compression import _compact_like_0_22_2

    _compact_like_0_22_2(db, parent_session_id, child_session_id)


@asynccontextmanager
async def _real_recovery_runtime(
        tmp_path, monkeypatch, *, fail_delivery=False, fail_pre_effect=False, block=False):
    """Build the real runner/authority/provider boundary used by recovery controls."""
    from gateway.platforms.base import BasePlatformAdapter, SendResult
    from gateway.platforms.event import MessageEvent
    from gateway.run import GatewayRunner
    from gateway.run_runtime import initialize_gateway_runtime
    from gateway.runtime_ownership import process_ownership
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override
    from tests.fakes.fake_llm_provider import FakeLLMServer, Text, write_hermes_home
    import hermes_state

    home = (tmp_path / "profile").resolve()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("TELEGRAM_ALLOWED_USERS", "recovery-user")
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", hermes_state._IMPORT_DEFAULT_DB_PATH)
    home_token = set_hermes_home_override(home)
    started, release = threading.Event(), threading.Event()
    platform_config = PlatformConfig(
        enabled=True,
        token="fixture-token",
        home_channel=HomeChannel(
            platform=Platform.TELEGRAM, chat_id="recovery-chat", name="recovery-fixture",
        ),
    )

    def answer(_record):
        started.set()
        if block:
            assert release.wait(15), "recovery provider gate timed out"
        return Text("recovered")

    class RecoveryAdapter(BasePlatformAdapter):
        def __init__(self):
            super().__init__(platform_config, Platform.TELEGRAM)
            self.deliveries = []
            self.fail_delivery = fail_delivery
            self.fail_pre_effect = fail_pre_effect
            self.pre_effect_failures = 0

        async def connect(self, *, is_reconnect=False):
            return True

        async def disconnect(self):
            return None

        async def _extract_response_content(
                self, response, event, session_key, *, is_ephemeral_response):
            if self.fail_pre_effect:
                self.fail_pre_effect = False
                self.pre_effect_failures += 1
                from gateway.session_ingress import DeliveryBoundaryError
                raise DeliveryBoundaryError("delivery_failed", effect_state="none")
            return await super()._extract_response_content(
                response, event, session_key, is_ephemeral_response=is_ephemeral_response,
            )

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            self.deliveries.append((chat_id, content))
            if self.fail_delivery:
                self.fail_delivery = False
                return SendResult(success=False, error="request timed out", error_kind="unknown")
            return SendResult(success=True, message_id=f"delivery-{len(self.deliveries)}")

        async def edit_message(self, chat_id, message_id, content, *, finalize=False):
            return SendResult(success=True, message_id=message_id)

        async def send_typing(self, chat_id, metadata=None):
            return None

        async def get_chat_info(self, chat_id):
            return {"id": chat_id}

    peer = FakeLLMServer(answer, aux=answer)
    peer.start()
    runner = None
    reserved = False
    try:
        write_hermes_home(home, peer.base_url)
        process_ownership.reserve([home])
        reserved = True
        runner = GatewayRunner(GatewayConfig(
            sessions_dir=home / "sessions",
            platforms={Platform.TELEGRAM: platform_config},
        ))
        adapter = RecoveryAdapter()
        adapter.gateway_runner = runner
        runner.adapters = {Platform.TELEGRAM: adapter}
        await initialize_gateway_runtime(runner)
        runner._wire_adapter_handlers(adapter)
        authority = runner.session_authority

        async def admit(text, *, chat_id="recovery-chat", request_id=None):
            source = adapter.build_source(
                chat_id=chat_id, chat_type="dm", user_id="recovery-user",
            )
            request_id = request_id or text.lower().replace(" ", "-")
            event = MessageEvent(text=text, source=source, message_id=request_id)
            # Keep the production discriminator and I01 producer lease.  The test pauses only
            # the normal post-commit scheduler; recovery then performs the source-owned I01->I10
            # handoff before it claims the queued row.
            schedule_admitted = authority._schedule_admitted
            authority._schedule_admitted = lambda _ref, _receipt=None: None
            try:
                receipt = await authority.admit_native(event)
            finally:
                authority._schedule_admitted = schedule_admitted
            from hermes_state_runtime import get_session_admission
            row = get_session_admission(authority.db, admission_id=receipt.admission_id)
            assert isinstance(row, dict)
            return SimpleNamespace(source=source, session_id=receipt.ref.session_id, row=row)

        yield SimpleNamespace(
            authority=authority, adapter=adapter, peer=peer, started=started, release=release,
            admit=admit, home=home,
        )
    finally:
        release.set()
        if runner is not None:
            runner.session_store.close_all_db_handles()
            runner.close_all_session_db_handles()
        if reserved:
            process_ownership.release(home)
        peer.stop()
        reset_hermes_home_override(home_token)


def _fixture(tmp_path, *, started=False):
    db = SessionDB(tmp_path / "state.db")
    store = SessionStore(tmp_path / "sessions", GatewayConfig())
    store._db = db
    runner = SimpleNamespace(_draining=False, session_store=store)
    source = SessionSource(
        platform=Platform.TELEGRAM, chat_id="recovery-chat", user_id="recovery-user",
        profile="default",
    )
    entry = store.get_or_create_session(source)
    old_epoch = begin_runtime_epoch(db, instance_id="previous-owner")
    row = admit_session_input(
        db, epoch=old_epoch, principal_id="messaging:recovery", session_id=entry.session_id,
        request_id="recovery-native", payload={
            "native_text_v1": {"route": entry.session_key, "source": {},
                               "event": {"message_id": "recovery-native"}},
        },
    )
    if started:
        claim_session_input(db, epoch=old_epoch, session_id=entry.session_id)
    new_epoch = begin_runtime_epoch(db, instance_id="current-owner")
    authority = SessionAuthority(
        runner, profile_id="default", instance_id="current-owner", db=db, epoch=new_epoch,
    )
    authority.runner.runtime_custody = RuntimeCustodyCoordinator(
        process_instance="current-owner", authority_epoch=new_epoch, fence=RetirementFence(),
    )
    return db, authority, source, entry.session_id, entry.session_key, row


@pytest.mark.asyncio
async def test_public_native_recovery_retains_one_current_i10_root(monkeypatch, tmp_path):
    async with _real_recovery_runtime(tmp_path, monkeypatch, block=True) as fixture:
        occurrence = await fixture.admit("BLOCK_I10", request_id="recovery-positive")
        authority = fixture.authority
        assert await authority.recover_native_sessions([(
            occurrence.session_id, occurrence.source, fixture.adapter,
        )]) == {occurrence.session_id: "ready"}
        assert await asyncio.wait_for(asyncio.to_thread(fixture.started.wait, 10), 10)
        admission_id = occurrence.row["admission_id"]
        handle = authority._preservation_handles[admission_id]
        child = authority._preservation_children[admission_id]
        assert handle.owner_class == "I10_startup_recovery_handoff"
        assert handle.profile_id == authority.profile_id
        assert handle.authority_epoch == authority.epoch
        assert child.owner_class == "I04_fifo_turn_execution"
        assert child.lease.parent_lease_id == handle.lease.lease_id
        fixture.release.set()
        live = authority.sessions[occurrence.session_id]
        assert live.task is not None
        await asyncio.wait_for(asyncio.shield(live.task), 30)
        from hermes_state_runtime import get_session_admission
        assert get_session_admission(authority.db, admission_id=admission_id)["status"] == "terminal"
        observed, delivered = _stored_receipt(authority, "startup", admission_id)
        from gateway.session_ingress import DeliveryReceipt
        assert observed.state == "terminal"
        assert isinstance(delivered, DeliveryReceipt)
        assert delivered.admission_id == admission_id and delivered.delivered is True
        assert authority._preservation_children.get(admission_id) is None
        assert authority._preservation_handles.get(admission_id) is None
        assert admission_id not in authority._i10_recovery_bindings
        completed = authority._preservation_coordinator()._completed
        assert completed[child.lease.lease_id] == "terminal"
        assert completed[handle.lease.lease_id] == "terminal"
        assert len(fixture.adapter.deliveries) == 1
        assert fixture.peer.requests


@pytest.mark.asyncio
async def test_managed_local_compression_owns_effect_before_real_sqlite_write(
        monkeypatch, tmp_path):
    """A due policy with no admission uses a real I11 root before the real adoption write."""
    async with _real_recovery_runtime(tmp_path, monkeypatch) as fixture:
        from gateway.session_contract import Principal
        from gateway.session_local import create_local_session
        from gateway.session_local_adoption import adopt_local_compressions
        from hermes_state_local import local_receipt
        from tests.waiting import async_wait_until

        authority = fixture.authority
        actor = Principal(
            'local-owner', authority.profile_id,
            frozenset({'session:create', 'session:read', 'session:submit'}), 'local-test',
        )
        ref = create_local_session(authority, actor, {
            'request_id': 'compression-policy', 'source': 'tui',
            'cwd': str(fixture.home), 'model': 'frozen', 'toolsets': [],
        })
        original = local_receipt(authority.db, ref.session_id)
        parent = original['entry']['session_id']
        child = f'{parent}-compression-child'
        _publish_untracked_local_compression(authority.db, parent, child)

        holder = sqlite3.connect(str(authority.db.db_path), timeout=0.1)
        holder.execute('BEGIN IMMEDIATE')
        task = asyncio.create_task(adopt_local_compressions(authority))
        await async_wait_until(
            lambda: any(record.get('owner_class') == 'I11_session_mutations_finalizers'
                        for record in authority._preservation_operations.values()),
            timeout=10, what='the pre-effect local compression operation',
        )
        record = next(record for record in authority._preservation_operations.values()
                      if record.get('owner_class') == 'I11_session_mutations_finalizers')
        assert record['parent_handle'] is None
        assert record['source_witness'].profile_id == authority.profile_id
        assert record['source_witness'].authority_epoch == authority.epoch
        assert record['source_witness'].session_id == ref.session_id
        assert record['source_witness'].effect_state == 'planned'
        assert record['source_witness'].physical_session_id == parent
        assert record['source_witness'].lineage == (ref.session_id,)
        assert record['source_witness'].expected_physical_session_id == child
        assert record['source_witness'].expected_lineage[-1] == child
        assert record['source_identity'] == authority._local_compression_source_identity(
            record['source_witness'],
        )
        from dataclasses import replace
        copied_source = replace(record['source_witness'], profile_id='copied-profile')
        assert authority._local_compression_source_current(copied_source) is False
        assert getattr(authority, '_i10_recovery_bindings', {}) == {}
        holder.rollback()
        holder.close()
        await task

        stored = local_receipt(authority.db, ref.session_id)
        assert stored['entry']['session_id'] == child
        assert stored['lineage'][-1] == child
        assert not authority._preservation_operations


@pytest.mark.asyncio
async def test_managed_local_compression_epoch_failure_retains_unknown(
        monkeypatch, tmp_path):
    """A real epoch fence after pre-effect reservation leaves the exact I11 root UNKNOWN."""
    async with _real_recovery_runtime(tmp_path, monkeypatch) as fixture:
        from gateway.session_contract import Principal
        from gateway.session_local import create_local_session
        from gateway.session_local_adoption import adopt_local_compressions
        from hermes_state_local import local_receipt
        from hermes_state_runtime import RuntimeStoreError
        from tests.waiting import async_wait_until

        authority = fixture.authority
        actor = Principal(
            'local-owner', authority.profile_id,
            frozenset({'session:create', 'session:read', 'session:submit'}), 'local-test',
        )
        ref = create_local_session(authority, actor, {
            'request_id': 'compression-epoch-failure', 'source': 'tui',
            'cwd': str(fixture.home), 'model': 'frozen', 'toolsets': [],
        })
        original = local_receipt(authority.db, ref.session_id)
        parent = original['entry']['session_id']
        _publish_untracked_local_compression(authority.db, parent, f'{parent}-epoch-child')

        holder = sqlite3.connect(str(authority.db.db_path), timeout=0.1)
        holder.execute('BEGIN IMMEDIATE')
        task = asyncio.create_task(adopt_local_compressions(authority))
        await async_wait_until(
            lambda: any(record.get('owner_class') == 'I11_session_mutations_finalizers'
                        for record in authority._preservation_operations.values()),
            timeout=10, what='the pre-effect local compression operation',
        )
        holder.execute('UPDATE runtime_epoch SET epoch = epoch + 1 WHERE singleton = 1')
        holder.commit()
        holder.close()
        await task

        record = next(record for record in authority._preservation_operations.values()
                      if record.get('owner_class') == 'I11_session_mutations_finalizers')
        handle = record['handle']
        coordinator_record = authority._preservation_coordinator()._records[handle.lease.lease_id]
        assert record['unknown'] is True
        assert coordinator_record.state == 'unknown'
        assert 'local_recovery' not in getattr(authority, '_i10_consumer_evidence', {})

        with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
            authority._preservation_operation_start(
                'I11_session_mutations_finalizers', principal_id='local-owner',
                session_id=ref.session_id, operation='local-compression-adoption',
                source_identity='local_recovery_compression',
            )
        with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
            authority._preservation_operation_start(
                'I11_session_mutations_finalizers', principal_id='local-owner',
                session_id=ref.session_id, operation='local-compression-adoption',
                source_identity=(
                    f"{authority._local_compression_source_identity(record['source_witness'])}:forged"
                ), source_proof=record['source_witness'],
            )
        with pytest.raises(RuntimeStoreError, match='preservation_owner_unmapped'):
            authority._preservation_operation_start(
                'I03_api_run_direct', principal_id=record['source_witness'].principal_id,
                session_id=record['source_witness'].session_id,
                operation='direct-run', request_id=record['source_witness'].request_id,
                source_identity=authority._local_compression_source_identity(
                    record['source_witness'],
                ), source_proof=record['source_witness'],
            )


@pytest.mark.asyncio
async def test_managed_local_compression_pre_effect_failure_retries_same_i11_handle(
        monkeypatch, tmp_path):
    """A real writer-lock failure retries the reserved source operation without a new handle."""
    async with _real_recovery_runtime(tmp_path, monkeypatch) as fixture:
        from gateway.session_contract import Principal
        from gateway.session_local import create_local_session
        from gateway.session_local_adoption import adopt_local_compressions
        from hermes_state_local import local_receipt
        from tests.waiting import async_wait_until

        authority = fixture.authority
        actor = Principal(
            'local-owner', authority.profile_id,
            frozenset({'session:create', 'session:read', 'session:submit'}), 'local-test',
        )
        ref = create_local_session(authority, actor, {
            'request_id': 'compression-pre-effect-retry', 'source': 'tui',
            'cwd': str(fixture.home), 'model': 'frozen', 'toolsets': [],
        })
        original = local_receipt(authority.db, ref.session_id)
        parent = original['entry']['session_id']
        child = f'{parent}-pre-effect-child'
        _publish_untracked_local_compression(authority.db, parent, child)

        holder = sqlite3.connect(str(authority.db.db_path), timeout=0.1)
        holder.execute('BEGIN IMMEDIATE')
        first = asyncio.create_task(adopt_local_compressions(authority))
        await async_wait_until(
            lambda: any(record.get('owner_class') == 'I11_session_mutations_finalizers'
                        for record in authority._preservation_operations.values()),
            timeout=10, what='the retained pre-effect local compression operation',
        )
        record = next(record for record in authority._preservation_operations.values()
                      if record.get('owner_class') == 'I11_session_mutations_finalizers')
        handle = record['handle']
        with pytest.raises(sqlite3.OperationalError):
            await asyncio.wait_for(asyncio.shield(first), 30)
        assert record['unknown'] is True
        assert authority._preservation_coordinator()._records[handle.lease.lease_id].state == 'unknown'
        assert local_receipt(authority.db, ref.session_id)['entry']['session_id'] == parent
        holder.rollback()
        holder.close()

        await adopt_local_compressions(authority)
        assert local_receipt(authority.db, ref.session_id)['entry']['session_id'] == child
        assert not authority._preservation_operations
        assert authority._preservation_coordinator()._completed[handle.lease.lease_id] == 'terminal'


@pytest.mark.asyncio
async def test_managed_local_compression_committed_readback_fault_replays_same_i11_handle(
        monkeypatch, tmp_path):
    """A real write plus a readback/terminal refusal replays the same exact I11 handle."""
    async with _real_recovery_runtime(tmp_path, monkeypatch) as fixture:
        from gateway.runtime_custody import CustodyRefusal
        from gateway.session_contract import Principal
        from gateway.session_local import create_local_session
        from gateway.session_local_adoption import adopt_local_compressions
        from hermes_state_local import local_receipt
        from hermes_state_runtime import RuntimeStoreError
        from tests.waiting import async_wait_until

        authority = fixture.authority
        actor = Principal(
            'local-owner', authority.profile_id,
            frozenset({'session:create', 'session:read', 'session:submit'}), 'local-test',
        )
        ref = create_local_session(authority, actor, {
            'request_id': 'compression-readback-retry', 'source': 'tui',
            'cwd': str(fixture.home), 'model': 'frozen', 'toolsets': [],
        })
        original = local_receipt(authority.db, ref.session_id)
        parent = original['entry']['session_id']
        child = f'{parent}-readback-child'
        _publish_untracked_local_compression(authority.db, parent, child)

        original_current_witness = authority._local_compression_current_witness
        readback_fault = True
        terminal_fault = False

        def fail_one_readback(source, *, committed=False):
            nonlocal readback_fault
            if committed and readback_fault:
                readback_fault = False
                raise RuntimeStoreError('read_failed')
            return original_current_witness(source, committed=committed)

        coordinator = authority._preservation_coordinator()
        original_mark_terminal = coordinator.mark_terminal

        def refuse_one_terminal(handle):
            nonlocal terminal_fault
            if terminal_fault:
                terminal_fault = False
                return CustodyRefusal('lease_release_failed', handle.owner_class)
            return original_mark_terminal(handle)

        monkeypatch.setattr(authority, '_local_compression_current_witness', fail_one_readback)
        task = asyncio.create_task(adopt_local_compressions(authority))
        await async_wait_until(
            lambda: any(record.get('owner_class') == 'I11_session_mutations_finalizers'
                        for record in authority._preservation_operations.values()),
            timeout=10, what='the readback-fault local compression operation',
        )
        record = next(record for record in authority._preservation_operations.values()
                      if record.get('owner_class') == 'I11_session_mutations_finalizers')
        handle = record['handle']
        await task
        assert readback_fault is False
        assert record['unknown'] is True
        assert local_receipt(authority.db, ref.session_id)['entry']['session_id'] == child

        terminal_fault = True
        monkeypatch.setattr(coordinator, 'mark_terminal', refuse_one_terminal)
        await adopt_local_compressions(authority)
        assert terminal_fault is False
        assert record['unknown'] is True
        assert authority._preservation_coordinator()._records[handle.lease.lease_id].state == 'unknown'
        assert local_receipt(authority.db, ref.session_id)['entry']['session_id'] == child

        monkeypatch.setattr(coordinator, 'mark_terminal', original_mark_terminal)
        await adopt_local_compressions(authority)
        assert not authority._preservation_operations
        assert authority._preservation_coordinator()._completed[handle.lease.lease_id] == 'terminal'


@pytest.mark.asyncio
async def test_real_local_recovery_binds_source_lineage_and_delivery(
        monkeypatch, tmp_path):
    """The real managed local source and real provider delivery close one exact I10 root."""
    async with _real_recovery_runtime(tmp_path, monkeypatch) as fixture:
        from tests.fakes.fake_llm_provider import MODEL_ID
        from gateway.session_contract import Principal, Submission
        from gateway.session_local import create_local_session
        from gateway.session_local_recovery import recover_local_sessions
        from hermes_state_local import local_receipt
        from hermes_state_runtime import get_session_admission

        authority = fixture.authority
        actor = Principal(
            'local-owner', authority.profile_id,
            frozenset({'session:create', 'session:read', 'session:submit'}), 'local-test',
        )
        ref = create_local_session(authority, actor, {
            'request_id': 'local-recovery-positive', 'source': 'tui',
            'cwd': str(fixture.home), 'model': MODEL_ID, 'toolsets': [],
        })
        target = local_receipt(authority.db, ref.session_id)['entry']['session_id']
        authority.db.end_session(target, 'tui_shutdown')
        schedule_admitted = authority._schedule_admitted
        authority._schedule_admitted = lambda _ref, _receipt=None: None
        try:
            admission_receipt = await authority.submit(
                actor, Submission(
                    'local-recovery-turn', ref, {'text': 'recover local'}, 'queue',
                ),
            )
        finally:
            authority._schedule_admitted = schedule_admitted
        admission = get_session_admission(authority.db, admission_id=admission_receipt.admission_id)
        assert admission['status'] == 'queued'
        authority.sessions.pop(ref.session_id)

        recover_local_sessions(authority, schedule=True, allow_legacy=False)
        live = authority.sessions[ref.session_id]
        handle = authority._preservation_handles[admission['admission_id']]
        await asyncio.wait_for(asyncio.shield(live.task), 30)

        assert get_session_admission(
            authority.db, admission_id=admission['admission_id'],
        )['status'] == 'terminal'
        observed_values = authority._i10_consumer_evidence['local_recovery']
        observed_values = observed_values if isinstance(observed_values, tuple) else (observed_values,)
        observed = next(item for item in observed_values if (
            item.identity is not None
            and item.identity.admission_id == admission['admission_id']
        ))
        from gateway.session_ingress import DeliveryReceipt
        from gateway.session_recovery_custody import LocalRecoverySourceReceipt
        source_receipt = next(
            receipt for receipt in observed.source_receipts
            if isinstance(receipt, LocalRecoverySourceReceipt)
        )
        delivery_receipt = next(
            receipt for receipt in observed.source_receipts
            if isinstance(receipt, DeliveryReceipt)
        )
        assert observed.state == 'terminal'
        assert observed.handle is handle
        assert source_receipt.logical_session_id == ref.session_id
        assert source_receipt.physical_session_id == target
        assert source_receipt.policy_principal_id == actor.subject
        assert source_receipt.policy_request_id == 'local-recovery-positive'
        assert source_receipt.admission_principal_id == admission['principal_id']
        assert source_receipt.admission_request_id == 'local-recovery-turn'
        assert source_receipt.lineage == (ref.session_id,)
        assert source_receipt.profile_id == authority.profile_id
        assert source_receipt.authority_epoch == authority.epoch
        assert source_receipt.process_instance == authority.instance_id
        assert delivery_receipt.admission_id == admission['admission_id']
        assert delivery_receipt.attempted is True
        assert delivery_receipt.delivered is True
        assert delivery_receipt.effect_state == 'delivered'
        assert authority._preservation_handles.get(admission['admission_id']) is None
        assert admission['admission_id'] not in authority._i10_recovery_bindings
        assert fixture.peer.requests


@pytest.mark.asyncio
async def test_recovery_without_current_profile_lock_stays_unknown(monkeypatch, tmp_path):
    db, authority, source, session_id, route, row = _fixture(tmp_path)

    async def route_check(_runner, _payload, _target, _source, _adapter):
        return source, route

    monkeypatch.setattr("gateway.session_envelope.check_native_route", route_check)
    try:
        result = await authority.recover_native_sessions([(session_id, source, object())])
        assert result == {session_id: "foreign_custody"}
        assert row["admission_id"] not in authority._preservation_handles
    finally:
        db.close()


@pytest.mark.asyncio
async def test_started_native_row_never_mints_restart_root(monkeypatch, tmp_path):
    db, authority, source, session_id, route, row = _fixture(tmp_path, started=True)

    async def route_check(_runner, _payload, _target, _source, _adapter):
        return source, route

    monkeypatch.setattr("gateway.session_envelope.check_native_route", route_check)
    from gateway.runtime_ownership import process_ownership

    home = Path(db.db_path).parent.resolve()
    process_ownership.reserve([home])
    try:
        result = await authority.recover_native_sessions([(session_id, source, object())])
        assert result == {session_id: "unknown_execution"}
        assert row["admission_id"] not in authority._preservation_handles
    finally:
        process_ownership.release(home)
        db.close()


@pytest.mark.asyncio
async def test_recovery_consumers_share_the_publicly_recovered_current_root(monkeypatch, tmp_path):
    db, authority, source, session_id, route, row = _fixture(tmp_path)
    authority._schedule = lambda _ref: None

    async def route_check(_runner, _payload, _target, _source, _adapter):
        return source, route

    monkeypatch.setattr("gateway.session_envelope.check_native_route", route_check)
    from gateway.runtime_ownership import process_ownership
    from gateway.session_recovery_custody import (
        CONSUMERS,
        RecoveryConsumerReceipt,
        observe_consumer_rows,
    )

    home = Path(db.db_path).parent.resolve()
    process_ownership.reserve([home])
    try:
        assert await authority.recover_native_sessions([(session_id, source, object())]) == {
            session_id: "ready"
        }
        handle = authority._preservation_handles[row["admission_id"]]
        for consumer in CONSUMERS:
            assert observe_consumer_rows(authority, consumer, rows=[row]) is True
            stored = authority._i10_consumer_evidence[consumer]
            receipts = stored if isinstance(stored, tuple) else (stored,)
            assert any(
                isinstance(receipt, RecoveryConsumerReceipt)
                and receipt.handle is handle
                and receipt.identity.admission_id == row["admission_id"]
                for receipt in receipts
            )
    finally:
        process_ownership.release(home)
        db.close()


@pytest.mark.asyncio
async def test_recovery_terminal_preflight_requires_same_occurrence_delivery_receipt(
        monkeypatch, tmp_path):
    db, authority, source, session_id, route, row = _fixture(tmp_path)
    authority._schedule = lambda _ref: None

    async def route_check(_runner, _payload, _target, _source, _adapter):
        return source, route

    monkeypatch.setattr("gateway.session_envelope.check_native_route", route_check)
    from gateway.runtime_ownership import process_ownership
    from gateway.session_ingress import DeliveryReceipt
    from gateway.session_recovery_custody import prepare_binding_retirement

    home = Path(db.db_path).parent.resolve()
    process_ownership.reserve([home])
    try:
        assert await authority.recover_native_sessions([(session_id, source, object())]) == {
            session_id: "ready"
        }
        ref = SessionRef(authority.profile_id, session_id)
        child = authority._preservation_reserve_claim(ref, row)
        claimed = claim_session_input(db, epoch=authority.epoch, session_id=session_id)
        assert isinstance(claimed, dict)
        authority._preservation_activate_claim(row["admission_id"], claimed, child)
        authority._preservation_bind_claim(row["admission_id"], claimed)
        settled = settle_session_input(
            db, epoch=authority.epoch, admission_id=row["admission_id"],
            generation=claimed["generation"], outcome="completed",
        )
        wrong = DeliveryReceipt("different-admission", True, True, effect_state="delivered")
        assert prepare_binding_retirement(authority, row["admission_id"], terminal_receipt=wrong) is None
        correct = DeliveryReceipt(row["admission_id"], True, True, effect_state="delivered")
        assert settled["status"] == "terminal"
        # A same-shaped receipt constructed by a caller is not the source-produced object bound to
        # this occurrence.  The terminal preflight must refuse it until the real delivery path
        # supplies and binds its own receipt instance.
        assert prepare_binding_retirement(
            authority, row["admission_id"], terminal_receipt=correct,
        ) is None
    finally:
        process_ownership.release(home)
        db.close()


def _stored_receipt(authority, consumer, admission_id):
    from gateway.session_ingress import DeliveryReceipt
    from gateway.session_recovery_custody import RecoveryConsumerReceipt

    stored = authority._i10_consumer_evidence[consumer]
    receipts = stored if isinstance(stored, tuple) else (stored,)
    matching = [item for item in receipts if (
        isinstance(item, RecoveryConsumerReceipt)
        and item.identity is not None
        and item.identity.admission_id == admission_id
    )]
    assert matching, (consumer, admission_id, receipts)
    source_receipts = matching[-1].source_receipts
    delivery_receipts = [item for item in source_receipts if isinstance(item, DeliveryReceipt)]
    assert delivery_receipts, (consumer, admission_id, source_receipts)
    return matching[-1], delivery_receipts[-1]


@pytest.mark.asyncio
async def test_recovery_pre_effect_failure_retries_same_i10_handles(monkeypatch, tmp_path):
    async with _real_recovery_runtime(
            tmp_path, monkeypatch, fail_pre_effect=True, block=True) as fixture:
        authority = fixture.authority
        occurrence = await fixture.admit("PRE_EFFECT_RETRY", request_id="recovery-pre-effect-retry")
        assert await authority.recover_native_sessions([(
            occurrence.session_id, occurrence.source, fixture.adapter,
        )]) == {occurrence.session_id: "ready"}
        assert await asyncio.wait_for(asyncio.to_thread(fixture.started.wait, 10), 10)
        admission_id = occurrence.row["admission_id"]
        parent = authority._preservation_handles[admission_id]
        child = authority._preservation_children[admission_id]
        lease_ids = {parent.lease.lease_id, child.lease.lease_id}
        fixture.release.set()
        live = authority.sessions[occurrence.session_id]
        assert live.task is not None
        await asyncio.wait_for(asyncio.shield(live.task), 30)

        from hermes_state_runtime import get_session_admission
        assert get_session_admission(authority.db, admission_id=admission_id)["status"] == "terminal"
        _observed, delivered = _stored_receipt(authority, "startup", admission_id)
        assert delivered.delivered is True
        assert delivered.effect_state == "delivered"
        assert fixture.adapter.pre_effect_failures == 1
        assert len(fixture.adapter.deliveries) == 1
        assert authority._preservation_handles.get(admission_id) is None
        assert authority._preservation_children.get(admission_id) is None
        assert admission_id not in authority._i10_recovery_bindings
        completed = authority._preservation_coordinator()._completed
        assert all(completed.get(lease_id) == "terminal" for lease_id in lease_ids)


@pytest.mark.asyncio
async def test_pre_effect_retry_cannot_bypass_simultaneous_classic_marker(
        monkeypatch, tmp_path):
    """A real classic marker remains an obligation while I10 retains its retry window."""
    async with _real_recovery_runtime(
            tmp_path, monkeypatch, fail_pre_effect=True, block=True) as fixture:
        authority = fixture.authority
        occurrence = await fixture.admit(
            "RETRY_WITH_CLASSIC_MARKER", request_id="recovery-classic-marker",
        )
        assert await authority.recover_native_sessions([(
            occurrence.session_id, occurrence.source, fixture.adapter,
        )]) == {occurrence.session_id: "ready"}
        assert await asyncio.wait_for(asyncio.to_thread(fixture.started.wait, 10), 10)
        admission_id = occurrence.row["admission_id"]
        from gateway.platforms.event import MessageEvent

        classic_event = MessageEvent(
            text="classic collision",
            source=occurrence.source,
            message_id=occurrence.row["request_id"],
        )
        classic_event._turn_marker_handoff = True
        session_key = authority.runner._session_key_for_source(occurrence.source)
        assert authority._classic_begin(classic_event, session_key, "fifo") is True
        from hermes_state_runtime import get_session_admission
        row = get_session_admission(authority.db, admission_id=admission_id)
        assert authority._classic_admission_committed(classic_event, row) is True
        assert authority.marker_adopters[admission_id] is classic_event

        fixture.release.set()
        live = authority.sessions[occurrence.session_id]
        assert live.task is not None
        await asyncio.wait_for(asyncio.shield(live.task), 30)

        from gateway.session_classic_delivery import (
            CLASSIC_CALLBACK_UNKNOWN, classic_callback_requirement,
        )
        from gateway.session_recovery_custody import i10_pre_effect_retry_pending

        assert i10_pre_effect_retry_pending(authority, admission_id) is True
        assert classic_callback_requirement(authority, admission_id) == CLASSIC_CALLBACK_UNKNOWN
        assert authority._preservation_delivery_retries.get(admission_id, 0) == 0
        assert authority._preservation_handles.get(admission_id) is not None
        assert authority._preservation_children.get(admission_id) is not None


@pytest.mark.asyncio
async def test_recovery_failure_retains_same_handles_and_rejects_foreign_receipt(
        monkeypatch, tmp_path):
    async with _real_recovery_runtime(tmp_path, monkeypatch, fail_delivery=True) as fixture:
        authority = fixture.authority
        first = await fixture.admit("FIRST_FAILURE", request_id="recovery-failure")
        assert await authority.recover_native_sessions([(
            first.session_id, first.source, fixture.adapter,
        )]) == {first.session_id: "ready"}
        first_task = authority.sessions[first.session_id].task
        assert first_task is not None
        await asyncio.wait_for(asyncio.shield(first_task), 30)
        first_id = first.row["admission_id"]
        first_parent = authority._preservation_handles[first_id]
        first_child = authority._preservation_children[first_id]
        from hermes_state_runtime import get_session_admission
        assert get_session_admission(authority.db, admission_id=first_id)["status"] == "terminal"
        assert first_parent.lease.parent_lease_id is None
        assert first_child.lease.parent_lease_id == first_parent.lease.lease_id
        assert first_id in authority._i10_recovery_bindings
        assert authority._preservation_handles[first_id] is first_parent
        assert authority._preservation_children[first_id] is first_child
        first_records = authority._preservation_coordinator()._records
        assert first_parent.lease.lease_id in first_records
        assert first_child.lease.lease_id in first_records
        assert first_records[first_parent.lease.lease_id].state == "unknown"
        assert first_records[first_child.lease.lease_id].state != "terminal"
        from gateway.session_ingress import DeliveryReceipt
        from gateway.session_recovery_custody import reconcile_binding_after_delivery
        forged = DeliveryReceipt(first_id, True, True, effect_state="delivered")
        assert reconcile_binding_after_delivery(authority, first_id, forged) is False
        authority._preservation_release_terminal(first_id, terminal_receipt=forged)
        assert authority._preservation_handles[first_id] is first_parent
        assert authority._preservation_children[first_id] is first_child

        second = await fixture.admit("SECOND_SUCCESS", chat_id="recovery-chat-2",
                                     request_id="recovery-success")
        assert await authority.recover_native_sessions([(
            second.session_id, second.source, fixture.adapter,
        )]) == {second.session_id: "ready"}
        second_task = authority.sessions[second.session_id].task
        assert second_task is not None
        await asyncio.wait_for(asyncio.shield(second_task), 30)
        _second_observed, foreign_receipt = _stored_receipt(
            authority, "startup", second.row["admission_id"])
        assert isinstance(foreign_receipt, DeliveryReceipt)
        assert foreign_receipt.admission_id == second.row["admission_id"]
        authority._preservation_release_terminal(first_id, terminal_receipt=foreign_receipt)
        assert first_id in authority._i10_recovery_bindings
        assert authority._preservation_handles[first_id] is first_parent
        assert authority._preservation_children[first_id] is first_child
        assert first_child.lease.lease_id in authority._preservation_coordinator()._records


@pytest.mark.asyncio
async def test_partial_reconcile_and_bind_failure_retain_same_unknown_handles(
        monkeypatch, tmp_path):
    """A partial retry/refusal cannot release or duplicate an I10 occurrence."""
    async with _real_recovery_runtime(tmp_path, monkeypatch, block=True) as fixture:
        authority = fixture.authority
        occurrence = await fixture.admit("PARTIAL_RECONCILE", request_id="recovery-partial")
        assert await authority.recover_native_sessions([(
            occurrence.session_id, occurrence.source, fixture.adapter,
        )]) == {occurrence.session_id: "ready"}
        assert await asyncio.wait_for(asyncio.to_thread(fixture.started.wait, 10), 10)
        admission_id = occurrence.row["admission_id"]
        parent = authority._preservation_handles[admission_id]
        child = authority._preservation_children[admission_id]
        original_release = authority._preservation_release_terminal
        authority._preservation_release_terminal = lambda *_args, **_kwargs: None
        try:
            fixture.release.set()
            task = authority.sessions[occurrence.session_id].task
            assert task is not None
            await asyncio.wait_for(asyncio.shield(task), 30)
        finally:
            authority._preservation_release_terminal = original_release

        _observed, delivered = _stored_receipt(authority, "startup", admission_id)
        coordinator = authority._preservation_coordinator()
        coordinator.mark_unknown(parent, "delivery_reconcile_failed")
        coordinator.mark_unknown(child, "delivery_reconcile_failed")
        original_reconcile = coordinator.reconcile_unknown
        calls = 0

        def fail_second_reconcile(handle, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                from gateway.runtime_custody import CustodyRefusal
                return CustodyRefusal("injected_reconcile_failure", handle.owner_class)
            return original_reconcile(handle, **kwargs)

        monkeypatch.setattr(coordinator, "reconcile_unknown", fail_second_reconcile)
        from gateway.session_recovery_custody import reconcile_binding_after_delivery
        assert reconcile_binding_after_delivery(authority, admission_id, delivered) is False
        assert calls == 2
        assert coordinator._records[parent.lease.lease_id].state == "unknown"
        assert coordinator._records[child.lease.lease_id].state == "unknown"
        assert authority._preservation_handles[admission_id] is parent
        assert authority._preservation_children[admission_id] is child
        assert len(fixture.adapter.deliveries) == 1

        monkeypatch.setattr(coordinator, "reconcile_unknown", original_reconcile)
        from gateway import session_recovery_custody
        monkeypatch.setattr(
            session_recovery_custody, "bind_terminal_receipt",
            lambda *_args, **_kwargs: False,
        )
        assert session_recovery_custody.reconcile_and_bind_i10_delivery(
            authority, admission_id, delivered,
        ) is False
        assert coordinator._records[parent.lease.lease_id].state == "unknown"
        assert coordinator._records[child.lease.lease_id].state == "unknown"
        assert authority._preservation_handles[admission_id] is parent
        assert authority._preservation_children[admission_id] is child
        assert len(fixture.adapter.deliveries) == 1



def _publish_due_local_compression(authority, actor, request_id):
    from gateway.session_local import create_local_session
    from hermes_state_local import local_receipt

    ref = create_local_session(authority, actor, {
        'request_id': request_id, 'source': 'tui',
        'cwd': str(Path(authority.db.db_path).parent), 'model': 'frozen', 'toolsets': [],
    })
    original = local_receipt(authority.db, ref.session_id)
    parent = original['entry']['session_id']
    child = f'{parent}-{request_id}-child'
    _publish_untracked_local_compression(authority.db, parent, child)
    return ref, parent, child


@pytest.mark.asyncio
async def test_managed_local_compression_reserves_every_active_local_parent_before_write(
        monkeypatch, tmp_path):
    """A FIFO with two local admissions keeps both exact I10 roots on the I11 operation."""
    async with _real_recovery_runtime(tmp_path, monkeypatch) as fixture:
        from gateway.session_contract import Principal
        from gateway.session_local_adoption import adopt_local_compressions
        from hermes_state_runtime import get_session_admission
        from tests.waiting import async_wait_until

        authority = fixture.authority
        actor = Principal(
            'local-owner', authority.profile_id,
            frozenset({'session:create', 'session:read', 'session:submit'}), 'local-test',
        )
        ref, _parent, _child = _publish_due_local_compression(
            authority, actor, 'compression-two-local-parents',
        )
        rows = [
            admit_session_input(
                authority.db, epoch=authority.epoch, principal_id='local-owner',
                session_id=ref.session_id, request_id=f'local-parent-{index}',
                payload={'text': f'queued local {index}'},
            )
            for index in (1, 2)
        ]
        holder = sqlite3.connect(str(authority.db.db_path), timeout=0.1)
        holder.execute('BEGIN IMMEDIATE')
        task = asyncio.create_task(adopt_local_compressions(authority))
        await async_wait_until(
            lambda: any(record.get('owner_class') == 'I11_session_mutations_finalizers'
                        for record in authority._preservation_operations.values()),
            timeout=10, what='the multi-parent local compression operation',
        )
        record = next(record for record in authority._preservation_operations.values()
                      if record.get('owner_class') == 'I11_session_mutations_finalizers')
        parents = record['parent_handles']
        expected = tuple(authority._preservation_handles[row['admission_id']] for row in rows)
        assert parents == expected
        assert record['parent_handle'] is expected[0]
        assert all(
            authority._preservation_coordinator()._records[parent.lease.lease_id].state == 'accepted'
            for parent in parents
        )
        assert all(get_session_admission(authority.db, admission_id=row['admission_id'])['status'] == 'queued'
                   for row in rows)
        holder.rollback()
        holder.close()
        await task
        assert not authority._preservation_operations


@pytest.mark.asyncio
async def test_managed_local_compression_retry_reopens_same_i11_and_all_i10_parents(
        monkeypatch, tmp_path):
    """A failed write reopens the same child and every exact queued parent atomically."""
    async with _real_recovery_runtime(tmp_path, monkeypatch) as fixture:
        from gateway.session_contract import Principal
        import gateway.session_local_adoption as adoption

        authority = fixture.authority
        actor = Principal(
            'local-owner', authority.profile_id,
            frozenset({'session:create', 'session:read', 'session:submit'}), 'local-test',
        )
        ref, _parent, _child = _publish_due_local_compression(
            authority, actor, 'compression-retry-two-local-parents',
        )
        rows = [
            admit_session_input(
                authority.db, epoch=authority.epoch, principal_id='local-owner',
                session_id=ref.session_id, request_id=f'retry-local-parent-{index}',
                payload={'text': f'queued retry {index}'},
            )
            for index in (1, 2)
        ]
        original_adopt = adoption._adopt
        attempts = []

        def fail_once(*args, **kwargs):
            if not attempts:
                attempts.append(True)
                from hermes_state_runtime import RuntimeStoreError
                raise RuntimeStoreError('read_failed')
            return original_adopt(*args, **kwargs)

        monkeypatch.setattr(adoption, '_adopt', fail_once)
        await adoption.adopt_local_compressions(authority)
        record = next(record for record in authority._preservation_operations.values()
                      if record.get('owner_class') == 'I11_session_mutations_finalizers')
        child = record['handle']
        parents = record['parent_handles']
        assert record['unknown'] is True
        assert all(
            authority._preservation_coordinator()._records[parent.lease.lease_id].state == 'unknown'
            for parent in parents
        )
        await adoption.adopt_local_compressions(authority)
        assert not authority._preservation_operations
        assert authority._preservation_coordinator()._completed[child.lease.lease_id] == 'terminal'
        assert all(
            authority._preservation_handles[row['admission_id']] is parents[index]
            for index, row in enumerate(rows)
        )


@pytest.mark.asyncio
async def test_managed_local_compression_lost_terminal_ack_retires_exact_operation(
        monkeypatch, tmp_path):
    """A committed coordinator terminal with a lost return value is reconciled by full identity."""
    async with _real_recovery_runtime(tmp_path, monkeypatch) as fixture:
        from gateway.session_contract import Principal
        from gateway.session_local_adoption import adopt_local_compressions
        from gateway.runtime_custody import CustodyRefusal

        authority = fixture.authority
        actor = Principal(
            'local-owner', authority.profile_id,
            frozenset({'session:create', 'session:read', 'session:submit'}), 'local-test',
        )
        _ref, _parent, child = _publish_due_local_compression(
            authority, actor, 'compression-lost-terminal-ack',
        )
        coordinator = authority._preservation_coordinator()
        original_terminal = coordinator.mark_terminal
        retired = []

        def commit_then_lose_ack(handle):
            result = original_terminal(handle)
            if result is None:
                retired.append(handle)
                return CustodyRefusal('stale_lease', handle.owner_class)
            return result

        monkeypatch.setattr(coordinator, 'mark_terminal', commit_then_lose_ack)
        await adopt_local_compressions(authority)
        assert len(retired) == 1
        lease_id = retired[0].lease.lease_id
        assert not authority._preservation_operations
        assert coordinator._completed.get(lease_id) == 'terminal'
        assert lease_id not in coordinator._records
        from hermes_state_local import local_receipt
        assert local_receipt(authority.db, _ref.session_id)['lineage'][-1] == child
