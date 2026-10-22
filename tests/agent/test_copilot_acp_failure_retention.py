"""Managed ACP failure retention and same-handle retry controls.

These tests consume the real admission/authority fixture and typed external-process operations.
The managed cases inject only the failure boundary under test.  The catalog case reserves a real standalone physical owner and injects a single close
refusal while retaining its actual child/readers/receipt. Windows cases are platform-marked
controls over the real launch transaction.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

from agent.copilot_acp_client import CopilotACPClient
from gateway.runtime_service_custody import reserve_service
from gateway.runtime_service_operations import (
    ProducerPolicy,
    parent_for_managed_service,
    provider_process_kwargs,
)
from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding


_HOLD_CHILD = "import time; time.sleep(60)"


async def _managed_client(tmp_path, monkeypatch, request_id):
    fixture = await real_runtime_binding(tmp_path, monkeypatch)
    binding = await fixture.claim(request_id)
    service = reserve_service(binding, f"service:{request_id}", producer_key="acp")
    parent = parent_for_managed_service(binding, service, operation_id=binding.admission_id)
    request = service.begin_request(binding)
    fixture.authority._preservation_bind_sidecar(
        binding.admission_id, binding.parent_handle, service, request,
    )
    policy = ProducerPolicy("managed_turn", parent, True, False, False)
    # Consume the same provider boundary used by a managed agent.  The returned kwargs are
    # intentionally checked so the test cannot bypass policy construction with a hand-built client.
    kwargs = provider_process_kwargs(
        SimpleNamespace(_external_process_policy=policy),
        SimpleNamespace(auth_type="external_process"),
        {"base_url": "acp://managed"},
    )
    assert kwargs["producer_policy"] is policy
    client = CopilotACPClient(
        command=sys.executable, args=["-c", _HOLD_CHILD], acp_cwd=str(tmp_path), producer_policy=policy,
    )
    return fixture, client, binding, service, parent, request


def _release_managed_failure(client, binding, service, parent, request):
    for process in tuple(client._active_processes):
        client._release_process(process, success=False, failure_reason="acp_session_failed")
    if service.unknown or parent.state == "unknown":
        assert client._retry_managed_process_failure(parent, service, binding, request)
    if parent._physical is not None:
        from gateway.runtime_service_operations import release_before_effect

        assert release_before_effect(parent)
    assert not client._active_processes


@pytest.mark.asyncio
async def test_managed_launch_bind_failure_retains_receipt_for_same_handle_retry(tmp_path, monkeypatch):
    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, "managed-bind-retry",
    )
    try:
        from gateway import runtime_service_operations as operations

        original_bind = operations.bind_physical
        calls = 0

        def fail_once(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise operations.ExternalProcessOperationError("synthetic_bind_failure")
            return original_bind(*args, **kwargs)

        monkeypatch.setattr(operations, "bind_physical", fail_once)
        with pytest.raises(operations.ExternalProcessOperationError, match="synthetic_bind_failure"):
            client._spawn()

        receipt = parent._physical
        assert receipt is service.physical
        assert receipt is not None and receipt.state == "unknown"
        physical_handle = receipt.handle
        assert receipt.unknown_reason == "spawn_bind_failed"
        assert parent.state == "unknown"
        assert any(item.get("receipt") is receipt for item in client._pending_managed_witnesses)

        process = client._spawn()
        assert calls == 2
        assert parent._physical is receipt
        assert service.physical is receipt
        assert receipt.process is process
        assert receipt.state == "process_bound"
        assert receipt.handle is physical_handle
    finally:
        _release_managed_failure(client, binding, service, parent, request)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_managed_partial_pump_setup_retains_readers_for_same_handle_retry(tmp_path, monkeypatch):
    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, "managed-pump-retry",
    )
    try:
        created = []
        from agent import memory_provider

        original_spawn = memory_provider.spawn_context_thread

        def fail_on_second_start(*args, **kwargs):
            reader = original_spawn(*args, **kwargs)
            created.append(reader)
            if len(created) == 2:
                def fail_start():
                    raise RuntimeError("synthetic second reader start failure")

                reader.start = fail_start
            return reader

        with monkeypatch.context() as patch:
            patch.setattr(memory_provider, "spawn_context_thread", fail_on_second_start)
            with pytest.raises(RuntimeError, match="synthetic second reader start failure"):
                with client._session(1.0):
                    pass

        receipt = parent._physical
        assert receipt is service.physical
        assert receipt is not None and receipt.state == "unknown"
        physical_handle = receipt.handle
        assert receipt.unknown_reason == "reader_setup_failed"
        assert set(receipt.readers) == {"stdout", "stderr"}
        assert receipt.readers["stdout"] is created[0]
        assert receipt.readers["stderr"] is created[1]
        assert created[0].is_alive() is False
        assert created[1].is_alive() is False
        assert any(item.get("receipt") is receipt for item in client._pending_managed_witnesses)

        process = client._spawn()
        assert parent._physical is receipt
        assert receipt.process is process
        assert receipt.state == "process_bound"
        assert receipt.handle is physical_handle
    finally:
        _release_managed_failure(client, binding, service, parent, request)
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.platforms("windows")
async def test_windows_no_popen_refusal_closes_job_before_parent_release(tmp_path, monkeypatch):
    fixture, client, _binding, service, parent, _request = await _managed_client(
        tmp_path, monkeypatch, "windows-no-popen-release",
    )
    try:
        import subprocess as subprocess_module

        def refuse_popen(*args, **kwargs):
            raise FileNotFoundError("synthetic executable refusal")

        monkeypatch.setattr(subprocess_module, "Popen", refuse_popen)
        with pytest.raises(RuntimeError, match="Could not start Copilot ACP command"):
            client._spawn()
        assert parent._physical is None
        assert service.physical is None
        assert parent.state == "request_reserved"
        assert not client._active_processes
    finally:
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.platforms("windows")
async def test_windows_bind_exception_retains_transaction_and_retries_same_handle(tmp_path, monkeypatch):
    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, "windows-bind-retry",
    )
    try:
        from gateway import runtime_service_operations as operations

        original_bind = operations.bind_windows_launch
        seen = []

        def fail_once(parent_arg, transaction, process):
            seen.append(transaction)
            if len(seen) == 1:
                raise ValueError("synthetic ordinary native bind failure")
            return original_bind(parent_arg, transaction, process)

        monkeypatch.setattr(operations, "bind_windows_launch", fail_once)
        with pytest.raises(ValueError, match="synthetic ordinary native bind failure"):
            client._spawn()
        receipt = parent._physical
        assert receipt is service.physical
        assert receipt is not None and receipt.state == "unknown"
        assert receipt.unknown_reason == "spawn_bind_failed"
        assert receipt.transaction is seen[0]
        assert receipt.process is seen[0].process
        assert seen[0].job is not None
        assert seen[0].assigned is True
        assert seen[0].resumed is True
        seen[0].process.wait(timeout=10)
        physical_handle = receipt.handle

        original_close = seen[0].close_after_observed_empty
        close_calls = 0

        def fail_close_once():
            nonlocal close_calls
            close_calls += 1
            if close_calls == 1:
                return False
            return original_close()

        seen[0].close_after_observed_empty = fail_close_once
        from gateway.runtime_service_custody import ServiceCustodyError

        with pytest.raises(ServiceCustodyError, match="managed_process_unknown"):
            client._spawn()
        assert receipt.transaction is seen[0]
        process = client._spawn()
        assert close_calls == 2
        assert parent._physical is receipt
        assert service.physical is receipt
        assert receipt.handle is physical_handle
        assert receipt.process is process
        assert receipt.transaction is not seen[0]
        assert receipt.state == "process_bound"
    finally:
        _release_managed_failure(client, binding, service, parent, request)
        fixture.cleanup()


def test_external_catalog_physical_close_unknown_retries_same_receipt(tmp_path, monkeypatch):
    from collections import deque
    import queue

    from agent.copilot_acp_client import _start_acp_pumps
    from gateway.runtime_service_operations import (
        close_after_finalizers,
        policy_for_parent,
        reserve_standalone_external_process_parent,
    )
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db")
    try:
        with reserve_standalone_external_process_parent(
            profile_home=tmp_path, profile_id=str(tmp_path), operation_id="catalog-retry",
            source_identity="copilot-acp", profile_db=db, producer_key="acp",
        ) as parent:
            policy = policy_for_parent(parent, mode="standalone_maintenance")
            client = CopilotACPClient(
                command=sys.executable, args=["-c", "pass"], producer_policy=policy,
            )
            try:
                process = client._spawn()
                receipt = parent._physical
                physical_handle = receipt.handle
                native_transaction = receipt.transaction
                _start_acp_pumps(client, process, queue.Queue(), deque(), lambda line: line)
                process.wait()
                for name, reader in receipt.readers.items():
                    reader.join(timeout=5)
                    assert reader.is_alive() is False
                    receipt.reader_joined(name, reader)
                assert receipt.eof_streams == {"stdout", "stderr"}
                # Match catalog discovery: settle the child/readers and forget its local
                # process record before the caller publishes its cache-readback proof.
                client._release_process(process, success=True)
                assert process not in client._active_processes
                assert parent._physical is receipt
                assert receipt.transaction is native_transaction

                # Count fault-boundary attempts, leaving the real native transaction in the
                # receipt untouched. The retry must run the real host's close/readback path.
                transaction = SimpleNamespace(close_calls=0)
                original_close = receipt.close_after_delivery

                def fail_close_once():
                    transaction.close_calls += 1
                    if transaction.close_calls == 1:
                        return receipt._delivery_finalize_unknown("native_close_failed")
                    return original_close()

                monkeypatch.setattr(receipt, "close_after_delivery", fail_close_once)
                with pytest.raises(RuntimeError, match="did not settle"):
                    client.finalize_external_catalog(
                        result_identity="copilot-acp:catalog-retry",
                        cache_identity="copilot-acp:fingerprint:1",
                        payload_digest="digest",
                    )
                assert parent._physical is receipt
                assert parent.state == "physical_bound"
                assert receipt.state == "unknown"
                assert receipt.unknown_reason == "native_close_failed"
                assert receipt.transaction is native_transaction
                assert close_after_finalizers(parent) is True
                assert transaction.close_calls == 2
                assert parent._physical is None
                assert parent.state == "terminal"
                assert receipt.state == "closed"
                assert receipt.handle is physical_handle
                assert receipt.transaction is native_transaction
            finally:
                client.close()
            assert parent.state == "terminal"
        assert parent._maintenance_hold_released is True
        assert parent.lock_owner.owns(tmp_path) is False
    finally:
        db.close()


@pytest.mark.asyncio
@pytest.mark.platforms("windows")
async def test_windows_post_launch_base_exception_retains_original_transaction(tmp_path, monkeypatch):
    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, "windows-launch-base-exception",
    )
    try:
        from tools.process_registry_windows import WindowsLaunchTransaction

        original_launch = WindowsLaunchTransaction.launch
        launched = []

        def interrupt_after_launch(transaction, *args, **kwargs):
            process = original_launch(transaction, *args, **kwargs)
            launched.append(transaction)
            if len(launched) == 1:
                raise KeyboardInterrupt("native launch interrupted after real process creation")
            return process

        monkeypatch.setattr(WindowsLaunchTransaction, "launch", interrupt_after_launch)
        with pytest.raises(KeyboardInterrupt, match="native launch interrupted"):
            client._spawn()
        receipt = parent._physical
        assert receipt is service.physical
        assert receipt.transaction is launched[0]
        assert receipt.process is launched[0].process
        assert receipt.state == "unknown"
        assert receipt.unknown_reason == "spawn_bind_failed"
        assert any(item.get("receipt") is receipt for item in client._pending_managed_witnesses)
        original_handle = receipt.handle
        original_process = receipt.process
        original_process.wait(timeout=10)

        replacement = client._spawn()
        assert parent._physical is receipt and service.physical is receipt
        assert receipt.handle is original_handle
        assert launched[0].process is original_process
        assert launched[0].state == "closed"
        assert receipt.process is replacement and replacement is not original_process
        assert receipt.transaction is launched[1]
    finally:
        _release_managed_failure(client, binding, service, parent, request)
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["bind", "start"])
async def test_managed_reader_base_exception_retains_every_created_reader(tmp_path, monkeypatch, boundary):
    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, f"managed-reader-base-exception-{boundary}",
    )
    try:
        from agent import memory_provider
        from gateway import runtime_service_operations as operations

        created = []
        original_spawn = memory_provider.spawn_context_thread
        original_bind = operations.bind_reader
        bind_count = 0

        def create_reader(*args, **kwargs):
            reader = original_spawn(*args, **kwargs)
            created.append(reader)
            if boundary == "start" and len(created) == 2:
                def interrupt_start():
                    raise KeyboardInterrupt("reader setup interrupted")

                reader.start = interrupt_start
            return reader

        def bind_reader(*args, **kwargs):
            nonlocal bind_count
            bind_count += 1
            if boundary == "bind" and bind_count == 2:
                raise KeyboardInterrupt("reader setup interrupted")
            return original_bind(*args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(memory_provider, "spawn_context_thread", create_reader)
            patch.setattr(operations, "bind_reader", bind_reader)
            with pytest.raises(KeyboardInterrupt, match="reader setup interrupted"):
                with client._session(1.0):
                    pass
        receipt = parent._physical
        assert receipt is service.physical
        assert receipt.state == "unknown" and receipt.unknown_reason == "reader_setup_failed"
        assert tuple(receipt.readers.values()) == tuple(created)
        assert set(receipt.readers) == {"stdout", "stderr"}
        assert not any(reader.is_alive() for reader in created)
        handle = receipt.handle
        replacement = client._spawn()
        assert parent._physical is receipt and receipt.handle is handle
        assert receipt.process is replacement and receipt.state == "process_bound"
    finally:
        _release_managed_failure(client, binding, service, parent, request)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_close_reads_complete_reader_tuple_after_actual_setup_fence(tmp_path, monkeypatch):
    import threading

    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, "managed-reader-close-interleaving",
    )
    closer = None
    failures = []
    try:
        from agent import copilot_acp_client, memory_provider
        from collections import deque
        import queue

        process = client._spawn()
        created = []
        close_wait_started = threading.Event()
        original_spawn = memory_provider.spawn_context_thread

        def close_process():
            try:
                client._release_process(process, success=False, failure_reason="acp_session_failed")
            except BaseException as exc:
                failures.append(type(exc).__name__)

        def create_reader(*args, **kwargs):
            nonlocal closer
            reader = original_spawn(*args, **kwargs)
            created.append(reader)
            if len(created) == 2:
                fence = client._process_setup_fences[process]
                original_wait = fence.wait

                def observed_wait(timeout=None):
                    close_wait_started.set()
                    return original_wait(timeout)

                fence.wait = observed_wait
                closer = original_spawn(target=close_process, name="acp-close-during-setup", daemon=True)
                closer.start()
                assert close_wait_started.wait(timeout=2.0)
            return reader

        with monkeypatch.context() as patch:
            patch.setattr(memory_provider, "spawn_context_thread", create_reader)
            copilot_acp_client._start_acp_pumps(client, process, queue.Queue(), deque(), lambda line: line)
        assert closer is not None
        closer.join(timeout=5.0)
        assert not closer.is_alive() and failures == []
        receipt = parent._physical
        assert receipt is service.physical
        assert tuple(receipt.readers.values()) == tuple(created)
        assert not any(reader.is_alive() for reader in created)
        assert receipt.unknown_reason == "acp_session_failed"
        assert client._process_readers[process] == tuple(created)
        assert receipt.joined_streams == {"stdout", "stderr"}
    finally:
        if closer is not None:
            closer.join(timeout=5.0)
        _release_managed_failure(client, binding, service, parent, request)
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.platforms("posix")
@pytest.mark.parametrize("failure_type", [ValueError, KeyboardInterrupt])
async def test_posix_opaque_popen_retains_unknown_receipt_without_release(
    tmp_path, monkeypatch, failure_type,
):
    fixture, client, _binding, service, parent, _request = await _managed_client(
        tmp_path, monkeypatch, "posix-opaque-popen-retention",
    )
    marker = tmp_path / "popen-entered"
    popen_calls = 0
    receipt = None
    try:
        from agent import copilot_acp_client
        from gateway.runtime_service_custody import ServiceCustodyError

        def interrupt_popen(*args, **kwargs):
            nonlocal popen_calls
            popen_calls += 1
            marker.write_text("entered", encoding="utf-8")
            raise failure_type("opaque POSIX launch interruption")

        monkeypatch.setattr(copilot_acp_client.subprocess, "Popen", interrupt_popen)
        with pytest.raises(failure_type, match="opaque POSIX launch interruption"):
            client._spawn()
        receipt = parent._physical
        assert marker.read_text(encoding="utf-8") == "entered"
        assert receipt is service.physical
        assert receipt is not None and receipt.process is None
        assert receipt.state == "unknown"
        assert receipt.unknown_reason == "opaque_launch_effect"
        assert parent.state == "unknown"
        assert any(item.get("receipt") is receipt for item in client._pending_managed_witnesses)

        # A transactionless constructor fault has no no-effect proof.  The same managed
        # parent must refuse before a second Popen rather than reset/relaunch a successor.
        with pytest.raises(ServiceCustodyError, match="managed_process_unknown"):
            client._spawn()
        assert popen_calls == 1
        assert parent._physical is receipt
        assert service.physical is receipt
        assert receipt.state == "unknown"
        assert receipt.unknown_reason == "opaque_launch_effect"
    finally:
        client.close()
        if receipt is not None:
            assert parent._physical is receipt
            assert service.physical is receipt
            assert receipt.state == "unknown"
        fixture.cleanup()


@pytest.mark.asyncio
async def test_setup_timeout_keeps_exact_event_and_process_until_late_setup_finishes(tmp_path, monkeypatch):
    import queue
    import threading
    from collections import deque

    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, "setup-fence-timeout-retention",
    )
    setup_thread = None
    continue_setup = threading.Event()
    second_reader_created = threading.Event()
    process = None
    try:
        from agent import copilot_acp_client, memory_provider

        process = client._spawn()
        created = []
        original_spawn = memory_provider.spawn_context_thread

        def delayed_spawn(*args, **kwargs):
            reader = original_spawn(*args, **kwargs)
            created.append(reader)
            if len(created) == 2:
                second_reader_created.set()
                assert continue_setup.wait(timeout=5.0)
            return reader

        with monkeypatch.context() as patch:
            patch.setattr(memory_provider, "spawn_context_thread", delayed_spawn)
            setup_thread = threading.Thread(
                target=copilot_acp_client._start_acp_pumps,
                args=(client, process, queue.Queue(), deque(), lambda line: line),
                daemon=True,
            )
            setup_thread.start()
            assert second_reader_created.wait(timeout=2.0)
            receipt = parent._physical
            fence = receipt.setup_fence
            assert fence is client._process_setup_fences[process]
            client._release_process(process, success=False, failure_reason="acp_session_failed")
            assert fence.is_set() is False
            assert process in client._active_processes
            assert client.is_closed is False
            assert receipt.state == "unknown"
            assert receipt.prepare_process_retry() is False
            assert client._process_readers[process] == (created[0],)
            continue_setup.set()
            setup_thread.join(timeout=5.0)
        assert not setup_thread.is_alive()
        assert fence.is_set() is True
        assert client._process_readers[process] == tuple(created)
        client._release_process(process, success=False, failure_reason="acp_session_failed")
    finally:
        continue_setup.set()
        if setup_thread is not None:
            setup_thread.join(timeout=5.0)
        _release_managed_failure(client, binding, service, parent, request)
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.platforms("posix")
async def test_cleanup_keyboard_interrupt_retains_owner_before_reraising(tmp_path, monkeypatch):
    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, "cleanup-keyboard-interrupt-retention",
    )
    try:
        process = client._spawn()

        def interrupt_cleanup(_process):
            raise KeyboardInterrupt("cleanup interrupted")

        with monkeypatch.context() as patch:
            patch.setattr(client, "_terminate_process", interrupt_cleanup)
            with pytest.raises(KeyboardInterrupt, match="cleanup interrupted"):
                client._release_process(process, success=False, failure_reason="acp_session_failed")
        receipt = parent._physical
        assert receipt is service.physical
        assert receipt.process is process
        assert receipt.state == "unknown"
        assert receipt.unknown_reason == "native_cleanup_interrupted"
        assert any(item.get("receipt") is receipt for item in client._pending_managed_witnesses)
        physical_handle = receipt.handle
        assert client._retry_managed_process_failure(parent, service, binding, request) is True
        assert parent._physical is receipt
        assert service.physical is receipt
        assert receipt.handle is physical_handle
        assert receipt.state == "reserved"
        assert receipt.process is None
        assert not client._pending_managed_witnesses
    finally:
        _release_managed_failure(client, binding, service, parent, request)
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.platforms("windows")
async def test_windows_transaction_cleanup_interrupt_retries_same_owned_process(tmp_path, monkeypatch):
    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, "windows-cleanup-retry",
    )
    try:
        process = client._spawn()
        receipt = parent._physical
        assert receipt is service.physical
        assert receipt is not None
        transaction = receipt.transaction
        assert transaction is not None
        assert transaction.process is process
        assert transaction.job is not None
        assert transaction.assigned is True and transaction.resumed is True
        physical_handle = receipt.handle

        def interrupt_native_termination(*args, **kwargs):
            raise KeyboardInterrupt("native transaction cleanup interrupted")

        with monkeypatch.context() as patch:
            patch.setattr(transaction, "terminate_owned_tree", interrupt_native_termination)
            with pytest.raises(KeyboardInterrupt, match="native transaction cleanup interrupted"):
                client._release_process(process, success=False, failure_reason="acp_session_failed")
        assert parent._physical is receipt and service.physical is receipt
        assert receipt.transaction is transaction
        assert receipt.process is process
        assert receipt.state == "unknown"
        assert receipt.unknown_reason == "native_cleanup_interrupted"
        assert client._retry_managed_process_failure(parent, service, binding, request) is True
        assert parent._physical is receipt and service.physical is receipt
        assert receipt.handle is physical_handle
        assert receipt.state == "reserved"
        assert receipt.process is None and receipt.transaction is None
        assert not client._pending_managed_witnesses
    finally:
        _release_managed_failure(client, binding, service, parent, request)
        fixture.cleanup()


@pytest.mark.asyncio
@pytest.mark.platforms("posix")
async def test_reader_cleanup_interruption_retries_same_owned_process(tmp_path, monkeypatch):
    import queue
    from collections import deque

    fixture, client, binding, service, parent, request = await _managed_client(
        tmp_path, monkeypatch, "reader-cleanup-retry",
    )
    try:
        # The short real child lets both actual ACP reader threads reach EOF before the
        # injected join boundary; retry must still use those exact reader identities.
        client._acp_args = ["-c", "pass"]
        process = client._spawn()
        from agent import copilot_acp_client

        copilot_acp_client._start_acp_pumps(client, process, queue.Queue(), deque(), lambda line: line)
        readers = client._process_readers[process]
        for reader in readers:
            reader.join(timeout=2.0)
        assert len(readers) == 2
        assert all(reader.is_alive() is False for reader in readers)

        def interrupt_join(_readers):
            raise KeyboardInterrupt("reader cleanup interrupted")

        with monkeypatch.context() as patch:
            patch.setattr(client, "_join_registered_readers", interrupt_join)
            with pytest.raises(KeyboardInterrupt, match="reader cleanup interrupted"):
                client._release_process(process, success=False, failure_reason="acp_session_failed")
        receipt = parent._physical
        assert receipt is service.physical
        assert receipt is not None and receipt.process is process
        assert receipt.state == "unknown"
        assert receipt.unknown_reason == "reader_cleanup_interrupted"
        physical_handle = receipt.handle
        assert client._retry_managed_process_failure(parent, service, binding, request) is True
        assert parent._physical is receipt
        assert service.physical is receipt
        assert receipt.handle is physical_handle
        assert receipt.state == "reserved"
        assert receipt.process is None
        assert not client._pending_managed_witnesses
    finally:
        _release_managed_failure(client, binding, service, parent, request)
        fixture.cleanup()
