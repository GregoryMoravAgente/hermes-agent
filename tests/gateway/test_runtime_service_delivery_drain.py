"""Delivery-close drains the exact retained Windows transaction before native close."""

from __future__ import annotations

import contextlib
import subprocess
import sys
from types import SimpleNamespace

import pytest


class _ExitedProcess:
    def poll(self) -> int:
        return 0


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class _CatalogTransaction:
    def __init__(
        self,
        process: object,
        *,
        drain_result: bool = True,
        drain_advance: float = 0.0,
        close_result: bool = True,
        clock: _Clock | None = None,
    ) -> None:
        self.process = process
        self.drain_result = drain_result
        self.drain_advance = drain_advance
        self.close_result = close_result
        self.clock = clock
        self.native_empty_observed = False
        self.scope_observation = "nonempty"
        self.events: list[str] = []
        self.drain_timeouts: list[float] = []
        self.close_calls = 0
        self.observe_calls = 0

    def drain_after_process_exit(self, timeout: float) -> bool:
        self.events.append("drain")
        self.drain_timeouts.append(timeout)
        if self.clock is not None:
            self.clock.now += self.drain_advance
        if not self.drain_result:
            return False
        self.native_empty_observed = True
        self.scope_observation = "empty"
        return True

    def observe_empty(self) -> None:
        self.events.append("observe")
        self.observe_calls += 1

    def close_after_observed_empty(self) -> bool:
        self.events.append("close")
        self.close_calls += 1
        if not self.close_result or self.native_empty_observed is not True:
            return False
        return True


class _NoDrainTransaction:
    def __init__(self, process: object) -> None:
        self.process = process
        self.native_empty_observed = False
        self.scope_observation = "nonempty"
        self.observe_calls = 0
        self.close_calls = 0

    def observe_empty(self) -> bool:
        self.observe_calls += 1
        return True

    def close_after_observed_empty(self) -> bool:
        self.close_calls += 1
        return True


class _Coordinator:
    def __init__(self) -> None:
        self.unknown: list[str] = []
        self.terminal_calls = 0

    def mark_unknown(self, _handle: object, reason: str) -> None:
        self.unknown.append(reason)

    def mark_terminal(self, _handle: object) -> None:
        self.terminal_calls += 1


def _receipt(transaction: object, *, delivery: bool = False):
    from gateway.runtime_service_physical import ServiceProcessReceipt

    coordinator = _Coordinator()
    handle = SimpleNamespace(
        owner_id="catalog:physical",
        profile_id="profile",
        process_instance="process",
        authority_epoch=1,
    )
    service = SimpleNamespace(binding=None, coordinator=coordinator, physical=None, unknown=False)
    receipt = ServiceProcessReceipt(service, handle, "catalog:physical")
    service.physical = receipt
    receipt.process = transaction.process
    receipt.transaction = transaction
    receipt.state = "process_bound"
    receipt.readers = {
        "stdout": SimpleNamespace(is_alive=lambda: False),
        "stderr": SimpleNamespace(is_alive=lambda: False),
    }
    receipt.eof_streams = {"stdout", "stderr"}
    receipt.joined_streams = {"stdout", "stderr"}
    receipt.completed_requests = {"request"}
    if delivery:
        receipt.durable_proofs = {"request": object()}
        receipt.delivery_requests = {"request"}
    return receipt, coordinator


def test_close_after_delivery_drains_before_closing_same_transaction() -> None:
    process = _ExitedProcess()
    transaction = _CatalogTransaction(process)
    receipt, coordinator = _receipt(transaction)

    assert receipt.close_after_delivery() is True
    assert transaction.events == ["drain", "close"]
    assert transaction.observe_calls == 0
    assert transaction.close_calls == 1
    assert 0.0 < transaction.drain_timeouts[0] <= 2.0
    assert receipt.transaction is transaction
    assert receipt.process is process
    assert receipt.state == "closed"
    assert coordinator.terminal_calls == 1


def test_prepare_delivery_close_uses_the_same_exact_transaction_drain() -> None:
    process = _ExitedProcess()
    transaction = _CatalogTransaction(process)
    receipt, _coordinator = _receipt(transaction, delivery=True)

    assert receipt.prepare_delivery_close("request") is True
    assert transaction.events == ["drain", "close"]
    assert receipt.transaction is transaction
    assert receipt.process is process
    assert receipt.native_close_committed is True
    assert receipt.state == "finalizing"


def test_prepare_delivery_close_keeps_existing_close_failure_reason() -> None:
    process = _ExitedProcess()
    transaction = _CatalogTransaction(process, close_result=False)
    receipt, coordinator = _receipt(transaction, delivery=True)

    assert receipt.prepare_delivery_close("request") is False
    assert transaction.events == ["drain", "close"]
    assert receipt.transaction is transaction
    assert receipt.process is process
    assert receipt.state == "unknown"
    assert receipt.unknown_reason == "delivery_finalize_failed"
    assert coordinator.unknown == ["delivery_finalize_failed"]


def test_close_after_delivery_retains_unknown_same_handles_when_drain_fails() -> None:
    process = _ExitedProcess()
    transaction = _CatalogTransaction(process, drain_result=False)
    receipt, coordinator = _receipt(transaction)

    assert receipt.close_after_delivery() is False
    assert transaction.events == ["drain"]
    assert transaction.close_calls == 0
    assert receipt.transaction is transaction
    assert receipt.process is process
    assert receipt.state == "unknown"
    assert receipt.unknown_reason == "native_job_state_unknown"
    assert coordinator.unknown == ["native_job_state_unknown"]


def test_close_after_delivery_refuses_late_drain_after_shared_deadline(monkeypatch) -> None:
    from gateway import runtime_service_physical

    clock = _Clock()
    process = _ExitedProcess()
    transaction = _CatalogTransaction(process, drain_advance=2.1, clock=clock)
    receipt, coordinator = _receipt(transaction)
    monkeypatch.setattr(runtime_service_physical.time, "monotonic", clock)

    assert receipt.close_after_delivery() is False
    assert transaction.events == ["drain"]
    assert transaction.close_calls == 0
    assert receipt.transaction is transaction
    assert receipt.process is process
    assert receipt.state == "unknown"
    assert receipt.unknown_reason == "native_job_state_unknown"
    assert coordinator.unknown == ["native_job_state_unknown"]


def test_close_after_delivery_refuses_foreign_process_without_touching_transaction() -> None:
    process = _ExitedProcess()
    foreign_process = _ExitedProcess()
    transaction = _CatalogTransaction(foreign_process)
    receipt, coordinator = _receipt(transaction)
    receipt.process = process

    assert receipt.close_after_delivery() is False
    assert transaction.events == []
    assert transaction.close_calls == 0
    assert receipt.transaction is transaction
    assert receipt.process is process
    assert receipt.state == "unknown"
    assert receipt.unknown_reason == "physical_process_live"
    assert coordinator.unknown == ["physical_process_live"]


def test_close_after_delivery_refuses_transaction_without_canonical_drain() -> None:
    process = _ExitedProcess()
    transaction = _NoDrainTransaction(process)
    receipt, coordinator = _receipt(transaction)

    assert receipt.close_after_delivery() is False
    assert transaction.observe_calls == 0
    assert transaction.close_calls == 0
    assert receipt.transaction is transaction
    assert receipt.process is process
    assert receipt.state == "unknown"
    assert receipt.unknown_reason == "native_job_state_unknown"
    assert coordinator.unknown == ["native_job_state_unknown"]


@pytest.mark.platforms("windows")
def test_windows_catalog_close_drains_real_retained_job_after_root_exit(tmp_path) -> None:
    from collections import deque
    import queue

    from agent.copilot_acp_client import CopilotACPClient, _start_acp_pumps
    from gateway.runtime_service_operations import (
        close_after_finalizers,
        policy_for_parent,
        reserve_standalone_external_process_parent,
    )
    from hermes_state import SessionDB
    from tests.waiting import wait_until

    child_code = (
        "import subprocess,sys; "
        "subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], "
        "stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)"
    )
    db = SessionDB(db_path=tmp_path / "state.db")
    process = None
    transaction = None
    try:
        with reserve_standalone_external_process_parent(
            profile_home=tmp_path, profile_id=str(tmp_path), operation_id="catalog-windows",
            source_identity="copilot-acp", profile_db=db, producer_key="acp",
        ) as parent:
            policy = policy_for_parent(parent, mode="standalone_maintenance")
            client = CopilotACPClient(
                command=sys.executable, args=["-c", child_code],
                acp_cwd=str(tmp_path), producer_policy=policy,
            )
            try:
                process = client._spawn()
                receipt = parent._physical
                assert receipt is not None
                physical_handle = receipt.handle
                transaction = receipt.transaction
                assert transaction is not None
                native_job = transaction.job
                assert transaction.process is process
                assert receipt.process is process

                _start_acp_pumps(client, process, queue.Queue(), deque(), lambda line: line)
                wait_until(lambda: process.poll() is not None, timeout=10, what="ACP root exit")
                assert process.returncode == 0
                wait_until(
                    lambda: native_job.active_processes() >= 1,
                    timeout=10,
                    what="retained Job descendant",
                )
                assert receipt.transaction is transaction
                assert receipt.process is process

                client._release_process(process, success=True)
                assert process not in client._active_processes
                assert parent._physical is receipt
                assert receipt.transaction is transaction
                assert transaction.process is process
                assert receipt.eof_streams == {"stdout", "stderr"}
                assert receipt.joined_streams == {"stdout", "stderr"}
                assert all(reader.is_alive() is False for reader in receipt.readers.values())

                client.finalize_external_catalog(
                    result_identity="copilot-acp:catalog-windows",
                    cache_identity="copilot-acp:fingerprint:1",
                    payload_digest="digest",
                )
                assert parent._physical is None
                assert parent.state == "request_reserved"
                assert receipt.transaction is transaction
                assert transaction.process is process
                assert transaction.native_empty_observed is True
                assert transaction.termination_attempts == 1
                assert transaction.process_handle_closed is True
                assert transaction.job_handle_closed is True
                assert receipt.state == "closed"
                assert receipt.handle is physical_handle
                assert close_after_finalizers(parent) is True
                assert parent.state == "terminal"
            finally:
                client.close()
            assert parent.state == "terminal"
        assert parent._maintenance_hold_released is True
    finally:
        if transaction is not None and transaction.job_handle_closed is False:
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                if process is not None and process.poll() is None:
                    transaction.terminate_owned_tree()
            if process is not None:
                with contextlib.suppress(OSError, subprocess.TimeoutExpired):
                    process.wait(timeout=10)
            with contextlib.suppress(OSError, RuntimeError, TypeError, ValueError):
                if transaction.observe_empty() is True:
                    transaction.close_after_observed_empty()
        db.close()
