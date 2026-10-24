"""Portable contract checks for retained Windows-process retry cleanup."""

from __future__ import annotations

import subprocess
from types import SimpleNamespace

import pytest


class _Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


class _DelayedProcess:
    def __init__(self, clock: _Clock, wait_duration: float = 0.75) -> None:
        self._clock = clock
        self._wait_duration = wait_duration
        self.exited = False
        self.wait_timeouts: list[float] = []

    def poll(self) -> int | None:
        return 0 if self.exited else None

    def wait(self, *, timeout: float) -> int:
        self.wait_timeouts.append(timeout)
        self._clock.now += self._wait_duration
        self.exited = True
        return 0


class _WindowsTransaction:
    def __init__(
        self,
        process: _DelayedProcess,
        *,
        drain_result: bool = True,
        drain_time_advance: float = 0.0,
        failed_scope_observation: str = "unknown",
        close_result: bool = True,
    ) -> None:
        self.process = process
        self.drain_result = drain_result
        self.drain_time_advance = drain_time_advance
        self.failed_scope_observation = failed_scope_observation
        self.close_result = close_result
        self.termination_calls = 0
        self.drain_timeouts: list[float] = []
        self.close_calls = 0
        self.native_empty_observed = False
        self.scope_observation = "unobserved"
        self.observe_empty_calls = 0
        self.reader_handles_closed = False
        self.state = "termination_requested"
        self.process_handle_closed = False
        self.job_handle_closed = False

    def terminate_owned_tree(self) -> bool:
        self.termination_calls += 1
        return True

    def drain_after_process_exit(self, timeout: float) -> bool:
        self.drain_timeouts.append(timeout)
        self.process._clock.now += self.drain_time_advance
        if not self.drain_result:
            self.scope_observation = self.failed_scope_observation
            return False
        self.native_empty_observed = True
        return True

    def observe_empty(self) -> bool:
        self.observe_empty_calls += 1
        return False

    def close_after_observed_empty(self) -> bool:
        self.close_calls += 1
        if not self.close_result:
            return False
        self.state = "closed"
        self.process_handle_closed = True
        self.job_handle_closed = True
        return True


def _receipt(process: object, transaction: object):
    from gateway.runtime_service_physical import ServiceProcessReceipt

    receipt = ServiceProcessReceipt(
        SimpleNamespace(), SimpleNamespace(owner_id="retry-owner"), "retry-owner",
    )
    receipt.process = process
    receipt.transaction = transaction
    receipt.state = "unknown"
    receipt.unknown_reason = "native_cleanup_interrupted"
    return receipt


def test_retained_windows_retry_waits_same_process_then_drains_same_job(monkeypatch) -> None:
    from gateway.runtime_service_physical import ServiceProcessReceipt
    from gateway import runtime_service_physical

    clock = _Clock()
    process = _DelayedProcess(clock)
    transaction = _WindowsTransaction(process)
    receipt = ServiceProcessReceipt(
        SimpleNamespace(), SimpleNamespace(owner_id="retry-owner"), "retry-owner",
    )
    receipt.process = process
    receipt.transaction = transaction
    receipt.state = "unknown"
    receipt.unknown_reason = "native_cleanup_interrupted"
    monkeypatch.setattr(runtime_service_physical.time, "monotonic", clock)

    assert receipt.prepare_process_retry() is True
    assert transaction.termination_calls == 1
    assert process.wait_timeouts == [pytest.approx(2.0)]
    assert transaction.drain_timeouts == [pytest.approx(1.25)]
    assert transaction.drain_timeouts[0] < process.wait_timeouts[0]
    assert transaction.close_calls == 1
    assert transaction.reader_handles_closed is False
    assert receipt.process is process
    assert receipt.transaction is transaction


@pytest.mark.parametrize("failed_scope_observation", ["unknown", "foreign"])
def test_retained_windows_retry_refuses_unknown_or_foreign_job_transition(
    monkeypatch, failed_scope_observation: str,
) -> None:
    from gateway.runtime_service_physical import ServiceProcessReceipt
    from gateway import runtime_service_physical

    clock = _Clock()
    process = _DelayedProcess(clock)
    transaction = _WindowsTransaction(
        process,
        drain_result=False,
        failed_scope_observation=failed_scope_observation,
    )
    receipt = ServiceProcessReceipt(
        SimpleNamespace(), SimpleNamespace(owner_id="retry-owner"), "retry-owner",
    )
    receipt.process = process
    receipt.transaction = transaction
    receipt.state = "unknown"
    receipt.unknown_reason = "native_cleanup_interrupted"
    monkeypatch.setattr(runtime_service_physical.time, "monotonic", clock)

    assert receipt.prepare_process_retry() is False
    assert transaction.termination_calls == 1
    assert transaction.close_calls == 0
    assert transaction.native_empty_observed is False
    assert transaction.scope_observation == failed_scope_observation
    assert transaction.observe_empty_calls == 0
    assert receipt.state == "unknown"
    assert receipt.unknown_reason == "native_cleanup_interrupted"
    assert receipt.process is process
    assert receipt.transaction is transaction


def test_retained_windows_retry_rejects_late_success_after_shared_deadline(monkeypatch) -> None:
    from gateway.runtime_service_physical import ServiceProcessReceipt
    from gateway import runtime_service_physical

    clock = _Clock()
    process = _DelayedProcess(clock)
    transaction = _WindowsTransaction(process, drain_time_advance=1.5)
    receipt = ServiceProcessReceipt(
        SimpleNamespace(), SimpleNamespace(owner_id="retry-owner"), "retry-owner",
    )
    receipt.process = process
    receipt.transaction = transaction
    receipt.state = "unknown"
    receipt.unknown_reason = "native_cleanup_interrupted"
    monkeypatch.setattr(runtime_service_physical.time, "monotonic", clock)

    assert receipt.prepare_process_retry() is False
    assert transaction.termination_calls == 1
    assert transaction.drain_timeouts == [pytest.approx(1.25)]
    assert transaction.close_calls == 0
    assert transaction.native_empty_observed is True
    assert receipt.state == "unknown"
    assert receipt.process is process
    assert receipt.transaction is transaction


def test_retained_windows_retry_keeps_same_handles_when_native_close_fails(monkeypatch) -> None:
    from gateway.runtime_service_physical import ServiceProcessReceipt
    from gateway import runtime_service_physical

    clock = _Clock()
    process = _DelayedProcess(clock)
    transaction = _WindowsTransaction(process, close_result=False)
    receipt = ServiceProcessReceipt(
        SimpleNamespace(), SimpleNamespace(owner_id="retry-owner"), "retry-owner",
    )
    receipt.process = process
    receipt.transaction = transaction
    receipt.state = "unknown"
    receipt.unknown_reason = "native_cleanup_interrupted"
    monkeypatch.setattr(runtime_service_physical.time, "monotonic", clock)

    assert receipt.prepare_process_retry() is False
    assert transaction.close_calls == 1
    assert transaction.process_handle_closed is False
    assert transaction.job_handle_closed is False
    assert receipt.process is process
    assert receipt.transaction is transaction


def test_retained_windows_retry_refuses_foreign_process_without_touching_transaction() -> None:
    process = _DelayedProcess(_Clock())
    foreign_process = _DelayedProcess(_Clock())
    transaction = _WindowsTransaction(foreign_process)
    receipt = _receipt(process, transaction)

    assert receipt.prepare_process_retry() is False
    assert transaction.termination_calls == 0
    assert transaction.drain_timeouts == []
    assert transaction.close_calls == 0
    assert receipt.process is process
    assert receipt.transaction is transaction


def test_retained_windows_retry_timeout_keeps_same_transaction(monkeypatch) -> None:
    from gateway.runtime_service_physical import ServiceProcessReceipt
    from gateway import runtime_service_physical

    class _TimeoutProcess(_DelayedProcess):
        def wait(self, *, timeout: float) -> int:
            self.wait_timeouts.append(timeout)
            self._clock.now += timeout
            raise subprocess.TimeoutExpired("retained-worker", timeout)

    clock = _Clock()
    process = _TimeoutProcess(clock)
    transaction = _WindowsTransaction(process)
    receipt = ServiceProcessReceipt(
        SimpleNamespace(), SimpleNamespace(owner_id="retry-owner"), "retry-owner",
    )
    receipt.process = process
    receipt.transaction = transaction
    receipt.state = "unknown"
    receipt.unknown_reason = "native_cleanup_interrupted"
    monkeypatch.setattr(runtime_service_physical.time, "monotonic", clock)

    assert receipt.prepare_process_retry() is False
    assert transaction.termination_calls == 1
    assert transaction.drain_timeouts == []
    assert transaction.close_calls == 0
    assert receipt.process is process
    assert receipt.transaction is transaction
