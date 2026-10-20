"""Exact phase predicates for retained foreground process custody.

These predicates keep the retry and reconciliation policy beside the custody adapter without
creating a second registry.  They accept only the original in-memory transaction, process, Job,
reader, binding, and coordinator identity; a durable row or PID cannot reconstruct any of them.
"""
from __future__ import annotations

from typing import Any

MANAGED_TERMINATION_FAILURE = "managed_termination_failed"
NATIVE_CLOSE_FAILURE = "join_failed"
FOREGROUND_DELIVERY_FAILURE = "foreground_delivery_settlement_failed"
NATIVE_SCOPE_NONEMPTY = "native_scope_nonempty"
NATIVE_SCOPE_OBSERVATION_FAILURE = "native_scope_observation_unavailable"


def native_scope_pending_reason(reason: Any) -> bool:
    """Identify the two typed native observations that may be refreshed later."""
    return reason in {NATIVE_SCOPE_NONEMPTY, NATIVE_SCOPE_OBSERVATION_FAILURE}


def foreground_delivery_close_generation(custody: Any, record: Any) -> Any:
    """Read the retained handle generation without accepting a replacement handle."""
    try:
        generation = custody.handle.close_generation
        if type(generation) is not int:
            return "foreground_delivery_identity_missing"
        if record.delivery_close_generation is not None and record.delivery_close_generation != generation:
            return "foreground_delivery_identity_mismatch"
        return generation
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return "foreground_delivery_identity_missing"


def foreground_record(transaction: Any) -> Any:
    try:
        return transaction._foreground_custody_record
    except AttributeError:
        return None


def managed_retry_identity_valid(
    transfer: Any, custody: Any, session: Any, source: str, consume_output: bool,
) -> bool:
    """Admit exactly one retry after the retained Job termination boundary failed."""
    try:
        transaction, handle = transfer.transaction, custody.handle
        binding, coordinator, job = custody.foreground_binding, custody.coordinator, transaction.job
        record = foreground_record(transaction)
        return all((
            transfer.phase == "unknown" and custody.state == "unknown"
            and custody.reason == MANAGED_TERMINATION_FAILURE,
            transaction.state == "termination_failed"
            and transaction.termination_retryable is True
            and transaction.termination_failures == transaction.termination_attempts == 1,
            transfer.process is session.process is transaction.process
            and session.custody is custody is transaction.custody
            and custody.native_scope is transaction,
            transaction.assigned is transaction.resumed is True
            and source == transfer.kill_source
            and consume_output == transfer.kill_consume_output,
            transaction.job_handle_closed is False
            and job._recovery_only is not True and job._handle is not None,
            transaction.termination_job is job
            and transaction.termination_job_handle is job._handle,
            (custody.owner_id, custody.profile_id, custody.process_instance,
             custody.authority_epoch) == (handle.owner_id, handle.profile_id,
             handle.process_instance, handle.authority_epoch)
            and type(handle.close_generation) is int,
            handle.coordinator_id == coordinator._coordinator_id
            and coordinator.process_instance == custody.process_instance
            and coordinator.authority_epoch == custody.authority_epoch,
            binding is custody.foreground_binding and binding is not None
            and record is not None and record.binding is binding
            and isinstance(binding.session_id, str) and bool(binding.session_id)
            and binding.session_key == session.session_key
            and (
                binding.session_id == session.parent_session_id
                if session.parent_session_id
                else binding.session_id == binding.session_key
            )
            and isinstance(binding.tool_call_id, str) and bool(binding.tool_call_id)
            and isinstance(binding.turn_id, str) and bool(binding.turn_id)
            and type(binding.run_generation) is int and binding.run_generation >= 0,
            transaction.matches(session.process, transaction.pid, transaction.host_start_time),
        ))
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def managed_retry_complete(
    transfer: Any, custody: Any, *, require_delivery: bool, pre_finish: bool = False,
) -> bool:
    """Check the real reader/native phase, with durable gates only after persistence."""
    try:
        transaction = transfer.transaction
        ready = all((
            transfer.kill_requested is True and custody.state == "unknown"
            and custody.reason == MANAGED_TERMINATION_FAILURE,
            transaction.state == "termination_requested",
            transaction.termination_failures == 1 and transaction.termination_attempts == 2,
            custody.native_scope is transaction,
            custody.reader_eof and custody.scope_settled and custody.cross_process_proven,
            transaction.native_empty_observed is True,
        ))
        if pre_finish:
            return ready
        return ready and custody.durable and custody.streams_closed and (
            not require_delivery or custody.delivery_acked
        )
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def native_close_retry_ready(custody: Any) -> bool:
    """Allow reconciliation only for the first failure of the exact native close."""
    try:
        transaction = custody.native_scope
        job = transaction.job
        return all((
            custody.state == "unknown" and custody.reason == NATIVE_CLOSE_FAILURE,
            transaction.custody is custody and transaction.state == "close_failed",
            transaction.close_retryable is True
            and transaction.close_attempts == transaction.close_failures == 1,
            transaction.native_empty_observed is True,
            transaction.process is not None and transaction.assigned is True
            and transaction.resumed is True
            and custody.process_bound
            and transaction.matches(transaction.process, custody.pid, custody.host_start_time),
            transaction.job_handle_closed is False
            and job._recovery_only is not True and job._handle is not None,
            transaction.close_job is job and transaction.close_job_handle is job._handle,
        ))
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def native_scope_observation_reason(transaction: Any) -> str:
    """Map one native observation to a typed custody reason without guessing."""
    observation = getattr(transaction, "scope_observation", "")
    if observation == "nonempty":
        return NATIVE_SCOPE_NONEMPTY
    if observation == "query_unavailable" and getattr(
        transaction, "scope_observation_failures", 0
    ) == 1:
        return NATIVE_SCOPE_OBSERVATION_FAILURE
    if observation == "foreign":
        return "foreign_custody"
    return "read_failed"


def mark_native_scope_unknown(custody: Any) -> None:
    """Record only the typed observation result; no missing query becomes empty."""
    custody.mark_unknown(native_scope_observation_reason(custody.native_scope))


def prepare_native_scope_retry(custody: Any) -> bool:
    """Refresh the same native owner without spending a descendant-drain retry."""
    try:
        transaction = custody.native_scope
        if custody.reason == NATIVE_SCOPE_NONEMPTY:
            observed = transaction.observe_empty()
            if observed is None and transaction.scope_observation == "foreign":
                custody.reason = "foreign_custody"
            elif observed is None and transaction.scope_observation == "query_unavailable":
                custody.reason = (
                    NATIVE_SCOPE_OBSERVATION_FAILURE
                    if transaction.scope_observation_failures == 1
                    else "read_failed"
                )
            return observed is True
        if custody.reason != NATIVE_SCOPE_OBSERVATION_FAILURE:
            return False
        if transaction.scope_observation_retries != 0:
            return False
        transaction.scope_observation_retries = 1
        observed = transaction.observe_empty()
        if observed is None and transaction.scope_observation == "foreign":
            custody.reason = "foreign_custody"
        elif observed is not True and transaction.scope_observation == "nonempty":
            custody.reason = NATIVE_SCOPE_NONEMPTY
            transaction.scope_observation_retries = 0
        elif observed is None and transaction.scope_observation == "query_unavailable":
            if transaction.scope_observation_failures > 1:
                custody.reason = "read_failed"
        return observed is True
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def native_scope_observation_ready(custody: Any) -> bool:
    """Refresh native scope before reconciliation while keeping exact nonempty pending."""
    try:
        transaction = custody.native_scope
        if native_scope_pending_reason(custody.reason):
            return prepare_native_scope_retry(custody)
        if not getattr(transaction, "native_empty_observed", False):
            return transaction.observe_empty() is True
        return True
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def reconcile_native_custody(custody: Any, *, observation_ready: bool = False) -> bool:
    """Reconcile one retained owner without reconstructing a process or Job."""
    if custody.state != "unknown" or custody.handle is None or custody.native_scope is None:
        return False
    if not observation_ready and not native_scope_observation_ready(custody):
        return False
    transfer = custody.foreground_transfer
    record = foreground_record(custody.native_scope)
    if not can_reconcile_unknown(custody, transfer, record):
        return False
    reconcile = getattr(custody.coordinator, "reconcile_unknown", None)
    if not callable(reconcile):
        return False
    refusal = reconcile(
        custody.handle, owner_id=custody.owner_id, profile_id=custody.profile_id,
        process_instance=custody.process_instance, authority_epoch=custody.authority_epoch,
    )
    if refusal is not None:
        return False
    custody.state = "accepted"
    custody.reason = ""
    custody.scope_settled = True
    custody.cross_process_proven = custody.process_bound and custody.reader_eof
    return True


def reconcile_native_foreground_retry(
    custody: Any, transfer: Any, session: Any, *, reader_eof: bool,
) -> bool:
    """Admit one same-transfer reader retry only after exact native emptiness."""
    try:
        transaction = custody.native_scope
        handle = custody.handle
        coordinator = custody.coordinator
        binding = custody.foreground_binding
        record = foreground_record(transaction)
        process = transaction.process
        if not all((
            custody.state == "unknown" and native_scope_pending_reason(custody.reason),
            transfer.phase == "scope_pending",
            transfer.transaction is transaction,
            custody.foreground_transfer is transfer,
            session.custody is custody,
            session.process is transfer.process is process,
            transaction.custody is custody,
            transaction.assigned is transaction.resumed is True,
            process is not None and process.poll() is not None,
            transaction.matches(process, transaction.pid, transaction.host_start_time),
            reader_eof is True,
            transfer.reader_stop is not None and not transfer.reader_stop.is_set(),
            transfer.stdin_thread is None or not transfer.stdin_thread.is_alive(),
            not transfer.stdin_errors,
            binding is not None and record is not None and record.binding is binding,
            isinstance(binding.session_id, str) and bool(binding.session_id),
            binding.session_key == session.session_key,
            (
                binding.session_id == session.parent_session_id
                if session.parent_session_id
                else binding.session_id == binding.session_key
            ),
            isinstance(binding.tool_call_id, str) and bool(binding.tool_call_id),
            isinstance(binding.turn_id, str) and bool(binding.turn_id),
            type(binding.run_generation) is int and binding.run_generation >= 0,
            session.custody_owner_id == custody.owner_id,
            session.profile_id == custody.profile_id,
            session.process_instance == custody.process_instance,
            session.authority_epoch == custody.authority_epoch,
            handle is not None
            and handle.owner_id == custody.owner_id
            and handle.profile_id == custody.profile_id
            and handle.process_instance == custody.process_instance
            and handle.authority_epoch == custody.authority_epoch
            and handle.coordinator_id == coordinator._coordinator_id
            and type(record.delivery_close_generation) is int
            and handle.close_generation == record.delivery_close_generation,
            coordinator.process_instance == custody.process_instance
            and coordinator.authority_epoch == custody.authority_epoch,
        )):
            return False
        if not native_scope_observation_ready(custody):
            return False
        return reconcile_native_custody(custody, observation_ready=True)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def native_scope_observation_retry_ready(custody: Any) -> bool:
    """Admit one retry after a retained Job observation, never a replacement owner."""
    try:
        transaction = custody.native_scope
        job = transaction.job
        handle = custody.handle
        coordinator = custody.coordinator
        reason = custody.reason
        expected_failures = (
            1 if reason == NATIVE_SCOPE_OBSERVATION_FAILURE
            else getattr(transaction, "scope_observation_failures", 0)
        )
        expected_retries = 1 if reason == NATIVE_SCOPE_OBSERVATION_FAILURE else 0
        return all((
            custody.state == "unknown" and native_scope_pending_reason(reason),
            transaction.custody is custody and transaction.state in {
                "resumed", "termination_requested",
            },
            transaction.scope_observation == "empty"
            and transaction.scope_observation_retries == expected_retries
            and transaction.scope_observation_failures == expected_failures
            and transaction.native_empty_observed is True,
            transaction.process is not None and transaction.assigned is True
            and transaction.resumed is True and custody.process_bound
            and transaction.matches(transaction.process, custody.pid, custody.host_start_time),
            transaction.scope_observation_job is job
            and transaction.scope_observation_job_handle is job._handle
            and transaction.scope_observation_process is transaction.process
            and transaction.scope_observation_pid == transaction.pid
            and transaction.scope_observation_start_time == transaction.host_start_time,
            transaction.process_handle_closed is False
            and transaction.job_handle_closed is False
            and job._recovery_only is not True and job._handle is not None,
            handle is not None
            and handle.owner_id == custody.owner_id
            and handle.profile_id == custody.profile_id
            and handle.process_instance == custody.process_instance
            and handle.authority_epoch == custody.authority_epoch
            and handle.coordinator_id == coordinator._coordinator_id,
            coordinator.process_instance == custody.process_instance
            and coordinator.authority_epoch == custody.authority_epoch,
        ))
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def foreground_delivery_retry_ready(custody: Any, record: Any) -> bool:
    """Admit only the already-authenticated DB delivery retry for this native owner."""
    try:
        transaction = custody.native_scope
        handle, coordinator = custody.handle, custody.coordinator
        binding, proof = record.binding, record.durable_proof
        return all((
            custody.state == "unknown" and custody.reason == FOREGROUND_DELIVERY_FAILURE,
            custody.durable is True and custody.delivery_acked is True,
            record.phase == "delivery_committed" and isinstance(record.delivery_token, str)
            and bool(record.delivery_token) and type(record.delivery_close_generation) is int,
            binding is custody.foreground_binding and binding is not None
            and proof is not None and proof.raw_flush_result is True
            and proof.session_id == binding.session_id
            and proof.tool_call_id == binding.tool_call_id
            and proof.database is not None,
            transaction.custody is custody and transaction.matches(
                transaction.process, transaction.pid, transaction.host_start_time,
            ),
            handle.owner_id == custody.owner_id and handle.profile_id == custody.profile_id
            and handle.process_instance == custody.process_instance
            and handle.authority_epoch == custody.authority_epoch
            and handle.coordinator_id == coordinator._coordinator_id
            and handle.close_generation == record.delivery_close_generation,
            coordinator.process_instance == custody.process_instance
            and coordinator.authority_epoch == custody.authority_epoch,
        ))
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return False


def can_reconcile_unknown(custody: Any, transfer: Any, record: Any) -> bool:
    """Keep every untyped unknown sticky; admit only the three authenticated phases."""
    if custody.reason == MANAGED_TERMINATION_FAILURE:
        return transfer is not None and managed_retry_complete(
            transfer, custody, require_delivery=True,
        )
    if custody.reason == NATIVE_CLOSE_FAILURE:
        return native_close_retry_ready(custody)
    if custody.reason == FOREGROUND_DELIVERY_FAILURE:
        return record is not None and foreground_delivery_retry_ready(custody, record)
    if native_scope_pending_reason(custody.reason):
        return native_scope_observation_retry_ready(custody)
    return False
