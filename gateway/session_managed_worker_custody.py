"""I04/I16 custody adapter for the admission-owned managed worker.

The managed worker is an actual producer process, not a generic PID slot.  This
module only composes the existing ``SidecarOwner``/``ServiceProcessReceipt``
objects with the worker's real process, readers, and finish proof.  It does not
create another ledger or a successor/adoption protocol.
"""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass
import os
import subprocess
from typing import Any

from gateway.runtime_service_custody import DurableResultProof
from gateway.runtime_sidecar_sources import SidecarOwner, reserve_managed_sidecar


class ManagedWorkerCustodyError(RuntimeError):
    """A managed worker could not cross an exact native custody boundary."""


class _ManagedWorkerPosixScope:
    """Observe and terminate the exact process group created for one worker."""

    __slots__ = ("pgid", "process")

    def __init__(self, process: Any, pgid: int):
        self.process = process
        self.pgid = pgid

    def observe_empty(self) -> bool | None:
        if self.process.poll() is None:
            return False
        killpg = getattr(os, "killpg", None)
        if killpg is None:
            return False
        try:
            killpg(self.pgid, 0)
        except ProcessLookupError:
            return True
        except (OSError, PermissionError):
            return False
        return False

    def terminate(self) -> None:
        killpg = getattr(os, "killpg", None)
        if killpg is None or os.name == "nt" or self.pgid == os.getpgrp():
            raise OSError("managed worker process group is not a safe source-owned scope")
        if self.observe_empty() is True:
            return
        try:
            pid = self.process.pid
            leader_live = pid > 0 and self.process.poll() is None
            leader_group = os.getpgid(pid) if leader_live else None
        except (AttributeError, OSError, ProcessLookupError, TypeError, ValueError):
            leader_live, leader_group = False, None
        if not leader_live or leader_group != self.pgid:
            raise OSError("managed worker process group identity is no longer current")
        killpg(self.pgid, 15)


@dataclass
class ManagedWorkerCustody:
    """One exact managed-worker view over the existing I16 sidecar owner."""

    owner: SidecarOwner
    binding: Any
    store_path: str

    @property
    def service(self):
        return self.owner.service

    @property
    def request(self):
        return self.owner.request

    @property
    def physical(self):
        return self.owner.physical

    @property
    def parent(self):
        return self.owner.parent

    def frame_metadata(self) -> dict[str, Any]:
        """Return source-owned finish identity for the child, never a caller claim."""
        return {
            "admission_id": self.binding.admission_id,
            "session_id": self.binding.session_id,
            "generation": self.binding.execution_generation,
            "store_path": self.store_path,
        }

    def configure_streams(self, streams) -> None:
        from gateway.runtime_sidecar_transport import configure_reader_streams

        if not configure_reader_streams(self.physical, streams):
            raise ManagedWorkerCustodyError("required_streams_invalid")

    def launch_windows(self, args, **kwargs):
        """Launch through the existing suspended Job transaction."""
        from tools.process_registry_windows import begin_windows_launch

        transaction = begin_windows_launch(self.physical)
        if self.physical.retain_launch_transaction(transaction) is not True:
            raise ManagedWorkerCustodyError("launch_transaction_identity_mismatch")

        # ``WindowsLaunchTransaction.launch`` deliberately keeps the exact Popen/Job
        # objects before assignment, scope capture, and resume.  Publish the same
        # process to the source owner at that first post-Popen boundary too; the
        # caller has not constructed ManagedWorker yet, and an opaque exception in
        # any later native phase must not leave the process reachable only through a
        # local transaction.
        caller_hook = kwargs.get("on_process_bound")

        def on_process_bound(bound_transaction):
            if (
                bound_transaction is not transaction
                or getattr(bound_transaction, "process", None) is None
            ):
                raise ManagedWorkerCustodyError("launch_process_identity_mismatch")
            process = bound_transaction.process
            self.owner.process = process
            self.owner.effect_observed = True
            if callable(caller_hook):
                caller_hook(bound_transaction)

        kwargs["on_process_bound"] = on_process_bound
        try:
            return transaction.launch(args, **kwargs)
        except BaseException:  # health: allow BLE001 -- native post-Popen failures retain the exact owner
            process = getattr(transaction, "process", None)
            if process is not None:
                self.cleanup_spawned_process(process)
            raise

    def cleanup_spawned_process(self, process: Any) -> None:
        """Cancellation hook retaining the exact process before ordinary worker cleanup."""
        if process is None:
            return
        transaction = getattr(self.physical, "transaction", None)
        if transaction is not None and getattr(transaction, "process", None) is not process:
            self.mark_unknown("launch_process_identity_mismatch")
            return
        # The Windows transaction may have returned a process object to no caller
        # at all: assignment, scope capture, resume, or a post-launch callback can
        # still fail before ``bind_windows_transaction`` runs.  Record the exact
        # source object before attempting cleanup, never a replacement or PID-only
        # stand-in.
        self.owner.process = process
        self.owner.effect_observed = True
        try:
            self.physical.retain_partial_launch(process, transaction, reason="partial_launch")
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            self.mark_unknown("partial_launch")
        try:
            from gateway.session_managed_worker import ManagedWorker

            worker = ManagedWorker(process)
            # Before POSIX bind there is no authenticated process-group provider yet;
            # keep the worker's existing exact-group cleanup in force.  A Windows
            # transaction is already source-owned, so its native callback is safe.
            if (getattr(self.physical, "contained_scope_provider", None) is not None
                    or transaction is not None):
                self.attach_worker(worker)
            worker.close()
        except BaseException:  # health: allow BLE001 -- cleanup failure retains the same native receipt
            self.mark_unknown("partial_launch")

    def bind_process(self, process: Any) -> None:
        """Bind the exact Popen object and an authenticated native scope."""
        if process is None:
            self.mark_unknown("partial_launch")
            raise ManagedWorkerCustodyError("physical_launch_identity_mismatch")
        physical = self.physical
        pid = getattr(process, "pid", None)
        if type(pid) is not int or pid <= 0:
            self.mark_unknown("process_identity_rejected")
            raise ManagedWorkerCustodyError("physical_launch_identity_mismatch")
        if os.name == "nt":
            # WindowsLaunchTransaction.launch performs the suspended-process identity
            # bind before resume.  A caller that reaches this branch without the
            # transaction has no native owner and must remain UNKNOWN.
            self.mark_unknown("windows_process_scope_unavailable")
            raise ManagedWorkerCustodyError("windows_process_scope_unavailable")
        try:
            pgid = os.getpgid(pid)
        except (OSError, ProcessLookupError):
            self.mark_unknown("physical_scope_unknown")
            raise ManagedWorkerCustodyError("physical_scope_unknown") from None
        if type(pgid) is not int or pgid != pid:
            physical.retain_partial_launch(
                process,
                reason="physical_scope_unknown",
                scope="posix_process_group",
                scope_identity=f"posix_process_group:{pid}:{pgid}",
                process_group_id=pgid if type(pgid) is int else None,
            )
            raise ManagedWorkerCustodyError("physical_scope_unknown")
        if not physical.bind_process(
            pid, None, "posix_process_group", f"posix_process_group:{pid}:{pgid}",
            process_group_id=pgid,
        ):
            physical.retain_partial_launch(
                process,
                reason="process_identity_rejected",
                scope="posix_process_group",
                scope_identity=f"posix_process_group:{pid}:{pgid}",
                process_group_id=pgid,
            )
            raise ManagedWorkerCustodyError("physical_launch_identity_mismatch")
        try:
            physical.contained_scope_provider = _ManagedWorkerPosixScope(process, pgid)
            physical.bind_launch(process)
        except BaseException:  # health: allow BLE001 -- partial physical binding retains the exact native child
            physical.retain_partial_launch(process, reason="partial_launch")
            raise
        self.owner.process = process
        self.owner.effect_observed = True
        self.parent.state = "physical_bound"

    def bind_windows_transaction(self, transaction: Any, process: Any) -> None:
        if transaction is None or process is None:
            self.mark_unknown("launch_transaction_missing")
            raise ManagedWorkerCustodyError("launch_transaction_missing")
        if self.physical.transaction is not transaction:
            self.mark_unknown("launch_transaction_identity_mismatch")
            raise ManagedWorkerCustodyError("launch_transaction_identity_mismatch")
        try:
            from gateway.runtime_service_operations import bind_windows_launch

            bind_windows_launch(self.parent, transaction, process)
        except BaseException:  # health: allow BLE001 -- Windows bind faults retain exact transaction custody
            self.physical.retain_partial_launch(process, transaction, reason="partial_launch")
            raise
        self.owner.process = process
        self.owner.effect_observed = True
        self.parent.state = "physical_bound"

    def attach_worker(self, worker: Any) -> None:
        """Give the worker exact native termination/readers callbacks."""
        worker._managed_worker_receipt = self.physical
        worker._managed_worker_native_terminate = self._terminate_native

    def _terminate_native(self, kill: bool = False) -> None:
        physical = self.physical
        transaction = getattr(physical, "transaction", None)
        if transaction is not None:
            terminate = getattr(transaction, "terminate_owned_tree", None)
            if getattr(transaction, "scope_ready", False) is not True:
                partial = getattr(transaction, "terminate_partial_exact", None)
                if callable(partial):
                    terminate = partial
            if not callable(terminate) or terminate() not in (True, False):
                raise ManagedWorkerCustodyError("native_termination_unknown")
            return
        provider = getattr(physical, "contained_scope_provider", None)
        if kill:
            process = getattr(provider, "process", None)
            pgid = getattr(provider, "pgid", None)
            if process is None or type(pgid) is not int or process.poll() is None:
                raise ManagedWorkerCustodyError("native_termination_unknown")
            try:
                if os.getpgid(process.pid) != pgid or pgid == os.getpgrp():
                    raise OSError("managed worker process group identity changed")
                os.killpg(pgid, 9)  # windows-footgun: ok — POSIX-only authenticated process-group scope
            except (OSError, ProcessLookupError, PermissionError) as exc:
                raise ManagedWorkerCustodyError("native_termination_unknown") from exc
            return
        terminate = getattr(provider, "terminate", None)
        if not callable(terminate):
            raise ManagedWorkerCustodyError("native_termination_unknown")
        terminate()

    def mark_unknown(self, reason: str) -> None:
        from gateway.runtime_sidecar_sources import mark_unknown

        mark_unknown(self.owner, reason)

    def release_before_effect(self) -> bool:
        from gateway.runtime_sidecar_sources import release_before_effect

        return release_before_effect(self.owner)

    def record_finish_proof(self, payload: Any) -> None:
        """Stage a typed proof produced by the worker's receipt/readback path."""
        if not isinstance(payload, dict):
            self.mark_unknown("durable_result_proof_missing")
            raise ManagedWorkerCustodyError("durable_result_proof_missing")
        fields = (
            "store_path", "session_id", "admission_id", "execution_generation",
            "row_ids", "row_snapshots", "row_sessions", "row_roles", "payload_digest",
            "result_row_ids",
        )
        if set(payload) != set(fields):
            self.mark_unknown("durable_result_proof_invalid")
            raise ManagedWorkerCustodyError("durable_result_proof_invalid")
        try:
            proof = DurableResultProof(
                store_path=payload["store_path"], session_id=payload["session_id"],
                admission_id=payload["admission_id"],
                execution_generation=payload["execution_generation"],
                row_ids=tuple(payload["row_ids"]), row_snapshots=tuple(payload["row_snapshots"]),
                row_sessions=tuple(payload["row_sessions"]), row_roles=tuple(payload["row_roles"]),
                payload_digest=payload["payload_digest"],
                result_row_ids=tuple(payload["result_row_ids"]),
            )
        except (KeyError, TypeError, ValueError):
            self.mark_unknown("durable_result_proof_invalid")
            raise ManagedWorkerCustodyError("durable_result_proof_invalid") from None
        if proof.store_path != self.store_path or proof.admission_id != self.binding.admission_id:
            self.mark_unknown("durable_result_identity_mismatch")
            raise ManagedWorkerCustodyError("durable_result_identity_mismatch")
        try:
            self.service.record_result_durable(self.binding, self.request, proof)
            self.binding.authority._preservation_release_sidecar(
                self.binding.admission_id, self.request, durable=True,
            )
        except BaseException:  # health: allow BLE001 -- durable proof failures retain the same source receipt
            self.mark_unknown("durable_result_identity_mismatch")
            raise


def _managed_cleanup_failure(worker: Any, receipt: Any, reason: str) -> None:
    if reason == "reader_close_failed":
        receipt.reader_error = reason
    receipt.mark_unknown(reason)
    worker.closed.set()
    worker._demand.release()


def _managed_stop(worker: Any) -> None:
    try:
        if worker.process.poll() is None:
            worker._signal_worker(kill=False)
            worker._signal_group(kill=False)
        try:
            worker.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            worker._signal_worker(kill=True)
            worker._signal_group(kill=True)
            with suppress(subprocess.TimeoutExpired):
                worker.process.wait(timeout=5)
        else:
            worker._signal_worker(kill=True)
    except BaseException:  # health: allow BLE001 -- native stop uncertainty retains the receipt
        _managed_cleanup_failure(worker, worker._managed_worker_receipt, "native_cleanup_interrupted")
        raise


def _managed_join_writer(worker: Any, receipt: Any) -> None:
    try:
        worker.writer.join(timeout=5)
    except BaseException:  # health: allow BLE001 -- an opaque writer join is UNKNOWN
        _managed_cleanup_failure(worker, receipt, "writer_join_failed")
        raise


def _managed_join_reader(worker: Any, receipt: Any, stream: str, reader: Any) -> None:
    try:
        reader.join(timeout=5)
    except BaseException:  # health: allow BLE001 -- an opaque reader join is UNKNOWN
        _managed_cleanup_failure(worker, receipt, "reader_join_failed")
        raise
    try:
        alive = reader.is_alive()
    except BaseException:  # health: allow BLE001 -- an opaque reader state is UNKNOWN
        _managed_cleanup_failure(worker, receipt, "reader_join_failed")
        raise
    if alive:
        receipt.mark_unknown("reader_still_alive")
        return
    try:
        receipt.reader_joined(stream, reader)
    except BaseException:  # health: allow BLE001 -- reader finalization is UNKNOWN
        _managed_cleanup_failure(worker, receipt, "reader_join_failed")
        raise


def _managed_close_stream(worker: Any, receipt: Any, stream: Any, reason: str) -> None:
    try:
        stream.close()
    except BaseException:  # health: allow BLE001 -- an opaque stream close retains custody
        _managed_cleanup_failure(worker, receipt, reason)
        raise


def close_managed_worker(worker: Any, receipt: Any) -> None:
    """Close an admitted worker while retaining every native fault on its receipt."""
    _managed_stop(worker)
    if worker.writer.ident is not None:
        _managed_join_writer(worker, receipt)
    _managed_close_stream(worker, receipt, worker.process.stdin, "writer_close_failed")
    if worker.reader is None:
        _managed_close_stream(worker, receipt, worker.process.stdout, "reader_close_failed")
    else:
        worker._demand.release()
        _managed_join_reader(worker, receipt, "stdout", worker.reader)
    if worker.stderr_drain is not None:
        _managed_join_reader(worker, receipt, "stderr", worker.stderr_drain)
    elif worker.process.stderr is not None:
        _managed_close_stream(worker, receipt, worker.process.stderr, "reader_close_failed")
    worker.closed.set()
    worker._demand.release()


def reserve_managed_worker(binding: Any, store_path: str) -> ManagedWorkerCustody:
    """Reserve I16 service/request/physical custody before worker Popen."""
    if type(store_path) is not str or not store_path:
        raise ManagedWorkerCustodyError("durable_store_identity_missing")
    owner = reserve_managed_sidecar(
        binding,
        family="managed_worker",
        source_identity=f"managed-worker:{binding.admission_id}",
        producer_key="provider",
        operation_id=binding.admission_id,
    )
    try:
        from gateway.runtime_service_operations import reserve_physical

        physical = reserve_physical(owner.parent)
        owner.physical = physical
        physical.source_owner = owner
        physical.source_parent = owner.parent
        physical.source_service = owner.service
        physical.source_root_handle = owner.handle
        physical.ephemeral_process = True
        return ManagedWorkerCustody(owner, binding, store_path)
    except BaseException as exc:  # health: allow BLE001 -- reservation faults retain the exact parent lease
        import logging
        logging.getLogger(__name__).warning(
            'managed sidecar reservation failed: %s %s', type(exc).__name__,
            getattr(exc, 'code', getattr(exc, 'reason', str(exc))),
        )
        from gateway.runtime_sidecar_sources import mark_unknown

        mark_unknown(owner, "partial_launch")
        raise


def finish_reader(custody: ManagedWorkerCustody, stream: str, reader: Any, *, eof: bool) -> None:
    """Publish reader EOF/join only when the actual reader observed it."""
    physical = custody.physical
    try:
        if eof:
            physical.reader_eof(stream, reader)
        physical.reader_joined(stream, reader)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        custody.mark_unknown("reader_state_unknown")


def reserve_execution_custody(authority: Any, binding: Any) -> ManagedWorkerCustody | None:
    """Reserve the I16 tuple only for an authenticated managed I04 admission."""
    if binding is None:
        return None
    from gateway.session_turn_physical_tip import canonical_store_path

    return reserve_managed_worker(binding, canonical_store_path(authority))


def attach_bootstrap_metadata(custody: ManagedWorkerCustody | None, frame: dict, process: Any, scope: dict) -> None:
    """Add the source-owned child proof identity after the worker scope is reserved."""
    if custody is None:
        return
    frame["managed_custody"] = custody.frame_metadata()
    custody.physical.managed_witness = {
        "kind": "managed_worker",
        "launcher_pid": getattr(process, "pid", None),
        "worker_pid": scope["pid"],
        "worker_birth": scope["birth"],
        "execution_id": scope["execution_id"],
        "generation": scope["generation"],
    }


def retain_after_failure(custody: ManagedWorkerCustody | None, scope: Any) -> None:
    """Release only a pre-effect reservation; retain every accepted/partial child UNKNOWN."""
    if custody is None:
        return
    physical = custody.physical
    if scope is None:
        if getattr(physical, "process", None) is None:
            with suppress(Exception):
                custody.release_before_effect()
        else:
            custody.mark_unknown("partial_launch")
    else:
        custody.mark_unknown("managed_worker_unknown")


async def settle_managed_worker(
    worker: Any, process: Any, custody: ManagedWorkerCustody | None, proof: Any,
) -> None:
    """Keep proof, native process, and readers on the same receipt until all close gates pass."""
    if custody is not None:
        custody.record_finish_proof(proof)
    with suppress(subprocess.TimeoutExpired):
        await asyncio.to_thread(process.wait, 10)
    await asyncio.to_thread(worker.close)
    if custody is not None and not custody.physical.close_native_after_process():
        custody.mark_unknown("physical_custody_pending")
        raise ManagedWorkerCustodyError("managed_worker_native_close_unknown")

