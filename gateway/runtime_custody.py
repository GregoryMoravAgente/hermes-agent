"""Authenticated preservation-drain custody for the live gateway runtime.

This is deliberately a narrow runtime coordinator around ``RetirementFence``.  The fence remains
responsible for lock-linearized exact leases and close generations; this module adds the closed
source-graph owner manifest, terminal/unknown state, and the retained native handoff receipt.
It never treats a counter, empty registry, PID, task completion, or deadline as proof of zero.
"""
from __future__ import annotations

from dataclasses import dataclass
import math
import secrets
import threading
import time
from typing import Any, Mapping, Optional

from hermes_cli.backend_retirement import (
    RetirementChildReservation,
    RetirementClose,
    RetirementLease,
    retirement,
)


@dataclass(frozen=True)
class OwnerSpec:
    owner_class: str
    source: str
    acceptance: str
    terminal: str
    child_or_handoff: str


# This is the closed-world source graph from the AGC1663 full20 contract.  The coordinator refuses
# to publish a handoff if a source hook has not explicitly sealed its class for this close generation.
OWNER_SPECS = {
    spec.owner_class: spec
    for spec in (
        OwnerSpec("I01_native_inbound", "gateway/platforms/webhook_ingress.py;gateway/run_inbound.py;gateway/session_ingress.py;gateway/session_authority.py:601-662", "before payload preparation/await; durable session_admissions commit", "terminal row + completion publication + delivery marker + waiter", "TurnLease or exact successor authority"),
        OwnerSpec("I02_api_session_turn", "gateway/platforms/api_server_openai_routes.py;gateway/platforms/api_server_response_admissions.py:_admit;gateway/session_api_turn.py:176-259;gateway/session_authority.py:140-331", "before context preparation; durable API/session admission", "terminal event + final delivery/receipt", "TurnLease or durable API successor"),
        OwnerSpec("I03_api_run_direct", "gateway/platforms/api_server_runs.py:236-326,704-840,997-1148;gateway/platforms/api_server.py", "before direct/canonical run task creation", "run status + stream/control finalizers + executor/process ownership", "RunLease or exact direct-run successor"),
        OwnerSpec("I04_fifo_turn_execution", "gateway/session_authority.py:1100-1195,1272-1388;gateway/session_finite.py:44-71;gateway/session_ingress.py:337-367", "after exact FIFO claim commit", "settlement + completion/delivery/waiter + child decision", "per-session mutation lock and TurnLease"),
        OwnerSpec("I05_controls_and_unknown", "gateway/session_authority.py:593-745;gateway/session_busy_controls.py;gateway/session_controls.py;gateway/run_turn_runner_approval_settle.py;gateway/run_turn_runner_clarify_delivery.py;gateway/platforms/api_server_runs.py:1291-1482", "before control mutation", "control receipt/event + approval/clarify resolution or explicit unknown", "parent TurnLease and exact generation"),
        OwnerSpec("I06_busy_followup_delivery", "gateway/session_ingress.py:385-426;gateway/session_ingress_burst.py;gateway/session_api_turn.py;gateway/session_busy_controls.py;gateway/session_bot_mailbox.py:151-206", "before observer/marker task", "observer/mailbox receipt + marker delivery/replay", "parent TurnLease or durable admission"),
        OwnerSpec("I07_automation_route_wakes", "gateway/session_automation.py:80-95,463-485,616-653;gateway/run_plugin_injection.py;gateway/session_bot.py;gateway/wake.py;gateway/run_heartbeat_acceptance.py", "before automation/wake durable FIFO commit", "wake/replacement/cancellation terminal or successor-owned", "AutomationLease or parent tick/turn"),
        OwnerSpec("I08_goals_loops_heartbeat", "gateway/run_goals.py:118-220,295-349,402-561;gateway/run_heartbeat_acceptance.py", "parent TurnLease through followup decision; TickLease before poll", "child/no-child decision + tick ack/defer + watcher join", "WatcherLease/TickLease and parent lease"),
        OwnerSpec("I09_a2a_hosted_kanban", "gateway/session_a2a.py:76-167;gateway/session_hosted_rpc.py:150-240;gateway/session_kanban.py:72-145,219-347;gateway/session_hosted_service.py;gateway/run_goals.py", "before claim/thread/callback/worker", "durable input/result + room/worker join", "room/task lease and exact authority handoff"),
        OwnerSpec("I10_startup_recovery_handoff", "gateway/run_startup.py;gateway/session_local_recovery.py;gateway/platforms/webhook_ingress.py;gateway/run_adapters.py;gateway/run_notifications.py;gateway/session_bot_mailbox.py", "before recovery/replay/handoff read or task", "replay/finalization/mailbox terminal or exact successor", "RecoveryLease and profile owner epoch"),
        OwnerSpec("I11_session_mutations_finalizers", "gateway/session_runtime_workers.py:26-119;gateway/session_automation.py:491-613;gateway/session_mutations.py;gateway/session_prune.py;gateway/session_deferred_pick.py;gateway/session_completion_notes.py;gateway/session_policy_refresh.py", "before mutation task/write", "durable commit + ordered callback/finalizer", "MutationLease and parent turn"),
        OwnerSpec("I12_cron_fire", "cron/scheduler_provider.py:183-237,293-377;cron/scheduler.py:2824-2927,3677-3850;cron/scheduler_tick.py;gateway/platforms/api_server.py:3950-4029", "before durable execution claim/fire", "execution terminal/interrupted/unknown + descendants", "CronExecutionLease and execution id"),
        OwnerSpec("I13_external_cron_worker", "cron/scheduler.py:3512-3617,3737-3819;cron/scheduler_script.py:215-280,487;cron/scheduler_detached_worker.py", "before pending handoff and Popen", "ack/result + process tree/pipes/reaper + durable terminal", "execution id + process identity + scope"),
        OwnerSpec("I14_api_detached_cron", "gateway/platforms/api_server.py:3950-4029;cron/scheduler_provider.py:fire_claimed", "before authenticated API cron claim/detach", "fire terminal + detached thread/receipt finalizer", "ApiCronLease and execution id"),
        OwnerSpec("I15_process_registry_terminal", "tools/process_registry.py:1175-1315,1563-1632,1901-1997,2233-2300,2495-2569;tools/terminal_tool_background.py:107-240,251-306;gateway/run_notifications.py:1677-1715", "before Popen/env.execute/adopt", "descendant/scope + EOF/readers + handles + watcher", "ProcessLease and atomic foreground-to-background handoff"),
        OwnerSpec("I16_turn_sidecars", "agent/copilot_acp_client.py:369-373;agent/transports/codex_app_server.py:141-149;agent/shell_hooks.py:490-497;tools/mcp_tool.py:586-596;tools/browser_tool_session.py:250-255;tools/browser_tool_lifecycle.py;tools/computer_use/cua_backend_daemon.py:166-176;tools/environments/local.py:1103-1113", "before sidecar/remote Popen or execution", "child/connection/scope finalizer + tool result", "child ServiceLease and parent turn"),
        OwnerSpec("I17_hosted_tui_services", "gateway/session_hosted_service.py;tui_gateway/host_supervisor.py:346-356;tui_gateway/server.py:294-303;tui_gateway/compute_host.py;tui_gateway/rpc_dispatch.py;tui_gateway/session_lifecycle.py", "before service/RPC worker or host process", "member handoff + service/process/thread join", "ServiceLease and exact authority epoch"),
        OwnerSpec("I18_multiplex_profile_retirement", "gateway/session_authorities.py:22-112,178-212;gateway/run_runtime.py;gateway/session_hosted_service.py", "before retiring/replacing authority", "authority/room join + registry removal + lock or successor", "ProfileLifecycleLease and new authority epoch"),
        OwnerSpec("I19_gateway_lifecycle", "gateway/run_shutdown.py;gateway/run_runtime.py;gateway/run_idle_exit.py:178-212;gateway/drain_control.py", "before restart/stop/idle/external-drain await", "foreign helper/successor/finalizer join", "ForeignLifecycleLease and close generation"),
        OwnerSpec("I20_runtime_ownership_and_foreign_handoff", "gateway/runtime_ownership.py;gateway/run_runtime.py;gateway/run_shutdown.py;gateway/run_startup.py", "before logical owner release or successor", "exact successor ACK then OS lock release", "process instance + authority epoch + successor receipt"),
    )
}
OWNER_CLASSES = tuple(OWNER_SPECS)

# These owner classes can have writers outside this gateway process (direct API runs, cron
# launchers, detached jobs, process/sidecar services, and lifecycle successors).  A local
# RetirementFence lease is necessary but cannot prove that an independent process is absent.  The
# source hook for one of these classes therefore has to provide a real profile-lock, durable
# handoff, or OS-child proof before it may seal the class.
CROSS_PROCESS_OWNER_CLASSES = frozenset({
    "I03_api_run_direct",
    "I12_cron_fire",
    "I13_external_cron_worker",
    "I14_api_detached_cron",
    "I15_process_registry_terminal",
    "I16_turn_sidecars",
    "I17_hosted_tui_services",
    "I18_multiplex_profile_retirement",
    "I19_gateway_lifecycle",
    "I20_runtime_ownership_and_foreign_handoff",
})

_OWNER_UNKNOWN_REASONS = {
    "read_failed", "enumeration_failed", "foreign_custody", "partial_launch", "join_failed",
    "cross_process_unproven", "unmapped_writer", "unsupported_external_owner", "lifecycle_owned",
    "missing_source_hook", "native_ack_missing",
}


@dataclass(frozen=True)
class CustodyRefusal:
    code: str
    owner_class: Optional[str] = None

    def as_wire(self) -> dict[str, Any]:
        return {"status": "refused", "reason": self.code, **({"owner_class": self.owner_class} if self.owner_class else {})}


@dataclass(frozen=True)
class DrainReceipt:
    receipt_id: str
    process_instance: str
    authority_epoch: int
    close_generation: int
    state: str

    def as_wire(self) -> dict[str, Any]:
        return {
            "status": self.state,
            "receipt_id": self.receipt_id,
            "process_instance": self.process_instance,
            "authority_epoch": self.authority_epoch,
            "close_generation": self.close_generation,
        }


@dataclass(frozen=True)
class CustodyHandle:
    owner_class: str
    owner_id: str
    profile_id: Optional[str]
    authority_epoch: int
    process_instance: str
    close_generation: int
    lease: RetirementLease
    coordinator_id: str


@dataclass(frozen=True)
class ChildReservation:
    owner_class: str
    owner_id: str
    profile_id: Optional[str]
    authority_epoch: int
    process_instance: str
    close_generation: int
    reservation: RetirementChildReservation
    coordinator_id: str


@dataclass
class _Record:
    handle: CustodyHandle
    state: str = "accepted"
    finalizer_id: Optional[str] = None
    reason: Optional[str] = None


class RuntimeCustodyCoordinator:
    """One process-owned ledger for the closed runtime source graph.

    ``RetirementFence`` supplies the exact lease/close linearization.  The coordinator only
    releases a lease after the caller records terminal publication and physical finalizers, and
    it keeps an unknown record live.  No caller can force release by copying an id or generation.
    """

    def __init__(self, *, process_instance: str, authority_epoch: int, fence=retirement):
        if not isinstance(process_instance, str) or not process_instance:
            raise ValueError("process_instance is required")
        if type(authority_epoch) is not int or authority_epoch < 1:
            raise ValueError("authority_epoch is required")
        self.process_instance = process_instance
        self.authority_epoch = authority_epoch
        self._fence = fence
        self._coordinator_id = secrets.token_urlsafe(24)
        self._lock = threading.RLock()
        self._state = "open"
        self._close: Optional[RetirementClose] = None
        self._receipt_id: Optional[str] = None
        self._records: dict[str, _Record] = {}
        self._completed: dict[str, str] = {}
        # ``unwired`` is deliberately not an empty/ready result.  Every class must be sealed by
        # its source hook for this exact close generation; an unconnected class blocks handoff.
        self._owner_state = {owner_class: "unwired" for owner_class in OWNER_CLASSES}
        self._owner_reason: dict[str, str] = {}
        self._native_ack = False

    @property
    def state(self) -> str:
        with self._lock:
            return self._state

    @property
    def owner_manifest(self) -> tuple[OwnerSpec, ...]:
        return tuple(OWNER_SPECS[owner_class] for owner_class in OWNER_CLASSES)

    def begin_close(self, *, owner_id: str, scope: str = "process", deadline_s: float = 30.0):
        """Claim the reversible close before any await; no accepted lease is discarded."""
        if not isinstance(owner_id, str) or not owner_id or not isinstance(scope, str) or not scope:
            return CustodyRefusal("invalid_params")
        if not isinstance(deadline_s, (int, float)) or not math.isfinite(deadline_s) or deadline_s <= 0:
            return CustodyRefusal("invalid_deadline")
        with self._lock:
            if self._state != "open":
                return CustodyRefusal("close_already_owned")
            close = self._fence.begin_close(
                owner_kind="runtime-preservation",
                owner_id=owner_id,
                scope=scope,
                deadline_s=float(deadline_s),
            )
            if close is None:
                return CustodyRefusal("close_conflict")
            self._close = close
            self._receipt_id = secrets.token_urlsafe(24)
            self._state = "closing"
            self._owner_state = {owner_class: "unwired" for owner_class in OWNER_CLASSES}
            self._owner_reason.clear()
            self._native_ack = False
            return DrainReceipt(
                self._receipt_id,
                self.process_instance,
                self.authority_epoch,
                close.close_generation,
                "closing",
            )

    def _check_close_identity(self, receipt: DrainReceipt) -> bool:
        return (
            isinstance(receipt, DrainReceipt)
            and receipt.receipt_id == self._receipt_id
            and receipt.process_instance == self.process_instance
            and receipt.authority_epoch == self.authority_epoch
            and self._close is not None
            and receipt.close_generation == self._close.close_generation
        )

    def seal_owner(self, owner_class: str, evidence: Mapping[str, Any]):
        """Seal one explicitly wired source owner for this close generation."""
        with self._lock:
            if self._state != "closing" or owner_class not in OWNER_SPECS:
                return CustodyRefusal("owner_not_in_closing_graph", owner_class)
            if not isinstance(evidence, dict) or evidence.get("source") != OWNER_SPECS[owner_class].source:
                self._owner_state[owner_class] = "unknown"
                self._owner_reason[owner_class] = "owner_evidence_mismatch"
                return CustodyRefusal("owner_evidence_mismatch", owner_class)
            if self._close is None or evidence.get("close_generation") != self._close.close_generation:
                self._owner_state[owner_class] = "unknown"
                self._owner_reason[owner_class] = "owner_epoch_mismatch"
                return CustodyRefusal("owner_epoch_mismatch", owner_class)
            if evidence.get("process_instance") != self.process_instance or evidence.get("authority_epoch") != self.authority_epoch:
                self._owner_state[owner_class] = "unknown"
                self._owner_reason[owner_class] = "owner_epoch_mismatch"
                return CustodyRefusal("owner_epoch_mismatch", owner_class)
            if not isinstance(evidence.get("owner_id"), str) or not evidence["owner_id"]:
                self._owner_state[owner_class] = "unknown"
                self._owner_reason[owner_class] = "owner_observation_missing"
                return CustodyRefusal("owner_observation_missing", owner_class)
            if evidence.get("observed") is not True:
                self._owner_state[owner_class] = "unknown"
                self._owner_reason[owner_class] = "owner_observation_missing"
                return CustodyRefusal("owner_observation_missing", owner_class)
            if owner_class in CROSS_PROCESS_OWNER_CLASSES and evidence.get("cross_process_proof") is not True:
                self._owner_state[owner_class] = "unknown"
                self._owner_reason[owner_class] = "cross_process_unproven"
                return CustodyRefusal("cross_process_unproven", owner_class)
            if owner_class == "I20_runtime_ownership_and_foreign_handoff" and evidence.get("os_lock_owner") is not True:
                self._owner_state[owner_class] = "unknown"
                self._owner_reason[owner_class] = "foreign_custody"
                return CustodyRefusal("foreign_custody", owner_class)
            self._owner_state[owner_class] = "sealed"
            self._owner_reason.pop(owner_class, None)
            return None

    def mark_owner_unknown(self, owner_class: str, reason: str):
        with self._lock:
            if owner_class not in OWNER_SPECS:
                return CustodyRefusal("unknown_owner_class", owner_class)
            self._owner_state[owner_class] = "unknown"
            self._owner_reason[owner_class] = reason if reason in _OWNER_UNKNOWN_REASONS else "owner_unknown"
            return CustodyRefusal(self._owner_reason[owner_class], owner_class)

    def _parent(self, parent: Optional[CustodyHandle]) -> Optional[RetirementLease]:
        if parent is None:
            return None
        record = self._records.get(parent.lease.lease_id)
        if (
            record is None
            or record.handle is not parent
            or parent.coordinator_id != self._coordinator_id
            or record.state not in {"accepted", "finalizing"}
        ):
            return None
        return parent.lease

    def reserve_root(
        self,
        owner_class: str,
        owner_id: str,
        *,
        profile_id: Optional[str],
        authority_epoch: int,
        parent: Optional[CustodyHandle] = None,
    ):
        """Reserve before the first await/task/spawn; parent leases may continue through close."""
        with self._lock:
            if owner_class not in OWNER_SPECS:
                return CustodyRefusal("unknown_owner_class", owner_class)
            if authority_epoch != self.authority_epoch:
                return CustodyRefusal("stale_authority_epoch", owner_class)
            if self._state in {"handoff_pending", "closed"} and parent is None:
                return CustodyRefusal("runtime_draining", owner_class)
            if self._state == "closing" and parent is None:
                return CustodyRefusal("runtime_draining", owner_class)
            parent_lease = self._parent(parent)
            if parent is not None and parent_lease is None:
                return CustodyRefusal("stale_parent_lease", owner_class)
            lease = self._fence.acquire(
                owner_kind=owner_class,
                owner_id=owner_id,
                profile_id=profile_id,
                parent=parent_lease,
            )
            if lease is None:
                return CustodyRefusal("runtime_draining", owner_class)
            handle = CustodyHandle(
                owner_class, owner_id, profile_id, authority_epoch, self.process_instance,
                self._close.close_generation if self._close is not None else 0,
                lease, self._coordinator_id,
            )
            self._records[lease.lease_id] = _Record(handle)
            return handle

    def reserve_child(
        self,
        parent: CustodyHandle,
        owner_class: str,
        owner_id: str,
        *,
        profile_id: Optional[str],
        authority_epoch: int,
    ):
        """Reserve a continuation before close when its parent may release before activation."""
        with self._lock:
            parent_lease = self._parent(parent)
            if parent_lease is None:
                return CustodyRefusal("stale_parent_lease", owner_class)
            if owner_class not in OWNER_SPECS or authority_epoch != self.authority_epoch:
                return CustodyRefusal("invalid_child_owner", owner_class)
            reservation = self._fence.reserve_child(
                parent_lease,
                owner_kind=owner_class,
                owner_id=owner_id,
                profile_id=profile_id,
            )
            if reservation is None:
                return CustodyRefusal("child_reservation_refused", owner_class)
            return ChildReservation(
                owner_class, owner_id, profile_id, authority_epoch, self.process_instance,
                reservation.close_generation, reservation, self._coordinator_id,
            )

    def activate_child(self, child: ChildReservation):
        with self._lock:
            if (
                not isinstance(child, ChildReservation)
                or child.coordinator_id != self._coordinator_id
                or child.authority_epoch != self.authority_epoch
                or child.process_instance != self.process_instance
            ):
                return CustodyRefusal("stale_child_reservation")
            lease = self._fence.activate_child(child.reservation)
            if lease is None:
                return CustodyRefusal("child_activation_refused", child.owner_class)
            handle = CustodyHandle(
                child.owner_class, child.owner_id, child.profile_id, child.authority_epoch,
                child.process_instance, lease.close_generation, lease, self._coordinator_id,
            )
            self._records[lease.lease_id] = _Record(handle)
            return handle

    def mark_finalizing(self, handle: CustodyHandle, finalizer_id: str):
        with self._lock:
            record = self._records.get(getattr(handle.lease, "lease_id", ""))
            if record is None or record.handle is not handle:
                return CustodyRefusal("stale_lease")
            if record.state == "unknown":
                # Unknown custody is sticky: a later callback/finalizer cannot overwrite the
                # fail-closed state and make a lease appear terminally attributable again.
                return CustodyRefusal("custody_unknown", handle.owner_class)
            if not isinstance(finalizer_id, str) or not finalizer_id:
                record.state = "unknown"
                record.reason = "finalizer_identity_missing"
                return CustodyRefusal("finalizer_identity_missing", handle.owner_class)
            record.state = "finalizing"
            record.finalizer_id = finalizer_id
            return None

    def mark_unknown(self, handle: CustodyHandle, reason: str):
        with self._lock:
            record = self._records.get(getattr(handle.lease, "lease_id", ""))
            if record is None or record.handle is not handle:
                return CustodyRefusal("stale_lease")
            record.state = "unknown"
            record.reason = reason if reason in _OWNER_UNKNOWN_REASONS else "custody_unknown"
            return CustodyRefusal(record.reason, handle.owner_class)

    def mark_terminal(self, handle: CustodyHandle):
        with self._lock:
            record = self._records.get(getattr(handle.lease, "lease_id", ""))
            if record is None or record.handle is not handle:
                return CustodyRefusal("stale_lease")
            if record.state == "unknown":
                return CustodyRefusal("custody_unknown", handle.owner_class)
            if record.state not in {"accepted", "finalizing"}:
                return CustodyRefusal("invalid_terminal_transition", handle.owner_class)
            if not self._fence.release(handle.lease):
                record.state = "unknown"
                record.reason = "lease_release_failed"
                return CustodyRefusal("lease_release_failed", handle.owner_class)
            record.state = "terminal"
            self._completed[handle.lease.lease_id] = "terminal"
            del self._records[handle.lease.lease_id]
            return None

    def release_unaccepted(self, handle: CustodyHandle):
        """Release a root that is proven to have no durable admission row.

        This is intentionally narrower than ``mark_terminal``: a caller may use it only while
        the coordinator record is still ``accepted``.  Once a row could have committed, the
        caller must reconcile that row and retain the exact lease instead of guessing that a
        failed write was a refusal.
        """
        with self._lock:
            record = self._records.get(getattr(handle.lease, "lease_id", ""))
            if record is None or record.handle is not handle:
                return CustodyRefusal("stale_lease")
            if record.state != "accepted":
                return CustodyRefusal("accepted_row_unknown", handle.owner_class)
            if not self._fence.release(handle.lease):
                record.state = "unknown"
                record.reason = "lease_release_failed"
                return CustodyRefusal("lease_release_failed", handle.owner_class)
            record.state = "not_accepted"
            self._completed[handle.lease.lease_id] = "not_accepted"
            del self._records[handle.lease.lease_id]
            return None

    def release_child_reservation(self, child: ChildReservation):
        """Release a child reservation only when the FIFO claim definitely did not commit."""
        with self._lock:
            if (
                not isinstance(child, ChildReservation)
                or child.coordinator_id != self._coordinator_id
                or child.authority_epoch != self.authority_epoch
                or child.process_instance != self.process_instance
            ):
                return CustodyRefusal("stale_child_reservation")
            if not self._fence.release_child_reservation(child.reservation):
                return CustodyRefusal("child_reservation_release_failed", child.owner_class)
            return None

    def prepare_handoff(self, receipt: DrainReceipt):
        """Commit the exact close only after every owner is sealed and every lease is terminal."""
        with self._lock:
            if not self._check_close_identity(receipt) or self._state != "closing":
                return CustodyRefusal("stale_close_receipt")
            unknown = [owner_class for owner_class in OWNER_CLASSES if self._owner_state[owner_class] != "sealed"]
            if unknown:
                return CustodyRefusal("unknown_owner", unknown[0])
            pending = [record for record in self._records.values() if record.state != "terminal"]
            if pending:
                return CustodyRefusal("custody_pending", pending[0].handle.owner_class)
            # A ChildReservation is accepted custody even before FIFO claim activation.  It is
            # not represented in ``_records`` until activation, so commit must consult the
            # fence's exact reservation ledger instead of manufacturing readiness from an empty
            # local record set.
            if self._fence.pending_child_count():
                return CustodyRefusal("custody_pending", "I04_fifo_turn_execution")
            if not self._fence.commit_close(self._close):
                # This includes any legacy/foreign lease that has not been adopted into this ledger.
                return CustodyRefusal("foreign_or_unmapped_lease")
            self._state = "handoff_pending"
            return DrainReceipt(
                self._receipt_id or "",
                self.process_instance,
                self.authority_epoch,
                self._close.close_generation,
                "handoff_pending",
            )

    def acknowledge_native(self, receipt: DrainReceipt):
        with self._lock:
            if (
                self._state != "handoff_pending"
                or not isinstance(receipt, DrainReceipt)
                or receipt.receipt_id != self._receipt_id
                or receipt.process_instance != self.process_instance
                or receipt.authority_epoch != self.authority_epoch
                or self._close is None
                or receipt.close_generation != self._close.close_generation
            ):
                return CustodyRefusal("native_ack_mismatch")
            self._native_ack = True
            self._state = "closed"
            return DrainReceipt(
                self._receipt_id or "",
                self.process_instance,
                self.authority_epoch,
                self._close.close_generation,
                "closed",
            )

    def abort_before_handoff(self, receipt: DrainReceipt):
        """Reopen only the exact pre-handoff owner; unresolved leases remain live."""
        with self._lock:
            if not self._check_close_identity(receipt) or self._state != "closing":
                return CustodyRefusal("stale_close_receipt")
            if not self._fence.release_close(self._close):
                return CustodyRefusal("close_release_failed")
            self._state = "open"
            self._close = None
            self._receipt_id = None
            self._owner_state = {owner_class: "unwired" for owner_class in OWNER_CLASSES}
            self._owner_reason.clear()
            return None

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "state": self._state,
                "process_instance": self.process_instance,
                "authority_epoch": self.authority_epoch,
                "close_generation": self._close.close_generation if self._close is not None else None,
                "owner_classes": list(OWNER_CLASSES),
                "owner_states": dict(self._owner_state),
                "unresolved_owner_classes": [k for k, v in self._owner_state.items() if v != "sealed"],
                "unknown_owner_classes": [k for k, v in self._owner_state.items() if v == "unknown"],
                "unwired_owner_classes": [k for k, v in self._owner_state.items() if v == "unwired"],
                "cross_process_owner_classes": sorted(CROSS_PROCESS_OWNER_CLASSES),
                "active_owner_classes": sorted({record.handle.owner_class for record in self._records.values()}),
                "pending_child_reservations": self._fence.pending_child_count(),
                "native_ack": self._native_ack,
            }

    def remaining_seconds(self) -> Optional[float]:
        with self._lock:
            if self._close is None:
                return None
            return max(0.0, self._close.deadline - time.monotonic())


def install_runtime_custody(runner) -> RuntimeCustodyCoordinator:
    descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
    coordinator = RuntimeCustodyCoordinator(
        process_instance=descriptor.get("instance_id", ""),
        authority_epoch=descriptor.get("authority_epoch", 0),
    )
    runner.runtime_custody = coordinator
    return coordinator
