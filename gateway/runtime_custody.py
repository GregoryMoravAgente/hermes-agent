"""Authenticated preservation-drain custody for the live gateway runtime.

This is deliberately a narrow runtime coordinator around ``RetirementFence``.  The fence remains
responsible for lock-linearized exact leases and close generations; this module adds the closed
source-graph owner manifest, terminal/unknown state, and the retained native handoff receipt.
It never treats a counter, empty registry, PID, task completion, or deadline as proof of zero.
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace
import math
import secrets
import threading
import time
import weakref
from typing import Any, Mapping, Optional

from hermes_cli.backend_retirement import (
    RetirementChildReservation,
    RetirementClose,
    RetirementLease,
    retirement,
)
from gateway.runtime_custody_profiles import ProfileScopeResolver, ProfileScopeResult
from gateway.runtime_custody_i17 import I17PreparedChildCustodyMixin
from gateway.runtime_native_cleanup import (
    NativeCleanupCapability,
    NativeCleanupCompletionReceipt,
    NativeCleanupMixin,
    NativeCleanupPlan,
    NativeCleanupResult,
    NativeCleanupZeroWorkReceipt,
)


_UNSPECIFIED_PHYSICAL_OWNER = object()


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
        OwnerSpec("I01_native_inbound", "gateway/platforms/webhook_ingress.py;gateway/platforms/msgraph_webhook.py:314-465;plugins/teams_pipeline/runtime.py:91-111;gateway/run_inbound.py;gateway/session_ingress.py;gateway/session_authority.py:601-662", "before payload preparation/await; durable session_admissions or existing scheduler job commit", "terminal row/job + completion publication + delivery marker + waiter", "TurnLease, scheduler task, or exact successor authority"),
        OwnerSpec("I02_api_session_turn", "gateway/platforms/api_server_openai_routes.py;gateway/platforms/api_server_response_admissions.py:_admit;gateway/session_api_turn.py:176-259;gateway/session_authority.py:140-331", "before context preparation; durable API/session admission", "terminal event + final delivery/receipt", "TurnLease or durable API successor"),
        OwnerSpec("I03_api_run_direct", "gateway/platforms/api_server_runs.py:236-326,704-840,997-1148;gateway/platforms/api_server.py", "before direct/canonical run task creation", "run status + stream/control finalizers + executor/process ownership", "RunLease or exact direct-run successor"),
        OwnerSpec("I04_fifo_turn_execution", "gateway/session_authority.py:1100-1195,1272-1388;gateway/session_finite.py:44-71;gateway/session_ingress.py:337-367", "after exact FIFO claim commit", "settlement + completion/delivery/waiter + child decision", "per-session mutation lock and TurnLease"),
        OwnerSpec("I05_controls_and_unknown", "gateway/session_controls.py:163-224,226-317,731-774;gateway/session_busy_controls.py:174-201;gateway/platforms/api_server_runs.py:1291-1491;gateway/run_turn_runner_approval_settle.py:23-84;gateway/run_turn_runner_clarify_delivery.py:42-184", "before control mutation", "control receipt/event + approval/clarify resolution or explicit unknown", "parent TurnLease and exact generation"),
        OwnerSpec("I06_busy_followup_delivery", "gateway/session_ingress.py:91-170,337-426;gateway/session_ingress_burst.py:44-132;gateway/session_bot_mailbox.py:151-206", "before observer/marker task", "observer/mailbox receipt + marker delivery/replay", "parent TurnLease or durable admission"),
        OwnerSpec("I07_automation_route_wakes", "gateway/session_automation.py:80-95,463-485,616-653;gateway/run_plugin_injection.py;gateway/session_bot.py;gateway/wake.py;gateway/run_heartbeat_acceptance.py", "before automation/wake durable FIFO commit", "wake/replacement/cancellation terminal or successor-owned", "AutomationLease or parent tick/turn"),
        OwnerSpec("I08_goals_loops_heartbeat", "gateway/run_goals.py:118-220,295-349,402-561;gateway/run_heartbeat_acceptance.py", "parent TurnLease through followup decision; TickLease before poll", "child/no-child decision + tick ack/defer + watcher join", "WatcherLease/TickLease and parent lease"),
        OwnerSpec("I09_a2a_hosted_kanban", "gateway/session_a2a.py:76-167;gateway/session_hosted_rpc.py:150-240;gateway/session_kanban.py:72-145,219-347;gateway/session_hosted_service.py;gateway/run_goals.py", "before claim/thread/callback/worker", "durable input/result + room/worker join", "room/task lease and exact authority handoff"),
        OwnerSpec("I10_startup_recovery_handoff", "gateway/run_startup.py;gateway/session_local_recovery.py;gateway/platforms/webhook_ingress.py;gateway/run_adapters.py;gateway/run_notifications.py;gateway/session_bot_mailbox.py", "before recovery/replay/handoff read or task", "replay/finalization/mailbox terminal or exact successor", "RecoveryLease and profile owner epoch"),
        OwnerSpec("I11_session_mutations_finalizers", "gateway/run_runtime.py:36-90,140-187,886-926;gateway/session_runtime_workers.py:26-164;gateway/session_authority_operations.py:895-1166;gateway/session_local_adoption.py:26-233;gateway/session_local_recovery.py:238-562;gateway/session_mutations.py:73-317;gateway/session_automation.py:491-613;gateway/session_deferred_pick.py:16-136;gateway/session_completion_notes.py:35-61;gateway/session_prune.py:24-61;gateway/session_policy_refresh.py:167-260", "before mutation task/write", "durable commit + ordered callback/finalizer", "MutationLease and parent turn"),
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
class PostOpenTransitionCapability:
    """Coordinator-issued authority for cleanup after a reversible OPEN transition.

    The wire ``DrainReceipt`` remains the pre-open retry identity.  Once the coordinator
    releases the close fence, source cleanup must use this exact capability rather than
    replaying a receipt or manufacturing a new generation.
    """

    coordinator: Any = field(repr=False, compare=False)
    receipt: DrainReceipt = field(repr=False, compare=False)
    transition: object = field(repr=False, compare=False)
    process_instance: str
    authority_epoch: int
    close_generation: int
    source_participants: tuple[tuple[Any, Any, Any], ...] = field(
        default_factory=tuple, repr=False, compare=False,
    )


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
    physical_owner_epoch: Optional[str] = None


@dataclass(frozen=True)
class TerminalRootRebindProof:
    """Source-owned proof for restoring one terminalized profile root during abort."""

    source: str
    predecessor: CustodyHandle
    predecessor_owner_id: str
    process_instance: str
    profile_id: str
    authority_epoch: int
    close_generation: int
    physical_owner_epoch: Optional[str]
    producer_graph_digest: str
    producer_registry: object
    source_binding: object


@dataclass(frozen=True)
class TerminalRootRebindObservation:
    """Fresh observation returned by the canonical I17 source binding at the lock boundary."""

    source: str
    predecessor: CustodyHandle
    predecessor_owner_id: str
    process_instance: str
    profile_id: str
    authority_epoch: int
    close_generation: int
    physical_owner_epoch: Optional[str]
    producer_graph_digest: str
    producer_registry: object
    source_binding: object


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
    physical_owner_epoch: Optional[str] = None


@dataclass
class _Record:
    handle: CustodyHandle
    state: str = "accepted"
    finalizer_id: Optional[str] = None
    reason: Optional[str] = None


class RuntimeCustodyCoordinator(I17PreparedChildCustodyMixin, NativeCleanupMixin):
    """One process-owned ledger for the closed runtime source graph.

    ``RetirementFence`` supplies the exact lease/close linearization.  The coordinator only
    releases a lease after the caller records terminal publication and physical finalizers, and
    it keeps an unknown record live.  No caller can force release by copying an id or generation.
    """

    def __init__(
        self, *, process_instance: str, authority_epoch: int, fence=retirement,
        profile_scope: ProfileScopeResolver | None = None,
    ):
        if not isinstance(process_instance, str) or not process_instance:
            raise ValueError("process_instance is required")
        if type(authority_epoch) is not int or authority_epoch < 1:
            raise ValueError("authority_epoch is required")
        self.process_instance = process_instance
        self.authority_epoch = authority_epoch
        self._fence = fence
        self._profile_scope = profile_scope
        self._coordinator_id = secrets.token_urlsafe(24)
        self._lock = threading.RLock()
        self._state = "open"
        self._close: Optional[RetirementClose] = None
        # The authority owns the source binding.  Keep only a weak coordinator witness so retired
        # profiles do not leave a strong global profile/binding cache behind.
        self._source_rebind_bindings = weakref.WeakValueDictionary()
        self._source_rebind_transition = object()
        self._post_open_transition: PostOpenTransitionCapability | None = None
        self._completed_post_open_transition: PostOpenTransitionCapability | None = None
        self._receipt_id: Optional[str] = None
        self._records: dict[str, _Record] = {}
        self._completed: dict[str, str] = {}
        # Retain the exact terminal handle as a short-lived identity witness.  A lease id,
        # process tuple, or copied generation is not enough to restore a reversible source root
        # after its old lease was terminalized; the abort path must prove that it owns the same
        # in-memory handle that this coordinator retired.
        self._completed_handles: weakref.WeakValueDictionary[str, CustodyHandle] = (
            weakref.WeakValueDictionary()
        )
        # A terminal I17 root is a one-shot predecessor for reversible abort.  Keep this separate
        # from the historical completed-status/identity map so ``is_terminal_handle`` retains its
        # existing compatibility meaning while a replay of the same old root cannot mint another
        # live root.
        self._rebound_terminal_roots: weakref.WeakValueDictionary[str, CustodyHandle] = (
            weakref.WeakValueDictionary()
        )
        # A prepared I17 abort needs one short-lived witness tying the live successor root to the
        # exact terminal predecessor, source binding, proof, and close receipt that created it.
        # This is an identity witness, not a second ownership ledger; it is consumed before the
        # coordinator reopens and is cleared with the close transition.
        self._rebound_successor_witnesses: dict[str, tuple] = {}
        # Keep exact parent/child identity in the coordinator itself.  The RetirementFence
        # retains the opaque leases, but its public count is intentionally only diagnostic;
        # this index is what prevents a parent terminalizer from racing an active child.
        self._children_by_parent: dict[str, set[str]] = {}
        self._pending_children_by_parent: dict[str, set[str]] = {}
        # A sidecar request has one primary service parent and one exact active-I04 parent.
        # Keep both directions in this same ledger so a copied parent cannot be detached by a
        # caller that only knows the request lease id.
        self._secondary_children_by_parent: dict[str, set[str]] = {}
        self._secondary_parents_by_child: dict[str, set[str]] = {}
        # Direct API runs are admitted before the canonical I02/I04 parent exists.  Keep that
        # later parent edge in this same coordinator, under the same lock and lease identities;
        # no second ledger or copied parent id can release the run.
        self._bound_parent_by_child: dict[str, str] = {}
        # ``unwired`` is deliberately not an empty/ready result.  Every class must be sealed by
        # its source hook for this exact close generation; an unconnected class blocks handoff.
        self._owner_state = {owner_class: "unwired" for owner_class in OWNER_CLASSES}
        self._owner_reason: dict[str, str] = {}
        self._native_ack = False
        self._native_cleanup_plan: NativeCleanupPlan | None = None
        self._native_cleanup_capability: NativeCleanupCapability | None = None
        self._native_cleanup_state = "not_started"
        self._native_cleanup_reason: str | None = None
        self._native_cleanup_in_progress = False
        self._native_cleanup_pending_participants: tuple[tuple[Any, Any, Any], ...] = ()

    def _profile_scope_result_locked(
        self, profile_id: Optional[str], authority_epoch: int, process_instance: str, *,
        require_serving: bool, allow_retiring: bool,
    ) -> ProfileScopeResult:
        """Resolve a profile tuple while ``_lock`` is held; no profile epoch is cached."""
        if profile_id is None:
            if process_instance != self.process_instance:
                return ProfileScopeResult(False, "stale_process_instance", "")
            if authority_epoch != self.authority_epoch:
                return ProfileScopeResult(False, "stale_authority_epoch", "")
            return ProfileScopeResult(True, "", "", self.authority_epoch)
        if self._profile_scope is None:
            if process_instance != self.process_instance:
                return ProfileScopeResult(False, "stale_process_instance", str(profile_id))
            if authority_epoch != self.authority_epoch:
                return ProfileScopeResult(False, "stale_authority_epoch", str(profile_id))
            return ProfileScopeResult(True, "", str(profile_id), authority_epoch)
        return self._profile_scope.check(
            profile_id, authority_epoch, process_instance,
            require_serving=require_serving, allow_retiring=allow_retiring,
        )

    def _scope_refusal_locked(
        self, profile_id: Optional[str], authority_epoch: int, owner_class: str, *,
        require_serving: bool, allow_retiring: bool,
        expected_physical_owner_epoch: object = _UNSPECIFIED_PHYSICAL_OWNER,
    ) -> CustodyRefusal | None:
        result = self._profile_scope_result_locked(
            profile_id, authority_epoch, self.process_instance,
            require_serving=require_serving, allow_retiring=allow_retiring,
        )
        if result.ok and (
            self._profile_scope is None
            or profile_id is None
            or expected_physical_owner_epoch is _UNSPECIFIED_PHYSICAL_OWNER
            or (
                type(expected_physical_owner_epoch) is str
                and result.physical_owner_epoch == expected_physical_owner_epoch
            )
        ):
            return None
        return CustodyRefusal(
            result.reason if not result.ok else "foreign_custody", owner_class,
        )

    def _scope_unknown_locked(self, handle: CustodyHandle) -> CustodyRefusal | None:
        """Retain one accepted lease unknown when its live profile owner is no longer proven."""
        result = self._profile_scope_result_locked(
            getattr(handle, "profile_id", None), getattr(handle, "authority_epoch", None),
            getattr(handle, "process_instance", None),
            require_serving=False, allow_retiring=True,
        )
        expected_owner_epoch = getattr(handle, "physical_owner_epoch", None)
        if (
            result.ok and self._profile_scope is not None
            and handle.profile_id is not None
            and (
                type(expected_owner_epoch) is not str
                or result.physical_owner_epoch != expected_owner_epoch
            )
        ):
            result = ProfileScopeResult(False, "foreign_custody", str(handle.profile_id))
        if result.ok:
            return None
        record = self._records.get(getattr(getattr(handle, "lease", None), "lease_id", ""))
        if record is not None and record.handle is handle:
            record.state = "unknown"
            record.reason = (
                result.reason if result.reason in _OWNER_UNKNOWN_REASONS else "foreign_custody"
            )
        return CustodyRefusal(result.reason, getattr(handle, "owner_class", None))

    def profile_scope_valid(
        self, profile_id: Optional[str], authority_epoch: int, *,
        require_serving: bool = False, allow_retiring: bool = True,
        physical_owner_epoch: object = _UNSPECIFIED_PHYSICAL_OWNER,
    ) -> bool:
        """Expose the same live tuple check to process-registry evidence consumers."""
        with self._lock:
            result = self._profile_scope_result_locked(
                profile_id, authority_epoch, self.process_instance,
                require_serving=require_serving, allow_retiring=allow_retiring,
            )
            return result.ok and (
                self._profile_scope is None or profile_id is None
                or physical_owner_epoch is _UNSPECIFIED_PHYSICAL_OWNER
                or (type(physical_owner_epoch) is str
                    and result.physical_owner_epoch == physical_owner_epoch)
            )

    def profile_scope_observation(
        self, profile_id: str, authority_epoch: int, *,
        require_serving: bool, allow_retiring: bool,
    ) -> ProfileScopeResult:
        """Return the live profile and physical-incarnation observation for a source validator."""
        with self._lock:
            return self._profile_scope_result_locked(
                profile_id, authority_epoch, self.process_instance,
                require_serving=require_serving, allow_retiring=allow_retiring,
            )

    def _source_binding_registration_identity_valid(
        self, owner_class: str, profile_id: str, binding, all_authorities,
    ) -> bool:
        authority = getattr(binding, "authority", None)
        runner = getattr(binding, "runner", None)
        registry = getattr(binding, "registry", None)
        if authority is None or runner is None or not isinstance(registry, dict):
            return False
        if getattr(binding, "coordinator", None) is not self:
            return False
        if getattr(authority, "_i17_rebind_source", None) is not binding:
            return False
        if not any(item is authority for item in all_authorities(runner)):
            return False
        if registry.get("source") != "tui_gateway.hosted_tui_custody:i17-producer-registry-v1":
            return False
        if getattr(authority, "profile_id", None) != profile_id:
            return False
        if getattr(authority, "runner", None) is not runner:
            return False
        if getattr(runner, "runtime_custody", None) is not self:
            return False
        if getattr(authority, "_i17_runtime_custody", None) is not self:
            return False
        return getattr(authority, "_i17_producer_registry", None) is registry

    def _source_binding_registration_scope_valid(
        self, profile_id: str, binding,
    ) -> bool:
        authority = binding.authority
        registry = binding.registry
        scope = self._profile_scope_result_locked(
            profile_id, getattr(authority, "epoch", None), self.process_instance,
            require_serving=False, allow_retiring=True,
        )
        return (
            scope.ok
            and type(scope.physical_owner_epoch) is str
            and bool(scope.physical_owner_epoch)
            and registry.get("authority_epoch") == getattr(authority, "epoch", None)
        )

    def _register_source_rebind_binding(self, owner_class: str, profile_id: str, binding):
        """Retain the boot-installed source capability used by closing-time rebind."""
        if not isinstance(owner_class, str) or not isinstance(profile_id, str) or not profile_id:
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        try:
            from tui_gateway.hosted_tui_custody import _I17RebindSource
            from gateway.session_authorities import all_authorities
        except (ImportError, AttributeError):
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        if type(binding) is not _I17RebindSource:
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        with self._lock:
            if not self._source_binding_registration_identity_valid(
                owner_class, profile_id, binding, all_authorities,
            ):
                return CustodyRefusal("owner_proof_mismatch", owner_class)
            if not self._source_binding_registration_scope_valid(profile_id, binding):
                return CustodyRefusal("owner_proof_mismatch", owner_class)
            current = self._source_rebind_bindings.get((owner_class, profile_id))
            if current is not None and current is not binding:
                return CustodyRefusal("owner_proof_mismatch", owner_class)
            self._source_rebind_bindings[(owner_class, profile_id)] = binding
            return None

    def _source_binding_is_canonical(self, owner_class: str, profile_id: str, binding) -> bool:
        """Check one boot-installed source capability by object identity under the coordinator lock."""
        with self._lock:
            return self._source_rebind_bindings.get((owner_class, profile_id)) is binding

    def _unregister_source_rebind_binding(
        self, owner_class: str, profile_id: str, binding, *, terminal_handle: CustodyHandle,
    ):
        """Release one exact source binding after its profile root is terminal.

        A weak registry value can remain alive while the retired authority is still referenced by
        a drained client or task.  The lifecycle owner therefore has to revoke that exact source
        capability before removing the authority slot.  The revocation is deliberately narrow:
        it requires the canonical binding object, its still-registered authority, the closed
        source graph, and the exact terminal handle.  A copied binding, an active/UNKNOWN root,
        or a binding that was already replaced is refused and remains fail-closed.
        """
        if (
            not isinstance(owner_class, str)
            or not isinstance(profile_id, str)
            or not profile_id
            or not isinstance(terminal_handle, CustodyHandle)
        ):
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        try:
            from tui_gateway.hosted_tui_custody import _I17RebindSource
            from gateway.session_authorities import all_authorities
        except (ImportError, AttributeError):
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        if type(binding) is not _I17RebindSource:
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        authority = getattr(binding, "authority", None)
        runner = getattr(binding, "runner", None)
        if (
            authority is None
            or runner is None
            or getattr(binding, "coordinator", None) is not self
            or getattr(authority, "_i17_rebind_source", None) is not binding
            or getattr(authority, "_i17_producer_registry", None) is not getattr(binding, "registry", None)
            or getattr(authority, "profile_id", None) != profile_id
            or getattr(authority, "_i17_service_closed", False) is not True
            or not any(item is authority for item in all_authorities(runner))
        ):
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        if not self.is_terminal_handle(
            terminal_handle,
            owner_class=owner_class,
            profile_id=profile_id,
            authority_epoch=getattr(terminal_handle, "authority_epoch", None),
        ):
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        with self._lock:
            key = (owner_class, profile_id)
            if self._source_rebind_bindings.get(key) is not binding:
                return CustodyRefusal("owner_proof_mismatch", owner_class)
            self._source_rebind_bindings.pop(key, None)
            return None

    def authority_epoch_for_profile(
        self, profile_id: str, *, require_serving: bool = True,
    ) -> int | None:
        """Read a profile's current epoch only through live authority and lock evidence."""
        if self._profile_scope is None:
            return self.authority_epoch
        return self._profile_scope.current_epoch(profile_id, require_serving=require_serving)

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
            if self._post_open_transition is not None:
                return CustodyRefusal("post_open_cleanup_pending")
            if self._state != "open":
                return CustodyRefusal("close_already_owned")
            self._completed_post_open_transition = None
            close = self._fence.begin_close(
                owner_kind="runtime-preservation",
                owner_id=owner_id,
                scope=scope,
                deadline_s=float(deadline_s),
            )
            if close is None:
                return CustodyRefusal("close_conflict")
            if not self._fence._register_abort_rebind_capability(
                close, self._coordinator_id, self._source_rebind_transition,
            ):
                self._fence.release_close(close)
                return CustodyRefusal("source_transition_unavailable")
            self._close = close
            self._receipt_id = secrets.token_urlsafe(24)
            self._state = "closing"
            self._owner_state = {owner_class: "unwired" for owner_class in OWNER_CLASSES}
            self._owner_reason.clear()
            self._native_ack = False
            self._native_cleanup_plan = None
            self._native_cleanup_capability = None
            self._native_cleanup_state = "not_started"
            self._native_cleanup_reason = None
            self._native_cleanup_in_progress = False
            self._native_cleanup_pending_participants = ()
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

    def _active_handle_locked(
        self, handle: CustodyHandle, owner_class: str, profile_id: Optional[str],
        authority_epoch: int,
    ) -> Optional[_Record]:
        """Return one exact active record while ``_lock`` is held."""
        lease_id = getattr(getattr(handle, "lease", None), "lease_id", None)
        record = self._records.get(lease_id)
        if (
            record is None
            or record.handle is not handle
            or getattr(handle, "coordinator_id", None) != self._coordinator_id
            or getattr(handle, "owner_class", None) != owner_class
            or getattr(handle, "profile_id", None) != profile_id
            or getattr(handle, "authority_epoch", None) != authority_epoch
            or getattr(handle, "process_instance", None) != self.process_instance
            or record.state not in {"accepted", "finalizing"}
        ):
            return None
        return record

    def _secondary_edge_ids_locked(self, child: CustodyHandle, parent: CustodyHandle):
        """Return exact child/parent ids for a valid existing secondary edge."""
        child_id = getattr(getattr(child, "lease", None), "lease_id", None)
        parent_id = getattr(getattr(parent, "lease", None), "lease_id", None)
        child_profile = getattr(child, "profile_id", None)
        child_epoch = getattr(child, "authority_epoch", None)
        child_record = self._active_handle_locked(
            child, "I16_turn_sidecars", child_profile, child_epoch,
        )
        parent_record = self._active_handle_locked(
            parent, "I04_fifo_turn_execution", child_profile, child_epoch,
        )
        if (
            child_record is None
            or parent_record is None
            or child_id == parent_id
            or getattr(child, "process_instance", None) != getattr(parent, "process_instance", None)
        ):
            return None
        return child_id, parent_id

    def _reserve_root_locked(
        self, owner_class: str, owner_id: str, *, profile_id: Optional[str],
        authority_epoch: int, parent: Optional[CustodyHandle] = None,
    ):
        """Reserve and index one root while ``_lock`` is held."""
        if owner_class not in OWNER_SPECS:
            return CustodyRefusal("unknown_owner_class", owner_class)
        if self._post_open_transition is not None and parent is None:
            return CustodyRefusal("post_open_cleanup_pending", owner_class)
        scope_result = self._profile_scope_result_locked(
            profile_id, authority_epoch, self.process_instance,
            require_serving=parent is None, allow_retiring=parent is not None,
        )
        if not scope_result.ok:
            return CustodyRefusal(scope_result.reason, owner_class)
        if self._state in {"handoff_pending", "closed"} and parent is None:
            return CustodyRefusal("runtime_draining", owner_class)
        if self._state == "closing" and parent is None:
            return CustodyRefusal("runtime_draining", owner_class)
        parent_lease = self._parent(parent)
        if parent is not None and parent_lease is None:
            return CustodyRefusal("stale_parent_lease", owner_class)
        if parent is not None and (
            parent.profile_id != profile_id
            or parent.authority_epoch != authority_epoch
            or parent.process_instance != self.process_instance
        ):
            return CustodyRefusal("stale_parent_lease", owner_class)
        if self._profile_scope is not None and parent is not None and (
            type(parent.physical_owner_epoch) is not str
            or scope_result.physical_owner_epoch != parent.physical_owner_epoch
        ):
            return CustodyRefusal("foreign_custody", owner_class)
        lease = self._fence.acquire(
            owner_kind=owner_class, owner_id=owner_id, profile_id=profile_id,
            parent=parent_lease,
        )
        if lease is None:
            return CustodyRefusal("runtime_draining", owner_class)
        handle = CustodyHandle(
            owner_class, owner_id, profile_id, authority_epoch, self.process_instance,
            self._close.close_generation if self._close is not None else 0,
            lease, self._coordinator_id, scope_result.physical_owner_epoch,
        )
        self._records[lease.lease_id] = _Record(handle)
        if parent_lease is not None:
            self._children_by_parent.setdefault(parent_lease.lease_id, set()).add(lease.lease_id)
        return handle

    def bind_secondary_parent(self, child: CustodyHandle, parent: CustodyHandle):
        """Bind one exact I04 parent to an I16 request before the request can run."""
        with self._lock:
            edge = self._secondary_edge_ids_locked(child, parent)
            if edge is None:
                return CustodyRefusal(
                    "secondary_parent_mismatch", getattr(child, "owner_class", None),
                )
            if self._profile_scope is not None and (
                child.physical_owner_epoch is None
                or parent.physical_owner_epoch != child.physical_owner_epoch
                or self._scope_refusal_locked(
                    child.profile_id, child.authority_epoch, child.owner_class,
                    require_serving=False, allow_retiring=True,
                    expected_physical_owner_epoch=child.physical_owner_epoch,
                ) is not None
            ):
                return CustodyRefusal("secondary_parent_mismatch", child.owner_class)
            child_id, parent_id = edge
            existing = self._secondary_parents_by_child.get(child_id, set())
            if parent_id in existing:
                return None
            if existing:
                return CustodyRefusal(
                    "secondary_parent_conflict", getattr(child, "owner_class", None),
                )
            self._secondary_parents_by_child.setdefault(child_id, set()).add(parent_id)
            self._secondary_children_by_parent.setdefault(parent_id, set()).add(child_id)
            return None

    def bind_parent(self, child: CustodyHandle, parent: CustodyHandle):
        """Bind one exact I02/I04 parent to an accepted I03 run.

        The run may be reserved before the API/session admission commits, but the edge is
        published only while both opaque handles are active in this coordinator.  A copied
        lease, a stale generation, a foreign coordinator, or a second parent is refused under
        the same lock that terminalization uses.
        """
        with self._lock:
            child_record = self._active_handle_locked(
                child, "I03_api_run_direct", child.profile_id, child.authority_epoch)
            if child_record is None:
                return CustodyRefusal("stale_child_lease", "I03_api_run_direct")
            parent_class = getattr(parent, "owner_class", None)
            if parent_class not in {"I02_api_session_turn", "I04_fifo_turn_execution"}:
                return CustodyRefusal("parent_owner_mismatch", "I03_api_run_direct")
            parent_record = self._active_handle_locked(
                parent, parent_class, child.profile_id, child.authority_epoch)
            if parent_record is None or parent.process_instance != child.process_instance:
                return CustodyRefusal("stale_parent_lease", "I03_api_run_direct")
            if self._profile_scope is not None and (
                child.physical_owner_epoch is None
                or parent.physical_owner_epoch != child.physical_owner_epoch
                or self._scope_refusal_locked(
                    child.profile_id, child.authority_epoch, child.owner_class,
                    require_serving=False, allow_retiring=True,
                    expected_physical_owner_epoch=child.physical_owner_epoch,
                ) is not None
            ):
                return CustodyRefusal("stale_parent_lease", "I03_api_run_direct")
            child_id = child.lease.lease_id
            parent_id = parent.lease.lease_id
            existing = self._bound_parent_by_child.get(child_id)
            if existing is not None:
                return (None if existing == parent_id
                        else CustodyRefusal("parent_already_bound", "I03_api_run_direct"))
            if parent_id == child_id or self._has_children(child_id):
                return CustodyRefusal("parent_cycle", "I03_api_run_direct")
            self._bound_parent_by_child[child_id] = parent_id
            self._children_by_parent.setdefault(parent_id, set()).add(child_id)
            return None

    def bound_parent(self, child: CustodyHandle):
        """Return the exact bound parent lease id for status/proof, never a copied handle."""
        with self._lock:
            return self._bound_parent_by_child.get(getattr(child.lease, "lease_id", ""))

    def has_active_bound_child(self, parent: CustodyHandle, owner_class: str) -> bool:
        """Check the live coordinator edge for one exact parent and child class."""
        with self._lock:
            parent_id = getattr(getattr(parent, "lease", None), "lease_id", None)
            parent_record = self._records.get(parent_id)
            if (
                parent_record is None
                or parent_record.handle is not parent
                or not isinstance(owner_class, str)
            ):
                return False
            return any(
                (child_record := self._records.get(child_id)) is not None
                and child_record.handle.owner_class == owner_class
                and self._bound_parent_by_child.get(child_id) == parent_id
                for child_id in self._children_by_parent.get(parent_id, ())
            )

    def _has_children(self, lease_id: str) -> bool:
        return bool(
            self._children_by_parent.get(lease_id)
            or self._secondary_children_by_parent.get(lease_id)
        )

    def _remove_secondary_edges(self, child_id: str) -> None:
        for parent_id in self._secondary_parents_by_child.pop(child_id, set()):
            children = self._secondary_children_by_parent.get(parent_id)
            if children is None:
                continue
            children.discard(child_id)
            if not children:
                self._secondary_children_by_parent.pop(parent_id, None)

    def _secondary_terminal_ids_locked(self, handle: CustodyHandle, parents):
        """Validate a terminal request and return its exact parent ids under ``_lock``."""
        child_id = getattr(getattr(handle, "lease", None), "lease_id", None)
        record = self._records.get(child_id)
        if (
            record is None
            or record.handle is not handle
            or handle.coordinator_id != self._coordinator_id
        ):
            return CustodyRefusal("stale_lease", getattr(handle, "owner_class", None))
        if handle.owner_class != "I16_turn_sidecars":
            return CustodyRefusal("secondary_parent_mismatch", handle.owner_class)
        if record.state == "unknown":
            return CustodyRefusal("custody_unknown", handle.owner_class)
        if record.state not in {"accepted", "finalizing"}:
            return CustodyRefusal("invalid_terminal_transition", handle.owner_class)
        if not isinstance(parents, tuple):
            return CustodyRefusal("secondary_parent_mismatch", handle.owner_class)
        expected = self._secondary_parents_by_child.get(child_id, set())
        supplied = set()
        for parent in parents:
            parent_id = getattr(getattr(parent, "lease", None), "lease_id", None)
            parent_record = self._active_handle_locked(
                parent, "I04_fifo_turn_execution", handle.profile_id, handle.authority_epoch,
            )
            if (
                parent_record is None
                or parent.process_instance != handle.process_instance
                or parent_id in supplied
            ):
                return CustodyRefusal("secondary_parent_mismatch", handle.owner_class)
            supplied.add(parent_id)
        if expected != supplied:
            return CustodyRefusal("secondary_parent_mismatch", handle.owner_class)
        return child_id, record

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
            return self._reserve_root_locked(
                owner_class, owner_id, profile_id=profile_id,
                authority_epoch=authority_epoch, parent=parent,
            )

    def reserve_root_with_secondary_parent(
        self, owner_class: str, owner_id: str, *, profile_id: Optional[str],
        authority_epoch: int, parent: CustodyHandle, secondary_parent: CustodyHandle,
    ):
        """Reserve an I16 request and publish both exact parent edges atomically."""
        with self._lock:
            if owner_class != "I16_turn_sidecars":
                return CustodyRefusal("secondary_parent_mismatch", owner_class)
            if self._active_handle_locked(
                parent, "I16_turn_sidecars", profile_id, authority_epoch,
            ) is None:
                return CustodyRefusal("stale_parent_lease", owner_class)
            if self._active_handle_locked(
                secondary_parent, "I04_fifo_turn_execution", profile_id, authority_epoch,
            ) is None:
                return CustodyRefusal("secondary_parent_mismatch", owner_class)
            scope_refusal = self._scope_refusal_locked(
                profile_id, authority_epoch, owner_class,
                require_serving=False, allow_retiring=True,
                expected_physical_owner_epoch=getattr(parent, "physical_owner_epoch", None),
            )
            if scope_refusal is not None:
                return CustodyRefusal("secondary_parent_mismatch", owner_class)
            if self._profile_scope is not None and (
                getattr(parent, "physical_owner_epoch", None) is None
                or getattr(secondary_parent, "physical_owner_epoch", None) is None
                or parent.physical_owner_epoch != secondary_parent.physical_owner_epoch
            ):
                return CustodyRefusal("secondary_parent_mismatch", owner_class)
            request = self._reserve_root_locked(
                owner_class, owner_id, profile_id=profile_id,
                authority_epoch=authority_epoch, parent=parent,
            )
            if isinstance(request, CustodyRefusal):
                return request
            child_id = request.lease.lease_id
            parent_id = secondary_parent.lease.lease_id
            self._secondary_parents_by_child[child_id] = {parent_id}
            self._secondary_children_by_parent.setdefault(parent_id, set()).add(child_id)
            return request

    def _i10_parent_ids_locked(
        self, parents, *, profile_id, authority_epoch, process_instance, allow_unknown=False,
    ):
        if not isinstance(parents, tuple) or not parents:
            return None
        parent_ids = set()
        allowed = {"accepted", "finalizing"}
        if allow_unknown:
            allowed.add("unknown")
        for parent in parents:
            if not isinstance(parent, CustodyHandle):
                return None
            parent_id = getattr(getattr(parent, "lease", None), "lease_id", None)
            if (parent.owner_class != "I10_startup_recovery_handoff"
                    or parent_id in parent_ids
                    or parent.profile_id != profile_id
                    or parent.authority_epoch != authority_epoch
                    or parent.process_instance != process_instance
                    or parent.coordinator_id != self._coordinator_id):
                return None
            scope_refusal = self._scope_refusal_locked(
                profile_id, authority_epoch, parent.owner_class,
                require_serving=False, allow_retiring=True,
                expected_physical_owner_epoch=parent.physical_owner_epoch,
            )
            if scope_refusal is not None:
                return None
            record = self._records.get(parent_id)
            if (record is None or record.handle is not parent
                    or record.state not in allowed):
                return None
            parent_ids.add(parent_id)
        return parent_ids

    def reserve_root_with_parents(
        self, owner_class: str, owner_id: str, *, profile_id: Optional[str],
        authority_epoch: int, parents: tuple[CustodyHandle, ...],
    ):
        """Reserve one I11 operation with every exact local I10 parent under one lock."""
        with self._lock:
            if owner_class != "I11_session_mutations_finalizers":
                return CustodyRefusal("parent_set_mismatch", owner_class)
            parent_ids = self._i10_parent_ids_locked(
                parents, profile_id=profile_id, authority_epoch=authority_epoch,
                process_instance=self.process_instance,
            )
            if parent_ids is None:
                return CustodyRefusal("parent_set_mismatch", owner_class)
            request = self._reserve_root_locked(
                owner_class, owner_id, profile_id=profile_id,
                authority_epoch=authority_epoch, parent=parents[0],
            )
            if isinstance(request, CustodyRefusal):
                return request
            child_id = request.lease.lease_id
            secondary = parent_ids - {parents[0].lease.lease_id}
            if secondary:
                self._secondary_parents_by_child[child_id] = set(secondary)
                for parent_id in secondary:
                    self._secondary_children_by_parent.setdefault(parent_id, set()).add(child_id)
            return request

    def _parent_set_terminal_ids_locked(self, handle: CustodyHandle, parents, *, allow_unknown=False):
        child_id = getattr(getattr(handle, "lease", None), "lease_id", None)
        record = self._records.get(child_id)
        if (record is None or record.handle is not handle
                or handle.coordinator_id != self._coordinator_id
                or handle.owner_class != "I11_session_mutations_finalizers"):
            return CustodyRefusal("stale_lease", getattr(handle, "owner_class", None))
        if record.state == "unknown" and not allow_unknown:
            return CustodyRefusal("custody_unknown", handle.owner_class)
        if record.state not in ({"accepted", "finalizing", "unknown"} if allow_unknown
                                else {"accepted", "finalizing"}):
            return CustodyRefusal("invalid_terminal_transition", handle.owner_class)
        parent_ids = self._i10_parent_ids_locked(
            parents, profile_id=handle.profile_id, authority_epoch=handle.authority_epoch,
            process_instance=handle.process_instance, allow_unknown=allow_unknown,
        )
        if parent_ids is None:
            return CustodyRefusal("parent_set_mismatch", handle.owner_class)
        expected = set(self._secondary_parents_by_child.get(child_id, set()))
        primary_id = getattr(getattr(handle, "lease", None), "parent_lease_id", None)
        if primary_id is not None:
            expected.add(primary_id)
        if expected != parent_ids:
            return CustodyRefusal("parent_set_mismatch", handle.owner_class)
        return child_id, record

    def mark_terminal_with_parents(
        self, handle: CustodyHandle, parents: tuple[CustodyHandle, ...],
    ):
        """Terminalize I11 only after every exact I10 parent is still active."""
        with self._lock:
            validation = self._parent_set_terminal_ids_locked(handle, parents)
            if isinstance(validation, CustodyRefusal):
                return validation
            child_id, record = validation
            if self._has_children(child_id):
                return CustodyRefusal("custody_pending", handle.owner_class)
            if not self._fence.release(handle.lease):
                record.state = "unknown"
                record.reason = "lease_release_failed"
                return CustodyRefusal("lease_release_failed", handle.owner_class)
            record.state = "terminal"
            self._completed[child_id] = "terminal"
            del self._records[child_id]
            primary_id = handle.lease.parent_lease_id
            if primary_id is not None:
                siblings = self._children_by_parent.get(primary_id)
                if siblings is not None:
                    siblings.discard(child_id)
                    if not siblings:
                        self._children_by_parent.pop(primary_id, None)
            self._children_by_parent.pop(child_id, None)
            self._bound_parent_by_child.pop(child_id, None)
            self._remove_secondary_edges(child_id)
            return None

    def reconcile_unknown_parents(
        self, parents: tuple[CustodyHandle, ...], *, profile_id, authority_epoch,
        process_instance,
    ):
        """Reopen an exact all-I10 parent tuple atomically after identity revalidation."""
        with self._lock:
            parent_ids = self._i10_parent_ids_locked(
                parents, profile_id=profile_id, authority_epoch=authority_epoch,
                process_instance=process_instance, allow_unknown=True,
            )
            if parent_ids is None:
                return CustodyRefusal("custody_identity_mismatch", "I10_startup_recovery_handoff")
            if any(self._records[parent_id].state != "unknown" for parent_id in parent_ids):
                return CustodyRefusal("custody_identity_mismatch", "I10_startup_recovery_handoff")
            for parent_id in parent_ids:
                self._records[parent_id].state = "accepted"
                self._records[parent_id].reason = None
            return None

    def reconcile_unknown_with_parents(
        self, handle: CustodyHandle, parents: tuple[CustodyHandle, ...],
    ):
        """Reopen one exact I11 plus all exact I10 parents atomically for a retry."""
        with self._lock:
            validation = self._parent_set_terminal_ids_locked(handle, parents, allow_unknown=True)
            if isinstance(validation, CustodyRefusal):
                return validation
            _child_id, record = validation
            if record.state != "unknown":
                return CustodyRefusal("custody_identity_mismatch", handle.owner_class)
            parent_ids = self._i10_parent_ids_locked(
                parents, profile_id=handle.profile_id, authority_epoch=handle.authority_epoch,
                process_instance=handle.process_instance, allow_unknown=True,
            )
            if parent_ids is None or any(self._records[parent_id].state != "unknown" for parent_id in parent_ids):
                return CustodyRefusal("custody_identity_mismatch", handle.owner_class)
            record.state = "accepted"
            record.reason = None
            for parent_id in parent_ids:
                self._records[parent_id].state = "accepted"
                self._records[parent_id].reason = None
            return None

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
            if (
                parent.profile_id != profile_id
                or parent.authority_epoch != authority_epoch
                or parent.process_instance != self.process_instance
            ):
                return CustodyRefusal("stale_parent_lease", owner_class)
            if owner_class not in OWNER_SPECS:
                return CustodyRefusal("invalid_child_owner", owner_class)
            scope_refusal = self._scope_refusal_locked(
                profile_id, authority_epoch, owner_class,
                require_serving=False, allow_retiring=True,
                expected_physical_owner_epoch=getattr(parent, "physical_owner_epoch", None),
            )
            if scope_refusal is not None:
                return scope_refusal
            reservation = self._fence.reserve_child(
                parent_lease,
                owner_kind=owner_class,
                owner_id=owner_id,
                profile_id=profile_id,
            )
            if reservation is None:
                return CustodyRefusal("child_reservation_refused", owner_class)
            self._pending_children_by_parent.setdefault(parent.lease.lease_id, set()).add(reservation.reservation_id)
            return ChildReservation(
                owner_class, owner_id, profile_id, authority_epoch, self.process_instance,
                reservation.close_generation, reservation, self._coordinator_id,
                getattr(parent, "physical_owner_epoch", None),
            )

    def activate_child(self, child: ChildReservation):
        with self._lock:
            if (
                not isinstance(child, ChildReservation)
                or child.coordinator_id != self._coordinator_id
                or child.process_instance != self.process_instance
            ):
                return CustodyRefusal("stale_child_reservation")
            scope_refusal = self._scope_refusal_locked(
                child.profile_id, child.authority_epoch, child.owner_class,
                require_serving=False, allow_retiring=True,
                expected_physical_owner_epoch=child.physical_owner_epoch,
            )
            if scope_refusal is not None:
                return CustodyRefusal("stale_child_reservation", child.owner_class)
            lease = self._fence.activate_child(child.reservation)
            if lease is None:
                return CustodyRefusal("child_activation_refused", child.owner_class)
            handle = CustodyHandle(
                child.owner_class, child.owner_id, child.profile_id, child.authority_epoch,
                child.process_instance, lease.close_generation, lease, self._coordinator_id,
                child.physical_owner_epoch,
            )
            self._records[lease.lease_id] = _Record(handle)
            pending = self._pending_children_by_parent.get(child.reservation.parent_lease_id)
            if pending is not None:
                pending.discard(child.reservation.reservation_id)
                if not pending:
                    self._pending_children_by_parent.pop(child.reservation.parent_lease_id, None)
            self._children_by_parent.setdefault(child.reservation.parent_lease_id, set()).add(lease.lease_id)
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

    def reconcile_unknown(
        self,
        handle: CustodyHandle,
        *,
        owner_id: str,
        profile_id: Optional[str],
        process_instance: str,
        authority_epoch: int,
    ):
        """Reopen one unknown lease only after the exact owner identity is revalidated.

        This is intentionally narrower than a new reservation: the caller must present the
        same in-memory handle, owner/profile/process/epoch tuple, and the fence must still hold
        the original lease.  A PID, copied generation, or newly-created lease cannot use this
        transition to turn unknown custody into clean custody.
        """
        with self._lock:
            record = self._records.get(getattr(handle.lease, "lease_id", ""))
            if (
                record is None
                or record.handle is not handle
                or record.state != "unknown"
                or handle.coordinator_id != self._coordinator_id
                or owner_id != handle.owner_id
                or profile_id != handle.profile_id
                or process_instance != handle.process_instance
                or authority_epoch != handle.authority_epoch
            ):
                return CustodyRefusal("custody_identity_mismatch", getattr(handle, "owner_class", None))
            scope_refusal = self._scope_refusal_locked(
                profile_id, authority_epoch, handle.owner_class,
                require_serving=False, allow_retiring=True,
                expected_physical_owner_epoch=handle.physical_owner_epoch,
            )
            if scope_refusal is not None:
                return scope_refusal
            record.state = "accepted"
            record.reason = None
            return None

    def mark_terminal(self, handle: CustodyHandle):
        with self._lock:
            record = self._records.get(getattr(handle.lease, "lease_id", ""))
            if record is None or record.handle is not handle:
                return CustodyRefusal("stale_lease")
            if record.state == "unknown":
                return CustodyRefusal("custody_unknown", handle.owner_class)
            if record.state not in {"accepted", "finalizing"}:
                return CustodyRefusal("invalid_terminal_transition", handle.owner_class)
            scope_refusal = self._scope_unknown_locked(handle)
            if scope_refusal is not None:
                return scope_refusal
            if self._secondary_parents_by_child.get(handle.lease.lease_id):
                return CustodyRefusal("secondary_parent_required", handle.owner_class)
            if self._has_children(handle.lease.lease_id):
                return CustodyRefusal("custody_pending", handle.owner_class)
            if not self._fence.release(handle.lease):
                record.state = "unknown"
                record.reason = "lease_release_failed"
                return CustodyRefusal("lease_release_failed", handle.owner_class)
            record.state = "terminal"
            self._completed[handle.lease.lease_id] = "terminal"
            self._completed_handles[handle.lease.lease_id] = handle
            del self._records[handle.lease.lease_id]
            parent_id = handle.lease.parent_lease_id
            if parent_id is not None:
                siblings = self._children_by_parent.get(parent_id)
                if siblings is not None:
                    siblings.discard(handle.lease.lease_id)
                    if not siblings:
                        self._children_by_parent.pop(parent_id, None)
            self._children_by_parent.pop(handle.lease.lease_id, None)
            parent_id = self._bound_parent_by_child.pop(handle.lease.lease_id, None)
            if parent_id is not None:
                siblings = self._children_by_parent.get(parent_id)
                if siblings is not None:
                    siblings.discard(handle.lease.lease_id)
                    if not siblings:
                        self._children_by_parent.pop(parent_id, None)
            self._remove_secondary_edges(handle.lease.lease_id)
            return None

    def mark_terminal_with_secondary_parents(
        self, handle: CustodyHandle, parents: tuple[CustodyHandle, ...],
    ):
        """Terminalize a request and remove its primary and exact secondary edges atomically."""
        with self._lock:
            scope_refusal = self._scope_unknown_locked(handle)
            if scope_refusal is not None:
                return scope_refusal
            validation = self._secondary_terminal_ids_locked(handle, parents)
            if isinstance(validation, CustodyRefusal):
                return validation
            child_id, record = validation
            if self._has_children(child_id):
                return CustodyRefusal("custody_pending", handle.owner_class)
            if not self._fence.release(handle.lease):
                record.state = "unknown"
                record.reason = "lease_release_failed"
                return CustodyRefusal("lease_release_failed", handle.owner_class)
            record.state = "terminal"
            self._completed[child_id] = "terminal"
            self._completed_handles[child_id] = handle
            del self._records[child_id]
            parent_id = handle.lease.parent_lease_id
            if parent_id is not None:
                siblings = self._children_by_parent.get(parent_id)
                if siblings is not None:
                    siblings.discard(child_id)
                    if not siblings:
                        self._children_by_parent.pop(parent_id, None)
            self._children_by_parent.pop(child_id, None)
            parent_id = self._bound_parent_by_child.pop(child_id, None)
            if parent_id is not None:
                siblings = self._children_by_parent.get(parent_id)
                if siblings is not None:
                    siblings.discard(child_id)
                    if not siblings:
                        self._children_by_parent.pop(parent_id, None)
            self._remove_secondary_edges(child_id)
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
            scope_refusal = self._scope_unknown_locked(handle)
            if scope_refusal is not None:
                return scope_refusal
            if self._has_children(handle.lease.lease_id):
                return CustodyRefusal("custody_pending", handle.owner_class)
            if self._secondary_parents_by_child.get(handle.lease.lease_id):
                return CustodyRefusal("secondary_parent_required", handle.owner_class)
            if not self._fence.release(handle.lease):
                record.state = "unknown"
                record.reason = "lease_release_failed"
                return CustodyRefusal("lease_release_failed", handle.owner_class)
            record.state = "not_accepted"
            self._completed[handle.lease.lease_id] = "not_accepted"
            del self._records[handle.lease.lease_id]
            parent_id = handle.lease.parent_lease_id
            if parent_id is not None:
                siblings = self._children_by_parent.get(parent_id)
                if siblings is not None:
                    siblings.discard(handle.lease.lease_id)
                    if not siblings:
                        self._children_by_parent.pop(parent_id, None)
            self._children_by_parent.pop(handle.lease.lease_id, None)
            parent_id = self._bound_parent_by_child.pop(handle.lease.lease_id, None)
            if parent_id is not None:
                siblings = self._children_by_parent.get(parent_id)
                if siblings is not None:
                    siblings.discard(handle.lease.lease_id)
                    if not siblings:
                        self._children_by_parent.pop(parent_id, None)
            return None

    def is_terminal_handle(
        self, handle: CustodyHandle, *, owner_class: str | None = None,
        profile_id: str | None = None, authority_epoch: int | None = None,
    ) -> bool:
        """Return whether this exact object was terminalized by this coordinator.

        This is intentionally stronger than checking a lease id in ``_completed``.  The I17
        reversible-abort path uses it before reserving a fresh profile root, so a stale or copied
        handle cannot be treated as an authenticated predecessor.
        """
        with self._lock:
            if self._completed.get(getattr(getattr(handle, "lease", None), "lease_id", "")) != "terminal":
                return False
            if self._completed_handles.get(handle.lease.lease_id) is not handle:
                return False
            if getattr(handle, "coordinator_id", None) != self._coordinator_id:
                return False
            return all(
                expected is None or actual == expected
                for actual, expected in (
                    (getattr(handle, "owner_class", None), owner_class),
                    (getattr(handle, "profile_id", None), profile_id),
                    (getattr(handle, "authority_epoch", None), authority_epoch),
                    (getattr(handle, "process_instance", None), self.process_instance),
                )
            )

    def is_active_handle(
        self, handle: CustodyHandle, *, owner_class: str | None = None,
        profile_id: str | None = None, authority_epoch: int | None = None,
    ) -> bool:
        """Check one exact live handle without changing its custody state."""
        with self._lock:
            record = self._records.get(getattr(getattr(handle, "lease", None), "lease_id", ""))
            if record is None or record.handle is not handle:
                return False
            if getattr(handle, "coordinator_id", None) != self._coordinator_id:
                return False
            if record.state not in {"accepted", "finalizing", "unknown"}:
                return False
            return all(
                expected is None or actual == expected
                for actual, expected in (
                    (getattr(handle, "owner_class", None), owner_class),
                    (getattr(handle, "profile_id", None), profile_id),
                    (getattr(handle, "authority_epoch", None), authority_epoch),
                    (getattr(handle, "process_instance", None), self.process_instance),
                )
            )

    def rebind_terminal_root(
        self, retired: CustodyHandle, owner_id: str, *, profile_id: str,
        authority_epoch: int, owner_class: str,
    ):
        """Reserve a new root only after proving its exact predecessor was terminalized.

        This is the narrow recovery transition for a reversible prepare/abort.  It never revives or
        copies the old lease; the old handle remains an immutable terminal receipt and the new
        root is allocated under the coordinator's current open fence.
        """
        with self._lock:
            if self._state != "open":
                return CustodyRefusal("runtime_draining", owner_class)
            lease_id = getattr(getattr(retired, "lease", None), "lease_id", None)
            if lease_id in self._rebound_terminal_roots:
                return CustodyRefusal("stale_terminal_root", owner_class)
            if not self.is_terminal_handle(
                retired, owner_class=owner_class, profile_id=profile_id,
                authority_epoch=authority_epoch,
            ):
                return CustodyRefusal("stale_terminal_root", owner_class)
            rebound = self._reserve_root_locked(
                owner_class, owner_id, profile_id=profile_id,
                authority_epoch=authority_epoch,
            )
            if getattr(rebound, "lease", None) is not None and isinstance(lease_id, str):
                self._rebound_terminal_roots[lease_id] = retired
            return rebound

    def _abort_rebind_request_refusal(
        self, owner_id: str, owner_class: str, receipt: DrainReceipt,
        source_proof: TerminalRootRebindProof,
    ):
        if self._state != "closing" or not self._check_close_identity(receipt):
            return CustodyRefusal("stale_close_receipt", owner_class)
        if owner_class != "I17_hosted_tui_services":
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        if not isinstance(owner_id, str) or not owner_id:
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        if not isinstance(source_proof, TerminalRootRebindProof):
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        return None

    def _observe_abort_rebind_source_locked(
        self, retired: CustodyHandle, profile_id: str,
        source_proof: TerminalRootRebindProof,
    ):
        binding = source_proof.source_binding
        canonical_binding = self._source_rebind_bindings.get(
            ("I17_hosted_tui_services", profile_id)
        )
        if canonical_binding is None or binding is not canonical_binding:
            return None
        observe = getattr(canonical_binding, "observe_rebind", None)
        try:
            canonical = observe(retired, self._close) if callable(observe) else None
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            canonical = None
        if not isinstance(canonical, TerminalRootRebindObservation):
            return None
        return canonical_binding, canonical

    def _abort_rebind_observation_matches(
        self, retired: CustodyHandle, profile_id: str, authority_epoch: int,
        source_proof: TerminalRootRebindProof, binding, canonical,
    ) -> bool:
        if canonical.predecessor is not retired:
            return False
        if canonical.source != "tui_gateway.hosted_tui_custody:i17-producer-registry-v1":
            return False
        if canonical.source_binding is not binding:
            return False
        if canonical.producer_registry is not source_proof.producer_registry:
            return False
        if source_proof.predecessor is not canonical.predecessor:
            return False
        if type(retired.physical_owner_epoch) is not str or not retired.physical_owner_epoch:
            return False
        if not isinstance(canonical.producer_graph_digest, str) or not canonical.producer_graph_digest:
            return False
        if canonical.physical_owner_epoch != retired.physical_owner_epoch:
            return False
        expected = (
            (canonical.source, source_proof.source),
            (canonical.predecessor_owner_id, retired.owner_id),
            (canonical.process_instance, self.process_instance),
            (canonical.profile_id, profile_id),
            (canonical.authority_epoch, authority_epoch),
            (canonical.close_generation, self._close.close_generation),
            (source_proof.predecessor_owner_id, canonical.predecessor_owner_id),
            (source_proof.process_instance, canonical.process_instance),
            (source_proof.profile_id, canonical.profile_id),
            (source_proof.authority_epoch, canonical.authority_epoch),
            (source_proof.close_generation, canonical.close_generation),
            (source_proof.physical_owner_epoch, canonical.physical_owner_epoch),
            (source_proof.producer_graph_digest, canonical.producer_graph_digest),
        )
        return all(actual == expected_value for actual, expected_value in expected)

    def _abort_rebind_predecessor_refusal_locked(
        self, retired: CustodyHandle, profile_id: str, authority_epoch: int,
        owner_class: str,
    ):
        physical_owner_epoch = getattr(retired, "physical_owner_epoch", None)
        if type(physical_owner_epoch) is not str or not physical_owner_epoch:
            return CustodyRefusal("owner_proof_mismatch", owner_class)
        lease_id = getattr(getattr(retired, "lease", None), "lease_id", None)
        if lease_id in self._rebound_terminal_roots:
            return CustodyRefusal("stale_terminal_root", owner_class)
        if not self.is_terminal_handle(
            retired, owner_class=owner_class, profile_id=profile_id,
            authority_epoch=authority_epoch,
        ):
            return CustodyRefusal("stale_terminal_root", owner_class)
        return self._scope_refusal_locked(
            profile_id, authority_epoch, owner_class,
            require_serving=False, allow_retiring=True,
            expected_physical_owner_epoch=physical_owner_epoch,
        )

    def rebind_terminal_root_during_abort(
        self, retired: CustodyHandle, owner_id: str, *, profile_id: str,
        authority_epoch: int, owner_class: str, receipt: DrainReceipt,
        source_proof: TerminalRootRebindProof,
    ):
        """Restore one source-owned root through the exact close and source transition."""
        with self._lock:
            refusal = self._abort_rebind_request_refusal(
                owner_id, owner_class, receipt, source_proof,
            )
            if refusal is not None:
                return refusal
            observed = self._observe_abort_rebind_source_locked(
                retired, profile_id, source_proof,
            )
            if observed is None:
                return CustodyRefusal("owner_proof_mismatch", owner_class)
            binding, canonical = observed
            if not self._abort_rebind_observation_matches(
                retired, profile_id, authority_epoch, source_proof, binding, canonical,
            ):
                return CustodyRefusal("owner_proof_mismatch", owner_class)
            refusal = self._abort_rebind_predecessor_refusal_locked(
                retired, profile_id, authority_epoch, owner_class,
            )
            if refusal is not None:
                return refusal
            lease_id = retired.lease.lease_id
            lease = self._fence.rebind_root_during_close(
                self._close, retired.lease,
                owner_kind=owner_class, owner_id=owner_id, profile_id=profile_id,
                coordinator_id=self._coordinator_id,
                source_transition=self._source_rebind_transition,
            )
            if lease is None:
                return CustodyRefusal("runtime_draining", owner_class)
            handle = CustodyHandle(
                owner_class, owner_id, profile_id, authority_epoch, self.process_instance,
                receipt.close_generation, lease, self._coordinator_id,
                retired.physical_owner_epoch,
            )
            self._records[lease.lease_id] = _Record(handle)
            self._rebound_terminal_roots[lease_id] = retired
            self._rebound_successor_witnesses[lease.lease_id] = (
                retired, binding, source_proof, receipt,
            )
            return handle

    def release_child_reservation(self, child: ChildReservation):
        """Release a child reservation only when the FIFO claim definitely did not commit."""
        with self._lock:
            if (
                not isinstance(child, ChildReservation)
                or child.coordinator_id != self._coordinator_id
                or child.process_instance != self.process_instance
            ):
                return CustodyRefusal("stale_child_reservation")
            scope_refusal = self._scope_refusal_locked(
                child.profile_id, child.authority_epoch, child.owner_class,
                require_serving=False, allow_retiring=True,
                expected_physical_owner_epoch=child.physical_owner_epoch,
            )
            if scope_refusal is not None:
                return scope_refusal
            if not self._fence.release_child_reservation(child.reservation):
                return CustodyRefusal("child_reservation_release_failed", child.owner_class)
            pending = self._pending_children_by_parent.get(child.reservation.parent_lease_id)
            if pending is not None:
                pending.discard(child.reservation.reservation_id)
                if not pending:
                    self._pending_children_by_parent.pop(child.reservation.parent_lease_id, None)
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
            handoff = DrainReceipt(
                self._receipt_id or "",
                self.process_instance,
                self.authority_epoch,
                self._close.close_generation,
                "handoff_pending",
            )
            closed = replace(handoff, state="closed")
            self._state = "handoff_pending"
            self._native_ack = False
            self._native_cleanup_plan = None
            self._native_cleanup_capability = NativeCleanupCapability(
                coordinator=self,
                receipt=handoff,
                closed_receipt=closed,
                close=self._close,
                transition=self._source_rebind_transition,
                process_instance=self.process_instance,
                authority_epoch=self.authority_epoch,
                close_generation=self._close.close_generation,
            )
            self._native_cleanup_state = "not_started"
            self._native_cleanup_reason = None
            self._native_cleanup_in_progress = False
            self._native_cleanup_pending_participants = ()
            return handoff

    def acknowledge_native(
        self, receipt: DrainReceipt, *, native_observation: NativeCleanupPlan | None = None,
    ):
        with self._lock:
            capability = self._native_cleanup_capability
            if capability is None or not isinstance(receipt, DrainReceipt):
                return CustodyRefusal("native_ack_mismatch")
            if self._state == "closed":
                if receipt != capability.closed_receipt:
                    return CustodyRefusal("native_ack_mismatch")
                if native_observation is not None and native_observation is not self._native_cleanup_plan:
                    return CustodyRefusal("native_cleanup_plan_replaced")
                return self._native_result_locked()
            if (
                self._state != "handoff_pending"
                or receipt != capability.receipt
                or not self._native_capability_valid_locked(capability)
            ):
                return CustodyRefusal("native_ack_mismatch")
            if native_observation is not None:
                refusal = self._native_plan_identity_refusal_locked(native_observation)
                if refusal is not None:
                    return refusal
                if self._native_cleanup_plan is not None and self._native_cleanup_plan is not native_observation:
                    return CustodyRefusal("native_cleanup_plan_replaced")
                if self._native_cleanup_plan is None:
                    self._native_cleanup_plan = native_observation
                    self._native_cleanup_capability = replace(capability, plan=native_observation)
                    capability = self._native_cleanup_capability
            self._native_ack = True
            self._state = "closed"
            if self._native_cleanup_plan is None:
                self._native_cleanup_state = "unknown"
                self._native_cleanup_reason = "native_cleanup_missing"
            elif self._native_cleanup_plan.zero_work is not None:
                self._native_cleanup_state = "complete"
                self._native_cleanup_reason = None
            else:
                self._native_cleanup_state = "pending"
                self._native_cleanup_reason = "native_cleanup_pending"
                self._native_cleanup_pending_participants = self._native_cleanup_plan.source_participants
            return self._native_result_locked()

    def abort_before_handoff(
        self, receipt: DrainReceipt, *, return_transition: bool = False,
        source_participants: tuple[tuple[Any, Any, Any], ...] = (),
    ):
        """Reopen the exact pre-handoff owner and optionally issue one post-open token."""
        with self._lock:
            if not self._check_close_identity(receipt) or self._state != "closing":
                return CustodyRefusal("stale_close_receipt")
            transition = object()
            capability = None
            if return_transition:
                capability = PostOpenTransitionCapability(
                    coordinator=self,
                    receipt=receipt,
                    transition=transition,
                    process_instance=self.process_instance,
                    authority_epoch=self.authority_epoch,
                    close_generation=receipt.close_generation,
                    source_participants=tuple(source_participants),
                )
            if not self._fence.release_close(self._close):
                return CustodyRefusal("close_release_failed")
            self._state = "open"
            self._close = None
            self._receipt_id = None
            self._native_ack = False
            self._native_cleanup_plan = None
            self._native_cleanup_capability = None
            self._native_cleanup_state = "not_started"
            self._native_cleanup_reason = None
            self._native_cleanup_in_progress = False
            self._native_cleanup_pending_participants = ()
            self._rebound_successor_witnesses.clear()
            self._owner_state = {owner_class: "unwired" for owner_class in OWNER_CLASSES}
            self._owner_reason.clear()
            self._source_rebind_transition = transition
            if not return_transition:
                return None
            self._post_open_transition = capability
            return capability

    @staticmethod
    def _same_drain_receipt(left: DrainReceipt, right: DrainReceipt) -> bool:
        return (
            isinstance(left, DrainReceipt)
            and isinstance(right, DrainReceipt)
            and left.receipt_id == right.receipt_id
            and left.process_instance == right.process_instance
            and left.authority_epoch == right.authority_epoch
            and left.close_generation == right.close_generation
        )

    def pending_post_open_transition_for(self, receipt: DrainReceipt):
        with self._lock:
            capability = self._post_open_transition
            if capability is None or not self._same_drain_receipt(capability.receipt, receipt):
                return None
            return capability

    def completed_post_open_transition_for(self, receipt: DrainReceipt):
        with self._lock:
            capability = self._completed_post_open_transition
            if capability is None or not self._same_drain_receipt(capability.receipt, receipt):
                return None
            return capability

    def pending_post_open_transition(self):
        with self._lock:
            return self._post_open_transition

    def bind_post_open_participants(self, capability, participants):
        """Bind the exact source participant tuples to the issued transition token."""
        with self._lock:
            if self._post_open_transition is not capability or self._state != "open":
                return None
            bound = replace(capability, source_participants=tuple(participants))
            self._post_open_transition = bound
            return bound

    def check_post_open_transition(self, capability) -> bool:
        with self._lock:
            return (
                isinstance(capability, PostOpenTransitionCapability)
                and capability.coordinator is self
                and self._post_open_transition is capability
                and self._state == "open"
                and self._source_rebind_transition is capability.transition
                and capability.process_instance == self.process_instance
                and capability.authority_epoch == self.authority_epoch
                and capability.close_generation == capability.receipt.close_generation
            )

    def complete_post_open_transition(self, capability):
        with self._lock:
            if self._completed_post_open_transition is capability:
                return None
            if not self.check_post_open_transition(capability):
                return CustodyRefusal("post_open_transition_mismatch")
            self._post_open_transition = None
            self._completed_post_open_transition = capability
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
                "active_child_parents": sorted(key for key, value in self._children_by_parent.items() if value),
                "active_secondary_child_parents": sorted(
                    key for key, value in self._secondary_children_by_parent.items() if value
                ),
                "active_bound_parent_children": sorted(self._bound_parent_by_child),
                "pending_child_parents": sorted(key for key, value in self._pending_children_by_parent.items() if value),
                "native_ack": self._native_ack,
                "native_cleanup": self._native_cleanup_state,
                "native_cleanup_reason": self._native_cleanup_reason,
                "native_cleanup_pending_participants": len(self._native_cleanup_pending_participants),
                "post_open_cleanup_pending": self._post_open_transition is not None,
            }

    def remaining_seconds(self) -> Optional[float]:
        with self._lock:
            if self._close is None:
                return None
            return max(0.0, self._close.deadline - time.monotonic())


def install_runtime_custody(runner) -> RuntimeCustodyCoordinator:
    descriptor = getattr(runner, "session_runtime_descriptor", None) or {}
    registry = getattr(runner, "session_authorities", None)
    coordinator = RuntimeCustodyCoordinator(
        process_instance=descriptor.get("instance_id", ""),
        authority_epoch=descriptor.get("authority_epoch", 0),
        profile_scope=ProfileScopeResolver(runner) if registry is not None else None,
    )
    runner.runtime_custody = coordinator
    authorities = list(registry) if registry is not None else [
        getattr(runner, 'session_authority', None),
    ]
    for authority in authorities:
        if authority is None:
            continue
        bind_graph = getattr(authority, '_register_preservation_producer_graph', None)
        if callable(bind_graph):
            # Bootstrap created and registered the exact empty producer containers.  Bind that
            # witness to this coordinator; a missing or colliding graph must fail closed rather
            # than leaving a source that can later appear quiescent by default.
            bind_graph(coordinator)
        bind_automation_graph = getattr(authority, '_register_automation_source_graph', None)
        if callable(bind_automation_graph):
            # I07 has several real producer families sharing the existing automation indexes.
            # Bind their identity witness before any runtime source can publish a new root.
            bind_automation_graph(coordinator)
        from gateway.session_goal_tick_custody import register_i08_source_graph
        # Bind I08's real watcher/goal/heartbeat indexes before any source task starts.
        register_i08_source_graph(runner, authority, coordinator)
    return coordinator
