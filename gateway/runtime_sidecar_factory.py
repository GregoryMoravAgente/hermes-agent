"""Profile-scoped MCP remote source factory.

The factory is a capability boundary, not an ownership ledger.  The serving
``SessionAuthority`` retains the exact ``ExternalProcessParent`` objects and
the runtime coordinator retains their I16 roots.  This object only closes MCP
ingress for a generation and constructs the typed remote receipt before the
SDK is allowed to connect.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
import uuid

from gateway.runtime_sidecar_remote import RemoteSidecarOwner, RemoteSourceError, RemoteSourceReceipt


@dataclass(frozen=True)
class FactoryCloseReceipt:
    profile_id: str
    process_instance: str
    authority_epoch: int
    close_generation: int
    outcome: Literal["active", "no_route", "unknown"]
    candidate_count: int
    pending_count: int
    physical_owner_epoch: str


@dataclass(frozen=True)
class FactoryAbortCapability:
    """Opaque, source-issued permission to publish one prepared MCP abort.

    The capability is deliberately bound to the factory object, the exact close receipt and
    coordinator receipt, and the factory's private finish token.  A copied wire receipt or a
    factory from another profile cannot satisfy the identity checks in the source methods below.
    """

    factory: Any = field(repr=False, compare=False)
    receipt: FactoryCloseReceipt = field(repr=False, compare=False)
    coordinator: Any = field(repr=False, compare=False)
    coordinator_receipt: Any = field(repr=False, compare=False)
    token: object = field(repr=False, compare=False)
    profile_id: str
    process_instance: str
    authority_epoch: int
    physical_owner_epoch: str


class MCPRemoteSourceFactory:
    """One authenticated MCP source factory attached to one live authority."""

    __slots__ = (
        "_close_generation", "_close_receipt", "_config_seen", "_configured_enabled",
        "_finish_backup", "_finish_capability", "_finish_committed",
        "_finish_completed_capability", "_finish_completed_coordinator",
        "_finish_completed_transition", "_finish_coordinator",
        "_finish_coordinator_receipt", "_finish_finalized", "_finish_token", "_ingress",
        "_pending_names",
        "authority", "family", "runner",
    )

    def __init__(self, runner: Any, authority: Any):
        if authority is None or getattr(authority, "runner", None) is not runner:
            raise RemoteSourceError("mcp_factory_authority_mismatch")
        profile_id = getattr(authority, "profile_id", None)
        instance_id = getattr(authority, "instance_id", None)
        epoch = getattr(authority, "epoch", None)
        if type(profile_id) is not str or not profile_id:
            raise RemoteSourceError("mcp_factory_profile_missing")
        if type(instance_id) is not str or not instance_id or type(epoch) is not int or epoch < 1:
            raise RemoteSourceError("mcp_factory_epoch_missing")
        self.runner = runner
        self.authority = authority
        self.family = "mcp_remote"
        self._ingress = "open"
        self._close_generation: int | None = None
        self._close_receipt: FactoryCloseReceipt | None = None
        self._finish_token: object | None = None
        self._finish_coordinator: Any | None = None
        self._finish_coordinator_receipt: Any | None = None
        self._finish_capability: FactoryAbortCapability | None = None
        self._finish_backup: tuple[Any, ...] | None = None
        self._finish_committed = False
        self._finish_finalized = False
        self._finish_completed_capability: FactoryAbortCapability | None = None
        self._finish_completed_transition: object | None = None
        self._finish_completed_coordinator: Any | None = None
        self._config_seen = False
        self._configured_enabled: frozenset[str] = frozenset()
        self._pending_names: frozenset[str] = frozenset()

    @property
    def ingress(self) -> str:
        return self._ingress

    @property
    def close_generation(self) -> int | None:
        return self._close_generation

    @property
    def close_receipt(self) -> FactoryCloseReceipt | None:
        """Return the one source-owned close witness, including during an abort retry."""
        return self._close_receipt

    def observe_config(self, servers: Any, *, pending: Any = ()) -> None:
        """Record the canonical candidate set without copying credentials or URLs."""
        if self._ingress != "open":
            raise RemoteSourceError("mcp_ingress_closed")
        if not isinstance(servers, dict):
            raise RemoteSourceError("mcp_config_unreadable")
        try:
            from tools.mcp_tool_common import mcp_server_enabled
            enabled = {
                str(name) for name, config in servers.items()
                if isinstance(name, str) and isinstance(config, dict)
                and "url" in config and mcp_server_enabled(config)
            }
        except (ImportError, TypeError, ValueError):
            raise RemoteSourceError("mcp_config_unreadable")
        pending_names = frozenset(
            str(name) for name in pending
            if isinstance(name, str) and str(name) in enabled
        )
        self._config_seen = True
        self._configured_enabled = frozenset(enabled)
        self._pending_names = pending_names

    def _current_identity(self) -> tuple[str, str, int, str]:
        identity = self._close_identity()
        if getattr(self.authority, "retiring", False):
            raise RemoteSourceError("mcp_factory_retiring")
        return identity

    def _close_identity(self) -> tuple[str, str, int, str]:
        """Read the same physical owner while a profile is entering retirement."""
        profile_id = getattr(self.authority, "profile_id", None)
        process_instance = getattr(self.authority, "instance_id", None)
        epoch = getattr(self.authority, "epoch", None)
        if type(profile_id) is not str or type(process_instance) is not str or type(epoch) is not int:
            raise RemoteSourceError("mcp_factory_epoch_missing")
        if getattr(self.authority, "retired", False):
            raise RemoteSourceError("mcp_factory_retiring")
        from gateway.session_authorities import authority_for_home
        if authority_for_home(self.runner, Path(profile_id)) is not self.authority:
            raise RemoteSourceError("mcp_factory_stale")
        try:
            from gateway.runtime_ownership import process_ownership
            physical = process_ownership.owner_epoch(Path(profile_id))
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            physical = None
        if type(physical) is not str or not physical:
            raise RemoteSourceError("physical_owner_unknown")
        return profile_id, process_instance, epoch, physical

    def reserve(self, server_name: str, config: dict[str, Any]) -> RemoteSidecarOwner:
        """Reserve the existing profile/authority root before SDK transport ingress."""
        if self._ingress != "open":
            raise RemoteSourceError("mcp_ingress_closed")
        if type(server_name) is not str or not server_name:
            raise RemoteSourceError("source_identity_missing")
        if not isinstance(config, dict) or "url" not in config:
            raise RemoteSourceError("mcp_remote_config_missing")
        if self._config_seen and server_name not in self._configured_enabled:
            raise RemoteSourceError("mcp_route_not_configured")
        coordinator = getattr(self.authority, "_preservation_coordinator", lambda: None)()
        coordinator_lock = getattr(coordinator, "_lock", None)
        if coordinator is None or coordinator_lock is None:
            raise RemoteSourceError("mcp_coordinator_unavailable")
        from gateway.runtime_service_operations import (
            ExternalProcessOperationError,
            resolve_external_process_parent,
            reserve_request,
        )

        with coordinator_lock:
            profile_id, _process_instance, _epoch, _physical = self._current_identity()
            source_identity = f"mcp:{server_name}"
            for retained in self.authority.external_process_finalizers():
                if getattr(retained, "source_identity", None) != source_identity:
                    continue
                if (
                    getattr(retained, "profile_authority", None) is not self.authority
                    or getattr(retained, "profile_id", None) != profile_id
                    or getattr(retained, "process_instance", None) != self.authority.instance_id
                    or getattr(retained, "authority_epoch", None) != self.authority.epoch
                    or getattr(retained, "physical_owner_epoch", None) != _physical
                ):
                    raise RemoteSourceError("mcp_route_owner_stale")
                receipt = getattr(retained, "_remote_receipt", None)
                if receipt is None or getattr(retained, "state", None) != "terminal":
                    raise RemoteSourceError("mcp_route_owner_retained")

            operation_id = f"mcp-remote:{server_name}:{uuid.uuid4().hex}"
            parent = None
            try:
                parent = resolve_external_process_parent(
                    self.runner,
                    profile_home=Path(profile_id),
                    operation_id=operation_id,
                    source_identity=source_identity,
                    mode="live_profile_discovery", producer_key="mcp",
                )
                reserve_request(parent)
                generation = self._coordinator_generation(parent.coordinator)
                receipt = RemoteSourceReceipt(
                    parent=parent,
                    family=self.family,
                    source_identity=source_identity,
                    operation_id=operation_id,
                    close_generation=generation,
                )
                parent._remote_receipt = receipt
                parent.finalizer = receipt
                return RemoteSidecarOwner(
                    family=self.family,
                    source_identity=source_identity,
                    parent=parent,
                    service=parent._service,
                    request=parent.root_handle,
                    receipt=receipt,
                )
            except RemoteSourceError:
                raise
            except (ExternalProcessOperationError, RuntimeError, TypeError, ValueError) as exc:
                if parent is not None:
                    from gateway.runtime_service_operations import mark_unknown
                    mark_unknown(parent, getattr(exc, "code", "mcp_remote_reservation_failed"))
                raise RemoteSourceError(getattr(exc, "code", "mcp_remote_reservation_failed")) from exc

    @staticmethod
    def _coordinator_generation(coordinator: Any) -> int:
        try:
            generation = coordinator.status().get("close_generation")
        except (AttributeError, RuntimeError, TypeError, ValueError):
            generation = None
        return generation if type(generation) is int and generation >= 0 else 0

    def _validate_close_receipt(self, receipt: FactoryCloseReceipt) -> None:
        """Validate the retained source witness at the physical ownership boundary."""
        if receipt is not self._close_receipt:
            raise RemoteSourceError("mcp_close_receipt_mismatch")
        if self._close_generation != receipt.close_generation:
            raise RemoteSourceError("mcp_close_generation_mismatch")
        profile_id, process_instance, epoch, physical = self._close_identity()
        if (
            receipt.profile_id != profile_id
            or receipt.process_instance != process_instance
            or receipt.authority_epoch != epoch
            or receipt.physical_owner_epoch != physical
        ):
            raise RemoteSourceError("mcp_close_receipt_stale")

    def _require_close_receipt(self, receipt: FactoryCloseReceipt) -> FactoryCloseReceipt:
        if not isinstance(receipt, FactoryCloseReceipt):
            raise RemoteSourceError("mcp_close_receipt_invalid")
        self._validate_close_receipt(receipt)
        return receipt

    def _require_retained_close_receipt(self, receipt: FactoryCloseReceipt) -> FactoryCloseReceipt:
        """Require the exact retained witness without rereading a possibly changed owner epoch."""
        if not isinstance(receipt, FactoryCloseReceipt):
            raise RemoteSourceError("mcp_close_receipt_invalid")
        if receipt is not self._close_receipt:
            raise RemoteSourceError("mcp_close_receipt_mismatch")
        if self._close_generation != receipt.close_generation:
            raise RemoteSourceError("mcp_close_generation_mismatch")
        return receipt

    def close_ingress(self, close_generation: int) -> FactoryCloseReceipt:
        """Stop new MCP admissions for one canonical runtime close generation."""
        if type(close_generation) is not int or close_generation < 0:
            raise RemoteSourceError("mcp_close_generation_invalid")
        if self._ingress == "closed":
            receipt = self._close_receipt
            if receipt is None or receipt.close_generation != close_generation:
                raise RemoteSourceError("mcp_close_generation_mismatch")
            self._validate_close_receipt(receipt)
            return receipt
        if self._ingress == "open_committed":
            raise RemoteSourceError("mcp_source_finalization_pending")
        if self._ingress == "open_pending":
            raise RemoteSourceError("mcp_close_resume_pending")
        profile_id, process_instance, epoch, physical = self._close_identity()
        self._finish_completed_capability = None
        self._finish_completed_transition = None
        self._finish_completed_coordinator = None
        parents = getattr(self.authority, "external_process_finalizers", lambda: ())()
        remote_states = tuple(
            getattr(getattr(parent, "_remote_receipt", None), "state", None)
            for parent in parents
        )
        active = tuple(
            parent for parent in parents
            if getattr(getattr(parent, "_remote_receipt", None), "state", None)
            not in {None, "closed", "terminal"}
        )
        for parent in active:
            remote = getattr(parent, "_remote_receipt", None)
            if remote is not None:
                remote.close_generation = close_generation
        if "unknown" in remote_states:
            outcome: Literal["active", "no_route", "unknown"] = "unknown"
        elif active:
            outcome = "active"
        elif not self._config_seen or self._pending_names:
            outcome = "unknown"
        elif self._configured_enabled:
            # A configured route without an exact accepted parent may be lazy, failed, or
            # foreign.  It is never authenticated no-route merely because a server map is empty.
            outcome = "unknown"
        else:
            outcome = "no_route"
        receipt = FactoryCloseReceipt(
            profile_id=profile_id,
            process_instance=process_instance,
            authority_epoch=epoch,
            close_generation=close_generation,
            outcome=outcome,
            candidate_count=len(self._configured_enabled),
            pending_count=len(self._pending_names),
            physical_owner_epoch=physical,
        )
        self._ingress = "closed"
        self._close_generation = close_generation
        self._close_receipt = receipt
        return receipt

    def abort_ingress(self, receipt: FactoryCloseReceipt) -> bool:
        """Move one exact close into a pending-open state without releasing its witness."""
        self._require_close_receipt(receipt)
        if self._ingress == "open_pending":
            return True
        if self._ingress != "closed":
            raise RemoteSourceError("mcp_close_state_mismatch")
        self._ingress = "open_pending"
        return True

    def reclose_ingress(self, receipt: FactoryCloseReceipt) -> bool:
        """Re-fence a source that was resumed while a sibling source still failed."""
        # Re-closing is the fail-closed direction.  It must preserve the exact source witness even
        # when the physical owner epoch became unreadable during the failed resume/finalization.
        self._require_retained_close_receipt(receipt)
        if self._ingress == "closed":
            return True
        if self._ingress != "open_pending":
            raise RemoteSourceError("mcp_close_state_mismatch")
        self._ingress = "closed"
        self._finish_token = None
        self._finish_coordinator = None
        self._finish_coordinator_receipt = None
        self._finish_capability = None
        self._finish_backup = None
        self._finish_committed = False
        self._finish_finalized = False
        self._finish_completed_capability = None
        self._finish_completed_transition = None
        self._finish_completed_coordinator = None
        return True

    def prepare_finish_abort(
        self, receipt: FactoryCloseReceipt, *, coordinator: Any, coordinator_receipt: Any,
    ) -> bool:
        """Authenticate the final source release while the canonical coordinator is CLOSING."""
        self._require_close_receipt(receipt)
        if self._ingress != "open_pending":
            raise RemoteSourceError("mcp_close_state_mismatch")
        if coordinator is not getattr(self.runner, "runtime_custody", None):
            raise RemoteSourceError("mcp_coordinator_mismatch")
        if getattr(coordinator, "state", None) != "closing":
            raise RemoteSourceError("mcp_coordinator_phase_mismatch")
        check_identity = getattr(coordinator, "_check_close_identity", None)
        if not callable(check_identity) or not check_identity(coordinator_receipt):
            raise RemoteSourceError("mcp_coordinator_receipt_mismatch")
        if self._finish_token is not None:
            raise RemoteSourceError("mcp_source_finalization_pending")
        self._finish_token = object()
        self._finish_coordinator = coordinator
        self._finish_coordinator_receipt = coordinator_receipt
        self._finish_capability = FactoryAbortCapability(
            factory=self,
            receipt=receipt,
            coordinator=coordinator,
            coordinator_receipt=coordinator_receipt,
            token=self._finish_token,
            profile_id=receipt.profile_id,
            process_instance=receipt.process_instance,
            authority_epoch=receipt.authority_epoch,
            physical_owner_epoch=receipt.physical_owner_epoch,
        )
        return True

    def prepared_finish_capability(
        self, receipt: FactoryCloseReceipt, *, coordinator: Any, coordinator_receipt: Any,
    ) -> FactoryAbortCapability:
        """Return the exact source-issued capability retained by ``prepare_finish_abort``."""
        capability = self._finish_capability
        if not isinstance(capability, FactoryAbortCapability):
            raise RemoteSourceError("mcp_source_finalization_unprepared")
        if (
            capability.factory is not self
            or capability.receipt is not receipt
            or capability.coordinator is not coordinator
            or capability.coordinator_receipt is not coordinator_receipt
            or capability.token is not self._finish_token
            or self._finish_coordinator is not coordinator
            or self._finish_coordinator_receipt is not coordinator_receipt
        ):
            raise RemoteSourceError("mcp_source_finalization_capability_mismatch")
        self._require_close_receipt(receipt)
        if self._ingress != "open_pending":
            raise RemoteSourceError("mcp_close_state_mismatch")
        if getattr(coordinator, "state", None) != "closing":
            raise RemoteSourceError("mcp_coordinator_phase_mismatch")
        check_identity = getattr(coordinator, "_check_close_identity", None)
        if not callable(check_identity) or not check_identity(coordinator_receipt):
            raise RemoteSourceError("mcp_coordinator_receipt_mismatch")
        return capability

    def commit_finish_abort(self, capability: FactoryAbortCapability) -> bool:
        """Publish this factory's staged open while the coordinator remains CLOSING.

        The exact pre-commit state remains in ``_finish_backup`` until the coordinator has
        published its own open transition.  This makes a sibling failure rollbackable without
        dropping the source receipt or manufacturing a new factory identity.
        """
        if (
            not isinstance(capability, FactoryAbortCapability)
            or capability.factory is not self
            or self._finish_capability is not capability
        ):
            raise RemoteSourceError("mcp_source_finalization_capability_mismatch")
        if (
            capability.receipt is not self._close_receipt
            or capability.token is not self._finish_token
            or capability.coordinator is not self._finish_coordinator
            or capability.coordinator_receipt is not self._finish_coordinator_receipt
            or self._ingress != "open_pending"
            or getattr(capability.coordinator, "state", None) != "closing"
            or not callable(getattr(capability.coordinator, "_check_close_identity", None))
            or not capability.coordinator._check_close_identity(
                capability.coordinator_receipt
            )
        ):
            raise RemoteSourceError("mcp_source_finalization_capability_mismatch")
        if self._finish_committed:
            raise RemoteSourceError("mcp_source_finalization_pending")
        self._finish_backup = (
            self._ingress,
            self._close_generation,
            self._close_receipt,
            self._finish_token,
            self._finish_coordinator,
            self._finish_coordinator_receipt,
            self._finish_capability,
            self._finish_committed,
            self._finish_finalized,
        )
        self._finish_committed = True
        self._finish_finalized = False
        # The source phase is private while the canonical coordinator remains CLOSING.  Keep the
        # exact close witness and coordinator links until the post-open transition capability is
        # completed; ordinary reserve() must not observe this as public ingress.
        self._ingress = "open_committed"
        return True

    def rollback_finish_abort(self, capability: FactoryAbortCapability) -> bool:
        """Restore the exact pre-commit witness after a sibling publication failure."""
        if (
            not isinstance(capability, FactoryAbortCapability)
            or capability.factory is not self
            or self._finish_capability is not capability
        ):
            raise RemoteSourceError("mcp_source_finalization_capability_mismatch")
        backup = self._finish_backup
        if backup is None:
            # A before-effect failure is still a successful rollback: the source remained in its
            # prepared state and retained the same close receipt.
            if (
                capability.receipt is self._close_receipt
                and self._ingress == "open_pending"
                and self._finish_token is capability.token
                and self._finish_coordinator is capability.coordinator
                and getattr(capability.coordinator, "state", None) == "closing"
                and callable(getattr(capability.coordinator, "_check_close_identity", None))
                and capability.coordinator._check_close_identity(
                    capability.coordinator_receipt
                )
            ):
                return True
            raise RemoteSourceError("mcp_source_finalization_rollback_missing")
        (
            ingress, close_generation, close_receipt, token, finish_coordinator,
            finish_coordinator_receipt, finish_capability, finish_committed,
            finish_finalized,
        ) = backup
        if (
            finish_coordinator is None
            or getattr(finish_coordinator, "state", None) != "closing"
            or not callable(getattr(finish_coordinator, "_check_close_identity", None))
            or not finish_coordinator._check_close_identity(finish_coordinator_receipt)
        ):
            raise RemoteSourceError("mcp_coordinator_phase_mismatch")
        self._ingress = ingress
        self._close_generation = close_generation
        self._close_receipt = close_receipt
        self._finish_token = token
        self._finish_coordinator = finish_coordinator
        self._finish_coordinator_receipt = finish_coordinator_receipt
        self._finish_capability = finish_capability
        self._finish_committed = finish_committed
        self._finish_finalized = finish_finalized
        self._finish_backup = None
        return True

    def finalize_finish_abort(self, capability: FactoryAbortCapability) -> bool:
        """Complete source-side validation before the coordinator fence can open."""
        if (
            not isinstance(capability, FactoryAbortCapability)
            or capability.factory is not self
            or self._finish_capability is not capability
        ):
            raise RemoteSourceError("mcp_source_finalization_capability_mismatch")
        if (
            self._ingress != "open_committed"
            or not self._finish_committed
            or self._finish_capability is not capability
            or self._finish_backup is None
        ):
            raise RemoteSourceError("mcp_source_finalization_unprepared")
        coordinator = self._finish_coordinator
        if (
            coordinator is None
            or getattr(coordinator, "state", None) != "closing"
            or not callable(getattr(coordinator, "_check_close_identity", None))
            or not coordinator._check_close_identity(self._finish_coordinator_receipt)
        ):
            raise RemoteSourceError("mcp_coordinator_phase_mismatch")
        self._finish_finalized = True
        return True

    def pending_finish_capability(self, coordinator: Any, transition: object):
        """Return the exact source capability retained for one post-open retry."""
        if (
            self._finish_capability is not None
            and self._finish_coordinator is coordinator
            and self._finish_committed
            and self._finish_capability.token is self._finish_token
        ):
            return self._finish_capability
        if (
            self._finish_completed_capability is not None
            and self._finish_completed_coordinator is coordinator
            and self._finish_completed_transition is transition
        ):
            return self._finish_completed_capability
        return None

    def complete_finish_abort(
        self, capability: FactoryAbortCapability, *, transition: object | None = None,
    ) -> bool:
        """Discard the private rollback snapshot after the exact coordinator transition."""
        if (
            self._finish_completed_capability is capability
            and self._finish_completed_transition is transition
            and self._finish_completed_coordinator is capability.coordinator
        ):
            # Completion after OPEN is intentionally idempotent for the same opaque source token;
            # a copied capability or a new transition never reaches this branch.
            return True
        if (
            not isinstance(capability, FactoryAbortCapability)
            or capability.factory is not self
            or self._finish_capability is not capability
        ):
            raise RemoteSourceError("mcp_source_finalization_capability_mismatch")
        if self._ingress != "open_committed" or not self._finish_committed:
            raise RemoteSourceError("mcp_source_finalization_unprepared")
        if not self._finish_finalized:
            raise RemoteSourceError("mcp_source_finalization_unprepared")
        coordinator = self._finish_coordinator
        if (
            coordinator is None
            or getattr(coordinator, "state", None) != "open"
            or not callable(getattr(coordinator, "check_post_open_transition", None))
            or not coordinator.check_post_open_transition(transition)
        ):
            raise RemoteSourceError("mcp_post_open_transition_mismatch")
        self._finish_completed_capability = capability
        self._finish_completed_transition = transition
        self._finish_completed_coordinator = coordinator
        self._ingress = "open"
        self._close_generation = None
        self._close_receipt = None
        self._finish_token = None
        self._finish_coordinator = None
        self._finish_coordinator_receipt = None
        self._finish_backup = None
        self._finish_capability = None
        self._finish_committed = False
        self._finish_finalized = False
        return True

    def finish_abort(
        self, receipt: FactoryCloseReceipt, *, coordinator: Any, coordinator_receipt: Any,
    ) -> bool:
        """Forget the bounded close witness only after the coordinator has reopened."""
        self._require_retained_close_receipt(receipt)
        if self._ingress != "open_pending":
            raise RemoteSourceError("mcp_close_state_mismatch")
        if (
            self._finish_token is None
            or coordinator is not self._finish_coordinator
            or coordinator_receipt is not self._finish_coordinator_receipt
        ):
            raise RemoteSourceError("mcp_source_finalization_unprepared")
        if getattr(coordinator, "state", None) != "open":
            raise RemoteSourceError("mcp_coordinator_phase_mismatch")
        self._ingress = "open"
        self._close_generation = None
        self._close_receipt = None
        self._finish_token = None
        self._finish_coordinator = None
        self._finish_coordinator_receipt = None
        self._finish_capability = None
        self._finish_backup = None
        self._finish_committed = False
        self._finish_finalized = False
        return True


def register_mcp_source_factory(runner: Any, authority: Any) -> MCPRemoteSourceFactory:
    """Install the factory on the authority before discovery can enter MCP."""
    current = getattr(authority, "_mcp_source_factory", None)
    if current is not None:
        if not isinstance(current, MCPRemoteSourceFactory) or current.authority is not authority:
            raise RemoteSourceError("mcp_factory_registry_collision")
        return current
    coordinator = getattr(runner, "runtime_custody", None)
    pending = getattr(coordinator, "pending_post_open_transition", None)
    if (
        coordinator is not None
        and getattr(coordinator, "state", None) == "open"
        and callable(pending)
        and pending() is not None
    ):
        raise RemoteSourceError("mcp_source_finalization_pending")
    factory = MCPRemoteSourceFactory(runner, authority)
    try:
        register = authority.register_mcp_source_factory
    except AttributeError:
        authority._mcp_source_factory = factory
    else:
        try:
            register(factory)
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            raise RemoteSourceError(getattr(exc, "code", "mcp_factory_registry_collision")) from exc
    return factory


def factory_for_authority(authority: Any) -> MCPRemoteSourceFactory | None:
    factory = getattr(authority, "_mcp_source_factory", None)
    if factory is None:
        return None
    if not isinstance(factory, MCPRemoteSourceFactory) or factory.authority is not authority:
        raise RemoteSourceError("mcp_factory_registry_mismatch")
    return factory


def factory_for_runner(runner: Any, profile_home: Path) -> MCPRemoteSourceFactory | None:
    from gateway.session_authorities import authority_for_home
    authority = authority_for_home(runner, Path(profile_home))
    return None if authority is None else factory_for_authority(authority)


def observe_mcp_config(
    runner: Any, profile_home: Path | None, servers: dict[str, dict], *, pending: Any = (),
) -> None:
    """Feed the canonical config candidate set to the selected profile factory."""
    if profile_home is None:
        from gateway.session_authorities import active_authority
        authority = active_authority(runner)
        factory = None if authority is None else factory_for_authority(authority)
    else:
        from gateway.session_authorities import authority_for_home
        authority = authority_for_home(runner, Path(profile_home))
        factory = factory_for_runner(runner, Path(profile_home))
    if factory is None:
        # Legacy/direct stdio registration remains compatible when no served
        # authority is present.  A URL route, however, must fail closed rather
        # than entering the SDK without a profile factory.
        if authority is None and getattr(runner, "session_authorities", None) is None:
            return
        if any(isinstance(config, dict) and "url" in config for config in servers.values()):
            raise RemoteSourceError("mcp_factory_unregistered")
        return
    factory.observe_config(servers, pending=pending)


def finish_remote_owner(owner: Any, *, task: Any = None) -> bool:
    """Close/read back an exact remote owner, then consume its existing parent root."""
    receipt = getattr(owner, "receipt", None)
    parent = getattr(owner, "parent", None)
    if not isinstance(receipt, RemoteSourceReceipt) or parent is None:
        return False
    if not receipt.close_after_shutdown(task):
        return False
    from gateway.runtime_service_operations import close_after_finalizers
    if close_after_finalizers(parent) is not True:
        receipt.mark_unknown("remote_parent_retire_failed")
        return False
    return receipt.readback() is not None
