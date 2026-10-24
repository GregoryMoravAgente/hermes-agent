"""Source-owned I15 registration and zero-effect evidence.

The process registry remains the owner of sessions, readers, queues and native handles.  This
module only binds those existing objects to an authenticated profile admission fence.  It does
not maintain a second session ledger: the exact admitted IDs live in ``ProcessScopeFence`` and
the live inventory is read from ``ProcessRegistry`` at the close fence.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
import threading
import uuid
from typing import Any, Optional

from hermes_cli.profile_provenance_bootstrap import ProfileProvenanceError, _safe_path
from tools.process_registry_custody_evidence import ProcessScopeFence
from tools.process_registry_terminal_history import finished_session_is_terminal_history


_SOURCE = (
    "tools/process_registry.py:1175-1315,1563-1632,1901-1997,2233-2300,2495-2569;"
    "tools/terminal_tool_background.py:107-240,251-306;gateway/run_notifications.py:1677-1715"
)
_OWNER_CLASS = "I15_process_registry_terminal"
_INDEX_NAMES = (
    "_lock",
    "_running",
    "_finished",
    "pending_watchers",
    "completion_queue",
    "_foreground_custody_transactions",
    "_completion_tokens",
)
COMPLETION_OUTPUT_CHARS = 2000


def completion_output(session: Any) -> dict[str, Any]:
    """Return the bounded, ANSI-free completion tail shared by registry result surfaces."""
    from tools.ansi_strip import strip_ansi

    limit = session.completion_output_chars or COMPLETION_OUTPUT_CHARS
    cut = len(session.output_buffer) - limit
    tail = strip_ansi(session.output_buffer[-limit:])
    return {"output": tail, **({"output_cut": cut} if cut > 0 else {})}


@dataclass
class _ProfileRegistration:
    profile_id: str
    authority_epoch: int
    profile_home: Path
    checkpoint_path: Path
    owner_task_id: str
    source_generation: str
    index_identity: tuple[tuple[str, int], ...]
    scope: ProcessScopeFence
    checkpoint_state: str = "unobserved"
    checkpoint_generation: Optional[str] = None
    checkpoint_digest: Optional[str] = None
    checkpoint_entries: tuple[dict[str, Any], ...] = ()
    first_run_owner_epoch: Optional[str] = None
    first_run_generation: Optional[str] = None
    first_run_birth_binding: Optional[tuple[Any, ...]] = None
    first_run_physical_binding: Optional[tuple[Any, ...]] = None
    first_run_record_binding: Optional[tuple[Any, ...]] = None
    first_run_native_descriptor: Optional[int] = None


@dataclass(frozen=True)
class ProcessRegistryZeroObservation:
    """One close-fence classification before external I20/I19 evidence is consumed."""

    kind: str
    reason: str = ""
    close_generation: Optional[int] = None
    registrations: tuple[_ProfileRegistration, ...] = field(default_factory=tuple, repr=False)


class ProcessRegistryZeroFactory:
    """Per-runtime, per-profile I15 admission and close-fence witness."""

    def __init__(self, registry: Any, coordinator: Any, *, runner: Any = None):
        self.registry = registry
        self.coordinator = coordinator
        self.runner = runner
        self.source_generation = uuid.uuid4().hex
        self._lock = threading.RLock()
        self._admission_lock = threading.RLock()
        self._registrations: dict[str, _ProfileRegistration] = {}
        self._closed_generation: Optional[int] = None
        self._observation: Optional[ProcessRegistryZeroObservation] = None

    @staticmethod
    def _index_identity(registry: Any) -> tuple[tuple[str, int], ...] | None:
        try:
            values = tuple((name, getattr(registry, name)) for name in _INDEX_NAMES)
        except (AttributeError, TypeError):
            return None
        lock = values[0][1]
        queue = values[4][1]
        if not all(callable(getattr(lock, name, None)) for name in ("acquire", "release")):
            return None
        if not isinstance(values[1][1], dict) or not isinstance(values[2][1], dict):
            return None
        if not isinstance(values[3][1], list) or queue is None:
            return None
        if not callable(getattr(queue, "get_nowait", None)) or not hasattr(queue, "mutex"):
            return None
        if not isinstance(values[5][1], dict) or not isinstance(values[6][1], dict):
            return None
        return tuple((name, id(value)) for name, value in values)

    def register_profile(
        self, profile_id: str, authority_epoch: int, profile_home: Path, *,
        checkpoint_path: Path | None = None,
    ) -> bool:
        """Register one exact profile before a managed process effect can occur."""
        if not isinstance(profile_id, str) or not profile_id:
            return False
        if type(authority_epoch) is not int or authority_epoch < 1:
            return False
        try:
            home = _safe_path(Path(profile_home), label="process profile home")
            checkpoint = (
                _safe_path(Path(checkpoint_path), label="process checkpoint")
                if checkpoint_path is not None else home / "processes.json"
            )
        except ProfileProvenanceError:
            return False
        identity = self._index_identity(self.registry)
        if identity is None:
            return False
        if not self.coordinator.profile_scope_valid(
            profile_id, authority_epoch, require_serving=True, allow_retiring=False,
        ):
            return False
        with self._admission_lock:
            with self._lock:
                if self._closed_generation is not None:
                    return False
                current = self._registrations.get(profile_id)
                if current is not None:
                    if (
                        current.authority_epoch == authority_epoch
                        and current.profile_home == home
                        and current.index_identity == identity
                    ):
                        return True
                    # A hot-served successor may reuse the profile path only after the prior
                    # source fence has closed with no accepted or pending native effect.  A
                    # live/late old session blocks replacement and keeps the exact old source
                    # registration reachable for reconciliation.
                    if not self._retire_profile_locked(profile_id, current.authority_epoch):
                        return False
                owner_task_id = f"{self.coordinator.process_instance}:I15:{profile_id}"
                self._registrations[profile_id] = _ProfileRegistration(
                    profile_id=profile_id,
                    authority_epoch=authority_epoch,
                    profile_home=home,
                    checkpoint_path=checkpoint,
                    owner_task_id=owner_task_id,
                    source_generation=self.source_generation,
                    index_identity=identity,
                    scope=ProcessScopeFence(
                        self.registry,
                        owner_task_id,
                        self.coordinator.process_instance,
                        authority_epoch,
                        profile_id,
                    ),
                )
                self._capture_first_run_witness(self._registrations[profile_id])
                return True

    def _capture_first_run_witness(self, registration: _ProfileRegistration) -> bool:
        """Capture an authenticated first-run witness before this profile can create effects.

        A missing checkpoint is not itself a first-run fact.  The only accepted cold path is a
        real profile owner whose lock has no predecessor record, whose current registry indexes
        are empty, and whose native descriptor/lock binding is observable through I20.  The
        retained owner epoch, lock-record binding, and descriptor are the durable source context;
        this method never creates a checkpoint or invents a checkpoint generation.  Existing
        profiles with a prior gateway owner, malformed checkpoint, or any pre-existing registry
        object never receive this witness and remain UNKNOWN at a childless close.
        """
        if registration.checkpoint_path.exists():
            return False
        inventory, reason = self._inventory()
        if reason is not None or any(inventory):
            return False
        try:
            from gateway.runtime_ownership import process_ownership
            lineage = process_ownership.reservation_lineage(registration.profile_home)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return False
        if lineage is None or not lineage.is_authenticated_new_profile:
            return False
        physical = self._physical_owner_evidence(registration)
        if physical is None:
            return False
        owner_epoch = physical.get("owner_epoch")
        binding = physical.get("physical_binding")
        record_binding = physical.get("record_binding")
        descriptor = physical.get("native_descriptor")
        if (
            type(owner_epoch) is not str or not owner_epoch
            or not isinstance(lineage.generation, str) or not lineage.generation
            or not isinstance(lineage.birth_binding, tuple) or len(lineage.birth_binding) != 3
            or not isinstance(binding, tuple) or len(binding) != 3
            or not isinstance(record_binding, tuple) or len(record_binding) != 5
            or type(descriptor) is not int or descriptor < 0
        ):
            return False
        registration.first_run_owner_epoch = owner_epoch
        registration.first_run_generation = lineage.generation
        registration.first_run_birth_binding = lineage.birth_binding
        registration.first_run_physical_binding = binding
        registration.first_run_record_binding = record_binding
        registration.first_run_native_descriptor = descriptor
        return True

    def _retire_profile_locked(self, profile_id: str, authority_epoch: int) -> bool:
        registration = self._registrations.get(profile_id)
        if registration is None or registration.authority_epoch != authority_epoch:
            return False
        receipt = registration.scope.close()
        if (
            receipt.get("pending_reservation_ids")
            or receipt.get("late_creation") is True
        ):
            return False
        if receipt.get("created_session_ids") and not registration.scope.terminal_history_retirement():
            return False
        self._registrations.pop(profile_id, None)
        return True

    def retire_profile(self, profile_id: str, authority_epoch: int) -> bool:
        """Remove one profile only after its exact source fence proves no pending native work."""
        with self._admission_lock:
            with self._lock:
                return self._retire_profile_locked(profile_id, authority_epoch)

    def ensure_legacy_profile(self, profile_id: str, authority_epoch: int) -> bool:
        """Support bare coordinator fixtures only through an explicit exact registration.

        A production coordinator always has a profile resolver and therefore refuses a missing
        profile.  Older process-registry callers bind a coordinator without that resolver; they
        may change the test home after binding, so register that new exact home rather than
        silently treating an unregistered source as admitted.
        """
        if getattr(self.coordinator, "_profile_scope", None) is not None:
            return False
        if not isinstance(profile_id, str) or type(authority_epoch) is not int:
            return False
        with self._lock:
            current = self._registrations.get(profile_id)
            if current is not None:
                return (
                    current.authority_epoch == authority_epoch
                    and current.index_identity == self._index_identity(self.registry)
                )
        return self.register_profile(profile_id, authority_epoch, Path(profile_id))

    def registrations(self) -> tuple[_ProfileRegistration, ...]:
        with self._lock:
            return tuple(self._registrations.values())

    def profile_registered(self, profile_id: str) -> bool:
        with self._lock:
            return isinstance(profile_id, str) and profile_id in self._registrations

    def _registration_for(self, profile_id: str | None) -> _ProfileRegistration | None:
        if profile_id is None:
            from hermes_constants import get_hermes_home
            try:
                profile_id = str(_safe_path(get_hermes_home(), label="process profile home"))
            except ProfileProvenanceError:
                return None
        return self._registrations.get(profile_id)

    def _session_profile(self, session: Any) -> str | None:
        profile_id = getattr(session, "profile_id", None)
        if isinstance(profile_id, str) and profile_id:
            return profile_id
        custody = getattr(session, "custody", None)
        profile_id = getattr(custody, "profile_id", None)
        return profile_id if isinstance(profile_id, str) and profile_id else None

    def admit_for_effect(self, session: Any) -> bool:
        """Hold the source admission lock across the next real spawn effect."""
        self._admission_lock.acquire()
        with self._lock:
            if self._closed_generation is not None:
                self._admission_lock.release()
                return False
            registration = self._registration_for(self._session_profile(session))
            if registration is None:
                self._admission_lock.release()
                return False
            if registration.scope.contains_session(session.id):
                return True
            if registration.scope.record_session(session.id, registration.owner_task_id):
                return True
        self._admission_lock.release()
        return False

    def finish_admission(self, session: Any, *, effect: bool) -> None:
        """Release the effect gate, removing only a proven pre-effect failure."""
        try:
            if not effect:
                with self._lock:
                    registration = self._registration_for(self._session_profile(session))
                    if registration is not None:
                        registration.scope.release_session(session.id, registration.owner_task_id)
        finally:
            self._admission_lock.release()

    def observe_existing(self, session: Any) -> bool:
        """Bind an already-born foreground child to the exact source fence."""
        with self._admission_lock:
            with self._lock:
                if self._closed_generation is not None:
                    return False
                registration = self._registration_for(self._session_profile(session))
                if registration is None:
                    return False
                if registration.scope.contains_session(session.id):
                    return True
                return registration.scope.record_session(session.id, registration.owner_task_id)

    def transfer_foreground(self, custody: Any, session: Any) -> bool:
        """Replace a pre-effect foreground reservation with its exact live session ID."""
        profile_id = getattr(custody, "profile_id", None)
        reservation_id = getattr(custody, "owner_id", None)
        session_id = getattr(session, "id", None)
        with self._admission_lock:
            with self._lock:
                registration = self._registration_for(profile_id)
                if registration is None or self._closed_generation is not None:
                    return False
                if not isinstance(reservation_id, str) or not isinstance(session_id, str):
                    return False
                return registration.scope.transfer_foreground(
                    reservation_id, session_id, registration.owner_task_id,
                )

    def has_foreground_reservation(self, custody: Any) -> bool:
        profile_id = getattr(custody, "profile_id", None)
        reservation_id = getattr(custody, "owner_id", None)
        with self._lock:
            registration = self._registration_for(profile_id)
            return bool(
                registration is not None
                and isinstance(reservation_id, str)
                and registration.scope.contains_foreground(reservation_id)
            )

    def release_unaccepted(self, session: Any) -> bool:
        """Release a source admission only when the native custody already proved no effect."""
        with self._admission_lock:
            with self._lock:
                registration = self._registration_for(self._session_profile(session))
                if registration is None:
                    return False
                if self._closed_generation is not None:
                    return False
                if not registration.scope.contains_session(session.id):
                    return True
                return registration.scope.release_session(session.id, registration.owner_task_id)

    def reserve_foreground(self, custody: Any) -> bool:
        """Record the exact foreground owner before a caller-owned native launch."""
        profile_id = getattr(custody, "profile_id", None)
        owner_id = getattr(custody, "owner_id", None)
        if not isinstance(owner_id, str) or not owner_id:
            return False
        with self._admission_lock:
            with self._lock:
                if self._closed_generation is not None:
                    return False
                registration = self._registration_for(profile_id)
                if registration is None:
                    epoch = getattr(custody, "authority_epoch", None)
                    if self.ensure_legacy_profile(profile_id, epoch):
                        registration = self._registration_for(profile_id)
                if registration is None:
                    return False
                if registration.scope.contains_session(owner_id) or registration.scope.contains_foreground(owner_id):
                    return True
                return registration.scope.reserve_foreground(owner_id, registration.owner_task_id)

    def release_foreground(self, custody: Any) -> bool:
        """Release a foreground reservation only after the native launcher proved no effect."""
        profile_id = getattr(custody, "profile_id", None)
        owner_id = getattr(custody, "owner_id", None)
        with self._admission_lock:
            with self._lock:
                registration = self._registration_for(profile_id)
                if registration is None or self._closed_generation is not None:
                    return False
                if registration.scope.contains_foreground(owner_id):
                    return registration.scope.release_foreground(owner_id)
                if not registration.scope.contains_session(owner_id):
                    return True
                return registration.scope.release_session(owner_id, registration.owner_task_id)

    def note_checkpoint_recovery(self, profile_id: str | None = None) -> bool:
        """Record the authenticated recovery observation for one served profile.

        A missing file is accepted only when the registration already captured an authenticated
        owner-incarnation witness before any process effect.  It is never inferred from
        ``recover_from_checkpoint() == 0`` or given a synthetic checkpoint generation.
        """
        if profile_id is None:
            from hermes_constants import get_hermes_home
            try:
                profile_id = str(_safe_path(get_hermes_home(), label="process profile home"))
            except ProfileProvenanceError:
                return False
        with self._lock:
            registration = self._registrations.get(profile_id)
            if registration is None:
                return False
            path = registration.checkpoint_path
            try:
                raw = path.read_bytes()
            except FileNotFoundError:
                if registration.first_run_owner_epoch is None:
                    registration.checkpoint_state = "unknown"
                    return False
                registration.checkpoint_state = "first_run_initialized"
                registration.checkpoint_generation = None
                registration.checkpoint_digest = None
                registration.checkpoint_entries = ()
                return True
            except (OSError, RuntimeError):
                registration.checkpoint_state = "unknown"
                return False
            try:
                entries = json.loads(raw.decode("utf-8-sig"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                registration.checkpoint_state = "unknown"
                return False
            if not isinstance(entries, list) or not all(isinstance(item, dict) for item in entries):
                registration.checkpoint_state = "unknown"
                return False
            digest = hashlib.sha256(raw).hexdigest()
            registration.checkpoint_digest = digest
            registration.checkpoint_generation = f"checkpoint:{digest}"
            registration.checkpoint_entries = tuple(dict(item) for item in entries)
            registration.checkpoint_state = "valid_nonempty" if entries else "valid_empty"
            return True

    def _checkpoint_unchanged(self, registration: _ProfileRegistration) -> bool:
        if registration.checkpoint_state == "first_run_initialized":
            if registration.checkpoint_path.exists():
                return False
            try:
                from gateway.runtime_ownership import canonical_home, process_ownership
                physical = process_ownership.reservation_evidence(registration.profile_home)
                lineage = process_ownership.reservation_lineage(registration.profile_home)
                expected_home = canonical_home(registration.profile_home)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                return False
            return (
                isinstance(physical, dict)
                and lineage is not None
                and lineage.is_authenticated_new_profile
                and lineage.generation == registration.first_run_generation
                and lineage.birth_binding == registration.first_run_birth_binding
                and physical.get("profile_home") == str(expected_home)
                and physical.get("owner_epoch") == registration.first_run_owner_epoch
                and physical.get("physical_binding") == registration.first_run_physical_binding
                and physical.get("record_binding") == registration.first_run_record_binding
                and physical.get("native_descriptor") == registration.first_run_native_descriptor
                and physical.get("native_handle_retained") is True
            )
        if registration.checkpoint_state != "valid_nonempty":
            return False
        try:
            raw = registration.checkpoint_path.read_bytes()
        except (FileNotFoundError, OSError):
            return False
        return (
            registration.checkpoint_digest == hashlib.sha256(raw).hexdigest()
            and registration.checkpoint_generation == f"checkpoint:{registration.checkpoint_digest}"
        )

    def _registration_identity_unchanged(self, registration: _ProfileRegistration) -> bool:
        return registration.index_identity == self._index_identity(self.registry)

    def _queue_is_empty(self) -> bool:
        queue = getattr(self.registry, "completion_queue", None)
        mutex = getattr(queue, "mutex", None)
        entries = getattr(queue, "queue", None)
        if mutex is None or entries is None:
            return False
        try:
            with mutex:
                return not entries
        except (AttributeError, RuntimeError, TypeError):
            return False

    def _inventory(
        self, registrations: tuple[_ProfileRegistration, ...] = (),
    ) -> tuple[tuple[Any, ...], str | None]:
        try:
            with self.registry._lock:
                running = tuple(self.registry._running.values())
                finished = tuple(self.registry._finished.values())
                registered_epochs = frozenset(
                    (registration.profile_id, registration.authority_epoch)
                    for registration in registrations
                )
                terminal_history_ids = frozenset(
                    session.id
                    for session in finished
                    if finished_session_is_terminal_history(
                        session,
                        completion_tokens=self.registry._completion_tokens,
                        pending_watchers=self.registry.pending_watchers,
                        # A terminal session from a retired profile epoch is history once the
                        # successor registration has replaced that profile's source fence.  A
                        # terminal session from a currently registered epoch remains current
                        # custody and must continue to block a zero-work claim.
                        live_coordinator=(
                            self.coordinator
                            if (
                                getattr(session, "profile_id", None),
                                getattr(session, "authority_epoch", None),
                            ) in registered_epochs
                            else None
                        ),
                    )
                )
                foreground = tuple(self.registry._foreground_custody_transactions.items())
                unbound = tuple(self.registry._custody_unbound_sessions)
                watchers = tuple(self.registry.pending_watchers)
                tokens = tuple(self.registry._completion_tokens)
        except (AttributeError, RuntimeError, TypeError):
            return (), "registry_inventory_unavailable"
        return (running, finished, foreground, unbound, watchers, tokens, terminal_history_ids), None

    def observe_close(self, close_generation: int) -> ProcessRegistryZeroObservation:
        """Fence admissions and classify zero, nonempty, or unknown conservatively."""
        with self._admission_lock:
            with self._lock:
                if self._observation is not None:
                    return self._observation
                if type(close_generation) is not int or close_generation < 1:
                    result = ProcessRegistryZeroObservation("unknown", "owner_epoch_mismatch")
                    self._observation = result
                    return result
                registrations = tuple(self._registrations.values())
                if not registrations:
                    result = ProcessRegistryZeroObservation("unknown", "source_registration_missing")
                    self._observation = result
                    return result
                for registration in registrations:
                    registration.scope.close()
                self._closed_generation = close_generation
                for registration in registrations:
                    if (
                        not self.coordinator.profile_scope_valid(
                            registration.profile_id,
                            registration.authority_epoch,
                            require_serving=False,
                            allow_retiring=True,
                        )
                        or not self._registration_identity_unchanged(registration)
                    ):
                        result = ProcessRegistryZeroObservation(
                            "unknown", "checkpoint_or_registration_unknown", close_generation,
                            registrations,
                        )
                        self._observation = result
                        return result
                inventory, reason = self._inventory(registrations)
                if reason is not None:
                    result = ProcessRegistryZeroObservation("unknown", reason, close_generation, registrations)
                    self._observation = result
                    return result
                (
                    running, finished, foreground, unbound, watchers, tokens,
                    terminal_history_ids,
                ) = inventory
                live_finished = tuple(
                    item for item in finished
                    if getattr(item, "id", None) not in terminal_history_ids
                )
                registered_profiles = {item.profile_id for item in registrations}
                if any(
                    getattr(item, "profile_id", None) not in registered_profiles
                    for item in (*running, *live_finished)
                ) or any(
                    getattr(getattr(transaction, "custody", None), "profile_id", None)
                    not in registered_profiles
                    for _owner_id, transaction in foreground
                ):
                    result = ProcessRegistryZeroObservation(
                        "unknown", "foreign_profile_inventory", close_generation, registrations,
                    )
                    self._observation = result
                    return result
                created = {
                    session_id
                    for registration in registrations
                    for session_id in registration.scope.close().get("created_session_ids", ())
                }
                live_created = created - terminal_history_ids
                known = {getattr(item, "id", None) for item in (*running, *live_finished)}
                known.update(owner_id for owner_id, _transaction in foreground)
                if any(item not in known for item in live_created):
                    result = ProcessRegistryZeroObservation(
                        "unknown", "accepted_inventory_conservation_failed", close_generation,
                        registrations,
                    )
                    self._observation = result
                    return result
                if any(item not in live_created for item in known):
                    result = ProcessRegistryZeroObservation(
                        "unknown", "unregistered_process_inventory", close_generation,
                        registrations,
                    )
                    self._observation = result
                    return result
                if any(
                    registration.scope.close().get("late_creation") is True
                    for registration in registrations
                ):
                    result = ProcessRegistryZeroObservation(
                        "unknown", "late_creation", close_generation, registrations,
                    )
                    self._observation = result
                    return result
                if any(
                    registration.scope.close().get("pending_reservation_ids")
                    for registration in registrations
                ):
                    result = ProcessRegistryZeroObservation(
                        "unknown", "pending_native_reservation", close_generation, registrations,
                    )
                    self._observation = result
                    return result
                if unbound or watchers or tokens or not self._queue_is_empty():
                    result = ProcessRegistryZeroObservation(
                        "unknown", "pending_process_delivery", close_generation, registrations,
                    )
                    self._observation = result
                    return result
                if running or live_finished or foreground or live_created:
                    result = ProcessRegistryZeroObservation(
                        "nonempty", "accepted_process_effect", close_generation, registrations,
                    )
                    self._observation = result
                    return result
                if any(
                    registration.checkpoint_state != "first_run_initialized"
                    or not self._checkpoint_unchanged(registration)
                    for registration in registrations
                ):
                    result = ProcessRegistryZeroObservation(
                        "unknown", "checkpoint_or_registration_unknown", close_generation, registrations,
                    )
                    self._observation = result
                    return result
                result = ProcessRegistryZeroObservation("zero", "", close_generation, registrations)
                self._observation = result
                return result

    @staticmethod
    def _physical_owner_evidence(registration: _ProfileRegistration) -> dict[str, Any] | None:
        try:
            from gateway.runtime_ownership import canonical_home, process_ownership
            evidence = process_ownership.reservation_evidence(registration.profile_home)
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            return None
        if not isinstance(evidence, dict):
            return None
        try:
            expected_path = canonical_home(registration.profile_home)
            observed_path = canonical_home(Path(evidence["profile_home"]))
            expected = str(expected_path)
            binding = evidence["physical_binding"]
            descriptor = evidence["native_descriptor"]
        except (KeyError, OSError, RuntimeError, TypeError, ValueError):
            return None
        if (
            observed_path != expected_path
            or evidence.get("native_handle_retained") is not True
            or evidence.get("os_lock_owner") is not True
            or evidence.get("cross_process_proof") is not True
            or evidence.get("observed") is not True
            or not isinstance(binding, tuple) or len(binding) != 3
            or binding[0] != expected or binding[1] != evidence.get("owner_epoch")
            or type(descriptor) is not int or descriptor < 0
        ):
            return None
        return evidence

    def _lifecycle_evidence(self, close_generation: int) -> dict[str, Any] | None:
        if self.runner is None:
            return None
        try:
            from gateway.lifecycle_custody import preservation_owner_evidence
            evidence = preservation_owner_evidence(
                self.runner,
                process_instance=self.coordinator.process_instance,
                authority_epoch=self.coordinator.authority_epoch,
                close_generation=close_generation,
                owner_id=f"{self.coordinator.process_instance}:I19_gateway_lifecycle",
            )
        except (AttributeError, ImportError, OSError, RuntimeError, TypeError, ValueError):
            return None
        if not isinstance(evidence, dict) or evidence.get("observed") is not True:
            return None
        if evidence.get("no_work") is not True or evidence.get("cross_process_proof") is not True:
            return None
        return evidence

    def zero_owner_evidence(
        self, *, process_instance: str, authority_epoch: int, close_generation: int,
        owner_id: str,
    ) -> dict[str, Any] | None:
        observation = self.observe_close(close_generation)
        if observation.kind != "zero" or observation.close_generation != close_generation:
            return None
        if (
            process_instance != self.coordinator.process_instance
            or authority_epoch != self.coordinator.authority_epoch
            or owner_id != f"{process_instance}:{_OWNER_CLASS}"
        ):
            return None
        physical = tuple(
            self._physical_owner_evidence(registration)
            for registration in observation.registrations
        )
        if not physical or any(item is None for item in physical):
            return None
        lifecycle = self._lifecycle_evidence(close_generation)
        if lifecycle is None:
            return None
        return {
            "source": _SOURCE,
            "process_instance": process_instance,
            "authority_epoch": authority_epoch,
            "close_generation": close_generation,
            "owner_id": owner_id,
            "profile_ids": tuple(item.profile_id for item in observation.registrations),
            "profile_epochs": tuple(
                (item.profile_id, item.authority_epoch) for item in observation.registrations
            ),
            "profile_homes": tuple(str(item.profile_home) for item in observation.registrations),
            "source_generation": self.source_generation,
            "checkpoint_generations": tuple(
                item.checkpoint_generation for item in observation.registrations
            ),
            "initialization_owner_epochs": tuple(
                item.first_run_owner_epoch for item in observation.registrations
            ),
            "initialization_generations": tuple(
                item.first_run_generation for item in observation.registrations
            ),
            "initialization_birth_bindings": tuple(
                item.first_run_birth_binding for item in observation.registrations
            ),
            "initialization_record_bindings": tuple(
                item.first_run_record_binding for item in observation.registrations
            ),
            "initialization_physical_bindings": tuple(
                item.first_run_physical_binding for item in observation.registrations
            ),
            "initialization_native_descriptors": tuple(
                item.first_run_native_descriptor for item in observation.registrations
            ),
            "registry_index_identity": tuple(
                item.index_identity for item in observation.registrations
            ),
            "accepted_count": 0,
            "pending_ids": (),
            "unknown_ids": (),
            "physical_owner_evidence": physical,
            "lifecycle_evidence": lifecycle,
            "native_handle_retained": True,
            "cross_process_proof": True,
            "observed": True,
        }


class ProcessRegistryZeroFactoryMixin:
    """Small facade hooks mixed into ProcessRegistry without another ownership ledger."""

    def _completion_queue_empty_for_rebind_locked(self) -> bool:
        queue = getattr(self, "completion_queue", None)
        mutex = getattr(queue, "mutex", None)
        entries = getattr(queue, "queue", None)
        if mutex is None or entries is None:
            return False
        try:
            with mutex:
                return not entries
        except (AttributeError, RuntimeError, TypeError):
            return False

    def _finished_history_rebind_safe_locked(self) -> bool:
        """Allow a coordinator replacement only after every retained result is terminal.

        The caller already holds the registry lock.  Finished output remains in place for
        ``list``/``read_log``; this predicate only separates settled history from live custody.
        """
        return all(
            finished_session_is_terminal_history(
                session,
                completion_tokens=self._completion_tokens,
                pending_watchers=self.pending_watchers,
            )
            for session in self._finished.values()
        )

    def reserve_foreground_custody(
        self, *, profile_id: Optional[str] = None,
        binding: Any = None,
        parent_handle: Any = None,
        scope_fence: ProcessScopeFence | None = None,
        owner_task_id: Optional[str] = None,
    ):
        """Reserve a foreground lease before the caller's native launcher can create a child."""
        from tools.process_registry_custody import ProcessCustody

        coordinator = self._runtime_custody
        if coordinator is None:
            return None
        custody = ProcessCustody.reserve(
            coordinator,
            f"{coordinator.process_instance}:process:foreground:{uuid.uuid4().hex}",
            profile_id or self._process_profile_id(),
            parent_handle=parent_handle,
            scope_fence=scope_fence,
        )
        custody.foreground_binding = binding
        factory = getattr(self, "_i15_zero_factory", None)
        if factory is not None:
            custody._i15_zero_factory = factory
            if not factory.reserve_foreground(custody):
                custody.release_unaccepted()
                return custody
        if scope_fence is not None:
            if not scope_fence.reserve_foreground(custody.owner_id, owner_task_id or ""):
                custody.mark_unknown("process_scope_unavailable")
            else:
                custody.foreground_reserved = True
        return custody

    def bind_process_registry_zero_factory(
        self, coordinator: Any, *, runner: Any = None,
        profiles: tuple[tuple[str, int, Path], ...] | None = None,
    ) -> ProcessRegistryZeroFactory:
        factory = getattr(self, "_i15_zero_factory", None)
        if factory is None:
            factory = ProcessRegistryZeroFactory(self, coordinator, runner=runner)
            self._i15_zero_factory = factory
        elif factory.coordinator is not coordinator:
            with self._lock:
                if (
                    self._running
                    or not self._finished_history_rebind_safe_locked()
                    or self._foreground_custody_transactions
                    or self._custody_unbound_sessions
                    or self.pending_watchers
                    or self._completion_tokens
                    or not self._completion_queue_empty_for_rebind_locked()
                ):
                    raise RuntimeError("I15 process factory coordinator changed with live custody")
            factory = ProcessRegistryZeroFactory(self, coordinator, runner=runner)
            self._i15_zero_factory = factory
        if runner is not None:
            factory.runner = runner
        if profiles is None:
            profile_id = self._process_profile_id()
            epoch = coordinator.authority_epoch_for_profile(profile_id, require_serving=True)
            if type(epoch) is int and epoch > 0:
                profiles = ((profile_id, epoch, Path(profile_id)),)
        for profile_id, epoch, home in profiles or ():
            if not factory.register_profile(profile_id, epoch, home):
                raise RuntimeError("I15 process factory profile registration refused")
        return factory

    def _i15_before_process_effect(self, session: Any) -> None:
        if getattr(self, "_runtime_custody", None) is None:
            return
        factory = getattr(self, "_i15_zero_factory", None)
        profile_id = getattr(session, "profile_id", None)
        if factory is not None and isinstance(profile_id, str) and profile_id:
            epoch = getattr(session, "authority_epoch", None)
            if not factory.profile_registered(profile_id):
                factory.ensure_legacy_profile(profile_id, epoch)
        if factory is None or not factory.admit_for_effect(session):
            try:
                self._custody_release_unaccepted(session)
            except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
                if getattr(session, "custody", None) is not None:
                    session.custody.mark_unknown("source_registration_missing")
            raise RuntimeError("I15 process source admission refused")
        session._i15_admission_held = True

    def _i15_after_process_effect(self, session: Any, *, effect: bool) -> None:
        factory = getattr(self, "_i15_zero_factory", None)
        if factory is None or getattr(session, "_i15_admission_held", False) is not True:
            return
        try:
            factory.finish_admission(session, effect=effect)
        finally:
            session._i15_admission_held = False

    def _i15_observe_existing_process(self, session: Any) -> bool:
        factory = getattr(self, "_i15_zero_factory", None)
        if getattr(self, "_runtime_custody", None) is None:
            return True
        if factory is None:
            if getattr(session, "custody", None) is not None:
                session.custody.mark_unknown("source_registration_missing")
            return False
        custody = getattr(session, "custody", None)
        # A live foreground child must have the source reservation created before its native
        # effect.  Generic observe_existing() would turn an omitted/late adoption into accepted
        # source evidence after the fact, so keep that exact child reachable as UNKNOWN instead.
        if custody is None or not factory.has_foreground_reservation(custody):
            if getattr(session, "custody", None) is not None:
                session.custody.mark_unknown("source_registration_missing")
            return False
        accepted = factory.transfer_foreground(custody, session)
        if not accepted and getattr(session, "custody", None) is not None:
            session.custody.mark_unknown("source_registration_missing")
        return accepted

    def _i15_release_unaccepted(self, session: Any) -> bool:
        factory = getattr(self, "_i15_zero_factory", None)
        return factory is None or factory.release_unaccepted(session)

    def _i15_reserve_foreground(self, custody: Any) -> bool:
        factory = getattr(self, "_i15_zero_factory", None)
        return factory is None or factory.reserve_foreground(custody)


def register_process_registry_profiles(registry: Any, coordinator: Any, runner: Any) -> bool:
    """Register every currently served authority at the authenticated runtime boundary."""
    from gateway.session_authorities import all_authorities

    profiles = []
    for authority in all_authorities(runner):
        if authority is None or getattr(authority, "retiring", False):
            continue
        profile_id = getattr(authority, "profile_id", None)
        epoch = getattr(authority, "epoch", None)
        if not isinstance(profile_id, str) or type(epoch) is not int or epoch < 1:
            return False
        try:
            canonical_path = _safe_path(Path(profile_id), label="process profile home")
        except ProfileProvenanceError:
            return False
        canonical = str(canonical_path)
        profiles.append((canonical, epoch, Path(canonical)))
    registry.bind_process_registry_zero_factory(
        coordinator, runner=runner, profiles=tuple(profiles),
    )
    return bool(profiles)


def process_registry_owner_evidence(
    registry: Any, runner: Any, *, process_instance: str, authority_epoch: int,
    close_generation: int, owner_id: str, launch_profile_id: str,
) -> tuple[dict[str, Any] | None, str | None]:
    """Select the strict zero path or the unchanged nonempty-session evidence path."""
    factory = getattr(registry, "_i15_zero_factory", None)
    if not isinstance(factory, ProcessRegistryZeroFactory):
        return None, "source_registration_missing"
    observation = factory.observe_close(close_generation)
    if observation.kind == "unknown":
        return None, observation.reason or "owner_observation_missing"
    if observation.kind == "zero":
        evidence = factory.zero_owner_evidence(
            process_instance=process_instance,
            authority_epoch=authority_epoch,
            close_generation=close_generation,
            owner_id=owner_id,
        )
        return (evidence, None) if evidence is not None else (None, "physical_exclusion_unproven")
    from tools.process_registry_custody_evidence import preservation_owner_evidence_for_profiles

    evidence = preservation_owner_evidence_for_profiles(
        registry,
        process_instance=process_instance,
        authority_epoch=authority_epoch,
        close_generation=close_generation,
        owner_id=owner_id,
        launch_profile_id=launch_profile_id,
    )
    return (evidence, None) if evidence is not None else (None, "terminal_evidence_missing")

