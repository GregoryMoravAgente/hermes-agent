"""Live identity checks for profile-scoped runtime custody.

The preservation coordinator owns one process-wide close receipt, but a multiplexed gateway has
one SessionAuthority and one runtime epoch per reserved profile home.  This module resolves that
live authority and the existing retained profile lock for each operation.  It deliberately keeps
no profile-to-epoch cache and never manufactures a native ownership handle.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ProfileScopeResult:
    """One authenticated live authority/physical-owner observation."""

    ok: bool
    reason: str
    profile_id: str
    authority_epoch: int | None = None
    physical_owner_epoch: str | None = None


class ProfileScopeResolver:
    """Resolve one operation against the runner's current authority registry and lock witness."""

    def __init__(self, runner: Any):
        # Keep the actual registry object installed at bootstrap.  Some supported legacy
        # adapters clear the runner attributes after bootstrap; retaining this object keeps
        # their canonical launch authority visible without copying any authority or epoch.
        self._runner = runner
        self._registry = getattr(runner, "session_authorities", None)
        self._launch_authority = getattr(runner, "session_authority", None)

    def _authority_for_profile(self, home: Path):
        if self._registry is not None:
            return self._registry.for_home(home)
        authority = self._launch_authority
        if authority is not None and self._authority_home(authority) == home:
            return authority
        return None

    @staticmethod
    def _authority_home(authority: Any) -> Path | None:
        db_path = getattr(getattr(authority, "db", None), "db_path", None)
        if db_path is None:
            return None
        try:
            return ProfileScopeResolver._canonical_profile(str(Path(db_path).parent))
        except (OSError, TypeError, ValueError):
            return None

    def _authority_for_id(self, profile_id: Any):
        home = self._canonical_profile(profile_id)
        if home is not None:
            return home, self._authority_for_profile(home)
        if type(profile_id) is not str or not profile_id:
            return None, None
        authorities = self._registry if self._registry is not None else (self._launch_authority,)
        for authority in authorities:
            if authority is not None and getattr(authority, "profile_id", None) == profile_id:
                return self._authority_home(authority), authority
        return None, None

    def _serving_authorities(self):
        if self._registry is not None:
            return self._registry.serving()
        authority = self._launch_authority
        return [authority] if authority is not None and not getattr(authority, "retiring", False) else []

    @staticmethod
    def _canonical_profile(profile_id: Any) -> Path | None:
        if type(profile_id) is not str or not profile_id:
            return None
        home = Path(profile_id).expanduser()
        if not home.is_absolute():
            return None
        from gateway.runtime_ownership import canonical_home

        try:
            return canonical_home(home)
        except (OSError, TypeError, ValueError):
            return None

    @staticmethod
    def _physical_owner(home: Path) -> str | None:
        from gateway.runtime_ownership import process_ownership

        try:
            evidence = process_ownership.reservation_evidence(home)
        except (OSError, TypeError, ValueError):
            return None
        if not isinstance(evidence, dict):
            return None
        owner_epoch = evidence.get("owner_epoch")
        binding = evidence.get("physical_binding")
        descriptor = evidence.get("native_descriptor")
        if (
            evidence.get("profile_home") != str(home)
            or type(owner_epoch) is not str or not owner_epoch
            or evidence.get("native_handle_retained") is not True
            or evidence.get("os_lock_owner") is not True
            or evidence.get("cross_process_proof") is not True
            or evidence.get("observed") is not True
            or binding != (str(home), owner_epoch, descriptor)
            or type(descriptor) is not int or descriptor < 0
        ):
            return None
        return owner_epoch

    def check(
        self, profile_id: Any, authority_epoch: Any, process_instance: Any, *,
        require_serving: bool, allow_retiring: bool,
    ) -> ProfileScopeResult:
        """Check one exact profile tuple against the current registry and retained lock."""
        current_registry = getattr(self._runner, "session_authorities", None)
        if current_registry is not None and current_registry is not self._registry:
            return ProfileScopeResult(False, "profile_unserved", str(profile_id))
        home, authority = self._authority_for_id(profile_id)
        if home is None or type(authority_epoch) is not int or authority_epoch < 1:
            return ProfileScopeResult(False, "invalid_profile_identity", str(profile_id))
        if type(process_instance) is not str or not process_instance:
            return ProfileScopeResult(False, "stale_process_instance", str(home))
        if authority is None:
            return ProfileScopeResult(False, "profile_unserved", str(home))
        authority_profile = getattr(authority, "profile_id", None)
        authority_home = self._canonical_profile(authority_profile)
        if (
            authority_home is not None and authority_home != home
        ) or (
            authority_home is None and authority_profile != profile_id
        ):
            return ProfileScopeResult(False, "profile_identity_mismatch", str(home))
        if getattr(authority, "instance_id", None) != process_instance:
            return ProfileScopeResult(False, "stale_process_instance", str(home))
        if getattr(authority, "epoch", None) != authority_epoch:
            return ProfileScopeResult(False, "stale_authority_epoch", str(home))
        if getattr(authority, "retired", False):
            return ProfileScopeResult(False, "runtime_retired", str(home), authority_epoch)
        if getattr(authority, "retiring", False) and not allow_retiring:
            return ProfileScopeResult(False, "profile_retiring", str(home), authority_epoch)
        if require_serving and not any(candidate is authority for candidate in self._serving_authorities()):
            return ProfileScopeResult(False, "profile_not_serving", str(home), authority_epoch)
        owner_epoch = self._physical_owner(home)
        if owner_epoch is None:
            return ProfileScopeResult(False, "foreign_custody", str(home), authority_epoch)
        return ProfileScopeResult(
            True, "", str(home), authority_epoch, owner_epoch,
        )

    def current_epoch(self, profile_id: Any, *, require_serving: bool) -> int | None:
        """Read the current authority epoch only after the same live identity/lock checks."""
        home, authority = self._authority_for_id(profile_id)
        if home is None:
            return None
        epoch = getattr(authority, "epoch", None) if authority is not None else None
        result = self.check(
            profile_id, epoch, getattr(authority, "instance_id", None),
            require_serving=require_serving, allow_retiring=not require_serving,
        )
        return result.authority_epoch if result.ok else None


def current_profile_epoch(coordinator: Any, profile_id: str, *, require_serving: bool) -> int | None:
    """Read a profile epoch through the coordinator, retaining standalone compatibility."""
    getter = getattr(coordinator, "authority_epoch_for_profile", None)
    if callable(getter):
        return getter(profile_id, require_serving=require_serving)
    return getattr(coordinator, "authority_epoch", None)


def profile_custody_matches(
    coordinator: Any, handle: Any, owner_class: str, owner_id: str,
    profile_id: str, authority_epoch: int,
) -> bool:
    """Require the exact process/profile lease and a current physical scope witness."""
    return (
        hasattr(handle, "lease")
        and getattr(handle, "owner_class", None) == owner_class
        and getattr(handle, "owner_id", None) == owner_id
        and getattr(handle, "profile_id", None) == profile_id
        and getattr(handle, "process_instance", None) == coordinator.process_instance
        and getattr(handle, "authority_epoch", None) == authority_epoch
        and profile_scope_valid_for_handle(coordinator, handle)
    )


def profile_scope_valid_for_handle(coordinator: Any, handle: Any) -> bool:
    """Check one handle against its retained physical lock incarnation."""
    physical_owner_epoch = getattr(handle, "physical_owner_epoch", None)
    return (
        getattr(coordinator, "_profile_scope", None) is None
        or getattr(handle, "profile_id", None) is None
        or type(physical_owner_epoch) is str
    ) and coordinator.profile_scope_valid(
        handle.profile_id, handle.authority_epoch,
        require_serving=False, allow_retiring=True,
        physical_owner_epoch=physical_owner_epoch,
    )
