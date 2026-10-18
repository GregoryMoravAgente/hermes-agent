"""Profile-bound ownership for a Teams pipeline JSON store.

The pipeline store may be redirected by a supported CLI flag or environment override.  A profile
lock does not cover an arbitrary redirected file, so a gateway-owned store holds a sibling OS file
lock for its lifetime and records the profile/authority identity beside the JSON.  The lease is
specific to this store path; it is not a general runtime broker.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
from typing import Any


class TeamsStoreOwnershipError(RuntimeError):
    """The redirected store is unavailable or is durably bound to another profile."""


def canonical_store_path(path: str | Path) -> Path:
    """Resolve the redirected store once so the data and sibling lease name one inode."""
    return Path(path).expanduser().resolve()


def _profile_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("Teams store ownership requires a profile id")
    if text.startswith(("/", "~")):
        return str(Path(text).expanduser().resolve())
    return text


@dataclass(frozen=True)
class TeamsStoreOwner:
    profile_id: str
    authority_epoch: str
    instance_id: str
    owner_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "profile_id", _profile_id(self.profile_id))
        for name in ("authority_epoch", "instance_id", "owner_id"):
            value = str(getattr(self, name) or "").strip()
            if not value:
                raise ValueError(f"Teams store ownership requires {name}")
            object.__setattr__(self, name, value)

    @classmethod
    def for_authority(cls, authority: Any, *, owner_id: str) -> TeamsStoreOwner:
        return cls(
            profile_id=getattr(authority, "profile_id", None),
            authority_epoch=str(getattr(authority, "epoch", "")),
            instance_id=str(getattr(authority, "instance_id", "")),
            owner_id=owner_id,
        )

    @classmethod
    def for_cli(cls) -> TeamsStoreOwner:
        from hermes_constants import get_hermes_home
        return cls(
            profile_id=str(get_hermes_home()),
            authority_epoch="cli",
            instance_id=f"cli:{os.getpid()}",
            owner_id=f"teams-cli:{os.getpid()}",
        )

    def record(self, path: Path) -> dict[str, str | int]:
        return {
            "schema": 1,
            "store_path": str(path),
            "profile_id": self.profile_id,
            "authority_epoch": self.authority_epoch,
            "instance_id": self.instance_id,
            "owner_id": self.owner_id,
        }


class TeamsStoreOwnership:
    """An exclusive, profile-bound lease for one JSON store path."""

    def __init__(self, path: str | Path, owner: TeamsStoreOwner):
        self.path = canonical_store_path(path)
        self.lock_path = self.path.with_name(f"{self.path.name}.owner")
        self.owner = owner
        self._handle = None
        self._lock_released = False

    @property
    def acquired(self) -> bool:
        return self._handle is not None

    def acquire(self) -> None:
        if self._handle is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        from gateway.runtime_ownership import _open_lock
        from gateway.status import _try_acquire_file_lock

        # Keep the lock record binary on purpose: reads decode with utf-8-sig while writes encode
        # plain UTF-8, so a Windows editor may add a BOM without this owner file emitting one.
        lock_fd = _open_lock(self.lock_path)
        handle = os.fdopen(lock_fd, "r+b", buffering=0)
        locked = False
        try:
            if os.name == "nt":
                handle.seek(0, os.SEEK_END)
                if handle.tell() == 0:
                    handle.write(b"\n")
                    handle.flush()
                handle.seek(0)
            if not _try_acquire_file_lock(handle):
                raise TeamsStoreOwnershipError("teams_pipeline_store_owned")
            locked = True
            if not self._still_names(handle):
                raise TeamsStoreOwnershipError("teams_pipeline_store_owner_path_replaced")
            previous = self._read_record(handle)
            self._validate_previous(previous)
            self._write_record(handle)
            self._lock_released = False
            self._handle = handle
        except BaseException:
            if locked:
                from gateway.status import _release_file_lock
                _release_file_lock(handle)
            handle.close()
            raise

    def _read_record(self, handle) -> dict[str, Any] | None:
        handle.seek(0)
        try:
            raw = handle.read().decode("utf-8-sig")
        except UnicodeError as exc:
            raise TeamsStoreOwnershipError("teams_pipeline_store_owner_invalid") from exc
        if not raw.strip():
            return None
        try:
            value = json.loads(raw)
        except (TypeError, ValueError) as exc:
            raise TeamsStoreOwnershipError("teams_pipeline_store_owner_invalid") from exc
        if not isinstance(value, dict):
            raise TeamsStoreOwnershipError("teams_pipeline_store_owner_invalid")
        return value

    def _validate_previous(self, previous: dict[str, Any] | None) -> None:
        if previous is None:
            return
        if previous.get("schema") != 1:
            raise TeamsStoreOwnershipError("teams_pipeline_store_owner_invalid")
        if str(previous.get("store_path") or "") != str(self.path):
            raise TeamsStoreOwnershipError("teams_pipeline_store_owner_path_mismatch")
        try:
            prior_profile = _profile_id(previous.get("profile_id"))
        except ValueError as exc:
            raise TeamsStoreOwnershipError("teams_pipeline_store_owner_invalid") from exc
        if prior_profile != self.owner.profile_id:
            raise TeamsStoreOwnershipError("teams_pipeline_store_profile_mismatch")

    def _write_record(self, handle) -> None:
        handle.seek(0)
        handle.truncate()
        handle.write(json.dumps(self.owner.record(self.path), sort_keys=True).encode("utf-8"))
        handle.flush()
        os.fsync(handle.fileno())

    def _still_names(self, handle) -> bool:
        from gateway.runtime_ownership import _still_names
        return _still_names(self.lock_path, handle.fileno())

    def _assert_handle_current(self) -> None:
        handle = self._handle
        if handle is None:
            raise TeamsStoreOwnershipError("teams_pipeline_store_owner_not_acquired")
        if self._lock_released:
            raise TeamsStoreOwnershipError("teams_pipeline_store_owner_closing")
        try:
            still_names = self._still_names(handle)
        except (OSError, ValueError):
            still_names = False
        if not still_names:
            raise TeamsStoreOwnershipError("teams_pipeline_store_owner_lost")

    def assert_current(self, *, profile_id: Any, authority_epoch: Any, instance_id: Any, owner_id: Any) -> None:
        expected = TeamsStoreOwner(
            profile_id=profile_id,
            authority_epoch=str(authority_epoch),
            instance_id=str(instance_id),
            owner_id=str(owner_id),
        )
        self._assert_handle_current()
        if expected != self.owner:
            raise TeamsStoreOwnershipError("teams_pipeline_store_owner_mismatch")

    @staticmethod
    def _release_file_lock_checked(handle) -> None:
        """Release the exact handle without the status helper's suppressed OSError."""
        from gateway import status
        if status._IS_WINDOWS:
            handle.seek(status._WINDOWS_LOCK_OFFSET)
            status.msvcrt.locking(handle.fileno(), status.msvcrt.LK_UNLCK, 1)
        else:
            status.fcntl.flock(handle.fileno(), status.fcntl.LOCK_UN)

    def close(self) -> bool:
        handle = self._handle
        if handle is None:
            return True
        try:
            if not self._lock_released:
                self._release_file_lock_checked(handle)
                self._lock_released = True
            handle.close()
        except OSError:
            # Keep the same descriptor and lease state for a later retry. The shared status helper
            # suppresses this class of error, so its None return cannot prove physical release.
            return False
        self._handle = None
        self._lock_released = False
        return True
