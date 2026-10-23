"""Runtime adapter for the stdlib-only profile provenance implementation."""

from __future__ import annotations

from pathlib import Path

from hermes_cli.profile_provenance_bootstrap import (
    PROVENANCE_FILENAME,
    STATE_NEVER_STARTED,
    STATE_STARTED,
    ExclusiveDirectoryBirth,
    ProfileProvenance,
    ProfileProvenanceError,
    directory_binding,
    discard_copied_runtime_origin,
    _create_install_id,
    canonical_install_root,
    provenance_path,
    read_canonical_install_id,
    read_profile_provenance as _read_profile_provenance,
)


def _identity(home: Path, *, create: bool = False) -> str:
    """Return the existing canonical install identity without mutating a read path."""
    value = read_canonical_install_id(home)
    if value is None and create:
        value = _create_install_id(canonical_install_root(home))
    if not isinstance(value, str) or not value:
        raise ProfileProvenanceError("install identity is unavailable")
    return value


def read_profile_provenance(home: Path) -> ProfileProvenance:
    return _read_profile_provenance(home, install_id=read_canonical_install_id(home))


def publish_new_profile_origin(
    home: Path, *, profile_kind: str, profile_name: str | None = None,
    birth: ExclusiveDirectoryBirth | None = None,
    expected_binding: object | None = None,
    publish_target: Path | None = None, retain_birth: bool = False,
) -> ProfileProvenance:
    from hermes_cli.profile_provenance_bootstrap import (
        _validate_directory_birth,
        publish_new_profile_origin as publish,
    )

    if expected_binding is not None:
        raise ProfileProvenanceError("binding tuples and caller booleans are not birth proof")
    _validate_directory_birth(birth, home)

    return publish(
        home,
        profile_kind=profile_kind,
        profile_name=profile_name,
        install_id=_identity(home, create=True),
        birth=birth,
        publish_target=publish_target,
        retain_birth=retain_birth,
    )


def mark_profile_started(
    home: Path, *, owner_epoch: str, record_binding: tuple[object, ...],
) -> ProfileProvenance:
    from hermes_cli.profile_provenance_bootstrap import mark_profile_started as mark

    return mark(
        home,
        owner_epoch=owner_epoch,
        record_binding=record_binding,
        install_id=_identity(home),
    )


__all__ = [
    "PROVENANCE_FILENAME",
    "STATE_NEVER_STARTED",
    "STATE_STARTED",
    "ExclusiveDirectoryBirth",
    "ProfileProvenance",
    "ProfileProvenanceError",
    "directory_binding",
    "discard_copied_runtime_origin",
    "mark_profile_started",
    "provenance_path",
    "publish_new_profile_origin",
    "read_profile_provenance",
]
