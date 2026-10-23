"""Initial profile files, written on the staging tree before publication."""
import contextlib
import os
from pathlib import Path
import shutil
from typing import Optional

from hermes_cli.profile_provenance_bootstrap import ExclusiveDirectoryBirth


def create_profile_staging_dir(profile_dir: Path) -> tuple[Path, ExclusiveDirectoryBirth]:
    """Create the private native directory whose identity anchors a new profile origin."""
    staging = profile_dir.parent / f".{profile_dir.name}.staging-{os.getpid()}"
    profile_dir.parent.mkdir(parents=True, exist_ok=True)
    if staging.is_symlink() or staging.is_file():
        staging.unlink()
    elif staging.is_dir():
        shutil.rmtree(staging, ignore_errors=True)
    from hermes_cli.profile_provenance_bootstrap import create_exclusive_directory_birth

    try:
        birth = create_exclusive_directory_birth(staging)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return staging, birth


def finish_profile_layout(
    profile_dir: Path, *, no_skills: bool, clone_all: bool, description: Optional[str],
    soul: Optional[str] = None, profile_name: Optional[str] = None,
    publish_target: Optional[Path] = None, origin_birth: Optional[ExclusiveDirectoryBirth] = None,
) -> None:
    """Seed files a fresh profile owns from day one; runs on the staging tree before publish."""
    from hermes_cli.profiles import (
        _seed_file_if_missing, _PLACEHOLDER_ENV, NO_BUNDLED_SKILLS_MARKER,
        _migrate_profile_config_if_outdated, write_profile_meta,
    )
    if soul is not None:
        (profile_dir / "SOUL.md").write_text(soul, encoding="utf-8")

    # Seed an empty .env so the profile owns a credentials file from day one. Without it,
    # profile-scoped env writes (dashboard Channels/Keys pages, `hermes -p <name> auth add`)
    # had no file until first write and the profile silently inherited shell API keys —
    # read by users as "the new profile reads the root .env". Skipped when a clone copied one.
    _seed_file_if_missing(profile_dir / ".env", _PLACEHOLDER_ENV, 0o600)

    # Default SOUL.md to customize immediately (skipped when a clone already provided one).
    with contextlib.suppress(Exception):  # best-effort — don't fail profile creation over this
        from hermes_cli.default_soul import DEFAULT_SOUL_MD
        _seed_file_if_missing(profile_dir / "SOUL.md", DEFAULT_SOUL_MD)

    # Opt-out marker read by seed_profile_skills() and `hermes update`'s all-profile sync
    # (the feature still works via the empty skills/ dir if this fails).
    if no_skills:
        _seed_file_if_missing(
            profile_dir / NO_BUNDLED_SKILLS_MARKER,
            "This profile opted out of bundled-skill seeding (`hermes profile create --no-skills`).\n"
            "Delete this file to re-enable sync on the next `hermes update`.\n",
        )

    # Migrate config-only clones now so desktop/status don't warn that a just-created
    # profile is v0/outdated; --clone-all snapshots stay byte-for-byte apart from the
    # explicit runtime/history stripping above.
    if not clone_all:
        _migrate_profile_config_if_outdated(profile_dir)

    # A clone may have carried the source's runtime marker/lock. The staging directory was born
    # exclusively for this profile, so discard those copied bytes only after rechecking its native
    # identity, then issue a fresh origin receipt before the publish rename. A copied source marker
    # can never authenticate a different home.
    if profile_name is not None:
        from hermes_cli.profile_provenance import (
            discard_copied_runtime_origin, publish_new_profile_origin,
        )
        if publish_target is None or origin_birth is None:
            raise RuntimeError("profile origin publication requires private staging evidence")
        discard_copied_runtime_origin(
            profile_dir, birth=origin_birth, publish_target=publish_target,
        )
        publish_new_profile_origin(
            profile_dir, profile_kind="named", profile_name=profile_name,
            birth=origin_birth, publish_target=publish_target, retain_birth=True,
        )

    # Description last, so a partial-create failure doesn't strand a description file.
    if description and description.strip():
        with contextlib.suppress(Exception):  # non-fatal — `hermes profile describe` works later
            write_profile_meta(profile_dir, description=description.strip(), description_auto=False)
