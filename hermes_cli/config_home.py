"""Directory initialization and storage diagnostics for the active Hermes home."""

import os
from pathlib import Path
from hermes_dir_acl import make_user_dir
from hermes_cli.profile_provenance_bootstrap import (
    ExclusiveDirectoryBirth,
    ProfileProvenanceError,
    create_exclusive_directory_birth,
)


class HomeInitializationError(RuntimeError):
    """The home skeleton is unavailable, not an invalid YAML document."""


def _directory_links(path: Path) -> list[Path]:
    return [part for part in (*reversed(path.parents), path) if part.is_symlink()]


def _operator_owned_links(links: list[Path], home: Path) -> list[Path]:
    """Keep only the links that make the operator the owner of the home's permissions.

    That boundary is the home itself and anything under it. A link *above* the home
    (macOS ``/tmp`` -> ``/private/tmp``, ``/var`` -> ``/private/var``, or any aliased parent
    the user happens to live in) says nothing about who owns :data:`home`, and treating it as
    an operator-owned link left a fresh home and its ``cron``/``sessions``/``logs``/``memories``
    subdirectories at the default ``0o755`` instead of ``0o700``.
    """
    return [link for link in links if link == home or home in link.parents]


def _ensure_directory(
    path: Path, *, create: bool, secure: bool, home: Path, exclusive_create: bool = False,
) -> ExclusiveDirectoryBirth | None:
    from hermes_cli.config import _secure_dir

    detail = ""
    birth: ExclusiveDirectoryBirth | None = None
    try:
        links = _directory_links(path)
        detail = "; ".join(f"{link} -> {link.readlink()}" for link in links)
        # Never materialize a missing mount's target on the underlying disk.
        for link in links:
            if not link.is_dir():
                raise FileNotFoundError(f"Directory link is unavailable: {link}")
        if create:
            if exclusive_create and not os.path.lexists(path):
                # The root birth is the provenance boundary. Create its parent normally, then
                # create the leaf exclusively so a concurrent initializer cannot be mistaken for
                # this process's clean first run.
                try:
                    birth = create_exclusive_directory_birth(path)
                except FileExistsError:
                    # Another initializer won the birth race. Continue normal initialization, but
                    # the provenance consumer remains UNKNOWN unless it published the marker.
                    make_user_dir(path, parents=False, exist_ok=True)
            else:
                make_user_dir(path, parents=True, exist_ok=True)
        elif not path.is_dir():
            raise FileNotFoundError(f"Required directory does not exist: {path}")
        # The operator owns permissions beyond a link, including logs/curator.
        if secure and not _operator_owned_links(links, home):
            _secure_dir(path)
    except (OSError, ProfileProvenanceError) as exc:
        raise HomeInitializationError(
            f"Cannot initialize Hermes directory {path}: {exc}. "
            + (f"Directory links: {detail}. " if detail else "")
            + "Check the directory/link target, mount availability and access permissions; "
            "restore the mount or repair the link before retrying. "
            "Hermes has not replaced the link or created its missing target."
        ) from exc
    return birth


def initialize_home(home: Path, subdirs: tuple[str, ...], ensured: set[str]) -> None:
    from hermes_cli.config import _ensure_default_soul_md, is_managed

    managed = is_managed()
    old_umask = os.umask(0o007) if managed else None
    try:
        home_birth = _ensure_directory(
            home, create=not managed, secure=not managed, home=home, exclusive_create=True,
        )
        if home_birth is not None:
            try:
                from hermes_cli.profile_provenance import publish_new_profile_origin
                publish_new_profile_origin(home, profile_kind="default", birth=home_birth)
            except Exception as exc:
                raise HomeInitializationError(
                    f"Cannot publish Hermes home creation provenance {home}: {exc}. "
                    "The new home was not authenticated; repair storage and retry."
                ) from exc
        required = ("cron", "sessions", "logs", "memories") if managed else subdirs
        for subdir in required:
            _ensure_directory(home / subdir, create=not managed, secure=not managed, home=home)
        if managed:
            _ensure_directory(home / "logs" / "curator", create=True, secure=False, home=home)
        try:
            _ensure_default_soul_md(home)
        except OSError as exc:
            raise HomeInitializationError(
                f"Cannot initialize Hermes home {home}: {exc}. "
                "Check storage availability and access permissions."
            ) from exc
    finally:
        if old_umask is not None:
            os.umask(old_umask)
    # Plugin discovery rebinds the resolved home. Do not repeat chmod through
    # that spelling after the operator-owned symlink boundary has been lost.
    ensured.update((str(home), str(home.resolve())))


def config_load_issue(exc: Exception):
    from hermes_cli.config import ConfigIssue

    if isinstance(exc, (HomeInitializationError, OSError)):
        return ConfigIssue(
            "error", f"Hermes storage is unavailable: {exc}",
            "Check the reported path, link target, mount and permissions; keep config.yaml unchanged.",
        )
    return ConfigIssue("error", "Could not load config.yaml", "Run 'hermes setup' to create a valid config")
