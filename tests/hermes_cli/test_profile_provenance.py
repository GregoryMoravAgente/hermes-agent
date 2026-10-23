"""Creation provenance for default homes and named profile publication."""

import copy
import os
import shutil
from pathlib import Path
import subprocess
import sys
import tempfile

import pytest

from gateway.runtime_ownership import process_ownership
from gateway.runtime_ownership import OwnershipConflict
from hermes_cli.config_home import initialize_home
from hermes_cli.profile_provenance import (
    STATE_NEVER_STARTED,
    STATE_STARTED,
    directory_binding,
    publish_new_profile_origin,
    read_profile_provenance,
)
from hermes_cli.profile_provenance_bootstrap import (
    ExclusiveDirectoryBirth,
    ProfileProvenanceError,
    bootstrap_profile_home,
    create_exclusive_directory_birth,
    read_existing_install_id,
    read_profile_provenance as read_bootstrap_provenance,
)
from hermes_cli.profiles import create_profile, rename_profile


@pytest.fixture()
def isolated_home(tmp_path, monkeypatch):
    root = (tmp_path / ".hermes").resolve()
    monkeypatch.setenv("HERMES_HOME", str(root))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    return root


def test_default_clean_home_publishes_authenticated_origin_before_gateway_start(
    isolated_home,
):
    ensured: set[str] = set()
    initialize_home(
        isolated_home,
        ("cron", "sessions", "logs", "logs/curator", "memories", "pairing", "hooks"),
        ensured,
    )
    origin = read_profile_provenance(isolated_home)
    assert origin.status == "valid"
    assert origin.profile_kind == "default"
    assert origin.state == STATE_NEVER_STARTED
    generation = origin.generation

    initialize_home(
        isolated_home,
        ("cron", "sessions", "logs", "logs/curator", "memories", "pairing", "hooks"),
        ensured,
    )
    assert read_profile_provenance(isolated_home).generation == generation

    process_ownership.reserve([isolated_home], purpose="i15-provenance-test")
    try:
        lineage = process_ownership.reservation_lineage(isolated_home)
        assert lineage is not None
        assert lineage.status == "new_profile_started"
        assert lineage.generation == generation
        assert read_profile_provenance(isolated_home).state == STATE_STARTED
    finally:
        assert process_ownership.release(isolated_home) is True


def test_named_profile_public_creation_issues_fresh_origin(isolated_home):
    profile = create_profile("alice", no_alias=True, no_skills=True)
    origin = read_profile_provenance(profile)
    assert origin.status == "valid"
    assert origin.profile_kind == "named"
    assert origin.profile_name == "alice"
    assert origin.state == STATE_NEVER_STARTED

    process_ownership.reserve([profile], purpose="i15-provenance-test")
    try:
        lineage = process_ownership.reservation_lineage(profile)
        assert lineage is not None and lineage.is_authenticated_new_profile
        assert read_profile_provenance(profile).state == STATE_STARTED
    finally:
        assert process_ownership.release(profile) is True


def test_cloning_never_reuses_source_origin_or_gateway_lock(isolated_home):
    source = create_profile("source", no_alias=True, no_skills=True)
    source_origin = read_profile_provenance(source)
    (source / "gateway.lock").write_text("copied runtime", encoding="utf-8")
    clone = create_profile("clone", clone_all=True, no_alias=True)
    clone_origin = read_profile_provenance(clone)

    assert clone_origin.status == "valid"
    assert clone_origin.generation != source_origin.generation
    assert clone_origin.state == STATE_NEVER_STARTED
    assert not (clone / "gateway.lock").exists()


def test_markerless_existing_home_and_copied_marker_are_unknown(tmp_path, monkeypatch):
    home = (tmp_path / "existing").resolve()
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    (home / "config.yaml").write_text("model: {}\n", encoding="utf-8")
    result = read_profile_provenance(home)
    assert result.status == "missing"
    assert result.reason == "origin_missing"

    source = (tmp_path / "source").resolve()
    monkeypatch.setenv("HERMES_HOME", str(source))
    bootstrap_profile_home(source, profile_kind="named", profile_name="source")
    copied = (tmp_path / "copied").resolve()
    shutil.copytree(source, copied)
    monkeypatch.setenv("HERMES_HOME", str(copied))
    copied_result = read_profile_provenance(copied)
    assert copied_result.status == "invalid"
    assert copied_result.reason == "origin_home_binding_mismatch"


def _tree_snapshot(root: Path) -> dict[str, tuple]:
    """Capture the exact small home surface used by the read-only provenance controls."""
    snapshot: dict[str, tuple] = {}
    for path in sorted((root, *root.rglob("*"))):
        relative = "." if path == root else str(path.relative_to(root))
        info = path.lstat()
        payload = None if not path.is_file() else path.read_bytes()
        snapshot[relative] = (info.st_mode, info.st_mtime_ns, info.st_size, payload)
    return snapshot


def test_read_markerless_home_is_read_only_and_does_not_mint_install_identity(
    tmp_path, monkeypatch,
):
    home = tmp_path / "markerless"
    home.mkdir()
    (home / "config.yaml").write_text("model: {}\n", encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(home))
    before = _tree_snapshot(home)

    result = read_profile_provenance(home)

    assert result.status == "missing"
    assert result.reason == "origin_missing"
    assert _tree_snapshot(home) == before
    assert not (home / "install_id").exists()
    assert not (home / ".install_id.lock").exists()


def test_existing_markerless_home_rejects_binding_tuple_and_boolean(tmp_path):
    home = tmp_path / "markerless-publisher"
    home.mkdir()
    (home / "user-file").write_text("keep", encoding="utf-8")
    before = _tree_snapshot(home)
    binding = directory_binding(home)
    assert binding is not None

    with pytest.raises(ProfileProvenanceError):
        publish_new_profile_origin(
            home, profile_kind="named", profile_name="existing", expected_binding=binding,
        )
    with pytest.raises(ProfileProvenanceError):
        publish_new_profile_origin(
            home, profile_kind="named", profile_name="existing", birth=True,
        )

    assert _tree_snapshot(home) == before


def test_exclusive_birth_is_opaque_single_use_and_copy_refused(tmp_path):
    home = tmp_path / "opaque-birth"
    birth = create_exclusive_directory_birth(home)
    assert isinstance(birth, ExclusiveDirectoryBirth)
    with pytest.raises(TypeError):
        copy.copy(birth)
    with pytest.raises(TypeError):
        copy.deepcopy(birth)

    result = publish_new_profile_origin(
        home, profile_kind="named", profile_name="opaque", birth=birth,
    )
    assert result.is_new_profile
    with pytest.raises(ProfileProvenanceError):
        publish_new_profile_origin(
            home, profile_kind="named", profile_name="opaque", birth=birth,
        )


def test_exclusive_birth_rejects_native_handle_loss(tmp_path):
    home = tmp_path / "native-loss"
    birth = create_exclusive_directory_birth(home)
    birth._native.close()
    with pytest.raises(ProfileProvenanceError):
        publish_new_profile_origin(
            home, profile_kind="named", profile_name="native-loss", birth=birth,
        )
    assert not (home / ".hermes-profile-origin.json").exists()


@pytest.mark.platforms("posix")
def test_exclusive_birth_rejects_path_replacement_after_native_birth(tmp_path):
    replaced = tmp_path / "replaced"
    replacement_birth = create_exclusive_directory_birth(replaced)
    moved = tmp_path / "moved"
    try:
        replaced.rename(moved)
        replaced.mkdir()
        with pytest.raises(ProfileProvenanceError):
            publish_new_profile_origin(
                replaced, profile_kind="named", profile_name="replaced", birth=replacement_birth,
            )
        assert not (replaced / ".hermes-profile-origin.json").exists()
    finally:
        replacement_birth._native.close()


@pytest.mark.platforms("posix")
def test_native_directory_close_failure_retains_exact_handle_for_retry(tmp_path, monkeypatch):
    """A failed native close is UNKNOWN custody, not a cleared handle."""
    from hermes_cli import profile_provenance_bootstrap as bootstrap

    home = tmp_path / "close-retry"
    home.mkdir()
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    fd = os.open(home, flags)
    native = bootstrap._NativeDirectoryHandle(fd, path=home, windows=False)
    real_close = os.close
    calls = {"count": 0}

    def fail_once(value):
        assert value == fd
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("injected ambiguous close")
        return real_close(value)

    monkeypatch.setattr(os, "close", fail_once)
    with pytest.raises(OSError, match="ambiguous close"):
        native.close()
    assert native._handle == fd

    monkeypatch.setattr(os, "close", real_close)
    native.close()
    assert native._handle is None


@pytest.mark.platforms("windows")
def test_windows_native_directory_identity_includes_volume_and_file_index(tmp_path):
    """The retained Windows handle must match its volume/file identity, not attributes alone."""
    from hermes_cli import profile_provenance_bootstrap as bootstrap

    home = tmp_path / "windows-identity"
    birth = bootstrap.create_exclusive_directory_birth(home)
    try:
        binding = birth._binding
        assert len(binding) == 3
        assert birth._native.matches(binding)
        for index in (0, 1):
            changed = list(binding)
            changed[index] = changed[index] ^ 1
            assert not birth._native.matches(tuple(changed))
    finally:
        birth._native.close()


@pytest.mark.platforms("windows")
def test_windows_native_directory_close_failure_retains_exact_handle_for_retry(tmp_path, monkeypatch):
    """A real Windows directory handle survives one failed CloseHandle and retries."""
    from hermes_cli import profile_provenance_bootstrap as bootstrap

    home = tmp_path / "close-retry-windows"
    home.mkdir()
    native = bootstrap._NativeDirectoryHandle.open(home, protect_name=True)
    real_close = bootstrap._close_windows_handle
    calls = {"count": 0}
    handle = native._handle

    def fail_once(value):
        assert value == handle
        calls["count"] += 1
        if calls["count"] == 1:
            raise OSError("injected CloseHandle failure")
        return real_close(value)

    monkeypatch.setattr(bootstrap, "_close_windows_handle", fail_once)
    with pytest.raises(OSError, match="CloseHandle failure"):
        native.close()
    assert native._handle == handle

    native.close()
    assert native._handle is None


@pytest.mark.platforms("posix")
def test_origin_publication_writes_through_birth_handle_after_home_replacement(tmp_path, monkeypatch):
    """A path replacement cannot redirect the origin marker into the replacement home."""
    from hermes_cli import profile_provenance_bootstrap as bootstrap

    home = tmp_path / "home"
    birth = bootstrap.create_exclusive_directory_birth(home)
    moved = tmp_path / "moved-home"
    original_write = bootstrap._write_exclusive
    triggered = {"value": False}

    def race(path, payload):
        if Path(path) == home / bootstrap.PROVENANCE_FILENAME and not triggered["value"]:
            triggered["value"] = True
            home.rename(moved)
            home.mkdir()
            (home / "state.db").write_text("replacement\n", encoding="utf-8")
        return original_write(path, payload)

    monkeypatch.setattr(bootstrap, "_write_exclusive", race)
    with pytest.raises(ProfileProvenanceError):
        bootstrap.publish_new_profile_origin(
            home, profile_kind="named", profile_name="race", install_id="a" * 32, birth=birth,
        )
    assert triggered["value"]
    assert (home / "state.db").read_text(encoding="utf-8") == "replacement\n"
    assert not (home / bootstrap.PROVENANCE_FILENAME).exists()
    assert not (moved / bootstrap.PROVENANCE_FILENAME).exists()


def test_origin_publication_refuses_target_that_appears_after_initial_check(tmp_path, monkeypatch):
    """The final target check preserves a concurrent destination instead of accepting it."""
    from hermes_cli import profile_provenance_bootstrap as bootstrap

    staging = tmp_path / "staging"
    target = tmp_path / "target"
    birth = bootstrap.create_exclusive_directory_birth(staging)
    original_lexists = bootstrap.os.path.lexists
    triggered = {"value": False}

    def race(path):
        result = original_lexists(path)
        if Path(path) == target and not triggered["value"]:
            triggered["value"] = True
            target.mkdir()
            (target / "state.db").write_text("existing target\n", encoding="utf-8")
        return result

    monkeypatch.setattr(bootstrap.os.path, "lexists", race)
    with pytest.raises(ProfileProvenanceError):
        bootstrap.publish_new_profile_origin(
            staging, profile_kind="named", profile_name="race", install_id="f" * 32,
            birth=birth, publish_target=target,
        )
    assert triggered["value"]
    assert (target / "state.db").read_text(encoding="utf-8") == "existing target\n"
    assert not (target / bootstrap.PROVENANCE_FILENAME).exists()
    assert not (staging / bootstrap.PROVENANCE_FILENAME).exists()


@pytest.mark.platforms("posix")
def test_started_marker_replacement_stays_on_original_native_directory(tmp_path, monkeypatch):
    """A home replacement cannot receive a started marker during the native transition."""
    from hermes_cli import profile_provenance_bootstrap as bootstrap

    home = tmp_path / "home"
    origin = bootstrap.bootstrap_profile_home(home, profile_kind="named", profile_name="race")
    moved = tmp_path / "moved-home"
    original_replace = bootstrap._NativeDirectoryHandle.replace_child
    triggered = {"value": False}

    def race(native, name, payload):
        if not triggered["value"]:
            triggered["value"] = True
            home.rename(moved)
            home.mkdir()
        return original_replace(native, name, payload)

    monkeypatch.setattr(bootstrap._NativeDirectoryHandle, "replace_child", race)
    with pytest.raises(ProfileProvenanceError, match="did not read back"):
        bootstrap.mark_profile_started(
            home,
            owner_epoch="epoch-r5",
            record_binding=("a", "b", "c", "d", "e"),
            install_id=origin.install_id,
        )

    assert triggered["value"] is True
    assert not (home / bootstrap.PROVENANCE_FILENAME).exists()
    assert not (moved / bootstrap.PROVENANCE_FILENAME).exists()


def test_named_profile_final_publication_refuses_late_empty_target(isolated_home, monkeypatch):
    """The final directory move must be native no-replace, not check-then-rename."""
    from hermes_cli import profile_provenance

    target = isolated_home / "profiles" / "race-final"
    original_publish = profile_provenance.publish_new_profile_origin
    triggered = {"value": False}

    def race(*args, **kwargs):
        result = original_publish(*args, **kwargs)
        if not triggered["value"]:
            triggered["value"] = True
            target.mkdir(parents=True)
        return result

    monkeypatch.setattr(profile_provenance, "publish_new_profile_origin", race)
    with pytest.raises(FileExistsError, match="no-replace profile publication"):
        create_profile("race-final", no_alias=True, no_skills=True)

    assert triggered["value"] is True
    assert target.is_dir()
    assert list(target.iterdir()) == []
    assert not any(target.parent.glob(".race-final.staging-*"))


@pytest.mark.platforms("posix")
def test_exclusive_birth_rejects_parent_replacement(tmp_path):
    parent = tmp_path / "parent"
    home = parent / "profile"
    birth = create_exclusive_directory_birth(home)
    moved_parent = tmp_path / "moved-parent"
    try:
        parent.rename(moved_parent)
        parent.mkdir()
        (parent / "profile").mkdir()
        with pytest.raises(ProfileProvenanceError):
            publish_new_profile_origin(
                home, profile_kind="named", profile_name="parent", birth=birth,
            )
    finally:
        birth._native.close()


@pytest.mark.platforms("posix")
def test_exclusive_birth_rejects_user_parent_alias(tmp_path):
    """A caller-owned link above the home is not a trusted OS alias."""
    real_parent = tmp_path / "real-parent"
    real_parent.mkdir()
    alias = tmp_path / "alias"
    alias.symlink_to(real_parent, target_is_directory=True)
    home = alias / "profile"

    with pytest.raises(ProfileProvenanceError, match="linked or reparse-backed"):
        create_exclusive_directory_birth(home)

    assert not (real_parent / "profile").exists()


@pytest.mark.platforms("macos")
def test_exclusive_birth_accepts_native_macos_parent_alias():
    """The native /var -> /private/var alias does not invalidate a new home."""
    assert os.path.realpath("/var") == "/private/var"
    with tempfile.TemporaryDirectory(prefix="hermes-native-alias-", dir="/var/tmp") as root:
        home = Path(root) / "profile"
        origin = bootstrap_profile_home(home, profile_kind="named", profile_name="native-alias")
        assert origin.is_authenticated
        assert read_bootstrap_provenance(home, install_id=origin.install_id).is_authenticated


def test_exclusive_birth_rejects_target_appearance(tmp_path):

    appeared = tmp_path / "appeared"
    appeared.mkdir()
    with pytest.raises(FileExistsError):
        create_exclusive_directory_birth(appeared)


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("replace_part", ["home", "parent"])
def test_windows_exclusive_birth_blocks_path_replacement(tmp_path, replace_part):
    """Windows sharing restrictions block replacement while the native birth handle is held."""
    parent = tmp_path / "parent"
    home = parent / "profile"
    birth = create_exclusive_directory_birth(home)
    moved = tmp_path / "moved"
    try:
        with pytest.raises(OSError) as error:
            (home if replace_part == "home" else parent).rename(moved)
        assert getattr(error.value, "winerror", None) in {5, 32}
    finally:
        birth._native.close()


def test_symlinked_home_is_refused_before_resolving_target(tmp_path, monkeypatch):
    target = tmp_path / "target"
    bootstrap_profile_home(target)
    link = tmp_path / "linked"
    link.symlink_to(target, target_is_directory=True)
    monkeypatch.setenv("HERMES_HOME", str(link))

    result = read_profile_provenance(link)

    assert result.status == "unknown"
    assert result.reason in {"home_path_linked", "home_identity_unavailable"}


def test_first_reservation_fails_closed_before_gateway_lock_when_start_transition_fails(
    tmp_path, monkeypatch,
):
    home = tmp_path / "transition-failure"
    bootstrap_profile_home(home, profile_kind="named", profile_name="transition-failure")

    def fail_transition(*_args, **_kwargs):
        raise ProfileProvenanceError("injected transition failure")

    monkeypatch.setattr("hermes_cli.profile_provenance.mark_profile_started", fail_transition)

    with pytest.raises(OwnershipConflict):
        process_ownership.reserve([home], purpose="i15-transition-failure")

    assert not (home / "gateway.lock").exists()
    assert not process_ownership.owns(home)
    assert read_profile_provenance(home).is_new_profile


def test_native_lock_failure_after_first_transition_cannot_replay_never_started(
    tmp_path, monkeypatch,
):
    home = tmp_path / "native-failure"
    bootstrap_profile_home(home, profile_kind="named", profile_name="native-failure")

    def fail_native_lock(*_args, **_kwargs):
        raise OSError("injected native lock failure")

    monkeypatch.setattr("gateway.runtime_ownership._open_lock", fail_native_lock)

    with pytest.raises(OSError, match="native lock failure"):
        process_ownership.reserve([home], purpose="i15-native-failure")

    origin = read_profile_provenance(home)
    assert origin.status == "valid"
    assert origin.state == STATE_STARTED
    assert not (home / "gateway.lock").exists()


def test_pm_only_precreated_home_does_not_become_first_run(tmp_path, monkeypatch):
    """A PM-created tools tree is existing state, not authenticated home birth."""
    home = (tmp_path / "pm-first").resolve()
    home.mkdir()
    (home / "tools").mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))

    initialize_home(home, ("cron", "sessions", "logs", "memories"), set())

    result = read_profile_provenance(home)
    assert result.status == "missing"
    assert result.reason == "origin_missing"
    assert not (home / ".hermes-profile-origin.json").exists()


def test_malformed_origin_is_never_upgraded_by_existing_runtime_files(tmp_path, monkeypatch):
    home = (tmp_path / "malformed").resolve()
    home.mkdir()
    (home / "tools").mkdir()
    (home / "gateway.lock").write_text("old owner", encoding="utf-8")
    (home / ".hermes-profile-origin.json").write_text("{not-json", encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(home))

    result = read_profile_provenance(home)
    assert result.status == "malformed"
    assert result.reason == "origin_malformed"


def test_started_origin_is_not_replayed_as_first_run(isolated_home):
    profile = create_profile("replay", no_alias=True, no_skills=True)
    process_ownership.reserve([profile], purpose="i15-provenance-test")
    try:
        first = process_ownership.reservation_lineage(profile)
        assert first is not None and first.status == "new_profile_started"
    finally:
        assert process_ownership.release(profile) is True

    process_ownership.reserve([profile], purpose="i15-provenance-replay")
    try:
        second = process_ownership.reservation_lineage(profile)
        assert second is not None
        assert second.status == "existing_profile_started"
        assert read_profile_provenance(profile).state == STATE_STARTED
    finally:
        assert process_ownership.release(profile) is True


def test_authenticated_rename_preserves_origin_binding(isolated_home):
    profile = create_profile("oldname", no_alias=True, no_skills=True)
    before = read_profile_provenance(profile)
    before_binding = directory_binding(profile)
    renamed = rename_profile("oldname", "newname")
    after = read_profile_provenance(renamed)
    assert after.status == "valid"
    assert after.generation == before.generation
    assert after.birth_binding == before.birth_binding == before_binding


def test_stdlib_bootstrap_publishes_default_before_runtime_import(tmp_path):
    home = tmp_path / "clean-default"
    result = bootstrap_profile_home(home)

    identity = read_existing_install_id(home)
    assert result.is_new_profile
    assert identity == result.install_id
    assert read_bootstrap_provenance(home, install_id=identity).birth_binding == directory_binding(home)
    assert not (home / "tools").exists()


def test_stdlib_bootstrap_publishes_named_home_against_existing_install(tmp_path):
    root = tmp_path / "root"
    bootstrap_profile_home(root)
    profile = root / "profiles" / "named"
    result = bootstrap_profile_home(
        profile, profile_kind="named", profile_name="named", identity_root=root,
    )

    identity = read_existing_install_id(root)
    assert result.profile_kind == "named"
    assert result.profile_name == "named"
    assert read_bootstrap_provenance(profile, install_id=identity).is_new_profile


def test_stdlib_bootstrap_does_not_upgrade_markerless_pm_home(tmp_path):
    home = tmp_path / "existing"
    home.mkdir()
    (home / "tools").mkdir()

    with pytest.raises(ProfileProvenanceError):
        bootstrap_profile_home(home)
    assert not (home / ".hermes-profile-origin.json").exists()


def test_stdlib_bootstrap_cli_runs_without_site_packages(tmp_path):
    home = tmp_path / "isolated-cli"
    helper = Path(__file__).parents[2] / "hermes_cli" / "profile_provenance_bootstrap.py"
    result = subprocess.run(
        [sys.executable, "-I", "-S", str(helper), "--home", str(home)],
        capture_output=True, text=True, check=False, timeout=30,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert '"status": "valid"' in result.stdout
    assert (home / ".hermes-profile-origin.json").is_file()


@pytest.mark.platforms("windows")
@pytest.mark.parametrize("replace_part", ["home", "parent"])
def test_windows_origin_publication_preserves_birth_directory(tmp_path, monkeypatch, replace_part):
    from hermes_cli import profile_provenance_bootstrap as bootstrap
    parent = tmp_path / 'parent'
    home = parent / 'home'
    birth = bootstrap.create_exclusive_directory_birth(home)
    moved = tmp_path / 'moved'
    original_write = bootstrap._write_exclusive
    observed = {'attempted': False, 'replacement_blocked': False, 'replacement_created': False, 'marker_written_into_replacement': False}
    def write(path, payload):
        if Path(path) == home / bootstrap.PROVENANCE_FILENAME and not observed['attempted']:
            observed['attempted'] = True
            try:
                (home if replace_part == 'home' else parent).rename(moved)
            except OSError:
                observed['replacement_blocked'] = True
            else:
                home.mkdir(parents=True)
                (home / 'state.db').write_text('existing replacement state\n', encoding='utf-8')
                observed['replacement_created'] = True
        result = original_write(path, payload)
        if observed['replacement_created']:
            observed['marker_written_into_replacement'] = (home / bootstrap.PROVENANCE_FILENAME).exists()
            assert observed['marker_written_into_replacement'] is False, 'origin marker write was redirected to a replacement directory'
        return result
    monkeypatch.setattr(bootstrap, '_write_exclusive', write)
    try:
        result = bootstrap.publish_new_profile_origin(home, profile_kind='default', install_id='a' * 32, birth=birth)
        assert result.is_new_profile
        assert observed['replacement_blocked'] is True
    except bootstrap.ProfileProvenanceError:
        assert observed['replacement_created'] is True
        assert observed['marker_written_into_replacement'] is False
    finally:
        if birth._native._handle is not None:
            birth._native.close()
