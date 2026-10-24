"""Regression tests for AGC-1721's ownership-safe profile-home matching."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from gateway.runtime_native_cleanup_profile import (
    _records_valid,
    _witness_homes_match_bindings,
    profile_bindings_match,
    profile_participants_match_witness,
)


def _binding(home: Path, handle: object) -> tuple[Path, object, dict[str, str]]:
    return home, handle, {"profile_home": str(home)}


def _mixed_case(path: Path) -> Path:
    return Path(
        "".join(
            character.swapcase() if character.isalpha() else character
            for character in str(path)
        )
    )


@pytest.mark.platforms("windows")
def test_profile_bindings_match_accepts_windows_case_alias_for_same_home(
    tmp_path: Path,
) -> None:
    home = tmp_path / "ProfileHome"
    home.mkdir()
    alias = _mixed_case(home)
    handle = object()

    assert str(alias) != str(home)
    assert alias.samefile(home)
    assert profile_bindings_match((_binding(home, handle),), (_binding(alias, handle),))


@pytest.mark.platforms("windows")
def test_profile_bindings_match_rejects_windows_canonical_duplicate(
    tmp_path: Path,
) -> None:
    home = tmp_path / "ProfileHome"
    home.mkdir()
    alias = _mixed_case(home)

    bindings = (_binding(home, object()), _binding(alias, object()))
    assert not profile_bindings_match(bindings, bindings)


@pytest.mark.platforms("windows")
def test_profile_participants_match_witness_uses_same_canonical_home_key(
    tmp_path: Path,
) -> None:
    home = tmp_path / "ProfileHome"
    home.mkdir()
    alias = _mixed_case(home)
    handle = object()
    participant = object()
    snapshot = SimpleNamespace(
        home=alias,
        handle=handle,
        evidence={"profile_home": str(alias)},
    )
    witness = SimpleNamespace(
        records=((str(home), object(), object(), object(), object(), object()),),
        profile_bindings=(_binding(home, handle),),
    )

    assert _witness_homes_match_bindings(witness)
    assert profile_participants_match_witness(
        witness,
        ((snapshot, participant, handle),),
    )


@pytest.mark.platforms("windows")
def test_records_valid_accepts_case_aliases_without_relaxing_record_identity(
    tmp_path: Path,
) -> None:
    home = tmp_path / "ProfileHome"
    home.mkdir()
    alias = _mixed_case(home)
    authority = object()
    graph = object()
    provider_snapshot = object()
    registered_witness = object()
    lease = SimpleNamespace(
        profile_id="profile",
        profile_home=str(home),
        authority_epoch="authority",
        process_instance="process",
        physical_owner_epoch="owner",
    )
    record = SimpleNamespace(
        authority=authority,
        lease=lease,
        graph=graph,
        close_generation="generation",
        state="closing",
        producer=None,
        global_provider_prepared=True,
        global_provider_snapshot=provider_snapshot,
        registered_witness=registered_witness,
    )
    witness = SimpleNamespace(
        custody=SimpleNamespace(_current={str(alias): record}),
        records=((str(home), record, authority, lease, graph, provider_snapshot),),
        close_generation="generation",
    )
    source_profiles = [
        {
            "profile_id": "profile",
            "profile_home": str(alias),
            "authority_epoch": "authority",
            "process_instance": "process",
            "physical_owner_epoch": "owner",
            "close_generation": "generation",
            "registered_witness": registered_witness,
        }
    ]

    assert _records_valid(witness, source_profiles, home)


@pytest.mark.platforms("linux")
def test_profile_bindings_match_keeps_posix_case_distinct_homes_separate(
    tmp_path: Path,
) -> None:
    upper = tmp_path / "ProfileHome"
    lower = tmp_path / "profilehome"
    upper.mkdir()
    lower.mkdir()
    handle = object()

    assert not profile_bindings_match(
        (_binding(upper, handle),),
        (_binding(lower, handle),),
    )


@pytest.mark.platforms("posix")
def test_profile_bindings_match_rejects_linked_home_alias(tmp_path: Path) -> None:
    target = tmp_path / "target"
    alias = tmp_path / "alias"
    target.mkdir()
    alias.symlink_to(target, target_is_directory=True)
    handle = object()

    assert not profile_bindings_match(
        (_binding(target, handle),),
        (_binding(alias, handle),),
    )


def test_profile_bindings_match_rejects_replaced_native_handle(tmp_path: Path) -> None:
    home = tmp_path / "ProfileHome"
    home.mkdir()

    assert not profile_bindings_match(
        (_binding(home, object()),),
        (_binding(home, object()),),
    )


def test_profile_bindings_match_rejects_relative_home(tmp_path: Path, monkeypatch) -> None:
    home = tmp_path / "ProfileHome"
    home.mkdir()
    monkeypatch.chdir(tmp_path)
    handle = object()

    assert not profile_bindings_match(
        (_binding(home, handle),),
        (_binding(Path("ProfileHome"), handle),),
    )
