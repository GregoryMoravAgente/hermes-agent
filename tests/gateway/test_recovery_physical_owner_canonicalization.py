"""Recovery physical ownership uses the retained reservation's native path identity."""

from pathlib import Path
from types import SimpleNamespace

import pytest


def test_recovery_requires_real_retained_profile_lock(tmp_path):
    from gateway.runtime_ownership import canonical_home, process_ownership
    from gateway.session_recovery_custody import _physical_owner
    from hermes_state_runtime import RuntimeStoreError

    home = tmp_path / "RecoveryProfile"
    home.mkdir()
    authority = SimpleNamespace(db=SimpleNamespace(db_path=home / "state.db"))
    process_ownership.reserve([home])
    try:
        proof = _physical_owner(authority)
        assert proof["profile_home"] == str(canonical_home(home))
        assert proof["native_handle_retained"] is True
        assert process_ownership.release(home) is True
        with pytest.raises(RuntimeStoreError, match="foreign_custody"):
            _physical_owner(authority)
    finally:
        process_ownership.close()


@pytest.mark.platforms("windows")
def test_windows_recovery_accepts_case_alias_but_refuses_lost_physical_owner(tmp_path):
    from gateway.runtime_ownership import process_ownership
    from gateway.session_recovery_custody import _physical_owner
    from hermes_state_runtime import RuntimeStoreError

    home = tmp_path / "RecoveryProfile"
    home.mkdir()
    alias = Path(str(home).swapcase())
    authority = SimpleNamespace(db=SimpleNamespace(db_path=alias / "state.db"))
    process_ownership.reserve([home])
    try:
        original = process_ownership.reservation_evidence(home)
        assert _physical_owner(authority)["physical_binding"] == original["physical_binding"]
        assert process_ownership.release(home) is True
        with pytest.raises(RuntimeStoreError, match="foreign_custody"):
            _physical_owner(authority)
    finally:
        process_ownership.close()
