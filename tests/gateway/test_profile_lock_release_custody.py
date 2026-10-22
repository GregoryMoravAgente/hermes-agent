"""Unknown profile-lock cleanup retains the descriptor for a same-owner retry."""

import pytest


def test_metadata_failure_rolls_back_before_fresh_reservation(tmp_path, monkeypatch):
    from gateway import runtime_ownership

    home = tmp_path / "profile"
    owner = runtime_ownership.ProfileOwnership()
    original_dump = runtime_ownership.json.dump
    attempts = 0

    def fail_once(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("metadata write failed")
        return original_dump(*args, **kwargs)

    monkeypatch.setattr(runtime_ownership.json, "dump", fail_once)
    try:
        with pytest.raises(OSError, match="metadata write failed"):
            owner.reserve([home])
        assert not owner.homes

        owner.reserve([home])
        assert owner.owner_epoch(home)
        assert attempts == 2
    finally:
        assert not owner.close().failed


def test_foreign_record_retains_unknown_handle_until_same_owner_retry(tmp_path):
    from gateway import runtime_ownership
    import json

    home = tmp_path / "profile"
    owner = runtime_ownership.ProfileOwnership()
    owner.reserve([home])
    epoch = owner.owner_epoch(home)
    record_path = home / "gateway.lock"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    record["owner_epoch"] = "foreign-incarnation"
    record_path.write_text(json.dumps(record), encoding="utf-8")
    try:
        assert owner.owner_epoch(home) is None
        assert not owner.owns(home)
        assert home.resolve() in owner.homes
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
        record["owner_epoch"] = epoch
        record_path.write_text(json.dumps(record), encoding="utf-8")
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.RELEASED
    finally:
        assert not owner.close().failed


def test_close_failure_keeps_handle_until_retry_and_does_not_release_new_owner(tmp_path):
    from gateway import runtime_ownership

    class CloseOnce:
        def __init__(self, wrapped):
            self.wrapped = wrapped
            self.fail = True

        @property
        def closed(self):
            return self.wrapped.closed

        def fileno(self):
            return self.wrapped.fileno()

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

        def close(self):
            if self.fail:
                raise OSError("close failed")
            return self.wrapped.close()

    home = tmp_path / "profile"
    owner = runtime_ownership.ProfileOwnership()
    owner.reserve([home])
    held = owner._handles[home.resolve()]
    wrapped = CloseOnce(held)
    owner._handles[home.resolve()] = wrapped
    try:
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
        assert not owner.owns(home)
        assert home.resolve() in owner.homes
        assert owner.owner_epoch(home) is None

        wrapped.fail = False
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.RELEASED
        replacement = runtime_ownership.ProfileOwnership()
        try:
            replacement.reserve([home])
            assert replacement.owner_epoch(home)
        finally:
            assert not replacement.close().failed
    finally:
        assert not owner.close().failed


def test_partial_global_close_keeps_failed_home_and_releases_other_home(tmp_path):
    from gateway import runtime_ownership

    class CloseOnce:
        def __init__(self, wrapped):
            self.wrapped = wrapped
            self.fail = True

        @property
        def closed(self):
            return self.wrapped.closed

        def fileno(self):
            return self.wrapped.fileno()

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

        def close(self):
            if self.fail:
                raise OSError("close failed")
            return self.wrapped.close()

    home_a = tmp_path / "profile-a"
    home_b = tmp_path / "profile-b"
    owner = runtime_ownership.ProfileOwnership()
    owner.reserve([home_a, home_b])
    wrapped = CloseOnce(owner._handles[home_a.resolve()])
    owner._handles[home_a.resolve()] = wrapped
    try:
        summary = owner.close()
        assert summary.failed is True
        assert summary.outcomes[home_a.resolve()] is runtime_ownership.ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
        assert summary.outcomes[home_b.resolve()] is runtime_ownership.ProfileReleaseOutcome.RELEASED
        assert home_a.resolve() in owner.homes
        assert home_b.resolve() not in owner.homes
        wrapped.fail = False
        assert owner.close().failed is False
    finally:
        assert not owner.close().failed


def test_baseexception_close_stays_unknown_until_same_handle_retry(tmp_path):
    from gateway import runtime_ownership

    class InterruptingClose:
        def __init__(self, wrapped):
            self.wrapped = wrapped
            self.interrupt = True

        @property
        def closed(self):
            return self.wrapped.closed

        def fileno(self):
            return self.wrapped.fileno()

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

        def close(self):
            if self.interrupt:
                raise KeyboardInterrupt
            return self.wrapped.close()

    home = tmp_path / "profile"
    owner = runtime_ownership.ProfileOwnership()
    owner.reserve([home])
    wrapped = InterruptingClose(owner._handles[home.resolve()])
    owner._handles[home.resolve()] = wrapped
    try:
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
        assert home.resolve() in owner.homes
        wrapped.interrupt = False
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.RELEASED
    finally:
        assert not owner.close().failed


def test_ambiguous_close_after_underlying_close_keeps_unknown_from_releasing_a_replacement(tmp_path):
    from gateway import runtime_ownership

    class CloseAfterUnderlyingClose:
        def __init__(self, wrapped):
            self.wrapped = wrapped

        @property
        def closed(self):
            return self.wrapped.closed

        def fileno(self):
            return self.wrapped.fileno()

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

        def close(self):
            self.wrapped.close()
            raise OSError("close outcome is unknown")

    home = tmp_path / "profile"
    owner = runtime_ownership.ProfileOwnership()
    owner.reserve([home])
    held = owner._handles[home.resolve()]
    owner._handles[home.resolve()] = CloseAfterUnderlyingClose(held)
    replacement = runtime_ownership.ProfileOwnership()
    try:
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
        assert not owner.owns(home)
        assert home.resolve() in owner.homes

        replacement.reserve([home])
        replacement_epoch = replacement.owner_epoch(home)
        assert isinstance(replacement_epoch, str)
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
        assert replacement.owner_epoch(home) == replacement_epoch
    finally:
        assert not replacement.close().failed
        # The underlying descriptor is already closed, but the raised close outcome is
        # intentionally unrecoverable: retain the unknown custody record for the caller.
        assert owner.close().failed


def test_foreign_replacement_cannot_validate_an_old_token_or_sibling(tmp_path):
    from gateway import runtime_ownership

    home = tmp_path / "profile"
    sibling = tmp_path / "sibling"
    owner = runtime_ownership.ProfileOwnership()
    foreign = runtime_ownership.ProfileOwnership()
    owner.reserve([home, sibling])
    old = owner.owner_epoch(home)
    try:
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.RELEASED
        foreign.reserve([home])
        assert not owner.reservation_is_current(home, old)
        assert owner.owner_epoch(sibling)
        assert foreign.owner_epoch(home)
    finally:
        assert not foreign.close().failed
        assert not owner.close().failed


def test_same_owner_live_start_drift_revalidates_through_real_lock(tmp_path, monkeypatch):
    from gateway import runtime_ownership, status

    home = tmp_path / "profile"
    owner = runtime_ownership.ProfileOwnership()
    owner.reserve([home])
    real_build = status._build_pid_record
    try:
        original = real_build()
        assert isinstance(original.get("start_time"), int)

        def drifted_current_record():
            current = real_build()
            current["start_time"] = int(current["start_time"]) + 1
            return current

        monkeypatch.setattr(status, "_build_pid_record", drifted_current_record)
        evidence = owner.reservation_evidence(home)
        assert evidence is not None
        assert evidence["record_binding"][3] == original["start_time"]
    finally:
        assert not owner.close().failed


def test_outer_unknown_maintenance_does_not_reuse_thread_owner_after_exit(tmp_path):
    from gateway.runtime_ownership import (
        OwnershipConflict, exclusive_maintenance, release_maintenance_hold, retain_maintenance,
    )
    from types import SimpleNamespace

    home = tmp_path / "profile"
    context = exclusive_maintenance([home], purpose="external-process-operation")
    owner = context.__enter__()
    finalizer = SimpleNamespace(state="unknown")
    first_epoch = owner.owner_epoch(home)
    try:
        retain_maintenance(home, finalizer, owner=owner)
        context.__exit__(None, None, None)
        with pytest.raises(OwnershipConflict, match="Exclusive maintenance refused"):
            with exclusive_maintenance([home], purpose="external-process-operation"):
                raise AssertionError("a fresh outer context reused a retained owner")

        finalizer.state = "terminal"
        assert release_maintenance_hold(home, finalizer, owner=owner) is True
        with exclusive_maintenance([home], purpose="external-process-operation") as successor:
            second_epoch = successor.owner_epoch(home)
            assert isinstance(second_epoch, str) and second_epoch != first_epoch
    finally:
        if owner.finalizer_hold(home) is not None:
            finalizer.state = "terminal"
            release_maintenance_hold(home, finalizer, owner=owner)
        assert not owner.close().failed


def test_closed_original_handle_cannot_validate_recycled_descriptor(tmp_path):
    import json
    import os

    from gateway import runtime_ownership

    home = tmp_path / "profile"
    owner = runtime_ownership.ProfileOwnership()
    owner.reserve([home])
    key = home.resolve()
    original = owner._handles[key]
    original_fd = original.fileno()
    record = json.loads((home / "gateway.lock").read_text(encoding="utf-8"))
    original.close()
    (home / "gateway.lock").write_text(json.dumps(record), encoding="utf-8")

    replacement_fd = os.open(home / "gateway.lock", os.O_RDWR)
    if replacement_fd != original_fd:
        os.dup2(replacement_fd, original_fd)
        os.close(replacement_fd)
        replacement_fd = original_fd
    replacement = os.fdopen(replacement_fd, "r+", encoding="utf-8")
    owner._handles[key] = replacement
    try:
        assert owner.owner_epoch(home) is None
        assert owner.reservation_evidence(home) is None
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
    finally:
        replacement.close()
        owner._handles.pop(key, None)
        owner._unknown_homes.discard(key)
        owner._owner_epochs.pop(key, None)
        owner._owner_record_bindings.pop(key, None)
        owner._predecessor_records.pop(key, None)


def test_outer_close_failure_keeps_owner_for_same_owner_retry(tmp_path):
    from gateway import runtime_ownership

    class CloseOnce:
        def __init__(self, wrapped):
            self.wrapped = wrapped
            self.fail = True

        @property
        def closed(self):
            return self.wrapped.closed

        def fileno(self):
            return self.wrapped.fileno()

        def __getattr__(self, name):
            return getattr(self.wrapped, name)

        def close(self):
            if self.fail:
                raise OSError("close failed")
            return self.wrapped.close()

    home = tmp_path / "profile"
    context = runtime_ownership.exclusive_maintenance(
        [home], purpose="external-process-operation")
    owner = context.__enter__()
    key = home.resolve()
    first_epoch = owner.owner_epoch(home)
    wrapped = CloseOnce(owner._handles[key])
    owner._handles[key] = wrapped
    try:
        with pytest.raises(runtime_ownership.OwnershipReleaseError):
            context.__exit__(None, None, None)
        state = getattr(runtime_ownership._maintenance, "state", None)
        assert isinstance(state, tuple) and state[1] is owner
        with pytest.raises(runtime_ownership.OwnershipConflict, match="Exclusive maintenance refused"):
            with runtime_ownership.exclusive_maintenance(
                [home], purpose="external-process-operation"):
                raise AssertionError("a fresh context reused unknown custody")

        wrapped.fail = False
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.RELEASED
        with runtime_ownership.exclusive_maintenance(
                [home], purpose="external-process-operation") as successor:
            second_epoch = successor.owner_epoch(home)
            assert isinstance(first_epoch, str)
            assert isinstance(second_epoch, str) and second_epoch != first_epoch
    finally:
        runtime_ownership._maintenance.state = None
        assert not owner.close().failed


def test_shutdown_watchdog_pid_failure_invalidates_clean_marker(tmp_path, monkeypatch):
    from gateway import shutdown_watchdog
    from gateway import status as gateway_status

    marker = tmp_path / ".clean_shutdown"
    marker.write_text("clean", encoding="utf-8")
    monkeypatch.setattr(shutdown_watchdog, "_process_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(gateway_status, "release_gateway_runtime_lock", lambda: None)
    monkeypatch.setattr(gateway_status, "remove_pid_file_checked", lambda: False)

    assert shutdown_watchdog._release_shutdown_watchdog_ownership() is False
    assert not marker.exists()


def test_shutdown_watchdog_release_baseexception_invalidates_clean_marker(tmp_path, monkeypatch):
    from gateway import shutdown_watchdog
    from gateway import status as gateway_status

    marker = tmp_path / ".clean_shutdown"
    marker.write_text("clean", encoding="utf-8")
    monkeypatch.setattr(shutdown_watchdog, "_process_hermes_home", lambda: tmp_path)

    def interrupt_release():
        raise KeyboardInterrupt

    monkeypatch.setattr(gateway_status, "release_gateway_runtime_lock", interrupt_release)
    assert shutdown_watchdog._release_shutdown_watchdog_ownership() is False
    assert not marker.exists()


def test_checked_pid_cleanup_rejects_unvalidated_successor_fence(tmp_path, monkeypatch):
    from gateway import status as gateway_status

    pid_path = tmp_path / "gateway.pid"
    pid_path.write_text('{"pid": 999999, "hermes_home": "foreign"}', encoding="utf-8")
    monkeypatch.setattr(gateway_status, "_get_pid_path", lambda: pid_path)
    monkeypatch.setattr(gateway_status, "_get_process_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(gateway_status, "_record_matches_live_gateway_pid", lambda *args, **kwargs: False)

    assert gateway_status.remove_pid_file_checked() is False
    assert pid_path.exists()
