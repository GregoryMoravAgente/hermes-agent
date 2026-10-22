"""Real store-custody control for a descriptor-close failure and retry."""

from __future__ import annotations

import pytest


class _CloseOnceFailure:
    """Delegate every operation to one real locked descriptor and fail only its first close."""

    def __init__(self, handle):
        self._handle = handle
        self.close_calls = 0

    def __getattr__(self, name):
        return getattr(self._handle, name)

    def close(self):
        self.close_calls += 1
        if self.close_calls == 1:
            raise OSError("descriptor close unavailable")
        return self._handle.close()


@pytest.mark.platforms("posix")
def test_store_close_failure_keeps_real_lease_until_retry(tmp_path):
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner, TeamsStoreOwnershipError

    store_path = tmp_path / "teams.json"
    owner_a = TeamsStoreOwner(
        profile_id=str(tmp_path / "profile-a"), authority_epoch="close-r3",
        instance_id="owner-a", owner_id="teams:owner-a",
    )
    owner_b = TeamsStoreOwner(
        profile_id=str(tmp_path / "profile-a"), authority_epoch="close-r3",
        instance_id="owner-b", owner_id="teams:owner-b",
    )
    store_a = TeamsPipelineStore(store_path, owner=owner_a, lock=True)
    real_handle = store_a._ownership._handle
    failing_handle = _CloseOnceFailure(real_handle)
    store_a._ownership._handle = failing_handle
    try:
        assert store_a.close() is False
        assert failing_handle.close_calls == 1

        # The independent real contender must be refused before the original descriptor is
        # retried; this is the ordering that would expose an unlock-before-close regression.
        with pytest.raises(TeamsStoreOwnershipError, match="owned"):
            TeamsPipelineStore(store_path, owner=owner_b, lock=True)

        # The failed close remains a usable retry state, so the original caller can still write.
        store_a.upsert_job("close-r3-job", {"status": "received"})
        assert store_a.close() is True
        with pytest.raises(TeamsStoreOwnershipError, match="closed"):
            store_a.list_jobs()

        successor = TeamsPipelineStore(store_path, owner=owner_b, lock=True)
        try:
            assert successor.get_job("close-r3-job")["status"] == "received"
        finally:
            assert successor.close() is True
    finally:
        if not store_a._closed:
            store_a.close()


class _CloseAfterReleaseFailure:
    """Close the real descriptor, then report an OSError once to model ambiguous close."""

    def __init__(self, handle):
        self._handle = handle
        self.close_calls = 0

    def __getattr__(self, name):
        return getattr(self._handle, name)

    def close(self):
        self.close_calls += 1
        if self.close_calls == 1:
            self._handle.close()
            raise OSError("descriptor close result unavailable")
        return self._handle.close()


@pytest.mark.platforms("posix")
def test_store_close_after_release_error_fails_closed_and_retry_is_stale_safe(tmp_path):
    from plugins.teams_pipeline.store import TeamsPipelineStore
    from plugins.teams_pipeline.store_ownership import TeamsStoreOwner, TeamsStoreOwnershipError

    store_path = tmp_path / "teams-after-release.json"
    owner_a = TeamsStoreOwner(
        profile_id=str(tmp_path / "profile-a"), authority_epoch="close-r4",
        instance_id="owner-a", owner_id="teams:owner-a",
    )
    owner_b = TeamsStoreOwner(
        profile_id=str(tmp_path / "profile-a"), authority_epoch="close-r4",
        instance_id="owner-b", owner_id="teams:owner-b",
    )
    store_a = TeamsPipelineStore(store_path, owner=owner_a, lock=True)
    real_handle = store_a._ownership._handle
    failing_handle = _CloseAfterReleaseFailure(real_handle)
    store_a._ownership._handle = failing_handle
    successor = None
    try:
        assert store_a.close() is False
        assert failing_handle.close_calls == 1

        # The underlying descriptor is already closed, so the stale caller must fail closed for
        # both reads and writes instead of touching a successor's newly acquired lease.
        with pytest.raises(TeamsStoreOwnershipError, match="owner_lost"):
            store_a.list_jobs()
        with pytest.raises(TeamsStoreOwnershipError, match="owner_lost"):
            store_a.upsert_job("stale-r4-job", {"status": "received"})

        successor = TeamsPipelineStore(store_path, owner=owner_b, lock=True)
        assert store_a.close() is True
        successor.upsert_job("successor-r4-job", {"status": "received"})
        assert successor.get_job("successor-r4-job")["status"] == "received"
    finally:
        if successor is not None and not successor._closed:
            successor.close()
        if not store_a._closed:
            store_a.close()
