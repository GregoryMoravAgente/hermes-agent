"""Causal controls for the authenticated external-cron jobs-lock handoff.

These tests cover the proposed state transition only.  Windows qualification
still requires the real parent/child/native-Job test on the hosted Windows
runner; the POSIX controls here must not be used as that qualification.
"""

from __future__ import annotations

import copy
import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cron import jobs


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_strict_jobs_lock_detaches_once_and_never_reuses_detached_depth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")

    with jobs._jobs_lock(strict=True) as token:
        key = jobs._jobs_lock_key()
        owner = object()
        jobs._jobs_lock_states()[key]["owner"] = owner
        assert jobs._detach_jobs_lock_for_handoff(
            expected_key=key, expected_token=object()).status == "refused"
        result = jobs._detach_jobs_lock_for_handoff(
            expected_key=key, expected_token=token, expected_owner=owner)
        assert result.detached is True
        assert jobs._jobs_lock_states()[key]["detached"] is True
        with pytest.raises(jobs.CronJobsLockUnavailable, match="detached"):
            with jobs._jobs_lock(strict=True):
                pass
        assert jobs._detach_jobs_lock_for_handoff(
            expected_key=key, expected_token=token).status == "refused"

    # The outer context performed no second unlock/close, and a fresh strict
    # section can acquire the same profile lock normally.
    with jobs._jobs_lock(strict=True):
        pass


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_jobs_lock_handoff_retains_exact_fd_on_unlock_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    original_release = jobs._release_handoff_transfer
    monkeypatch.setattr(
        jobs,
        "_release_handoff_transfer",
        lambda transfer: jobs._CronJobsLockHandoff("pending", transfer.key, transfer),
    )

    with jobs._jobs_lock(strict=True) as token:
        key = jobs._jobs_lock_key()
        state = jobs._jobs_lock_states()[key]
        original_fd = state["lock_fd"]
        owner = object()
        state["owner"] = owner
        result = jobs._detach_jobs_lock_for_handoff(
            expected_key=key, expected_token=token, expected_owner=owner)
        assert result.pending is True
        assert state["lock_fd"] is original_fd
        assert state["detached"] is False
        transfer_result = jobs._transfer_jobs_lock_handoff(
            expected_key=key,
            expected_token=token,
            expected_owner=owner,
        )
        assert transfer_result.status == "transferred"
        transfer = transfer_result.transfer
        assert transfer is not None
        assert transfer.fd is original_fd
        assert state["lock_fd"] is None

    # Restore the real release path before retrying the exact retained
    # descriptor. The retry must close that descriptor and remove the
    # thread-local state; a fresh lock must then be acquirable.
    monkeypatch.setattr(jobs, "_release_handoff_transfer", original_release)
    retry = jobs._retry_transferred_jobs_lock(transfer, expected_owner=owner)
    assert retry.detached is True
    assert transfer.fd is None
    assert jobs._jobs_lock_states() == {}
    with jobs._jobs_lock(strict=True):
        pass


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_refused_release_keeps_normal_context_cleanup_owned_by_parent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    monkeypatch.setattr(
        jobs,
        "_release_handoff_transfer",
        lambda transfer: jobs._CronJobsLockHandoff("refused", transfer.key, transfer),
    )

    with jobs._jobs_lock(strict=True) as token:
        key = jobs._jobs_lock_key()
        state = jobs._jobs_lock_states()[key]
        original_fd = state["lock_fd"]
        owner = object()
        state["owner"] = owner
        result = jobs._detach_jobs_lock_for_handoff(
            expected_key=key, expected_token=token, expected_owner=owner,
        )
        assert result.status == "refused"
        assert state["handoff_pending"] is False
        assert state["lock_fd"] is original_fd

    # A non-retryable refusal remains parent-owned and the ordinary context
    # finalizer releases it; no stale transfer state or fd survives.
    assert jobs._jobs_lock_states() == {}
    with jobs._jobs_lock(strict=True):
        pass


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_jobs_lock_handoff_refuses_replaced_path_or_native_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    monkeypatch.setattr(jobs, "_lock_file_identity_matches", lambda _state, _fd: False)

    with jobs._jobs_lock(strict=True) as token:
        key = jobs._jobs_lock_key()
        state = jobs._jobs_lock_states()[key]
        original_fd = state["lock_fd"]
        owner = object()
        state["owner"] = owner
        result = jobs._detach_jobs_lock_for_handoff(
            expected_key=key, expected_token=token, expected_owner=owner,
        )
        assert result.status == "refused"
        assert state["lock_fd"] is original_fd
        assert state["detached"] is False


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_identity_race_retains_same_descriptor_for_later_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gateway.runtime_cron_source_registry import CronAdmissionLease

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    original_matches = jobs._transfer_identity_matches
    monkeypatch.setattr(jobs, "_transfer_identity_matches", lambda _transfer: False)

    lease = CronAdmissionLease(None, "worker", tmp_path)
    with lease:
        outcome = lease.detach_jobs_lock_after_handoff()
        assert outcome.retained is True
        assert lease._jobs_lock_handoff_pending is True

    transfer = lease._handoff_transfer
    assert lease.active is True
    assert transfer is not None
    retained_fd = transfer.fd
    assert retained_fd is not None
    assert jobs._jobs_lock_states() == {}

    # The source-owned retry may proceed only after the original native/path
    # witness is restored; it must release this exact descriptor, not acquire
    # a replacement lock.
    monkeypatch.setattr(jobs, "_transfer_identity_matches", original_matches)
    assert lease.retry_jobs_lock_after_handoff().detached is True
    assert transfer.fd is None
    assert lease.release_after_handoff_retry().status == "released"
    assert lease.active is False


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_worker_admission_keeps_source_lease_after_jobs_lock_detach(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gateway.runtime_cron_source_registry import CronAdmissionLease

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")

    lease = CronAdmissionLease(None, "worker", tmp_path)
    with lease:
        assert copy.copy(lease).detach_jobs_lock_after_handoff().status == "refused"
        outcome = lease.detach_jobs_lock_after_handoff()
        assert outcome.detached is True
        assert lease.active is True
    assert lease.active is False


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_failed_detach_survives_context_exit_for_same_fd_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gateway.runtime_cron_source_registry import CronAdmissionLease

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    original_release = jobs._release_handoff_transfer
    monkeypatch.setattr(
        jobs,
        "_release_handoff_transfer",
        lambda transfer: jobs._CronJobsLockHandoff("pending", transfer.key, transfer),
    )

    previous_store = jobs._cron_store_override.get()
    lease = CronAdmissionLease(None, "worker", tmp_path)
    with lease:
        outcome = lease.detach_jobs_lock_after_handoff()
        assert outcome.pending is True

    # __exit__ intentionally leaves the lease and exact descriptor live; it
    # must not silently close the only retryable ownership witness.  The
    # thread-local state and in-process RLock are gone; the lease owns the fd.
    transfer = lease._handoff_transfer
    assert lease.active is True
    assert jobs._cron_store_override.get() is previous_store
    assert transfer is not None
    retained_fd = transfer.fd
    assert transfer.fd is retained_fd
    assert jobs._jobs_lock_states() == {}

    monkeypatch.setattr(jobs, "_release_handoff_transfer", original_release)
    retry = lease.retry_jobs_lock_after_handoff()
    assert retry.detached is True
    assert lease.release_after_handoff_retry().status == "released"
    assert lease.active is False


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_pending_descriptor_transfers_to_later_thread_and_retries_same_fd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gateway.runtime_cron_source_registry import CronAdmissionLease

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    original_release = jobs._release_handoff_transfer
    attempts = {"count": 0}

    def fail_once(transfer):
        attempts["count"] += 1
        if attempts["count"] == 1:
            return jobs._CronJobsLockHandoff("pending", transfer.key, transfer)
        return original_release(transfer)

    monkeypatch.setattr(jobs, "_release_handoff_transfer", fail_once)
    lease = CronAdmissionLease(None, "worker", tmp_path)
    with lease:
        assert lease.detach_jobs_lock_after_handoff().pending is True
        key = jobs._jobs_lock_key()

    transfer = lease._handoff_transfer
    assert lease.active is True
    assert transfer is not None
    assert jobs._jobs_lock_states() == {}
    retained_fd = transfer.fd
    result_box = {}

    def retry_from_other_thread():
        result_box["retry"] = lease.retry_jobs_lock_after_handoff()
        result_box["release"] = lease.release_after_handoff_retry()

    thread = threading.Thread(target=retry_from_other_thread)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert result_box["retry"].detached is True
    assert result_box["release"].status == "released"
    assert transfer.fd is None
    assert retained_fd is not None
    assert lease.active is False
    assert key not in jobs._jobs_lock_states()


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_persistent_later_thread_failure_retains_transfer_and_fd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from gateway.runtime_cron_source_registry import CronAdmissionLease

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    original_release = jobs._release_handoff_transfer
    monkeypatch.setattr(
        jobs,
        "_release_handoff_transfer",
        lambda transfer: jobs._CronJobsLockHandoff("pending", transfer.key, transfer),
    )

    lease = CronAdmissionLease(None, "worker", tmp_path)
    with lease:
        assert lease.detach_jobs_lock_after_handoff().pending is True
    transfer = lease._handoff_transfer
    assert transfer is not None
    retained_fd = transfer.fd
    result_box = {}

    def persistent_retry():
        result_box["retry"] = lease.retry_jobs_lock_after_handoff()

    thread = threading.Thread(target=persistent_retry)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert result_box["retry"].pending is True
    assert transfer.fd is retained_fd
    assert transfer.unlocked is False
    assert lease.active is True

    monkeypatch.setattr(jobs, "_release_handoff_transfer", original_release)
    assert lease.retry_jobs_lock_after_handoff().detached is True
    assert lease.release_after_handoff_retry().status == "released"


def test_registry_lifecycle_retries_transferred_lock_after_context_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production continuation retries the transferred fd from another thread."""
    from cron import scheduler
    from gateway.runtime_cron_source_registry import CronAdmissionLease

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    original_release = jobs._release_handoff_transfer
    attempts = {"count": 0}

    def fail_once(transfer):
        attempts["count"] += 1
        if attempts["count"] == 1:
            return jobs._CronJobsLockHandoff("pending", transfer.key, transfer)
        return original_release(transfer)

    monkeypatch.setattr(jobs, "_release_handoff_transfer", fail_once)
    finished = threading.Event()

    class _LifecycleRegistry:
        def _leave_lease(self, _lease):
            finished.set()

    lease = CronAdmissionLease(None, "worker", tmp_path)
    with lease:
        assert lease.detach_jobs_lock_after_handoff().pending is True
    transfer = lease._handoff_transfer
    assert transfer is not None
    lease.registry = _LifecycleRegistry()

    waited = threading.Event()

    def wait_for_worker(*_args, **_kwargs):
        waited.set()
        return True

    monkeypatch.setattr(scheduler, "_wait_for_external_cron_worker", wait_for_worker)
    assert scheduler._schedule_external_worker_handoff_retry(
        lease,
        process=SimpleNamespace(pid=1234),
        execution_id="exec-lifecycle",
        job_id="job-lifecycle",
        handoff_files=(),
        stderr_path=tmp_path / "worker.stderr",
        custody=Mock(),
        dispatch_mode="direct",
        job={"id": "job-lifecycle", "execution_id": "exec-lifecycle"},
    ) is True
    assert waited.wait(timeout=5)
    assert finished.wait(timeout=5)
    assert attempts["count"] == 2
    assert transfer.fd is None
    assert lease.active is False


def test_registry_lifecycle_retains_persistent_transferred_lock_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A later-thread retry timeout retains the same fd and active source lease."""
    from cron import scheduler
    from gateway.runtime_cron_source_registry import CronAdmissionLease

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    monkeypatch.setattr(scheduler, "_CRON_LOCK_HANDOFF_RETRY_SECONDS", 0.05)
    original_release = jobs._release_handoff_transfer
    monkeypatch.setattr(
        jobs,
        "_release_handoff_transfer",
        lambda transfer: jobs._CronJobsLockHandoff("pending", transfer.key, transfer),
    )

    class _LifecycleRegistry:
        def __init__(self):
            self.releases = 0

        def _leave_lease(self, _lease):
            self.releases += 1

    lease = CronAdmissionLease(None, "worker", tmp_path)
    with lease:
        assert lease.detach_jobs_lock_after_handoff().pending is True
    transfer = lease._handoff_transfer
    assert transfer is not None
    retained_fd = transfer.fd
    registry = _LifecycleRegistry()
    lease.registry = registry

    from agent import memory_provider

    retry_finished = threading.Event()
    original_spawn = memory_provider.spawn_context_thread

    def observable_spawn(target, *, name, daemon=True, args=(), kwargs=None):
        def wrapped():
            try:
                target(*args, **(kwargs or {}))
            finally:
                retry_finished.set()

        return original_spawn(wrapped, name=name, daemon=daemon)

    monkeypatch.setattr(memory_provider, "spawn_context_thread", observable_spawn)

    assert scheduler._schedule_external_worker_handoff_retry(
        lease,
        process=SimpleNamespace(pid=1234),
        execution_id="exec-persistent",
        job_id="job-persistent",
        handoff_files=(),
        stderr_path=tmp_path / "worker.stderr",
        custody=Mock(),
        dispatch_mode="direct",
        job={"id": "job-persistent", "execution_id": "exec-persistent"},
    ) is True
    assert retry_finished.wait(timeout=5)
    # The continuation is deliberately bounded; no wait or release is allowed
    # while the exact descriptor remains pending.
    assert transfer.fd is retained_fd
    assert transfer.unlocked is False
    assert lease.active is True
    assert registry.releases == 0

    monkeypatch.setattr(jobs, "_release_handoff_transfer", original_release)
    assert lease.retry_jobs_lock_after_handoff().detached is True
    assert lease.release_after_handoff_retry().status == "released"
    assert registry.releases == 1


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_real_registry_handoff_retry_waits_for_active_lease_and_releases_same_fd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real source registry owns the pending lease and later-thread retry.

    This intentionally exercises ``CronSourceRegistry._active_leases`` and its
    close condition.  A dummy registry assigned after context exit would miss
    the production begin-close race this control covers.
    """
    from cron import scheduler
    from gateway.runtime_cron_source_registry import CronAdmissionLease, CronSourceRegistry
    from cron import scheduler_worker_failure
    from hermes_cli import sqlite_util
    from hermes_constants import get_hermes_home

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    original_release = jobs._release_handoff_transfer
    attempts = {"count": 0}

    def fail_once(transfer):
        attempts["count"] += 1
        if attempts["count"] == 1:
            return jobs._CronJobsLockHandoff("pending", transfer.key, transfer)
        return original_release(transfer)

    monkeypatch.setattr(jobs, "_release_handoff_transfer", fail_once)
    registry = CronSourceRegistry(SimpleNamespace())
    # The handoff test is about the real lease/close lifecycle; graph bootstrap
    # is covered by the source-registry suite, so retain its admission seam
    # while using the production registry object and active-lease map here.
    monkeypatch.setattr(registry, "_validate_admission_locked", lambda *_args: None)
    monkeypatch.setattr(registry, "_validate_graph_locked", lambda: True)

    lease = CronAdmissionLease(registry, "worker", tmp_path)
    with lease:
        assert lease.detach_jobs_lock_after_handoff().pending is True
        assert registry._active_leases.get(lease.lease_id) is lease
        # A real close cannot pass while this accepted lease still owns the
        # source custody, and it must reopen after the bounded wait rather than
        # stealing or discarding the pending descriptor.
        assert registry.begin_close(1, deadline=time.monotonic() + 0.03) is False
        assert registry.state == "open"

    transfer = lease._handoff_transfer
    assert transfer is not None
    retained_fd = transfer.fd
    waited = threading.Event()
    released = threading.Event()
    release_result: dict[str, object] = {}

    def wait_for_worker(*_args, **_kwargs):
        waited.set()
        raise RuntimeError("synthetic later-thread wait failure")

    monkeypatch.setattr(scheduler, "_wait_for_external_cron_worker", wait_for_worker)
    scope_seen: dict[str, object] = {}
    execution_db_paths: list[Path] = []
    original_open_db = sqlite_util.open_db

    def observe_open_db(path, *args, **kwargs):
        execution_db_paths.append(Path(path).resolve())
        return original_open_db(path, *args, **kwargs)

    monkeypatch.setattr(sqlite_util, "open_db", observe_open_db)

    def record_unknown(*_args, **_kwargs):
        scope_seen["store"] = jobs._cron_store_override.get()
        scope_seen["hermes_home"] = get_hermes_home().resolve()
        from cron import executions

        scope_seen["execution_lookup"] = executions.get_execution("exec-real-registry")
        return True

    monkeypatch.setattr(
        scheduler_worker_failure, "record_unknown_worker_outcome", record_unknown,
    )
    original_release_lease = lease.release_after_handoff_retry

    def release_and_observe():
        release_result["outcome"] = original_release_lease()
        released.set()
        return release_result["outcome"]

    monkeypatch.setattr(lease, "release_after_handoff_retry", release_and_observe)

    assert scheduler._schedule_external_worker_handoff_retry(
        lease,
        process=SimpleNamespace(pid=1234),
        execution_id="exec-real-registry",
        job_id="job-real-registry",
        handoff_files=(),
        stderr_path=tmp_path / "worker.stderr",
        custody=Mock(),
        dispatch_mode="direct",
        job={"id": "job-real-registry", "execution_id": "exec-real-registry"},
    ) is True
    assert waited.wait(timeout=5)
    assert released.wait(timeout=5)
    assert attempts["count"] >= 2
    assert transfer.fd is None
    assert retained_fd is not None
    assert getattr(release_result["outcome"], "status", None) == "released"
    store_paths = scope_seen.get("store")
    assert store_paths is not None
    assert store_paths.cron_dir == cron_dir
    assert scope_seen["hermes_home"] == tmp_path.resolve()
    assert scope_seen["execution_lookup"] is None
    assert tmp_path.resolve() / "cron" / "executions.db" in execution_db_paths
    assert lease.lease_id not in registry._active_leases


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_real_registry_closing_allows_accepted_worker_os_detach_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Close may release an accepted worker's OS lock, but not its active lease."""
    from gateway.runtime_cron_source_registry import CronAdmissionLease, CronSourceRegistry

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")

    registry = CronSourceRegistry(SimpleNamespace())
    monkeypatch.setattr(registry, "_validate_admission_locked", lambda *_args: None)
    monkeypatch.setattr(registry, "_validate_graph_locked", lambda: True)
    close_waiting = threading.Event()
    close_done = threading.Event()
    close_result: dict[str, object] = {}
    close_error: list[BaseException] = []
    original_wait = registry._wait_for_active_leases_locked

    def observe_close_wait(deadline):
        close_waiting.set()
        return original_wait(deadline)

    monkeypatch.setattr(registry, "_wait_for_active_leases_locked", observe_close_wait)
    lease = CronAdmissionLease(registry, "worker", tmp_path)

    with lease:
        def close_registry() -> None:
            try:
                close_result["value"] = registry.begin_close(
                    1, deadline=time.monotonic() + 5.0)
            except BaseException as error:
                close_error.append(error)
            finally:
                close_done.set()

        close_thread = threading.Thread(target=close_registry)
        close_thread.start()
        assert close_waiting.wait(timeout=5)
        assert registry.state == "closing"
        assert registry._active_leases.get(lease.lease_id) is lease

        detached = lease.detach_jobs_lock_after_handoff()
        assert detached.detached is True
        assert lease.active is True
        assert close_done.is_set() is False

        assert lease.release_after_handoff_retry().status == "released"

    close_thread.join(timeout=5)
    assert not close_thread.is_alive()
    assert close_error == []
    assert close_result["value"] is True
    assert registry.state == "closing"
    assert registry.abort_close(1) is True


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_real_registry_persistent_retry_deduplicates_and_retains_same_fd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One real registry retry consumer persists until the exact fd can detach."""
    from agent import memory_provider
    from cron import scheduler
    from gateway.runtime_cron_source_registry import CronAdmissionLease, CronSourceRegistry

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    original_release = jobs._release_handoff_transfer
    monkeypatch.setattr(
        jobs,
        "_release_handoff_transfer",
        lambda transfer: jobs._CronJobsLockHandoff("pending", transfer.key, transfer),
    )

    registry = CronSourceRegistry(SimpleNamespace())
    monkeypatch.setattr(registry, "_validate_admission_locked", lambda *_args: None)
    monkeypatch.setattr(registry, "_validate_graph_locked", lambda: True)
    lease = CronAdmissionLease(registry, "worker", tmp_path)
    with lease:
        assert lease.detach_jobs_lock_after_handoff().pending is True
    transfer = lease._handoff_transfer
    assert transfer is not None
    retained_fd = transfer.fd
    assert registry._active_leases.get(lease.lease_id) is lease

    started = threading.Event()
    completed = threading.Event()
    spawn_count = {"value": 0}
    original_spawn = memory_provider.spawn_context_thread

    def observe_spawn(target, *, name, daemon=True, args=(), kwargs=None):
        spawn_count["value"] += 1

        def wrapped():
            started.set()
            try:
                target(*args, **(kwargs or {}))
            finally:
                completed.set()

        return original_spawn(wrapped, name=name, daemon=daemon)

    monkeypatch.setattr(memory_provider, "spawn_context_thread", observe_spawn)
    monkeypatch.setattr(
        scheduler, "_wait_for_external_cron_worker",
        lambda *_args, **_kwargs: True,
    )
    retry_args = dict(
        process=SimpleNamespace(pid=1234),
        execution_id="exec-real-persistent",
        job_id="job-real-persistent",
        handoff_files=(),
        stderr_path=tmp_path / "worker.stderr",
        custody=Mock(),
        dispatch_mode="direct",
        job={"id": "job-real-persistent", "execution_id": "exec-real-persistent"},
    )
    assert scheduler._schedule_external_worker_handoff_retry(lease, **retry_args) is True
    assert started.wait(timeout=5)
    # The retry is still pending, so a second scheduler observation must reuse
    # the same registered consumer rather than starting another waiter.
    assert scheduler._schedule_external_worker_handoff_retry(lease, **retry_args) is True
    assert spawn_count["value"] == 1
    assert transfer.fd is retained_fd
    assert lease.active is True
    assert registry.begin_close(1, deadline=time.monotonic() + 0.03) is False

    monkeypatch.setattr(jobs, "_release_handoff_transfer", original_release)
    assert completed.wait(timeout=5)
    assert transfer.fd is None
    assert lease.active is False
    assert lease.lease_id not in registry._active_leases


@pytest.mark.skipif(jobs.fcntl is None, reason="POSIX lock-state control")
def test_real_registry_ambiguous_retry_start_retains_registration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An ambiguous start never falls back to a second retry consumer."""
    from agent import memory_provider
    from cron import scheduler
    from gateway.runtime_cron_source_registry import CronAdmissionLease, CronSourceRegistry

    cron_dir = tmp_path / "cron"
    monkeypatch.setattr(jobs, "CRON_DIR", cron_dir)
    monkeypatch.setattr(jobs, "JOBS_FILE", cron_dir / "jobs.json")
    monkeypatch.setattr(jobs, "OUTPUT_DIR", cron_dir / "output")
    original_release = jobs._release_handoff_transfer
    monkeypatch.setattr(
        jobs,
        "_release_handoff_transfer",
        lambda transfer: jobs._CronJobsLockHandoff("pending", transfer.key, transfer),
    )

    registry = CronSourceRegistry(SimpleNamespace())
    monkeypatch.setattr(registry, "_validate_admission_locked", lambda *_args: None)
    monkeypatch.setattr(registry, "_validate_graph_locked", lambda: True)
    lease = CronAdmissionLease(registry, "worker", tmp_path)
    with lease:
        assert lease.detach_jobs_lock_after_handoff().pending is True

    class AmbiguousStart:
        ident = None

        def start(self):
            raise RuntimeError("native start outcome unknown")

        def is_alive(self):
            return False

    thread = AmbiguousStart()
    spawn_count = {"value": 0}

    def ambiguous_spawn(*_args, **_kwargs):
        spawn_count["value"] += 1
        return thread

    monkeypatch.setattr(memory_provider, "spawn_context_thread", ambiguous_spawn)
    retry_args = dict(
        process=SimpleNamespace(pid=1234),
        execution_id="exec-ambiguous-start",
        job_id="job-ambiguous-start",
        handoff_files=(),
        stderr_path=tmp_path / "worker.stderr",
        custody=Mock(),
        dispatch_mode="direct",
        job={"id": "job-ambiguous-start", "execution_id": "exec-ambiguous-start"},
    )
    assert scheduler._schedule_external_worker_handoff_retry(lease, **retry_args) is True
    assert lease._handoff_retry_start_status == "unknown"
    assert lease._handoff_retry_registered is True
    assert registry._handoff_retry_threads[lease.lease_id] is thread
    assert scheduler._schedule_external_worker_handoff_retry(lease, **retry_args) is True
    assert spawn_count["value"] == 1

    # The synthetic thread cannot run; release the exact retained descriptor only
    # as test teardown after asserting the unknown-start retention contract.
    registry._handoff_retry_threads.pop(lease.lease_id, None)
    lease._handoff_retry_registered = False
    monkeypatch.setattr(jobs, "_release_handoff_transfer", original_release)
    assert lease.retry_jobs_lock_after_handoff().detached is True
    assert lease.release_after_handoff_retry().status == "released"


def _run_handoff(
    scheduler, tmp_path: Path, admission, *, ack: dict, custody=None,
):
    payload = tmp_path / "payload.json"
    payload.write_text("{}", encoding="utf-8")
    ack_path = tmp_path / "worker.ready"
    ack_path.write_text(json.dumps(ack), encoding="utf-8")
    stderr = tmp_path / "worker.stderr"
    process = SimpleNamespace(pid=1234, poll=lambda: None)
    dispatch = SimpleNamespace(mode="direct")
    return scheduler._await_external_worker_handoff(
        process,
        execution_id="exec-1",
        job_id="job-1",
        payload_path=payload,
        ack_path=ack_path,
        stderr_path=stderr,
        worker_custody=custody,
        dispatch=dispatch,
        worker_env={},
        admission=admission,
    )


def test_authenticated_handoff_detaches_before_terminal_wait(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cron import scheduler

    custody = Mock()
    admission = Mock()
    parent = object()
    admission.source_key = "worker"
    admission.parent = parent
    custody.parent = parent
    admission.detach_jobs_lock_after_handoff.return_value = SimpleNamespace(
        status="detached", detached=True,
    )
    wait = Mock(return_value=True)
    monkeypatch.setattr(scheduler, "_wait_for_external_cron_worker", wait)
    monkeypatch.setattr(scheduler, "_record_external_cron_worker", Mock())

    assert _run_handoff(
        scheduler, tmp_path, admission,
        ack={"execution_id": "exec-1", "pid": 1234}, custody=custody,
    ) is True
    custody.activate_external_worker.assert_called_once()
    admission.detach_jobs_lock_after_handoff.assert_called_once_with()
    wait.assert_called_once()


def test_pending_handoff_never_waits_under_the_parent_jobs_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cron import scheduler

    custody = Mock()
    admission = Mock()
    parent = object()
    admission.source_key = "worker"
    admission.parent = parent
    custody.parent = parent
    admission.detach_jobs_lock_after_handoff.return_value = SimpleNamespace(
        status="pending", detached=False,
    )
    wait = Mock(side_effect=AssertionError("wait ran while lock was retained"))
    monkeypatch.setattr(scheduler, "_wait_for_external_cron_worker", wait)

    with pytest.raises(scheduler._ExternalWorkerPostHandoffError, match="pending"):
        _run_handoff(
            scheduler, tmp_path, admission,
            ack={"execution_id": "exec-1", "pid": 1234}, custody=custody,
        )
    wait.assert_not_called()


def test_wrong_i12_parent_refuses_before_child_activation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cron import scheduler

    custody = Mock()
    custody.parent = object()
    admission = Mock()
    admission.source_key = "worker"
    admission.parent = object()
    wait = Mock(side_effect=AssertionError("wait ran after parent mismatch"))
    monkeypatch.setattr(scheduler, "_wait_for_external_cron_worker", wait)

    with pytest.raises(scheduler._ExternalWorkerPostHandoffError, match="parent/admission"):
        _run_handoff(
            scheduler, tmp_path, admission,
            ack={"execution_id": "exec-1", "pid": 1234}, custody=custody,
        )
    custody.activate_external_worker.assert_not_called()
    admission.detach_jobs_lock_after_handoff.assert_not_called()
    wait.assert_not_called()


def test_late_or_missing_ack_enters_post_handoff_recovery_without_waiting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from cron import scheduler

    monkeypatch.setattr(scheduler, "HANDOFF_ADOPTION_GRACE_SECONDS", 0)
    wait = Mock(side_effect=AssertionError("wait ran before authenticated handoff"))
    monkeypatch.setattr(scheduler, "_wait_for_external_cron_worker", wait)
    admission = Mock()
    custody = Mock()
    payload = tmp_path / "payload.json"
    payload.write_text("{}", encoding="utf-8")
    ack_path = tmp_path / "worker.ready"
    stderr = tmp_path / "worker.stderr"

    with pytest.raises(scheduler._ExternalWorkerPostHandoffError, match="timed out"):
        scheduler._await_external_worker_handoff(
            SimpleNamespace(pid=1234, poll=lambda: None),
            execution_id="exec-1", job_id="job-1", payload_path=payload,
            ack_path=ack_path, stderr_path=stderr, worker_custody=custody,
            dispatch=SimpleNamespace(mode="direct"), worker_env={}, admission=admission,
        )
    wait.assert_not_called()
