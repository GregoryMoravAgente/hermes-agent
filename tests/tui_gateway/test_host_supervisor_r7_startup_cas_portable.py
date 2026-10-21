"""Portable causal RED/GREEN oracle for the R7 startup timeout/CAS race."""
from __future__ import annotations

from pathlib import Path
import threading

import pytest

from tests.tui_gateway.r7_startup_cas_portable_controller import (
    posix_startup_record,
    release_and_join,
    seed_startup_boundary,
    wait_for_pid_read,
)


@pytest.mark.platforms("not windows")
def test_startup_abort_rejects_late_pid_binding_without_stale_identity(tmp_path: Path):
    """The same selector is RED on R7 and GREEN after the R8 cancellation CAS.

    The existing POSIX startup branch is used naturally. The worker passes the
    current entry generation check, then blocks at the actual Popen ``pid``
    property before publishing startup identity. The main thread invokes the
    existing supervisor release path while that read is blocked. R7 releases
    owner custody and later publishes the stale identity; R8 must cancel the
    generation, refuse the late bind, and leave no stale producer state.
    """
    supervisor, process, owner = seed_startup_boundary(tmp_path)
    accepted: list[bool] = []
    failures: list[BaseException] = []

    def accept_startup() -> None:
        try:
            accepted.append(
                supervisor._accept_producer_startup(
                    posix_startup_record(process.value),
                    proc=process,
                    host_generation=7,
                )
            )
        except BaseException as exc:  # retain the causal worker failure for the assertion
            failures.append(exc)

    worker = threading.Thread(target=accept_startup, name="portable-r7-startup-bind")
    worker.start()
    try:
        wait_for_pid_read(process)

        # This is the real current release path. It does not acquire hello_lock,
        # which is the boundary that exposes the old entry-only CAS.
        supervisor._release_owner()

        release_and_join(process, worker)
        assert failures == []
        # R7 returns True and publishes _startup_identity here: that is the
        # expected causal RED. R8 must return False and leave it unset.
        assert accepted == [False]
        assert supervisor._startup_identity is None
        assert supervisor._producer_process_handle is None
        assert supervisor._release_owner() is True
        assert owner.close_called.is_set()
        assert supervisor._owner is None
    finally:
        process.allow_pid_read.set()
        worker.join(timeout=10)
