"""Source-owned controls for ambiguous unmanaged cron-worker handoffs.

The original restart-safe worker-death evidence file remains unchanged.  These
controls cover the legacy/no-runtime seam added by the bounded receiving proposal:
the exact Popen and profile admission stay available when ACK parsing or release
fails, while a mismatched ACK never becomes an ownership proof.
"""

import json
import subprocess
import sys
from types import SimpleNamespace

import pytest


class _Admission:
    source_key = "worker"
    parent = None

    def __init__(self, home, status):
        self.home = home
        self.status = status
        self.release_calls = 0

    def release_after_handoff_retry(self):
        self.release_calls += 1
        return SimpleNamespace(status=self.status)


def _handoff_paths(tmp_path):
    return (
        tmp_path / "payload.json",
        tmp_path / "worker.ready",
        tmp_path / "worker.stderr",
    )


def _await(scheduler, *, process, execution_id, paths, admission):
    payload_path, ack_path, stderr_path = paths
    return scheduler._await_external_worker_handoff(
        process,
        execution_id=execution_id,
        job_id="job-1",
        payload_path=payload_path,
        ack_path=ack_path,
        stderr_path=stderr_path,
        worker_custody=None,
        dispatch=SimpleNamespace(mode="degraded"),
        worker_env={},
        admission=admission,
    )


def test_unmanaged_ack_parse_failure_retains_exact_wait_context(tmp_path):
    from cron import scheduler

    process = subprocess.Popen([sys.executable, "-c", "pass"])
    try:
        paths = _handoff_paths(tmp_path)
        paths[1].write_text("{", encoding="utf-8")
        admission = _Admission(tmp_path, "refused")

        with pytest.raises(scheduler._ExternalWorkerPostHandoffError) as raised:
            _await(
                scheduler,
                process=process,
                execution_id="execution-parse",
                paths=paths,
                admission=admission,
            )

        error = raised.value
        assert error.admission is admission
        assert error.wait_context["legacy_unmanaged"] is True
        assert error.wait_context["process"] is process
        assert error.wait_context["execution_id"] == "execution-parse"
        assert error.wait_context["handoff_files"] == paths
    finally:
        process.wait(timeout=30)


def test_unmanaged_mismatched_ack_releases_before_existing_waiter(
    tmp_path, monkeypatch,
):
    from cron import scheduler

    process = subprocess.Popen([sys.executable, "-c", "pass"])
    try:
        paths = _handoff_paths(tmp_path)
        paths[1].write_text(
            json.dumps({"execution_id": "foreign-execution", "pid": 999999}),
            encoding="utf-8",
        )
        admission = _Admission(tmp_path, "released")
        observed = {}

        def wait(process, **kwargs):
            observed["process"] = process
            observed.update(kwargs)
            return True

        monkeypatch.setattr(scheduler, "_wait_for_external_cron_worker", wait)
        assert _await(
            scheduler,
            process=process,
            execution_id="execution-owned",
            paths=paths,
            admission=admission,
        ) is True
        assert admission.release_calls == 1
        assert observed["process"] is process
        assert observed["admission"] is None
        assert observed["custody"] is None
    finally:
        process.wait(timeout=30)


def test_unmanaged_authenticated_ack_uses_spawned_process_identity(
    tmp_path, monkeypatch,
):
    from cron import scheduler

    process = subprocess.Popen([sys.executable, "-c", "pass"])
    try:
        paths = _handoff_paths(tmp_path)
        paths[1].write_text(
            json.dumps({"execution_id": "execution-owned", "pid": 999999}),
            encoding="utf-8",
        )
        admission = _Admission(tmp_path, "released")
        recorded = []
        monkeypatch.setattr(
            scheduler,
            "_record_external_cron_worker",
            lambda *args, **kwargs: recorded.append((args, kwargs)),
        )
        monkeypatch.setattr(
            scheduler,
            "_wait_for_external_cron_worker",
            lambda process, **kwargs: True,
        )

        assert _await(
            scheduler,
            process=process,
            execution_id="execution-owned",
            paths=paths,
            admission=admission,
        ) is True
        assert recorded == [
            (("job-1", process.pid), {"scope_isolated": False}),
        ]
    finally:
        process.wait(timeout=30)
