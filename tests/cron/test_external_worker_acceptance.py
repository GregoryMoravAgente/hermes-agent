"""Causal parent acceptance controls for restart-safe external Cron workers."""
from __future__ import annotations

import json
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace

import pytest

from cron.scheduler_worker_identity import (
    WorkerIdentityUnavailable,
    wait_for_worker_acceptance,
    worker_ack_matches_generation,
    write_worker_acceptance,
)
from gateway.runtime_custody import RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence


@pytest.fixture
def acceptance_runtime(monkeypatch, tmp_path):
    from cron import executions, runtime_custody, scheduler
    import hermes_constants

    db = tmp_path / "cron" / "executions.db"
    monkeypatch.setattr(executions, "EXECUTIONS_FILE", db)
    monkeypatch.setattr(hermes_constants, "get_hermes_home", lambda: tmp_path)
    monkeypatch.setattr(scheduler, "_get_hermes_home", lambda: tmp_path)
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    coordinator = RuntimeCustodyCoordinator(
        process_instance="cron-acceptance-test", authority_epoch=17, fence=RetirementFence())
    runner = SimpleNamespace(
        config=SimpleNamespace(
            multiplex_profiles=True,
            _runtime_profile_homes=[("cron-acceptance", tmp_path)],
        ),
        _primary_profile_name="cron-acceptance",
    )
    monkeypatch.setattr(
        runtime_custody,
        "_runtime",
        lambda: (
            runner,
            coordinator,
            {"instance_id": "cron-acceptance-test", "authority_epoch": 17},
            tmp_path,
            17,
        ),
    )
    try:
        yield coordinator, executions, tmp_path
    finally:
        with runtime_custody._BINDINGS_LOCK:
            runtime_custody._BINDINGS_BY_EXECUTION.clear()
        with runtime_custody._SCOPE_PROOF_LOCK:
            runtime_custody._SETTLED_SCOPE_PROOFS.clear()


def test_generation_and_acceptance_reject_alias_types_and_extra_keys(tmp_path):
    expected = {
        "process_instance": "cron-acceptance-test",
        "authority_epoch": 17,
        "close_generation": 0,
    }
    for mutation in (
        {**expected, "authority_epoch": True},
        {**expected, "authority_epoch": 17.0},
        {**expected, "close_generation": False},
        {**expected, "close_generation": 0.0},
        {**expected, "unexpected": "field"},
    ):
        assert not worker_ack_matches_generation(
            {"custody_generation": mutation}, expected)

    acknowledgement = {
        "handoff_nonce": "nonce-1",
        "execution_id": "execution-1",
        "pid": 4312,
        "process_started_at": 9876,
        "process_creation_filetime": None,
        "dispatch_mode": "direct",
        "scope_unit": None,
        "scope_control_group": None,
        "custody_generation": expected,
    }
    accept = tmp_path / "execution.accept"
    write_worker_acceptance(accept, acknowledgement, expected_generation=expected)
    observed = json.loads(accept.read_text(encoding="utf-8"))
    assert set(observed) == {
        "decision",
        "handoff_nonce",
        "execution_id",
        "pid",
        "process_started_at",
        "process_creation_filetime",
        "dispatch_mode",
        "scope_unit",
        "scope_control_group",
        "custody_generation",
    }
    observed["custody_generation"] = {**expected, "close_generation": 0.0}
    accept.write_text(json.dumps(observed), encoding="utf-8")
    assert not wait_for_worker_acceptance(
        accept, acknowledgement, timeout=2)

    windows_ack = {**acknowledgement, "process_creation_filetime": 134367929582396490}
    write_worker_acceptance(accept, windows_ack, expected_generation=expected)
    observed = json.loads(accept.read_text(encoding="utf-8"))
    observed["process_creation_filetime"] = 134367929582396490.0
    accept.write_text(json.dumps(observed), encoding="utf-8")
    assert not wait_for_worker_acceptance(accept, windows_ack, timeout=2)

    observed = json.loads(
        write_acceptance_for_test(accept, acknowledgement, expected)
    )
    observed["unexpected"] = True
    accept.write_text(json.dumps(observed), encoding="utf-8")
    assert not wait_for_worker_acceptance(
        accept, acknowledgement, timeout=2)

    # The ready ACK has no extension point: unknown fields, including the parent-only decision,
    # must be refused before the acceptance file can erase their provenance.
    for mutation in (
        {**acknowledgement, "unexpected": True},
        {**acknowledgement, "decision": "accepted"},
    ):
        assert not worker_ack_matches_generation(mutation, expected)
        with pytest.raises(WorkerIdentityUnavailable, match="identity_mismatch"):
            write_worker_acceptance(accept, mutation, expected_generation=expected)


def write_acceptance_for_test(path: Path, acknowledgement: dict, expected: dict) -> str:
    write_worker_acceptance(path, acknowledgement, expected_generation=expected)
    return path.read_text(encoding="utf-8")


def test_external_worker_cli_consuming_gate_precedes_real_job_side_effect(
    acceptance_runtime, monkeypatch
):
    _coordinator, executions, home = acceptance_runtime
    from cron import jobs, scheduler
    from cron.runtime_custody import reserve_execution
    from tools.process_registry import GatewayChildDispatch
    from cron.runtime_custody import CronOwnerBinding

    scripts = home / "scripts"
    scripts.mkdir(parents=True, exist_ok=True)
    marker = home / "accepted-job.marker"
    script = scripts / "acceptance-job.py"
    script.write_text(
        "from pathlib import Path\n"
        f"Path({str(marker)!r}).write_text('ran', encoding='utf-8')\n",
        encoding="utf-8",
    )
    job = jobs.create_job(
        prompt=None,
        schedule="every 1h",
        name="acceptance gate",
        script=script.name,
        no_agent=True,
        deliver="local",
    )
    binding = reserve_execution(
        owner_class="I12_cron_fire",
        job_id=job["id"],
        source="builtin",
        required=True,
    )
    record = executions.create_execution(
        job["id"], source="builtin", execution_id=binding.owner_id)
    binding.bind_execution_record(record)
    job = dict(job, execution_id=binding.execution_id, _cron_custody=binding)

    monkeypatch.setattr(
        "tools.process_registry.restart_safe_gateway_child_argv",
        lambda command, **_: GatewayChildDispatch("degraded", command),
    )
    activated = Event()
    release = Event()
    launch_result = []
    real_activate = CronOwnerBinding.activate_external_worker

    def hold_after_real_activation(self, acknowledgement, process, **kwargs):
        result = real_activate(self, acknowledgement, process, **kwargs)
        activated.set()
        assert release.wait(timeout=10), "parent acceptance release was not observed"
        return result

    monkeypatch.setattr(
        CronOwnerBinding,
        "activate_external_worker",
        hold_after_real_activation,
    )
    launcher = Thread(
        target=lambda: launch_result.append(
            scheduler._launch_external_cron_worker(job)),
        daemon=True,
    )
    launcher.start()
    try:
        assert activated.wait(timeout=15), "real parent activation did not reach the gate"
        assert not marker.exists(), "job side effect occurred before parent acceptance"
        row = executions.get_execution(binding.execution_id)
        assert isinstance(row, dict)
        assert row["status"] == "running"
        release.set()
        launcher.join(timeout=30)
        assert not launcher.is_alive()
        assert launch_result == [True]
        assert marker.read_text(encoding="utf-8") == "ran"
        final = executions.get_execution(binding.execution_id)
        assert isinstance(final, dict)
        assert final["status"] == "completed"
    finally:
        release.set()
        launcher.join(timeout=10)
        if launcher.is_alive():
            raise AssertionError("external worker launcher did not settle")
