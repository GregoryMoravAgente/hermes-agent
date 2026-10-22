"""Real managed local sidecar admission and process binding controls."""

import json
import subprocess
import sys

import pytest

from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding


def _spawn_local_probe():
    return subprocess.Popen(
        [sys.executable, "-c", "import time; print('sidecar-ready', flush=True); time.sleep(.1)"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )


@pytest.mark.asyncio
async def test_managed_local_sidecar_reserves_and_binds_real_child(tmp_path, monkeypatch):
    """A local child must extend the real I04 admission before its Popen effect."""
    from gateway.runtime_service_custody import bind_turn, reset_turn
    from tools.environments.local_custody import (
        bind_local_process,
        finish_local_process,
        prepare_managed_local_sidecar,
    )

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    process = None
    try:
        binding = await fixture.claim("sidecar-local-admission")
        token = bind_turn(binding)
        try:
            owner = prepare_managed_local_sidecar(
                source_identity="terminal:sidecar-local-admission",
                operation_id="local-sidecar-admission",
            )
        finally:
            reset_turn(token)

        assert owner is not None
        assert owner.parent._service is owner.service
        assert owner.request is owner.service.requests[binding.admission_id]
        assert owner.parent.turn_binding is binding
        assert owner.handle is owner.service.handle
        assert owner.parent.profile_authority is binding.authority

        process = _spawn_local_probe()
        assert bind_local_process(owner, process) is True
        assert owner.process is process
        assert owner.physical.process is process
        assert owner.physical.pid == process.pid
        assert owner.physical.scope == "posix_process_group"
        assert owner.physical.scope_identity.startswith(
            f"posix_process_group:{process.pid}:"
        )
        assert process.stdout.readline().strip() == "sidecar-ready"
        assert process.wait(timeout=5) == 0

        # Local result/readback is not yet a typed durable I16 completion.  The
        # production consumer must retain the exact physical receipt as UNKNOWN.
        finish_local_process(owner)
        assert owner.physical.process is process
        assert owner.physical.state == "unknown"
        assert owner.physical.unknown_reason == "local_readback_unavailable"
        assert owner.parent.state == "unknown"
        assert owner.handle in {
            record.handle
            for record in owner.coordinator._records.values()
            if record.handle is not None
        }
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        fixture.cleanup()


@pytest.mark.asyncio
async def test_public_terminal_binds_real_managed_local_child(tmp_path, monkeypatch):
    """The public terminal consumer carries the real turn binding into its spawn worker."""
    from gateway.runtime_service_custody import bind_turn, reset_turn
    from tools import terminal_tool
    from tools.environments import local_custody

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    observed = []
    original_prepare = local_custody.prepare_managed_local_sidecar

    def observe_prepare(**kwargs):
        owner = original_prepare(**kwargs)
        if owner is not None:
            observed.append(owner)
        return owner

    monkeypatch.setattr(local_custody, "prepare_managed_local_sidecar", observe_prepare)
    monkeypatch.setenv("TERMINAL_ENV", "local")
    monkeypatch.setenv("TERMINAL_CWD", str(tmp_path))
    try:
        binding = await fixture.claim("sidecar-public-terminal")
        token = bind_turn(binding)
        try:
            result = json.loads(
                terminal_tool.terminal_tool(
                    command="printf public-terminal",
                    task_id="sidecar-public-terminal",
                    timeout=10,
                    force=True,
                )
            )
        finally:
            reset_turn(token)

        assert result["exit_code"] == 0
        assert result["output"].strip() == "public-terminal"
        assert len(observed) == 1
        owner = observed[0]
        assert owner.parent.turn_binding is binding
        assert owner.process is not None
        assert owner.physical is not None
        assert owner.physical.process is owner.process
        assert owner.physical.state == "unknown"
        assert owner.parent.state == "unknown"
    finally:
        fixture.cleanup()
