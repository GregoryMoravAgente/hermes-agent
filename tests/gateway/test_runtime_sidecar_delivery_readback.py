"""Readback and EOF boundaries for real local sidecar streams."""

import subprocess
import sys
import threading

import pytest

from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding


def _spawn_local_readback_probe():
    return subprocess.Popen(
        [sys.executable, "-c", "import sys; sys.stdout.write('readback\\n'); sys.stdout.flush()"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )


@pytest.mark.asyncio
async def test_local_sidecar_retains_exact_reader_when_delivery_is_unproven(tmp_path, monkeypatch):
    """EOF on the real pipe is recorded while durable delivery remains UNKNOWN."""
    from gateway.runtime_service_custody import bind_turn, reset_turn
    from tools.environments.local_custody import (
        bind_local_process,
        finish_local_process,
        prepare_managed_local_sidecar,
    )
    from gateway.runtime_sidecar_sources import bind_reader, mark_process_finished

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    process = None
    reader = None
    try:
        binding = await fixture.claim("sidecar-local-readback")
        token = bind_turn(binding)
        try:
            owner = prepare_managed_local_sidecar(
                source_identity="terminal:sidecar-local-readback",
                operation_id="local-sidecar-readback",
            )
        finally:
            reset_turn(token)
        assert owner is not None

        process = _spawn_local_readback_probe()
        assert bind_local_process(owner, process) is True

        captured = []
        reader = threading.Thread(
            target=lambda: captured.append(process.stdout.read()),
            name="i16-local-reader",
            daemon=False,
        )
        bind_reader(owner, "stdout", reader)
        reader.start()
        assert process.wait(timeout=5) == 0
        reader.join(timeout=5)
        assert not reader.is_alive()
        assert captured == ["readback\n"]

        # The source adapter receives the exact reader join/readback boundary.  The
        # local consumer then keeps that result UNKNOWN because no durable delivery
        # receipt exists for this first milestone.
        mark_process_finished(
            owner, reader=reader, reason="local_delivery_readback_unavailable",
        )
        finish_local_process(owner, reason="local_delivery_readback_unavailable")
        physical = owner.physical
        assert physical is not None
        assert physical.readers["stdout"] is reader
        assert physical.eof_streams == {"stdout"}
        assert physical.joined_streams == {"stdout"}
        assert physical.state == "unknown"
        assert physical.unknown_reason == "local_delivery_readback_unavailable"
        assert owner.parent.state == "unknown"
    finally:
        if reader is not None:
            reader.join(timeout=5)
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        fixture.cleanup()
