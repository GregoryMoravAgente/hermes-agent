"""Managed authority callback faults retain custody before the producer task boundary."""

import asyncio
import json
from pathlib import Path
import sys

import pytest


@pytest.mark.asyncio
async def test_real_authority_consumer_keyboard_interrupt_is_observed_before_occurrence_return(
    tmp_path,
):
    proof_path = tmp_path / "authority-keyboardinterrupt-proof.json"
    child_tmp = tmp_path / "authority-keyboardinterrupt-child"
    child_tmp.mkdir()
    repo_root = Path(__file__).resolve().parents[2]
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "tests.gateway.fixtures.classic_callback_fault_observer",
        "--run-authority-keyboardinterrupt",
        "--tmp-path",
        str(child_tmp),
        "--proof",
        str(proof_path),
        cwd=repo_root,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await child.communicate()
    output = stdout.decode() + stderr.decode()
    assert child.returncode != 0
    assert "KeyboardInterrupt" in output
    assert proof_path.is_file()
    proof = json.loads(proof_path.read_text())
    assert proof["process_error_type"] == "KeyboardInterrupt"
    assert proof["fault_exception_type"] == "KeyboardInterrupt"
    assert proof["fault_before_production_occurrence"] is True
    assert proof["consumer_path_observed"] is True
    assert proof["same_task_occurrence_and_fault"] is True
    assert proof["third_queue"]["accepted"] is True
    assert proof["third_queue"]["durable_queued"] is True
    assert proof["third_queue"]["owner_class"] == "I06_busy_followup_delivery"
    assert proof["cleanup"]["third_model_started_before_reconciliation"] is False
    assert proof["cleanup"]["third_delivery_started_before_reconciliation"] is False
    second_records = [
        record for record in proof["records"]
        if record.get("request_id") == "authority-inbound-2"
    ]
    assert len(second_records) == 1
    assert second_records[0]["unknown"] is True
