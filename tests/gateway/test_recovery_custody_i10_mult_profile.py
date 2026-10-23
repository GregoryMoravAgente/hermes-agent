"""Causal close-time I10 aggregation across real profile authorities."""

import pytest

from tests.gateway.test_runtime_custody_profile_epochs import _boot, _close_boot


async def _begin_close(runner):
    from gateway import run_runtime
    from gateway.runtime_custody import DrainReceipt

    runner._running = True
    runner._restart_requested = False
    runner._stop_task = None
    descriptor = runner.session_runtime_descriptor
    params = {
        "instance_id": descriptor["instance_id"],
        "authority_epoch": descriptor["authority_epoch"],
        "owner_id": "i10-real-profile-close",
        "scope": "profile",
        "deadline_s": 30,
    }
    receipt = await run_runtime.begin_preservation_drain(runner, params, "i10-test-peer")
    assert receipt["status"] == "closing", receipt
    return params, receipt, DrainReceipt(
        receipt["receipt_id"], receipt["process_instance"], receipt["authority_epoch"],
        receipt["close_generation"], receipt["status"],
    )


@pytest.mark.asyncio
async def test_i10_real_bootstrap_close_seals_two_profile_epochs(tmp_path, monkeypatch):
    from gateway import run_runtime

    runner, entries = await _boot(tmp_path, monkeypatch)
    captured = []
    close_receipt = None
    original_seal = runner.runtime_custody.seal_owner

    def capture(owner_class, evidence):
        if owner_class == "I10_startup_recovery_handoff":
            captured.append(evidence)
        return original_seal(owner_class, evidence)

    runner.runtime_custody.seal_owner = capture
    try:
        authorities = list(runner.session_authorities)
        assert len(authorities) == 2
        assert authorities[0].epoch != authorities[1].epoch
        assert all(
            set(authority._i10_source_registry) == {
                "startup", "local_recovery", "webhook_replay", "adapter_reconnect",
                "completion_notification", "bot_mailbox",
            }
            for authority in authorities
        )
        params, wire, close_receipt = await _begin_close(runner)
        await run_runtime.prepare_preservation_handoff(runner, {
            "receipt": wire,
        }, "i10-test-peer")
        status = runner.runtime_custody.status()
        assert status["owner_states"]["I10_startup_recovery_handoff"] == "sealed", status
        assert captured
        evidence = captured[-1]
        assert len(evidence["profiles"]) == 2
        assert set(evidence["profile_ids"]) == {authority.profile_id for authority in authorities}
        assert {epoch for _profile, epoch in evidence["profile_epochs"]} == {
            authority.epoch for authority in authorities
        }
        assert all(len(proof["consumers"]) == 6 for proof in evidence["profiles"])
        assert run_runtime._preservation_receipt({"receipt": wire}) is not None
        assert params["authority_epoch"] == runner.runtime_custody.authority_epoch
    finally:
        if runner.runtime_custody.status()["state"] == "closing" and close_receipt is not None:
            runner.runtime_custody.abort_before_handoff(close_receipt)
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i10_missing_source_registration_stays_unknown(tmp_path, monkeypatch):
    from gateway import run_runtime

    runner, entries = await _boot(tmp_path, monkeypatch)
    close_receipt = None
    try:
        del runner.session_authorities.launch._i10_source_registry["startup"]
        _params, wire, close_receipt = await _begin_close(runner)
        await run_runtime.prepare_preservation_handoff(runner, {"receipt": wire}, "i10-test-peer")
        status = runner.runtime_custody.status()
        assert status["owner_states"]["I10_startup_recovery_handoff"] == "unknown"
        assert status["close_generation"] == close_receipt.close_generation
    finally:
        if runner.runtime_custody.status()["state"] == "closing" and close_receipt is not None:
            runner.runtime_custody.abort_before_handoff(close_receipt)
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i10_source_read_failure_stays_unknown(tmp_path, monkeypatch):
    from gateway import run_runtime

    runner, entries = await _boot(tmp_path, monkeypatch)
    close_receipt = None
    try:
        def fail_probe(_authority):
            raise RuntimeError("source enumeration failed")

        runner.session_authorities.launch._i10_source_registry["completion_notification"]["probe"] = fail_probe
        _params, wire, close_receipt = await _begin_close(runner)
        await run_runtime.prepare_preservation_handoff(runner, {"receipt": wire}, "i10-test-peer")
        assert runner.runtime_custody.status()["owner_states"]["I10_startup_recovery_handoff"] == "unknown"
    finally:
        if runner.runtime_custody.status()["state"] == "closing" and close_receipt is not None:
            runner.runtime_custody.abort_before_handoff(close_receipt)
        assert await _close_boot(runner, entries) is True


@pytest.mark.asyncio
async def test_i10_late_source_work_stays_unknown_with_close_handle(tmp_path, monkeypatch):
    from gateway import run_runtime

    runner, entries = await _boot(tmp_path, monkeypatch)
    close_receipt = None
    try:
        runner.session_authorities.launch._i10_source_registry["adapter_reconnect"]["probe"] = (
            lambda _authority: {"pending_ids": ("late-adapter-event",), "rows": ()}
        )
        _params, wire, close_receipt = await _begin_close(runner)
        close_generation = close_receipt.close_generation
        await run_runtime.prepare_preservation_handoff(runner, {"receipt": wire}, "i10-test-peer")
        status = runner.runtime_custody.status()
        assert status["owner_states"]["I10_startup_recovery_handoff"] == "unknown"
        assert status["state"] == "closing"
        assert status["close_generation"] == close_generation
    finally:
        if runner.runtime_custody.status()["state"] == "closing" and close_receipt is not None:
            runner.runtime_custody.abort_before_handoff(close_receipt)
        assert await _close_boot(runner, entries) is True
