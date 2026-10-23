"""Consumer contracts for physical profile ownership and finalizer custody.

These controls use the real temporary profile lock and descriptor.  A PID, boolean owner flag,
or fabricated owner map is never accepted as an I20 positive.
"""

import json
import threading
from types import SimpleNamespace

import pytest


def test_i20_evidence_requires_current_descriptor_and_physical_epoch(tmp_path):
    from gateway import runtime_ownership

    home = tmp_path / "profile"
    owner = runtime_ownership.ProfileOwnership()
    original_record = None
    owner.reserve([home])
    try:
        evidence = owner.reservation_evidence(home)
        assert evidence is not None
        assert evidence["native_handle_retained"] is True
        assert evidence["os_lock_owner"] is True
        assert evidence["cross_process_proof"] is True
        assert evidence["physical_binding"] == (
            str(runtime_ownership.canonical_home(home)),
            evidence["owner_epoch"], evidence["native_descriptor"],
        )
        epoch = evidence["owner_epoch"]
        assert owner.reservation_is_current(home, epoch)
        original_record = json.loads((home / "gateway.lock").read_text(encoding="utf-8"))

        (home / "gateway.lock").write_text(
            '{"pid": 1, "kind": "hermes-gateway", "hermes_home": "foreign", '
            '"owner_epoch": "replacement"}',
            encoding="utf-8",
        )
        assert owner.reservation_evidence(home) is None
        assert owner.reservation_is_current(home, epoch) is False
    finally:
        # Restore the record only for the same descriptor's final cleanup; no successor is
        # allowed to use the tampered record as evidence.
        if owner.homes and original_record is not None:
            (home / "gateway.lock").write_text(json.dumps(original_record), encoding="utf-8")
        owner.close()



def test_i20_evidence_rejects_same_epoch_tuple_tamper_from_lock_record(tmp_path):
    from gateway import runtime_ownership

    home = tmp_path / "profile"
    owner = runtime_ownership.ProfileOwnership()
    owner.reserve([home])
    try:
        original = owner.reservation_evidence(home)
        assert original is not None
        epoch = original["owner_epoch"]
        record = original["record"]
        for field, value in ((
            "pid", int(record["pid"]) + 1),
            ("kind", "foreign-gateway"),
            ("start_time", int(record["start_time"]) + 1),
            ("hermes_home", str(tmp_path / "foreign")),
        ):
            tampered = dict(record)
            tampered["owner_epoch"] = epoch
            tampered[field] = value
            (home / "gateway.lock").write_text(json.dumps(tampered), encoding="utf-8")
            assert owner.reservation_evidence(home) is None
            (home / "gateway.lock").write_text(json.dumps(record), encoding="utf-8")
            # Restore the same retained descriptor's record and prove it can recover.
            assert owner.reservation_evidence(home) is not None
    finally:
        owner.close()

def test_finalizer_hold_is_same_object_and_terminal_before_release(tmp_path):
    from gateway import runtime_ownership

    class Finalizer:
        state = "closing"

    home = tmp_path / "profile"
    owner = runtime_ownership.ProfileOwnership()
    token = Finalizer()
    copied = Finalizer()
    owner.reserve([home])
    try:
        owner.retain_finalizer_hold(home, token)
        assert owner.release_outcome(home) is runtime_ownership.ProfileReleaseOutcome.UNKNOWN_LOST_EXCLUSION
        assert owner.release(home) is False
        assert owner.release_finalizer_hold(home, copied) is False
        token.state = "terminal"
        assert owner.release_finalizer_hold(home, token) is True
        assert owner.homes == ()
    finally:
        owner.close()


def test_live_writer_keeps_release_summary_unknown_until_writer_stops(tmp_path):
    from gateway import runtime_ownership

    home = tmp_path / "profile"
    owner = runtime_ownership.ProfileOwnership()
    owner.reserve([home])
    stopped = threading.Event()
    writer = threading.Thread(target=stopped.wait, daemon=True)
    owner.start_writer(writer)
    try:
        summary = owner.close()
        assert summary.failed is True
        assert summary.writers_alive is True
        assert home in owner.homes
    finally:
        stopped.set()
        writer.join(timeout=2)
        assert owner.close().failed is False


@pytest.mark.asyncio
async def test_authenticated_preservation_route_seals_i20_from_real_profile_lock(tmp_path, monkeypatch):
    from gateway import run_runtime
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.runtime_ownership import process_ownership
    from hermes_cli.backend_retirement import RetirementFence
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    home = (tmp_path / "profile").resolve()
    monkeypatch.setenv("HERMES_HOME", str(home))
    home_token = set_hermes_home_override(home)
    runner = SimpleNamespace(
        config=SimpleNamespace(multiplex_profiles=False),
        _primary_profile_name="default",
        _running=True,
        _restart_requested=False,
        _stop_task=None,
        session_runtime_descriptor={"instance_id": "route-instance", "authority_epoch": 7},
        runtime_custody=RuntimeCustodyCoordinator(
            process_instance="route-instance", authority_epoch=7, fence=RetirementFence(),
        ),
        adapters={},
    )
    params = {
        "instance_id": "route-instance", "authority_epoch": 7,
        "owner_id": "route-owner", "scope": "profile", "deadline_s": 30,
    }
    try:
        process_ownership.reserve([home])
        closing = await run_runtime.begin_preservation_drain(runner, params, "authenticated-peer")
        assert closing["status"] == "closing"
        prepared = await run_runtime.prepare_preservation_handoff(
            runner, {"receipt": closing}, "authenticated-peer",
        )
        assert prepared["status"] == "refused"
        assert runner.runtime_custody.status()["owner_states"][
            "I20_runtime_ownership_and_foreign_handoff"
        ] == "sealed"
    finally:
        process_ownership.close()
        reset_hermes_home_override(home_token)
