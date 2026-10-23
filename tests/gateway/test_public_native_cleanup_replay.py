"""Public native cleanup retries retain one plan after an earlier profile lock closes."""

from collections import Counter

from tests.gateway import test_public_three_profile_native_handoff as public_handoff_fixtures

authenticated_started_mux = public_handoff_fixtures.authenticated_started_mux


def test_public_partial_profile_close_retries_exact_closed_receipt(
    authenticated_started_mux, monkeypatch,
):
    from gateway import run_runtime
    from gateway.run_bootstrap import _start_gateway_start_cron_and_housekeeping
    from gateway.run import (
        _CRON_SHUTDOWN_DRAIN_TIMEOUT,
        _HOUSEKEEPING_SHUTDOWN_DRAIN_TIMEOUT,
        _await_thread_exit,
        _stop_cron_provider,
    )
    from gateway.runtime_ownership import canonical_home, process_ownership

    runner, homes, _loop, call = authenticated_started_mux

    async def start_cron():
        return _start_gateway_start_cron_and_housekeeping(runner)

    cron_stop, provider, cron_thread, housekeeping_thread = call(start_cron())
    try:
        call(run_runtime.recover_gateway_native_sessions(runner))
        descriptor = runner.session_runtime_descriptor
        wire = call(run_runtime.begin_preservation_drain(runner, {
            "instance_id": descriptor["instance_id"],
            "authority_epoch": descriptor["authority_epoch"],
            "owner_id": "public-partial-native-retry",
            "scope": "process",
            "deadline_s": 30,
        }, "fixture-peer"))
        assert wire["status"] == "closing", wire
        prepared = call(run_runtime.prepare_preservation_handoff(
            runner, {"receipt": wire}, "fixture-peer",
        ))
        assert prepared["status"] == "handoff_pending", prepared
        with process_ownership._mutex:
            handles = {
                canonical_home(home): process_ownership._handles[canonical_home(home)]
                for home in homes.values()
            }
        assert len(handles) == 3
        original_release = process_ownership.release_outcome
        attempts = []
        effects = Counter()

        def refuse_second_release_once(home):
            home = canonical_home(home)
            attempts.append(home)
            if len(attempts) == 2:
                raise OSError("synthetic pre-effect profile release refusal")
            outcome = original_release(home)
            if outcome.value == "released":
                effects[home] += 1
            return outcome

        monkeypatch.setattr(process_ownership, "release_outcome", refuse_second_release_once)
        first = call(run_runtime.acknowledge_preservation_native(
            runner, {"receipt": prepared}, "fixture-peer",
        ))
        assert first["status"] == "closed", first
        assert first["cleanup"] == "unknown", first
        assert len(attempts) == 2
        assert handles[attempts[0]].closed is True
        assert sum(not handle.closed for handle in handles.values()) == 2
        coordinator = runner.runtime_custody
        plan = coordinator._native_cleanup_plan
        pending = tuple(coordinator._native_cleanup_pending_participants)
        assert len(pending) == 2

        # A changed close identity cannot spend the retained native cleanup capability.
        for field, value in (
            ("receipt_id", first["receipt_id"] + "-stale"),
            ("process_instance", first["process_instance"] + "-foreign"),
            ("authority_epoch", first["authority_epoch"] + 1),
            ("close_generation", first["close_generation"] + 1),
        ):
            altered = {**first, field: value}
            refused = call(run_runtime.acknowledge_preservation_native(
                runner, {"receipt": altered}, "fixture-peer",
            ))
            assert refused["status"] == "refused", refused
            assert len(attempts) == 2
            assert coordinator._native_cleanup_plan is plan
            assert coordinator._native_cleanup_pending_participants == pending
            assert sum(not handle.closed for handle in handles.values()) == 2

        completed = call(run_runtime.acknowledge_preservation_native(
            runner, {"receipt": first}, "fixture-peer",
        ))
        assert completed["status"] == "closed", completed
        assert completed["cleanup"] == "complete", completed
        assert coordinator._native_cleanup_plan is plan
        assert all(handle.closed for handle in handles.values())
        assert effects == Counter({home: 1 for home in handles})
        before_replay = list(attempts)
        replay = call(run_runtime.acknowledge_preservation_native(
            runner, {"receipt": completed}, "fixture-peer",
        ))
        assert replay["cleanup"] == "complete", replay
        assert attempts == before_replay
        assert coordinator._native_cleanup_plan is plan
    finally:
        cron_stop.set()
        _stop_cron_provider(provider)
        assert call(_await_thread_exit(cron_thread, timeout=_CRON_SHUTDOWN_DRAIN_TIMEOUT))
        assert call(_await_thread_exit(
            housekeeping_thread, timeout=_HOUSEKEEPING_SHUTDOWN_DRAIN_TIMEOUT,
        ))
