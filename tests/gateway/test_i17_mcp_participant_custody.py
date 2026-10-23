"""Causal public controls for exact MCP participant retention after OPEN."""

from dataclasses import replace
from functools import partial
import threading

import pytest


def _start_pending_mcp_abort(started_mux, monkeypatch, *, configure_all=True, before=False):
    from gateway import run_runtime
    from gateway.run_runtime import recover_gateway_native_sessions
    from gateway.runtime_sidecar_factory import observe_mcp_config
    from gateway.runtime_sidecar_remote import RemoteSourceError
    from gateway.session_authorities import serving_authorities

    runner, _homes, _loop, call = started_mux
    call(recover_gateway_native_sessions(runner))
    authorities = tuple(serving_authorities(runner))
    configured = authorities if configure_all else authorities[:1]
    for authority in configured:
        observe_mcp_config(runner, authority.profile_id, {})
    factories = tuple(authority._mcp_source_factory for authority in configured)
    assert all(factory is not None for factory in factories)
    target = factories[0]
    real_complete = type(target).complete_finish_abort
    calls = {"count": 0}

    def fail_after_effect(factory, capability, *, transition=None):
        if factory is target and calls["count"] == 0:
            calls["count"] += 1
            if before:
                raise RemoteSourceError("diagnostic_participant_before_effect")
            real_complete(factory, capability, transition=transition)
            raise RemoteSourceError("diagnostic_participant_after_effect")
        return real_complete(factory, capability, transition=transition)

    monkeypatch.setattr(type(target), "complete_finish_abort", fail_after_effect)
    descriptor = runner.session_runtime_descriptor
    params = {
        "instance_id": descriptor["instance_id"],
        "authority_epoch": descriptor["authority_epoch"],
        "owner_id": "i17-participant-custody",
        "scope": "process",
        "deadline_s": 30,
    }
    wire = call(run_runtime.begin_preservation_drain(runner, params, "fixture-peer"))
    assert wire["status"] == "closing", wire
    prepared = call(run_runtime.prepare_preservation_handoff(
        runner, {"receipt": wire}, "fixture-peer",
    ))
    assert prepared["status"] == "refused", prepared
    pending = call(run_runtime.abort_preservation_drain(
        runner, {"receipt": wire}, "fixture-peer",
    ))
    assert pending["status"] == "open", pending
    assert pending["pending_cleanup"]["source"] == "preservation_sources", pending
    assert pending["pending_cleanup"]["reason"] == "mcp_source_finalization_pending", pending
    assert runner.runtime_custody.status()["post_open_cleanup_pending"] is True
    transition = runner.runtime_custody.pending_post_open_transition()
    assert transition is not None
    for source, participant_receipt, capability in transition.source_participants:
        assert capability.receipt is participant_receipt
        if capability.__class__.__name__ == "I08AbortCapability":
            assert capability.runner is source
        elif capability.__class__.__name__ == "CronAbortCapability":
            assert capability.registry is source
        else:
            assert capability.factory is source
    cron_registry = getattr(runner.runtime_custody, "_cron_source_registry", None)
    assert cron_registry is not None
    assert cron_registry._graph_ready_locked(), {
        "missing": dict(cron_registry.missing),
        "bindings": tuple(cron_registry.producer_bindings),
        "homes": cron_registry.homes,
        "authorities": cron_registry.authorities,
        "provider": cron_registry.provider,
        "ticker": cron_registry.ticker_thread,
    }
    assert cron_registry._validate_graph_locked(), {
        "missing": dict(cron_registry.missing),
        "captured_adapters": cron_registry.api_adapters,
        "current_adapters": getattr(runner, "adapters", None),
    }
    assert any(
        capability.__class__.__name__ == "I08AbortCapability"
        for _source, _receipt, capability in transition.source_participants
    )
    assert any(
        capability.__class__.__name__ == "CronAbortCapability"
        for _source, _receipt, capability in transition.source_participants
    )
    monkeypatch.setattr(type(target), "complete_finish_abort", real_complete)
    return runner, call, wire, authorities, factories, target, transition


@pytest.mark.parametrize("mutation", ["removed", "replaced", "receipt", "epoch", "physical"])
def test_public_i17_retry_retains_exact_mcp_participant_after_detach_or_identity_change(
    started_mux, monkeypatch, mutation,
):
    """A changed participant stays OPEN/cleanup-pending until the original is restored."""
    from gateway import run_runtime

    runner, call, wire, _authorities, _factories, target, _transition = _start_pending_mcp_abort(
        started_mux, monkeypatch, before=mutation == "receipt",
    )
    authority = target.authority
    original_receipt = target.close_receipt
    original_epoch = authority.epoch
    original_factory = target
    replacement = type(target)(runner, authority)
    from gateway.runtime_ownership import process_ownership

    original_owner_epoch = process_ownership.owner_epoch
    receipt_mutation = (
        partial(
            monkeypatch.setattr,
            target,
            "_close_receipt",
            replace(original_receipt, close_generation=original_receipt.close_generation + 1),
        )
        if original_receipt is not None
        else partial(pytest.fail, "receipt mutation requires a retained source receipt")
    )
    mutations = {
        "removed": partial(monkeypatch.setattr, authority, "_mcp_source_factory", None),
        "replaced": partial(monkeypatch.setattr, authority, "_mcp_source_factory", replacement),
        "receipt": receipt_mutation,
        "epoch": partial(monkeypatch.setattr, authority, "epoch", original_epoch + 1),
        "physical": partial(
            monkeypatch.setattr, process_ownership, "owner_epoch", lambda _home: "foreign-owner",
        ),
    }
    mutations[mutation]()

    blocked = call(run_runtime.abort_preservation_drain(
        runner, {"receipt": wire}, "fixture-peer",
    ))
    assert blocked["status"] == "open", blocked
    assert blocked["pending_cleanup"]["source"] == "preservation_sources", blocked
    assert blocked["pending_cleanup"]["reason"] == "mcp_source_finalization_pending", blocked
    assert runner.runtime_custody.status()["post_open_cleanup_pending"] is True
    assert runner.session_runtime_descriptor["state"] != "ready"

    restores = {
        "removed": partial(monkeypatch.setattr, authority, "_mcp_source_factory", original_factory),
        "replaced": partial(monkeypatch.setattr, authority, "_mcp_source_factory", original_factory),
        "receipt": partial(monkeypatch.setattr, target, "_close_receipt", original_receipt),
        "epoch": partial(monkeypatch.setattr, authority, "epoch", original_epoch),
        "physical": partial(monkeypatch.setattr, process_ownership, "owner_epoch", original_owner_epoch),
    }
    restores[mutation]()

    recovered = call(run_runtime.abort_preservation_drain(
        runner, {"receipt": wire}, "fixture-peer",
    ))
    assert recovered["status"] == "open", recovered
    assert runner.runtime_custody.status()["post_open_cleanup_pending"] is False
    assert runner.session_runtime_descriptor["state"] == "ready"


def test_public_i17_retry_rejects_new_registered_participant_until_original_set_is_restored(
    started_mux, monkeypatch,
):
    """A factory added after OPEN cannot be silently omitted from the old transaction."""
    from gateway import run_runtime

    runner, call, wire, authorities, _factories, _target, _transition = _start_pending_mcp_abort(
        started_mux, monkeypatch, configure_all=False,
    )
    added = authorities[1]
    added_factory = added._mcp_source_factory
    from gateway.runtime_sidecar_factory import observe_mcp_config

    observe_mcp_config(runner, added.profile_id, {})
    blocked = call(run_runtime.abort_preservation_drain(
        runner, {"receipt": wire}, "fixture-peer",
    ))
    assert blocked["status"] == "open", blocked
    assert blocked["pending_cleanup"]["source"] == "preservation_sources", blocked
    assert runner.session_runtime_descriptor["state"] != "ready"

    monkeypatch.setattr(added_factory, "_config_seen", False)
    monkeypatch.setattr(added_factory, "_configured_enabled", frozenset())
    monkeypatch.setattr(added_factory, "_pending_names", frozenset())
    recovered = call(run_runtime.abort_preservation_drain(
        runner, {"receipt": wire}, "fixture-peer",
    ))
    assert recovered["status"] == "open", recovered
    assert runner.runtime_custody.status()["post_open_cleanup_pending"] is False


def test_public_i17_registration_refuses_new_factory_while_original_is_unknown(
    started_mux, monkeypatch,
):
    """The public registration seam cannot overwrite an unresolved old participant."""
    from gateway import run_runtime

    runner, call, wire, _authorities, _factories, target, _transition = _start_pending_mcp_abort(
        started_mux, monkeypatch,
    )
    authority = target.authority
    original = authority._mcp_source_factory
    monkeypatch.setattr(authority, "_mcp_source_factory", None)
    from gateway.runtime_sidecar_factory import register_mcp_source_factory
    from gateway.runtime_sidecar_remote import RemoteSourceError

    with pytest.raises(RemoteSourceError, match="finalization_pending"):
        register_mcp_source_factory(runner, authority)
    monkeypatch.setattr(authority, "_mcp_source_factory", original)
    recovered = call(run_runtime.abort_preservation_drain(
        runner, {"receipt": wire}, "fixture-peer",
    ))
    assert recovered["status"] == "open", recovered


def test_i17_participant_validation_rejects_copied_foreign_capability(started_mux, monkeypatch):
    """A copied capability in a copied participant tuple cannot consume the live token."""
    from gateway import run_runtime
    from gateway.run_runtime_mcp_participants import validate_post_open_participants

    runner, call, wire, _authorities, _factories, _target, transition = _start_pending_mcp_abort(
        started_mux, monkeypatch,
    )
    copied_entries = tuple(
        (factory, receipt, replace(capability))
        for factory, receipt, capability in transition.source_participants
    )
    copied_transition = replace(transition, source_participants=copied_entries)
    assert validate_post_open_participants(
        runner, runner.runtime_custody, copied_transition,
    ) is False
    recovered = call(run_runtime.abort_preservation_drain(
        runner, {"receipt": wire}, "fixture-peer",
    ))
    assert recovered["status"] == "open", recovered


def test_i17_source_callbacks_can_read_coordinator_from_worker_thread(
    started_mux, monkeypatch,
):
    """Source callbacks do not deadlock a worker that reports coordinator status.

    The callback is a real MCP factory method on the started public gateway.  Its worker-thread
    status read is the causal control: the old implementation held the coordinator lock across
    the source callback, so the worker could not complete until the callback returned.
    """
    from gateway import run_runtime
    from gateway.runtime_sidecar_factory import MCPRemoteSourceFactory

    callback_threads = []
    blocked_callbacks = []
    callback_errors = []

    def probe_status(coordinator):
        completed = threading.Event()

        def read_status():
            try:
                coordinator.status()
            except BaseException as exc:  # pragma: no cover - a causal diagnostic, not a fake pass
                callback_errors.append(exc)
            finally:
                completed.set()

        thread = threading.Thread(target=read_status, name="i17-status-probe", daemon=True)
        callback_threads.append((thread, completed))
        thread.start()
        if not completed.wait(1.0):
            blocked_callbacks.append(thread.name)

    real_commit = MCPRemoteSourceFactory.commit_finish_abort
    real_finalize = MCPRemoteSourceFactory.finalize_finish_abort
    real_complete = MCPRemoteSourceFactory.complete_finish_abort

    def commit_probe(factory, capability):
        probe_status(capability.coordinator)
        return real_commit(factory, capability)

    def finalize_probe(factory, capability):
        probe_status(capability.coordinator)
        return real_finalize(factory, capability)

    def complete_probe(factory, capability, *, transition=None):
        probe_status(capability.coordinator)
        return real_complete(factory, capability, transition=transition)

    monkeypatch.setattr(MCPRemoteSourceFactory, "commit_finish_abort", commit_probe)
    monkeypatch.setattr(MCPRemoteSourceFactory, "finalize_finish_abort", finalize_probe)

    # Leave one exact post-open source token pending so the same control exercises the retry path
    # (complete_finish_abort) after the initial CLOSING->OPEN transaction.
    runner, call, wire, _authorities, _factories, _target, _transition = _start_pending_mcp_abort(
        started_mux, monkeypatch, before=True,
    )
    monkeypatch.setattr(MCPRemoteSourceFactory, "complete_finish_abort", complete_probe)

    recovered = call(run_runtime.abort_preservation_drain(
        runner, {"receipt": wire}, "fixture-peer",
    ))
    assert recovered["status"] == "open", recovered
    assert runner.session_runtime_descriptor["state"] == "ready"

    for thread, completed in callback_threads:
        assert completed.wait(2.0), f"status callback did not settle: {thread.name}"
        thread.join(2.0)
        assert not thread.is_alive(), f"status callback leaked: {thread.name}"
    assert not callback_errors, callback_errors
    assert not blocked_callbacks, blocked_callbacks

