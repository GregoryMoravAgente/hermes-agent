"""Causal controls for the process-global I16 sidecar source graph."""

from dataclasses import dataclass
import importlib
from types import SimpleNamespace

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from gateway.runtime_i16_source_registry import (
    I16_FACTORY_ATTRIBUTES,
    I16_PRODUCER_ENTRYPOINTS,
    bootstrap_standalone_i16_source_registry,
    explicit_i16_producer_key,
    ensure_i16_source_registry,
    refresh_i16_source_registry,
    seal_source_owner,
    source_owner_evidence,
)
from gateway.runtime_service_custody import (
    DurableResultProof,
    ServiceCustodyError,
    ServiceLease,
    service_for_external_parent,
)
from gateway.runtime_service_physical import ServiceProcessReceipt
from hermes_cli.backend_retirement import RetirementFence


class _FactoryAuthority:
    _mcp_source_factory = object()
    _browser_source_factory = object()
    _cua_source_factory = object()


class _AuthorityRegistry:
    def __init__(self, authorities):
        self._authorities = list(authorities)

    def __iter__(self):
        return iter(self._authorities)


@dataclass
class _StoppedProcess:
    def poll(self):
        return 0


@dataclass
class _StoppedReader:
    def is_alive(self):
        return False


def _coordinator_with_registry():
    coordinator = RuntimeCustodyCoordinator(
        process_instance="i16-process",
        authority_epoch=7,
        fence=RetirementFence(),
    )
    registry = ensure_i16_source_registry(coordinator)
    for key, (module_name, attr_name) in I16_PRODUCER_ENTRYPOINTS.items():
        entrypoint = getattr(importlib.import_module(module_name), attr_name)
        assert registry.register_producer(key, entrypoint) is entrypoint
    authority = _FactoryAuthority()
    registry.authorities = (authority,)
    for key, attr_name in I16_FACTORY_ATTRIBUTES.items():
        assert registry.register_factory(key, authority, getattr(authority, attr_name))
    registry.bootstrapped = True
    return coordinator, registry


def _terminal_service(coordinator, registry):
    service_handle = coordinator.reserve_root(
        "I16_turn_sidecars", "mcp:service", profile_id="profile-1", authority_epoch=7,
    )
    service = ServiceLease(
        coordinator, service_handle, None, "mcp:service", {}, producer_key="mcp",
    )
    registry.register_service(service, "mcp")

    physical_handle = coordinator.reserve_root(
        "I16_turn_sidecars", "mcp:service:physical", profile_id="profile-1",
        authority_epoch=7, parent=service_handle,
    )
    receipt = ServiceProcessReceipt(service, physical_handle, "mcp:service:physical")
    service.physical = receipt
    registry.register_receipt(service, receipt)

    stdout = _StoppedReader()
    stderr = _StoppedReader()
    receipt.process = _StoppedProcess()
    receipt.scope = "posix_direct"
    receipt.scope_identity = "posix_direct"
    receipt.state = "closed"
    receipt.native_close_committed = True
    receipt.readers = {"stdout": stdout, "stderr": stderr}
    receipt.reader_start_states = {"stdout": "started", "stderr": "started"}
    receipt.eof_streams = {"stdout", "stderr"}
    receipt.joined_streams = {"stdout", "stderr"}
    proof = DurableResultProof(
        store_path="/tmp/profile/state.db",
        session_id="session-1",
        admission_id="admission-1",
        execution_generation=1,
        row_ids=(1, 2),
        row_snapshots=("user", "assistant"),
        row_sessions=("session-1", "session-1"),
        row_roles=("user", "assistant"),
        payload_digest="digest-1",
        result_row_ids=(2,),
    )
    receipt.durable_proofs["request-1"] = proof
    receipt.durable_proof_history.append(proof)
    receipt.completed_requests.add("request-1")
    assert coordinator.mark_terminal(physical_handle) is None
    assert coordinator.mark_terminal(service_handle) is None
    return service, receipt


def test_i16_source_graph_carries_receipt_to_global_seal():
    coordinator, registry = _coordinator_with_registry()
    _service, receipt = _terminal_service(coordinator, registry)
    close = coordinator.begin_close(owner_id="i16-close")

    evidence, refusal = seal_source_owner(
        coordinator,
        process_instance="i16-process",
        authority_epoch=7,
        close_generation=close.close_generation,
        owner_id="i16-owner",
    )

    assert evidence is not None
    assert evidence["terminal_receipts"][0]["handle"] == receipt.handle.lease.lease_id
    assert evidence["cross_process_proof"] is True
    assert refusal is None


def test_i16_missing_producer_registration_refuses_seal():
    coordinator, registry = _coordinator_with_registry()
    _terminal_service(coordinator, registry)
    registry.producer_bindings.pop("browser")
    close = coordinator.begin_close(owner_id="i16-missing")

    assert source_owner_evidence(
        coordinator,
        process_instance="i16-process",
        authority_epoch=7,
        close_generation=close.close_generation,
        owner_id="i16-owner",
    ) is None


def test_i16_caller_names_cannot_authenticate_an_unmapped_source():
    """A descriptive owner/source string cannot become the registered provider route."""
    assert explicit_i16_producer_key(None) is None

    coordinator = RuntimeCustodyCoordinator(
        process_instance="standalone-process",
        authority_epoch=7,
        fence=RetirementFence(),
    )
    registry = bootstrap_standalone_i16_source_registry(
        coordinator,
        profile_id="profile-1",
        process_instance="standalone-process",
        authority_epoch=7,
    )
    root = coordinator.reserve_root(
        "I16_turn_sidecars", "external:provider:forged",
        profile_id="profile-1", authority_epoch=7,
    )

    with pytest.raises(ServiceCustodyError, match="i16_source_unregistered"):
        service_for_external_parent(
            coordinator, root, "external:provider:forged",
        )

    assert registry.services == {}


def test_i16_empty_authority_graph_refuses_seal():
    coordinator, registry = _coordinator_with_registry()
    _terminal_service(coordinator, registry)
    registry.authorities = ()
    registry.factory_bindings.clear()
    close = coordinator.begin_close(owner_id="i16-no-authorities")

    assert source_owner_evidence(
        coordinator,
        process_instance="i16-process",
        authority_epoch=7,
        close_generation=close.close_generation,
        owner_id="i16-owner",
    ) is None


def test_i16_live_or_unknown_physical_receipt_refuses_seal():
    coordinator, registry = _coordinator_with_registry()
    service_handle = coordinator.reserve_root(
        "I16_turn_sidecars", "browser:service", profile_id="profile-1", authority_epoch=7,
    )
    service = ServiceLease(
        coordinator, service_handle, None, "browser:service", {}, producer_key="browser",
    )
    registry.register_service(service, "browser")
    physical_handle = coordinator.reserve_root(
        "I16_turn_sidecars", "browser:service:physical", profile_id="profile-1",
        authority_epoch=7, parent=service_handle,
    )
    receipt = ServiceProcessReceipt(service, physical_handle, "browser:service:physical")
    service.physical = receipt
    registry.register_receipt(service, receipt)
    receipt.mark_unknown("reader_failed")
    close = coordinator.begin_close(owner_id="i16-unknown")

    assert source_owner_evidence(
        coordinator,
        process_instance="i16-process",
        authority_epoch=7,
        close_generation=close.close_generation,
        owner_id="i16-owner",
    ) is None


def test_i16_foreign_scope_refuses_seal():
    coordinator, registry = _coordinator_with_registry()
    _service, receipt = _terminal_service(coordinator, registry)
    receipt.scope = "posix_process_group"
    close = coordinator.begin_close(owner_id="i16-foreign-scope")

    assert source_owner_evidence(
        coordinator,
        process_instance="i16-process",
        authority_epoch=7,
        close_generation=close.close_generation,
        owner_id="i16-owner",
    ) is None


def test_i16_reader_failure_or_missing_result_refuses_seal():
    coordinator, registry = _coordinator_with_registry()
    _service, receipt = _terminal_service(coordinator, registry)
    receipt.reader_error = "reader_failed"
    close = coordinator.begin_close(owner_id="i16-reader-failure")

    assert source_owner_evidence(
        coordinator,
        process_instance="i16-process",
        authority_epoch=7,
        close_generation=close.close_generation,
        owner_id="i16-owner",
    ) is None

    coordinator, registry = _coordinator_with_registry()
    _service, receipt = _terminal_service(coordinator, registry)
    receipt.durable_proofs.clear()
    receipt.durable_proof_history.clear()
    close = coordinator.begin_close(owner_id="i16-no-result")

    assert source_owner_evidence(
        coordinator,
        process_instance="i16-process",
        authority_epoch=7,
        close_generation=close.close_generation,
        owner_id="i16-owner",
    ) is None


def test_i16_hot_successor_is_added_to_factory_graph_before_seal():
    coordinator, registry = _coordinator_with_registry()
    _terminal_service(coordinator, registry)
    first = registry.authorities[0]
    successor = _FactoryAuthority()
    runner = SimpleNamespace(session_authorities=_AuthorityRegistry([first, successor]))

    refreshed = refresh_i16_source_registry(runner, coordinator)

    assert refreshed is registry
    assert refreshed.authorities == (first, successor)
    assert refreshed._factory_graph_is_complete()
    for key, attr in I16_FACTORY_ATTRIBUTES.items():
        assert any(
            owner is successor and registered is getattr(successor, attr)
            for owner, registered in refreshed.factory_bindings[key]
        )
    close = coordinator.begin_close(owner_id="i16-hot-successor")
    assert source_owner_evidence(
        coordinator,
        process_instance="i16-process",
        authority_epoch=7,
        close_generation=close.close_generation,
        owner_id="i16-owner",
    ) is not None


def test_i16_close_rejects_new_hook_but_keeps_existing_request_continuation():
    coordinator = RuntimeCustodyCoordinator(
        process_instance="i16-process",
        authority_epoch=7,
        fence=RetirementFence(),
    )
    service_handle = coordinator.reserve_root(
        "I16_turn_sidecars", "cua:service", profile_id="profile-1", authority_epoch=7,
    )
    service = ServiceLease(coordinator, service_handle, None, "cua:service", {}, producer_key="cua")
    parent = SimpleNamespace(
        coordinator=coordinator, parent_handle=service_handle,
        profile_id="profile-1", authority_epoch=7, owner=SimpleNamespace(),
    )
    close = coordinator.begin_close(owner_id="i16-close-fence")
    assert close is not None

    with pytest.raises(ServiceCustodyError, match="runtime_draining"):
        service.begin_hook_request(parent, "late-hook")
    binding = SimpleNamespace(admission_id="late-turn")
    service.validate_binding = lambda _binding: None
    with pytest.raises(ServiceCustodyError, match="runtime_draining"):
        service.begin_request(binding)

    continued = RuntimeCustodyCoordinator(
        process_instance="i16-continuation",
        authority_epoch=7,
        fence=RetirementFence(),
    )
    continued_handle = continued.reserve_root(
        "I16_turn_sidecars", "cua:continued", profile_id="profile-1", authority_epoch=7,
    )
    continued_service = ServiceLease(
        continued, continued_handle, None, "cua:continued", {}, producer_key="cua",
    )
    continued_parent = SimpleNamespace(
        coordinator=continued, parent_handle=continued_handle,
        profile_id="profile-1", authority_epoch=7, owner=SimpleNamespace(),
    )
    accepted_request = continued.reserve_root(
        "I16_turn_sidecars", "cua:continued:accepted", profile_id="profile-1",
        authority_epoch=7, parent=continued_handle,
    )
    continued_service.requests["accepted"] = accepted_request
    assert continued.begin_close(owner_id="i16-continuation-close") is not None
    receipt = continued_service.reserve_hook_physical(continued_parent)
    assert receipt.handle.lease.parent_lease_id == accepted_request.lease.lease_id


def test_i16_posix_launch_requires_exact_process_pid():
    coordinator = RuntimeCustodyCoordinator(
        process_instance="i16-process",
        authority_epoch=7,
        fence=RetirementFence(),
    )
    service_handle = coordinator.reserve_root(
        "I16_turn_sidecars", "codex:service", profile_id="profile-1", authority_epoch=7,
    )
    service = ServiceLease(coordinator, service_handle, None, "codex:service", {}, producer_key="codex")
    physical_handle = coordinator.reserve_root(
        "I16_turn_sidecars", "codex:service:physical", profile_id="profile-1",
        authority_epoch=7, parent=service_handle,
    )
    receipt = ServiceProcessReceipt(service, physical_handle, "codex:service:physical")
    assert receipt.bind_process(111, None, "posix_direct", "posix_direct")

    with pytest.raises(ServiceCustodyError, match="physical_launch_identity_mismatch"):
        receipt.bind_launch(SimpleNamespace(pid=222))

    assert receipt.state == "unknown"
    assert receipt.unknown_reason == "launch_identity_mismatch"


def test_i16_reader_must_be_started_before_terminal_graph_evidence():
    coordinator, registry = _coordinator_with_registry()
    _service, receipt = _terminal_service(coordinator, registry)
    receipt.reader_start_states = {"stdout": "bound", "stderr": "bound"}
    close = coordinator.begin_close(owner_id="i16-reader-not-started")

    assert source_owner_evidence(
        coordinator,
        process_instance="i16-process",
        authority_epoch=7,
        close_generation=close.close_generation,
        owner_id="i16-owner",
    ) is None


@pytest.mark.parametrize("producer_key", ("acp", "codex", "cua"))
def test_i16_standalone_bootstrap_authenticates_supported_service_without_profile_factory(
    producer_key,
):
    coordinator = RuntimeCustodyCoordinator(
        process_instance="standalone-process",
        authority_epoch=7,
        fence=RetirementFence(),
    )
    registry = bootstrap_standalone_i16_source_registry(
        coordinator,
        profile_id="profile-1",
        process_instance="standalone-process",
        authority_epoch=7,
    )
    service_handle = coordinator.reserve_root(
        "I16_turn_sidecars", "external:cua", profile_id="profile-1", authority_epoch=7,
    )
    service = ServiceLease(
        coordinator, service_handle, None, f"external:{producer_key}", {}, producer_key=producer_key,
    )
    registry.register_service(service, producer_key)
    assert coordinator.mark_terminal(service_handle) is None

    evidence = source_owner_evidence(
        coordinator,
        process_instance="standalone-process",
        authority_epoch=7,
        close_generation=0,
        owner_id="standalone-process:I16",
    )

    assert evidence is not None
    assert evidence["standalone"] is True
    assert evidence["registered_producers"] == tuple(I16_PRODUCER_ENTRYPOINTS)
    assert evidence["registered_factories"] == ()

