"""Producer-policy boundary controls for external-process model discovery and turns."""

import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest


def _external_profile(fetch_models, fallback=("curated/model",)):
    return SimpleNamespace(
        auth_type="external_process",
        base_url="",
        fallback_models=list(fallback),
        fetch_models=fetch_models,
    )


def test_external_turn_without_admission_binding_refuses(monkeypatch):
    from gateway import run_turn_runner
    from gateway.runtime_service_custody import ServiceCustodyError

    monkeypatch.setattr(
        "hermes_cli.runtime_provider_backends._is_external_process_provider",
        lambda provider: provider == "test-external",
    )
    monkeypatch.setattr("gateway.runtime_service_custody.current_turn_binding", lambda: None)

    with pytest.raises(ServiceCustodyError, match="preservation_owner_unmapped"):
        run_turn_runner._require_external_process_turn_parent("test-external")


def test_missing_binding_does_not_reuse_managed_policy(monkeypatch, tmp_path):
    from gateway.runtime_service_operations import (
        ExternalProcessOperationError, ExternalProcessParent, ProducerPolicy, producer_policy_for_turn,
    )

    monkeypatch.setattr(
        "hermes_cli.runtime_provider_backends._is_external_process_provider",
        lambda provider: provider == "test-external",
    )
    parent = ExternalProcessParent(
        mode="managed_turn", profile_home=tmp_path, profile_id=str(tmp_path), operation_id="old",
        source_identity="source", principal_id="principal", process_instance="process",
        authority_epoch=1, coordinator=object(), root_handle=object(), state="request_reserved",
    )
    context = SimpleNamespace(
        _external_process_policy=ProducerPolicy("managed_turn", parent, True, False, False),
    )

    with pytest.raises(ExternalProcessOperationError, match="preservation_owner_unmapped"):
        producer_policy_for_turn(SimpleNamespace(), context, "test-external")


def test_external_catalog_consumes_explicit_policy(monkeypatch):
    from hermes_cli import models
    import providers

    calls = []

    def fetch_models(**kwargs):
        calls.append(kwargs)
        return ["live/model"]

    profile = _external_profile(fetch_models)
    policy = SimpleNamespace(parent=object())
    monkeypatch.setattr(providers, "get_provider_profile", lambda _name: profile)

    rows = models._profile_live_catalog("test-external", producer_policy=policy)

    assert "live/model" in rows
    assert calls == [{"producer_policy": policy}]


def test_external_catalog_legacy_abi_is_one_shot(monkeypatch):
    from hermes_cli import models
    import providers

    calls = []

    def fetch_models():
        calls.append("legacy")
        return ["live/model"]

    profile = _external_profile(fetch_models)
    monkeypatch.setattr(providers, "get_provider_profile", lambda _name: profile)

    rows = models._profile_live_catalog("test-external", producer_policy=SimpleNamespace(parent=object()))

    assert "live/model" in rows
    assert len(calls) == 1


def test_external_catalog_typeerror_after_policy_is_typed_failure(monkeypatch):
    from hermes_cli import models
    import providers
    import gateway.runtime_service_operations as operations

    reasons = []
    parent = object()

    def fetch_models(**kwargs):
        assert kwargs
        raise TypeError("provider runtime failure")

    profile = _external_profile(fetch_models)
    monkeypatch.setattr(providers, "get_provider_profile", lambda _name: profile)
    monkeypatch.setattr(operations, "mark_unknown", lambda actual, reason: reasons.append((actual, reason)))

    rows = models._profile_live_catalog(
        "test-external", producer_policy=SimpleNamespace(parent=parent),
    )

    assert list(rows) == ["curated/model"]
    assert reasons == [(parent, "catalog_discovery_failed")]


def test_external_catalog_failure_retains_parent_unknown(monkeypatch):
    from hermes_cli import models
    import providers
    import gateway.runtime_service_operations as operations

    reasons = []
    parent = object()

    def fetch_models(**_kwargs):
        raise RuntimeError("child failed")

    profile = _external_profile(fetch_models)
    monkeypatch.setattr(providers, "get_provider_profile", lambda _name: profile)
    monkeypatch.setattr(operations, "mark_unknown", lambda actual, reason: reasons.append((actual, reason)))

    rows = models._profile_live_catalog(
        "test-external", producer_policy=SimpleNamespace(parent=parent),
    )

    assert list(rows) == ["curated/model"]
    assert reasons == [(parent, "catalog_discovery_failed")]


def test_external_catalog_without_policy_does_not_spawn(monkeypatch):
    from hermes_cli import models
    import providers

    def fetch_models(**_kwargs):
        raise AssertionError("unowned external process launch")

    monkeypatch.setattr(providers, "get_provider_profile", lambda _name: _external_profile(fetch_models))

    rows = models._profile_live_catalog("test-external")

    assert list(rows) == ["curated/model"]


def test_actual_external_producer_constructs_only_with_owned_policy(tmp_path):
    from agent.copilot_acp_client import CopilotACPClient
    from gateway.runtime_ownership import ProfileOwnership
    from gateway.runtime_service_operations import (
        ExternalProcessOperationError, ExternalProcessParent, ProducerPolicy, policy_without_spawn,
    )

    owner = ProfileOwnership()
    owner.reserve([tmp_path])
    parent = ExternalProcessParent(
        mode="standalone_maintenance", profile_home=tmp_path, profile_id=str(tmp_path),
        operation_id="catalog", source_identity="copilot-acp", principal_id=None,
        process_instance="process", authority_epoch=1, coordinator=object(), root_handle=object(),
        lock_owner=owner, physical_owner_epoch=owner.owner_epoch(tmp_path),
    )
    policy = ProducerPolicy("standalone_maintenance", parent, True, False, False)
    try:
        client = CopilotACPClient(command=sys.executable, args=["-c", "pass"], producer_policy=policy)
        assert client._producer_policy is policy
        assert parent._catalog_client is client

        with pytest.raises(ExternalProcessOperationError, match="injected_no_spawn"):
            CopilotACPClient(command=sys.executable, args=["-c", "pass"], producer_policy=policy_without_spawn())
    finally:
        assert not owner.close().failed


def _probe_maintenance_lock(home: Path) -> subprocess.CompletedProcess:
    """Use a separate process to exercise the real gateway.lock contention boundary."""
    probe = """
import sys
from pathlib import Path
from gateway.runtime_ownership import OwnershipConflict, exclusive_maintenance

try:
    with exclusive_maintenance([Path(sys.argv[1])], purpose="external-process-operation"):
        raise SystemExit(1)
except OwnershipConflict:
    raise SystemExit(0)
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(path or os.getcwd() for path in sys.path)
    return subprocess.run(
        [sys.executable, "-c", probe, str(home)],
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def test_unknown_external_finalizer_retains_cross_process_lock_until_same_owner_terminal(tmp_path):
    from gateway.runtime_ownership import (
        exclusive_maintenance, release_maintenance_hold, retain_maintenance,
    )

    home = tmp_path / "profile"
    context = exclusive_maintenance([home], purpose="external-process-operation")
    owner = context.__enter__()
    context_closed = False
    finalizer = SimpleNamespace(state="unknown")
    successor = SimpleNamespace(state="terminal")
    try:
        retain_maintenance(home, finalizer, owner=owner)
        context.__exit__(None, None, None)
        context_closed = True
        assert owner.release(home) is False
        blocked = _probe_maintenance_lock(home)
        assert blocked.returncode == 0, blocked.stderr
        assert release_maintenance_hold(home, successor, owner=owner) is False
        still_blocked = _probe_maintenance_lock(home)
        assert still_blocked.returncode == 0, still_blocked.stderr
        finalizer.state = "terminal"
        assert release_maintenance_hold(home, finalizer, owner=owner) is True
        released = _probe_maintenance_lock(home)
        assert released.returncode == 1, released.stderr
    finally:
        # The explicit terminal release above is the only successful owner release.  If a
        # setup assertion fails, make the test's cleanup truthful without releasing an unknown
        # finalizer or replacing its handle.
        if owner.finalizer_hold(home) is not None:
            finalizer.state = "terminal"
            release_maintenance_hold(home, finalizer, owner=owner)
        if not context_closed:
            context.__exit__(None, None, None)
