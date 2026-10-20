"""Source-owned publication and cache-readback controls."""

import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest


def test_catalog_finalizer_consumes_exact_cache_readback():
    from gateway.runtime_service_operations import (
        ExternalProcessParent, ProducerPolicy, _finalize_external_catalog_cache,
    )

    parent = ExternalProcessParent(
        mode="standalone_maintenance", profile_home=Path("/synthetic/profile"), profile_id="profile",
        operation_id="catalog", source_identity="copilot-acp", principal_id=None,
        process_instance="process", authority_epoch=1, coordinator=object(), root_handle=object(),
    )
    parent._physical = object()
    calls = []
    parent._catalog_client = SimpleNamespace(
        finalize_external_catalog=lambda **kwargs: calls.append(kwargs),
    )
    policy = ProducerPolicy("standalone_maintenance", parent, True, False, False)
    row = {"fp": "fingerprint", "at": 1.0, "models": ["live/model"]}

    _finalize_external_catalog_cache(
        policy, "copilot-acp", row, ["live/model"], expected_fingerprint="fingerprint",
    )

    assert calls and calls[0]["cache_identity"].startswith("copilot-acp:fingerprint:")
    assert calls[0]["payload_digest"]


def test_catalog_finalizer_closes_real_receipt_after_disk_readback(tmp_path):
    from agent.copilot_acp_client import CopilotACPClient
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.runtime_service_operations import (
        ExternalProcessParent, ProducerPolicy, _finalize_external_catalog_cache, bind_physical,
        reserve_physical,
    )
    from gateway.runtime_service_custody import service_for_external_parent
    from gateway.runtime_ownership import ProfileOwnership

    coordinator = RuntimeCustodyCoordinator(process_instance="catalog-process", authority_epoch=1)
    owner = ProfileOwnership()
    owner.reserve([tmp_path])
    root = coordinator.reserve_root(
        "I16_turn_sidecars", "external:catalog", profile_id=str(tmp_path), authority_epoch=1,
    )
    parent = ExternalProcessParent(
        mode="standalone_maintenance", profile_home=tmp_path, profile_id=str(tmp_path),
        operation_id="catalog", source_identity="copilot-acp", principal_id=None,
        process_instance="catalog-process", authority_epoch=1, coordinator=coordinator, root_handle=root,
        lock_owner=owner, physical_owner_epoch=owner.owner_epoch(tmp_path),
    )
    parent._service = service_for_external_parent(coordinator, root, "external:catalog")
    policy = ProducerPolicy("standalone_maintenance", parent, True, False, False)
    client = CopilotACPClient(command=sys.executable, args=["-c", "pass"], producer_policy=policy)
    process = subprocess.Popen(
        [sys.executable, "-c", "pass"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    receipt = reserve_physical(parent)
    bind_physical(
        parent, None, process, pid=process.pid, process_started_at=None,
        scope="posix_direct", scope_identity="posix_direct",
    )

    class _DoneReader:
        def is_alive(self):
            return False

    stdout_reader, stderr_reader = _DoneReader(), _DoneReader()
    receipt.bind_reader("stdout", stdout_reader)
    receipt.bind_reader("stderr", stderr_reader)
    process.wait()
    receipt.reader_eof("stdout", stdout_reader)
    receipt.reader_eof("stderr", stderr_reader)
    receipt.reader_joined("stdout", stdout_reader)
    receipt.reader_joined("stderr", stderr_reader)

    cache_file = tmp_path / "provider_models_cache.json"
    cache_file.write_text(json.dumps({"fp": "fingerprint", "at": 1.0, "models": ["live/model"]}))
    reread = json.loads(cache_file.read_text())
    _finalize_external_catalog_cache(
        policy, "copilot-acp", reread, ["live/model"], expected_fingerprint="fingerprint",
    )

    assert parent._physical is None
    assert parent._service.physical is None
    assert parent.state == "request_reserved"
    assert client is parent._catalog_client
    client.close()
    assert parent.state == "terminal"
    assert not owner.close().failed


def test_catalog_finalizer_refuses_same_models_from_a_foreign_credential(tmp_path):
    from agent.copilot_acp_client import CopilotACPClient
    from gateway.runtime_custody import RuntimeCustodyCoordinator
    from gateway.runtime_service_operations import (
        ExternalProcessOperationError, ExternalProcessParent, ProducerPolicy,
        _finalize_external_catalog_cache, bind_physical, reserve_physical,
    )
    from gateway.runtime_service_custody import service_for_external_parent
    from gateway.runtime_ownership import ProfileOwnership

    coordinator = RuntimeCustodyCoordinator(process_instance="catalog-process", authority_epoch=1)
    owner = ProfileOwnership()
    owner.reserve([tmp_path])
    root = coordinator.reserve_root(
        "I16_turn_sidecars", "external:catalog", profile_id=str(tmp_path), authority_epoch=1,
    )
    parent = ExternalProcessParent(
        mode="standalone_maintenance", profile_home=tmp_path, profile_id=str(tmp_path),
        operation_id="catalog", source_identity="copilot-acp", principal_id=None,
        process_instance="catalog-process", authority_epoch=1, coordinator=coordinator, root_handle=root,
        lock_owner=owner, physical_owner_epoch=owner.owner_epoch(tmp_path),
    )
    parent._service = service_for_external_parent(coordinator, root, "external:catalog")
    policy = ProducerPolicy("standalone_maintenance", parent, True, False, False)
    client = CopilotACPClient(command=sys.executable, args=["-c", "pass"], producer_policy=policy)
    process = subprocess.Popen(
        [sys.executable, "-c", "pass"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    receipt = reserve_physical(parent)
    bind_physical(
        parent, None, process, pid=process.pid, process_started_at=None,
        scope="posix_direct", scope_identity="posix_direct",
    )

    class _DoneReader:
        def is_alive(self):
            return False

    stdout_reader, stderr_reader = _DoneReader(), _DoneReader()
    receipt.bind_reader("stdout", stdout_reader)
    receipt.bind_reader("stderr", stderr_reader)
    process.wait()
    receipt.reader_eof("stdout", stdout_reader)
    receipt.reader_eof("stderr", stderr_reader)
    receipt.reader_joined("stdout", stdout_reader)
    receipt.reader_joined("stderr", stderr_reader)
    try:
        with pytest.raises(ExternalProcessOperationError, match="cache_readback_unknown"):
            _finalize_external_catalog_cache(
                policy, "copilot-acp", {"fp": "foreign", "at": 1.0, "models": ["same/model"]},
                ["same/model"], expected_fingerprint="authorizing",
            )
        assert parent._physical is receipt
        assert receipt.state == "unknown"
        assert receipt.unknown_reason == "cache_readback_unknown"
    finally:
        client.close()
        assert not owner.close().failed


def test_catalog_finalizer_preserves_a_prior_typed_unknown_reason():
    from gateway.runtime_service_operations import (
        ExternalProcessOperationError, ExternalProcessParent, ProducerPolicy,
        _finalize_external_catalog_cache,
    )

    parent = ExternalProcessParent(
        mode="standalone_maintenance", profile_home=Path("/synthetic/profile"), profile_id="profile",
        operation_id="catalog", source_identity="copilot-acp", principal_id=None,
        process_instance="process", authority_epoch=1, coordinator=object(), root_handle=object(),
    )
    parent._physical = SimpleNamespace(state="unknown", unknown_reason="acp_session_failed")
    policy = ProducerPolicy("standalone_maintenance", parent, True, False, False)

    with pytest.raises(ExternalProcessOperationError, match="custody_unknown"):
        _finalize_external_catalog_cache(
            policy, "copilot-acp", {"fp": "fingerprint", "models": ["same/model"]},
            ["same/model"], expected_fingerprint="fingerprint",
        )
    assert parent._physical.unknown_reason == "acp_session_failed"
