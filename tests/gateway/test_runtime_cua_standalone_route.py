"""Behavioral proof for the source-owned standalone CUA route."""

from __future__ import annotations

import asyncio
import sys

import pytest


pytestmark = pytest.mark.platforms("posix")


def _standalone_owner(monkeypatch: pytest.MonkeyPatch, tmp_path):
    import gateway.run
    import hermes_state
    from gateway.runtime_sidecar_cua import reserve_cua_stdio

    home = tmp_path / "hermes-home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setattr(hermes_state, "DEFAULT_DB_PATH", home / "state.db")
    monkeypatch.setattr(gateway.run, "_gateway_runner_ref", lambda: None)
    return reserve_cua_stdio(session_id="standalone-cua", permission_mode="standard")


async def _run_real_cua_child(owner) -> None:
    from gateway.runtime_sidecar_cua import _bind_cua_physical

    async def lifecycle() -> None:
        owner.bind_task(asyncio.current_task())
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "import sys; sys.exit(0)",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        owner.receipt.bind_process(process, (process.stdout, process.stdin))
        _bind_cua_physical(owner, process, (process.stdout, process.stdin))
        owner.receipt.bind_session(object(), asyncio.current_task())
        owner.receipt.close_session(None, None)
        await owner.receipt.finish_transport(None)

    await asyncio.create_task(lifecycle())


def _reconcile_partial_route(owner, request, physical) -> None:
    coordinator = owner.parent.coordinator
    parent = owner.parent
    for handle in (parent.root_handle, physical.handle):
        assert coordinator.reconcile_unknown(
            handle,
            owner_id=handle.owner_id,
            profile_id=parent.profile_id,
            process_instance=parent.process_instance,
            authority_epoch=parent.authority_epoch,
        ) is None
    physical.state = "reserved"
    physical.unknown_reason = None
    owner.parent._service.unknown = False
    owner.receipt.state = "reserved"
    owner.receipt.unknown_reason = None
    owner.parent._request = request
    owner.parent.state = "request_reserved"


@pytest.mark.asyncio
async def test_reserve_cua_stdio_standalone_closes_real_source_route_and_seals(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    from gateway.runtime_custody import CustodyRefusal

    owner = _standalone_owner(monkeypatch, tmp_path)
    parent = owner.parent
    service = parent._service
    request = parent._request
    physical = parent._physical

    assert parent.mode == "standalone_maintenance"
    assert service.binding is None
    assert service.external_parent is parent
    assert service.requests[parent.operation_id] is request
    assert physical.service is service
    assert physical.handle.lease.parent_lease_id == request.lease.lease_id
    registry = parent.coordinator._i16_source_registry
    assert registry.services[parent.root_handle.lease.lease_id] is service
    assert registry.requests[request.lease.lease_id] is request
    assert registry.receipts[parent.root_handle.lease.lease_id][0] is physical

    await _run_real_cua_child(owner)
    assert owner.finish() is True
    assert parent.state == "terminal"
    assert service.requests == {}
    assert physical.state == "closed"
    assert owner.receipt.source_factory is None
    assert owner.receipt.readback() is not None

    close = parent.coordinator.begin_close(owner_id="standalone-cua-close")
    assert not isinstance(close, CustodyRefusal)
    evidence, refusal = registry.source_owner_evidence_and_seal(
        process_instance=parent.process_instance,
        authority_epoch=parent.authority_epoch,
        close_generation=close.close_generation,
        owner_id=owner.source_identity,
    )
    assert evidence is not None
    assert evidence["standalone"] is True
    assert evidence["terminal_receipts"][0]["state"] == "closed"
    assert refusal is None
    assert parent.coordinator.abort_before_handoff(close) is None


@pytest.mark.asyncio
async def test_standalone_cua_partial_route_refuses_before_native_bind_and_reconciles(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    from gateway.runtime_sidecar_cua import _bind_cua_physical

    owner = _standalone_owner(monkeypatch, tmp_path)
    parent = owner.parent
    request = parent._request
    physical = parent._physical
    parent._request = None

    with pytest.raises(RuntimeError, match="cua_standalone_owner_incomplete"):
        _bind_cua_physical(owner, object(), ())
    assert owner.receipt.process is None
    assert owner.receipt.state == "unknown"
    assert physical.state == "unknown"
    assert parent.state == "unknown"

    _reconcile_partial_route(owner, request, physical)
    assert owner.release_before_effect() is True
    assert parent.state == "terminal"
    assert physical.state == "released"
    assert parent._service.requests == {}


@pytest.mark.asyncio
async def test_standalone_native_close_retry_keeps_the_same_physical_receipt(
    monkeypatch: pytest.MonkeyPatch, tmp_path,
) -> None:
    owner = _standalone_owner(monkeypatch, tmp_path)
    parent = owner.parent
    physical, request, root = parent._physical, parent._request, parent.root_handle
    await _run_real_cua_child(owner)
    close_native = physical.close_native_after_process
    monkeypatch.setattr(physical, 'close_native_after_process', lambda: False)
    try:
        assert owner.finish() is False
        assert parent._physical is physical
        assert parent._request is request
        assert parent.root_handle is root
        assert parent.state == 'unknown'
        monkeypatch.setattr(physical, 'close_native_after_process', close_native)
        assert owner.finish() is True
        assert physical.state == 'closed'
        assert parent.state == 'terminal'
        assert parent.root_handle is root
    finally:
        monkeypatch.setattr(physical, 'close_native_after_process', close_native)
