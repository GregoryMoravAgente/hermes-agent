"""Managed-worker receipt identity remains exact across stale edges and release."""

from types import SimpleNamespace

import pytest

from gateway.runtime_service_physical import ServiceProcessReceipt


class _Coordinator:
    def __init__(self):
        self.unknown_calls = []

    def mark_unknown(self, handle, reason):
        self.unknown_calls.append((handle, reason))

    def release_unaccepted(self, handle):
        pass


def _managed_receipt(*, replacement=False):
    coordinator = _Coordinator()
    service = SimpleNamespace(
        coordinator=coordinator,
        handle=object(),
        external_parent=None,
        physical=None,
        unknown=False,
    )
    old_parent = SimpleNamespace(
        state="request_reserved", root_handle=service.handle,
        _service=service, _physical=None,
    )
    owner = SimpleNamespace(parent=old_parent, service=service, physical=None)
    receipt = ServiceProcessReceipt(service, object(), "managed:physical")
    receipt.source_owner = owner
    receipt.source_parent = old_parent
    receipt.source_service = service
    receipt.source_root_handle = service.handle
    receipt.ephemeral_process = True
    old_parent._physical = receipt
    owner.physical = receipt
    service.physical = receipt
    if replacement:
        replacement_parent = SimpleNamespace(
            state="request_reserved", _service=service, _physical=object(),
        )
        service.external_parent = replacement_parent
    else:
        service.external_parent = old_parent
    return coordinator, service, old_parent, owner, receipt


def test_stale_managed_receipt_does_not_poison_replacement_edges():
    coordinator, service, old_parent, owner, receipt = _managed_receipt(replacement=True)
    replacement_parent = service.external_parent
    replacement_physical = replacement_parent._physical

    receipt.mark_unknown("reader_failed")

    assert receipt.state == "unknown"
    assert receipt.unknown_reason == "reader_failed"
    assert coordinator.unknown_calls == [(receipt.handle, "reader_failed")]
    assert service.unknown is False
    assert service.physical is receipt
    assert old_parent.state == "request_reserved"
    assert old_parent._physical is receipt
    assert owner.physical is receipt
    assert replacement_parent.state == "request_reserved"
    assert replacement_parent._physical is replacement_physical


def test_verified_no_effect_release_clears_only_exact_source_aliases():
    coordinator, service, old_parent, owner, receipt = _managed_receipt(replacement=True)
    replacement_parent = service.external_parent
    replacement_physical = replacement_parent._physical

    assert receipt.release_before_effect() is True

    assert receipt.state == "released"
    assert service.physical is None
    assert old_parent._physical is None
    assert owner.physical is None
    assert replacement_parent._physical is replacement_physical
    assert coordinator.unknown_calls == []


@pytest.mark.platforms("posix")
@pytest.mark.asyncio
async def test_real_managed_receipt_rejects_owner_parent_rebind(tmp_path, monkeypatch):
    """A real managed reservation retains its captured parent across owner rebinding."""
    from gateway.session_managed_worker_custody import reserve_managed_worker
    from gateway.session_turn_physical_tip import canonical_store_path
    from tests.gateway.test_runtime_service_parent_edges import real_runtime_binding

    fixture = await real_runtime_binding(tmp_path, monkeypatch, include_probe=False)
    custody = None
    try:
        binding = await fixture.claim("managed-owner-parent-rebind")
        custody = reserve_managed_worker(binding, canonical_store_path(fixture.authority))
        receipt = custody.physical
        owner = custody.owner
        captured_parent = receipt.source_parent
        captured_service = receipt.source_service
        captured_root = receipt.source_root_handle
        original_state = captured_parent.state
        replacement_parent = SimpleNamespace(
            state="request_reserved", _service=captured_service, _physical=object(),
        )
        owner.parent = replacement_parent

        receipt.mark_unknown("owner_parent_rebound")

        assert receipt.state == "unknown"
        assert receipt.unknown_reason == "owner_parent_rebound"
        assert receipt.source_parent is captured_parent
        assert receipt.source_service is captured_service
        assert receipt.source_root_handle is captured_root
        assert owner.parent is replacement_parent
        assert captured_parent.state == original_state
        assert captured_service.unknown is False
        assert replacement_parent.state == "request_reserved"
        assert captured_parent._physical is receipt
        assert replacement_parent._physical is not receipt
    finally:
        fixture.cleanup()
