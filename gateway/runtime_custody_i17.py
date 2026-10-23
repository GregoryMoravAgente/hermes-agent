"""I17 prepared-successor custody helpers for the runtime coordinator."""

from __future__ import annotations


class I17PreparedChildCustodyMixin:
    """Authenticate the one hosted-room child that may restart while a close is held."""

    def _validate_i17_prepared_root_locked(self, successor, receipt, source_binding):
        from gateway.runtime_custody import (
            CustodyHandle, CustodyRefusal, DrainReceipt, TerminalRootRebindProof,
        )

        if (
            self._state != "closing"
            or not self._check_close_identity(receipt)
            or not isinstance(successor, CustodyHandle)
            or successor.owner_class != "I17_hosted_tui_services"
            or successor.close_generation != receipt.close_generation
        ):
            return CustodyRefusal("i17_rebind_capability_mismatch", "I17_hosted_tui_services")
        record = self._records.get(getattr(successor.lease, "lease_id", ""))
        if (
            record is None
            or record.handle is not successor
            or record.state not in {"accepted", "finalizing", "unknown"}
            or successor.coordinator_id != self._coordinator_id
            or successor.profile_id is None
            or type(successor.authority_epoch) is not int
            or successor.authority_epoch < 1
            or successor.process_instance != self.process_instance
        ):
            return CustodyRefusal("i17_rebind_capability_mismatch", "I17_hosted_tui_services")
        binding = self._source_rebind_bindings.get(
            ("I17_hosted_tui_services", str(successor.profile_id)),
        )
        if binding is None or binding is not source_binding:
            return CustodyRefusal("i17_rebind_capability_mismatch", "I17_hosted_tui_services")
        witness = self._rebound_successor_witnesses.get(successor.lease.lease_id)
        if witness is None:
            return CustodyRefusal("i17_rebind_capability_missing", "I17_hosted_tui_services")
        predecessor, witnessed_binding, proof, witnessed_receipt = witness
        if (
            witnessed_binding is not binding
            or not isinstance(witnessed_receipt, DrainReceipt)
            or witnessed_receipt.receipt_id != receipt.receipt_id
            or witnessed_receipt.process_instance != receipt.process_instance
            or witnessed_receipt.authority_epoch != receipt.authority_epoch
            or witnessed_receipt.close_generation != receipt.close_generation
            or not isinstance(proof, TerminalRootRebindProof)
            or proof.source_binding is not binding
            or proof.predecessor is not predecessor
            or proof.close_generation != receipt.close_generation
            or proof.profile_id != successor.profile_id
            or proof.authority_epoch != successor.authority_epoch
            or proof.process_instance != self.process_instance
            or proof.physical_owner_epoch != successor.physical_owner_epoch
            or self._rebound_terminal_roots.get(predecessor.lease.lease_id) is not predecessor
            or not self.is_terminal_handle(
                predecessor, owner_class="I17_hosted_tui_services",
                profile_id=successor.profile_id,
                authority_epoch=successor.authority_epoch,
            )
        ):
            return CustodyRefusal("i17_rebind_capability_mismatch", "I17_hosted_tui_services")
        authority = getattr(binding, "authority", None)
        registry = getattr(binding, "registry", None)
        if (
            authority is None
            or getattr(authority, "_i17_service_handle", None) is not successor
            or getattr(authority, "_i17_service_closed", False) is True
            or getattr(authority, "_i17_rebind_source", None) is not binding
            or getattr(authority, "_i17_producer_registry", None) is not registry
            or not isinstance(registry, dict)
            or registry.get("intake_closed") is not True
            or registry.get("authority") is not authority
            or registry.get("runner") is not getattr(binding, "runner", None)
            or registry.get("profile_id") != successor.profile_id
            or registry.get("authority_epoch") != successor.authority_epoch
            or registry.get("process_instance") != self.process_instance
        ):
            return CustodyRefusal("i17_rebind_capability_mismatch", "I17_hosted_tui_services")
        scope = self._profile_scope_result_locked(
            successor.profile_id, successor.authority_epoch, self.process_instance,
            require_serving=False, allow_retiring=True,
        )
        if (
            not scope.ok
            or type(scope.physical_owner_epoch) is not str
            or scope.physical_owner_epoch != successor.physical_owner_epoch
        ):
            return CustodyRefusal("foreign_custody", "I17_hosted_tui_services")
        return None

    def validate_i17_prepared_root(self, successor, receipt, source_binding):
        """Validate the exact prepared successor root without opening intake or the fence."""
        with self._lock:
            return self._validate_i17_prepared_root_locked(successor, receipt, source_binding)

    def i17_prepared_predecessor(self, successor, receipt, source_binding):
        """Return the exact terminal predecessor authenticated for one prepared successor."""
        with self._lock:
            refusal = self._validate_i17_prepared_root_locked(successor, receipt, source_binding)
            if refusal is not None:
                return refusal
            return self._rebound_successor_witnesses[successor.lease.lease_id][0]

    def admit_i17_prepared_child(
        self, parent, *, owner_id, profile_id, authority_epoch, receipt, source_binding,
    ):
        """Admit the authenticated I17 room child under a rebound root while CLOSING."""
        from gateway.runtime_custody import CustodyHandle, CustodyRefusal, _Record

        with self._lock:
            refusal = self._validate_i17_prepared_root_locked(parent, receipt, source_binding)
            if refusal is not None:
                return refusal
            if (
                type(owner_id) is not str or not owner_id
                or owner_id != f"room-service:{profile_id}"
                or profile_id != parent.profile_id
                or authority_epoch != parent.authority_epoch
            ):
                return CustodyRefusal("i17_rebind_capability_mismatch", "I17_hosted_tui_services")
            scope_refusal = self._scope_refusal_locked(
                profile_id, authority_epoch, "I17_hosted_tui_services",
                require_serving=False, allow_retiring=True,
                expected_physical_owner_epoch=parent.physical_owner_epoch,
            )
            if scope_refusal is not None:
                return scope_refusal
            parent_id = parent.lease.lease_id
            for child_id in tuple(self._children_by_parent.get(parent_id, ())):
                existing = self._records.get(child_id)
                if existing is None:
                    continue
                handle = existing.handle
                if (
                    handle.owner_class == "I17_hosted_tui_services"
                    and handle.owner_id == owner_id
                    and handle.profile_id == profile_id
                    and handle.authority_epoch == authority_epoch
                ):
                    return handle
            lease = self._fence.acquire_abort_child(
                self._close, parent.lease,
                owner_kind="I17_hosted_tui_services", owner_id=owner_id,
                profile_id=profile_id, coordinator_id=self._coordinator_id,
                source_transition=self._source_rebind_transition,
            )
            if lease is None:
                return CustodyRefusal("i17_child_admission_refused", "I17_hosted_tui_services")
            handle = CustodyHandle(
                "I17_hosted_tui_services", owner_id, profile_id, authority_epoch,
                self.process_instance, receipt.close_generation, lease,
                self._coordinator_id, parent.physical_owner_epoch,
            )
            self._records[lease.lease_id] = _Record(handle)
            self._children_by_parent.setdefault(parent_id, set()).add(lease.lease_id)
            return handle
