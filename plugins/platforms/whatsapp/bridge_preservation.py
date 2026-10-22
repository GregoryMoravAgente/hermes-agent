"""Preservation lifecycle methods for the authenticated WhatsApp bridge."""

from __future__ import annotations

import asyncio
from uuid import uuid4


class BridgePreservationMixin:
    def _preservation_close_source(self, close_generation: int) -> bool:
        current = getattr(self, "_preservation_source_close_generation", None)
        if current is not None and current != close_generation:
            return False
        if current is None:
            previous_resume = getattr(self, "_preservation_bridge_resume_task", None)
            if previous_resume is not None and not previous_resume.done():
                return False
            # A completed task belongs to the preceding generation. Never let
            # its open response satisfy the new generation's resume contract.
            self._preservation_bridge_resume_task = None
        self._preservation_source_close_generation = close_generation
        self._preservation_bridge_close_generation = close_generation
        self._preservation_bridge_resume_result = False
        self._preservation_bridge_snapshot = None
        if not getattr(self, "_http_session", None):
            self._preservation_source_unknown(self, "whatsapp bridge close binding unavailable")
            return False
        existing = getattr(self, "_preservation_bridge_close_task", None)
        if existing is not None and not existing.done():
            return True
        try:
            self._preservation_bridge_close_task = asyncio.create_task(
                self._bridge_control("close", close_generation)
            )
        except (RuntimeError, TypeError):
            return False
        return True

    def _preservation_resume_source(self, close_generation: int) -> bool:
        if getattr(self, "_preservation_source_close_generation", None) != close_generation:
            return False
        if not getattr(self, "_http_session", None):
            return False
        if getattr(self, "_preservation_bridge_resume_result", False):
            self._preservation_source_close_generation = None
            self._preservation_bridge_close_generation = None
            self._preservation_bridge_resume_result = False
            self._preservation_bridge_snapshot = None
            return True
        existing = getattr(self, "_preservation_bridge_resume_task", None)
        if existing is not None and not existing.done():
            return True
        try:
            self._preservation_bridge_resume_task = asyncio.create_task(
                self._bridge_control("resume", close_generation)
            )
        except (RuntimeError, TypeError):
            return False
        return True

    async def _preservation_await_source_close(self, close_generation: int, timeout: float) -> bool:
        task = getattr(self, "_preservation_bridge_close_task", None)
        http_session = getattr(self, "_http_session", None)
        if not http_session or task is None:
            self._preservation_source_unknown(self, "whatsapp bridge close receipt unavailable")
            return False
        try:
            result = await asyncio.wait_for(asyncio.shield(task), timeout=max(0.0, timeout))
        except (asyncio.CancelledError, OSError, RuntimeError, TimeoutError, TypeError, ValueError):
            return False
        if not isinstance(result, dict) or result.get("status") != "closing":
            return False
        from gateway.preservation_source_close import SourcePendingSnapshot

        def _decode_bridge_snapshot(value):
            if not isinstance(value, dict):
                return None
            if getattr(self, "_bridge_capability_required", False):
                state = value.get("producer_state")
                if state not in {"unknown", "connected", "disconnected"}:
                    return None
            else:
                value = {**value, "producer_state": "not_required"}
            try:
                return SourcePendingSnapshot(**{
                    key: value.get(key)
                    for key in SourcePendingSnapshot.__dataclass_fields__
                    if key in value
                })
            except (TypeError, ValueError):
                return None

        snapshot = result.get("snapshot")
        decoded = _decode_bridge_snapshot(snapshot)
        if decoded is None:
            return False
        required = {"close_generation", "queue_epoch", "bridge_instance", "owner_epoch"}
        if not required.issubset(snapshot):
            return False
        self._preservation_bridge_snapshot = decoded
        bound = self._queue_binding()

        def _matches_binding(current):
            owner_binding = current.owner_binding
            return (
                current.close_generation == close_generation
                and bound is not None
                and current.queue_epoch == bound.get("queue_epoch")
                and current.bridge_instance == bound.get("bridge_instance")
                and current.owner_epoch == bound.get("owner_epoch")
                and isinstance(owner_binding, dict)
                and owner_binding.get("consumer_id") == bound.get("consumer_id")
                and owner_binding.get("consumer_epoch") == bound.get("consumer_epoch")
                and (
                    not getattr(self, "_bridge_capability_required", False)
                    or (
                        current.producer_state == "connected"
                        and isinstance(current.producer_receipt_digest, str)
                        and bool(current.producer_receipt_digest)
                        and isinstance(current.producer_generation, str)
                        and current.producer_generation == getattr(
                            self, "_bridge_producer_generation", current.producer_generation
                        )
                    )
                )
            )

        if not _matches_binding(self._preservation_bridge_snapshot):
            return False
        if self._preservation_bridge_snapshot.preclose_pending:
            drained = await self._drain_bridge_prefix(close_generation, timeout)
            if not drained:
                return False
        refreshed = await self._bridge_json("get", "health", timeout=min(timeout, 5))
        if not isinstance(refreshed, tuple) or refreshed[0] != 200 or not isinstance(refreshed[1], dict):
            return False
        health_queue = refreshed[1].get("queue")
        health_snapshot = _decode_bridge_snapshot(health_queue)
        if health_snapshot is None or bound is None:
            return False
        if (
            health_snapshot.close_generation != close_generation
            or health_snapshot.queue_epoch != bound.get("queue_epoch")
            or health_snapshot.bridge_instance != bound.get("bridge_instance")
            or health_snapshot.owner_epoch != bound.get("owner_epoch")
            or (
                getattr(self, "_bridge_capability_required", False)
                and (
                    health_snapshot.producer_state != "connected"
                    or not health_snapshot.producer_receipt_digest
                    or health_snapshot.producer_generation != getattr(
                        self, "_bridge_producer_generation", health_snapshot.producer_generation
                    )
                )
            )
        ):
            return False
        # /health is intentionally not consumer-bound, so obtain the final
        # snapshot through the authenticated route before accepting custody.
        current = await self._bridge_json("get", "messages", timeout=min(timeout, 5))
        if not isinstance(current, tuple) or current[0] != 200:
            return False
        current_snapshot = _decode_bridge_snapshot(current[1])
        if current_snapshot is None or not _matches_binding(current_snapshot):
            return False
        self._preservation_bridge_snapshot = current_snapshot
        registry = self._preservation_source_registry()
        if registry is not None:
            registry._signal()
        return bool(self._preservation_bridge_snapshot.validated and self._preservation_bridge_snapshot.drain_complete)

    async def _drain_bridge_prefix(self, close_generation: int, timeout: float) -> bool:
        """Consume only the durable pre-close prefix through the normal poll path."""
        deadline = asyncio.get_running_loop().time() + max(0.0, timeout)
        while asyncio.get_running_loop().time() < deadline:
            binding = await self._ensure_queue_binding()
            if binding is None:
                return False
            receipt = getattr(self, "_preservation_bridge_snapshot", None)
            if receipt is None or receipt.close_generation != close_generation:
                return False
            request_id = uuid4().hex
            result = await self._bridge_operation(
                "messages/drain-claim",
                {
                    "protocol": "hermes.whatsapp.preclose-drain.v1",
                    "request_id": request_id,
                    "close_generation": close_generation,
                    "watermark": receipt.watermark,
                    "close_receipt_digest": getattr(receipt, "close_receipt_digest", None)
                    or getattr(receipt, "receipt_digest", None),
                    "limit": 1,
                    "binding": binding,
                },
                timeout=min(10, max(0.5, deadline - asyncio.get_running_loop().time())),
            )
            if not isinstance(result, dict):
                return False
            if result.get("status") == "drained":
                return True
            if result.get("status") == "blocked":
                await asyncio.sleep(min(0.25, max(0.01, deadline - asyncio.get_running_loop().time())))
                continue
            if result.get("status") != "claimed":
                return False
            item = result.get("item") or {}
            payload = item.get("payload") if isinstance(item, dict) else None
            if not isinstance(payload, dict):
                return False
            delivery = {
                "queue_epoch": binding.get("queue_epoch"),
                "bridge_instance": binding.get("bridge_instance"),
                "owner_epoch": binding.get("owner_epoch"),
                "consumer_id": result.get("binding", {}).get("consumer_id", binding.get("consumer_id")),
                "consumer_epoch": result.get("binding", {}).get("consumer_epoch", binding.get("consumer_epoch")),
                "item_id": item.get("item_id"),
                "sequence": item.get("sequence"),
                "close_generation": item.get("close_generation", close_generation),
                "claim_id": result.get("claim_id"),
                "custody_ref": result.get("custody_ref"),
                "settlement_challenge": result.get("settlement_challenge", item.get("settlement_challenge")),
                "producer_generation": item.get(
                    "producer_generation", binding.get("producer_generation")
                ),
                "wire_identity": item.get("wire_identity"),
                "state": "claimed",
            }
            await self._dispatch_polled_message({**payload, "_bridge_payload": payload, "_bridge_delivery": delivery})
        return False

    async def _preservation_begin_resume(self, close_generation: int, deadline: float) -> bool:
        """Start resume only after the same authenticated close handshake finished."""
        close_task = getattr(self, "_preservation_bridge_close_task", None)
        if close_task is None:
            return False
        closed = await asyncio.wait_for(
            asyncio.shield(close_task),
            timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
        )
        if not isinstance(closed, dict) or closed.get("status") != "closing":
            return False
        snapshot = closed.get("snapshot")
        binding = self._queue_binding()
        if not isinstance(snapshot, dict) or not isinstance(binding, dict):
            return False
        owner_binding = snapshot.get("owner_binding")
        if not isinstance(owner_binding, dict):
            return False
        keys = ("queue_epoch", "bridge_instance", "owner_epoch")
        consumer_keys = ("consumer_id", "consumer_epoch")
        if (
            snapshot.get("close_generation") != close_generation
            or any(snapshot.get(key) != binding.get(key) for key in keys)
            or any(owner_binding.get(key) != binding.get(key) for key in consumer_keys)
        ):
            return False
        return self._preservation_resume_source(close_generation)

    async def _preservation_await_source_resume(self, close_generation: int, timeout: float) -> bool:
        if (
            getattr(self, "_preservation_source_close_generation", None) != close_generation
            or not getattr(self, "_http_session", None)
        ):
            return False
        if getattr(self, "_preservation_bridge_resume_result", False):
            return True
        deadline = asyncio.get_running_loop().time() + max(0.0, timeout)
        task = getattr(self, "_preservation_bridge_resume_task", None)
        try:
            if task is not None and task.done():
                previous = task.result()
                if isinstance(previous, dict) and previous.get("status") == "open":
                    self._preservation_bridge_resume_result = True
                    return True
            if task is None or task.done():
                if not await self._preservation_begin_resume(close_generation, deadline):
                    return False
                task = self._preservation_bridge_resume_task
            result = await asyncio.wait_for(
                asyncio.shield(task),
                timeout=max(0.0, deadline - asyncio.get_running_loop().time()),
            )
        except (asyncio.CancelledError, OSError, RuntimeError, TimeoutError, TypeError, ValueError):
            return False
        opened = isinstance(result, dict) and result.get("status") == "open"
        if opened:
            # The registry's abort transition consumes this result and clears
            # the generation fence only after authenticated transport reopening.
            self._preservation_bridge_resume_result = True
        return opened

    def _preservation_source_pending(self, close_generation: int):
        if getattr(self, "_preservation_source_close_generation", None) != close_generation:
            return ("source_close_generation_mismatch",)
        snapshot = getattr(self, "_preservation_bridge_snapshot", None)
        if snapshot is not None:
            return snapshot
        if not getattr(self, "_http_session", None):
            return ("bridge_close_unproven",)
        return ("bridge_close_pending",)
