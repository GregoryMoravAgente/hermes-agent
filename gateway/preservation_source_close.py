"""Exact source-owned admission contracts for reversible preservation close.

The runtime coordinator owns durable custody.  This module owns the smaller interval before a
producer callback reaches that coordinator: the producer must register an actual per-instance
close gate, issue an opaque admission token before it acknowledges or queues work, and transfer
that token to the native callback or an explicit held/replay path.  There is deliberately no
class-level monkey patch and no inference from empty adapter dictionaries.
"""
from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
import inspect
import hashlib
import json
import secrets
from typing import Any, Callable, Iterable

from hermes_state_runtime import RuntimeStoreError


_LEASE_ATTR = "_preservation_source_lease"
_TOKENS_ATTR = "_preservation_source_tokens"
_SOURCE_HANDOFF: ContextVar[tuple[SourceAdmissionToken, ...]] = ContextVar(
    "preservation_source_handoff", default=()
)


@contextmanager
def source_handoff_context(tokens: Iterable[SourceAdmissionToken] | None):
    """Carry one explicit producer handoff through Base scheduling into native ingress."""
    value = tuple(token for token in (tokens or ()) if isinstance(token, SourceAdmissionToken))
    marker = _SOURCE_HANDOFF.set(value)
    try:
        yield value
    finally:
        _SOURCE_HANDOFF.reset(marker)


def current_source_handoff() -> tuple[SourceAdmissionToken, ...]:
    return _SOURCE_HANDOFF.get()


def _safe_contract_call(callback, *args: Any, default: Any = None) -> Any:
    if not callable(callback):
        return default
    try:
        return callback(*args)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return default


@dataclass(frozen=True)
class SourcePendingSnapshot:
    """Durable source inventory returned by a registered producer close hook."""

    close_generation: int | None
    queue_epoch: str | None
    bridge_instance: str | None
    owner_epoch: int | None
    watermark: int = 0
    pending: int = 0
    preclose_pending: int = 0
    postclose_retained: int = 0
    active_claims: int = 0
    accepted: int = 0
    unknown: int = 0
    acked: int = 0
    storage_fault: object | None = None
    hidden_head: bool = False
    validated: bool = False
    drain_complete: bool = False
    close_receipt_digest: str | None = None
    owner_binding: object | None = None
    # Other source adapters do not have the WhatsApp producer lifecycle and
    # retain their existing queue-only close contract.
    producer_state: str = "not_required"
    producer_receipt_digest: str | None = None
    # A connected producer receipt is scoped to one bridge generation.  Keep
    # that generation in the close snapshot so a reconnect cannot satisfy an
    # older close with a predecessor's readiness proof.
    producer_generation: str | None = None

    @property
    def ready(self) -> bool:
        return bool(
            self.validated
            and self.drain_complete
            and self.storage_fault is None
            and not self.hidden_head
            and self.active_claims == 0
            and self.accepted == 0
            and self.unknown == 0
            and (
                self.producer_state == "not_required"
                or (
                    self.producer_state == "connected"
                    and isinstance(self.producer_receipt_digest, str)
                    and bool(self.producer_receipt_digest)
                    and isinstance(self.producer_generation, str)
                    and bool(self.producer_generation)
                )
            )
        )


@dataclass(frozen=True)
class SourceFinalizerReceipt:
    """Immutable observation emitted by the actual source/background finalizer."""

    protocol: str
    status: str
    reason: str
    registration_generation: int
    native_occurrence_id: str
    bridge_binding_digest: str | None
    producer_generation: str | None
    transport_receipt_digest: str | None
    queue_epoch: str | None
    bridge_instance: str | None
    owner_epoch: int | None
    close_generation: int | None
    item_id: str | None
    sequence: int | None
    custody_ref: str | None
    finalizer_digest: str

    @classmethod
    def build(cls, token: SourceAdmissionToken, *, status: str, reason: str = ""):
        delivery = token.transport_receipt if isinstance(token.transport_receipt, dict) else {}
        identity = {
            "registration_generation": token.registration_generation,
            "native_occurrence_id": token.native_occurrence_id,
            "bridge_binding_digest": token.bridge_binding_digest,
            "producer_generation": getattr(token, "producer_generation", None),
            "transport_receipt_digest": token.transport_receipt_digest,
            "queue_epoch": delivery.get("queue_epoch"),
            "bridge_instance": delivery.get("bridge_instance"),
            "owner_epoch": delivery.get("owner_epoch"),
            "close_generation": delivery.get("close_generation"),
            "item_id": delivery.get("item_id"),
            "sequence": delivery.get("sequence"),
            "custody_ref": token.custody_ref or delivery.get("custody_ref"),
            "status": status,
            "reason": reason,
        }
        encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        return cls(
            protocol="hermes.whatsapp.source-finalizer.v1",
            status=status,
            reason=reason,
            registration_generation=token.registration_generation,
            native_occurrence_id=str(token.native_occurrence_id or ""),
            bridge_binding_digest=token.bridge_binding_digest,
            producer_generation=getattr(token, "producer_generation", None),
            transport_receipt_digest=token.transport_receipt_digest,
            queue_epoch=delivery.get("queue_epoch"),
            bridge_instance=delivery.get("bridge_instance"),
            owner_epoch=delivery.get("owner_epoch"),
            close_generation=delivery.get("close_generation"),
            item_id=delivery.get("item_id"),
            sequence=delivery.get("sequence"),
            custody_ref=token.custody_ref or delivery.get("custody_ref"),
            finalizer_digest=hashlib.sha256(encoded.encode("utf-8")).hexdigest(),
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "protocol": self.protocol,
            "status": self.status,
            "reason": self.reason,
            "registration_generation": self.registration_generation,
            "native_occurrence_id": self.native_occurrence_id,
            "bridge_binding_digest": self.bridge_binding_digest,
            "producer_generation": self.producer_generation,
            "transport_receipt_digest": self.transport_receipt_digest,
            "queue_epoch": self.queue_epoch,
            "bridge_instance": self.bridge_instance,
            "owner_epoch": self.owner_epoch,
            "close_generation": self.close_generation,
            "item_id": self.item_id,
            "sequence": self.sequence,
            "custody_ref": self.custody_ref,
            "finalizer_digest": self.finalizer_digest,
        }


@dataclass(frozen=True)
class SourceCloseContract:
    """The source-owned lifecycle seam exposed by one registered adapter instance."""

    owner: object
    name: str
    close: Callable[[int], bool]
    resume: Callable[[int], bool]
    pending: Callable[[int], Iterable[Any] | SourcePendingSnapshot]
    supported: bool = True
    await_close: Callable[[int, float], Any] | None = None
    await_resume: Callable[[int, float], Any] | None = None

    @classmethod
    def for_adapter(
        cls, adapter: object, name: str | None = None, *, supported: bool = True,
    ) -> SourceCloseContract:
        label = name or str(getattr(getattr(adapter, "platform", None), "value", "adapter"))

        def _close(generation: int) -> bool:
            callback = getattr(adapter, "_preservation_close_source", None)
            if callable(callback):
                return bool(callback(generation))
            current = getattr(adapter, "_preservation_source_close_generation", None)
            if current is not None and current != generation:
                return False
            adapter._preservation_source_close_generation = generation
            return True

        def _resume(generation: int) -> bool:
            callback = getattr(adapter, "_preservation_resume_source", None)
            if callable(callback):
                return bool(callback(generation))
            if getattr(adapter, "_preservation_source_close_generation", None) != generation:
                return False
            adapter._preservation_source_close_generation = None
            return True

        def _pending(generation: int) -> tuple | SourcePendingSnapshot:
            callback = getattr(adapter, "_preservation_source_pending", None)
            if callable(callback):
                value = callback(generation)
                return value if isinstance(value, SourcePendingSnapshot) else tuple(value or ())
            if getattr(adapter, "_preservation_source_close_generation", None) != generation:
                return ("source_close_generation_mismatch",)
            return tuple(getattr(adapter, "_preservation_source_pending_tokens", ()) or ())

        async def _await_close(generation: int, timeout: float) -> bool:
            callback = getattr(adapter, "_preservation_await_source_close", None)
            if not callable(callback):
                return True
            result = callback(generation, timeout)
            if inspect.isawaitable(result):
                result = await result
            return bool(result)

        async def _await_resume(generation: int, timeout: float) -> bool:
            callback = getattr(adapter, "_preservation_await_source_resume", None)
            if not callable(callback):
                return True
            result = callback(generation, timeout)
            if inspect.isawaitable(result):
                result = await result
            return bool(result)

        return cls(
            owner=adapter,
            name=label,
            close=_close,
            resume=_resume,
            pending=_pending,
            supported=supported,
            await_close=_await_close,
            await_resume=_await_resume,
        )


@dataclass(eq=False)
class SourceAdmissionToken:
    """Opaque identity for one accepted source item."""

    registration: _SourceRegistration
    marker: object
    phase: str
    event: object | None = None
    native_entered: bool = False
    native_exited: bool = False
    producer_done: bool = False
    state: str = "accepted"
    close_generation: int | None = None
    identity: tuple[Any, ...] = field(default_factory=tuple)
    invocation_id: int = 0
    custody_ref: str | None = None
    transport_receipt: object | None = None
    transport_settled: bool = True
    transport_settlement: Callable[..., Any] | None = None
    transport_settlement_started: bool = False
    # Cross-process transports keep these immutable occurrence fields on the
    # source token.  They are observations owned by the admission/finalizer
    # path; HTTP callers cannot replace them with public payload values.
    native_occurrence_id: str | None = None
    bridge_binding_digest: str | None = None
    wire_descriptor_digest: str | None = None
    source_event_digest: str | None = None
    source_identity_digest: str | None = None
    transport_owner_digest: str | None = None
    transport_receipt_digest: str | None = None
    transport_status: str | None = None
    settlement_request_id: str | None = None
    settlement_witness: object | None = None
    settlement_receipt: object | None = None
    finalizer_receipt: SourceFinalizerReceipt | None = None
    producer_generation: str | None = None

    @property
    def adapter(self) -> object:
        return self.registration.adapter

    @property
    def profile(self) -> str | None:
        return self.registration.profile

    @property
    def platform(self) -> object:
        return self.registration.platform

    @property
    def registration_generation(self) -> int:
        return self.registration.generation

    @property
    def transport_owner(self) -> object:
        return self.registration.transport_owner

    def build_settlement_witness(self, key: bytes, receipt: object | None = None):
        """Sign the exact live occurrence after producer/native settlement."""
        from gateway.whatsapp_bridge_witness import build_settlement_witness
        witness = build_settlement_witness(self, self.transport_receipt or {}, key, receipt)
        self.settlement_witness = witness
        return witness

    def record_finalizer_receipt(self, *, status: str, reason: str = "") -> SourceFinalizerReceipt:
        if status not in {"success", "unknown", "failed"}:
            raise ValueError("invalid source finalizer status")
        if self.native_occurrence_id is None:
            raise ValueError("native occurrence is required")
        receipt = SourceFinalizerReceipt.build(self, status=status, reason=reason)
        self.finalizer_receipt = receipt
        self._signal_finalizer()
        return receipt

    def record_finalizer_or_unknown(
        self, *, status: str, reason: str = "", unknown_reason: str,
    ) -> bool:
        """Record the actual finalizer outcome or retain the token as unknown."""
        try:
            self.record_finalizer_receipt(status=status, reason=reason)
        except (AttributeError, RuntimeError, TypeError, ValueError):
            registry = getattr(self.adapter, "_preservation_source_close", None)
            mark_unknown = getattr(registry, "mark_unknown", None)
            if callable(mark_unknown):
                mark_unknown(self, unknown_reason)
            return False
        return True

    def _signal_finalizer(self) -> None:
        registry = getattr(self.adapter, "_preservation_source_close", None)
        if registry is not None:
            registry._signal()
            # The receipt is the source-owned finalizer boundary.  Re-evaluate
            # the exact token now so its authenticated transport settlement can
            # run; a failed settlement remains UNKNOWN in the registry.
            registry._maybe_release(self)

    def capture_bridge_delivery(self, delivery: object) -> dict[str, Any]:
        """Capture source-owned bridge facts before the adapter performs acceptance."""
        if not isinstance(delivery, dict):
            raise ValueError("bridge delivery is required")
        from gateway.whatsapp_bridge_witness import digest, source_identity_projection

        self.producer_generation = delivery.get("producer_generation")
        projection = source_identity_projection(self, delivery)
        self.bridge_binding_digest = digest(projection["bridge_binding"])
        self.wire_descriptor_digest = digest(
            delivery.get("wire_identity", delivery.get("payload", {})),
        )
        self.source_identity_digest = digest(projection["source_identity"])
        self.source_event_digest = digest(projection["source_event"])
        self.transport_owner_digest = digest(projection["transport_owner"])
        self.transport_receipt_digest = digest({
            key: delivery.get(key)
            for key in (
                "item_id", "sequence", "claim_id", "custody_ref", "settlement_challenge",
                "wire_identity",
            )
        })
        self.transport_status = "pending"
        fields = {
            "queue_epoch": delivery.get("queue_epoch"),
            "bridge_instance": delivery.get("bridge_instance"),
            "owner_epoch": delivery.get("owner_epoch"),
            "consumer_id": delivery.get("consumer_id"),
            "consumer_epoch": delivery.get("consumer_epoch"),
            "close_generation": delivery.get("close_generation"),
            "claim_id": delivery.get("claim_id"),
            "item_id": delivery.get("item_id"),
            "sequence": delivery.get("sequence"),
            "custody_ref": delivery.get("custody_ref"),
            "settlement_challenge": delivery.get("settlement_challenge"),
            "bridge_binding_digest": self.bridge_binding_digest,
            "wire_descriptor_digest": self.wire_descriptor_digest,
            "source_event_digest": self.source_event_digest,
            "source_identity_digest": self.source_identity_digest,
            "transport_owner_digest": self.transport_owner_digest,
            "transport_receipt_digest": self.transport_receipt_digest,
            "native_occurrence_id": self.native_occurrence_id or projection["source_identity"]["native_occurrence_id"],
            "registration_generation": self.registration_generation,
            "transport_status": "pending",
        }
        self._settlement_fields = fields
        delivery["_settlement_fields"] = fields
        return fields


# Kept as a private compatibility name for callers/tests written against R1.
_SourceLease = SourceAdmissionToken


@dataclass(eq=False)
class _SourceRegistration:
    adapter: object
    profile: str | None
    platform: object
    generation: int
    contract: SourceCloseContract
    contract_marker: tuple[Any, ...]
    handler_marker: object
    transport_owner: object
    close_generation: int | None = None
    retired: bool = False


@dataclass(frozen=True)
class _SourceRefusalWitness:
    """Identity-bound proof for a source callback refused before admission."""

    event: object
    source_identity: object
    marker: object
    adapter: object
    profile: str | None
    transport_owner: object
    registration: _SourceRegistration
    registration_generation: int
    close_generation: int
    reason: str


class PreservationSourceClose:
    """Track registered producer instances and their exact accepted source tokens."""

    def __init__(self, runner):
        self.runner = runner
        self._registrations: dict[int, _SourceRegistration] = {}
        self._retired_registrations: list[_SourceRegistration] = []
        self._leases: set[SourceAdmissionToken] = set()
        self._event_tokens: dict[int, list[SourceAdmissionToken]] = {}
        self._unknown_sources: dict[object, object] = {}
        self._registration_generation = 0
        self._invocation_id = 0
        self._refusal_id = 0
        self._state = "open"
        self._close_generation: int | None = None
        self._wake: asyncio.Event | None = None
        self._wake_loop = None

    @property
    def state(self) -> str:
        return self._state

    @property
    def close_generation(self) -> int | None:
        return self._close_generation

    @property
    def registrations(self) -> tuple[_SourceRegistration, ...]:
        return tuple(self._registrations.values()) + tuple(self._retired_registrations)

    @property
    def active_leases(self) -> tuple[SourceAdmissionToken, ...]:
        return tuple(self._leases)

    def register_adapter(self, adapter, profile: str | None = None) -> None:
        """Register one concrete adapter instance before it can receive source work."""
        adapter_id = id(adapter)
        if self._state == "closing":
            self._unknown_sources[("late_registration", adapter_id)] = "late_source_registration"
            raise RuntimeStoreError("runtime_draining")

        contract_factory = getattr(adapter, "_preservation_source_contract", None)
        contract = contract_factory() if callable(contract_factory) else None
        install_instance_guard = contract is None
        if contract is None:
            # Small test doubles and legacy direct adapters have a real handle_message method but
            # no pre-handler queue. They still get an exact Base-style callback gate. A source
            # with no callable handler is unsupported and stays UNKNOWN.
            if callable(getattr(adapter, "handle_message", None)):
                contract = SourceCloseContract.for_adapter(
                    adapter, name="direct-callback", supported=False,
                )
            else:
                self._unknown_sources[("unsupported", adapter_id)] = "source_contract_missing"
                return
        if not isinstance(contract, SourceCloseContract) or contract.owner is not adapter:
            self._unknown_sources[("unsupported", adapter_id)] = "source_contract_invalid"
            return

        if install_instance_guard and not getattr(adapter, "_preservation_source_instance_guard", False):
            original = getattr(adapter, "handle_message", None)

            async def _guarded(event):
                lease = self.admit_public(adapter, event, profile)
                try:
                    result = original(event)
                    if inspect.isawaitable(result):
                        result = await result
                except (asyncio.CancelledError, Exception):
                    if not bool(getattr(event, "_gateway_accepted", False)):
                        self.reconcile(lease, "refused_before_accept")
                    else:
                        self.mark_unknown(lease, "standalone_callback_failed")
                    raise
                if not bool(getattr(event, "_gateway_accepted", False)):
                    self.reconcile(lease, "refused_before_accept")
                else:
                    self.mark_producer_done(lease)
                return result

            # This is an instance-only compatibility gate for direct adapters that expose no
            # source contract. Production BasePlatformAdapter instances use their explicit seam.
            adapter.handle_message = _guarded
            adapter._preservation_source_instance_guard = True
        handler_marker = self._handler_marker(adapter)
        contract_marker = self._contract_marker(adapter, contract)
        prior = self._registrations.get(adapter_id)
        retired_prior = None
        if prior is not None and (prior.profile != profile or prior.platform != getattr(adapter, "platform", None)):
            # A profile/adapter swap creates a new registration generation. Any old token remains
            # attached to its original identity and blocks until explicitly replayed/reconciled.
            prior.retired = True
            self._retired_registrations.append(prior)
            self._unknown_sources[("replacement", prior.generation)] = "adapter_or_profile_replaced"
            retired_prior = prior
            prior = None
        if prior is None:
            self._registration_generation += 1
            prior = _SourceRegistration(
                adapter=adapter,
                profile=profile,
                platform=getattr(adapter, "platform", None),
                generation=self._registration_generation,
                contract=contract,
                contract_marker=contract_marker,
                handler_marker=handler_marker,
                transport_owner=adapter,
            )
            self._registrations[adapter_id] = prior
        else:
            # Re-wiring the same instance must not silently replace its handler or contract.
            if prior.handler_marker != handler_marker or prior.contract_marker != contract_marker:
                if any(token.registration is prior for token in self._leases):
                    self._unknown_sources[("mutation", prior.generation)] = (
                        "source_contract_or_handler_replaced"
                    )
                    self._signal()
                    return
                prior.retired = True
                self._retired_registrations.append(prior)
                self._unknown_sources[("replacement", prior.generation)] = (
                    "source_contract_or_handler_replaced"
                )
                self._registrations.pop(adapter_id, None)
                self._cleanup_retired(prior)
                self._registration_generation += 1
                prior = _SourceRegistration(
                    adapter=adapter,
                    profile=profile,
                    platform=getattr(adapter, "platform", None),
                    generation=self._registration_generation,
                    contract=contract,
                    contract_marker=contract_marker,
                    handler_marker=handler_marker,
                    transport_owner=adapter,
                )
                self._registrations[adapter_id] = prior
            prior.profile = profile
        adapter._preservation_source_close = self
        adapter._preservation_source_profile = profile
        if retired_prior is not None:
            self._cleanup_retired(retired_prior)
        self._signal()

    @staticmethod
    def _handler_marker(adapter: object) -> object:
        handler = getattr(adapter, "handle_message", None)
        return getattr(handler, "__func__", handler)

    @staticmethod
    def _callable_signature(callback: object) -> object:
        callback = getattr(callback, "__func__", callback)
        code = getattr(callback, "__code__", None)
        if code is None:
            return callback
        return (code.co_filename, code.co_firstlineno, code.co_name, code.co_code)

    @classmethod
    def _contract_marker(cls, adapter: object, contract: SourceCloseContract) -> tuple[Any, ...]:
        return (
            cls._callable_signature(getattr(adapter, "_preservation_source_contract", None)),
            contract.name,
            bool(contract.supported),
            cls._callable_signature(contract.close),
            cls._callable_signature(contract.resume),
            cls._callable_signature(contract.pending),
            cls._callable_signature(contract.await_close),
            cls._callable_signature(contract.await_resume),
            cls._handler_marker(getattr(adapter, "_preservation_close_source", None)),
            cls._handler_marker(getattr(adapter, "_preservation_resume_source", None)),
            cls._handler_marker(getattr(adapter, "_preservation_source_pending", None)),
        )

    @staticmethod
    def _source_identity(event: object) -> object:
        source = getattr(event, "source", None)
        to_dict = getattr(source, "to_dict", None)
        if not callable(to_dict):
            return source
        try:
            return tuple(sorted(to_dict().items()))
        except (AttributeError, TypeError, ValueError):
            return source

    def _current_contract(self, adapter: object) -> SourceCloseContract | None:
        factory = getattr(adapter, "_preservation_source_contract", None)
        try:
            contract = factory() if callable(factory) else None
        except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
            contract = None
        if contract is not None:
            return contract if isinstance(contract, SourceCloseContract) else None
        if callable(getattr(adapter, "handle_message", None)):
            return SourceCloseContract.for_adapter(
                adapter, name="direct-callback", supported=False,
            )
        return None

    def _registration_is_current(self, registration: _SourceRegistration) -> bool:
        if registration.retired:
            return False
        adapter = registration.adapter
        contract = self._current_contract(adapter)
        if (
            contract is None
            or contract.owner is not adapter
            or self._handler_marker(adapter) != registration.handler_marker
            or self._contract_marker(adapter, contract) != registration.contract_marker
            or getattr(adapter, "platform", None) != registration.platform
            or getattr(adapter, "_preservation_source_profile", registration.profile) != registration.profile
        ):
            self._unknown_sources[("mutation", registration.generation)] = (
                "source_contract_or_handler_replaced"
            )
            self._signal()
            return False
        return True

    def _all_registrations(self) -> tuple[_SourceRegistration, ...]:
        return self.registrations

    def _cleanup_retired(self, registration: _SourceRegistration) -> None:
        if not registration.retired or any(token.registration is registration for token in self._leases):
            return
        self._retired_registrations = [
            item for item in self._retired_registrations if item is not registration
        ]
        self._unknown_sources.pop(("replacement", registration.generation), None)
        self._unknown_sources.pop(("registration", registration.generation), None)
        self._unknown_sources.pop(("mutation", registration.generation), None)

    def _registration_for_adapter(self, adapter: object, profile: str | None = None) -> _SourceRegistration | None:
        registration = self._registrations.get(id(adapter))
        if registration is None or registration.adapter is not adapter:
            return None
        if profile is not None and registration.profile != profile:
            return None
        if not self._registration_is_current(registration):
            return None
        return registration

    def _registration_for_event(self, event, profile: str | None) -> _SourceRegistration | None:
        owner = None
        source = getattr(event, "source", None)
        transport_owner = getattr(self.runner, "_transport_owner", None)
        if callable(transport_owner) and source is not None:
            try:
                owner = transport_owner(source)
            except (AttributeError, RuntimeError, TypeError, ValueError):
                owner = None
        if isinstance(owner, tuple) and len(owner) == 2:
            adapter, owner_profile = owner
            if profile is not None and owner_profile != profile:
                return None
            return self._registration_for_adapter(adapter, owner_profile)
        return None

    def _refuse_new_callback(self, event=None, reason: str = "runtime_draining"):
        if event is not None:
            try:
                event._gateway_accepted = False
            except AttributeError:
                pass
        raise RuntimeStoreError(reason)

    def _ensure_open(self, event=None) -> None:
        if self._state == "closing":
            self._refuse_new_callback(event)

    @staticmethod
    def _normalize_tokens(value) -> tuple[SourceAdmissionToken, ...]:
        if isinstance(value, SourceAdmissionToken):
            return (value,)
        if isinstance(value, (tuple, list, set, frozenset)):
            return tuple(token for token in value if isinstance(token, SourceAdmissionToken))
        return ()

    def handoff_source(
        self, adapter: object, source_tokens, *, profile: str | None = None,
        event: object | None = None,
    ) -> tuple[SourceAdmissionToken, ...]:
        """Validate an explicit producer-to-Base handoff without event-keyed token reuse."""
        registration = self._registration_for_adapter(adapter, profile)
        if isinstance(source_tokens, (tuple, list, set, frozenset)) and any(
                not isinstance(item, SourceAdmissionToken) for item in source_tokens):
            self._unknown_sources[('handoff', id(source_tokens))] = 'source_witness_invalid'
            return self._refuse_new_callback(event, 'source_identity_mismatch')
        tokens = self._normalize_tokens(source_tokens)
        if registration is None or not tokens:
            if registration is None:
                self._unknown_sources[("adapter", id(adapter))] = "source_registration_missing"
            return self._refuse_new_callback(event, "source_registration_missing")
        for token in tokens:
            expected_identity = (
                registration.adapter, registration.profile, registration.transport_owner,
                registration.generation, self._close_generation,
            )
            if token not in self._leases or token.registration is not registration:
                self.mark_unknown(token, "source_handoff_registration_mismatch")
                return self._refuse_new_callback(event, "source_identity_mismatch")
            if token.state == "unknown" or token.identity != expected_identity:
                self.mark_unknown(token, "source_handoff_identity_mismatch")
                return self._refuse_new_callback(event, "source_identity_mismatch")
            if event is not None:
                self.bind_event(token, event)
        return tokens

    def admit_source(
        self,
        adapter: object,
        marker: object,
        *,
        profile: str | None = None,
        event: object | None = None,
        phase: str = "source",
        source_token=None,
    ):
        """Admit a source item before provider acknowledgement, queueing, or task creation."""
        if source_token is not None:
            handed_off = self.handoff_source(adapter, source_token, profile=profile, event=event)
            return handed_off[0] if len(handed_off) == 1 else handed_off
        registration = self._registration_for_adapter(adapter, profile)
        if registration is None:
            self._unknown_sources[("adapter", id(adapter))] = "source_registration_missing"
            return self._refuse_new_callback(event, "source_registration_missing")
        self._ensure_open(event)
        self._invocation_id += 1
        token = SourceAdmissionToken(
            registration=registration,
            marker=marker,
            phase=str(phase),
            close_generation=self._close_generation,
            identity=(registration.adapter, registration.profile, registration.transport_owner,
                      registration.generation, self._close_generation),
            invocation_id=self._invocation_id,
        )
        self._leases.add(token)
        pending = getattr(adapter, "_preservation_source_pending_tokens", None)
        if isinstance(pending, set):
            pending.add(token)
        if event is not None:
            self.bind_event(token, event)
        self._signal()
        return token

    def admit_public(self, adapter, event, profile: str | None = None) -> SourceAdmissionToken:
        """Admit a direct callback as a new invocation; handoff callers pass the token explicitly."""
        return self.admit_source(adapter, event, profile=profile, event=event, phase="base-callback")

    def bind_event(self, token: SourceAdmissionToken | None, event: object) -> SourceAdmissionToken | None:
        """Transfer a raw/claim token to the normalized event without changing its identity."""
        if not isinstance(token, SourceAdmissionToken) or token not in self._leases:
            return None
        if token.registration.retired:
            self.mark_unknown(token, "adapter_registration_retired")
            return None
        prior = self._event_tokens.setdefault(id(event), [])
        if token not in prior:
            prior.append(token)
        token.event = event
        attrs = getattr(event, _TOKENS_ATTR, None)
        if not isinstance(attrs, list):
            attrs = []
            setattr(event, _TOKENS_ATTR, attrs)
        if token not in attrs:
            attrs.append(token)
        setattr(event, _LEASE_ATTR, attrs[0])
        return token

    def bind_claim_event(self, token: SourceAdmissionToken | None, event: object) -> SourceAdmissionToken | None:
        return self.bind_event(token, event)

    def attach_transport(
        self, token: SourceAdmissionToken | None, receipt: object, settlement: Callable[..., Any],
        custody_ref: str | None = None,
    ) -> str | None:
        """Bind one live source transport receipt to one opaque admission token."""
        if not isinstance(token, SourceAdmissionToken) or token not in self._leases:
            return None
        if not callable(settlement):
            self.mark_unknown(token, "transport_settlement_missing")
            return None
        token.transport_receipt = receipt
        token.custody_ref = custody_ref if isinstance(custody_ref, str) and custody_ref else secrets.token_urlsafe(32)
        token.transport_settled = False
        token.transport_settlement = settlement
        token.transport_settlement_started = False
        token.settlement_witness = None
        token.settlement_receipt = None
        if isinstance(receipt, dict) and ("queue_epoch" in receipt or "wire_identity" in receipt):
            try:
                token.capture_bridge_delivery(receipt)
            except (AttributeError, TypeError, ValueError):
                self.mark_unknown(token, "bridge_delivery_identity_missing")
                return None
        self._signal()
        return token.custody_ref

    def tokens_for_event(self, event: object) -> tuple[SourceAdmissionToken, ...]:
        attrs = getattr(event, _TOKENS_ATTR, None)
        if isinstance(attrs, list):
            return tuple(token for token in attrs if token in self._leases)
        token = getattr(event, _LEASE_ATTR, None)
        return (token,) if isinstance(token, SourceAdmissionToken) and token in self._leases else ()

    def merge_events(self, target: object, source: object) -> None:
        for token in self.tokens_for_event(source):
            self.bind_event(token, target)
        for attr in (_TOKENS_ATTR, _LEASE_ATTR):
            try:
                delattr(source, attr)
            except AttributeError:
                pass
        self._event_tokens.pop(id(source), None)

    def rehome(self, token: SourceAdmissionToken | None, adapter: object, event: object,
               profile: str | None = None) -> SourceAdmissionToken | None:
        """Transfer an accepted held item to a newly registered adapter instance atomically."""
        if not isinstance(token, SourceAdmissionToken) or token not in self._leases:
            return None
        registration = self._registration_for_adapter(adapter, profile)
        if registration is None:
            self.mark_unknown(token, "rehome_registration_missing")
            return None
        replacement = SourceAdmissionToken(
            registration=registration,
            marker=token.marker,
            phase="rehome",
            close_generation=token.close_generation,
            producer_done=token.producer_done,
            identity=(registration.adapter, registration.profile, registration.transport_owner,
                      registration.generation, token.close_generation),
            custody_ref=token.custody_ref,
            transport_receipt=token.transport_receipt,
            transport_settled=token.transport_settled,
            transport_settlement=token.transport_settlement,
            transport_settlement_started=token.transport_settlement_started,
            native_occurrence_id=token.native_occurrence_id,
            bridge_binding_digest=token.bridge_binding_digest,
            wire_descriptor_digest=token.wire_descriptor_digest,
            source_event_digest=token.source_event_digest,
            source_identity_digest=token.source_identity_digest,
            transport_owner_digest=token.transport_owner_digest,
            transport_receipt_digest=token.transport_receipt_digest,
            transport_status=token.transport_status,
            settlement_request_id=token.settlement_request_id,
            settlement_witness=token.settlement_witness,
            settlement_receipt=token.settlement_receipt,
            finalizer_receipt=token.finalizer_receipt,
            producer_generation=token.producer_generation,
        )
        if isinstance(getattr(token, "_settlement_fields", None), dict):
            replacement._settlement_fields = dict(token._settlement_fields)
        self._leases.add(replacement)
        pending = getattr(adapter, "_preservation_source_pending_tokens", None)
        if isinstance(pending, set):
            pending.add(replacement)
        self.bind_event(replacement, event)
        prior_unknown = self._unknown_sources.pop(token, None)
        if prior_unknown is not None:
            self._unknown_sources[replacement] = prior_unknown
        token.state = "replayed"
        self._release_one(token)
        return replacement

    def enter_native(
        self,
        event,
        profile: str | None = None,
        source_token: SourceAdmissionToken | None = None,
    ) -> SourceAdmissionToken | None:
        """Bind the native callback to the exact source/profile/registration token."""
        if isinstance(source_token, (tuple, list, set, frozenset)) and any(
                not isinstance(item, SourceAdmissionToken) for item in source_token):
            self._unknown_sources[('witness', id(source_token))] = 'source_witness_invalid'
            return self._refuse_new_callback(event, 'source_identity_mismatch')
        explicit_tokens = self._normalize_tokens(source_token) if source_token is not None else ()
        empty_witness = (
            source_token is not None
            and isinstance(source_token, (tuple, list, set, frozenset))
            and not source_token
        )
        if source_token is not None and not explicit_tokens and not empty_witness:
            self._unknown_sources[('witness', id(source_token))] = 'source_witness_invalid'
            return self._refuse_new_callback(event, 'source_identity_mismatch')
        tokens = explicit_tokens or self.tokens_for_event(event)
        if not explicit_tokens and len(tokens) > 1:
            for token in tokens:
                self.mark_unknown(token, 'ambiguous_event_source_tokens')
            return self._refuse_new_callback(event, 'source_identity_ambiguous')
        registration = self._registration_for_event(event, profile)
        if registration is None and tokens and all(token in self._leases for token in tokens):
            candidates = tuple({id(token.registration): token.registration for token in tokens}.values())
            if len(candidates) == 1 and (profile is None or candidates[0].profile == profile):
                registration = candidates[0]
            elif len(candidates) > 1:
                for token in tokens:
                    self.mark_unknown(token, 'ambiguous_source_registration')
                return self._refuse_new_callback(event, 'source_identity_ambiguous')
        invalid_tokens = tuple(token for token in tokens if token not in self._leases)
        if invalid_tokens:
            for token in invalid_tokens:
                self.mark_unknown(token, 'source_witness_missing')
            return self._refuse_new_callback(event, 'source_identity_mismatch')
        for token in tokens:
            expected_identity = (
                token.registration.adapter, token.registration.profile,
                token.registration.transport_owner, token.registration.generation,
                self._close_generation,
            )
            if registration is None or token.registration is not registration or token.identity != expected_identity:
                self.mark_unknown(token, "transport_or_profile_swapped")
                return self._refuse_new_callback(event, "source_identity_mismatch")
            if token.state == "unknown":
                self.mark_unknown(token, "unknown_source_witness")
                return self._refuse_new_callback(event, "source_identity_mismatch")
            if token.registration.retired:
                self.mark_unknown(token, "adapter_registration_retired")
                return self._refuse_new_callback(event, "source_identity_mismatch")
            token.native_entered = True
            if token.native_occurrence_id is None:
                token.native_occurrence_id = f"{token.registration.generation}:{token.invocation_id}"
            token.state = "native"
            self.bind_event(token, event)
        active = tuple(token for token in tokens if token in self._leases)
        if active:
            return active[0] if len(active) == 1 else active
        if registration is None:
            if self._state == "closing":
                self._unknown_sources[("event", id(event))] = "source_registration_missing"
                return self._refuse_new_callback(event, "runtime_draining")
            return None
        token = self.admit_source(registration.adapter, event, profile=profile, event=event, phase="native-callback")
        token.native_entered = True
        token.native_occurrence_id = token.native_occurrence_id or f"{token.registration.generation}:{token.invocation_id}"
        token.state = "native"
        token.producer_done = True
        return token

    def release(self, lease: SourceAdmissionToken | None) -> None:
        tokens = self._normalize_tokens(lease)
        for token in tokens:
            token.native_exited = True
            self._maybe_release(token)

    def release_event(self, event: object) -> None:
        for token in self.tokens_for_event(event):
            token.native_exited = True
            self._maybe_release(token)

    def mark_producer_done(self, item: object) -> None:
        tokens = self._normalize_tokens(item) or self.tokens_for_event(item)
        for token in tokens:
            if token in self._leases:
                token.producer_done = True
                self._maybe_release(token)

    def _maybe_release(self, token: SourceAdmissionToken) -> None:
        if token.state == "unknown" or not token.native_exited or not token.producer_done:
            return
        if token.transport_receipt is not None and token.finalizer_receipt is None:
            # The native callback and producer task are not the terminal
            # witness.  Wait for the actual background finalizer to publish
            # its occurrence-bound receipt before settling transport custody.
            return
        if token.finalizer_receipt is not None and token.finalizer_receipt.status != "success":
            self.mark_unknown(token, "source_finalizer_failed")
            return
        if token.transport_receipt is not None and not token.transport_settled:
            if token.transport_settlement_started:
                return
            callback = token.transport_settlement
            if not callable(callback):
                self.mark_unknown(token, "transport_settlement_missing")
                return
            token.transport_settlement_started = True
            try:
                result = callback(token, token.custody_ref)
                if inspect.isawaitable(result):
                    task = asyncio.ensure_future(result)
                    task.add_done_callback(lambda done, current=token: self._finish_transport_settlement(current, done))
                    return
                self._finish_transport_settlement(token, result)
            except (asyncio.CancelledError, OSError, RuntimeError, TypeError, ValueError):
                self.mark_unknown(token, "transport_settlement_failed")
            return
        self._release_one(token)

    def _finish_transport_settlement(self, token: SourceAdmissionToken, result: Any) -> None:
        try:
            settled = result.result() if isinstance(result, asyncio.Future) else result
        except (asyncio.CancelledError, OSError, RuntimeError, TypeError, ValueError):
            self.mark_unknown(token, "transport_settlement_failed")
            return
        if settled is True:
            token.transport_settlement = None
            token.transport_status = "success"
        elif isinstance(settled, dict) and settled.get("settled") is True:
            token.settlement_receipt = settled
            token.transport_status = str(settled.get("status", "success"))
        else:
            self.mark_unknown(token, "transport_settlement_failed")
            return
        token.transport_settled = True
        token.transport_settlement_started = False
        self._signal()
        self._maybe_release(token)

    def _release_one(self, token: SourceAdmissionToken) -> None:
        if token not in self._leases:
            return
        self._resolve_source_refusal(token)
        token.state = "released"
        self._leases.discard(token)
        pending = getattr(token.adapter, "_preservation_source_pending_tokens", None)
        if isinstance(pending, set):
            pending.discard(token)
        if token.event is not None:
            values = self._event_tokens.get(id(token.event), [])
            values = [item for item in values if item is not token]
            if values:
                self._event_tokens[id(token.event)] = values
                try:
                    setattr(token.event, _TOKENS_ATTR, values)
                    setattr(token.event, _LEASE_ATTR, values[0])
                except AttributeError:
                    pass
            else:
                self._event_tokens.pop(id(token.event), None)
                for attr in (_TOKENS_ATTR, _LEASE_ATTR):
                    try:
                        delattr(token.event, attr)
                    except AttributeError:
                        pass
        self._cleanup_retired(token.registration)
        self._signal()

    def reconcile(self, token: SourceAdmissionToken | None, outcome: str, *, generation: int | None = None) -> bool:
        """Resolve only a proven refusal/terminal/replay; accepted work is never cancelled."""
        if not isinstance(token, SourceAdmissionToken) or token not in self._leases:
            return False
        if generation is not None and token.close_generation != generation:
            self.mark_unknown(token, "close_generation_mismatch")
            return False
        if outcome not in {"refused_before_accept", "terminal", "replayed"}:
            self.mark_unknown(token, str(outcome))
            return False
        if token.transport_receipt is not None and outcome in {"terminal", "replayed"} and not token.transport_settled:
            self.mark_unknown(token, "transport_custody_unproven")
            return False
        self._unknown_sources.pop(token, None)
        if outcome == "replayed" and not token.native_exited:
            token.state = "native" if token.native_entered else "accepted"
            self._signal()
            return True
        token.state = "reconciled"
        self._release_one(token)
        return True

    def mark_unknown(self, item: object, reason: str) -> None:
        tokens = self.tokens_for_event(item) if not isinstance(item, SourceAdmissionToken) else (item,)
        if not tokens:
            try:
                hash(item)
                key = item
            except TypeError:
                key = ("item", id(item))
            self._unknown_sources[key] = str(reason)
            self._signal()
            return
        for token in tokens:
            if token in self._leases:
                token.state = "unknown"
                self._unknown_sources[token] = str(reason)
        self._signal()

    def record_source_refusal(
        self, adapter: object, event: object, *, marker: object,
        profile: str | None = None, reason: str,
    ) -> bool:
        """Retain a pre-admission refusal until its exact source is replayed and settled."""
        if reason != "runtime_draining" or self._state != "closing" or self._close_generation is None:
            self.mark_unknown(event, reason)
            return False
        try:
            hash(marker)
        except TypeError:
            self.mark_unknown(event, "source_refusal_marker_unknown")
            return False
        registration = self._registration_for_adapter(adapter, profile)
        source_identity = self._source_identity(event)
        if registration is None or source_identity is None:
            self.mark_unknown(event, "source_refusal_identity_unknown")
            return False
        self._refusal_id += 1
        self._unknown_sources[("source_refusal", self._refusal_id)] = _SourceRefusalWitness(
            event=event,
            source_identity=source_identity,
            marker=marker,
            adapter=adapter,
            profile=registration.profile,
            transport_owner=registration.transport_owner,
            registration=registration,
            registration_generation=registration.generation,
            close_generation=self._close_generation,
            reason=reason,
        )
        self._signal()
        return True

    def _resolve_source_refusal(self, token: SourceAdmissionToken) -> None:
        if not token.producer_done or not token.native_exited or token.event is None:
            return
        source_identity = self._source_identity(token.event)
        for key, value in tuple(self._unknown_sources.items()):
            if not isinstance(value, _SourceRefusalWitness):
                continue
            if (
                value.reason != "runtime_draining"
                or value.registration is not token.registration
                or value.registration_generation != token.registration.generation
                or value.adapter is not token.adapter
                or value.transport_owner is not token.transport_owner
                or value.profile != token.profile
                or value.marker != token.marker
                or value.source_identity != source_identity
                or value.close_generation == token.close_generation
            ):
                continue
            self._unknown_sources.pop(key, None)
            self._signal()
            return

    def begin_close(self, close_generation: int) -> bool:
        """Close each registered producer at one exact coordinator generation."""
        if type(close_generation) is not int or close_generation < 1:
            return False
        if self._state == "closing":
            if self._close_generation != close_generation:
                return False
            return bool(self._registrations) and all(
                self._registration_is_current(registration)
                for registration in self._registrations.values()
            )
        self._state = "closing"
        self._close_generation = close_generation
        ok = bool(self._registrations)
        for registration in self._all_registrations():
            if registration.retired:
                ok = False
                registration.close_generation = close_generation
                self._unknown_sources[("registration", registration.generation)] = "retired_source_registration"
                continue
            if not self._registration_is_current(registration):
                ok = False
                continue
            registration.close_generation = close_generation
            closed = bool(_safe_contract_call(registration.contract.close, close_generation, default=False))
            if not closed or not registration.contract.supported:
                ok = False
                self._unknown_sources[("contract", registration.generation)] = "source_close_hook_missing"
        for token in tuple(self._leases):
            token.close_generation = close_generation
            token.identity = (token.registration.adapter, token.registration.profile,
                              token.registration.transport_owner, token.registration.generation,
                              close_generation)
        self._signal()
        return ok

    def abort(self, close_generation: int) -> bool:
        """Resume the exact generation while preserving every accepted/unknown token."""
        if self._state != "closing" or self._close_generation != close_generation:
            return False
        ok = True
        for registration in self._all_registrations():
            if registration.close_generation != close_generation:
                continue
            resumed = bool(_safe_contract_call(registration.contract.resume, close_generation, default=False))
            ok = ok and resumed
            if not resumed:
                self._unknown_sources[("resume", registration.generation)] = "source_resume_failed"
            registration.close_generation = None
        self._state = "open"
        self._close_generation = None
        for token in tuple(self._leases):
            token.close_generation = None
            token.identity = (token.registration.adapter, token.registration.profile,
                              token.registration.transport_owner, token.registration.generation, None)
        self._signal()
        return ok

    def _pending_for(
        self, registration: _SourceRegistration, generation: int,
    ) -> tuple[Any, ...] | SourcePendingSnapshot | None:
        pending = _safe_contract_call(registration.contract.pending, generation, default=None)
        if pending is None:
            self._unknown_sources[("pending", registration.generation)] = "source_pending_unknown"
            return None
        if isinstance(pending, SourcePendingSnapshot):
            return pending
        return tuple(pending or ())

    def _source_quiescent(self, close_generation: int) -> bool:
        if not (
            self._state == "closing"
            and self._close_generation == close_generation
            and bool(self._registrations)
            and not self._leases
            and not self._unknown_sources
        ):
            return False
        if not all(
            self._registration_is_current(registration)
            and registration.close_generation == close_generation
            for registration in self._registrations.values()
        ):
            return False
        for registration in self._all_registrations():
            pending = self._pending_for(registration, close_generation)
            if pending is None:
                return False
            if isinstance(pending, SourcePendingSnapshot):
                if pending.close_generation != close_generation or not pending.ready:
                    return False
                continue
            if pending:
                return False
        return True

    def owner_ready(self, close_generation: int) -> bool:
        return self._source_quiescent(close_generation)

    def evidence(self, owner_class: str, *, process_instance: str, authority_epoch: int,
                 close_generation: int, owner_id: str) -> dict[str, Any] | None:
        if not self.owner_ready(close_generation):
            return None
        from gateway.runtime_custody import OWNER_SPECS
        return {
            "source": OWNER_SPECS[owner_class].source,
            "process_instance": process_instance,
            "authority_epoch": authority_epoch,
            "close_generation": close_generation,
            "owner_id": owner_id,
            "observed": True,
            "source_close_generation": close_generation,
            "registered_source_generations": tuple(sorted(item.generation for item in self._registrations.values())),
            "active_callbacks": 0,
            "pending_adapter_work": False,
        }

    async def await_quiescence(self, close_generation: int, timeout: float) -> bool:
        if not self._registrations:
            return False
        deadline = asyncio.get_running_loop().time() + max(0.0, float(timeout))
        for registration in self._all_registrations():
            callback = registration.contract.await_close
            if not callable(callback):
                continue
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            try:
                result = callback(close_generation, remaining)
                if inspect.isawaitable(result):
                    result = await asyncio.wait_for(result, timeout=remaining)
            except (asyncio.CancelledError, OSError, RuntimeError, TimeoutError, TypeError, ValueError):
                return False
            if result is not True:
                return False
        while not self._source_quiescent(close_generation):
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            event = self._wake_event()
            event.clear()
            if self._source_quiescent(close_generation):
                return True
            try:
                await asyncio.wait_for(event.wait(), timeout=remaining)
            except TimeoutError:
                return False
        return True

    async def await_resume(self, close_generation: int, timeout: float) -> bool:
        if self._state != "closing" or self._close_generation != close_generation:
            return False
        deadline = asyncio.get_running_loop().time() + max(0.0, float(timeout))
        for registration in self._all_registrations():
            callback = registration.contract.await_resume
            if not callable(callback):
                continue
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return False
            try:
                result = callback(close_generation, remaining)
                if inspect.isawaitable(result):
                    result = await asyncio.wait_for(result, timeout=remaining)
            except (asyncio.CancelledError, OSError, RuntimeError, TimeoutError, TypeError, ValueError):
                return False
            if result is not True:
                return False
        return True

    def _wake_event(self) -> asyncio.Event:
        loop = asyncio.get_running_loop()
        if self._wake is None or self._wake_loop is not loop:
            self._wake = asyncio.Event()
            self._wake_loop = loop
        return self._wake

    def _signal(self) -> None:
        if self._wake is not None:
            self._wake.set()


def ensure_source_close(runner) -> PreservationSourceClose:
    registry = getattr(runner, "_preservation_source_close", None)
    if registry is None:
        registry = PreservationSourceClose(runner)
        runner._preservation_source_close = registry
    return registry


def _runner_for_adapter(adapter: object):
    runner = getattr(adapter, "gateway_runner", None)
    if runner is not None:
        return runner
    handler = getattr(adapter, "_message_handler", None)
    return getattr(handler, "__self__", None)


def register_source_adapter(runner, adapter, profile: str | None = None) -> None:
    ensure_source_close(runner).register_adapter(adapter, profile)


def source_admit(adapter: object, marker: object, *, event: object | None = None,
                 profile: str | None = None, phase: str = "source") -> SourceAdmissionToken | None:
    runner = _runner_for_adapter(adapter)
    registry = getattr(runner, "_preservation_source_close", None)
    if registry is None:
        return None
    return registry.admit_source(adapter, marker, profile=profile, event=event, phase=phase)


def source_bind_event(
    adapter: object, token: SourceAdmissionToken | None, event: object,
) -> SourceAdmissionToken | None:
    runner = _runner_for_adapter(adapter)
    registry = getattr(runner, "_preservation_source_close", None)
    return registry.bind_event(token, event) if registry is not None else None


def source_release(adapter: object, token: SourceAdmissionToken | None) -> None:
    runner = _runner_for_adapter(adapter)
    registry = getattr(runner, "_preservation_source_close", None)
    if registry is not None:
        registry.release(token)


def source_mark_producer_done(adapter: object, item: object) -> None:
    runner = _runner_for_adapter(adapter)
    registry = getattr(runner, "_preservation_source_close", None)
    if registry is not None:
        registry.mark_producer_done(item)


def source_rehome(adapter: object, token: SourceAdmissionToken | None, replacement_adapter: object,
                  event: object, profile: str | None = None) -> SourceAdmissionToken | None:
    runner = _runner_for_adapter(adapter) or _runner_for_adapter(replacement_adapter)
    registry = getattr(runner, "_preservation_source_close", None)
    return registry.rehome(token, replacement_adapter, event, profile) if registry is not None else None


def source_reconcile(adapter: object, token: SourceAdmissionToken | None, outcome: str,
                     *, generation: int | None = None) -> bool:
    runner = _runner_for_adapter(adapter)
    registry = getattr(runner, "_preservation_source_close", None)
    return registry.reconcile(token, outcome, generation=generation) if registry is not None else False


def source_mark_unknown(adapter: object, item: object, reason: str) -> None:
    runner = _runner_for_adapter(adapter)
    registry = getattr(runner, "_preservation_source_close", None)
    if registry is not None:
        registry.mark_unknown(item, reason)


def begin_source_close(runner, close_generation: int) -> bool:
    return ensure_source_close(runner).begin_close(close_generation)


def abort_source_close(runner, close_generation: int) -> bool:
    registry = getattr(runner, "_preservation_source_close", None)
    return registry is not None and registry.abort(close_generation)


async def await_source_close(runner, close_generation: int, timeout: float) -> bool:
    registry = getattr(runner, "_preservation_source_close", None)
    return registry is not None and await registry.await_quiescence(close_generation, timeout)


async def await_source_resume(runner, close_generation: int, timeout: float) -> bool:
    registry = getattr(runner, "_preservation_source_close", None)
    return registry is not None and await registry.await_resume(close_generation, timeout)


def source_owner_ready(runner, close_generation: int) -> bool:
    registry = getattr(runner, "_preservation_source_close", None)
    return registry is not None and registry.owner_ready(close_generation)


def source_owner_evidence(runner, owner_class: str, *, process_instance: str,
                          authority_epoch: int, close_generation: int,
                          owner_id: str) -> dict[str, Any] | None:
    registry = getattr(runner, "_preservation_source_close", None)
    if registry is None:
        return None
    return registry.evidence(
        owner_class,
        process_instance=process_instance,
        authority_epoch=authority_epoch,
        close_generation=close_generation,
        owner_id=owner_id,
    )
