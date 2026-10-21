"""Typed, source-owned settlement witnesses for the WhatsApp bridge.

The queue accepts a witness only after the source admission token has observed the
producer and native callback finish.  This module contains no key storage: callers
supply the already-authenticated, process-local signing key and the durable bridge
challenge.  Public payload fields are digests and occurrence identifiers, never an
owner claim.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any, Mapping


PROTOCOL = "hermes.whatsapp.settlement.v1"
US = "\x1f"
RS = "\x1e"
FIELDS = (
    "source_witness_key_id", "queue_epoch", "bridge_instance", "owner_epoch",
    "consumer_id", "consumer_epoch", "bridge_binding_digest", "close_generation", "claim_id", "item_id",
    "sequence", "custody_ref", "source_event_digest", "wire_descriptor_digest",
    "source_identity_digest", "registration_generation", "transport_owner_digest",
    "native_occurrence_id", "transport_receipt_digest", "transport_status",
    "settlement_challenge", "request_id_digest", "producer_generation",
    "finalizer_receipt_digest", "finalizer_status",
)


def _canonical(value: Any) -> str:
    def _validate(node: Any) -> None:
        if node is None or isinstance(node, (str, int, float, bool)):
            return
        if isinstance(node, (list, tuple)):
            for child in node:
                _validate(child)
            return
        if isinstance(node, dict):
            if any(not isinstance(key, str) for key in node):
                raise TypeError("witness object keys must be strings")
            for child in node.values():
                _validate(child)
            return
        raise TypeError("witness identity contains a non-JSON value")

    _validate(value)
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False,
    )


_BRIDGE_BINDING_FIELDS = (
    "queue_epoch", "bridge_instance", "owner_epoch", "consumer_id",
    "consumer_epoch", "close_generation",
)


def source_identity_projection(
    token: Any, delivery: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return immutable source-owned identity facts in a strictly JSON-safe shape.

    ``SourceAdmissionToken.identity`` remains the process-local object-identity fence. This
    projection is only the signed/readback representation; it refuses opaque objects instead of
    replacing them with repr/id/type metadata. The same projection feeds capture and fallback
    settlement paths so a receipt cannot change identity vocabulary between them.
    """
    registration = getattr(token, "registration", None)
    profile = getattr(registration, "profile", None)
    registration_generation = getattr(registration, "generation", None)
    close_generation = getattr(token, "close_generation", None)
    invocation_id = getattr(token, "invocation_id", None)
    phase = getattr(token, "phase", None)
    occurrence = getattr(token, "native_occurrence_id", None)
    if type(registration_generation) is not int or registration_generation < 1:
        raise TypeError("source registration generation is required")
    if profile is not None and not isinstance(profile, str):
        raise TypeError("source profile must be JSON-safe")
    if close_generation is not None and (
            type(close_generation) is not int or close_generation < 1
    ):
        raise TypeError("source close generation must be JSON-safe")
    if type(invocation_id) is not int or invocation_id < 1:
        raise TypeError("source invocation is required")
    if not isinstance(phase, str) or not phase:
        raise TypeError("source phase is required")
    if occurrence is None:
        occurrence = f"{registration_generation}:{invocation_id}"
    elif not isinstance(occurrence, str) or not occurrence:
        raise TypeError("native occurrence must be JSON-safe")
    if delivery is None:
        delivery = {}
    if not isinstance(delivery, Mapping):
        raise TypeError("bridge delivery must be a mapping")
    bridge_binding = {name: delivery.get(name) for name in _BRIDGE_BINDING_FIELDS}
    producer_generation = getattr(token, "producer_generation", None)
    if producer_generation is None:
        producer_generation = delivery.get("producer_generation")
    if producer_generation is not None and not isinstance(producer_generation, str):
        raise TypeError("producer generation must be JSON-safe")
    source = getattr(getattr(token, "event", None), "source", None)
    to_dict = getattr(source, "to_dict", None)
    if callable(to_dict):
        source = to_dict()
    projection = {
        "source_identity": {
            "profile": profile,
            "registration_generation": registration_generation,
            "close_generation": close_generation,
            "native_occurrence_id": occurrence,
            "producer_generation": producer_generation,
            "source": source,
        },
        "source_event": {
            "marker": getattr(token, "marker", None),
            "phase": phase,
            "invocation_id": invocation_id,
            "native_occurrence_id": occurrence,
            "source": source,
        },
        "bridge_binding": bridge_binding,
        "transport_owner": {
            "profile": profile,
            "registration_generation": registration_generation,
            "bridge_binding": bridge_binding,
        },
    }
    _canonical(projection)
    return projection


def digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def key_id(key: bytes) -> str:
    if not isinstance(key, bytes) or len(key) < 16:
        raise ValueError("source witness key is required")
    return hashlib.sha256(key).hexdigest()


def _field(value: Any, name: str) -> str:
    text = "-" if value is None else str(value)
    if not text or len(text) > 512 or any(char in text for char in ("\x00", US, RS, "\r", "\n")):
        raise ValueError(f"invalid witness field: {name}")
    return text


@dataclass(frozen=True)
class BridgeOwnerBinding:
    queue_epoch: str
    bridge_instance: str
    owner_epoch: int
    consumer_id: str
    consumer_epoch: int
    close_generation: int | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "queue_epoch": self.queue_epoch,
            "bridge_instance": self.bridge_instance,
            "owner_epoch": self.owner_epoch,
            "consumer_id": self.consumer_id,
            "consumer_epoch": self.consumer_epoch,
            "close_generation": self.close_generation,
        }

    @property
    def digest(self) -> str:
        return digest(self.as_dict())


@dataclass(frozen=True)
class BridgeSettlementWitness:
    fields: tuple[tuple[str, str], ...]
    witness_bytes: bytes
    mac_hex: str
    key_id: str

    @property
    def witness_digest(self) -> str:
        return hashlib.sha256(self.witness_bytes).hexdigest()

    def envelope(self) -> dict[str, str]:
        return {
            "protocol": PROTOCOL,
            "witness_b64url": base64.urlsafe_b64encode(self.witness_bytes).decode("ascii").rstrip("="),
            "mac_hex": self.mac_hex,
            "witness_digest": self.witness_digest,
        }


def _transport_digest(receipt: Mapping[str, Any]) -> str:
    return digest({
        "status": receipt.get("status", receipt.get("transport_status", "success")),
        "channel": receipt.get("channel"),
        "message_id": receipt.get("message_id", receipt.get("messageId")),
        "request_id": receipt.get("request_id"),
        "operation_id": receipt.get("operation_id"),
    })


def _request_id_digest(request_id: Any) -> str:
    """Hash the validated request identifier as its exact UTF-8 bytes."""
    if not isinstance(request_id, str) or not request_id:
        raise ValueError("settlement request id is required")
    return hashlib.sha256(request_id.encode("utf-8")).hexdigest()


def settlement_fields(
    token: Any, delivery: Mapping[str, Any], receipt: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the source-owned field projection used by signing and queue verification."""
    receipt = receipt if isinstance(receipt, Mapping) else {}
    projection = source_identity_projection(token, delivery)
    captured = getattr(token, "_settlement_fields", None)
    if not isinstance(captured, Mapping):
        captured = {}
    return {
        "source_witness_key_id": None,
        "queue_epoch": delivery.get("queue_epoch"),
        "bridge_instance": delivery.get("bridge_instance"),
        "owner_epoch": delivery.get("owner_epoch", delivery.get("binding", {}).get("owner_epoch")),
        "consumer_id": delivery.get("consumer_id"),
        "consumer_epoch": delivery.get("consumer_epoch"),
        "close_generation": delivery.get("close_generation"),
        "claim_id": delivery.get("claim_id"),
        "item_id": delivery.get("item_id"),
        "sequence": delivery.get("sequence"),
        "custody_ref": delivery.get("custody_ref") or getattr(token, "custody_ref", None),
        "source_event_digest": getattr(token, "source_event_digest", None)
        or digest(projection["source_event"]),
        "wire_descriptor_digest": getattr(token, "wire_descriptor_digest", None)
        or digest(delivery.get("wire_identity", delivery.get("payload", {}))),
        "source_identity_digest": getattr(token, "source_identity_digest", None)
        or digest(projection["source_identity"]),
        "registration_generation": getattr(token, "registration_generation", None),
        "transport_owner_digest": getattr(token, "transport_owner_digest", None)
        or digest(projection["transport_owner"]),
        "native_occurrence_id": getattr(token, "native_occurrence_id", None)
        or projection["source_identity"]["native_occurrence_id"],
        "transport_receipt_digest": getattr(token, "transport_receipt_digest", None)
        or _transport_digest(receipt),
        "transport_status": getattr(token, "transport_status", None) or "success",
        "settlement_challenge": delivery.get("settlement_challenge"),
        # The signed witness is created for the durable settle operation and
        # is then replayed by the idempotent ACK. Never bind its identity to
        # the later ACK request id: a lost settle response must be retryable
        # with the same authenticated occurrence.
        "request_id_digest": _request_id_digest(
            delivery.get("settle_request_id", delivery.get("request_id", delivery.get("ack_request_id")))
        ),
        "producer_generation": delivery.get("producer_generation")
        or getattr(token, "producer_generation", None),
        "finalizer_receipt_digest": getattr(getattr(token, "finalizer_receipt", None), "finalizer_digest", None),
        "finalizer_status": getattr(getattr(token, "finalizer_receipt", None), "status", None),
        **captured,
    }


def build_settlement_witness(token: Any, delivery: Mapping[str, Any], key: bytes,
                             receipt: Mapping[str, Any] | None = None) -> BridgeSettlementWitness:
    """Build one immutable witness from the live token and its exact delivery.

    It intentionally refuses missing lifecycle observations.  The adapter cannot
    manufacture a witness by supplying replacement identity fields because all
    identity values are read from the delivery claim and token registration.
    """
    receipt = receipt if isinstance(receipt, Mapping) else {}
    finalizer = getattr(token, "finalizer_receipt", None)
    if not getattr(token, "producer_done", False) or not getattr(token, "native_entered", False) \
            or not getattr(token, "native_exited", False) or finalizer is None \
            or finalizer.status != "success":
        raise ValueError("source lifecycle is not settled")
    if receipt.get("status", receipt.get("transport_status", "success")) not in {"success", "delivered", True}:
        raise ValueError("transport receipt is not successful")
    fields_map = settlement_fields(token, delivery, receipt)
    if fields_map.get("finalizer_status") != "success" or not fields_map.get("finalizer_receipt_digest"):
        raise ValueError("source finalizer receipt is required")
    fields_map["source_witness_key_id"] = key_id(key)
    values = tuple((name, _field(fields_map.get(name), name)) for name in FIELDS)
    wire = (US.join(value for _, value in values) + RS).encode("ascii")
    mac = hmac.new(key, wire, hashlib.sha256).hexdigest()
    return BridgeSettlementWitness(values, wire, mac, key_id(key))
