"""Authenticated I17 contract for a hosted compute child.

The coordinator remains in the gateway process.  A child therefore receives an immutable
description of the already reserved host parent and reports failures against that parent; it does
not create a second custody ledger or attempt to copy the gateway coordinator across a process
boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Callable
import uuid

BRIDGE_ENV = "HERMES_I17_CHILD_BRIDGE"
BRIDGE_SCHEMA = 1


class ChildBridgeError(ValueError):
    """The child bridge identity is absent, malformed, stale, or foreign."""


@dataclass(frozen=True)
class ChildBridgeIdentity:
    schema: int
    process_instance: str
    authority_epoch: int
    profile_id: str
    profile_home: str
    coordinator_id: str
    root_lease_id: str
    parent_lease_id: str
    root_owner_id: str
    parent_owner_id: str

    def as_wire(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "process_instance": self.process_instance,
            "authority_epoch": self.authority_epoch,
            "profile_id": self.profile_id,
            "profile_home": self.profile_home,
            "coordinator_id": self.coordinator_id,
            "root_lease_id": self.root_lease_id,
            "parent_lease_id": self.parent_lease_id,
            "root_owner_id": self.root_owner_id,
            "parent_owner_id": self.parent_owner_id,
        }


@dataclass(frozen=True)
class ChildProducerToken:
    """One child operation covered by the parent host lease.

    This token is a typed operation witness only.  Its lifetime is settled by the host parent's
    coordinator record; the child never releases or recreates a gateway lease.
    """

    bridge: ChildBridgeIdentity
    operation_id: str
    method: str


def build_identity(root, parent, *, profile_home: str) -> dict[str, Any]:
    if getattr(root, "lease", None) is None or getattr(parent, "lease", None) is None:
        raise ChildBridgeError("I17 child parent is unavailable")
    values = {
        "schema": BRIDGE_SCHEMA,
        "process_instance": root.process_instance,
        "authority_epoch": root.authority_epoch,
        "profile_id": root.profile_id,
        "profile_home": profile_home,
        "coordinator_id": root.coordinator_id,
        "root_lease_id": root.lease.lease_id,
        "parent_lease_id": parent.lease.lease_id,
        "root_owner_id": root.owner_id,
        "parent_owner_id": parent.owner_id,
    }
    _validate_wire(values)
    return values


def encode(payload: dict[str, Any]) -> str:
    _validate_wire(payload)
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def decode(raw: str | bytes | None) -> ChildBridgeIdentity | None:
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ChildBridgeError("invalid I17 child bridge") from exc
    _validate_wire(payload)
    return ChildBridgeIdentity(**payload)


def _validate_wire(payload: Any) -> None:
    if not isinstance(payload, dict):
        raise ChildBridgeError("I17 child bridge must be an object")
    required = {
        "schema", "process_instance", "authority_epoch", "profile_id", "profile_home",
        "coordinator_id", "root_lease_id", "parent_lease_id", "root_owner_id",
        "parent_owner_id",
    }
    if set(payload) != required or payload.get("schema") != BRIDGE_SCHEMA:
        raise ChildBridgeError("I17 child bridge schema mismatch")
    for key in required - {"schema", "authority_epoch"}:
        if not isinstance(payload.get(key), str) or not payload[key]:
            raise ChildBridgeError("I17 child bridge identity missing")
    if type(payload.get("authority_epoch")) is not int or payload["authority_epoch"] < 1:
        raise ChildBridgeError("I17 child bridge epoch missing")


def validate_parent(supervisor, hello: dict[str, Any]) -> bool:
    """Validate the child hello against the exact parent handle and current authority."""
    expected = getattr(supervisor, "_i17_child_bridge", None)
    got = hello.get("i17_child_bridge") if isinstance(hello, dict) else None
    if not isinstance(expected, dict) or got != expected:
        # A supervisor without an I17 parent is still allowed to run the legacy standalone
        # compute-host fixture.  Once bind_supervisor attached a real host handle, however,
        # a missing bridge is an unauthenticated child and must fail the startup CAS just like
        # a foreign or stale payload; accepting ``None`` here would let a managed child bypass
        # the parent-owned coordinator entirely.
        if expected is None and got is None:
            return (
                getattr(supervisor, "_i17_service_root", None) is None
                and getattr(supervisor, "_i17_host_handle", None) is None
            )
        return False
    try:
        _validate_wire(got)
    except ChildBridgeError:
        return False
    root = getattr(supervisor, "_i17_service_root", None)
    parent = getattr(supervisor, "_i17_host_handle", None)
    authority = getattr(supervisor, "_i17_profile_authority", None)
    coordinator = getattr(supervisor, "_i17_runtime_custody", None)
    return bool(
        root is not None and parent is not None and authority is not None and coordinator is not None
        and got["root_lease_id"] == getattr(root.lease, "lease_id", None)
        and got["parent_lease_id"] == getattr(parent.lease, "lease_id", None)
        and got["coordinator_id"] == getattr(root, "coordinator_id", None)
        and got["process_instance"] == getattr(root, "process_instance", None)
        and got["authority_epoch"] == getattr(root, "authority_epoch", None)
        and got["profile_id"] == getattr(root, "profile_id", None)
        and got["profile_home"] == str(getattr(supervisor, "_i17_profile_home", ""))
        and got["root_owner_id"] == getattr(root, "owner_id", None)
        and got["parent_owner_id"] == getattr(parent, "owner_id", None)
        and coordinator.is_active_handle(parent)
    )


class ChildProducerBridge:
    """A child-local adapter whose operations remain owned by the parent host lease."""

    def __init__(self, identity: ChildBridgeIdentity, on_unknown: Callable[[], None] | None = None):
        self.identity = identity
        self._on_unknown = on_unknown

    def admit(self, request: dict[str, Any]) -> tuple[ChildProducerToken | None, str | None]:
        if not isinstance(request, dict):
            return None, "invalid_request"
        params = request.get("params")
        owner = params.get("owner") if isinstance(params, dict) else None
        profile = owner.get("profile_id") if isinstance(owner, dict) else self.identity.profile_id
        if profile != self.identity.profile_id:
            return None, "foreign_child_bridge"
        method = str(request.get("method") or "rpc")
        return ChildProducerToken(self.identity, uuid.uuid4().hex, method), None

    def settle(self, token: ChildProducerToken | None, *, known: bool) -> bool:
        if token is None or token.bridge != self.identity:
            return False
        if not known and self._on_unknown is not None:
            self._on_unknown()
        return bool(known)


def install(payload: dict[str, Any], on_unknown: Callable[[], None] | None = None) -> ChildProducerBridge:
    _validate_wire(payload)
    return ChildProducerBridge(ChildBridgeIdentity(**payload), on_unknown=on_unknown)
