"""Durable local state for the Teams pipeline plugin."""

from __future__ import annotations

import weakref
import hashlib
import json
import os
import threading
from copy import deepcopy
from datetime import datetime, timezone, UTC
from functools import partialmethod
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Optional

from hermes_constants import get_hermes_home
from plugins.teams_pipeline.store_ownership import (
    TeamsStoreOwner,
    TeamsStoreOwnership,
    TeamsStoreOwnershipError,
    canonical_store_path,
)


DEFAULT_TEAMS_PIPELINE_STORE_FILENAME = "teams_pipeline_store.json"
# Persisted top-level buckets; the on-disk JSON shape must stay stable for existing user stores.
_BUCKETS = ("subscriptions", "notification_receipts", "event_timestamps", "jobs", "sink_records")
_RECOVERABLE_JOB_STATES = frozenset({
    "received", "resolving_meeting", "fetching_transcript", "downloading_recording",
    "transcribing_audio", "summarizing", "writing_notion", "writing_linear", "sending_teams",
    "retry_scheduled",
})


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def resolve_teams_pipeline_store_path(path: str | Path | None = None) -> Path:
    explicit = str(path).strip() if path is not None else ""
    env_path = os.getenv("MSGRAPH_WEBHOOK_STORE_PATH", "").strip()
    selected = Path(explicit or env_path) if (explicit or env_path) else get_hermes_home() / DEFAULT_TEAMS_PIPELINE_STORE_FILENAME
    return canonical_store_path(selected)


class TeamsPipelineStore:
    """JSON-backed durable store for Teams pipeline state; every write is an atomic temp-file replace."""

    def __init__(self, path: str | Path, *, owner: TeamsStoreOwner | None = None, lock: bool = False):
        self.path = canonical_store_path(path)
        self._lock = threading.RLock()
        self._owner = owner
        self._ownership = TeamsStoreOwnership(self.path, owner) if lock and owner is not None else None
        if lock and owner is None:
            raise ValueError("a Teams store lock requires an owner")
        self._closed = False
        self._state: dict[str, dict[str, Any]] = {bucket: {} for bucket in _BUCKETS}
        try:
            if self._ownership is not None:
                self._ownership.acquire()
                # The finalizer retains only the lease, not this store object, so one-shot CLI
                # commands release the descriptor as soon as their local store goes out of scope.
                self._finalizer = weakref.finalize(self, self._ownership.close)
            self._load()
        except BaseException:
            self.close()
            raise

    @property
    def owner_required(self) -> bool:
        return self._ownership is not None

    def assert_runtime_owner(self, *, profile_id: Any, authority_epoch: Any, instance_id: Any, owner_id: Any) -> None:
        self._assert_usable()
        if self._ownership is None:
            raise RuntimeError("Teams pipeline store has no runtime ownership lease")
        self._ownership.assert_current(
            profile_id=profile_id, authority_epoch=authority_epoch,
            instance_id=instance_id, owner_id=owner_id,
        )

    def close(self) -> bool:
        # A writer holds this same RLock across its state mutation and atomic file replace.  Do
        # not release the OS lease underneath that write: a close request from another thread is
        # a retryable refusal until the writer has left the critical section.
        if not self._lock.acquire(blocking=False):
            return False
        try:
            if self._ownership is not None:
                closed = self._ownership.close()
            else:
                closed = True
            if closed:
                self._closed = True
            return closed
        finally:
            self._lock.release()

    def _assert_usable(self) -> None:
        if self._closed:
            raise TeamsStoreOwnershipError("teams_pipeline_store_closed")
        if self._ownership is not None:
            self._ownership.assert_current(
                profile_id=self._owner.profile_id,
                authority_epoch=self._owner.authority_epoch,
                instance_id=self._owner.instance_id,
                owner_id=self._owner.owner_id,
            )

    def _load(self) -> None:
        with self._lock:
            self._assert_usable()
            if not self.path.exists():
                return
            data = json.loads(self.path.read_text(encoding="utf-8-sig") or "{}")
            if not isinstance(data, dict):
                return
            self._state = {bucket: dict(data.get(bucket) or {}) for bucket in _BUCKETS}

    def _persist(self) -> None:
        self._assert_usable()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with NamedTemporaryFile("w", encoding="utf-8", dir=str(self.path.parent), delete=False) as tmp:
            json.dump(self._state, tmp, indent=2, sort_keys=True)
            tmp.flush()
            tmp_path = Path(tmp.name)
        tmp_path.replace(self.path)

    def _list(self, bucket: str) -> dict[str, dict[str, Any]]:
        with self._lock:
            self._assert_usable()
            return deepcopy(self._state[bucket])

    def _get(self, bucket: str, key: str) -> Optional[dict[str, Any]]:
        with self._lock:
            self._assert_usable()
            record = self._state[bucket].get(key)
            return deepcopy(record) if isinstance(record, dict) else None

    def _upsert(self, bucket: str, id_field: str, key: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Merge ``payload`` over the existing record, stamping ``id_field`` and created/updated timestamps."""
        with self._lock:
            self._assert_usable()
            existing = self._state[bucket].get(key, {})
            if bucket == "jobs" and self._owner is not None:
                stored_profile = str(existing.get("runtime_profile_id") or "").strip()
                if stored_profile and stored_profile != self._owner.profile_id:
                    raise RuntimeError("teams_pipeline_store_profile_mismatch")
            merged = {**existing, **deepcopy(payload)}
            merged[id_field] = key
            if bucket == "jobs" and self._owner is not None:
                merged.update({
                    "runtime_profile_id": self._owner.profile_id,
                    "runtime_authority_epoch": self._owner.authority_epoch,
                    "runtime_instance_id": self._owner.instance_id,
                    "runtime_owner": self._owner.owner_id,
                })
            merged.setdefault("created_at", existing.get("created_at") or _utc_now_iso())
            merged["updated_at"] = _utc_now_iso()
            self._state[bucket][key] = merged
            self._persist()
            return deepcopy(merged)

    list_subscriptions = partialmethod(_list, "subscriptions")
    get_subscription = partialmethod(_get, "subscriptions")
    upsert_subscription = partialmethod(_upsert, "subscriptions", "subscription_id")
    list_jobs = partialmethod(_list, "jobs")
    get_job = partialmethod(_get, "jobs")
    upsert_job = partialmethod(_upsert, "jobs", "job_id")
    get_sink_record = partialmethod(_get, "sink_records")
    upsert_sink_record = partialmethod(_upsert, "sink_records", "sink_key")

    def delete_subscription(self, subscription_id: str) -> bool:
        with self._lock:
            self._assert_usable()
            if self._state["subscriptions"].pop(subscription_id, None) is None:
                return False
            self._persist()
            return True

    @classmethod
    def build_notification_receipt_key(cls, notification: dict[str, Any]) -> str:
        if explicit_id := notification.get("id"):
            return f"id:{explicit_id}"
        canonical = json.dumps(notification, sort_keys=True, separators=(",", ":"))
        return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"

    def record_notification_receipt(self, receipt_key: str, payload: Optional[dict[str, Any]] = None, *, received_at: Optional[str] = None) -> bool:
        """Record a receipt once; returns False when the key was already seen (duplicate delivery)."""
        with self._lock:
            self._assert_usable()
            if receipt_key in self._state["notification_receipts"]:
                return False
            self._state["notification_receipts"][receipt_key] = {"received_at": received_at or _utc_now_iso(),
                                                                 "payload": deepcopy(payload) if isinstance(payload, dict) else payload}
            self._persist()
            return True

    def claim_recoverable_jobs(
        self, owner_id: str, *, profile_id: Any = None, authority_epoch: Any = None,
        instance_id: Any = None,
    ) -> list[dict[str, Any]]:
        """Claim durable non-terminal jobs for the exact profile/authority that owns this store.

        The sibling store lease is the cross-process exclusion for redirected paths.  Durable job
        fields retain the profile and current authority epoch so a successor of the same profile can
        recover work while another profile cannot adopt it.  Older rows without these fields are
        stamped only while this store's exclusive lease is held.
        """
        owner = str(owner_id).strip()
        if not owner:
            raise ValueError("owner_id is required")
        self._assert_usable()
        if self.owner_required:
            self.assert_runtime_owner(
                profile_id=profile_id, authority_epoch=authority_epoch,
                instance_id=instance_id, owner_id=owner,
            )
            expected_profile = self._owner.profile_id
            expected_epoch = self._owner.authority_epoch
            expected_instance = self._owner.instance_id
        else:
            expected_profile = str(profile_id or "").strip() or None
            expected_epoch = str(authority_epoch or "").strip() or None
            expected_instance = str(instance_id or "").strip() or None
        with self._lock:
            self._assert_usable()
            recoverable = [
                record for record in self._state["jobs"].values()
                if isinstance(record, dict) and record.get("status") in _RECOVERABLE_JOB_STATES
            ]
            if expected_profile is not None:
                for record in recoverable:
                    stored_profile = str(record.get("runtime_profile_id") or "").strip()
                    if stored_profile and stored_profile != expected_profile:
                        raise RuntimeError("teams_pipeline_store_profile_mismatch")
            claimed: list[dict[str, Any]] = []
            changed = False
            for record in recoverable:
                if expected_profile is not None:
                    fields = {
                        "runtime_profile_id": expected_profile,
                        "runtime_authority_epoch": expected_epoch,
                        "runtime_instance_id": expected_instance,
                    }
                    if any(record.get(key) != value for key, value in fields.items()):
                        record.update(fields)
                        changed = True
                if record.get("runtime_owner") != owner or record.get("runtime_claim") != "claimed":
                    record["runtime_owner"] = owner
                    record["runtime_claim"] = "claimed"
                    record["runtime_claimed_at"] = _utc_now_iso()
                    changed = True
                claimed.append(deepcopy(record))
            if changed:
                self._persist()
            return claimed

    def stats(self) -> dict[str, int]:
        with self._lock:
            self._assert_usable()
            return {bucket: len(self._state[bucket]) for bucket in _BUCKETS}
