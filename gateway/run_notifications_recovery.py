"""Source-owned completion notification readback for the I10 recovery close."""

from __future__ import annotations

import _thread
from collections import deque
from queue import Queue


_PROCESS_REGISTRY_BINDING_KEYS = (
    "registry", "pending_watchers", "completion_queue", "queue_mutex", "queue_storage",
    "registry_lock", "running", "finished", "batches", "batch_tasks",
)


def _validate_registry_indexes(indexes):
    pending_watchers = indexes["pending_watchers"]
    completion_queue = indexes["completion_queue"]
    registry_lock = indexes["registry_lock"]
    running = indexes["running"]
    finished = indexes["finished"]
    if (
        not isinstance(pending_watchers, list)
        or not isinstance(completion_queue, Queue)
        or not isinstance(indexes["queue_mutex"], _thread.LockType)
        or not isinstance(indexes["queue_storage"], deque)
        or not isinstance(registry_lock, _thread.LockType)
        or not isinstance(running, dict)
        or not isinstance(finished, dict)
    ):
        raise RuntimeError("notification registry has invalid indexes")
    for watcher in pending_watchers:
        if (
            not isinstance(watcher, dict)
            or type(watcher.get("session_id")) is not str
            or not watcher["session_id"]
            or type(watcher.get("session_key")) is not str
            or not watcher["session_key"]
        ):
            raise RuntimeError("notification registry watcher identity is malformed")
    with completion_queue.mutex:
        queued = tuple(completion_queue.queue)
    if any(not isinstance(item, dict) for item in queued):
        raise RuntimeError("notification registry completion index is malformed")
    return pending_watchers, queued, registry_lock, running, finished


def _read_process_registry_indexes(process_registry):
    try:
        indexes = {
            "registry": process_registry,
            "pending_watchers": process_registry.pending_watchers,
            "completion_queue": process_registry.completion_queue,
            "queue_mutex": process_registry.completion_queue.mutex,
            "queue_storage": process_registry.completion_queue.queue,
            "registry_lock": process_registry._lock,
            "running": process_registry._running,
            "finished": process_registry._finished,
        }
    except AttributeError as exc:
        raise RuntimeError("notification registry is incomplete") from exc
    _validate_registry_indexes(indexes)
    return indexes


def _source_binding(runner):
    binding = getattr(runner, "_i10_notification_source_binding", None)
    if (
        not isinstance(binding, dict)
        or any(key not in binding for key in _PROCESS_REGISTRY_BINDING_KEYS)
    ):
        raise RuntimeError("notification registry binding is unavailable")
    return binding


def _bind_source_indexes(runner, process_registry):
    from tools.process_registry import ProcessRegistry

    if not isinstance(process_registry, ProcessRegistry):
        raise RuntimeError("notification registry is invalid")
    indexes = _read_process_registry_indexes(process_registry)
    indexes["batches"] = runner._completion_notification_batches
    indexes["batch_tasks"] = runner._completion_notification_batch_tasks
    runner._i10_notification_source_binding = indexes


def initialize_i10_source_indexes(runner) -> None:
    """Create completion indexes during the real gateway bootstrap."""
    for attribute, expected in (
        ("_completion_notification_batches", dict),
        ("_completion_notification_batch_tasks", dict),
    ):
        value = getattr(runner, attribute, None)
        if value is None:
            value = expected()
            setattr(runner, attribute, value)
        if not isinstance(value, expected):
            raise RuntimeError(f"notification registry {attribute} is invalid")
    from tools.process_registry import process_registry
    _bind_source_indexes(runner, process_registry)


def _notification_source_registry(runner):
    """Return the canonical ProcessRegistry after validating every source index it reads."""
    from tools.process_registry import process_registry

    binding = _source_binding(runner)
    if process_registry is not binding["registry"]:
        raise RuntimeError("notification registry identity is invalid")
    current = _read_process_registry_indexes(process_registry)
    if any(current[key] is not binding[key] for key in _PROCESS_REGISTRY_BINDING_KEYS[:8]):
        raise RuntimeError("notification registry identity is invalid")
    try:
        batches = runner._completion_notification_batches
        batch_tasks = runner._completion_notification_batch_tasks
    except AttributeError as exc:
        raise RuntimeError("notification registry is incomplete") from exc
    if batches is not binding["batches"] or batch_tasks is not binding["batch_tasks"]:
        raise RuntimeError("notification registry identity is invalid")
    if not isinstance(batches, dict) or not isinstance(batch_tasks, dict):
        raise RuntimeError("notification registry has invalid indexes")
    current["batches"] = batches
    current["batch_tasks"] = batch_tasks
    return (*_validate_registry_indexes(current), batches, batch_tasks)


def _task_is_done(task):
    done = getattr(task, "done", None)
    if not callable(done):
        raise RuntimeError("notification registry batch task is malformed")
    try:
        value = done()
    except Exception as exc:
        raise RuntimeError("notification registry batch task is unreadable") from exc
    if type(value) is not bool:
        raise RuntimeError("notification registry batch task state is unknown")
    return value


def i10_source_snapshot(authority):
    """Read process watchers, completion events and notification batches for one profile."""
    runner = authority.runner
    registry = getattr(runner, "session_authorities", None)
    profile_name = registry.profile_name(authority) if registry is not None else None
    prefix = f"agent:{profile_name}:" if profile_name else None

    def belongs(item):
        if not isinstance(item, dict):
            return False
        key = item.get("session_key") or ""
        if prefix is None:
            served_homes = getattr(runner, "_served_profile_homes", None)
            if served_homes is None:
                served_homes = {}
            if not isinstance(served_homes, dict):
                raise RuntimeError("notification registry profile binding is invalid")
            return not any(
                isinstance(key, str) and key.startswith(f"agent:{name}:")
                for name, _home in served_homes.items()
                if name
            )
        return isinstance(key, str) and key.startswith(prefix)

    pending_watchers, queued, registry_lock, running, finished, batches, batch_tasks = (
        _notification_source_registry(runner)
    )
    pending = set()
    for watcher in tuple(pending_watchers):
        if belongs(watcher):
            pending.add(f"watcher:{watcher.get('session_id', '')}")
    with registry_lock:
        sessions = tuple(running.values()) + tuple(finished.values())
    for event in queued:
        if belongs(event):
            pending.add(f"event:{event.get('type', 'completion')}:{event.get('session_id', '')}")
    for session in sessions:
        watcher = {
            "session_key": getattr(session, "session_key", ""),
            "session_id": getattr(session, "id", ""),
        }
        if belongs(watcher) and getattr(session, "notify_on_complete", False):
            custody = getattr(session, "custody", None)
            if custody is None or not getattr(custody, "delivery_acked", False):
                pending.add(f"session:{watcher['session_id']}")
    for batch_name, entries in batches.items():
        for _text, event, _future in tuple(entries or ()):
            if belongs(event):
                pending.add(f"batch:{event.get('session_id', '')}:{batch_name!r}")
    for key, task in batch_tasks.items():
        session_key = key[0] if isinstance(key, tuple) and key else ""
        if not _task_is_done(task) and belongs({"session_key": session_key}):
            pending.add(f"batch-task:{key!r}")
    return {
        "pending_ids": tuple(sorted(item for item in pending if item != "watcher:")),
        "registry": {
            "pending_watchers": tuple(sorted(w.get("session_id", "") for w in pending_watchers)),
            "completion_queue_depth": len(queued),
        },
    }


def register_i10_source(authority):
    """Install completion notification's canonical close-time probe for one authority."""
    from gateway.session_recovery_custody import register_i10_consumer_source

    return register_i10_consumer_source(authority, "completion_notification", i10_source_snapshot)
