"""Standalone CUA native-close retry fences."""

from __future__ import annotations

from typing import Any


def native_close_ready(physical: Any) -> bool:
    from gateway.runtime_sidecar_transport import required_reader_streams

    required = required_reader_streams(physical)
    return (
        physical.reader_error is None
        and set(physical.readers) == required
        and set(physical.reader_start_states) == required
        and all(state == "started" for state in physical.reader_start_states.values())
        and physical.eof_streams == required
        and physical.joined_streams == required
        and physical.close_native_after_process() is True
    )


def reconcile_close_handles(parent: Any, physical: Any) -> bool:
    coordinator = parent.coordinator
    restored = []
    for handle in (physical.handle, parent.root_handle):
        result = coordinator.reconcile_unknown(
            handle,
            owner_id=handle.owner_id,
            profile_id=parent.profile_id,
            process_instance=parent.process_instance,
            authority_epoch=parent.authority_epoch,
        )
        if result is not None:
            for completed in restored:
                coordinator.mark_unknown(completed, "cua_physical_close_unknown")
            return False
        restored.append(handle)
    return True


def retry_close(parent: Any, physical: Any) -> bool:
    service = parent._service
    if (
        parent.state != "unknown"
        or physical.state != "unknown"
        or physical.unknown_reason != "cua_physical_close_unknown"
        or getattr(service, "unknown", False) is not True
    ):
        return False
    if not native_close_ready(physical) or not reconcile_close_handles(parent, physical):
        return False
    result = parent.coordinator.mark_terminal(physical.handle)
    if result is not None:
        physical.unknown_reason = "cua_physical_release_failed"
        parent.coordinator.mark_unknown(physical.handle, "cua_physical_release_failed")
        parent.coordinator.mark_unknown(parent.root_handle, "cua_physical_release_failed")
        return False
    physical.state = "closed"
    physical.unknown_reason = None
    service.unknown = False
    return True
