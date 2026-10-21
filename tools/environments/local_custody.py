"""I16 source ownership for managed local terminal children.

The process registry remains the I15 physical child owner.  This module only joins the
same real Popen/process-group to the I16 service parent reserved by the active I04 turn;
there is no second process ledger and no PID-only success path.
"""

from __future__ import annotations

import os
from typing import Any

from gateway.runtime_service_custody import current_turn_binding
from gateway.runtime_sidecar_sources import SidecarOwner, bind_process, mark_unknown, reserve_managed_sidecar


def prepare_managed_local_sidecar(
    *,
    source_identity: str,
    operation_id: str | None = None,
    binding: Any = None,
) -> SidecarOwner | None:
    """Reserve the I16 parent before LocalEnvironment's Popen effect."""
    active_binding = binding if binding is not None else current_turn_binding()
    if active_binding is None:
        return None
    return reserve_managed_sidecar(
        active_binding,
        family="local_terminal",
        source_identity=source_identity,
        operation_id=operation_id,
    )


def bind_local_process(owner: SidecarOwner, process: Any) -> bool:
    """Bind the exact Popen and POSIX process group to the reserved source receipt."""
    pid = getattr(process, "pid", None)
    if type(pid) is not int or pid <= 0:
        mark_unknown(owner, "process_identity_rejected")
        return False
    if os.name == "nt":
        # The existing I15 WindowsLaunchTransaction owns the native Job.  Reusing that
        # transaction for the I16 receipt would violate its exact-custody identity, so the
        # source remains retained UNKNOWN until a typed shared Job witness exists.
        mark_unknown(owner, "windows_process_scope_unavailable")
        return False
    try:
        pgid = getattr(process, "_hermes_pgid", None) or os.getpgid(pid)
    except (AttributeError, OSError, ProcessLookupError):
        mark_unknown(owner, "physical_scope_unknown")
        return False
    if type(pgid) is not int or pgid <= 0:
        mark_unknown(owner, "physical_scope_unknown")
        return False
    try:
        bind_process(
            owner,
            process,
            scope="posix_process_group",
            scope_identity=f"posix_process_group:{pid}:{pgid}",
            process_group_id=pgid,
        )
    except BaseException:  # health: allow BLE001 -- a bind failure retains the exact source owner as UNKNOWN
        mark_unknown(owner, "partial_launch")
        return False
    return True


def finish_local_process(owner: SidecarOwner, *, reason: str = "local_readback_unavailable") -> None:
    """Retain the exact parent when local result/readback cannot prove I16 delivery."""
    mark_unknown(owner, reason)
