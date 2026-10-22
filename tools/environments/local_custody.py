"""I16 source ownership for managed local terminal children.

The process registry remains the I15 physical child owner.  This module only joins the
same real Popen/process-group to the I16 service parent reserved by the active I04 turn;
there is no second process ledger and no PID-only success path.
"""

from __future__ import annotations

import os
import signal
from typing import Any

from gateway.runtime_service_custody import current_turn_binding
from gateway.runtime_sidecar_sources import SidecarOwner, bind_process, mark_unknown, reserve_managed_sidecar


class _LocalPosixScope:
    """Observe and terminate the exact process group created for one local child."""

    __slots__ = ("pgid", "process")

    def __init__(self, process: Any, pgid: int):
        self.process = process
        self.pgid = pgid

    def observe_empty(self) -> bool | None:
        if self.process.poll() is None:
            return False
        killpg = getattr(os, "killpg", None)
        if killpg is None:
            return False
        try:
            killpg(self.pgid, 0)
        except ProcessLookupError:
            return True
        except (OSError, PermissionError):
            return False
        return False

    def terminate(self) -> None:
        killpg = getattr(os, "killpg", None)
        if killpg is None or os.name == "nt" or self.pgid == os.getpgrp():
            raise OSError("local process group is not a safe source-owned scope")
        if self.observe_empty() is True:
            return
        try:
            pid = self.process.pid
            leader_live = pid > 0 and self.process.poll() is None
            leader_group = os.getpgid(pid) if leader_live else None
        except (AttributeError, OSError, ProcessLookupError, TypeError, ValueError):
            leader_live, leader_group = False, None
        if not leader_live or leader_group != self.pgid:
            raise OSError("local process group leader identity is no longer current")
        killpg(self.pgid, signal.SIGTERM)


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
        producer_key="environment",
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
        owner.physical.contained_scope_provider = _LocalPosixScope(process, pgid)
    except BaseException:  # health: allow BLE001 -- a bind failure retains the exact source owner as UNKNOWN
        mark_unknown(owner, "partial_launch")
        return False
    return True


def record_local_readback(owner: SidecarOwner, reader: Any) -> bool:
    """Attach the terminal reader's joined EOF to the exact managed sidecar receipt."""
    physical = getattr(owner, "physical", None)
    if physical is None or getattr(owner, "process", None) is None or reader is None:
        mark_unknown(owner, "local_readback_unavailable")
        return False
    try:
        from gateway.runtime_sidecar_sources import bind_reader

        bind_reader(owner, "stdout", reader)
        physical.reader_eof("stdout", reader)
        physical.reader_joined("stdout", reader)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        mark_unknown(owner, "local_readback_unavailable")
        return False
    return (
        physical.state != "unknown"
        and physical.eof_streams == {"stdout"}
        and physical.joined_streams == {"stdout"}
    )


def finish_local_process(owner: SidecarOwner, *, reason: str = "local_readback_unavailable") -> None:
    """Retain the exact parent when local result/readback cannot prove I16 delivery."""
    mark_unknown(owner, reason)
