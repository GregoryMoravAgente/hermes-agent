"""Physical-tip witness validation shared by the managed SessionDB writer paths.

This topical sibling keeps the persistence proof facade within its file-size budget while
retaining one strict resolver for direct and ACP writer handoffs.
"""

from __future__ import annotations

from pathlib import Path
import sqlite3
from typing import Any

_UNSET = object()


def _store_path(agent: Any) -> str | None:
    db = getattr(agent, "_session_db", None)
    raw = getattr(db, "db_path", None)
    if raw is None or isinstance(raw, bool):
        return None
    try:
        return str(Path(raw).expanduser().resolve())
    except (OSError, TypeError, ValueError):
        return None


def _physical_tip_value(value: Any) -> Any:
    """Extract only the canonical immutable physical-tip witness from a retained value."""
    if value is None:
        return None
    try:
        from gateway.session_turn_physical_tip import PhysicalTipWitness
    except (AttributeError, ImportError):
        return None
    if type(value) is PhysicalTipWitness:
        return value
    nested = getattr(value, "physical_tip_witness", _UNSET)
    return nested if type(nested) is PhysicalTipWitness else None

def _is_direct_physical_tip_witness(value: Any) -> bool:
    """Recognize the exact raw tip witness used by direct OpenAI/classic turns."""
    if value is None:
        return False
    try:
        from gateway.session_turn_physical_tip import PhysicalTipWitness
    except (AttributeError, ImportError):
        return False
    return type(value) is PhysicalTipWitness

def _is_managed_receipt_witness(value: Any) -> bool:
    """Recognize the existing ACP receipt witness without accepting lookalike values."""
    if value is None:
        return False
    try:
        from agent.copilot_acp_durable_receipt import _ManagedDurableReceiptWitness
    except (AttributeError, ImportError):
        return False
    return type(value) is _ManagedDurableReceiptWitness

def _managed_physical_tip_valid(agent: Any, binding: Any, expected: Any = None) -> bool:
    physical = getattr(agent, "session_id", None)
    root = getattr(binding, "session_id", None)
    try:
        from gateway.runtime_service_custody import TurnCustodyBinding
        from gateway.session_classic_custody import ClassicWriterBinding
        from gateway.session_turn_physical_tip import resolve_physical_tip_witness
    except (AttributeError, ImportError):
        return False
    if isinstance(binding, ClassicWriterBinding):
        return physical == root and expected is None
    if not isinstance(binding, TurnCustodyBinding):
        return False
    try:
        # Turn custody always resolves the current source-owned tip, including an
        # uncompressed root.  The root shortcut is reserved for Classic's legacy carrier.
        witness = resolve_physical_tip_witness(binding, physical, _store_path(agent))
        expected_tip = _physical_tip_value(expected) if expected is not None else None
        return witness is not None and (expected is None or witness == expected_tip)
    except (
        AttributeError, ImportError, KeyError, OSError, RuntimeError, TypeError, ValueError,
        sqlite3.Error,
    ):
        return False

def _gateway_writer_physical_witness_valid(
    agent: Any, binding: Any, proof: Any, physical_witness: Any,
) -> bool:
    """Revalidate the immutable tip for every path, then apply the typed ACP guard only to ACP receipts.

    Direct OpenAI turns retain a raw ``PhysicalTipWitness``.  They have no managed ACP receipt
    validator, so routing that value through ``validate_managed_durable_receipt_witness`` would
    reject an otherwise authenticated direct retry.  A managed ACP receipt still must pass its
    existing provider/receipt validator after the same current physical-tip check.
    """
    if physical_witness is None or not _managed_physical_tip_valid(
        agent, binding, _physical_tip_value(physical_witness),
    ):
        return False
    if _is_direct_physical_tip_witness(physical_witness):
        return True
    if not _is_managed_receipt_witness(physical_witness):
        return False
    acp = getattr(agent, "_managed_acp_client", None)
    validator = getattr(acp, "validate_managed_durable_receipt_witness", None)
    return callable(validator) and validator(agent, proof, physical_witness) is True
