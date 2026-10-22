"""Reader-shape helpers shared by cold sidecar transports and ServiceProcessReceipt."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any


_DEFAULT_STREAMS = frozenset({"stdout", "stderr"})
_STREAM_NAMES = frozenset({"stdout", "stderr"})


class RequiredStreamsError(RuntimeError):
    """A receipt cannot prove which reader streams its source requires."""


def configure_reader_streams(receipt: Any, streams: Iterable[str]) -> bool:
    try:
        required = frozenset(streams)
    except (TypeError, ValueError):
        receipt.required_streams = None
        receipt.mark_unknown("required_streams_invalid")
        return False
    if (
        not required or not required.issubset(_STREAM_NAMES)
        or getattr(receipt, "state", None) not in {"reserved", "identity_bound"}
        or getattr(receipt, "process", None) is not None
        or getattr(receipt, "readers", None)
        or getattr(receipt, "eof_streams", None)
        or getattr(receipt, "joined_streams", None)
    ):
        receipt.required_streams = None
        receipt.mark_unknown("required_streams_invalid")
        return False
    receipt.required_streams = required
    return True


def required_reader_streams(receipt: Any) -> frozenset[str]:
    required = getattr(receipt, "required_streams", _DEFAULT_STREAMS)
    if not isinstance(required, frozenset) or not required or not required.issubset(_STREAM_NAMES):
        receipt.mark_unknown("required_streams_invalid")
        # Returning an empty set lets a caller accidentally prove an invalid receipt with
        # another empty set.  A typed refusal keeps every consumer fail-closed while the
        # receipt remains UNKNOWN and recoverable through its existing coordinator handle.
        raise RequiredStreamsError("required_streams_invalid")
    return required
