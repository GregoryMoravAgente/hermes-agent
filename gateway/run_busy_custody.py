"""Runner-side bridges for classic busy producer ownership.

The helpers resolve the active, profile-scoped ``SessionAuthority`` at call time.  They do not
cache a profile or manufacture a parent lease, which keeps queue callbacks on the same authority
that admitted the source event.
"""
from __future__ import annotations


def _authority(runner, event=None):
    bound = getattr(event, '_classic_custody_authority', None) if event is not None else None
    if bound is not None:
        return bound
    if runner is None:
        return None
    # A shared primary adapter can route an event into a secondary profile before the
    # profile-scoped message handler runs.  Resolve the classic custody owner from the
    # source's pinned runtime identity at this producer boundary; ambient scope is still
    # the fallback for legacy/unpinned events.
    if event is not None:
        try:
            from gateway.session_identity import identity_of
            from gateway.session_authorities import authority_for_home
            identity = identity_of(getattr(event, 'source', None))
            if identity is not None:
                scoped = authority_for_home(runner, identity.runtime_home)
                if scoped is not None:
                    return scoped
        except (AttributeError, KeyError, TypeError, ValueError):
            pass
    from gateway.session_authorities import active_authority
    return active_authority(runner)


def begin_classic_event(runner, event, session_key, producer, *, state='queued', parent=None):
    bound = getattr(event, '_classic_custody_authority', None)
    active = _authority(runner, event)
    if bound is not None and getattr(bound, 'runner', None) is not runner:
        return False
    if bound is not None and active is None and runner is not None and (
        hasattr(runner, 'session_authorities') or hasattr(runner, 'session_authority')
    ):
        return False
    if bound is not None and active is not None and bound is not active:
        return False
    authority = bound or active
    if authority is None:
        # Bare adapter doubles and legacy transports have no authority surface at all and retain
        # their historical compatibility path.  A supported GatewayRunner that exposes an
        # authority registry must fail closed before it mutates a FIFO/debounce/running slot.
        if runner is None or not (
            hasattr(runner, 'session_authorities') or hasattr(runner, 'session_authority')
        ):
            return True
        return False
    return authority._classic_begin(event, session_key, producer, state=state, parent=parent)


def mark_classic_event(runner, event, state):
    authority = _authority(runner, event)
    if authority is not None:
        authority._classic_mark_state(event, state)


def attach_classic_event(runner, aggregate, event):
    authority = _authority(runner, aggregate) or _authority(runner, event)
    if authority is not None:
        authority._classic_attach_event(aggregate, event)


def finish_classic_event(runner, event, *, success, reason='join_failed', receipt=None):
    authority = _authority(runner, event)
    if authority is not None:
        authority._classic_finish(event, success=success, reason=reason, receipt=receipt)


def release_unaccepted_classic_event(runner, event):
    authority = _authority(runner, event)
    if authority is not None:
        authority._classic_release_unaccepted(event)
