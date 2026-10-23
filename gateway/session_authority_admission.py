"""Committed admission callbacks for the session authority.

The mixin keeps the durable-row bind and pending publication order together while the
SessionAuthority facade owns the storage, scheduler, and receipt implementations.
"""

from __future__ import annotations

from hermes_state_runtime import RuntimeStoreError


class SessionAuthorityAdmissionMixin:
    def _admitted(self, ref, event, row):
        if event is not None:
            # The durable row has committed before this callback. Keep the established accepted
            # marker reachable for heartbeat/wake retry, while a typed False still stops pending
            # publication and the after-scheduler until the exact classic owner is reconciled.
            event._gateway_accepted = True
            committed = self._classic_admission_committed(event, row)
            if committed is False:
                raise RuntimeStoreError('preservation_owner_unmapped')
            if row['status'] == 'queued' and (on_commit := getattr(event, '_on_commit', None)) is not None:
                on_commit()  # slash-command ACK follows durable admission and custody binding
        self._publish_pending(ref)
        return self._receipt(row)

    def _schedule_admitted(self, ref, _receipt=None):
        # tracked_write's ``after`` runs only after the admission receipt is committed.
        self._schedule(ref)
