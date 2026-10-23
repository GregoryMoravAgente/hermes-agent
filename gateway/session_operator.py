"""Private provenance for already-authorized operator input.

Operator access is profile-local, not account linking or a change of authorship.
It never claims unowned legacy history: that still requires the native migration
path. Only SessionAuthority.submit writes this envelope after authorization;
public submission parameters cannot supply it.
"""
from hermes_state_runtime import RuntimeStoreError

# A local ``prompt.submit`` is a server-owned managed producer when the session's
# frozen policy selects the out-of-process worker.  Keep this discriminator in its
# own closed descriptor: ``attachments_v1`` is an immutable media schema and must
# not carry execution routing metadata.
MANAGED_WORKER_FIELD = 'managed_worker_v1'
MANAGED_WORKER_DESCRIPTOR = {'producer_process_mode': 'managed_turn'}

# Fields ``SessionAuthority.submit`` commits for a local session's input; producers commit their own envelope.
LOCAL_INPUT_FIELDS = frozenset({'text', 'attachments_v1', 'finite', 'unattended', 'surface_v1', 'display_v1',
                                'cli_turn_v1', 'kanban_card_v1', MANAGED_WORKER_FIELD})


def is_viewer_input(row):
    """A row a session viewer submitted through ``session.submit`` (a client's own prompt), not a producer
    admission: ``local_automation_v1`` (plugin injection, delegation completions, process and watch notices,
    /loop, heartbeat), a hosted-room task (``hosted:`` request id) or any other envelope."""
    return (not str(row['request_id']).startswith('hosted:')
            and {'text'} <= set(row['payload']) <= LOCAL_INPUT_FIELDS | {'local_operator_v1'})


def check_local_input(authority, ref, row, *, source=None):
    payload = row['payload']
    allowed = set(LOCAL_INPUT_FIELDS)
    managed_descriptor = payload.get(MANAGED_WORKER_FIELD)
    if managed_descriptor is not None:
        if managed_descriptor != MANAGED_WORKER_DESCRIPTOR:
            raise RuntimeStoreError('permission_denied')
        # The marker is written only by SessionAuthority._submit.  Re-check the
        # frozen policy at the claim boundary so a copied/old marker cannot route a
        # normal in-process turn through the managed custody path.
        from gateway.session_managed_worker import managed_policy
        if not callable(getattr(authority.runner, '_adapter_for_source', None)):
            raise RuntimeStoreError('permission_denied')
        if managed_policy(authority, ref) is None:
            raise RuntimeStoreError('permission_denied')
    if 'local_operator_v1' in payload:
        allowed.add('local_operator_v1')
        if payload['local_operator_v1'] != {
                'profile_id': authority.profile_id, 'session_id': ref.session_id,
                'principal_id': row['principal_id']}:
            raise RuntimeStoreError('permission_denied')
    else:
        source = source or authority.sessions[ref.session_id].source
        if row['principal_id'] != source.user_id:
            raise RuntimeStoreError('permission_denied')
    if not {'text'} <= set(payload) <= allowed:
        raise RuntimeStoreError('permission_denied')
