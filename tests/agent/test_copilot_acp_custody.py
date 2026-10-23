"""Provider-boundary controls for the real Copilot ACP producer."""

from types import SimpleNamespace
import sys


def test_provider_boundary_attaches_the_exact_parent_before_client_creation(tmp_path):
    from gateway.runtime_service_operations import (
        ExternalProcessParent, ProducerPolicy, provider_process_kwargs,
    )

    parent = ExternalProcessParent(
        mode="standalone_maintenance", profile_home=tmp_path, profile_id=str(tmp_path),
        operation_id="acp-turn", source_identity="copilot-acp", principal_id=None,
        process_instance="process", authority_epoch=1, coordinator=object(), root_handle=object(),
    )
    policy = ProducerPolicy("standalone_maintenance", parent, True, False, False)
    agent = SimpleNamespace(_external_process_policy=policy)
    profile = SimpleNamespace(auth_type="external_process")

    kwargs = provider_process_kwargs(agent, profile, {"base_url": "acp://copilot"})

    assert kwargs["producer_policy"] is policy
    assert agent._external_process_parent is parent
    assert parent._owner_agent is agent


def test_real_acp_child_publishes_and_releases_two_sequential_completions(tmp_path):
    """Drive the real ACP child/readers and source-owned receipt finalizer twice."""
    from gateway.runtime_service_operations import (
        policy_for_parent, reserve_standalone_external_process_parent,
    )
    from hermes_state import SessionDB
    from agent.copilot_acp_client import CopilotACPClient

    script = (
        "import json,sys\n"
        "for line in sys.stdin:\n"
        " m=json.loads(line); method=m.get('method'); rid=m.get('id')\n"
        " if method == 'initialize': out={'jsonrpc':'2.0','id':rid,'result':{}}\n"
        " elif method == 'session/new': out={'jsonrpc':'2.0','id':rid,'result':{'sessionId':'acp-test-session'}}\n"
        " elif method == 'session/prompt':\n"
        "  sid=m['params']['sessionId']; out={'jsonrpc':'2.0','method':'session/update','params':{'sessionId':sid,'update':{'sessionUpdate':'agent_message_chunk','content':{'text':'ok'}}}}; print(json.dumps(out),flush=True); out={'jsonrpc':'2.0','id':rid,'result':{}}\n"
        " else: out={'jsonrpc':'2.0','id':rid,'result':{}}\n"
        " print(json.dumps(out),flush=True)\n"
    )
    db = SessionDB(db_path=tmp_path / "state.db")
    try:
        with reserve_standalone_external_process_parent(
            profile_home=tmp_path, profile_id=str(tmp_path), operation_id="acp-turn",
            source_identity="copilot-acp", profile_db=db, producer_key="acp",
        ) as parent:
            policy = policy_for_parent(parent, mode="standalone_maintenance")
            client = CopilotACPClient(
                command=sys.executable, args=["-c", script], acp_cwd=str(tmp_path),
                producer_policy=policy,
            )
            try:
                for _ in range(2):
                    text, _reasoning = client._run_prompt("hello", timeout_seconds=5)
                    assert text == "ok"
                    assert parent._physical is None
                    assert parent._service.physical is None
            finally:
                client.close()
            assert parent.state == "terminal"
    finally:
        db.close()
