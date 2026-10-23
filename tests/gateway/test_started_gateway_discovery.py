"""Public gateway fixtures must expose the production discovery envelope."""


def test_started_mux_is_discoverable_through_its_live_control_listener(started_mux):
    from hermes_cli.gateway_client import _session_ticket
    from hermes_cli.gateway_runtime import discover_gateway_endpoint

    runner, homes, _loop, _call = started_mux
    discovery = discover_gateway_endpoint(homes["default"])

    assert discovery.state == "ready", discovery
    assert discovery.reason_code is None
    endpoint = discovery.endpoint
    descriptor = runner.session_runtime_descriptor
    assert endpoint.instance_id == descriptor["instance_id"]
    assert endpoint.authority_epoch == descriptor["authority_epoch"]
    assert endpoint.runtime_protocol == descriptor["runtime_protocol"]
    assert endpoint.api_origin == descriptor["api_origin"]
    assert endpoint.supervisor in {"none", "systemd", "launchd", "windows", "external"}
    ticket = _session_ticket(homes["default"], endpoint)
    grant = runner.session_ticket_store.redeem(
        ticket, profile_id=endpoint.profile_id, purpose="interactive",
    )
    assert grant["instance_id"] == endpoint.instance_id
    assert grant["profile_id"] == endpoint.profile_id
    assert grant["subject"].startswith(("uid:", "sid:"))
    assert "session:create" in grant["capabilities"]
