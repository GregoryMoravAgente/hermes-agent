"""Session client PATH precedence around Hermes's fallback directory."""

import os

import pytest

from tools.environments import local


@pytest.mark.parametrize(
    "client_env,local_client,payload,existing_parts,expected",
    [
        ({"PATH": "/client/bin"}, True, False, ("/client/bin", "/usr/bin"),
         ["/client/bin", "/usr/bin", "/opt/hermes/bin"]),
        ({"PATH": "/client/bin"}, False, False, ("/daemon/bin", "/usr/bin"),
         ["/opt/hermes/bin", "/daemon/bin", "/usr/bin"]),
        ({"NVM_DIR": "/client/.nvm"}, True, False, ("/daemon/bin", "/usr/bin"),
         ["/opt/hermes/bin", "/daemon/bin", "/usr/bin"]),
        ({"PATH": "/client/bin"}, True, True, ("/client/bin", "/usr/bin"),
         ["/opt/hermes/bin", "/client/bin", "/usr/bin"]),
    ],
)
def test_session_client_path_order_keeps_remote_and_payload_boundaries(
    monkeypatch, client_env, local_client, payload, existing_parts, expected
):
    """Only a locally accepted client PATH moves Hermes behind user tools."""
    from agent.session_client_env import client_env_scope

    monkeypatch.setattr(local, "_HERMES_BIN_DIR", "/opt/hermes/bin")
    monkeypatch.setattr(local, "_HERMES_BIN_DIR_IS_PAYLOAD", payload)
    with client_env_scope(client_env, local=local_client):
        result = local._prepend_hermes_bin_dir(os.pathsep.join(existing_parts))
    assert result.split(os.pathsep) == expected
