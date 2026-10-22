"""A provider profile can supply its own client — the registration seam.

A provider whose wire protocol is not OpenAI-over-HTTP (the ACP subprocess
shims) supplies its client via ``ProviderProfile.create_client()``, from inside
or outside the tree. These tests pin the hook through the real entry point
(``create_openai_client``), its failure isolation, and the capability flags
that let such a client opt out of the auxiliary transport/async wrappers.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import providers as _providers
from providers.base import ProviderProfile


class _FakeClient:
    HERMES_SKIP_TRANSPORT_WRAP = True
    HERMES_SKIP_ASYNC_WRAP = True
    api_key = "k"
    base_url = "acp://seam-test"

    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _SeamProfile(ProviderProfile):
    def create_client(self, **kwargs):
        return _FakeClient(**kwargs)


class _ExplodingProfile(ProviderProfile):
    def create_client(self, **kwargs):
        raise RuntimeError("plugin is broken")


@pytest.fixture
def registered():
    """Register profiles for one test and restore the registry afterwards."""
    _providers._discover_providers()
    snapshot = (dict(_providers._REGISTRY), dict(_providers._ALIASES), _providers._PROVIDER_LIST_CACHE)
    yield _providers.register_provider
    _providers._REGISTRY.clear()
    _providers._REGISTRY.update(snapshot[0])
    _providers._ALIASES.clear()
    _providers._ALIASES.update(snapshot[1])
    _providers._PROVIDER_LIST_CACHE = snapshot[2]


@pytest.fixture
def standalone_external_policy(tmp_path):
    """Give external-process profiles a real source-owned policy in the seam tests."""
    from gateway.runtime_service_operations import (
        close_after_finalizers,
        policy_for_parent,
        reserve_standalone_external_process_parent,
    )
    from hermes_state import SessionDB

    db = SessionDB(db_path=tmp_path / "state.db")
    try:
        with reserve_standalone_external_process_parent(
            profile_home=tmp_path,
            profile_id=str(tmp_path),
            operation_id="provider-client-seam",
            source_identity="provider-client-seam",
            profile_db=db,
        ) as parent:
            policy = policy_for_parent(parent, mode="standalone_maintenance")
            yield policy
            if parent.state != "terminal":
                assert close_after_finalizers(parent) is True
    finally:
        db.close()


def _agent(provider: str = "", policy=None):
    return SimpleNamespace(
        provider=provider,
        _external_process_policy=policy,
        _client_log_context=lambda: "",
        _build_keepalive_http_client=lambda *a, **k: None,
    )


def _build(provider, base_url, policy=None):
    from agent.agent_runtime_helpers import create_openai_client

    return create_openai_client(
        _agent(provider, policy), {"api_key": "k", "base_url": base_url}, reason="t", shared=False,
    )


def test_an_out_of_tree_profile_supplies_the_client_through_create_openai_client(
    registered, standalone_external_policy,
):
    from openai import OpenAI

    registered(_SeamProfile(name="seam-test", aliases=("seam",), base_url="acp://seam-test"))
    # By name, by alias, and by base_url prefix when no provider name is set.
    assert isinstance(_build("seam-test", "acp://seam-test", standalone_external_policy), _FakeClient)
    assert isinstance(_build("seam", "acp://seam-test", standalone_external_policy), _FakeClient)
    assert isinstance(_build("", "acp://seam-test/x", standalone_external_policy), _FakeClient)
    # No hook → the standard client, untouched by the seam.
    assert isinstance(_build("openai-api", "https://api.example/v1"), OpenAI)


def test_a_broken_plugin_falls_through_instead_of_taking_the_turn_down(
    registered, standalone_external_policy,
):
    from agent.agent_runtime_helpers import _provider_supplied_client

    registered(_ExplodingProfile(name="seam-boom", base_url="acp://seam-boom"))
    assert _provider_supplied_client(
        _agent("seam-boom", standalone_external_policy), {"api_key": "k"},
    ) is None


def test_copilot_acp_still_gets_its_acp_client_via_its_profile(standalone_external_policy):
    from agent.copilot_acp_client import CopilotACPClient

    assert isinstance(_build("copilot-acp", "acp://copilot", standalone_external_policy), CopilotACPClient)
    assert isinstance(_build("", "acp://copilot", standalone_external_policy), CopilotACPClient)


def test_skip_flags_replace_the_isinstance_checks_for_in_and_out_of_tree_clients():
    from agent.auxiliary_client import _maybe_wrap_anthropic, _to_async_client
    from agent.copilot_acp_client import CopilotACPClient
    from agent.gemini_native_adapter import GeminiNativeClient

    assert CopilotACPClient.HERMES_SKIP_TRANSPORT_WRAP and CopilotACPClient.HERMES_SKIP_ASYNC_WRAP
    assert GeminiNativeClient.HERMES_SKIP_TRANSPORT_WRAP
    assert not getattr(GeminiNativeClient, "HERMES_SKIP_ASYNC_WRAP", False)

    client = _FakeClient()  # never imported by auxiliary_client
    assert _maybe_wrap_anthropic(client, "m", "k", "acp://seam-test") is client
    assert _to_async_client(client, "m")[0] is client
