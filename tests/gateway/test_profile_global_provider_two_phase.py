"""Focused contracts for the I18 global reversible provider boundary."""

import asyncio
from types import SimpleNamespace

import pytest


class _SourceCustody:
    def __init__(self, agents):
        self._agents = tuple(
            (agent, object(), SimpleNamespace(), agent._memory_manager) for agent in agents
        )
        self.prepared = []

    def global_provider_agents(self, generation):
        assert generation == 7
        return self._agents

    def global_provider_snapshots_valid(self, generation):
        return generation == 7

    def mark_global_provider_prepared(self, generation, prepared):
        assert generation == 7
        self.prepared.append(prepared)
        return True


class _FakeAgent:
    def __init__(self, *, resume=True, commit_results=("released",)):
        self._memory_manager = object()
        self.resume_result = resume
        self.commit_results = list(commit_results)
        self.prepare_calls = 0
        self.resume_calls = []
        self.commit_calls = []

    def prepare_memory_provider(self):
        self.prepare_calls += 1
        return _PreparationReport(self._memory_manager)

    def resume_memory_provider(self, lease):
        self.resume_calls.append(lease)
        return self.resume_result

    def commit_memory_provider(self, lease):
        self.commit_calls.append(lease)
        return _RetirementReport(self.commit_results.pop(0))


def _PreparationReport(manager):
    from agent.memory_retirement import ManagerPreparationReport, ManagerRetirementLease

    lease = ManagerRetirementLease(
        manager=manager, providers=(), receipts=(), provider_leases=[],
        provider_prepared=[], provider_attempts=[], provider_reasons=[], scope=(None, None),
        setup_generation=0, provider_preparation_calls=[],
    )
    return ManagerPreparationReport("prepared", "prepared", lease)


def _RetirementReport(status):
    from agent.memory_retirement import MemoryRetirementReport

    return MemoryRetirementReport(status, status)


class _DelayedAgent(_FakeAgent):
    def __init__(self):
        super().__init__()
        self.release_prepare = asyncio.Event()

    async def prepare_memory_provider(self):
        await self.release_prepare.wait()
        return _PreparationReport(self._memory_manager)


@pytest.mark.asyncio
async def test_global_prepare_abort_keeps_exact_manager_lease_on_partial_resume():
    from gateway.profile_global_provider import custody_for

    first = _FakeAgent()
    second = _FakeAgent(resume=False)
    runner = SimpleNamespace(_profile_lifecycle_custody=_SourceCustody((first, second)))
    custody = custody_for(runner)

    prepared = await custody.prepare(7)
    assert prepared.ready is True

    assert await custody.resume(7) is False
    assert first.prepare_calls == 2
    assert second.prepare_calls == 1
    assert first.resume_calls and second.resume_calls
    assert runner._profile_lifecycle_custody.prepared[-1] is True


@pytest.mark.asyncio
async def test_global_commit_unknown_reuses_same_lease_until_released():
    from gateway.profile_global_provider import custody_for

    agent = _FakeAgent(commit_results=("unknown", "released"))
    runner = SimpleNamespace(_profile_lifecycle_custody=_SourceCustody((agent,)))
    custody = custody_for(runner)

    assert (await custody.prepare(7)).ready is True
    assert await custody.commit(7) is False
    lease = agent.commit_calls[0]
    assert await custody.commit(7) is True
    assert agent.commit_calls == [lease, lease]


@pytest.mark.asyncio
async def test_global_prepare_timeout_keeps_abort_closed_until_same_call_settles():
    from gateway.profile_global_provider import custody_for

    agent = _DelayedAgent()
    runner = SimpleNamespace(_profile_lifecycle_custody=_SourceCustody((agent,)))
    custody = custody_for(runner)

    prepared = await custody.prepare(7, remaining=0.001)
    assert prepared.ready is False
    assert prepared.reason == "preparation_timeout"
    assert await custody.resume(7, remaining=0.001) is False

    agent.release_prepare.set()
    assert await custody.resume(7, remaining=1.0) is True


@pytest.mark.asyncio
async def test_public_global_route_prepares_and_resumes_without_legacy_hard_retire(
    tmp_path, monkeypatch,
):
    from gateway import run_runtime
    from gateway.session import _session_key_namespace
    from tests.gateway.test_recovery_custody_handoff import (
        _begin_public_preservation_close, _real_recovery_runtime,
    )

    class PublicAgent(_FakeAgent):
        def __init__(self):
            super().__init__()
            self._memory_manager = object()

        def retire_memory_provider(self):
            raise AssertionError("global prepare must not use legacy hard retirement")

    async with _real_recovery_runtime(tmp_path, monkeypatch) as fixture:
        runner = fixture.authority.runner
        agent = PublicAgent()
        key = f"{_session_key_namespace('default')}:global-provider-public"
        runner._agent_cache[key] = (agent, "canonical")
        runner, _params, closing = await _begin_public_preservation_close(fixture)
        record = next(iter(runner._profile_lifecycle_custody._current.values()))
        assert runner._profile_lifecycle_custody.global_provider_agents(
            closing["close_generation"],
        ), record.global_provider_snapshot

        prepared = await run_runtime.prepare_preservation_handoff(
            runner, {"receipt": closing}, "i10-test-peer",
        )
        assert agent.prepare_calls == 1, prepared
        assert not agent.commit_calls
        assert prepared["status"] == "refused"

        reopened = await run_runtime.abort_preservation_drain(
            runner, {"receipt": closing}, "i10-test-peer",
        )
        assert reopened["status"] == "open"
        assert len(agent.resume_calls) == 1
