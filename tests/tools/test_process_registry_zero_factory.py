"""Real I15 cold-start and source-admission controls.

The zero path is deliberately tested as a typed close-fence classification.  It may classify a
real first-run registry as childless, but it cannot produce a preservation seal without the
existing physical profile owner and lifecycle witnesses.
"""

import json
import shlex
import subprocess
import sys

import pytest

from gateway.runtime_custody import RuntimeCustodyCoordinator
from gateway.runtime_ownership import canonical_home, process_ownership
from hermes_cli.backend_retirement import RetirementFence
from hermes_cli.profile_provenance_bootstrap import bootstrap_profile_home
from tools.process_registry import ProcessRegistry
from tools.process_registry_zero_factory import ProcessRegistryZeroFactory, process_registry_owner_evidence


def _runtime(tmp_path, monkeypatch, *, reserve_owner=False, profile_home=None):
    home = (profile_home or (tmp_path / "profile")).resolve()
    if reserve_owner:
        bootstrap_profile_home(home, profile_kind="named", profile_name="i15-cold-test")
    else:
        home.mkdir(exist_ok=True)
    monkeypatch.setenv("HERMES_HOME", str(home))
    if reserve_owner:
        process_ownership.reserve([home], purpose="i15-cold-test")
    coordinator = RuntimeCustodyCoordinator(
        process_instance="i15-cold-test",
        authority_epoch=7,
        fence=RetirementFence(),
    )
    registry = ProcessRegistry()
    registry.bind_runtime_custody(coordinator)
    factory = registry._i15_zero_factory
    factory.note_checkpoint_recovery(str(home))
    return home, registry, coordinator, factory


def _blocking_command() -> str:
    return f"{shlex.quote(sys.executable)} -c {shlex.quote('import threading; threading.Event().wait()')}"


def test_missing_checkpoint_without_authenticated_first_run_is_unknown(tmp_path, monkeypatch):
    _home, _registry, _coordinator, factory = _runtime(tmp_path, monkeypatch)

    observation = factory.observe_close(1)
    assert observation.kind == "unknown"
    assert observation.reason == "checkpoint_or_registration_unknown"


def test_existing_pm_state_and_prior_lock_do_not_authenticate_cold_zero(tmp_path, monkeypatch):
    home, _registry, _coordinator, factory = _runtime(tmp_path, monkeypatch)
    (home / "tools").mkdir()
    (home / "installs").mkdir()
    (home / "gateway.lock").write_text("prior owner", encoding="utf-8")

    observation = factory.observe_close(1)
    assert observation.kind == "unknown"
    assert observation.reason == "checkpoint_or_registration_unknown"


def test_malformed_prior_lock_stays_unknown_without_creation_provenance(tmp_path, monkeypatch):
    home = (tmp_path / "malformed-prior").resolve()
    home.mkdir()
    (home / "gateway.lock").write_text("{malformed", encoding="utf-8")
    monkeypatch.setenv("HERMES_HOME", str(home))
    process_ownership.reserve([home], purpose="i15-cold-test")
    try:
        _home, _registry, _coordinator, factory = _runtime(
            tmp_path, monkeypatch, profile_home=home,
        )
        observation = factory.observe_close(1)
        assert observation.kind == "unknown"
        assert observation.reason == "checkpoint_or_registration_unknown"
    finally:
        assert process_ownership.release(home) is True


def test_symlink_profile_and_checkpoint_are_refused_before_process_path_resolution(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    checkpoint_target = tmp_path / "checkpoint.json"
    checkpoint_target.write_text("[]", encoding="utf-8")
    checkpoint_link = tmp_path / "linked-checkpoint.json"
    checkpoint_link.symlink_to(checkpoint_target)
    coordinator = RuntimeCustodyCoordinator(
        process_instance="i15-symlink-test", authority_epoch=7, fence=RetirementFence(),
    )
    registry = ProcessRegistry()
    factory = ProcessRegistryZeroFactory(registry, coordinator)

    assert factory.register_profile("linked", 7, linked) is False
    assert factory.register_profile(
        "real", 7, target, checkpoint_path=checkpoint_link,
    ) is False
    assert factory.registrations() == ()


def test_deleted_prior_lock_stays_unknown_without_creation_provenance(tmp_path, monkeypatch):
    home = (tmp_path / "deleted-prior").resolve()
    home.mkdir()
    previous = type(process_ownership)()
    previous.reserve([home], purpose="i15-previous")
    assert previous.close().failed is False
    (home / "gateway.lock").unlink()
    monkeypatch.setenv("HERMES_HOME", str(home))
    process_ownership.reserve([home], purpose="i15-cold-test")
    try:
        _home, _registry, _coordinator, factory = _runtime(
            tmp_path, monkeypatch, profile_home=home,
        )
        observation = factory.observe_close(1)
        assert observation.kind == "unknown"
        assert observation.reason == "checkpoint_or_registration_unknown"
    finally:
        assert process_ownership.release(home) is True


def test_authenticated_first_run_zero_stays_physical_seal_unknown(tmp_path, monkeypatch):
    home, registry, coordinator, factory = _runtime(tmp_path, monkeypatch, reserve_owner=True)
    try:
        assert factory.note_checkpoint_recovery(str(home)) is True
        observation = factory.observe_close(1)
        assert observation.kind == "zero"
        registration = observation.registrations[0]
        assert registration.checkpoint_generation is None
        assert isinstance(registration.first_run_owner_epoch, str)
        assert registration.first_run_physical_binding[0] == str(canonical_home(home))
        assert isinstance(registration.first_run_native_descriptor, int)

        evidence, reason = process_registry_owner_evidence(
            registry,
            None,
            process_instance=coordinator.process_instance,
            authority_epoch=coordinator.authority_epoch,
            close_generation=1,
            owner_id=f"{coordinator.process_instance}:I15_process_registry_terminal",
            launch_profile_id=str(home),
        )
        assert evidence is None
        assert reason == "physical_exclusion_unproven"
    finally:
        assert process_ownership.release(home) is True


def test_lost_native_profile_handle_keeps_zero_observation_unknown(tmp_path, monkeypatch):
    home, _registry, _coordinator, factory = _runtime(
        tmp_path, monkeypatch, reserve_owner=True,
    )
    try:
        handle = process_ownership._handles[home]
        handle.close()
        observation = factory.observe_close(1)
        assert observation.kind == "unknown"
        assert observation.reason == "checkpoint_or_registration_unknown"
    finally:
        # The lost descriptor is deliberately retained as unresolved custody; process exit
        # releases the native exclusion, but this test must not turn it into clean shutdown.
        assert process_ownership.release(home) is False


def test_two_profile_first_run_requires_each_authenticated_witness(tmp_path, monkeypatch):
    home, _registry, _coordinator, factory = _runtime(
        tmp_path, monkeypatch, reserve_owner=True,
    )
    secondary = (tmp_path / "secondary").resolve()
    bootstrap_profile_home(
        secondary, profile_kind="named", profile_name="secondary", identity_root=secondary,
    )
    process_ownership.reserve([secondary], purpose="i15-cold-test")
    try:
        assert factory.register_profile(str(secondary), 7, secondary) is True
        assert factory.note_checkpoint_recovery(str(secondary)) is True

        observation = factory.observe_close(1)
        assert observation.kind == "zero"
        assert {registration.profile_id for registration in observation.registrations} == {
            str(home), str(secondary),
        }
    finally:
        assert process_ownership.release(secondary) is True
        assert process_ownership.release(home) is True


def test_missing_secondary_witness_blocks_two_profile_zero(tmp_path, monkeypatch):
    home, _registry, _coordinator, factory = _runtime(
        tmp_path, monkeypatch, reserve_owner=True,
    )
    secondary = (tmp_path / "secondary").resolve()
    secondary.mkdir()
    try:
        assert factory.register_profile(str(secondary), 7, secondary) is True
        assert factory.note_checkpoint_recovery(str(home)) is True
        assert factory.note_checkpoint_recovery(str(secondary)) is False
        observation = factory.observe_close(1)
        assert observation.kind == "unknown"
        assert observation.reason == "checkpoint_or_registration_unknown"
    finally:
        assert process_ownership.release(home) is True


def test_malformed_checkpoint_is_unknown_not_zero(tmp_path, monkeypatch):
    home, _registry, _coordinator, factory = _runtime(tmp_path, monkeypatch)
    checkpoint = home / "processes.json"
    checkpoint.write_text(json.dumps({"unexpected": True}), encoding="utf-8")
    assert factory.note_checkpoint_recovery(str(home)) is False

    observation = factory.observe_close(1)
    assert observation.kind == "unknown"
    assert observation.reason == "checkpoint_or_registration_unknown"


def test_changed_checkpoint_generation_is_unknown_for_zero(tmp_path, monkeypatch):
    home, _registry, _coordinator, factory = _runtime(tmp_path, monkeypatch)
    checkpoint = home / "processes.json"
    checkpoint.write_text("[]", encoding="utf-8")
    assert factory.note_checkpoint_recovery(str(home)) is True

    observation = factory.observe_close(1)
    assert observation.kind == "unknown"
    assert observation.reason == "checkpoint_or_registration_unknown"


def test_replaced_registry_index_is_unknown(tmp_path, monkeypatch):
    _home, registry, _coordinator, factory = _runtime(tmp_path, monkeypatch)
    registry._running = {}

    observation = factory.observe_close(1)
    assert observation.kind == "unknown"
    assert observation.reason == "checkpoint_or_registration_unknown"


def test_refused_post_fence_spawn_does_not_create_a_child(tmp_path, monkeypatch):
    home, registry, _coordinator, factory = _runtime(tmp_path, monkeypatch)
    assert factory.observe_close(1).kind == "unknown"

    with pytest.raises(RuntimeError, match="I15 process source admission refused"):
        registry.spawn_local("printf refused", cwd=str(home), task_id="late")
    assert not registry._running
    assert not registry._finished


def test_pre_effect_foreground_release_removes_exact_pending_reservation(tmp_path, monkeypatch):
    home, registry, _coordinator, factory = _runtime(tmp_path, monkeypatch)
    custody = registry.reserve_foreground_custody(profile_id=str(home))
    assert custody is not None
    assert factory.has_foreground_reservation(custody) is True

    assert custody.release_unaccepted() is True
    assert factory.has_foreground_reservation(custody) is False
    observation = factory.observe_close(1)
    assert observation.kind == "unknown"


def test_real_held_child_is_nonempty_and_retained(tmp_path, monkeypatch):
    home, registry, _coordinator, factory = _runtime(tmp_path, monkeypatch)
    session = registry.spawn_local(
        _blocking_command(), cwd=str(home), task_id="held", session_key="held",
    )
    try:
        observation = factory.observe_close(1)
        assert observation.kind == "nonempty"
        assert observation.reason == "accepted_process_effect"
    finally:
        if session.process is not None and session.process.poll() is None:
            registry.kill_process(session.id, consume_output=False)


def test_foreground_reservation_transfers_to_exact_live_session(tmp_path, monkeypatch):
    home, registry, _coordinator, factory = _runtime(tmp_path, monkeypatch)
    custody = registry.reserve_foreground_custody(profile_id=str(home))
    process = subprocess.Popen(
        [sys.executable, "-c", "import threading; threading.Event().wait()"],
        cwd=str(home), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    session = None
    try:
        session = registry.adopt_local(
            process,
            command="foreground-test",
            cwd=str(home),
            task_id="foreground",
            session_key="foreground",
            notify_on_complete=False,
            custody=custody,
        )
        observation = factory.observe_close(1)
        assert observation.kind == "nonempty"
        assert observation.reason == "accepted_process_effect"
        assert session.id in observation.registrations[0].scope.close()["created_session_ids"]
    finally:
        if session is not None and process.poll() is None:
            registry.kill_process(session.id, consume_output=False)


def test_omitted_foreground_lease_keeps_exact_adopted_child_unknown(tmp_path, monkeypatch):
    home, registry, _coordinator, factory = _runtime(
        tmp_path, monkeypatch, reserve_owner=True,
    )
    process = subprocess.Popen(
        [sys.executable, "-c", "import time; print('late-child', flush=True); time.sleep(5)"],
        cwd=str(home), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    session = None
    try:
        session = registry.adopt_local(
            process,
            command="omitted-foreground-lease",
            cwd=str(home),
            task_id="omitted-lease",
            session_key="omitted-lease",
            notify_on_complete=False,
            custody=None,
        )
        registration = factory.registrations()[0]
        assert session.process is process
        assert session.pid == process.pid
        assert session.custody_state == "unknown"
        assert session.id in registry._custody_unbound_sessions
        assert registration.scope.contains_session(session.id) is False
        assert factory.observe_close(1).kind == "unknown"
    finally:
        if session is not None and process.poll() is None:
            registry.kill_process(session.id, consume_output=False)
        elif process.poll() is None:
            process.terminate()
            process.wait(timeout=5)
        assert process.poll() is not None
        assert process_ownership.release(home) is True
