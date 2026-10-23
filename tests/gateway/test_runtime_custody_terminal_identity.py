"""Terminal identity witnesses must not retain every completed custody object."""

import gc
import weakref
from dataclasses import replace

from gateway.runtime_custody import RuntimeCustodyCoordinator
from hermes_cli.backend_retirement import RetirementFence


def _coordinator():
    return RuntimeCustodyCoordinator(
        process_instance="terminal-identity-test", authority_epoch=1,
        fence=RetirementFence(),
    )


def test_terminal_turn_handle_is_collectible_after_its_source_releases_it():
    coordinator = _coordinator()
    handle = coordinator.reserve_root(
        "I02_api_session_turn", "completed-turn", profile_id="profile", authority_epoch=1,
    )
    assert coordinator.mark_terminal(handle) is None
    assert coordinator.is_terminal_handle(handle)
    reference = weakref.ref(handle)
    del handle
    gc.collect()
    assert reference() is None


def test_consumed_root_is_collectible_and_its_copy_cannot_rebind():
    coordinator = _coordinator()
    root = coordinator.reserve_root(
        "I17_hosted_tui_services", "hosted-root", profile_id="profile", authority_epoch=1,
    )
    receipt = coordinator.begin_close(owner_id="identity-close")
    assert coordinator.mark_terminal(root) is None
    assert coordinator.abort_before_handoff(receipt) is None
    successor = coordinator.rebind_terminal_root(
        root, "hosted-root-next", profile_id="profile", authority_epoch=1,
        owner_class="I17_hosted_tui_services",
    )
    assert coordinator.is_active_handle(successor)
    replay = coordinator.rebind_terminal_root(
        root, "hosted-root-replayed", profile_id="profile", authority_epoch=1,
        owner_class="I17_hosted_tui_services",
    )
    assert replay.code == "stale_terminal_root"
    copied = replace(root)
    reference = weakref.ref(root)
    del root
    gc.collect()
    assert reference() is None
    forged = coordinator.rebind_terminal_root(
        copied, "copied-root-replayed", profile_id="profile", authority_epoch=1,
        owner_class="I17_hosted_tui_services",
    )
    assert forged.code == "stale_terminal_root"
    assert coordinator.is_active_handle(successor)
