#!/usr/bin/env python3
"""AGC-1721 own-fork native Windows full-bank source-only transport guard.

This guard is copied into a single diagnostic transport commit whose parent is
an already-qualified candidate. It batches one ``git ls-files -z`` call, hashes
the materialized source/test/fixture byte closure, and requires the after
closure to equal the before closure. The manifest is deliberately excluded
from its own transport hash; all angle-bracket values must be replaced once by
the receiving owner before this file is committed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import NoReturn

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SHA1 = re.compile(r"^[0-9a-f]{40}$")
_MANIFEST_REL = '.github/proof/agc1721_native_windows_manifest.json'
_GUARD_REL = '.github/proof/agc1721_native_windows_source_guard.py'
_WORKFLOW_REL = '.github/workflows/windows-venv-e2e.yml'
_TRANSPORT_PATHS = {'.github/proof/agc1721_native_windows_manifest.json', '.github/proof/agc1721_native_windows_source_guard.py', '.github/workflows/windows-venv-e2e.yml'}
_EXPECTED_GROUPS = {
    "exporter_profile": [
    "tests/cron/test_complete_export.py",
    "tests/cron/test_complete_export_fences.py",
    "tests/cron/test_complete_export_sources.py",
    "tests/cron/test_complete_export_sqlite_refusals.py",
    "tests/cron/test_export_writer_custody.py",
    "tests/cron/test_file_writer_jobs_custody.py",
    "tests/cron/test_file_writer_monitor_custody.py",
    "tests/cron/test_file_writer_profile_custody.py",
    "tests/hermes_state/test_complete_export.py",
    "tests/hermes_state/test_complete_export_schema30_optional.py",
    "tests/hermes_state/test_preservation_receipt_budget_order.py",
    "tests/hermes_state/test_preservation_runtime_relation_shadow.py",
    "tests/hermes_state/test_preservation_runtime_snapshot.py",
    "tests/test_hermes_cron_custody_windows.py",
    "tests/test_hermes_cron_export_custody.py",
    "tests/tui_gateway/test_compute_host_borrowed_lease.py",
    "tests/tui_gateway/test_compute_host_late_compress_ack.py",
    "tests/tui_gateway/test_compute_host_profile_fencing.py",
    "tests/tui_gateway/test_compute_host_profile_ownership.py",
    "tests/tui_gateway/test_profiles_list_canonical_session.py",
    "tests/tui_gateway/test_tui_gateway_server.py"
],
    "windows_receiving_tests": [
    "tests/hermes_cli/test_ipv6_url_authorities.py",
    "tests/tools/test_voice_stop_phrase.py",
    "tests/tui_gateway/test_composite_carrier_rewind.py",
    "tests/tui_gateway/test_desktop_session_row_git_meta.py",
    "tests/tui_gateway/test_free_tier_task_done.py",
    "tests/tui_gateway/test_immediate_thread_shim.py",
    "tests/tui_gateway/test_lazy_session_regressions.py",
    "tests/tui_gateway/test_out_of_band_history_adoption.py",
    "tests/tui_gateway/test_profile_resume_cwd.py",
    "tests/tui_gateway/test_routing_provenance_reopen.py",
    "tests/tui_gateway/test_session_git_metadata_generation.py",
    "tests/tui_gateway/test_ssh_session_cwd_persistence.py",
    "tests/tui_gateway/test_tui_gateway_server.py",
    "tests/tui_gateway/test_tui_gateway_server_deep_truncate.py",
    "tests/tui_gateway/test_remote_workdir_path_semantics.py"
],
    "native_original": [
    "tests/tui_gateway/test_compute_host.py",
    "tests/tui_gateway/test_compute_host_turn_protocol.py",
    "tests/tui_gateway/test_interrupt_dead_compute_host.py",
    "tests/tui_gateway/test_served_profile_child_env.py",
    "tests/tui_gateway/test_served_profile_child_env_authority.py"
],
}
_EXPECTED_TESTS = [
  "tests/cron/test_complete_export.py",
  "tests/cron/test_complete_export_fences.py",
  "tests/cron/test_complete_export_sources.py",
  "tests/cron/test_complete_export_sqlite_refusals.py",
  "tests/cron/test_export_writer_custody.py",
  "tests/cron/test_file_writer_jobs_custody.py",
  "tests/cron/test_file_writer_monitor_custody.py",
  "tests/cron/test_file_writer_profile_custody.py",
  "tests/hermes_state/test_complete_export.py",
  "tests/hermes_state/test_complete_export_schema30_optional.py",
  "tests/hermes_state/test_preservation_receipt_budget_order.py",
  "tests/hermes_state/test_preservation_runtime_relation_shadow.py",
  "tests/hermes_state/test_preservation_runtime_snapshot.py",
  "tests/test_hermes_cron_custody_windows.py",
  "tests/test_hermes_cron_export_custody.py",
  "tests/tui_gateway/test_compute_host_borrowed_lease.py",
  "tests/tui_gateway/test_compute_host_late_compress_ack.py",
  "tests/tui_gateway/test_compute_host_profile_fencing.py",
  "tests/tui_gateway/test_compute_host_profile_ownership.py",
  "tests/tui_gateway/test_profiles_list_canonical_session.py",
  "tests/tui_gateway/test_tui_gateway_server.py",
  "tests/hermes_cli/test_ipv6_url_authorities.py",
  "tests/tools/test_voice_stop_phrase.py",
  "tests/tui_gateway/test_composite_carrier_rewind.py",
  "tests/tui_gateway/test_desktop_session_row_git_meta.py",
  "tests/tui_gateway/test_free_tier_task_done.py",
  "tests/tui_gateway/test_immediate_thread_shim.py",
  "tests/tui_gateway/test_lazy_session_regressions.py",
  "tests/tui_gateway/test_out_of_band_history_adoption.py",
  "tests/tui_gateway/test_profile_resume_cwd.py",
  "tests/tui_gateway/test_routing_provenance_reopen.py",
  "tests/tui_gateway/test_session_git_metadata_generation.py",
  "tests/tui_gateway/test_ssh_session_cwd_persistence.py",
  "tests/tui_gateway/test_tui_gateway_server_deep_truncate.py",
  "tests/tui_gateway/test_remote_workdir_path_semantics.py",
  "tests/tui_gateway/test_compute_host.py",
  "tests/tui_gateway/test_compute_host_turn_protocol.py",
  "tests/tui_gateway/test_interrupt_dead_compute_host.py",
  "tests/tui_gateway/test_served_profile_child_env.py",
  "tests/tui_gateway/test_served_profile_child_env_authority.py"
]
_TARGET_TESTS = _EXPECTED_TESTS
_TARGET_CASE = "test_session_resume_deferred_history_acknowledges_and_reuses"
_EXECUTION_SCOPE = "full-file-context-per-file"
_EXPECTED_FULL_CASE_COUNT = 1021
_EXPECTED_FULL_SKIP_COUNT = 9
_EXPECTED_JUNIT_CASES_SHA256 = "7795899cae829346a158066f9e71dfa50d71ef59a6836de523260366fce18f2a"
_EXPECTED_JUNIT_CASES_SOURCE_SHA256 = "e6ffef6dabd165262360f04d9d6904429ee6792ea8d2743e3b3563e54447800c"
_EXPECTED_SOURCE_FILE_COUNT = 10853
_EXPECTED_TEST_FILE_COUNT = 40
_EXPECTED_FIXTURE_FILE_COUNT = 0
_EXPECTED_BYTE_CLOSURE_FILE_COUNT = 10893

_EXPECTED_LINUX_TESTS = [
    "tests/cron/test_agent_scheduling_gate.py",
    "tests/cron/test_atomic_paused_creation.py",
    "tests/cron/test_blueprint_catalog.py",
    "tests/cron/test_bot_chat_delivery_no_owner.py",
    "tests/cron/test_bot_chat_legacy_isolation.py",
    "tests/cron/test_canonical_completion_retention.py",
    "tests/cron/test_canonical_cron.py",
    "tests/cron/test_canonical_firer_give_up.py",
    "tests/cron/test_canonical_receipt_bookkeeping.py",
    "tests/cron/test_catchup_policy.py",
    "tests/cron/test_claim_job_for_fire.py",
    "tests/cron/test_cleanup_timeout.py",
    "tests/cron/test_codex_execution_paths.py",
    "tests/cron/test_complete_export.py",
    "tests/cron/test_complete_export_fences.py",
    "tests/cron/test_complete_export_sources.py",
    "tests/cron/test_complete_export_sqlite_refusals.py",
    "tests/cron/test_compute_next_run_dst.py",
    "tests/cron/test_compute_next_run_last_run_at.py",
    "tests/cron/test_context_from_response_extraction.py",
    "tests/cron/test_continuity_silent_audits.py",
    "tests/cron/test_cron_bot_chat_create_if_missing.py",
    "tests/cron/test_cron_bot_chat_delivery.py",
    "tests/cron/test_cron_context_from.py",
    "tests/cron/test_cron_created_delivery.py",
    "tests/cron/test_cron_delivery_redaction.py",
    "tests/cron/test_cron_direct_api_call.py",
    "tests/cron/test_cron_direct_api_call_watchdog.py",
    "tests/cron/test_cron_emfile_stall.py",
    "tests/cron/test_cron_empty_payload.py",
    "tests/cron/test_cron_empty_turn_stays_silent.py",
    "tests/cron/test_cron_execution_identity.py",
    "tests/cron/test_cron_explicit_empty_toolsets.py",
    "tests/cron/test_cron_failure_deliver.py",
    "tests/cron/test_cron_failure_notice_copy.py",
    "tests/cron/test_cron_failure_summarizer_inactivity.py",
    "tests/cron/test_cron_home_target_profile_scope.py",
    "tests/cron/test_cron_import_error_skew_hint.py",
    "tests/cron/test_cron_inactivity_timeout.py",
    "tests/cron/test_cron_incidents.py",
    "tests/cron/test_cron_incidents_resolve.py",
    "tests/cron/test_cron_journal_terminal_exits.py",
    "tests/cron/test_cron_kanban_env_isolation.py",
    "tests/cron/test_cron_live_bot_delivery.py",
    "tests/cron/test_cron_live_delivery_confirmation.py",
    "tests/cron/test_cron_manage_profile_scope.py",
    "tests/cron/test_cron_mcp_toolset_empty_block.py",
    "tests/cron/test_cron_memory_contract.py",
    "tests/cron/test_cron_multiplex_desktop_ticker_scope.py",
    "tests/cron/test_cron_multiplex_inflight_release.py",
    "tests/cron/test_cron_multiplex_liveness_and_pools.py",
    "tests/cron/test_cron_multiplex_profile_route_preflight.py",
    "tests/cron/test_cron_multiplex_served_profile_parity.py",
    "tests/cron/test_cron_multiplex_shared_route_delivery.py",
    "tests/cron/test_cron_multiplex_tick_ownership.py",
    "tests/cron/test_cron_next_run_profile_timezone.py",
    "tests/cron/test_cron_no_agent.py",
    "tests/cron/test_cron_non_push_origin.py",
    "tests/cron/test_cron_origin_synthetic_thread.py",
    "tests/cron/test_cron_pinned_job_fallback.py",
    "tests/cron/test_cron_pre_agent_fallback_notice.py",
    "tests/cron/test_cron_profile_isolation.py",
    "tests/cron/test_cron_prompt_injection_skill.py",
    "tests/cron/test_cron_provider_pin.py",
    "tests/cron/test_cron_reasoning_effort.py",
    "tests/cron/test_cron_reconnect_only_rejection.py",
    "tests/cron/test_cron_recover_after_commit.py",
    "tests/cron/test_cron_recover_pause.py",
    "tests/cron/test_cron_recovery.py",
    "tests/cron/test_cron_relay_delivery_guards.py",
    "tests/cron/test_cron_relay_dm_origin_user_id.py",
    "tests/cron/test_cron_relay_run_forward.py",
    "tests/cron/test_cron_run_metrics.py",
    "tests/cron/test_cron_run_stale_claim_reap.py",
    "tests/cron/test_cron_script.py",
    "tests/cron/test_cron_session_workdir.py",
    "tests/cron/test_cron_thread_seed_dm_keying.py",
    "tests/cron/test_cron_tick_stale_yield.py",
    "tests/cron/test_cron_ticker_drift.py",
    "tests/cron/test_cron_timezone_migration_catchup.py",
    "tests/cron/test_cron_windows_venv_abi.py",
    "tests/cron/test_cron_workdir.py",
    "tests/cron/test_cronjob_gateway_liveness.py",
    "tests/cron/test_dead_owner_claim_reclaim.py",
    "tests/cron/test_delivery_outcome_writeback.py",
    "tests/cron/test_delivery_queue.py",
    "tests/cron/test_delivery_queue_retention_race.py",
    "tests/cron/test_dispatch_lateness_stamp.py",
    "tests/cron/test_due_scan_dst_fold.py",
    "tests/cron/test_due_scan_save_failure.py",
    "tests/cron/test_due_stale_cron_edit.py",
    "tests/cron/test_duration_dst.py",
    "tests/cron/test_estop.py",
    "tests/cron/test_execution_ledger.py",
    "tests/cron/test_execution_progress_stamp.py",
    "tests/cron/test_export_writer_custody.py",
    "tests/cron/test_external_worker_config_hooks.py",
    "tests/cron/test_external_worker_secret_sources.py",
    "tests/cron/test_file_permissions.py",
    "tests/cron/test_file_writer_jobs_custody.py",
    "tests/cron/test_file_writer_monitor_custody.py",
    "tests/cron/test_file_writer_profile_custody.py",
    "tests/cron/test_fire_claim_lost_after_delivery.py",
    "tests/cron/test_fire_forward_failure_stamp.py",
    "tests/cron/test_idle_tick_config_skip.py",
    "tests/cron/test_inflight_stale_guard.py",
    "tests/cron/test_jobs.py",
    "tests/cron/test_jobs_changed_notify.py",
    "tests/cron/test_jobs_crossprocess_lock.py",
    "tests/cron/test_jobs_file_ownership.py",
    "tests/cron/test_jobs_shrink_merge.py",
    "tests/cron/test_last_failure_stamp.py",
    "tests/cron/test_lifecycle_guard_budget.py",
    "tests/cron/test_lifecycle_guard_heredoc_walk.py",
    "tests/cron/test_lifecycle_guard_live_db.py",
    "tests/cron/test_lifecycle_guard_profile_scan.py",
    "tests/cron/test_live_firer_receipt_recovery.py",
    "tests/cron/test_media_delivery_parity.py",
    "tests/cron/test_media_send_timeout.py",
    "tests/cron/test_mirror_origin_fallback.py",
    "tests/cron/test_misfire_backstop_oneshot_grace.py",
    "tests/cron/test_misfire_catchup.py",
    "tests/cron/test_missed_window_catchup.py",
    "tests/cron/test_monitor_kind.py",
    "tests/cron/test_notepad.py",
    "tests/cron/test_oneshot_dispatch_failure_run_claim.py",
    "tests/cron/test_oneshot_grace_due_scan.py",
    "tests/cron/test_oneshot_guard_warning.py",
    "tests/cron/test_owner_start_time_drift.py",
    "tests/cron/test_parallel_pool.py",
    "tests/cron/test_persisted_error_rearm_legality.py",
    "tests/cron/test_preflight_config.py",
    "tests/cron/test_preflight_credential_verdict_names_home.py",
    "tests/cron/test_preflight_managed_routes.py",
    "tests/cron/test_queued_delivery_fencing.py",
    "tests/cron/test_quota_hold.py",
    "tests/cron/test_recurring_persisted_error_recovery.py",
    "tests/cron/test_recurring_wedge_selfheal.py",
    "tests/cron/test_registration_ownership.py",
    "tests/cron/test_relay_fronted_delivery.py",
    "tests/cron/test_restart_safe_worker.py",
    "tests/cron/test_restart_safe_worker_death_evidence.py",
    "tests/cron/test_retirement_dispatch.py",
    "tests/cron/test_rewrite_skill_refs.py",
    "tests/cron/test_run_error_diagnostics.py",
    "tests/cron/test_run_one_job.py",
    "tests/cron/test_scheduled_occurrence.py",
    "tests/cron/test_scheduler.py",
    "tests/cron/test_scheduler_completion_verification.py",
    "tests/cron/test_scheduler_cron_session_isolation.py",
    "tests/cron/test_scheduler_provider.py",
    "tests/cron/test_scheduler_session_title_nudge.py",
    "tests/cron/test_scheduler_shutdown_guard.py",
    "tests/cron/test_script_claim_heartbeat.py",
    "tests/cron/test_sessiondb_init_hang.py",
    "tests/cron/test_shutdown_interrupt.py",
    "tests/cron/test_stale_module_leaf_imports.py",
    "tests/cron/test_stale_running_recovery.py",
    "tests/cron/test_suggestions.py",
    "tests/cron/test_terminal_job_rearm.py",
    "tests/cron/test_tick_without_gateway.py",
    "tests/cron/test_ticker_stall.py",
    "tests/cron/test_ticker_startup_survival.py",
    "tests/cron/test_timezone.py",
    "tests/cron/test_unreachable_retry.py",
    "tests/cron/test_upgrade_module_skew.py",
    "tests/cron/test_usage_audit_logger.py",
    "tests/cron/test_warning_delivery_policy.py",
    "tests/cron/test_warning_execution_outcome.py",
    "tests/cron/test_warning_transport_contract.py",
    "tests/cron/test_worktree_maintenance_isolation.py",
    "tests/hermes_cli/test_ipv6_url_authorities.py",
    "tests/hermes_state/test_append_message_after_close.py",
    "tests/hermes_state/test_append_messages_batch.py",
    "tests/hermes_state/test_auto_archived_lineage_reactivation.py",
    "tests/hermes_state/test_auto_vacuum_holder_gate.py",
    "tests/hermes_state/test_auto_vacuum_in_process_holder_gate.py",
    "tests/hermes_state/test_automatic_ended_stamp.py",
    "tests/hermes_state/test_aux_usage_accounting.py",
    "tests/hermes_state/test_background_review_session_isolation.py",
    "tests/hermes_state/test_bounded_recent_sessions.py",
    "tests/hermes_state/test_canonical_title_guard.py",
    "tests/hermes_state/test_clean_close_residual_poison.py",
    "tests/hermes_state/test_compacted_message_reactions.py",
    "tests/hermes_state/test_complete_export.py",
    "tests/hermes_state/test_complete_export_schema30_optional.py",
    "tests/hermes_state/test_composite_carrier_rewind.py",
    "tests/hermes_state/test_compression_lease_refresh_before_publish.py",
    "tests/hermes_state/test_compression_lineage_guard.py",
    "tests/hermes_state/test_compression_watermark_commit.py",
    "tests/hermes_state/test_conftest_wal_gate.py",
    "tests/hermes_state/test_conversation_root.py",
    "tests/hermes_state/test_corrupt_row_robustness.py",
    "tests/hermes_state/test_cron_run_sessions.py",
    "tests/hermes_state/test_cross_vm_fs_wal_refusal.py",
    "tests/hermes_state/test_dedupe_migration_contention.py",
    "tests/hermes_state/test_delegate_cascade.py",
    "tests/hermes_state/test_delegate_child_routing_inheritance.py",
    "tests/hermes_state/test_delete_branched_root_leftovers.py",
    "tests/hermes_state/test_delete_compression_chain.py",
    "tests/hermes_state/test_delete_mode_read_busy_wait.py",
    "tests/hermes_state/test_delete_session_write_guards.py",
    "tests/hermes_state/test_deleted_wal_checkpoint_guard.py",
    "tests/hermes_state/test_deleted_wal_generation_guard.py",
    "tests/hermes_state/test_disk_full_error.py",
    "tests/hermes_state/test_display_dedupe_pruned_tool_payload.py",
    "tests/hermes_state/test_display_projection_parity.py",
    "tests/hermes_state/test_display_split_heal.py",
    "tests/hermes_state/test_downgrade_delete_guard.py",
    "tests/hermes_state/test_empty_sweep_archived_transcript.py",
    "tests/hermes_state/test_fts_cjk_bigram.py",
    "tests/hermes_state/test_fts_fresh_bootstrap_admission.py",
    "tests/hermes_state/test_fts_holder_instance_scope.py",
    "tests/hermes_state/test_fts_index_fail_open.py",
    "tests/hermes_state/test_fts_orphan_shadow_repair.py",
    "tests/hermes_state/test_fts_projection_integrity.py",
    "tests/hermes_state/test_fts_rebuild_admission.py",
    "tests/hermes_state/test_fts_rebuild_corruption_class.py",
    "tests/hermes_state/test_fts_runtime_rebuild.py",
    "tests/hermes_state/test_fts_tool_write_bounds.py",
    "tests/hermes_state/test_fts_trigram_cron_exclusion.py",
    "tests/hermes_state/test_fts_trigram_subagent_exclusion.py",
    "tests/hermes_state/test_fts_update_of_narrowing.py",
    "tests/hermes_state/test_get_anchored_view.py",
    "tests/hermes_state/test_get_messages_around.py",
    "tests/hermes_state/test_get_messages_include_compacted.py",
    "tests/hermes_state/test_guest_durability_barriers.py",
    "tests/hermes_state/test_hermes_state.py",
    "tests/hermes_state/test_hermes_state_archive_open.py",
    "tests/hermes_state/test_hermes_state_compression_busy_retry.py",
    "tests/hermes_state/test_hermes_state_compression_locks.py",
    "tests/hermes_state/test_hermes_state_ids.py",
    "tests/hermes_state/test_hermes_state_readonly_preflight.py",
    "tests/hermes_state/test_hermes_state_ux_messages.py",
    "tests/hermes_state/test_hermes_state_wal_fallback.py",
    "tests/hermes_state/test_history_login_label.py",
    "tests/hermes_state/test_import_deleted_session.py",
    "tests/hermes_state/test_isolation_marker_env.py",
    "tests/hermes_state/test_journal_mode_config.py",
    "tests/hermes_state/test_journal_mode_upgrade_warning.py",
    "tests/hermes_state/test_lease_pid_namespace.py",
    "tests/hermes_state/test_ledger_delete_consistency.py",
    "tests/hermes_state/test_live_db_guard_ancestry.py",
    "tests/hermes_state/test_live_db_isolation_guard.py",
    "tests/hermes_state/test_lockguard_fcntl_tolerance.py",
    "tests/hermes_state/test_message_uid.py",
    "tests/hermes_state/test_named_profile_session_db.py",
    "tests/hermes_state/test_never_active_keyed_prune.py",
    "tests/hermes_state/test_no_locked_readers_gate.py",
    "tests/hermes_state/test_no_more_rows_retry.py",
    "tests/hermes_state/test_orphan_gateway_session_repair.py",
    "tests/hermes_state/test_orphan_sweep_cross_backend.py",
    "tests/hermes_state/test_pinned_archived_sidebar.py",
    "tests/hermes_state/test_pinned_lineage.py",
    "tests/hermes_state/test_preservation_receipt_budget_order.py",
    "tests/hermes_state/test_preservation_runtime_relation_shadow.py",
    "tests/hermes_state/test_preservation_runtime_snapshot.py",
    "tests/hermes_state/test_prune_activity_recency.py",
    "tests/hermes_state/test_purge_profile_state.py",
    "tests/hermes_state/test_read_path_transient_ioerr.py",
    "tests/hermes_state/test_readonly_fts_decode_error.py",
    "tests/hermes_state/test_reasoning_roundtrip.py",
    "tests/hermes_state/test_rebuild_table_atomic.py",
    "tests/hermes_state/test_recent_model_route.py",
    "tests/hermes_state/test_rekey_profile_state.py",
    "tests/hermes_state/test_remove_session_files_glob_escape.py",
    "tests/hermes_state/test_repaired_history_commit_coverage.py",
    "tests/hermes_state/test_replace_messages_archive_siblings.py",
    "tests/hermes_state/test_replace_messages_archives_suffix_only.py",
    "tests/hermes_state/test_reset_lineage_delete_queued_work.py",
    "tests/hermes_state/test_reset_owner_housekeeping.py",
    "tests/hermes_state/test_resolve_resume_session_id.py",
    "tests/hermes_state/test_restore_alternation_repair.py",
    "tests/hermes_state/test_retired_fence_outlives_horizon.py",
    "tests/hermes_state/test_retired_wal_generation_capture.py",
    "tests/hermes_state/test_rewind_surfaces_invariant.py",
    "tests/hermes_state/test_runtime_admission.py",
    "tests/hermes_state/test_runtime_adoption.py",
    "tests/hermes_state/test_runtime_destructive_mutations.py",
    "tests/hermes_state/test_runtime_import_mutations.py",
    "tests/hermes_state/test_runtime_mutation_actions.py",
    "tests/hermes_state/test_runtime_mutation_retired_import.py",
    "tests/hermes_state/test_runtime_mutations.py",
    "tests/hermes_state/test_runtime_recovery.py",
    "tests/hermes_state/test_runtime_rewind_carrier_handoff.py",
    "tests/hermes_state/test_runtime_rewind_mutations.py",
    "tests/hermes_state/test_runtime_schema.py",
    "tests/hermes_state/test_runtime_worker.py",
    "tests/hermes_state/test_runtime_worker_compaction.py",
    "tests/hermes_state/test_runtime_worker_compression.py",
    "tests/hermes_state/test_runtime_worker_compression_admission.py",
    "tests/hermes_state/test_runtime_worker_compression_annotations.py",
    "tests/hermes_state/test_runtime_worker_compression_cleanup.py",
    "tests/hermes_state/test_runtime_worker_compression_flush.py",
    "tests/hermes_state/test_runtime_worker_context.py",
    "tests/hermes_state/test_runtime_worker_history.py",
    "tests/hermes_state/test_runtime_worker_lifecycle.py",
    "tests/hermes_state/test_runtime_worker_metadata.py",
    "tests/hermes_state/test_runtime_worker_operations.py",
    "tests/hermes_state/test_runtime_worker_read_fence.py",
    "tests/hermes_state/test_runtime_worker_rotation_automation.py",
    "tests/hermes_state/test_runtime_worker_title_retry.py",
    "tests/hermes_state/test_runtime_worker_turn_cleanup.py",
    "tests/hermes_state/test_runtime_worker_usage.py",
    "tests/hermes_state/test_schema_read_probe.py",
    "tests/hermes_state/test_search_or_relaxed_fallback.py",
    "tests/hermes_state/test_session_archived_round_trip.py",
    "tests/hermes_state/test_session_archiving.py",
    "tests/hermes_state/test_session_db_acp_cwd_backfill.py",
    "tests/hermes_state/test_session_db_context_manager.py",
    "tests/hermes_state/test_session_db_leak_sweep.py",
    "tests/hermes_state/test_session_db_profile_stamp.py",
    "tests/hermes_state/test_session_db_read_conn_pool.py",
    "tests/hermes_state/test_session_db_read_path_split.py",
    "tests/hermes_state/test_session_delete_hook.py",
    "tests/hermes_state/test_session_delete_profile_isolation.py",
    "tests/hermes_state/test_session_export_timings.py",
    "tests/hermes_state/test_session_git_metadata_generation.py",
    "tests/hermes_state/test_session_hidden.py",
    "tests/hermes_state/test_session_id_recognition_recovery.py",
    "tests/hermes_state/test_session_import_keep_flags.py",
    "tests/hermes_state/test_session_import_value_validation.py",
    "tests/hermes_state/test_session_lifecycle_status.py",
    "tests/hermes_state/test_session_list_index_choice.py",
    "tests/hermes_state/test_session_md_export.py",
    "tests/hermes_state/test_session_model_usage_pk_heal.py",
    "tests/hermes_state/test_session_read_state.py",
    "tests/hermes_state/test_session_turn_lease.py",
    "tests/hermes_state/test_session_workspace_binding.py",
    "tests/hermes_state/test_shared_session_db_registry.py",
    "tests/hermes_state/test_show_subagents_listing.py",
    "tests/hermes_state/test_sqlite_lock_safe_inspection.py",
    "tests/hermes_state/test_sqlite_wal_reset_gate.py",
    "tests/hermes_state/test_stale_tool_call_marker_session_repair.py",
    "tests/hermes_state/test_startup_maintenance_lease.py",
    "tests/hermes_state/test_state_db_corrupt_quarantine.py",
    "tests/hermes_state/test_state_db_darwin_fd_scan_hang.py",
    "tests/hermes_state/test_state_db_file_identity.py",
    "tests/hermes_state/test_state_db_fts_segment_collision_probe.py",
    "tests/hermes_state/test_state_db_holders.py",
    "tests/hermes_state/test_state_db_holders_windows_live.py",
    "tests/hermes_state/test_state_db_lock_fail_closed.py",
    "tests/hermes_state/test_state_db_maintenance_conformance.py",
    "tests/hermes_state/test_state_db_malformed_repair.py",
    "tests/hermes_state/test_state_db_notadb_fail_closed.py",
    "tests/hermes_state/test_state_db_repair_live_writer_guard.py",
    "tests/hermes_state/test_state_db_repair_loop_cap.py",
    "tests/hermes_state/test_state_db_repair_loop_mtime.py",
    "tests/hermes_state/test_state_db_repair_non_destructive.py",
    "tests/hermes_state/test_state_db_second_process_maintenance.py",
    "tests/hermes_state/test_state_db_stats.py",
    "tests/hermes_state/test_state_db_wal_unlink_race.py",
    "tests/hermes_state/test_state_db_write_durability.py",
    "tests/hermes_state/test_state_synchronous_pragma.py",
    "tests/hermes_state/test_storage_failure_copy.py",
    "tests/hermes_state/test_storage_health_latch.py",
    "tests/hermes_state/test_storage_import_boundary.py",
    "tests/hermes_state/test_sweep_orphaned_sessions.py",
    "tests/hermes_state/test_tail_rewind_semantics.py",
    "tests/hermes_state/test_target_advance_fence.py",
    "tests/hermes_state/test_telegram_topic_selfheal.py",
    "tests/hermes_state/test_terminal_retirement.py",
    "tests/hermes_state/test_unaddressed_rewrite_identity.py",
    "tests/hermes_state/test_wal_active_confirmed.py",
    "tests/hermes_state/test_wal_checkpoint_strategy.py",
    "tests/hermes_state/test_wal_lock_guard.py",
    "tests/hermes_state/test_wal_lock_guard_lifecycle.py",
    "tests/hermes_state/test_watermark_interplay.py",
    "tests/hermes_state/test_worker_lineage_exclusion.py",
    "tests/hermes_state/test_worker_lineage_index.py",
    "tests/hermes_state/test_write_lock_owner_attribution.py",
    "tests/hermes_state/test_write_lock_patience.py",
    "tests/hermes_state/test_writer_conn_thread_safety.py",
    "tests/hermes_state/test_zeroed_db_quarantine_race.py",
    "tests/hermes_state/test_zeroed_state_db.py",
    "tests/test_hermes_cron_custody_windows.py",
    "tests/test_hermes_cron_export_custody.py",
    "tests/tools/test_voice_stop_phrase.py",
    "tests/tui_gateway/contracts/test_generated.py",
    "tests/tui_gateway/contracts/test_profile_rename_history.py",
    "tests/tui_gateway/test_adversarial_sidecar_availability.py",
    "tests/tui_gateway/test_approval_cancelled_on_reap.py",
    "tests/tui_gateway/test_attach_does_not_wait_for_agent.py",
    "tests/tui_gateway/test_auto_continue.py",
    "tests/tui_gateway/test_backend_dir_entries_preconfirmed.py",
    "tests/tui_gateway/test_backend_heartbeat.py",
    "tests/tui_gateway/test_background_thread_profile_scope.py",
    "tests/tui_gateway/test_billing_rpc.py",
    "tests/tui_gateway/test_bot_live_owner_delivery.py",
    "tests/tui_gateway/test_bot_mode_silence_delivery.py",
    "tests/tui_gateway/test_bot_relay_methods.py",
    "tests/tui_gateway/test_branch_system_prompt.py",
    "tests/tui_gateway/test_busy_compression_demotion.py",
    "tests/tui_gateway/test_canonical_voice_events.py",
    "tests/tui_gateway/test_change_watcher.py",
    "tests/tui_gateway/test_change_watcher_sessions.py",
    "tests/tui_gateway/test_checkpoints_config_fallback.py",
    "tests/tui_gateway/test_closed_session_turn_lease.py",
    "tests/tui_gateway/test_codex_app_server_live_events.py",
    "tests/tui_gateway/test_cold_start_gil_stall.py",
    "tests/tui_gateway/test_command_catalog_plugin_discovery.py",
    "tests/tui_gateway/test_command_description_fidelity.py",
    "tests/tui_gateway/test_compaction_status.py",
    "tests/tui_gateway/test_completion_cwd_profile_fallback.py",
    "tests/tui_gateway/test_composite_carrier_rewind.py",
    "tests/tui_gateway/test_compress_lock_skip.py",
    "tests/tui_gateway/test_compress_session_cwd.py",
    "tests/tui_gateway/test_compression_config_hot_reload.py",
    "tests/tui_gateway/test_compute_host.py",
    "tests/tui_gateway/test_compute_host_borrowed_lease.py",
    "tests/tui_gateway/test_compute_host_late_compress_ack.py",
    "tests/tui_gateway/test_compute_host_pending_model_switch.py",
    "tests/tui_gateway/test_compute_host_phase1.py",
    "tests/tui_gateway/test_compute_host_profile_fencing.py",
    "tests/tui_gateway/test_compute_host_profile_ownership.py",
    "tests/tui_gateway/test_compute_host_routed_secret_hydration.py",
    "tests/tui_gateway/test_compute_host_turn_protocol.py",
    "tests/tui_gateway/test_config_profile_scope.py",
    "tests/tui_gateway/test_config_set_display_toggles.py",
    "tests/tui_gateway/test_config_set_session_profile_scope.py",
    "tests/tui_gateway/test_config_set_voice_chat_mode.py",
    "tests/tui_gateway/test_connector_account_surface.py",
    "tests/tui_gateway/test_connector_accounts_rpc.py",
    "tests/tui_gateway/test_connector_operation_e2e.py",
    "tests/tui_gateway/test_connector_operation_rpc.py",
    "tests/tui_gateway/test_container_backend_workdir.py",
    "tests/tui_gateway/test_context_local_slash.py",
    "tests/tui_gateway/test_credit_producer_suppression.py",
    "tests/tui_gateway/test_cross_process_orphan_ownership.py",
    "tests/tui_gateway/test_custom_provider_session_persistence.py",
    "tests/tui_gateway/test_default_profile_session_name.py",
    "tests/tui_gateway/test_deferred_lease_reaper.py",
    "tests/tui_gateway/test_deferred_model_history.py",
    "tests/tui_gateway/test_deferred_model_switch_confirm.py",
    "tests/tui_gateway/test_delegation_session_lifecycle.py",
    "tests/tui_gateway/test_desktop_session_login_identity.py",
    "tests/tui_gateway/test_desktop_session_row_git_meta.py",
    "tests/tui_gateway/test_desktop_session_workspace_persistence.py",
    "tests/tui_gateway/test_detached_loop_keepalive.py",
    "tests/tui_gateway/test_diagnostic_notification_presentation.py",
    "tests/tui_gateway/test_diagnostic_turn_end_to_end.py",
    "tests/tui_gateway/test_diagnostics_share_nous.py",
    "tests/tui_gateway/test_display_methods.py",
    "tests/tui_gateway/test_display_watch.py",
    "tests/tui_gateway/test_eager_multiplex_activation.py",
    "tests/tui_gateway/test_empty_fast_status.py",
    "tests/tui_gateway/test_entry_home_ownership.py",
    "tests/tui_gateway/test_entry_import_off_main_thread.py",
    "tests/tui_gateway/test_entry_inline_handler_error.py",
    "tests/tui_gateway/test_entry_picker_prewarm.py",
    "tests/tui_gateway/test_entry_rpc_stdin_cloexec.py",
    "tests/tui_gateway/test_entry_sys_path.py",
    "tests/tui_gateway/test_ephemeral_profile_override.py",
    "tests/tui_gateway/test_external_process_picker.py",
    "tests/tui_gateway/test_failed_agent_build_retry.py",
    "tests/tui_gateway/test_failed_turn_retention.py",
    "tests/tui_gateway/test_fallback_chain_hot_reload.py",
    "tests/tui_gateway/test_fanout_burst_batching.py",
    "tests/tui_gateway/test_fanout_send_membership.py",
    "tests/tui_gateway/test_fast_session_scope.py",
    "tests/tui_gateway/test_file_attachment_allocation.py",
    "tests/tui_gateway/test_finalize_session_cancels_background_review.py",
    "tests/tui_gateway/test_finalize_session_persist.py",
    "tests/tui_gateway/test_first_contact_onboarding.py",
    "tests/tui_gateway/test_foreground_notification_snapshot.py",
    "tests/tui_gateway/test_free_tier_rpc.py",
    "tests/tui_gateway/test_free_tier_task_done.py",
    "tests/tui_gateway/test_gateway_bootstrap_control_home.py",
    "tests/tui_gateway/test_gateway_bootstrap_recover.py",
    "tests/tui_gateway/test_gateway_owned_session_reap.py",
    "tests/tui_gateway/test_goal_command.py",
    "tests/tui_gateway/test_groups_methods.py",
    "tests/tui_gateway/test_groups_replication_methods.py",
    "tests/tui_gateway/test_gui_surface_toolsets.py",
    "tests/tui_gateway/test_heartbeat_tui_tick.py",
    "tests/tui_gateway/test_heartbeat_wakes.py",
    "tests/tui_gateway/test_history_identity.py",
    "tests/tui_gateway/test_history_inline_images.py",
    "tests/tui_gateway/test_hosted_file_publication.py",
    "tests/tui_gateway/test_hosted_room_attachment_routes.py",
    "tests/tui_gateway/test_hosted_room_driver_runtime.py",
    "tests/tui_gateway/test_hosted_room_frozen_publication.py",
    "tests/tui_gateway/test_hosted_room_member_activity_hook.py",
    "tests/tui_gateway/test_hosted_room_member_submit.py",
    "tests/tui_gateway/test_hosted_room_peer_http.py",
    "tests/tui_gateway/test_hosted_room_peer_transport.py",
    "tests/tui_gateway/test_hosted_room_policy_upgrade.py",
    "tests/tui_gateway/test_hosted_room_prompt_fence.py",
    "tests/tui_gateway/test_hosted_room_server_rpc.py",
    "tests/tui_gateway/test_hosted_room_service.py",
    "tests/tui_gateway/test_hosted_room_settled_retry.py",
    "tests/tui_gateway/test_hosted_room_turn_slot_release.py",
    "tests/tui_gateway/test_hosted_room_two_gateway_scoped.py",
    "tests/tui_gateway/test_hud_surface_note.py",
    "tests/tui_gateway/test_human_input_hooks_tui.py",
    "tests/tui_gateway/test_i18n_methods.py",
    "tests/tui_gateway/test_image_history_display.py",
    "tests/tui_gateway/test_image_ref_message.py",
    "tests/tui_gateway/test_image_routing_stale_model.py",
    "tests/tui_gateway/test_immediate_thread_shim.py",
    "tests/tui_gateway/test_init_targets_session_cwd.py",
    "tests/tui_gateway/test_ink_helper_dependencies.py",
    "tests/tui_gateway/test_inline_rpc_gil_starvation.py",
    "tests/tui_gateway/test_interim_assistant_callback.py",
    "tests/tui_gateway/test_interrupt_agent_loop_stopped_hook.py",
    "tests/tui_gateway/test_interrupt_dead_compute_host.py",
    "tests/tui_gateway/test_interrupt_does_not_wait_for_agent.py",
    "tests/tui_gateway/test_iso_certify_seam.py",
    "tests/tui_gateway/test_isolated_orphan_activity.py",
    "tests/tui_gateway/test_kanban_notify_poller.py",
    "tests/tui_gateway/test_kanban_resume_guard.py",
    "tests/tui_gateway/test_launch_db_home_override_race.py",
    "tests/tui_gateway/test_launch_home_isolation.py",
    "tests/tui_gateway/test_launch_profile_scope_plugin_hooks.py",
    "tests/tui_gateway/test_lazy_session_regressions.py",
    "tests/tui_gateway/test_legacy_fallback_profile_scope.py",
    "tests/tui_gateway/test_legacy_fallback_ticket_profile_verbs.py",
    "tests/tui_gateway/test_legacy_paste_profile_home.py",
    "tests/tui_gateway/test_legacy_turn_canonical_barrier.py",
    "tests/tui_gateway/test_live_config_context_pin_mirror.py",
    "tests/tui_gateway/test_local_model_session_identity.py",
    "tests/tui_gateway/test_loop_command.py",
    "tests/tui_gateway/test_machine_facts_rpc.py",
    "tests/tui_gateway/test_main_merge_projections.py",
    "tests/tui_gateway/test_make_agent_personality_prompt.py",
    "tests/tui_gateway/test_make_agent_prefill.py",
    "tests/tui_gateway/test_make_agent_provider.py",
    "tests/tui_gateway/test_manual_compress_history_fence.py",
    "tests/tui_gateway/test_mcp_late_refresh_thread_owner.py",
    "tests/tui_gateway/test_mcp_oauth_cancel.py",
    "tests/tui_gateway/test_mcp_oauth_client_callback.py",
    "tests/tui_gateway/test_mcp_profile_rpcs.py",
    "tests/tui_gateway/test_mcp_reload_all_sessions.py",
    "tests/tui_gateway/test_mcp_reload_rev.py",
    "tests/tui_gateway/test_moa_reference_emit.py",
    "tests/tui_gateway/test_model_options_profile_scope.py",
    "tests/tui_gateway/test_model_rpc_skew_guard.py",
    "tests/tui_gateway/test_model_switch_marker_role.py",
    "tests/tui_gateway/test_model_switch_reasoning_flag.py",
    "tests/tui_gateway/test_multi_client_fanout.py",
    "tests/tui_gateway/test_multi_profile_hosting_fail_closed.py",
    "tests/tui_gateway/test_multi_profile_hosting_transitions.py",
    "tests/tui_gateway/test_none_agent_turn_guard.py",
    "tests/tui_gateway/test_notification_admission_refusal.py",
    "tests/tui_gateway/test_notification_turn_release.py",
    "tests/tui_gateway/test_onboarding_e2e.py",
    "tests/tui_gateway/test_onboarding_endings.py",
    "tests/tui_gateway/test_onboarding_fresh_launch.py",
    "tests/tui_gateway/test_onboarding_run_rpc.py",
    "tests/tui_gateway/test_onboarding_session.py",
    "tests/tui_gateway/test_onboarding_welcome_claim.py",
    "tests/tui_gateway/test_out_of_band_history_adoption.py",
    "tests/tui_gateway/test_pending_hidden_consumption.py",
    "tests/tui_gateway/test_personality_clobbers_system_prompt.py",
    "tests/tui_gateway/test_ping_probe.py",
    "tests/tui_gateway/test_plugin_inject_host.py",
    "tests/tui_gateway/test_plugin_skill_slash.py",
    "tests/tui_gateway/test_plugins_catalog_lookup.py",
    "tests/tui_gateway/test_plugins_manage_install.py",
    "tests/tui_gateway/test_plugins_manage_late_activation.py",
    "tests/tui_gateway/test_plugins_manage_remove.py",
    "tests/tui_gateway/test_plugins_manage_settings.py",
    "tests/tui_gateway/test_pre_agent_fallback_notice.py",
    "tests/tui_gateway/test_preview_action_bridge_fail_fast.py",
    "tests/tui_gateway/test_profile_db_readonly.py",
    "tests/tui_gateway/test_profile_rebuild_commit.py",
    "tests/tui_gateway/test_profile_resume_cwd.py",
    "tests/tui_gateway/test_profile_resume_cwd_persist.py",
    "tests/tui_gateway/test_profile_roster_reader_cache.py",
    "tests/tui_gateway/test_profile_shell_hooks.py",
    "tests/tui_gateway/test_profile_target_unavailable.py",
    "tests/tui_gateway/test_profile_terminal_scope_entrypoints.py",
    "tests/tui_gateway/test_profiles_configure.py",
    "tests/tui_gateway/test_profiles_configure_model_guard.py",
    "tests/tui_gateway/test_profiles_create_env_refs.py",
    "tests/tui_gateway/test_profiles_describe_secret_scope.py",
    "tests/tui_gateway/test_profiles_inherit_launch_model.py",
    "tests/tui_gateway/test_profiles_list_canonical_session.py",
    "tests/tui_gateway/test_profiles_list_session_field_cache.py",
    "tests/tui_gateway/test_profiles_list_ui_meta_cache.py",
    "tests/tui_gateway/test_profiles_list_worker_session.py",
    "tests/tui_gateway/test_profiles_toolset_pin.py",
    "tests/tui_gateway/test_profiles_ui_meta_cas.py",
    "tests/tui_gateway/test_project_tree.py",
    "tests/tui_gateway/test_projects_rpc.py",
    "tests/tui_gateway/test_prompt_accept_logging.py",
    "tests/tui_gateway/test_protocol.py",
    "tests/tui_gateway/test_provider_not_configured_code.py",
    "tests/tui_gateway/test_queued_prompt_persistence.py",
    "tests/tui_gateway/test_queued_prompt_restart_retire.py",
    "tests/tui_gateway/test_readiness_singleflight.py",
    "tests/tui_gateway/test_reaper_flush_profile_scope.py",
    "tests/tui_gateway/test_reasoning_config_per_model.py",
    "tests/tui_gateway/test_reasoning_session_scope.py",
    "tests/tui_gateway/test_relay_live_author.py",
    "tests/tui_gateway/test_remote_workdir_path_semantics.py",
    "tests/tui_gateway/test_restored_delegation_transport.py",
    "tests/tui_gateway/test_resume_child_run_profile_scope.py",
    "tests/tui_gateway/test_resume_live_lazy_session.py",
    "tests/tui_gateway/test_resume_live_profile_scope.py",
    "tests/tui_gateway/test_resume_profile_scope.py",
    "tests/tui_gateway/test_resume_readonly_mount.py",
    "tests/tui_gateway/test_resume_session_yolo.py",
    "tests/tui_gateway/test_resume_switched_provider_endpoint.py",
    "tests/tui_gateway/test_retirement_admission.py",
    "tests/tui_gateway/test_retirement_continuations.py",
    "tests/tui_gateway/test_retirement_workers.py",
    "tests/tui_gateway/test_review_summary_callback.py",
    "tests/tui_gateway/test_rollback_container_refusal.py",
    "tests/tui_gateway/test_rollback_restore_safe.py",
    "tests/tui_gateway/test_routing_provenance_reopen.py",
    "tests/tui_gateway/test_runtime_identity.py",
    "tests/tui_gateway/test_secondary_profile_model_and_key.py",
    "tests/tui_gateway/test_secret_capture_session.py",
    "tests/tui_gateway/test_seeded_branch_title_routing.py",
    "tests/tui_gateway/test_seeded_session_create.py",
    "tests/tui_gateway/test_serve_exit_flush.py",
    "tests/tui_gateway/test_served_profile_child_env.py",
    "tests/tui_gateway/test_served_profile_child_env_authority.py",
    "tests/tui_gateway/test_server_requests.py",
    "tests/tui_gateway/test_session_agent_cwd.py",
    "tests/tui_gateway/test_session_archive_rpc.py",
    "tests/tui_gateway/test_session_control.py",
    "tests/tui_gateway/test_session_create_model_provider_guard.py",
    "tests/tui_gateway/test_session_cwd_follow.py",
    "tests/tui_gateway/test_session_cwd_profile_key.py",
    "tests/tui_gateway/test_session_db_ownership_teardown.py",
    "tests/tui_gateway/test_session_delete_rpc.py",
    "tests/tui_gateway/test_session_finalize_reaps_kernels.py",
    "tests/tui_gateway/test_session_foreign.py",
    "tests/tui_gateway/test_session_git_metadata_generation.py",
    "tests/tui_gateway/test_session_hidden_rpc.py",
    "tests/tui_gateway/test_session_history_codex_sidecar.py",
    "tests/tui_gateway/test_session_id_injection.py",
    "tests/tui_gateway/test_session_images_dir.py",
    "tests/tui_gateway/test_session_interrupt_cancels_background_review.py",
    "tests/tui_gateway/test_session_list_lineage_rpc.py",
    "tests/tui_gateway/test_session_platform_resolution.py",
    "tests/tui_gateway/test_session_profile_db.py",
    "tests/tui_gateway/test_session_reclaim_notify.py",
    "tests/tui_gateway/test_session_resume_db_ownership.py",
    "tests/tui_gateway/test_session_resume_materializes_minted_key.py",
    "tests/tui_gateway/test_session_usage_account_lines.py",
    "tests/tui_gateway/test_shared_metrics_consent_rpc.py",
    "tests/tui_gateway/test_shared_metrics_slash_command_rpc.py",
    "tests/tui_gateway/test_shared_metrics_update_run_rpc.py",
    "tests/tui_gateway/test_shared_session_delivery.py",
    "tests/tui_gateway/test_shell_exec_security.py",
    "tests/tui_gateway/test_show_reasoning_display_gate.py",
    "tests/tui_gateway/test_skills_manage_install.py",
    "tests/tui_gateway/test_skin_watcher_profile_scope.py",
    "tests/tui_gateway/test_slash_fuzzy.py",
    "tests/tui_gateway/test_slash_prompt_compose_seed.py",
    "tests/tui_gateway/test_slash_refine_live.py",
    "tests/tui_gateway/test_slash_review_owner.py",
    "tests/tui_gateway/test_slash_worker_ansi.py",
    "tests/tui_gateway/test_slash_worker_mcp_discovery.py",
    "tests/tui_gateway/test_slash_worker_provider.py",
    "tests/tui_gateway/test_slash_worker_skill_refuse.py",
    "tests/tui_gateway/test_slash_worker_sys_path.py",
    "tests/tui_gateway/test_slash_worker_watchdog.py",
    "tests/tui_gateway/test_spawn_tree_records.py",
    "tests/tui_gateway/test_ssh_session_cwd_persistence.py",
    "tests/tui_gateway/test_stale_provider_resume_live.py",
    "tests/tui_gateway/test_startup_orphan_sweep.py",
    "tests/tui_gateway/test_status_profile_home.py",
    "tests/tui_gateway/test_stranded_session_adoption.py",
    "tests/tui_gateway/test_stream_interrupt_recovery_orphan.py",
    "tests/tui_gateway/test_subagent_child_mirror.py",
    "tests/tui_gateway/test_subagent_snapshot.py",
    "tests/tui_gateway/test_submit_row_session_target.py",
    "tests/tui_gateway/test_submit_time_user_row.py",
    "tests/tui_gateway/test_subprocess_encoding.py",
    "tests/tui_gateway/test_sudo_command_context.py",
    "tests/tui_gateway/test_theme_bridge.py",
    "tests/tui_gateway/test_todo_state_events.py",
    "tests/tui_gateway/test_tools_readback_scope.py",
    "tests/tui_gateway/test_tour_bridge_fail_fast.py",
    "tests/tui_gateway/test_tui_entry_mcp_owner.py",
    "tests/tui_gateway/test_tui_gateway_event_replay.py",
    "tests/tui_gateway/test_tui_gateway_loop_noise.py",
    "tests/tui_gateway/test_tui_gateway_notification_poller.py",
    "tests/tui_gateway/test_tui_gateway_queue_on_busy.py",
    "tests/tui_gateway/test_tui_gateway_server.py",
    "tests/tui_gateway/test_tui_gateway_server_branch_idempotency.py",
    "tests/tui_gateway/test_tui_gateway_server_crash_history.py",
    "tests/tui_gateway/test_tui_gateway_server_deep_truncate.py",
    "tests/tui_gateway/test_tui_gateway_ws.py",
    "tests/tui_gateway/test_tui_mcp_late_refresh.py",
    "tests/tui_gateway/test_turn_finished_failure_cause.py",
    "tests/tui_gateway/test_turn_payload_interim_final.py",
    "tests/tui_gateway/test_turn_payload_response_transformed.py",
    "tests/tui_gateway/test_turn_payload_reused_final.py",
    "tests/tui_gateway/test_undo_command.py",
    "tests/tui_gateway/test_user_messages.py",
    "tests/tui_gateway/test_vault_methods.py",
    "tests/tui_gateway/test_voice_live_delegation.py",
    "tests/tui_gateway/test_wait_for_mcp_discovery.py",
    "tests/tui_gateway/test_warning_callback_conservation.py",
    "tests/tui_gateway/test_ws_keepalive.py",
    "tests/tui_gateway/test_ws_keepalive_config.py",
    "tests/tui_gateway/test_ws_orphan_races.py",
    "tests/tui_gateway/test_ws_send_timeout.py",
    "tests/tui_gateway/test_ws_surrogate_send.py",
]
_LINUX_TEST_COUNT = 704



def _fail(message: str) -> NoReturn:
    print(f"agc1721 native transport guard: {message}", file=sys.stderr)
    raise SystemExit(1)


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args], cwd=root, check=False, capture_output=True, text=True,
    )
    if result.returncode:
        _fail(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def _git_bytes(root: Path, revision: str, path: str) -> bytes:
    result = subprocess.run(
        ["git", "show", f"{revision}:{path}"],
        cwd=root, check=False, capture_output=True,
    )
    if result.returncode:
        _fail(f"git show failed for {revision}:{path}")
    return result.stdout


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _safe_path(root: Path, raw: object, category: str) -> Path:
    if not isinstance(raw, str) or not raw or Path(raw).is_absolute():
        _fail(f"{category} contains a non-relative path")
    relative = Path(raw)
    if ".." in relative.parts:
        _fail(f"{category} escapes the checkout: {raw!r}")
    path = (root / relative).resolve()
    if root != path and root not in path.parents:
        _fail(f"{category} resolves outside the checkout: {raw!r}")
    if not path.is_file():
        _fail(f"{category} is not a file in the checkout: {raw!r}")
    return path


def _tracked_files(root: Path) -> set[str]:
    """Load the tracked-path set once; never spawn one Git process per entry."""
    result = subprocess.run(
        ["git", "ls-files", "-z"], cwd=root, check=False, capture_output=True,
    )
    if result.returncode:
        _fail(f"git ls-files failed: {result.stderr.decode('utf-8', 'replace').strip()}")
    try:
        return {item.decode("utf-8") for item in result.stdout.split(b"\0") if item}
    except UnicodeDecodeError:
        _fail("git ls-files returned a non-UTF-8 path")


def _verify_entry(
    root: Path,
    entry: object,
    category: str,
    transport: set[str],
    tracked_files: set[str],
) -> tuple[str, str]:
    if not isinstance(entry, dict):
        _fail(f"{category} entry is not an object")
    raw_path = entry.get("path")
    expected = entry.get("sha256")
    if not isinstance(expected, str) or not _SHA256.fullmatch(expected):
        _fail(f"{category} has an unresolved or malformed sha256 for {raw_path!r}")
    path = _safe_path(root, raw_path, category)
    relative = path.relative_to(root).as_posix()
    if relative in transport:
        _fail(f"{category} overlaps a transport path: {relative}")
    if relative not in tracked_files:
        _fail(f"{category} is not tracked at {relative}")
    actual = _sha256(path)
    if actual != expected:
        _fail(f"{category} bytes differ at {relative}: {actual} != {expected}")
    return relative, actual


def _closure_digest(files: dict[str, str]) -> str:
    digest = hashlib.sha256()
    for relative in sorted(files):
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(bytes.fromhex(files[relative]))
        digest.update(b"\0")
    return digest.hexdigest()


def _canonical_json_digest(value: object) -> str:
    return _sha256_bytes(json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("utf-8"))


def _xml_name(test_path: str) -> str:
    if not test_path.endswith(".py"):
        _fail(f"test path is not a Python file: {test_path}")
    return test_path[:-3].replace("/", "__") + "_py.xml"


def _validate_expected_cases(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list) or len(value) != _EXPECTED_FULL_CASE_COUNT:
        _fail("expected_junit_cases must contain exactly 1021 cases")
    cases: list[dict[str, object]] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, dict):
            _fail(f"expected_junit_cases entry {index} is not an object")
        file_name = raw.get("file")
        classname = raw.get("classname")
        name = raw.get("name")
        skipped = raw.get("skip")
        if (
            not isinstance(file_name, str)
            or not isinstance(classname, str)
            or not isinstance(name, str)
            or not isinstance(skipped, bool)
            or not file_name
            or not classname
            or not name
        ):
            _fail(f"expected_junit_cases entry {index} has malformed fields")
        cases.append({"file": file_name, "classname": classname, "name": name, "skip": skipped})
    return cases


def _read_json(path: Path, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        _fail(f"cannot read {label}: {type(exc).__name__}")
    if not isinstance(value, dict):
        _fail(f"{label} is not a JSON object")
    return value


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--proof-sha", required=True)
    parser.add_argument("--phase", choices=("before", "after"), required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--baseline-receipt", type=Path)
    args = parser.parse_args()
    root = Path.cwd().resolve()
    manifest_path = args.manifest.resolve()
    try:
        manifest_relative = manifest_path.relative_to(root).as_posix()
    except ValueError:
        _fail("manifest is outside the checkout")
    if manifest_relative != _MANIFEST_REL:
        _fail(f"manifest must be {_MANIFEST_REL}")
    if not _SHA1.fullmatch(args.proof_sha):
        _fail("proof SHA is malformed")
    actual_head = _git(root, "rev-parse", "HEAD")
    if actual_head != args.proof_sha:
        _fail(f"HEAD {actual_head} does not equal proof SHA {args.proof_sha}")
    parent = _git(root, "rev-parse", "HEAD^")
    status = _git(root, "status", "--porcelain=v1", "--untracked-files=all")
    if status:
        _fail(f"checkout is dirty: {status[:500]}")
    changed = _git(root, "diff", "--name-only", "--no-renames", "HEAD^", "HEAD").splitlines()
    if set(changed) != _TRANSPORT_PATHS or len(changed) != len(_TRANSPORT_PATHS):
        _fail(f"proof commit must change exactly {sorted(_TRANSPORT_PATHS)}; got {changed}")

    manifest = _read_json(manifest_path, "manifest")
    if manifest.get("candidate_commit") != parent:
        _fail(f"manifest candidate_commit does not equal HEAD^ {parent}")
    if manifest.get("transport_paths") != sorted(_TRANSPORT_PATHS):
        _fail("manifest transport_paths are not the exact three diagnostic paths")
    transport_hashes = manifest.get("transport_hashes")
    if not isinstance(transport_hashes, dict) or set(transport_hashes) != {"workflow", "guard"}:
        _fail("manifest must hash workflow and guard only; manifest is not self-hashed")
    for key, relative in (("workflow", _WORKFLOW_REL), ("guard", _GUARD_REL)):
        expected = transport_hashes.get(key)
        if not isinstance(expected, str) or not _SHA256.fullmatch(expected):
            _fail(f"manifest transport hash {key} is unresolved or malformed")
        if _sha256(root / relative) != expected:
            _fail(f"{key} transport bytes do not match manifest")
    if _git_bytes(root, "HEAD", _MANIFEST_REL) != manifest_path.read_bytes():
        _fail("manifest bytes differ from the manifest stored in the proof commit")

    bank = manifest.get("bank")
    if not isinstance(bank, dict):
        _fail("bank metadata is missing")
    for name, paths in _EXPECTED_GROUPS.items():
        if bank.get(name) != paths:
            _fail(f"bank group {name} differs from the reviewed bank")
    if bank.get("excluded_shared_fixture") != "tests/_fixtures/thread_shim.py":
        _fail("bank exclusion for the shared fixture helper is missing")
    if bank.get("excluded_production") != "tui_gateway/session_workdir.py":
        _fail("bank exclusion for the production path is missing")

    source_entries = manifest.get("source_files")
    test_entries = manifest.get("test_files")
    fixture_entries = manifest.get("fixture_files")
    if not isinstance(source_entries, list) or not source_entries:
        _fail("source_files must contain the materialized source byte closure")
    if not isinstance(test_entries, list) or [
        e.get("path") for e in test_entries if isinstance(e, dict)
    ] != _EXPECTED_TESTS:
        _fail("test_files does not equal the explicit reviewed union")
    if len(source_entries) != _EXPECTED_SOURCE_FILE_COUNT:
        _fail(f"source_files must contain exactly {_EXPECTED_SOURCE_FILE_COUNT} entries")
    if len(test_entries) != _EXPECTED_TEST_FILE_COUNT:
        _fail(f"test_files must contain exactly {_EXPECTED_TEST_FILE_COUNT} entries")
    if not isinstance(fixture_entries, list):
        _fail("fixture_files must be a list, possibly empty")
    if len(fixture_entries) != _EXPECTED_FIXTURE_FILE_COUNT:
        _fail(f"fixture_files must contain exactly {_EXPECTED_FIXTURE_FILE_COUNT} entries")
    target_test_files = manifest.get("target_test_files")
    if target_test_files != _TARGET_TESTS:
        _fail("target_test_files must select the exact reviewed 40-file bank")
    if len(target_test_files) != _EXPECTED_TEST_FILE_COUNT or len(set(target_test_files)) != _EXPECTED_TEST_FILE_COUNT:
        _fail("target_test_files must contain exactly 40 unique paths")
    if manifest.get("target_case") != _TARGET_CASE:
        _fail("target_case must select the original deferred-history trace case")
    if manifest.get("execution_scope") != _EXECUTION_SCOPE:
        _fail("execution_scope must require complete context for every selected file")
    expected_cases = _validate_expected_cases(manifest.get("expected_junit_cases"))
    if manifest.get("expected_case_count") != _EXPECTED_FULL_CASE_COUNT:
        _fail("expected_case_count must be 1021")
    if manifest.get("expected_skip_count") != _EXPECTED_FULL_SKIP_COUNT:
        _fail("expected_skip_count must be nine")
    if manifest.get("expected_junit_cases_sha256") != _EXPECTED_JUNIT_CASES_SHA256:
        _fail("expected_junit_cases_sha256 does not match the reviewed full-bank receipt")
    if manifest.get("expected_junit_cases_source_sha256") != _EXPECTED_JUNIT_CASES_SOURCE_SHA256:
        _fail("expected_junit_cases_source_sha256 does not match the reviewed input receipt")
    if _canonical_json_digest(expected_cases) != _EXPECTED_JUNIT_CASES_SHA256:
        _fail("expected_junit_cases bytes differ from the reviewed full-bank receipt")
    expected_xml_files = {_xml_name(path) for path in _EXPECTED_TESTS}
    actual_xml_files = {case["file"] for case in expected_cases}
    if actual_xml_files != expected_xml_files:
        _fail("expected_junit_cases do not cover exactly the 40 selected XML files")
    if sum(1 for case in expected_cases if case["skip"]) != _EXPECTED_FULL_SKIP_COUNT:
        _fail("expected_junit_cases must contain exactly nine skips")
    expected_skip_cases = {(case["file"], case["classname"], case["name"]) for case in expected_cases if case["skip"]}
    expected_skip_digest = _canonical_json_digest([
        {"file": file_name, "classname": classname, "name": name, "skip": True}
        for file_name, classname, name in sorted(expected_skip_cases)
    ])
    if manifest.get("expected_skip_cases_sha256") != expected_skip_digest:
        _fail("expected_skip_cases_sha256 does not match the embedded nine-case skip set")
    if sum(1 for case in expected_cases if case["name"] == _TARGET_CASE) != 1:
        _fail("target deferred-history case must occur exactly once in expected_junit_cases")
    if manifest.get("file_retries") != 0 or manifest.get("parallelism") != 1:
        _fail("full-bank transport must use one process per file with zero retries")
    if manifest.get("per_file_timeout_seconds") != 300:
        _fail("full-bank transport must retain the 300-second per-file timeout")
    if manifest.get("bank_timeout_minutes") != 30 or manifest.get("job_timeout_minutes") != 50:
        _fail("full-bank transport timeout accounting is not the reviewed 30/50-minute budget")
    if manifest.get("postguard_upload_reserve_minutes") != 6:
        _fail("transport must retain at least six minutes after the bank for guards and upload")
    linux_test_files = manifest.get("linux_test_files")
    if not isinstance(linux_test_files, list) or linux_test_files != _EXPECTED_LINUX_TESTS:
        _fail("linux_test_files does not equal the exact reviewed 704-file bank")
    if len(linux_test_files) != _LINUX_TEST_COUNT or len(set(linux_test_files)) != _LINUX_TEST_COUNT:
        _fail("linux_test_files must contain exactly 704 unique paths")
    tracked_files = _tracked_files(root)
    checked: dict[str, str] = {}
    for category, entries in (
        ("source_files", source_entries),
        ("test_files", test_entries),
        ("fixture_files", fixture_entries),
    ):
        for entry in entries:
            relative, actual = _verify_entry(
                root, entry, category, _TRANSPORT_PATHS, tracked_files,
            )
            if relative in checked:
                _fail(f"duplicate byte-closure path across groups: {relative}")
            checked[relative] = actual
    if len(checked) != _EXPECTED_BYTE_CLOSURE_FILE_COUNT:
        _fail(f"selected byte closure must contain exactly {_EXPECTED_BYTE_CLOSURE_FILE_COUNT} files")
    if "tests/_fixtures/thread_shim.py" in [
        path for path in checked if path in _EXPECTED_TESTS
    ]:
        _fail("shared fixture helper must not be in the native test bank")
    for linux_path in linux_test_files:
        _safe_path(root, linux_path, "linux_test_files")
        if linux_path not in checked:
            _fail(f"linux_test_files path is outside the selected byte closure: {linux_path}")

    closure_digest = _closure_digest(checked)
    if args.phase == "after":
        if args.baseline_receipt is None:
            _fail("after phase requires --baseline-receipt")
        baseline = _read_json(args.baseline_receipt.resolve(), "before receipt")
        if baseline.get("files") != checked:
            _fail("selected byte closure changed between before and after")
        if baseline.get("byte_closure_sha256") != closure_digest:
            _fail("byte closure digest changed between before and after")

    receipt = {
        "schema": "agc1721.native_windows_source_only_receipt.r9.full40.v1",
        "purpose": "own-fork-source-only-native-windows-full40-diagnostic-transport",
        "phase": args.phase,
        "proof_sha": actual_head,
        "candidate_commit": parent,
        "workspace": str(root),
        "transport_paths": sorted(_TRANSPORT_PATHS),
        "selected_test_files": _EXPECTED_TESTS,
        "target_test_files": target_test_files,
        "target_case": _TARGET_CASE,
        "execution_scope": _EXECUTION_SCOPE,
        "expected_case_count": _EXPECTED_FULL_CASE_COUNT,
        "expected_skip_count": _EXPECTED_FULL_SKIP_COUNT,
        "expected_junit_cases_sha256": _EXPECTED_JUNIT_CASES_SHA256,
        "expected_junit_cases_source_sha256": _EXPECTED_JUNIT_CASES_SOURCE_SHA256,
        "selected_linux_test_files": _EXPECTED_LINUX_TESTS,
        "file_retries": 0,
        "parallelism": 1,
        "per_file_timeout_seconds": 300,
        "bank_timeout_minutes": 30,
        "job_timeout_minutes": 50,
        "files": checked,
        "byte_closure_sha256": closure_digest,
        "clean_checkout": True,
        "zero_file_retries": True,
    }
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    summary = {
        "phase": args.phase,
        "proof_sha": actual_head,
        "candidate_commit": parent,
        "execution_scope": _EXECUTION_SCOPE,
        "source_files": len(source_entries),
        "test_files": len(test_entries),
        "target_test_files": len(target_test_files),
        "target_case": _TARGET_CASE,
        "expected_case_count": _EXPECTED_FULL_CASE_COUNT,
        "expected_skip_count": _EXPECTED_FULL_SKIP_COUNT,
        "expected_junit_cases_sha256": _EXPECTED_JUNIT_CASES_SHA256,
        "file_retries": 0,
        "parallelism": 1,
        "per_file_timeout_seconds": 300,
        "bank_timeout_minutes": 30,
        "job_timeout_minutes": 50,
        "fixture_files": len(fixture_entries),
        "linux_test_files": len(linux_test_files),
        "byte_closure_files": len(checked),
        "byte_closure_sha256": closure_digest,
    }
    print(json.dumps(summary, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
