"""Gateway runtime wiring for the Teams meeting pipeline plugin."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from gateway.config import Platform
from gateway.session_authority import SchedulerTaskHandoff
from plugins.teams_pipeline.pipeline import TeamsMeetingPipeline
from plugins.teams_pipeline.store import TeamsPipelineStore, resolve_teams_pipeline_store_path
from plugins.teams_pipeline.subscriptions import build_graph_client

logger = logging.getLogger(__name__)

# Teams platform settings mirrored into the pipeline's teams_delivery config (pipeline keys win).
_DELIVERY_KEYS = ("incoming_webhook_url", "access_token", "team_id", "channel_id", "chat_id")


def _teams_delivery_is_configured(teams_extra: dict[str, Any], teams_delivery: dict[str, Any]) -> bool:
    def pick(key: str) -> Any:
        return teams_delivery.get(key) or teams_extra.get(key)
    delivery_mode = str(teams_delivery.get("mode") or pick("delivery_mode") or "").strip().lower()
    if delivery_mode == "incoming_webhook":
        return bool(pick("incoming_webhook_url"))
    if delivery_mode == "graph":
        return bool(pick("chat_id") or (pick("team_id") and pick("channel_id")))
    return False


def build_pipeline_runtime_config(gateway_config: Any) -> dict[str, Any]:
    """Pipeline knobs live under ``teams.extra.meeting_pipeline``; Teams delivery targets are
    mirrored from the Teams platform config so they are not configured twice."""
    teams_config = gateway_config.platforms.get(Platform("teams"))
    teams_extra = dict((teams_config.extra or {}) if teams_config else {})
    pipeline_config = dict(teams_extra.get("meeting_pipeline") or {})
    if teams_config and teams_config.enabled:
        teams_delivery = dict(pipeline_config.get("teams_delivery") or {})
        if delivery_mode := str(teams_extra.get("delivery_mode") or "").strip():
            teams_delivery["mode"] = delivery_mode
        for key in _DELIVERY_KEYS:
            value = teams_extra.get(key)
            if value not in {None, ""}:
                teams_delivery[key] = value
        if teams_delivery:
            teams_delivery["enabled"] = _teams_delivery_is_configured(teams_extra, teams_delivery)
            pipeline_config["teams_delivery"] = teams_delivery
    return pipeline_config


def build_pipeline_runtime(gateway: Any) -> TeamsMeetingPipeline:
    teams_sender = None
    teams_config = gateway.config.platforms.get(Platform("teams"))
    pipeline_config = build_pipeline_runtime_config(gateway.config)
    if teams_config and teams_config.enabled and (pipeline_config.get("teams_delivery") or {}).get("enabled"):
        try:
            from plugins.platforms.teams.summary_writer import TeamsSummaryWriter
        except ImportError:
            logger.debug("TeamsSummaryWriter unavailable; Teams outbound delivery remains disabled until the adapter layer is present.")
        else:
            teams_sender = TeamsSummaryWriter(platform_config=teams_config)
    return TeamsMeetingPipeline(
        graph_client=build_graph_client(), store=TeamsPipelineStore(resolve_teams_pipeline_store_path()),
        config=pipeline_config, teams_sender=teams_sender,
    )


def _teams_owner_id(gateway: Any) -> str:
    descriptor = getattr(gateway, "session_runtime_descriptor", None) or {}
    instance_id = str(descriptor.get("instance_id") or "").strip()
    if not instance_id:
        raise RuntimeError("Teams pipeline recovery requires a published runtime instance")
    return f"teams:{instance_id}"


def _adopt_recoverable_jobs(gateway: Any, runtime: TeamsMeetingPipeline) -> list[asyncio.Task]:
    """Adopt durable Teams work through the same I01 scheduler custody as Graph notifications."""
    store = getattr(runtime, "store", None)
    if store is None:
        return []
    claim = getattr(store, "claim_recoverable_jobs", None)
    list_jobs = getattr(store, "list_jobs", None)
    if not callable(claim) or not callable(list_jobs):
        raise RuntimeError("Teams pipeline store cannot inspect recoverable jobs")
    recoverable = {
        "received", "resolving_meeting", "fetching_transcript",
        "downloading_recording", "transcribing_audio", "summarizing",
        "writing_notion", "writing_linear", "sending_teams", "retry_scheduled",
    }
    snapshot = list_jobs()
    if not isinstance(snapshot, dict) or not any(
        isinstance(record, dict) and record.get("status") in recoverable
        for record in snapshot.values()
    ):
        return []
    from gateway.session_authorities import active_authority
    authority = active_authority(gateway)
    if authority is None:
        raise RuntimeError("Teams pipeline recovery has no active session authority")
    descriptor = getattr(gateway, "session_runtime_descriptor", None) or {}
    if not str(descriptor.get("instance_id") or "").strip():
        raise RuntimeError("Teams pipeline recovery requires a published runtime instance")
    from pathlib import Path
    from gateway.runtime_ownership import process_ownership
    authority_db = getattr(getattr(authority, "db", None), "db_path", None)
    if authority_db is None or not process_ownership.owns(Path(authority_db).resolve().parent):
        raise RuntimeError("Teams pipeline recovery requires profile ownership")
    records = claim(_teams_owner_id(gateway))
    if not records:
        return []
    active_jobs = getattr(gateway, "_teams_pipeline_active_jobs", None)
    if active_jobs is None:
        active_jobs = gateway._teams_pipeline_active_jobs = set()
    adapter = gateway.adapters.get(Platform.MSGRAPH_WEBHOOK)
    background = getattr(adapter, "_background_tasks", set())
    tasks: list[asyncio.Task] = []
    for record in records:
        job_id = str(record.get("job_id") or "").strip()
        if not job_id or job_id in active_jobs:
            continue
        scheduler_id = authority._preservation_adopt_scheduler_job(job_id)
        task = asyncio.create_task(runtime.run_job(job_id))
        active_jobs.add(job_id)
        background.add(task)
        task.add_done_callback(background.discard)
        task.add_done_callback(lambda _task, key=job_id: active_jobs.discard(key))
        task.add_done_callback(
            lambda done, owner=authority, sid=scheduler_id: owner._preservation_scheduler_done(sid, done))
        tasks.append(task)
    return tasks


def _install_runtime_refusal(adapter: Any) -> None:
    """Keep authenticated Graph retries retryable when the Teams runtime cannot be observed."""
    async def _refuse(_notification: dict[str, Any], _event: Any) -> None:
        from hermes_state_runtime import RuntimeStoreError
        raise RuntimeStoreError("teams_runtime_unavailable")
    adapter.set_notification_scheduler(_refuse)


def bind_gateway_runtime(gateway: Any) -> bool:
    """Attach the Teams pipeline runtime to the msgraph webhook adapter."""
    adapter = gateway.adapters.get(Platform.MSGRAPH_WEBHOOK)
    if adapter is None:
        return False
    existing = getattr(gateway, "_teams_pipeline_runtime", None)
    runtime = existing
    if runtime is not None:
        try:
            _adopt_recoverable_jobs(gateway, runtime)
        except Exception as exc:  # health: allow BLE001 -- binding/recovery is an opaque persistence boundary; refusal preserves retry and custody
            gateway._teams_pipeline_runtime_error = str(exc)
            _install_runtime_refusal(adapter)
            return False
        gateway._teams_pipeline_runtime_error = None
    else:
        try:
            runtime = build_pipeline_runtime(gateway)
        except Exception as exc:  # health: allow BLE001 -- runtime construction failure must install typed refusal instead of acknowledging work
            gateway._teams_pipeline_runtime_error = str(exc)
            logger.warning(
                "Teams pipeline runtime unavailable: %s. Installing a refusal scheduler "
                "so Graph retries remain visible and no work is acknowledged as dropped.", exc,
            )
            _install_runtime_refusal(adapter)
            return False

        gateway._teams_pipeline_runtime = runtime
        gateway._teams_pipeline_runtime_error = None
        try:
            _adopt_recoverable_jobs(gateway, runtime)
        except Exception as exc:  # health: allow BLE001 -- durable recovery observation failure retains retryable ingress and exact custody
            gateway._teams_pipeline_runtime_error = str(exc)
            logger.warning("Teams pipeline durable recovery unavailable: %s", exc)
            _install_runtime_refusal(adapter)
            return False

    # Share the active-job set with the live Graph scheduler.  A recovered ``received`` record may
    # still be in its first task turn when a duplicate notification arrives; the adapter must not
    # create a second task for that exact durable job.
    scheduled = getattr(gateway, "_teams_pipeline_active_jobs", None)
    if scheduled is None:
        scheduled = gateway._teams_pipeline_active_jobs = set()

    # Graph wants a prompt 202 once the work is durable: accept (persist the job) inside the request,
    # run the fetch/transcript/summary pipeline detached. An accept failure still fails the request.
    background = getattr(adapter, "_background_tasks", set())
    # Claim a job before scheduling its external effects: a no-id duplicate in one Graph batch
    # returns the same still-"received" job (run_job persists progress only once it runs).
    async def _schedule(notification: dict[str, Any], event: Any) -> SchedulerTaskHandoff | None:
        job = runtime.create_job_from_notification(notification)
        if job.status != "received" or job.job_id in scheduled:  # duplicate of a running or finished job
            return
        scheduled.add(job.job_id)
        task = asyncio.create_task(runtime.run_job(job.job_id))
        background.add(task)
        task.add_done_callback(background.discard)
        task.add_done_callback(lambda _task: scheduled.discard(job.job_id))
        # The adapter owns the I01 root until this real pipeline task settles. Return a typed
        # handoff so the scheduler boundary binds the exact durable job before it accepts completion.
        return SchedulerTaskHandoff(task=task, expected_job_id=job.job_id)
    adapter.set_notification_scheduler(_schedule)
    return True
