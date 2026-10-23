"""Read-only projection of retained structured API run custody."""

def active_run_custody_count(self, *, excluded_run_ids=(), exclude_legacy=False) -> int:
    """Count retained API custody that is not already represented by legacy work.

    Legacy ``_run_agent`` turns keep their source launch indexed until the durable
    result fence settles, while the adapter's handler count already represents that
    live turn. Structured ``/v1/runs`` custody has no legacy source launch and stays
    in this projection.
    """
    excluded = set(excluded_run_ids)
    custody = getattr(self, "_run_custody", {})
    return sum(
        1 for run_id, item in custody.items()
        if run_id not in excluded
        and not getattr(item, "released", False)
        and not (
            exclude_legacy
            and getattr(getattr(item, "source_launch", None), "queue", object()) is None
        )
    )
