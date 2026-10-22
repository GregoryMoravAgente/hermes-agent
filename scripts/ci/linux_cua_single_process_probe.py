"""Single-process browser preflight helpers and diagnostic entry point."""

from __future__ import annotations


def diagnostic_fields(
    direct_file: bool,
    single_process: bool,
    variant_count: int,
    requires_causal_pair: bool,
    all_variants_ok: bool,
) -> tuple[bool, str]:
    experiment_complete = (
        (direct_file and variant_count == (1 if single_process else 2))
        or (requires_causal_pair and variant_count == 2)
        or (not requires_causal_pair and variant_count == 1)
    )
    if direct_file:
        prefix = "single_process_" if single_process else ""
        status = "closed" if all_variants_ok else "failed"
        experiment_result = f"{prefix}direct_file_variant_{status}"
    elif requires_causal_pair and all_variants_ok:
        experiment_result = "both_variants_closed"
    elif not requires_causal_pair and all_variants_ok:
        experiment_result = "setuid_default_closed"
    else:
        experiment_result = "causal_variant_failed"
    return experiment_complete, experiment_result


if __name__ == "__main__":
    from linux_cua_custody import browser_preflight

    browser_preflight(output_transport="direct_files", single_process=True)
