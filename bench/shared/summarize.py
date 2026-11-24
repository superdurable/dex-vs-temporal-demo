"""Summarises one run of the worker memory experiment.

Inputs are the files the sampler and the load driver write. Output is a
Markdown table plus a JSON object with the same numbers.

Usage:
  python summarize.py --name run1 --stats stats.csv --events events.jsonl \
    --latency rush-latency.csv [--latency fill-latency.csv] [--json-out summary.json]

Rush latency is also split at the first OOM kill: "before OOM" holds messages
posted before it, "after OOM" holds the rest.
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

MEBIBYTE = 1024 * 1024
LIMIT_FRACTION = 0.95
# The sampler writes one row per second; a larger gap means missed samples.
MAX_SAMPLE_GAP_SECONDS = 2.5
STICKY_CACHE_COUNTERS = (
    "temporal_sticky_cache_hit",
    "temporal_sticky_cache_miss",
    "temporal_sticky_cache_total_forced_eviction",
)
# The Dex demo Worker's own counters (dex/worker_metrics.py); the SDK exports none.
DEX_BLOB_CACHE_COUNTERS = (
    "dex_demo_blob_cache_hits",
    "dex_demo_blob_cache_misses",
    "dex_demo_blob_cache_puts",
    "dex_demo_blob_cache_puts_admitted",
)
DEX_MEMORY_GAUGES = (
    "dex_demo_blob_cache_directory_bytes",
    "dex_demo_process_rss_bytes",
    "dex_demo_process_rss_peak_bytes",
    "dex_demo_cgroup_anon_bytes",
    "dex_demo_cgroup_file_bytes",
    "dex_demo_cgroup_active_file_bytes",
)


def percentile(sorted_values: list[float], fraction: float) -> float | None:
    if not sorted_values:
        return None
    index = min(len(sorted_values) - 1, max(0, round(fraction * (len(sorted_values) - 1))))
    return sorted_values[index]


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


def read_events(path: Path) -> list[dict]:
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            record = json.loads(line)
            events.append(
                {
                    "epoch": record.get("timeNano", 0) / 1e9 or float(record.get("time", 0)),
                    "action": record.get("Action") or record.get("status", ""),
                    "exit_code": record.get("Actor", {}).get("Attributes", {}).get("exitCode", ""),
                }
            )
    return events


def summarize_latencies(rows: list[dict[str, str]]) -> dict:
    """Latency percentiles cover observed replies only; other attempts are counted apart."""
    latencies = sorted(float(row["latency_ms"]) for row in rows if row["status"] == "ok")
    return {
        "attempts": len(rows),
        "attempts_with_no_observed_reply": sum(1 for row in rows if row["status"] != "ok"),
        "p50_ms": percentile(latencies, 0.50),
        "p95_ms": percentile(latencies, 0.95),
        "p99_ms": percentile(latencies, 0.99),
        "max_ms": latencies[-1] if latencies else None,
    }


def accumulate_counter_across_restarts(values: list[float]) -> float | None:
    """Sums a counter whose process restarts reset it to zero."""
    if not values:
        return None
    total = 0.0
    previous = 0.0
    for value in values:
        if value < previous:
            total += previous
        previous = value
    return total + previous


def sample_coverage(stats: list[dict[str, str]]) -> dict:
    epochs = [float(row["epoch"]) for row in stats]
    gaps = [later - earlier for earlier, later in zip(epochs, epochs[1:])]
    return {
        "samples": len(stats),
        "first_sample_epoch": epochs[0] if epochs else None,
        "last_sample_epoch": epochs[-1] if epochs else None,
        "largest_sample_gap_seconds": round(max(gaps), 3) if gaps else None,
    }


def coverage_warnings(
    coverage: dict, events_present: bool, latency_rows: list[dict[str, str]], restarts: int, dies: int
) -> list[str]:
    warnings = []
    if not events_present:
        warnings.append("no Docker event file: OOM kills unknown")
    if coverage["samples"] == 0:
        warnings.append("no samples")
        return warnings
    last_attempt = max(
        (float(row["reply_epoch"] or row["post_epoch"]) for row in latency_rows), default=None
    )
    if last_attempt is not None and coverage["last_sample_epoch"] < last_attempt:
        warnings.append("sampling ended before the last recorded attempt: the sampler exited early")
    if coverage["largest_sample_gap_seconds"] and coverage["largest_sample_gap_seconds"] > MAX_SAMPLE_GAP_SECONDS:
        warnings.append(f"a gap of {coverage['largest_sample_gap_seconds']} s between samples")
    if restarts > dies:
        warnings.append(f"{restarts} restarts but {dies} die events: events are missing")
    return warnings


def summarize(args: argparse.Namespace) -> dict:
    stats = read_csv_rows(Path(args.stats))
    events_present = Path(args.events).exists()
    events = read_events(Path(args.events)) if events_present else []
    latency_rows: list[dict[str, str]] = []
    for path in args.latency:
        latency_rows.extend(read_csv_rows(Path(path)))

    rush_rows = [row for row in latency_rows if row["phase"] == "rush"]
    rush_start = min((float(row["post_epoch"]) for row in rush_rows), default=None)
    oom_epochs = [event["epoch"] for event in events if event["action"] == "oom"]
    die_events = [event for event in events if event["action"] == "die"]
    first_oom = oom_epochs[0] if oom_epochs else None

    def since_rush(epoch: float) -> float | None:
        return round(epoch - rush_start, 1) if rush_start is not None else None

    working_set = [int(row["mem_working_set_bytes"]) for row in stats if row["mem_working_set_bytes"]]
    raw_usage = [int(row["mem_usage_bytes"]) for row in stats if row["mem_usage_bytes"]]
    limits = [int(row["mem_limit_bytes"]) for row in stats if row["mem_limit_bytes"]]
    cpu = [float(row["cpu_percent"]) for row in stats if row["cpu_percent"]]
    rush_cpu = [
        float(row["cpu_percent"]) for row in stats
        if row["cpu_percent"] and rush_start is not None and float(row["epoch"]) >= rush_start
    ]
    restart_counts = [int(row["restart_count"]) for row in stats if row["restart_count"]]
    # Samples (1 per second) whose working set is at or above 95% of the configured cap.
    samples_at_cap = sum(
        1 for row in stats
        if row["mem_working_set_bytes"] and row["mem_limit_bytes"]
        and int(row["mem_working_set_bytes"]) >= LIMIT_FRACTION * int(row["mem_limit_bytes"])
    )
    rush_answered = sum(1 for row in rush_rows if row["status"] == "ok")

    phases: dict[str, dict] = {}
    for phase in sorted({row["phase"] for row in latency_rows}):
        phases[phase] = summarize_latencies([row for row in latency_rows if row["phase"] == phase])
    if first_oom is not None and rush_rows:
        phases["rush before OOM"] = summarize_latencies(
            [row for row in rush_rows if float(row["post_epoch"]) < first_oom]
        )
        phases["rush after OOM"] = summarize_latencies(
            [row for row in rush_rows if float(row["post_epoch"]) >= first_oom]
        )

    metrics: dict[str, float | None] = {}
    if stats and "temporal_sticky_cache_size" in stats[0]:
        sizes = [float(row["temporal_sticky_cache_size"]) for row in stats if row["temporal_sticky_cache_size"]]
        metrics["temporal_sticky_cache_size max"] = max(sizes) if sizes else None
        for counter in STICKY_CACHE_COUNTERS:
            if counter in stats[0]:
                values = [float(row[counter]) for row in stats if row[counter]]
                metrics[f"{counter} total"] = accumulate_counter_across_restarts(values)
    if stats and DEX_BLOB_CACHE_COUNTERS[0] in stats[0]:
        for counter in DEX_BLOB_CACHE_COUNTERS:
            values = [float(row[counter]) for row in stats if row.get(counter)]
            metrics[f"{counter} total"] = accumulate_counter_across_restarts(values)
        for gauge in DEX_MEMORY_GAUGES:
            values = [float(row[gauge]) for row in stats if row.get(gauge)]
            metrics[f"{gauge} max MiB"] = round(max(values) / MEBIBYTE, 1) if values else None

    restarts = max(restart_counts) if restart_counts else 0
    coverage = sample_coverage(stats)
    return {
        "name": args.name,
        "warnings": coverage_warnings(coverage, events_present, latency_rows, restarts, len(die_events)),
        "peak_working_set_mib": round(max(working_set) / MEBIBYTE, 1) if working_set else None,
        "peak_raw_usage_mib": round(max(raw_usage) / MEBIBYTE, 1) if raw_usage else None,
        "cap_mib": round(max(limits) / MEBIBYTE, 1) if limits else None,
        "oom_kills": len(oom_epochs) if events_present else None,
        "oom_seconds_after_rush_start": [since_rush(epoch) for epoch in oom_epochs],
        "exit_codes": [event["exit_code"] for event in die_events],
        "restarts": restarts,
        **coverage,
        "samples_not_running": sum(1 for row in stats if row["status"] != "running"),
        "samples_working_set_at_or_above_95pct_of_cap": samples_at_cap,
        "rush_follow_up_attempts_planned": args.planned_messages,
        "rush_follow_up_attempts": len(rush_rows),
        "rush_attempts_with_observed_reply": rush_answered,
        "rush_attempts_with_no_observed_reply": len(rush_rows) - rush_answered,
        # Includes the cooldown samples after the last attempt.
        "cpu_mean_percent_from_rush_start_to_end_of_sampling": (
            round(sum(rush_cpu) / len(rush_cpu), 1) if rush_cpu else None
        ),
        "cpu_peak_percent": max(cpu) if cpu else None,
        "latency_by_phase": phases,
        "worker_metrics": metrics,
    }


def format_milliseconds(value: float | None) -> str:
    return "-" if value is None else f"{value:.0f}"


def print_markdown(summary: dict) -> None:
    print(f"### {summary['name']}")
    print()
    for warning in summary["warnings"]:
        print(f"**WARNING: {warning}**")
        print()
    print("| Measure | Value |")
    print("| --- | --- |")
    for key in (
        "peak_working_set_mib", "peak_raw_usage_mib", "cap_mib", "oom_kills",
        "oom_seconds_after_rush_start", "exit_codes", "restarts",
        "samples", "first_sample_epoch", "last_sample_epoch", "largest_sample_gap_seconds",
        "samples_not_running", "samples_working_set_at_or_above_95pct_of_cap",
        "rush_follow_up_attempts_planned", "rush_follow_up_attempts",
        "rush_attempts_with_observed_reply", "rush_attempts_with_no_observed_reply",
        "cpu_mean_percent_from_rush_start_to_end_of_sampling", "cpu_peak_percent",
    ):
        print(f"| {key} | {summary[key]} |")
    for key, value in summary["worker_metrics"].items():
        print(f"| {key} | {value} |")
    print()
    print("Latency percentiles cover observed replies only.")
    print()
    print(
        "| Phase | Follow-up attempts | Attempts with no observed reply | "
        "p50 ms | p95 ms | p99 ms | max ms |"
    )
    print("| --- | --- | --- | --- | --- | --- | --- |")
    for phase, values in summary["latency_by_phase"].items():
        print(
            f"| {phase} | {values['attempts']} | {values['attempts_with_no_observed_reply']} | "
            f"{format_milliseconds(values['p50_ms'])} | {format_milliseconds(values['p95_ms'])} | "
            f"{format_milliseconds(values['p99_ms'])} | {format_milliseconds(values['max_ms'])} |"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--name", required=True)
    parser.add_argument("--stats", required=True)
    parser.add_argument("--events", required=True)
    parser.add_argument("--latency", action="append", required=True)
    parser.add_argument("--json-out", default="")
    parser.add_argument("--planned-messages", type=int, default=None, help="K x F of the rush")
    args = parser.parse_args()
    summary = summarize(args)
    print_markdown(summary)
    if args.json_out:
        Path(args.json_out).write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
