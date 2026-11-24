### temporal-default-cache-512m

| Measure | Value |
| --- | --- |
| peak_working_set_mib | 512.0 |
| peak_raw_usage_mib | 512.2 |
| cap_mib | 512.0 |
| oom_kills | 1 |
| oom_seconds_after_rush_start | [330.0] |
| exit_codes | ['137'] |
| restarts | 1 |
| samples | 685 |
| first_sample_epoch | 1795487057.512 |
| last_sample_epoch | 1795487741.52 |
| largest_sample_gap_seconds | 1.01 |
| samples_not_running | 0 |
| samples_working_set_at_or_above_95pct_of_cap | 583 |
| rush_follow_up_attempts_planned | 3000 |
| rush_follow_up_attempts | 1757 |
| rush_attempts_with_observed_reply | 1042 |
| rush_attempts_with_no_observed_reply | 715 |
| cpu_mean_percent_from_rush_start_to_end_of_sampling | 176.2 |
| cpu_peak_percent | 208.9 |
| temporal_sticky_cache_size max | 336.0 |
| temporal_sticky_cache_hit total | 1600.0 |
| temporal_sticky_cache_miss total | None |
| temporal_sticky_cache_total_forced_eviction total | None |

Latency percentiles cover observed replies only.

| Phase | Follow-up attempts | Attempts with no observed reply | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | --- | --- | --- | --- | --- |
| rush | 1757 | 715 | 100 | 246638 | 250186 | 253172 |
| rush before OOM | 1622 | 580 | 100 | 246638 | 250186 | 253172 |
| rush after OOM | 135 | 135 | - | - | - | - |
