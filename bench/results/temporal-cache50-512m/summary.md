### temporal-cache50-512m

| Measure | Value |
| --- | --- |
| peak_working_set_mib | 511.8 |
| peak_raw_usage_mib | 512.2 |
| cap_mib | 512.0 |
| oom_kills | 2 |
| oom_seconds_after_rush_start | [344.5, 609.7] |
| exit_codes | ['137', '137'] |
| restarts | 2 |
| samples | 980 |
| first_sample_epoch | 1795489304.154 |
| last_sample_epoch | 1795490283.151 |
| largest_sample_gap_seconds | 1.01 |
| samples_not_running | 1 |
| samples_working_set_at_or_above_95pct_of_cap | 831 |
| rush_follow_up_attempts_planned | 3000 |
| rush_follow_up_attempts | 2130 |
| rush_attempts_with_observed_reply | 1493 |
| rush_attempts_with_no_observed_reply | 637 |
| cpu_mean_percent_from_rush_start_to_end_of_sampling | 180.8 |
| cpu_peak_percent | 209.8 |
| temporal_sticky_cache_size max | 50.0 |
| temporal_sticky_cache_hit total | 1300.0 |
| temporal_sticky_cache_miss total | 1186.0 |
| temporal_sticky_cache_total_forced_eviction total | 2497.0 |

Latency percentiles cover observed replies only.

| Phase | Follow-up attempts | Attempts with no observed reply | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | --- | --- | --- | --- | --- |
| rush | 2130 | 637 | 100 | 263205 | 269715 | 273456 |
| rush before OOM | 1666 | 406 | 99 | 257307 | 260472 | 270718 |
| rush after OOM | 464 | 231 | 260752 | 269903 | 272745 | 273456 |
