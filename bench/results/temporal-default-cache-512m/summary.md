### temporal-default-cache-512m

| Measure | Value |
| --- | --- |
| peak_working_set_mib | 512.0 |
| peak_raw_usage_mib | 512.2 |
| limit_mib | 512.0 |
| oom_kills | 1 |
| oom_seconds_after_rush_start | [330.0] |
| exit_codes | ['137'] |
| restarts | 1 |
| samples | 685 |
| samples_not_running | 0 |
| seconds_working_set_at_95pct_of_limit | 583 |
| rush_messages_planned | 3000 |
| rush_messages_posted | 1757 |
| rush_messages_answered | 1042 |
| rush_messages_posted_without_reply | 715 |
| cpu_mean_percent_during_rush | 176.2 |
| cpu_peak_percent | 208.9 |
| temporal_sticky_cache_size max | 336.0 |
| temporal_sticky_cache_hit total | 1600.0 |
| temporal_sticky_cache_miss total | None |
| temporal_sticky_cache_total_forced_eviction total | None |

| Phase | Messages | Errors | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | --- | --- | --- | --- | --- |
| rush | 1757 | 715 | 100 | 246638 | 250186 | 253172 |
| rush before OOM | 1622 | 580 | 100 | 246638 | 250186 | 253172 |
| rush after OOM | 135 | 135 | - | - | - | - |
