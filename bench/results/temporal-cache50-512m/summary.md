### temporal-cache50-512m

| Measure | Value |
| --- | --- |
| peak_working_set_mib | 511.8 |
| peak_raw_usage_mib | 512.2 |
| limit_mib | 512.0 |
| oom_kills | 2 |
| oom_seconds_after_rush_start | [344.5, 609.7] |
| exit_codes | ['137', '137'] |
| restarts | 2 |
| samples | 980 |
| samples_not_running | 1 |
| seconds_working_set_at_95pct_of_limit | 831 |
| rush_messages_planned | 3000 |
| rush_messages_posted | 2130 |
| rush_messages_answered | 1493 |
| rush_messages_posted_without_reply | 637 |
| cpu_mean_percent_during_rush | 180.8 |
| cpu_peak_percent | 209.8 |
| temporal_sticky_cache_size max | 50.0 |
| temporal_sticky_cache_hit total | 1300.0 |
| temporal_sticky_cache_miss total | 1186.0 |
| temporal_sticky_cache_total_forced_eviction total | 2497.0 |

| Phase | Messages | Errors | p50 ms | p95 ms | p99 ms | max ms |
| --- | --- | --- | --- | --- | --- | --- |
| rush | 2130 | 637 | 100 | 263205 | 269715 | 273456 |
| rush before OOM | 1666 | 406 | 99 | 257307 | 260472 | 270718 |
| rush after OOM | 464 | 231 | 260752 | 269903 | 272745 | 273456 |
