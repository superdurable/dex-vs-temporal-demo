# Worker memory benchmark

This benchmark measures what happens to a conversation worker when many idle
conversations come back at once. The worker runs in a container with explicit
`--memory` and `--cpus` limits. The engine's server and the load driver run on
the host. The results are in the root `README.md`, demo "5. Worker memory",
and the raw data is under `results/`.

## Layout

| Path | Engine | What it does |
| --- | --- | --- |
| `shared/conversation_adapter.py` | any | The adapter interface the driver calls |
| `shared/load_driver.py` | any | `fill` and `rush` phases; one CSV row per customer message |
| `shared/docker_sampler.py` | any | Samples the worker container once per second through the Docker API |
| `shared/summarize.py` | any | Peak memory, OOM kills, restarts, samples at the cap, sample coverage, CPU, latency percentiles per phase |
| `shared/fill_summary.py` | any | Per-conversation exchange counts of the fill, and the fill and rush wall-clock times |
| `shared/memory_run.sh` | any | Process handling of both run scripts: run lock, labelled containers, preflight checks |
| `temporal/temporal_adapter.py` | Temporal | Adapter: workflow start, signal, history long-poll |
| `temporal/Dockerfile` | Temporal | Worker image `dvt-temporal-worker` |
| `../scripts/temporal/t3-memory.sh` | Temporal | One complete run: server, fill, rush, summary, cleanup |
| `dex/dex_adapter.py` | Dex | Adapter: Flow start, `post_message` RPC, Step-completion wait |
| `dex/Dockerfile` | Dex | Worker image `dvt-dex-worker` |
| `../scripts/dex/d3-memory.sh` | Dex | One complete run: server, fill, rush, summary, cleanup, server data deletion |
| `../dex/worker_metrics.py` | Dex | The demo Worker's own metrics: BlobCache call counts, RSS, cgroup memory split, BlobCache directory size |
| `results/<run>/` | both | The published data of each run in the root README |

## Requirements

- macOS with a Docker backend that forwards `host.docker.internal` to the
  host loopback. The published runs used OrbStack (Docker Engine 29.4.0,
  cgroup v2) on macOS 26. Docker Desktop provides the same name; it was
  not tested.
- Native Linux Docker is untested. A container there does not reach the
  host loopback through `host.docker.internal` by default. Both scripts
  check this from a container after the server starts and stop with an
  error when it fails.
- `uv`, `lsof`, the `docker` CLI, and the engine CLI: `temporal` for
  `t3-memory.sh`, `dexcli` for `d3-memory.sh`.
- Free disk for `d3-memory.sh`: more than `MIN_FREE_GB` (default 80)
  gigabytes of 10^9 bytes under its run directory.

## Workload

1. **Fill.** Create `N` conversations. Fill each with `M` exchanges: a
   customer message of `B` bytes and a mock AI reply of `B` bytes. The fill
   uses a separate worker container without a tight limit. It is then
   stopped, so all conversations are idle and no worker holds them. The fill
   fails, and writes no manifest, unless every conversation has an observed
   reply for each of its `M` exchanges.
2. **Rush.** Start a clean, limited worker container. `K` conversations
   come back at `R` conversations per second. Each sends `F` follow-ups.
   Each follow-up is posted `T` seconds after the previous reply was
   observed. After an attempt with no observed reply, that conversation
   sends no further follow-ups.

Each run starts its own server and fills its own population, so runs do not
see each other's history or leftover work. Each published configuration was
run once.

Defaults of both scripts. Every published run used them; the control runs
override only `CACHE_SIZE` and `MEM_LIMIT`.

| Parameter | Value |
| --- | --- |
| `CONVERSATIONS` (N), `EXCHANGES` (M), `MESSAGE_BYTES` (B) | 1,200, 40, 8,192 |
| `FILL_CONCURRENCY` | 200 conversations at a time |
| `RETURNING` (K), `RATE` (R), `FOLLOW_UPS` (F), `THINK_SECONDS` (T) | 1,000, 4 per second, 3, 10 s |
| `SEED` | 1 |
| `BASELINE_SECONDS`, `COOLDOWN_SECONDS`, `MAX_RUSH_SECONDS` | 15, 20, 1,500 |
| `MEM_LIMIT`, `CPUS` | `512m` (512 MiB), 2; swap disabled (`--memory-swap` equals `--memory`) |
| Temporal: `CACHE_SIZE`, `WF_TASK_SLOTS`, `ACTIVITY_SLOTS` | SDK default (1,000), 100, 100 |
| Temporal fill worker | 6 GiB, 4 CPUs, cache `FILL_CONCURRENCY + 100` |
| Dex: `TASKS`, `BLOB_CACHE_BYTES` | 32, 1 GiB |
| Dex fill Worker | 4 GiB, 4 CPUs, 64 task slots |

Fixed values that `parameters.txt` of the published runs does not store:

- Reply timeout: 120 s per fill exchange, 300 s per rush attempt
  (`load_driver.py --reply-timeout`).
- Idle window and close-after window of the benchmark conversations:
  86,400 s (`DEMO_IDLE_SECONDS`). No nudge or close happens during a run.
  The 30 s idle window of the other demos in the root README does not apply.
- `BASELINE_SECONDS`, `COOLDOWN_SECONDS`, `MAX_RUSH_SECONDS` as in the table.
  Runs made with the current scripts record them in `parameters.txt`.

## Running

Each script runs one complete run per invocation and writes it to
`<run dir>/<RUN_NAME>/`:

```bash
RUN_NAME=temporal-default-cache-512m scripts/temporal/t3-memory.sh
CACHE_SIZE=50 RUN_NAME=temporal-cache50-512m scripts/temporal/t3-memory.sh
CACHE_SIZE=50 MEM_LIMIT=2g RUN_NAME=temporal-cache50-2g scripts/temporal/t3-memory.sh
RUN_NAME=dex-512m scripts/dex/d3-memory.sh
MEM_LIMIT=2g RUN_NAME=dex-2g scripts/dex/d3-memory.sh
```

The run dir is `RUN_DIR` (Temporal, default `/tmp/dex-video/temporal-mem-run`)
or `DEX_DEMO_RUN_DIR` (Dex, default `/tmp/dex-video/dex-mem-run`).

Process handling:

- One memory run at a time. Both scripts share the lock
  `/tmp/dex-video/memory-bench.lock` (`MEMORY_RUN_LOCK_DIR`). A second run
  stops with an error while the first one runs. A lock whose process is gone
  is removed.
- Each script starts its own server and refuses to use one that is already
  listening on its port. It stops only the processes it started. An
  inherited `KEEP=1` does not keep the Temporal server of a memory run.
- Its containers carry the label `dvt-mem-run=<run ID>`. It removes the
  containers whose IDs it recorded and those with its exact label value,
  including the short-lived version and connectivity probes.
- Cleanup runs on every exit path: success, failure, SIGINT, SIGTERM, and
  the time limit `MAX_RUN_SECONDS` (default 3,600 s). A second signal during
  cleanup is ignored.
- `cleanup <run ID>` removes the leftover containers of one earlier run. It
  refuses while that run is active.

Server data:

- The Temporal dev server keeps its data in memory. It goes away when the
  script stops the server.
- `d3-memory.sh` refuses to start when `zd-data` or `blob-cache` already
  exists in its run directory, and keeps that directory. After the summary
  it deletes the server data that this run created, unless
  `KEEP_SERVER_DATA=1`. It deletes it only when no process uses the data
  directory and nothing listens on `DEX_PORT`; otherwise it keeps the data
  and says so. Every reply rewrites the conversation's growing transcript
  chunk, so a full 8 KiB fill stores about 50 GB.

## Outputs per run

`parameters.txt`, `stats.csv` (sampler), `events.jsonl` (the Docker event
stream of the worker container while the sampler runs: container lifecycle
events, including `oom`, `die`, and `start`; the first start comes before the
sampler subscribes and is not in the file), `rush-latency.csv`,
`fill-latency.csv`, `driver.log`, `fill-driver.log`, `worker.log`,
`fill-summary.txt`, `summary.md`, `summary.json`. Temporal adds
`server.log`. Dex adds `server-data-size.txt`, `dexcli-dev.log`, and
`server-logs/`.

`results/<run>/` publishes `parameters.txt`, `stats.csv`, `events.jsonl`,
`rush-latency.csv`, `fill-summary.txt`, `summary.md`, and `summary.json`,
plus `server-data-size.txt` for Dex. The `fill-summary.txt` files of the
published runs were made with `shared/fill_summary.py` from each run's
unpublished fill CSV and driver logs.

## Measures

- **Memory.** `mem_working_set_bytes` is the container's memory usage minus
  its `inactive_file` memory, the figure `docker stats` shows on cgroup v2.
  `mem_usage_bytes` is the raw cgroup usage. They are different
  observations: the Docker memory cap applies to the raw usage, and
  "at the cap" counts the working set. The Dex demo metric
  `dex_demo_cgroup_file_bytes` is the whole cgroup's file memory
  (`memory.stat` `file`), which includes the page cache of the BlobCache
  files.
- **OOM kills** come from the Docker event stream (`oom` events). The exit
  code of each `die` event is listed; 137 is a SIGKILL. Without an event
  file the summary reports the OOM kills as unknown and prints a warning.
- **Samples at the cap** counts the samples, taken once per second, whose
  working set is at or above 95% of the configured cap.
- **Sample coverage.** The summary lists the number of samples, the first
  and last sample time, and the largest gap between samples. It prints a
  warning when sampling ended before the last recorded attempt, when a gap
  exceeds 2.5 s, or when the container restarted more often than the event
  file shows.
- **Follow-up attempts.** Planned is `K x F`. Follow-up attempts counts the
  rows of `rush-latency.csv`. An attempt has no observed reply when the
  send call failed, when waiting for the reply failed, or when no reply was
  observed within 300 s. The 300 s start when the send call returns.
- **Reply latency** is the driver-observed round trip: from just before the
  send call until the adapter observes the reply. The percentiles cover
  attempts with an observed reply only.
- **CPU.** `cpu_mean_percent_from_rush_start_to_end_of_sampling` includes
  the 20 s cooldown after the last attempt. 100 is one full CPU.

## Known limits of the harness

These are known and documented, not changed, so the published results come
from the code in this repository.

- Attempts still in flight when `MAX_RUSH_SECONDS` ends the rush are not
  written to `rush-latency.csv`. None of the published runs reached that
  limit.
- The 300 s reply wait and the send call have separate budgets; there is no
  single deadline per attempt measured from its start.
- The Temporal adapter treats the first workflow task completed after a
  reply activity as the reply. It does not correlate signals, activities,
  and tasks by a request ID.
- The sampler computes the working set with the cgroup v2 formula
  (`usage - inactive_file`). On cgroup v1 the Docker CLI uses
  `total_inactive_file`; the published runs used cgroup v2.
- External commands such as `docker build` and `uv sync` run in the script's
  foreground. A stop signal is handled after such a command returns.
- `dex/worker_metrics.py` walks the BlobCache directory on each scrape and
  reads its counters without a lock. The counters are sampled values.
- The published Temporal runs use the SDK's default slot counts (100
  workflow task slots and 100 activity slots). No run with fewer slots was
  made.

## Plugging in another engine

An engine needs three pieces. The shared code does not change.

1. **Adapter.** A class with a no-argument constructor that implements
   `ConversationAdapter` in `shared/conversation_adapter.py`:
   `connect`, `close`, `start_conversation(id, first_message)`,
   `observe_conversation(id)`, `post_message(id, text)`, and
   `wait_for_reply(id, timeout_seconds)`. It reads its settings from
   environment variables. Observing a reply must not make the worker load the
   conversation, for example through a query; otherwise the measurement
   changes what it measures. `observe_conversation` runs before the timed
   part and may do setup work, such as catching up with an event stream.
   Load it with `load_driver.py --adapter <module>:<ClassName>` and put its
   directory on `PYTHONPATH`.
2. **Worker image.** A Dockerfile under `bench/<engine>/` whose container runs
   the same conversation logic as the Temporal worker. The application reply
   activity or step returns `B` bytes (both demo workers read
   `DEMO_REPLY_BYTES`). The worker process should be PID 1, so an OOM kill
   stops the container. A graceful stop signal (`STOPSIGNAL`) is optional.
3. **Run script.** A script under `scripts/<engine>/` that sources
   `shared/memory_run.sh`, starts the server on the host, builds the image,
   starts the worker container with the same `--memory`, `--memory-swap`,
   `--cpus`, and `--restart on-failure` flags, runs `docker_sampler.py`
   against it, and runs `load_driver.py fill` and `rush` with the same
   parameters. Pass the engine's worker metrics to the sampler with
   `--metrics-url` and `--metric` if it exports any.

Keep these identical across engines for a fair comparison: `N`, `M`, `B`,
`K`, `R`, `F`, `T`, the seed, the container limits, the host, and the
reply-visible definition of latency.

## Temporal specifics

- **Workflow.** `ConversationWorkflow` v1 (`temporal/conversation_workflow.py`).
- **Reply visible.** The adapter long-polls the workflow history until the
  workflow task after the reply activity has completed. It never sends a
  query.
- **Posting** is a signal, accepted by the server alone.
- **Sticky queue.** The worker image stops on SIGINT, so a stopped fill
  worker releases its sticky queue. The adapter resets sticky affinity
  before the rush and backs off when the dev server's namespace rate limit
  rejects a call.
- **Metrics.** The SDK's Prometheus endpoint gives the sticky cache size,
  hits, misses, and forced evictions.

## Dex specifics

- **Reply visible.** The n-th customer message of a conversation is
  answered by execution n of `GenerateReplyStep`. That execution writes the
  message and the reply to the transcript in one commit. The adapter waits
  with the Client's Step-completion wait, then confirms the reply count with
  one Client Attribute read; the latency includes that read. Dex Server
  answers both; the Worker is not involved.
- **Posting** is the `post_message` RPC, which runs on the Worker. A
  Temporal signal is accepted by the server alone.
- **Worker ID.** Each Worker process appends a UUID to the ID
  (`DEMO_WORKER_ID_PER_START=1`), because a Worker ID must change after a
  restart, such as a container restart after an OOM kill.
- **BlobCache.** On the container's writable layer under `/data`, never
  tmpfs. Its byte limit is `BLOB_CACHE_BYTES` (default 1 GiB). The page
  cache of its files counts toward the container's raw memory usage.
- **Metrics.** The SDK exports none. `dex/worker_metrics.py` counts the
  BlobCache calls the SDK makes and reports RSS, the cgroup `anon` and
  `file` split, and the cache directory size. All names start with
  `dex_demo_`.
