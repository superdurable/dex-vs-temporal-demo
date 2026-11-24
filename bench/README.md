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
| `shared/summarize.py` | any | Peak memory, OOM kills, restarts, seconds at the limit, CPU, latency percentiles per phase |
| `temporal/temporal_adapter.py` | Temporal | Adapter: workflow start, signal, history long-poll |
| `temporal/Dockerfile` | Temporal | Worker image `dvt-temporal-worker` |
| `../scripts/temporal/t3-memory.sh` | Temporal | One complete run: server, fill, rush, summary, cleanup |
| `dex/dex_adapter.py` | Dex | Adapter: Flow start, `post_message` RPC, Step-completion wait |
| `dex/Dockerfile` | Dex | Worker image `dvt-dex-worker` |
| `../scripts/dex/d3-memory.sh` | Dex | One complete run: server, fill, rush, summary, cleanup, server data deletion |
| `../dex/worker_metrics.py` | Dex | The demo Worker's own metrics: BlobCache call counts, RSS, cgroup memory split, BlobCache directory size |
| `results/<run>/` | both | The published data of each run in the root README |

## Workload

1. **Fill.** Create `N` conversations. Fill each with `M` exchanges: a
   customer message of `B` bytes and a mock AI reply of `B` bytes. The fill
   uses a separate worker container without a tight limit. It is then
   stopped, so all conversations are idle and no worker holds them.
2. **Rush.** Start a clean, limited worker container. `K` conversations
   come back at `R` conversations per second. Each sends `F` follow-ups.
   Each follow-up is posted `T` seconds after the previous reply. A
   conversation whose message gets no reply within 300 s sends no further
   follow-ups.

A reply's latency is the time from posting the customer message until the
reply is visible to a reader of the conversation.

Each run starts its own server and fills its own population, so runs do not
see each other's history or leftover work.

Defaults of both scripts. Every published run used them; the control runs
override only `CACHE_SIZE` and `MEM_LIMIT`.

| Parameter | Value |
| --- | --- |
| `CONVERSATIONS` (N), `EXCHANGES` (M), `MESSAGE_BYTES` (B) | 1,200, 40, 8,192 |
| `FILL_CONCURRENCY` | 200 conversations at a time |
| `RETURNING` (K), `RATE` (R), `FOLLOW_UPS` (F), `THINK_SECONDS` (T) | 1,000, 4 per second, 3, 10 s |
| `SEED`, `BASELINE_SECONDS`, `COOLDOWN_SECONDS`, `MAX_RUSH_SECONDS` | 1, 15, 20, 1,500 |
| `MEM_LIMIT`, `CPUS` | `512m`, 2 |
| Temporal: `CACHE_SIZE`, `WF_TASK_SLOTS`, `ACTIVITY_SLOTS` | SDK default (1,000), 100, 100 |
| Temporal fill worker | 6 GB, 4 CPUs, cache `FILL_CONCURRENCY + 100` |
| Dex: `TASKS`, `BLOB_CACHE_BYTES` | 32, 1 GiB |
| Dex fill Worker | 4 GB, 4 CPUs, 64 task slots |

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

- Each script starts its own server and refuses to use one that is already
  listening on its port. It stops only the processes it started.
- Its containers are named `dvt-mem-<run ID>-...` and carry the label
  `dvt-mem-run=<run ID>`. It removes them when it exits.
- Cleanup runs on every exit path: success, failure, SIGINT, SIGTERM, and
  the time limit `MAX_RUN_SECONDS` (default 3,600 s).
- `cleanup` removes leftover containers that carry the `dvt-mem-run` label.

Server data:

- The Temporal dev server keeps its data in memory. It goes away when the
  script stops the server.
- `d3-memory.sh` refuses to fill with less than `MIN_FREE_GB` (default 80)
  GB free disk. After the summary it deletes the server data, unless
  `KEEP_SERVER_DATA=1`. Every reply rewrites the conversation's growing
  transcript chunk, so a full 8 KiB fill writes tens of GB.

## Outputs per run

`parameters.txt`, `stats.csv` (sampler), `events.jsonl` (Docker events:
start, oom, die), `rush-latency.csv`, `fill-latency.csv`, `driver.log`,
`fill-driver.log`, `worker.log`, `server.log` (Temporal),
`server-data-size.txt` (Dex), `summary.md`, `summary.json`.

`results/<run>/` publishes `parameters.txt`, `stats.csv`, `events.jsonl`,
`rush-latency.csv`, `summary.md`, and `summary.json`, plus
`server-data-size.txt` for Dex.

## Measures

- **Memory.** `mem_working_set_bytes` is the container's memory usage minus
  its inactive file cache, the figure `docker stats` shows.
  `mem_usage_bytes` is the raw cgroup usage. The Dex Worker's raw usage
  includes the page cache of its on-disk BlobCache files.
- **OOM kills** come from the Docker event stream (`oom` events). The exit
  code of each `die` event is listed; 137 is a SIGKILL.
- **Seconds at the limit** counts the one-second samples whose working set
  is at least 95% of the container limit.
- **Messages.** Planned is `K x F`. Posted counts the follow-ups the driver
  sent. Posted without a reply counts the posted follow-ups that got no
  reply within 300 s or failed.

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
3. **Run script.** A script under `scripts/<engine>/` that starts the server
   on the host, builds the image, starts the worker container with the same
   `--memory`, `--memory-swap`, `--cpus`, and `--restart on-failure` flags,
   runs `docker_sampler.py` against it, and runs `load_driver.py fill` and
   `rush` with the same parameters. Pass the engine's worker metrics to the
   sampler with `--metrics-url` and `--metric` if it exports any.

Keep these identical across engines for a fair comparison: `N`, `M`, `B`,
`K`, `R`, `F`, `T`, the seed, the container limits, the host, and the
reply-visible definition of latency.

## Temporal specifics

- **Reply visible.** The adapter long-polls the workflow history until the
  workflow task that appends the reply has completed. It never sends a
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
  one Client Attribute read. Dex Server answers both; the Worker is not
  involved.
- **Posting** is the `post_message` RPC, which runs on the Worker. A
  Temporal signal is accepted by the server alone.
- **Worker ID.** Each Worker start appends its start time to the ID
  (`DEMO_WORKER_ID_PER_START=1`), because a Worker ID must change after a
  restart, such as a container restart after an OOM kill.
- **BlobCache.** On the container's writable layer under `/data`, never
  tmpfs. Its byte limit is `BLOB_CACHE_BYTES` (default 1 GiB). The page
  cache of its files counts toward the container's raw memory usage.
- **Metrics.** The SDK exports none. `dex/worker_metrics.py` counts the
  BlobCache calls the SDK makes and reports RSS, the cgroup `anon` and
  `file` split, and the cache directory size. All names start with
  `dex_demo_`.
