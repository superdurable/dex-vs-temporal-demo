"""Samples one worker container once per second into a CSV.

Columns: epoch, container state, restart count, OOMKilled, exit code,
memory usage (raw cgroup usage, and usage minus inactive_file as docker stats
shows it), memory limit, CPU percent (100 = one full CPU), and optional
Prometheus metrics scraped from the worker. It also writes the Docker event
stream (start, die, oom, restart) of the container to an events file.

It reads the Docker Engine API over the Docker socket, so it works for any
engine's worker image. It runs until SIGTERM or SIGINT.

Usage:
  python docker_sampler.py --container dvt-temporal-worker --out stats.csv \
    --events-out events.jsonl [--metrics-url http://127.0.0.1:9464/metrics \
    --metric temporal_sticky_cache_size --metric ...]
"""

from __future__ import annotations

import argparse
import csv
import http.client
import json
import signal
import socket
import subprocess
import threading
import time
import urllib.request
from pathlib import Path


class UnixSocketHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str, timeout: float) -> None:
        super().__init__("localhost", timeout=timeout)
        self._socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._socket_path)


def resolve_docker_socket(explicit_path: str) -> str:
    if explicit_path:
        return explicit_path
    host = subprocess.run(
        ["docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    if not host.startswith("unix://"):
        raise SystemExit(f"unsupported Docker host {host}; pass --docker-socket")
    return host.removeprefix("unix://")


def docker_api_get(socket_path: str, path: str) -> dict | None:
    connection = UnixSocketHTTPConnection(socket_path, timeout=5.0)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        body = response.read()
        if response.status != 200:
            return None
        return json.loads(body)
    finally:
        connection.close()


def scrape_metrics(url: str, metric_names: list[str]) -> dict[str, float | None]:
    values: dict[str, float | None] = {name: None for name in metric_names}
    try:
        with urllib.request.urlopen(url, timeout=0.8) as response:
            text = response.read().decode("utf-8", "replace")
    except Exception:
        return values
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        name_part, _, value_part = line.rpartition(" ")
        metric_name = name_part.split("{", 1)[0]
        if metric_name in values:
            try:
                values[metric_name] = (values[metric_name] or 0.0) + float(value_part)
            except ValueError:
                pass
    return values


def follow_docker_events(container: str, events_path: Path) -> subprocess.Popen:
    with events_path.open("w", encoding="utf-8") as events_file:
        return subprocess.Popen(
            ["docker", "events", "--filter", f"container={container}", "--format", "{{json .}}"],
            stdout=events_file, stderr=subprocess.DEVNULL,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--container", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--events-out", required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--docker-socket", default="")
    parser.add_argument("--metrics-url", default="")
    parser.add_argument("--metric", action="append", default=[])
    args = parser.parse_args()

    socket_path = resolve_docker_socket(args.docker_socket)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    events_process = follow_docker_events(args.container, Path(args.events_out))

    header = [
        "epoch", "status", "restart_count", "oom_killed", "exit_code", "started_at",
        "mem_usage_bytes", "mem_working_set_bytes", "mem_limit_bytes", "cpu_percent",
    ] + args.metric
    previous_cpu: tuple[int, int] | None = None
    with open(args.out, "w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerow(header)
        next_tick = time.monotonic()
        while not stop.is_set():
            epoch = time.time()
            state: dict = {}
            inspect = docker_api_get(socket_path, f"/containers/{args.container}/json")
            restart_count = ""
            if inspect:
                state = inspect.get("State", {})
                restart_count = inspect.get("RestartCount", "")
            mem_usage = mem_working_set = mem_limit = cpu_percent = ""
            if state.get("Running"):
                stats = docker_api_get(
                    socket_path, f"/containers/{args.container}/stats?stream=false&one-shot=true"
                )
                if stats:
                    memory = stats.get("memory_stats", {})
                    usage = memory.get("usage")
                    inactive_file = memory.get("stats", {}).get("inactive_file", 0)
                    if usage is not None:
                        mem_usage = usage
                        mem_working_set = usage - inactive_file
                    mem_limit = memory.get("limit", "")
                    cpu = stats.get("cpu_stats", {})
                    total = cpu.get("cpu_usage", {}).get("total_usage")
                    system = cpu.get("system_cpu_usage")
                    online = cpu.get("online_cpus") or 1
                    if total is not None and system is not None:
                        if previous_cpu and system > previous_cpu[1] and total >= previous_cpu[0]:
                            cpu_percent = round(
                                (total - previous_cpu[0]) / (system - previous_cpu[1]) * online * 100, 1
                            )
                        previous_cpu = (total, system)
            else:
                previous_cpu = None
            metrics = scrape_metrics(args.metrics_url, args.metric) if args.metrics_url else {}
            writer.writerow(
                [
                    f"{epoch:.3f}", state.get("Status", "missing"), restart_count,
                    state.get("OOMKilled", ""), state.get("ExitCode", ""), state.get("StartedAt", ""),
                    mem_usage, mem_working_set, mem_limit, cpu_percent,
                ]
                + ["" if metrics.get(name) is None else metrics[name] for name in args.metric]
            )
            output.flush()
            next_tick += args.interval
            stop.wait(max(0.0, next_tick - time.monotonic()))
    events_process.terminate()
    events_process.wait(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
