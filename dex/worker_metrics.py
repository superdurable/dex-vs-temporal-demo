"""Memory experiment instrumentation for the Worker, on only when DEMO_METRICS_BIND is set.

The Dex Python SDK exports no metrics and no BlobCache hit or miss counts.
This module counts the BlobCache calls that the SDK makes through the public
BlobCache protocol, and serves them in the Prometheus text format together
with the process RSS, the container's cgroup memory split, and the size of
the BlobCache directory. Every metric name is this demo's own.
"""

from __future__ import annotations

import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from dex import BlobCache, BlobCacheConfig

CGROUP_MEMORY_STAT = Path("/sys/fs/cgroup/memory.stat")
CGROUP_MEMORY_STAT_FIELDS = ("anon", "file", "active_file", "inactive_file", "shmem")
PROCESS_STATUS_FIELDS = {"VmRSS": "rss", "VmHWM": "rss_peak"}


class CountingBlobCache:
    """Delegates to an open BlobCache and counts each call; stores nothing itself."""

    def __init__(self, cache: BlobCache, directory: Path) -> None:
        self._cache = cache
        self.directory = directory
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.puts = 0
        self.puts_admitted = 0
        self.deletes = 0

    @property
    def config(self) -> BlobCacheConfig:
        return self._cache.config

    def get(self, blob_id: str) -> bytes | None:
        payload = self._cache.get(blob_id)
        with self._lock:
            if payload is None:
                self.misses += 1
            else:
                self.hits += 1
        return payload

    def put(self, blob_id: str, payload: bytes) -> bool:
        admitted = self._cache.put(blob_id, payload)
        with self._lock:
            self.puts += 1
            self.puts_admitted += int(admitted)
        return admitted

    def delete(self, blob_id: str) -> None:
        self._cache.delete(blob_id)
        with self._lock:
            self.deletes += 1

    def delete_all(self) -> None:
        self._cache.delete_all()

    def close(self) -> None:
        self._cache.close()


def directory_bytes(directory: Path) -> int:
    total = 0
    for root, _, files in os.walk(directory):
        for name in files:
            try:
                total += os.stat(os.path.join(root, name)).st_size
            except OSError:
                pass
    return total


def read_process_status_bytes() -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        lines = Path("/proc/self/status").read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for line in lines:
        key, _, rest = line.partition(":")
        if key in PROCESS_STATUS_FIELDS:
            values[PROCESS_STATUS_FIELDS[key]] = int(rest.split()[0]) * 1024
    return values


def read_cgroup_memory_bytes() -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        lines = CGROUP_MEMORY_STAT.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for line in lines:
        key, _, value = line.partition(" ")
        if key in CGROUP_MEMORY_STAT_FIELDS:
            values[key] = int(value)
    return values


def render_metrics(cache: CountingBlobCache) -> str:
    lines = [
        f"dex_demo_blob_cache_hits {cache.hits}",
        f"dex_demo_blob_cache_misses {cache.misses}",
        f"dex_demo_blob_cache_puts {cache.puts}",
        f"dex_demo_blob_cache_puts_admitted {cache.puts_admitted}",
        f"dex_demo_blob_cache_deletes {cache.deletes}",
        f"dex_demo_blob_cache_directory_bytes {directory_bytes(cache.directory)}",
        f"dex_demo_blob_cache_max_bytes {cache.config.max_bytes}",
    ]
    for key, value in read_process_status_bytes().items():
        lines.append(f"dex_demo_process_{key}_bytes {value}")
    for key, value in read_cgroup_memory_bytes().items():
        lines.append(f"dex_demo_cgroup_{key}_bytes {value}")
    return "\n".join(lines) + "\n"


def serve_metrics(bind_address: str, cache: CountingBlobCache) -> ThreadingHTTPServer:
    """Serve GET /metrics on host:port from a daemon thread; return the server."""
    host, _, port = bind_address.rpartition(":")

    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 (http.server naming)
            body = render_metrics(cache).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer((host or "0.0.0.0", int(port)), MetricsHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
