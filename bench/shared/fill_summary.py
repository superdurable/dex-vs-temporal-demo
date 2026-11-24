"""Summarizes the fill of one run: per-conversation exchange counts and phase wall-clock times.

The fill CSV is not published; this summary is. A conversation is complete
when every exchange from 1 to M has an observed reply.

Usage:
  python fill_summary.py --fill-latency fill-latency.csv --fill-log fill-driver.log \
    --rush-log driver.log --conversations 1200 --exchanges 40
"""

from __future__ import annotations

import argparse
import csv
import re
from collections import defaultdict
from pathlib import Path

ELAPSED_PATTERN = re.compile(r"elapsed=([0-9.]+)s")


def finished_elapsed_seconds(log_path: Path, marker: str) -> str:
    """The elapsed= value of the log's "<marker> finished" line, or "unknown"."""
    if not log_path.exists():
        return "unknown"
    for line in log_path.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{marker} finished"):
            match = ELAPSED_PATTERN.search(line)
            if match:
                return match.group(1)
    return "unknown"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--fill-latency", required=True)
    parser.add_argument("--fill-log", required=True)
    parser.add_argument("--rush-log", required=True)
    parser.add_argument("--conversations", type=int, required=True)
    parser.add_argument("--exchanges", type=int, required=True)
    args = parser.parse_args()

    replied_indexes: dict[str, set[int]] = defaultdict(set)
    ok_rows = error_rows = 0
    with open(args.fill_latency, newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            if row["status"] == "ok":
                ok_rows += 1
                replied_indexes[row["conversation_id"]].add(int(row["message_index"]))
            else:
                error_rows += 1
    confirmed = []
    for conversation_id in replied_indexes:
        count = 0
        while count + 1 in replied_indexes[conversation_id]:
            count += 1
        confirmed.append(count)
    confirmed += [0] * (args.conversations - len(confirmed))
    complete = sum(1 for count in confirmed if count >= args.exchanges)

    print(f"conversations_planned={args.conversations}")
    print(f"exchanges_per_conversation_planned={args.exchanges}")
    print(f"conversations_with_all_exchanges_confirmed={complete}")
    print(f"min_confirmed_exchanges_per_conversation={min(confirmed) if confirmed else 0}")
    print(f"max_confirmed_exchanges_per_conversation={max(confirmed) if confirmed else 0}")
    print(f"fill_exchanges_with_observed_reply={ok_rows}")
    print(f"fill_exchanges_failed={error_rows}")
    print(f"fill_wall_clock_seconds={finished_elapsed_seconds(Path(args.fill_log), 'fill')}")
    print(f"rush_wall_clock_seconds={finished_elapsed_seconds(Path(args.rush_log), 'rush')}")
    return 0 if complete == args.conversations else 1


if __name__ == "__main__":
    raise SystemExit(main())
