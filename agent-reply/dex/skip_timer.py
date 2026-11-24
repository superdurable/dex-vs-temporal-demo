"""Operator call used only by skip-timer.sh: make the auto-send Timer fire now.

Usage: skip_timer.py <flow-id>
Skips the Timer of SendReplyStep-1, the only Timer of its wait (index 0).
"""

import asyncio
import sys

from dex import AsyncClient, ClientOptions, StepExecutionId, TimerId

from app import REGISTRY, SERVER_ADDRESS, open_process_blob_cache


async def main(flow_id: str) -> None:
    cache = open_process_blob_cache("operator")
    client = AsyncClient(REGISTRY, cache, ClientOptions(server_address=SERVER_ADDRESS))
    try:
        await client.skip_timer(
            flow_id, StepExecutionId("SendReplyStep", 1), TimerId(condition_index=0)
        )
        print(f"skipped the Timer of SendReplyStep-1 on {flow_id}")
    finally:
        await client.close()
        cache.close()


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
