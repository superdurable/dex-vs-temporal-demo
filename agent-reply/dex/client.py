"""Start, approve, and inspect ReplyFlow runs.

  start <flow-id> <ticket>      start a Flow
  approve <flow-id>             invoke the approve_reply RPC
  result <flow-id> [seconds]    wait for the result (default 120 s) and print it
  status <flow-id>              print the Flow status
"""

import asyncio
import sys
import time
from datetime import timedelta

from dex import AsyncClient, ClientOptions, LongPollTimeoutError

from app import FLOW, REGISTRY, SERVER_ADDRESS, open_process_blob_cache


async def print_result(client: AsyncClient, flow_id: str, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while True:
        remaining = max(1, int(deadline - time.monotonic()))
        try:
            result = await client.wait_for_flow(flow_id, timedelta(seconds=remaining))
            break
        except LongPollTimeoutError:
            if time.monotonic() >= deadline:
                print(f"{flow_id}: no result within {seconds:g} s")
                return
    outputs = [completion.decode(str) for completion in result.completions]
    print(f"result of {flow_id}: {result.status.name} {outputs}")


async def main(command: str, flow_id: str, *args: str) -> None:
    cache = open_process_blob_cache("client")
    client = AsyncClient(REGISTRY, cache, ClientOptions(server_address=SERVER_ADDRESS))
    try:
        if command == "start":
            await client.start_flow(FLOW, flow_id, args[0])
            print(f"started {flow_id}")
        elif command == "approve":
            await client.invoke_rpc(FLOW.approve_reply, flow_id)
            print(f"invoked approve_reply on {flow_id}")
        elif command == "result":
            await print_result(client, flow_id, float(args[0]) if args else 120)
        elif command == "status":
            info = await client.describe_flow(flow_id)
            print(f"{flow_id}: {info.status.name}")
        else:
            sys.exit(f"unknown command {command}")
    finally:
        await client.close()
        cache.close()


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
