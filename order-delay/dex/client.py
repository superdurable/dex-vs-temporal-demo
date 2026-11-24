"""Start, trigger, and wait for OrderFlow runs.

  start <flow-id> <order-id>   start a Flow and print its run ID
  send-now <flow-id>           invoke the send_order_now RPC (needs ORDER_CODE=v2)
  result <flow-id>             wait for the result and print it
  skip-timer <flow-id>         skip the Timer of SendToKitchenStep-1 (operator call)
"""

import asyncio
import sys
from datetime import timedelta

from dex import (
    AsyncClient,
    ClientOptions,
    LongPollTimeoutError,
    StepExecutionId,
    TimerId,
)

from app import FLOW, REGISTRY, SERVER_ADDRESS, open_process_blob_cache


async def wait_for_result(client: AsyncClient, flow_id: str) -> None:
    while True:
        try:
            result = await client.wait_for_flow(flow_id, timedelta(seconds=30))
            break
        except LongPollTimeoutError:
            continue
    outputs = [completion.decode(str) for completion in result.completions]
    print(f"result of {flow_id}: {result.status.name} {outputs}")


async def main(command: str, flow_id: str, *args: str) -> None:
    cache = open_process_blob_cache("client")
    client = AsyncClient(REGISTRY, cache, ClientOptions(server_address=SERVER_ADDRESS))
    try:
        if command == "start":
            run_id = await client.start_flow(FLOW, flow_id, args[0])
            print(f"started {flow_id} run {run_id}")
        elif command == "send-now":
            await client.invoke_rpc(FLOW.send_order_now, flow_id)
            print(f"invoked send_order_now on {flow_id}")
        elif command == "result":
            await wait_for_result(client, flow_id)
        elif command == "skip-timer":
            await client.skip_timer(
                flow_id,
                StepExecutionId("SendToKitchenStep", 1),
                TimerId(condition_index=0),
            )
            print(f"skipped the Timer of SendToKitchenStep-1 on {flow_id}")
        else:
            sys.exit(f"unknown command {command}")
    finally:
        await client.close()
        cache.close()


if __name__ == "__main__":
    asyncio.run(main(*sys.argv[1:]))
