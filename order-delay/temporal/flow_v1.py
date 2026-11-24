from datetime import timedelta

from temporalio import activity, workflow

from settings import DELAY


@activity.defn
async def confirm_order(order_id: str) -> str:
    return f"order {order_id} confirmed"


@activity.defn
async def send_to_kitchen(order: str) -> str:
    return f"{order}, sent to kitchen"


@workflow.defn
class OrderWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> str:
        order = await workflow.execute_activity(
            confirm_order, order_id,
            start_to_close_timeout=timedelta(seconds=10),
        )
        await workflow.sleep(DELAY)
        return await workflow.execute_activity(
            send_to_kitchen, order,
            start_to_close_timeout=timedelta(seconds=10),
        )
