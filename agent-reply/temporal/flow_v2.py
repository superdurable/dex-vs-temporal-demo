import asyncio
from datetime import timedelta

from temporalio import activity, workflow

from settings import AUTO_SEND_AFTER


@activity.defn
async def draft_reply(ticket: str) -> str:
    return f"draft reply for {ticket}"


@activity.defn
async def send_reply(draft: str) -> str:
    return f"sent: {draft}"


@workflow.defn
class ReplyWorkflow:
    def __init__(self) -> None:
        self.approved = False

    @workflow.signal
    def approve_reply(self) -> None:
        self.approved = True

    @workflow.run
    async def run(self, ticket: str) -> str:
        draft = await workflow.execute_activity(
            draft_reply, ticket,
            start_to_close_timeout=timedelta(seconds=10),
        )
        if workflow.patched("auto-send"):
            try:
                await workflow.wait_condition(
                    lambda: self.approved, timeout=AUTO_SEND_AFTER
                )
            except asyncio.TimeoutError:
                pass
        else:
            await workflow.wait_condition(lambda: self.approved)
        return await workflow.execute_activity(
            send_reply, draft,
            start_to_close_timeout=timedelta(seconds=10),
        )
