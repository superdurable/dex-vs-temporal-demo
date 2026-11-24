from dex import (
    Channel, Context, Flow, PersistenceSchema, Step, StepDecision,
    StepList, Wait, go_to, graceful_complete, rpc,
)

APPROVE = Channel("approve", type(None))


class DraftReplyStep(Step[str]):
    def execute(self, context: Context, input: str) -> StepDecision:
        return go_to(SendReplyStep, f"draft reply for {input}")


class SendReplyStep(Step[str]):
    def wait_for(self, context: Context, input: str) -> Wait:
        return Wait.until(APPROVE.for_one())

    def execute(self, context: Context, input: str) -> StepDecision:
        return graceful_complete(f"sent: {input}")


class ReplyFlow(Flow[str]):
    def __init__(self) -> None:
        self.draft_reply = DraftReplyStep()
        self.send_reply = SendReplyStep()

    def get_steps(self) -> StepList[str]:
        return StepList.start_step(self.draft_reply).other_steps(
            self.send_reply
        )

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(APPROVE)

    @rpc
    def approve_reply(self, context: Context) -> None:
        APPROVE.publish(context, None)
