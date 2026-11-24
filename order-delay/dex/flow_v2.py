from dex import (
    Channel, Context, Flow, PersistenceSchema, Step, StepDecision,
    StepList, Timer, Wait, go_to, graceful_complete, rpc,
)

from settings import DELAY

SEND_NOW = Channel("send-now", type(None))


class ConfirmOrderStep(Step[str]):
    def execute(self, context: Context, input: str) -> StepDecision:
        return go_to(SendToKitchenStep, f"order {input} confirmed")


class SendToKitchenStep(Step[str]):
    def wait_for(self, context: Context, input: str) -> Wait:
        return Wait.any_of(Timer.by_duration(DELAY), SEND_NOW.for_one())

    def execute(self, context: Context, input: str) -> StepDecision:
        return graceful_complete(f"{input}, sent to kitchen")


class OrderFlow(Flow[str]):
    def __init__(self) -> None:
        self.confirm_order = ConfirmOrderStep()
        self.send_to_kitchen = SendToKitchenStep()

    def get_steps(self) -> StepList[str]:
        return StepList.start_step(self.confirm_order).other_steps(
            self.send_to_kitchen
        )

    def get_persistence_schema(self) -> PersistenceSchema:
        return PersistenceSchema.of(SEND_NOW)

    @rpc
    def send_order_now(self, context: Context) -> None:
        SEND_NOW.publish(context, None)
