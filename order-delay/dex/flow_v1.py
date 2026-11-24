from dex import (
    Context, Flow, Step, StepDecision, StepList, Timer, Wait,
    go_to, graceful_complete,
)

from settings import DELAY


class ConfirmOrderStep(Step[str]):
    def execute(self, context: Context, input: str) -> StepDecision:
        return go_to(SendToKitchenStep, f"order {input} confirmed")


class SendToKitchenStep(Step[str]):
    def wait_for(self, context: Context, input: str) -> Wait:
        return Wait.until(Timer.by_duration(DELAY))

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
