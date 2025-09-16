import threading
import time
from collections import namedtuple
from dataclasses import dataclass
from datetime import timedelta

from durabletask.client import TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker


@dataclass
class Order:
    Cost: float
    Product: str
    Quantity: int

    def __str__(self):
        return f"{self.Product} ({self.Quantity})"


def send_approval_request(_, order: Order) -> None:
    time.sleep(1)
    print(f"*** Sending approval request for order: {order}")


def place_order(_, order: Order) -> None:
    print(f"*** Placing order: {order}")


async def purchase_order_workflow(ctx, order: Order):
    if order.Cost < 1000:
        return "Auto-approved"

    await ctx.activity("send_approval_request", input=order)

    approval = ctx.wait_for_external_event("approval_received")
    timeout = ctx.sleep(timedelta(seconds=24 * 60 * 60))
    winner = await ctx.when_any([approval, timeout])
    if winner == timeout:
        return "Cancelled"

    await ctx.activity("place_order", input=order)
    approval_details = approval.get_result()
    return f"Approved by '{approval_details.approver}'"


def main():
    with TaskHubGrpcWorker() as worker:
        worker.add_activity(send_approval_request)
        worker.add_activity(place_order)
        worker.add_async_orchestrator(
            purchase_order_workflow, name="async_purchase_order"
        )
        worker.start()
        worker.wait_for_ready(timeout=5)

        client = TaskHubGrpcClient()
        order = Order(2000, "MyProduct", 1)
        instance_id = client.schedule_new_orchestration(
            "async_purchase_order", input=order
        )

        def prompt_for_approval():
            input("Press [ENTER] to approve the order...\n")
            approval_event = namedtuple("Approval", ["approver"])("Me")
            client.raise_orchestration_event(
                instance_id, "approval_received", data=approval_event
            )

        threading.Thread(target=prompt_for_approval, daemon=True).start()
        state = client.wait_for_orchestration_completion(instance_id, timeout=120)
        if state:
            state.raise_if_failed()
            print("Output:", state.serialized_output)
        time.sleep(1)


if __name__ == "__main__":
    main()
