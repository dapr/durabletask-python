import os
import time

from durabletask.aio import AsyncWorkflowContext
from durabletask.client import TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker


def to_upper(_, text: str) -> str:
    """Activity function that converts text to uppercase."""
    return text.upper()


async def async_sequence_workflow(ctx: AsyncWorkflowContext, word: str) -> str:
    """Async workflow that processes a word through multiple activities.

    Args:
        ctx: The async workflow context
        word: Input word to process

    Returns:
        Processed word in uppercase with exclamation
    """
    a = await ctx.call_activity(to_upper, input=word)
    await ctx.sleep(1.0)
    b = await ctx.call_activity(to_upper, input=a + "!")
    return b


def main():
    # Point to your sidecar via DURABLETASK_GRPC_ENDPOINT (uses default if unset)
    os.environ.setdefault("DURABLETASK_GRPC_ENDPOINT", "localhost:4001")

    with TaskHubGrpcWorker() as worker:
        worker.add_activity(to_upper)
        worker.add_orchestrator(async_sequence_workflow)
        worker.start()
        worker.wait_for_ready(timeout=5)

        client = TaskHubGrpcClient()
        instance_id = client.schedule_new_orchestration(async_sequence_workflow, input="hello")
        print(f"Started instance: {instance_id}")

        state = client.wait_for_orchestration_completion(instance_id, timeout=60)
        if state:
            if state.failure_details:
                print(
                    "Failure:",
                    state.failure_details.error_type,
                    state.failure_details.message,
                )
                print("Stack:\n", state.failure_details.stack_trace)
            state.raise_if_failed()
            print(f"Completed with output: {state.serialized_output}")

        # Give worker a moment to flush logs before shutdown
        time.sleep(1)


if __name__ == "__main__":
    main()
