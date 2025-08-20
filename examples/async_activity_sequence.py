import os
import time

from durabletask.client import TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker


def to_upper(_, text: str) -> str:
    return text.upper()


async def async_sequence(ctx, word: str):
    a = await ctx.activity("to_upper", input=word)
    await ctx.sleep(1)
    b = await ctx.activity("to_upper", input=a + "!")
    return b


def main():
    # Point to your sidecar, or set DURABLETASK_GRPC_ENDPOINT in env
    os.environ.setdefault("DURABLETASK_GRPC_ENDPOINT", "localhost:4001")

    with TaskHubGrpcWorker() as worker:
        worker.add_activity(to_upper)
        worker.add_async_orchestrator(async_sequence, name="async_sequence", sandbox_mode="off")
        worker.start()
        worker.wait_for_ready(timeout=5)

        client = TaskHubGrpcClient()
        instance_id = client.schedule_new_orchestration("async_sequence", input="hello")
        print(f"Started instance: {instance_id}")

        state = client.wait_for_orchestration_completion(instance_id, timeout=60)
        if state:
            if state.failure_details:
                print("Failure:", state.failure_details.error_type, state.failure_details.message)
                print("Stack:\n", state.failure_details.stack_trace)
            state.raise_if_failed()
            print(f"Completed with output: {state.serialized_output}")

        # Give worker a moment to flush logs before shutdown
        time.sleep(1)


if __name__ == "__main__":
    main()


