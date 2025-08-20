import os
import time

from durabletask.client import TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker


async def await_event(ctx, _):
    val = await ctx.wait_for_external_event("greeting")
    return f"Got: {val}"


def main():
    os.environ.setdefault("DURABLETASK_GRPC_ENDPOINT", "localhost:50001")

    with TaskHubGrpcWorker() as worker:
        worker.add_async_orchestrator(await_event, name="await_event", sandbox_mode="off")
        worker.start()
        worker.wait_for_ready(timeout=5)
        client = TaskHubGrpcClient()
        instance_id = client.schedule_new_orchestration("await_event", input=None)
        client.raise_orchestration_event(instance_id, "greeting", data="hello")
        state = client.wait_for_orchestration_completion(instance_id, timeout=30)
        if state:
            state.raise_if_failed()
            print("Output:", state.serialized_output)
        time.sleep(1)


if __name__ == "__main__":
    main()


