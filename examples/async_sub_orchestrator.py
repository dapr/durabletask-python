import os
import time

from durabletask.client import TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker


def identity(_, v: int) -> int:
    return v


async def child(ctx, x: int):
    y = await ctx.activity("identity", input=x)
    return y + 1


async def parent(ctx, x: int):
    y = await ctx.sub_orchestrator(child, input=x)
    return y * 2


def main():
    os.environ.setdefault("DURABLETASK_GRPC_ENDPOINT", "localhost:50001")

    with TaskHubGrpcWorker() as worker:
        worker.add_activity(identity)
        worker.add_async_orchestrator(child, name="child", sandbox_mode="off")
        worker.add_async_orchestrator(parent, name="parent", sandbox_mode="off")
        
        print("Starting worker")
        worker.start()
        worker.wait_for_ready(timeout=5)
        print("Worker ready")

        client = TaskHubGrpcClient()
        instance_id = client.schedule_new_orchestration("parent", input=3)
        print("Started:", instance_id)
        state = client.wait_for_orchestration_completion(instance_id, timeout=30)
        if state:
            state.raise_if_failed()
            print("Output:", state.serialized_output)
        time.sleep(1)


if __name__ == "__main__":
    main()


