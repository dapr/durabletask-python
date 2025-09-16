import os
import time
from typing import List

from durabletask.client import TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker


def get_work_items(_, __) -> List[str]:
    return ["len_5", "len__6", "len___7", "len____8", "len_____9"]


def process_work_item(_, item: str) -> int:
    return len(item)


async def async_fanout_fanin(ctx, _):
    work_items = await ctx.activity("get_work_items")
    tasks = [ctx.activity("process_work_item", input=item) for item in work_items]
    results = await ctx.when_all(tasks)
    return {
        "work_items": work_items,
        "results": results,
        "total": sum(results),
    }


def main():
    os.environ.setdefault("DURABLETASK_GRPC_ENDPOINT", "localhost:4001")

    with TaskHubGrpcWorker() as worker:
        worker.add_activity(get_work_items)
        worker.add_activity(process_work_item)
        worker.add_async_orchestrator(
            async_fanout_fanin, name="async_fanout_fanin", sandbox_mode="off"
        )
        worker.start()
        worker.wait_for_ready(timeout=5)

        client = TaskHubGrpcClient()
        instance_id = client.schedule_new_orchestration("async_fanout_fanin")
        print("Started:", instance_id)
        state = client.wait_for_orchestration_completion(instance_id, timeout=60)
        if state:
            state.raise_if_failed()
            print("Output:", state.serialized_output)
        time.sleep(1)


if __name__ == "__main__":
    main()
