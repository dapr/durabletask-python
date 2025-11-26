# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

import asyncio
import json
import logging
from typing import Any, Optional, Tuple

from durabletask import task, worker

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)
TEST_LOGGER = logging.getLogger("tests")
TEST_INSTANCE_ID = "abc123"
TEST_TASK_ID = 42


def test_activity_inputs():
    """Validates activity function input population"""

    def test_activity(ctx: task.ActivityContext, test_input: Any):
        # return all activity inputs back as the output
        return test_input, ctx.orchestration_id, ctx.task_id

    activity_input = "Hello, 世界!"
    executor, name = _get_activity_executor(test_activity)
    result = asyncio.run(
        executor.execute(TEST_INSTANCE_ID, name, TEST_TASK_ID, json.dumps(activity_input))
    )
    assert result is not None

    result_input, result_orchestration_id, result_task_id = json.loads(result)
    assert activity_input == result_input
    assert TEST_INSTANCE_ID == result_orchestration_id
    assert TEST_TASK_ID == result_task_id


def test_activity_not_registered():
    def test_activity(ctx: task.ActivityContext, _):
        pass  # not used

    executor, _ = _get_activity_executor(test_activity)

    caught_exception: Optional[Exception] = None
    try:
        asyncio.run(executor.execute(TEST_INSTANCE_ID, "Bogus", TEST_TASK_ID, None))
    except Exception as ex:
        caught_exception = ex

    assert type(caught_exception) is worker.ActivityNotRegisteredError
    assert "Bogus" in str(caught_exception)


def _get_activity_executor(fn: task.Activity) -> Tuple[worker._ActivityExecutor, str]:
    registry = worker._Registry()
    name = registry.add_activity(fn)
    executor = worker._ActivityExecutor(registry, TEST_LOGGER)
    return executor, name


def test_async_activity_basic():
    """Validates basic async activity execution"""

    async def async_activity(ctx: task.ActivityContext, test_input: str):
        # Simple async activity that returns modified input
        return f"async:{test_input}"

    activity_input = "test"
    executor, name = _get_activity_executor(async_activity)

    # Run the async executor
    result = asyncio.run(
        executor.execute(TEST_INSTANCE_ID, name, TEST_TASK_ID, json.dumps(activity_input))
    )
    assert result is not None

    result_output = json.loads(result)
    assert result_output == "async:test"


def test_async_activity_with_input():
    """Validates async activity with complex input/output"""

    async def async_activity(ctx: task.ActivityContext, test_input: dict):
        # Return all activity inputs back as the output
        return {
            "input": test_input,
            "orchestration_id": ctx.orchestration_id,
            "task_id": ctx.task_id,
            "processed": True,
        }

    activity_input = {"key": "value", "number": 42}
    executor, name = _get_activity_executor(async_activity)
    result = asyncio.run(
        executor.execute(TEST_INSTANCE_ID, name, TEST_TASK_ID, json.dumps(activity_input))
    )
    assert result is not None

    result_data = json.loads(result)
    assert result_data["input"] == activity_input
    assert result_data["orchestration_id"] == TEST_INSTANCE_ID
    assert result_data["task_id"] == TEST_TASK_ID
    assert result_data["processed"] is True


def test_async_activity_with_await():
    """Validates async activity that performs async I/O"""

    async def async_activity_with_io(ctx: task.ActivityContext, delay: float):
        # Simulate async I/O operation
        await asyncio.sleep(delay)
        return f"completed after {delay}s"

    activity_input = 0.01  # 10ms delay
    executor, name = _get_activity_executor(async_activity_with_io)
    result = asyncio.run(
        executor.execute(TEST_INSTANCE_ID, name, TEST_TASK_ID, json.dumps(activity_input))
    )
    assert result is not None

    result_output = json.loads(result)
    assert result_output == "completed after 0.01s"


def test_mixed_sync_async_activities():
    """Validates that sync and async activities work together"""

    def sync_activity(ctx: task.ActivityContext, test_input: str):
        return f"sync:{test_input}"

    async def async_activity(ctx: task.ActivityContext, test_input: str):
        return f"async:{test_input}"

    registry = worker._Registry()
    sync_name = registry.add_activity(sync_activity)
    async_name = registry.add_activity(async_activity)
    executor = worker._ActivityExecutor(registry, TEST_LOGGER)

    activity_input = "test"

    # Execute sync activity
    sync_result = asyncio.run(
        executor.execute(TEST_INSTANCE_ID, sync_name, TEST_TASK_ID, json.dumps(activity_input))
    )
    assert json.loads(sync_result) == "sync:test"

    # Execute async activity
    async_result = asyncio.run(
        executor.execute(TEST_INSTANCE_ID, async_name, TEST_TASK_ID + 1, json.dumps(activity_input))
    )
    assert json.loads(async_result) == "async:test"
