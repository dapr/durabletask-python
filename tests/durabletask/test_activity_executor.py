# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

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
    result = executor.execute(TEST_INSTANCE_ID, name, TEST_TASK_ID, json.dumps(activity_input))
    assert result is not None

    result_input, result_orchestration_id, result_task_id = json.loads(result)
    assert activity_input == result_input
    assert TEST_INSTANCE_ID == result_orchestration_id
    assert TEST_TASK_ID == result_task_id


def test_activity_trace_context_passthrough():
    """Validate ActivityContext exposes trace fields (populated by worker from request)."""

    # We'll simulate that the worker populates ActivityContext.trace_parent/state before invoking
    def test_activity(ctx: task.ActivityContext, _):
        return ctx.trace_parent, ctx.trace_state, ctx.workflow_span_id

    executor, name = _get_activity_executor(test_activity)

    # Call execute with injected trace context and assert activity receives it
    result = executor.execute(
        TEST_INSTANCE_ID,
        name,
        TEST_TASK_ID,
        json.dumps(None),
        trace_parent="00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01",
        trace_state="tenant=contoso",
        workflow_span_id="bbbbbbbbbbbbbbbb",
    )
    assert result is not None
    tp, ts, sid = json.loads(result)
    assert tp.endswith("-bbbbbbbbbbbbbbbb-01")
    assert ts == "tenant=contoso"
    assert sid == "bbbbbbbbbbbbbbbb"


def test_activity_not_registered():
    def test_activity(ctx: task.ActivityContext, _):
        pass  # not used

    executor, _ = _get_activity_executor(test_activity)

    caught_exception: Optional[Exception] = None
    try:
        executor.execute(TEST_INSTANCE_ID, "Bogus", TEST_TASK_ID, None)
    except Exception as ex:
        caught_exception = ex

    assert type(caught_exception) is worker.ActivityNotRegisteredError
    assert "Bogus" in str(caught_exception)


def test_activity_attempt_temp_hack_no_effect_in_direct_executor():
    """
    Temporary attempt hack is applied by worker scheduling path, not direct executor calls.
    Direct executor usage should leave ctx.attempt as None.
    """

    def probe_attempt(ctx: task.ActivityContext, _):
        return {"attempt": ctx.attempt}

    executor, name = _get_activity_executor(probe_attempt)
    # Provide a JSON-encoded null payload to get a valid StringValue in executor path
    result = executor.execute(TEST_INSTANCE_ID, name, TEST_TASK_ID, json.dumps(None))
    assert result is not None
    parsed = json.loads(result)
    assert isinstance(parsed, dict)
    assert parsed.get("attempt") is None


def _get_activity_executor(fn: task.Activity) -> Tuple[worker._ActivityExecutor, str]:
    registry = worker._Registry()
    name = registry.add_activity(fn)
    executor = worker._ActivityExecutor(registry, TEST_LOGGER)
    return executor, name
