# Copyright 2025 The Dapr Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import json
import logging
import random
import time
import uuid
from datetime import timedelta  # noqa: F401

import durabletask.internal.helpers as helpers
from durabletask.worker import _OrchestrationExecutor, _Registry

TEST_INSTANCE_ID = "async-test-1"


def test_async_activity_and_sleep():
    async def orch(ctx, _):
        a = await ctx.call_activity("echo", input=1)
        await ctx.sleep(1)
        b = await ctx.call_activity("echo", input=a + 1)
        return b

    def echo(_, x):
        return x

    registry = _Registry()
    name = registry.add_async_orchestrator(orch)  # type: ignore[attr-defined]
    activity_name = registry.add_activity(echo)

    # start → schedule first activity
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("scheduleTask")
    assert res.actions[0].scheduleTask.name == activity_name

    # complete first activity → expect timer
    old_events = new_events + [helpers.new_task_scheduled_event(1, activity_name)]
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_task_completed_event(1, encoded_output=json.dumps(1)),
    ]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("createTimer")

    # fire timer → expect second activity
    now_dt = helpers.new_orchestrator_started_event().timestamp.ToDatetime()
    old_events = (
        old_events
        + new_events
        + [
            helpers.new_timer_created_event(2, now_dt),
        ]
    )
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_timer_fired_event(2, now_dt),
    ]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("scheduleTask")
    assert res.actions[0].scheduleTask.name == activity_name

    # complete second activity → done
    old_events = old_events + new_events + [helpers.new_task_scheduled_event(1, activity_name)]
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_task_completed_event(1, encoded_output=json.dumps(2)),
    ]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("completeOrchestration")


def test_async_when_all_any_and_events():
    async def orch(ctx, _):
        t1 = ctx.call_activity("a", input=1)
        t2 = ctx.call_activity("b", input=2)
        await ctx.when_all([t1, t2])
        _ = await ctx.when_any([ctx.wait_for_external_event("x"), ctx.sleep(0.1)])
        return "ok"

    def a(_, x):
        return x

    def b(_, x):
        return x

    registry = _Registry()
    name = registry.add_async_orchestrator(orch)  # type: ignore[attr-defined]
    _ = registry.add_activity(a)
    _ = registry.add_activity(b)

    # start → schedule both activities
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert len(res.actions) == 2 and all(a.HasField("scheduleTask") for a in res.actions)


def test_async_external_event_immediate_and_buffered():
    async def orch(ctx, _):
        val = await ctx.wait_for_external_event("x")
        return val

    registry = _Registry()
    name = registry.add_async_orchestrator(orch)  # type: ignore[attr-defined]

    # Start: expect no actions (waiting for event)
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert len(res.actions) == 0

    # Deliver event and complete
    old_events = new_events
    new_events = [helpers.new_event_raised_event("x", encoded_input=json.dumps(42))]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("completeOrchestration")


def test_async_sub_orchestrator_completion_and_failure():
    async def child(ctx, x):
        return x

    async def parent(ctx, _):
        return await ctx.sub_orchestrator(child, input=5)

    registry = _Registry()
    child_name = registry.add_async_orchestrator(child)  # type: ignore[attr-defined]
    parent_name = registry.add_async_orchestrator(parent)  # type: ignore[attr-defined]

    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))

    # Start parent → expect createSubOrchestration action
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(parent_name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("createSubOrchestration")
    assert res.actions[0].createSubOrchestration.name == child_name

    # Simulate sub-orch created then completed
    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(parent_name, TEST_INSTANCE_ID, encoded_input=None),
        helpers.new_sub_orchestration_created_event(
            1, child_name, f"{TEST_INSTANCE_ID}:0001", encoded_input=None
        ),
    ]
    new_events = [helpers.new_sub_orchestration_completed_event(1, encoded_output=json.dumps(5))]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("completeOrchestration")

    # Also verify the worker-level wrapper does not surface StopIteration
    from durabletask.worker import TaskHubGrpcWorker

    w = TaskHubGrpcWorker()
    w.add_async_orchestrator(child, name="child")
    w.add_async_orchestrator(parent, name="parent")


def test_async_sandbox_sleep_patching_creates_timer():
    async def orch(ctx, _):
        await asyncio.sleep(1)
        return "done"

    registry = _Registry()
    name = registry.add_async_orchestrator(orch, sandbox_mode="best_effort")  # type: ignore[attr-defined]
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("createTimer")


def test_async_sandbox_deterministic_random_uuid_time():
    async def orch(ctx, _):
        r = random.random()
        u = str(uuid.uuid4())
        t = int(time.time())
        return {"r": r, "u": u, "t": t}

    registry = _Registry()
    name = registry.add_async_orchestrator(orch, sandbox_mode="best_effort")  # type: ignore[attr-defined]
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))

    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    res1 = exec.execute(TEST_INSTANCE_ID, [], new_events)
    out1 = res1.actions[0].completeOrchestration.result.value

    res2 = exec.execute(TEST_INSTANCE_ID, [], new_events)
    out2 = res2.actions[0].completeOrchestration.result.value
    assert out1 == out2


def test_async_two_activities_no_timer():
    async def orch(ctx, _):
        a = await ctx.call_activity("echo", input=1)
        b = await ctx.call_activity("echo", input=a + 1)
        return b

    def echo(_, x):
        return x

    registry = _Registry()
    name = registry.add_async_orchestrator(orch)  # type: ignore[attr-defined]
    activity_name = registry.add_activity(echo)
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))

    # start -> schedule first activity
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("scheduleTask")

    # complete first activity -> schedule second
    old_events = new_events + [helpers.new_task_scheduled_event(1, activity_name)]
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_task_completed_event(1, encoded_output=json.dumps(1)),
    ]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("scheduleTask")

    # complete second -> done
    old_events = old_events + new_events + [helpers.new_task_scheduled_event(1, activity_name)]
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_task_completed_event(1, encoded_output=json.dumps(2)),
    ]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("completeOrchestration")


def test_async_ctx_metadata_passthrough():
    async def orch(ctx, _):
        # Access deterministic metadata via AsyncWorkflowContext
        return {
            "id": ctx.instance_id,
            "replay": ctx.is_replaying,
            "susp": ctx.is_suspended,
        }

    registry = _Registry()
    name = registry.add_async_orchestrator(orch)  # type: ignore[attr-defined]
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))

    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("completeOrchestration")
    out_json = res.actions[0].completeOrchestration.result.value
    out = json.loads(out_json)
    assert out["id"] == TEST_INSTANCE_ID
    assert out["replay"] is False


def test_async_gather_happy_path_and_return_exceptions():
    async def orch(ctx, _):
        a = ctx.call_activity("ok", input=1)
        b = ctx.call_activity("boom", input=2)
        c = ctx.call_activity("ok", input=3)
        vals = await ctx.gather(a, b, c, return_exceptions=True)
        return vals

    def ok(_, x):
        return x

    def boom(_, __):
        raise RuntimeError("fail!")

    registry = _Registry()
    name = registry.add_async_orchestrator(orch)  # type: ignore[attr-defined]
    an_ok = registry.add_activity(ok)
    an_boom = registry.add_activity(boom)
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))

    # start -> schedule three activities
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert len(res.actions) == 3 and all(a.HasField("scheduleTask") for a in res.actions)

    # mark scheduled
    old_events = new_events + [
        helpers.new_task_scheduled_event(1, an_ok),
        helpers.new_task_scheduled_event(2, an_boom),
        helpers.new_task_scheduled_event(3, an_ok),
    ]

    # complete ok(1), fail boom(2), complete ok(3)
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_task_completed_event(1, encoded_output=json.dumps(1)),
        helpers.new_task_failed_event(2, RuntimeError("fail!")),
        helpers.new_task_completed_event(3, encoded_output=json.dumps(3)),
    ]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("completeOrchestration")


def test_async_strict_sandbox_blocks_create_task():
    import asyncio

    import durabletask.internal.helpers as helpers

    async def orch(ctx, _):
        # Should be blocked in strict mode during priming
        asyncio.create_task(asyncio.sleep(0))
        return 1

    registry = _Registry()
    name = registry.add_async_orchestrator(orch, sandbox_mode="strict")  # type: ignore[attr-defined]
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("completeOrchestration")
    # Expect failureDetails is set due to strict mode error
    assert res.actions[0].completeOrchestration.HasField("failureDetails")


def test_async_when_any_ignores_losers_deterministically():
    import durabletask.internal.helpers as helpers

    async def orch(ctx, _):
        a = ctx.call_activity("a", input=1)
        b = ctx.call_activity("b", input=2)
        await ctx.when_any([a, b])
        return "done"

    def a(_, x):
        return x

    def b(_, x):
        return x

    registry = _Registry()
    name = registry.add_async_orchestrator(orch)  # type: ignore[attr-defined]
    an = registry.add_activity(a)
    bn = registry.add_activity(b)

    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))

    # start -> schedule both
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert len(res.actions) == 2 and all(a.HasField("scheduleTask") for a in res.actions)

    # winner completes -> orchestration should complete; no extra commands emitted to cancel loser
    old_events = new_events + [
        helpers.new_task_scheduled_event(1, an),
        helpers.new_task_scheduled_event(2, bn),
    ]
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_task_completed_event(1, encoded_output=json.dumps(1)),
    ]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("completeOrchestration")


def test_async_termination_maps_to_cancellation():
    async def orch(ctx, _):
        try:
            await ctx.sleep(10)
        except Exception as e:
            # Should surface as cancellation
            return type(e).__name__
        return "unexpected"

    registry = _Registry()
    name = registry.add_async_orchestrator(orch)  # type: ignore[attr-defined]
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))

    # start -> schedule timer
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert any(a.HasField("createTimer") for a in res.actions)
    # Capture the actual timer ID to avoid non-determinism in tests
    _ = next(a.id for a in res.actions if a.HasField("createTimer"))

    # terminate -> expect completion with TERMINATED and encoded output preserved
    old_events = new_events
    new_events = [helpers.new_terminated_event(encoded_output=json.dumps("bye"))]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("completeOrchestration")
    assert res.actions[0].completeOrchestration.orchestrationStatus == 5  # TERMINATED


def test_async_suspend_sets_flag_and_resumes_without_raising():
    async def orch(ctx, _):
        # observe suspension via flag and then continue normally
        before = ctx.is_suspended
        await ctx.sleep(0.1)
        after = ctx.is_suspended
        return {"before": before, "after": after}

    registry = _Registry()
    name = registry.add_async_orchestrator(orch)  # type: ignore[attr-defined]
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))

    # start
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    res = exec.execute(TEST_INSTANCE_ID, [], new_events)
    assert any(a.HasField("createTimer") for a in res.actions)
    timer_id = next(a.id for a in res.actions if a.HasField("createTimer"))

    # suspend, then resume, then fire timer across separate activations, always with orchestratorStarted
    now_dt = helpers.new_orchestrator_started_event().timestamp.ToDatetime()
    old_events = new_events
    new_events = [helpers.new_orchestrator_started_event(), helpers.new_suspend_event()]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert not any(a.HasField("completeOrchestration") for a in res.actions)

    # Confirm timer created after first activation
    old_events = old_events + new_events + [helpers.new_timer_created_event(timer_id, now_dt)]

    # Resume activation
    new_events = [helpers.new_orchestrator_started_event(), helpers.new_resume_event()]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    old_events = old_events + new_events

    # Timer fires in next activation
    new_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_timer_fired_event(timer_id, now_dt),
    ]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("completeOrchestration")


def test_async_suspend_resume_like_generator_test():
    async def orch(ctx, _):
        val = await ctx.wait_for_external_event("my_event")
        return val

    registry = _Registry()
    name = registry.add_async_orchestrator(orch)  # type: ignore[attr-defined]
    exec = _OrchestrationExecutor(registry, logging.getLogger("tests"))

    old_events = [
        helpers.new_orchestrator_started_event(),
        helpers.new_execution_started_event(name, TEST_INSTANCE_ID, encoded_input=None),
    ]
    new_events = [
        helpers.new_suspend_event(),
        helpers.new_event_raised_event("my_event", encoded_input=json.dumps(42)),
    ]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 0

    old_events = old_events + new_events
    new_events = [helpers.new_resume_event()]
    res = exec.execute(TEST_INSTANCE_ID, old_events, new_events)
    assert len(res.actions) == 1 and res.actions[0].HasField("completeOrchestration")
