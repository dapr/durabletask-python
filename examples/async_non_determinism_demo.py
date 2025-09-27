"""
Demonstration of non-determinism detection in async workflows.

This example shows how the enhanced asyncio compatibility layer detects
and warns about non-deterministic function usage that could break workflow
replay semantics.
"""

import os
import time
import warnings
from datetime import datetime

from durabletask.aio import SandboxMode
from durabletask.client import TaskHubGrpcClient
from durabletask.worker import TaskHubGrpcWorker


def simple_activity(_, message: str) -> str:
    """A simple activity that returns a message."""
    return f"Activity processed: {message}"


async def problematic_workflow_best_effort(ctx, input_data):
    """
    Workflow that uses non-deterministic functions.
    In 'best_effort' mode, this will generate warnings.
    """
    print(f"[{ctx.instance_id}] Starting problematic workflow")

    # This will generate a warning in best_effort mode
    import datetime as dt

    current_time = dt.datetime.now()  # NON-DETERMINISTIC!
    print(f"[{ctx.instance_id}] Current time (non-deterministic): {current_time}")

    # This is the correct, deterministic way
    deterministic_time = ctx.now()
    print(f"[{ctx.instance_id}] Deterministic time: {deterministic_time}")

    # More non-deterministic operations that will trigger warnings
    import random
    import uuid

    random_val = random.random()  # NON-DETERMINISTIC!
    uuid_val = uuid.uuid4()  # NON-DETERMINISTIC!

    print(f"[{ctx.instance_id}] Random value (non-deterministic): {random_val}")
    print(f"[{ctx.instance_id}] UUID (non-deterministic): {uuid_val}")

    # Correct deterministic alternatives
    det_random = ctx.random().random()
    det_uuid = ctx.uuid4()

    print(f"[{ctx.instance_id}] Deterministic random: {det_random}")
    print(f"[{ctx.instance_id}] Deterministic UUID: {det_uuid}")

    # Run an activity
    result = await ctx.activity(
        "simple_activity", input="Hello from problematic workflow"
    )

    return {
        "result": result,
        "non_deterministic_time": current_time.isoformat(),
        "deterministic_time": deterministic_time.isoformat(),
        "non_deterministic_random": random_val,
        "deterministic_random": det_random,
        "non_deterministic_uuid": str(uuid_val),
        "deterministic_uuid": str(det_uuid),
    }


async def problematic_workflow_strict(ctx, input_data):
    """
    Workflow that will fail in strict mode due to non-deterministic functions.
    """
    print(f"[{ctx.instance_id}] Starting strict mode workflow")

    # This will raise an AsyncWorkflowError in strict mode
    try:
        with open("test.txt", "w") as f:  # BLOCKED IN STRICT MODE!
            f.write("This will fail")
    except RuntimeError as e:
        print(f"[{ctx.instance_id}] Caught expected error: {e}")

    # Use deterministic alternatives
    result = await ctx.activity(simple_activity, input="Hello from strict workflow")

    return {"result": result, "message": "Strict mode workflow completed successfully"}


async def good_workflow(ctx, input_data):
    """
    Workflow that uses only deterministic functions.
    This will not generate any warnings.
    """
    print(f"[{ctx.instance_id}] Starting good workflow")

    # All deterministic operations
    current_time = ctx.now()
    random_val = ctx.random().random()
    uuid_val = ctx.uuid4()

    print(f"[{ctx.instance_id}] Deterministic time: {current_time}")
    print(f"[{ctx.instance_id}] Deterministic random: {random_val}")
    print(f"[{ctx.instance_id}] Deterministic UUID: {uuid_val}")

    # Run an activity
    result = await ctx.activity(simple_activity, input="Hello from good workflow")

    return {
        "result": result,
        "time": current_time.isoformat(),
        "random": random_val,
        "uuid": str(uuid_val),
    }


def main():
    # Enable debug mode to see detailed logging
    os.environ["DAPR_WF_DEBUG"] = "true"

    # Point to your sidecar, or set DURABLETASK_GRPC_ENDPOINT in env
    os.environ.setdefault("DURABLETASK_GRPC_ENDPOINT", "localhost:4001")

    # Capture warnings to show them clearly
    warnings.filterwarnings("always", category=UserWarning)

    with TaskHubGrpcWorker() as worker:
        # Register activity
        worker.add_activity(simple_activity)

        # Register workflows with different sandbox modes
        worker.add_async_orchestrator(
            problematic_workflow_best_effort,
            name="problematic_best_effort",
            sandbox_mode=SandboxMode.BEST_EFFORT,  # Will generate warnings
        )

        worker.add_async_orchestrator(
            problematic_workflow_strict,
            name="problematic_strict",
            sandbox_mode=SandboxMode.STRICT,  # Will block dangerous operations
        )

        worker.add_async_orchestrator(
            good_workflow,
            name="good_workflow",
            sandbox_mode=SandboxMode.BEST_EFFORT,  # No warnings expected
        )

        worker.start()
        worker.wait_for_ready(timeout=5)

        client = TaskHubGrpcClient()

        print("=" * 60)
        print("DEMO: Non-Determinism Detection in Async Workflows")
        print("=" * 60)

        # Test 1: Best effort mode with problematic code
        print("\n1. Testing BEST_EFFORT mode with non-deterministic functions:")
        print("   (Should generate warnings but continue execution)")

        instance_id_1 = client.schedule_new_orchestration(
            "problematic_best_effort", input={"test": "best_effort"}
        )
        print(f"   Started instance: {instance_id_1}")

        state_1 = client.wait_for_orchestration_completion(instance_id_1, timeout=30)
        if state_1:
            if state_1.failure_details:
                print(f"   ❌ Failed: {state_1.failure_details.message}")
            else:
                print(f"   ✅ Completed with warnings")
                print(f"   Result: {state_1.serialized_output}")

        # Test 2: Strict mode with blocked operations
        print("\n2. Testing STRICT mode with blocked operations:")
        print("   (Should block dangerous operations but continue)")

        instance_id_2 = client.schedule_new_orchestration(
            "problematic_strict", input={"test": "strict"}
        )
        print(f"   Started instance: {instance_id_2}")

        state_2 = client.wait_for_orchestration_completion(instance_id_2, timeout=30)
        if state_2:
            if state_2.failure_details:
                print(f"   ❌ Failed: {state_2.failure_details.message}")
            else:
                print(f"   ✅ Completed successfully")
                print(f"   Result: {state_2.serialized_output}")

        # Test 3: Good workflow with no issues
        print("\n3. Testing workflow with only deterministic functions:")
        print("   (Should complete without any warnings)")

        instance_id_3 = client.schedule_new_orchestration(
            "good_workflow", input={"test": "good"}
        )
        print(f"   Started instance: {instance_id_3}")

        state_3 = client.wait_for_orchestration_completion(instance_id_3, timeout=30)
        if state_3:
            if state_3.failure_details:
                print(f"   ❌ Failed: {state_3.failure_details.message}")
            else:
                print(f"   ✅ Completed without warnings")
                print(f"   Result: {state_3.serialized_output}")

        print("\n" + "=" * 60)
        print("SUMMARY:")
        print("- Best effort mode: Warns about non-deterministic functions")
        print("- Strict mode: Blocks dangerous operations")
        print("- Good practices: Use ctx.now(), ctx.random(), ctx.uuid4()")
        print("- Always test workflows with sandbox_mode='best_effort' or 'strict'")
        print("=" * 60)

        # Give worker a moment to flush logs before shutdown
        time.sleep(1)


if __name__ == "__main__":
    main()

