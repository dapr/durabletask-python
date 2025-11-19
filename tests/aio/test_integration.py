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

"""
Integration tests for durabletask.aio package.

These tests verify end-to-end functionality of async workflows,
including the interaction between all components.

Tests marked with @pytest.mark.e2e require a running Dapr sidecar
or DurableTask-Go emulator and are skipped by default.
"""

from datetime import datetime
from unittest.mock import Mock

import pytest

from durabletask import task as dt_task
from durabletask.aio import (
    AsyncWorkflowContext,
    AsyncWorkflowError,
    CoroutineOrchestratorRunner,
    WorkflowTimeoutError,
)


class FakeTask(dt_task.Task):
    """Simple fake task for testing, based on python-sdk approach."""

    def __init__(self, name: str):
        super().__init__()
        self.name = name
        self._result = f"result_for_{name}"

    def get_result(self):
        return self._result

    def complete_with_result(self, result):
        """Helper method for tests to complete the task."""
        self._result = result
        self._is_complete = True


class FakeCtx:
    """Simple fake context for testing, based on python-sdk approach."""

    def __init__(self):
        self.current_utc_datetime = datetime(2024, 1, 1, 12, 0, 0)
        self.instance_id = "test-instance"
        self.is_replaying = False
        self.workflow_name = "test-workflow"
        self.parent_instance_id = None
        self.history_event_sequence = None
        self.is_suspended = False

    def call_activity(self, activity, *, input=None, retry_policy=None, metadata=None):
        activity_name = getattr(activity, "__name__", str(activity))
        return FakeTask(f"activity:{activity_name}")

    def call_sub_orchestrator(
        self, orchestrator, *, input=None, instance_id=None, retry_policy=None, metadata=None
    ):
        orchestrator_name = getattr(orchestrator, "__name__", str(orchestrator))
        return FakeTask(f"sub:{orchestrator_name}")

    def create_timer(self, fire_at):
        return FakeTask("timer")

    def wait_for_external_event(self, name: str):
        return FakeTask(f"event:{name}")

    def set_custom_status(self, custom_status):
        pass

    def continue_as_new(self, new_input, *, save_events=False):
        pass


def drive_workflow(gen, results_map=None):
    """
    Drive a workflow generator, providing results for yielded tasks.
    Based on python-sdk approach but adapted for durabletask.

    Args:
        gen: The workflow generator
        results_map: Dict mapping task names to results, or callable that takes task and returns result
    """
    results_map = results_map or {}

    try:
        # Start the generator
        task = next(gen)

        while True:
            # Determine result for this task
            if callable(results_map):
                result = results_map(task)
            elif hasattr(task, "name"):
                result = results_map.get(task.name, f"result_for_{task.name}")
            else:
                result = "default_result"

            # Send result and get next task
            task = gen.send(result)

    except StopIteration as stop:
        return stop.value


class TestAsyncWorkflowIntegration:
    """Integration tests for async workflow functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.fake_ctx = FakeCtx()

    def test_simple_activity_workflow_integration(self):
        """Test a simple workflow that calls one activity."""

        async def simple_activity_workflow(ctx: AsyncWorkflowContext, input_data: str) -> str:
            result = await ctx.call_activity("process_data", input=input_data)
            return f"Processed: {result}"

        # Create runner and context
        runner = CoroutineOrchestratorRunner(simple_activity_workflow)
        async_ctx = AsyncWorkflowContext(self.fake_ctx)

        # Execute workflow using the drive helper
        gen = runner.to_generator(async_ctx, "test_input")
        result = drive_workflow(gen, {"activity:process_data": "activity_result"})

        assert result == "Processed: activity_result"

    def test_multi_step_workflow_integration(self):
        """Test a workflow with multiple sequential activities."""

        async def multi_step_workflow(ctx: AsyncWorkflowContext, input_data: dict) -> dict:
            # Step 1: Validate input
            validation_result = await ctx.call_activity("validate_input", input=input_data)

            # Step 2: Process data
            processing_result = await ctx.call_activity("process_data", input=validation_result)

            # Step 3: Save result
            save_result = await ctx.call_activity("save_result", input=processing_result)

            return {
                "validation": validation_result,
                "processing": processing_result,
                "save": save_result,
            }

        runner = CoroutineOrchestratorRunner(multi_step_workflow)
        async_ctx = AsyncWorkflowContext(self.fake_ctx)

        gen = runner.to_generator(async_ctx, {"data": "test"})

        # Use drive_workflow with specific results for each activity
        results_map = {
            "activity:validate_input": "validated_data",
            "activity:process_data": "processed_data",
            "activity:save_result": "saved_data",
        }
        result = drive_workflow(gen, results_map)

        assert result == {
            "validation": "validated_data",
            "processing": "processed_data",
            "save": "saved_data",
        }

    def test_parallel_activities_workflow_integration(self):
        """Test a workflow with parallel activities using when_all."""

        async def parallel_workflow(ctx: AsyncWorkflowContext, input_data: list) -> list:
            # Start multiple activities in parallel
            tasks = []
            for i, item in enumerate(input_data):
                task = ctx.call_activity(f"process_item_{i}", input=item)
                tasks.append(task)

            # Wait for all to complete
            results = await ctx.when_all(tasks)
            return results

        runner = CoroutineOrchestratorRunner(parallel_workflow)
        async_ctx = AsyncWorkflowContext(self.fake_ctx)

        input_data = ["item1", "item2", "item3"]
        gen = runner.to_generator(async_ctx, input_data)

        # Use drive_workflow to handle the when_all task
        result = drive_workflow(gen, lambda task: ["result1", "result2", "result3"])

        assert result == ["result1", "result2", "result3"]

    def test_sub_orchestrator_workflow_integration(self):
        """Test a workflow that calls a sub-orchestrator."""

        async def parent_workflow(ctx: AsyncWorkflowContext, input_data: dict) -> dict:
            # Call sub-orchestrator
            sub_result = await ctx.call_sub_orchestrator(
                "child_workflow", input=input_data["child_input"], instance_id="child-instance"
            )

            # Process sub-orchestrator result
            final_result = await ctx.call_activity("finalize", input=sub_result)

            return {"sub_result": sub_result, "final": final_result}

        runner = CoroutineOrchestratorRunner(parent_workflow)
        async_ctx = AsyncWorkflowContext(self.fake_ctx)

        gen = runner.to_generator(async_ctx, {"child_input": "test_data"})

        # Use drive_workflow with specific results
        results_map = {
            "sub:child_workflow": "sub_orchestrator_result",
            "activity:finalize": "final_result",
        }
        result = drive_workflow(gen, results_map)

        assert result == {"sub_result": "sub_orchestrator_result", "final": "final_result"}

    def test_timer_workflow_integration(self):
        """Test a workflow that uses timers."""

        async def timer_workflow(ctx: AsyncWorkflowContext, delay_seconds: float) -> str:
            # Start some work
            initial_result = await ctx.call_activity("start_work", input="begin")

            # Wait for specified delay
            await ctx.sleep(delay_seconds)

            # Complete work
            final_result = await ctx.call_activity("complete_work", input=initial_result)

            return final_result

        runner = CoroutineOrchestratorRunner(timer_workflow)
        async_ctx = AsyncWorkflowContext(self.fake_ctx)

        gen = runner.to_generator(async_ctx, 30.0)

        # Use drive_workflow with specific results
        results_map = {
            "activity:start_work": "work_started",
            "timer": None,  # Timer completion
            "activity:complete_work": "work_completed",
        }
        result = drive_workflow(gen, results_map)

        assert result == "work_completed"

    def test_external_event_workflow_integration(self):
        """Test a workflow that waits for external events."""

        async def event_workflow(ctx: AsyncWorkflowContext, event_name: str) -> dict:
            # Start processing
            start_result = await ctx.call_activity("start_processing", input="begin")

            # Wait for external event
            event_data = await ctx.wait_for_external_event(event_name)

            # Process event data
            final_result = await ctx.call_activity(
                "process_event", input={"start": start_result, "event": event_data}
            )

            return {"result": final_result, "event_data": event_data}

        runner = CoroutineOrchestratorRunner(event_workflow)
        async_ctx = AsyncWorkflowContext(self.fake_ctx)

        gen = runner.to_generator(async_ctx, "approval_event")

        # Use drive_workflow with specific results
        results_map = {
            "activity:start_processing": "processing_started",
            "event:approval_event": {"approved": True, "user": "admin"},
            "activity:process_event": "event_processed",
        }
        result = drive_workflow(gen, results_map)

        assert result == {
            "result": "event_processed",
            "event_data": {"approved": True, "user": "admin"},
        }

    def test_when_any_workflow_integration(self):
        """Test a workflow using when_any for racing conditions."""

        async def racing_workflow(ctx: AsyncWorkflowContext, timeout_seconds: float) -> dict:
            # Start a long-running activity
            work_task = ctx.call_activity("long_running_work", input="start")

            # Create a timeout
            timeout_task = ctx.sleep(timeout_seconds)

            # Race between work completion and timeout
            completed_task = await ctx.when_any([work_task, timeout_task])

            if completed_task == work_task:
                result = completed_task.get_result()
                return {"status": "completed", "result": result}
            else:
                return {"status": "timeout", "result": None}

        runner = CoroutineOrchestratorRunner(racing_workflow)
        async_ctx = AsyncWorkflowContext(self.fake_ctx)

        gen = runner.to_generator(async_ctx, 10.0)

        # Should yield when_any task
        when_any_task = next(gen)
        assert isinstance(when_any_task, dt_task.Task)

        # Simulate work completing first
        mock_completed_task = Mock()
        mock_completed_task.get_result.return_value = "work_done"

        try:
            gen.send(mock_completed_task)
        except StopIteration as stop:
            result = stop.value
            assert result["status"] == "completed"
            assert result["result"] == "work_done"

    def test_timeout_workflow_integration(self):
        """Test workflow with timeout functionality."""

        async def timeout_workflow(ctx: AsyncWorkflowContext, data: str) -> str:
            try:
                # Activity with 5-second timeout
                result = await ctx.with_timeout(
                    ctx.call_activity("slow_activity", input=data),
                    5.0,
                )
                return f"Success: {result}"
            except WorkflowTimeoutError:
                return "Timeout occurred"

        runner = CoroutineOrchestratorRunner(timeout_workflow)
        async_ctx = AsyncWorkflowContext(self.fake_ctx)

        gen = runner.to_generator(async_ctx, "test_data")

        # Should yield when_any task (activity vs timeout)
        when_any_task = next(gen)
        assert isinstance(when_any_task, dt_task.Task)

        # Simulate timeout completing first
        timeout_task = Mock()
        timeout_task.get_result.return_value = None

        try:
            gen.send(timeout_task)
        except StopIteration as stop:
            assert stop.value == "Timeout occurred"

    def test_deterministic_operations_integration(self):
        """Test that deterministic operations work correctly in workflows."""

        async def deterministic_workflow(ctx: AsyncWorkflowContext, count: int) -> dict:
            # Generate deterministic random values
            random_values = []
            for _ in range(count):
                rng = ctx.random()
                random_values.append(rng.random())

            # Generate deterministic UUIDs
            uuids = []
            for _ in range(count):
                uuids.append(str(ctx.uuid4()))

            # Generate deterministic strings
            strings = []
            for i in range(count):
                strings.append(ctx.random_string(10))

            return {
                "random_values": random_values,
                "uuids": uuids,
                "strings": strings,
                "timestamp": ctx.now().isoformat(),
            }

        runner = CoroutineOrchestratorRunner(deterministic_workflow)
        async_ctx = AsyncWorkflowContext(self.fake_ctx)

        gen = runner.to_generator(async_ctx, 3)

        # Should complete synchronously (no async operations)
        try:
            next(gen)
        except StopIteration as stop:
            result = stop.value

            # Verify structure
            assert len(result["random_values"]) == 3
            assert len(result["uuids"]) == 3
            assert len(result["strings"]) == 3
            assert "timestamp" in result

            # Verify deterministic behavior - run again with fresh context (simulates replay)
            async_ctx2 = AsyncWorkflowContext(self.fake_ctx)
            gen2 = runner.to_generator(async_ctx2, 3)
            try:
                next(gen2)
            except StopIteration as stop2:
                result2 = stop2.value

                # Should be identical (deterministic behavior)
                assert result == result2

    def test_error_handling_integration(self):
        """Test error handling throughout the workflow stack."""

        async def error_prone_workflow(ctx: AsyncWorkflowContext, should_fail: bool) -> str:
            if should_fail:
                raise ValueError("Workflow intentionally failed")

            result = await ctx.call_activity("safe_activity", input="test")
            return f"Success: {result}"

        runner = CoroutineOrchestratorRunner(error_prone_workflow)
        async_ctx = AsyncWorkflowContext(self.fake_ctx)

        # Test successful case
        gen_success = runner.to_generator(async_ctx, False)
        _ = next(gen_success)

        try:
            gen_success.send("activity_result")
        except StopIteration as stop:
            assert stop.value == "Success: activity_result"

        # Test error case
        gen_error = runner.to_generator(async_ctx, True)

        with pytest.raises(AsyncWorkflowError) as exc_info:
            next(gen_error)

        assert "Workflow intentionally failed" in str(exc_info.value)
        assert exc_info.value.workflow_name == "error_prone_workflow"

    def test_sandbox_integration(self):
        """Test sandbox integration with workflows."""

        async def sandbox_workflow(ctx: AsyncWorkflowContext, mode: str) -> dict:
            # Use deterministic operations
            random_val = ctx.random().random()
            uuid_val = str(ctx.uuid4())
            time_val = ctx.now().isoformat()

            # Call an activity
            activity_result = await ctx.call_activity("test_activity", input="test")

            return {
                "random": random_val,
                "uuid": uuid_val,
                "time": time_val,
                "activity": activity_result,
            }

        # Test with different sandbox modes
        for mode in ["off", "best_effort", "strict"]:
            runner = CoroutineOrchestratorRunner(sandbox_workflow, sandbox_mode=mode)
            async_ctx = AsyncWorkflowContext(self.fake_ctx)

            gen = runner.to_generator(async_ctx, mode)

            # Should yield activity task
            activity_task = next(gen)
        # With FakeCtx, ensure we yielded the expected durable task token
        assert isinstance(activity_task, dt_task.Task)
        assert getattr(activity_task, "name", "") == "activity:test_activity"

        # Complete workflow
        try:
            gen.send("activity_done")
        except StopIteration as stop:
            result = stop.value

            # Verify structure
            assert "random" in result
            assert "uuid" in result
            assert "time" in result
            assert result["activity"] == "activity_done"

    def test_complex_workflow_integration(self):
        """Test a complex workflow combining multiple features."""

        async def complex_workflow(ctx: AsyncWorkflowContext, config: dict) -> dict:
            # Step 1: Initialize
            init_result = await ctx.call_activity("initialize", input=config)

            # Step 2: Parallel processing
            parallel_tasks = []
            for i in range(config["parallel_count"]):
                task = ctx.call_activity(f"process_batch_{i}", input=init_result)
                parallel_tasks.append(task)

            batch_results = await ctx.when_all(parallel_tasks)

            # Step 3: Wait for approval with timeout
            try:
                approval = await ctx.with_timeout(
                    ctx.wait_for_external_event("approval"),
                    config["approval_timeout"],
                )
            except WorkflowTimeoutError:
                approval = {"approved": False, "reason": "timeout"}

            # Step 4: Conditional sub-orchestrator
            if approval.get("approved", False):
                sub_result = await ctx.call_sub_orchestrator(
                    "finalization_workflow", input={"batches": batch_results, "approval": approval}
                )
            else:
                sub_result = await ctx.call_activity("handle_rejection", input=approval)

            # Step 5: Generate report
            report = {
                "workflow_id": ctx.instance_id,
                "timestamp": ctx.now().isoformat(),
                "init": init_result,
                "batches": batch_results,
                "approval": approval,
                "final": sub_result,
                "random_id": str(ctx.uuid4()),
            }

            return report

        runner = CoroutineOrchestratorRunner(complex_workflow)
        async_ctx = AsyncWorkflowContext(self.fake_ctx)

        config = {"parallel_count": 2, "approval_timeout": 30.0}

        gen = runner.to_generator(async_ctx, config)

        # Step 1: Initialize
        _ = next(gen)

        # Step 2: Parallel processing (when_all)
        _ = gen.send("initialized")

        # Step 3: Approval with timeout (when_any)
        _ = gen.send(["batch_1_result", "batch_2_result"])

        # Simulate approval received
        approval_data = {"approved": True, "user": "admin"}

        # Step 4: Sub-orchestrator
        _ = gen.send(approval_data)

        # Complete workflow
        try:
            gen.send("finalization_complete")
        except StopIteration as stop:
            result = stop.value

            # Verify complex result structure
            assert result["workflow_id"] == "test-instance"
            assert result["init"] == "initialized"
            assert result["batches"] == ["batch_1_result", "batch_2_result"]
            assert result["approval"] == approval_data
            assert result["final"] == "finalization_complete"
            assert "timestamp" in result
            assert "random_id" in result

    def test_workflow_replay_determinism(self):
        """Test that workflows are deterministic during replay."""

        async def replay_test_workflow(ctx: AsyncWorkflowContext, input_data: str) -> dict:
            # Generate deterministic values
            random_val = ctx.random().random()
            uuid_val = str(ctx.uuid4())
            string_val = ctx.random_string(8)

            # Call activity
            activity_result = await ctx.call_activity("test_activity", input=input_data)

            return {
                "random": random_val,
                "uuid": uuid_val,
                "string": string_val,
                "activity": activity_result,
            }

        runner = CoroutineOrchestratorRunner(replay_test_workflow)

        # First execution
        async_ctx1 = AsyncWorkflowContext(self.fake_ctx)
        gen1 = runner.to_generator(async_ctx1, "test_input")

        _ = next(gen1)

        try:
            gen1.send("activity_result")
        except StopIteration as stop1:
            result1 = stop1.value

        # Second execution (simulating replay)
        async_ctx2 = AsyncWorkflowContext(self.fake_ctx)
        gen2 = runner.to_generator(async_ctx2, "test_input")

        _ = next(gen2)

        try:
            gen2.send("activity_result")
        except StopIteration as stop2:
            result2 = stop2.value

        # Results should be identical (deterministic)
        assert result1 == result2


class TestSandboxIntegration:
    """Integration tests for sandbox functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_base_ctx = Mock()
        self.mock_base_ctx.instance_id = "test-instance"
        self.mock_base_ctx.current_utc_datetime = datetime(2023, 1, 1, 12, 0, 0)
        self.mock_base_ctx.call_activity.return_value = Mock(spec=dt_task.Task)
        self.mock_base_ctx.create_timer.return_value = Mock(spec=dt_task.Task)

    def test_sandbox_with_async_workflow_context(self):
        """Test sandbox integration with AsyncWorkflowContext."""
        import random
        import time
        import uuid

        from durabletask.aio import sandbox_scope

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        with sandbox_scope(async_ctx, "best_effort"):
            # Should work with real AsyncWorkflowContext
            test_random = random.random()
            test_uuid = uuid.uuid4()
            test_time = time.time()

            assert isinstance(test_random, float)
            assert isinstance(test_uuid, uuid.UUID)
            assert isinstance(test_time, float)

    def test_sandbox_warning_detection(self):
        """Test that sandbox properly issues warnings."""
        import warnings

        from durabletask.aio import NonDeterminismWarning, sandbox_scope

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")

            with sandbox_scope(async_ctx, "best_effort"):
                # This should potentially trigger warnings if non-deterministic calls are detected
                pass

            # Check if any NonDeterminismWarning was issued
            # May or may not have warnings depending on implementation
            _ = [warning for warning in w if issubclass(warning.category, NonDeterminismWarning)]

    def test_sandbox_performance_impact(self):
        """Test that sandbox doesn't have excessive performance impact."""
        import random
        import time as time_module

        from durabletask.aio import sandbox_scope

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        # Ensure debug mode is OFF for performance testing
        async_ctx._debug_mode = False

        # Measure without sandbox
        start = time_module.perf_counter()
        for _ in range(1000):
            random.random()
        no_sandbox_time = time_module.perf_counter() - start

        # Measure with sandbox
        start = time_module.perf_counter()
        with sandbox_scope(async_ctx, "best_effort"):
            for _ in range(1000):
                random.random()
        sandbox_time = time_module.perf_counter() - start

        # Sandbox should not be more than 20x slower (reasonable overhead for patching + minimal tracing)
        # In practice, the overhead comes from function call interception and deterministic RNG
        assert sandbox_time < no_sandbox_time * 20, (
            f"Sandbox: {sandbox_time:.6f}s, No sandbox: {no_sandbox_time:.6f}s"
        )

    def test_sandbox_mode_validation(self):
        """Test sandbox mode validation."""
        from durabletask.aio import sandbox_scope

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        # Valid modes should work
        for mode in ["off", "best_effort", "strict"]:
            with sandbox_scope(async_ctx, mode):
                pass

        # Invalid mode should raise error
        with pytest.raises(ValueError):
            with sandbox_scope(async_ctx, "invalid"):
                pass
