# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for driver functionality in durabletask.aio.
"""

import asyncio
from typing import Any, Protocol, runtime_checkable
from unittest.mock import Mock

import pytest

from durabletask import task as dt_task
from durabletask.aio import (
    AsyncWorkflowContext,
    AsyncWorkflowError,
    CoroutineOrchestratorRunner,
    WorkflowFunction,
    WorkflowValidationError,
)

# DTPOperation deprecated: tests removed


class TestWorkflowFunction:
    """Test WorkflowFunction protocol."""

    def test_workflow_function_protocol(self):
        """Test WorkflowFunction protocol recognition."""
        # Valid async workflow function
        async def valid_workflow(ctx: AsyncWorkflowContext, input_data: Any) -> str:
            return "result"
        
        # Should be recognized as WorkflowFunction
        assert isinstance(valid_workflow, WorkflowFunction)

    def test_non_async_function_protocol(self):
        """Test that non-async functions are still recognized structurally."""
        # Non-async function with correct signature
        def not_async_workflow(ctx: AsyncWorkflowContext, input_data: Any) -> str:
            return "result"
        
        # Should still be recognized as WorkflowFunction due to structural typing
        # The actual async validation happens in CoroutineOrchestratorRunner
        assert isinstance(not_async_workflow, WorkflowFunction)


class TestCoroutineOrchestratorRunner:
    """Test CoroutineOrchestratorRunner functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        self.mock_base_ctx.instance_id = "test-instance"
        self.mock_base_ctx.current_utc_datetime = Mock()

    def test_runner_creation(self):
        """Test creating a CoroutineOrchestratorRunner."""
        async def test_workflow(ctx: AsyncWorkflowContext, input_data: Any) -> str:
            return "result"
        
        runner = CoroutineOrchestratorRunner(test_workflow)
        
        assert runner._async_orchestrator is test_workflow
        assert runner._sandbox_mode == "off"
        assert runner._workflow_name == "test_workflow"

    def test_runner_with_sandbox_mode(self):
        """Test creating runner with sandbox mode."""
        async def test_workflow(ctx: AsyncWorkflowContext, input_data: Any) -> str:
            return "result"
        
        runner = CoroutineOrchestratorRunner(test_workflow, sandbox_mode="strict")
        
        assert runner._sandbox_mode == "strict"

    def test_runner_with_lambda_function(self):
        """Test creating runner with lambda function."""
        # Lambda functions must be async to be valid
        lambda_workflow = lambda ctx, input_data: "result"
        
        # Should raise validation error for non-async lambda
        with pytest.raises(WorkflowValidationError) as exc_info:
            CoroutineOrchestratorRunner(lambda_workflow)
        
        assert "async function" in str(exc_info.value)
        
        # Test with async lambda (though not recommended)
        async_lambda = lambda ctx, input_data: "result"
        async_lambda.__code__ = async_lambda.__code__.replace(co_flags=async_lambda.__code__.co_flags | 0x80)
        
        # This is complex to test properly, so let's just test that validation works

    def test_simple_synchronous_workflow(self):
        """Test running a simple synchronous workflow."""
        async def simple_workflow(ctx: AsyncWorkflowContext) -> str:
            return "hello world"
        
        runner = CoroutineOrchestratorRunner(simple_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        # Convert to generator and run
        gen = runner.to_generator(async_ctx, None)
        
        # Should complete immediately with StopIteration
        with pytest.raises(StopIteration) as exc_info:
            next(gen)
        
        assert exc_info.value.value == "hello world"

    def test_workflow_with_single_activity(self):
        """Test workflow with a single activity call."""
        async def activity_workflow(ctx: AsyncWorkflowContext, input_data: str) -> str:
            result = await ctx.call_activity("test_activity", input=input_data)
            return f"processed: {result}"
        
        # Mock the activity call
        mock_task = Mock(spec=dt_task.Task)
        self.mock_base_ctx.call_activity.return_value = mock_task
        
        runner = CoroutineOrchestratorRunner(activity_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        # Convert to generator
        gen = runner.to_generator(async_ctx, "test_input")
        
        # First yield should be the activity task
        yielded_task = next(gen)
        assert yielded_task is mock_task
        
        # Send result back
        try:
            gen.send("activity_result")
        except StopIteration as stop:
            assert stop.value == "processed: activity_result"
        else:
            pytest.fail("Expected StopIteration")

    def test_workflow_initialization_error(self):
        """Test workflow initialization error handling."""
        async def failing_workflow(ctx: AsyncWorkflowContext, input_data: Any) -> str:
            raise ValueError("Initialization failed")
        
        runner = CoroutineOrchestratorRunner(failing_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        # The error should be raised when we try to start the generator
        gen = runner.to_generator(async_ctx, None)
        
        with pytest.raises(AsyncWorkflowError) as exc_info:
            next(gen)  # This will trigger the initialization error
        
        assert "Workflow failed during initialization" in str(exc_info.value)
        assert exc_info.value.workflow_name == "failing_workflow"
        assert exc_info.value.step == "initialization"

    def test_workflow_invalid_signature(self):
        """Test workflow with invalid signature."""
        async def invalid_workflow() -> str:  # Missing ctx parameter
            return "result"
        
        # Should raise validation error during runner creation
        with pytest.raises(WorkflowValidationError) as exc_info:
            CoroutineOrchestratorRunner(invalid_workflow)
        
        assert "at least one parameter" in str(exc_info.value)

    def test_workflow_yielding_invalid_object(self):
        """Test workflow yielding invalid object."""
        # Create a workflow that yields an invalid object
        # We need to simulate this by creating a workflow that awaits something invalid
        class InvalidAwaitable:
            def __await__(self):
                yield "invalid"  # This will cause the error
                return "result"
        
        async def invalid_yield_workflow(ctx: AsyncWorkflowContext) -> str:
            result = await InvalidAwaitable()
            return result
        
        runner = CoroutineOrchestratorRunner(invalid_yield_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        gen = runner.to_generator(async_ctx, None)
        
        with pytest.raises(AsyncWorkflowError) as exc_info:
            next(gen)
        
        assert "awaited unsupported object type" in str(exc_info.value)

    def test_workflow_with_direct_task_yield(self):
        """Test workflow with custom awaitable that yields task directly."""
        # Create a custom awaitable that yields task directly (current approach)
        class DirectTaskAwaitable:
            def __init__(self, task):
                self.task = task
            
            def __await__(self):
                result = yield self.task
                return f"result: {result}"
        
        async def direct_task_workflow(ctx: AsyncWorkflowContext) -> str:
            mock_task = Mock(spec=dt_task.Task)
            result = await DirectTaskAwaitable(mock_task)
            return result
        
        runner = CoroutineOrchestratorRunner(direct_task_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        gen = runner.to_generator(async_ctx, None)
        
        # Should yield the underlying task
        yielded_task = next(gen)
        assert isinstance(yielded_task, Mock)  # The mock task
        
        # Send result back
        try:
            gen.send("operation_result")
        except StopIteration as stop:
            assert stop.value == "result: operation_result"

    def test_workflow_exception_handling(self):
        """Test workflow exception handling during execution."""
        async def exception_workflow(ctx: AsyncWorkflowContext) -> str:
            result = await ctx.call_activity("failing_activity")
            return result
        
        # Mock the activity call
        mock_task = Mock(spec=dt_task.Task)
        self.mock_base_ctx.call_activity.return_value = mock_task
        
        runner = CoroutineOrchestratorRunner(exception_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        gen = runner.to_generator(async_ctx, None)
        
        # First yield should be the activity task
        yielded_task = next(gen)
        assert yielded_task is mock_task
        
        # Throw an exception
        test_exception = Exception("Activity failed")
        try:
            gen.throw(test_exception)
        except StopIteration:
            pytest.fail("Expected exception to propagate")
        except AsyncWorkflowError as e:
            # The driver wraps the original exception in AsyncWorkflowError
            assert "Activity failed" in str(e)
            assert e.workflow_name == "exception_workflow"

    def test_workflow_step_tracking(self):
        """Test that workflow steps are tracked for error reporting."""
        # Test that the runner correctly tracks workflow name and steps
        async def multi_step_workflow(ctx: AsyncWorkflowContext) -> str:
            result1 = await ctx.call_activity("step1")
            result2 = await ctx.call_activity("step2")
            return f"{result1}+{result2}"
        
        # Mock the activity calls
        mock_task = Mock(spec=dt_task.Task)
        self.mock_base_ctx.call_activity.return_value = mock_task
        
        runner = CoroutineOrchestratorRunner(multi_step_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        # Verify workflow name is tracked
        assert runner._workflow_name == "multi_step_workflow"
        
        gen = runner.to_generator(async_ctx, None)
        
        # First step
        yielded_task = next(gen)
        assert yielded_task is mock_task
        
        # Complete first step
        yielded_task = gen.send("result1")
        assert yielded_task is mock_task
        
        # Complete second step
        try:
            gen.send("result2")
        except StopIteration as stop:
            assert stop.value == "result1+result2"

    def test_runner_slots(self):
        """Test that CoroutineOrchestratorRunner has __slots__."""
        assert hasattr(CoroutineOrchestratorRunner, "__slots__")

    def test_workflow_with_input_parameter(self):
        """Test workflow that accepts input parameter."""
        async def input_workflow(ctx: AsyncWorkflowContext, input_data: dict) -> str:
            name = input_data.get("name", "world")
            return f"Hello, {name}!"
        
        runner = CoroutineOrchestratorRunner(input_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        gen = runner.to_generator(async_ctx, {"name": "Alice"})
        
        with pytest.raises(StopIteration) as exc_info:
            next(gen)
        
        assert exc_info.value.value == "Hello, Alice!"

    def test_workflow_without_input_parameter(self):
        """Test workflow that doesn't accept input parameter."""
        async def no_input_workflow(ctx: AsyncWorkflowContext) -> str:
            return "No input needed"
        
        runner = CoroutineOrchestratorRunner(no_input_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        # Should work with None input
        gen = runner.to_generator(async_ctx, None)
        
        with pytest.raises(StopIteration) as exc_info:
            next(gen)
        
        assert exc_info.value.value == "No input needed"

        # Should also work with actual input (will be ignored)
        gen = runner.to_generator(async_ctx, {"ignored": "data"})
        
        with pytest.raises(StopIteration) as exc_info:
            next(gen)
        
        assert exc_info.value.value == "No input needed"
