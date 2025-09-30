# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for driver functionality in durabletask.aio.
"""

from typing import Any
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
            return 'result'

        # Should be recognized as WorkflowFunction
        assert isinstance(valid_workflow, WorkflowFunction)

    def test_non_async_function_protocol(self):
        """Test that non-async functions are still recognized structurally."""

        # Non-async function with correct signature
        def not_async_workflow(ctx: AsyncWorkflowContext, input_data: Any) -> str:
            return 'result'

        # Should still be recognized as WorkflowFunction due to structural typing
        # The actual async validation happens in CoroutineOrchestratorRunner
        assert isinstance(not_async_workflow, WorkflowFunction)


class TestCoroutineOrchestratorRunner:
    """Test CoroutineOrchestratorRunner functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        self.mock_base_ctx.instance_id = 'test-instance'
        self.mock_base_ctx.current_utc_datetime = Mock()

    def test_runner_creation(self):
        """Test creating a CoroutineOrchestratorRunner."""

        async def test_workflow(ctx: AsyncWorkflowContext, input_data: Any) -> str:
            return 'result'

        runner = CoroutineOrchestratorRunner(test_workflow)

        assert runner._async_orchestrator is test_workflow
        assert runner._sandbox_mode == 'off'
        assert runner._workflow_name == 'test_workflow'

    def test_runner_with_sandbox_mode(self):
        """Test creating runner with sandbox mode."""

        async def test_workflow(ctx: AsyncWorkflowContext, input_data: Any) -> str:
            return 'result'

        runner = CoroutineOrchestratorRunner(test_workflow, sandbox_mode='strict')

        assert runner._sandbox_mode == 'strict'

    def test_runner_with_lambda_function(self):
        """Test creating runner with lambda function."""

        # Lambda functions must be async to be valid
        def lambda_workflow(ctx, input_data):
            return 'result'

        # Should raise validation error for non-async lambda
        with pytest.raises(WorkflowValidationError) as exc_info:
            CoroutineOrchestratorRunner(lambda_workflow)

        assert 'async function' in str(exc_info.value)

    def test_simple_synchronous_workflow(self):
        """Test running a simple synchronous workflow."""

        async def simple_workflow(ctx: AsyncWorkflowContext) -> str:
            return 'hello world'

        runner = CoroutineOrchestratorRunner(simple_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        # Convert to generator and run
        gen = runner.to_generator(async_ctx, None)

        # Should complete immediately with StopIteration
        with pytest.raises(StopIteration) as exc_info:
            next(gen)

        assert exc_info.value.value == 'hello world'

    def test_workflow_with_single_activity(self):
        """Test workflow with a single activity call."""

        async def activity_workflow(ctx: AsyncWorkflowContext, input_data: str) -> str:
            result = await ctx.call_activity('test_activity', input=input_data)
            return f'processed: {result}'

        # Mock the activity call
        mock_task = Mock(spec=dt_task.Task)
        self.mock_base_ctx.call_activity.return_value = mock_task

        runner = CoroutineOrchestratorRunner(activity_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        # Convert to generator
        gen = runner.to_generator(async_ctx, 'test_input')

        # First yield should be the activity task
        yielded_task = next(gen)
        assert yielded_task is mock_task

        # Send result back
        try:
            gen.send('activity_result')
        except StopIteration as stop:
            assert stop.value == 'processed: activity_result'
        else:
            pytest.fail('Expected StopIteration')

    def test_workflow_initialization_error(self):
        """Test workflow initialization error handling."""

        async def failing_workflow(ctx: AsyncWorkflowContext, input_data: Any) -> str:
            raise ValueError('Initialization failed')

        runner = CoroutineOrchestratorRunner(failing_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        # The error should be raised when we try to start the generator
        gen = runner.to_generator(async_ctx, None)

        with pytest.raises(AsyncWorkflowError) as exc_info:
            next(gen)  # This will trigger the initialization error

        assert 'Workflow failed during initialization' in str(exc_info.value)
        assert exc_info.value.workflow_name == 'failing_workflow'
        assert exc_info.value.step == 'initialization'

    def test_workflow_invalid_signature(self):
        """Test workflow with invalid signature."""

        async def invalid_workflow() -> str:  # Missing ctx parameter
            return 'result'

        # Should raise validation error during runner creation
        with pytest.raises(WorkflowValidationError) as exc_info:
            CoroutineOrchestratorRunner(invalid_workflow)

        assert 'at least one parameter' in str(exc_info.value)

    def test_workflow_yielding_invalid_object(self):
        """Test workflow yielding invalid object."""

        # Create a workflow that yields an invalid object
        # We need to simulate this by creating a workflow that awaits something invalid
        class InvalidAwaitable:
            def __await__(self):
                yield 'invalid'  # This will cause the error
                return 'result'

        async def invalid_yield_workflow(ctx: AsyncWorkflowContext) -> str:
            result = await InvalidAwaitable()
            return result

        runner = CoroutineOrchestratorRunner(invalid_yield_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        gen = runner.to_generator(async_ctx, None)

        with pytest.raises(AsyncWorkflowError) as exc_info:
            next(gen)

        assert 'awaited unsupported object type' in str(exc_info.value)

    def test_workflow_with_direct_task_yield(self):
        """Test workflow with custom awaitable that yields task directly."""

        # Create a custom awaitable that yields task directly (current approach)
        class DirectTaskAwaitable:
            def __init__(self, task):
                self.task = task

            def __await__(self):
                result = yield self.task
                return f'result: {result}'

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
            gen.send('operation_result')
        except StopIteration as stop:
            assert stop.value == 'result: operation_result'

    def test_workflow_exception_handling(self):
        """Test workflow exception handling during execution."""

        async def exception_workflow(ctx: AsyncWorkflowContext) -> str:
            result = await ctx.call_activity('failing_activity')
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
        test_exception = Exception('Activity failed')
        try:
            gen.throw(test_exception)
        except StopIteration:
            pytest.fail('Expected exception to propagate')
        except AsyncWorkflowError as e:
            # The driver wraps the original exception in AsyncWorkflowError
            assert 'Activity failed' in str(e)
            assert e.workflow_name == 'exception_workflow'

    def test_workflow_step_tracking(self):
        """Test that workflow steps are tracked for error reporting."""

        # Test that the runner correctly tracks workflow name and steps
        async def multi_step_workflow(ctx: AsyncWorkflowContext) -> str:
            result1 = await ctx.call_activity('step1')
            result2 = await ctx.call_activity('step2')
            return f'{result1}+{result2}'

        # Mock the activity calls
        mock_task = Mock(spec=dt_task.Task)
        self.mock_base_ctx.call_activity.return_value = mock_task

        runner = CoroutineOrchestratorRunner(multi_step_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        # Verify workflow name is tracked
        assert runner._workflow_name == 'multi_step_workflow'

        gen = runner.to_generator(async_ctx, None)

        # First step
        yielded_task = next(gen)
        assert yielded_task is mock_task

        # Complete first step
        yielded_task = gen.send('result1')
        assert yielded_task is mock_task

        # Complete second step
        try:
            gen.send('result2')
        except StopIteration as stop:
            assert stop.value == 'result1+result2'

    def test_runner_slots(self):
        """Test that CoroutineOrchestratorRunner has __slots__."""
        assert hasattr(CoroutineOrchestratorRunner, '__slots__')

    def test_workflow_too_many_parameters(self):
        """Test workflow with too many parameters."""

        async def too_many_params_workflow(
            ctx: AsyncWorkflowContext, input_data: Any, extra: Any
        ) -> str:
            return 'result'

        # Should raise validation error during runner creation
        with pytest.raises(WorkflowValidationError) as exc_info:
            CoroutineOrchestratorRunner(too_many_params_workflow)

        assert 'at most two parameters' in str(exc_info.value)
        assert exc_info.value.validation_type == 'function_signature'

    def test_workflow_not_callable(self):
        """Test workflow that is not callable."""
        not_callable = 'not a function'

        # Should raise validation error during runner creation
        with pytest.raises(WorkflowValidationError) as exc_info:
            CoroutineOrchestratorRunner(not_callable)

        assert 'must be callable' in str(exc_info.value)
        assert exc_info.value.validation_type == 'function_type'

    def test_workflow_coroutine_instantiation_error(self):
        """Test error during coroutine instantiation."""

        async def problematic_workflow(ctx: AsyncWorkflowContext, input_data: Any) -> str:
            return 'result'

        # Mock the workflow to raise TypeError when called
        runner = CoroutineOrchestratorRunner(problematic_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        # Replace the orchestrator with one that raises TypeError
        def bad_orchestrator(*args, **kwargs):
            raise TypeError('Bad instantiation')

        runner._async_orchestrator = bad_orchestrator

        gen = runner.to_generator(async_ctx, None)

        with pytest.raises(AsyncWorkflowError) as exc_info:
            next(gen)

        assert 'Failed to instantiate workflow coroutine' in str(exc_info.value)
        assert exc_info.value.step == 'initialization'

    def test_workflow_with_direct_task_awaitable(self):
        """Test workflow that awaits a Task directly (tests Task branch in to_iter)."""

        async def direct_task_workflow(ctx: AsyncWorkflowContext) -> str:
            # This will be caught by the to_iter function's Task branch
            mock_task = Mock(spec=dt_task.Task)
            # We need to make the coroutine return a Task directly, not await it
            return mock_task

        runner = CoroutineOrchestratorRunner(direct_task_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        gen = runner.to_generator(async_ctx, None)

        # Should complete immediately since it's synchronous
        try:
            next(gen)
        except StopIteration as stop:
            assert isinstance(stop.value, Mock)

    def test_awaitable_completes_synchronously(self):
        """Test awaitable that completes without yielding."""

        class SyncAwaitable:
            def __await__(self):
                # Complete immediately without yielding
                return
                yield  # unreachable but makes this a generator

        async def sync_awaitable_workflow(ctx: AsyncWorkflowContext) -> str:
            await SyncAwaitable()
            return 'completed'

        runner = CoroutineOrchestratorRunner(sync_awaitable_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        gen = runner.to_generator(async_ctx, None)

        # Should complete without yielding any tasks
        with pytest.raises(StopIteration) as exc_info:
            next(gen)

        assert exc_info.value.value == 'completed'

    def test_awaitable_yields_non_task(self):
        """Test awaitable that yields non-Task object during execution."""

        class BadAwaitable:
            def __await__(self):
                yield 'not a task'  # This should trigger the non-Task error
                return 'result'

        async def bad_awaitable_workflow(ctx: AsyncWorkflowContext) -> str:
            result = await BadAwaitable()
            return result

        runner = CoroutineOrchestratorRunner(bad_awaitable_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        gen = runner.to_generator(async_ctx, None)

        with pytest.raises(AsyncWorkflowError) as exc_info:
            next(gen)

        assert 'awaited unsupported object type' in str(exc_info.value)
        assert exc_info.value.step == 'awaitable_conversion'

    def test_awaitable_exception_handling_with_completion(self):
        """Test exception handling where awaitable completes after exception."""

        class ExceptionThenCompleteAwaitable:
            def __init__(self):
                self.threw = False

            def __await__(self):
                task = Mock(spec=dt_task.Task)
                try:
                    result = yield task
                    return f'normal: {result}'
                except Exception as e:
                    self.threw = True
                    return f'exception handled: {e}'

        async def exception_handling_workflow(ctx: AsyncWorkflowContext) -> str:
            awaitable = ExceptionThenCompleteAwaitable()
            result = await awaitable
            return result

        runner = CoroutineOrchestratorRunner(exception_handling_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        gen = runner.to_generator(async_ctx, None)

        # Get the task
        _ = next(gen)

        # Throw an exception
        test_exception = Exception('test error')
        try:
            gen.throw(test_exception)
        except StopIteration as stop:
            assert 'exception handled: test error' in stop.value

    def test_awaitable_exception_propagation(self):
        """Test exception propagation through awaitable."""

        class ExceptionPropagatingAwaitable:
            def __await__(self):
                task = Mock(spec=dt_task.Task)
                result = yield task
                return result

        async def exception_propagation_workflow(ctx: AsyncWorkflowContext) -> str:
            result = await ExceptionPropagatingAwaitable()
            return result

        runner = CoroutineOrchestratorRunner(exception_propagation_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        gen = runner.to_generator(async_ctx, None)

        # Get the task
        _ = next(gen)

        # Throw an exception that should propagate to the coroutine
        test_exception = Exception('propagated error')
        with pytest.raises(AsyncWorkflowError) as exc_info:
            gen.throw(test_exception)

        assert 'propagated error' in str(exc_info.value)
        assert exc_info.value.step == 'execution'

    def test_multi_yield_awaitable(self):
        """Test awaitable that yields multiple tasks."""

        class MultiYieldAwaitable:
            def __await__(self):
                task1 = Mock(spec=dt_task.Task)
                task2 = Mock(spec=dt_task.Task)
                result1 = yield task1
                result2 = yield task2
                return f'{result1}+{result2}'

        async def multi_yield_workflow(ctx: AsyncWorkflowContext) -> str:
            result = await MultiYieldAwaitable()
            return result

        runner = CoroutineOrchestratorRunner(multi_yield_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        gen = runner.to_generator(async_ctx, None)

        # First task
        task1 = next(gen)
        assert isinstance(task1, Mock)

        # Second task
        task2 = gen.send('result1')
        assert isinstance(task2, Mock)

        # Final result
        try:
            gen.send('result2')
        except StopIteration as stop:
            assert stop.value == 'result1+result2'

    def test_multi_yield_awaitable_with_non_task(self):
        """Test multi-yield awaitable that yields non-Task."""

        class BadMultiYieldAwaitable:
            def __await__(self):
                task1 = Mock(spec=dt_task.Task)
                result1 = yield task1
                yield 'not a task'  # This should cause error
                return result1

        async def bad_multi_yield_workflow(ctx: AsyncWorkflowContext) -> str:
            result = await BadMultiYieldAwaitable()
            return result

        runner = CoroutineOrchestratorRunner(bad_multi_yield_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        gen = runner.to_generator(async_ctx, None)

        # First task
        _ = next(gen)

        # Send result, should get error on second yield
        with pytest.raises(AsyncWorkflowError) as exc_info:
            gen.send('result1')

        assert 'awaited unsupported object type' in str(exc_info.value)

    def test_multi_yield_awaitable_exception_in_continuation(self):
        """Test exception handling in multi-yield awaitable continuation."""

        class ExceptionInContinuationAwaitable:
            def __await__(self):
                task1 = Mock(spec=dt_task.Task)
                _ = yield task1
                # This will cause an exception when we try to continue
                raise ValueError('continuation error')

        async def exception_continuation_workflow(ctx: AsyncWorkflowContext) -> str:
            result = await ExceptionInContinuationAwaitable()
            return result

        runner = CoroutineOrchestratorRunner(exception_continuation_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        gen = runner.to_generator(async_ctx, None)

        # First task
        _ = next(gen)

        # Send result, should get error in continuation
        with pytest.raises(AsyncWorkflowError) as exc_info:
            gen.send('result1')

        assert 'continuation error' in str(exc_info.value)

    def test_runner_properties(self):
        """Test runner property getters."""

        async def test_workflow(ctx: AsyncWorkflowContext) -> str:
            return 'result'

        runner = CoroutineOrchestratorRunner(
            test_workflow, sandbox_mode='strict', workflow_name='custom_name'
        )

        assert runner.workflow_name == 'custom_name'
        assert runner.sandbox_mode == 'strict'

    def test_runner_with_custom_workflow_name(self):
        """Test runner with custom workflow name."""

        async def test_workflow(ctx: AsyncWorkflowContext) -> str:
            return 'result'

        runner = CoroutineOrchestratorRunner(test_workflow, workflow_name='custom_workflow')

        assert runner._workflow_name == 'custom_workflow'

    def test_runner_with_function_without_name(self):
        """Test runner with function that has no __name__ attribute."""

        async def test_workflow(ctx: AsyncWorkflowContext) -> str:
            return 'result'

        # Mock getattr to return None for __name__
        from unittest.mock import patch

        with patch('durabletask.aio.driver.getattr') as mock_getattr:

            def side_effect(obj, attr, default=None):
                if attr == '__name__':
                    return None  # Simulate missing __name__
                return getattr(obj, attr, default)

            mock_getattr.side_effect = side_effect

            runner = CoroutineOrchestratorRunner(test_workflow)
            assert runner._workflow_name == 'unknown'

    def test_awaitable_that_yields_task_then_non_task(self):
        """Test awaitable that first yields a Task, then yields non-Task (hits line 269-277)."""

        class TaskThenNonTaskAwaitable:
            def __await__(self):
                task1 = Mock(spec=dt_task.Task)
                result1 = yield task1
                # This second yield should trigger the non-Task error in the while loop
                yield 'not a task'
                return result1

        async def task_then_non_task_workflow(ctx: AsyncWorkflowContext) -> str:
            result = await TaskThenNonTaskAwaitable()
            return result

        runner = CoroutineOrchestratorRunner(task_then_non_task_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        gen = runner.to_generator(async_ctx, None)

        # First task should be yielded
        task1 = next(gen)
        assert isinstance(task1, Mock)

        # Send result, should get error on second yield
        with pytest.raises(AsyncWorkflowError) as exc_info:
            gen.send('result1')

        assert 'awaited unsupported object type' in str(exc_info.value)
        assert exc_info.value.step == 'awaitable_conversion'

    def test_workflow_with_input_parameter(self):
        """Test workflow that accepts input parameter."""

        async def input_workflow(ctx: AsyncWorkflowContext, input_data: dict) -> str:
            name = input_data.get('name', 'world')
            return f'Hello, {name}!'

        runner = CoroutineOrchestratorRunner(input_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        gen = runner.to_generator(async_ctx, {'name': 'Alice'})

        with pytest.raises(StopIteration) as exc_info:
            next(gen)

        assert exc_info.value.value == 'Hello, Alice!'

    def test_workflow_without_input_parameter(self):
        """Test workflow that doesn't accept input parameter."""

        async def no_input_workflow(ctx: AsyncWorkflowContext) -> str:
            return 'No input needed'

        runner = CoroutineOrchestratorRunner(no_input_workflow)
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        # Should work with None input
        gen = runner.to_generator(async_ctx, None)

        with pytest.raises(StopIteration) as exc_info:
            next(gen)

        assert exc_info.value.value == 'No input needed'

        # Should also work with actual input (will be ignored)
        gen = runner.to_generator(async_ctx, {'ignored': 'data'})

        with pytest.raises(StopIteration) as exc_info:
            next(gen)

        assert exc_info.value.value == 'No input needed'
