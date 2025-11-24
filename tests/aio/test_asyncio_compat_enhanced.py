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
Comprehensive tests for enhanced asyncio compatibility features.
"""

import asyncio
import os
from datetime import datetime
from unittest.mock import Mock, patch

import pytest

from durabletask import task as dt_task
from durabletask.aio import (
    AsyncWorkflowContext,
    AsyncWorkflowError,
    CoroutineOrchestratorRunner,
    SandboxViolationError,
    WorkflowFunction,
)
from durabletask.aio.sandbox import _sandbox_scope


class TestAsyncWorkflowError:
    """Test the enhanced error handling."""

    def test_basic_error(self):
        error = AsyncWorkflowError("Test error")
        assert str(error) == "Test error"

    def test_error_with_context(self):
        error = AsyncWorkflowError(
            "Test error",
            instance_id="test-123",
            workflow_name="test_workflow",
            step="initialization",
        )
        expected = "Test error (workflow: test_workflow, instance: test-123, step: initialization)"
        assert str(error) == expected

    def test_error_partial_context(self):
        error = AsyncWorkflowError("Test error", instance_id="test-123")
        assert str(error) == "Test error (instance: test-123)"


class TestAsyncWorkflowContext:
    """Test enhanced AsyncWorkflowContext features."""

    def setup_method(self):
        self.mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        self.mock_base_ctx.instance_id = "test-instance-123"
        self.mock_base_ctx.current_utc_datetime = datetime(2023, 1, 1, 12, 0, 0)
        self.mock_base_ctx.is_replaying = False
        self.mock_base_ctx.is_suspended = False
        self.ctx = AsyncWorkflowContext(self.mock_base_ctx)

    def test_debug_mode_detection(self):
        with patch.dict(os.environ, {"DAPR_WF_DEBUG": "true"}):
            ctx = AsyncWorkflowContext(self.mock_base_ctx)
            assert ctx._debug_mode is True

        with patch.dict(os.environ, {"DT_DEBUG": "true"}):
            ctx = AsyncWorkflowContext(self.mock_base_ctx)
            assert ctx._debug_mode is True

        with patch.dict(os.environ, {}, clear=True):
            ctx = AsyncWorkflowContext(self.mock_base_ctx)
            assert ctx._debug_mode is False

    def test_operation_logging(self):
        with patch.dict(os.environ, {"DAPR_WF_DEBUG": "true"}):
            ctx = AsyncWorkflowContext(self.mock_base_ctx)

            ctx._log_operation("test_op", {"param": "value"})

            assert len(ctx._operation_history) == 1
            op = ctx._operation_history[0]
            assert op["operation"] == "test_op"
            assert op["details"] == {"param": "value"}
            assert op["sequence"] == 0

    def test_get_debug_info(self):
        with patch.dict(os.environ, {"DAPR_WF_DEBUG": "true"}):
            ctx = AsyncWorkflowContext(self.mock_base_ctx)
            ctx._log_operation("test_op", {"param": "value"})

            debug_info = ctx.get_debug_info()

            assert debug_info["instance_id"] == "test-instance-123"
            assert len(debug_info["operation_history"]) == 1
            assert debug_info["operation_history"][0]["type"] == "test_op"

    def test_cleanup_registry(self):
        cleanup_called = []

        def cleanup_fn():
            cleanup_called.append("sync")

        async def async_cleanup_fn():
            cleanup_called.append("async")

        self.ctx.add_cleanup(cleanup_fn)
        self.ctx.add_cleanup(async_cleanup_fn)

        # Test cleanup execution
        async def test_cleanup():
            async with self.ctx:
                pass

        asyncio.run(test_cleanup())

        # Cleanup should be called in reverse order
        assert cleanup_called == ["async", "sync"]

    def test_activity_logging(self):
        with patch.dict(os.environ, {"DAPR_WF_DEBUG": "true"}):
            ctx = AsyncWorkflowContext(self.mock_base_ctx)

            ctx.call_activity("test_activity", input="test")

            assert len(ctx._operation_history) == 1
            op = ctx._operation_history[0]
            assert op["operation"] == "activity"
            assert op["details"]["function"] == "test_activity"
            assert op["details"]["input"] == "test"

    def test_sleep_logging(self):
        with patch.dict(os.environ, {"DAPR_WF_DEBUG": "true"}):
            ctx = AsyncWorkflowContext(self.mock_base_ctx)

            ctx.create_timer(5.0)

            assert len(ctx._operation_history) == 1
            op = ctx._operation_history[0]
            assert op["operation"] == "sleep"
            assert op["details"]["duration"] == 5.0

    def test_with_timeout(self):
        mock_awaitable = Mock()
        timeout_awaitable = self.ctx.with_timeout(mock_awaitable, 10.0)

        assert timeout_awaitable is not None
        assert hasattr(timeout_awaitable, "_timeout")


class TestCoroutineOrchestratorRunner:
    """Test enhanced CoroutineOrchestratorRunner features."""

    def test_orchestrator_validation_success(self):
        async def valid_orchestrator(ctx, input_data):
            return "result"

        # Should not raise
        runner = CoroutineOrchestratorRunner(valid_orchestrator)
        assert runner is not None

    def test_orchestrator_validation_not_callable(self):
        with pytest.raises(AsyncWorkflowError, match="must be callable"):
            CoroutineOrchestratorRunner("not_callable")

    def test_orchestrator_validation_wrong_params(self):
        async def wrong_params():  # No parameters - should fail
            return "result"

        with pytest.raises(AsyncWorkflowError, match="at least one parameter"):
            CoroutineOrchestratorRunner(wrong_params)

    def test_orchestrator_validation_not_async(self):
        def not_async(ctx, input_data):
            return "result"

        with pytest.raises(AsyncWorkflowError, match="must be an async function"):
            CoroutineOrchestratorRunner(not_async)

    def test_enhanced_error_context(self):
        async def failing_orchestrator(ctx, input_data):
            raise ValueError("Test error")

        runner = CoroutineOrchestratorRunner(failing_orchestrator)
        mock_ctx = Mock(spec=dt_task.OrchestrationContext)
        mock_ctx.instance_id = "test-123"
        async_ctx = AsyncWorkflowContext(mock_ctx)

        gen = runner.to_generator(async_ctx, "input")

        with pytest.raises(AsyncWorkflowError) as exc_info:
            next(gen)

        error = exc_info.value
        assert "initialization" in str(error)
        assert "test-123" in str(error)


class TestEnhancedSandboxing:
    """Test enhanced sandboxing capabilities."""

    def setup_method(self):
        self.mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        self.mock_base_ctx.instance_id = "test-instance"
        self.mock_base_ctx.current_utc_datetime = datetime(2023, 1, 1, 12, 0, 0)
        self.mock_base_ctx.is_replaying = False
        self.mock_base_ctx.is_suspended = False
        self.async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

    def test_datetime_patching_limitation(self):
        # Note: datetime.datetime is immutable and cannot be patched
        # This test documents the current limitation
        import datetime as dt

        with _sandbox_scope(self.async_ctx, "best_effort"):
            # datetime.now cannot be patched due to immutability
            # Users should use ctx.now() instead
            now_result = dt.datetime.now()

            # This will NOT be the deterministic time (unless by coincidence)
            # We just verify that the call works and returns a datetime
            assert isinstance(now_result, datetime)

            # The deterministic time is available via ctx.now()
            deterministic_time = self.async_ctx.now()
            assert isinstance(deterministic_time, datetime)

        # datetime.datetime methods remain unchanged (they can't be patched)
        assert hasattr(dt.datetime, "now")
        assert hasattr(dt.datetime, "utcnow")

    def test_random_getrandbits_patching(self):
        import random

        original_getrandbits = random.getrandbits

        with _sandbox_scope(self.async_ctx, "best_effort"):
            # Should use deterministic random
            result1 = random.getrandbits(32)
            result2 = random.getrandbits(32)
            assert isinstance(result1, int)
            assert isinstance(result2, int)

        # Should be restored
        assert random.getrandbits is original_getrandbits

    def test_strict_mode_file_blocking(self):
        with pytest.raises(SandboxViolationError, match="File I/O operations are not allowed"):
            with _sandbox_scope(self.async_ctx, "strict"):
                open("test.txt", "w")

    def test_strict_mode_urandom_blocking(self):
        import os

        if hasattr(os, "urandom"):
            with pytest.raises(SandboxViolationError, match="os.urandom is not allowed"):
                with _sandbox_scope(self.async_ctx, "strict"):
                    os.urandom(16)

    def test_strict_mode_secrets_blocking(self):
        try:
            import secrets

            with pytest.raises(SandboxViolationError, match="secrets module is not allowed"):
                with _sandbox_scope(self.async_ctx, "strict"):
                    secrets.token_bytes(16)
        except ImportError:
            # secrets module not available, skip test
            pass

    def test_asyncio_sleep_patching(self):
        import asyncio

        original_sleep = asyncio.sleep

        with _sandbox_scope(self.async_ctx, "best_effort"):
            # asyncio.sleep should be patched
            sleep_awaitable = asyncio.sleep(1.0)
            assert hasattr(sleep_awaitable, "__await__")

        # Should be restored
        assert asyncio.sleep is original_sleep


class TestConcurrencyPrimitives:
    """Test enhanced concurrency primitives."""

    def setup_method(self):
        self.mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        self.mock_base_ctx.instance_id = "test-instance"
        self.mock_base_ctx.is_replaying = False
        self.mock_base_ctx.is_suspended = False
        self.ctx = AsyncWorkflowContext(self.mock_base_ctx)

    def test_timeout_awaitable(self):
        from durabletask.aio import TimeoutAwaitable

        mock_awaitable = Mock()
        timeout_awaitable = TimeoutAwaitable(mock_awaitable, 5.0, self.ctx)

        assert timeout_awaitable._awaitable is mock_awaitable
        assert timeout_awaitable._timeout == 5.0
        assert timeout_awaitable._ctx is self.ctx


class TestPerformanceOptimizations:
    """Test performance optimizations."""

    def test_awaitable_slots(self):
        from durabletask.aio import (
            ActivityAwaitable,
            ExternalEventAwaitable,
            SleepAwaitable,
            SubOrchestratorAwaitable,
            SwallowExceptionAwaitable,
            WhenAllAwaitable,
            WhenAnyAwaitable,
        )

        # All awaitable classes should have __slots__
        classes_with_slots = [
            ActivityAwaitable,
            SubOrchestratorAwaitable,
            SleepAwaitable,
            ExternalEventAwaitable,
            WhenAllAwaitable,
            WhenAnyAwaitable,
            SwallowExceptionAwaitable,
        ]

        for cls in classes_with_slots:
            assert hasattr(cls, "__slots__"), f"{cls.__name__} should have __slots__"


class TestWorkflowFunctionProtocol:
    """Test WorkflowFunction protocol."""

    def test_valid_workflow_function(self):
        async def valid_workflow(ctx: AsyncWorkflowContext, input_data) -> str:
            return "result"

        # Should be recognized as WorkflowFunction
        assert isinstance(valid_workflow, WorkflowFunction)

    def test_invalid_workflow_function(self):
        def not_async_workflow(ctx, input_data):
            return "result"

        # Note: runtime_checkable protocols are structural, not nominal
        # A function with the right signature will pass isinstance check
        # The actual validation happens in CoroutineOrchestratorRunner
        # This test documents the current behavior
        assert isinstance(
            not_async_workflow, WorkflowFunction
        )  # This passes due to structural typing


if __name__ == "__main__":
    pytest.main([__file__])
