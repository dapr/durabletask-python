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
Compatibility tests to ensure AsyncWorkflowContext maintains API compatibility
with the base OrchestrationContext interface.

This test suite validates that AsyncWorkflowContext provides all the properties
and methods expected by the base OrchestrationContext, helping prevent regressions
and ensuring smooth upstream merges.
"""

import inspect
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest

from durabletask import task
from durabletask.aio import AsyncWorkflowContext


class TestAsyncWorkflowContextCompatibility:
    """Test suite to validate AsyncWorkflowContext compatibility with OrchestrationContext."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_base_ctx = Mock(spec=task.OrchestrationContext)
        self.mock_base_ctx.instance_id = "test-instance-123"
        self.mock_base_ctx.current_utc_datetime = datetime(2023, 1, 1, 12, 0, 0)
        self.mock_base_ctx.is_replaying = False
        self.mock_base_ctx.workflow_name = "test_workflow"
        self.mock_base_ctx.is_suspended = False

        self.async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

    def test_all_orchestration_context_properties_exist(self):
        """Test that AsyncWorkflowContext has all properties from OrchestrationContext."""
        # Get all properties from OrchestrationContext
        orchestration_properties = []
        for name, member in inspect.getmembers(task.OrchestrationContext):
            if isinstance(member, property) and not name.startswith("_"):
                orchestration_properties.append(name)

        # Check that AsyncWorkflowContext has all these properties
        for prop_name in orchestration_properties:
            assert hasattr(self.async_ctx, prop_name), (
                f"AsyncWorkflowContext is missing property: {prop_name}"
            )

            # Verify the property is actually callable (not just an attribute)
            prop_value = getattr(self.async_ctx, prop_name)
            assert prop_value is not None, f"Property {prop_name} returned None unexpectedly"

    def test_all_orchestration_context_methods_exist(self):
        """Test that AsyncWorkflowContext has all methods from OrchestrationContext."""
        # Get all abstract methods from OrchestrationContext
        orchestration_methods = []
        for name, member in inspect.getmembers(task.OrchestrationContext):
            if (inspect.isfunction(member) or inspect.ismethod(member)) and not name.startswith(
                "_"
            ):
                orchestration_methods.append(name)

        # Check that AsyncWorkflowContext has all these methods
        for method_name in orchestration_methods:
            assert hasattr(self.async_ctx, method_name), (
                f"AsyncWorkflowContext is missing method: {method_name}"
            )

            # Verify the method is callable
            method = getattr(self.async_ctx, method_name)
            assert callable(method), f"Method {method_name} is not callable"

    def test_property_compatibility_instance_id(self):
        """Test instance_id property compatibility."""
        assert self.async_ctx.instance_id == "test-instance-123"
        assert isinstance(self.async_ctx.instance_id, str)

    def test_property_compatibility_current_utc_datetime(self):
        """Test current_utc_datetime property compatibility."""
        assert self.async_ctx.current_utc_datetime == datetime(2023, 1, 1, 12, 0, 0)
        assert isinstance(self.async_ctx.current_utc_datetime, datetime)

    def test_property_compatibility_is_replaying(self):
        """Test is_replaying property compatibility."""
        assert self.async_ctx.is_replaying is False
        assert isinstance(self.async_ctx.is_replaying, bool)

    def test_property_compatibility_workflow_name(self):
        """Test workflow_name property compatibility."""
        assert self.async_ctx.workflow_name == "test_workflow"
        assert isinstance(self.async_ctx.workflow_name, (str, type(None)))

    def test_property_compatibility_is_suspended(self):
        """Test is_suspended property compatibility."""
        assert self.async_ctx.is_suspended is False
        assert isinstance(self.async_ctx.is_suspended, bool)

    def test_method_compatibility_set_custom_status(self):
        """Test set_custom_status method compatibility."""
        # Test that method exists and can be called
        self.async_ctx.set_custom_status({"status": "running"})
        self.mock_base_ctx.set_custom_status.assert_called_once_with({"status": "running"})

    def test_method_compatibility_create_timer(self):
        """Test create_timer method compatibility."""
        # Mock the return value
        mock_task = Mock(spec=task.Task)
        self.mock_base_ctx.create_timer.return_value = mock_task

        # Test with timedelta
        timer_awaitable = self.async_ctx.create_timer(timedelta(seconds=30))
        assert timer_awaitable is not None

        # Test with datetime
        future_time = datetime(2023, 1, 1, 13, 0, 0)
        timer_awaitable2 = self.async_ctx.create_timer(future_time)
        assert timer_awaitable2 is not None

    def test_method_compatibility_call_activity(self):
        """Test call_activity method compatibility."""

        def test_activity(input_data):
            return f"processed: {input_data}"

        activity_awaitable = self.async_ctx.call_activity(test_activity, input="test")
        assert activity_awaitable is not None

        # Test with retry policy
        retry_policy = task.RetryPolicy(
            first_retry_interval=timedelta(seconds=1), max_number_of_attempts=3
        )
        activity_awaitable2 = self.async_ctx.call_activity(
            test_activity, input="test", retry_policy=retry_policy
        )
        assert activity_awaitable2 is not None

    def test_method_compatibility_call_sub_orchestrator(self):
        """Test call_sub_orchestrator method compatibility."""

        async def test_orchestrator(ctx, input_data):
            return f"orchestrated: {input_data}"

        sub_orch_awaitable = self.async_ctx.call_sub_orchestrator(test_orchestrator, input="test")
        assert sub_orch_awaitable is not None

        # Test with instance_id and retry_policy
        retry_policy = task.RetryPolicy(
            first_retry_interval=timedelta(seconds=2), max_number_of_attempts=2
        )
        sub_orch_awaitable2 = self.async_ctx.call_sub_orchestrator(
            test_orchestrator, input="test", instance_id="sub-123", retry_policy=retry_policy
        )
        assert sub_orch_awaitable2 is not None

    def test_method_compatibility_wait_for_external_event(self):
        """Test wait_for_external_event method compatibility."""
        event_awaitable = self.async_ctx.wait_for_external_event("test_event")
        assert event_awaitable is not None

    def test_method_compatibility_continue_as_new(self):
        """Test continue_as_new method compatibility."""
        # Test basic call
        self.async_ctx.continue_as_new({"new": "input"})
        self.mock_base_ctx.continue_as_new.assert_called_once_with({"new": "input"})

    def test_method_signature_compatibility(self):
        """Test that method signatures are compatible with OrchestrationContext."""
        # Get method signatures from both classes
        base_methods = {}
        for name, method in inspect.getmembers(
            task.OrchestrationContext, predicate=inspect.isfunction
        ):
            if not name.startswith("_"):
                base_methods[name] = inspect.signature(method)

        async_methods = {}
        for name, method in inspect.getmembers(AsyncWorkflowContext, predicate=inspect.ismethod):
            if not name.startswith("_") and name in base_methods:
                async_methods[name] = inspect.signature(method)

        # Compare signatures (allowing for additional parameters in async version)
        for method_name, base_sig in base_methods.items():
            if method_name in async_methods:
                async_sig = async_methods[method_name]

                # Check that all base parameters exist in async version
                base_params = list(base_sig.parameters.keys())
                async_params = list(async_sig.parameters.keys())

                # Skip 'self' parameter for comparison
                if "self" in base_params:
                    base_params.remove("self")
                if "self" in async_params:
                    async_params.remove("self")

                # Async version can have additional parameters, but must have all base ones
                for param in base_params:
                    assert param in async_params or param == "self", (
                        f"Method {method_name} missing parameter {param} in AsyncWorkflowContext"
                    )

    def test_return_type_compatibility(self):
        """Test that methods return compatible types."""

        # Test that activity calls return awaitables
        def test_activity():
            return "result"

        activity_result = self.async_ctx.call_activity(test_activity)
        assert hasattr(activity_result, "__await__"), "call_activity should return an awaitable"

        # Test that timer calls return awaitables
        timer_result = self.async_ctx.create_timer(timedelta(seconds=1))
        assert hasattr(timer_result, "__await__"), "create_timer should return an awaitable"

        # Test that external event calls return awaitables
        event_result = self.async_ctx.wait_for_external_event("test")
        assert hasattr(event_result, "__await__"), (
            "wait_for_external_event should return an awaitable"
        )

    def test_async_context_additional_methods(self):
        """Test that AsyncWorkflowContext provides additional async-specific methods."""
        # These are enhancements that don't exist in base OrchestrationContext
        additional_methods = [
            "sleep",  # Alias for create_timer
            "sub_orchestrator",  # Alias for call_sub_orchestrator
            "when_all",  # Concurrency primitive
            "when_any",  # Concurrency primitive
            "when_any_with_result",  # Enhanced concurrency primitive
            "with_timeout",  # Timeout wrapper
            "gather",  # asyncio.gather equivalent
            "now",  # Deterministic datetime (from mixin)
            "random",  # Deterministic random (from mixin)
            "uuid4",  # Deterministic UUID (from mixin)
            "new_guid",  # Alias for uuid4
            "random_string",  # Deterministic string generation
            "add_cleanup",  # Cleanup task registration
            "get_debug_info",  # Debug information
        ]

        for method_name in additional_methods:
            assert hasattr(self.async_ctx, method_name), (
                f"AsyncWorkflowContext missing enhanced method: {method_name}"
            )

            method = getattr(self.async_ctx, method_name)
            assert callable(method), f"Enhanced method {method_name} is not callable"

    def test_async_context_manager_compatibility(self):
        """Test that AsyncWorkflowContext supports async context manager protocol."""
        assert hasattr(self.async_ctx, "__aenter__"), (
            "AsyncWorkflowContext should support async context manager (__aenter__)"
        )
        assert hasattr(self.async_ctx, "__aexit__"), (
            "AsyncWorkflowContext should support async context manager (__aexit__)"
        )

    def test_property_delegation_to_base_context(self):
        """Test that properties correctly delegate to the base context."""
        # Change base context values and verify async context reflects them
        self.mock_base_ctx.instance_id = "new-instance-456"
        assert self.async_ctx.instance_id == "new-instance-456"

        new_time = datetime(2023, 6, 15, 10, 30, 0)
        self.mock_base_ctx.current_utc_datetime = new_time
        assert self.async_ctx.current_utc_datetime == new_time

        self.mock_base_ctx.is_replaying = True
        assert self.async_ctx.is_replaying is True

    def test_method_delegation_to_base_context(self):
        """Test that methods correctly delegate to the base context."""
        # Test set_custom_status delegation
        self.async_ctx.set_custom_status("test_status")
        self.mock_base_ctx.set_custom_status.assert_called_with("test_status")

        # Test continue_as_new delegation
        self.async_ctx.continue_as_new("new_input")
        self.mock_base_ctx.continue_as_new.assert_called_with("new_input")


class TestOrchestrationContextProtocolCompliance:
    """Test that AsyncWorkflowContext can be used wherever OrchestrationContext is expected."""

    def test_async_context_is_orchestration_context_compatible(self):
        """Test that AsyncWorkflowContext can be used as OrchestrationContext."""
        mock_base_ctx = Mock(spec=task.OrchestrationContext)
        mock_base_ctx.instance_id = "test-123"
        mock_base_ctx.current_utc_datetime = datetime.now()
        mock_base_ctx.is_replaying = False

        async_ctx = AsyncWorkflowContext(mock_base_ctx)

        # Test that it can be used in functions expecting OrchestrationContext
        def function_expecting_orchestration_context(ctx: task.OrchestrationContext) -> str:
            return f"Instance: {ctx.instance_id}, Replaying: {ctx.is_replaying}"

        # This should work without type errors
        result = function_expecting_orchestration_context(async_ctx)
        assert "test-123" in result
        assert "False" in result

    def test_duck_typing_compatibility(self):
        """Test that AsyncWorkflowContext satisfies duck typing for OrchestrationContext."""
        mock_base_ctx = Mock(spec=task.OrchestrationContext)
        mock_base_ctx.instance_id = "duck-test"
        mock_base_ctx.current_utc_datetime = datetime(2023, 1, 1)
        mock_base_ctx.is_replaying = False
        mock_base_ctx.workflow_name = "duck_workflow"

        async_ctx = AsyncWorkflowContext(mock_base_ctx)

        # Test all the key properties and methods that would be used in duck typing
        assert hasattr(async_ctx, "instance_id")
        assert hasattr(async_ctx, "current_utc_datetime")
        assert hasattr(async_ctx, "is_replaying")
        assert hasattr(async_ctx, "call_activity")
        assert hasattr(async_ctx, "call_sub_orchestrator")
        assert hasattr(async_ctx, "create_timer")
        assert hasattr(async_ctx, "wait_for_external_event")
        assert hasattr(async_ctx, "set_custom_status")
        assert hasattr(async_ctx, "continue_as_new")

        # Test that they return the expected types
        assert isinstance(async_ctx.instance_id, str)
        assert isinstance(async_ctx.current_utc_datetime, datetime)
        assert isinstance(async_ctx.is_replaying, bool)


if __name__ == "__main__":
    pytest.main([__file__])
