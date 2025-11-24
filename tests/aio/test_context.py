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
Tests for AsyncWorkflowContext in durabletask.aio.
"""

import random
import uuid
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest

from durabletask import task as dt_task
from durabletask.aio import (
    ActivityAwaitable,
    AsyncWorkflowContext,
    AwaitableBase,
    ExternalEventAwaitable,
    SleepAwaitable,
    SubOrchestratorAwaitable,
    TimeoutAwaitable,
    WhenAllAwaitable,
    WhenAnyAwaitable,
)


class TestAsyncWorkflowContext:
    """Test AsyncWorkflowContext functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        self.mock_base_ctx.instance_id = "test-instance-123"
        self.mock_base_ctx.current_utc_datetime = datetime(2023, 1, 1, 12, 0, 0)
        self.mock_base_ctx.is_replaying = False
        self.mock_base_ctx.is_suspended = False

        # Mock methods
        self.mock_base_ctx.call_activity.return_value = Mock(spec=dt_task.Task)
        self.mock_base_ctx.call_sub_orchestrator.return_value = Mock(spec=dt_task.Task)
        self.mock_base_ctx.create_timer.return_value = Mock(spec=dt_task.Task)
        self.mock_base_ctx.wait_for_external_event.return_value = Mock(spec=dt_task.Task)
        self.mock_base_ctx.set_custom_status = Mock()
        self.mock_base_ctx.continue_as_new = Mock()

        self.ctx = AsyncWorkflowContext(self.mock_base_ctx)

    def test_context_creation(self):
        """Test creating AsyncWorkflowContext."""
        assert self.ctx._base_ctx is self.mock_base_ctx
        assert isinstance(self.ctx._operation_history, list)
        assert isinstance(self.ctx._cleanup_tasks, list)

    def test_instance_id_property(self):
        """Test instance_id property."""
        assert self.ctx.instance_id == "test-instance-123"

    def test_current_utc_datetime_property(self):
        """Test current_utc_datetime property."""
        assert self.ctx.current_utc_datetime == datetime(2023, 1, 1, 12, 0, 0)

    def test_is_replaying_property(self):
        """Test is_replaying property."""
        assert self.ctx.is_replaying == False

        self.mock_base_ctx.is_replaying = True
        assert self.ctx.is_replaying == True

    def test_is_suspended_property(self):
        """Test is_suspended property."""
        assert self.ctx.is_suspended == False

        self.mock_base_ctx.is_suspended = True
        assert self.ctx.is_suspended == True

    def test_now_method(self):
        """Test now() method from DeterministicContextMixin."""
        now = self.ctx.now()
        assert now == datetime(2023, 1, 1, 12, 0, 0)
        assert now is self.ctx.current_utc_datetime

    def test_random_method(self):
        """Test random() method from DeterministicContextMixin."""
        rng = self.ctx.random()
        assert isinstance(rng, random.Random)

        # Should be deterministic
        rng1 = self.ctx.random()
        rng2 = self.ctx.random()

        val1 = rng1.random()
        val2 = rng2.random()
        assert val1 == val2  # Same seed should produce same values

    def test_uuid4_method(self):
        """Test uuid4() method from DeterministicContextMixin."""
        test_uuid = self.ctx.uuid4()
        assert isinstance(test_uuid, uuid.UUID)
        assert test_uuid.version == 5  # Now using UUID v5 for .NET compatibility

        # Should increment counter - each call produces different UUID
        uuid1 = self.ctx.uuid4()
        uuid2 = self.ctx.uuid4()
        assert uuid1 != uuid2  # Counter increments

    def test_new_guid_method(self):
        """Test new_guid() alias method."""
        guid = self.ctx.new_guid()
        assert isinstance(guid, uuid.UUID)
        assert guid.version == 5  # Now using UUID v5 for .NET compatibility

    def test_random_string_method(self):
        """Test random_string() method from DeterministicContextMixin."""
        # Test default alphabet
        s1 = self.ctx.random_string(10)
        assert len(s1) == 10
        assert all(c.isalnum() for c in s1)

        # Test custom alphabet
        s2 = self.ctx.random_string(5, alphabet="ABC")
        assert len(s2) == 5
        assert all(c in "ABC" for c in s2)

        # Test deterministic behavior
        s3 = self.ctx.random_string(10)
        assert s1 == s3  # Same context should produce same string

    def test_call_activity_method(self):
        """Test call_activity() method."""
        activity_fn = Mock(__name__="test_activity")

        # Basic call
        awaitable = self.ctx.call_activity(activity_fn, input="test_input")

        assert isinstance(awaitable, ActivityAwaitable)
        assert awaitable._ctx is self.mock_base_ctx
        assert awaitable._activity_fn is activity_fn
        assert awaitable._input == "test_input"
        assert awaitable._retry_policy is None
        assert awaitable._metadata is None

    def test_call_activity_with_retry_policy(self):
        """Test call_activity() with retry policy."""
        activity_fn = Mock(__name__="test_activity")
        retry_policy = Mock()

        awaitable = self.ctx.call_activity(
            activity_fn, input="test_input", retry_policy=retry_policy
        )

        assert awaitable._retry_policy is retry_policy

    def test_call_activity_with_metadata(self):
        """Test call_activity() with metadata."""
        activity_fn = Mock(__name__="test_activity")
        metadata = {"key": "value"}

        awaitable = self.ctx.call_activity(activity_fn, input="test_input", metadata=metadata)

        assert awaitable._metadata == metadata

    def test_call_sub_orchestrator_method(self):
        """Test call_sub_orchestrator() method."""
        workflow_fn = Mock(__name__="test_workflow")

        awaitable = self.ctx.call_sub_orchestrator(
            workflow_fn, input="test_input", instance_id="sub-instance"
        )

        assert isinstance(awaitable, SubOrchestratorAwaitable)
        assert awaitable._ctx is self.mock_base_ctx
        assert awaitable._workflow_fn is workflow_fn
        assert awaitable._input == "test_input"
        assert awaitable._instance_id == "sub-instance"

    def test_create_timer_method(self):
        """Test create_timer() method."""
        # Test with timedelta
        duration = timedelta(seconds=30)
        awaitable = self.ctx.create_timer(duration)

        assert isinstance(awaitable, SleepAwaitable)
        assert awaitable._ctx is self.mock_base_ctx
        assert awaitable._duration is duration

    def test_sleep_method(self):
        """Test sleep() method."""
        # Test with float
        awaitable = self.ctx.create_timer(5.0)

        assert isinstance(awaitable, SleepAwaitable)
        assert awaitable._duration == 5.0

        # Test with timedelta
        duration = timedelta(minutes=1)
        awaitable = self.ctx.create_timer(duration)
        assert awaitable._duration is duration

        # Test with datetime
        deadline = datetime(2023, 1, 1, 13, 0, 0)
        awaitable = self.ctx.create_timer(deadline)
        assert awaitable._duration is deadline

    def test_wait_for_external_event_method(self):
        """Test wait_for_external_event() method."""
        awaitable = self.ctx.wait_for_external_event("test_event")

        assert isinstance(awaitable, ExternalEventAwaitable)
        assert awaitable._ctx is self.mock_base_ctx
        assert awaitable._name == "test_event"

    def test_when_all_method(self):
        """Test when_all() method."""
        # Create mock awaitables
        awaitable1 = Mock()
        awaitable2 = Mock()
        awaitables = [awaitable1, awaitable2]

        result = self.ctx.when_all(awaitables)

        assert isinstance(result, WhenAllAwaitable)
        assert result._tasks_like == awaitables

    def test_when_any_method(self):
        """Test when_any() method."""
        awaitable1 = Mock(spec=AwaitableBase)
        awaitable1._to_task.return_value = Mock(spec=dt_task.Task)
        awaitable2 = Mock(spec=AwaitableBase)
        awaitable2._to_task.return_value = Mock(spec=dt_task.Task)
        awaitables = [awaitable1, awaitable2]

        result = self.ctx.when_any(awaitables)

        assert isinstance(result, WhenAnyAwaitable)
        assert result._originals == awaitables

    def test_with_timeout_method(self):
        """Test with_timeout() method."""
        mock_awaitable = Mock()

        result = self.ctx.with_timeout(mock_awaitable, 5.0)

        assert isinstance(result, TimeoutAwaitable)
        assert result._awaitable is mock_awaitable
        assert result._timeout_seconds == 5.0
        assert result._ctx is self.mock_base_ctx

    def test_set_custom_status_method(self):
        """Test set_custom_status() method."""
        self.ctx.set_custom_status("Processing data")

        self.mock_base_ctx.set_custom_status.assert_called_once_with("Processing data")

    def test_set_custom_status_not_supported(self):
        """Test set_custom_status() when not supported by base context."""
        # Remove the method to simulate unsupported base context
        del self.mock_base_ctx.set_custom_status

        # Should not raise error
        self.ctx.set_custom_status("test")

    def test_continue_as_new_method(self):
        """Test continue_as_new() method."""
        new_input = {"restart": True}

        self.ctx.continue_as_new(new_input, save_events=True)

        self.mock_base_ctx.continue_as_new.assert_called_once_with(new_input, save_events=True)

    def test_debug_mode_enabled(self):
        """Test debug mode functionality."""
        import os
        from unittest.mock import patch

        # Test with DAPR_WF_DEBUG
        with patch.dict(os.environ, {"DAPR_WF_DEBUG": "true"}):
            debug_ctx = AsyncWorkflowContext(self.mock_base_ctx)
            assert debug_ctx._debug_mode == True

        # Test with DT_DEBUG
        with patch.dict(os.environ, {"DT_DEBUG": "true"}):
            debug_ctx = AsyncWorkflowContext(self.mock_base_ctx)
            assert debug_ctx._debug_mode == True

    def test_operation_logging_in_debug_mode(self):
        """Test that operations are logged in debug mode."""
        import os
        from unittest.mock import patch

        with patch.dict(os.environ, {"DAPR_WF_DEBUG": "true"}):
            debug_ctx = AsyncWorkflowContext(self.mock_base_ctx)

            # Perform some operations
            debug_ctx.call_activity("test_activity", input="test")
            debug_ctx.create_timer(5.0)
            debug_ctx.wait_for_external_event("test_event")

            # Should have logged operations
            assert len(debug_ctx._operation_history) == 3

            # Check operation details
            ops = debug_ctx._operation_history
            assert ops[0]["type"] == "activity"
            assert ops[1]["type"] == "sleep"
            assert ops[2]["type"] == "wait_for_external_event"

    def test_get_debug_info_method(self):
        """Test get_debug_info() method."""
        debug_info = self.ctx._get_info_snapshot()

        assert isinstance(debug_info, dict)
        assert debug_info["instance_id"] == "test-instance-123"
        assert debug_info["is_replaying"] == False
        assert "operation_history" in debug_info
        assert "cleanup_tasks_count" in debug_info

    def test_detection_disabled_property(self):
        """Test _detection_disabled property."""
        import os
        from unittest.mock import patch

        # Test with environment variable
        with patch.dict(os.environ, {"DAPR_WF_DISABLE_DETERMINISTIC_DETECTION": "true"}):
            disabled_ctx = AsyncWorkflowContext(self.mock_base_ctx)
            assert disabled_ctx._detection_disabled == True

        # Test without environment variable
        assert self.ctx._detection_disabled == False

    def test_workflow_name_tracking(self):
        """Test workflow name tracking."""
        # Should start as None
        assert self.ctx._workflow_name is None

        # Can be set
        self.ctx._workflow_name = "test_workflow"
        assert self.ctx._workflow_name == "test_workflow"

    def test_current_step_tracking(self):
        """Test current step tracking."""
        # Should start as None
        assert self.ctx._current_step is None

        # Can be set
        self.ctx._current_step = "step_1"
        assert self.ctx._current_step == "step_1"

    def test_context_slots(self):
        """Test that AsyncWorkflowContext uses __slots__."""
        assert hasattr(AsyncWorkflowContext, "__slots__")

    def test_deterministic_context_mixin_integration(self):
        """Test integration with DeterministicContextMixin."""
        from durabletask.deterministic import DeterministicContextMixin

        # Should be an instance of the mixin
        assert isinstance(self.ctx, DeterministicContextMixin)

        # Should have all mixin methods
        assert hasattr(self.ctx, "now")
        assert hasattr(self.ctx, "random")
        assert hasattr(self.ctx, "uuid4")
        assert hasattr(self.ctx, "new_guid")
        assert hasattr(self.ctx, "random_string")

    def test_context_with_string_activity_name(self):
        """Test context methods with string activity/workflow names."""
        # Test with string activity name
        awaitable = self.ctx.call_activity("string_activity_name", input="test")
        assert isinstance(awaitable, ActivityAwaitable)
        assert awaitable._activity_fn == "string_activity_name"

        # Test with string workflow name
        awaitable = self.ctx.call_sub_orchestrator("string_workflow_name", input="test")
        assert isinstance(awaitable, SubOrchestratorAwaitable)
        assert awaitable._workflow_fn == "string_workflow_name"

    def test_context_method_parameter_validation(self):
        """Test parameter validation in context methods."""
        # Test random_string with invalid parameters
        with pytest.raises(ValueError):
            self.ctx.random_string(-1)  # Negative length

        with pytest.raises(ValueError):
            self.ctx.random_string(5, alphabet="")  # Empty alphabet
