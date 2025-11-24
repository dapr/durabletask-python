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
Simplified tests for AsyncWorkflowContext in durabletask.aio.

These tests focus on the actual implementation rather than expected features.
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


class TestAsyncWorkflowContextBasic:
    """Test basic AsyncWorkflowContext functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        self.mock_base_ctx.instance_id = "test-instance-123"
        self.mock_base_ctx.current_utc_datetime = datetime(2023, 1, 1, 12, 0, 0)
        self.mock_base_ctx.is_replaying = False
        self.mock_base_ctx.is_suspended = False

        # Mock methods that might exist
        self.mock_base_ctx.call_activity.return_value = Mock(spec=dt_task.Task)
        self.mock_base_ctx.call_sub_orchestrator.return_value = Mock(spec=dt_task.Task)
        self.mock_base_ctx.create_timer.return_value = Mock(spec=dt_task.Task)
        self.mock_base_ctx.wait_for_external_event.return_value = Mock(spec=dt_task.Task)

        self.ctx = AsyncWorkflowContext(self.mock_base_ctx)

    def test_context_creation(self):
        """Test creating AsyncWorkflowContext."""
        assert self.ctx._base_ctx is self.mock_base_ctx

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

    def test_activity_method_alias(self):
        """Test activity() method alias."""
        activity_fn = Mock(__name__="test_activity")

        awaitable = self.ctx.call_activity(activity_fn, input="test_input")

        assert isinstance(awaitable, ActivityAwaitable)
        assert awaitable._activity_fn is activity_fn

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

    def test_sub_orchestrator_method_alias(self):
        """Test sub_orchestrator() method alias."""
        workflow_fn = Mock(__name__="test_workflow")

        awaitable = self.ctx.sub_orchestrator(workflow_fn, input="test_input")

        assert isinstance(awaitable, SubOrchestratorAwaitable)
        assert awaitable._workflow_fn is workflow_fn

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

    def test_create_timer_method(self):
        """Test create_timer() method."""
        # Test with timedelta
        duration = timedelta(seconds=30)
        awaitable = self.ctx.create_timer(duration)

        assert isinstance(awaitable, SleepAwaitable)
        assert awaitable._ctx is self.mock_base_ctx
        assert awaitable._duration is duration

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

    def test_set_custom_status_method(self):
        """Test set_custom_status() method."""
        # Should not raise error even if base context doesn't support it
        self.ctx.set_custom_status("Processing data")

    def test_continue_as_new_method(self):
        """Test continue_as_new() method."""
        new_input = {"restart": True}

        # Should not raise error even if base context doesn't support it
        self.ctx.continue_as_new(new_input)

    def test_get_debug_info_method(self):
        """Test get_debug_info() method."""
        debug_info = self.ctx._get_info_snapshot()

        assert isinstance(debug_info, dict)
        assert debug_info["instance_id"] == "test-instance-123"
        assert debug_info["is_replaying"] == False

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

    def test_context_repr(self):
        """Test context string representation."""
        repr_str = repr(self.ctx)
        assert "AsyncWorkflowContext" in repr_str
        assert "test-instance-123" in repr_str
