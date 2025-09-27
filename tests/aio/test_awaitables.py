# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for awaitable classes in durabletask.aio.
"""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytest

from durabletask import task as dt_task
from durabletask.aio import (
    ActivityAwaitable,
    AwaitableBase,
    ExternalEventAwaitable,
    SleepAwaitable,
    SubOrchestratorAwaitable,
    SwallowExceptionAwaitable,
    TimeoutAwaitable,
    WhenAllAwaitable,
    WhenAnyAwaitable,
    WhenAnyResultAwaitable,
    WorkflowTimeoutError,
)


class TestAwaitableBase:
    """Test AwaitableBase functionality."""

    def test_awaitable_base_abstract(self):
        """Test that AwaitableBase cannot be instantiated directly."""
        # AwaitableBase is not technically abstract but should not be used directly
        # It will raise NotImplementedError when _to_task is called
        awaitable = AwaitableBase()
        with pytest.raises(NotImplementedError):
            awaitable._to_task()

    def test_awaitable_base_slots(self):
        """Test that AwaitableBase has __slots__."""
        assert hasattr(AwaitableBase, "__slots__")


class TestActivityAwaitable:
    """Test ActivityAwaitable functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_ctx = Mock()
        self.mock_ctx.call_activity.return_value = dt_task.CompletableTask()
        self.activity_fn = Mock(__name__="test_activity")

    def test_activity_awaitable_creation(self):
        """Test creating an ActivityAwaitable."""
        awaitable = ActivityAwaitable(
            self.mock_ctx,
            self.activity_fn,
            input="test_input",
            retry_policy=None,
            metadata={"key": "value"}
        )
        
        assert awaitable._ctx is self.mock_ctx
        assert awaitable._activity_fn is self.activity_fn
        assert awaitable._input == "test_input"
        assert awaitable._retry_policy is None
        assert awaitable._metadata == {"key": "value"}

    def test_activity_awaitable_to_task(self):
        """Test converting ActivityAwaitable to task."""
        awaitable = ActivityAwaitable(
            self.mock_ctx,
            self.activity_fn,
            input="test_input"
        )
        
        task = awaitable._to_task()
        
        self.mock_ctx.call_activity.assert_called_once_with(
            self.activity_fn,
            input="test_input"
        )
        assert isinstance(task, dt_task.Task)

    def test_activity_awaitable_with_retry_policy(self):
        """Test ActivityAwaitable with retry policy."""
        retry_policy = Mock()
        awaitable = ActivityAwaitable(
            self.mock_ctx,
            self.activity_fn,
            input="test_input",
            retry_policy=retry_policy
        )
        
        awaitable._to_task()
        
        self.mock_ctx.call_activity.assert_called_once_with(
            self.activity_fn,
            input="test_input",
            retry_policy=retry_policy
        )

    def test_activity_awaitable_slots(self):
        """Test that ActivityAwaitable has __slots__."""
        assert hasattr(ActivityAwaitable, "__slots__")


class TestSubOrchestratorAwaitable:
    """Test SubOrchestratorAwaitable functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_ctx = Mock()
        self.mock_ctx.call_sub_orchestrator.return_value = dt_task.CompletableTask()
        self.workflow_fn = Mock(__name__="test_workflow")

    def test_sub_orchestrator_awaitable_creation(self):
        """Test creating a SubOrchestratorAwaitable."""
        awaitable = SubOrchestratorAwaitable(
            self.mock_ctx,
            self.workflow_fn,
            input="test_input",
            instance_id="test-instance",
            retry_policy=None,
            metadata={"key": "value"}
        )
        
        assert awaitable._ctx is self.mock_ctx
        assert awaitable._workflow_fn is self.workflow_fn
        assert awaitable._input == "test_input"
        assert awaitable._instance_id == "test-instance"
        assert awaitable._retry_policy is None
        assert awaitable._metadata == {"key": "value"}

    def test_sub_orchestrator_awaitable_to_task(self):
        """Test converting SubOrchestratorAwaitable to task."""
        awaitable = SubOrchestratorAwaitable(
            self.mock_ctx,
            self.workflow_fn,
            input="test_input",
            instance_id="test-instance"
        )
        
        task = awaitable._to_task()
        
        self.mock_ctx.call_sub_orchestrator.assert_called_once_with(
            self.workflow_fn,
            input="test_input",
            instance_id="test-instance"
        )
        assert isinstance(task, dt_task.Task)

    def test_sub_orchestrator_awaitable_slots(self):
        """Test that SubOrchestratorAwaitable has __slots__."""
        assert hasattr(SubOrchestratorAwaitable, "__slots__")


class TestSleepAwaitable:
    """Test SleepAwaitable functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_ctx = Mock()
        self.mock_ctx.create_timer.return_value = dt_task.CompletableTask()

    def test_sleep_awaitable_creation(self):
        """Test creating a SleepAwaitable."""
        duration = timedelta(seconds=5)
        awaitable = SleepAwaitable(self.mock_ctx, duration)
        
        assert awaitable._ctx is self.mock_ctx
        assert awaitable._duration is duration

    def test_sleep_awaitable_to_task(self):
        """Test converting SleepAwaitable to task."""
        duration = timedelta(seconds=5)
        awaitable = SleepAwaitable(self.mock_ctx, duration)
        
        task = awaitable._to_task()
        
        self.mock_ctx.create_timer.assert_called_once_with(duration)
        assert isinstance(task, dt_task.Task)

    def test_sleep_awaitable_with_float(self):
        """Test SleepAwaitable with float duration."""
        awaitable = SleepAwaitable(self.mock_ctx, 5.0)
        awaitable._to_task()
        
        self.mock_ctx.create_timer.assert_called_once_with(timedelta(seconds=5.0))

    def test_sleep_awaitable_with_datetime(self):
        """Test SleepAwaitable with datetime."""
        deadline = datetime(2023, 1, 1, 12, 0, 0)
        awaitable = SleepAwaitable(self.mock_ctx, deadline)
        awaitable._to_task()
        
        self.mock_ctx.create_timer.assert_called_once_with(deadline)

    def test_sleep_awaitable_slots(self):
        """Test that SleepAwaitable has __slots__."""
        assert hasattr(SleepAwaitable, "__slots__")


class TestExternalEventAwaitable:
    """Test ExternalEventAwaitable functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_ctx = Mock()
        self.mock_ctx.wait_for_external_event.return_value = dt_task.CompletableTask()

    def test_external_event_awaitable_creation(self):
        """Test creating an ExternalEventAwaitable."""
        awaitable = ExternalEventAwaitable(self.mock_ctx, "test_event")
        
        assert awaitable._ctx is self.mock_ctx
        assert awaitable._name == "test_event"

    def test_external_event_awaitable_to_task(self):
        """Test converting ExternalEventAwaitable to task."""
        awaitable = ExternalEventAwaitable(self.mock_ctx, "test_event")
        
        task = awaitable._to_task()
        
        self.mock_ctx.wait_for_external_event.assert_called_once_with("test_event")
        assert isinstance(task, dt_task.Task)

    def test_external_event_awaitable_slots(self):
        """Test that ExternalEventAwaitable has __slots__."""
        assert hasattr(ExternalEventAwaitable, "__slots__")


class TestWhenAllAwaitable:
    """Test WhenAllAwaitable functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_task1 = Mock(spec=dt_task.Task)
        self.mock_task2 = Mock(spec=dt_task.Task)
        self.mock_awaitable1 = Mock(spec=AwaitableBase)
        self.mock_awaitable1._to_task.return_value = self.mock_task1
        self.mock_awaitable2 = Mock(spec=AwaitableBase)
        self.mock_awaitable2._to_task.return_value = self.mock_task2

    def test_when_all_awaitable_creation(self):
        """Test creating a WhenAllAwaitable."""
        awaitables = [self.mock_awaitable1, self.mock_awaitable2]
        awaitable = WhenAllAwaitable(awaitables)
        
        assert awaitable._tasks_like == awaitables

    def test_when_all_awaitable_to_task(self):
        """Test converting WhenAllAwaitable to task."""
        awaitables = [self.mock_awaitable1, self.mock_awaitable2]
        awaitable = WhenAllAwaitable(awaitables)
        
        with patch('durabletask.task.when_all') as mock_when_all:
            mock_when_all.return_value = Mock(spec=dt_task.Task)
            task = awaitable._to_task()
            
            mock_when_all.assert_called_once_with([self.mock_task1, self.mock_task2])
            assert isinstance(task, dt_task.Task)

    def test_when_all_awaitable_with_tasks(self):
        """Test WhenAllAwaitable with direct tasks."""
        tasks = [self.mock_task1, self.mock_task2]
        awaitable = WhenAllAwaitable(tasks)
        
        with patch('durabletask.task.when_all') as mock_when_all:
            mock_when_all.return_value = Mock(spec=dt_task.Task)
            awaitable._to_task()
            
            mock_when_all.assert_called_once_with([self.mock_task1, self.mock_task2])

    def test_when_all_awaitable_slots(self):
        """Test that WhenAllAwaitable has __slots__."""
        assert hasattr(WhenAllAwaitable, "__slots__")


class TestWhenAnyAwaitable:
    """Test WhenAnyAwaitable functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_task1 = Mock(spec=dt_task.Task)
        self.mock_task2 = Mock(spec=dt_task.Task)
        self.mock_awaitable1 = Mock(spec=AwaitableBase)
        self.mock_awaitable1._to_task.return_value = self.mock_task1
        self.mock_awaitable2 = Mock(spec=AwaitableBase)
        self.mock_awaitable2._to_task.return_value = self.mock_task2

    def test_when_any_awaitable_creation(self):
        """Test creating a WhenAnyAwaitable."""
        awaitables = [self.mock_awaitable1, self.mock_awaitable2]
        awaitable = WhenAnyAwaitable(awaitables)
        
        assert awaitable._tasks_like == awaitables

    def test_when_any_awaitable_to_task(self):
        """Test converting WhenAnyAwaitable to task."""
        awaitables = [self.mock_awaitable1, self.mock_awaitable2]
        awaitable = WhenAnyAwaitable(awaitables)
        
        with patch('durabletask.task.when_any') as mock_when_any:
            mock_when_any.return_value = Mock(spec=dt_task.Task)
            task = awaitable._to_task()
            
            mock_when_any.assert_called_once_with([self.mock_task1, self.mock_task2])
            assert isinstance(task, dt_task.Task)

    def test_when_any_awaitable_slots(self):
        """Test that WhenAnyAwaitable has __slots__."""
        assert hasattr(WhenAnyAwaitable, "__slots__")


class TestSwallowExceptionAwaitable:
    """Test SwallowExceptionAwaitable functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_awaitable = Mock(spec=AwaitableBase)
        self.mock_task = Mock(spec=dt_task.Task)
        self.mock_awaitable._to_task.return_value = self.mock_task

    def test_swallow_exception_awaitable_creation(self):
        """Test creating a SwallowExceptionAwaitable."""
        awaitable = SwallowExceptionAwaitable(self.mock_awaitable)
        
        assert awaitable._awaitable is self.mock_awaitable

    def test_swallow_exception_awaitable_to_task(self):
        """Test converting SwallowExceptionAwaitable to task."""
        awaitable = SwallowExceptionAwaitable(self.mock_awaitable)
        
        task = awaitable._to_task()
        
        self.mock_awaitable._to_task.assert_called_once()
        assert task is self.mock_task

    def test_swallow_exception_awaitable_slots(self):
        """Test that SwallowExceptionAwaitable has __slots__."""
        assert hasattr(SwallowExceptionAwaitable, "__slots__")


class TestWhenAnyResultAwaitable:
    """Test WhenAnyResultAwaitable functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_task1 = Mock(spec=dt_task.Task)
        self.mock_task2 = Mock(spec=dt_task.Task)
        self.mock_awaitable1 = Mock(spec=AwaitableBase)
        self.mock_awaitable1._to_task.return_value = self.mock_task1
        self.mock_awaitable2 = Mock(spec=AwaitableBase)
        self.mock_awaitable2._to_task.return_value = self.mock_task2

    def test_when_any_result_awaitable_creation(self):
        """Test creating a WhenAnyResultAwaitable."""
        awaitables = [self.mock_awaitable1, self.mock_awaitable2]
        awaitable = WhenAnyResultAwaitable(awaitables)
        
        assert awaitable._tasks_like == awaitables

    def test_when_any_result_awaitable_to_task(self):
        """Test converting WhenAnyResultAwaitable to task."""
        awaitables = [self.mock_awaitable1, self.mock_awaitable2]
        awaitable = WhenAnyResultAwaitable(awaitables)
        
        with patch('durabletask.task.when_any') as mock_when_any:
            mock_when_any.return_value = Mock(spec=dt_task.Task)
            task = awaitable._to_task()
            
            mock_when_any.assert_called_once_with([self.mock_task1, self.mock_task2])
            assert isinstance(task, dt_task.Task)

    def test_when_any_result_awaitable_slots(self):
        """Test that WhenAnyResultAwaitable has __slots__."""
        assert hasattr(WhenAnyResultAwaitable, "__slots__")


class TestTimeoutAwaitable:
    """Test TimeoutAwaitable functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_ctx = Mock()
        self.mock_ctx.create_timer.return_value = Mock(spec=dt_task.Task)
        self.mock_awaitable = Mock(spec=AwaitableBase)
        self.mock_task = Mock(spec=dt_task.Task)
        self.mock_awaitable._to_task.return_value = self.mock_task

    def test_timeout_awaitable_creation(self):
        """Test creating a TimeoutAwaitable."""
        awaitable = TimeoutAwaitable(self.mock_awaitable, 5.0, self.mock_ctx)
        
        assert awaitable._ctx is self.mock_ctx
        assert awaitable._awaitable is self.mock_awaitable
        assert awaitable._timeout_seconds == 5.0

    def test_timeout_awaitable_to_task(self):
        """Test converting TimeoutAwaitable to task."""
        awaitable = TimeoutAwaitable(self.mock_awaitable, 5.0, self.mock_ctx)
        
        with patch('durabletask.task.when_any') as mock_when_any:
            mock_when_any.return_value = Mock(spec=dt_task.Task)
            task = awaitable._to_task()
            
            # Should create timer and call when_any
            self.mock_ctx.create_timer.assert_called_once()
            self.mock_awaitable._to_task.assert_called_once()
            mock_when_any.assert_called_once()
            assert isinstance(task, dt_task.Task)

    def test_timeout_awaitable_slots(self):
        """Test that TimeoutAwaitable has __slots__."""
        assert hasattr(TimeoutAwaitable, "__slots__")


class TestAwaitableSlots:
    """Test that all awaitable classes use __slots__ for performance."""

    def test_all_awaitables_have_slots(self):
        """Test that all awaitable classes have __slots__."""
        awaitable_classes = [
            AwaitableBase,
            ActivityAwaitable,
            SubOrchestratorAwaitable,
            SleepAwaitable,
            ExternalEventAwaitable,
            WhenAllAwaitable,
            WhenAnyAwaitable,
            SwallowExceptionAwaitable,
            WhenAnyResultAwaitable,
            TimeoutAwaitable,
        ]
        
        for cls in awaitable_classes:
            assert hasattr(cls, "__slots__"), f"{cls.__name__} should have __slots__"
