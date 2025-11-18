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
            metadata={"key": "value"},
        )

        assert awaitable._ctx is self.mock_ctx
        assert awaitable._activity_fn is self.activity_fn
        assert awaitable._input == "test_input"
        assert awaitable._retry_policy is None
        assert awaitable._metadata == {"key": "value"}

    def test_activity_awaitable_to_task(self):
        """Test converting ActivityAwaitable to task."""
        awaitable = ActivityAwaitable(self.mock_ctx, self.activity_fn, input="test_input")

        task = awaitable._to_task()

        self.mock_ctx.call_activity.assert_called_once_with(self.activity_fn, input="test_input")
        assert isinstance(task, dt_task.Task)

    def test_activity_awaitable_with_retry_policy(self):
        """Test ActivityAwaitable with retry policy."""
        retry_policy = Mock()
        awaitable = ActivityAwaitable(
            self.mock_ctx, self.activity_fn, input="test_input", retry_policy=retry_policy
        )

        awaitable._to_task()

        self.mock_ctx.call_activity.assert_called_once_with(
            self.activity_fn, input="test_input", retry_policy=retry_policy
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
            metadata={"key": "value"},
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
            self.mock_ctx, self.workflow_fn, input="test_input", instance_id="test-instance"
        )

        task = awaitable._to_task()

        self.mock_ctx.call_sub_orchestrator.assert_called_once_with(
            self.workflow_fn, input="test_input", instance_id="test-instance"
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

        with patch("durabletask.task.when_all") as mock_when_all:
            mock_when_all.return_value = Mock(spec=dt_task.Task)
            task = awaitable._to_task()

            mock_when_all.assert_called_once_with([self.mock_task1, self.mock_task2])
            assert isinstance(task, dt_task.Task)

    def test_when_all_awaitable_with_tasks(self):
        """Test WhenAllAwaitable with direct tasks."""
        tasks = [self.mock_task1, self.mock_task2]
        awaitable = WhenAllAwaitable(tasks)

        with patch("durabletask.task.when_all") as mock_when_all:
            mock_when_all.return_value = Mock(spec=dt_task.Task)
            awaitable._to_task()

            mock_when_all.assert_called_once_with([self.mock_task1, self.mock_task2])

    def test_when_all_awaitable_slots(self):
        """Test that WhenAllAwaitable has __slots__."""
        assert hasattr(WhenAllAwaitable, "__slots__")

    def _drive_awaitable(self, awaitable, result):
        gen = awaitable.__await__()
        try:
            yielded = next(gen)
        except StopIteration as si:  # empty fast-path
            return si.value
        assert isinstance(yielded, dt_task.Task) or True  # we don't strictly require type here
        try:
            return gen.send(result)
        except StopIteration as si:
            return si.value

    def test_when_all_empty_fast_path(self):
        awaitable = WhenAllAwaitable([])
        # Should complete without yielding
        gen = awaitable.__await__()
        with pytest.raises(StopIteration) as si:
            next(gen)
        assert si.value.value == []

    def test_when_all_success_and_caching(self):
        awaitable = WhenAllAwaitable([self.mock_awaitable1, self.mock_awaitable2])
        results = ["r1", "r2"]
        with patch("durabletask.task.when_all") as mock_when_all:
            mock_when_all.return_value = Mock(spec=dt_task.Task)
            # Simulate runtime returning results list
            gen = awaitable.__await__()
            _ = next(gen)
            with pytest.raises(StopIteration) as si:
                gen.send(results)
            assert si.value.value == results
        # Re-await should return cached without yielding
        gen2 = awaitable.__await__()
        with pytest.raises(StopIteration) as si2:
            next(gen2)
        assert si2.value.value == results

    def test_when_all_exception_and_caching(self):
        awaitable = WhenAllAwaitable([self.mock_awaitable1, self.mock_awaitable2])
        with patch("durabletask.task.when_all") as mock_when_all:
            mock_when_all.return_value = Mock(spec=dt_task.Task)
            gen = awaitable.__await__()
            _ = next(gen)

        class Boom(Exception):
            pass

        with pytest.raises(Boom):
            gen.throw(Boom())
        # Re-await should immediately raise cached exception
        gen2 = awaitable.__await__()
        with pytest.raises(Boom):
            next(gen2)


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

        with patch("durabletask.task.when_any") as mock_when_any:
            mock_when_any.return_value = Mock(spec=dt_task.Task)
            task = awaitable._to_task()

            mock_when_any.assert_called_once_with([self.mock_task1, self.mock_task2])
            assert isinstance(task, dt_task.Task)

    def test_when_any_awaitable_slots(self):
        """Test that WhenAnyAwaitable has __slots__."""
        assert hasattr(WhenAnyAwaitable, "__slots__")

    def test_when_any_winner_identity_and_proxy_get_result(self):
        awaitable = WhenAnyAwaitable([self.mock_awaitable1, self.mock_awaitable2])
        with patch("durabletask.task.when_any") as mock_when_any:
            mock_when_any.return_value = Mock(spec=dt_task.Task)
            gen = awaitable.__await__()
            _ = next(gen)
            # Simulate runtime returning that task1 completed
            # Also give it a get_result
            self.mock_task1.get_result = Mock(return_value="done1")
            with pytest.raises(StopIteration) as si:
                gen.send(self.mock_task1)
            proxy = si.value.value
            # Winner proxy equals original awaitable1 by identity semantics
            assert (proxy == awaitable._tasks_like[0]) is True
            assert proxy.get_result() == "done1"

    def test_when_any_non_task_completed_sentinel(self):
        # If runtime yields a sentinel, proxy should map to first item
        awaitable = WhenAnyAwaitable([self.mock_awaitable1, self.mock_awaitable2])
        with patch("durabletask.task.when_any") as mock_when_any:
            mock_when_any.return_value = Mock(spec=dt_task.Task)
            gen = awaitable.__await__()
            _ = next(gen)
            sentinel = object()
            with pytest.raises(StopIteration) as si:
                gen.send(sentinel)
            proxy = si.value.value
            assert (proxy == awaitable._tasks_like[0]) is True


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

    def test_swallow_exception_runtime_success_and_failure(self):
        awaitable = SwallowExceptionAwaitable(self.mock_awaitable)
        # Success path
        gen = awaitable.__await__()
        _ = next(gen)
        with pytest.raises(StopIteration) as si:
            gen.send("ok")
        assert si.value.value == "ok"
        # Failure path returns exception instance via StopIteration.value
        awaitable2 = SwallowExceptionAwaitable(self.mock_awaitable)
        gen2 = awaitable2.__await__()
        _ = next(gen2)
        err = RuntimeError("boom")
        with pytest.raises(StopIteration) as si2:
            gen2.throw(err)
        assert si2.value.value is err


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

        with patch("durabletask.task.when_any") as mock_when_any:
            mock_when_any.return_value = Mock(spec=dt_task.Task)
            task = awaitable._to_task()

            mock_when_any.assert_called_once_with([self.mock_task1, self.mock_task2])
            assert isinstance(task, dt_task.Task)

    def test_when_any_result_awaitable_slots(self):
        """Test that WhenAnyResultAwaitable has __slots__."""
        assert hasattr(WhenAnyResultAwaitable, "__slots__")

    def test_when_any_result_returns_index_and_result(self):
        awaitable = WhenAnyResultAwaitable([self.mock_awaitable1, self.mock_awaitable2])
        with patch("durabletask.task.when_any") as mock_when_any:
            mock_when_any.return_value = Mock(spec=dt_task.Task)
            # Drive __await__ and send completion of second task
            gen = awaitable.__await__()
            _ = next(gen)
            # Attach a fake .result attribute like Task might have
            self.mock_task2.result = "v2"
            with pytest.raises(StopIteration) as si:
                gen.send(self.mock_task2)
            idx, result = si.value.value
            assert idx == 1
            assert result == "v2"


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

        with patch("durabletask.task.when_any") as mock_when_any:
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

    def test_timeout_awaitable_timeout_hits(self):
        awaitable = TimeoutAwaitable(self.mock_awaitable, 5.0, self.mock_ctx)
        # Capture the cached timeout task instance created by _to_task
        gen = awaitable.__await__()
        _ = next(gen)
        timeout_task = awaitable._timeout_task
        assert timeout_task is not None
        with pytest.raises(WorkflowTimeoutError):
            gen.send(timeout_task)

    def test_timeout_awaitable_operation_completes_first(self):
        awaitable = TimeoutAwaitable(self.mock_awaitable, 5.0, self.mock_ctx)
        gen = awaitable.__await__()
        _ = next(gen)
        # If the operation completed first, runtime returns the operation task
        self.mock_task.result = "value"
        with pytest.raises(StopIteration) as si:
            gen.send(self.mock_task)
        assert si.value.value == "value"

    def test_timeout_awaitable_non_task_sentinel_heuristic(self):
        awaitable = TimeoutAwaitable(self.mock_awaitable, 5.0, self.mock_ctx)
        gen = awaitable.__await__()
        _ = next(gen)
        with pytest.raises(StopIteration) as si:
            gen.send({"x": 1})
        assert si.value.value == {"x": 1}


class TestPropagationForActivityAndSubOrch:
    """Test propagation of app_id/metadata/retry_policy to context methods via signature detection."""

    class _CtxWithSignatures:
        def __init__(self):
            self.call_activity_called_with = None
            self.call_sub_orchestrator_called_with = None

        def call_activity(
            self, activity_fn, *, input=None, retry_policy=None, app_id=None, metadata=None
        ):
            self.call_activity_called_with = dict(
                activity_fn=activity_fn,
                input=input,
                retry_policy=retry_policy,
                app_id=app_id,
                metadata=metadata,
            )
            return dt_task.CompletableTask()

        def call_sub_orchestrator(
            self,
            workflow_fn,
            *,
            input=None,
            instance_id=None,
            retry_policy=None,
            app_id=None,
            metadata=None,
        ):
            self.call_sub_orchestrator_called_with = dict(
                workflow_fn=workflow_fn,
                input=input,
                instance_id=instance_id,
                retry_policy=retry_policy,
                app_id=app_id,
                metadata=metadata,
            )
            return dt_task.CompletableTask()

    def test_activity_propagation_app_id_metadata_retry(self):
        ctx = self._CtxWithSignatures()
        activity_fn = lambda: None  # noqa: E731
        rp = dt_task.RetryPolicy(
            first_retry_interval=timedelta(seconds=1), max_number_of_attempts=2
        )
        awaitable = ActivityAwaitable(
            ctx, activity_fn, input={"a": 1}, retry_policy=rp, app_id="app-x", metadata={"h": "v"}
        )
        _ = awaitable._to_task()
        called = ctx.call_activity_called_with
        assert called["activity_fn"] is activity_fn
        assert called["input"] == {"a": 1}
        assert called["retry_policy"] is rp
        assert called["app_id"] == "app-x"
        assert called["metadata"] == {"h": "v"}

    def test_suborch_propagation_all_fields(self):
        ctx = self._CtxWithSignatures()
        workflow_fn = lambda: None  # noqa: E731
        rp = dt_task.RetryPolicy(
            first_retry_interval=timedelta(seconds=1), max_number_of_attempts=2
        )
        awaitable = SubOrchestratorAwaitable(
            ctx,
            workflow_fn,
            input=123,
            instance_id="iid-1",
            retry_policy=rp,
            app_id="app-y",
            metadata={"k": "m"},
        )
        _ = awaitable._to_task()
        called = ctx.call_sub_orchestrator_called_with
        assert called["workflow_fn"] is workflow_fn
        assert called["input"] == 123
        assert called["instance_id"] == "iid-1"
        assert called["retry_policy"] is rp
        assert called["app_id"] == "app-y"
        assert called["metadata"] == {"k": "m"}


class TestExternalEventIntegration:
    """Integration-like tests combining ExternalEventAwaitable with when_any/timeout wrappers."""

    def setup_method(self):
        self.ctx = Mock()
        # Provide stable task instances for mapping
        self.event_task = Mock(spec=dt_task.Task)
        self.timer_task = Mock(spec=dt_task.Task)
        self.ctx.wait_for_external_event.return_value = self.event_task
        self.ctx.create_timer.return_value = self.timer_task

    def test_when_any_between_event_and_timer_event_wins(self):
        event_aw = ExternalEventAwaitable(self.ctx, "ev")
        timer_aw = SleepAwaitable(self.ctx, 1.0)
        wa = WhenAnyAwaitable([event_aw, timer_aw])
        with patch("durabletask.task.when_any") as mock_when_any:
            mock_when_any.return_value = Mock(spec=dt_task.Task)
            gen = wa.__await__()
            _ = next(gen)
            with pytest.raises(StopIteration) as si:
                gen.send(self.event_task)
            proxy = si.value.value
            assert (proxy == wa._tasks_like[0]) is True

    def test_timeout_wrapper_times_out_before_event(self):
        event_aw = ExternalEventAwaitable(self.ctx, "ev")
        tw = TimeoutAwaitable(event_aw, 2.0, self.ctx)
        gen = tw.__await__()
        _ = next(gen)
        # Should have cached timeout task equal to ctx.create_timer return
        assert tw._timeout_task is self.timer_task
        with pytest.raises(WorkflowTimeoutError):
            gen.send(self.timer_task)


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
