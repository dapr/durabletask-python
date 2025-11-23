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
Awaitable classes for async workflows.

This module provides awaitable wrappers for DurableTask operations that can be
used in async workflows. Each awaitable yields a durabletask.task.Task which
the driver yields to the runtime and feeds the result back to the coroutine.
"""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    TypeVar,
    Union,
    cast,
)

from durabletask import task

# Forward reference for the operation wrapper - imported at runtime to avoid circular imports

TOutput = TypeVar("TOutput")


class AwaitableBase(Awaitable[TOutput]):
    """
    Base class for all workflow awaitables.

    Provides the common interface for converting workflow operations
    into DurableTask tasks that can be yielded to the runtime.
    """

    __slots__ = ()

    def _to_task(self) -> task.Task[Any]:
        """
        Convert this awaitable to a DurableTask task.

        Subclasses must implement this method to define how they
        translate to the underlying task system.

        Returns:
            A DurableTask task representing this operation
        """
        raise NotImplementedError("Subclasses must implement _to_task")

    def __await__(self) -> Generator[Any, Any, TOutput]:
        """
        Make this object awaitable by yielding the underlying task.

        This is called when the awaitable is used with 'await' in an
        async workflow function.
        """
        # Yield the task directly - the worker expects durabletask.task.Task objects
        t = self._to_task()
        result = yield t
        return cast(TOutput, result)


class ActivityAwaitable(AwaitableBase[TOutput]):
    """Awaitable for activity function calls."""

    __slots__ = ("_ctx", "_activity_fn", "_input", "_retry_policy", "_app_id", "_metadata")

    def __init__(
        self,
        ctx: Any,
        activity_fn: Union[Callable[..., Any], str],
        *,
        input: Any = None,
        retry_policy: Any = None,
        app_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize an activity awaitable.

        Args:
            ctx: The workflow context
            activity_fn: The activity function to call
            input: Input data for the activity
            retry_policy: Optional retry policy
            app_id: Optional target app ID for routing
            metadata: Optional metadata for the activity call
        """
        super().__init__()
        self._ctx = ctx
        self._activity_fn = activity_fn
        self._input = input
        self._retry_policy = retry_policy
        self._app_id = app_id
        self._metadata = metadata

    def _to_task(self) -> task.Task[Any]:
        """Convert to a call_activity task."""
        # Check if the context supports metadata parameter
        import inspect

        sig = inspect.signature(self._ctx.call_activity)
        supports_metadata = "metadata" in sig.parameters
        supports_app_id = "app_id" in sig.parameters

        if self._retry_policy is None:
            if (supports_metadata and self._metadata is not None) or (
                supports_app_id and self._app_id is not None
            ):
                kwargs: Dict[str, Any] = {"input": self._input}
                if supports_metadata and self._metadata is not None:
                    kwargs["metadata"] = self._metadata
                if supports_app_id and self._app_id is not None:
                    kwargs["app_id"] = self._app_id
                return cast(task.Task[Any], self._ctx.call_activity(self._activity_fn, **kwargs))
            else:
                return cast(
                    task.Task[Any], self._ctx.call_activity(self._activity_fn, input=self._input)
                )
        else:
            if (supports_metadata and self._metadata is not None) or (
                supports_app_id and self._app_id is not None
            ):
                kwargs2: Dict[str, Any] = {"input": self._input, "retry_policy": self._retry_policy}
                if supports_metadata and self._metadata is not None:
                    kwargs2["metadata"] = self._metadata
                if supports_app_id and self._app_id is not None:
                    kwargs2["app_id"] = self._app_id
                return cast(
                    task.Task[Any],
                    self._ctx.call_activity(
                        self._activity_fn,
                        **kwargs2,
                    ),
                )
            else:
                return cast(
                    task.Task[Any],
                    self._ctx.call_activity(
                        self._activity_fn,
                        input=self._input,
                        retry_policy=self._retry_policy,
                    ),
                )


class SubOrchestratorAwaitable(AwaitableBase[TOutput]):
    """Awaitable for sub-orchestrator calls."""

    __slots__ = (
        "_ctx",
        "_workflow_fn",
        "_input",
        "_instance_id",
        "_retry_policy",
        "_app_id",
        "_metadata",
    )

    def __init__(
        self,
        ctx: Any,
        workflow_fn: Union[Callable[..., Any], str],
        *,
        input: Any = None,
        instance_id: Optional[str] = None,
        retry_policy: Any = None,
        app_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize a sub-orchestrator awaitable.

        Args:
            ctx: The workflow context
            workflow_fn: The sub-orchestrator function to call
            input: Input data for the sub-orchestrator
            instance_id: Optional instance ID for the sub-orchestrator
            retry_policy: Optional retry policy
            app_id: Optional target app ID for routing
            metadata: Optional metadata for the sub-orchestrator call
        """
        super().__init__()
        self._ctx = ctx
        self._workflow_fn = workflow_fn
        self._input = input
        self._instance_id = instance_id
        self._retry_policy = retry_policy
        self._app_id = app_id
        self._metadata = metadata

    def _to_task(self) -> task.Task[Any]:
        """Convert to a call_sub_orchestrator task."""
        # The underlying context uses call_sub_orchestrator (durabletask naming)
        # Check if the context supports metadata parameter
        import inspect

        sig = inspect.signature(self._ctx.call_sub_orchestrator)
        supports_metadata = "metadata" in sig.parameters
        supports_app_id = "app_id" in sig.parameters

        if self._retry_policy is None:
            if (supports_metadata and self._metadata is not None) or (
                supports_app_id and self._app_id is not None
            ):
                kwargs: Dict[str, Any] = {"input": self._input, "instance_id": self._instance_id}
                if supports_metadata and self._metadata is not None:
                    kwargs["metadata"] = self._metadata
                if supports_app_id and self._app_id is not None:
                    kwargs["app_id"] = self._app_id
                return cast(
                    task.Task[Any],
                    self._ctx.call_sub_orchestrator(
                        self._workflow_fn,
                        **kwargs,
                    ),
                )
            else:
                return cast(
                    task.Task[Any],
                    self._ctx.call_sub_orchestrator(
                        self._workflow_fn,
                        input=self._input,
                        instance_id=self._instance_id,
                    ),
                )
        else:
            if (supports_metadata and self._metadata is not None) or (
                supports_app_id and self._app_id is not None
            ):
                kwargs2: Dict[str, Any] = {
                    "input": self._input,
                    "instance_id": self._instance_id,
                    "retry_policy": self._retry_policy,
                }
                if supports_metadata and self._metadata is not None:
                    kwargs2["metadata"] = self._metadata
                if supports_app_id and self._app_id is not None:
                    kwargs2["app_id"] = self._app_id
                return cast(
                    task.Task[Any],
                    self._ctx.call_sub_orchestrator(
                        self._workflow_fn,
                        **kwargs2,
                    ),
                )
            else:
                return cast(
                    task.Task[Any],
                    self._ctx.call_sub_orchestrator(
                        self._workflow_fn,
                        input=self._input,
                        instance_id=self._instance_id,
                        retry_policy=self._retry_policy,
                    ),
                )


class SleepAwaitable(AwaitableBase[None]):
    """Awaitable for timer/sleep operations."""

    __slots__ = ("_ctx", "_duration")

    def __init__(self, ctx: Any, duration: Union[float, timedelta, datetime]):
        """
        Initialize a sleep awaitable.

        Args:
            ctx: The workflow context
            duration: Sleep duration (seconds, timedelta, or absolute datetime)
        """
        super().__init__()
        self._ctx = ctx
        self._duration = duration

    def _to_task(self) -> task.Task[Any]:
        """Convert to a create_timer task."""
        # Convert numeric durations to timedelta objects
        fire_at: Union[datetime, timedelta]
        if isinstance(self._duration, (int, float)):
            fire_at = timedelta(seconds=float(self._duration))
        else:
            fire_at = self._duration
        return cast(task.Task[Any], self._ctx.create_timer(fire_at))


class ExternalEventAwaitable(AwaitableBase[TOutput]):
    """Awaitable for external event operations."""

    __slots__ = ("_ctx", "_name")

    def __init__(self, ctx: Any, name: str):
        """
        Initialize an external event awaitable.

        Args:
            ctx: The workflow context
            name: Name of the external event to wait for
        """
        super().__init__()
        self._ctx = ctx
        self._name = name

    def _to_task(self) -> task.Task[Any]:
        """Convert to a wait_for_external_event task."""
        return cast(task.Task[Any], self._ctx.wait_for_external_event(self._name))


class WhenAllAwaitable(AwaitableBase[List[TOutput]]):
    """Awaitable for when_all operations (wait for all tasks to complete).

    Adds:
    - Empty fast-path: returns [] without creating a task
    - Multi-await safety: caches the result/exception for repeated awaits
    """

    __slots__ = ("_tasks_like", "_cached_result", "_cached_exception")

    def __init__(self, tasks_like: Iterable[Union[AwaitableBase[Any], task.Task[Any]]]):
        super().__init__()
        self._tasks_like = list(tasks_like)
        self._cached_result: Optional[List[Any]] = None
        self._cached_exception: Optional[BaseException] = None

    def _to_task(self) -> task.Task[Any]:
        """Convert to a when_all task."""
        # Empty fast-path: no durable task required
        if len(self._tasks_like) == 0:
            # Create a trivial completed task-like by when_all([])
            return cast(task.Task[Any], task.when_all([]))
        underlying: List[task.Task[Any]] = []
        for a in self._tasks_like:
            if isinstance(a, AwaitableBase):
                underlying.append(a._to_task())
            elif isinstance(a, task.Task):
                underlying.append(a)
            else:
                raise TypeError("when_all expects AwaitableBase or durabletask.task.Task")
        return cast(task.Task[Any], task.when_all(underlying))

    def __await__(self) -> Generator[Any, Any, List[TOutput]]:
        if self._cached_exception is not None:
            raise self._cached_exception
        if self._cached_result is not None:
            return cast(List[TOutput], self._cached_result)
        # Empty fast-path: return [] immediately
        if len(self._tasks_like) == 0:
            self._cached_result = []
            return cast(List[TOutput], self._cached_result)
        t = self._to_task()
        try:
            results = yield t
            # Cache and return (ensure list)
            self._cached_result = list(results) if isinstance(results, list) else [results]
            return cast(List[TOutput], self._cached_result)
        except BaseException as e:  # noqa: BLE001
            self._cached_exception = e
            raise


class WhenAnyAwaitable(AwaitableBase[task.Task[Any]]):
    """Awaitable for when_any operations (wait for any task to complete)."""

    __slots__ = ("_originals", "_underlying")

    def __init__(self, tasks_like: Iterable[Union[AwaitableBase[Any], task.Task[Any]]]):
        """
        Initialize a when_any awaitable.

        Args:
            tasks_like: Iterable of awaitables or tasks to wait for
        """
        super().__init__()
        self._originals = list(tasks_like)
        # Defer conversion to avoid issues with incomplete mocks and coroutine reuse
        self._underlying: Optional[List[task.Task[Any]]] = None

    def _ensure_underlying(self) -> List[task.Task[Any]]:
        """Lazily convert originals to tasks, caching the result."""
        if self._underlying is None:
            self._underlying = []
            for a in self._originals:
                if isinstance(a, AwaitableBase):
                    self._underlying.append(a._to_task())
                elif isinstance(a, task.Task):
                    self._underlying.append(a)
                else:
                    raise TypeError("when_any expects AwaitableBase or durabletask.task.Task")
        return self._underlying

    def _to_task(self) -> task.Task[Any]:
        """Convert to a when_any task."""
        return cast(task.Task[Any], task.when_any(self._ensure_underlying()))

    def __await__(self) -> Generator[Any, Any, Any]:
        """Return a proxy that compares equal to the original item and exposes get_result()."""
        underlying = self._ensure_underlying()
        when_any_task = task.when_any(underlying)
        completed = yield when_any_task

        class _CompletedProxy:
            __slots__ = ("_original", "_completed")

            def __init__(self, original: Any, completed_obj: Any):
                self._original = original
                self._completed = completed_obj

            def __eq__(self, other: object) -> bool:
                return other is self._original

            def get_result(self) -> Any:
                # Prefer task.get_result() if available, else try attribute access
                if hasattr(self._completed, "get_result") and callable(self._completed.get_result):
                    return self._completed.get_result()
                return getattr(self._completed, "result", None)

            @property
            def __dict__(self) -> dict[str, Any]:
                """Expose a dict-like view for compatibility with user code."""
                return {
                    "_original": self._original,
                    "_completed": self._completed,
                }

            def __repr__(self) -> str:  # pragma: no cover
                return f"<WhenAnyCompleted proxy for {self._original!r}>"

        # If the runtime returned a non-task sentinel (e.g., tests), assume first item won
        if not isinstance(completed, task.Task):
            return _CompletedProxy(self._originals[0], completed)

        # Map completed task back to the original item and return proxy
        for original, under in zip(self._originals, underlying, strict=False):
            if completed == under:
                return _CompletedProxy(original, completed)

        # Fallback proxy; treat the first as original
        return _CompletedProxy(self._originals[0], completed)


class WhenAnyResultAwaitable(AwaitableBase[tuple[int, Any]]):
    """
    Enhanced when_any that returns both the index and result of the first completed task.

    This is useful when you need to know which task completed first, not just its result.
    """

    __slots__ = ("_originals", "_underlying")

    def __init__(self, tasks_like: Iterable[Union[AwaitableBase[Any], task.Task[Any]]]):
        """
        Initialize a when_any_with_result awaitable.

        Args:
            tasks_like: Iterable of awaitables or tasks to wait for
        """
        super().__init__()
        self._originals = list(tasks_like)
        # Defer conversion to avoid issues with incomplete mocks and coroutine reuse
        self._underlying: Optional[List[task.Task[Any]]] = None

    def _ensure_underlying(self) -> List[task.Task[Any]]:
        """Lazily convert originals to tasks, caching the result."""
        if self._underlying is None:
            self._underlying = []
            for a in self._originals:
                if isinstance(a, AwaitableBase):
                    self._underlying.append(a._to_task())
                elif isinstance(a, task.Task):
                    self._underlying.append(a)
                else:
                    raise TypeError(
                        "when_any_with_result expects AwaitableBase or durabletask.task.Task"
                    )
        return self._underlying

    def _to_task(self) -> task.Task[Any]:
        """Convert to a when_any task with result tracking."""
        return cast(task.Task[Any], task.when_any(self._ensure_underlying()))

    def __await__(self) -> Generator[Any, Any, tuple[int, Any]]:
        """Override to provide index + result tuple."""
        underlying = self._ensure_underlying()
        when_any_task = task.when_any(underlying)
        completed_task = yield when_any_task

        # The completed_task should match one of our underlying tasks
        for i, underlying_task in enumerate(underlying):
            if underlying_task == completed_task:
                return (i, completed_task.result if hasattr(completed_task, "result") else None)

        # Fallback: return the completed task result with index 0
        return (0, completed_task.result if hasattr(completed_task, "result") else None)


class TimeoutAwaitable(AwaitableBase[TOutput]):
    """
    Awaitable that adds timeout functionality to any other awaitable.

    Raises TimeoutError if the operation doesn't complete within the specified time.
    """

    __slots__ = ("_awaitable", "_timeout_seconds", "_timeout", "_ctx", "_timeout_task")

    def __init__(self, awaitable: AwaitableBase[TOutput], timeout_seconds: float, ctx: Any):
        """
        Initialize a timeout awaitable.

        Args:
            awaitable: The awaitable to add timeout to
            timeout_seconds: Timeout in seconds
            ctx: The workflow context (needed for timer creation)
        """
        super().__init__()
        self._awaitable = awaitable
        self._timeout_seconds = timeout_seconds
        self._timeout = timeout_seconds  # Alias for compatibility
        self._ctx = ctx
        self._timeout_task: Optional[task.Task[Any]] = None

    def _to_task(self) -> task.Task[Any]:
        """Convert to a when_any between the operation and a timeout timer."""
        operation_task = self._awaitable._to_task()
        # Cache the timeout task instance so __await__ compares against the same object
        if self._timeout_task is None:
            self._timeout_task = cast(
                task.Task[Any], self._ctx.create_timer(timedelta(seconds=self._timeout_seconds))
            )
        return cast(task.Task[Any], task.when_any([operation_task, self._timeout_task]))

    def __await__(self) -> Generator[Any, Any, TOutput]:
        """Override to handle timeout logic."""
        task_obj = self._to_task()
        completed_task = yield task_obj
        # If runtime provided a sentinel instead of a Task, decide heuristically
        if not isinstance(completed_task, task.Task):
            # Dicts, lists, tuples, and simple primitives are considered operation results
            if isinstance(completed_task, (dict, list, tuple, str, int, float, bool, type(None))):
                return cast(TOutput, completed_task)
            # Otherwise, treat as timeout (e.g., mocks or opaque sentinels)
            from .errors import WorkflowTimeoutError

            raise WorkflowTimeoutError(
                timeout_seconds=self._timeout_seconds,
                operation=str(self._awaitable.__class__.__name__),
            )

        # Check if it was the timeout that completed (compare to cached instance)
        if self._timeout_task is not None and completed_task == self._timeout_task:
            from .errors import WorkflowTimeoutError

            raise WorkflowTimeoutError(
                timeout_seconds=self._timeout_seconds,
                operation=str(self._awaitable.__class__.__name__),
            )

        # Return the actual result
        return cast(TOutput, completed_task.result if hasattr(completed_task, "result") else None)


class SwallowExceptionAwaitable(AwaitableBase[Any]):
    """
    Awaitable that swallows exceptions and returns them as values.

    This is useful for gather operations with return_exceptions=True.
    """

    __slots__ = ("_awaitable",)

    def __init__(self, awaitable: AwaitableBase[Any]):
        """
        Initialize a swallow exception awaitable.

        Args:
            awaitable: The awaitable to wrap
        """
        super().__init__()
        self._awaitable = awaitable

    def _to_task(self) -> task.Task[Any]:
        """Convert to the underlying task."""
        return self._awaitable._to_task()

    def __await__(self) -> Generator[Any, Any, Any]:
        """Override to catch and return exceptions."""
        try:
            t = self._to_task()
            result = yield t
            return result
        except Exception as e:  # noqa: BLE001
            return e


# Utility functions for working with awaitables


def _resolve_callable(module_name: str, qualname: str) -> Callable[..., Any]:
    """
    Resolve a callable from module name and qualified name.

    This is used internally for gather operations that need to serialize
    and deserialize callable references.
    """
    mod = importlib.import_module(module_name)
    obj: Any = mod
    for part in qualname.split("."):
        obj = getattr(obj, part)
    if not callable(obj):
        raise TypeError(f"resolved object {module_name}.{qualname} is not callable")
    return cast(Callable[..., Any], obj)


def gather(
    *awaitables: AwaitableBase[Any], return_exceptions: bool = False
) -> WhenAllAwaitable[Any]:
    """
    Gather multiple awaitables, similar to asyncio.gather.

    Args:
        *awaitables: The awaitables to gather
        return_exceptions: If True, exceptions are returned as results instead of raised

    Returns:
        A WhenAllAwaitable that will complete when all awaitables complete
    """
    if return_exceptions:
        # Wrap each awaitable to swallow exceptions
        wrapped = [SwallowExceptionAwaitable(aw) for aw in awaitables]
        return WhenAllAwaitable(wrapped)
    # Empty fast-path handled by WhenAllAwaitable
    return WhenAllAwaitable(awaitables)
