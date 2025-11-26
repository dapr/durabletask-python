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
Driver for async workflow orchestrators in durabletask.aio.

This module provides the CoroutineOrchestratorRunner that bridges async/await
syntax with the generator-based DurableTask runtime, ensuring proper replay
semantics and deterministic execution.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Generator
from typing import Any, Callable, Optional, Protocol, TypeVar, cast, runtime_checkable

from durabletask import task
from durabletask.aio.errors import AsyncWorkflowError, WorkflowValidationError
from durabletask.aio.sandbox import SandboxMode

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


@runtime_checkable
class WorkflowFunction(Protocol):
    """Protocol for workflow functions."""

    async def __call__(self, ctx: Any, input_data: Optional[Any] = None) -> Any: ...


class CoroutineOrchestratorRunner:
    """
    Wraps an async orchestrator function into a generator-compatible runner.

    This class bridges the gap between async/await syntax and the generator-based
    DurableTask runtime, enabling developers to write workflows using modern
    async Python while maintaining deterministic execution semantics.

    The implementation uses an iterator pattern to properly handle replay scenarios
    and avoid coroutine reuse issues that can occur during workflow replay.
    """

    __slots__ = ("_async_orchestrator", "_sandbox_mode", "_workflow_name")

    def __init__(
        self,
        async_orchestrator: Callable[..., Awaitable[Any]],
        *,
        sandbox_mode: str = "best_effort",
        workflow_name: Optional[str] = None,
    ):
        """
        Initialize the coroutine orchestrator runner.

        Args:
            async_orchestrator: The async workflow function to wrap
            sandbox_mode: Sandbox mode ('off', 'best_effort', 'strict'). Default: 'best_effort'
            workflow_name: Optional workflow name for error reporting
        """
        self._async_orchestrator = async_orchestrator
        self._sandbox_mode = sandbox_mode
        name_attr = getattr(async_orchestrator, "__name__", None)
        base_name: str = name_attr if isinstance(name_attr, str) else "unknown"
        self._workflow_name: str = workflow_name if workflow_name is not None else base_name
        self._validate_orchestrator(async_orchestrator)

    def _validate_orchestrator(self, orchestrator_fn: Callable[..., Awaitable[Any]]) -> None:
        """
        Validate that the orchestrator function is suitable for async workflows.

        Args:
            orchestrator_fn: The function to validate

        Raises:
            WorkflowValidationError: If the function is not valid
        """
        if not callable(orchestrator_fn):
            raise WorkflowValidationError(
                "Orchestrator must be callable",
                validation_type="function_type",
                workflow_name=self._workflow_name,
            )

        if not inspect.iscoroutinefunction(orchestrator_fn):
            raise WorkflowValidationError(
                "Orchestrator must be an async function (defined with 'async def')",
                validation_type="async_function",
                workflow_name=self._workflow_name,
            )

        # Check function signature
        sig = inspect.signature(orchestrator_fn)
        params = list(sig.parameters.values())

        if len(params) < 1:
            raise WorkflowValidationError(
                "Orchestrator must accept at least one parameter (context)",
                validation_type="function_signature",
                workflow_name=self._workflow_name,
            )

        if len(params) > 2:
            raise WorkflowValidationError(
                "Orchestrator must accept at most two parameters (context, input)",
                validation_type="function_signature",
                workflow_name=self._workflow_name,
            )

    def to_generator(
        self, async_ctx: Any, input_data: Optional[Any] = None
    ) -> Generator[task.Task[Any], Any, Any]:
        """
        Convert the async orchestrator to a generator that the DurableTask runtime can drive.

        This implementation uses an iterator pattern similar to the original to properly
        handle replay scenarios and avoid coroutine reuse issues.

        Args:
            async_ctx: The async workflow context
            input_data: Optional input data for the workflow

        Returns:
            A generator that yields tasks and receives results

        Raises:
            AsyncWorkflowError: If there are issues during workflow execution
        """
        # Import sandbox here to avoid circular imports
        from .sandbox import _sandbox_scope

        def driver_gen() -> Generator[task.Task[Any], Any, Any]:
            """Inner generator that drives the coroutine execution."""
            # Instantiate the coroutine with appropriate parameters
            try:
                sig = inspect.signature(self._async_orchestrator)
                params = list(sig.parameters.values())

                if len(params) == 1:
                    # Single parameter (context only)
                    coro = self._async_orchestrator(async_ctx)
                else:
                    # Two parameters (context and input)
                    coro = self._async_orchestrator(async_ctx, input_data)

            except TypeError as e:
                raise AsyncWorkflowError(
                    f"Failed to instantiate workflow coroutine: {e}",
                    workflow_name=self._workflow_name,
                    step="initialization",
                ) from e

            # Prime the coroutine to first await point or finish synchronously
            try:
                if self._sandbox_mode == SandboxMode.OFF:
                    awaited_obj = cast(Any, coro).send(None)
                else:
                    with _sandbox_scope(async_ctx, self._sandbox_mode):
                        awaited_obj = cast(Any, coro).send(None)
            except StopIteration as stop:
                return stop.value
            except Exception as e:
                # Close the coroutine to avoid "never awaited" warning
                coro.close()
                # Re-raise NonRetryableError directly to preserve its type for the runtime
                if isinstance(e, task.NonRetryableError):
                    raise
                raise AsyncWorkflowError(
                    f"Workflow failed during initialization: {e}",
                    workflow_name=self._workflow_name,
                    instance_id=getattr(async_ctx, "instance_id", None),
                    step="initialization",
                ) from e

            def to_iter(obj: Any) -> Generator[Any, Any, Any]:
                if hasattr(obj, "__await__"):
                    return cast(Generator[Any, Any, Any], obj.__await__())
                if isinstance(obj, task.Task):
                    # Wrap a single Task into a one-shot awaitable iterator
                    def _one_shot() -> Generator[task.Task[Any], Any, Any]:
                        res = yield obj
                        return res

                    return _one_shot()
                raise AsyncWorkflowError(
                    f"Async orchestrator awaited unsupported object type: {type(obj)}",
                    workflow_name=self._workflow_name,
                    step="awaitable_conversion",
                )

            awaited_iter = to_iter(awaited_obj)
            while True:
                # Advance the awaitable to a DT Task to yield
                try:
                    request = awaited_iter.send(None)
                except StopIteration as stop_await:
                    # Awaitable finished synchronously; feed result back to coroutine
                    try:
                        if self._sandbox_mode == "off":
                            awaited_obj = cast(Any, coro).send(stop_await.value)
                        else:
                            with _sandbox_scope(async_ctx, self._sandbox_mode):
                                awaited_obj = cast(Any, coro).send(stop_await.value)
                    except StopIteration as stop:
                        return stop.value
                    except Exception as e:
                        # Close the coroutine to avoid "never awaited" warning
                        coro.close()
                        # Check if this is a TaskFailedError wrapping a NonRetryableError
                        if isinstance(e, task.TaskFailedError):
                            details = e.details
                            if details.error_type == "NonRetryableError":
                                # Reconstruct NonRetryableError to preserve its type for the runtime
                                raise task.NonRetryableError(details.message) from e
                        # Re-raise NonRetryableError directly to preserve its type for the runtime
                        if isinstance(e, task.NonRetryableError):
                            raise
                        raise AsyncWorkflowError(
                            f"Workflow failed: {e}",
                            workflow_name=self._workflow_name,
                            step="execution",
                        ) from e
                    awaited_iter = to_iter(awaited_obj)
                    continue

                if not isinstance(request, task.Task):
                    raise AsyncWorkflowError(
                        f"Async awaitable yielded a non-Task object: {type(request)}",
                        workflow_name=self._workflow_name,
                        step="execution",
                    )

                # Yield to runtime and resume awaitable with task result
                try:
                    result = yield request
                except Exception as e:
                    # Route exception into awaitable first; if it completes, continue; otherwise forward to coroutine
                    try:
                        awaited_iter.throw(e)
                    except StopIteration as stop_await:
                        try:
                            if self._sandbox_mode == SandboxMode.OFF:
                                awaited_obj = cast(Any, coro).send(stop_await.value)
                            else:
                                with _sandbox_scope(async_ctx, self._sandbox_mode):
                                    awaited_obj = cast(Any, coro).send(stop_await.value)
                        except StopIteration as stop:
                            return stop.value
                        except Exception as workflow_exc:
                            # Close the coroutine to avoid "never awaited" warning
                            coro.close()
                            # Check if this is a TaskFailedError wrapping a NonRetryableError
                            if isinstance(workflow_exc, task.TaskFailedError):
                                details = workflow_exc.details
                                if details.error_type == "NonRetryableError":
                                    # Reconstruct NonRetryableError to preserve its type for the runtime
                                    raise task.NonRetryableError(details.message) from workflow_exc
                            # Re-raise NonRetryableError directly to preserve its type for the runtime
                            if isinstance(workflow_exc, task.NonRetryableError):
                                raise
                            raise AsyncWorkflowError(
                                f"Workflow failed: {workflow_exc}",
                                workflow_name=self._workflow_name,
                                step="execution",
                            ) from workflow_exc
                        awaited_iter = to_iter(awaited_obj)
                    except Exception as exc:
                        try:
                            if self._sandbox_mode == SandboxMode.OFF:
                                awaited_obj = cast(Any, coro).throw(exc)
                            else:
                                with _sandbox_scope(async_ctx, self._sandbox_mode):
                                    awaited_obj = cast(Any, coro).throw(exc)
                        except StopIteration as stop:
                            return stop.value
                        except Exception as workflow_exc:
                            # Close the coroutine to avoid "never awaited" warning
                            coro.close()
                            # Check if this is a TaskFailedError wrapping a NonRetryableError
                            if isinstance(workflow_exc, task.TaskFailedError):
                                details = workflow_exc.details
                                if details.error_type == "NonRetryableError":
                                    # Reconstruct NonRetryableError to preserve its type for the runtime
                                    raise task.NonRetryableError(details.message) from workflow_exc
                            # Re-raise NonRetryableError directly to preserve its type for the runtime
                            if isinstance(workflow_exc, task.NonRetryableError):
                                raise
                            raise AsyncWorkflowError(
                                f"Workflow failed: {workflow_exc}",
                                workflow_name=self._workflow_name,
                                step="execution",
                            ) from workflow_exc
                        awaited_iter = to_iter(awaited_obj)
                    continue

                # Success: feed result to awaitable; it may yield more tasks until it stops
                try:
                    next_req = awaited_iter.send(result)
                    while True:
                        if not isinstance(next_req, task.Task):
                            raise AsyncWorkflowError(
                                f"Async awaitable yielded a non-Task object: {type(next_req)}",
                                workflow_name=self._workflow_name,
                                step="execution",
                            )
                        result = yield next_req
                        next_req = awaited_iter.send(result)
                except StopIteration as stop_await:
                    try:
                        if self._sandbox_mode == SandboxMode.OFF:
                            awaited_obj = cast(Any, coro).send(stop_await.value)
                        else:
                            with _sandbox_scope(async_ctx, self._sandbox_mode):
                                awaited_obj = cast(Any, coro).send(stop_await.value)
                    except StopIteration as stop:
                        return stop.value
                    except Exception as e:
                        # Check if this is a TaskFailedError wrapping a NonRetryableError
                        if isinstance(e, task.TaskFailedError):
                            details = e.details
                            if details.error_type == "NonRetryableError":
                                # Reconstruct NonRetryableError to preserve its type for the runtime
                                raise task.NonRetryableError(details.message) from e
                        # Re-raise NonRetryableError directly to preserve its type for the runtime
                        if isinstance(e, task.NonRetryableError):
                            raise
                        raise AsyncWorkflowError(
                            f"Workflow failed: {e}",
                            workflow_name=self._workflow_name,
                            step="execution",
                        ) from e
                    awaited_iter = to_iter(awaited_obj)

        return driver_gen()

    @property
    def workflow_name(self) -> str:
        """Get the workflow name."""
        return self._workflow_name

    @property
    def sandbox_mode(self) -> str:
        """Get the sandbox mode."""
        return self._sandbox_mode
