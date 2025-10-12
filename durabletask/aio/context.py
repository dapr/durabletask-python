# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Generic async workflow context for DurableTask workflows.

This module provides a generic AsyncWorkflowContext that can be used across
different SDK implementations, providing a consistent async interface for
workflow operations.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union, cast

from durabletask import task as dt_task
from durabletask.deterministic import DeterministicContextMixin

from .awaitables import (
    ActivityAwaitable,
    AwaitableBase,
    ExternalEventAwaitable,
    SleepAwaitable,
    SubOrchestratorAwaitable,
    TimeoutAwaitable,
    WhenAllAwaitable,
    WhenAnyAwaitable,
    WhenAnyResultAwaitable,
    gather,
)
from .compatibility import ensure_compatibility

# Generic type variable for awaitable result (module-level)
T = TypeVar("T")


@dataclass(frozen=True)
class WorkflowInfo:
    """
    Read-only metadata snapshot about the running workflow execution.

    Similar to Temporal's workflow.info, this provides convenient access to
    workflow execution metadata in a single immutable object.
    """

    instance_id: str
    workflow_name: Optional[str]
    is_replaying: bool
    is_suspended: bool
    parent_instance_id: Optional[str]
    current_time: datetime
    history_event_sequence: int
    trace_parent: Optional[str]
    trace_state: Optional[str]
    orchestration_span_id: Optional[str]


@ensure_compatibility
class AsyncWorkflowContext(DeterministicContextMixin):
    """
    Generic async workflow context providing a consistent interface for workflow operations.

    This context wraps a base DurableTask OrchestrationContext and provides async-friendly
    methods for common workflow operations like calling activities, creating timers,
    waiting for external events, and coordinating multiple operations.
    """

    __slots__ = (
        "_base_ctx",
        "_rng",
        "_debug_mode",
        "_operation_history",
        "_cleanup_tasks",
        "_detection_disabled",
        "_workflow_name",
        "_current_step",
        "_sandbox_originals",
        "_sandbox_mode",
    )

    # Generic type variable for awaitable result
    def __init__(self, base_ctx: dt_task.OrchestrationContext):
        """
        Initialize the async workflow context.

        Args:
            base_ctx: The underlying DurableTask OrchestrationContext
        """
        self._base_ctx = base_ctx
        self._rng = None
        self._debug_mode = os.getenv("DAPR_WF_DEBUG") == "true" or os.getenv("DT_DEBUG") == "true"
        self._operation_history: list[Dict[str, Any]] = []
        self._cleanup_tasks: list[Callable[[], Any]] = []
        self._workflow_name: Optional[str] = None
        self._current_step: Optional[str] = None
        # Set by sandbox when active
        self._sandbox_originals: Optional[Dict[str, Any]] = None
        self._sandbox_mode: Optional[str] = None

        # Performance optimization: Check if detection should be globally disabled
        self._detection_disabled = os.getenv("DAPR_WF_DISABLE_DETECTION") == "true"

    # Core properties from base context
    @property
    def instance_id(self) -> str:
        """Get the workflow instance ID."""
        return self._base_ctx.instance_id

    @property
    def current_utc_datetime(self) -> datetime:
        """Get the current orchestration time."""
        return self._base_ctx.current_utc_datetime

    @property
    def is_replaying(self) -> bool:
        """Check if the workflow is currently replaying."""
        return self._base_ctx.is_replaying

    @property
    def is_suspended(self) -> bool:
        """Check if the workflow is currently suspended."""
        return getattr(self._base_ctx, "is_suspended", False)

    @property
    def workflow_name(self) -> Optional[str]:
        """Get the workflow name."""
        return getattr(self._base_ctx, "workflow_name", None)

    @property
    def parent_instance_id(self) -> Optional[str]:
        """Get the parent instance ID (for sub-orchestrators)."""
        return getattr(self._base_ctx, "parent_instance_id", None)

    @property
    def history_event_sequence(self) -> int:
        """Get the current history event sequence number."""
        return getattr(self._base_ctx, "history_event_sequence", 0)

    # Tracing properties (if available)
    @property
    def trace_parent(self) -> Optional[str]:
        """Get the trace parent for distributed tracing."""
        return getattr(self._base_ctx, "trace_parent", None)

    @property
    def trace_state(self) -> Optional[str]:
        """Get the trace state for distributed tracing."""
        return getattr(self._base_ctx, "trace_state", None)

    @property
    def orchestration_span_id(self) -> Optional[str]:
        """Get the orchestration span ID for tracing."""
        return getattr(self._base_ctx, "orchestration_span_id", None)

    @property
    def execution_info(self) -> Optional[Any]:
        """Get execution_info from the base context if available, else None."""
        return getattr(self._base_ctx, "execution_info", None)

    @property
    def workflow_span_id(self) -> Optional[str]:
        """Alias for orchestration_span_id for compatibility."""
        return self.orchestration_span_id

    @property
    def info(self) -> WorkflowInfo:
        """
        Get a read-only snapshot of workflow execution metadata.

        This provides a Temporal-style info object bundling instance_id, workflow_name,
        is_replaying, timestamps, tracing info, and other metadata in a single immutable object.
        Useful for deterministic logging, idempotency keys, and conditional logic based on replay state.

        Returns:
            WorkflowInfo: Immutable dataclass with workflow execution metadata
        """
        return WorkflowInfo(
            instance_id=self.instance_id,
            workflow_name=self.workflow_name,
            is_replaying=self.is_replaying,
            is_suspended=self.is_suspended,
            parent_instance_id=self.parent_instance_id,
            current_time=self.current_utc_datetime,
            history_event_sequence=self.history_event_sequence,
            trace_parent=self.trace_parent,
            trace_state=self.trace_state,
            orchestration_span_id=self.orchestration_span_id,
        )

    # Unsafe escape hatch: real wall-clock UTC now (best_effort only, not during replay)
    def unsafe_wall_clock_now(self) -> datetime:
        """
        Return the real UTC wall-clock time.

        Constraints:
        - Raises RuntimeError if called during replay.
        - Raises RuntimeError if sandbox mode is 'strict'.
        - Intended for telemetry/logging only; do not use for workflow decisions.
        """
        if self.is_replaying:
            raise RuntimeError("unsafe_wall_clock_now() cannot be used during replay")
        mode = getattr(self, "_sandbox_mode", None)
        if mode == "strict":
            raise RuntimeError("unsafe_wall_clock_now() is disabled in strict sandbox mode")
        originals = getattr(self, "_sandbox_originals", None)
        if not originals or "time.time" not in originals:
            # Fallback to system if sandbox not active
            import time as _time
            from datetime import timezone

            return datetime.fromtimestamp(_time.time(), tz=timezone.utc)
        real_time = originals["time.time"]
        try:
            from datetime import timezone

            ts = float(real_time())  # type: ignore[call-arg]
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception as e:
            raise RuntimeError(f"unsafe_wall_clock_now() failed: {e}")

    # Activity operations
    def activity(
        self,
        activity_fn: Union[dt_task.Activity[Any, Any], str],
        *,
        input: Any = None,
        retry_policy: Any = None,
        app_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> ActivityAwaitable[Any]:
        """
        Create an awaitable for calling an activity function.

        Args:
            activity_fn: The activity function or name to call
            input: Input data for the activity
            retry_policy: Optional retry policy
            metadata: Optional metadata for the activity call

        Returns:
            An awaitable that will complete when the activity finishes
        """
        self._log_operation("activity", {"function": str(activity_fn), "input": input})
        return ActivityAwaitable(
            self._base_ctx,
            cast(Callable[..., Any], activity_fn),
            input=input,
            retry_policy=retry_policy,
            app_id=app_id,
            metadata=metadata,
        )

    def call_activity(
        self,
        activity_fn: Union[dt_task.Activity[Any, Any], str],
        *,
        input: Any = None,
        retry_policy: Any = None,
        app_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> ActivityAwaitable[Any]:
        """Alias for activity() method for API compatibility."""
        return self.activity(
            activity_fn,
            input=input,
            retry_policy=retry_policy,
            app_id=app_id,
            metadata=metadata,
        )

    # Sub-orchestrator operations
    def sub_orchestrator(
        self,
        workflow_fn: Union[dt_task.Orchestrator[Any, Any], str],
        *,
        input: Any = None,
        instance_id: Optional[str] = None,
        retry_policy: Any = None,
        app_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> SubOrchestratorAwaitable[Any]:
        """
        Create an awaitable for calling a sub-orchestrator.

        Args:
            workflow_fn: The sub-orchestrator function or name to call
            input: Input data for the sub-orchestrator
            instance_id: Optional instance ID for the sub-orchestrator
            retry_policy: Optional retry policy
            metadata: Optional metadata for the sub-orchestrator call

        Returns:
            An awaitable that will complete when the sub-orchestrator finishes
        """
        self._log_operation(
            "sub_orchestrator",
            {"function": str(workflow_fn), "input": input, "instance_id": instance_id},
        )
        return SubOrchestratorAwaitable(
            self._base_ctx,
            cast(Callable[..., Any], workflow_fn),
            input=input,
            instance_id=instance_id,
            retry_policy=retry_policy,
            app_id=app_id,
            metadata=metadata,
        )

    def call_sub_orchestrator(
        self,
        workflow_fn: Union[dt_task.Orchestrator[Any, Any], str],
        *,
        input: Any = None,
        instance_id: Optional[str] = None,
        retry_policy: Any = None,
        app_id: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> SubOrchestratorAwaitable[Any]:
        """Call a sub-orchestrator workflow (durabletask naming convention)."""
        return self.sub_orchestrator(
            workflow_fn,
            input=input,
            instance_id=instance_id,
            retry_policy=retry_policy,
            app_id=app_id,
            metadata=metadata,
        )

    # Timer operations
    def sleep(self, duration: Union[float, timedelta, datetime]) -> SleepAwaitable:
        """
        Create an awaitable for sleeping/waiting.

        Args:
            duration: Sleep duration (seconds, timedelta, or absolute datetime)

        Returns:
            An awaitable that will complete after the specified duration
        """
        self._log_operation("sleep", {"duration": duration})
        return SleepAwaitable(self._base_ctx, duration)

    def create_timer(self, duration: Union[float, timedelta, datetime]) -> SleepAwaitable:
        """Alias for sleep() method for API compatibility."""
        return self.sleep(duration)

    # External event operations
    def wait_for_external_event(self, name: str) -> ExternalEventAwaitable[Any]:
        """
        Create an awaitable for waiting for an external event.

        Args:
            name: Name of the external event to wait for

        Returns:
            An awaitable that will complete when the external event is received
        """
        self._log_operation("wait_for_external_event", {"name": name})
        return ExternalEventAwaitable(self._base_ctx, name)

    # Coordination operations
    def when_all(self, awaitables: List[Any]) -> WhenAllAwaitable[Any]:
        """
        Create an awaitable that completes when all provided awaitables complete.

        Args:
            awaitables: List of awaitables to wait for

        Returns:
            An awaitable that will complete with a list of all results
        """
        self._log_operation("when_all", {"count": len(awaitables)})
        return WhenAllAwaitable(awaitables)

    def when_any(self, awaitables: List[Any]) -> WhenAnyAwaitable:
        """
        Create an awaitable that completes when any of the provided awaitables completes.

        Args:
            awaitables: List of awaitables to wait for

        Returns:
            An awaitable that will complete with the first completed task
        """
        self._log_operation("when_any", {"count": len(awaitables)})
        return WhenAnyAwaitable(awaitables)

    def when_any_with_result(self, awaitables: List[Any]) -> WhenAnyResultAwaitable:
        """
        Create an awaitable that completes when any awaitable completes, returning index and result.

        Args:
            awaitables: List of awaitables to wait for

        Returns:
            An awaitable that will complete with (index, result) tuple
        """
        self._log_operation("when_any_with_result", {"count": len(awaitables)})
        return WhenAnyResultAwaitable(awaitables)

    def gather(
        self, *awaitables: AwaitableBase[Any], return_exceptions: bool = False
    ) -> WhenAllAwaitable[Any]:
        """
        Gather multiple awaitables, similar to asyncio.gather.

        Args:
            *awaitables: The awaitables to gather
            return_exceptions: If True, exceptions are returned as results instead of raised

        Returns:
            An awaitable that will complete when all awaitables complete
        """
        self._log_operation(
            "gather", {"count": len(awaitables), "return_exceptions": return_exceptions}
        )
        return gather(*awaitables, return_exceptions=return_exceptions)

    # Enhanced operations
    def with_timeout(self, awaitable: "AwaitableBase[T]", timeout: float) -> TimeoutAwaitable[T]:
        """
        Add timeout functionality to any awaitable.

        Args:
            awaitable: The awaitable to add timeout to
            timeout: Timeout in seconds

        Returns:
            An awaitable that will raise TimeoutError if not completed within timeout
        """
        self._log_operation("with_timeout", {"timeout": timeout})
        return TimeoutAwaitable(awaitable, float(timeout), self._base_ctx)

    # Custom status operations
    def set_custom_status(self, status: Any) -> None:
        """
        Set custom status for the workflow instance.

        Args:
            status: Custom status object
        """
        if hasattr(self._base_ctx, "set_custom_status"):
            self._base_ctx.set_custom_status(status)
        self._log_operation("set_custom_status", {"status": status})

    def continue_as_new(self, input_data: Any = None, *, save_events: bool = False) -> None:
        """
        Continue the workflow as new with optional new input.

        Args:
            input_data: Optional new input data
            save_events: Whether to save events (matches base durabletask API)
        """
        self._log_operation("continue_as_new", {"input": input_data, "save_events": save_events})

        if hasattr(self._base_ctx, "continue_as_new"):
            # For compatibility with mocks/tests expecting positional-only when default is used,
            # call without the keyword when save_events is False; otherwise pass explicitly.
            if save_events is False:
                self._base_ctx.continue_as_new(input_data)
            else:
                self._base_ctx.continue_as_new(input_data, save_events=save_events)

    # Metadata and header methods
    def set_metadata(self, metadata: Dict[str, str]) -> None:
        """
        Set metadata for the workflow instance.

        Args:
            metadata: Dictionary of metadata key-value pairs
        """
        if hasattr(self._base_ctx, "set_metadata"):
            self._base_ctx.set_metadata(metadata)
        self._log_operation("set_metadata", {"metadata": metadata})

    def get_metadata(self) -> Optional[Dict[str, str]]:
        """
        Get metadata for the workflow instance.

        Returns:
            Dictionary of metadata or None if not available
        """
        if hasattr(self._base_ctx, "get_metadata"):
            val: Any = self._base_ctx.get_metadata()
            if isinstance(val, dict):
                return cast(Dict[str, str], val)
        return None

    def set_headers(self, headers: Dict[str, str]) -> None:
        """
        Set headers for the workflow instance (alias for set_metadata).

        Args:
            headers: Dictionary of header key-value pairs
        """
        self.set_metadata(headers)

    def get_headers(self) -> Optional[Dict[str, str]]:
        """
        Get headers for the workflow instance (alias for get_metadata).

        Returns:
            Dictionary of headers or None if not available
        """
        return self.get_metadata()

    # Enhanced context management
    async def __aenter__(self) -> "AsyncWorkflowContext":
        """Async context manager entry."""
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        """Async context manager exit with cleanup."""
        # Run cleanup tasks in reverse order (LIFO)
        for cleanup_task in reversed(self._cleanup_tasks):
            try:
                result = cleanup_task()
                # If the cleanup returns an awaitable, await it
                try:
                    import inspect as _inspect

                    if _inspect.isawaitable(result):
                        await result
                except Exception:
                    # If inspection fails, ignore and continue
                    pass
            except Exception as e:
                if self._debug_mode:
                    print(f"[WORKFLOW DEBUG] Cleanup task failed: {e}")

        self._cleanup_tasks.clear()

    def add_cleanup(self, cleanup_fn: Callable[[], Any]) -> None:
        """
        Add a cleanup function to be called when the context exits.

        Args:
            cleanup_fn: Function to call during cleanup
        """
        self._cleanup_tasks.append(cleanup_fn)

    # Debug and monitoring
    def _log_operation(self, operation: str, details: Dict[str, Any]) -> None:
        """Log workflow operation for debugging."""
        if self._debug_mode:
            entry = {
                "type": operation,  # Use "type" for compatibility
                "operation": operation,
                "details": details,
                "sequence": len(self._operation_history),
                "timestamp": self.current_utc_datetime.isoformat(),
                "is_replaying": self.is_replaying,
            }
            self._operation_history.append(entry)
            print(f"[WORKFLOW DEBUG] {operation}: {details}")

    def get_debug_info(self) -> Dict[str, Any]:
        """
        Get debug information about the workflow execution.

        Returns:
            Dictionary containing debug information
        """
        return {
            "instance_id": self.instance_id,
            "current_time": self.current_utc_datetime.isoformat(),
            "is_replaying": self.is_replaying,
            "is_suspended": self.is_suspended,
            "operation_history": self._operation_history.copy(),
            "cleanup_tasks_count": len(self._cleanup_tasks),
            "debug_mode": self._debug_mode,
            "detection_disabled": self._detection_disabled,
        }

    def __repr__(self) -> str:
        """String representation of the context."""
        return (
            f"AsyncWorkflowContext("
            f"instance_id='{self.instance_id}', "
            f"is_replaying={self.is_replaying}, "
            f"operations={len(self._operation_history)})"
        )
