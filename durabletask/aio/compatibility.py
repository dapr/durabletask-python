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
Compatibility protocol for AsyncWorkflowContext.

This module provides the core protocol definition that AsyncWorkflowContext
must implement to maintain compatibility with OrchestrationContext.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Optional, Protocol, Union, runtime_checkable

from durabletask import task


@runtime_checkable
class OrchestrationContextProtocol(Protocol):
    """
    Protocol defining the interface that AsyncWorkflowContext must maintain
    for compatibility with OrchestrationContext.

    This protocol ensures that AsyncWorkflowContext provides all the essential
    properties and methods expected by the base OrchestrationContext interface.
    """

    # Core properties
    @property
    def instance_id(self) -> str:
        """Get the ID of the current orchestration instance."""
        ...

    @property
    def current_utc_datetime(self) -> datetime:
        """Get the current date/time as UTC."""
        ...

    @property
    def is_replaying(self) -> bool:
        """Get whether the orchestrator is replaying from history."""
        ...

    @property
    def workflow_name(self) -> Optional[str]:
        """Get the orchestrator name/type for this instance."""
        ...

    @property
    def is_suspended(self) -> bool:
        """Get whether this orchestration is currently suspended."""
        ...

    # Core methods
    def set_custom_status(self, custom_status: Any) -> None:
        """Set the orchestration instance's custom status."""
        ...

    def create_timer(self, fire_at: Union[datetime, timedelta]) -> Any:
        """Create a Timer Task to fire at the specified deadline."""
        ...

    def call_activity(
        self,
        activity: Union[task.Activity[Any, Any], str],
        *,
        input: Optional[Any] = None,
        retry_policy: Optional[task.RetryPolicy] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Schedule an activity for execution."""
        ...

    def call_sub_orchestrator(
        self,
        orchestrator: Union[task.Orchestrator[Any, Any], str],
        *,
        input: Optional[Any] = None,
        instance_id: Optional[str] = None,
        retry_policy: Optional[task.RetryPolicy] = None,
        metadata: Optional[Dict[str, str]] = None,
    ) -> Any:
        """Schedule sub-orchestrator function for execution."""
        ...

    def wait_for_external_event(self, name: str) -> Any:
        """Wait asynchronously for an event to be raised."""
        ...

    def continue_as_new(self, new_input: Any) -> None:
        """Continue the orchestration execution as a new instance."""
        ...


def ensure_compatibility(context_class: type) -> type:
    """
    Decorator to ensure a context class maintains OrchestrationContext compatibility.

    This is a lightweight decorator that performs basic structural validation
    at class definition time.

    Args:
        context_class: The context class to validate

    Returns:
        The same class (for use as decorator)

    Raises:
        TypeError: If the class doesn't implement required protocol
    """
    # Basic structural check - ensure required attributes exist
    required_properties = [
        "instance_id",
        "current_utc_datetime",
        "is_replaying",
        "workflow_name",
        "is_suspended",
    ]

    required_methods = [
        "set_custom_status",
        "create_timer",
        "call_activity",
        "call_sub_orchestrator",
        "wait_for_external_event",
        "continue_as_new",
    ]

    missing_items = []

    for prop_name in required_properties:
        if not hasattr(context_class, prop_name):
            missing_items.append(f"property: {prop_name}")

    for method_name in required_methods:
        if not hasattr(context_class, method_name):
            missing_items.append(f"method: {method_name}")

    if missing_items:
        raise TypeError(
            f"{context_class.__name__} does not implement OrchestrationContextProtocol. "
            f"Missing: {', '.join(missing_items)}"
        )

    return context_class
