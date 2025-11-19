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
Compatibility testing utilities for AsyncWorkflowContext.

This module provides utilities for testing and validating AsyncWorkflowContext
compatibility with OrchestrationContext. These are testing/validation utilities
and should not be part of the main production code.
"""

from __future__ import annotations

import inspect
import warnings
from datetime import datetime
from typing import Any, Dict
from unittest.mock import Mock

from durabletask import task


class CompatibilityChecker:
    """
    Utility class for checking AsyncWorkflowContext compatibility with OrchestrationContext.

    This class provides methods to validate that AsyncWorkflowContext maintains
    all required properties and methods for compatibility.
    """

    @staticmethod
    def check_protocol_compliance(context_class: type) -> bool:
        """
        Check if a context class complies with the OrchestrationContextProtocol.

        Args:
            context_class: The context class to check

        Returns:
            True if the class complies with the protocol, False otherwise
        """
        # For protocols with properties, we need to check the class structure
        # rather than using issubclass() which doesn't work with property protocols

        # Get all required members from the protocol
        required_properties = [
            "instance_id",
            "current_utc_datetime",
            "is_replaying",
            "workflow_name",
            "parent_instance_id",
            "history_event_sequence",
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

        # Check if the class has all required members
        for prop_name in required_properties:
            if not hasattr(context_class, prop_name):
                return False

        for method_name in required_methods:
            if not hasattr(context_class, method_name):
                return False

        return True

    @staticmethod
    def validate_context_compatibility(context_instance: Any) -> list[str]:
        """
        Validate that a context instance has all required properties and methods.

        Args:
            context_instance: The context instance to validate

        Returns:
            List of missing properties/methods (empty if fully compatible)
        """
        missing_items = []

        # Check required properties
        required_properties = [
            "instance_id",
            "current_utc_datetime",
            "is_replaying",
            "workflow_name",
            "parent_instance_id",
            "history_event_sequence",
            "is_suspended",
        ]

        for prop_name in required_properties:
            if not hasattr(context_instance, prop_name):
                missing_items.append(f"property: {prop_name}")

        # Check required methods
        required_methods = [
            "set_custom_status",
            "create_timer",
            "call_activity",
            "call_sub_orchestrator",
            "wait_for_external_event",
            "continue_as_new",
        ]

        for method_name in required_methods:
            if not hasattr(context_instance, method_name):
                missing_items.append(f"method: {method_name}")
            elif not callable(getattr(context_instance, method_name)):
                missing_items.append(f"method: {method_name} (not callable)")

        return missing_items

    @staticmethod
    def compare_with_orchestration_context(context_instance: Any) -> Dict[str, Any]:
        """
        Compare a context instance with OrchestrationContext interface.

        Args:
            context_instance: The context instance to compare

        Returns:
            Dictionary with comparison results
        """
        # Get OrchestrationContext members
        base_members = {}
        for name, member in inspect.getmembers(task.OrchestrationContext):
            if not name.startswith("_"):
                if isinstance(member, property):
                    base_members[name] = "property"
                elif inspect.isfunction(member):
                    base_members[name] = "method"

        # Check context instance
        context_members = {}
        missing_members = []
        extra_members = []

        for name, member_type in base_members.items():
            if hasattr(context_instance, name):
                context_members[name] = member_type
            else:
                missing_members.append(f"{member_type}: {name}")

        # Find extra members (enhancements)
        for name, member in inspect.getmembers(context_instance):
            if (
                not name.startswith("_")
                and name not in base_members
                and (isinstance(member, property) or callable(member))
            ):
                member_type = "property" if isinstance(member, property) else "method"
                extra_members.append(f"{member_type}: {name}")

        return {
            "base_members": base_members,
            "context_members": context_members,
            "missing_members": missing_members,
            "extra_members": extra_members,
            "is_compatible": len(missing_members) == 0,
        }

    @staticmethod
    def warn_about_compatibility_issues(context_instance: Any) -> None:
        """
        Issue warnings about any compatibility issues found.

        Args:
            context_instance: The context instance to check
        """
        missing_items = CompatibilityChecker.validate_context_compatibility(context_instance)

        if missing_items:
            warning_msg = (
                f"AsyncWorkflowContext compatibility issue: missing {', '.join(missing_items)}. "
                "This may cause issues with upstream merges or when used as OrchestrationContext."
            )
            warnings.warn(warning_msg, UserWarning, stacklevel=3)


def validate_runtime_compatibility(context_instance: Any, *, strict: bool = False) -> bool:
    """
    Validate runtime compatibility of a context instance.

    Args:
        context_instance: The context instance to validate
        strict: If True, raise exception on compatibility issues; if False, just warn

    Returns:
        True if compatible, False otherwise

    Raises:
        RuntimeError: If strict=True and compatibility issues are found
    """
    missing_items = CompatibilityChecker.validate_context_compatibility(context_instance)

    if missing_items:
        error_msg = (
            f"Runtime compatibility check failed: {context_instance.__class__.__name__} "
            f"is missing {', '.join(missing_items)}"
        )

        if strict:
            raise RuntimeError(error_msg)
        else:
            warnings.warn(error_msg, UserWarning, stacklevel=2)
            return False

    return True


def check_async_context_compatibility() -> Dict[str, Any]:
    """
    Check AsyncWorkflowContext compatibility with OrchestrationContext.

    Returns:
        Dictionary with detailed compatibility information
    """
    from durabletask.aio import AsyncWorkflowContext

    # Create a mock base context for testing
    mock_base_ctx = Mock(spec=task.OrchestrationContext)
    mock_base_ctx.instance_id = "test"
    mock_base_ctx.current_utc_datetime = datetime.now()
    mock_base_ctx.is_replaying = False

    # Create AsyncWorkflowContext instance
    async_ctx = AsyncWorkflowContext(mock_base_ctx)

    # Perform compatibility check
    return CompatibilityChecker.compare_with_orchestration_context(async_ctx)
