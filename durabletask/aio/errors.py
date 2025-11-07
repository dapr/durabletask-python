# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Enhanced error handling for async workflows.

This module provides specialized exceptions for async workflow operations
with rich context information to aid in debugging and error handling.
"""

from __future__ import annotations

from typing import Any, Optional


class AsyncWorkflowError(Exception):
    """Enhanced exception for async workflow issues with context information."""

    def __init__(
        self,
        message: str,
        *,
        instance_id: Optional[str] = None,
        step: Optional[str] = None,
        workflow_name: Optional[str] = None,
    ):
        """
        Initialize an AsyncWorkflowError with context information.

        Args:
            message: The error message
            instance_id: The workflow instance ID where the error occurred
            step: The workflow step/operation where the error occurred
            workflow_name: The name of the workflow where the error occurred
        """
        self.instance_id = instance_id
        self.step = step
        self.workflow_name = workflow_name

        context_parts = []
        if workflow_name:
            context_parts.append(f"workflow: {workflow_name}")
        if instance_id:
            context_parts.append(f"instance: {instance_id}")
        if step:
            context_parts.append(f"step: {step}")

        context_str = f" ({', '.join(context_parts)})" if context_parts else ""
        super().__init__(f"{message}{context_str}")


class NonDeterminismWarning(UserWarning):
    """Warning raised when non-deterministic functions are detected in workflows."""

    pass


class WorkflowTimeoutError(AsyncWorkflowError):
    """Exception raised when a workflow operation times out."""

    def __init__(
        self,
        message: str = "Operation timed out",
        *,
        timeout_seconds: Optional[float] = None,
        operation: Optional[str] = None,
        **kwargs: Any,
    ):
        """
        Initialize a WorkflowTimeoutError.

        Args:
            message: The error message
            timeout_seconds: The timeout value that was exceeded
            operation: The operation that timed out
            **kwargs: Additional context passed to AsyncWorkflowError
        """
        self.timeout_seconds = timeout_seconds
        self.operation = operation

        if timeout_seconds and operation:
            message = f"{operation} timed out after {timeout_seconds}s"
        elif timeout_seconds:
            message = f"Operation timed out after {timeout_seconds}s"
        elif operation:
            message = f"{operation} timed out"

        super().__init__(message, **kwargs)


class WorkflowValidationError(AsyncWorkflowError):
    """Exception raised when workflow validation fails."""

    def __init__(self, message: str, *, validation_type: Optional[str] = None, **kwargs: Any):
        """
        Initialize a WorkflowValidationError.

        Args:
            message: The error message
            validation_type: The type of validation that failed
            **kwargs: Additional context passed to AsyncWorkflowError
        """
        self.validation_type = validation_type

        if validation_type:
            message = f"{validation_type} validation failed: {message}"

        super().__init__(message, **kwargs)


class SandboxViolationError(AsyncWorkflowError):
    """Exception raised when sandbox restrictions are violated."""

    def __init__(
        self,
        message: str,
        *,
        violation_type: Optional[str] = None,
        suggested_alternative: Optional[str] = None,
        **kwargs: Any,
    ):
        """
        Initialize a SandboxViolationError.

        Args:
            message: The error message
            violation_type: The type of sandbox violation
            suggested_alternative: Suggested alternative approach
            **kwargs: Additional context passed to AsyncWorkflowError
        """
        self.violation_type = violation_type
        self.suggested_alternative = suggested_alternative

        full_message = message
        if suggested_alternative:
            full_message += f". Consider using: {suggested_alternative}"

        super().__init__(full_message, **kwargs)
