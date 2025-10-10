"""Async workflow primitives (aio namespace).

This package contains the async implementation previously under
`durabletask.asyncio`, now moved to `durabletask.aio` for naming
consistency.
"""

# Deterministic utilities
from durabletask.deterministic import (
    DeterminismSeed,
    DeterministicContextMixin,
    derive_seed,
    deterministic_random,
    deterministic_uuid4,
)

# Awaitable classes
from .awaitables import (
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
    gather,
)

# Compatibility protocol (core functionality only)
from .compatibility import OrchestrationContextProtocol, ensure_compatibility

# Core context and driver
from .context import AsyncWorkflowContext, WorkflowInfo
from .driver import CoroutineOrchestratorRunner, WorkflowFunction

# Sandbox and error handling
from .errors import (
    AsyncWorkflowError,
    NonDeterminismWarning,
    SandboxViolationError,
    WorkflowTimeoutError,
    WorkflowValidationError,
)
from .sandbox import (
    SandboxMode,
    _NonDeterminismDetector,
    sandbox_best_effort,
    sandbox_off,
    sandbox_scope,
    sandbox_strict,
)

__all__ = [
    # Core classes
    "AsyncWorkflowContext",
    "WorkflowInfo",
    "CoroutineOrchestratorRunner",
    "WorkflowFunction",
    # Deterministic utilities
    "DeterministicContextMixin",
    "DeterminismSeed",
    "derive_seed",
    "deterministic_random",
    "deterministic_uuid4",
    # Awaitable classes
    "AwaitableBase",
    "ActivityAwaitable",
    "SubOrchestratorAwaitable",
    "SleepAwaitable",
    "ExternalEventAwaitable",
    "WhenAllAwaitable",
    "WhenAnyAwaitable",
    "WhenAnyResultAwaitable",
    "TimeoutAwaitable",
    "SwallowExceptionAwaitable",
    "gather",
    # Sandbox and utilities
    "sandbox_scope",
    "SandboxMode",
    "sandbox_off",
    "sandbox_best_effort",
    "sandbox_strict",
    "_NonDeterminismDetector",
    # Compatibility protocol
    "OrchestrationContextProtocol",
    "ensure_compatibility",
    # Exceptions
    "AsyncWorkflowError",
    "NonDeterminismWarning",
    "WorkflowTimeoutError",
    "WorkflowValidationError",
    "SandboxViolationError",
]
