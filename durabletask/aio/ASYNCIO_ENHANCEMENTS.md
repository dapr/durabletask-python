# Enhanced Async Workflow Features

This document describes the enhanced async workflow capabilities added to this fork of durabletask-python. For a deep dive into architecture and internals, see [ASYNCIO_INTERNALS.md](ASYNCIO_INTERNALS.md).

## Overview

This fork extends the original durabletask-python SDK with comprehensive async workflow enhancements, providing a production-ready async authoring experience with advanced debugging, error handling, and determinism enforcement.

## Quick Start

```python
from durabletask.worker import TaskHubGrpcWorker
from durabletask.aio import AsyncWorkflowContext, SandboxMode

async def enhanced_workflow(ctx: AsyncWorkflowContext, input_data) -> str:
    # Enhanced error handling with rich context
    try:
        result = await ctx.with_timeout(
            ctx.call_activity("my_activity", input=input_data),
            30.0,  # 30 second timeout
        )
    except TimeoutError:
        result = "Activity timed out"
    
    # Enhanced concurrency with result indexing
    tasks = [ctx.call_activity(f"task_{i}") for i in range(3)]
    completed_index, first_result = await ctx.when_any_with_result(tasks)
    
    # Deterministic operations
    current_time = ctx.now()
    random_value = ctx.random().random()
    unique_id = ctx.uuid4()
    
    return {
        "result": result,
        "first_completed": completed_index,
        "timestamp": current_time.isoformat(),
        "random": random_value,
        "id": str(unique_id)
    }

# Register with enhanced features
with TaskHubGrpcWorker() as worker:
    # Async orchestrators are auto-detected - both forms work:
    worker.add_orchestrator(enhanced_workflow)  # Auto-detects async
    
    # Or specify sandbox mode explicitly:
    worker.add_orchestrator(
        enhanced_workflow,
        sandbox_mode=SandboxMode.BEST_EFFORT  # or "best_effort"
    )
    
    worker.start()
    # ... rest of your code
```

## Enhanced Features

### 1. **Advanced Error Handling**
- `AsyncWorkflowError` with rich context (instance ID, workflow name, step)
- Enhanced error messages with actionable suggestions
- Better exception propagation and debugging support

### 2. **Non-Determinism Detection**
- Automatic detection of non-deterministic function calls
- Three modes: `"off"` (default), `"best_effort"` (warnings), `"strict"` (errors)
- Comprehensive coverage of problematic functions
- Helpful suggestions for deterministic alternatives

### 3. **Enhanced Concurrency Primitives**
- `when_any_with_result()` - Returns (index, result) tuple
- `with_timeout()` - Add timeout to any operation
- `gather(*awaitables, return_exceptions=False)` - Compose awaitables:
  - Preserves input order; returns list of results
  - `return_exceptions=True` captures exceptions as values
  - Empty gather resolves immediately to `[]`
  - Safe to await the same gather result multiple times (cached)

### 4. **Async Context Management**
- Full async context manager support (`async with ctx:`)
- Cleanup task registry with `ctx.add_cleanup()`
- Automatic resource cleanup

### 5. **Debugging and Monitoring**
- Operation history tracking when debug mode is enabled
- `ctx.get_debug_info()` for workflow introspection
- Enhanced logging with operation details

### 6. **Performance Optimizations**
- `__slots__` on all awaitable classes for memory efficiency
- Optimized hot paths in coroutine-to-generator bridge
- Reduced object allocations

### 7. **Enhanced Sandboxing**
- Extended coverage of non-deterministic functions
- Strict mode blocks for dangerous operations
- Better patching of time, random, and UUID functions

### 8. **Type Safety**
- Runtime validation of workflow functions
- Enhanced type annotations
- `WorkflowFunction` protocol for better IDE support

## Registration

Async orchestrators are automatically detected when using `add_orchestrator()`:

```python
from durabletask.aio import SandboxMode

# Auto-detection - simplest form
worker.add_orchestrator(my_async_workflow)

# With explicit sandbox mode
worker.add_orchestrator(
    my_async_workflow,
    sandbox_mode=SandboxMode.BEST_EFFORT  # or "best_effort" string
)
```

Note: The `sandbox_mode` parameter accepts both `SandboxMode` enum values and string literals (`"off"`, `"best_effort"`, `"strict"`).

## Sandbox Modes

Control non-determinism detection with the `sandbox_mode` parameter:

```python
# Production: Zero overhead (default)
worker.add_orchestrator(workflow, sandbox_mode="off")

# Development: Warnings for non-deterministic calls
worker.add_orchestrator(workflow, sandbox_mode=SandboxMode.BEST_EFFORT)

# Testing: Errors for non-deterministic calls
worker.add_orchestrator(workflow, sandbox_mode=SandboxMode.STRICT)
```

Why enable detection (briefly):
- Catch accidental non-determinism in development (BEST_EFFORT) before it ships.
- Keep production fast with zero overhead (OFF).
- Enforce determinism in CI (STRICT) to prevent regressions.

### Performance Impact
- `"off"`: Zero overhead (recommended for production)
- `"best_effort"/"strict"`: ~100-200% overhead due to Python tracing
- Global disable: Set `DAPR_WF_DISABLE_DETECTION=true` environment variable

## Environment Variables

- `DAPR_WF_DEBUG=true` / `DT_DEBUG=true` - Enable debug logging, operation tracking, and non-determinism warnings
- `DAPR_WF_DISABLE_DETECTION=true` - Globally disable non-determinism detection

## Developer Mode
## Workflow Metadata and Headers (Async Only)

Purpose:
- Carry lightweight key/value context (e.g., tracing IDs, tenant, app info) across workflow steps.
- Enable routing and observability without embedding data into workflow inputs/outputs.

API:
```python
md_before = ctx.get_metadata()  # Optional[Dict[str, str]]
ctx.set_metadata({"tenant": "acme", "x-trace": trace_id})

# Header aliases (same data for users familiar with other SDKs)
ctx.set_headers({"region": "us-east"})
headers = ctx.get_headers()
```

Notes:
- In python-sdk, metadata/headers are available for both async and generator orchestrators; this repo currently implements the asyncio path.
- Metadata is intended for small strings; avoid large payloads.
- Sidecar integrations may forward metadata as gRPC headers to activities and sub-orchestrations.

Set `DAPR_WF_DEBUG=true` during development to enable:
- Non-determinism warnings for problematic function calls
- Detailed operation logging and debugging information
- Enhanced error messages with suggested alternatives

```bash
# Enable developer warnings
export DAPR_WF_DEBUG=true
python your_workflow.py

# Production mode (no warnings, optimal performance)
unset DAPR_WF_DEBUG
python your_workflow.py
```

This approach is similar to tools like mypy - rich feedback during development, zero runtime overhead in production.

## Examples

### Timeout Support
```python
from durabletask.aio import AsyncWorkflowContext

async def workflow_with_timeout(ctx: AsyncWorkflowContext, input_data) -> str:
    try:
        result = await ctx.with_timeout(
            ctx.call_activity("slow_activity"),
            10.0,  # timeout first
        )
    except TimeoutError:
        result = "Operation timed out"
    return result
```

### Enhanced when_any
Note: `when_any` still exists. `when_any_with_result` is an addition for cases where you also want the index of the first completed.

```python
# Both forms are supported
winner_value = await ctx.when_any(tasks)
winner_index, winner_value = await ctx.when_any_with_result(tasks)
```
```python
async def competitive_workflow(ctx, input_data):
    tasks = [
        ctx.call_activity("provider_a"),
        ctx.call_activity("provider_b"), 
        ctx.call_activity("provider_c")
    ]
    
    # Get both index and result of first completed
    winner_index, result = await ctx.when_any_with_result(tasks)
    return f"Provider {winner_index} won with: {result}"
```

### Error Handling with Context
```python
async def robust_workflow(ctx, input_data):
    try:
        return await ctx.call_activity("risky_activity")
    except Exception as e:
        # Enhanced error will include workflow context
        debug_info = ctx.get_debug_info()
        return {"error": str(e), "debug": debug_info}
```

### Cleanup Tasks
```python
async def workflow_with_cleanup(ctx, input_data):
    async with ctx:  # Automatic cleanup
        # Register cleanup tasks
        ctx.add_cleanup(lambda: print("Workflow completed"))
        
        result = await ctx.call_activity("main_work")
        return result
    # Cleanup tasks run automatically here
```

## Best Practices

1. **Use deterministic alternatives**:
   - `ctx.now()` instead of `datetime.now()` (async workflows)
   - `context.current_utc_datetime` instead of `datetime.now()` (generator/non-async)
   - `ctx.random()` instead of `random`
   - `ctx.uuid4()` instead of `uuid.uuid4()`

2. **Enable detection during development**:
   ```python
   sandbox_mode = "best_effort" if os.getenv("ENV") == "dev" else "off"
   ```

3. **Add timeouts to external operations**:
   ```python
   result = await ctx.with_timeout(ctx.call_activity("external_api"), 30.0)
   ```

4. **Use cleanup tasks for resource management**:
   ```python
   ctx.add_cleanup(lambda: cleanup_resources())
   ```

5. **Enable debug mode during development**:
   ```bash
   export DAPR_WF_DEBUG=true
   ```
