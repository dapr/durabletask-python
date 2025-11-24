# Enhanced Async Workflow Features

This document describes the enhanced async workflow capabilities added to this fork of durabletask-python. For a deep dive into architecture and internals, see [ASYNCIO_INTERNALS.md](ASYNCIO_INTERNALS.md).

## Overview

The durabletask-python SDK includes comprehensive async workflow enhancements, providing a production-ready async authoring experience with advanced debugging, error handling, and determinism enforcement. This works seamlessly with the existing python workflow authoring experience.

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
- Three modes: `"best_effort"` (default), `"strict"` (errors), `"off"` (no patching)
- Comprehensive coverage of problematic functions
- Helpful suggestions for deterministic alternatives

### 3. **Enhanced Concurrency Primitives**
- `when_all()` - Waits for all tasks to complete and returns list of results in order
- `when_any()` - Returns (index, result) tuple indicating which task completed first
- `with_timeout()` - Add timeout to any operation

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
# Default: Patches asyncio functions for determinism, optional warnings
worker.add_orchestrator(workflow)  # Uses "best_effort" by default

# Development: Same as default, warnings when debug mode enabled
worker.add_orchestrator(workflow, sandbox_mode=SandboxMode.BEST_EFFORT)

# Testing: Errors for non-deterministic calls
worker.add_orchestrator(workflow, sandbox_mode=SandboxMode.STRICT)

# No patching: Use only if all code uses ctx.* methods explicitly
worker.add_orchestrator(workflow, sandbox_mode="off")
```

Why "best_effort" is the default:
- Makes standard asyncio patterns work correctly (asyncio.sleep, asyncio.gather, etc.)
- Patches random/time/uuid to be deterministic automatically
- Optional warnings only when debug mode is enabled (low overhead)
- Provides "pit of success" for async workflow authoring

### Performance Impact
- `"best_effort"` (default): Minimal overhead from function patching. Tracing overhead present but uses lightweight noop tracer unless debug mode is enabled.
- `"strict"`: ~100-200% overhead due to full Python tracing for detection
- `"off"`: Zero overhead (no patching, no tracing)
- Global disable: Set `DAPR_WF_DISABLE_DETERMINISTIC_DETECTION=true` environment variable

Note: Function patching overhead is minimal (single-digit percentage). Tracing overhead (when enabled) is more significant due to Python's sys.settrace() mechanism.

## Environment Variables

- `DAPR_WF_DEBUG=true` / `DT_DEBUG=true` - Enable debug logging, operation tracking, and non-determinism warnings
- `DAPR_WF_DISABLE_DETERMINISTIC_DETECTION=true` - Globally disable non-determinism detection

## Developer Mode

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

### when_any with index and result

```python
async def competitive_workflow(ctx, input_data):
    tasks = [
        ctx.call_activity("provider_a"),
        ctx.call_activity("provider_b"), 
        ctx.call_activity("provider_c")
    ]
    
    # when_any returns (index, result) tuple
    winner_index, result = await ctx.when_any(tasks)
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

2. **Use strict mode in testing**:
   ```python
   sandbox_mode = "strict" if os.getenv("CI") else "best_effort"
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
