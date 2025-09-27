# Enhanced Async Workflow Features

This document describes the enhanced features available in the asyncio compatibility layer for Dapr workflows.

## New Features

### 1. Enhanced Error Handling

The `AsyncWorkflowError` class provides better error context:

```python
async def my_workflow(ctx, input_data):
    try:
        result = await ctx.activity("risky_activity")
    except Exception as e:
        # Enhanced error context is automatically added
        # Error will include instance_id, workflow_name, and step information
        raise
```

### 2. Debugging and Monitoring

Enable debug mode to track workflow operations:

```python
import os
os.environ["DAPR_WF_DEBUG"] = "true"  # or DT_DEBUG

def to_upper(_, s: str) -> str:
    return s.upper()

async def my_workflow(ctx, input_data):
    # Operations are automatically logged in debug mode
    await ctx.activity(to_upper, input="hello")
    
    # Get debug information
    debug_info = ctx.get_debug_info()
    print(f"Operations executed: {debug_info['operation_count']}")
```

### 3. Async Context Management

Use workflows as async context managers for automatic cleanup:

```python
async def my_workflow(ctx, input_data):
    async with ctx:
        # Register cleanup tasks
        def cleanup():
            print("Workflow cleanup executed")
        
        ctx.add_cleanup(cleanup)
        
        # Do workflow work
        result = await ctx.activity(to_upper, input="hello")
        return result
    # Cleanup tasks run automatically here
```

### 4. Enhanced Concurrency Primitives

#### Timeout Support

Add timeouts to any operation:

```python
async def my_workflow(ctx, input_data):
    try:
        # Timeout after 30 seconds
        result = await ctx.with_timeout(
            ctx.activity("slow_activity"),
            30.0,
        )
    except TimeoutError:
        return "Activity timed out"
```

#### Enhanced when_any with Results

Get both the index and result of the first completed task:

```python
async def my_workflow(ctx, input_data):
    tasks = [
        ctx.activity("task_a"),
        ctx.activity("task_b"),
        ctx.activity("task_c"),
    ]
    
    # Returns (index, result) tuple
    completed_index, result = await ctx.when_any_with_result(tasks)
    print(f"Task {completed_index} completed first with: {result}")
```

#### Improved Error Handling with gather

Handle exceptions gracefully in parallel operations:

```python
async def my_workflow(ctx, input_data):
    tasks = [ctx.activity(to_upper, input=str(i)) for i in range(3)]
    
    # Get results and exceptions
    results = await ctx.gather(*tasks, return_exceptions=True)
    
    successes = []
    failures = []
    
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            failures.append(f"Task {i} failed: {result}")
        else:
            successes.append(f"Task {i} succeeded: {result}")
    
    return {"successes": successes, "failures": failures}
```

### 5. Enhanced Sandboxing

Use strict mode to prevent non-deterministic operations:

```python
from durabletask.aio import SandboxMode

# Register workflow with strict sandboxing
worker.add_async_orchestrator(
    my_workflow,
    name="my_workflow",
    sandbox_mode=SandboxMode.STRICT  # Blocks file I/O, os.urandom, secrets module, etc.
)
```

Available sandbox modes:
- `"off"`: No sandboxing (default)
- `"best_effort"`: Patches common non-deterministic functions
- `"strict"`: Blocks dangerous operations and patches deterministic equivalents

### 6. Deterministic Utilities

Use deterministic alternatives to common operations:

```python
async def my_workflow(ctx, input_data):
    # Deterministic time (same on replay)
    current_time = ctx.now()
    
    # Deterministic random (same on replay)
    rng = ctx.random()
    random_number = rng.randint(1, 100)
    
    # Deterministic UUID (same on replay)
    unique_id = ctx.uuid4()
    
    return {
        "timestamp": current_time.isoformat(),
        "random_number": random_number,
        "unique_id": str(unique_id)
    }
```

### 7. Performance Optimizations

The enhanced version includes several performance improvements:

- `__slots__` on all awaitable classes to reduce memory usage
- Optimized hot paths in the coroutine-to-generator bridge
- Reduced object allocations during workflow execution

### 8. Type Safety

Enhanced type annotations and runtime validation:

```python
from durabletask.aio import WorkflowFunction

# Workflows are validated at registration time
async def my_workflow(ctx: AsyncWorkflowContext, input_data: dict) -> str:
    return "result"

# This will raise AsyncWorkflowError if validation fails
runner = CoroutineOrchestratorRunner(my_workflow)
```

## Migration Guide

### From Basic Async Workflows

Existing async workflows continue to work without changes:

```python
# This still works exactly as before
async def existing_workflow(ctx, input_data):
    result = await ctx.activity(to_upper, input="hello")
    await ctx.sleep(1)
    return result
```

### Adding Enhanced Features

To use new features, simply update your workflow code:

```python
async def enhanced_workflow(ctx, input_data):
    # Enable debug logging
    debug_info = ctx.get_debug_info()
    
    # Use timeout for reliability
    try:
        result = await ctx.with_timeout(
            ctx.activity("my_activity"),
            10.0,            
        )
    except TimeoutError:
        result = "default_value"
    
    # Use async context management
    async with ctx:
        ctx.add_cleanup(lambda: print("Cleanup executed"))
        return result
```

## Best Practices

1. **Use `ctx.now()` instead of `datetime.now()`** for deterministic time
2. **Use `ctx.random()` instead of `random`** for deterministic randomness
3. **Use `ctx.uuid4()` instead of `uuid.uuid4()`** for deterministic UUIDs
4. **Enable debug mode during development** with `DAPR_WF_DEBUG=true`
5. **Sandbox modes**: BEST_EFFORT (dev), STRICT (CI), OFF (prod)
6. **Add timeouts to external operations** to prevent hanging workflows
7. **Use cleanup tasks** for resource management and logging

## Examples

See the following examples for complete demonstrations:

- `async_enhanced_features.py` - Comprehensive example showcasing all new features
- `async_activity_sequence.py` - Basic async workflow (unchanged)
- `async_external_event.py` - External event handling (unchanged)

### 9. Non-Determinism Detection

The enhanced version includes automatic detection of non-deterministic function calls:

```python
from durabletask.aio import SandboxMode

# This will generate warnings in best_effort mode
async def problematic_workflow(ctx, input_data):
    import datetime as dt
    import random
    import uuid
    
    # These will trigger warnings
    current_time = dt.datetime.now()  # ⚠️ NON-DETERMINISTIC
    random_val = random.random()      # ⚠️ NON-DETERMINISTIC  
    uuid_val = uuid.uuid4()           # ⚠️ NON-DETERMINISTIC
    
    # Use these deterministic alternatives instead
    det_time = ctx.now()              # ✅ DETERMINISTIC
    det_random = ctx.random().random() # ✅ DETERMINISTIC
    det_uuid = ctx.uuid4()            # ✅ DETERMINISTIC
    
    return {"time": det_time, "random": det_random, "uuid": str(det_uuid)}

# Register with detection enabled
worker.add_async_orchestrator(
    problematic_workflow,
    name="my_workflow", 
    sandbox_mode=SandboxMode.BEST_EFFORT  # Warns about non-deterministic calls
)
```

**Detection Modes:**
- `"off"`: No detection (default - zero overhead)
- `"best_effort"`: Warns about non-deterministic calls but continues execution
- `"strict"`: Raises `AsyncWorkflowError` for non-deterministic calls

**Performance Impact:**
- `"off"`: Zero overhead (recommended for production)
- `"best_effort"/"strict"`: ~100-200% overhead due to Python tracing
- Global disable: Set `DAPR_WF_DISABLE_DETECTION=true` to force disable all detection

**Detected Functions:**
- `datetime.now()`, `datetime.utcnow()`
- `time.time()`, `time.time_ns()`
- `random.random()`, `random.randint()`, `random.choice()`
- `uuid.uuid1()`, `uuid.uuid4()`
- `os.urandom()`, `secrets.token_bytes()`
- File I/O operations (`open()`)
- And more...

## Limitations

1. **datetime.datetime patching**: Due to immutability, `datetime.datetime.now()` cannot be patched. Use `ctx.now()` instead.
2. **Sandboxing scope**: Sandboxing only applies during workflow execution steps, not globally.
3. **Protocol validation**: The `WorkflowFunction` protocol uses structural typing, so runtime validation is still needed.
4. **Detection limitations**: The tracing-based detection may not catch all non-deterministic calls, especially in complex scenarios.

## Troubleshooting

### Common Issues

1. **TimeoutError in workflows**: Check if activities are taking longer than expected
2. **Non-deterministic behavior**: Enable strict mode to catch problematic code
3. **Memory usage**: The performance optimizations should reduce memory usage, but monitor large workflows

### Debug Mode

Enable debug logging to troubleshoot issues:

```bash
export DAPR_WF_DEBUG=true
# or
export DT_DEBUG=true
```

This will log all workflow operations and provide detailed debug information.
