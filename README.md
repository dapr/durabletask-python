# Durable Task Client SDK for Python (Dapr fork)

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Build Validation](https://github.com/microsoft/durabletask-python/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/microsoft/durabletask-python/actions/workflows/pr-validation.yml)
[![PyPI version](https://badge.fury.io/py/durabletask.svg)](https://badge.fury.io/py/durabletask)

This repo contains a Python client SDK for use with the [Durable Task Framework for Go](https://github.com/microsoft/durabletask-go) and [Dapr Workflow](https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-overview/). With this SDK, you can define, schedule, and manage durable orchestrations using ordinary Python code.

> **🚀 Enhanced Async Features**: This fork includes comprehensive async workflow enhancements with advanced error handling, non-determinism detection, timeout support, and debugging tools. See [ASYNC_ENHANCEMENTS.md](./ASYNC_ENHANCEMENTS.md) for details.

## Quick Start - Async Workflows

For async workflow development, use the new `durabletask.aio` package:

```python
from durabletask.aio import AsyncWorkflowContext
from durabletask.worker import TaskHubGrpcWorker

async def my_workflow(ctx: AsyncWorkflowContext, name: str) -> str:
    result = await ctx.call_activity(say_hello, input=name)
    await ctx.sleep(1.0)
    return f"Workflow completed: {result}"

def say_hello(ctx, name: str) -> str:
    return f"Hello, {name}!"

# Register and run
with TaskHubGrpcWorker() as worker:
    worker.add_activity(say_hello)
    worker.add_orchestrator(my_workflow)
    worker.start()
    # ... schedule workflows with client
```

⚠️ **This SDK is currently under active development and is not yet ready for production use.** ⚠️

> Note that this project is **not** currently affiliated with the [Durable Functions](https://docs.microsoft.com/azure/azure-functions/durable/durable-functions-overview) project for Azure Functions. If you are looking for a Python SDK for Durable Functions, please see [this repo](https://github.com/Azure/azure-functions-durable-python).


## Supported patterns

The following orchestration patterns are currently supported.

### Function chaining

An orchestration can chain a sequence of function calls using the following syntax:

```python
# simple activity function that returns a greeting
def hello(ctx: task.ActivityContext, name: str) -> str:
    return f'Hello {name}!'

# orchestrator function that sequences the activity calls
def sequence(ctx: task.OrchestrationContext, _):
    result1 = yield ctx.call_activity(hello, input='Tokyo')
    result2 = yield ctx.call_activity(hello, input='Seattle')
    result3 = yield ctx.call_activity(hello, input='London')

    return [result1, result2, result3]
```

You can find the full sample [here](./examples/activity_sequence.py).

### Fan-out/fan-in

An orchestration can fan-out a dynamic number of function calls in parallel and then fan-in the results using the following syntax:

```python
# activity function for getting the list of work items
def get_work_items(ctx: task.ActivityContext, _) -> List[str]:
    # ...

# activity function for processing a single work item
def process_work_item(ctx: task.ActivityContext, item: str) -> int:
    # ...

# orchestrator function that fans-out the work items and then fans-in the results
def orchestrator(ctx: task.OrchestrationContext, _):
    # the number of work-items is unknown in advance
    work_items = yield ctx.call_activity(get_work_items)

    # fan-out: schedule the work items in parallel and wait for all of them to complete
    tasks = [ctx.call_activity(process_work_item, input=item) for item in work_items]
    results = yield task.when_all(tasks)

    # fan-in: summarize and return the results
    return {'work_items': work_items, 'results': results, 'total': sum(results)}
```

You can find the full sample [here](./examples/fanout_fanin.py).

### Human interaction and durable timers

An orchestration can wait for a user-defined event, such as a human approval event, before proceding to the next step. In addition, the orchestration can create a timer with an arbitrary duration that triggers some alternate action if the external event hasn't been received:

```python
def purchase_order_workflow(ctx: task.OrchestrationContext, order: Order):
    """Orchestrator function that represents a purchase order workflow"""
    # Orders under $1000 are auto-approved
    if order.Cost < 1000:
        return "Auto-approved"

    # Orders of $1000 or more require manager approval
    yield ctx.call_activity(send_approval_request, input=order)

    # Approvals must be received within 24 hours or they will be canceled.
    approval_event = ctx.wait_for_external_event("approval_received")
    timeout_event = ctx.create_timer(timedelta(hours=24))
    winner = yield task.when_any([approval_event, timeout_event])
    if winner == timeout_event:
        return "Canceled"

    # The order was approved
    yield ctx.call_activity(place_order, input=order)
    approval_details = approval_event.get_result()
    return f"Approved by '{approval_details.approver}'"
```

As an aside, you'll also notice that the example orchestration above works with custom business objects. Support for custom business objects includes support for custom classes, custom data classes, and named tuples. Serialization and deserialization of these objects is handled automatically by the SDK.

You can find the full sample [here](./examples/human_interaction.py).

## Feature overview

The following features are currently supported:

### Orchestrations

Orchestrations are implemented using ordinary Python functions that take an `OrchestrationContext` as their first parameter. The `OrchestrationContext` provides APIs for starting child orchestrations, scheduling activities, and waiting for external events, among other things. Orchestrations are fault-tolerant and durable, meaning that they can automatically recover from failures and rebuild their local execution state. Orchestrator functions must be deterministic, meaning that they must always produce the same output given the same input.

### Activities

Activities are implemented using ordinary Python functions that take an `ActivityContext` as their first parameter. Activity functions are scheduled by orchestrations and have at-least-once execution guarantees, meaning that they will be executed at least once but may be executed multiple times in the event of a transient failure. Activity functions are where the real "work" of any orchestration is done.

### Durable timers

Orchestrations can schedule durable timers using the `create_timer` API. These timers are durable, meaning that they will survive orchestrator restarts and will fire even if the orchestrator is not actively in memory. Durable timers can be of any duration, from milliseconds to months.

### Sub-orchestrations

Orchestrations can start child orchestrations using the `call_sub_orchestrator` API. Child orchestrations are useful for encapsulating complex logic and for breaking up large orchestrations into smaller, more manageable pieces.

### External events

Orchestrations can wait for external events using the `wait_for_external_event` API. External events are useful for implementing human interaction patterns, such as waiting for a user to approve an order before continuing.

### Continue-as-new

Orchestrations can be continued as new using the `continue_as_new` API. This API allows an orchestration to restart itself from scratch, optionally with a new input.

### Suspend, resume, and terminate

Orchestrations can be suspended using the `suspend_orchestration` client API and will remain suspended until resumed using the `resume_orchestration` client API. A suspended orchestration will stop processing new events, but will continue to buffer any that happen to arrive until resumed, ensuring that no data is lost. An orchestration can also be terminated using the `terminate_orchestration` client API. Terminated orchestrations will stop processing new events and will discard any buffered events.

### Retry policies

Orchestrations can specify retry policies for activities and sub-orchestrations. These policies control how many times and how frequently an activity or sub-orchestration will be retried in the event of a transient error.

## Getting Started

### Prerequisites

- Python 3.9
- A Durable Task-compatible sidecar, like [Dapr Workflow](https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-overview/)

### Installing the Durable Task Python client SDK

Installation is currently only supported from source. Ensure pip, setuptools, and wheel are up-to-date.

```sh
python3 -m pip install --upgrade pip setuptools wheel
```

To install this package from source, clone this repository and run the following command from the project root:

```sh
python3 -m pip install .
```

### Run the samples

See the [examples](./examples) directory for a list of sample orchestrations and instructions on how to run them.

**Enhanced Async Examples:**
- `async_activity_sequence.py` - Updated to use new `durabletask.aio` package
- `async_fanout_fanin.py` - Updated to use new `durabletask.aio` package  
- `async_enhanced_features.py` - Comprehensive demo of all enhanced features
- `async_non_determinism_demo.py` - Non-determinism detection demonstration
- See [ASYNC_ENHANCEMENTS.md](./durabletask/aio/ASYNCIO_ENHANCEMENTS.md) for detailed examples and usage patterns

## Development

The following is more information about how to develop this project. Note that development commands require that `make` is installed on your local machine. If you're using Windows, you can install `make` using [Chocolatey](https://chocolatey.org/) or use WSL.

### Generating protobufs

```sh
# install dev dependencies for generating protobufs and running tests
pip3 install '.[dev]'

make gen-proto
```

This will download the `orchestrator_service.proto` from the `microsoft/durabletask-protobuf` repo and compile it using `grpcio-tools`. The version of the source proto file that was downloaded can be found in the file `durabletask/internal/PROTO_SOURCE_COMMIT_HASH`.

### Running unit tests

Unit tests can be run using the following command from the project root. 
Unit tests _don't_ require a sidecar process to be running.

To run on a specific python version (eg: 3.11), run the following command from the project root:

```sh
tox -e py311
```

### Running E2E tests

The E2E (end-to-end) tests require a sidecar process to be running. 

For non-multi app activities test you can use the Durable Task test sidecar using the following command:

```sh
go install github.com/dapr/durabletask-go@main
durabletask-go --port 4001
```

Certain aspects like multi-app activities require the full dapr runtime to be running.

```shell
dapr init || true

dapr run --app-id test-app --dapr-grpc-port  4001 --components-path ./examples/components/
```

To run the E2E tests on a specific python version (eg: 3.11), run the following command from the project root:

```sh
tox -e py311-e2e
```

### Configuration

#### Connection Configuration

The SDK connects to a Durable Task sidecar. By default it uses `localhost:4001`. You can override via environment variables (checked in order):

- `DAPR_GRPC_ENDPOINT` - Full endpoint (e.g., `localhost:4001`, `grpcs://host:443`)
- `DAPR_GRPC_HOST` (or `DAPR_RUNTIME_HOST`) and `DAPR_GRPC_PORT` - Host and port separately

Example (common ports: 4001 for DurableTask-Go emulator, 50001 for Dapr sidecar):

```sh
export DAPR_GRPC_ENDPOINT=localhost:4001
# or
export DAPR_GRPC_HOST=localhost
export DAPR_GRPC_PORT=50001
```

#### GRPC Keepalive Configuration

Configure GRPC keepalive settings to maintain long-lived connections:

- `DAPR_GRPC_KEEPALIVE_ENABLED` - Enable keepalive (default: `false`)
- `DAPR_GRPC_KEEPALIVE_TIME_MS` - Keepalive time in milliseconds (default: `120000`)
- `DAPR_GRPC_KEEPALIVE_TIMEOUT_MS` - Keepalive timeout in milliseconds (default: `20000`)
- `DAPR_GRPC_KEEPALIVE_PERMIT_WITHOUT_CALLS` - Permit keepalive without active calls (default: `false`)

Example:

```sh
export DAPR_GRPC_KEEPALIVE_ENABLED=true
export DAPR_GRPC_KEEPALIVE_TIME_MS=60000
export DAPR_GRPC_KEEPALIVE_TIMEOUT_MS=10000
```

#### GRPC Retry Configuration

Configure automatic retry behavior for transient failures:

- `DAPR_GRPC_RETRY_ENABLED` - Enable automatic retries (default: `false`)
- `DAPR_GRPC_RETRY_MAX_ATTEMPTS` - Maximum retry attempts (default: `4`)
- `DAPR_GRPC_RETRY_INITIAL_BACKOFF_MS` - Initial backoff in milliseconds (default: `100`)
- `DAPR_GRPC_RETRY_MAX_BACKOFF_MS` - Maximum backoff in milliseconds (default: `1000`)
- `DAPR_GRPC_RETRY_BACKOFF_MULTIPLIER` - Backoff multiplier (default: `2.0`)
- `DAPR_GRPC_RETRY_CODES` - Comma-separated status codes to retry (default: `UNAVAILABLE,DEADLINE_EXCEEDED`)

Example:

```sh
export DAPR_GRPC_RETRY_ENABLED=true
export DAPR_GRPC_RETRY_MAX_ATTEMPTS=5
export DAPR_GRPC_RETRY_INITIAL_BACKOFF_MS=200
```

#### Async Workflow Configuration

Configure async workflow behavior and debugging:

- `DAPR_WF_DEBUG` or `DT_DEBUG` - Enable debug mode for workflows (set to `true`)
- `DAPR_WF_DISABLE_DETECTION` - Disable non-determinism detection (set to `true`)

Example:

```sh
export DAPR_WF_DEBUG=true
export DAPR_WF_DISABLE_DETECTION=false
```

### Async workflow authoring

For a deeper tour of the async authoring surface (determinism helpers, sandbox modes, timeouts, concurrency patterns), see the Async Enhancements guide: [ASYNC_ENHANCEMENTS.md](./ASYNC_ENHANCEMENTS.md). The developer-facing migration notes are in [DEVELOPER_TRANSITION_GUIDE.md](./DEVELOPER_TRANSITION_GUIDE.md).

You can author orchestrators with `async def` using the new `durabletask.aio` package, which provides a comprehensive async workflow API:

```python
from durabletask.worker import TaskHubGrpcWorker
from durabletask.aio import AsyncWorkflowContext

async def my_orch(ctx: AsyncWorkflowContext, input) -> str:
    r1 = await ctx.call_activity(act1, input=input)
    await ctx.sleep(1.0)
    r2 = await ctx.call_activity(act2, input=r1)
    return r2

with TaskHubGrpcWorker() as worker:
    worker.add_orchestrator(my_orch)
```

Optional sandbox mode (`best_effort` or `strict`) patches `asyncio.sleep`, `random`, `uuid.uuid4`, and `time.time` within the workflow step to deterministic equivalents. This is best-effort and not a correctness guarantee.

In `strict` mode, `asyncio.create_task` is blocked inside workflows to preserve determinism and will raise a `SandboxViolationError` if used.

> **Enhanced Sandbox Features**: The enhanced version includes comprehensive non-determinism detection, timeout support, enhanced concurrency primitives, and debugging tools. See [ASYNC_ENHANCEMENTS.md](./durabletask/aio/ASYNCIO_ENHANCEMENTS.md) for complete documentation.

#### Async patterns

- Activities and sub-orchestrations can be referenced by function object or by their registered string name. Both forms are supported:
- Function reference (preferred for IDE/type support) or string name (useful across modules/languages).

- Activities:
```python
result = await ctx.call_activity("process", input={"x": 1})
# or: result = await ctx.call_activity(process, input={"x": 1})
```

- Timers:
```python
await ctx.sleep(1.5)  # seconds or timedelta
```

- External events:
```python
val = await ctx.wait_for_external_event("approval")
```

- Concurrency:
```python
t1 = ctx.call_activity("a"); t2 = ctx.call_activity("b")
await ctx.when_all([t1, t2])
winner = await ctx.when_any([ctx.wait_for_external_event("x"), ctx.sleep(5)])

# gather combines awaitables and preserves order
results = await ctx.gather(t1, t2)
# gather with exception capture
results_or_errors = await ctx.gather(t1, t2, return_exceptions=True)
```

#### Async vs. generator API differences

- Async authoring (`durabletask.aio`): awaiting returns the operation's value. Exceptions are raised on `await` (no `is_failed`).
- Generator authoring (`durabletask.task`): yielding returns `Task` objects. Use `get_result()` to read values; failures surface via `is_failed()` or by raising on `get_result()`.

Examples:

```python
# Async authoring (await returns value)
# when_any returns a proxy that compares equal to the original awaitable
# and exposes get_result() for the completed item.
approval = ctx.wait_for_external_event("approval")
winner = await ctx.when_any([approval, ctx.sleep(60)])
if winner == approval:
    details = winner.get_result()
```

```python
# Async authoring (index + result)
idx, result = await ctx.when_any_with_result([approval, ctx.sleep(60)])
if idx == 0:  # approval won
    details = result
```

```python
# Generator authoring (yield returns Task)
approval = ctx.wait_for_external_event("approval")
winner = yield task.when_any([approval, ctx.create_timer(timedelta(seconds=60))])
if winner == approval:
    details = approval.get_result()
```

Failure handling in async:

```python
try:
    val = await ctx.call_activity("might_fail")
except Exception as e:
    # handle failure branch
    ...
```

Or capture with gather:

```python
res = await ctx.gather(ctx.call_activity("a"), return_exceptions=True)
if isinstance(res[0], Exception):
    ...
```

- Sub-orchestrations (function reference or registered name):
```python
out = await ctx.call_sub_orchestrator(child_fn, input=payload)
# or: out = await ctx.call_sub_orchestrator("child", input=payload)
```

- Deterministic utilities:
```python
now = ctx.now(); rid = ctx.random().random(); uid = ctx.uuid4()
```

- Workflow metadata and info:
```python
# Read-only info snapshot (Temporal-style convenience)
info = ctx.info
print(f"Workflow: {info.workflow_name}, Instance: {info.instance_id}")
print(f"Replaying: {info.is_replaying}, Suspended: {info.is_suspended}")

# Or access properties directly
instance_id = ctx.instance_id
is_replaying = ctx.is_replaying
is_suspended = ctx.is_suspended
workflow_name = ctx.workflow_name
parent_instance_id = ctx.parent_instance_id  # for sub-orchestrators

# Execution info (internal metadata if provided by sidecar)
exec_info = ctx.execution_info

# Tracing span IDs
span_id = ctx.orchestration_span_id  # or ctx.workflow_span_id (alias)
```

- Workflow metadata/headers (async only for now):
```python
# Attach contextual metadata (e.g., tracing, tenant, app info)
ctx.set_metadata({"x-trace": trace_id, "tenant": "acme"})
md = ctx.get_metadata()

# Header aliases (same data)
ctx.set_headers({"region": "us-east"})
headers = ctx.get_headers()
```
Notes:
- Useful for routing, observability, and cross-cutting concerns passed along activity/sub-orchestrator calls via the sidecar.
- In python-sdk, available for both async and generator orchestrators. In this repo, currently implemented on `durabletask.aio`; generator parity is planned.

- Cross-app activity/sub-orchestrator routing (async only for now):
```python
# Route activity to a different app via app_id
result = await ctx.call_activity("process", input=data, app_id="worker-app-2")

# Route sub-orchestrator to a different app
child_result = await ctx.call_sub_orchestrator("child_workflow", input=data, app_id="orchestrator-app-2")
```
Notes:
- The `app_id` parameter enables multi-app orchestrations where activities or child workflows run in different application instances.
- Requires sidecar support for cross-app invocation.

#### Worker readiness

When starting a worker and scheduling immediately, wait for the connection to the sidecar to be established:

```python
with TaskHubGrpcWorker() as worker:
    worker.add_orchestrator(my_orch)
    worker.start()
    worker.wait_for_ready(timeout=5)
    # Now safe to schedule
```

#### Suspension & termination

- `ctx.is_suspended` reflects suspension state during replay/processing.
- Suspend pauses progress without raising inside async orchestrators.
- Terminate completes with `TERMINATED` status; use client APIs to terminate/resume.
 - Only new events are buffered while suspended; replay events continue to apply to rebuild local state deterministically.

### Tracing and context propagation

The SDK surfaces W3C tracing context provided by the sidecar:

- Orchestrations: `ctx.trace_parent`, `ctx.trace_state`, and `ctx.orchestration_span_id` are available on `OrchestrationContext` (and on `AsyncWorkflowContext`).
- Activities: `ctx.trace_parent` and `ctx.trace_state` are available on `ActivityContext`.

Propagate tracing to external systems (e.g., HTTP):

```python
def activity(ctx, payload):
    headers = {
        "traceparent": ctx.trace_parent or "",
        "tracestate": ctx.trace_state or "",
    }
    # requests.post(url, headers=headers, json=payload)
    return "ok"
```

Notes:
- The sidecar controls inbound `traceparent`/`tracestate`. App code can append vendor entries to `tracestate` for outbound calls but cannot currently alter the sidecar’s propagation for downstream Durable operations.
- Configure the sidecar endpoint with `DURABLETASK_GRPC_ENDPOINT` (e.g., `127.0.0.1:56178`).

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
