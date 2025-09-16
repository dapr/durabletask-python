# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

from __future__ import annotations

import contextlib
import hashlib
import time as _time
import uuid as _uuid
from datetime import datetime, timedelta
from typing import (
    Any,
    Awaitable,
    Callable,
    Generator,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)

from durabletask import task as dt_task

TInput = TypeVar("TInput")
TOutput = TypeVar("TOutput")


class AsyncWorkflowContext:
    """Async authoring surface that maps to the underlying Durable Task context.

    This context exposes deterministic async helpers that translate to Durable Task
    operations, enabling authoring orchestrators using async/await while preserving
    determinism and replay semantics.
    """

    def __init__(self, base_ctx: dt_task.OrchestrationContext):
        self._base_ctx = base_ctx
        self._rng = None  # lazy-initialized deterministic RNG

    # Activities & Sub-orchestrations
    def activity(
        self,
        name: Union[dt_task.Activity, str],
        *,
        input: Optional[Any] = None,
        retry_policy: Optional[dt_task.RetryPolicy] = None,
    ) -> "_ActivityAwaitable":
        return _ActivityAwaitable(
            self._base_ctx, name, input=input, retry_policy=retry_policy
        )

    def sub_orchestrator(
        self,
        orchestrator: dt_task.Orchestrator[TInput, TOutput],
        *,
        input: Optional[Any] = None,
        instance_id: Optional[str] = None,
        retry_policy: Optional[dt_task.RetryPolicy] = None,
    ) -> "_SubOrchestratorAwaitable[TOutput]":
        return _SubOrchestratorAwaitable(
            self._base_ctx,
            orchestrator,
            input=input,
            instance_id=instance_id,
            retry_policy=retry_policy,
        )

    # Timers & Events
    def sleep(self, duration: Union[float, timedelta, datetime]) -> "_SleepAwaitable":
        return _SleepAwaitable(self._base_ctx, duration)

    def wait_for_external_event(self, name: str) -> "_ExternalEventAwaitable":
        return _ExternalEventAwaitable(self._base_ctx, name)

    # Concurrency
    def when_all(
        self, awaitables: Sequence["_AwaitableBase[Any]"]
    ) -> "_WhenAllAwaitable":
        return _WhenAllAwaitable(awaitables)

    def when_any(
        self, awaitables: Sequence["_AwaitableBase[Any]"]
    ) -> "_WhenAnyAwaitable":
        return _WhenAnyAwaitable(awaitables)

    # Gather (asyncio-like)
    def gather(
        self, *aws: "_AwaitableBase[Any]", return_exceptions: bool = False
    ) -> "_WhenAllAwaitable":
        if return_exceptions:
            wrapped: list[_AwaitableBase[Any]] = [
                _SwallowExceptionAwaitable(a) for a in aws
            ]
            return _WhenAllAwaitable(wrapped)
        return _WhenAllAwaitable(list(aws))

    # Deterministic utilities
    def now(self) -> datetime:
        return self._base_ctx.current_utc_datetime

    def random(self):
        import random as _random

        if self._rng is None:
            # Use a deterministic seed derived from instance_id
            # Avoid Python's built-in hash() salt; use SHA-256 for stable results
            seed_bytes = hashlib.sha256(
                self._base_ctx.instance_id.encode("utf-8")
            ).digest()[:8]
            seed = int.from_bytes(seed_bytes, byteorder="big", signed=False)
            self._rng = _random.Random(seed)
        return self._rng

    @property
    def is_suspended(self) -> bool:
        return getattr(self._base_ctx, "is_suspended", False)

    @property
    def is_replaying(self) -> bool:
        return getattr(self._base_ctx, "is_replaying", False)

    @property
    def instance_id(self) -> str:
        return getattr(self._base_ctx, "instance_id")

    @property
    def workflow_name(self) -> str:
        return getattr(self._base_ctx, "workflow_name", "")

    @property
    def parent_instance_id(self) -> Optional[str]:
        return getattr(self._base_ctx, "parent_instance_id", None)

    @property
    def history_event_sequence(self) -> Optional[int]:
        return getattr(self._base_ctx, "history_event_sequence", None)

    @property
    def trace_parent(self) -> Optional[str]:
        return getattr(self._base_ctx, "trace_parent", None)

    @property
    def trace_state(self) -> Optional[str]:
        return getattr(self._base_ctx, "trace_state", None)

    @property
    def orchestration_span_id(self) -> Optional[str]:
        return getattr(self._base_ctx, "orchestration_span_id", None)

    def set_custom_status(self, status: Any) -> None:
        self._base_ctx.set_custom_status(status)

    def continue_as_new(self, new_input: Any, *, save_events: bool = False) -> None:
        self._base_ctx.continue_as_new(new_input, save_events=save_events)

    def uuid4(self) -> _uuid.UUID:
        rng = self.random()
        # Generate 128 random bits deterministically
        rand_int = rng.getrandbits(128)
        # Conform to UUIDv4 variant/version bits
        rand_int &= ~(0xF << 76)
        rand_int |= 4 << 76  # version 4
        rand_int &= ~(0x3 << 62)
        rand_int |= 0x2 << 62  # variant 10
        return _uuid.UUID(int=rand_int)


class _AwaitableBase(Awaitable[TOutput]):
    def _to_durable_task(self) -> dt_task.Task[TOutput]:
        raise NotImplementedError()

    def __await__(self):  # type: ignore[override]
        result = yield self._to_durable_task()
        return result


class _ActivityAwaitable(_AwaitableBase[TOutput]):
    def __init__(
        self,
        ctx: dt_task.OrchestrationContext,
        name: Union[dt_task.Activity, str],
        *,
        input: Optional[Any] = None,
        retry_policy: Optional[dt_task.RetryPolicy] = None,
    ):
        self._ctx = ctx
        self._name = name
        self._input = input
        self._retry_policy = retry_policy

    def _to_durable_task(self) -> dt_task.Task[TOutput]:
        return self._ctx.call_activity(
            self._name, input=self._input, retry_policy=self._retry_policy
        )


class _SubOrchestratorAwaitable(_AwaitableBase[TOutput]):
    def __init__(
        self,
        ctx: dt_task.OrchestrationContext,
        orchestrator: dt_task.Orchestrator[TInput, TOutput],
        *,
        input: Optional[Any] = None,
        instance_id: Optional[str] = None,
        retry_policy: Optional[dt_task.RetryPolicy] = None,
    ):
        self._ctx = ctx
        self._orchestrator = orchestrator
        self._input = input
        self._instance_id = instance_id
        self._retry_policy = retry_policy

    def _to_durable_task(self) -> dt_task.Task[TOutput]:
        return self._ctx.call_sub_orchestrator(
            self._orchestrator,
            input=self._input,
            instance_id=self._instance_id,
            retry_policy=self._retry_policy,
        )


class _SleepAwaitable(_AwaitableBase[None]):
    def __init__(
        self,
        ctx: dt_task.OrchestrationContext,
        duration: Union[float, timedelta, datetime],
    ):
        self._ctx = ctx
        self._duration = duration

    def _to_durable_task(self) -> dt_task.Task[None]:
        fire_at: Union[datetime, timedelta]
        if isinstance(self._duration, (int, float)):
            fire_at = timedelta(seconds=float(self._duration))
        else:
            fire_at = self._duration
        return self._ctx.create_timer(fire_at)


class _ExternalEventAwaitable(_AwaitableBase[TOutput]):
    def __init__(self, ctx: dt_task.OrchestrationContext, name: str):
        self._ctx = ctx
        self._name = name

    def _to_durable_task(self) -> dt_task.Task[TOutput]:
        return self._ctx.wait_for_external_event(self._name)


class _WhenAllAwaitable(_AwaitableBase[list[TOutput]]):
    def __init__(self, awaitables: Sequence[_AwaitableBase[TOutput]]):
        self._awaitables = list(awaitables)

    def _to_durable_task(self) -> dt_task.Task[list[TOutput]]:
        child_tasks = [a._to_durable_task() for a in self._awaitables]
        return dt_task.when_all(child_tasks)


class _WhenAnyAwaitable(_AwaitableBase[dt_task.Task]):
    def __init__(self, awaitables: Sequence[_AwaitableBase[Any]]):
        self._awaitables = list(awaitables)

    def _to_durable_task(self) -> dt_task.Task[dt_task.Task]:
        child_tasks = [a._to_durable_task() for a in self._awaitables]
        return dt_task.when_any(child_tasks)


class _SwallowExceptionAwaitable(_AwaitableBase[Any]):
    """Wrapper awaitable that returns exception object instead of raising it."""

    def __init__(self, inner: _AwaitableBase[Any]):
        self._inner = inner

    def _to_durable_task(self) -> dt_task.Task[Any]:
        # Delegate to inner to schedule its task
        return self._inner._to_durable_task()

    def __await__(self):  # type: ignore[override]
        try:
            result = yield self._to_durable_task()
            return result
        except Exception as e:
            # Return the exception object instead of propagating
            return e


class CoroutineOrchestratorRunner:
    """Driver that bridges an async orchestrator to a generator orchestrator."""

    def __init__(
        self,
        async_orchestrator: Callable[[AsyncWorkflowContext, Any], Awaitable[Any]],
        *,
        sandbox_mode: str = "off",
    ) -> None:
        self._async_orchestrator = async_orchestrator
        self._sandbox_mode = sandbox_mode

    def to_generator(
        self, async_ctx: AsyncWorkflowContext, input_data: Any
    ) -> Generator[dt_task.Task, Any, Any]:
        coro = cast(Any, self._async_orchestrator(async_ctx, input_data))

        def to_iter(obj):
            if hasattr(obj, "__await__"):
                return obj.__await__()
            if isinstance(obj, dt_task.Task):
                # Wrap a single Task into a one-shot awaitable iterator
                def _one_shot():
                    res = yield obj
                    return res

                return _one_shot()
            raise TypeError("Async orchestrator awaited unsupported object type")

        def driver_gen():
            # Prime the coroutine to first await point
            try:
                with _compat_sandbox(async_ctx, self._sandbox_mode):
                    awaited_obj = coro.send(None)
            except StopIteration as stop:
                return stop.value
            except BaseException as cancel_exc:
                # If coroutine raised on first send (e.g., cancellation), surface as orchestration failure
                raise cancel_exc

            awaited_iter = to_iter(awaited_obj)
            while True:
                # Advance the awaitable to a DT Task to yield
                try:
                    request = awaited_iter.send(None)
                except StopIteration as stop_await:
                    # Awaitable finished synchronously; feed result back to coroutine
                    try:
                        with _compat_sandbox(async_ctx, self._sandbox_mode):
                            awaited_obj = coro.send(stop_await.value)
                    except StopIteration as stop:
                        return stop.value
                    except BaseException as cancel_exc:
                        raise cancel_exc
                    awaited_iter = to_iter(awaited_obj)
                    continue

                if not isinstance(request, dt_task.Task):
                    raise TypeError("Async awaitable yielded a non-Task object")

                # Yield to runtime and resume awaitable with task result
                try:
                    result = yield request
                except Exception as e:
                    # Route exception into awaitable first; if it completes, continue; otherwise forward to coroutine
                    try:
                        awaited_iter.throw(e)
                    except StopIteration as stop_await:
                        try:
                            with _compat_sandbox(async_ctx, self._sandbox_mode):
                                awaited_obj = coro.send(stop_await.value)
                        except StopIteration as stop:
                            return stop.value
                        except BaseException as cancel_exc:
                            raise cancel_exc
                        awaited_iter = to_iter(awaited_obj)
                    except Exception as exc:
                        try:
                            with _compat_sandbox(async_ctx, self._sandbox_mode):
                                awaited_obj = coro.throw(exc)
                        except StopIteration as stop:
                            return stop.value
                        except BaseException as cancel_exc:
                            raise cancel_exc
                        awaited_iter = to_iter(awaited_obj)
                    continue

                # Success: feed result to awaitable; it may yield more tasks until it stops
                try:
                    next_req = awaited_iter.send(result)
                    while True:
                        if not isinstance(next_req, dt_task.Task):
                            raise TypeError("Async awaitable yielded a non-Task object")
                        result = yield next_req
                        next_req = awaited_iter.send(result)
                except StopIteration as stop_await:
                    try:
                        with _compat_sandbox(async_ctx, self._sandbox_mode):
                            awaited_obj = coro.send(stop_await.value)
                    except StopIteration as stop:
                        return stop.value
                    except BaseException as cancel_exc:
                        raise cancel_exc
                    awaited_iter = to_iter(awaited_obj)

        return driver_gen()


@contextlib.contextmanager
def _compat_sandbox(async_ctx: AsyncWorkflowContext, mode: str):
    """Best-effort, opt-in compatibility patching.

    Patches asyncio.sleep, random, uuid, and time to deterministic equivalents
    for the duration of a single step into the coroutine. No global patching by default.
    """
    if mode not in ("off", "best_effort", "strict"):
        mode = "off"
    if mode == "off":
        yield
        return

    import asyncio as _asyncio
    import random as _random
    import time as _time_mod
    import uuid as _uuid_mod

    originals = {
        "asyncio.sleep": _asyncio.sleep,
        "asyncio.create_task": getattr(_asyncio, "create_task", None),
        "random.random": _random.random,
        "random.randrange": _random.randrange,
        "uuid.uuid4": _uuid_mod.uuid4,
        "time.time": _time_mod.time,
        "time.time_ns": getattr(_time_mod, "time_ns", None),
    }

    def patched_sleep(delay: Union[float, int]):
        class _PatchedSleepAwaitable:
            def __await__(self):
                result = yield async_ctx._base_ctx.create_timer(
                    timedelta(seconds=float(delay))
                )
                return result

        return _PatchedSleepAwaitable()

    rng = async_ctx.random()

    def patched_random():
        return rng.random()

    def patched_randrange(start, stop=None, step=1, _int=int):
        # Simplistic deterministic randrange using rng
        if stop is None:
            start, stop = 0, start
        width = stop - start
        if step == 1 and width > 0:
            return start + _int(rng.random() * width)
        # Fallback: generate until fits
        while True:
            n = start + _int(rng.random() * width)
            if (n - start) % step == 0:
                return n

    def patched_uuid4():
        return async_ctx.uuid4()

    def patched_time():
        dt = async_ctx.now()
        return dt.timestamp()

    def patched_time_ns():
        dt = async_ctx.now()
        return _time.mktime(dt.timetuple()) * 1_000_000_000 + dt.microsecond * 1000

    # Apply patches
    _asyncio.sleep = patched_sleep  # type: ignore
    if mode == "strict" and hasattr(_asyncio, "create_task"):

        def _blocked_create_task(*args, **kwargs):  # noqa: N802
            raise RuntimeError(
                "asyncio.create_task is not allowed in workflows (strict mode)"
            )

        _asyncio.create_task = _blocked_create_task  # type: ignore
    _random.random = patched_random  # type: ignore
    _random.randrange = patched_randrange  # type: ignore
    _uuid_mod.uuid4 = patched_uuid4  # type: ignore
    _time_mod.time = patched_time  # type: ignore
    if originals["time.time_ns"] is not None:
        _time_mod.time_ns = patched_time_ns  # type: ignore

    try:
        yield
    finally:
        # Restore originals
        import asyncio as _asyncio2
        import random as _random2
        import time as _time2
        import uuid as _uuid2

        _asyncio2.sleep = originals["asyncio.sleep"]  # type: ignore
        if originals["asyncio.create_task"] is not None:
            _asyncio2.create_task = originals["asyncio.create_task"]  # type: ignore
        _random2.random = originals["random.random"]  # type: ignore
        _random2.randrange = originals["random.randrange"]  # type: ignore
        _uuid2.uuid4 = originals["uuid.uuid4"]  # type: ignore
        _time2.time = originals["time.time"]  # type: ignore
        if originals["time.time_ns"] is not None:
            _time2.time_ns = originals["time.time_ns"]  # type: ignore

        _asyncio2.sleep = originals["asyncio.sleep"]  # type: ignore
        _random2.random = originals["random.random"]  # type: ignore
        _random2.randrange = originals["random.randrange"]  # type: ignore
        _uuid2.uuid4 = originals["uuid.uuid4"]  # type: ignore
        _time2.time = originals["time.time"]  # type: ignore
        if originals["time.time_ns"] is not None:
            _time2.time_ns = originals["time.time_ns"]  # type: ignore
