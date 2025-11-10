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

from __future__ import annotations

from typing import Any, Generator, List

from durabletask import task
from durabletask.aio.awaitables import (
    AwaitableBase,
    SwallowExceptionAwaitable,
    WhenAllAwaitable,
    gather,
)


class _DummyAwaitable(AwaitableBase[Any]):
    """Minimal awaitable for testing that yields a trivial durable task."""

    __slots__ = ()

    def _to_task(self) -> task.Task[Any]:
        # Use when_all([]) to get a trivial durable Task instance
        return task.when_all([])


def _drive(awaitable: AwaitableBase[Any], send_value: Any) -> Any:
    """Drive an awaitable by manually advancing its __await__ generator.

    Returns the value completed by the awaitable when resuming with send_value.
    """
    gen: Generator[Any, Any, Any] = awaitable.__await__()
    try:
        next(gen)  # yield the durable task
    except StopIteration as stop:
        # completed synchronously
        return stop.value
    # Resume with a result from the runtime
    try:
        result = gen.send(send_value)
    except StopIteration as stop:
        return stop.value
    return result


def test_gather_empty_returns_immediately() -> None:
    wa = WhenAllAwaitable([])
    gen = wa.__await__()
    try:
        next(gen)
        assert False, "empty gather should complete without yielding"
    except StopIteration as stop:
        assert stop.value == []


def test_gather_order_preservation() -> None:
    a1 = _DummyAwaitable()
    a2 = _DummyAwaitable()
    wa = WhenAllAwaitable([a1, a2])
    # Drive and inject two results in order
    result = _drive(wa, ["r1", "r2"])  # runtime returns list in order
    assert result == ["r1", "r2"]


def test_gather_multi_await_caching() -> None:
    a1 = _DummyAwaitable()
    wa = WhenAllAwaitable([a1])
    # First await drives and caches
    first = _drive(wa, ["ok"])  # runtime returns ["ok"]
    assert first == ["ok"]
    # Second await should not yield again; completes immediately with cached value
    gen2 = wa.__await__()
    try:
        next(gen2)
        assert False, "cached gather should not yield again"
    except StopIteration as stop:
        assert stop.value == ["ok"]


def test_gather_return_exceptions_wraps_children() -> None:
    a1 = _DummyAwaitable()
    a2 = _DummyAwaitable()
    wa = gather(a1, a2, return_exceptions=True)
    # The underlying tasks_like should be SwallowExceptionAwaitable instances
    assert isinstance(wa, WhenAllAwaitable)
    # Access internal for type check
    wrapped: List[Any] = wa._tasks_like  # type: ignore[attr-defined]
    assert all(isinstance(w, SwallowExceptionAwaitable) for w in wrapped)
