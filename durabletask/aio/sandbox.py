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
Sandbox for deterministic workflow execution.

This module provides sandboxing capabilities that patch non-deterministic
Python functions with deterministic alternatives during workflow execution.
It also includes non-determinism detection to help developers identify
problematic code patterns.
"""

from __future__ import annotations

import contextlib
import os
import sys
import warnings
from contextlib import ContextDecorator
from datetime import timedelta
from enum import Enum
from types import FrameType
from typing import Any, Callable, Dict, Optional, Set, Type, Union, cast

from durabletask.deterministic import deterministic_random, deterministic_uuid4

from .errors import NonDeterminismWarning, SandboxViolationError

# Capture environment variable at module load to avoid triggering non-determinism detection
_DISABLE_DETECTION = os.getenv("DAPR_WF_DISABLE_DETERMINISTIC_DETECTION") == "true"


class SandboxMode(str, Enum):
    """Sandbox mode options.

    Use as an alternative to string literals to avoid typos and enable IDE support.
    """

    OFF = "off"
    BEST_EFFORT = "best_effort"
    STRICT = "strict"

    @classmethod
    def from_string(cls, mode: str) -> SandboxMode:
        if mode not in cls.__members__.values():
            raise ValueError(
                f"Invalid sandbox mode: {mode}. Must be one of: {cls.__members__.values()}."
            )
        return cls(mode)


class _NonDeterminismDetector:
    """Detects and warns about non-deterministic function calls in workflows."""

    def __init__(self, async_ctx: Any, mode: Union[str, SandboxMode]):
        self.async_ctx = async_ctx
        self.mode = SandboxMode.from_string(mode)
        self.detected_calls: Set[str] = set()
        self.original_trace_func: Optional[Callable[[FrameType, str, Any], Any]] = None
        self._restore_trace_func: Optional[Callable[[FrameType, str, Any], Any]] = None
        self._active_trace_func: Optional[Callable[[FrameType, str, Any], Any]] = None

    def _noop_trace(
        self, frame: FrameType, event: str, arg: Any
    ) -> Optional[Callable[[FrameType, str, Any], Any]]:  # lightweight tracer
        return None

    def __enter__(self) -> "_NonDeterminismDetector":
        enable_full_detection = self.mode == "strict" or (
            self.mode == "best_effort" and getattr(self.async_ctx, "_debug_mode", False)
        )
        if self.mode in ("best_effort", "strict"):
            self.original_trace_func = sys.gettrace()
            # Use full detection tracer in strict or when debug mode is enabled
            self._active_trace_func = (
                self._trace_calls if enable_full_detection else self._noop_trace
            )
            sys.settrace(self._active_trace_func)
            self._restore_trace_func = sys.gettrace()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        if self.mode in ("best_effort", "strict"):
            # Restore to the trace function that was active before __enter__
            sys.settrace(self.original_trace_func)

    def _trace_calls(
        self, frame: FrameType, event: str, arg: Any
    ) -> Optional[Callable[[FrameType, str, Any], Any]]:
        """Trace function calls to detect non-deterministic operations."""
        # Only handle function call events to minimize overhead
        if event != "call":
            return self.original_trace_func(frame, event, arg) if self.original_trace_func else None

        # Perform best-effort detection on call sites
        self._check_frame_for_non_determinism(frame)

        # Do not install a per-frame local tracer; let original (if any) handle further events
        return self.original_trace_func if self.original_trace_func else None

    def _check_frame_for_non_determinism(self, frame: FrameType) -> None:
        """Check if the current frame contains non-deterministic function calls."""
        code = frame.f_code
        filename = code.co_filename
        func_name = code.co_name

        # Fast module/function check via globals to reduce overhead
        try:
            module_name = frame.f_globals.get("__name__", "")
        except Exception:
            module_name = ""

        if module_name:
            fast_map = {
                "datetime": {"now", "utcnow"},
                "time": {"time", "time_ns"},
                "random": {"random", "randint", "choice", "shuffle"},
                "uuid": {"uuid1", "uuid4"},
                "os": {"urandom", "getenv"},
                "secrets": {"token_bytes", "token_hex", "choice"},
                "socket": {"gethostname"},
                "platform": {"node"},
                "threading": {"current_thread"},
            }
            funcs = fast_map.get(module_name)
            if funcs and func_name in funcs:
                # Whitelist deterministic RNG method calls bound to our patched RNG instance
                if module_name == "random" and func_name in {
                    "random",
                    "randint",
                    "choice",
                    "shuffle",
                }:
                    try:
                        bound_self = frame.f_locals.get("self")
                        if getattr(bound_self, "_dt_deterministic", False):
                            return
                    except Exception:
                        pass
                self._handle_non_deterministic_call(f"{module_name}.{func_name}", frame)
                if self.mode == "best_effort":
                    return

        # Skip our own code and system modules
        if "durabletask" in filename or filename.startswith("<"):
            return

        # Check for problematic function calls
        non_deterministic_patterns = [
            ("datetime", "now"),
            ("datetime", "utcnow"),
            ("time", "time"),
            ("time", "time_ns"),
            ("random", "random"),
            ("random", "randint"),
            ("random", "choice"),
            ("random", "shuffle"),
            ("uuid", "uuid1"),
            ("uuid", "uuid4"),
            ("os", "urandom"),
            ("os", "getenv"),
            ("secrets", "token_bytes"),
            ("secrets", "token_hex"),
            ("secrets", "choice"),
            ("socket", "gethostname"),
            ("platform", "node"),
            ("threading", "current_thread"),
        ]

        if self.mode != "best_effort":
            # Check local variables for module usage
            for var_name, var_value in frame.f_locals.items():
                module_name = getattr(var_value, "__module__", None)
                if module_name:
                    for pattern_module, pattern_func in non_deterministic_patterns:
                        if (
                            pattern_module in module_name
                            and hasattr(var_value, pattern_func)
                            and func_name == pattern_func
                        ):
                            self._handle_non_deterministic_call(
                                f"{pattern_module}.{pattern_func}", frame
                            )

            # Check for direct function calls in globals (guard against non-mapping f_globals)
            try:
                globals_map = frame.f_globals
            except Exception:
                globals_map = {}
            for pattern_module, pattern_func in non_deterministic_patterns:
                full_name = f"{pattern_module}.{pattern_func}"
                try:
                    if (
                        isinstance(globals_map, dict)
                        and full_name in globals_map
                        and func_name == pattern_func
                    ):
                        self._handle_non_deterministic_call(full_name, frame)
                except Exception:
                    continue

    def _handle_non_deterministic_call(self, function_name: str, frame: FrameType) -> None:
        """Handle detection of a non-deterministic function call."""
        if function_name in self.detected_calls:
            return  # Already reported

        self.detected_calls.add(function_name)

        # Get context information
        code = frame.f_code
        filename = code.co_filename
        lineno = frame.f_lineno
        func = code.co_name

        # Create detailed message with suggestions
        suggestions = {
            "datetime.now": "ctx.now()",
            "datetime.utcnow": "ctx.now()",
            "time.time": "ctx.now().timestamp()",
            "time.time_ns": "int(ctx.now().timestamp() * 1_000_000_000)",
            "random.random": "ctx.random().random()",
            "random.randint": "ctx.random().randint()",
            "random.choice": "ctx.random().choice()",
            "uuid.uuid4": "ctx.uuid4()",
            "os.urandom": "ctx.random().randbytes()",
            "secrets.token_bytes": "ctx.random().randbytes()",
            "secrets.token_hex": "ctx.random_string()",
        }

        suggestion = suggestions.get(function_name, "a deterministic alternative")
        message = (
            f"Non-deterministic function '{function_name}' detected at {filename}:{lineno} "
            f"(in {func}). Consider using {suggestion} instead."
        )

        # Log debug information if enabled
        if hasattr(self.async_ctx, "_debug_mode") and self.async_ctx._debug_mode:
            print(f"[WORKFLOW DEBUG] {message}")

        if self.mode == "strict":
            raise SandboxViolationError(
                f"Non-deterministic function '{function_name}' is not allowed in strict mode",
                violation_type="non_deterministic_call",
                suggested_alternative=suggestion,
                workflow_name=getattr(self.async_ctx, "_workflow_name", None),
                instance_id=getattr(self.async_ctx, "instance_id", None),
            )
        elif self.mode == "best_effort":
            # Warn only once per function and do not escalate to error in best_effort
            warnings.warn(message, NonDeterminismWarning, stacklevel=3)

    def _get_deterministic_alternative(self, function_name: str) -> str:
        """Get deterministic alternative suggestion for a function."""
        suggestions = {
            "datetime.now": "ctx.now()",
            "datetime.utcnow": "ctx.now()",
            "time.time": "ctx.now().timestamp()",
            "time.time_ns": "int(ctx.now().timestamp() * 1_000_000_000)",
            "random.random": "ctx.random().random()",
            "random.randint": "ctx.random().randint()",
            "random.choice": "ctx.random().choice()",
            "random.shuffle": "ctx.random().shuffle()",
            "uuid.uuid1": "ctx.uuid4() (deterministic)",
            "uuid.uuid4": "ctx.uuid4()",
            "os.urandom": "ctx.random().randbytes() or ctx.random().getrandbits()",
            "secrets.token_bytes": "ctx.random().randbytes()",
            "secrets.token_hex": "ctx.random().randbytes().hex()",
            "socket.gethostname": "hardcoded hostname or activity call",
            "threading.current_thread": "avoid threading in workflows",
        }
        return suggestions.get(function_name, "a deterministic alternative")


class _Sandbox(ContextDecorator):
    """Context manager for sandboxing workflow execution."""

    def __init__(self, async_ctx: Any, mode: Union[str, SandboxMode]):
        self.async_ctx = async_ctx
        self.mode = SandboxMode.from_string(mode)
        self.originals: Dict[str, Any] = {}
        self.detector: Optional[_NonDeterminismDetector] = None

    def __enter__(self) -> "_Sandbox":
        if self.mode == SandboxMode.OFF:
            return self

        # Check for global disable
        if getattr(self.async_ctx, "_detection_disabled", False):
            return self

        # Enable non-determinism detection
        self.detector = _NonDeterminismDetector(self.async_ctx, self.mode)
        self.detector.__enter__()

        # Apply patches for best_effort and strict modes
        self._apply_patches()

        # Expose originals/mode to the async workflow context for controlled unsafe access
        try:
            self.async_ctx._sandbox_originals = dict(self.originals)
            self.async_ctx._sandbox_mode = self.mode
        except Exception:
            # Context may not support attribute assignment; ignore
            pass

        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        if self.detector:
            self.detector.__exit__(exc_type, exc_val, exc_tb)

        if self.mode != SandboxMode.OFF and self.originals:
            self._restore_originals()

        # Remove exposed references from the async context
        try:
            if hasattr(self.async_ctx, "_sandbox_originals"):
                delattr(self.async_ctx, "_sandbox_originals")
            if hasattr(self.async_ctx, "_sandbox_mode"):
                delattr(self.async_ctx, "_sandbox_mode")
        except Exception:
            pass

    def _apply_patches(self) -> None:
        """Apply patches to non-deterministic functions."""
        import asyncio as _asyncio
        import datetime as _datetime
        import random as _random
        import time as _time_mod
        import uuid as _uuid_mod

        # Store originals for restoration
        self.originals = {
            "asyncio.sleep": _asyncio.sleep,
            "asyncio.gather": getattr(_asyncio, "gather", None),
            "asyncio.create_task": getattr(_asyncio, "create_task", None),
            "random.random": _random.random,
            "random.randrange": _random.randrange,
            "random.randint": _random.randint,
            "random.getrandbits": _random.getrandbits,
            "uuid.uuid4": _uuid_mod.uuid4,
            "time.time": _time_mod.time,
            "time.time_ns": getattr(_time_mod, "time_ns", None),
            "datetime.now": _datetime.datetime.now,
            "datetime.utcnow": _datetime.datetime.utcnow,
        }

        # Add strict mode blocks for potentially dangerous operations
        if self.mode == SandboxMode.STRICT:
            import builtins
            import os as _os
            import secrets as _secrets

            self.originals.update(
                {
                    "builtins.open": builtins.open,
                    "os.urandom": getattr(_os, "urandom", None),
                    "secrets.token_bytes": getattr(_secrets, "token_bytes", None),
                    "secrets.token_hex": getattr(_secrets, "token_hex", None),
                }
            )

        # Create patched functions
        def patched_sleep(delay: Union[float, int]) -> Any:
            # Capture the context in the closure
            base_ctx = self.async_ctx._base_ctx

            class _PatchedSleepAwaitable:
                def __await__(self) -> Any:
                    result = yield base_ctx.create_timer(timedelta(seconds=float(delay)))
                    return result

            # Pass through zero-or-negative delays to the original asyncio.sleep
            try:
                if float(delay) <= 0:
                    orig_sleep = self.originals.get("asyncio.sleep")
                    if orig_sleep is not None:
                        return orig_sleep(0)  # return the original coroutine
            except Exception:
                pass

            return _PatchedSleepAwaitable()

        # Derive RNG from instance/time to make results deterministic per-context
        # Fallbacks ensure this works with plain mocks used in tests
        iid = getattr(self.async_ctx, "instance_id", None)
        if iid is None:
            base = getattr(self.async_ctx, "_base_ctx", None)
            iid = getattr(base, "instance_id", "") if base is not None else ""
        now_dt = None
        if hasattr(self.async_ctx, "now"):
            try:
                now_dt = self.async_ctx.now()
            except Exception:
                now_dt = None
        if now_dt is None:
            if hasattr(self.async_ctx, "current_utc_datetime"):
                now_dt = self.async_ctx.current_utc_datetime
            else:
                base = getattr(self.async_ctx, "_base_ctx", None)
                now_dt = getattr(base, "current_utc_datetime", None) if base is not None else None
        if now_dt is None:
            now_dt = _datetime.datetime.fromtimestamp(0, _datetime.timezone.utc)
        rng = deterministic_random(iid or "", now_dt)
        # Mark as deterministic so the detector can whitelist bound method calls
        try:
            rng._dt_deterministic = True
        except Exception:
            pass

        def patched_random() -> float:
            return rng.random()

        def patched_randrange(
            start: int,
            stop: Optional[int] = None,
            step: int = 1,
            _int: Callable[[float], int] = int,
        ) -> int:
            # Deterministic randrange using rng
            if stop is None:
                start, stop = 0, start
            assert stop is not None
            width = stop - start
            if step == 1 and width > 0:
                return start + _int(rng.random() * width)
            # Fallback: generate until fits
            while True:
                n = start + _int(rng.random() * width)
                if (n - start) % step == 0:
                    return n

        def patched_getrandbits(k: int) -> int:
            return rng.getrandbits(k)

        def patched_randint(a: int, b: int) -> int:
            return rng.randint(a, b)

        def patched_uuid4() -> Any:
            return deterministic_uuid4(rng)

        def patched_time() -> float:
            dt = self.async_ctx.now()
            return float(dt.timestamp())

        def patched_time_ns() -> int:
            dt = self.async_ctx.now()
            return int(dt.timestamp() * 1_000_000_000)

        def patched_datetime_now(tz: Optional[Any] = None) -> Any:
            base_dt = self.async_ctx.now()
            return base_dt.replace(tzinfo=tz) if tz else base_dt

        def patched_datetime_utcnow() -> Any:
            return self.async_ctx.now()

        # Apply patches - only patch local imports to maintain context isolation
        _asyncio.sleep = cast(Any, patched_sleep)

        # Patch asyncio.gather to a replay-safe, one-shot awaitable wrapper
        def _is_workflow_awaitable(obj: Any) -> bool:
            try:
                from .awaitables import AwaitableBase as _AwaitableBase  # local import

                if isinstance(obj, _AwaitableBase):
                    return True
            except Exception:
                pass
            try:
                from durabletask import task as _dt

                if isinstance(obj, _dt.Task):
                    return True
            except Exception:
                pass
            return False

        class _OneShot:
            """Replay-safe one-shot awaitable wrapper.

            Schedules the underlying coroutine/factory exactly once at the
            first await, caches either the result or the exception, and on
            subsequent awaits simply replays the cached outcome without
            re-scheduling any work. This prevents side effects during
            orchestrator replays and makes multiple awaits deterministic.
            """

            def __init__(self, factory: Callable[[], Any]) -> None:
                self._factory = factory
                self._done = False
                self._res: Any = None
                self._exc: Optional[BaseException] = None

            def __await__(self) -> Any:
                if self._done:

                    async def _replay() -> Any:
                        if self._exc is not None:
                            raise self._exc
                        return self._res

                    return _replay().__await__()

                async def _compute() -> Any:
                    try:
                        out = await self._factory()
                        self._res = out
                        self._done = True
                        return out
                    except BaseException as e:  # noqa: BLE001
                        self._exc = e
                        self._done = True
                        raise

                return _compute().__await__()

        def _patched_gather(*aws: Any, return_exceptions: bool = False) -> Any:
            """Replay-safe gather that returns a one-shot awaitable.

            - Empty input returns a cached empty list.
            - If all inputs are workflow awaitables, uses WhenAllAwaitable (fan-out)
              and caches the combined result.
            - Mixed inputs: workflow awaitables are batched via WhenAll (fan-out), then
              native awaitables are awaited sequentially; results are merged in the
              original order. return_exceptions is honored for both groups.

            The returned object can be awaited multiple times safely without
            re-scheduling underlying operations.
            """
            # Empty gather returns [] and can be awaited multiple times safely
            if not aws:

                async def _empty() -> list[Any]:
                    return []

                return _OneShot(_empty)

            # If all awaitables are workflow awaitables or durable tasks, map to when_all (fan-out best scenario)
            if all(_is_workflow_awaitable(a) for a in aws):

                async def _await_when_all() -> Any:
                    from .awaitables import WhenAllAwaitable  # local import to avoid cycles

                    combined: Any = WhenAllAwaitable(list(aws))
                    return await combined

                return _OneShot(_await_when_all)

            # Mixed inputs: fan-out workflow awaitables via WhenAll, then await native sequentially; merge preserving order
            async def _run_mixed() -> list[Any]:
                from .awaitables import AwaitableBase as _AwaitableBase
                from .awaitables import SwallowExceptionAwaitable, WhenAllAwaitable

                items: list[Any] = list(aws)
                total = len(items)
                # Partition into workflow vs native
                wf_indices: list[int] = []
                wf_items: list[Any] = []
                native_indices: list[int] = []
                native_items: list[Any] = []
                for idx, it in enumerate(items):
                    if _is_workflow_awaitable(it):
                        wf_indices.append(idx)
                        wf_items.append(it)
                    else:
                        native_indices.append(idx)
                        native_items.append(it)
                merged: list[Any] = [None] * total
                # Fan-out workflow group first (optionally swallow exceptions for AwaitableBase entries)
                if wf_items:
                    wf_group: list[Any] = []
                    if return_exceptions:
                        for it in wf_items:
                            if isinstance(it, _AwaitableBase):
                                wf_group.append(SwallowExceptionAwaitable(it))
                            else:
                                wf_group.append(it)
                    else:
                        wf_group = wf_items
                    wf_results: list[Any] = await WhenAllAwaitable(wf_group)  # type: ignore[assignment]
                    for pos, val in zip(wf_indices, wf_results, strict=False):
                        merged[pos] = val
                # Then process native sequentially, honoring return_exceptions
                for pos, it in zip(native_indices, native_items, strict=False):
                    try:
                        merged[pos] = await it
                    except Exception as e:  # noqa: BLE001
                        if return_exceptions:
                            merged[pos] = e
                        else:
                            raise
                return merged

            return _OneShot(_run_mixed)

        if self.originals.get("asyncio.gather") is not None:
            # Assign a fresh closure each enter so identity differs per context
            def _patched_gather_wrapper_factory() -> Callable[..., Any]:
                def _patched_gather_wrapper(*aws: Any, return_exceptions: bool = False) -> Any:
                    return _patched_gather(*aws, return_exceptions=return_exceptions)

                return _patched_gather_wrapper

            _asyncio.gather = cast(Any, _patched_gather_wrapper_factory())

        if self.mode == SandboxMode.STRICT and hasattr(_asyncio, "create_task"):

            def _blocked_create_task(*args: Any, **kwargs: Any) -> None:
                # If a coroutine object was already created by caller (e.g., create_task(dummy_coro())), close it
                try:
                    import inspect as _inspect

                    if args and _inspect.iscoroutine(args[0]) and hasattr(args[0], "close"):
                        try:
                            args[0].close()
                        except Exception:
                            pass
                except Exception:
                    pass
                raise SandboxViolationError(
                    "asyncio.create_task is not allowed in workflows (strict mode)",
                    violation_type="blocked_operation",
                    suggested_alternative="use workflow awaitables instead",
                )

            _asyncio.create_task = cast(Any, _blocked_create_task)

        _random.random = cast(Any, patched_random)
        _random.randrange = cast(Any, patched_randrange)
        _random.randint = cast(Any, patched_randint)
        _random.getrandbits = cast(Any, patched_getrandbits)
        _uuid_mod.uuid4 = cast(Any, patched_uuid4)
        _time_mod.time = cast(Any, patched_time)

        if self.originals["time.time_ns"] is not None:
            _time_mod.time_ns = cast(Any, patched_time_ns)

        # Note: datetime.datetime is immutable, so we can't patch it directly
        # This is a limitation of the current sandboxing approach
        # Users should use ctx.now() instead of datetime.now() in workflows

        # Apply strict mode blocks
        if self.mode == SandboxMode.STRICT:
            import builtins
            import os as _os
            import secrets as _secrets

            def _blocked_open(*args: Any, **kwargs: Any) -> Any:
                raise SandboxViolationError(
                    "File I/O operations are not allowed in workflows (strict mode)",
                    violation_type="blocked_operation",
                    suggested_alternative="use activities for I/O operations",
                )

            def _blocked_urandom(*args: Any, **kwargs: Any) -> Any:
                raise SandboxViolationError(
                    "os.urandom is not allowed in workflows (strict mode)",
                    violation_type="blocked_operation",
                    suggested_alternative="ctx.random().randbytes()",
                )

            def _blocked_secrets(*args: Any, **kwargs: Any) -> Any:
                raise SandboxViolationError(
                    "secrets module is not allowed in workflows (strict mode)",
                    violation_type="blocked_operation",
                    suggested_alternative="ctx.random() methods",
                )

            builtins.open = cast(Any, _blocked_open)
            if self.originals["os.urandom"] is not None:
                _os.urandom = cast(Any, _blocked_urandom)
            if self.originals["secrets.token_bytes"] is not None:
                _secrets.token_bytes = cast(Any, _blocked_secrets)
            if self.originals["secrets.token_hex"] is not None:
                _secrets.token_hex = cast(Any, _blocked_secrets)

    def _restore_originals(self) -> None:
        """Restore original functions after sandboxing."""
        import asyncio as _asyncio2
        import random as _random2
        import time as _time2
        import uuid as _uuid2

        _asyncio2.sleep = cast(Any, self.originals["asyncio.sleep"])
        if self.originals["asyncio.gather"] is not None:
            _asyncio2.gather = cast(Any, self.originals["asyncio.gather"])
        if self.originals["asyncio.create_task"] is not None:
            _asyncio2.create_task = cast(Any, self.originals["asyncio.create_task"])
        _random2.random = cast(Any, self.originals["random.random"])
        _random2.randrange = cast(Any, self.originals["random.randrange"])
        _random2.getrandbits = cast(Any, self.originals["random.getrandbits"])
        _uuid2.uuid4 = cast(Any, self.originals["uuid.uuid4"])
        _time2.time = cast(Any, self.originals["time.time"])

        if self.originals["time.time_ns"] is not None:
            _time2.time_ns = cast(Any, self.originals["time.time_ns"])

        # Note: datetime.datetime is immutable, so we can't restore it
        # This is a limitation of the current sandboxing approach

        # Restore strict mode blocks
        if self.mode == SandboxMode.STRICT:
            import builtins
            import os as _os
            import secrets as _secrets

            builtins.open = cast(Any, self.originals["builtins.open"])
            if self.originals["os.urandom"] is not None:
                _os.urandom = cast(Any, self.originals["os.urandom"])
            if self.originals["secrets.token_bytes"] is not None:
                _secrets.token_bytes = cast(Any, self.originals["secrets.token_bytes"])
            if self.originals["secrets.token_hex"] is not None:
                _secrets.token_hex = cast(Any, self.originals["secrets.token_hex"])


@contextlib.contextmanager
def _sandbox_scope(async_ctx: Any, mode: Union[str, SandboxMode]) -> Any:
    """
    Create a sandbox context for deterministic workflow execution.

    Args:
        async_ctx: The async workflow context
        mode: Sandbox mode ('off', 'best_effort', 'strict')

    Yields:
        None

    Raises:
        ValueError: If mode is invalid
        SandboxViolationError: If non-deterministic operations are detected in strict mode
    """
    mode = SandboxMode.from_string(mode)
    # Check for global disable (captured at module load to avoid non-determinism detection)
    if mode != SandboxMode.OFF and _DISABLE_DETECTION:
        mode = SandboxMode.OFF

    with _Sandbox(async_ctx, mode):
        yield
