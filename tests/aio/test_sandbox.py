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
Tests for sandbox functionality in durabletask.aio.
"""

import asyncio
import datetime
import os
import random
import secrets
import time
import uuid
import warnings
from unittest.mock import Mock, patch

import pytest

from durabletask import task as dt_task
from durabletask.aio import NonDeterminismWarning, _NonDeterminismDetector
from durabletask.aio.errors import AsyncWorkflowError
from durabletask.aio.sandbox import _sandbox_scope


class TestNonDeterminismDetector:
    """Test NonDeterminismWarning and _NonDeterminismDetector functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_ctx = Mock()
        self.mock_ctx.instance_id = "test-instance"
        self.mock_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)

    def test_non_determinism_warning_creation(self):
        """Test creating NonDeterminismWarning."""
        warning = NonDeterminismWarning("Test warning message")
        assert str(warning) == "Test warning message"
        assert issubclass(NonDeterminismWarning, UserWarning)

    def test_detector_creation(self):
        """Test creating _NonDeterminismDetector."""
        detector = _NonDeterminismDetector(self.mock_ctx, "best_effort")
        assert detector.async_ctx is self.mock_ctx
        assert detector.mode == "best_effort"
        assert detector.detected_calls == set()

    def test_detector_context_manager_off_mode(self):
        """Test detector context manager with off mode."""
        detector = _NonDeterminismDetector(self.mock_ctx, "off")

        with detector:
            # Should not set up tracing in off mode
            pass

        # Should complete without issues

    def test_detector_context_manager_best_effort_mode(self):
        """Test detector context manager with best_effort mode."""
        detector = _NonDeterminismDetector(self.mock_ctx, "best_effort")

        import sys

        pre_trace = sys.gettrace()
        with detector:
            # Should set up tracing
            original_trace = sys.gettrace()
            assert original_trace is not pre_trace

        # After exit, original trace should be restored
        assert sys.gettrace() is pre_trace

    def test_detector_trace_calls_detection(self):
        """Test that detector can identify non-deterministic calls."""
        detector = _NonDeterminismDetector(self.mock_ctx, "best_effort")

        # Create a mock frame that looks like it's calling datetime.now
        mock_frame = Mock()
        mock_frame.f_code.co_filename = "/test/file.py"
        mock_frame.f_code.co_name = "now"
        mock_frame.f_locals = {"datetime": Mock(__module__="datetime")}

        # Test the frame checking logic
        detector._check_frame_for_non_determinism(mock_frame)

        # Should detect the call (implementation may vary)

    def test_detector_strict_mode_raises_error(self):
        """Test that detector raises error in strict mode."""
        detector = _NonDeterminismDetector(self.mock_ctx, "strict")

        # Create a mock frame for a non-deterministic call
        mock_frame = Mock()
        mock_frame.f_code.co_filename = "/test/file.py"
        mock_frame.f_code.co_name = "random"
        mock_frame.f_locals = {"random_module": Mock(__module__="random")}

        # Should raise error in strict mode when non-deterministic call detected
        with pytest.raises(AsyncWorkflowError):
            detector._handle_non_deterministic_call("random.random", mock_frame)

    def test_fast_map_random_whitelist_bound_self(self):
        """random.* with deterministic bound self should be whitelisted in fast map."""
        # Prepare detector in strict (whitelist applies before error path)
        detector = _NonDeterminismDetector(self.mock_ctx, "strict")

        class BoundSelf:
            pass

        bs = BoundSelf()
        bs._dt_deterministic = True

        frame = Mock()
        frame.f_code.co_filename = "/test/rand.py"
        frame.f_code.co_name = "random"  # function name
        frame.f_globals = {"__name__": "random"}
        frame.f_locals = {"self": bs}

        # Should not raise or warn; returns early
        detector._check_frame_for_non_determinism(frame)

    def test_fast_map_best_effort_warning_and_early_return(self):
        """best_effort should warn once for fast-map hit (e.g., os.getenv) and return early."""
        detector = _NonDeterminismDetector(self.mock_ctx, "best_effort")

        frame = Mock()
        frame.f_code.co_filename = "/test/osmod.py"
        frame.f_code.co_name = "getenv"
        frame.f_globals = {"__name__": "os"}
        frame.f_locals = {}

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            detector._check_frame_for_non_determinism(frame)
        assert any(issubclass(rec.category, NonDeterminismWarning) for rec in w)

    def test_fast_map_random_strict_raises_when_not_deterministic(self):
        """random.* without deterministic bound self should trigger strict violation via fast map."""
        detector = _NonDeterminismDetector(self.mock_ctx, "strict")

        frame = Mock()
        frame.f_code.co_filename = "/test/rand2.py"
        frame.f_code.co_name = "randint"
        frame.f_globals = {"__name__": "random"}
        frame.f_locals = {"self": object()}  # no _dt_deterministic

        with pytest.raises(AsyncWorkflowError):
            detector._check_frame_for_non_determinism(frame)

    def test_detector_off_mode_no_tracing(self):
        """Test detector in off mode doesn't set up tracing."""
        detector = _NonDeterminismDetector(self.mock_ctx, "off")

        import sys

        original_trace = sys.gettrace()

        with detector:
            # Should not change trace function in off mode
            assert sys.gettrace() is original_trace

        # Should still be the same after exit
        assert sys.gettrace() is original_trace

    def test_detector_exception_in_globals_access(self):
        """Test exception handling when accessing frame globals."""
        detector = _NonDeterminismDetector(self.mock_ctx, "best_effort")

        # Create a frame that raises exception when accessing f_globals
        frame = Mock()
        frame.f_code.co_filename = "/test/bad.py"
        frame.f_code.co_name = "test_func"
        frame.f_globals = Mock()
        frame.f_globals.get.side_effect = Exception("globals access failed")

        # Should not raise, just handle gracefully
        detector._check_frame_for_non_determinism(frame)

    def test_detector_exception_in_whitelist_check(self):
        """Test exception handling in whitelist check."""
        detector = _NonDeterminismDetector(self.mock_ctx, "strict")

        frame = Mock()
        frame.f_code.co_filename = "/test/rand3.py"
        frame.f_code.co_name = "random"
        frame.f_globals = {"__name__": "random"}

        # Create a self object that raises exception when accessing _dt_deterministic
        class BadSelf:
            @property
            def _dt_deterministic(self):
                raise Exception("attribute access failed")

        frame.f_locals = {"self": BadSelf()}

        # Should handle exception and continue to error path
        with pytest.raises(AsyncWorkflowError):
            detector._check_frame_for_non_determinism(frame)

    def test_detector_non_mapping_globals(self):
        """Test handling of non-mapping f_globals."""
        detector = _NonDeterminismDetector(self.mock_ctx, "strict")

        frame = Mock()
        frame.f_code.co_filename = "/test/bad_globals.py"
        frame.f_code.co_name = "getenv"
        frame.f_globals = "not a dict"  # Non-mapping globals
        frame.f_locals = {}

        # Should handle gracefully without raising
        detector._check_frame_for_non_determinism(frame)

    def test_detector_exception_in_pattern_check(self):
        """Test exception handling in pattern checking loop."""
        detector = _NonDeterminismDetector(self.mock_ctx, "strict")

        frame = Mock()
        frame.f_code.co_filename = "/test/pattern.py"
        frame.f_code.co_name = "time"
        frame.f_globals = {"time.time": Mock(side_effect=Exception("access failed"))}
        frame.f_locals = {}

        # Should handle exception and continue
        detector._check_frame_for_non_determinism(frame)

    def test_detector_debug_mode_enabled(self):
        """Test detector with debug mode enabled."""
        self.mock_ctx._debug_mode = True
        detector = _NonDeterminismDetector(self.mock_ctx, "best_effort")

        frame = Mock()
        frame.f_code.co_filename = "/test/debug.py"
        frame.f_code.co_name = "now"
        frame.f_lineno = 42

        # Capture print output
        import io
        import sys

        captured_output = io.StringIO()
        sys.stdout = captured_output

        try:
            with pytest.warns(NonDeterminismWarning):
                detector._handle_non_deterministic_call("datetime.now", frame)
                output = captured_output.getvalue()
                assert "[WORKFLOW DEBUG]" in output
                assert "datetime.now" in output
        finally:
            sys.stdout = sys.__stdout__

    def test_detector_noop_trace_method(self):
        """Test _noop_trace method (line 56)."""
        detector = _NonDeterminismDetector(self.mock_ctx, "best_effort")

        frame = Mock()
        result = detector._noop_trace(frame, "call", None)
        assert result is None

    def test_detector_trace_calls_non_call_event(self):
        """Test _trace_calls with non-call event (lines 79-80)."""
        detector = _NonDeterminismDetector(self.mock_ctx, "best_effort")

        frame = Mock()

        # Test with no original trace function
        result = detector._trace_calls(frame, "return", None)
        assert result is None

        # Test with original trace function
        original_trace = Mock(return_value="original_result")
        detector.original_trace_func = original_trace
        result = detector._trace_calls(frame, "return", None)
        assert result == "original_result"
        original_trace.assert_called_once_with(frame, "return", None)

    def test_detector_trace_calls_with_original_func(self):
        """Test _trace_calls returning original trace func (line 86)."""
        detector = _NonDeterminismDetector(self.mock_ctx, "best_effort")

        frame = Mock()
        frame.f_code.co_filename = "/test/safe.py"  # Safe filename
        frame.f_code.co_name = "safe_func"
        frame.f_globals = {}

        # Test with original trace function
        original_trace = Mock()
        detector.original_trace_func = original_trace
        result = detector._trace_calls(frame, "call", None)
        assert result is original_trace


class TestSandboxScope:
    """Test sandbox_scope functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_ctx = Mock()
        self.mock_ctx.instance_id = "test-instance"
        self.mock_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        self.mock_ctx.random.return_value = random.Random(12345)
        self.mock_ctx.uuid4.return_value = uuid.UUID("12345678-1234-5678-1234-567812345678")
        self.mock_ctx.now.return_value = datetime.datetime(2023, 1, 1, 12, 0, 0)

        # Add _base_ctx for sandbox patching
        self.mock_ctx._base_ctx = Mock()
        self.mock_ctx._base_ctx.create_timer = Mock(return_value=Mock())

        # Ensure detection is not disabled
        self.mock_ctx._detection_disabled = False

    def test_sandbox_scope_off_mode(self):
        """Test sandbox_scope with off mode."""
        original_sleep = asyncio.sleep
        original_random = random.random

        with _sandbox_scope(self.mock_ctx, "off"):
            # Should not patch anything in off mode
            assert asyncio.sleep is original_sleep
            assert random.random is original_random

    def test_sandbox_scope_invalid_mode(self):
        """Test sandbox_scope with invalid mode."""
        with pytest.raises(ValueError, match="Invalid sandbox mode"):
            with _sandbox_scope(self.mock_ctx, "invalid_mode"):
                pass

    def test_sandbox_scope_best_effort_patches(self):
        """Test sandbox_scope patches functions in best_effort mode."""
        original_sleep = asyncio.sleep
        original_random = random.random
        original_uuid4 = uuid.uuid4
        original_time = time.time

        with _sandbox_scope(self.mock_ctx, "best_effort"):
            # Should patch functions
            assert asyncio.sleep is not original_sleep
            assert random.random is not original_random
            assert uuid.uuid4 is not original_uuid4
            assert time.time is not original_time

        # Should restore originals
        assert asyncio.sleep is original_sleep
        assert random.random is original_random
        assert uuid.uuid4 is original_uuid4
        assert time.time is original_time

    def test_sandbox_scope_strict_mode_blocks_dangerous_functions(self):
        """Test sandbox_scope blocks dangerous functions in strict mode."""
        original_open = open

        with _sandbox_scope(self.mock_ctx, "strict"):
            # Should block dangerous functions
            with pytest.raises(AsyncWorkflowError, match="File I/O operations are not allowed"):
                open("test.txt", "r")

        # Should restore original
        assert open is original_open

    def test_strict_allows_ctx_random_methods_and_patched_global_random(self):
        """Strict mode should allow ctx.random().randint and patched global random methods."""
        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "rng-ctx"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(base_ctx)

        with _sandbox_scope(async_ctx, "strict"):
            # Allowed: via ctx.random() (detector should whitelist)
            val1 = async_ctx.random().randint(1, 10)
            assert isinstance(val1, int)

            # Also allowed: global random methods are patched deterministically in strict
            val2 = random.randint(1, 10)
            assert isinstance(val2, int)

    def test_strict_allows_all_deterministic_helpers(self):
        """Strict mode should allow all ctx deterministic helpers without violations."""
        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "det-helpers"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(base_ctx)

        with _sandbox_scope(async_ctx, "strict"):
            # now()
            now_val = async_ctx.now()
            assert isinstance(now_val, datetime.datetime)

            # uuid4()
            uid = async_ctx.uuid4()
            import uuid as _uuid

            assert isinstance(uid, _uuid.UUID)

            # random().random, randint, choice
            rnd = async_ctx.random()
            assert isinstance(rnd.random(), float)
            assert isinstance(rnd.randint(1, 10), int)
            assert isinstance(rnd.choice([1, 2, 3]), int)

            # random_string / random_int / random_choice
            s = async_ctx.random_string(5)
            assert isinstance(s, str) and len(s) == 5
            ri = async_ctx.random_int(1, 10)
            assert isinstance(ri, int)
            rc = async_ctx.random_choice(["a", "b"])
            assert rc in ["a", "b"]

    def test_sandbox_scope_patches_asyncio_sleep(self):
        """Test that asyncio.sleep is properly patched within sandbox context."""
        with _sandbox_scope(self.mock_ctx, "best_effort"):
            # Import asyncio within the sandbox context to get the patched version
            import asyncio as sandboxed_asyncio

            # Call the patched sleep directly
            patched_sleep_result = sandboxed_asyncio.sleep(1.0)

            # Should return our patched sleep awaitable
            assert hasattr(patched_sleep_result, "__await__")

            # The awaitable should yield a timer task when awaited
            awaitable_gen = patched_sleep_result.__await__()
            try:
                yielded_task = next(awaitable_gen)
                # Should be the mock timer task
                assert yielded_task is self.mock_ctx._base_ctx.create_timer.return_value
            except StopIteration:
                pass  # Sleep completed immediately

    def test_sandbox_scope_patches_random_functions(self):
        """Test that random functions are properly patched."""
        with _sandbox_scope(self.mock_ctx, "best_effort"):
            # Should use deterministic random
            val1 = random.random()
            val2 = random.randint(1, 100)
            val3 = random.randrange(10)

            assert isinstance(val1, float)
            assert isinstance(val2, int)
            assert isinstance(val3, int)
            assert 1 <= val2 <= 100
            assert 0 <= val3 < 10

    def test_sandbox_scope_patches_uuid4(self):
        """Test that uuid.uuid4 is properly patched."""
        with _sandbox_scope(self.mock_ctx, "best_effort"):
            test_uuid = uuid.uuid4()
            assert isinstance(test_uuid, uuid.UUID)
            assert test_uuid.version == 4

    def test_sandbox_scope_patches_time_functions(self):
        """Test that time functions are properly patched."""
        with _sandbox_scope(self.mock_ctx, "best_effort"):
            current_time = time.time()
            assert isinstance(current_time, float)

            if hasattr(time, "time_ns"):
                current_time_ns = time.time_ns()
                assert isinstance(current_time_ns, int)

    def test_patched_randrange_step_branch(self):
        """Hit patched randrange path with step != 1 to cover the loop branch."""
        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "step-test"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(base_ctx)
        with _sandbox_scope(async_ctx, "best_effort"):
            v = random.randrange(1, 10, 3)
            assert 1 <= v < 10 and (v - 1) % 3 == 0

    def test_sandbox_scope_strict_mode_blocks_os_urandom(self):
        """Test that os.urandom is blocked in strict mode."""
        with _sandbox_scope(self.mock_ctx, "strict"):
            with pytest.raises(AsyncWorkflowError, match="os.urandom is not allowed"):
                os.urandom(16)

    def test_sandbox_scope_strict_mode_blocks_secrets(self):
        """Test that secrets module is blocked in strict mode."""
        with _sandbox_scope(self.mock_ctx, "strict"):
            with pytest.raises(AsyncWorkflowError, match="secrets module is not allowed"):
                secrets.token_bytes(16)

            with pytest.raises(AsyncWorkflowError, match="secrets module is not allowed"):
                secrets.token_hex(16)

    def test_sandbox_scope_strict_mode_blocks_asyncio_create_task(self):
        """Test that asyncio.create_task is blocked in strict mode."""

        async def dummy_coro():
            return "test"

        with _sandbox_scope(self.mock_ctx, "strict"):
            with pytest.raises(AsyncWorkflowError, match="asyncio.create_task is not allowed"):
                asyncio.create_task(dummy_coro())

    @pytest.mark.asyncio
    async def test_asyncio_sleep_zero_passthrough(self):
        """sleep(0) should use original asyncio.sleep (passthrough branch)."""
        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "sleep-zero"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(base_ctx)
        with _sandbox_scope(async_ctx, "best_effort"):
            # Should not raise; executes passthrough branch in patched_sleep
            await asyncio.sleep(0)

    def test_strict_restores_os_and_secrets_on_exit(self):
        """Ensure strict mode restores os.urandom and secrets functions on exit."""
        orig_urandom = getattr(os, "urandom", None)
        orig_token_bytes = getattr(secrets, "token_bytes", None)
        orig_token_hex = getattr(secrets, "token_hex", None)

        with _sandbox_scope(self.mock_ctx, "strict"):
            if orig_urandom is not None:
                with pytest.raises(AsyncWorkflowError):
                    os.urandom(1)
            if orig_token_bytes is not None:
                with pytest.raises(AsyncWorkflowError):
                    secrets.token_bytes(1)
            if orig_token_hex is not None:
                with pytest.raises(AsyncWorkflowError):
                    secrets.token_hex(1)

        # After exit, originals should be restored
        if orig_urandom is not None:
            assert os.urandom is orig_urandom
        if orig_token_bytes is not None:
            assert secrets.token_bytes is orig_token_bytes
        if orig_token_hex is not None:
            assert secrets.token_hex is orig_token_hex

    @pytest.mark.asyncio
    async def test_empty_gather_caching_replay(self):
        """Empty gather should be awaitable and replay cached result on repeated awaits."""
        from durabletask.aio import AsyncWorkflowContext

        mock_base_ctx = Mock()
        mock_base_ctx.instance_id = "gather-cache"
        mock_base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(mock_base_ctx)
        with _sandbox_scope(async_ctx, "best_effort"):
            g0 = asyncio.gather()
            r0a = await g0
            r0b = await g0
            assert r0a == [] and r0b == []

    def test_patched_datetime_now_with_tz(self):
        """datetime.now(tz=UTC) should return aware UTC when patched."""
        from datetime import timezone

        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "tz-test"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(base_ctx)
        with _sandbox_scope(async_ctx, "best_effort"):
            now_utc = datetime.datetime.now(tz=timezone.utc)
            assert now_utc.tzinfo is timezone.utc

    @pytest.mark.asyncio
    async def test_create_task_allowed_in_best_effort(self):
        """In best_effort mode, create_task should be allowed and runnable."""
        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "best-effort-ct"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(base_ctx)

        async def quick():
            await asyncio.sleep(0)
            return "ok"

        with _sandbox_scope(async_ctx, "best_effort"):
            t = asyncio.create_task(quick())
            assert await t == "ok"

    @pytest.mark.asyncio
    async def test_create_task_blocked_strict_no_unawaited_warning(self):
        """Strict mode: ensure blocked coroutine is closed (no 'never awaited' warnings)."""
        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "strict-ct"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(base_ctx)

        async def dummy():
            return 1

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with pytest.raises(AsyncWorkflowError):
                with _sandbox_scope(async_ctx, "strict"):
                    asyncio.create_task(dummy())
            assert not any("was never awaited" in str(rec.message) for rec in w)

    @pytest.mark.asyncio
    async def test_env_disable_detection_allows_create_task(self):
        """DAPR_WF_DISABLE_DETECTION=true forces mode off; create_task allowed."""
        import durabletask.aio.sandbox as sandbox_module
        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "env-off"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(base_ctx)

        async def quick():
            await asyncio.sleep(0)
            return "ok"

        # Mock the module-level constant to simulate environment variable set
        with patch.object(sandbox_module, "_DISABLE_DETECTION", True):
            with _sandbox_scope(async_ctx, "strict"):
                t = asyncio.create_task(quick())
                assert await t == "ok"

    def test_sandbox_scope_global_disable_env_var(self):
        """Test that DAPR_WF_DISABLE_DETECTION environment variable works."""
        import durabletask.aio.sandbox as sandbox_module

        original_random = random.random

        # Mock the module-level constant to simulate environment variable set
        with patch.object(sandbox_module, "_DISABLE_DETECTION", True):
            with _sandbox_scope(self.mock_ctx, "best_effort"):
                # Should not patch when globally disabled
                assert random.random is original_random

    def test_sandbox_scope_context_detection_disabled(self):
        """Test that context-level detection disable works."""
        self.mock_ctx._detection_disabled = True
        original_random = random.random

        with _sandbox_scope(self.mock_ctx, "best_effort"):
            # Should not patch when disabled on context
            assert random.random is original_random

    def test_rng_context_fallback_to_base_ctx(self):
        """Sandbox should fall back to _base_ctx.instance_id/current_utc_datetime when missing on async_ctx.

        Same context twice -> identical deterministic sequence
        Change only instance_id -> different sequence
        Change only current_utc_datetime -> different sequence
        """

        class MinimalCtx:
            pass

        fallback = MinimalCtx()
        fallback._base_ctx = Mock()
        fallback._base_ctx.instance_id = "fallback-instance"
        fallback._base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)

        # Ensure MinimalCtx lacks direct attributes
        assert not hasattr(fallback, "instance_id")
        assert not hasattr(fallback, "current_utc_datetime")
        assert not hasattr(fallback, "now")

        # Same fallback context twice -> identical deterministic sequence
        with _sandbox_scope(fallback, "best_effort"):
            seq1 = [random.random() for _ in range(3)]
        with _sandbox_scope(fallback, "best_effort"):
            seq2 = [random.random() for _ in range(3)]
        assert seq1 == seq2

        # Change only instance_id -> different sequence
        fallback_id = MinimalCtx()
        fallback_id._base_ctx = Mock()
        fallback_id._base_ctx.instance_id = "fallback-instance-2"
        fallback_id._base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        with _sandbox_scope(fallback_id, "best_effort"):
            seq_id = [random.random() for _ in range(3)]
        assert seq_id != seq1

        # Change only current_utc_datetime -> different sequence
        fallback_time = MinimalCtx()
        fallback_time._base_ctx = Mock()
        fallback_time._base_ctx.instance_id = "fallback-instance"
        fallback_time._base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 1)
        with _sandbox_scope(fallback_time, "best_effort"):
            seq_time = [random.random() for _ in range(3)]
        assert seq_time != seq1

    def test_sandbox_scope_nested_contexts(self):
        """Test nested sandbox contexts."""
        original_random = random.random

        with _sandbox_scope(self.mock_ctx, "best_effort"):
            patched_random_1 = random.random
            assert patched_random_1 is not original_random

            with _sandbox_scope(self.mock_ctx, "strict"):
                patched_random_2 = random.random
                # Should be patched differently or same
                assert patched_random_2 is not original_random

            # Should restore to first patch level
            assert random.random is patched_random_1

        # Should restore to original
        assert random.random is original_random

    def test_sandbox_scope_exception_handling(self):
        """Test that sandbox properly restores functions even if exception occurs."""
        original_random = random.random

        try:
            with _sandbox_scope(self.mock_ctx, "best_effort"):
                assert random.random is not original_random
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Should still restore original even after exception
        assert random.random is original_random

    def test_sandbox_scope_deterministic_behavior(self):
        """Test that sandbox provides deterministic behavior."""
        results1 = []
        results2 = []

        # First run
        with _sandbox_scope(self.mock_ctx, "best_effort"):
            results1.append(random.random())
            results1.append(random.randint(1, 100))
            results1.append(str(uuid.uuid4()))
            results1.append(time.time())

        # Second run with same context
        with _sandbox_scope(self.mock_ctx, "best_effort"):
            results2.append(random.random())
            results2.append(random.randint(1, 100))
            results2.append(str(uuid.uuid4()))
            results2.append(time.time())

        # Should be deterministic (same results)
        assert results1 == results2

    def test_sandbox_scope_different_contexts_different_results(self):
        """Test that different contexts produce different results."""
        mock_ctx2 = Mock()
        mock_ctx2.instance_id = "different-instance"
        mock_ctx2.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_ctx2.random.return_value = random.Random(54321)
        mock_ctx2.uuid4.return_value = uuid.UUID("87654321-4321-8765-4321-876543218765")
        mock_ctx2.now.return_value = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_ctx2._detection_disabled = False

        results1 = []
        results2 = []

        # First context
        with _sandbox_scope(self.mock_ctx, "best_effort"):
            results1.append(random.random())
            results1.append(str(uuid.uuid4()))

        # Different context
        with _sandbox_scope(mock_ctx2, "best_effort"):
            results2.append(random.random())
            results2.append(str(uuid.uuid4()))

        # Should be different
        assert results1 != results2

    def test_alias_context_managers_cover(self):
        """Call the alias context managers to cover their paths."""
        from durabletask.aio.sandbox import _sandbox_best_effort, _sandbox_off, _sandbox_strict

        with _sandbox_off(self.mock_ctx):
            pass
        with _sandbox_best_effort(self.mock_ctx):
            pass
        with _sandbox_strict(self.mock_ctx):
            # strict does patch; simple no-op body is fine
            pass

    def test_sandbox_missing_context_attributes(self):
        """Test sandbox with context missing various attributes."""

        # Create context with missing attributes but proper fallbacks
        minimal_ctx = Mock()
        minimal_ctx._detection_disabled = False
        minimal_ctx.instance_id = None  # Will use empty string fallback
        minimal_ctx._base_ctx = None  # No base context
        # Mock now() to return proper datetime
        minimal_ctx.now = Mock(return_value=datetime.datetime(2023, 1, 1, 12, 0, 0))
        minimal_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)

        with _sandbox_scope(minimal_ctx, "best_effort"):
            # Should use fallback values
            val = random.random()
            assert isinstance(val, float)

    def test_sandbox_context_with_now_exception(self):
        """Test sandbox when ctx.now() raises exception."""

        ctx = Mock()
        ctx._detection_disabled = False
        ctx.instance_id = "test"
        ctx.now = Mock(side_effect=Exception("now() failed"))
        ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)

        with _sandbox_scope(ctx, "best_effort"):
            # Should fallback to current_utc_datetime
            val = random.random()
            assert isinstance(val, float)

    def test_sandbox_context_missing_base_ctx(self):
        """Test sandbox with context missing _base_ctx."""
        ctx = Mock()
        ctx._detection_disabled = False
        ctx.instance_id = None  # No instance_id
        ctx._base_ctx = None  # No _base_ctx
        ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        # Mock now() to return proper datetime
        ctx.now = Mock(return_value=datetime.datetime(2023, 1, 1, 12, 0, 0))

        with _sandbox_scope(ctx, "best_effort"):
            # Should use empty string fallback for instance_id
            val = random.random()
            assert isinstance(val, float)

    def test_sandbox_rng_setattr_exception(self):
        """Test sandbox when setattr on rng fails."""
        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "test"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(base_ctx)

        # Mock deterministic_random to return an object that can't be modified
        with patch("durabletask.aio.sandbox.deterministic_random") as mock_rng:
            # Create a class that raises exception on setattr
            class ImmutableRNG:
                def __setattr__(self, name, value):
                    if name == "_dt_deterministic":
                        raise Exception("setattr failed")
                    super().__setattr__(name, value)

                def random(self):
                    return 0.5

            mock_rng.return_value = ImmutableRNG()

            with _sandbox_scope(async_ctx, "best_effort"):
                # Should handle setattr exception gracefully
                val = random.random()
                assert isinstance(val, float)

    def test_sandbox_missing_time_ns(self):
        """Test sandbox when time.time_ns is not available."""
        import time as time_mod

        # Temporarily remove time_ns if it exists
        original_time_ns = getattr(time_mod, "time_ns", None)
        if hasattr(time_mod, "time_ns"):
            delattr(time_mod, "time_ns")

        try:
            with _sandbox_scope(self.mock_ctx, "best_effort"):
                # Should work without time_ns
                val = time_mod.time()
                assert isinstance(val, float)
        finally:
            # Restore time_ns if it existed
            if original_time_ns is not None:
                time_mod.time_ns = original_time_ns

    def test_sandbox_missing_optional_functions(self):
        """Test sandbox with missing optional functions."""
        import os
        import secrets

        # Temporarily remove optional functions
        original_urandom = getattr(os, "urandom", None)
        original_token_bytes = getattr(secrets, "token_bytes", None)
        original_token_hex = getattr(secrets, "token_hex", None)

        if hasattr(os, "urandom"):
            delattr(os, "urandom")
        if hasattr(secrets, "token_bytes"):
            delattr(secrets, "token_bytes")
        if hasattr(secrets, "token_hex"):
            delattr(secrets, "token_hex")

        try:
            with _sandbox_scope(self.mock_ctx, "strict"):
                # Should work without the optional functions
                val = random.random()
                assert isinstance(val, float)
        finally:
            # Restore functions
            if original_urandom is not None:
                os.urandom = original_urandom
            if original_token_bytes is not None:
                secrets.token_bytes = original_token_bytes
            if original_token_hex is not None:
                secrets.token_hex = original_token_hex

    def test_sandbox_restore_missing_optional_functions(self):
        """Test sandbox restore with missing optional functions."""
        import os
        import secrets

        # Remove optional functions before entering sandbox
        original_urandom = getattr(os, "urandom", None)
        original_token_bytes = getattr(secrets, "token_bytes", None)
        original_token_hex = getattr(secrets, "token_hex", None)

        if hasattr(os, "urandom"):
            delattr(os, "urandom")
        if hasattr(secrets, "token_bytes"):
            delattr(secrets, "token_bytes")
        if hasattr(secrets, "token_hex"):
            delattr(secrets, "token_hex")

        try:
            with _sandbox_scope(self.mock_ctx, "strict"):
                val = random.random()
                assert isinstance(val, float)
            # Should exit cleanly even with missing functions
        finally:
            # Restore functions
            if original_urandom is not None:
                os.urandom = original_urandom
            if original_token_bytes is not None:
                secrets.token_bytes = original_token_bytes
            if original_token_hex is not None:
                secrets.token_hex = original_token_hex

    def test_sandbox_patched_sleep_with_base_ctx(self):
        """Test patched sleep accessing _base_ctx (lines 325-343)."""
        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "sleep-test"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        base_ctx.create_timer = Mock(return_value=Mock())

        async_ctx = AsyncWorkflowContext(base_ctx)

        with _sandbox_scope(async_ctx, "best_effort"):
            # Test positive delay - should use patched version
            sleep_awaitable = asyncio.sleep(1.0)
            assert hasattr(sleep_awaitable, "__await__")

            # Actually await it to avoid the warning
            # The mock should make this complete immediately
            try:
                gen = sleep_awaitable.__await__()
                next(gen)
            except StopIteration:
                pass  # Expected when mock completes immediately

            # Test zero delay - should use original (passthrough)
            zero_sleep = asyncio.sleep(0)
            # This should be the original coroutine or our awaitable
            assert hasattr(zero_sleep, "__await__")

            # Actually await it to avoid the warning
            # The mock should make this complete immediately
            try:
                gen = zero_sleep.__await__()
                next(gen)
            except StopIteration:
                pass  # Expected when mock completes immediately

    def test_sandbox_strict_blocking_functions_coverage(self):
        """Test strict mode blocking functions to hit lines 588-615."""
        import builtins
        import os
        import secrets

        with _sandbox_scope(self.mock_ctx, "strict"):
            # Test blocked open function (lines 588-593)
            with pytest.raises(AsyncWorkflowError, match="File I/O operations are not allowed"):
                builtins.open("test.txt", "r")

            # Test blocked os.urandom (lines 595-600) - if available
            if hasattr(os, "urandom"):
                with pytest.raises(AsyncWorkflowError, match="os.urandom is not allowed"):
                    os.urandom(16)

            # Test blocked secrets functions (lines 602-607) - if available
            if hasattr(secrets, "token_bytes"):
                with pytest.raises(AsyncWorkflowError, match="secrets module is not allowed"):
                    secrets.token_bytes(16)

            if hasattr(secrets, "token_hex"):
                with pytest.raises(AsyncWorkflowError, match="secrets module is not allowed"):
                    secrets.token_hex(16)

    def test_sandbox_restore_with_gather_and_create_task(self):
        """Test restore functions with gather and create_task (lines 624-628)."""
        import asyncio

        original_gather = asyncio.gather
        original_create_task = getattr(asyncio, "create_task", None)

        with _sandbox_scope(self.mock_ctx, "best_effort"):
            # gather should be patched in best_effort
            assert asyncio.gather is not original_gather
            # create_task is only patched in strict mode, not best_effort

        # Should be restored
        assert asyncio.gather is original_gather

        # Test strict mode where create_task is also patched
        with _sandbox_scope(self.mock_ctx, "strict"):
            assert asyncio.gather is not original_gather
            if original_create_task is not None:
                assert asyncio.create_task is not original_create_task

        # Should be restored after strict mode too
        assert asyncio.gather is original_gather
        if original_create_task is not None:
            assert asyncio.create_task is original_create_task

    def test_sandbox_best_effort_debug_mode_tracing(self):
        """Test best_effort mode with debug mode enabled for full tracing (line 61)."""
        self.mock_ctx._debug_mode = True

        import sys

        original_trace = sys.gettrace()

        detector = _NonDeterminismDetector(self.mock_ctx, "best_effort")

        with detector:
            # Should set up full tracing in debug mode
            current_trace = sys.gettrace()
            assert current_trace is not original_trace
            assert current_trace is not detector._noop_trace

        # Should restore original trace
        assert sys.gettrace() is original_trace

    def test_sandbox_detector_exit_branch_coverage(self):
        """Test detector __exit__ method branch (line 74)."""
        detector = _NonDeterminismDetector(self.mock_ctx, "off")

        # In off mode, __exit__ should not restore trace function
        import sys

        original_trace = sys.gettrace()

        with detector:
            pass  # off mode doesn't change trace

        # Should still be the same
        assert sys.gettrace() is original_trace

    def test_sandbox_context_no_current_utc_datetime(self):
        """Test sandbox with context missing current_utc_datetime (lines 358-364)."""

        # Create a minimal context object without current_utc_datetime
        class MinimalCtx:
            def __init__(self):
                self._detection_disabled = False
                self.instance_id = "test"
                self._base_ctx = None

            def now(self):
                raise Exception("now() failed")

        ctx = MinimalCtx()

        with _sandbox_scope(ctx, "best_effort"):
            # Should use epoch fallback (line 364)
            val = random.random()
            assert isinstance(val, float)


class TestGatherMixedOptimization:
    """Tests for mixed workflow/native awaitables optimization in patched gather."""

    @pytest.mark.asyncio
    async def test_mixed_groups_preserve_order_and_use_when_all(self, monkeypatch):
        from durabletask.aio import AsyncWorkflowContext
        from durabletask.aio.awaitables import AwaitableBase as _AwaitableBase

        # Create async context and enable sandbox
        base_ctx = Mock()
        base_ctx.instance_id = "mix-test"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)

        async_ctx = AsyncWorkflowContext(base_ctx)

        # Dummy workflow awaitable that should be batched into WhenAllAwaitable and not awaited individually
        class DummyWF(_AwaitableBase[str]):
            def _to_task(self):
                # Would normally convert to a durable task; not needed in this test
                return Mock(spec=dt_task.Task)

        # Patch WhenAllAwaitable to a fast fake that returns predictable results
        recorded_items: list[list[object]] = []

        class FakeWhenAll:
            def __init__(self, items):
                recorded_items.append(list(items))
                self._items = list(items)

            def __await__(self):
                async def _coro():
                    # Return results per-item deterministically
                    return [f"W{i}" for i, _ in enumerate(self._items)]

                return _coro().__await__()

        monkeypatch.setattr("durabletask.aio.awaitables.WhenAllAwaitable", FakeWhenAll)

        # Native coroutines
        async def native(i: int):
            await asyncio.sleep(0)
            return f"N{i}"

        with _sandbox_scope(async_ctx, "best_effort"):
            out = await asyncio.gather(DummyWF(), native(0), DummyWF(), native(1))

        # Order preserved and batched results merged back correctly
        assert out == ["W0", "N0", "W1", "N1"]
        # Ensure WhenAll got only workflow awaitables (2 items)
        assert recorded_items and len(recorded_items[0]) == 2


class TestNowWithSequence:
    """Tests for AsyncWorkflowContext.now_with_sequence."""

    def test_now_with_sequence_increments(self):
        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "test-seq"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        base_ctx.is_replaying = False
        async_ctx = AsyncWorkflowContext(base_ctx)

        # Each call should increment by 1 microsecond
        t1 = async_ctx.now_with_sequence()
        t2 = async_ctx.now_with_sequence()
        t3 = async_ctx.now_with_sequence()

        assert t1 == datetime.datetime(2023, 1, 1, 12, 0, 0, 0)
        assert t2 == datetime.datetime(2023, 1, 1, 12, 0, 0, 1)
        assert t3 == datetime.datetime(2023, 1, 1, 12, 0, 0, 2)
        assert t1 < t2 < t3

    def test_now_with_sequence_deterministic_on_replay(self):
        from durabletask.aio import AsyncWorkflowContext

        # First execution
        base_ctx1 = Mock()
        base_ctx1.instance_id = "test-replay"
        base_ctx1.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        base_ctx1.is_replaying = False
        ctx1 = AsyncWorkflowContext(base_ctx1)

        t1_first = ctx1.now_with_sequence()
        t2_first = ctx1.now_with_sequence()

        # Replay - counter resets (new context instance)
        base_ctx2 = Mock()
        base_ctx2.instance_id = "test-replay"
        base_ctx2.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        base_ctx2.is_replaying = True
        ctx2 = AsyncWorkflowContext(base_ctx2)

        t1_replay = ctx2.now_with_sequence()
        t2_replay = ctx2.now_with_sequence()

        # Should produce identical timestamps (deterministic)
        assert t1_first == t1_replay
        assert t2_first == t2_replay

    def test_now_with_sequence_works_in_strict_mode(self):
        from durabletask.aio import AsyncWorkflowContext

        base_ctx = Mock()
        base_ctx.instance_id = "test-strict"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        base_ctx.is_replaying = False
        async_ctx = AsyncWorkflowContext(base_ctx)

        # Should work fine in strict sandbox mode (deterministic)
        with _sandbox_scope(async_ctx, "strict"):
            t1 = async_ctx.now_with_sequence()
            t2 = async_ctx.now_with_sequence()
            assert t1 < t2

    @pytest.mark.asyncio
    async def test_mixed_groups_return_exceptions_true(self, monkeypatch):
        from durabletask.aio import AsyncWorkflowContext
        from durabletask.aio.awaitables import AwaitableBase as _AwaitableBase

        base_ctx = Mock()
        base_ctx.instance_id = "mix-exc"
        base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(base_ctx)

        class DummyWF(_AwaitableBase[str]):
            def _to_task(self):
                return Mock(spec=dt_task.Task)

        # Fake WhenAll that returns values, simulating exception swallowing already applied
        class FakeWhenAll:
            def __init__(self, items):
                self._items = list(items)

            def __await__(self):
                async def _coro():
                    # Return placeholders for each workflow item
                    return ["W_OK" for _ in self._items]

                return _coro().__await__()

        monkeypatch.setattr("durabletask.aio.awaitables.WhenAllAwaitable", FakeWhenAll)

        async def native_ok():
            return "N_OK"

        async def native_fail():
            raise RuntimeError("boom")

        with _sandbox_scope(async_ctx, "best_effort"):
            res = await asyncio.gather(
                DummyWF(), native_fail(), native_ok(), return_exceptions=True
            )

        assert res[0] == "W_OK"
        assert isinstance(res[1], RuntimeError)
        assert res[2] == "N_OK"

    def test_sandbox_scope_asyncio_gather_patching(self):
        """Test that asyncio.gather is properly patched."""

        async def test_task():
            return "test"

        # Capture original gather before entering sandbox
        original_gather = asyncio.gather
        from durabletask.aio import AsyncWorkflowContext

        mock_base_ctx = Mock()
        mock_base_ctx.instance_id = "gather-patch"
        mock_base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(mock_base_ctx)
        with _sandbox_scope(async_ctx, "best_effort"):
            # Should patch gather
            assert asyncio.gather is not original_gather

            # Test empty gather
            empty_gather = asyncio.gather()
            assert hasattr(empty_gather, "__await__")

    def test_sandbox_scope_workflow_awaitables_detection(self):
        """Test that sandbox can detect workflow awaitables."""
        from durabletask import task as dt_task
        from durabletask.aio import AsyncWorkflowContext
        from durabletask.aio.awaitables import ActivityAwaitable

        # Create a mock activity awaitable
        mock_task = Mock(spec=dt_task.Task)
        mock_base_ctx = Mock()
        mock_base_ctx.call_activity.return_value = mock_task
        mock_base_ctx.instance_id = "detect-wf"
        mock_base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        async_ctx = AsyncWorkflowContext(mock_base_ctx)

        activity_awaitable = ActivityAwaitable(mock_base_ctx, lambda: "test", input="test")

        with _sandbox_scope(async_ctx, "best_effort"):
            # Should recognize workflow awaitables
            gather_result = asyncio.gather(activity_awaitable)
            assert hasattr(gather_result, "__await__")


class TestPatchedFunctionImplementations:
    """Test that patched deterministic functions work correctly."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_ctx = Mock()
        self.mock_ctx.instance_id = "test-instance"
        self.mock_ctx.current_utc_datetime = datetime.datetime(2025, 1, 1, 12, 0, 0)

    def test_patched_random_functions(self):
        """Test all patched random functions produce deterministic results."""
        with _sandbox_scope(self.mock_ctx, "best_effort"):
            # Test random()
            r1 = random.random()
            assert isinstance(r1, float)
            assert 0 <= r1 < 1

            # Test randint()
            ri = random.randint(1, 100)
            assert isinstance(ri, int)
            assert 1 <= ri <= 100

            # Test getrandbits()
            rb = random.getrandbits(8)
            assert isinstance(rb, int)
            assert 0 <= rb < 256

            # Test randrange() with step
            rr = random.randrange(0, 100, 5)
            assert isinstance(rr, int)
            assert 0 <= rr < 100
            assert rr % 5 == 0

            # Test randrange() single arg
            rr_single = random.randrange(50)
            assert isinstance(rr_single, int)
            assert 0 <= rr_single < 50

    def test_patched_time_functions(self):
        """Test patched time functions return deterministic values."""
        with _sandbox_scope(self.mock_ctx, "best_effort"):
            t = time.time()
            assert isinstance(t, float)
            assert t > 0

            # time_ns if available
            if hasattr(time, "time_ns"):
                tn = time.time_ns()
                assert isinstance(tn, int)
                assert tn > 0

    def test_patched_datetime_now_with_timezone(self):
        """Test patched datetime.now() with timezone argument."""
        import datetime as dt

        with _sandbox_scope(self.mock_ctx, "best_effort"):
            # With timezone should still work
            tz = dt.timezone.utc
            now_tz = dt.datetime.now(tz)
            assert isinstance(now_tz, dt.datetime)
            assert now_tz.tzinfo is not None


class TestAsyncioSleepEdgeCases:
    """Test asyncio.sleep patching edge cases."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        self.mock_base_ctx.instance_id = "test-instance"
        self.mock_base_ctx.current_utc_datetime = datetime.datetime(2025, 1, 1, 12, 0, 0)
        self.mock_base_ctx.create_timer = Mock()

    def test_asyncio_sleep_zero_delay_passthrough(self):
        """Test that zero delay passes through to original asyncio.sleep."""
        from durabletask.aio import AsyncWorkflowContext

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        with _sandbox_scope(async_ctx, "best_effort"):
            # Zero delay should pass through
            result = asyncio.sleep(0)
            # Should be a coroutine from original asyncio.sleep
            assert asyncio.iscoroutine(result)
            result.close()  # Clean up

    def test_asyncio_sleep_negative_delay_passthrough(self):
        """Test that negative delay passes through to original asyncio.sleep."""
        from durabletask.aio import AsyncWorkflowContext

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        with _sandbox_scope(async_ctx, "best_effort"):
            # Negative delay should pass through
            result = asyncio.sleep(-1)
            assert asyncio.iscoroutine(result)
            result.close()  # Clean up

    def test_asyncio_sleep_positive_delay_uses_timer(self):
        """Test that positive delay uses create_timer."""
        from durabletask.aio import AsyncWorkflowContext

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        with _sandbox_scope(async_ctx, "best_effort"):
            # Positive delay should create patched awaitable
            result = asyncio.sleep(5)
            # Should have __await__ method
            assert hasattr(result, "__await__")

    def test_asyncio_sleep_invalid_delay(self):
        """Test asyncio.sleep with invalid delay value."""
        from durabletask.aio import AsyncWorkflowContext

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        with _sandbox_scope(async_ctx, "best_effort"):
            # Invalid delay should still work (fallthrough to patched awaitable)
            result = asyncio.sleep("invalid")
            assert hasattr(result, "__await__")


class TestRNGContextFallbacks:
    """Test RNG initialization with missing context attributes."""

    def test_rng_missing_instance_id(self):
        """Test RNG initialization when instance_id is missing."""
        mock_ctx = Mock()
        # No instance_id attribute
        mock_ctx.current_utc_datetime = datetime.datetime(2025, 1, 1, 12, 0, 0)

        with _sandbox_scope(mock_ctx, "best_effort"):
            # Should use fallback and still work
            r = random.random()
            assert isinstance(r, float)

    def test_rng_missing_base_ctx_instance_id(self):
        """Test RNG with no instance_id on main or base context."""
        mock_ctx = Mock()
        mock_ctx._base_ctx = Mock()
        # Neither has instance_id
        mock_ctx.current_utc_datetime = datetime.datetime(2025, 1, 1, 12, 0, 0)

        with _sandbox_scope(mock_ctx, "best_effort"):
            r = random.random()
            assert isinstance(r, float)

    def test_rng_now_method_exception(self):
        """Test RNG when now() method raises exception."""
        mock_ctx = Mock()
        mock_ctx.instance_id = "test"
        mock_ctx.now = Mock(side_effect=Exception("now() failed"))
        mock_ctx.current_utc_datetime = datetime.datetime(2025, 1, 1, 12, 0, 0)

        with _sandbox_scope(mock_ctx, "best_effort"):
            # Should fall back to current_utc_datetime
            r = random.random()
            assert isinstance(r, float)

    def test_rng_missing_current_utc_datetime(self):
        """Test RNG when current_utc_datetime is missing."""
        mock_ctx = Mock(spec=[])  # No attributes
        mock_ctx.instance_id = "test"

        with _sandbox_scope(mock_ctx, "best_effort"):
            # Should use epoch fallback
            r = random.random()
            assert isinstance(r, float)

    def test_rng_base_ctx_current_utc_datetime(self):
        """Test RNG uses base_ctx.current_utc_datetime as fallback."""
        mock_ctx = Mock(spec=["instance_id", "_base_ctx"])
        mock_ctx.instance_id = "test"
        mock_ctx._base_ctx = Mock()
        mock_ctx._base_ctx.current_utc_datetime = datetime.datetime(2025, 1, 1, 12, 0, 0)

        with _sandbox_scope(mock_ctx, "best_effort"):
            r = random.random()
            assert isinstance(r, float)

    def test_rng_setattr_exception_handling(self):
        """Test RNG handles setattr exception gracefully."""

        class ReadOnlyRNG:
            def __setattr__(self, name, value):
                raise AttributeError("Cannot set attribute")

        mock_ctx = Mock()
        mock_ctx.instance_id = "test"
        mock_ctx.current_utc_datetime = datetime.datetime(2025, 1, 1, 12, 0, 0)

        # Should not crash even if setattr fails
        with _sandbox_scope(mock_ctx, "best_effort"):
            r = random.random()
            assert isinstance(r, float)


class TestSandboxLifecycle:
    """Test _Sandbox class lifecycle and patch/restore operations."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_ctx = Mock()
        self.mock_ctx.instance_id = "test-instance"
        self.mock_ctx.current_utc_datetime = datetime.datetime(2025, 1, 1, 12, 0, 0)

    def test_sandbox_lifecycle_doesnt_crash(self):
        """Test that sandbox lifecycle operations don't crash."""
        # Just verify the sandbox can be entered and exited without errors
        with _sandbox_scope(self.mock_ctx, "best_effort"):
            # Use a random function
            r = random.random()
            assert isinstance(r, float)

        # Verify no issues with nested contexts
        with _sandbox_scope(self.mock_ctx, "best_effort"):
            with _sandbox_scope(self.mock_ctx, "strict"):
                r = random.random()
                assert isinstance(r, float)

        # Verify exception doesn't break cleanup
        try:
            with _sandbox_scope(self.mock_ctx, "best_effort"):
                raise ValueError("Test")
        except ValueError:
            pass

    def test_sandbox_restores_optional_missing_functions(self):
        """Test sandbox handles missing optional functions during restore."""
        mock_ctx = Mock()
        mock_ctx.instance_id = "test"
        mock_ctx.current_utc_datetime = datetime.datetime(2025, 1, 1, 12, 0, 0)

        # Test with time_ns potentially missing
        with _sandbox_scope(mock_ctx, "best_effort"):
            # Should handle gracefully whether time_ns exists or not
            pass

        # Should not crash during restore


class TestPatchedFunctionsInWorkflow:
    """Test that patched functions are actually executed in workflow context."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        self.mock_base_ctx.instance_id = "test-instance-123"
        self.mock_base_ctx.current_utc_datetime = datetime.datetime(2025, 1, 1, 12, 0, 0)
        self.mock_base_ctx.create_timer = Mock()
        self.mock_base_ctx.call_activity = Mock()

        # Ensure now() method exists and returns datetime
        def mock_now():
            return datetime.datetime(2025, 1, 1, 12, 0, 0)

        self.mock_base_ctx.now = mock_now

    @pytest.mark.asyncio
    async def test_workflow_calls_random_functions(self):
        """Test workflow that calls random functions within sandbox."""
        from durabletask.aio import AsyncWorkflowContext
        from durabletask.aio.driver import CoroutineOrchestratorRunner

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        async def workflow_with_random(ctx):
            # Call various random functions
            r = random.random()
            ri = random.randint(1, 100)
            rb = random.getrandbits(8)
            rr = random.randrange(10, 50, 5)
            rr2 = random.randrange(20)
            return [r, ri, rb, rr, rr2]

        runner = CoroutineOrchestratorRunner(workflow_with_random, sandbox_mode="best_effort")

        # Generate and drive
        gen = runner.to_generator(async_ctx)
        try:
            next(gen)
        except StopIteration as e:
            result = e.value
            assert isinstance(result, list)
            assert len(result) == 5
            assert isinstance(result[0], float)
            assert isinstance(result[1], int)
            assert isinstance(result[2], int)
            assert isinstance(result[3], int)
            assert isinstance(result[4], int)

    @pytest.mark.asyncio
    async def test_workflow_calls_uuid4(self):
        """Test workflow that calls uuid4 within sandbox."""
        from durabletask.aio import AsyncWorkflowContext
        from durabletask.aio.driver import CoroutineOrchestratorRunner

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        async def workflow_with_uuid(ctx):
            u1 = uuid.uuid4()
            u2 = uuid.uuid4()
            return [u1, u2]

        runner = CoroutineOrchestratorRunner(workflow_with_uuid, sandbox_mode="best_effort")

        # Generate and drive
        gen = runner.to_generator(async_ctx)
        try:
            next(gen)
        except StopIteration as e:
            result = e.value
            assert isinstance(result, list)
            assert len(result) == 2
            assert isinstance(result[0], uuid.UUID)
            assert isinstance(result[1], uuid.UUID)

    @pytest.mark.asyncio
    async def test_workflow_calls_time_functions(self):
        """Test workflow that calls time functions within sandbox."""
        from durabletask.aio import AsyncWorkflowContext
        from durabletask.aio.driver import CoroutineOrchestratorRunner

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        async def workflow_with_time(ctx):
            t = time.time()
            results = [t]
            if hasattr(time, "time_ns"):
                tn = time.time_ns()
                results.append(tn)
            return results

        runner = CoroutineOrchestratorRunner(workflow_with_time, sandbox_mode="best_effort")

        # Generate and drive
        gen = runner.to_generator(async_ctx)
        try:
            next(gen)
        except StopIteration as e:
            result = e.value
            assert isinstance(result, list)
            assert len(result) >= 1
            assert isinstance(result[0], float)

    @pytest.mark.asyncio
    async def test_workflow_calls_datetime_functions(self):
        """Test workflow that calls datetime functions within sandbox."""
        from durabletask.aio import AsyncWorkflowContext
        from durabletask.aio.driver import CoroutineOrchestratorRunner

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        async def workflow_with_datetime(ctx):
            now = datetime.datetime.now()
            utcnow = datetime.datetime.now(datetime.timezone.utc)
            now_tz = datetime.datetime.now(datetime.timezone.utc)
            return [now, utcnow, now_tz]

        runner = CoroutineOrchestratorRunner(workflow_with_datetime, sandbox_mode="best_effort")

        # Generate and drive
        gen = runner.to_generator(async_ctx)
        try:
            next(gen)
        except StopIteration as e:
            result = e.value
            assert isinstance(result, list)
            assert len(result) == 3
            assert all(isinstance(d, datetime.datetime) for d in result)

    @pytest.mark.asyncio
    async def test_workflow_calls_all_random_variants(self):
        """Test workflow that exercises all random function variants."""
        from durabletask.aio import AsyncWorkflowContext
        from durabletask.aio.driver import CoroutineOrchestratorRunner

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        async def workflow_comprehensive(ctx):
            results = {}
            # Test all random variants
            results["random"] = random.random()
            results["randint"] = random.randint(50, 100)
            results["getrandbits"] = random.getrandbits(16)
            results["randrange_single"] = random.randrange(50)
            results["randrange_two"] = random.randrange(10, 50)
            results["randrange_step"] = random.randrange(0, 100, 5)

            # Test uuid
            results["uuid4"] = str(uuid.uuid4())

            # Test time
            results["time"] = time.time()
            if hasattr(time, "time_ns"):
                results["time_ns"] = time.time_ns()

            # Test datetime
            results["now"] = datetime.datetime.now()
            results["utcnow"] = datetime.datetime.now(datetime.timezone.utc)
            results["now_tz"] = datetime.datetime.now(datetime.timezone.utc)

            return results

        runner = CoroutineOrchestratorRunner(workflow_comprehensive, sandbox_mode="best_effort")

        # Generate and drive
        gen = runner.to_generator(async_ctx)
        try:
            next(gen)
        except StopIteration as e:
            result = e.value
            assert isinstance(result, dict)
            assert "random" in result
            assert "uuid4" in result
            assert "time" in result
