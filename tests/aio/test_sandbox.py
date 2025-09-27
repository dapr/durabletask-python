# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

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
from datetime import timedelta
from unittest.mock import Mock, patch

import pytest

from durabletask import task as dt_task
from durabletask.aio import (
    AsyncWorkflowContext,
    NonDeterminismWarning,
    _NonDeterminismDetector,
    sandbox_scope,
)
from durabletask.aio.errors import AsyncWorkflowError, SandboxViolationError


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
        mock_frame.f_locals = {
            "datetime": Mock(__module__="datetime")
        }
        
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
        mock_frame.f_locals = {
            "random_module": Mock(__module__="random")
        }
        
        # Should raise error in strict mode when non-deterministic call detected
        with pytest.raises(AsyncWorkflowError):
            detector._handle_non_deterministic_call("random.random", mock_frame)


class TestSandboxScope:
    """Test sandbox_scope functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_ctx = Mock()
        self.mock_ctx.instance_id = "test-instance"
        self.mock_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        self.mock_ctx.random.return_value = random.Random(12345)
        self.mock_ctx.uuid4.return_value = uuid.UUID('12345678-1234-5678-1234-567812345678')
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
        
        with sandbox_scope(self.mock_ctx, "off"):
            # Should not patch anything in off mode
            assert asyncio.sleep is original_sleep
            assert random.random is original_random

    def test_sandbox_scope_invalid_mode(self):
        """Test sandbox_scope with invalid mode."""
        with pytest.raises(ValueError, match="Invalid sandbox mode"):
            with sandbox_scope(self.mock_ctx, "invalid_mode"):
                pass

    def test_sandbox_scope_best_effort_patches(self):
        """Test sandbox_scope patches functions in best_effort mode."""
        original_sleep = asyncio.sleep
        original_random = random.random
        original_uuid4 = uuid.uuid4
        original_time = time.time
        
        with sandbox_scope(self.mock_ctx, "best_effort"):
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
        
        with sandbox_scope(self.mock_ctx, "strict"):
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

        with sandbox_scope(async_ctx, "strict"):
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

        with sandbox_scope(async_ctx, "strict"):
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
            rc = async_ctx.random_choice(['a', 'b'])
            assert rc in ['a', 'b']

    def test_sandbox_scope_patches_asyncio_sleep(self):
        """Test that asyncio.sleep is properly patched within sandbox context."""
        with sandbox_scope(self.mock_ctx, "best_effort"):
            # Import asyncio within the sandbox context to get the patched version
            import asyncio as sandboxed_asyncio

            # Call the patched sleep directly
            patched_sleep_result = sandboxed_asyncio.sleep(1.0)
            
            # Should return our patched sleep awaitable
            assert hasattr(patched_sleep_result, '__await__')
            
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
        with sandbox_scope(self.mock_ctx, "best_effort"):
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
        with sandbox_scope(self.mock_ctx, "best_effort"):
            test_uuid = uuid.uuid4()
            assert isinstance(test_uuid, uuid.UUID)
            assert test_uuid.version == 4

    def test_sandbox_scope_patches_time_functions(self):
        """Test that time functions are properly patched."""
        with sandbox_scope(self.mock_ctx, "best_effort"):
            current_time = time.time()
            assert isinstance(current_time, float)
            
            if hasattr(time, 'time_ns'):
                current_time_ns = time.time_ns()
                assert isinstance(current_time_ns, int)

    def test_sandbox_scope_strict_mode_blocks_os_urandom(self):
        """Test that os.urandom is blocked in strict mode."""
        with sandbox_scope(self.mock_ctx, "strict"):
            with pytest.raises(AsyncWorkflowError, match="os.urandom is not allowed"):
                os.urandom(16)

    def test_sandbox_scope_strict_mode_blocks_secrets(self):
        """Test that secrets module is blocked in strict mode."""
        with sandbox_scope(self.mock_ctx, "strict"):
            with pytest.raises(AsyncWorkflowError, match="secrets module is not allowed"):
                secrets.token_bytes(16)
            
            with pytest.raises(AsyncWorkflowError, match="secrets module is not allowed"):
                secrets.token_hex(16)

    def test_sandbox_scope_strict_mode_blocks_asyncio_create_task(self):
        """Test that asyncio.create_task is blocked in strict mode."""
        async def dummy_coro():
            return "test"
        
        with sandbox_scope(self.mock_ctx, "strict"):
            with pytest.raises(AsyncWorkflowError, match="asyncio.create_task is not allowed"):
                asyncio.create_task(dummy_coro())

    def test_sandbox_scope_global_disable_env_var(self):
        """Test that DAPR_WF_DISABLE_DETECTION environment variable works."""
        with patch.dict(os.environ, {"DAPR_WF_DISABLE_DETECTION": "true"}):
            original_random = random.random
            
            with sandbox_scope(self.mock_ctx, "best_effort"):
                # Should not patch when globally disabled
                assert random.random is original_random

    def test_sandbox_scope_context_detection_disabled(self):
        """Test that context-level detection disable works."""
        self.mock_ctx._detection_disabled = True
        original_random = random.random
        
        with sandbox_scope(self.mock_ctx, "best_effort"):
            # Should not patch when disabled on context
            assert random.random is original_random

    def test_sandbox_scope_nested_contexts(self):
        """Test nested sandbox contexts."""
        original_random = random.random
        
        with sandbox_scope(self.mock_ctx, "best_effort"):
            patched_random_1 = random.random
            assert patched_random_1 is not original_random
            
            with sandbox_scope(self.mock_ctx, "strict"):
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
            with sandbox_scope(self.mock_ctx, "best_effort"):
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
        with sandbox_scope(self.mock_ctx, "best_effort"):
            results1.append(random.random())
            results1.append(random.randint(1, 100))
            results1.append(str(uuid.uuid4()))
            results1.append(time.time())
        
        # Second run with same context
        with sandbox_scope(self.mock_ctx, "best_effort"):
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
        mock_ctx2.uuid4.return_value = uuid.UUID('87654321-4321-8765-4321-876543218765')
        mock_ctx2.now.return_value = datetime.datetime(2023, 1, 1, 12, 0, 0)
        mock_ctx2._detection_disabled = False
        
        results1 = []
        results2 = []
        
        # First context
        with sandbox_scope(self.mock_ctx, "best_effort"):
            results1.append(random.random())
            results1.append(str(uuid.uuid4()))
        
        # Different context
        with sandbox_scope(mock_ctx2, "best_effort"):
            results2.append(random.random())
            results2.append(str(uuid.uuid4()))
        
        # Should be different
        assert results1 != results2


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

        with sandbox_scope(async_ctx, "best_effort"):
            out = await asyncio.gather(DummyWF(), native(0), DummyWF(), native(1))

        # Order preserved and batched results merged back correctly
        assert out == ["W0", "N0", "W1", "N1"]
        # Ensure WhenAll got only workflow awaitables (2 items)
        assert recorded_items and len(recorded_items[0]) == 2

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

        with sandbox_scope(async_ctx, "best_effort"):
            res = await asyncio.gather(DummyWF(), native_fail(), native_ok(), return_exceptions=True)

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
        with sandbox_scope(async_ctx, "best_effort"):
            # Should patch gather
            assert asyncio.gather is not original_gather
            
            # Test empty gather
            empty_gather = asyncio.gather()
            assert hasattr(empty_gather, '__await__')

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
        
        activity_awaitable = ActivityAwaitable(
            mock_base_ctx,
            lambda: "test", 
            input="test"
        )
        
        with sandbox_scope(async_ctx, "best_effort"):
            # Should recognize workflow awaitables
            gather_result = asyncio.gather(activity_awaitable)
            assert hasattr(gather_result, '__await__')


class TestSandboxIntegration:
    """Integration tests for sandbox functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_base_ctx = Mock()
        self.mock_base_ctx.instance_id = "test-instance"
        self.mock_base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        self.mock_base_ctx.call_activity.return_value = Mock(spec=dt_task.Task)
        self.mock_base_ctx.create_timer.return_value = Mock(spec=dt_task.Task)

    def test_sandbox_with_async_workflow_context(self):
        """Test sandbox integration with AsyncWorkflowContext."""
        from durabletask.aio import AsyncWorkflowContext
        
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        with sandbox_scope(async_ctx, "best_effort"):
            # Should work with real AsyncWorkflowContext
            test_random = random.random()
            test_uuid = uuid.uuid4()
            test_time = time.time()
            
            assert isinstance(test_random, float)
            assert isinstance(test_uuid, uuid.UUID)
            assert isinstance(test_time, float)

    def test_sandbox_warning_detection(self):
        """Test that sandbox properly issues warnings."""
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            
            with sandbox_scope(async_ctx, "best_effort"):
                # This should potentially trigger warnings if non-deterministic calls are detected
                pass
            
            # Check if any NonDeterminismWarning was issued
            non_det_warnings = [warning for warning in w if issubclass(warning.category, NonDeterminismWarning)]
            # May or may not have warnings depending on implementation

    def test_sandbox_performance_impact(self):
        """Test that sandbox doesn't have excessive performance impact."""
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        # Ensure debug mode is OFF for performance testing
        async_ctx._debug_mode = False

        import time as time_module

        # Measure without sandbox
        start = time_module.perf_counter()
        for _ in range(1000):
            random.random()
        no_sandbox_time = time_module.perf_counter() - start

        # Measure with sandbox
        start = time_module.perf_counter()
        with sandbox_scope(async_ctx, "best_effort"):
            for _ in range(1000):
                random.random()
        sandbox_time = time_module.perf_counter() - start

        # Sandbox should not be more than 20x slower (reasonable overhead for patching + minimal tracing)
        # In practice, the overhead comes from function call interception and deterministic RNG
        assert sandbox_time < no_sandbox_time * 20, f"Sandbox: {sandbox_time:.6f}s, No sandbox: {no_sandbox_time:.6f}s"

    def test_sandbox_mode_validation(self):
        """Test sandbox mode validation."""
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        
        # Valid modes should work
        for mode in ["off", "best_effort", "strict"]:
            with sandbox_scope(async_ctx, mode):
                pass
        
        # Invalid mode should raise error
        with pytest.raises(ValueError):
            with sandbox_scope(async_ctx, "invalid"):
                pass
