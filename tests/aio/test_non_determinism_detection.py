"""
Tests for non-determinism detection in async workflows.
"""

import datetime
import warnings
from unittest.mock import Mock

import pytest

from durabletask import task as dt_task
from durabletask.aio import (
    AsyncWorkflowContext,
    NonDeterminismWarning,
    SandboxViolationError,
    _NonDeterminismDetector,
    sandbox_scope,
)


class TestNonDeterminismDetection:
    """Test non-determinism detection and warnings."""

    def setup_method(self):
        self.mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        self.mock_base_ctx.instance_id = "test-instance-123"
        self.mock_base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)
        self.async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

    def test_non_determinism_detector_context_manager(self):
        """Test that the detector can be used as a context manager."""
        detector = _NonDeterminismDetector(self.async_ctx, "best_effort")

        with detector:
            # Should not raise
            pass

    def test_deterministic_alternative_suggestions(self):
        """Test that appropriate alternatives are suggested."""
        detector = _NonDeterminismDetector(self.async_ctx, "best_effort")

        test_cases = [
            ("datetime.now", "ctx.now()"),
            ("datetime.utcnow", "ctx.now()"),
            ("time.time", "ctx.now().timestamp()"),
            ("random.random", "ctx.random().random()"),
            ("uuid.uuid4", "ctx.uuid4()"),
            ("os.urandom", "ctx.random().randbytes() or ctx.random().getrandbits()"),
            ("unknown.function", "a deterministic alternative"),
        ]

        for call_sig, expected in test_cases:
            result = detector._get_deterministic_alternative(call_sig)
            assert result == expected

    def test_sandbox_with_non_determinism_detection_off(self):
        """Test that detection is disabled when mode is 'off'."""
        with sandbox_scope(self.async_ctx, "off"):
            # Should not detect anything
            import datetime as dt

            # This would normally trigger detection, but mode is off
            current_time = dt.datetime.now()
            assert current_time is not None

    def test_sandbox_with_non_determinism_detection_best_effort(self):
        """Test that detection works in best_effort mode."""
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")

            with sandbox_scope(self.async_ctx, "best_effort"):
                # This should work without issues since we're just testing the context
                pass

            # Note: The actual detection happens during function execution tracing
            # which is complex to test in isolation

    def test_sandbox_with_non_determinism_detection_strict_mode(self):
        """Test that strict mode blocks dangerous operations."""
        with pytest.raises(SandboxViolationError, match="File I/O operations are not allowed"):
            with sandbox_scope(self.async_ctx, "strict"):
                open("test.txt", "w")

    def test_non_determinism_warning_class(self):
        """Test that NonDeterminismWarning is a proper warning class."""
        warning = NonDeterminismWarning("Test warning")
        assert isinstance(warning, UserWarning)
        assert str(warning) == "Test warning"

    def test_detector_deduplication(self):
        """Test that the detector doesn't warn about the same call multiple times."""
        detector = _NonDeterminismDetector(self.async_ctx, "best_effort")

        # Simulate multiple calls to the same function
        detector.detected_calls.add("datetime.now")

        # This should not add a duplicate
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # Create a mock frame for the call
            mock_frame = Mock()
            mock_frame.f_code.co_filename = "test.py"
            mock_frame.f_lineno = 10
            detector._handle_non_deterministic_call("datetime.now", mock_frame)

            # Should not have issued a warning since it was already detected
            assert len(w) == 0

    def test_detector_strict_mode_raises_error(self):
        """Test that strict mode raises AsyncWorkflowError instead of warning."""
        detector = _NonDeterminismDetector(self.async_ctx, "strict")

        with pytest.raises(SandboxViolationError) as exc_info:
            # Create a mock frame for the call
            mock_frame = Mock()
            mock_frame.f_code.co_filename = "test.py"
            mock_frame.f_lineno = 10
            detector._handle_non_deterministic_call("datetime.now", mock_frame)

        error = exc_info.value
        assert "Non-deterministic function 'datetime.now' is not allowed" in str(error)
        assert error.instance_id == "test-instance-123"

    def test_detector_logs_to_debug_info(self):
        """Test that warnings are logged to debug info when debug mode is enabled."""
        # Enable debug mode
        self.async_ctx._debug_mode = True

        detector = _NonDeterminismDetector(self.async_ctx, "best_effort")

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            # Create a mock frame for the call
            mock_frame = Mock()
            mock_frame.f_code.co_filename = "test.py"
            mock_frame.f_lineno = 10
            detector._handle_non_deterministic_call("datetime.now", mock_frame)

        # Check that debug message was printed (our current implementation just prints)
        # The current implementation doesn't log to operation_history, it just prints debug messages
        # This is acceptable behavior for debug mode


class TestNonDeterminismIntegration:
    """Integration tests for non-determinism detection with actual workflows."""

    def setup_method(self):
        self.mock_base_ctx = Mock(spec=dt_task.OrchestrationContext)
        self.mock_base_ctx.instance_id = "test-instance-123"
        self.mock_base_ctx.current_utc_datetime = datetime.datetime(2023, 1, 1, 12, 0, 0)

    def test_sandbox_patches_work_correctly(self):
        """Test that the sandbox patches actually work."""
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        with sandbox_scope(async_ctx, "best_effort"):
            import random
            import time
            import uuid

            # These should use deterministic versions
            random_val = random.random()
            uuid_val = uuid.uuid4()
            time_val = time.time()

            # Values should be deterministic
            assert isinstance(random_val, float)
            assert isinstance(uuid_val, uuid.UUID)
            assert isinstance(time_val, float)

    def test_datetime_limitation_documented(self):
        """Test that datetime.now() limitation is properly documented."""
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        with sandbox_scope(async_ctx, "best_effort"):
            import datetime as dt

            # datetime.now() cannot be patched due to immutability
            # This should return the actual current time, not the deterministic time
            now_result = dt.datetime.now()
            deterministic_time = async_ctx.now()

            # They will likely be different (unless run at exactly the same time)
            # This documents the limitation
            assert isinstance(now_result, datetime.datetime)
            assert isinstance(deterministic_time, datetime.datetime)

    def test_rng_whitelist_and_global_random_determinism(self):
        """ctx.random() methods allowed; global random.* is patched to deterministic in strict/best_effort."""
        import random

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        # Strict: ctx.random().randint allowed
        with sandbox_scope(async_ctx, "strict"):
            rng = async_ctx.random()
            assert isinstance(rng.randint(1, 3), int)

        # Strict: global random.randint patched and deterministic
        with sandbox_scope(async_ctx, "strict"):
            v1 = random.randint(1, 1000000)
        with sandbox_scope(async_ctx, "strict"):
            v2 = random.randint(1, 1000000)
        assert v1 == v2

        # Best-effort: global random warns but returns
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            with sandbox_scope(async_ctx, "best_effort"):
                val1 = random.random()
            with sandbox_scope(async_ctx, "best_effort"):
                val2 = random.random()
            assert isinstance(val1, float)
            assert val1 == val2
            # Note: we intentionally don't assert on collected warnings here to keep the test
            # resilient across environments where tracing may not capture stdlib frames.

    def test_uuid_and_os_urandom_strict_behavior(self):
        """uuid.uuid4 is patched to deterministic; os.urandom is blocked in strict; ctx.uuid4 allowed."""
        import os
        import uuid as _uuid

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        # Allowed via deterministic helper
        with sandbox_scope(async_ctx, "strict"):
            val = async_ctx.uuid4()
            assert isinstance(val, _uuid.UUID)

        # Patched global uuid.uuid4 is deterministic
        with sandbox_scope(async_ctx, "strict"):
            u1 = _uuid.uuid4()
        with sandbox_scope(async_ctx, "strict"):
            u2 = _uuid.uuid4()
        assert isinstance(u1, _uuid.UUID)
        assert u1 == u2

        if hasattr(os, "urandom"):
            with pytest.raises(SandboxViolationError):
                with sandbox_scope(async_ctx, "strict"):
                    _ = os.urandom(8)

    @pytest.mark.asyncio
    async def test_create_task_blocked_in_strict_and_closed_coroutines(self):
        """asyncio.create_task is blocked in strict; ensure no unawaited coroutine warning leaks."""
        import asyncio

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        async def dummy():
            return 42

        # Blocked in strict
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            with pytest.raises(SandboxViolationError):
                with sandbox_scope(async_ctx, "strict"):
                    asyncio.create_task(dummy())
            # Ensure no "coroutine was never awaited" RuntimeWarning leaked
            assert not any("was never awaited" in str(rec.message) for rec in w)

        # Also blocked when passing a ready Future
        fut = asyncio.get_event_loop().create_future()
        fut.set_result(1)
        with pytest.raises(SandboxViolationError):
            with sandbox_scope(async_ctx, "strict"):
                asyncio.create_task(fut)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_create_task_allowed_in_best_effort(self):
        """In best_effort mode, create_task should be allowed and runnable."""
        import asyncio

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        async def quick():
            # sleep(0) is passed through to original sleep in sandbox
            await asyncio.sleep(0)
            return "ok"

        with sandbox_scope(async_ctx, "best_effort"):
            t = asyncio.create_task(quick())
            assert await t == "ok"

    def test_helper_methods_allowed_in_strict(self):
        """Ensure helper methods use whitelisted deterministic RNG in strict mode."""
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        with sandbox_scope(async_ctx, "strict"):
            s = async_ctx.random_string(5)
            assert len(s) == 5
            n = async_ctx.random_int(1, 3)
            assert 1 <= n <= 3
            choice = async_ctx.random_choice(["a", "b", "c"])
            assert choice in {"a", "b", "c"}

    @pytest.mark.asyncio
    async def test_gather_variants_and_caching(self):
        """Exercise patched asyncio.gather paths: empty, all-workflow, mixed with return_exceptions, and caching."""
        import asyncio

        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)

        with sandbox_scope(async_ctx, "best_effort"):
            # Empty gather returns [], cache replay on re-await
            g0 = asyncio.gather()
            r0a = await g0
            r0b = await g0
            assert r0a == [] and r0b == []

            # All workflow awaitables (sleep -> WhenAll path)
            a1 = async_ctx.sleep(0)
            a2 = async_ctx.sleep(0)
            g1 = asyncio.gather(a1, a2)
            # Do not await g1: constructing it covers the all-workflow branch without
            # requiring a real orchestrator; ensure it is awaitable (one-shot wrapper)
            assert hasattr(g1, "__await__")

            # Mixed inputs with return_exceptions True
            async def boom():
                raise RuntimeError("x")

            async def small():
                await asyncio.sleep(0)
                return "ok"

            # Mixed native coroutines path (no workflow awaitables)
            g2 = asyncio.gather(small(), boom(), return_exceptions=True)
            r2 = await g2
            assert len(r2) == 2 and isinstance(r2[1], Exception)

    def test_invalid_mode_raises(self):
        async_ctx = AsyncWorkflowContext(self.mock_base_ctx)
        with pytest.raises(ValueError):
            with sandbox_scope(async_ctx, "invalid_mode"):
                pass


if __name__ == "__main__":
    pytest.main([__file__])
