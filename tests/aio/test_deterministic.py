# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Tests for deterministic utilities in durabletask.aio.
"""

import random
import uuid
from datetime import datetime

import pytest

from durabletask.aio import (
    DeterminismSeed,
    DeterministicContextMixin,
    derive_seed,
    deterministic_random,
    deterministic_uuid4,
)


class TestDeterminismSeed:
    """Test DeterminismSeed functionality."""

    def test_seed_creation(self):
        """Test creating a determinism seed."""
        seed = DeterminismSeed(instance_id="test-123", orchestration_unix_ts=1234567890)
        assert seed.instance_id == "test-123"
        assert seed.orchestration_unix_ts == 1234567890

    def test_seed_to_int(self):
        """Test converting seed to integer."""
        seed = DeterminismSeed(instance_id="test-123", orchestration_unix_ts=1234567890)
        int_seed = seed.to_int()
        assert isinstance(int_seed, int)
        assert int_seed > 0

    def test_seed_deterministic(self):
        """Test that same inputs produce same seed."""
        seed1 = DeterminismSeed(instance_id="test-123", orchestration_unix_ts=1234567890)
        seed2 = DeterminismSeed(instance_id="test-123", orchestration_unix_ts=1234567890)
        assert seed1.to_int() == seed2.to_int()

    def test_seed_different_inputs(self):
        """Test that different inputs produce different seeds."""
        seed1 = DeterminismSeed(instance_id="test-123", orchestration_unix_ts=1234567890)
        seed2 = DeterminismSeed(instance_id="test-456", orchestration_unix_ts=1234567890)
        seed3 = DeterminismSeed(instance_id="test-123", orchestration_unix_ts=1234567891)

        assert seed1.to_int() != seed2.to_int()
        assert seed1.to_int() != seed3.to_int()
        assert seed2.to_int() != seed3.to_int()


class TestDeriveSeed:
    """Test derive_seed function."""

    def test_derive_seed(self):
        """Test deriving seed from instance ID and datetime."""
        dt = datetime(2023, 1, 1, 12, 0, 0)
        seed = derive_seed("test-instance", dt)
        assert isinstance(seed, int)
        assert seed > 0

    def test_derive_seed_deterministic(self):
        """Test that same inputs produce same seed."""
        dt = datetime(2023, 1, 1, 12, 0, 0)
        seed1 = derive_seed("test-instance", dt)
        seed2 = derive_seed("test-instance", dt)
        assert seed1 == seed2

    def test_derive_seed_different_inputs(self):
        """Test that different inputs produce different seeds."""
        dt1 = datetime(2023, 1, 1, 12, 0, 0)
        dt2 = datetime(2023, 1, 1, 12, 0, 1)

        seed1 = derive_seed("test-instance", dt1)
        seed2 = derive_seed("different-instance", dt1)
        seed3 = derive_seed("test-instance", dt2)

        assert seed1 != seed2
        assert seed1 != seed3
        assert seed2 != seed3


class TestDeterministicRandom:
    """Test deterministic random generation."""

    def test_deterministic_random(self):
        """Test creating deterministic random generator."""
        dt = datetime(2023, 1, 1, 12, 0, 0)
        rng = deterministic_random("test-instance", dt)
        assert isinstance(rng, random.Random)

    def test_deterministic_random_reproducible(self):
        """Test that same inputs produce same random sequence."""
        dt = datetime(2023, 1, 1, 12, 0, 0)
        rng1 = deterministic_random("test-instance", dt)
        rng2 = deterministic_random("test-instance", dt)

        # Generate same sequence
        values1 = [rng1.random() for _ in range(10)]
        values2 = [rng2.random() for _ in range(10)]

        assert values1 == values2

    def test_deterministic_random_different_seeds(self):
        """Test that different inputs produce different sequences."""
        dt = datetime(2023, 1, 1, 12, 0, 0)
        rng1 = deterministic_random("test-instance-1", dt)
        rng2 = deterministic_random("test-instance-2", dt)

        # Generate sequences
        values1 = [rng1.random() for _ in range(10)]
        values2 = [rng2.random() for _ in range(10)]

        assert values1 != values2


class TestDeterministicUuid4:
    """Test deterministic UUID generation."""

    def test_deterministic_uuid4(self):
        """Test creating deterministic UUID."""
        dt = datetime(2023, 1, 1, 12, 0, 0)
        rng = deterministic_random("test-instance", dt)
        uuid_val = deterministic_uuid4(rng)

        assert isinstance(uuid_val, uuid.UUID)
        assert uuid_val.version == 4

    def test_deterministic_uuid4_reproducible(self):
        """Test that same RNG produces same UUID."""
        dt = datetime(2023, 1, 1, 12, 0, 0)
        rng1 = deterministic_random("test-instance", dt)
        rng2 = deterministic_random("test-instance", dt)

        uuid1 = deterministic_uuid4(rng1)
        uuid2 = deterministic_uuid4(rng2)

        assert uuid1 == uuid2

    def test_deterministic_uuid4_different_rngs(self):
        """Test that different RNGs produce different UUIDs."""
        dt = datetime(2023, 1, 1, 12, 0, 0)
        rng1 = deterministic_random("test-instance-1", dt)
        rng2 = deterministic_random("test-instance-2", dt)

        uuid1 = deterministic_uuid4(rng1)
        uuid2 = deterministic_uuid4(rng2)

        assert uuid1 != uuid2


class TestDeterministicContextMixin:
    """Test DeterministicContextMixin functionality."""

    def setup_method(self):
        """Set up test fixtures."""

        # Create a mock context that uses the mixin
        class MockContext(DeterministicContextMixin):
            def __init__(self):
                self.instance_id = "test-instance"
                self.current_utc_datetime = datetime(2023, 1, 1, 12, 0, 0)

        self.ctx = MockContext()

    def test_now(self):
        """Test now() method."""
        now = self.ctx.now()
        assert now == datetime(2023, 1, 1, 12, 0, 0)
        assert now is self.ctx.current_utc_datetime

    def test_random(self):
        """Test random() method."""
        rng = self.ctx.random()
        assert isinstance(rng, random.Random)

    def test_random_deterministic(self):
        """Test that random() is deterministic."""
        rng1 = self.ctx.random()
        rng2 = self.ctx.random()

        # Should produce same sequence
        values1 = [rng1.random() for _ in range(5)]
        values2 = [rng2.random() for _ in range(5)]

        assert values1 == values2

    def test_uuid4(self):
        """Test uuid4() method."""
        uuid_val = self.ctx.uuid4()
        assert isinstance(uuid_val, uuid.UUID)
        assert uuid_val.version == 4

    def test_uuid4_deterministic(self):
        """Test that uuid4() is deterministic."""
        uuid1 = self.ctx.uuid4()
        uuid2 = self.ctx.uuid4()

        # Each call to uuid4() creates a new random generator with the same seed,
        # so they should produce the same UUID (deterministic behavior)
        assert uuid1 == uuid2

        # Create new context with same parameters
        class MockContext(DeterministicContextMixin):
            def __init__(self):
                self.instance_id = "test-instance"
                self.current_utc_datetime = datetime(2023, 1, 1, 12, 0, 0)

        ctx2 = MockContext()
        uuid3 = ctx2.uuid4()

        # Should match the UUID from first context (same seed)
        assert uuid1 == uuid3

        # Test with different context parameters
        class DifferentMockContext(DeterministicContextMixin):
            def __init__(self):
                self.instance_id = "different-instance"
                self.current_utc_datetime = datetime(2023, 1, 1, 12, 0, 0)

        ctx3 = DifferentMockContext()
        uuid4 = ctx3.uuid4()

        # Should be different UUID (different seed)
        assert uuid1 != uuid4

    def test_new_guid(self):
        """Test new_guid() alias."""
        guid = self.ctx.new_guid()
        assert isinstance(guid, uuid.UUID)
        assert guid.version == 4

    def test_random_string(self):
        """Test random_string() method."""
        # Test default alphabet
        s1 = self.ctx.random_string(10)
        assert len(s1) == 10
        assert all(c.isalnum() for c in s1)

        # Test custom alphabet
        s2 = self.ctx.random_string(5, alphabet="ABC")
        assert len(s2) == 5
        assert all(c in "ABC" for c in s2)

    def test_random_string_deterministic(self):
        """Test that random_string() is deterministic."""
        s1 = self.ctx.random_string(10)

        # Create new context with same parameters
        class MockContext(DeterministicContextMixin):
            def __init__(self):
                self.instance_id = "test-instance"
                self.current_utc_datetime = datetime(2023, 1, 1, 12, 0, 0)

        ctx2 = MockContext()
        s2 = ctx2.random_string(10)

        assert s1 == s2

    def test_random_string_edge_cases(self):
        """Test random_string() edge cases."""
        # Zero length
        s = self.ctx.random_string(0)
        assert s == ""

        # Negative length should raise error
        with pytest.raises(ValueError, match="length must be non-negative"):
            self.ctx.random_string(-1)

        # Empty alphabet should raise error
        with pytest.raises(ValueError, match="alphabet must not be empty"):
            self.ctx.random_string(5, alphabet="")
