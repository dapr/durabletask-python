# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Deterministic utilities for async workflows.

This module provides deterministic alternatives to non-deterministic Python
functions, ensuring workflow replay consistency across different executions.
"""

from __future__ import annotations

import hashlib
import random
import string as _string
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, Sequence, TypeVar, runtime_checkable


@dataclass
class DeterminismSeed:
    """Seed data for deterministic operations."""
    instance_id: str
    orchestration_unix_ts: int
    
    def to_int(self) -> int:
        """Convert seed to integer for PRNG initialization."""
        # Create a deterministic hash from instance_id and timestamp
        combined = f"{self.instance_id}:{self.orchestration_unix_ts}"
        hash_bytes = hashlib.sha256(combined.encode('utf-8')).digest()
        # Convert first 8 bytes to int for seed
        return int.from_bytes(hash_bytes[:8], byteorder='big')


def derive_seed(instance_id: str, orchestration_time: datetime) -> int:
    """
    Derive a deterministic seed from instance ID and orchestration time.
    
    Args:
        instance_id: The workflow instance identifier
        orchestration_time: The current orchestration time
        
    Returns:
        A deterministic integer seed
    """
    ts = int(orchestration_time.timestamp())
    return DeterminismSeed(instance_id=instance_id, orchestration_unix_ts=ts).to_int()


def deterministic_random(instance_id: str, orchestration_time: datetime) -> random.Random:
    """
    Create a deterministic random number generator.
    
    Args:
        instance_id: The workflow instance identifier
        orchestration_time: The current orchestration time
        
    Returns:
        A seeded Random instance that will produce consistent results
    """
    seed = derive_seed(instance_id, orchestration_time)
    return random.Random(seed)


def deterministic_uuid4(rnd: random.Random) -> uuid.UUID:
    """
    Generate a deterministic UUID4 using the provided random generator.
    
    Args:
        rnd: A seeded Random instance
        
    Returns:
        A UUID4 that will be consistent for the same random state
    """
    # Generate 16 random bytes
    bytes_ = bytes(rnd.randrange(0, 256) for _ in range(16))
    
    # Set version (4) and variant bits to make it a proper UUID4
    # Version 4: bits 12-15 of time_hi_and_version should be 0100
    bytes_list = list(bytes_)
    bytes_list[6] = (bytes_list[6] & 0x0f) | 0x40  # Version 4
    bytes_list[8] = (bytes_list[8] & 0x3f) | 0x80  # Variant bits
    
    return uuid.UUID(bytes=bytes(bytes_list))


@runtime_checkable
class DeterministicContextProtocol(Protocol):
    """Protocol for contexts that provide deterministic operations."""
    
    @property
    def instance_id(self) -> str:
        """The workflow instance identifier."""
        ...
    
    @property
    def current_utc_datetime(self) -> datetime:
        """The current orchestration time."""
        ...


class DeterministicContextMixin:
    """
    Mixin providing deterministic helpers for workflow contexts.
    
    Assumes the inheriting class exposes `instance_id` and `current_utc_datetime` attributes
    or properties that conform to the DeterministicContextProtocol.
    """
    
    def now(self) -> datetime:
        """
        Return orchestration time (deterministic current UTC time not local time as is with datetime.now()).
        An alias for `current_utc_datetime` to make it similar to python's `datetime.now()` and `datetime.utcnow()`.
        
        This should be used instead of datetime.now() or datetime.utcnow()
        to ensure deterministic behavior during workflow replay.
        
        Returns:
            The current orchestration time
        """
        # mypy: the concrete context provides a datetime-typed current_utc_datetime
        value = self.current_utc_datetime  # type: ignore[attr-defined]
        assert isinstance(value, datetime)
        return value
    
    def random(self) -> random.Random:
        """
        Return a PRNG seeded deterministically from instance id and orchestration time.
        
        This should be used instead of the global random module to ensure
        deterministic behavior during workflow replay.
        
        Returns:
            A seeded Random instance
        """
        return deterministic_random(
            self.instance_id,  # type: ignore[attr-defined]
            self.current_utc_datetime,  # type: ignore[attr-defined]
        )
    
    def uuid4(self) -> uuid.UUID:
        """
        Return a deterministically generated UUID using the deterministic PRNG.
        
        This should be used instead of uuid.uuid4() to ensure deterministic
        behavior during workflow replay.
        
        Returns:
            A deterministic UUID4
        """
        rnd = self.random()
        return deterministic_uuid4(rnd)
    
    def new_guid(self) -> uuid.UUID:
        """
        Alias for uuid4 for API parity with other SDKs.
        
        Returns:
            A deterministic UUID4
        """
        return self.uuid4()
    
    def random_string(self, length: int, *, alphabet: str | None = None) -> str:
        """
        Return a deterministically generated random string of the given length.
        
        This should be used instead of generating random strings with the
        global random module to ensure deterministic behavior during replay.
        
        Args:
            length: Desired length of the string. Must be >= 0.
            alphabet: Optional set of characters to sample from. 
                     Defaults to ASCII letters + digits.
                     
        Returns:
            A deterministic random string
            
        Raises:
            ValueError: If length is negative or alphabet is empty
        """
        if length < 0:
            raise ValueError('length must be non-negative')
        
        chars = alphabet if alphabet is not None else (_string.ascii_letters + _string.digits)
        if not chars:
            raise ValueError('alphabet must not be empty')
        
        rnd = self.random()
        size = len(chars)
        return ''.join(chars[rnd.randrange(0, size)] for _ in range(length))
    
    def random_int(self, min_value: int = 0, max_value: int = 2**31 - 1) -> int:
        """
        Return a deterministic random integer in the specified range.
        
        Args:
            min_value: Minimum value (inclusive)
            max_value: Maximum value (inclusive)
            
        Returns:
            A deterministic random integer
            
        Raises:
            ValueError: If min_value > max_value
        """
        if min_value > max_value:
            raise ValueError('min_value must be <= max_value')
        
        rnd = self.random()
        return rnd.randint(min_value, max_value)
    
    T = TypeVar("T")

    def random_choice(self, sequence: Sequence[T]) -> T:
        """
        Return a deterministic random element from a non-empty sequence.
        
        Args:
            sequence: A non-empty sequence to choose from
            
        Returns:
            A deterministic random element from the sequence
            
        Raises:
            IndexError: If sequence is empty
        """
        if not sequence:
            raise IndexError('Cannot choose from empty sequence')
        
        rnd = self.random()
        return rnd.choice(sequence)
