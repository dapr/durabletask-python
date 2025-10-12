# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

"""
Deterministic utilities for Durable Task workflows (async and generator).

This module provides deterministic alternatives to non-deterministic Python
functions, ensuring workflow replay consistency across different executions.
It is shared by both the asyncio authoring model and the generator-based model.
"""

from __future__ import annotations

import hashlib
import random
import string as _string
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Protocol, TypeVar, runtime_checkable


@dataclass
class DeterminismSeed:
    """Seed data for deterministic operations."""

    instance_id: str
    orchestration_unix_ts: int

    def to_int(self) -> int:
        """Convert seed to integer for PRNG initialization."""
        combined = f"{self.instance_id}:{self.orchestration_unix_ts}"
        hash_bytes = hashlib.sha256(combined.encode("utf-8")).digest()
        return int.from_bytes(hash_bytes[:8], byteorder="big")


def derive_seed(instance_id: str, orchestration_time: datetime) -> int:
    """
    Derive a deterministic seed from instance ID and orchestration time.
    """
    ts = int(orchestration_time.timestamp())
    return DeterminismSeed(instance_id=instance_id, orchestration_unix_ts=ts).to_int()


def deterministic_random(instance_id: str, orchestration_time: datetime) -> random.Random:
    """
    Create a deterministic random number generator.
    """
    seed = derive_seed(instance_id, orchestration_time)
    return random.Random(seed)


def deterministic_uuid4(rnd: random.Random) -> uuid.UUID:
    """Generate a deterministic UUID4 using the provided random generator."""
    bytes_ = bytes(rnd.randrange(0, 256) for _ in range(16))
    bytes_list = list(bytes_)
    bytes_list[6] = (bytes_list[6] & 0x0F) | 0x40  # Version 4
    bytes_list[8] = (bytes_list[8] & 0x3F) | 0x80  # Variant bits
    return uuid.UUID(bytes=bytes(bytes_list))


@runtime_checkable
class DeterministicContextProtocol(Protocol):
    """Protocol for contexts that provide deterministic operations."""

    @property
    def instance_id(self) -> str: ...

    @property
    def current_utc_datetime(self) -> datetime: ...


class DeterministicContextMixin:
    """
    Mixin providing deterministic helpers for workflow contexts.

    Assumes the inheriting class exposes `instance_id` and `current_utc_datetime` attributes.
    """

    def now(self) -> datetime:
        """Return orchestration time (deterministic UTC)."""
        value = self.current_utc_datetime  # type: ignore[attr-defined]
        assert isinstance(value, datetime)
        return value

    def random(self) -> random.Random:
        """Return a PRNG seeded deterministically from instance id and orchestration time."""
        rnd = deterministic_random(
            self.instance_id,  # type: ignore[attr-defined]
            self.current_utc_datetime,  # type: ignore[attr-defined]
        )
        # Mark as deterministic for sandbox detector whitelisting of bound methods
        try:
            setattr(rnd, "_dt_deterministic", True)
        except Exception:
            pass
        return rnd

    def uuid4(self) -> uuid.UUID:
        """Return a deterministically generated UUID using the deterministic PRNG."""
        rnd = self.random()
        return deterministic_uuid4(rnd)

    def new_guid(self) -> uuid.UUID:
        """Alias for uuid4 for API parity with other SDKs."""
        return self.uuid4()

    def random_string(self, length: int, *, alphabet: Optional[str] = None) -> str:
        """Return a deterministically generated random string of the given length."""
        if length < 0:
            raise ValueError("length must be non-negative")
        chars = alphabet if alphabet is not None else (_string.ascii_letters + _string.digits)
        if not chars:
            raise ValueError("alphabet must not be empty")
        rnd = self.random()
        size = len(chars)
        return "".join(chars[rnd.randrange(0, size)] for _ in range(length))

    def random_int(self, min_value: int = 0, max_value: int = 2**31 - 1) -> int:
        """Return a deterministic random integer in the specified range."""
        if min_value > max_value:
            raise ValueError("min_value must be <= max_value")
        rnd = self.random()
        return rnd.randint(min_value, max_value)

    T = TypeVar("T")

    def random_choice(self, sequence: Sequence[T]) -> T:
        """Return a deterministic random element from a non-empty sequence."""
        if not sequence:
            raise IndexError("Cannot choose from empty sequence")
        rnd = self.random()
        return rnd.choice(sequence)
