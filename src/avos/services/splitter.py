from abc import ABC, abstractmethod
from typing import List, Iterable, Union
import hashlib


class BaseSplitter(ABC):
    """
    Abstract base class for all splitters.
    """

    @abstractmethod
    def assign_variant(
        self,
        unit_id: Union[str, int],
        variants: List[str],
        allocations: Iterable[float]
    ) -> str:
        """
        Assign a unit to a variant based on provided allocations.
        Args:
            unit_id: Unique identifier for the subject (user/session/etc).
            variants: List of variant names.
            allocations: Iterable of allocation fractions (should sum to 1.0).
        Returns:
            variant name assigned to this unit.
        """
        pass

class HashBasedSplitter(BaseSplitter):
    """
    Hash-based deterministic variant allocation.
    """

    def __init__(self, experiment_id: str):
        self.exp_id = experiment_id

    def assign_variant(
        self,
        unit_id: Union[str, int],
        variants: List[str],
        allocations: Iterable[float]
    ) -> str:
        # Validate inputs
        allocations = list(allocations)
        if not variants or len(variants) != len(allocations):
            raise ValueError("Variants and allocations must have the same length")
        total = sum(allocations)
        if abs(total - 1.0) > 1e-6:
            raise ValueError("Allocations must sum to 1.0")

        # Compute bucket boundaries
        buckets = []
        cumulative = 0.0
        for variant, alloc in zip(variants, allocations):
            cumulative += alloc
            buckets.append((variant, cumulative))

        # Hash to [0, 1)
        digest = hashlib.md5(f"{unit_id}{self.exp_id}".encode()).hexdigest()
        val = int(digest, 16) / 2**128

        for variant, boundary in buckets:
            if val < boundary:
                return variant
        return variants[-1]  # Fallback if rounding edge-case
