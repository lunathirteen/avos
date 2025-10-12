from abc import ABC, abstractmethod
from typing import List, Iterable, Union
import hashlib
import random


class BaseSplitter(ABC):
    """
    Abstract base class for all splitters.
    """

    @abstractmethod
    def assign_variant(self, unit_id: Union[str, int], variants: List[str], allocations: Iterable[float]) -> str:
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


class RandomSplitter(BaseSplitter):
    """
    Randomized assignment (not deterministic, not stable between runs!).
    """

    def assign_variant(self, unit_id, variants, allocations):
        allocations = list(allocations)
        if len(variants) != len(allocations):
            raise ValueError("Mismatched variants and allocations")
        total = sum(allocations)
        if abs(total - 1.0) > 1e-6:
            raise ValueError("Allocations must sum to 1.0")
        # Use random.choices for weighted selection
        return random.choices(variants, weights=allocations, k=1)[0]


class HashBasedSplitter(BaseSplitter):
    """
    Hash-based deterministic variant allocation.
    """

    def __init__(self, experiment_id: str):
        self.exp_id = experiment_id

    def assign_variant(self, unit_id: Union[str, int], variants: List[str], allocations: Iterable[float]) -> str:
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
        base_string = f"{unit_id}{self.exp_id}"
        digest = hashlib.md5(base_string.encode()).hexdigest()
        val = int(digest, 16) / 2**128

        for variant, boundary in buckets:
            if val < boundary:
                return variant
        return variants[-1]  # Fallback if rounding edge-case


class SegmentedSplitter(BaseSplitter):
    """
    Segment-based deterministic assignment.
    Each segment can have its own allocations.
    Example: { "US": [0.5, 0.5], "UK": [0.7, 0.3], ... }
    """

    def __init__(self, experiment_id: str, segment_allocations: dict):
        self.exp_id = experiment_id
        self.segment_allocations = segment_allocations  # {segment: ([variants], [allocations])}

    def assign_variant(self, unit_id, variants, allocations, segment=None):
        if segment is None or segment not in self.segment_allocations:
            raise ValueError(f"Required segment for assignment!")
        seg_variants, seg_allocs = self.segment_allocations[segment]
        if len(seg_variants) != len(seg_allocs):
            raise ValueError("Segment allocations misconfigured")

        # Use hash-based deterministic split within segment
        base_string = f"{unit_id}{self.exp_id}{segment}"

        digest = hashlib.md5(base_string.encode()).hexdigest()
        val = int(digest, 16) / 2**128

        # Buckets
        buckets, cumulative = [], 0.0
        for v, a in zip(seg_variants, seg_allocs):
            cumulative += a
            buckets.append((v, cumulative))
        for v, upper in buckets:
            if val < upper:
                return v
        return seg_variants[-1]


class StratifiedSplitter(BaseSplitter):
    """
    Stratified splitter—guarantees ratio within each stratum.
    stratum_allocations: {stratum: ([variants], [allocations])}
    """

    def __init__(self, experiment_id: str, stratum_allocations: dict):
        self.exp_id = experiment_id
        self.stratum_allocations = stratum_allocations

    def assign_variant(self, unit_id, variants, allocations, stratum=None):
        if stratum is None or stratum not in self.stratum_allocations:
            raise ValueError("Stratum required for stratified split!")
        stratum_variants, stratum_allocs = self.stratum_allocations[stratum]
        if len(stratum_variants) != len(stratum_allocs):
            raise ValueError("Stratum allocation misconfigured")
        # Deterministic hash (add stratum to salt)
        base_string = f"{unit_id}{self.exp_id}{stratum}"
        import hashlib

        digest = hashlib.md5(base_string.encode()).hexdigest()
        val = int(digest, 16) / 2**128
        buckets, cumulative = [], 0.0
        for v, a in zip(stratum_variants, stratum_allocs):
            cumulative += a
            buckets.append((v, cumulative))
        for v, upper in buckets:
            if val < upper:
                return v
        return stratum_variants[-1]


class GeoBasedSplitter(BaseSplitter):
    """
    Geo-based deterministic assignment (by country/region).
    geo_allocations: {geo: ([variants], [allocations])}
    """

    def __init__(self, experiment_id: str, geo_allocations: dict):
        self.exp_id = experiment_id
        self.geo_allocations = geo_allocations

    def assign_variant(self, unit_id, variants, allocations, geo=None):
        if geo is None or geo not in self.geo_allocations:
            raise ValueError("Geo required for geo-based split!")
        geo_variants, geo_allocs = self.geo_allocations[geo]
        if len(geo_variants) != len(geo_allocs):
            raise ValueError("Geo allocation misconfigured")
        base_string = f"{unit_id}{self.exp_id}{geo}"
        import hashlib

        digest = hashlib.md5(base_string.encode()).hexdigest()
        val = int(digest, 16) / 2**128
        buckets, cumulative = [], 0.0
        for v, a in zip(geo_variants, geo_allocs):
            cumulative += a
            buckets.append((v, cumulative))
        for v, upper in buckets:
            if val < upper:
                return v
        return geo_variants[-1]
