import hashlib
from typing import Iterable, List

class HashBasedSplitter:
    def __init__(self, experiment_id: str):
        self.exp_id = experiment_id

    def assign_variant(
        self, unit_id: str | int, variants: List[str], allocations: Iterable[float]
    ) -> str:
        buckets = []
        total = 0.0
        for v, p in zip(variants, allocations):
            buckets.append((v, total + p))
            total += p

        digest = hashlib.md5(f"{unit_id}{self.exp_id}".encode()).hexdigest()
        val = (int(digest, 16) % 10000) / 100.0  # 0-100

        for v, upper in buckets:
            if val < upper:
                return v
        return variants[-1]  # fallback
