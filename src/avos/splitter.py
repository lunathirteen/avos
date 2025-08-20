from abc import ABC, abstractmethod
from typing import List, Optional
import hashlib


class SplitterStrategy(ABC):
    @abstractmethod
    def assign_variant(self, user_id: str, variants: List[str], weights: List[float]) -> str:
        """
        Method assigns user to the variant
        """
        pass


class HashBasedSplitter(SplitterStrategy):
    def __init__(self, experiment_id: str, salt: Optional[str] = ""):
        self.experiment_id = experiment_id
        self.salt = salt or ""

    def assign_variant(self, user_id: str, variants: List[str], weights: List[float]) -> str:
        if not user_id:
            raise ValueError("user_id cannot be empty")
        if len(variants) != len(weights):
            raise ValueError("variants and weights lists must be of the same length")
        if any(weight == 0 for weight in weights):
            raise ValueError(f"weight must be greater than 0")
        if abs(sum(weights) - 100.0) > 0.01:
            raise ValueError("weights must sum to 100")

        hash_input = f"{user_id}{self.experiment_id}{self.salt}".encode("utf-8")
        hash_digest = hashlib.md5(hash_input).hexdigest()
        hash_int = int(hash_digest, 16)
        scaled = (hash_int % 10000) / 100.0  # Scale to 0.00 - 100.00

        cumulative = 0.0
        for variant, weight in zip(variants, weights):
            cumulative += weight
            if scaled < cumulative:
                return variant
        return variants[-1]


class SplitterContext:
    def __init__(self, strategy: SplitterStrategy):
        self._strategy = strategy

    def assign(self, user_id: str, variants: List[str], weights: List[float]) -> str:
        return self._strategy.assign_variant(user_id, variants, weights)
