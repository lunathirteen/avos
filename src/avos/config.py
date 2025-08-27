from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime


@dataclass
class ExperimentConfig:
    """Configuration for a single experiment.

    :param experiment_id: Unique experiment identifier
    :param name: Human-readable experiment name
    :param variants: List of variants (e.g., ['control', 'treatment'])
    :param traffic_allocation: Percent of traffic for each variant (must sum to 100)
    :param traffic_percentage: Share of layer traffic this experiment should receive
    :param start_date: Optional start date for the experiment
    :param end_date: Optional end date for the experiment
    :param target_audience: Optional targeting criteria
    """

    experiment_id: str
    name: str
    variants: List[str]
    traffic_allocation: Dict[str, float]
    traffic_percentage: float = 100.0
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    target_audience: Optional[Dict[str, Any]] = field(default=None)

    def __post_init__(self):
        # Validate traffic allocation
        if any(allocation_perc <= 0 for allocation_perc in self.traffic_allocation.values()):
            raise ValueError("All traffic_allocation values must be greater than 0")

        total_traffic = sum(self.traffic_allocation.values())
        if abs(total_traffic - 100.0) > 0.01:
            raise ValueError(f"Sum of traffic_allocation must be 100%, but is {total_traffic:.2f}%")

        # Validate dates
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be before end_date")

        # Validate traffic percentage
        if self.traffic_percentage <= 0 or self.traffic_percentage > 100:
            raise ValueError("traffic_percentage must be in (0, 100]")

@dataclass
class ExperimentRange:
    """Represents the hash range allocated to an experiment within a layer."""

    experiment_id: str
    start_range: int  # inclusive
    end_range: int    # exclusive

    def contains(self, hash_value: int) -> bool:
        """Check if hash value falls into this experiment's range."""
        return self.start_range <= hash_value < self.end_range

    def __str__(self) -> str:
        return f"[{self.start_range}, {self.end_range})"
