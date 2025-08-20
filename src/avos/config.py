from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime


@dataclass
class ExperimentConfig:
    """Class for storing experiment config.

    :param experiment_id: Unique experiment identificator
    :param name: Experiment name
    :param variants: List of variants (e.g. ['control', 'treatment'])
    :param traffic_allocation: Percent of traffic for each variant (e.g. {'control': 50.0, 'treatment': 50.0})
    :param traffic_percentage: Share of users in experiment (100 by default)
    :param start_date: Date of experiment start, may be None
    :param end_date: Date of experiment end, may be None
    :param target_audience: Used for targeting, may be None

    :return: выбранный вариант
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
        if any(allocation_perc == 0 for allocation_perc in self.traffic_allocation.values()):
            raise ValueError(f"traffic_allocation must be greater than 0")
        total_traffic = sum(self.traffic_allocation.values())
        if abs(total_traffic - 100.0) > 0.01:
            raise ValueError(f"Sum of traffic_allocation must be 100%, but is {total_traffic}")
        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be before end_date")
