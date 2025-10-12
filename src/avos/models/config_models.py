from pydantic import BaseModel, Field
from typing import List, Dict

class ExperimentConfig(BaseModel):
    experiment_id: str
    layer_id: str
    name: str
    variants: List[str]
    traffic_allocation: Dict[str, int]
    traffic_percentage: float
    start_date: str
    end_date: str
    status: str
    priority: int


class LayerConfig(BaseModel):
    layer_id: str
    layer_salt: str
    total_slots: int
    total_traffic_percentage: float
    experiments: List[ExperimentConfig] = []
