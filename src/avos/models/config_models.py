from pydantic import BaseModel, Field
from datetime import datetime
from typing import List, Dict, Optional, Literal


class LayerSlotConfig(BaseModel):
    slot_index: int
    experiment_id: Optional[str] = None


class ExperimentConfig(BaseModel):
    experiment_id: str
    layer_id: str
    name: str
    variants: List[str]
    traffic_allocation: Dict[str, float]
    status: Literal["draft", "active", "paused", "completed"] = "draft"
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    segment_allocations: Optional[Dict[str, Dict[str, float]]] = None
    geo_allocations: Optional[Dict[str, Dict[str, float]]] = None
    stratum_allocations: Optional[Dict[str, Dict[str, float]]] = None
    splitter_type: Optional[str] = "hash"
    traffic_percentage: float = 100.0
    priority: int = 0


class LayerConfig(BaseModel):
    layer_id: str
    layer_salt: str
    total_slots: int = 100
    total_traffic_percentage: float = 100.0
    experiments: List[ExperimentConfig] = Field(default_factory=list)
    slots: Optional[List[LayerSlotConfig]] = None
