from pydantic import BaseModel, Field, model_validator
from datetime import datetime
from typing import List, Dict, Optional, Literal

from avos.constants import BUCKET_SPACE

_ALLOC_TOLERANCE = 1e-6
_ALLOWED_SPLITTER_TYPES = {"hash", "random", "stratified", "geo", "segment"}


def _validate_unique_variants(variants: List[str]) -> None:
    if not variants:
        raise ValueError("variants must be non-empty")
    if len(set(variants)) != len(variants):
        raise ValueError("variants must be unique")


def _validate_allocation_map(variants: List[str], allocation_map: Dict[str, float], context: str) -> None:
    if not isinstance(allocation_map, dict):
        raise ValueError(f"{context} must be a dict of variant to allocation")

    missing = [variant for variant in variants if variant not in allocation_map]
    extra = [variant for variant in allocation_map if variant not in variants]
    if missing or extra:
        raise ValueError(f"{context} keys must match variants (missing={missing}, extra={extra})")

    try:
        values = [float(allocation_map[variant]) for variant in variants]
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{context} values must be numeric") from exc

    if any(value < 0 for value in values):
        raise ValueError(f"{context} must be non-negative")
    total = sum(values)
    if abs(total - 1.0) > _ALLOC_TOLERANCE:
        raise ValueError(f"{context} must sum to 1.0 (got {total})")


def _validate_segmented_allocations(
    variants: List[str], allocations: Optional[Dict[str, Dict[str, float]]], context: str
) -> None:
    if allocations is None:
        return
    if not isinstance(allocations, dict):
        raise ValueError(f"{context} must be a dict of segment to allocation map")
    for key, allocation_map in allocations.items():
        _validate_allocation_map(variants, allocation_map, f"{context} '{key}'")


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
    traffic_percentage: float = 1.0
    priority: int = 0

    @model_validator(mode="after")
    def validate_allocations(self):
        _validate_unique_variants(self.variants)
        _validate_allocation_map(self.variants, self.traffic_allocation, "traffic_allocation")
        _validate_segmented_allocations(self.variants, self.segment_allocations, "segment_allocations")
        _validate_segmented_allocations(self.variants, self.geo_allocations, "geo_allocations")
        _validate_segmented_allocations(self.variants, self.stratum_allocations, "stratum_allocations")

        if self.traffic_percentage < 0 or self.traffic_percentage > 1:
            raise ValueError("traffic_percentage must be between 0 and 1")
        if self.splitter_type is not None and self.splitter_type not in _ALLOWED_SPLITTER_TYPES:
            raise ValueError(f"splitter_type must be one of {sorted(_ALLOWED_SPLITTER_TYPES)}")

        if self.segment_allocations and self.splitter_type != "segment":
            raise ValueError("segment_allocations requires splitter_type='segment'")
        if self.geo_allocations and self.splitter_type != "geo":
            raise ValueError("geo_allocations requires splitter_type='geo'")
        if self.stratum_allocations and self.splitter_type != "stratified":
            raise ValueError("stratum_allocations requires splitter_type='stratified'")

        if self.start_date and self.end_date and self.start_date > self.end_date:
            raise ValueError("start_date must be before end_date")

        return self


class LayerConfig(BaseModel):
    layer_id: str
    layer_salt: str
    total_slots: int = BUCKET_SPACE
    total_traffic_percentage: float = 1.0
    experiments: List[ExperimentConfig] = Field(default_factory=list)
    slots: Optional[List[LayerSlotConfig]] = None

    @model_validator(mode="after")
    def validate_layer(self):
        if self.total_slots != BUCKET_SPACE:
            raise ValueError(f"total_slots must be {BUCKET_SPACE} for fixed bucket space")
        if self.total_traffic_percentage <= 0 or self.total_traffic_percentage > 1:
            raise ValueError("total_traffic_percentage must be between 0 and 1")
        if self.slots:
            seen = set()
            for slot in self.slots:
                if slot.slot_index in seen:
                    raise ValueError("slots must have unique slot_index values")
                if slot.slot_index < 0 or slot.slot_index >= self.total_slots:
                    raise ValueError("slot_index must be within total_slots range")
                seen.add(slot.slot_index)
        return self
