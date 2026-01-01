from __future__ import annotations

import json
import math
from sqlalchemy import select, func
from sqlalchemy.orm import Session

from avos.models.config_models import LayerConfig, ExperimentConfig
from avos.models.experiment import Experiment, ExperimentStatus
from avos.models.layer import LayerSlot
from avos.services.layer_service import LayerService
from avos.utils.datetime_utils import to_utc


def apply_layer_configs(session: Session, layer_configs: list[LayerConfig]) -> None:
    for layer_config in layer_configs:
        _apply_layer_config(session, layer_config)


def _apply_layer_config(session: Session, layer_config: LayerConfig) -> None:
    if layer_config.slots:
        raise ValueError("slots config is not supported in sync; use traffic_percentage instead")

    layer = LayerService.get_layer(session, layer_config.layer_id)
    if layer is None:
        layer = LayerService.create_layer(
            session,
            layer_id=layer_config.layer_id,
            layer_salt=layer_config.layer_salt,
            total_slots=layer_config.total_slots,
            total_traffic_percentage=layer_config.total_traffic_percentage,
        )
    else:
        if layer.layer_salt != layer_config.layer_salt:
            raise ValueError(f"layer_salt mismatch for layer {layer_config.layer_id}")
        if layer.total_slots != layer_config.total_slots:
            raise ValueError(f"total_slots mismatch for layer {layer_config.layer_id}")
        if layer.total_traffic_percentage != layer_config.total_traffic_percentage:
            layer.total_traffic_percentage = layer_config.total_traffic_percentage
            session.commit()

    for experiment_config in layer_config.experiments:
        if experiment_config.layer_id != layer_config.layer_id:
            raise ValueError(
                f"experiment {experiment_config.experiment_id} layer_id does not match layer {layer_config.layer_id}"
            )
        _apply_experiment_config(session, layer, experiment_config)


def _apply_experiment_config(session: Session, layer, experiment_config: ExperimentConfig) -> None:
    existing = LayerService.get_experiment(session, experiment_config.experiment_id)
    if experiment_config.status == "completed":
        if existing:
            if existing.layer_id != experiment_config.layer_id:
                raise ValueError(f"experiment {experiment_config.experiment_id} layer_id cannot be changed")
            if existing.splitter_type != experiment_config.splitter_type:
                raise ValueError(f"experiment {experiment_config.experiment_id} splitter_type cannot be changed")
            if existing.get_variant_list() != experiment_config.variants:
                raise ValueError(f"experiment {experiment_config.experiment_id} variants cannot be changed")
            if existing.get_segment_allocations() != _normalize_allocations_optional(experiment_config.segment_allocations):
                raise ValueError(
                    f"experiment {experiment_config.experiment_id} segment_allocations cannot be changed; "
                    "create a new experiment"
                )
            if existing.get_geo_allocations() != _normalize_allocations_optional(experiment_config.geo_allocations):
                raise ValueError(
                    f"experiment {experiment_config.experiment_id} geo_allocations cannot be changed; "
                    "create a new experiment"
                )
            if existing.get_stratum_allocations() != _normalize_allocations_optional(experiment_config.stratum_allocations):
                raise ValueError(
                    f"experiment {experiment_config.experiment_id} stratum_allocations cannot be changed; "
                    "create a new experiment"
                )
            if existing.get_traffic_dict() != experiment_config.traffic_allocation:
                if not _is_winner_allocation(experiment_config.variants, experiment_config.traffic_allocation):
                    raise ValueError(
                        f"experiment {experiment_config.experiment_id} traffic_allocation can only change to a "
                        "winner allocation when status is completed"
                    )
                existing.traffic_allocation = json.dumps(experiment_config.traffic_allocation)
            LayerService.remove_experiment(session, layer, experiment_config.experiment_id)
        return

    if existing is None:
        experiment = _build_experiment(experiment_config)
        success = LayerService.add_experiment(session, layer, experiment)
        if not success:
            raise ValueError(f"failed to add experiment {experiment_config.experiment_id} to layer {layer.layer_id}")
        return

    if existing.status == ExperimentStatus.COMPLETED:
        raise ValueError(f"experiment {experiment_config.experiment_id} is completed and cannot be modified")
    _validate_experiment_immutables(existing, experiment_config)
    _validate_traffic_percentage_change(existing, experiment_config)
    _validate_reserved_percentage_change(session, layer, existing, experiment_config)
    _apply_reservation_slots(session, layer, existing, experiment_config)
    _apply_ramp_up_slots(session, layer, existing, experiment_config)
    _update_experiment(existing, experiment_config)
    session.commit()


def _build_experiment(experiment_config: ExperimentConfig) -> Experiment:
    return Experiment(
        experiment_id=experiment_config.experiment_id,
        layer_id=experiment_config.layer_id,
        name=experiment_config.name,
        variants=experiment_config.variants,
        traffic_allocation=experiment_config.traffic_allocation,
        status=ExperimentStatus(experiment_config.status),
        start_date=experiment_config.start_date,
        end_date=experiment_config.end_date,
        segment_allocations=experiment_config.segment_allocations,
        geo_allocations=experiment_config.geo_allocations,
        stratum_allocations=experiment_config.stratum_allocations,
        splitter_type=experiment_config.splitter_type,
        traffic_percentage=experiment_config.traffic_percentage,
        reserved_percentage=experiment_config.reserved_percentage,
        priority=experiment_config.priority,
    )


def _validate_experiment_immutables(existing: Experiment, experiment_config: ExperimentConfig) -> None:
    if existing.layer_id != experiment_config.layer_id:
        raise ValueError(f"experiment {experiment_config.experiment_id} layer_id cannot be changed")
    if existing.splitter_type != experiment_config.splitter_type:
        raise ValueError(f"experiment {experiment_config.experiment_id} splitter_type cannot be changed")
    if existing.get_variant_list() != experiment_config.variants:
        raise ValueError(f"experiment {experiment_config.experiment_id} variants cannot be changed")
    if existing.get_segment_allocations() != _normalize_allocations_optional(experiment_config.segment_allocations):
        raise ValueError(
            f"experiment {experiment_config.experiment_id} segment_allocations cannot be changed; "
            "create a new experiment"
        )
    if existing.get_geo_allocations() != _normalize_allocations_optional(experiment_config.geo_allocations):
        raise ValueError(
            f"experiment {experiment_config.experiment_id} geo_allocations cannot be changed; "
            "create a new experiment"
        )
    if existing.get_stratum_allocations() != _normalize_allocations_optional(experiment_config.stratum_allocations):
        raise ValueError(
            f"experiment {experiment_config.experiment_id} stratum_allocations cannot be changed; "
            "create a new experiment"
        )
    if existing.get_traffic_dict() != experiment_config.traffic_allocation:
        if _is_winner_allocation(experiment_config.variants, experiment_config.traffic_allocation):
            raise ValueError(
                f"experiment {experiment_config.experiment_id} winner allocation is allowed only when status "
                "is completed"
            )
        raise ValueError(
            f"experiment {experiment_config.experiment_id} traffic_allocation cannot be changed; "
            "create a new experiment"
        )


def _update_experiment(existing: Experiment, experiment_config: ExperimentConfig) -> None:
    existing.name = experiment_config.name
    existing.traffic_allocation = json.dumps(experiment_config.traffic_allocation)
    existing.status = ExperimentStatus(experiment_config.status)
    existing.start_date = to_utc(experiment_config.start_date)
    existing.end_date = to_utc(experiment_config.end_date)
    existing.segment_allocations = _dump_optional(experiment_config.segment_allocations)
    existing.geo_allocations = _dump_optional(experiment_config.geo_allocations)
    existing.stratum_allocations = _dump_optional(experiment_config.stratum_allocations)
    existing.traffic_percentage = experiment_config.traffic_percentage
    existing.reserved_percentage = experiment_config.reserved_percentage
    existing.priority = experiment_config.priority


def _dump_optional(value):
    return json.dumps(value) if value is not None else None


def _normalize_allocations_optional(value):
    return value or {}


def _validate_reserved_percentage_change(
    session: Session, layer, existing: Experiment, experiment_config: ExperimentConfig
):
    if experiment_config.reserved_percentage < existing.reserved_percentage - 1e-9:
        raise ValueError(
            f"experiment {experiment_config.experiment_id} reserved_percentage cannot decrease; "
            "create a new experiment"
        )
    if experiment_config.reserved_percentage + 1e-9 < experiment_config.traffic_percentage:
        raise ValueError(
            f"experiment {experiment_config.experiment_id} reserved_percentage must be >= traffic_percentage"
        )

    total_other = (
        session.execute(
            select(func.sum(Experiment.reserved_percentage)).where(
                Experiment.layer_id == layer.layer_id,
                Experiment.experiment_id != existing.experiment_id,
                Experiment.status != ExperimentStatus.COMPLETED,
            )
        ).scalar()
        or 0.0
    )
    if total_other + experiment_config.reserved_percentage > layer.total_traffic_percentage + 1e-9:
        raise ValueError(
            f"experiment {experiment_config.experiment_id} reserved_percentage exceeds layer capacity"
        )


def _validate_traffic_percentage_change(existing: Experiment, experiment_config: ExperimentConfig):
    if experiment_config.traffic_percentage < existing.traffic_percentage - 1e-9:
        raise ValueError(
            f"experiment {experiment_config.experiment_id} traffic_percentage cannot decrease; "
            "create a new experiment"
        )
    if experiment_config.traffic_percentage > experiment_config.reserved_percentage + 1e-9:
        raise ValueError(
            f"experiment {experiment_config.experiment_id} traffic_percentage cannot exceed reserved_percentage"
        )


def _apply_reservation_slots(session: Session, layer, existing: Experiment, experiment_config: ExperimentConfig):
    desired_slots = math.ceil(experiment_config.reserved_percentage * layer.total_slots)
    current_slots = (
        session.execute(
            select(func.count())
            .select_from(LayerSlot)
            .where(
                LayerSlot.layer_id == layer.layer_id,
                LayerSlot.reserved_experiment_id == existing.experiment_id,
            )
        ).scalar()
        or 0
    )
    if desired_slots < current_slots:
        raise ValueError(
            f"experiment {experiment_config.experiment_id} reserved_percentage cannot decrease; "
            "create a new experiment"
        )
    additional_slots = desired_slots - current_slots
    if additional_slots <= 0:
        return

    free_slots = (
        session.execute(
            select(LayerSlot)
            .where(LayerSlot.layer_id == layer.layer_id, LayerSlot.reserved_experiment_id.is_(None))
            .order_by(LayerSlot.slot_index)
            .limit(additional_slots)
        )
        .scalars()
        .all()
    )
    if len(free_slots) < additional_slots:
        raise ValueError(
            f"experiment {experiment_config.experiment_id} has insufficient free slots for reservation"
        )
    for slot in free_slots:
        slot.reserved_experiment_id = existing.experiment_id


def _apply_ramp_up_slots(session: Session, layer, existing: Experiment, experiment_config: ExperimentConfig):
    desired_slots = math.ceil(experiment_config.traffic_percentage * layer.total_slots)
    current_slots = (
        session.execute(
            select(func.count())
            .select_from(LayerSlot)
            .where(LayerSlot.layer_id == layer.layer_id, LayerSlot.experiment_id == existing.experiment_id)
        ).scalar()
        or 0
    )
    if desired_slots < current_slots:
        raise ValueError(
            f"experiment {experiment_config.experiment_id} traffic_percentage cannot decrease; "
            "create a new experiment"
        )
    additional_slots = desired_slots - current_slots
    if additional_slots <= 0:
        return

    free_reserved_slots = (
        session.execute(
            select(LayerSlot)
            .where(
                LayerSlot.layer_id == layer.layer_id,
                LayerSlot.reserved_experiment_id == existing.experiment_id,
                LayerSlot.experiment_id.is_(None),
            )
            .order_by(LayerSlot.slot_index)
            .limit(additional_slots)
        )
        .scalars()
        .all()
    )
    if len(free_reserved_slots) < additional_slots:
        raise ValueError(
            f"experiment {experiment_config.experiment_id} has insufficient reserved slots for ramp up"
        )
    for slot in free_reserved_slots:
        slot.experiment_id = existing.experiment_id


def _is_winner_allocation(variants, allocation):
    if not allocation:
        return False
    winner = None
    for variant in variants:
        value = allocation.get(variant, 0.0)
        if abs(value - 1.0) <= 1e-6:
            if winner is not None:
                return False
            winner = variant
        elif abs(value) > 1e-6:
            return False
    return winner is not None
