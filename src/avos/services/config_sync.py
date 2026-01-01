from __future__ import annotations

import json
from sqlalchemy.orm import Session

from avos.models.config_models import LayerConfig, ExperimentConfig
from avos.models.experiment import Experiment, ExperimentStatus
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
            LayerService.remove_experiment(session, layer, experiment_config.experiment_id)
        return

    if existing is None:
        experiment = _build_experiment(experiment_config)
        success = LayerService.add_experiment(session, layer, experiment)
        if not success:
            raise ValueError(f"failed to add experiment {experiment_config.experiment_id} to layer {layer.layer_id}")
        return

    _validate_experiment_immutables(existing, experiment_config)
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
        priority=experiment_config.priority,
    )


def _validate_experiment_immutables(existing: Experiment, experiment_config: ExperimentConfig) -> None:
    if existing.layer_id != experiment_config.layer_id:
        raise ValueError(f"experiment {experiment_config.experiment_id} layer_id cannot be changed")
    if existing.splitter_type != experiment_config.splitter_type:
        raise ValueError(f"experiment {experiment_config.experiment_id} splitter_type cannot be changed")
    if existing.get_variant_list() != experiment_config.variants:
        raise ValueError(f"experiment {experiment_config.experiment_id} variants cannot be changed")
    if existing.status == ExperimentStatus.COMPLETED:
        raise ValueError(f"experiment {experiment_config.experiment_id} is completed and cannot be modified")


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
    existing.priority = experiment_config.priority


def _dump_optional(value):
    return json.dumps(value) if value is not None else None
