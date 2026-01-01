from __future__ import annotations
from typing import Dict, Any
import math
from sqlalchemy.orm import Session
from sqlalchemy import select, func

from avos.constants import BUCKET_SPACE
from avos.models.layer import Layer, LayerSlot
from avos.models.experiment import Experiment, ExperimentStatus


class LayerService:
    """Layer and Experiment CRUD operations."""

    # ---------- layer CRUD ----------
    @staticmethod
    def create_layer(
        session: Session,
        layer_id: str,
        layer_salt: str,
        total_slots: int = BUCKET_SPACE,
        total_traffic_percentage: float = 1.0,
    ) -> Layer:
        """Create a new layer with pre-allocated empty slots."""
        if total_slots != BUCKET_SPACE:
            raise ValueError(f"total_slots must be {BUCKET_SPACE} for fixed bucket space")

        layer = Layer(
            layer_id=layer_id,
            layer_salt=layer_salt,
            total_slots=total_slots,
            total_traffic_percentage=total_traffic_percentage,
        )
        session.add(layer)

        # Pre-create empty slots
        for i in range(total_slots):
            session.add(
                LayerSlot(
                    layer_id=layer_id,
                    slot_index=i,
                    experiment_id=None,
                    reserved_experiment_id=None,
                )
            )

        session.commit()
        return layer

    @staticmethod
    def get_layer(session: Session, layer_id: str) -> Layer | None:
        """Get layer by ID."""
        return session.execute(select(Layer).where(Layer.layer_id == layer_id)).scalar_one_or_none()

    @staticmethod
    def get_layers(session: Session) -> list[Layer]:
        """Get layers."""
        result = session.execute(select(Layer)).scalars().all()
        return list(result)

    @staticmethod
    def delete_layer(session: Session, layer_id: str) -> bool:
        """Delete layer and all its slots/experiments."""
        layer = session.execute(select(Layer).where(Layer.layer_id == layer_id)).scalar_one_or_none()

        if not layer:
            return False

        session.delete(layer)  # Cascade will handle slots/experiments
        session.commit()
        return True

    # ---------- experiment CRUD ----------
    @staticmethod
    def add_experiment(session: Session, layer: Layer, experiment: Experiment) -> bool:
        """Add experiment to layer, allocating required slots."""
        # Validation
        if experiment.layer_id != layer.layer_id:
            raise ValueError("Experiment.layer_id must match the target layer")

        if experiment.reserved_percentage < experiment.traffic_percentage - 1e-9:
            raise ValueError("Experiment.reserved_percentage must be >= traffic_percentage")

        # Check reservation capacity
        current_reserved = sum(
            e.reserved_percentage for e in layer.experiments if e.status != ExperimentStatus.COMPLETED
        )
        if current_reserved + experiment.reserved_percentage > layer.total_traffic_percentage + 1e-9:
            print("Reservation exceeds capacity")  # TODO replace with logging
            return False

        # Check slot availability for reservation
        reserved_slots_needed = math.ceil(experiment.reserved_percentage * layer.total_slots)

        free_slots = (
            session.execute(
                select(LayerSlot)
                .where(LayerSlot.layer_id == layer.layer_id, LayerSlot.reserved_experiment_id.is_(None))
                .order_by(LayerSlot.slot_index)
                .limit(reserved_slots_needed)
            )
            .scalars()
            .all()
        )

        if len(free_slots) < reserved_slots_needed:
            print("Not enough free slots")  # TODO replace with logging
            return False

        # Reserve slots for experiment
        for slot in free_slots:
            slot.reserved_experiment_id = experiment.experiment_id

        # Activate slots for current traffic
        active_slots_needed = math.ceil(experiment.traffic_percentage * layer.total_slots)
        if active_slots_needed > reserved_slots_needed:
            raise ValueError("Experiment.traffic_percentage cannot exceed reserved_percentage")

        # Assign slots to experiment
        for slot in free_slots[:active_slots_needed]:
            slot.experiment_id = experiment.experiment_id

        session.add(experiment)
        session.commit()
        return True

    @staticmethod
    def remove_experiment(session: Session, layer: Layer, experiment_id: str) -> bool:
        """Remove experiment from layer, freeing its slots."""
        experiment = session.execute(
            select(Experiment).where(Experiment.experiment_id == experiment_id, Experiment.layer_id == layer.layer_id)
        ).scalar_one_or_none()

        if not experiment:
            return False

        reserved_slots = (
            session.execute(
                select(LayerSlot).where(
                    LayerSlot.layer_id == layer.layer_id, LayerSlot.reserved_experiment_id == experiment_id
                )
            )
            .scalars()
            .all()
        )

        for slot in reserved_slots:
            slot.experiment_id = None
            slot.reserved_experiment_id = None

        # Mark experiment as completed
        experiment.status = ExperimentStatus.COMPLETED
        session.commit()
        return True

    @staticmethod
    def get_experiment(session: Session, experiment_id: str) -> Experiment | None:
        """Get experiment by ID."""
        return session.get(Experiment, experiment_id)

    # ---------- layer stats ----------
    @staticmethod
    def get_layer_info(session: Session, layer: Layer) -> Dict[str, Any]:
        """Get detailed information about layer utilization."""
        total_slots = layer.total_slots

        free_slots_result = session.execute(
            select(func.count())
            .select_from(LayerSlot)
            .where(LayerSlot.layer_id == layer.layer_id, LayerSlot.reserved_experiment_id.is_(None))
        ).scalar()
        free_slots = free_slots_result or 0

        # Count slots per experiment
        experiment_slot_counts = {}
        for experiment in layer.experiments:
            count_result = session.execute(
                select(func.count())
                .select_from(LayerSlot)
                .where(LayerSlot.layer_id == layer.layer_id, LayerSlot.experiment_id == experiment.experiment_id)
            ).scalar()
            count = count_result or 0
            experiment_slot_counts[experiment.experiment_id] = count

        return {
            "layer_id": layer.layer_id,
            "total_slots": total_slots,
            "free_slots": free_slots,
            "used_slots": total_slots - free_slots,
            "utilization_percentage": ((total_slots - free_slots) / total_slots) * 100,
            "active_experiments": len([e for e in layer.experiments if e.status == ExperimentStatus.ACTIVE]),
            "experiment_slot_counts": experiment_slot_counts,
        }

    @staticmethod
    def get_layers_by_prefix(session: Session, prefix: str) -> list[Layer]:
        """Get all layers with IDs starting with prefix."""
        result = session.execute(select(Layer).where(Layer.layer_id.like(f"{prefix}%"))).scalars().all()
        return list(result)

    @staticmethod
    def bulk_free_experiment_slots(session: Session, layer_id: str, experiment_id: str) -> int:
        """Bulk free all slots for an experiment. Returns number of slots freed."""
        from sqlalchemy import update

        result = session.execute(
            update(LayerSlot)
            .where(LayerSlot.layer_id == layer_id, LayerSlot.reserved_experiment_id == experiment_id)
            .values(experiment_id=None, reserved_experiment_id=None)
        )
        session.commit()
        return result.rowcount
