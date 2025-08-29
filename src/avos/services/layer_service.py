from __future__ import annotations
from typing import Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import select, func

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
        total_slots: int = 100,
        total_traffic_percentage: float = 100.0,
    ) -> Layer:
        """Create a new layer with pre-allocated empty slots."""
        layer = Layer(
            layer_id=layer_id,
            layer_salt=layer_salt,
            total_slots=total_slots,
            total_traffic_percentage=total_traffic_percentage,
        )
        session.add(layer)

        # Pre-create empty slots
        for i in range(total_slots):
            session.add(LayerSlot(layer_id=layer_id, slot_index=i, experiment_id=None))

        session.commit()
        return layer

    @staticmethod
    def get_layer(session: Session, layer_id: str) -> Layer | None:
        """Get layer by ID."""
        return session.execute(select(Layer).where(Layer.layer_id == layer_id)).scalar_one_or_none()

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

        # Check traffic capacity
        current_traffic = sum(e.traffic_percentage for e in layer.experiments if e.status != ExperimentStatus.COMPLETED)
        if current_traffic + experiment.traffic_percentage > layer.total_traffic_percentage + 1e-9:
            return False

        # Check slot availability
        slots_needed = int((experiment.traffic_percentage / 100) * layer.total_slots)

        free_slots = (
            session.execute(
                select(LayerSlot)
                .where(LayerSlot.layer_id == layer.layer_id, LayerSlot.experiment_id.is_(None))
                .limit(slots_needed)
            )
            .scalars()
            .all()
        )

        if len(free_slots) < slots_needed:
            return False

        # Assign slots to experiment
        for slot in free_slots:
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

        freed_slots = (
            session.execute(
                select(LayerSlot).where(LayerSlot.layer_id == layer.layer_id, LayerSlot.experiment_id == experiment_id)
            )
            .scalars()
            .all()
        )

        for slot in freed_slots:
            slot.experiment_id = None

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

        free_slots = session.execute(
            select(func.count())
            .select_from(LayerSlot)
            .where(LayerSlot.layer_id == layer.layer_id, LayerSlot.experiment_id.is_(None))
        ).scalar()

        # Count slots per experiment
        experiment_slot_counts = {}
        for experiment in layer.experiments:
            count = session.execute(
                select(func.count())
                .select_from(LayerSlot)
                .where(LayerSlot.layer_id == layer.layer_id, LayerSlot.experiment_id == experiment.experiment_id)
            ).scalar()
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
        return session.execute(select(Layer).where(Layer.layer_id.like(f"{prefix}%"))).scalars().all()

    @staticmethod
    def bulk_free_experiment_slots(session: Session, layer_id: str, experiment_id: str) -> int:
        """Bulk free all slots for an experiment. Returns number of slots freed."""
        from sqlalchemy import update

        result = session.execute(
            update(LayerSlot)
            .where(LayerSlot.layer_id == layer_id, LayerSlot.experiment_id == experiment_id)
            .values(experiment_id=None)
        )
        session.commit()
        return result.rowcount
