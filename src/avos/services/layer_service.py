from __future__ import annotations
import hashlib
from typing import Dict, Any

from sqlalchemy.orm import Session

from avos.models.layer import Layer, LayerSlot
from avos.models.experiment import Experiment, ExperimentStatus
from avos.splitter import HashBasedSplitter


class LayerService:
    """All heavy logic previously inside the dataclass Layer."""

    # ---------- layer initialisation ----------
    @staticmethod
    def create_layer(session: Session, layer_id: str, layer_salt: str,
                     total_slots: int = 100, total_traffic_percentage: float = 100.0) -> Layer:
        layer = Layer(
            layer_id=layer_id,
            layer_salt=layer_salt,
            total_slots=total_slots,
            total_traffic_percentage=total_traffic_percentage,
        )
        session.add(layer)
        # pre-create empty slots
        for i in range(total_slots):
            session.add(LayerSlot(layer_id=layer_id, slot_index=i, experiment_id=None))
        session.commit()
        return layer

    # ---------- add experiment ----------
    @staticmethod
    def add_experiment(
        session: Session, layer: Layer, experiment: Experiment
    ) -> bool:
        # basic checks
        if experiment.layer_id != layer.layer_id:
            raise ValueError("Experiment.layer_id must match the target layer")

        # Calculate current traffic already allocated
        current_traffic = sum(
            e.traffic_percentage for e in layer.experiments if e.status != ExperimentStatus.COMPLETED
        )
        if current_traffic + experiment.traffic_percentage > layer.total_traffic_percentage + 1e-9:
            return False

        slots_needed = int((experiment.traffic_percentage / 100) * layer.total_slots)
        free_slots_q = (
            session.query(LayerSlot)
            .filter_by(layer_id=layer.layer_id, experiment_id=None)
            .limit(slots_needed)
            .all()
        )
        if len(free_slots_q) < slots_needed:
            return False

        # Assign slots
        for slot in free_slots_q:
            slot.experiment_id = experiment.experiment_id
        session.add(experiment)
        session.commit()
        return True

    # ---------- user assignment ----------
    @staticmethod
    def get_user_assignment(session: Session, layer: Layer, unit_id: str | int) -> Dict[str, Any]:
        slot_index = LayerService._assign_slot(layer.layer_salt, layer.total_slots, unit_id)
        slot: LayerSlot | None = (
            session.query(LayerSlot)
            .filter_by(layer_id=layer.layer_id, slot_index=slot_index)
            .first()
        )

        if slot is None or slot.experiment_id is None:
            return {
                "unit_id": unit_id,
                "experiment_id": None,
                "variant": None,
                "status": "not_assigned",
                "slot_id": slot_index,
            }

        experiment: Experiment = session.query(Experiment).get(slot.experiment_id)
        splitter = HashBasedSplitter(experiment_id=experiment.experiment_id)
        variant = splitter.assign_variant(
            unit_id, experiment.variant_list(), experiment.traffic_dict().values()
        )
        return {
            "unit_id": unit_id,
            "experiment_id": experiment.experiment_id,
            "variant": variant,
            "status": "assigned",
            "slot_id": slot_index,
            "experiment_name": experiment.name,
        }

    # ---------- remove / finish experiment ----------
    @staticmethod
    def remove_experiment(session: Session, layer: Layer, experiment_id: str) -> bool:
        exp = session.query(Experiment).filter_by(
            experiment_id=experiment_id, layer_id=layer.layer_id
        ).first()
        if not exp:
            return False

        # Free slots
        freed = (
            session.query(LayerSlot)
            .filter_by(layer_id=layer.layer_id, experiment_id=experiment_id)
            .all()
        )
        for slot in freed:
            slot.experiment_id = None

        # Optionally mark experiment completed
        exp.status = ExperimentStatus.COMPLETED
        session.commit()
        return True

    # ---------- layer stats ----------
    @staticmethod
    def layer_info(session: Session, layer: Layer) -> Dict[str, Any]:
        total = layer.total_slots
        free = (
            session.query(LayerSlot)
            .filter_by(layer_id=layer.layer_id, experiment_id=None)
            .count()
        )
        exp_slot_counts = {
            e.experiment_id: session.query(LayerSlot)
            .filter_by(layer_id=layer.layer_id, experiment_id=e.experiment_id)
            .count()
            for e in layer.experiments
        }
        return {
            "layer_id": layer.layer_id,
            "total_slots": total,
            "free_slots": free,
            "used_slots": total - free,
            "utilization_percentage": ((total - free) / total) * 100,
            "active_experiments": len(layer.experiments),
            "experiment_slots": exp_slot_counts,
        }

    # ---------- helpers ----------
    @staticmethod
    def _assign_slot(layer_salt: str, total_slots: int, user_id: str | int) -> int:
        digest = hashlib.md5(f"{user_id}{layer_salt}".encode()).hexdigest()
        return int(int(digest, 16) % total_slots)
