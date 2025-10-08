from __future__ import annotations
import hashlib
from typing import Dict, Any, Iterable, List  # Added List import
from sqlalchemy.orm import Session
from sqlalchemy import select

from avos.models.layer import Layer, LayerSlot
from avos.models.experiment import Experiment
from avos.services.assignment_logger import MotherDuckAssignmentLogger
from avos.utils.datetime_utils import utc_now


class AssignmentService:
    """User assignment and variant allocation logic."""

    _assignment_logger = MotherDuckAssignmentLogger()

    @staticmethod
    def get_user_assignment(session: Session, layer: Layer, unit_id: str | int) -> Dict[str, Any]:
        """Determine which experiment and variant a user should see."""
        # Calculate which slot this user belongs to
        slot_index = AssignmentService._calculate_user_slot(layer.layer_salt, layer.total_slots, unit_id)

        # Find the slot and check if it's assigned to an experiment
        slot = session.execute(
            select(LayerSlot).where(LayerSlot.layer_id == layer.layer_id, LayerSlot.slot_index == slot_index)
        ).scalar_one_or_none()

        if not slot or not slot.experiment_id:
            assignment = {
                "unit_id": str(unit_id),
                "layer_id": layer.layer_id,
                "slot_index": slot_index,
                "experiment_id": None,
                "variant": None,
                "status": "not_assigned",
            }
            AssignmentService._assignment_logger.log_assignments([assignment])
            return assignment

        # Get the experiment and determine variant
        experiment = session.get(Experiment, slot.experiment_id)
        if not experiment or not experiment.is_active(utc_now()):
            assignment = {
                "unit_id": str(unit_id),
                "layer_id": layer.layer_id,
                "slot_index": slot_index,
                "experiment_id": slot.experiment_id,
                "variant": None,
                "status": "experiment_inactive",
            }
            AssignmentService._assignment_logger.log_assignments([assignment])
            return assignment

        # Use splitter to determine variant within the experiment
        splitter = HashBasedSplitter(experiment_id=experiment.experiment_id)
        variant = splitter.assign_variant(
            unit_id, experiment.get_variant_list(), list(experiment.get_traffic_dict().values())
        )

        assignment = {
            "unit_id": str(unit_id),
            "layer_id": layer.layer_id,
            "slot_index": slot_index,
            "experiment_id": experiment.experiment_id,
            "experiment_name": experiment.name,
            "variant": variant,
            "status": "assigned",
        }
        AssignmentService._assignment_logger.log_assignments([assignment])
        return assignment

    @staticmethod
    def get_user_assignments_bulk(
        session: Session, layer: Layer, unit_ids: list[str | int]
    ) -> Dict[str | int, Dict[str, Any]]:
        assignments = {}
        batch = []
        for unit_id in unit_ids:
            assignment = AssignmentService.get_user_assignment(session, layer, unit_id)
            assignments[unit_id] = assignment
            batch.append(assignment)

            # For very large batches, flush periodically (e.g., every 1000)
            if len(batch) >= 1000:
                AssignmentService._assignment_logger.log_assignments(batch)
                batch = []

        # Flush any remaining assignments
        if batch:
            AssignmentService._assignment_logger.log_assignments(batch)

        return assignments

    @staticmethod
    def _calculate_user_slot(layer_salt: str, total_slots: int, user_id: str | int) -> int:
        """Calculate which slot a user belongs to using consistent hashing."""
        hash_input = f"{user_id}{layer_salt}".encode("utf-8")
        digest = hashlib.md5(hash_input).hexdigest()
        hash_value = int(digest, 16)
        return hash_value % total_slots

    @staticmethod
    def preview_assignment_distribution(
        session: Session, layer: Layer, sample_user_ids: list[str | int]
    ) -> Dict[str, Any]:
        """Preview how users would be distributed across experiments."""
        distribution: Dict[str, int] = {}
        unassigned_count = 0

        for user_id in sample_user_ids:
            assignment = AssignmentService.get_user_assignment(session, layer, user_id)

            if assignment["status"] == "assigned":
                exp_id = assignment["experiment_id"]
                variant = assignment["variant"]
                key = f"{exp_id}:{variant}"
                distribution[key] = distribution.get(key, 0) + 1
            else:
                unassigned_count += 1

        return {
            "total_users": len(sample_user_ids),
            "assignment_distribution": distribution,
            "unassigned_count": unassigned_count,
            "assignment_rate": (len(sample_user_ids) - unassigned_count) / len(sample_user_ids) * 100,
        }
