from __future__ import annotations
import hashlib
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select
from avos.models.layer import Layer, LayerSlot
from avos.models.experiment import Experiment
from avos.services.assignment_logger import MotherDuckAssignmentLogger
from avos.services.splitter import HashBasedSplitter, RandomSplitter, StratifiedSplitter, GeoBasedSplitter
from avos.utils.datetime_utils import utc_now

class AssignmentService:
    """Layer/slot AB assignment logic, with preview, batch, and extensible splitter support."""

    _assignment_logger = MotherDuckAssignmentLogger()

    @staticmethod
    def assign_for_layer(
        session: Session,
        layer: Layer,
        unit_id: str | int,
        segment: Optional[str]=None,
        geo: Optional[str]=None,
        stratum: Optional[str]=None
    ) -> Dict[str, Any]:
        # 1. Compute slot index deterministically
        slot_index = AssignmentService._calculate_user_slot(layer.layer_salt, layer.total_slots, unit_id)
        # 2. Query slot assignment
        slot = session.execute(
            select(LayerSlot).where(
                LayerSlot.layer_id == layer.layer_id,
                LayerSlot.slot_index == slot_index
            )
        ).scalar_one_or_none()
        if not slot or not slot.experiment_id:
            assignment = AssignmentService._make_assignment(unit_id, layer, slot_index, None, None, "not_assigned")
            AssignmentService._assignment_logger.log_assignments([assignment])
            return assignment

        experiment = session.get(Experiment, slot.experiment_id)
        if not experiment or not experiment.is_active(utc_now()):
            assignment = AssignmentService._make_assignment(unit_id, layer, slot_index, slot.experiment_id, None, "experiment_inactive")
            AssignmentService._assignment_logger.log_assignments([assignment])
            return assignment

        splitter = AssignmentService._select_splitter(
            experiment.splitter_type or "hash",
            experiment,
            segment,
            geo,
            stratum
        )
        # Support all splitter signatures
        splitter_kwargs = {}
        if segment: splitter_kwargs['segment'] = segment
        if geo: splitter_kwargs['geo'] = geo
        if stratum: splitter_kwargs['stratum'] = stratum

        variant = splitter.assign_variant(
            unit_id,
            experiment.get_variant_list(),
            list(experiment.get_traffic_dict().values()),
            **splitter_kwargs
        )
        assignment = AssignmentService._make_assignment(unit_id, layer, slot_index, experiment.experiment_id, variant, "assigned")
        AssignmentService._assignment_logger.log_assignments([assignment])
        return assignment

    @staticmethod
    def assign_bulk_for_layer(
        session: Session,
        layer: Layer,
        unit_ids: List[str | int],
        segment: Optional[str]=None,
        geo: Optional[str]=None,
        stratum: Optional[str]=None
    ) -> Dict[str | int, Dict[str, Any]]:
        """Bulk-assign and log for many users (flush periodically for thousands)."""
        assignments = {}
        batch = []
        for uid in unit_ids:
            assignment = AssignmentService.assign_for_layer(session, layer, uid, segment=segment, geo=geo, stratum=stratum)
            assignments[uid] = assignment
            batch.append(assignment)
            if len(batch) >= 1000:
                AssignmentService._assignment_logger.log_assignments(batch)
                batch = []
        if batch:
            AssignmentService._assignment_logger.log_assignments(batch)
        return assignments

    @staticmethod
    def preview_assignment_distribution(
        session: Session,
        layer: Layer,
        sample_unit_ids: List[str | int],
        segment: Optional[str]=None,
        geo: Optional[str]=None,
        stratum: Optional[str]=None
    ) -> Dict[str, Any]:
        """Preview experiment/variant distribution for SRM monitoring and slot QA."""
        distribution = {}
        unassigned_count = 0
        for uid in sample_unit_ids:
            assignment = AssignmentService.assign_for_layer(session, layer, uid, segment=segment, geo=geo, stratum=stratum)
            if assignment["status"] == "assigned":
                key = f"{assignment['experiment_id']}:{assignment['variant']}"
                distribution[key] = distribution.get(key, 0) + 1
            else:
                unassigned_count += 1
        total = len(sample_unit_ids)
        return {
            "total_users": total,
            "assignment_distribution": distribution,
            "unassigned_count": unassigned_count,
            "assignment_rate": ((total - unassigned_count) / total * 100) if total else None,
        }

    @staticmethod
    def _make_assignment(unit_id, layer, slot_index, experiment_id, variant, status):
        return {
            "unit_id": str(unit_id),
            "layer_id": layer.layer_id,
            "slot_index": slot_index,
            "experiment_id": experiment_id,
            "variant": variant,
            "status": status,
        }

    @staticmethod
    def _select_splitter(splitter_type, experiment, segment, geo, stratum):
        if splitter_type == "hash":
            return HashBasedSplitter(experiment.experiment_id)
        elif splitter_type == "random":
            return RandomSplitter()
        elif splitter_type == "stratified":
            return StratifiedSplitter(experiment.experiment_id, experiment.get_stratum_allocations())
        elif splitter_type == "geo":
            return GeoBasedSplitter(experiment.experiment_id, experiment.get_geo_allocations())
        # You can add additional mappings here as needed
        else:
            raise ValueError(f"Unknown splitter type: {splitter_type}")

    @staticmethod
    def _calculate_user_slot(layer_salt: str, total_slots: int, unit_id: str | int) -> int:
        """Consistent hash to slot index for a layer."""
        hash_input = f"{unit_id}{layer_salt}".encode("utf-8")
        digest = hashlib.md5(hash_input).hexdigest()
        hash_value = int(digest, 16)
        return hash_value % total_slots
