from __future__ import annotations
import hashlib
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import select
from avos.models.layer import Layer, LayerSlot
from avos.models.experiment import Experiment
from avos.srm_tester import SRMTester
from avos.services.splitter import (
    HashBasedSplitter,
    RandomSplitter,
    StratifiedSplitter,
    GeoBasedSplitter,
    SegmentedSplitter,
    normalize_allocations,
)
from avos.utils.datetime_utils import utc_now


class AssignmentService:
    """Layer/slot AB assignment logic, with preview and bulk assignment, extensible splitter support."""

    @staticmethod
    def assign_for_layer(
        session: Session,
        layer: Layer,
        unit_id: str | int,
        segment: Optional[str] = None,
        geo: Optional[str] = None,
        stratum: Optional[str] = None,
        assignment_logger: Optional[Any] = None,
    ) -> Dict[str, Any]:
        slot_index = AssignmentService._calculate_user_slot(layer.layer_salt, layer.total_slots, unit_id)
        slot = session.execute(
            select(LayerSlot).where(LayerSlot.layer_id == layer.layer_id, LayerSlot.slot_index == slot_index)
        ).scalar_one_or_none()
        if not slot or not slot.experiment_id:
            assignment = AssignmentService._make_assignment(
                unit_id, layer, slot_index, None, None, "not_assigned", None
            )
            AssignmentService._log_assignments(assignment_logger, [assignment])
            return assignment

        experiment = session.get(Experiment, slot.experiment_id)
        if not experiment or not experiment.is_active(utc_now()):
            assignment = AssignmentService._make_assignment(
                unit_id,
                layer,
                slot_index,
                slot.experiment_id,
                None,
                "experiment_inactive",
                experiment.name if experiment else None,
            )
            AssignmentService._log_assignments(assignment_logger, [assignment])
            return assignment

        splitter = AssignmentService._select_splitter(
            experiment.splitter_type or "hash", experiment, segment, geo, stratum
        )
        splitter_kwargs = {}
        if segment:
            splitter_kwargs["segment"] = segment
        if geo:
            splitter_kwargs["geo"] = geo
        if stratum:
            splitter_kwargs["stratum"] = stratum

        variants = experiment.get_variant_list()
        allocations = normalize_allocations(variants, experiment.get_traffic_dict(), context="traffic_allocation")
        variant = splitter.assign_variant(unit_id, variants, allocations, **splitter_kwargs)
        assignment = AssignmentService._make_assignment(
            unit_id, layer, slot_index, experiment.experiment_id, variant, "assigned", experiment.name
        )
        AssignmentService._log_assignments(assignment_logger, [assignment])
        return assignment

    @staticmethod
    def assign_bulk_for_layer(
        session: Session,
        layer: Layer,
        unit_ids: List[str | int],
        segment: Optional[str] = None,
        geo: Optional[str] = None,
        stratum: Optional[str] = None,
        assignment_logger: Optional[Any] = None,
    ) -> Dict[str | int, Dict[str, Any]]:
        """Bulk-assign for many users."""
        assignments = {}
        for uid in unit_ids:
            assignment = AssignmentService.assign_for_layer(
                session,
                layer,
                uid,
                segment=segment,
                geo=geo,
                stratum=stratum,
                assignment_logger=None,
            )
            assignments[uid] = assignment
        AssignmentService._log_assignments(assignment_logger, list(assignments.values()))
        return assignments

    @staticmethod
    def preview_assignment_distribution(
        session: Session,
        layer: Layer,
        sample_unit_ids: List[str | int],
        segment: Optional[str] = None,
        geo: Optional[str] = None,
        stratum: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Preview experiment/variant distribution for SRM monitoring and slot QA."""
        distribution = {}
        unassigned_count = 0
        for uid in sample_unit_ids:
            assignment = AssignmentService.assign_for_layer(
                session, layer, uid, segment=segment, geo=geo, stratum=stratum
            )
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
    def preview_assignment_metrics(
        session: Session,
        layer: Layer,
        sample_unit_ids: List[str | int],
        segment: Optional[str] = None,
        geo: Optional[str] = None,
        stratum: Optional[str] = None,
        srm_tester: Optional[SRMTester] = None,
    ) -> Dict[str, Any]:
        distribution, per_experiment_counts, unassigned_count = AssignmentService._collect_assignment_stats(
            session, layer, sample_unit_ids, segment=segment, geo=geo, stratum=stratum
        )
        total = len(sample_unit_ids)
        result = {
            "total_users": total,
            "assignment_distribution": distribution,
            "unassigned_count": unassigned_count,
            "assignment_rate": ((total - unassigned_count) / total * 100) if total else None,
        }
        if srm_tester is not None:
            result["srm_results"] = AssignmentService._compute_srm_results(
                session, per_experiment_counts, segment=segment, geo=geo, stratum=stratum, srm_tester=srm_tester
            )
        return result

    @staticmethod
    def _make_assignment(unit_id, layer, slot_index, experiment_id, variant, status, experiment_name):
        return {
            "unit_id": str(unit_id),
            "layer_id": layer.layer_id,
            "slot_index": slot_index,
            "experiment_id": experiment_id,
            "experiment_name": experiment_name,
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
        elif splitter_type == "segment":
            return SegmentedSplitter(experiment.experiment_id, experiment.get_segment_allocations())
        else:
            raise ValueError(f"Unknown splitter type: {splitter_type}")

    @staticmethod
    def _calculate_user_slot(layer_salt: str, total_slots: int, unit_id: str | int) -> int:
        hash_input = f"{unit_id}{layer_salt}".encode("utf-8")
        digest = hashlib.md5(hash_input).hexdigest()
        hash_value = int(digest, 16)
        return hash_value % total_slots

    @staticmethod
    def _log_assignments(assignment_logger: Optional[Any], assignments: List[Dict[str, Any]]) -> None:
        if assignment_logger is None or not assignments:
            return
        if hasattr(assignment_logger, "log_assignments"):
            assignment_logger.log_assignments(assignments)
        else:
            raise ValueError("assignment_logger must implement log_assignments(assignments)")

    @staticmethod
    def _collect_assignment_stats(
        session: Session,
        layer: Layer,
        sample_unit_ids: List[str | int],
        segment: Optional[str] = None,
        geo: Optional[str] = None,
        stratum: Optional[str] = None,
    ):
        distribution: Dict[str, int] = {}
        unassigned_count = 0
        per_experiment_counts: Dict[str, Dict[str, int]] = {}
        for uid in sample_unit_ids:
            assignment = AssignmentService.assign_for_layer(
                session, layer, uid, segment=segment, geo=geo, stratum=stratum
            )
            if assignment["status"] == "assigned":
                exp_id = assignment["experiment_id"]
                variant = assignment["variant"]
                key = f"{exp_id}:{variant}"
                distribution[key] = distribution.get(key, 0) + 1
                exp_counts = per_experiment_counts.setdefault(exp_id, {})
                exp_counts[variant] = exp_counts.get(variant, 0) + 1
            else:
                unassigned_count += 1
        return distribution, per_experiment_counts, unassigned_count

    @staticmethod
    def _compute_srm_results(
        session: Session,
        per_experiment_counts: Dict[str, Dict[str, int]],
        segment: Optional[str],
        geo: Optional[str],
        stratum: Optional[str],
        srm_tester: SRMTester,
    ) -> Dict[str, Any]:
        results: Dict[str, Any] = {}
        for exp_id, counts_by_variant in per_experiment_counts.items():
            experiment = session.get(Experiment, exp_id)
            if not experiment:
                continue
            expected_allocations = AssignmentService._expected_allocations_for_experiment(
                experiment, segment=segment, geo=geo, stratum=stratum
            )
            if not expected_allocations:
                continue
            variants = experiment.get_variant_list()
            observed_counts = [counts_by_variant.get(variant, 0) for variant in variants]
            if sum(observed_counts) == 0:
                continue
            expected_proportions = normalize_allocations(
                variants, expected_allocations, context="expected_allocations"
            )
            results[exp_id] = srm_tester.test(
                observed_counts=observed_counts,
                expected_proportions=expected_proportions,
                experiment_id=exp_id,
            )
        return results

    @staticmethod
    def _expected_allocations_for_experiment(
        experiment: Experiment,
        segment: Optional[str],
        geo: Optional[str],
        stratum: Optional[str],
    ) -> Optional[Dict[str, float]]:
        splitter_type = experiment.splitter_type or "hash"
        if splitter_type == "segment":
            if not segment:
                return None
            return experiment.get_segment_allocations().get(segment)
        if splitter_type == "geo":
            if not geo:
                return None
            return experiment.get_geo_allocations().get(geo)
        if splitter_type == "stratified":
            if not stratum:
                return None
            return experiment.get_stratum_allocations().get(stratum)
        return experiment.get_traffic_dict()
