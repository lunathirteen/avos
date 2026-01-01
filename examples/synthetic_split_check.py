from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from avos.models.base import Base
from avos.models.config_models import LayerConfig, ExperimentConfig
from avos.services.assignment_service import AssignmentService
from avos.services.config_sync import apply_layer_configs
from avos.services.layer_service import LayerService
from avos.srm_tester import SRMTester


def _summarize(assignments):
    total = len(assignments)
    assigned = [a for a in assignments.values() if a["status"] == "assigned"]
    rate = (len(assigned) / total) if total else 0.0
    variants = {}
    for assignment in assigned:
        variants[assignment["variant"]] = variants.get(assignment["variant"], 0) + 1
    return rate, variants


def _srm_for_assignments(assignments, expected_allocations):
    assigned = [a for a in assignments.values() if a["status"] == "assigned"]
    if not assigned:
        return None
    variants = list(expected_allocations.keys())
    index = {variant: idx for idx, variant in enumerate(variants)}
    counts = [0] * len(variants)
    for assignment in assigned:
        variant = assignment["variant"]
        if variant in index:
            counts[index[variant]] += 1
    expected_props = [expected_allocations[variant] for variant in variants]
    return SRMTester().test(counts, expected_props)


def main():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    expected_allocations = {"A": 0.5, "B": 0.5}
    layer_config = LayerConfig(
        layer_id="layer_synth",
        layer_salt="salt_synth",
        total_traffic_percentage=1.0,
        experiments=[
            ExperimentConfig(
                experiment_id="exp_synth",
                layer_id="layer_synth",
                name="Synthetic Split",
                variants=["A", "B"],
                traffic_allocation=expected_allocations,
                status="active",
                traffic_percentage=0.3,
                reserved_percentage=0.6,
            )
        ],
    )
    apply_layer_configs(session, [layer_config])
    layer = LayerService.get_layer(session, "layer_synth")

    unit_ids = [f"user_{i:05d}" for i in range(5000)]
    before = AssignmentService.assign_bulk_for_layer(session, layer, unit_ids)
    rate_before, variants_before = _summarize(before)
    srm_before = _srm_for_assignments(before, expected_allocations)

    print("Before ramp-up")
    print(f"  assignment_rate: {rate_before:.3f}")
    print(f"  variant_counts: {variants_before}")
    if srm_before:
        print(f"  srm: {srm_before}")

    ramped_config = LayerConfig(
        layer_id="layer_synth",
        layer_salt="salt_synth",
        total_traffic_percentage=1.0,
        experiments=[
            ExperimentConfig(
                experiment_id="exp_synth",
                layer_id="layer_synth",
                name="Synthetic Split",
                variants=["A", "B"],
                traffic_allocation=expected_allocations,
                status="active",
                traffic_percentage=0.5,
                reserved_percentage=0.6,
            )
        ],
    )
    apply_layer_configs(session, [ramped_config])

    after = AssignmentService.assign_bulk_for_layer(session, layer, unit_ids)
    rate_after, variants_after = _summarize(after)
    srm_after = _srm_for_assignments(after, expected_allocations)

    stable_count = 0
    for uid, assignment in before.items():
        if assignment["status"] != "assigned":
            continue
        updated = after[uid]
        if updated["status"] == "assigned" and updated["variant"] == assignment["variant"]:
            stable_count += 1

    print("After ramp-up")
    print(f"  assignment_rate: {rate_after:.3f}")
    print(f"  variant_counts: {variants_after}")
    if srm_after:
        print(f"  srm: {srm_after}")
    print(f"  stable_assignments: {stable_count} / {sum(1 for a in before.values() if a['status']=='assigned')}")

    session.close()


if __name__ == "__main__":
    main()
