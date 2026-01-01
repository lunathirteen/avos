import math
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from avos.models.base import Base
from avos.models.experiment import Experiment, ExperimentStatus
from avos.models.config_models import LayerConfig, ExperimentConfig
from avos.services.assignment_service import AssignmentService
from avos.services.layer_service import LayerService
from avos.services.config_sync import apply_layer_configs
from avos.services.splitter import HashBasedSplitter


def _make_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()


def test_hash_splitter_distribution_is_close_to_expected():
    splitter = HashBasedSplitter("exp_dist")
    variants = ["A", "B"]
    allocations = [0.5, 0.5]
    unit_ids = [f"user_{i}" for i in range(10000)]

    counts = {"A": 0, "B": 0}
    for uid in unit_ids:
        variant = splitter.assign_variant(uid, variants, allocations)
        counts[variant] += 1

    ratio_a = counts["A"] / len(unit_ids)
    assert abs(ratio_a - 0.5) < 0.02


def test_assignment_rate_matches_traffic_percentage():
    session = _make_session()
    layer = LayerService.create_layer(session, "layer_synth", "salt_synth")
    experiment = Experiment(
        experiment_id="exp_synth",
        layer_id="layer_synth",
        name="Synth Test",
        variants=["A", "B"],
        traffic_allocation={"A": 0.5, "B": 0.5},
        traffic_percentage=0.3,
        reserved_percentage=0.3,
        status=ExperimentStatus.ACTIVE,
    )
    assert LayerService.add_experiment(session, layer, experiment) is True

    unit_ids = [f"user_{i}" for i in range(2000)]
    assignments = AssignmentService.assign_bulk_for_layer(session, layer, unit_ids)
    assigned_count = sum(1 for a in assignments.values() if a["status"] == "assigned")
    rate = assigned_count / len(unit_ids)

    assert abs(rate - 0.3) < 0.03


def test_ramp_up_keeps_existing_assignments():
    session = _make_session()
    layer_config = LayerConfig(
        layer_id="layer_ramp",
        layer_salt="salt_ramp",
        total_traffic_percentage=1.0,
        experiments=[
            ExperimentConfig(
                experiment_id="exp_ramp",
                layer_id="layer_ramp",
                name="Ramp Test",
                variants=["A", "B"],
                traffic_allocation={"A": 0.5, "B": 0.5},
                status="active",
                traffic_percentage=0.3,
                reserved_percentage=0.6,
            )
        ],
    )
    apply_layer_configs(session, [layer_config])

    layer = LayerService.get_layer(session, "layer_ramp")
    unit_ids = [f"user_{i}" for i in range(2000)]
    before = AssignmentService.assign_bulk_for_layer(session, layer, unit_ids)
    assigned_before = {
        uid: (assignment["experiment_id"], assignment["variant"])
        for uid, assignment in before.items()
        if assignment["status"] == "assigned"
    }

    ramped_config = LayerConfig(
        layer_id="layer_ramp",
        layer_salt="salt_ramp",
        total_traffic_percentage=1.0,
        experiments=[
            ExperimentConfig(
                experiment_id="exp_ramp",
                layer_id="layer_ramp",
                name="Ramp Test",
                variants=["A", "B"],
                traffic_allocation={"A": 0.5, "B": 0.5},
                status="active",
                traffic_percentage=0.5,
                reserved_percentage=0.6,
            )
        ],
    )
    apply_layer_configs(session, [ramped_config])

    after = AssignmentService.assign_bulk_for_layer(session, layer, unit_ids)
    assigned_after = sum(1 for a in after.values() if a["status"] == "assigned")
    assert assigned_after >= len(assigned_before)

    for uid, (exp_id, variant) in assigned_before.items():
        updated = after[uid]
        assert updated["status"] == "assigned"
        assert updated["experiment_id"] == exp_id
        assert updated["variant"] == variant
