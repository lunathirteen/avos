import pytest
from avos.constants import BUCKET_SPACE
from avos.models.config_models import LayerConfig, ExperimentConfig


def test_experiment_config_valid():
    data = {
        "experiment_id": "exp1",
        "layer_id": "layer1",
        "name": "Test AB",
        "variants": ["A", "B"],
        "traffic_allocation": {"A": 0.6, "B": 0.4},
        "status": "active",
        "start_date": "2025-11-01T00:00:00Z",
        "end_date": "2025-11-15T00:00:00Z",
        "segment_allocations": {"US": {"A": 0.6, "B": 0.4}},
        "splitter_type": "segment",
    }
    experiment = ExperimentConfig(**data)
    assert experiment.status == "active"
    assert experiment.variants == ["A", "B"]
    assert experiment.segment_allocations["US"]["A"] == 0.6


def test_layer_config_with_experiments_and_slots():
    layer_data = {
        "layer_id": "layer1",
        "layer_salt": "abc123",
        "total_slots": BUCKET_SPACE,
        "total_traffic_percentage": 1.0,
        "experiments": [
            {
                "experiment_id": "exp2",
                "layer_id": "layer1",
                "name": "Button Color",
                "variants": ["red", "blue"],
                "traffic_allocation": {"red": 0.5, "blue": 0.5},
                "status": "draft",
            }
        ],
        "slots": [{"slot_index": 0, "experiment_id": "exp2"}],
    }
    layer = LayerConfig(**layer_data)
    assert layer.layer_id == "layer1"
    assert layer.experiments[0].name == "Button Color"
    assert layer.slots[0].slot_index == 0


def test_experiment_config_missing_required_raises():
    with pytest.raises(Exception):
        ExperimentConfig(
            experiment_id="exp3", variants=["A"], traffic_allocation={"A": 1.0}  # missing layer_id and name
        )


def test_layer_config_defaults():
    layer = LayerConfig(layer_id="l2", layer_salt="salt", experiments=[])
    assert layer.total_slots == BUCKET_SPACE  # default value


def test_layer_config_custom_total_slots_rejected():
    with pytest.raises(Exception):
        LayerConfig(layer_id="l3", layer_salt="salt", total_slots=10)


def test_experiment_reserved_defaults_to_traffic():
    exp = ExperimentConfig(
        experiment_id="exp_reserved_default",
        layer_id="layer1",
        name="Reserved Default",
        variants=["A", "B"],
        traffic_allocation={"A": 0.5, "B": 0.5},
        traffic_percentage=0.4,
    )
    assert exp.reserved_percentage == 0.4


def test_experiment_reserved_below_traffic_rejected():
    with pytest.raises(Exception):
        ExperimentConfig(
            experiment_id="exp_reserved_low",
            layer_id="layer1",
            name="Reserved Low",
            variants=["A", "B"],
            traffic_allocation={"A": 0.5, "B": 0.5},
            traffic_percentage=0.5,
            reserved_percentage=0.4,
        )


def test_experiment_invalid_status():
    with pytest.raises(Exception):
        ExperimentConfig(
            experiment_id="exp4",
            layer_id="layer2",
            name="Bad Status",
            variants=["A", "B"],
            traffic_allocation={"A": 0.5, "B": 0.5},
            status="in_progress",  # not allowed by Literal
        )


def test_experiment_allocation_sum_invalid():
    with pytest.raises(Exception):
        ExperimentConfig(
            experiment_id="exp_bad_sum",
            layer_id="layer1",
            name="Bad Sum",
            variants=["A", "B"],
            traffic_allocation={"A": 0.6, "B": 0.3},
        )


def test_experiment_allocation_keys_mismatch():
    with pytest.raises(Exception):
        ExperimentConfig(
            experiment_id="exp_bad_keys",
            layer_id="layer1",
            name="Bad Keys",
            variants=["A", "B"],
            traffic_allocation={"A": 0.5, "C": 0.5},
        )


def test_segment_allocations_require_segment_splitter():
    with pytest.raises(Exception):
        ExperimentConfig(
            experiment_id="exp_bad_segment",
            layer_id="layer1",
            name="Bad Segment",
            variants=["A", "B"],
            traffic_allocation={"A": 0.5, "B": 0.5},
            segment_allocations={"US": {"A": 0.5, "B": 0.5}},
            splitter_type="hash",
        )
