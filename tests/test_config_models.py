import pytest
from avos.models.config_models import LayerConfig, ExperimentConfig


def test_experiment_config_valid():
    data = {
        "experiment_id": "exp1",
        "layer_id": "layer1",
        "name": "Test AB",
        "variants": ["A", "B"],
        "traffic_allocation": {"A": 60, "B": 40},
        "status": "active",
        "start_date": "2025-11-01T00:00:00Z",
        "end_date": "2025-11-15T00:00:00Z",
        "segment_allocations": {"US": 20, "Canada": 10},
    }
    experiment = ExperimentConfig(**data)
    assert experiment.status == "active"
    assert experiment.variants == ["A", "B"]
    assert experiment.segment_allocations["US"] == 20


def test_layer_config_with_experiments_and_slots():
    layer_data = {
        "layer_id": "layer1",
        "layer_salt": "abc123",
        "total_slots": 100,
        "total_traffic_percentage": 100.0,
        "experiments": [
            {
                "experiment_id": "exp2",
                "layer_id": "layer1",
                "name": "Button Color",
                "variants": ["red", "blue"],
                "traffic_allocation": {"red": 50, "blue": 50},
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
            experiment_id="exp3", variants=["A"], traffic_allocation={"A": 100}  # missing layer_id and name
        )


def test_layer_config_defaults():
    layer = LayerConfig(layer_id="l2", layer_salt="salt", experiments=[])
    assert layer.total_slots == 100  # default value
    assert layer.total_traffic_percentage == 100.0


def test_experiment_invalid_status():
    with pytest.raises(Exception):
        ExperimentConfig(
            experiment_id="exp4",
            layer_id="layer2",
            name="Bad Status",
            variants=["A", "B"],
            traffic_allocation={"A": 50, "B": 50},
            status="in_progress",  # not allowed by Literal
        )
