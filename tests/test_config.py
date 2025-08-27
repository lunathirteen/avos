from datetime import datetime, timedelta
import pytest
from avos.config import ExperimentConfig, Layer


def test_experiment_config_creation():
    config = ExperimentConfig(
        experiment_id="exp_001",
        name="Test experiment",
        variants=["control", "treatment"],
        traffic_allocation={"control": 50.0, "treatment": 50.0},
        traffic_percentage=80.0,
        start_date=datetime.now(),
        end_date=datetime.now() + timedelta(days=7),
        target_audience={"country": "RU"},
    )

    assert config.experiment_id == "exp_001"
    assert len(config.variants) == 2
    assert abs(sum(config.traffic_allocation.values()) - 100.0) < 0.01
    assert config.traffic_percentage == 80.0
    assert config.start_date < config.end_date
    assert config.target_audience == {"country": "RU"}


def test_experiment_traffic_allocation_validation():
    with pytest.raises(ValueError, match="Sum of traffic_allocation must be 100%"):
        ExperimentConfig(
            experiment_id="exp_error",
            name="Invalid traffic",
            variants=["a", "b"],
            traffic_allocation={"a": 60.0, "b": 30.0},  # Sum is 90%, exception
        )


def test_experiment_dates():
    with pytest.raises(ValueError, match="start_date must be before end_date"):
        ExperimentConfig(
            experiment_id="exp_error",
            name="Invalid dates",
            variants=["a", "b"],
            traffic_allocation={
                "a": 50.0,
                "b": 50.0,
            },
            start_date=datetime.now(),
            end_date=datetime.now() - timedelta(days=7),  # end_date is before start_date, exception
        )


def test_experiment_zero_traffic_allocation():
    with pytest.raises(ValueError, match="All traffic_allocation values must be greater than 0"):
        ExperimentConfig(
            experiment_id="exp_error",
            name="0 percent allocation",
            variants=["a", "b"],
            traffic_allocation={
                "a": 60.0,
                "b": 0.0,
            },  # 0 allocation percent for variant, exception
        )

def test_layer_creation():
    layer = Layer(
        layer_id="layer_001",
        layer_salt="layer_salt"
    )

    assert layer.layer_id == "layer_001"
    assert layer.layer_salt == "layer_salt"
    assert layer.total_traffic_percentage == 100.0
    assert layer.total_slots == 100
    assert len(layer.slots) == layer.total_slots
    assert layer.experiments == {}
    assert layer.get_free_slots_count() == 100

def test_empty_layer_id():
    with pytest.raises(ValueError, match="layer_id cannot be empty"):
        Layer(
            layer_id="",
            layer_salt="layer_salt"
        )

def test_empty_layer_salt():
    with pytest.raises(ValueError, match="layer_salt cannot be empty"):
        Layer(
            layer_id="layer_id",
            layer_salt=""
        )

def test_layer_traffic_percentage():
    with pytest.raises(ValueError, match="layer total_traffic_percentage must be in"):
        Layer(
            layer_id="bad_slot",
            layer_salt="layer_salt",
            total_traffic_percentage=110.0,
        )

def test_layer_traffic_percentage():
    with pytest.raises(ValueError, match="total_slots cannot be 0"):
        Layer(
            layer_id="bad_slot",
            layer_salt="layer_salt",
            total_slots=0
        )

def test_layer_traffic_percentage():
    with pytest.raises(ValueError, match="The number of slots must match total_slots"):
        Layer(
            layer_id="bad_slot",
            layer_salt="layer_salt",
            slots=['exp1', 'exp1']
        )

def test_layer_add_exp():
    layer = Layer("ui_experiments", "ui_salt_2025", total_slots=100)
    button_exp = ExperimentConfig(
        experiment_id="button_color",
        name="Button Color Test",
        variants=["control", "treatment"],
        traffic_allocation={"control": 50.0, "treatment": 50.0},
        traffic_percentage=30
    )
    layout_exp = ExperimentConfig(
        experiment_id="layout_test",
        name="Layout Experiment",
        variants=["old", "new"],
        traffic_allocation={"old": 60.0, "new": 40.0},
        traffic_percentage=25
    )

    pricing_exp = ExperimentConfig(
        experiment_id="pricing_test",
        name="Pricing Test",
        variants=["current", "discount_5", "discount_10"],
        traffic_allocation={"current": 50.0, "discount_5": 25.0, "discount_10": 25.0},
        traffic_percentage=40
    )

    success1 = layer.add_experiment(button_exp)
    success2 = layer.add_experiment(layout_exp)
    success3 = layer.add_experiment(pricing_exp)

    assert all([success1, success2, success3]) is True
    assert layer.get_free_slots_count() == 5

def test_layer_assign_unit():
    layer = Layer("ui_experiments", "ui_salt_2025", total_slots=100)
    button_exp = ExperimentConfig(
        experiment_id="button_color",
        name="Button Color Test",
        variants=["control", "treatment"],
        traffic_allocation={"control": 50.0, "treatment": 50.0},
        traffic_percentage=30
    )

    success1 = layer.add_experiment(button_exp)

    assignment1 = layer.get_user_assignment('user_123456')
    assignment2 = layer.get_user_assignment('user_123456')

    assert assignment1 == assignment2
