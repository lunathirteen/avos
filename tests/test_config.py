from datetime import datetime, timedelta
import pytest
from avos.config import ExperimentConfig, Layer


def test_layer_creation():
    layer = Layer(layer_id="layer_001", layer_salt="layer_salt")

    assert layer.layer_id == "layer_001"
    assert layer.layer_salt == "layer_salt"
    assert layer.total_traffic_percentage == 100.0
    assert layer.total_slots == 100
    assert len(layer.slots) == layer.total_slots
    assert layer.experiments == {}
    assert layer.get_free_slots_count() == 100


def test_empty_layer_id():
    with pytest.raises(ValueError, match="layer_id cannot be empty"):
        Layer(layer_id="", layer_salt="layer_salt")


def test_empty_layer_salt():
    with pytest.raises(ValueError, match="layer_salt cannot be empty"):
        Layer(layer_id="layer_id", layer_salt="")


def test_layer_traffic_percentage():
    with pytest.raises(ValueError, match="layer total_traffic_percentage must be in"):
        Layer(
            layer_id="bad_slot",
            layer_salt="layer_salt",
            total_traffic_percentage=110.0,
        )


def test_layer_traffic_percentage():
    with pytest.raises(ValueError, match="total_slots cannot be 0"):
        Layer(layer_id="bad_slot", layer_salt="layer_salt", total_slots=0)


def test_layer_traffic_percentage():
    with pytest.raises(ValueError, match="The number of slots must match total_slots"):
        Layer(layer_id="bad_slot", layer_salt="layer_salt", slots=["exp1", "exp1"])


def test_layer_add_exp():
    layer = Layer("ui_experiments", "ui_salt_2025", total_slots=100)
    button_exp = ExperimentConfig(
        experiment_id="button_color",
        name="Button Color Test",
        variants=["control", "treatment"],
        traffic_allocation={"control": 50.0, "treatment": 50.0},
        traffic_percentage=30,
    )
    layout_exp = ExperimentConfig(
        experiment_id="layout_test",
        name="Layout Experiment",
        variants=["old", "new"],
        traffic_allocation={"old": 60.0, "new": 40.0},
        traffic_percentage=25,
    )

    pricing_exp = ExperimentConfig(
        experiment_id="pricing_test",
        name="Pricing Test",
        variants=["current", "discount_5", "discount_10"],
        traffic_allocation={"current": 50.0, "discount_5": 25.0, "discount_10": 25.0},
        traffic_percentage=40,
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
        traffic_percentage=30,
    )

    success1 = layer.add_experiment(button_exp)

    assignment1 = layer.get_user_assignment("user_123456")
    assignment2 = layer.get_user_assignment("user_123456")

    assert assignment1 == assignment2


def test_layer_remove_exp():
    layer = Layer("ui_experiments", "ui_salt_2025", total_slots=100)
    button_exp = ExperimentConfig(
        experiment_id="button_color",
        name="Button Color Test",
        variants=["control", "treatment"],
        traffic_allocation={"control": 50.0, "treatment": 50.0},
        traffic_percentage=30,
    )

    layout_exp = ExperimentConfig(
        experiment_id="layout_test",
        name="Layout Experiment",
        variants=["old", "new"],
        traffic_allocation={"old": 60.0, "new": 40.0},
        traffic_percentage=25,
    )

    success1 = layer.add_experiment(button_exp)
    success2 = layer.add_experiment(layout_exp)

    assert layer.get_free_slots_count() == 45

    success3 = layer.remove_experiment("layout_test")

    assert success3 is True
    assert layer.get_free_slots_count() == 70
