from datetime import datetime, timedelta
import pytest
from avos.config import ExperimentConfig, ExperimentRange


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


def test_traffic_allocation_validation():
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


def test_zero_traffic_allocation():
    with pytest.raises(ValueError, match="traffic_allocation must be greater than 0"):
        ExperimentConfig(
            experiment_id="exp_error",
            name="0 percent allocation",
            variants=["a", "b"],
            traffic_allocation={
                "a": 60.0,
                "b": 0.0,
            },  # 0 allocation percent for variant, exception
        )

def test_experiment_range_creation():
    exp_range = ExperimentRange(
        experiment_id="exp_001",
        start_range=0,
        end_range=50
    )

    assert exp_range.contains(50) is False
    assert exp_range.contains(10) is True
