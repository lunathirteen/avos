import pytest
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from avos.models.base import Base
from avos.models.layer import Layer, LayerSlot
from avos.models.experiment import Experiment, ExperimentStatus
from avos.services.layer_service import LayerService
from avos.utils.datetime_utils import utc_now, UTC


@pytest.fixture
def db_session():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.rollback()
    session.close()


@pytest.fixture
def sample_layer(db_session):
    """Create a sample layer for testing."""
    layer = LayerService.create_layer(db_session, layer_id="test_layer", layer_salt="test_salt", total_slots=100)
    return layer


@pytest.fixture
def sample_experiment_data():
    """Sample experiment data for testing."""
    return {
        "experiment_id": "test_exp_001",
        "layer_id": "test_layer",
        "name": "Homepage Button Test",
        "variants": ["control", "treatment"],
        "traffic_allocation": {"control": 50, "treatment": 50},
        "traffic_percentage": 100.0,
        "start_date": utc_now() - timedelta(days=1),  # UTC
        "end_date": utc_now() + timedelta(days=7),  # UTC
        "status": ExperimentStatus.ACTIVE,
        "priority": 1,
    }


class TestExperimentCreation:
    """Test experiment creation and basic functionality."""

    def test_create_experiment_with_valid_data(self, db_session, sample_layer, sample_experiment_data):
        """Test creating an experiment with valid data."""
        exp = Experiment(**sample_experiment_data)
        db_session.add(exp)
        db_session.commit()

        # Verify experiment was saved
        saved_exp = db_session.execute(
            select(Experiment).where(Experiment.experiment_id == "test_exp_001")
        ).scalar_one_or_none()
        assert saved_exp is not None
        assert saved_exp.name == "Homepage Button Test"
        assert saved_exp.layer_id == "test_layer"

    def test_experiment_json_serialization(self, db_session, sample_layer, sample_experiment_data):
        """Test that variants and traffic_allocation are properly JSON serialized."""
        exp = Experiment(**sample_experiment_data)
        db_session.add(exp)
        db_session.commit()

        saved_exp = db_session.execute(
            select(Experiment).where(Experiment.experiment_id == "test_exp_001")
        ).scalar_one_or_none()

        # Check that raw database values are JSON strings
        assert isinstance(saved_exp.variants, str)
        assert isinstance(saved_exp.traffic_allocation, str)

        # Check JSON parsing
        variants_parsed = json.loads(saved_exp.variants)
        traffic_parsed = json.loads(saved_exp.traffic_allocation)

        assert variants_parsed == ["control", "treatment"]
        assert traffic_parsed == {"control": 50, "treatment": 50}

    def test_experiment_helper_methods(self, db_session, sample_layer, sample_experiment_data):
        """Test get_variant_list() and get_traffic_dict() helper methods."""
        exp = Experiment(**sample_experiment_data)

        assert exp.get_variant_list() == ["control", "treatment"]
        assert exp.get_traffic_dict() == {"control": 50, "treatment": 50}

    def test_experiment_timestamps_auto_populated(self, db_session, sample_layer, sample_experiment_data):
        """Test that created_at and updated_at are automatically populated."""
        # Don't provide timestamps
        sample_experiment_data.pop("created_at", None)
        sample_experiment_data.pop("updated_at", None)

        exp = Experiment(**sample_experiment_data)

        assert exp.created_at is not None
        assert exp.updated_at is not None
        assert isinstance(exp.created_at, datetime)
        assert isinstance(exp.updated_at, datetime)


class TestExperimentIsActive:
    """Test the is_active() method logic."""

    def test_active_experiment_within_dates(self, sample_experiment_data):
        """Test that an active experiment within date range returns True."""
        exp = Experiment(**sample_experiment_data)
        now = datetime.now(UTC)

        assert exp.is_active(now) is True

    def test_inactive_experiment_paused_status(self, sample_experiment_data):
        """Test that paused experiment returns False regardless of dates."""
        sample_experiment_data["status"] = ExperimentStatus.PAUSED
        exp = Experiment(**sample_experiment_data)
        now = datetime.now(UTC)

        assert exp.is_active(now) is False

    def test_inactive_experiment_before_start_date(self, sample_experiment_data):
        """Test experiment is inactive before start date."""
        sample_experiment_data["start_date"] = datetime.now(UTC) + timedelta(days=1)
        exp = Experiment(**sample_experiment_data)
        now = datetime.now(UTC)

        assert exp.is_active(now) is False

    def test_inactive_experiment_after_end_date(self, sample_experiment_data):
        """Test experiment is inactive after end date."""
        sample_experiment_data["end_date"] = datetime.now(UTC) - timedelta(days=1)
        exp = Experiment(**sample_experiment_data)
        now = datetime.now(UTC)

        assert exp.is_active(now) is False

    def test_experiment_with_no_dates(self, sample_experiment_data):
        """Test experiment with no start/end dates."""
        sample_experiment_data["start_date"] = None
        sample_experiment_data["end_date"] = None
        exp = Experiment(**sample_experiment_data)
        now = datetime.now(UTC)

        assert exp.is_active(now) is True

    def test_is_active_uses_current_time_by_default(self, sample_experiment_data):
        """Test that is_active() uses current time when no time provided."""
        exp = Experiment(**sample_experiment_data)

        # Should not raise any errors and return boolean
        result = exp.is_active()
        assert isinstance(result, bool)


class TestExperimentValidation:
    """Test experiment validation scenarios."""

    def test_experiment_with_different_statuses(self, sample_experiment_data):
        """Test creating experiments with different statuses."""
        statuses = [
            ExperimentStatus.DRAFT,
            ExperimentStatus.ACTIVE,
            ExperimentStatus.PAUSED,
            ExperimentStatus.COMPLETED,
        ]

        for status in statuses:
            sample_experiment_data["experiment_id"] = f"exp_{status.value}"
            sample_experiment_data["status"] = status

            exp = Experiment(**sample_experiment_data)
            assert exp.status == status

    def test_experiment_with_empty_variants(self, sample_experiment_data):
        """Test experiment with empty variants list."""
        sample_experiment_data["variants"] = []
        sample_experiment_data["traffic_allocation"] = {}

        exp = Experiment(**sample_experiment_data)
        assert exp.get_variant_list() == []
        assert exp.get_traffic_dict() == {}


class TestExperimentRelationships:
    """Test experiment relationships with other models."""

    def test_experiment_layer_relationship(self, db_session, sample_layer, sample_experiment_data):
        """Test that experiment properly links to its layer."""
        exp = Experiment(**sample_experiment_data)
        db_session.add(exp)
        db_session.commit()

        # Refresh from database
        db_session.refresh(exp)

        assert exp.layer is not None
        assert exp.layer.layer_id == "test_layer"


class TestExperimentEdgeCases:
    """Test edge cases and error conditions."""

    def test_experiment_with_complex_traffic_allocation(self, sample_experiment_data):
        """Test experiment with complex traffic allocation."""
        complex_allocation = {"control": 25.5, "variant_a": 25.5, "variant_b": 24.0, "variant_c": 25.0}

        sample_experiment_data["variants"] = list(complex_allocation.keys())
        sample_experiment_data["traffic_allocation"] = complex_allocation

        exp = Experiment(**sample_experiment_data)

        assert exp.get_traffic_dict() == complex_allocation
        assert sum(exp.get_traffic_dict().values()) == 100.0

    def test_experiment_with_unicode_names(self, sample_experiment_data):
        """Test experiment with unicode characters in name."""
        sample_experiment_data["name"] = "测试实验 🧪"

        exp = Experiment(**sample_experiment_data)
        assert exp.name == "测试实验 🧪"

    def test_experiment_priority_values(self, sample_experiment_data):
        """Test different priority values."""
        priorities = [0, 1, 10, 100, -1]

        for priority in priorities:
            sample_experiment_data["experiment_id"] = f"exp_priority_{priority}"
            sample_experiment_data["priority"] = priority

            exp = Experiment(**sample_experiment_data)
            assert exp.priority == priority


# Integration test with LayerService
class TestExperimentIntegration:
    """Integration tests with LayerService."""

    def test_add_experiment_to_layer(self, db_session):
        """Test adding experiment to layer via LayerService."""
        # Create layer
        layer = LayerService.create_layer(db_session, "integration_layer", "salt", total_slots=100)

        # Create experiment
        exp = Experiment(
            experiment_id="integration_exp",
            layer_id="integration_layer",
            name="Integration Test",
            variants=["control", "treatment"],
            traffic_allocation={"control": 50, "treatment": 50},
            traffic_percentage=50.0,
            status=ExperimentStatus.ACTIVE,
        )

        # Add experiment to layer
        success = LayerService.add_experiment(db_session, layer, exp)

        assert success is True

        # Verify experiment is in database
        saved_exp = db_session.execute(
            select(Experiment).where(Experiment.experiment_id == "integration_exp")
        ).scalar_one_or_none()
        assert saved_exp is not None
        assert saved_exp.layer_id == "integration_layer"
