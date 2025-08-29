import pytest
from datetime import datetime, timedelta, UTC
from sqlalchemy import create_engine, select, func
from sqlalchemy.orm import sessionmaker

from avos.models.base import Base
from avos.models.layer import Layer, LayerSlot
from avos.models.experiment import Experiment, ExperimentStatus
from avos.services.layer_service import LayerService


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
def sample_experiment_data():
    """Sample experiment data for testing."""
    return {
        "experiment_id": "test_exp_001",
        "layer_id": "test_layer",
        "name": "Test Experiment",
        "variants": ["control", "treatment"],
        "traffic_allocation": {"control": 50, "treatment": 50},
        "traffic_percentage": 50.0,
        "start_date": datetime.now(UTC),
        "end_date": datetime.now(UTC) + timedelta(days=7),
        "status": ExperimentStatus.ACTIVE,
        "priority": 1,
    }


class TestLayerCRUD:
    """Test Layer CRUD operations."""

    def test_create_layer_with_default_values(self, db_session):
        """Test creating a layer with default parameters."""
        layer = LayerService.create_layer(db_session, "layer_001", "salt_123")

        assert layer.layer_id == "layer_001"
        assert layer.layer_salt == "salt_123"
        assert layer.total_slots == 100  # default
        assert layer.total_traffic_percentage == 100.0  # default

        # Verify layer is in database
        saved_layer = db_session.execute(select(Layer).where(Layer.layer_id == "layer_001")).scalar_one_or_none()
        assert saved_layer is not None
        assert saved_layer.layer_id == "layer_001"

    def test_create_layer_with_custom_values(self, db_session):
        """Test creating a layer with custom parameters."""
        layer = LayerService.create_layer(
            db_session, "custom_layer", "custom_salt", total_slots=50, total_traffic_percentage=80.0
        )

        assert layer.total_slots == 50
        assert layer.total_traffic_percentage == 80.0

    def test_create_layer_populates_slots(self, db_session):
        """Test that creating a layer pre-populates all slots."""
        layer = LayerService.create_layer(db_session, "slot_test", "salt", total_slots=20)

        # Check all slots were created
        slots = db_session.execute(select(LayerSlot).where(LayerSlot.layer_id == "slot_test")).scalars().all()
        assert len(slots) == 20

        # Check slot indices are correct
        slot_indices = [slot.slot_index for slot in slots]
        assert sorted(slot_indices) == list(range(20))

        # Check all slots are initially empty
        assert all(slot.experiment_id is None for slot in slots)

    def test_get_layer_exists(self, db_session):
        """Test getting an existing layer."""
        created_layer = LayerService.create_layer(db_session, "get_test", "salt")
        retrieved_layer = LayerService.get_layer(db_session, "get_test")

        assert retrieved_layer is not None
        assert retrieved_layer.layer_id == created_layer.layer_id
        assert retrieved_layer.layer_salt == created_layer.layer_salt

    def test_get_layer_not_exists(self, db_session):
        """Test getting a non-existent layer returns None."""
        layer = LayerService.get_layer(db_session, "nonexistent")
        assert layer is None

    def test_delete_layer_exists(self, db_session):
        """Test deleting an existing layer."""
        LayerService.create_layer(db_session, "delete_test", "salt", total_slots=10)

        # Verify layer exists
        layer = db_session.execute(select(Layer).where(Layer.layer_id == "delete_test")).scalar_one_or_none()
        assert layer is not None

        # Delete layer
        result = LayerService.delete_layer(db_session, "delete_test")
        assert result is True

        # Verify layer is gone
        layer = db_session.execute(select(Layer).where(Layer.layer_id == "delete_test")).scalar_one_or_none()
        assert layer is None

        # Verify slots are gone (cascade delete)
        slots = db_session.execute(select(LayerSlot).where(LayerSlot.layer_id == "delete_test")).scalars().all()
        assert len(slots) == 0

    def test_delete_layer_not_exists(self, db_session):
        """Test deleting a non-existent layer returns False."""
        result = LayerService.delete_layer(db_session, "nonexistent")
        assert result is False


class TestExperimentCRUD:
    """Test Experiment CRUD operations."""

    def test_add_experiment_success(self, db_session, sample_experiment_data):
        """Test successfully adding an experiment to a layer."""
        # Create layer
        layer = LayerService.create_layer(db_session, "test_layer", "salt", total_slots=100)

        # Create experiment
        experiment = Experiment(**sample_experiment_data)

        # Add experiment
        success = LayerService.add_experiment(db_session, layer, experiment)
        assert success is True

        # Verify experiment is in database
        saved_exp = db_session.execute(
            select(Experiment).where(Experiment.experiment_id == "test_exp_001")
        ).scalar_one_or_none()
        assert saved_exp is not None
        assert saved_exp.layer_id == "test_layer"

        # Verify slots were allocated (50% of 100 slots = 50 slots)
        allocated_slots = db_session.execute(
            select(func.count())
            .select_from(LayerSlot)
            .where(LayerSlot.layer_id == "test_layer", LayerSlot.experiment_id == "test_exp_001")
        ).scalar()
        assert allocated_slots == 50

    def test_add_experiment_layer_id_mismatch(self, db_session, sample_experiment_data):
        """Test adding experiment with mismatched layer_id raises error."""
        layer = LayerService.create_layer(db_session, "layer_a", "salt")
        sample_experiment_data["layer_id"] = "layer_b"  # Mismatch
        experiment = Experiment(**sample_experiment_data)

        with pytest.raises(ValueError, match="Experiment.layer_id must match the target layer"):
            LayerService.add_experiment(db_session, layer, experiment)

    def test_add_experiment_exceeds_traffic_capacity(self, db_session, sample_experiment_data):
        """Test adding experiment that would exceed traffic capacity."""
        # Create layer with limited traffic capacity
        layer = LayerService.create_layer(
            db_session, "test_layer", "salt", total_slots=100, total_traffic_percentage=60.0
        )

        # Add first experiment (50% traffic)
        exp1 = Experiment(**sample_experiment_data)
        success1 = LayerService.add_experiment(db_session, layer, exp1)
        assert success1 is True

        # Try to add second experiment (50% traffic) - should fail
        sample_experiment_data["experiment_id"] = "test_exp_002"
        exp2 = Experiment(**sample_experiment_data)
        success2 = LayerService.add_experiment(db_session, layer, exp2)
        assert success2 is False

    def test_add_experiment_not_enough_slots(self, db_session, sample_experiment_data):
        """Test adding experiment when not enough free slots available."""
        # Create small layer
        layer = LayerService.create_layer(db_session, "test_layer", "salt", total_slots=10)

        # Manually occupy 8 slots
        slots = db_session.execute(select(LayerSlot).where(LayerSlot.layer_id == "test_layer").limit(8)).scalars().all()
        for slot in slots:
            slot.experiment_id = "existing_exp"
        db_session.commit()

        # Try to add experiment requiring 5 slots (50% of 10) - should fail
        experiment = Experiment(**sample_experiment_data)
        success = LayerService.add_experiment(db_session, layer, experiment)
        assert success is False

    def test_add_experiment_excludes_completed_experiments_from_traffic(self, db_session, sample_experiment_data):
        """Test that completed experiments don't count toward traffic limit."""
        layer = LayerService.create_layer(
            db_session, "test_layer", "salt", total_slots=100, total_traffic_percentage=60.0
        )

        # Add completed experiment (50% traffic - shouldn't count)
        sample_experiment_data["status"] = ExperimentStatus.COMPLETED
        exp1 = Experiment(**sample_experiment_data)
        success1 = LayerService.add_experiment(db_session, layer, exp1)
        assert success1 is True

        # Add active experiment (50% traffic - should succeed)
        sample_experiment_data["experiment_id"] = "test_exp_002"
        sample_experiment_data["status"] = ExperimentStatus.ACTIVE
        exp2 = Experiment(**sample_experiment_data)
        success2 = LayerService.add_experiment(db_session, layer, exp2)
        assert success2 is True

    def test_remove_experiment_success(self, db_session, sample_experiment_data):
        """Test successfully removing an experiment."""
        # Setup: create layer and add experiment
        layer = LayerService.create_layer(db_session, "test_layer", "salt", total_slots=100)
        experiment = Experiment(**sample_experiment_data)
        LayerService.add_experiment(db_session, layer, experiment)

        # Verify experiment and slots are allocated
        assert (
            db_session.execute(
                select(Experiment).where(Experiment.experiment_id == "test_exp_001")
            ).scalar_one_or_none()
            is not None
        )
        allocated_slots_before = db_session.execute(
            select(func.count())
            .select_from(LayerSlot)
            .where(LayerSlot.layer_id == "test_layer", LayerSlot.experiment_id == "test_exp_001")
        ).scalar()
        assert allocated_slots_before > 0

        # Remove experiment
        success = LayerService.remove_experiment(db_session, layer, "test_exp_001")
        assert success is True

        # Verify experiment status changed to COMPLETED
        experiment = db_session.execute(
            select(Experiment).where(Experiment.experiment_id == "test_exp_001")
        ).scalar_one_or_none()
        assert experiment.status == ExperimentStatus.COMPLETED

        # Verify slots were freed
        allocated_slots_after = db_session.execute(
            select(func.count())
            .select_from(LayerSlot)
            .where(LayerSlot.layer_id == "test_layer", LayerSlot.experiment_id == "test_exp_001")
        ).scalar()
        assert allocated_slots_after == 0

    def test_remove_experiment_not_exists(self, db_session):
        """Test removing a non-existent experiment returns False."""
        layer = LayerService.create_layer(db_session, "test_layer", "salt")
        success = LayerService.remove_experiment(db_session, layer, "nonexistent_exp")
        assert success is False

    def test_get_experiment_exists(self, db_session, sample_experiment_data):
        """Test getting an existing experiment."""
        layer = LayerService.create_layer(db_session, "test_layer", "salt")
        created_experiment = Experiment(**sample_experiment_data)
        LayerService.add_experiment(db_session, layer, created_experiment)

        retrieved_experiment = LayerService.get_experiment(db_session, "test_exp_001")
        assert retrieved_experiment is not None
        assert retrieved_experiment.experiment_id == "test_exp_001"
        assert retrieved_experiment.name == "Test Experiment"

    def test_get_experiment_not_exists(self, db_session):
        """Test getting a non-existent experiment returns None."""
        experiment = LayerService.get_experiment(db_session, "nonexistent")
        assert experiment is None


class TestLayerInfo:
    """Test layer information and statistics."""

    def test_get_layer_info_empty_layer(self, db_session):
        """Test getting info for a layer with no experiments."""
        layer = LayerService.create_layer(db_session, "empty_layer", "salt", total_slots=50)

        info = LayerService.get_layer_info(db_session, layer)

        assert info["layer_id"] == "empty_layer"
        assert info["total_slots"] == 50
        assert info["free_slots"] == 50
        assert info["used_slots"] == 0
        assert info["utilization_percentage"] == 0.0
        assert info["active_experiments"] == 0
        assert info["experiment_slot_counts"] == {}

    def test_get_layer_info_with_experiments(self, db_session, sample_experiment_data):
        """Test getting info for a layer with experiments."""
        layer = LayerService.create_layer(db_session, "info_layer", "salt", total_slots=100)

        # Add active experiment (50% traffic = 50 slots)
        sample_experiment_data["layer_id"] = "info_layer"
        exp1 = Experiment(**sample_experiment_data)
        LayerService.add_experiment(db_session, layer, exp1)

        # Add draft experiment (30% traffic = 30 slots)
        sample_experiment_data["experiment_id"] = "test_exp_002"
        sample_experiment_data["traffic_percentage"] = 30.0
        sample_experiment_data["status"] = ExperimentStatus.DRAFT
        exp2 = Experiment(**sample_experiment_data)
        LayerService.add_experiment(db_session, layer, exp2)

        info = LayerService.get_layer_info(db_session, layer)

        assert info["layer_id"] == "info_layer"
        assert info["total_slots"] == 100
        assert info["free_slots"] == 20  # 100 - 50 - 30
        assert info["used_slots"] == 80  # 50 + 30
        assert info["utilization_percentage"] == 80.0
        assert info["active_experiments"] == 1  # Only one ACTIVE experiment
        assert info["experiment_slot_counts"]["test_exp_001"] == 50
        assert info["experiment_slot_counts"]["test_exp_002"] == 30

    def test_get_layer_info_with_completed_experiments(self, db_session, sample_experiment_data):
        """Test that completed experiments show up in slot counts but not active count."""
        layer = LayerService.create_layer(db_session, "completed_layer", "salt", total_slots=100)

        # Add and then remove experiment (marks as COMPLETED)
        sample_experiment_data["layer_id"] = "completed_layer"
        exp = Experiment(**sample_experiment_data)
        LayerService.add_experiment(db_session, layer, exp)
        LayerService.remove_experiment(db_session, layer, "test_exp_001")

        info = LayerService.get_layer_info(db_session, layer)

        assert info["active_experiments"] == 0  # No active experiments
        assert info["free_slots"] == 100  # All slots freed
        assert info["used_slots"] == 0


class TestLayerServiceEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_create_layer_with_zero_slots(self, db_session):
        """Test creating a layer with zero slots."""
        layer = LayerService.create_layer(db_session, "zero_slots", "salt", total_slots=0)

        assert layer.total_slots == 0
        slots = db_session.execute(select(LayerSlot).where(LayerSlot.layer_id == "zero_slots")).scalars().all()
        assert len(slots) == 0

    def test_add_experiment_zero_traffic_percentage(self, db_session, sample_experiment_data):
        """Test adding experiment with 0% traffic."""
        layer = LayerService.create_layer(db_session, "zero_traffic", "salt", total_slots=100)

        sample_experiment_data["layer_id"] = "zero_traffic"
        sample_experiment_data["traffic_percentage"] = 0.0
        exp = Experiment(**sample_experiment_data)

        success = LayerService.add_experiment(db_session, layer, exp)
        assert success is True

        # Should allocate 0 slots
        allocated_slots = db_session.execute(
            select(func.count())
            .select_from(LayerSlot)
            .where(LayerSlot.layer_id == "zero_traffic", LayerSlot.experiment_id == "test_exp_001")
        ).scalar()
        assert allocated_slots == 0

    def test_add_experiment_100_percent_traffic(self, db_session, sample_experiment_data):
        """Test adding experiment with 100% traffic."""
        layer = LayerService.create_layer(db_session, "full_traffic", "salt", total_slots=50)

        sample_experiment_data["layer_id"] = "full_traffic"
        sample_experiment_data["traffic_percentage"] = 100.0
        exp = Experiment(**sample_experiment_data)

        success = LayerService.add_experiment(db_session, layer, exp)
        assert success is True

        # Should allocate all 50 slots
        allocated_slots = db_session.execute(
            select(func.count())
            .select_from(LayerSlot)
            .where(LayerSlot.layer_id == "full_traffic", LayerSlot.experiment_id == "test_exp_001")
        ).scalar()
        assert allocated_slots == 50

    def test_multiple_experiments_different_traffic_percentages(self, db_session, sample_experiment_data):
        """Test adding multiple experiments with different traffic percentages."""
        layer = LayerService.create_layer(db_session, "multi_exp", "salt", total_slots=100)

        # Add 30% experiment
        sample_experiment_data["layer_id"] = "multi_exp"
        sample_experiment_data["experiment_id"] = "exp_30"
        sample_experiment_data["traffic_percentage"] = 30.0
        exp1 = Experiment(**sample_experiment_data)
        success1 = LayerService.add_experiment(db_session, layer, exp1)
        assert success1 is True

        # Add 25% experiment
        sample_experiment_data["experiment_id"] = "exp_25"
        sample_experiment_data["traffic_percentage"] = 25.0
        exp2 = Experiment(**sample_experiment_data)
        success2 = LayerService.add_experiment(db_session, layer, exp2)
        assert success2 is True

        # Add 45% experiment
        sample_experiment_data["experiment_id"] = "exp_45"
        sample_experiment_data["traffic_percentage"] = 45.0
        exp3 = Experiment(**sample_experiment_data)
        success3 = LayerService.add_experiment(db_session, layer, exp3)
        assert success3 is True

        # Verify slot allocation
        info = LayerService.get_layer_info(db_session, layer)
        assert info["experiment_slot_counts"]["exp_30"] == 30
        assert info["experiment_slot_counts"]["exp_25"] == 25
        assert info["experiment_slot_counts"]["exp_45"] == 45
        assert info["free_slots"] == 0


# Integration tests
class TestLayerServiceIntegration:
    """Integration tests combining multiple operations."""

    def test_full_layer_lifecycle(self, db_session, sample_experiment_data):
        """Test complete layer lifecycle: create, add experiments, get info, remove, delete."""
        # Create layer
        layer = LayerService.create_layer(db_session, "lifecycle", "salt", total_slots=100)

        # Add experiment
        sample_experiment_data["layer_id"] = "lifecycle"
        exp = Experiment(**sample_experiment_data)
        LayerService.add_experiment(db_session, layer, exp)

        # Check layer info
        info = LayerService.get_layer_info(db_session, layer)
        assert info["active_experiments"] == 1
        assert info["used_slots"] == 50

        # Remove experiment
        LayerService.remove_experiment(db_session, layer, "test_exp_001")

        # Check layer info after removal
        info = LayerService.get_layer_info(db_session, layer)
        assert info["active_experiments"] == 0
        assert info["used_slots"] == 0

        # Delete layer
        success = LayerService.delete_layer(db_session, "lifecycle")
        assert success is True

        # Verify layer is gone
        assert LayerService.get_layer(db_session, "lifecycle") is None
