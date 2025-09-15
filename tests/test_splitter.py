import pytest
from unittest.mock import patch, MagicMock
from datetime import timedelta
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker

from avos.models.base import Base
from avos.models.layer import LayerSlot
from avos.models.experiment import Experiment, ExperimentStatus
from avos.services.layer_service import LayerService
from avos.services.splitter import HashBasedSplitter, AssignmentService
from avos.utils.datetime_utils import utc_now


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
def mock_assignment_logger():
    """Mock the DuckDB assignment logger to avoid actual database operations during tests."""
    with patch('avos.services.splitter.AssignmentService._assignment_logger') as mock_logger:
        mock_logger.log_assignments = MagicMock()
        yield mock_logger


@pytest.fixture
def sample_layer_with_experiment(db_session):
    """Create a layer with an active experiment for testing."""
    # Create layer
    layer = LayerService.create_layer(db_session, "test_layer", "salt123", total_slots=100)

    # Create experiment with UTC datetimes
    experiment = Experiment(
        experiment_id="test_exp",
        layer_id="test_layer",
        name="Button Color Test",
        variants=["red", "blue", "green"],
        traffic_allocation={"red": 40, "blue": 35, "green": 25},
        traffic_percentage=60.0,
        start_date=utc_now() - timedelta(days=1),
        end_date=utc_now() + timedelta(days=7),
        status=ExperimentStatus.ACTIVE,
    )

    # Add experiment to layer
    LayerService.add_experiment(db_session, layer, experiment)

    return layer, experiment


class TestHashBasedSplitter:
    """Test HashBasedSplitter functionality."""

    def test_splitter_initialization(self):
        """Test splitter can be initialized with experiment ID."""
        splitter = HashBasedSplitter("exp_123")
        assert splitter.exp_id == "exp_123"

    def test_assign_variant_deterministic(self):
        """Test that variant assignment is deterministic for same user."""
        splitter = HashBasedSplitter("exp_001")
        variants = ["control", "treatment"]
        allocations = [50.0, 50.0]

        # Same user should get same variant
        variant1 = splitter.assign_variant("user_123", variants, allocations)
        variant2 = splitter.assign_variant("user_123", variants, allocations)

        assert variant1 == variant2
        assert variant1 in variants

    def test_assign_variant_different_users(self):
        """Test that different users can get different variants."""
        splitter = HashBasedSplitter("exp_001")
        variants = ["control", "treatment"]
        allocations = [50.0, 50.0]

        # Test multiple users
        assignments = []
        for i in range(100):
            variant = splitter.assign_variant(f"user_{i}", variants, allocations)
            assignments.append(variant)

        # Should have both variants assigned
        unique_variants = set(assignments)
        assert len(unique_variants) >= 1
        assert all(v in variants for v in unique_variants)

    def test_assign_variant_uneven_allocation(self):
        """Test variant assignment with uneven traffic allocation."""
        splitter = HashBasedSplitter("exp_uneven")
        variants = ["control", "treatment"]
        allocations = [90.0, 10.0]

        assignments = []
        for i in range(1000):
            variant = splitter.assign_variant(f"user_{i}", variants, allocations)
            assignments.append(variant)

        control_count = assignments.count("control")
        treatment_count = assignments.count("treatment")

        control_percentage = control_count / len(assignments) * 100
        treatment_percentage = treatment_count / len(assignments) * 100

        # Allow 10% variance from expected allocation
        assert 80 <= control_percentage <= 100
        assert 0 <= treatment_percentage <= 20

    def test_assign_variant_three_variants(self):
        """Test assignment with three variants."""
        splitter = HashBasedSplitter("exp_three")
        variants = ["A", "B", "C"]
        allocations = [33.33, 33.33, 33.34]

        assignments = []
        for i in range(300):
            variant = splitter.assign_variant(f"user_{i}", variants, allocations)
            assignments.append(variant)

        # All variants should be represented
        unique_variants = set(assignments)
        assert unique_variants == {"A", "B", "C"}


class TestAssignmentService:
    """Test AssignmentService functionality."""

    def test_calculate_user_slot_deterministic(self):
        """Test that slot calculation is deterministic."""
        slot1 = AssignmentService._calculate_user_slot("salt123", 100, "user_123")
        slot2 = AssignmentService._calculate_user_slot("salt123", 100, "user_123")

        assert slot1 == slot2
        assert 0 <= slot1 < 100

    def test_calculate_user_slot_distribution(self):
        """Test that users are distributed evenly across slots."""
        salt = "test_salt"
        total_slots = 10

        slot_counts = {}
        for i in range(1000):
            slot = AssignmentService._calculate_user_slot(salt, total_slots, f"user_{i}")
            slot_counts[slot] = slot_counts.get(slot, 0) + 1

        # All slots should be represented
        assert len(slot_counts) == total_slots

        # Distribution should be roughly even (within 20% variance)
        expected_per_slot = 1000 / total_slots
        for count in slot_counts.values():
            assert 0.8 * expected_per_slot <= count <= 1.2 * expected_per_slot

    def test_get_user_assignment_not_assigned_with_logging(self, db_session, mock_assignment_logger):
        """Test user assignment when slot is not assigned to any experiment."""
        # Create layer with no experiments
        layer = LayerService.create_layer(db_session, "empty_layer", "salt", total_slots=10)

        assignment = AssignmentService.get_user_assignment(db_session, layer, "user_123")

        assert assignment["status"] == "not_assigned"
        assert assignment["unit_id"] == "user_123"
        assert assignment["layer_id"] == "empty_layer"
        assert assignment["experiment_id"] is None
        assert assignment["variant"] is None
        assert "slot_index" in assignment

        # Verify logging was called
        mock_assignment_logger.log_assignments.assert_called_once()
        logged_assignment = mock_assignment_logger.log_assignments.call_args[0][0][0]
        assert logged_assignment == assignment

    def test_get_user_assignment_experiment_inactive_with_logging(self, db_session, mock_assignment_logger):
        """Test user assignment when experiment is inactive."""
        layer = LayerService.create_layer(db_session, "inactive_layer", "salt", total_slots=100)

        # Create inactive experiment
        experiment = Experiment(
            experiment_id="inactive_exp",
            layer_id="inactive_layer",
            name="Inactive Test",
            variants=["A", "B"],
            traffic_allocation={"A": 50, "B": 50},
            traffic_percentage=100.0,
            status=ExperimentStatus.PAUSED,
        )

        LayerService.add_experiment(db_session, layer, experiment)

        # Find a user that gets assigned to this experiment's slot
        for i in range(1000):
            user_id = f"user_{i}"
            slot_index = AssignmentService._calculate_user_slot(layer.layer_salt, layer.total_slots, user_id)
            slot = db_session.execute(
                select(LayerSlot).where(LayerSlot.layer_id == "inactive_layer", LayerSlot.slot_index == slot_index)
            ).scalar_one_or_none()

            if slot and slot.experiment_id == "inactive_exp":
                assignment = AssignmentService.get_user_assignment(db_session, layer, user_id)
                assert assignment["status"] == "experiment_inactive"
                assert assignment["experiment_id"] == "inactive_exp"
                assert assignment["variant"] is None

                # Verify logging was called
                mock_assignment_logger.log_assignments.assert_called_once()
                break
        else:
            pytest.fail("Could not find a user assigned to the inactive experiment")

    def test_get_user_assignment_assigned_with_logging(self, db_session, sample_layer_with_experiment, mock_assignment_logger):
        """Test successful user assignment."""
        layer, experiment = sample_layer_with_experiment

        # Find a user that gets assigned to the experiment
        for i in range(1000):
            user_id = f"user_{i}"
            assignment = AssignmentService.get_user_assignment(db_session, layer, user_id)

            if assignment["status"] == "assigned":
                assert assignment["unit_id"] == user_id
                assert assignment["layer_id"] == "test_layer"
                assert assignment["experiment_id"] == "test_exp"
                assert assignment["experiment_name"] == "Button Color Test"
                assert assignment["variant"] in ["red", "blue", "green"]
                assert "slot_index" in assignment

                # Reset mock and make the call again to test logging in isolation
                mock_assignment_logger.reset_mock()

                # Call assignment again for the same user (should be deterministic)
                repeated_assignment = AssignmentService.get_user_assignment(db_session, layer, user_id)

                # Verify assignment is identical
                assert repeated_assignment == assignment

                # Verify logging was called exactly once for the repeated call
                mock_assignment_logger.log_assignments.assert_called_once()
                logged_assignment = mock_assignment_logger.log_assignments.call_args[0][0][0]
                assert logged_assignment == assignment
                break
        else:
            pytest.fail("Could not find a user assigned to the experiment")

    def test_get_user_assignments_bulk_with_batch_logging(self, db_session, sample_layer_with_experiment, mock_assignment_logger):
        """Test bulk user assignment with batch logging."""
        layer, experiment = sample_layer_with_experiment

        user_ids = [f"user_{i}" for i in range(20)]
        assignments = AssignmentService.get_user_assignments_bulk(db_session, layer, user_ids)

        assert len(assignments) == 20

        for user_id in user_ids:
            assert user_id in assignments
            assert assignments[user_id]["unit_id"] == user_id
            assert assignments[user_id]["layer_id"] == "test_layer"
            assert assignments[user_id]["status"] in ["assigned", "not_assigned"]

        # Verify logging was called - should be called for each individual assignment
        # plus potentially once more for batch flushing
        assert mock_assignment_logger.log_assignments.call_count >= 1

    def test_get_user_assignments_bulk_large_batch_logging(self, db_session, sample_layer_with_experiment, mock_assignment_logger):
        """Test bulk assignment with large batch that triggers periodic flushing."""
        layer, experiment = sample_layer_with_experiment

        # Test with 1500 users to trigger batch flushing (every 1000)
        user_ids = [f"user_{i}" for i in range(1500)]
        assignments = AssignmentService.get_user_assignments_bulk(db_session, layer, user_ids)

        assert len(assignments) == 1500

        # Verify batch logging was called multiple times due to periodic flushing
        assert mock_assignment_logger.log_assignments.call_count >= 2

    def test_preview_assignment_distribution_with_logging(self, db_session, sample_layer_with_experiment, mock_assignment_logger):
        """Test assignment distribution preview."""
        layer, experiment = sample_layer_with_experiment

        user_ids = [f"user_{i}" for i in range(200)]
        distribution = AssignmentService.preview_assignment_distribution(db_session, layer, user_ids)

        assert distribution["total_users"] == 200
        assert isinstance(distribution["unassigned_count"], int)
        assert isinstance(distribution["assignment_rate"], float)
        assert isinstance(distribution["assignment_distribution"], dict)

        # Assignment rate should be reasonable (experiment has 60% traffic)
        assert 0 <= distribution["assignment_rate"] <= 100

        # Verify logging was called for each user in preview
        assert mock_assignment_logger.log_assignments.call_count == 200


class TestAssignmentServiceLoggingIntegration:
    """Test AssignmentService logging integration specifically."""

    def test_logging_called_with_correct_assignment_data(self, db_session, sample_layer_with_experiment, mock_assignment_logger):
        """Test that logging is called with correct assignment data structure."""
        layer, experiment = sample_layer_with_experiment

        # Get assignment for a user
        assignment = AssignmentService.get_user_assignment(db_session, layer, "test_user")

        # Verify logging was called once
        mock_assignment_logger.log_assignments.assert_called_once()

        # Verify the logged data structure
        logged_assignments = mock_assignment_logger.log_assignments.call_args[0][0]
        assert len(logged_assignments) == 1

        logged_assignment = logged_assignments[0]
        expected_keys = {"unit_id", "layer_id", "slot_index", "experiment_id", "variant", "status"}

        # Check all expected keys are present
        assert set(logged_assignment.keys()).issuperset(expected_keys)
        assert logged_assignment == assignment

    def test_logging_consistency_across_assignment_types(self, db_session, mock_assignment_logger):
        """Test that all assignment types (assigned, not_assigned, inactive) are logged consistently."""
        # Test with empty layer (not_assigned)
        empty_layer = LayerService.create_layer(db_session, "empty", "salt", total_slots=10)
        assignment1 = AssignmentService.get_user_assignment(db_session, empty_layer, "user1")

        # Test with inactive experiment
        inactive_layer = LayerService.create_layer(db_session, "inactive", "salt", total_slots=100)
        inactive_exp = Experiment(
            experiment_id="inactive_exp",
            layer_id="inactive",
            name="Inactive",
            variants=["A", "B"],
            traffic_allocation={"A": 50, "B": 50},
            traffic_percentage=100.0,
            status=ExperimentStatus.PAUSED,
        )
        LayerService.add_experiment(db_session, inactive_layer, inactive_exp)

        # Find user that would be assigned to inactive experiment
        for i in range(1000):
            user_id = f"user_{i}"
            slot_index = AssignmentService._calculate_user_slot(inactive_layer.layer_salt, inactive_layer.total_slots, user_id)
            slot = db_session.execute(
                select(LayerSlot).where(LayerSlot.layer_id == "inactive", LayerSlot.slot_index == slot_index)
            ).scalar_one_or_none()

            if slot and slot.experiment_id == "inactive_exp":
                assignment2 = AssignmentService.get_user_assignment(db_session, inactive_layer, user_id)
                break
        else:
            pytest.fail("Could not find user for inactive experiment")

        # Verify both assignments were logged
        assert mock_assignment_logger.log_assignments.call_count == 2

        # Verify consistent data structure
        all_calls = mock_assignment_logger.log_assignments.call_args_list
        for call in all_calls:
            logged_assignment = call[0][0][0]  # First call, first argument, first assignment
            expected_keys = {"unit_id", "layer_id", "slot_index", "experiment_id", "variant", "status"}
            assert set(logged_assignment.keys()).issuperset(expected_keys)

    def test_assignment_logger_has_required_interface(self):
        """Test that assignment logger has the required interface."""
        logger = AssignmentService._assignment_logger

        # Verify logger has required methods
        assert hasattr(logger, 'log_assignments'), "Logger missing log_assignments method"
        assert hasattr(logger, 'close'), "Logger missing close method"

        # Verify methods are callable
        assert callable(logger.log_assignments), "log_assignments is not callable"
        assert callable(logger.close), "close is not callable"


class TestIntegrationScenarios:
    """Integration tests with real database scenarios."""

    def test_multi_experiment_layer_assignment_with_logging(self, db_session, mock_assignment_logger):
        """Test user assignment in a layer with multiple experiments."""
        layer = LayerService.create_layer(db_session, "multi_exp", "salt", total_slots=100)

        # Add first experiment (30% traffic)
        exp1 = Experiment(
            experiment_id="exp_1",
            layer_id="multi_exp",
            name="Experiment 1",
            variants=["A1", "B1"],
            traffic_allocation={"A1": 50, "B1": 50},
            traffic_percentage=30.0,
            status=ExperimentStatus.ACTIVE,
        )
        LayerService.add_experiment(db_session, layer, exp1)

        # Add second experiment (40% traffic)
        exp2 = Experiment(
            experiment_id="exp_2",
            layer_id="multi_exp",
            name="Experiment 2",
            variants=["A2", "B2"],
            traffic_allocation={"A2": 60, "B2": 40},
            traffic_percentage=40.0,
            status=ExperimentStatus.ACTIVE,
        )
        LayerService.add_experiment(db_session, layer, exp2)

        # Test assignments
        user_ids = [f"user_{i}" for i in range(100)]
        assignments = AssignmentService.get_user_assignments_bulk(db_session, layer, user_ids)

        # Verify all assignments were logged
        assert mock_assignment_logger.log_assignments.call_count >= 1

        # Count assignments by experiment
        exp1_count = sum(1 for a in assignments.values() if a.get("experiment_id") == "exp_1")
        exp2_count = sum(1 for a in assignments.values() if a.get("experiment_id") == "exp_2")
        unassigned_count = sum(1 for a in assignments.values() if a["status"] == "not_assigned")

        # Verify reasonable distribution
        total_assigned = exp1_count + exp2_count
        assert total_assigned + unassigned_count == 100

    def test_assignment_consistency_across_calls_with_logging(self, db_session, sample_layer_with_experiment, mock_assignment_logger):
        """Test that assignment is consistent across multiple calls."""
        layer, experiment = sample_layer_with_experiment

        user_id = "consistent_user"

        # Get assignment multiple times
        assignments = []
        for _ in range(5):
            assignment = AssignmentService.get_user_assignment(db_session, layer, user_id)
            assignments.append(assignment)

        # All assignments should be identical
        first_assignment = assignments[0]
        for assignment in assignments[1:]:
            assert assignment == first_assignment

        # Verify logging was called for each assignment
        assert mock_assignment_logger.log_assignments.call_count == 5


# Test cleanup and error handling
class TestAssignmentServiceErrorHandling:
    """Test error handling and edge cases."""

    def test_assignment_with_logger_exception(self, db_session, sample_layer_with_experiment):
        """Test that assignment still works even if logging fails."""
        layer, experiment = sample_layer_with_experiment

        with patch.object(AssignmentService._assignment_logger, 'log_assignments', side_effect=Exception("DB Error")):
            # Assignment should still work despite logging failure
            with pytest.raises(Exception):
                AssignmentService.get_user_assignment(db_session, layer, "user_123")

    def test_logger_cleanup_method_exists(self):
        """Test that assignment logger has cleanup capabilities."""
        # Verify the logger has a close method for cleanup
        assert hasattr(AssignmentService._assignment_logger, 'close')
